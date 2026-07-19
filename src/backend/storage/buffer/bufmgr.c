/*-------------------------------------------------------------------------
 *
 * bufmgr.c
 *	  buffer manager interface routines
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/bufmgr.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * Principal entry points:
 *
 * ReadBuffer() -- find or create a buffer holding the requested page,
 *		and pin it so that no one can destroy it while this process
 *		is using it.
 *
 * ReleaseBuffer() -- unpin a buffer
 *
 * MarkBufferDirty() -- mark a pinned buffer's contents as "dirty".
 *		The disk write is delayed until buffer replacement or checkpoint.
 *
 * See also these files:
 *		freelist.c -- chooses victim for buffer replacement
 *		buf_table.c -- manages the buffer lookup table
 */
#include "postgres.h"

#include <sys/file.h>
#include <unistd.h>

#include "access/tableam.h"
#include "access/xlog.h"		/* PGRAC (spec-6.14 D8): GetXLogInsertRecPtr */
#ifdef USE_PGRAC_CLUSTER
#include "access/transam.h"		/* FirstNormalObjectId */
#endif
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/catalog.h"
#ifdef USE_ASSERT_CHECKING
#include "catalog/pg_tablespace_d.h"
#endif
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "executor/instrument.h"
#include "lib/binaryheap.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "postmaster/bgwriter.h"
#include "storage/buf_internals.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_catalog_stats.h"	/* spec-6.14 D10b — catalog buf hit/miss */
#include "cluster/cluster_mode.h"	/* PGRAC (spec-6.14 D8): storage-mode gate */
#include "cluster/cluster_pcm_direct_init.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_grd.h"		/* existing block-path fail-closed counter */
#include "cluster/cluster_guc.h"		/* spec-4.7a D2 — cluster_gcs_block_local_cache */
#include "cluster/cluster_block_recovery.h" /* spec-4.10 D1 — online block recovery */
#include "cluster/cluster_conf.h"			/* spec-5.7 HW — cluster_conf_node_count */
#include "cluster/cluster_hw.h"				/* spec-5.7 HW — relation-extend authority */
#include "cluster/cluster_xnode_lever.h"	/* spec-6.12h — PI keep counter */
#include "cluster/cluster_pi_shadow.h"	/* spec-6.12h D-h3a — PI ship-SCN stamp */
#include "cluster/cluster_hw_lease.h"	/* spec-6.12d — per-node HW space leases */
#include "cluster/cluster_extend_gate.h" /* spec-5.7 §3.1d — liveness engage gate */
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_sf_dep.h"		/* spec-6.2 Smart Fusion DBWR brake */
#include "cluster/cluster_xnode_profile.h"	/* spec-5.59 D3/D4 — read probe + relkind hint */
#include "cluster/cluster_itl.h"	/* spec-6.12a — quiescent check for X->S downgrade */
#include "cluster/cluster_inject.h" /* GCS-race round-4c P1 — yield-notify-drop point */
#include "cluster/cluster_pcm_own.h" /* ownership-generation wave — per-buffer gen + flags */
#include "cluster/cluster_pcm_x_bufmgr.h" /* spec-2.36a C1 opaque reservation API */
#include "cluster/cluster_pcm_x_convert.h"

/*
 * PGRAC (spec-4.10 D1): ignore_checksum_failure is defined in bufpage.c with
 * no public extern; the online-recovery read hook consults it to honor Q3
 * (online recovery takes precedence over ignore_checksum_failure).  Reading
 * this bool short-circuits the Q3 re-check on the healthy path (default off),
 * so no extra per-read checksum work.
 */
extern bool ignore_checksum_failure;
#endif
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/smgr.h"
#include "storage/standby.h"
#include "utils/memdebug.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/rel.h"
#include "utils/resowner_private.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"


/* Note: these two macros only work on shared buffers, not local ones! */
#define BufHdrGetBlock(bufHdr)	((Block) (BufferBlocks + ((Size) (bufHdr)->buf_id) * BLCKSZ))
#define BufferGetLSN(bufHdr)	(PageGetLSN(BufHdrGetBlock(bufHdr)))

/* Note: this macro only works on local buffers, not shared ones! */
#define LocalBufHdrGetBlock(bufHdr) \
	LocalBufferBlockPointers[-((bufHdr)->buf_id + 2)]

/* Bits in SyncOneBuffer's return value */
#define BUF_WRITTEN				0x01
#define BUF_REUSABLE			0x02

#define RELS_BSEARCH_THRESHOLD		20

#ifdef USE_PGRAC_CLUSTER
/*
 * spec-6.14 D4: single PCM-tracking criterion by relfilenumber.  Under
 * cluster.shared_catalog the catalog lives in the single shared tree, so its
 * pages need the same N/S/X + CR + PI lost-write coherency as user pages
 * (routing a catalog page into the shared tree without PCM tracking would be a
 * double-write hazard -- pairs with the D3 smgr flip, INV-14-1).  All shared
 * buffers under shared_catalog=on hold permanent (non-temp) relations: temp
 * relations use local buffers and never reach these paths, and unlogged
 * permanent relations are rejected at DDL time (Q12).  Off mode keeps the
 * historic FirstNormalObjectId user-only boundary.
 *
 * The eviction/release hooks (HC112) MUST use this SAME criterion as the
 * acquire path, otherwise a tracked catalog page's PCM lock would leak on
 * eviction (spec-6.14 R6 -- do not let a gate drift).
 */
static inline bool
cluster_bufmgr_reln_pcm_tracked(RelFileNumber relnum)
{
	return cluster_shared_catalog || relnum >= (RelFileNumber) FirstNormalObjectId;
}

static inline bool
cluster_bufmgr_should_pcm_track(BufferDesc *buf)
{
	return cluster_bufmgr_reln_pcm_tracked(BufTagGetRelNumber(&buf->tag));
}

/* Defined with the GCS copy/drop substrate below.  The queue ownership
 * adapters live earlier so LockBuffer can share them without exposing raw
 * buffer-manager pin mechanics outside this file. */
static XLogRecPtr cluster_gcs_clamp_ship_flush_lsn(XLogRecPtr page_lsn);
static void cluster_bufmgr_pin_for_gcs_locked(BufferDesc *buf, uint32 buf_state);
static void cluster_bufmgr_unpin_for_gcs(BufferDesc *buf);

static ClusterPcmOwnResult
cluster_pcm_own_bump_failure(BufferDesc *buf, uint64 generation, uint32 *out_flags)
{
	ClusterPcmOwnResult live_result;
	uint64 reservation_token;
	uint32 flags;

	reservation_token = cluster_pcm_own_reservation_token_get(buf->buf_id);
	flags = cluster_pcm_own_flags_get(buf->buf_id);
	if (out_flags != NULL)
		*out_flags = flags;
	live_result = cluster_pcm_own_classify_live_flags(flags, reservation_token);
	if (live_result != CLUSTER_PCM_OWN_OK)
		return live_result;
	if (generation == UINT64_MAX)
		return CLUSTER_PCM_OWN_EXHAUSTED;
	if (ClusterPcmOwnArray == NULL)
		return CLUSTER_PCM_OWN_NOT_READY;
	return CLUSTER_PCM_OWN_CORRUPT;
}

static void
cluster_pcm_own_report_bump_failure(BufferDesc *buf, ClusterPcmOwnResult result,
								   uint64 generation, uint32 flags, const char *context)
{
	if (result == CLUSTER_PCM_OWN_BUSY)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("cluster PCM ownership transition conflicts with an active reservation"),
				 errdetail("context=%s buffer=%d generation=%llu flags=0x%x", context,
						   buf->buf_id, (unsigned long long) generation, flags)));
	if (result == CLUSTER_PCM_OWN_EXHAUSTED)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cluster PCM ownership generation exhausted for buffer %d", buf->buf_id),
				 errdetail("context=%s generation=%llu flags=0x%x", context,
						   (unsigned long long) generation, flags)));

	ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
			 errmsg("cluster PCM ownership generation bump failed for buffer %d", buf->buf_id),
			 errdetail("context=%s generation=%llu flags=0x%x result=%d", context,
					   (unsigned long long) generation, flags, (int) result)));
}

static inline ClusterPcmOwnResult cluster_pcm_own_bump_locked(BufferDesc *buf,
													  uint32 set_flags, uint32 clear_flags,
													  uint64 *out_generation,
													  uint32 *out_flags);

/*
 * PGRAC ownership-generation wave — coherent
 * (pcm_state, generation, reservation_token, flags) mutation / read helpers.
 *
 * The complete tuple must move together under ONE lock so a reader never sees
 * a torn view (TOCTOU).  We use the buffer header spinlock as that lock.
 * pcm_state lives on the BufferDesc; generation + reservation_token + flags
 * live in the parallel ClusterPcmOwnArray indexed by buf_id (cluster_pcm_own.h).
 *
 * cluster_pcm_own_transition: for callers that do NOT already hold the header
 * spinlock (e.g. the LockBuffer grant mirror + the X->S downgrade, which hold
 * only the content lock).  Sets pcm_state, bumps the generation, and applies
 * flag changes atomically under the header spinlock.  Every COMMITTED local
 * ownership transition routes through here (or the *_locked bump below) so the
 * generation increments exactly once per ownership change.
 */
static inline void
cluster_pcm_own_transition(BufferDesc *buf, uint8 new_pcm_state, uint32 set_flags,
						   uint32 clear_flags)
{
	ClusterPcmOwnResult bump_result;
	uint32 buf_state;
	uint32 observed_flags = 0;
	uint64 new_generation;

	buf_state = LockBufHdr(buf);
	bump_result = cluster_pcm_own_bump_locked(buf, set_flags, clear_flags,
										  &new_generation, &observed_flags);
	if (bump_result != CLUSTER_PCM_OWN_OK) {
		UnlockBufHdr(buf, buf_state);
		cluster_pcm_own_report_bump_failure(buf, bump_result, new_generation,
										observed_flags, "ownership transition");
	}
	buf->pcm_state = new_pcm_state;
	UnlockBufHdr(buf, buf_state);
}

/*
 * cluster_pcm_own_transition_locked: for callers that ALREADY hold the header
 * spinlock (the N-flip invalidate/drop/evict sites).  Same effect minus the
 * lock/unlock.  Caller sets pcm_state itself (it already does); this bumps the
 * generation and applies flags in the same tuple-critical section.  The
 * monotonic reservation token is intentionally unchanged.
 */
static inline ClusterPcmOwnResult
cluster_pcm_own_bump_locked(BufferDesc *buf, uint32 set_flags, uint32 clear_flags,
							uint64 *out_generation, uint32 *out_flags)
{
	uint64 generation;

	if (!cluster_pcm_own_gen_bump_checked(buf->buf_id, &generation)) {
		if (out_generation != NULL)
			*out_generation = generation;
		return cluster_pcm_own_bump_failure(buf, generation, out_flags);
	}
	if (set_flags != 0 || clear_flags != 0)
		cluster_pcm_own_flags_apply(buf->buf_id, set_flags, clear_flags);
	if (out_generation != NULL)
		*out_generation = generation;
	if (out_flags != NULL)
		*out_flags = cluster_pcm_own_flags_get(buf->buf_id);
	return CLUSTER_PCM_OWN_OK;
}

static inline void
cluster_pcm_own_snapshot_locked(BufferDesc *buf, ClusterPcmOwnSnapshot *out)
{
	memset(out, 0, sizeof(*out));
	out->tag = buf->tag;
	out->generation = cluster_pcm_own_gen_get(buf->buf_id);
	out->reservation_token = cluster_pcm_own_reservation_token_get(buf->buf_id);
	out->flags = cluster_pcm_own_flags_get(buf->buf_id);
	out->pcm_state = buf->pcm_state;
}

/* A normal D-h1 PI is !BM_VALID.  PCM-X retained-image transfer deliberately
 * uses the otherwise-unoccupied PI+BM_VALID shape while N+REVOKING keeps the
 * exact descriptor pinned to the image record.  Write/reuse gates recognize
 * the conservative PI+VALID prefix: malformed sidecar metadata must remain
 * blocked rather than making old bytes flushable.  Call with header authority
 * or while holding the buffer content lock (retain/release take EXCLUSIVE). */
static inline bool
cluster_bufmgr_pcm_x_retained_image_locked(BufferDesc *buf, uint32 buf_state)
{
	return buf != NULL && buf->buffer_type == (uint8) BUF_TYPE_PI
		&& (buf_state & BM_VALID) != 0;
}

/* A resident mapping is GCS-shippable only while its node owns matching PCM
 * authority.  In particular, PI+BM_VALID N mirrors kept solely to break a
 * passive-pin invalidate ring must never be mistaken for a current source. */
static inline bool
cluster_bufmgr_pcm_current_image_locked(BufferDesc *buf, uint32 buf_state)
{
	return buf != NULL
		&& cluster_pcm_x_current_image_shape(buf->pcm_state, buf->buffer_type,
										 (buf_state & BM_VALID) != 0);
}

static inline bool
cluster_bufmgr_pcm_x_retained_image_reuse_blocked_locked(BufferDesc *buf,
												  uint32 buf_state)
{
	uint32 flags;

	if (!cluster_bufmgr_pcm_x_retained_image_locked(buf, buf_state))
		return false;
	flags = cluster_pcm_own_flags_get(buf->buf_id);

	/*
	 * A clean N remnant with no live reservation is ordinary eviction
	 * material.  Its monotonic token may legitimately still be zero before
	 * the first reservation, so flags -- not token value -- are the active
	 * lifecycle authority.  Every live or malformed shape remains blocked. */
	return buf->pcm_state != (uint8) PCM_STATE_N
		|| (buf_state
			& (BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED | BM_IO_ERROR)) != 0
		|| flags != 0;
}

static inline bool cluster_pcm_own_snapshot_matches_locked(
	BufferDesc *buf, const ClusterPcmOwnSnapshot *expected);

static inline void
cluster_pcm_own_eviction_capture_locked(BufferDesc *buf, ClusterPcmOwnEvictionCapture *out)
{
	cluster_pcm_own_snapshot_locked(buf, out);
}

static ClusterPcmOwnResult
cluster_pcm_own_eviction_commit_locked(BufferDesc *buf,
									   const ClusterPcmOwnEvictionCapture *capture,
									   uint64 *out_generation, uint32 *out_flags)
{
	ClusterPcmOwnResult result;

	if (!cluster_pcm_own_snapshot_matches_locked(buf, capture))
		return CLUSTER_PCM_OWN_STALE;
	if (!cluster_pcm_own_eviction_reuse_allowed(capture)) {
		result = cluster_pcm_own_classify_live_flags(capture->flags,
													 capture->reservation_token);
		return result != CLUSTER_PCM_OWN_OK ? result : CLUSTER_PCM_OWN_EXHAUSTED;
	}

	result = cluster_pcm_own_bump_locked(buf, 0, 0, out_generation, out_flags);
	if (result == CLUSTER_PCM_OWN_OK) {
		buf->pcm_state = (uint8)PCM_STATE_N;
		buf->buffer_type = (uint8)BUF_TYPE_CURRENT;
	}
	return result;
}

static inline bool
cluster_pcm_own_snapshot_matches_locked(BufferDesc *buf, const ClusterPcmOwnSnapshot *expected)
{
	return BufferTagsEqual(&buf->tag, &expected->tag)
		   && cluster_pcm_own_gen_get(buf->buf_id) == expected->generation
		   && cluster_pcm_own_reservation_token_get(buf->buf_id) == expected->reservation_token
		   && cluster_pcm_own_flags_get(buf->buf_id) == expected->flags
		   && buf->pcm_state == expected->pcm_state;
}

ClusterPcmOwnResult
cluster_bufmgr_pcm_own_snapshot(BufferDesc *buf, ClusterPcmOwnSnapshot *out_snapshot)
{
	uint32		buf_state;

	if (buf == NULL || out_snapshot == NULL)
		return CLUSTER_PCM_OWN_INVALID;
	memset(out_snapshot, 0, sizeof(*out_snapshot));
	if (ClusterPcmOwnArray == NULL)
		return CLUSTER_PCM_OWN_NOT_READY;
	buf_state = LockBufHdr(buf);
	cluster_pcm_own_snapshot_locked(buf, out_snapshot);
	UnlockBufHdr(buf, buf_state);
	return CLUSTER_PCM_OWN_OK;
}

bool
cluster_bufmgr_pcm_x_content_write_permitted(BufferDesc *buf)
{
	bool		permitted;
	uint32		buf_state;
	uint32		flags;

	if (buf == NULL)
		return false;
	buf_state = LockBufHdr(buf);
	flags = cluster_pcm_own_flags_get(buf->buf_id);
	permitted = (flags & PCM_OWN_FLAG_REVOKING) == 0
		&& (!cluster_bufmgr_pcm_x_retained_image_locked(buf, buf_state)
			|| flags == PCM_OWN_FLAG_GRANT_PENDING);
	UnlockBufHdr(buf, buf_state);
	return permitted;
}

ClusterPcmOwnResult
cluster_bufmgr_pcm_own_snapshot_by_tag(const BufferTag *tag, int *out_buffer_id,
									   ClusterPcmOwnSnapshot *out_snapshot)
{
	BufferDesc *buf;
	BufferTag	lookup_tag;
	LWLock	   *partition_lock;
	uint32		hashcode;
	uint32		buf_state;
	int			buf_id;
	ClusterPcmOwnResult result;

	if (out_buffer_id != NULL)
		*out_buffer_id = -1;
	if (out_snapshot != NULL)
		memset(out_snapshot, 0, sizeof(*out_snapshot));
	if (tag == NULL || out_buffer_id == NULL || out_snapshot == NULL)
		return CLUSTER_PCM_OWN_INVALID;
	if (ClusterPcmOwnArray == NULL)
		return CLUSTER_PCM_OWN_NOT_READY;

	lookup_tag = *tag;
	hashcode = BufTableHashCode(&lookup_tag);
	partition_lock = BufMappingPartitionLock(hashcode);
	LWLockAcquire(partition_lock, LW_SHARED);
	buf_id = BufTableLookup(&lookup_tag, hashcode);
	if (buf_id < 0)
	{
		LWLockRelease(partition_lock);
		return CLUSTER_PCM_OWN_STALE;
	}
	buf = GetBufferDescriptor(buf_id);
	buf_state = LockBufHdr(buf);
	if (!BufferTagsEqual(&buf->tag, tag) || (buf_state & BM_VALID) == 0)
		result = CLUSTER_PCM_OWN_STALE;
	else
	{
		cluster_pcm_own_snapshot_locked(buf, out_snapshot);
		*out_buffer_id = buf_id;
		result = CLUSTER_PCM_OWN_OK;
	}
	UnlockBufHdr(buf, buf_state);
	LWLockRelease(partition_lock);
	return result;
}

/*
 * Complete the descriptor side of an exact S->N queue normalization.
 *
 * The caller owns the wire/application proof: the authoritative master has
 * already ACKed this node's exact S release.  This boundary owns only the
 * local coherent tuple.  It therefore accepts one byte-exact S snapshot,
 * changes state and generation together under the buffer header spinlock,
 * and returns the fresh N snapshot that ACTIVE_TRANSFER/PREPARE must later
 * present.  Queue JOIN/WAIT never sets a reservation flag here.
 */
ClusterPcmOwnResult
cluster_bufmgr_pcm_own_finish_s_release_to_n(
	BufferDesc *buf, const ClusterPcmOwnSnapshot *expected_s,
	ClusterPcmOwnSnapshot *out_n_snapshot)
{
	ClusterPcmOwnResult result;
	uint32		buf_state;

	if (buf == NULL || expected_s == NULL || out_n_snapshot == NULL)
		return CLUSTER_PCM_OWN_INVALID;
	memset(out_n_snapshot, 0, sizeof(*out_n_snapshot));
	if (ClusterPcmOwnArray == NULL)
		return CLUSTER_PCM_OWN_NOT_READY;
	if (expected_s->pcm_state != (uint8) PCM_STATE_S || expected_s->flags != 0)
		return CLUSTER_PCM_OWN_STALE;

	buf_state = LockBufHdr(buf);
	if (!cluster_pcm_own_snapshot_matches_locked(buf, expected_s))
		result = CLUSTER_PCM_OWN_STALE;
	else
	{
		result = cluster_pcm_own_bump_locked(buf, 0, 0, NULL, NULL);
		if (result == CLUSTER_PCM_OWN_OK)
		{
			buf->pcm_state = (uint8) PCM_STATE_N;
			buf->buffer_type = (uint8) BUF_TYPE_CURRENT;
			cluster_pcm_own_snapshot_locked(buf, out_n_snapshot);
		}
	}
	UnlockBufHdr(buf, buf_state);
	return result;
}

/*
 * Release a passively pinned MAIN/INIT S mirror for a queue INVALIDATE.
 *
 * A PG pin protects descriptor identity but is not content authority.  The
 * ordinary invalidate path nevertheless cannot unmap a pinned descriptor,
 * which makes every hot-block writer wait on its own pin.  Bind tag->buffer
 * under the mapping lock, add one raw pin, then serialize all byte users with
 * content EXCLUSIVE.  Two exact tuple checks around WAL flush close retag,
 * generation, reservation, and page-LSN ABA windows.  Success keeps the clean
 * bytes BM_VALID as an N mirror; every later LockBuffer must reacquire PCM
 * before consuming them.  VM/FSM never enter this shape because their
 * pin-only readers may consume bytes without that content-lock gate.
 */
ClusterPcmOwnResult
cluster_bufmgr_pcm_own_release_pinned_s_for_gcs(const BufferTag *tag,
												XLogRecPtr *out_page_lsn,
												uint64 *out_page_scn)
{
	BufferTag lookup_tag;
	ClusterPcmOwnSnapshot expected_s;
	ClusterPcmOwnResult live_result;
	ClusterPcmOwnResult result = CLUSTER_PCM_OWN_OK;
	BufferDesc *buf;
	LWLock *content_lock;
	LWLock *partition_lock;
	Page page;
	XLogRecPtr page_lsn = InvalidXLogRecPtr;
	uint64 page_scn = 0;
	uint32 buf_state;
	uint32 hashcode;
	uint32 shared_refcount;
	uint32 flags;
	uint64 token;
	int buf_id;
	bool dirty = false;
	volatile bool content_locked = false;

	if (out_page_lsn != NULL)
		*out_page_lsn = InvalidXLogRecPtr;
	if (out_page_scn != NULL)
		*out_page_scn = 0;
	if (tag == NULL || out_page_lsn == NULL || out_page_scn == NULL)
		return CLUSTER_PCM_OWN_INVALID;
	if (ClusterPcmOwnArray == NULL)
		return CLUSTER_PCM_OWN_NOT_READY;

	lookup_tag = *tag;
	hashcode = BufTableHashCode(&lookup_tag);
	partition_lock = BufMappingPartitionLock(hashcode);
	LWLockAcquire(partition_lock, LW_SHARED);
	buf_id = BufTableLookup(&lookup_tag, hashcode);
	if (buf_id < 0)
	{
		LWLockRelease(partition_lock);
		return CLUSTER_PCM_OWN_STALE;
	}
	buf = GetBufferDescriptor(buf_id);
	buf_state = LockBufHdr(buf);
	if (!BufferTagsEqual(&buf->tag, tag) || (buf_state & BM_VALID) == 0)
		result = CLUSTER_PCM_OWN_STALE;
	else
	{
		shared_refcount = BUF_STATE_GET_REFCOUNT(buf_state);
		token = cluster_pcm_own_reservation_token_get(buf->buf_id);
		flags = cluster_pcm_own_flags_get(buf->buf_id);
		live_result = cluster_pcm_own_classify_live_flags(flags, token);
		if (live_result == CLUSTER_PCM_OWN_CORRUPT)
			result = live_result;
		else if (buf->pcm_state != (uint8) PCM_STATE_S)
			result = CLUSTER_PCM_OWN_STALE;
		else if (flags != 0)
			result = CLUSTER_PCM_OWN_BUSY;
		else if (!cluster_bufmgr_pcm_current_image_locked(buf, buf_state))
			result = CLUSTER_PCM_OWN_CORRUPT;
		else if ((buf_state & BM_IO_IN_PROGRESS) != 0)
			result = CLUSTER_PCM_OWN_BUSY;
		else if (shared_refcount == 0)
			result = CLUSTER_PCM_OWN_STALE;
		else if (cluster_pcm_x_revoke_finish_mode(tag, shared_refcount)
				 != CLUSTER_PCM_X_REVOKE_FINISH_RETAIN)
			result = CLUSTER_PCM_OWN_BUSY;
		else
		{
			cluster_pcm_own_snapshot_locked(buf, &expected_s);
			cluster_bufmgr_pin_for_gcs_locked(buf, buf_state);
			buf_state = 0;
		}
	}
	if (buf_state != 0)
		UnlockBufHdr(buf, buf_state);
	LWLockRelease(partition_lock);
	if (result != CLUSTER_PCM_OWN_OK)
		return result;

	content_lock = BufferDescriptorGetContentLock(buf);
	PG_TRY();
	{
		LWLockAcquire(content_lock, LW_EXCLUSIVE);
		content_locked = true;
		buf_state = LockBufHdr(buf);
		if (!cluster_pcm_own_snapshot_matches_locked(buf, &expected_s)
			|| (buf_state & BM_VALID) == 0
			|| !cluster_bufmgr_pcm_current_image_locked(buf, buf_state)
			|| (buf_state & BM_IO_IN_PROGRESS) != 0)
			result = CLUSTER_PCM_OWN_STALE;
		else
		{
			page = (Page) BufHdrGetBlock(buf);
			page_lsn = PageGetLSN(page);
			page_scn = (uint64) ((PageHeader) page)->pd_block_scn;
			dirty = (buf_state & BM_DIRTY) != 0;
		}
		UnlockBufHdr(buf, buf_state);

		if (result == CLUSTER_PCM_OWN_OK && dirty && !XLogRecPtrIsInvalid(page_lsn))
			XLogFlush(cluster_gcs_clamp_ship_flush_lsn(page_lsn));

		if (result == CLUSTER_PCM_OWN_OK)
		{
			buf_state = LockBufHdr(buf);
			page = (Page) BufHdrGetBlock(buf);
			if (!cluster_pcm_own_snapshot_matches_locked(buf, &expected_s)
				|| (buf_state & BM_VALID) == 0
				|| !cluster_bufmgr_pcm_current_image_locked(buf, buf_state)
				|| (buf_state & BM_IO_IN_PROGRESS) != 0
				|| PageGetLSN(page) != page_lsn)
				result = CLUSTER_PCM_OWN_STALE;
			else
			{
				result = cluster_pcm_own_bump_locked(buf, 0, 0, NULL, NULL);
				if (result == CLUSTER_PCM_OWN_OK)
				{
					buf->pcm_state = (uint8) PCM_STATE_N;
					/* PI+BM_VALID is the established never-write/never-serve shape.
					 * Unlike a source retain it has no live reservation flag (the
					 * monotonic token remains idle): the next exact GRANT_PENDING
					 * install may overwrite it, then republishes CURRENT. */
					buf->buffer_type = (uint8) BUF_TYPE_PI;
					buf_state &= ~(BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED
								   | BM_IO_ERROR);
				}
			}
			UnlockBufHdr(buf, buf_state);
		}

		LWLockRelease(content_lock);
		content_locked = false;
	}
	PG_CATCH();
	{
		if (content_locked && LWLockHeldByMe(content_lock))
			LWLockRelease(content_lock);
		cluster_bufmgr_unpin_for_gcs(buf);
		PG_RE_THROW();
	}
	PG_END_TRY();

	cluster_bufmgr_unpin_for_gcs(buf);
	if (result == CLUSTER_PCM_OWN_OK)
	{
		*out_page_lsn = page_lsn;
		*out_page_scn = page_scn;
	}
	return result;
}

/* Make freshly installed bytes visible only while the same queue
 * reservation still owns both the descriptor and content EXCLUSIVE. */
ClusterPcmOwnResult
cluster_bufmgr_pcm_own_publish_installed_x_image(
	BufferDesc *buf, const ClusterPcmOwnSnapshot *expected, uint64 reservation_token)
{
	ClusterPcmOwnResult result = CLUSTER_PCM_OWN_OK;
	uint32 buf_state;

	if (buf == NULL || expected == NULL || reservation_token == 0)
		return CLUSTER_PCM_OWN_INVALID;
	if (!LWLockHeldByMe(BufferDescriptorGetContentLock(buf)))
		return CLUSTER_PCM_OWN_INVALID;
	if (ClusterPcmOwnArray == NULL)
		return CLUSTER_PCM_OWN_NOT_READY;
	if (expected->pcm_state != (uint8) PCM_STATE_N || expected->flags != 0
		|| expected->reservation_token == UINT64_MAX
		|| reservation_token != expected->reservation_token + 1)
		return CLUSTER_PCM_OWN_STALE;

	buf_state = LockBufHdr(buf);
	if (!BufferTagsEqual(&buf->tag, &expected->tag)
		|| cluster_pcm_own_gen_get(buf->buf_id) != expected->generation
		|| cluster_pcm_own_reservation_token_get(buf->buf_id) != reservation_token
		|| cluster_pcm_own_flags_get(buf->buf_id) != PCM_OWN_FLAG_GRANT_PENDING
		|| buf->pcm_state != (uint8) PCM_STATE_N)
		result = CLUSTER_PCM_OWN_STALE;
	else
	{
		buf_state |= BM_VALID;
		buf->buffer_type = (uint8) BUF_TYPE_CURRENT;
	}
	UnlockBufHdr(buf, buf_state);
	return result;
}

static ClusterPcmOwnResult
cluster_pcm_own_begin_grant_reservation(BufferDesc *buf, ClusterPcmOwnSnapshot *out_base,
										uint64 *out_token)
{
	ClusterPcmOwnResult result;
	uint32 buf_state;

	if (buf == NULL || out_base == NULL || out_token == NULL)
		return CLUSTER_PCM_OWN_INVALID;
	memset(out_base, 0, sizeof(*out_base));
	*out_token = 0;
	if (ClusterPcmOwnArray == NULL)
		return CLUSTER_PCM_OWN_NOT_READY;

	buf_state = LockBufHdr(buf);
	cluster_pcm_own_snapshot_locked(buf, out_base);
	result = cluster_pcm_own_reservation_begin_exact(buf->buf_id, out_base->generation,
													 PCM_OWN_FLAG_GRANT_PENDING, out_token);
	UnlockBufHdr(buf, buf_state);
	return result;
}

ClusterPcmOwnResult
cluster_bufmgr_pcm_own_begin_x_reservation(BufferDesc *buf, const ClusterPcmOwnSnapshot *expected,
										   uint64 *out_token)
{
	ClusterPcmOwnResult result;
	uint32 buf_state;

	if (buf == NULL || expected == NULL || out_token == NULL)
		return CLUSTER_PCM_OWN_INVALID;
	*out_token = 0;
	if (ClusterPcmOwnArray == NULL)
		return CLUSTER_PCM_OWN_NOT_READY;
	if (expected->pcm_state != (uint8)PCM_STATE_N || expected->flags != 0)
		return CLUSTER_PCM_OWN_STALE;

	buf_state = LockBufHdr(buf);
	if (!cluster_pcm_own_snapshot_matches_locked(buf, expected))
		result = CLUSTER_PCM_OWN_STALE;
	else
		result = cluster_pcm_own_reservation_begin_exact(buf->buf_id, expected->generation,
														 PCM_OWN_FLAG_GRANT_PENDING, out_token);
	UnlockBufHdr(buf, buf_state);
	return result;
}

/* Reuse an exact requester-as-source N/S/X revoke lifecycle for its X grant.
 * The caller holds content EXCLUSIVE across immutable-page validation and
 * this linearization; no second token or generation is allocated here.  An
 * exact duplicate PREPARE observes GRANT_PENDING and is idempotent.  After
 * the handoff ACTIVE_TRANSFER is irreversible: cleanup must preserve
 * GRANT_PENDING evidence and fail closed, never toggle it back. */
ClusterPcmOwnResult
cluster_bufmgr_pcm_own_handoff_revoke_to_x_reservation(
	BufferDesc *buf, const ClusterPcmOwnSnapshot *expected_revoking, uint64 *out_token)
{
	ClusterPcmOwnResult live_result;
	ClusterPcmOwnResult result = CLUSTER_PCM_OWN_OK;
	LWLock *content_lock;
	uint64 live_token;
	uint32 flags;
	uint32 buf_state;
	bool source_is_n;
	bool source_is_s;
	bool source_is_x;

	if (out_token != NULL)
		*out_token = 0;
	if (buf == NULL || expected_revoking == NULL || out_token == NULL)
		return CLUSTER_PCM_OWN_INVALID;
	source_is_n = expected_revoking->pcm_state == (uint8)PCM_STATE_N;
	source_is_s = expected_revoking->pcm_state == (uint8)PCM_STATE_S;
	source_is_x = expected_revoking->pcm_state == (uint8)PCM_STATE_X;
	if ((!source_is_n && !source_is_s && !source_is_x)
		|| expected_revoking->flags != PCM_OWN_FLAG_REVOKING
		|| expected_revoking->reservation_token == 0)
		return CLUSTER_PCM_OWN_STALE;
	if (ClusterPcmOwnArray == NULL)
		return CLUSTER_PCM_OWN_NOT_READY;
	content_lock = BufferDescriptorGetContentLock(buf);
	if (!LWLockHeldByMe(content_lock))
		return CLUSTER_PCM_OWN_INVALID;

	buf_state = LockBufHdr(buf);
	live_token = cluster_pcm_own_reservation_token_get(buf->buf_id);
	flags = cluster_pcm_own_flags_get(buf->buf_id);
	live_result = cluster_pcm_own_classify_live_flags(flags, live_token);
	if (!BufferTagsEqual(&buf->tag, &expected_revoking->tag)
		|| cluster_pcm_own_gen_get(buf->buf_id) != expected_revoking->generation
		|| buf->pcm_state != expected_revoking->pcm_state
		|| live_token != expected_revoking->reservation_token)
		result = CLUSTER_PCM_OWN_STALE;
	else if (live_result == CLUSTER_PCM_OWN_CORRUPT)
		result = live_result;
	else if ((buf_state & BM_VALID) == 0)
		result = CLUSTER_PCM_OWN_CORRUPT;
	else if (source_is_n
			 && ((buf_state & (BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED | BM_IO_ERROR))
					 != 0
				 || buf->buffer_type != (uint8)BUF_TYPE_CURRENT))
		result = CLUSTER_PCM_OWN_CORRUPT;
	else if (!source_is_n && !cluster_bufmgr_pcm_current_image_locked(buf, buf_state))
		result = CLUSTER_PCM_OWN_CORRUPT;
	else if ((buf_state & BM_IO_IN_PROGRESS) != 0)
		result = CLUSTER_PCM_OWN_BUSY;
	else if (flags == PCM_OWN_FLAG_GRANT_PENDING)
		result = CLUSTER_PCM_OWN_OK;
	else if (flags == 0)
		result = CLUSTER_PCM_OWN_STALE;
	else if (flags != PCM_OWN_FLAG_REVOKING)
		result = CLUSTER_PCM_OWN_BUSY;
	else
		result = cluster_pcm_own_revoke_to_grant_handoff_exact(
			buf->buf_id, expected_revoking->generation, expected_revoking->reservation_token);
	UnlockBufHdr(buf, buf_state);
	if (result == CLUSTER_PCM_OWN_OK)
		*out_token = expected_revoking->reservation_token;
	return result;
}

ClusterPcmOwnResult
cluster_bufmgr_pcm_own_handoff_s_revoke_to_x_reservation(
	BufferDesc *buf, const ClusterPcmOwnSnapshot *expected_revoking, uint64 *out_token)
{
	if (expected_revoking == NULL || expected_revoking->pcm_state != (uint8)PCM_STATE_S)
		return CLUSTER_PCM_OWN_STALE;
	return cluster_bufmgr_pcm_own_handoff_revoke_to_x_reservation(buf, expected_revoking,
																  out_token);
}

static ClusterPcmOwnResult
cluster_pcm_own_finish_grant_reservation(BufferDesc *buf, const ClusterPcmOwnSnapshot *base,
										 uint64 reservation_token, uint8 new_pcm_state,
										 uint64 *out_committed_generation)
{
	ClusterPcmOwnSnapshot live;
	ClusterPcmXGrantReservationKind kind;
	ClusterPcmOwnResult result;
	uint32 buf_state;

	if (buf == NULL || base == NULL || out_committed_generation == NULL)
		return CLUSTER_PCM_OWN_INVALID;
	*out_committed_generation = 0;
	if (new_pcm_state != (uint8)PCM_STATE_S && new_pcm_state != (uint8)PCM_STATE_X)
		return CLUSTER_PCM_OWN_INVALID;
	if (ClusterPcmOwnArray == NULL)
		return CLUSTER_PCM_OWN_NOT_READY;

	buf_state = LockBufHdr(buf);
	cluster_pcm_own_snapshot_locked(buf, &live);
	kind = cluster_pcm_x_grant_reservation_kind(&live, base, reservation_token);
	if (kind == CLUSTER_PCM_X_GRANT_RESERVATION_INVALID) {
		ClusterPcmOwnResult live_shape
			= cluster_pcm_own_classify_live_flags(live.flags, live.reservation_token);

		result = live_shape == CLUSTER_PCM_OWN_CORRUPT ? live_shape : CLUSTER_PCM_OWN_STALE;
	} else if (kind != CLUSTER_PCM_X_GRANT_RESERVATION_N_NEW
			   && (new_pcm_state != (uint8)PCM_STATE_X
				   || !LWLockHeldByMe(BufferDescriptorGetContentLock(buf))))
		result = CLUSTER_PCM_OWN_INVALID;
	else {
		result = cluster_pcm_own_grant_commit_exact(buf->buf_id, base->generation,
													reservation_token, out_committed_generation);
		if (result == CLUSTER_PCM_OWN_OK) {
			buf->buffer_type
				= new_pcm_state == (uint8)PCM_STATE_S ? (uint8)BUF_TYPE_SCUR : (uint8)BUF_TYPE_XCUR;
			buf->pcm_state = new_pcm_state;
		}
	}
	UnlockBufHdr(buf, buf_state);
	return result;
}

ClusterPcmOwnResult
cluster_bufmgr_pcm_own_finish_x_commit(BufferDesc *buf, const ClusterPcmOwnSnapshot *expected,
									   uint64 reservation_token, uint64 *out_committed_generation)
{
	if (expected == NULL
		|| (expected->pcm_state != (uint8)PCM_STATE_N && expected->pcm_state != (uint8)PCM_STATE_S
			&& expected->pcm_state != (uint8)PCM_STATE_X))
		return CLUSTER_PCM_OWN_STALE;
	return cluster_pcm_own_finish_grant_reservation(buf, expected, reservation_token,
													(uint8)PCM_STATE_X, out_committed_generation);
}

static ClusterPcmOwnResult
cluster_pcm_own_abort_grant_reservation(BufferDesc *buf, const ClusterPcmOwnSnapshot *base,
										uint64 reservation_token)
{
	ClusterPcmOwnResult result;
	uint32 buf_state;

	if (buf == NULL || base == NULL)
		return CLUSTER_PCM_OWN_INVALID;
	if (base->flags != 0 || base->reservation_token == UINT64_MAX
		|| reservation_token != base->reservation_token + 1)
		return CLUSTER_PCM_OWN_STALE;
	if (ClusterPcmOwnArray == NULL)
		return CLUSTER_PCM_OWN_NOT_READY;

	buf_state = LockBufHdr(buf);
	if (!BufferTagsEqual(&buf->tag, &base->tag)
		|| cluster_pcm_own_gen_get(buf->buf_id) != base->generation
		|| buf->pcm_state != base->pcm_state)
		result = CLUSTER_PCM_OWN_STALE;
	else
		result = cluster_pcm_own_reservation_abort_exact(
			buf->buf_id, base->generation, reservation_token, PCM_OWN_FLAG_GRANT_PENDING);
	UnlockBufHdr(buf, buf_state);
	return result;
}

static void
cluster_pcm_own_abort_grant_or_error(BufferDesc *buf, const ClusterPcmOwnSnapshot *base,
									 uint64 reservation_token, const char *context)
{
	ClusterPcmOwnResult result;

	result = cluster_pcm_own_abort_grant_reservation(buf, base, reservation_token);
	if (result != CLUSTER_PCM_OWN_OK)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("could not abort exact cluster PCM grant reservation: result=%d",
							   (int)result),
						errdetail("context=%s buffer=%d generation=%llu token=%llu", context,
								  buf != NULL ? buf->buf_id : -1,
								  (unsigned long long)(base != NULL ? base->generation : 0),
								  (unsigned long long)reservation_token)));
}

static void
cluster_pcm_own_abort_grant_after_error(BufferDesc *buf, const ClusterPcmOwnSnapshot *base,
										uint64 reservation_token, const char *context)
{
	ClusterPcmOwnResult result;

	result = cluster_pcm_own_abort_grant_reservation(buf, base, reservation_token);
	if (result != CLUSTER_PCM_OWN_OK)
		elog(LOG,
			 "failed to abort exact cluster PCM grant reservation while handling "
			 "another error: context=%s buffer=%d generation=%llu token=%llu result=%d; "
			 "reservation remains fail-closed",
			 context, buf != NULL ? buf->buf_id : -1,
			 (unsigned long long)(base != NULL ? base->generation : 0),
			 (unsigned long long)reservation_token, (int)result);
}

static ClusterPcmOwnResult
cluster_pcm_own_abort_grant_after_master_rollback(BufferDesc *buf,
												  const ClusterPcmOwnSnapshot *base,
												  uint64 reservation_token)
{
	ClusterPcmOwnResult result;
	uint32 buf_state;
	uint64 new_generation;

	if (buf == NULL || base == NULL)
		return CLUSTER_PCM_OWN_INVALID;
	if (base->flags != 0 || base->reservation_token == UINT64_MAX
		|| reservation_token != base->reservation_token + 1)
		return CLUSTER_PCM_OWN_STALE;
	if (ClusterPcmOwnArray == NULL)
		return CLUSTER_PCM_OWN_NOT_READY;

	buf_state = LockBufHdr(buf);
	if (base->pcm_state != (uint8)PCM_STATE_N && base->pcm_state != (uint8)PCM_STATE_S)
		result = CLUSTER_PCM_OWN_INVALID;
	else if (!BufferTagsEqual(&buf->tag, &base->tag)
			 || cluster_pcm_own_gen_get(buf->buf_id) != base->generation
			 || cluster_pcm_own_reservation_token_get(buf->buf_id) != reservation_token
			 || cluster_pcm_own_flags_get(buf->buf_id) != PCM_OWN_FLAG_GRANT_PENDING
			 || buf->pcm_state != base->pcm_state)
		result = CLUSTER_PCM_OWN_STALE;
	else if (base->pcm_state == (uint8)PCM_STATE_S && base->generation == UINT64_MAX)
		result = CLUSTER_PCM_OWN_EXHAUSTED;
	else {
		result = cluster_pcm_own_reservation_abort_exact(
			buf->buf_id, base->generation, reservation_token, PCM_OWN_FLAG_GRANT_PENDING);
		if (result == CLUSTER_PCM_OWN_OK && base->pcm_state == (uint8)PCM_STATE_S) {
			if (!cluster_pcm_own_gen_bump_checked(buf->buf_id, &new_generation)) {
				/*
				 * The MAX check above and this header-lock hold make failure
				 * unreachable for a coherent entry.  Restore the same token's
				 * live marker if corruption/not-ready is nevertheless
				 * observed, so the caller fails closed instead of advertising
				 * completed cleanup.
				 */
				cluster_pcm_own_flags_apply(buf->buf_id, PCM_OWN_FLAG_GRANT_PENDING, 0);
				result = CLUSTER_PCM_OWN_CORRUPT;
			} else
				buf->pcm_state = (uint8)PCM_STATE_N;
		}
	}
	UnlockBufHdr(buf, buf_state);
	return result;
}

pg_attribute_noreturn() static void
cluster_pcm_own_rollback_grant_after_error_and_rethrow(
	BufferDesc *buf, const ClusterPcmOwnSnapshot *base, uint64 reservation_token,
	PcmLockMode acquired_mode, bool has_reservation, ErrorData *original_error,
	MemoryContext caller_context)
{
	ClusterPcmOwnResult rollback_result;
	ErrorData *cleanup_error;
	volatile bool master_released = false;

	/*
	 * LockBuffer saved and flushed the original error before either holder or
	 * master cleanup.  Work in that caller-owned context so a throwing remote
	 * release can be copied, logged, and discarded without replacing it.
	 */
	Assert(original_error != NULL);
	MemoryContextSwitchTo(caller_context);

	PG_TRY();
	{
		cluster_pcm_lock_release_buffer_for_eviction(buf, acquired_mode);
		master_released = true;
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(caller_context);
		cleanup_error = CopyErrorData();
		FlushErrorState();
		elog(LOG,
			 "failed to roll back cluster PCM master grant while preserving an earlier "
			 "content-lock error: buffer=%d token=%llu cleanup_error=%s; reservation "
			 "remains fail-closed",
			 buf != NULL ? buf->buf_id : -1, (unsigned long long)reservation_token,
			 cleanup_error->message != NULL ? cleanup_error->message : "unknown");
		FreeErrorData(cleanup_error);
	}
	PG_END_TRY();

	if (master_released) {
		if (has_reservation) {
			rollback_result = cluster_pcm_own_abort_grant_after_master_rollback(
				buf, base, reservation_token);
			if (rollback_result != CLUSTER_PCM_OWN_OK)
				elog(LOG,
					 "failed to converge local cluster PCM state after content-lock error "
					 "master rollback: buffer=%d token=%llu result=%d; reservation remains "
					 "fail-closed",
					 buf != NULL ? buf->buf_id : -1,
					 (unsigned long long)reservation_token, (int)rollback_result);
		} else
			elog(LOG,
				 "cluster PCM content-lock error rolled back a master grant without local "
				 "reservation evidence for buffer %d",
				 buf != NULL ? buf->buf_id : -1);
	}

	ReThrowError(original_error);
}

static void
cluster_pcm_own_finish_grant_or_rollback(BufferDesc *buf, const ClusterPcmOwnSnapshot *base,
										 uint64 reservation_token, uint8 new_pcm_state,
										 PcmLockMode acquired_mode,
										 uint64 *out_committed_generation)
{
	ClusterPcmOwnResult finish_result;
	ClusterPcmOwnResult rollback_result;

	finish_result = cluster_pcm_own_finish_grant_reservation(
		buf, base, reservation_token, new_pcm_state, out_committed_generation);
	if (finish_result == CLUSTER_PCM_OWN_OK)
		return;

	/*
	 * The master grant is already durable.  Do not clear local reservation
	 * evidence until its X/S holder has been released successfully.
	 */
	PG_TRY();
	{
		cluster_pcm_lock_release_buffer_for_eviction(buf, acquired_mode);
	}
	PG_CATCH();
	{
		elog(LOG,
			 "cluster PCM exact finish failed (result=%d) and master grant rollback "
			 "also failed for buffer %d token=%llu; reservation remains fail-closed",
			 (int)finish_result, buf != NULL ? buf->buf_id : -1,
			 (unsigned long long)reservation_token);
		PG_RE_THROW();
	}
	PG_END_TRY();

	rollback_result
		= cluster_pcm_own_abort_grant_after_master_rollback(buf, base, reservation_token);
	if (rollback_result != CLUSTER_PCM_OWN_OK)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("could not converge local cluster PCM state after master grant rollback: "
						"result=%d",
						(int)rollback_result),
				 errdetail("buffer=%d base_state=%u generation=%llu token=%llu finish_result=%d",
						   buf != NULL ? buf->buf_id : -1,
						   base != NULL ? (unsigned int)base->pcm_state : 0,
						   (unsigned long long)(base != NULL ? base->generation : 0),
						   (unsigned long long)reservation_token, (int)finish_result)));

	ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
			 errmsg("could not finish exact cluster PCM grant reservation: result=%d",
					(int)finish_result),
			 errdetail("master grant was rolled back for buffer %d token=%llu",
					   buf != NULL ? buf->buf_id : -1, (unsigned long long)reservation_token)));
}

ClusterPcmOwnResult
cluster_bufmgr_pcm_own_abort_x_reservation(BufferDesc *buf, const ClusterPcmOwnSnapshot *expected,
										   uint64 reservation_token)
{
	if (expected == NULL || expected->pcm_state != (uint8)PCM_STATE_N)
		return CLUSTER_PCM_OWN_STALE;
	return cluster_pcm_own_abort_grant_reservation(buf, expected, reservation_token);
}

/*
 * W2 RED substrate — process-local marker: true while a GCS drop's
 * InvalidateBufferTry attempt is in flight, so the drop-prepin inject inside
 * InvalidateBufferTry fires ONLY for GCS drops (never for plain evictions).
 * The drop runs to completion in one call chain in the pump process, so a
 * plain static is race-free.
 */
static bool cluster_bufmgr_in_gcs_drop = false;

/*
 * cluster_pcm_own_read: read the (pcm_state, generation, flags) projection of
 * the coherent ownership tuple under the header spinlock.  These consumers do
 * not act on a reservation identity, so they intentionally omit its token.
 * Used by the cached-X writer re-verify and the drop-restore ABA check.  Any
 * *out may be NULL.
 */
static inline void
cluster_pcm_own_read(BufferDesc *buf, uint8 *out_state, uint64 *out_gen, uint32 *out_flags)
{
	uint32		buf_state;

	buf_state = LockBufHdr(buf);
	if (out_state != NULL)
		*out_state = buf->pcm_state;
	if (out_gen != NULL)
		*out_gen = cluster_pcm_own_gen_get(buf->buf_id);
	if (out_flags != NULL)
		*out_flags = cluster_pcm_own_flags_get(buf->buf_id);
	UnlockBufHdr(buf, buf_state);
}

/* One backend-local entry per held shared-buffer content LWLock.  This is
 * bounded by PG's own held-LWLock ceiling and stores the complete exact
 * handle returned by PCM-X; error cleanup never reconstructs identity from a
 * possibly changed BufferDesc ownership mirror. */
typedef enum ClusterPcmXHolderLedgerPhase
{
	PCM_X_HOLDER_LEDGER_UNUSED = 0,
	PCM_X_HOLDER_LEDGER_ACQUIRING,
	PCM_X_HOLDER_LEDGER_ACTIVE,
	PCM_X_HOLDER_LEDGER_RELEASING,
	PCM_X_HOLDER_LEDGER_DEFERRED
} ClusterPcmXHolderLedgerPhase;

typedef struct ClusterPcmXHolderLedgerEntry
{
	ClusterPcmXHolderLedgerPhase phase;
	int32		buffer_id;
	LWLock	   *content_lock;
	PcmXLocalHolderHandle handle;
} ClusterPcmXHolderLedgerEntry;

static ClusterPcmXHolderLedgerEntry
	cluster_bufmgr_pcm_x_holder_ledger[LWLOCK_MAX_HELD_BY_PROC];
static uint64 cluster_bufmgr_pcm_x_holder_identity = 0;

StaticAssertDecl(lengthof(cluster_bufmgr_pcm_x_holder_ledger) == LWLOCK_MAX_HELD_BY_PROC,
				 "PCM-X holder ledger must match the process held-LWLock bound");

/* Writer execution authority is deliberately not folded into the holder
 * ledger.  A holder describes one content-lock occupant for remote revoke;
 * a writer claim is the FIFO right that must survive from queue grant through
 * content unlock even when holder registration is retried independently. */
typedef enum ClusterPcmXWriterLedgerPhase {
	PCM_X_WRITER_LEDGER_UNUSED = 0,
	PCM_X_WRITER_LEDGER_HANDOFF,
	PCM_X_WRITER_LEDGER_ACQUIRING,
	PCM_X_WRITER_LEDGER_ACTIVE,
	PCM_X_WRITER_LEDGER_RELEASING,
	PCM_X_WRITER_LEDGER_DEFERRED
} ClusterPcmXWriterLedgerPhase;

typedef struct ClusterPcmXWriterLedgerEntry {
	ClusterPcmXWriterLedgerPhase phase;
	int32 buffer_id;
	LWLock *content_lock;
	PcmXLocalWriterClaim claim;
	ClusterPcmOwnSnapshot granted;
	bool claim_handed_off;
} ClusterPcmXWriterLedgerEntry;

static ClusterPcmXWriterLedgerEntry cluster_bufmgr_pcm_x_writer_ledger[LWLOCK_MAX_HELD_BY_PROC];

StaticAssertDecl(lengthof(cluster_bufmgr_pcm_x_writer_ledger) == LWLOCK_MAX_HELD_BY_PROC,
				 "PCM-X writer ledger must match the process held-LWLock bound");

static ClusterPcmXWriterLedgerEntry *cluster_bufmgr_pcm_x_writer_find(BufferDesc *buf);
static void cluster_bufmgr_pcm_x_holder_report_failure(PcmXQueueResult result, BufferDesc *buf,
													   const char *operation);
static bool cluster_bufmgr_pcm_x_writer_claim_entry_exact(const ClusterPcmXWriterLedgerEntry *entry,
														  BufferDesc *buf);
static bool cluster_bufmgr_pcm_x_writer_entry_exact(const ClusterPcmXWriterLedgerEntry *entry,
													BufferDesc *buf);
static void cluster_bufmgr_pcm_x_writer_report_failure(PcmXQueueResult result, BufferDesc *buf,
													   const char *operation);
static void cluster_bufmgr_pcm_direct_init_snapshot_locked(BufferDesc *buf, uint32 buf_state,
														   bool page_is_new,
														   ClusterPcmDirectInitSnapshot *out);

static void
cluster_bufmgr_pcm_x_holder_retry_wait(LWLock *content_lock, int32 buffer_id,
								   uint32 wait_index)
{
	long		delay_ms;
	PcmXQueueResult guard_result;

	/*
	 * This is a production lock-order guard.  Waiting while the page content
	 * lock is held would make RETIRE depend on the holder it is draining.
	 */
	if (content_lock == NULL || LWLockHeldByMe(content_lock))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cannot wait for a cluster PCM-X holder gate while holding content authority"),
				 errdetail("buffer=%d wait_index=%u", buffer_id, wait_index)));
	/* A different held content lock may itself be the holder that a frozen
	 * PROBE is draining.  Reuse the protocol's exact held-lock snapshot so a
	 * safe lock-coupling path can still wait, but never sleep across a closed
	 * holder barrier or a torn snapshot.
	 */
	guard_result = cluster_pcm_x_nested_wait_guard_before_block();
	if (guard_result != PCM_X_QUEUE_OK)
		cluster_bufmgr_pcm_x_holder_report_failure(
			guard_result, GetBufferDescriptor(buffer_id), "retry wait nested guard");
	delay_ms = cluster_pcm_x_holder_retry_delay_ms(wait_index);
	CHECK_FOR_INTERRUPTS();
	(void) WaitLatch(MyLatch, WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, delay_ms,
					 WAIT_EVENT_PCM_BLOCK_CONVERT_WAIT);
	CHECK_FOR_INTERRUPTS();
}

/*
 * PGRAC (t/400 L3 review R2 P0-2): a legacy grant reservation that observes
 * a live GRANT_PENDING/REVOKING lifecycle sits in a normal ms-scale revoke or
 * grant window — for a reader (or a rare un-valid-page writer) that is a
 * transient to wait out off-lock, not a client ERROR.  The retry is only a
 * fresh begin against the re-sampled complete ownership tuple; another
 * lifecycle's token/flags are never touched.  No content lock is held at
 * either call site, and a backend holding OTHER frozen-tag content locks
 * must not sleep (nested-guard discipline) — both the guard refusal and the
 * bounded-wait exhaustion fall back to the historical fail-closed report in
 * the caller.
 */
#define CLUSTER_BUFMGR_PCM_RESERVATION_MAX_WAITS 64

static ClusterPcmOwnResult
cluster_bufmgr_pcm_begin_grant_reservation_wait(BufferDesc *buf,
												ClusterPcmOwnSnapshot *base_out,
												uint64 *token_out)
{
	ClusterPcmOwnResult result;
	uint32		waits = 0;

	for (;;)
	{
		result = cluster_pcm_own_begin_grant_reservation(buf, base_out, token_out);
		if (result != CLUSTER_PCM_OWN_BUSY
			|| waits >= CLUSTER_BUFMGR_PCM_RESERVATION_MAX_WAITS)
			return result;
		if (cluster_pcm_x_nested_wait_guard_before_block() != PCM_X_QUEUE_OK)
			return result;
		cluster_pcm_x_stats_note_own_busy();
		CHECK_FOR_INTERRUPTS();
		(void) WaitLatch(MyLatch, WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 cluster_pcm_x_holder_retry_delay_ms(waits),
						 WAIT_EVENT_PCM_BLOCK_CONVERT_WAIT);
		CHECK_FOR_INTERRUPTS();
		waits++;
	}
}

static ClusterPcmXHolderLedgerEntry *
cluster_bufmgr_pcm_x_holder_find(BufferDesc *buf)
{
	int			i;

	if (buf == NULL)
		return NULL;
	for (i = 0; i < lengthof(cluster_bufmgr_pcm_x_holder_ledger); i++)
	{
		ClusterPcmXHolderLedgerEntry *entry = &cluster_bufmgr_pcm_x_holder_ledger[i];

		if (entry->phase != PCM_X_HOLDER_LEDGER_UNUSED && entry->buffer_id == buf->buf_id)
			return entry;
	}
	return NULL;
}

static ClusterPcmXHolderLedgerEntry *
cluster_bufmgr_pcm_x_holder_free_entry(void)
{
	int			i;

	for (i = 0; i < lengthof(cluster_bufmgr_pcm_x_holder_ledger); i++)
		if (cluster_bufmgr_pcm_x_holder_ledger[i].phase == PCM_X_HOLDER_LEDGER_UNUSED)
			return &cluster_bufmgr_pcm_x_holder_ledger[i];
	return NULL;
}

static bool
cluster_bufmgr_pcm_x_holder_entry_exact(const ClusterPcmXHolderLedgerEntry *entry,
										BufferDesc *buf)
{
	PcmXRuntimeSnapshot runtime;
	uint64		cluster_epoch;

	if (entry == NULL || buf == NULL)
		return false;
	if (entry->buffer_id != buf->buf_id
		|| entry->content_lock != BufferDescriptorGetContentLock(buf)
		|| entry->handle.key.buffer_id != buf->buf_id
		|| !BufferTagsEqual(&entry->handle.key.identity.tag, &buf->tag))
		return false;
	if (cluster_node_id < 0 || cluster_node_id >= PCM_X_PROTOCOL_NODE_LIMIT
		|| entry->handle.key.identity.node_id != cluster_node_id || MyProc == NULL
		|| MyProc->pgprocno < 0
		|| entry->handle.key.identity.procno != (uint32) MyProc->pgprocno
		|| entry->handle.key.identity.request_id == 0
		|| entry->handle.key.identity.wait_seq != entry->handle.key.identity.request_id
		|| entry->handle.tag_slot.slot_index == PCM_X_INVALID_SLOT_INDEX
		|| entry->handle.holder_slot.slot_index == PCM_X_INVALID_SLOT_INDEX)
		return false;
	cluster_epoch = cluster_epoch_get_current();
	runtime = cluster_pcm_x_runtime_snapshot();
	if (entry->handle.key.identity.cluster_epoch != cluster_epoch
		|| runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0)
		return false;
	return true;
}

static void
cluster_bufmgr_pcm_x_holder_report_failure(PcmXQueueResult result, BufferDesc *buf,
											const char *operation)
{
	if (result == PCM_X_QUEUE_NOT_READY || result == PCM_X_QUEUE_BUSY
		|| result == PCM_X_QUEUE_GATE_RETRY || result == PCM_X_QUEUE_BARRIER_CLOSED
		|| result == PCM_X_QUEUE_NO_CAPACITY)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("cluster PCM-X holder operation is not ready"),
				 errdetail("operation=%s buffer=%d result=%d", operation,
						   buf != NULL ? buf->buf_id : -1, (int) result)));

	ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
			 errmsg("cluster PCM-X holder operation failed"),
			 errdetail("operation=%s buffer=%d result=%d", operation,
					   buf != NULL ? buf->buf_id : -1, (int) result)));
}

static void
cluster_bufmgr_pcm_x_holder_clear(ClusterPcmXHolderLedgerEntry *entry)
{
	if (entry != NULL)
		MemSet(entry, 0, sizeof(*entry));
}

static void
cluster_bufmgr_pcm_x_holder_defer_fail_closed(ClusterPcmXHolderLedgerEntry *entry)
{
	if (entry != NULL)
		entry->phase = PCM_X_HOLDER_LEDGER_DEFERRED;
	cluster_pcm_x_runtime_fail_closed();
}

/* Error and transaction-tail cleanup never enters explicit WaitLatch/backoff,
 * CHECK_FOR_INTERRUPTS, or ereport.  Exact detach may acquire its internal
 * LWLocks once; that API converts a lock-acquire ERROR to CORRUPT.  A retryable
 * gate leaves the complete backend-local handle in DEFERRED for a later safe
 * LockBuffer entrance. */
static void
cluster_bufmgr_pcm_x_holder_drain_deferred_nowait(void)
{
	int			i;

	for (i = 0; i < lengthof(cluster_bufmgr_pcm_x_holder_ledger); i++)
	{
		ClusterPcmXHolderLedgerEntry *entry = &cluster_bufmgr_pcm_x_holder_ledger[i];
		PcmXQueueResult result;

		if (entry->phase != PCM_X_HOLDER_LEDGER_DEFERRED)
			continue;
		if (entry->content_lock == NULL)
		{
			cluster_bufmgr_pcm_x_holder_defer_fail_closed(entry);
			elog(LOG, "could not drain deferred cluster PCM-X holder with no content lock: buffer=%d",
				 entry->buffer_id);
			continue;
		}
		if (LWLockHeldByMe(entry->content_lock))
		{
			elog(LOG, "could not drain deferred cluster PCM-X holder while content lock is held: buffer=%d",
				 entry->buffer_id);
			continue;
		}
		result = cluster_pcm_x_local_holder_exceptional_detach_exact(&entry->handle,
														  entry->content_lock);
		if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_NOT_FOUND)
			cluster_bufmgr_pcm_x_holder_clear(entry);
		else if (result != PCM_X_QUEUE_GATE_RETRY && result != PCM_X_QUEUE_BUSY)
		{
			cluster_bufmgr_pcm_x_holder_defer_fail_closed(entry);
			elog(LOG, "could not drain deferred exact cluster PCM-X holder: buffer=%d result=%d",
				 entry->buffer_id, (int) result);
		}
	}
}

/* Same-buffer reuse cannot proceed around old RELEASING evidence.  Each wait
 * batch is bounded (2+4+8+16+32ms) but healthy registration may start the
 * next batch; CHECK_FOR_INTERRUPTS remains the user-visible cancellation
 * boundary. */
static void
cluster_bufmgr_pcm_x_holder_drain_deferred(ClusterPcmXHolderLedgerEntry *entry)
{
	PcmXRuntimeSnapshot runtime;
	uint32		wait_index = 0;

	if (entry == NULL || entry->phase != PCM_X_HOLDER_LEDGER_DEFERRED)
		return;
	for (;;)
	{
		PcmXQueueResult result;
		ClusterPcmXHolderRetryAction action;

		if (entry->content_lock == NULL || LWLockHeldByMe(entry->content_lock))
		{
			cluster_bufmgr_pcm_x_holder_defer_fail_closed(entry);
			cluster_bufmgr_pcm_x_holder_report_failure(PCM_X_QUEUE_BAD_STATE,
											 GetBufferDescriptor(entry->buffer_id),
											 "deferred detach lock order");
		}
		result = cluster_pcm_x_local_holder_exceptional_detach_exact(&entry->handle,
														  entry->content_lock);
		action = cluster_pcm_x_holder_unregister_retry_action(result, 0);
		if (action == CLUSTER_PCM_X_HOLDER_RETRY_COMPLETE)
		{
			cluster_bufmgr_pcm_x_holder_clear(entry);
			return;
		}
		if (action != CLUSTER_PCM_X_HOLDER_RETRY_WAIT)
		{
			cluster_bufmgr_pcm_x_holder_defer_fail_closed(entry);
			cluster_bufmgr_pcm_x_holder_report_failure(result,
											 GetBufferDescriptor(entry->buffer_id),
											 "deferred detach");
		}
		cluster_bufmgr_pcm_x_holder_retry_wait(
			entry->content_lock, entry->buffer_id,
			wait_index % CLUSTER_PCM_X_HOLDER_RETRY_BATCH_WAITS);
		wait_index++;
		if (wait_index % CLUSTER_PCM_X_HOLDER_RETRY_BATCH_WAITS == 0)
		{
			runtime = cluster_pcm_x_runtime_snapshot();
			if (runtime.state != PCM_X_RUNTIME_ACTIVE
				|| runtime.master_session_incarnation == 0)
			{
				cluster_bufmgr_pcm_x_holder_defer_fail_closed(entry);
				cluster_bufmgr_pcm_x_holder_report_failure(
					PCM_X_QUEUE_NOT_READY, GetBufferDescriptor(entry->buffer_id),
					"deferred detach runtime");
			}
		}
	}
}

static ClusterPcmXHolderLedgerEntry *
cluster_bufmgr_pcm_x_holder_prepare(BufferDesc *buf)
{
	ClusterPcmXHolderLedgerEntry *entry;
	ClusterPcmXWriterLedgerEntry *writer_entry;
	PcmXRuntimeSnapshot runtime;
	PcmXLocalHolderKey key;
	PcmXLocalHolderHandle handle;
	PcmXQueueResult result;
	TransactionId xid;
	uint64 cluster_epoch;
	uint64 committed_own_generation = 0;
	uint64 identity;
	uint64 own_generation;
	LWLock *content_lock;
	uint32 wait_index = 0;
	bool writer_authorized = false;

	cluster_bufmgr_pcm_x_holder_drain_deferred_nowait();
	entry = cluster_bufmgr_pcm_x_holder_find(buf);
	if (entry != NULL && entry->phase == PCM_X_HOLDER_LEDGER_DEFERRED)
	{
		cluster_bufmgr_pcm_x_holder_drain_deferred(entry);
		entry = NULL;
	}
	if (entry != NULL)
	{
		if (entry->phase != PCM_X_HOLDER_LEDGER_ACQUIRING
			|| !cluster_bufmgr_pcm_x_holder_entry_exact(entry, buf))
		{
			cluster_bufmgr_pcm_x_holder_defer_fail_closed(entry);
			cluster_bufmgr_pcm_x_holder_report_failure(PCM_X_QUEUE_BAD_STATE, buf,
											 "reuse existing holder ledger");
		}
		if (LWLockHeldByMe(entry->content_lock))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_IN_USE),
					 errmsg("cannot register a cluster PCM-X holder for a pre-held content lock"),
					 errdetail("buffer=%d", buf->buf_id)));
		return entry;
	}
	if (!cluster_bufmgr_should_pcm_track(buf))
		return NULL;
	writer_entry = cluster_bufmgr_pcm_x_writer_find(buf);
	if (writer_entry != NULL) {
		if (writer_entry->phase != PCM_X_WRITER_LEDGER_ACQUIRING || !writer_entry->claim_handed_off
			|| !cluster_bufmgr_pcm_x_writer_entry_exact(writer_entry, buf))
			cluster_bufmgr_pcm_x_writer_report_failure(PCM_X_QUEUE_BAD_STATE, buf,
													   "holder writer authority");
		writer_authorized = true;
	}

	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0) {
		/*
		 * A queued writer already owns exact FIFO execution authority.  It
		 * may never cross this runtime transition without the matching holder
		 * lane; ERROR unwinds through writer_abort_acquiring and completes
		 * the turn.
		 */
		if (writer_authorized)
			cluster_bufmgr_pcm_x_writer_report_failure(PCM_X_QUEUE_NOT_READY, buf,
													   "holder runtime");
		return NULL;
	}
	content_lock = BufferDescriptorGetContentLock(buf);
	if (LWLockHeldByMe(content_lock))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("cannot register a cluster PCM-X holder for a pre-held content lock"),
				 errdetail("buffer=%d", buf->buf_id)));
	entry = cluster_bufmgr_pcm_x_holder_free_entry();
	if (entry == NULL)
	{
		int			i;

		/*
		 * Deferred exact handles do not consume the ledger forever.  If every
		 * slot is occupied, wait for one safe detach before declaring a real
		 * held-LWLock bound violation. */
		for (i = 0; i < lengthof(cluster_bufmgr_pcm_x_holder_ledger); i++)
		{
			ClusterPcmXHolderLedgerEntry *candidate
				= &cluster_bufmgr_pcm_x_holder_ledger[i];

			if (candidate->phase != PCM_X_HOLDER_LEDGER_DEFERRED)
				continue;
			cluster_bufmgr_pcm_x_holder_drain_deferred(candidate);
			entry = candidate;
			break;
		}
	}
	if (entry == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cluster PCM-X holder ledger is full"),
				 errdetail("maximum entries=%d", LWLOCK_MAX_HELD_BY_PROC)));

	cluster_pcm_own_read(buf, NULL, &own_generation, NULL);
	cluster_epoch = cluster_epoch_get_current();
	if (cluster_node_id < 0 || cluster_node_id >= PCM_X_PROTOCOL_NODE_LIMIT || MyProc == NULL
		|| MyProc->pgprocno < 0)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster PCM-X holder identity is unavailable"),
						errdetail("buffer=%d node=%d procno=%d epoch=%llu", buf->buf_id,
								  cluster_node_id, MyProc != NULL ? MyProc->pgprocno : -1,
								  (unsigned long long)cluster_epoch)));
	if (!writer_authorized && cluster_bufmgr_pcm_x_holder_identity == UINT64_MAX)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						errmsg("cluster PCM-X holder identity exhausted"),
						errdetail("buffer=%d", buf->buf_id)));
	MemSet(&key, 0, sizeof(key));
	if (writer_authorized) {
		key.identity = writer_entry->claim.writer.identity;
		key.identity.base_own_generation = writer_entry->granted.generation;
	} else {
		identity = ++cluster_bufmgr_pcm_x_holder_identity;
		xid = GetTopTransactionIdIfAny();
		key.identity.tag = buf->tag;
		key.identity.node_id = cluster_node_id;
		key.identity.procno = (uint32)MyProc->pgprocno;
		key.identity.xid = xid;
		key.identity.cluster_epoch = cluster_epoch;
		key.identity.request_id = identity;
		key.identity.wait_seq = identity;
		key.identity.base_own_generation = own_generation;
	}
	key.buffer_id = buf->buf_id;
	for (;;)
	{
		ClusterPcmXHolderRetryAction action;
		PcmXRuntimeSnapshot current_runtime;

		if (writer_authorized)
			result = cluster_pcm_x_local_writer_holder_register_exact(
				&key, &writer_entry->claim, &handle, &committed_own_generation);
		else
			result = cluster_pcm_x_local_holder_register(&key, &handle);
		current_runtime = cluster_pcm_x_runtime_snapshot();
		action = cluster_pcm_x_holder_register_retry_action(
			result, current_runtime.state == PCM_X_RUNTIME_ACTIVE
						&& current_runtime.master_session_incarnation != 0);
		if (action == CLUSTER_PCM_X_HOLDER_RETRY_COMPLETE) {
			/*
			 * Publish the exact handle before any validation that can ERROR.
			 * From this point UnlockBuffers/owner-exit owns the registration
			 * and can detach it even if the committed-generation proof is
			 * corrupt.
			 */
			entry->buffer_id = buf->buf_id;
			entry->content_lock = content_lock;
			entry->handle = handle;
			entry->phase = PCM_X_HOLDER_LEDGER_ACQUIRING;
			if (writer_authorized && committed_own_generation != writer_entry->granted.generation)
				cluster_bufmgr_pcm_x_writer_report_failure(PCM_X_QUEUE_CORRUPT, buf,
														   "holder generation");
			break;
		}
		if (action != CLUSTER_PCM_X_HOLDER_RETRY_WAIT)
			cluster_bufmgr_pcm_x_holder_report_failure(result, buf, "register");
		cluster_bufmgr_pcm_x_holder_retry_wait(
			content_lock, buf->buf_id,
			wait_index++ % CLUSTER_PCM_X_HOLDER_RETRY_BATCH_WAITS);
	}

	return entry;
}

static void
cluster_bufmgr_pcm_x_holder_activate(ClusterPcmXHolderLedgerEntry *entry)
{
	PcmXQueueResult result;

	if (entry == NULL)
		return;
	if (entry->phase != PCM_X_HOLDER_LEDGER_ACQUIRING)
		cluster_bufmgr_pcm_x_holder_report_failure(PCM_X_QUEUE_BAD_STATE,
											 GetBufferDescriptor(entry->buffer_id), "activate phase");
	result = cluster_pcm_x_local_holder_activate_exact(&entry->handle);
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
		cluster_bufmgr_pcm_x_holder_report_failure(result,
										 GetBufferDescriptor(entry->buffer_id), "activate");
	entry->phase = PCM_X_HOLDER_LEDGER_ACTIVE;
}

static void
cluster_bufmgr_pcm_x_holder_mark_releasing(ClusterPcmXHolderLedgerEntry *entry)
{
	PcmXQueueResult result;

	if (entry == NULL)
		return;
	if (entry->phase != PCM_X_HOLDER_LEDGER_ACTIVE)
		cluster_bufmgr_pcm_x_holder_report_failure(PCM_X_QUEUE_BAD_STATE,
											 GetBufferDescriptor(entry->buffer_id),
											 "mark releasing phase");
	result = cluster_pcm_x_local_holder_mark_releasing_exact(&entry->handle);
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
		cluster_bufmgr_pcm_x_holder_report_failure(result,
										 GetBufferDescriptor(entry->buffer_id), "mark releasing");
	entry->phase = PCM_X_HOLDER_LEDGER_RELEASING;
}

static void
cluster_bufmgr_pcm_x_holder_unregister(ClusterPcmXHolderLedgerEntry *entry)
{
	PcmXQueueResult result;
	uint32		waits_used = 0;

	if (entry == NULL)
		return;
	if (entry->phase != PCM_X_HOLDER_LEDGER_RELEASING)
		cluster_bufmgr_pcm_x_holder_report_failure(PCM_X_QUEUE_BAD_STATE,
											 GetBufferDescriptor(entry->buffer_id),
											 "unregister phase");
	if (entry->content_lock == NULL || LWLockHeldByMe(entry->content_lock))
		cluster_bufmgr_pcm_x_holder_report_failure(PCM_X_QUEUE_BAD_STATE,
											 GetBufferDescriptor(entry->buffer_id),
											 "unregister lock order");
	for (;;)
	{
		ClusterPcmXHolderRetryAction action;

		result = cluster_pcm_x_local_holder_unregister_exact(&entry->handle);
		action = cluster_pcm_x_holder_unregister_retry_action(result, waits_used);
		if (action == CLUSTER_PCM_X_HOLDER_RETRY_COMPLETE)
		{
			cluster_bufmgr_pcm_x_holder_clear(entry);
			return;
		}
		if (action == CLUSTER_PCM_X_HOLDER_RETRY_DEFER)
		{
			entry->phase = PCM_X_HOLDER_LEDGER_DEFERRED;
			return;
		}
		if (action != CLUSTER_PCM_X_HOLDER_RETRY_WAIT)
		{
			cluster_bufmgr_pcm_x_holder_defer_fail_closed(entry);
			cluster_bufmgr_pcm_x_holder_report_failure(
				result, GetBufferDescriptor(entry->buffer_id), "unregister");
		}
		cluster_bufmgr_pcm_x_holder_retry_wait(entry->content_lock, entry->buffer_id,
											 waits_used++);
	}
}

static void
cluster_bufmgr_pcm_x_holder_abort_acquiring(ClusterPcmXHolderLedgerEntry *entry)
{
	PcmXQueueResult result;

	if (entry == NULL)
		return;
	if (entry->content_lock == NULL)
	{
		cluster_bufmgr_pcm_x_holder_defer_fail_closed(entry);
		elog(LOG, "could not abort cluster PCM-X ACQUIRING holder with no content lock: buffer=%d",
			 entry->buffer_id);
		return;
	}
	if (LWLockHeldByMe(entry->content_lock))
		return;
	result = cluster_pcm_x_local_holder_abort_acquiring_exact(&entry->handle);
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_NOT_FOUND)
		cluster_bufmgr_pcm_x_holder_clear(entry);
	else if (result == PCM_X_QUEUE_GATE_RETRY || result == PCM_X_QUEUE_BUSY)
		entry->phase = PCM_X_HOLDER_LEDGER_DEFERRED;
	else
	{
		cluster_bufmgr_pcm_x_holder_defer_fail_closed(entry);
		elog(LOG, "could not abort exact cluster PCM-X ACQUIRING holder: buffer=%d result=%d",
			 entry->buffer_id, (int) result);
	}
}

static void
cluster_bufmgr_pcm_x_holder_exception_cleanup_all(void)
{
	int			i;

	for (i = 0; i < lengthof(cluster_bufmgr_pcm_x_holder_ledger); i++)
	{
		ClusterPcmXHolderLedgerEntry *entry = &cluster_bufmgr_pcm_x_holder_ledger[i];
		PcmXQueueResult result;

		if (entry->phase == PCM_X_HOLDER_LEDGER_UNUSED)
			continue;
		if (entry->content_lock == NULL)
		{
			cluster_bufmgr_pcm_x_holder_defer_fail_closed(entry);
			elog(LOG, "could not detach cluster PCM-X holder after error with no content lock: buffer=%d",
				 entry->buffer_id);
			continue;
		}
		if (LWLockHeldByMe(entry->content_lock))
			continue;
		result = cluster_pcm_x_local_holder_exceptional_detach_exact(&entry->handle,
														  entry->content_lock);
		if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_NOT_FOUND)
			cluster_bufmgr_pcm_x_holder_clear(entry);
		else if (result == PCM_X_QUEUE_GATE_RETRY || result == PCM_X_QUEUE_BUSY)
			entry->phase = PCM_X_HOLDER_LEDGER_DEFERRED;
		else
		{
			cluster_bufmgr_pcm_x_holder_defer_fail_closed(entry);
			elog(LOG, "could not detach exact cluster PCM-X holder after error: buffer=%d result=%d",
				 entry->buffer_id, (int) result);
		}
	}
}

static void
cluster_bufmgr_pcm_x_holder_reset(void)
{
	MemSet(cluster_bufmgr_pcm_x_holder_ledger, 0,
		   sizeof(cluster_bufmgr_pcm_x_holder_ledger));
	cluster_bufmgr_pcm_x_holder_identity = 0;
}

static ClusterPcmXWriterLedgerEntry *
cluster_bufmgr_pcm_x_writer_find(BufferDesc *buf)
{
	int i;

	if (buf == NULL)
		return NULL;
	for (i = 0; i < lengthof(cluster_bufmgr_pcm_x_writer_ledger); i++) {
		ClusterPcmXWriterLedgerEntry *entry = &cluster_bufmgr_pcm_x_writer_ledger[i];

		if (entry->phase != PCM_X_WRITER_LEDGER_UNUSED && entry->buffer_id == buf->buf_id)
			return entry;
	}
	return NULL;
}

static ClusterPcmXWriterLedgerEntry *
cluster_bufmgr_pcm_x_writer_free_entry(void)
{
	int i;

	for (i = 0; i < lengthof(cluster_bufmgr_pcm_x_writer_ledger); i++)
		if (cluster_bufmgr_pcm_x_writer_ledger[i].phase == PCM_X_WRITER_LEDGER_UNUSED)
			return &cluster_bufmgr_pcm_x_writer_ledger[i];
	return NULL;
}

static bool
cluster_bufmgr_pcm_x_writer_claim_entry_exact(const ClusterPcmXWriterLedgerEntry *entry,
											  BufferDesc *buf)
{
	return entry != NULL && buf != NULL && entry->buffer_id == buf->buf_id
		   && entry->content_lock == BufferDescriptorGetContentLock(buf) && entry->claim.flags == 0
		   && entry->claim.writer.flags == 0
		   && entry->claim.active_slot.slot_index == entry->claim.writer.membership_slot.slot_index
		   && entry->claim.active_slot.slot_generation
				  == entry->claim.writer.membership_slot.slot_generation
		   && entry->claim.active_slot.slot_index != PCM_X_INVALID_SLOT_INDEX
		   && entry->claim.active_slot.slot_generation != 0 && entry->claim.claim_generation != 0
		   && entry->claim.local_round != 0
		   && entry->claim.local_round == entry->claim.writer.local_round
		   && entry->claim.role == entry->claim.writer.role
		   && (entry->claim.role == PCM_X_LOCAL_ROLE_NODE_LEADER
			   || entry->claim.role == PCM_X_LOCAL_ROLE_FOLLOWER);
}

static bool
cluster_bufmgr_pcm_x_writer_entry_exact(const ClusterPcmXWriterLedgerEntry *entry, BufferDesc *buf)
{
	return cluster_bufmgr_pcm_x_writer_claim_entry_exact(entry, buf)
		   && BufferTagsEqual(&entry->claim.writer.identity.tag, &buf->tag)
		   && cluster_pcm_x_writer_grant_snapshot_exact(&entry->claim, &entry->granted,
														&entry->granted);
}

static void
cluster_bufmgr_pcm_x_writer_report_failure(PcmXQueueResult result, BufferDesc *buf,
										   const char *operation)
{
	BufferTag	guard_tag;
	int			guard_result = 0;
	char		guard_detail[96];

	/* Name the exact requester escape arm and, for a nested-guard refusal,
	 * the held content-lock tag that closed the guard. */
	guard_detail[0] = '\0';
	if (cluster_pcm_x_nested_wait_guard_last_block(&guard_tag, &guard_result))
		snprintf(guard_detail, sizeof(guard_detail), " guard_result=%d guard_rel=%u guard_blk=%u",
				 guard_result, guard_tag.relNumber, guard_tag.blockNum);
	if (result == PCM_X_QUEUE_NOT_READY || result == PCM_X_QUEUE_BUSY
		|| result == PCM_X_QUEUE_GATE_RETRY || result == PCM_X_QUEUE_BARRIER_CLOSED
		|| result == PCM_X_QUEUE_NO_CAPACITY)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE),
						errmsg("cluster PCM-X writer operation is not ready"),
						errdetail("operation=%s buffer=%d result=%d fail_line=%d%s", operation,
								  buf != NULL ? buf->buf_id : -1, (int)result,
								  cluster_gcs_pcm_x_requester_last_fail_line(), guard_detail)));

	ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED), errmsg("cluster PCM-X writer operation failed"),
			 errdetail("operation=%s buffer=%d result=%d fail_line=%d%s", operation,
					   buf != NULL ? buf->buf_id : -1, (int)result,
					   cluster_gcs_pcm_x_requester_last_fail_line(), guard_detail)));
}

static void
cluster_bufmgr_pcm_x_writer_clear(ClusterPcmXWriterLedgerEntry *entry)
{
	if (entry != NULL)
		MemSet(entry, 0, sizeof(*entry));
}

static void cluster_bufmgr_pcm_x_writer_release(ClusterPcmXWriterLedgerEntry *entry);

static void
cluster_bufmgr_pcm_x_writer_drain_deferred_nowait(void)
{
	int i;

	for (i = 0; i < lengthof(cluster_bufmgr_pcm_x_writer_ledger); i++) {
		ClusterPcmXWriterLedgerEntry *entry = &cluster_bufmgr_pcm_x_writer_ledger[i];
		PcmXQueueResult result;

		if (entry->phase != PCM_X_WRITER_LEDGER_DEFERRED)
			continue;
		if (entry->content_lock == NULL || LWLockHeldByMe(entry->content_lock)) {
			cluster_pcm_x_runtime_fail_closed();
			elog(LOG,
				 "could not drain deferred cluster PCM-X writer with content authority: buffer=%d",
				 entry->buffer_id);
			continue;
		}
		result = cluster_gcs_pcm_x_writer_claim_cleanup_and_wake_noexcept(&entry->claim);
		if (result == PCM_X_QUEUE_OK)
			cluster_bufmgr_pcm_x_writer_clear(entry);
		else if (result != PCM_X_QUEUE_GATE_RETRY && result != PCM_X_QUEUE_BUSY) {
			cluster_pcm_x_runtime_fail_closed();
			elog(LOG, "could not drain deferred exact cluster PCM-X writer: buffer=%d result=%d",
				 entry->buffer_id, (int)result);
		}
	}
}

static ClusterPcmXWriterLedgerEntry *
cluster_bufmgr_pcm_x_writer_prepare(BufferDesc *buf, PcmLockMode mode, bool *barrier_refused)
{
	ClusterPcmXWriterLedgerEntry *entry;
	ClusterPcmDirectInitSnapshot observed;
	ClusterPcmOwnSnapshot granted;
	ClusterPcmOwnResult own_result;
	PcmXQueueResult release_result;
	PcmXQueueResult result;
	LWLock *content_lock;
	uint32 buf_state;

	if (mode != PCM_LOCK_MODE_X || buf == NULL || !cluster_bufmgr_should_pcm_track(buf))
		return NULL;
	content_lock = BufferDescriptorGetContentLock(buf);
	if (LWLockHeldByMe(content_lock))
		cluster_bufmgr_pcm_x_writer_report_failure(PCM_X_QUEUE_BAD_STATE, buf,
												   "prepare lock order");

	cluster_bufmgr_pcm_x_writer_drain_deferred_nowait();
	entry = cluster_bufmgr_pcm_x_writer_find(buf);
	if (entry != NULL && entry->phase == PCM_X_WRITER_LEDGER_DEFERRED) {
		cluster_bufmgr_pcm_x_writer_release(entry);
		entry = NULL;
	}
	if (entry != NULL)
		cluster_bufmgr_pcm_x_writer_report_failure(PCM_X_QUEUE_BAD_STATE, buf,
												   "reuse writer ledger");
	entry = cluster_bufmgr_pcm_x_writer_free_entry();
	if (entry == NULL)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						errmsg("cluster PCM-X writer ledger is full"),
						errdetail("maximum entries=%d", LWLOCK_MAX_HELD_BY_PROC)));
	entry->buffer_id = buf->buf_id;
	entry->content_lock = content_lock;
	entry->phase = PCM_X_WRITER_LEDGER_HANDOFF;
	entry->claim_handed_off = false;

	buf_state = LockBufHdr(buf);
	cluster_bufmgr_pcm_direct_init_snapshot_locked(buf, buf_state, false, &observed);
	UnlockBufHdr(buf, buf_state);
	if ((observed.buf_state & BM_VALID) == 0) {
		cluster_bufmgr_pcm_x_writer_clear(entry);
		return NULL;
	}
	MemSet(&entry->claim, 0, sizeof(entry->claim));
	entry->claim_handed_off = false;
	pg_write_barrier();
	result = cluster_gcs_pcm_x_acquire_writer(buf, &entry->claim, &entry->claim_handed_off);
	if (result != PCM_X_QUEUE_OK) {
		if (entry->claim_handed_off) {
			cluster_pcm_x_runtime_fail_closed();
			cluster_bufmgr_pcm_x_writer_report_failure(PCM_X_QUEUE_CORRUPT, buf,
													   "failed claim handoff");
		}
		cluster_bufmgr_pcm_x_writer_clear(entry);

		/*
		 * PGRAC (t/400 L3 item 3): a nested-guard BARRIER_CLOSED means this
		 * backend already holds another content lock whose tag sits under a
		 * frozen revoke barrier; the requester unwound cleanly (no claim, no
		 * wait identity).  A barrier-aware caller owns the unwind: hand the
		 * refusal back instead of escalating to a client ERROR.  bufmgr must
		 * never release the foreign content lock itself.
		 */
		if (result == PCM_X_QUEUE_BARRIER_CLOSED && barrier_refused != NULL)
		{
			*barrier_refused = true;
			cluster_pcm_x_stats_note_barrier_unwind();
			return NULL;
		}
		cluster_bufmgr_pcm_x_writer_report_failure(result, buf, "queue acquire");
	}
	if (!entry->claim_handed_off) {
		cluster_pcm_x_runtime_fail_closed();
		cluster_bufmgr_pcm_x_writer_report_failure(PCM_X_QUEUE_CORRUPT, buf, "claim handoff");
	}

	own_result = cluster_bufmgr_pcm_own_snapshot(buf, &granted);
	if (own_result != CLUSTER_PCM_OWN_OK
		|| !cluster_pcm_x_writer_grant_snapshot_exact(&entry->claim, &granted, &granted)) {
		release_result = cluster_gcs_pcm_x_writer_claim_cleanup_and_wake_noexcept(&entry->claim);
		if (release_result == PCM_X_QUEUE_OK)
			cluster_bufmgr_pcm_x_writer_clear(entry);
		else {
			/* Keep the exact claim reachable for owner-exit/deferred retry. */
			entry->phase = PCM_X_WRITER_LEDGER_DEFERRED;
			cluster_pcm_x_runtime_fail_closed();
			cluster_bufmgr_pcm_x_writer_report_failure(release_result, buf,
													   "grant snapshot cleanup");
		}
		cluster_pcm_x_runtime_fail_closed();
		cluster_bufmgr_pcm_x_writer_report_failure(PCM_X_QUEUE_CORRUPT, buf, "grant snapshot");
	}

	entry->granted = granted;
	entry->phase = PCM_X_WRITER_LEDGER_ACQUIRING;
	return entry;
}

static void
cluster_bufmgr_pcm_x_writer_activate(ClusterPcmXWriterLedgerEntry *entry)
{
	ClusterPcmOwnSnapshot live;
	ClusterPcmOwnResult own_result;
	BufferDesc *buf;

	if (entry == NULL)
		return;
	buf = GetBufferDescriptor(entry->buffer_id);
	if (entry->phase != PCM_X_WRITER_LEDGER_ACQUIRING
		|| !cluster_bufmgr_pcm_x_writer_entry_exact(entry, buf) || entry->content_lock == NULL
		|| !LWLockHeldByMe(entry->content_lock))
		cluster_bufmgr_pcm_x_writer_report_failure(PCM_X_QUEUE_BAD_STATE, buf, "activate phase");
	own_result = cluster_bufmgr_pcm_own_snapshot(buf, &live);
	if (own_result != CLUSTER_PCM_OWN_OK
		|| !cluster_pcm_x_writer_grant_snapshot_exact(&entry->claim, &entry->granted, &live)) {
		cluster_pcm_x_runtime_fail_closed();
		cluster_bufmgr_pcm_x_writer_report_failure(PCM_X_QUEUE_CORRUPT, buf, "activate ownership");
	}
	entry->phase = PCM_X_WRITER_LEDGER_ACTIVE;
}

static void
cluster_bufmgr_pcm_x_writer_mark_releasing(ClusterPcmXWriterLedgerEntry *entry)
{
	BufferDesc *buf;

	if (entry == NULL)
		return;
	buf = GetBufferDescriptor(entry->buffer_id);
	if (entry->phase != PCM_X_WRITER_LEDGER_ACTIVE
		|| !cluster_bufmgr_pcm_x_writer_entry_exact(entry, buf) || entry->content_lock == NULL
		|| !LWLockHeldByMe(entry->content_lock))
		cluster_bufmgr_pcm_x_writer_report_failure(PCM_X_QUEUE_BAD_STATE, buf, "mark releasing");
	entry->phase = PCM_X_WRITER_LEDGER_RELEASING;
}

static void
cluster_bufmgr_pcm_x_writer_release(ClusterPcmXWriterLedgerEntry *entry)
{
	BufferDesc *buf;
	uint32 waits_used = 0;

	if (entry == NULL)
		return;
	buf = GetBufferDescriptor(entry->buffer_id);
	if ((entry->phase != PCM_X_WRITER_LEDGER_RELEASING
		 && entry->phase != PCM_X_WRITER_LEDGER_DEFERRED)
		|| !cluster_bufmgr_pcm_x_writer_claim_entry_exact(entry, buf) || entry->content_lock == NULL
		|| LWLockHeldByMe(entry->content_lock))
		cluster_bufmgr_pcm_x_writer_report_failure(PCM_X_QUEUE_BAD_STATE, buf, "release phase");
	for (;;) {
		ClusterPcmXWriterRetryAction action;
		PcmXQueueResult result;

		result = cluster_gcs_pcm_x_writer_claim_release_and_wake_exact(&entry->claim);
		action = cluster_pcm_x_writer_release_retry_action(result, waits_used);
		if (action == CLUSTER_PCM_X_WRITER_RETRY_COMPLETE) {
			cluster_bufmgr_pcm_x_writer_clear(entry);
			return;
		}
		if (action == CLUSTER_PCM_X_WRITER_RETRY_FAIL) {
			entry->phase = PCM_X_WRITER_LEDGER_DEFERRED;
			cluster_pcm_x_runtime_fail_closed();
			cluster_bufmgr_pcm_x_writer_report_failure(result, buf, "release");
		}
		if (action == CLUSTER_PCM_X_WRITER_RETRY_DEFER) {
			PcmXRuntimeSnapshot runtime = cluster_pcm_x_runtime_snapshot();

			if (runtime.state != PCM_X_RUNTIME_ACTIVE || runtime.master_session_incarnation == 0) {
				entry->phase = PCM_X_WRITER_LEDGER_DEFERRED;
				cluster_pcm_x_runtime_fail_closed();
				cluster_bufmgr_pcm_x_writer_report_failure(PCM_X_QUEUE_NOT_READY, buf,
														   "release runtime");
			}
			waits_used = 0;
		}
		cluster_bufmgr_pcm_x_holder_retry_wait(entry->content_lock, entry->buffer_id,
											   waits_used++
												   % CLUSTER_PCM_X_HOLDER_RETRY_BATCH_WAITS);
	}
}

static void
cluster_bufmgr_pcm_x_writer_abort_acquiring(ClusterPcmXWriterLedgerEntry *entry)
{
	PcmXQueueResult result;

	if (entry == NULL || entry->phase != PCM_X_WRITER_LEDGER_ACQUIRING)
		return;
	if (entry->content_lock == NULL || LWLockHeldByMe(entry->content_lock))
		return;
	result = cluster_gcs_pcm_x_writer_claim_cleanup_and_wake_noexcept(&entry->claim);
	if (result == PCM_X_QUEUE_OK)
		cluster_bufmgr_pcm_x_writer_clear(entry);
	else if (result == PCM_X_QUEUE_GATE_RETRY || result == PCM_X_QUEUE_BUSY)
		entry->phase = PCM_X_WRITER_LEDGER_DEFERRED;
	else {
		entry->phase = PCM_X_WRITER_LEDGER_DEFERRED;
		cluster_pcm_x_runtime_fail_closed();
		elog(LOG, "could not abort exact cluster PCM-X ACQUIRING writer: buffer=%d result=%d",
			 entry->buffer_id, (int)result);
	}
}

static void
cluster_bufmgr_pcm_x_writer_exception_cleanup_all(void)
{
	int i;

	for (i = 0; i < lengthof(cluster_bufmgr_pcm_x_writer_ledger); i++) {
		ClusterPcmXWriterLedgerEntry *entry = &cluster_bufmgr_pcm_x_writer_ledger[i];
		PcmXQueueResult result;

		if (entry->phase == PCM_X_WRITER_LEDGER_UNUSED)
			continue;
		if (entry->phase == PCM_X_WRITER_LEDGER_HANDOFF && !entry->claim_handed_off) {
			/* requester cleanup still owns any in-flight claim */
			cluster_bufmgr_pcm_x_writer_clear(entry);
			continue;
		}
		if (entry->content_lock == NULL || LWLockHeldByMe(entry->content_lock))
			continue;
		result = cluster_gcs_pcm_x_writer_claim_cleanup_and_wake_noexcept(&entry->claim);
		if (result == PCM_X_QUEUE_OK)
			cluster_bufmgr_pcm_x_writer_clear(entry);
		else if (result == PCM_X_QUEUE_GATE_RETRY || result == PCM_X_QUEUE_BUSY)
			entry->phase = PCM_X_WRITER_LEDGER_DEFERRED;
		else {
			entry->phase = PCM_X_WRITER_LEDGER_DEFERRED;
			cluster_pcm_x_runtime_fail_closed();
			elog(LOG,
				 "could not release exact cluster PCM-X writer after error: buffer=%d result=%d",
				 entry->buffer_id, (int)result);
		}
	}
}

/* on_shmem_exit has no later backend entrance that could consume a DEFERRED
 * ledger.  Once LWLockReleaseAll has dropped content authority, retry both
 * queue lanes to an exact shared terminal while the same runtime remains
 * active.  No CHECK_FOR_INTERRUPTS, ereport, or latch wait is legal here. */
static bool
cluster_bufmgr_pcm_x_writer_owner_exit_drain_once(bool runtime_active)
{
	bool retry = false;
	int i;

	for (i = 0; i < lengthof(cluster_bufmgr_pcm_x_writer_ledger); i++) {
		ClusterPcmXWriterLedgerEntry *entry = &cluster_bufmgr_pcm_x_writer_ledger[i];
		ClusterPcmXOwnerExitAction action;
		PcmXQueueResult result;
		BufferDesc *buf;

		if (entry->phase == PCM_X_WRITER_LEDGER_UNUSED)
			continue;
		if (entry->phase == PCM_X_WRITER_LEDGER_HANDOFF && !entry->claim_handed_off) {
			/*
			 * The before_shmem requester callback remained the sole owner.
			 * It either completed exact cleanup or preserved shared evidence
			 * behind RECOVERY_BLOCKED; guessing a second release here would
			 * be unsafe.
			 */
			continue;
		}
		if (entry->buffer_id < 0 || entry->buffer_id >= NBuffers) {
			cluster_pcm_x_runtime_fail_closed();
			elog(LOG,
				 "could not owner-exit drain cluster PCM-X writer with invalid buffer: buffer=%d",
				 entry->buffer_id);
			continue;
		}
		buf = GetBufferDescriptor(entry->buffer_id);
		if (entry->content_lock == NULL || LWLockHeldByMe(entry->content_lock)
			|| !cluster_bufmgr_pcm_x_writer_claim_entry_exact(entry, buf)) {
			cluster_pcm_x_runtime_fail_closed();
			elog(LOG,
				 "could not owner-exit drain malformed cluster PCM-X writer: buffer=%d phase=%d",
				 entry->buffer_id, (int)entry->phase);
			continue;
		}
		result = cluster_gcs_pcm_x_writer_claim_cleanup_and_wake_noexcept(&entry->claim);
		action = cluster_pcm_x_owner_exit_action(result, false, runtime_active);
		if (action == CLUSTER_PCM_X_OWNER_EXIT_COMPLETE)
			cluster_bufmgr_pcm_x_writer_clear(entry);
		else if (action == CLUSTER_PCM_X_OWNER_EXIT_RETRY) {
			entry->phase = PCM_X_WRITER_LEDGER_DEFERRED;
			retry = true;
		} else {
			entry->phase = PCM_X_WRITER_LEDGER_DEFERRED;
			cluster_pcm_x_runtime_fail_closed();
			elog(LOG, "preserving cluster PCM-X writer at owner exit: buffer=%d result=%d",
				 entry->buffer_id, (int)result);
		}
	}
	return retry;
}

static bool
cluster_bufmgr_pcm_x_holder_owner_exit_drain_once(bool runtime_active)
{
	bool retry = false;
	int i;

	for (i = 0; i < lengthof(cluster_bufmgr_pcm_x_holder_ledger); i++) {
		ClusterPcmXHolderLedgerEntry *entry = &cluster_bufmgr_pcm_x_holder_ledger[i];
		ClusterPcmXOwnerExitAction action;
		PcmXQueueResult result;
		BufferDesc *buf;

		if (entry->phase == PCM_X_HOLDER_LEDGER_UNUSED)
			continue;
		if (entry->buffer_id < 0 || entry->buffer_id >= NBuffers) {
			cluster_bufmgr_pcm_x_holder_defer_fail_closed(entry);
			elog(LOG,
				 "could not owner-exit drain cluster PCM-X holder with invalid buffer: buffer=%d",
				 entry->buffer_id);
			continue;
		}
		buf = GetBufferDescriptor(entry->buffer_id);
		if (entry->content_lock == NULL
			|| entry->content_lock != BufferDescriptorGetContentLock(buf)
			|| LWLockHeldByMe(entry->content_lock)) {
			cluster_bufmgr_pcm_x_holder_defer_fail_closed(entry);
			elog(LOG,
				 "could not owner-exit drain malformed cluster PCM-X holder: buffer=%d phase=%d",
				 entry->buffer_id, (int)entry->phase);
			continue;
		}
		result = cluster_pcm_x_local_holder_exceptional_detach_exact(&entry->handle,
																	 entry->content_lock);
		action = cluster_pcm_x_owner_exit_action(result, true, runtime_active);
		if (action == CLUSTER_PCM_X_OWNER_EXIT_COMPLETE)
			cluster_bufmgr_pcm_x_holder_clear(entry);
		else if (action == CLUSTER_PCM_X_OWNER_EXIT_RETRY) {
			entry->phase = PCM_X_HOLDER_LEDGER_DEFERRED;
			retry = true;
		} else {
			cluster_bufmgr_pcm_x_holder_defer_fail_closed(entry);
			elog(LOG, "preserving cluster PCM-X holder at owner exit: buffer=%d result=%d",
				 entry->buffer_id, (int)result);
		}
	}
	return retry;
}

static void
cluster_bufmgr_pcm_x_owner_exit_drain(void)
{
	for (;;) {
		PcmXRuntimeSnapshot runtime = cluster_pcm_x_runtime_snapshot();
		bool runtime_active
			= runtime.state == PCM_X_RUNTIME_ACTIVE && runtime.master_session_incarnation != 0;
		bool writer_retry;
		bool holder_retry;

		writer_retry = cluster_bufmgr_pcm_x_writer_owner_exit_drain_once(runtime_active);

		/*
		 * Content LWLocks are already gone.  The holder lane may therefore
		 * detach even when the writer's short admission gate asks for another
		 * pass; active_writer still blocks both a successor claim and DRAIN
		 * until WRITER_COMPLETE commits on a later iteration.
		 */
		holder_retry = cluster_bufmgr_pcm_x_holder_owner_exit_drain_once(runtime_active);
		if (!writer_retry && !holder_retry)
			return;
		if (!runtime_active) {
			cluster_pcm_x_runtime_fail_closed();
			return;
		}
		pg_usleep(1000L);
	}
}

static void
cluster_bufmgr_pcm_x_writer_reset(void)
{
	MemSet(cluster_bufmgr_pcm_x_writer_ledger, 0, sizeof(cluster_bufmgr_pcm_x_writer_ledger));
}

/* Defined with the process-local pin table below.  Direct-init proofs call it
 * while holding the matching buffer header spinlock so pin + mapping identity
 * are captured in one critical section. */
static inline int32 GetPrivateRefCount(Buffer buffer);

/* Capture the proof projection while tag, ownership, and shared refcount are
 * serialized by the BufferDesc header lock.  The private refcount table is
 * process-local and cannot change concurrently inside this backend. */
static void
cluster_bufmgr_pcm_direct_init_snapshot_locked(BufferDesc *buf, uint32 buf_state,
											   bool page_is_new,
											   ClusterPcmDirectInitSnapshot *out)
{
	memset(out, 0, sizeof(*out));
	out->tag = buf->tag;
	out->generation = cluster_pcm_own_gen_get(buf->buf_id);
	out->reservation_token = cluster_pcm_own_reservation_token_get(buf->buf_id);
	out->flags = cluster_pcm_own_flags_get(buf->buf_id);
	out->buf_state = buf_state;
	out->buf_id = buf->buf_id;
	out->private_refcount = GetPrivateRefCount(BufferDescriptorGetBuffer(buf));
	out->buffer_type = buf->buffer_type;
	out->pcm_state = buf->pcm_state;
	out->page_is_new = page_is_new;
}

pg_attribute_noreturn() static void
cluster_bufmgr_pcm_direct_init_report_failure(BufferDesc *buf, ClusterPcmOwnResult result,
											  const ClusterPcmDirectInitSnapshot *observed,
											  const char *context)
{
	uint64		generation = observed != NULL ? observed->generation : 0;
	uint32		flags = observed != NULL ? observed->flags : 0;

	cluster_grd_inc_block_path_failclosed();
	if (result == CLUSTER_PCM_OWN_BUSY || result == CLUSTER_PCM_OWN_CORRUPT ||
		result == CLUSTER_PCM_OWN_EXHAUSTED || result == CLUSTER_PCM_OWN_NOT_READY)
		cluster_pcm_own_report_bump_failure(buf, result, generation, flags, context);

	ereport(ERROR,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			 errmsg("cluster PCM direct initialization lacks an exact live proof"),
			 errdetail("context=%s buffer=%d result=%d generation=%llu flags=0x%x",
					   context, buf != NULL ? buf->buf_id : -1, (int) result,
					   (unsigned long long) generation, flags)));
}

pg_attribute_noreturn() static void
cluster_bufmgr_pcm_direct_init_no_grant_failclosed(BufferDesc *buf,
												  ClusterPcmDirectInitKind kind)
{
	cluster_grd_inc_block_path_failclosed();
	ereport(ERROR,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			 errmsg("cluster PCM direct initialization did not obtain X ownership"),
			 errdetail("buffer=%d operation=%d returned a one-shot image without a durable grant",
					   buf != NULL ? buf->buf_id : -1, (int) kind)));
}

/* Arm only at a source-authorized operation site.  This does not reserve the
 * buffer: the consumer revalidates the complete identity before publishing
 * GRANT_PENDING, so a changed/reused descriptor fails closed. */
static void
cluster_bufmgr_pcm_arm_direct_init(BufferDesc *buf, ClusterPcmDirectInitKind kind,
								   ClusterPcmDirectInitProof *proof)
{
	ClusterPcmDirectInitSnapshot observed;
	ClusterPcmOwnResult result;
	uint32		buf_state;

	memset(proof, 0, sizeof(*proof));
	if (!cluster_pcm_is_active() || !cluster_bufmgr_should_pcm_track(buf))
		return;

	buf_state = LockBufHdr(buf);
	cluster_bufmgr_pcm_direct_init_snapshot_locked(
		buf, buf_state, PageIsNew((Page) BufHdrGetBlock(buf)), &observed);
	result = cluster_pcm_direct_init_proof_arm(kind, &observed, proof);
	UnlockBufHdr(buf, buf_state);

	if (result != CLUSTER_PCM_OWN_OK)
		cluster_bufmgr_pcm_direct_init_report_failure(buf, result, &observed,
												  "direct-init arm");
}

/*
 * PCM write gate for the few operations which initialize known-new bytes and
 * therefore cannot use the ordinary N/S -> X convert queue.  The proof is
 * stack-local and single-use.  Its live recheck and GRANT_PENDING publication
 * share one BufferDesc header-lock interval; no wire request can be emitted
 * from an unproved or stale descriptor.
 */
static void
cluster_bufmgr_pcm_gate_direct_init(BufferDesc *buf, ClusterPcmDirectInitKind kind,
									ClusterPcmDirectInitProof *proof)
{
	ClusterPcmDirectInitSnapshot observed;
	ClusterPcmOwnSnapshot pending_base;
	ClusterPcmOwnResult pending_result;
	bool		grant_acquired = false;
	uint32		buf_state;
	uint64		pending_token = 0;
	uint64		committed_generation;

	if (!cluster_pcm_is_active() || !cluster_bufmgr_should_pcm_track(buf))
		return;
	if (proof == NULL)
		cluster_bufmgr_pcm_direct_init_report_failure(buf, CLUSTER_PCM_OWN_INVALID, NULL,
												  "direct-init missing proof");

	buf_state = LockBufHdr(buf);
	cluster_bufmgr_pcm_direct_init_snapshot_locked(
		buf, buf_state, PageIsNew((Page) BufHdrGetBlock(buf)), &observed);
	pending_result = cluster_pcm_direct_init_proof_consume(kind, &observed, proof);
	if (pending_result == CLUSTER_PCM_OWN_OK)
		pending_result = cluster_pcm_own_reservation_begin_exact(
			buf->buf_id, observed.generation, PCM_OWN_FLAG_GRANT_PENDING, &pending_token);
	if (pending_result == CLUSTER_PCM_OWN_OK)
	{
		memset(&pending_base, 0, sizeof(pending_base));
		pending_base.tag = observed.tag;
		pending_base.generation = observed.generation;
		pending_base.reservation_token = observed.reservation_token;
		pending_base.flags = observed.flags;
		pending_base.pcm_state = observed.pcm_state;
	}
	UnlockBufHdr(buf, buf_state);

	if (pending_result != CLUSTER_PCM_OWN_OK)
		cluster_bufmgr_pcm_direct_init_report_failure(buf, pending_result, &observed,
												  "direct-init consume");

	PG_TRY();
	{
		grant_acquired = cluster_pcm_lock_acquire_buffer(buf, PCM_LOCK_MODE_X);
	}
	PG_CATCH();
	{
		cluster_pcm_own_abort_grant_after_error(buf, &pending_base, pending_token,
												"direct-init acquire");
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (grant_acquired)
		cluster_pcm_own_finish_grant_or_rollback(buf, &pending_base, pending_token,
											 (uint8) PCM_STATE_X, PCM_LOCK_MODE_X,
											 &committed_generation);
	else
	{
		cluster_pcm_own_abort_grant_or_error(buf, &pending_base, pending_token,
												 "direct-init read-image");
		cluster_bufmgr_pcm_direct_init_no_grant_failclosed(buf, kind);
	}
}

#endif

/*
 * This is the size (in the number of blocks) above which we scan the
 * entire buffer pool to remove the buffers for all the pages of relation
 * being dropped. For the relations with size below this threshold, we find
 * the buffers by doing lookups in BufMapping table.
 */
#define BUF_DROP_FULL_SCAN_THRESHOLD		(uint64) (NBuffers / 32)

typedef struct PrivateRefCountEntry
{
	Buffer		buffer;
	int32		refcount;
} PrivateRefCountEntry;

/* 64 bytes, about the size of a cache line on common systems */
#define REFCOUNT_ARRAY_ENTRIES 8

/*
 * Status of buffers to checkpoint for a particular tablespace, used
 * internally in BufferSync.
 */
typedef struct CkptTsStatus
{
	/* oid of the tablespace */
	Oid			tsId;

	/*
	 * Checkpoint progress for this tablespace. To make progress comparable
	 * between tablespaces the progress is, for each tablespace, measured as a
	 * number between 0 and the total number of to-be-checkpointed pages. Each
	 * page checkpointed in this tablespace increments this space's progress
	 * by progress_slice.
	 */
	float8		progress;
	float8		progress_slice;

	/* number of to-be checkpointed pages in this tablespace */
	int			num_to_scan;
	/* already processed pages in this tablespace */
	int			num_scanned;

	/* current offset in CkptBufferIds for this tablespace */
	int			index;
} CkptTsStatus;

/*
 * Type for array used to sort SMgrRelations
 *
 * FlushRelationsAllBuffers shares the same comparator function with
 * DropRelationsAllBuffers. Pointer to this struct and RelFileLocator must be
 * compatible.
 */
typedef struct SMgrSortArray
{
	RelFileLocator rlocator;	/* This must be the first member */
	SMgrRelation srel;
} SMgrSortArray;

/* GUC variables */
bool		zero_damaged_pages = false;
int			bgwriter_lru_maxpages = 100;
double		bgwriter_lru_multiplier = 2.0;
bool		track_io_timing = false;

/*
 * How many buffers PrefetchBuffer callers should try to stay ahead of their
 * ReadBuffer calls by.  Zero means "never prefetch".  This value is only used
 * for buffers not belonging to tablespaces that have their
 * effective_io_concurrency parameter set.
 */
int			effective_io_concurrency = DEFAULT_EFFECTIVE_IO_CONCURRENCY;

/*
 * Like effective_io_concurrency, but used by maintenance code paths that might
 * benefit from a higher setting because they work on behalf of many sessions.
 * Overridden by the tablespace setting of the same name.
 */
int			maintenance_io_concurrency = DEFAULT_MAINTENANCE_IO_CONCURRENCY;

/*
 * GUC variables about triggering kernel writeback for buffers written; OS
 * dependent defaults are set via the GUC mechanism.
 */
int			checkpoint_flush_after = DEFAULT_CHECKPOINT_FLUSH_AFTER;
int			bgwriter_flush_after = DEFAULT_BGWRITER_FLUSH_AFTER;
int			backend_flush_after = DEFAULT_BACKEND_FLUSH_AFTER;

/* local state for LockBufferForCleanup */
static BufferDesc *PinCountWaitBuf = NULL;

/*
 * Backend-Private refcount management:
 *
 * Each buffer also has a private refcount that keeps track of the number of
 * times the buffer is pinned in the current process.  This is so that the
 * shared refcount needs to be modified only once if a buffer is pinned more
 * than once by an individual backend.  It's also used to check that no buffers
 * are still pinned at the end of transactions and when exiting.
 *
 *
 * To avoid - as we used to - requiring an array with NBuffers entries to keep
 * track of local buffers, we use a small sequentially searched array
 * (PrivateRefCountArray) and an overflow hash table (PrivateRefCountHash) to
 * keep track of backend local pins.
 *
 * Until no more than REFCOUNT_ARRAY_ENTRIES buffers are pinned at once, all
 * refcounts are kept track of in the array; after that, new array entries
 * displace old ones into the hash table. That way a frequently used entry
 * can't get "stuck" in the hashtable while infrequent ones clog the array.
 *
 * Note that in most scenarios the number of pinned buffers will not exceed
 * REFCOUNT_ARRAY_ENTRIES.
 *
 *
 * To enter a buffer into the refcount tracking mechanism first reserve a free
 * entry using ReservePrivateRefCountEntry() and then later, if necessary,
 * fill it with NewPrivateRefCountEntry(). That split lets us avoid doing
 * memory allocations in NewPrivateRefCountEntry() which can be important
 * because in some scenarios it's called with a spinlock held...
 */
static struct PrivateRefCountEntry PrivateRefCountArray[REFCOUNT_ARRAY_ENTRIES];
static HTAB *PrivateRefCountHash = NULL;
static int32 PrivateRefCountOverflowed = 0;
static uint32 PrivateRefCountClock = 0;
static PrivateRefCountEntry *ReservedRefCountEntry = NULL;

static void ReservePrivateRefCountEntry(void);
static PrivateRefCountEntry *NewPrivateRefCountEntry(Buffer buffer);
static PrivateRefCountEntry *GetPrivateRefCountEntry(Buffer buffer, bool do_move);
static inline int32 GetPrivateRefCount(Buffer buffer);
static void ForgetPrivateRefCountEntry(PrivateRefCountEntry *ref);

/*
 * Ensure that the PrivateRefCountArray has sufficient space to store one more
 * entry. This has to be called before using NewPrivateRefCountEntry() to fill
 * a new entry - but it's perfectly fine to not use a reserved entry.
 */
static void
ReservePrivateRefCountEntry(void)
{
	/* Already reserved (or freed), nothing to do */
	if (ReservedRefCountEntry != NULL)
		return;

	/*
	 * First search for a free entry the array, that'll be sufficient in the
	 * majority of cases.
	 */
	{
		int			i;

		for (i = 0; i < REFCOUNT_ARRAY_ENTRIES; i++)
		{
			PrivateRefCountEntry *res;

			res = &PrivateRefCountArray[i];

			if (res->buffer == InvalidBuffer)
			{
				ReservedRefCountEntry = res;
				return;
			}
		}
	}

	/*
	 * No luck. All array entries are full. Move one array entry into the hash
	 * table.
	 */
	{
		/*
		 * Move entry from the current clock position in the array into the
		 * hashtable. Use that slot.
		 */
		PrivateRefCountEntry *hashent;
		bool		found;

		/* select victim slot */
		ReservedRefCountEntry =
			&PrivateRefCountArray[PrivateRefCountClock++ % REFCOUNT_ARRAY_ENTRIES];

		/* Better be used, otherwise we shouldn't get here. */
		Assert(ReservedRefCountEntry->buffer != InvalidBuffer);

		/* enter victim array entry into hashtable */
		hashent = hash_search(PrivateRefCountHash,
							  &(ReservedRefCountEntry->buffer),
							  HASH_ENTER,
							  &found);
		Assert(!found);
		hashent->refcount = ReservedRefCountEntry->refcount;

		/* clear the now free array slot */
		ReservedRefCountEntry->buffer = InvalidBuffer;
		ReservedRefCountEntry->refcount = 0;

		PrivateRefCountOverflowed++;
	}
}

/*
 * Fill a previously reserved refcount entry.
 */
static PrivateRefCountEntry *
NewPrivateRefCountEntry(Buffer buffer)
{
	PrivateRefCountEntry *res;

	/* only allowed to be called when a reservation has been made */
	Assert(ReservedRefCountEntry != NULL);

	/* use up the reserved entry */
	res = ReservedRefCountEntry;
	ReservedRefCountEntry = NULL;

	/* and fill it */
	res->buffer = buffer;
	res->refcount = 0;

	return res;
}

/*
 * Return the PrivateRefCount entry for the passed buffer.
 *
 * Returns NULL if a buffer doesn't have a refcount entry. Otherwise, if
 * do_move is true, and the entry resides in the hashtable the entry is
 * optimized for frequent access by moving it to the array.
 */
static PrivateRefCountEntry *
GetPrivateRefCountEntry(Buffer buffer, bool do_move)
{
	PrivateRefCountEntry *res;
	int			i;

	Assert(BufferIsValid(buffer));
	Assert(!BufferIsLocal(buffer));

	/*
	 * First search for references in the array, that'll be sufficient in the
	 * majority of cases.
	 */
	for (i = 0; i < REFCOUNT_ARRAY_ENTRIES; i++)
	{
		res = &PrivateRefCountArray[i];

		if (res->buffer == buffer)
			return res;
	}

	/*
	 * By here we know that the buffer, if already pinned, isn't residing in
	 * the array.
	 *
	 * Only look up the buffer in the hashtable if we've previously overflowed
	 * into it.
	 */
	if (PrivateRefCountOverflowed == 0)
		return NULL;

	res = hash_search(PrivateRefCountHash, &buffer, HASH_FIND, NULL);

	if (res == NULL)
		return NULL;
	else if (!do_move)
	{
		/* caller doesn't want us to move the hash entry into the array */
		return res;
	}
	else
	{
		/* move buffer from hashtable into the free array slot */
		bool		found;
		PrivateRefCountEntry *free;

		/* Ensure there's a free array slot */
		ReservePrivateRefCountEntry();

		/* Use up the reserved slot */
		Assert(ReservedRefCountEntry != NULL);
		free = ReservedRefCountEntry;
		ReservedRefCountEntry = NULL;
		Assert(free->buffer == InvalidBuffer);

		/* and fill it */
		free->buffer = buffer;
		free->refcount = res->refcount;

		/* delete from hashtable */
		hash_search(PrivateRefCountHash, &buffer, HASH_REMOVE, &found);
		Assert(found);
		Assert(PrivateRefCountOverflowed > 0);
		PrivateRefCountOverflowed--;

		return free;
	}
}

/*
 * Returns how many times the passed buffer is pinned by this backend.
 *
 * Only works for shared memory buffers!
 */
static inline int32
GetPrivateRefCount(Buffer buffer)
{
	PrivateRefCountEntry *ref;

	Assert(BufferIsValid(buffer));
	Assert(!BufferIsLocal(buffer));

	/*
	 * Not moving the entry - that's ok for the current users, but we might
	 * want to change this one day.
	 */
	ref = GetPrivateRefCountEntry(buffer, false);

	if (ref == NULL)
		return 0;
	return ref->refcount;
}

/*
 * Release resources used to track the reference count of a buffer which we no
 * longer have pinned and don't want to pin again immediately.
 */
static void
ForgetPrivateRefCountEntry(PrivateRefCountEntry *ref)
{
	Assert(ref->refcount == 0);

	if (ref >= &PrivateRefCountArray[0] &&
		ref < &PrivateRefCountArray[REFCOUNT_ARRAY_ENTRIES])
	{
		ref->buffer = InvalidBuffer;

		/*
		 * Mark the just used entry as reserved - in many scenarios that
		 * allows us to avoid ever having to search the array/hash for free
		 * entries.
		 */
		ReservedRefCountEntry = ref;
	}
	else
	{
		bool		found;
		Buffer		buffer = ref->buffer;

		hash_search(PrivateRefCountHash, &buffer, HASH_REMOVE, &found);
		Assert(found);
		Assert(PrivateRefCountOverflowed > 0);
		PrivateRefCountOverflowed--;
	}
}

/*
 * BufferIsPinned
 *		True iff the buffer is pinned (also checks for valid buffer number).
 *
 *		NOTE: what we check here is that *this* backend holds a pin on
 *		the buffer.  We do not care whether some other backend does.
 */
#define BufferIsPinned(bufnum) \
( \
	!BufferIsValid(bufnum) ? \
		false \
	: \
		BufferIsLocal(bufnum) ? \
			(LocalRefCount[-(bufnum) - 1] > 0) \
		: \
	(GetPrivateRefCount(bufnum) > 0) \
)


static Buffer ReadBuffer_common(SMgrRelation smgr, char relpersistence,
								ForkNumber forkNum, BlockNumber blockNum,
								ReadBufferMode mode, BufferAccessStrategy strategy,
								bool *hit);
static BlockNumber ExtendBufferedRelCommon(BufferManagerRelation bmr,
										   ForkNumber fork,
										   BufferAccessStrategy strategy,
										   uint32 flags,
										   uint32 extend_by,
										   BlockNumber extend_upto,
										   Buffer *buffers,
										   uint32 *extended_by);
static BlockNumber ExtendBufferedRelShared(BufferManagerRelation bmr,
										   ForkNumber fork,
										   BufferAccessStrategy strategy,
										   uint32 flags,
										   uint32 extend_by,
										   BlockNumber extend_upto,
										   Buffer *buffers,
										   uint32 *extended_by);
static bool PinBuffer(BufferDesc *buf, BufferAccessStrategy strategy);
static void PinBuffer_Locked(BufferDesc *buf);
static void UnpinBuffer(BufferDesc *buf);
static void BufferSync(int flags);
#ifdef USE_PGRAC_CLUSTER
/* PGRAC: spec-6.12h D-h1 — Past Image conversion (definition below). */
static bool cluster_bufmgr_convert_to_pi_locked(BufferDesc *buf, uint32 buf_state);

/* PGRAC: spec-6.12h D-h2 — FlushBuffer write-note (cluster_gcs_block.h is
 * included mid-file, after FlushBuffer; redeclare the one symbol it needs). */
extern void cluster_gcs_block_pi_write_note(BufferTag tag, SCN page_scn);

/* PGRAC ownership-generation wave (W3) — RED delivery shim, used by LockBuffer
 * (before the mid-file include; same redeclare pattern as above). */
extern bool cluster_gcs_block_test_deliver_self_invalidate(BufferTag tag);
#endif
static uint32 WaitBufHdrUnlocked(BufferDesc *buf);
static int	SyncOneBuffer(int buf_id, bool skip_recently_used,
						  WritebackContext *wb_context);
static void WaitIO(BufferDesc *buf);
static bool StartBufferIO(BufferDesc *buf, bool forInput);
static void TerminateBufferIO(BufferDesc *buf, bool clear_dirty,
							  uint32 set_flag_bits);
static void shared_buffer_write_error_callback(void *arg);
static void local_buffer_write_error_callback(void *arg);

/* PGRAC: GCS serve-stall round-5 — InvalidateBuffer tail extraction + the
 * bounded non-waiting variant for the IC dispatch pump. */
static bool InvalidateBufferCommitLocked(BufferDesc *buf, BufferTag *oldTag, uint32 oldHash,
										 LWLock *oldPartitionLock, uint32 buf_state);
static void InvalidateBufferCommitTailLocked(BufferDesc *buf, BufferTag *oldTag, uint32 oldHash,
										 LWLock *oldPartitionLock, uint32 buf_state,
										 uint8 old_pcm_mode, bool release_pcm_holder);
static bool InvalidateBufferTry(BufferDesc *buf);
static BufferDesc *BufferAlloc(SMgrRelation smgr,
							   char relpersistence,
							   ForkNumber forkNum,
							   BlockNumber blockNum,
							   BufferAccessStrategy strategy,
							   bool *foundPtr, IOContext io_context);
static Buffer GetVictimBuffer(BufferAccessStrategy strategy, IOContext io_context);
static void FlushBuffer(BufferDesc *buf, SMgrRelation reln,
						IOObject io_object, IOContext io_context);
static void FindAndDropRelationBuffers(RelFileLocator rlocator,
									   ForkNumber forkNum,
									   BlockNumber nForkBlock,
									   BlockNumber firstDelBlock);
static void RelationCopyStorageUsingBuffer(RelFileLocator srclocator,
										   RelFileLocator dstlocator,
										   ForkNumber forkNum, bool permanent);
static void AtProcExit_Buffers(int code, Datum arg);
static void CheckForBufferLeaks(void);
#ifdef USE_ASSERT_CHECKING
static void AssertNotCatalogBufferLock(LWLock *lock, LWLockMode mode,
									   void *unused_context);
#endif
static int	rlocator_comparator(const void *p1, const void *p2);
static inline int buffertag_comparator(const BufferTag *ba, const BufferTag *bb);
static inline int ckpt_buforder_comparator(const CkptSortItem *a, const CkptSortItem *b);
static int	ts_ckpt_progress_comparator(Datum a, Datum b, void *arg);


/*
 * Implementation of PrefetchBuffer() for shared buffers.
 */
PrefetchBufferResult
PrefetchSharedBuffer(SMgrRelation smgr_reln,
					 ForkNumber forkNum,
					 BlockNumber blockNum)
{
	PrefetchBufferResult result = {InvalidBuffer, false};
	BufferTag	newTag;			/* identity of requested block */
	uint32		newHash;		/* hash value for newTag */
	LWLock	   *newPartitionLock;	/* buffer partition lock for it */
	int			buf_id;

	Assert(BlockNumberIsValid(blockNum));

	/* create a tag so we can lookup the buffer */
	InitBufferTag(&newTag, &smgr_reln->smgr_rlocator.locator,
				  forkNum, blockNum);

	/* determine its hash code and partition lock ID */
	newHash = BufTableHashCode(&newTag);
	newPartitionLock = BufMappingPartitionLock(newHash);

	/* see if the block is in the buffer pool already */
	LWLockAcquire(newPartitionLock, LW_SHARED);
	buf_id = BufTableLookup(&newTag, newHash);
	LWLockRelease(newPartitionLock);

	/* If not in buffers, initiate prefetch */
	if (buf_id < 0)
	{
#ifdef USE_PREFETCH
		/*
		 * Try to initiate an asynchronous read.  This returns false in
		 * recovery if the relation file doesn't exist.
		 */
		if ((io_direct_flags & IO_DIRECT_DATA) == 0 &&
			smgrprefetch(smgr_reln, forkNum, blockNum))
		{
			result.initiated_io = true;
		}
#endif							/* USE_PREFETCH */
	}
	else
	{
		/*
		 * Report the buffer it was in at that time.  The caller may be able
		 * to avoid a buffer table lookup, but it's not pinned and it must be
		 * rechecked!
		 */
		result.recent_buffer = buf_id + 1;
	}

	/*
	 * If the block *is* in buffers, we do nothing.  This is not really ideal:
	 * the block might be just about to be evicted, which would be stupid
	 * since we know we are going to need it soon.  But the only easy answer
	 * is to bump the usage_count, which does not seem like a great solution:
	 * when the caller does ultimately touch the block, usage_count would get
	 * bumped again, resulting in too much favoritism for blocks that are
	 * involved in a prefetch sequence. A real fix would involve some
	 * additional per-buffer state, and it's not clear that there's enough of
	 * a problem to justify that.
	 */

	return result;
}

/*
 * PrefetchBuffer -- initiate asynchronous read of a block of a relation
 *
 * This is named by analogy to ReadBuffer but doesn't actually allocate a
 * buffer.  Instead it tries to ensure that a future ReadBuffer for the given
 * block will not be delayed by the I/O.  Prefetching is optional.
 *
 * There are three possible outcomes:
 *
 * 1.  If the block is already cached, the result includes a valid buffer that
 * could be used by the caller to avoid the need for a later buffer lookup, but
 * it's not pinned, so the caller must recheck it.
 *
 * 2.  If the kernel has been asked to initiate I/O, the initiated_io member is
 * true.  Currently there is no way to know if the data was already cached by
 * the kernel and therefore didn't really initiate I/O, and no way to know when
 * the I/O completes other than using synchronous ReadBuffer().
 *
 * 3.  Otherwise, the buffer wasn't already cached by PostgreSQL, and
 * USE_PREFETCH is not defined (this build doesn't support prefetching due to
 * lack of a kernel facility), direct I/O is enabled, or the underlying
 * relation file wasn't found and we are in recovery.  (If the relation file
 * wasn't found and we are not in recovery, an error is raised).
 */
PrefetchBufferResult
PrefetchBuffer(Relation reln, ForkNumber forkNum, BlockNumber blockNum)
{
	Assert(RelationIsValid(reln));
	Assert(BlockNumberIsValid(blockNum));

	if (RelationUsesLocalBuffers(reln))
	{
		/* see comments in ReadBufferExtended */
		if (RELATION_IS_OTHER_TEMP(reln))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot access temporary tables of other sessions")));

		/* pass it off to localbuf.c */
		return PrefetchLocalBuffer(RelationGetSmgr(reln), forkNum, blockNum);
	}
	else
	{
		/* pass it to the shared buffer version */
		return PrefetchSharedBuffer(RelationGetSmgr(reln), forkNum, blockNum);
	}
}

/*
 * ReadRecentBuffer -- try to pin a block in a recently observed buffer
 *
 * Compared to ReadBuffer(), this avoids a buffer mapping lookup when it's
 * successful.  Return true if the buffer is valid and still has the expected
 * tag.  In that case, the buffer is pinned and the usage count is bumped.
 */
bool
ReadRecentBuffer(RelFileLocator rlocator, ForkNumber forkNum, BlockNumber blockNum,
				 Buffer recent_buffer)
{
	BufferDesc *bufHdr;
	BufferTag	tag;
	uint32		buf_state;
	bool		have_private_ref;

	Assert(BufferIsValid(recent_buffer));

	ResourceOwnerEnlargeBuffers(CurrentResourceOwner);
	ReservePrivateRefCountEntry();
	InitBufferTag(&tag, &rlocator, forkNum, blockNum);

	if (BufferIsLocal(recent_buffer))
	{
		int			b = -recent_buffer - 1;

		bufHdr = GetLocalBufferDescriptor(b);
		buf_state = pg_atomic_read_u32(&bufHdr->state);

		/* Is it still valid and holding the right tag? */
		if ((buf_state & BM_VALID) && BufferTagsEqual(&tag, &bufHdr->tag))
		{
			PinLocalBuffer(bufHdr, true);

			pgBufferUsage.local_blks_hit++;

			return true;
		}
	}
	else
	{
		bufHdr = GetBufferDescriptor(recent_buffer - 1);
		have_private_ref = GetPrivateRefCount(recent_buffer) > 0;

		/*
		 * Do we already have this buffer pinned with a private reference?  If
		 * so, it must be valid and it is safe to check the tag without
		 * locking.  If not, we have to lock the header first and then check.
		 */
		if (have_private_ref)
			buf_state = pg_atomic_read_u32(&bufHdr->state);
		else
			buf_state = LockBufHdr(bufHdr);

		if ((buf_state & BM_VALID) && BufferTagsEqual(&tag, &bufHdr->tag))
		{
			/*
			 * It's now safe to pin the buffer.  We can't pin first and ask
			 * questions later, because it might confuse code paths like
			 * InvalidateBuffer() if we pinned a random non-matching buffer.
			 */
			if (have_private_ref)
				PinBuffer(bufHdr, NULL);	/* bump pin count */
			else
				PinBuffer_Locked(bufHdr);	/* pin for first time */

			pgBufferUsage.shared_blks_hit++;

			return true;
		}

		/* If we locked the header above, now unlock. */
		if (!have_private_ref)
			UnlockBufHdr(bufHdr, buf_state);
	}

	return false;
}

/*
 * ReadBuffer -- a shorthand for ReadBufferExtended, for reading from main
 *		fork with RBM_NORMAL mode and default strategy.
 */
Buffer
ReadBuffer(Relation reln, BlockNumber blockNum)
{
	return ReadBufferExtended(reln, MAIN_FORKNUM, blockNum, RBM_NORMAL, NULL);
}

/*
 * ReadBufferExtended -- returns a buffer containing the requested
 *		block of the requested relation.  If the blknum
 *		requested is P_NEW, extend the relation file and
 *		allocate a new block.  (Caller is responsible for
 *		ensuring that only one backend tries to extend a
 *		relation at the same time!)
 *
 * Returns: the buffer number for the buffer containing
 *		the block read.  The returned buffer has been pinned.
 *		Does not return on error --- elog's instead.
 *
 * Assume when this function is called, that reln has been opened already.
 *
 * In RBM_NORMAL mode, the page is read from disk, and the page header is
 * validated.  An error is thrown if the page header is not valid.  (But
 * note that an all-zero page is considered "valid"; see
 * PageIsVerifiedExtended().)
 *
 * RBM_ZERO_ON_ERROR is like the normal mode, but if the page header is not
 * valid, the page is zeroed instead of throwing an error. This is intended
 * for non-critical data, where the caller is prepared to repair errors.
 *
 * In RBM_ZERO_AND_LOCK mode, if the page isn't in buffer cache already, it's
 * filled with zeros instead of reading it from disk.  Useful when the caller
 * is going to fill the page from scratch, since this saves I/O and avoids
 * unnecessary failure if the page-on-disk has corrupt page headers.
 * The page is returned locked to ensure that the caller has a chance to
 * initialize the page before it's made visible to others.
 * Caution: do not use this mode to read a page that is beyond the relation's
 * current physical EOF; that is likely to cause problems in md.c when
 * the page is modified and written out. P_NEW is OK, though.
 *
 * RBM_ZERO_AND_CLEANUP_LOCK is the same as RBM_ZERO_AND_LOCK, but acquires
 * a cleanup-strength lock on the page.
 *
 * RBM_NORMAL_NO_LOG mode is treated the same as RBM_NORMAL here.
 *
 * If strategy is not NULL, a nondefault buffer access strategy is used.
 * See buffer/README for details.
 */
Buffer
ReadBufferExtended(Relation reln, ForkNumber forkNum, BlockNumber blockNum,
				   ReadBufferMode mode, BufferAccessStrategy strategy)
{
	bool		hit;
	Buffer		buf;

	/*
	 * Reject attempts to read non-local temporary relations; we would be
	 * likely to get wrong data since we have no visibility into the owning
	 * session's local buffers.
	 */
	if (RELATION_IS_OTHER_TEMP(reln))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot access temporary tables of other sessions")));

#ifdef USE_PGRAC_CLUSTER

	/*
	 * PGRAC: spec-5.59 D4 — relation-kind hint for the cross-node profiling
	 * index axis.  This is the last point on the read path that still has
	 * the Relation (and thus relkind); the GCS block layer consumes the
	 * hint by exact BufferTag match.  Profiling-only, GUC-gated inside the
	 * setter, no behavior change.
	 */
	if (unlikely(cluster_xnode_profile_enabled) &&
		blockNum != P_NEW &&
		!RelationUsesLocalBuffers(reln))
	{
		BufferTag	xp_tag;

		InitBufferTag(&xp_tag, &reln->rd_locator, forkNum, blockNum);
		cluster_xp_relkind_hint_set(&xp_tag,
									reln->rd_rel->relkind == RELKIND_INDEX);
	}
#endif

	/*
	 * Read the buffer, and update pgstat counters to reflect a cache hit or
	 * miss.
	 */
	pgstat_count_buffer_read(reln);
	buf = ReadBuffer_common(RelationGetSmgr(reln), reln->rd_rel->relpersistence,
							forkNum, blockNum, mode, strategy, &hit);
	if (hit)
		pgstat_count_buffer_hit(reln);
	return buf;
}


/*
 * ReadBufferWithoutRelcache -- like ReadBufferExtended, but doesn't require
 *		a relcache entry for the relation.
 *
 * Pass permanent = true for a RELPERSISTENCE_PERMANENT relation, and
 * permanent = false for a RELPERSISTENCE_UNLOGGED relation. This function
 * cannot be used for temporary relations (and making that work might be
 * difficult, unless we only want to read temporary relations for our own
 * BackendId).
 */
Buffer
ReadBufferWithoutRelcache(RelFileLocator rlocator, ForkNumber forkNum,
						  BlockNumber blockNum, ReadBufferMode mode,
						  BufferAccessStrategy strategy, bool permanent)
{
	bool		hit;

	SMgrRelation smgr = smgropen(rlocator, InvalidBackendId);

	return ReadBuffer_common(smgr, permanent ? RELPERSISTENCE_PERMANENT :
							 RELPERSISTENCE_UNLOGGED, forkNum, blockNum,
							 mode, strategy, &hit);
}

/*
 * Convenience wrapper around ExtendBufferedRelBy() extending by one block.
 */
Buffer
ExtendBufferedRel(BufferManagerRelation bmr,
				  ForkNumber forkNum,
				  BufferAccessStrategy strategy,
				  uint32 flags)
{
	Buffer		buf;
	uint32		extend_by = 1;

	ExtendBufferedRelBy(bmr, forkNum, strategy, flags, extend_by,
						&buf, &extend_by);

	return buf;
}

/*
 * Extend relation by multiple blocks.
 *
 * Tries to extend the relation by extend_by blocks. Depending on the
 * availability of resources the relation may end up being extended by a
 * smaller number of pages (unless an error is thrown, always by at least one
 * page). *extended_by is updated to the number of pages the relation has been
 * extended to.
 *
 * buffers needs to be an array that is at least extend_by long. Upon
 * completion, the first extend_by array elements will point to a pinned
 * buffer.
 *
 * If EB_LOCK_FIRST is part of flags, the first returned buffer is
 * locked. This is useful for callers that want a buffer that is guaranteed to
 * be empty.
 */
BlockNumber
ExtendBufferedRelBy(BufferManagerRelation bmr,
					ForkNumber fork,
					BufferAccessStrategy strategy,
					uint32 flags,
					uint32 extend_by,
					Buffer *buffers,
					uint32 *extended_by)
{
	Assert((bmr.rel != NULL) != (bmr.smgr != NULL));
	Assert(bmr.smgr == NULL || bmr.relpersistence != 0);
	Assert(extend_by > 0);

	if (bmr.smgr == NULL)
	{
		bmr.smgr = RelationGetSmgr(bmr.rel);
		bmr.relpersistence = bmr.rel->rd_rel->relpersistence;
	}

	return ExtendBufferedRelCommon(bmr, fork, strategy, flags,
								   extend_by, InvalidBlockNumber,
								   buffers, extended_by);
}

/*
 * Extend the relation so it is at least extend_to blocks large, return buffer
 * (extend_to - 1).
 *
 * This is useful for callers that want to write a specific page, regardless
 * of the current size of the relation (e.g. useful for visibilitymap and for
 * crash recovery).
 */
Buffer
ExtendBufferedRelTo(BufferManagerRelation bmr,
					ForkNumber fork,
					BufferAccessStrategy strategy,
					uint32 flags,
					BlockNumber extend_to,
					ReadBufferMode mode)
{
	BlockNumber current_size;
	uint32		extended_by = 0;
	Buffer		buffer = InvalidBuffer;
	Buffer		buffers[64];

	Assert((bmr.rel != NULL) != (bmr.smgr != NULL));
	Assert(bmr.smgr == NULL || bmr.relpersistence != 0);
	Assert(extend_to != InvalidBlockNumber && extend_to > 0);

	if (bmr.smgr == NULL)
	{
		bmr.smgr = RelationGetSmgr(bmr.rel);
		bmr.relpersistence = bmr.rel->rd_rel->relpersistence;
	}

	/*
	 * If desired, create the file if it doesn't exist.  If
	 * smgr_cached_nblocks[fork] is positive then it must exist, no need for
	 * an smgrexists call.
	 */
	if ((flags & EB_CREATE_FORK_IF_NEEDED) &&
		(bmr.smgr->smgr_cached_nblocks[fork] == 0 ||
		 bmr.smgr->smgr_cached_nblocks[fork] == InvalidBlockNumber) &&
		!smgrexists(bmr.smgr, fork))
	{
		LockRelationForExtension(bmr.rel, ExclusiveLock);

		/* could have been closed while waiting for lock */
		if (bmr.rel)
			bmr.smgr = RelationGetSmgr(bmr.rel);

		/* recheck, fork might have been created concurrently */
		if (!smgrexists(bmr.smgr, fork))
			smgrcreate(bmr.smgr, fork, flags & EB_PERFORMING_RECOVERY);

		UnlockRelationForExtension(bmr.rel, ExclusiveLock);
	}

	/*
	 * If requested, invalidate size cache, so that smgrnblocks asks the
	 * kernel.
	 */
	if (flags & EB_CLEAR_SIZE_CACHE)
		bmr.smgr->smgr_cached_nblocks[fork] = InvalidBlockNumber;

	/*
	 * Estimate how many pages we'll need to extend by. This avoids acquiring
	 * unnecessarily many victim buffers.
	 */
	current_size = smgrnblocks(bmr.smgr, fork);

	/*
	 * Since no-one else can be looking at the page contents yet, there is no
	 * difference between an exclusive lock and a cleanup-strength lock. Note
	 * that we pass the original mode to ReadBuffer_common() below, when
	 * falling back to reading the buffer to a concurrent relation extension.
	 */
	if (mode == RBM_ZERO_AND_LOCK || mode == RBM_ZERO_AND_CLEANUP_LOCK)
		flags |= EB_LOCK_TARGET;

	while (current_size < extend_to)
	{
		uint32		num_pages = lengthof(buffers);
		BlockNumber first_block;

		if ((uint64) current_size + num_pages > extend_to)
			num_pages = extend_to - current_size;

		first_block = ExtendBufferedRelCommon(bmr, fork, strategy, flags,
											  num_pages, extend_to,
											  buffers, &extended_by);

		current_size = first_block + extended_by;
		Assert(num_pages != 0 || current_size >= extend_to);

		for (int i = 0; i < extended_by; i++)
		{
			if (first_block + i != extend_to - 1)
				ReleaseBuffer(buffers[i]);
			else
				buffer = buffers[i];
		}
	}

	/*
	 * It's possible that another backend concurrently extended the relation.
	 * In that case read the buffer.
	 *
	 * XXX: Should we control this via a flag?
	 */
	if (buffer == InvalidBuffer)
	{
		bool		hit;

		Assert(extended_by == 0);
		buffer = ReadBuffer_common(bmr.smgr, bmr.relpersistence,
								   fork, extend_to - 1, mode, strategy,
								   &hit);
	}

	return buffer;
}

/*
 * ReadBuffer_common -- common logic for all ReadBuffer variants
 *
 * *hit is set to true if the request was satisfied from shared buffer cache.
 */
static Buffer
ReadBuffer_common(SMgrRelation smgr, char relpersistence, ForkNumber forkNum,
				  BlockNumber blockNum, ReadBufferMode mode,
				  BufferAccessStrategy strategy, bool *hit)
{
	BufferDesc *bufHdr;
	Block		bufBlock;
	bool		found;
	IOContext	io_context;
	IOObject	io_object;
	bool		isLocalBuf = SmgrIsTemp(smgr);
#ifdef USE_PGRAC_CLUSTER
	ClusterPcmDirectInitProof direct_init_proof;
#endif

	*hit = false;

	/*
	 * Backward compatibility path, most code should use ExtendBufferedRel()
	 * instead, as acquiring the extension lock inside ExtendBufferedRel()
	 * scales a lot better.
	 */
	if (unlikely(blockNum == P_NEW))
	{
		uint32		flags = EB_SKIP_EXTENSION_LOCK;

		/*
		 * Since no-one else can be looking at the page contents yet, there is
		 * no difference between an exclusive lock and a cleanup-strength
		 * lock.
		 */
		if (mode == RBM_ZERO_AND_LOCK || mode == RBM_ZERO_AND_CLEANUP_LOCK)
			flags |= EB_LOCK_FIRST;

		return ExtendBufferedRel(BMR_SMGR(smgr, relpersistence),
								 forkNum, strategy, flags);
	}

	/* Make sure we will have room to remember the buffer pin */
	ResourceOwnerEnlargeBuffers(CurrentResourceOwner);

	TRACE_POSTGRESQL_BUFFER_READ_START(forkNum, blockNum,
									   smgr->smgr_rlocator.locator.spcOid,
									   smgr->smgr_rlocator.locator.dbOid,
									   smgr->smgr_rlocator.locator.relNumber,
									   smgr->smgr_rlocator.backend);

	if (isLocalBuf)
	{
		/*
		 * We do not use a BufferAccessStrategy for I/O of temporary tables.
		 * However, in some cases, the "strategy" may not be NULL, so we can't
		 * rely on IOContextForStrategy() to set the right IOContext for us.
		 * This may happen in cases like CREATE TEMPORARY TABLE AS...
		 */
		io_context = IOCONTEXT_NORMAL;
		io_object = IOOBJECT_TEMP_RELATION;
		bufHdr = LocalBufferAlloc(smgr, forkNum, blockNum, &found);
		if (found)
			pgBufferUsage.local_blks_hit++;
		else if (mode == RBM_NORMAL || mode == RBM_NORMAL_NO_LOG ||
				 mode == RBM_ZERO_ON_ERROR)
			pgBufferUsage.local_blks_read++;
	}
	else
	{
		/*
		 * lookup the buffer.  IO_IN_PROGRESS is set if the requested block is
		 * not currently in memory.
		 */
		io_context = IOContextForStrategy(strategy);
		io_object = IOOBJECT_RELATION;
		bufHdr = BufferAlloc(smgr, relpersistence, forkNum, blockNum,
							 strategy, &found, io_context);
		if (found)
			pgBufferUsage.shared_blks_hit++;
		else if (mode == RBM_NORMAL || mode == RBM_NORMAL_NO_LOG ||
				 mode == RBM_ZERO_ON_ERROR)
			pgBufferUsage.shared_blks_read++;

#ifdef USE_PGRAC_CLUSTER

		/*
		 * spec-6.14 D10b: catalog-page buffer traffic under the shared
		 * catalog (dump keys catalog.buf_hit_count / buf_miss_count).  A miss
		 * is a catalog page sourced from shared storage or a cross-node CF
		 * transfer -- the cache-locality signal the A-L9 observability leg
		 * watches.
		 */
		if (cluster_shared_catalog &&
			smgr->smgr_rlocator.locator.relNumber <
			(RelFileNumber) FirstNormalObjectId)
		{
			if (found)
				cluster_catalog_stats_buf_hit_inc();
			else
				cluster_catalog_stats_buf_miss_inc();
		}
#endif
	}

	/* At this point we do NOT hold any locks. */

	/* if it was already in the buffer pool, we're done */
	if (found)
	{
		/* Just need to update stats before we exit */
		*hit = true;
		VacuumPageHit++;
		pgstat_count_io_op(io_object, io_context, IOOP_HIT);

		if (VacuumCostActive)
			VacuumCostBalance += VacuumCostPageHit;

		TRACE_POSTGRESQL_BUFFER_READ_DONE(forkNum, blockNum,
										  smgr->smgr_rlocator.locator.spcOid,
										  smgr->smgr_rlocator.locator.dbOid,
										  smgr->smgr_rlocator.locator.relNumber,
										  smgr->smgr_rlocator.backend,
										  found);

		/*
		 * In RBM_ZERO_AND_LOCK mode the caller expects the page to be locked
		 * on return.
		 */
		if (!isLocalBuf)
		{
			if (mode == RBM_ZERO_AND_LOCK)
				LockBuffer(BufferDescriptorGetBuffer(bufHdr), BUFFER_LOCK_EXCLUSIVE);
			else if (mode == RBM_ZERO_AND_CLEANUP_LOCK)
				LockBufferForCleanup(BufferDescriptorGetBuffer(bufHdr));
		}

		return BufferDescriptorGetBuffer(bufHdr);
	}

	/*
	 * if we have gotten to this point, we have allocated a buffer for the
	 * page but its contents are not yet valid.  IO_IN_PROGRESS is set for it,
	 * if it's a shared buffer.
	 */
	Assert(!(pg_atomic_read_u32(&bufHdr->state) & BM_VALID));	/* spinlock not needed */

	bufBlock = isLocalBuf ? LocalBufHdrGetBlock(bufHdr) : BufHdrGetBlock(bufHdr);

	/*
	 * Read in the page, unless the caller intends to overwrite it and just
	 * wants us to allocate a buffer.
	 */
	if (mode == RBM_ZERO_AND_LOCK || mode == RBM_ZERO_AND_CLEANUP_LOCK)
	{
		MemSet((char *) bufBlock, 0, BLCKSZ);
#ifdef USE_PGRAC_CLUSTER
		if (!isLocalBuf)
			cluster_bufmgr_pcm_arm_direct_init(
				bufHdr, CLUSTER_PCM_DIRECT_INIT_READ_MISS, &direct_init_proof);
#endif
	}
	else
	{
		instr_time	io_start = pgstat_prepare_io_time();
		bool		verified;

		smgrread(smgr, forkNum, blockNum, bufBlock);

		pgstat_count_io_op_time(io_object, io_context,
								IOOP_READ, io_start, 1);

		/* check for garbage data */
		verified = PageIsVerifiedExtended((Page) bufBlock, blockNum,
										  PIV_LOG_WARNING | PIV_REPORT_STAT);

#ifdef USE_PGRAC_CLUSTER

		/*
		 * PGRAC (spec-4.10 D1/D4): try to rebuild a corrupt block from WAL
		 * before applying the zero/error policy.  PERMANENT shared-buffer
		 * relations only (relpersistence == RELPERSISTENCE_PERMANENT &&
		 * !isLocalBuf): temp / unlogged relations have no authoritative WAL
		 * chain -- a reused relfilenode could even match stale WAL of a
		 * dropped relation -- so they are never WAL-reconstructed.
		 * Single-node / own-thread only (cross-node forward Stage 5).  Q3
		 * (recovery-precedence): with online recovery on, also rebuild when
		 * ignore_checksum_failure masked a real checksum mismatch
		 * (PageIsVerifiedExtended returned true); if it cannot rebuild,
		 * ignore_checksum_failure's "return the page as-is" remains the
		 * fallback (verified stays true).
		 */
		if (!verified)
		{
			if (relpersistence == RELPERSISTENCE_PERMANENT && !isLocalBuf
				&& cluster_block_recovery_on_read(smgr, forkNum, blockNum, (char *) bufBlock))
				verified = true;
		}
		else if (relpersistence == RELPERSISTENCE_PERMANENT && !isLocalBuf
				 && ignore_checksum_failure && cluster_online_block_recovery
				 && cluster_block_recovery_checksum_mismatch((char *) bufBlock, blockNum))
		{
			(void) cluster_block_recovery_on_read(smgr, forkNum, blockNum, (char *) bufBlock);
		}
#endif

		if (!verified)
		{
			if (mode == RBM_ZERO_ON_ERROR || zero_damaged_pages)
			{
				ereport(WARNING,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("invalid page in block %u of relation %s; zeroing out page",
								blockNum,
								relpath(smgr->smgr_rlocator, forkNum))));
				MemSet((char *) bufBlock, 0, BLCKSZ);
			}
#ifdef USE_PGRAC_CLUSTER
			else if (cluster_online_block_recovery)
			{
				/*
				 * Online recovery on but the block could not be rebuilt.
				 * PANIC escalation (operator opt-in) is restricted to the
				 * main data fork: VM/FSM and other auxiliary forks are
				 * reconstructible from scratch, so an unrebuildable one
				 * should fail the query (ERROR), not crash the instance.
				 */
				int			elevel = (cluster_block_recovery_on_unrecoverable == CLUSTER_BLKREC_ACTION_PANIC
									  && forkNum == MAIN_FORKNUM)
					? PANIC : ERROR;

				ereport(elevel,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("invalid page in block %u of relation %s",
								blockNum,
								relpath(smgr->smgr_rlocator, forkNum)),
						 errhint("online block recovery could not rebuild this block from WAL "
								 "(no full-page-image base in retained WAL, an unsupported "
								 "change, or a cross-node block); restore from backup.")));
			}
#endif
			else
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("invalid page in block %u of relation %s",
								blockNum,
								relpath(smgr->smgr_rlocator, forkNum))));
		}
	}

	/*
	 * In RBM_ZERO_AND_LOCK / RBM_ZERO_AND_CLEANUP_LOCK mode, grab the buffer
	 * content lock before marking the page as valid, to make sure that no
	 * other backend sees the zeroed page before the caller has had a chance
	 * to initialize it.
	 *
	 * Since no-one else can be looking at the page contents yet, there is no
	 * difference between an exclusive lock and a cleanup-strength lock. (Note
	 * that we cannot use LockBuffer() or LockBufferForCleanup() here, because
	 * they assert that the buffer is already valid.)
	 */
	if ((mode == RBM_ZERO_AND_LOCK || mode == RBM_ZERO_AND_CLEANUP_LOCK) &&
		!isLocalBuf)
	{
#ifdef USE_PGRAC_CLUSTER
		cluster_bufmgr_pcm_gate_direct_init(
			bufHdr, CLUSTER_PCM_DIRECT_INIT_READ_MISS, &direct_init_proof);
#endif
		LWLockAcquire(BufferDescriptorGetContentLock(bufHdr), LW_EXCLUSIVE);
	}

	if (isLocalBuf)
	{
		/* Only need to adjust flags */
		uint32		buf_state = pg_atomic_read_u32(&bufHdr->state);

		buf_state |= BM_VALID;
		pg_atomic_unlocked_write_u32(&bufHdr->state, buf_state);
	}
	else
	{
		/* Set BM_VALID, terminate IO, and wake up any waiters */
		TerminateBufferIO(bufHdr, false, BM_VALID);
	}

	VacuumPageMiss++;
	if (VacuumCostActive)
		VacuumCostBalance += VacuumCostPageMiss;

	TRACE_POSTGRESQL_BUFFER_READ_DONE(forkNum, blockNum,
									  smgr->smgr_rlocator.locator.spcOid,
									  smgr->smgr_rlocator.locator.dbOid,
									  smgr->smgr_rlocator.locator.relNumber,
									  smgr->smgr_rlocator.backend,
									  found);

	return BufferDescriptorGetBuffer(bufHdr);
}

/*
 * BufferAlloc -- subroutine for ReadBuffer.  Handles lookup of a shared
 *		buffer.  If no buffer exists already, selects a replacement
 *		victim and evicts the old page, but does NOT read in new page.
 *
 * "strategy" can be a buffer replacement strategy object, or NULL for
 * the default strategy.  The selected buffer's usage_count is advanced when
 * using the default strategy, but otherwise possibly not (see PinBuffer).
 *
 * The returned buffer is pinned and is already marked as holding the
 * desired page.  If it already did have the desired page, *foundPtr is
 * set true.  Otherwise, *foundPtr is set false and the buffer is marked
 * as IO_IN_PROGRESS; ReadBuffer will now need to do I/O to fill it.
 *
 * *foundPtr is actually redundant with the buffer's BM_VALID flag, but
 * we keep it for simplicity in ReadBuffer.
 *
 * io_context is passed as an output parameter to avoid calling
 * IOContextForStrategy() when there is a shared buffers hit and no IO
 * statistics need be captured.
 *
 * No locks are held either at entry or exit.
 */
static BufferDesc *
BufferAlloc(SMgrRelation smgr, char relpersistence, ForkNumber forkNum,
			BlockNumber blockNum,
			BufferAccessStrategy strategy,
			bool *foundPtr, IOContext io_context)
{
	BufferTag	newTag;			/* identity of requested block */
	uint32		newHash;		/* hash value for newTag */
	LWLock	   *newPartitionLock;	/* buffer partition lock for it */
	int			existing_buf_id;
	Buffer		victim_buffer;
	BufferDesc *victim_buf_hdr;
	uint32		victim_buf_state;

	/* create a tag so we can lookup the buffer */
	InitBufferTag(&newTag, &smgr->smgr_rlocator.locator, forkNum, blockNum);

	/* determine its hash code and partition lock ID */
	newHash = BufTableHashCode(&newTag);
	newPartitionLock = BufMappingPartitionLock(newHash);

	/* see if the block is in the buffer pool already */
	LWLockAcquire(newPartitionLock, LW_SHARED);
	existing_buf_id = BufTableLookup(&newTag, newHash);
	if (existing_buf_id >= 0)
	{
		BufferDesc *buf;
		bool		valid;

		/*
		 * Found it.  Now, pin the buffer so no one can steal it from the
		 * buffer pool, and check to see if the correct data has been loaded
		 * into the buffer.
		 */
		buf = GetBufferDescriptor(existing_buf_id);

		valid = PinBuffer(buf, strategy);

		/* Can release the mapping lock as soon as we've pinned it */
		LWLockRelease(newPartitionLock);

		*foundPtr = true;

		if (!valid)
		{
			/*
			 * We can only get here if (a) someone else is still reading in
			 * the page, or (b) a previous read attempt failed.  We have to
			 * wait for any active read attempt to finish, and then set up our
			 * own read attempt if the page is still not BM_VALID.
			 * StartBufferIO does it all.
			 */
			if (StartBufferIO(buf, true))
			{
				/*
				 * If we get here, previous attempts to read the buffer must
				 * have failed ... but we shall bravely try again.
				 */
				*foundPtr = false;
			}
		}

		return buf;
	}

	/*
	 * Didn't find it in the buffer pool.  We'll have to initialize a new
	 * buffer.  Remember to unlock the mapping lock while doing the work.
	 */
	LWLockRelease(newPartitionLock);

	/*
	 * Acquire a victim buffer. Somebody else might try to do the same, we
	 * don't hold any conflicting locks. If so we'll have to undo our work
	 * later.
	 */
	victim_buffer = GetVictimBuffer(strategy, io_context);
	victim_buf_hdr = GetBufferDescriptor(victim_buffer - 1);

	/*
	 * Try to make a hashtable entry for the buffer under its new tag. If
	 * somebody else inserted another buffer for the tag, we'll release the
	 * victim buffer we acquired and use the already inserted one.
	 */
	LWLockAcquire(newPartitionLock, LW_EXCLUSIVE);
	existing_buf_id = BufTableInsert(&newTag, newHash, victim_buf_hdr->buf_id);
	if (existing_buf_id >= 0)
	{
		BufferDesc *existing_buf_hdr;
		bool		valid;

		/*
		 * Got a collision. Someone has already done what we were about to do.
		 * We'll just handle this as if it were found in the buffer pool in
		 * the first place.  First, give up the buffer we were planning to
		 * use.
		 *
		 * We could do this after releasing the partition lock, but then we'd
		 * have to call ResourceOwnerEnlargeBuffers() &
		 * ReservePrivateRefCountEntry() before acquiring the lock, for the
		 * rare case of such a collision.
		 */
		UnpinBuffer(victim_buf_hdr);

		/*
		 * The victim buffer we acquired previously is clean and unused, let
		 * it be found again quickly
		 */
		StrategyFreeBuffer(victim_buf_hdr);

		/* remaining code should match code at top of routine */

		existing_buf_hdr = GetBufferDescriptor(existing_buf_id);

		valid = PinBuffer(existing_buf_hdr, strategy);

		/* Can release the mapping lock as soon as we've pinned it */
		LWLockRelease(newPartitionLock);

		*foundPtr = true;

		if (!valid)
		{
			/*
			 * We can only get here if (a) someone else is still reading in
			 * the page, or (b) a previous read attempt failed.  We have to
			 * wait for any active read attempt to finish, and then set up our
			 * own read attempt if the page is still not BM_VALID.
			 * StartBufferIO does it all.
			 */
			if (StartBufferIO(existing_buf_hdr, true))
			{
				/*
				 * If we get here, previous attempts to read the buffer must
				 * have failed ... but we shall bravely try again.
				 */
				*foundPtr = false;
			}
		}

		return existing_buf_hdr;
	}

	/*
	 * Need to lock the buffer header too in order to change its tag.
	 */
	victim_buf_state = LockBufHdr(victim_buf_hdr);

	/* some sanity checks while we hold the buffer header lock */
	Assert(BUF_STATE_GET_REFCOUNT(victim_buf_state) == 1);
	Assert(!(victim_buf_state & (BM_TAG_VALID | BM_VALID | BM_DIRTY | BM_IO_IN_PROGRESS)));

	victim_buf_hdr->tag = newTag;

	/*
	 * Make sure BM_PERMANENT is set for buffers that must be written at every
	 * checkpoint.  Unlogged buffers only need to be written at shutdown
	 * checkpoints, except for their "init" forks, which need to be treated
	 * just like permanent relations.
	 */
	victim_buf_state |= BM_TAG_VALID | BUF_USAGECOUNT_ONE;
	if (relpersistence == RELPERSISTENCE_PERMANENT || forkNum == INIT_FORKNUM)
		victim_buf_state |= BM_PERMANENT;

	UnlockBufHdr(victim_buf_hdr, victim_buf_state);

	LWLockRelease(newPartitionLock);

	/*
	 * Buffer contents are currently invalid.  Try to obtain the right to
	 * start I/O.  If StartBufferIO returns false, then someone else managed
	 * to read it before we did, so there's nothing left for BufferAlloc() to
	 * do.
	 */
	if (StartBufferIO(victim_buf_hdr, true))
		*foundPtr = false;
	else
		*foundPtr = true;

	return victim_buf_hdr;
}

/*
 * InvalidateBuffer -- mark a shared buffer invalid and return it to the
 * freelist.
 *
 * The buffer header spinlock must be held at entry.  We drop it before
 * returning.  (This is sane because the caller must have locked the
 * buffer in order to be sure it should be dropped.)
 *
 * This is used only in contexts such as dropping a relation.  We assume
 * that no other backend could possibly be interested in using the page,
 * so the only reason the buffer might be pinned is if someone else is
 * trying to write it out.  We have to let them finish before we can
 * reclaim the buffer.
 *
 * The buffer could get reclaimed by someone else while we are waiting
 * to acquire the necessary locks; if so, don't mess it up.
 *
 * PGRAC modifications by SqlRush:
 *   What changed: the commit tail (clear tag/flags -> eviction hook ->
 *   BufTableDelete -> StrategyFreeBuffer) is extracted into
 *   InvalidateBufferCommitLocked so the GCS serve-stall round-5
 *   InvalidateBufferTry variant below can share it verbatim.  The retry
 *   loop and every effect of this function are unchanged.
 *   Why: the LMS DATA dispatch pump must never enter this function's
 *   unbounded pin-wait loop (a foreign pin held by a backend that is
 *   itself waiting on a GCS reply this pump would deliver = a circular
 *   wait resolved only by the reply-wait timeout — the measured 33-96s
 *   S3 serve-stall wall).
 */
static void
InvalidateBuffer(BufferDesc *buf)
{
	BufferTag	oldTag;
	uint32		oldHash;		/* hash value for oldTag */
	LWLock	   *oldPartitionLock;	/* buffer partition lock for it */
	uint32		buf_state;

	/* Save the original buffer tag before dropping the spinlock */
	oldTag = buf->tag;

	buf_state = pg_atomic_read_u32(&buf->state);
	Assert(buf_state & BM_LOCKED);
	UnlockBufHdr(buf, buf_state);

	/*
	 * Need to compute the old tag's hashcode and partition lock ID. XXX is it
	 * worth storing the hashcode in BufferDesc so we need not recompute it
	 * here?  Probably not.
	 */
	oldHash = BufTableHashCode(&oldTag);
	oldPartitionLock = BufMappingPartitionLock(oldHash);

retry:

	/*
	 * Acquire exclusive mapping lock in preparation for changing the buffer's
	 * association.
	 */
	LWLockAcquire(oldPartitionLock, LW_EXCLUSIVE);

	/* Re-lock the buffer header */
	buf_state = LockBufHdr(buf);

	/* If it's changed while we were waiting for lock, do nothing */
	if (!BufferTagsEqual(&buf->tag, &oldTag))
	{
		UnlockBufHdr(buf, buf_state);
		LWLockRelease(oldPartitionLock);
		return;
	}

	/*
	 * We assume the only reason for it to be pinned is that someone else is
	 * flushing the page out.  Wait for them to finish.  (This could be an
	 * infinite loop if the refcount is messed up... it would be nice to time
	 * out after awhile, but there seems no way to be sure how many loops may
	 * be needed.  Note that if the other guy has pinned the buffer but not
	 * yet done StartBufferIO, WaitIO will fall through and we'll effectively
	 * be busy-looping here.)
	 */
	if (BUF_STATE_GET_REFCOUNT(buf_state) != 0)
	{
		UnlockBufHdr(buf, buf_state);
		LWLockRelease(oldPartitionLock);
		/* safety check: should definitely not be our *own* pin */
		if (GetPrivateRefCount(BufferDescriptorGetBuffer(buf)) > 0)
			elog(ERROR, "buffer is pinned in InvalidateBuffer");
		WaitIO(buf);
		goto retry;
	}

	if (!InvalidateBufferCommitLocked(buf, &oldTag, oldHash, oldPartitionLock, buf_state))
	{
		pg_usleep(1000L);
		goto retry;
	}
}

/*
 * InvalidateBufferCommitLocked -- shared commit tail of InvalidateBuffer
 * and InvalidateBufferTry.
 *
 * Entry: buffer header spinlock held, oldPartitionLock held EXCLUSIVE,
 * tag verified equal to *oldTag, refcount known zero.  Releases both
 * locks.  This is the original InvalidateBuffer tail, extracted verbatim
 * (PGRAC GCS serve-stall round-5; see the InvalidateBuffer header note).
 */
static bool
InvalidateBufferCommitLocked(BufferDesc *buf, BufferTag *oldTag, uint32 oldHash,
								 LWLock *oldPartitionLock, uint32 buf_state)
{
	uint8		old_pcm_mode = 0;
	bool		release_pcm_holder = false;
#ifdef USE_PGRAC_CLUSTER
	ClusterPcmOwnEvictionCapture eviction_capture;
	ClusterPcmOwnResult eviction_result;
	uint32		observed_flags = 0;
	uint64		observed_generation = 0;

	/*
	 * D5a: descriptor reuse is an exact ownership-tuple commit.  Refuse to
	 * erase the tag while a reservation/revoke is live, at generation MAX, or
	 * after any tuple drift.  The buffer cannot enter StrategyFreeBuffer
	 * until this commit and (when applicable) the saved-tag master release
	 * succeed.
	 */
	cluster_pcm_own_eviction_capture_locked(buf, &eviction_capture);
	eviction_result = cluster_pcm_own_eviction_commit_locked(
		buf, &eviction_capture, &observed_generation, &observed_flags);
	if (eviction_result != CLUSTER_PCM_OWN_OK)
	{
		UnlockBufHdr(buf, buf_state);
		LWLockRelease(oldPartitionLock);
		if (eviction_result == CLUSTER_PCM_OWN_BUSY || eviction_result == CLUSTER_PCM_OWN_STALE)
			return false;
		cluster_pcm_own_report_bump_failure(buf, eviction_result,
										observed_generation != 0 ? observed_generation
																 : eviction_capture.generation,
										observed_flags != 0 ? observed_flags : eviction_capture.flags,
										"buffer eviction");
	}
	old_pcm_mode = eviction_capture.pcm_state;
	release_pcm_holder = cluster_pcm_is_active()
		&& cluster_bufmgr_reln_pcm_tracked(BufTagGetRelNumber(oldTag))
		&& (old_pcm_mode == (uint8) PCM_LOCK_MODE_S || old_pcm_mode == (uint8) PCM_LOCK_MODE_X);
#endif

	InvalidateBufferCommitTailLocked(buf, oldTag, oldHash, oldPartitionLock, buf_state,
									 old_pcm_mode, release_pcm_holder);
	return true;
}

/*
 * InvalidateBufferCommitTailLocked -- remove a mapping after its ownership
 * tuple has already been committed under the current header-lock hold.
 *
 * The normal D5a path above and the exact queue revoke path share this tail.
 * The latter passes N/false because its master-visible release is driven by
 * the queue protocol, not the cache-eviction wire.
 */
static void
InvalidateBufferCommitTailLocked(BufferDesc *buf, BufferTag *oldTag, uint32 oldHash,
								 LWLock *oldPartitionLock, uint32 buf_state,
								 uint8 old_pcm_mode, bool release_pcm_holder)
{
	uint32		oldFlags;

	/*
	 * Clear out the buffer's tag and flags.  We must do this to ensure that
	 * linear scans of the buffer array don't think the buffer is valid.
	 */
	oldFlags = buf_state & BUF_FLAG_MASK;
	ClearBufferTag(&buf->tag);
	buf_state &= ~(BUF_FLAG_MASK | BUF_USAGECOUNT_MASK);
#ifdef USE_PGRAC_CLUSTER

	/*
	 * PGRAC: spec-6.12h D-h3a — the mapping dies here; reset the cluster
	 * copy label under the same header-lock hold so a later retag can never
	 * inherit a stale BUF_TYPE_PI (the PI shape must stay reachable ONLY
	 * through cluster_bufmgr_convert_to_pi_locked, or the D-h3 shadow stamp
	 * could be paired with another residency's bytes). */
	buf->buffer_type = (uint8) BUF_TYPE_CURRENT;
#endif
	UnlockBufHdr(buf, buf_state);

	/*
	 * Remove the buffer from the lookup hashtable, if it was in there.
	 */
	if (oldFlags & BM_TAG_VALID)
		BufTableDelete(oldTag, oldHash);

	/*
	 * Done with mapping lock.
	 */
	LWLockRelease(oldPartitionLock);
#ifdef USE_PGRAC_CLUSTER

	/*
	 * The descriptor tag is intentionally already cleared.  Releasing by the
	 * immutable capture avoids the old temporary tag restoration race.  On a
	 * throwing release the buffer is absent from the mapping table but is not
	 * yet reusable, which is the fail-closed state. */
	if (release_pcm_holder)
	{
		PG_TRY();
		{
			cluster_pcm_lock_release_saved_tag_for_eviction(*oldTag, (PcmLockMode) old_pcm_mode);
		}
		PG_CATCH();
		{
			elog(LOG,
				 "cluster PCM saved-tag eviction release failed for buffer %d mode=%d; "
				 "old mapping is removed and descriptor was not returned to the freelist",
				 buf->buf_id, (int) old_pcm_mode);
			PG_RE_THROW();
		}
		PG_END_TRY();
	}
#endif

	/*
	 * Insert the buffer at the head of the list of free buffers.
	 */
	StrategyFreeBuffer(buf);
}

/*
 * InvalidateBufferTry -- bounded, non-waiting variant of InvalidateBuffer
 * (PGRAC GCS serve-stall round-5).
 *
 * Same entry contract as InvalidateBuffer (buffer header spinlock held;
 * dropped before returning) and the same effects on success, but a
 * foreign pin FAILS the call instead of entering the pin-wait retry loop:
 * the IC dispatch pump (LMS / LMON context) must never block on a pin
 * whose holder may itself be waiting on a reply only this pump can
 * deliver.  Returns true when the buffer was invalidated OR was already
 * re-tagged (nothing left to invalidate — same silent-success as
 * InvalidateBuffer's tag-changed arm);  false when a pin held it and
 * NOTHING was changed (the caller parks the job and retries later, or
 * fail-closes with a retryable deny).
 */
static bool
InvalidateBufferTry(BufferDesc *buf)
{
	BufferTag	oldTag;
	uint32		oldHash;		/* hash value for oldTag */
	LWLock	   *oldPartitionLock;	/* buffer partition lock for it */
	uint32		buf_state;

	/* Save the original buffer tag before dropping the spinlock */
	oldTag = buf->tag;

	buf_state = pg_atomic_read_u32(&buf->state);
	Assert(buf_state & BM_LOCKED);
	UnlockBufHdr(buf, buf_state);

	oldHash = BufTableHashCode(&oldTag);
	oldPartitionLock = BufMappingPartitionLock(oldHash);

#ifdef USE_PGRAC_CLUSTER

	/*
	 * PGRAC ownership-generation wave (W2) — drop-prepin window.  The
	 * header spinlock was released above and the partition lock is not yet
	 * taken, so this is the only gap where a foreign pin can slip in and fail
	 * the refcount recheck below (a continuously-held pin is caught by the
	 * drop's entry gates BEFORE staging N and never reaches this function).
	 * The sleep inject holds the gap open so the W2 RED can place that pin
	 * deterministically.  Gated to GCS drops only — plain evictions must
	 * not stall.  No lock is held across the sleep.
	 */
	if (cluster_bufmgr_in_gcs_drop
		&& BufTagGetForkNum(&oldTag) == MAIN_FORKNUM)
		CLUSTER_INJECTION_POINT("cluster-pcm-drop-prepin-window");
#endif

	/*
	 * Acquire exclusive mapping lock in preparation for changing the buffer's
	 * association.  Single attempt — no retry loop.
	 */
	LWLockAcquire(oldPartitionLock, LW_EXCLUSIVE);

	buf_state = LockBufHdr(buf);

	/* If it's changed while we were waiting for lock, do nothing */
	if (!BufferTagsEqual(&buf->tag, &oldTag))
	{
		UnlockBufHdr(buf, buf_state);
		LWLockRelease(oldPartitionLock);
		return true;
	}

	if (BUF_STATE_GET_REFCOUNT(buf_state) != 0)
	{
		UnlockBufHdr(buf, buf_state);
		LWLockRelease(oldPartitionLock);
		/* safety check: should definitely not be our *own* pin */
		if (GetPrivateRefCount(BufferDescriptorGetBuffer(buf)) > 0)
			elog(ERROR, "buffer is pinned in InvalidateBufferTry");
		return false;			/* foreign pin — caller parks / fail-closes */
	}

	return InvalidateBufferCommitLocked(buf, &oldTag, oldHash, oldPartitionLock, buf_state);
}

/*
 * Helper routine for GetVictimBuffer()
 *
 * Needs to be called on a buffer with a valid tag, pinned, but without the
 * buffer header spinlock held.
 *
 * Returns true if the buffer can be reused, in which case the buffer is only
 * pinned by this backend and marked as invalid, false otherwise.
 */
static bool
InvalidateVictimBuffer(BufferDesc *buf_hdr)
{
	uint32		buf_state;
	uint32		hash;
	LWLock	   *partition_lock;
	BufferTag	tag;

	Assert(GetPrivateRefCount(BufferDescriptorGetBuffer(buf_hdr)) == 1);

	/* have buffer pinned, so it's safe to read tag without lock */
	tag = buf_hdr->tag;

	hash = BufTableHashCode(&tag);
	partition_lock = BufMappingPartitionLock(hash);

	LWLockAcquire(partition_lock, LW_EXCLUSIVE);

	/* lock the buffer header */
	buf_state = LockBufHdr(buf_hdr);

	/*
	 * We have the buffer pinned nobody else should have been able to unset
	 * this concurrently.
	 */
	Assert(buf_state & BM_TAG_VALID);
	Assert(BUF_STATE_GET_REFCOUNT(buf_state) > 0);
	Assert(BufferTagsEqual(&buf_hdr->tag, &tag));

#ifdef USE_PGRAC_CLUSTER
	/* PCM-X retained images deliberately keep BM_VALID and may still have this
	 * victim pin.  The live REVOKING token, not passive refcount, is the reuse
	 * authority; never let the clock-sweep shortcut bypass D5a. */
	if (cluster_bufmgr_pcm_x_retained_image_reuse_blocked_locked(buf_hdr, buf_state))
	{
		UnlockBufHdr(buf_hdr, buf_state);
		LWLockRelease(partition_lock);
		return false;
	}
#endif

	/*
	 * If somebody else pinned the buffer since, or even worse, dirtied it,
	 * give up on this buffer: It's clearly in use.
	 */
	if (BUF_STATE_GET_REFCOUNT(buf_state) != 1 || (buf_state & BM_DIRTY))
	{
		Assert(BUF_STATE_GET_REFCOUNT(buf_state) > 0);

		UnlockBufHdr(buf_hdr, buf_state);
		LWLockRelease(partition_lock);

		return false;
	}

	/*
	 * Clear out the buffer's tag and flags and usagecount.  This is not
	 * strictly required, as BM_TAG_VALID/BM_VALID needs to be checked before
	 * doing anything with the buffer. But currently it's beneficial, as the
	 * cheaper pre-check for several linear scans of shared buffers use the
	 * tag (see e.g. FlushDatabaseBuffers()).
	 */
	ClearBufferTag(&buf_hdr->tag);
	buf_state &= ~(BUF_FLAG_MASK | BUF_USAGECOUNT_MASK);
#ifdef USE_PGRAC_CLUSTER

	/*
	 * PGRAC: spec-6.12h D-h3a — victim reuse retags this buffer; reset the
	 * cluster copy label under the header lock (same stale-BUF_TYPE_PI
	 * containment as InvalidateBuffer above). */
	buf_hdr->buffer_type = (uint8) BUF_TYPE_CURRENT;
#endif
	UnlockBufHdr(buf_hdr, buf_state);

	Assert(BUF_STATE_GET_REFCOUNT(buf_state) > 0);

#ifdef USE_PGRAC_CLUSTER

	/*
	 * PGRAC: spec-2.35 D4 (HC112) — cache eviction hook for victim buffer.
	 * LRU selected this buffer + we already confirmed it is reusable. Drop
	 * the cache-residency bit + propagate master_holder lifecycle (HC110)
	 * before BufTableDelete completes the eviction.
	 */
	if (cluster_pcm_is_active()
		&& cluster_bufmgr_reln_pcm_tracked(BufTagGetRelNumber(&tag))
		&& buf_hdr->pcm_state != (uint8) PCM_STATE_N)
	{
		PcmLockMode old_mode = (PcmLockMode) buf_hdr->pcm_state;

		buf_hdr->tag = tag;	/* restore for release helper (cleared above) */
		cluster_pcm_lock_release_buffer_for_eviction(buf_hdr, old_mode);
		ClearBufferTag(&buf_hdr->tag);

		/*
		 * PGRAC ownership-gen: coherent N-flip + generation bump (the header
		 * spinlock was dropped above at UnlockBufHdr) so a buf_id reuse after
		 * this victim eviction cannot alias a stale captured generation. */
		cluster_pcm_own_transition(buf_hdr, (uint8) PCM_STATE_N, 0,
								   PCM_OWN_FLAG_GRANT_PENDING | PCM_OWN_FLAG_REVOKING);
	}
#endif

	/* finally delete buffer from the buffer mapping table */
	BufTableDelete(&tag, hash);

	LWLockRelease(partition_lock);

	Assert(!(buf_state & (BM_DIRTY | BM_VALID | BM_TAG_VALID)));
	Assert(BUF_STATE_GET_REFCOUNT(buf_state) > 0);
	Assert(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&buf_hdr->state)) > 0);

	return true;
}

static Buffer
GetVictimBuffer(BufferAccessStrategy strategy, IOContext io_context)
{
	BufferDesc *buf_hdr;
	Buffer		buf;
	uint32		buf_state;
	bool		from_ring;

	/*
	 * Ensure, while the spinlock's not yet held, that there's a free refcount
	 * entry.
	 */
	ReservePrivateRefCountEntry();
	ResourceOwnerEnlargeBuffers(CurrentResourceOwner);

	/* we return here if a prospective victim buffer gets used concurrently */
again:

	/*
	 * Select a victim buffer.  The buffer is returned with its header
	 * spinlock still held!
	 */
	buf_hdr = StrategyGetBuffer(strategy, &buf_state, &from_ring);
	buf = BufferDescriptorGetBuffer(buf_hdr);

	Assert(BUF_STATE_GET_REFCOUNT(buf_state) == 0);

	/* Pin the buffer and then release the buffer spinlock */
	PinBuffer_Locked(buf_hdr);

	/*
	 * We shouldn't have any other pins for this buffer.
	 */
	CheckBufferIsPinnedOnce(buf);

	/*
	 * If the buffer was dirty, try to write it out.  There is a race
	 * condition here, in that someone might dirty it after we released the
	 * buffer header lock above, or even while we are writing it out (since
	 * our share-lock won't prevent hint-bit updates).  We will recheck the
	 * dirty bit after re-locking the buffer header.
	 */
	if (buf_state & BM_DIRTY)
	{
		LWLock	   *content_lock;

		Assert(buf_state & BM_TAG_VALID);
		Assert(buf_state & BM_VALID);

		/*
		 * We need a share-lock on the buffer contents to write it out (else
		 * we might write invalid data, eg because someone else is compacting
		 * the page contents while we write).  We must use a conditional lock
		 * acquisition here to avoid deadlock.  Even though the buffer was not
		 * pinned (and therefore surely not locked) when StrategyGetBuffer
		 * returned it, someone else could have pinned and exclusive-locked it
		 * by the time we get here. If we try to get the lock unconditionally,
		 * we'd block waiting for them; if they later block waiting for us,
		 * deadlock ensues. (This has been observed to happen when two
		 * backends are both trying to split btree index pages, and the second
		 * one just happens to be trying to split the page the first one got
		 * from StrategyGetBuffer.)
		 */
		content_lock = BufferDescriptorGetContentLock(buf_hdr);
		if (!LWLockConditionalAcquire(content_lock, LW_SHARED))
		{
			/*
			 * Someone else has locked the buffer, so give it up and loop back
			 * to get another one.
			 */
			UnpinBuffer(buf_hdr);
			goto again;
		}

		/*
		 * If using a nondefault strategy, and writing the buffer would
		 * require a WAL flush, let the strategy decide whether to go ahead
		 * and write/reuse the buffer or to choose another victim.  We need a
		 * lock to inspect the page LSN, so this can't be done inside
		 * StrategyGetBuffer.
		 */
		if (strategy != NULL)
		{
			XLogRecPtr	lsn;

			/* Read the LSN while holding buffer header lock */
			buf_state = LockBufHdr(buf_hdr);
			lsn = BufferGetLSN(buf_hdr);
			UnlockBufHdr(buf_hdr, buf_state);

			if (XLogNeedsFlush(lsn)
				&& StrategyRejectBuffer(strategy, buf_hdr, from_ring))
			{
				LWLockRelease(content_lock);
				UnpinBuffer(buf_hdr);
				goto again;
			}
		}

		/* OK, do the I/O */
		FlushBuffer(buf_hdr, NULL, IOOBJECT_RELATION, io_context);
		LWLockRelease(content_lock);

		ScheduleBufferTagForWriteback(&BackendWritebackContext, io_context,
									  &buf_hdr->tag);
	}


	if (buf_state & BM_VALID)
	{
		/*
		 * When a BufferAccessStrategy is in use, blocks evicted from shared
		 * buffers are counted as IOOP_EVICT in the corresponding context
		 * (e.g. IOCONTEXT_BULKWRITE). Shared buffers are evicted by a
		 * strategy in two cases: 1) while initially claiming buffers for the
		 * strategy ring 2) to replace an existing strategy ring buffer
		 * because it is pinned or in use and cannot be reused.
		 *
		 * Blocks evicted from buffers already in the strategy ring are
		 * counted as IOOP_REUSE in the corresponding strategy context.
		 *
		 * At this point, we can accurately count evictions and reuses,
		 * because we have successfully claimed the valid buffer. Previously,
		 * we may have been forced to release the buffer due to concurrent
		 * pinners or erroring out.
		 */
		pgstat_count_io_op(IOOBJECT_RELATION, io_context,
						   from_ring ? IOOP_REUSE : IOOP_EVICT);
	}

	/*
	 * If the buffer has an entry in the buffer mapping table, delete it. This
	 * can fail because another backend could have pinned or dirtied the
	 * buffer.
	 */
	if ((buf_state & BM_TAG_VALID) && !InvalidateVictimBuffer(buf_hdr))
	{
		UnpinBuffer(buf_hdr);
		goto again;
	}

	/* a final set of sanity checks */
#ifdef USE_ASSERT_CHECKING
	buf_state = pg_atomic_read_u32(&buf_hdr->state);

	Assert(BUF_STATE_GET_REFCOUNT(buf_state) == 1);
	Assert(!(buf_state & (BM_TAG_VALID | BM_VALID | BM_DIRTY)));

	CheckBufferIsPinnedOnce(buf);
#endif

	return buf;
}

/*
 * Limit the number of pins a batch operation may additionally acquire, to
 * avoid running out of pinnable buffers.
 *
 * One additional pin is always allowed, as otherwise the operation likely
 * cannot be performed at all.
 *
 * The number of allowed pins for a backend is computed based on
 * shared_buffers and the maximum number of connections possible. That's very
 * pessimistic, but outside of toy-sized shared_buffers it should allow
 * sufficient pins.
 */
static void
LimitAdditionalPins(uint32 *additional_pins)
{
	uint32		max_backends;
	int			max_proportional_pins;

	if (*additional_pins <= 1)
		return;

	max_backends = MaxBackends + NUM_AUXILIARY_PROCS;
	max_proportional_pins = NBuffers / max_backends;

	/*
	 * Subtract the approximate number of buffers already pinned by this
	 * backend. We get the number of "overflowed" pins for free, but don't
	 * know the number of pins in PrivateRefCountArray. The cost of
	 * calculating that exactly doesn't seem worth it, so just assume the max.
	 */
	max_proportional_pins -= PrivateRefCountOverflowed + REFCOUNT_ARRAY_ENTRIES;

	if (max_proportional_pins <= 0)
		max_proportional_pins = 1;

	if (*additional_pins > max_proportional_pins)
		*additional_pins = max_proportional_pins;
}

/*
 * Logic shared between ExtendBufferedRelBy(), ExtendBufferedRelTo(). Just to
 * avoid duplicating the tracing and relpersistence related logic.
 */
static BlockNumber
ExtendBufferedRelCommon(BufferManagerRelation bmr,
						ForkNumber fork,
						BufferAccessStrategy strategy,
						uint32 flags,
						uint32 extend_by,
						BlockNumber extend_upto,
						Buffer *buffers,
						uint32 *extended_by)
{
	BlockNumber first_block;

	TRACE_POSTGRESQL_BUFFER_EXTEND_START(fork,
										 bmr.smgr->smgr_rlocator.locator.spcOid,
										 bmr.smgr->smgr_rlocator.locator.dbOid,
										 bmr.smgr->smgr_rlocator.locator.relNumber,
										 bmr.smgr->smgr_rlocator.backend,
										 extend_by);

	if (bmr.relpersistence == RELPERSISTENCE_TEMP)
		first_block = ExtendBufferedRelLocal(bmr, fork, flags,
											 extend_by, extend_upto,
											 buffers, &extend_by);
	else
		first_block = ExtendBufferedRelShared(bmr, fork, strategy, flags,
											  extend_by, extend_upto,
											  buffers, &extend_by);
	*extended_by = extend_by;

	TRACE_POSTGRESQL_BUFFER_EXTEND_DONE(fork,
										bmr.smgr->smgr_rlocator.locator.spcOid,
										bmr.smgr->smgr_rlocator.locator.dbOid,
										bmr.smgr->smgr_rlocator.locator.relNumber,
										bmr.smgr->smgr_rlocator.backend,
										*extended_by,
										first_block);

	return first_block;
}

/*
 * Implementation of ExtendBufferedRelBy() and ExtendBufferedRelTo() for
 * shared buffers.
 */
static BlockNumber
ExtendBufferedRelShared(BufferManagerRelation bmr,
						ForkNumber fork,
						BufferAccessStrategy strategy,
						uint32 flags,
						uint32 extend_by,
						BlockNumber extend_upto,
						Buffer *buffers,
						uint32 *extended_by)
{
	BlockNumber first_block;
	IOContext	io_context = IOContextForStrategy(strategy);
	instr_time	io_start;

#ifdef USE_PGRAC_CLUSTER
	/*----------
	 * PGRAC MODIFICATIONS — spec-5.7 HW (relation-extend block-number authority).
	 *
	 * The PG-native extend below sources the block number from the local file
	 * size (first_block = smgrnblocks).  In a multi-node cluster that is unsafe:
	 * two nodes can read the same stale-low size (non-coherent storage, L368)
	 * and allocate the same block range -> silent corruption.  For a GLOBALIZE
	 * relation the range comes from the cluster authority (cluster_hw_allocate
	 * under HW(X)) instead.
	 *
	 * Scope (only the genuinely racy case is globalized; everything else stays
	 * PG-native):
	 *   - fork == MAIN_FORKNUM only.  FSM/VM forks use positional extend-to (a
	 *     block addresses a fixed heap range) and are page-coordinated by Cache
	 *     Fusion, so the sequential authority does not apply to them.
	 *   - runtime-multi-node only (cluster_extend_liveness_engage, spec-5.7
	 *     §3.1d): engage the authority only when a peer is actually alive and the
	 *     GES/LMS substrate is ready.  A single-alive / degraded / single-node-
	 *     compat node extends PG-natively (no cross-node coherence problem, and
	 *     avoids a per-extend WAL flush) instead of fail-closing on the static
	 *     configured node count.
	 *   - live backend extends only (bmr.rel != NULL && !RecoveryInProgress());
	 *     recovery redo replays deterministic block numbers from WAL and rebuilds
	 *     the authority via HW_RESERVE redo.
	 * An unlogged relation in a multi-node cluster classifies FAIL_CLOSED and is
	 * rejected here (53RA6), before any buffer/lock work.
	 *----------
	 */
	ClusterHwClass hwc = CLUSTER_HW_NATIVE_LOCAL;
	HwLock		hwlk;
	ClusterResId hw_resid;
	uint32		hw_lease_tail = 0;	/* spec-6.12d: parked grant tail */

	if (cluster_relation_extend_lock_enabled && bmr.rel != NULL && cluster_node_id >= 0
		&& fork == MAIN_FORKNUM && !RecoveryInProgress()
		&& bmr.rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP)
	{
		/*
		 * Decide whether to engage the cross-node authority from runtime
		 * liveness, not the static configured node count (spec-5.7 §3.1d,
		 * v1.5 amend).  HW passes wait_for_lms = true so a DML waits out a
		 * brief LMS warmup instead of failing.  Temp relations are excluded
		 * above (always PG-local).
		 */
		ClusterExtendEngage engage = cluster_extend_liveness_engage(true);

		if (engage == CLUSTER_EXTEND_ENGAGE_FAIL_CLOSED)
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_RELATION_EXTEND_UNAVAILABLE),
					 errmsg("could not acquire the cluster relation-extend lock for \"%s\"",
							RelationGetRelationName(bmr.rel)),
					 errdetail("The cluster coordination substrate is not ready and an alive peer could not be ruled out.")));

		if (engage == CLUSTER_EXTEND_ENGAGE_COORDINATE)
		{
			/*
			 * A peer is alive and the substrate is ready: a permanent
			 * relation is GLOBALIZE (authority-owned from block 0); an
			 * unlogged relation has no WAL authority and cannot be
			 * coordinated -> fail closed.
			 */
			hwc = cluster_hw_classify_persistence(bmr.rel->rd_rel->relpersistence, true);
			if (hwc == CLUSTER_HW_FAIL_CLOSED)
				ereport(ERROR,
						(errcode(ERRCODE_CLUSTER_RELATION_EXTEND_UNAVAILABLE),
						 errmsg("unlogged relation \"%s\" cannot be safely extended in a multi-node cluster",
								RelationGetRelationName(bmr.rel)),
						 errhint("Unlogged relations have no WAL authority to coordinate cross-node extension.")));
		}

		/*
		 * engage == NATIVE: no alive peer to coordinate with -> hwc stays
		 * CLUSTER_HW_NATIVE_LOCAL and the relation extends PG-natively.
		 */
	}
#endif

	LimitAdditionalPins(&extend_by);

	/*
	 * Acquire victim buffers for extension without holding extension lock.
	 * Writing out victim buffers is the most expensive part of extending the
	 * relation, particularly when doing so requires WAL flushes. Zeroing out
	 * the buffers is also quite expensive, so do that before holding the
	 * extension lock as well.
	 *
	 * These pages are pinned by us and not valid. While we hold the pin they
	 * can't be acquired as victim buffers by another backend.
	 */
	for (uint32 i = 0; i < extend_by; i++)
	{
		Block		buf_block;

		buffers[i] = GetVictimBuffer(strategy, io_context);
		buf_block = BufHdrGetBlock(GetBufferDescriptor(buffers[i] - 1));

		/* new buffers are zero-filled */
		MemSet((char *) buf_block, 0, BLCKSZ);
	}

	/* in case we need to pin an existing buffer below */
	ResourceOwnerEnlargeBuffers(CurrentResourceOwner);

	/*
	 * Lock relation against concurrent extensions, unless requested not to.
	 *
	 * We use the same extension lock for all forks. That's unnecessarily
	 * restrictive, but currently extensions for forks don't happen often
	 * enough to make it worth locking more granularly.
	 *
	 * Note that another backend might have extended the relation by the time
	 * we get the lock.
	 */
#ifdef USE_PGRAC_CLUSTER

	/*
	 * §3.1a M1a lock order: HW(X) (cross-node) BEFORE
	 * LockRelationForExtension (local), so a backend never holds the local
	 * extension lock while waiting cross-node.  DL (the bulk-load lease,
	 * spec-5.7 D4) is already held by the caller above when this is a bulk
	 * load.  HW is held only across the allocate; it is a synthetic resid
	 * (not auto-released by LockReleaseAll), so the brief window relies on
	 * reconfig-revoke (AD-013) + backend-exit cleanup as the backstop on an
	 * error path, mirroring CF (spec-5.6).
	 */
	if (hwc == CLUSTER_HW_GLOBALIZE)
	{
		cluster_hw_resid_encode(RelationGetSmgr(bmr.rel)->smgr_rlocator.locator, fork, &hw_resid);
		if (!cluster_hw_lock(&hw_resid, &hwlk))
		{
			/* release the victim buffers pinned above (the extension lock is not
			 * held yet) before failing closed, symmetric with the allocate-fail
			 * path below (review P2). */
			for (uint32 i = 0; i < extend_by; i++)
			{
				BufferDesc *buf_hdr = GetBufferDescriptor(buffers[i] - 1);

				StrategyFreeBuffer(buf_hdr);
				UnpinBuffer(buf_hdr);
			}
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_RELATION_EXTEND_UNAVAILABLE),
					 errmsg("could not acquire the cluster relation-extend lock for \"%s\"",
							RelationGetRelationName(bmr.rel))));
		}
	}
#endif

	if (!(flags & EB_SKIP_EXTENSION_LOCK))
	{
		LockRelationForExtension(bmr.rel, ExclusiveLock);
		if (bmr.rel)
			bmr.smgr = RelationGetSmgr(bmr.rel);
	}

	/*
	 * If requested, invalidate size cache, so that smgrnblocks asks the
	 * kernel.
	 */
	if (flags & EB_CLEAR_SIZE_CACHE)
		bmr.smgr->smgr_cached_nblocks[fork] = InvalidBlockNumber;

#ifdef USE_PGRAC_CLUSTER
	if (hwc == CLUSTER_HW_GLOBALIZE)
	{
		uint32		hw_granted = 0;
		BlockNumber seed_nblocks;

		/*
		 * §3.1c Q14/Q17 establishment seed: a FORCED re-stat of the fork's
		 * real size (invalidate the smgr_cached_nblocks cache first so
		 * smgrnblocks re-stats the shared file, not a stale cache).  We hold
		 * HW(X), so the size is quiescent (no concurrent authority extend)
		 * and -- on cluster_fs -- the shared file's EOF is physically
		 * coherent (Q14).  The master uses this ONLY at first establishment,
		 * to own a relation whose first blocks were created by a
		 * non-authority path (private build, sequence create) from its true
		 * EOF; for an established resid the master ignores it and the
		 * authority counter is the sole source (FileSize never read again).
		 */
		bmr.smgr->smgr_cached_nblocks[fork] = InvalidBlockNumber;
		seed_nblocks = smgrnblocks(bmr.smgr, fork);

		/*
		 * The block range is the cluster authority's, not the file size.
		 * HW(X) is released right after the allocate: the granted ranges are
		 * disjoint across nodes, so smgrzeroextend of distinct ranges is safe
		 * without it held.  The authority may grant fewer blocks than
		 * requested; release the surplus victim buffers (mirroring the
		 * extend_upto path below).
		 *
		 * PGRAC: spec-6.12d -- with static space affinity on, a plain-heap
		 * extend over-asks the master (up to cluster.space_lease_blocks) so
		 * the unconsumed tail of the grant can be parked as this node's
		 * private lease: later writers on this node consume it without a
		 * master round-trip (Q17-A extent-interior per-instance grouping).
		 * Restricted to relkind 'r' main-fork extends without extend_upto,
		 * the only shape the hio.c lease consumer serves.
		 */
		{
			uint32		hw_want = extend_by;

			if (cluster_hw_lease_active() &&
				extend_upto == InvalidBlockNumber &&
				bmr.rel->rd_rel->relkind == RELKIND_RELATION &&
				(uint32) cluster_space_lease_blocks > extend_by)
				hw_want = (uint32) cluster_space_lease_blocks;

			first_block = cluster_hw_allocate(RelationGetSmgr(bmr.rel)->smgr_rlocator.locator, fork,
											  hw_want, seed_nblocks, &hw_granted);
		}
		cluster_hw_unlock(&hwlk);

		if (first_block == InvalidBlockNumber || hw_granted == 0)
		{
			/* fail closed: release the extension lock + all victim buffers, then
			 * raise 53RA6 (we are outside any critical section). */
			if (!(flags & EB_SKIP_EXTENSION_LOCK))
				UnlockRelationForExtension(bmr.rel, ExclusiveLock);
			for (uint32 i = 0; i < extend_by; i++)
			{
				BufferDesc *buf_hdr = GetBufferDescriptor(buffers[i] - 1);

				StrategyFreeBuffer(buf_hdr);
				UnpinBuffer(buf_hdr);
			}
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_RELATION_EXTEND_UNAVAILABLE),
					 errmsg("cluster relation-extend authority unavailable for \"%s\"",
							RelationGetRelationName(bmr.rel)),
					 errhint("The HW_ALLOC round trip to the resource master could not be proven; retry.")));
		}

		if (hw_granted < extend_by)
		{
			for (uint32 i = hw_granted; i < extend_by; i++)
			{
				BufferDesc *buf_hdr = GetBufferDescriptor(buffers[i] - 1);

				StrategyFreeBuffer(buf_hdr);
				UnpinBuffer(buf_hdr);
			}
			extend_by = hw_granted;
		}
		else
		{
			/*
			 * PGRAC: spec-6.12d -- the over-ask surplus becomes this node's
			 * lease.  It MUST also be zero-extended below: the HWM advance
			 * for the whole grant is already durable, so leaving the tail
			 * beyond EOF would break the dense-file contract (Q17-A) for
			 * every EOF consumer.  Parked after the physical extension
			 * succeeds.
			 */
			hw_lease_tail = hw_granted - extend_by;
		}
	}
	else
#endif
	first_block = smgrnblocks(bmr.smgr, fork);

	/*
	 * Now that we have the accurate relation size, check if the caller wants
	 * us to extend to only up to a specific size. If there were concurrent
	 * extensions, we might have acquired too many buffers and need to release
	 * them.
	 */
	if (extend_upto != InvalidBlockNumber)
	{
		uint32		orig_extend_by = extend_by;

		if (first_block > extend_upto)
			extend_by = 0;
		else if ((uint64) first_block + extend_by > extend_upto)
			extend_by = extend_upto - first_block;

		for (uint32 i = extend_by; i < orig_extend_by; i++)
		{
			BufferDesc *buf_hdr = GetBufferDescriptor(buffers[i] - 1);

			/*
			 * The victim buffer we acquired previously is clean and unused,
			 * let it be found again quickly
			 */
			StrategyFreeBuffer(buf_hdr);
			UnpinBuffer(buf_hdr);
		}

		if (extend_by == 0)
		{
			if (!(flags & EB_SKIP_EXTENSION_LOCK))
				UnlockRelationForExtension(bmr.rel, ExclusiveLock);
			*extended_by = extend_by;
			return first_block;
		}
	}

	/* Fail if relation is already at maximum possible length */
	if ((uint64) first_block + extend_by >= MaxBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cannot extend relation %s beyond %u blocks",
						relpath(bmr.smgr->smgr_rlocator, fork),
						MaxBlockNumber)));

	/*
	 * Insert buffers into buffer table, mark as IO_IN_PROGRESS.
	 *
	 * This needs to happen before we extend the relation, because as soon as
	 * we do, other backends can start to read in those pages.
	 */
	for (int i = 0; i < extend_by; i++)
	{
		Buffer		victim_buf = buffers[i];
		BufferDesc *victim_buf_hdr = GetBufferDescriptor(victim_buf - 1);
		BufferTag	tag;
		uint32		hash;
		LWLock	   *partition_lock;
		int			existing_id;

		InitBufferTag(&tag, &bmr.smgr->smgr_rlocator.locator, fork, first_block + i);
		hash = BufTableHashCode(&tag);
		partition_lock = BufMappingPartitionLock(hash);

		LWLockAcquire(partition_lock, LW_EXCLUSIVE);

		existing_id = BufTableInsert(&tag, hash, victim_buf_hdr->buf_id);

		/*
		 * We get here only in the corner case where we are trying to extend
		 * the relation but we found a pre-existing buffer. This can happen
		 * because a prior attempt at extending the relation failed, and
		 * because mdread doesn't complain about reads beyond EOF (when
		 * zero_damaged_pages is ON) and so a previous attempt to read a block
		 * beyond EOF could have left a "valid" zero-filled buffer.
		 * Unfortunately, we have also seen this case occurring because of
		 * buggy Linux kernels that sometimes return an lseek(SEEK_END) result
		 * that doesn't account for a recent write. In that situation, the
		 * pre-existing buffer would contain valid data that we don't want to
		 * overwrite.  Since the legitimate cases should always have left a
		 * zero-filled buffer, complain if not PageIsNew.
		 */
		if (existing_id >= 0)
		{
			BufferDesc *existing_hdr = GetBufferDescriptor(existing_id);
			Block		buf_block;
			bool		valid;

			/*
			 * Pin the existing buffer before releasing the partition lock,
			 * preventing it from being evicted.
			 */
			valid = PinBuffer(existing_hdr, strategy);

			LWLockRelease(partition_lock);

			/*
			 * The victim buffer we acquired previously is clean and unused,
			 * let it be found again quickly
			 */
			StrategyFreeBuffer(victim_buf_hdr);
			UnpinBuffer(victim_buf_hdr);

			buffers[i] = BufferDescriptorGetBuffer(existing_hdr);
			buf_block = BufHdrGetBlock(existing_hdr);

			if (valid && !PageIsNew((Page) buf_block))
				ereport(ERROR,
						(errmsg("unexpected data beyond EOF in block %u of relation %s",
								existing_hdr->tag.blockNum, relpath(bmr.smgr->smgr_rlocator, fork)),
						 errhint("This has been seen to occur with buggy kernels; consider updating your system.")));

			/*
			 * We *must* do smgr[zero]extend before succeeding, else the page
			 * will not be reserved by the kernel, and the next P_NEW call
			 * will decide to return the same page.  Clear the BM_VALID bit,
			 * do StartBufferIO() and proceed.
			 *
			 * Loop to handle the very small possibility that someone re-sets
			 * BM_VALID between our clearing it and StartBufferIO inspecting
			 * it.
			 */
#ifdef USE_PGRAC_CLUSTER
			{
				uint32		cluster_state = LockBufHdr(existing_hdr);
				bool		blocked
					= cluster_bufmgr_pcm_x_retained_image_locked(existing_hdr, cluster_state)
					|| (cluster_pcm_own_flags_get(existing_hdr->buf_id)
						& PCM_OWN_FLAG_REVOKING) != 0;

				UnlockBufHdr(existing_hdr, cluster_state);
				if (blocked)
					ereport(ERROR,
							(errcode(ERRCODE_OBJECT_IN_USE),
							 errmsg("cannot reuse retained cluster PCM image during "
									"relation extension")));
			}
#endif
			do
			{
				uint32		buf_state = LockBufHdr(existing_hdr);

				buf_state &= ~BM_VALID;
				UnlockBufHdr(existing_hdr, buf_state);
			} while (!StartBufferIO(existing_hdr, true));
		}
		else
		{
			uint32		buf_state;

			buf_state = LockBufHdr(victim_buf_hdr);

			/* some sanity checks while we hold the buffer header lock */
			Assert(!(buf_state & (BM_VALID | BM_TAG_VALID | BM_DIRTY | BM_JUST_DIRTIED)));
			Assert(BUF_STATE_GET_REFCOUNT(buf_state) == 1);

			victim_buf_hdr->tag = tag;

			buf_state |= BM_TAG_VALID | BUF_USAGECOUNT_ONE;
			if (bmr.relpersistence == RELPERSISTENCE_PERMANENT || fork == INIT_FORKNUM)
				buf_state |= BM_PERMANENT;

			UnlockBufHdr(victim_buf_hdr, buf_state);

			LWLockRelease(partition_lock);

			/* XXX: could combine the locked operations in it with the above */
			StartBufferIO(victim_buf_hdr, true);
		}
	}

	io_start = pgstat_prepare_io_time();

	/*
	 * Note: if smgrzeroextend fails, we will end up with buffers that are
	 * allocated but not marked BM_VALID.  The next relation extension will
	 * still select the same block number (because the relation didn't get any
	 * longer on disk) and so future attempts to extend the relation will find
	 * the same buffers (if they have not been recycled) but come right back
	 * here to try smgrzeroextend again.
	 *
	 * We don't need to set checksum for all-zero pages.
	 */
	smgrzeroextend(bmr.smgr, fork, first_block, extend_by, false);

#ifdef USE_PGRAC_CLUSTER

	/*
	 * PGRAC: spec-6.12d -- physically materialize the lease tail of the HW
	 * grant as zero pages (the file MUST stay dense up to the durably
	 * advanced HWM) and only then park it.  No victim buffers exist for
	 * these blocks -- they are disk-only zero pages, which is exactly what
	 * keeps them out of the shared FSM (Q19-A #4) until a local writer
	 * consumes them via the hio.c lease hook.  If smgrzeroextend throws,
	 * no lease is installed and the range degrades to the documented
	 * orphan-zero-page fail-safe once a later extend covers it.
	 */
	if (hw_lease_tail > 0)
	{
		smgrzeroextend(bmr.smgr, fork, first_block + extend_by,
					   (int) hw_lease_tail, false);
		cluster_hw_lease_install(bmr.smgr->smgr_rlocator.locator, fork,
								 first_block + extend_by, hw_lease_tail);
	}
#endif

	/*
	 * Release the file-extension lock; it's now OK for someone else to extend
	 * the relation some more.
	 *
	 * We remove IO_IN_PROGRESS after this, as waking up waiting backends can
	 * take noticeable time.
	 */
	if (!(flags & EB_SKIP_EXTENSION_LOCK))
		UnlockRelationForExtension(bmr.rel, ExclusiveLock);

	pgstat_count_io_op_time(IOOBJECT_RELATION, io_context, IOOP_EXTEND,
							io_start, extend_by);

	/* Set BM_VALID, terminate IO, and wake up any waiters */
	for (int i = 0; i < extend_by; i++)
	{
		Buffer		buf = buffers[i];
		BufferDesc *buf_hdr = GetBufferDescriptor(buf - 1);
		bool		lock = false;

		if (flags & EB_LOCK_FIRST && i == 0)
			lock = true;
		else if (flags & EB_LOCK_TARGET)
		{
			Assert(extend_upto != InvalidBlockNumber);
			if (first_block + i + 1 == extend_upto)
				lock = true;
		}

		if (lock)
		{
#ifdef USE_PGRAC_CLUSTER
			ClusterPcmDirectInitProof direct_init_proof;

			/* This exact buffer is in the range successfully zeroextended above
			 * and still owns BM_IO_IN_PROGRESS + !BM_VALID. */
			cluster_bufmgr_pcm_arm_direct_init(
				buf_hdr, CLUSTER_PCM_DIRECT_INIT_EXTEND, &direct_init_proof);
			cluster_bufmgr_pcm_gate_direct_init(
				buf_hdr, CLUSTER_PCM_DIRECT_INIT_EXTEND, &direct_init_proof);
#endif
			LWLockAcquire(BufferDescriptorGetContentLock(buf_hdr), LW_EXCLUSIVE);
		}

		TerminateBufferIO(buf_hdr, false, BM_VALID);
	}

	pgBufferUsage.shared_blks_written += extend_by;

	*extended_by = extend_by;

	return first_block;
}

/*
 * MarkBufferDirty
 *
 *		Marks buffer contents as dirty (actual write happens later).
 *
 * Buffer must be pinned and exclusive-locked.  (If caller does not hold
 * exclusive lock, then somebody could be in process of writing the buffer,
 * leading to risk of bad data written to disk.)
 */
void
MarkBufferDirty(Buffer buffer)
{
	BufferDesc *bufHdr;
	uint32		buf_state;
	uint32		old_buf_state;

	if (!BufferIsValid(buffer))
		elog(ERROR, "bad buffer ID: %d", buffer);

	if (BufferIsLocal(buffer))
	{
		MarkLocalBufferDirty(buffer);
		return;
	}

	bufHdr = GetBufferDescriptor(buffer - 1);

	Assert(BufferIsPinned(buffer));
	Assert(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(bufHdr),
								LW_EXCLUSIVE));

#ifdef USE_PGRAC_CLUSTER

	/*
	 * A retained source page is immutable transfer evidence, not a current
	 * writable copy.  Content EXCLUSIVE makes this check stable against the
	 * retain/release boundary.
	 */
	buf_state = LockBufHdr(bufHdr);
	if (cluster_bufmgr_pcm_x_retained_image_locked(bufHdr, buf_state))
	{
		UnlockBufHdr(bufHdr, buf_state);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot dirty a retained cluster PCM image"),
				 errdetail("buffer=%d", bufHdr->buf_id)));
	}
	UnlockBufHdr(bufHdr, buf_state);
#endif

	old_buf_state = pg_atomic_read_u32(&bufHdr->state);
	for (;;)
	{
		if (old_buf_state & BM_LOCKED)
			old_buf_state = WaitBufHdrUnlocked(bufHdr);

		buf_state = old_buf_state;

		Assert(BUF_STATE_GET_REFCOUNT(buf_state) > 0);
		buf_state |= BM_DIRTY | BM_JUST_DIRTIED;

		if (pg_atomic_compare_exchange_u32(&bufHdr->state, &old_buf_state,
										   buf_state))
			break;
	}

	/*
	 * If the buffer was not dirty already, do vacuum accounting.
	 */
	if (!(old_buf_state & BM_DIRTY))
	{
		VacuumPageDirty++;
		pgBufferUsage.shared_blks_dirtied++;
		if (VacuumCostActive)
			VacuumCostBalance += VacuumCostPageDirty;
	}
}

/*
 * ReleaseAndReadBuffer -- combine ReleaseBuffer() and ReadBuffer()
 *
 * Formerly, this saved one cycle of acquiring/releasing the BufMgrLock
 * compared to calling the two routines separately.  Now it's mainly just
 * a convenience function.  However, if the passed buffer is valid and
 * already contains the desired block, we just return it as-is; and that
 * does save considerable work compared to a full release and reacquire.
 *
 * Note: it is OK to pass buffer == InvalidBuffer, indicating that no old
 * buffer actually needs to be released.  This case is the same as ReadBuffer,
 * but can save some tests in the caller.
 */
Buffer
ReleaseAndReadBuffer(Buffer buffer,
					 Relation relation,
					 BlockNumber blockNum)
{
	ForkNumber	forkNum = MAIN_FORKNUM;
	BufferDesc *bufHdr;

	if (BufferIsValid(buffer))
	{
		Assert(BufferIsPinned(buffer));
		if (BufferIsLocal(buffer))
		{
			bufHdr = GetLocalBufferDescriptor(-buffer - 1);
			if (bufHdr->tag.blockNum == blockNum &&
				BufTagMatchesRelFileLocator(&bufHdr->tag, &relation->rd_locator) &&
				BufTagGetForkNum(&bufHdr->tag) == forkNum)
				return buffer;
			UnpinLocalBuffer(buffer);
		}
		else
		{
			bufHdr = GetBufferDescriptor(buffer - 1);
			/* we have pin, so it's ok to examine tag without spinlock */
			if (bufHdr->tag.blockNum == blockNum &&
				BufTagMatchesRelFileLocator(&bufHdr->tag, &relation->rd_locator) &&
				BufTagGetForkNum(&bufHdr->tag) == forkNum)
				return buffer;
			UnpinBuffer(bufHdr);
		}
	}

	return ReadBuffer(relation, blockNum);
}

/*
 * PinBuffer -- make buffer unavailable for replacement.
 *
 * For the default access strategy, the buffer's usage_count is incremented
 * when we first pin it; for other strategies we just make sure the usage_count
 * isn't zero.  (The idea of the latter is that we don't want synchronized
 * heap scans to inflate the count, but we need it to not be zero to discourage
 * other backends from stealing buffers from our ring.  As long as we cycle
 * through the ring faster than the global clock-sweep cycles, buffers in
 * our ring won't be chosen as victims for replacement by other backends.)
 *
 * This should be applied only to shared buffers, never local ones.
 *
 * Since buffers are pinned/unpinned very frequently, pin buffers without
 * taking the buffer header lock; instead update the state variable in loop of
 * CAS operations. Hopefully it's just a single CAS.
 *
 * Note that ResourceOwnerEnlargeBuffers must have been done already.
 *
 * Returns true if buffer is BM_VALID, else false.  This provision allows
 * some callers to avoid an extra spinlock cycle.
 */
static bool
PinBuffer(BufferDesc *buf, BufferAccessStrategy strategy)
{
	Buffer		b = BufferDescriptorGetBuffer(buf);
	bool		result;
	PrivateRefCountEntry *ref;

	Assert(!BufferIsLocal(b));

	ref = GetPrivateRefCountEntry(b, true);

	if (ref == NULL)
	{
		uint32		buf_state;
		uint32		old_buf_state;

		ReservePrivateRefCountEntry();
		ref = NewPrivateRefCountEntry(b);

		old_buf_state = pg_atomic_read_u32(&buf->state);
		for (;;)
		{
			if (old_buf_state & BM_LOCKED)
				old_buf_state = WaitBufHdrUnlocked(buf);

			buf_state = old_buf_state;

			/* increase refcount */
			buf_state += BUF_REFCOUNT_ONE;

			if (strategy == NULL)
			{
				/* Default case: increase usagecount unless already max. */
				if (BUF_STATE_GET_USAGECOUNT(buf_state) < BM_MAX_USAGE_COUNT)
					buf_state += BUF_USAGECOUNT_ONE;
			}
			else
			{
				/*
				 * Ring buffers shouldn't evict others from pool.  Thus we
				 * don't make usagecount more than 1.
				 */
				if (BUF_STATE_GET_USAGECOUNT(buf_state) == 0)
					buf_state += BUF_USAGECOUNT_ONE;
			}

			if (pg_atomic_compare_exchange_u32(&buf->state, &old_buf_state,
											   buf_state))
			{
				result = (buf_state & BM_VALID) != 0;

				/*
				 * Assume that we acquired a buffer pin for the purposes of
				 * Valgrind buffer client checks (even in !result case) to
				 * keep things simple.  Buffers that are unsafe to access are
				 * not generally guaranteed to be marked undefined or
				 * non-accessible in any case.
				 */
				VALGRIND_MAKE_MEM_DEFINED(BufHdrGetBlock(buf), BLCKSZ);
				break;
			}
		}
	}
	else
	{
		/*
		 * If we previously pinned the buffer, it must surely be valid.
		 *
		 * Note: We deliberately avoid a Valgrind client request here.
		 * Individual access methods can optionally superimpose buffer page
		 * client requests on top of our client requests to enforce that
		 * buffers are only accessed while locked (and pinned).  It's possible
		 * that the buffer page is legitimately non-accessible here.  We
		 * cannot meddle with that.
		 */
		result = true;
	}

	ref->refcount++;
	Assert(ref->refcount > 0);
	ResourceOwnerRememberBuffer(CurrentResourceOwner, b);
	return result;
}

/*
 * PinBuffer_Locked -- as above, but caller already locked the buffer header.
 * The spinlock is released before return.
 *
 * As this function is called with the spinlock held, the caller has to
 * previously call ReservePrivateRefCountEntry().
 *
 * Currently, no callers of this function want to modify the buffer's
 * usage_count at all, so there's no need for a strategy parameter.
 * Also we don't bother with a BM_VALID test (the caller could check that for
 * itself).
 *
 * Also all callers only ever use this function when it's known that the
 * buffer can't have a preexisting pin by this backend. That allows us to skip
 * searching the private refcount array & hash, which is a boon, because the
 * spinlock is still held.
 *
 * Note: use of this routine is frequently mandatory, not just an optimization
 * to save a spin lock/unlock cycle, because we need to pin a buffer before
 * its state can change under us.
 */
static void
PinBuffer_Locked(BufferDesc *buf)
{
	Buffer		b;
	PrivateRefCountEntry *ref;
	uint32		buf_state;

	/*
	 * As explained, We don't expect any preexisting pins. That allows us to
	 * manipulate the PrivateRefCount after releasing the spinlock
	 */
	Assert(GetPrivateRefCountEntry(BufferDescriptorGetBuffer(buf), false) == NULL);

	/*
	 * Buffer can't have a preexisting pin, so mark its page as defined to
	 * Valgrind (this is similar to the PinBuffer() case where the backend
	 * doesn't already have a buffer pin)
	 */
	VALGRIND_MAKE_MEM_DEFINED(BufHdrGetBlock(buf), BLCKSZ);

	/*
	 * Since we hold the buffer spinlock, we can update the buffer state and
	 * release the lock in one operation.
	 */
	buf_state = pg_atomic_read_u32(&buf->state);
	Assert(buf_state & BM_LOCKED);
	buf_state += BUF_REFCOUNT_ONE;
	UnlockBufHdr(buf, buf_state);

	b = BufferDescriptorGetBuffer(buf);

	ref = NewPrivateRefCountEntry(b);
	ref->refcount++;

	ResourceOwnerRememberBuffer(CurrentResourceOwner, b);
}

/*
 * UnpinBuffer -- make buffer available for replacement.
 *
 * This should be applied only to shared buffers, never local ones.  This
 * always adjusts CurrentResourceOwner.
 */
static void
UnpinBuffer(BufferDesc *buf)
{
	PrivateRefCountEntry *ref;
	Buffer		b = BufferDescriptorGetBuffer(buf);

	Assert(!BufferIsLocal(b));

	/* not moving as we're likely deleting it soon anyway */
	ref = GetPrivateRefCountEntry(b, false);
	Assert(ref != NULL);

	ResourceOwnerForgetBuffer(CurrentResourceOwner, b);

	Assert(ref->refcount > 0);
	ref->refcount--;
	if (ref->refcount == 0)
	{
		uint32		buf_state;
		uint32		old_buf_state;

		/*
		 * Mark buffer non-accessible to Valgrind.
		 *
		 * Note that the buffer may have already been marked non-accessible
		 * within access method code that enforces that buffers are only
		 * accessed while a buffer lock is held.
		 */
		VALGRIND_MAKE_MEM_NOACCESS(BufHdrGetBlock(buf), BLCKSZ);

		/* I'd better not still hold the buffer content lock */
		Assert(!LWLockHeldByMe(BufferDescriptorGetContentLock(buf)));

		/*
		 * Decrement the shared reference count.
		 *
		 * Since buffer spinlock holder can update status using just write,
		 * it's not safe to use atomic decrement here; thus use a CAS loop.
		 */
		old_buf_state = pg_atomic_read_u32(&buf->state);
		for (;;)
		{
			if (old_buf_state & BM_LOCKED)
				old_buf_state = WaitBufHdrUnlocked(buf);

			buf_state = old_buf_state;

			buf_state -= BUF_REFCOUNT_ONE;

			if (pg_atomic_compare_exchange_u32(&buf->state, &old_buf_state,
											   buf_state))
				break;
		}

		/* Support LockBufferForCleanup() */
		if (buf_state & BM_PIN_COUNT_WAITER)
		{
			/*
			 * Acquire the buffer header lock, re-check that there's a waiter.
			 * Another backend could have unpinned this buffer, and already
			 * woken up the waiter.  There's no danger of the buffer being
			 * replaced after we unpinned it above, as it's pinned by the
			 * waiter.
			 */
			buf_state = LockBufHdr(buf);

			if ((buf_state & BM_PIN_COUNT_WAITER) &&
				BUF_STATE_GET_REFCOUNT(buf_state) == 1)
			{
				/* we just released the last pin other than the waiter's */
				int			wait_backend_pgprocno = buf->wait_backend_pgprocno;

				buf_state &= ~BM_PIN_COUNT_WAITER;
				UnlockBufHdr(buf, buf_state);
				ProcSendSignal(wait_backend_pgprocno);
			}
			else
				UnlockBufHdr(buf, buf_state);
		}
		ForgetPrivateRefCountEntry(ref);
	}
}

#define ST_SORT sort_checkpoint_bufferids
#define ST_ELEMENT_TYPE CkptSortItem
#define ST_COMPARE(a, b) ckpt_buforder_comparator(a, b)
#define ST_SCOPE static
#define ST_DEFINE
#include <lib/sort_template.h>

/*
 * BufferSync -- Write out all dirty buffers in the pool.
 *
 * This is called at checkpoint time to write out all dirty shared buffers.
 * The checkpoint request flags should be passed in.  If CHECKPOINT_IMMEDIATE
 * is set, we disable delays between writes; if CHECKPOINT_IS_SHUTDOWN,
 * CHECKPOINT_END_OF_RECOVERY or CHECKPOINT_FLUSH_ALL is set, we write even
 * unlogged buffers, which are otherwise skipped.  The remaining flags
 * currently have no effect here.
 */
static void
BufferSync(int flags)
{
	uint32		buf_state;
	int			buf_id;
	int			num_to_scan;
	int			num_spaces;
	int			num_processed;
	int			num_written;
	CkptTsStatus *per_ts_stat = NULL;
	Oid			last_tsid;
	binaryheap *ts_heap;
	int			i;
	int			mask = BM_DIRTY;
	WritebackContext wb_context;

	/* Make sure we can handle the pin inside SyncOneBuffer */
	ResourceOwnerEnlargeBuffers(CurrentResourceOwner);

	/*
	 * Unless this is a shutdown checkpoint or we have been explicitly told,
	 * we write only permanent, dirty buffers.  But at shutdown or end of
	 * recovery, we write all dirty buffers.
	 */
	if (!((flags & (CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY |
					CHECKPOINT_FLUSH_ALL))))
		mask |= BM_PERMANENT;

	/*
	 * Loop over all buffers, and mark the ones that need to be written with
	 * BM_CHECKPOINT_NEEDED.  Count them as we go (num_to_scan), so that we
	 * can estimate how much work needs to be done.
	 *
	 * This allows us to write only those pages that were dirty when the
	 * checkpoint began, and not those that get dirtied while it proceeds.
	 * Whenever a page with BM_CHECKPOINT_NEEDED is written out, either by us
	 * later in this function, or by normal backends or the bgwriter cleaning
	 * scan, the flag is cleared.  Any buffer dirtied after this point won't
	 * have the flag set.
	 *
	 * Note that if we fail to write some buffer, we may leave buffers with
	 * BM_CHECKPOINT_NEEDED still set.  This is OK since any such buffer would
	 * certainly need to be written for the next checkpoint attempt, too.
	 */
	num_to_scan = 0;
	for (buf_id = 0; buf_id < NBuffers; buf_id++)
	{
		BufferDesc *bufHdr = GetBufferDescriptor(buf_id);

		/*
		 * Header spinlock is enough to examine BM_DIRTY, see comment in
		 * SyncOneBuffer.
		 */
		buf_state = LockBufHdr(bufHdr);

		if ((buf_state & mask) == mask)
		{
			CkptSortItem *item;

			buf_state |= BM_CHECKPOINT_NEEDED;

			item = &CkptBufferIds[num_to_scan++];
			item->buf_id = buf_id;
			item->tsId = bufHdr->tag.spcOid;
			item->relNumber = BufTagGetRelNumber(&bufHdr->tag);
			item->forkNum = BufTagGetForkNum(&bufHdr->tag);
			item->blockNum = bufHdr->tag.blockNum;
		}

		UnlockBufHdr(bufHdr, buf_state);

		/* Check for barrier events in case NBuffers is large. */
		if (ProcSignalBarrierPending)
			ProcessProcSignalBarrier();
	}

	if (num_to_scan == 0)
		return;					/* nothing to do */

	WritebackContextInit(&wb_context, &checkpoint_flush_after);

	TRACE_POSTGRESQL_BUFFER_SYNC_START(NBuffers, num_to_scan);

	/*
	 * Sort buffers that need to be written to reduce the likelihood of random
	 * IO. The sorting is also important for the implementation of balancing
	 * writes between tablespaces. Without balancing writes we'd potentially
	 * end up writing to the tablespaces one-by-one; possibly overloading the
	 * underlying system.
	 */
	sort_checkpoint_bufferids(CkptBufferIds, num_to_scan);

	num_spaces = 0;

	/*
	 * Allocate progress status for each tablespace with buffers that need to
	 * be flushed. This requires the to-be-flushed array to be sorted.
	 */
	last_tsid = InvalidOid;
	for (i = 0; i < num_to_scan; i++)
	{
		CkptTsStatus *s;
		Oid			cur_tsid;

		cur_tsid = CkptBufferIds[i].tsId;

		/*
		 * Grow array of per-tablespace status structs, every time a new
		 * tablespace is found.
		 */
		if (last_tsid == InvalidOid || last_tsid != cur_tsid)
		{
			Size		sz;

			num_spaces++;

			/*
			 * Not worth adding grow-by-power-of-2 logic here - even with a
			 * few hundred tablespaces this should be fine.
			 */
			sz = sizeof(CkptTsStatus) * num_spaces;

			if (per_ts_stat == NULL)
				per_ts_stat = (CkptTsStatus *) palloc(sz);
			else
				per_ts_stat = (CkptTsStatus *) repalloc(per_ts_stat, sz);

			s = &per_ts_stat[num_spaces - 1];
			memset(s, 0, sizeof(*s));
			s->tsId = cur_tsid;

			/*
			 * The first buffer in this tablespace. As CkptBufferIds is sorted
			 * by tablespace all (s->num_to_scan) buffers in this tablespace
			 * will follow afterwards.
			 */
			s->index = i;

			/*
			 * progress_slice will be determined once we know how many buffers
			 * are in each tablespace, i.e. after this loop.
			 */

			last_tsid = cur_tsid;
		}
		else
		{
			s = &per_ts_stat[num_spaces - 1];
		}

		s->num_to_scan++;

		/* Check for barrier events. */
		if (ProcSignalBarrierPending)
			ProcessProcSignalBarrier();
	}

	Assert(num_spaces > 0);

	/*
	 * Build a min-heap over the write-progress in the individual tablespaces,
	 * and compute how large a portion of the total progress a single
	 * processed buffer is.
	 */
	ts_heap = binaryheap_allocate(num_spaces,
								  ts_ckpt_progress_comparator,
								  NULL);

	for (i = 0; i < num_spaces; i++)
	{
		CkptTsStatus *ts_stat = &per_ts_stat[i];

		ts_stat->progress_slice = (float8) num_to_scan / ts_stat->num_to_scan;

		binaryheap_add_unordered(ts_heap, PointerGetDatum(ts_stat));
	}

	binaryheap_build(ts_heap);

	/*
	 * Iterate through to-be-checkpointed buffers and write the ones (still)
	 * marked with BM_CHECKPOINT_NEEDED. The writes are balanced between
	 * tablespaces; otherwise the sorting would lead to only one tablespace
	 * receiving writes at a time, making inefficient use of the hardware.
	 */
	num_processed = 0;
	num_written = 0;
	while (!binaryheap_empty(ts_heap))
	{
		BufferDesc *bufHdr = NULL;
		CkptTsStatus *ts_stat = (CkptTsStatus *)
			DatumGetPointer(binaryheap_first(ts_heap));

		buf_id = CkptBufferIds[ts_stat->index].buf_id;
		Assert(buf_id != -1);

		bufHdr = GetBufferDescriptor(buf_id);

		num_processed++;

		/*
		 * We don't need to acquire the lock here, because we're only looking
		 * at a single bit. It's possible that someone else writes the buffer
		 * and clears the flag right after we check, but that doesn't matter
		 * since SyncOneBuffer will then do nothing.  However, there is a
		 * further race condition: it's conceivable that between the time we
		 * examine the bit here and the time SyncOneBuffer acquires the lock,
		 * someone else not only wrote the buffer but replaced it with another
		 * page and dirtied it.  In that improbable case, SyncOneBuffer will
		 * write the buffer though we didn't need to.  It doesn't seem worth
		 * guarding against this, though.
		 */
		if (pg_atomic_read_u32(&bufHdr->state) & BM_CHECKPOINT_NEEDED)
		{
			if (SyncOneBuffer(buf_id, false, &wb_context) & BUF_WRITTEN)
			{
				TRACE_POSTGRESQL_BUFFER_SYNC_WRITTEN(buf_id);
				PendingCheckpointerStats.buf_written_checkpoints++;
				num_written++;
			}
		}

		/*
		 * Measure progress independent of actually having to flush the buffer
		 * - otherwise writing become unbalanced.
		 */
		ts_stat->progress += ts_stat->progress_slice;
		ts_stat->num_scanned++;
		ts_stat->index++;

		/* Have all the buffers from the tablespace been processed? */
		if (ts_stat->num_scanned == ts_stat->num_to_scan)
		{
			binaryheap_remove_first(ts_heap);
		}
		else
		{
			/* update heap with the new progress */
			binaryheap_replace_first(ts_heap, PointerGetDatum(ts_stat));
		}

		/*
		 * Sleep to throttle our I/O rate.
		 *
		 * (This will check for barrier events even if it doesn't sleep.)
		 */
		CheckpointWriteDelay(flags, (double) num_processed / num_to_scan);
	}

	/*
	 * Issue all pending flushes. Only checkpointer calls BufferSync(), so
	 * IOContext will always be IOCONTEXT_NORMAL.
	 */
	IssuePendingWritebacks(&wb_context, IOCONTEXT_NORMAL);

	pfree(per_ts_stat);
	per_ts_stat = NULL;
	binaryheap_free(ts_heap);

	/*
	 * Update checkpoint statistics. As noted above, this doesn't include
	 * buffers written by other backends or bgwriter scan.
	 */
	CheckpointStats.ckpt_bufs_written += num_written;

	TRACE_POSTGRESQL_BUFFER_SYNC_DONE(NBuffers, num_written, num_to_scan);
}

/*
 * BgBufferSync -- Write out some dirty buffers in the pool.
 *
 * This is called periodically by the background writer process.
 *
 * Returns true if it's appropriate for the bgwriter process to go into
 * low-power hibernation mode.  (This happens if the strategy clock sweep
 * has been "lapped" and no buffer allocations have occurred recently,
 * or if the bgwriter has been effectively disabled by setting
 * bgwriter_lru_maxpages to 0.)
 */
bool
BgBufferSync(WritebackContext *wb_context)
{
	/* info obtained from freelist.c */
	int			strategy_buf_id;
	uint32		strategy_passes;
	uint32		recent_alloc;

	/*
	 * Information saved between calls so we can determine the strategy
	 * point's advance rate and avoid scanning already-cleaned buffers.
	 */
	static bool saved_info_valid = false;
	static int	prev_strategy_buf_id;
	static uint32 prev_strategy_passes;
	static int	next_to_clean;
	static uint32 next_passes;

	/* Moving averages of allocation rate and clean-buffer density */
	static float smoothed_alloc = 0;
	static float smoothed_density = 10.0;

	/* Potentially these could be tunables, but for now, not */
	float		smoothing_samples = 16;
	float		scan_whole_pool_milliseconds = 120000.0;

	/* Used to compute how far we scan ahead */
	long		strategy_delta;
	int			bufs_to_lap;
	int			bufs_ahead;
	float		scans_per_alloc;
	int			reusable_buffers_est;
	int			upcoming_alloc_est;
	int			min_scan_buffers;

	/* Variables for the scanning loop proper */
	int			num_to_scan;
	int			num_written;
	int			reusable_buffers;

	/* Variables for final smoothed_density update */
	long		new_strategy_delta;
	uint32		new_recent_alloc;

	/*
	 * Find out where the freelist clock sweep currently is, and how many
	 * buffer allocations have happened since our last call.
	 */
	strategy_buf_id = StrategySyncStart(&strategy_passes, &recent_alloc);

	/* Report buffer alloc counts to pgstat */
	PendingBgWriterStats.buf_alloc += recent_alloc;

	/*
	 * If we're not running the LRU scan, just stop after doing the stats
	 * stuff.  We mark the saved state invalid so that we can recover sanely
	 * if LRU scan is turned back on later.
	 */
	if (bgwriter_lru_maxpages <= 0)
	{
		saved_info_valid = false;
		return true;
	}

	/*
	 * Compute strategy_delta = how many buffers have been scanned by the
	 * clock sweep since last time.  If first time through, assume none. Then
	 * see if we are still ahead of the clock sweep, and if so, how many
	 * buffers we could scan before we'd catch up with it and "lap" it. Note:
	 * weird-looking coding of xxx_passes comparisons are to avoid bogus
	 * behavior when the passes counts wrap around.
	 */
	if (saved_info_valid)
	{
		int32		passes_delta = strategy_passes - prev_strategy_passes;

		strategy_delta = strategy_buf_id - prev_strategy_buf_id;
		strategy_delta += (long) passes_delta * NBuffers;

		Assert(strategy_delta >= 0);

		if ((int32) (next_passes - strategy_passes) > 0)
		{
			/* we're one pass ahead of the strategy point */
			bufs_to_lap = strategy_buf_id - next_to_clean;
#ifdef BGW_DEBUG
			elog(DEBUG2, "bgwriter ahead: bgw %u-%u strategy %u-%u delta=%ld lap=%d",
				 next_passes, next_to_clean,
				 strategy_passes, strategy_buf_id,
				 strategy_delta, bufs_to_lap);
#endif
		}
		else if (next_passes == strategy_passes &&
				 next_to_clean >= strategy_buf_id)
		{
			/* on same pass, but ahead or at least not behind */
			bufs_to_lap = NBuffers - (next_to_clean - strategy_buf_id);
#ifdef BGW_DEBUG
			elog(DEBUG2, "bgwriter ahead: bgw %u-%u strategy %u-%u delta=%ld lap=%d",
				 next_passes, next_to_clean,
				 strategy_passes, strategy_buf_id,
				 strategy_delta, bufs_to_lap);
#endif
		}
		else
		{
			/*
			 * We're behind, so skip forward to the strategy point and start
			 * cleaning from there.
			 */
#ifdef BGW_DEBUG
			elog(DEBUG2, "bgwriter behind: bgw %u-%u strategy %u-%u delta=%ld",
				 next_passes, next_to_clean,
				 strategy_passes, strategy_buf_id,
				 strategy_delta);
#endif
			next_to_clean = strategy_buf_id;
			next_passes = strategy_passes;
			bufs_to_lap = NBuffers;
		}
	}
	else
	{
		/*
		 * Initializing at startup or after LRU scanning had been off. Always
		 * start at the strategy point.
		 */
#ifdef BGW_DEBUG
		elog(DEBUG2, "bgwriter initializing: strategy %u-%u",
			 strategy_passes, strategy_buf_id);
#endif
		strategy_delta = 0;
		next_to_clean = strategy_buf_id;
		next_passes = strategy_passes;
		bufs_to_lap = NBuffers;
	}

	/* Update saved info for next time */
	prev_strategy_buf_id = strategy_buf_id;
	prev_strategy_passes = strategy_passes;
	saved_info_valid = true;

	/*
	 * Compute how many buffers had to be scanned for each new allocation, ie,
	 * 1/density of reusable buffers, and track a moving average of that.
	 *
	 * If the strategy point didn't move, we don't update the density estimate
	 */
	if (strategy_delta > 0 && recent_alloc > 0)
	{
		scans_per_alloc = (float) strategy_delta / (float) recent_alloc;
		smoothed_density += (scans_per_alloc - smoothed_density) /
			smoothing_samples;
	}

	/*
	 * Estimate how many reusable buffers there are between the current
	 * strategy point and where we've scanned ahead to, based on the smoothed
	 * density estimate.
	 */
	bufs_ahead = NBuffers - bufs_to_lap;
	reusable_buffers_est = (float) bufs_ahead / smoothed_density;

	/*
	 * Track a moving average of recent buffer allocations.  Here, rather than
	 * a true average we want a fast-attack, slow-decline behavior: we
	 * immediately follow any increase.
	 */
	if (smoothed_alloc <= (float) recent_alloc)
		smoothed_alloc = recent_alloc;
	else
		smoothed_alloc += ((float) recent_alloc - smoothed_alloc) /
			smoothing_samples;

	/* Scale the estimate by a GUC to allow more aggressive tuning. */
	upcoming_alloc_est = (int) (smoothed_alloc * bgwriter_lru_multiplier);

	/*
	 * If recent_alloc remains at zero for many cycles, smoothed_alloc will
	 * eventually underflow to zero, and the underflows produce annoying
	 * kernel warnings on some platforms.  Once upcoming_alloc_est has gone to
	 * zero, there's no point in tracking smaller and smaller values of
	 * smoothed_alloc, so just reset it to exactly zero to avoid this
	 * syndrome.  It will pop back up as soon as recent_alloc increases.
	 */
	if (upcoming_alloc_est == 0)
		smoothed_alloc = 0;

	/*
	 * Even in cases where there's been little or no buffer allocation
	 * activity, we want to make a small amount of progress through the buffer
	 * cache so that as many reusable buffers as possible are clean after an
	 * idle period.
	 *
	 * (scan_whole_pool_milliseconds / BgWriterDelay) computes how many times
	 * the BGW will be called during the scan_whole_pool time; slice the
	 * buffer pool into that many sections.
	 */
	min_scan_buffers = (int) (NBuffers / (scan_whole_pool_milliseconds / BgWriterDelay));

	if (upcoming_alloc_est < (min_scan_buffers + reusable_buffers_est))
	{
#ifdef BGW_DEBUG
		elog(DEBUG2, "bgwriter: alloc_est=%d too small, using min=%d + reusable_est=%d",
			 upcoming_alloc_est, min_scan_buffers, reusable_buffers_est);
#endif
		upcoming_alloc_est = min_scan_buffers + reusable_buffers_est;
	}

	/*
	 * Now write out dirty reusable buffers, working forward from the
	 * next_to_clean point, until we have lapped the strategy scan, or cleaned
	 * enough buffers to match our estimate of the next cycle's allocation
	 * requirements, or hit the bgwriter_lru_maxpages limit.
	 */

	/* Make sure we can handle the pin inside SyncOneBuffer */
	ResourceOwnerEnlargeBuffers(CurrentResourceOwner);

	num_to_scan = bufs_to_lap;
	num_written = 0;
	reusable_buffers = reusable_buffers_est;

	/* Execute the LRU scan */
	while (num_to_scan > 0 && reusable_buffers < upcoming_alloc_est)
	{
		int			sync_state = SyncOneBuffer(next_to_clean, true,
											   wb_context);

		if (++next_to_clean >= NBuffers)
		{
			next_to_clean = 0;
			next_passes++;
		}
		num_to_scan--;

		if (sync_state & BUF_WRITTEN)
		{
			reusable_buffers++;
			if (++num_written >= bgwriter_lru_maxpages)
			{
				PendingBgWriterStats.maxwritten_clean++;
				break;
			}
		}
		else if (sync_state & BUF_REUSABLE)
			reusable_buffers++;
	}

	PendingBgWriterStats.buf_written_clean += num_written;

#ifdef BGW_DEBUG
	elog(DEBUG1, "bgwriter: recent_alloc=%u smoothed=%.2f delta=%ld ahead=%d density=%.2f reusable_est=%d upcoming_est=%d scanned=%d wrote=%d reusable=%d",
		 recent_alloc, smoothed_alloc, strategy_delta, bufs_ahead,
		 smoothed_density, reusable_buffers_est, upcoming_alloc_est,
		 bufs_to_lap - num_to_scan,
		 num_written,
		 reusable_buffers - reusable_buffers_est);
#endif

	/*
	 * Consider the above scan as being like a new allocation scan.
	 * Characterize its density and update the smoothed one based on it. This
	 * effectively halves the moving average period in cases where both the
	 * strategy and the background writer are doing some useful scanning,
	 * which is helpful because a long memory isn't as desirable on the
	 * density estimates.
	 */
	new_strategy_delta = bufs_to_lap - num_to_scan;
	new_recent_alloc = reusable_buffers - reusable_buffers_est;
	if (new_strategy_delta > 0 && new_recent_alloc > 0)
	{
		scans_per_alloc = (float) new_strategy_delta / (float) new_recent_alloc;
		smoothed_density += (scans_per_alloc - smoothed_density) /
			smoothing_samples;

#ifdef BGW_DEBUG
		elog(DEBUG2, "bgwriter: cleaner density alloc=%u scan=%ld density=%.2f new smoothed=%.2f",
			 new_recent_alloc, new_strategy_delta,
			 scans_per_alloc, smoothed_density);
#endif
	}

	/* Return true if OK to hibernate */
	return (bufs_to_lap == 0 && recent_alloc == 0);
}

/*
 * SyncOneBuffer -- process a single buffer during syncing.
 *
 * If skip_recently_used is true, we don't write currently-pinned buffers, nor
 * buffers marked recently used, as these are not replacement candidates.
 *
 * Returns a bitmask containing the following flag bits:
 *	BUF_WRITTEN: we wrote the buffer.
 *	BUF_REUSABLE: buffer is available for replacement, ie, it has
 *		pin count 0 and usage count 0.
 *
 * (BUF_WRITTEN could be set in error if FlushBuffer finds the buffer clean
 * after locking it, but we don't care all that much.)
 *
 * Note: caller must have done ResourceOwnerEnlargeBuffers.
 */
static int
SyncOneBuffer(int buf_id, bool skip_recently_used, WritebackContext *wb_context)
{
	BufferDesc *bufHdr = GetBufferDescriptor(buf_id);
	int			result = 0;
	uint32		buf_state;
	BufferTag	tag;

	ReservePrivateRefCountEntry();

	/*
	 * Check whether buffer needs writing.
	 *
	 * We can make this check without taking the buffer content lock so long
	 * as we mark pages dirty in access methods *before* logging changes with
	 * XLogInsert(): if someone marks the buffer dirty just after our check we
	 * don't worry because our checkpoint.redo points before log record for
	 * upcoming changes and so we are not required to write such dirty buffer.
	 */
	buf_state = LockBufHdr(bufHdr);

#ifdef USE_PGRAC_CLUSTER

	/*
	 * A live retained image is neither reusable nor writable.  Test before
	 * advertising BUF_REUSABLE; finish/release serialize the later race with
	 * the content-lock recheck below. */
	if (cluster_bufmgr_pcm_x_retained_image_reuse_blocked_locked(bufHdr, buf_state))
	{
		UnlockBufHdr(bufHdr, buf_state);
		return 0;
	}
#endif

	if (BUF_STATE_GET_REFCOUNT(buf_state) == 0 &&
		BUF_STATE_GET_USAGECOUNT(buf_state) == 0)
	{
		result |= BUF_REUSABLE;
	}
	else if (skip_recently_used)
	{
		/* Caller told us not to write recently-used buffers */
		UnlockBufHdr(bufHdr, buf_state);
		return result;
	}

	if (!(buf_state & BM_VALID) || !(buf_state & BM_DIRTY))
	{
		/* It's clean, so nothing to do */
		UnlockBufHdr(bufHdr, buf_state);
		return result;
	}

	/*
	 * Pin it, share-lock it, write it.  (FlushBuffer will do nothing if the
	 * buffer is clean by the time we've locked it.)
	 */
	PinBuffer_Locked(bufHdr);
	LWLockAcquire(BufferDescriptorGetContentLock(bufHdr), LW_SHARED);

#ifdef USE_PGRAC_CLUSTER

	/*
	 * If retain won after the optimistic header probe, content SHARE proves
	 * the transition is now complete and stable.
	 */
	buf_state = LockBufHdr(bufHdr);
	if (cluster_bufmgr_pcm_x_retained_image_reuse_blocked_locked(bufHdr, buf_state))
	{
		UnlockBufHdr(bufHdr, buf_state);
		LWLockRelease(BufferDescriptorGetContentLock(bufHdr));
		UnpinBuffer(bufHdr);
		return result & ~BUF_REUSABLE;
	}
	UnlockBufHdr(bufHdr, buf_state);
#endif

	FlushBuffer(bufHdr, NULL, IOOBJECT_RELATION, IOCONTEXT_NORMAL);

	LWLockRelease(BufferDescriptorGetContentLock(bufHdr));

	tag = bufHdr->tag;

	UnpinBuffer(bufHdr);

	/*
	 * SyncOneBuffer() is only called by checkpointer and bgwriter, so
	 * IOContext will always be IOCONTEXT_NORMAL.
	 */
	ScheduleBufferTagForWriteback(wb_context, IOCONTEXT_NORMAL, &tag);

	return result | BUF_WRITTEN;
}

/*
 *		AtEOXact_Buffers - clean up at end of transaction.
 *
 *		As of PostgreSQL 8.0, buffer pins should get released by the
 *		ResourceOwner mechanism.  This routine is just a debugging
 *		cross-check that no pins remain.
 */
void
AtEOXact_Buffers(bool isCommit)
{
	CheckForBufferLeaks();

	AtEOXact_LocalBuffers(isCommit);

#ifdef USE_PGRAC_CLUSTER

	/*
	 * Normal UNLOCK may defer an exact RELEASING holder when RETIRE owns the
	 * short gate.  Transaction tail gets one non-waiting drain opportunity;
	 * unresolved evidence remains backend-local for the next safe entrance.
	 */
	cluster_bufmgr_pcm_x_writer_drain_deferred_nowait();
	cluster_bufmgr_pcm_x_holder_drain_deferred_nowait();
#endif

	Assert(PrivateRefCountOverflowed == 0);
}

/*
 * Initialize access to shared buffer pool
 *
 * This is called during backend startup (whether standalone or under the
 * postmaster).  It sets up for this backend's access to the already-existing
 * buffer pool.
 */
void
InitBufferPoolAccess(void)
{
	HASHCTL		hash_ctl;

	memset(&PrivateRefCountArray, 0, sizeof(PrivateRefCountArray));
#ifdef USE_PGRAC_CLUSTER
	cluster_bufmgr_pcm_x_writer_reset();
	cluster_bufmgr_pcm_x_holder_reset();
#endif

	hash_ctl.keysize = sizeof(int32);
	hash_ctl.entrysize = sizeof(PrivateRefCountEntry);

	PrivateRefCountHash = hash_create("PrivateRefCount", 100, &hash_ctl,
									  HASH_ELEM | HASH_BLOBS);

	/*
	 * AtProcExit_Buffers needs LWLock access, and thereby has to be called at
	 * the corresponding phase of backend shutdown.
	 */
	Assert(MyProc != NULL);
	on_shmem_exit(AtProcExit_Buffers, 0);
}

/*
 * During backend exit, ensure that we released all shared-buffer locks and
 * assert that we have no remaining pins.
 */
static void
AtProcExit_Buffers(int code, Datum arg)
{
	UnlockBuffers();

#ifdef USE_PGRAC_CLUSTER
	cluster_bufmgr_pcm_x_owner_exit_drain();
#endif

	CheckForBufferLeaks();

	/* localbuf.c needs a chance too */
	AtProcExit_LocalBuffers();
}

/*
 *		CheckForBufferLeaks - ensure this backend holds no buffer pins
 *
 *		As of PostgreSQL 8.0, buffer pins should get released by the
 *		ResourceOwner mechanism.  This routine is just a debugging
 *		cross-check that no pins remain.
 */
static void
CheckForBufferLeaks(void)
{
#ifdef USE_ASSERT_CHECKING
	int			RefCountErrors = 0;
	PrivateRefCountEntry *res;
	int			i;

	/* check the array */
	for (i = 0; i < REFCOUNT_ARRAY_ENTRIES; i++)
	{
		res = &PrivateRefCountArray[i];

		if (res->buffer != InvalidBuffer)
		{
			PrintBufferLeakWarning(res->buffer);
			RefCountErrors++;
		}
	}

	/* if necessary search the hash */
	if (PrivateRefCountOverflowed)
	{
		HASH_SEQ_STATUS hstat;

		hash_seq_init(&hstat, PrivateRefCountHash);
		while ((res = (PrivateRefCountEntry *) hash_seq_search(&hstat)) != NULL)
		{
			PrintBufferLeakWarning(res->buffer);
			RefCountErrors++;
		}
	}

	Assert(RefCountErrors == 0);
#endif
}

#ifdef USE_ASSERT_CHECKING
/*
 * Check for exclusive-locked catalog buffers.  This is the core of
 * AssertCouldGetRelation().
 *
 * A backend would self-deadlock on LWLocks if the catalog scan read the
 * exclusive-locked buffer.  The main threat is exclusive-locked buffers of
 * catalogs used in relcache, because a catcache search on any catalog may
 * build that catalog's relcache entry.  We don't have an inventory of
 * catalogs relcache uses, so just check buffers of most catalogs.
 *
 * It's better to minimize waits while holding an exclusive buffer lock, so it
 * would be nice to broaden this check not to be catalog-specific.  However,
 * bttextcmp() accesses pg_collation, and non-core opclasses might similarly
 * read tables.  That is deadlock-free as long as there's no loop in the
 * dependency graph: modifying table A may cause an opclass to read table B,
 * but it must not cause a read of table A.
 */
void
AssertBufferLocksPermitCatalogRead(void)
{
	ForEachLWLockHeldByMe(AssertNotCatalogBufferLock, NULL);
}

static void
AssertNotCatalogBufferLock(LWLock *lock, LWLockMode mode,
						   void *unused_context)
{
	BufferDesc *bufHdr;
	BufferTag	tag;
	Oid relid;

	if (mode != LW_EXCLUSIVE)
		return;

	/*
	 * PGRAC (stage 1.6 hardening, codex review 2026-05-02 P1/P2 #1):
	 *
	 * BufferDesc now contains both content_lock (offset 36 on PG 16.13) and
	 * pcm_lock (offset 104).  The address-range check above matches both --
	 * pcm_lock is also "in BufferDescriptors range".
	 *
	 * Without this guard, when Stage 2 真值激活 acquires pcm_lock
	 * exclusively, this debug helper would interpret it as content_lock and
	 * reverse-deref using offsetof(content_lock) -- yielding a BufferDesc
	 * pointer pointing to garbage (pcm_lock_addr - 36 falls in the middle of
	 * a previous slot's cold-section fields).  Reading bufHdr->tag would then
	 * return random memory, with arbitrary consequences ranging from spurious
	 * assertion fail to silent garbage relid checks against system catalog.
	 *
	 * Use the shared exact reverse-map; otherwise pcm_lock (or a future
	 * embedded LWLock) could be misidentified as content_lock.
	 *
	 * Spec: spec-stage1-codex-fixes.md §1.2 Deliverable 5 + spec-1.6 §11
	 * hardening
	 */
	bufHdr = BufferDescriptorFromContentLock(lock);
	if (bufHdr == NULL)
		return; /* not an exact shared-buffer content lock */
	tag = bufHdr->tag;

	/*
	 * This relNumber==relid assumption holds until a catalog experiences
	 * VACUUM FULL or similar.  After a command like that, relNumber will be
	 * in the normal (non-catalog) range, and we lose the ability to detect
	 * hazardous access to that catalog.  Calling RelidByRelfilenumber() would
	 * close that gap, but RelidByRelfilenumber() might then deadlock with a
	 * held lock.
	 */
	relid = tag.relNumber;

	Assert(!IsCatalogRelationOid(relid));
	/* Shared rels are always catalogs: detect even after VACUUM FULL. */
	Assert(tag.spcOid != GLOBALTABLESPACE_OID);
}
#endif


/*
 * Helper routine to issue warnings when a buffer is unexpectedly pinned
 */
void
PrintBufferLeakWarning(Buffer buffer)
{
	BufferDesc *buf;
	int32		loccount;
	char	   *path;
	BackendId	backend;
	uint32		buf_state;

	Assert(BufferIsValid(buffer));
	if (BufferIsLocal(buffer))
	{
		buf = GetLocalBufferDescriptor(-buffer - 1);
		loccount = LocalRefCount[-buffer - 1];
		backend = MyBackendId;
	}
	else
	{
		buf = GetBufferDescriptor(buffer - 1);
		loccount = GetPrivateRefCount(buffer);
		backend = InvalidBackendId;
	}

	/* theoretically we should lock the bufhdr here */
	path = relpathbackend(BufTagGetRelFileLocator(&buf->tag), backend,
						  BufTagGetForkNum(&buf->tag));
	buf_state = pg_atomic_read_u32(&buf->state);
	elog(WARNING,
		 "buffer refcount leak: [%03d] "
		 "(rel=%s, blockNum=%u, flags=0x%x, refcount=%u %d)",
		 buffer, path,
		 buf->tag.blockNum, buf_state & BUF_FLAG_MASK,
		 BUF_STATE_GET_REFCOUNT(buf_state), loccount);
	pfree(path);
}

/*
 * CheckPointBuffers
 *
 * Flush all dirty blocks in buffer pool to disk at checkpoint time.
 *
 * Note: temporary relations do not participate in checkpoints, so they don't
 * need to be flushed.
 */
void
CheckPointBuffers(int flags)
{
	BufferSync(flags);
}

/*
 * BufferGetBlockNumber
 *		Returns the block number associated with a buffer.
 *
 * Note:
 *		Assumes that the buffer is valid and pinned, else the
 *		value may be obsolete immediately...
 */
BlockNumber
BufferGetBlockNumber(Buffer buffer)
{
	BufferDesc *bufHdr;

	Assert(BufferIsPinned(buffer));

	if (BufferIsLocal(buffer))
		bufHdr = GetLocalBufferDescriptor(-buffer - 1);
	else
		bufHdr = GetBufferDescriptor(buffer - 1);

	/* pinned, so OK to read tag without spinlock */
	return bufHdr->tag.blockNum;
}

/*
 * BufferGetTag
 *		Returns the relfilelocator, fork number and block number associated with
 *		a buffer.
 */
void
BufferGetTag(Buffer buffer, RelFileLocator *rlocator, ForkNumber *forknum,
			 BlockNumber *blknum)
{
	BufferDesc *bufHdr;

	/* Do the same checks as BufferGetBlockNumber. */
	Assert(BufferIsPinned(buffer));

	if (BufferIsLocal(buffer))
		bufHdr = GetLocalBufferDescriptor(-buffer - 1);
	else
		bufHdr = GetBufferDescriptor(buffer - 1);

	/* pinned, so OK to read tag without spinlock */
	*rlocator = BufTagGetRelFileLocator(&bufHdr->tag);
	*forknum = BufTagGetForkNum(&bufHdr->tag);
	*blknum = bufHdr->tag.blockNum;
}

/*
 * FlushBuffer
 *		Physically write out a shared buffer.
 *
 * NOTE: this actually just passes the buffer contents to the kernel; the
 * real write to disk won't happen until the kernel feels like it.  This
 * is okay from our point of view since we can redo the changes from WAL.
 * However, we will need to force the changes to disk via fsync before
 * we can checkpoint WAL.
 *
 * The caller must hold a pin on the buffer and have share-locked the
 * buffer contents.  (Note: a share-lock does not prevent updates of
 * hint bits in the buffer, so the page could change while the write
 * is in progress, but we assume that that will not invalidate the data
 * written.)
 *
 * If the caller has an smgr reference for the buffer's relation, pass it
 * as the second parameter.  If not, pass NULL.
 */
static void
FlushBuffer(BufferDesc *buf, SMgrRelation reln, IOObject io_object,
			IOContext io_context)
{
	XLogRecPtr	recptr;
	ErrorContextCallback errcallback;
	instr_time	io_start;
	Block		bufBlock;
	char	   *bufToWrite;
	uint32		buf_state;

#ifdef USE_PGRAC_CLUSTER

	/*
	 * Caller holds content SHARE.  Retain/release require EXCLUSIVE, so this
	 * coherent PI+VALID observation cannot change before return.  Never even
	 * enter StartBufferIO for old transfer bytes.
	 */
	buf_state = LockBufHdr(buf);
	if (cluster_bufmgr_pcm_x_retained_image_locked(buf, buf_state))
	{
		UnlockBufHdr(buf, buf_state);
		return;
	}
	UnlockBufHdr(buf, buf_state);
#endif

	/*
	 * Try to start an I/O operation.  If StartBufferIO returns false, then
	 * someone else flushed the buffer before we could, so we need not do
	 * anything.
	 */
	if (!StartBufferIO(buf, false))
		return;

#ifdef USE_PGRAC_CLUSTER

	/*
	 * PGRAC spec-6.2 D7: a Smart Fusion dependent buffer must not be written
	 * to shared storage before every origin dependency is observed durable.
	 * End this I/O attempt without clearing BM_DIRTY; a later checkpoint or
	 * bgwriter pass can write it after the dependency vector is cleared.
	 */
	if (cluster_smart_fusion && cluster_sf_dep_buffer_flush_blocked(buf)) {
		TerminateBufferIO(buf, false, 0);
		return;
	}
#endif

	/* Setup error traceback support for ereport() */
	errcallback.callback = shared_buffer_write_error_callback;
	errcallback.arg = (void *) buf;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/* Find smgr relation for buffer */
	if (reln == NULL)
		reln = smgropen(BufTagGetRelFileLocator(&buf->tag), InvalidBackendId);

	TRACE_POSTGRESQL_BUFFER_FLUSH_START(BufTagGetForkNum(&buf->tag),
										buf->tag.blockNum,
										reln->smgr_rlocator.locator.spcOid,
										reln->smgr_rlocator.locator.dbOid,
										reln->smgr_rlocator.locator.relNumber);

	buf_state = LockBufHdr(buf);

	/*
	 * Run PageGetLSN while holding header lock, since we don't have the
	 * buffer locked exclusively in all cases.
	 */
	recptr = BufferGetLSN(buf);

	/* To check if block content changes while flushing. - vadim 01/17/97 */
	buf_state &= ~BM_JUST_DIRTIED;
	UnlockBufHdr(buf, buf_state);

	/*
	 * Force XLOG flush up to buffer's LSN.  This implements the basic WAL
	 * rule that log updates must hit disk before any of the data-file changes
	 * they describe do.
	 *
	 * However, this rule does not apply to unlogged relations, which will be
	 * lost after a crash anyway.  Most unlogged relation pages do not bear
	 * LSNs since we never emit WAL records for them, and therefore flushing
	 * up through the buffer LSN would be useless, but harmless.  However,
	 * GiST indexes use LSNs internally to track page-splits, and therefore
	 * unlogged GiST pages bear "fake" LSNs generated by
	 * GetFakeLSNForUnloggedRel.  It is unlikely but possible that the fake
	 * LSN counter could advance past the WAL insertion point; and if it did
	 * happen, attempting to flush WAL through that location would fail, with
	 * disastrous system-wide consequences.  To make sure that can't happen,
	 * skip the flush if the buffer isn't permanent.
	 */
#ifdef USE_PGRAC_CLUSTER

	/*
	 * PGRAC (spec-6.14 D8): a shipped Cache Fusion image carries the
	 * ORIGIN's page LSN.  Every ship path flushes the origin's WAL through
	 * that LSN before the bytes go on the wire (HC82 master serve, the
	 * xheld-read holder forward, and the X-transfer capture), so
	 * WAL-before-data for the shipped content is already durable at the
	 * origin.  This node cannot flush another node's WAL: XLogFlush would
	 * compare the foreign LSN against the LOCAL flush horizon and, whenever
	 * the local WAL is numerically behind, FATAL out ("xlog flush request
	 * is not satisfied" -- a mostly-read node's shutdown checkpoint writing
	 * a hint-dirtied shipped page, the t/337 crash).  An LSN beyond the
	 * local insert position can only be foreign: local WAL stamps never
	 * exceed the local insert point.  Skip the local flush for such a page;
	 * the only local additions on top of a shipped image are hint-class
	 * changes (ITL lazy cleanout, index kill bits), which emit no WAL.
	 */
	if (cluster_storage_mode_enabled() && !RecoveryInProgress()
		&& recptr > GetXLogInsertRecPtr())
		recptr = InvalidXLogRecPtr;
#endif
	if (buf_state & BM_PERMANENT)
		XLogFlush(recptr);

	/*
	 * Now it's safe to write buffer to disk. Note that no one else should
	 * have been able to write it while we were busy with log flushing because
	 * only one process at a time can set the BM_IO_IN_PROGRESS bit.
	 */
	bufBlock = BufHdrGetBlock(buf);

	/*
	 * Update page checksum if desired.  Since we have only shared lock on the
	 * buffer, other processes might be updating hint bits in it, so we must
	 * copy the page to private storage if we do checksumming.
	 */
	bufToWrite = PageSetChecksumCopy((Page) bufBlock, buf->tag.blockNum);

	io_start = pgstat_prepare_io_time();

	/*
	 * bufToWrite is either the shared buffer or a copy, as appropriate.
	 */
	smgrwrite(reln,
			  BufTagGetForkNum(&buf->tag),
			  buf->tag.blockNum,
			  bufToWrite,
			  false);

#ifdef USE_PGRAC_CLUSTER

	/*
	 * PGRAC spec-6.12h D-h2 (Q25-A "写盘成功" face): a cluster-tracked block's
	 * CURRENT copy was just written toward shared storage.  Record (tag,
	 * pd_block_scn) in the PI-discard note ring; the note becomes a discard
	 * trigger only after the checkpointer's sync phase proves it durable
	 * (the "checkpoint 推进" face brackets ProcessSyncRequests).  bufToWrite
	 * is the flushed image, so the recorded pd_block_scn is exactly the
	 * version that reached storage (the page LSN is deliberately not part of
	 * the protocol: per-thread WAL keeps LSNs in per-node spaces, so only
	 * the AD-008 Lamport pd_block_scn is cross-node comparable).  Advisory:
	 * pcm_state is read unlocked (a spurious or missed note is harmless —
	 * the master's watermark check and the holder's strict PI-only drop own
	 * correctness).
	 */
	if (cluster_past_image && buf->pcm_state != (uint8) PCM_STATE_N)
		cluster_gcs_block_pi_write_note(buf->tag,
										((PageHeader) bufToWrite)->pd_block_scn);
#endif

	/*
	 * When a strategy is in use, only flushes of dirty buffers already in the
	 * strategy ring are counted as strategy writes (IOCONTEXT
	 * [BULKREAD|BULKWRITE|VACUUM] IOOP_WRITE) for the purpose of IO
	 * statistics tracking.
	 *
	 * If a shared buffer initially added to the ring must be flushed before
	 * being used, this is counted as an IOCONTEXT_NORMAL IOOP_WRITE.
	 *
	 * If a shared buffer which was added to the ring later because the
	 * current strategy buffer is pinned or in use or because all strategy
	 * buffers were dirty and rejected (for BAS_BULKREAD operations only)
	 * requires flushing, this is counted as an IOCONTEXT_NORMAL IOOP_WRITE
	 * (from_ring will be false).
	 *
	 * When a strategy is not in use, the write can only be a "regular" write
	 * of a dirty shared buffer (IOCONTEXT_NORMAL IOOP_WRITE).
	 */
	pgstat_count_io_op_time(IOOBJECT_RELATION, io_context,
							IOOP_WRITE, io_start, 1);

	pgBufferUsage.shared_blks_written++;

	/*
	 * Mark the buffer as clean (unless BM_JUST_DIRTIED has become set) and
	 * end the BM_IO_IN_PROGRESS state.
	 */
	TerminateBufferIO(buf, true, 0);

	TRACE_POSTGRESQL_BUFFER_FLUSH_DONE(BufTagGetForkNum(&buf->tag),
									   buf->tag.blockNum,
									   reln->smgr_rlocator.locator.spcOid,
									   reln->smgr_rlocator.locator.dbOid,
									   reln->smgr_rlocator.locator.relNumber);

	/* Pop the error context stack */
	error_context_stack = errcallback.previous;
}

/*
 * RelationGetNumberOfBlocksInFork
 *		Determines the current number of pages in the specified relation fork.
 *
 * Note that the accuracy of the result will depend on the details of the
 * relation's storage. For builtin AMs it'll be accurate, but for external AMs
 * it might not be.
 */
BlockNumber
RelationGetNumberOfBlocksInFork(Relation relation, ForkNumber forkNum)
{
	if (RELKIND_HAS_TABLE_AM(relation->rd_rel->relkind))
	{
		/*
		 * Not every table AM uses BLCKSZ wide fixed size blocks. Therefore
		 * tableam returns the size in bytes - but for the purpose of this
		 * routine, we want the number of blocks. Therefore divide, rounding
		 * up.
		 */
		uint64		szbytes;

		szbytes = table_relation_size(relation, forkNum);

		return (szbytes + (BLCKSZ - 1)) / BLCKSZ;
	}
	else if (RELKIND_HAS_STORAGE(relation->rd_rel->relkind))
	{
		return smgrnblocks(RelationGetSmgr(relation), forkNum);
	}
	else
		Assert(false);

	return 0;					/* keep compiler quiet */
}

/*
 * BufferIsPermanent
 *		Determines whether a buffer will potentially still be around after
 *		a crash.  Caller must hold a buffer pin.
 */
bool
BufferIsPermanent(Buffer buffer)
{
	BufferDesc *bufHdr;

	/* Local buffers are used only for temp relations. */
	if (BufferIsLocal(buffer))
		return false;

	/* Make sure we've got a real buffer, and that we hold a pin on it. */
	Assert(BufferIsValid(buffer));
	Assert(BufferIsPinned(buffer));

	/*
	 * BM_PERMANENT can't be changed while we hold a pin on the buffer, so we
	 * need not bother with the buffer header spinlock.  Even if someone else
	 * changes the buffer header state while we're doing this, the state is
	 * changed atomically, so we'll read the old value or the new value, but
	 * not random garbage.
	 */
	bufHdr = GetBufferDescriptor(buffer - 1);
	return (pg_atomic_read_u32(&bufHdr->state) & BM_PERMANENT) != 0;
}

/*
 * BufferGetLSNAtomic
 *		Retrieves the LSN of the buffer atomically using a buffer header lock.
 *		This is necessary for some callers who may not have an exclusive lock
 *		on the buffer.
 */
XLogRecPtr
BufferGetLSNAtomic(Buffer buffer)
{
	char	   *page = BufferGetPage(buffer);
	BufferDesc *bufHdr;
	XLogRecPtr	lsn;
	uint32		buf_state;

	/*
	 * If we don't need locking for correctness, fastpath out.
	 */
	if (!XLogHintBitIsNeeded() || BufferIsLocal(buffer))
		return PageGetLSN(page);

	/* Make sure we've got a real buffer, and that we hold a pin on it. */
	Assert(BufferIsValid(buffer));
	Assert(BufferIsPinned(buffer));

	bufHdr = GetBufferDescriptor(buffer - 1);
	buf_state = LockBufHdr(bufHdr);
	lsn = PageGetLSN(page);
	UnlockBufHdr(bufHdr, buf_state);

	return lsn;
}

/* ---------------------------------------------------------------------
 *		DropRelationBuffers
 *
 *		This function removes from the buffer pool all the pages of the
 *		specified relation forks that have block numbers >= firstDelBlock.
 *		(In particular, with firstDelBlock = 0, all pages are removed.)
 *		Dirty pages are simply dropped, without bothering to write them
 *		out first.  Therefore, this is NOT rollback-able, and so should be
 *		used only with extreme caution!
 *
 *		Currently, this is called only from smgr.c when the underlying file
 *		is about to be deleted or truncated (firstDelBlock is needed for
 *		the truncation case).  The data in the affected pages would therefore
 *		be deleted momentarily anyway, and there is no point in writing it.
 *		It is the responsibility of higher-level code to ensure that the
 *		deletion or truncation does not lose any data that could be needed
 *		later.  It is also the responsibility of higher-level code to ensure
 *		that no other process could be trying to load more pages of the
 *		relation into buffers.
 * --------------------------------------------------------------------
 */
void
DropRelationBuffers(SMgrRelation smgr_reln, ForkNumber *forkNum,
					int nforks, BlockNumber *firstDelBlock)
{
	int			i;
	int			j;
	RelFileLocatorBackend rlocator;
	BlockNumber nForkBlock[MAX_FORKNUM];
	uint64		nBlocksToInvalidate = 0;

	rlocator = smgr_reln->smgr_rlocator;

	/* If it's a local relation, it's localbuf.c's problem. */
	if (RelFileLocatorBackendIsTemp(rlocator))
	{
		if (rlocator.backend == MyBackendId)
		{
			for (j = 0; j < nforks; j++)
				DropRelationLocalBuffers(rlocator.locator, forkNum[j],
										 firstDelBlock[j]);
		}
		return;
	}

	/*
	 * To remove all the pages of the specified relation forks from the buffer
	 * pool, we need to scan the entire buffer pool but we can optimize it by
	 * finding the buffers from BufMapping table provided we know the exact
	 * size of each fork of the relation. The exact size is required to ensure
	 * that we don't leave any buffer for the relation being dropped as
	 * otherwise the background writer or checkpointer can lead to a PANIC
	 * error while flushing buffers corresponding to files that don't exist.
	 *
	 * To know the exact size, we rely on the size cached for each fork by us
	 * during recovery which limits the optimization to recovery and on
	 * standbys but we can easily extend it once we have shared cache for
	 * relation size.
	 *
	 * In recovery, we cache the value returned by the first lseek(SEEK_END)
	 * and the future writes keeps the cached value up-to-date. See
	 * smgrextend. It is possible that the value of the first lseek is smaller
	 * than the actual number of existing blocks in the file due to buggy
	 * Linux kernels that might not have accounted for the recent write. But
	 * that should be fine because there must not be any buffers after that
	 * file size.
	 */
	for (i = 0; i < nforks; i++)
	{
		/* Get the number of blocks for a relation's fork */
		nForkBlock[i] = smgrnblocks_cached(smgr_reln, forkNum[i]);

		if (nForkBlock[i] == InvalidBlockNumber)
		{
			nBlocksToInvalidate = InvalidBlockNumber;
			break;
		}

		/* calculate the number of blocks to be invalidated */
		nBlocksToInvalidate += (nForkBlock[i] - firstDelBlock[i]);
	}

	/*
	 * We apply the optimization iff the total number of blocks to invalidate
	 * is below the BUF_DROP_FULL_SCAN_THRESHOLD.
	 */
	if (BlockNumberIsValid(nBlocksToInvalidate) &&
		nBlocksToInvalidate < BUF_DROP_FULL_SCAN_THRESHOLD)
	{
		for (j = 0; j < nforks; j++)
			FindAndDropRelationBuffers(rlocator.locator, forkNum[j],
									   nForkBlock[j], firstDelBlock[j]);
		return;
	}

	for (i = 0; i < NBuffers; i++)
	{
		BufferDesc *bufHdr = GetBufferDescriptor(i);
		uint32		buf_state;

		/*
		 * We can make this a tad faster by prechecking the buffer tag before
		 * we attempt to lock the buffer; this saves a lot of lock
		 * acquisitions in typical cases.  It should be safe because the
		 * caller must have AccessExclusiveLock on the relation, or some other
		 * reason to be certain that no one is loading new pages of the rel
		 * into the buffer pool.  (Otherwise we might well miss such pages
		 * entirely.)  Therefore, while the tag might be changing while we
		 * look at it, it can't be changing *to* a value we care about, only
		 * *away* from such a value.  So false negatives are impossible, and
		 * false positives are safe because we'll recheck after getting the
		 * buffer lock.
		 *
		 * We could check forkNum and blockNum as well as the rlocator, but
		 * the incremental win from doing so seems small.
		 */
		if (!BufTagMatchesRelFileLocator(&bufHdr->tag, &rlocator.locator))
			continue;

		buf_state = LockBufHdr(bufHdr);

		for (j = 0; j < nforks; j++)
		{
			if (BufTagMatchesRelFileLocator(&bufHdr->tag, &rlocator.locator) &&
				BufTagGetForkNum(&bufHdr->tag) == forkNum[j] &&
				bufHdr->tag.blockNum >= firstDelBlock[j])
			{
				InvalidateBuffer(bufHdr);	/* releases spinlock */
				break;
			}
		}
		if (j >= nforks)
			UnlockBufHdr(bufHdr, buf_state);
	}
}

/* ---------------------------------------------------------------------
 *		DropRelationsAllBuffers
 *
 *		This function removes from the buffer pool all the pages of all
 *		forks of the specified relations.  It's equivalent to calling
 *		DropRelationBuffers once per fork per relation with firstDelBlock = 0.
 *		--------------------------------------------------------------------
 */
void
DropRelationsAllBuffers(SMgrRelation *smgr_reln, int nlocators)
{
	int			i;
	int			n = 0;
	SMgrRelation *rels;
	BlockNumber (*block)[MAX_FORKNUM + 1];
	uint64		nBlocksToInvalidate = 0;
	RelFileLocator *locators;
	bool		cached = true;
	bool		use_bsearch;

	if (nlocators == 0)
		return;

	rels = palloc(sizeof(SMgrRelation) * nlocators);	/* non-local relations */

	/* If it's a local relation, it's localbuf.c's problem. */
	for (i = 0; i < nlocators; i++)
	{
		if (RelFileLocatorBackendIsTemp(smgr_reln[i]->smgr_rlocator))
		{
			if (smgr_reln[i]->smgr_rlocator.backend == MyBackendId)
				DropRelationAllLocalBuffers(smgr_reln[i]->smgr_rlocator.locator);
		}
		else
			rels[n++] = smgr_reln[i];
	}

	/*
	 * If there are no non-local relations, then we're done. Release the
	 * memory and return.
	 */
	if (n == 0)
	{
		pfree(rels);
		return;
	}

	/*
	 * This is used to remember the number of blocks for all the relations
	 * forks.
	 */
	block = (BlockNumber (*)[MAX_FORKNUM + 1])
		palloc(sizeof(BlockNumber) * n * (MAX_FORKNUM + 1));

	/*
	 * We can avoid scanning the entire buffer pool if we know the exact size
	 * of each of the given relation forks. See DropRelationBuffers.
	 */
	for (i = 0; i < n && cached; i++)
	{
		for (int j = 0; j <= MAX_FORKNUM; j++)
		{
			/* Get the number of blocks for a relation's fork. */
			block[i][j] = smgrnblocks_cached(rels[i], j);

			/* We need to only consider the relation forks that exists. */
			if (block[i][j] == InvalidBlockNumber)
			{
				if (!smgrexists(rels[i], j))
					continue;
				cached = false;
				break;
			}

			/* calculate the total number of blocks to be invalidated */
			nBlocksToInvalidate += block[i][j];
		}
	}

	/*
	 * We apply the optimization iff the total number of blocks to invalidate
	 * is below the BUF_DROP_FULL_SCAN_THRESHOLD.
	 */
	if (cached && nBlocksToInvalidate < BUF_DROP_FULL_SCAN_THRESHOLD)
	{
		for (i = 0; i < n; i++)
		{
			for (int j = 0; j <= MAX_FORKNUM; j++)
			{
				/* ignore relation forks that doesn't exist */
				if (!BlockNumberIsValid(block[i][j]))
					continue;

				/* drop all the buffers for a particular relation fork */
				FindAndDropRelationBuffers(rels[i]->smgr_rlocator.locator,
										   j, block[i][j], 0);
			}
		}

		pfree(block);
		pfree(rels);
		return;
	}

	pfree(block);
	locators = palloc(sizeof(RelFileLocator) * n);	/* non-local relations */
	for (i = 0; i < n; i++)
		locators[i] = rels[i]->smgr_rlocator.locator;

	/*
	 * For low number of relations to drop just use a simple walk through, to
	 * save the bsearch overhead. The threshold to use is rather a guess than
	 * an exactly determined value, as it depends on many factors (CPU and RAM
	 * speeds, amount of shared buffers etc.).
	 */
	use_bsearch = n > RELS_BSEARCH_THRESHOLD;

	/* sort the list of rlocators if necessary */
	if (use_bsearch)
		pg_qsort(locators, n, sizeof(RelFileLocator), rlocator_comparator);

	for (i = 0; i < NBuffers; i++)
	{
		RelFileLocator *rlocator = NULL;
		BufferDesc *bufHdr = GetBufferDescriptor(i);
		uint32		buf_state;

		/*
		 * As in DropRelationBuffers, an unlocked precheck should be safe and
		 * saves some cycles.
		 */

		if (!use_bsearch)
		{
			int			j;

			for (j = 0; j < n; j++)
			{
				if (BufTagMatchesRelFileLocator(&bufHdr->tag, &locators[j]))
				{
					rlocator = &locators[j];
					break;
				}
			}
		}
		else
		{
			RelFileLocator locator;

			locator = BufTagGetRelFileLocator(&bufHdr->tag);
			rlocator = bsearch((const void *) &(locator),
							   locators, n, sizeof(RelFileLocator),
							   rlocator_comparator);
		}

		/* buffer doesn't belong to any of the given relfilelocators; skip it */
		if (rlocator == NULL)
			continue;

		buf_state = LockBufHdr(bufHdr);
		if (BufTagMatchesRelFileLocator(&bufHdr->tag, rlocator))
			InvalidateBuffer(bufHdr);	/* releases spinlock */
		else
			UnlockBufHdr(bufHdr, buf_state);
	}

	pfree(locators);
	pfree(rels);
}

/* ---------------------------------------------------------------------
 *		FindAndDropRelationBuffers
 *
 *		This function performs look up in BufMapping table and removes from the
 *		buffer pool all the pages of the specified relation fork that has block
 *		number >= firstDelBlock. (In particular, with firstDelBlock = 0, all
 *		pages are removed.)
 * --------------------------------------------------------------------
 */
static void
FindAndDropRelationBuffers(RelFileLocator rlocator, ForkNumber forkNum,
						   BlockNumber nForkBlock,
						   BlockNumber firstDelBlock)
{
	BlockNumber curBlock;

	for (curBlock = firstDelBlock; curBlock < nForkBlock; curBlock++)
	{
		uint32		bufHash;	/* hash value for tag */
		BufferTag	bufTag;		/* identity of requested block */
		LWLock	   *bufPartitionLock;	/* buffer partition lock for it */
		int			buf_id;
		BufferDesc *bufHdr;
		uint32		buf_state;

		/* create a tag so we can lookup the buffer */
		InitBufferTag(&bufTag, &rlocator, forkNum, curBlock);

		/* determine its hash code and partition lock ID */
		bufHash = BufTableHashCode(&bufTag);
		bufPartitionLock = BufMappingPartitionLock(bufHash);

		/* Check that it is in the buffer pool. If not, do nothing. */
		LWLockAcquire(bufPartitionLock, LW_SHARED);
		buf_id = BufTableLookup(&bufTag, bufHash);
		LWLockRelease(bufPartitionLock);

		if (buf_id < 0)
			continue;

		bufHdr = GetBufferDescriptor(buf_id);

		/*
		 * We need to lock the buffer header and recheck if the buffer is
		 * still associated with the same block because the buffer could be
		 * evicted by some other backend loading blocks for a different
		 * relation after we release lock on the BufMapping table.
		 */
		buf_state = LockBufHdr(bufHdr);

		if (BufTagMatchesRelFileLocator(&bufHdr->tag, &rlocator) &&
			BufTagGetForkNum(&bufHdr->tag) == forkNum &&
			bufHdr->tag.blockNum >= firstDelBlock)
			InvalidateBuffer(bufHdr);	/* releases spinlock */
		else
			UnlockBufHdr(bufHdr, buf_state);
	}
}

/* ---------------------------------------------------------------------
 *		DropDatabaseBuffers
 *
 *		This function removes all the buffers in the buffer cache for a
 *		particular database.  Dirty pages are simply dropped, without
 *		bothering to write them out first.  This is used when we destroy a
 *		database, to avoid trying to flush data to disk when the directory
 *		tree no longer exists.  Implementation is pretty similar to
 *		DropRelationBuffers() which is for destroying just one relation.
 * --------------------------------------------------------------------
 */
void
DropDatabaseBuffers(Oid dbid)
{
	int			i;

	/*
	 * We needn't consider local buffers, since by assumption the target
	 * database isn't our own.
	 */

	for (i = 0; i < NBuffers; i++)
	{
		BufferDesc *bufHdr = GetBufferDescriptor(i);
		uint32		buf_state;

		/*
		 * As in DropRelationBuffers, an unlocked precheck should be safe and
		 * saves some cycles.
		 */
		if (bufHdr->tag.dbOid != dbid)
			continue;

		buf_state = LockBufHdr(bufHdr);
		if (bufHdr->tag.dbOid == dbid)
			InvalidateBuffer(bufHdr);	/* releases spinlock */
		else
			UnlockBufHdr(bufHdr, buf_state);
	}
}

/* -----------------------------------------------------------------
 *		PrintBufferDescs
 *
 *		this function prints all the buffer descriptors, for debugging
 *		use only.
 * -----------------------------------------------------------------
 */
#ifdef NOT_USED
void
PrintBufferDescs(void)
{
	int			i;

	for (i = 0; i < NBuffers; ++i)
	{
		BufferDesc *buf = GetBufferDescriptor(i);
		Buffer		b = BufferDescriptorGetBuffer(buf);

		/* theoretically we should lock the bufhdr here */
		elog(LOG,
			 "[%02d] (freeNext=%d, rel=%s, "
			 "blockNum=%u, flags=0x%x, refcount=%u %d)",
			 i, buf->freeNext,
			 relpathbackend(BufTagGetRelFileLocator(&buf->tag),
							InvalidBackendId, BufTagGetForkNum(&buf->tag)),
			 buf->tag.blockNum, buf->flags,
			 buf->refcount, GetPrivateRefCount(b));
	}
}
#endif

#ifdef NOT_USED
void
PrintPinnedBufs(void)
{
	int			i;

	for (i = 0; i < NBuffers; ++i)
	{
		BufferDesc *buf = GetBufferDescriptor(i);
		Buffer		b = BufferDescriptorGetBuffer(buf);

		if (GetPrivateRefCount(b) > 0)
		{
			/* theoretically we should lock the bufhdr here */
			elog(LOG,
				 "[%02d] (freeNext=%d, rel=%s, "
				 "blockNum=%u, flags=0x%x, refcount=%u %d)",
				 i, buf->freeNext,
				 relpathperm(BufTagGetRelFileLocator(&buf->tag),
							 BufTagGetForkNum(&buf->tag)),
				 buf->tag.blockNum, buf->flags,
				 buf->refcount, GetPrivateRefCount(b));
		}
	}
}
#endif

/* ---------------------------------------------------------------------
 *		FlushRelationBuffers
 *
 *		This function writes all dirty pages of a relation out to disk
 *		(or more accurately, out to kernel disk buffers), ensuring that the
 *		kernel has an up-to-date view of the relation.
 *
 *		Generally, the caller should be holding AccessExclusiveLock on the
 *		target relation to ensure that no other backend is busy dirtying
 *		more blocks of the relation; the effects can't be expected to last
 *		after the lock is released.
 *
 *		XXX currently it sequentially searches the buffer pool, should be
 *		changed to more clever ways of searching.  This routine is not
 *		used in any performance-critical code paths, so it's not worth
 *		adding additional overhead to normal paths to make it go faster.
 * --------------------------------------------------------------------
 */
void
FlushRelationBuffers(Relation rel)
{
	int			i;
	BufferDesc *bufHdr;

	if (RelationUsesLocalBuffers(rel))
	{
		for (i = 0; i < NLocBuffer; i++)
		{
			uint32		buf_state;
			instr_time	io_start;

			bufHdr = GetLocalBufferDescriptor(i);
			if (BufTagMatchesRelFileLocator(&bufHdr->tag, &rel->rd_locator) &&
				((buf_state = pg_atomic_read_u32(&bufHdr->state)) &
				 (BM_VALID | BM_DIRTY)) == (BM_VALID | BM_DIRTY))
			{
				ErrorContextCallback errcallback;
				Page		localpage;

				localpage = (char *) LocalBufHdrGetBlock(bufHdr);

				/* Setup error traceback support for ereport() */
				errcallback.callback = local_buffer_write_error_callback;
				errcallback.arg = (void *) bufHdr;
				errcallback.previous = error_context_stack;
				error_context_stack = &errcallback;

				PageSetChecksumInplace(localpage, bufHdr->tag.blockNum);

				io_start = pgstat_prepare_io_time();

				smgrwrite(RelationGetSmgr(rel),
						  BufTagGetForkNum(&bufHdr->tag),
						  bufHdr->tag.blockNum,
						  localpage,
						  false);

				pgstat_count_io_op_time(IOOBJECT_TEMP_RELATION,
										IOCONTEXT_NORMAL, IOOP_WRITE,
										io_start, 1);

				buf_state &= ~(BM_DIRTY | BM_JUST_DIRTIED);
				pg_atomic_unlocked_write_u32(&bufHdr->state, buf_state);

				pgBufferUsage.local_blks_written++;

				/* Pop the error context stack */
				error_context_stack = errcallback.previous;
			}
		}

		return;
	}

	/* Make sure we can handle the pin inside the loop */
	ResourceOwnerEnlargeBuffers(CurrentResourceOwner);

	for (i = 0; i < NBuffers; i++)
	{
		uint32		buf_state;

		bufHdr = GetBufferDescriptor(i);

		/*
		 * As in DropRelationBuffers, an unlocked precheck should be safe and
		 * saves some cycles.
		 */
		if (!BufTagMatchesRelFileLocator(&bufHdr->tag, &rel->rd_locator))
			continue;

		ReservePrivateRefCountEntry();

		buf_state = LockBufHdr(bufHdr);
		if (BufTagMatchesRelFileLocator(&bufHdr->tag, &rel->rd_locator) &&
			(buf_state & (BM_VALID | BM_DIRTY)) == (BM_VALID | BM_DIRTY))
		{
			PinBuffer_Locked(bufHdr);
			LWLockAcquire(BufferDescriptorGetContentLock(bufHdr), LW_SHARED);
			FlushBuffer(bufHdr, RelationGetSmgr(rel), IOOBJECT_RELATION, IOCONTEXT_NORMAL);
			LWLockRelease(BufferDescriptorGetContentLock(bufHdr));
			UnpinBuffer(bufHdr);
		}
		else
			UnlockBufHdr(bufHdr, buf_state);
	}
}

/* ---------------------------------------------------------------------
 *		FlushRelationsAllBuffers
 *
 *		This function flushes out of the buffer pool all the pages of all
 *		forks of the specified smgr relations.  It's equivalent to calling
 *		FlushRelationBuffers once per relation.  The relations are assumed not
 *		to use local buffers.
 * --------------------------------------------------------------------
 */
void
FlushRelationsAllBuffers(SMgrRelation *smgrs, int nrels)
{
	int			i;
	SMgrSortArray *srels;
	bool		use_bsearch;

	if (nrels == 0)
		return;

	/* fill-in array for qsort */
	srels = palloc(sizeof(SMgrSortArray) * nrels);

	for (i = 0; i < nrels; i++)
	{
		Assert(!RelFileLocatorBackendIsTemp(smgrs[i]->smgr_rlocator));

		srels[i].rlocator = smgrs[i]->smgr_rlocator.locator;
		srels[i].srel = smgrs[i];
	}

	/*
	 * Save the bsearch overhead for low number of relations to sync. See
	 * DropRelationsAllBuffers for details.
	 */
	use_bsearch = nrels > RELS_BSEARCH_THRESHOLD;

	/* sort the list of SMgrRelations if necessary */
	if (use_bsearch)
		pg_qsort(srels, nrels, sizeof(SMgrSortArray), rlocator_comparator);

	/* Make sure we can handle the pin inside the loop */
	ResourceOwnerEnlargeBuffers(CurrentResourceOwner);

	for (i = 0; i < NBuffers; i++)
	{
		SMgrSortArray *srelent = NULL;
		BufferDesc *bufHdr = GetBufferDescriptor(i);
		uint32		buf_state;

		/*
		 * As in DropRelationBuffers, an unlocked precheck should be safe and
		 * saves some cycles.
		 */

		if (!use_bsearch)
		{
			int			j;

			for (j = 0; j < nrels; j++)
			{
				if (BufTagMatchesRelFileLocator(&bufHdr->tag, &srels[j].rlocator))
				{
					srelent = &srels[j];
					break;
				}
			}
		}
		else
		{
			RelFileLocator rlocator;

			rlocator = BufTagGetRelFileLocator(&bufHdr->tag);
			srelent = bsearch((const void *) &(rlocator),
							  srels, nrels, sizeof(SMgrSortArray),
							  rlocator_comparator);
		}

		/* buffer doesn't belong to any of the given relfilelocators; skip it */
		if (srelent == NULL)
			continue;

		ReservePrivateRefCountEntry();

		buf_state = LockBufHdr(bufHdr);
		if (BufTagMatchesRelFileLocator(&bufHdr->tag, &srelent->rlocator) &&
			(buf_state & (BM_VALID | BM_DIRTY)) == (BM_VALID | BM_DIRTY))
		{
			PinBuffer_Locked(bufHdr);
			LWLockAcquire(BufferDescriptorGetContentLock(bufHdr), LW_SHARED);
			FlushBuffer(bufHdr, srelent->srel, IOOBJECT_RELATION, IOCONTEXT_NORMAL);
			LWLockRelease(BufferDescriptorGetContentLock(bufHdr));
			UnpinBuffer(bufHdr);
		}
		else
			UnlockBufHdr(bufHdr, buf_state);
	}

	pfree(srels);
}

/* ---------------------------------------------------------------------
 *		RelationCopyStorageUsingBuffer
 *
 *		Copy fork's data using bufmgr.  Same as RelationCopyStorage but instead
 *		of using smgrread and smgrextend this will copy using bufmgr APIs.
 *
 *		Refer comments atop CreateAndCopyRelationData() for details about
 *		'permanent' parameter.
 * --------------------------------------------------------------------
 */
static void
RelationCopyStorageUsingBuffer(RelFileLocator srclocator,
							   RelFileLocator dstlocator,
							   ForkNumber forkNum, bool permanent)
{
	Buffer		srcBuf;
	Buffer		dstBuf;
	Page		srcPage;
	Page		dstPage;
	bool		use_wal;
	BlockNumber nblocks;
	BlockNumber blkno;
	PGIOAlignedBlock buf;
	BufferAccessStrategy bstrategy_src;
	BufferAccessStrategy bstrategy_dst;

	/*
	 * In general, we want to write WAL whenever wal_level > 'minimal', but we
	 * can skip it when copying any fork of an unlogged relation other than
	 * the init fork.
	 */
	use_wal = XLogIsNeeded() && (permanent || forkNum == INIT_FORKNUM);

	/* Get number of blocks in the source relation. */
	nblocks = smgrnblocks(smgropen(srclocator, InvalidBackendId),
						  forkNum);

	/* Nothing to copy; just return. */
	if (nblocks == 0)
		return;

	/*
	 * Bulk extend the destination relation of the same size as the source
	 * relation before starting to copy block by block.
	 */
	memset(buf.data, 0, BLCKSZ);
	smgrextend(smgropen(dstlocator, InvalidBackendId), forkNum, nblocks - 1,
			   buf.data, true);

	/* This is a bulk operation, so use buffer access strategies. */
	bstrategy_src = GetAccessStrategy(BAS_BULKREAD);
	bstrategy_dst = GetAccessStrategy(BAS_BULKWRITE);

	/* Iterate over each block of the source relation file. */
	for (blkno = 0; blkno < nblocks; blkno++)
	{
		CHECK_FOR_INTERRUPTS();

		/* Read block from source relation. */
		srcBuf = ReadBufferWithoutRelcache(srclocator, forkNum, blkno,
										   RBM_NORMAL, bstrategy_src,
										   permanent);
		LockBuffer(srcBuf, BUFFER_LOCK_SHARE);
		srcPage = BufferGetPage(srcBuf);

		dstBuf = ReadBufferWithoutRelcache(dstlocator, forkNum, blkno,
										   RBM_ZERO_AND_LOCK, bstrategy_dst,
										   permanent);
		dstPage = BufferGetPage(dstBuf);

		START_CRIT_SECTION();

		/* Copy page data from the source to the destination. */
		memcpy(dstPage, srcPage, BLCKSZ);
		MarkBufferDirty(dstBuf);

		/* WAL-log the copied page. */
		if (use_wal)
			log_newpage_buffer(dstBuf, true);

		END_CRIT_SECTION();

		UnlockReleaseBuffer(dstBuf);
		UnlockReleaseBuffer(srcBuf);
	}

	FreeAccessStrategy(bstrategy_src);
	FreeAccessStrategy(bstrategy_dst);
}

/* ---------------------------------------------------------------------
 *		CreateAndCopyRelationData
 *
 *		Create destination relation storage and copy all forks from the
 *		source relation to the destination.
 *
 *		Pass permanent as true for permanent relations and false for
 *		unlogged relations.  Currently this API is not supported for
 *		temporary relations.
 * --------------------------------------------------------------------
 */
void
CreateAndCopyRelationData(RelFileLocator src_rlocator,
						  RelFileLocator dst_rlocator, bool permanent)
{
	RelFileLocatorBackend rlocator;
	char		relpersistence;

	/* Set the relpersistence. */
	relpersistence = permanent ?
		RELPERSISTENCE_PERMANENT : RELPERSISTENCE_UNLOGGED;

	/*
	 * Create and copy all forks of the relation.  During create database we
	 * have a separate cleanup mechanism which deletes complete database
	 * directory.  Therefore, each individual relation doesn't need to be
	 * registered for cleanup.
	 */
	RelationCreateStorage(dst_rlocator, relpersistence, false);

	/* copy main fork. */
	RelationCopyStorageUsingBuffer(src_rlocator, dst_rlocator, MAIN_FORKNUM,
								   permanent);

	/* copy those extra forks that exist */
	for (ForkNumber forkNum = MAIN_FORKNUM + 1;
		 forkNum <= MAX_FORKNUM; forkNum++)
	{
		if (smgrexists(smgropen(src_rlocator, InvalidBackendId), forkNum))
		{
			smgrcreate(smgropen(dst_rlocator, InvalidBackendId), forkNum, false);

			/*
			 * WAL log creation if the relation is persistent, or this is the
			 * init fork of an unlogged relation.
			 */
			if (permanent || forkNum == INIT_FORKNUM)
				log_smgrcreate(&dst_rlocator, forkNum);

			/* Copy a fork's data, block by block. */
			RelationCopyStorageUsingBuffer(src_rlocator, dst_rlocator, forkNum,
										   permanent);
		}
	}

	/* close source and destination smgr if exists. */
	rlocator.backend = InvalidBackendId;

	rlocator.locator = src_rlocator;
	smgrcloserellocator(rlocator);

	rlocator.locator = dst_rlocator;
	smgrcloserellocator(rlocator);
}

/* ---------------------------------------------------------------------
 *		FlushDatabaseBuffers
 *
 *		This function writes all dirty pages of a database out to disk
 *		(or more accurately, out to kernel disk buffers), ensuring that the
 *		kernel has an up-to-date view of the database.
 *
 *		Generally, the caller should be holding an appropriate lock to ensure
 *		no other backend is active in the target database; otherwise more
 *		pages could get dirtied.
 *
 *		Note we don't worry about flushing any pages of temporary relations.
 *		It's assumed these wouldn't be interesting.
 * --------------------------------------------------------------------
 */
void
FlushDatabaseBuffers(Oid dbid)
{
	int			i;
	BufferDesc *bufHdr;

	/* Make sure we can handle the pin inside the loop */
	ResourceOwnerEnlargeBuffers(CurrentResourceOwner);

	for (i = 0; i < NBuffers; i++)
	{
		uint32		buf_state;

		bufHdr = GetBufferDescriptor(i);

		/*
		 * As in DropRelationBuffers, an unlocked precheck should be safe and
		 * saves some cycles.
		 */
		if (bufHdr->tag.dbOid != dbid)
			continue;

		ReservePrivateRefCountEntry();

		buf_state = LockBufHdr(bufHdr);
		if (bufHdr->tag.dbOid == dbid &&
			(buf_state & (BM_VALID | BM_DIRTY)) == (BM_VALID | BM_DIRTY))
		{
			PinBuffer_Locked(bufHdr);
			LWLockAcquire(BufferDescriptorGetContentLock(bufHdr), LW_SHARED);
			FlushBuffer(bufHdr, NULL, IOOBJECT_RELATION, IOCONTEXT_NORMAL);
			LWLockRelease(BufferDescriptorGetContentLock(bufHdr));
			UnpinBuffer(bufHdr);
		}
		else
			UnlockBufHdr(bufHdr, buf_state);
	}
}

/*
 * Flush a previously, shared or exclusively, locked and pinned buffer to the
 * OS.
 */
void
FlushOneBuffer(Buffer buffer)
{
	BufferDesc *bufHdr;

	/* currently not needed, but no fundamental reason not to support */
	Assert(!BufferIsLocal(buffer));

	Assert(BufferIsPinned(buffer));

	bufHdr = GetBufferDescriptor(buffer - 1);

	Assert(LWLockHeldByMe(BufferDescriptorGetContentLock(bufHdr)));

	FlushBuffer(bufHdr, NULL, IOOBJECT_RELATION, IOCONTEXT_NORMAL);
}

/*
 * ReleaseBuffer -- release the pin on a buffer
 */
void
ReleaseBuffer(Buffer buffer)
{
	if (!BufferIsValid(buffer))
		elog(ERROR, "bad buffer ID: %d", buffer);

	if (BufferIsLocal(buffer))
		UnpinLocalBuffer(buffer);
	else
		UnpinBuffer(GetBufferDescriptor(buffer - 1));
}

/*
 * UnlockReleaseBuffer -- release the content lock and pin on a buffer
 *
 * This is just a shorthand for a common combination.
 */
void
UnlockReleaseBuffer(Buffer buffer)
{
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	ReleaseBuffer(buffer);
}

/*
 * IncrBufferRefCount
 *		Increment the pin count on a buffer that we have *already* pinned
 *		at least once.
 *
 *		This function cannot be used on a buffer we do not have pinned,
 *		because it doesn't change the shared buffer state.
 */
void
IncrBufferRefCount(Buffer buffer)
{
	Assert(BufferIsPinned(buffer));
	ResourceOwnerEnlargeBuffers(CurrentResourceOwner);
	if (BufferIsLocal(buffer))
		LocalRefCount[-buffer - 1]++;
	else
	{
		PrivateRefCountEntry *ref;

		ref = GetPrivateRefCountEntry(buffer, true);
		Assert(ref != NULL);
		ref->refcount++;
	}
	ResourceOwnerRememberBuffer(CurrentResourceOwner, buffer);
}

/*
 * MarkBufferDirtyHint
 *
 *	Mark a buffer dirty for non-critical changes.
 *
 * This is essentially the same as MarkBufferDirty, except:
 *
 * 1. The caller does not write WAL; so if checksums are enabled, we may need
 *	  to write an XLOG_FPI_FOR_HINT WAL record to protect against torn pages.
 * 2. The caller might have only share-lock instead of exclusive-lock on the
 *	  buffer's content lock.
 * 3. This function does not guarantee that the buffer is always marked dirty
 *	  (due to a race condition), so it cannot be used for important changes.
 */
void
MarkBufferDirtyHint(Buffer buffer, bool buffer_std)
{
	BufferDesc *bufHdr;
	Page		page = BufferGetPage(buffer);
#ifdef USE_PGRAC_CLUSTER
	uint32		retained_state;
#endif

	if (!BufferIsValid(buffer))
		elog(ERROR, "bad buffer ID: %d", buffer);

	if (BufferIsLocal(buffer))
	{
		MarkLocalBufferDirty(buffer);
		return;
	}

	bufHdr = GetBufferDescriptor(buffer - 1);

	Assert(GetPrivateRefCount(buffer) > 0);
	/* here, either share or exclusive lock is OK */
	Assert(LWLockHeldByMe(BufferDescriptorGetContentLock(bufHdr)));

#ifdef USE_PGRAC_CLUSTER

	/*
	 * Hint changes are optional.  A retained image may already have had an
	 * in-memory hint bit touched by its caller, but it must never regain
	 * dirty state and become eligible for stale output.
	 */
	retained_state = LockBufHdr(bufHdr);
	if (cluster_bufmgr_pcm_x_retained_image_locked(bufHdr, retained_state))
	{
		UnlockBufHdr(bufHdr, retained_state);
		return;
	}
	UnlockBufHdr(bufHdr, retained_state);
#endif

	/*
	 * This routine might get called many times on the same page, if we are
	 * making the first scan after commit of an xact that added/deleted many
	 * tuples. So, be as quick as we can if the buffer is already dirty.  We
	 * do this by not acquiring spinlock if it looks like the status bits are
	 * already set.  Since we make this test unlocked, there's a chance we
	 * might fail to notice that the flags have just been cleared, and failed
	 * to reset them, due to memory-ordering issues.  But since this function
	 * is only intended to be used in cases where failing to write out the
	 * data would be harmless anyway, it doesn't really matter.
	 */
	if ((pg_atomic_read_u32(&bufHdr->state) & (BM_DIRTY | BM_JUST_DIRTIED)) !=
		(BM_DIRTY | BM_JUST_DIRTIED))
	{
		XLogRecPtr	lsn = InvalidXLogRecPtr;
		bool		dirtied = false;
		bool		delayChkptFlags = false;
		uint32		buf_state;

		/*
		 * If we need to protect hint bit updates from torn writes, WAL-log a
		 * full page image of the page. This full page image is only necessary
		 * if the hint bit update is the first change to the page since the
		 * last checkpoint.
		 *
		 * We don't check full_page_writes here because that logic is included
		 * when we call XLogInsert() since the value changes dynamically.
		 */
		if (XLogHintBitIsNeeded() &&
			(pg_atomic_read_u32(&bufHdr->state) & BM_PERMANENT))
		{
			/*
			 * If we must not write WAL, due to a relfilelocator-specific
			 * condition or being in recovery, don't dirty the page.  We can
			 * set the hint, just not dirty the page as a result so the hint
			 * is lost when we evict the page or shutdown.
			 *
			 * See src/backend/storage/page/README for longer discussion.
			 */
			if (RecoveryInProgress() ||
				RelFileLocatorSkippingWAL(BufTagGetRelFileLocator(&bufHdr->tag)))
				return;

			/*
			 * If the block is already dirty because we either made a change
			 * or set a hint already, then we don't need to write a full page
			 * image.  Note that aggressive cleaning of blocks dirtied by hint
			 * bit setting would increase the call rate. Bulk setting of hint
			 * bits would reduce the call rate...
			 *
			 * We must issue the WAL record before we mark the buffer dirty.
			 * Otherwise we might write the page before we write the WAL. That
			 * causes a race condition, since a checkpoint might occur between
			 * writing the WAL record and marking the buffer dirty. We solve
			 * that with a kluge, but one that is already in use during
			 * transaction commit to prevent race conditions. Basically, we
			 * simply prevent the checkpoint WAL record from being written
			 * until we have marked the buffer dirty. We don't start the
			 * checkpoint flush until we have marked dirty, so our checkpoint
			 * must flush the change to disk successfully or the checkpoint
			 * never gets written, so crash recovery will fix.
			 *
			 * It's possible we may enter here without an xid, so it is
			 * essential that CreateCheckPoint waits for virtual transactions
			 * rather than full transactionids.
			 */
			Assert((MyProc->delayChkptFlags & DELAY_CHKPT_START) == 0);
			MyProc->delayChkptFlags |= DELAY_CHKPT_START;
			delayChkptFlags = true;
			lsn = XLogSaveBufferForHint(buffer, buffer_std);
		}

		buf_state = LockBufHdr(bufHdr);

		Assert(BUF_STATE_GET_REFCOUNT(buf_state) > 0);

		if (!(buf_state & BM_DIRTY))
		{
			dirtied = true;		/* Means "will be dirtied by this action" */

			/*
			 * Set the page LSN if we wrote a backup block. We aren't supposed
			 * to set this when only holding a share lock but as long as we
			 * serialise it somehow we're OK. We choose to set LSN while
			 * holding the buffer header lock, which causes any reader of an
			 * LSN who holds only a share lock to also obtain a buffer header
			 * lock before using PageGetLSN(), which is enforced in
			 * BufferGetLSNAtomic().
			 *
			 * If checksums are enabled, you might think we should reset the
			 * checksum here. That will happen when the page is written
			 * sometime later in this checkpoint cycle.
			 */
			if (!XLogRecPtrIsInvalid(lsn))
				PageSetLSN(page, lsn);
		}

		buf_state |= BM_DIRTY | BM_JUST_DIRTIED;
		UnlockBufHdr(bufHdr, buf_state);

		if (delayChkptFlags)
			MyProc->delayChkptFlags &= ~DELAY_CHKPT_START;

		if (dirtied)
		{
			VacuumPageDirty++;
			pgBufferUsage.shared_blks_dirtied++;
			if (VacuumCostActive)
				VacuumCostBalance += VacuumCostPageDirty;
		}
	}
}

/*
 * Release buffer content locks for shared buffers.
 *
 * Used to clean up after errors.
 *
 * Currently, we can expect that lwlock.c's LWLockReleaseAll() took care
 * of releasing buffer content locks per se; the only thing we need to deal
 * with here is clearing any PIN_COUNT request that was in progress.
 */
void
UnlockBuffers(void)
{
	BufferDesc *buf = PinCountWaitBuf;

	if (buf)
	{
		uint32		buf_state;

		buf_state = LockBufHdr(buf);

		/*
		 * Don't complain if flag bit not set; it could have been reset but we
		 * got a cancel/die interrupt before getting the signal.
		 */
		if ((buf_state & BM_PIN_COUNT_WAITER) != 0 &&
			buf->wait_backend_pgprocno == MyProc->pgprocno)
			buf_state &= ~BM_PIN_COUNT_WAITER;

		UnlockBufHdr(buf, buf_state);

		PinCountWaitBuf = NULL;
	}
#ifdef USE_PGRAC_CLUSTER
	cluster_bufmgr_pcm_x_writer_exception_cleanup_all();
	cluster_bufmgr_pcm_x_holder_exception_cleanup_all();
#endif
}

/*
 * Acquire or release the content_lock for the buffer.
 *
 * PGRAC: spec-2.31 wires the PCM state machine as the outer lock for
 * shared-buffer content locks.  PCM acquisition happens before the local
 * content_lock; release happens after the local content_lock is released.
 *
 * PGRAC (t/400 L3 item 3): pcm_barrier_refused, when non-NULL, arms the
 * barrier-aware mode used by ClusterLockBufferExclusiveBarrierAware(): a
 * nested-guard BARRIER_CLOSED from the PCM-X queue acquire sets the flag and
 * returns WITHOUT taking the content lock, leaving the unwind to the caller
 * that owns the outer (frozen-tag) content lock.  A NULL pointer keeps the
 * historical behavior (the refusal escalates to a client ERROR).
 */
static void
LockBufferInternal(Buffer buffer, int mode, bool *pcm_barrier_refused)
{
	BufferDesc *buf;
#ifdef USE_PGRAC_CLUSTER
	bool		pcm_acquired = false;
	PcmLockMode pcm_mode = PCM_LOCK_MODE_N;
	bool		pcm_covered = false;	/* took the cached-cover fast path */
	uint64		pcm_covered_gen = 0;	/* ownership generation captured at cover */
	bool		pcm_pending_set = false;	/* we set GRANT_PENDING (W3), must clear */
	MemoryContext pcm_error_context = CurrentMemoryContext;
	ClusterPcmOwnSnapshot pcm_pending_base;
	ClusterPcmOwnResult pcm_pending_result = CLUSTER_PCM_OWN_OK;
	uint64		pcm_pending_token = 0;
	uint64		pcm_committed_generation = 0;
	ClusterPcmXHolderLedgerEntry *pcm_x_holder = NULL;
	ClusterPcmXWriterLedgerEntry *pcm_x_writer = NULL;
	bool pcm_x_writer_managed = false;
#endif

	Assert(BufferIsPinned(buffer));
	if (BufferIsLocal(buffer))
		return;					/* local buffers need no lock */

	buf = GetBufferDescriptor(buffer - 1);

	if (mode != BUFFER_LOCK_UNLOCK &&
		mode != BUFFER_LOCK_SHARE &&
		mode != BUFFER_LOCK_EXCLUSIVE)
		elog(ERROR, "unrecognized buffer lock mode: %d", mode);

#ifdef USE_PGRAC_CLUSTER
	if (mode != BUFFER_LOCK_UNLOCK &&
		cluster_pcm_is_active() &&
		cluster_bufmgr_should_pcm_track(buf))
	{
		uint8		pcm_initial_state = (uint8)PCM_STATE_N;
		uint32		pcm_initial_flags = 0;

		pcm_mode = (mode == BUFFER_LOCK_SHARE) ? PCM_LOCK_MODE_S : PCM_LOCK_MODE_X;

		/* A queue grant leaves node X cached until revoke.  Preserve that
		 * authority before deciding that this LockBuffer is a new writer
		 * conversion; otherwise every local write needlessly retires and
		 * re-enqueues the same node X.  The post-content-lock generation
		 * check below remains the race-closing authority. */
		if (pcm_mode == PCM_LOCK_MODE_X && cluster_gcs_block_local_cache) {
			cluster_pcm_own_read(buf, &pcm_initial_state, &pcm_covered_gen,
								 &pcm_initial_flags);
			pcm_covered = cluster_pcm_x_cached_cover_bypasses_queue(cluster_gcs_block_local_cache,
				pcm_mode == PCM_LOCK_MODE_X, pcm_initial_state, pcm_initial_flags);
		}

		if (!pcm_covered)
			pcm_x_writer = cluster_bufmgr_pcm_x_writer_prepare(buf, pcm_mode,
															   pcm_barrier_refused);
		if (pcm_barrier_refused != NULL && *pcm_barrier_refused)
			return;				/* barrier-aware caller owns the unwind */
		if (pcm_x_writer == NULL && !pcm_covered) {
			/*
			 * PGRAC: spec-4.7a D2 — hold-until-revoked acquire gate.
			 *
			 * If this node already holds a PCM mode that covers the requested
			 * mode (X ⊇ {S,X}, S ⊇ S), skip the remote master round-trip
			 * entirely.  buf->pcm_state is the node-level cache: it is
			 * finalized after a successful acquire (below) and cleared by the
			 * INVALIDATE handler + eviction/drop paths, so a covering value
			 * means the node still genuinely holds the lock (Rule 8.A — no
			 * stale grant).  This is what stops a bulk single-node workload
			 * from issuing one PCM round-trip per LockBuffer (the spec-4.7a
			 * D0 request storm that floods the dedup HTAB →
			 * DENIED_DEDUP_FULL → 53R90).
			 *
			 * spec-2.33 D7 — when an acquire IS needed, the buffer-aware
			 * entry point lets the GCS data-plane sender install received
			 * block bytes directly into this BufferDesc on GRANTED (HC84
			 * PageSetLSN + memcpy under content_lock EXCLUSIVE).
			 */
			if (cluster_gcs_block_local_cache
				&& cluster_pcm_mode_covers((PcmLockMode)buf->pcm_state, pcm_mode)) {
				/*
				 * Already covered locally — no master round-trip, nothing
				 * to finalize or roll back (pcm_acquired stays false).  The
				 * cover decision was made on the raw pcm_state read; a
				 * concurrent BAST downgrade (X->S) can still fire before we
				 * take the content lock below, so this cover must be
				 * RE-VERIFIED after the content lock (ownership-generation
				 * P0).
				 */
				pcm_covered = true;

				/*
				 * Capture the ownership generation NOW (header spinlock), so
				 * the post-content-lock re-verify can detect any ownership
				 * round that raced the content-lock window (a BAST X->S, or
				 * an N->X->N ABA) even when pcm_state looks unchanged
				 * (ownership-generation P0).
				 */
				cluster_pcm_own_read(buf, NULL, &pcm_covered_gen, NULL);

				/*
				 * PGRAC: spec-5.59 §3.6 read amortization probe — a share
				 * acquire served entirely from locally-held PCM state is the
				 * "S-holder hit" the probe counts (GUC-gated inside).
				 */
				if (pcm_mode == PCM_LOCK_MODE_S)
					cluster_xp_note_read(false);
			} else {
				/*
				 * PGRAC ownership-generation wave (W3): mark GRANT_PENDING
				 * before the request goes out.  Between the install (inside
				 * acquire, done under its own content lock) and this
				 * backend's finalize below, pcm_state is still N; a same-tag
				 * INVALIDATE arriving in that window must NOT treat N as
				 * already-invalidated and ACK away this in-flight grant
				 * (which would strand a stale X after finalize). The
				 * invalidate handler parks/denies while PENDING is set.
				 */
				/*
				 * Legacy acquire path: token-exact, but still pre-request.
				 * The convert-queue path must bypass this point during
				 * JOIN/WAIT and call begin_x_reservation only from
				 * ACTIVE_TRANSFER/PREPARE.
				 */
				pcm_pending_result = cluster_bufmgr_pcm_begin_grant_reservation_wait(
					buf, &pcm_pending_base, &pcm_pending_token);
				if (pcm_pending_result != CLUSTER_PCM_OWN_OK)
					cluster_pcm_own_report_bump_failure(
						buf, pcm_pending_result, pcm_pending_base.generation,
						pcm_pending_base.flags, "LockBuffer master reservation");
				pcm_pending_set = true;

				/*
				 * PGRAC: spec-5.2 D2 — a one-shot READ_IMAGE returns false
				 * (no durable grant); pcm_acquired stays false so the
				 * ownership mirror below is skipped and buf->pcm_state is
				 * left at N (the next access re-fetches — a cached copy
				 * with no invalidation path would go stale, Rule 8.A).
				 */
				PG_TRY();
				{
					pcm_acquired = cluster_pcm_lock_acquire_buffer(buf, pcm_mode);
				}
				PG_CATCH();
				{
					/*
					 * W3 — an acquire that THREW (upgrade-invalidate
					 * timeout, transfer deny, ...) must not leak
					 * GRANT_PENDING: a stale marker parks every later
					 * same-tag INVALIDATE on this node (never ACKed -> remote
					 * upgrades wedge, liveness).  The later PG_CATCH below
					 * only covers the content-lock window, not this acquire.
					 */
					cluster_pcm_own_abort_grant_after_error(
						buf, &pcm_pending_base, pcm_pending_token, "LockBuffer master acquire");
					PG_RE_THROW();
				}
				PG_END_TRY();
			}
		}
	}
#endif

	if (mode == BUFFER_LOCK_UNLOCK)
	{
#ifdef USE_PGRAC_CLUSTER
		pcm_x_writer = cluster_bufmgr_pcm_x_writer_find(buf);
		pcm_x_writer_managed = pcm_x_writer != NULL;
		pcm_x_holder = cluster_bufmgr_pcm_x_holder_find(buf);
		cluster_bufmgr_pcm_x_writer_mark_releasing(pcm_x_writer);
		cluster_bufmgr_pcm_x_holder_mark_releasing(pcm_x_holder);
#endif
		LWLockRelease(BufferDescriptorGetContentLock(buf));
#ifdef USE_PGRAC_CLUSTER
		cluster_bufmgr_pcm_x_writer_release(pcm_x_writer);
		cluster_bufmgr_pcm_x_holder_unregister(pcm_x_holder);
#endif
	}
	else
	{
#ifdef USE_PGRAC_CLUSTER
		/*
		 * GCS serve-stall round-6 (ownership P0) — cached-X BAST window.
		 * When the cover fast path above decided we already hold X, a
		 * concurrent BAST X->S self-downgrade can still grab the content lock
		 * and flip pcm_state to S in the window before we acquire it.  This
		 * inject holds that window open so the RED can drive the downgrade
		 * deterministically; the post-content-lock re-verify below is what
		 * closes it.
		 */
		/*
		 * MAIN-fork gate: the visibilitymap_pin X prefetch also runs a
		 * covered-X LockBuffer on the VM page and would otherwise consume the
		 * stall ahead of the heap block under test.
		 */
		if (pcm_covered && pcm_mode == PCM_LOCK_MODE_X
			&& BufTagGetForkNum(&buf->tag) == MAIN_FORKNUM)
			CLUSTER_INJECTION_POINT("cluster-pcm-writer-cached-x-stall");
		PG_TRY();
		{
			pcm_x_holder = cluster_bufmgr_pcm_x_holder_prepare(buf);
#endif
			if (mode == BUFFER_LOCK_SHARE)
				LWLockAcquire(BufferDescriptorGetContentLock(buf), LW_SHARED);
			else
				LWLockAcquire(BufferDescriptorGetContentLock(buf), LW_EXCLUSIVE);
#ifdef USE_PGRAC_CLUSTER
			cluster_bufmgr_pcm_x_writer_activate(pcm_x_writer);

			/*
			 * PGRAC ownership-generation wave (W1) — cached-cover
			 * re-verify. The cover fast path decided we already held the mode
			 * on a raw, unlocked pcm_state read.  A BAST X->S downgrade (or
			 * any ownership round) can have raced this content-lock window
			 * and revoked the cover.  Re-read the state/generation/flags
			 * projection of the coherent ownership tuple under the header
			 * spinlock: if the generation moved, the mode no longer covers,
			 * or a grant/revoke is in flight, the cover is STALE --
			 * writing/reading a block we no longer own is the Rule 8.A
			 * violation.  Release, do a real master re-acquire, re-take the
			 * content lock.  Bounded: after a real acquire we hold the
			 * content lock and the downgrade path serializes under it, so no
			 * further downgrade can intervene -- at most one fallback.
			 */
			if (pcm_covered)
			{
				uint8		cur_state;
				uint64		cur_gen;
				uint32		cur_flags;

				cluster_pcm_own_read(buf, &cur_state, &cur_gen, &cur_flags);
				if (cur_gen != pcm_covered_gen
					|| !cluster_pcm_mode_covers((PcmLockMode) cur_state, pcm_mode)
					|| (cur_flags & (PCM_OWN_FLAG_GRANT_PENDING | PCM_OWN_FLAG_REVOKING)) != 0)
				{
					cluster_pcm_note_writer_cover_stale_detected();
					pcm_covered = false;
					LWLockRelease(BufferDescriptorGetContentLock(buf));
					pcm_pending_result = cluster_bufmgr_pcm_begin_grant_reservation_wait(
						buf, &pcm_pending_base, &pcm_pending_token);
					if (pcm_pending_result != CLUSTER_PCM_OWN_OK)
						cluster_pcm_own_report_bump_failure(
							buf, pcm_pending_result, pcm_pending_base.generation,
							pcm_pending_base.flags, "LockBuffer revalidate reservation");
					pcm_pending_set = true;
					pcm_acquired = cluster_pcm_lock_acquire_buffer(buf, pcm_mode);
					pcm_x_holder = cluster_bufmgr_pcm_x_holder_prepare(buf);
					if (mode == BUFFER_LOCK_SHARE)
						LWLockAcquire(BufferDescriptorGetContentLock(buf), LW_SHARED);
					else
						LWLockAcquire(BufferDescriptorGetContentLock(buf), LW_EXCLUSIVE);
					cluster_pcm_note_writer_reverify_reacquire();
				}
			}
			cluster_bufmgr_pcm_x_holder_activate(pcm_x_holder);
		}
		PG_CATCH();
		{
			ErrorData  *original_error;

			/*
			 * Holder detach is itself allowed to encounter an LWLock ERROR.
			 * Preserve the content-lock failure outside ErrorContext and
			 * flush it before any cleanup begins; nested cleanup may then
			 * discard only its own error without destroying the user-visible
			 * original.
			 */
			MemoryContextSwitchTo(pcm_error_context);
			original_error = CopyErrorData();
			FlushErrorState();
			if (pcm_x_writer != NULL && !LWLockHeldByMe(BufferDescriptorGetContentLock(buf)))
				cluster_bufmgr_pcm_x_writer_abort_acquiring(pcm_x_writer);
			if (pcm_x_holder != NULL && !LWLockHeldByMe(BufferDescriptorGetContentLock(buf)))
				cluster_bufmgr_pcm_x_holder_abort_acquiring(pcm_x_holder);
			if (pcm_acquired) {
				/*
				 * PGRAC: spec-2.35 D4 (HC112) — acquire-then-LWLock-fail
				 * rollback path:  PCM acquire succeeded but LWLockAcquire
				 * threw.  Roll back the cache-residency claim using the
				 * eviction release (it fully clears the bitmap bit so the
				 * partial acquire does not leak a stale holder).
				 */
				cluster_pcm_own_rollback_grant_after_error_and_rethrow(
					buf, &pcm_pending_base, pcm_pending_token, pcm_mode, pcm_pending_set,
					original_error, pcm_error_context);
			}

			/*
			 * W3: an error before a durable acquire leaves GRANT_PENDING set
			 * -- clear it exactly so a later invalidate is not blocked by a
			 * phantom in-flight grant.
			 */
			else if (pcm_pending_set)
				cluster_pcm_own_abort_grant_after_error(buf, &pcm_pending_base, pcm_pending_token,
												"LockBuffer content-lock acquire");
			ReThrowError(original_error);
		}
		PG_END_TRY();

		/*
		 * PGRAC ownership-generation wave (W3) — grant-finalize window.  A
		 * real X acquire has installed the grant but pcm_state is still N
		 * with GRANT_PENDING set (finalize below flips it to X).  This inject
		 * holds that window open so the RED can land a peer INVALIDATE and
		 * prove the GRANT_PENDING consult parks it instead of acking the
		 * grant away.
		 */
		if (pcm_acquired && pcm_pending_set && pcm_mode == PCM_LOCK_MODE_X)
		{
			CLUSTER_INJECTION_POINT("cluster-pcm-grant-finalize-window");

			/*
			 * W3 RED delivery — when armed (:skip, one-shot), drive the
			 * REAL invalidate handler with a synthetic same-tag directive
			 * right here, while pcm_state is still N and GRANT_PENDING is
			 * set.  A master INVALIDATE cannot be steered into this window
			 * from SQL (it targets S-holders; a mirror-N node is the
			 * X-grantee and is served by X-forward instead — the real
			 * producers are master/mirror asymmetry races, e.g. a deferred
			 * eviction release).  The shim must observe the park (return
			 * false + parked counter); an already_invalidated ACK here is the
			 * W3 defect.
			 */
			CLUSTER_INJECTION_POINT("cluster-pcm-grant-finalize-deliver-invalidate");
			if (cluster_injection_should_skip(
					"cluster-pcm-grant-finalize-deliver-invalidate"))
			{
				if (cluster_gcs_block_test_deliver_self_invalidate(buf->tag))
					elog(WARNING,
						 "cluster W3 delivery shim: synthetic INVALIDATE was ACKed instead of parked (GRANT_PENDING not honored)");
			}
		}

		if (pcm_acquired)
		{
			/*
			 * Finalize the grant under the header spinlock: set buffer_type +
			 * pcm_state, bump the ownership generation, and clear
			 * GRANT_PENDING atomically.  Consumers that captured the
			 * generation before this point now observe the change
			 * (ownership-generation P0).
			 */
			cluster_pcm_own_finish_grant_or_rollback(
				buf, &pcm_pending_base, pcm_pending_token,
				(pcm_mode == PCM_LOCK_MODE_S) ? (uint8) PCM_STATE_S : (uint8) PCM_STATE_X,
				pcm_mode,
				&pcm_committed_generation);
		}
		else if (pcm_pending_set)
		{
			/* No durable grant (one-shot READ_IMAGE): clear the PENDING marker
			 * we set before the acquire so it does not linger. */
			cluster_pcm_own_abort_grant_or_error(buf, &pcm_pending_base, pcm_pending_token,
												 "LockBuffer read-image");
		}
#endif
	}

#ifdef USE_PGRAC_CLUSTER

	/*
	 * PGRAC: spec-5.2 §3.5 D11 — the deferred-writer read-image marker
	 * (PCM_STATE_READ_IMAGE) is transient: it lives only for the
	 * install->write window under this content lock so the cluster_itl
	 * forward-write guard can fail closed.  Clear it back to N on unlock,
	 * BEFORE the residency/eviction bookkeeping below, so every other PCM
	 * path treats the buffer as the unowned read-image it is.  A later
	 * LockBuffer re-acquires (cluster_pcm_mode_covers(N, ...) is false),
	 * getting real X once the remote holder is terminal.
	 */
	if (mode == BUFFER_LOCK_UNLOCK
		&& buf->pcm_state == (uint8) PCM_STATE_READ_IMAGE)
		cluster_pcm_own_transition(buf, (uint8) PCM_STATE_N, 0, 0);

	if (mode == BUFFER_LOCK_UNLOCK &&
		cluster_pcm_is_active() &&
		cluster_bufmgr_should_pcm_track(buf) &&
		buf->pcm_state != (uint8) PCM_STATE_N)
	{
		/* PGRAC: spec-2.35 D4 (HC111 + HC112) — content-lock unlock path.
		 * SCUR preserves cache residency (bit stays set so CF 2-way
		 * forward can still target this node).  XCUR delegates to the
		 * eviction release (single-holder semantic preserved).  Real
		 * cache eviction is handled by the InvalidateBuffer /
		 * InvalidateVictimBuffer / Drop*Buffers hook points (below). */
		PcmLockMode old_mode = (PcmLockMode) buf->pcm_state;

		/*
		 * PGRAC: spec-4.7a D2 — hold-until-revoked.  With the node-level
		 * cache on, the PCM lock (S residency per HC111, AND X per spec-4.7a)
		 * is kept across content-lock unlock; buf->pcm_state is preserved so
		 * the next acquire is served locally with no master round-trip.  The
		 * lock and buf->pcm_state are cleared together by the INVALIDATE
		 * handler and the eviction/drop hooks (Rule 8.A — no stale grant).
		 * With the cache off, a legacy writer falls back to the
		 * spec-2.33/2.35 behavior: release X here and mirror the queried
		 * state (X → N, S residency preserved).  A queue-managed writer
		 * must retain node X; DRAIN/RETIRE owns that release after every
		 * same-round FIFO successor records WRITER_COMPLETE.
		 */
		if (cluster_pcm_x_should_release_legacy_on_unlock(cluster_gcs_block_local_cache,
														  pcm_x_writer_managed)) {
			cluster_pcm_lock_unlock_content_buffer(buf, old_mode);

			/*
			 * PGRAC ownership-gen: coherent state mirror + generation bump
			 * for the cache-off X-release (X -> queried state).
			 */
			cluster_pcm_own_transition(buf, (uint8)cluster_pcm_lock_query(buf->tag), 0, 0);
		}
	}
#endif
}

void
LockBuffer(Buffer buffer, int mode)
{
	LockBufferInternal(buffer, mode, NULL);
}

/*
 * PGRAC (t/400 L3 item 3): EXCLUSIVE LockBuffer that reports a nested-guard
 * BARRIER_CLOSED refusal instead of raising a client ERROR.
 *
 * Returns true with the content lock held (every non-barrier path behaves
 * exactly like LockBuffer, including its error surface).  Returns false with
 * NOTHING held when the PCM-X queue acquire refused because this backend
 * already holds another content lock whose tag sits under a frozen revoke
 * barrier: sleeping here could deadlock across locks and the frozen barrier
 * snapshot cannot see the nested edge.  The caller — the owner of that outer
 * content lock — must release its own lock(s), resolve the map-page
 * conversion while holding no content lock, and re-enter a requalify point.
 */
bool
ClusterLockBufferExclusiveBarrierAware(Buffer buffer)
{
	bool		barrier_refused = false;

	LockBufferInternal(buffer, BUFFER_LOCK_EXCLUSIVE, &barrier_refused);
	return !barrier_refused;
}

/* VM/FSM pages read with RBM_ZERO_ON_ERROR become BM_VALID before their
 * PageIsNew initialization.  Dedicated wrappers are the provenance: a plain
 * LockBuffer(X) on the same N buffer remains queue-only. */
#ifdef USE_PGRAC_CLUSTER
static void
LockBufferForAuxiliaryPageInit(Buffer buffer, ClusterPcmDirectInitKind kind)
{
	BufferDesc *buf;

	Assert(BufferIsPinned(buffer));
	Assert(kind == CLUSTER_PCM_DIRECT_INIT_VM || kind == CLUSTER_PCM_DIRECT_INIT_FSM);
	if (BufferIsLocal(buffer))
		return;

	buf = GetBufferDescriptor(buffer - 1);
	if (cluster_pcm_is_active() && cluster_bufmgr_should_pcm_track(buf))
	{
		ClusterPcmDirectInitProof direct_init_proof;
		uint8		pcm_state;
		uint32		flags;

		/*
		 * A concurrent initializer may already have established X while the
		 * caller's unlocked PageIsNew probe was true.  The ordinary cached-X
		 * path revalidates that grant after taking the content lock.
		 */
		cluster_pcm_own_read(buf, &pcm_state, NULL, &flags);
		if (pcm_state == (uint8) PCM_STATE_X && flags == 0)
		{
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
			return;
		}

		cluster_bufmgr_pcm_arm_direct_init(buf, kind, &direct_init_proof);
		cluster_bufmgr_pcm_gate_direct_init(buf, kind, &direct_init_proof);
	}
	LWLockAcquire(BufferDescriptorGetContentLock(buf), LW_EXCLUSIVE);
}
#endif

void
LockBufferForVisibilityMapPageInit(Buffer buffer)
{
#ifdef USE_PGRAC_CLUSTER
	LockBufferForAuxiliaryPageInit(buffer, CLUSTER_PCM_DIRECT_INIT_VM);
#else
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
#endif
}

void
LockBufferForFreeSpaceMapPageInit(Buffer buffer)
{
#ifdef USE_PGRAC_CLUSTER
	LockBufferForAuxiliaryPageInit(buffer, CLUSTER_PCM_DIRECT_INIT_FSM);
#else
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
#endif
}

/*
 * Acquire the content_lock for the buffer, but only if we don't have to wait.
 *
 * This assumes the caller wants BUFFER_LOCK_EXCLUSIVE mode.
 *
 * PGRAC ownership-generation audit — this is NOT a PCM bypass entrance for the
 * ownership-generation wave: it acquires only the content lock and never
 * initiates a PCM conversion.  A shared-buffer caller may therefore succeed
 * only when the coherent ownership tuple already names an unencumbered local
 * X image.  N/S ownership, retained bytes, or a live transition returns false
 * so the caller cannot write around the queue while preserving this API's
 * don't-wait contract.
 */
bool
ConditionalLockBuffer(Buffer buffer)
{
	BufferDesc *buf;
	bool		acquired;
#ifdef USE_PGRAC_CLUSTER
	bool		blocked;
	uint32		buf_state;
#endif

	Assert(BufferIsPinned(buffer));
	if (BufferIsLocal(buffer))
		return true;			/* act as though we got it */

	buf = GetBufferDescriptor(buffer - 1);

	acquired = LWLockConditionalAcquire(BufferDescriptorGetContentLock(buf),
									  LW_EXCLUSIVE);
#ifdef USE_PGRAC_CLUSTER
	if (acquired)
	{
		/* This API bypasses LockBuffer/W1.  Recreate its reservation boundary
		 * after acquiring content authority: an already-running user wins and
		 * serializes before revoke finish; a user arriving after REVOKING or
		 * GRANT_PENDING must not modify protocol-owned bytes.
		 */
		buf_state = LockBufHdr(buf);
		blocked = !cluster_pcm_x_conditional_lock_allowed(
			cluster_pcm_is_active(), cluster_bufmgr_should_pcm_track(buf),
			cluster_bufmgr_pcm_x_retained_image_locked(buf, buf_state), buf->pcm_state,
			cluster_pcm_own_flags_get(buf->buf_id));
		UnlockBufHdr(buf, buf_state);
		if (blocked)
		{
			LWLockRelease(BufferDescriptorGetContentLock(buf));
			return false;
		}
	}
#endif
	return acquired;
}

/*
 * Verify that this backend is pinning the buffer exactly once.
 *
 * NOTE: Like in BufferIsPinned(), what we check here is that *this* backend
 * holds a pin on the buffer.  We do not care whether some other backend does.
 */
void
CheckBufferIsPinnedOnce(Buffer buffer)
{
	if (BufferIsLocal(buffer))
	{
		if (LocalRefCount[-buffer - 1] != 1)
			elog(ERROR, "incorrect local pin count: %d",
				 LocalRefCount[-buffer - 1]);
	}
	else
	{
		if (GetPrivateRefCount(buffer) != 1)
			elog(ERROR, "incorrect local pin count: %d",
				 GetPrivateRefCount(buffer));
	}
}

/*
 * LockBufferForCleanup - lock a buffer in preparation for deleting items
 *
 * Items may be deleted from a disk page only when the caller (a) holds an
 * exclusive lock on the buffer and (b) has observed that no other backend
 * holds a pin on the buffer.  If there is a pin, then the other backend
 * might have a pointer into the buffer (for example, a heapscan reference
 * to an item --- see README for more details).  It's OK if a pin is added
 * after the cleanup starts, however; the newly-arrived backend will be
 * unable to look at the page until we release the exclusive lock.
 *
 * To implement this protocol, a would-be deleter must pin the buffer and
 * then call LockBufferForCleanup().  LockBufferForCleanup() is similar to
 * LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE), except that it loops until
 * it has successfully observed pin count = 1.
 *
 * PGRAC: no additional outer PCM acquire is needed here.  The internal
 * LockBuffer(BUFFER_LOCK_EXCLUSIVE) / LockBuffer(BUFFER_LOCK_UNLOCK) loop
 * drives the spec-2.31 PCM hook per attempt, and avoids holding PCM X while
 * waiting for unrelated local pins to drain.
 */
void
LockBufferForCleanup(Buffer buffer)
{
	BufferDesc *bufHdr;
	TimestampTz waitStart = 0;
	bool		waiting = false;
	bool		logged_recovery_conflict = false;

	Assert(BufferIsPinned(buffer));
	Assert(PinCountWaitBuf == NULL);

	CheckBufferIsPinnedOnce(buffer);

	/* Nobody else to wait for */
	if (BufferIsLocal(buffer))
		return;

	bufHdr = GetBufferDescriptor(buffer - 1);

	for (;;)
	{
		uint32		buf_state;

		/* Try to acquire lock */
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		buf_state = LockBufHdr(bufHdr);

		Assert(BUF_STATE_GET_REFCOUNT(buf_state) > 0);
		if (BUF_STATE_GET_REFCOUNT(buf_state) == 1)
		{
			/* Successfully acquired exclusive lock with pincount 1 */
			UnlockBufHdr(bufHdr, buf_state);

			/*
			 * Emit the log message if recovery conflict on buffer pin was
			 * resolved but the startup process waited longer than
			 * deadlock_timeout for it.
			 */
			if (logged_recovery_conflict)
				LogRecoveryConflict(PROCSIG_RECOVERY_CONFLICT_BUFFERPIN,
									waitStart, GetCurrentTimestamp(),
									NULL, false);

			if (waiting)
			{
				/* reset ps display to remove the suffix if we added one */
				set_ps_display_remove_suffix();
				waiting = false;
			}
			return;
		}
		/* Failed, so mark myself as waiting for pincount 1 */
		if (buf_state & BM_PIN_COUNT_WAITER)
		{
			UnlockBufHdr(bufHdr, buf_state);
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			elog(ERROR, "multiple backends attempting to wait for pincount 1");
		}
		bufHdr->wait_backend_pgprocno = MyProc->pgprocno;
		PinCountWaitBuf = bufHdr;
		buf_state |= BM_PIN_COUNT_WAITER;
		UnlockBufHdr(bufHdr, buf_state);
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

		/* Wait to be signaled by UnpinBuffer() */
		if (InHotStandby)
		{
			if (!waiting)
			{
				/* adjust the process title to indicate that it's waiting */
				set_ps_display_suffix("waiting");
				waiting = true;
			}

			/*
			 * Emit the log message if the startup process is waiting longer
			 * than deadlock_timeout for recovery conflict on buffer pin.
			 *
			 * Skip this if first time through because the startup process has
			 * not started waiting yet in this case. So, the wait start
			 * timestamp is set after this logic.
			 */
			if (waitStart != 0 && !logged_recovery_conflict)
			{
				TimestampTz now = GetCurrentTimestamp();

				if (TimestampDifferenceExceeds(waitStart, now,
											   DeadlockTimeout))
				{
					LogRecoveryConflict(PROCSIG_RECOVERY_CONFLICT_BUFFERPIN,
										waitStart, now, NULL, true);
					logged_recovery_conflict = true;
				}
			}

			/*
			 * Set the wait start timestamp if logging is enabled and first
			 * time through.
			 */
			if (log_recovery_conflict_waits && waitStart == 0)
				waitStart = GetCurrentTimestamp();

			/* Publish the bufid that Startup process waits on */
			SetStartupBufferPinWaitBufId(buffer - 1);
			/* Set alarm and then wait to be signaled by UnpinBuffer() */
			ResolveRecoveryConflictWithBufferPin();
			/* Reset the published bufid */
			SetStartupBufferPinWaitBufId(-1);
		}
		else
			ProcWaitForSignal(PG_WAIT_BUFFER_PIN);

		/*
		 * Remove flag marking us as waiter. Normally this will not be set
		 * anymore, but ProcWaitForSignal() can return for other signals as
		 * well.  We take care to only reset the flag if we're the waiter, as
		 * theoretically another backend could have started waiting. That's
		 * impossible with the current usages due to table level locking, but
		 * better be safe.
		 */
		buf_state = LockBufHdr(bufHdr);
		if ((buf_state & BM_PIN_COUNT_WAITER) != 0 &&
			bufHdr->wait_backend_pgprocno == MyProc->pgprocno)
			buf_state &= ~BM_PIN_COUNT_WAITER;
		UnlockBufHdr(bufHdr, buf_state);

		PinCountWaitBuf = NULL;
		/* Loop back and try again */
	}
}

/*
 * Check called from RecoveryConflictInterrupt handler when Startup
 * process requests cancellation of all pin holders that are blocking it.
 */
bool
HoldingBufferPinThatDelaysRecovery(void)
{
	int			bufid = GetStartupBufferPinWaitBufId();

	/*
	 * If we get woken slowly then it's possible that the Startup process was
	 * already woken by other backends before we got here. Also possible that
	 * we get here by multiple interrupts or interrupts at inappropriate
	 * times, so make sure we do nothing if the bufid is not set.
	 */
	if (bufid < 0)
		return false;

	if (GetPrivateRefCount(bufid + 1) > 0)
		return true;

	return false;
}

/*
 * ConditionalLockBufferForCleanup - as above, but don't wait to get the lock
 *
 * We won't loop, but just check once to see if the pin count is OK.  If
 * not, return false with no lock held.
 */
bool
ConditionalLockBufferForCleanup(Buffer buffer)
{
	BufferDesc *bufHdr;
	uint32		buf_state,
				refcount;

	Assert(BufferIsValid(buffer));

	if (BufferIsLocal(buffer))
	{
		refcount = LocalRefCount[-buffer - 1];
		/* There should be exactly one pin */
		Assert(refcount > 0);
		if (refcount != 1)
			return false;
		/* Nobody else to wait for */
		return true;
	}

	/* There should be exactly one local pin */
	refcount = GetPrivateRefCount(buffer);
	Assert(refcount);
	if (refcount != 1)
		return false;

	/* Try to acquire lock */
	if (!ConditionalLockBuffer(buffer))
		return false;

	bufHdr = GetBufferDescriptor(buffer - 1);
	buf_state = LockBufHdr(bufHdr);
	refcount = BUF_STATE_GET_REFCOUNT(buf_state);

	Assert(refcount > 0);
	if (refcount == 1)
	{
		/* Successfully acquired exclusive lock with pincount 1 */
		UnlockBufHdr(bufHdr, buf_state);
		return true;
	}

	/* Failed, so release the lock */
	UnlockBufHdr(bufHdr, buf_state);
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	return false;
}

/*
 * IsBufferCleanupOK - as above, but we already have the lock
 *
 * Check whether it's OK to perform cleanup on a buffer we've already
 * locked.  If we observe that the pin count is 1, our exclusive lock
 * happens to be a cleanup lock, and we can proceed with anything that
 * would have been allowable had we sought a cleanup lock originally.
 */
bool
IsBufferCleanupOK(Buffer buffer)
{
	BufferDesc *bufHdr;
	uint32		buf_state;

	Assert(BufferIsValid(buffer));

	if (BufferIsLocal(buffer))
	{
		/* There should be exactly one pin */
		if (LocalRefCount[-buffer - 1] != 1)
			return false;
		/* Nobody else to wait for */
		return true;
	}

	/* There should be exactly one local pin */
	if (GetPrivateRefCount(buffer) != 1)
		return false;

	bufHdr = GetBufferDescriptor(buffer - 1);

	/* caller must hold exclusive lock on buffer */
	Assert(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(bufHdr),
								LW_EXCLUSIVE));

	buf_state = LockBufHdr(bufHdr);

	Assert(BUF_STATE_GET_REFCOUNT(buf_state) > 0);
	if (BUF_STATE_GET_REFCOUNT(buf_state) == 1)
	{
		/* pincount is OK. */
		UnlockBufHdr(bufHdr, buf_state);
		return true;
	}

	UnlockBufHdr(bufHdr, buf_state);
	return false;
}


/*
 *	Functions for buffer I/O handling
 *
 *	Note: We assume that nested buffer I/O never occurs.
 *	i.e at most one BM_IO_IN_PROGRESS bit is set per proc.
 *
 *	Also note that these are used only for shared buffers, not local ones.
 */

/*
 * WaitIO -- Block until the IO_IN_PROGRESS flag on 'buf' is cleared.
 */
static void
WaitIO(BufferDesc *buf)
{
	ConditionVariable *cv = BufferDescriptorGetIOCV(buf);

	ConditionVariablePrepareToSleep(cv);
	for (;;)
	{
		uint32		buf_state;

		/*
		 * It may not be necessary to acquire the spinlock to check the flag
		 * here, but since this test is essential for correctness, we'd better
		 * play it safe.
		 */
		buf_state = LockBufHdr(buf);
		UnlockBufHdr(buf, buf_state);

		if (!(buf_state & BM_IO_IN_PROGRESS))
			break;
		ConditionVariableSleep(cv, WAIT_EVENT_BUFFER_IO);
	}
	ConditionVariableCancelSleep();
}

/*
 * StartBufferIO: begin I/O on this buffer
 *	(Assumptions)
 *	My process is executing no IO
 *	The buffer is Pinned
 *
 * In some scenarios there are race conditions in which multiple backends
 * could attempt the same I/O operation concurrently.  If someone else
 * has already started I/O on this buffer then we will block on the
 * I/O condition variable until he's done.
 *
 * Input operations are only attempted on buffers that are not BM_VALID,
 * and output operations only on buffers that are BM_VALID and BM_DIRTY,
 * so we can always tell if the work is already done.
 *
 * Returns true if we successfully marked the buffer as I/O busy,
 * false if someone else already did the work.
 *
 * PGRAC modifications by SqlRush <sqlrush@gmail.com>:
 * What changed: spec-6.12h D-h3a — a read IO about to start over a
 *	Past Image buffer (BUF_TYPE_PI, D-h1) resets buffer_type back to
 *	BUF_TYPE_CURRENT under the same header-lock hold.
 * Why: the bytes of a !BM_VALID shared buffer are only ever written
 *	inside the StartBufferIO..TerminateBufferIO window, so this single
 *	seam (with the retag resets in InvalidateBuffer /
 *	InvalidateVictimBuffer) makes the PI shape (BUF_TYPE_PI &&
 *	BM_TAG_VALID && !BM_VALID) reachable ONLY through
 *	cluster_bufmgr_convert_to_pi_locked with bytes frozen since its
 *	D-h3 shadow stamp.  It is the D-h1 "re-read = implicit discard"
 *	made explicit BEFORE any byte changes: a failed or aborted read
 *	(!BM_VALID + BM_IO_ERROR) can then never masquerade as a PI whose
 *	bytes drifted after the stamp (a wrong recovery base, Rule 8.A).
 */
static bool
StartBufferIO(BufferDesc *buf, bool forInput)
{
	uint32		buf_state;
#ifdef USE_PGRAC_CLUSTER
	bool		pi_implicit_discard = false;
#endif

	ResourceOwnerEnlargeBufferIOs(CurrentResourceOwner);

	for (;;)
	{
		buf_state = LockBufHdr(buf);

		if (!(buf_state & BM_IO_IN_PROGRESS))
			break;
		UnlockBufHdr(buf, buf_state);
		WaitIO(buf);
	}

	/* Once we get here, there is definitely no I/O active on this buffer */

	if (forInput ? (buf_state & BM_VALID) : !(buf_state & BM_DIRTY))
	{
		/* someone else already did the I/O */
		UnlockBufHdr(buf, buf_state);
		return false;
	}

#ifdef USE_PGRAC_CLUSTER
	/* PGRAC: spec-6.12h D-h3a — see the function header note. */
	if (forInput && buf->buffer_type == (uint8) BUF_TYPE_PI)
	{
		buf->buffer_type = (uint8) BUF_TYPE_CURRENT;
		pi_implicit_discard = true;
	}
#endif

	buf_state |= BM_IO_IN_PROGRESS;
	UnlockBufHdr(buf, buf_state);

#ifdef USE_PGRAC_CLUSTER
	if (pi_implicit_discard)
		cluster_lever_h_note_pi_implicit_discard();
#endif

	ResourceOwnerRememberBufferIO(CurrentResourceOwner,
								  BufferDescriptorGetBuffer(buf));

	return true;
}

/*
 * TerminateBufferIO: release a buffer we were doing I/O on
 *	(Assumptions)
 *	My process is executing IO for the buffer
 *	BM_IO_IN_PROGRESS bit is set for the buffer
 *	The buffer is Pinned
 *
 * If clear_dirty is true and BM_JUST_DIRTIED is not set, we clear the
 * buffer's BM_DIRTY flag.  This is appropriate when terminating a
 * successful write.  The check on BM_JUST_DIRTIED is necessary to avoid
 * marking the buffer clean if it was re-dirtied while we were writing.
 *
 * set_flag_bits gets ORed into the buffer's flags.  It must include
 * BM_IO_ERROR in a failure case.  For successful completion it could
 * be 0, or BM_VALID if we just finished reading in the page.
 */
static void
TerminateBufferIO(BufferDesc *buf, bool clear_dirty, uint32 set_flag_bits)
{
	uint32		buf_state;

	buf_state = LockBufHdr(buf);

	Assert(buf_state & BM_IO_IN_PROGRESS);

	buf_state &= ~(BM_IO_IN_PROGRESS | BM_IO_ERROR);
	if (clear_dirty && !(buf_state & BM_JUST_DIRTIED))
		buf_state &= ~(BM_DIRTY | BM_CHECKPOINT_NEEDED);

	buf_state |= set_flag_bits;
	UnlockBufHdr(buf, buf_state);

	ResourceOwnerForgetBufferIO(CurrentResourceOwner,
								BufferDescriptorGetBuffer(buf));

	ConditionVariableBroadcast(BufferDescriptorGetIOCV(buf));
}

/*
 * AbortBufferIO: Clean up active buffer I/O after an error.
 *
 *	All LWLocks we might have held have been released,
 *	but we haven't yet released buffer pins, so the buffer is still pinned.
 *
 *	If I/O was in progress, we always set BM_IO_ERROR, even though it's
 *	possible the error condition wasn't related to the I/O.
 */
void
AbortBufferIO(Buffer buffer)
{
	BufferDesc *buf_hdr = GetBufferDescriptor(buffer - 1);
	uint32		buf_state;

	buf_state = LockBufHdr(buf_hdr);
	Assert(buf_state & (BM_IO_IN_PROGRESS | BM_TAG_VALID));

	if (!(buf_state & BM_VALID))
	{
		Assert(!(buf_state & BM_DIRTY));
		UnlockBufHdr(buf_hdr, buf_state);
	}
	else
	{
		Assert(buf_state & BM_DIRTY);
		UnlockBufHdr(buf_hdr, buf_state);

		/* Issue notice if this is not the first failure... */
		if (buf_state & BM_IO_ERROR)
		{
			/* Buffer is pinned, so we can read tag without spinlock */
			char	   *path;

			path = relpathperm(BufTagGetRelFileLocator(&buf_hdr->tag),
							   BufTagGetForkNum(&buf_hdr->tag));
			ereport(WARNING,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg("could not write block %u of %s",
							buf_hdr->tag.blockNum, path),
					 errdetail("Multiple failures --- write error might be permanent.")));
			pfree(path);
		}
	}

	TerminateBufferIO(buf_hdr, false, BM_IO_ERROR);
}

/*
 * Error context callback for errors occurring during shared buffer writes.
 */
static void
shared_buffer_write_error_callback(void *arg)
{
	BufferDesc *bufHdr = (BufferDesc *) arg;

	/* Buffer is pinned, so we can read the tag without locking the spinlock */
	if (bufHdr != NULL)
	{
		char	   *path = relpathperm(BufTagGetRelFileLocator(&bufHdr->tag),
									   BufTagGetForkNum(&bufHdr->tag));

		errcontext("writing block %u of relation %s",
				   bufHdr->tag.blockNum, path);
		pfree(path);
	}
}

/*
 * Error context callback for errors occurring during local buffer writes.
 */
static void
local_buffer_write_error_callback(void *arg)
{
	BufferDesc *bufHdr = (BufferDesc *) arg;

	if (bufHdr != NULL)
	{
		char	   *path = relpathbackend(BufTagGetRelFileLocator(&bufHdr->tag),
										  MyBackendId,
										  BufTagGetForkNum(&bufHdr->tag));

		errcontext("writing block %u of relation %s",
				   bufHdr->tag.blockNum, path);
		pfree(path);
	}
}

/*
 * RelFileLocator qsort/bsearch comparator; see RelFileLocatorEquals.
 */
static int
rlocator_comparator(const void *p1, const void *p2)
{
	RelFileLocator n1 = *(const RelFileLocator *) p1;
	RelFileLocator n2 = *(const RelFileLocator *) p2;

	if (n1.relNumber < n2.relNumber)
		return -1;
	else if (n1.relNumber > n2.relNumber)
		return 1;

	if (n1.dbOid < n2.dbOid)
		return -1;
	else if (n1.dbOid > n2.dbOid)
		return 1;

	if (n1.spcOid < n2.spcOid)
		return -1;
	else if (n1.spcOid > n2.spcOid)
		return 1;
	else
		return 0;
}

/*
 * Lock buffer header - set BM_LOCKED in buffer state.
 */
uint32
LockBufHdr(BufferDesc *desc)
{
	SpinDelayStatus delayStatus;
	uint32		old_buf_state;

	Assert(!BufferIsLocal(BufferDescriptorGetBuffer(desc)));

	init_local_spin_delay(&delayStatus);

	while (true)
	{
		/* set BM_LOCKED flag */
		old_buf_state = pg_atomic_fetch_or_u32(&desc->state, BM_LOCKED);
		/* if it wasn't set before we're OK */
		if (!(old_buf_state & BM_LOCKED))
			break;
		perform_spin_delay(&delayStatus);
	}
	finish_spin_delay(&delayStatus);
	return old_buf_state | BM_LOCKED;
}

/*
 * Wait until the BM_LOCKED flag isn't set anymore and return the buffer's
 * state at that point.
 *
 * Obviously the buffer could be locked by the time the value is returned, so
 * this is primarily useful in CAS style loops.
 */
static uint32
WaitBufHdrUnlocked(BufferDesc *buf)
{
	SpinDelayStatus delayStatus;
	uint32		buf_state;

	init_local_spin_delay(&delayStatus);

	buf_state = pg_atomic_read_u32(&buf->state);

	while (buf_state & BM_LOCKED)
	{
		perform_spin_delay(&delayStatus);
		buf_state = pg_atomic_read_u32(&buf->state);
	}

	finish_spin_delay(&delayStatus);

	return buf_state;
}

/*
 * BufferTag comparator.
 */
static inline int
buffertag_comparator(const BufferTag *ba, const BufferTag *bb)
{
	int			ret;
	RelFileLocator rlocatora;
	RelFileLocator rlocatorb;

	rlocatora = BufTagGetRelFileLocator(ba);
	rlocatorb = BufTagGetRelFileLocator(bb);

	ret = rlocator_comparator(&rlocatora, &rlocatorb);

	if (ret != 0)
		return ret;

	if (BufTagGetForkNum(ba) < BufTagGetForkNum(bb))
		return -1;
	if (BufTagGetForkNum(ba) > BufTagGetForkNum(bb))
		return 1;

	if (ba->blockNum < bb->blockNum)
		return -1;
	if (ba->blockNum > bb->blockNum)
		return 1;

	return 0;
}

/*
 * Comparator determining the writeout order in a checkpoint.
 *
 * It is important that tablespaces are compared first, the logic balancing
 * writes between tablespaces relies on it.
 */
static inline int
ckpt_buforder_comparator(const CkptSortItem *a, const CkptSortItem *b)
{
	/* compare tablespace */
	if (a->tsId < b->tsId)
		return -1;
	else if (a->tsId > b->tsId)
		return 1;
	/* compare relation */
	if (a->relNumber < b->relNumber)
		return -1;
	else if (a->relNumber > b->relNumber)
		return 1;
	/* compare fork */
	else if (a->forkNum < b->forkNum)
		return -1;
	else if (a->forkNum > b->forkNum)
		return 1;
	/* compare block number */
	else if (a->blockNum < b->blockNum)
		return -1;
	else if (a->blockNum > b->blockNum)
		return 1;
	/* equal page IDs are unlikely, but not impossible */
	return 0;
}

/*
 * Comparator for a Min-Heap over the per-tablespace checkpoint completion
 * progress.
 */
static int
ts_ckpt_progress_comparator(Datum a, Datum b, void *arg)
{
	CkptTsStatus *sa = (CkptTsStatus *) a;
	CkptTsStatus *sb = (CkptTsStatus *) b;

	/* we want a min-heap, so return 1 for the a < b */
	if (sa->progress < sb->progress)
		return 1;
	else if (sa->progress == sb->progress)
		return 0;
	else
		return -1;
}

/*
 * Initialize a writeback context, discarding potential previous state.
 *
 * *max_pending is a pointer instead of an immediate value, so the coalesce
 * limits can easily changed by the GUC mechanism, and so calling code does
 * not have to check the current configuration. A value of 0 means that no
 * writeback control will be performed.
 */
void
WritebackContextInit(WritebackContext *context, int *max_pending)
{
	Assert(*max_pending <= WRITEBACK_MAX_PENDING_FLUSHES);

	context->max_pending = max_pending;
	context->nr_pending = 0;
}

/*
 * Add buffer to list of pending writeback requests.
 */
void
ScheduleBufferTagForWriteback(WritebackContext *wb_context, IOContext io_context,
							  BufferTag *tag)
{
	PendingWriteback *pending;

	if (io_direct_flags & IO_DIRECT_DATA)
		return;

	/*
	 * Add buffer to the pending writeback array, unless writeback control is
	 * disabled.
	 */
	if (*wb_context->max_pending > 0)
	{
		Assert(*wb_context->max_pending <= WRITEBACK_MAX_PENDING_FLUSHES);

		pending = &wb_context->pending_writebacks[wb_context->nr_pending++];

		pending->tag = *tag;
	}

	/*
	 * Perform pending flushes if the writeback limit is exceeded. This
	 * includes the case where previously an item has been added, but control
	 * is now disabled.
	 */
	if (wb_context->nr_pending >= *wb_context->max_pending)
		IssuePendingWritebacks(wb_context, io_context);
}

#define ST_SORT sort_pending_writebacks
#define ST_ELEMENT_TYPE PendingWriteback
#define ST_COMPARE(a, b) buffertag_comparator(&a->tag, &b->tag)
#define ST_SCOPE static
#define ST_DEFINE
#include <lib/sort_template.h>

/*
 * Issue all pending writeback requests, previously scheduled with
 * ScheduleBufferTagForWriteback, to the OS.
 *
 * Because this is only used to improve the OSs IO scheduling we try to never
 * error out - it's just a hint.
 */
void
IssuePendingWritebacks(WritebackContext *wb_context, IOContext io_context)
{
	instr_time	io_start;
	int			i;

	if (wb_context->nr_pending == 0)
		return;

	/*
	 * Executing the writes in-order can make them a lot faster, and allows to
	 * merge writeback requests to consecutive blocks into larger writebacks.
	 */
	sort_pending_writebacks(wb_context->pending_writebacks,
							wb_context->nr_pending);

	io_start = pgstat_prepare_io_time();

	/*
	 * Coalesce neighbouring writes, but nothing else. For that we iterate
	 * through the, now sorted, array of pending flushes, and look forward to
	 * find all neighbouring (or identical) writes.
	 */
	for (i = 0; i < wb_context->nr_pending; i++)
	{
		PendingWriteback *cur;
		PendingWriteback *next;
		SMgrRelation reln;
		int			ahead;
		BufferTag	tag;
		RelFileLocator currlocator;
		Size		nblocks = 1;

		cur = &wb_context->pending_writebacks[i];
		tag = cur->tag;
		currlocator = BufTagGetRelFileLocator(&tag);

		/*
		 * Peek ahead, into following writeback requests, to see if they can
		 * be combined with the current one.
		 */
		for (ahead = 0; i + ahead + 1 < wb_context->nr_pending; ahead++)
		{

			next = &wb_context->pending_writebacks[i + ahead + 1];

			/* different file, stop */
			if (!RelFileLocatorEquals(currlocator,
									  BufTagGetRelFileLocator(&next->tag)) ||
				BufTagGetForkNum(&cur->tag) != BufTagGetForkNum(&next->tag))
				break;

			/* ok, block queued twice, skip */
			if (cur->tag.blockNum == next->tag.blockNum)
				continue;

			/* only merge consecutive writes */
			if (cur->tag.blockNum + 1 != next->tag.blockNum)
				break;

			nblocks++;
			cur = next;
		}

		i += ahead;

		/* and finally tell the kernel to write the data to storage */
		reln = smgropen(currlocator, InvalidBackendId);
		smgrwriteback(reln, BufTagGetForkNum(&tag), tag.blockNum, nblocks);
	}

	/*
	 * Assume that writeback requests are only issued for buffers containing
	 * blocks of permanent relations.
	 */
	pgstat_count_io_op_time(IOOBJECT_RELATION, io_context,
							IOOP_WRITEBACK, io_start, wb_context->nr_pending);

	wb_context->nr_pending = 0;
}


/*
 * Implement slower/larger portions of TestForOldSnapshot
 *
 * Smaller/faster portions are put inline, but the entire set of logic is too
 * big for that.
 */
void
TestForOldSnapshot_impl(Snapshot snapshot, Relation relation)
{
	if (RelationAllowsEarlyPruning(relation)
		&& (snapshot)->whenTaken < GetOldSnapshotThresholdTimestamp())
		ereport(ERROR,
				(errcode(ERRCODE_SNAPSHOT_TOO_OLD),
				 errmsg("snapshot too old")));
}


#ifdef USE_PGRAC_CLUSTER
/*
 * ============================================================================
 * PGRAC MODIFICATIONS — spec-2.33 D4 (GCS block-shipping bufmgr helpers).
 *
 * Modified by: SqlRush <sqlrush@gmail.com>
 *
 * What changed:
 *   Added two helper functions exposed to cluster_gcs_block.c so that the
 *   master-side GCS_BLOCK_REQUEST handler can copy a buffer's 8KB page bytes
 *   off the local buffer pool under the HC82 I-WAL-before-ship invariant.
 *
 *   - cluster_bufmgr_probe_block_for_gcs(tag): returns true iff a shared
 *     buffer currently holds the tag.  Used by the master-side handler to
 *     decide GRANTED_STORAGE_FALLBACK vs the full ship path (HC88).
 *
 *   - cluster_bufmgr_copy_block_for_gcs(tag, *out_page_lsn, *dst):  pins the
 *     buffer with a raw shared refcount increment (no backend-local
 *     ResourceOwner / private-refcount state), takes content_lock shared,
 *     reads page_lsn, releases the content_lock, XLogFlush(page_lsn)
 *     (HC82 — I-WAL-before-ship anchor), reacquires the content_lock shared,
 *     revalidates (tag still matches + PageGetLSN(page) stable), memcpy BLCKSZ
 *     bytes into *dst, and drops the raw pin.  HC89: on revalidation mismatch
 *     the helper retries ONCE (release content_lock, re-XLogFlush the new
 *     page_lsn, reacquire, revalidate again);  if the second revalidation also
 *     fails the function returns false and the caller replies
 *     DENIED_MASTER_NOT_HOLDER.  Unbounded loop forbidden (hot-page starvation
 *     defense);  0-retry fail-closed forbidden (normal WAL drift false
 *     positive defense).
 *
 * Why:
 *   The block-shipping data plane (cluster_gcs_block.c) needs to read 8KB
 *   page bytes safely from another module without leaking BufferDesc /
 *   partition-lock internals.  Putting these helpers here lets bufmgr keep
 *   ownership of pin / content_lock / buf table lookup semantics.
 *
 * Spec: spec-2.33-gcs-block-shipping-substrate.md §1.2 D4 + §3.2 + HC82 +
 *       HC88 + HC89.
 * ============================================================================
 */

#include "access/xlog.h"		 /* XLogFlush */
#include "cluster/cluster_gcs.h" /* spec-6.12a ㉕ — downgrade notify
									 * (nowait) */

/*
 * cluster_gcs_clamp_ship_flush_lsn -- spec-6.14 D11, the ship-path sibling of
 * the FlushBuffer D8 foreign-LSN guard above.
 *
 *	A page LSN beyond the LOCAL insert position can only be a FOREIGN
 *	thread's LSN (per-thread WAL: cross-thread LSNs are not comparable;
 *	local stamps never exceed the local insert point).  This node cannot
 *	flush another node's WAL -- XLogFlush(foreign_lsn) compares against the
 *	local horizon and ERRORs ("flush request is not satisfied"), killing
 *	the serve (seen: LMON's gcs_block_forward handler dropping the frame,
 *	the requester PANICking on transfer timeout, when a page modified by
 *	THIS node after an X-transfer keeps the origin's higher LSN under the
 *	monotonic pd_lsn discipline).
 *
 *	The foreign origin flushed its own WAL through that LSN before the
 *	image was ever shipped (HC82 holds on every ship path), so the only
 *	WAL still owed locally is this node's OWN records for modifications
 *	made after adoption -- all at or below the local insert position.
 *	Clamping the flush target to the local insert position keeps
 *	WAL-before-data cluster-wide (foreign half durable at the origin,
 *	local half durable here) at the cost of over-flushing local WAL.
 */
static XLogRecPtr
cluster_gcs_clamp_ship_flush_lsn(XLogRecPtr page_lsn)
{
	XLogRecPtr	local_insert;

	if (XLogRecPtrIsInvalid(page_lsn) || RecoveryInProgress())
		return page_lsn;

	local_insert = GetXLogInsertRecPtr();
	return (page_lsn > local_insert) ? local_insert : page_lsn;
}

/*
 * LMON handles remote GCS_BLOCK_REQUEST messages, so the block-copy path
 * cannot use PinBuffer_Locked()/UnpinBuffer(): those routines maintain
 * backend-local private refcounts and CurrentResourceOwner state.  The raw pin
 * below only protects the shared buffer from replacement while LMON copies
 * bytes under content_lock.
 */
static void
cluster_bufmgr_pin_for_gcs_locked(BufferDesc *buf, uint32 buf_state)
{
	Assert(buf_state & BM_LOCKED);

	VALGRIND_MAKE_MEM_DEFINED(BufHdrGetBlock(buf), BLCKSZ);
	buf_state += BUF_REFCOUNT_ONE;
	UnlockBufHdr(buf, buf_state);
}

static void
cluster_bufmgr_unpin_for_gcs(BufferDesc *buf)
{
	uint32		buf_state;
	uint32		old_buf_state;

	Assert(!LWLockHeldByMe(BufferDescriptorGetContentLock(buf)));

	VALGRIND_MAKE_MEM_NOACCESS(BufHdrGetBlock(buf), BLCKSZ);

	old_buf_state = pg_atomic_read_u32(&buf->state);
	for (;;)
	{
		if (old_buf_state & BM_LOCKED)
			old_buf_state = WaitBufHdrUnlocked(buf);

		buf_state = old_buf_state;
		buf_state -= BUF_REFCOUNT_ONE;

		if (pg_atomic_compare_exchange_u32(&buf->state, &old_buf_state,
										   buf_state))
			break;
	}

	if (buf_state & BM_PIN_COUNT_WAITER)
	{
		buf_state = LockBufHdr(buf);

		if ((buf_state & BM_PIN_COUNT_WAITER) &&
			BUF_STATE_GET_REFCOUNT(buf_state) == 1)
		{
			int			wait_backend_pgprocno = buf->wait_backend_pgprocno;

			buf_state &= ~BM_PIN_COUNT_WAITER;
			UnlockBufHdr(buf, buf_state);
			ProcSendSignal(wait_backend_pgprocno);
		}
		else
			UnlockBufHdr(buf, buf_state);
	}
}

/*
 * Look up `tag` in the shared buffer table without pinning.  Returns true
 * iff a valid buffer is present.  The partition lock is held only across
 * the lookup itself.
 *
 * HC88: master-side handler uses this to distinguish "no holder" (no buffer
 * present) from "holder may need block ship".  No content_lock acquired;
 * pin not taken.  Race with eviction is acceptable — false negatives lead
 * to DENIED_MASTER_NOT_HOLDER which is recoverable at the requester via
 * spec-2.34 retransmit.
 */
bool
cluster_bufmgr_probe_block_for_gcs(BufferTag tag)
{
	uint32		hashcode = BufTableHashCode(&tag);
	LWLock	   *partition_lock = BufMappingPartitionLock(hashcode);
	int			buf_id;
	bool		valid = false;

	LWLockAcquire(partition_lock, LW_SHARED);
	buf_id = BufTableLookup(&tag, hashcode);
	if (buf_id >= 0)
	{
		BufferDesc *buf = GetBufferDescriptor(buf_id);
		uint32		buf_state;

		buf_state = LockBufHdr(buf);
		valid = BufferTagsEqual(&buf->tag, &tag)
			&& cluster_bufmgr_pcm_current_image_locked(buf, buf_state);
		UnlockBufHdr(buf, buf_state);
	}
	LWLockRelease(partition_lock);

	return valid;
}

/*
 * Read the shared-storage version for a GCS lost-write rescue probe.  This is
 * deliberately independent of the local buffer table: a BM_VALID mirror is
 * residency only and must not be consulted as the storage-version carrier (the
 * resident copy is exactly what the caller has just proven stale).  The
 * physical read happens without any PCM/GRD lock held by the caller.
 *
 * Returns false (with *out_page_scn = InvalidScn) when the page fails
 * verification; the caller must then keep its fail-closed verdict.
 */
bool
cluster_bufmgr_read_storage_scn_for_gcs(BufferTag tag, SCN *out_page_scn)
{
	PGAlignedBlock scratch;
	SMgrRelation reln;

	if (out_page_scn != NULL)
		*out_page_scn = InvalidScn;

	reln = smgropen(BufTagGetRelFileLocator(&tag), InvalidBackendId);
	smgrread(reln, BufTagGetForkNum(&tag), tag.blockNum, scratch.data);
	if (!PageIsVerifiedExtended((Page) scratch.data, tag.blockNum,
								PIV_LOG_WARNING | PIV_REPORT_STAT))
		return false;

	if (out_page_scn != NULL)
		*out_page_scn = ((PageHeader) scratch.data)->pd_block_scn;
	return true;
}

/*
 * Copy the 8KB block bytes for `tag` into *dst, flushing WAL up to the
 * page's LSN before reading the bytes (HC82 I-WAL-before-ship).  Sets
 * *out_page_lsn to the page LSN observed at the second-stable revalidation.
 *
 * Returns false on:
 *   - Buffer no longer in pool (evicted between probe and pin)
 *   - HC89 revalidation single-retry exhausted
 *
 * Returns true with *dst populated and *out_page_lsn set otherwise.
 *
 * Concurrency: caller does NOT hold any buffer/partition locks.  We take a
 * SHARED partition lock to look up, raw-pin the shared buffer refcount, then
 * drop the partition lock and operate on the buffer's content_lock.  The raw
 * pin is always released before return.
 */
bool
cluster_bufmgr_copy_block_for_gcs(BufferTag tag, XLogRecPtr *out_page_lsn, char *dst)
{
	uint32		hashcode;
	LWLock	   *partition_lock;
	int			buf_id;
	BufferDesc *buf;
	LWLock	   *content_lock;
	XLogRecPtr	first_lsn;
	XLogRecPtr	second_lsn;
	int			retries;
	bool		stable;
	Page		page;

	Assert(dst != NULL);
	Assert(out_page_lsn != NULL);

	hashcode = BufTableHashCode(&tag);
	partition_lock = BufMappingPartitionLock(hashcode);

	LWLockAcquire(partition_lock, LW_SHARED);
	buf_id = BufTableLookup(&tag, hashcode);
	if (buf_id < 0)
	{
		LWLockRelease(partition_lock);
		return false;
	}
	buf = GetBufferDescriptor(buf_id);

	/* Partition lock keeps the buffer from being recycled before we raw-pin. */
	{
		uint32		buf_state;

		buf_state = LockBufHdr(buf);
		/* Re-verify tag under header lock to defend against tag-rewrite
		 * races between the partition-lock-protected lookup and the pin. */
		if (!BufferTagsEqual(&buf->tag, &tag))
		{
			UnlockBufHdr(buf, buf_state);
			LWLockRelease(partition_lock);
			return false;
		}
		if (!cluster_bufmgr_pcm_current_image_locked(buf, buf_state))
		{
			UnlockBufHdr(buf, buf_state);
			LWLockRelease(partition_lock);
			return false;
		}
		cluster_bufmgr_pin_for_gcs_locked(buf, buf_state);
	}
	LWLockRelease(partition_lock);

	content_lock = BufferDescriptorGetContentLock(buf);
	page = (Page) BufHdrGetBlock(buf);

	/*
	 * HC89: revalidation loop with single retry budget. retries = 0 — first
	 * attempt retries = 1 — single retry permitted retries = 2 — give up,
	 * fail-closed
	 */
	stable = false;
	for (retries = 0; retries < 2; retries++)
	{
		/* Read page_lsn under content_lock SHARED. */
		LWLockAcquire(content_lock, LW_SHARED);
		first_lsn = PageGetLSN(page);
		LWLockRelease(content_lock);

		/*
		 * HC82: flush WAL up to the page LSN before shipping the bytes. Use
		 * XLogFlush(page_lsn) specifically — NOT XLogFlush(insert pointer)
		 * which would be correct but over-flushes and doesn't express the
		 * "flush this page before ship" safety contract.
		 */
#ifdef USE_CLUSTER_UNIT
		if (cluster_gcs_block_test_xlog_flush_hook != NULL)
			cluster_gcs_block_test_xlog_flush_hook((uint64) first_lsn);
#endif
		if (!XLogRecPtrIsInvalid(first_lsn))
			XLogFlush(cluster_gcs_clamp_ship_flush_lsn(first_lsn));

		/*
		 * Reacquire content_lock SHARED and revalidate that the page LSN has
		 * not advanced past first_lsn AND the buffer tag still matches.
		 * Either signals concurrent mutation that would break HC82's "ship
		 * the bytes that I just flushed WAL for" contract.
		 */
		LWLockAcquire(content_lock, LW_SHARED);
		{
			uint32 buf_state = LockBufHdr(buf);
			bool current = BufferTagsEqual(&buf->tag, &tag)
				&& cluster_bufmgr_pcm_current_image_locked(buf, buf_state);

			UnlockBufHdr(buf, buf_state);
			if (!current)
			{
				LWLockRelease(content_lock);
				break;
			}
		}
		second_lsn = PageGetLSN(page);

#ifdef USE_CLUSTER_UNIT

		/*
		 * Spec L25/L26 hook:  if the test injection returns N > 0 we mimic N
		 * consecutive LSN drift events.  retries 0 + drift available means
		 * second_lsn != first_lsn synthetically.
		 */
		if (cluster_gcs_block_test_lsn_drift_hook != NULL)
		{
			int			drift_remaining = cluster_gcs_block_test_lsn_drift_hook();

			if (drift_remaining > retries)
				second_lsn = first_lsn + 1;	/* synthetic mismatch */
		}
#endif

		if (BufferTagsEqual(&buf->tag, &tag) && first_lsn == second_lsn)
		{
			memcpy(dst, page, BLCKSZ);
			*out_page_lsn = second_lsn;
			LWLockRelease(content_lock);
			stable = true;
			break;
		}
		LWLockRelease(content_lock);
		/* fall through: retry once if budget remains */
	}

	cluster_bufmgr_unpin_for_gcs(buf);
	return stable;
}

/*
 * Borrow a live shared_buffers page for a registered RDMA SGE.
 *
 * This mirrors cluster_bufmgr_copy_block_for_gcs() through the HC82 WAL flush
 * and HC89 single revalidation, but returns a raw-pinned page pointer instead
 * of copying bytes.  content_lock is deliberately not held across return:
 * callers must keep the existing end-to-end CRC fail-closed behavior because
 * hint-bit or page-content drift after the revalidation can still make the
 * shipped bytes differ from the pre-post checksum.  The raw pin only prevents
 * buffer reuse while the asynchronous SEND is in flight.
 */
bool
cluster_bufmgr_borrow_block_for_gcs_live_sge(BufferTag tag, XLogRecPtr *out_page_lsn,
											 void **out_page_addr, BufferDesc **out_buf)
{
	uint32		hashcode;
	LWLock	   *partition_lock;
	int			buf_id;
	BufferDesc *buf;
	LWLock	   *content_lock;
	XLogRecPtr	first_lsn;
	XLogRecPtr	second_lsn;
	int			retries;
	Page		page;

	Assert(out_page_lsn != NULL);
	Assert(out_page_addr != NULL);
	Assert(out_buf != NULL);

	*out_page_lsn = InvalidXLogRecPtr;
	*out_page_addr = NULL;
	*out_buf = NULL;

	hashcode = BufTableHashCode(&tag);
	partition_lock = BufMappingPartitionLock(hashcode);

	LWLockAcquire(partition_lock, LW_SHARED);
	buf_id = BufTableLookup(&tag, hashcode);
	if (buf_id < 0)
	{
		LWLockRelease(partition_lock);
		return false;
	}
	buf = GetBufferDescriptor(buf_id);

	{
		uint32		buf_state;

		buf_state = LockBufHdr(buf);
		if (!BufferTagsEqual(&buf->tag, &tag)
			|| !cluster_bufmgr_pcm_current_image_locked(buf, buf_state))
		{
			UnlockBufHdr(buf, buf_state);
			LWLockRelease(partition_lock);
			return false;
		}
		cluster_bufmgr_pin_for_gcs_locked(buf, buf_state);
	}
	LWLockRelease(partition_lock);

	content_lock = BufferDescriptorGetContentLock(buf);
	page = (Page) BufHdrGetBlock(buf);

	for (retries = 0; retries < 2; retries++)
	{
		LWLockAcquire(content_lock, LW_SHARED);
		first_lsn = PageGetLSN(page);
		LWLockRelease(content_lock);

#ifdef USE_CLUSTER_UNIT
		if (cluster_gcs_block_test_xlog_flush_hook != NULL)
			cluster_gcs_block_test_xlog_flush_hook((uint64) first_lsn);
#endif
		if (!XLogRecPtrIsInvalid(first_lsn))
			XLogFlush(cluster_gcs_clamp_ship_flush_lsn(first_lsn));

		LWLockAcquire(content_lock, LW_SHARED);
		{
			uint32 buf_state = LockBufHdr(buf);
			bool current = BufferTagsEqual(&buf->tag, &tag)
				&& cluster_bufmgr_pcm_current_image_locked(buf, buf_state);

			UnlockBufHdr(buf, buf_state);
			if (!current)
			{
				LWLockRelease(content_lock);
				break;
			}
		}
		second_lsn = PageGetLSN(page);

#ifdef USE_CLUSTER_UNIT
		if (cluster_gcs_block_test_lsn_drift_hook != NULL)
		{
			int			drift_remaining = cluster_gcs_block_test_lsn_drift_hook();

			if (drift_remaining > retries)
				second_lsn = first_lsn + 1;
		}
#endif

		if (BufferTagsEqual(&buf->tag, &tag) && first_lsn == second_lsn)
		{
			*out_page_lsn = second_lsn;
			*out_page_addr = page;
			*out_buf = buf;
			LWLockRelease(content_lock);
			return true;
		}
		LWLockRelease(content_lock);
	}

	cluster_bufmgr_unpin_for_gcs(buf);
	return false;
}

void
cluster_bufmgr_release_block_for_gcs_live_sge(BufferDesc *buf)
{
	if (buf != NULL)
		cluster_bufmgr_unpin_for_gcs(buf);
}

/*
 * Prepare a caller-pinned shared buffer as a direct-land target for a GCS block
 * reply.  The target must already be tagged for the requested block, but it
 * must not be visible as valid data.  We mark BM_IO_IN_PROGRESS so ordinary
 * readers wait instead of observing RDMA-written bytes before the verifier has
 * accepted the sidecar/header/checksum chain.
 */
bool
cluster_bufmgr_prepare_direct_land_target_for_gcs(BufferDesc *buf, BufferTag tag,
												  void **out_page_addr)
{
	uint32		buf_state;

	Assert(out_page_addr != NULL);

	*out_page_addr = NULL;
	if (buf == NULL)
		return false;

	buf_state = LockBufHdr(buf);
	if (!BufferTagsEqual(&buf->tag, &tag)
		|| (buf_state & BM_TAG_VALID) == 0
		|| (buf_state & BM_VALID) != 0
		|| (buf_state & BM_IO_IN_PROGRESS) != 0)
	{
		UnlockBufHdr(buf, buf_state);
		return false;
	}
	UnlockBufHdr(buf, buf_state);

	if (!StartBufferIO(buf, true))
		return false;

	*out_page_addr = BufHdrGetBlock(buf);
	return true;
}

/*
 * Finish a direct-land target prepared by
 * cluster_bufmgr_prepare_direct_land_target_for_gcs().  The caller guarantees
 * any posted receive has completed or the block-reply lane was flushed/reset.
 * On success, set the page LSN while holding content_lock and then publish
 * BM_VALID through TerminateBufferIO.  On failure, leave the buffer invalid.
 */
void
cluster_bufmgr_finish_direct_land_target_for_gcs(BufferDesc *buf, bool valid,
												 XLogRecPtr page_lsn)
{
	if (buf == NULL)
		return;

	if (valid)
	{
		LWLock	   *content_lock = BufferDescriptorGetContentLock(buf);
		Page		page = (Page) BufHdrGetBlock(buf);

		LWLockAcquire(content_lock, LW_EXCLUSIVE);
		PageSetLSN(page, page_lsn);
		LWLockRelease(content_lock);
		TerminateBufferIO(buf, false, BM_VALID);
	}
	else
		TerminateBufferIO(buf, false, 0);
}

/* ========================================================================
 * PGRAC MODIFICATIONS by SqlRush — spec-6.12a (quiescent X->S downgrade).
 *
 *   cluster_bufmgr_downgrade_x_to_s_for_gcs(tag) — master==holder serve-side
 *   self-downgrade so a cross-node read becomes a durable S grant instead of
 *   a one-shot read image.  Under the buffer content_lock EXCLUSIVE:
 *
 *     1. Bail (false) unless our copy holds PCM X and the page is quiescent
 *        (no ACTIVE / LOCK_ONLY_ACTIVE ITL slot).
 *     2. FlushBuffer when dirty — makes shared storage CURRENT before any
 *        S copy exists, so the pre-existing S invalidate + storage-fallback
 *        machinery stays trivially correct (every S copy in the cluster is
 *        storage-consistent; no PI needed until spec-6.12h).  FlushBuffer
 *        performs its own XLogFlush(page_lsn) WAL-before-data.
 *     3. Apply the master-side PCM_TRANS_X_TO_S_DOWNGRADE (caller
 *        guarantees this node is the block master AND the recorded X
 *        holder), then flip the local pcm_state cache X -> S.
 *
 *   Holding the content_lock EXCLUSIVE across flush + flip closes the
 *   local-writer race: a writer that arrives after the flip sees S, which
 *   does not cover X, so its LockBuffer(EXCLUSIVE) takes the full S->X
 *   upgrade round-trip (spec-2.36 invalidate-then-grant) — the write-
 *   permission revocation the downgrade requires (Rule 8.A).
 *
 *   Any bail-out leaves state exactly as found (a completed flush is
 *   harmless).  Runs in the LMON IC-dispatch context; the FlushBuffer
 *   call mirrors the checkpointer contract (pin + content lock held).
 * ======================================================================== */
bool
cluster_bufmgr_downgrade_x_to_s_for_gcs(BufferTag tag)
{
	uint32		hashcode;
	LWLock	   *partition_lock;
	int			buf_id;
	BufferDesc *buf;
	LWLock	   *content_lock;
	uint32		buf_state;
	bool		dirty;

	hashcode = BufTableHashCode(&tag);
	partition_lock = BufMappingPartitionLock(hashcode);

	LWLockAcquire(partition_lock, LW_SHARED);
	buf_id = BufTableLookup(&tag, hashcode);
	if (buf_id < 0)
	{
		LWLockRelease(partition_lock);
		return false;
	}
	buf = GetBufferDescriptor(buf_id);

	buf_state = LockBufHdr(buf);
	if (!BufferTagsEqual(&buf->tag, &tag) || (buf_state & BM_VALID) == 0)
	{
		UnlockBufHdr(buf, buf_state);
		LWLockRelease(partition_lock);
		return false;
	}
	cluster_bufmgr_pin_for_gcs_locked(buf, buf_state);
	LWLockRelease(partition_lock);

	content_lock = BufferDescriptorGetContentLock(buf);
	LWLockAcquire(content_lock, LW_EXCLUSIVE);
	if (!cluster_bufmgr_pcm_x_content_write_permitted(buf))
	{
		LWLockRelease(content_lock);
		cluster_bufmgr_unpin_for_gcs(buf);
		return false;
	}

	/*
	 * Re-verify under the content lock: still our tag, still PCM X, and
	 * quiescent.  An ACTIVE ITL slot means a local transaction still needs
	 * this in-memory copy for its commit stamp (the P0-2 dependency) — the
	 * caller falls back to the one-shot read-image ship.
	 */
	if (!BufferTagsEqual(&buf->tag, &tag)
		|| (PcmState) buf->pcm_state != PCM_STATE_X
		|| cluster_itl_page_has_active_slot((Page) BufHdrGetBlock(buf)))
	{
		LWLockRelease(content_lock);
		cluster_bufmgr_unpin_for_gcs(buf);
		return false;
	}

	buf_state = LockBufHdr(buf);
	dirty = (buf_state & BM_DIRTY) != 0;
	UnlockBufHdr(buf, buf_state);

	if (dirty)
		FlushBuffer(buf, NULL, IOOBJECT_RELATION, IOCONTEXT_NORMAL);

	if (!cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_X_TO_S_DOWNGRADE,
											   cluster_node_id))
	{
		/* Master refused (state moved under us) — leave local X untouched. */
		LWLockRelease(content_lock);
		cluster_bufmgr_unpin_for_gcs(buf);
		return false;
	}

	/* PGRAC ownership-generation wave: the X->S downgrade is a committed
	 * ownership transition -- set pcm_state and bump the generation atomically
	 * (header spinlock) so a cached-X writer racing the content-lock window
	 * detects the revoke even across an X->S->X ABA. */
	cluster_pcm_own_transition(buf, (uint8) PCM_STATE_S, 0, 0);

	LWLockRelease(content_lock);
	cluster_bufmgr_unpin_for_gcs(buf);
	return true;
}

/* ========================================================================
 * PGRAC MODIFICATIONS by SqlRush — spec-6.12g (block self-containment,
 * opportunistic commit cleanout).
 *
 *   cluster_bufmgr_lock_resident_for_stamp(rlocator, forknum, blocknum) —
 *   a NO-FETCH resident-buffer acquire for the commit-time ITL stamp path.
 *   Returns a normally-pinned + content-lock-EXCLUSIVE Buffer iff the block
 *   is ALREADY resident in this backend's buffer pool; InvalidBuffer
 *   otherwise (never reads from storage / cache-fusion).
 *
 *   Residency IS the ownership proof under spec-6.12g: a self-contained
 *   X-transfer (cluster_gcs_block.c D-g2) InvalidateBuffer-drops the
 *   holder's copy as it ships, so a block still resident here was NOT
 *   transferred away and its ITL slots are ours to stamp.  Stamping a
 *   re-fetched copy of a block we no longer own would either trip the
 *   P0-2 stamp assert or (worse) later flush a stale image over the new
 *   holder's version -- hence the NO-FETCH contract (a freshly-fetched
 *   buffer's pcm_state is not an authoritative ownership signal).
 *
 *   Caller MUST release with cluster_bufmgr_unlock_resident_stamp().  Runs
 *   in a backend at commit (has a resource owner), so the standard
 *   PinBuffer_Locked / ReleaseBuffer accounting applies (unlike the LMON
 *   raw-pin helpers).
 * ======================================================================== */
Buffer
cluster_bufmgr_lock_resident_for_stamp(RelFileLocator rlocator, ForkNumber forknum,
									   BlockNumber blocknum)
{
	BufferTag	tag;
	uint32		hashcode;
	uint32		buf_state;
	LWLock	   *partition_lock;
	int			buf_id;
	BufferDesc *buf;

	InitBufferTag(&tag, &rlocator, forknum, blocknum);
	hashcode = BufTableHashCode(&tag);
	partition_lock = BufMappingPartitionLock(hashcode);

	ReservePrivateRefCountEntry();
	ResourceOwnerEnlargeBuffers(CurrentResourceOwner);

	LWLockAcquire(partition_lock, LW_SHARED);
	buf_id = BufTableLookup(&tag, hashcode);
	if (buf_id < 0)
	{
		/* Not resident -> transferred away (or never cached).  Skip stamp. */
		LWLockRelease(partition_lock);
		return InvalidBuffer;
	}
	buf = GetBufferDescriptor(buf_id);

	/*
	 * Pin under the header spinlock, then drop the partition lock (the pin
	 * keeps the identity stable), and take the content lock EXCLUSIVE for the
	 * GenericXLog stamp mutation.
	 */

	/*
	 * PGRAC: spec-6.12g bugfix — PinBuffer (NOT PinBuffer_Locked) because
	 * the committing backend may ALREADY hold a pin on this block (a same-txn
	 * DML that has not yet released it).  PinBuffer_Locked asserts a
	 * first-time pin (Assert(GetPrivateRefCountEntry(...) == NULL)) and
	 * crashes the whole postmaster under concurrent write load; PinBuffer
	 * handles the already-pinned case (bumps the existing private refcount).
	 * The standard buffer-hit path (BufferAlloc) pins the same way while
	 * holding the partition lock, so this is a legal call sequence.
	 */
	(void)PinBuffer(buf, NULL);
	LWLockRelease(partition_lock);

	LWLockAcquire(BufferDescriptorGetContentLock(buf), LW_EXCLUSIVE);

	/*
	 * Re-validate under the content lock: a concurrent transfer could have
	 * evicted + reused this descriptor between the pin and the lock.  A
	 * retained/revoking source is still resident by design but is no longer a
	 * legal commit-stamp target.
	 */
	buf_state = LockBufHdr(buf);
	if (!BufferTagsEqual(&buf->tag, &tag)
		|| (buf_state & BM_VALID) == 0
		|| cluster_bufmgr_pcm_x_retained_image_locked(buf, buf_state)
		|| (cluster_pcm_own_flags_get(buf->buf_id) & PCM_OWN_FLAG_REVOKING) != 0)
	{
		UnlockBufHdr(buf, buf_state);
		LWLockRelease(BufferDescriptorGetContentLock(buf));
		ReleaseBuffer(BufferDescriptorGetBuffer(buf));
		return InvalidBuffer;
	}
	UnlockBufHdr(buf, buf_state);

	return BufferDescriptorGetBuffer(buf);
}

void
cluster_bufmgr_unlock_resident_stamp(Buffer buffer)
{
	BufferDesc *buf;

	if (!BufferIsValid(buffer))
		return;
	buf = GetBufferDescriptor(buffer - 1);
	LWLockRelease(BufferDescriptorGetContentLock(buf));
	ReleaseBuffer(buffer);
}

/* ========================================================================
 * PGRAC MODIFICATIONS by SqlRush — spec-6.12a ㉕ (remote-holder X->S
 * downgrade).
 *
 *   cluster_bufmgr_downgrade_x_to_s_remote_for_gcs(tag, master_node) — the
 *   REMOTE-holder twin of cluster_bufmgr_downgrade_x_to_s_for_gcs above.
 *   Runs on the X HOLDER when the block's GCS master is a DIFFERENT node
 *   (the master forwarded an N->S read with the downgrade-request flag).
 *   Steps 1-2 (verify X + quiescent under the content_lock EXCLUSIVE,
 *   FlushBuffer when dirty) are identical to the local variant.  Step 3
 *   differs: the master PCM entry lives on master_node, so the
 *   PCM_TRANS_X_TO_S_DOWNGRADE is delivered as a fire-and-forget wire
 *   notify (cluster_gcs_send_transition_nowait) — this handler runs in the
 *   LMON IC-dispatch context, which structurally cannot block on a reply it
 *   would have to deliver itself.  The local pcm_state flips X->S ONLY
 *   after the notify was handed to the transport; a send failure bails with
 *   every state exactly as found (master unchanged, local X kept).
 *
 *   Safety of not observing the master's verdict: while this node holds a
 *   local X (verified under the content_lock), the master's recorded
 *   x_holder for the tag must be this node in the same epoch — every
 *   transfer path destroys the holder's local X copy first, so a same-epoch
 *   x_holder mismatch is unreachable; the notify carries the current epoch
 *   and a post-reconfig master rejects it stale (HC73), where the epoch
 *   cache-invalidation sweep already handles surviving copies.  A LOST
 *   notify leaves master=X@us, local=S: our own writes then take the S->X
 *   upgrade round-trip which fails closed until re-converged, and the
 *   requester's registration try-ACK fails closed to one-shot semantics —
 *   no path serves a copy the master does not track (Rule 8.A).
 * ======================================================================== */
bool
cluster_bufmgr_downgrade_x_to_s_remote_for_gcs(BufferTag tag, int32 master_node)
{
	uint32		hashcode;
	LWLock	   *partition_lock;
	int			buf_id;
	BufferDesc *buf;
	LWLock	   *content_lock;
	uint32		buf_state;
	bool		dirty;

	if (master_node < 0 || master_node == cluster_node_id)
		return false;

	hashcode = BufTableHashCode(&tag);
	partition_lock = BufMappingPartitionLock(hashcode);

	LWLockAcquire(partition_lock, LW_SHARED);
	buf_id = BufTableLookup(&tag, hashcode);
	if (buf_id < 0)
	{
		LWLockRelease(partition_lock);
		return false;
	}
	buf = GetBufferDescriptor(buf_id);

	buf_state = LockBufHdr(buf);
	if (!BufferTagsEqual(&buf->tag, &tag) || (buf_state & BM_VALID) == 0)
	{
		UnlockBufHdr(buf, buf_state);
		LWLockRelease(partition_lock);
		return false;
	}
	cluster_bufmgr_pin_for_gcs_locked(buf, buf_state);
	LWLockRelease(partition_lock);

	content_lock = BufferDescriptorGetContentLock(buf);
	LWLockAcquire(content_lock, LW_EXCLUSIVE);
	if (!cluster_bufmgr_pcm_x_content_write_permitted(buf))
	{
		LWLockRelease(content_lock);
		cluster_bufmgr_unpin_for_gcs(buf);
		return false;
	}

	/* Same re-verify as the local variant: tag, PCM X, quiescent. */
	if (!BufferTagsEqual(&buf->tag, &tag)
		|| (PcmState) buf->pcm_state != PCM_STATE_X
		|| cluster_itl_page_has_active_slot((Page) BufHdrGetBlock(buf)))
	{
		LWLockRelease(content_lock);
		cluster_bufmgr_unpin_for_gcs(buf);
		return false;
	}

	buf_state = LockBufHdr(buf);
	dirty = (buf_state & BM_DIRTY) != 0;
	UnlockBufHdr(buf, buf_state);

	if (dirty)
		FlushBuffer(buf, NULL, IOOBJECT_RELATION, IOCONTEXT_NORMAL);

	/*
	 * PGRAC: GCS-race round-4c P1 (yield-notify liveness) — deterministic
	 * wire-loss simulation.  SKIP models the notify being handed to the
	 * transport and then LOST (fire-and-forget has no ACK): the local state
	 * flips to S exactly as on a successful handoff, but the master keeps
	 * recording X@us.  Drives the renotify self-heal TAP leg (t/348 L8).
	 */
	CLUSTER_INJECTION_POINT("cluster-gcs-block-yield-notify-drop");
	if (cluster_injection_should_skip("cluster-gcs-block-yield-notify-drop"))
	{
		/* simulated post-handoff loss: fall through to the S flip */
	}
	else if (!cluster_gcs_send_transition_nowait(tag, PCM_TRANS_X_TO_S_DOWNGRADE, master_node))
	{
		/* Notify not handed to transport — keep local X, master unchanged. */
		LWLockRelease(content_lock);
		cluster_bufmgr_unpin_for_gcs(buf);
		return false;
	}

	/* PGRAC ownership-generation wave: the X->S downgrade is a committed
	 * ownership transition -- set pcm_state and bump the generation atomically
	 * (header spinlock) so a cached-X writer racing the content-lock window
	 * detects the revoke even across an X->S->X ABA. */
	cluster_pcm_own_transition(buf, (uint8) PCM_STATE_S, 0, 0);

	LWLockRelease(content_lock);
	cluster_bufmgr_unpin_for_gcs(buf);
	return true;
}

/* ========================================================================
 * PGRAC MODIFICATIONS by SqlRush — GCS-race round-4c P1 (yield-notify
 *   liveness self-heal).
 *
 *   cluster_bufmgr_renotify_s_for_gcs(tag, master_node) — the X->S yield
 *   notify above is fire-and-forget; if it is lost on the wire the master
 *   keeps recording X@us and keeps BAST-nudging, while the local state is
 *   already S so the downgrade helper refuses every nudge — the requester
 *   starves to its retransmit budget.  When a nudge arrives for a block we
 *   already hold in S, re-send the (idempotent) downgrade notify: a master
 *   that already applied it rejects the duplicate as an illegal S->S
 *   transition and nothing changes;  the lost-notify master applies it and
 *   the requester's next retry proceeds.  Returns true when a notify was
 *   re-sent.
 * ======================================================================== */
bool
cluster_bufmgr_renotify_s_for_gcs(BufferTag tag, int32 master_node)
{
	uint32		hashcode;
	LWLock	   *partition_lock;
	int			buf_id;
	BufferDesc *buf;
	uint32		buf_state;
	bool		is_s;

	if (master_node < 0 || master_node == cluster_node_id)
		return false;

	hashcode = BufTableHashCode(&tag);
	partition_lock = BufMappingPartitionLock(hashcode);

	LWLockAcquire(partition_lock, LW_SHARED);
	buf_id = BufTableLookup(&tag, hashcode);
	if (buf_id < 0)
	{
		LWLockRelease(partition_lock);
		return false;
	}
	buf = GetBufferDescriptor(buf_id);

	buf_state = LockBufHdr(buf);
	is_s = BufferTagsEqual(&buf->tag, &tag) && (buf_state & BM_VALID) != 0
		&& (PcmState) buf->pcm_state == PCM_STATE_S;
	UnlockBufHdr(buf, buf_state);
	LWLockRelease(partition_lock);

	if (!is_s)
		return false;

	return cluster_gcs_send_transition_nowait(tag, PCM_TRANS_X_TO_S_DOWNGRADE, master_node);
}

/*
 * Smart Fusion copy helper: same stable raw-pin/content_lock contract as
 * cluster_bufmgr_copy_block_for_gcs(), but it deliberately does not flush WAL
 * before shipping.  Instead it returns a per-origin dependency vector that the
 * receiver must install before the copied bytes can participate in DBWR or
 * commit.  Any missing origin/LSN evidence returns false; callers must not
 * silently downgrade that specific Smart Fusion attempt.
 */
bool
cluster_bufmgr_copy_block_for_gcs_smart_fusion(BufferTag tag, XLogRecPtr *out_page_lsn, char *dst,
											   ClusterSfDepVec *out_dep_vec)
{
	uint32		hashcode;
	LWLock	   *partition_lock;
	int			buf_id;
	BufferDesc *buf;
	LWLock	   *content_lock;
	XLogRecPtr	first_lsn;
	XLogRecPtr	second_lsn;
	int			retries;
	bool		copied_stable;
	bool		stable;
	Page		page;

	if (dst == NULL || out_page_lsn == NULL || out_dep_vec == NULL)
		return false;
	if (!cluster_sf_dep_origin_valid(cluster_node_id))
		return false;

	cluster_sf_dep_vec_reset(out_dep_vec);
	hashcode = BufTableHashCode(&tag);
	partition_lock = BufMappingPartitionLock(hashcode);

	LWLockAcquire(partition_lock, LW_SHARED);
	buf_id = BufTableLookup(&tag, hashcode);
	if (buf_id < 0)
	{
		LWLockRelease(partition_lock);
		return false;
	}
	buf = GetBufferDescriptor(buf_id);

	{
		uint32		buf_state;

		buf_state = LockBufHdr(buf);
		if (!BufferTagsEqual(&buf->tag, &tag)
			|| !cluster_bufmgr_pcm_current_image_locked(buf, buf_state))
		{
			UnlockBufHdr(buf, buf_state);
			LWLockRelease(partition_lock);
			return false;
		}
		cluster_bufmgr_pin_for_gcs_locked(buf, buf_state);
	}
	LWLockRelease(partition_lock);

	content_lock = BufferDescriptorGetContentLock(buf);
	page = (Page) BufHdrGetBlock(buf);
	copied_stable = false;
	stable = false;
	for (retries = 0; retries < 2; retries++)
	{
		LWLockAcquire(content_lock, LW_SHARED);
		{
			uint32 buf_state = LockBufHdr(buf);
			bool current = BufferTagsEqual(&buf->tag, &tag)
				&& cluster_bufmgr_pcm_current_image_locked(buf, buf_state);

			UnlockBufHdr(buf, buf_state);
			if (!current)
			{
				LWLockRelease(content_lock);
				break;
			}
		}
		first_lsn = PageGetLSN(page);
		memcpy(dst, page, BLCKSZ);
		second_lsn = PageGetLSN(page);
		if (BufferTagsEqual(&buf->tag, &tag) && !XLogRecPtrIsInvalid(first_lsn)
			&& first_lsn == second_lsn)
		{
			*out_page_lsn = second_lsn;
			LWLockRelease(content_lock);
			copied_stable = true;
			break;
		}
		LWLockRelease(content_lock);
	}

	if (copied_stable)
	{
		ClusterSfDepVec existing_vec;
		bool		has_existing_deps;
		bool		add_local_dep;
		uint32		buf_state;

		/*
		 * Keep lock ordering compatible with receiver install:
		 * cluster_sf_dep_install_vec() takes the Smart Fusion dep lock before
		 * page install may take content_lock EXCLUSIVE.  We therefore read
		 * the dep store after releasing content_lock, while the raw pin keeps
		 * this descriptor/tag from being recycled.
		 */
		has_existing_deps =
			cluster_sf_dep_vec_for_ship(BufferDescriptorGetBuffer(buf), &existing_vec);
		cluster_sf_dep_vec_reset(out_dep_vec);
		if (has_existing_deps)
			(void) cluster_sf_dep_vec_union(out_dep_vec, &existing_vec);

		buf_state = LockBufHdr(buf);
		add_local_dep = (buf_state & (BM_DIRTY | BM_JUST_DIRTIED)) != 0
						|| !has_existing_deps;
		UnlockBufHdr(buf, buf_state);
		if (add_local_dep)
			(void) cluster_sf_dep_vec_set(out_dep_vec, cluster_node_id, *out_page_lsn);
		stable = !cluster_sf_dep_vec_is_empty(out_dep_vec);
	}

	cluster_bufmgr_unpin_for_gcs(buf);
	if (!stable)
		cluster_sf_dep_vec_reset(out_dep_vec);
	return stable;
}

/*
 * cluster_bufmgr_redeclare_scan_chunk -- spec-4.7 D2 (Q6-A' worker-centric).
 *
 *	Scan a bounded chunk [start_buf, start_buf + max_scan) of the shared
 *	buffer pool;  for every locally-resident buffer that holds a covering PCM
 *	mode (BM_VALID ∧ !BM_IO_IN_PROGRESS ∧ pcm_state ∈ {S,X} ∧ PCM-tracked ∧
 *	valid page_lsn) invoke cb(tag, mode, page_lsn, arg).  Returns the next
 *	cursor (== NBuffers once the whole pool has been scanned), so the LMON
 *	reconfig tick can drive it in bounded chunks across ticks without
 *	blocking the heartbeat (§6 risk mitigation).
 *
 *	pcm_state + tag are read under the buffer-header spinlock (the shared
 *	authoritative per-node PCM state — Q6-A', not backend-private);  page_lsn
 *	is read under content_lock SHARED after a raw pin (mirrors
 *	cluster_bufmgr_copy_block_for_gcs, minus the HC82 WAL flush / HC89
 *	revalidation — the re-declare page_lsn is a watermark hint, D5 does the
 *	authoritative lost-write check).  No PCM state is mutated.
 */
int
cluster_bufmgr_redeclare_scan_chunk(int start_buf, int max_scan,
									ClusterGcsRedeclareCallback cb, void *arg)
{
	int			i;
	int			end;

	Assert(cb != NULL);

	if (start_buf < 0)
		start_buf = 0;
	end = start_buf + max_scan;
	if (end > NBuffers)
		end = NBuffers;

	for (i = start_buf; i < end; i++)
	{
		BufferDesc *buf = GetBufferDescriptor(i);
		uint32		buf_state;
		uint8		mode;
		BufferTag	tag;
		LWLock	   *content_lock;
		XLogRecPtr	page_lsn;
		SCN			page_scn;	/* spec-2.41 D3 — re-declare SCN carrier */

		buf_state = LockBufHdr(buf);
		mode = buf->pcm_state;
		if ((buf_state & BM_VALID) == 0
			|| (buf_state & BM_IO_IN_PROGRESS) != 0
			|| (mode != (uint8) PCM_STATE_S && mode != (uint8) PCM_STATE_X)
			|| !cluster_bufmgr_should_pcm_track(buf))
		{
			UnlockBufHdr(buf, buf_state);
			continue;
		}
		tag = buf->tag;
		/* pin + unlock header (raw pin, mirrors copy_block_for_gcs). */
		cluster_bufmgr_pin_for_gcs_locked(buf, buf_state);

		content_lock = BufferDescriptorGetContentLock(buf);
		LWLockAcquire(content_lock, LW_SHARED);
		page_lsn = PageGetLSN((Page) BufHdrGetBlock(buf));
		/* PGRAC: spec-2.41 D3 — also read pd_block_scn so the re-declare carries
		 * the cross-node version for the detector's SCN watermark (alongside
		 * page_lsn for the redo-coverage required_lsn). */
		page_scn = ((PageHeader) BufHdrGetBlock(buf))->pd_block_scn;
		LWLockRelease(content_lock);

		cluster_bufmgr_unpin_for_gcs(buf);

		/* P1#2: only re-declare a buffer with a valid pd_lsn. */
		if (!XLogRecPtrIsInvalid(page_lsn))
			cb(tag, mode, page_lsn, page_scn, arg);
	}

	return end;
}

/* ========================================================================
 * PGRAC MODIFICATIONS by SqlRush — spec-2.36 D4 (HC118 / HC123) +
 *   spec-2.41 D3 (*out_page_scn).
 *
 *   cluster_bufmgr_invalidate_block_for_gcs(tag, expected_mode, *out_page_lsn,
 *   *out_page_scn) — by-tag invalidate wrapper called from the holder-side
 *   invalidate handler in cluster_gcs_block.c.  InvalidateBuffer is a static
 *   helper inside this file (bufmgr.c), so the cluster/ subdirectory cannot
 *   call it directly;  this wrapper exposes a controlled entrypoint.
 *   spec-2.41 D3 also returns the dropping page's pd_block_scn (*out_page_scn)
 *   so the invalidate ACK can carry the cross-node version:
 *
 *     1. Partition lookup by tag.
 *     2. Header lock + tag recheck + BM_VALID gate + foreign-pin fast
 *        fail (GCS serve-stall round-5: PINNED result, nothing changed).
 *     3. XLogFlush(page_lsn) when BM_DIRTY (HC123 invariant — lost-
 *        write safety since spec-2.36 ships no PI buffer copy).
 *     4. InvalidateBufferTry to drop the local copy — bounded;  a raced
 *        pin re-fails with PINNED (round-5;  see
 *        ClusterBufmgrGcsDropResult in cluster_gcs_block.h).
 *
 *   `expected_mode` is advisory.
 * ======================================================================== */
ClusterBufmgrGcsDropResult
cluster_bufmgr_invalidate_block_for_gcs(BufferTag tag, PcmLockMode expected_mode,
										XLogRecPtr *out_page_lsn, SCN *out_page_scn)
{
	uint32		hashcode;
	LWLock	   *partition_lock;
	int			buf_id;
	BufferDesc *buf;
	uint32		buf_state;
	XLogRecPtr	page_lsn = InvalidXLogRecPtr;
	SCN			page_scn = InvalidScn;	/* PGRAC: spec-2.41 D3 — page version for the ACK SCN carrier */
	bool		was_dirty = false;
	uint8		saved_pcm_state;
	uint64		staged_gen;		/* PGRAC W2: ownership gen captured at stage-N */

	(void) expected_mode;

	if (out_page_lsn != NULL)
		*out_page_lsn = InvalidXLogRecPtr;
	if (out_page_scn != NULL)
		*out_page_scn = InvalidScn;

	hashcode = BufTableHashCode(&tag);
	partition_lock = BufMappingPartitionLock(hashcode);

	LWLockAcquire(partition_lock, LW_SHARED);
	buf_id = BufTableLookup(&tag, hashcode);
	if (buf_id < 0)
	{
		LWLockRelease(partition_lock);
		return CLUSTER_BUFMGR_GCS_DROP_NOT_RESIDENT;
	}
	buf = GetBufferDescriptor(buf_id);

	buf_state = LockBufHdr(buf);
	/* Re-verify tag under the header lock to defend against a tag-rewrite race
	 * between the partition-lock lookup and the raw pin (copy_block_for_gcs
	 * convention). */
	if (!BufferTagsEqual(&buf->tag, &tag) || (buf_state & BM_VALID) == 0)
	{
		UnlockBufHdr(buf, buf_state);
		LWLockRelease(partition_lock);
		return CLUSTER_BUFMGR_GCS_DROP_NOT_RESIDENT;
	}

	/*
	 * PGRAC: GCS serve-stall round-5 (A2) — fast pin fail.  A foreign pin
	 * means the drop cannot complete without InvalidateBuffer's unbounded
	 * pin-wait loop, which this IC-dispatch context must never enter (see
	 * ClusterBufmgrGcsDropResult).  Bail before the WAL flush — nothing is
	 * dropped, so the HC123 flush-before-drop invariant is not owed yet.
	 */
	if (BUF_STATE_GET_REFCOUNT(buf_state) != 0)
	{
		UnlockBufHdr(buf, buf_state);
		LWLockRelease(partition_lock);
		return CLUSTER_BUFMGR_GCS_DROP_PINNED;
	}
	was_dirty = (buf_state & BM_DIRTY) != 0;

	/*
	 * PGRAC: spec-2.41 D3 — read page_lsn AND pd_block_scn under content_lock
	 * SHARED, not the buffer-header spinlock.  The page-header CONTENTS
	 * (including pd_block_scn) are protected by the content lock, NOT by the
	 * buffer-header spinlock; reading them under the header lock alone could
	 * race a concurrent content_lock-EXCLUSIVE writer and tear the 8-byte SCN.
	 * Since the ACK page_scn is now a correctness watermark source for the
	 * lost-write detector, raw-pin the buffer under the header lock (so it
	 * cannot be replaced after we drop the header lock), release the partition
	 * lock, then snapshot both values in a single content_lock SHARED hold —
	 * the same raw-pin / content-lock pattern used by
	 * cluster_bufmgr_copy_block_for_gcs and cluster_bufmgr_redeclare_scan_chunk.
	 * The pin keeps the buffer identity stable, so the pre-pin tag match holds.
	 */
	cluster_bufmgr_pin_for_gcs_locked(buf, buf_state);	/* raw-pin + unlock header */
	LWLockRelease(partition_lock);
	{
		LWLock	   *content_lock = BufferDescriptorGetContentLock(buf);
		Page		page = (Page) BufHdrGetBlock(buf);

		LWLockAcquire(content_lock, LW_SHARED);
		page_lsn = PageGetLSN(page);
		page_scn = ((PageHeader) page)->pd_block_scn;
		LWLockRelease(content_lock);
	}
	cluster_bufmgr_unpin_for_gcs(buf);

	if (out_page_lsn != NULL)
		*out_page_lsn = page_lsn;
	if (out_page_scn != NULL)
		*out_page_scn = page_scn;

	if (was_dirty && !XLogRecPtrIsInvalid(page_lsn))
		XLogFlush(cluster_gcs_clamp_ship_flush_lsn(page_lsn));

	/*
	 * PGRAC: spec-6.12a ㉕ (latent-bug fix) — InvalidateBuffer requires
	 * the buffer header spinlock held at ENTRY (it Assert()s BM_LOCKED) and
	 * releases it internally.  The bare call here previously ran with the
	 * header unlocked; it never fired because the invalidate handler's
	 * remote-holder path short-circuited before reaching the drop (see the
	 * matching handler fix), so the first real caller tripped the assert.
	 * Re-lock, re-validate the mapping (the buffer may have been evicted /
	 * retagged while unpinned during XLogFlush), and clear the residency mode
	 * under the lock so InvalidateBuffer's eviction hook sees PCM_STATE_N and
	 * skips the remote-master release wire (the §3.5 LMON context has no
	 * backend slot to send one — same BLOCKER A contract as
	 * cluster_bufmgr_drop_block_for_gcs_no_wire below).
	 */
	buf_state = LockBufHdr(buf);
	if (!BufferTagsEqual(&buf->tag, &tag) || (buf_state & BM_VALID) == 0)
	{
		UnlockBufHdr(buf, buf_state);
		return CLUSTER_BUFMGR_GCS_DROP_NOT_RESIDENT;
	}

	/*
	 * PGRAC: GCS serve-stall round-5 (A2) — a pin acquired while we were
	 * unpinned for the XLogFlush re-fails the drop (bounded contract; the
	 * pcm_state is untouched so nothing observes a half-dropped state).
	 */
	if (BUF_STATE_GET_REFCOUNT(buf_state) != 0)
	{
		UnlockBufHdr(buf, buf_state);
		return CLUSTER_BUFMGR_GCS_DROP_PINNED;
	}

	saved_pcm_state = buf->pcm_state;
	staged_gen = cluster_pcm_own_gen_get(buf->buf_id);	/* PGRAC W2 */
	buf->pcm_state = (uint8) PCM_STATE_N;

	/* PGRAC: spec-6.12h D-h1 — keep a Past Image instead of dropping. */
	if (cluster_bufmgr_convert_to_pi_locked(buf, buf_state))
		return CLUSTER_BUFMGR_GCS_DROP_DROPPED;

	/*
	 * PGRAC: GCS serve-stall round-5 (A2) — bounded drop.  A pinner racing
	 * in between the refcount check above and the Try variant's own
	 * partition-ordered recheck fails the call;  restore the residency mode
	 * (it was cleared for the eviction hook) so the still-resident copy keeps
	 * its true PCM state.
	 */
	cluster_bufmgr_in_gcs_drop = true;	/* gates the drop-prepin inject */
	if (!InvalidateBufferTry(buf))	/* releases the header spinlock */
	{
		cluster_bufmgr_in_gcs_drop = false;

		/*
		 * GCS serve-stall round-6 wave-2 — restore-ABA window.  The header
		 * spinlock was dropped for the InvalidateBufferTry attempt, so a full
		 * concurrent ownership round (N->X->N) can complete here, leaving
		 * pcm_state back at N with a NEW ownership generation.  This inject
		 * holds the window open so the RED can drive the N->X->N
		 * deterministically; the generation compare added by the ownership
		 * mechanism is what closes it.
		 */
		CLUSTER_INJECTION_POINT("cluster-pcm-restore-aba-window");

		/*
		 * W2 RED force-round — when armed (:skip, one-shot), complete the
		 * concurrent ownership round at the exact window point: one coherent
		 * transition leaving pcm_state at N with the generation bumped, which
		 * is indistinguishable to the guard below from a real N->X->N round
		 * (grant finalize bump + drop-back-to-N bump).  Same force-behavior
		 * inject pattern as cluster-gcs-block-duplicate-grant-reply.
		 */
		CLUSTER_INJECTION_POINT("cluster-pcm-restore-aba-force-round");
		if (cluster_injection_should_skip("cluster-pcm-restore-aba-force-round"))
			cluster_pcm_own_transition(buf, (uint8) PCM_STATE_N, 0, 0);

		buf_state = LockBufHdr(buf);

		/*
		 * PGRAC ownership-generation wave (W2) — restore the staged N ONLY
		 * when it is still the same ownership epoch we staged.  The ==N guard
		 * alone blocks a plain overwrite but NOT an N->X->N ABA: a concurrent
		 * round that granted X (finalize bumped the generation) and then
		 * dropped back to N leaves pcm_state==N again with a NEW generation.
		 * Restoring saved_pcm_state (the stale pre-drop X/S) over that
		 * re-owned block would resurrect a dead grant (Rule 8.A double/stale
		 * holder).  Gate the restore on the generation being unchanged; if it
		 * moved, the block was legitimately re-owned and dropped in between,
		 * so leave it at N.
		 */
		if (BufferTagsEqual(&buf->tag, &tag) &&
			buf->pcm_state == (uint8) PCM_STATE_N &&
			cluster_pcm_own_gen_get(buf->buf_id) == staged_gen)
			buf->pcm_state = saved_pcm_state;
		else if (BufferTagsEqual(&buf->tag, &tag) &&
				 buf->pcm_state == (uint8) PCM_STATE_N &&
				 cluster_pcm_own_gen_get(buf->buf_id) != staged_gen)
			cluster_pcm_note_restore_aba_detected();
		UnlockBufHdr(buf, buf_state);
		return CLUSTER_BUFMGR_GCS_DROP_PINNED;
	}
	cluster_bufmgr_in_gcs_drop = false;

	return CLUSTER_BUFMGR_GCS_DROP_DROPPED;
}

/* ========================================================================
 * PGRAC MODIFICATIONS by SqlRush — GCS-race round-4c FUNC-1 (storage-
 *   fallback SCN verify / refresh; the spec-2.41 lost-write detector
 *   extended to the GRANTED_STORAGE_FALLBACK consume side).
 *
 *   cluster_bufmgr_read_block_scn_for_gcs(buf) — snapshot pd_block_scn
 *   under content_lock SHARED.  Page-header contents are content-lock
 *   protected, NOT buffer-header protected; a raw read could tear the
 *   8-byte SCN against a concurrent content_lock-EXCLUSIVE writer (the
 *   same rule as the invalidate wrapper above).
 *
 *   cluster_bufmgr_refresh_block_from_storage_for_gcs(buf, out_scn) —
 *   discard the CLEAN local bytes and re-read the shared-storage page
 *   into the buffer.  The physical read + verify runs OUTSIDE the content
 *   lock into a scratch block, then the copy happens under content_lock
 *   EXCLUSIVE with BM_DIRTY re-checked inside that hold (hint-bit setters
 *   need at least content SHARE, so the flag cannot flip mid-copy).
 *   Returns false WITHOUT touching the bytes when the buffer is dirty:
 *   dirt could be a newer local version and must never be overwritten
 *   (nor may a provably-stale page ever be flushed over the newer storage
 *   copy) — the caller fail-closes.  The caller holds a pin (LockBuffer
 *   contract), so the buffer identity is stable throughout.
 * ======================================================================== */
SCN
cluster_bufmgr_read_block_scn_for_gcs(BufferDesc *buf)
{
	LWLock	   *content_lock = BufferDescriptorGetContentLock(buf);
	SCN			scn;

	LWLockAcquire(content_lock, LW_SHARED);
	scn = ((PageHeader) BufHdrGetBlock(buf))->pd_block_scn;
	LWLockRelease(content_lock);
	return scn;
}

/*
 * cluster_bufmgr_block_is_extension_for_gcs -- fix 2 (crash-rejoin cold-GRD
 * watermark) extension-block whitelist input.
 *
 *	True iff the tag's block number is at or beyond the relation's current
 *	durable size, i.e. a freshly-extended block that has never been written to
 *	shared storage (its InvalidScn watermark is correct, so the cold-GRD gate
 *	must SKIP it, not fail-closed).  A pre-existing block (blockNum < nblocks)
 *	returns false, so an Invalid watermark on it under a self-fence is treated
 *	as a wiped/cold GRD watermark and fails closed.  Invalidates the cached
 *	size first so a concurrent extension is not missed (cheap: one lseek).
 */
bool
cluster_bufmgr_block_is_extension_for_gcs(BufferTag tag)
{
	SMgrRelation reln = smgropen(BufTagGetRelFileLocator(&tag), InvalidBackendId);
	ForkNumber	fork = BufTagGetForkNum(&tag);

	/* Drop any cached size so a concurrent extension is not missed. */
	smgrrelease(reln);
	return tag.blockNum >= smgrnblocks(reln, fork);
}

bool
cluster_bufmgr_refresh_block_from_storage_for_gcs(BufferDesc *buf, SCN *out_page_scn)
{
	PGAlignedBlock scratch;
	BufferTag	tag = buf->tag;
	SMgrRelation reln;
	LWLock	   *content_lock;
	uint32		buf_state;
	bool		dirty;
	bool		write_permitted;

	if (out_page_scn != NULL)
		*out_page_scn = InvalidScn;

	/*
	 * Physical read + verify outside the content lock.  The caller only
	 * reaches this with a VALID master pi_watermark_scn for the tag, which
	 * implies a previous X holder flushed this block (the watermark advances
	 * on yield/invalidate flush paths) — so the block exists on storage and
	 * this cannot read past EOF (a concurrent truncate would ereport here,
	 * which is the correct fail-closed outcome for a dropped block).
	 */
	reln = smgropen(BufTagGetRelFileLocator(&tag), InvalidBackendId);
	smgrread(reln, BufTagGetForkNum(&tag), tag.blockNum, scratch.data);
	if (!PageIsVerifiedExtended((Page) scratch.data, tag.blockNum,
								PIV_LOG_WARNING | PIV_REPORT_STAT))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid page in block %u of relation %u during GCS storage-fallback refresh",
						tag.blockNum, tag.relNumber)));

	content_lock = BufferDescriptorGetContentLock(buf);
	LWLockAcquire(content_lock, LW_EXCLUSIVE);
	write_permitted = cluster_bufmgr_pcm_x_content_write_permitted(buf);
	buf_state = LockBufHdr(buf);
	dirty = (buf_state & BM_DIRTY) != 0;
	UnlockBufHdr(buf, buf_state);
	if (dirty || !write_permitted)
	{
		LWLockRelease(content_lock);
		return false;
	}
	memcpy((char *) BufHdrGetBlock(buf), scratch.data, BLCKSZ);
	LWLockRelease(content_lock);

	if (out_page_scn != NULL)
		*out_page_scn = ((PageHeader) scratch.data)->pd_block_scn;
	return true;
}

/* ========================================================================
 * PGRAC MODIFICATIONS by SqlRush — spec-6.12h D-h1 (AD-002 PI orthogonal
 * state activation).
 *
 *   cluster_bufmgr_convert_to_pi_locked(buf, buf_state) — with
 *   cluster.past_image on and the buffer unpinned, convert the outgoing
 *   copy into a Past Image INSTEAD of InvalidateBuffer: clear BM_VALID and
 *   every dirty-tracking flag, stamp buffer_type = BUF_TYPE_PI, and keep
 *   the tag mapping + page bytes intact.  Returns true when it converted
 *   (header lock released); false when the caller must fall back to the
 *   plain drop (GUC off, or the buffer is pinned — a PI is fail-safe by
 *   contract, so ANY doubt falls back to today's InvalidateBuffer).
 *
 *   Why this exact flag shape is the never-serve-read guard (§3.4b hard
 *   invariant): PG's BufTable holds ONE buffer per tag, so an Oracle-style
 *   PI+current pair cannot coexist; a BM_TAG_VALID/!BM_VALID buffer is
 *   native PG for "mapping exists, content needs IO" — every reader treats
 *   it as a miss (StartBufferIO + re-read/install overwrite the bytes =
 *   the implicit discard), the checkpointer/bgwriter skip it (!BM_DIRTY;
 *   never flushing a PI is ALSO the no-stale-flush 8.A contract: shared
 *   storage must not be clobbered under the new holder), and clock-sweep
 *   eviction may reuse it at any time (dropping a PI only loses the D-h3
 *   recovery shortcut, never correctness — storage + full redo remain).
 *
 *   D-h3a: the conversion also stamps the SHIP SCN — the local Lamport
 *   clock at the conversion instant — into the PI shadow slot
 *   (cluster_pi_shadow.h holds the full recovery-boundary proof).  The
 *   sample MUST be ordered after the refcount==0 check above, i.e. taken
 *   inside this header-lock hold: a writer that came and went between an
 *   earlier (outside-the-lock) sample and LockBufHdr would leave a
 *   lineage record stamped ABOVE the boundary, which D-h3 replay would
 *   re-apply onto bytes that already contain it (double-apply, Rule 8.A).
 *   cluster_scn_current() is lock-free atomic reads only (spec-1.17), so
 *   sampling under the buffer-header spinlock is the same class as the
 *   lever ticks above.
 * ======================================================================== */
static bool
cluster_bufmgr_convert_to_pi_locked(BufferDesc *buf, uint32 buf_state)
{
	if (!cluster_past_image)
		return false;			/* wave off: caller drops as today */
	if (BUF_STATE_GET_REFCOUNT(buf_state) != 0)
	{
		/* Pinned: fail-safe fallback to the plain drop (single lock-free
		 * atomic tick; same class as PG's own buf->state atomics under
		 * this spinlock). */
		cluster_lever_h_note_pi_ineligible();
		return false;
	}

	buf_state &= ~(BM_VALID | BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED | BM_IO_ERROR);
	buf->buffer_type = (uint8) BUF_TYPE_PI;
	/* D-h3a ship-SCN boundary stamp — see the header note on why the
	 * clock sample must sit inside this lock hold.  An unarmed clock
	 * stamps InvalidScn and the D-h3 gate fails closed to UNUSABLE. */
	cluster_pi_shadow_stamp(buf->buf_id, cluster_scn_current());
	UnlockBufHdr(buf, buf_state);
	cluster_lever_h_note_pi_kept();
	return true;
}

/* ========================================================================
 * PGRAC MODIFICATIONS by SqlRush — spec-6.12a ㉕.
 *
 *   cluster_bufmgr_block_pcm_state(tag) — the resident buffer's node-local
 *   PCM residency mirror for `tag` (PCM_STATE_N when the block is not
 *   resident).  The INVALIDATE envelope handler needs the HOLDER-side view:
 *   consulting cluster_pcm_lock_query there reads the local MASTER hash
 *   table, which on a non-master holder has no entry and always answers N —
 *   the latent pre-㉕ bug that made every remote-holder INVALIDATE
 *   short-circuit to "already invalidated" while the cached S copy silently
 *   survived (Rule 8.A stale-S hazard, masked on the phantom-shared harness
 *   by the AD-015 divergence honesty note).
 * ======================================================================== */
PcmLockMode
cluster_bufmgr_block_pcm_state(BufferTag tag)
{
	uint32		hashcode;
	LWLock	   *partition_lock;
	int			buf_id;
	BufferDesc *buf;
	uint32		buf_state;
	PcmLockMode mode = PCM_LOCK_MODE_N;

	hashcode = BufTableHashCode(&tag);
	partition_lock = BufMappingPartitionLock(hashcode);

	LWLockAcquire(partition_lock, LW_SHARED);
	buf_id = BufTableLookup(&tag, hashcode);
	if (buf_id < 0)
	{
		LWLockRelease(partition_lock);
		return PCM_LOCK_MODE_N;
	}
	buf = GetBufferDescriptor(buf_id);

	buf_state = LockBufHdr(buf);
	if (BufferTagsEqual(&buf->tag, &tag) && (buf_state & BM_VALID) != 0)
		mode = (PcmLockMode) buf->pcm_state;
	UnlockBufHdr(buf, buf_state);
	LWLockRelease(partition_lock);

	return mode;
}

/*
 * PGRAC ownership-generation wave (W3) — is a grant for this tag in flight to
 * install on THIS node?  Between the install (inside acquire, under its own
 * content lock) and the LockBuffer finalize, pcm_state is still N but the
 * GRANT_PENDING flag is set.  An invalidate handler that sees N must consult
 * this before treating the block as already-invalidated: acking it away would
 * strand a stale grant after finalize.  Same by-tag lookup as
 * cluster_bufmgr_block_pcm_state; returns false when the block is not resident.
 */
bool
cluster_bufmgr_block_grant_pending(BufferTag tag)
{
	uint32		hashcode;
	LWLock	   *partition_lock;
	int			buf_id;
	BufferDesc *buf;
	uint32		buf_state;
	bool		pending = false;

	hashcode = BufTableHashCode(&tag);
	partition_lock = BufMappingPartitionLock(hashcode);

	LWLockAcquire(partition_lock, LW_SHARED);
	buf_id = BufTableLookup(&tag, hashcode);
	if (buf_id < 0)
	{
		LWLockRelease(partition_lock);
		return false;
	}
	buf = GetBufferDescriptor(buf_id);

	buf_state = LockBufHdr(buf);
	if (BufferTagsEqual(&buf->tag, &tag) && (buf_state & BM_VALID) != 0)
		pending = (cluster_pcm_own_flags_get(buf->buf_id) & PCM_OWN_FLAG_GRANT_PENDING) != 0;
	UnlockBufHdr(buf, buf_state);
	LWLockRelease(partition_lock);

	return pending;
}

/* ========================================================================
 * PGRAC MODIFICATIONS by SqlRush — spec-5.2 D11 (writer-transfer-revoke).
 *
 *   cluster_bufmgr_drop_block_for_gcs_no_wire(tag, *out_page_lsn)
 *   — by-tag local buffer drop with NO GCS release wire, called from the
 *   holder-side X-transfer branch of the GCS forward handler, which runs
 *   in the §3.5 IC-dispatch (LMON) context.
 *
 *   Identical to cluster_bufmgr_invalidate_block_for_gcs (partition lookup
 *   → XLogFlush when dirty for HC123 lost-write safety → InvalidateBuffer)
 *   with ONE difference: it clears the BufferDesc pcm_state to PCM_STATE_N
 *   under the buffer-header lock BEFORE InvalidateBuffer runs.  That
 *   pre-empts InvalidateBuffer's spec-2.35 D4 (HC112) cache-eviction hook,
 *   whose guard is `buf->pcm_state != N`: for an X holder the hook would
 *   call cluster_pcm_lock_release_buffer_for_eviction(buf, X) which, when
 *   the master is a REMOTE node, sends an X→N release transition wire via
 *   cluster_gcs_send_transition_and_wait → gcs_reserve_slot, which needs a
 *   backend slot (MyProcNumber/MyBackendId).  LMON is an aux process with
 *   no backend slot, so that wire raises ERROR and the §3.5 wrapper drops
 *   the whole frame (the reply is never sent).
 *
 *   No wire is correct here: in the spec-5.2 path-A topology the REQUESTER
 *   is the local master (it forwarded N→X with IsXTransfer to us, a remote
 *   X holder), so it already owns the transfer and records itself as the
 *   new X holder on install.  We (the previous holder) therefore drop our
 *   copy unilaterally — there is no one to notify.  The current image was
 *   already shipped in the reply before this call, so dropping cannot lose
 *   data, and the XLogFlush + InvalidateBuffer pair preserves Rule 8.A
 *   no-stale-flush.
 * ======================================================================== */
ClusterBufmgrGcsDropResult
cluster_bufmgr_drop_block_for_gcs_no_wire(BufferTag tag, XLogRecPtr expected_lsn,
										  XLogRecPtr *out_page_lsn)
{
	uint32		hashcode;
	LWLock	   *partition_lock;
	int			buf_id;
	BufferDesc *buf;
	uint32		buf_state;
	XLogRecPtr	page_lsn = InvalidXLogRecPtr;
	bool		was_dirty = false;
	uint8		saved_pcm_state;
	uint64		staged_gen;		/* PGRAC W2: ownership gen captured at stage-N */

	if (out_page_lsn != NULL)
		*out_page_lsn = InvalidXLogRecPtr;

	hashcode = BufTableHashCode(&tag);
	partition_lock = BufMappingPartitionLock(hashcode);

	LWLockAcquire(partition_lock, LW_SHARED);
	buf_id = BufTableLookup(&tag, hashcode);
	if (buf_id < 0)
	{
		LWLockRelease(partition_lock);
		return CLUSTER_BUFMGR_GCS_DROP_NOT_RESIDENT;
	}
	buf = GetBufferDescriptor(buf_id);

	buf_state = LockBufHdr(buf);
	if (!BufferTagsEqual(&buf->tag, &tag) || (buf_state & BM_VALID) == 0)
	{
		UnlockBufHdr(buf, buf_state);
		LWLockRelease(partition_lock);
		return CLUSTER_BUFMGR_GCS_DROP_NOT_RESIDENT;
	}

	/*
	 * PGRAC: GCS serve-stall round-5 (A2) — fast pin fail (see
	 * ClusterBufmgrGcsDropResult;  the X-transfer caller fail-closes with a
	 * retryable deny instead of parking InvalidateBuffer's pin-wait loop in
	 * the dispatch pump).
	 */
	if (BUF_STATE_GET_REFCOUNT(buf_state) != 0)
	{
		UnlockBufHdr(buf, buf_state);
		LWLockRelease(partition_lock);
		return CLUSTER_BUFMGR_GCS_DROP_PINNED;
	}
	was_dirty = (buf_state & BM_DIRTY) != 0;
	{
		Page		page = (Page) BufHdrGetBlock(buf);

		page_lsn = PageGetLSN(page);
	}

	/*
	 * PGRAC: GCS serve-stall round-6 — generation gate.  A local writer
	 * that held a cached X grant could commit to this page in the window
	 * between the transfer's ship-image copy and this drop (gaps (a) cached-X
	 * no-reverify + (b) copy->drop admission).  Its pin is already gone (the
	 * refcount check above passed), so PINNED cannot catch it; the page LSN,
	 * read here under the same header spinlock that the writer's unpin
	 * released, is the generation token.  A LSN past the caller's copy-time
	 * expected_lsn means the captured ship image is STALE: refuse the drop so
	 * the caller fail-closes with a retryable deny and the re-serve copies
	 * the current page (Rule 8.A — never grant a stale image over a
	 * committed write).  expected_lsn == Invalid skips the gate.
	 */
	if (!XLogRecPtrIsInvalid(expected_lsn) && page_lsn != expected_lsn)
	{
		UnlockBufHdr(buf, buf_state);
		LWLockRelease(partition_lock);
		return CLUSTER_BUFMGR_GCS_DROP_STALE;
	}

	UnlockBufHdr(buf, buf_state);
	LWLockRelease(partition_lock);

	if (out_page_lsn != NULL)
		*out_page_lsn = page_lsn;

	/*
	 * WAL-before-share: the image already shipped to the new holder carries
	 * page_lsn, so its WAL must be durable before that holder relies on it.
	 * Flush WAL only -- never the (now stale) data page (Rule 8.A
	 * no-stale-flush); InvalidateBuffer below discards the dirty buffer.
	 */
	if (was_dirty && !XLogRecPtrIsInvalid(page_lsn))
		XLogFlush(cluster_gcs_clamp_ship_flush_lsn(page_lsn));

	/*
	 * InvalidateBuffer requires the buffer header spinlock held at ENTRY and
	 * releases it internally (it Assert()s BM_LOCKED).  We dropped the header
	 * lock for the (blocking) XLogFlush above, so re-lock and re-validate the
	 * buffer still maps our tag; if it was evicted / reused meanwhile there
	 * is nothing to drop.  Clear the cache-residency mode under this lock so
	 * InvalidateBuffer's eviction hook sees PCM_STATE_N and skips the remote-
	 * master release wire (the §3.5 LMON context has no backend slot to send
	 * one -- BLOCKER A).
	 */
	buf_state = LockBufHdr(buf);
	if (!BufferTagsEqual(&buf->tag, &tag) || (buf_state & BM_VALID) == 0)
	{
		UnlockBufHdr(buf, buf_state);
		return CLUSTER_BUFMGR_GCS_DROP_NOT_RESIDENT;
	}

	/* PGRAC: GCS serve-stall round-5 (A2) — a pin acquired during the
	 * XLogFlush window re-fails the drop (nothing observed half-dropped). */
	if (BUF_STATE_GET_REFCOUNT(buf_state) != 0)
	{
		UnlockBufHdr(buf, buf_state);
		return CLUSTER_BUFMGR_GCS_DROP_PINNED;
	}

	/*
	 * PGRAC: GCS serve-stall round-6 — re-check the generation gate under
	 * this second header-lock hold: a local writer could have committed
	 * during the (blocking) XLogFlush window above.  A LSN past expected_lsn
	 * means the ship image is stale; refuse without touching pcm_state
	 * (Rule 8.A). */
	if (!XLogRecPtrIsInvalid(expected_lsn) &&
		PageGetLSN((Page) BufHdrGetBlock(buf)) != expected_lsn)
	{
		UnlockBufHdr(buf, buf_state);
		return CLUSTER_BUFMGR_GCS_DROP_STALE;
	}

	saved_pcm_state = buf->pcm_state;
	staged_gen = cluster_pcm_own_gen_get(buf->buf_id);	/* PGRAC W2 */
	buf->pcm_state = (uint8) PCM_STATE_N;

	/*
	 * PGRAC: spec-6.12h D-h1 — keep a Past Image instead of dropping (the
	 * shipped current went to the new holder; our copy becomes the PI).
	 */
	if (cluster_bufmgr_convert_to_pi_locked(buf, buf_state))
		return CLUSTER_BUFMGR_GCS_DROP_DROPPED;

	/* PGRAC: GCS serve-stall round-5 (A2) — bounded drop;  restore the
	 * residency mode on a raced pin (mirrors the invalidate wrapper). */
	cluster_bufmgr_in_gcs_drop = true;	/* gates the drop-prepin inject */
	if (!InvalidateBufferTry(buf))	/* releases the header spinlock */
	{
		cluster_bufmgr_in_gcs_drop = false;

		/*
		 * GCS serve-stall round-6 wave-2 — restore-ABA window.  The header
		 * spinlock was dropped for the InvalidateBufferTry attempt, so a full
		 * concurrent ownership round (N->X->N) can complete here, leaving
		 * pcm_state back at N with a NEW ownership generation.  This inject
		 * holds the window open so the RED can drive the N->X->N
		 * deterministically; the generation compare added by the ownership
		 * mechanism is what closes it.
		 */
		CLUSTER_INJECTION_POINT("cluster-pcm-restore-aba-window");

		/* W2 RED force-round — see the twin arm above. */
		CLUSTER_INJECTION_POINT("cluster-pcm-restore-aba-force-round");
		if (cluster_injection_should_skip("cluster-pcm-restore-aba-force-round"))
			cluster_pcm_own_transition(buf, (uint8) PCM_STATE_N, 0, 0);

		buf_state = LockBufHdr(buf);

		/*
		 * PGRAC ownership-generation wave (W2) — see the twin arm above:
		 * restore the staged N only when the ownership generation is
		 * unchanged, so an N->X->N ABA that re-owned and dropped the block in
		 * the InvalidateBuffer window does not get a stale pre-drop state
		 * restored over it.
		 */
		if (BufferTagsEqual(&buf->tag, &tag) &&
			buf->pcm_state == (uint8) PCM_STATE_N &&
			cluster_pcm_own_gen_get(buf->buf_id) == staged_gen)
			buf->pcm_state = saved_pcm_state;
		else if (BufferTagsEqual(&buf->tag, &tag) &&
				 buf->pcm_state == (uint8) PCM_STATE_N &&
				 cluster_pcm_own_gen_get(buf->buf_id) != staged_gen)
			cluster_pcm_note_restore_aba_detected();
		UnlockBufHdr(buf, buf_state);
		return CLUSTER_BUFMGR_GCS_DROP_PINNED;
	}
	cluster_bufmgr_in_gcs_drop = false;

	return CLUSTER_BUFMGR_GCS_DROP_DROPPED;
}

/* Abort only the matching requester-as-source N staging reservation. */
ClusterPcmOwnResult
cluster_bufmgr_pcm_own_abort_n_revoke(BufferDesc *buf,
									  const ClusterPcmOwnSnapshot *expected_revoking)
{
	ClusterPcmOwnResult live_result;
	ClusterPcmOwnResult result;
	uint32 buf_state;

	if (buf == NULL || expected_revoking == NULL)
		return CLUSTER_PCM_OWN_INVALID;
	if (expected_revoking->pcm_state != (uint8)PCM_STATE_N
		|| expected_revoking->flags != PCM_OWN_FLAG_REVOKING
		|| expected_revoking->reservation_token == 0)
		return CLUSTER_PCM_OWN_INVALID;
	if (ClusterPcmOwnArray == NULL)
		return CLUSTER_PCM_OWN_NOT_READY;

	buf_state = LockBufHdr(buf);
	if (!cluster_pcm_own_snapshot_matches_locked(buf, expected_revoking))
		result = CLUSTER_PCM_OWN_STALE;
	else {
		live_result = cluster_pcm_own_classify_live_flags(expected_revoking->flags,
														  expected_revoking->reservation_token);
		if (live_result == CLUSTER_PCM_OWN_CORRUPT)
			result = live_result;
		else
			result = cluster_pcm_own_reservation_abort_exact(
				buf->buf_id, expected_revoking->generation, expected_revoking->reservation_token,
				PCM_OWN_FLAG_REVOKING);
	}
	UnlockBufHdr(buf, buf_state);
	return result;
}

/*
 * Fence a clean N descriptor, refresh it from shared storage, and return one
 * immutable requester-as-source image.
 *
 * REVOKING binds descriptor identity before the physical read.  Storage is
 * read and verified into scratch without holding the content lock; content
 * EXCLUSIVE plus a second full tuple/header check then makes the resident
 * copy and caller copy derive from that exact scratch.  No READY state is
 * published here.  Ordinary failures remove only this exact reservation;
 * inability to do so is corruption and retains the evidence fail-closed.
 */
ClusterPcmOwnResult
cluster_bufmgr_pcm_own_prepare_n_source_image(BufferDesc *buf,
											  const ClusterPcmOwnSnapshot *expected_n,
											  ClusterPcmOwnSnapshot *out_revoking,
											  char block_data[BLCKSZ], XLogRecPtr *out_page_lsn,
											  uint64 *out_page_scn)
{
	PGIOAlignedBlock scratch;
	BufferTag tag;
	ClusterPcmOwnSnapshot live;
	ClusterPcmOwnResult abort_result;
	volatile ClusterPcmOwnResult result = CLUSTER_PCM_OWN_OK;
	SMgrRelation reln;
	LWLock *content_lock;
	uint64 reservation_token = 0;
	uint32 buf_state;
	volatile bool content_locked = false;

	if (out_revoking != NULL)
		memset(out_revoking, 0, sizeof(*out_revoking));
	if (block_data != NULL)
		memset(block_data, 0, BLCKSZ);
	if (out_page_lsn != NULL)
		*out_page_lsn = InvalidXLogRecPtr;
	if (out_page_scn != NULL)
		*out_page_scn = 0;
	if (buf == NULL || expected_n == NULL || out_revoking == NULL || block_data == NULL
		|| out_page_lsn == NULL || out_page_scn == NULL)
		return CLUSTER_PCM_OWN_INVALID;
	if (expected_n->pcm_state != (uint8)PCM_STATE_N || expected_n->flags != 0)
		return CLUSTER_PCM_OWN_INVALID;
	if (ClusterPcmOwnArray == NULL)
		return CLUSTER_PCM_OWN_NOT_READY;

	tag = expected_n->tag;
	/* The dirty-page branch below may pin under the header lock. */
	ReservePrivateRefCountEntry();
	ResourceOwnerEnlargeBuffers(CurrentResourceOwner);
	buf_state = LockBufHdr(buf);
	if (!cluster_pcm_own_snapshot_matches_locked(buf, expected_n) || (buf_state & BM_VALID) == 0)
		result = CLUSTER_PCM_OWN_STALE;
	else if ((buf_state & BM_IO_ERROR) != 0)
		result = CLUSTER_PCM_OWN_CORRUPT;
	else if ((buf_state & (BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED)) != 0)
	{
		/*
		 * PGRAC: a dirty N page here is legitimate, not corruption evidence:
		 * relation extension (PageInit + MarkBufferDirty) and recovery redo
		 * both dirty a page before any PCM grant exists, so the first
		 * cluster-aware writer meets its own pre-grant dirt.  The N-source
		 * contract serves STORAGE bytes and overwrites the resident copy, so
		 * consuming the page now would discard the newer local bytes (a lost
		 * write).  Push the local bytes out first (FlushBuffer is WAL-first)
		 * and report BUSY: one flush converges the state and the image pump
		 * retries against a clean page.
		 */
		PinBuffer_Locked(buf);	/* consumes the buffer header lock */
		LWLockAcquire(BufferDescriptorGetContentLock(buf), LW_SHARED);
		FlushBuffer(buf, NULL, IOOBJECT_RELATION, IOCONTEXT_NORMAL);
		LWLockRelease(BufferDescriptorGetContentLock(buf));
		UnpinBuffer(buf);
		return CLUSTER_PCM_OWN_BUSY;
	}
	else if ((buf_state & BM_IO_IN_PROGRESS) != 0)
		result = CLUSTER_PCM_OWN_BUSY;
	else {
		result = cluster_pcm_own_reservation_begin_exact(buf->buf_id, expected_n->generation,
														 PCM_OWN_FLAG_REVOKING, &reservation_token);
		if (result == CLUSTER_PCM_OWN_OK)
			cluster_pcm_own_snapshot_locked(buf, &live);
	}
	UnlockBufHdr(buf, buf_state);
	if (result != CLUSTER_PCM_OWN_OK)
		return result;

	content_lock = BufferDescriptorGetContentLock(buf);
	PG_TRY();
	{
		reln = smgropen(BufTagGetRelFileLocator(&tag), InvalidBackendId);
		smgrread(reln, BufTagGetForkNum(&tag), tag.blockNum, scratch.data);
		if (!PageIsVerifiedExtended((Page)scratch.data, tag.blockNum,
									PIV_LOG_WARNING | PIV_REPORT_STAT))
		{
			result = CLUSTER_PCM_OWN_CORRUPT;
		}

		if (result == CLUSTER_PCM_OWN_OK) {
			LWLockAcquire(content_lock, LW_EXCLUSIVE);
			content_locked = true;
			buf_state = LockBufHdr(buf);
			if (!cluster_pcm_own_snapshot_matches_locked(buf, &live)
				|| live.flags != PCM_OWN_FLAG_REVOKING || live.pcm_state != (uint8)PCM_STATE_N
				|| (buf_state & BM_VALID) == 0)
				result = CLUSTER_PCM_OWN_STALE;
			else if ((buf_state & BM_IO_ERROR) != 0)
				result = CLUSTER_PCM_OWN_CORRUPT;
			else if ((buf_state & (BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED)) != 0)
			{
				/* PGRAC: REVOKING already blocks data writes, so dirt appearing
				 * between the flush above and this recheck can only be an
				 * idempotent hint-bit write.  Retry via BUSY; the next pass
				 * flushes it and converges. */
				result = CLUSTER_PCM_OWN_BUSY;
			}
			else if ((buf_state & BM_IO_IN_PROGRESS) != 0)
				result = CLUSTER_PCM_OWN_BUSY;
			else {
				memcpy((char *)BufHdrGetBlock(buf), scratch.data, BLCKSZ);
				buf->buffer_type = (uint8)BUF_TYPE_CURRENT;
				memcpy(block_data, scratch.data, BLCKSZ);
				*out_page_lsn = PageGetLSN((Page)scratch.data);
				*out_page_scn = (uint64)((PageHeader)scratch.data)->pd_block_scn;
				*out_revoking = live;
			}
			UnlockBufHdr(buf, buf_state);
			LWLockRelease(content_lock);
			content_locked = false;
		}
	}
	PG_CATCH();
	{
		if (content_locked && LWLockHeldByMe(content_lock))
			LWLockRelease(content_lock);
		abort_result = cluster_bufmgr_pcm_own_abort_n_revoke(buf, &live);
		if (abort_result != CLUSTER_PCM_OWN_OK)
			elog(LOG,
				 "could not abort exact PCM-X N-source reservation after storage read error: "
				 "buffer=%d generation=%llu token=%llu result=%d; evidence retained",
				 buf->buf_id, (unsigned long long)live.generation,
				 (unsigned long long)live.reservation_token, (int)abort_result);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (result == CLUSTER_PCM_OWN_OK)
		return CLUSTER_PCM_OWN_OK;
	abort_result = cluster_bufmgr_pcm_own_abort_n_revoke(buf, &live);
	if (abort_result != CLUSTER_PCM_OWN_OK) {
		elog(LOG,
			 "could not abort exact PCM-X N-source reservation after refresh failure: "
			 "buffer=%d generation=%llu token=%llu result=%d; evidence retained",
			 buf->buf_id, (unsigned long long)live.generation,
			 (unsigned long long)live.reservation_token, (int)abort_result);
		return CLUSTER_PCM_OWN_CORRUPT;
	}
	memset(out_revoking, 0, sizeof(*out_revoking));
	memset(block_data, 0, BLCKSZ);
	*out_page_lsn = InvalidXLogRecPtr;
	*out_page_scn = 0;
	return result;
}

/*
 * Begin an exact source-holder S revoke for PCM-X image staging.
 *
 * The S source remains S while REVOKING is live.  The flag closes the local
 * cached-cover window without inventing an X grant.  Passive PG pins are not
 * ownership holders and therefore do not block this protocol reservation;
 * the final byte boundary is serialized by the content lock instead.
 */
ClusterPcmOwnResult
cluster_bufmgr_pcm_own_begin_s_revoke(BufferDesc *buf,
									 const ClusterPcmOwnSnapshot *expected_s,
									 ClusterPcmOwnSnapshot *out_revoking)
{
	ClusterPcmOwnResult live_result;
	ClusterPcmOwnResult result;
	uint64		live_token;
	uint64		reservation_token = 0;
	uint32		flags;
	uint32		buf_state;

	if (buf == NULL || expected_s == NULL || out_revoking == NULL)
		return CLUSTER_PCM_OWN_INVALID;
	memset(out_revoking, 0, sizeof(*out_revoking));
	if (expected_s->pcm_state != (uint8) PCM_STATE_S || expected_s->flags != 0)
		return CLUSTER_PCM_OWN_INVALID;
	if (ClusterPcmOwnArray == NULL)
		return CLUSTER_PCM_OWN_NOT_READY;

	buf_state = LockBufHdr(buf);
	if (!BufferTagsEqual(&buf->tag, &expected_s->tag)
		|| (buf_state & BM_VALID) == 0
		|| cluster_pcm_own_gen_get(buf->buf_id) != expected_s->generation
		|| buf->pcm_state != (uint8) PCM_STATE_S)
		result = CLUSTER_PCM_OWN_STALE;
	else if (!cluster_bufmgr_pcm_current_image_locked(buf, buf_state))
		result = CLUSTER_PCM_OWN_CORRUPT;
	else
	{
		live_token = cluster_pcm_own_reservation_token_get(buf->buf_id);
		flags = cluster_pcm_own_flags_get(buf->buf_id);
		live_result = cluster_pcm_own_classify_live_flags(flags, live_token);
		if (live_result != CLUSTER_PCM_OWN_OK)
			result = live_result;
		else if (live_token != expected_s->reservation_token)
			result = CLUSTER_PCM_OWN_STALE;
		else
		{
			result = cluster_pcm_own_reservation_begin_exact(
				buf->buf_id, expected_s->generation, PCM_OWN_FLAG_REVOKING,
				&reservation_token);
			if (result == CLUSTER_PCM_OWN_OK)
				cluster_pcm_own_snapshot_locked(buf, out_revoking);
		}
	}
	UnlockBufHdr(buf, buf_state);
	return result;
}

/* Abort only the matching S-source staging reservation. */
ClusterPcmOwnResult
cluster_bufmgr_pcm_own_abort_s_revoke(BufferDesc *buf,
									 const ClusterPcmOwnSnapshot *expected_revoking)
{
	ClusterPcmOwnResult live_result;
	ClusterPcmOwnResult result;
	uint64		live_token;
	uint32		flags;
	uint32		buf_state;

	if (buf == NULL || expected_revoking == NULL)
		return CLUSTER_PCM_OWN_INVALID;
	if (expected_revoking->pcm_state != (uint8) PCM_STATE_S
		|| expected_revoking->flags != PCM_OWN_FLAG_REVOKING
		|| expected_revoking->reservation_token == 0)
		return CLUSTER_PCM_OWN_INVALID;
	if (ClusterPcmOwnArray == NULL)
		return CLUSTER_PCM_OWN_NOT_READY;

	buf_state = LockBufHdr(buf);
	if (!BufferTagsEqual(&buf->tag, &expected_revoking->tag)
		|| (buf_state & BM_VALID) == 0
		|| cluster_pcm_own_gen_get(buf->buf_id) != expected_revoking->generation
		|| buf->pcm_state != (uint8) PCM_STATE_S)
		result = CLUSTER_PCM_OWN_STALE;
	else if (!cluster_bufmgr_pcm_current_image_locked(buf, buf_state))
		result = CLUSTER_PCM_OWN_CORRUPT;
	else
	{
		live_token = cluster_pcm_own_reservation_token_get(buf->buf_id);
		flags = cluster_pcm_own_flags_get(buf->buf_id);
		live_result = cluster_pcm_own_classify_live_flags(flags, live_token);
		if (live_result == CLUSTER_PCM_OWN_CORRUPT)
			result = live_result;
		else if (flags == 0)
			result = CLUSTER_PCM_OWN_STALE;
		else if (flags != PCM_OWN_FLAG_REVOKING)
			result = CLUSTER_PCM_OWN_BUSY;
		else if (live_token != expected_revoking->reservation_token)
			result = CLUSTER_PCM_OWN_STALE;
		else
			result = cluster_pcm_own_reservation_abort_exact(
				buf->buf_id, expected_revoking->generation,
				expected_revoking->reservation_token, PCM_OWN_FLAG_REVOKING);
	}
	UnlockBufHdr(buf, buf_state);
	return result;
}

/*
 * Begin an exact source-holder X revoke.
 *
 * This is deliberately an opaque BufferDesc operation.  The returned
 * snapshot is the complete live REVOKING identity consumed by byte staging,
 * abort, and retained finish; callers never inspect the ownership sidecar.
 */
ClusterPcmOwnResult
cluster_bufmgr_pcm_own_begin_x_revoke(BufferDesc *buf,
									 const ClusterPcmOwnSnapshot *expected_x,
									 ClusterPcmOwnSnapshot *out_revoking)
{
	ClusterPcmOwnResult live_result;
	ClusterPcmOwnResult result;
	uint64		live_token;
	uint64		reservation_token = 0;
	uint32		flags;
	uint32		buf_state;

	if (buf == NULL || expected_x == NULL || out_revoking == NULL)
		return CLUSTER_PCM_OWN_INVALID;
	memset(out_revoking, 0, sizeof(*out_revoking));
	if (expected_x->pcm_state != (uint8) PCM_STATE_X || expected_x->flags != 0)
		return CLUSTER_PCM_OWN_INVALID;
	if (ClusterPcmOwnArray == NULL)
		return CLUSTER_PCM_OWN_NOT_READY;

	buf_state = LockBufHdr(buf);
	if (!BufferTagsEqual(&buf->tag, &expected_x->tag)
		|| (buf_state & BM_VALID) == 0
		|| cluster_pcm_own_gen_get(buf->buf_id) != expected_x->generation
		|| buf->pcm_state != (uint8) PCM_STATE_X)
		result = CLUSTER_PCM_OWN_STALE;
	else if (!cluster_bufmgr_pcm_current_image_locked(buf, buf_state))
		result = CLUSTER_PCM_OWN_CORRUPT;
	else
	{
		live_token = cluster_pcm_own_reservation_token_get(buf->buf_id);
		flags = cluster_pcm_own_flags_get(buf->buf_id);
		live_result = cluster_pcm_own_classify_live_flags(flags, live_token);
		if (live_result != CLUSTER_PCM_OWN_OK)
			result = live_result;
		else if (live_token != expected_x->reservation_token)
			result = CLUSTER_PCM_OWN_STALE;
		else
		{
			result = cluster_pcm_own_reservation_begin_exact(
				buf->buf_id, expected_x->generation, PCM_OWN_FLAG_REVOKING,
				&reservation_token);
			if (result == CLUSTER_PCM_OWN_OK)
				cluster_pcm_own_snapshot_locked(buf, out_revoking);
		}
	}
	UnlockBufHdr(buf, buf_state);
	return result;
}

/*
 * Abort one exact source-holder revoke before any ownership transfer.
 */
ClusterPcmOwnResult
cluster_bufmgr_pcm_own_abort_x_revoke(BufferDesc *buf,
									 const ClusterPcmOwnSnapshot *expected_revoking)
{
	ClusterPcmOwnResult live_result;
	ClusterPcmOwnResult result;
	uint64		live_token;
	uint32		flags;
	uint32		buf_state;

	if (buf == NULL || expected_revoking == NULL)
		return CLUSTER_PCM_OWN_INVALID;
	if (expected_revoking->pcm_state != (uint8) PCM_STATE_X
		|| expected_revoking->flags != PCM_OWN_FLAG_REVOKING
		|| expected_revoking->reservation_token == 0)
		return CLUSTER_PCM_OWN_INVALID;
	if (ClusterPcmOwnArray == NULL)
		return CLUSTER_PCM_OWN_NOT_READY;

	buf_state = LockBufHdr(buf);
	if (!BufferTagsEqual(&buf->tag, &expected_revoking->tag)
		|| (buf_state & BM_VALID) == 0
		|| cluster_pcm_own_gen_get(buf->buf_id) != expected_revoking->generation
		|| buf->pcm_state != (uint8) PCM_STATE_X)
		result = CLUSTER_PCM_OWN_STALE;
	else if (!cluster_bufmgr_pcm_current_image_locked(buf, buf_state))
		result = CLUSTER_PCM_OWN_CORRUPT;
	else
	{
		live_token = cluster_pcm_own_reservation_token_get(buf->buf_id);
		flags = cluster_pcm_own_flags_get(buf->buf_id);
		live_result = cluster_pcm_own_classify_live_flags(flags, live_token);
		if (live_result == CLUSTER_PCM_OWN_CORRUPT)
			result = live_result;
		else if (flags == 0)
			result = CLUSTER_PCM_OWN_STALE;
		else if (flags != PCM_OWN_FLAG_REVOKING)
			result = CLUSTER_PCM_OWN_BUSY;
		else if (live_token != expected_revoking->reservation_token)
			result = CLUSTER_PCM_OWN_STALE;
		else
			result = cluster_pcm_own_reservation_abort_exact(
				buf->buf_id, expected_revoking->generation,
				expected_revoking->reservation_token, PCM_OWN_FLAG_REVOKING);
	}
	UnlockBufHdr(buf, buf_state);
	return result;
}

/* Commit a staged VM/FSM source without leaving a BM_VALID mapping behind.
 * The exclusive mapping lock closes the lookup-to-pin window around the
 * zero-refcount proof.  The immutable A-record already owns the copied bytes,
 * so the ordinary revoke commit may clear REVOKING and the descriptor may be
 * removed with the same generation bump as its reuse linearization. */
static ClusterPcmOwnResult
cluster_bufmgr_pcm_own_finish_revoke_drop_unpinned(BufferDesc *buf,
												   const ClusterPcmOwnSnapshot *expected_revoking,
												   XLogRecPtr expected_lsn,
												   ClusterPcmOwnSnapshot *out_finished)
{
	ClusterPcmXRevokeFinishMode finish_mode;
	ClusterPcmOwnResult live_result;
	ClusterPcmOwnResult result = CLUSTER_PCM_OWN_OK;
	BufferTag tag = expected_revoking->tag;
	LWLock *partition_lock;
	uint64 committed_generation = 0;
	uint64 live_token;
	uint32 hashcode;
	uint32 flags;
	uint32 buf_state;
	int mapped_buf_id;

	hashcode = BufTableHashCode(&tag);
	partition_lock = BufMappingPartitionLock(hashcode);
	LWLockAcquire(partition_lock, LW_EXCLUSIVE);
	mapped_buf_id = BufTableLookup(&tag, hashcode);
	if (mapped_buf_id != buf->buf_id) {
		LWLockRelease(partition_lock);
		return CLUSTER_PCM_OWN_STALE;
	}

	buf_state = LockBufHdr(buf);
	finish_mode = cluster_pcm_x_revoke_finish_mode(&tag, BUF_STATE_GET_REFCOUNT(buf_state));
	if (!BufferTagsEqual(&buf->tag, &tag) || (buf_state & BM_VALID) == 0
		|| cluster_pcm_own_gen_get(buf->buf_id) != expected_revoking->generation
		|| buf->pcm_state != expected_revoking->pcm_state)
		result = CLUSTER_PCM_OWN_STALE;
	else if (!cluster_bufmgr_pcm_current_image_locked(buf, buf_state))
		result = CLUSTER_PCM_OWN_CORRUPT;
	else if (finish_mode == CLUSTER_PCM_X_REVOKE_FINISH_BUSY)
		result = CLUSTER_PCM_OWN_BUSY;
	else if (finish_mode != CLUSTER_PCM_X_REVOKE_FINISH_DROP)
		result = CLUSTER_PCM_OWN_CORRUPT;
	else if ((buf_state & BM_IO_IN_PROGRESS) != 0)
		result = CLUSTER_PCM_OWN_BUSY;
	else {
		live_token = cluster_pcm_own_reservation_token_get(buf->buf_id);
		flags = cluster_pcm_own_flags_get(buf->buf_id);
		live_result = cluster_pcm_own_classify_live_flags(flags, live_token);
		if (live_result == CLUSTER_PCM_OWN_CORRUPT)
			result = live_result;
		else if (flags == 0)
			result = CLUSTER_PCM_OWN_STALE;
		else if (flags != PCM_OWN_FLAG_REVOKING)
			result = CLUSTER_PCM_OWN_BUSY;
		else if (live_token != expected_revoking->reservation_token)
			result = CLUSTER_PCM_OWN_STALE;
		else if (PageGetLSN((Page)BufHdrGetBlock(buf)) != expected_lsn)
			result = CLUSTER_PCM_OWN_STALE;
	}

	if (result == CLUSTER_PCM_OWN_OK)
		result = cluster_pcm_own_revoke_commit_exact(buf->buf_id, expected_revoking->generation,
													 expected_revoking->reservation_token,
													 &committed_generation);
	if (result != CLUSTER_PCM_OWN_OK) {
		UnlockBufHdr(buf, buf_state);
		LWLockRelease(partition_lock);
		return result;
	}

	Assert(expected_revoking->generation != UINT64_MAX);
	Assert(committed_generation == expected_revoking->generation + 1);
	buf->pcm_state = (uint8)PCM_STATE_N;
	buf->buffer_type = (uint8)BUF_TYPE_CURRENT;
	cluster_pcm_own_snapshot_locked(buf, out_finished);
	InvalidateBufferCommitTailLocked(buf, &tag, hashcode, partition_lock, buf_state,
									 (uint8)PCM_STATE_N, false);
	return CLUSTER_PCM_OWN_OK;
}

/*
 * Commit one staged S/X source as a retained image, except for VM/FSM.
 *
 * REVOKING already binds the descriptor to its tag through D5a and the victim
 * guard, so this path deliberately does not nest a mapping lock with the
 * content lock.  Content EXCLUSIVE is the byte/I/O linearization: a flush
 * that already owns SHARE completes first; once this lock is acquired, no
 * writer or output I/O can overlap the exact LSN check and transition.
 * Passive pins remain untouched.  The descriptor keeps tag, BM_VALID,
 * refcount, and bytes, while PI+N marks those bytes non-current and the exact
 * REVOKING token prevents reuse until DRAIN.
 */
ClusterPcmOwnResult
cluster_bufmgr_pcm_own_finish_revoke_retain(
	BufferDesc *buf, const ClusterPcmOwnSnapshot *expected_revoking,
	XLogRecPtr expected_lsn, ClusterPcmOwnSnapshot *out_retained)
{
	ClusterPcmOwnResult live_result;
	ClusterPcmOwnResult result = CLUSTER_PCM_OWN_OK;
	BufferTag	tag;
	LWLock	   *content_lock;
	uint64		committed_generation;
	uint64		live_token;
	uint32		flags;
	uint32		buf_state;
	bool		source_is_s;
	bool		source_is_x;
	ClusterPcmXRevokeFinishMode finish_mode;

	if (out_retained != NULL)
		memset(out_retained, 0, sizeof(*out_retained));
	if (buf == NULL || expected_revoking == NULL || out_retained == NULL)
		return CLUSTER_PCM_OWN_INVALID;
	source_is_s = expected_revoking->pcm_state == (uint8) PCM_STATE_S;
	source_is_x = expected_revoking->pcm_state == (uint8) PCM_STATE_X;
	if ((!source_is_s && !source_is_x)
		|| expected_revoking->flags != PCM_OWN_FLAG_REVOKING
		|| expected_revoking->reservation_token == 0)
		return CLUSTER_PCM_OWN_INVALID;
	if (ClusterPcmOwnArray == NULL)
		return CLUSTER_PCM_OWN_NOT_READY;

	tag = expected_revoking->tag;
	finish_mode = cluster_pcm_x_revoke_finish_mode(&tag, 0);
	if (finish_mode == CLUSTER_PCM_X_REVOKE_FINISH_INVALID)
		return CLUSTER_PCM_OWN_INVALID;
	if (finish_mode == CLUSTER_PCM_X_REVOKE_FINISH_DROP)
		return cluster_bufmgr_pcm_own_finish_revoke_drop_unpinned(buf, expected_revoking,
																  expected_lsn, out_retained);
	Assert(finish_mode == CLUSTER_PCM_X_REVOKE_FINISH_RETAIN);
	content_lock = BufferDescriptorGetContentLock(buf);
	LWLockAcquire(content_lock, LW_EXCLUSIVE);

	buf_state = LockBufHdr(buf);
	if (!BufferTagsEqual(&buf->tag, &tag)
		|| (buf_state & BM_VALID) == 0
		|| cluster_pcm_own_gen_get(buf->buf_id) != expected_revoking->generation
		|| buf->pcm_state != expected_revoking->pcm_state)
		result = CLUSTER_PCM_OWN_STALE;
	else if (!cluster_bufmgr_pcm_current_image_locked(buf, buf_state))
		result = CLUSTER_PCM_OWN_CORRUPT;
	else if ((buf_state & BM_IO_IN_PROGRESS) != 0)
		result = CLUSTER_PCM_OWN_BUSY;
	else
	{
		live_token = cluster_pcm_own_reservation_token_get(buf->buf_id);
		flags = cluster_pcm_own_flags_get(buf->buf_id);
		live_result = cluster_pcm_own_classify_live_flags(flags, live_token);
		if (live_result == CLUSTER_PCM_OWN_CORRUPT)
			result = live_result;
		else if (flags == 0)
			result = CLUSTER_PCM_OWN_STALE;
		else if (flags != PCM_OWN_FLAG_REVOKING)
			result = CLUSTER_PCM_OWN_BUSY;
		else if (live_token != expected_revoking->reservation_token)
			result = CLUSTER_PCM_OWN_STALE;
		else if (PageGetLSN((Page) BufHdrGetBlock(buf)) != expected_lsn)
			result = CLUSTER_PCM_OWN_STALE;
	}

	if (result == CLUSTER_PCM_OWN_OK)
	{
		result = cluster_pcm_own_revoke_retain_commit_exact(
			buf->buf_id, expected_revoking->generation,
			expected_revoking->reservation_token, &committed_generation);
		if (result == CLUSTER_PCM_OWN_OK)
		{
			buf->pcm_state = (uint8) PCM_STATE_N;
			buf->buffer_type = (uint8) BUF_TYPE_PI;
			buf_state &= ~(BM_DIRTY | BM_JUST_DIRTIED |
						   BM_CHECKPOINT_NEEDED | BM_IO_ERROR);
			cluster_pcm_own_snapshot_locked(buf, out_retained);
		}
	}

	UnlockBufHdr(buf, buf_state);
	LWLockRelease(content_lock);
	return result;
}

/* Release only the retained descriptor created from source_generation.  The
 * generation+1 and current token bind a DRAIN to one image round; a delayed
 * DRAIN cannot clear REVOKING on a later transfer. */
ClusterPcmOwnResult
cluster_bufmgr_pcm_own_release_retained_image(const BufferTag *tag,
											  uint64 source_generation)
{
	ClusterPcmOwnResult live_result;
	ClusterPcmOwnResult result = CLUSTER_PCM_OWN_OK;
	BufferDesc *buf;
	BufferTag	lookup_tag;
	LWLock	   *content_lock;
	LWLock	   *partition_lock;
	uint64		committed_generation;
	uint64		live_token;
	uint64		retained_token = 0;
	uint32		hashcode;
	uint32		flags;
	uint32		buf_state;
	int			buf_id;

	if (tag == NULL || source_generation == UINT64_MAX)
		return CLUSTER_PCM_OWN_INVALID;
	if (ClusterPcmOwnArray == NULL)
		return CLUSTER_PCM_OWN_NOT_READY;
	committed_generation = source_generation + 1;
	lookup_tag = *tag;
	hashcode = BufTableHashCode(&lookup_tag);
	partition_lock = BufMappingPartitionLock(hashcode);
	LWLockAcquire(partition_lock, LW_SHARED);
	buf_id = BufTableLookup(&lookup_tag, hashcode);
	if (buf_id < 0)
	{
		LWLockRelease(partition_lock);
		return CLUSTER_PCM_OWN_STALE;
	}
	buf = GetBufferDescriptor(buf_id);

	/*
	 * Phase 1 binds tag -> descriptor under the mapping lock and proves the
	 * live retention token before dropping mapping authority.  REVOKING then
	 * prevents every reuse path until phase 2 revalidates under content
	 * EXCLUSIVE.  Never hold mapping and content together (avoids ABBA with
	 * ordinary content-lock callers).
	 */
	buf_state = LockBufHdr(buf);
	if (!BufferTagsEqual(&buf->tag, tag)
		|| (buf_state & BM_VALID) == 0
		|| buf->buffer_type != (uint8) BUF_TYPE_PI
		|| buf->pcm_state != (uint8) PCM_STATE_N
		|| cluster_pcm_own_gen_get(buf->buf_id) != committed_generation)
		result = CLUSTER_PCM_OWN_STALE;
	else
	{
		live_token = cluster_pcm_own_reservation_token_get(buf->buf_id);
		flags = cluster_pcm_own_flags_get(buf->buf_id);
		live_result = cluster_pcm_own_classify_live_flags(flags, live_token);
		if (live_result == CLUSTER_PCM_OWN_CORRUPT)
			result = live_result;
		else if (flags == 0)
			result = CLUSTER_PCM_OWN_STALE;
		else if (flags != PCM_OWN_FLAG_REVOKING)
			result = CLUSTER_PCM_OWN_BUSY;
		else
			retained_token = live_token;
	}
	UnlockBufHdr(buf, buf_state);
	LWLockRelease(partition_lock);
	if (result != CLUSTER_PCM_OWN_OK)
		return result;

	content_lock = BufferDescriptorGetContentLock(buf);
	LWLockAcquire(content_lock, LW_EXCLUSIVE);

	buf_state = LockBufHdr(buf);
	if (!BufferTagsEqual(&buf->tag, tag)
		|| (buf_state & BM_VALID) == 0
		|| buf->buffer_type != (uint8) BUF_TYPE_PI
		|| buf->pcm_state != (uint8) PCM_STATE_N
		|| cluster_pcm_own_gen_get(buf->buf_id) != committed_generation)
		result = CLUSTER_PCM_OWN_STALE;
	else if ((buf_state
			  & (BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED | BM_IO_ERROR)) != 0)
		result = CLUSTER_PCM_OWN_CORRUPT;
	else
	{
		live_token = cluster_pcm_own_reservation_token_get(buf->buf_id);
		flags = cluster_pcm_own_flags_get(buf->buf_id);
		live_result = cluster_pcm_own_classify_live_flags(flags, live_token);
		if (live_result == CLUSTER_PCM_OWN_CORRUPT)
			result = live_result;
		else if (flags == 0)
			result = CLUSTER_PCM_OWN_STALE;
		else if (flags != PCM_OWN_FLAG_REVOKING)
			result = CLUSTER_PCM_OWN_BUSY;
		else if (live_token != retained_token)
			result = CLUSTER_PCM_OWN_STALE;
		else {
			result = cluster_pcm_own_revoke_retain_release_exact(
				buf->buf_id, committed_generation, retained_token);
			if (result == CLUSTER_PCM_OWN_OK) {
				/*
				 * The exact token is the last stale-write fence.  Release it
				 * before making the retained PI reloadable, while content
				 * EXCLUSIVE and the header lock keep page bytes invalid.
				 * Keep the mapping so any pre-existing pin can leave
				 * normally; the next ordinary read reloads the current page
				 * image.
				 */
				buf_state &= ~BM_VALID;
				buf->buffer_type = (uint8)BUF_TYPE_CURRENT;
			}
		}
	}

	UnlockBufHdr(buf, buf_state);
	LWLockRelease(content_lock);
	return result;
}

/* Prove that a sole-requester source image was adopted by the exact S->X
 * handoff.  This is intentionally read-only: a delayed DRAIN may release its
 * immutable A-record, but it must never mutate the current X descriptor. */
ClusterPcmOwnResult
cluster_bufmgr_pcm_own_self_handoff_x_exact(const BufferTag *tag, uint64 source_generation)
{
	ClusterPcmOwnResult live_result;
	ClusterPcmOwnResult result = CLUSTER_PCM_OWN_OK;
	BufferDesc *buf;
	BufferTag lookup_tag;
	LWLock *partition_lock;
	uint64 live_token;
	uint32 flags;
	uint32 hashcode;
	uint32 buf_state;
	int buf_id;

	if (tag == NULL || source_generation == UINT64_MAX)
		return CLUSTER_PCM_OWN_INVALID;
	if (ClusterPcmOwnArray == NULL)
		return CLUSTER_PCM_OWN_NOT_READY;
	lookup_tag = *tag;
	hashcode = BufTableHashCode(&lookup_tag);
	partition_lock = BufMappingPartitionLock(hashcode);
	LWLockAcquire(partition_lock, LW_SHARED);
	buf_id = BufTableLookup(&lookup_tag, hashcode);
	if (buf_id < 0) {
		LWLockRelease(partition_lock);
		return CLUSTER_PCM_OWN_STALE;
	}
	buf = GetBufferDescriptor(buf_id);
	buf_state = LockBufHdr(buf);
	live_token = cluster_pcm_own_reservation_token_get(buf->buf_id);
	flags = cluster_pcm_own_flags_get(buf->buf_id);
	live_result = cluster_pcm_own_classify_live_flags(flags, live_token);
	if (!BufferTagsEqual(&buf->tag, tag)
		|| cluster_pcm_own_gen_get(buf->buf_id) != source_generation + 1)
		result = CLUSTER_PCM_OWN_STALE;
	else if (live_result == CLUSTER_PCM_OWN_CORRUPT)
		result = live_result;
	else if (flags != 0)
		result = CLUSTER_PCM_OWN_BUSY;
	else if (live_token == 0 || (buf_state & BM_VALID) == 0 || buf->pcm_state != (uint8)PCM_STATE_X
			 || buf->buffer_type != (uint8)BUF_TYPE_XCUR)
		result = CLUSTER_PCM_OWN_STALE;
	UnlockBufHdr(buf, buf_state);
	LWLockRelease(partition_lock);
	return result;
}

/* ========================================================================
 * PGRAC MODIFICATIONS by SqlRush — spec-6.12h D-h2 (PI discard helpers).
 *
 *   cluster_bufmgr_block_is_pi(tag) — read-only probe: does this tag map to
 *   a D-h1 Past Image buffer (buffer_type BUF_TYPE_PI)?  The conversion
 *   sites call it right after the drop helpers above to learn whether the
 *   drop actually kept a PI (the helpers deliberately return the same true
 *   for "dropped" and "converted", keeping their ㉕-era ABI), and report
 *   kept-PI to the master's pi_holders_bitmap.
 *
 *   cluster_bufmgr_discard_pi_block(tag) — the PI_DISCARD consumer: drop
 *   the tag's buffer iff it is strictly a real unpinned Past Image
 *   (buffer_type PI + !BM_VALID + refcount 0).  A current copy (BM_VALID
 *   or non-PI buffer_type) is NEVER touched — the master's bitmap may
 *   over-approximate (legacy HC58 downgrade bits cover live-S holders), so
 *   the strictness lives HERE.  A pinned PI is a racing re-reader already
 *   installing over the bytes (the implicit discard) — skip it.  The
 *   partition lock is released before InvalidateBuffer (it re-acquires the
 *   partition lock itself; holding it across the call would self-deadlock);
 *   the header-lock tag re-check covers the unlocked window.
 * ======================================================================== */
bool
cluster_bufmgr_block_is_pi(BufferTag tag)
{
	uint32		hash = BufTableHashCode(&tag);
	LWLock	   *partition_lock = BufMappingPartitionLock(hash);
	int			buf_id;
	BufferDesc *buf;
	uint32		buf_state;
	bool		is_pi;

	LWLockAcquire(partition_lock, LW_SHARED);
	buf_id = BufTableLookup(&tag, hash);
	if (buf_id < 0)
	{
		LWLockRelease(partition_lock);
		return false;
	}
	buf = GetBufferDescriptor(buf_id);
	buf_state = LockBufHdr(buf);
	is_pi = BufferTagsEqual(&buf->tag, &tag)
		&& buf->buffer_type == (uint8) BUF_TYPE_PI
		&& (buf_state & BM_VALID) == 0;
	UnlockBufHdr(buf, buf_state);
	LWLockRelease(partition_lock);

	return is_pi;
}

bool
cluster_bufmgr_discard_pi_block(BufferTag tag)
{
	uint32		hash = BufTableHashCode(&tag);
	LWLock	   *partition_lock = BufMappingPartitionLock(hash);
	int			buf_id;
	BufferDesc *buf;
	uint32		buf_state;

	LWLockAcquire(partition_lock, LW_SHARED);
	buf_id = BufTableLookup(&tag, hash);
	LWLockRelease(partition_lock);
	if (buf_id < 0)
		return false;

	buf = GetBufferDescriptor(buf_id);
	buf_state = LockBufHdr(buf);
	if (!BufferTagsEqual(&buf->tag, &tag)
		|| buf->buffer_type != (uint8) BUF_TYPE_PI
		|| (buf_state & BM_VALID) != 0
		|| BUF_STATE_GET_REFCOUNT(buf_state) != 0)
	{
		UnlockBufHdr(buf, buf_state);
		return false;
	}

	/*
	 * A PI carries no residency claim (D-h1 set pcm_state N at conversion),
	 * but clear it again under the lock so InvalidateBuffer's eviction hook
	 * can never see a stale mode and emit a release wire from LMON. */
	buf->pcm_state = (uint8) PCM_STATE_N;
	/* PGRAC: spec-6.12h D-h3a — hygiene: drop the shadow stamp with the PI.
	 * Correctness never depends on this clear (the D-h3 consumer
	 * re-validates the PI shape + tag under this same header lock, and
	 * InvalidateBuffer below breaks the shape), but a directive-discarded
	 * slot should not linger as a plausible-looking stamp. */
	cluster_pi_shadow_clear(buf->buf_id);
	InvalidateBuffer(buf);		/* releases the header spinlock */

	return true;
}

/* ========================================================================
 * PGRAC MODIFICATIONS by SqlRush — spec-6.12h D-h3b (PI snapshot for the
 * detached recovery rebuild).
 *
 *   cluster_bufmgr_snapshot_pi_block(tag, dst, out_ship_scn) — copy a Past
 *   Image's bytes + its D-h3a ship-SCN stamp out of the buffer pool.  The
 *   D-h3 rebuild never mutates the resident PI; it replays the dead
 *   thread's records onto this private copy, so a retry simply re-snapshots
 *   (idempotence without a page-version gate).
 *
 *   Copy safety without a content lock: a !BM_VALID buffer's bytes are only
 *   ever written inside the StartBufferIO..TerminateBufferIO window, and
 *   the D-h3a seam resets buffer_type OUT of BUF_TYPE_PI under the header
 *   lock BEFORE that window opens.  So: validate the PI shape + read the
 *   stamp under the header lock, raw-pin (keeps buf_id's identity: discard
 *   and victim reuse both need refcount 0), copy unlocked, then re-lock and
 *   recheck shape + tag + stamp — if the recheck still sees the stamped PI,
 *   no overwrite can have STARTED during the copy, hence the copied bytes
 *   are exactly the conversion-frozen image.  Any recheck failure returns
 *   false (fail-safe: the caller falls back to storage + full redo).
 * ======================================================================== */
bool
cluster_bufmgr_snapshot_pi_block(BufferTag tag, char *dst, SCN *out_ship_scn)
{
	uint32		hash = BufTableHashCode(&tag);
	LWLock	   *partition_lock = BufMappingPartitionLock(hash);
	int			buf_id;
	BufferDesc *buf;
	uint32		buf_state;
	SCN			ship_scn;
	bool		intact;

	*out_ship_scn = InvalidScn;

	LWLockAcquire(partition_lock, LW_SHARED);
	buf_id = BufTableLookup(&tag, hash);
	if (buf_id < 0)
	{
		LWLockRelease(partition_lock);
		return false;
	}
	buf = GetBufferDescriptor(buf_id);

	buf_state = LockBufHdr(buf);
	if (!BufferTagsEqual(&buf->tag, &tag)
		|| buf->buffer_type != (uint8) BUF_TYPE_PI
		|| (buf_state & BM_VALID) != 0
		|| (buf_state & BM_TAG_VALID) == 0
		|| (buf_state & BM_IO_IN_PROGRESS) != 0)
	{
		UnlockBufHdr(buf, buf_state);
		LWLockRelease(partition_lock);
		return false;
	}
	ship_scn = cluster_pi_shadow_read(buf->buf_id);
	if (!SCN_VALID(ship_scn))
	{
		/* Unstamped PI (clock unarmed at conversion): the recovery boundary
		 * is unprovable, so the PI is useless as a base (gate UNUSABLE). */
		UnlockBufHdr(buf, buf_state);
		LWLockRelease(partition_lock);
		return false;
	}
	cluster_bufmgr_pin_for_gcs_locked(buf, buf_state);	/* raw-pin + unlock header */
	LWLockRelease(partition_lock);

	memcpy(dst, (char *) BufHdrGetBlock(buf), BLCKSZ);

	buf_state = LockBufHdr(buf);
	/* SCN_CMP_OK: identity recheck of the same slot (same-stamp equality),
	 * not a Lamport-order comparison. */
	intact = BufferTagsEqual(&buf->tag, &tag)
		&& buf->buffer_type == (uint8) BUF_TYPE_PI
		&& (buf_state & BM_VALID) == 0
		&& cluster_pi_shadow_read(buf->buf_id) == ship_scn;
	UnlockBufHdr(buf, buf_state);
	cluster_bufmgr_unpin_for_gcs(buf);

	if (!intact)
		return false;

	*out_ship_scn = ship_scn;
	return true;
}

/* ========================================================================
 * PGRAC MODIFICATIONS by SqlRush — spec-5.2a D4 (backend eager flush).
 *
 *   cluster_bufmgr_flush_seq_page_to_storage(buffer)
 *   — flush a cluster sequence page to shared storage from the BACKEND that
 *   just wrote it.  Caller holds a pin and the buffer content lock (any mode;
 *   nextval/setval/init hold EXCLUSIVE).  FlushOneBuffer -> FlushBuffer does
 *   XLogFlush(page_lsn) (WAL-before-data) + smgrwrite to shared storage, which
 *   is safe HERE (unlike the LMON drop path) because the backend's own WAL
 *   insert is complete and flushable.  After this the page is clean and
 *   storage-current, so the cross-node clean X-transfer (which drops via the
 *   spec-5.2 D11 drop_no_wire path in LMON) and the stale-holder
 *   storage-fallback both read the current value.  Fails closed (ereport) on a
 *   write/flush error via the underlying smgr path.
 * ======================================================================== */
void
cluster_bufmgr_flush_seq_page_to_storage(Buffer buffer)
{
	Assert(BufferIsValid(buffer));
	if (BufferIsLocal(buffer))
		return;					/* temp/local buffers are never CF-shared */
	FlushOneBuffer(buffer);
}

/* ========================================================================
 * PGRAC MODIFICATIONS by SqlRush — spec-5.13 D5b (clean-leave GCS flush seam).
 *
 *   cluster_bufmgr_flush_and_release_x_for_leave(void) -> uint32
 *   — a leaving node, while still alive, force-persists every dirty block it
 *   holds X on to shared storage and then releases that X (cache-residency
 *   pcm_state X -> N).  After this returns the leaving node holds NO in-memory
 *   current for any block: shared storage is the sole authority, so a survivor
 *   can read the current image from storage once it has invalidated its stale
 *   cache (CL-I5 / §0.3 命门 — the only sound way to bypass the Stage-6 cross-
 *   instance cache-coherence wall, because no in-memory current remains
 *   anywhere).
 *
 *   FlushBuffer is a bufmgr private static (cluster code cannot call it), so
 *   this seam MUST live in bufmgr.c.  It is the persist-and-confirm sibling of
 *   cluster_bufmgr_copy_block_for_gcs (which is copy-only, never persists).
 *
 *   Pin discipline mirrors FlushDatabaseBuffers / FlushRelationBuffers exactly
 *   (CL-I9 — runs in the leaving node's own backend/checkpointer with a valid
 *   CurrentResourceOwner, NEVER in the LMON aux process, where XLogFlush /
 *   FlushBuffer are not legal):
 *     - ResourceOwnerEnlargeBuffers once before the loop;
 *     - per buffer: ReservePrivateRefCountEntry + LockBufHdr; select
 *       VALID & DIRTY & pcm_state == PCM_STATE_X.  No unlocked precheck — the
 *       X residency bit is mutable; the leaving node is quiesced so no
 *       concurrent X-acquire races, but we lock every header for an 8.A-safe
 *       read regardless;
 *     - PinBuffer_Locked (releases the header spinlock) -> content_lock SHARED
 *       -> FlushBuffer (does its own XLogFlush(page_lsn) for BM_PERMANENT +
 *       smgrwrite to shared storage) -> content_lock release;
 *     - release X ONLY after the flush succeeds: re-LockBufHdr (the pin keeps
 *       the descriptor valid across the brief unlocked window, so no tag re-
 *       validate is needed — a pinned buffer is never reused), set pcm_state =
 *       PCM_STATE_N, UnlockBufHdr, then UnpinBuffer.
 *
 *   Fail-closed (Rule 8.A / 8.B): a write/flush error makes FlushBuffer
 *   ereport(ERROR); the ResourceOwner unwinds the pin + content lock and
 *   pcm_state stays X — the block keeps its X grant and is NEVER falsely
 *   advertised as flushed.  The S5 driver wraps this in PG_TRY/CATCH and goes
 *   ABORTED_ESCALATE rather than assume a half-completed drain succeeded.
 *   Clean (non-dirty) X blocks are intentionally left here: their storage image
 *   is already current; the PCM directory holder record is cleared separately
 *   by cluster_pcm_lock_clean_leave_release_all_self.  Over-flush is harmless.
 *   Returns the count of blocks flushed + X-released.
 * ======================================================================== */
uint32
cluster_bufmgr_flush_and_release_x_for_leave(void)
{
	uint32		flushed = 0;
	int			i;

	/* CL-I9: must run with a real ResourceOwner (backend/checkpointer). */
	Assert(CurrentResourceOwner != NULL);

	/*
	 * Make sure we can handle the pin inside the loop (as
	 * FlushDatabaseBuffers).
	 */
	ResourceOwnerEnlargeBuffers(CurrentResourceOwner);

	for (i = 0; i < NBuffers; i++)
	{
		BufferDesc *bufHdr = GetBufferDescriptor(i);
		uint32		buf_state;

		ReservePrivateRefCountEntry();

		buf_state = LockBufHdr(bufHdr);
		if ((buf_state & (BM_VALID | BM_DIRTY)) == (BM_VALID | BM_DIRTY) &&
			(PcmState) bufHdr->pcm_state == PCM_STATE_X)
		{
			ClusterPcmOwnResult bump_result;
			uint32		observed_flags = 0;
			uint64		observed_generation = 0;

			PinBuffer_Locked(bufHdr);	/* releases the header spinlock */
			LWLockAcquire(BufferDescriptorGetContentLock(bufHdr), LW_SHARED);
			FlushBuffer(bufHdr, NULL, IOOBJECT_RELATION, IOCONTEXT_NORMAL);
			LWLockRelease(BufferDescriptorGetContentLock(bufHdr));

			/*
			 * Release X only now that the block is durable + current on
			 * shared storage.  The pin held it valid across the unlocked
			 * window above.
			 */
			buf_state = LockBufHdr(bufHdr);
			/* PGRAC ownership-gen: bump under the held spinlock (X -> N release
			 * after the block is durable on shared storage). */
			bump_result = cluster_pcm_own_bump_locked(bufHdr, 0, 0,
												&observed_generation, &observed_flags);
			if (bump_result != CLUSTER_PCM_OWN_OK)
			{
				UnlockBufHdr(bufHdr, buf_state);
				UnpinBuffer(bufHdr);
				cluster_pcm_own_report_bump_failure(bufHdr, bump_result, observed_generation,
												  observed_flags, "clean-leave X release");
			}
			bufHdr->pcm_state = (uint8) PCM_STATE_N;
			UnlockBufHdr(bufHdr, buf_state);

			UnpinBuffer(bufHdr);
			flushed++;
		}
		else
			UnlockBufHdr(bufHdr, buf_state);
	}

	return flushed;
}

/* ========================================================================
 * PGRAC MODIFICATIONS by SqlRush — spec-5.2 §3.5 D11 (writer-transfer-revoke,
 *   active-ITL hard boundary, multi-row fail-closed leg).
 *
 *   cluster_bufmgr_block_write_permitted(buffer)
 *   — false when buffer's block is held by this backend as a DEFERRED
 *   READ-IMAGE (pcm_state == PCM_STATE_READ_IMAGE), or when PCM-X has
 *   published REVOKING/retained-image authority.  In the former case a remote
 *   node holds X and deferred its writer-transfer because it still has an
 *   uncommitted ITL slot.  In the latter, immutable transfer bytes must remain
 *   non-writable through exact DRAIN.  Outside those explicit states,
 *   pcm_state != X alone is not a valid "no write" test: freshly extended and
 *   storage-fallback N buffers can be legitimately writable.
 *
 *   In the contended-row case the heap AM waits and re-acquires real X
 *   (overwriting the marker) before writing, so it passes here; this catches
 *   only the writer whose own target row was NOT contended (it never entered
 *   the cross-node TX wait) and would otherwise mutate a non-owned read-image,
 *   diverging from the holder's X copy — a silent lost-update (Rule 8.A).  The
 *   cluster_itl forward-write allocation path fails closed (53R9H, retryable)
 *   when this returns false.
 *
 *   PGRAC modifications by SqlRush (spec-6.12a):
 *   What changed: with cluster.read_scache on, PCM_STATE_S also denies the
 *   forward ITL write.  Why: the quiescent X->S downgrade turns S into a
 *   revoked-write state whose copies other nodes may serve reads from; a
 *   legal writer re-acquires X via LockBuffer (S does not cover X) BEFORE
 *   reaching this gate, so denying S here only backstops paths that skipped
 *   the coherence upgrade (write-gate polarity flip, Rule 8.A).  Off keeps
 *   the deny-list byte-identical to the spec-5.2 baseline.
 * ======================================================================== */
bool
cluster_bufmgr_block_write_permitted(Buffer buffer)
{
	BufferDesc *buf;
	PcmState	state;
	uint32		buf_state;
	uint32		own_flags;

	if (BufferIsLocal(buffer))
		return true;			/* temp / local buffers are never CF-shared */
	buf = GetBufferDescriptor(buffer - 1);
	buf_state = LockBufHdr(buf);
	state = (PcmState) buf->pcm_state;
	own_flags = cluster_pcm_own_flags_get(buf->buf_id);
	if (cluster_bufmgr_pcm_x_retained_image_locked(buf, buf_state)
		|| (own_flags & PCM_OWN_FLAG_REVOKING) != 0)
	{
		UnlockBufHdr(buf, buf_state);
		return false;
	}
	UnlockBufHdr(buf, buf_state);
	if (state == PCM_STATE_READ_IMAGE)
		return false;
	if (cluster_read_scache && state == PCM_STATE_S)
		return false;			/* spec-6.12a: S is a revoked-write state */
	return true;
}

#endif							/* USE_PGRAC_CLUSTER */
