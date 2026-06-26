/*-------------------------------------------------------------------------
 *
 * cluster_cr_pool.c
 *	  pgrac dedicated shared CR buffer pool, v1 (spec-5.51).
 *
 *	  Per-instance, cross-backend shared-memory pool of full-block CR images
 *	  (L2 behind the backend-local L1 cache, cluster_cr_cache.c).  Physically
 *	  separate from shared_buffers.  See cluster_cr_pool.h for the contract and
 *	  the five 8.A safety properties (copy-out, slot generation, handle key,
 *	  pool_epoch lifecycle gate, exact-key safe-miss).
 *
 *	  Partitioned into CR_POOL_NSEGMENTS bands, one named LWLock per band; a key
 *	  maps to exactly one segment by hash, so all ops on a key touch a single
 *	  segment lock.  Construction (which may ereport) happens OUTSIDE the lock
 *	  (two-phase reserve/publish; rule 16).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-5.51-dedicated-shared-cr-buffer-pool-v1.md (FROZEN v1.0)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cr_pool.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "cluster/cluster_cr_pool.h"
#include "cluster/cluster_cr_cache.h" /* ClusterCRCacheKey, key_equal */
#include "cluster/cluster_shmem.h"	  /* ClusterShmemRegion, register */
#include "storage/bufpage.h"		  /* BLCKSZ */
#include "storage/lwlock.h"
#include "storage/shmem.h"

/* GUC backing (registered in cluster_guc.c, spec-5.51 D8). */
int cluster_shared_cr_pool_size_blocks = 0;	 /* 0 = disabled / zero memory */
bool cluster_shared_cr_pool_enabled = false; /* master switch (size=0 also off) */

/* Number of lock/slot partitions.  Compile-time constant so the named LWLock
 * tranche size is fixed.  Slots are split into NSEGMENTS contiguous bands. */
#define CR_POOL_NSEGMENTS 16
/* Defensive cap on total slots (mirrors the GUC max in cluster_guc.c). */
#define CR_POOL_HARD_CAP 262144 /* 2 GB of CR images */
#define CR_POOL_LWLOCK_TRANCHE "ClusterCRPool"

typedef enum ClusterCRPoolSlotState {
	CRPOOL_FREE = 0,
	CRPOOL_RESERVED,
	CRPOOL_VALID
} ClusterCRPoolSlotState;

typedef struct ClusterCRPoolSlot {
	ClusterCRCacheKey key;
	uint8 state;		  /* ClusterCRPoolSlotState */
	pg_atomic_uint32 ref; /* clock second-chance; the only field written under
							  * a SHARED lock (lookup hit), hence atomic */
	uint32 generation;	  /* ++ on every FREE->RESERVED and on evict (ABA) */
	uint64 pool_epoch;	  /* epoch stamped at reserve; lookup requires == cur */
	char page[BLCKSZ];
} ClusterCRPoolSlot;

typedef struct ClusterCRPoolShared {
	int nslots;	 /* total slots (multiple of NSEGMENTS) */
	int per_seg; /* slots per segment */
	pg_atomic_uint64 cr_pool_epoch;
	uint32 clock_hand[CR_POOL_NSEGMENTS]; /* per-segment, EXCLUSIVE-guarded */
	/* counters (read lock-free by dump) */
	pg_atomic_uint64 hit_count;
	pg_atomic_uint64 miss_count;
	pg_atomic_uint64 reserve_count;
	pg_atomic_uint64 publish_count;
	pg_atomic_uint64 abort_count;
	pg_atomic_uint64 evict_count;
	pg_atomic_uint64 epoch_bump_count;
	pg_atomic_uint64 publish_stale_release_count;
	/*
	 * spec-5.53 D5: identity/reuse-fence mismatch diagnostics (dumped under the
	 * 'cr' category).  key/epoch/base_lsn are most-specific near-miss buckets set
	 * on a lookup miss; generation is the publish-side ABA; locator_reuse_reject
	 * is the catalog-incarnation belt's 8.A safety-gate observable and stays 0 in
	 * this build (D0 = RED → floor-only; the floor attributes relfilenode-reuse
	 * MISSes to epoch_mismatch, spec-5.53 §3.2).
	 */
	pg_atomic_uint64 key_mismatch_count;
	pg_atomic_uint64 epoch_mismatch_count;
	pg_atomic_uint64 generation_mismatch_count;
	pg_atomic_uint64 base_lsn_mismatch_count;
	pg_atomic_uint64 locator_reuse_reject_count;
	ClusterCRPoolSlot slots[FLEXIBLE_ARRAY_MEMBER];
} ClusterCRPoolShared;

static ClusterCRPoolShared *CRPool = NULL;
static LWLockPadded *cr_pool_locks = NULL;

/* ============================================================
 * Sizing helpers
 * ============================================================ */

/*
 * Effective slot count: 0 when disabled, otherwise size_blocks rounded down to
 * a multiple of NSEGMENTS (>= NSEGMENTS so every segment owns >= 1 slot),
 * clamped to the hard cap.
 */
static int
cr_pool_effective_nslots(void)
{
	int n;

	if (!cluster_shared_cr_pool_enabled || cluster_shared_cr_pool_size_blocks <= 0)
		return 0;

	n = cluster_shared_cr_pool_size_blocks;
	if (n > CR_POOL_HARD_CAP)
		n = CR_POOL_HARD_CAP;
	if (n < CR_POOL_NSEGMENTS)
		n = CR_POOL_NSEGMENTS;
	n -= (n % CR_POOL_NSEGMENTS); /* exact multiple of NSEGMENTS */
	return n;
}

Size
cluster_cr_pool_shmem_size(void)
{
	int n = cr_pool_effective_nslots();

	if (n == 0)
		return 0; /* spec-5.51 P1#4: registered but zero bytes when disabled */
	return MAXALIGN(offsetof(ClusterCRPoolShared, slots) + (Size)n * sizeof(ClusterCRPoolSlot));
}

/* ============================================================
 * Hashing / segment mapping
 * ============================================================ */

static inline uint32
cr_pool_key_hash(const ClusterCRCacheKey *key)
{
	uint32 h = 2166136261u; /* FNV-1a basis */

#define CR_POOL_MIX(v) (h = (h ^ (uint32)(v)) * 16777619u)
	CR_POOL_MIX(key->rlocator.spcOid);
	CR_POOL_MIX(key->rlocator.dbOid);
	CR_POOL_MIX(key->rlocator.relNumber);
	CR_POOL_MIX((uint32)key->forknum);
	CR_POOL_MIX(key->blockno);
	CR_POOL_MIX((uint32)(key->read_scn & 0xffffffffu));
	CR_POOL_MIX((
		uint32)(key->read_scn
				>> 32)); /* SCN_CMP_OK: bit-shift hash mix of read_scn, not an ordering comparison */
	CR_POOL_MIX((uint32)(key->base_page_lsn & 0xffffffffu));
	CR_POOL_MIX((uint32)(key->base_page_lsn >> 32));
#undef CR_POOL_MIX
	return h;
}

static inline int
cr_pool_seg_of(const ClusterCRCacheKey *key)
{
	return (int)(cr_pool_key_hash(key) % CR_POOL_NSEGMENTS);
}

static inline void
cr_pool_seg_range(int seg, int *lo, int *hi)
{
	*lo = seg * CRPool->per_seg;
	*hi = *lo + CRPool->per_seg;
}

static inline LWLock *
cr_pool_seg_lock(int seg)
{
	return &cr_pool_locks[seg].lock;
}

/* ============================================================
 * Shmem region lifecycle
 * ============================================================ */

void
cluster_cr_pool_request_lwlocks(void)
{
	/* Only reserve the tranche when the pool is actually enabled; size_blocks
	 * is PGC_POSTMASTER so its value is final at the request phase. */
	if (cr_pool_effective_nslots() > 0)
		RequestNamedLWLockTranche(CR_POOL_LWLOCK_TRANCHE, CR_POOL_NSEGMENTS);
}

void
cluster_cr_pool_shmem_init(void)
{
	int n = cr_pool_effective_nslots();
	bool found;
	Size sz = cluster_cr_pool_shmem_size();

	if (n == 0 || sz == 0) {
		/* Disabled: no region, every op is a safe no-op / miss. */
		CRPool = NULL;
		cr_pool_locks = NULL;
		return;
	}

	CRPool = (ClusterCRPoolShared *)ShmemInitStruct("ClusterCRPool", sz, &found);
	cr_pool_locks = GetNamedLWLockTranche(CR_POOL_LWLOCK_TRANCHE);

	if (!found) {
		int i;

		CRPool->nslots = n;
		CRPool->per_seg = n / CR_POOL_NSEGMENTS;
		pg_atomic_init_u64(&CRPool->cr_pool_epoch, 1); /* 0 reserved for "disabled" */
		for (i = 0; i < CR_POOL_NSEGMENTS; i++)
			CRPool->clock_hand[i] = 0;
		pg_atomic_init_u64(&CRPool->hit_count, 0);
		pg_atomic_init_u64(&CRPool->miss_count, 0);
		pg_atomic_init_u64(&CRPool->reserve_count, 0);
		pg_atomic_init_u64(&CRPool->publish_count, 0);
		pg_atomic_init_u64(&CRPool->abort_count, 0);
		pg_atomic_init_u64(&CRPool->evict_count, 0);
		pg_atomic_init_u64(&CRPool->epoch_bump_count, 0);
		pg_atomic_init_u64(&CRPool->publish_stale_release_count, 0);
		pg_atomic_init_u64(&CRPool->key_mismatch_count, 0);
		pg_atomic_init_u64(&CRPool->epoch_mismatch_count, 0);
		pg_atomic_init_u64(&CRPool->generation_mismatch_count, 0);
		pg_atomic_init_u64(&CRPool->base_lsn_mismatch_count, 0);
		pg_atomic_init_u64(&CRPool->locator_reuse_reject_count, 0);
		for (i = 0; i < n; i++) {
			memset(&CRPool->slots[i].key, 0, sizeof(ClusterCRCacheKey));
			CRPool->slots[i].state = CRPOOL_FREE;
			pg_atomic_init_u32(&CRPool->slots[i].ref, 0);
			CRPool->slots[i].generation = 0;
			CRPool->slots[i].pool_epoch = 0;
		}
	}
}

static const ClusterShmemRegion cluster_cr_pool_region = {
	.name = "pgrac cluster cr pool",
	.size_fn = cluster_cr_pool_shmem_size,
	.init_fn = cluster_cr_pool_shmem_init,
	.lwlock_count = CR_POOL_NSEGMENTS, /* informational; requested separately */
	.owner_subsys = "cluster_cr_pool",
	.reserved_flags = 0,
};

void
cluster_cr_pool_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_cr_pool_region);
}

/* ============================================================
 * Lifecycle epoch
 * ============================================================ */

void
cluster_cr_pool_bump_epoch(void)
{
	if (CRPool == NULL)
		return;
	pg_atomic_fetch_add_u64(&CRPool->cr_pool_epoch, 1);
	pg_atomic_fetch_add_u64(&CRPool->epoch_bump_count, 1);
}

uint64
cluster_cr_pool_current_epoch(void)
{
	if (CRPool == NULL)
		return 0;
	return pg_atomic_read_u64(&CRPool->cr_pool_epoch);
}

/* ============================================================
 * Lookup (copy-out) — spec-5.51 D2
 * ============================================================ */

bool
cluster_cr_pool_lookup_copy(const ClusterCRCacheKey *key, char *dst)
{
	int seg;
	int lo, hi, i;
	uint64 cur;
	bool hit = false;
	bool near_epoch = false; /* spec-5.53 D5: exact-key slot at a stale epoch */

	if (CRPool == NULL || key == NULL || dst == NULL)
		return false;

	cur = pg_atomic_read_u64(&CRPool->cr_pool_epoch);
	seg = cr_pool_seg_of(key);
	cr_pool_seg_range(seg, &lo, &hi);

	/*
	 * spec-5.53 D5: while scanning for the exact hit, note an exact-key slot that
	 * is at a STALE epoch (the L2 reuse fence firing across backends).  This is
	 * reliable: an exact key hashes to this same segment.  The over-miss
	 * near-miss buckets (base_lsn / key) are classified at L1 instead, where the
	 * scan is linear — at L2 a churned/diverged key hashes to a DIFFERENT segment
	 * (cr_pool_seg_of mixes read_scn + base_page_lsn) so a near-match here is not
	 * reliably visible.
	 */
	LWLockAcquire(cr_pool_seg_lock(seg), LW_SHARED);
	for (i = lo; i < hi; i++) {
		ClusterCRPoolSlot *s = &CRPool->slots[i];

		if (s->state != CRPOOL_VALID)
			continue; /* never serve / classify FREE or RESERVED */

		/* Exact-key + epoch match (8.A no-false-hit). */
		if (cluster_cr_cache_key_equal(&s->key, key)) {
			if (s->pool_epoch == cur) {
				pg_atomic_write_u32(&s->ref, 1); /* second chance */
				memcpy(dst, s->page, BLCKSZ);	 /* copy-out WHILE locked */
				hit = true;
				break;
			}
			/* full key match, stale epoch: a relfilenode lifecycle event (DROP /
			 * TRUNCATE / reuse / sinval) happened since install — the epoch fence
			 * forces a MISS (rule 8.A: never a stale hit).  Keep scanning: after a
			 * reuse the segment can hold BOTH this stale slot AND a fresh
			 * current-epoch slot for the same key, and the current one must hit. */
			near_epoch = true;
			continue;
		}
	}
	LWLockRelease(cr_pool_seg_lock(seg));

	if (hit) {
		pg_atomic_fetch_add_u64(&CRPool->hit_count, 1);
		return true;
	}

	pg_atomic_fetch_add_u64(&CRPool->miss_count, 1);
	if (near_epoch)
		pg_atomic_fetch_add_u64(&CRPool->epoch_mismatch_count, 1);
	return false;
}

/* ============================================================
 * Two-phase insert — spec-5.51 D3
 * ============================================================ */

/* Pick a victim within [lo,hi): prefer FREE; else clock-sweep skipping RESERVED
 * and clearing ref.  Caller holds the segment EXCLUSIVE lock. */
static int
cr_pool_pick_victim(int seg, int lo, int hi, bool *evicted)
{
	int span = hi - lo;
	int i;

	*evicted = false;

	for (i = lo; i < hi; i++) {
		if (CRPool->slots[i].state == CRPOOL_FREE)
			return i;
	}

	/* All non-FREE: clock sweep.  Bounded at 2*span steps; RESERVED is skipped
	 * (a slot mid-construction must not be evicted).  If every slot is RESERVED
	 * there is no victim -> -1. */
	{
		int hand = CRPool->clock_hand[seg];
		int steps;

		if (hand < lo || hand >= hi)
			hand = lo;
		for (steps = 0; steps < 2 * span; steps++) {
			int idx = hand;

			hand = (hand + 1 - lo) % span + lo;
			if (CRPool->slots[idx].state == CRPOOL_RESERVED)
				continue;
			/* VALID slot */
			if (pg_atomic_read_u32(&CRPool->slots[idx].ref) != 0) {
				pg_atomic_write_u32(&CRPool->slots[idx].ref, 0);
				continue;
			}
			CRPool->clock_hand[seg] = hand;
			*evicted = true;
			return idx;
		}
		CRPool->clock_hand[seg] = hand;
	}
	return -1; /* all RESERVED (or churn) -> caller falls back to construct-only */
}

bool
cluster_cr_pool_reserve(const ClusterCRCacheKey *key, ClusterCRPoolHandle *out)
{
	int seg, lo, hi, idx;
	bool evicted = false;

	if (out != NULL)
		out->valid = false;

	if (CRPool == NULL || key == NULL || out == NULL)
		return false;

	seg = cr_pool_seg_of(key);
	cr_pool_seg_range(seg, &lo, &hi);

	LWLockAcquire(cr_pool_seg_lock(seg), LW_EXCLUSIVE);
	idx = cr_pool_pick_victim(seg, lo, hi, &evicted);
	if (idx < 0) {
		LWLockRelease(cr_pool_seg_lock(seg));
		return false;
	}

	if (evicted)
		pg_atomic_fetch_add_u64(&CRPool->evict_count, 1);

	/* Reserve: INVALID until publish; bump generation so any pre-existing handle
	 * to this slot (from an aborted/recycled reservation) is now stale. */
	CRPool->slots[idx].state = CRPOOL_RESERVED;
	CRPool->slots[idx].generation++;
	CRPool->slots[idx].key = *key;
	CRPool->slots[idx].pool_epoch = pg_atomic_read_u64(&CRPool->cr_pool_epoch);
	pg_atomic_write_u32(&CRPool->slots[idx].ref, 0);

	out->valid = true;
	out->slot_idx = idx;
	out->seg = seg;
	out->generation = CRPool->slots[idx].generation;
	out->pool_epoch = CRPool->slots[idx].pool_epoch;
	out->key = *key;

	LWLockRelease(cr_pool_seg_lock(seg));
	pg_atomic_fetch_add_u64(&CRPool->reserve_count, 1);
	return true;
}

void
cluster_cr_pool_publish(const ClusterCRPoolHandle *h, const char *page)
{
	ClusterCRPoolSlot *s;
	uint64 cur;

	if (CRPool == NULL || h == NULL || !h->valid || page == NULL)
		return;

	LWLockAcquire(cr_pool_seg_lock(h->seg), LW_EXCLUSIVE);
	s = &CRPool->slots[h->slot_idx];

	/* Generation mismatch: the slot was recycled by someone else since reserve.
	 * Do not touch it (it is now theirs).  spec-5.53 D5: count the ABA case
	 * (generation moved) distinctly so two-phase-insert contention is observable. */
	if (s->state != CRPOOL_RESERVED || s->generation != h->generation
		|| !cluster_cr_cache_key_equal(&s->key, &h->key)) {
		bool aba = (s->generation != h->generation);

		LWLockRelease(cr_pool_seg_lock(h->seg));
		if (aba)
			pg_atomic_fetch_add_u64(&CRPool->generation_mismatch_count, 1);
		return;
	}

	cur = pg_atomic_read_u64(&CRPool->cr_pool_epoch);
	if (s->pool_epoch != cur) {
		/* Generation still ours, but a lifecycle epoch bump happened during
		 * construction: the image may be stale for the new epoch.  Release the
		 * reservation (FREE, generation++) without installing so the RESERVED
		 * slot does not leak (spec-5.51). */
		s->state = CRPOOL_FREE;
		s->generation++;
		pg_atomic_write_u32(&s->ref, 0);
		LWLockRelease(cr_pool_seg_lock(h->seg));
		pg_atomic_fetch_add_u64(&CRPool->publish_stale_release_count, 1);
		return;
	}

	memcpy(s->page, page, BLCKSZ);
	s->state = CRPOOL_VALID;
	pg_atomic_write_u32(&s->ref, 1); /* one grace period after install */
	LWLockRelease(cr_pool_seg_lock(h->seg));
	pg_atomic_fetch_add_u64(&CRPool->publish_count, 1);
}

void
cluster_cr_pool_abort(const ClusterCRPoolHandle *h)
{
	ClusterCRPoolSlot *s;

	if (CRPool == NULL || h == NULL || !h->valid)
		return;

	LWLockAcquire(cr_pool_seg_lock(h->seg), LW_EXCLUSIVE);
	s = &CRPool->slots[h->slot_idx];
	if (s->state == CRPOOL_RESERVED && s->generation == h->generation) {
		s->state = CRPOOL_FREE;
		s->generation++;
		pg_atomic_write_u32(&s->ref, 0);
		LWLockRelease(cr_pool_seg_lock(h->seg));
		pg_atomic_fetch_add_u64(&CRPool->abort_count, 1);
		return;
	}
	LWLockRelease(cr_pool_seg_lock(h->seg));
}

/* ============================================================
 * Counters + diagnostics
 * ============================================================ */

#define CR_POOL_COUNTER_ACCESSOR(fn, field)                                                        \
	uint64 fn(void)                                                                                \
	{                                                                                              \
		return (CRPool != NULL) ? pg_atomic_read_u64(&CRPool->field) : 0;                          \
	}

CR_POOL_COUNTER_ACCESSOR(cluster_cr_pool_hit_count, hit_count)
CR_POOL_COUNTER_ACCESSOR(cluster_cr_pool_miss_count, miss_count)
CR_POOL_COUNTER_ACCESSOR(cluster_cr_pool_reserve_count, reserve_count)
CR_POOL_COUNTER_ACCESSOR(cluster_cr_pool_publish_count, publish_count)
CR_POOL_COUNTER_ACCESSOR(cluster_cr_pool_abort_count, abort_count)
CR_POOL_COUNTER_ACCESSOR(cluster_cr_pool_evict_count, evict_count)
CR_POOL_COUNTER_ACCESSOR(cluster_cr_pool_epoch_bump_count, epoch_bump_count)
CR_POOL_COUNTER_ACCESSOR(cluster_cr_pool_publish_stale_release_count, publish_stale_release_count)
/* spec-5.53 D5: identity/reuse-fence mismatch diagnostics. */
CR_POOL_COUNTER_ACCESSOR(cluster_cr_pool_key_mismatch_count, key_mismatch_count)
CR_POOL_COUNTER_ACCESSOR(cluster_cr_pool_epoch_mismatch_count, epoch_mismatch_count)
CR_POOL_COUNTER_ACCESSOR(cluster_cr_pool_generation_mismatch_count, generation_mismatch_count)
CR_POOL_COUNTER_ACCESSOR(cluster_cr_pool_base_lsn_mismatch_count, base_lsn_mismatch_count)
CR_POOL_COUNTER_ACCESSOR(cluster_cr_pool_locator_reuse_reject_count, locator_reuse_reject_count)

/*
 * cluster_cr_pool_note_l1_epoch_mismatch -- the backend-local L1 fence
 * (cluster_cr_cache.c) rejected an exact-key entry whose stamped epoch was stale
 * (spec-5.53 D2c).  Fold it into the SAME epoch_mismatch counter as the L2
 * per-slot fence so "L1+L2 same fence" reports one number.  No-op when the pool
 * is disabled (epoch is 0, so the L1 fence never rejects).
 */
void
cluster_cr_pool_note_l1_epoch_mismatch(void)
{
	if (CRPool != NULL)
		pg_atomic_fetch_add_u64(&CRPool->epoch_mismatch_count, 1);
}

/*
 * cluster_cr_pool_note_base_lsn_mismatch / _key_mismatch -- the backend-local L1
 * scan (cluster_cr_cache.c, reliable because it is linear) attributed a MISS to
 * a churned page version (base_lsn) or a diverged snapshot (read_scn).  Recorded
 * in the pool region so all five mismatch diagnostics dump together (spec-5.53
 * D5).  No-op when the pool is disabled.
 */
void
cluster_cr_pool_note_base_lsn_mismatch(void)
{
	if (CRPool != NULL)
		pg_atomic_fetch_add_u64(&CRPool->base_lsn_mismatch_count, 1);
}

void
cluster_cr_pool_note_key_mismatch(void)
{
	if (CRPool != NULL)
		pg_atomic_fetch_add_u64(&CRPool->key_mismatch_count, 1);
}

int
cluster_cr_pool_live_entries(void)
{
	int i;
	int n = 0;

	if (CRPool == NULL)
		return 0;
	for (i = 0; i < CRPool->nslots; i++) {
		if (CRPool->slots[i].state == CRPOOL_VALID)
			n++;
	}
	return n;
}
