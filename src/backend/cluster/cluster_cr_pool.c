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

/* GUC backing for the per-relation generation table (spec-5.56 D4, Part B).
 * 0 = disabled (coarse global-epoch bump; zero behavior change). */
int cluster_cr_pool_rel_generation_slots = 0;

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
	uint64 rel_gen;		  /* spec-5.56 D4: per-relation install generation stamped at
						   * reserve; lookup_copy/publish require == the locator's
						   * current gen (composite {pool_epoch, rel_gen} fence, P1-c).
						   * 0 = gen table disabled => epoch-only (spec-5.53). */
	char page[BLCKSZ];
} ClusterCRPoolSlot;

/*
 * spec-5.56 D1: lock the lifecycle FENCE field set on the pool slot.  rel_gen is
 * the last fence field; if a fence field is added/removed or its width changes,
 * re-review the §3.1 C1-C4 lifecycle contract (every "失效 / 存活 / fail-closed"
 * state) and the composite-fence recheck points (§2.2 ①-⑤) before changing it.
 */
StaticAssertDecl(sizeof(((ClusterCRPoolSlot *)0)->pool_epoch) == sizeof(uint64)
					 && sizeof(((ClusterCRPoolSlot *)0)->rel_gen) == sizeof(uint64),
				 "CR pool slot lifecycle fence field widths changed: re-review the "
				 "spec-5.56 §3.1 lifecycle contract + §2.2 composite-fence recheck points.");

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
	/*
	 * spec-5.56 D5: lifecycle counters (folded into THIS region + the 'cr' dump
	 * category, no new region — 5.53 mismatch-counter precedent).  All 0 when the
	 * pool / gen table is disabled.
	 */
	pg_atomic_uint64 global_epoch_fallback_bump_count;		/* coarse fallback freq */
	pg_atomic_uint64 rel_gen_bump_count;					/* fine-grained bumps (GO) */
	pg_atomic_uint64 rel_gen_table_overflow_count;			/* register skip-cache */
	pg_atomic_uint64 retention_horizon_advance_noted_count; /* C3 observation */
	pg_atomic_uint64 reconfig_intra_survived_count;			/* C4 evidence */
	ClusterCRPoolSlot slots[FLEXIBLE_ARRAY_MEMBER];
} ClusterCRPoolShared;

static ClusterCRPoolShared *CRPool = NULL;
static LWLockPadded *cr_pool_locks = NULL;

/* ============================================================
 * Per-relation lifecycle generation table (spec-5.56 D4, Part B)
 * ============================================================ */

#define CR_RELGEN_LWLOCK_TRANCHE "ClusterCRRelGen"
/* Defensive cap mirroring the GUC max (cluster_guc.c). */
#define CR_RELGEN_HARD_CAP 262144

typedef struct ClusterCRRelGenEntry {
	RelFileLocator locator; /* full identity (spc/db/rel); valid iff gen >= 1 */
	uint64 gen;				/* 1-based monotone generation; 0 = empty slot */
} ClusterCRRelGenEntry;

typedef struct ClusterCRRelGenShared {
	int nslots;			   /* total slots (multiple of NSEGMENTS) */
	int per_seg;		   /* slots per segment (open-addressing band) */
	pg_atomic_uint64 live; /* registered-locator count (diagnostic) */
	ClusterCRRelGenEntry entries[FLEXIBLE_ARRAY_MEMBER];
} ClusterCRRelGenShared;

static ClusterCRRelGenShared *CRRelGen = NULL;
static LWLockPadded *cr_relgen_locks = NULL;

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

/*
 * Effective gen-table slot count (spec-5.56 D4): 0 unless the POOL is enabled
 * (the gen table is meaningless without the pool) AND the gen slots GUC > 0;
 * otherwise rel_generation_slots rounded down to a multiple of NSEGMENTS (>=
 * NSEGMENTS), clamped to the hard cap.
 */
static int
cr_relgen_effective_nslots(void)
{
	int n;

	if (cr_pool_effective_nslots() == 0 || cluster_cr_pool_rel_generation_slots <= 0)
		return 0;

	n = cluster_cr_pool_rel_generation_slots;
	if (n > CR_RELGEN_HARD_CAP)
		n = CR_RELGEN_HARD_CAP;
	if (n < CR_POOL_NSEGMENTS)
		n = CR_POOL_NSEGMENTS;
	n -= (n % CR_POOL_NSEGMENTS);
	return n;
}

void
cluster_cr_pool_request_lwlocks(void)
{
	/* Only reserve the tranche when the pool is actually enabled; size_blocks
	 * is PGC_POSTMASTER so its value is final at the request phase. */
	if (cr_pool_effective_nslots() > 0)
		RequestNamedLWLockTranche(CR_POOL_LWLOCK_TRANCHE, CR_POOL_NSEGMENTS);
	/* spec-5.56 D4: per-relation generation table tranche (separate region). */
	if (cr_relgen_effective_nslots() > 0)
		RequestNamedLWLockTranche(CR_RELGEN_LWLOCK_TRANCHE, CR_POOL_NSEGMENTS);
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
		pg_atomic_init_u64(&CRPool->global_epoch_fallback_bump_count, 0);
		pg_atomic_init_u64(&CRPool->rel_gen_bump_count, 0);
		pg_atomic_init_u64(&CRPool->rel_gen_table_overflow_count, 0);
		pg_atomic_init_u64(&CRPool->retention_horizon_advance_noted_count, 0);
		pg_atomic_init_u64(&CRPool->reconfig_intra_survived_count, 0);
		for (i = 0; i < n; i++) {
			memset(&CRPool->slots[i].key, 0, sizeof(ClusterCRCacheKey));
			CRPool->slots[i].state = CRPOOL_FREE;
			pg_atomic_init_u32(&CRPool->slots[i].ref, 0);
			CRPool->slots[i].generation = 0;
			CRPool->slots[i].pool_epoch = 0;
			CRPool->slots[i].rel_gen = 0;
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

/* ---- per-relation generation table region (spec-5.56 D4) ---- */

Size
cluster_cr_pool_relgen_shmem_size(void)
{
	int n = cr_relgen_effective_nslots();

	if (n == 0)
		return 0; /* registered but zero bytes when disabled (deterministic baseline) */
	return MAXALIGN(offsetof(ClusterCRRelGenShared, entries)
					+ (Size)n * sizeof(ClusterCRRelGenEntry));
}

void
cluster_cr_pool_relgen_shmem_init(void)
{
	int n = cr_relgen_effective_nslots();
	bool found;
	Size sz = cluster_cr_pool_relgen_shmem_size();

	if (n == 0 || sz == 0) {
		CRRelGen = NULL;
		cr_relgen_locks = NULL;
		return;
	}

	CRRelGen = (ClusterCRRelGenShared *)ShmemInitStruct("ClusterCRRelGen", sz, &found);
	cr_relgen_locks = GetNamedLWLockTranche(CR_RELGEN_LWLOCK_TRANCHE);

	if (!found) {
		int i;

		CRRelGen->nslots = n;
		CRRelGen->per_seg = n / CR_POOL_NSEGMENTS;
		pg_atomic_init_u64(&CRRelGen->live, 0);
		for (i = 0; i < n; i++) {
			memset(&CRRelGen->entries[i].locator, 0, sizeof(RelFileLocator));
			CRRelGen->entries[i].gen = 0; /* empty */
		}
	}
}

static const ClusterShmemRegion cluster_cr_relgen_region = {
	.name = "pgrac cluster cr relgen",
	.size_fn = cluster_cr_pool_relgen_shmem_size,
	.init_fn = cluster_cr_pool_relgen_shmem_init,
	.lwlock_count = CR_POOL_NSEGMENTS, /* informational; requested separately */
	.owner_subsys = "cluster_cr_pool",
	.reserved_flags = 0,
};

void
cluster_cr_pool_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_cr_pool_region);
	cluster_shmem_register_region(&cluster_cr_relgen_region); /* spec-5.56 D4 */
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

void
cluster_cr_pool_global_fallback_bump(void)
{
	if (CRPool == NULL)
		return;
	/* The detected-failure backstop: a coarse whole-pool flush (spec-5.53). */
	pg_atomic_fetch_add_u64(&CRPool->cr_pool_epoch, 1);
	pg_atomic_fetch_add_u64(&CRPool->epoch_bump_count, 1);
	pg_atomic_fetch_add_u64(&CRPool->global_epoch_fallback_bump_count, 1);
}

/* ============================================================
 * Per-relation generation table ops — spec-5.56 D4
 *
 * Open-addressing hash partitioned into CR_POOL_NSEGMENTS bands by a locator-only
 * hash; one named LWLock per band.  APPEND-ONLY (INV-G2): entries are never
 * removed, so a lookup's probe sequence is contiguous and stops at the first
 * empty (gen == 0) slot.  unlink bumps gen in place (handles relfilenode reuse:
 * the reused locator's next install reads the bumped gen).
 * ============================================================ */

static inline uint32
cr_relgen_locator_hash(RelFileLocator loc)
{
	uint32 h = 2166136261u; /* FNV-1a basis */

#define CR_RELGEN_MIX(v) (h = (h ^ (uint32)(v)) * 16777619u)
	CR_RELGEN_MIX(loc.spcOid);
	CR_RELGEN_MIX(loc.dbOid);
	CR_RELGEN_MIX(loc.relNumber);
#undef CR_RELGEN_MIX
	return h;
}

static inline LWLock *
cr_relgen_seg_lock(int seg)
{
	return &cr_relgen_locks[seg].lock;
}

/* Resolve the band [*lo, *lo + per) and the in-band probe start for `loc`. */
static inline int
cr_relgen_locate(RelFileLocator loc, int *lo, int *per)
{
	uint32 h = cr_relgen_locator_hash(loc);
	int seg = (int)(h % CR_POOL_NSEGMENTS);

	*per = CRRelGen->per_seg;
	*lo = seg * (*per);
	/* in-band start = a second hash slice mod per_seg (distinct from the segment
	 * selector so two locators in one band do not collide on the home slot). */
	return *lo + (int)((h / CR_POOL_NSEGMENTS) % (uint32)(*per));
}

bool
cluster_cr_pool_rel_generation_enabled(void)
{
	return CRRelGen != NULL;
}

bool
cluster_cr_pool_rel_generation(RelFileLocator loc, uint64 *out_gen)
{
	int lo, per, start, i, seg;
	bool found = false;

	if (out_gen != NULL)
		*out_gen = 0;
	if (CRRelGen == NULL)
		return false; /* disabled => no per-rel fence (epoch-only) */

	start = cr_relgen_locate(loc, &lo, &per);
	seg = lo / per;
	LWLockAcquire(cr_relgen_seg_lock(seg), LW_SHARED);
	for (i = 0; i < per; i++) {
		const ClusterCRRelGenEntry *e = &CRRelGen->entries[lo + ((start - lo) + i) % per];

		if (e->gen == 0)
			break; /* contiguous probe end (append-only): not registered */
		if (RelFileLocatorEquals(e->locator, loc)) {
			if (out_gen != NULL)
				*out_gen = e->gen;
			found = true;
			break;
		}
	}
	LWLockRelease(cr_relgen_seg_lock(seg));
	return found;
}

bool
cluster_cr_pool_register_locator(RelFileLocator loc, uint64 *out_gen)
{
	int lo, per, start, i, seg, free_idx = -1;

	if (out_gen != NULL)
		*out_gen = 0;
	if (CRRelGen == NULL)
		return true; /* disabled: proceed, epoch-only fence (out_gen stays 0) */

	start = cr_relgen_locate(loc, &lo, &per);
	seg = lo / per;
	LWLockAcquire(cr_relgen_seg_lock(seg), LW_EXCLUSIVE);
	for (i = 0; i < per; i++) {
		int idx = lo + ((start - lo) + i) % per;
		const ClusterCRRelGenEntry *e = &CRRelGen->entries[idx];

		if (e->gen == 0) {
			free_idx = idx; /* first empty in the probe = install site */
			break;
		}
		if (RelFileLocatorEquals(e->locator, loc)) {
			if (out_gen != NULL)
				*out_gen = e->gen; /* already registered => current gen */
			LWLockRelease(cr_relgen_seg_lock(seg));
			return true;
		}
	}
	if (free_idx < 0) {
		/* band full: serve-but-skip-cache (P2-d′: NO global bump — nothing was
		 * cached for this locator, so there is nothing to invalidate). */
		LWLockRelease(cr_relgen_seg_lock(seg));
		if (CRPool != NULL)
			pg_atomic_fetch_add_u64(&CRPool->rel_gen_table_overflow_count, 1);
		return false;
	}
	CRRelGen->entries[free_idx].locator = loc;
	CRRelGen->entries[free_idx].gen = 1; /* 1-based (P2-e′: 0 reserved for empty) */
	pg_atomic_fetch_add_u64(&CRRelGen->live, 1);
	if (out_gen != NULL)
		*out_gen = 1;
	LWLockRelease(cr_relgen_seg_lock(seg));
	return true;
}

void
cluster_cr_pool_unlink_locator(RelFileLocator loc)
{
	int lo, per, start, i, seg;
	bool bumped = false;

	if (CRRelGen == NULL)
		return; /* disabled: the caller does the unconditional global bump */

	start = cr_relgen_locate(loc, &lo, &per);
	seg = lo / per;
	LWLockAcquire(cr_relgen_seg_lock(seg), LW_EXCLUSIVE);
	for (i = 0; i < per; i++) {
		ClusterCRRelGenEntry *e = &CRRelGen->entries[lo + ((start - lo) + i) % per];

		if (e->gen == 0)
			break; /* untracked: INV-G3 guarantees no L1/L2 entry => safe NO-OP */
		if (RelFileLocatorEquals(e->locator, loc)) {
			e->gen++; /* fine-grained: only THIS relation's images are fenced */
			bumped = true;
			break;
		}
	}
	LWLockRelease(cr_relgen_seg_lock(seg));
	if (bumped && CRPool != NULL)
		pg_atomic_fetch_add_u64(&CRPool->rel_gen_bump_count, 1);
}

int
cluster_cr_pool_rel_generation_live(void)
{
	if (CRRelGen == NULL)
		return 0;
	return (int)pg_atomic_read_u64(&CRRelGen->live);
}

/*
 * cr_relgen_slot_current -- the per-relation half of the composite fence for an
 * L2 slot: true iff the gen table is disabled (epoch-only) OR the slot's locator
 * is registered AND the slot's stamped rel_gen equals the locator's CURRENT
 * generation.  An unregistered locator on a VALID slot is impossible (INV-G3) but
 * is treated as a fail-closed MISS.  Callers hold the slot's pool segment lock;
 * this nests the relgen band lock (consistent order pool -> relgen, no cycle).
 */
static inline bool
cr_relgen_slot_current(const ClusterCRPoolSlot *s)
{
	uint64 g;

	if (CRRelGen == NULL)
		return true; /* disabled: epoch-only */
	if (!cluster_cr_pool_rel_generation(s->key.rlocator, &g))
		return false; /* unregistered locator (must not happen for VALID): MISS */
	return s->rel_gen == g;
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

		/* Exact-key + COMPOSITE lifecycle fence (8.A no-false-hit; spec-5.56 ③):
		 * serve only when BOTH the global epoch AND the per-relation generation are
		 * current.  cr_relgen_slot_current reads the locator's CURRENT gen (fresh,
		 * under the relgen band lock) and compares the slot's stamp — so a
		 * fine-grained per-relation unlink (GO mode, no global epoch bump) that
		 * raced our top-level capture is still caught here. */
		if (cluster_cr_cache_key_equal(&s->key, key)) {
			if (s->pool_epoch == cur && cr_relgen_slot_current(s)) {
				pg_atomic_write_u32(&s->ref, 1); /* second chance */
				memcpy(dst, s->page, BLCKSZ);	 /* copy-out WHILE locked */
				hit = true;
				break;
			}
			/* full key match, stale epoch OR stale per-relation gen: a lifecycle
			 * event (DROP / TRUNCATE / reuse / sinval, coarse OR per-relation)
			 * happened since install — the composite fence forces a MISS (rule
			 * 8.A: never a stale hit).  Keep scanning: after a reuse the segment can
			 * hold BOTH this stale slot AND a fresh current slot for the same key,
			 * and the current one must hit. */
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
	/* spec-5.56: epoch-only callers reserve with install_gen == 0 (no per-rel
	 * fence; the disabled / cluster_unit-substrate path). */
	return cluster_cr_pool_reserve_gen(key, 0, out);
}

bool
cluster_cr_pool_reserve_gen(const ClusterCRCacheKey *key, uint64 install_gen,
							ClusterCRPoolHandle *out)
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
	CRPool->slots[idx].rel_gen = install_gen; /* spec-5.56 D4: per-relation stamp */
	pg_atomic_write_u32(&CRPool->slots[idx].ref, 0);

	out->valid = true;
	out->slot_idx = idx;
	out->seg = seg;
	out->generation = CRPool->slots[idx].generation;
	out->pool_epoch = CRPool->slots[idx].pool_epoch;
	out->rel_gen = install_gen;
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
	if (s->pool_epoch != cur || !cr_relgen_slot_current(s)) {
		/* Generation still ours, but a lifecycle event happened during
		 * construction (spec-5.56 ⑤): the global epoch advanced (coarse) OR this
		 * relation's per-relation generation advanced (a per-relation unlink in GO
		 * mode, no global bump — cr_relgen_slot_current reads the locator's CURRENT
		 * gen vs the slot's install stamp).  The image may be stale for the new
		 * (epoch, rel_gen); release the reservation (FREE, generation++) without
		 * installing so the RESERVED slot does not leak (spec-5.51). */
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

/* ---- spec-5.56 D5: lifecycle counters ---- */
CR_POOL_COUNTER_ACCESSOR(cluster_cr_pool_global_epoch_fallback_bump_count,
						 global_epoch_fallback_bump_count)
CR_POOL_COUNTER_ACCESSOR(cluster_cr_pool_rel_gen_bump_count, rel_gen_bump_count)
CR_POOL_COUNTER_ACCESSOR(cluster_cr_pool_rel_gen_table_overflow_count, rel_gen_table_overflow_count)
CR_POOL_COUNTER_ACCESSOR(cluster_cr_pool_retention_horizon_advance_noted_count,
						 retention_horizon_advance_noted_count)
CR_POOL_COUNTER_ACCESSOR(cluster_cr_pool_reconfig_intra_survived_count,
						 reconfig_intra_survived_count)

/*
 * cluster_cr_pool_note_retention_horizon_advance -- spec-5.56 D2 (C3): a CR
 * lookup observed the undo retention horizon having advanced past a cached
 * image's read_scn.  PURE OBSERVATION — NOT an invalidation.  The materialized
 * image is a key pure function (AD-006 / INV-R1), so recycling the SOURCE undo
 * does not rewrite the already-materialized bytes; the image stays correct.  This
 * only quantifies that the retention-recycle scenario really occurs (the §3.2
 * proof's negative-probe evidence).  No-op when the pool is disabled.
 */
void
cluster_cr_pool_note_retention_horizon_advance(void)
{
	if (CRPool != NULL)
		pg_atomic_fetch_add_u64(&CRPool->retention_horizon_advance_noted_count, 1);
}

/*
 * cluster_cr_pool_note_reconfig_intra_survived -- spec-5.56 D3 (C4): unit/
 * injection evidence that an own-instance or merged-materialized-remote image
 * survived a membership reconfig epoch advance (INV-C1/C2: reconfig changes
 * membership / lock ownership, NOT own / durable-materialized undo, and read_scn
 * is a global SCN not a membership epoch).  No-op when the pool is disabled.
 */
void
cluster_cr_pool_note_reconfig_intra_survived(void)
{
	if (CRPool != NULL)
		pg_atomic_fetch_add_u64(&CRPool->reconfig_intra_survived_count, 1);
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
