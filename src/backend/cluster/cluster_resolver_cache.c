/*-------------------------------------------------------------------------
 *
 * cluster_resolver_cache.c
 *	  pgrac shared (per-instance) resolver cache -- spec-5.55 search-shortcut
 *	  memo for the own-instance CR Source 3 WRAP_ANY by-xid durable scan.
 *
 *	  A position-hint memo: (xid_epoch, raw_xid, origin) -> {seg, slot, wrap}.
 *	  probe() looks the key up and LOCK-FREE re-validates the single hint slot
 *	  (gate (1) non-authoritative + (2) exact-(xid,wrap) reval + (4) epoch fence);
 *	  the caller (cluster_cr.c) then runs the SAME wrap_suspect acceptance gate it
 *	  runs on a fresh scan (gate (3)), so a memo hit is verdict-equivalent to the
 *	  O(segments) fresh scan it replaces.  install() caches the {seg,slot,wrap}
 *	  the fresh scan matched (own-instance gate (5) + COMMITTED gate (6), only
 *	  after acceptance passed).
 *
 *	  rule 16: the durable re-validation (header I/O) runs OUTSIDE the partition
 *	  LWLock; the lock is held only to copy the hint out (probe) or write it in
 *	  (install).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-5.55-shared-resolver-cache.md (FROZEN v1.0)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_resolver_cache.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/transam.h" /* ReadNextFullTransactionId, EpochFromFullTransactionId */
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"

#include "cluster/cluster_conf.h" /* cluster_node_id */
#include "cluster/cluster_guc.h"  /* cluster_undo_retention_horizon_enabled */
#include "cluster/cluster_resolver_cache.h"
#include "cluster/cluster_shmem.h"		/* ClusterShmemRegion */
#include "cluster/cluster_tt_durable.h" /* CLUSTER_TT_WRAP_ANY, lookup */
#include "cluster/cluster_tt_slot.h"

/* ============================================================
 * GUCs (spec-5.55 D7; registered in cluster_guc.c)
 * ============================================================ */
int cluster_shared_resolver_cache_entries = 0;
bool cluster_resolver_cache_enabled = false;
bool cluster_resolver_cache_measure = false;

#define RESOLVER_CACHE_LWLOCK_TRANCHE "ClusterResolverCache"
#define RESOLVER_CACHE_NPART 8
#define RESOLVER_CACHE_HARD_CAP (1 << 20) /* 1,048,576 hint slots */

#define RESOLVER_STATE_FREE 0
#define RESOLVER_STATE_VALID 1

/* ============================================================
 * On-shmem layout (spec-5.55 D3)
 * ============================================================ */

/*
 * spec-5.55 §2.1: position-hint key.  raw_xid is ambiguous across an own-instance
 * xid wraparound; xid_epoch (a SINGLE monotonic counter, D2) makes a hint from
 * epoch E a key MISS in E+1.  No XOR/multi-component combine (a correctness-
 * bearing fence must be collision-free).
 */
typedef struct ClusterResolverKey {
	uint64 xid_epoch;	   /* offset 0  (8B) -- own-instance xid wraparound epoch */
	TransactionId raw_xid; /* offset 8  (4B) -- own-node cr_xmax */
	uint16 origin_node_id; /* offset 12 (2B) -- == own node in v1 */
	uint16 _pad;		   /* offset 14 (2B) -- explicit, zeroed */
} ClusterResolverKey;
StaticAssertDecl(sizeof(ClusterResolverKey) == 16, "spec-5.55: 16-byte hint key");

typedef struct ClusterResolverSlot {
	ClusterResolverKey key;
	uint8 state; /* RESOLVER_STATE_FREE / _VALID */
	uint8 ref;	 /* clock reference bit (GO-time eviction) */
	uint16 seg;	 /* hint: matched undo_segment_id */
	uint16 slot; /* hint: matched tt_slot offset */
	uint16 wrap; /* hint: matched slot wrap (re-probe expected_wrap) */
	uint16 _pad2;
	SCN commit_scn_debug; /* observability ONLY -- never returned (gate (1)) */
} ClusterResolverSlot;
StaticAssertDecl(sizeof(ClusterResolverSlot) % 8 == 0, "spec-5.55: 8B-aligned slot");

typedef struct ClusterResolverCacheShared {
	int nslots;
	/* spec-5.55 D8 counters (read lock-free by the dump) */
	pg_atomic_uint64 lookup_count;				  /* probe() calls (region live) */
	pg_atomic_uint64 key_present_count;			  /* full key (epoch+xid+origin) matched */
	pg_atomic_uint64 epoch_miss_count;			  /* raw_xid+origin matched but epoch differed */
	pg_atomic_uint64 hit_count;					  /* key match + exact reval succeeded */
	pg_atomic_uint64 revalidate_miss_count;		  /* key match but exact reval failed */
	pg_atomic_uint64 acceptance_pass_count;		  /* of hits: caller's acceptance passed */
	pg_atomic_uint64 acceptance_failclosed_count; /* of hits: caller's acceptance fail-closed */
	pg_atomic_uint64 install_count;				  /* hints installed / refreshed */
	pg_atomic_uint64 evict_count;				  /* install overwrote a different live key */
	pg_atomic_uint64 nonown_skip_count;			  /* install gate (5): non-own origin rejected */
	pg_atomic_uint64 nonterminal_skip_count; /* install gate (6): non-COMMITTED (structural 0 v1) */
	ClusterResolverSlot slots[FLEXIBLE_ARRAY_MEMBER];
} ClusterResolverCacheShared;

static ClusterResolverCacheShared *ResolverCache = NULL;
static LWLockPadded *resolver_cache_locks = NULL;

/* ============================================================
 * Sizing / lifecycle
 * ============================================================ */

static int
resolver_cache_effective_nslots(void)
{
	int n;

	/* allocate when EITHER mode is on (entries > 0); off in both = zero memory. */
	if (!(cluster_resolver_cache_enabled || cluster_resolver_cache_measure)
		|| cluster_shared_resolver_cache_entries <= 0)
		return 0;
	n = cluster_shared_resolver_cache_entries;
	if (n > RESOLVER_CACHE_HARD_CAP)
		n = RESOLVER_CACHE_HARD_CAP;
	return n;
}

Size
cluster_resolver_cache_shmem_size(void)
{
	int n = resolver_cache_effective_nslots();

	if (n == 0)
		return 0; /* registered but zero bytes when disabled */
	return MAXALIGN(offsetof(ClusterResolverCacheShared, slots)
					+ (Size)n * sizeof(ClusterResolverSlot));
}

void
cluster_resolver_cache_request_lwlocks(void)
{
	/* entries / modes are PGC_POSTMASTER so the value is final at this phase. */
	if (resolver_cache_effective_nslots() > 0)
		RequestNamedLWLockTranche(RESOLVER_CACHE_LWLOCK_TRANCHE, RESOLVER_CACHE_NPART);
}

void
cluster_resolver_cache_shmem_init(void)
{
	int n = resolver_cache_effective_nslots();
	bool found;
	Size sz = cluster_resolver_cache_shmem_size();

	if (n == 0 || sz == 0) {
		/* Disabled: no region, every probe()/install() is a safe no-op. */
		ResolverCache = NULL;
		resolver_cache_locks = NULL;
		return;
	}

	ResolverCache
		= (ClusterResolverCacheShared *)ShmemInitStruct("ClusterResolverCache", sz, &found);
	resolver_cache_locks = GetNamedLWLockTranche(RESOLVER_CACHE_LWLOCK_TRANCHE);

	if (!found) {
		int i;

		ResolverCache->nslots = n;
		pg_atomic_init_u64(&ResolverCache->lookup_count, 0);
		pg_atomic_init_u64(&ResolverCache->key_present_count, 0);
		pg_atomic_init_u64(&ResolverCache->epoch_miss_count, 0);
		pg_atomic_init_u64(&ResolverCache->hit_count, 0);
		pg_atomic_init_u64(&ResolverCache->revalidate_miss_count, 0);
		pg_atomic_init_u64(&ResolverCache->acceptance_pass_count, 0);
		pg_atomic_init_u64(&ResolverCache->acceptance_failclosed_count, 0);
		pg_atomic_init_u64(&ResolverCache->install_count, 0);
		pg_atomic_init_u64(&ResolverCache->evict_count, 0);
		pg_atomic_init_u64(&ResolverCache->nonown_skip_count, 0);
		pg_atomic_init_u64(&ResolverCache->nonterminal_skip_count, 0);
		for (i = 0; i < n; i++) {
			memset(&ResolverCache->slots[i].key, 0, sizeof(ClusterResolverKey));
			ResolverCache->slots[i].state = RESOLVER_STATE_FREE;
			ResolverCache->slots[i].ref = 0;
			ResolverCache->slots[i].seg = 0;
			ResolverCache->slots[i].slot = 0;
			ResolverCache->slots[i].wrap = 0;
			ResolverCache->slots[i]._pad2 = 0;
			ResolverCache->slots[i].commit_scn_debug = InvalidScn;
		}
	}
}

static const ClusterShmemRegion cluster_resolver_cache_region = {
	.name = "pgrac cluster resolver cache",
	.size_fn = cluster_resolver_cache_shmem_size,
	.init_fn = cluster_resolver_cache_shmem_init,
	.lwlock_count = RESOLVER_CACHE_NPART, /* informational; requested separately */
	.owner_subsys = "cluster_resolver_cache",
	.reserved_flags = 0,
};

void
cluster_resolver_cache_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_resolver_cache_region);
}

bool
cluster_resolver_cache_trust(void)
{
	return cluster_resolver_cache_enabled && ResolverCache != NULL;
}

/* ============================================================
 * spec-5.55 D2: own-instance xid-wraparound fence (single monotonic counter)
 * ============================================================ */

uint64
cluster_resolver_xid_epoch(void)
{
	/*
	 * The own node's FullTransactionId epoch.  pgrac segments the xid space by
	 * (origin_node_id, xid) compound key (cluster_remote_xact), NOT by value-
	 * range, so a raw 32-bit xid is reused on the own node only once per 2^32 own
	 * xids -- exactly one increment of this epoch.  install + probe both read the
	 * CURRENT epoch; a hit therefore requires the epoch to be unchanged since
	 * install (< 2^32 xids elapsed), within which the raw xid is unique and a
	 * previous-epoch incarnation cannot still be retained (retention << 2^32).
	 */
	return (uint64)EpochFromFullTransactionId(ReadNextFullTransactionId());
}

/* ============================================================
 * Hashing / partition mapping (direct-mapped).  The slot index is keyed by
 * (raw_xid, origin) ONLY -- NOT xid_epoch -- so a given raw xid always maps to
 * the same slot regardless of epoch; the epoch is then matched on the slot's key
 * (gate (4)), making a cross-epoch hint an OBSERVABLE epoch_miss rather than a
 * silent absent miss.  lowbias32 (Chris Wellons) spreads small consecutive xids
 * well (a plain FNV word-at-a-time barely moves the low bits, causing false
 * eviction storms).
 * ============================================================ */

static inline uint32
resolver_mix32(uint32 x)
{
	x ^= x >> 16;
	x *= 0x7feb352du;
	x ^= x >> 15;
	x *= 0x846ca68bu;
	x ^= x >> 16;
	return x;
}

static inline uint32
resolver_key_hash(TransactionId raw_xid, uint16 origin)
{
	return resolver_mix32((uint32)raw_xid) ^ (resolver_mix32((uint32)origin) * 0x9e3779b1u);
}

static inline int
resolver_slot_idx(TransactionId raw_xid, uint16 origin)
{
	return (int)(resolver_key_hash(raw_xid, origin) % (uint32)ResolverCache->nslots);
}

static inline LWLock *
resolver_idx_lock(int idx)
{
	return &resolver_cache_locks[idx % RESOLVER_CACHE_NPART].lock;
}

/* ============================================================
 * spec-5.55 D4: probe (lock-free reval; candidate only, NO acceptance)
 * ============================================================ */

bool
cluster_resolver_cache_probe(uint16 origin_node, TransactionId raw_xid, SCN *out_scn)
{
	uint64 epoch;
	int idx;
	LWLock *lk;
	bool present = false;
	bool raw_match_wrong_epoch = false;
	uint16 h_seg = 0;
	uint16 h_slot = 0;
	uint16 h_wrap = 0;
	SCN probe_scn = InvalidScn;

	if (out_scn != NULL)
		*out_scn = InvalidScn;
	if (ResolverCache == NULL || out_scn == NULL)
		return false; /* off -> no count, byte-identical */
	if (!TransactionIdIsNormal(raw_xid))
		return false;

	pg_atomic_fetch_add_u64(&ResolverCache->lookup_count, 1);

	epoch = cluster_resolver_xid_epoch();
	idx = resolver_slot_idx(raw_xid, origin_node);
	lk = resolver_idx_lock(idx);

	/* copy the hint out under a SHARED lock, then release (rule 16). */
	LWLockAcquire(lk, LW_SHARED);
	{
		const ClusterResolverSlot *s = &ResolverCache->slots[idx];

		if (s->state == RESOLVER_STATE_VALID && s->key.raw_xid == raw_xid
			&& s->key.origin_node_id == origin_node) {
			if (s->key.xid_epoch == epoch) {
				present = true; /* gate (4): full key match */
				h_seg = s->seg;
				h_slot = s->slot;
				h_wrap = s->wrap;
			} else
				raw_match_wrong_epoch = true; /* gate (4): cross-epoch -> miss */
		}
	}
	LWLockRelease(lk);

	if (!present) {
		if (raw_match_wrong_epoch)
			pg_atomic_fetch_add_u64(&ResolverCache->epoch_miss_count, 1);
		return false;
	}

	pg_atomic_fetch_add_u64(&ResolverCache->key_present_count, 1);

	/*
	 * gate (1)+(2): LOCK-FREE exact (xid, wrap) re-validation.  The returned scn
	 * is a fresh durable re-read (never the cached commit_scn_debug).  A recycled
	 * or wrap-bumped slot fails the lookup -> the hint is stale -> the caller
	 * re-scans (a pure search shortcut).
	 */
	if (!cluster_tt_slot_durable_lookup(h_seg, h_slot, raw_xid, h_wrap, &probe_scn)) {
		pg_atomic_fetch_add_u64(&ResolverCache->revalidate_miss_count, 1);
		return false;
	}

	pg_atomic_fetch_add_u64(&ResolverCache->hit_count, 1);
	*out_scn = probe_scn;
	return true;
}

/* ============================================================
 * spec-5.55 D5: install (gated; own-instance + COMMITTED + post-acceptance)
 * ============================================================ */

void
cluster_resolver_cache_install(uint16 origin_node, TransactionId raw_xid, uint16 seg, uint16 slot,
							   uint16 wrap, SCN commit_scn)
{
	uint64 epoch;
	int idx;
	LWLock *lk;

	if (ResolverCache == NULL)
		return;
	if (!TransactionIdIsNormal(raw_xid))
		return;
	/* gate (5): own-instance only.  CR Source 3 resolves via the own node, so a
	 * non-own origin is a programming error -- never cached. */
	if ((int)origin_node != cluster_node_id) {
		pg_atomic_fetch_add_u64(&ResolverCache->nonown_skip_count, 1);
		return;
	}

	epoch = cluster_resolver_xid_epoch();
	idx = resolver_slot_idx(raw_xid, origin_node);
	lk = resolver_idx_lock(idx);

	LWLockAcquire(lk, LW_EXCLUSIVE);
	{
		ClusterResolverSlot *s = &ResolverCache->slots[idx];

		/* direct-mapped: overwriting a different live key is an eviction. */
		if (s->state == RESOLVER_STATE_VALID
			&& !(s->key.xid_epoch == epoch && s->key.raw_xid == raw_xid
				 && s->key.origin_node_id == origin_node))
			pg_atomic_fetch_add_u64(&ResolverCache->evict_count, 1);

		s->key.xid_epoch = epoch;
		s->key.raw_xid = raw_xid;
		s->key.origin_node_id = origin_node;
		s->key._pad = 0;
		s->state = RESOLVER_STATE_VALID;
		s->ref = 1;
		s->seg = seg;
		s->slot = slot;
		s->wrap = wrap;
		s->_pad2 = 0;
		s->commit_scn_debug = commit_scn;
	}
	LWLockRelease(lk);

	pg_atomic_fetch_add_u64(&ResolverCache->install_count, 1);
}

void
cluster_resolver_cache_count_acceptance(bool passed)
{
	if (ResolverCache == NULL)
		return;
	if (passed)
		pg_atomic_fetch_add_u64(&ResolverCache->acceptance_pass_count, 1);
	else
		pg_atomic_fetch_add_u64(&ResolverCache->acceptance_failclosed_count, 1);
}

/* ============================================================
 * Counters (lock-free reads)
 * ============================================================ */

#define RESOLVER_CACHE_COUNTER(fn, field)                                                          \
	uint64 fn(void)                                                                                \
	{                                                                                              \
		if (ResolverCache == NULL)                                                                 \
			return 0;                                                                              \
		return pg_atomic_read_u64(&ResolverCache->field);                                          \
	}

RESOLVER_CACHE_COUNTER(cluster_resolver_cache_lookup_count, lookup_count)
RESOLVER_CACHE_COUNTER(cluster_resolver_cache_key_present_count, key_present_count)
RESOLVER_CACHE_COUNTER(cluster_resolver_cache_epoch_miss_count, epoch_miss_count)
RESOLVER_CACHE_COUNTER(cluster_resolver_cache_hit_count, hit_count)
RESOLVER_CACHE_COUNTER(cluster_resolver_cache_revalidate_miss_count, revalidate_miss_count)
RESOLVER_CACHE_COUNTER(cluster_resolver_cache_acceptance_pass_count, acceptance_pass_count)
RESOLVER_CACHE_COUNTER(cluster_resolver_cache_acceptance_failclosed_count,
					   acceptance_failclosed_count)
RESOLVER_CACHE_COUNTER(cluster_resolver_cache_install_count, install_count)
RESOLVER_CACHE_COUNTER(cluster_resolver_cache_evict_count, evict_count)
RESOLVER_CACHE_COUNTER(cluster_resolver_cache_nonown_skip_count, nonown_skip_count)
RESOLVER_CACHE_COUNTER(cluster_resolver_cache_nonterminal_skip_count, nonterminal_skip_count)

int
cluster_resolver_cache_live_entries(void)
{
	int live = 0;
	int i;

	if (ResolverCache == NULL)
		return 0;
	for (i = 0; i < ResolverCache->nslots; i++) {
		LWLock *lk = resolver_idx_lock(i);

		LWLockAcquire(lk, LW_SHARED);
		if (ResolverCache->slots[i].state == RESOLVER_STATE_VALID)
			live++;
		LWLockRelease(lk);
	}
	return live;
}
