/*-------------------------------------------------------------------------
 *
 * cluster_cr_admit.c
 *	  pgrac shared CR buffer pool admission policy (spec-5.52).
 *
 *	  Backend-local, advisory admission gate on the insert side of the spec-5.51
 *	  shared CR pool.  cluster_cr_pool_admit() returns whether a just-keyed CR
 *	  image should be reserve+publish'd into L2 (true) or bypassed into L1 only
 *	  (false).  Pure membership heuristic: every return value serves the SAME
 *	  correct image (correctness lives in the spec-5.51 substrate, not here).
 *	  See cluster_cr_admit.h for the four 8.A negative invariants (INV-A1..A4).
 *
 *	  All state is backend-local: a scan-kind marker, a small page-identity churn
 *	  table, a small per-relation admit-count table, and a pressure gauge that
 *	  reads the spec-5.51 shared counters.  No shmem region, no LWLock on the hot
 *	  path (rule 16).  Corruption of any of it only mis-admits / mis-bypasses
 *	  (hit/miss), never mis-serves (INV-A3).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-5.52-cr-cache-admission-policy.md (FROZEN v0.4)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cr_admit.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/parallel.h" /* IsParallelWorker */
#include "cluster/cluster_cr_admit.h"
#include "cluster/cluster_cr_pool.h" /* evict/hit counters for the pressure gauge */

/*
 * GUC-backing variables (registration in cluster_guc.c, spec-5.52 D8).  Defaults
 * reproduce spec-5.51 v1 when the pool is enabled and spec-3.10 when it is not:
 * admit_all = populate-on-construct; caps disabled.
 */
int cluster_cr_pool_admission_policy = CR_ADMIT_ALL;
int cluster_cr_pool_admit_relation_backend_cap = 0;
int cluster_cr_pool_admit_pressure_ratio = 0;

/*
 * CR_ADMIT_CHURN_SETTLE (the churn settle window, spec-5.52 D4 / 5.50 CONDITION
 * 2) is defined in cluster_cr_admit.h so tests share one definition.
 *
 * Backend-local table sizes (small, fixed, lazy-cleared; advisory).
 */
#define CR_ADMIT_CHURN_SLOTS 256
#define CR_ADMIT_RELCAP_SLOTS 64

/* ---- scan-kind marker (D3) ---- */

static ClusterCRScanKind cr_admit_scan_kind = CR_SCAN_POINT;

/* ---- last decision (D9 classification) ---- */

static ClusterCRAdmitReason cr_admit_last_reason = CR_ADMIT_REASON_ADMITTED;

/* ---- page-identity churn table (D4) ---- *
 * Key is PURE page identity {rlocator, forknum, blockno}; the value is the last
 * base_page_lsn seen for that page.  base_page_lsn is the VALUE, NEVER part of
 * the key: keying on it would make every lsn change a new entry and defeat churn
 * detection (spec-5.52 §2.4).  This is deliberately NOT a ClusterCRCacheKey.
 */
typedef struct CRAdmitChurnSlot {
	bool valid;
	RelFileLocator rlocator;
	ForkNumber forknum;
	BlockNumber blockno;
	XLogRecPtr last_lsn;
	int settle_streak;
} CRAdmitChurnSlot;

static CRAdmitChurnSlot cr_admit_churn[CR_ADMIT_CHURN_SLOTS];

/* ---- per-relation admit-count table (D5, per-backend throttle) ---- */

typedef struct CRAdmitRelcapSlot {
	bool valid;
	RelFileLocator rlocator;
	uint32 admit_count;
} CRAdmitRelcapSlot;

static CRAdmitRelcapSlot cr_admit_relcap[CR_ADMIT_RELCAP_SLOTS];

/* ---- pressure gauge (D6) ---- */

static uint64 cr_admit_prev_evict = 0;
static uint64 cr_admit_prev_hit = 0;
static uint32 cr_admit_pressure_tick = 0; /* deterministic decimation counter */

/*
 * cr_admit_mix_rlocator -- cheap hash of a RelFileLocator's three OIDs.
 */
static inline uint32
cr_admit_mix_rlocator(const RelFileLocator *rl)
{
	uint32 h = (uint32)rl->spcOid;

	h = h * 31 + (uint32)rl->dbOid;
	h = h * 31 + (uint32)rl->relNumber;
	return h;
}

/*
 * cr_admit_page_churning -- D4 volatile-page detection.  Updates the churn table
 *   for the page identity in `key` and returns true iff the page is still
 *   churning (base_page_lsn changed within the recent window, or not yet settled
 *   for CR_ADMIT_CHURN_SETTLE consecutive observations).  Advisory only.
 */
static bool
cr_admit_page_churning(const ClusterCRCacheKey *key)
{
	uint32 h = cr_admit_mix_rlocator(&key->rlocator);
	CRAdmitChurnSlot *s;

	h = h * 31 + (uint32)key->forknum;
	h = h * 31 + (uint32)key->blockno;
	s = &cr_admit_churn[h % CR_ADMIT_CHURN_SLOTS];

	if (s->valid && RelFileLocatorEquals(s->rlocator, key->rlocator) && s->forknum == key->forknum
		&& s->blockno == key->blockno) {
		if (s->last_lsn == key->base_page_lsn) {
			/* Same version observed again: count toward settling. */
			if (s->settle_streak < CR_ADMIT_CHURN_SETTLE)
				s->settle_streak++;
			return s->settle_streak < CR_ADMIT_CHURN_SETTLE;
		}

		/* Page version changed since last seen: churning. */
		s->last_lsn = key->base_page_lsn;
		s->settle_streak = 0;
		return true;
	}

	/* First touch (or slot stolen by another page): unproven -> churning. */
	s->valid = true;
	s->rlocator = key->rlocator;
	s->forknum = key->forknum;
	s->blockno = key->blockno;
	s->last_lsn = key->base_page_lsn;
	s->settle_streak = 0;
	return true;
}

/*
 * cr_admit_relcap_exceeded -- D5 per-backend per-relation throttle check.  Reads
 *   (never mutates) this backend's admit-count for the key's relation; returns
 *   true iff it is at/over the cap.  Disabled (cap<=0) -> never exceeded.
 *
 *   NB: this is a PER-BACKEND throttle, not a global pool-occupancy cap.  A
 *   backend-local module cannot observe the shared pool's global per-relation
 *   occupancy; the true global cap is forward 5.52 v2/5.58 (spec-5.52 §1.3).
 */
static bool
cr_admit_relcap_exceeded(const ClusterCRCacheKey *key)
{
	int cap = cluster_cr_pool_admit_relation_backend_cap;
	uint32 h;
	CRAdmitRelcapSlot *s;

	if (cap <= 0)
		return false;

	h = cr_admit_mix_rlocator(&key->rlocator);
	s = &cr_admit_relcap[h % CR_ADMIT_RELCAP_SLOTS];

	if (s->valid && RelFileLocatorEquals(s->rlocator, key->rlocator))
		return s->admit_count >= (uint32)cap;

	return false;
}

/*
 * cr_admit_under_pressure -- D6 clock-pressure negative feedback.  Reads the
 *   spec-5.51 evict/hit counters, computes the recent (delta) evict:hit ratio,
 *   and under pressure decimates admission deterministically (reject every other
 *   candidate).  Disabled (ratio<=0) -> never under pressure.  Advisory only;
 *   the threshold/decimation are pending 5.58 calibration.
 */
static bool
cr_admit_under_pressure(void)
{
	int thresh = cluster_cr_pool_admit_pressure_ratio;
	uint64 evict;
	uint64 hit;
	uint64 d_evict;
	uint64 d_hit;
	uint64 ratio;

	if (thresh <= 0)
		return false;

	evict = cluster_cr_pool_evict_count();
	hit = cluster_cr_pool_hit_count();
	d_evict = (evict >= cr_admit_prev_evict) ? evict - cr_admit_prev_evict : 0;
	d_hit = (hit >= cr_admit_prev_hit) ? hit - cr_admit_prev_hit : 0;
	cr_admit_prev_evict = evict;
	cr_admit_prev_hit = hit;

	ratio = (d_evict * 100) / (d_hit == 0 ? 1 : d_hit);
	if (ratio < (uint64)thresh)
		return false;

	/* Under pressure: deterministic decimation (negative admission). */
	return (cr_admit_pressure_tick++ & 1) != 0;
}

/* ---- public API ---- */

bool
cluster_cr_pool_admit(const ClusterCRCacheKey *key, const ClusterCRAdmitCtx *ctx)
{
	ClusterCRScanKind scan_kind = (ctx != NULL) ? ctx->scan_kind : CR_SCAN_POINT;

	switch (cluster_cr_pool_admission_policy) {
	case CR_ADMIT_NO_ADMIT:
		cr_admit_last_reason = CR_ADMIT_REASON_NO_ADMIT;
		return false;

	case CR_ADMIT_ALL:
		cr_admit_last_reason = CR_ADMIT_REASON_ADMITTED;
		return true;

	case CR_ADMIT_SCAN_RESISTANT:
	default:
		break;
	}

	/* scan_resistant: ordered checks, any reject -> bypass (spec-5.52 §3.2). */
	if (scan_kind == CR_SCAN_BULK) {
		cr_admit_last_reason = CR_ADMIT_REASON_REJECT_BULK;
		return false;
	}
	if (scan_kind == CR_SCAN_PARALLEL) {
		cr_admit_last_reason = CR_ADMIT_REASON_REJECT_PARALLEL;
		return false;
	}
	if (key->forknum != MAIN_FORKNUM) {
		cr_admit_last_reason = CR_ADMIT_REASON_REJECT_NONMAIN_FORK;
		return false;
	}
	if (cr_admit_page_churning(key)) {
		cr_admit_last_reason = CR_ADMIT_REASON_REJECT_VOLATILE;
		return false;
	}
	if (cr_admit_relcap_exceeded(key)) {
		cr_admit_last_reason = CR_ADMIT_REASON_REJECT_RELCAP;
		return false;
	}
	if (cr_admit_under_pressure()) {
		cr_admit_last_reason = CR_ADMIT_REASON_REJECT_PRESSURE;
		return false;
	}

	cr_admit_last_reason = CR_ADMIT_REASON_ADMITTED;
	return true;
}

ClusterCRAdmitReason
cluster_cr_admit_last_reason(void)
{
	return cr_admit_last_reason;
}

ClusterCRScanKind
cluster_cr_admit_set_scan_kind(ClusterCRScanKind kind)
{
	ClusterCRScanKind prev = cr_admit_scan_kind;

	cr_admit_scan_kind = kind;
	return prev;
}

void
cluster_cr_admit_restore_scan_kind(ClusterCRScanKind prev)
{
	cr_admit_scan_kind = prev;
}

void
cluster_cr_admit_reset_scan_kind(void)
{
	cr_admit_scan_kind = CR_SCAN_POINT;
}

ClusterCRScanKind
cluster_cr_admit_current_scan_kind(void)
{
	/* Parallel workers are intrinsically PARALLEL (no marker -> cannot leak). */
	if (IsParallelWorker())
		return CR_SCAN_PARALLEL;
	return cr_admit_scan_kind;
}

void
cluster_cr_admit_note_published(const ClusterCRCacheKey *key)
{
	uint32 h;
	CRAdmitRelcapSlot *s;

	if (cluster_cr_pool_admit_relation_backend_cap <= 0)
		return;

	h = cr_admit_mix_rlocator(&key->rlocator);
	s = &cr_admit_relcap[h % CR_ADMIT_RELCAP_SLOTS];

	if (s->valid && RelFileLocatorEquals(s->rlocator, key->rlocator)) {
		if (s->admit_count != PG_UINT32_MAX)
			s->admit_count++;
		return;
	}

	/* Claim/steal the slot for this relation (LRU-ish; advisory). */
	s->valid = true;
	s->rlocator = key->rlocator;
	s->admit_count = 1;
}

void
cluster_cr_admit_reset_state(void)
{
	cr_admit_scan_kind = CR_SCAN_POINT;
	cr_admit_last_reason = CR_ADMIT_REASON_ADMITTED;
	memset(cr_admit_churn, 0, sizeof(cr_admit_churn));
	memset(cr_admit_relcap, 0, sizeof(cr_admit_relcap));
	cr_admit_prev_evict = 0;
	cr_admit_prev_hit = 0;
	cr_admit_pressure_tick = 0;
}
