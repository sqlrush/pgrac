/*-------------------------------------------------------------------------
 *
 * cluster_cr_admit.h
 *	  pgrac shared CR buffer pool admission policy (spec-5.52).
 *
 *	  A backend-local, advisory admission gate layered on TOP of the spec-5.51
 *	  frozen shared CR pool substrate.  It answers ONE question at the pool's
 *	  insert side: "should this just-keyed CR image be reserve+publish'd into the
 *	  shared L2 pool, or bypassed (bare-construct into L1 only)?"  It is a pure
 *	  membership heuristic: it NEVER decides which image is served to the caller
 *	  and NEVER participates in a visibility verdict.
 *
 *	  8.A negative obligations (spec-5.52 §3.1, correctness lives entirely in the
 *	  spec-5.51 substrate; admission only ever affects hit/miss):
 *	   - INV-A1: a reject/bypass serves the SAME bare-constructed image as a
 *	     disabled pool (byte-identical); admission adds no served-image path.
 *	   - INV-A2: admission acts only on the insert side; the L2 lookup_copy gate
 *	     (key-equal + epoch + copy-out + generation) is never bypassed/weakened.
 *	   - INV-A3: admission metadata (scan-kind / churn hash / pressure gauge /
 *	     relcap counts) is advisory; the visibility path never reads it.  Corrupt
 *	     or stale admission state can only mis-admit / mis-bypass (hit/miss),
 *	     never mis-serve.
 *	   - INV-A4: correctness is orthogonal to policy.  Pool disabled
 *	     (size=0/epoch=0, independent of policy) == spec-3.10.  admit_all ==
 *	     spec-5.51 v1.  no_admit stops new inserts but already-pooled entries are
 *	     still served through the full substrate gate (!= 3.10 unless the pool is
 *	     already empty).  Every policy value is correct.
 *
 *	  All state is backend-local: no shmem region, no LWLock on the hot path
 *	  (spec-5.52 §2.4, rule 16).  catversion is not bumped.
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
 *	  src/include/cluster/cluster_cr_admit.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CR_ADMIT_H
#define CLUSTER_CR_ADMIT_H

#ifndef FRONTEND

#include "postgres.h"

#include "cluster/cluster_cr_cache.h" /* ClusterCRCacheKey */

/*
 * ClusterCRAdmitPolicy -- GUC cluster.cr_pool_admission_policy (spec-5.52 D8).
 *   The pool's true on/off is the spec-5.51 size_blocks/epoch (orthogonal); this
 *   policy only governs whether NEW images are inserted, never lookups.
 */
typedef enum ClusterCRAdmitPolicy {
	CR_ADMIT_ALL = 0,		 /* DEFAULT: spec-5.51 v1 populate-on-construct */
	CR_ADMIT_NO_ADMIT,		 /* stop inserting new entries (NOT a pool disable) */
	CR_ADMIT_SCAN_RESISTANT, /* opt-in: scan-kind/volatile/relcap/pressure bypass */
} ClusterCRAdmitPolicy;

extern int cluster_cr_pool_admission_policy;		   /* enum-backing GUC int */
extern int cluster_cr_pool_admit_relation_backend_cap; /* per-backend per-rel cap; 0=off */
extern int cluster_cr_pool_admit_pressure_ratio;	   /* evict:hit*100 threshold; 0=off */

/*
 * ClusterCRScanKind -- backend-local scan-kind hint (spec-5.52 D3).  POINT is
 *   the default (admittable); BULK / PARALLEL bypass the shared pool (5.50 axis
 *   B pollution / axis C non-source).  PARALLEL is derived intrinsically from
 *   IsParallelWorker() and never set via the marker (so it cannot leak).
 */
typedef enum ClusterCRScanKind {
	CR_SCAN_POINT = 0,
	CR_SCAN_BULK,
	CR_SCAN_PARALLEL,
} ClusterCRScanKind;

/*
 * ClusterCRAdmitCtx -- per-call admission context (backend-local, hot path,
 *   no lock).  Filled by the D2 gate from cluster_cr_admit_current_scan_kind().
 *   Purely advisory: corruption only affects hit/miss (INV-A3).
 */
typedef struct ClusterCRAdmitCtx {
	ClusterCRScanKind scan_kind;
} ClusterCRAdmitCtx;

/*
 * CR_ADMIT_CHURN_SETTLE -- consecutive same-base_page_lsn observations of a page
 *   before the D4 churn detector considers it settled (admittable) rather than
 *   volatile (spec-5.52 §3.2; 5.50 CONDITION 2 first-touch churn).  Exposed so
 *   tests can reason about the settle window; exact value pending 5.58.
 */
#define CR_ADMIT_CHURN_SETTLE 2

/*
 * ClusterCRAdmitReason -- classification of the last admit() decision.  The D2
 *   gate maps this to a shared reject reason counter (spec-5.52 D9); exposed
 *   here so the (backend-local) classification is observable/unit-testable
 *   without the shared counters.  Advisory only (INV-A3).
 */
typedef enum ClusterCRAdmitReason {
	CR_ADMIT_REASON_ADMITTED = 0,
	CR_ADMIT_REASON_NO_ADMIT, /* policy == no_admit */
	CR_ADMIT_REASON_REJECT_BULK,
	CR_ADMIT_REASON_REJECT_PARALLEL,
	CR_ADMIT_REASON_REJECT_NONMAIN_FORK,
	CR_ADMIT_REASON_REJECT_VOLATILE,
	CR_ADMIT_REASON_REJECT_RELCAP,
	CR_ADMIT_REASON_REJECT_PRESSURE,
	CR_ADMIT_REASON__COUNT,
} ClusterCRAdmitReason;

/*
 * cluster_cr_pool_admit -- advisory admission predicate (spec-5.52 D1/§3.2).
 *   Returns true when the image SHOULD be reserve+publish'd into L2, false to
 *   bypass (caller bare-constructs into L1, F0-2).  policy==no_admit -> false;
 *   policy==admit_all -> true; policy==scan_resistant -> ordered checks
 *   (scan-kind, non-main-fork, volatile churn, per-backend relcap, pressure).
 *   NEVER affects correctness: any return value serves the same correct image.
 */
extern bool cluster_cr_pool_admit(const ClusterCRCacheKey *key, const ClusterCRAdmitCtx *ctx);

/* Last admit() decision classification (backend-local; for the D2 gate + tests). */
extern ClusterCRAdmitReason cluster_cr_admit_last_reason(void);

/*
 * scan-kind marker lifecycle (spec-5.52 D3).  set() returns the previous value
 *   for restore (nested scans).  The serial sequential-scan executor
 *   (nodeSeqscan.c SeqNext, under USE_PGRAC_CLUSTER) wraps each
 *   table_scan_getnextslot() fetch with set(BULK) / restore(prev) -- a
 *   per-fetch guarded marker, armed only under the scan_resistant policy and
 *   restored on every path including error via PG_FINALLY, so the marker covers
 *   exactly the CR construct that the fetch may trigger and never leaks across
 *   fetches or statements.  reset() forces POINT as a defensive belt (e.g. an
 *   AtEOXact hook) should any path ever set() without a matching restore().
 *   current() folds in IsParallelWorker() (parallel workers report PARALLEL
 *   intrinsically, without needing the marker).
 */
extern ClusterCRScanKind cluster_cr_admit_set_scan_kind(ClusterCRScanKind kind);
extern void cluster_cr_admit_restore_scan_kind(ClusterCRScanKind prev);
extern void cluster_cr_admit_reset_scan_kind(void);
extern ClusterCRScanKind cluster_cr_admit_current_scan_kind(void);

/*
 * cluster_cr_admit_note_published -- record that one image for `key`'s relation
 *   was actually admitted into L2 (per-backend relcap accounting, spec-5.52 D5).
 *   Called by the D2 gate after a successful publish.  No-op when the cap is
 *   disabled.  Advisory (INV-A3).
 */
extern void cluster_cr_admit_note_published(const ClusterCRCacheKey *key);

/* Test/diagnostic: clear all backend-local admission state (churn/relcap/pressure). */
extern void cluster_cr_admit_reset_state(void);

/*
 * Admission reason counters (spec-5.52 D9).  These live in an INDEPENDENT small
 * shmem region (cluster_cr_admit_stat.c), deliberately NOT in the spec-5.51
 * ClusterCRShared struct, so the held 5.51 substrate layout is untouched.
 * Instance-wide atomic counters for pg_cluster_state observability: the D2 gate
 * bumps the last admit() reason; the dump reads them lock-free.  Advisory; no
 * correctness role (INV-A3).
 */
extern Size cluster_cr_admit_shmem_size(void);
extern void cluster_cr_admit_shmem_init(void);
extern void cluster_cr_admit_shmem_register(void);
extern void cluster_cr_admit_stat_bump(ClusterCRAdmitReason reason);
extern uint64 cluster_cr_admit_stat_count(ClusterCRAdmitReason reason);

#endif /* !FRONTEND */

#endif /* CLUSTER_CR_ADMIT_H */
