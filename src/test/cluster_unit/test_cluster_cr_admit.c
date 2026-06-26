/*-------------------------------------------------------------------------
 *
 * test_cluster_cr_admit.c
 *	  pgrac spec-5.52 — cluster_unit tests for the backend-local CR pool
 *	  admission predicate + scan-kind marker lifecycle + page-identity churn
 *	  detector + per-backend relcap throttle + clock-pressure gauge
 *	  (cluster_cr_admit.c).
 *
 *	  Links cluster_cr_admit.o standalone; stubs the three external symbols it
 *	  references: ParallelWorkerNumber (IsParallelWorker) and the spec-5.51
 *	  evict/hit counter accessors (read only by the pressure gauge).
 *
 *	  Scope note (honest): U3 (served-image byte-identical) and U17 (admit does
 *	  not change lookup) are properties of the D2 integration gate + the
 *	  spec-5.51 lookup path, NOT of this backend-local predicate, so they are
 *	  covered by the gate unit / TAP, not here.  L2/L3 cross-backend behavior is
 *	  cluster-only (nightly), inherited from spec-5.51 H2.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-5.52-cr-cache-admission-policy.md (FROZEN v0.4)
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cr_admit.c
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stdlib.h>
#include <string.h>

#include "cluster/cluster_cr_admit.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* ---- stubs for the three external symbols cluster_cr_admit.o references ---- */

int ParallelWorkerNumber = -1; /* IsParallelWorker() == false by default */

static uint64 stub_evict = 0;
static uint64 stub_hit = 0;

uint64
cluster_cr_pool_evict_count(void)
{
	return stub_evict;
}

uint64
cluster_cr_pool_hit_count(void)
{
	return stub_hit;
}

/* ---- helpers ---- */

static ClusterCRCacheKey
mk_key(Oid rel, ForkNumber fork, BlockNumber blk, XLogRecPtr lsn)
{
	ClusterCRCacheKey key;

	memset(&key, 0, sizeof(key));
	key.rlocator.spcOid = 1663;
	key.rlocator.dbOid = 5;
	key.rlocator.relNumber = rel;
	key.forknum = fork;
	key.blockno = blk;
	key.base_page_lsn = lsn;
	return key;
}

/* Reset all module state + GUCs + counter stubs to a known baseline. */
static void
fresh(void)
{
	cluster_cr_admit_reset_state();
	cluster_cr_pool_admission_policy = CR_ADMIT_ALL;
	cluster_cr_pool_admit_relation_backend_cap = 0;
	cluster_cr_pool_admit_pressure_ratio = 0;
	ParallelWorkerNumber = -1;
	stub_evict = 0;
	stub_hit = 0;
}

/*
 * Advance a POINT/main-fork page past the churn settle window so subsequent
 * admit() calls reach the relcap/pressure steps.  Returns the result of the
 * first post-settle admit() call.
 */
static bool
settle_and_admit(const ClusterCRCacheKey *key)
{
	ClusterCRAdmitCtx ctx = { .scan_kind = CR_SCAN_POINT };
	bool r = false;
	int i;

	for (i = 0; i <= CR_ADMIT_CHURN_SETTLE; i++)
		r = cluster_cr_pool_admit(key, &ctx);
	return r;
}

/* ===== U1-U2: policy endpoints (INV-A4 orthogonality) ===== */

UT_TEST(test_policy_no_admit_always_false)
{
	ClusterCRCacheKey key = mk_key(100, MAIN_FORKNUM, 1, 42);
	ClusterCRAdmitCtx ctx = { .scan_kind = CR_SCAN_POINT };

	fresh();
	cluster_cr_pool_admission_policy = CR_ADMIT_NO_ADMIT;
	UT_ASSERT(!cluster_cr_pool_admit(&key, &ctx));
	UT_ASSERT_EQ(cluster_cr_admit_last_reason(), CR_ADMIT_REASON_NO_ADMIT);
	/* no_admit never admits regardless of scan-kind */
	ctx.scan_kind = CR_SCAN_BULK;
	UT_ASSERT(!cluster_cr_pool_admit(&key, &ctx));
}

UT_TEST(test_policy_admit_all_always_true)
{
	ClusterCRCacheKey key = mk_key(101, MAIN_FORKNUM, 1, 42);
	ClusterCRAdmitCtx ctx = { .scan_kind = CR_SCAN_BULK }; /* even BULK admits under admit_all */

	fresh();
	cluster_cr_pool_admission_policy = CR_ADMIT_ALL;
	UT_ASSERT(cluster_cr_pool_admit(&key, &ctx));
	UT_ASSERT_EQ(cluster_cr_admit_last_reason(), CR_ADMIT_REASON_ADMITTED);
}

/* ===== U5-U7: scan_resistant scan-kind + fork guards ===== */

UT_TEST(test_scan_resistant_bulk_rejects)
{
	ClusterCRCacheKey key = mk_key(102, MAIN_FORKNUM, 1, 42);
	ClusterCRAdmitCtx ctx = { .scan_kind = CR_SCAN_BULK };

	fresh();
	cluster_cr_pool_admission_policy = CR_ADMIT_SCAN_RESISTANT;
	UT_ASSERT(!cluster_cr_pool_admit(&key, &ctx));
	UT_ASSERT_EQ(cluster_cr_admit_last_reason(), CR_ADMIT_REASON_REJECT_BULK);
}

UT_TEST(test_scan_resistant_parallel_rejects)
{
	ClusterCRCacheKey key = mk_key(103, MAIN_FORKNUM, 1, 42);
	ClusterCRAdmitCtx ctx = { .scan_kind = CR_SCAN_PARALLEL };

	fresh();
	cluster_cr_pool_admission_policy = CR_ADMIT_SCAN_RESISTANT;
	UT_ASSERT(!cluster_cr_pool_admit(&key, &ctx));
	UT_ASSERT_EQ(cluster_cr_admit_last_reason(), CR_ADMIT_REASON_REJECT_PARALLEL);
}

UT_TEST(test_scan_resistant_nonmain_fork_rejects)
{
	ClusterCRCacheKey key = mk_key(104, INIT_FORKNUM, 1, 42);
	ClusterCRAdmitCtx ctx = { .scan_kind = CR_SCAN_POINT };

	fresh();
	cluster_cr_pool_admission_policy = CR_ADMIT_SCAN_RESISTANT;
	UT_ASSERT(!cluster_cr_pool_admit(&key, &ctx));
	UT_ASSERT_EQ(cluster_cr_admit_last_reason(), CR_ADMIT_REASON_REJECT_NONMAIN_FORK);
}

/* ===== U8-U9: D4 churn (page identity, base_page_lsn is value not key) ===== */

UT_TEST(test_volatile_churn_rejects)
{
	ClusterCRAdmitCtx ctx = { .scan_kind = CR_SCAN_POINT };
	int i;

	fresh();
	cluster_cr_pool_admission_policy = CR_ADMIT_SCAN_RESISTANT;

	/* A page whose base_page_lsn changes every touch never settles. */
	for (i = 0; i < 6; i++) {
		ClusterCRCacheKey key = mk_key(105, MAIN_FORKNUM, 1, 1000 + i);

		UT_ASSERT(!cluster_cr_pool_admit(&key, &ctx));
		UT_ASSERT_EQ(cluster_cr_admit_last_reason(), CR_ADMIT_REASON_REJECT_VOLATILE);
	}
}

UT_TEST(test_churn_settles_then_admits)
{
	ClusterCRCacheKey key = mk_key(106, MAIN_FORKNUM, 7, 2000);
	ClusterCRAdmitCtx ctx = { .scan_kind = CR_SCAN_POINT };
	int i;
	bool last;

	fresh();
	cluster_cr_pool_admission_policy = CR_ADMIT_SCAN_RESISTANT;

	/* The first CR_ADMIT_CHURN_SETTLE touches are volatile (settling). */
	for (i = 0; i < CR_ADMIT_CHURN_SETTLE; i++) {
		UT_ASSERT(!cluster_cr_pool_admit(&key, &ctx));
		UT_ASSERT_EQ(cluster_cr_admit_last_reason(), CR_ADMIT_REASON_REJECT_VOLATILE);
	}
	/* Once the same lsn has been seen enough, the page admits. */
	last = cluster_cr_pool_admit(&key, &ctx);
	UT_ASSERT(last);
	UT_ASSERT_EQ(cluster_cr_admit_last_reason(), CR_ADMIT_REASON_ADMITTED);
}

/* ===== U10-U11: D5 per-backend relcap throttle ===== */

UT_TEST(test_relcap_throttle_over_cap_rejects)
{
	ClusterCRCacheKey key = mk_key(107, MAIN_FORKNUM, 3, 3000);
	ClusterCRAdmitCtx ctx = { .scan_kind = CR_SCAN_POINT };

	fresh();
	cluster_cr_pool_admission_policy = CR_ADMIT_SCAN_RESISTANT;
	cluster_cr_pool_admit_relation_backend_cap = 2;

	/* Settle the page so we reach the relcap step. */
	UT_ASSERT(settle_and_admit(&key));

	/* Simulate two real admits into L2 for this relation. */
	cluster_cr_admit_note_published(&key);
	cluster_cr_admit_note_published(&key);

	/* Now this backend is at the cap -> reject_relcap. */
	UT_ASSERT(!cluster_cr_pool_admit(&key, &ctx));
	UT_ASSERT_EQ(cluster_cr_admit_last_reason(), CR_ADMIT_REASON_REJECT_RELCAP);
}

UT_TEST(test_relcap_disabled_never_throttles)
{
	ClusterCRCacheKey key = mk_key(108, MAIN_FORKNUM, 3, 4000);

	fresh();
	cluster_cr_pool_admission_policy = CR_ADMIT_SCAN_RESISTANT;
	cluster_cr_pool_admit_relation_backend_cap = 0; /* disabled */

	UT_ASSERT(settle_and_admit(&key));
	/* note_published is a no-op when disabled; admit stays true. */
	cluster_cr_admit_note_published(&key);
	cluster_cr_admit_note_published(&key);
	cluster_cr_admit_note_published(&key);
	UT_ASSERT(settle_and_admit(&key));
	UT_ASSERT_EQ(cluster_cr_admit_last_reason(), CR_ADMIT_REASON_ADMITTED);
}

/* ===== U12-U13: D6 clock-pressure gauge ===== */

UT_TEST(test_pressure_high_evict_ratio_decimates)
{
	ClusterCRCacheKey key = mk_key(109, MAIN_FORKNUM, 9, 5000);
	ClusterCRAdmitCtx ctx = { .scan_kind = CR_SCAN_POINT };
	int pressure_rejects = 0;
	int i;

	fresh();
	cluster_cr_pool_admission_policy = CR_ADMIT_SCAN_RESISTANT;
	cluster_cr_pool_admit_pressure_ratio = 50; /* evict:hit >= 50% */

	UT_ASSERT(settle_and_admit(&key)); /* settle past churn */

	/* Keep evict climbing far faster than hit -> sustained high pressure. */
	for (i = 0; i < 8; i++) {
		stub_evict += 100;
		stub_hit += 10;
		if (!cluster_cr_pool_admit(&key, &ctx)
			&& cluster_cr_admit_last_reason() == CR_ADMIT_REASON_REJECT_PRESSURE)
			pressure_rejects++;
	}
	UT_ASSERT(pressure_rejects > 0);
}

UT_TEST(test_pressure_disabled_never_rejects)
{
	ClusterCRCacheKey key = mk_key(110, MAIN_FORKNUM, 9, 6000);
	ClusterCRAdmitCtx ctx = { .scan_kind = CR_SCAN_POINT };
	int i;

	fresh();
	cluster_cr_pool_admission_policy = CR_ADMIT_SCAN_RESISTANT;
	cluster_cr_pool_admit_pressure_ratio = 0; /* disabled */

	UT_ASSERT(settle_and_admit(&key));
	for (i = 0; i < 8; i++) {
		stub_evict += 1000;
		stub_hit += 1;
		UT_ASSERT(cluster_cr_pool_admit(&key, &ctx));
		UT_ASSERT_EQ(cluster_cr_admit_last_reason(), CR_ADMIT_REASON_ADMITTED);
	}
}

/* ===== U15-U16: D3 scan-kind marker lifecycle ===== */

UT_TEST(test_scan_marker_set_restore_nesting)
{
	ClusterCRScanKind outer;
	ClusterCRScanKind inner;

	fresh();
	UT_ASSERT_EQ(cluster_cr_admit_current_scan_kind(), CR_SCAN_POINT);

	/* outer bulk scan */
	outer = cluster_cr_admit_set_scan_kind(CR_SCAN_BULK);
	UT_ASSERT_EQ(outer, CR_SCAN_POINT);
	UT_ASSERT_EQ(cluster_cr_admit_current_scan_kind(), CR_SCAN_BULK);

	/* nested point sub-scan saves + restores the outer bulk kind */
	inner = cluster_cr_admit_set_scan_kind(CR_SCAN_POINT);
	UT_ASSERT_EQ(inner, CR_SCAN_BULK);
	UT_ASSERT_EQ(cluster_cr_admit_current_scan_kind(), CR_SCAN_POINT);
	cluster_cr_admit_restore_scan_kind(inner);
	UT_ASSERT_EQ(cluster_cr_admit_current_scan_kind(), CR_SCAN_BULK);

	cluster_cr_admit_restore_scan_kind(outer);
	UT_ASSERT_EQ(cluster_cr_admit_current_scan_kind(), CR_SCAN_POINT);
}

UT_TEST(test_scan_marker_abort_reset_no_leak)
{
	fresh();
	/* A bulk scan set the marker, then ereport'd before restore. */
	(void)cluster_cr_admit_set_scan_kind(CR_SCAN_BULK);
	UT_ASSERT_EQ(cluster_cr_admit_current_scan_kind(), CR_SCAN_BULK);

	/* AtEOXact abort cleanup must force POINT (no BULK leak into next stmt). */
	cluster_cr_admit_reset_scan_kind();
	UT_ASSERT_EQ(cluster_cr_admit_current_scan_kind(), CR_SCAN_POINT);
}

UT_TEST(test_current_scan_kind_parallel_intrinsic)
{
	fresh();
	/* Marker says POINT, but a parallel worker reports PARALLEL intrinsically. */
	UT_ASSERT_EQ(cluster_cr_admit_current_scan_kind(), CR_SCAN_POINT);
	ParallelWorkerNumber = 3; /* IsParallelWorker() == true */
	UT_ASSERT_EQ(cluster_cr_admit_current_scan_kind(), CR_SCAN_PARALLEL);
	ParallelWorkerNumber = -1;
	UT_ASSERT_EQ(cluster_cr_admit_current_scan_kind(), CR_SCAN_POINT);
}

/* ===== U18: GUC policy switch takes effect immediately ===== */

UT_TEST(test_policy_switch_immediate)
{
	ClusterCRCacheKey key = mk_key(111, MAIN_FORKNUM, 1, 7000);
	ClusterCRAdmitCtx ctx = { .scan_kind = CR_SCAN_BULK };

	fresh();
	cluster_cr_pool_admission_policy = CR_ADMIT_ALL;
	UT_ASSERT(cluster_cr_pool_admit(&key, &ctx)); /* admit_all: BULK still admitted */

	cluster_cr_pool_admission_policy = CR_ADMIT_SCAN_RESISTANT;
	UT_ASSERT(!cluster_cr_pool_admit(&key, &ctx)); /* immediately: BULK bypassed */

	cluster_cr_pool_admission_policy = CR_ADMIT_NO_ADMIT;
	UT_ASSERT(!cluster_cr_pool_admit(&key, &ctx));
}

/* ===== U4 (partial, INV-A3): arbitrary state never crashes / always boolean ===== */

UT_TEST(test_corrupt_state_safe_boolean)
{
	ClusterCRCacheKey key = mk_key(112, MAIN_FORKNUM, 1, 8000);
	bool r;

	fresh();
	cluster_cr_pool_admission_policy = CR_ADMIT_SCAN_RESISTANT;
	/* NULL ctx defaults to POINT, must not crash. */
	r = cluster_cr_pool_admit(&key, NULL);
	UT_ASSERT(r == true || r == false);
	/* reason is always a hit/miss classification, never out of range. */
	UT_ASSERT(cluster_cr_admit_last_reason() < CR_ADMIT_REASON__COUNT);
}

int
main(void)
{
	UT_PLAN(16);
	UT_RUN(test_policy_no_admit_always_false);
	UT_RUN(test_policy_admit_all_always_true);
	UT_RUN(test_scan_resistant_bulk_rejects);
	UT_RUN(test_scan_resistant_parallel_rejects);
	UT_RUN(test_scan_resistant_nonmain_fork_rejects);
	UT_RUN(test_volatile_churn_rejects);
	UT_RUN(test_churn_settles_then_admits);
	UT_RUN(test_relcap_throttle_over_cap_rejects);
	UT_RUN(test_relcap_disabled_never_throttles);
	UT_RUN(test_pressure_high_evict_ratio_decimates);
	UT_RUN(test_pressure_disabled_never_rejects);
	UT_RUN(test_scan_marker_set_restore_nesting);
	UT_RUN(test_scan_marker_abort_reset_no_leak);
	UT_RUN(test_current_scan_kind_parallel_intrinsic);
	UT_RUN(test_policy_switch_immediate);
	UT_RUN(test_corrupt_state_safe_boolean);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
