/*-------------------------------------------------------------------------
 *
 * test_cluster_resolver_cache.c
 *	  spec-5.55 unit tests for the shared resolver cache module
 *	  (cluster_resolver_cache.o), with malloc-backed shmem + no-op LWLock stubs
 *	  and a MOCKED cluster_tt_slot_durable_lookup (the gate (2) re-validation) +
 *	  MOCKED ReadNextFullTransactionId (the gate (4) own-instance xid epoch).
 *
 *	  Covers the module-local gates: (1) memo non-authoritative (probe returns the
 *	  mocked DURABLE scn, never the cached commit_scn_debug), (2) exact-(xid,wrap)
 *	  re-validation (probe passes hint.wrap; a lookup miss -> revalidate_miss),
 *	  (4) xid_epoch fence (a hint installed at epoch E is an epoch_miss in E+1),
 *	  (5) own-instance install gate, plus the counter taxonomy, eviction, the
 *	  trust predicate and the entries==0 zero-memory no-op.  gate (3) (the
 *	  acceptance same-segment rerun) lives in cluster_cr.c (caller side), so it is
 *	  split across two homes: its VERDICT LOGIC is the pure
 *	  cluster_tt_recovery_wrap_suspect gate, unit-tested by
 *	  test_cluster_tt_durable.c (the test_wrap_suspect_* matrix); its memo-path
 *	  fail-closed BRANCH is exercised by the cluster_tap test (315 L5, injection-
 *	  driven -- the natural workload cannot reach a wrap-suspect memo hit).  The
 *	  16-byte key / 8-byte-aligned slot StaticAsserts are enforced at
 *	  cluster_resolver_cache.o compile time.
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
 *	  src/test/cluster_unit/test_cluster_resolver_cache.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdlib.h>
#include <string.h>

#include "access/transam.h" /* FullTransactionId */
#include "cluster/cluster_resolver_cache.h"
#include "cluster/cluster_shmem.h"		/* ClusterShmemRegion */
#include "cluster/cluster_tt_durable.h" /* cluster_tt_slot_durable_lookup proto */
#include "storage/lwlock.h"
#include "storage/shmem.h" /* ShmemInitStruct proto */

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

/* own node id referenced by the install gate (5). */
int cluster_node_id = 0;

/* ---- shmem + LWLock stubs (single-threaded; fresh region per init) ---- */
void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *foundPtr)
{
	void *p = malloc(size);

	if (p != NULL)
		memset(p, 0, size);
	if (foundPtr != NULL)
		*foundPtr = false;
	return p;
}

static LWLockPadded stub_locks[64];

LWLockPadded *
GetNamedLWLockTranche(const char *tranche_name pg_attribute_unused())
{
	return stub_locks;
}

void
RequestNamedLWLockTranche(const char *tranche_name pg_attribute_unused(),
						  int num_lwlocks pg_attribute_unused())
{}

bool
LWLockAcquire(LWLock *l pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}

void
LWLockRelease(LWLock *l pg_attribute_unused())
{}

void
cluster_shmem_register_region(const ClusterShmemRegion *r pg_attribute_unused())
{}

/* ---- mock: gate (4) own-instance xid epoch ---- */
static uint64 mock_epoch = 1;

FullTransactionId
ReadNextFullTransactionId(void)
{
	FullTransactionId f;

	/* epoch in the high 32 bits; low bits are an arbitrary live xid. */
	f.value = (mock_epoch << 32) | 0x1000u;
	return f;
}

/* ---- mock: gate (2) durable exact re-validation ---- */
static bool mock_lookup_result = true; /* slot still binds (xid,wrap)? */
static SCN mock_lookup_scn = 0;		   /* the DURABLE scn returned on a hit */
static int mock_lookup_calls = 0;
static uint16 mock_lookup_seg = 0;
static uint16 mock_lookup_slot = 0;
static uint32 mock_lookup_wrap = 0;
static TransactionId mock_lookup_xid = 0;

bool
cluster_tt_slot_durable_lookup(uint32 segment_id, uint16 slot_offset, TransactionId xid,
							   uint32 expected_wrap, SCN *commit_scn)
{
	mock_lookup_calls++;
	mock_lookup_seg = (uint16)segment_id;
	mock_lookup_slot = slot_offset;
	mock_lookup_wrap = expected_wrap;
	mock_lookup_xid = xid;
	if (!mock_lookup_result)
		return false;
	if (commit_scn != NULL)
		*commit_scn = mock_lookup_scn;
	return true;
}

/* ---- helpers ---- */

static void
rc_init(int entries, bool measure, bool enabled)
{
	cluster_shared_resolver_cache_entries = entries;
	cluster_resolver_cache_measure = measure;
	cluster_resolver_cache_enabled = enabled;
	cluster_resolver_cache_shmem_init();
}

static void
mock_reset(void)
{
	mock_epoch = 1;
	mock_lookup_result = true;
	mock_lookup_scn = 0;
	mock_lookup_calls = 0;
	mock_lookup_seg = mock_lookup_slot = 0;
	mock_lookup_wrap = 0;
	mock_lookup_xid = 0;
}

/* =====================================================================
 * Tests
 * ===================================================================== */

/* U: entries == 0 -> true zero memory; probe/install are no-ops, counters 0. */
UT_TEST(test_off_entries0_noop)
{
	SCN got = 0;

	mock_reset();
	rc_init(0, true, true); /* entries 0 dominates -> no region */
	UT_ASSERT_EQ((int)cluster_resolver_cache_probe(0, 12345, &got), 0);
	cluster_resolver_cache_install(0, 12345, 1, 2, 3, scn_encode(1, 50));
	UT_ASSERT_EQ((int)cluster_resolver_cache_lookup_count(), 0);
	UT_ASSERT_EQ((int)cluster_resolver_cache_install_count(), 0);
	UT_ASSERT_EQ(cluster_resolver_cache_live_entries(), 0);
	UT_ASSERT_EQ((int)cluster_resolver_cache_trust(), 0); /* no region -> not trusting */
}

/* U2 + U3 (gate 1): a hit returns the DURABLE re-read scn, never commit_scn_debug. */
UT_TEST(test_probe_hit_returns_durable_not_debug)
{
	SCN got = 0;

	mock_reset();
	rc_init(64, false, true);
	mock_epoch = 5;
	/* install with a "debug" scn of 100 */
	cluster_resolver_cache_install(0, 777, 3, 7, 2, scn_encode(1, 100));
	/* the durable slot now reads a DIFFERENT scn (200) */
	mock_lookup_result = true;
	mock_lookup_scn = scn_encode(1, 200);
	UT_ASSERT_EQ((int)cluster_resolver_cache_probe(0, 777, &got), 1);
	UT_ASSERT_EQ((int)scn_local(got), 200); /* gate 1: durable, not the cached 100 */
	UT_ASSERT_EQ((int)cluster_resolver_cache_hit_count(), 1);
	UT_ASSERT_EQ((int)cluster_resolver_cache_key_present_count(), 1);
	UT_ASSERT_EQ((int)cluster_resolver_cache_lookup_count(), 1);
}

/* U (gate 2): probe re-validates with the HINT WRAP (concrete), not WRAP_ANY. */
UT_TEST(test_probe_uses_hint_wrap)
{
	SCN got = 0;

	mock_reset();
	rc_init(64, false, true);
	mock_epoch = 5;
	cluster_resolver_cache_install(0, 888, 4, 9, 6 /* wrap */, scn_encode(1, 10));
	mock_lookup_scn = scn_encode(1, 10);
	(void)cluster_resolver_cache_probe(0, 888, &got);
	UT_ASSERT_EQ((int)mock_lookup_seg, 4);
	UT_ASSERT_EQ((int)mock_lookup_slot, 9);
	UT_ASSERT_EQ((int)mock_lookup_wrap, 6); /* hint.wrap, concrete */
	UT_ASSERT_EQ((int)mock_lookup_xid, 888);
}

/* U6 (gate 2): a recycled / wrap-bumped slot fails re-validation -> revalidate_miss. */
UT_TEST(test_probe_revalidate_miss)
{
	SCN got = 0;

	mock_reset();
	rc_init(64, false, true);
	mock_epoch = 5;
	cluster_resolver_cache_install(0, 999, 1, 1, 1, scn_encode(1, 10));
	mock_lookup_result = false; /* slot recycled */
	UT_ASSERT_EQ((int)cluster_resolver_cache_probe(0, 999, &got), 0);
	UT_ASSERT_EQ((int)SCN_VALID(got), 0);
	UT_ASSERT_EQ((int)cluster_resolver_cache_key_present_count(), 1);
	UT_ASSERT_EQ((int)cluster_resolver_cache_revalidate_miss_count(), 1);
	UT_ASSERT_EQ((int)cluster_resolver_cache_hit_count(), 0);
}

/* U7 (gate 4): a hint from epoch E is an epoch_miss in E+1 (before raw_xid reuse). */
UT_TEST(test_epoch_fence_miss)
{
	SCN got = 0;

	mock_reset();
	rc_init(64, false, true);
	mock_epoch = 5;
	cluster_resolver_cache_install(0, 4242, 2, 2, 2, scn_encode(1, 50));
	mock_epoch = 6; /* own-instance xid wraparound */
	UT_ASSERT_EQ((int)cluster_resolver_cache_probe(0, 4242, &got), 0);
	UT_ASSERT_EQ((int)cluster_resolver_cache_epoch_miss_count(), 1);
	UT_ASSERT_EQ((int)cluster_resolver_cache_key_present_count(), 0); /* never a key match */
	UT_ASSERT_EQ((int)cluster_resolver_cache_hit_count(), 0);
	UT_ASSERT_EQ((int)mock_lookup_calls, 0); /* no reval on an epoch miss */
}

/* U (gate 4 control): same epoch -> the hint is a key match. */
UT_TEST(test_epoch_same_hit)
{
	SCN got = 0;

	mock_reset();
	rc_init(64, false, true);
	mock_epoch = 9;
	cluster_resolver_cache_install(0, 4242, 2, 2, 2, scn_encode(1, 50));
	mock_lookup_scn = scn_encode(1, 50);
	UT_ASSERT_EQ((int)cluster_resolver_cache_probe(0, 4242, &got), 1);
	UT_ASSERT_EQ((int)cluster_resolver_cache_hit_count(), 1);
}

/* U8 (gate 4): the fence is the RAW epoch (single field, no XOR combine). */
UT_TEST(test_xid_epoch_is_raw_counter)
{
	mock_reset();
	mock_epoch = 42;
	UT_ASSERT_EQ((int)cluster_resolver_xid_epoch(), 42);
	mock_epoch = 43;
	UT_ASSERT_EQ((int)cluster_resolver_xid_epoch(), 43);
	/* a single monotonic counter: epoch N+1 differs from N by exactly 1, so no
	 * two distinct epochs can collide (unlike an epoch<<1 XOR generation). */
	UT_ASSERT_EQ((int)(cluster_resolver_xid_epoch() != (uint64)42), 1);
}

/* U10 (gate 5): install with a non-own origin is rejected (nonown_skip). */
UT_TEST(test_install_nonown_skip)
{
	SCN got = 0;

	mock_reset();
	rc_init(64, false, true);
	mock_epoch = 5;
	cluster_node_id = 0;
	cluster_resolver_cache_install(1 /* != own 0 */, 555, 1, 1, 1, scn_encode(1, 10));
	UT_ASSERT_EQ((int)cluster_resolver_cache_nonown_skip_count(), 1);
	UT_ASSERT_EQ((int)cluster_resolver_cache_install_count(), 0);
	UT_ASSERT_EQ(cluster_resolver_cache_live_entries(), 0);
	/* the non-own origin was never cached */
	UT_ASSERT_EQ((int)cluster_resolver_cache_probe(1, 555, &got), 0);
}

/* U: probe of a never-installed xid -> lookup counted, no key match, no reval. */
UT_TEST(test_probe_absent_key)
{
	SCN got = 0;

	mock_reset();
	rc_init(64, false, true);
	mock_epoch = 5;
	UT_ASSERT_EQ((int)cluster_resolver_cache_probe(0, 31337, &got), 0);
	UT_ASSERT_EQ((int)cluster_resolver_cache_lookup_count(), 1);
	UT_ASSERT_EQ((int)cluster_resolver_cache_key_present_count(), 0);
	UT_ASSERT_EQ((int)cluster_resolver_cache_epoch_miss_count(), 0);
	UT_ASSERT_EQ((int)mock_lookup_calls, 0);
}

/* U: direct-mapped eviction -- a different key overwriting a live slot bumps evict. */
UT_TEST(test_evict_on_collision)
{
	mock_reset();
	rc_init(1, false, true); /* 1 slot -> every key maps to idx 0 */
	mock_epoch = 5;
	cluster_resolver_cache_install(0, 100, 1, 1, 1, scn_encode(1, 10));
	UT_ASSERT_EQ((int)cluster_resolver_cache_evict_count(), 0);			/* fresh slot */
	cluster_resolver_cache_install(0, 200, 2, 2, 2, scn_encode(1, 20)); /* evicts xid 100 */
	UT_ASSERT_EQ((int)cluster_resolver_cache_evict_count(), 1);
	UT_ASSERT_EQ((int)cluster_resolver_cache_install_count(), 2);
}

/* U: re-installing the SAME key is a refresh, not an eviction. */
UT_TEST(test_refresh_no_evict)
{
	mock_reset();
	rc_init(1, false, true);
	mock_epoch = 5;
	cluster_resolver_cache_install(0, 100, 1, 1, 1, scn_encode(1, 10));
	cluster_resolver_cache_install(0, 100, 1, 1, 1, scn_encode(1, 11)); /* same key */
	UT_ASSERT_EQ((int)cluster_resolver_cache_evict_count(), 0);
	UT_ASSERT_EQ((int)cluster_resolver_cache_install_count(), 2);
}

/* U: live_entries counts distinct installed slots. */
UT_TEST(test_live_entries)
{
	mock_reset();
	rc_init(64, false, true);
	mock_epoch = 5;
	/* normal xids only (>= FirstNormalTransactionId == 3); a real deleter xid is
	 * always normal, and install guards non-normal xids. */
	cluster_resolver_cache_install(0, 10, 1, 1, 1, scn_encode(1, 1));
	cluster_resolver_cache_install(0, 20, 1, 1, 1, scn_encode(1, 2));
	cluster_resolver_cache_install(0, 30, 1, 1, 1, scn_encode(1, 3));
	UT_ASSERT_EQ((int)cluster_resolver_cache_install_count(), 3);
	UT_ASSERT_EQ(cluster_resolver_cache_live_entries(), 3);
}

/* U: trust predicate reflects (enabled && region live). */
UT_TEST(test_trust_predicate)
{
	mock_reset();
	rc_init(64, true, false); /* measure-only -> region live but not trusting */
	UT_ASSERT_EQ((int)cluster_resolver_cache_trust(), 0);
	rc_init(64, false, true); /* enabled -> trusting */
	UT_ASSERT_EQ((int)cluster_resolver_cache_trust(), 1);
	rc_init(0, false, true); /* enabled but no region -> not trusting */
	UT_ASSERT_EQ((int)cluster_resolver_cache_trust(), 0);
}

/* U: install / probe of an abnormal (non-normal) xid is a guarded no-op. */
UT_TEST(test_abnormal_xid_guarded)
{
	SCN got = 0;

	mock_reset();
	rc_init(64, false, true);
	mock_epoch = 5;
	cluster_resolver_cache_install(0, InvalidTransactionId, 1, 1, 1, scn_encode(1, 10));
	UT_ASSERT_EQ((int)cluster_resolver_cache_install_count(), 0);
	UT_ASSERT_EQ((int)cluster_resolver_cache_probe(0, InvalidTransactionId, &got), 0);
	UT_ASSERT_EQ((int)cluster_resolver_cache_lookup_count(), 0); /* guarded before count */
}

/* U: the acceptance-outcome counter splits pass vs fail-closed. */
UT_TEST(test_count_acceptance)
{
	mock_reset();
	rc_init(64, false, true);
	cluster_resolver_cache_count_acceptance(true);
	cluster_resolver_cache_count_acceptance(true);
	cluster_resolver_cache_count_acceptance(false);
	UT_ASSERT_EQ((int)cluster_resolver_cache_acceptance_pass_count(), 2);
	UT_ASSERT_EQ((int)cluster_resolver_cache_acceptance_failclosed_count(), 1);
}

/* U: nonterminal_skip is structurally 0 in v1 (COMMITTED enforced at the single
 * RESOLVED_SCN call site in cluster_cr.c; install never sees a non-terminal). */
UT_TEST(test_nonterminal_skip_structural_zero)
{
	mock_reset();
	rc_init(64, false, true);
	mock_epoch = 5;
	cluster_resolver_cache_install(0, 10, 1, 1, 1, scn_encode(1, 1));
	UT_ASSERT_EQ((int)cluster_resolver_cache_nonterminal_skip_count(), 0);
}

/* U: a full cycle accounts lookup = key_present + (absent + epoch_miss). */
UT_TEST(test_counter_accounting)
{
	SCN got = 0;

	mock_reset();
	rc_init(64, false, true);
	mock_epoch = 5;
	cluster_resolver_cache_install(0, 10, 1, 1, 1, scn_encode(1, 10));
	mock_lookup_scn = scn_encode(1, 10);
	(void)cluster_resolver_cache_probe(0, 10, &got); /* hit */
	(void)cluster_resolver_cache_probe(0, 11, &got); /* absent */
	mock_epoch = 6;
	(void)cluster_resolver_cache_probe(0, 10, &got); /* epoch miss */
	UT_ASSERT_EQ((int)cluster_resolver_cache_lookup_count(), 3);
	UT_ASSERT_EQ((int)cluster_resolver_cache_key_present_count(), 1);
	UT_ASSERT_EQ((int)cluster_resolver_cache_hit_count(), 1);
	UT_ASSERT_EQ((int)cluster_resolver_cache_epoch_miss_count(), 1);
}

/* U: measure mode also allocates the region and counts (without a trust decision). */
UT_TEST(test_measure_mode_region_live)
{
	SCN got = 0;

	mock_reset();
	rc_init(64, true, false); /* measure, not enabled */
	mock_epoch = 5;
	cluster_resolver_cache_install(0, 71, 1, 1, 1, scn_encode(1, 7));
	mock_lookup_scn = scn_encode(1, 7);
	UT_ASSERT_EQ((int)cluster_resolver_cache_probe(0, 71, &got), 1); /* probe still works */
	UT_ASSERT_EQ((int)cluster_resolver_cache_hit_count(), 1);
	UT_ASSERT_EQ((int)cluster_resolver_cache_trust(), 0); /* but not trusting */
}

int
main(int argc pg_attribute_unused(), char **argv pg_attribute_unused())
{
	UT_PLAN(18);
	UT_RUN(test_off_entries0_noop);
	UT_RUN(test_probe_hit_returns_durable_not_debug);
	UT_RUN(test_probe_uses_hint_wrap);
	UT_RUN(test_probe_revalidate_miss);
	UT_RUN(test_epoch_fence_miss);
	UT_RUN(test_epoch_same_hit);
	UT_RUN(test_xid_epoch_is_raw_counter);
	UT_RUN(test_install_nonown_skip);
	UT_RUN(test_probe_absent_key);
	UT_RUN(test_evict_on_collision);
	UT_RUN(test_refresh_no_evict);
	UT_RUN(test_live_entries);
	UT_RUN(test_trust_predicate);
	UT_RUN(test_abnormal_xid_guarded);
	UT_RUN(test_count_acceptance);
	UT_RUN(test_nonterminal_skip_structural_zero);
	UT_RUN(test_counter_accounting);
	UT_RUN(test_measure_mode_region_live);
	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
