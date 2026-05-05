/*-------------------------------------------------------------------------
 *
 * test_cluster_scn.c
 *	  Compile-time + link-time invariants for the SCN typedef + encoding
 *	  layer.
 *
 *	  Stage 1.4 stub (5 tests, preserved): typedef size 8B + InvalidScn = 0
 *	  + SCN_VALID rejects 0 / accepts non-zero + SCN_FORMAT non-empty.
 *
 *	  Spec-1.15 encoding layer (12 tests, new): bit layout (8 + 56) +
 *	  scn_encode roundtrip + scn_local / scn_node_id extraction +
 *	  SCN_NODE_ID_VALID three-segment classification + wraparound
 *	  thresholds + total_cmp / time_cmp tie-break semantics + recovery_cmp
 *	  three-level tie-break.
 *
 *	  cmp functions (scn_time_cmp / scn_total_cmp / scn_recovery_cmp) are
 *	  pure functions; this binary links cluster_scn.o with PG-backend
 *	  symbols stubbed.  Runtime advance / observe paths (LWLock,
 *	  shmem, GetCurrentTimestamp) are NOT exercised here -- they live in
 *	  TAP t/065_scn_encoding.pl where postmaster orchestration is real.
 *
 *	  Spec: spec-1.15-scn-encoding-layer.md §4 cluster_unit list (12 UT)
 *	  Design: docs/scn-protocol-design.md v1.1 §3.1 + §3.2.1
 *	  AD-008 (SCN protocol; Lamport-style distributed counters).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_scn.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Each test_*.c is a standalone executable; see unit_test.h.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <string.h>

#include "access/xact.h"		/* PGRAC: spec-1.18 xl_xact_scn struct */
#include "cluster/cluster_scn.h"
#include "port/atomics.h"
#include "storage/lwlock.h"

/* Drop PG's port.h printf -> pg_printf override; unit_test.h uses
 * stdlib printf and we don't link libpgport in this test binary. */
#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"


/* ----------
 * Stubs needed to link cluster_scn.o standalone.  Runtime paths
 * (advance/observe with LWLock + shmem + ereport) are not exercised
 * here; only the pure-function cmp layer is invoked.
 * ----------
 */

bool IsUnderPostmaster = false;

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

bool
errstart(int e pg_attribute_unused(), const char *d pg_attribute_unused())
{
	return false;
}
bool
errstart_cold(int e pg_attribute_unused(), const char *d pg_attribute_unused())
{
	return false;
}
void
errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		  const char *fn pg_attribute_unused())
{}
int
errcode(int s pg_attribute_unused())
{
	return 0;
}
int
errmsg(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errmsg_internal(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errdetail(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errhint(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
void
elog_start(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		   const char *fn pg_attribute_unused())
{}
void
elog_finish(int e pg_attribute_unused(), const char *f pg_attribute_unused(), ...)
{}

/* shmem / lwlock / fmgr / pg_atomic stubs (advance/observe path; never
 * invoked in this binary -- address-only / pure-function tests). */
void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size pg_attribute_unused(),
				bool *foundPtr)
{
	*foundPtr = true;
	return NULL;
}
void
RequestAddinShmemSpace(Size s pg_attribute_unused())
{}
void
LWLockInitialize(LWLock *l pg_attribute_unused(), int t pg_attribute_unused())
{}
bool
LWLockAcquire(LWLock *l pg_attribute_unused(), LWLockMode m pg_attribute_unused())
{
	return true;
}
void
LWLockRelease(LWLock *l pg_attribute_unused())
{}

TimestampTz
GetCurrentTimestamp(void)
{
	return 0;
}

NodeId cluster_node_id = 0; /* GUC backing store mock */
/* Spec-1.16 Hardening v1.0.1 (round 9 P1 finding 1): cluster_scn skip
 * helper now references cluster_enabled.  Pin to true so unit-test
 * advance/observe paths aren't silently skipped (behavior tests live
 * in TAP 066 L12). */
bool cluster_enabled = true;

/* superuser stub for SQL UDF wrappers (never called in this binary). */
bool
superuser(void)
{
	return true;
}

/* injection stub */
bool
cluster_injection_should_skip(const char *p pg_attribute_unused())
{
	return false;
}
void
cluster_injection_check(const char *p pg_attribute_unused())
{}

/* fmgr stub for SQL UDFs (address-only) */
struct FunctionCallInfoBaseData;

/* injection extras (cluster_scn.c references run + armed_count) */
pg_atomic_uint32 cluster_injection_armed_count;
void
cluster_injection_run(const char *p pg_attribute_unused())
{}
bool
cluster_injection_should_skip_full(const char *p pg_attribute_unused())
{
	return false;
}

/* timestamp helper used by wraparound watermark logging */
bool
TimestampDifferenceExceeds(TimestampTz a pg_attribute_unused(), TimestampTz b pg_attribute_unused(),
						   int ms pg_attribute_unused())
{
	return false;
}

/* spec-1.17 D2: TimestampDifference stub for boc_tick throttle. */
void
TimestampDifference(TimestampTz a pg_attribute_unused(), TimestampTz b pg_attribute_unused(),
					long *secs, int *usecs)
{
	*secs = 0;
	*usecs = 0;
}

/* spec-1.17 D4: cluster_boc_sweep_interval_ms GUC backing var. */
int cluster_boc_sweep_interval_ms = 1;

/* shmem region registry stub (advance/observe path NOT exercised) */
void
cluster_shmem_register_region(const void *r pg_attribute_unused())
{}

UT_DEFINE_GLOBALS();


/* ============================================================
 * Stage 1.4 stub tests (preserved unchanged)
 * ============================================================
 */

UT_TEST(test_scn_typedef_size_is_8_bytes)
{
	UT_ASSERT_EQ(sizeof(SCN), 8);
}


UT_TEST(test_invalid_scn_is_zero)
{
	UT_ASSERT_EQ((int)InvalidScn, 0);
}


UT_TEST(test_scn_valid_macro_rejects_zero)
{
	SCN zero = 0;

	UT_ASSERT_EQ((int)SCN_VALID(InvalidScn), 0);
	UT_ASSERT_EQ((int)SCN_VALID(zero), 0);
}


UT_TEST(test_scn_valid_macro_accepts_nonzero)
{
	SCN one = 1;
	SCN big = (SCN)1 << 56;

	UT_ASSERT(SCN_VALID(one));
	UT_ASSERT(SCN_VALID(big));
}


UT_TEST(test_scn_format_macro_produces_nonempty_string)
{
	const char *fmt = SCN_FORMAT;
	SCN test_val = 12345;
	unsigned long arg = SCN_FORMAT_ARG(test_val);

	UT_ASSERT_NOT_NULL((void *)fmt);
	UT_ASSERT(strlen(fmt) > 0);
	UT_ASSERT_EQ((int)arg, 12345);
}


/* ============================================================
 * Spec-1.15 encoding layer tests (NEW)
 * ============================================================
 */

UT_TEST(test_scn_bit_layout_invariants)
{
	/* 8 + 56 = 64 (full width) */
	UT_ASSERT_EQ(SCN_NODE_ID_BITS + SCN_LOCAL_BITS, 64);
	UT_ASSERT_EQ(SCN_NODE_ID_SHIFT, 56);
	UT_ASSERT_EQ(SCN_INVARIANT_SIZE, 8);
	/* SCN_LOCAL_MASK fills 56 low bits */
	UT_ASSERT_EQ(SCN_LOCAL_MASK, (uint64)0x00FFFFFFFFFFFFFFULL);
}


UT_TEST(test_scn_node_id_valid_three_segments)
{
	/* invalid: -1 (unset) */
	UT_ASSERT_EQ((int)SCN_NODE_ID_VALID((NodeId)-1), 0);

	/* valid: 0..127 */
	UT_ASSERT(SCN_NODE_ID_VALID((NodeId)0));
	UT_ASSERT(SCN_NODE_ID_VALID((NodeId)1));
	UT_ASSERT(SCN_NODE_ID_VALID((NodeId)127));

	/* reserved (treated as invalid for now): 128..255 */
	UT_ASSERT_EQ((int)SCN_NODE_ID_VALID((NodeId)128), 0);
	UT_ASSERT_EQ((int)SCN_NODE_ID_VALID((NodeId)255), 0);
}


UT_TEST(test_scn_encode_roundtrip)
{
	SCN s1, s2, s3;

	/* encode(0, 1) -> decode -> (0, 1) */
	s1 = scn_encode(0, 1);
	UT_ASSERT_EQ((int)scn_node_id(s1), 0);
	UT_ASSERT_EQ((int)scn_local(s1), 1);

	/* encode(127, MAX_LOCAL) -> decode roundtrip */
	s2 = scn_encode(127, SCN_MAX_LOCAL);
	UT_ASSERT_EQ((int)scn_node_id(s2), 127);
	UT_ASSERT_EQ(scn_local(s2), SCN_MAX_LOCAL);

	/* encode(42, 0xDEADBEEF) -> decode roundtrip */
	s3 = scn_encode(42, 0xDEADBEEF);
	UT_ASSERT_EQ((int)scn_node_id(s3), 42);
	UT_ASSERT_EQ((int)scn_local(s3), (int)0xDEADBEEF);
}


UT_TEST(test_scn_encode_node_id_in_high_byte)
{
	SCN s, s2;

	/* encode(1, 0) sets bit 56; node_id 1 occupies high 8 bits */
	s = scn_encode(1, 0);
	UT_ASSERT_EQ(s, (SCN)1ULL << 56);

	/* encode(127, 0) = 127 << 56 */
	s2 = scn_encode(127, 0);
	UT_ASSERT_EQ(s2, (SCN)127ULL << 56);
}


UT_TEST(test_scn_local_extraction_masks_high_bits)
{
	SCN s = scn_encode(99, 0xABCDEF);

	UT_ASSERT_EQ((int)scn_local(s), (int)0xABCDEF);
	UT_ASSERT_EQ((int)scn_node_id(s), 99);
}


UT_TEST(test_scn_wraparound_thresholds)
{
	/* Spec-1.15 L6: 2^50 WARNING / 2^55 PANIC, NOT 2^48 / 2^56 */
	UT_ASSERT_EQ(SCN_WRAP_WARNING_THRESHOLD, (uint64)1ULL << 50);
	UT_ASSERT_EQ(SCN_WRAP_PANIC_THRESHOLD, (uint64)1ULL << 55);

	/* PANIC threshold must be < SCN_MAX_LOCAL (still in 56-bit range) */
	UT_ASSERT(SCN_WRAP_PANIC_THRESHOLD < SCN_MAX_LOCAL);
	/* WARNING < PANIC */
	UT_ASSERT(SCN_WRAP_WARNING_THRESHOLD < SCN_WRAP_PANIC_THRESHOLD);
}


UT_TEST(test_scn_time_cmp_uses_local_only)
{
	SCN a, b, c, d;

	/* time_cmp ignores node_id; only local_scn matters */
	a = scn_encode(0, 100);
	b = scn_encode(127, 100); /* same local, different node */
	UT_ASSERT_EQ(scn_time_cmp(a, b), 0);

	/* a smaller local -> a < b */
	c = scn_encode(0, 99);
	d = scn_encode(0, 100);
	UT_ASSERT(scn_time_cmp(c, d) < 0);
	UT_ASSERT(scn_time_cmp(d, c) > 0);
}


UT_TEST(test_scn_total_cmp_local_first_then_node)
{
	SCN a, b, c, d, e, f;

	/*
	 * Critical: raw uint64 cmp would let high node_id bits dominate.
	 * encode(0, 200) vs encode(127, 100) -- raw cmp says 127's > 0's
	 * (high byte wins) but spec-1.15 L4 says local 200 > local 100.
	 */
	a = scn_encode(0, 200);
	b = scn_encode(127, 100);

	UT_ASSERT(scn_total_cmp(a, b) > 0); /* 200 > 100 wins */
	UT_ASSERT(scn_total_cmp(b, a) < 0);

	/* Equal local -> node_id breaks tie */
	c = scn_encode(1, 100);
	d = scn_encode(2, 100);

	UT_ASSERT(scn_total_cmp(c, d) < 0);
	UT_ASSERT(scn_total_cmp(d, c) > 0);

	/* Same node, same local -> equal */
	e = scn_encode(5, 50);
	f = scn_encode(5, 50);

	UT_ASSERT_EQ(scn_total_cmp(e, f), 0);
}


UT_TEST(test_scn_recovery_cmp_three_level_tiebreak)
{
	SCN a, b, c, d;
	XLogRecPtr lsn1 = (XLogRecPtr)0x1000;
	XLogRecPtr lsn2 = (XLogRecPtr)0x2000;

	a = scn_encode(1, 100);
	b = scn_encode(2, 100);

	/* Same local, different LSN -> LSN tie-break */
	UT_ASSERT(scn_recovery_cmp(a, lsn1, 1, b, lsn2, 2) < 0);
	UT_ASSERT(scn_recovery_cmp(b, lsn2, 2, a, lsn1, 1) > 0);

	/* Same local, same LSN -> node_id tie-break */
	UT_ASSERT(scn_recovery_cmp(a, lsn1, 1, b, lsn1, 2) < 0);
	UT_ASSERT(scn_recovery_cmp(b, lsn1, 2, a, lsn1, 1) > 0);

	/* All equal -> 0 */
	UT_ASSERT_EQ(scn_recovery_cmp(a, lsn1, 1, a, lsn1, 1), 0);

	/* Different local dominates LSN */
	c = scn_encode(0, 200);
	d = scn_encode(0, 100);

	UT_ASSERT(scn_recovery_cmp(c, lsn1, 0, d, lsn2, 0) > 0);
}


UT_TEST(test_scn_total_cmp_invariant_no_raw_uint64_wins)
{
	/*
	 * Regression for spec-1.15 L4: total_cmp MUST NOT delegate to raw
	 * uint64 comparison.  This test pins the contract: a node_id-1 SCN
	 * with local_scn=200 must outrank a node_id-127 SCN with local_scn=100,
	 * even though the raw 64-bit values rank the latter higher.
	 */
	SCN low_node_high_local = scn_encode(1, 200);
	SCN high_node_low_local = scn_encode(127, 100);
	uint64 raw_low = (uint64)low_node_high_local;
	uint64 raw_high = (uint64)high_node_low_local;

	/* Raw uint64 cmp would say raw_high > raw_low (127 in high byte > 1) */
	UT_ASSERT(raw_high > raw_low);

	/* But total_cmp must say low_node_high_local > high_node_low_local */
	UT_ASSERT(scn_total_cmp(low_node_high_local, high_node_low_local) > 0);
}


UT_TEST(test_scn_invalid_remains_zero_under_encoding)
{
	SCN zero, s;

	/* InvalidScn = 0 must still pass SCN_VALID rejection under encoding */
	zero = scn_encode(0, 0);
	UT_ASSERT_EQ(zero, (SCN)0);
	UT_ASSERT_EQ((int)SCN_VALID(zero), 0);

	/* Any non-zero local with node 0 is valid */
	s = scn_encode(0, 1);
	UT_ASSERT(SCN_VALID(s));
}


UT_TEST(test_scn_node_id_extracts_unsigned_byte)
{
	SCN s, s2;

	/*
	 * scn_node_id casts via uint8 to avoid sign-extension surprises.
	 * Encoding stores node 127 as 0x7F in high byte; decoding must
	 * return 127, not -1.
	 */
	s = scn_encode(127, 0);
	UT_ASSERT_EQ((int)scn_node_id(s), 127);

	/* node 0 returns 0 (no sign issues) */
	s2 = scn_encode(0, 42);
	UT_ASSERT_EQ((int)scn_node_id(s2), 0);
}


/* ============================================================
 * Spec-1.16 commit/abort hook + observe Lamport bump symbol tests
 * ============================================================
 *
 *	cluster_unit binary stubs ShmemInitStruct so cluster_scn_state stays
 *	NULL.  Behavior tests (advance / observe Lamport bump / commit/abort
 *	counter increment) live in TAP t/066_scn_commit_advance.pl + 065
 *	upgrade where a real PG instance backs the shmem region.  The unit
 *	tests below only verify symbol linkability.
 */

UT_TEST(test_spec116_advance_for_commit_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_scn_advance_for_commit);
}

UT_TEST(test_spec116_advance_for_abort_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_scn_advance_for_abort);
}

UT_TEST(test_spec116_commit_advance_count_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_scn_commit_advance_count);
}

UT_TEST(test_spec116_abort_advance_count_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_scn_abort_advance_count);
}

UT_TEST(test_spec116_observe_bump_count_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_scn_observe_bump_count);
}

/* spec-1.17 BOC tick + 4 stat accessor symbol-linkable tests. */
UT_TEST(test_spec117_boc_tick_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_scn_boc_tick);
}
UT_TEST(test_spec117_boc_sweep_count_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_scn_boc_sweep_count);
}
UT_TEST(test_spec117_boc_pending_at_last_sweep_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_scn_boc_pending_at_last_sweep);
}
UT_TEST(test_spec117_boc_max_batch_size_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_scn_boc_max_batch_size);
}

/*
 * spec-1.18 symbol-linkable smoke tests.
 *
 *	Real semantics (commit_scn round-tripping through xl_xact_scn,
 *	xact_redo_commit observe wiring, replay 3-layer gate) are exercised
 *	by 068_wal_xl_scn.pl TAP test which spins a real PG instance.  Unit
 *	tests just verify the linker resolves the new spec-1.18 symbols.
 */
UT_TEST(test_spec118_recovery_replay_observe_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_scn_recovery_replay_observe);
}

UT_TEST(test_spec118_xl_xact_scn_size_is_8_bytes)
{
	UT_ASSERT_EQ(sizeof(xl_xact_scn), (size_t) 8);
}


int
main(void)
{
	UT_PLAN(28);

	/* Stage 1.4 stub (5) */
	UT_RUN(test_scn_typedef_size_is_8_bytes);
	UT_RUN(test_invalid_scn_is_zero);
	UT_RUN(test_scn_valid_macro_rejects_zero);
	UT_RUN(test_scn_valid_macro_accepts_nonzero);
	UT_RUN(test_scn_format_macro_produces_nonempty_string);

	/* Spec-1.15 encoding layer (12) */
	UT_RUN(test_scn_bit_layout_invariants);
	UT_RUN(test_scn_node_id_valid_three_segments);
	UT_RUN(test_scn_encode_roundtrip);
	UT_RUN(test_scn_encode_node_id_in_high_byte);
	UT_RUN(test_scn_local_extraction_masks_high_bits);
	UT_RUN(test_scn_wraparound_thresholds);
	UT_RUN(test_scn_time_cmp_uses_local_only);
	UT_RUN(test_scn_total_cmp_local_first_then_node);
	UT_RUN(test_scn_recovery_cmp_three_level_tiebreak);
	UT_RUN(test_scn_total_cmp_invariant_no_raw_uint64_wins);
	UT_RUN(test_scn_invalid_remains_zero_under_encoding);
	UT_RUN(test_scn_node_id_extracts_unsigned_byte);

	/* Spec-1.16 commit/abort + observe Lamport hook symbols (5) */
	UT_RUN(test_spec116_advance_for_commit_linkable);
	UT_RUN(test_spec116_advance_for_abort_linkable);
	UT_RUN(test_spec116_commit_advance_count_linkable);
	UT_RUN(test_spec116_abort_advance_count_linkable);
	UT_RUN(test_spec116_observe_bump_count_linkable);

	/* Spec-1.17 BOC tick + accessor symbols (4) */
	UT_RUN(test_spec117_boc_tick_linkable);
	UT_RUN(test_spec117_boc_sweep_count_linkable);
	UT_RUN(test_spec117_boc_pending_at_last_sweep_linkable);
	UT_RUN(test_spec117_boc_max_batch_size_linkable);

	/* Spec-1.18 WAL xl_scn + replay observe wrapper (2) */
	UT_RUN(test_spec118_recovery_replay_observe_linkable);
	UT_RUN(test_spec118_xl_xact_scn_size_is_8_bytes);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
