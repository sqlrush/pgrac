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

#include "access/xact.h"				 /* PGRAC: spec-1.18 xl_xact_scn struct */
#include "cluster/cluster_conf.h"		 /* CLUSTER_MAX_NODES */
#include "cluster/cluster_ic_envelope.h" /* spec-2.9 D4:  ClusterICEnvelope + PGRAC_IC_MSG_BOC_BROADCAST */
#include "cluster/cluster_ic_router.h" /* spec-2.9 D4: ClusterICFanoutResult */
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

/* shmem / lwlock / fmgr / pg_atomic stubs.
 *
 *	spec-2.11 P1.2 修订:  原 stub 返回 NULL + *foundPtr=true → 任何对
 *	cluster_scn_state 字段的 atomic 读写都会 SEGV(NULL pointer deref).
 *	T-scn-15c 真行为测试需要 atomic fetch_add 真生效,所以扩展 stub:
 *
 *	  (i) 维护一个 per-name static buffer cache(指针稳定),首次访问
 *	      *foundPtr=false 触发 caller init path(zero-fill atomic
 *	      fields);后续访问 *foundPtr=true 保持复用
 *	  (ii) buffer 用 BSS static(zero-init by C runtime) + uint64 union
 *	      member 强制至少 8-byte alignment — 满足 LWLock /
 *	      pg_atomic_uint64 alignment 和 init 前置条件
 *	  (iii) 仅 "pgrac cluster scn" 名走真 buffer 路径;其他 region 名
 *	      retain 旧 NULL 行为(避免影响其他 spec stub 假设)
 *
 *	T-scn-15c 用法:测试体前置调 cluster_scn_shmem_init() → 触发
 *	ShmemInitStruct("pgrac cluster scn", ...) → 首次 *foundPtr=false
 *	→ cluster_scn_shmem_init body 执行 atomic init zero loop →
 *	cluster_scn_state 真指向 valid buffer → atomic ops 真生效.
 */
void *
ShmemInitStruct(const char *name, Size size, bool *foundPtr)
{
	/* spec-2.11 P1.2:  per-name persistent buffer cache for cluster_scn.
	 * Use a union instead of plain char[] so pg_atomic_uint64 and LWLock
	 * inside ClusterScnSharedState are not placed on a 1-byte-aligned
	 * address in standalone unit tests. */
	static union {
		uint64 force_align;
		char data[8192]; /* generous;  cluster_scn_shmem_size() << 8KB */
	} scn_buf;
	static bool scn_initialized = false;

	if (name != NULL && strcmp(name, "pgrac cluster scn") == 0) {
		Assert(size <= sizeof(scn_buf.data)); /* catch shmem layout growth */
		*foundPtr = scn_initialized;
		scn_initialized = true; /* subsequent calls see found=true */
		return scn_buf.data;
	}

	/* All other names:  retain spec-1.X stub behavior. */
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

/*
 * spec-2.12 D6 / T-scn-16d:  GetCurrentTimestamp stub now backed by
 * a writable test_clock_us global.  Default 0 preserves spec-1.X /
 * 2.X test semantics (existing tests do not set the global);
 * T-scn-16d sets test_clock_us before each cluster_scn_observe call
 * to drive deterministic max_gap_ms transitions.
 */
TimestampTz test_clock_us = 0;

TimestampTz
GetCurrentTimestamp(void)
{
	return test_clock_us;
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

/*
 * spec-2.9 D4 / L104 standalone-test-stub-must-cover-cross-module-call:
 *
 *	cluster_scn.c now references cluster_ic_send_envelope_fanout and
 *	cluster_cssd_get_alive_peer_count from its LMON-side BOC drain body
 *	(spec-2.9 D2 review fix).  Both symbols live in sibling cluster_ic_router.o and
 *	cluster_cssd.o respectively;  this standalone unit-test binary links
 *	cluster_scn.o only, so the cross-module refs need local stubs to
 *	satisfy the linker.
 *
 *	The LMON drain path itself is never invoked from any T-scn-13 test
 *	(handler branch is exercised directly), so the stubs can be vacuous:
 *	all peers PEER_DOWN for fanout, 0 for alive-peer-count.
 */
void
cluster_ic_send_envelope_fanout(uint8 msg_type pg_attribute_unused(),
								const void *payload pg_attribute_unused(),
								uint32 payload_len pg_attribute_unused(),
								ClusterICFanoutResult per_peer[])
{
	if (per_peer != NULL) {
		int i;

		for (i = 0; i < CLUSTER_MAX_NODES; i++)
			per_peer[i] = CLUSTER_IC_FANOUT_PEER_DOWN;
	}
}

int
cluster_cssd_get_alive_peer_count(void)
{
	return 0;
}

/*
 * spec-2.9 v0.4 D2/D3 cross-module symbol stubs (CI strict-linker fix):
 *
 *	cluster_scn.c references CritSectionCount (Q6/I8 Assert in
 *	cluster_scn_emit_broadcast_pulse;  inlined into cluster_scn_boc_tick
 *	by LTO/static visibility) and MyBackendType (Q1 Assert in
 *	cluster_scn_lmon_drain_boc_broadcast).  Linux ld + macOS strict ld
 *	require these as defined symbols at link time;  local macOS ld is
 *	more permissive and missed the gap during cluster_unit make check.
 *
 *	Standalone test binary never invokes either path, so initial values
 *	are vacuous (CritSectionCount = 0 satisfies the walwriter-not-in-crit
 *	Assert if ever exercised;  MyBackendType = B_LMON would satisfy the
 *	drain LMON-only Assert if ever exercised).
 */
#include "miscadmin.h"
volatile uint32 CritSectionCount = 0;
BackendType MyBackendType = B_LMON;

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
 * spec-2.10 D6 T-scn-14 (v0.2 scope 收紧):
 *
 *	Standalone unit binary 不能验真 GUC default (cluster_boc_sweep_interval_ms
 *	是 unit test stub 变量,改 stub != 证明真 cluster_guc.c default);不能
 *	sizeof/offsetof private ClusterScnSharedState (cluster_scn.c 内部 struct,
 *	header 0 references)。真 GUC default 验证移到 D7 TAP via SHOW.
 *
 *	保留:
 *	  T-scn-14a: cluster_scn_boc_broadcast_fanout_count 符号 linkable
 *	  T-scn-14c: cluster_scn_shmem_size() public smoke (pure function,
 *	             不依赖 cluster_scn_state)
 *
 *	删除(v0.1 → v0.2):
 *	  T-scn-14b cluster_boc_sweep_interval_ms == 100 (stub 测 stub)
 *	  T-scn-14 sizeof(ClusterScnSharedState) increase (私有 struct)
 *	  T-scn-14 offsetof layout invariant (私有 struct)
 */
UT_TEST(test_spec210_boc_broadcast_fanout_count_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_scn_boc_broadcast_fanout_count);
}

UT_TEST(test_spec210_scn_shmem_size_smoke)
{
	/* cluster_scn_shmem_size() 是 public extern (cluster_scn.h:359);
	 * 不依赖 cluster_scn_state pointer。返回值应 > 0 (struct 含 ≥ 1
	 * pg_atomic_uint64);spec-2.10 加 boc_broadcast_fanout_count 后理
	 * 论 > spec-2.9 baseline,但不在此处比较 baseline(避免 hard-code
	 * baseline value spec-by-spec 增加的脆性)。仅 smoke > 0. */
	UT_ASSERT(cluster_scn_shmem_size() > 0);
}

/*
 * spec-2.11 D6 T-scn-15:  commit_scn cross-instance lookup skeleton.
 *
 *	4 tests per spec-2.11 Q4.3 + user 修正(真行为验证):
 *	  T-scn-15a: cluster_scn_lookup_commit_remote 符号 linkable
 *	  T-scn-15b: ClusterScnLookupResult enum 4 值 invariant
 *	  T-scn-15c: 真行为 — read defer_count → call lookup → assert
 *	             DEFER + sentinel unchanged + counter +1
 *	  T-scn-15d: cluster_scn_commit_lookup_defer_count accessor linkable
 *
 *	P1.2 fix:  T-scn-15c 真行为需 cluster_scn_state 真 init.  Test 体
 *	前置调 cluster_scn_shmem_init() 触发 ShmemInitStruct("pgrac
 *	cluster scn", ...) 走 P1.2 扩展的 stub 路径(static scn_buf +
 *	首次 *foundPtr=false 触发 init zero loop).
 */
UT_TEST(test_spec211_commit_lookup_remote_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_scn_lookup_commit_remote);
}

UT_TEST(test_spec211_lookup_result_enum_invariant)
{
	/* spec-2.11 Q3.2:  FOUND=0 / DEFER=1 / NOT_FOUND=2 / ERROR=3.
	 * Catch silent renumber per L74 cross-ref grep watch.  These
	 * values are part of the lookup ABI;  bumping them silently would
	 * break future spec-2.26+ caller switch dispatch. */
	UT_ASSERT_EQ((int)CLUSTER_SCN_LOOKUP_FOUND, 0);
	UT_ASSERT_EQ((int)CLUSTER_SCN_LOOKUP_DEFER, 1);
	UT_ASSERT_EQ((int)CLUSTER_SCN_LOOKUP_NOT_FOUND, 2);
	UT_ASSERT_EQ((int)CLUSTER_SCN_LOOKUP_ERROR, 3);
}

UT_TEST(test_spec211_lookup_stub_real_behavior)
{
	/* spec-2.11 Q4.3 user 修正:  真行为验证 — 不仅 symbol linkable.
	 *
	 *	(1) read defer_count baseline
	 *	(2) mock SCN sentinel = magic value
	 *	(3) call lookup(xid, &sentinel)
	 *	(4) assert return == DEFER
	 *	(5) assert sentinel UNCHANGED (stub does not write out_commit_scn)
	 *	(6) assert defer_count == baseline + 1
	 *
	 *	P1.2 fix:  cluster_scn_state 必须 真 init 才能 atomic ops 生效.
	 *	test 前置调 cluster_scn_shmem_init() → ShmemInitStruct 走 P1.2
	 *	扩展路径 → cluster_scn_state 真指向 static scn_buf.
	 */
	uint64 pre;
	SCN sentinel = (SCN)0xDEADBEEFCAFEBABEULL;
	ClusterScnLookupResult result;

	/* P1.2:  trigger ShmemInitStruct + cluster_scn_state init. */
	cluster_scn_shmem_init();

	pre = cluster_scn_commit_lookup_defer_count();

	result = cluster_scn_lookup_commit_remote((TransactionId)123, &sentinel);

	UT_ASSERT_EQ((int)result, (int)CLUSTER_SCN_LOOKUP_DEFER);
	UT_ASSERT_EQ((uint64)sentinel, (uint64)0xDEADBEEFCAFEBABEULL);
	UT_ASSERT_EQ(cluster_scn_commit_lookup_defer_count(), pre + 1);
}

UT_TEST(test_spec211_commit_lookup_defer_count_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_scn_commit_lookup_defer_count);
}

/*
 * spec-2.12 D6 T-scn-16:  SCN convergence boundary verification.
 *
 *	4 unit tests per spec-2.12 v0.3 Q4.3 + P1.1/P2.1 修正:
 *	  T-scn-16a: 2 accessor 符号 linkable
 *	  T-scn-16b: 真行为 single observe — first observe sets
 *	             last_observe_at > 0, max_gap_ms still 0 (no prev to diff)
 *	  T-scn-16c: 真行为 max_gap update via 可控 test_clock_us + 显式递增
 *	             remote SCN (P2.1 fix:cluster_scn_observe 仅在
 *	             cur > remote 不成立时 CAS bump,必须 ascending)
 *	  T-scn-16d: L106 lock-free invariant + L104 cross-module stub
 *	             (隐式 — accessors 都已 atomic_read;test_cluster_debug.c
 *	             提供 cross-module accessor stubs)
 */

extern TimestampTz test_clock_us; /* defined above as GetCurrentTimestamp backing */

/*
 * spec-2.11 P1.2 lesson inherited:  真 GUC default 验证移到 TAP 102 L3
 * (stub-GUC == stub-value 在 unit 测无意义);unit test 只验 metric 真行为.
 * 故 T-scn-16a (GUC default) 不在 unit test 实装.
 */

UT_TEST(test_spec212_last_observe_at_accessor_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_scn_last_observe_at);
}

UT_TEST(test_spec212_observed_max_observe_gap_accessor_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_scn_observed_max_observe_gap_ms);
}

UT_TEST(test_spec212_single_observe_real_behavior)
{
	/* P1.2 fix 模式:  init shmem then drive one observe with controllable
	 * test_clock_us;  assert last_observe_at_us reflects clock value +
	 * max_gap remains 0 (first observe — no prev_ts to diff). */
	TimestampTz observe_t;

	cluster_scn_shmem_init();

	/* baseline:  fresh state */
	UT_ASSERT_EQ((uint64)cluster_scn_last_observe_at(), (uint64)0);
	UT_ASSERT_EQ(cluster_scn_observed_max_observe_gap_ms(), (uint64)0);

	/* drive 1 observe at t=1_000_000us (1ms;trivial nonzero) */
	test_clock_us = (TimestampTz)1000000;
	cluster_scn_observe((SCN)100);
	observe_t = cluster_scn_last_observe_at();

	UT_ASSERT_EQ((uint64)observe_t, (uint64)1000000);
	/* first observe — no prev → max_gap still 0 */
	UT_ASSERT_EQ(cluster_scn_observed_max_observe_gap_ms(), (uint64)0);

	/* cleanup global clock */
	test_clock_us = 0;
}

UT_TEST(test_spec212_max_gap_update_ascending_remote_scn)
{
	/* spec-2.12 Q4.4-P1 + P2.1 真验位置:max_gap_ms atomic-max update
	 * 行为唯一可控测试位置(TAP 102 不能 assert max_gap 上界).
	 *
	 *	Sequence uses the existing last_observe_at as base so the test does
	 *	not depend on prior test ordering or a fresh shmem buffer:
	 *	  Δ=(base_gap+3)ms, observe(SCN=200): max_gap grows to target
	 *	  Δ=1ms,            observe(SCN=300): max_gap unchanged
	 *	  Δ=(target+5)ms,   observe(SCN=400): max_gap grows again
	 *
	 *	Each remote SCN > prev cluster_scn current_local (since previous
	 *	observe bumped current_local to remote+1);  cluster_scn_observe
	 *	CAS bump succeeds → metric update fires.
	 */
	TimestampTz base_ts;
	uint64 base_gap;
	uint64 target_gap;
	uint64 larger_gap;

	cluster_scn_shmem_init();

	base_ts = cluster_scn_last_observe_at();
	base_gap = cluster_scn_observed_max_observe_gap_ms();
	if (base_ts == 0) {
		test_clock_us = (TimestampTz)1000000;
		cluster_scn_observe((SCN)100);
		base_ts = cluster_scn_last_observe_at();
		base_gap = cluster_scn_observed_max_observe_gap_ms();
	}
	UT_ASSERT(base_ts != 0);

	target_gap = base_gap + 3;
	larger_gap = target_gap + 5;

	/* Step 1: grow max_gap beyond whatever prior tests left behind. */
	test_clock_us = base_ts + (TimestampTz)(target_gap * 1000);
	cluster_scn_observe((SCN)200);
	UT_ASSERT_EQ(cluster_scn_observed_max_observe_gap_ms(), target_gap);

	/* Step 2:  Δ=1ms < target_gap → max_gap unchanged. */
	test_clock_us += (TimestampTz)1000;
	cluster_scn_observe((SCN)300);
	UT_ASSERT_EQ(cluster_scn_observed_max_observe_gap_ms(), target_gap);

	/* Step 3:  larger Δ updates atomic-max again. */
	test_clock_us += (TimestampTz)(larger_gap * 1000);
	cluster_scn_observe((SCN)400);
	UT_ASSERT_EQ(cluster_scn_observed_max_observe_gap_ms(), larger_gap);

	/* cleanup global clock */
	test_clock_us = 0;
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
	UT_ASSERT_EQ(sizeof(xl_xact_scn), (size_t)8);
}


/*
 * ============================================================
 * spec-2.9 D4:  T-scn-13 BOC broadcast skeleton tests
 *	Q1-Q10 frozen v0.3.  T-scn-13c (handler does NOT call
 *	cluster_scn_observe) is a static grep / code-review invariant
 *	per spec-2.9 §4.1; not a runtime UT here (cluster_scn.o standalone
 *	can't observe stubbed cluster_scn_observe coverage without breaking
 *	cluster_scn's own internal observe paths).
 * ============================================================
 */

/* T-scn-13a: handler symbol linkable (taking address). */
UT_TEST(test_spec29_boc_broadcast_handler_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_scn_boc_broadcast_handler);
}

/*
 * T-scn-13b: handler call with mock envelope succeeds and asserts
 *	payload_zero invariant (env->payload_length == 0).  Verifies
 *	receive-side runtime path is exercisable in unit test scope.
 */
UT_TEST(test_spec29_boc_broadcast_handler_payload_zero_invariant)
{
	ClusterICEnvelope env;

	memset(&env, 0, sizeof(env));
	env.magic = PGRAC_IC_ENVELOPE_MAGIC;
	env.version = PGRAC_IC_ENVELOPE_VERSION_V1;
	env.msg_type = PGRAC_IC_MSG_BOC_BROADCAST;
	env.source_node_id = 7;
	env.scn = 12345;
	env.payload_length = 0;

	/* If handler crashes on Assert(env->payload_length == 0) or env
	 * NULL guard, abort() fires via ExceptionalCondition stub.  We
	 * reach here only on success. */
	cluster_scn_boc_broadcast_handler(&env, NULL);

	/* Reached only if Assert(env != NULL) + Assert(payload_length == 0)
	 * both passed and ereport(DEBUG2) didn't crash via stubs. */
	UT_ASSERT(true);
}

/*
 * T-scn-13d: PGRAC_IC_MSG_BOC_BROADCAST enum value invariant.
 *	Catches silent renumber per L74 cross-ref grep watch.  Wire-level
 *	value frozen at 3 per spec-2.0 §4 + spec-2.9 §0 Q3.
 */
UT_TEST(test_spec29_boc_broadcast_msg_type_enum_value)
{
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_BOC_BROADCAST, 3);
}


int
main(void)
{
	UT_PLAN(41);

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

	/* Spec-2.9 D4 BOC broadcast skeleton tests (3) — T-scn-13a/b/d.
	 * T-scn-13c is a code-review invariant (handler does NOT call
	 * cluster_scn_observe); see comment block above for rationale. */
	UT_RUN(test_spec29_boc_broadcast_handler_linkable);
	UT_RUN(test_spec29_boc_broadcast_handler_payload_zero_invariant);
	UT_RUN(test_spec29_boc_broadcast_msg_type_enum_value);

	/* Spec-2.10 D6 BOC piggyback observability skeleton (2) — T-scn-14a/c.
	 * T-scn-14b/14 sizeof/14 offsetof 删除 per v0.2 P2 scope 收紧;真 GUC
	 * default 验证移到 D7 TAP 101 via SHOW cluster.boc_sweep_interval_ms. */
	UT_RUN(test_spec210_boc_broadcast_fanout_count_linkable);
	UT_RUN(test_spec210_scn_shmem_size_smoke);

	/* Spec-2.11 D6 commit_scn cross-instance lookup skeleton (4) —
	 * T-scn-15 a/b/c/d.  c 真行为验证 per Q4.3 user 修正(read counter
	 * → call → assert DEFER + sentinel unchanged + counter +1);依赖
	 * P1.2 ShmemInitStruct stub 扩展(static scn_buf + 首次 *foundPtr
	 * = false → cluster_scn_shmem_init zero loop → cluster_scn_state
	 * 真指向 valid buffer → atomic ops 真生效)。 */
	UT_RUN(test_spec211_commit_lookup_remote_linkable);
	UT_RUN(test_spec211_lookup_result_enum_invariant);
	UT_RUN(test_spec211_lookup_stub_real_behavior);
	UT_RUN(test_spec211_commit_lookup_defer_count_linkable);

	/* spec-2.12 D6 SCN convergence boundary verification (5) — T-scn-16
	 * b/c/d/e:  2 accessor linkable + single observe real behavior +
	 * max_gap update via 可控 test_clock_us + 显式递增 remote SCN (P2.1
	 * fix:cluster_scn_observe lock-free CAS only bumps when cur > remote
	 * 不成立).  T-scn-16a (GUC default) 真验证移到 TAP 102 L3 SHOW parse
	 * (spec-2.11 P1.2 lesson:stub-GUC == stub-value 在 unit 测无意义). */
	UT_RUN(test_spec212_last_observe_at_accessor_linkable);
	UT_RUN(test_spec212_observed_max_observe_gap_accessor_linkable);
	UT_RUN(test_spec212_single_observe_real_behavior);
	UT_RUN(test_spec212_max_gap_update_ascending_remote_scn);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
