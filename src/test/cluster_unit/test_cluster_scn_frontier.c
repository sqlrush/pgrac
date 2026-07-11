/*-------------------------------------------------------------------------
 *
 * test_cluster_scn_frontier.c
 *	  Real-behavior tests for the spec-7.4 D1 durable_safe_scn frontier
 *	  registry in cluster_scn.c.
 *
 *	  The frontier is the CONTIGUOUS durable watermark an origin may
 *	  publish on the BOC channel:  every own-origin commit SCN <= the
 *	  frontier is durable on WAL.  These tests pin the pending-registry
 *	  semantics (register at commit-SCN allocation, LSN fill-in at
 *	  commit-record insert, three-way discharge:  sync commit / walwriter
 *	  flush horizon / abort) plus the sticky overflow freeze and the
 *	  monotonic-publish guard.
 *
 *	  Linking model mirrors test_cluster_scn.c:  cluster_scn.o standalone
 *	  with PG-backend symbols stubbed;  the ShmemInitStruct stub backs
 *	  "pgrac cluster scn" with a static aligned buffer so shmem-state
 *	  paths run for real in-process.  Wire / event / TAP-level behavior
 *	  (BOC payload v1, dirty wakeup, walwriter hook) is TAP-tested.
 *
 *	  Spec: spec-7.4-commit-scn-propagation-freshness.md D1
 *	  Wire contract: spec-2.9-scn-broadcast-service-activation.md (I5 v0.5)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_scn_frontier.c
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
#include "cluster/cluster_xnode_profile.h" /* spec-5.59 D2 stub — profiling gate */
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

/* spec-5.59 D2 stubs: cluster_scn.o now carries GUC-gated profiling probes
 * (cluster_xnode_profile.h); the unit harness links neither cluster_guc.o
 * nor cluster_xnode_profile.o, so define the two gate symbols inertly
 * (probes early-return on enabled=false / Ctl=NULL). */
bool cluster_xnode_profile_enabled = false;
ClusterXnodeProfileShared *ClusterXnodeProfileCtl = NULL;

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
		char data[16384]; /* generous;  covers spec-7.4 durable_pending growth */
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
ClusterConf test_cluster_conf = {
	.magic = PGRAC_CLUSTER_CONF_MAGIC,
	.node_count = 2,
};
ClusterConf *ClusterConfShmem = &test_cluster_conf;

/* Spec-1.16 Hardening v1.0.1 (round 9 P1 finding 1): cluster_scn skip
 * helper now references cluster_enabled.  Pin to true so unit-test
 * advance/observe paths aren't silently skipped (behavior tests live
 * in TAP 066 L12). */
bool cluster_enabled = true;
bool cluster_enable_adg = false; /* spec-7.4 frontier tests: keep the ADG
 * registry quiet so durable_pending assertions are not entangled with
 * adg_pending bookkeeping (ADG real-behavior tests live in
 * test_cluster_scn.c). */

/* superuser stub for SQL UDF wrappers (never called in this binary). */
bool
superuser(void)
{
	return true;
}

/* injection stub */
/* spec-7.4 D1-2: configurable suppress-injection stub -- tests set
 * test_injection_skip_point to a point name to make the armed-state
 * peek (cluster_cr_injection_armed) report it armed.  The production
 * probe also requires cluster_injection_armed_count > 0 (fast gate),
 * so tests raise the gate for the suppression window. */
const char *test_injection_skip_point = NULL;

bool
cluster_injection_should_skip(const char *p)
{
	return test_injection_skip_point != NULL && strcmp(p, test_injection_skip_point) == 0;
}

bool
cluster_cr_injection_armed(const char *p, uint64 *out_param)
{
	if (out_param != NULL)
		*out_param = 0;
	return test_injection_skip_point != NULL && strcmp(p, test_injection_skip_point) == 0;
}
void
cluster_injection_check(const char *p pg_attribute_unused())
{}

/* fmgr stub for SQL UDFs (address-only) */
struct FunctionCallInfoBaseData;

/* injection extras (cluster_scn.c references run + armed_count; the
 * gate is a plain int in cluster_inject.h, mirror that here). */
int cluster_injection_armed_count;
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
/* spec-7.4 D1-2: capturing fanout stub -- drain tests assert on the
 * last frame's msg_type / payload bytes. */
int test_fanout_calls = 0;
uint8 test_fanout_last_msg_type = 0;
uint32 test_fanout_last_len = 0;
uint8 test_fanout_last_payload[64];

void
cluster_ic_send_envelope_fanout(uint8 msg_type, const void *payload, uint32 payload_len,
								ClusterICFanoutResult per_peer[])
{
	test_fanout_calls++;
	test_fanout_last_msg_type = msg_type;
	test_fanout_last_len = payload_len;
	memset(test_fanout_last_payload, 0, sizeof(test_fanout_last_payload));
	if (payload != NULL && payload_len > 0 && payload_len <= sizeof(test_fanout_last_payload))
		memcpy(test_fanout_last_payload, payload, payload_len);
	if (per_peer != NULL) {
		int i;

		for (i = 0; i < CLUSTER_MAX_NODES; i++)
			per_peer[i] = CLUSTER_IC_FANOUT_PEER_DOWN;
	}
}

/* spec-7.4 D1-2: configurable so drain tests can pass the zero-peer
 * short-circuit (I9). */
int test_alive_peer_count = 0;

int
cluster_cssd_get_alive_peer_count(void)
{
	return test_alive_peer_count;
}

int
cluster_conf_node_count(void)
{
	return 2;
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

/* spec-7.4 D1 / L104: cluster_scn.o boc_tick now calls GetFlushRecPtr
 * for the walwriter async-commit frontier discharge.  Standalone unit
 * binaries link cluster_scn.o only;  stub returns 0 (InvalidXLogRecPtr)
 * so discharge_upto is a guarded no-op unless a test drives the API
 * directly with explicit horizons. */
XLogRecPtr
GetFlushRecPtr(TimeLineID *insertTLI)
{
	if (insertTLI != NULL)
		*insertTLI = 0;
	return 0;
}

/* spec-7.4 D1-2: LMON wakeup stub counts SetLatch-equivalent signals. */
int test_lmon_wakeup_count = 0;

void
cluster_lmon_marker_complete_wakeup(void)
{
	test_lmon_wakeup_count++;
}

/* spec-7.4 D1-2 GUC backing var (cluster_guc.o not linked). */
bool cluster_boc_event_publish = true;


UT_DEFINE_GLOBALS();


/* ============================================================
 * Spec-7.4 D1 durable_safe_scn frontier registry tests
 * ============================================================
 *
 *	The published watermark is a CONTIGUOUS durable frontier:  every
 *	own-origin commit SCN <= frontier is durable on WAL.  A plain
 *	atomic-max of flushed commit SCNs is forbidden (P0):  with T1
 *	allocating S then stalling while T2 allocates S+1 and flushes
 *	first, max would publish S+1 and falsely claim S durable.
 *
 *	Tests share one in-process shmem buffer (ShmemInitStruct stub
 *	caches per-name);  state is cumulative across tests, so every
 *	assertion below is written against SCNs allocated locally in the
 *	same test.  The overflow test freezes the frontier STICKILY and
 *	therefore MUST run last.
 *
 *	Spec: spec-7.4-commit-scn-propagation-freshness.md D1
 *	Mini-plan: docs/stage7-74-d1-miniplan.md v1.1 §1.1 (pgrac repo)
 */

UT_TEST(test_spec74_frontier_accessors_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_scn_durable_safe_scn);
	UT_ASSERT_NOT_NULL((void *)cluster_scn_durable_pending_fill_lsn);
	UT_ASSERT_NOT_NULL((void *)cluster_scn_durable_pending_discharge_scn);
	UT_ASSERT_NOT_NULL((void *)cluster_scn_durable_pending_abort_self);
	UT_ASSERT_NOT_NULL((void *)cluster_scn_durable_pending_discharge_upto);
	UT_ASSERT_NOT_NULL((void *)cluster_scn_durable_pending_count);
	UT_ASSERT_NOT_NULL((void *)cluster_scn_durable_frontier_frozen);
	UT_ASSERT_NOT_NULL((void *)cluster_scn_durable_frontier_overflow_count);
	UT_ASSERT_NOT_NULL((void *)cluster_scn_durable_frontier_regression_count);
}

UT_TEST(test_spec74_frontier_initial_state_invalid_unfrozen)
{
	/* Real bound check:  the ShmemInitStruct stub buffer is 16KB and
	 * its Assert is compiled out in no-cassert unit builds -- keep an
	 * always-on guard against silent BSS overflow as the state struct
	 * grows. */
	UT_ASSERT(cluster_scn_shmem_size() <= 16384);

	cluster_scn_shmem_init();

	UT_ASSERT_EQ(cluster_scn_durable_safe_scn(), InvalidScn);
	UT_ASSERT_EQ(cluster_scn_durable_pending_count(), 0);
	UT_ASSERT(!cluster_scn_durable_frontier_frozen());
	UT_ASSERT_EQ(cluster_scn_durable_frontier_overflow_count(), 0);
	UT_ASSERT_EQ(cluster_scn_durable_frontier_regression_count(), 0);
}

UT_TEST(test_spec74_frontier_commit_advance_registers_pending)
{
	uint64 pre;
	SCN s;
	SCN f;

	cluster_scn_shmem_init();
	pre = cluster_scn_durable_pending_count();

	s = cluster_scn_advance_for_commit();
	UT_ASSERT(SCN_VALID(s));
	UT_ASSERT_EQ(cluster_scn_durable_pending_count(), pre + 1);

	/* An allocated-but-not-durable commit SCN must not be covered. */
	f = cluster_scn_durable_safe_scn();
	UT_ASSERT(!SCN_VALID(f) || scn_total_cmp(f, s) < 0);

	/* Clean up for the next test:  this commit never flushes. */
	UT_ASSERT(cluster_scn_durable_pending_abort_self());
	UT_ASSERT_EQ(cluster_scn_durable_pending_count(), pre);
}

UT_TEST(test_spec74_frontier_sync_discharge_advances_to_scn)
{
	SCN s;

	cluster_scn_shmem_init();

	s = cluster_scn_advance_for_commit();
	UT_ASSERT(SCN_VALID(s));

	UT_ASSERT(cluster_scn_durable_pending_fill_lsn(s, (XLogRecPtr)1000));
	UT_ASSERT(cluster_scn_durable_pending_discharge_scn(s));

	UT_ASSERT_EQ(scn_total_cmp(cluster_scn_durable_safe_scn(), s), 0);
}

UT_TEST(test_spec74_frontier_out_of_order_flush_does_not_pass_stalled)
{
	SCN s1;
	SCN s2;
	SCN f;

	cluster_scn_shmem_init();

	/* T1 allocates s1 and stalls BEFORE its commit record exists
	 * (lsn never filled).  T2 allocates s2, inserts and flushes. */
	s1 = cluster_scn_advance_for_commit();
	s2 = cluster_scn_advance_for_commit();
	UT_ASSERT(SCN_VALID(s1));
	UT_ASSERT(SCN_VALID(s2));
	UT_ASSERT(scn_total_cmp(s1, s2) < 0);

	UT_ASSERT(cluster_scn_durable_pending_fill_lsn(s2, (XLogRecPtr)2000));
	UT_ASSERT(cluster_scn_durable_pending_discharge_scn(s2));

	/* P0 heart:  the frontier must NOT pass the stalled s1. */
	f = cluster_scn_durable_safe_scn();
	UT_ASSERT(SCN_VALID(f));
	UT_ASSERT(scn_total_cmp(f, s1) < 0);

	/* T1 resumes:  record inserted at lsn 1500, walwriter flushes
	 * through 1500 -> both discharged -> frontier reaches s2. */
	UT_ASSERT(cluster_scn_durable_pending_fill_lsn(s1, (XLogRecPtr)1500));
	UT_ASSERT_EQ(cluster_scn_durable_pending_discharge_upto((XLogRecPtr)1500), 1);
	UT_ASSERT_EQ(scn_total_cmp(cluster_scn_durable_safe_scn(), s2), 0);
}

UT_TEST(test_spec74_frontier_walwriter_upto_respects_lsn_order)
{
	SCN s;
	SCN f_before;

	cluster_scn_shmem_init();

	s = cluster_scn_advance_for_commit();
	UT_ASSERT(SCN_VALID(s));
	f_before = cluster_scn_durable_safe_scn();

	UT_ASSERT(cluster_scn_durable_pending_fill_lsn(s, (XLogRecPtr)5000));

	/* Flush horizon below the entry's lsn:  nothing discharges. */
	UT_ASSERT_EQ(cluster_scn_durable_pending_discharge_upto((XLogRecPtr)4999), 0);
	UT_ASSERT_EQ(scn_total_cmp(cluster_scn_durable_safe_scn(), f_before), 0);

	/* Flush horizon reaches the entry:  discharge + frontier = s. */
	UT_ASSERT_EQ(cluster_scn_durable_pending_discharge_upto((XLogRecPtr)5000), 1);
	UT_ASSERT_EQ(scn_total_cmp(cluster_scn_durable_safe_scn(), s), 0);
}

UT_TEST(test_spec74_frontier_unfilled_lsn_never_discharged_by_upto)
{
	SCN s;
	SCN f;

	cluster_scn_shmem_init();

	/* Allocated but commit record not yet inserted (lsn unknown):
	 * even an infinite flush horizon must not discharge it. */
	s = cluster_scn_advance_for_commit();
	UT_ASSERT(SCN_VALID(s));

	UT_ASSERT_EQ(cluster_scn_durable_pending_discharge_upto((XLogRecPtr)PG_UINT64_MAX), 0);
	f = cluster_scn_durable_safe_scn();
	UT_ASSERT(!SCN_VALID(f) || scn_total_cmp(f, s) < 0);

	UT_ASSERT(cluster_scn_durable_pending_abort_self());
}

UT_TEST(test_spec74_frontier_abort_unblocks_frontier)
{
	SCN s1;
	SCN s2;
	SCN f;

	cluster_scn_shmem_init();

	/* s1 aborts after allocation (commit record never written);  a
	 * later durable commit s2 must not be held back by it. */
	s1 = cluster_scn_advance_for_commit();
	UT_ASSERT(SCN_VALID(s1));
	f = cluster_scn_durable_safe_scn();
	UT_ASSERT(!SCN_VALID(f) || scn_total_cmp(f, s1) < 0);

	UT_ASSERT(cluster_scn_durable_pending_abort_self());

	s2 = cluster_scn_advance_for_commit();
	UT_ASSERT(cluster_scn_durable_pending_fill_lsn(s2, (XLogRecPtr)7000));
	UT_ASSERT(cluster_scn_durable_pending_discharge_scn(s2));

	/* Aborted s1 is not a commit:  frontier covering s2 (> s1) makes
	 * no false durability claim about s1. */
	UT_ASSERT_EQ(scn_total_cmp(cluster_scn_durable_safe_scn(), s2), 0);
}

UT_TEST(test_spec74_frontier_register_after_publish_does_not_regress)
{
	SCN s1;
	SCN s2;
	uint64 pre_regress;

	cluster_scn_shmem_init();
	pre_regress = cluster_scn_durable_frontier_regression_count();

	s1 = cluster_scn_advance_for_commit();
	UT_ASSERT(cluster_scn_durable_pending_fill_lsn(s1, (XLogRecPtr)8000));
	UT_ASSERT(cluster_scn_durable_pending_discharge_scn(s1));
	UT_ASSERT_EQ(scn_total_cmp(cluster_scn_durable_safe_scn(), s1), 0);

	/* A new pending registration recomputes the frontier to
	 * min(pending)-1 == s1:  published value must hold, and the
	 * monotonic guard must not count a regression. */
	s2 = cluster_scn_advance_for_commit();
	UT_ASSERT_EQ(scn_total_cmp(cluster_scn_durable_safe_scn(), s1), 0);
	UT_ASSERT_EQ(cluster_scn_durable_frontier_regression_count(), pre_regress);

	UT_ASSERT(cluster_scn_durable_pending_abort_self());
}


/* ============================================================
 * Spec-7.4 D1 BOC payload v1 codec + handler + remote cache tests
 * ============================================================
 *
 *	Wire contract (spec-2.9 I5 v0.5 amend):  payload_length in {0, 8}.
 *	8B = origin_durable_safe_scn, explicit little-endian.  Receive side
 *	validates length, decodability, and that the SCN's node bits match
 *	env.source_node_id;  every reject is a counted fail-closed drop.
 *	Accepted values land in a per-(origin, epoch) cache;  an epoch
 *	change rebuilds the entry, a lower SCN within the same epoch is a
 *	counted regression reject.
 */

static ClusterICEnvelope
frontier_test_env(uint32 source, uint64 epoch, uint32 payload_length)
{
	ClusterICEnvelope env;

	memset(&env, 0, sizeof(env));
	env.magic = PGRAC_IC_ENVELOPE_MAGIC;
	env.version = PGRAC_IC_ENVELOPE_VERSION_V1;
	env.msg_type = PGRAC_IC_MSG_BOC_BROADCAST;
	env.source_node_id = source;
	env.epoch = epoch;
	env.scn = 12345;
	env.payload_length = payload_length;
	return env;
}

UT_TEST(test_spec74_payload_codec_le_roundtrip)
{
	uint8 buf[CLUSTER_SCN_BOC_PAYLOAD_V1_LEN];
	SCN scn = (SCN)0x0102030405060708ULL;

	cluster_scn_boc_payload_encode(scn, buf);
	UT_ASSERT_EQ(buf[0], 0x08);
	UT_ASSERT_EQ(buf[1], 0x07);
	UT_ASSERT_EQ(buf[2], 0x06);
	UT_ASSERT_EQ(buf[3], 0x05);
	UT_ASSERT_EQ(buf[4], 0x04);
	UT_ASSERT_EQ(buf[5], 0x03);
	UT_ASSERT_EQ(buf[6], 0x02);
	UT_ASSERT_EQ(buf[7], 0x01);
	UT_ASSERT_EQ(cluster_scn_boc_payload_decode(buf), scn);
}

UT_TEST(test_spec74_handler_v1_payload_updates_cache)
{
	ClusterICEnvelope env = frontier_test_env(3, 7, CLUSTER_SCN_BOC_PAYLOAD_V1_LEN);
	uint8 payload[CLUSTER_SCN_BOC_PAYLOAD_V1_LEN];
	SCN sent = scn_encode(3, 500);
	SCN got = InvalidScn;
	uint64 got_epoch = 0;
	uint64 pre_accept;

	cluster_scn_shmem_init();
	pre_accept = cluster_scn_boc_payload_accept_count();

	cluster_scn_boc_payload_encode(sent, payload);
	cluster_scn_boc_broadcast_handler(&env, payload);

	UT_ASSERT_EQ(cluster_scn_boc_payload_accept_count(), pre_accept + 1);
	UT_ASSERT(cluster_scn_remote_durable_safe(3, &got_epoch, &got));
	UT_ASSERT_EQ(scn_total_cmp(got, sent), 0);
	UT_ASSERT_EQ(got_epoch, 7);
}

UT_TEST(test_spec74_handler_zero_payload_keeps_cache)
{
	ClusterICEnvelope env = frontier_test_env(3, 7, 0);
	SCN got = InvalidScn;
	uint64 got_epoch = 0;

	cluster_scn_shmem_init();

	/* Old sender / publish off:  0-byte frame stays a no-op and the
	 * cache from the previous test survives. */
	cluster_scn_boc_broadcast_handler(&env, NULL);

	UT_ASSERT(cluster_scn_remote_durable_safe(3, &got_epoch, &got));
	UT_ASSERT_EQ(scn_total_cmp(got, scn_encode(3, 500)), 0);
}

UT_TEST(test_spec74_handler_bad_length_dropped)
{
	ClusterICEnvelope env = frontier_test_env(3, 7, 4);
	uint8 payload[CLUSTER_SCN_BOC_PAYLOAD_V1_LEN];
	SCN got = InvalidScn;
	uint64 got_epoch = 0;
	uint64 pre_bad;

	cluster_scn_shmem_init();
	pre_bad = cluster_scn_boc_payload_bad_length_count();

	cluster_scn_boc_payload_encode(scn_encode(3, 900), payload);
	cluster_scn_boc_broadcast_handler(&env, payload);

	UT_ASSERT_EQ(cluster_scn_boc_payload_bad_length_count(), pre_bad + 1);
	UT_ASSERT(cluster_scn_remote_durable_safe(3, &got_epoch, &got));
	UT_ASSERT_EQ(scn_total_cmp(got, scn_encode(3, 500)), 0); /* unchanged */
}

UT_TEST(test_spec74_handler_undecodable_payload_dropped)
{
	ClusterICEnvelope env = frontier_test_env(3, 7, CLUSTER_SCN_BOC_PAYLOAD_V1_LEN);
	uint8 payload[CLUSTER_SCN_BOC_PAYLOAD_V1_LEN];
	uint64 pre_bad;

	cluster_scn_shmem_init();
	pre_bad = cluster_scn_boc_payload_bad_length_count();

	/* All-zero payload decodes to InvalidScn:  malformed frame. */
	memset(payload, 0, sizeof(payload));
	cluster_scn_boc_broadcast_handler(&env, payload);

	UT_ASSERT_EQ(cluster_scn_boc_payload_bad_length_count(), pre_bad + 1);
}

UT_TEST(test_spec74_handler_node_mismatch_dropped)
{
	ClusterICEnvelope env = frontier_test_env(3, 7, CLUSTER_SCN_BOC_PAYLOAD_V1_LEN);
	uint8 payload[CLUSTER_SCN_BOC_PAYLOAD_V1_LEN];
	SCN got = InvalidScn;
	uint64 got_epoch = 0;
	uint64 pre_mismatch;

	cluster_scn_shmem_init();
	pre_mismatch = cluster_scn_boc_payload_node_mismatch_count();

	/* SCN claims node 5 but the envelope sender is node 3. */
	cluster_scn_boc_payload_encode(scn_encode(5, 900), payload);
	cluster_scn_boc_broadcast_handler(&env, payload);

	UT_ASSERT_EQ(cluster_scn_boc_payload_node_mismatch_count(), pre_mismatch + 1);
	UT_ASSERT(cluster_scn_remote_durable_safe(3, &got_epoch, &got));
	UT_ASSERT_EQ(scn_total_cmp(got, scn_encode(3, 500)), 0); /* unchanged */
}

UT_TEST(test_spec74_handler_regression_rejected_same_epoch)
{
	ClusterICEnvelope env = frontier_test_env(3, 7, CLUSTER_SCN_BOC_PAYLOAD_V1_LEN);
	uint8 payload[CLUSTER_SCN_BOC_PAYLOAD_V1_LEN];
	SCN got = InvalidScn;
	uint64 got_epoch = 0;
	uint64 pre_regress;

	cluster_scn_shmem_init();
	pre_regress = cluster_scn_boc_payload_regression_count();

	/* Advance the cache to 600 first. */
	cluster_scn_boc_payload_encode(scn_encode(3, 600), payload);
	cluster_scn_boc_broadcast_handler(&env, payload);

	/* A reordered older frame (550) must not roll the cache back. */
	cluster_scn_boc_payload_encode(scn_encode(3, 550), payload);
	cluster_scn_boc_broadcast_handler(&env, payload);

	UT_ASSERT_EQ(cluster_scn_boc_payload_regression_count(), pre_regress + 1);
	UT_ASSERT(cluster_scn_remote_durable_safe(3, &got_epoch, &got));
	UT_ASSERT_EQ(scn_total_cmp(got, scn_encode(3, 600)), 0);
}

UT_TEST(test_spec74_handler_epoch_change_rebuilds_cache)
{
	ClusterICEnvelope env = frontier_test_env(3, 8, CLUSTER_SCN_BOC_PAYLOAD_V1_LEN);
	uint8 payload[CLUSTER_SCN_BOC_PAYLOAD_V1_LEN];
	SCN got = InvalidScn;
	uint64 got_epoch = 0;
	uint64 pre_regress;

	cluster_scn_shmem_init();
	pre_regress = cluster_scn_boc_payload_regression_count();

	/* New epoch 8 with a LOWER scn than epoch 7's cached 600:  the
	 * entry is rebuilt, not regression-rejected (R5 epoch semantics). */
	cluster_scn_boc_payload_encode(scn_encode(3, 550), payload);
	cluster_scn_boc_broadcast_handler(&env, payload);

	UT_ASSERT_EQ(cluster_scn_boc_payload_regression_count(), pre_regress);
	UT_ASSERT(cluster_scn_remote_durable_safe(3, &got_epoch, &got));
	UT_ASSERT_EQ(scn_total_cmp(got, scn_encode(3, 550)), 0);
	UT_ASSERT_EQ(got_epoch, 8);
}

UT_TEST(test_spec74_remote_accessor_unseen_origin_false)
{
	SCN got = InvalidScn;
	uint64 got_epoch = 0;

	cluster_scn_shmem_init();

	UT_ASSERT(!cluster_scn_remote_durable_safe(99, &got_epoch, &got));
	UT_ASSERT(!cluster_scn_remote_durable_safe(CLUSTER_MAX_NODES, &got_epoch, &got));
	UT_ASSERT(!cluster_scn_remote_durable_safe(-1, &got_epoch, &got));
}

UT_TEST(test_spec74_frontier_abort_self_keeps_filled_entry)
{
	SCN s;
	uint64 pre_count;

	cluster_scn_shmem_init();
	pre_count = cluster_scn_durable_pending_count();

	/* Async-commit shape:  the commit record was inserted (lsn filled)
	 * but not yet flushed.  A later abort of a DIFFERENT transaction on
	 * this backend must not discharge the still-pending real commit --
	 * a filled lsn proves a commit record exists, so only the walwriter
	 * flush horizon may prove it durable. */
	s = cluster_scn_advance_for_commit();
	UT_ASSERT(SCN_VALID(s));
	UT_ASSERT(cluster_scn_durable_pending_fill_lsn(s, (XLogRecPtr)4242));

	UT_ASSERT(!cluster_scn_durable_pending_abort_self());
	UT_ASSERT_EQ(cluster_scn_durable_pending_count(), pre_count + 1);

	/* The walwriter horizon still discharges it normally. */
	UT_ASSERT_EQ(cluster_scn_durable_pending_discharge_upto((XLogRecPtr)4242), 1);
	UT_ASSERT_EQ(scn_total_cmp(cluster_scn_durable_safe_scn(), s), 0);
}


/* ============================================================
 * Spec-7.4 D1-2 event protocol tests (dirty + wake + drain payload)
 * ============================================================
 *
 *	Producer protocol (mini-plan v1.1 ruling #4):  publish (under the
 *	SCN lwlock) -> exchange-set dirty -> SetLatch(LMON) only on the
 *	0->1 transition.  Consumer (LMON drain):  exchange-clear dirty
 *	BEFORE snapshotting, so a racing publish re-arms the wakeup.
 *	cluster.boc_event_publish=off restores payload=0 frames AND no
 *	wakeups (byte equivalence with the pre-D1 sweep-only channel).
 */

UT_TEST(test_spec74_event_dirty_wake_on_frontier_advance)
{
	SCN s;

	cluster_scn_shmem_init();
	(void)cluster_scn_boc_event_consume(); /* drain stale dirty */
	test_lmon_wakeup_count = 0;

	s = cluster_scn_advance_for_commit();
	UT_ASSERT(cluster_scn_durable_pending_fill_lsn(s, (XLogRecPtr)11000));
	UT_ASSERT(cluster_scn_durable_pending_discharge_scn(s));

	UT_ASSERT_EQ(test_lmon_wakeup_count, 1);
	UT_ASSERT(cluster_scn_boc_event_consume());
	UT_ASSERT(!cluster_scn_boc_event_consume());
}

UT_TEST(test_spec74_event_dirty_dedup_second_advance_no_wake)
{
	SCN s1;
	SCN s2;

	cluster_scn_shmem_init();
	(void)cluster_scn_boc_event_consume();
	test_lmon_wakeup_count = 0;

	s1 = cluster_scn_advance_for_commit();
	s2 = cluster_scn_advance_for_commit();
	UT_ASSERT(cluster_scn_durable_pending_fill_lsn(s1, (XLogRecPtr)12000));
	UT_ASSERT(cluster_scn_durable_pending_fill_lsn(s2, (XLogRecPtr)12001));

	/* First advance wakes;  the second finds dirty already set and
	 * must not signal again (natural batching). */
	UT_ASSERT(cluster_scn_durable_pending_discharge_scn(s1));
	UT_ASSERT(cluster_scn_durable_pending_discharge_scn(s2));

	UT_ASSERT_EQ(test_lmon_wakeup_count, 1);
	UT_ASSERT(cluster_scn_boc_event_consume());
}

UT_TEST(test_spec74_event_no_frontier_advance_no_wake)
{
	SCN s1;
	SCN s2;

	cluster_scn_shmem_init();
	(void)cluster_scn_boc_event_consume();
	test_lmon_wakeup_count = 0;

	s1 = cluster_scn_advance_for_commit();
	s2 = cluster_scn_advance_for_commit();
	UT_ASSERT(cluster_scn_durable_pending_fill_lsn(s1, (XLogRecPtr)13000));
	UT_ASSERT(cluster_scn_durable_pending_fill_lsn(s2, (XLogRecPtr)13001));

	/* Discharging the LATER scn first leaves min(pending)=s1:  the
	 * recomputed frontier equals the already-published value, so no
	 * event fires. */
	UT_ASSERT(cluster_scn_durable_pending_discharge_scn(s2));
	UT_ASSERT_EQ(test_lmon_wakeup_count, 0);
	UT_ASSERT(!cluster_scn_boc_event_consume());

	UT_ASSERT(cluster_scn_durable_pending_discharge_scn(s1));
	UT_ASSERT_EQ(test_lmon_wakeup_count, 1);
	UT_ASSERT(cluster_scn_boc_event_consume());
}

UT_TEST(test_spec74_event_publish_off_no_dirty_no_wake)
{
	SCN s;

	cluster_scn_shmem_init();
	(void)cluster_scn_boc_event_consume();
	test_lmon_wakeup_count = 0;

	cluster_boc_event_publish = false;
	s = cluster_scn_advance_for_commit();
	UT_ASSERT(cluster_scn_durable_pending_fill_lsn(s, (XLogRecPtr)14000));
	UT_ASSERT(cluster_scn_durable_pending_discharge_scn(s));
	cluster_boc_event_publish = true;

	UT_ASSERT_EQ(test_lmon_wakeup_count, 0);
	UT_ASSERT(!cluster_scn_boc_event_consume());
}

UT_TEST(test_spec74_event_injection_suppresses_signal)
{
	SCN s;

	cluster_scn_shmem_init();
	(void)cluster_scn_boc_event_consume();
	test_lmon_wakeup_count = 0;

	test_injection_skip_point = "cluster-boc-event-publish";
	cluster_injection_armed_count = 1;
	s = cluster_scn_advance_for_commit();
	UT_ASSERT(cluster_scn_durable_pending_fill_lsn(s, (XLogRecPtr)15000));
	UT_ASSERT(cluster_scn_durable_pending_discharge_scn(s));
	cluster_injection_armed_count = 0;
	test_injection_skip_point = NULL;

	/* Suppressed event; the sweep cadence remains the fallback (L2). */
	UT_ASSERT_EQ(test_lmon_wakeup_count, 0);
	UT_ASSERT(!cluster_scn_boc_event_consume());
}

UT_TEST(test_spec74_drain_attaches_payload_v1_when_on)
{
	SCN s;

	cluster_scn_shmem_init();
	test_alive_peer_count = 1;

	/* Arm an event, then drain as LMON would. */
	s = cluster_scn_advance_for_commit();
	UT_ASSERT(cluster_scn_durable_pending_fill_lsn(s, (XLogRecPtr)16000));
	UT_ASSERT(cluster_scn_durable_pending_discharge_scn(s));

	test_fanout_calls = 0;
	cluster_scn_lmon_drain_boc_broadcast();

	UT_ASSERT_EQ(test_fanout_calls, 1);
	UT_ASSERT_EQ(test_fanout_last_msg_type, PGRAC_IC_MSG_BOC_BROADCAST);
	UT_ASSERT_EQ(test_fanout_last_len, CLUSTER_SCN_BOC_PAYLOAD_V1_LEN);
	UT_ASSERT_EQ(scn_total_cmp(cluster_scn_boc_payload_decode(test_fanout_last_payload),
							   cluster_scn_durable_safe_scn()),
				 0);
	test_alive_peer_count = 0;
}

UT_TEST(test_spec74_drain_zero_len_when_off_even_if_dirty)
{
	SCN s;

	cluster_scn_shmem_init();
	test_alive_peer_count = 1;

	/* Dirty was armed while the GUC was on;  a SIGHUP flips it off
	 * before the drain runs.  The stale event drains as a 0-byte
	 * pulse:  off means payload=0 on the wire, unconditionally. */
	s = cluster_scn_advance_for_commit();
	UT_ASSERT(cluster_scn_durable_pending_fill_lsn(s, (XLogRecPtr)17000));
	UT_ASSERT(cluster_scn_durable_pending_discharge_scn(s));

	cluster_boc_event_publish = false;
	test_fanout_calls = 0;
	cluster_scn_lmon_drain_boc_broadcast();
	cluster_boc_event_publish = true;

	UT_ASSERT_EQ(test_fanout_calls, 1);
	UT_ASSERT_EQ(test_fanout_last_msg_type, PGRAC_IC_MSG_BOC_BROADCAST);
	UT_ASSERT_EQ(test_fanout_last_len, 0);
	test_alive_peer_count = 0;
}

/*
 * MUST RUN LAST:  overflow freezes the frontier stickily (until
 * restart);  every test after this one would see frozen state.
 */
UT_TEST(test_spec74_frontier_overflow_freezes_sticky)
{
	SCN last_safe;
	SCN first_s = InvalidScn;
	SCN s;
	uint64 pre_overflow;
	int i;

	cluster_scn_shmem_init();
	pre_overflow = cluster_scn_durable_frontier_overflow_count();
	last_safe = cluster_scn_durable_safe_scn();

	/* Fill the registry to capacity, then one more. */
	for (i = 0; i <= CLUSTER_SCN_DURABLE_PENDING_MAX; i++) {
		s = cluster_scn_advance_for_commit();
		UT_ASSERT(SCN_VALID(s));
		if (i == 0)
			first_s = s;
	}

	UT_ASSERT(cluster_scn_durable_frontier_frozen());
	UT_ASSERT(cluster_scn_durable_frontier_overflow_count() > pre_overflow);

	/* Frozen frontier never advances -- discharging a tracked entry
	 * still removes it, but the published value stays put (sticky
	 * until restart;  consumers fall back to the existing fetch-reply
	 * piggyback sampling). */
	UT_ASSERT(cluster_scn_durable_pending_fill_lsn(first_s, (XLogRecPtr)9000));
	UT_ASSERT(cluster_scn_durable_pending_discharge_scn(first_s));
	UT_ASSERT_EQ(scn_total_cmp(cluster_scn_durable_safe_scn(), last_safe), 0);
	UT_ASSERT(cluster_scn_durable_frontier_frozen());
}

int
main(void)
{
	/* Spec-7.4 D1 durable_safe_scn frontier registry (9) --
	 * T-scn-74-1a..1i.  Cumulative shmem state;  overflow test last. */
	UT_RUN(test_spec74_frontier_accessors_linkable);
	UT_RUN(test_spec74_frontier_initial_state_invalid_unfrozen);
	UT_RUN(test_spec74_frontier_commit_advance_registers_pending);
	UT_RUN(test_spec74_frontier_sync_discharge_advances_to_scn);
	UT_RUN(test_spec74_frontier_out_of_order_flush_does_not_pass_stalled);
	UT_RUN(test_spec74_frontier_walwriter_upto_respects_lsn_order);
	UT_RUN(test_spec74_frontier_unfilled_lsn_never_discharged_by_upto);
	UT_RUN(test_spec74_frontier_abort_unblocks_frontier);
	UT_RUN(test_spec74_frontier_register_after_publish_does_not_regress);
	UT_RUN(test_spec74_payload_codec_le_roundtrip);
	UT_RUN(test_spec74_handler_v1_payload_updates_cache);
	UT_RUN(test_spec74_handler_zero_payload_keeps_cache);
	UT_RUN(test_spec74_handler_bad_length_dropped);
	UT_RUN(test_spec74_handler_undecodable_payload_dropped);
	UT_RUN(test_spec74_handler_node_mismatch_dropped);
	UT_RUN(test_spec74_handler_regression_rejected_same_epoch);
	UT_RUN(test_spec74_handler_epoch_change_rebuilds_cache);
	UT_RUN(test_spec74_remote_accessor_unseen_origin_false);
	UT_RUN(test_spec74_frontier_abort_self_keeps_filled_entry);
	UT_RUN(test_spec74_event_dirty_wake_on_frontier_advance);
	UT_RUN(test_spec74_event_dirty_dedup_second_advance_no_wake);
	UT_RUN(test_spec74_event_no_frontier_advance_no_wake);
	UT_RUN(test_spec74_event_publish_off_no_dirty_no_wake);
	UT_RUN(test_spec74_event_injection_suppresses_signal);
	UT_RUN(test_spec74_drain_attaches_payload_v1_when_on);
	UT_RUN(test_spec74_drain_zero_len_when_off_even_if_dirty);
	UT_RUN(test_spec74_frontier_overflow_freezes_sticky);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
