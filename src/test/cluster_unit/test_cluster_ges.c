/*-------------------------------------------------------------------------
 *
 * test_cluster_ges.c
 *	  Standalone unit tests for spec-2.13 GES protocol skeleton.
 *
 *	  T-ges-1 (5 tests, spec-2.13 D6):
 *	    a) cluster_ges_request_handler / cluster_ges_reply_handler symbol
 *	       linkable (UT_ASSERT_NOT_NULL fn ptr).
 *	    b) cluster_ges_request_defer_count / cluster_ges_reply_defer_count
 *	       accessors linkable + initial read returns 0.
 *	    c) **真行为 — request stub**:  pre = cluster_ges_request_defer_count()
 *	       → invoke cluster_ges_request_handler(envelope_sentinel, NULL) →
 *	       assert (1) handler 静默返回 (no ERROR/FATAL abort), (2)
 *	       cluster_ges_request_defer_count() == pre + 1, (3)
 *	       cluster_ges_reply_defer_count() == pre_reply (unchanged).
 *	    d) **真行为 — reply stub**:  symmetric with (c) but reply path;
 *	       assert reply_defer_count +1 + request_defer_count unchanged.
 *	    e) handler 跨 multiple invocations 真测 monotonic non-decrease +
 *	       counter accuracy (handler 调用 N 次 → counter 真递增 N).
 *
 *	  Stubs:
 *	    - ShmemInitStruct returns a union force-aligned buffer per L105
 *	      (Apple silicon tolerates misaligned atomic but strict-alignment
 *	      platform ARM Linux / SPARC SIGBUS without union force-align).
 *	    - cluster_shmem_register_region: no-op (region 注册不真测).
 *	    - elog / ereport: stubbed pass-through (DEBUG2 from handler
 *	      should be silent in test runner).
 *
 *	  Spec: spec-2.13 D6 + Q5.2 + Q9 (L105 union force-align).
 *	  Cross-spec lesson inheritance: L94 / L105 / L106 / L107.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_ges.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Standalone binary linking
 *	  cluster_ges.o only; all PG backend symbols stubbed locally.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <string.h>

#include "cluster/cluster_ges.h"
#include "cluster/cluster_grd_outbound.h"
#include "cluster/cluster_grd_work_queue.h"
#include "cluster/cluster_ic_envelope.h"
#include "port/atomics.h"

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


/* ============================================================
 * Stubs needed to link cluster_ges.o standalone.
 *
 *	ShmemInitStruct uses L105 union force-align pattern (mirror
 *	test_cluster_scn.c spec-2.11 P1.2 fix) — pg_atomic_uint64 must
 *	be 8-byte aligned on strict-alignment platforms.
 * ============================================================ */

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

/*
 * spec-2.13 Q9 (L105 inherit):  ShmemInitStruct stub uses union
 * force-align to guarantee 8-byte alignment for pg_atomic_uint64
 * fields inside ClusterGesSharedState.
 */
void *
ShmemInitStruct(const char *name, Size size, bool *foundPtr)
{
	static union {
		uint64 force_align;
		char data[1024]; /* generous; cluster_ges_shmem_size() << 1KB */
	} ges_buf;
	static bool ges_initialized = false;

	if (name != NULL && strcmp(name, "pgrac cluster ges") == 0) {
		Assert(size <= sizeof(ges_buf.data)); /* catch shmem layout growth */
		*foundPtr = ges_initialized;
		ges_initialized = true;
		return ges_buf.data;
	}

	*foundPtr = true;
	return NULL;
}

void
cluster_shmem_register_region(const void *r pg_attribute_unused())
{}


/* ============================================================
 * spec-2.16 Step 6 L104 stubs — cross-module deps activated by
 *   Step 3 D6 handler (5-item validation + work_queue enqueue +
 *   REJECT_BUSY fallback).  Stubs default to "pass-through" so
 *   既有 T-ges-1 tests still PASS with handler 真激活 behavior.
 * ============================================================ */

int cluster_node_id = 0;

bool
cluster_qvotec_in_quorum(void)
{
	return true; /* default in-quorum so validation step 4 passes */
}

uint64
cluster_epoch_get_current(void)
{
	return 0; /* default epoch 0 — matches env_sentinel.epoch */
}

/* cluster_conf — return non-NULL so validation step 4 declared check passes */
const void *
cluster_conf_lookup_node(int32 node_id pg_attribute_unused())
{
	static char dummy;
	return (const void *)&dummy;
}

int32
cluster_grd_lookup_master(const struct ClusterResId *resid pg_attribute_unused())
{
	return cluster_node_id;
}

/* GRD inc helpers — stub bump local counters (test verifies via accessor) */
static uint64 stub_work_queue_full = 0;
static uint64 stub_inbound_validation_fail = 0;
static uint64 stub_cleanup_deferred = 0;
static uint64 stub_reply_deferred = 0;
static uint64 stub_reply_dropped = 0;
static uint64 stub_work_queue_enqueue_count = 0;
static uint64 stub_lmon_reply_enqueue_count = 0;
static uint64 stub_bast_received = 0;
static uint64 stub_bast_ack = 0;
static uint64 stub_deadlock_probe_drop = 0;

void
cluster_grd_inc_ges_work_queue_full(void)
{
	stub_work_queue_full++;
}

/* spec-2.18 Sprint A Step 3 D9 L104 stub:  cluster_lms_wake_drain
 * broadcasts CV after successful work_queue enqueue. */
void cluster_lms_wake_drain(void);
void
cluster_lms_wake_drain(void)
{}

/* spec-2.19 Sprint A Step 3 D8 L104 stubs:  cluster_lmd_is_ready (HC4
 * exact predicate) + cluster_lmd_submit_wait_edge (HC3 producer wake +
 * HC6 skeleton "no graph maintenance").  Called from cluster_ges.c
 * GES_REQ_OPCODE_DEADLOCK_PROBE handler. */
bool cluster_lmd_is_ready(void);
bool
cluster_lmd_is_ready(void)
{
	/* Standalone unit test:LMD shmem not attached, predicate false. */
	return false;
}
void cluster_lmd_submit_wait_edge(void);
void
cluster_lmd_submit_wait_edge(void)
{}
void
cluster_grd_inc_ges_inbound_validation_fail(void)
{
	stub_inbound_validation_fail++;
}
void
cluster_grd_inc_ges_cleanup_deferred(void)
{
	stub_cleanup_deferred++;
}
void
cluster_grd_inc_ges_reply_deferred(void)
{
	stub_reply_deferred++;
}
void
cluster_grd_inc_ges_reply_dropped(void)
{
	stub_reply_dropped++;
}
void
cluster_grd_inc_bast_received(void)
{
	stub_bast_received++;
}
void
cluster_grd_inc_bast_ack(void)
{
	stub_bast_ack++;
}
void
cluster_grd_inc_deadlock_probe_drop(void)
{
	stub_deadlock_probe_drop++;
}

/* work_queue + outbound enqueue stubs — accept always (no overflow path tested
 * at unit layer; TAP exercises overflow with real shmem). */
bool
cluster_grd_work_queue_enqueue(uint32 src pg_attribute_unused(),
							   const void *p pg_attribute_unused(), uint16 l pg_attribute_unused())
{
	stub_work_queue_enqueue_count++;
	return true;
}

bool
cluster_grd_work_queue_dequeue(ClusterGrdWorkItem *out pg_attribute_unused())
{
	return false;
}

void
cluster_grd_outbound_enqueue_lmon_reply(uint32 d pg_attribute_unused(),
										const void *p pg_attribute_unused(),
										uint16 l pg_attribute_unused())
{
	stub_lmon_reply_enqueue_count++;
}


/* ============================================================
 * T-ges-1 a/b/c/d/e (spec-2.13 D6 Q5.2).
 * ============================================================ */

UT_TEST(test_ges_request_handler_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_ges_request_handler);
}

UT_TEST(test_ges_reply_handler_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_ges_reply_handler);
}

UT_TEST(test_ges_accessors_linkable_and_initial_zero)
{
	cluster_ges_shmem_init();

	UT_ASSERT_NOT_NULL((void *)cluster_ges_request_defer_count);
	UT_ASSERT_NOT_NULL((void *)cluster_ges_reply_defer_count);

	UT_ASSERT_EQ(cluster_ges_request_defer_count(), (uint64)0);
	UT_ASSERT_EQ(cluster_ges_reply_defer_count(), (uint64)0);
}

UT_TEST(test_ges_request_handler_real_behavior)
{
	ClusterICEnvelope env_sentinel;
	uint64 pre_request;
	uint64 pre_reply;

	cluster_ges_shmem_init();

	memset(&env_sentinel, 0, sizeof(env_sentinel));
	env_sentinel.source_node_id = 7; /* sentinel non-zero peer id */

	pre_request = cluster_ges_request_defer_count();
	pre_reply = cluster_ges_reply_defer_count();

	/* Invoke stub — must return silently without ERROR/FATAL. */
	cluster_ges_request_handler(&env_sentinel, NULL);

	/* (1) handler returned (would not get here on FATAL abort).
	 * (2) request counter +1.
	 * (3) reply counter unchanged. */
	UT_ASSERT_EQ(cluster_ges_request_defer_count(), pre_request + 1);
	UT_ASSERT_EQ(cluster_ges_reply_defer_count(), pre_reply);
}

UT_TEST(test_ges_reply_handler_real_behavior)
{
	ClusterICEnvelope env_sentinel;
	uint64 pre_request;
	uint64 pre_reply;

	cluster_ges_shmem_init();

	memset(&env_sentinel, 0, sizeof(env_sentinel));
	env_sentinel.source_node_id = 11;

	pre_request = cluster_ges_request_defer_count();
	pre_reply = cluster_ges_reply_defer_count();

	cluster_ges_reply_handler(&env_sentinel, NULL);

	UT_ASSERT_EQ(cluster_ges_reply_defer_count(), pre_reply + 1);
	UT_ASSERT_EQ(cluster_ges_request_defer_count(), pre_request);
}

UT_TEST(test_ges_handler_counter_monotonic_n_invocations)
{
	ClusterICEnvelope env_sentinel;
	uint64 pre_request;
	uint64 pre_reply;
	const int N = 7;
	int i;

	cluster_ges_shmem_init();

	memset(&env_sentinel, 0, sizeof(env_sentinel));
	env_sentinel.source_node_id = 3;

	pre_request = cluster_ges_request_defer_count();
	pre_reply = cluster_ges_reply_defer_count();

	for (i = 0; i < N; i++)
		cluster_ges_request_handler(&env_sentinel, NULL);

	for (i = 0; i < N; i++)
		cluster_ges_reply_handler(&env_sentinel, NULL);

	UT_ASSERT_EQ(cluster_ges_request_defer_count(), pre_request + (uint64)N);
	UT_ASSERT_EQ(cluster_ges_reply_defer_count(), pre_reply + (uint64)N);
}

UT_TEST(test_ges_request_valid_payload_enqueues_work)
{
	ClusterICEnvelope env;
	GesRequestPayload req;
	uint64 pre_fail = stub_inbound_validation_fail;
	uint64 pre_enqueue = stub_work_queue_enqueue_count;

	cluster_ges_shmem_init();
	cluster_node_id = 0;
	memset(&env, 0, sizeof(env));
	env.source_node_id = 1;
	env.epoch = 0;

	memset(&req, 0, sizeof(req));
	req.opcode = GES_REQ_OPCODE_REQUEST;
	req.holder_node_id = 1;

	cluster_ges_request_handler(&env, &req);

	UT_ASSERT_EQ(stub_inbound_validation_fail, pre_fail);
	UT_ASSERT_EQ(stub_work_queue_enqueue_count, pre_enqueue + 1);
}

UT_TEST(test_ges_reply_valid_payload_echoes_local_holder)
{
	ClusterICEnvelope env;
	GesReplyPayload rep;
	uint64 pre_fail = stub_inbound_validation_fail;
	uint64 pre_reply = cluster_ges_reply_defer_count();

	cluster_ges_shmem_init();
	cluster_node_id = 0;
	memset(&env, 0, sizeof(env));
	env.source_node_id = 1;
	env.epoch = 0;

	memset(&rep, 0, sizeof(rep));
	rep.opcode = GES_REPLY_OPCODE_REJECT;
	rep.reject_reason = GES_REJECT_REASON_LOCK_CONFLICT;
	rep.holder_node_id = 0;

	cluster_ges_reply_handler(&env, &rep);

	UT_ASSERT_EQ(stub_inbound_validation_fail, pre_fail);
	UT_ASSERT_EQ(cluster_ges_reply_defer_count(), pre_reply + 1);
}

UT_TEST(test_ges_lmon_drain_work_queue_symbol_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_ges_lmon_drain_work_queue);
}


UT_DEFINE_GLOBALS();


/* spec-2.17 Step 2 — NEW T-ges-3 opcode + payload size invariant. */
static void
test_ges_opcode_enum_spec_2_17_extension(void)
{
	/* spec-2.17 Q5 v0.6:  7 opcode 全集 — BAST=4 / BAST_ACK=5 /
	 * DEADLOCK_PROBE=6 / CANCEL_PENDING=7. */
	UT_ASSERT_EQ((int)GES_REQ_OPCODE_REQUEST, 1);
	UT_ASSERT_EQ((int)GES_REQ_OPCODE_CONVERT, 2);
	UT_ASSERT_EQ((int)GES_REQ_OPCODE_RELEASE, 3);
	UT_ASSERT_EQ((int)GES_REQ_OPCODE_BAST, 4);
	UT_ASSERT_EQ((int)GES_REQ_OPCODE_BAST_ACK, 5);
	UT_ASSERT_EQ((int)GES_REQ_OPCODE_DEADLOCK_PROBE, 6);
	UT_ASSERT_EQ((int)GES_REQ_OPCODE_CANCEL_PENDING, 7);
}

static void
test_ges_bast_opcode_validates_as_target_local(void)
{
	ClusterICEnvelope env;
	GesRequestPayload req;
	uint64 pre_fail = stub_inbound_validation_fail;
	uint64 pre_bast = stub_bast_received;
	uint64 pre_enqueue = stub_work_queue_enqueue_count;

	cluster_ges_shmem_init();
	cluster_node_id = 0;
	memset(&env, 0, sizeof(env));
	env.source_node_id = 1; /* master */
	env.epoch = 0;

	memset(&req, 0, sizeof(req));
	req.opcode = GES_REQ_OPCODE_BAST;
	req.holder_node_id = 0; /* local target, not envelope source */

	cluster_ges_request_handler(&env, &req);

	UT_ASSERT_EQ(stub_inbound_validation_fail, pre_fail);
	UT_ASSERT_EQ(stub_bast_received, pre_bast + 1);
	UT_ASSERT_EQ(stub_work_queue_enqueue_count, pre_enqueue);
}

static void
test_ges_bast_ack_opcode_validates_as_source_holder(void)
{
	ClusterICEnvelope env;
	GesRequestPayload req;
	uint64 pre_fail = stub_inbound_validation_fail;
	uint64 pre_ack = stub_bast_ack;
	uint64 pre_enqueue = stub_work_queue_enqueue_count;

	cluster_ges_shmem_init();
	cluster_node_id = 0;
	memset(&env, 0, sizeof(env));
	env.source_node_id = 1; /* holder */
	env.epoch = 0;

	memset(&req, 0, sizeof(req));
	req.opcode = GES_REQ_OPCODE_BAST_ACK;
	req.holder_node_id = 1; /* holder must match envelope source */

	cluster_ges_request_handler(&env, &req);

	UT_ASSERT_EQ(stub_inbound_validation_fail, pre_fail);
	UT_ASSERT_EQ(stub_bast_ack, pre_ack + 1);
	UT_ASSERT_EQ(stub_work_queue_enqueue_count, pre_enqueue);
}

int
main(int argc pg_attribute_unused(), char *argv[] pg_attribute_unused())
{
	UT_PLAN(12);

	UT_RUN(test_ges_request_handler_linkable);
	UT_RUN(test_ges_reply_handler_linkable);
	UT_RUN(test_ges_accessors_linkable_and_initial_zero);
	UT_RUN(test_ges_request_handler_real_behavior);
	UT_RUN(test_ges_reply_handler_real_behavior);
	UT_RUN(test_ges_handler_counter_monotonic_n_invocations);
	UT_RUN(test_ges_request_valid_payload_enqueues_work);
	UT_RUN(test_ges_reply_valid_payload_echoes_local_holder);
	UT_RUN(test_ges_lmon_drain_work_queue_symbol_linkable);
	UT_RUN(test_ges_opcode_enum_spec_2_17_extension);
	UT_RUN(test_ges_bast_opcode_validates_as_target_local);
	UT_RUN(test_ges_bast_ack_opcode_validates_as_source_holder);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
