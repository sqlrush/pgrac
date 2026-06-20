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
#include "cluster/cluster_grd.h"
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
 * spec-2.22 D6 — cluster_ges.c handler calls pgstat_report_wait_start/_end
 * for WAIT_EVENT_CLUSTER_LMD_PROBE.  The inline helpers reference
 * my_wait_event_info which lives in pgstat backend code.  Provide a
 * file-static fake so the standalone link resolves cleanly.
 */
#include "pgstat.h"
static uint32 ut_wait_event_info_storage = 0;
uint32 *my_wait_event_info = &ut_wait_event_info_storage;

/* cluster_lmd_graph_snapshot_copy stub — cluster_ges DEADLOCK_PROBE
 * handler calls into LMD graph;  test_cluster_ges binary links
 * cluster_ges.o standalone (no cluster_lmd_graph.o), so stub it. */
#include "cluster/cluster_lmd.h"

int
cluster_lmd_graph_snapshot_copy(ClusterLmdWaitEdge *out_buf pg_attribute_unused(),
								int max_edges pg_attribute_unused(), uint64 *out_gen_at_snapshot)
{
	if (out_gen_at_snapshot)
		*out_gen_at_snapshot = 0;
	return 0;
}
/* cluster_lmd_is_ready stub already provided below (line ~275). */

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

/* cluster_conf — return non-NULL so validation step 4 declared check
 * passes.  Signature must match cluster_conf.h (pulled in via
 * cluster_grd.h since spec-4.6). */
const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node_id pg_attribute_unused())
{
	static ClusterNodeInfo dummy;
	return &dummy;
}

/* spec-4.6 D3/D4 stubs:  the drain path now reads the shard recovery
 * phase (master-side freeze gate) and handles GES_REDECLARE via the
 * insert-or-rebind entry API.  Standalone fixture:  NORMAL phase +
 * always-OK rebind. */
uint32
cluster_grd_shard_for_resource(const ClusterResId *resid pg_attribute_unused())
{
	return 0;
}

ClusterGrdShardPhase
cluster_grd_shard_phase(uint32 shard_id pg_attribute_unused())
{
	return GRD_SHARD_NORMAL;
}

/* spec-4.6 P0#3 stub:  REDECLARE_DONE handler records peer barrier
 * completion.  Standalone fixture has no recovery shmem; no-op. */
void
cluster_grd_recovery_mark_peer_done(int32 node pg_attribute_unused(),
									uint64 epoch pg_attribute_unused())
{}

ClusterGrdEntryResult
cluster_grd_entry_rebind_or_insert_holder(const ClusterResId *resid pg_attribute_unused(),
										  const struct ClusterGrdHolderId *nh pg_attribute_unused(),
										  int32 src pg_attribute_unused(),
										  int lockmode pg_attribute_unused())
{
	return CLUSTER_GRD_ENTRY_OK;
}

int32
cluster_grd_lookup_master(const struct ClusterResId *resid pg_attribute_unused())
{
	return cluster_node_id;
}

void
cluster_grd_resid_encode(const LOCKTAG *src, struct ClusterResId *dst)
{
	memset(dst, 0, sizeof(*dst));
	if (src != NULL) {
		dst->type = src->locktag_type;
		dst->lockmethodid = src->locktag_lockmethodid;
	}
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
static uint64 stub_lmd_cancel_enqueue_count = 0;
static uint32 stub_lmd_cancel_last_source = 0;
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

/*
 * spec-5.1c D1 stubs — cluster_ges_send_bast_targeted's local-delivery branch
 * references these backend primitives.  This test never drives a live local
 * holder (it does not call send_bast_targeted), so the stubs are inert and
 * link-only: ProcGlobal is never dereferenced, and SendProcSignal /
 * cluster_grd_bast_local_deliver_ok are never invoked.
 */
void *ProcGlobal = NULL;

int SendProcSignal(int pid, int reason, int backendid);
int
SendProcSignal(int pid pg_attribute_unused(), int reason pg_attribute_unused(),
			   int backendid pg_attribute_unused())
{
	return -1;
}

bool
cluster_grd_bast_local_deliver_ok(uint32 procno pg_attribute_unused(),
								  int proc_count pg_attribute_unused(),
								  uint64 holder_epoch pg_attribute_unused(),
								  uint64 current_epoch pg_attribute_unused(),
								  int target_pid pg_attribute_unused(),
								  int target_backendid pg_attribute_unused())
{
	return false;
}

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

/* spec-2.23 D14 R13 stub audit — new symbol surface introduced by
 * Steps 1-9 needs file-local stubs so cluster_ges.o links standalone
 * in this test binary.  All stubs are minimal inert bodies; behavior
 * coverage lives in TAP 108/109 where real backend wiring runs. */

bool
cluster_grd_outbound_enqueue_backend_request(uint32 d pg_attribute_unused(),
											 const void *p pg_attribute_unused(),
											 uint16 l pg_attribute_unused())
{
	return true;
}

/* spec-2.24 D14 stub audit. */
void
cluster_grd_outbound_enqueue_lmd_cancel(uint32 d pg_attribute_unused(),
										const void *p pg_attribute_unused(),
										uint16 l pg_attribute_unused())
{}
bool
cluster_lmd_cancel_queue_enqueue(uint32 s, const void *p pg_attribute_unused(),
								 uint16 l pg_attribute_unused())
{
	stub_lmd_cancel_enqueue_count++;
	stub_lmd_cancel_last_source = s;
	return true;
}
void
cluster_lmd_cross_node_cancel_queue_full_count_inc(uint64 d pg_attribute_unused())
{}
void
cluster_lmd_cross_node_cancel_received_count_inc(uint64 d pg_attribute_unused())
{}
void
cluster_lmd_cross_node_victim_cancel_sent_count_inc(uint64 d pg_attribute_unused())
{}

void
cluster_grd_inc_bast_sent(void)
{}

/* spec-5.1c D1 — local BAST delivery bumps stale_drop on a guard miss. */
void
cluster_grd_inc_bast_stale_drop(void)
{}

/* spec-2.25 D14 R10 stub audit — native-lock probe 3 NEW symbols.
 * Real behavior tested in test_cluster_native_lock_probe.c (Step 11
 * forward-link).  Here we only need link-surface satisfaction +
 * call counters for T-ges-14..16 dispatch assertions.
 *
 * NOTE:  cannot #include cluster_native_lock_probe.h here because it
 * pulls in cluster_grd.h which conflicts with the local opaque stubs
 * for cluster_grd_entry_enqueue_or_grant et al.  Forward-declare the
 * probe enum + use struct ClusterGrdHolderId opaque (typedef'd in
 * cluster_grd.h but compatible with opaque struct here per C tag/
 * typedef rules — function pointer compares correctly at link time
 * because all callers see the same struct tag). */
static int stub_native_probe_local_calls;
static int stub_native_probe_recv_calls;
static int stub_native_probe_outbound_calls;
static uint32 stub_native_probe_outbound_last_dest;
static uint16 stub_native_probe_outbound_last_len;

/* Forward decls matching real prototypes in
 * src/include/cluster/cluster_native_lock_probe.h. */
typedef enum ClusterNativeLockProbeReply {
	CLUSTER_NATIVE_LOCK_PROBE_CLEAR_LOCAL = 0,
} ClusterNativeLockProbeReplyLocal;

int
cluster_native_lock_probe_local(const void *locktag pg_attribute_unused(),
								int lockmode pg_attribute_unused(),
								const struct ClusterGrdHolderId *eh pg_attribute_unused())
{
	stub_native_probe_local_calls++;
	return CLUSTER_NATIVE_LOCK_PROBE_CLEAR_LOCAL;
}

void
cluster_lms_native_probe_recv_reply(uint64 probe_id pg_attribute_unused(),
									int32 sender pg_attribute_unused(),
									int status pg_attribute_unused())
{
	stub_native_probe_recv_calls++;
}

/* spec-2.27 D2 / D7 R10 stub audit. */
int cluster_ges_retransmit_max_attempts = 5;
int cluster_ges_dedup_max_entries = 8192;

uint64
cluster_lms_get_shard_master_generation(void)
{
	return 0;
}

void
cluster_lms_inc_priority_starvation_observed(void)
{}

int /* ClusterGesDedupLookupStatus */
cluster_ges_dedup_lookup_or_register(const void *key pg_attribute_unused(),
									 uint8 *reply_out pg_attribute_unused(),
									 uint16 reply_buf_len pg_attribute_unused(),
									 uint16 *reply_len_out pg_attribute_unused())
{
	if (reply_len_out)
		*reply_len_out = 0;
	return 0; /* CLUSTER_GES_DEDUP_MISS_REGISTERED */
}

void
cluster_ges_dedup_record_reply(const void *key pg_attribute_unused(),
							   const uint8 *reply pg_attribute_unused(),
							   uint16 reply_len pg_attribute_unused())
{}

bool
cluster_lms_native_probe_required(const struct ClusterResId *resid pg_attribute_unused(),
								  int lockmode pg_attribute_unused())
{
	return false;
}

bool
cluster_lms_native_probe_schedule_grant(
	const struct ClusterResId *resid pg_attribute_unused(), int lockmode pg_attribute_unused(),
	const struct ClusterGrdHolderId *requester pg_attribute_unused(),
	int32 source_node_id pg_attribute_unused(), uint32 request_opcode pg_attribute_unused(),
	uint64 shard_master_generation pg_attribute_unused(),
	int convert_current_mode pg_attribute_unused())
{
	return false;
}

/* spec-5.3 — stubs for the convert wrappers + sync native probe (cluster_ges.c
 * references them; this harness exercises the dispatch/dedup paths, not the
 * convert decision, which is covered by test_cluster_grd). */
bool
cluster_lms_native_probe_wait_clear(
	const struct ClusterResId *resid pg_attribute_unused(), int lockmode pg_attribute_unused(),
	const struct ClusterGrdHolderId *requester pg_attribute_unused(),
	int timeout_ms pg_attribute_unused())
{
	return true;
}

ClusterGrdConvertResult
cluster_grd_convert_or_enqueue(
	const struct ClusterResId *resid pg_attribute_unused(), int32 node_id pg_attribute_unused(),
	uint32 procno pg_attribute_unused(), uint64 cluster_epoch pg_attribute_unused(),
	int current_mode pg_attribute_unused(), int requested_mode pg_attribute_unused(),
	uint64 convert_request_id pg_attribute_unused(), int32 source_node_id pg_attribute_unused(),
	uint64 shard_master_generation pg_attribute_unused(),
	ClusterGrdConflictHolder *conflict_holders_out pg_attribute_unused(),
	int *n_conflict_out pg_attribute_unused())
{
	return CLUSTER_GRD_CONVERT_NOT_READY;
}

int
cluster_grd_release_and_drain(const struct ClusterResId *resid pg_attribute_unused(),
							  const struct ClusterGrdHolderId *holder pg_attribute_unused(),
							  ClusterGrdGrantIdentity *granted_out pg_attribute_unused(),
							  int max_out pg_attribute_unused())
{
	return 0;
}

ClusterGrdEntryResult
cluster_grd_rollback_convert(const struct ClusterResId *resid pg_attribute_unused(),
							 int32 node_id pg_attribute_unused(),
							 uint32 procno pg_attribute_unused(),
							 int upgraded_mode pg_attribute_unused(),
							 int old_mode pg_attribute_unused(),
							 uint64 old_request_id pg_attribute_unused())
{
	return CLUSTER_GRD_ENTRY_NOT_FOUND;
}

int cluster_ges_convert_timeout_ms = 30000;

void
cluster_grd_outbound_enqueue_lms_native_probe(uint32 dest, const void *p pg_attribute_unused(),
											  uint16 len)
{
	stub_native_probe_outbound_calls++;
	stub_native_probe_outbound_last_dest = dest;
	stub_native_probe_outbound_last_len = len;
}

/* cluster_ges_reply_wait API stubs (spec-2.23 D1). */
struct GesReplyWaitKey;
struct GesReplyWaitEntry;
struct GesReplyWaitEntry *
cluster_ges_reply_wait_insert(const struct GesReplyWaitKey *k pg_attribute_unused(),
							  int64 deadline pg_attribute_unused())
{
	return NULL;
}
struct GesReplyWaitEntry *
cluster_ges_reply_wait_lookup(const struct GesReplyWaitKey *k pg_attribute_unused())
{
	return NULL;
}
void
cluster_ges_reply_wait_wake(struct GesReplyWaitEntry *e pg_attribute_unused(),
							uint32 opcode pg_attribute_unused(),
							uint32 reason pg_attribute_unused())
{}
void
cluster_ges_reply_wait_delete(const struct GesReplyWaitKey *k pg_attribute_unused())
{}
void
cluster_ges_inc_release_ack(void)
{}
void
cluster_ges_inc_reply_late_drop(void)
{}

/* cluster_lmd probe collector receive (spec-2.23 D8). */
struct GesDeadlockReportHeader;
bool
cluster_lmd_probe_collect_receive(const struct GesDeadlockReportHeader *r pg_attribute_unused(),
								  Size len pg_attribute_unused())
{
	return false;
}

/* cluster_grd GRD-owned waiter API (spec-2.23 D6). */
struct ClusterGrdConflictHolder;
struct ClusterGrdWaiterIdentity;
ClusterGrdGrantAction
cluster_grd_entry_enqueue_or_grant(const struct ClusterResId *r pg_attribute_unused(),
								   const struct ClusterGrdHolderId *h pg_attribute_unused(),
								   int32 src pg_attribute_unused(),
								   uint64 req_id pg_attribute_unused(),
								   uint64 shard_master_generation pg_attribute_unused(),
								   uint32 op pg_attribute_unused(), int mode pg_attribute_unused(),
								   struct ClusterGrdConflictHolder *out pg_attribute_unused(),
								   int *nout pg_attribute_unused())
{
	if (nout != NULL)
		*nout = 0;
	return CLUSTER_GRD_GRANT_NOW;
}
int
cluster_grd_entry_release_and_pop_compatible_waiter(
	const struct ClusterResId *r pg_attribute_unused(),
	const struct ClusterGrdHolderId *h pg_attribute_unused(),
	struct ClusterGrdWaiterIdentity *out pg_attribute_unused(), int max_out pg_attribute_unused())
{
	return 0;
}

ClusterGrdEntryResult
cluster_grd_release_holder_by_id(const struct ClusterResId *r pg_attribute_unused(),
								 const struct ClusterGrdHolderId *h pg_attribute_unused())
{
	return CLUSTER_GRD_ENTRY_OK;
}

ClusterGrdEntryResult
cluster_grd_cancel_waiter_by_id(const struct ClusterResId *r pg_attribute_unused(),
								const struct ClusterGrdHolderId *h pg_attribute_unused())
{
	return CLUSTER_GRD_ENTRY_NOT_FOUND;
}

/* GUC + PG runtime stubs. */
int cluster_ges_request_timeout_ms = 60000;

/* CHECK_FOR_INTERRUPTS() in the local-master wait loop. */
volatile sig_atomic_t InterruptPending = false;

void
ProcessInterrupts(void)
{}

/* PG_TRY/PG_CATCH machinery referenced by the local-master conflict wait loop.
 * The wait path is never exercised by these tests (no cross-node enqueue), so
 * these only need to satisfy the standalone link. */
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

void
pg_re_throw(void)
{
	abort();
}

bool
DoLockModesConflict(int a pg_attribute_unused(), int b pg_attribute_unused())
{
	return false;
}

int64
GetCurrentTimestamp(void)
{
	return 0;
}

void *MyProc;

#include "storage/condition_variable.h"
void
ConditionVariablePrepareToSleep(ConditionVariable *cv pg_attribute_unused())
{}
bool
ConditionVariableCancelSleep(void)
{
	return false;
}
bool
ConditionVariableTimedSleep(ConditionVariable *cv pg_attribute_unused(),
							long timeout pg_attribute_unused(),
							uint32 wait_event pg_attribute_unused())
{
	return true;
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
	env.payload_length = sizeof(GesRequestPayload);

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
	env.payload_length = sizeof(GesRequestPayload);

	memset(&req, 0, sizeof(req));
	req.opcode = GES_REQ_OPCODE_BAST;
	req.holder_node_id = 0; /* local target, not envelope source */

	cluster_ges_request_handler(&env, &req);

	UT_ASSERT_EQ(stub_inbound_validation_fail, pre_fail);
	UT_ASSERT_EQ(stub_bast_received, pre_bast + 1);
	UT_ASSERT_EQ(stub_work_queue_enqueue_count, pre_enqueue);
}

static void
test_ges_cancel_pending_opcode_validates_as_target_local(void)
{
	ClusterICEnvelope env;
	GesRequestPayload req;
	uint64 pre_fail = stub_inbound_validation_fail;
	uint64 pre_cancel = stub_lmd_cancel_enqueue_count;

	cluster_ges_shmem_init();
	cluster_node_id = 0;
	memset(&env, 0, sizeof(env));
	env.source_node_id = 1; /* coordinator */
	env.epoch = 0;
	env.payload_length = sizeof(GesRequestPayload);

	memset(&req, 0, sizeof(req));
	req.opcode = GES_REQ_OPCODE_CANCEL_PENDING;
	req.holder_node_id = 0; /* local victim target, not envelope source */

	cluster_ges_request_handler(&env, &req);

	UT_ASSERT_EQ(stub_inbound_validation_fail, pre_fail);
	UT_ASSERT_EQ(stub_lmd_cancel_enqueue_count, pre_cancel + 1);
	UT_ASSERT_EQ(stub_lmd_cancel_last_source, (uint32)1);
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
	env.payload_length = sizeof(GesRequestPayload);

	memset(&req, 0, sizeof(req));
	req.opcode = GES_REQ_OPCODE_BAST_ACK;
	req.holder_node_id = 1; /* holder must match envelope source */

	cluster_ges_request_handler(&env, &req);

	UT_ASSERT_EQ(stub_inbound_validation_fail, pre_fail);
	UT_ASSERT_EQ(stub_bast_ack, pre_ack + 1);
	UT_ASSERT_EQ(stub_work_queue_enqueue_count, pre_enqueue);
}

/* ============================================================
 * spec-2.25 T-ges-14..16 — NATIVE_LOCK_PROBE opcode dispatch tests.
 *
 *	T-ges-14:  opcode enum values 9 / 10 + opcode_max = 10 boundary.
 *	T-ges-15:  request handler dispatches to local probe + outbound reply
 *	           (HC33 source/target/sender validation prefix passes).
 *	T-ges-16:  reply handler routes status to LMS collector (recv_reply
 *	           counter increments;  HC36 stale drop deferred to TAP).
 * ============================================================ */

UT_TEST(test_ges_native_lock_probe_opcode_enum_extension)
{
	/* spec-2.25 D6:  opcode 9 + 10 ABI lock. */
	UT_ASSERT_EQ((int)GES_REQ_OPCODE_NATIVE_LOCK_PROBE, 9);
	UT_ASSERT_EQ((int)GES_REQ_OPCODE_NATIVE_LOCK_PROBE_REPLY, 10);
	/* Payload size lock.  spec-5.3 grew the probe REQUEST to 40B (+requester
	 * node/procno identity, so the peer can exclude the original requester's own
	 * sub-SUEX holds from the conflict scan);  the REPLY stays 32B. */
	UT_ASSERT_EQ((int)sizeof(GesNativeLockProbePayload), 40);
	UT_ASSERT_EQ((int)sizeof(GesNativeLockProbeReplyPayload), 32);
}

UT_TEST(test_ges_native_lock_probe_request_dispatch)
{
	/* spec-2.25 D5:  request handler probes local + emits reply. */
	GesNativeLockProbePayload probe;
	ClusterICEnvelope env;
	int pre_local = stub_native_probe_local_calls;
	int pre_outbound = stub_native_probe_outbound_calls;

	memset(&env, 0, sizeof(env));
	env.source_node_id = 7;
	env.epoch = 0;
	env.payload_length = sizeof(probe);

	memset(&probe, 0, sizeof(probe));
	probe.opcode = GES_REQ_OPCODE_NATIVE_LOCK_PROBE;
	probe.lockmode = 5;
	probe.probe_id = 0xDEADBEEFCAFEBABEull;

	cluster_ges_handle_native_lock_probe_request(&env, &probe);

	UT_ASSERT_EQ(stub_native_probe_local_calls, pre_local + 1);
	UT_ASSERT_EQ(stub_native_probe_outbound_calls, pre_outbound + 1);
	UT_ASSERT_EQ(stub_native_probe_outbound_last_dest, 7u); /* echo source */
	UT_ASSERT_EQ((int)stub_native_probe_outbound_last_len, 32);
}

UT_TEST(test_ges_native_lock_probe_reply_dispatch)
{
	/* spec-2.25 D5:  reply handler routes to LMS collector. */
	GesNativeLockProbeReplyPayload reply;
	ClusterICEnvelope env;
	int pre_recv = stub_native_probe_recv_calls;

	memset(&env, 0, sizeof(env));
	env.source_node_id = 3;
	env.epoch = 0;
	env.payload_length = sizeof(reply);

	memset(&reply, 0, sizeof(reply));
	reply.opcode = GES_REQ_OPCODE_NATIVE_LOCK_PROBE_REPLY;
	reply.status = 0; /* CLUSTER_NATIVE_LOCK_PROBE_CLEAR — value 0 */
	reply.probe_id = 0xAAAAAAAA00000001ull;
	reply.sender_node_id = 3; /* HC33 dual-source: == env.source_node_id */

	cluster_ges_handle_native_lock_probe_reply(&env, &reply);

	UT_ASSERT_EQ(stub_native_probe_recv_calls, pre_recv + 1);
}

int
main(int argc pg_attribute_unused(), char *argv[] pg_attribute_unused())
{
	UT_PLAN(16);

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
	UT_RUN(test_ges_cancel_pending_opcode_validates_as_target_local);
	UT_RUN(test_ges_bast_ack_opcode_validates_as_source_holder);
	UT_RUN(test_ges_native_lock_probe_opcode_enum_extension);
	UT_RUN(test_ges_native_lock_probe_request_dispatch);
	UT_RUN(test_ges_native_lock_probe_reply_dispatch);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
