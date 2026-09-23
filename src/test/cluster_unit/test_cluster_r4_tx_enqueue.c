/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_tx_enqueue.c
 *    R4 D9 SOURCE/TARGET transaction-wait and exit-lifecycle contract.
 *
 * The production source is included so this standalone fixture can inspect
 * its private fixed-size waiter slots and prove cleanup after every exit.
 * All external resolver, latch, WFG, epoch and shmem services are scripted.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "cluster/cluster_tx_enqueue.h"
#include "miscadmin.h"
#include "portability/instr_time.h"
#include "storage/proc.h"
#include "utils/elog.h"

/*
 * Exercise the real wait-state writer in this D9 fixture.  Rename its public
 * entry points so the wrappers below can retain the existing observations,
 * and intercept its two write barriers to inject a controlled pending FATAL
 * at the odd-sequence midpoint without adding a production test hook.
 */
static void test_wait_state_write_barrier(void);

extern void test_real_wait_state_init(ClusterLmdProcWaitState *ws);
extern void test_real_wait_state_reset(ClusterLmdProcWaitState *ws);
extern uint64 test_real_wait_state_publish(ClusterLmdProcWaitState *ws, uint8 kind,
										   uint64 request_id, uint64 cluster_epoch,
										   TransactionId xid);
extern void test_real_wait_state_clear(ClusterLmdProcWaitState *ws);
extern ClusterLmdWaitStateReadResult
test_real_wait_state_read_exact(ClusterLmdProcWaitState *ws, ClusterLmdWaitStateSnapshot *out);
extern bool test_real_wait_state_read(ClusterLmdProcWaitState *ws,
									  ClusterLmdWaitStateSnapshot *out);

#define cluster_lmd_wait_state_init test_real_wait_state_init
#define cluster_lmd_wait_state_reset test_real_wait_state_reset
#define cluster_lmd_wait_state_publish test_real_wait_state_publish
#define cluster_lmd_wait_state_clear test_real_wait_state_clear
#define cluster_lmd_wait_state_read_exact test_real_wait_state_read_exact
#define cluster_lmd_wait_state_read test_real_wait_state_read
#undef pg_write_barrier
#define pg_write_barrier() test_wait_state_write_barrier()
#include "../../backend/cluster/cluster_lmd_wait_state.c"
#undef cluster_lmd_wait_state_init
#undef cluster_lmd_wait_state_reset
#undef cluster_lmd_wait_state_publish
#undef cluster_lmd_wait_state_clear
#undef cluster_lmd_wait_state_read_exact
#undef cluster_lmd_wait_state_read

/* Exercise the actual timeout branch without sleeping for a minute. */
static instr_time test_txw_now(void);
static instr_time
test_real_now(void)
{
	instr_time now;

	INSTR_TIME_SET_CURRENT(now);
	return now;
}
#undef INSTR_TIME_SET_CURRENT
#define INSTR_TIME_SET_CURRENT(t) ((t) = test_txw_now())
#include "../../backend/cluster/cluster_tx_enqueue.c"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

#define TEST_NSLOTS 2
#define TEST_XID ((TransactionId)900)
#define TEST_EPOCH UINT64CONST(77)

static char test_txw_storage[MAXALIGN(
	offsetof(ClusterTxwShmem, slots)
	+ TEST_NSLOTS * sizeof(ClusterTxwWaitSlot))] pg_attribute_aligned(MAXIMUM_ALIGNOF);
static PGPROC test_procs[TEST_NSLOTS];
static PROC_HDR test_proc_hdr;

static ClusterTxOutcome test_resolve_outcomes[16];
static ClusterTxResolveReason test_resolve_reasons[16];
static int test_resolve_count;
static int test_resolve_pos;
static uint64 test_epochs[16];
static int test_epoch_count;
static int test_epoch_pos;
static NodeId test_uba_origin;
static bool test_wfg_accept;
static bool test_wfg_live;
static int test_wfg_submit_calls;
static int test_wfg_cancel_calls;
static int test_wfg_exact_cancel_calls;
static ClusterLmdGraphRemoveResult test_wfg_last_remove_result;
static char test_cleanup_events[16];
static int test_cleanup_event_count;
static ClusterLmdVertex test_wfg_waiter;
static ClusterLmdVertex test_wfg_blocker;
static int test_wait_publish_calls;
static int test_wait_clear_calls;
static int test_wait_read_calls;
static uint8 test_wait_kind;
static uint64 test_wait_epoch;
static ClusterLmdWaitStateReadResult test_wait_read_result;
static ClusterLmdWaitStateSnapshot test_wait_snapshot;
static int test_wait_latch_calls;
static int test_set_latch_calls[TEST_NSLOTS];
static bool test_wait_latch_throws;
static bool test_wait_latch_sleeps;
static bool test_wait_latch_delivers_cancel;
static bool test_scripted_clock;
static uint64 test_clock_ms;
static TransactionId test_local_xid;
static bool test_legacy_tt_found;
static ClusterTTStatus test_legacy_tt_status;
static int test_legacy_tt_dispatch_calls;
static bool test_fail_stop_armed;
static int test_last_elevel;
static bool test_exceptional_condition_hit;
static bool test_wait_read_override;
static bool test_mutate_slot_during_wait_read;
static uint32 test_mutated_slot_kind;
static ClusterLmdProcWaitState *test_wait_write_ws;
static int test_wait_write_barrier_count;
static bool test_inject_pending_fatal;
static bool test_pending_fatal_delivery;
static bool test_pending_fatal_saw_odd;
static bool test_pending_fatal_saw_holdoff;
static int test_pending_fatal_deliveries;
static bool test_cancel_token_pending;
static uint64 test_current_mx_stats[CMX_STAT_COUNT];
static sigjmp_buf test_fail_stop_stack;

PGPROC *MyProc;
PROC_HDR *ProcGlobal;
Latch *MyLatch;
int MaxBackends;
ProcessingMode Mode;
BackendType MyBackendType = B_BACKEND;
bool cluster_enabled;
int cluster_node_id;
volatile sig_atomic_t InterruptPending;
volatile sig_atomic_t QueryCancelPending;
volatile uint32 InterruptHoldoffCount;
volatile uint32 QueryCancelHoldoffCount;
volatile uint32 CritSectionCount;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

static instr_time
test_txw_now(void)
{
	instr_time now = test_real_now();

	if (test_scripted_clock)
		now.ticks = (int64)test_clock_ms * NS_PER_MS;
	return now;
}

void
cluster_multixact_current_stats_bump(ClusterCurrentMxStatId stat)
{
	UT_ASSERT(stat >= 0 && stat < CMX_STAT_COUNT);
	test_current_mx_stats[stat]++;
}

bool
cluster_cancel_token_consume(void)
{
	bool consumed = test_cancel_token_pending;

	test_cancel_token_pending = false;
	return consumed;
}

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	test_exceptional_condition_hit = true;
	if (test_fail_stop_armed) {
		test_fail_stop_armed = false;
		siglongjmp(test_fail_stop_stack, 1);
	}
	abort();
}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	test_last_elevel = elevel;
	if (elevel >= ERROR && test_fail_stop_armed) {
		test_fail_stop_armed = false;
		siglongjmp(test_fail_stop_stack, 1);
	}
	if (elevel >= ERROR)
		abort();
	return false;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}

void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size pg_attribute_unused(),
				bool *found)
{
	*found = true;
	return test_txw_storage;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

TransactionId
GetTopTransactionIdIfAny(void)
{
	return test_local_xid;
}

uint64
cluster_epoch_get_current(void)
{
	int pos;

	UT_ASSERT(test_epoch_count > 0);
	pos = test_epoch_pos < test_epoch_count ? test_epoch_pos++ : test_epoch_count - 1;
	return test_epochs[pos];
}

NodeId
uba_origin_node_id(UBA uba pg_attribute_unused())
{
	return test_uba_origin;
}

ClusterTxOutcome
cluster_tx_resolve_exact(const ClusterTxLocator *locator pg_attribute_unused(),
						 ClusterTxResolveMode mode, ClusterTxResolution *out,
						 ClusterTxResolveReason *reason_out)
{
	int pos;

	UT_ASSERT_EQ((int)mode, (int)CLUSTER_TX_RESOLVE_ROW_WAIT);
	UT_ASSERT(test_resolve_count > 0);
	pos = test_resolve_pos < test_resolve_count ? test_resolve_pos++ : test_resolve_count - 1;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (reason_out != NULL)
		*reason_out = test_resolve_reasons[pos];
	return test_resolve_outcomes[pos];
}

ClusterSemanticAdmissionResult
cluster_tt_status_source_dispatch(ClusterTTStatusSourceOp op,
								  const ClusterTTStatusSourceRequest *request,
								  ClusterTTStatusSourceResult *result)
{
	test_legacy_tt_dispatch_calls++;
	UT_ASSERT_EQ((int)op, (int)CLUSTER_TT_SOURCE_LOOKUP);
	UT_ASSERT_NOT_NULL(request);
	UT_ASSERT_NOT_NULL(request->key);
	memset(result, 0, sizeof(*result));
	if (!test_legacy_tt_found)
		return CLUSTER_SEMANTIC_ADMISSION_OK;
	result->bool_value = true;
	result->lookup.authoritative = true;
	result->lookup.status = test_legacy_tt_status;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

uint64
cluster_lmd_wait_state_publish(ClusterLmdProcWaitState *ws, uint8 kind, uint64 request_id,
							   uint64 epoch, TransactionId xid)
{
	uint64 wait_seq;

	test_wait_publish_calls++;
	test_wait_kind = kind;
	test_wait_epoch = epoch;
	UT_ASSERT_EQ(request_id, 0);
	UT_ASSERT_EQ((int)xid, (int)test_local_xid);
	test_wait_write_ws = ws;
	test_wait_write_barrier_count = 0;
	wait_seq = test_real_wait_state_publish(ws, kind, request_id, epoch, xid);
	test_wait_write_ws = NULL;
	test_wait_read_result = test_real_wait_state_read_exact(ws, &test_wait_snapshot);
	return wait_seq;
}

void
cluster_lmd_wait_state_clear(ClusterLmdProcWaitState *ws)
{
	test_wait_clear_calls++;
	UT_ASSERT(test_cleanup_event_count < (int)sizeof(test_cleanup_events));
	test_cleanup_events[test_cleanup_event_count++] = 'W';
	test_wait_write_ws = ws;
	test_wait_write_barrier_count = 0;
	test_real_wait_state_clear(ws);
	test_wait_write_ws = NULL;
	test_wait_read_result = test_real_wait_state_read_exact(ws, &test_wait_snapshot);
}

ClusterLmdWaitStateReadResult
cluster_lmd_wait_state_read_exact(ClusterLmdProcWaitState *ws, ClusterLmdWaitStateSnapshot *out)
{
	ClusterLmdWaitStateReadResult result;

	test_wait_read_calls++;
	if (test_wait_read_override) {
		*out = test_wait_snapshot;
		result = test_wait_read_result;
	} else
		result = test_real_wait_state_read_exact(ws, out);
	if (test_mutate_slot_during_wait_read) {
		ClusterTxw->slots[MyProc->pgprocno].waiting = test_mutated_slot_kind;
		test_mutate_slot_during_wait_read = false;
	}
	return result;
}

bool
cluster_lmd_submit_wait_edge_real(const ClusterLmdVertex *waiter, const ClusterLmdVertex *blocker,
								  uint64 request_id)
{
	test_wfg_submit_calls++;
	test_wfg_waiter = *waiter;
	test_wfg_blocker = *blocker;
	UT_ASSERT_EQ(request_id, 0);
	if (test_wfg_accept)
		test_wfg_live = true;
	return test_wfg_accept;
}

void
cluster_lmd_cancel_wait_edge_real(const ClusterLmdVertex *waiter)
{
	test_wfg_cancel_calls++;
	UT_ASSERT(test_cleanup_event_count < (int)sizeof(test_cleanup_events));
	test_cleanup_events[test_cleanup_event_count++] = 'G';
	UT_ASSERT_EQ(memcmp(waiter, &test_wfg_waiter, sizeof(*waiter)), 0);
	test_wfg_live = false;
}

ClusterLmdGraphRemoveResult
cluster_lmd_graph_remove_edge_by_waiter_exact_result(const ClusterLmdVertex *waiter)
{
	test_wfg_exact_cancel_calls++;
	UT_ASSERT(test_cleanup_event_count < (int)sizeof(test_cleanup_events));
	test_cleanup_events[test_cleanup_event_count++] = 'G';
	if (!test_wfg_live)
		test_wfg_last_remove_result = CLUSTER_LMD_GRAPH_REMOVE_ABSENT;
	else if (waiter->node_id == test_wfg_waiter.node_id && waiter->procno == test_wfg_waiter.procno
			 && waiter->cluster_epoch == test_wfg_waiter.cluster_epoch
			 && waiter->request_id == test_wfg_waiter.request_id
			 && waiter->wait_seq == test_wfg_waiter.wait_seq) {
		test_wfg_live = false;
		test_wfg_last_remove_result = CLUSTER_LMD_GRAPH_REMOVE_REMOVED;
	} else
		test_wfg_last_remove_result = CLUSTER_LMD_GRAPH_REMOVE_STALE;
	return test_wfg_last_remove_result;
}

void
ResetLatch(Latch *latch pg_attribute_unused())
{}

void
SetLatch(Latch *latch)
{
	int i;

	for (i = 0; i < TEST_NSLOTS; i++)
		if (latch == &test_procs[i].procLatch)
			test_set_latch_calls[i]++;
}

void
pg_usleep(long microsec)
{
	struct timespec delay;

	delay.tv_sec = microsec / 1000000L;
	delay.tv_nsec = (microsec % 1000000L) * 1000L;
	(void)nanosleep(&delay, NULL);
}

int
WaitLatch(Latch *latch pg_attribute_unused(), int wakeEvents pg_attribute_unused(), long timeout,
		  uint32 wait_event_info pg_attribute_unused())
{
	test_wait_latch_calls++;
	if (test_scripted_clock) {
		UT_ASSERT(timeout > 0 && timeout <= CLUSTER_TXW_TICK_MS);
		test_clock_ms += UINT64_C(75000);
	}
	if (test_wait_latch_throws)
		siglongjmp(*PG_exception_stack, 1);
	if (test_wait_latch_delivers_cancel)
		test_cancel_token_pending = true;
	if (test_wait_latch_sleeps)
		pg_usleep((long)Max(timeout, 2) * 1000L);
	return WL_TIMEOUT;
}

TimestampTz
GetCurrentTimestamp(void)
{
	return 0;
}

void
ProcessInterrupts(void)
{
	if (test_pending_fatal_delivery && InterruptPending) {
		if (!INTERRUPTS_CAN_BE_PROCESSED())
			return;
		InterruptPending = false;
		test_pending_fatal_deliveries++;
		cluster_tx_enqueue_cleanup_on_backend_exit_callback(0, (Datum)0);
		return;
	}
	InterruptPending = false;
}

static void
test_wait_state_write_barrier(void)
{
	test_wait_write_barrier_count++;
	if (!test_inject_pending_fatal || test_wait_write_barrier_count != 1)
		return;

	UT_ASSERT_NOT_NULL(test_wait_write_ws);
	test_pending_fatal_saw_odd = (pg_atomic_read_u32(&test_wait_write_ws->change_seq) & 1U) != 0;
	test_pending_fatal_saw_holdoff = InterruptHoldoffCount > 0;
	InterruptPending = true;
	test_pending_fatal_delivery = true;
	ProcessInterrupts();
}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

static ClusterTxLocator
test_locator(void)
{
	ClusterTxLocator locator;

	memset(&locator, 0, sizeof(locator));
	locator.uba = uba_encode(1, 9, 3, 4);
	locator.xid = TEST_XID;
	locator.tt_wrap = 5;
	locator.itl_kind = ITL_FLAG_ACTIVE;
	locator.itl_slot_index = 2;
	return locator;
}

static ClusterTTStatusKey
test_source_key(void)
{
	ClusterTTStatusKey key;

	memset(&key, 0, sizeof(key));
	key.origin_node_id = 3;
	key.undo_segment_id = 4;
	key.tt_slot_id = 5;
	key.cluster_epoch = 6;
	key.local_xid = TEST_XID;
	return key;
}

static void
prepare_owned_active_wait(uint32 slot_kind, bool insert_edge)
{
	ClusterLmdVertex blocker;
	uint64 wait_seq;

	UT_ASSERT(slot_kind == CLUSTER_TXW_SLOT_SOURCE || slot_kind == CLUSTER_TXW_SLOT_TARGET);
	if (slot_kind == CLUSTER_TXW_SLOT_SOURCE) {
		ClusterTTStatusKey source = test_source_key();

		txw_slot_set(0, &source, false);
	} else {
		ClusterTxLocator locator = test_locator();

		UT_ASSERT(txw_target_slot_set_if_free(0, &locator));
	}
	wait_seq = cluster_lmd_wait_state_publish(&MyProc->cluster_lmd_wait, CLUSTER_LMD_WAIT_TX, 0,
											  TEST_EPOCH, test_local_xid);
	txw_exact_waiter_vertex(&test_wfg_waiter, 0, TEST_EPOCH, test_local_xid, wait_seq);
	memset(&blocker, 0, sizeof(blocker));
	test_wfg_blocker = blocker;
	if (insert_edge)
		UT_ASSERT(cluster_lmd_submit_wait_edge_real(&test_wfg_waiter, &blocker, 0));
}

static void
script_resolve(int index, ClusterTxOutcome outcome, ClusterTxResolveReason reason)
{
	UT_ASSERT(index >= 0 && index < (int)lengthof(test_resolve_outcomes));
	test_resolve_outcomes[index] = outcome;
	test_resolve_reasons[index] = reason;
	if (test_resolve_count <= index)
		test_resolve_count = index + 1;
}

static void
reset_fixture(void)
{
	ClusterTxwShmem *state = (ClusterTxwShmem *)test_txw_storage;

	memset(test_txw_storage, 0, sizeof(test_txw_storage));
	state->nslots = TEST_NSLOTS;
	pg_atomic_init_u32(&state->active_waiters, 0);
	pg_atomic_init_u64(&state->wait_count, 0);
	pg_atomic_init_u64(&state->wakeup_count, 0);
	pg_atomic_init_u64(&state->timeout_count, 0);
	ClusterTxw = state;

	memset(test_procs, 0, sizeof(test_procs));
	test_procs[0].pgprocno = 0;
	test_procs[1].pgprocno = 1;
	memset(&test_proc_hdr, 0, sizeof(test_proc_hdr));
	test_proc_hdr.allProcs = test_procs;
	test_proc_hdr.allProcCount = TEST_NSLOTS;
	ProcGlobal = &test_proc_hdr;
	MyProc = &test_procs[0];
	MyLatch = &test_procs[0].procLatch;
	test_real_wait_state_init(&MyProc->cluster_lmd_wait);
	MaxBackends = TEST_NSLOTS;
	Mode = NormalProcessing;
	cluster_enabled = true;
	cluster_node_id = 1;

	memset(test_resolve_outcomes, 0, sizeof(test_resolve_outcomes));
	memset(test_resolve_reasons, 0, sizeof(test_resolve_reasons));
	test_resolve_count = 0;
	test_resolve_pos = 0;
	memset(test_epochs, 0, sizeof(test_epochs));
	test_epochs[0] = TEST_EPOCH;
	test_epoch_count = 1;
	test_epoch_pos = 0;
	test_uba_origin = 0;
	test_wfg_accept = true;
	test_wfg_live = false;
	test_wfg_submit_calls = 0;
	test_wfg_cancel_calls = 0;
	test_wfg_exact_cancel_calls = 0;
	test_wfg_last_remove_result = CLUSTER_LMD_GRAPH_REMOVE_ABSENT;
	memset(test_cleanup_events, 0, sizeof(test_cleanup_events));
	test_cleanup_event_count = 0;
	memset(&test_wfg_waiter, 0, sizeof(test_wfg_waiter));
	memset(&test_wfg_blocker, 0, sizeof(test_wfg_blocker));
	test_wait_publish_calls = 0;
	test_wait_clear_calls = 0;
	test_wait_read_calls = 0;
	test_wait_kind = 0;
	test_wait_epoch = 0;
	test_wait_read_result = CLUSTER_LMD_WAIT_STATE_READ_INACTIVE;
	memset(&test_wait_snapshot, 0, sizeof(test_wait_snapshot));
	test_wait_latch_calls = 0;
	memset(test_set_latch_calls, 0, sizeof(test_set_latch_calls));
	memset(test_current_mx_stats, 0, sizeof(test_current_mx_stats));
	test_wait_latch_throws = false;
	test_wait_latch_sleeps = false;
	test_wait_latch_delivers_cancel = false;
	test_scripted_clock = false;
	test_clock_ms = 0;
	test_local_xid = (TransactionId)700;
	test_legacy_tt_found = false;
	test_legacy_tt_status = CLUSTER_TT_STATUS_IN_PROGRESS;
	test_legacy_tt_dispatch_calls = 0;
	test_fail_stop_armed = false;
	test_last_elevel = 0;
	test_exceptional_condition_hit = false;
	test_wait_read_override = false;
	test_mutate_slot_during_wait_read = false;
	test_mutated_slot_kind = CLUSTER_TXW_SLOT_FREE;
	test_wait_write_ws = NULL;
	test_wait_write_barrier_count = 0;
	test_inject_pending_fatal = false;
	test_pending_fatal_delivery = false;
	test_pending_fatal_saw_odd = false;
	test_pending_fatal_saw_holdoff = false;
	test_pending_fatal_deliveries = 0;
	test_cancel_token_pending = false;
	InterruptPending = false;
	QueryCancelPending = false;
	InterruptHoldoffCount = 0;
	QueryCancelHoldoffCount = 0;
	CritSectionCount = 0;
}

static void
assert_slot_clean(void)
{
	UT_ASSERT_EQ(ClusterTxw->slots[0].waiting, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterTxw->active_waiters), 0);
}

static bool
invoke_exit_callback_expect_fail_stop(void)
{
	if (sigsetjmp(test_fail_stop_stack, 1) == 0) {
		test_fail_stop_armed = true;
		cluster_tx_enqueue_cleanup_on_backend_exit_callback(0, (Datum)0);
		test_fail_stop_armed = false;
		return false;
	}
	test_fail_stop_armed = false;
	return true;
}

static bool
invoke_process_interrupts_expect_fail_stop(void)
{
	if (sigsetjmp(test_fail_stop_stack, 1) == 0) {
		test_fail_stop_armed = true;
		ProcessInterrupts();
		test_fail_stop_armed = false;
		return false;
	}
	test_fail_stop_armed = false;
	return true;
}

typedef struct TestD9AuthoritySnapshot {
	ClusterTxwWaitSlot slot;
	uint32 active_waiters;
	ClusterLmdProcWaitState wait_state;
	bool wfg_live;
	ClusterLmdVertex wfg_waiter;
	ClusterLmdVertex wfg_blocker;
} TestD9AuthoritySnapshot;

static TestD9AuthoritySnapshot
snapshot_d9_authorities(void)
{
	TestD9AuthoritySnapshot snapshot;

	memset(&snapshot, 0, sizeof(snapshot));
	memcpy(&snapshot.slot, &ClusterTxw->slots[0], sizeof(snapshot.slot));
	snapshot.active_waiters = pg_atomic_read_u32(&ClusterTxw->active_waiters);
	memcpy(&snapshot.wait_state, &MyProc->cluster_lmd_wait, sizeof(snapshot.wait_state));
	snapshot.wfg_live = test_wfg_live;
	snapshot.wfg_waiter = test_wfg_waiter;
	snapshot.wfg_blocker = test_wfg_blocker;
	return snapshot;
}

static void
assert_d9_authorities_equal(const TestD9AuthoritySnapshot *expected)
{
	UT_ASSERT_EQ(memcmp(&ClusterTxw->slots[0], &expected->slot, sizeof(expected->slot)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterTxw->active_waiters), expected->active_waiters);
	UT_ASSERT_EQ(
		memcmp(&MyProc->cluster_lmd_wait, &expected->wait_state, sizeof(expected->wait_state)), 0);
	UT_ASSERT_EQ(test_wfg_live, expected->wfg_live);
	UT_ASSERT_EQ(memcmp(&test_wfg_waiter, &expected->wfg_waiter, sizeof(expected->wfg_waiter)), 0);
	UT_ASSERT_EQ(memcmp(&test_wfg_blocker, &expected->wfg_blocker, sizeof(expected->wfg_blocker)),
				 0);
}

static void
assert_panic_before_cleanup_mutation(const TestD9AuthoritySnapshot *before)
{
	UT_ASSERT(invoke_exit_callback_expect_fail_stop());
	UT_ASSERT_EQ(test_last_elevel, PANIC);
	UT_ASSERT(!test_exceptional_condition_hit);
	UT_ASSERT_EQ(test_cleanup_event_count, 0);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 0);
	UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 0);
	UT_ASSERT_EQ(test_wait_clear_calls, 0);
	assert_d9_authorities_equal(before);
}

UT_TEST(test_exact_wait_abi_and_shmem_size_are_frozen)
{
	reset_fixture();
	UT_ASSERT_EQ(CLUSTER_TXW_RESOLVED, 0);
	UT_ASSERT_EQ(CLUSTER_TXW_TIMEOUT, 1);
	UT_ASSERT_EQ(CLUSTER_TXW_DEAD_HOLDER, 2);
	UT_ASSERT_EQ(CLUSTER_TXW_RETRY, 3);
	UT_ASSERT_EQ(CLUSTER_TXW_UNPROVABLE, 4);
	UT_ASSERT_EQ(sizeof(ClusterTTStatusKey), 24);
	UT_ASSERT_EQ(__alignof__(ClusterTTStatusKey), 4);
	UT_ASSERT_EQ(sizeof(ClusterTxLocator), 24);
	UT_ASSERT_EQ(__alignof__(ClusterTxLocator), 8);
	UT_ASSERT_EQ(sizeof(ClusterTxwIdentity), 24);
	UT_ASSERT_EQ(__alignof__(ClusterTxwIdentity), 4);
	UT_ASSERT_EQ(offsetof(ClusterTxwWaitSlot, waiting), 24);
	UT_ASSERT_EQ(sizeof(ClusterTxwWaitSlot), 28);
	UT_ASSERT_EQ(__alignof__(ClusterTxwWaitSlot), 4);
	UT_ASSERT_EQ(cluster_tx_enqueue_shmem_size(),
				 MAXALIGN(offsetof(ClusterTxwShmem, slots) + TEST_NSLOTS * 28));
}

UT_TEST(test_fixed_false_precedes_malformed_and_shared_state)
{
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_fixture();
	ClusterTxw = NULL;
	script_resolve(0, CLUSTER_TX_UNKNOWN, CLUSTER_TX_RESOLVE_TARGET_DISABLED);
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(NULL, 1, &reason), CLUSTER_TXW_UNPROVABLE);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_TARGET_DISABLED);
	UT_ASSERT_EQ(test_resolve_pos, 1);
}

UT_TEST(test_initial_terminal_never_registers)
{
	ClusterTxLocator locator = test_locator();
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_fixture();
	script_resolve(0, CLUSTER_TX_COMMITTED, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, 1, &reason), CLUSTER_TXW_RESOLVED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterTxw->wait_count), 0);
	UT_ASSERT_EQ(test_wait_publish_calls, 0);
	assert_slot_clean();
}

UT_TEST(test_live_post_registration_terminal_race_resolves)
{
	ClusterTxLocator locator = test_locator();
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_fixture();
	script_resolve(0, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
	script_resolve(1, CLUSTER_TX_ABORTED, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, 1000, &reason), CLUSTER_TXW_RESOLVED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(test_resolve_pos, 2);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterTxw->wait_count), 1);
	UT_ASSERT_EQ(test_wait_publish_calls, 1);
	UT_ASSERT_EQ(test_wait_clear_calls, 1);
	UT_ASSERT_EQ(test_wfg_submit_calls, 1);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 0);
	UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 1);
	UT_ASSERT_EQ(test_wfg_last_remove_result, CLUSTER_LMD_GRAPH_REMOVE_REMOVED);
	UT_ASSERT_EQ(test_wait_epoch, TEST_EPOCH);
	UT_ASSERT_EQ(test_wfg_waiter.node_id, cluster_node_id);
	UT_ASSERT_EQ(test_wfg_waiter.cluster_epoch, TEST_EPOCH);
	UT_ASSERT_EQ(test_wfg_blocker.node_id, test_uba_origin);
	UT_ASSERT_EQ(test_wfg_blocker.procno, CLUSTER_LMD_TX_HOLDER_PROCNO);
	UT_ASSERT_EQ(test_wfg_blocker.cluster_epoch, TEST_EPOCH);
	UT_ASSERT_EQ((int)test_wfg_blocker.xid, (int)locator.xid);
	UT_ASSERT_EQ(test_wait_latch_calls, 0);
	assert_slot_clean();
}

UT_TEST(test_prepared_waits_and_lost_wakes_only_repoll)
{
	ClusterTxLocator locator = test_locator();
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_fixture();
	script_resolve(0, CLUSTER_TX_PREPARED, CLUSTER_TX_RESOLVE_NONE);
	script_resolve(1, CLUSTER_TX_PREPARED, CLUSTER_TX_RESOLVE_NONE);
	script_resolve(2, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
	script_resolve(3, CLUSTER_TX_COMMITTED, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, 1000, &reason), CLUSTER_TXW_RESOLVED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(test_wait_latch_calls, 2);
	assert_slot_clean();
}

UT_TEST(test_unknown_fails_before_registration_with_exact_reason)
{
	ClusterTxLocator locator = test_locator();
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_fixture();
	script_resolve(0, CLUSTER_TX_UNKNOWN, CLUSTER_TX_RESOLVE_COVERAGE_GAP);
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, 1, &reason), CLUSTER_TXW_UNPROVABLE);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_COVERAGE_GAP);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterTxw->wait_count), 0);
	assert_slot_clean();
}

UT_TEST(test_formation_drift_cleans_registered_state)
{
	ClusterTxLocator locator = test_locator();
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_fixture();
	script_resolve(0, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
	test_epochs[0] = TEST_EPOCH;
	test_epochs[1] = TEST_EPOCH;
	test_epochs[2] = TEST_EPOCH;
	test_epochs[3] = TEST_EPOCH;
	test_epochs[4] = TEST_EPOCH + 1;
	test_epoch_count = 5;
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, 1000, &reason), CLUSTER_TXW_UNPROVABLE);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_RF_DEFERRED);
	UT_ASSERT_EQ(test_wait_clear_calls, 1);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 0);
	UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 1);
	assert_slot_clean();
}

UT_TEST(test_zero_epoch_is_a_valid_stable_formation)
{
	ClusterTxLocator locator = test_locator();
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_fixture();
	script_resolve(0, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
	script_resolve(1, CLUSTER_TX_ABORTED, CLUSTER_TX_RESOLVE_NONE);
	test_epochs[0] = 0;
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, 1000, &reason), CLUSTER_TXW_RESOLVED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(test_wait_publish_calls, 1);
	UT_ASSERT_EQ(test_wfg_submit_calls, 1);
	assert_slot_clean();
}

UT_TEST(test_zero_to_nonzero_epoch_drift_fails_closed)
{
	ClusterTxLocator locator = test_locator();
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_fixture();
	script_resolve(0, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
	test_epochs[0] = 0;
	test_epochs[1] = 0;
	test_epochs[2] = 0;
	test_epochs[3] = 0;
	test_epochs[4] = 1;
	test_epoch_count = 5;
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, 1000, &reason), CLUSTER_TXW_UNPROVABLE);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_RF_DEFERRED);
	UT_ASSERT_EQ(test_wait_clear_calls, 1);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 0);
	UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 1);
	assert_slot_clean();
}

UT_TEST(test_monotonic_timeout_uses_only_timeout_counter)
{
	ClusterTxLocator locator = test_locator();
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_fixture();
	script_resolve(0, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
	test_wait_latch_sleeps = true;
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, 1, &reason), CLUSTER_TXW_TIMEOUT);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_TIMEOUT);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterTxw->timeout_count), 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterTxw->wait_count), 1);
	assert_slot_clean();
}

UT_TEST(test_exact_wait_consumes_deadlock_token_before_repoll)
{
	ClusterTxLocator locator = test_locator();
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_fixture();
	script_resolve(0, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
	script_resolve(1, CLUSTER_TX_COMMITTED, CLUSTER_TX_RESOLVE_NONE);
	test_cancel_token_pending = true;
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, 1000, &reason), CLUSTER_TXW_DEADLOCK);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT(!test_cancel_token_pending);
	UT_ASSERT_EQ(test_resolve_pos, 1);
	UT_ASSERT_EQ(test_wait_latch_calls, 0);
	UT_ASSERT_EQ(test_wait_clear_calls, 1);
	UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 1);
	UT_ASSERT(!test_wfg_live);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterTxw->timeout_count), 0);
	assert_slot_clean();
}

UT_TEST(test_perpetual_wait_survives_age_and_resolves_commit_or_abort)
{
	int leg;

	for (leg = 0; leg < 2; leg++) {
		ClusterTxLocator locator = test_locator();
		ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

		reset_fixture();
		test_scripted_clock = true;
		script_resolve(0, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
		script_resolve(1, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
		script_resolve(2, CLUSTER_TX_PREPARED, CLUSTER_TX_RESOLVE_NONE);
		script_resolve(3, leg == 0 ? CLUSTER_TX_COMMITTED : CLUSTER_TX_ABORTED,
					   CLUSTER_TX_RESOLVE_NONE);
		UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, -1, &reason), CLUSTER_TXW_RESOLVED);
		UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
		UT_ASSERT_EQ(test_wait_latch_calls, 2);
		UT_ASSERT_EQ(test_clock_ms, UINT64_C(150000));
		UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterTxw->timeout_count), 0);
		UT_ASSERT_EQ(test_wait_clear_calls, 1);
		UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 1);
		UT_ASSERT(!test_wfg_live);
		assert_slot_clean();
	}
}

UT_TEST(test_nonperpetual_budget_values_still_expire)
{
	const int budgets[] = { 0, -2, 60000 };
	size_t i;

	for (i = 0; i < lengthof(budgets); i++) {
		ClusterTxLocator locator = test_locator();
		ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

		reset_fixture();
		test_scripted_clock = true;
		script_resolve(0, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
		script_resolve(1, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
		script_resolve(2, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
		script_resolve(3, CLUSTER_TX_COMMITTED, CLUSTER_TX_RESOLVE_NONE);
		UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, budgets[i], &reason),
					 CLUSTER_TXW_TIMEOUT);
		UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_TIMEOUT);
		UT_ASSERT_EQ(test_wait_latch_calls, 1);
		UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterTxw->timeout_count), 1);
		assert_slot_clean();
	}
}

UT_TEST(test_perpetual_wait_keeps_deadlock_epoch_and_unknown_exits)
{
	int leg;

	for (leg = 0; leg < 3; leg++) {
		ClusterTxLocator locator = test_locator();
		ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

		reset_fixture();
		test_scripted_clock = true;
		script_resolve(0, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
		script_resolve(1, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
		script_resolve(2, CLUSTER_TX_UNKNOWN, CLUSTER_TX_RESOLVE_PROTOCOL);
		if (leg == 0)
			test_wait_latch_delivers_cancel = true;
		if (leg == 1) {
			int j;

			for (j = 0; j < 7; j++)
				test_epochs[j] = TEST_EPOCH;
			test_epochs[7] = TEST_EPOCH + 1;
			test_epoch_count = 8;
		}
		UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, -1, &reason),
					 leg == 0 ? CLUSTER_TXW_DEADLOCK : CLUSTER_TXW_UNPROVABLE);
		UT_ASSERT_EQ(reason, leg == 0	? CLUSTER_TX_RESOLVE_NONE
							 : leg == 1 ? CLUSTER_TX_RESOLVE_RF_DEFERRED
										: CLUSTER_TX_RESOLVE_PROTOCOL);
		UT_ASSERT_EQ(test_wait_clear_calls, 1);
		UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 1);
		UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterTxw->timeout_count), 0);
		UT_ASSERT(!test_wfg_live);
		assert_slot_clean();
	}
}

UT_TEST(test_exact_wait_consumes_deadlock_token_after_latch_wake)
{
	ClusterTxLocator locator = test_locator();
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_fixture();
	script_resolve(0, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
	script_resolve(1, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
	script_resolve(2, CLUSTER_TX_COMMITTED, CLUSTER_TX_RESOLVE_NONE);
	test_wait_latch_delivers_cancel = true;
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, 1000, &reason), CLUSTER_TXW_DEADLOCK);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT(!test_cancel_token_pending);
	UT_ASSERT_EQ(test_resolve_pos, 2);
	UT_ASSERT_EQ(test_wait_latch_calls, 1);
	UT_ASSERT_EQ(test_wait_clear_calls, 1);
	UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 1);
	UT_ASSERT(!test_wfg_live);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterTxw->timeout_count), 0);
	assert_slot_clean();
}

UT_TEST(test_reentrant_source_and_target_slots_are_not_overwritten)
{
	ClusterTxLocator locator = test_locator();
	ClusterTTStatusKey source = test_source_key();
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	unsigned char before[sizeof(ClusterTxwWaitSlot)];

	reset_fixture();
	txw_slot_set(0, &source, false);
	memcpy(before, &ClusterTxw->slots[0], sizeof(before));
	script_resolve(0, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, 1, &reason), CLUSTER_TXW_UNPROVABLE);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_REENTRANT);
	UT_ASSERT_EQ(memcmp(before, &ClusterTxw->slots[0], sizeof(before)), 0);

	reset_fixture();
	memcpy(&ClusterTxw->slots[0], &locator, sizeof(locator));
	ClusterTxw->slots[0].waiting = 2;
	pg_atomic_write_u32(&ClusterTxw->active_waiters, 1);
	memcpy(before, &ClusterTxw->slots[0], sizeof(before));
	script_resolve(0, CLUSTER_TX_PREPARED, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, 1, &reason), CLUSTER_TXW_UNPROVABLE);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_REENTRANT);
	UT_ASSERT_EQ(memcmp(before, &ClusterTxw->slots[0], sizeof(before)), 0);

	reset_fixture();
	memcpy(&ClusterTxw->slots[0], &locator, sizeof(locator));
	ClusterTxw->slots[0].waiting = CLUSTER_TXW_SLOT_TARGET;
	pg_atomic_write_u32(&ClusterTxw->active_waiters, 1);
	memcpy(before, &ClusterTxw->slots[0], sizeof(before));
	test_legacy_tt_found = true;
	test_legacy_tt_status = CLUSTER_TT_STATUS_COMMITTED;
	UT_ASSERT_EQ(cluster_tx_enqueue_wait(&source, 1), CLUSTER_TXW_UNPROVABLE);
	UT_ASSERT_EQ(memcmp(before, &ClusterTxw->slots[0], sizeof(before)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterTxw->active_waiters), 1);
}

UT_TEST(test_wfg_capacity_refusal_runs_full_cleanup)
{
	ClusterTxLocator locator = test_locator();
	ClusterTTStatusKey source = test_source_key();
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_fixture();
	script_resolve(0, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
	test_wfg_accept = false;
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, 1000, &reason), CLUSTER_TXW_UNPROVABLE);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_CAPACITY);
	UT_ASSERT_EQ(test_wait_publish_calls, 1);
	UT_ASSERT_EQ(test_wait_clear_calls, 1);
	UT_ASSERT_EQ(test_wfg_submit_calls, 1);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 0);
	assert_slot_clean();

	reset_fixture();
	test_wfg_accept = false;
	test_legacy_tt_found = true;
	test_legacy_tt_status = CLUSTER_TT_STATUS_COMMITTED;
	UT_ASSERT_EQ(cluster_tx_enqueue_wait(&source, 1000), CLUSTER_TXW_UNPROVABLE);
	UT_ASSERT_EQ(test_wfg_submit_calls, 1);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 0);
	UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 0);
	UT_ASSERT_EQ(test_wait_clear_calls, 1);
	assert_slot_clean();
}

UT_TEST(test_error_longjmp_runs_same_cleanup_funnel)
{
	volatile int leg;

	for (leg = 0; leg < 2; leg++) {
		ClusterTxLocator locator = test_locator();
		ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
		volatile bool caught = false;

		reset_fixture();
		script_resolve(0, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
		test_wait_latch_throws = true;
		PG_TRY();
		{
			(void)cluster_tx_enqueue_wait_exact(&locator, leg == 0 ? 1000 : -1, &reason);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(test_wait_clear_calls, 1);
		UT_ASSERT_EQ(test_wfg_cancel_calls, 0);
		UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 1);
		assert_slot_clean();
	}
}

UT_TEST(test_hint_waker_matches_source_discriminant_only)
{
	ClusterTTStatusKey source = test_source_key();

	reset_fixture();
	memcpy(&ClusterTxw->slots[0], &source, sizeof(source));
	ClusterTxw->slots[0].waiting = 1;
	memcpy(&ClusterTxw->slots[1], &source, sizeof(source));
	ClusterTxw->slots[1].waiting = 2;
	pg_atomic_write_u32(&ClusterTxw->active_waiters, 2);
	cluster_txw_wake_waiters(&source);
	UT_ASSERT_EQ(test_set_latch_calls[0], 1);
	UT_ASSERT_EQ(test_set_latch_calls[1], 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterTxw->wakeup_count), 1);
}

UT_TEST(test_current_mx_waker_counts_only_current_source)
{
	ClusterTTStatusKey source = test_source_key();

	reset_fixture();
	UT_ASSERT(txw_slot_set(0, &source, true));
	UT_ASSERT(txw_slot_set(1, &source, false));
	cluster_txw_wake_waiters(&source);
	UT_ASSERT_EQ(test_set_latch_calls[0], 1);
	UT_ASSERT_EQ(test_set_latch_calls[1], 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterTxw->wakeup_count), 2);
	UT_ASSERT_EQ(test_current_mx_stats[CMX_STAT_WAKEUP], 1);
}

UT_TEST(test_current_mx_source_wait_consumes_deadlock_token_after_cleanup)
{
	ClusterTTStatusKey source = test_source_key();
	uint64 deadline_mono_us = 0;

	reset_fixture();
	test_cancel_token_pending = true;
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_current_mx(&source, 1000, &deadline_mono_us),
				 CLUSTER_TXW_DEADLOCK);
	UT_ASSERT(deadline_mono_us != 0);
	UT_ASSERT(!test_cancel_token_pending);
	UT_ASSERT_EQ(test_wait_clear_calls, 1);
	UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 1);
	assert_slot_clean();
}

UT_TEST(test_current_mx_active_poll_retries_without_dormant_source)
{
	ClusterTTStatusKey source = test_source_key();
	uint64 deadline_mono_us = 0;
	uint64 frozen_deadline_mono_us;

	reset_fixture();
	/* A terminal legacy SOURCE row must not decide an active R4 wait. */
	test_legacy_tt_found = true;
	test_legacy_tt_status = CLUSTER_TT_STATUS_COMMITTED;
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_current_mx(&source, 1000, &deadline_mono_us),
				 CLUSTER_TXW_RETRY);
	UT_ASSERT(deadline_mono_us != 0);
	frozen_deadline_mono_us = deadline_mono_us;
	UT_ASSERT_EQ(test_legacy_tt_dispatch_calls, 0);
	UT_ASSERT_EQ(test_wait_latch_calls, 1);
	UT_ASSERT_EQ(test_wait_clear_calls, 1);
	UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 1);
	assert_slot_clean();

	/* A full tuple/DESCRIBE retry keeps the original operation deadline. */
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_current_mx(&source, 1000, &deadline_mono_us),
				 CLUSTER_TXW_RETRY);
	UT_ASSERT_EQ(deadline_mono_us, frozen_deadline_mono_us);
	UT_ASSERT_EQ(test_legacy_tt_dispatch_calls, 0);
	UT_ASSERT_EQ(test_wait_latch_calls, 2);
	assert_slot_clean();

	/* An exhausted retained budget times out after exact registration cleanup. */
	deadline_mono_us = 1;
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_current_mx(&source, 1000, &deadline_mono_us),
				 CLUSTER_TXW_TIMEOUT);
	UT_ASSERT_EQ(deadline_mono_us, 1);
	UT_ASSERT_EQ(test_legacy_tt_dispatch_calls, 0);
	UT_ASSERT_EQ(test_wait_latch_calls, 2);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterTxw->timeout_count), 1);
	assert_slot_clean();
}

/*
 * A backend can leave the exact wait through proc_exit/FATAL rather than the
 * stack PG_FINALLY.  The before_shmem_exit hook must remove this backend's
 * exact SOURCE or TARGET ownership, and it must be idempotent so pgprocno
 * reuse can immediately register a fresh wait.
 */
extern void cluster_tx_enqueue_cleanup_on_backend_exit_callback(int code, Datum arg);

UT_TEST(test_backend_exit_cleans_exact_target_and_allows_procno_reuse)
{
	ClusterTxLocator locator = test_locator();
	ClusterLmdVertex waiter;
	ClusterLmdVertex blocker;
	uint64 wait_seq;

	reset_fixture();
	UT_ASSERT(txw_target_slot_set_if_free(0, &locator));
	wait_seq = cluster_lmd_wait_state_publish(&MyProc->cluster_lmd_wait, CLUSTER_LMD_WAIT_TX, 0,
											  TEST_EPOCH, test_local_xid);
	txw_exact_waiter_vertex(&waiter, 0, TEST_EPOCH, test_local_xid, wait_seq);
	memset(&blocker, 0, sizeof(blocker));
	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&waiter, &blocker, 0));

	cluster_tx_enqueue_cleanup_on_backend_exit_callback(0, (Datum)0);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 0);
	UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 1);
	UT_ASSERT_EQ(test_wfg_last_remove_result, CLUSTER_LMD_GRAPH_REMOVE_REMOVED);
	UT_ASSERT(!test_wfg_live);
	UT_ASSERT_EQ(test_wait_clear_calls, 1);
	UT_ASSERT_EQ(test_wait_read_result, CLUSTER_LMD_WAIT_STATE_READ_INACTIVE);
	assert_slot_clean();

	/* Re-entry after cleanup is a no-op, and the reused procno is writable. */
	cluster_tx_enqueue_cleanup_on_backend_exit_callback(0, (Datum)0);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 0);
	UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 1);
	UT_ASSERT_EQ(test_wait_clear_calls, 1);
	UT_ASSERT(txw_target_slot_set_if_free(0, &locator));
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterTxw->active_waiters), 1);
	txw_slot_clear(0, CLUSTER_TXW_SLOT_TARGET);
}

UT_TEST(test_backend_exit_cleans_exact_source_and_allows_procno_reuse)
{
	ClusterTxLocator locator = test_locator();
	ClusterTTStatusKey source = test_source_key();
	ClusterLmdVertex waiter;
	ClusterLmdVertex blocker;
	uint64 wait_seq;

	reset_fixture();
	txw_slot_set(0, &source, false);
	wait_seq = cluster_lmd_wait_state_publish(&MyProc->cluster_lmd_wait, CLUSTER_LMD_WAIT_TX, 0,
											  TEST_EPOCH, test_local_xid);
	txw_exact_waiter_vertex(&waiter, 0, TEST_EPOCH, test_local_xid, wait_seq);
	memset(&blocker, 0, sizeof(blocker));
	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&waiter, &blocker, 0));

	cluster_tx_enqueue_cleanup_on_backend_exit_callback(0, (Datum)0);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 0);
	UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 1);
	UT_ASSERT_EQ(test_wfg_last_remove_result, CLUSTER_LMD_GRAPH_REMOVE_REMOVED);
	UT_ASSERT(!test_wfg_live);
	UT_ASSERT_EQ(test_wait_clear_calls, 1);
	UT_ASSERT_EQ(test_wait_read_result, CLUSTER_LMD_WAIT_STATE_READ_INACTIVE);
	assert_slot_clean();

	/* A duplicate callback is harmless and the procno can be reused now. */
	cluster_tx_enqueue_cleanup_on_backend_exit_callback(0, (Datum)0);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 0);
	UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 1);
	UT_ASSERT_EQ(test_wait_clear_calls, 1);
	UT_ASSERT(txw_target_slot_set_if_free(0, &locator));
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterTxw->active_waiters), 1);
	txw_slot_clear(0, CLUSTER_TXW_SLOT_TARGET);
}

UT_TEST(test_backend_exit_inactive_state_cleans_owned_slot_without_wfg_cancel)
{
	ClusterTxLocator locator = test_locator();

	reset_fixture();
	UT_ASSERT(txw_target_slot_set_if_free(0, &locator));
	cluster_tx_enqueue_cleanup_on_backend_exit_callback(0, (Datum)0);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 0);
	UT_ASSERT_EQ(test_wait_clear_calls, 0);
	assert_slot_clean();
}

UT_TEST(test_source_normal_cleanup_cancels_wfg_before_state_and_slot)
{
	ClusterTTStatusKey source = test_source_key();

	reset_fixture();
	test_legacy_tt_found = true;
	test_legacy_tt_status = CLUSTER_TT_STATUS_COMMITTED;
	UT_ASSERT_EQ(cluster_tx_enqueue_wait(&source, 1000), CLUSTER_TXW_RESOLVED);
	UT_ASSERT_EQ(test_cleanup_event_count, 2);
	UT_ASSERT_EQ(test_cleanup_events[0], 'G');
	UT_ASSERT_EQ(test_cleanup_events[1], 'W');
	UT_ASSERT_EQ(test_wfg_cancel_calls, 0);
	UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 1);
	assert_slot_clean();
}

UT_TEST(test_source_error_cleanup_cancels_wfg_before_state_and_slot)
{
	ClusterTTStatusKey source = test_source_key();
	bool caught = false;

	reset_fixture();
	test_wait_latch_throws = true;
	PG_TRY();
	{
		(void)cluster_tx_enqueue_wait(&source, 1000);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(test_cleanup_event_count, 2);
	UT_ASSERT_EQ(test_cleanup_events[0], 'G');
	UT_ASSERT_EQ(test_cleanup_events[1], 'W');
	UT_ASSERT_EQ(test_wfg_cancel_calls, 0);
	UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 1);
	assert_slot_clean();
}

UT_TEST(test_backend_exit_free_before_slot_is_noop_without_panic)
{
	TestD9AuthoritySnapshot before;

	reset_fixture();
	before = snapshot_d9_authorities();
	UT_ASSERT(!invoke_exit_callback_expect_fail_stop());
	UT_ASSERT_EQ(test_last_elevel, 0);
	UT_ASSERT_EQ(test_wait_read_calls, 0);
	UT_ASSERT_EQ(test_cleanup_event_count, 0);
	assert_d9_authorities_equal(&before);
}

UT_TEST(test_backend_exit_source_slot_before_publish_cleans_slot_only)
{
	ClusterTTStatusKey source = test_source_key();

	reset_fixture();
	txw_slot_set(0, &source, false);
	UT_ASSERT(!invoke_exit_callback_expect_fail_stop());
	UT_ASSERT_EQ(test_last_elevel, 0);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 0);
	UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 0);
	UT_ASSERT_EQ(test_wait_clear_calls, 0);
	assert_slot_clean();
}

UT_TEST(test_backend_exit_target_active_before_edge_cancels_exact_absence_then_state_slot)
{
	reset_fixture();
	prepare_owned_active_wait(CLUSTER_TXW_SLOT_TARGET, false);
	UT_ASSERT(!invoke_exit_callback_expect_fail_stop());
	UT_ASSERT_EQ(test_last_elevel, 0);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 0);
	UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 1);
	UT_ASSERT_EQ(test_wfg_last_remove_result, CLUSTER_LMD_GRAPH_REMOVE_ABSENT);
	UT_ASSERT_EQ(test_cleanup_event_count, 2);
	UT_ASSERT_EQ(test_cleanup_events[0], 'G');
	UT_ASSERT_EQ(test_cleanup_events[1], 'W');
	assert_slot_clean();
}

UT_TEST(test_backend_exit_source_active_before_edge_cancels_exact_absence_then_state_slot)
{
	reset_fixture();
	prepare_owned_active_wait(CLUSTER_TXW_SLOT_SOURCE, false);
	UT_ASSERT(!invoke_exit_callback_expect_fail_stop());
	UT_ASSERT_EQ(test_last_elevel, 0);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 0);
	UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 1);
	UT_ASSERT_EQ(test_wfg_last_remove_result, CLUSTER_LMD_GRAPH_REMOVE_ABSENT);
	UT_ASSERT_EQ(test_cleanup_event_count, 2);
	UT_ASSERT_EQ(test_cleanup_events[0], 'G');
	UT_ASSERT_EQ(test_cleanup_events[1], 'W');
	assert_slot_clean();
}

UT_TEST(test_backend_exit_after_edge_cancel_recancels_absence_then_state_slot)
{
	reset_fixture();
	prepare_owned_active_wait(CLUSTER_TXW_SLOT_TARGET, true);
	UT_ASSERT_EQ(cluster_lmd_graph_remove_edge_by_waiter_exact_result(&test_wfg_waiter),
				 CLUSTER_LMD_GRAPH_REMOVE_REMOVED);
	test_wfg_exact_cancel_calls = 0;
	test_cleanup_event_count = 0;
	UT_ASSERT(!invoke_exit_callback_expect_fail_stop());
	UT_ASSERT_EQ(test_last_elevel, 0);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 0);
	UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 1);
	UT_ASSERT_EQ(test_wfg_last_remove_result, CLUSTER_LMD_GRAPH_REMOVE_ABSENT);
	UT_ASSERT_EQ(test_cleanup_event_count, 2);
	UT_ASSERT_EQ(test_cleanup_events[0], 'G');
	UT_ASSERT_EQ(test_cleanup_events[1], 'W');
	assert_slot_clean();
}

UT_TEST(test_backend_exit_after_inactive_publish_cleans_slot_only)
{
	reset_fixture();
	prepare_owned_active_wait(CLUSTER_TXW_SLOT_TARGET, true);
	UT_ASSERT_EQ(cluster_lmd_graph_remove_edge_by_waiter_exact_result(&test_wfg_waiter),
				 CLUSTER_LMD_GRAPH_REMOVE_REMOVED);
	cluster_lmd_wait_state_clear(&MyProc->cluster_lmd_wait);
	test_wfg_exact_cancel_calls = 0;
	test_wait_clear_calls = 0;
	test_cleanup_event_count = 0;
	UT_ASSERT(!invoke_exit_callback_expect_fail_stop());
	UT_ASSERT_EQ(test_last_elevel, 0);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 0);
	UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 0);
	UT_ASSERT_EQ(test_wait_clear_calls, 0);
	UT_ASSERT_EQ(test_cleanup_event_count, 0);
	assert_slot_clean();
}

UT_TEST(test_backend_exit_free_after_slot_release_is_noop_without_panic)
{
	ClusterTxLocator locator = test_locator();
	TestD9AuthoritySnapshot before;

	reset_fixture();
	UT_ASSERT(txw_target_slot_set_if_free(0, &locator));
	txw_slot_clear(0, CLUSTER_TXW_SLOT_TARGET);
	before = snapshot_d9_authorities();
	UT_ASSERT(!invoke_exit_callback_expect_fail_stop());
	UT_ASSERT_EQ(test_last_elevel, 0);
	UT_ASSERT_EQ(test_wait_read_calls, 0);
	UT_ASSERT_EQ(test_cleanup_event_count, 0);
	assert_d9_authorities_equal(&before);
}

UT_TEST(test_pending_fatal_during_wait_state_publish_finishes_even_before_exit)
{
	ClusterTxLocator locator = test_locator();
	bool caught = false;

	reset_fixture();
	UT_ASSERT(txw_target_slot_set_if_free(0, &locator));
	test_inject_pending_fatal = true;
	if (sigsetjmp(test_fail_stop_stack, 1) == 0) {
		test_fail_stop_armed = true;
		(void)cluster_lmd_wait_state_publish(&MyProc->cluster_lmd_wait, CLUSTER_LMD_WAIT_TX, 0,
											 TEST_EPOCH, test_local_xid);
		test_fail_stop_armed = false;
	} else {
		test_fail_stop_armed = false;
		caught = true;
	}
	test_inject_pending_fatal = false;
	UT_ASSERT(!caught);
	if (caught) {
		test_real_wait_state_reset(&MyProc->cluster_lmd_wait);
		txw_slot_clear(0, CLUSTER_TXW_SLOT_TARGET);
		return;
	}
	UT_ASSERT(test_pending_fatal_saw_odd);
	UT_ASSERT(test_pending_fatal_saw_holdoff);
	UT_ASSERT_EQ(test_pending_fatal_deliveries, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_lmd_wait.change_seq) & 1U, 0);
	UT_ASSERT(InterruptPending);
	UT_ASSERT(!invoke_process_interrupts_expect_fail_stop());
	UT_ASSERT_EQ(test_pending_fatal_deliveries, 1);
	UT_ASSERT_EQ(test_last_elevel, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_lmd_wait.change_seq) & 1U, 0);
	assert_slot_clean();
}

UT_TEST(test_pending_fatal_during_wait_state_clear_finishes_even_before_exit)
{
	bool caught = false;

	reset_fixture();
	prepare_owned_active_wait(CLUSTER_TXW_SLOT_TARGET, true);
	UT_ASSERT_EQ(cluster_lmd_graph_remove_edge_by_waiter_exact_result(&test_wfg_waiter),
				 CLUSTER_LMD_GRAPH_REMOVE_REMOVED);
	test_wfg_exact_cancel_calls = 0;
	test_cleanup_event_count = 0;
	test_inject_pending_fatal = true;
	if (sigsetjmp(test_fail_stop_stack, 1) == 0) {
		test_fail_stop_armed = true;
		cluster_lmd_wait_state_clear(&MyProc->cluster_lmd_wait);
		test_fail_stop_armed = false;
	} else {
		test_fail_stop_armed = false;
		caught = true;
	}
	test_inject_pending_fatal = false;
	UT_ASSERT(!caught);
	if (caught) {
		test_real_wait_state_reset(&MyProc->cluster_lmd_wait);
		txw_slot_clear(0, CLUSTER_TXW_SLOT_TARGET);
		return;
	}
	UT_ASSERT(test_pending_fatal_saw_odd);
	UT_ASSERT(test_pending_fatal_saw_holdoff);
	UT_ASSERT_EQ(test_pending_fatal_deliveries, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_lmd_wait.change_seq) & 1U, 0);
	UT_ASSERT(InterruptPending);
	UT_ASSERT(!invoke_process_interrupts_expect_fail_stop());
	UT_ASSERT_EQ(test_pending_fatal_deliveries, 1);
	UT_ASSERT_EQ(test_last_elevel, 0);
	UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 0);
	UT_ASSERT_EQ(test_wait_clear_calls, 1);
	assert_slot_clean();
}

UT_TEST(test_backend_exit_wrong_request_panics_before_any_mutation)
{
	TestD9AuthoritySnapshot before;

	reset_fixture();
	prepare_owned_active_wait(CLUSTER_TXW_SLOT_TARGET, true);
	UT_ASSERT_EQ(test_real_wait_state_read_exact(&MyProc->cluster_lmd_wait, &test_wait_snapshot),
				 CLUSTER_LMD_WAIT_STATE_READ_ACTIVE);
	test_wait_snapshot.request_id = 99;
	test_wait_read_result = CLUSTER_LMD_WAIT_STATE_READ_ACTIVE;
	test_wait_read_override = true;
	before = snapshot_d9_authorities();
	assert_panic_before_cleanup_mutation(&before);
}

UT_TEST(test_backend_exit_zero_wait_seq_panics_before_any_mutation)
{
	TestD9AuthoritySnapshot before;

	reset_fixture();
	prepare_owned_active_wait(CLUSTER_TXW_SLOT_TARGET, true);
	UT_ASSERT_EQ(test_real_wait_state_read_exact(&MyProc->cluster_lmd_wait, &test_wait_snapshot),
				 CLUSTER_LMD_WAIT_STATE_READ_ACTIVE);
	test_wait_snapshot.wait_seq = 0;
	test_wait_read_result = CLUSTER_LMD_WAIT_STATE_READ_ACTIVE;
	test_wait_read_override = true;
	before = snapshot_d9_authorities();
	assert_panic_before_cleanup_mutation(&before);
}

UT_TEST(test_backend_exit_changed_slot_kind_panics_before_any_mutation)
{
	ClusterLmdProcWaitState wait_before;

	reset_fixture();
	prepare_owned_active_wait(CLUSTER_TXW_SLOT_SOURCE, true);
	memcpy(&wait_before, &MyProc->cluster_lmd_wait, sizeof(wait_before));
	test_mutate_slot_during_wait_read = true;
	test_mutated_slot_kind = CLUSTER_TXW_SLOT_TARGET;
	UT_ASSERT(invoke_exit_callback_expect_fail_stop());
	UT_ASSERT_EQ(test_last_elevel, PANIC);
	UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 0);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 0);
	UT_ASSERT_EQ(test_wait_clear_calls, 0);
	UT_ASSERT(test_wfg_live);
	UT_ASSERT_EQ(ClusterTxw->slots[0].waiting, CLUSTER_TXW_SLOT_TARGET);
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterTxw->active_waiters), 1);
	UT_ASSERT_EQ(memcmp(&MyProc->cluster_lmd_wait, &wait_before, sizeof(wait_before)), 0);

	reset_fixture();
	prepare_owned_active_wait(CLUSTER_TXW_SLOT_TARGET, true);
	memcpy(&wait_before, &MyProc->cluster_lmd_wait, sizeof(wait_before));
	test_mutate_slot_during_wait_read = true;
	test_mutated_slot_kind = CLUSTER_TXW_SLOT_FREE;
	UT_ASSERT(invoke_exit_callback_expect_fail_stop());
	UT_ASSERT_EQ(test_last_elevel, PANIC);
	UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 0);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 0);
	UT_ASSERT_EQ(test_wait_clear_calls, 0);
	UT_ASSERT(test_wfg_live);
	UT_ASSERT_EQ(ClusterTxw->slots[0].waiting, CLUSTER_TXW_SLOT_FREE);
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterTxw->active_waiters), 1);
	UT_ASSERT_EQ(memcmp(&MyProc->cluster_lmd_wait, &wait_before, sizeof(wait_before)), 0);

	reset_fixture();
	ClusterTxw->slots[0].waiting = 4;
	pg_atomic_write_u32(&ClusterTxw->active_waiters, 1);
	memcpy(&wait_before, &MyProc->cluster_lmd_wait, sizeof(wait_before));
	UT_ASSERT(invoke_exit_callback_expect_fail_stop());
	UT_ASSERT_EQ(test_last_elevel, PANIC);
	UT_ASSERT_EQ(test_wait_read_calls, 0);
	UT_ASSERT_EQ(test_cleanup_event_count, 0);
	UT_ASSERT_EQ(ClusterTxw->slots[0].waiting, 4);
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterTxw->active_waiters), 1);
	UT_ASSERT_EQ(memcmp(&MyProc->cluster_lmd_wait, &wait_before, sizeof(wait_before)), 0);
}

UT_TEST(test_backend_exit_active_counter_underflow_panics_before_any_mutation)
{
	TestD9AuthoritySnapshot before;

	reset_fixture();
	prepare_owned_active_wait(CLUSTER_TXW_SLOT_TARGET, true);
	pg_atomic_write_u32(&ClusterTxw->active_waiters, 0);
	before = snapshot_d9_authorities();
	assert_panic_before_cleanup_mutation(&before);
}

UT_TEST(test_backend_exit_busy_state_fails_stop_without_releasing_owner)
{
	ClusterTxLocator locator = test_locator();
	TestD9AuthoritySnapshot before;

	reset_fixture();
	UT_ASSERT(txw_target_slot_set_if_free(0, &locator));
	test_wait_read_override = true;
	test_wait_read_result = CLUSTER_LMD_WAIT_STATE_READ_BUSY;
	before = snapshot_d9_authorities();
	assert_panic_before_cleanup_mutation(&before);
	txw_slot_clear(0, CLUSTER_TXW_SLOT_TARGET);
}

UT_TEST(test_backend_exit_malformed_active_state_fails_stop_without_releasing_owner)
{
	ClusterTxLocator locator = test_locator();
	TestD9AuthoritySnapshot before;

	reset_fixture();
	UT_ASSERT(txw_target_slot_set_if_free(0, &locator));
	test_wait_read_override = true;
	test_wait_read_result = CLUSTER_LMD_WAIT_STATE_READ_ACTIVE;
	test_wait_snapshot.active = true;
	test_wait_snapshot.kind = CLUSTER_LMD_WAIT_GES;
	test_wait_snapshot.wait_seq = 1;
	before = snapshot_d9_authorities();
	assert_panic_before_cleanup_mutation(&before);
	txw_slot_clear(0, CLUSTER_TXW_SLOT_TARGET);
}

UT_TEST(test_backend_exit_counter_underflow_fails_stop_without_freeing_slot)
{
	ClusterTxLocator locator = test_locator();
	TestD9AuthoritySnapshot before;

	reset_fixture();
	memcpy(ClusterTxw->slots[0].identity.target_locator_words, &locator, sizeof(locator));
	ClusterTxw->slots[0].waiting = CLUSTER_TXW_SLOT_TARGET;
	before = snapshot_d9_authorities();
	assert_panic_before_cleanup_mutation(&before);
	ClusterTxw->slots[0].waiting = CLUSTER_TXW_SLOT_FREE;
}

int
main(void)
{
	UT_PLAN(43);
	UT_RUN(test_exact_wait_abi_and_shmem_size_are_frozen);
	UT_RUN(test_fixed_false_precedes_malformed_and_shared_state);
	UT_RUN(test_initial_terminal_never_registers);
	UT_RUN(test_live_post_registration_terminal_race_resolves);
	UT_RUN(test_prepared_waits_and_lost_wakes_only_repoll);
	UT_RUN(test_unknown_fails_before_registration_with_exact_reason);
	UT_RUN(test_formation_drift_cleans_registered_state);
	UT_RUN(test_zero_epoch_is_a_valid_stable_formation);
	UT_RUN(test_zero_to_nonzero_epoch_drift_fails_closed);
	UT_RUN(test_monotonic_timeout_uses_only_timeout_counter);
	UT_RUN(test_perpetual_wait_survives_age_and_resolves_commit_or_abort);
	UT_RUN(test_nonperpetual_budget_values_still_expire);
	UT_RUN(test_perpetual_wait_keeps_deadlock_epoch_and_unknown_exits);
	UT_RUN(test_exact_wait_consumes_deadlock_token_before_repoll);
	UT_RUN(test_exact_wait_consumes_deadlock_token_after_latch_wake);
	UT_RUN(test_reentrant_source_and_target_slots_are_not_overwritten);
	UT_RUN(test_wfg_capacity_refusal_runs_full_cleanup);
	UT_RUN(test_error_longjmp_runs_same_cleanup_funnel);
	UT_RUN(test_hint_waker_matches_source_discriminant_only);
	UT_RUN(test_current_mx_waker_counts_only_current_source);
	UT_RUN(test_current_mx_source_wait_consumes_deadlock_token_after_cleanup);
	UT_RUN(test_current_mx_active_poll_retries_without_dormant_source);
	UT_RUN(test_backend_exit_cleans_exact_target_and_allows_procno_reuse);
	UT_RUN(test_backend_exit_cleans_exact_source_and_allows_procno_reuse);
	UT_RUN(test_backend_exit_inactive_state_cleans_owned_slot_without_wfg_cancel);
	UT_RUN(test_source_normal_cleanup_cancels_wfg_before_state_and_slot);
	UT_RUN(test_source_error_cleanup_cancels_wfg_before_state_and_slot);
	UT_RUN(test_backend_exit_free_before_slot_is_noop_without_panic);
	UT_RUN(test_backend_exit_source_slot_before_publish_cleans_slot_only);
	UT_RUN(test_backend_exit_target_active_before_edge_cancels_exact_absence_then_state_slot);
	UT_RUN(test_backend_exit_source_active_before_edge_cancels_exact_absence_then_state_slot);
	UT_RUN(test_backend_exit_after_edge_cancel_recancels_absence_then_state_slot);
	UT_RUN(test_backend_exit_after_inactive_publish_cleans_slot_only);
	UT_RUN(test_backend_exit_free_after_slot_release_is_noop_without_panic);
	UT_RUN(test_pending_fatal_during_wait_state_publish_finishes_even_before_exit);
	UT_RUN(test_pending_fatal_during_wait_state_clear_finishes_even_before_exit);
	UT_RUN(test_backend_exit_wrong_request_panics_before_any_mutation);
	UT_RUN(test_backend_exit_zero_wait_seq_panics_before_any_mutation);
	UT_RUN(test_backend_exit_changed_slot_kind_panics_before_any_mutation);
	UT_RUN(test_backend_exit_active_counter_underflow_panics_before_any_mutation);
	UT_RUN(test_backend_exit_busy_state_fails_stop_without_releasing_owner);
	UT_RUN(test_backend_exit_malformed_active_state_fails_stop_without_releasing_owner);
	UT_RUN(test_backend_exit_counter_underflow_fails_stop_without_freeing_slot);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
