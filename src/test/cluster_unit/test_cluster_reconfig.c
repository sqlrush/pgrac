/*-------------------------------------------------------------------------
 *
 * test_cluster_reconfig.c
 *	  spec-2.29 Sprint A Step 1 unit tests — cluster_reconfig foundation.
 *
 *	  Step 1 cases (this binary):
 *	    T-reconfig-1  ReconfigEvent + ClusterReconfigState sizeof bounds
 *	                  (P2.8 — natural-aligned, StaticAssertDecl ≤ 96 ≥ 64);
 *	                  cluster_reconfig_shmem_size > 0 + shmem_init succeeds
 *	                  + idempotent (init twice safe via found-flag);
 *	                  CLUSTER_RECONFIG_DEAD_BITMAP_BYTES == 16
 *	    T-reconfig-9  cluster_epoch_observe_remote CAS-loop semantics:
 *	                  - initial epoch=0, observe_remote(7) → epoch=7, returns true
 *	                  - observe_remote(7) again → epoch stays 7, returns false
 *	                  - observe_remote(3) (stale) → epoch stays 7, returns false
 *	                  - observe_remote(10) → epoch=10, returns true
 *	                  - CLUSTER_EPOCH_OBSERVE_MAX_JUMP == 16 constant
 *
 *	  Step 2 / Step 3 add T-reconfig-2..8 + T-reconfig-10/11
 *	  (event_id dedup / Q2 A'' rule / mid-tick rotation / PROCSIG handler
 *	  triplet / broadcast-vs-epoch++ split / I6 commit-durable guard /
 *	  envelope tri-branch / declared-peer filter end-to-end).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_reconfig.c
 *
 * NOTES
 *	  pgrac-original file.  Spec:  spec-2.29-reconfig-coordinator-
 *	  internal.md (DRAFT v0.3).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stddef.h>
#include <string.h>

#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_epoch.h"

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

UT_DEFINE_GLOBALS();


/* ============================================================
 * Stubs — link cluster_reconfig.o + cluster_epoch.o standalone.
 *
 *	cluster_reconfig.c (Step 2 body) now references:
 *	  - cluster_conf_lookup_node (declared-peer filter F11)
 *	  - cluster_cssd_get_peer_state (CSSD survivor SSOT P1.1)
 *	  - cluster_cssd_get_dead_generation (P1.2 hash input)
 *	  - cluster_qvotec_in_quorum (I2 in_quorum gate)
 *	  - cluster_node_id (extern int)
 *	  - cluster_enabled (extern bool)
 *	  - IsTransactionState (D4 I6 absorb)
 *	  - GetTopTransactionIdIfAny (D4 writable-tx guard)
 *	  - GetXLogInsertRecPtr (epoch_changed_at_lsn stamp)
 *	  - GetCurrentTimestamp (event applied_at)
 *	  - BackendIdGetProc / SendProcSignal / MaxBackends / MyProcPid
 *	  - cluster_reconfig_start_pending (handler-set sig_atomic_t)
 *	  - cluster_injection_* (D10 injection point callsites)
 *
 *	Unit-test scope: T-2 (compute_event_id determinism), T-3 (publish
 *	dedup via lmon_tick gated path), T-7 (broadcast vs epoch++ split
 *	semantics — verified at compute layer), T-8 (D4 I6 IsTransactionState
 *	absorb path).  T-4/4b/5/5b/6 are best covered by TAP 099 (Step 5)
 *	+ cluster_signal unit T6 (existing).
 * ============================================================ */

#include "storage/shmem.h"
static char reconfig_shmem_storage[256] __attribute__((aligned(64)));
static char epoch_shmem_storage[64] __attribute__((aligned(64)));
static bool reconfig_init_done = false;
static bool epoch_init_done = false;

void *
ShmemInitStruct(const char *name, Size size pg_attribute_unused(), bool *foundPtr)
{
	if (strcmp(name, "pgrac cluster reconfig") == 0) {
		*foundPtr = reconfig_init_done;
		reconfig_init_done = true;
		return reconfig_shmem_storage;
	} else if (strcmp(name, "pgrac cluster epoch") == 0) {
		*foundPtr = epoch_init_done;
		epoch_init_done = true;
		return epoch_shmem_storage;
	}
	*foundPtr = false;
	return NULL;
}

#include "storage/lwlock.h"
void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}
bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}
void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}

#include "cluster/cluster_shmem.h"
void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* errmsg / errhint / errcode helpers — actual errstart / errstart_cold /
 * errfinish stubs are defined below alongside setjmp catcher state. */
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
errhint(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errcode(int s pg_attribute_unused())
{
	return 0;
}

/* Step 2 deps — cluster_reconfig.c lmon_tick body + ProcessInterrupts. */
#include "cluster/cluster_conf.h"
#include "cluster/cluster_cssd.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_signal.h"

bool cluster_enabled = false;
int cluster_node_id = 0;
volatile sig_atomic_t cluster_reconfig_start_pending = 0;
volatile sig_atomic_t InterruptPending = 0;

/* Mocked CSSD / QVOTEC / conf state — tests override via globals. */
static bool ut_in_quorum_value = false;
bool
cluster_qvotec_in_quorum(void)
{
	return ut_in_quorum_value;
}

static ClusterCssdPeerState ut_peer_state[CLUSTER_MAX_NODES];
static uint64 ut_dead_generation = 0;
ClusterCssdPeerState
cluster_cssd_get_peer_state(int32 peer_id)
{
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return CLUSTER_CSSD_PEER_ALIVE;
	return ut_peer_state[peer_id];
}
uint64
cluster_cssd_get_dead_generation(void)
{
	return ut_dead_generation;
}

/* declared-peer set:  bit i set → node i is declared in cluster.conf. */
static bool ut_declared_set[CLUSTER_MAX_NODES];
static ClusterNodeInfo ut_dummy_node;
const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node_id)
{
	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return NULL;
	return ut_declared_set[node_id] ? &ut_dummy_node : NULL;
}

#include "access/transam.h"
#include "access/xact.h"
/* IsTransactionState stub.  D4 ProcessInterrupts I6 absorb path. */
static bool ut_in_tx_state = false;
static TransactionId ut_top_xid = InvalidTransactionId;
bool
IsTransactionState(void)
{
	return ut_in_tx_state;
}
TransactionId
GetTopTransactionIdIfAny(void)
{
	return ut_top_xid;
}

/* Injection framework stubs for D10 callsites in cluster_reconfig.c. */
#include "cluster/cluster_inject.h"
int cluster_injection_armed_count = 0;
void
cluster_injection_run(const char *name pg_attribute_unused())
{}
bool
cluster_injection_should_skip(const char *name pg_attribute_unused())
{
	return false;
}

/* GetCurrentTimestamp + GetXLogInsertRecPtr stubs. */
#include "datatype/timestamp.h"
TimestampTz
GetCurrentTimestamp(void)
{
	return 1700000000000000LL;
}
#include "access/xlogdefs.h"
XLogRecPtr
GetXLogInsertRecPtr(void)
{
	return (XLogRecPtr)0x10000000;
}

/* SRF stubs (Step 3 D5b) — test never invokes cluster_get_reconfig_state but
 * the symbol must link.  Mirrors test_cluster_views.c pattern. */
#include "funcapi.h"
void
InitMaterializedSRF(FunctionCallInfo fcinfo pg_attribute_unused(),
					bits32 flags pg_attribute_unused())
{}
void
tuplestore_putvalues(Tuplestorestate *state pg_attribute_unused(),
					 TupleDesc tdesc pg_attribute_unused(), Datum *values pg_attribute_unused(),
					 bool *isnull pg_attribute_unused())
{}
text *
cstring_to_text(const char *s pg_attribute_unused())
{
	return NULL;
}

/* ProcArray / signal stubs. */
#include "storage/proc.h"
#include "storage/procsignal.h"
int MaxBackends = 0;
int MyProcPid = 99999;
PGPROC *
BackendIdGetProc(BackendId beid pg_attribute_unused())
{
	return NULL;
}
int
SendProcSignal(pid_t pid pg_attribute_unused(), ProcSignalReason r pg_attribute_unused(),
			   BackendId beid pg_attribute_unused())
{
	return 0;
}

/* setjmp-based ereport catcher (mirrors test_cluster_fence pattern). */
#include <setjmp.h>
static sigjmp_buf ut_ereport_jump;
static bool ut_ereport_jump_armed = false;
static int ut_ereport_fired_count = 0;
#undef errstart
#undef errstart_cold
#undef errfinish
bool
errstart(int elevel, const char *d pg_attribute_unused())
{
	return elevel >= 21; /* ERROR threshold */
}
bool
errstart_cold(int elevel, const char *d)
{
	return errstart(elevel, d);
}
void
errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		  const char *fn pg_attribute_unused())
{
	ut_ereport_fired_count++;
	if (ut_ereport_jump_armed)
		siglongjmp(ut_ereport_jump, 1);
}

/* spec-2.34 D4 stub: cluster_reconfig_apply_epoch_bump_as_coordinator
 * calls cluster_gcs_block_on_epoch_advance.  Fixture has no GCS shmem
 * state; stub is a no-op. */
void
cluster_gcs_block_on_epoch_advance(uint64 new_epoch pg_attribute_unused())
{}

/* Reset helper for between-test mock state. */
static void
ut_reset_mocks(void)
{
	int i;
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		ut_peer_state[i] = CLUSTER_CSSD_PEER_ALIVE;
		ut_declared_set[i] = false;
	}
	ut_in_quorum_value = false;
	ut_dead_generation = 0;
	ut_in_tx_state = false;
	ut_top_xid = InvalidTransactionId;
	cluster_enabled = true;
	cluster_node_id = 0;
	cluster_reconfig_start_pending = 0;
	InterruptPending = 0;
	ut_ereport_fired_count = 0;
	ut_ereport_jump_armed = false;
}


/* ============================================================
 * T-reconfig-1 — Foundation: sizeof bounds + shmem layout.
 * ============================================================ */

UT_TEST(test_reconfig_dead_bitmap_bytes_eq_16)
{
	/* P2.8 fix:  dead_bitmap must be uint8[16] = 128 bits for 128
	 * declared nodes (CLUSTER_MAX_NODES).  v0.1's uint64 (64 bits)
	 * was rejected — verify the constant is 16. */
	UT_ASSERT_EQ(CLUSTER_RECONFIG_DEAD_BITMAP_BYTES, 16);
}


UT_TEST(test_reconfig_event_sizeof_bounds)
{
	/* P2.8 fix:  natural-aligned, NOT pg_attribute_packed.  Lower bound
	 * 64 catches accidental field removal;upper bound 96 catches
	 * accidental field bloat. */
	UT_ASSERT(sizeof(ReconfigEvent) >= 64);
	UT_ASSERT(sizeof(ReconfigEvent) <= 96);

	/* Field-level sanity:  expect exactly 80 bytes on 64-bit ABI
	 * with natural alignment (8+4+4 + 8+8+16 + 8+4+4 + 8+8 = 80). */
	UT_ASSERT_EQ(sizeof(ReconfigEvent), 80);
}


UT_TEST(test_reconfig_shmem_size_positive)
{
	Size s = cluster_reconfig_shmem_size();
	/* MAXALIGN(sizeof(ClusterReconfigState)) — must be > sizeof
	 * ReconfigEvent because state struct wraps event + lock + 3
	 * atomic counters. */
	UT_ASSERT(s > sizeof(ReconfigEvent));
	UT_ASSERT(s <= sizeof(reconfig_shmem_storage));
}


UT_TEST(test_reconfig_shmem_init_idempotent)
{
	ReconfigEvent evt;

	reconfig_init_done = false;

	/* First init — found = false branch. */
	cluster_reconfig_shmem_init();
	UT_ASSERT(reconfig_init_done);

	/* get_last_event should populate with never-applied sentinel
	 * (event_id = 0, observer_role = NONE). */
	cluster_reconfig_get_last_event(&evt);
	UT_ASSERT_EQ((unsigned long long)evt.event_id, 0ULL);
	UT_ASSERT_EQ(evt.observer_role, CLUSTER_RECONFIG_OBSERVER_NONE);
	UT_ASSERT_EQ((long long)evt.applied_at, 0LL);

	/* Second init — found = true branch.  Should NOT re-zero state
	 * (postmaster restart preserves shmem on the same shmem segment
	 * for the same process — the found-flag prevents double init). */
	cluster_reconfig_shmem_init();
	cluster_reconfig_get_last_event(&evt);
	UT_ASSERT_EQ((unsigned long long)evt.event_id, 0ULL);
}


UT_TEST(test_reconfig_publish_increments_apply_counter)
{
	ReconfigEvent evt;
	ReconfigEvent in;

	reconfig_init_done = false;
	cluster_enabled = true;
	cluster_reconfig_shmem_init();

	memset(&in, 0, sizeof(in));
	in.event_id = 0xABCDEF;
	in.coordinator_node_id = 0;
	in.old_epoch = 5;
	in.new_epoch = 6;
	in.observer_role = CLUSTER_RECONFIG_OBSERVER_COORDINATOR;
	in.event_seq = 42; /* publish path owns the final monotonic value. */
	in.cssd_dead_generation = 3;

	cluster_reconfig_publish_event(&in);

	cluster_reconfig_get_last_event(&evt);
	UT_ASSERT_EQ((unsigned long long)evt.event_id, 0xABCDEFULL);
	UT_ASSERT_EQ(evt.coordinator_node_id, 0);
	UT_ASSERT_EQ((unsigned long long)evt.new_epoch, 6ULL);
	UT_ASSERT_EQ(evt.observer_role, CLUSTER_RECONFIG_OBSERVER_COORDINATOR);
	UT_ASSERT_EQ((unsigned long long)evt.event_seq, 1ULL);
	UT_ASSERT_EQ((unsigned long long)evt.cssd_dead_generation, 3ULL);
}


UT_TEST(test_reconfig_publish_overwrites_event_seq_monotonically)
{
	ReconfigEvent evt;
	ReconfigEvent in;

	reconfig_init_done = false;
	cluster_reconfig_shmem_init();

	memset(&in, 0, sizeof(in));
	in.event_id = 1;
	in.event_seq = 99;
	in.observer_role = CLUSTER_RECONFIG_OBSERVER_COORDINATOR;
	cluster_reconfig_publish_event(&in);
	cluster_reconfig_get_last_event(&evt);
	UT_ASSERT_EQ((unsigned long long)evt.event_seq, 1ULL);

	in.event_id = 2;
	in.event_seq = 99;
	cluster_reconfig_publish_event(&in);
	cluster_reconfig_get_last_event(&evt);
	UT_ASSERT_EQ((unsigned long long)evt.event_seq, 2ULL);
}


UT_TEST(test_reconfig_broadcast_increments_counter)
{
	reconfig_init_done = false;
	cluster_reconfig_shmem_init();

	/* Real body walks ProcArray; MaxBackends=0 in this unit harness, so
	 * the loop is empty but the invocation counter still advances. */
	cluster_reconfig_broadcast_local_procsig();
	cluster_reconfig_broadcast_local_procsig();

	UT_ASSERT_EQ((unsigned long long)cluster_reconfig_get_procsig_broadcast_count(), 2ULL);
}


/* ============================================================
 * T-reconfig-9 — cluster_epoch_observe_remote CAS-loop semantics
 *                + CLUSTER_EPOCH_OBSERVE_MAX_JUMP constant.
 * ============================================================ */

UT_TEST(test_epoch_observe_remote_advance_from_zero)
{
	bool advanced;

	epoch_init_done = false;
	cluster_epoch_shmem_init();
	UT_ASSERT_EQ((unsigned long long)cluster_epoch_get_current(), 0ULL);

	/* Advance from 0 → 7. */
	advanced = cluster_epoch_observe_remote(7);
	UT_ASSERT(advanced);
	UT_ASSERT_EQ((unsigned long long)cluster_epoch_get_current(), 7ULL);
}


UT_TEST(test_epoch_observe_remote_no_op_equal)
{
	bool advanced;

	epoch_init_done = false;
	cluster_epoch_shmem_init();
	(void)cluster_epoch_observe_remote(7); /* establish baseline */

	/* observe_remote(7) again — local already at 7, no advance. */
	advanced = cluster_epoch_observe_remote(7);
	UT_ASSERT(!advanced);
	UT_ASSERT_EQ((unsigned long long)cluster_epoch_get_current(), 7ULL);
}


UT_TEST(test_epoch_observe_remote_no_retreat)
{
	bool advanced;

	epoch_init_done = false;
	cluster_epoch_shmem_init();
	(void)cluster_epoch_observe_remote(7);

	/* observe_remote(3) — stale, must NOT retreat. */
	advanced = cluster_epoch_observe_remote(3);
	UT_ASSERT(!advanced);
	UT_ASSERT_EQ((unsigned long long)cluster_epoch_get_current(), 7ULL);
}


UT_TEST(test_epoch_observe_remote_monotonic_chain)
{
	epoch_init_done = false;
	cluster_epoch_shmem_init();

	/* Apply a chain of advances + no-ops + retreats;final must be
	 * the max observed, not the last observed. */
	UT_ASSERT(cluster_epoch_observe_remote(5));	  /* 0 → 5 */
	UT_ASSERT(!cluster_epoch_observe_remote(3));  /* stale */
	UT_ASSERT(cluster_epoch_observe_remote(10));  /* 5 → 10 */
	UT_ASSERT(!cluster_epoch_observe_remote(8));  /* stale */
	UT_ASSERT(!cluster_epoch_observe_remote(10)); /* no-op */
	UT_ASSERT(cluster_epoch_observe_remote(11));  /* 10 → 11 */

	UT_ASSERT_EQ((unsigned long long)cluster_epoch_get_current(), 11ULL);
}


UT_TEST(test_epoch_advance_for_reconfig_pre_post_snapshots)
{
	uint64 old_v, new_v;

	epoch_init_done = false;
	cluster_epoch_shmem_init();

	/* From 0 → 1. */
	cluster_epoch_advance_for_reconfig(&old_v, &new_v);
	UT_ASSERT_EQ((unsigned long long)old_v, 0ULL);
	UT_ASSERT_EQ((unsigned long long)new_v, 1ULL);
	UT_ASSERT_EQ((unsigned long long)cluster_epoch_get_current(), 1ULL);

	/* Idempotent — each call advances by exactly 1. */
	cluster_epoch_advance_for_reconfig(&old_v, &new_v);
	UT_ASSERT_EQ((unsigned long long)old_v, 1ULL);
	UT_ASSERT_EQ((unsigned long long)new_v, 2ULL);
	UT_ASSERT_EQ((unsigned long long)cluster_epoch_get_current(), 2ULL);
}


UT_TEST(test_epoch_observe_max_jump_constant)
{
	/* spec-2.29 D18b — bounded jump defense against hostile-spoof
	 * envelope frames.  Caller (D20 envelope verify path) checks
	 * remote - my <= MAX_JUMP before calling observe_remote;
	 * constant must be exactly 16 per spec §3.7-bis + §6 R11. */
	UT_ASSERT_EQ((unsigned long long)CLUSTER_EPOCH_OBSERVE_MAX_JUMP, 16ULL);
}


UT_TEST(test_epoch_changed_at_lsn_set_and_get)
{
	uint64 lsn;

	epoch_init_done = false;
	cluster_epoch_shmem_init();

	UT_ASSERT_EQ((unsigned long long)cluster_epoch_get_changed_at_lsn(), 0ULL);

	cluster_epoch_set_changed_at_lsn(0xDEADBEEFCAFEBABEULL);
	lsn = cluster_epoch_get_changed_at_lsn();
	UT_ASSERT_EQ((unsigned long long)lsn, 0xDEADBEEFCAFEBABEULL);
}


/* ============================================================
 * T-reconfig-2 — compute_event_id deterministic + P1.2 invariants.
 * ============================================================ */

UT_TEST(test_reconfig_compute_event_id_deterministic)
{
	uint8 bmp[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	uint64 id1, id2;

	bmp[0] = 0x02; /* node 1 dead */

	id1 = cluster_reconfig_compute_event_id(bmp, 7);
	id2 = cluster_reconfig_compute_event_id(bmp, 7);
	UT_ASSERT_EQ((unsigned long long)id1, (unsigned long long)id2);
	/* sanity: hash output != 0 (probabilistically); 0 reserved sentinel. */
	UT_ASSERT(id1 != 0);
}


UT_TEST(test_reconfig_compute_event_id_dead_bitmap_sensitivity)
{
	uint8 bmp1[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	uint8 bmp2[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	uint64 id1, id2;

	bmp1[0] = 0x02; /* node 1 dead */
	bmp2[0] = 0x06; /* nodes 1 + 2 dead */

	id1 = cluster_reconfig_compute_event_id(bmp1, 5);
	id2 = cluster_reconfig_compute_event_id(bmp2, 5);
	UT_ASSERT(id1 != id2);
}


UT_TEST(test_reconfig_compute_event_id_dead_gen_sensitivity)
{
	uint8 bmp[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	uint64 id_gen5, id_gen6;

	bmp[0] = 0x02;

	/* P1.2 invariant: same dead_bitmap with different cssd_dead_generation
	 * MUST produce different event_id, so rejoin-then-redeath fires fresh
	 * reconfig event even when bitmap unchanged. */
	id_gen5 = cluster_reconfig_compute_event_id(bmp, 5);
	id_gen6 = cluster_reconfig_compute_event_id(bmp, 6);
	UT_ASSERT(id_gen5 != id_gen6);
}


/* ============================================================
 * T-reconfig-3 — lmon_tick dedup (same event_id → skip).
 * ============================================================ */

UT_TEST(test_reconfig_lmon_tick_dedups_same_event_id)
{
	uint64 first_apply, second_apply;

	ut_reset_mocks();
	reconfig_init_done = false;
	epoch_init_done = false;
	cluster_reconfig_shmem_init();
	cluster_epoch_shmem_init();

	/* Set up: node 0 self in_quorum, node 1 declared + DEAD, node 0 declared. */
	cluster_node_id = 0;
	ut_in_quorum_value = true;
	ut_declared_set[0] = true;
	ut_declared_set[1] = true;
	ut_peer_state[1] = CLUSTER_CSSD_PEER_DEAD;
	ut_dead_generation = 1;

	/* First tick: should fire (apply_counter 0 → 1). */
	cluster_reconfig_lmon_tick();
	first_apply = cluster_reconfig_get_apply_counter();

	/* Second tick: same dead_bitmap + same dead_gen → dedup skip. */
	cluster_reconfig_lmon_tick();
	second_apply = cluster_reconfig_get_apply_counter();

	UT_ASSERT_EQ((unsigned long long)first_apply, 1ULL);
	UT_ASSERT_EQ((unsigned long long)second_apply, 1ULL); /* unchanged */
}


UT_TEST(test_reconfig_lmon_tick_refires_on_dead_gen_bump)
{
	uint64 apply1, apply2;

	ut_reset_mocks();
	reconfig_init_done = false;
	epoch_init_done = false;
	cluster_reconfig_shmem_init();
	cluster_epoch_shmem_init();

	cluster_node_id = 0;
	ut_in_quorum_value = true;
	ut_declared_set[0] = true;
	ut_declared_set[1] = true;
	ut_peer_state[1] = CLUSTER_CSSD_PEER_DEAD;
	ut_dead_generation = 1;

	cluster_reconfig_lmon_tick();
	apply1 = cluster_reconfig_get_apply_counter();

	/* Rejoin-then-redeath:  dead_generation bumps;same dead_bitmap → new
	 * event_id → re-fire (P1.2). */
	ut_dead_generation = 2;
	cluster_reconfig_lmon_tick();
	apply2 = cluster_reconfig_get_apply_counter();

	UT_ASSERT_EQ((unsigned long long)apply1, 1ULL);
	UT_ASSERT_EQ((unsigned long long)apply2, 2ULL);
}


/* ============================================================
 * T-reconfig-4 + 4b — Q2 A'' rule + in_quorum gate.
 * ============================================================ */

UT_TEST(test_reconfig_lmon_tick_skips_when_not_in_quorum)
{
	uint64 apply_before;

	ut_reset_mocks();
	reconfig_init_done = false;
	cluster_reconfig_shmem_init();

	cluster_node_id = 0;
	ut_in_quorum_value = false; /* I2:  not in_quorum */
	ut_declared_set[0] = true;
	ut_declared_set[1] = true;
	ut_peer_state[1] = CLUSTER_CSSD_PEER_DEAD;

	apply_before = cluster_reconfig_get_apply_counter();
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ((unsigned long long)cluster_reconfig_get_apply_counter(),
				 (unsigned long long)apply_before);
}


UT_TEST(test_reconfig_lmon_tick_skips_when_disabled)
{
	uint64 apply_before;

	ut_reset_mocks();
	reconfig_init_done = false;
	cluster_reconfig_shmem_init();

	cluster_enabled = false; /* L20: disable-cluster runtime gate */
	cluster_node_id = 0;
	ut_in_quorum_value = true;
	ut_declared_set[0] = true;
	ut_declared_set[1] = true;
	ut_peer_state[1] = CLUSTER_CSSD_PEER_DEAD;

	apply_before = cluster_reconfig_get_apply_counter();
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ((unsigned long long)cluster_reconfig_get_apply_counter(),
				 (unsigned long long)apply_before);
}


UT_TEST(test_reconfig_lmon_tick_skips_on_empty_dead_bitmap)
{
	uint64 apply_before;

	ut_reset_mocks();
	reconfig_init_done = false;
	cluster_reconfig_shmem_init();

	cluster_node_id = 0;
	ut_in_quorum_value = true;
	ut_declared_set[0] = true;
	ut_declared_set[1] = true;
	/* All peers ALIVE — no dead_bitmap bits set. */

	apply_before = cluster_reconfig_get_apply_counter();
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ((unsigned long long)cluster_reconfig_get_apply_counter(),
				 (unsigned long long)apply_before);
}


UT_TEST(test_reconfig_lmon_tick_undeclared_peer_ignored_F11)
{
	uint64 apply_before;

	ut_reset_mocks();
	reconfig_init_done = false;
	cluster_reconfig_shmem_init();

	cluster_node_id = 0;
	ut_in_quorum_value = true;
	ut_declared_set[0] = true;
	/* node 1 NOT declared.  CSSD peer_state defaults ALIVE (per
	 * cluster_cssd_get_peer_state shmem-NULL-safe behavior).  But our
	 * mock returns ALIVE anyway. */
	ut_declared_set[1] = false;
	ut_peer_state[1] = CLUSTER_CSSD_PEER_DEAD; /* irrelevant — filtered out */

	apply_before = cluster_reconfig_get_apply_counter();
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ((unsigned long long)cluster_reconfig_get_apply_counter(),
				 (unsigned long long)apply_before);
}


/* ============================================================
 * T-reconfig-7 — broadcast vs epoch++ split (P1.3 I7).
 *
 *	When self is the coordinator: epoch advances.
 *	When self is NOT the coordinator: epoch stays (only piggyback via D20
 *	receive path would advance it, which is not exercised here).
 * ============================================================ */

UT_TEST(test_reconfig_lmon_tick_coordinator_advances_epoch)
{
	uint64 epoch_before, epoch_after;
	ReconfigEvent evt;

	ut_reset_mocks();
	reconfig_init_done = false;
	epoch_init_done = false;
	cluster_reconfig_shmem_init();
	cluster_epoch_shmem_init();

	/* self = node 0, node 1 dead → survivor_set = {0} → coordinator = 0 = self */
	cluster_node_id = 0;
	ut_in_quorum_value = true;
	ut_declared_set[0] = true;
	ut_declared_set[1] = true;
	ut_peer_state[1] = CLUSTER_CSSD_PEER_DEAD;
	ut_dead_generation = 1;

	epoch_before = cluster_epoch_get_current();
	cluster_reconfig_lmon_tick();
	epoch_after = cluster_epoch_get_current();

	UT_ASSERT_EQ((unsigned long long)epoch_before, 0ULL);
	UT_ASSERT_EQ((unsigned long long)epoch_after, 1ULL); /* coordinator bumped */

	cluster_reconfig_get_last_event(&evt);
	UT_ASSERT_EQ(evt.coordinator_node_id, 0);
	UT_ASSERT_EQ(evt.observer_role, CLUSTER_RECONFIG_OBSERVER_COORDINATOR);
	UT_ASSERT_EQ((unsigned long long)evt.new_epoch, 1ULL);
}


UT_TEST(test_reconfig_lmon_tick_survivor_does_not_advance_epoch)
{
	uint64 epoch_before, epoch_after;
	ReconfigEvent evt;

	ut_reset_mocks();
	reconfig_init_done = false;
	epoch_init_done = false;
	cluster_reconfig_shmem_init();
	cluster_epoch_shmem_init();

	/* self = node 1, node 2 dead → alive = {0, 1}, coord = 0, self != coord */
	cluster_node_id = 1;
	ut_in_quorum_value = true;
	ut_declared_set[0] = true;
	ut_declared_set[1] = true;
	ut_declared_set[2] = true;
	ut_peer_state[0] = CLUSTER_CSSD_PEER_ALIVE;
	ut_peer_state[2] = CLUSTER_CSSD_PEER_DEAD;
	ut_dead_generation = 1;

	epoch_before = cluster_epoch_get_current();
	cluster_reconfig_lmon_tick();
	epoch_after = cluster_epoch_get_current();

	UT_ASSERT_EQ((unsigned long long)epoch_before, 0ULL);
	/* I7:  non-coord survivor MUST NOT advance epoch — that's coord's job. */
	UT_ASSERT_EQ((unsigned long long)epoch_after, 0ULL);

	cluster_reconfig_get_last_event(&evt);
	UT_ASSERT_EQ(evt.coordinator_node_id, 0);
	UT_ASSERT_EQ(evt.observer_role, CLUSTER_RECONFIG_OBSERVER_SURVIVOR);
}


/* ============================================================
 * T-reconfig-8 — ProcessInterrupts I6 guard (D4).
 *
 *	Verify: when pending=true but IsTransactionState()=false (idle/
 *	post-commit cleanup), no ereport fires.  Pending flag is cleared.
 *	When pending=true AND IsTransactionState()=true, ereport fires
 *	(verified via setjmp catcher).
 * ============================================================ */

UT_TEST(test_reconfig_check_pending_disabled_silent)
{
	ut_reset_mocks();
	cluster_enabled = false;
	cluster_reconfig_start_pending = 1;
	ut_in_tx_state = true;

	cluster_reconfig_check_pending_in_proc_interrupts();

	UT_ASSERT_EQ(ut_ereport_fired_count, 0);
	/* pending NOT cleared when cluster.enabled=off — early return before
	 * read-clear (matches cluster_fence pattern). */
	UT_ASSERT_EQ((int)cluster_reconfig_start_pending, 1);
}


UT_TEST(test_reconfig_check_pending_no_pending_fast_path)
{
	ut_reset_mocks();
	cluster_reconfig_start_pending = 0;
	ut_in_tx_state = true;

	cluster_reconfig_check_pending_in_proc_interrupts();

	UT_ASSERT_EQ(ut_ereport_fired_count, 0);
	UT_ASSERT_EQ((int)cluster_reconfig_start_pending, 0);
}


UT_TEST(test_reconfig_check_pending_idle_absorbs_I6)
{
	ut_reset_mocks();
	cluster_reconfig_start_pending = 1;
	ut_in_tx_state = false; /* idle / post-commit cleanup tail */

	cluster_reconfig_check_pending_in_proc_interrupts();

	UT_ASSERT_EQ(ut_ereport_fired_count, 0);
	/* I6:  pending cleared (read-clear-FIRST) even though we absorbed. */
	UT_ASSERT_EQ((int)cluster_reconfig_start_pending, 0);
}


UT_TEST(test_reconfig_check_pending_read_only_xact_absorbs)
{
	ut_reset_mocks();
	cluster_reconfig_start_pending = 1;
	ut_in_tx_state = true;
	ut_top_xid = InvalidTransactionId; /* no writes yet */
	ut_in_quorum_value = true;

	cluster_reconfig_check_pending_in_proc_interrupts();

	UT_ASSERT_EQ(ut_ereport_fired_count, 0);
	UT_ASSERT_EQ((int)cluster_reconfig_start_pending, 0);
}


UT_TEST(test_reconfig_check_pending_in_tx_quorum_lost_errors)
{
	ut_reset_mocks();
	cluster_reconfig_start_pending = 1;
	ut_in_tx_state = true;
	ut_top_xid = 42;			/* writable tx */
	ut_in_quorum_value = false; /* quorum lost → 53R50 branch */

	if (sigsetjmp(ut_ereport_jump, 1) == 0) {
		ut_ereport_jump_armed = true;
		cluster_reconfig_check_pending_in_proc_interrupts();
		UT_ASSERT(false); /* should have ereport ERROR */
	} else {
		ut_ereport_jump_armed = false;
		UT_ASSERT_EQ(ut_ereport_fired_count, 1);
	}
}


UT_TEST(test_reconfig_check_pending_in_tx_in_quorum_53R60_errors)
{
	ut_reset_mocks();
	cluster_reconfig_start_pending = 1;
	ut_in_tx_state = true;
	ut_top_xid = 42;		   /* writable tx */
	ut_in_quorum_value = true; /* in_quorum → 53R60 reconfig_in_progress */

	if (sigsetjmp(ut_ereport_jump, 1) == 0) {
		ut_ereport_jump_armed = true;
		cluster_reconfig_check_pending_in_proc_interrupts();
		UT_ASSERT(false);
	} else {
		ut_ereport_jump_armed = false;
		UT_ASSERT_EQ(ut_ereport_fired_count, 1);
	}
}


/* ============================================================
 * Main — register + run all tests.
 * ============================================================ */

int
main(void)
{
	UT_PLAN(31);

	/* T-reconfig-1 */
	UT_RUN(test_reconfig_dead_bitmap_bytes_eq_16);
	UT_RUN(test_reconfig_event_sizeof_bounds);
	UT_RUN(test_reconfig_shmem_size_positive);
	UT_RUN(test_reconfig_shmem_init_idempotent);
	UT_RUN(test_reconfig_publish_increments_apply_counter);
	UT_RUN(test_reconfig_publish_overwrites_event_seq_monotonically);
	UT_RUN(test_reconfig_broadcast_increments_counter);

	/* T-reconfig-9 */
	UT_RUN(test_epoch_observe_remote_advance_from_zero);
	UT_RUN(test_epoch_observe_remote_no_op_equal);
	UT_RUN(test_epoch_observe_remote_no_retreat);
	UT_RUN(test_epoch_observe_remote_monotonic_chain);
	UT_RUN(test_epoch_advance_for_reconfig_pre_post_snapshots);
	UT_RUN(test_epoch_observe_max_jump_constant);
	UT_RUN(test_epoch_changed_at_lsn_set_and_get);

	/* T-reconfig-2 — compute_event_id pure-function (P1.2). */
	UT_RUN(test_reconfig_compute_event_id_deterministic);
	UT_RUN(test_reconfig_compute_event_id_dead_bitmap_sensitivity);
	UT_RUN(test_reconfig_compute_event_id_dead_gen_sensitivity);

	/* T-reconfig-3 — lmon_tick dedup. */
	UT_RUN(test_reconfig_lmon_tick_dedups_same_event_id);
	UT_RUN(test_reconfig_lmon_tick_refires_on_dead_gen_bump);

	/* T-reconfig-4 / 4b — Q2 A'' + I2 + L20 + F11 + empty-dead-set gates. */
	UT_RUN(test_reconfig_lmon_tick_skips_when_not_in_quorum);
	UT_RUN(test_reconfig_lmon_tick_skips_when_disabled);
	UT_RUN(test_reconfig_lmon_tick_skips_on_empty_dead_bitmap);
	UT_RUN(test_reconfig_lmon_tick_undeclared_peer_ignored_F11);

	/* T-reconfig-7 — broadcast vs epoch++ split (P1.3 I7). */
	UT_RUN(test_reconfig_lmon_tick_coordinator_advances_epoch);
	UT_RUN(test_reconfig_lmon_tick_survivor_does_not_advance_epoch);

	/* T-reconfig-8 — ProcessInterrupts D4 I6 guard. */
	UT_RUN(test_reconfig_check_pending_disabled_silent);
	UT_RUN(test_reconfig_check_pending_no_pending_fast_path);
	UT_RUN(test_reconfig_check_pending_idle_absorbs_I6);
	UT_RUN(test_reconfig_check_pending_read_only_xact_absorbs);
	UT_RUN(test_reconfig_check_pending_in_tx_quorum_lost_errors);
	UT_RUN(test_reconfig_check_pending_in_tx_in_quorum_53R60_errors);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
