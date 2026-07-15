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
#include "cluster/cluster_write_fence.h" /* spec-4.12 D4 marker submit stubs */

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

/* spec-5.22e D5-8 stub: cluster_membership_set_state notes self-admission
 * into the undo horizon shmem (cluster_undo_horizon_ic.c not linked here);
 * also satisfies the cluster_node_id extern via cluster_guc.o linkage or
 * local definition in this binary. */
void cluster_undo_horizon_note_self_member(void);
void
cluster_undo_horizon_note_self_member(void)
{}

/* spec-2.29a: cluster_reconfig.c gates the async marker stage on
 * MyBackendType == B_LMON.  The fixture exercises the pre-2.29a bounded-wait
 * semantics (write_fence submit stub returns FAILED synchronously), so run as
 * a non-LMON backend; the async FSM itself is covered by
 * test_cluster_marker_async.c. */
#include "miscadmin.h"
BackendType MyBackendType = B_INVALID;


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
/* spec-5.13 D3 grew ClusterReconfigState with clean_departed_epoch[CLUSTER_MAX_NODES]
 * (1 KiB) + clean_departed_bitmap + counter; spec-5.15 D2 added the membership SSOT
 * table (last_admitted_incarnation[CLUSTER_MAX_NODES] 1 KiB + membership_state[] +
 * pending_join_bitmap + self_join_admitted) plus the D1 observed-slot snapshot
 * (observed_incarnation[] + observed_generation[], 2 KiB).  spec-5.15 H1.3 added
 * observed_fresh_alive[] and spec-5.16 added observed_committed_join_incarnation[] +
 * observed_committed_join_epoch[] (3 more pg_atomic_uint64[CLUSTER_MAX_NODES=128]
 * arrays, +3 KiB).  Bump the mock backing store to fit the grown state struct. */
static char reconfig_shmem_storage[16384] __attribute__((aligned(64)));
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
errdetail(const char *f pg_attribute_unused(), ...) /* spec-5.14 D4 40R01 detail */
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
bool cluster_touched_peers_trace = false; /* spec-5.14 D4/D6 diag GUC stub */
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

/*
 * Hardening v1.0.4 stub: cluster_reconfig.c's join driver now consults
 * cluster_clean_leave_in_progress() (one membership reconfig at a time) — defined
 * in cluster_clean_leave.c, which this standalone unit does not link.  No clean
 * leave is ever in progress in these reconfig unit tests, so return false.
 */
bool
cluster_clean_leave_in_progress(void)
{
	return false;
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

/* spec-2.39 D14 stub: cluster_reconfig_apply_epoch_bump_as_coordinator
 * calls cluster_sinval_reset_all_on_reconfig.  Fixture has no sinval shmem;
 * stub no-op. */
void
cluster_sinval_reset_all_on_reconfig(void)
{}

/* spec-4.12 D4 stubs: the coordinator's marker-before-publish gate references
 * the enforcement GUC + the submit entry.  Enforcement OFF here so the gate is a
 * no-op (reconfig behaves as pre-4.12 in this unit harness). */
int cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_OFF;
ClusterFenceMarkerSubmitResult cluster_write_fence_submit_marker(const ClusterFenceMarker *m);
ClusterFenceMarkerSubmitResult
cluster_write_fence_submit_marker(const ClusterFenceMarker *m pg_attribute_unused())
{
	return CLUSTER_FENCE_MARKER_SUBMIT_FAILED;
}
/* spec-2.29a review r1 P1-c: controllable async-marker stubs so the tick-level
 * P1-1 invariants (bump-once while PENDING, node-remove zero-false-contest,
 * publish deferred to ACK) can be hard-asserted.  Defaults preserve the
 * pre-existing inert behavior (submit rejected, poll idle). */
static bool ut_fence_async_submit_ok = false;
static ClusterMarkerPollResult ut_fence_async_poll_pr = CLUSTER_MARKER_POLL_IDLE;
static uint32 ut_fence_async_poll_result = CLUSTER_FENCE_MARKER_SUBMIT_FAILED;
static int ut_fence_async_submit_calls = 0;
static int ut_fence_async_poll_calls = 0;

bool
cluster_write_fence_submit_marker_async(ClusterMarkerAsync *a,
										const ClusterFenceMarker *m pg_attribute_unused(),
										ClusterMarkerAsyncKind kind, int32 target_node,
										TimestampTz now pg_attribute_unused())
{
	ut_fence_async_submit_calls++;
	if (!ut_fence_async_submit_ok)
		return false;
	/* mirror the real wrapper's FSM effect so is_submitted() sees SUBMITTED */
	a->state = CLUSTER_MARKER_ASYNC_SUBMITTED;
	a->kind = kind;
	a->target_node = target_node;
	return true;
}
ClusterMarkerPollResult
cluster_write_fence_poll_marker_async(ClusterMarkerAsync *a, TimestampTz now pg_attribute_unused(),
									  uint32 *out_result, uint64 *out_elapsed_us)
{
	ut_fence_async_poll_calls++;
	if (out_result != NULL)
		*out_result = ut_fence_async_poll_result;
	if (out_elapsed_us != NULL)
		*out_elapsed_us = 0;
	if (ut_fence_async_poll_pr == CLUSTER_MARKER_POLL_ACKED
		|| ut_fence_async_poll_pr == CLUSTER_MARKER_POLL_TIMEOUT)
		a->state = CLUSTER_MARKER_ASYNC_IDLE;
	return ut_fence_async_poll_pr;
}
void
cluster_lmon_marker_complete_wakeup(void)
{}

/* spec-3.1 D7 stub: cluster_reconfig_apply_epoch_bump_as_coordinator
 * calls cluster_tt_status_flush_all.  Fixture has no TT overlay shmem;
 * stub no-op. */
void
cluster_tt_status_flush_all(uint32 new_epoch pg_attribute_unused())
{}

/* spec-5.15 D4 stubs: the join-marker handshake + seed reference these.  The
 * join path is gated by cluster_online_join (off by default in this fixture), so
 * apply/commit/submit are never exercised at runtime here; the symbols just need
 * a definition for the standalone link. */
bool cluster_online_join = false;
int cluster_quorum_poll_interval_ms = 100;
int cluster_join_convergence_timeout_ms = 30000;
/* spec-5.16 D6 GUC + joiner-home PCM fence arm referenced by the reconfig lmon tick /
 * note_self_admitted; the join-remaster path is off by default in this fixture, so
 * stub for the standalone link. */
bool cluster_join_remaster_enabled = false;
void cluster_grd_arm_join_pcm_fence(const uint8 *rejoining_set);
void
cluster_grd_arm_join_pcm_fence(const uint8 *rejoining_set pg_attribute_unused())
{}

/* Shape A (crash-rejoin re-declare barrier) stubs — the off-path rejoin tick
 * references these; the reconfig unit legs do not exercise the crash-rejoin
 * arm, so inert stubs suffice. */
int
cluster_conf_node_count(void)
{
	int n = 0;
	int i;

	for (i = 0; i < CLUSTER_MAX_NODES; i++)
		if (ut_declared_set[i])
			n++;
	return n;
}
bool
cluster_qvotec_prior_unclean_death(void)
{
	return false;
}
void
cluster_grd_set_offpath_boot_decided(void)
{}
void
cluster_grd_inc_offpath_crash_rejoin_fenced(void)
{}
/* spec-6.15 D5b: stripe joiner gate (not exercised here) — PROCEED so
 * the vanilla membership legs run unchanged. */
#include "cluster/cluster_xid_stripe_boot.h"
ClusterXidStripeJoinVerdict
cluster_xid_stripe_join_gate(bool self_may_seed pg_attribute_unused())
{
	return CLUSTER_XID_STRIPE_JOIN_PROCEED;
}
/* pgstat backend global referenced by pgstat_report_wait_start/end (the D4 join
 * marker submit wait); provide a file-static fake so the standalone link works. */
static uint32 ut_wait_event_info_storage;
uint32 *my_wait_event_info = &ut_wait_event_info_storage;
#include "storage/latch.h"
void
SetLatch(Latch *latch pg_attribute_unused())
{}
#include "storage/ipc.h"
void
on_shmem_exit(pg_on_exit_callback function pg_attribute_unused(), Datum arg pg_attribute_unused())
{}
#include "cluster/cluster_voting_disk_io.h"
ClusterVotingDiskIoState
cluster_voting_disk_read_join_slot(int fd pg_attribute_unused(),
								   uint32 node_id pg_attribute_unused(),
								   void *out_slot512 pg_attribute_unused())
{
	return CLUSTER_VOTING_DISK_IO_FAILED; /* no marker -> seed is a no-op */
}

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
	 * 64 catches accidental field removal;upper bound widened to 112 by
	 * spec-5.15 (join_bitmap[16]) catches accidental field bloat. */
	UT_ASSERT(sizeof(ReconfigEvent) >= 64);
	UT_ASSERT(sizeof(ReconfigEvent) <= 112);

	/* Field-level sanity:  88 bytes through spec-5.14 (8+4+4 + 8+8+16 + 8+4+4 +
	 * 8+8 = 80, plus reconfig_kind uint8 + _pad2[7] = 8), plus spec-5.15's
	 * join_bitmap[16] -> 104. */
	UT_ASSERT_EQ(sizeof(ReconfigEvent), 104);
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
 * T-reconfig-2.29a — P1-1 staged-record hard assertions (spec-2.29a
 * review r1 P1-c).
 *
 *	(a) fail-stop fence stage: while the marker is PENDING across ticks
 *	    the epoch is bumped EXACTLY ONCE and nothing publishes; the
 *	    staged event publishes only on ACKED+ACK.
 *	(b) node-remove stage: a tick re-entry while the fence marker is
 *	    PENDING returns 0 with *out_contest == false (no false contest
 *	    against our own already-advanced baseline) and no re-bump.
 * ============================================================ */

UT_TEST(test_reconfig_failstop_fence_stage_bump_once_while_pending)
{
	uint64 epoch_after_bump;
	uint64 apply_before;

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

	cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_ON;
	MyBackendType = B_LMON;
	ut_fence_async_submit_ok = true;
	ut_fence_async_poll_pr = CLUSTER_MARKER_POLL_PENDING;
	ut_fence_async_poll_result = CLUSTER_FENCE_MARKER_SUBMIT_FAILED;

	apply_before = cluster_reconfig_get_apply_counter();

	/* tick #1: stage-entry — bump once, submit, DO NOT publish. */
	cluster_reconfig_lmon_tick();
	epoch_after_bump = cluster_epoch_get_current();
	UT_ASSERT(epoch_after_bump > 0);
	UT_ASSERT_EQ((unsigned long long)cluster_reconfig_get_apply_counter(),
				 (unsigned long long)apply_before);

	/* ticks #2/#3: marker PENDING — the bump path must NOT re-enter
	 * (P1-1 invariant 0: zero re-bump, zero publish, zero side-effects). */
	cluster_reconfig_lmon_tick();
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ((unsigned long long)cluster_epoch_get_current(),
				 (unsigned long long)epoch_after_bump);
	UT_ASSERT_EQ((unsigned long long)cluster_reconfig_get_apply_counter(),
				 (unsigned long long)apply_before);

	/* tick #4: ACKED + ACK — the STAGED event publishes; still no re-bump. */
	ut_fence_async_poll_pr = CLUSTER_MARKER_POLL_ACKED;
	ut_fence_async_poll_result = CLUSTER_FENCE_MARKER_SUBMIT_ACK;
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ((unsigned long long)cluster_epoch_get_current(),
				 (unsigned long long)epoch_after_bump);
	UT_ASSERT_EQ((unsigned long long)cluster_reconfig_get_apply_counter(),
				 (unsigned long long)(apply_before + 1));

	/* restore fixture defaults for the neighboring tests */
	MyBackendType = B_INVALID;
	cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_OFF;
	ut_fence_async_submit_ok = false;
	ut_fence_async_poll_pr = CLUSTER_MARKER_POLL_IDLE;
	ut_fence_async_poll_result = CLUSTER_FENCE_MARKER_SUBMIT_FAILED;
}


UT_TEST(test_reconfig_node_remove_stage_no_false_contest)
{
	uint64 baseline;
	uint64 epoch_after_bump;
	uint64 ret;
	bool contest;

	ut_reset_mocks();
	reconfig_init_done = false;
	epoch_init_done = false;
	cluster_reconfig_shmem_init();
	cluster_epoch_shmem_init();

	cluster_node_id = 0;
	ut_in_quorum_value = true;
	ut_declared_set[0] = true;
	ut_declared_set[1] = true;

	cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_ON;
	MyBackendType = B_LMON;
	ut_fence_async_submit_ok = true;
	ut_fence_async_poll_pr = CLUSTER_MARKER_POLL_PENDING;
	ut_fence_async_poll_result = CLUSTER_FENCE_MARKER_SUBMIT_FAILED;

	baseline = cluster_epoch_get_current();

	/* call #1: guarded CAS advances baseline→baseline+1, stages the fence
	 * marker, returns 0 (not yet committed), contest MUST be false. */
	contest = true;
	ret = cluster_reconfig_apply_node_removed_as_coordinator(1, baseline, 42, 7, &contest);
	UT_ASSERT_EQ((unsigned long long)ret, 0ULL);
	UT_ASSERT(!contest);
	epoch_after_bump = cluster_epoch_get_current();
	UT_ASSERT_EQ((unsigned long long)epoch_after_bump, (unsigned long long)(baseline + 1));

	/* call #2 (tick re-entry with the SAME stale baseline while PENDING):
	 * without the staged record this would re-run the baseline CAS against
	 * our own advanced epoch and report a FALSE contest.  P1-1: it must
	 * poll the stage instead — 0, contest==false, no second bump. */
	contest = true;
	ret = cluster_reconfig_apply_node_removed_as_coordinator(1, baseline, 42, 7, &contest);
	UT_ASSERT_EQ((unsigned long long)ret, 0ULL);
	UT_ASSERT(!contest);
	UT_ASSERT_EQ((unsigned long long)cluster_epoch_get_current(),
				 (unsigned long long)epoch_after_bump);

	/* call #3: ACKED + ACK — removal publishes at the STAGED epoch. */
	ut_fence_async_poll_pr = CLUSTER_MARKER_POLL_ACKED;
	ut_fence_async_poll_result = CLUSTER_FENCE_MARKER_SUBMIT_ACK;
	contest = true;
	ret = cluster_reconfig_apply_node_removed_as_coordinator(1, baseline, 42, 7, &contest);
	UT_ASSERT_EQ((unsigned long long)ret, (unsigned long long)epoch_after_bump);
	UT_ASSERT(!contest);
	UT_ASSERT_EQ((unsigned long long)cluster_epoch_get_current(),
				 (unsigned long long)epoch_after_bump);

	MyBackendType = B_INVALID;
	cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_OFF;
	ut_fence_async_submit_ok = false;
	ut_fence_async_poll_pr = CLUSTER_MARKER_POLL_IDLE;
	ut_fence_async_poll_result = CLUSTER_FENCE_MARKER_SUBMIT_FAILED;
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
 * spec-5.14 U6 — reconfig_kind field round-trip + enum boundary.
 * ============================================================ */
UT_TEST(test_reconfig_kind_field_roundtrip)
{
	ReconfigEvent evt;
	ReconfigEvent got;

	/* enum constants are the on-the-wire-free shmem contract. */
	UT_ASSERT_EQ((int)RECONFIG_KIND_NONE, 0);
	UT_ASSERT_EQ((int)RECONFIG_KIND_FAIL_STOP, 1);
	UT_ASSERT_EQ((int)RECONFIG_KIND_CLEAN_LEAVE, 2);

	cluster_reconfig_shmem_init();

	memset(&evt, 0, sizeof(evt));
	evt.event_id = 999;
	evt.reconfig_kind = RECONFIG_KIND_FAIL_STOP;
	cluster_reconfig_publish_event(&evt);

	cluster_reconfig_get_last_event(&got);
	UT_ASSERT_EQ((int)got.reconfig_kind, (int)RECONFIG_KIND_FAIL_STOP);

	/* CLEAN_LEAVE field also round-trips (defensive D4 path reachability). */
	memset(&evt, 0, sizeof(evt));
	evt.event_id = 1000;
	evt.reconfig_kind = RECONFIG_KIND_CLEAN_LEAVE;
	cluster_reconfig_publish_event(&evt);
	cluster_reconfig_get_last_event(&got);
	UT_ASSERT_EQ((int)got.reconfig_kind, (int)RECONFIG_KIND_CLEAN_LEAVE);
}


/* ============================================================
 * spec-5.14 U8 — classify_verdict decision matrix (§3.2), pure.
 *	4 quadrants (read/write × touched/non-touched) × quorum.
 * ============================================================ */
UT_TEST(test_reconfig_classify_verdict_matrix)
{
	/* touched (read OR write) + in quorum -> 40R01 (ABORT_TOUCHED) */
	UT_ASSERT_EQ((int)cluster_reconfig_classify_verdict(true, false, true),
				 (int)RECONFIG_VERDICT_ABORT_TOUCHED);
	UT_ASSERT_EQ((int)cluster_reconfig_classify_verdict(true, true, true),
				 (int)RECONFIG_VERDICT_ABORT_TOUCHED);

	/* touched + lost quorum -> 53R50 (ABORT_QUORUM, terminal) */
	UT_ASSERT_EQ((int)cluster_reconfig_classify_verdict(true, false, false),
				 (int)RECONFIG_VERDICT_ABORT_QUORUM);
	UT_ASSERT_EQ((int)cluster_reconfig_classify_verdict(true, true, false),
				 (int)RECONFIG_VERDICT_ABORT_QUORUM);

	/* non-touched read-only -> ABSORB (INV-TP5), regardless of quorum */
	UT_ASSERT_EQ((int)cluster_reconfig_classify_verdict(false, false, true),
				 (int)RECONFIG_VERDICT_ABSORB);
	UT_ASSERT_EQ((int)cluster_reconfig_classify_verdict(false, false, false),
				 (int)RECONFIG_VERDICT_ABSORB);

	/* non-touched writable + in quorum -> 53R60 (ABORT_RECONFIG) */
	UT_ASSERT_EQ((int)cluster_reconfig_classify_verdict(false, true, true),
				 (int)RECONFIG_VERDICT_ABORT_RECONFIG);

	/* non-touched writable + lost quorum -> 53R50 (ABORT_QUORUM) */
	UT_ASSERT_EQ((int)cluster_reconfig_classify_verdict(false, true, false),
				 (int)RECONFIG_VERDICT_ABORT_QUORUM);
}


/* ============================================================
 * spec-5.15 D1 — cluster_reconfig_compute_join_bitmap (join-edge detection).
 *
 *	These exercise the join-edge detector against the membership SSOT + the
 *	qvotec-published observed slots (U6-U9 of the spec §4.1 list; they live here
 *	rather than test_cluster_membership.c because the detector reads
 *	ClusterReconfigState + the CSSD/conf mocks this fixture already provides).
 * ============================================================ */

/* test a bit in a returned join_bitmap (mirrors the module-private helper). */
static bool
jb_test(const uint8 *bmp, int i)
{
	return (bmp[i / 8] & (uint8)(1u << (i % 8))) != 0;
}

/* fresh reconfig shmem (membership all ABSENT, observed all 0) + clean mocks. */
static void
ut_join_setup(void)
{
	ut_reset_mocks();
	reconfig_init_done = false;
	cluster_reconfig_shmem_init(); /* memset clean + attach membership table */
	cluster_node_id = 0;		   /* self */
	ut_declared_set[0] = true;	   /* self declared */
}

/* U6 — declared-peer filter: an un-declared CSSD-ALIVE peer is never a join edge. */
UT_TEST(test_join_bitmap_declared_peer_filter)
{
	uint8 jb[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	int n;

	ut_join_setup();
	/* node 3 declared, ALIVE, fresh slot, membership ABSENT -> join edge */
	ut_declared_set[3] = true;
	ut_peer_state[3] = CLUSTER_CSSD_PEER_ALIVE;
	cluster_reconfig_record_observed_slot(3, 10, 1, 0);
	/* node 5 NOT declared, ALIVE, fresh slot -> filtered out */
	ut_peer_state[5] = CLUSTER_CSSD_PEER_ALIVE;
	cluster_reconfig_record_observed_slot(5, 10, 1, 0);

	n = cluster_reconfig_compute_join_bitmap(jb);
	UT_ASSERT_EQ(n, 1);
	UT_ASSERT(jb_test(jb, 3));
	UT_ASSERT(!jb_test(jb, 5));
}

/* U7 — DEAD->ALIVE edge is a join; a steady-state MEMBER is not. */
UT_TEST(test_join_bitmap_dead_edge_not_member)
{
	uint8 jb[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	int n;

	ut_join_setup();
	/* node 1: MEMBER + ALIVE -> already a member, NOT a join edge */
	ut_declared_set[1] = true;
	ut_peer_state[1] = CLUSTER_CSSD_PEER_ALIVE;
	cluster_membership_set_state(1, CLUSTER_MEMBER_MEMBER);
	cluster_reconfig_record_observed_slot(1, 10, 1, 0);
	/* node 2: DEAD + ALIVE + fresh -> join edge */
	ut_declared_set[2] = true;
	ut_peer_state[2] = CLUSTER_CSSD_PEER_ALIVE;
	cluster_membership_set_state(2, CLUSTER_MEMBER_DEAD);
	cluster_reconfig_record_observed_slot(2, 10, 1, 0);

	n = cluster_reconfig_compute_join_bitmap(jb);
	UT_ASSERT_EQ(n, 1);
	UT_ASSERT(!jb_test(jb, 1));
	UT_ASSERT(jb_test(jb, 2));
}

/* U8 — multiple simultaneous joiners. */
UT_TEST(test_join_bitmap_multi_joiner)
{
	uint8 jb[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	int n;

	ut_join_setup();
	ut_declared_set[1] = true;
	ut_peer_state[1] = CLUSTER_CSSD_PEER_ALIVE;
	cluster_membership_set_state(1, CLUSTER_MEMBER_DEAD);
	cluster_reconfig_record_observed_slot(1, 10, 1, 0);
	ut_declared_set[2] = true;
	ut_peer_state[2] = CLUSTER_CSSD_PEER_ALIVE;
	cluster_membership_set_state(2, CLUSTER_MEMBER_DEAD);
	cluster_reconfig_record_observed_slot(2, 10, 1, 0);

	n = cluster_reconfig_compute_join_bitmap(jb);
	UT_ASSERT_EQ(n, 2);
	UT_ASSERT(jb_test(jb, 1));
	UT_ASSERT(jb_test(jb, 2));
}

/* U9 — a stale incarnation (<= floor) and a not-ready (generation 0) peer are
 * both excluded; only the fresh+ready DEAD->ALIVE peer is a join edge. */
UT_TEST(test_join_bitmap_stale_and_notready_excluded)
{
	uint8 jb[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	int n;

	ut_join_setup();
	/* node 1: stale — observed incarnation 10 <= admitted floor 10 */
	ut_declared_set[1] = true;
	ut_peer_state[1] = CLUSTER_CSSD_PEER_ALIVE;
	cluster_membership_set_state(1, CLUSTER_MEMBER_DEAD);
	cluster_membership_record_admitted(1, 10);
	cluster_reconfig_record_observed_slot(1, 10, 1, 0);
	/* node 2: not ready — observed generation 0 (no valid published slot) */
	ut_declared_set[2] = true;
	ut_peer_state[2] = CLUSTER_CSSD_PEER_ALIVE;
	cluster_membership_set_state(2, CLUSTER_MEMBER_DEAD);
	cluster_reconfig_record_observed_slot(2, 99, 0, 0);
	/* node 3: fresh (11 > floor 10) + ready -> join edge */
	ut_declared_set[3] = true;
	ut_peer_state[3] = CLUSTER_CSSD_PEER_ALIVE;
	cluster_membership_set_state(3, CLUSTER_MEMBER_DEAD);
	cluster_membership_record_admitted(3, 10);
	cluster_reconfig_record_observed_slot(3, 11, 1, 0);

	n = cluster_reconfig_compute_join_bitmap(jb);
	UT_ASSERT_EQ(n, 1);
	UT_ASSERT(!jb_test(jb, 1));
	UT_ASSERT(!jb_test(jb, 2));
	UT_ASSERT(jb_test(jb, 3));
}

/* U12 (D3, INV-J11) — event_id_v2: the 4 real kinds are mutually distinct under
 * their actual non-collision bases; FAIL_STOP stays byte-compatible with the
 * legacy hash; CLEAN_LEAVE also uses legacy (5.13 marker binding, RC-1) and is
 * distinguished from FAIL_STOP by cssd_dead_generation; the two JOIN phases fold
 * the kind; NONE is the 0 sentinel. */
UT_TEST(test_event_id_v2_kind_distinctness)
{
	uint8 dead[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint8 join[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint64 incs[CLUSTER_MAX_NODES];
	uint64 incs2[CLUSTER_MAX_NODES];
	uint64 gen = 7;
	uint64 legacy, id_fs, id_cl, id_jp, id_jc;

	memset(dead, 0, sizeof(dead));
	memset(join, 0, sizeof(join));
	memset(incs, 0, sizeof(incs));
	dead[0] = 0x02;	 /* node 1 in the leave set */
	join[0] = 0x04;	 /* node 2 in the join set */
	incs[2] = 12345; /* joiner 2 incarnation */

	/* NONE is the never-applied sentinel. */
	UT_ASSERT_EQ(cluster_reconfig_compute_event_id_v2(RECONFIG_KIND_NONE, dead, join, incs, gen),
				 0);

	/* FAIL_STOP == the legacy hash over (dead, gen). */
	legacy = cluster_reconfig_compute_event_id(dead, gen);
	id_fs = cluster_reconfig_compute_event_id_v2(RECONFIG_KIND_FAIL_STOP, dead, join, incs, gen);
	UT_ASSERT_EQ(id_fs, legacy);

	/* CLEAN_LEAVE also uses legacy (RC-1): equal to FAIL_STOP for identical
	 * (dead, gen); the real-world distinction is cssd_dead_generation. */
	id_cl = cluster_reconfig_compute_event_id_v2(RECONFIG_KIND_CLEAN_LEAVE, dead, join, incs, gen);
	UT_ASSERT_EQ(id_cl, legacy);
	UT_ASSERT(
		cluster_reconfig_compute_event_id_v2(RECONFIG_KIND_CLEAN_LEAVE, dead, join, incs, gen + 1)
		!= id_fs);

	/* JOIN_PENDING != JOIN_COMMITTED (kind folded); both != the leave ids, != 0. */
	id_jp = cluster_reconfig_compute_event_id_v2(RECONFIG_KIND_JOIN_PENDING, dead, join, incs, gen);
	id_jc
		= cluster_reconfig_compute_event_id_v2(RECONFIG_KIND_JOIN_COMMITTED, dead, join, incs, gen);
	UT_ASSERT(id_jp != id_jc);
	UT_ASSERT(id_jp != id_fs);
	UT_ASSERT(id_jc != id_fs);
	UT_ASSERT(id_jp != 0);
	UT_ASSERT(id_jc != 0);

	/* distinct joiner incarnations -> distinct join ids. */
	memcpy(incs2, incs, sizeof(incs2));
	incs2[2] = 99999;
	UT_ASSERT(
		cluster_reconfig_compute_event_id_v2(RECONFIG_KIND_JOIN_PENDING, dead, join, incs2, gen)
		!= id_jp);
}

/* U14 (D4, INV-J10) — clearing clean_departed (what commit_member does at
 * JOIN_COMMITTED) re-enables a node's later fail-stop: a clean-left node that
 * rejoins must have clean_departed[N] cleared so effective_dead = cssd_dead &
 * ~clean_departed once again includes N's later real CSSD DEAD.  The full
 * marker-durable commit path is covered e2e by TAP L6; here we exercise the
 * clear primitive the commit uses + that it is JOIN_COMMITTED-only (Phase-1 /
 * apply does NOT clear). */
UT_TEST(test_clean_departed_clear_for_rejoin)
{
	ut_join_setup();
	ut_declared_set[2] = true;

	/* node 2 cleanly departed at epoch 5 -> masked from effective_dead */
	cluster_reconfig_record_clean_departed(2, 5, false);
	UT_ASSERT(cluster_reconfig_is_clean_departed(2));
	UT_ASSERT_EQ(cluster_reconfig_get_clean_departed_epoch(2), 5);

	/* Phase-1 (apply_join) sets JOINING + pending but does NOT clear (only a real
	 * MEMBER commit may) — assert the suppression still holds at JOINING. */
	cluster_membership_set_state(2, CLUSTER_MEMBER_JOINING);
	UT_ASSERT(cluster_reconfig_is_clean_departed(2));

	/* JOIN_COMMITTED clears it (commit_member calls clear_clean_departed). */
	cluster_reconfig_clear_clean_departed(2);
	UT_ASSERT(!cluster_reconfig_is_clean_departed(2));
	UT_ASSERT_EQ(cluster_reconfig_get_clean_departed_epoch(2), 0);
}

/* D5 (INV-J9) — joiner write-gate lifecycle: a fresh node defaults ALLOW; when
 * it detects a running cluster (a peer at epoch > 0) the tick closes the gate
 * (53R60, retry-safe); note_self_admitted reopens it (ALLOW). */
UT_TEST(test_self_join_gate_lifecycle)
{
	ut_join_setup();
	cluster_node_id = 2;
	ut_declared_set[2] = true;
	ut_declared_set[0] = true;
	ut_in_quorum_value = true;
	cluster_enabled = true;
	cluster_online_join = true;

	/* default gate open */
	UT_ASSERT_EQ((int)cluster_reconfig_self_join_gate_verdict(), (int)CLUSTER_JOIN_GATE_ALLOW);

	/* a peer observed at epoch > 0 => cluster running => this node is rejoining;
	 * the tick's joiner self-tick closes the gate. */
	cluster_reconfig_record_observed_slot(0, 100, 1, 7); /* node 0 at epoch 7 */
	cluster_reconfig_lmon_tick();
	UT_ASSERT_EQ((int)cluster_reconfig_self_join_gate_verdict(),
				 (int)CLUSTER_JOIN_GATE_BLOCK_53R60);

	/* admission (note_self_admitted) reopens the gate. */
	cluster_reconfig_note_self_admitted(7);
	UT_ASSERT_EQ((int)cluster_reconfig_self_join_gate_verdict(), (int)CLUSTER_JOIN_GATE_ALLOW);

	cluster_online_join = false; /* leave global off for other tests */
}

/* ======================================================================
 * U18 (HF-1 / INV-J9) -- the publish-proof is true only when a MAJORITY of the
 * current MEMBER survivors have reached admitted_epoch: i.e. the coordinator's
 * JOIN_COMMITTED publish actually propagated.  A marker-durable-but-unpublished
 * state (survivors still behind) is NOT proven -> the joiner gate stays closed
 * (the half-publish window, P1-1).
 * ====================================================================== */
UT_TEST(test_reconfig_join_publish_proven_member_quorum)
{
	ut_join_setup();		   /* self = node 0, the joiner */
	ut_declared_set[1] = true; /* peers 1 and 2 are MEMBER survivors */
	ut_declared_set[2] = true;
	cluster_membership_set_state(1, CLUSTER_MEMBER_MEMBER);
	cluster_membership_set_state(2, CLUSTER_MEMBER_MEMBER);

	/* admitted_epoch 5; nobody advanced yet -> not proven (half-publish). */
	cluster_reconfig_record_observed_slot(1, 1, 1, 0);
	cluster_reconfig_record_observed_slot(2, 1, 1, 0);
	UT_ASSERT(!cluster_reconfig_join_publish_proven(5));

	/* one of two members reached it -> still below majority(2) -> not proven. */
	cluster_reconfig_record_observed_slot(1, 1, 1, 5);
	UT_ASSERT(!cluster_reconfig_join_publish_proven(5));

	/* both members reached it -> proven. */
	cluster_reconfig_record_observed_slot(2, 1, 1, 5);
	UT_ASSERT(cluster_reconfig_join_publish_proven(5));

	/* admitted_epoch 0 is never a real epoch -> fail-closed. */
	UT_ASSERT(!cluster_reconfig_join_publish_proven(0));
}

/* ======================================================================
 * U18b (HF-1) -- zero visible MEMBER survivor -> cannot prove -> fail-closed
 * (a peer at a high epoch that is NOT a member does not count).
 * ====================================================================== */
UT_TEST(test_reconfig_join_publish_proven_no_member_failclosed)
{
	ut_join_setup();
	ut_declared_set[1] = true;
	cluster_membership_set_state(1, CLUSTER_MEMBER_JOINING); /* not a member */
	cluster_reconfig_record_observed_slot(1, 1, 1, 9);
	UT_ASSERT(!cluster_reconfig_join_publish_proven(5));
}

/* ======================================================================
 * U19 (HF-2 / INV-J14) -- bootstrap is a POSITIVE epoch proof, not a timing
 * grace: quorum of declared CSSD-alive AND no peer past INITIAL.  A running
 * cluster (any peer past INITIAL) is NOT a bootstrap -> false (a slow rejoiner
 * stays fail-closed, P1-2).  Too few alive -> undecided -> false.
 * ====================================================================== */
UT_TEST(test_reconfig_bootstrap_quorum_epoch_proof)
{
	ut_join_setup();		   /* self = node 0 */
	ut_declared_set[1] = true; /* 3 declared nodes */
	ut_declared_set[2] = true;

	/* quorum of declared FRESH-ALIVE co-boot slots at INITIAL -> bootstrap proven
	 * (v1.3: durable voting-disk heartbeat freshness + valid slot, not live CSSD). */
	cluster_reconfig_record_observed_slot(1, 1, 1, 0);
	cluster_reconfig_record_observed_slot(2, 1, 1, 0);
	cluster_reconfig_record_observed_fresh_alive(1, true);
	cluster_reconfig_record_observed_fresh_alive(2, true);
	UT_ASSERT(cluster_reconfig_bootstrap_quorum_at_initial());

	/* a peer past INITIAL (running cluster) -> NOT a bootstrap (fail-closed). */
	cluster_reconfig_record_observed_slot(1, 1, 1, 4);
	UT_ASSERT(!cluster_reconfig_bootstrap_quorum_at_initial());

	/* no valid co-boot slot on either peer (generation 0) -> only self proven ->
	 * below quorum -> false (never latch on a default-0 placeholder). */
	cluster_reconfig_record_observed_slot(1, 0, 0, 0);
	cluster_reconfig_record_observed_slot(2, 0, 0, 0);
	UT_ASSERT(!cluster_reconfig_bootstrap_quorum_at_initial());
}


/* ======================================================================
 * U20 (spec-5.15 Hardening v1.2 / INV-J14 self-join-gate race) -- the
 * cold-bootstrap proof must rest on a VALID durable co-boot slot
 * (cluster_reconfig_get_observed_slot true, generation > 0, observed_epoch
 * == INITIAL), NOT on live CSSD state and NOT on a default-0 placeholder.
 *
 * Root cause it guards: a founding survivor that has durable proof of co-
 * booting at INITIAL but whose peers' live CSSD is momentarily DOWN (IC /
 * heartbeat churn) was denied bootstrap by the v1.1 CSSD-quorum proof, so it
 * stayed UNDECIDED; a later UNRELATED node fail-stop then advanced the epoch
 * and reclassified this genuine member as a rejoiner -> 53R61 (refused its own
 * writes).  Anchoring the proof on the durable voting-disk slot lets the member
 * latch reliably during formation (immune to CSSD churn), closing the window.
 * ====================================================================== */
UT_TEST(test_reconfig_bootstrap_proof_valid_slot_not_cssd)
{
	/* --- A. FRESH-ALIVE co-boot slots at INITIAL but peers CSSD-DEAD -> still a
	 *        proven bootstrap (durable voting-disk heartbeat, not live CSSD).  v1.1
	 *        returned false here (the race window); v1.3 returns true because the
	 *        liveness is the voting-disk fresh-alive signal, immune to CSSD churn. --- */
	ut_join_setup();		   /* self = node 0 */
	ut_declared_set[1] = true; /* 3 declared nodes */
	ut_declared_set[2] = true;
	cluster_reconfig_record_observed_slot(1, 7, 1, 0);	   /* valid slot, INITIAL */
	cluster_reconfig_record_observed_slot(2, 7, 1, 0);	   /* valid slot, INITIAL */
	cluster_reconfig_record_observed_fresh_alive(1, true); /* voting-disk fresh */
	cluster_reconfig_record_observed_fresh_alive(2, true);
	ut_peer_state[1] = CLUSTER_CSSD_PEER_DEAD; /* live CSSD churned down */
	ut_peer_state[2] = CLUSTER_CSSD_PEER_DEAD;
	UT_ASSERT(cluster_reconfig_bootstrap_quorum_at_initial());

	/* --- B. CSSD-alive but NO valid slot (generation 0 placeholder) must
	 *        NOT prove bootstrap — never latch on a default-0 epoch. --- */
	ut_join_setup();
	ut_declared_set[1] = true;
	ut_declared_set[2] = true;
	ut_peer_state[1] = CLUSTER_CSSD_PEER_ALIVE;
	ut_peer_state[2] = CLUSTER_CSSD_PEER_ALIVE;
	/* no record_observed_slot -> generation 0 -> not a valid co-boot proof */
	UT_ASSERT(!cluster_reconfig_bootstrap_quorum_at_initial());

	/* --- C. a peer observed past INITIAL is a running cluster, never a
	 *        bootstrap (rejoiner fail-closed) — unchanged from v1.1. --- */
	ut_join_setup();
	ut_declared_set[1] = true;
	ut_declared_set[2] = true;
	cluster_reconfig_record_observed_slot(1, 7, 1, 0); /* valid, INITIAL */
	cluster_reconfig_record_observed_slot(2, 7, 1, 5); /* valid, past INITIAL */
	UT_ASSERT(!cluster_reconfig_bootstrap_quorum_at_initial());
}


/* ======================================================================
 * U21 (spec-5.15 Hardening v1.3 / INV-J14 stale-slot fail-open) -- a valid
 * generation > 0 slot at epoch INITIAL is NOT proof of co-booting: it may be a
 * CRASHED peer's stale leftover (decide_quorum_view's P2.1 freshness gate marks
 * it not-fresh).  The cold-bootstrap proof must additionally require the per-node
 * FRESH-ALIVE signal (durable voting-disk heartbeat), not slot existence alone —
 * else a node with self + a stale peer slot fail-opens (latches BOOTSTRAP without
 * a live co-boot quorum).  v1.2 counted such a stale slot (the regression this
 * guards); v1.3 fail-closes on it.
 * ====================================================================== */
UT_TEST(test_reconfig_bootstrap_proof_stale_slot_failclosed)
{
	/* --- A. valid slots at INITIAL but STALE heartbeat (fresh_alive=false) must
	 *        NOT count -> only self proven -> below quorum -> false.  v1.2 (no
	 *        freshness) counted them = fail-open; v1.3 fail-closes. --- */
	ut_join_setup();		   /* self = node 0 */
	ut_declared_set[1] = true; /* 3 declared nodes */
	ut_declared_set[2] = true;
	cluster_reconfig_record_observed_slot(1, 7, 1, 0);		/* valid slot, INITIAL */
	cluster_reconfig_record_observed_slot(2, 7, 1, 0);		/* valid slot, INITIAL */
	cluster_reconfig_record_observed_fresh_alive(1, false); /* crashed peer: stale hb */
	cluster_reconfig_record_observed_fresh_alive(2, false);
	UT_ASSERT(!cluster_reconfig_bootstrap_quorum_at_initial());

	/* --- B. the same slots but FRESH-ALIVE -> genuine co-boot -> proven. --- */
	cluster_reconfig_record_observed_fresh_alive(1, true);
	cluster_reconfig_record_observed_fresh_alive(2, true);
	UT_ASSERT(cluster_reconfig_bootstrap_quorum_at_initial());

	/* --- C. one fresh + one stale -> self + the one fresh = quorum (3-node) ->
	 *        proven; the stale peer simply does not contribute (no over-reject). --- */
	cluster_reconfig_record_observed_fresh_alive(2, false);
	UT_ASSERT(cluster_reconfig_bootstrap_quorum_at_initial());
}


/* ============================================================
 * Main — register + run all tests.
 * ============================================================ */

int
main(void)
{
	UT_PLAN(45);

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
	UT_RUN(test_reconfig_failstop_fence_stage_bump_once_while_pending);
	UT_RUN(test_reconfig_node_remove_stage_no_false_contest);

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

	/* spec-5.14 — reconfig_kind round-trip (U6) + verdict matrix (U8). */
	UT_RUN(test_reconfig_kind_field_roundtrip);
	UT_RUN(test_reconfig_classify_verdict_matrix);

	/* spec-5.15 D1 — join-edge detection (U6-U9). */
	UT_RUN(test_join_bitmap_declared_peer_filter);
	UT_RUN(test_join_bitmap_dead_edge_not_member);
	UT_RUN(test_join_bitmap_multi_joiner);
	UT_RUN(test_join_bitmap_stale_and_notready_excluded);

	/* spec-5.15 D3 — event_id_v2 kind distinctness (U12). */
	UT_RUN(test_event_id_v2_kind_distinctness);

	/* spec-5.15 D4 — clean-departed clear for rejoin (U14). */
	UT_RUN(test_clean_departed_clear_for_rejoin);

	/* spec-5.15 D5 — joiner write-gate lifecycle (INV-J9). */
	UT_RUN(test_self_join_gate_lifecycle);

	/* spec-5.15 Hardening v1.1 — HF-1 publish-proof + HF-2 bootstrap epoch-proof. */
	UT_RUN(test_reconfig_join_publish_proven_member_quorum);
	UT_RUN(test_reconfig_join_publish_proven_no_member_failclosed);
	UT_RUN(test_reconfig_bootstrap_quorum_epoch_proof);
	UT_RUN(test_reconfig_bootstrap_proof_valid_slot_not_cssd);
	UT_RUN(test_reconfig_bootstrap_proof_stale_slot_failclosed);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
