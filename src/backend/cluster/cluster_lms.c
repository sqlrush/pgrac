/*-------------------------------------------------------------------------
 *
 * cluster_lms.c
 *	  pgrac LMS (Lock Master / Grant Service) cluster background process —
 *	  spec-2.18 Sprint A Step 1-2 skeleton implementation.
 *
 *	  Spec-2.18 ships the lifecycle skeleton + grant-decision ownership
 *	  migration from LMON to LMS.  Single ownership path with fail-closed
 *	  semantics — no runtime LMON fallback grant (§1.4.5 F1).  Real
 *	  grant state machine, BAST send/receive, deadlock detection,
 *	  cleanup_on_backend_exit entry sweep, lock class expansion all
 *	  defer to spec-2.19+.
 *
 *	  HC1 fail-closed:  LMS_READY 后 LMON grant hard-disabled.  LMS
 *	  crash → backend receives SQLSTATE 53R80 (Step 4).
 *
 *	  HC2 4-state semantic:  DISABLED (lms_enabled=off startup-only)
 *	  vs NOT_STARTED / STARTING vs READY / DRAINING vs STOPPED is
 *	  distinguished in pg_cluster_lms view + 53R80 reason field
 *	  (Step 4 view delivery).
 *
 *	  HC3 ConditionVariable substrate is retained for producer-side
 *	  wake API compatibility, but the Step 6 LMS skeleton uses the
 *	  proven aux-process WaitLatch idle path until a dedicated LMS
 *	  latch handoff lands in the production activation spec.
 *
 *	  HC4 single ownership atomic guard.  Public helper
 *	  cluster_lms_owns_grant() reads lms_state atomic; LMON tick body
 *	  入口 calls this to early-return when LMS owns grant (Step 3).
 *	  No lms_drain_owner second field.
 *
 *	  HC5 NUM_AUXILIARY_PROCS bump:  LMS is 7th cluster aux process
 *	  (after LMON / LCK / DIAG / Cluster Stats / CSSD / QVOTEC);
 *	  src/include/storage/proc.h bumps NUM_AUXILIARY_PROCS 12 → 13
 *	  (D3b NEW deliverable; v0.3 L2.7).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_lms.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-2.18-lms-daemon-grant-ownership-migration.md
 *	  (FROZEN v0.3 2026-05-14 user approve).
 *	  Anchor: cluster_lmon.c (spec-1.11) for skeleton structure.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>

#include "cluster/cluster_conf.h"
#include "cluster/cluster_ges.h"
#include "cluster/cluster_grd_outbound.h"
#include "cluster/cluster_grd_work_queue.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_native_lock_probe.h"
#include "cluster/cluster_shmem.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/auxprocess.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "storage/condition_variable.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"


/*
 * Idle sleep timeout for the main loop WaitLatch fallback poll.
 * Hardcoded for the Step 6 skeleton; producer-side CV broadcast remains
 * present, but this aux process does not rely on CV sleeper registration.
 */
#define LMS_IDLE_TIMEOUT_MS 100


/* External hook from postmaster.c (mirrors cluster_lmon Q2 thin proxy). */
extern pid_t cluster_postmaster_start_lms(void);


/* ============================================================
 * Module-local state.
 * ============================================================ */

static ClusterLmsSharedState *cluster_lms_state = NULL;

static const char *cluster_lms_state_strings[] = {
	"not_started", "starting", "ready", "draining", "stopped", "disabled",
};

/*
 * spec-1.3 region registry descriptor.  Registered once at postmaster
 * startup via cluster_lms_shmem_register().
 */
static const ClusterShmemRegion cluster_lms_region = {
	.name = "pgrac cluster lms",
	.size_fn = cluster_lms_shmem_size,
	.init_fn = cluster_lms_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "spec-2.18 LMS",
	.reserved_flags = 0,
};


/* ============================================================
 * State string mapping.
 * ============================================================ */

const char *
cluster_lms_state_to_string(ClusterLmsState s)
{
	if ((int)s < 0 || (int)s > CLUSTER_LMS_STATE_LAST)
		return "(unknown)";
	return cluster_lms_state_strings[(int)s];
}


/* ============================================================
 * shmem region helpers (spec-1.3 registry-backed).
 * ============================================================ */

Size
cluster_lms_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterLmsSharedState));
}

void
cluster_lms_shmem_init(void)
{
	bool found;

	cluster_lms_state = (ClusterLmsSharedState *)ShmemInitStruct(
		"pgrac cluster lms", sizeof(ClusterLmsSharedState), &found);

	if (!found) {
		memset(cluster_lms_state, 0, sizeof(*cluster_lms_state));
		LWLockInitialize(&cluster_lms_state->lwlock, LWTRANCHE_CLUSTER_LMS);
		pg_atomic_init_u32(&cluster_lms_state->lms_state,
						   cluster_lms_enabled ? CLUSTER_LMS_NOT_STARTED : CLUSTER_LMS_DISABLED);
		pg_atomic_init_u32(&cluster_lms_state->work_queue_count, 0);
		pg_atomic_init_u64(&cluster_lms_state->lms_started_count, 0);
		pg_atomic_init_u64(&cluster_lms_state->lms_ready_at_us, 0);
		pg_atomic_init_u64(&cluster_lms_state->lms_work_drained_count, 0);
		/* spec-2.20 D4 — 3 NEW counter (replacing single lms_decision_count). */
		pg_atomic_init_u64(&cluster_lms_state->lms_decision_grant_count, 0);
		pg_atomic_init_u64(&cluster_lms_state->lms_decision_reject_count, 0);
		pg_atomic_init_u64(&cluster_lms_state->lms_decision_convert_count, 0);
		pg_atomic_init_u64(&cluster_lms_state->lms_drain_empty_count, 0);
		pg_atomic_init_u64(&cluster_lms_state->lms_error_count, 0);
		/* spec-2.25 D4 / D13 — native-lock probe counters. */
		pg_atomic_init_u64(&cluster_lms_state->native_probe_sent_count, 0);
		pg_atomic_init_u64(&cluster_lms_state->native_probe_reply_recv_count, 0);
		pg_atomic_init_u64(&cluster_lms_state->native_probe_collector_slot_full_count, 0);
		pg_atomic_init_u64(&cluster_lms_state->native_probe_aggregate_holder_conflict_count, 0);
		pg_atomic_init_u64(&cluster_lms_state->native_probe_aggregate_waiter_conflict_count, 0);
		pg_atomic_init_u64(&cluster_lms_state->native_probe_retry_count, 0);
		pg_atomic_init_u64(&cluster_lms_state->native_probe_timeout_count, 0);
		pg_atomic_init_u64(&cluster_lms_state->native_probe_next_id, 1);
		/* slot in_use atomics initialized via memset above (all 0 = free). */
		ConditionVariableInit(&cluster_lms_state->cv);
	}
}

void
cluster_lms_shmem_request(void)
{
	RequestAddinShmemSpace(cluster_lms_shmem_size());
}

void
cluster_lms_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_lms_region);
}

ClusterLmsSharedState *
cluster_lms_shared_state(void)
{
	return cluster_lms_state;
}


/* ============================================================
 * Internal helpers.
 * ============================================================ */

/*
 * Atomically transition state.  Caller must hold lwlock LW_EXCLUSIVE
 * for non-atomic field updates (pid / spawned_at / ready_at);
 * lms_state itself is atomic so monotonic reads remain race-free
 * outside the lock.
 */
static void
lms_set_state(ClusterLmsState new_state)
{
	pg_atomic_write_u32(&cluster_lms_state->lms_state, (uint32)new_state);
}

static ClusterLmsState
lms_get_state(void)
{
	if (cluster_lms_state == NULL)
		return CLUSTER_LMS_NOT_STARTED;
	return (ClusterLmsState)pg_atomic_read_u32(&cluster_lms_state->lms_state);
}

static bool
lms_shutdown_requested(void)
{
	bool requested;

	if (cluster_lms_state == NULL)
		return true;
	LWLockAcquire(&cluster_lms_state->lwlock, LW_SHARED);
	requested = cluster_lms_state->shutdown_requested;
	LWLockRelease(&cluster_lms_state->lwlock);
	return requested;
}


/* ============================================================
 * Postmaster-side API (Q2 thin proxy / Q3 bounded polling).
 * ============================================================ */

int
cluster_lms_start(void)
{
	pid_t pid;

	Assert(!IsUnderPostmaster);

	if (!cluster_lms_enabled) {
		/*
		 * Startup-time fallback: no LMS child should be forked when the
		 * POSTMASTER GUC is off.  Mark DISABLED so SQL/debug surfaces can
		 * distinguish intentional opt-out from "not started yet".
		 */
		if (cluster_lms_state != NULL) {
			LWLockAcquire(&cluster_lms_state->lwlock, LW_EXCLUSIVE);
			lms_set_state(CLUSTER_LMS_DISABLED);
			LWLockRelease(&cluster_lms_state->lwlock);
		}
		return 0;
	}

	pid = cluster_postmaster_start_lms();
	return (int)pid;
}

bool
cluster_lms_wait_for_ready(int timeout_ms)
{
	const int poll_interval_ms = 100;
	int waited_ms = 0;

	Assert(!IsUnderPostmaster);

	if (cluster_lms_state == NULL)
		return false;

	/*
	 * DISABLED is a legitimate "ready-or-skip" terminal: when
	 * lms_enabled = off at startup, LMS process never forks, so
	 * the postmaster phase 1 driver should not block waiting for
	 * READY.  Return true immediately to skip the polling loop.
	 */
	if (lms_get_state() == CLUSTER_LMS_DISABLED)
		return true;

	while (waited_ms < timeout_ms) {
		ClusterLmsState state = lms_get_state();

		if (state == CLUSTER_LMS_READY)
			return true;

		/* Early failure: shutdown / stopped before ready. */
		if (state == CLUSTER_LMS_DRAINING || state == CLUSTER_LMS_STOPPED)
			return false;

		pg_usleep(poll_interval_ms * 1000L);
		waited_ms += poll_interval_ms;
	}

	return false;
}

void
cluster_lms_request_shutdown(void)
{
	Assert(!IsUnderPostmaster);

	if (cluster_lms_state == NULL)
		return;

	LWLockAcquire(&cluster_lms_state->lwlock, LW_EXCLUSIVE);
	cluster_lms_state->shutdown_requested = true;
	LWLockRelease(&cluster_lms_state->lwlock);

	/* Wake any future CV-based LMS waiter; current skeleton also polls latch. */
	ConditionVariableBroadcast(&cluster_lms_state->cv);
}


/* ============================================================
 * Read-only accessors (LW_SHARED + atomic reads).
 * ============================================================ */

ClusterLmsState
cluster_lms_get_state(void)
{
	return lms_get_state();
}

pid_t
cluster_lms_get_pid(void)
{
	pid_t pid;

	if (cluster_lms_state == NULL)
		return 0;
	LWLockAcquire(&cluster_lms_state->lwlock, LW_SHARED);
	pid = cluster_lms_state->pid;
	LWLockRelease(&cluster_lms_state->lwlock);
	return pid;
}

TimestampTz
cluster_lms_get_spawned_at(void)
{
	TimestampTz t;

	if (cluster_lms_state == NULL)
		return 0;
	LWLockAcquire(&cluster_lms_state->lwlock, LW_SHARED);
	t = cluster_lms_state->spawned_at;
	LWLockRelease(&cluster_lms_state->lwlock);
	return t;
}

TimestampTz
cluster_lms_get_ready_at(void)
{
	TimestampTz t;

	if (cluster_lms_state == NULL)
		return 0;
	LWLockAcquire(&cluster_lms_state->lwlock, LW_SHARED);
	t = cluster_lms_state->ready_at;
	LWLockRelease(&cluster_lms_state->lwlock);
	return t;
}

uint64
cluster_lms_get_started_count(void)
{
	if (cluster_lms_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lms_state->lms_started_count);
}

uint64
cluster_lms_get_work_drained_count(void)
{
	if (cluster_lms_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lms_state->lms_work_drained_count);
}

/*
 * spec-2.20 D9 — 3 NEW decision counter accessors.
 *
 *	Replaces the single cluster_lms_get_decision_count() — each grant
 *	decision body in LWLock window inc exactly one of grant/reject/convert.
 *	dump_lms (D10) + pg_cluster_lms view (D11) reflect the 3 counters.
 */
uint64
cluster_lms_get_decision_grant_count(void)
{
	if (cluster_lms_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lms_state->lms_decision_grant_count);
}

uint64
cluster_lms_get_decision_reject_count(void)
{
	if (cluster_lms_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lms_state->lms_decision_reject_count);
}

uint64
cluster_lms_get_decision_convert_count(void)
{
	if (cluster_lms_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lms_state->lms_decision_convert_count);
}

uint64
cluster_lms_get_drain_empty_count(void)
{
	if (cluster_lms_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lms_state->lms_drain_empty_count);
}

uint64
cluster_lms_get_error_count(void)
{
	if (cluster_lms_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lms_state->lms_error_count);
}

/* spec-2.25 D13 — 7 NEW native-lock probe counter accessors. */
#define DEFINE_LMS_NATIVE_PROBE_COUNTER_GETTER(name)                                               \
	uint64 cluster_lms_get_##name(void)                                                            \
	{                                                                                              \
		if (cluster_lms_state == NULL)                                                             \
			return 0;                                                                              \
		return pg_atomic_read_u64(&cluster_lms_state->name);                                       \
	}

DEFINE_LMS_NATIVE_PROBE_COUNTER_GETTER(native_probe_sent_count)
DEFINE_LMS_NATIVE_PROBE_COUNTER_GETTER(native_probe_reply_recv_count)
DEFINE_LMS_NATIVE_PROBE_COUNTER_GETTER(native_probe_collector_slot_full_count)
DEFINE_LMS_NATIVE_PROBE_COUNTER_GETTER(native_probe_aggregate_holder_conflict_count)
DEFINE_LMS_NATIVE_PROBE_COUNTER_GETTER(native_probe_aggregate_waiter_conflict_count)
DEFINE_LMS_NATIVE_PROBE_COUNTER_GETTER(native_probe_retry_count)
DEFINE_LMS_NATIVE_PROBE_COUNTER_GETTER(native_probe_timeout_count)


/* ============================================================
 * HC4 single ownership atomic guard.
 *
 * LMON tick body entry calls this to determine whether LMS owns
 * grant decisions.  Read-only atomic load — no LWLock needed; state
 * is monotonic so a stale read can only be one step behind, which
 * is acceptable for a non-correctness guard (the LMS-side LWLock-
 * protected critical section is the ultimate ownership boundary).
 * ============================================================ */

bool
cluster_lms_owns_grant(void)
{
	ClusterLmsState state;

	if (cluster_lms_state == NULL)
		return false;

	state = (ClusterLmsState)pg_atomic_read_u32(&cluster_lms_state->lms_state);

	/*
	 * DISABLED is a startup-only opt-out and must preserve LMON fallback.
	 * STOPPED is a runtime LMS failure and must remain fail-closed (no
	 * fallback grant path), matching HC1.
	 */
	return state == CLUSTER_LMS_READY || state == CLUSTER_LMS_DRAINING
		   || state == CLUSTER_LMS_STOPPED;
}


/*
 * HC4 EXACT predicate (spec-2.20 v0.3 frozen — L124 inherit).
 *
 *	cluster_lms_is_ready() returns true iff lms_state == CLUSTER_LMS_READY.
 *	**Critical regression防御** — 既有 cluster_lms_owns_grant() 有 latent
 *	bug 返回 READY OR DRAINING OR STOPPED(spec-2.19 P1.5 同款 bug 在 LMS
 *	pre-existing,只是无 caller exercise 未暴露)。spec-2.20 新代码必走
 *	cluster_lms_is_ready()(exact == READY)避免 false-positive:
 *	  - DRAINING (3) — LMS 正在 shutdown
 *	  - STOPPED (4) — LMS 已死 / not yet spawned
 *	  - DISABLED (5) — startup-time opt-out
 *	均 NOT ready。
 */
bool
cluster_lms_is_ready(void)
{
	if (cluster_lms_state == NULL)
		return false;

	return ((ClusterLmsState)pg_atomic_read_u32(&cluster_lms_state->lms_state))
		   == CLUSTER_LMS_READY;
}


/* ============================================================
 * HC3 producer wake.
 *
 * Producers call this after a successful enqueue.  Step 6 keeps the
 * producer-side CV contract as a stable API while the LMS skeleton uses
 * WaitLatch polling for idle sleep; production activation can switch the
 * consumer to CV once it owns a fully verified aux-process wake path.
 * No-op when LMS DISABLED (lms_enabled=off startup) since the LMS process
 * is not running to receive the wake.
 * ============================================================ */

void
cluster_lms_wake_drain(void)
{
	if (cluster_lms_state == NULL)
		return;
	if (lms_get_state() == CLUSTER_LMS_DISABLED)
		return;
	ConditionVariableBroadcast(&cluster_lms_state->cv);
}


/* ============================================================
 * LMS main entry.
 *
 *	Invoked from auxprocess.c dispatch when MyAuxProcType == LmsProcess.
 *	Runs the drain consumer loop until shutdown.  The Step 6 skeleton
 *	uses the same WaitLatch idle pattern as the existing cluster aux
 *	processes; producer-side ConditionVariable broadcast is retained as
 *	the compatibility surface for the later event-driven LMS path.
 *	      (CV ops happen outside signal context;handlers only set
 *	      latch / ShutdownRequestPending)
 * ============================================================ */

void
LmsMain(void)
{
	Assert(IsUnderPostmaster);

	MyBackendType = B_LMS;
	init_ps_display(NULL);

	/* Standard PG aux-process signal layout (modeled on cluster_lmon.c). */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SignalHandlerForShutdownRequest);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	/* SIGQUIT installed by InitPostmasterChild */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	/* No ProcSignal slot in the skeleton; see auxprocess.c early setup. */
	pqsignal(SIGUSR1, SIG_IGN);
	pqsignal(SIGUSR2, SIG_IGN);
	pqsignal(SIGCHLD, SIG_DFL);

	sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);

	if (cluster_lms_state == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_INTERNAL_ERROR), errmsg("cluster_lms shmem region not attached"),
				 errhint("cluster_lms_shmem_init() must run during "
						 "CreateSharedMemoryAndSemaphores().")));

	/* Publish STARTING + record pid / spawned_at. */
	LWLockAcquire(&cluster_lms_state->lwlock, LW_EXCLUSIVE);
	cluster_lms_state->pid = MyProcPid;
	cluster_lms_state->spawned_at = GetCurrentTimestamp();
	lms_set_state(CLUSTER_LMS_STARTING);
	LWLockRelease(&cluster_lms_state->lwlock);

	/* Transition to READY. */
	LWLockAcquire(&cluster_lms_state->lwlock, LW_EXCLUSIVE);
	cluster_lms_state->ready_at = GetCurrentTimestamp();
	pg_atomic_write_u64(&cluster_lms_state->lms_ready_at_us, (uint64)cluster_lms_state->ready_at);
	pg_atomic_fetch_add_u64(&cluster_lms_state->lms_started_count, 1);
	lms_set_state(CLUSTER_LMS_READY);
	LWLockRelease(&cluster_lms_state->lwlock);

	/*
	 * spec-2.18 Sprint A Step 1-6 skeleton main loop.  LMS daemon exists for
	 * catalog visibility + ABI surface (B_LMS / LmsProcess / pg_cluster_lms
	 * later) but does NOT yet own the GES drain consumer — LMON keeps that
	 * role until the Hardening round wires ownership transfer.  The body is
	 * a pure idle WaitLatch loop: SIGTERM sets ShutdownRequestPending +
	 * MyLatch, WaitLatch returns, the head guard breaks, proc_exit(0)
	 * completes cleanly without any cluster shmem cleanup.  No CV
	 * sleep / broadcast in this skeleton — pss_barrierCV is exercised
	 * only via standard PG paths during proc_exit, which is verified safe.
	 */
	for (;;) {
		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending) {
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (ShutdownRequestPending || lms_shutdown_requested())
			break;

		pg_atomic_fetch_add_u64(&cluster_lms_state->lms_drain_empty_count, 1);

		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						LMS_IDLE_TIMEOUT_MS, WAIT_EVENT_PG_SLEEP);
		ResetLatch(MyLatch);
	}

	/* Transition to DRAINING then STOPPED. */
	LWLockAcquire(&cluster_lms_state->lwlock, LW_EXCLUSIVE);
	lms_set_state(CLUSTER_LMS_DRAINING);
	LWLockRelease(&cluster_lms_state->lwlock);

	LWLockAcquire(&cluster_lms_state->lwlock, LW_EXCLUSIVE);
	cluster_lms_state->stopped_at = GetCurrentTimestamp();
	lms_set_state(CLUSTER_LMS_STOPPED);
	LWLockRelease(&cluster_lms_state->lwlock);

	proc_exit(0);
}


/* ============================================================
 * spec-2.25 D4 — native-lock probe collector lifecycle (Step 5).
 *
 *	Public API mirrors cluster_native_lock_probe.h declarations.
 *	Slot acquire/release uses pg_atomic_compare_exchange on slot->in_use
 *	(slot-level barrier — coarse LMS lwlock kept for aggregate /
 *	retry-tick which mutate cross-slot state).
 *
 *	HC29:  per-shard bounded array sized by GUC max_inflight;  Step 5
 *	implementation is single-LMS-process (spec-2.18 daemon shape — no
 *	shard partition yet);  per-shard split lands when LMS shard daemons
 *	come online (post spec-2.27).
 *
 *	HC32 retry-poll loop:  cluster_lms_native_probe_retry_tick is wired
 *	into LmsMain idle tick (call site added below in dispatch helper);
 *	wakeups via cluster_lms_state->cv allow event-driven advance on
 *	reply arrival in addition to interval polling.
 *
 *	HC36 stale-reply drop:  reply with probe_id ∉ active slots returns
 *	silently + counter increments — handled in recv_reply path.
 * ============================================================ */

static int
probe_active_capacity(void)
{
	int cap = cluster_lms_native_lock_probe_max_inflight;

	if (cap < 1)
		cap = 1;
	if (cap > CLUSTER_LMS_NATIVE_LOCK_PROBE_MAX_SLOTS)
		cap = CLUSTER_LMS_NATIVE_LOCK_PROBE_MAX_SLOTS;
	return cap;
}

bool
cluster_lms_native_probe_slot_acquire(int32 origin_node_id, const LOCKTAG *locktag,
									  LOCKMODE lockmode, const ClusterGrdHolderId *requester,
									  uint32 *slot_idx_out)
{
	int cap, i;

	Assert(cluster_lms_state != NULL);
	Assert(locktag != NULL);
	Assert(requester != NULL);
	Assert(slot_idx_out != NULL);
	if (cluster_lms_state == NULL || locktag == NULL || requester == NULL || slot_idx_out == NULL)
		return false;

	cap = probe_active_capacity();

	for (i = 0; i < cap; i++) {
		ClusterLmsNativeLockProbeSlot *slot = &cluster_lms_state->native_probe_slots[i];
		uint64 expected = 0;

		if (pg_atomic_compare_exchange_u64(&slot->in_use, &expected, 1)) {
			/* Slot acquired — initialize.  No LWLock needed because in_use
			 * atomically guards visibility for all readers. */
			slot->probe_id = pg_atomic_fetch_add_u64(&cluster_lms_state->native_probe_next_id, 1);
			slot->locktag = *locktag;
			slot->lockmode = lockmode;
			slot->origin_node_id = origin_node_id;
			slot->requester_procno = (int32)requester->procno;
			slot->requester = *requester;
			slot->start_ts = GetCurrentTimestamp();
			slot->retry_count = 0;
			slot->expected_replies_bitmap = 0;
			slot->received_replies_bitmap = 0;
			slot->aggregated_status_packed = 0;

			*slot_idx_out = (uint32)i;
			return true;
		}
	}

	/* HC29:  capacity exhausted.  Caller enqueues into LMS pending queue
	 * (Step 5 surface; pending queue wire 推 spec-2.27 + future shard
	 * partition).  Bump counter for observability. */
	pg_atomic_fetch_add_u64(&cluster_lms_state->native_probe_collector_slot_full_count, 1);
	return false;
}

void
cluster_lms_native_probe_slot_release(uint32 slot_idx)
{
	ClusterLmsNativeLockProbeSlot *slot;

	Assert(cluster_lms_state != NULL);
	Assert(slot_idx < CLUSTER_LMS_NATIVE_LOCK_PROBE_MAX_SLOTS);
	if (cluster_lms_state == NULL || slot_idx >= CLUSTER_LMS_NATIVE_LOCK_PROBE_MAX_SLOTS)
		return;

	slot = &cluster_lms_state->native_probe_slots[slot_idx];
	pg_atomic_write_u64(&slot->in_use, 0);
}

void
cluster_lms_native_probe_dispatch(uint32 slot_idx)
{
	ClusterLmsNativeLockProbeSlot *slot;
	GesNativeLockProbePayload payload;
	int peer_count = 0;
	int self_node = cluster_node_id;
	/* Local fan-out walk:  for every live peer in cluster_conf, send a
	 * probe message + mark expected_replies_bitmap.  Origin self short-
	 * circuits (HC32a) by invoking cluster_native_lock_probe_local()
	 * directly + marking own bit as already-received CLEAR/HOLDER/WAITER. */

	Assert(cluster_lms_state != NULL);
	Assert(slot_idx < CLUSTER_LMS_NATIVE_LOCK_PROBE_MAX_SLOTS);
	if (cluster_lms_state == NULL || slot_idx >= CLUSTER_LMS_NATIVE_LOCK_PROBE_MAX_SLOTS)
		return;

	slot = &cluster_lms_state->native_probe_slots[slot_idx];

	memset(&payload, 0, sizeof(payload));
	payload.opcode = GES_REQ_OPCODE_NATIVE_LOCK_PROBE;
	payload.lockmode = (uint32)slot->lockmode;
	memcpy(payload.locktag_bytes, &slot->locktag, sizeof(LOCKTAG));
	payload.probe_id = slot->probe_id;

	/* Origin self-probe (HC32a):  short-circuit local function call. */
	{
		ClusterNativeLockProbeReply self_status;
		uint32 self_bit;

		self_status
			= cluster_native_lock_probe_local(&slot->locktag, slot->lockmode, &slot->requester);
		self_bit = 1u << ((uint32)self_node & 31);
		slot->expected_replies_bitmap |= self_bit;
		slot->received_replies_bitmap |= self_bit;
		if (self_node >= 0 && self_node < 16) {
			slot->aggregated_status_packed |= ((uint32)self_status & 0x3)
											  << ((uint32)self_node * 2);
		}
	}

	/* Fan-out to live peers.  ClusterConfShmem.nodes[] is private to
	 * cluster_conf.c, so iterate by candidate node_id over the declared
	 * range (0..CLUSTER_MAX_NODES-1) and use the public lookup helper.
	 * 128 candidates × 1 lookup is cheap and avoids exposing the internal
	 * table layout. */
	{
		int32 candidate;

		for (candidate = 0; candidate < CLUSTER_MAX_NODES; candidate++) {
			const ClusterNodeInfo *conf_node;
			uint32 peer_bit;

			if (candidate == self_node)
				continue;
			conf_node = cluster_conf_lookup_node(candidate);
			if (conf_node == NULL)
				continue;
			peer_bit = 1u << ((uint32)candidate & 31);
			slot->expected_replies_bitmap |= peer_bit;
			cluster_grd_outbound_enqueue_lms_native_probe((uint32)candidate, &payload,
														  (uint16)sizeof(payload));
			pg_atomic_fetch_add_u64(&cluster_lms_state->native_probe_sent_count, 1);
			peer_count++;
		}
	}

	/* Capture dispatch ts for retry-tick (HC32 retry-poll). */
	slot->start_ts = GetCurrentTimestamp();

	/* Wake any LMS waiter watching for collector advance. */
	ConditionVariableBroadcast(&cluster_lms_state->cv);

	(void)peer_count; /* peer_count == 0 path is legitimate single-node mode */
}

void
cluster_lms_native_probe_recv_reply(uint64 probe_id, int32 sender_node_id,
									ClusterNativeLockProbeReply status)
{
	int cap, i;

	Assert(cluster_lms_state != NULL);
	if (cluster_lms_state == NULL)
		return;

	pg_atomic_fetch_add_u64(&cluster_lms_state->native_probe_reply_recv_count, 1);

	cap = probe_active_capacity();
	for (i = 0; i < cap; i++) {
		ClusterLmsNativeLockProbeSlot *slot = &cluster_lms_state->native_probe_slots[i];
		uint32 sender_bit;

		if (pg_atomic_read_u64(&slot->in_use) == 0)
			continue;
		if (slot->probe_id != probe_id)
			continue;

		sender_bit = 1u << ((uint32)sender_node_id & 31);
		if ((slot->expected_replies_bitmap & sender_bit) == 0)
			return; /* HC36 stale reply (not expected from this node) */
		if ((slot->received_replies_bitmap & sender_bit) != 0)
			return; /* duplicate reply — silent drop */

		slot->received_replies_bitmap |= sender_bit;
		if (sender_node_id >= 0 && sender_node_id < 16) {
			slot->aggregated_status_packed |= ((uint32)status & 0x3)
											  << ((uint32)sender_node_id * 2);
		}

		/* Wake LMS to attempt aggregate resolution. */
		ConditionVariableBroadcast(&cluster_lms_state->cv);
		return;
	}

	/* No matching slot — HC36 stale-reply drop. */
}

void
cluster_lms_native_probe_aggregate_and_resolve(uint32 slot_idx)
{
	ClusterLmsNativeLockProbeSlot *slot;
	uint32 i;
	bool saw_holder = false;
	bool saw_waiter = false;

	Assert(cluster_lms_state != NULL);
	Assert(slot_idx < CLUSTER_LMS_NATIVE_LOCK_PROBE_MAX_SLOTS);

	slot = &cluster_lms_state->native_probe_slots[slot_idx];
	if (pg_atomic_read_u64(&slot->in_use) == 0)
		return;

	if (slot->expected_replies_bitmap != slot->received_replies_bitmap)
		return; /* not all replies in yet */

	for (i = 0; i < 16; i++) {
		uint32 status = (slot->aggregated_status_packed >> (i * 2)) & 0x3;

		if (status == (uint32)CLUSTER_NATIVE_LOCK_PROBE_HOLDER_CONFLICT)
			saw_holder = true;
		else if (status == (uint32)CLUSTER_NATIVE_LOCK_PROBE_WAITER_CONFLICT)
			saw_waiter = true;
	}

	if (saw_holder) {
		pg_atomic_fetch_add_u64(&cluster_lms_state->native_probe_aggregate_holder_conflict_count,
								1);
		/* Caller (LMS grant decision) retries probe on next tick — slot kept
		 * in_use; retry_tick advances retry_count. */
		return;
	}
	if (saw_waiter) {
		pg_atomic_fetch_add_u64(&cluster_lms_state->native_probe_aggregate_waiter_conflict_count,
								1);
		return;
	}

	/* CLEAR aggregate — slot owner (grant decision) consumes status and
	 * releases slot.  Step 6 ges request handler reply path drives this. */
}

void
cluster_lms_native_probe_retry_tick(void)
{
	int cap, i;
	TimestampTz now;
	int interval_ms;
	int budget;

	if (cluster_lms_state == NULL)
		return;

	cap = probe_active_capacity();
	now = GetCurrentTimestamp();
	interval_ms = cluster_lms_native_lock_probe_retry_interval_ms;
	budget = cluster_lms_native_lock_probe_retry_budget;

	for (i = 0; i < cap; i++) {
		ClusterLmsNativeLockProbeSlot *slot = &cluster_lms_state->native_probe_slots[i];
		long diff_ms;

		if (pg_atomic_read_u64(&slot->in_use) == 0)
			continue;
		if (slot->expected_replies_bitmap == slot->received_replies_bitmap)
			continue; /* aggregate path drives non-retry advance */

		diff_ms = (long)((now - slot->start_ts) / 1000); /* µs → ms */
		if (diff_ms < interval_ms)
			continue;

		/* HC32 retry — clear received bitmap (forces fresh fan-out) and
		 * re-dispatch.  retry_count bumped + budget check. */
		slot->retry_count++;
		pg_atomic_fetch_add_u64(&cluster_lms_state->native_probe_retry_count, 1);

		if (slot->retry_count > (uint32)budget) {
			/* HC32 fail-closed:  emit timeout signal to caller (Step 6 ges
			 * reply path encodes 53R83 → requester ereports).  Mark slot for
			 * release once caller consumes timeout result. */
			pg_atomic_fetch_add_u64(&cluster_lms_state->native_probe_timeout_count, 1);
			/* Release slot — caller's wait must time out via ges reply
			 * pipeline; explicit 53R83 emission lands in Step 6 D5 when
			 * grant decision body integrates. */
			cluster_lms_native_probe_slot_release((uint32)i);
			continue;
		}

		/* Re-dispatch with fresh expected/received bitmaps (origin self short-
		 * circuit + peer fan-out).  dispatch resets bitmaps + ts. */
		slot->expected_replies_bitmap = 0;
		slot->received_replies_bitmap = 0;
		slot->aggregated_status_packed = 0;
		cluster_lms_native_probe_dispatch((uint32)i);
	}
}

void
cluster_lms_native_probe_cleanup_on_node_dead(int32 dead_node_id)
{
	int cap, i;
	uint32 dead_bit;

	if (cluster_lms_state == NULL)
		return;

	/* HC35 fence-gated:  caller MUST verify CSSD/fence + GRD
	 * cleanup_on_node_dead generation completed before invoking this.
	 * Premature CLEAR-on-dead risks split-brain false grant. */

	cap = probe_active_capacity();
	dead_bit = 1u << ((uint32)dead_node_id & 31);

	for (i = 0; i < cap; i++) {
		ClusterLmsNativeLockProbeSlot *slot = &cluster_lms_state->native_probe_slots[i];

		if (pg_atomic_read_u64(&slot->in_use) == 0)
			continue;
		if ((slot->expected_replies_bitmap & dead_bit) == 0)
			continue;

		/* Treat dead node as CLEAR (post-fence safe).  Set received bit +
		 * leave packed status at default 0 (CLEAR encoding). */
		slot->received_replies_bitmap |= dead_bit;
	}

	ConditionVariableBroadcast(&cluster_lms_state->cv);
}

void
cluster_lms_native_probe_cleanup_on_backend_exit(int procno)
{
	int cap, i;

	if (cluster_lms_state == NULL)
		return;

	cap = probe_active_capacity();
	for (i = 0; i < cap; i++) {
		ClusterLmsNativeLockProbeSlot *slot = &cluster_lms_state->native_probe_slots[i];

		if (pg_atomic_read_u64(&slot->in_use) == 0)
			continue;
		if (slot->requester_procno != procno)
			continue;

		/* HC34 — release slot;  any in-flight reply will be dropped via
		 * HC36 stale-reply path (probe_id no longer matches active slot). */
		cluster_lms_native_probe_slot_release((uint32)i);
	}
}
