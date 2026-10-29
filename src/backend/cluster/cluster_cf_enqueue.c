/*-------------------------------------------------------------------------
 *
 * cluster_cf_enqueue.c
 *	  CF (control file) cluster enqueue over the spec-5.3 GES substrate
 *	  (spec-5.6).
 *
 *	  (this stage): the singleton CF resource-id encoder.  It adds the
 *	  cluster_cf_lock/unlock acquire/release wrappers that build a
 *	  ClusterLockAcquireRequest for the CF resid and drive the GES seven-
 *	  step state machine.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cf_enqueue.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <errno.h>

#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_cf_stats.h"
#include "cluster/cluster_clean_leave.h" /* RF-ROOT P6: leaver write-refusal gate */
#include "cluster/cluster_conf.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_ges.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_lock_acquire.h"
#include "cluster/cluster_lock_owner.h"
#include "miscadmin.h" /* AmStartupProcess / AmCheckpointerProcess */
#include "portability/instr_time.h"
#include "storage/fd.h"
#include "storage/lock.h"
#include "storage/proc.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

/*
 * CfHoldState -- per-backend record of a held CF lock so the matching
 * release can target the exact GRD holder + request_id the acquire
 * registered.  `coordinated` is false when the acquire returned OK_NATIVE
 * (cluster/LMS layer inactive): nothing was registered in the GRD, so the
 * release must not run S6 against a phantom holder.  `release_pending` revokes
 * local authority before S6 can yield or throw, without forgetting a possibly
 * live remote holder.  There is one slot per mode (X / S); CF is not reentrant
 * within a backend for a given mode.
 */
typedef struct CfHoldState {
	bool held;
	bool coordinated;
	bool release_pending;
	const void *caller;		/* Process-local task lifetime, never remote authority. */
	ClusterLockOwner owner; /* Stable S5/reconstruction/S6 ownership. */
} CfHoldState;

static CfHoldState cf_hold_x;
static CfHoldState cf_hold_s;
static uint64 cf_retired_x, cf_retired_s;
static ClusterCfAttemptObservation cf_attempt;
static struct {
	ClusterGrdHolderId holder;
	uint64 started_ms, master_generation;
} cf_trace_origin;

static ClusterCfReleaseResult cf_unlock(LOCKMODE mode, const void *caller);

/* PGRAC: publication is local to this synchronous caller. Even an early
 * refusal invalidates its predecessor; saturation permanently fails closed.
 * Author: SqlRush <sqlrush@gmail.com> */
static void
cf_attempt_begin(LOCKMODE mode, bool cooperative, const void *caller)
{
	uint64 sequence = cf_attempt.sequence;

	memset(&cf_attempt, 0, sizeof(cf_attempt));
	cf_attempt.sequence = sequence == UINT64_MAX ? sequence : sequence + 1;
	cf_attempt.mode = mode;
	cf_attempt.cooperative = cooperative;
	cf_attempt.caller = caller;
	cf_attempt.result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
}

uint64
cluster_cf_attempt_sequence(void)
{
	return cf_attempt.sequence;
}

bool
cluster_cf_last_attempt_since(uint64 before, ClusterCfAttemptObservation *out)
{
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (cf_attempt.sequence == UINT64_MAX || before >= cf_attempt.sequence)
		return false;
	*out = cf_attempt;
	return true;
}

bool
cluster_cf_attempt_wait_slice(const ClusterCfAttemptObservation *observation)
{
	return observation != NULL && observation->sequence != 0 && observation->sequence != UINT64_MAX
		   && observation->sequence == cf_attempt.sequence && observation->classified_wait
		   && !observation->cooperative && observation->caller == NULL
		   && observation->result == CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT
		   && observation->source == CLUSTER_GES_TSRC_CV_WAIT_TIMEOUT
		   && (observation->owner_state_at_return == CLUSTER_LOCK_OWNER_EMPTY
			   || (observation->owner_state_at_return == CLUSTER_LOCK_OWNER_RETIRING
				   && observation->control_owner_id != 0));
}

/*
 * spec-5.6: set while this process is the bootstrap single-node authority
 * (sole-liveness + storage contract proven, GES not yet ready).  Lets the
 * write path proceed without a held CF X during early recovery.
 */
static bool cf_bootstrap_authority = false;

/*
 * RF-B: process-local permission for the EOR checkpointer.  It is enabled only
 * after the shared INSTALLED -> ACTIVE CAS and is never inferred from phase.
 */
static bool cf_owner_eor_authority = false;

/*
 * spec-5.6 increment (ii/iii): JOIN_READONLY marks an attaching (join) node
 * recovering against a live peer that owns the shared authority.  Unlike the
 * single-node OWNER window above (process-local: only the startup process
 * writes the authority during single-node bring-up), the join role must be
 * visible CROSS-PROCESS -- the checkpointer's end-of-recovery checkpoint must
 * see it to skip CF X before GES is ready -- so it is backed by the lock-free
 * CF shmem flag (cluster_cf_stats_*), not a process-local static.
 */

static CfHoldState *
cf_slot(LOCKMODE mode)
{
	Assert(mode == ShareLock || mode == ExclusiveLock);
	return (mode == ExclusiveLock) ? &cf_hold_x : &cf_hold_s;
}

/*
 * cluster_cf_resid_encode -- build the singleton CF resource id.
 *
 *	CF is one whole-file lock, so every map field is zero; only the type
 *	byte (CLUSTER_CF_RESID_TYPE) places it in the CF namespace.  The
 *	lockmethodid mirrors the SQ encoder (DEFAULT_LOCKMETHOD) so the GES
 *	routing hash treats it uniformly.
 */
void
cluster_cf_resid_encode(ClusterResId *dst)
{
	Assert(dst != NULL);
	if (dst == NULL)
		return;

	dst->field1 = 0;
	dst->field2 = 0;
	dst->field3 = 0;
	dst->field4 = 0;
	dst->type = CLUSTER_CF_RESID_TYPE;
	dst->lockmethodid = DEFAULT_LOCKMETHOD;
}

/* Preserve the original request even when an early refusal cannot be retried. */
static void
cf_attempt_owner(const CfHoldState *slot)
{
	cf_attempt.owner_state_at_return = slot->owner.state;
	cf_attempt.request_id = slot->owner.request.request_id;
	cf_attempt.request_epoch = slot->owner.request.holder.cluster_epoch;
	cf_attempt.control_owner_id = slot->owner.request.control_owner_id;
	if (cluster_trace_control_config) {
		cf_trace_origin.holder = slot->owner.request.holder;
		cf_trace_origin.started_ms = slot->owner.request.caller_local_start_ts_ms;
		cf_trace_origin.master_generation = slot->owner.request.master_gen_snapshot;
	}
}

/*
 * cluster_cf_lock -- acquire the singleton CF lock in `mode` via GES.
 */
static bool
cf_lock(LOCKMODE mode, bool cooperative, const void *caller, uint64 deadline_ns)
{
	ClusterLockAcquireResult r;
	CfHoldState *slot = cf_slot(mode);
	ClusterLockAcquireRequest *req = &slot->owner.request;
	const ClusterGesTimeoutDetail *timeout;
	bool resume;
	int acquire_timeout_ms;
	int cleanup_timeout_ms = 0;

	cf_attempt_begin(mode, cooperative, caller);
	cf_attempt_owner(slot);
	/* Several duties share an auxiliary process. Only the initiating duty
	 * may resume its pending request or drain its old hold. This check must
	 * precede every poll/release, including the legacy stale-hold cleanup. */
	if (slot->held && slot->caller != caller)
		return false;
	resume = cooperative && slot->held && slot->owner.cooperative_acquire
			 && slot->owner.state == CLUSTER_LOCK_OWNER_ACQUIRING;

	/*
	 * RF-ROOT P6 (shutdown-handoff wiring, "stop new local CF requests"):
	 * while THIS node is the leaver in a clean-leave drain
	 * (REQUESTED..COMMITTED) no new local CF acquire may start — the
	 * checkpointer driver is exempt, as it is the designated drain context
	 * and acquires the CF for the shutdown checkpoint and THREAD_CLEAN_CLOSE
	 * itself.  Fail closed (the caller surfaces CF-unavailable); the one-shot
	 * quiesce already aborts writable backends, this gate closes the race for
	 * a backend that slips past it.
	 */
	if (cluster_clean_leave_node_refuses_writes() && !AmCheckpointerProcess()) {
		ereport(LOG, (errmsg("cluster CF acquire refused: clean-leave drain in "
							 "progress on this node (mode %d)",
							 (int)mode)));
		return false;
	}

	/*
	 * CF is not reentrant within a backend (one caller-level acquire).
	 *
	 * RF-ROOT P6 hardening (AGENTS.md: required runtime safety must not
	 * depend on Assert()):  a slot can be left marked held by a prior
	 * UNCONFIRMED release -- cluster_cf_unlock_confirmed deliberately
	 * keeps `held` set when the cross-node S6 release cannot be proven,
	 * because clearing it would risk a double grant.  A later acquire in
	 * the same process used to trip the non-reentrant Assert and crash
	 * the process (observed: seed-member checkpointer TRAP during the
	 * clean-close root publish, turning a clean shutdown abnormal).
	 * Drain the stale hold with a confirmed release attempt instead; if
	 * the drain still cannot confirm, fail closed.  An uncoordinated
	 * (native) hold drains to NOT_HELD with the slot cleared, which is
	 * equally safe to proceed from.
	 */
	if (slot->held && !resume) {
		ClusterCfReleaseResult drain = cf_unlock(mode, caller);

		if (drain == CLUSTER_CF_RELEASE_UNCONFIRMED) {
			cf_attempt_owner(slot);
			ereport(LOG, (errmsg("cluster CF acquire refused: stale held slot could "
								 "not be drained (mode %d)",
								 (int)mode)));
			return false;
		}
	}
	if (resume)
		goto acquire;
	Assert(!slot->held);
	cf_attempt_owner(slot);

	/* Only the caller that owns an absolute deadline supplies it. Read the
	 * remaining budget after old-slot cleanup, immediately before publishing
	 * a new request. Cleanup retains its independent original timeout.
	 * Author: SqlRush <sqlrush@gmail.com> */
	acquire_timeout_ms = cluster_cf_enqueue_timeout_ms;
	cleanup_timeout_ms = acquire_timeout_ms;
	if (deadline_ns != 0) {
		struct timespec now;
		uint64 now_ns, remaining_ms;

		if (clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0 || now.tv_nsec < 0
			|| now.tv_nsec >= NS_PER_S
			|| (uint64)now.tv_sec > (UINT64_MAX - (uint64)now.tv_nsec) / NS_PER_S)
			return false;
		now_ns = (uint64)now.tv_sec * NS_PER_S + (uint64)now.tv_nsec;
		if (now_ns >= deadline_ns || acquire_timeout_ms <= 0)
			return false;
		remaining_ms = (deadline_ns - now_ns) / NS_PER_MS;
		if (remaining_ms == 0)
			return false;
		acquire_timeout_ms = (int)Min((uint64)acquire_timeout_ms, remaining_ms);
	}

	memset(req, 0, sizeof(*req));
	cluster_cf_resid_encode(&req->resid);
	/* locktag left zeroed: not LOCKTAG_ADVISORY, so normal blocking semantics. */
	req->lockmode = mode;
	req->op = CLUSTER_LOCK_OP_REQUEST; /* CF never converts (X/S independent) */
	req->current_mode = NoLock;
	req->lockmethod_id = DEFAULT_LOCKMETHOD;
	req->dontwait = false; /* block until granted or timeout */
	req->sessionLock = false;
	req->caller_local_start_ts_ms = (uint64)(GetCurrentTimestamp() / 1000);
	/* spec-5.6 Dc4b: bound the CF acquire wait and label it ClusterCfEnqueueWait. */
	req->timeout_ms = acquire_timeout_ms;
	req->wait_event = WAIT_EVENT_CLUSTER_CF_ENQUEUE;

	/* Visible cleanup responsibility precedes every possible S3/S4 mutation;
	 * usable authority still requires the owned S5 completion. */
	slot->held = slot->coordinated = slot->release_pending = true;
	slot->caller = caller;
acquire:
	PG_TRY();
	{
		r = cooperative ? cluster_lock_owner_acquire_poll(&slot->owner)
						: cluster_lock_owner_acquire(&slot->owner);
	}
	PG_CATCH();
	{
		if (deadline_ns != 0 && slot->owner.state != CLUSTER_LOCK_OWNER_EMPTY)
			req->timeout_ms = cleanup_timeout_ms;
		cf_attempt_owner(slot);
		if (slot->owner.state == CLUSTER_LOCK_OWNER_EMPTY)
			memset(slot, 0, sizeof(*slot));
		PG_RE_THROW();
	}
	PG_END_TRY();
	/* The cap belongs only to S1-S5. Every retained success, refusal or
	 * ERROR keeps the original release budget, including its first bounded
	 * confirmation. Identity and registry binding are unchanged. */
	if (deadline_ns != 0 && slot->owner.state != CLUSTER_LOCK_OWNER_EMPTY)
		req->timeout_ms = cleanup_timeout_ms;
	/* Capture before EMPTY-slot cleanup erases the just-completed request. */
	timeout = r == CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT ? cluster_ges_timeout_detail_get() : NULL;
	cf_attempt.result = r;
	cf_attempt.source = timeout != NULL ? timeout->source : CLUSTER_GES_TSRC_NONE;
	cf_attempt_owner(slot);
	cf_attempt.classified_wait = !cooperative && caller == NULL && cf_attempt.sequence != UINT64_MAX
								 && r == CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT
								 && cf_attempt.source == CLUSTER_GES_TSRC_CV_WAIT_TIMEOUT
								 && (slot->owner.state == CLUSTER_LOCK_OWNER_EMPTY
									 || slot->owner.state == CLUSTER_LOCK_OWNER_RETIRING);

	switch (r) {
	case CLUSTER_LOCK_ACQUIRE_OK_NATIVE:

		/*
			 * Cluster/LMS layer inactive (single-node or gate off): no cross-
			 * node coordination is needed and nothing was registered in the
			 * GRD.  Treat as held but uncoordinated so release is a no-op.
			 */
		slot->held = true;
		slot->coordinated = false;
		slot->release_pending = false;
		cluster_cf_counter_inc(mode == ExclusiveLock ? CLUSTER_CF_X_ACQUIRE : CLUSTER_CF_S_ACQUIRE);
		return true;

	case CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK:
	case CLUSTER_LOCK_ACQUIRE_OK_GRANTED:
	case CLUSTER_LOCK_ACQUIRE_OK_CONVERTED:

		/* CF has no PG-native lock. Its stable owner already ran S5. */
		if (!cluster_lock_owner_is_usable(&slot->owner)) {
			cluster_cf_counter_inc(CLUSTER_CF_FAILCLOSED);
			return false;
		}
		slot->release_pending = false;
		cluster_cf_counter_inc(mode == ExclusiveLock ? CLUSTER_CF_X_ACQUIRE : CLUSTER_CF_S_ACQUIRE);
		return true;

	case CLUSTER_LOCK_ACQUIRE_PENDING:
		if (cooperative) {
			if (slot->owner.state == CLUSTER_LOCK_OWNER_EMPTY)
				memset(slot, 0, sizeof(*slot));
			return false; /* Stable pending request, no new ID and no error. */
		}
		/* fall through */
	default:

		/*
			 * NOT_AVAIL (try conflict; CF blocks so this is unexpected),
			 * timeout, LMS-unavailable, deadlock, internal, etc.  The lock
			 * could not be proven held: fail closed.  The caller raises the
			 * appropriate FATAL/ERROR (CF correctness).
			 */
		cluster_cf_counter_inc(CLUSTER_CF_FAILCLOSED);
		ereport(
			LOG,
			(errmsg("cluster CF acquire failed (mode %d, result %d)", (int)mode, (int)r),
			 errdetail("PGRAC_FAMILY=CF_ACQUIRE request=" UINT64_FORMAT " epoch=" UINT64_FORMAT
					   " master_generation=" UINT64_FORMAT " owner_state=%d registration=%s"
					   " timeout_source=%d timeout_master=%d elapsed_ms=%ld attempts=%d "
					   "conflicts=%d timeout_ms=%d",
					   req->request_id, req->holder.cluster_epoch, req->master_gen_snapshot,
					   (int)slot->owner.state,
					   req->registration_failure_reason != NULL ? req->registration_failure_reason
																: "UNREPORTED",
					   timeout != NULL ? (int)timeout->source : 0,
					   timeout != NULL ? timeout->master_node : -1,
					   timeout != NULL ? timeout->elapsed_ms : 0L,
					   timeout != NULL ? timeout->attempts : 0,
					   timeout != NULL ? timeout->conflict_holders : -1,
					   timeout != NULL ? timeout->timeout_ms : 0)));
		if (slot->owner.state == CLUSTER_LOCK_OWNER_EMPTY)
			memset(slot, 0, sizeof(*slot));
		return false;
	}
}

/* Copy original return facts, including requests cleared by EMPTY cleanup.
 * A grant record maps this exact tuple to its own PID, never a remote procno. */
static bool
cf_lock_observed(LOCKMODE mode, bool cooperative, const void *caller, uint64 deadline_ns)
{
	static TimestampTz last_refusal;
	bool acquired = cf_lock(mode, cooperative, caller, deadline_ns);
	TimestampTz now;
	uint64 now_ms, age_ms = 0;
	bool age_known, pid_matches;
	CfHoldState *slot;

	if (!cluster_trace_control_config || CritSectionCount != 0)
		return acquired;
	now = GetCurrentTimestamp();
	if (!acquired) {
		if (last_refusal != 0 && now >= last_refusal
			&& now - last_refusal < INT64_C(5000000))
			return acquired;
		last_refusal = now;
	}
	now_ms = now >= 0 ? (uint64)(now / 1000) : 0;
	age_known = cf_trace_origin.started_ms != 0 && now_ms >= cf_trace_origin.started_ms;
	if (age_known)
		age_ms = now_ms - cf_trace_origin.started_ms;
	slot = cf_slot(mode);
	pid_matches = acquired && slot->coordinated && MyProc != NULL
		&& cf_trace_origin.holder.node_id == (uint32)cluster_node_id
		&& cf_trace_origin.holder.procno == (uint32)MyProc->pgprocno
		&& cf_trace_origin.holder.request_id != 0
		&& cf_trace_origin.holder.request_id == cf_attempt.request_id;
	ereport(LOG,
		(errmsg("PGRAC_FAMILY=CF_RETURN resource=CF res_type=%u lockmethod=%u field1=0 field2=0 field3=0 ts=" INT64_FORMAT
				" host_node=%d caller_pid=%d role=%d mode=%d acquired=%d cooperative=%d"
				" result=%d source=%d owner_state=%d retained=%d same_task=%d coordinated=%d"
				" request_node=%u request_procno=%u request_epoch=" UINT64_FORMAT
				" request_id=" UINT64_FORMAT " control_owner_id=" UINT64_FORMAT
				" master_generation=" UINT64_FORMAT " age_known=%d request_age_ms=" UINT64_FORMAT
				" pid_matches_holder=%d holder_pid=%d",
				CLUSTER_CF_RESID_TYPE, DEFAULT_LOCKMETHOD, now, cluster_node_id, MyProcPid,
				(int)MyBackendType, (int)mode, acquired, cooperative, (int)cf_attempt.result,
				(int)cf_attempt.source, (int)cf_attempt.owner_state_at_return,
				slot->held, slot->held && slot->caller == caller, slot->coordinated,
				cf_trace_origin.holder.node_id, cf_trace_origin.holder.procno,
				cf_trace_origin.holder.cluster_epoch, cf_trace_origin.holder.request_id,
				cf_attempt.control_owner_id, cf_trace_origin.master_generation, age_known,
				age_ms, pid_matches, pid_matches ? MyProcPid : 0)));
	return acquired;
}

bool
cluster_cf_lock(LOCKMODE mode)
{
	return cf_lock_observed(mode, false, NULL, 0);
}

bool
cluster_cf_lock_deadline_v1(LOCKMODE mode, uint64 deadline_ns)
{
	return cf_lock_observed(mode, false, NULL, deadline_ns);
}

bool
cluster_cf_lock_poll(LOCKMODE mode)
{
	if (!cluster_shared_config || (MyBackendType != B_LMON && MyBackendType != B_LMS))
		return false;
	return cf_lock_observed(mode, true, NULL, 0);
}

bool
cluster_cf_acquire_pending(LOCKMODE mode)
{
	CfHoldState *slot = cf_slot(mode);

	return slot->held && slot->caller == NULL && slot->owner.cooperative_acquire
		   && slot->owner.state == CLUSTER_LOCK_OWNER_ACQUIRING;
}

bool
cluster_cf_lock_poll_owned(LOCKMODE mode, const void *caller)
{
	if (caller == NULL || !cluster_shared_config
		|| (MyBackendType != B_LMON && MyBackendType != B_LMS))
		return false;
	return cf_lock_observed(mode, true, caller, 0);
}

bool
cluster_cf_acquire_pending_owned(LOCKMODE mode, const void *caller)
{
	CfHoldState *slot = cf_slot(mode);

	return caller != NULL && slot->held && slot->caller == caller && slot->owner.cooperative_acquire
		   && slot->owner.state == CLUSTER_LOCK_OWNER_ACQUIRING;
}

bool
cluster_cf_held_by(LOCKMODE mode, const void *caller)
{
	CfHoldState *slot = cf_slot(mode);

	return caller != NULL && slot->held && slot->caller == caller;
}

ClusterCfReleaseResult
cluster_cf_unlock_owned(LOCKMODE mode, const void *caller)
{
	return caller != NULL ? cf_unlock(mode, caller) : CLUSTER_CF_RELEASE_UNCONFIRMED;
}

uint64
cluster_cf_owner_cookie(LOCKMODE mode)
{
	CfHoldState *slot = cf_slot(mode);

	return slot->held && slot->coordinated && slot->owner.shared
			   ? slot->owner.request.control_owner_id
			   : 0;
}

bool
cluster_cf_release_completed(LOCKMODE mode, uint64 cookie)
{
	Assert(mode == ShareLock || mode == ExclusiveLock);
	return cookie != 0 && cookie == (mode == ShareLock ? cf_retired_s : cf_retired_x);
}

bool
cluster_cf_retirement_snapshot(LOCKMODE mode, const void *caller, ClusterCfRetirementSnapshot *out)
{
	CfHoldState *slot;
	const ClusterLockOwner *owner;
	const ClusterLockAcquireRequest *request;
	ClusterControlRequestView view;
	ClusterResId resid;
	uint64 epoch, generation;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if ((mode != ShareLock && mode != ExclusiveLock) || !cluster_shared_config || MyProc == NULL
		|| MyProcPid <= 0)
		return false;
	slot = cf_slot(mode);
	owner = &slot->owner;
	request = &owner->request;
	epoch = cluster_epoch_get_current();
	generation = cluster_grd_redeclare_generation();
	cluster_cf_resid_encode(&resid);
	if (!slot->held || !slot->coordinated || !slot->release_pending || slot->caller != caller
		|| !owner->shared || owner->state != CLUSTER_LOCK_OWNER_RETIRING
		|| owner->cooperative_acquire || owner->conversion != CLUSTER_LOCK_OWNER_NO_CONVERSION
		|| owner->secondary.generation != 0 || owner->attempt.generation == 0 || epoch == 0
		|| generation == 0 || owner->generation != generation || request->control_owner_id == 0
		|| request->request_id == 0 || request->holder.request_id != request->request_id
		|| request->holder.node_id != (uint32)cluster_node_id
		|| request->holder.procno != (uint32)MyProc->pgprocno
		|| request->holder.cluster_epoch != epoch || request->lockmode != mode
		|| memcmp(&request->resid, &resid, sizeof(resid)) != 0
		|| !cluster_control_request_snapshot(&owner->attempt, &view)
		|| (view.state != CLUSTER_CONTROL_REQUEST_ABANDONED
			&& view.state != CLUSTER_CONTROL_REQUEST_TERMINAL)
		|| view.owner_exited || view.owner.incarnation == 0 || view.owner.pid != MyProcPid
		|| view.owner.procno != (uint32)MyProc->pgprocno || view.mode != mode
		|| memcmp(&view.message.key.resid, &resid, sizeof(resid)) != 0
		|| memcmp(&view.message.key.holder, &request->holder, sizeof(request->holder)) != 0
		|| cluster_epoch_get_current() != epoch || cluster_grd_redeclare_generation() != generation)
		return false;
	/* This handle came from the embedded request's original registration,
	 * not from a caller-supplied lookup by PID or resource. Retain its exact
	 * creator incarnation when comparing subsequent observations. */
	out->cookie = request->control_owner_id;
	out->registry = view;
	return true;
}

static void
cf_note_retired(LOCKMODE mode, uint64 cookie)
{
	if (cookie != 0) {
		if (mode == ShareLock)
			cf_retired_s = cookie;
		else
			cf_retired_x = cookie;
	}
}

/*
 * cluster_cf_unlock -- release a previously-held CF lock in `mode`.
 */
void
cluster_cf_unlock(LOCKMODE mode)
{
	/* Legacy callers cannot consume a verdict, but must retain cleanup duty. */
	(void)cluster_cf_unlock_confirmed(mode);
}

bool
cluster_cf_held_is_usable(LOCKMODE mode)
{
	CfHoldState *slot = cf_slot(mode);

	return slot->held && !slot->release_pending
		   && (!slot->coordinated || cluster_lock_owner_is_usable(&slot->owner));
}

bool
cluster_cf_held_is_clusterwide(LOCKMODE mode)
{
	CfHoldState *slot = cf_slot(mode);

	return cluster_cf_held_is_usable(mode) && slot->coordinated;
}

ClusterCfReleaseResult
cluster_cf_unlock_confirmed(LOCKMODE mode)
{
	return cf_unlock(mode, NULL);
}

static ClusterCfReleaseResult
cf_unlock(LOCKMODE mode, const void *caller)
{
	CfHoldState *slot = cf_slot(mode);
	uint64 cookie = cluster_cf_owner_cookie(mode);

	if (!slot->held)
		return CLUSTER_CF_RELEASE_NOT_HELD;
	if (slot->caller != caller)
		return CLUSTER_CF_RELEASE_UNCONFIRMED;
	if (!slot->coordinated) {
		memset(slot, 0, sizeof(*slot));
		return CLUSTER_CF_RELEASE_NOT_HELD;
	}

	/* S6 may remove the remote holder then lose its reply, or unwind on error.
	 * Neither case permits another read/write under this old request. Keep
	 * the exact identity visible to retirement, lock-order and stop checks. */
	slot->release_pending = true;
	if (!cluster_lock_owner_release(&slot->owner))
		return CLUSTER_CF_RELEASE_UNCONFIRMED;

	cf_note_retired(mode, cookie);
	memset(slot, 0, sizeof(*slot));
	return CLUSTER_CF_RELEASE_CONFIRMED;
}

/* PGRAC: complete only an explicitly abandoned/releasing private slot.
 * Service loops never wait for their own CONTROL work; a still-usable hold
 * or an in-flight acquisition must remain with its caller.
 * Author: SqlRush <sqlrush@gmail.com> */
void
cluster_cf_retirement_poll(void)
{
	CfHoldState *slots[2] = { &cf_hold_s, &cf_hold_x };
	unsigned i;

	if (!cluster_shared_config)
		return;
	for (i = 0; i < lengthof(slots); ++i) {
		CfHoldState *slot = slots[i];
		uint64 cookie = slot->owner.request.control_owner_id;

		if (slot->held && slot->coordinated && slot->release_pending
			&& slot->owner.state == CLUSTER_LOCK_OWNER_RETIRING
			&& cluster_lock_owner_release_poll(&slot->owner)) {
			cf_note_retired(i == 0 ? ShareLock : ExclusiveLock, cookie);
			memset(slot, 0, sizeof(*slot));
		}
	}
}

/*
 * cluster_cf_held -- does this backend retain ownership/cleanup in `mode`?
 */
bool
cluster_cf_held(LOCKMODE mode)
{
	return cf_slot(mode)->held;
}

/*
 * cluster_cf_set_bootstrap_authority -- mark/clear the bootstrap window.
 */
void
cluster_cf_set_bootstrap_authority(bool on)
{
	cf_bootstrap_authority = on;
}

/*
 * cluster_cf_write_permitted -- is a shared-authority control-file write
 * currently allowed (held CF X, Startup owner, or EOR owner)?
 */
bool
cluster_cf_write_permitted(void)
{
	return cluster_cf_held_is_usable(ExclusiveLock) || cf_bootstrap_authority
		   || cf_owner_eor_authority;
}

/*
 * cluster_cf_in_bootstrap_window -- is this process the bootstrap authority?
 */
bool
cluster_cf_in_bootstrap_window(void)
{
	return cf_bootstrap_authority;
}

/*
 * cluster_cf_exactly_one_declared_node
 *
 * RF-B must use the same exact-one authority fact in Startup and in the EOR
 * checkpointer.  A normal cluster boot has already populated ClusterConfShmem.
 * The native shared-catalog seed deliberately runs with cluster.enabled=off,
 * so spec-2.1 forbids cluster_conf_load() and the shmem count remains zero.
 * In that one case, inspect only the postmaster-static node declarations and
 * fail closed on a missing, malformed, duplicate, or foreign node section.
 */
bool
cluster_cf_exactly_one_declared_node(void)
{
	const char *path;
	FILE *f;
	char line[1024];
	int loaded_count;
	int declared_count = 0;
	bool valid = true;

	loaded_count = cluster_conf_node_count();
	if (loaded_count != 0)
		return loaded_count == 1;

	path = (cluster_config_file != NULL && cluster_config_file[0] != '\0') ? cluster_config_file
																		   : "pgrac.conf";
	f = AllocateFile(path, "r");
	if (f == NULL)
		return false;

	while (fgets(line, sizeof(line), f) != NULL) {
		char *p = line;
		char *endptr;
		long node_id;

		while (*p != '\0' && isspace((unsigned char)*p))
			p++;
		if (strncmp(p, "[node.", 6) != 0)
			continue;

		errno = 0;
		node_id = strtol(p + 6, &endptr, 10);
		if (errno != 0 || endptr == p + 6 || *endptr != ']' || node_id < 0
			|| node_id >= CLUSTER_MAX_NODES || node_id != cluster_node_id) {
			valid = false;
			break;
		}
		endptr++;
		while (*endptr != '\0' && isspace((unsigned char)*endptr))
			endptr++;
		if (*endptr != '\0' && *endptr != '#' && *endptr != ';') {
			valid = false;
			break;
		}

		declared_count++;
		if (declared_count > 1) {
			valid = false;
			break;
		}
	}
	if (ferror(f))
		valid = false;
	if (FreeFile(f) != 0)
		valid = false;

	return valid && declared_count == 1;
}

/*
 * cluster_cf_owner_eor_install -- transport the exact one-node OWNER decision.
 */
bool
cluster_cf_owner_eor_install(void)
{
	if (!cluster_controlfile_shared_authority || !cluster_cf_exactly_one_declared_node()
		|| cluster_cf_join_readonly() || cf_bootstrap_authority)
		return false;
	if (!cluster_cf_owner_eor_phase_install())
		return false;

	cf_bootstrap_authority = true;
	return true;
}

/*
 * cluster_cf_owner_eor_consume -- grant this EOR checkpointer local permission
 * only after all fresh gates and the exact INSTALLED -> ACTIVE transition.
 */
bool
cluster_cf_owner_eor_consume(bool end_of_recovery, bool identity_ok)
{
	if (!end_of_recovery || !identity_ok || !cluster_controlfile_shared_authority
		|| !cluster_cf_exactly_one_declared_node() || cluster_cf_join_readonly()
		|| cf_owner_eor_authority)
		return false;
	if (!cluster_cf_owner_eor_phase_activate())
		return false;

	cf_owner_eor_authority = true;
	return true;
}

bool
cluster_cf_owner_eor_local_active(void)
{
	return cf_owner_eor_authority;
}

bool
cluster_cf_owner_eor_complete(void)
{
	if (!cf_owner_eor_authority || !cluster_cf_owner_eor_phase_done())
		return false;

	cf_owner_eor_authority = false;
	return true;
}

void
cluster_cf_owner_eor_abort(void)
{
	/* No I/O, logging or shared transition: ACTIVE deliberately survives. */
	cf_owner_eor_authority = false;
}

bool
cluster_cf_owner_eor_close(void)
{
	if (!cf_bootstrap_authority)
		return cluster_cf_owner_eor_phase_read() == CLUSTER_CF_OWNER_EOR_EMPTY;
	if (!cluster_cf_owner_eor_phase_clear())
		return false;

	cf_bootstrap_authority = false;
	return true;
}

/*
 * cluster_cf_set_join_readonly -- mark/clear this NODE as a join-read-only
 * bring-up node (cross-process via the CF shmem flag).
 */
void
cluster_cf_set_join_readonly(bool on)
{
	cluster_cf_stats_set_join_readonly(on);
}

/*
 * cluster_cf_join_readonly -- is this node attaching read-only to a peer-owned
 * authority (recovery writes must be skipped, not applied)?  Cross-process:
 * the checkpointer reads the flag the startup process set.
 */
bool
cluster_cf_join_readonly(void)
{
	return cluster_cf_stats_get_join_readonly();
}

/*
 * cf_write_skip -- PROCESS-LOCAL bring-up authority-write-skip (see header).
 * Distinct from the node-wide shmem join_readonly: the chokepoint consults
 * ONLY this, so a lingering shmem flag cannot silently skip a steady-state
 * write from another path.
 */
static bool cf_write_skip = false;

ClusterNormalStopPollResult
cluster_cf_normal_stop_poll(bool post_checkpoint, const char **reason)
{
	ClusterNormalStopPollResult result = CLUSTER_NORMAL_STOP_INVALID, shared;
	const char *why = "CF_OWNER_INVALID", *shared_reason;

	if (!IsUnderPostmaster || (!AmCheckpointerProcess() && MyBackendType != B_LMON))
		goto done;
	/* Observe this process's original holds, not another process's zeroed
	 * copy. GRD separately verifies the registered holders. An unconfirmed
	 * S6 remains owned; the observer does not attempt its release. */
	shared = cluster_cf_normal_stop_shared_poll(post_checkpoint, &shared_reason);
	if (shared != CLUSTER_NORMAL_STOP_READY && shared != CLUSTER_NORMAL_STOP_PENDING) {
		why = shared_reason;
		goto done;
	}
	if (cf_bootstrap_authority || cf_owner_eor_authority) {
		why = "CF_RECOVERY_PERMISSION_ACTIVE";
		goto done;
	}
	/* A prior EOR may leave a process-local skip hint. The original next
	 * normal checkpoint sets it false before taking CF X; requiring that
	 * change before the checkpoint itself would deadlock the first stop. */
	if (cf_write_skip && (post_checkpoint || !AmCheckpointerProcess())) {
		why = "CF_POST_CHECKPOINT_WRITE_SKIP";
		goto done;
	}
	if (cf_hold_x.held || cf_hold_s.held) {
		why = cf_hold_x.held ? "CF_LOCAL_X_HELD" : "CF_LOCAL_S_HELD";
		result = CLUSTER_NORMAL_STOP_PENDING;
	} else {
		why = shared_reason;
		result = shared;
	}
done:
	if (reason)
		*reason = why;
	return result;
}

void
cluster_cf_set_write_skip(bool on)
{
	cf_write_skip = on;
}

bool
cluster_cf_write_skip(void)
{
	return cf_write_skip;
}
