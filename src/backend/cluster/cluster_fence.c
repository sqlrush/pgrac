/*-------------------------------------------------------------------------
 *
 * cluster_fence.c
 *	  pgrac Fence-lite — spec-2.28 Sprint A Step 1.
 *
 *	  Internal-only fence-lite skeleton.  ProcSignal-driven freeze/thaw
 *	  broadcast for in-flight transaction abort + postmaster orderly
 *	  self-shutdown on persistent quorum loss.  Consumes ClusterQvotec
 *	  Shmem.quorum_state (spec-2.6) via LMON tick.
 *
 *	  Step 1 scope (this commit):
 *	    - ClusterFenceShmem private 128-byte (2 cache-line) region with
 *	      LWLock for multi-writer coordination
 *	    - ClusterFenceFreezePending volatile sig_atomic_t per-backend
 *	      flag (defined here, set by cluster_signal.c handler in Step 2)
 *	    - shmem_size / shmem_init / shmem_register
 *	    - broadcast_freeze / broadcast_thaw / self_request stubs that
 *	      update ClusterFenceShmem fields but do NOT iterate ProcArray /
 *	      send signals (Step 3 D5 wires real broadcast)
 *	    - cluster_fence_check_interrupts no-op stub (Step 2 D4 wires
 *	      ereport ERRCODE_CLUSTER_QUORUM_LOST_BACKEND in postgres.c)
 *	    - cluster_fence_postmaster_check no-op stub (Step 3 D6 wires
 *	      kill(MyProcPid, SIGINT) per v0.3 F4 amend)
 *	    - cluster_get_fence_state SRF callback skeleton (Step 4 D10
 *	      wires into pg_proc.dat;Step 1 returns ERRCODE_FEATURE_NOT
 *	      _SUPPORTED)
 *
 *	  Step 1 explicitly DEFERS:
 *	    - Real ProcSignal handler bodies in cluster_signal.c — Step 2 D3
 *	    - postgres.c ProcessInterrupts hook wiring — Step 2 D4
 *	    - cluster_lmon.c quorum_state poll + broadcast helper — Step 3 D5
 *	    - postmaster.c kill self SIGINT trigger — Step 3 D6
 *	    - 1 SQLSTATE 53R50 / 1 wait event / view / 4 atomic counters /
 *	      3 inject points — Step 4
 *	    - 098 TAP fence_freeze_writes_2node + 096 L4-L5 unblock — Step 5
 *
 *	  Three user-imposed invariants (spec §3.0) are enforced by
 *	  later steps;Step 1 is just shmem region + API surface.  See
 *	  cluster_fence.h banner for full context.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_fence.c
 *
 * NOTES
 *	  pgrac-original file.  Compiled only in --enable-cluster builds;
 *	  see src/backend/cluster/Makefile for OBJS rules.
 *
 *	  Spec authority:  pgrac:specs/spec-2.28-fence-lite-self-fence-
 *	  procsignal-freeze-thaw.md (frozen v0.3 2026-05-10).
 *
 *	  Sprint A Step 1 prerequisite (Invariant I3):  linkdb tag
 *	  v0.14.2-stage2.6 nightly run 25618433189 ✓ (Hardening v0.6
 *	  F1 quorum_state post-write recompute + F2 fast-restart ghost-
 *	  detect dual-layer).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_fence.h"

#ifdef USE_PGRAC_CLUSTER

#include <string.h>

#include "access/xact.h" /* IsTransactionState (spec-2.28 Step 2 D4 hook) */
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/timestamp.h"
#include "funcapi.h"

#include "cluster/cluster_guc.h"   /* cluster_enabled */
#include "cluster/cluster_shmem.h" /* cluster_shmem_register_region */


/* ============================================================
 * Per-backend volatile flag.
 *
 *	Set by cluster_signal.c handler for PROCSIG_CLUSTER_FREEZE_WRITES
 *	(Step 2 D3) using async-signal-safe write.  Read-and-cleared by
 *	cluster_fence_check_interrupts (Step 2 D4) when called from
 *	postgres.c ProcessInterrupts.  Storage class is sig_atomic_t per
 *	POSIX signal-safe write requirement.
 * ============================================================ */
volatile sig_atomic_t ClusterFenceFreezePending = 0;


/* ============================================================
 * ClusterFenceShmem — private 128-byte (2 cache-line) region.
 *
 *	Layout (offset / size / field):
 *	   0..63  LWLock (sized to one cache line on most platforms;
 *	                  shmem.c PG-native; protects all fields below)
 *	  64..71  uint64 last_freeze_at_us         (TimestampTz, 0 = never)
 *	  72..79  uint64 last_thaw_at_us           (0 = never)
 *	  80..87  uint64 self_fence_requested_at_us (0 = no pending request)
 *	  88..95  uint64 freeze_event_count
 *	  96..103 uint64 thaw_event_count
 *	 104..111 uint64 self_fence_initiated_count
 *	 112..127 uint8[16] _reserved
 *
 *	Note:  freeze_signal_received_count is NOT in this region —
 *	per spec D11, that counter is per-backend pg_atomic in the
 *	cluster_pgstat shmem region (cluster.fence.freeze_signal_received_
 *	count) so cross-process aggregation works via the existing mirror
 *	sync framework.  This avoids spec-2.6 backlog #4 limitation.
 *
 *	The LWLock is sized via PG's standard LWLockPadded mechanism;
 *	this struct uses a direct LWLock member (one tranche slot) and
 *	relies on natural alignment.  StaticAssertDecl pins total size.
 * ============================================================ */
typedef struct ClusterFenceShmemStruct {
	LWLock lock;							/* protects all fields below */
	TimestampTz last_freeze_at_us;			/* 0 = never */
	TimestampTz last_thaw_at_us;			/* 0 = never */
	TimestampTz self_fence_requested_at_us; /* 0 = no pending */
	uint64 freeze_event_count;				/* lifetime broadcasts */
	uint64 thaw_event_count;
	uint64 self_fence_initiated_count; /* postmaster-only writer */
	uint8 _reserved[16];
} ClusterFenceShmemStruct;

static ClusterFenceShmemStruct *ClusterFenceShmem = NULL;

/* The LWLock alone is platform-variable in size (LWLOCK_PADDED_SIZE
 * is 32 or 64 depending on cache line + atomics support).  Don't
 * StaticAssertDecl an exact byte count of the whole struct — the
 * size_fn computes from sizeof at runtime;cache-line layout is
 * conceptual not byte-exact. */


/* ============================================================
 * shmem hookup.
 * ============================================================ */

Size
cluster_fence_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterFenceShmemStruct));
}

void
cluster_fence_shmem_init(void)
{
	bool found;

	ClusterFenceShmem = (ClusterFenceShmemStruct *)ShmemInitStruct(
		"pgrac cluster fence", cluster_fence_shmem_size(), &found);

	if (!found) {
		LWLockInitialize(&ClusterFenceShmem->lock, LWTRANCHE_CLUSTER_FENCE);
		ClusterFenceShmem->last_freeze_at_us = 0;
		ClusterFenceShmem->last_thaw_at_us = 0;
		ClusterFenceShmem->self_fence_requested_at_us = 0;
		ClusterFenceShmem->freeze_event_count = 0;
		ClusterFenceShmem->thaw_event_count = 0;
		ClusterFenceShmem->self_fence_initiated_count = 0;
		memset(ClusterFenceShmem->_reserved, 0, sizeof(ClusterFenceShmem->_reserved));
	}
}

static const ClusterShmemRegion cluster_fence_region = {
	.name = "pgrac cluster fence",
	.size_fn = cluster_fence_shmem_size,
	.init_fn = cluster_fence_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_fence",
	.reserved_flags = 0,
};

void
cluster_fence_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_fence_region);
}


/* ============================================================
 * Step 1 stubs — broadcast / self_request / check_interrupts /
 * postmaster_check.  Real bodies land Step 2-3 per spec §1.2.
 *
 *	The shmem fields ARE updated by these stubs so cluster_unit
 *	T-fence-1 can verify the API surface compiles + links cleanly.
 *	Real ProcArray loop / SendProcSignal / kill(SIGINT) wiring is
 *	in cluster_lmon.c (Step 3 D5) and postmaster.c (Step 3 D6).
 * ============================================================ */

void
cluster_fence_broadcast_freeze(const char *reason, uint64 scn)
{
	if (!cluster_enabled)
		return; /* L19/L20 silent skip */
	if (!cluster_freeze_writes_enabled)
		return; /* §3.6.1 GUC kill switch */
	if (ClusterFenceShmem == NULL)
		return; /* shmem not yet initialised — pre-RUNNING */

	(void)reason;
	(void)scn;

	LWLockAcquire(&ClusterFenceShmem->lock, LW_EXCLUSIVE);
	ClusterFenceShmem->last_freeze_at_us = GetCurrentTimestamp();
	ClusterFenceShmem->freeze_event_count++;
	/* Step 3 D5 will SendProcSignal loop here. */
	LWLockRelease(&ClusterFenceShmem->lock);
}

void
cluster_fence_broadcast_thaw(const char *reason, uint64 scn)
{
	if (!cluster_enabled)
		return;
	if (!cluster_freeze_writes_enabled)
		return;
	if (ClusterFenceShmem == NULL)
		return;

	(void)reason;
	(void)scn;

	LWLockAcquire(&ClusterFenceShmem->lock, LW_EXCLUSIVE);
	ClusterFenceShmem->last_thaw_at_us = GetCurrentTimestamp();
	ClusterFenceShmem->thaw_event_count++;
	/* Per Invariant I2:  thaw is observation-only — DO NOT clear
	 * ClusterFenceFreezePending here, DO NOT change the commit gate
	 * predicate.  Step 3 D5 also wires the cancel-pending-self-fence
	 * effect:  self_fence_requested_at_us = 0. */
	ClusterFenceShmem->self_fence_requested_at_us = 0;
	LWLockRelease(&ClusterFenceShmem->lock);
}

void
cluster_fence_self_request(const char *reason, uint64 scn)
{
	if (!cluster_enabled)
		return;
	if (!cluster_self_fence_enabled)
		return; /* §3.6.1 GUC kill switch */
	if (ClusterFenceShmem == NULL)
		return;

	(void)reason;
	(void)scn;

	LWLockAcquire(&ClusterFenceShmem->lock, LW_EXCLUSIVE);
	if (ClusterFenceShmem->self_fence_requested_at_us == 0)
		ClusterFenceShmem->self_fence_requested_at_us = GetCurrentTimestamp();
	/* Idempotent:  multiple calls within grace_ms window keep the
	 * earliest timestamp so postmaster_check measures from first
	 * request, not most recent. */
	LWLockRelease(&ClusterFenceShmem->lock);
}

void
cluster_fence_check_interrupts(void)
{
	if (!cluster_enabled)
		return; /* L19/L20:  cluster.enabled silent skip first line */

	/* Per v0.3 F1 amend:  no manual CS guard — PG ProcessInterrupts
	 * already returned early when CritSectionCount > 0 (postgres.c:
	 * 3226-3227), so we cannot be inside a CS here. */

	/*
	 * Per spec-2.28 §3.7 C4:  read-clear-then-decide.
	 *
	 * 1. Cheap pre-check returns immediately for the common no-pending
	 *    path (avoids the cost of writing a sig_atomic_t in the hot
	 *    interrupt-checking loop).
	 * 2. Clear the flag BEFORE the enabled-check + IsTransactionState
	 *    check.  If we leave the flag set when GUC is disabled or we
	 *    are idle, a later GUC re-enable + new transaction would see
	 *    a stale freeze bit and ereport against a quorum loss that
	 *    has long since recovered.  Clear-first is the same idiom PG
	 *    uses for QueryCancelPending (postgres.c:3047-3070).
	 * 3. After clearing, decide whether to actually ereport based on
	 *    the GUC + transaction state.
	 */
	if (ClusterFenceFreezePending == 0)
		return;

	ClusterFenceFreezePending = 0;

	if (!cluster_freeze_writes_enabled)
		return; /* dev/debug GUC absorb */

	if (!IsTransactionState())
		return; /* idle backend absorb — commit gate handles next BEGIN */

	ereport(ERROR, (errcode(ERRCODE_CLUSTER_QUORUM_LOST_BACKEND),
					errmsg("transaction aborted: cluster quorum lost in flight"),
					errhint("the cluster lost majority quorum during your "
							"transaction;all uncommitted writes have been "
							"rolled back;auto-retry will resume after quorum "
							"recovers (typically O(seconds))")));
}

void
cluster_fence_postmaster_check(void)
{
	if (IsUnderPostmaster)
		return; /* CLAUDE.md rule 16 §postmaster-once / L91 EXEC_BACKEND */
	if (!cluster_enabled)
		return;
	if (!cluster_self_fence_enabled)
		return;
	if (ClusterFenceShmem == NULL)
		return;

	/* Step 3 D6 will:
	 *   read self_fence_requested_at_us
	 *   if non-zero AND now - requested >= grace_ms × 1000:
	 *     ereport(LOG, "self-fence on persistent quorum loss")
	 *     inc self_fence_initiated_count
	 *     kill(MyProcPid, SIGINT)   <- v0.3 F4 amend (no pmdie() callable)
	 *
	 * Step 1 no-op stub. */
}


/* ============================================================
 * SRF callback for pg_cluster_fence_state view (Step 4 D10).
 *
 *	Step 1 returns FEATURE_NOT_SUPPORTED — view is not yet wired in
 *	pg_proc.dat, so this function is unreachable from SQL.  When
 *	Step 4 wires the view, this body implements the 8-column single-
 *	row return.
 * ============================================================ */
Datum
cluster_get_fence_state(PG_FUNCTION_ARGS)
{
	(void)fcinfo;
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("pg_cluster_fence_state not yet implemented "
						   "(spec-2.28 Sprint A Step 4 D10)")));
	PG_RETURN_NULL();
}

#endif /* USE_PGRAC_CLUSTER */
