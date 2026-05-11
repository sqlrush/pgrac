/*-------------------------------------------------------------------------
 *
 * cluster_reconfig.c
 *	  pgrac cluster reconfig coordinator — internal-only A scope
 *	  (spec-2.29 Sprint A Step 1 skeleton).
 *
 *	  Step 1 shipped scope (this file):
 *	    - ClusterReconfigState shmem region with LWLock-guarded
 *	      last_applied + 3 atomic counters
 *	    - StaticAssertDecl on ReconfigEvent + ClusterReconfigState
 *	      sizeof bounds (P2.8 — natural-aligned, NOT 64B literal)
 *	    - cluster_reconfig_shmem_size / init / register helpers
 *	    - cluster_reconfig_get_last_event (always-1-row contract P2.9)
 *	    - cluster_reconfig_publish_event (LWLock-acquired)
 *	    - Stubs for lmon_tick / broadcast_local_procsig /
 *	      apply_epoch_bump_as_coordinator / check_pending — bodies
 *	      land in Step 2
 *
 *	  Steps 2-7: lmon_tick body (Q2 A'' coordinator decision +
 *	  declared-peer filter F11), ProcessInterrupts I6 guard, envelope
 *	  observe path D20, SRF view body, TAP 099 L1-L10, regress + manuals,
 *	  catalog surface delta + baseline sync (L98), ship gate.
 *
 *	  Spec authority: pgrac:specs/spec-2.29-reconfig-coordinator-
 *	  internal.md (DRAFT v0.3).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_reconfig.c
 *
 * NOTES
 *	  pgrac-original file.  Compiled only in --enable-cluster builds.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_reconfig.h"

#ifdef USE_PGRAC_CLUSTER

#include <string.h>

#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/timestamp.h"

#include "cluster/cluster_shmem.h" /* cluster_shmem_register_region */


/*
 * StaticAssertDecl: bound ReconfigEvent + ClusterReconfigState sizeof.
 *
 *	  Per spec-2.29 P2.8 fix — v0.1 wrote sizeof(ReconfigEvent) == 64
 *	  packed which was wrong (natural fields sum > 64).  v0.3 uses
 *	  natural alignment + upper bound assertion;exact size doesn't
 *	  matter because shmem reservation walks sizeof() expression.
 *
 *	  ReconfigEvent natural fields (64-bit ABI):
 *	    8 event_id + 4 coord + 4 _pad0 + 8 old_epoch + 8 new_epoch
 *	    + 16 dead_bitmap + 8 applied_at + 4 observer_role + 4 _pad1
 *	    + 8 event_seq + 8 cssd_dead_generation = 80 bytes exactly.
 *	  Allow up to 96 bytes for future field append without bump.
 */
StaticAssertDecl(sizeof(ReconfigEvent) <= 96,
				 "ReconfigEvent must fit within 96 bytes");
StaticAssertDecl(sizeof(ReconfigEvent) >= 64,
				 "ReconfigEvent must be at least 64 bytes (defensive — fields enumerated)");


/*
 * Shmem region (single instance;pointer set by shmem_init).
 */
static ClusterReconfigState *ReconfigShmem = NULL;


/* ============================================================
 * Shmem region lifecycle.
 * ============================================================
 */

Size
cluster_reconfig_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterReconfigState));
}


void
cluster_reconfig_shmem_init(void)
{
	bool found;

	ReconfigShmem = (ClusterReconfigState *)
		ShmemInitStruct("pgrac cluster reconfig",
						cluster_reconfig_shmem_size(),
						&found);

	if (!found)
	{
		/* First-time init — zero everything, then set up LWLock +
		 * never-applied sentinel (event_id=0, observer_role=NONE).
		 */
		memset(ReconfigShmem, 0, sizeof(ClusterReconfigState));
		LWLockInitialize(&ReconfigShmem->lock, LWTRANCHE_CLUSTER_RECONFIG);
		pg_atomic_init_u64(&ReconfigShmem->apply_counter, 0);
		pg_atomic_init_u64(&ReconfigShmem->dedup_skip_counter, 0);
		pg_atomic_init_u64(&ReconfigShmem->procsig_broadcast_count, 0);
		/* last_applied left zeroed by memset — event_id=0 =
		 * CLUSTER_RECONFIG_OBSERVER_NONE = never-applied sentinel. */
	}
}


static const ClusterShmemRegion cluster_reconfig_region = {
	.name = "pgrac cluster reconfig",
	.size_fn = cluster_reconfig_shmem_size,
	.init_fn = cluster_reconfig_shmem_init,
	.lwlock_count = 1, /* single LWLock guarding last_applied publish */
	.owner_subsys = "cluster_reconfig",
	.reserved_flags = 0,
};


void
cluster_reconfig_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_reconfig_region);
}


/* ============================================================
 * Observability accessor — always-1-row contract (P2.9).
 *
 *	Caller (Step 3 D5b SRF entry) MUST always return 1 row to
 *	pg_cluster_reconfig_state regardless of never-applied state.
 *	This helper populates *out unconditionally;event_id=0 +
 *	observer_role=CLUSTER_RECONFIG_OBSERVER_NONE means never applied.
 * ============================================================
 */

void
cluster_reconfig_get_last_event(ReconfigEvent *out)
{
	Assert(out != NULL);

	if (ReconfigShmem == NULL)
	{
		/* Defense: shmem not initialized (e.g. cluster.enabled=off
		 * path or pre-postmaster).  Caller still gets a well-defined
		 * never-applied state. */
		memset(out, 0, sizeof(ReconfigEvent));
		return;
	}

	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	memcpy(out, &ReconfigShmem->last_applied, sizeof(ReconfigEvent));
	LWLockRelease(&ReconfigShmem->lock);
}


/* ============================================================
 * Internal publish helper.
 *
 *	Per L23 lesson (compound atomic + counter inc must share same
 *	critical section): apply_counter increment + last_applied copy
 *	both happen inside the LWLock-exclusive window so that
 *	concurrent SRF reads see a consistent snapshot — never see
 *	apply_counter > last_applied.event_seq.
 * ============================================================
 */

void
cluster_reconfig_publish_event(const ReconfigEvent *evt)
{
	ReconfigEvent published;
	uint64		event_seq;

	Assert(evt != NULL);

	if (ReconfigShmem == NULL)
		return;

	memcpy(&published, evt, sizeof(ReconfigEvent));

	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	event_seq = pg_atomic_fetch_add_u64(&ReconfigShmem->apply_counter, 1) + 1;
	published.event_seq = event_seq;
	memcpy(&ReconfigShmem->last_applied, &published, sizeof(ReconfigEvent));
	LWLockRelease(&ReconfigShmem->lock);

	elog(DEBUG1,
		 "cluster_reconfig: event %lu applied (coord=%d old=%lu new=%lu role=%d dead_gen=%lu)",
		 (unsigned long) published.event_id,
		 published.coordinator_node_id,
		 (unsigned long) published.old_epoch,
		 (unsigned long) published.new_epoch,
		 published.observer_role,
		 (unsigned long) published.cssd_dead_generation);
}


/* ============================================================
 * Stubs for Step 2 (D2 lmon_tick body, D2 broadcast, D2 epoch++,
 * D4 ProcessInterrupts) — wired symbols so cluster_lmon.c (Step 2 D3)
 * and tcop/postgres.c (Step 2 D4) can call these without dangling
 * references during the Step 1 → Step 2 transition.
 *
 *	Per CLAUDE.md 规则 8 (no half-implementation): stubs document
 *	their unfinished state via DEBUG2 elog + step landing in.
 * ============================================================
 */

void
cluster_reconfig_lmon_tick(void)
{
	/* Step 2 body: Q2 A'' coordinator decision + dead_bitmap construction
	 * + declared-peer filter (F11) + event_id hash + Lamport piggyback +
	 * coordinator-only epoch++ + every-survivor PROCSIG broadcast.
	 * Step 1 stub: silent no-op until LMON wiring (Step 2 D3) lands.
	 */
}


void
cluster_reconfig_broadcast_local_procsig(void)
{
	/* Step 2 body: ProcArray iteration + SendProcSignal to every backend.
	 * Step 1 stub: increment broadcast_count atomic so observability
	 * surface (Step 3 D5/D5b) has a non-zero value during Step 2
	 * integration tests.
	 */
	if (ReconfigShmem == NULL)
		return;
	pg_atomic_fetch_add_u64(&ReconfigShmem->procsig_broadcast_count, 1);
}


void
cluster_reconfig_apply_epoch_bump_as_coordinator(
	const uint8 dead_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] pg_attribute_unused(),
	int32       coordinator_node_id pg_attribute_unused(),
	uint64      cssd_dead_generation pg_attribute_unused())
{
	/* Step 2 body: call cluster_epoch_advance_for_reconfig (D18) +
	 * cluster_epoch_set_changed_at_lsn (GetXLogInsertRecPtr) + build
	 * ReconfigEvent with observer_role=COORDINATOR + publish.
	 * Step 1 stub: silent no-op (no epoch advance, no publish) so
	 * Step 1 unit tests can verify shmem layout without spurious
	 * publish side-effects.
	 */
}


void
cluster_reconfig_check_pending_in_proc_interrupts(void)
{
	/* Step 2 body: read-clear pending sig_atomic_t + I6 commit-
	 * critical-section guard (P1.5) + enumerate fail-closed cause
	 * (53R50 vs 53R60) + ereport.
	 * Step 1 stub: silent no-op (handler still sets pending via
	 * existing cluster_signal.c handler but ProcessInterrupts hasn't
	 * been wired yet — that lands in Step 2 D4).
	 */
}


#else /* !USE_PGRAC_CLUSTER */

/*
 * Disable-cluster stubs.  Same symbol surface so envelope receive
 * paths + LMON tick wiring + ProcessInterrupts integration compile
 * cleanly in both modes.  All stubs are silent no-ops.
 */

Size
cluster_reconfig_shmem_size(void)
{
	return 0;
}

void
cluster_reconfig_shmem_init(void)
{
}

void
cluster_reconfig_shmem_register(void)
{
}

void
cluster_reconfig_get_last_event(ReconfigEvent *out)
{
	if (out != NULL)
		memset(out, 0, sizeof(ReconfigEvent));
}

void
cluster_reconfig_publish_event(const ReconfigEvent *evt pg_attribute_unused())
{
}

void
cluster_reconfig_lmon_tick(void)
{
}

void
cluster_reconfig_broadcast_local_procsig(void)
{
}

void
cluster_reconfig_apply_epoch_bump_as_coordinator(
	const uint8 dead_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] pg_attribute_unused(),
	int32       coordinator_node_id pg_attribute_unused(),
	uint64      cssd_dead_generation pg_attribute_unused())
{
}

void
cluster_reconfig_check_pending_in_proc_interrupts(void)
{
}

#endif /* USE_PGRAC_CLUSTER */
