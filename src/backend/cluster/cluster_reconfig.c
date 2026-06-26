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

#include "access/transam.h" /* TransactionIdIsValid */
#include "access/xact.h"	/* IsTransactionState (Step 2 D4) */
#include "access/xlog.h"	/* GetXLogInsertRecPtr (Step 2 D2) */
#include "common/hashfn.h"	/* hash_bytes_extended */
#include "fmgr.h"			/* PG_FUNCTION_ARGS (Step 3 D5b SRF) */
#include "funcapi.h"		/* InitMaterializedSRF (Step 3 D5b SRF) */
#include "miscadmin.h"		/* MyProcPid */
#include "storage/lwlock.h"
#include "storage/proc.h"		/* PGPROC */
#include "storage/procsignal.h" /* SendProcSignal + PROCSIG_CLUSTER_RECONFIG_START */
#include "storage/shmem.h"
#include "storage/sinvaladt.h" /* BackendIdGetProc */
#include "utils/builtins.h"	   /* cstring_to_text */
#include "utils/timestamp.h"
#include "utils/wait_event.h" /* WAIT_EVENT_CLUSTER_BGPROC_LMON_RECONFIG_TICK (D9) */

#include "cluster/cluster_conf.h"		   /* cluster_conf_lookup_node */
#include "cluster/cluster_cssd.h"		   /* cluster_cssd_get_peer_state, get_dead_generation */
#include "cluster/cluster_elog.h"		   /* cluster_node_id */
#include "cluster/cluster_epoch.h"		   /* advance + observe + set_changed_at_lsn */
#include "cluster/cluster_gcs_block.h"	   /* spec-2.34 D4 — eager epoch wake hook */
#include "cluster/cluster_sinval.h"		   /* spec-2.39 D14 — RESET-all reconfig hook */
#include "cluster/cluster_tt_status.h"	   /* spec-3.1 D7 — TT status overlay flush hook */
#include "cluster/cluster_guc.h"		   /* cluster_enabled */
#include "cluster/cluster_inject.h"		   /* CLUSTER_INJECTION_POINT */
#include "cluster/cluster_write_fence.h"   /* spec-4.12 D4 — durable fence marker submit */
#include "cluster/cluster_qvotec.h"		   /* cluster_qvotec_in_quorum */
#include "cluster/cluster_shmem.h"		   /* cluster_shmem_register_region */
#include "cluster/cluster_signal.h"		   /* cluster_reconfig_start_pending */
#include "cluster/cluster_touched_peers.h" /* spec-5.14 D4 — touched ∩ dead dispatch */


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
StaticAssertDecl(sizeof(ReconfigEvent) <= 96, "ReconfigEvent must fit within 96 bytes");
StaticAssertDecl(sizeof(ReconfigEvent) >= 64,
				 "ReconfigEvent must be at least 64 bytes (defensive — fields enumerated)");

/* spec-5.14 D6 — the per-kind counter array must cover every touch class. */
StaticAssertDecl(CLUSTER_RECONFIG_TOUCH_KIND_COUNT == CLUSTER_TOUCH_KIND_COUNT,
				 "reconfig touched-kind counter array must match ClusterTouchKind count");


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

	ReconfigShmem = (ClusterReconfigState *)ShmemInitStruct("pgrac cluster reconfig",
															cluster_reconfig_shmem_size(), &found);

	if (!found) {
		/* First-time init — zero everything, then set up LWLock +
		 * never-applied sentinel (event_id=0, observer_role=NONE).
		 */
		memset(ReconfigShmem, 0, sizeof(ClusterReconfigState));
		LWLockInitialize(&ReconfigShmem->lock, LWTRANCHE_CLUSTER_RECONFIG);
		pg_atomic_init_u64(&ReconfigShmem->apply_counter, 0);
		pg_atomic_init_u64(&ReconfigShmem->dedup_skip_counter, 0);
		pg_atomic_init_u64(&ReconfigShmem->procsig_broadcast_count, 0);
		/* spec-5.14 D6 — touched_peers observability counters. */
		pg_atomic_init_u64(&ReconfigShmem->touched_abort_count, 0);
		pg_atomic_init_u64(&ReconfigShmem->touched_stamp_count, 0);
		for (int k = 0; k < CLUSTER_RECONFIG_TOUCH_KIND_COUNT; k++)
			pg_atomic_init_u64(&ReconfigShmem->touched_stamp_by_kind[k], 0);
		pg_atomic_init_u64(&ReconfigShmem->clean_leave_rejected_count, 0);
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

	if (ReconfigShmem == NULL) {
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
	uint64 event_seq;

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
		 (unsigned long)published.event_id, published.coordinator_node_id,
		 (unsigned long)published.old_epoch, (unsigned long)published.new_epoch,
		 published.observer_role, (unsigned long)published.cssd_dead_generation);
}


/* ============================================================
 * Counter accessors (Step 2 + Step 3 SRF support).
 * ============================================================
 */

uint64
cluster_reconfig_get_apply_counter(void)
{
	if (ReconfigShmem == NULL)
		return 0;
	return pg_atomic_read_u64(&ReconfigShmem->apply_counter);
}

uint64
cluster_reconfig_get_dedup_skip_counter(void)
{
	if (ReconfigShmem == NULL)
		return 0;
	return pg_atomic_read_u64(&ReconfigShmem->dedup_skip_counter);
}

uint64
cluster_reconfig_get_procsig_broadcast_count(void)
{
	if (ReconfigShmem == NULL)
		return 0;
	return pg_atomic_read_u64(&ReconfigShmem->procsig_broadcast_count);
}


/* ============================================================
 * spec-5.14 D6 — touched_peers observability counter mutators + getters.
 *
 *	  Called from the hot cross-node ingress path (note_touched_stamp,
 *	  via cluster_touched_peers_stamp) and the D4 dispatch
 *	  (note_touched_abort / note_clean_leave_rejected).  All atomic, no
 *	  lock, never ereport (L213).
 * ============================================================
 */
void
cluster_reconfig_note_touched_stamp(int kind)
{
	if (ReconfigShmem == NULL)
		return;
	pg_atomic_fetch_add_u64(&ReconfigShmem->touched_stamp_count, 1);
	if (kind >= 0 && kind < CLUSTER_RECONFIG_TOUCH_KIND_COUNT)
		pg_atomic_fetch_add_u64(&ReconfigShmem->touched_stamp_by_kind[kind], 1);
}

uint64
cluster_reconfig_note_touched_abort(void)
{
	if (ReconfigShmem == NULL)
		return 1; /* pretend non-zero so caller skips the LOG-once */
	return pg_atomic_fetch_add_u64(&ReconfigShmem->touched_abort_count, 1);
}

void
cluster_reconfig_note_clean_leave_rejected(void)
{
	if (ReconfigShmem == NULL)
		return;
	pg_atomic_fetch_add_u64(&ReconfigShmem->clean_leave_rejected_count, 1);
}

uint64
cluster_reconfig_get_touched_abort_count(void)
{
	if (ReconfigShmem == NULL)
		return 0;
	return pg_atomic_read_u64(&ReconfigShmem->touched_abort_count);
}

uint64
cluster_reconfig_get_touched_stamp_count(void)
{
	if (ReconfigShmem == NULL)
		return 0;
	return pg_atomic_read_u64(&ReconfigShmem->touched_stamp_count);
}

uint64
cluster_reconfig_get_touched_stamp_by_kind(int kind)
{
	if (ReconfigShmem == NULL || kind < 0 || kind >= CLUSTER_RECONFIG_TOUCH_KIND_COUNT)
		return 0;
	return pg_atomic_read_u64(&ReconfigShmem->touched_stamp_by_kind[kind]);
}

uint64
cluster_reconfig_get_clean_leave_rejected_count(void)
{
	if (ReconfigShmem == NULL)
		return 0;
	return pg_atomic_read_u64(&ReconfigShmem->clean_leave_rejected_count);
}


/* ============================================================
 * Step 2 internal helpers.
 * ============================================================
 */

/*
 * Set / test bit i (0-based) in a 128-bit bitmap stored as uint8[16].
 * Bit i is byte (i/8) bit (i%8).  Little-endian byte order (consistent
 * with hex serialization in pg_cluster_reconfig_state.dead_bitmap).
 */
static inline void
dead_bitmap_set_bit(uint8 *bmp, int i)
{
	Assert(i >= 0 && i < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES * 8);
	bmp[i / 8] |= (uint8)(1u << (i % 8));
}


static inline bool
dead_bitmap_is_zero(const uint8 *bmp)
{
	int i;
	for (i = 0; i < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES; i++)
		if (bmp[i] != 0)
			return false;
	return true;
}


/* Returns lowest bit index set in bmp, or -1 if all zero. */
static int
dead_bitmap_lowest_bit_set(const uint8 *bmp)
{
	int i, j;
	for (i = 0; i < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES; i++) {
		if (bmp[i] == 0)
			continue;
		for (j = 0; j < 8; j++)
			if (bmp[i] & (uint8)(1u << j))
				return i * 8 + j;
	}
	return -1;
}


/*
 * spec-2.29 P1.2: event_id = hash_bytes_extended(dead_bitmap[16] || cssd_dead_generation).
 *
 *	  NOT hash(old_epoch, ...) — old_epoch would self-loop per P1.2 finding.
 *	  hash_bytes_extended is PG's 64-bit murmurhash-style;collision-resistance
 *	  is sufficient for dedup (R2 mitigation).  event_id=0 reserved as
 *	  never-applied sentinel;in the astronomically rare case real hash
 *	  yields 0 we treat that as fresh-tick (re-publish, no harm).
 */
uint64
cluster_reconfig_compute_event_id(const uint8 dead_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES],
								  uint64 cssd_dead_generation)
{
	uint8 hash_input[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES + sizeof(uint64)];

	memcpy(hash_input, dead_bitmap, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
	memcpy(hash_input + CLUSTER_RECONFIG_DEAD_BITMAP_BYTES, &cssd_dead_generation, sizeof(uint64));
	return hash_bytes_extended(hash_input, sizeof(hash_input), 0);
}


/*
 * Read snapshot of last_applied.event_id under shared lock.  Used by
 * lmon_tick dedup check before deciding whether to broadcast +
 * publish.  LWLock SHARED so multiple LMON ticks (race window during
 * coordinator switch) are read-side concurrent.
 */
static uint64
cluster_reconfig_get_last_event_id(void)
{
	uint64 id;

	if (ReconfigShmem == NULL)
		return 0;
	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	id = ReconfigShmem->last_applied.event_id;
	LWLockRelease(&ReconfigShmem->lock);
	return id;
}


/* ============================================================
 * Step 2 D2 — cluster_reconfig_lmon_tick body.
 *
 *	  Stateless deterministic per Q6 C.  Runs every LMON tick (~100ms).
 *	  Implements:
 *	    §3.1  CSSD DEAD edge detection (declared-peer filter F11)
 *	    §3.2  Q2 A'' coordinator decision (P1.1 — CSSD survivor SSOT)
 *	    §3.2  event_id hash dedup (P1.2 — dead_gen, NOT old_epoch)
 *	    §3.4  I7 every-in_quorum-survivor PROCSIG broadcast (P1.3)
 *	    §3.3  I7 coordinator-only epoch++ (P1.3)
 * ============================================================
 */

void
cluster_reconfig_lmon_tick(void)
{
	uint8 dead_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	uint8 alive_set[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	int32 self_id;
	int coordinator;
	uint64 cssd_dead_generation;
	uint64 event_id;
	int i;

	/* L20: runtime feature flag check first line. */
	if (!cluster_enabled)
		return;

	CLUSTER_INJECTION_POINT("cluster-reconfig-tick-entry");

	/* spec-2.29 D9 wait event registered for pg_stat_cluster_wait_events
	 * SRF visibility;pgstat_report_wait_start wrapping deferred to Sprint
	 * A hardening (lmon_tick has many early returns; clean wait_start/
	 * wait_end pairing needs cleanup refactor). */

	/* I2 + I8: only in_quorum nodes participate in reconfig. */
	if (!cluster_qvotec_in_quorum())
		return;

	self_id = cluster_node_id;
	if (self_id < 0 || self_id >= CLUSTER_MAX_NODES)
		return; /* defensive: bad self id, cannot participate */

	/*
	 * §3.1 + F11: build dead_bitmap and alive_set using CSSD peer_state,
	 * filtering out un-declared peers.  Self is added to alive_set only
	 * if self is declared in cluster.conf (sanity guard) and in_quorum.
	 */
	if (cluster_conf_lookup_node(self_id) != NULL)
		dead_bitmap_set_bit(alive_set, self_id);
	else
		return; /* self un-declared — must not be coordinator */

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		ClusterCssdPeerState state;

		if (i == self_id)
			continue;
		if (cluster_conf_lookup_node(i) == NULL)
			continue; /* F11: skip un-declared peer */

		state = cluster_cssd_get_peer_state(i);
		if (state == CLUSTER_CSSD_PEER_DEAD)
			dead_bitmap_set_bit(dead_bitmap, i);
		else /* ALIVE or SUSPECTED counts as alive for survivor set */
			dead_bitmap_set_bit(alive_set, i);
	}

	/* No peer death → no reconfig event. */
	if (dead_bitmap_is_zero(dead_bitmap))
		return;

	/* survivor_set is alive_set (alive_set never overlaps dead_bitmap
	 * by construction:  alive_set adds ALIVE/SUSPECTED, dead_bitmap
	 * adds DEAD, and CSSD state is mutually exclusive). */
	coordinator = dead_bitmap_lowest_bit_set(alive_set);
	if (coordinator < 0)
		return; /* total cluster failure;fail-closed already via QVOTEC */

	CLUSTER_INJECTION_POINT("cluster-reconfig-decide-coordinator");

	/* §3.2 P1.2: event_id from dead_bitmap + dead_generation snapshot. */
	cssd_dead_generation = cluster_cssd_get_dead_generation();
	event_id = cluster_reconfig_compute_event_id(dead_bitmap, cssd_dead_generation);

	/* Dedup against last_applied.  Same dead_bitmap within one DEAD
	 * episode → same dead_gen → same event_id → skip.  Rejoin-then-
	 * redeath bumps dead_gen → different event_id → re-fire. */
	if (event_id == cluster_reconfig_get_last_event_id()) {
		if (ReconfigShmem != NULL)
			pg_atomic_fetch_add_u64(&ReconfigShmem->dedup_skip_counter, 1);
		return;
	}

	/* P1.3 (b) + I7:  ONLY the deterministic coordinator advances epoch
	 * and publishes coordinator-role event.  Non-coordinator survivors
	 * publish observer-role event for local observability.
	 *
	 * spec-5.14 (ordering fix):  publish last_applied BEFORE broadcasting the
	 * PROCSIG below.  The spec-2.29 writable abort path (53R60) never reads
	 * last_applied, but the spec-5.14 read-side touched abort does (it needs
	 * reconfig_kind + dead_bitmap).  If a survivor's idle backend processed the
	 * PROCSIG before the event was published, it would read a stale
	 * (reconfig_kind=NONE) last_applied, see touched=false, absorb, and clear
	 * its pending flag — missing the touched abort.  Publishing first closes
	 * that race; the broadcast only signals local backends (no cross-node
	 * protocol change). */
	if (self_id == coordinator) {
		cluster_reconfig_apply_epoch_bump_as_coordinator(dead_bitmap, coordinator,
														 cssd_dead_generation);
	} else {
		ReconfigEvent evt;

		memset(&evt, 0, sizeof(evt));
		evt.event_id = event_id;
		evt.coordinator_node_id = coordinator;
		evt.old_epoch = cluster_epoch_get_current();
		evt.new_epoch = evt.old_epoch; /* survivor not yet observed via piggyback */
		memcpy(evt.dead_bitmap, dead_bitmap, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
		evt.applied_at = GetCurrentTimestamp();
		evt.observer_role = CLUSTER_RECONFIG_OBSERVER_SURVIVOR;
		evt.cssd_dead_generation = cssd_dead_generation;
		/* spec-5.14 D3: the only membership-change trigger today is a CSSD
		 * DEAD edge (dead_bitmap non-empty) → fail-stop.  CLEAN_LEAVE has no
		 * producer until spec-5.13. */
		evt.reconfig_kind = RECONFIG_KIND_FAIL_STOP;
		cluster_reconfig_publish_event(&evt);
	}

	/* P1.3 (a) + I7:  EVERY in_quorum survivor (NOT just coordinator)
	 * broadcasts PROCSIG_CLUSTER_RECONFIG_START to its local backends — AFTER
	 * the event is published above so the touched check reads a consistent
	 * last_applied (spec-5.14 ordering fix).
	 *
	 * review P1-A: broadcast ONLY if the event was actually published.  The
	 * coordinator's apply_epoch_bump can fail-close (fence marker did not reach
	 * a voting-disk majority) and return WITHOUT publishing — last_applied then
	 * still carries the prior event_id.  Broadcasting in that case would make
	 * survivors process the PROCSIG against a stale (reconfig_kind=NONE)
	 * last_applied, absorb the touched abort, and clear their pending flag
	 * (INV-TP2 miss).  The survivor path above always publishes, so this gate
	 * lets it through; the next LMON tick re-fires the failed coordinator. */
	if (cluster_reconfig_get_last_event_id() == event_id)
		cluster_reconfig_broadcast_local_procsig();
}


/*
 * Step 2 D2 — cluster_reconfig_broadcast_local_procsig.
 *
 *	  P1.3 (a) + I7:  every in_quorum survivor calls this on a fresh
 *	  dead_bitmap event_id.  Walks ProcArray (1..MaxBackends) and
 *	  SendProcSignal(PROCSIG_CLUSTER_RECONFIG_START) to every live
 *	  backend's pid.  Pattern mirrors cluster_fence_broadcast_freeze
 *	  (spec-2.28 D5):  no lock held during SendProcSignal, ProcArray
 *	  snapshot read is safe-stale.
 */
void
cluster_reconfig_broadcast_local_procsig(void)
{
	int beid;
	int signaled = 0;
	pid_t self_pid = MyProcPid;

	if (!cluster_enabled)
		return;
	if (ReconfigShmem == NULL)
		return;

	CLUSTER_INJECTION_POINT("cluster-reconfig-broadcast-procsig-pre");

	for (beid = 1; beid <= MaxBackends; beid++) {
		PGPROC *proc = BackendIdGetProc((BackendId)beid);
		pid_t pid;

		if (proc == NULL)
			continue;
		pid = proc->pid;
		if (pid == 0 || pid == self_pid)
			continue; /* skip LMON self */
		(void)SendProcSignal(pid, PROCSIG_CLUSTER_RECONFIG_START, (BackendId)beid);
		signaled++;
	}

	pg_atomic_fetch_add_u64(&ReconfigShmem->procsig_broadcast_count, 1);

	elog(DEBUG1, "cluster_reconfig: broadcast PROCSIG_CLUSTER_RECONFIG_START to %d backend(s)",
		 signaled);
}


/*
 * Step 2 D2 — cluster_reconfig_apply_epoch_bump_as_coordinator.
 *
 *	  P1.3 (b):  only the deterministic coordinator (min(survivor)) calls
 *	  this.  Atomically advances epoch via D18 cluster_epoch_advance_
 *	  for_reconfig, stamps the WAL insert LSN, publishes a coordinator-
 *	  role ReconfigEvent.  IC envelope piggyback (spec-2.4 + D20 receive
 *	  path observe) propagates the new epoch to non-coord survivors.
 */
void
cluster_reconfig_apply_epoch_bump_as_coordinator(
	const uint8 dead_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES], int32 coordinator_node_id,
	uint64 cssd_dead_generation)
{
	uint64 old_epoch, new_epoch;
	XLogRecPtr lsn;
	ReconfigEvent evt;

	if (!cluster_enabled)
		return;

	CLUSTER_INJECTION_POINT("cluster-reconfig-epoch-bump-pre");

	/* D18:  atomic CAS-loop increment.  Returns pre/post snapshots. */
	cluster_epoch_advance_for_reconfig(&old_epoch, &new_epoch);

	/* D18:  stamp the LSN at which epoch changed (for SRF observability
	 * + future WAL replay).  GetXLogInsertRecPtr is the next insert
	 * position;adequate for "approximately when" semantics. */
	lsn = GetXLogInsertRecPtr();
	cluster_epoch_set_changed_at_lsn((uint64)lsn);

	/*
	 * PGRAC: spec-2.34 D4 (HC95) — eager wake of GCS block-shipping
	 * outstanding slots.  Must run AFTER cluster_epoch_advance_for_reconfig
	 * + cluster_epoch_set_changed_at_lsn (so slot.request_epoch <
	 * new_epoch comparison is well-defined) and BEFORE
	 * cluster_reconfig_publish_event (so peer backends start retrying
	 * before the reconfig event broadcast hits them).  Callsite uniqueness
	 * enforced by DoD grep (spec-2.34 §7).
	 */
	cluster_gcs_block_on_epoch_advance(new_epoch);

	/*
	 * spec-2.39 D14:  reconfig RESET-all hook.  Triggers local SIResetAll
	 * via the SinvalBcast aux process + clears stale ack_wait entries so
	 * blocked enqueuers don't wait forever on a peer that just died /
	 * was added.  Local-only (each surviving node runs this for itself);
	 * cluster弹性收敛.
	 */
	cluster_sinval_reset_all_on_reconfig();

	/*
	 * spec-3.1 D7 (v0.4 N11):  flush cluster Undo TT status overlay on
	 * reconfig epoch bump.  Adopt the spec-2.39 D14 hardcoded-callsite
	 * pattern (linkdb has no register-based reconfig callback API).
	 *
	 * Why here:  old-epoch overlay entries become invalid when the
	 * cluster epoch advances (HC182);  a fresh epoch must start with a
	 * clean overlay to avoid stale-status leaks across reconfig.
	 * Generation bump inside flush_all means future readers naturally
	 * skip pre-flush entries even if the flush races with concurrent
	 * lookups (HC181 fail-closed).
	 *
	 * PG CLOG is intentionally NOT touched (feature-069 L176).
	 */
	cluster_tt_status_flush_all((uint32)new_epoch);

	memset(&evt, 0, sizeof(evt));
	evt.event_id = cluster_reconfig_compute_event_id(dead_bitmap, cssd_dead_generation);
	evt.coordinator_node_id = coordinator_node_id;
	evt.old_epoch = old_epoch;
	evt.new_epoch = new_epoch;
	memcpy(evt.dead_bitmap, dead_bitmap, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
	evt.applied_at = GetCurrentTimestamp();
	evt.observer_role = CLUSTER_RECONFIG_OBSERVER_COORDINATOR;
	evt.cssd_dead_generation = cssd_dead_generation;
	/* spec-5.14 D3: CSSD DEAD edge → fail-stop (see survivor path note). */
	evt.reconfig_kind = RECONFIG_KIND_FAIL_STOP;

	/*
	 * spec-4.12 D4 (core 8.A order):  when the write fence is enforced, the durable
	 * fence marker MUST be on >= quorum-majority voting disks BEFORE we publish the
	 * coordinator event (publishing is what starts recovery on the survivors).  We
	 * hand the marker to qvotec (the sole voting-disk writer) and wait for a
	 * quorum-majority ack.  If the ack does not come (write failure / qvotec down /
	 * timeout) we FAIL CLOSED:  do NOT publish, do NOT start recovery.  The epoch is
	 * already bumped (a safe frozen/write-fenced state -- stale tokens no longer
	 * match), and the next LMON tick retries (last_event_id is only set by
	 * publish_event, so a failed submit re-fires rather than dedup-skipping).
	 *
	 * Skipped entirely when enforcement is off/dev so a non-fenced cluster pays no
	 * marker-write cost and reconfig behaves exactly as before (zero regression).
	 */
	if (cluster_write_fence_enforcement == CLUSTER_WRITE_FENCE_ENFORCE_ON) {
		ClusterFenceMarker marker;

		memset(&marker, 0, sizeof(marker));
		marker.magic = CLUSTER_FENCE_MARKER_MAGIC;
		marker.version = CLUSTER_FENCE_MARKER_VERSION;
		marker.fence_epoch = new_epoch;
		marker.fence_event_id = evt.event_id; /* identity only */
		marker.fence_generation = cssd_dead_generation;
		marker.issuer_node_id = coordinator_node_id;
		memcpy(marker.fenced_dead_bitmap, dead_bitmap, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);

		if (cluster_write_fence_submit_marker(&marker) != CLUSTER_FENCE_MARKER_SUBMIT_ACK) {
			ereport(LOG, (errmsg("cluster reconfig: fence marker did not reach a voting-disk "
								 "majority for epoch %llu; not publishing reconfig event "
								 "(write-fenced, will retry)",
								 (unsigned long long)new_epoch)));
			return; /* fail-closed: epoch bumped, event NOT published, recovery NOT started */
		}
	}

	cluster_reconfig_publish_event(&evt);
}


/*
 * Step 2 D4 — cluster_reconfig_check_pending_in_proc_interrupts.
 *
 *	  Called from tcop/postgres.c::ProcessInterrupts after
 *	  cluster_fence_check_interrupts.  PG's ProcessInterrupts already
 *	  returns early when CritSectionCount > 0, so the I6 commit-
 *	  durable safety guard (P1.5) is partially enforced by PG itself.
 *	  We additionally absorb when IsTransactionState() is false (idle /
 *	  post-commit cleanup completed) or when no top-level xid has been
 *	  assigned yet (read-only transaction so far) to avoid 53R60 firing
 *	  on non-writes.
 *
 *	  Read-clear-then-decide pattern (per Q5 A' + spec-2.28 §3.7 C4):
 *	    1. cheap pre-check on sig_atomic_t (avoid hot-loop write)
 *	    2. clear flag BEFORE GUC / tx-state checks (prevents stale
 *	       pending after disable + re-enable + new tx)
 *	    3. decide whether to ereport based on GUC + writable tx state
 *	       + quorum state
 *
 *	  Error code routing (spec-2.29 §2.4):
 *	    - 53R50 ERRCODE_CLUSTER_QUORUM_LOST_BACKEND  — not in_quorum
 *	    - 53R60 ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS — in_quorum + epoch changed
 */
ClusterReconfigVerdict
cluster_reconfig_classify_verdict(bool touched, bool has_top_xid, bool in_quorum)
{
	/* touched (read OR write): abort; lost quorum escalates to terminal. */
	if (touched)
		return in_quorum ? RECONFIG_VERDICT_ABORT_TOUCHED : RECONFIG_VERDICT_ABORT_QUORUM;

	/* non-touched read-only: absorb (INV-TP5).  Even on lost quorum a
	 * read-only tx is not aborted here — the spec-2.28 fence path owns that. */
	if (!has_top_xid)
		return RECONFIG_VERDICT_ABSORB;

	/* non-touched writable: 53R60 normally, 53R50 if quorum was lost. */
	return in_quorum ? RECONFIG_VERDICT_ABORT_RECONFIG : RECONFIG_VERDICT_ABORT_QUORUM;
}


void
cluster_reconfig_check_pending_in_proc_interrupts(void)
{
	ReconfigEvent ev;
	bool touched;
	ClusterReconfigVerdict verdict;

	if (!cluster_enabled)
		return;

	if (cluster_reconfig_start_pending == 0)
		return; /* hot-path early return */

	cluster_reconfig_start_pending = false; /* read-clear FIRST */

	/* I6:  PG ProcessInterrupts already guards CritSectionCount > 0
	 * (postgres.c top of function).  We add IsTransactionState absorb
	 * to silently no-op on idle / post-commit cleanup tail. */
	if (!IsTransactionState())
		return;

	cluster_reconfig_get_last_event(&ev); /* shared-lock copy */

	/*
	 * spec-5.14 D4 — fold any exited parallel workers' touches into this
	 * leader's bitmap (Q7) before deciding, then test touched ∩ dead.  A
	 * touched transaction (read OR write) aborts, breaking the no-top-xid
	 * read-only absorb below and closing the read-side 8.A hole (INV-TP2);
	 * a non-touched transaction keeps the unchanged spec-2.29 behaviour so an
	 * innocent local-only read-only transaction is never killed (INV-TP5).
	 */
	cluster_touched_peers_merge_active_parallel_workers();
	touched = (ev.reconfig_kind != RECONFIG_KIND_NONE)
			  && cluster_touched_peers_intersects(ev.dead_bitmap);

	/*
	 * Defensive: CLEAN_LEAVE has no producer until spec-5.13.  A touched
	 * CLEAN_LEAVE here is unexpected — tally + WARN, then handle it
	 * conservatively as a fail-stop (rule 8: explicit, not silent).
	 */
	if (touched && ev.reconfig_kind == RECONFIG_KIND_CLEAN_LEAVE) {
		cluster_reconfig_note_clean_leave_rejected();
		ereport(WARNING, (errmsg("cluster reconfig: unexpected CLEAN_LEAVE kind during transition; "
								 "handling conservatively as fail-stop")));
	}

	/* diag (default off): dump this tx's touched-set hex on any touched abort. */
	if (touched && cluster_touched_peers_trace) {
		char hexbuf[24];

		cluster_touched_peers_self_hex(hexbuf, sizeof(hexbuf));
		ereport(LOG, (errmsg("cluster fail-stop touched-set (low 64 nodes): %s", hexbuf)));
	}

	verdict = cluster_reconfig_classify_verdict(
		touched, TransactionIdIsValid(GetTopTransactionIdIfAny()), cluster_qvotec_in_quorum());

	switch (verdict) {
	case RECONFIG_VERDICT_ABSORB:
		return;

	case RECONFIG_VERDICT_ABORT_TOUCHED:
		/* L213: LOG once per cold start, not per aborted backend. */
		if (cluster_reconfig_note_touched_abort() == 0)
			ereport(LOG, (errmsg("cluster fail-stop: aborting in-flight transactions that "
								 "consumed volatile state from a failed cluster member")));
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_RECONFIG_ABORT), /* 40R01, Class 40 retry-safe */
						errmsg("transaction aborted: cluster member fail-stop during "
							   "reconfiguration"),
						errdetail("this transaction read or held volatile state from a node that "
								  "fail-stopped"),
						errhint("retry the transaction;affected resources will be remastered")));
		break;

	case RECONFIG_VERDICT_ABORT_QUORUM:
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_QUORUM_LOST_BACKEND),
						errmsg("transaction aborted: cluster quorum lost during reconfig"),
						errhint("the cluster lost majority quorum;all uncommitted writes "
								"have been rolled back;retry after quorum recovery")));
		break;

	case RECONFIG_VERDICT_ABORT_RECONFIG:
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
						errmsg("transaction aborted: cluster reconfiguration in progress"),
						errhint("cluster membership changed during your transaction;"
								" the transaction was aborted before commit;retry is safe")));
		break;
	}
}


/* ============================================================
 * Step 3 D5b — SRF body for pg_cluster_reconfig_state view.
 *
 *	P2.9 always-1-row contract:  never-applied state surfaces as
 *	event_id=0 + observer_role='none' + applied_at NULL.  Disabled
 *	cluster path (cluster.enabled=off) returns 0 rows so that
 *	observability tooling can distinguish "feature off" from
 *	"feature on, no event yet".
 *
 *	Columns (9):  event_id int8 / coordinator_node_id int4 /
 *	old_epoch int8 / new_epoch int8 / dead_bitmap text /
 *	applied_at timestamptz / observer_role text /
 *	event_seq int8 / cssd_dead_generation int8
 * ============================================================
 */

static const char *
reconfig_observer_role_to_string(int32 role)
{
	switch (role) {
	case CLUSTER_RECONFIG_OBSERVER_COORDINATOR:
		return "coordinator";
	case CLUSTER_RECONFIG_OBSERVER_SURVIVOR:
		return "survivor";
	case CLUSTER_RECONFIG_OBSERVER_NONE:
	default:
		return "none";
	}
}


/* spec-5.14 D6 — render ReconfigEvent.reconfig_kind for the SRF view. */
static const char *
reconfig_kind_to_string(uint8 kind)
{
	switch (kind) {
	case RECONFIG_KIND_FAIL_STOP:
		return "fail_stop";
	case RECONFIG_KIND_CLEAN_LEAVE:
		return "clean_leave";
	case RECONFIG_KIND_NONE:
	default:
		return "none";
	}
}


static text *
reconfig_dead_bitmap_to_hex_text(const uint8 *bmp)
{
	/* "0x" + 32 hex digits + NUL = 35 bytes. */
	char buf[40];
	int i;

	buf[0] = '0';
	buf[1] = 'x';
	for (i = 0; i < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES; i++)
		snprintf(buf + 2 + (i * 2), 3, "%02x", bmp[i]);
	buf[2 + CLUSTER_RECONFIG_DEAD_BITMAP_BYTES * 2] = '\0';
	return cstring_to_text(buf);
}


Datum
cluster_get_reconfig_state(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo;
	ReconfigEvent evt;
	Datum values[10];
	bool nulls[10];

	InitMaterializedSRF(fcinfo, 0);
	rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

	if (!cluster_enabled)
		return (Datum)0; /* disabled — 0 rows */

	cluster_reconfig_get_last_event(&evt);

	memset(nulls, false, sizeof(nulls));

	values[0] = Int64GetDatum((int64)evt.event_id);
	values[1] = Int32GetDatum(evt.coordinator_node_id);
	values[2] = Int64GetDatum((int64)evt.old_epoch);
	values[3] = Int64GetDatum((int64)evt.new_epoch);
	values[4] = PointerGetDatum(reconfig_dead_bitmap_to_hex_text(evt.dead_bitmap));

	if (evt.applied_at == 0)
		nulls[5] = true; /* never-applied: applied_at NULL */
	else
		values[5] = TimestampTzGetDatum(evt.applied_at);

	values[6]
		= PointerGetDatum(cstring_to_text(reconfig_observer_role_to_string(evt.observer_role)));
	values[7] = Int64GetDatum((int64)evt.event_seq);
	values[8] = Int64GetDatum((int64)evt.cssd_dead_generation);
	/* spec-5.14 D6 — fail-stop vs clean-leave membership-event kind. */
	values[9] = PointerGetDatum(cstring_to_text(reconfig_kind_to_string(evt.reconfig_kind)));

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	return (Datum)0;
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
{}

void
cluster_reconfig_shmem_register(void)
{}

void
cluster_reconfig_get_last_event(ReconfigEvent *out)
{
	if (out != NULL)
		memset(out, 0, sizeof(ReconfigEvent));
}

void
cluster_reconfig_publish_event(const ReconfigEvent *evt pg_attribute_unused())
{}

void
cluster_reconfig_lmon_tick(void)
{}

void
cluster_reconfig_broadcast_local_procsig(void)
{}

void
cluster_reconfig_apply_epoch_bump_as_coordinator(
	const uint8 dead_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] pg_attribute_unused(),
	int32 coordinator_node_id pg_attribute_unused(),
	uint64 cssd_dead_generation pg_attribute_unused())
{}

void
cluster_reconfig_check_pending_in_proc_interrupts(void)
{}

#endif /* USE_PGRAC_CLUSTER */
