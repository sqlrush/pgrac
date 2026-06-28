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

#include "cluster/cluster_conf.h"		 /* cluster_conf_lookup_node */
#include "cluster/cluster_cssd.h"		 /* cluster_cssd_get_peer_state, get_dead_generation */
#include "cluster/cluster_elog.h"		 /* cluster_node_id */
#include "cluster/cluster_epoch.h"		 /* advance + observe + set_changed_at_lsn */
#include "cluster/cluster_gcs_block.h"	 /* spec-2.34 D4 — eager epoch wake hook */
#include "cluster/cluster_sinval.h"		 /* spec-2.39 D14 — RESET-all reconfig hook */
#include "cluster/cluster_tt_status.h"	 /* spec-3.1 D7 — TT status overlay flush hook */
#include "storage/ipc.h"				 /* on_shmem_exit (spec-5.15 D4 latch clear) */
#include "storage/latch.h"				 /* Latch / SetLatch (spec-5.15 D4 marker mailbox) */
#include "cluster/cluster_clean_leave.h" /* v1.0.4 — cluster_clean_leave_in_progress (serialize) */
#include "cluster/cluster_guc.h"		 /* cluster_enabled, cluster_online_join */
#include "cluster/cluster_inject.h"		 /* CLUSTER_INJECTION_POINT */
#include "cluster/cluster_voting_disk_io.h" /* spec-5.15 D4 — region-3 join-marker slot I/O */
#include "cluster/cluster_write_fence.h"	/* spec-4.12 D4 — durable fence marker submit */
#include "cluster/cluster_qvotec.h"			/* cluster_qvotec_in_quorum */
#include "cluster/cluster_shmem.h"			/* cluster_shmem_register_region */
#include "cluster/cluster_signal.h"			/* cluster_reconfig_start_pending */
#include "cluster/cluster_touched_peers.h"	/* spec-5.14 D4 — touched ∩ dead dispatch */


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
/* spec-5.15 D3 widened 96 -> 112: join_bitmap[16] grows ReconfigEvent 88 -> 104. */
StaticAssertDecl(sizeof(ReconfigEvent) <= 112, "ReconfigEvent must fit within 112 bytes");
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
		pg_atomic_init_u64(&ReconfigShmem->clean_leave_drain_grace_count, 0);
		/* spec-5.13 D3 — clean_departed_bitmap + clean_departed_epoch[] left
		 * zeroed by memset (no node departed); init the lifetime counter. */
		pg_atomic_init_u64(&ReconfigShmem->clean_departed_count, 0);
		/* spec-5.18 D3 — removed_bitmap + removed_epoch[] left zeroed by memset
		 * (no node removed); init the lifetime counter. */
		pg_atomic_init_u64(&ReconfigShmem->removed_count, 0);
		/* last_applied left zeroed by memset — event_id=0 =
		 * CLUSTER_RECONFIG_OBSERVER_NONE = never-applied sentinel. */

		/* spec-5.15 D2/D5 — membership table + pending_join_bitmap left zeroed by
		 * memset (all nodes CLUSTER_MEMBER_ABSENT, no pending join).
		 *
		 * Hardening v1.1 (HF-2): default the joiner write gate CLOSED when
		 * online_join is on (fail-closed — a freshly-booted node must PROVE it is
		 * a cold-bootstrap member or be admitted before it may write; this closes
		 * the boot-to-first-LMON-tick fail-open window, P1-2).  When online_join
		 * is off the gate is open: no online membership gating, so bootstrap and
		 * steady-state writes are unaffected. */
		ReconfigShmem->self_join_admitted = cluster_online_join ? 0 : 1;

		/* spec-5.15 D1/D4 — no slot observed yet (generation 0 = absent/not ready). */
		for (int n = 0; n < CLUSTER_MAX_NODES; n++) {
			pg_atomic_init_u64(&ReconfigShmem->observed_incarnation[n], 0);
			pg_atomic_init_u64(&ReconfigShmem->observed_generation[n], 0);
			pg_atomic_init_u64(&ReconfigShmem->observed_epoch[n], 0);
		}

		/* spec-5.15 D4 — join-marker submit mailbox (latch published by qvotec). */
		ReconfigShmem->join_qvotec_latch = NULL;
		pg_atomic_init_u64(&ReconfigShmem->join_marker_request_seq, 0);
		pg_atomic_init_u64(&ReconfigShmem->join_marker_completion_seq, 0);
		pg_atomic_init_u32(&ReconfigShmem->join_marker_result, CLUSTER_JOIN_MARKER_SUBMIT_FAILED);
		ReconfigShmem->join_marker_target_node_id = -1;
		/* join_pending_marker left zeroed by memset. */

		/* spec-5.15 D6 — online-join observability counters. */
		pg_atomic_init_u64(&ReconfigShmem->join_pending_count, 0);
		pg_atomic_init_u64(&ReconfigShmem->join_apply_count, 0);
		pg_atomic_init_u64(&ReconfigShmem->join_reject_count, 0);
		pg_atomic_init_u64(&ReconfigShmem->join_timeout_count, 0);
		pg_atomic_init_u64(&ReconfigShmem->clean_departed_cleared_count, 0);
	}

	/*
	 * spec-5.15 D2 — every backend points the cluster_membership accessors at the
	 * shared table in this region (outside the !found block: EXEC_BACKEND children
	 * re-attach their process-local pointer to the inherited shmem).
	 */
	cluster_membership_attach(&ReconfigShmem->membership);
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
cluster_reconfig_note_clean_leave_drain_grace(void)
{
	if (ReconfigShmem == NULL)
		return 1; /* pretend non-zero so the caller skips the LOG-once */
	return pg_atomic_fetch_add_u64(&ReconfigShmem->clean_leave_drain_grace_count, 1);
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

uint64
cluster_reconfig_get_clean_leave_drain_grace_count(void)
{
	if (ReconfigShmem == NULL)
		return 0;
	return pg_atomic_read_u64(&ReconfigShmem->clean_leave_drain_grace_count);
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
dead_bitmap_test_bit(const uint8 *bmp, int i)
{
	Assert(i >= 0 && i < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES * 8);
	return (bmp[i / 8] & (uint8)(1u << (i % 8))) != 0;
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
 * spec-5.15 D3 (INV-J11) — kind-aware event_id, folding the discriminator for
 * the kinds that need it.
 *
 *	FAIL_STOP / CLEAN_LEAVE  -> the LEGACY cluster_reconfig_compute_event_id over
 *	    (dead_bitmap, cssd_dead_generation).  This is byte-identical to the 2.29
 *	    death path AND to what shipped spec-5.13 binds into the durable leave
 *	    marker (RC-1, verified against linkdb: cluster_clean_leave.c binds the
 *	    legacy id — folding CLEAN_LEAVE would break that binding).  FAIL_STOP and
 *	    CLEAN_LEAVE never share a (dead_bitmap, cssd_dead_generation): each
 *	    death / leave episode advances cssd_dead_generation, which is the actual
 *	    non-collision basis (NOT a folded kind byte).
 *	JOIN_PENDING / JOIN_COMMITTED -> FOLD kind || join_bitmap ||
 *	    joiner_incarnations || cssd_dead_generation.  Join events have an empty
 *	    dead_bitmap, so the legacy hash would collide across the two phases and
 *	    across distinct joins (R12); folding the kind distinguishes PENDING from
 *	    COMMITTED and the incarnations distinguish distinct joiner sets.
 *	NONE -> 0 (the never-applied sentinel; not a real event).
 */
uint64
cluster_reconfig_compute_event_id_v2(uint8 reconfig_kind,
									 const uint8 dead_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES],
									 const uint8 join_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES],
									 const uint64 joiner_incarnations[CLUSTER_MAX_NODES],
									 uint64 cssd_dead_generation)
{
	uint8 hash_input[1 + CLUSTER_RECONFIG_DEAD_BITMAP_BYTES + sizeof(uint64) * CLUSTER_MAX_NODES
					 + sizeof(uint64)];
	Size off = 0;

	switch (reconfig_kind) {
	case RECONFIG_KIND_NONE:
		return 0; /* sentinel — not a real event */

	case RECONFIG_KIND_FAIL_STOP:
	case RECONFIG_KIND_CLEAN_LEAVE:
		return cluster_reconfig_compute_event_id(dead_bitmap, cssd_dead_generation);

	case RECONFIG_KIND_JOIN_PENDING:
	case RECONFIG_KIND_JOIN_COMMITTED:
	default:
		Assert(join_bitmap != NULL && joiner_incarnations != NULL);
		hash_input[off++] = reconfig_kind;
		memcpy(hash_input + off, join_bitmap, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
		off += CLUSTER_RECONFIG_DEAD_BITMAP_BYTES;
		memcpy(hash_input + off, joiner_incarnations, sizeof(uint64) * CLUSTER_MAX_NODES);
		off += sizeof(uint64) * CLUSTER_MAX_NODES;
		memcpy(hash_input + off, &cssd_dead_generation, sizeof(uint64));
		off += sizeof(uint64);
		return hash_bytes_extended(hash_input, off, 0);
	}
}

/*
 * spec-5.18 D3 (R14) — NODE_REMOVED event identity.
 *
 *	The legacy event_id hashes only (dead_bitmap, cssd_dead_generation).  A
 *	clean-left removal leaves dead_bitmap unchanged (the node already departed)
 *	and does not bump cssd_dead_generation, so the legacy id would collide with
 *	the prior event and be deduped away — the removal would never publish (打穿
 *	INV-LF1).  Fold the kind + removed_bitmap + removal_event_id (the per-attempt
 *	identity) — NOT old_epoch (2.29 P1.2 anti-self-loop) — so each removal attempt
 *	produces a distinct, non-deduped event id.
 */
uint64
cluster_reconfig_compute_removal_event_id(
	const uint8 removed_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES], uint64 removal_event_id)
{
	uint8 hash_input[1 + CLUSTER_RECONFIG_DEAD_BITMAP_BYTES + sizeof(uint64)];
	Size off = 0;

	hash_input[off++] = RECONFIG_KIND_NODE_REMOVED;
	memcpy(hash_input + off, removed_bitmap, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
	off += CLUSTER_RECONFIG_DEAD_BITMAP_BYTES;
	memcpy(hash_input + off, &removal_event_id, sizeof(uint64));
	off += sizeof(uint64);
	return hash_bytes_extended(hash_input, off, 0);
}


/*
 * spec-5.15 D1 — cluster_reconfig_compute_join_bitmap.
 *
 *	Compute the set of declared peers that transitioned from a non-member state
 *	(DEAD / ABSENT) into a fresh-ALIVE state since the last applied reconfig (the
 *	"join edge"), mirroring the death-edge computation in the tick.  A bit i is
 *	set iff ALL hold:
 *	  - cluster_conf_lookup_node(i) != NULL          (declared-peer filter)
 *	  - cluster_membership_get_state(i) is DEAD/ABSENT (not currently a member)
 *	  - cluster_cssd_get_peer_state(i) == ALIVE      (now heart-beating)
 *	  - the peer's observed voting-slot incarnation is strictly greater than
 *	    last_admitted_incarnation[i]                 (freshness, anti-stale: a
 *	    stale rejoin never even raises a join edge; the coordinator vet (D4) is
 *	    the authoritative gate that issues REJECT_STALE on the TOCTOU window)
 *
 *	Pure w.r.t. shmem reads (uses the qvotec-published observed slot, not a disk
 *	read).  Caller holds the reconfig LWLock (membership_state reads).  Returns
 *	the number of join-edge bits set.
 */
int
cluster_reconfig_compute_join_bitmap(uint8 join_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES])
{
	int i;
	int count = 0;

	memset(join_bitmap, 0, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);

	if (ReconfigShmem == NULL)
		return 0;

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		ClusterMembershipState ms;
		uint64 observed_incarnation = 0;
		uint64 observed_generation = 0;

		if (i == cluster_node_id)
			continue; /* self is never a join candidate */
		if (cluster_conf_lookup_node(i) == NULL)
			continue; /* declared-peer filter */

		ms = cluster_membership_get_state(i);
		if (ms != CLUSTER_MEMBER_DEAD && ms != CLUSTER_MEMBER_ABSENT)
			continue; /* already a member / mid-join (JOINING) — not a new edge */

		if (cluster_cssd_get_peer_state(i) != CLUSTER_CSSD_PEER_ALIVE)
			continue; /* not heart-beating yet */

		if (!cluster_reconfig_get_observed_slot(i, &observed_incarnation, &observed_generation))
			continue; /* no valid published slot — not ready */
		if (observed_incarnation <= cluster_membership_get_last_admitted_incarnation(i))
			continue; /* stale incarnation (INV-J1 pre-filter); vet REJECTs at commit */

		dead_bitmap_set_bit(join_bitmap, i);
		count++;
	}

	return count;
}


/*
 * spec-5.15 D1 — qvotec publishes the freshest observed voting-slot incarnation
 * + generation per node here each poll (it is the sole disk reader).  pg_atomic
 * write so qvotec, a different process, publishes lock-free.  NULL-safe / range-
 * checked.
 */
void
cluster_reconfig_record_observed_slot(int32 node_id, uint64 incarnation, uint64 generation,
									  uint64 epoch)
{
	if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return;
	pg_atomic_write_u64(&ReconfigShmem->observed_incarnation[node_id], incarnation);
	pg_atomic_write_u64(&ReconfigShmem->observed_generation[node_id], generation);
	pg_atomic_write_u64(&ReconfigShmem->observed_epoch[node_id], epoch);
}

uint64
cluster_reconfig_get_observed_epoch(int32 node_id)
{
	if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return 0;
	return pg_atomic_read_u64(&ReconfigShmem->observed_epoch[node_id]);
}


/*
 * spec-5.15 D1 — read the freshest observed voting-slot incarnation + generation
 * for node_id.  Returns true iff a valid slot was observed (generation > 0);
 * out-params always written (0 when absent).  Used by the join-edge detector and
 * the coordinator vet.
 */
bool
cluster_reconfig_get_observed_slot(int32 node_id, uint64 *incarnation, uint64 *generation)
{
	uint64 gen;

	if (incarnation != NULL)
		*incarnation = 0;
	if (generation != NULL)
		*generation = 0;

	if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return false;

	gen = pg_atomic_read_u64(&ReconfigShmem->observed_generation[node_id]);
	if (incarnation != NULL)
		*incarnation = pg_atomic_read_u64(&ReconfigShmem->observed_incarnation[node_id]);
	if (generation != NULL)
		*generation = gen;
	return gen > 0;
}

/*
 * spec-5.15 Hardening v1.3 (INV-J14 stale-slot fail-open) — publish / read the
 * per-node FRESH-ALIVE liveness qvotec derived from decide_quorum_view's
 * heartbeat-freshness gate (P2.1).  The cold-bootstrap proof counts a peer only
 * when it is fresh-alive at epoch INITIAL — a generation > 0 slot alone may be a
 * crashed peer's stale leftover.  Anchored on the durable voting-disk heartbeat,
 * not live CSSD, so the v1.2 IC-churn race fix is preserved.
 */
void
cluster_reconfig_record_observed_fresh_alive(int32 node_id, bool fresh_alive)
{
	if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return;
	pg_atomic_write_u64(&ReconfigShmem->observed_fresh_alive[node_id], fresh_alive ? 1 : 0);
}

bool
cluster_reconfig_get_observed_fresh_alive(int32 node_id)
{
	if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return false;
	return pg_atomic_read_u64(&ReconfigShmem->observed_fresh_alive[node_id]) != 0;
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
 * spec-5.13 D3 (CL-I13) — clean-departed bitmap record / clear / query.
 *
 *	A node enters clean_departed_bitmap when a CLEAN_LEAVE reconfig commits
 *	naming it (survivor observe), or at startup rebuilt from a durable §2.5
 *	COMMITTED marker.  cluster_reconfig_lmon_tick masks it out of the dead set
 *	so its later CSSD DEAD never re-triggers fail-stop (the spurious second
 *	reconfig of R18 / 40R01 of a drain-grace tx).  Mutations take `lock`
 *	EXCLUSIVE; the lmon_tick masking reads under SHARED.
 * ============================================================
 */

static inline bool
clean_departed_test_bit_locked(const uint8 *bmp, int i)
{
	if (i < 0 || i >= CLUSTER_RECONFIG_DEAD_BITMAP_BYTES * 8)
		return false;
	return (bmp[i / 8] & (uint8)(1u << (i % 8))) != 0;
}

void
cluster_reconfig_record_clean_departed(int32 node_id, uint64 leave_epoch, bool raise_epoch_floor)
{
	bool newly_set = false;

	if (ReconfigShmem == NULL)
		return;
	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return;

	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	if (!clean_departed_test_bit_locked(ReconfigShmem->clean_departed_bitmap, node_id)) {
		dead_bitmap_set_bit(ReconfigShmem->clean_departed_bitmap, node_id);
		newly_set = true;
	}
	/* Always record the latest leave epoch (a re-departure raises it). */
	if (leave_epoch > ReconfigShmem->clean_departed_epoch[node_id])
		ReconfigShmem->clean_departed_epoch[node_id] = leave_epoch;
	LWLockRelease(&ReconfigShmem->lock);

	if (newly_set)
		pg_atomic_fetch_add_u64(&ReconfigShmem->clean_departed_count, 1);

	/*
	 * P1-V0.7 epoch-floor recovery: the membership epoch is not durable, so a
	 * durable COMMITTED marker is the only proof the cluster reached
	 * leave_epoch.  Raise the local floor (max-merge, monotone, never retreats)
	 * — exempt from OBSERVE_MAX_JUMP since this is startup recovery, not a
	 * hostile inbound envelope.  Done outside the reconfig lock (epoch is its
	 * own shmem with its own CAS).
	 */
	if (raise_epoch_floor && leave_epoch > 0)
		(void)cluster_epoch_observe_remote(leave_epoch);
}

void
cluster_reconfig_clear_clean_departed(int32 node_id)
{
	if (ReconfigShmem == NULL)
		return;
	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return;

	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	if (clean_departed_test_bit_locked(ReconfigShmem->clean_departed_bitmap, node_id))
		ReconfigShmem->clean_departed_bitmap[node_id / 8] &= (uint8) ~(1u << (node_id % 8));
	ReconfigShmem->clean_departed_epoch[node_id] = 0;
	LWLockRelease(&ReconfigShmem->lock);
}

bool
cluster_reconfig_is_clean_departed(int32 node_id)
{
	bool departed;

	if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return false;

	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	departed = clean_departed_test_bit_locked(ReconfigShmem->clean_departed_bitmap, node_id);
	LWLockRelease(&ReconfigShmem->lock);
	return departed;
}

uint64
cluster_reconfig_get_clean_departed_epoch(int32 node_id)
{
	uint64 epoch;

	if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return 0;

	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	epoch = ReconfigShmem->clean_departed_epoch[node_id];
	LWLockRelease(&ReconfigShmem->lock);
	return epoch;
}

uint64
cluster_reconfig_get_clean_departed_count(void)
{
	if (ReconfigShmem == NULL)
		return 0;
	return pg_atomic_read_u64(&ReconfigShmem->clean_departed_count);
}

/* ============================================================
 * spec-5.18 D3 — permanently-removed set accessors (INV-LF1).
 * ============================================================ */

void
cluster_reconfig_record_removed(int32 node_id, uint64 remove_epoch, bool raise_epoch_floor)
{
	bool newly_set = false;

	if (ReconfigShmem == NULL)
		return;
	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return;

	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	if (!clean_departed_test_bit_locked(ReconfigShmem->removed_bitmap, node_id)) {
		dead_bitmap_set_bit(ReconfigShmem->removed_bitmap, node_id);
		newly_set = true;
	}
	if (remove_epoch > ReconfigShmem->removed_epoch[node_id])
		ReconfigShmem->removed_epoch[node_id] = remove_epoch;
	/* a removed node supersedes a dormant clean-departed one (no auto re-admit). */
	if (clean_departed_test_bit_locked(ReconfigShmem->clean_departed_bitmap, node_id)) {
		ReconfigShmem->clean_departed_bitmap[node_id / 8] &= (uint8) ~(1u << (node_id % 8));
		ReconfigShmem->clean_departed_epoch[node_id] = 0;
	}
	LWLockRelease(&ReconfigShmem->lock);

	if (newly_set)
		pg_atomic_fetch_add_u64(&ReconfigShmem->removed_count, 1);

	/* P1-V0.7 epoch-floor recovery (mirror clean_departed): the removal marker is
	 * the only durable proof the cluster reached remove_epoch. */
	if (raise_epoch_floor && remove_epoch > 0)
		(void)cluster_epoch_observe_remote(remove_epoch);
}

bool
cluster_reconfig_is_removed(int32 node_id)
{
	bool removed;

	if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return false;

	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	removed = clean_departed_test_bit_locked(ReconfigShmem->removed_bitmap, node_id);
	LWLockRelease(&ReconfigShmem->lock);
	return removed;
}

/*
 * spec-5.18 HF-2: lock-free removed test for the 53R64 self-demote write gate
 * (cluster_node_remove_self_is_removed), called at every writable-xid assignment.
 * Reads a single removed_bitmap bit without the reconfig lock — safe because the
 * removed set is monotonic at runtime (a removal is terminal, INV-LF1; only an
 * operator un-fence, not implemented, could clear it), so a one-byte read cannot
 * tear and the bit never spuriously clears.  The durable bitmap is the
 * authoritative floor: unlike membership_state[self] (which the joiner / lmon
 * self-state paths rewrite each tick), it cannot be flipped REMOVED -> not-removed
 * by membership churn, so the write gate stays fail-closed for a removed node.
 */
bool
cluster_reconfig_is_removed_unlocked(int32 node_id)
{
	if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_RECONFIG_DEAD_BITMAP_BYTES * 8)
		return false;
	return (ReconfigShmem->removed_bitmap[node_id / 8] & (uint8)(1u << (node_id % 8))) != 0;
}

uint64
cluster_reconfig_get_removed_epoch(int32 node_id)
{
	uint64 epoch;

	if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return 0;

	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	epoch = ReconfigShmem->removed_epoch[node_id];
	LWLockRelease(&ReconfigShmem->lock);
	return epoch;
}

uint64
cluster_reconfig_get_removed_count(void)
{
	if (ReconfigShmem == NULL)
		return 0;
	return pg_atomic_read_u64(&ReconfigShmem->removed_count);
}

/*
 * spec-5.18 D3 — startup-rebuild seed: record the node removed (removed_bitmap +
 * epoch floor) AND shrink its membership_state to REMOVED, under the reconfig lock.
 * Used by cluster_node_remove_rebuild_from_disks so the driver does not need direct
 * access to ReconfigShmem->lock for the membership mutation.
 */
void
cluster_reconfig_seed_removed_membership(int32 node_id, uint64 remove_epoch,
										 uint64 removed_incarnation, bool raise_epoch_floor)
{
	cluster_reconfig_record_removed(node_id, remove_epoch, raise_epoch_floor);
	if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return;
	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	cluster_membership_shrink_to_removed(node_id, removed_incarnation);
	LWLockRelease(&ReconfigShmem->lock);
}

/*
 * Snapshot the removed_bitmap under SHARED lock (qvotec ORs it into the fence
 * baseline, INV-LF10; the driver folds it into the removal event_id).
 */
void
cluster_reconfig_snapshot_removed_bitmap(uint8 *out)
{
	if (out == NULL)
		return;
	if (ReconfigShmem == NULL) {
		memset(out, 0, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
		return;
	}
	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	memcpy(out, ReconfigShmem->removed_bitmap, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
	LWLockRelease(&ReconfigShmem->lock);
}

/* spec-5.15 D6 — online-join observability counter accessors. */
uint64
cluster_reconfig_get_join_pending_count(void)
{
	return ReconfigShmem == NULL ? 0 : pg_atomic_read_u64(&ReconfigShmem->join_pending_count);
}
uint64
cluster_reconfig_get_join_apply_count(void)
{
	return ReconfigShmem == NULL ? 0 : pg_atomic_read_u64(&ReconfigShmem->join_apply_count);
}
uint64
cluster_reconfig_get_join_reject_count(void)
{
	return ReconfigShmem == NULL ? 0 : pg_atomic_read_u64(&ReconfigShmem->join_reject_count);
}
uint64
cluster_reconfig_get_join_timeout_count(void)
{
	return ReconfigShmem == NULL ? 0 : pg_atomic_read_u64(&ReconfigShmem->join_timeout_count);
}
uint64
cluster_reconfig_get_clean_departed_cleared_count(void)
{
	return ReconfigShmem == NULL ? 0
								 : pg_atomic_read_u64(&ReconfigShmem->clean_departed_cleared_count);
}

/*
 * cluster_reconfig_join_in_progress -- Hardening v1.0.4 (spec-5.13 clean-leave x
 * spec-5.15 online-join serialization, P1-1/P2): is a membership JOIN currently in
 * its pending window anywhere this node can observe?  The clean-leave request +
 * real-announce paths consult this so a clean leave does not START (or a survivor
 * does not ACCEPT an announce) while a join is mid-flight — the symmetric half of
 * "one membership reconfig at a time" (the join driver checks the mirror predicate
 * cluster_clean_leave_in_progress()).  A join bumps the membership epoch with
 * dead_gen unchanged, which the leaving node would otherwise mis-observe as its own
 * clean-leave commit and wedge in BARRIER_WAIT (P2).  Read under the reconfig lock
 * (consistent snapshot of the 16-byte pending bitmap); called only at reconfig-rate
 * boundaries, never on a hot path.
 */
bool
cluster_reconfig_join_in_progress(void)
{
	bool in_progress = false;
	int i;

	if (ReconfigShmem == NULL)
		return false;

	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	if (ReconfigShmem->self_join_admitted == 0)
		in_progress = true; /* this node is itself a not-yet-admitted joiner */
	else if (ReconfigShmem->last_applied.reconfig_kind == RECONFIG_KIND_JOIN_PENDING)
		in_progress = true; /* the last published edge is a JOIN Phase-1 */
	else {
		for (i = 0; i < (int)sizeof(ReconfigShmem->pending_join_bitmap); i++) {
			if (ReconfigShmem->pending_join_bitmap[i] != 0) {
				in_progress = true; /* a declared peer is in the join pending window */
				break;
			}
		}
	}
	LWLockRelease(&ReconfigShmem->lock);
	return in_progress;
}


/*
 * spec-5.15 D4 §3.3 — has the JOIN_PENDING converged?  Every existing MEMBER
 * survivor (other than self and the joiner) must have observed the coordinator's
 * current membership epoch (the joiner is NOT in the convergence set — INV-J12 —
 * it adopts from the COMMITTED marker, breaking the commit<-converge<-adopt
 * cycle).  At 2 nodes the only MEMBER survivor is self, so this is trivially true
 * and Phase-2 follows on the next tick.
 */
static bool
cluster_reconfig_join_converged(int joiner)
{
	uint64 target_epoch = cluster_epoch_get_current();
	int i;

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (i == cluster_node_id || i == joiner)
			continue;
		if (cluster_conf_lookup_node(i) == NULL)
			continue;
		if (!cluster_membership_is_member(i))
			continue; /* not a member -> not in the convergence set */
		if (cluster_reconfig_get_observed_epoch(i) < target_epoch)
			return false; /* this MEMBER survivor has not caught up */
	}
	return true;
}

/*
 * spec-5.15 Hardening v1.1 (HF-1 / INV-J9 strengthened) — proof that the
 * coordinator's JOIN_COMMITTED publish actually propagated to a quorum of the
 * membership, NOT merely that the COMMITTED marker is durable.  A joiner opens
 * its write gate only after a majority of the current MEMBER survivors have
 * advanced their durable (voting-disk-observed) epoch to >= admitted_epoch.
 *
 * The survivor epoch is observable because qvotec publishes the LIVE
 * cluster_epoch_get_current() into each slot (5.15 ship substrate-fix #1);
 * after the coordinator's publish advances the cluster epoch to admitted_epoch
 * (== new_epoch == current+1 at marker-write time) the members reach it via the
 * normal bounded observe and persist it, and the joiner reads it back through
 * observed_epoch[].  If the coordinator crashes AFTER the COMMITTED marker is
 * durable but BEFORE the publish (injection cluster-reconfig-join-commit-marker-
 * durable), the survivors never advance, this proof never holds, the gate stays
 * closed, and the joiner times out -> 53R61 -> restarts with a fresh incarnation
 * (P1-1: the half-publish window the v1.0 note_self_admitted left open).
 *
 * Lock-free, mirroring cluster_reconfig_join_converged: is_member is a single-
 * byte read and get_observed_epoch is an atomic read.  Fail-closed: zero visible
 * MEMBER survivor (e.g. transient) -> cannot prove -> false.
 */
bool
cluster_reconfig_join_publish_proven(uint64 admitted_epoch)
{
	uint32 members = 0;
	uint32 advanced = 0;
	int i;

	if (ReconfigShmem == NULL || admitted_epoch == 0)
		return false;

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (i == cluster_node_id)
			continue;
		if (cluster_conf_lookup_node(i) == NULL)
			continue;
		if (!cluster_membership_is_member(i))
			continue; /* only existing MEMBER survivors carry the publish */
		members++;
		if (cluster_reconfig_get_observed_epoch(i) >= admitted_epoch)
			advanced++;
	}

	if (members == 0)
		return false; /* nobody to prove it -> fail-closed */
	return advanced >= ((members / 2u) + 1u);
}

/*
 * spec-5.15 D4 — coordinator-side join driver (called from the tick when
 * online_join is on and self is the min-MEMBER coordinator).  Phase-1: fresh
 * join edges -> apply_join_as_coordinator (JOIN_PENDING).  Phase-2: pending joins
 * whose convergence is met -> re-vet (TOCTOU, INV-J1) -> commit_member.
 */
static void
cluster_reconfig_drive_joins(int coordinator)
{
	uint8 join_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint8 pending_snapshot[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint64 joiner_incarnations[CLUSTER_MAX_NODES];
	int n_join;
	int i;

	if (ReconfigShmem == NULL)
		return;

	/* Phase-1 detection + a snapshot of the current pending set, under the lock
	 * (compute_join_bitmap reads membership_state). */
	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	n_join = cluster_reconfig_compute_join_bitmap(join_bitmap);
	memcpy(pending_snapshot, ReconfigShmem->pending_join_bitmap, sizeof(pending_snapshot));
	LWLockRelease(&ReconfigShmem->lock);

	if (n_join > 0) {
		memset(joiner_incarnations, 0, sizeof(joiner_incarnations));
		for (i = 0; i < CLUSTER_MAX_NODES; i++) {
			if (!dead_bitmap_test_bit(join_bitmap, i))
				continue;
			(void)cluster_reconfig_get_observed_slot(i, &joiner_incarnations[i], NULL);
		}
		cluster_reconfig_apply_join_as_coordinator(join_bitmap, coordinator, joiner_incarnations);
		/* enter the JOIN_PENDING transition fail-closed on every local backend. */
		cluster_reconfig_broadcast_local_procsig();
	}

	/* Phase-2: commit pending joins that have converged.  The just-added Phase-1
	 * joiner is NOT in pending_snapshot (set after the snapshot), so it commits on
	 * a later tick — the intended two-phase, two-tick bracket. */
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		uint64 admitted_incarnation = 0;
		uint64 admitted_generation = 0;

		if (!dead_bitmap_test_bit(pending_snapshot, i))
			continue;
		if (!cluster_reconfig_join_converged(i))
			continue;
		if (!cluster_reconfig_get_observed_slot(i, &admitted_incarnation, &admitted_generation))
			continue; /* no valid slot now -> wait */
		/* authoritative re-vet at the commit point (TOCTOU, INV-J1): stale /
		 * not-ready / out-of-quorum -> skip (the joiner times out -> REJECT). */
		if (cluster_membership_vet_joiner(i, admitted_incarnation, admitted_generation)
			!= CLUSTER_JOIN_ACCEPT) {
			pg_atomic_fetch_add_u64(&ReconfigShmem->join_reject_count, 1);
			continue;
		}
		(void)cluster_reconfig_commit_member(i, admitted_incarnation);
	}
}


/* ============================================================
 * spec-5.15 D5 — joiner-side write gate + admission.
 * ============================================================
 */

ClusterJoinGateVerdict
cluster_reconfig_self_join_gate_verdict(void)
{
	if (ReconfigShmem == NULL)
		return CLUSTER_JOIN_GATE_ALLOW;
	/* single-byte reads — naturally atomic; this is a hot xact-entry check. */
	if (ReconfigShmem->self_join_failed)
		return CLUSTER_JOIN_GATE_BLOCK_53R61;
	if (!ReconfigShmem->self_join_admitted)
		return CLUSTER_JOIN_GATE_BLOCK_53R60;
	return CLUSTER_JOIN_GATE_ALLOW;
}

void
cluster_reconfig_note_self_admitted(uint64 admitted_epoch)
{
	if (ReconfigShmem == NULL)
		return;
	if (cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES)
		return;

	/*
	 * Hardening v1.1 (HF-1 / INV-J9): this is called ONLY after the caller
	 * (qvotec) has confirmed BOTH a same-commit majority COMMITTED marker (HF-3)
	 * AND the publish-proof — cluster_reconfig_join_publish_proven — i.e. a
	 * member quorum has reached admitted_epoch, proving the coordinator's
	 * JOIN_COMMITTED publish actually propagated.  The v1.0 "gate-open guard =
	 * adopt && state==MEMBER" was vacuous (this function self-set state==MEMBER),
	 * which left the half-publish window open (P1-1); the real guard is now the
	 * publish-proof at the caller.  Here we just adopt the admitted epoch
	 * (quorum-authenticated, may jump >16 — INV-J12), set self MEMBER and open
	 * the gate.
	 */
	cluster_epoch_adopt_admitted(admitted_epoch);
	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	cluster_membership_set_state(cluster_node_id, CLUSTER_MEMBER_MEMBER);
	ReconfigShmem->self_join_admitted = 1;
	ReconfigShmem->self_join_failed = 0;
	ReconfigShmem->self_join_deadline_us = 0;
	LWLockRelease(&ReconfigShmem->lock);
}

/*
 * Is the cluster already running (so a freshly-booted node is REJOINING, not
 * bootstrapping — §3.4)?  Signal: a declared peer observed at a committed epoch
 * > CLUSTER_EPOCH_INITIAL.  A node only becomes absent after the survivors
 * noticed and reconfigured (which advances the epoch), so by the time it reboots
 * its peers are past epoch 0; a cold bootstrap has every node at epoch 0.
 */
static bool
cluster_reconfig_cluster_already_running(void)
{
	int i;

	if (ReconfigShmem == NULL)
		return false;
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (i == cluster_node_id)
			continue;
		if (cluster_conf_lookup_node(i) == NULL)
			continue;
		if (cluster_reconfig_get_observed_epoch(i) > CLUSTER_EPOCH_INITIAL)
			return true;
	}
	return false;
}

/*
 * spec-5.15 Hardening v1.1 (HF-2 / INV-J14) — positive cold-bootstrap proof,
 * REVISED by Hardening v1.2 (INV-J14 self-join-gate race) to rest on the durable
 * voting-disk slot rather than live CSSD.
 *
 * A node may keep its write gate open WITHOUT online-join admission only when a
 * majority of declared nodes are observed CO-BOOTING at CLUSTER_EPOCH_INITIAL on
 * a VALID durable slot (qvotec saw a real voting-disk slot, generation > 0, at
 * epoch INITIAL) AND no declared peer is observed past CLUSTER_EPOCH_INITIAL.
 * This is an EPOCH proof, not a timing grace: a slow qvotec leaves the decision
 * UNDECIDED (gate stays closed, fail-closed) instead of mis-deciding bootstrap
 * and permanently fail-opening (P1-2).  A rejoiner can never satisfy it — by the
 * time it sees its peers they are already at epoch > INITIAL.
 *
 * v1.2 RATIONALE: the v1.1 proof counted live CSSD-alive peers, which a
 * transient IC / heartbeat churn could momentarily drop below quorum — leaving a
 * GENUINE founding member UNDECIDED (never latched).  An UNRELATED peer's later
 * fail-stop then advanced the epoch, and joiner_self_tick reclassified that
 * still-UNDECIDED member as a rejoiner: it closed its own write gate and timed
 * out to 53R61 (refused its own writes), never participating again.  Anchoring
 * the quorum on the DURABLE slot (stable across CSSD churn) lets a founding
 * member latch reliably during formation, closing the UNDECIDED window before
 * any unrelated epoch advance.  A default-0 placeholder (generation 0) is NOT
 * proof and must never count (else a node with no real evidence fail-opens) —
 * the v1.2 user constraint: latch only on a valid co-boot slot, never on 0.
 * Quorum (not all-declared) is retained so a degraded co-boot (e.g. 2 of 3) can
 * still form, and because requiring every peer would only WIDEN the UNDECIDED
 * window the race exploits.
 */
bool
cluster_reconfig_bootstrap_quorum_at_initial(void)
{
	uint32 declared = 0;
	uint32 proven_at_initial = 0;
	int i;

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		uint64 inc = 0;
		uint64 gen = 0;
		uint64 ep;

		if (cluster_conf_lookup_node(i) == NULL)
			continue;
		declared++;
		if (i == cluster_node_id) {
			proven_at_initial++; /* self is up, at INITIAL (not yet admitted) */
			continue;
		}
		ep = cluster_reconfig_get_observed_epoch(i);
		/* any declared peer past INITIAL => a running cluster, NOT a bootstrap */
		if (ep > CLUSTER_EPOCH_INITIAL)
			return false;
		/*
		 * Count a peer only on a FRESH-ALIVE co-boot slot: a real observed
		 * voting-disk slot (generation > 0) that qvotec's decide_quorum_view saw
		 * FRESH-ALIVE this poll (heartbeat_ts_us recent, the P2.1 freshness gate),
		 * at epoch INITIAL.  Hardening v1.3: the generation > 0 test alone is NOT
		 * liveness — a CRASHED peer leaves a stale leftover slot (gen > 0, epoch
		 * INITIAL) that v1.2 wrongly counted, letting a node fail-open (latch
		 * BOOTSTRAP on self + a stale peer slot, with no live co-boot quorum).  The
		 * fresh-alive signal is anchored on the durable voting-disk heartbeat (NOT
		 * live CSSD), so it rejects stale slots WITHOUT reintroducing the v1.2
		 * IC-churn race (the disk heartbeat keeps flowing while CSSD/tier1 churns).
		 * A default-0 placeholder (generation 0) never counts either.
		 */
		if (cluster_reconfig_get_observed_slot(i, &inc, &gen) && gen > 0
			&& cluster_reconfig_get_observed_fresh_alive(i) && ep == CLUSTER_EPOCH_INITIAL)
			proven_at_initial++;
	}
	if (declared == 0)
		return false;
	return proven_at_initial >= ((declared / 2u) + 1u);
}

/*
 * spec-5.15 D5 — joiner self-tick (runs on every node each LMON tick).
 *
 *	Decides, fail-closed, whether THIS node may write before it is a confirmed
 *	member.  Hardening v1.1 (HF-2 / INV-J14) replaces the v1.0 timing-grace
 *	heuristic — which permanently fail-opened a rejoiner that a slow qvotec
 *	mis-saw as a cold bootstrap (P1-2) — with a POSITIVE epoch proof:
 *	  - any declared peer past INITIAL          -> REJOINER: close the gate
 *	    (53R60), start the join, latch a convergence deadline -> 53R61 on timeout
 *	    (restart with a fresh incarnation, INV-J4 never half-admit).  qvotec's
 *	    note_self_admitted opens the gate once the COMMITTED marker AND the
 *	    publish-proof (HF-1) both hold.
 *	  - quorum of declared CSSD-alive at INITIAL -> BOOTSTRAP: open the gate
 *	    (boot-time formation is not gated by online_join) and latch it so a later
 *	    epoch advance does not re-close this genuine member.
 *	  - neither proven yet                       -> UNDECIDED: keep the gate
 *	    closed (fail-closed); a slow qvotec waits here, it never mis-opens.
 */
static bool joiner_gate_decided = false;

static void
cluster_reconfig_joiner_self_tick(void)
{
	uint64 now_us;

	if (ReconfigShmem == NULL || !cluster_online_join)
		return;
	if (cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES)
		return;

	/*
	 * spec-5.18 INV-LF9 (HF-2): a durably-removed node (this node was permanently
	 * removed; startup rebuild seeded removed_bitmap[self]) must NOT run the joiner
	 * gate — doing so would flip its own membership_state REMOVED -> JOINING/MEMBER
	 * (the REJOINER/bootstrap branches below) and defeat the 53R64 self-demote write
	 * gate.  A removed node can only return via operator un-fence + a fresh-
	 * incarnation join (external plane, §1.3) — never by re-running this gate.
	 */
	if (cluster_reconfig_is_removed_unlocked(cluster_node_id))
		return;

	now_us = (uint64)GetCurrentTimestamp();

	/*
	 * Catch up to the cluster epoch observed on the durable voting disk (quorum-
	 * authenticated — qvotec is the sole CRC-checked slot writer) so a rejoiner
	 * that booted at CLUSTER_EPOCH_INITIAL can COMMUNICATE.  The IC envelope drops
	 * a frame whose epoch is BELOW the receiver's (anti-stale, spec-2.4): until a
	 * rejoiner reaches the cluster epoch, its own CSSD heartbeats are stale-dropped
	 * by the survivors, so it is never seen ALIVE → never detected → never
	 * admitted (a join deadlock).  Adopting the max observed in-quorum peer epoch
	 * bridges that gap WITHOUT bypassing admission: the incarnation vet (INV-J1) +
	 * the §2.6 COMMITTED marker still gate MEMBER; this only unblocks the
	 * transport.  Runs every tick so the joiner tracks the cluster while joining.
	 */
	{
		uint64 self_epoch = cluster_epoch_get_current();
		uint64 max_peer_epoch = self_epoch;
		int i;

		for (i = 0; i < CLUSTER_MAX_NODES; i++) {
			uint64 pe;

			if (i == cluster_node_id || cluster_conf_lookup_node(i) == NULL)
				continue;
			pe = cluster_reconfig_get_observed_epoch(i);
			if (pe > max_peer_epoch)
				max_peer_epoch = pe;
		}
		if (max_peer_epoch > self_epoch)
			cluster_epoch_adopt_admitted(max_peer_epoch);
	}

	/*
	 * Hardening v1.0.4 (P2 serialization): do NOT start driving this node's own join
	 * while a clean leave is active here.  Deferring is safe — joiner_self_tick is
	 * LMON-driven and retries next tick once the leave reaches a terminal state (one
	 * membership reconfig at a time; the leave side refuses to start while a join is
	 * pending via cluster_reconfig_join_in_progress).
	 */
	if (!joiner_gate_decided && !cluster_clean_leave_in_progress()) {
		if (cluster_reconfig_cluster_already_running()) {
			/* REJOINER: a running cluster exists.  Close the gate, start the
			 * join, latch a convergence deadline (-> 53R61 on timeout). */
			joiner_gate_decided = true;
			LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
			ReconfigShmem->self_join_admitted = 0;
			ReconfigShmem->self_join_failed = 0;
			ReconfigShmem->self_join_deadline_us
				= now_us + (uint64)cluster_join_convergence_timeout_ms * 1000ULL;
			cluster_membership_set_state(cluster_node_id, CLUSTER_MEMBER_JOINING);
			LWLockRelease(&ReconfigShmem->lock);
			ereport(LOG,
					(errmsg("cluster membership: node %d joining a running cluster — write gate "
							"closed (53R60) pending admission",
							cluster_node_id)));
		} else if (cluster_reconfig_bootstrap_quorum_at_initial()) {
			/* BOOTSTRAP proven: quorum of declared nodes CSSD-alive at INITIAL.
			 * Open the gate (boot formation is not gated) and latch so a later
			 * epoch advance does not re-close this genuine member (INV-J14). */
			joiner_gate_decided = true;
			LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
			ReconfigShmem->self_join_admitted = 1;
			ReconfigShmem->self_join_failed = 0;
			ReconfigShmem->self_join_deadline_us = 0;
			cluster_membership_set_state(cluster_node_id, CLUSTER_MEMBER_MEMBER);
			LWLockRelease(&ReconfigShmem->lock);
			ereport(LOG,
					(errmsg("cluster membership: node %d cold-bootstrap membership formation — "
							"write gate open",
							cluster_node_id)));
		} else {
			/* UNDECIDED: neither proof holds yet.  Keep the gate CLOSED
			 * (fail-closed) and re-evaluate next tick — a slow qvotec waits here
			 * rather than mis-opening as bootstrap (P1-2). */
			LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
			ReconfigShmem->self_join_admitted = 0;
			cluster_membership_set_state(cluster_node_id, CLUSTER_MEMBER_JOINING);
			LWLockRelease(&ReconfigShmem->lock);
		}
		return;
	}

	/* Gate decided.  If closed + not yet admitted, fail closed on timeout (53R61);
	 * note_self_admitted opens the gate directly when self's COMMITTED marker is
	 * observed. */
	if (!ReconfigShmem->self_join_admitted && !ReconfigShmem->self_join_failed
		&& ReconfigShmem->self_join_deadline_us != 0
		&& now_us >= ReconfigShmem->self_join_deadline_us) {
		LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
		ReconfigShmem->self_join_failed = 1;
		cluster_membership_set_state(cluster_node_id, CLUSTER_MEMBER_REJECTED);
		LWLockRelease(&ReconfigShmem->lock);
		pg_atomic_fetch_add_u64(&ReconfigShmem->join_timeout_count, 1);
		ereport(LOG, (errmsg("cluster membership: node %d join did not converge within %d ms — "
							 "writes now 53R61 (restart with a fresh incarnation)",
							 cluster_node_id, cluster_join_convergence_timeout_ms)));
	}
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
	 * §3.1 + F11: build the raw CSSD DEAD bitmap, filtering out un-declared
	 * peers.  Self is alive by construction (it is running this tick, in
	 * quorum).  Lock-free snapshot — the membership-state maintenance and the
	 * survivor-set build below run under the reconfig lock.
	 */
	if (cluster_conf_lookup_node(self_id) == NULL)
		return; /* self un-declared — must not be coordinator */

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (i == self_id)
			continue;
		if (cluster_conf_lookup_node(i) == NULL)
			continue; /* F11: skip un-declared peer */

		if (cluster_cssd_get_peer_state(i) == CLUSTER_CSSD_PEER_DEAD)
			dead_bitmap_set_bit(dead_bitmap, i);
	}

	/*
	 * spec-5.15 D5 — joiner self-tick: decide (once, in the early boot window) if
	 * THIS node is rejoining a running cluster and close its write gate; latch
	 * 53R61 on convergence timeout.  Runs before the membership maintenance so the
	 * self-state below reflects the gate.
	 */
	cluster_reconfig_joiner_self_tick();

	/*
	 * spec-5.15 D1 (INV-J8): the membership-state table — NOT raw CSSD — is the
	 * decision SSOT for the survivor / coordinator set.  Maintain it and build
	 * alive_set from it, under EXCLUSIVE (we mutate membership_state):
	 *   self                         -> MEMBER (running + in quorum)
	 *   peer CSSD DEAD               -> DEAD   (a member that died; demote)
	 *   peer CSSD alive + state ABSENT -> MEMBER (bootstrap join, §3.4: a
	 *       never-seen declared node forming the initial membership; NOT gated
	 *       by online_join, which gates only runtime readmission)
	 *   peer CSSD alive + state DEAD -> stays DEAD (a recovered node is NOT
	 *       auto-readmitted — a JOIN_COMMITTED reconfig (D4) is the only
	 *       DEAD->MEMBER path; this is the §3.4 online_join=off isolation + the
	 *       P1c fix that closes "CSSD ALIVE silently counts as a member")
	 *   peer CSSD alive + JOINING/MEMBER -> unchanged
	 *
	 * Then apply spec-5.13 CL-I13 effective_dead = cssd_dead & ~clean_departed
	 * (a cleanly-departed member stops heart-beating and shows up CSSD DEAD;
	 * masking it out suppresses the spurious SECOND fail-stop reconfig).  Folded
	 * into the same EXCLUSIVE section (was a separate SHARED read pre-5.15).
	 *
	 * When the region is absent (cluster off / pre-postmaster) fall back to the
	 * pre-5.15 raw-CSSD survivor set so the degraded path is unchanged.
	 */
	if (ReconfigShmem != NULL) {
		int b;

		LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);

		/*
		 * spec-5.18 INV-LF9 (HF-2): REMOVED is TERMINAL for self too.  A removed
		 * node still running / restarted (rebuild seeded removed_bitmap[self] +
		 * membership_state[self]=REMOVED) must keep self REMOVED — the joiner gate
		 * below must NOT flip it back to MEMBER/JOINING, which would defeat the
		 * 53R64 self-demote write gate and let a removed node serve writes.  The
		 * durable removed_bitmap is the authoritative floor (mirrors the peer
		 * REMOVED terminal guard further down + the joiner_self_tick guard).
		 */
		if (clean_departed_test_bit_locked(ReconfigShmem->removed_bitmap, self_id)
			|| cluster_membership_get_state(self_id) == CLUSTER_MEMBER_REMOVED)
			cluster_membership_set_state(self_id, CLUSTER_MEMBER_REMOVED);
		/* self-state follows the joiner gate (D5): MEMBER when admitted / steady,
		 * JOINING while this node is itself a joiner whose gate is closed. */
		else if (ReconfigShmem->self_join_admitted && !ReconfigShmem->self_join_failed)
			cluster_membership_set_state(self_id, CLUSTER_MEMBER_MEMBER);
		else
			cluster_membership_set_state(self_id, CLUSTER_MEMBER_JOINING);

		for (i = 0; i < CLUSTER_MAX_NODES; i++) {
			ClusterMembershipState ms;

			if (i == self_id)
				continue;
			if (cluster_conf_lookup_node(i) == NULL)
				continue;

			ms = cluster_membership_get_state(i);
			/*
			 * spec-5.18 INV-LF1 (P0): REMOVED is TERMINAL.  This loop reads the RAW
			 * CSSD dead set (the removed mask is applied later, below), so a
			 * fail-stopped-then-removed node is still CSSD-DEAD here — without this
			 * guard the next branch would flip its membership_state REMOVED -> DEAD
			 * every tick, silently defeating the vet_joiner REJECT_REMOVED_FENCED
			 * gate (which keys on state==REMOVED) and letting a fresh-incarnation
			 * zombie passively re-admit.  A removed node never leaves REMOVED here.
			 */
			if (ms == CLUSTER_MEMBER_REMOVED)
				continue;
			if (dead_bitmap_test_bit(dead_bitmap, i))
				cluster_membership_set_state(i, CLUSTER_MEMBER_DEAD);
			else if (ms == CLUSTER_MEMBER_ABSENT)
				cluster_membership_set_state(i, CLUSTER_MEMBER_MEMBER);
			/* else DEAD stays DEAD (no auto-readmit); JOINING/MEMBER unchanged */
		}

		for (i = 0; i < CLUSTER_MAX_NODES; i++) {
			if (cluster_conf_lookup_node(i) == NULL)
				continue;
			if (cluster_membership_is_member(i))
				dead_bitmap_set_bit(alive_set, i);
		}

		/*
		 * effective_dead = cssd_dead & ~clean_departed_bitmap & ~removed_bitmap.
		 * A permanently-removed node (spec-5.18, INV-LF1) is masked out so its
		 * subsequent CSSD DEAD/ALIVE never re-triggers a reconfig nor passively
		 * re-admits it — it is no longer a member.
		 */
		for (b = 0; b < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES; b++)
			dead_bitmap[b] &= (uint8) ~(ReconfigShmem->clean_departed_bitmap[b]
										| ReconfigShmem->removed_bitmap[b]);

		LWLockRelease(&ReconfigShmem->lock);
	} else {
		dead_bitmap_set_bit(alive_set, self_id);
		for (i = 0; i < CLUSTER_MAX_NODES; i++) {
			if (i == self_id)
				continue;
			if (cluster_conf_lookup_node(i) == NULL)
				continue;
			if (!dead_bitmap_test_bit(dead_bitmap, i))
				dead_bitmap_set_bit(alive_set, i);
		}
	}

	/*
	 * survivor_set / coordinator = the min MEMBER survivor (INV-J8; alive_set is
	 * the MEMBER set built above).  Computed before the death/join dispatch since
	 * both directions use it.
	 */
	coordinator = dead_bitmap_lowest_bit_set(alive_set);
	if (coordinator < 0)
		return; /* total cluster failure;fail-closed already via QVOTEC */

	/*
	 * §3.5 ordering: handle the leave/death edge FIRST (it shrinks the MEMBER set,
	 * stabilizing the survivor base), THEN the join edge.  Each is an independent
	 * ReconfigEvent; neither early-returns past the other.
	 */
	if (!dead_bitmap_is_zero(dead_bitmap)) {
		CLUSTER_INJECTION_POINT("cluster-reconfig-decide-coordinator");

		/* §3.2 P1.2: event_id from dead_bitmap + dead_generation snapshot. */
		cssd_dead_generation = cluster_cssd_get_dead_generation();
		event_id = cluster_reconfig_compute_event_id(dead_bitmap, cssd_dead_generation);

		/* Dedup against last_applied.  Same dead_bitmap within one DEAD episode →
		 * same dead_gen → same event_id → skip.  Rejoin-then-redeath bumps
		 * dead_gen → different event_id → re-fire. */
		if (event_id == cluster_reconfig_get_last_event_id()) {
			if (ReconfigShmem != NULL)
				pg_atomic_fetch_add_u64(&ReconfigShmem->dedup_skip_counter, 1);
		} else {
			/*
			 * P1.3 (b) + I7: ONLY the deterministic coordinator advances epoch +
			 * publishes coordinator-role; non-coordinator survivors publish
			 * observer-role for local observability.  spec-5.14 ordering: publish
			 * last_applied BEFORE broadcasting the PROCSIG (the read-side touched
			 * abort reads reconfig_kind + dead_bitmap from last_applied).
			 */
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
				evt.reconfig_kind = RECONFIG_KIND_FAIL_STOP;
				cluster_reconfig_publish_event(&evt);
			}

			/*
			 * P1.3 (a) + I7: EVERY in_quorum survivor broadcasts PROCSIG — AFTER
			 * publish (spec-5.14 ordering), and ONLY if the event was actually
			 * published (review P1-A: a coordinator fence-marker fail-close does
			 * not publish; the next tick re-fires).
			 */
			if (cluster_reconfig_get_last_event_id() == event_id)
				cluster_reconfig_broadcast_local_procsig();
		}
	}

	/*
	 * §3.5 join edge (spec-5.15 D4): online declared-node readmission, driven by
	 * the coordinator.  Runs whether or not there was a death this tick; gated by
	 * online_join (off = no runtime readmit, §3.4 fail-closed-safe via INV-J8).
	 */
	/*
	 * Hardening v1.0.4 (P2 serialization, one membership reconfig at a time): do NOT
	 * drive any node's join while a clean leave is active on this node (leaver or
	 * survivor).  A join Phase-1 / commit bumps the membership epoch with dead_gen
	 * unchanged, which the leaving node mis-observes as its own clean-leave commit
	 * and wedges in BARRIER_WAIT (the leave's epoch-observe premise holds ONLY under
	 * no concurrent dead_gen-unchanged reconfig).  Joins are LMON-driven and simply
	 * retry next tick once the leave finishes; the leave side symmetrically refuses
	 * to start while a join is pending.
	 */
	if (cluster_online_join && self_id == coordinator && !cluster_clean_leave_in_progress())
		cluster_reconfig_drive_joins(coordinator);
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
		/*
		 * spec-5.18 INV-LF10: when there ARE permanently-removed nodes, every
		 * reconfig-issued fence marker must also carry them, so a later fail-stop
		 * fence (dead = {M}) does not drop a previously-removed node {N} from the
		 * authority — the fenced set is dead | removed (superset-monotone, removed
		 * only grows).  Guarded on removed_count so a cluster that never removed a
		 * node pays nothing and the fence marker is byte-identical to pre-5.18.
		 */
		if (cluster_reconfig_get_removed_count() > 0) {
			uint8 removed_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
			int b;

			cluster_reconfig_snapshot_removed_bitmap(removed_bitmap);
			for (b = 0; b < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES; b++)
				marker.fenced_dead_bitmap[b] |= removed_bitmap[b];
		}

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
 * spec-5.13 D3 — cluster_reconfig_apply_clean_leave_as_coordinator.
 *
 *	The commit point of the §3.1 two-phase clean-leave commit, run on the
 *	survivor coordinator (min node id, Q6-A).  Bumps the membership epoch and
 *	publishes a CLEAN_LEAVE reconfig event naming the leaving node in
 *	dead_bitmap, then records it clean-departed at the new epoch so the lmon_tick
 *	mask suppresses its later CSSD DEAD (CL-I13).  Mirrors
 *	apply_epoch_bump_as_coordinator's epoch-advance side effects (eager GCS wake
 *	+ sinval reset + TT overlay flush) so survivors invalidate stale leaving-node
 *	cache at epoch advance (CL-I5 happens-before).  No write-fence marker: the
 *	leaving node drained cooperatively (nothing to fence); the durable record is
 *	the §2.5 leave-intent marker, written by the driver BEFORE (COMMITTING) and
 *	AFTER (COMMITTED) this call.  PROCSIG broadcast is intentionally NOT done
 *	here — the touched drain-grace dispatch (CL-I12) + serve-gate are wired in
 *	spec-5.13 S6; the driver/LMON owns any survivor-side wake.  Returns the new
 *	epoch E (the driver stamps it into the COMMITTED marker).
 */
uint64
cluster_reconfig_apply_clean_leave_as_coordinator(int32 leaving_node_id, uint64 baseline_epoch)
{
	uint64 old_epoch, new_epoch;
	XLogRecPtr lsn;
	uint8 dead_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	uint64 cssd_dead_generation;
	ReconfigEvent evt;

	if (!cluster_enabled || ReconfigShmem == NULL)
		return 0;
	if (leaving_node_id < 0 || leaving_node_id >= CLUSTER_MAX_NODES)
		return 0;

	/* inject points for the clean-leave path are D12 (spec-5.13 S7). */

	dead_bitmap_set_bit(dead_bitmap, leaving_node_id);
	cssd_dead_generation = cluster_cssd_get_dead_generation();

	/*
	 * CL-I3 guarded advance: bump to baseline+1 ONLY if the epoch is still the
	 * baseline the leave committed against.  At >=3 nodes a third node's death
	 * could bump the epoch between the driver's pre-check and here; an
	 * unconditional CAS-loop would then stack the clean-leave on top of that
	 * death's view (a stale-baseline commit, CL-I3 violation).  The single
	 * compare_exchange fails closed in that case — return 0 so the driver does
	 * NOT write the COMMITTED marker; the leaving node observes the foreign
	 * event and escalates to fail-stop.  At 2 nodes this is exactly equivalent
	 * to the old unconditional bump (no third party can move the epoch).
	 */
	old_epoch = baseline_epoch;
	if (!cluster_epoch_advance_for_reconfig_if_baseline(baseline_epoch, &new_epoch))
		return 0;
	lsn = GetXLogInsertRecPtr();
	cluster_epoch_set_changed_at_lsn((uint64)lsn);

	/*
	 * Same epoch-advance side effects as the fail-stop coordinator path: wake
	 * GCS block-shipping slots, RESET sinval, flush the TT status overlay.  On
	 * each survivor these run when it observes the new epoch; on the coordinator
	 * they run here.  This is what invalidates stale leaving-node cache so the
	 * post-epoch storage read returns the just-flushed current (CL-I5).
	 */
	cluster_gcs_block_on_epoch_advance(new_epoch);
	cluster_sinval_reset_all_on_reconfig();
	cluster_tt_status_flush_all((uint32)new_epoch);

	memset(&evt, 0, sizeof(evt));
	evt.event_id = cluster_reconfig_compute_event_id(dead_bitmap, cssd_dead_generation);
	evt.coordinator_node_id = cluster_node_id;
	evt.old_epoch = old_epoch;
	evt.new_epoch = new_epoch;
	memcpy(evt.dead_bitmap, dead_bitmap, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
	evt.applied_at = GetCurrentTimestamp();
	evt.observer_role = CLUSTER_RECONFIG_OBSERVER_COORDINATOR;
	evt.cssd_dead_generation = cssd_dead_generation;
	evt.reconfig_kind = RECONFIG_KIND_CLEAN_LEAVE;
	cluster_reconfig_publish_event(&evt);

	/* CL-I13: record clean-departed at E so the lmon_tick mask suppresses the
	 * node's later CSSD DEAD (no epoch-floor raise — the epoch is live here). */
	cluster_reconfig_record_clean_departed(leaving_node_id, new_epoch, false);

	return new_epoch;
}


/*
 * spec-5.18 D3 — cluster_reconfig_apply_node_removed_as_coordinator.
 *
 *	The membership-shrink commit point of permanent removal (§3.1
 *	SHRINK_COMMITTING), run on the survivor coordinator AFTER the 4.12 fence
 *	marker for the removed node is majority-durable (INV-LF2, enforced by the
 *	driver).  Mirrors apply_clean_leave_as_coordinator's guarded epoch advance +
 *	epoch-advance side effects, but publishes a NODE_REMOVED event whose id folds
 *	removed_bitmap + removal_event_id (R14 — never deduped even when dead_bitmap is
 *	unchanged), then records the node removed (removed_bitmap + epoch, masking it
 *	out of effective_dead) and shrinks membership_state to REMOVED.  The published
 *	event's dead_bitmap is empty: the removed node already departed (clean-left /
 *	fail-stopped), so this is a membership change, not a new death.  Returns the
 *	new epoch, or 0 if the guarded advance lost (a real death intruded — the driver
 *	ABORTED_ESCALATEs, pre-SHRUNK).
 */
uint64
cluster_reconfig_apply_node_removed_as_coordinator(int32 removed_node_id, uint64 baseline_epoch,
												   uint64 removal_event_id, uint64 last_incarnation,
												   bool *out_contest)
{
	uint64 old_epoch, new_epoch;
	XLogRecPtr lsn;
	uint8 empty_dead[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	uint8 removed_with_n[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint64 cssd_dead_generation;
	ReconfigEvent evt;

	/*
	 * *out_contest distinguishes the two zero-returns for the driver (P1-A): a lost
	 * guarded-advance means ANOTHER node moved the epoch (a real contest -> the
	 * driver classifies escalate vs cleanup-blocked); a fence-submit failure is a
	 * transient self-bumped retry (NOT a contest -> the driver just retries).
	 */
	if (out_contest != NULL)
		*out_contest = false;

	if (!cluster_enabled || ReconfigShmem == NULL)
		return 0;
	if (removed_node_id < 0 || removed_node_id >= CLUSTER_MAX_NODES)
		return 0;

	CLUSTER_INJECTION_POINT("cluster-node-remove-shrink-committing");

	cssd_dead_generation = cluster_cssd_get_dead_generation();

	/* CL-I3-style guarded advance: fail closed if a real death moved the epoch. */
	old_epoch = baseline_epoch;
	if (!cluster_epoch_advance_for_reconfig_if_baseline(baseline_epoch, &new_epoch)) {
		if (out_contest != NULL)
			*out_contest = true; /* another node moved the epoch -> real contest */
		return 0;
	}
	lsn = GetXLogInsertRecPtr();
	cluster_epoch_set_changed_at_lsn((uint64)lsn);

	/* same epoch-advance side effects as the other coordinator paths (cache
	 * invalidation happens-before): wake GCS slots, reset sinval, flush TT overlay. */
	cluster_gcs_block_on_epoch_advance(new_epoch);
	cluster_sinval_reset_all_on_reconfig();
	cluster_tt_status_flush_all((uint32)new_epoch);

	/* R14: event_id folds the removed set (current removed_bitmap | {N}) + the
	 * per-attempt removal_event_id, so a clean-left removal (dead_bitmap unchanged)
	 * still produces a distinct, non-deduped id. */
	cluster_reconfig_snapshot_removed_bitmap(removed_with_n);
	removed_with_n[removed_node_id / 8] |= (uint8)(1u << (removed_node_id % 8));

	/*
	 * INV-LF2 (fence-before-shrink): arm the 4.12 write fence for the removed node
	 * BEFORE publishing the membership shrink — exactly like the fail-stop coordinator
	 * submits its fence before publishing.  The marker is at NEW epoch with the
	 * removed node in the fenced set, so the lower-epoch steady-state baseline is
	 * stale-guarded and cannot drop it in the arm->publish window.  Fail-closed: if
	 * the marker does not reach a voting-disk majority, do NOT publish / shrink (the
	 * epoch is bumped = a safe frozen state; the driver retries).  Skipped when
	 * enforcement is off (single-node / non-fenced cluster pays nothing).
	 */
	if (cluster_write_fence_enforcement == CLUSTER_WRITE_FENCE_ENFORCE_ON) {
		ClusterFenceMarker marker;

		memset(&marker, 0, sizeof(marker));
		marker.magic = CLUSTER_FENCE_MARKER_MAGIC;
		marker.version = CLUSTER_FENCE_MARKER_VERSION;
		marker.fence_epoch = new_epoch;
		marker.fence_event_id
			= cluster_reconfig_compute_removal_event_id(removed_with_n, removal_event_id);
		marker.fence_generation = cssd_dead_generation;
		marker.issuer_node_id = cluster_node_id;
		marker.marker_kind = CLUSTER_FENCE_MARKER_KIND_NODE_REMOVED;
		/* fenced set = removed (already includes N) — the removal needs only N fenced;
		 * a concurrent dead set, if any, is carried by its own fail-stop fence. */
		memcpy(marker.fenced_dead_bitmap, removed_with_n, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);

		if (cluster_write_fence_submit_marker(&marker) != CLUSTER_FENCE_MARKER_SUBMIT_ACK) {
			ereport(LOG, (errmsg("cluster node removal: fence marker for node %d did not reach a "
								 "voting-disk majority for epoch %llu; not publishing removal "
								 "(write-fenced, will retry)",
								 removed_node_id, (unsigned long long)new_epoch)));
			return 0; /* fail-closed: epoch bumped, removal NOT published, driver retries */
		}
	}

	CLUSTER_INJECTION_POINT("cluster-node-remove-fence-armed");

	memset(&evt, 0, sizeof(evt));
	evt.event_id = cluster_reconfig_compute_removal_event_id(removed_with_n, removal_event_id);
	evt.coordinator_node_id = cluster_node_id;
	evt.old_epoch = old_epoch;
	evt.new_epoch = new_epoch;
	memcpy(evt.dead_bitmap, empty_dead, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
	evt.applied_at = GetCurrentTimestamp();
	evt.observer_role = CLUSTER_RECONFIG_OBSERVER_COORDINATOR;
	evt.cssd_dead_generation = cssd_dead_generation;
	evt.reconfig_kind = RECONFIG_KIND_NODE_REMOVED;
	cluster_reconfig_publish_event(&evt);

	/* durable removed set (masks the node out of effective_dead, INV-LF1) + the
	 * member-set shrink (membership_state -> REMOVED), pinning the incarnation floor. */
	cluster_reconfig_record_removed(removed_node_id, new_epoch, false);
	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	cluster_membership_shrink_to_removed(removed_node_id, last_incarnation);
	LWLockRelease(&ReconfigShmem->lock);

	return new_epoch;
}


/* ============================================================
 * spec-5.15 D4 — §2.6 join-commit-marker qvotec handshake.
 *
 *	Mirrors the spec-4.12 fence / spec-5.13 leave mailbox: the coordinator stages
 *	a marker for the joiner, wakes qvotec, and blocks (bounded) until qvotec — the
 *	sole voting-disk writer — has written it to the joiner's region-3 slot on a
 *	quorum-majority of disks.  Unlike the leave marker (written to the writer's
 *	OWN slot), the join marker is written to the JOINER's slot (so each joiner's
 *	admit record persists independently of how many joins the coordinator drives).
 * ============================================================
 */

static uint64 join_qvotec_inflight_marker_seq = 0;
static uint64 join_qvotec_last_processed_marker_seq = 0;

ClusterJoinMarkerSubmitResult
cluster_reconfig_submit_join_marker(int32 target_node, const ClusterJoinCommitMarker *m)
{
	uint64 seq;
	struct Latch *qlatch;
	uint64 deadline_us;
	int wait_ms;

	if (ReconfigShmem == NULL || m == NULL)
		return CLUSTER_JOIN_MARKER_SUBMIT_FAILED;
	if (target_node < 0 || target_node >= CLUSTER_MAX_NODES)
		return CLUSTER_JOIN_MARKER_SUBMIT_FAILED;

	/* Stage target + marker, then publish the request (write barrier between so
	 * qvotec never reads a half-written mailbox). */
	ReconfigShmem->join_marker_target_node_id = target_node;
	ReconfigShmem->join_pending_marker = *m;
	pg_write_barrier();
	seq = pg_atomic_add_fetch_u64(&ReconfigShmem->join_marker_request_seq, 1);

	qlatch = ReconfigShmem->join_qvotec_latch;
	if (qlatch != NULL)
		SetLatch(qlatch);

	wait_ms = cluster_quorum_poll_interval_ms * 3 + 2000;
	deadline_us = (uint64)GetCurrentTimestamp() + (uint64)wait_ms * 1000ULL;
	pgstat_report_wait_start(WAIT_EVENT_RECONFIG_JOIN_CONVERGENCE);
	for (;;) {
		if (pg_atomic_read_u64(&ReconfigShmem->join_marker_completion_seq) == seq) {
			pgstat_report_wait_end();
			pg_read_barrier();
			return (ClusterJoinMarkerSubmitResult)pg_atomic_read_u32(
				&ReconfigShmem->join_marker_result);
		}
		if ((uint64)GetCurrentTimestamp() >= deadline_us) {
			pgstat_report_wait_end();
			return CLUSTER_JOIN_MARKER_SUBMIT_TIMEOUT;
		}
		pg_usleep(2 * 1000); /* 2 ms */
	}
}

bool
cluster_reconfig_join_qvotec_poll_pending(int32 *out_target_node, void *out_slot512)
{
	uint64 req;

	if (ReconfigShmem == NULL || out_slot512 == NULL || out_target_node == NULL)
		return false;

	req = pg_atomic_read_u64(&ReconfigShmem->join_marker_request_seq);
	if (req == join_qvotec_last_processed_marker_seq)
		return false; /* nothing new */

	pg_read_barrier();
	*out_target_node = ReconfigShmem->join_marker_target_node_id;
	memset(out_slot512, 0, CLUSTER_VOTING_SLOT_BYTES);
	memcpy(out_slot512, &ReconfigShmem->join_pending_marker,
		   sizeof(ReconfigShmem->join_pending_marker));
	join_qvotec_inflight_marker_seq = req;
	return true;
}

void
cluster_reconfig_join_qvotec_complete(bool acked)
{
	if (ReconfigShmem == NULL)
		return;

	pg_atomic_write_u32(&ReconfigShmem->join_marker_result,
						acked ? CLUSTER_JOIN_MARKER_SUBMIT_ACK : CLUSTER_JOIN_MARKER_SUBMIT_FAILED);
	pg_write_barrier();
	pg_atomic_write_u64(&ReconfigShmem->join_marker_completion_seq,
						join_qvotec_inflight_marker_seq);
	join_qvotec_last_processed_marker_seq = join_qvotec_inflight_marker_seq;
}

static void
join_clear_qvotec_latch(int code, Datum arg)
{
	if (ReconfigShmem != NULL)
		ReconfigShmem->join_qvotec_latch = NULL;
}

void
cluster_reconfig_publish_join_qvotec_latch(struct Latch *latch)
{
	if (ReconfigShmem == NULL)
		return;
	ReconfigShmem->join_qvotec_latch = latch;
	on_shmem_exit(join_clear_qvotec_latch, (Datum)0);
}


/*
 * spec-5.15 D2/D4 — cluster_membership_seed_last_admitted_from_voting_disk.
 *
 *	Startup bring-up (INV-J7): scan region 3, and for each declared node N with a
 *	struct-valid COMMITTED join marker (node_id == slot) on a quorum-majority of
 *	disks, seed last_admitted[N] from the marker's admitted_incarnation — so a
 *	restart does not zero the floor and re-open the gate to a stale incarnation.
 *	The trust gate is phase==COMMITTED + majority + crc, NEVER an epoch compare.
 *
 *	Also resolves RC-5 / INV-J10 across restart: 5.13's leave-marker rebuild (run
 *	earlier in startup) may have re-set clean_departed[N] from the still-COMMITTED
 *	leave marker of a node that has since rejoined.  If this node's COMMITTED join
 *	marker is newer than that leave (admitted_epoch > clean_departed_epoch[N]),
 *	clear clean_departed[N] so N's later real fail-stop is not masked.  MUST run
 *	AFTER the leave rebuild (see the startup wiring).
 */
void
cluster_membership_seed_last_admitted_from_voting_disk(const int *fds, int n_disks)
{
	int s;
	uint32 majority;

	if (ReconfigShmem == NULL || fds == NULL || n_disks <= 0)
		return;

	majority = ((uint32)n_disks / 2u) + 1u;

	for (s = 0; s < CLUSTER_MAX_NODES; s++) {
		union {
			uint8 bytes[CLUSTER_VOTING_SLOT_BYTES];
			uint64 _align;
		} slot;
		ClusterJoinCommitMarker committed[CLUSTER_MAX_VOTING_DISKS];
		int n_committed = 0;
		int win = -1;
		uint32 win_agree = 0;
		int d, a, b;

		if (cluster_conf_lookup_node(s) == NULL)
			continue;

		/* Collect every committed-basis marker for slot s across the disks. */
		for (d = 0; d < n_disks; d++) {
			ClusterJoinCommitMarker m;

			if (cluster_voting_disk_read_join_slot(fds[d], (uint32)s, slot.bytes)
				!= CLUSTER_VOTING_DISK_IO_OK)
				continue;
			memcpy(&m, slot.bytes, sizeof(m));
			if (!cluster_join_marker_is_committed_basis(&m, s))
				continue;
			committed[n_committed++] = m;
		}

		/*
		 * INV-J13: require a MAJORITY of the SAME commit (identical identity /
		 * nonce), not "any COMMITTED marker".  Two minority writes from
		 * different attempts (different coordinator / epoch) must not aggregate.
		 * Only a marker that actually reached a disk majority represents a real
		 * admission, so only it raises the monotonic floor (P1-3).  O(disks^2),
		 * disks <= CLUSTER_MAX_VOTING_DISKS (small).
		 */
		for (a = 0; a < n_committed; a++) {
			uint32 same = 0;

			for (b = 0; b < n_committed; b++)
				if (cluster_join_marker_same_commit(&committed[a], &committed[b]))
					same++;
			if (same >= majority) {
				win = a;
				win_agree = same;
				break;
			}
		}

		if (win >= 0 && committed[win].admitted_incarnation > 0) {
			uint64 win_incarnation = committed[win].admitted_incarnation;
			uint64 win_admitted_epoch = committed[win].admitted_epoch;

			LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
			cluster_membership_record_admitted(s, win_incarnation);
			LWLockRelease(&ReconfigShmem->lock);

			/* RC-5 / INV-J10 durable supersede: a rejoin newer than the leave
			 * clears clean_departed[N] so a survivor restart does not keep
			 * masking N's later real fail-stop. */
			if (cluster_reconfig_is_clean_departed(s)
				&& win_admitted_epoch > cluster_reconfig_get_clean_departed_epoch(s))
				cluster_reconfig_clear_clean_departed(s);

			ereport(LOG,
					(errmsg("cluster membership: seeded last_admitted[%d]=%llu from %u/%d durable "
							"COMMITTED join marker(s) of one commit (INV-J13)",
							s, (unsigned long long)win_incarnation, win_agree, n_disks)));
		}
	}
}


/* ============================================================
 * spec-5.15 D4 — two-phase online-join publication.
 * ============================================================
 */

void
cluster_reconfig_apply_join_as_coordinator(
	const uint8 join_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES], int32 coordinator_node_id,
	const uint64 joiner_incarnations[CLUSTER_MAX_NODES])
{
	uint64 old_epoch, new_epoch;
	XLogRecPtr lsn;
	uint64 cssd_dead_generation;
	uint8 empty_dead[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ReconfigEvent evt;
	int i;

	if (!cluster_enabled || ReconfigShmem == NULL)
		return;

	CLUSTER_INJECTION_POINT("cluster-reconfig-join-pending-pre");

	cssd_dead_generation = cluster_cssd_get_dead_generation();

	/* Phase-1 epoch bump (regular advance new=old+1) + the same epoch side
	 * effects as the other coordinator paths. */
	cluster_epoch_advance_for_reconfig(&old_epoch, &new_epoch);
	lsn = GetXLogInsertRecPtr();
	cluster_epoch_set_changed_at_lsn((uint64)lsn);
	cluster_gcs_block_on_epoch_advance(new_epoch);
	cluster_sinval_reset_all_on_reconfig();
	cluster_tt_status_flush_all((uint32)new_epoch);

	/* Mark joiners JOINING + pending (candidates, NOT members yet — INV-J2). */
	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (!dead_bitmap_test_bit(join_bitmap, i))
			continue;
		cluster_membership_set_state(i, CLUSTER_MEMBER_JOINING);
		dead_bitmap_set_bit(ReconfigShmem->pending_join_bitmap, i);
	}
	LWLockRelease(&ReconfigShmem->lock);

	/* Durable PREPARE marker per joiner (records the presented incarnation; does
	 * NOT seed — only COMMITTED is a basis).  Best-effort: PREPARE failure does
	 * not block the JOIN_PENDING publish (the COMMITTED marker in Phase-2 is the
	 * commit point that must be majority-durable). */
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		ClusterJoinCommitMarker m;

		if (!dead_bitmap_test_bit(join_bitmap, i))
			continue;
		memset(&m, 0, sizeof(m));
		m.magic = CLUSTER_JCMK_MAGIC;
		m.version = CLUSTER_JCMK_VERSION;
		m.node_id = i;
		m.phase = CLUSTER_JCMK_PHASE_PREPARE;
		m.admitted_incarnation = joiner_incarnations[i];
		m.generation = joiner_incarnations[i]; /* monotonic per node (read-newest intent) */
		m.admitted_epoch = new_epoch;
		cluster_join_marker_compute_crc(&m);
		(void)cluster_reconfig_submit_join_marker(i, &m);
	}

	memset(&evt, 0, sizeof(evt));
	evt.event_id
		= cluster_reconfig_compute_event_id_v2(RECONFIG_KIND_JOIN_PENDING, empty_dead, join_bitmap,
											   joiner_incarnations, cssd_dead_generation);
	evt.coordinator_node_id = coordinator_node_id;
	evt.old_epoch = old_epoch;
	evt.new_epoch = new_epoch;
	memcpy(evt.join_bitmap, join_bitmap, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
	evt.applied_at = GetCurrentTimestamp();
	evt.observer_role = CLUSTER_RECONFIG_OBSERVER_COORDINATOR;
	evt.cssd_dead_generation = cssd_dead_generation;
	evt.reconfig_kind = RECONFIG_KIND_JOIN_PENDING;
	cluster_reconfig_publish_event(&evt);
	pg_atomic_fetch_add_u64(&ReconfigShmem->join_pending_count, 1);
}

bool
cluster_reconfig_commit_member(int32 node_id, uint64 admitted_incarnation)
{
	ClusterJoinCommitMarker m;
	uint64 old_epoch, new_epoch;
	XLogRecPtr lsn;
	ReconfigEvent evt;
	uint8 jb[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	uint8 empty_dead[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	uint64 incs[CLUSTER_MAX_NODES];

	if (!cluster_enabled || ReconfigShmem == NULL)
		return false;
	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return false;
	/*
	 * Hardening v1.0.4 (P2 serialization, defense re-check at the hard-commit point):
	 * resolve the residual race where drive_joins started before a clean leave became
	 * active on this node.  If a clean leave is active now, do NOT commit the join
	 * (which would bump the epoch and wedge the leaver, P2).  Return false so the
	 * coordinator retries next tick once the leave finishes; the joiner stays JOINING.
	 */
	if (cluster_clean_leave_in_progress())
		return false;

	CLUSTER_INJECTION_POINT("cluster-reconfig-join-commit-pre");

	/*
	 * ① COMMITTED marker majority-durable = the commit point (P1-r5).  Written
	 * BEFORE the publish; if it does not reach a disk majority we FAIL CLOSED —
	 * no publish, no MEMBER, no gate-open; the joiner stays JOINING and the next
	 * tick retries (or times out joiner-side -> REJECT).
	 */
	memset(&m, 0, sizeof(m));
	m.magic = CLUSTER_JCMK_MAGIC;
	m.version = CLUSTER_JCMK_VERSION;
	m.node_id = node_id;
	m.phase = CLUSTER_JCMK_PHASE_COMMITTED;
	m.admitted_incarnation = admitted_incarnation;
	m.generation = admitted_incarnation;
	m.admitted_epoch = cluster_epoch_get_current() + 1; /* the epoch we publish below */
	m.supersedes_leave_epoch = cluster_reconfig_get_clean_departed_epoch(node_id);
	/*
	 * INV-J13 (Hardening v1.1): per-attempt nonce so the majority judgement
	 * groups by full commit identity.  Computed ONCE here -> all disks of THIS
	 * attempt share it; a later attempt (>= 1 LMON tick away -> distinct µs
	 * timestamp) or a different coordinator (distinct node_id high bits) gets a
	 * distinct nonce, so two minority writes from different attempts cannot be
	 * mis-counted as one majority (P1-3).  No new shmem field needed.
	 */
	m.commit_nonce = ((uint64)cluster_node_id << 56) ^ (uint64)GetCurrentTimestamp();
	cluster_join_marker_compute_crc(&m);

	if (cluster_reconfig_submit_join_marker(node_id, &m) != CLUSTER_JOIN_MARKER_SUBMIT_ACK) {
		ereport(LOG,
				(errmsg("cluster membership: COMMITTED join marker for node %d did not reach a "
						"voting-disk majority; not committing (will retry)",
						node_id)));
		return false;
	}

	CLUSTER_INJECTION_POINT("cluster-reconfig-join-commit-marker-durable");

	/*
	 * ② publish: bump JOIN_COMMITTED epoch + state MEMBER + last_admitted + clear
	 * pending + clear clean_departed[node] (INV-J10).  Strict order — the publish
	 * (epoch + state MEMBER) precedes the joiner opening its write gate (D5).
	 */
	cluster_epoch_advance_for_reconfig(&old_epoch, &new_epoch);
	lsn = GetXLogInsertRecPtr();
	cluster_epoch_set_changed_at_lsn((uint64)lsn);
	cluster_gcs_block_on_epoch_advance(new_epoch);
	cluster_sinval_reset_all_on_reconfig();
	cluster_tt_status_flush_all((uint32)new_epoch);

	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	cluster_membership_set_state(node_id, CLUSTER_MEMBER_MEMBER);
	cluster_membership_record_admitted(node_id, admitted_incarnation);
	ReconfigShmem->pending_join_bitmap[node_id / 8] &= (uint8) ~(1u << (node_id % 8));
	LWLockRelease(&ReconfigShmem->lock);

	/* INV-J10: clear the in-shmem clean_departed suppression so a clean-left node
	 * that just rejoined has its later real fail-stop honored again (the durable
	 * supersede across restart is resolved by the seed, RC-5). */
	if (cluster_reconfig_is_clean_departed(node_id))
		pg_atomic_fetch_add_u64(&ReconfigShmem->clean_departed_cleared_count, 1);
	cluster_reconfig_clear_clean_departed(node_id);

	dead_bitmap_set_bit(jb, node_id);
	memset(incs, 0, sizeof(incs));
	incs[node_id] = admitted_incarnation;

	memset(&evt, 0, sizeof(evt));
	evt.event_id = cluster_reconfig_compute_event_id_v2(
		RECONFIG_KIND_JOIN_COMMITTED, empty_dead, jb, incs, cluster_cssd_get_dead_generation());
	evt.coordinator_node_id = cluster_node_id;
	evt.old_epoch = old_epoch;
	evt.new_epoch = new_epoch;
	memcpy(evt.join_bitmap, jb, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
	evt.applied_at = GetCurrentTimestamp();
	evt.observer_role = CLUSTER_RECONFIG_OBSERVER_COORDINATOR;
	evt.cssd_dead_generation = cluster_cssd_get_dead_generation();
	evt.reconfig_kind = RECONFIG_KIND_JOIN_COMMITTED;
	cluster_reconfig_publish_event(&evt);
	pg_atomic_fetch_add_u64(&ReconfigShmem->join_apply_count, 1);

	return true;
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
	 * spec-5.13 S6 (CL-I12) — touched-tx drain-grace dispatch by reconfig_kind.
	 * FAIL_STOP: the departed member's volatile state may be torn/lost, so a
	 * survivor tx that touched it MUST abort (40R01, the classify_verdict path
	 * below).  CLEAN_LEAVE: the leaving member flushed all its dirty blocks to
	 * shared storage before the commit, so the data is PRESERVED — a touched
	 * survivor tx is NOT aborted; it continues (drain-grace).  Its reads of any
	 * leaving-node block are gated by the CL-I5 serve-gate
	 * (cluster_clean_leave_block_serve_gate_allows in the GCS block-serve path):
	 * fail-closed (53R62 retry) until the leave commits + the cache invalidates,
	 * then it reads the just-flushed current.  This is the spec-5.14 Q1=B
	 * consumer contract: FAIL_STOP → abort (data lost); CLEAN_LEAVE →
	 * wait-then-continue (data preserved).
	 */
	if (touched && ev.reconfig_kind == RECONFIG_KIND_CLEAN_LEAVE) {
		if (cluster_reconfig_note_clean_leave_drain_grace() == 0)
			ereport(LOG,
					(errmsg("cluster clean-leave: in-flight transactions that touched the "
							"leaving node continue under drain-grace (data preserved); reads of "
							"leaving-node blocks are serve-gated until the leave commits")));
		return; /* ABSORB — drain-grace, NOT 40R01 */
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
	case RECONFIG_KIND_JOIN_PENDING:
		return "join_pending"; /* spec-5.15 D6 */
	case RECONFIG_KIND_JOIN_COMMITTED:
		return "join_committed"; /* spec-5.15 D6 */
	case RECONFIG_KIND_NODE_REMOVED:
		return "node_removed"; /* spec-5.18 D3 */
	case RECONFIG_KIND_NONE:
	default:
		return "none";
	}
}

/* spec-5.15 D6 — render ClusterMembershipState for pg_cluster_membership. */
static const char *
membership_state_to_string(ClusterMembershipState st)
{
	switch (st) {
	case CLUSTER_MEMBER_ABSENT:
		return "absent";
	case CLUSTER_MEMBER_DEAD:
		return "dead";
	case CLUSTER_MEMBER_JOINING:
		return "joining";
	case CLUSTER_MEMBER_MEMBER:
		return "member";
	case CLUSTER_MEMBER_REJECTED:
		return "rejected";
	case CLUSTER_MEMBER_REMOVED:
		return "removed"; /* spec-5.18 D4 */
	default:
		return "unknown";
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


/*
 * spec-5.15 D6 — pg_cluster_membership SRF.  One row per declared node showing
 * the decision-SSOT membership state + the incarnation floor / observed values.
 */
Datum
cluster_get_membership(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo;
	int i;

	InitMaterializedSRF(fcinfo, 0);
	rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

	if (!cluster_enabled)
		return (Datum)0; /* disabled — 0 rows */

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		Datum values[8];
		bool nulls[8];
		uint64 presented = 0;

		if (cluster_conf_lookup_node(i) == NULL)
			continue; /* only declared nodes */

		(void)cluster_reconfig_get_observed_slot(i, &presented, NULL);

		memset(nulls, false, sizeof(nulls));
		values[0] = Int32GetDatum(i);
		values[1] = BoolGetDatum(true); /* declared (only declared rows emitted) */
		values[2] = PointerGetDatum(
			cstring_to_text(membership_state_to_string(cluster_membership_get_state(i))));
		values[3] = Int64GetDatum((int64)presented);
		values[4] = Int64GetDatum((int64)cluster_membership_get_last_admitted_incarnation(i));
		values[5] = Int64GetDatum((int64)cluster_reconfig_get_observed_epoch(i));
		/* spec-5.18 D15: +2 cols — permanently-removed flag + removal epoch. */
		values[6] = BoolGetDatum(cluster_reconfig_is_removed(i));
		values[7] = Int64GetDatum((int64)cluster_reconfig_get_removed_epoch(i));

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
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

int
cluster_reconfig_compute_join_bitmap(uint8 join_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES])
{
	if (join_bitmap != NULL)
		memset(join_bitmap, 0, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
	return 0;
}

uint64
cluster_reconfig_compute_event_id_v2(
	uint8 reconfig_kind pg_attribute_unused(),
	const uint8 dead_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] pg_attribute_unused(),
	const uint8 join_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] pg_attribute_unused(),
	const uint64 joiner_incarnations[CLUSTER_MAX_NODES] pg_attribute_unused(),
	uint64 cssd_dead_generation pg_attribute_unused())
{
	return 0;
}

void
cluster_reconfig_record_observed_slot(int32 node_id pg_attribute_unused(),
									  uint64 incarnation pg_attribute_unused(),
									  uint64 generation pg_attribute_unused(),
									  uint64 epoch pg_attribute_unused())
{}

bool
cluster_reconfig_get_observed_slot(int32 node_id pg_attribute_unused(), uint64 *incarnation,
								   uint64 *generation)
{
	if (incarnation != NULL)
		*incarnation = 0;
	if (generation != NULL)
		*generation = 0;
	return false;
}

uint64
cluster_reconfig_get_observed_epoch(int32 node_id pg_attribute_unused())
{
	return 0;
}

void
cluster_reconfig_record_observed_fresh_alive(int32 node_id pg_attribute_unused(),
											 bool fresh_alive pg_attribute_unused())
{}

bool
cluster_reconfig_get_observed_fresh_alive(int32 node_id pg_attribute_unused())
{
	return false;
}

ClusterJoinMarkerSubmitResult
cluster_reconfig_submit_join_marker(int32 target_node pg_attribute_unused(),
									const ClusterJoinCommitMarker *m pg_attribute_unused())
{
	return CLUSTER_JOIN_MARKER_SUBMIT_FAILED;
}

bool
cluster_reconfig_join_qvotec_poll_pending(int32 *out_target_node,
										  void *out_slot512 pg_attribute_unused())
{
	if (out_target_node != NULL)
		*out_target_node = -1;
	return false;
}

void
cluster_reconfig_join_qvotec_complete(bool acked pg_attribute_unused())
{}

void
cluster_reconfig_publish_join_qvotec_latch(struct Latch *latch pg_attribute_unused())
{}

void
cluster_membership_seed_last_admitted_from_voting_disk(const int *fds pg_attribute_unused(),
													   int n_disks pg_attribute_unused())
{}

void
cluster_reconfig_apply_join_as_coordinator(
	const uint8 join_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] pg_attribute_unused(),
	int32 coordinator_node_id pg_attribute_unused(),
	const uint64 joiner_incarnations[CLUSTER_MAX_NODES] pg_attribute_unused())
{}

bool
cluster_reconfig_commit_member(int32 node_id pg_attribute_unused(),
							   uint64 admitted_incarnation pg_attribute_unused())
{
	return false;
}

ClusterJoinGateVerdict
cluster_reconfig_self_join_gate_verdict(void)
{
	return CLUSTER_JOIN_GATE_ALLOW;
}

void
cluster_reconfig_note_self_admitted(uint64 admitted_epoch pg_attribute_unused())
{}

bool
cluster_reconfig_join_publish_proven(uint64 admitted_epoch pg_attribute_unused())
{
	return false;
}

bool
cluster_reconfig_bootstrap_quorum_at_initial(void)
{
	return false;
}

uint64
cluster_reconfig_get_join_pending_count(void)
{
	return 0;
}
uint64
cluster_reconfig_get_join_apply_count(void)
{
	return 0;
}
uint64
cluster_reconfig_get_join_reject_count(void)
{
	return 0;
}
uint64
cluster_reconfig_get_join_timeout_count(void)
{
	return 0;
}
uint64
cluster_reconfig_get_clean_departed_cleared_count(void)
{
	return 0;
}

#endif /* USE_PGRAC_CLUSTER */
