/*-------------------------------------------------------------------------
 *
 * cluster_node_remove.c
 *	  pgrac online node leave: fence + cluster-wide cleanup (spec-5.18) —
 *	  runtime driver.
 *
 *	  The survivor-coordinator removal state machine (§3.1) + the three-phase
 *	  durable removal-marker commit (§2.5) + the cluster-wide cleanup_on_exit
 *	  (GRD remaster / GES-PCM clear / verify_no_leftover) + the IC announce/ack
 *	  orchestration + crash-recovery from the durable marker.  The pure decision
 *	  layer (cluster_node_remove_policy.c) owns the phase-FSM validity, precheck
 *	  matrix, INV-LF2/LF7 classifiers, marker validation/authority, and IC
 *	  payload integrity; this file snapshots the live facts and acts on the
 *	  verdicts.
 *
 *	  INV-LF2 (fence-before-shrink): the 4.12 fence marker for the removed node
 *	  is armed (majority-durable, at the new epoch) inside
 *	  cluster_reconfig_apply_node_removed_as_coordinator BEFORE the membership
 *	  shrink is published — exactly like the fail-stop coordinator submits its
 *	  fence before publishing.  A fence-submit failure fail-closes the publish.
 *
 *	  INV-LF7 (crash-safe three-phase commit): REMOVING (pre-bump, not trust) ->
 *	  SHRUNK (post-bump: non-member + fenced, cleanup PENDING) -> REMOVED (only
 *	  after verify_no_leftover + all-survivor ACK).  A coordinator crash resumes
 *	  from the durable marker per the cluster_node_remove_recover_phase matrix.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_node_remove.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster builds.
 *	  Spec: spec-5.18-online-node-leave-fence-cleanup.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/timestamp.h"

#include "cluster/cluster_conf.h"
#include "cluster/cluster_cssd.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_node_remove.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_pgstat.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_xid_stripe_boot.h" /* spec-6.15 D5c retire-before-removal */
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_voting_disk_io.h"
#include "cluster/cluster_write_fence.h"

/* the cluster-wide enable / node id come from cluster_guc.c. */
extern PGDLLIMPORT int cluster_node_id;
extern PGDLLIMPORT bool cluster_enabled;

/* on-slot StaticAssert: the removal marker must fit within the voting-slot
 * _reserved1 region after the 4.12 fence marker, clear of the slot crc32c. */
StaticAssertDecl(CLUSTER_REMOVAL_MARKER_RESERVED1_OFFSET + sizeof(ClusterRemovalMarker)
					 <= sizeof(((ClusterVotingSlot *)0)->_reserved1),
				 "removal marker must fit within voting-slot _reserved1 (after fence marker)");

static ClusterNodeRemoveState *nr_state = NULL;


/* ============================================================
 * shmem region (D2)
 * ============================================================ */

Size
cluster_node_remove_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterNodeRemoveState));
}

void
cluster_node_remove_shmem_init(void)
{
	bool found;

	nr_state = (ClusterNodeRemoveState *)ShmemInitStruct("pgrac cluster node_remove",
														 cluster_node_remove_shmem_size(), &found);
	if (!found) {
		memset(nr_state, 0, sizeof(*nr_state));
		LWLockInitialize(&nr_state->lock, LWTRANCHE_CLUSTER_NODE_REMOVE);
		pg_atomic_init_u32(&nr_state->phase, CLUSTER_REMOVE_IDLE);
		nr_state->target_node_id = -1;
		nr_state->coordinator_node_id = -1;
		pg_atomic_init_u64(&nr_state->removal_request_count, 0);
		pg_atomic_init_u64(&nr_state->removal_committed_count, 0);
		pg_atomic_init_u64(&nr_state->removal_aborted_count, 0);
		pg_atomic_init_u64(&nr_state->removal_escalate_count, 0);
		pg_atomic_init_u64(&nr_state->cleanup_blocked_count, 0);
		pg_atomic_init_u64(&nr_state->leftover_detected_count, 0);
		pg_atomic_init_u64(&nr_state->zombie_write_rejected_count, 0);
		pg_atomic_init_u32(&nr_state->survivor_acked, 0);
		pg_atomic_init_u32(&nr_state->announce_sent, 0);
		nr_state->qvotec_latch = NULL;
		pg_atomic_init_u64(&nr_state->marker_request_seq, 0);
		pg_atomic_init_u64(&nr_state->marker_completion_seq, 0);
		pg_atomic_init_u32(&nr_state->marker_result, CLUSTER_REMOVAL_MARKER_SUBMIT_ACK);
		memset(&nr_state->pending_marker, 0, sizeof(nr_state->pending_marker));
	}
}

static const ClusterShmemRegion cluster_node_remove_region = {
	.name = "pgrac cluster node_remove",
	.size_fn = cluster_node_remove_shmem_size,
	.init_fn = cluster_node_remove_shmem_init,
	.lwlock_count = 1, /* ClusterNodeRemoveState.lock (LWTRANCHE_CLUSTER_NODE_REMOVE) */
	.owner_subsys = "cluster_node_remove",
	.reserved_flags = 0,
};

void
cluster_node_remove_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_node_remove_region);
}

void
cluster_node_remove_get_state(ClusterNodeRemoveState *out)
{
	if (out == NULL)
		return;
	if (nr_state == NULL) {
		memset(out, 0, sizeof(*out));
		out->target_node_id = -1;
		out->coordinator_node_id = -1;
		return;
	}
	LWLockAcquire(&nr_state->lock, LW_SHARED);
	*out = *nr_state;
	LWLockRelease(&nr_state->lock);
}


/* ============================================================
 * §2.5 removal-marker submit mailbox (qvotec-mediated) — mirrors the 4.12
 * fence / 5.13 leave mailbox.  The marker rides THIS node's own self-slot
 * _reserved1[64..] on a quorum-majority of disks.
 * ============================================================ */

static uint64 nr_qvotec_last_processed_marker_seq = 0;
static uint64 nr_qvotec_inflight_marker_seq = 0;

ClusterRemovalMarkerSubmitResult
cluster_node_remove_submit_marker(const ClusterRemovalMarker *m)
{
	uint64 seq;
	Latch *qlatch;
	uint64 deadline_us;
	int wait_ms;

	if (nr_state == NULL || m == NULL)
		return CLUSTER_REMOVAL_MARKER_SUBMIT_FAILED;

	nr_state->pending_marker = *m;
	pg_write_barrier();
	seq = pg_atomic_add_fetch_u64(&nr_state->marker_request_seq, 1);

	qlatch = nr_state->qvotec_latch;
	if (qlatch != NULL)
		SetLatch(qlatch);

	wait_ms = cluster_quorum_poll_interval_ms * 3 + 2000;
	deadline_us = (uint64)GetCurrentTimestamp() + (uint64)wait_ms * 1000ULL;
	for (;;) {
		if (pg_atomic_read_u64(&nr_state->marker_completion_seq) == seq) {
			pg_read_barrier();
			return (ClusterRemovalMarkerSubmitResult)pg_atomic_read_u32(&nr_state->marker_result);
		}
		if ((uint64)GetCurrentTimestamp() >= deadline_us)
			return CLUSTER_REMOVAL_MARKER_SUBMIT_TIMEOUT;
		pg_usleep(2 * 1000); /* 2 ms */
	}
}

bool
cluster_node_remove_qvotec_poll_pending(ClusterRemovalMarker *out)
{
	uint64 req;

	if (nr_state == NULL || out == NULL)
		return false;

	req = pg_atomic_read_u64(&nr_state->marker_request_seq);
	if (req == nr_qvotec_last_processed_marker_seq)
		return false; /* nothing new */

	pg_read_barrier();
	*out = nr_state->pending_marker;
	nr_qvotec_inflight_marker_seq = req;
	return true;
}

void
cluster_node_remove_qvotec_complete(bool acked)
{
	if (nr_state == NULL)
		return;
	pg_atomic_write_u32(&nr_state->marker_result, acked ? CLUSTER_REMOVAL_MARKER_SUBMIT_ACK
														: CLUSTER_REMOVAL_MARKER_SUBMIT_FAILED);
	pg_write_barrier();
	pg_atomic_write_u64(&nr_state->marker_completion_seq, nr_qvotec_inflight_marker_seq);
	nr_qvotec_last_processed_marker_seq = nr_qvotec_inflight_marker_seq;
}

static void
nr_clear_qvotec_latch(int code, Datum arg)
{
	if (nr_state != NULL)
		nr_state->qvotec_latch = NULL;
}

void
cluster_node_remove_publish_qvotec_latch(struct Latch *latch)
{
	if (nr_state == NULL)
		return;
	nr_state->qvotec_latch = latch;
	on_shmem_exit(nr_clear_qvotec_latch, (Datum)0);
}

/* build + submit one durable removal marker; returns true iff majority-durable. */
static bool
nr_write_marker(int phase, int32 node_id, uint64 remove_epoch, uint64 removed_incarnation,
				uint64 removal_event_id)
{
	ClusterRemovalMarker m;

	memset(&m, 0, sizeof(m));
	m.magic = CLUSTER_REMOVAL_MARKER_MAGIC;
	m.version = CLUSTER_REMOVAL_MARKER_VERSION;
	m.phase = (uint16)phase;
	m.removed_node_id = node_id;
	m.remove_epoch = remove_epoch;
	m.removed_incarnation = removed_incarnation;
	m.removal_event_id = removal_event_id;
	cluster_removal_marker_compute_crc(&m);

	return cluster_node_remove_submit_marker(&m) == CLUSTER_REMOVAL_MARKER_SUBMIT_ACK;
}


/* ============================================================
 * INV-LF9 self-demote
 * ============================================================ */

bool
cluster_node_remove_self_is_removed(void)
{
	if (!cluster_enabled || cluster_node_id < 0)
		return false;
	/*
	 * Lock-free hot-path check (called at every writable-xid assignment).  REMOVED
	 * is published into this node's own table by the startup durable-marker rebuild
	 * (rebuild_from_disks) and, for a still-running removed node, by the
	 * NODE_REMOVE_ANNOUNCE handler (nr_announce_handler self-demote).
	 *
	 * HF-2: consult the durable removed_bitmap (lock-free, monotonic) IN ADDITION to
	 * the membership_state[self] byte.  membership_state[self] is rewritten every
	 * LMON tick by the joiner / self-state maintenance paths; although those now
	 * carry a REMOVED terminal guard, the durable bitmap is the authoritative floor
	 * and closes any residual window where a stale self-state write could transiently
	 * un-REMOVE this node and open the 53R64 write gate.  Either signal = removed.
	 * No reconfig lock is taken (mirrors the clean-leave refuse-writes gate).
	 */
	return cluster_membership_get_state(cluster_node_id) == CLUSTER_MEMBER_REMOVED
		   || cluster_reconfig_is_removed_unlocked(cluster_node_id);
}


/* ============================================================
 * INV-LF3 cluster-wide cleanup_on_exit + zero-leftover proof
 * ============================================================ */

bool
cluster_node_remove_verify_no_leftover(int32 node_id)
{
	/* reuse the spec-5.13 leave verifiers (master[] + holder/waiter/convert for
	 * GRD; x_holder/s/pi for PCM) — a removed node must leave zero of either. */
	if (!cluster_grd_clean_leave_verify_no_leftover(node_id))
		return false;
	if (!cluster_pcm_lock_clean_leave_verify_no_leftover(node_id))
		return false;
	return true;
}

bool
cluster_node_remove_run_cleanup(int32 node_id, uint64 remove_epoch)
{
	uint64 nonmember[CLUSTER_MAX_NODES / 64];
	int i;

	/*
	 * Permanent GRD remaster: move every shard mastered by a NON-MEMBER (the
	 * removed node, plus any stale dead/joining masters) to a MEMBER survivor.
	 * Building the "dead" set as (declared - MEMBER) makes the survivor list the
	 * permanent member set (the removed node is excluded for good, §0.3).
	 */
	memset(nonmember, 0, sizeof(nonmember));
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (cluster_conf_lookup_node(i) == NULL)
			continue;
		if (!cluster_membership_is_member(i))
			nonmember[i / 64] |= (UINT64CONST(1) << (i % 64));
	}
	(void)cluster_grd_master_map_remaster(nonmember, remove_epoch);

	/* clear the removed node's GES/PCM leftover (HC27 guarded; also clears the
	 * PCM pending-X requester for the node). */
	cluster_grd_cleanup_on_node_dead(node_id);
	(void)cluster_pcm_lock_clear_pending_x_for_node(node_id);

	/* INV-LF3 proof: zero leftover anywhere. */
	if (!cluster_node_remove_verify_no_leftover(node_id)) {
		pg_atomic_fetch_add_u64(&nr_state->leftover_detected_count, 1);
		return false;
	}
	return true;
}


/* ============================================================
 * precheck input snapshot + request entry (D16 §3.2)
 * ============================================================ */

/* INV-LF4: a node is removable only if it already left (clean-departed) or is
 * down (DEAD / ABSENT membership) — never a live MEMBER (would lose its memory
 * current/dirty with no recovery path). */
static bool
nr_node_is_drained(int32 node_id)
{
	ClusterMembershipState st;

	if (cluster_reconfig_is_clean_departed(node_id))
		return true;
	st = cluster_membership_get_state(node_id);
	return st == CLUSTER_MEMBER_DEAD || st == CLUSTER_MEMBER_ABSENT || st == CLUSTER_MEMBER_REMOVED;
}

/* generate a per-attempt removal identity (R14 folds it into the event id). */
static uint64
nr_make_event_id(int32 node_id)
{
	uint64 ts = (uint64)GetCurrentTimestamp();
	uint64 ctr = pg_atomic_read_u64(&nr_state->removal_request_count);

	return ts ^ ((uint64)(uint32)cluster_node_id << 56) ^ ((uint64)(uint32)node_id << 48) ^ ctr;
}

ClusterRemoveRequestResult
cluster_node_remove_request(int32 node_id)
{
	ClusterRemoveRequestResult verdict;
	bool feature, is_self, declared, drained, in_quorum, cleanup_blocked, drive_active;
	int marker_phase = 0;
	ClusterRemovePhase cur_phase;
	int32 cur_target;

	CLUSTER_INJECTION_POINT("cluster-node-remove-request");

	feature = cluster_online_node_removal;
	is_self = (node_id == cluster_node_id);
	declared = (node_id >= 0 && node_id < CLUSTER_MAX_NODES
				&& cluster_conf_lookup_node(node_id) != NULL);
	drained = declared && nr_node_is_drained(node_id);
	in_quorum = cluster_qvotec_in_quorum();

	cleanup_blocked = false;
	drive_active = false;
	if (nr_state != NULL) {
		LWLockAcquire(&nr_state->lock, LW_SHARED);
		cur_phase = (ClusterRemovePhase)pg_atomic_read_u32(&nr_state->phase);
		cur_target = nr_state->target_node_id;
		LWLockRelease(&nr_state->lock);

		if (cur_target == node_id) {
			if (cur_phase == CLUSTER_REMOVE_COMMITTED)
				marker_phase = CLUSTER_REMOVAL_MARKER_REMOVED;
			else if (cur_phase == CLUSTER_REMOVE_CLEANUP_BLOCKED)
				cleanup_blocked = true;
			else if (cur_phase == CLUSTER_REMOVE_CLEANUP)
				marker_phase = CLUSTER_REMOVAL_MARKER_SHRUNK;
			else if (cur_phase != CLUSTER_REMOVE_IDLE)
				drive_active = true;
		} else if (cur_target >= 0 && cur_phase != CLUSTER_REMOVE_IDLE
				   && cur_phase != CLUSTER_REMOVE_COMMITTED && cur_phase != CLUSTER_REMOVE_ABORTED
				   && cur_phase != CLUSTER_REMOVE_ABORTED_ESCALATE) {
			drive_active = true; /* a different removal is in progress */
		}
	}
	/* durable already-removed (no active state) -> already_removed. */
	if (marker_phase == 0 && !cleanup_blocked && declared && cluster_reconfig_is_removed(node_id)
		&& cluster_membership_get_state(node_id) == CLUSTER_MEMBER_REMOVED)
		marker_phase = CLUSTER_REMOVAL_MARKER_REMOVED;

	verdict = cluster_node_remove_precheck(feature, is_self, declared, drained, in_quorum,
										   marker_phase, cleanup_blocked, drive_active);

	if (verdict != CLUSTER_REMOVE_REQ_ACCEPTED && verdict != CLUSTER_REMOVE_REQ_RESUME)
		return verdict;

	/* reserve / re-drive: record the target + a fresh attempt id, enter REQUESTED.
	 * The coordinator's lmon_tick drives the phases (IC sends are LMON-only). */
	if (nr_state == NULL)
		return CLUSTER_REMOVE_REQ_FEATURE_DISABLED;

	LWLockAcquire(&nr_state->lock, LW_EXCLUSIVE);
	cur_phase = (ClusterRemovePhase)pg_atomic_read_u32(&nr_state->phase);
	if (verdict == CLUSTER_REMOVE_REQ_ACCEPTED
		&& (cur_phase == CLUSTER_REMOVE_IDLE || cur_phase == CLUSTER_REMOVE_COMMITTED
			|| cur_phase == CLUSTER_REMOVE_ABORTED
			|| cur_phase == CLUSTER_REMOVE_ABORTED_ESCALATE)) {
		nr_state->target_node_id = node_id;
		nr_state->coordinator_node_id = cluster_node_id;
		nr_state->remove_epoch = 0;
		nr_state->removal_event_id = nr_make_event_id(node_id);
		nr_state->target_last_incarnation
			= cluster_membership_get_last_admitted_incarnation(node_id);
		nr_state->remove_baseline_dead_gen = cluster_cssd_get_dead_generation();
		nr_state->fence_armed = false;
		nr_state->membership_shrunk = false;
		nr_state->grd_cleaned = false;
		nr_state->pcm_cleaned = false;
		memset(nr_state->ack_bitmap, 0, sizeof(nr_state->ack_bitmap));
		nr_state->cleanup_deadline_us = 0;
		pg_atomic_write_u32(&nr_state->survivor_acked, 0);
		pg_atomic_write_u32(&nr_state->announce_sent, 0);
		pg_atomic_write_u32(&nr_state->phase, CLUSTER_REMOVE_REQUESTED);
		pg_atomic_fetch_add_u64(&nr_state->removal_request_count, 1);
	}
	/*
	 * HF-5: ACCEPTED at the lock-free precheck but the phase advanced out of the
	 * reservable set between the precheck and this exclusive section — a concurrent
	 * request reserved first.  Do NOT return a stale ACCEPTED without actually
	 * reserving (the caller would believe its target is being removed when it is
	 * not); downgrade to removal_in_progress so it retries.
	 */
	else if (verdict == CLUSTER_REMOVE_REQ_ACCEPTED) {
		verdict = CLUSTER_REMOVE_REQ_IN_PROGRESS;
	}
	/* RESUME: keep the existing target/epoch; just re-arm the drive from SHRUNK by
	 * moving CLEANUP_BLOCKED back to CLEANUP (the lmon_tick retries the cleanup). */
	else if (verdict == CLUSTER_REMOVE_REQ_RESUME && cur_phase == CLUSTER_REMOVE_CLEANUP_BLOCKED) {
		pg_atomic_write_u32(&nr_state->phase, CLUSTER_REMOVE_CLEANUP);
	}
	LWLockRelease(&nr_state->lock);

	return verdict;
}


/* ============================================================
 * coordinator phase state machine (drive) — called from lmon_tick on the
 * coordinator survivor.
 * ============================================================ */

/* all MEMBER survivors except self must have ACKed the cleanup (2-node: none). */
static bool
nr_all_survivors_acked(void)
{
	int i;

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (i == cluster_node_id)
			continue;
		if (cluster_conf_lookup_node(i) == NULL)
			continue;
		if (!cluster_membership_is_member(i))
			continue; /* not a survivor expected to ACK */
		if ((nr_state->ack_bitmap[i / 8] & (uint8)(1u << (i % 8))) == 0)
			return false;
	}
	return true;
}

void
cluster_node_remove_drive(void)
{
	ClusterRemovePhase phase;
	int32 node_id;
	uint64 removal_event_id;
	uint64 last_incarnation;
	uint64 baseline_dead_gen;

	if (nr_state == NULL || !cluster_enabled)
		return;

	LWLockAcquire(&nr_state->lock, LW_SHARED);
	phase = (ClusterRemovePhase)pg_atomic_read_u32(&nr_state->phase);
	node_id = nr_state->target_node_id;
	removal_event_id = nr_state->removal_event_id;
	last_incarnation = nr_state->target_last_incarnation;
	baseline_dead_gen = nr_state->remove_baseline_dead_gen;
	LWLockRelease(&nr_state->lock);

	/* only the coordinator drives; nothing to do when idle / terminal. */
	if (node_id < 0 || nr_state->coordinator_node_id != cluster_node_id)
		return;

	switch (phase) {
	case CLUSTER_REMOVE_REQUESTED:
		pg_atomic_write_u32(&nr_state->phase, CLUSTER_REMOVE_PRECHECK);
		/* fall through to re-validate on the same tick */
		/* FALLTHROUGH */
	case CLUSTER_REMOVE_PRECHECK: {
		CLUSTER_INJECTION_POINT("cluster-node-remove-precheck");
		/* re-validate (a node may have come back ALIVE, or quorum lost). */
		if (!cluster_qvotec_in_quorum() || !nr_node_is_drained(node_id)) {
			pg_atomic_write_u32(&nr_state->phase, CLUSTER_REMOVE_ABORTED);
			pg_atomic_fetch_add_u64(&nr_state->removal_aborted_count, 1);
			nr_state->target_node_id = -1;
			return;
		}
		pg_atomic_write_u32(&nr_state->phase, CLUSTER_REMOVE_FENCE_ARMING);
	}
		/* FALLTHROUGH */
	case CLUSTER_REMOVE_FENCE_ARMING: {
		uint64 baseline_epoch = cluster_epoch_get_current();
		uint64 new_epoch;
		bool contest = false;

		(void)baseline_dead_gen; /* contest is now signalled by out_contest, not derived */

		/* §2.5: durable REMOVING marker (pre-commit; not a trust source). */
		(void)nr_write_marker(CLUSTER_REMOVAL_MARKER_REMOVING, node_id, baseline_epoch,
							  last_incarnation, removal_event_id);

		/*
		 * spec-6.15 D5c (appendix B.3): durably retire the removed node's
		 * xid stripe slot BEFORE the removal point of no return below.
		 * Ordering is the identity-reuse defence: once removal commits, a
		 * later fresh join of the same node_id (new incarnation = a new
		 * durable owner) must land on a retired slot and refuse (53RB1),
		 * never resume the old owner's congruence class.  Not durable yet
		 * -> stay FENCE_ARMING and retry next tick (fail-closed; same
		 * shape as a transient fence/quorum failure).  A never-activated
		 * cluster returns true (nothing to retire).
		 */
		if (!cluster_xid_stripe_submit_retire(node_id, last_incarnation)) {
			ereport(LOG, (errmsg("cluster node removal: stripe slot retire for node %d not durable "
								 "yet — retrying before the removal commit",
								 node_id)));
			return;
		}

		/*
		 * The commit point: guarded epoch bump + fence-arm (majority-durable, at
		 * the new epoch, INV-LF2) + publish membership shrink + record removed +
		 * shrink membership_state to REMOVED.  Returns 0 either because the guarded
		 * advance LOST (another node moved the epoch = a real contest, out_contest
		 * set) or because the fence submit did not reach majority (transient, NOT a
		 * contest) — both fail-closed (nothing published on 0).
		 */
		new_epoch = cluster_reconfig_apply_node_removed_as_coordinator(
			node_id, baseline_epoch, removal_event_id, last_incarnation, &contest);
		if (new_epoch == 0) {
			if (contest) {
				/* a real death/contest intruded BEFORE the membership commit
				 * (pre-SHRUNK) -> ABORTED_ESCALATE (hand to fail-stop 5.14).  Routed
				 * through classify_contest for the pre/post-SHRUNK discipline (here
				 * always pre-SHRUNK: membership not yet shrunk). */
				ClusterRemovePhase next
					= cluster_node_remove_classify_contest(nr_state->membership_shrunk);

				CLUSTER_INJECTION_POINT("cluster-node-remove-escalate");
				pg_atomic_write_u32(&nr_state->phase, next);
				if (next == CLUSTER_REMOVE_ABORTED_ESCALATE) {
					pg_atomic_fetch_add_u64(&nr_state->removal_escalate_count, 1);
					nr_state->target_node_id = -1;
				}
			}
			/* else: transient fence/quorum failure -> stay FENCE_ARMING, retry next
			 * tick (the epoch may have self-bumped; the next baseline re-reads it). */
			return;
		}

		LWLockAcquire(&nr_state->lock, LW_EXCLUSIVE);
		nr_state->remove_epoch = new_epoch;
		nr_state->fence_armed = true;
		nr_state->membership_shrunk = true; /* committed: N is now a fenced non-member */
		nr_state->cleanup_deadline_us
			= (TimestampTz)((uint64)GetCurrentTimestamp()
							+ (uint64)cluster_node_removal_cleanup_timeout_ms * 1000ULL);
		LWLockRelease(&nr_state->lock);

		/* membership is committed-shrunk: advance to SHRINK_COMMITTING, which writes
		 * the durable SHRUNK marker.  apply() is NOT called again past this point, so
		 * a later marker/cleanup failure can never re-escalate a committed removal. */
		pg_atomic_write_u32(&nr_state->phase, CLUSTER_REMOVE_SHRINK_COMMITTING);
	}
		/* FALLTHROUGH */
	case CLUSTER_REMOVE_SHRINK_COMMITTING: {
		uint64 epoch;

		LWLockAcquire(&nr_state->lock, LW_SHARED);
		epoch = nr_state->remove_epoch;
		LWLockRelease(&nr_state->lock);

		/* §2.5: durable SHRUNK marker (post-bump: non-member + fenced, cleanup
		 * PENDING).  If it does not reach majority, stay in SHRINK_COMMITTING and
		 * retry (do NOT report COMMITTED, do NOT re-run the commit); the membership
		 * is already shrunk + fenced, so this only gates the crash-recovery trust
		 * source. */
		if (!nr_write_marker(CLUSTER_REMOVAL_MARKER_SHRUNK, node_id, epoch, last_incarnation,
							 removal_event_id))
			return; /* retry SHRUNK marker next tick */

		CLUSTER_INJECTION_POINT("cluster-node-remove-shrink-committed");
		pg_atomic_write_u32(&nr_state->phase, CLUSTER_REMOVE_CLEANUP);
	}
		/* FALLTHROUGH */
	case CLUSTER_REMOVE_CLEANUP:
	case CLUSTER_REMOVE_CLEANUP_BLOCKED: {
		uint64 epoch;
		bool clean;

		LWLockAcquire(&nr_state->lock, LW_SHARED);
		epoch = nr_state->remove_epoch;
		LWLockRelease(&nr_state->lock);

		/* idempotent cluster-wide cleanup + zero-leftover proof. */
		clean = cluster_node_remove_run_cleanup(node_id, epoch);
		if (clean) {
			LWLockAcquire(&nr_state->lock, LW_EXCLUSIVE);
			nr_state->grd_cleaned = true;
			nr_state->pcm_cleaned = true;
			LWLockRelease(&nr_state->lock);
			pg_atomic_write_u32(&nr_state->survivor_acked, 1); /* self ACK */
		}

		/* all-survivor ACK barrier (2-node: only self, satisfied immediately). */
		if (!clean || !nr_all_survivors_acked()) {
			/* post-SHRUNK leftover / missing ACK past the deadline -> CLEANUP_BLOCKED
			 * (resumable, fail-closed; NEVER COMMITTED, NEVER escalate — INV-LF3). */
			if (nr_state->cleanup_deadline_us != 0
				&& (uint64)GetCurrentTimestamp() >= (uint64)nr_state->cleanup_deadline_us) {
				if (phase != CLUSTER_REMOVE_CLEANUP_BLOCKED) {
					pg_atomic_write_u32(&nr_state->phase, CLUSTER_REMOVE_CLEANUP_BLOCKED);
					pg_atomic_fetch_add_u64(&nr_state->cleanup_blocked_count, 1);
					ereport(LOG,
							(errmsg("cluster node removal: cleanup of node %d blocked (leftover or "
									"missing survivor ACK past deadline); removal is committed "
									"(fenced + non-member), cleanup will resume",
									node_id)));
				}
			}
			return; /* retry cleanup next tick */
		}

		CLUSTER_INJECTION_POINT("cluster-node-remove-cleanup-done");

		/* §2.5: REMOVED marker (the final trust source) only after verify + all-ACK. */
		if (!nr_write_marker(CLUSTER_REMOVAL_MARKER_REMOVED, node_id, epoch, last_incarnation,
							 removal_event_id))
			return; /* retry REMOVED marker next tick (do NOT report COMMITTED) */

		pg_atomic_write_u32(&nr_state->phase, CLUSTER_REMOVE_COMMITTED);
		pg_atomic_fetch_add_u64(&nr_state->removal_committed_count, 1);
		return;
	}
	default:
		return; /* IDLE / COMMITTED / ABORTED* — nothing to drive */
	}
}


/* ============================================================
 * LMON orchestration + survivor ACK + crash-recovery resume
 * ============================================================ */

void
cluster_node_remove_survivor_ack(int32 target_node_id, uint64 remove_epoch)
{
	uint64 removal_event_id;
	uint64 removed_incarnation;
	int32 coordinator;

	/*
	 * HF-1/HF-3 (INV-LF11): a non-coordinator survivor APPLIES the removal
	 * locally — not just drops its N-refs.  It seeds the durable removed set +
	 * membership_state[N]=REMOVED (so its own removed_bitmap carries N for any
	 * fence baseline it later publishes, INV-LF10), permanently remasters
	 * N-mastered shards onto a MEMBER survivor, clears N's GES/PCM, and PROVES
	 * zero leftover — and only ACKs once verify passes.  The coordinator's final
	 * REMOVED marker (the trust source) is built on "local verify + all-survivor
	 * ACK", so an ACK must mean THIS survivor is genuinely clean, not merely
	 * "dropped some refs".  Idempotent: the survivor lmon_tick re-runs it every
	 * tick until it converges (the announce is one-shot, so a transient leftover
	 * must be retried locally, not re-announced).
	 */
	if (nr_state == NULL || target_node_id < 0 || target_node_id >= CLUSTER_MAX_NODES)
		return;

	/* identity for the seed + ACK = THIS attempt (recorded by the announce
	 * handler / lmon_tick adopt path, HF-4). */
	LWLockAcquire(&nr_state->lock, LW_SHARED);
	removal_event_id = nr_state->removal_event_id;
	removed_incarnation = nr_state->target_last_incarnation;
	coordinator = nr_state->coordinator_node_id;
	LWLockRelease(&nr_state->lock);

	/* seed the durable removed set + membership REMOVED with the coordinator's
	 * pinned incarnation floor (carried in the announce, HF-1). */
	cluster_reconfig_seed_removed_membership(target_node_id, remove_epoch, removed_incarnation,
											 /*raise_epoch_floor*/ true);

	/* full cluster-wide cleanup on THIS survivor + zero-leftover proof (HF-3).
	 * run_cleanup bumps leftover_detected_count + returns false when not clean. */
	if (!cluster_node_remove_run_cleanup(target_node_id, remove_epoch)) {
		pg_atomic_write_u32(&nr_state->survivor_acked, 0);
		return; /* leftover -> retry next survivor tick, do NOT ACK */
	}

	/* clean: ACK with THIS attempt's identity so the coordinator's barrier keys on
	 * this removal, not a stale prior one. */
	cluster_node_remove_ic_send_ack(coordinator, target_node_id, remove_epoch, removal_event_id);
	pg_atomic_write_u32(&nr_state->survivor_acked, 1);
}

void
cluster_node_remove_lmon_tick(void)
{
	ClusterRemovePhase phase;
	int32 node_id;
	int32 coordinator;
	uint64 remove_epoch;

	if (nr_state == NULL || !cluster_enabled || !cluster_online_node_removal)
		return;
	if (!cluster_qvotec_in_quorum())
		return; /* only an in-quorum survivor participates */

	LWLockAcquire(&nr_state->lock, LW_SHARED);
	phase = (ClusterRemovePhase)pg_atomic_read_u32(&nr_state->phase);
	node_id = nr_state->target_node_id;
	coordinator = nr_state->coordinator_node_id;
	remove_epoch = nr_state->remove_epoch;
	LWLockRelease(&nr_state->lock);

	if (node_id < 0)
		return;

	if (coordinator == cluster_node_id) {
		/* coordinator: broadcast the announce once (post-shrink) so other survivors
		 * drop their refs + ACK, then drive the phase machine. */
		if (phase >= CLUSTER_REMOVE_CLEANUP && phase <= CLUSTER_REMOVE_CLEANUP_BLOCKED
			&& pg_atomic_read_u32(&nr_state->announce_sent) == 0) {
			cluster_node_remove_ic_broadcast_announce(node_id, nr_state->remove_epoch,
													  nr_state->removal_event_id,
													  nr_state->target_last_incarnation);
			pg_atomic_write_u32(&nr_state->announce_sent, 1);
		}
		cluster_node_remove_drive();
	} else {
		/*
		 * HF-1/HF-3 (INV-LF11): survivor side — (re)apply the recorded removal
		 * locally + ACK; retry each tick until verify converges.  The announce is
		 * one-shot, so a transient leftover (or an announce that arrived before the
		 * GRD/PCM state was settleable) must be retried here, not re-announced.
		 */
		if (pg_atomic_read_u32(&nr_state->survivor_acked) == 0)
			cluster_node_remove_survivor_ack(node_id, remove_epoch);
	}
}


/* ============================================================
 * IC wire (D10): NODE_REMOVE_ANNOUNCE (broadcast) + REMOVE_CLEANUP_ACK (p2p).
 * ============================================================ */

/* survivor side: a coordinator announced a removal — apply it locally + ACK. */
static void
nr_announce_handler(const ClusterICEnvelope *env, const void *payload)
{
	const ClusterNodeRemoveAnnouncePayload *p = (const ClusterNodeRemoveAnnouncePayload *)payload;

	if (nr_state == NULL || !cluster_node_remove_announce_payload_valid(p))
		return;
	if (p->target_node_id == cluster_node_id) {
		/*
		 * INV-LF9 self-demote: THIS node is the one being removed.  Publish REMOVED
		 * into our own membership table (lock-free SSOT for the self-demote write
		 * gate) + the durable removed set so a still-running removed node fail-closes
		 * new writable transactions (53R64) instead of serving as a phantom member.
		 * HF-1: pin the coordinator's incarnation floor from the announce so a future
		 * re-admit must present a strictly newer incarnation.  Do NOT send a cleanup
		 * ACK — a removed node is not a survivor.
		 */
		cluster_reconfig_seed_removed_membership(cluster_node_id, p->remove_epoch,
												 p->removed_incarnation,
												 /*raise_epoch_floor*/ false);
		return;
	}

	/*
	 * HF-4: adopt THIS removal attempt's identity when our recorded one is absent,
	 * terminal, or a different attempt — a survivor never runs the driver's
	 * abort/commit resets, so a prior attempt's identity would otherwise linger and
	 * get this attempt's ACK rejected by the coordinator (event_id mismatch),
	 * wedging the next removal's cleanup barrier.  Same event_id = an idempotent
	 * re-announce: keep progress (do not reset survivor_acked).
	 */
	LWLockAcquire(&nr_state->lock, LW_EXCLUSIVE);
	if (nr_state->removal_event_id != p->removal_event_id
		|| nr_state->target_node_id != p->target_node_id) {
		nr_state->target_node_id = p->target_node_id;
		nr_state->coordinator_node_id = p->coordinator_node_id;
		nr_state->remove_epoch = p->remove_epoch;
		nr_state->removal_event_id = p->removal_event_id;
		nr_state->target_last_incarnation = p->removed_incarnation;
		pg_atomic_write_u32(&nr_state->survivor_acked, 0); /* re-apply for the new attempt */
	}
	LWLockRelease(&nr_state->lock);

	/* INV-LF11: apply the removal locally + ACK when clean (the survivor lmon_tick
	 * retries until verify converges). */
	cluster_node_remove_survivor_ack(p->target_node_id, p->remove_epoch);
}

/* coordinator side: a survivor ACKed its cleanup — set its bit in the barrier. */
static void
nr_ack_handler(const ClusterICEnvelope *env, const void *payload)
{
	const ClusterNodeRemoveCleanupAckPayload *p
		= (const ClusterNodeRemoveCleanupAckPayload *)payload;

	if (nr_state == NULL || !cluster_node_remove_ack_payload_valid(p))
		return;
	if (p->survivor_node_id < 0 || p->survivor_node_id >= CLUSTER_NODE_REMOVE_ACK_BITMAP_BYTES * 8)
		return;

	/*
	 * HF-4: an ACK counts toward the barrier only for THIS exact removal attempt —
	 * validate the full identity tuple (target, epoch, event_id) under the lock, not
	 * just event_id, so a stale ACK from a prior attempt can never satisfy the
	 * current barrier (and the snapshot is consistent with the bitmap write).
	 */
	LWLockAcquire(&nr_state->lock, LW_EXCLUSIVE);
	if (p->removal_event_id == nr_state->removal_event_id
		&& p->target_node_id == nr_state->target_node_id
		&& p->remove_epoch == nr_state->remove_epoch)
		nr_state->ack_bitmap[p->survivor_node_id / 8] |= (uint8)(1u << (p->survivor_node_id % 8));
	LWLockRelease(&nr_state->lock);
}

void
cluster_node_remove_register_ic_msg_types(void)
{
	const ClusterICMsgTypeInfo announce_info = {
		.msg_type = PGRAC_IC_MSG_NODE_REMOVE_ANNOUNCE,
		.name = "node_remove_announce",
		.allowed_producer_mask = CLUSTER_IC_PRODUCER_LMON,
		.broadcast_ok = true,
		.handler = nr_announce_handler,
	};
	const ClusterICMsgTypeInfo ack_info = {
		.msg_type = PGRAC_IC_MSG_REMOVE_CLEANUP_ACK,
		.name = "remove_cleanup_ack",
		.allowed_producer_mask = CLUSTER_IC_PRODUCER_LMON,
		.broadcast_ok = false,
		.handler = nr_ack_handler,
	};

	cluster_ic_register_msg_type(&announce_info);
	cluster_ic_register_msg_type(&ack_info);
}

void
cluster_node_remove_ic_broadcast_announce(int32 target_node_id, uint64 remove_epoch,
										  uint64 removal_event_id, uint64 removed_incarnation)
{
	ClusterNodeRemoveAnnouncePayload p;
	ClusterICFanoutResult per_peer[CLUSTER_MAX_NODES];

	memset(&p, 0, sizeof(p));
	p.magic = CLUSTER_NODE_REMOVE_IC_MAGIC;
	p.version = CLUSTER_NODE_REMOVE_IC_VERSION;
	p.coordinator_node_id = cluster_node_id;
	p.target_node_id = target_node_id;
	p.remove_epoch = remove_epoch;
	p.removal_event_id = removal_event_id;
	p.removed_incarnation = removed_incarnation; /* HF-1: incarnation floor for survivor seed */
	cluster_node_remove_announce_compute_crc(&p);

	cluster_ic_send_envelope_fanout(PGRAC_IC_MSG_NODE_REMOVE_ANNOUNCE, &p, (uint32)sizeof(p),
									per_peer);
}

void
cluster_node_remove_ic_send_ack(int32 dest_node_id, int32 target_node_id, uint64 remove_epoch,
								uint64 removal_event_id)
{
	ClusterNodeRemoveCleanupAckPayload p;

	if (dest_node_id < 0)
		return;

	memset(&p, 0, sizeof(p));
	p.magic = CLUSTER_NODE_REMOVE_IC_MAGIC;
	p.version = CLUSTER_NODE_REMOVE_IC_VERSION;
	p.survivor_node_id = cluster_node_id;
	p.target_node_id = target_node_id;
	p.remove_epoch = remove_epoch;
	p.removal_event_id = removal_event_id;
	cluster_node_remove_ack_compute_crc(&p);

	(void)cluster_ic_send_envelope(PGRAC_IC_MSG_REMOVE_CLEANUP_ACK, dest_node_id, &p,
								   (uint32)sizeof(p));
}


/* ============================================================
 * INV-LF2 fence-arm helper (exposed for tests / external callers)
 * ============================================================ */

bool
cluster_node_remove_arm_fence(int32 node_id, uint64 remove_epoch)
{
	/*
	 * The production fence-arm is folded into the commit point
	 * (cluster_reconfig_apply_node_removed_as_coordinator) so it is atomic with
	 * the guarded epoch bump (the fence rides the NEW epoch, baseline-protected).
	 * This wrapper reports whether the durable fence authority now lists node_id
	 * as fenced (used by acceptance assertions); it does not itself submit.
	 */
	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return false;
	return cluster_write_fence_verify_durable(remove_epoch);
}


/* ============================================================
 * crash-recovery from the durable §2.5 marker (INV-LF7)
 * ============================================================ */

void
cluster_node_remove_rebuild_from_disks(const int *fds, int n_disks)
{
	int slot;
	int i;

	if (nr_state == NULL || fds == NULL || n_disks <= 0)
		return;

	/*
	 * Scan every node's self-slot _reserved1[64..] across the voting disks.  A
	 * struct-valid removal marker that reaches a majority (identical tuple) is the
	 * durable trust source.  Per the INV-LF7 fence x removal matrix: SHRUNK/REMOVED
	 * seed removed_bitmap + raise the epoch floor; SHRUNK additionally enqueues a
	 * cleanup resume (never report COMMITTED until REMOVED is written).
	 */
	for (slot = 0; slot < CLUSTER_MAX_NODES; slot++) {
		ClusterRemovalMarker per_disk[CLUSTER_MAX_VOTING_DISKS];
		bool has[CLUSTER_MAX_VOTING_DISKS];
		ClusterRemovalMarker authority;
		bool fence_says_fenced;
		bool corrupt = false;
		ClusterRemovePhase resume;
		int32 rn; /* the REMOVED node named by the marker (NOT the slot index — a
				   * removal marker lives in the COORDINATOR's slot and names the
				   * departed node, mirroring the clean-leave marker). */

		if (cluster_conf_lookup_node(slot) == NULL)
			continue;

		for (i = 0; i < n_disks && i < CLUSTER_MAX_VOTING_DISKS; i++) {
			ClusterVotingSlot vs;

			has[i] = false;
			if (cluster_voting_disk_read_slot(fds[i], i, (uint32)slot, &vs)
				!= CLUSTER_VOTING_DISK_IO_OK)
				continue;
			if (cluster_removal_marker_unpack(vs._reserved1, &per_disk[i]))
				has[i] = true;
		}

		if (!cluster_removal_marker_authority_decide(
				per_disk, has,
				(n_disks < CLUSTER_MAX_VOTING_DISKS ? n_disks : CLUSTER_MAX_VOTING_DISKS),
				&authority))
			continue; /* no majority removal marker on this slot */
		/* structural-only validation (magic/version/crc/phase/range); the marker's
		 * removed_node_id is the node to seed, not the slot it physically lives in. */
		if (!cluster_removal_marker_struct_valid(&authority, -1))
			continue;
		rn = authority.removed_node_id;
		if (cluster_conf_lookup_node(rn) == NULL)
			continue; /* removed node must be declared */

		/*
		 * INV-LF7 at restart: a majority SHRUNK/REMOVED removal marker is itself the
		 * proof the removal committed (and INV-LF2 guaranteed the fence was durable
		 * AT commit time).  The 4.12 fence on this coordinator's own slot is cleared
		 * on clean shutdown and re-established by the cluster-wide baseline; at the
		 * instant rebuild runs that baseline may not have re-fired, so we do NOT gate
		 * recovery on a fresh fence read — we trust the majority removal marker and
		 * re-fence by seeding removed_bitmap (every later fence marker then carries
		 * the removed node, INV-LF10).  fence_says_fenced is advisory only.
		 */
		fence_says_fenced = cluster_write_fence_verify_durable(authority.remove_epoch);
		resume = cluster_node_remove_recover_phase(true /* trust the durable marker */,
												   authority.phase, &corrupt);
		(void)fence_says_fenced;
		if (corrupt)
			continue; /* unknown marker phase — ignore (struct_valid already vetted phase) */

		if (authority.phase == CLUSTER_REMOVAL_MARKER_SHRUNK
			|| authority.phase == CLUSTER_REMOVAL_MARKER_REMOVED) {
			/* durable: the node is a non-member; seed the removed set + floor +
			 * membership_state=REMOVED (under the reconfig lock, via the helper).
			 * The seeded removed_bitmap makes the next baseline re-fence the node. */
			cluster_reconfig_seed_removed_membership(rn, authority.remove_epoch,
													 authority.removed_incarnation,
													 /*raise_epoch_floor*/ true);

			/* SHRUNK -> resume cleanup (do not report COMMITTED); REMOVED -> trusted. */
			LWLockAcquire(&nr_state->lock, LW_EXCLUSIVE);
			nr_state->target_node_id = rn;
			nr_state->coordinator_node_id = cluster_node_id;
			nr_state->remove_epoch = authority.remove_epoch;
			nr_state->removal_event_id = authority.removal_event_id;
			nr_state->target_last_incarnation = authority.removed_incarnation;
			nr_state->fence_armed = true;
			nr_state->membership_shrunk = true;
			nr_state->cleanup_deadline_us
				= (TimestampTz)((uint64)GetCurrentTimestamp()
								+ (uint64)cluster_node_removal_cleanup_timeout_ms * 1000ULL);
			pg_atomic_write_u32(&nr_state->phase, (resume == CLUSTER_REMOVE_COMMITTED)
													  ? CLUSTER_REMOVE_COMMITTED
													  : CLUSTER_REMOVE_CLEANUP);
			LWLockRelease(&nr_state->lock);
		}
	}
}
