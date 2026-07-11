/*-------------------------------------------------------------------------
 *
 * cluster_hw_remaster.h
 *	  HW (relation extend) authority online-remaster rebuild (spec-5.7 D3 S5,
 *	  §3.1b R4/R6/R9).
 *
 *	  When a node dies and the GRD remasters its shards to a survivor, the dead
 *	  master's in-memory HW authority HWMs are gone.  Before the survivor may
 *	  serve HW_ALLOC for an adopted (rel,fork) -- the §3.1b R4/R6 serve gate
 *	  (cluster_hw_serve_allowed) keeps it fail-closed until then -- it must
 *	  rebuild those HWMs from durable state:
 *	    1. load the dead master's HW snapshot (cluster_hw_snapshot_read);
 *	    2. replay the dead master's HW_RESERVE WAL tail from the snapshot_lsn,
 *	       so a reservation made after the dead master's last checkpoint (and
 *	       durably flushed before it was replied -- R5) is not lost;
 *	    3. durably write the survivor's own ADOPTION snapshot (R9 lineage), so a
 *	       chained remaster reads one collapsed anchor;
 *	    4. mark the adopted shards rebuilt for the current remaster generation
 *	       (cluster_hw_mark_shard_rebuilt), which opens the serve gate at P7.
 *	  Any step that cannot prove the HWM is fully rebuilt fails closed (BLOCKED):
 *	  the shard stays frozen, HW_ALLOC raises 53RA6, NEVER an auto-create at
 *	  block 0 over an already-allocated range (R6, 8.A).
 *
 *	  Scope: 2-node (a single survivor adopts every dead shard, so the rebuild
 *	  applies the whole dead-master tail; multi-survivor shard-split is forward).
 *	  Driven off the LMON heartbeat tick by a dedicated rebuild worker so the
 *	  potentially large WAL-tail scan never stalls heartbeats; the GRD reconfig
 *	  FSM launches it whenever the HW authority is active and gates P7 on it
 *	  (independent of cluster.online_thread_recovery, which is default-off).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_hw_remaster.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D3, §3.1b R4/R6/R9)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_HW_REMASTER_H
#define CLUSTER_HW_REMASTER_H

#include "cluster/cluster_hw.h" /* ClusterHwRemasterResult */

#define CLUSTER_HW_REMASTER_NO_DEADLINE PG_UINT64_MAX
#define CLUSTER_HW_REMASTER_BACKOFF_CAP_MS 60000

typedef enum ClusterHwRemasterLaunchAction {
	CLUSTER_HW_REMASTER_LAUNCH_SKIP = 0,
	CLUSTER_HW_REMASTER_LAUNCH_INITIAL,
	CLUSTER_HW_REMASTER_LAUNCH_RETRY,
	CLUSTER_HW_REMASTER_LAUNCH_MARK_EXHAUSTED,
	CLUSTER_HW_REMASTER_LAUNCH_MARK_STRUCTURAL,
} ClusterHwRemasterLaunchAction;

typedef struct ClusterHwRemasterRelaunchDecision {
	ClusterHwRemasterLaunchAction action;
	uint32 next_attempts;
	uint64 next_attempt_at;
	ClusterHwRemasterResult next_result;
} ClusterHwRemasterRelaunchDecision;

static inline uint32
cluster_hw_remaster_compute_backoff_ms(int base_ms, uint32 completed_retry_attempts)
{
	uint64 ms;
	uint32 shift;

	if (base_ms < 1)
		base_ms = 1;
	ms = (uint64)base_ms;
	shift = completed_retry_attempts > 0 ? completed_retry_attempts - 1 : 0;
	while (shift-- > 0 && ms < CLUSTER_HW_REMASTER_BACKOFF_CAP_MS)
		ms <<= 1;
	if (ms > CLUSTER_HW_REMASTER_BACKOFF_CAP_MS)
		ms = CLUSTER_HW_REMASTER_BACKOFF_CAP_MS;
	return (uint32)ms;
}

static inline ClusterHwRemasterRelaunchDecision
cluster_hw_remaster_relaunch_decide(uint64 launched_episode, uint64 current_episode,
									ClusterHwRemasterResult result, uint32 attempts,
									uint64 next_attempt_at, uint64 now, int retry_max_attempts)
{
	ClusterHwRemasterRelaunchDecision d;

	d.action = CLUSTER_HW_REMASTER_LAUNCH_SKIP;
	d.next_attempts = attempts;
	d.next_attempt_at = next_attempt_at;
	d.next_result = result;

	if (current_episode == 0)
		return d;
	if (launched_episode != current_episode || result == CLUSTER_HW_REMASTER_NONE) {
		d.action = CLUSTER_HW_REMASTER_LAUNCH_INITIAL;
		d.next_attempts = 0;
		d.next_attempt_at = 0;
		d.next_result = CLUSTER_HW_REMASTER_RUNNING;
		return d;
	}

	if (result == CLUSTER_HW_REMASTER_BLOCKED_STRUCTURAL) {
		if (next_attempt_at != CLUSTER_HW_REMASTER_NO_DEADLINE) {
			d.action = CLUSTER_HW_REMASTER_LAUNCH_MARK_STRUCTURAL;
			d.next_attempt_at = CLUSTER_HW_REMASTER_NO_DEADLINE;
		}
		return d;
	}
	if (result != CLUSTER_HW_REMASTER_BLOCKED)
		return d;

	if (retry_max_attempts <= 0 || attempts >= (uint32)retry_max_attempts) {
		if (next_attempt_at != CLUSTER_HW_REMASTER_NO_DEADLINE) {
			d.action = CLUSTER_HW_REMASTER_LAUNCH_MARK_EXHAUSTED;
			d.next_attempt_at = CLUSTER_HW_REMASTER_NO_DEADLINE;
		}
		return d;
	}
	if (next_attempt_at != CLUSTER_HW_REMASTER_NO_DEADLINE && now < next_attempt_at)
		return d;

	d.action = CLUSTER_HW_REMASTER_LAUNCH_RETRY;
	d.next_attempts = attempts + 1;
	d.next_attempt_at = 0;
	d.next_result = CLUSTER_HW_REMASTER_RUNNING;
	return d;
}

#ifndef FRONTEND

/* Datum (worker entry arg) comes from postgres.h, which every backend caller of
 * this header includes first (mirrors cluster_thread_recovery.h). */

/*
 * cluster_hw_remaster_rebuild_origin -- rebuild this survivor's HW authority for
 * the shards adopted from dead node `dead_node_id`, durably write the adoption
 * snapshot, then (if `episode_epoch` is still the locked reconfig episode) mark
 * the adopted (REBUILDING, self-mastered) shards rebuilt for their current
 * generation (R4/R9 order ①→②③④).  Returns DONE on a proven rebuild, BLOCKED on
 * any fail-closed condition (missing/corrupt dead snapshot, unusable WAL window,
 * WAL gap, or the episode advancing mid-rebuild), NOT_APPLICABLE when the HW
 * authority is not active.  Runs in the dedicated rebuild worker (off the LMON
 * tick).
 */
extern ClusterHwRemasterResult cluster_hw_remaster_rebuild_origin(int dead_node_id,
																  uint64 episode_epoch);

/*
 * cluster_hw_remaster_worker_main -- dynamic-bgworker entry point (main_arg = the
 * dead origin node id).  Captures the live reconfig episode and drives
 * cluster_hw_remaster_rebuild_origin off the LMON tick.  A before_shmem_exit
 * callback records an abnormal exit as retryable BLOCKED so the same episode can
 * self-heal instead of staying pinned on the launch idempotency bit.
 */
extern void cluster_hw_remaster_worker_main(Datum main_arg);

/*
 * cluster_hw_remaster_launch_workers -- the GRD reconfig FSM launch side (S5d):
 * for each dead origin this episode whose shards the HW authority must rebuild,
 * register one per-episode rebuild worker (idempotent via the launched-episode
 * field).  A NO-OP when the HW authority is not active (so a non-HW reconfig is
 * unchanged).  Called each WAIT_CLUSTER tick with the dead-node bitmap + locked
 * episode epoch.
 */
extern void cluster_hw_remaster_launch_workers(const uint64 *dead, int nwords,
											   uint64 episode_epoch);

/*
 * cluster_hw_remaster_gate_unfreeze -- the P7 unfreeze gate (S5d, R4 ④): returns
 * true (STAY FROZEN) while any shard this node masters is still REBUILDING with
 * its HW authority not yet rebuilt for the current generation (hw_rebuilt_
 * generation != master_generation).  Returns false (ready to unfreeze) when the
 * HW authority is inactive (no gating) or every such shard is rebuilt.  The
 * reconfig FSM consults this before P7 so a shard never unfreezes (NORMAL) with
 * an unrebuilt HWM that would auto-create at 0 (R9, 8.A).
 */
extern bool cluster_hw_remaster_gate_unfreeze(void);

#endif /* !FRONTEND */

#endif /* CLUSTER_HW_REMASTER_H */
