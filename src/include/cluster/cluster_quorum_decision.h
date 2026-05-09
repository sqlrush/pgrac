/*-------------------------------------------------------------------------
 *
 * cluster_quorum_decision.h
 *	  Pure-logic majority view + collision detection — spec-2.6 D4.
 *
 *	  Step 2 D4.  Stateless function family that takes a per-(disk,
 *	  node) slot matrix collected by Step 2 D3 cluster_voting_disk_io
 *	  and returns a ClusterQuorumDecision struct holding:
 *	    - quorum_state (OK / UNCERTAIN / LOST)
 *	    - alive_bitmap (per-instance alive view)
 *	    - epoch_max (largest current_epoch observed across alive slots —
 *	      used by boot-time epoch recovery per spec-2.0 §3 R10)
 *	    - collision_state (Q6 v0.2 newer-self-FATAL flag)
 *
 *	  Functions in this header are PURE — no shmem, no I/O, no logging,
 *	  no GUC reads.  Callers (cluster_qvotec poll cycle) are responsible
 *	  for collecting the slot matrix, invoking decide_quorum_view, and
 *	  acting on the returned ClusterQuorumDecision.
 *
 *	  Q4 v0.2 lease semantics live in cluster_qvotec.c (writes the
 *	  decision into shmem with lease_expire_at_us);this module just
 *	  computes the decision.
 *
 *	  Q6 v0.2 collision direction:
 *	    - self.incarnation > slot.incarnation → newer-self-FATAL
 *	      (let the older serving instance keep its in-progress
 *	      transactions / cached buffers)
 *	    - self.incarnation < slot.incarnation → OBSERVED_OLDER
 *	      (the other instance is the newer comer and should self-
 *	      FATAL on its own quorum poll;we continue;UNCERTAIN until
 *	      the other side exits)
 *	    - self.incarnation == slot.incarnation → NONE (no collision)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_quorum_decision.h
 *
 * NOTES
 *	  pgrac-original file.  Compiled only in --enable-cluster mode.
 *
 *	  Spec authority: pgrac:specs/spec-2.6-voting-disk-quorum-lite.md
 *	  (frozen v0.2 2026-05-09;§3.1 poll cycle decide_quorum + §3.4
 *	  Q6 v0.2 collision newer-self-FATAL).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_QUORUM_DECISION_H
#define CLUSTER_QUORUM_DECISION_H

#include "cluster/cluster_qvotec.h" /* ClusterVotingSlot, enums */


#ifdef USE_PGRAC_CLUSTER

/*
 * ClusterQuorumDecision — output of decide_quorum_view().
 *
 *	Caller (cluster_qvotec poll cycle) writes these fields into the
 *	corresponding ClusterQvotecShmem atomics + lease_expire_at_us per
 *	Q4 v0.2 lease semantics.
 *
 *	alive_bitmap is a 128-bit bitmap (CLUSTER_MAX_NODES = 128) of
 *	currently-alive instances as observed across the disk slots.
 */
typedef struct ClusterQuorumDecision {
	ClusterQvotecQuorumState quorum_state;
	uint32 disks_ok_count;
	uint32 disks_total_count;
	uint64 epoch_max;		/* boot-time epoch recovery (spec-2.0 §3 R10) */
	uint8 alive_bitmap[16]; /* 128 bits = 16 bytes */
	ClusterCollisionDetectionState collision_state;
	uint32 collision_other_node_id; /* Q6 valid when collision != NONE */
	uint64 collision_other_incarnation;
} ClusterQuorumDecision;


/*
 * decide_quorum_view — pure majority decision.
 *
 *	Inputs:
 *	  slots[N_disks][N_max_nodes]  — slot matrix from
 *	    cluster_voting_disk_io.read_slot per (disk, node) pair
 *	  io_states[N_disks]           — per-disk I/O outcome from
 *	    most-recent read_all_disks pass (per Q2 v0.2 anything not OK
 *	    means this disk's column data is untrusted this cycle)
 *	  N_disks                      — total voting disk count
 *	  self_node_id                 — caller's node_id (for collision
 *	    detection)
 *	  self_incarnation             — caller's boot incarnation
 *
 *	Output:
 *	  *out  populated;return code = quorum_state shortcut for callers
 *	  who don't care about the rest of the struct.
 *
 *	Behavior:
 *	  - disks_ok_count = count of io_states == OK
 *	  - disks_total_count = N_disks
 *	  - quorum_state:
 *	      OK         if disks_ok_count >= (N_disks/2) + 1
 *	      LOST       if disks_ok_count == 0
 *	      UNCERTAIN  otherwise (>0 but < majority)
 *	  - epoch_max = max(slot.current_epoch) across all OK-disk
 *	    slots that have generation > 0 (skip never-written)
 *	  - alive_bitmap: bit set for every node_id where ANY OK disk
 *	    has a slot with generation > 0 + flags.alive set
 *	  - collision_state per Q6 v0.2 — scan all OK-disk slots for
 *	    slot.node_id == self_node_id with different incarnation
 */
extern ClusterQvotecQuorumState decide_quorum_view(const ClusterVotingSlot *slots,
												   const ClusterVotingDiskIoState *io_states,
												   uint32 n_disks, uint32 n_max_nodes,
												   uint32 self_node_id, uint64 self_incarnation,
												   ClusterQuorumDecision *out);

#endif /* USE_PGRAC_CLUSTER */


#endif /* CLUSTER_QUORUM_DECISION_H */
