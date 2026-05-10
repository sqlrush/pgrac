/*-------------------------------------------------------------------------
 *
 * cluster_quorum_decision.c
 *	  Pure-logic majority view + collision detection — spec-2.6 D4.
 *
 *	  See cluster_quorum_decision.h for full design notes.
 *
 *	  This file deliberately contains NO shmem reads, NO I/O, NO logging,
 *	  NO GUC reads — the decide_quorum_view function is pure so it can
 *	  be unit-tested with a synthetic slot matrix without spinning up a
 *	  full PG instance.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_quorum_decision.c
 *
 * NOTES
 *	  pgrac-original file.  Compiled only in --enable-cluster builds.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_quorum_decision.h"

#ifdef USE_PGRAC_CLUSTER

#include <string.h>


static inline void
alive_bitmap_set(uint8 *bitmap, uint32 node_id)
{
	bitmap[node_id >> 3] |= (uint8)(1u << (node_id & 7u));
}


ClusterQvotecQuorumState
decide_quorum_view(const ClusterVotingSlot *slots, const ClusterVotingDiskIoState *io_states,
				   uint32 n_disks, uint32 n_max_nodes, uint32 self_node_id, uint64 self_incarnation,
				   uint64 now_us, uint64 heartbeat_timeout_us, ClusterQuorumDecision *out)
{
	uint32 disk_idx;
	uint32 node_idx;
	uint32 disks_ok = 0;
	uint32 quorum_size;

	if (out == NULL)
		return CLUSTER_QVOTEC_QUORUM_LOST; /* defensive */

	memset(out, 0, sizeof(*out));
	out->disks_total_count = n_disks;
	out->collision_state = CLUSTER_COLLISION_NONE;

	if (slots == NULL || io_states == NULL || n_disks == 0) {
		out->quorum_state = CLUSTER_QVOTEC_QUORUM_LOST;
		return out->quorum_state;
	}

	/*
	 * Pass 1:  count OK disks.  Anything not OK (TORN, FAILED,
	 * NOT_TRIED) makes this disk's slot column untrusted this cycle.
	 */
	for (disk_idx = 0; disk_idx < n_disks; disk_idx++) {
		if (io_states[disk_idx] == CLUSTER_VOTING_DISK_IO_OK)
			disks_ok++;
	}
	out->disks_ok_count = disks_ok;

	/* Simple majority. */
	quorum_size = (n_disks / 2u) + 1u;

	/*
	 * Pass 2:  walk the slot matrix and accumulate alive_bitmap +
	 * epoch_max + collision detection.  Only consider slots from OK
	 * disks (per Q2 v0.2 anything else is untrusted this cycle).
	 */
	for (disk_idx = 0; disk_idx < n_disks; disk_idx++) {
		if (io_states[disk_idx] != CLUSTER_VOTING_DISK_IO_OK)
			continue;

		for (node_idx = 0; node_idx < n_max_nodes; node_idx++) {
			const ClusterVotingSlot *s = &slots[disk_idx * n_max_nodes + node_idx];
			bool is_fresh;

			/*
			 * generation == 0 means "never written" (a freshly
			 * formatted slot from cluster_voting_disk_format).
			 * Skip — this node has not yet polled.
			 */
			if (s->generation == 0)
				continue;

			/*
			 * Track epoch_max across ALL written slots — fresh OR stale.
			 * Boot-time epoch recovery (spec-2.0 §3 R10) computes
			 * "boot epoch = max(observed) + 1"; a crashed peer's stale
			 * slot still contributes its (last-known-valid) epoch.
			 */
			if (s->current_epoch > out->epoch_max)
				out->epoch_max = s->current_epoch;

			/*
			 * P2.1 freshness gate (Hardening v0.3):
			 * Only slots whose heartbeat is within the timeout window
			 * are considered for alive_bitmap + collision detection.
			 * A crashed peer leaves ALIVE flag + stale heartbeat_ts_us
			 * behind;without this gate that slot would continue to
			 * vote alive forever and pollute collision judgement.
			 *
			 * heartbeat_timeout_us == 0 disables the gate (epoch
			 * recovery / fsck path).
			 */
			if (heartbeat_timeout_us == 0)
				is_fresh = true;
			else if (s->heartbeat_ts_us == 0)
				is_fresh = false; /* never set — treat as stale */
			else if (now_us > s->heartbeat_ts_us
					 && (now_us - s->heartbeat_ts_us) > heartbeat_timeout_us)
				is_fresh = false; /* aged out */
			else
				is_fresh = true; /* covers now_us <= heartbeat_ts_us
								  * (small clock drift) and within window */

			if (!is_fresh)
				continue;

			if (s->flags & CLUSTER_VOTING_SLOT_FLAG_ALIVE) {
				if (s->node_id < (uint32)(sizeof(out->alive_bitmap) * 8u))
					alive_bitmap_set(out->alive_bitmap, s->node_id);
			}

			/*
			 * Q6 v0.2 collision detection.  Only meaningful for fresh
			 * ALIVE slots where node_id == self_node_id;in that case
			 * the slot was written by the OTHER serving instance using
			 * the same node_id.  Stale slots are ignored above, and
			 * non-ALIVE slots are clean-shutdown tombstones that must
			 * not trip fast-restart false collision.
			 */
			if ((s->flags & CLUSTER_VOTING_SLOT_FLAG_ALIVE) && s->node_id == self_node_id
				&& s->incarnation != self_incarnation) {
				if (self_incarnation > s->incarnation) {
					/*
					 * Q6 v0.2:  newer-self-FATAL.  Self is the newer
					 * comer (just booted with a higher incarnation);
					 * the older serving instance is on the disk.
					 * Caller (cluster_qvotec) will FATAL this process
					 * with ERRCODE_CLUSTER_NODE_ID_COLLISION.
					 */
					out->collision_state = CLUSTER_COLLISION_FATAL_NEWER_SELF;
					out->collision_other_node_id = s->node_id;
					out->collision_other_incarnation = s->incarnation;
				} else {
					/*
					 * self.incarnation < slot.incarnation — the OTHER
					 * instance is the newer comer.  By Q6 v0.2 it
					 * should self-FATAL on its own poll.  We continue
					 * (UNCERTAIN), but record that we observed an
					 * older slot (not "older than us" — older as in
					 * lower incarnation than the other side, which
					 * means the other side is newer and will exit).
					 *
					 * NB: this branch only fires if the OBSERVED
					 * incarnation is HIGHER than self;the field
					 * naming is from the slot's perspective.
					 */
					if (out->collision_state == CLUSTER_COLLISION_NONE) {
						out->collision_state = CLUSTER_COLLISION_OBSERVED_OLDER;
						out->collision_other_node_id = s->node_id;
						out->collision_other_incarnation = s->incarnation;
					}
					/*
					 * If FATAL_NEWER_SELF was already set on a different
					 * disk this cycle, keep the FATAL — it's strictly
					 * more severe.
					 */
				}
			}
		}
	}

	/* Quorum state decision. */
	if (disks_ok >= quorum_size)
		out->quorum_state = CLUSTER_QVOTEC_QUORUM_OK;
	else if (disks_ok == 0)
		out->quorum_state = CLUSTER_QVOTEC_QUORUM_LOST;
	else
		out->quorum_state = CLUSTER_QVOTEC_QUORUM_UNCERTAIN;

	return out->quorum_state;
}

#endif /* USE_PGRAC_CLUSTER */
