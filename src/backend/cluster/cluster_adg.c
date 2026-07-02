/*-------------------------------------------------------------------------
 *
 * cluster_adg.c
 *	  Dependency-light ADG standby/read-only correctness primitives.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_adg.c
 *
 * NOTES
 *	  This is a pgrac-original file for spec-6.4 ADG standby correctness.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_adg.h"

static bool
cluster_adg_valid_thread_id(const ClusterAdgScnTracker *tracker, uint16 thread_id, uint16 *index)
{
	if (tracker == NULL || tracker->thread_count == 0
		|| tracker->thread_count > CLUSTER_ADG_MAX_THREADS)
		return false;
	if (thread_id == 0 || thread_id > tracker->thread_count)
		return false;
	if (index != NULL)
		*index = (uint16)(thread_id - 1);
	return true;
}

static bool
cluster_adg_recompute_consistent_scn(ClusterAdgScnTracker *tracker)
{
	SCN min_scn = InvalidScn;
	bool have_active = false;
	uint16 i;

	for (i = 0; i < tracker->thread_count; i++) {
		const ClusterAdgThreadStatus *thread = &tracker->threads[i];

		if (!thread->active)
			continue;
		have_active = true;
		if (!thread->has_barrier || !SCN_VALID(thread->barrier_safe_scn))
			return true;
		if (!SCN_VALID(min_scn) || scn_time_cmp(thread->barrier_safe_scn, min_scn) < 0
			|| (scn_time_cmp(thread->barrier_safe_scn, min_scn) == 0
				&& scn_total_cmp(thread->barrier_safe_scn, min_scn) < 0))
			min_scn = thread->barrier_safe_scn;
	}

	if (!have_active || !SCN_VALID(min_scn))
		return true;
	if (SCN_VALID(tracker->standby_consistent_scn)
		&& scn_time_cmp(min_scn, tracker->standby_consistent_scn) < 0)
		return false;
	if (!SCN_VALID(tracker->standby_consistent_scn)
		|| scn_time_cmp(min_scn, tracker->standby_consistent_scn) > 0
		|| scn_total_cmp(min_scn, tracker->standby_consistent_scn) != 0) {
		tracker->standby_consistent_scn = min_scn;
		tracker->publish_count++;
	}
	return true;
}

bool
cluster_adg_scn_tracker_init(ClusterAdgScnTracker *tracker, uint16 thread_count)
{
	if (tracker == NULL)
		return false;

	memset(tracker, 0, sizeof(*tracker));
	if (thread_count == 0 || thread_count > CLUSTER_ADG_MAX_THREADS)
		return false;

	tracker->thread_count = thread_count;
	tracker->standby_consistent_scn = InvalidScn;
	return true;
}

bool
cluster_adg_mark_received(ClusterAdgScnTracker *tracker, uint16 thread_id, XLogRecPtr receive_lsn,
						  int64 receive_time_ms)
{
	ClusterAdgThreadStatus *thread;
	uint16 index;

	if (!cluster_adg_valid_thread_id(tracker, thread_id, &index))
		return false;
	thread = &tracker->threads[index];

	if (thread->has_receive && receive_lsn < thread->receive_lsn)
		return false;

	thread->active = true;
	thread->has_receive = true;
	thread->receive_lsn = receive_lsn;
	thread->receive_time_ms = receive_time_ms;
	return true;
}

bool
cluster_adg_mark_applied(ClusterAdgScnTracker *tracker, uint16 thread_id, XLogRecPtr apply_lsn,
						 SCN apply_scn, int64 apply_time_ms)
{
	ClusterAdgThreadStatus *thread;
	uint16 index;

	if (!cluster_adg_valid_thread_id(tracker, thread_id, &index))
		return false;
	if (!SCN_VALID(apply_scn))
		return false;
	thread = &tracker->threads[index];

	if (thread->has_receive && apply_lsn > thread->receive_lsn)
		return false;
	if (thread->has_apply) {
		if (apply_lsn < thread->apply_lsn)
			return false;
	}

	thread->active = true;
	thread->has_apply = true;
	thread->apply_lsn = apply_lsn;
	if (!SCN_VALID(thread->last_apply_scn) || scn_time_cmp(apply_scn, thread->last_apply_scn) > 0
		|| (scn_time_cmp(apply_scn, thread->last_apply_scn) == 0
			&& scn_total_cmp(apply_scn, thread->last_apply_scn) > 0))
		thread->last_apply_scn = apply_scn;
	thread->apply_time_ms = apply_time_ms;
	return true;
}

bool
cluster_adg_apply_thread_barrier(ClusterAdgScnTracker *tracker, uint16 thread_id,
								 XLogRecPtr apply_lsn, SCN barrier_safe_scn, int64 apply_time_ms)
{
	ClusterAdgThreadStatus *thread;
	uint16 index;

	if (!cluster_adg_valid_thread_id(tracker, thread_id, &index))
		return false;
	if (!SCN_VALID(barrier_safe_scn))
		return false;
	thread = &tracker->threads[index];

	if (thread->has_receive && apply_lsn > thread->receive_lsn)
		return false;
	if (thread->has_apply && apply_lsn < thread->apply_lsn)
		return false;
	if (thread->has_barrier && scn_time_cmp(barrier_safe_scn, thread->barrier_safe_scn) < 0)
		return false;

	thread->active = true;
	thread->has_apply = true;
	thread->has_barrier = true;
	thread->apply_lsn = apply_lsn;
	if (!SCN_VALID(thread->last_apply_scn)
		|| scn_time_cmp(barrier_safe_scn, thread->last_apply_scn) > 0
		|| (scn_time_cmp(barrier_safe_scn, thread->last_apply_scn) == 0
			&& scn_total_cmp(barrier_safe_scn, thread->last_apply_scn) > 0))
		thread->last_apply_scn = barrier_safe_scn;
	thread->barrier_safe_scn = barrier_safe_scn;
	thread->apply_time_ms = apply_time_ms;
	return cluster_adg_recompute_consistent_scn(tracker);
}

SCN
cluster_adg_scn_tracker_consistent_scn(const ClusterAdgScnTracker *tracker)
{
	if (tracker == NULL)
		return InvalidScn;
	return tracker->standby_consistent_scn;
}

uint64
cluster_adg_apply_master_next_term(uint64 durable_term)
{
	if (durable_term == UINT64_MAX)
		return 0;
	return durable_term + 1;
}

pg_crc32c
cluster_adg_apply_master_lease_crc(const ClusterAdgApplyMasterLease *lease)
{
	pg_crc32c crc;

	if (lease == NULL)
		return 0;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, lease, offsetof(ClusterAdgApplyMasterLease, crc));
	FIN_CRC32C(crc);
	return crc;
}

void
cluster_adg_apply_master_lease_init(ClusterAdgApplyMasterLease *lease, uint64 term,
									int32 owner_node_id, int64 lease_expires_at_ms,
									uint64 generation)
{
	if (lease == NULL)
		return;

	memset(lease, 0, sizeof(*lease));
	lease->magic = CLUSTER_ADG_APPLY_LEASE_MAGIC;
	lease->version = CLUSTER_ADG_APPLY_LEASE_VERSION;
	lease->term = term;
	lease->owner_node_id = owner_node_id;
	lease->lease_expires_at_ms = lease_expires_at_ms;
	lease->generation = generation;
	lease->crc = cluster_adg_apply_master_lease_crc(lease);
}

bool
cluster_adg_apply_master_lease_valid(const ClusterAdgApplyMasterLease *lease)
{
	if (lease == NULL)
		return false;
	if (lease->magic != CLUSTER_ADG_APPLY_LEASE_MAGIC)
		return false;
	if (lease->version != CLUSTER_ADG_APPLY_LEASE_VERSION)
		return false;
	if (lease->term == 0)
		return false;
	if (lease->owner_node_id < 0 || lease->owner_node_id > SCN_MAX_VALID_NODE_ID)
		return false;
	return lease->crc == cluster_adg_apply_master_lease_crc(lease);
}

bool
cluster_adg_apply_master_lease_pack(void *slot512, const ClusterAdgApplyMasterLease *lease)
{
	if (slot512 == NULL || lease == NULL || !cluster_adg_apply_master_lease_valid(lease))
		return false;
	memset(slot512, 0, CLUSTER_ADG_APPLY_LEASE_SLOT_BYTES);
	memcpy(slot512, lease, sizeof(*lease));
	return true;
}

bool
cluster_adg_apply_master_lease_unpack(const void *slot512, ClusterAdgApplyMasterLease *lease)
{
	if (slot512 == NULL || lease == NULL)
		return false;
	memcpy(lease, slot512, sizeof(*lease));
	return cluster_adg_apply_master_lease_valid(lease);
}

static bool
cluster_adg_apply_master_lease_same(const ClusterAdgApplyMasterLease *a,
									const ClusterAdgApplyMasterLease *b)
{
	return a->term == b->term && a->generation == b->generation
		   && a->owner_node_id == b->owner_node_id;
}

static bool
cluster_adg_apply_master_lease_newer(const ClusterAdgApplyMasterLease *a,
									 const ClusterAdgApplyMasterLease *b)
{
	if (a->term != b->term)
		return a->term > b->term;
	if (a->generation != b->generation)
		return a->generation > b->generation;
	return a->owner_node_id < b->owner_node_id;
}

bool
cluster_adg_apply_master_lease_quorum(const ClusterAdgApplyMasterLease leases[], const bool valid[],
									  int lease_count, int quorum,
									  ClusterAdgApplyMasterLeaseQuorum *out)
{
	if (out == NULL)
		return false;

	memset(out, 0, sizeof(*out));
	out->owner_node_id = -1;

	if (leases == NULL || valid == NULL || lease_count <= 0 || quorum <= 0 || quorum > lease_count)
		return false;

	for (int i = 0; i < lease_count; i++) {
		ClusterAdgApplyMasterLease candidate = leases[i];
		int count = 0;
		int64 max_expires_at_ms = candidate.lease_expires_at_ms;
		bool should_publish;

		if (!valid[i])
			continue;
		for (int j = 0; j < lease_count; j++) {
			if (!valid[j] || !cluster_adg_apply_master_lease_same(&candidate, &leases[j]))
				continue;
			count++;
			if (leases[j].lease_expires_at_ms > max_expires_at_ms)
				max_expires_at_ms = leases[j].lease_expires_at_ms;
		}
		if (count < quorum)
			continue;

		should_publish = !out->attached;
		if (!should_publish) {
			ClusterAdgApplyMasterLease current;

			memset(&current, 0, sizeof(current));
			current.term = out->durable_term;
			current.owner_node_id = out->owner_node_id;
			current.generation = out->generation;
			current.lease_expires_at_ms = out->lease_expires_at_ms;
			should_publish = cluster_adg_apply_master_lease_newer(&candidate, &current);
		}
		if (should_publish) {
			out->attached = true;
			out->count = count;
			out->durable_term = candidate.term;
			out->owner_node_id = candidate.owner_node_id;
			out->lease_expires_at_ms = max_expires_at_ms;
			out->generation = candidate.generation;
		}
	}

	return true;
}

ClusterAdgReadDecision
cluster_adg_read_only_decide(bool enable_adg, bool standby_role, bool read_service_available,
							 SCN standby_consistent_scn, int64 apply_lag_ms, int64 max_lag_ms)
{
	if (!enable_adg || !standby_role)
		return CLUSTER_ADG_READ_UNRESOLVABLE;
	if (!read_service_available || !SCN_VALID(standby_consistent_scn))
		return CLUSTER_ADG_READ_UNRESOLVABLE;
	if (max_lag_ms >= 0 && apply_lag_ms > max_lag_ms)
		return CLUSTER_ADG_READ_LAG_EXCESSIVE;
	return CLUSTER_ADG_READ_ALLOW;
}

bool
cluster_adg_reply_trailer_valid(uint32 magic, uint16 version, uint16 thread_id,
								SCN standby_consistent_scn pg_attribute_unused(),
								uint64 apply_master_term pg_attribute_unused())
{
	if (magic != CLUSTER_ADG_REPLY_MAGIC)
		return false;
	if (version != CLUSTER_ADG_REPLY_VERSION)
		return false;
	if (thread_id != XLP_THREAD_ID_LEGACY
		&& (thread_id < XLP_THREAD_ID_FIRST_REAL || thread_id > CLUSTER_WAL_THREAD_MAX))
		return false;
	return true;
}

#endif /* USE_PGRAC_CLUSTER */
