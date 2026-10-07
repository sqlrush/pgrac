/*-------------------------------------------------------------------------
 *
 * cluster_storage_quorum.c
 *    Bind database eligibility to a current storage-component observation.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/backend/cluster/cluster_storage_quorum.c
 *
 * NOTES
 *    PGRAC-original code; exported symbols use the cluster_ prefix.
 *    Spec: spec-s9p2-06-online-membership.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_guc.h"
#include "cluster/cluster_storage_quorum.h"
#include <time.h>

static ClusterStorageQuorumState *storage_state;
static ClusterStorageCheckResult storage_view_result(const ClusterStorageQuorumView *view,
													 uint64 now);

/* This observation expires within one OS boot, independent of wall-clock
 * corrections. It is never persisted or reused after postmaster restart. */
uint64
cluster_storage_quorum_now_us(void)
{
	struct timespec now;

	if (clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0
		|| (uint64)now.tv_sec > (UINT64_MAX - 999999) / 1000000)
		return 0;
	return (uint64)now.tv_sec * 1000000 + (uint64)now.tv_nsec / 1000;
}

/* Parse decimal digits without accepting signs, whitespace or overflow. */
static bool
storage_number(const char **text, uint32 *out)
{
	const char *p = *text;
	uint32 value = 0;

	if (*p < '0' || *p > '9')
		return false;
	while (*p >= '0' && *p <= '9') {
		uint32 digit = (uint32)(*p++ - '0');

		if (value > (UINT32_MAX - digit) / 10)
			return false;
		value = value * 10 + digit;
	}
	*text = p;
	*out = value;
	return true;
}

/*
 * cluster_storage_quorum_parse_nodes -- Validate an explicit, complete map.
 * Inputs: immutable slot:id text and the configured database member bitmap.
 * Returns: true only for a one-to-one mapping of exactly those slots.
 * Side Effects: replaces map; partial output is unusable on failure.
 */
bool
cluster_storage_quorum_parse_nodes(const char *text, const uint64 configured[2],
								   uint32 map[CLUSTER_MAX_NODES])
{
	uint64 seen[2] = { 0, 0 };

	if (text == NULL || *text == '\0' || configured == NULL || map == NULL)
		return false;
	memset(map, 0, sizeof(uint32) * CLUSTER_MAX_NODES);
	while (*text) {
		uint32 node;
		uint32 id;
		int i;

		if (!storage_number(&text, &node) || node >= CLUSTER_MAX_NODES || *text++ != ':'
			|| !storage_number(&text, &id) || id == 0 || map[node] != 0
			|| (configured[node / 64] & (UINT64_C(1) << (node % 64))) == 0)
			return false;
		for (i = 0; i < CLUSTER_MAX_NODES; i++)
			if (map[i] == id)
				return false;
		map[node] = id;
		seen[node / 64] |= UINT64_C(1) << (node % 64);
		if (*text == '\0')
			break;
		if (*text++ != ',' || *text == '\0')
			return false;
	}
	return seen[0] == configured[0] && seen[1] == configured[1];
}

/*
 * cluster_storage_quorum_decode_component -- Map one complete provider view.
 * Inputs: provider ring/member tuple, local provider identity, configured map.
 * Returns: true only for a quorate, mapped component containing this instance.
 * Side Effects: initializes out; rejects duplicate or unknown provider members.
 */
bool
cluster_storage_quorum_decode_component(ClusterStorageQuorumView *out, uint32 ring_node,
										uint64 ring_sequence, uint32 quorate, const uint32 *ids,
										uint32 count, uint32 local_id, int self_node,
										const uint32 map[CLUSTER_MAX_NODES])
{
	uint32 i;
	bool ring_member = false;

	memset(out, 0, sizeof(*out));
	out->reason = CLUSTER_STORAGE_QUORUM_CONFIGURATION;
	if (self_node < 0 || self_node >= CLUSTER_MAX_NODES || map == NULL || local_id == 0
		|| map[self_node] != local_id || ids == NULL || count == 0 || count > CLUSTER_MAX_NODES
		|| ring_node == 0 || ring_sequence == 0)
		return false;
	if (quorate != 1) {
		out->reason = CLUSTER_STORAGE_QUORUM_NOT_QUORATE;
		return false;
	}
	for (i = 0; i < count; i++) {
		int node;
		uint64 bit;

		for (node = 0; node < CLUSTER_MAX_NODES; node++)
			if (map[node] != 0 && map[node] == ids[i])
				break;
		if (node == CLUSTER_MAX_NODES)
			return false;
		bit = UINT64_C(1) << (node % 64);
		if ((out->members[node / 64] & bit) != 0)
			return false;
		out->members[node / 64] |= bit;
		ring_member |= ids[i] == ring_node;
	}
	if (!ring_member || (out->members[self_node / 64] & (UINT64_C(1) << (self_node % 64))) == 0)
		return false;
	out->ring_node = ring_node;
	out->ring_sequence = ring_sequence;
	out->reason = CLUSTER_STORAGE_QUORUM_READY;
	return true;
}

/* Attach to the region allocated and initialized with the QVOTEC owner. */
void
cluster_storage_quorum_attach(ClusterStorageQuorumState *state, bool initialize)
{
	storage_state = state;
	if (state == NULL || !initialize)
		return;
	pg_atomic_init_u32(&state->sequence, 0);
	pg_atomic_init_u32(&state->reason, CLUSTER_STORAGE_QUORUM_UNAVAILABLE);
	pg_atomic_init_u32(&state->ring_node, 0);
	pg_atomic_init_u32(&state->provider_diagnostic, 0);
	pg_atomic_init_u64(&state->ring_sequence, 0);
	pg_atomic_init_u64(&state->members[0], 0);
	pg_atomic_init_u64(&state->members[1], 0);
	pg_atomic_init_u64(&state->sampled_us, 0);
	pg_atomic_init_u64(&state->expires_us, 0);
	pg_atomic_init_u64(&state->generation, 0);
	pg_atomic_init_u64(&state->loss_generation, 1);
	for (int i = 0; i < CLUSTER_STORAGE_DIAG_FIELDS; i++)
		pg_atomic_init_u64(&state->diagnostic[i], 0);
}

/*
 * cluster_storage_quorum_refresh -- Publish a new observation from QVOTEC.
 * Inputs: cycle start and the existing database observation lease duration.
 * Returns: void; incomplete notifications cannot renew a prior observation.
 * Side Effects: calls the storage provider; atomically replaces the shared view.
 * No observer calls this function and no disk ALIVE evidence is discarded.
 */
void
cluster_storage_quorum_refresh(uint64 now_us, uint64 duration_us)
{
	ClusterStorageQuorumView view;
	ClusterStorageQuorumView previous;
	uint64 generation;
	uint64 loss_generation;
	uint64 published_at;
	bool had_previous;
	bool lost;

	if (!cluster_shared_config || storage_state == NULL)
		return;
	/* Independent diagnostic fields: OUTCOME=0 and FINISHED=0 mean in flight.
	 * None of these timestamps participate in a lease or continuity check. */
	pg_atomic_write_u64(&storage_state->diagnostic[CLUSTER_STORAGE_DIAG_OUTCOME], 0);
	pg_atomic_write_u64(&storage_state->diagnostic[CLUSTER_STORAGE_DIAG_FINISHED], 0);
	pg_atomic_write_u64(&storage_state->diagnostic[CLUSTER_STORAGE_DIAG_STARTED], now_us);
	memset(&view, 0, sizeof(view));
	cluster_storage_corosync_sample(&view);
	pg_atomic_write_u64(&storage_state->diagnostic[CLUSTER_STORAGE_DIAG_RESULT],
						((uint64)view.provider_diagnostic << 32) | (uint32)view.reason);
	generation = pg_atomic_read_u64(&storage_state->generation);
	if (now_us == 0 || duration_us == 0 || now_us > UINT64_MAX - duration_us
		|| generation == UINT64_MAX)
		view.reason = CLUSTER_STORAGE_QUORUM_UNAVAILABLE;
	if (view.reason == CLUSTER_STORAGE_QUORUM_INCOMPLETE) {
		ClusterStorageQuorumView current;

		/* No new authority: retain the exact original expiry and generation.
		 * A valid partial membership can still prove removal immediately.
		 * API/configuration failures and explicit loss never enter this path. */
		if (cluster_storage_quorum_snapshot(&current)
			&& storage_view_result(&current, cluster_storage_quorum_now_us())
				   == CLUSTER_STORAGE_CHECK_ALLOWED
			&& now_us >= current.sampled_us && now_us < current.expires_us
			&& ((view.members[0] == 0 && view.members[1] == 0)
				|| ((current.members[0] & ~view.members[0]) == 0
					&& (current.members[1] & ~view.members[1]) == 0))) {
			pg_atomic_write_u64(&storage_state->diagnostic[CLUSTER_STORAGE_DIAG_OUTCOME], 2);
			pg_atomic_write_u64(&storage_state->diagnostic[CLUSTER_STORAGE_DIAG_FINISHED],
								cluster_storage_quorum_now_us());
			return;
		}
	}
	if (view.reason != CLUSTER_STORAGE_QUORUM_READY) {
		view.members[0] = view.members[1] = 0;
		view.ring_sequence = 0;
		view.ring_node = 0;
	} else {
		view.sampled_us = now_us;
		view.expires_us = now_us + duration_us;
	}
	view.generation = generation == UINT64_MAX ? generation : generation + 1;
	/* A later READY must not hide a negative or an expired previous lease.
	 * Qualified membership changes are a new observation, not local loss;
	 * the membership/formation gates still validate that new cut separately.
	 * The owner alone writes, and this field shares the view's publication. */
	had_previous = cluster_storage_quorum_snapshot(&previous);
	pg_atomic_fetch_add_u32(&storage_state->sequence, 1);
	pg_write_barrier();
	/* Read the clock after excluding readers of the old view: otherwise a
	 * descheduled writer could overwrite a stably observed EXPIRED with READY. */
	published_at = cluster_storage_quorum_now_us();
	lost = !had_previous
		   || storage_view_result(&previous, published_at) != CLUSTER_STORAGE_CHECK_ALLOWED
		   || storage_view_result(&view, published_at) != CLUSTER_STORAGE_CHECK_ALLOWED
		   || now_us < previous.sampled_us || now_us >= previous.expires_us;
	loss_generation = pg_atomic_read_u64(&storage_state->loss_generation);
	if (!had_previous || loss_generation == 0)
		loss_generation = UINT64_MAX;
	else if (lost && loss_generation != UINT64_MAX)
		loss_generation++;
	pg_atomic_write_u64(&storage_state->loss_generation, loss_generation);
	pg_atomic_write_u32(&storage_state->reason, view.reason);
	pg_atomic_write_u32(&storage_state->ring_node, view.ring_node);
	pg_atomic_write_u32(&storage_state->provider_diagnostic, view.provider_diagnostic);
	pg_atomic_write_u64(&storage_state->ring_sequence, view.ring_sequence);
	pg_atomic_write_u64(&storage_state->members[0], view.members[0]);
	pg_atomic_write_u64(&storage_state->members[1], view.members[1]);
	pg_atomic_write_u64(&storage_state->sampled_us, view.sampled_us);
	pg_atomic_write_u64(&storage_state->expires_us, view.expires_us);
	pg_atomic_write_u64(&storage_state->generation, view.generation);
	pg_write_barrier();
	pg_atomic_fetch_add_u32(&storage_state->sequence, 1);
	pg_atomic_write_u64(&storage_state->diagnostic[CLUSTER_STORAGE_DIAG_OUTCOME], 1);
	pg_atomic_write_u64(&storage_state->diagnostic[CLUSTER_STORAGE_DIAG_FINISHED],
						cluster_storage_quorum_now_us());
}

/* Record an original libquorum callback, never a Corosync token receipt.
 * The poll owner alone writes this diagnostic suffix. Author: SqlRush <sqlrush@gmail.com> */
void
cluster_storage_quorum_note_notification(bool quorum, uint32 ring_node, uint64 ring_sequence)
{
	int time_field
		= quorum ? CLUSTER_STORAGE_DIAG_QUORUM_NOTIFY : CLUSTER_STORAGE_DIAG_MEMBERS_NOTIFY;
	int ring_field = quorum ? CLUSTER_STORAGE_DIAG_QUORUM_RING : CLUSTER_STORAGE_DIAG_MEMBERS_RING;
	int node_field = quorum ? CLUSTER_STORAGE_DIAG_QUORUM_NODE : CLUSTER_STORAGE_DIAG_MEMBERS_NODE;
	int count_field
		= quorum ? CLUSTER_STORAGE_DIAG_QUORUM_COUNT : CLUSTER_STORAGE_DIAG_MEMBERS_COUNT;

	if (storage_state == NULL)
		return;
	pg_atomic_write_u64(&storage_state->diagnostic[time_field], cluster_storage_quorum_now_us());
	pg_atomic_write_u64(&storage_state->diagnostic[ring_field], ring_sequence);
	pg_atomic_write_u64(&storage_state->diagnostic[node_field], ring_node);
	pg_atomic_fetch_add_u64(&storage_state->diagnostic[count_field], 1);
}

/* Bounded, passive evidence: one attempt, no retry/clock/lock/provider call.
 * A torn view is explicitly unknown, not loss of eligibility. The suffix
 * fields are independent observations, not an atomic tuple or permission.
 * Author: SqlRush <sqlrush@gmail.com> */
void
cluster_storage_quorum_diagnostic_format(char *out, size_t size)
{
	ClusterStorageQuorumView view = { 0 };
	uint64 diag[CLUSTER_STORAGE_DIAG_FIELDS] = { 0 };
	uint32 before = 0, after = 0;
	bool stable = false;

	if (storage_state != NULL) {
		before = pg_atomic_read_u32(&storage_state->sequence);
		if ((before & 1) == 0) {
			pg_read_barrier();
			view.reason = pg_atomic_read_u32(&storage_state->reason);
			view.ring_node = pg_atomic_read_u32(&storage_state->ring_node);
			view.ring_sequence = pg_atomic_read_u64(&storage_state->ring_sequence);
			view.members[0] = pg_atomic_read_u64(&storage_state->members[0]);
			view.members[1] = pg_atomic_read_u64(&storage_state->members[1]);
			view.sampled_us = pg_atomic_read_u64(&storage_state->sampled_us);
			view.expires_us = pg_atomic_read_u64(&storage_state->expires_us);
			view.generation = pg_atomic_read_u64(&storage_state->generation);
			pg_read_barrier();
			after = pg_atomic_read_u32(&storage_state->sequence);
			stable = before == after;
		} else
			after = before;
		if (!stable)
			memset(&view, 0, sizeof(view));
		for (int i = 0; i < CLUSTER_STORAGE_DIAG_FIELDS; i++)
			diag[i] = pg_atomic_read_u64(&storage_state->diagnostic[i]);
	}
#define DIAG_VALUE(field) (unsigned long long)diag[CLUSTER_STORAGE_DIAG_##field]
	snprintf(out, size,
			 "storage_view_stable=%d storage_seq=%u/%u reason=%u generation=%llu loss=unobserved "
			 "ring=%u/%llu members=%016llx/%016llx clock_storage=MONOTONIC "
			 "sampled_mono_us=%llu expiry_mono_us=%llu sample_started_mono_us=%llu "
			 "sample_finished_mono_us=%llu raw_reason=%u raw_provider=%u sample_outcome=%llu "
			 "quorum_callback_mono_us=%llu members_callback_mono_us=%llu "
			 "quorum_callback_ring=%llu/%llu members_callback_ring=%llu/%llu "
			 "quorum_callback_count=%llu members_callback_count=%llu token_rx=unobserved",
			 stable, before, after, (unsigned)view.reason, (unsigned long long)view.generation,
			 view.ring_node, (unsigned long long)view.ring_sequence,
			 (unsigned long long)view.members[0], (unsigned long long)view.members[1],
			 (unsigned long long)view.sampled_us, (unsigned long long)view.expires_us,
			 DIAG_VALUE(STARTED), DIAG_VALUE(FINISHED), (uint32)diag[CLUSTER_STORAGE_DIAG_RESULT],
			 (uint32)(diag[CLUSTER_STORAGE_DIAG_RESULT] >> 32), DIAG_VALUE(OUTCOME),
			 DIAG_VALUE(QUORUM_NOTIFY), DIAG_VALUE(MEMBERS_NOTIFY), DIAG_VALUE(QUORUM_NODE),
			 DIAG_VALUE(QUORUM_RING), DIAG_VALUE(MEMBERS_NODE), DIAG_VALUE(MEMBERS_RING),
			 DIAG_VALUE(QUORUM_COUNT), DIAG_VALUE(MEMBERS_COUNT));
#undef DIAG_VALUE
}

/* Four fast reads cover an uncontended publication. On overlap, yield at most
 * ten times for 100us, also bounded by 1ms of monotonic elapsed time. The sole
 * writer's odd section takes no locks and never waits for a reader, including
 * callers that already hold a reconfiguration lock or have no PGPROC. */
#define STORAGE_SNAPSHOT_FAST_READS 4
#define STORAGE_SNAPSHOT_MAX_WAITS 10
#define STORAGE_SNAPSHOT_WAIT_US 100

typedef struct StorageSnapshotWait {
	uint64 started_us;
	uint64 last_us;
	uint64 sampled_us;
	uint32 count;
	ClusterStorageSnapshotStop stop;
} StorageSnapshotWait;

/* Keep all samples from one bounded read ordered, including the caller's
 * final qualification sample. A regression above the start is still unknown. */
static bool
storage_snapshot_time_valid(StorageSnapshotWait *wait, uint64 now)
{
	wait->sampled_us = now;
	if (now == 0) {
		wait->stop = CLUSTER_STORAGE_SNAPSHOT_CLOCK_UNAVAILABLE;
		return false;
	}
	if (now < wait->last_us) {
		wait->stop = CLUSTER_STORAGE_SNAPSHOT_CLOCK_REGRESSED;
		return false;
	}
	if (wait->started_us == 0)
		wait->started_us = now;
	if (now - wait->started_us >= STORAGE_SNAPSHOT_MAX_WAITS * STORAGE_SNAPSHOT_WAIT_US) {
		wait->stop = CLUSTER_STORAGE_SNAPSHOT_DEADLINE;
		return false;
	}
	wait->last_us = now;
	return true;
}

/* Obtain one stable view. Neither a wait nor a reader extends its expiry. */
static bool
storage_snapshot(ClusterStorageQuorumView *out, ClusterStorageQuorumCheck *check,
				 StorageSnapshotWait *wait)
{
	int retry;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (storage_state == NULL)
		return false;
	for (retry = 0; retry < STORAGE_SNAPSHOT_FAST_READS + STORAGE_SNAPSHOT_MAX_WAITS; retry++) {
		uint32 before;
		uint32 after;

		if (retry >= STORAGE_SNAPSHOT_FAST_READS) {
			uint64 now = cluster_storage_quorum_now_us();
			uint64 budget = STORAGE_SNAPSHOT_MAX_WAITS * STORAGE_SNAPSHOT_WAIT_US;

			if (!storage_snapshot_time_valid(wait, now))
				break;
			wait->count++;
			pg_usleep(
				(long)Min((uint64)STORAGE_SNAPSHOT_WAIT_US, budget - (now - wait->started_us)));
			/* Scheduling can oversleep, and a failed/reversed clock cannot
			 * make a completed publisher evidence within this wait budget. */
			now = cluster_storage_quorum_now_us();
			if (!storage_snapshot_time_valid(wait, now))
				break;
		}
		before = pg_atomic_read_u32(&storage_state->sequence);
		if (check != NULL) {
			check->attempts = retry + 1;
			check->sequence_before = check->sequence_after = before;
		}
		if (before & 1)
			continue;
		pg_read_barrier();
		out->reason = pg_atomic_read_u32(&storage_state->reason);
		out->ring_node = pg_atomic_read_u32(&storage_state->ring_node);
		out->ring_sequence = pg_atomic_read_u64(&storage_state->ring_sequence);
		out->members[0] = pg_atomic_read_u64(&storage_state->members[0]);
		out->members[1] = pg_atomic_read_u64(&storage_state->members[1]);
		out->sampled_us = pg_atomic_read_u64(&storage_state->sampled_us);
		out->expires_us = pg_atomic_read_u64(&storage_state->expires_us);
		out->generation = pg_atomic_read_u64(&storage_state->generation);
		out->loss_generation = pg_atomic_read_u64(&storage_state->loss_generation);
		out->provider_diagnostic = pg_atomic_read_u32(&storage_state->provider_diagnostic);
		pg_read_barrier();
		after = pg_atomic_read_u32(&storage_state->sequence);
		if (check != NULL)
			check->sequence_after = after;
		if (before == after) {
			/* A reader descheduled during the copy must also respect the
			 * same deadline; the uncontended fast path needs no extra clock. */
			if (retry >= STORAGE_SNAPSHOT_FAST_READS) {
				uint64 now = cluster_storage_quorum_now_us();

				if (!storage_snapshot_time_valid(wait, now))
					break;
			}
			return true;
		}
	}
	if (wait->stop == CLUSTER_STORAGE_SNAPSHOT_COMPLETE)
		wait->stop = CLUSTER_STORAGE_SNAPSHOT_READ_LIMIT;
	memset(out, 0, sizeof(*out));
	return false;
}

bool
cluster_storage_quorum_snapshot(ClusterStorageQuorumView *out)
{
	StorageSnapshotWait wait = { 0 };

	return storage_snapshot(out, NULL, &wait);
}

static ClusterStorageCheckResult
storage_view_result(const ClusterStorageQuorumView *view, uint64 now)
{
	if (cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES)
		return CLUSTER_STORAGE_CHECK_INVALID_SELF;
	if (view->reason != CLUSTER_STORAGE_QUORUM_READY)
		return CLUSTER_STORAGE_CHECK_PROVIDER;
	if (view->generation == 0 || view->ring_node == 0 || view->ring_sequence == 0
		|| view->sampled_us == 0)
		return CLUSTER_STORAGE_CHECK_INCOMPLETE;
	if (now < view->sampled_us)
		return CLUSTER_STORAGE_CHECK_CLOCK_BEFORE_SAMPLE;
	if (now >= view->expires_us)
		return CLUSTER_STORAGE_CHECK_EXPIRED;
	if ((view->members[cluster_node_id / 64] & (UINT64_C(1) << (cluster_node_id % 64))) == 0)
		return CLUSTER_STORAGE_CHECK_SELF_ABSENT;
	return CLUSTER_STORAGE_CHECK_ALLOWED;
}

/* No new authority is created here: this only narrows existing DB admission. */
bool
cluster_storage_quorum_allows_node(int node_id)
{
	return cluster_storage_quorum_check_node(node_id, NULL);
}

/* The optional output captures the same bounded snapshot attempt and predicate
 * inputs; it adds no resampling or authority. Diagnostics never change the verdict. */
bool
cluster_storage_quorum_check_node(int node_id, ClusterStorageQuorumCheck *out)
{
	ClusterStorageQuorumView view;
	ClusterStorageCheckResult result;
	StorageSnapshotWait wait = { 0 };
	uint64 now;

	if (out != NULL) {
		memset(out, 0, sizeof(*out));
		out->target_node = node_id;
		out->self_node = cluster_node_id;
	}
	if (!cluster_shared_config) {
		result = CLUSTER_STORAGE_CHECK_NATIVE;
		goto done;
	}
	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES) {
		result = CLUSTER_STORAGE_CHECK_INVALID_TARGET;
		goto done;
	}
	if (!storage_snapshot(&view, out, &wait)) {
		result = storage_state == NULL ? CLUSTER_STORAGE_CHECK_UNATTACHED
									   : CLUSTER_STORAGE_CHECK_UNSTABLE;
		goto done;
	}
	now = cluster_storage_quorum_now_us();
	if (wait.started_us != 0 && !storage_snapshot_time_valid(&wait, now)) {
		result = CLUSTER_STORAGE_CHECK_UNSTABLE;
		goto done;
	}
	if (out != NULL) {
		out->stable = true;
		out->now_us = now;
		out->view = view;
	}
	result = storage_view_result(&view, now);
	if (result == CLUSTER_STORAGE_CHECK_ALLOWED
		&& (view.members[node_id / 64] & (UINT64_C(1) << (node_id % 64))) == 0)
		result = CLUSTER_STORAGE_CHECK_TARGET_ABSENT;
done:
	if (out != NULL) {
		out->result = result;
		out->snapshot_stop = wait.stop;
		out->wait_count = wait.count;
		out->wait_started_us = wait.started_us;
		out->wait_sampled_us = wait.sampled_us;
	}
	return result == CLUSTER_STORAGE_CHECK_ALLOWED || result == CLUSTER_STORAGE_CHECK_NATIVE;
}

bool
cluster_storage_quorum_allows_members(uint64 members_lo, uint64 members_hi)
{
	ClusterStorageQuorumView view;
	StorageSnapshotWait wait = { 0 };
	uint64 now;

	if (!cluster_shared_config)
		return true;
	if ((members_lo | members_hi) == 0 || !storage_snapshot(&view, NULL, &wait))
		return false;
	now = cluster_storage_quorum_now_us();
	if (wait.started_us != 0 && !storage_snapshot_time_valid(&wait, now))
		return false;
	return storage_view_result(&view, now) == CLUSTER_STORAGE_CHECK_ALLOWED
		   && (members_lo & ~view.members[0]) == 0 && (members_hi & ~view.members[1]) == 0;
}
