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
	uint64 generation;

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
	pg_atomic_fetch_add_u32(&storage_state->sequence, 1);
	pg_write_barrier();
	pg_atomic_write_u32(&storage_state->reason, view.reason);
	pg_atomic_write_u32(&storage_state->ring_node, view.ring_node);
	pg_atomic_write_u32(&storage_state->provider_diagnostic, view.provider_diagnostic);
	pg_atomic_write_u64(&storage_state->ring_sequence, view.ring_sequence);
	pg_atomic_write_u64(&storage_state->members[0], view.members[0]);
	pg_atomic_write_u64(&storage_state->members[1], view.members[1]);
	pg_atomic_write_u64(&storage_state->sampled_us, view.sampled_us);
	pg_atomic_write_u64(&storage_state->expires_us, view.expires_us);
	pg_atomic_write_u64(&storage_state->generation,
						generation == UINT64_MAX ? generation : generation + 1);
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

/* Obtain one stable view. The expiry is never extended by readers. */
static bool
storage_snapshot(ClusterStorageQuorumView *out, ClusterStorageQuorumCheck *check)
{
	int retry;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (storage_state == NULL)
		return false;
	for (retry = 0; retry < 4; retry++) {
		uint32 before = pg_atomic_read_u32(&storage_state->sequence);
		uint32 after;

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
		out->provider_diagnostic = pg_atomic_read_u32(&storage_state->provider_diagnostic);
		pg_read_barrier();
		after = pg_atomic_read_u32(&storage_state->sequence);
		if (check != NULL)
			check->sequence_after = after;
		if (before == after)
			return true;
	}
	memset(out, 0, sizeof(*out));
	return false;
}

bool
cluster_storage_quorum_snapshot(ClusterStorageQuorumView *out)
{
	return storage_snapshot(out, NULL);
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

static bool
storage_view_current(const ClusterStorageQuorumView *view)
{
	return storage_view_result(view, cluster_storage_quorum_now_us())
		   == CLUSTER_STORAGE_CHECK_ALLOWED;
}

/* No new authority is created here: this only narrows existing DB admission. */
bool
cluster_storage_quorum_allows_node(int node_id)
{
	return cluster_storage_quorum_check_node(node_id, NULL);
}

/* The optional output captures the same predicate inputs, with no resample,
 * extra retry, or authority. Provider diagnostics never affect the verdict. */
bool
cluster_storage_quorum_check_node(int node_id, ClusterStorageQuorumCheck *out)
{
	ClusterStorageQuorumView view;
	ClusterStorageCheckResult result;
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
	if (!storage_snapshot(&view, out)) {
		result = storage_state == NULL ? CLUSTER_STORAGE_CHECK_UNATTACHED
									   : CLUSTER_STORAGE_CHECK_UNSTABLE;
		goto done;
	}
	now = cluster_storage_quorum_now_us();
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
	if (out != NULL)
		out->result = result;
	return result == CLUSTER_STORAGE_CHECK_ALLOWED || result == CLUSTER_STORAGE_CHECK_NATIVE;
}

bool
cluster_storage_quorum_allows_members(uint64 members_lo, uint64 members_hi)
{
	ClusterStorageQuorumView view;

	if (!cluster_shared_config)
		return true;
	return (members_lo | members_hi) != 0 && cluster_storage_quorum_snapshot(&view)
		   && storage_view_current(&view) && (members_lo & ~view.members[0]) == 0
		   && (members_hi & ~view.members[1]) == 0;
}
