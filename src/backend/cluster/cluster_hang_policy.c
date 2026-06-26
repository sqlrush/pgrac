/*-------------------------------------------------------------------------
 *
 * cluster_hang_policy.c
 *	  pgrac Hang Manager pure decision/policy layer (spec-5.11).
 *
 *	  This file holds the side-effect-free decisions of the Hang Manager:
 *	  the long-wait threshold test, the coarse wait-source tag, the
 *	  exclusion rules, the wait-duration kind (§0.2 source matrix), the
 *	  completeness / forward-safety quality label, and the bounded top-N
 *	  sample store with its consistent-snapshot publish/read protocol.
 *
 *	  Keeping these helpers free of PostgreSQL runtime dependencies (only
 *	  the DIAG LWLock is touched, for the snapshot protocol) lets
 *	  cluster_unit exercise the whole policy surface directly.  The runtime
 *	  gathering (pgstat + lock snapshots) and DIAG-loop / ProcSignal glue
 *	  live in cluster_hang.c.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_hang_policy.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-5.11-hang-manager-skeleton.md (§2.1b / §2.2 / §3.2 / §3.3).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "cluster/cluster_hang.h"
#include "miscadmin.h"
#include "utils/backend_status.h"
#include "utils/wait_event.h"

/* High byte of wait_event_info encodes the wait class (wait_event.h). */
#define CLUSTER_HANG_WAIT_CLASS(info) ((info) & 0xFF000000U)


/*
 * cluster_hang_wait_exceeds_threshold
 *
 *	True when a measured wait duration reaches the configured threshold.
 *	A non-positive duration (clock skew / not actually waiting) never
 *	qualifies — fail-OPEN means we drop the doubtful sample, not flag it.
 */
bool
cluster_hang_wait_exceeds_threshold(int64 duration_us, int threshold_ms)
{
	if (duration_us <= 0)
		return false;
	return duration_us >= (int64)threshold_ms * INT64CONST(1000);
}


/*
 * cluster_hang_classify_wait_source
 *
 *	Coarse wait-source tag.  Cluster waits (Cache Fusion, undo) share
 *	generic PG wait classes, so we refine by event name when one is
 *	available; otherwise we classify by wait class.  The full feature-054
 *	classifier is forward to #54 — this is only "enough to read the dump".
 */
ClusterHangWaitSource
cluster_hang_classify_wait_source(uint32 wait_event_info, const char *wait_event_name)
{
	if (wait_event_name != NULL) {
		if (strstr(wait_event_name, "Undo") != NULL)
			return HANG_WAIT_UNDO;
		if (strstr(wait_event_name, "Cf") != NULL || strstr(wait_event_name, "CacheFusion") != NULL)
			return HANG_WAIT_CACHE_FUSION;
	}

	switch (CLUSTER_HANG_WAIT_CLASS(wait_event_info)) {
	case PG_WAIT_LOCK:
		return HANG_WAIT_LOCK;
	case PG_WAIT_LWLOCK:
		return HANG_WAIT_LWLOCK;
	case PG_WAIT_IO:
		return HANG_WAIT_IO;
	default:
		return HANG_WAIT_UNKNOWN;
	}
}


/*
 * cluster_hang_wait_is_idle_class
 *
 *	True when the backend is not waiting on a resource the hang manager
 *	cares about: not waiting at all (info == 0), waiting for the client
 *	(PG_WAIT_CLIENT), or a normal idle activity wait (PG_WAIT_ACTIVITY).
 *	Used to exclude idle / idle-in-transaction client waits (spec L4).
 */
bool
cluster_hang_wait_is_idle_class(uint32 wait_event_info)
{
	uint32 class = CLUSTER_HANG_WAIT_CLASS(wait_event_info);

	return class == 0 || class == PG_WAIT_CLIENT || class == PG_WAIT_ACTIVITY;
}


/*
 * cluster_hang_duration_kind
 *
 *	TRUE when a real wait-start timestamp is available (local heavyweight
 *	LOCK via LockInstanceData.waitStart, or a D0-confirmed GES wait_start);
 *	APPROX otherwise (GES default / TX / generic wait).  See §0.2 matrix.
 */
ClusterHangDurationKind
cluster_hang_duration_kind(bool has_true_wait_start)
{
	return has_true_wait_start ? HANG_DUR_TRUE : HANG_DUR_APPROX;
}


/*
 * cluster_hang_quality
 *
 *	Resolve the completeness label from blocker + duration facts.  The
 *	blocker conditions take precedence (a sample with a vanished or remote
 *	blocker can never be COMPLETE regardless of duration kind), then the
 *	soft-blocker-only case, then the duration kind decides COMPLETE vs
 *	APPROXIMATE.  This label is the forward-safety contract for spec-5.12.
 */
ClusterHangSampleQuality
cluster_hang_quality(ClusterHangDurationKind dk, int blocker_pid, int blocker_remote_node,
					 bool blocker_gone, bool soft_blocker_only)
{
	if (blocker_gone)
		return HANG_SAMPLE_BLOCKER_GONE;
	if (blocker_remote_node >= 0)
		return HANG_SAMPLE_REMOTE_BOUNDARY;
	if (soft_blocker_only)
		return HANG_SAMPLE_INCOMPLETE;
	if (dk == HANG_DUR_TRUE)
		return HANG_SAMPLE_COMPLETE;
	return HANG_SAMPLE_APPROXIMATE;
}


/*
 * cluster_hang_exclude_reason
 *
 *	Decide whether a waiting backend is counted as a hang candidate.
 *	Precedence: a confirmed-deadlock waiter is owned by the deadlock
 *	detector; background workers are out of scope for the skeleton; idle /
 *	idle-in-transaction client waits and "not actually waiting" backends
 *	are not resource hangs.  Everything else is a genuine candidate.
 */
ClusterHangExcludeReason
cluster_hang_exclude_reason(int backend_state, int backend_type, bool in_confirmed_deadlock,
							uint32 wait_event_info)
{
	if (in_confirmed_deadlock)
		return HANG_EXCLUDE_DEADLOCK;
	if (backend_type == B_BG_WORKER)
		return HANG_EXCLUDE_BGWORKER;
	if (backend_state == STATE_IDLE)
		return HANG_EXCLUDE_IDLE;
	if (cluster_hang_wait_is_idle_class(wait_event_info))
		return HANG_EXCLUDE_IDLE;
	return HANG_EXCLUDE_NONE;
}


/*
 * cluster_hang_sample_actionable
 *
 *	forward-safety gate (§3.3): spec-5.12 may only consider a sample for
 *	remediation when it is provably COMPLETE and is not already a confirmed
 *	deadlock waiter.  Everything else is observational only.
 */
bool
cluster_hang_sample_actionable(const ClusterHangSampleSlot *slot)
{
	return slot->quality == HANG_SAMPLE_COMPLETE && !slot->in_confirmed_deadlock;
}


/*
 * cluster_hang_store_reset
 *
 *	Clear a working store before building a fresh sampling round.  Leaves
 *	sample_epoch alone (epochs are only meaningful on the shared store, and
 *	are advanced by cluster_hang_store_publish()).
 */
void
cluster_hang_store_reset(ClusterHangSampleStore *store)
{
	store->n_samples = 0;
	store->truncated = false;
}


/*
 * cluster_hang_store_consider
 *
 *	Offer a long-wait candidate to a working store, keeping at most
 *	min(max_samples, CLUSTER_HANG_MAX_SAMPLES) entries ordered by longest
 *	duration.  When the store is full we replace the shortest-held sample
 *	if the candidate is longer; either way a dropped long-wait sets
 *	truncated = true so the dump can surface that the view is partial
 *	(spec §2.1b — truncation is never silent).
 */
void
cluster_hang_store_consider(ClusterHangSampleStore *store, const ClusterHangSampleSlot *cand,
							int max_samples)
{
	int cap = Min(max_samples, CLUSTER_HANG_MAX_SAMPLES);
	int i;
	int min_idx;

	if (cap < 1) {
		store->truncated = true;
		return;
	}

	if (store->n_samples < cap) {
		store->slots[store->n_samples++] = *cand;
		return;
	}

	/* Full: find the shortest-held sample. */
	min_idx = 0;
	for (i = 1; i < store->n_samples; i++) {
		if (store->slots[i].duration_us < store->slots[min_idx].duration_us)
			min_idx = i;
	}

	store->truncated = true;
	if (cand->duration_us > store->slots[min_idx].duration_us)
		store->slots[min_idx] = *cand;
}


/*
 * cluster_hang_store_publish
 *
 *	Atomically replace the shared store with a completed round under the
 *	DIAG LWLock (LW_EXCLUSIVE), advancing sample_epoch.  O(n) copy with no
 *	nested locks (n <= CLUSTER_HANG_MAX_SAMPLES, DIAG-only writer).
 */
void
cluster_hang_store_publish(ClusterHangSampleStore *shared, LWLock *lock,
						   const ClusterHangSampleStore *round)
{
	int n = round->n_samples;

	if (n > CLUSTER_HANG_MAX_SAMPLES)
		n = CLUSTER_HANG_MAX_SAMPLES;

	LWLockAcquire(lock, LW_EXCLUSIVE);
	shared->n_samples = n;
	shared->truncated = round->truncated;
	if (n > 0)
		memcpy(shared->slots, round->slots, sizeof(ClusterHangSampleSlot) * n);
	shared->sample_epoch++;
	LWLockRelease(lock);
}


/*
 * cluster_hang_store_snapshot
 *
 *	Copy a consistent snapshot of the shared store under LW_SHARED into
 *	*out (a caller-local buffer) and return the number of valid samples.
 *	The whole copy happens inside the lock so a reader can never observe a
 *	half-written round.
 */
int
cluster_hang_store_snapshot(const ClusterHangSampleStore *shared, LWLock *lock,
							ClusterHangSampleStore *out)
{
	int n;

	LWLockAcquire(lock, LW_SHARED);
	out->sample_epoch = shared->sample_epoch;
	out->n_samples = shared->n_samples;
	out->truncated = shared->truncated;
	n = shared->n_samples;
	if (n > CLUSTER_HANG_MAX_SAMPLES)
		n = CLUSTER_HANG_MAX_SAMPLES;
	if (n > 0)
		memcpy(out->slots, shared->slots, sizeof(ClusterHangSampleSlot) * n);
	LWLockRelease(lock);

	return out->n_samples;
}


/*
 * cluster_hang_node_to_slot
 *
 *	Distill a sampler-internal working node into a publishable POD slot,
 *	stamping the owning instance's node_id.  The being_resolved (5.9) and
 *	fairness_boosted (5.10) fields are carried straight through; in v1 they
 *	are always false because their readers are not compiled (spec-5.9 / 5.10 forward).
 */
void
cluster_hang_node_to_slot(const ClusterHangNode *node, int node_id, ClusterHangSampleSlot *slot)
{
	memset(slot, 0, sizeof(*slot));
	slot->pid = node->pid;
	slot->backendId = node->backendId;
	slot->node_id = node_id;
	strlcpy(slot->wait_event, node->wait_event, sizeof(slot->wait_event));
	slot->duration_kind = (uint8)node->duration_kind;
	slot->wait_since = node->wait_since;
	slot->duration_us = node->duration_us;
	slot->source = (uint8)node->source;
	slot->quality = (uint8)node->quality;
	slot->blocker_pid = node->blocker_pid;
	slot->blocker_backendId = node->blocker_backendId;
	slot->blocker_remote_node = node->blocker_remote_node;
	slot->in_confirmed_deadlock = node->in_confirmed_deadlock;
	slot->being_resolved = node->being_resolved;
	slot->fairness_boosted = node->fairness_boosted;
}
