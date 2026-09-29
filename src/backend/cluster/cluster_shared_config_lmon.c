/*-------------------------------------------------------------------------
 *
 * cluster_shared_config_lmon.c
 *    Background owner of selected configuration delivery, not node admission.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/backend/cluster/cluster_shared_config_lmon.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <signal.h>
#include "miscadmin.h"
#include "postmaster/syslogger.h"
#include "storage/proc.h"
#include "utils/timestamp.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_shared_config.h"
#include "cluster/cluster_config_members.h"
#include "cluster/cluster_config_channels.h"
#include "cluster/cluster_config_producers.h"

static bool selection_pending;
static ClusterR4MembershipSnapshot selection_members;
static TimestampTz next_probe;
/* Static solely for ERROR cleanup of an owned returned image. It is not an
 * extra configuration authority, and is empty between successful ticks. */
static ClusterSharedConfigImage selected_image;

/* Notification suppression only, NEVER node admission. This bounded census
 * need not freeze concurrent process births/exits: future forks inherit the
 * actual parent state, and the next maintenance tick rechecks missed changes.
 * Pending/deferred counts are real outcomes, not missing delivery; the later
 * dependent-use owner must separately decide whether those outcomes suffice.
 */
static bool
delivery_needs_signal(const ClusterSharedConfigRef *ref, int node_id)
{
	ClusterSharedConfigCensus census;
	/* A legal session override can invalidate raw diagnostics without changing
	 * common POSTMASTER/SIGHUP values. Do not repeatedly signal the family for
	 * that expected state; old/failed/unknown common application still retries. */
	return !cluster_shared_config_node_common_census(ref, node_id, &census)
		   || census.waiting_processes != 0 || census.failed_processes != 0
		   || census.parallel_processes != 0 || census.active_missing_processes != 0;
}

void
cluster_shared_config_delivery_lmon_cancel(void)
{
	if (!IsUnderPostmaster || MyBackendType != B_LMON)
		return;
	cluster_shared_config_free(&selected_image);
	cluster_control_root_config_cancel();
	cluster_config_members_cancel();
	cluster_config_channels_cancel();
	selection_pending = false;
	memset(&selection_members, 0, sizeof(selection_members));
	next_probe = 0;
}

/* Receiver-local observation generations can advance at every healthy poll.
 * Both snapshots independently prove freshness; only actual MEMBER identity
 * belongs to this retained attempt. Never turn heartbeat progress into an ABA
 * identity or impose a cluster-global meaning on those local counters. */
static bool
delivery_same_members(const ClusterR4MembershipSnapshot *a, const ClusterR4MembershipSnapshot *b)
{
	return a->formation_epoch == b->formation_epoch
		   && a->admitted_members_lo == b->admitted_members_lo
		   && a->admitted_members_hi == b->admitted_members_hi
		   && a->local_self_boot_incarnation == b->local_self_boot_incarnation
		   && memcmp(a->admitted_incarnation, b->admitted_incarnation,
					 sizeof(a->admitted_incarnation))
				  == 0;
}

static void
delivery_attempt(const ClusterSharedConfigProcess *actual, TimestampTz now)
{
	ClusterR4MembershipSnapshot before, after;
	ClusterSharedConfigSelected selected;
	ClusterControlRootResult result;

	if (cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES
		|| actual->node_id != (uint32)cluster_node_id
		|| !cluster_reconfig_lmon_snapshot_r4_membership(&before)
		|| (selection_pending && !delivery_same_members(&selection_members, &before))) {
		cluster_shared_config_delivery_lmon_cancel();
		return;
	}
	if (!selection_pending)
		selection_members = before;
	result = cluster_control_root_config_poll(&actual->ref, &selected, &selected_image);
	selection_pending = result == CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	if (!selection_pending)
		next_probe = now + INT64CONST(1000000);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		if (!selection_pending)
			cluster_config_members_cancel();
		return;
	}
	if (!cluster_reconfig_lmon_snapshot_r4_membership(&after)
		|| !delivery_same_members(&selection_members, &after)
		|| (after.admitted_members_lo & ~selected.ref.identity.configured[0]) != 0
		|| (after.admitted_members_hi & ~selected.ref.identity.configured[1]) != 0) {
		cluster_shared_config_delivery_lmon_cancel();
		return;
	}
	if (!cluster_shared_config_delivery_publish(&selected.ref, &selected_image)) {
		cluster_config_members_cancel();
		return;
	}
	cluster_config_members_poll(&selected.ref, &after);
	if (delivery_needs_signal(&selected.ref, actual->node_id)) {
		/* No receipt is inferred from kill's return. A missing/failed
		 * signal leaves real outcomes old and will be retried. */
		if (PostmasterPid > 0)
			(void)kill(PostmasterPid, SIGHUP);
	}
}

void
cluster_shared_config_delivery_lmon_tick(void)
{
	ClusterSharedConfigProcess actual;
	TimestampTz now;
	if (!IsUnderPostmaster || MyBackendType != B_LMON)
		return;
	if (!cluster_shared_config || !cluster_enabled || cluster_normal_stop_requested()
		|| cluster_clean_leave_node_refuses_writes()) {
		cluster_shared_config_delivery_lmon_cancel();
		return;
	}
	now = GetCurrentTimestamp();
	/* Discovery is fresh work. An already owned CF request must continue to
	 * its original terminal state even after the native FRONT cut closes. */
	if (!selection_pending
		&& !cluster_config_producers_fresh_allowed(CLUSTER_CONFIG_PRODUCERS_FRONT))
		return;
	/* Prefix/service readiness is driven at the original LMON idle boundary,
	 * not inside this CF selection duty or its retained work bracket. */
	/* Scheduling, not a proof/expiry/deadline. A pending exact CF request is
	 * driven every tick; a backward clock step cannot strand maintenance. */
	if (!selection_pending && now < next_probe && next_probe - now <= INT64CONST(1000000))
		return;
	if (!cluster_shared_config_process_observe(&actual) || actual.ref.identity.generation == 0) {
		cluster_shared_config_delivery_lmon_cancel();
		return;
	}
	PG_TRY();
	{
		delivery_attempt(&actual, now);
		cluster_shared_config_free(&selected_image);
	}
	PG_CATCH();
	{
		cluster_shared_config_delivery_lmon_cancel();
		PG_RE_THROW();
	}
	PG_END_TRY();
}
