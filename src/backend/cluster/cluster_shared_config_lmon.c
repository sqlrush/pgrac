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
#include "cluster/cluster_shared_config.h"

static bool selection_pending;
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
	return !cluster_shared_config_node_census(ref, node_id, &census)
		   || census.waiting_processes != 0 || census.failed_processes != 0
		   || census.parallel_processes != 0;
}

void
cluster_shared_config_delivery_lmon_cancel(void)
{
	if (!IsUnderPostmaster || MyBackendType != B_LMON)
		return;
	cluster_shared_config_free(&selected_image);
	cluster_control_root_config_cancel();
	selection_pending = false;
	next_probe = 0;
}

void
cluster_shared_config_delivery_lmon_tick(void)
{
	ClusterSharedConfigProcess actual;
	ClusterSharedConfigSelected selected;
	ClusterControlRootResult result;
	TimestampTz now;
	if (!IsUnderPostmaster || MyBackendType != B_LMON)
		return;
	if (!cluster_shared_config || !cluster_enabled || cluster_normal_stop_requested()
		|| cluster_clean_leave_node_refuses_writes()) {
		cluster_shared_config_delivery_lmon_cancel();
		return;
	}
	now = GetCurrentTimestamp();
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
		result = cluster_control_root_config_poll(&actual.ref, &selected, &selected_image);
		selection_pending = result == CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
		if (!selection_pending)
			next_probe = now + INT64CONST(1000000);
		if ((result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
			 || result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
			&& cluster_shared_config_delivery_publish(&selected.ref, &selected_image)
			&& delivery_needs_signal(&selected.ref, actual.node_id)) {
			/* No receipt is inferred from kill's return. A missing/failed
			 * signal leaves real outcomes old and will be retried. */
			if (PostmasterPid > 0)
				(void)kill(PostmasterPid, SIGHUP);
		}
		cluster_shared_config_free(&selected_image);
	}
	PG_CATCH();
	{
		cluster_shared_config_delivery_lmon_cancel();
		PG_RE_THROW();
	}
	PG_END_TRY();
}
