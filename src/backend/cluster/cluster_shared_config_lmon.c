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
#include "utils/timestamp.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_shared_config.h"
#include "cluster/cluster_config_members.h"

static bool selection_pending;
static TimestampTz next_probe;
/* A successful signal means notification, never process application. */
static ClusterSharedConfigRef notified_ref;
static bool notified;
/* Static solely for ERROR cleanup of an owned returned image. It is not an
 * extra configuration authority, and is empty between successful ticks. */
static ClusterSharedConfigImage selected_image;

void
cluster_shared_config_delivery_lmon_cancel(void)
{
	if (!IsUnderPostmaster || MyBackendType != B_LMON)
		return;
	cluster_shared_config_free(&selected_image);
	cluster_control_root_config_cancel();
	cluster_config_members_cancel();
	selection_pending = false;
	notified = false;
	memset(&notified_ref, 0, sizeof(notified_ref));
	next_probe = 0;
}

static void
delivery_attempt(const ClusterSharedConfigProcess *actual, TimestampTz now)
{
	ClusterR4MembershipSnapshot members;
	ClusterSharedConfigSelected selected;
	ClusterControlRootResult result;

	if (cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES
		|| actual->node_id != (uint32)cluster_node_id) {
		cluster_shared_config_delivery_lmon_cancel();
		return;
	}
	/* The CF owner already checks its exact grant and selected root identity.
	 * Ordinary SIGHUP delivery does not add an online member/application cut. */
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
	if (!notified || memcmp(&notified_ref, &selected.ref, sizeof(notified_ref)) != 0) {
		if (!cluster_shared_config_delivery_publish(&selected.ref, &selected_image)) {
			cluster_config_members_cancel();
			return;
		}
		/* Publish before signaling. Do not rewrite a notified image: a reader
		 * colliding with that unnecessary rewrite would have no later SIGHUP.
		 * A new image always has its own signal. Native postmaster fan-out owns
		 * reload, including failures and pending_restart; there is no ACK. */
		if (PostmasterPid > 0 && kill(PostmasterPid, SIGHUP) == 0) {
			notified_ref = selected.ref;
			notified = true;
		}
	}
	/* This independent observation is exclusively a boot/join equality proof.
	 * Missing or changed membership cannot withhold an ordinary native reload. */
	if (cluster_reconfig_lmon_snapshot_r4_membership(&members))
		cluster_config_members_poll(&selected.ref, &members);
	else
		cluster_config_members_cancel();
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
