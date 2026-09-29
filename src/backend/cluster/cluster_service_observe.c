/*-------------------------------------------------------------------------
 *
 * cluster_service_observe.c
 *    Read-only fanout over the original native service responsibilities.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/backend/cluster/cluster_service_observe.c
 *
 * NOTES
 *    PGRAC-original local observation, shared by configuration and shutdown.
 *    It never retires an original owner or changes admission. The module polls
 *    use the normal-stop result vocabulary. The online entry does not require
 *    a stop request; the stop entry preserves the original CLOSE staging.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "cluster/cluster_control_request.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_service_observe.h"
#include "cluster/cluster_xid_stripe_boot.h"

typedef enum ServiceModule {
	SERVICE_GRD_WORK,
	SERVICE_GRD_OUTBOUND,
	SERVICE_CR,
	SERVICE_NATIVE_PROBE,
	SERVICE_GCS_LOCAL,
	SERVICE_TRANSPORT,
	SERVICE_SEMANTIC,
	SERVICE_SCN,
	SERVICE_RECONFIG,
	SERVICE_CLOSE,
	SERVICE_REMOVE,
	SERVICE_FENCE,
	SERVICE_WRITE_FENCE,
	SERVICE_CF,
	SERVICE_RECOVERY,
	SERVICE_BACKUP,
	SERVICE_MRP,
	SERVICE_GCS_DEDUP,
	SERVICE_GES_DEDUP,
	SERVICE_LMD_PROBE,
	SERVICE_CONTROL_REQUEST,
	SERVICE_LMS_OUTBOUND,
	SERVICE_LMD,
	SERVICE_LMD_PENDING,
	SERVICE_LMD_GRAPH,
	SERVICE_SINVAL,
	SERVICE_KO,
	SERVICE_XID_WRAP
} ServiceModule;

static const ServiceModule lmon_modules[]
	= { SERVICE_GRD_WORK,	  SERVICE_GRD_OUTBOUND, SERVICE_CR,
		SERVICE_NATIVE_PROBE, SERVICE_GCS_LOCAL,	SERVICE_TRANSPORT,
		SERVICE_SEMANTIC,	  SERVICE_SCN,			SERVICE_RECONFIG,
		SERVICE_CLOSE,		  SERVICE_REMOVE,		SERVICE_FENCE,
		SERVICE_WRITE_FENCE,  SERVICE_CF,			SERVICE_RECOVERY,
		SERVICE_BACKUP,		  SERVICE_MRP,			SERVICE_GCS_DEDUP,
		SERVICE_GES_DEDUP,	  SERVICE_LMD_PROBE,	SERVICE_CONTROL_REQUEST,
		SERVICE_XID_WRAP };
static const ServiceModule lms_modules[] = { SERVICE_CR, SERVICE_NATIVE_PROBE, SERVICE_GCS_LOCAL,
											 SERVICE_LMS_OUTBOUND, SERVICE_TRANSPORT };
static const ServiceModule lmd_modules[]
	= { SERVICE_LMD, SERVICE_LMD_PENDING, SERVICE_LMD_GRAPH, SERVICE_LMD_PROBE };
static const ServiceModule sinval_modules[] = { SERVICE_SINVAL, SERVICE_KO };

static ClusterNormalStopPollResult
service_observe_one(ServiceModule module, bool stopping, ClusterServiceObservation *out)
{
	ClusterNormalStopPollResult result;
	switch (module) {
	case SERVICE_GRD_WORK:
		out->domain = "GRD_WORK";
		result = cluster_grd_work_queue_normal_stop_poll(&out->position, &out->reason);
		out->slot = (int)out->position;
		return result;
	case SERVICE_GRD_OUTBOUND:
		out->domain = "GRD_OUTBOUND";
		result = cluster_grd_outbound_normal_stop_poll(&out->position, &out->reason);
		out->slot = (int)out->position;
		return result;
	case SERVICE_CR:
		out->domain = "CR";
		return cluster_cr_server_normal_stop_poll(&out->slot, &out->reason);
	case SERVICE_NATIVE_PROBE:
		out->domain = "NATIVE_PROBE";
		return cluster_lms_native_probe_normal_stop_poll(&out->slot, &out->reason);
	case SERVICE_GCS_LOCAL:
		out->domain = "GCS_LOCAL";
		return cluster_gcs_block_normal_stop_local_poll(&out->slot, &out->reason);
	case SERVICE_TRANSPORT:
		return cluster_ic_normal_stop_poll(&out->domain, &out->slot, &out->position, &out->reason);
	case SERVICE_SEMANTIC:
		return cluster_semantic_normal_stop_poll(&out->domain, &out->key, &out->reason);
	case SERVICE_SCN:
		return cluster_scn_normal_stop_poll(&out->domain, &out->key, &out->reason);
	case SERVICE_RECONFIG:
		return cluster_reconfig_normal_stop_poll(&out->domain, &out->key, &out->reason);
	case SERVICE_CLOSE:
		out->domain = "CLOSE_CONTROL";
		return stopping ? cluster_clean_leave_normal_stop_local_poll(&out->slot, &out->reason)
						: cluster_clean_leave_service_poll(&out->slot, &out->reason);
	case SERVICE_REMOVE:
		return cluster_node_remove_normal_stop_poll(&out->domain, &out->key, &out->reason);
	case SERVICE_FENCE:
		return cluster_fence_normal_stop_poll(&out->domain, &out->key, &out->reason);
	case SERVICE_WRITE_FENCE:
		return cluster_write_fence_normal_stop_poll(&out->domain, &out->key, &out->reason);
	case SERVICE_CF:
		out->domain = "CF";
		/* Own retained CF work is never exempt. The checkpoint, not this
		 * local observer, consumes the separate node-wide join obligation. */
		return cluster_cf_normal_stop_poll(false, &out->reason);
	case SERVICE_RECOVERY:
		return cluster_recovery_normal_stop_poll(&out->domain, &out->key, &out->reason);
	case SERVICE_BACKUP:
		return cluster_backup_normal_stop_poll(&out->domain, &out->key, &out->reason);
	case SERVICE_MRP:
		return cluster_mrp_normal_stop_poll(&out->domain, &out->key, &out->reason);
	case SERVICE_GCS_DEDUP:
		return cluster_gcs_dedup_normal_stop_poll(&out->domain, &out->key, &out->reason);
	case SERVICE_GES_DEDUP:
		return cluster_ges_dedup_normal_stop_poll(&out->domain, &out->key, &out->reason);
	case SERVICE_LMD_PROBE:
		out->domain = "LMD_PROBE";
		return cluster_lmd_probe_normal_stop_poll(&out->key, &out->reason);
	case SERVICE_CONTROL_REQUEST:
		out->domain = "CONTROL_REQUEST";
		out->reason = "OWNED_OR_UNINITIALIZED";
		return !cluster_shared_config || cluster_control_request_empty()
				   ? CLUSTER_NORMAL_STOP_READY
				   : CLUSTER_NORMAL_STOP_PENDING;
	case SERVICE_LMS_OUTBOUND:
		out->domain = "OUTBOUND";
		return cluster_lms_outbound_normal_stop_poll(&out->slot, &out->position, &out->reason);
	case SERVICE_LMD:
		return cluster_lmd_normal_stop_poll(&out->domain, &out->key, &out->reason);
	case SERVICE_LMD_PENDING:
		out->domain = "PENDING_CANCEL";
		return cluster_lmd_pending_normal_stop_poll(&out->key, &out->reason);
	case SERVICE_LMD_GRAPH:
		out->domain = "WAIT_GRAPH";
		return cluster_lmd_graph_normal_stop_poll(&out->key, &out->reason);
	case SERVICE_SINVAL:
		return cluster_sinval_normal_stop_poll(&out->domain, &out->key, &out->reason);
	case SERVICE_KO:
		out->domain = "KO";
		result = cluster_ko_normal_stop_poll(&out->position, &out->reason);
		out->slot = (int)out->position;
		return result;
	case SERVICE_XID_WRAP: {
		bool pending;
		out->domain = "XID_WRAP";
		out->reason = "UNAVAILABLE";
		if (!cluster_xid_wrap_barrier_observe(&pending))
			return CLUSTER_NORMAL_STOP_INVALID;
		out->reason = pending ? "ADMITTED_ROUND" : "NONE";
		return pending ? CLUSTER_NORMAL_STOP_PENDING : CLUSTER_NORMAL_STOP_READY;
	}
	}
	return CLUSTER_NORMAL_STOP_INVALID;
}

/*
 * service_observe -- Inspect every responsibility of this real role.
 *
 * Inputs: non-NULL result storage; actual native role at an unlocked boundary.
 * Returns: READY, PENDING, or INVALID, with the first highest-priority cause.
 * Side effects: only the original read-only module observations. No stop seal,
 * cancellation, log, wait-for-completion or configuration application.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static ClusterNormalStopPollResult
service_observe(bool stopping, ClusterServiceObservation *out)
{
	const ServiceModule *modules;
	unsigned count;
	ClusterNormalStopPollResult aggregate = CLUSTER_NORMAL_STOP_READY;
	if (out == NULL)
		return CLUSTER_NORMAL_STOP_INVALID;
	memset(out, 0, sizeof(*out));
	out->domain = "NATIVE_SERVICE";
	out->reason = "OWNER_MISMATCH";
	out->slot = -1;
	if (!IsUnderPostmaster)
		return CLUSTER_NORMAL_STOP_INVALID;
	if (MyBackendType == B_LMON && AmLmonProcess()) {
		modules = lmon_modules;
		count = lengthof(lmon_modules);
		/* Online configuration has a retained fresh-wrap cut. Do not silently
		 * change the separate existing shutdown contract in this adapter. */
		if (stopping)
			--count;
	} else if ((MyBackendType == B_LMS && AmLmsProcess())
			   || (MyBackendType == B_LMS_WORKER && AmLmsWorkerProcess())) {
		modules = lms_modules;
		count = lengthof(lms_modules);
	} else if (MyBackendType == B_LMD && AmLmdProcess()) {
		modules = lmd_modules;
		count = lengthof(lmd_modules);
	} else if (MyBackendType == B_SINVAL_BCAST && AmSinvalBcastProcess()) {
		modules = sinval_modules;
		count = lengthof(sinval_modules);
	} else
		return CLUSTER_NORMAL_STOP_INVALID;
	out->domain = out->reason = "NONE";
	for (unsigned i = 0; i < count; ++i) {
		ClusterServiceObservation part = { .domain = "NONE", .reason = "NONE", .slot = -1 };
		ClusterNormalStopPollResult result = service_observe_one(modules[i], stopping, &part);
		if (result != CLUSTER_NORMAL_STOP_READY && result != CLUSTER_NORMAL_STOP_PENDING)
			result = CLUSTER_NORMAL_STOP_INVALID;
		if ((result == CLUSTER_NORMAL_STOP_INVALID && aggregate != CLUSTER_NORMAL_STOP_INVALID)
			|| (result == CLUSTER_NORMAL_STOP_PENDING && aggregate == CLUSTER_NORMAL_STOP_READY)) {
			aggregate = result;
			*out = part;
		}
	}
	out->modules = count;
	return aggregate;
}

ClusterNormalStopPollResult
cluster_service_observe(ClusterServiceObservation *out)
{
	return service_observe(false, out);
}

ClusterNormalStopPollResult
cluster_service_normal_stop_observe(ClusterServiceObservation *out)
{
	return service_observe(true, out);
}
