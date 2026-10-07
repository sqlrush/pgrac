/*-------------------------------------------------------------------------
 * cluster_control_retire.c -- ordered, nonblocking LMON retirement service.
 * The sender fence and FIFO consumer establish a terminal request barrier.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_control_retire.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_ges.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_ir.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_startup_phase.h"
#include "cluster/cluster_wal_retention.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "utils/timestamp.h"

/* Pacing is not a correctness deadline: no elapsed-time path discards debt. */
#define CONTROL_RETIRE_RETRY_US INT64CONST(100000)
#define CONTROL_RETIRE_BATCH 16

static uint64 control_driver;
static uint32 control_cursor;
static struct {
	uint64 generation;
	TimestampTz sent_at;
} control_retry[CLUSTER_CONTROL_REQUEST_CAPACITY];

bool
cluster_control_retire_is_frame(const void *payload, Size length)
{
	const uint8 *bytes = payload;

	return bytes != NULL && length >= 4 && bytes[0] == CLUSTER_CONTROL_RETIRE_OPCODE
		   && bytes[1] == 0 && bytes[2] == 0 && bytes[3] == 0;
}

static bool
control_transport_ready(void)
{
	return cluster_qvotec_in_quorum()
		   && (!cluster_authority_readiness_managed() || cluster_serving_ready_is_current()
			   || cluster_recovery_transport_components_current()
			   || cluster_recovery_transport_is_current());
}

bool
cluster_control_retire_cut(const ClusterResId *resid, ClusterControlRequestCut *out)
{
	ClusterControlRequestCut cut;

	if (out == NULL || !cluster_control_request_resid_valid(resid))
		return false;
	memset(&cut, 0, sizeof(cut));
	cut.epoch = cluster_epoch_get_current();
	cut.master = cluster_grd_lookup_master_gen(resid, &cut.generation);
	if (cut.epoch == 0 || cut.generation == 0 || cut.master < 0 || cut.master >= CLUSTER_MAX_NODES
		|| cluster_epoch_get_current() != cut.epoch)
		return false;
	*out = cut;
	return true;
}

static bool
control_cut_current(const ClusterResId *resid, const ClusterControlRequestCut *before)
{
	ClusterControlRequestCut after;

	return cluster_control_retire_cut(resid, &after) && after.epoch == before->epoch
		   && after.generation == before->generation && after.master == before->master;
}

void
cluster_control_retire_lmon_start(void)
{
	Assert(MyBackendType == B_LMON);
	if (MyBackendType != B_LMON)
		return;
	control_driver = cluster_control_request_driver_start();
	control_cursor = 0;
	memset(control_retry, 0, sizeof(control_retry));
}

static void
control_receive_ack(const ClusterControlRetireMessage *message, int32 source)
{
	ClusterControlRequestCut cut;
	ClusterControlRequestOwner owner;

	if (message->key.holder.node_id != (uint32)cluster_node_id
		|| !cluster_control_retire_cut(&message->key.resid, &cut) || source != cut.master
		|| message->cleanup_epoch != cut.epoch || !control_cut_current(&message->key.resid, &cut))
		return;
	/* The registry retains the terminal cut. Its consumer must recheck that
	 * cut again before forgetting, so a concurrent epoch change is not lost. */
	if (cluster_control_request_ack_owner(message, source, control_driver, &cut, &owner)
		&& owner.incarnation != 0 && ProcGlobal != NULL && ProcGlobal->allProcs != NULL
		&& owner.procno < ProcGlobal->allProcCount
		&& ProcGlobal->allProcs[owner.procno].pid == owner.pid)
		SetLatch(&ProcGlobal->allProcs[owner.procno].procLatch);
	/* A latch is only a wake hint. The creator still rechecks the exact
	 * shared handle and current cut before releasing any local ownership. */
}

void
cluster_control_retire_lmon_tick(void)
{
	ClusterControlRequestView view;
	int sent = 0;
	TimestampTz now;

	if (MyBackendType != B_LMON || control_driver == 0 || !control_transport_ready())
		return;
	now = GetCurrentTimestamp();
	while (sent < CONTROL_RETIRE_BATCH && cluster_control_request_next(&control_cursor, &view)) {
		ClusterControlRequestCut cut;
		ClusterControlRetireMessage message;
		uint8 wire[CLUSTER_CONTROL_RETIRE_BYTES];
		uint32 slot = view.handle.slot;

		if (view.state != CLUSTER_CONTROL_REQUEST_ABANDONED
			&& view.state != CLUSTER_CONTROL_REQUEST_TERMINAL)
			continue;
		if (!cluster_control_retire_cut(&view.message.key.resid, &cut))
			continue;
		if (view.state == CLUSTER_CONTROL_REQUEST_TERMINAL && view.owner_exited
			&& control_cut_current(&view.message.key.resid, &cut)
			&& cluster_control_request_forget(&view.handle, &view.owner, &cut))
			continue;
		if (control_retry[slot].generation == view.handle.generation
			&& now >= control_retry[slot].sent_at
			&& now - control_retry[slot].sent_at < CONTROL_RETIRE_RETRY_US)
			continue;
		if (!cluster_control_request_claim(&view.handle, control_driver, &cut, &message)
			|| !cluster_control_retire_encode(&message, wire)
			|| !control_cut_current(&message.key.resid, &cut))
			continue;
		control_retry[slot].generation = view.handle.generation;
		control_retry[slot].sent_at = now;
		sent++;
		if (cut.master == cluster_node_id)
			(void)cluster_grd_work_queue_enqueue((uint32)cluster_node_id, wire, sizeof(wire));
		else
			(void)cluster_ic_send_envelope(PGRAC_IC_MSG_GES_REQUEST, cut.master, wire,
										   sizeof(wire));
		/* Every send result retains the same claim. Queue full, disconnect,
		 * send refusal and ACK loss all retry; none means RETIRED. */
	}
	if (control_cursor >= CLUSTER_CONTROL_REQUEST_CAPACITY)
		control_cursor = 0;
}

void
cluster_control_retire_ingress(const ClusterICEnvelope *env, const void *payload)
{
	ClusterControlRetireMessage message;
	ClusterControlRequestCut cut;

	if (MyBackendType != B_LMON || env == NULL || !control_transport_ready()
		|| !cluster_control_retire_decode(payload, env->payload_length, &message)
		|| env->epoch != cluster_epoch_get_current() || message.cleanup_epoch != env->epoch
		|| cluster_conf_lookup_node((int32)env->source_node_id) == NULL)
		return;
	if (message.verb != CLUSTER_CONTROL_RETIRE) {
		control_receive_ack(&message, (int32)env->source_node_id);
		return;
	}
	if (message.key.holder.node_id != env->source_node_id
		|| !cluster_control_retire_cut(&message.key.resid, &cut) || cut.master != cluster_node_id
		|| cut.epoch != message.cleanup_epoch)
		return;
	/* Never execute inline: even a local requester must follow all earlier
	 * accepted mutations through the single FIFO consumer. */
	(void)cluster_grd_work_queue_enqueue(env->source_node_id, payload, env->payload_length);
}

void
cluster_control_retire_drain(const ClusterGrdWorkItem *item)
{
	ClusterControlRetireMessage message;
	ClusterControlRequestCut cut;
	uint8 wire[CLUSTER_CONTROL_RETIRE_BYTES];

	if (MyBackendType != B_LMON || item == NULL || !control_transport_ready()
		|| !cluster_control_retire_decode(item->payload, item->payload_len, &message)
		|| message.verb != CLUSTER_CONTROL_RETIRE
		|| message.key.holder.node_id != item->source_node_id
		|| cluster_conf_lookup_node((int32)item->source_node_id) == NULL
		|| !cluster_control_retire_cut(&message.key.resid, &cut) || cut.master != cluster_node_id
		|| message.cleanup_epoch != cut.epoch || item->routing_generation != cut.generation)
		return;
	message.verb = cluster_ges_control_retire_at_master(&message, &cut);
	if (!control_cut_current(&message.key.resid, &cut) || !control_transport_ready())
		return;
	if (message.key.holder.node_id == (uint32)cluster_node_id)
		control_receive_ack(&message, cluster_node_id);
	else if (cluster_control_retire_encode(&message, wire))
		(void)cluster_ic_send_envelope(PGRAC_IC_MSG_GES_REQUEST, (int32)message.key.holder.node_id,
									   wire, sizeof(wire));
}

bool
cluster_control_retire_outbound_allowed(uint8 type, const void *payload, uint16 length)
{
	GesRequestPayload request;
	ClusterControlRequestKey key;

	if (type != PGRAC_IC_MSG_GES_REQUEST || payload == NULL || length != sizeof(request))
		return true;
	memcpy(&request, payload, sizeof(request));
	if (request.opcode != GES_REQ_OPCODE_REQUEST && request.opcode != GES_REQ_OPCODE_CONVERT
		&& request.opcode != GES_REQ_OPCODE_REQUEST_NOWAIT
		&& request.opcode != GES_REQ_OPCODE_REDECLARE)
		return true;
	memset(&key, 0, sizeof(key));
	memcpy(&key.resid, request.resid, sizeof(key.resid));
	if (key.resid.type != CLUSTER_CF_RESID_TYPE
		&& key.resid.type != CLUSTER_WAL_RETENTION_RESID_TYPE
		&& key.resid.type != CLUSTER_IR_RESID_TYPE)
		return true;
	key.holder.node_id = request.holder_node_id;
	key.holder.procno = request.holder_procno;
	key.holder.cluster_epoch
		= (uint64)request.holder_cluster_epoch_lo | ((uint64)request.holder_cluster_epoch_hi << 32);
	key.holder.request_id
		= (uint64)request.holder_request_id_lo | ((uint64)request.holder_request_id_hi << 32);
	return cluster_control_request_send_mode_allowed(&key, (LOCKMODE)request.lockmode);
}
