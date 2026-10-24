/*-------------------------------------------------------------------------
 * cluster_ges_holder.c -- read-only queries on the original GES work owner.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_ges_holder.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_startup_phase.h"
#include "miscadmin.h"

StaticAssertDecl(CLUSTER_GES_HOLDER_PROBE_BYTES <= sizeof(((ClusterGrdWorkItem *)0)->payload),
				 "holder query must fit the original work queue");

static uint64
holder_get(const uint8 *p, int width)
{
	uint64 value = 0;
	for (int i = 0; i < width; i++)
		value |= (uint64)p[i] << (8 * i);
	return value;
}
static void
holder_put(uint8 *p, uint64 value, int width)
{
	for (int i = 0; i < width; i++)
		p[i] = (uint8)(value >> (8 * i));
}
static bool
holder_probe_valid(const ClusterGesHolderProbe *q)
{
	if (q == NULL || q->resid.type != LOCKTAG_RELATION
		|| q->resid.lockmethodid != DEFAULT_LOCKMETHOD || q->resid.field2 == InvalidOid
		|| q->resid.field3 != 0 || q->resid.field4 != 0 || q->holder.node_id >= CLUSTER_MAX_NODES
		|| q->holder.procno > INT_MAX || q->holder.cluster_epoch == 0
		|| q->holder.cluster_epoch == PG_UINT64_MAX || q->holder.request_id == 0
		|| q->holder.request_id == PG_UINT64_MAX || q->nonce == 0 || q->nonce == PG_UINT64_MAX
		|| q->collector_generation == 0 || q->collector_generation == PG_UINT64_MAX
		|| q->collector_boot == 0 || q->collector_boot == PG_UINT64_MAX || q->master_boot == 0
		|| q->master_boot == PG_UINT64_MAX)
		return false;
	if (!q->reply)
		return q->observation == CLUSTER_GRD_HOLDER_UNKNOWN && q->mode == NoLock;
	if (q->observation == CLUSTER_GRD_HOLDER_PRESENT)
		return q->mode >= AccessShareLock && q->mode <= AccessExclusiveLock;
	return (q->observation == CLUSTER_GRD_HOLDER_UNKNOWN
			|| q->observation == CLUSTER_GRD_HOLDER_ABSENT)
		   && q->mode == NoLock;
}

bool
cluster_ges_holder_probe_is_frame(const void *payload, Size length)
{
	const uint8 *p = payload;
	return p != NULL && length >= 4
		   && (holder_get(p, 4) == CLUSTER_GES_HOLDER_QUERY_OPCODE
			   || holder_get(p, 4) == CLUSTER_GES_HOLDER_REPLY_OPCODE);
}

bool
cluster_ges_holder_probe_encode(const ClusterGesHolderProbe *q, void *payload, Size length)
{
	uint8 *p = payload;
	if (p == NULL || length != CLUSTER_GES_HOLDER_PROBE_BYTES || !holder_probe_valid(q))
		return false;
	memset(p, 0, length);
	holder_put(p, q->reply ? CLUSTER_GES_HOLDER_REPLY_OPCODE : CLUSTER_GES_HOLDER_QUERY_OPCODE, 4);
	p[4] = 1;
	p[5] = (uint8)q->observation;
	p[6] = (uint8)q->mode;
	holder_put(p + 8, q->resid.field1, 4);
	holder_put(p + 12, q->resid.field2, 4);
	holder_put(p + 16, q->resid.field3, 4);
	holder_put(p + 20, q->resid.field4, 2);
	p[22] = q->resid.type;
	p[23] = q->resid.lockmethodid;
	holder_put(p + 24, q->holder.node_id, 4);
	holder_put(p + 28, q->holder.procno, 4);
	holder_put(p + 32, q->holder.cluster_epoch, 8);
	holder_put(p + 40, q->holder.request_id, 8);
	holder_put(p + 48, q->nonce, 8);
	holder_put(p + 56, q->collector_generation, 8);
	holder_put(p + 64, q->collector_boot, 8);
	holder_put(p + 72, q->master_boot, 8);
	return true;
}

bool
cluster_ges_holder_probe_decode(const void *payload, Size length, ClusterGesHolderProbe *out)
{
	const uint8 *p = payload;
	ClusterGesHolderProbe q;
	if (out == NULL || length != CLUSTER_GES_HOLDER_PROBE_BYTES
		|| !cluster_ges_holder_probe_is_frame(payload, length) || p[4] != 1 || p[7] != 0)
		return false;
	memset(&q, 0, sizeof(q));
	q.reply = holder_get(p, 4) == CLUSTER_GES_HOLDER_REPLY_OPCODE;
	q.observation = (ClusterGrdHolderObservation)p[5];
	q.mode = p[6];
	q.resid.field1 = holder_get(p + 8, 4);
	q.resid.field2 = holder_get(p + 12, 4);
	q.resid.field3 = holder_get(p + 16, 4);
	q.resid.field4 = holder_get(p + 20, 2);
	q.resid.type = p[22];
	q.resid.lockmethodid = p[23];
	q.holder.node_id = holder_get(p + 24, 4);
	q.holder.procno = holder_get(p + 28, 4);
	q.holder.cluster_epoch = holder_get(p + 32, 8);
	q.holder.request_id = holder_get(p + 40, 8);
	q.nonce = holder_get(p + 48, 8);
	q.collector_generation = holder_get(p + 56, 8);
	q.collector_boot = holder_get(p + 64, 8);
	q.master_boot = holder_get(p + 72, 8);
	if (!holder_probe_valid(&q))
		return false;
	*out = q;
	return true;
}

static bool
holder_probe_endpoint_current(int32 node, uint64 boot)
{
	uint32 word, connection;
	if (node < 0 || node >= CLUSTER_MAX_NODES)
		return false;
	if (node == cluster_node_id)
		return boot == cluster_qvotec_get_self_incarnation();
	return cluster_membership_get_state(node) == CLUSTER_MEMBER_MEMBER
		   && boot == cluster_membership_get_last_admitted_incarnation(node)
		   && cluster_sf_peer_capability_word_sample(node, PGRAC_IC_HELLO_CAP_KO_COMMIT_FENCE_V1,
													 &word, &connection)
		   && connection != 0 && (word & PGRAC_IC_HELLO_CAP_KO_COMMIT_FENCE_V1) != 0;
}

static bool
holder_probe_current(const ClusterGesHolderProbe *q, int32 collector, int32 master,
					 uint64 local_generation)
{
	uint64 generation;
	return cluster_enabled && cluster_shared_config && MyBackendType == B_LMON
		   && cluster_qvotec_in_quorum()
		   && (!cluster_authority_readiness_managed() || cluster_serving_ready_is_current())
		   && holder_probe_valid(q) && q->holder.cluster_epoch == cluster_epoch_get_current()
		   && holder_probe_endpoint_current(collector, q->collector_boot)
		   && holder_probe_endpoint_current(master, q->master_boot)
		   && cluster_grd_lookup_master_gen(&q->resid, &generation) == master && generation != 0
		   && generation != PG_UINT64_MAX
		   && (local_generation == 0 || generation == local_generation)
		   && q->holder.cluster_epoch == cluster_epoch_get_current();
}

bool
cluster_ges_holder_probe_send(int32 master, const ClusterGesHolderProbe *q)
{
	uint8 wire[CLUSTER_GES_HOLDER_PROBE_BYTES];
	if (q == NULL || q->reply
		|| !holder_probe_current(q, cluster_node_id, master, q->collector_generation)
		|| !cluster_ges_holder_probe_encode(q, wire, sizeof(wire)))
		return false;
	if (master == cluster_node_id)
		return cluster_grd_work_queue_enqueue((uint32)cluster_node_id, wire, sizeof(wire));
	return cluster_ic_send_envelope(PGRAC_IC_MSG_GES_REQUEST, master, wire, sizeof(wire))
		   == CLUSTER_IC_SEND_DONE;
}

void
cluster_ges_holder_probe_ingress(const ClusterICEnvelope *env, const void *payload)
{
	ClusterGesHolderProbe q;
	if (env == NULL || env->source_node_id == (uint32)cluster_node_id
		|| !cluster_ges_holder_probe_decode(payload, env->payload_length, &q)
		|| env->epoch != q.holder.cluster_epoch)
		return;
	if (q.reply) {
		if (holder_probe_current(&q, cluster_node_id, env->source_node_id, q.collector_generation))
			cluster_ko_peer_holder_observed(env->source_node_id, &q);
	} else if (holder_probe_current(&q, env->source_node_id, cluster_node_id, 0))
		(void)cluster_grd_work_queue_enqueue(env->source_node_id, payload, env->payload_length);
}

void
cluster_ges_holder_probe_drain(const ClusterGrdWorkItem *item)
{
	ClusterGesHolderProbe q;
	uint8 wire[CLUSTER_GES_HOLDER_PROBE_BYTES];
	if (item == NULL || !cluster_ges_holder_probe_decode(item->payload, item->payload_len, &q)
		|| q.reply || item->routing_generation == 0
		|| !holder_probe_current(&q, item->source_node_id, cluster_node_id,
								 item->routing_generation))
		return;
	/* The queue cut belongs to this receiver. The collector's wire token
	 * is immutable correlation data, not this node's LMS generation. */
	q.observation
		= cluster_grd_holder_observe(&q.resid, &q.holder, item->routing_generation, &q.mode);
	q.reply = true;
	if (!holder_probe_current(&q, item->source_node_id, cluster_node_id, item->routing_generation))
		return;
	if (item->source_node_id == (uint32)cluster_node_id) {
		if (holder_probe_current(&q, cluster_node_id, cluster_node_id, q.collector_generation))
			cluster_ko_peer_holder_observed(cluster_node_id, &q);
	} else if (cluster_ges_holder_probe_encode(&q, wire, sizeof(wire)))
		(void)cluster_ic_send_envelope(PGRAC_IC_MSG_GES_REQUEST, item->source_node_id, wire,
									   sizeof(wire));
	/* No reply or a full transport leaves the collector's query pending.
	 * Its bounded original-owner retry cannot turn loss into ABSENT. */
}
