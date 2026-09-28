/*-------------------------------------------------------------------------
 *
 * cluster_config_prefix.c
 *    Exact byte-prefix exchange owned by an original native stream.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/backend/cluster/cluster_config_prefix.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_config_prefix.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_sf_dep.h"
#include "miscadmin.h"

static bool
prefix_nonzero(const void *bytes, Size len)
{
	const uint8 *p = bytes;
	for (Size i = 0; i < len; ++i)
		if (p[i] != 0)
			return true;
	return false;
}
static bool
prefix_overlap(const void *a, Size an, const void *b, Size bn)
{
	uintptr_t x = (uintptr_t)a, y = (uintptr_t)b;
	return a != NULL && b != NULL && (x <= y ? y - x < an : x - y < bn);
}
static uint64
prefix_get(const uint8 *bytes, unsigned width)
{
	uint64 value = 0;
	for (unsigned i = 0; i < width; ++i)
		value |= (uint64)bytes[i] << (8 * i);
	return value;
}
static void
prefix_put(uint8 *bytes, uint64 value, unsigned width)
{
	for (unsigned i = 0; i < width; ++i)
		bytes[i] = (uint8)(value >> (8 * i));
}
static bool
prefix_member(const uint64 set[2], uint32 node)
{
	return node < CLUSTER_MAX_NODES && (set[node / 64] & (UINT64CONST(1) << (node % 64))) != 0;
}
static bool
prefix_message_valid(const ClusterConfigPrefixMessage *m)
{
	const ClusterSharedConfigIdentity *id = &m->key.ref.identity;
	return (m->verb == CLUSTER_CONFIG_PREFIX_MARK || m->verb == CLUSTER_CONFIG_PREFIX_ACK)
		   && m->sender != m->receiver && prefix_member(m->key.required, m->sender)
		   && prefix_member(m->key.required, m->receiver)
		   && (m->key.required[0] & ~id->configured[0]) == 0
		   && (m->key.required[1] & ~id->configured[1]) == 0 && id->system_identifier != 0
		   && id->database_incarnation != 0 && id->generation != 0 && m->key.epoch != 0
		   && m->sender_incarnation != 0 && m->receiver_incarnation != 0
		   && prefix_nonzero(id->storage_uuid, 16) && prefix_nonzero(id->authority_uuid, 16)
		   && prefix_nonzero(m->key.ref.sha256, 32) && prefix_nonzero(m->key.members_sha256, 32)
		   && prefix_nonzero(m->episode, 16) && prefix_nonzero(m->challenge, 16)
		   && ((m->plane == CLUSTER_IC_PLANE_CONTROL && m->channel == PG_UINT32_MAX)
			   || (m->plane == CLUSTER_IC_PLANE_DATA
				   && m->channel < CLUSTER_IC_TIER1_DATA_CHANNELS));
}

bool
cluster_config_prefix_encode(const ClusterConfigPrefixMessage *m,
							 uint8 bytes[CLUSTER_CONFIG_PREFIX_BYTES])
{
	const ClusterSharedConfigIdentity *id;
	if (prefix_overlap(m, sizeof(*m), bytes, CLUSTER_CONFIG_PREFIX_BYTES))
		return false;
	if (bytes != NULL)
		memset(bytes, 0, CLUSTER_CONFIG_PREFIX_BYTES);
	if (m == NULL || bytes == NULL || !prefix_message_valid(m))
		return false;
	id = &m->key.ref.identity;
	memcpy(bytes, "PCPX", 4);
	prefix_put(bytes + 4, 1, 2);
	prefix_put(bytes + 6, CLUSTER_CONFIG_PREFIX_BYTES, 2);
	prefix_put(bytes + 8, m->verb, 4);
	prefix_put(bytes + 12, m->sender, 4);
	prefix_put(bytes + 16, m->receiver, 4);
	prefix_put(bytes + 20, m->plane, 4);
	prefix_put(bytes + 24, m->channel, 4);
	prefix_put(bytes + 32, m->key.epoch, 8);
	prefix_put(bytes + 40, m->sender_incarnation, 8);
	prefix_put(bytes + 48, m->receiver_incarnation, 8);
	memcpy(bytes + 56, m->episode, 16);
	memcpy(bytes + 72, m->challenge, 16);
	prefix_put(bytes + 88, id->system_identifier, 8);
	prefix_put(bytes + 96, id->database_incarnation, 8);
	prefix_put(bytes + 104, id->generation, 8);
	memcpy(bytes + 112, id->storage_uuid, 16);
	memcpy(bytes + 128, id->authority_uuid, 16);
	prefix_put(bytes + 144, id->configured[0], 8);
	prefix_put(bytes + 152, id->configured[1], 8);
	memcpy(bytes + 160, m->key.ref.sha256, 32);
	prefix_put(bytes + 192, m->key.required[0], 8);
	prefix_put(bytes + 200, m->key.required[1], 8);
	memcpy(bytes + 208, m->key.members_sha256, 32);
	return true;
}

bool
cluster_config_prefix_decode(const void *bytes, Size length, ClusterConfigPrefixMessage *out)
{
	const uint8 *b = bytes;
	ClusterConfigPrefixMessage m = { 0 };
	ClusterSharedConfigIdentity *id = &m.key.ref.identity;
	if (prefix_overlap(bytes, length, out, sizeof(*out)))
		return false;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (b == NULL || out == NULL || length != CLUSTER_CONFIG_PREFIX_BYTES
		|| memcmp(b, "PCPX", 4) != 0 || prefix_get(b + 4, 2) != 1
		|| prefix_get(b + 6, 2) != CLUSTER_CONFIG_PREFIX_BYTES || prefix_nonzero(b + 28, 4)
		|| prefix_nonzero(b + 240, 16))
		return false;
	m.verb = prefix_get(b + 8, 4);
	m.sender = prefix_get(b + 12, 4);
	m.receiver = prefix_get(b + 16, 4);
	m.plane = prefix_get(b + 20, 4);
	m.channel = prefix_get(b + 24, 4);
	m.key.epoch = prefix_get(b + 32, 8);
	m.sender_incarnation = prefix_get(b + 40, 8);
	m.receiver_incarnation = prefix_get(b + 48, 8);
	memcpy(m.episode, b + 56, 16);
	memcpy(m.challenge, b + 72, 16);
	id->system_identifier = prefix_get(b + 88, 8);
	id->database_incarnation = prefix_get(b + 96, 8);
	id->generation = prefix_get(b + 104, 8);
	memcpy(id->storage_uuid, b + 112, 16);
	memcpy(id->authority_uuid, b + 128, 16);
	id->configured[0] = prefix_get(b + 144, 8);
	id->configured[1] = prefix_get(b + 152, 8);
	memcpy(m.key.ref.sha256, b + 160, 32);
	m.key.required[0] = prefix_get(b + 192, 8);
	m.key.required[1] = prefix_get(b + 200, 8);
	memcpy(m.key.members_sha256, b + 208, 32);
	if (!prefix_message_valid(&m))
		return false;
	*out = m;
	return true;
}

static bool
prefix_native_role(uint32 plane, uint32 channel)
{
	/* A live TCP stamp cannot certify a frame routed over RDMA. In particular,
	 * a mux's temporary TCP fallback says nothing about its earlier RDMA debt.
	 * Only the fixed native TCP profile has the captured FIFO guarantee. */
	if (!IsUnderPostmaster || !cluster_enabled || !cluster_shared_config
		|| ClusterICOps_Active != &ClusterICOps_Tier1
		|| cluster_interconnect_tier != CLUSTER_IC_TIER_1)
		return false;
	if (plane == CLUSTER_IC_PLANE_CONTROL)
		return MyBackendType == B_LMON && AmLmonProcess() && channel == PG_UINT32_MAX;
	return plane == CLUSTER_IC_PLANE_DATA
		   && ((MyBackendType == B_LMS && AmLmsProcess() && channel == 0)
			   || (MyBackendType == B_LMS_WORKER && AmLmsWorkerProcess()
				   && channel == (uint32)ClusterLmsWorkerIdForType(MyAuxProcType)));
}

static bool
prefix_current(ClusterConfigPrefixExchange *e)
{
	if (e == NULL || !e->active || e->failed)
		return false;
	if (e->mark.sender != (uint32)cluster_node_id || e->stream.owner_pid != MyProcPid
		|| !prefix_native_role(e->mark.plane, e->mark.channel)
		|| !cluster_ic_tier1_stream_current(&e->stream)
		|| !cluster_sf_peer_capability_generation_matches(
			e->mark.receiver, PGRAC_IC_HELLO_CAP_CONFIG_PREFIX_V1, e->capability_generation)) {
		e->failed = true;
		return false;
	}
	return true;
}

bool
cluster_config_prefix_begin(ClusterConfigPrefixExchange *e, const ClusterConfigMembersKey *key,
							const uint8 episode[16], int32 peer, uint64 local_incarnation,
							uint64 peer_incarnation)
{
	ClusterConfigPrefixExchange next = { 0 };
	uint32 caps;
	if (e == NULL || key == NULL || episode == NULL || e->active
		|| prefix_overlap(e, sizeof(*e), key, sizeof(*key))
		|| prefix_overlap(e, sizeof(*e), episode, 16) || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_MAX_NODES || peer < 0 || peer >= CLUSTER_MAX_NODES
		|| peer == cluster_node_id)
		return false;
	if (!cluster_ic_tier1_stream_capture(peer, &next.stream))
		return false;
	next.mark.key = *key;
	memcpy(next.mark.episode, episode, 16);
	next.mark.sender = cluster_node_id;
	next.mark.receiver = peer;
	next.mark.sender_incarnation = local_incarnation;
	next.mark.receiver_incarnation = peer_incarnation;
	next.mark.plane = next.stream.plane;
	next.mark.channel = (uint32)next.stream.channel;
	next.mark.verb = CLUSTER_CONFIG_PREFIX_MARK;
	if (next.stream.epoch != key->epoch || next.stream.owner_pid != MyProcPid
		|| !prefix_native_role(next.mark.plane, next.mark.channel)
		|| !cluster_sf_peer_capability_word_sample(peer, PGRAC_IC_HELLO_CAP_CONFIG_PREFIX_V1, &caps,
												   &next.capability_generation)
		|| !pg_strong_random(next.mark.challenge, sizeof(next.mark.challenge))
		|| !prefix_message_valid(&next.mark))
		return false;
	next.active = true;
	if (!prefix_current(&next))
		return false;
	*e = next;
	return true;
}

static bool
prefix_send(ClusterConfigPrefixExchange *e, bool ack)
{
	ClusterConfigPrefixMessage m = e->mark;
	uint8 bytes[CLUSTER_CONFIG_PREFIX_BYTES];
	ClusterICSendResult result;
	if (ack) {
		m.verb = CLUSTER_CONFIG_PREFIX_ACK;
		memcpy(m.challenge, e->peer_challenge, 16);
	}
	if (!cluster_config_prefix_encode(&m, bytes)) {
		e->failed = true;
		return false;
	}
	result = cluster_ic_send_envelope(m.plane == CLUSTER_IC_PLANE_CONTROL
										  ? PGRAC_IC_MSG_CONFIG_PREFIX_CONTROL
										  : PGRAC_IC_MSG_CONFIG_PREFIX_DATA,
									  m.receiver, bytes, sizeof(bytes));
	if (!prefix_current(e))
		return false;
	if (result == CLUSTER_IC_SEND_DONE || result == CLUSTER_IC_SEND_WOULD_BLOCK)
		return true;
	if (result != CLUSTER_IC_SEND_NOT_ADMITTED)
		e->failed = true;
	return false;
}

void
cluster_config_prefix_poll(ClusterConfigPrefixExchange *e)
{
	if (!prefix_current(e))
		return;
	PG_TRY();
	{
		if (!e->mark_admitted)
			e->mark_admitted = prefix_send(e, false);
		if (!e->failed && e->peer_mark_seen && !e->peer_ack_admitted)
			e->peer_ack_admitted = prefix_send(e, true);
	}
	PG_CATCH();
	{
		e->failed = true;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

bool
cluster_config_prefix_ingress(ClusterConfigPrefixExchange *e, const ClusterICEnvelope *env,
							  const void *payload)
{
	ClusterConfigPrefixMessage m;
	if (!prefix_current(e) || env == NULL
		|| !cluster_config_prefix_decode(payload, env->payload_length, &m)
		|| env->msg_type
			   != (m.plane == CLUSTER_IC_PLANE_CONTROL ? PGRAC_IC_MSG_CONFIG_PREFIX_CONTROL
													   : PGRAC_IC_MSG_CONFIG_PREFIX_DATA)
		|| env->source_node_id != m.sender || env->dest_node_id != m.receiver
		|| env->epoch != m.key.epoch || m.sender != e->mark.receiver || m.receiver != e->mark.sender
		|| m.sender_incarnation != e->mark.receiver_incarnation
		|| m.receiver_incarnation != e->mark.sender_incarnation || m.plane != e->mark.plane
		|| m.channel != e->mark.channel || memcmp(&m.key, &e->mark.key, sizeof(m.key)) != 0
		|| memcmp(m.episode, e->mark.episode, 16) != 0)
		return false;
	if (m.verb == CLUSTER_CONFIG_PREFIX_MARK) {
		if (e->peer_mark_seen && memcmp(m.challenge, e->peer_challenge, 16) != 0) {
			e->failed = true;
			return false;
		}
		memcpy(e->peer_challenge, m.challenge, 16);
		e->peer_mark_seen = true;
	} else {
		if (!e->mark_admitted || memcmp(m.challenge, e->mark.challenge, 16) != 0)
			return false;
		e->mark_acknowledged = true;
	}
	return prefix_current(e);
}

bool
cluster_config_prefix_complete(ClusterConfigPrefixExchange *e)
{
	return prefix_current(e) && e->mark_admitted && e->mark_acknowledged && e->peer_mark_seen
		   && e->peer_ack_admitted;
}

void
cluster_config_prefix_invalidate(ClusterConfigPrefixExchange *e)
{
	if (e != NULL && e->active)
		e->failed = true;
}

void
cluster_config_prefix_clear(ClusterConfigPrefixExchange *e)
{
	if (e != NULL)
		memset(e, 0, sizeof(*e));
}
