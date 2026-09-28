/*-------------------------------------------------------------------------
 *
 * cluster_config_members.c
 *    LMON-owned observations of actual configuration on required members.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/backend/cluster/cluster_config_members.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_config_members.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_sf_dep.h"
#include "miscadmin.h"
#include "../../common/sha2_int.h"

/* One LMON lifetime, not durable authority, remote work or a permission cache. */
static struct {
	bool active;
	uint64 incarnation[CLUSTER_MAX_NODES];
	uint32 connection[CLUSTER_MAX_NODES];
	ClusterConfigMembersObservation report;
} member_round;

static bool
nonzero(const void *p, Size n)
{
	const uint8 *b = p;
	for (Size i = 0; i < n; i++)
		if (b[i] != 0)
			return true;
	return false;
}

static bool
overlaps(const void *a, Size an, const void *b, Size bn)
{
	uintptr_t x = (uintptr_t)a, y = (uintptr_t)b;
	return a != NULL && b != NULL && (x <= y ? y - x < an : x - y < bn);
}

static uint64
get_le(const uint8 *b, unsigned width)
{
	uint64 v = 0;
	for (unsigned i = 0; i < width; i++)
		v |= (uint64)b[i] << (8 * i);
	return v;
}

static void
put_le(uint8 *b, uint64 v, unsigned width)
{
	for (unsigned i = 0; i < width; i++)
		b[i] = (uint8)(v >> (8 * i));
}

static bool
in_set(const uint64 set[2], unsigned node)
{
	return node < CLUSTER_MAX_NODES && (set[node / 64] & (UINT64_C(1) << (node % 64))) != 0;
}

static bool
key_valid(const ClusterConfigMembersKey *k)
{
	return k->ref.identity.system_identifier != 0 && k->ref.identity.database_incarnation != 0
		   && k->ref.identity.generation != 0 && k->epoch != 0
		   && (k->required[0] != 0 || k->required[1] != 0)
		   && (k->required[0] & ~k->ref.identity.configured[0]) == 0
		   && (k->required[1] & ~k->ref.identity.configured[1]) == 0
		   && nonzero(k->ref.identity.storage_uuid, 16)
		   && nonzero(k->ref.identity.authority_uuid, 16) && nonzero(k->ref.sha256, 32)
		   && nonzero(k->members_sha256, 32);
}

static bool
census_valid(const ClusterSharedConfigCensus *c)
{
	uint64 exclusive = (uint64)c->current_processes + c->waiting_processes + c->failed_processes
					   + c->parallel_processes;
	const ClusterSharedConfigActive *a = &c->active;
	if (c->participants == 0 || exclusive != c->participants
		|| c->pending_processes > c->current_processes
		|| c->deferred_processes > c->current_processes
		|| c->active_missing_processes > c->current_processes
		|| c->static_mismatch_processes > c->current_processes - c->active_missing_processes
		|| c->dynamic_mismatch_processes > c->current_processes - c->active_missing_processes
		|| c->pending_entries < c->pending_processes || c->deferred_entries < c->deferred_processes
		|| c->pending_entries > (uint64)c->pending_processes * CLUSTER_SHARED_CONFIG_MAX_ENTRIES
		|| c->deferred_entries > (uint64)c->deferred_processes * CLUSTER_SHARED_CONFIG_MAX_ENTRIES)
		return false;
	if (a->version == 0)
		return !nonzero(a, sizeof(*a)) && c->static_mismatch_processes == 0
			   && c->dynamic_mismatch_processes == 0;
	return a->version == CLUSTER_SHARED_CONFIG_ACTIVE_VERSION && a->static_entries != 0
		   && a->dynamic_entries != 0
		   && (uint64)a->static_entries + a->dynamic_entries <= CLUSTER_SHARED_CONFIG_MAX_ENTRIES
		   && nonzero(a->static_sha256, 32) && nonzero(a->dynamic_sha256, 32);
}

static bool
message_valid(const ClusterConfigMembersMessage *m)
{
	if (!key_valid(&m->key) || !in_set(m->key.required, m->collector)
		|| !in_set(m->key.required, m->responder) || m->nonce == 0 || m->collector_incarnation == 0
		|| m->responder_incarnation == 0)
		return false;
	if (m->verb == CLUSTER_CONFIG_MEMBERS_REQUEST)
		return m->outcome == 0 && !nonzero(&m->census, sizeof(m->census));
	if (m->verb != CLUSTER_CONFIG_MEMBERS_REPLY)
		return false;
	if (m->outcome == CLUSTER_CONFIG_MEMBERS_UNAVAILABLE)
		return !nonzero(&m->census, sizeof(m->census));
	return m->outcome == CLUSTER_CONFIG_MEMBERS_OBSERVED && m->census.node_id == m->responder
		   && memcmp(&m->census.ref, &m->key.ref, sizeof(m->key.ref)) == 0
		   && census_valid(&m->census);
}

bool
cluster_config_members_encode(const ClusterConfigMembersMessage *message,
							  uint8 bytes[CLUSTER_CONFIG_MEMBERS_BYTES])
{
	const ClusterSharedConfigIdentity *id;
	const ClusterSharedConfigCensus *c;
	uint32 counts[10];
	if (overlaps(message, sizeof(*message), bytes, CLUSTER_CONFIG_MEMBERS_BYTES))
		return false;
	if (bytes != NULL)
		memset(bytes, 0, CLUSTER_CONFIG_MEMBERS_BYTES);
	if (message == NULL || bytes == NULL || !message_valid(message))
		return false;
	id = &message->key.ref.identity;
	memcpy(bytes, "PCSO", 4);
	put_le(bytes + 4, 1, 2);
	put_le(bytes + 6, CLUSTER_CONFIG_MEMBERS_BYTES, 2);
	put_le(bytes + 8, message->verb, 4);
	put_le(bytes + 12, message->collector, 4);
	put_le(bytes + 16, message->responder, 4);
	put_le(bytes + 20, message->outcome, 4);
	put_le(bytes + 24, message->key.epoch, 8);
	put_le(bytes + 32, message->collector_incarnation, 8);
	put_le(bytes + 40, message->responder_incarnation, 8);
	put_le(bytes + 48, message->nonce, 8);
	put_le(bytes + 56, id->system_identifier, 8);
	put_le(bytes + 64, id->database_incarnation, 8);
	put_le(bytes + 72, id->generation, 8);
	memcpy(bytes + 80, id->storage_uuid, 16);
	memcpy(bytes + 96, id->authority_uuid, 16);
	put_le(bytes + 112, id->configured[0], 8);
	put_le(bytes + 120, id->configured[1], 8);
	memcpy(bytes + 128, message->key.ref.sha256, 32);
	put_le(bytes + 160, message->key.required[0], 8);
	put_le(bytes + 168, message->key.required[1], 8);
	memcpy(bytes + 176, message->key.members_sha256, 32);
	c = &message->census;
	counts[0] = c->participants;
	counts[1] = c->current_processes;
	counts[2] = c->waiting_processes;
	counts[3] = c->failed_processes;
	counts[4] = c->parallel_processes;
	counts[5] = c->pending_processes;
	counts[6] = c->deferred_processes;
	counts[7] = c->active_missing_processes;
	counts[8] = c->static_mismatch_processes;
	counts[9] = c->dynamic_mismatch_processes;
	for (unsigned i = 0; i < 10; ++i)
		put_le(bytes + 208 + i * 4, counts[i], 4);
	put_le(bytes + 248, c->pending_entries, 8);
	put_le(bytes + 256, c->deferred_entries, 8);
	put_le(bytes + 264, c->active.version, 4);
	put_le(bytes + 268, c->active.static_entries, 4);
	put_le(bytes + 272, c->active.dynamic_entries, 4);
	memcpy(bytes + 276, c->active.static_sha256, 32);
	memcpy(bytes + 308, c->active.dynamic_sha256, 32);
	return true;
}

bool
cluster_config_members_decode(const void *bytes, Size length, ClusterConfigMembersMessage *out)
{
	const uint8 *b = bytes;
	ClusterConfigMembersMessage m = { 0 };
	ClusterSharedConfigIdentity *id = &m.key.ref.identity;
	ClusterSharedConfigCensus *c = &m.census;
	if (overlaps(bytes, length, out, sizeof(*out)))
		return false;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (bytes == NULL || out == NULL || length != CLUSTER_CONFIG_MEMBERS_BYTES
		|| memcmp(b, "PCSO", 4) != 0 || get_le(b + 4, 2) != 1
		|| get_le(b + 6, 2) != CLUSTER_CONFIG_MEMBERS_BYTES || nonzero(b + 340, 12))
		return false;
	m.verb = get_le(b + 8, 4);
	m.collector = get_le(b + 12, 4);
	m.responder = get_le(b + 16, 4);
	m.outcome = get_le(b + 20, 4);
	m.key.epoch = get_le(b + 24, 8);
	m.collector_incarnation = get_le(b + 32, 8);
	m.responder_incarnation = get_le(b + 40, 8);
	m.nonce = get_le(b + 48, 8);
	id->system_identifier = get_le(b + 56, 8);
	id->database_incarnation = get_le(b + 64, 8);
	id->generation = get_le(b + 72, 8);
	memcpy(id->storage_uuid, b + 80, 16);
	memcpy(id->authority_uuid, b + 96, 16);
	id->configured[0] = get_le(b + 112, 8);
	id->configured[1] = get_le(b + 120, 8);
	memcpy(m.key.ref.sha256, b + 128, 32);
	m.key.required[0] = get_le(b + 160, 8);
	m.key.required[1] = get_le(b + 168, 8);
	memcpy(m.key.members_sha256, b + 176, 32);
	if (m.verb == CLUSTER_CONFIG_MEMBERS_REPLY && m.outcome == CLUSTER_CONFIG_MEMBERS_OBSERVED) {
		c->ref = m.key.ref;
		c->node_id = m.responder;
		c->participants = get_le(b + 208, 4);
		c->current_processes = get_le(b + 212, 4);
		c->waiting_processes = get_le(b + 216, 4);
		c->failed_processes = get_le(b + 220, 4);
		c->parallel_processes = get_le(b + 224, 4);
		c->pending_processes = get_le(b + 228, 4);
		c->deferred_processes = get_le(b + 232, 4);
		c->active_missing_processes = get_le(b + 236, 4);
		c->static_mismatch_processes = get_le(b + 240, 4);
		c->dynamic_mismatch_processes = get_le(b + 244, 4);
		c->pending_entries = get_le(b + 248, 8);
		c->deferred_entries = get_le(b + 256, 8);
		c->active.version = get_le(b + 264, 4);
		c->active.static_entries = get_le(b + 268, 4);
		c->active.dynamic_entries = get_le(b + 272, 4);
		memcpy(c->active.static_sha256, b + 276, 32);
		memcpy(c->active.dynamic_sha256, b + 308, 32);
	} else if (nonzero(b + 208, 132))
		return false;
	if (!message_valid(&m))
		return false;
	*out = m;
	return true;
}

static bool
make_key(const ClusterSharedConfigRef *ref, const ClusterR4MembershipSnapshot *members,
		 ClusterConfigMembersKey *key)
{
	static const uint8 domain[] = "PGRAC config admitted incarnations v1";
	pg_sha256_ctx hash;
	uint8 encoded[8];
	memset(key, 0, sizeof(*key));
	if (ref == NULL || members == NULL || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_MAX_NODES)
		return false;
	key->ref = *ref;
	key->epoch = members->formation_epoch;
	key->required[0] = members->admitted_members_lo;
	key->required[1] = members->admitted_members_hi;
	if (!in_set(key->required, cluster_node_id) || members->local_self_boot_incarnation == 0
		|| members->local_self_boot_incarnation != members->admitted_incarnation[cluster_node_id])
		return false;
	pg_sha256_init(&hash);
	pg_sha256_update(&hash, domain, sizeof(domain));
	for (unsigned node = 0; node < CLUSTER_MAX_NODES; ++node) {
		uint64 inc = in_set(key->required, node) ? members->admitted_incarnation[node] : 0;
		if (in_set(key->required, node) && inc == 0)
			return false;
		put_le(encoded, inc, 8);
		pg_sha256_update(&hash, encoded, sizeof(encoded));
	}
	pg_sha256_final(&hash, key->members_sha256);
	return key_valid(key);
}

static bool
local_role(void)
{
	return MyBackendType == B_LMON && cluster_enabled && cluster_shared_config
		   && (cluster_ic_local_capability_word() & PGRAC_IC_HELLO_CAP_CONFIG_MEMBERS_V1) != 0;
}

static bool
round_current(void)
{
	ClusterR4MembershipSnapshot members;
	ClusterConfigMembersKey key;
	if (!local_role() || !member_round.active
		|| !cluster_reconfig_lmon_snapshot_r4_membership(&members)
		|| !make_key(&member_round.report.key.ref, &members, &key)
		|| memcmp(&key, &member_round.report.key, sizeof(key)) != 0)
		return false;
	for (unsigned node = 0; node < CLUSTER_MAX_NODES; ++node)
		if (in_set(key.required, node) && node != (uint32)cluster_node_id
			&& !cluster_sf_peer_capability_generation_matches(
				node, PGRAC_IC_HELLO_CAP_CONFIG_MEMBERS_V1, member_round.connection[node]))
			return false;
	return true;
}

static bool
complete(void)
{
	for (unsigned i = 0; i < 2; ++i)
		if ((member_round.report.observed[i] | member_round.report.unavailable[i])
			!= member_round.report.key.required[i])
			return false;
	return true;
}

static ClusterConfigMembersMessage
request_for(unsigned node)
{
	ClusterConfigMembersMessage m = { 0 };
	m.key = member_round.report.key;
	m.nonce = member_round.report.nonce;
	m.collector = cluster_node_id;
	m.responder = node;
	m.collector_incarnation = member_round.incarnation[cluster_node_id];
	m.responder_incarnation = member_round.incarnation[node];
	m.verb = CLUSTER_CONFIG_MEMBERS_REQUEST;
	return m;
}

static bool
observe_local(ClusterConfigMembersMessage *m)
{
	if (!round_current() || memcmp(&m->key, &member_round.report.key, sizeof(m->key)) != 0
		|| m->responder != (uint32)cluster_node_id
		|| m->responder_incarnation != member_round.incarnation[cluster_node_id]
		|| m->collector_incarnation != member_round.incarnation[m->collector])
		return false;
	m->verb = CLUSTER_CONFIG_MEMBERS_REPLY;
	if (cluster_shared_config_node_census(&m->key.ref, cluster_node_id, &m->census))
		m->outcome = CLUSTER_CONFIG_MEMBERS_OBSERVED;
	else {
		memset(&m->census, 0, sizeof(m->census));
		m->outcome = CLUSTER_CONFIG_MEMBERS_UNAVAILABLE;
	}
	return message_valid(m) && round_current();
}

static void
accept_observation(const ClusterConfigMembersMessage *m)
{
	unsigned word = m->responder / 64;
	uint64 bit = UINT64_C(1) << (m->responder % 64);
	if ((member_round.report.observed[word] | member_round.report.unavailable[word]) & bit)
		return;
	if (m->outcome == CLUSTER_CONFIG_MEMBERS_OBSERVED) {
		member_round.report.node[m->responder] = m->census;
		member_round.report.observed[word] |= bit;
	} else
		member_round.report.unavailable[word] |= bit;
}

void
cluster_config_members_poll(const ClusterSharedConfigRef *selected,
							const ClusterR4MembershipSnapshot *members)
{
	ClusterConfigMembersKey key, current;
	ClusterR4MembershipSnapshot fresh;
	uint32 connection[CLUSTER_MAX_NODES] = { 0 };
	if (!local_role())
		return;
	if (!make_key(selected, members, &key) || !cluster_reconfig_lmon_snapshot_r4_membership(&fresh)
		|| !make_key(selected, &fresh, &current) || memcmp(&key, &current, sizeof(key)) != 0)
		goto unavailable;
	for (unsigned node = 0; node < CLUSTER_MAX_NODES; ++node) {
		uint32 word;
		if (in_set(key.required, node) && node != (uint32)cluster_node_id
			&& !cluster_sf_peer_capability_word_sample(node, PGRAC_IC_HELLO_CAP_CONFIG_MEMBERS_V1,
													   &word, &connection[node]))
			goto unavailable;
	}
	if (!member_round.active || !round_current() || complete()
		|| memcmp(&key, &member_round.report.key, sizeof(key)) != 0) {
		uint64 nonce;
		cluster_config_members_cancel();
		if (!pg_strong_random(&nonce, sizeof(nonce)) || nonce == 0)
			return;
		member_round.report.key = key;
		member_round.report.nonce = nonce;
		memcpy(member_round.incarnation, fresh.admitted_incarnation,
			   sizeof(member_round.incarnation));
		memcpy(member_round.connection, connection, sizeof(connection));
		member_round.active = true;
	}
	/* Native capture precedes remote sends. A membership change in capture
	 * must not send a seemingly current query or publish even the local result. */
	if (!in_set(member_round.report.observed, cluster_node_id)
		&& !in_set(member_round.report.unavailable, cluster_node_id)) {
		ClusterConfigMembersMessage m = request_for(cluster_node_id);
		if (!observe_local(&m))
			goto unavailable;
		accept_observation(&m);
	}
	for (unsigned node = 0; node < CLUSTER_MAX_NODES; ++node) {
		ClusterConfigMembersMessage m;
		uint8 bytes[CLUSTER_CONFIG_MEMBERS_BYTES];
		if (!in_set(key.required, node) || node == (uint32)cluster_node_id
			|| in_set(member_round.report.observed, node)
			|| in_set(member_round.report.unavailable, node))
			continue;
		m = request_for(node);
		if (cluster_config_members_encode(&m, bytes))
			(void)cluster_ic_send_envelope(PGRAC_IC_MSG_CONFIG_MEMBERS, node, bytes, sizeof(bytes));
		/* Queue refusal retains this exact missing member for the next tick. */
	}
	if (round_current())
		return;
unavailable:
	cluster_config_members_cancel();
}

void
cluster_config_members_cancel(void)
{
	if (MyBackendType == B_LMON)
		memset(&member_round, 0, sizeof(member_round));
}

bool
cluster_config_members_observe(ClusterConfigMembersObservation *out)
{
	if (out == NULL || overlaps(out, sizeof(*out), &member_round, sizeof(member_round)))
		return false;
	memset(out, 0, sizeof(*out));
	if (!round_current())
		return false;
	*out = member_round.report;
	if (round_current())
		return true;
	memset(out, 0, sizeof(*out));
	return false;
}

static void
config_members_ingress_impl(const ClusterICEnvelope *env, const void *payload)
{
	ClusterConfigMembersMessage m;
	uint8 bytes[CLUSTER_CONFIG_MEMBERS_BYTES];
	if (!local_role() || env == NULL || env->msg_type != PGRAC_IC_MSG_CONFIG_MEMBERS
		|| env->source_node_id >= CLUSTER_MAX_NODES || env->dest_node_id != (uint32)cluster_node_id
		|| !cluster_config_members_decode(payload, env->payload_length, &m)
		|| env->epoch != m.key.epoch || !round_current()
		|| memcmp(&m.key, &member_round.report.key, sizeof(m.key)) != 0
		|| m.collector_incarnation != member_round.incarnation[m.collector]
		|| m.responder_incarnation != member_round.incarnation[m.responder])
		return;
	if (m.verb == CLUSTER_CONFIG_MEMBERS_REQUEST) {
		if (env->source_node_id != m.collector || m.responder != (uint32)cluster_node_id
			|| !observe_local(&m) || !cluster_config_members_encode(&m, bytes))
			return;
		(void)cluster_ic_send_envelope(PGRAC_IC_MSG_CONFIG_MEMBERS, m.collector, bytes,
									   sizeof(bytes));
	} else if (env->source_node_id == m.responder && m.collector == (uint32)cluster_node_id
			   && m.nonce == member_round.report.nonce)
		accept_observation(&m);
}

void
cluster_config_members_ingress(const ClusterICEnvelope *env, const void *payload)
{
	PG_TRY();
	{
		config_members_ingress_impl(env, payload);
	}
	PG_CATCH();
	{
		/* The router owns dispatch memory and catches ERROR. Retire this
		 * partial collection before it can accept a later reply. */
		cluster_config_members_cancel();
		PG_RE_THROW();
	}
	PG_END_TRY();
}

void
cluster_config_members_register(void)
{
	static const ClusterICMsgTypeInfo info = { .msg_type = PGRAC_IC_MSG_CONFIG_MEMBERS,
											   .name = "config_member_observation",
											   .allowed_producer_mask = CLUSTER_IC_PRODUCER_LMON,
											   .broadcast_ok = false,
											   .handler = cluster_config_members_ingress,
											   .plane = CLUSTER_IC_PLANE_CONTROL };
	cluster_ic_register_msg_type(&info);
}
