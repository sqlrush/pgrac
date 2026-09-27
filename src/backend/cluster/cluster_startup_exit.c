/*-------------------------------------------------------------------------
 *
 * cluster_startup_exit.c
 *    LMON-owned startup evidence, never a second publication authority.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/backend/cluster/cluster_startup_exit.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_startup_exit.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_qvotec.h"
#include "common/cryptohash.h"
#include "miscadmin.h"
#include "utils/timestamp.h"

#define EXIT_RETRY_US INT64CONST(100000)

/* One coordinator-owned observation, not durable work or shared authority. */
static struct {
	bool active;
	bool poisoned;
	bool attempted;
	ClusterStartupExitCut cut;
	uint64 nonce;
	TimestampTz last_send;
	bool seen[CLUSTER_MAX_NODES];
	uint8 evidence[CLUSTER_MAX_NODES][32];
} exit_round;

static bool
nonzero(const void *data, Size length)
{
	const uint8 *bytes = data;
	for (Size i = 0; i < length; ++i)
		if (bytes[i] != 0)
			return true;
	return false;
}

static bool
overlaps(const void *a, Size alen, const void *b, Size blen)
{
	uintptr_t x = (uintptr_t)a, y = (uintptr_t)b;
	return a != NULL && b != NULL && (x <= y ? y - x < alen : x - y < blen);
}

static uint64
get_le(const uint8 *p, unsigned width)
{
	uint64 value = 0;
	for (unsigned i = 0; i < width; ++i)
		value |= (uint64)p[i] << (8 * i);
	return value;
}

static void
put_le(uint8 *p, uint64 value, unsigned width)
{
	for (unsigned i = 0; i < width; ++i)
		p[i] = (uint8)(value >> (8 * i));
}

static bool
key_valid(const ClusterStartupExitKey *key)
{
	return key->system_identifier != 0 && key->database_incarnation != 0
		   && key->config_generation != 0 && key->epoch != 0 && key->root_sequence != 0
		   && key->coordinator < CLUSTER_MAX_NODES && key->coordinator_incarnation != 0
		   && key->reserved == 0 && nonzero(key->root_sha256, 32) && nonzero(key->storage_uuid, 16)
		   && nonzero(key->authority_uuid, 16);
}

static bool
message_valid(const ClusterStartupExitMessage *m)
{
	return key_valid(&m->key) && m->node < CLUSTER_MAX_NODES && m->old_incarnation != 0
		   && m->observing_incarnation > m->old_incarnation && m->nonce != 0
		   && ((m->verb == CLUSTER_STARTUP_EXIT_REQUEST && !nonzero(m->evidence_sha256, 32))
			   || (m->verb == CLUSTER_STARTUP_EXIT_REPLY && nonzero(m->evidence_sha256, 32)));
}

bool
cluster_startup_exit_encode(const ClusterStartupExitMessage *message,
							uint8 bytes[CLUSTER_STARTUP_EXIT_BYTES])
{
	bool alias = overlaps(message, sizeof(*message), bytes, CLUSTER_STARTUP_EXIT_BYTES);
	const ClusterStartupExitKey *key;

	if (bytes != NULL)
		memset(bytes, 0, CLUSTER_STARTUP_EXIT_BYTES);
	if (alias || message == NULL || bytes == NULL || !message_valid(message))
		return false;
	key = &message->key;
	memcpy(bytes, "PWEX", 4);
	put_le(bytes + 4, 1, 2);
	put_le(bytes + 6, CLUSTER_STARTUP_EXIT_BYTES, 2);
	put_le(bytes + 8, message->verb, 4);
	put_le(bytes + 12, message->node, 4);
	put_le(bytes + 16, message->old_incarnation, 8);
	put_le(bytes + 24, message->observing_incarnation, 8);
	put_le(bytes + 32, key->coordinator, 4);
	put_le(bytes + 40, key->coordinator_incarnation, 8);
	put_le(bytes + 48, key->epoch, 8);
	put_le(bytes + 56, key->root_sequence, 8);
	memcpy(bytes + 64, key->root_sha256, 32);
	put_le(bytes + 96, key->system_identifier, 8);
	put_le(bytes + 104, key->database_incarnation, 8);
	put_le(bytes + 112, key->config_generation, 8);
	memcpy(bytes + 120, key->storage_uuid, 16);
	memcpy(bytes + 136, key->authority_uuid, 16);
	put_le(bytes + 152, message->nonce, 8);
	memcpy(bytes + 160, message->evidence_sha256, 32);
	return true;
}

bool
cluster_startup_exit_decode(const void *bytes, Size length, ClusterStartupExitMessage *out)
{
	const uint8 *b = bytes;
	ClusterStartupExitMessage decoded = { 0 };
	bool alias = overlaps(bytes, length, out, sizeof(*out));

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (alias || bytes == NULL || out == NULL || length != CLUSTER_STARTUP_EXIT_BYTES
		|| memcmp(b, "PWEX", 4) != 0 || get_le(b + 4, 2) != 1
		|| get_le(b + 6, 2) != CLUSTER_STARTUP_EXIT_BYTES || nonzero(b + 36, 4)
		|| nonzero(b + 192, 16))
		return false;
	decoded.verb = get_le(b + 8, 4);
	decoded.node = get_le(b + 12, 4);
	decoded.old_incarnation = get_le(b + 16, 8);
	decoded.observing_incarnation = get_le(b + 24, 8);
	decoded.key.coordinator = get_le(b + 32, 4);
	decoded.key.coordinator_incarnation = get_le(b + 40, 8);
	decoded.key.epoch = get_le(b + 48, 8);
	decoded.key.root_sequence = get_le(b + 56, 8);
	memcpy(decoded.key.root_sha256, b + 64, 32);
	decoded.key.system_identifier = get_le(b + 96, 8);
	decoded.key.database_incarnation = get_le(b + 104, 8);
	decoded.key.config_generation = get_le(b + 112, 8);
	memcpy(decoded.key.storage_uuid, b + 120, 16);
	memcpy(decoded.key.authority_uuid, b + 136, 16);
	decoded.nonce = get_le(b + 152, 8);
	memcpy(decoded.evidence_sha256, b + 160, 32);
	if (!message_valid(&decoded))
		return false;
	*out = decoded;
	return true;
}

static bool
member_current(uint32 node, uint64 incarnation)
{
	return node < CLUSTER_MAX_NODES && incarnation != 0
		   && cluster_conf_lookup_node((int32)node) != NULL
		   && cluster_membership_is_member((int32)node)
		   && cluster_membership_get_last_admitted_incarnation((int32)node) == incarnation;
}

static bool
local_current(const ClusterStartupExitKey *key)
{
	return cluster_enabled && cluster_shared_config && MyBackendType == B_LMON
		   && cluster_node_id >= 0 && cluster_node_id < CLUSTER_MAX_NODES && key_valid(key)
		   && cluster_qvotec_in_quorum() && cluster_epoch_get_current() == key->epoch
		   && member_current(key->coordinator, key->coordinator_incarnation)
		   && member_current((uint32)cluster_node_id, cluster_qvotec_get_self_incarnation());
}

static bool
required(const ClusterStartupExitCut *cut, unsigned node)
{
	return (cut->required[node / 64] & (UINT64_C(1) << (node % 64))) != 0;
}

static bool
cut_current(const ClusterStartupExitCut *cut)
{
	if (!local_current(&cut->key) || cut->key.coordinator != (uint32)cluster_node_id
		|| cut->key.coordinator_incarnation != cluster_qvotec_get_self_incarnation()
		|| (cut->required[0] == 0 && cut->required[1] == 0))
		return false;
	for (unsigned node = 0; node < CLUSTER_MAX_NODES; ++node) {
		if (!required(cut, node)) {
			if (cut->predecessor[node] != 0 || cut->observer[node] != 0)
				return false;
			continue;
		}
		if (cut->predecessor[node] == 0 || cut->observer[node] <= cut->predecessor[node]
			|| !member_current(node, cut->observer[node]))
			return false;
	}
	return cluster_epoch_get_current() == cut->key.epoch && cluster_qvotec_in_quorum();
}

static ClusterStartupExitMessage
request_for(unsigned node)
{
	ClusterStartupExitMessage m = { 0 };
	m.key = exit_round.cut.key;
	m.node = node;
	m.old_incarnation = exit_round.cut.predecessor[node];
	m.observing_incarnation = exit_round.cut.observer[node];
	m.nonce = exit_round.nonce;
	m.verb = CLUSTER_STARTUP_EXIT_REQUEST;
	return m;
}

/* Hash the validated captured bytes, not a caller-provided boolean or digest.
 * QVOTEC owns their CRC/identity/disk-completeness validation. The receiver
 * treats this as an authenticated observation, not a portable signature. */
static bool
observe_local(ClusterStartupExitMessage *m)
{
	ClusterQvotecPriorExitObservation observation;
	pg_cryptohash_ctx *hash;
	uint8 wire[CLUSTER_STARTUP_EXIT_BYTES];
	uint8 digest[32];
	bool ok;

	if (!local_current(&m->key) || m->node != (uint32)cluster_node_id
		|| m->observing_incarnation != cluster_qvotec_get_self_incarnation()
		|| !cluster_startup_exit_encode(m, wire)
		|| !cluster_qvotec_prior_exit_observe(m->node, m->old_incarnation, m->observing_incarnation,
											  &observation))
		return false;
	hash = pg_cryptohash_create(PG_SHA256);
	if (hash == NULL)
		return false;
	ok = pg_cryptohash_init(hash) == 0 && pg_cryptohash_update(hash, wire, sizeof(wire)) == 0
		 && pg_cryptohash_update(hash, (const uint8 *)&observation, sizeof(observation)) == 0
		 && pg_cryptohash_final(hash, digest, sizeof(digest)) == 0;
	pg_cryptohash_free(hash);
	if (!ok || !local_current(&m->key)
		|| m->observing_incarnation != cluster_qvotec_get_self_incarnation())
		return false;
	m->verb = CLUSTER_STARTUP_EXIT_REPLY;
	memcpy(m->evidence_sha256, digest, sizeof(digest));
	return true;
}

static bool
round_digest(uint8 digest[32])
{
	static const uint8 domain[] = "PGRAC startup prior-exit set v1";
	uint8 bitmap[16], wire[CLUSTER_STARTUP_EXIT_BYTES];
	pg_cryptohash_ctx *hash = pg_cryptohash_create(PG_SHA256);
	bool ok;

	if (hash == NULL)
		return false;
	put_le(bitmap, exit_round.cut.required[0], 8);
	put_le(bitmap + 8, exit_round.cut.required[1], 8);
	ok = pg_cryptohash_init(hash) == 0 && pg_cryptohash_update(hash, domain, sizeof(domain)) == 0
		 && pg_cryptohash_update(hash, bitmap, sizeof(bitmap)) == 0;
	for (unsigned node = 0; ok && node < CLUSTER_MAX_NODES; ++node) {
		ClusterStartupExitMessage m;
		if (!required(&exit_round.cut, node))
			continue;
		m = request_for(node);
		m.verb = CLUSTER_STARTUP_EXIT_REPLY;
		memcpy(m.evidence_sha256, exit_round.evidence[node], 32);
		ok = exit_round.seen[node] && cluster_startup_exit_encode(&m, wire)
			 && pg_cryptohash_update(hash, wire, sizeof(wire)) == 0;
	}
	ok = ok && pg_cryptohash_final(hash, digest, 32) == 0;
	pg_cryptohash_free(hash);
	return ok;
}

ClusterStartupExitResult
cluster_startup_exit_collect(const ClusterStartupExitCut *cut, uint8 digest[32])
{
	bool alias = overlaps(cut, sizeof(*cut), digest, 32);
	bool all = true;
	TimestampTz now;
	bool send;

	if (digest != NULL)
		memset(digest, 0, 32);
	if (alias || cut == NULL || digest == NULL || !cut_current(cut))
		return CLUSTER_STARTUP_EXIT_UNAVAILABLE;
	if (!exit_round.active || memcmp(cut, &exit_round.cut, sizeof(*cut)) != 0) {
		uint64 nonce;
		cluster_startup_exit_cancel();
		if (!pg_strong_random(&nonce, sizeof(nonce)) || nonce == 0)
			return CLUSTER_STARTUP_EXIT_UNAVAILABLE;
		exit_round.cut = *cut;
		exit_round.nonce = nonce;
		exit_round.active = true;
	}
	if (exit_round.poisoned)
		return CLUSTER_STARTUP_EXIT_UNAVAILABLE;
	now = GetCurrentTimestamp();
	/* Clock movement only paces retransmission. It never proves exit. */
	send = !exit_round.attempted || now < exit_round.last_send
		   || now - exit_round.last_send >= EXIT_RETRY_US;
	for (unsigned node = 0; node < CLUSTER_MAX_NODES; ++node) {
		ClusterStartupExitMessage m;
		uint8 wire[CLUSTER_STARTUP_EXIT_BYTES];

		if (!required(cut, node) || exit_round.seen[node])
			continue;
		m = request_for(node);
		if (node == (uint32)cluster_node_id) {
			if (observe_local(&m)) {
				memcpy(exit_round.evidence[node], m.evidence_sha256, 32);
				exit_round.seen[node] = true;
			}
		} else if (send && cluster_startup_exit_encode(&m, wire))
			(void)cluster_ic_send_envelope(PGRAC_IC_MSG_STARTUP_EXIT, (int32)node, wire,
										   sizeof(wire));
		/* A refused send or missing reply retains this exact round for retry. */
		if (!exit_round.seen[node])
			all = false;
	}
	if (send) {
		exit_round.attempted = true;
		exit_round.last_send = now;
	}
	if (!cut_current(cut))
		return CLUSTER_STARTUP_EXIT_UNAVAILABLE;
	if (!all)
		return CLUSTER_STARTUP_EXIT_WAITING;
	if (!round_digest(digest) || !cut_current(cut)) {
		memset(digest, 0, 32);
		return CLUSTER_STARTUP_EXIT_UNAVAILABLE;
	}
	return CLUSTER_STARTUP_EXIT_READY;
}

void
cluster_startup_exit_cancel(void)
{
	if (MyBackendType == B_LMON)
		memset(&exit_round, 0, sizeof(exit_round));
}

void
cluster_startup_exit_ingress(const ClusterICEnvelope *env, const void *payload)
{
	ClusterStartupExitMessage m;
	uint8 wire[CLUSTER_STARTUP_EXIT_BYTES];

	/* Source identity is already bound by the CONTROL router to its HELLO
 * connection. All semantic identities still have to match this live cut. */
	if (env == NULL || env->msg_type != PGRAC_IC_MSG_STARTUP_EXIT
		|| env->dest_node_id != (uint32)cluster_node_id || env->source_node_id >= CLUSTER_MAX_NODES
		|| !cluster_startup_exit_decode(payload, env->payload_length, &m)
		|| env->epoch != m.key.epoch || !local_current(&m.key))
		return;
	if (m.verb == CLUSTER_STARTUP_EXIT_REQUEST) {
		if (env->source_node_id != m.key.coordinator || m.node != (uint32)cluster_node_id
			|| !observe_local(&m) || !cluster_startup_exit_encode(&m, wire))
			return;
		(void)cluster_ic_send_envelope(PGRAC_IC_MSG_STARTUP_EXIT, (int32)m.key.coordinator, wire,
									   sizeof(wire));
		/* The coordinator, not a lost-reply timeout here, re-drives a request. */
	} else {
		if (!exit_round.active || exit_round.poisoned || !cut_current(&exit_round.cut)
			|| m.key.coordinator != (uint32)cluster_node_id || env->source_node_id != m.node
			|| !required(&exit_round.cut, m.node)
			|| memcmp(&m.key, &exit_round.cut.key, sizeof(m.key)) != 0
			|| m.nonce != exit_round.nonce
			|| m.old_incarnation != exit_round.cut.predecessor[m.node]
			|| m.observing_incarnation != exit_round.cut.observer[m.node])
			return;
		if (exit_round.seen[m.node]
			&& memcmp(m.evidence_sha256, exit_round.evidence[m.node], 32) != 0) {
			exit_round.poisoned = true;
			return;
		}
		memcpy(exit_round.evidence[m.node], m.evidence_sha256, 32);
		exit_round.seen[m.node] = true;
	}
}

void
cluster_startup_exit_register(void)
{
	static const ClusterICMsgTypeInfo info = { .msg_type = PGRAC_IC_MSG_STARTUP_EXIT,
											   .name = "startup_prior_exit",
											   .allowed_producer_mask = CLUSTER_IC_PRODUCER_LMON,
											   .broadcast_ok = false,
											   .handler = cluster_startup_exit_ingress,
											   .plane = CLUSTER_IC_PLANE_CONTROL };
	cluster_ic_register_msg_type(&info);
}
