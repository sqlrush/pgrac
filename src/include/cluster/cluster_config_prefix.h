/*-------------------------------------------------------------------------
 *
 * cluster_config_prefix.h
 *    Held native-channel byte-prefix exchange, not application authority.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/include/cluster/cluster_config_prefix.h
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CONFIG_PREFIX_H
#define CLUSTER_CONFIG_PREFIX_H

#include "cluster/cluster_config_members.h"
#include "cluster/cluster_ic_tier1.h"

#define CLUSTER_CONFIG_PREFIX_BYTES 256
#define CLUSTER_CONFIG_PREFIX_MARK 1
#define CLUSTER_CONFIG_PREFIX_ACK 2

typedef struct ClusterConfigPrefixMessage {
	ClusterConfigMembersKey key;
	uint8 episode[16];
	uint8 challenge[16];
	uint64 sender_incarnation;
	uint64 receiver_incarnation;
	uint32 sender;
	uint32 receiver;
	uint32 plane;
	uint32 channel;
	uint32 verb;
} ClusterConfigPrefixMessage;

/* Process-private original owner. Never serialize/copy to another process.
 * The native adapter is its sole caller; no SQL or DATA permission surface. */
typedef struct ClusterConfigPrefixExchange {
	ClusterConfigPrefixMessage mark;
	ClusterICTier1Stream stream;
	uint8 peer_challenge[16];
	uint32 capability_generation;
	bool active;
	bool failed;
	bool mark_admitted;
	bool mark_acknowledged;
	bool peer_mark_seen;
	bool peer_ack_admitted;
} ClusterConfigPrefixExchange;

extern bool cluster_config_prefix_encode(const ClusterConfigPrefixMessage *message,
										 uint8 bytes[CLUSTER_CONFIG_PREFIX_BYTES]);
extern bool cluster_config_prefix_decode(const void *bytes, Size length,
										 ClusterConfigPrefixMessage *out);

/* Both actual endpoints must be armed at the same held episode before poll.
 * Original service/queue/chunk census and membership are caller obligations,
 * not facts supplied by these parameters. Actual native stream selects plane
 * and worker, and a new random challenge distinguishes every local start. */
extern bool cluster_config_prefix_begin(ClusterConfigPrefixExchange *exchange,
										const ClusterConfigMembersKey *key, const uint8 episode[16],
										int32 peer, uint64 local_incarnation,
										uint64 peer_incarnation);
extern void cluster_config_prefix_poll(ClusterConfigPrefixExchange *exchange);
/* Verified native envelope required. Ingress only records a pending ACK;
 * the normal owner tick sends it, with ordinary queue ownership semantics. */
extern bool cluster_config_prefix_ingress(ClusterConfigPrefixExchange *exchange,
										  const ClusterICEnvelope *env, const void *payload);
extern bool cluster_config_prefix_complete(ClusterConfigPrefixExchange *exchange);
extern void cluster_config_prefix_invalidate(ClusterConfigPrefixExchange *exchange);
/* Discards only this coordination object, never socket/module responsibilities. */
extern void cluster_config_prefix_clear(ClusterConfigPrefixExchange *exchange);

#endif
