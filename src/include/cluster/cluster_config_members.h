/*-------------------------------------------------------------------------
 *
 * cluster_config_members.h
 *    Exact member configuration observations, never DATA permission.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/include/cluster/cluster_config_members.h
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CONFIG_MEMBERS_H
#define CLUSTER_CONFIG_MEMBERS_H

#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_shared_config.h"

#define CLUSTER_CONFIG_MEMBERS_BYTES 352
#define CLUSTER_CONFIG_MEMBERS_REQUEST 1
#define CLUSTER_CONFIG_MEMBERS_REPLY 2
#define CLUSTER_CONFIG_MEMBERS_OBSERVED 1
#define CLUSTER_CONFIG_MEMBERS_UNAVAILABLE 2

/* Memory carriers only. All wire fields use explicit literal encoding. */
typedef struct ClusterConfigMembersKey {
	ClusterSharedConfigRef ref;
	uint64 epoch;
	uint64 required[2];
	uint8 members_sha256[32];
} ClusterConfigMembersKey;

typedef struct ClusterConfigMembersMessage {
	ClusterConfigMembersKey key;
	uint64 nonce;
	uint64 collector_incarnation;
	uint64 responder_incarnation;
	uint32 collector;
	uint32 responder;
	uint32 verb;
	uint32 outcome;
	ClusterSharedConfigCensus census;
} ClusterConfigMembersMessage;

typedef struct ClusterConfigMembersObservation {
	ClusterConfigMembersKey key;
	uint64 nonce;
	uint64 observed[2];
	uint64 unavailable[2];
	ClusterSharedConfigCensus node[CLUSTER_MAX_NODES];
} ClusterConfigMembersObservation;

/* Refusal clears nonaliasing output; an alias leaves both carriers untouched. */
extern bool cluster_config_members_encode(const ClusterConfigMembersMessage *message,
										  uint8 bytes[CLUSTER_CONFIG_MEMBERS_BYTES]);
extern bool cluster_config_members_decode(const void *bytes, Size length,
										  ClusterConfigMembersMessage *out);

/* Canonical local admitted-member key; output must not alias either input. */
extern bool cluster_config_members_make_key(const ClusterSharedConfigRef *ref,
											const ClusterR4MembershipSnapshot *members,
											ClusterConfigMembersKey *key);

/* LMON only, after actual CF selection/retirement at this member cut. One
 * observational round, retried by the existing delivery tick; no timeout ACK.
 * Does not freeze births, assignments or remote state after observation.
 * No CF/native hooks in ingress. Cancellation leaves no peer-owned obligation.
 */
extern void cluster_config_members_poll(const ClusterSharedConfigRef *selected,
										const ClusterR4MembershipSnapshot *members);
extern void cluster_config_members_cancel(void);
extern bool cluster_config_members_observe(ClusterConfigMembersObservation *out);
extern void cluster_config_members_ingress(const ClusterICEnvelope *env, const void *payload);
extern void cluster_config_members_register(void);

#endif /* CLUSTER_CONFIG_MEMBERS_H */
