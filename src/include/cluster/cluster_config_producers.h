/*-------------------------------------------------------------------------
 *
 * cluster_config_producers.h
 *    Original LMON ownership of ordered local configuration producer cuts.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/include/cluster/cluster_config_producers.h
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CONFIG_PRODUCERS_H
#define CLUSTER_CONFIG_PRODUCERS_H

#include "cluster/cluster_config_members.h"
#include "cluster/cluster_config_use_gate.h"

typedef enum ClusterConfigProducerStage {
	CLUSTER_CONFIG_PRODUCERS_FRONT = 1,
	CLUSTER_CONFIG_PRODUCERS_STORAGE,
	CLUSTER_CONFIG_PRODUCERS_QUIET
} ClusterConfigProducerStage;

typedef enum ClusterConfigProducerResult {
	CLUSTER_CONFIG_PRODUCERS_INVALID,
	CLUSTER_CONFIG_PRODUCERS_PENDING,
	CLUSTER_CONFIG_PRODUCERS_READY
} ClusterConfigProducerResult;

#define CLUSTER_CONFIG_PRODUCERS_COUNT (2 + CLUSTER_CONFIG_BACKGROUND_COUNT)

typedef struct ClusterConfigProducerCensus {
	ClusterConfigMembersKey key;
	uint8 episode[16];
	uint32 stage;
	uint32 held;
	uint32 pending;
	uint32 failed;
	bool bound;
	ClusterSharedConfigActive common;
} ClusterConfigProducerCensus;

/* Original registered LMON only, after actual CF selection. Stages cannot be
 * skipped. Before STORAGE/QUIET the retained CLUSTER controller must prove
 * all peers at the prior cut and old receipt/WAL obligations retired. This
 * local adapter cannot provide that fact. No wire, SQL or admission here. */
extern ClusterConfigProducerResult
cluster_config_producers_hold(const ClusterSharedConfigRef *selected, const uint8 episode[16],
							  ClusterConfigProducerStage stage);
extern ClusterConfigProducerResult
cluster_config_producers_observe(ClusterConfigProducerCensus *out);
/* Original LMON's synchronous fresh entry only. FRONT holds periodic CF
 * discovery; QUIET holds new asynchronous/automatic duties. Existing work
 * must keep retiring. Reads actual family cuts, not a private owner flag;
 * this is neither root authority nor permission to release any cut. */
extern bool cluster_config_producers_fresh_allowed(ClusterConfigProducerStage stage);
/* Bind real native common census to every future producer, with all cuts held.
 * This is NOT permission to APPLY or an all-member application certificate. */
extern ClusterConfigProducerResult cluster_config_producers_bind(void);
/* Caller already owns exact all-member APPLIED and service/channel/root proof.
 * Rechecks local owner/member/census/cookies. No failed/foreign cut is opened.
 * There is deliberately no generic reset/cancel-unlock API. */
extern bool cluster_config_producers_release(void);

#endif /* CLUSTER_CONFIG_PRODUCERS_H */
