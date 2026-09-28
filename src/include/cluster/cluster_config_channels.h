/*-------------------------------------------------------------------------
 *
 * cluster_config_channels.h
 *    Ephemeral native-owner coordination of configuration channel prefixes.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/include/cluster/cluster_config_channels.h
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CONFIG_CHANNELS_H
#define CLUSTER_CONFIG_CHANNELS_H

#include "cluster/cluster_config_prefix.h"
#include "port/atomics.h"

#define CLUSTER_CONFIG_CHANNEL_OWNERS (1 + CLUSTER_IC_TIER1_DATA_CHANNELS)

typedef struct ClusterConfigChannelOwner {
	uint64 registration;
	int32 pid;
	int32 procno;
} ClusterConfigChannelOwner;

/* Native family memory only. No struct below is persisted or sent on wire. */
typedef struct ClusterConfigChannelsCommand {
	ClusterConfigMembersKey key;
	uint64 incarnation[CLUSTER_MAX_NODES];
	uint8 episode[16];
	ClusterConfigChannelOwner owner;
	uint64 serial;
	uint32 phase;
	uint32 workers;
} ClusterConfigChannelsCommand;

typedef struct ClusterConfigChannelReport {
	ClusterConfigChannelOwner owner;
	uint64 serial;
	uint64 armed[2];
	uint64 complete[2];
	uint32 capability_generation[CLUSTER_MAX_NODES];
	uint32 phase;
} ClusterConfigChannelReport;

typedef struct ClusterConfigChannelSlot {
	pg_atomic_uint64 sequence;
	ClusterConfigChannelReport report;
} ClusterConfigChannelSlot;

typedef struct ClusterConfigChannelsBoard {
	pg_atomic_uint64 command_sequence;
	ClusterConfigChannelsCommand command;
	ClusterConfigChannelSlot slots[CLUSTER_CONFIG_CHANNEL_OWNERS];
} ClusterConfigChannelsBoard;

typedef struct ClusterConfigChannelsCensus {
	uint64 serial;
	uint32 required;
	uint32 armed;
	uint32 complete;
	uint32 invalid;
} ClusterConfigChannelsCensus;

/* Initialization is allowed only at native family creation/all-old-child reset. */
static inline void
cluster_config_channels_init(ClusterConfigChannelsBoard *board)
{
	memset(board, 0, sizeof(*board));
	pg_atomic_init_u64(&board->command_sequence, 0);
	for (unsigned i = 0; i < CLUSTER_CONFIG_CHANNEL_OWNERS; ++i)
		pg_atomic_init_u64(&board->slots[i].sequence, 0);
}
extern ClusterConfigChannelsBoard *cluster_shared_config_delivery_channels(int32 *lmon_pid);
/* Actual retained LMON coordinator only. ARM does not send. The caller must
 * hold original services/producers and prove ALL remote endpoints armed
 * before EXCHANGE; this local component cannot supply that cluster fact. */
extern bool cluster_config_channels_arm(const ClusterSharedConfigRef *selected,
										const uint8 episode[16]);
extern bool cluster_config_channels_exchange(void);
extern void cluster_config_channels_cancel(void);
/* Bounded local observation only, never module retirement or application ACK. */
extern bool cluster_config_channels_observe(ClusterConfigChannelsCensus *out);
extern void cluster_config_channels_tick(void);
/* Native transport calls before retiring/replacing a stream; -1 means all.
 * Bounded publication only: no allocation, locking, send or resource release. */
extern void cluster_config_channels_stream_retiring(int32 peer);
extern void cluster_config_channels_ingress(const ClusterICEnvelope *env, const void *bytes);
extern void cluster_config_channels_register(void);

#endif /* CLUSTER_CONFIG_CHANNELS_H */
