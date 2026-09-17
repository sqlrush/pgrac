/*-------------------------------------------------------------------------
 *
 * cluster_replacement_episode.h
 *    Same-node replacement episode node-local mirror (spec-5.15A §2.4).
 *
 *    This dependency-free layer owns only the exact 96-byte host-order
 *    shape and pure structural/identity/generation validation.  It owns no
 *    wire, voting-disk, lock, shmem allocation, or actor integration.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_REPLACEMENT_EPISODE_H
#define CLUSTER_REPLACEMENT_EPISODE_H

#include "c.h"

#include "cluster/cluster_conf.h"


#define CLUSTER_REPLACEMENT_EPISODE_BYTES 96
#define CLUSTER_REPLACEMENT_EPISODE_BITMAP_BYTES 16

#define CLUSTER_REPLACEMENT_EPISODE_GRAMMAR_FINGERPRINT UINT64_C(0x8e0dae5b428905e4)

#define CLUSTER_REPLACEMENT_EPISODE_R4A_TARGET_READY UINT8_C(0x01)
#define CLUSTER_REPLACEMENT_EPISODE_GRD_POSTEPOCH_READY UINT8_C(0x02)
#define CLUSTER_REPLACEMENT_EPISODE_INTENT_CLEARED UINT8_C(0x08)
#define CLUSTER_REPLACEMENT_EPISODE_READINESS_MASK                                                 \
	(CLUSTER_REPLACEMENT_EPISODE_R4A_TARGET_READY                                                  \
	 | CLUSTER_REPLACEMENT_EPISODE_GRD_POSTEPOCH_READY                                             \
	 | CLUSTER_REPLACEMENT_EPISODE_INTENT_CLEARED)

typedef enum ClusterReplacementEpisodePhase {
	CLUSTER_REPLACEMENT_EPISODE_EMPTY = 0,
	CLUSTER_REPLACEMENT_EPISODE_PREPARE_DURABLE = 1,
	CLUSTER_REPLACEMENT_EPISODE_PURGING = 2,
	CLUSTER_REPLACEMENT_EPISODE_PURGE_COMPLETE = 3,
	CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED = 4,
	CLUSTER_REPLACEMENT_EPISODE_POST_EPOCH = 5,
	CLUSTER_REPLACEMENT_EPISODE_ADMITTED = 6,
	CLUSTER_REPLACEMENT_EPISODE_HOLD = 7
} ClusterReplacementEpisodePhase;

typedef struct ClusterReplacementEpisode {
	uint64 request_nonce;												/* 0 */
	uint64 baseline_epoch;												/* 8 */
	uint64 reserved_or_committed_epoch;									/* 16: exact baseline+1 */
	uint64 old_admitted_incarnation;									/* 24 */
	uint64 fresh_incarnation;											/* 32 */
	uint64 grammar_fingerprint;											/* 40 */
	uint8 expected_survivors[CLUSTER_REPLACEMENT_EPISODE_BITMAP_BYTES]; /* 48 */
	uint8 acknowledgements[CLUSTER_REPLACEMENT_EPISODE_BITMAP_BYTES];	/* 64 */
	int32 target_node_id;												/* 80 */
	int32 coordinator_node_id;											/* 84 */
	uint32 state_generation;											/* 88; zero means empty */
	uint8 phase;														/* 92 */
	uint8 readiness_flags;												/* 93 */
	uint8 reserved[2];													/* 94: zero */
} ClusterReplacementEpisode;

StaticAssertDecl(sizeof(ClusterReplacementEpisode) == CLUSTER_REPLACEMENT_EPISODE_BYTES,
				 "replacement episode must remain 96 bytes");
StaticAssertDecl(offsetof(ClusterReplacementEpisode, request_nonce) == 0,
				 "replacement request nonce offset must remain 0");
StaticAssertDecl(offsetof(ClusterReplacementEpisode, baseline_epoch) == 8,
				 "replacement baseline epoch offset must remain 8");
StaticAssertDecl(offsetof(ClusterReplacementEpisode, reserved_or_committed_epoch) == 16,
				 "replacement committed epoch offset must remain 16");
StaticAssertDecl(offsetof(ClusterReplacementEpisode, old_admitted_incarnation) == 24,
				 "replacement old incarnation offset must remain 24");
StaticAssertDecl(offsetof(ClusterReplacementEpisode, fresh_incarnation) == 32,
				 "replacement fresh incarnation offset must remain 32");
StaticAssertDecl(offsetof(ClusterReplacementEpisode, grammar_fingerprint) == 40,
				 "replacement fingerprint offset must remain 40");
StaticAssertDecl(offsetof(ClusterReplacementEpisode, expected_survivors) == 48,
				 "replacement survivor bitmap offset must remain 48");
StaticAssertDecl(offsetof(ClusterReplacementEpisode, acknowledgements) == 64,
				 "replacement acknowledgement bitmap offset must remain 64");
StaticAssertDecl(offsetof(ClusterReplacementEpisode, target_node_id) == 80,
				 "replacement target offset must remain 80");
StaticAssertDecl(offsetof(ClusterReplacementEpisode, coordinator_node_id) == 84,
				 "replacement coordinator offset must remain 84");
StaticAssertDecl(offsetof(ClusterReplacementEpisode, state_generation) == 88,
				 "replacement state generation offset must remain 88");
StaticAssertDecl(offsetof(ClusterReplacementEpisode, phase) == 92,
				 "replacement phase offset must remain 92");
StaticAssertDecl(offsetof(ClusterReplacementEpisode, readiness_flags) == 93,
				 "replacement readiness offset must remain 93");
StaticAssertDecl(offsetof(ClusterReplacementEpisode, reserved) == 94,
				 "replacement reserved offset must remain 94");

extern bool cluster_replacement_episode_is_empty(const ClusterReplacementEpisode *episode);
extern bool cluster_replacement_episode_is_valid(const ClusterReplacementEpisode *episode);
extern bool cluster_replacement_episode_next_generation(uint32 current, uint32 *next);

#endif /* CLUSTER_REPLACEMENT_EPISODE_H */
