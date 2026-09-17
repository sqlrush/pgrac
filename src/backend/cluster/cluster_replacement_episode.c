/*-------------------------------------------------------------------------
 *
 * cluster_replacement_episode.c
 *    Pure validation for the spec-5.15A replacement episode local mirror.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_replacement_episode.h"


static bool
replacement_episode_node_valid(int32 node_id)
{
	return node_id >= 0 && node_id < CLUSTER_MAX_NODES;
}

static bool
replacement_episode_phase_valid(uint8 phase)
{
	return phase >= CLUSTER_REPLACEMENT_EPISODE_PREPARE_DURABLE
		   && phase <= CLUSTER_REPLACEMENT_EPISODE_HOLD;
}

static bool
replacement_episode_bitmap_empty(const uint8 bitmap[CLUSTER_REPLACEMENT_EPISODE_BITMAP_BYTES])
{
	int i;

	for (i = 0; i < CLUSTER_REPLACEMENT_EPISODE_BITMAP_BYTES; i++) {
		if (bitmap[i] != 0)
			return false;
	}
	return true;
}

static bool
replacement_episode_bitmap_has(const uint8 bitmap[CLUSTER_REPLACEMENT_EPISODE_BITMAP_BYTES],
							   int32 node_id)
{
	return (bitmap[node_id / 8] & (uint8)(1u << (node_id % 8))) != 0;
}

static bool
replacement_episode_acks_bounded(const ClusterReplacementEpisode *episode)
{
	int i;

	for (i = 0; i < CLUSTER_REPLACEMENT_EPISODE_BITMAP_BYTES; i++) {
		if ((episode->acknowledgements[i] & (uint8)~episode->expected_survivors[i]) != 0)
			return false;
	}
	return true;
}

bool
cluster_replacement_episode_is_empty(const ClusterReplacementEpisode *episode)
{
	static const ClusterReplacementEpisode empty_episode = { 0 };

	return episode != NULL && memcmp(episode, &empty_episode, sizeof(*episode)) == 0;
}

bool
cluster_replacement_episode_is_valid(const ClusterReplacementEpisode *episode)
{
	if (episode == NULL)
		return false;
	if (episode->state_generation == 0)
		return cluster_replacement_episode_is_empty(episode);

	if (!replacement_episode_phase_valid(episode->phase)
		|| (episode->readiness_flags & (uint8)~CLUSTER_REPLACEMENT_EPISODE_READINESS_MASK) != 0
		|| episode->reserved[0] != 0 || episode->reserved[1] != 0)
		return false;

	if (episode->request_nonce == 0 || episode->old_admitted_incarnation == 0
		|| episode->fresh_incarnation <= episode->old_admitted_incarnation
		|| episode->baseline_epoch == UINT64_MAX
		|| episode->reserved_or_committed_epoch != episode->baseline_epoch + 1
		|| episode->grammar_fingerprint != CLUSTER_REPLACEMENT_EPISODE_GRAMMAR_FINGERPRINT)
		return false;

	if (!replacement_episode_node_valid(episode->target_node_id)
		|| !replacement_episode_node_valid(episode->coordinator_node_id)
		|| replacement_episode_bitmap_empty(episode->expected_survivors)
		|| replacement_episode_bitmap_has(episode->expected_survivors, episode->target_node_id)
		|| !replacement_episode_bitmap_has(episode->expected_survivors,
										   episode->coordinator_node_id)
		|| !replacement_episode_acks_bounded(episode))
		return false;

	return true;
}

bool
cluster_replacement_episode_next_generation(uint32 current, uint32 *next)
{
	if (next == NULL || current == UINT32_MAX)
		return false;
	*next = current + 1;
	return true;
}
