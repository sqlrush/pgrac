/*-------------------------------------------------------------------------
 *
 * test_cluster_replacement_episode.c
 *    Pure tests for the spec-5.15A replacement episode local mirror.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stddef.h>

#include "cluster/cluster_replacement_episode.h"

#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

static void
episode_bitmap_set(uint8 bitmap[CLUSTER_REPLACEMENT_EPISODE_BITMAP_BYTES], int node_id)
{
	bitmap[node_id / 8] |= (uint8)(1u << (node_id % 8));
}

static ClusterReplacementEpisode
make_valid_episode(void)
{
	ClusterReplacementEpisode episode;

	memset(&episode, 0, sizeof(episode));
	episode.request_nonce = UINT64_C(0x1112131415161718);
	episode.baseline_epoch = UINT64_C(40);
	episode.reserved_or_committed_epoch = UINT64_C(41);
	episode.old_admitted_incarnation = UINT64_C(70);
	episode.fresh_incarnation = UINT64_C(71);
	episode.grammar_fingerprint = CLUSTER_REPLACEMENT_EPISODE_GRAMMAR_FINGERPRINT;
	episode.target_node_id = 3;
	episode.coordinator_node_id = 1;
	episode.state_generation = 9;
	episode.phase = CLUSTER_REPLACEMENT_EPISODE_PREPARE_DURABLE;
	episode.readiness_flags = 0;
	episode_bitmap_set(episode.expected_survivors, 1);
	episode_bitmap_set(episode.expected_survivors, 2);
	episode_bitmap_set(episode.acknowledgements, 1);
	return episode;
}

UT_TEST(test_episode_layout_is_exactly_96_bytes)
{
	UT_ASSERT_EQ((int)sizeof(ClusterReplacementEpisode), 96);
	UT_ASSERT_EQ((int)offsetof(ClusterReplacementEpisode, request_nonce), 0);
	UT_ASSERT_EQ((int)offsetof(ClusterReplacementEpisode, baseline_epoch), 8);
	UT_ASSERT_EQ((int)offsetof(ClusterReplacementEpisode, reserved_or_committed_epoch), 16);
	UT_ASSERT_EQ((int)offsetof(ClusterReplacementEpisode, old_admitted_incarnation), 24);
	UT_ASSERT_EQ((int)offsetof(ClusterReplacementEpisode, fresh_incarnation), 32);
	UT_ASSERT_EQ((int)offsetof(ClusterReplacementEpisode, grammar_fingerprint), 40);
	UT_ASSERT_EQ((int)offsetof(ClusterReplacementEpisode, expected_survivors), 48);
	UT_ASSERT_EQ((int)offsetof(ClusterReplacementEpisode, acknowledgements), 64);
	UT_ASSERT_EQ((int)offsetof(ClusterReplacementEpisode, target_node_id), 80);
	UT_ASSERT_EQ((int)offsetof(ClusterReplacementEpisode, coordinator_node_id), 84);
	UT_ASSERT_EQ((int)offsetof(ClusterReplacementEpisode, state_generation), 88);
	UT_ASSERT_EQ((int)offsetof(ClusterReplacementEpisode, phase), 92);
	UT_ASSERT_EQ((int)offsetof(ClusterReplacementEpisode, readiness_flags), 93);
	UT_ASSERT_EQ((int)offsetof(ClusterReplacementEpisode, reserved), 94);
}

UT_TEST(test_only_the_canonical_zero_episode_is_empty)
{
	ClusterReplacementEpisode episode;

	memset(&episode, 0, sizeof(episode));
	UT_ASSERT(cluster_replacement_episode_is_empty(&episode));
	UT_ASSERT(cluster_replacement_episode_is_valid(&episode));
	UT_ASSERT(!cluster_replacement_episode_is_empty(NULL));
	UT_ASSERT(!cluster_replacement_episode_is_valid(NULL));

	episode.request_nonce = 1;
	UT_ASSERT(!cluster_replacement_episode_is_empty(&episode));
	UT_ASSERT(!cluster_replacement_episode_is_valid(&episode));
}

UT_TEST(test_all_frozen_phases_and_readiness_combinations_validate)
{
	static const uint8 valid_flags[] = {
		0,
		CLUSTER_REPLACEMENT_EPISODE_R4A_TARGET_READY,
		CLUSTER_REPLACEMENT_EPISODE_GRD_POSTEPOCH_READY,
		CLUSTER_REPLACEMENT_EPISODE_R4A_TARGET_READY
			| CLUSTER_REPLACEMENT_EPISODE_GRD_POSTEPOCH_READY,
		CLUSTER_REPLACEMENT_EPISODE_INTENT_CLEARED,
		CLUSTER_REPLACEMENT_EPISODE_R4A_TARGET_READY | CLUSTER_REPLACEMENT_EPISODE_INTENT_CLEARED,
		CLUSTER_REPLACEMENT_EPISODE_GRD_POSTEPOCH_READY
			| CLUSTER_REPLACEMENT_EPISODE_INTENT_CLEARED,
		CLUSTER_REPLACEMENT_EPISODE_READINESS_MASK,
	};
	ClusterReplacementEpisode episode;
	int phase;
	int i;

	for (phase = CLUSTER_REPLACEMENT_EPISODE_PREPARE_DURABLE;
		 phase <= CLUSTER_REPLACEMENT_EPISODE_HOLD; phase++) {
		for (i = 0; i < lengthof(valid_flags); i++) {
			episode = make_valid_episode();
			episode.phase = (uint8)phase;
			episode.readiness_flags = valid_flags[i];
			UT_ASSERT(cluster_replacement_episode_is_valid(&episode));
		}
	}

	episode = make_valid_episode();
	episode.phase = CLUSTER_REPLACEMENT_EPISODE_EMPTY;
	UT_ASSERT(!cluster_replacement_episode_is_valid(&episode));
	episode.phase = UINT8_C(8);
	UT_ASSERT(!cluster_replacement_episode_is_valid(&episode));
	episode = make_valid_episode();
	episode.readiness_flags = UINT8_C(0x04);
	UT_ASSERT(!cluster_replacement_episode_is_valid(&episode));
	episode.readiness_flags = UINT8_C(0x80);
	UT_ASSERT(!cluster_replacement_episode_is_valid(&episode));
	episode = make_valid_episode();
	episode.reserved[1] = 1;
	UT_ASSERT(!cluster_replacement_episode_is_valid(&episode));
}

UT_TEST(test_episode_identity_is_fail_closed)
{
	ClusterReplacementEpisode episode;

	episode = make_valid_episode();
	episode.request_nonce = 0;
	UT_ASSERT(!cluster_replacement_episode_is_valid(&episode));
	episode = make_valid_episode();
	episode.old_admitted_incarnation = 0;
	UT_ASSERT(!cluster_replacement_episode_is_valid(&episode));
	episode = make_valid_episode();
	episode.fresh_incarnation = episode.old_admitted_incarnation;
	UT_ASSERT(!cluster_replacement_episode_is_valid(&episode));
	episode = make_valid_episode();
	episode.fresh_incarnation = episode.old_admitted_incarnation - 1;
	UT_ASSERT(!cluster_replacement_episode_is_valid(&episode));
	episode = make_valid_episode();
	episode.reserved_or_committed_epoch++;
	UT_ASSERT(!cluster_replacement_episode_is_valid(&episode));
	episode = make_valid_episode();
	episode.baseline_epoch = UINT64_MAX;
	episode.reserved_or_committed_epoch = 0;
	UT_ASSERT(!cluster_replacement_episode_is_valid(&episode));
	episode = make_valid_episode();
	episode.grammar_fingerprint ^= UINT64_C(1);
	UT_ASSERT(!cluster_replacement_episode_is_valid(&episode));
	episode = make_valid_episode();
	episode.target_node_id = -1;
	UT_ASSERT(!cluster_replacement_episode_is_valid(&episode));
	episode = make_valid_episode();
	episode.target_node_id = 128;
	UT_ASSERT(!cluster_replacement_episode_is_valid(&episode));
	episode = make_valid_episode();
	episode.coordinator_node_id = -1;
	UT_ASSERT(!cluster_replacement_episode_is_valid(&episode));
	episode = make_valid_episode();
	episode.coordinator_node_id = 128;
	UT_ASSERT(!cluster_replacement_episode_is_valid(&episode));
	episode = make_valid_episode();
	episode.coordinator_node_id = 4;
	UT_ASSERT(!cluster_replacement_episode_is_valid(&episode));
	episode = make_valid_episode();
	episode.state_generation = 0;
	UT_ASSERT(!cluster_replacement_episode_is_valid(&episode));
}

UT_TEST(test_survivor_and_acknowledgement_sets_are_bounded)
{
	ClusterReplacementEpisode episode;

	episode = make_valid_episode();
	memset(episode.expected_survivors, 0, sizeof(episode.expected_survivors));
	memset(episode.acknowledgements, 0, sizeof(episode.acknowledgements));
	UT_ASSERT(!cluster_replacement_episode_is_valid(&episode));

	episode = make_valid_episode();
	episode_bitmap_set(episode.expected_survivors, episode.target_node_id);
	UT_ASSERT(!cluster_replacement_episode_is_valid(&episode));

	episode = make_valid_episode();
	episode_bitmap_set(episode.acknowledgements, 4);
	UT_ASSERT(!cluster_replacement_episode_is_valid(&episode));
}

UT_TEST(test_state_generation_increment_is_checked)
{
	uint32 next;

	next = UINT32_C(99);
	UT_ASSERT(cluster_replacement_episode_next_generation(0, &next));
	UT_ASSERT_EQ((int)next, 1);
	UT_ASSERT(cluster_replacement_episode_next_generation(UINT32_C(41), &next));
	UT_ASSERT_EQ((int)next, 42);
	UT_ASSERT(cluster_replacement_episode_next_generation(UINT32_MAX - 1, &next));
	UT_ASSERT(next == UINT32_MAX);

	next = UINT32_C(77);
	UT_ASSERT(!cluster_replacement_episode_next_generation(UINT32_MAX, &next));
	UT_ASSERT_EQ((int)next, 77);
	UT_ASSERT(!cluster_replacement_episode_next_generation(1, NULL));
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_episode_layout_is_exactly_96_bytes);
	UT_RUN(test_only_the_canonical_zero_episode_is_empty);
	UT_RUN(test_all_frozen_phases_and_readiness_combinations_validate);
	UT_RUN(test_episode_identity_is_fail_closed);
	UT_RUN(test_survivor_and_acknowledgement_sets_are_bounded);
	UT_RUN(test_state_generation_increment_is_checked);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
