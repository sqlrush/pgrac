/* Manually selected full-consumer acceptance RED. Literal member proofs
 * exercise the original pure decision/recompose bodies, not live authority.
 * Legacy wire decoders retain their own bounded-domain tests.
 *
 * Bind the currently available consumer here. A future independent full-domain
 * entry must replace both bindings, including the signature gates; this test
 * does not require widening a legacy wire decoder or its bounded adapter. */
int k_full_consumer_fixture_main(void);
#define main k_full_consumer_fixture_main
#include "test_cluster_multixact_current.c"
#undef main
#define K_FULL_CONSUMER_DECIDE cluster_multixact_current_decide_observed
#define K_FULL_CONSUMER_RECOMPOSE cluster_multixact_current_recompose

typedef struct KFullConsumerInput {
	uint32 count;
	ClusterCurrentMxMemberDesc *members;
	ClusterCurrentMemberProof *proofs;
} KFullConsumerInput;

static KFullConsumerInput
input(uint32 count, ClusterCurrentMemberState state)
{
	KFullConsumerInput data = { 0 };
	/* The current entry can represent these literal counts exactly. The
	 * separate API-domain test covers counts it cannot receive at all. */
	UT_ASSERT(count > 0 && count <= UINT16_MAX);
	data.count = count;
	data.members = calloc(count, sizeof(*data.members));
	data.proofs = calloc(count, sizeof(*data.proofs));
	if (data.members == NULL || data.proofs == NULL)
		abort();
	for (uint32 i = 0; i < count; i++) {
		test_member(&data.members[i], 100 + i, MultiXactStatusForShare);
		test_proof(&data.proofs[i], &data.members[i], (uint16)i, state, 2, 1);
	}
	return data;
}

static void
finish(KFullConsumerInput *data)
{
	free(data->proofs);
	free(data->members);
	memset(data, 0, sizeof(*data));
}

static ClusterCurrentMxDecision
decide(const KFullConsumerInput *data, bool deleted, ClusterTTStatusKey *wait)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxRequestContext ctx;
	test_context(&ctx, &key, CCM_ACTION_LOCK, MultiXactStatusForUpdate, LockTupleExclusive);
	if (deleted)
		ctx.tuple_shape = CCM_SHAPE_DELETED;
	return K_FULL_CONSUMER_DECIDE(data->members, data->proofs,
		(uint16)data->count, &ctx, NULL, NULL, wait, NULL);
}

UT_TEST(original_two_and_256_member_controls)
{
	const uint32 sizes[] = { 2, 256 };
	for (unsigned s = 0; s < lengthof(sizes); s++) {
		KFullConsumerInput data = input(sizes[s], CCM_COMMITTED);
		MultiXactMember result[1];
		uint16 count = 99;
		UT_ASSERT_EQ(decide(&data, false, NULL), CMDL_CONTINUE);
		memset(result, 0xa5, sizeof(result));
		UT_ASSERT_EQ(K_FULL_CONSUMER_RECOMPOSE(data.members, data.proofs,
			(uint16)data.count, 7, MultiXactStatusForShare, result, 1, &count), CMX_RECOMPOSE_OK);
		UT_ASSERT_EQ(count, 1); UT_ASSERT_EQ(result[0].xid, 7);
		UT_ASSERT_EQ(result[0].status, MultiXactStatusForShare);
		/* The tail-updater fixture must be valid in the existing domain. */
		data.members[data.count - 1].member_status = MultiXactStatusUpdate;
		data.proofs[data.count - 1].member_status = MultiXactStatusUpdate;
		UT_ASSERT_EQ(decide(&data, true, NULL), CMDL_DELETED);
		finish(&data);
	}
	{
		KFullConsumerInput data = input(255, CCM_ACTIVE);
		MultiXactMember result[256];
		uint16 count = 99;
		/* Structural ACTIVE proofs test only the pure normalization plan. */
		UT_ASSERT_EQ(K_FULL_CONSUMER_RECOMPOSE(data.members, data.proofs,
			255, 7, MultiXactStatusForShare, result, 256, &count), CMX_RECOMPOSE_OK);
		UT_ASSERT_EQ(count, 256);
		if (count == 256) {
			for (uint32 i = 0; i < 255; i++) {
				UT_ASSERT_EQ(result[i].xid, 100 + i);
				UT_ASSERT_EQ(result[i].status, MultiXactStatusForShare);
			}
			UT_ASSERT_EQ(result[255].xid, 7);
			UT_ASSERT_EQ(result[255].status, MultiXactStatusForShare);
		}
		finish(&data);
	}
}

UT_TEST(full_terminal_lockers_allow_original_decision)
{
	const uint32 sizes[] = { 257, 65535 };
	for (unsigned s = 0; s < lengthof(sizes); s++) {
		KFullConsumerInput data = input(sizes[s], CCM_COMMITTED);
		UT_ASSERT_EQ(decide(&data, false, NULL), CMDL_CONTINUE);
		finish(&data);
	}
}

UT_TEST(full_tail_committed_updater_is_not_a_locker_prefix)
{
	const uint32 sizes[] = { 257, 65535 };
	for (unsigned s = 0; s < lengthof(sizes); s++) {
		KFullConsumerInput data = input(sizes[s], CCM_COMMITTED);
		uint32 last = data.count - 1;
		data.members[last].member_status = MultiXactStatusUpdate;
		data.proofs[last].member_status = MultiXactStatusUpdate;
		UT_ASSERT_EQ(decide(&data, true, NULL), CMDL_DELETED);
		finish(&data);
	}
}

UT_TEST(full_unknown_tail_never_returns_positive_decision)
{
	const uint32 sizes[] = { 257, 65535 };
	static const ClusterTTStatusKey zero;
	for (unsigned s = 0; s < lengthof(sizes); s++) {
		KFullConsumerInput data = input(sizes[s], CCM_COMMITTED);
		ClusterTTStatusKey wait;
		data.proofs[data.count - 1].state = CCM_UNKNOWN;
		data.proofs[data.count - 1].commit_scn = InvalidScn;
		memset(&wait, 0xa5, sizeof(wait));
		UT_ASSERT_EQ(decide(&data, false, &wait), CMDL_UNKNOWN);
		UT_ASSERT_EQ(memcmp(&wait, &zero, sizeof(wait)), 0);
		finish(&data);
	}
}

UT_TEST(full_unknown_tail_clears_entire_recompose_output)
{
	KFullConsumerInput data = input(257, CCM_COMMITTED);
	MultiXactMember result[2];
	const MultiXactMember zero[2] = { { 0 } };
	uint16 count = 99;
	data.proofs[256].state = CCM_UNKNOWN;
	data.proofs[256].commit_scn = InvalidScn;
	memset(result, 0xa5, sizeof(result));
	UT_ASSERT_EQ(K_FULL_CONSUMER_RECOMPOSE(data.members, data.proofs,
		257, 7, MultiXactStatusForShare, result, 2, &count), CMX_RECOMPOSE_UNKNOWN);
	UT_ASSERT_EQ(count, 0); UT_ASSERT_EQ(memcmp(result, zero, sizeof(result)), 0);
	finish(&data);
}

UT_TEST(all_256_live_lockers_and_new_requester_are_preserved)
{
	KFullConsumerInput data = input(256, CCM_ACTIVE);
	MultiXactMember result[257];
	uint16 count = 99;
	memset(result, 0xa5, sizeof(result));
	UT_ASSERT_EQ(K_FULL_CONSUMER_RECOMPOSE(data.members, data.proofs,
		256, 7, MultiXactStatusForShare, result, 257, &count), CMX_RECOMPOSE_OK);
	UT_ASSERT_EQ(count, 257);
	/* Do not inspect unproduced output after the known product RED. */
	if (count == 257) {
		for (uint32 i = 0; i < 256; i++) {
			UT_ASSERT_EQ(result[i].xid, 100 + i);
			UT_ASSERT_EQ(result[i].status, MultiXactStatusForShare);
		}
		UT_ASSERT_EQ(result[256].xid, 7);
		UT_ASSERT_EQ(result[256].status, MultiXactStatusForShare);
	}
	finish(&data);
}

UT_TEST(full_consumer_can_receive_65537_without_count_narrowing)
{
	typedef ClusterCurrentMxDecision (*WideDecision)(const ClusterCurrentMxMemberDesc *,
		const ClusterCurrentMemberProof *, uint32, const ClusterCurrentMxRequestContext *,
		const ClusterCurrentUpdaterChallenge *, const ClusterCurrentUpdaterProof *,
		ClusterTTStatusKey *, ClusterCurrentMxDecisionTrace *);
	typedef ClusterMxRecomposeResult (*WideRecompose)(const ClusterCurrentMxMemberDesc *,
		const ClusterCurrentMemberProof *, uint32, TransactionId, MultiXactStatus,
		MultiXactMember *, uint32, uint32 *);
	/* API RED, not a truncated runtime call: 65537 cannot be passed to the
	 * current uint16 consumer. Keep canonical F192's local ordinal intact. */
	UT_ASSERT(_Generic(&K_FULL_CONSUMER_DECIDE, WideDecision: true, default: false));
	UT_ASSERT(_Generic(&K_FULL_CONSUMER_RECOMPOSE, WideRecompose: true, default: false));
}

UT_TEST(full_terminal_recompose_retires_every_locker)
{
	const uint32 sizes[] = { 257, 65535 };
	for (unsigned s = 0; s < lengthof(sizes); s++) {
		KFullConsumerInput data = input(sizes[s], CCM_COMMITTED);
		MultiXactMember result[1];
		uint16 count = 99;
		memset(result, 0xa5, sizeof(result));
		UT_ASSERT_EQ(K_FULL_CONSUMER_RECOMPOSE(data.members, data.proofs,
			(uint16)data.count, 7, MultiXactStatusForShare, result, 1, &count), CMX_RECOMPOSE_OK);
		UT_ASSERT_EQ(count, 1);
		if (count == 1) UT_ASSERT_EQ(result[0].xid, 7);
		finish(&data);
	}
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(original_two_and_256_member_controls);
	UT_RUN(full_terminal_lockers_allow_original_decision);
	UT_RUN(full_tail_committed_updater_is_not_a_locker_prefix);
	UT_RUN(full_unknown_tail_never_returns_positive_decision);
	UT_RUN(full_unknown_tail_clears_entire_recompose_output);
	UT_RUN(all_256_live_lockers_and_new_requester_are_preserved);
	UT_RUN(full_consumer_can_receive_65537_without_count_narrowing);
	UT_RUN(full_terminal_recompose_retires_every_locker);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
