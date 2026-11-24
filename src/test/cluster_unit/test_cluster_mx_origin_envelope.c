/* Exercise the original GCS exact-operation continuation and reply codec.
 * The route fixture controls TT/current/transport leaves; this is not a
 * retained-origin storage or native receipt-admission test. */
int route_fixture_main(int, char **);
#define main route_fixture_main
#include "test_cluster_r4_route_policy.c"
#undef main

static void
exact_origin_envelope(bool updater, ClusterTTStatus status, uint8 effect)
{
	ClusterCurrentMxOperationForwardV6 request;
	ClusterCurrentMxOperationAsk ask;
	ClusterCurrentMemberOperationProof proofs[7];
	ClusterCurrentUpdaterProof updater_proof;
	ClusterMxResolveResult result;
	ClusterICEnvelope env;
	uint16 count = 0;
	uint32 capability = 0;
	int saved = cluster_node_id;
	bool decoded;

	cluster_node_id = UT_MASTER_NODE;
	mx_exact_request(&request, &env, updater);
	mx_origin_sample_status = status;
	mx_origin_physical_active = status == CLUSTER_TT_STATUS_IN_PROGRESS;
	mx_exact_final_effect = effect;
	UT_ASSERT(cluster_multixact_operation_wire_get_ask(&request, 0, &ask));
	UT_ASSERT(cluster_gcs_block_test_current_mx_forward128(&env, &request));
	mx_exact_drain();
	decoded = cluster_multixact_operation_wire_validate_reply(route_seam.raw_send_page,
		BLCKSZ, UT_MASTER_NODE, UT_FORMATION_EPOCH, &request, &result,
		proofs, lengthof(proofs), &count, &updater_proof, &capability);
	UT_ASSERT(decoded);
	if (decoded) {
		UT_ASSERT_EQ(result, CMX_RESOLVE_OK);
		UT_ASSERT_EQ(count, 1);
		if (count == 1) {
			const uint8 *bytes = proofs[0].base.origin_fullxid_v2;
			static const uint8 empty[CLUSTER_CTRC_ORIGIN_FULLXID_V2_BYTES];

			UT_ASSERT_EQ(proofs[0].flags, CLUSTER_CURRENT_OPERATION_PROOF_EXACT);
			UT_ASSERT_EQ(proofs[0].effect_state, effect);
			UT_ASSERT_EQ(U64FromFullTransactionId(proofs[0].top_fxid),
				U64FromFullTransactionId(ask.operation.top_fxid));
			if (status == CLUSTER_TT_STATUS_IN_PROGRESS
				&& effect == CLUSTER_SAVEPOINT_EFFECT_LIVE) {
				/* A grant without the original envelope is not a usable result. */
				UT_ASSERT_EQ(proofs[0].base.state, CCM_ACTIVE);
				UT_ASSERT_EQ(ClusterCurrentMemberProofGetCtrcGrant(&proofs[0].base), 71);
				UT_ASSERT(memcmp(bytes, empty, sizeof(empty)) != 0);
				UT_ASSERT_EQ(ClusterR4WireReadU16(bytes),
					CLUSTER_CTRC_ORIGIN_FULLXID_V2_VERSION);
				UT_ASSERT_EQ(ClusterR4WireReadU16(bytes + 2), sizeof(empty));
				UT_ASSERT_EQ(ClusterR4WireReadU32(bytes + 4), 71);
				UT_ASSERT_EQ(ClusterR4WireReadU64(bytes + 8),
					U64FromFullTransactionId(ask.operation.top_fxid));
				UT_ASSERT_EQ(ClusterR4WireReadU16(bytes + 112), UT_REQUESTER_NODE);
				UT_ASSERT_EQ(ClusterR4WireReadU16(bytes + 114), 0);
				UT_ASSERT_EQ(ClusterR4WireReadU32(bytes + 116),
					UT_CURRENT_MX_CAPABILITY_GENERATION);
				UT_ASSERT_EQ(ClusterR4WireReadU64(bytes + 128), UT_FORMATION_EPOCH);
			} else {
				UT_ASSERT_EQ(ClusterCurrentMemberProofGetCtrcGrant(&proofs[0].base), 0);
				UT_ASSERT_EQ(memcmp(bytes, empty, sizeof(empty)), 0);
			}
		}
	}
	UT_ASSERT_EQ(mx_origin_guard_held, 0);
	UT_ASSERT_EQ(route_seam.candidate_acquire_begin_calls, updater ? 6 : 3);
	UT_ASSERT_EQ(route_seam.candidate_release_begin_calls, updater ? 6 : 3);
	mx_origin_sample_enabled = mx_exact_enabled = false;
	origin_generation_sample_enabled = false;
	cluster_node_id = saved;
}

UT_TEST(exact_active_live_carries_origin_envelope)
{
	exact_origin_envelope(false, CLUSTER_TT_STATUS_IN_PROGRESS, CLUSTER_SAVEPOINT_EFFECT_LIVE);
}

UT_TEST(exact_active_live_with_independent_B_carries_origin_envelope)
{
	exact_origin_envelope(true, CLUSTER_TT_STATUS_IN_PROGRESS, CLUSTER_SAVEPOINT_EFFECT_LIVE);
}

UT_TEST(exact_discarded_has_no_origin_envelope)
{
	exact_origin_envelope(false, CLUSTER_TT_STATUS_IN_PROGRESS,
		CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
}

UT_TEST(exact_committed_has_no_origin_envelope)
{
	exact_origin_envelope(false, CLUSTER_TT_STATUS_COMMITTED, CLUSTER_SAVEPOINT_EFFECT_LIVE);
}

int
main(int argc pg_attribute_unused(), char **argv pg_attribute_unused())
{
	UT_PLAN(4);
	UT_RUN(exact_active_live_carries_origin_envelope);
	UT_RUN(exact_active_live_with_independent_B_carries_origin_envelope);
	UT_RUN(exact_discarded_has_no_origin_envelope);
	UT_RUN(exact_committed_has_no_origin_envelope);
	UT_DONE();
	return ut_failed_count != 0;
}
