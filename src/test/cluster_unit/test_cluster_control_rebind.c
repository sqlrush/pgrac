/* CONTROL rebind codecs and original serving-owner baseline capture. */
#include "postgres.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_relmap_arb.h"

/* Reuse the actual semantic engine and existing external-input fixture.
 * The cold-start driver's dependencies are not used by this binary. */
int control_rebind_original_fixture_main(void);
#define main control_rebind_original_fixture_main
#include "test_cluster_r4_activation_fsm.c"
#undef main

static ClusterSemanticControlRebindWireV3
rebind_message(bool ack)
{
	ClusterSemanticControlRebindWireV3 message = { 0 };
	message.kind = ack ? 2 : 1;
	message.stage = 5;
	message.result = ack ? 1 : 0;
	message.coordinator_node = 1;
	message.member_node = 3;
	message.transition_epoch = 7;
	message.record_generation = 6;
	message.round_nonce = 42;
	message.source_feature_bitmap = 1;
	message.target_feature_bitmap = 1025;
	message.admitted_members_lo = 15;
	message.capability_sample_digest = 0xabcd;
	message.boot_id = ack ? 0x103 : 0x101;
	message.admitted_incarnation = message.boot_id;
	message.capability_word = 0x0071b000;
	memset(message.proof_binding, 0x5a, sizeof(message.proof_binding));
	return message;
}

UT_TEST(rebind_codec_has_independent_exact_request_and_ack_bytes)
{
	for (unsigned ack = 0; ack < 2; ack++) {
		ClusterSemanticControlRebindWireV3 input = rebind_message(ack != 0), output;
		ClusterSemanticActivationAckWireV1 old_output;
		uint8 bytes[152];
		uint8 expected[152] = {
			[0] = 0x41, [1] = 0x43, [2] = 0x4b, [3] = 0x31, [4] = 3,
			[6] = 1, [7] = 5, [16] = 1, [20] = 3, [24] = 7, [32] = 6,
			[40] = 42, [48] = 1, [56] = 1, [57] = 4, [72] = 15,
			[88] = 0xcd, [89] = 0xab, [96] = 1, [97] = 1,
			[104] = 1, [105] = 1, [113] = 0xb0, [114] = 0x71
		};
		if (ack) {
			expected[6] = 2;
			expected[8] = 1;
			expected[96] = expected[104] = 3;
		}
		memset(expected + 120, 0x5a, 32);
		UT_ASSERT(cluster_semantic_control_rebind_wire_encode(&input, bytes));
		UT_ASSERT(memcmp(bytes, expected, sizeof(bytes)) == 0);
		UT_ASSERT(cluster_semantic_control_rebind_wire_decode(expected, &output));
		UT_ASSERT(memcmp(&input, &output, sizeof(input)) == 0);
		UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(expected, &old_output));
	}
}

UT_TEST(rebind_codec_rejects_invalid_identity_without_mutating_output)
{
	for (unsigned fault = 0; fault < 28; fault++) {
		ClusterSemanticControlRebindWireV3 message = rebind_message(false);
		uint8 bytes[152], saved[152];
		memset(bytes, 0xa5, sizeof(bytes));
		memcpy(saved, bytes, sizeof(saved));
		switch (fault) {
		case 0: message.kind = 0; break;
		case 1: message.stage = 4; break;
		case 2: message.result = 1; break;
		case 3: message.reason = 1; break;
		case 4: message.coordinator_node = 4; break;
		case 5: message.member_node = 4; break;
		case 6: message.member_node = message.coordinator_node; break;
		case 7: message.transition_epoch = 0; break;
		case 8: message.transition_epoch = UINT64_MAX; break;
		case 9: message.record_generation = 0; break;
		case 10: message.record_generation = 5; break;
		case 11: message.record_generation = UINT64_MAX; break;
		case 12: message.round_nonce = 0; break;
		case 13: message.round_nonce = UINT64_MAX; break;
		case 14: message.source_feature_bitmap = 0; break;
		case 15: message.target_feature_bitmap = 1; break;
		case 16: message.rollback_feature_bitmap = 1; break;
		case 17: message.admitted_members_lo = 14; break;
		case 18: message.admitted_members_hi = 1; break;
		case 19: message.capability_sample_digest = 0; break;
		case 20: message.boot_id = 0; break;
		case 21: message.boot_id = UINT64_MAX; break;
		case 22: message.admitted_incarnation = 0; break;
		case 23: message.admitted_incarnation = UINT64_MAX; break;
		case 24: message.capability_word = 1; break;
		case 25: memset(message.proof_binding, 0, sizeof(message.proof_binding)); break;
		case 26: message.kind = 2; message.result = 2; break;
		case 27: message.kind = 2; message.result = 0; break;
		}
		UT_ASSERT(!cluster_semantic_control_rebind_wire_encode(&message, bytes));
		UT_ASSERT(memcmp(bytes, saved, sizeof(bytes)) == 0);
	}
}

UT_TEST(rebind_decoder_rejects_wrong_version_reserved_and_identity)
{
	const unsigned offsets[] = { 0, 4, 6, 7, 8, 12, 16, 20, 24, 32, 40,
		48, 56, 64, 72, 80, 88, 96, 104, 112, 116, 117, 118, 119, 120 };
	for (unsigned fault = 0; fault < lengthof(offsets); fault++) {
		ClusterSemanticControlRebindWireV3 input = rebind_message(false), output, saved;
		uint8 bytes[152];
		UT_ASSERT(cluster_semantic_control_rebind_wire_encode(&input, bytes));
		memset(&output, 0xa5, sizeof(output));
		memcpy(&saved, &output, sizeof(saved));
		if (offsets[fault] == 120)
			memset(bytes + 120, 0, 32);
		else if (offsets[fault] == 24 || offsets[fault] == 32 || offsets[fault] == 40
				 || offsets[fault] == 88 || offsets[fault] == 96 || offsets[fault] == 104)
			memset(bytes + offsets[fault], 0, 8);
		else if (offsets[fault] == 112)
			memset(bytes + 112, 0, 4);
		else
			bytes[offsets[fault]] ^= 0x80;
		UT_ASSERT(!cluster_semantic_control_rebind_wire_decode(bytes, &output));
		UT_ASSERT(memcmp(&output, &saved, sizeof(output)) == 0);
	}
	UT_ASSERT(!cluster_semantic_control_rebind_wire_decode(NULL, NULL));
	for (unsigned version = 2; version <= 4; version += 2) {
		ClusterSemanticControlRebindWireV3 input = rebind_message(false), output;
		uint8 bytes[152];
		UT_ASSERT(cluster_semantic_control_rebind_wire_encode(&input, bytes));
		bytes[4] = version;
		UT_ASSERT(!cluster_semantic_control_rebind_wire_decode(bytes, &output));
	}
}

static void
rebind_setup(void)
{
	memset(&semantic_control_rebind_baseline, 0, sizeof(semantic_control_rebind_baseline));
	test_serving_setup(false);
}

UT_TEST(rebind_baseline_requires_original_root_owner_completion)
{
	rebind_setup();
	UT_ASSERT(!semantic_control_rebind_baseline.valid);
	test_serving_tick();
	UT_ASSERT(!semantic_control_rebind_baseline.valid);
	test_serving_complete_read();
	test_serving_result = CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	test_serving_tick();
	UT_ASSERT(!semantic_control_rebind_baseline.valid);
	test_serving_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	test_serving_tick();
	UT_ASSERT(semantic_control_rebind_baseline.valid);
	UT_ASSERT_EQ(semantic_control_rebind_baseline.cut.system_identifier, test_system_identifier);
	UT_ASSERT(memcmp(semantic_control_rebind_baseline.pgsa,
		SemanticActivationShmem->record_cas_desired_bytes, 512) == 0);
	UT_ASSERT(memcmp(semantic_control_rebind_baseline.root, test_serving_expected_root, 512) == 0);
	UT_ASSERT(memcmp(&semantic_control_rebind_baseline.table, SemanticActivationAckTable,
		sizeof(*SemanticActivationAckTable)) == 0);
	test_first_open_finish();
}

static void
rebind_change_generation_during_root_poll(void)
{
	test_peer_capability_generation++;
}

UT_TEST(rebind_baseline_rejects_drift_and_recovery_only_carriers)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		rebind_setup();
		test_serving_tick();
		test_serving_complete_read();
		test_serving_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
		if (fault == 0)
			SemanticActivationAckTable->flags |= CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_OPEN_PROOF;
		if (fault == 1)
			test_serving_token.system_identifier++;
		if (fault == 2)
			test_serving_hook = rebind_change_generation_during_root_poll;
		if (fault == 3) {
			ClusterUndoRootDescriptorV1 descriptor;
			uint8 changed[512];
			UT_ASSERT_EQ(cluster_undo_root_descriptor_decode(test_serving_expected_root,
				test_system_identifier, &descriptor), CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID);
			descriptor.root_uuid[0] ^= 1;
			UT_ASSERT(cluster_undo_root_descriptor_encode(&descriptor, changed));
			UT_ASSERT(semantic_activation_pgrd_snapshot_publish(changed));
		}
		test_serving_tick();
		UT_ASSERT(!semantic_control_rebind_baseline.valid);
		test_first_open_finish();
	}
}

UT_TEST(rebind_baseline_does_not_refresh_the_original_ack_or_admit_reconnect)
{
	SemanticControlRebindBaseline saved;
	ClusterSemanticActivationAckTableV1 table;
	SemanticStopTrace trace = { 0 };
	rebind_setup();
	test_serving_finish_root();
	UT_ASSERT(semantic_control_rebind_baseline.valid);
	memcpy(&saved, &semantic_control_rebind_baseline, sizeof(saved));
	memcpy(&table, SemanticActivationAckTable, sizeof(table));
	test_peer_capability_generation++;
	IsUnderPostmaster = true;
	UT_ASSERT_EQ(semantic_normal_stop_match_impl(&test_serving_expected_open,
		test_serving_expected_root, NULL, NULL, &trace), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_STR_EQ(trace.predicate, "ack.peer.capability_generation");
	test_serving_tick();
	UT_ASSERT(memcmp(&table, SemanticActivationAckTable, sizeof(table)) == 0);
	UT_ASSERT(memcmp(&saved, &semantic_control_rebind_baseline, sizeof(saved)) == 0);
	printf("# baseline resident bytes=%zu reserved-with-two-tables=%zu\n",
		sizeof(semantic_control_rebind_baseline) + sizeof(semantic_serving.pgsa),
		sizeof(semantic_control_rebind_baseline) + sizeof(semantic_serving.pgsa)
			+ 2 * sizeof(ClusterSemanticActivationAckTableV1));
	test_first_open_finish();
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(rebind_codec_has_independent_exact_request_and_ack_bytes);
	UT_RUN(rebind_codec_rejects_invalid_identity_without_mutating_output);
	UT_RUN(rebind_decoder_rejects_wrong_version_reserved_and_identity);
	UT_RUN(rebind_baseline_requires_original_root_owner_completion);
	UT_RUN(rebind_baseline_rejects_drift_and_recovery_only_carriers);
	UT_RUN(rebind_baseline_does_not_refresh_the_original_ack_or_admit_reconnect);
	UT_RUN(test_stop_identity_trace_names_nested_peer_generation);
	UT_RUN(test_145k_restore_open_proof_does_not_overwrite_resource_x_carrier);
	UT_RUN(test_serving_owner_waits_for_exact_root_release_and_frees_mailbox);
	UT_RUN(test_serving_rechecks_identity_after_root_poll);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
