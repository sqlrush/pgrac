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
	test_terminal_peer_record_enabled = false;
	test_terminal_peer_eligible = false;
	test_source_close_found = false;
	test_serving_setup(false);
	memset(&semantic_control_rebind, 0, sizeof(semantic_control_rebind));
}

UT_TEST(rebind_baseline_requires_original_root_owner_completion)
{
	rebind_setup();
	UT_ASSERT(!SemanticControlRebindState->baseline.valid);
	test_serving_tick();
	UT_ASSERT(!SemanticControlRebindState->baseline.valid);
	test_serving_complete_read();
	test_serving_result = CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	test_serving_tick();
	UT_ASSERT(!SemanticControlRebindState->baseline.valid);
	test_serving_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	test_serving_tick();
	UT_ASSERT(SemanticControlRebindState->baseline.valid);
	UT_ASSERT_EQ(SemanticControlRebindState->baseline.cut.system_identifier, test_system_identifier);
	UT_ASSERT(memcmp(SemanticControlRebindState->baseline.pgsa,
		SemanticActivationShmem->record_cas_desired_bytes, 512) == 0);
	UT_ASSERT(memcmp(SemanticControlRebindState->baseline.root, test_serving_expected_root, 512) == 0);
	UT_ASSERT(memcmp(&SemanticControlRebindState->baseline.table, SemanticActivationAckTable,
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
		UT_ASSERT(!SemanticControlRebindState->baseline.valid);
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
	UT_ASSERT(SemanticControlRebindState->baseline.valid);
	memcpy(&saved, &SemanticControlRebindState->baseline, sizeof(saved));
	memcpy(&table, SemanticActivationAckTable, sizeof(table));
	test_peer_capability_generation++;
	IsUnderPostmaster = true;
	UT_ASSERT_EQ(semantic_normal_stop_match_impl(&test_serving_expected_open,
		test_serving_expected_root, NULL, NULL, &trace), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_STR_EQ(trace.predicate, "ack.peer.capability_generation");
	test_serving_tick();
	UT_ASSERT(memcmp(&table, SemanticActivationAckTable, sizeof(table)) == 0);
	UT_ASSERT(memcmp(&saved, &SemanticControlRebindState->baseline, sizeof(saved)) == 0);
	printf(
		"# rebind resident shared=%zu owner=%zu pointer=%zu raw-read=%zu total=%zu budget=65536\n",
		MAXALIGN(sizeof(SemanticControlRebindShmem)), sizeof(SemanticControlRebindRuntime),
		sizeof(SemanticControlRebindState), sizeof(semantic_serving.pgsa),
		MAXALIGN(sizeof(SemanticControlRebindShmem)) + sizeof(SemanticControlRebindRuntime)
			+ sizeof(SemanticControlRebindState) + sizeof(semantic_serving.pgsa));
	test_first_open_finish();
}

static void
rebind_tick(TimestampTz now)
{
	IsUnderPostmaster = true;
	MyAuxProcType = LmonProcess;
	(void)semantic_control_rebind_tick(now);
	MyAuxProcType = NotAnAuxProcess;
	IsUnderPostmaster = false;
}

static void
rebind_start(void)
{
	rebind_setup();
	test_serving_finish_root();
	UT_ASSERT(SemanticControlRebindState->baseline.valid);
	test_peer_capability_generation++;
	for (unsigned peer = 0; peer < 4; peer++)
		test_rebind_stream_serial[peer] += 100;
	rebind_tick(1000000);
	UT_ASSERT(semantic_control_rebind.read_seq != 0);
}

static void
rebind_complete_read(void)
{
	uint64 seq = semantic_control_rebind.read_seq;
	UT_ASSERT(seq != 0);
	UT_ASSERT(cluster_semantic_activation_record_encode(
		&test_serving_expected_open, SemanticActivationShmem->record_cas_desired_bytes));
	SemanticActivationShmem->record_cas_expected_source_feature_bitmap = 0;
	UT_ASSERT(semantic_activation_authority_mailbox_complete(
		CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_READ, seq, CLUSTER_SEMANTIC_ACTIVATION_OK));
}

static ClusterSemanticControlRebindWireV3
rebind_ack(unsigned peer)
{
	ClusterSemanticControlRebindWireV3 message;
	UT_ASSERT(cluster_semantic_control_rebind_wire_decode(test_send_payloads[peer], &message));
	message.kind = 2;
	message.result = 1;
	message.boot_id = test_remote_admitted_incarnations[peer];
	message.admitted_incarnation = message.boot_id;
	message.capability_word = test_peer_capability_word;
	return message;
}

static bool
rebind_receive(const ClusterSemanticControlRebindWireV3 *message, unsigned peer, unsigned fault)
{
	uint8 bytes[152];
	ClusterICEnvelope env = { 0 };
	ClusterICTier1Stream stream;
	bool accepted;
	UT_ASSERT(cluster_semantic_control_rebind_wire_encode(message, bytes));
	env.msg_type = PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1;
	env.source_node_id = peer;
	env.dest_node_id = cluster_node_id;
	env.payload_length = 152;
	env.epoch = test_current_epoch;
	UT_ASSERT(cluster_ic_tier1_stream_capture(peer, &stream));
	memset(&test_rebind_dispatch_ingress, 0, sizeof(test_rebind_dispatch_ingress));
	test_rebind_dispatch_ingress.serial = stream.serial;
	test_rebind_dispatch_ingress.epoch = stream.epoch;
	test_rebind_dispatch_ingress.owner_pid = stream.owner_pid;
	test_rebind_dispatch_ingress.source_node = peer;
	test_rebind_dispatch_ingress.carrier = CLUSTER_IC_CONTROL_TCP;
	if (fault == 1)
		test_rebind_dispatch_ingress.serial--;
	if (fault == 2)
		test_rebind_dispatch_ingress.owner_pid++;
	if (fault == 3)
		test_rebind_dispatch_ingress.carrier = CLUSTER_IC_CONTROL_UNKNOWN;
	if (fault == 4)
		env.source_node_id = cluster_node_id;
	test_rebind_dispatch_envelope = fault == 5 ? NULL : &env;
	IsUnderPostmaster = true;
	MyAuxProcType = LmonProcess;
	accepted = semantic_control_rebind_ingress(&env, bytes);
	MyAuxProcType = NotAnAuxProcess;
	IsUnderPostmaster = false;
	test_rebind_dispatch_envelope = NULL;
	return accepted;
}

UT_TEST(rebind_collects_only_three_actual_current_stream_acks)
{
	ClusterSemanticActivationAckTableV1 ordinary;
	rebind_start();
	memcpy(&ordinary, SemanticActivationAckTable, sizeof(ordinary));
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticControlRebindState->completion_seq), 0);
	rebind_complete_read();
	rebind_tick(1000001);
	UT_ASSERT_EQ(semantic_control_rebind.table.observed_members_lo, 1);
	UT_ASSERT_EQ(semantic_control_rebind.phase, SEMANTIC_CONTROL_REBIND_COLLECTING);
	for (unsigned step = 0; step < 3; step++) {
		const unsigned peers[] = { 3, 1, 2 };
		unsigned peer = peers[step];
		ClusterSemanticControlRebindWireV3 ack = rebind_ack(peer);
		UT_ASSERT_EQ(test_send_calls[peer], 1);
		UT_ASSERT(rebind_receive(&ack, peer, 0));
		UT_ASSERT(rebind_receive(&ack, peer, 0));
		rebind_tick(1000002 + step);
		if (step < 2)
			UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticControlRebindState->completion_seq), 0);
	}
	UT_ASSERT_EQ(semantic_control_rebind.phase, SEMANTIC_CONTROL_REBIND_COMPLETE);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticControlRebindState->completion_seq), 2);
	UT_ASSERT_EQ(SemanticControlRebindState->completion.table.observed_members_lo, 15);
	UT_ASSERT_EQ(SemanticControlRebindState->completion.table.expected[3].capability_generation,
				 20);
	UT_ASSERT(memcmp(&ordinary, SemanticActivationAckTable, sizeof(ordinary)) == 0);
	UT_ASSERT_EQ(SemanticControlRebindState->baseline.table.expected[3].capability_generation, 19);
	test_first_open_finish();
}

UT_TEST(rebind_refuses_forged_stale_and_wrong_round_ack)
{
	for (unsigned fault = 1; fault <= 10; fault++) {
		ClusterSemanticControlRebindWireV3 ack;
		rebind_start();
		rebind_complete_read();
		rebind_tick(1000001);
		ack = rebind_ack(3);
		if (fault == 6)
			ack.round_nonce++;
		if (fault == 7)
			ack.proof_binding[0] ^= 1;
		if (fault == 8)
			ack.boot_id = ack.admitted_incarnation = 0x999;
		if (fault == 9)
			ack.capability_word ^= 1;
		if (fault == 10)
			ack.member_node = 2;
		(void)rebind_receive(&ack, 3, fault);
		rebind_tick(1000002);
		UT_ASSERT_EQ(semantic_control_rebind.table.observed_members_lo, 1);
		UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticControlRebindState->completion_seq), 0);
		test_first_open_finish();
	}
}

UT_TEST(rebind_missing_acks_and_backpressure_use_original_heartbeat)
{
	ClusterSemanticControlRebindWireV3 ack;
	rebind_start();
	test_send_results[3] = CLUSTER_IC_SEND_NOT_ADMITTED;
	rebind_complete_read();
	rebind_tick(1000001);
	ack = rebind_ack(1);
	UT_ASSERT(rebind_receive(&ack, 1, 0));
	rebind_tick(1000002);
	rebind_tick(1999999);
	UT_ASSERT_EQ(test_send_calls[1], 1);
	UT_ASSERT_EQ(test_send_calls[2], 1);
	UT_ASSERT_EQ(test_send_calls[3], 1);
	rebind_tick(2000001);
	UT_ASSERT_EQ(test_send_calls[1], 1);
	UT_ASSERT_EQ(test_send_calls[2], 2);
	UT_ASSERT_EQ(test_send_calls[3], 2);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticControlRebindState->completion_seq), 0);
	UT_ASSERT_EQ(semantic_control_rebind.table.observed_members_lo, 3);
	test_first_open_finish();
}

UT_TEST(rebind_read_result_cannot_be_stolen_or_reused_after_new_hello)
{
	uint64 old_seq, old_nonce;
	rebind_start();
	old_seq = semantic_control_rebind.read_seq;
	old_nonce = semantic_control_rebind.nonce;
	rebind_complete_read();
	UT_ASSERT(semantic_activation_authority_mailbox_owner(true) >= 0);
	test_peer_capability_generation++;
	for (unsigned peer = 0; peer < 4; peer++)
		test_rebind_stream_serial[peer]++;
	rebind_tick(1000001);
	UT_ASSERT(semantic_control_rebind.read_seq > old_seq);
	UT_ASSERT(semantic_control_rebind.nonce > old_nonce);
	UT_ASSERT_EQ(test_send_calls[3], 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticControlRebindState->completion_seq), 0);
	test_first_open_finish();
}

UT_TEST(rebind_rejects_changed_nontransport_identity)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		rebind_start();
		rebind_complete_read();
		if (fault == 0)
			test_remote_admitted_incarnations[3]++;
		if (fault == 1)
			test_peer_capability_word ^= 1;
		if (fault == 2)
			test_serving_formation++;
		if (fault == 3)
			test_system_identifier++;
		if (fault == 4)
			test_gate_publish(6, 1, 6, 7, false);
		if (fault == 5)
			test_terminal_stop = true;
		rebind_tick(1000001);
		UT_ASSERT_EQ(test_send_calls[3], 0);
		UT_ASSERT_EQ(semantic_control_rebind.read_seq, 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticControlRebindState->completion_seq), 0);
		test_first_open_finish();
	}
}

UT_TEST(rebind_successor_never_inherits_private_serial_or_nonce)
{
	uint64 old_nonce, baseline_seq;
	rebind_start();
	rebind_complete_read();
	rebind_tick(1000001);
	old_nonce = semantic_control_rebind.nonce;
	baseline_seq = pg_atomic_read_u64(&SemanticControlRebindState->baseline_seq);
	memset(&semantic_control_rebind, 0, sizeof(semantic_control_rebind));
	MyProcPid++;
	MyProc->cluster_grd_generation++;
	rebind_tick(2000000);
	UT_ASSERT(semantic_control_rebind.nonce > old_nonce);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticControlRebindState->baseline_seq), baseline_seq);
	UT_ASSERT_EQ(semantic_control_rebind.table.observed_members_lo, 1);
	UT_ASSERT_EQ(semantic_control_rebind.phase, SEMANTIC_CONTROL_REBIND_READING);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticControlRebindState->completion_seq), 0);
	test_first_open_finish();
}

UT_TEST(rebind_exhaustion_never_wraps_or_completes)
{
	rebind_setup();
	test_serving_finish_root();
	pg_atomic_write_u64(&SemanticControlRebindState->nonce, UINT64_MAX - 1);
	test_peer_capability_generation++;
	for (unsigned peer = 0; peer < 4; peer++)
		test_rebind_stream_serial[peer]++;
	rebind_tick(1000000);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticControlRebindState->nonce), UINT64_MAX - 1);
	UT_ASSERT_EQ(semantic_control_rebind.read_seq, 0);
	UT_ASSERT_EQ(test_send_calls[3], 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticControlRebindState->completion_seq), 0);
	test_first_open_finish();
}

UT_TEST(rebind_unconsumed_read_survives_loss_of_private_owner)
{
	uint64 seq;
	rebind_start();
	rebind_complete_read();
	seq = semantic_control_rebind.read_seq;
	memset(&semantic_control_rebind, 0, sizeof(semantic_control_rebind));
	MyProcPid++;
	MyProc->cluster_grd_generation++;
	rebind_tick(2000000);
	UT_ASSERT(semantic_activation_authority_mailbox_owner(true) >= 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->record_cas_request_seq), seq);
	UT_ASSERT_EQ(semantic_control_rebind.read_seq, 0);
	UT_ASSERT_EQ(test_send_calls[3], 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticControlRebindState->completion_seq), 0);
	test_first_open_finish();
}

UT_TEST(rebind_reply_needs_no_local_collection_completion)
{
	ClusterSemanticControlRebindWireV3 request, reply;
	const ClusterSemanticActivationAckTableV1 *base;
	rebind_setup();
	test_serving_finish_root();
	request = rebind_message(false);
	base = &SemanticControlRebindState->baseline.table;
	request.coordinator_node = 3;
	request.member_node = 0;
	request.round_nonce = 999;
	request.boot_id = request.admitted_incarnation = base->expected[3].boot_id;
	request.transition_epoch = base->transition_epoch;
	request.record_generation = base->record_generation;
	request.source_feature_bitmap = base->source_feature_bitmap;
	request.target_feature_bitmap = base->target_feature_bitmap;
	request.capability_sample_digest = base->capability_sample_digest;
	request.capability_word = base->expected[3].capability_word;
	UT_ASSERT(semantic_control_rebind_binding(3, 999, request.proof_binding));
	UT_ASSERT(rebind_receive(&request, 3, 0));
	rebind_tick(1000000);
	rebind_complete_read();
	rebind_tick(1000001);
	UT_ASSERT_EQ(test_send_calls[3], 1);
	UT_ASSERT_EQ(test_send_calls[1], 0);
	UT_ASSERT(cluster_semantic_control_rebind_wire_decode(test_send_payloads[3], &reply));
	UT_ASSERT_EQ(reply.kind, 2);
	UT_ASSERT_EQ(reply.coordinator_node, 3);
	UT_ASSERT_EQ(reply.member_node, 0);
	UT_ASSERT_EQ(reply.round_nonce, 999);
	UT_ASSERT_EQ(reply.boot_id, UINT64_C(0x445566778899aabb));
	UT_ASSERT(memcmp(reply.proof_binding, request.proof_binding, 32) == 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticControlRebindState->completion_seq), 0);
	UT_ASSERT(rebind_receive(&request, 3, 0));
	rebind_tick(2000001);
	UT_ASSERT_EQ(test_send_calls[3], 2);
	request.round_nonce--;
	UT_ASSERT(semantic_control_rebind_binding(3, 998, request.proof_binding));
	UT_ASSERT(!rebind_receive(&request, 3, 0));
	test_first_open_finish();
}

UT_TEST(rebind_retained_reply_backpressure_keeps_exact_owner)
{
	ClusterSemanticControlRebindWireV3 request, response;
	rebind_start();
	rebind_complete_read();
	rebind_tick(1000001);
	request = rebind_ack(3);
	request.kind = 1;
	request.result = 0;
	request.coordinator_node = 3;
	request.member_node = 0;
	request.round_nonce = 777;
	UT_ASSERT(semantic_control_rebind_binding(3, 777, request.proof_binding));
	test_send_results[3] = CLUSTER_IC_SEND_NOT_ADMITTED;
	UT_ASSERT(rebind_receive(&request, 3, 0));
	rebind_tick(1100000);
	UT_ASSERT(semantic_control_rebind.requests[3].valid);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticControlRebindState->completion_seq), 0);
	test_send_results[3] = CLUSTER_IC_SEND_WOULD_BLOCK;
	rebind_tick(2100000);
	UT_ASSERT(!semantic_control_rebind.requests[3].valid);
	UT_ASSERT(
		cluster_semantic_control_rebind_wire_decode(test_send_history_payloads[3][2], &response));
	UT_ASSERT_EQ(response.kind, 2);
	UT_ASSERT_EQ(response.round_nonce, 777);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticControlRebindState->completion_seq), 0);
	test_first_open_finish();
}

UT_TEST(rebind_new_hello_after_partial_ack_retires_old_nonce)
{
	ClusterSemanticControlRebindWireV3 old_ack;
	uint64 old_nonce;
	rebind_start();
	rebind_complete_read();
	rebind_tick(1000001);
	old_ack = rebind_ack(3);
	UT_ASSERT(rebind_receive(&old_ack, 3, 0));
	rebind_tick(1000002);
	old_nonce = semantic_control_rebind.nonce;
	test_peer_capability_generation++;
	for (unsigned peer = 0; peer < 4; peer++)
		test_rebind_stream_serial[peer]++;
	rebind_tick(1000003);
	UT_ASSERT(semantic_control_rebind.nonce > old_nonce);
	UT_ASSERT(!rebind_receive(&old_ack, 3, 0));
	UT_ASSERT_EQ(semantic_control_rebind.table.observed_members_lo, 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticControlRebindState->completion_seq), 0);
	test_first_open_finish();
}

UT_TEST(rebind_new_generation_requires_actual_old_stream_close)
{
	rebind_setup();
	test_serving_finish_root();
	test_peer_capability_generation++;
	rebind_tick(1000000);
	UT_ASSERT_EQ(semantic_control_rebind.read_seq, 0);
	UT_ASSERT_EQ(test_send_calls[3], 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticControlRebindState->completion_seq), 0);
	test_first_open_finish();
}

UT_TEST(rebind_one_reconnected_peer_keeps_other_generation_domains)
{
	const unsigned peers[] = { 1, 2, 3 };
	rebind_setup();
	test_serving_finish_root();
	test_terminal_peer_record_enabled = true;
	test_terminal_peer_record.valid = true;
	test_terminal_peer_record.bits = test_peer_capability_word;
	test_terminal_peer_record.generation = 20;
	test_rebind_stream_serial[3]++;
	rebind_tick(1000000);
	rebind_complete_read();
	rebind_tick(1000001);
	for (unsigned i = 0; i < 3; i++) {
		ClusterSemanticControlRebindWireV3 ack = rebind_ack(peers[i]);
		UT_ASSERT(rebind_receive(&ack, peers[i], 0));
	}
	rebind_tick(1000002);
	UT_ASSERT_EQ(semantic_control_rebind.phase, SEMANTIC_CONTROL_REBIND_COMPLETE);
	UT_ASSERT_EQ(SemanticControlRebindState->completion.table.expected[1].capability_generation,
				 19);
	UT_ASSERT_EQ(SemanticControlRebindState->completion.table.expected[2].capability_generation,
				 19);
	UT_ASSERT_EQ(SemanticControlRebindState->completion.table.expected[3].capability_generation,
				 20);
	UT_ASSERT_EQ(SemanticControlRebindState->baseline.table.expected[3].capability_generation, 19);
	test_first_open_finish();
}

UT_TEST(rebind_shared_publication_survives_attaching_without_reset)
{
	uint64 nonce, seq;
	rebind_start();
	nonce = pg_atomic_read_u64(&SemanticControlRebindState->nonce);
	seq = semantic_control_rebind.read_seq;
	test_rebind_found = true;
	test_serving_found = true;
	test_normal_start_found = true;
	test_shmem_found = test_utility_mailbox_found = test_ack_table_found = true;
	test_pgrd_snapshot_found = test_bit22_latch_found = test_bit22_seam_found = true;
	test_source_close_found = true;
	cluster_semantic_activation_shmem_init();
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticControlRebindState->nonce), nonce);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticControlRebindState->baseline_seq), 2);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticControlRebindState->read_seq), seq);
	UT_ASSERT(SemanticControlRebindState->baseline.valid);
	UT_ASSERT(semantic_activation_authority_mailbox_owner(true) >= 0);
	test_first_open_finish();
}

UT_TEST(rebind_invalid_durable_read_never_sends_or_completes)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		rebind_start();
		rebind_complete_read();
		if (fault == 0)
			SemanticActivationShmem->record_cas_expected_source_feature_bitmap = 1;
		if (fault == 1)
			SemanticActivationShmem->record_cas_desired_bytes[112] ^= 1;
		if (fault == 2)
			pg_atomic_write_u32(&SemanticActivationShmem->record_cas_result,
								CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
		rebind_tick(1000001);
		UT_ASSERT_EQ(semantic_control_rebind.phase, SEMANTIC_CONTROL_REBIND_INVALID);
		UT_ASSERT_EQ(semantic_control_rebind.read_seq, 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticControlRebindState->read_seq), 0);
		UT_ASSERT_EQ(test_send_calls[3], 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticControlRebindState->completion_seq), 0);
		test_first_open_finish();
	}
}

UT_TEST(rebind_completion_sequence_exhaustion_is_closed)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		uint64 seq;
		rebind_start();
		rebind_complete_read();
		rebind_tick(1000001);
		for (unsigned peer = 1; peer < 4; peer++) {
			ClusterSemanticControlRebindWireV3 ack = rebind_ack(peer);
			UT_ASSERT(rebind_receive(&ack, peer, 0));
		}
		seq = fault == 0 ? UINT64_MAX - 1 : 1;
		pg_atomic_write_u64(&SemanticControlRebindState->completion_seq, seq);
		rebind_tick(1000002);
		UT_ASSERT(semantic_control_rebind.phase != SEMANTIC_CONTROL_REBIND_COMPLETE);
		UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticControlRebindState->completion_seq), seq);
		UT_ASSERT_EQ(SemanticControlRebindState->completion.nonce, 0);
		test_first_open_finish();
	}
}

int
main(void)
{
	UT_PLAN(26);
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
 UT_RUN(rebind_collects_only_three_actual_current_stream_acks);
 UT_RUN(rebind_refuses_forged_stale_and_wrong_round_ack);
 UT_RUN(rebind_missing_acks_and_backpressure_use_original_heartbeat);
 UT_RUN(rebind_read_result_cannot_be_stolen_or_reused_after_new_hello);
 UT_RUN(rebind_rejects_changed_nontransport_identity);
 UT_RUN(rebind_successor_never_inherits_private_serial_or_nonce);
 UT_RUN(rebind_exhaustion_never_wraps_or_completes);
 UT_RUN(rebind_unconsumed_read_survives_loss_of_private_owner);
 UT_RUN(rebind_reply_needs_no_local_collection_completion);
 UT_RUN(rebind_retained_reply_backpressure_keeps_exact_owner);
 UT_RUN(rebind_new_hello_after_partial_ack_retires_old_nonce);
 UT_RUN(rebind_new_generation_requires_actual_old_stream_close);
 UT_RUN(rebind_one_reconnected_peer_keeps_other_generation_domains);
 UT_RUN(rebind_shared_publication_survives_attaching_without_reset);
 UT_RUN(rebind_invalid_durable_read_never_sends_or_completes);
 UT_RUN(rebind_completion_sequence_exhaustion_is_closed);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
