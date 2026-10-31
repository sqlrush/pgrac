/* CONTROL rebind codecs and original serving-owner baseline capture. */
#include "postgres.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_relmap_arb.h"
#include "common/cryptohash.h"

static unsigned rebind_owned_hash_calls;
static pg_cryptohash_ctx *rebind_counted_hash_create(pg_cryptohash_type type);

/* Reuse the actual semantic engine and existing external-input fixture.
 * The cold-start driver's dependencies are not used by this binary. */
int control_rebind_original_fixture_main(void);
#define main control_rebind_original_fixture_main
#define pg_cryptohash_create rebind_counted_hash_create
#include "test_cluster_r4_activation_fsm.c"
#undef pg_cryptohash_create
#undef main

/* Count the real ResourceOwner-dependent provider boundary; retain the
 * actual configured hash provider and its real output. */
static pg_cryptohash_ctx *
rebind_counted_hash_create(pg_cryptohash_type type)
{
	rebind_owned_hash_calls++;
	return pg_cryptohash_create(type);
}

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
	ClusterSemanticActivationAckTableV1 ordinary;
	test_terminal_peer_record_enabled = false;
	test_terminal_peer_eligible = false;
	test_source_close_found = false;
	test_serving_setup(false);
	/* The original stop helper initializes a table in place (seq0). Real
	 * ordinary OPEN is published; retain that actual publication domain. */
	ordinary = *SemanticActivationAckTable;
	UT_ASSERT(semantic_activation_ack_table_publish(&ordinary));
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
		"# rebind resident shared=%zu owner=%zu pointer=%zu raw-read=%zu serving-proof=%zu total=%zu budget=65536\n",
		MAXALIGN(sizeof(SemanticControlRebindShmem)), sizeof(SemanticControlRebindRuntime),
		sizeof(SemanticControlRebindState), sizeof(semantic_serving.pgsa),
		2 * sizeof(ClusterSemanticOpenProofIdentity),
		MAXALIGN(sizeof(SemanticControlRebindShmem)) + sizeof(SemanticControlRebindRuntime)
			+ sizeof(SemanticControlRebindState) + sizeof(semantic_serving.pgsa)
			+ 2 * sizeof(ClusterSemanticOpenProofIdentity));
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
	if (fault == 6) {
		cluster_semantic_activation_ack_handler(&env, bytes);
		accepted = semantic_control_rebind.acks[peer].valid;
	} else
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
			test_terminal_prebump = true; /* Stop itself permits CONTROL draining. */
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

static void
rebind_finish_current_round(void)
{
	rebind_complete_read();
	rebind_tick(1000001);
	for (unsigned peer = 1; peer < 4; peer++) {
		ClusterSemanticControlRebindWireV3 ack = rebind_ack(peer);
		UT_ASSERT(rebind_receive(&ack, peer, 0));
		rebind_tick(1000002 + peer);
	}
	UT_ASSERT_EQ(semantic_control_rebind.phase, SEMANTIC_CONTROL_REBIND_COMPLETE);
}

static void
rebind_finish_proof(void)
{
	rebind_start();
	rebind_finish_current_round();
}

static ClusterSemanticAdmissionToken
rebind_resource_x_token(void)
{
	ClusterSemanticAdmissionToken token = {
		.feature_bit = CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1,
		.record_generation = 6,
		.formation_epoch = 7,
		.side = CLUSTER_SEMANTIC_TARGET_SIDE,
		.entered = true
	};
	return token;
}

UT_TEST(rebind_reply_only_preserves_the_unchanged_ordinary_proof)
{
	ClusterSemanticControlRebindWireV3 request;
	ClusterSemanticAdmissionToken token = rebind_resource_x_token();
	const ClusterSemanticActivationAckTableV1 *base;

	rebind_setup();
	test_serving_finish_root();
	base = &SemanticControlRebindState->baseline.table;
	request = rebind_message(false);
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
	IsUnderPostmaster = true;
	UT_ASSERT_EQ(cluster_semantic_activation_resource_x_peer_open_check(&token, 3, 19),
		CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_MATCH);
	rebind_complete_read();
	rebind_tick(1000001);
	UT_ASSERT_EQ(test_send_calls[3], 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticControlRebindState->completion_seq), 0);
	IsUnderPostmaster = true;
	UT_ASSERT_EQ(cluster_semantic_activation_resource_x_peer_open_check(&token, 3, 19),
		CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_MATCH);
	test_first_open_finish();
}

/* A completed real collector must become the common consumer proof. Local
 * capability sampling alone is deliberately insufficient in earlier tests. */
UT_TEST(rebind_current_completion_authorizes_r4_and_resource_x_consumers)
{
	ClusterSemanticAdmissionToken token = {
		.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
		.record_generation = 6,
		.formation_epoch = 7,
		.side = CLUSTER_SEMANTIC_TARGET_SIDE,
		.entered = true
	};
	ClusterSemanticActivationAckTableV1 ordinary;
	rebind_finish_proof();
	ordinary = *SemanticActivationAckTable;
	UT_ASSERT_EQ(ordinary.expected[3].capability_generation, 19);
	IsUnderPostmaster = true;
	UT_ASSERT(cluster_semantic_activation_peer_open_matches(
		&token, 3, CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS, 20));
	UT_ASSERT(!cluster_semantic_activation_peer_open_matches(
		&token, 3, CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS, 19));
	token.feature_bit = CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1;
	UT_ASSERT_EQ(cluster_semantic_activation_resource_x_peer_open_check(&token, 3, 20),
		CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_MATCH);
	UT_ASSERT_EQ(cluster_semantic_activation_resource_x_peer_open_check(&token, 3, 19),
		CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_CAPABILITY_DRIFT);
	UT_ASSERT(memcmp(&ordinary, SemanticActivationAckTable, sizeof(ordinary)) == 0);
	test_first_open_finish();
}

UT_TEST(rebind_partial_proof_waits_only_for_same_key_live_owner)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		ClusterSemanticAdmissionToken token = rebind_resource_x_token();
		ClusterSemanticResourceXPeerOpenResult result;
		rebind_start();
		rebind_complete_read();
		rebind_tick(1000001);
		switch (fault) {
		case 1: test_remote_admitted_incarnations[2]++; break;
		case 2: test_peer_capability_word ^= 1; break;
		case 3: test_current_epoch++; break;
		case 4: test_rebind_proc.cluster_grd_generation++; break;
		case 5: test_rebind_proc.pid = 0; break;
		case 6: test_serving_formation++; break;
		case 7: test_resource_x_gate_snapshot.freeze_generation++; break;
		case 8: pg_atomic_fetch_add_u64(&SemanticActivationShmem->admission_seq, 2); break;
		}
		IsUnderPostmaster = true;
		result = cluster_semantic_activation_resource_x_peer_open_check(&token, 3, 20);
		if (fault == 0)
			UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_REBIND_PENDING);
		else {
			UT_ASSERT(result != CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_REBIND_PENDING);
			UT_ASSERT(result != CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_MATCH);
		}
		test_first_open_finish();
	}
}

UT_TEST(rebind_completion_requires_exact_provenance_and_current_owner)
{
	for (unsigned fault = 0; fault < 11; fault++) {
		ClusterSemanticAdmissionToken token = rebind_resource_x_token();
		SemanticControlRebindCompletion *proof;
		rebind_finish_proof();
		proof = &SemanticControlRebindState->completion;
		switch (fault) {
		case 0: proof->proof_binding[0] ^= 1; break;
		case 1: proof->live_digest[0] ^= 1; break;
		case 2: proof->nonce++; break;
		case 3: proof->cut.owner_generation++; break;
		case 4: proof->cut.base_seq += 2; break;
		case 5: proof->table.flags |= CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_OPEN_PROOF; break;
		case 6: proof->table.observed_members_lo &= ~UINT64_C(4); break;
		case 7: proof->table.observed[2].boot_id++; break;
		case 8: pg_atomic_fetch_add_u64(&SemanticControlRebindState->completion_seq, 1); break;
		case 9: test_rebind_proc.pid = 0; break;
		case 10: test_peer_capability_generation++; break;
		}
		IsUnderPostmaster = true;
		UT_ASSERT(cluster_semantic_activation_resource_x_peer_open_check(&token, 3, 20)
			!= CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_MATCH);
		test_first_open_finish();
	}
}

UT_TEST(rebind_proof_survives_only_original_carrier_invalidation_shape)
{
	ClusterSemanticAdmissionToken token = rebind_resource_x_token();
	ClusterSemanticActivationAckTableV1 retired;
	rebind_finish_proof();
	retired = *SemanticActivationAckTable;
	UT_ASSERT(semantic_activation_ack_image_invalidate(&retired));
	retired = *SemanticActivationAckTable;
	IsUnderPostmaster = true;
	UT_ASSERT_EQ(cluster_semantic_activation_resource_x_peer_open_check(&token, 3, 20),
		CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_MATCH);
	UT_ASSERT(memcmp(&retired, SemanticActivationAckTable, sizeof(retired)) == 0);
	SemanticActivationAckTable->capability_sample_digest++;
	UT_ASSERT(cluster_semantic_activation_resource_x_peer_open_check(&token, 3, 20)
		!= CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_MATCH);
	test_first_open_finish();
}

UT_TEST(rebind_terminal_reference_binds_completion_and_all_data_lifetimes)
{
	ClusterSemanticAdmissionToken token;
	ClusterSemanticTerminalPeerSnapshot before, after;
	ClusterSemanticActivationAckTableV1 ordinary;
	rebind_setup();
	test_serving_finish_root();
	ut_terminal_members_and_sessions();
	test_terminal_sessions.control_stream_generation = 19;
	IsUnderPostmaster = true;
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
		CLUSTER_SEMANTIC_TARGET_SIDE, &token), CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(cluster_semantic_activation_terminal_peer_capture(&token, 3,
		CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS, &before));
	ordinary = *SemanticActivationAckTable;
	test_peer_capability_generation = 20;
	for (unsigned peer = 0; peer < 4; peer++)
		test_rebind_stream_serial[peer] += 100;
	rebind_tick(1000000);
	rebind_finish_current_round();
	test_terminal_sessions.control_stream_generation = 20;
	IsUnderPostmaster = true;
	UT_ASSERT(!cluster_semantic_activation_terminal_peer_current(&token, &before));
	UT_ASSERT(cluster_semantic_activation_terminal_peer_capture(&token, 3,
		CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS, &after));
	UT_ASSERT_EQ(after.open_proof.source, CLUSTER_SEMANTIC_OPEN_PROOF_REBOUND);
	UT_ASSERT_EQ(after.open_proof.base_publication_seq, before.ack_publication_seq);
	UT_ASSERT_EQ(after.open_proof.completion_seq, 2);
	UT_ASSERT_EQ(after.ack_publication_seq, before.ack_publication_seq);
	UT_ASSERT(cluster_semantic_activation_terminal_peer_current(&token, &after));
	test_terminal_sessions.data_stream_generation[0]++;
	UT_ASSERT(!cluster_semantic_activation_terminal_peer_current(&token, &after));
	test_terminal_sessions.data_stream_generation[0]--;
	semantic_control_rebind_clear_completion();
	UT_ASSERT(!cluster_semantic_activation_terminal_peer_current(&token, &after));
	UT_ASSERT(memcmp(&ordinary, SemanticActivationAckTable, sizeof(ordinary)) == 0);
	cluster_semantic_activation_leave(&token);
	test_first_open_finish();
}

UT_TEST(rebind_serving_requires_new_original_owner_root_publication)
{
	ClusterSemanticActivationRefusal refusal;
	SemanticServingCut cut;
	rebind_finish_proof();
	IsUnderPostmaster = true;
	UT_ASSERT(semantic_serving_cut_read(&cut));
	UT_ASSERT_EQ(cut.open_proof.source, CLUSTER_SEMANTIC_OPEN_PROOF_REBOUND);
	UT_ASSERT_EQ(cut.open_proof.completion_seq, 2);
	UT_ASSERT(!semantic_serving_ready(&cut));
	IsUnderPostmaster = false;
	UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
	test_serving_tick();
	UT_ASSERT(semantic_serving.read_seq != 0);
	test_serving_complete_read();
	test_serving_tick();
	UT_ASSERT_EQ(test_serving_calls, 2);
	MyProc = NULL;
	UT_ASSERT(cluster_semantic_activation_startup_poll(&refusal));
	test_open_proof_caps_busy = true;
	UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
	test_open_proof_caps_busy = false;
	test_peer_capability_generation++;
	UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
	MyProc = &test_rebind_proc;
	test_first_open_finish();
}

UT_TEST(rebind_postmaster_view_never_calls_the_owned_hash_provider)
{
	SemanticServingCut cut;

	rebind_finish_proof();
	IsUnderPostmaster = false;
	MyProc = NULL;
	rebind_owned_hash_calls = 0;
	UT_ASSERT(semantic_serving_cut_read(&cut));
	UT_ASSERT_EQ(cut.open_proof.source, CLUSTER_SEMANTIC_OPEN_PROOF_REBOUND);
	UT_ASSERT_EQ(rebind_owned_hash_calls, 0);
	MyProc = &test_rebind_proc;
	test_first_open_finish();
}

UT_TEST(rebind_capability_contention_waits_without_hiding_known_contradictions)
{
	for (unsigned complete = 0; complete < 2; complete++) {
		for (unsigned fault = 0; fault < 5; fault++) {
			ClusterSemanticAdmissionToken token = rebind_resource_x_token();
			rebind_start();
			if (complete)
				rebind_finish_current_round();
			test_open_proof_caps_busy = fault != 4;
			if (fault == 4)
				test_capability_missing_peer = 2;
			if (fault == 1)
				test_remote_admitted_incarnations[2]++;
			if (fault == 2)
				test_serving_formation++;
			if (fault == 3) {
				if (complete)
					SemanticControlRebindState->completion.proof_binding[0] ^= 1;
				else
					test_resource_x_gate_snapshot.freeze_generation++;
			}
			IsUnderPostmaster = true;
			UT_ASSERT_EQ(cluster_semantic_activation_resource_x_peer_open_check(&token, 3, 20),
				fault == 0 || fault == 4 ? CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_IMAGE_OBSERVATION_PENDING
				: CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_IMAGE_NOT_CURRENT);
			test_first_open_finish();
		}
	}
}

UT_TEST(rebind_stack_hash_keeps_the_same_sha256_bytes)
{
	const unsigned lengths[] = { 0, 1, 55, 56, 64, 96, 1236, 1280 };
	const uint8 abc[32] = {
		0xba,0x78,0x16,0xbf,0x8f,0x01,0xcf,0xea,0x41,0x41,0x40,0xde,0x5d,0xae,0x22,0x23,
		0xb0,0x03,0x61,0xa3,0x96,0x17,0x7a,0x9c,0xb4,0x10,0xff,0x61,0xf2,0x00,0x15,0xad
	};
	uint8 bytes[1280], actual[32], configured[32];

	for (unsigned at = 0; at < sizeof(bytes); at++)
		bytes[at] = (uint8)(at * 31 + 7);
	for (unsigned item = 0; item < lengthof(lengths); item++) {
		UT_ASSERT(normal_start_sha256(bytes, lengths[item], configured));
		UT_ASSERT(semantic_control_rebind_sha256(bytes, lengths[item], actual));
		UT_ASSERT(memcmp(configured, actual, sizeof(actual)) == 0);
	}
	UT_ASSERT(semantic_control_rebind_sha256((const uint8 *)"abc", 3, actual));
	UT_ASSERT(memcmp(abc, actual, sizeof(actual)) == 0);
}

UT_TEST(rebind_final_observation_contention_is_not_identity_drift)
{
	for (unsigned sample = 3; sample <= 4; sample++) {
		ClusterSemanticAdmissionToken token = rebind_resource_x_token();
		rebind_finish_proof();
		test_open_proof_caps_samples = 0;
		test_open_proof_caps_busy_at = sample;
		IsUnderPostmaster = true;
		UT_ASSERT_EQ(cluster_semantic_activation_resource_x_peer_open_check(&token, 3, 20),
			CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_IMAGE_OBSERVATION_PENDING);
		test_open_proof_caps_busy_at = 0;
		UT_ASSERT_EQ(cluster_semantic_activation_resource_x_peer_open_check(&token, 3, 20),
			CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_MATCH);
		test_first_open_finish();
	}
}

/* The real LMON-local census, with only original external inputs. */
static ClusterNormalStopPollResult
rebind_stop_poll(const char **domain, const char **reason)
{
	ClusterNormalStopPollResult result;
	IsUnderPostmaster = true;
	MyAuxProcType = LmonProcess;
	result = cluster_semantic_normal_stop_poll(domain, NULL, reason);
	MyAuxProcType = NotAnAuxProcess;
	IsUnderPostmaster = false;
	return result;
}

UT_TEST(rebind_stop_consumes_complete_proof_with_online_closed)
{
	ClusterSemanticActivationAckTableV1 ordinary;
	ClusterSemanticAdmissionToken token = rebind_resource_x_token();
	uint64 members[4];
	rebind_finish_proof();
	ordinary = *SemanticActivationAckTable;
	test_terminal_stop = true;
	IsUnderPostmaster = true;
	UT_ASSERT_EQ(cluster_semantic_normal_stop_match(&test_serving_expected_open,
		test_serving_expected_root, members, NULL), CLUSTER_NORMAL_STOP_READY);
	for (unsigned node = 0; node < 4; node++)
		UT_ASSERT_EQ(members[node], SemanticControlRebindState->baseline.cut.members[node]);
	UT_ASSERT(cluster_semantic_activation_resource_x_peer_open_check(&token, 3, 20)
		!= CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_MATCH);
	UT_ASSERT(memcmp(&ordinary, SemanticActivationAckTable, sizeof(ordinary)) == 0);
	test_first_open_finish();
}

UT_TEST(rebind_stop_partial_wait_cannot_hide_nontransport_drift)
{
	rebind_start();
	test_terminal_stop = true;
	IsUnderPostmaster = true;
	UT_ASSERT_EQ(cluster_semantic_normal_stop_match(&test_serving_expected_open,
		test_serving_expected_root, NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	test_serving_formation++;
	UT_ASSERT_EQ(cluster_semantic_normal_stop_match(&test_serving_expected_open,
		test_serving_expected_root, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	test_first_open_finish();
}

UT_TEST(rebind_stop_request_preserves_original_collection)
{
	rebind_start();
	test_terminal_stop = true;
	rebind_finish_current_round();
	UT_ASSERT_EQ(semantic_control_rebind.phase, SEMANTIC_CONTROL_REBIND_COMPLETE);
	UT_ASSERT_EQ(rebind_stop_poll(NULL, NULL), CLUSTER_NORMAL_STOP_READY);
	IsUnderPostmaster = true;
	UT_ASSERT_EQ(cluster_semantic_normal_stop_match(&test_serving_expected_open,
		test_serving_expected_root, NULL, NULL), CLUSTER_NORMAL_STOP_READY);
	test_first_open_finish();
}

UT_TEST(rebind_stop_census_tracks_collection_and_reply_retirement)
{
	ClusterSemanticControlRebindWireV3 request;
	const char *domain;
	rebind_start();
	rebind_complete_read();
	rebind_tick(1000001);
	UT_ASSERT_EQ(rebind_stop_poll(&domain, NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(strcmp(domain, "R4_CONTROL_REBIND") == 0);
	for (unsigned peer = 1; peer < 4; peer++) {
		ClusterSemanticControlRebindWireV3 ack = rebind_ack(peer);
		UT_ASSERT(rebind_receive(&ack, peer, 0));
	}
	rebind_tick(1000002);
	UT_ASSERT_EQ(rebind_stop_poll(NULL, NULL), CLUSTER_NORMAL_STOP_READY);
	request = rebind_ack(3);
	request.kind = 1;
	request.result = 0;
	request.coordinator_node = 3;
	request.member_node = 0;
	request.round_nonce = 88;
	UT_ASSERT(semantic_control_rebind_binding(3, 88, request.proof_binding));
	UT_ASSERT(rebind_receive(&request, 3, 0));
	test_send_results[3] = CLUSTER_IC_SEND_NOT_ADMITTED;
	rebind_tick(1000003);
	UT_ASSERT_EQ(rebind_stop_poll(NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(semantic_control_rebind.requests[3].valid);
	test_send_results[3] = CLUSTER_IC_SEND_WOULD_BLOCK;
	rebind_tick(1000004);
	UT_ASSERT(!semantic_control_rebind.requests[3].valid);
	UT_ASSERT_EQ(rebind_stop_poll(NULL, NULL), CLUSTER_NORMAL_STOP_READY);
	test_first_open_finish();
}

UT_TEST(rebind_stop_census_bad_geometry_overrides_other_pending)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		rebind_finish_proof();
		semantic_activation_local_inflight[0][0] = 1;
		pg_atomic_write_u32(&SemanticActivationShmem->inflight[0][0], 1);
		switch (fault) {
		case 0: semantic_control_rebind.phase = 99; break;
		case 1: semantic_control_rebind.nonce++; break;
		case 2: semantic_control_rebind.read_seq = 99; break;
		case 3: pg_atomic_write_u64(&SemanticControlRebindState->read_seq, 99); break;
		case 4: semantic_control_rebind.acks[1].message.member_node = 2; break;
		case 5: semantic_control_rebind.requests[0] = semantic_control_rebind.acks[1]; break;
		case 6: semantic_control_rebind.table.observed_members_hi = 1; break;
		case 7: pg_atomic_write_u64(&SemanticControlRebindState->progress_seq, 3); break;
		case 8: pg_atomic_write_u64(&SemanticControlRebindState->completion_seq, 3); break;
		}
		UT_ASSERT_EQ(rebind_stop_poll(NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
		test_first_open_finish();
	}
}

UT_TEST(rebind_live_lmon_and_dispatch_use_original_entries)
{
	uint64 old_ring;
	rebind_setup();
	test_serving_finish_root();
	test_peer_capability_generation++;
	for (unsigned peer = 0; peer < 4; peer++)
		test_rebind_stream_serial[peer] += 100;
	test_serving_tick();
	UT_ASSERT(semantic_control_rebind.read_seq != 0);
	rebind_complete_read();
	test_serving_tick();
	old_ring = semantic_activation_ack_local_ingress.producer_seq;
	for (unsigned peer = 1; peer < 4; peer++) {
		ClusterSemanticControlRebindWireV3 ack = rebind_ack(peer);
		UT_ASSERT(rebind_receive(&ack, peer, 6));
	}
	UT_ASSERT_EQ(semantic_activation_ack_local_ingress.producer_seq, old_ring);
	test_serving_tick();
	UT_ASSERT_EQ(semantic_control_rebind.phase, SEMANTIC_CONTROL_REBIND_COMPLETE);
	test_first_open_finish();
}

UT_TEST(rebind_stop_completed_read_yields_to_original_rebind_owner)
{
	ClusterSemanticActivationRecord got;
	uint8 root[512];
	uint64 stop_seq;
	rebind_setup();
	test_serving_finish_root();
	test_terminal_stop = true;
	IsUnderPostmaster = true;
	MyAuxProcType = LmonProcess;
	UT_ASSERT_EQ(cluster_semantic_normal_stop_read_identity(&got, root, NULL, NULL),
		CLUSTER_NORMAL_STOP_PENDING);
	stop_seq = semantic_activation_lmon_stop_read_seq;
	UT_ASSERT(stop_seq != 0);
	ut_a148_complete_stop_read(&test_serving_expected_open);
	test_peer_capability_generation++;
	for (unsigned peer = 0; peer < 4; peer++)
		test_rebind_stream_serial[peer] += 100;
	UT_ASSERT_EQ(cluster_semantic_normal_stop_read_identity(&got, root, NULL, NULL),
		CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(semantic_activation_lmon_stop_read_seq, 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->record_cas_completion_seq), stop_seq);
	UT_ASSERT_EQ(cluster_semantic_normal_stop_read_identity(&got, root, NULL, NULL),
		CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(semantic_control_rebind.read_seq > stop_seq);
	UT_ASSERT_EQ(semantic_activation_lmon_stop_read_seq, 0);
	MyAuxProcType = NotAnAuxProcess;
	IsUnderPostmaster = false;
	rebind_finish_current_round();
	IsUnderPostmaster = true;
	MyAuxProcType = LmonProcess;
	UT_ASSERT_EQ(cluster_semantic_normal_stop_read_identity(&got, root, NULL, NULL),
		CLUSTER_NORMAL_STOP_PENDING);
	ut_a148_complete_stop_read(&test_serving_expected_open);
	UT_ASSERT_EQ(cluster_semantic_normal_stop_read_identity(&got, root, NULL, NULL),
		CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(got.record_generation, test_serving_expected_open.record_generation);
	test_first_open_finish();
}

UT_TEST(rebind_control_seal_preserves_completed_proof_without_new_work)
{
	uint64 nonce, completion, requests;
	rebind_finish_proof();
	test_terminal_stop = true;
	test_rebind_control_sealed = true;
	nonce = semantic_control_rebind.nonce;
	completion = pg_atomic_read_u64(&SemanticControlRebindState->completion_seq);
	requests = pg_atomic_read_u64(&SemanticActivationShmem->record_cas_request_seq);
	rebind_tick(2000000);
	UT_ASSERT_EQ(semantic_control_rebind.phase, SEMANTIC_CONTROL_REBIND_COMPLETE);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticControlRebindState->completion_seq), completion);
	UT_ASSERT_EQ(semantic_control_rebind.nonce, nonce);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->record_cas_request_seq), requests);
	UT_ASSERT_EQ(rebind_stop_poll(NULL, NULL), CLUSTER_NORMAL_STOP_READY);
	IsUnderPostmaster = true;
	UT_ASSERT_EQ(cluster_semantic_normal_stop_match(&test_serving_expected_open,
		test_serving_expected_root, NULL, NULL), CLUSTER_NORMAL_STOP_READY);
	test_first_open_finish();
}


UT_TEST(rebind_capability_gap_retains_original_pending_reply_and_round)
{
	ClusterSemanticControlRebindWireV3 request;
	uint64 nonce;
	rebind_start();
	rebind_complete_read();
	rebind_tick(1000001);
	request = rebind_ack(3);
	request.kind = 1;
	request.result = 0;
	request.coordinator_node = 3;
	request.member_node = 0;
	request.round_nonce = 88;
	UT_ASSERT(semantic_control_rebind_binding(3, 88, request.proof_binding));
	UT_ASSERT(rebind_receive(&request, 3, 0));
	nonce = semantic_control_rebind.nonce;
	test_terminal_stop = true;
	test_capability_missing_bitmap = UINT64CONST(1) << 2;
	rebind_tick(1000002);
	UT_ASSERT(semantic_control_rebind.requests[3].valid);
	UT_ASSERT_EQ(semantic_control_rebind.phase, SEMANTIC_CONTROL_REBIND_COLLECTING);
	UT_ASSERT_EQ(semantic_control_rebind.nonce, nonce);
	IsUnderPostmaster = true;
	UT_ASSERT_EQ(cluster_semantic_normal_stop_match(&test_serving_expected_open,
		test_serving_expected_root, NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	test_capability_missing_bitmap = 0;
	rebind_tick(1000003);
	UT_ASSERT(!semantic_control_rebind.requests[3].valid);
	UT_ASSERT_EQ(semantic_control_rebind.nonce, nonce);
	test_first_open_finish();
}

UT_TEST(rebind_sealed_control_refuses_ingress_and_new_round)
{
	ClusterSemanticControlRebindWireV3 request;
	uint64 nonce, seq;
	rebind_finish_proof();
	request = rebind_ack(3);
	request.kind = 1;
	request.result = 0;
	request.coordinator_node = 3;
	request.member_node = 0;
	request.round_nonce = 88;
	UT_ASSERT(semantic_control_rebind_binding(3, 88, request.proof_binding));
	test_terminal_stop = test_rebind_control_sealed = true;
	UT_ASSERT(!rebind_receive(&request, 3, 0));
	UT_ASSERT(!semantic_control_rebind.requests[3].valid);
	nonce = semantic_control_rebind.nonce;
	seq = pg_atomic_read_u64(&SemanticActivationShmem->record_cas_request_seq);
	test_peer_capability_generation++;
	for (unsigned peer = 0; peer < 4; peer++)
		test_rebind_stream_serial[peer]++;
	rebind_tick(3000000);
	UT_ASSERT_EQ(semantic_control_rebind.nonce, nonce);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->record_cas_request_seq), seq);
	IsUnderPostmaster = true;
	UT_ASSERT_EQ(cluster_semantic_normal_stop_match(&test_serving_expected_open,
		test_serving_expected_root, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	test_first_open_finish();
}

UT_TEST(rebind_ingress_without_original_success_has_no_responsibility)
{
	ClusterSemanticControlRebindWireV3 request = rebind_message(false);
	rebind_setup();
	request.coordinator_node = 3;
	request.member_node = 0;
	request.capability_word = test_peer_capability_word;
	UT_ASSERT(!SemanticControlRebindState->baseline.valid);
	UT_ASSERT(!rebind_receive(&request, 3, 0));
	UT_ASSERT(!semantic_control_rebind.requests[3].valid);
	rebind_tick(1000000);
	UT_ASSERT_EQ(rebind_stop_poll(NULL, NULL), CLUSTER_NORMAL_STOP_READY);
	test_first_open_finish();
}

/* A completed proof preserves the original exact RELEASE receipt tail.
 * This observation neither repairs the cap store nor signs the sixth leg. */
UT_TEST(rebind_completed_stop_observes_original_terminal_peer_receipt_tail)
{
	ClusterSemanticActivationAckTableV1 complete, ordinary;
	ClusterSemanticOpenProofIdentity before, after;
	SemanticControlRebindShmem shared;
	SemanticStopTrace trace = { 0 };
	const char *reason = NULL;
	uint64 incarnations[4], read_seq;
	const uint64 expected_members[4]
		= { UINT64_C(0x445566778899aabb), UINT64_C(0x101), UINT64_C(0x102), UINT64_C(0x103) };
	int sends[CLUSTER_MAX_NODES];

	rebind_finish_proof();
	test_terminal_stop = true;
	IsUnderPostmaster = true;
	MyAuxProcType = CheckpointerProcess;
	ordinary = *SemanticActivationAckTable;
	UT_ASSERT_EQ(semantic_open_proof_snapshot_mode(&complete, &before, true),
				 SEMANTIC_OPEN_PROOF_REBOUND);
	UT_ASSERT_EQ(cluster_semantic_normal_stop_match(&test_serving_expected_open,
													test_serving_expected_root, incarnations, NULL),
				 CLUSTER_NORMAL_STOP_READY);

	test_terminal_peer_open = test_serving_expected_open;
	memcpy(test_terminal_peer_root, test_serving_expected_root, sizeof(test_terminal_peer_root));
	test_terminal_peer_record_enabled = true;
	test_terminal_peer_record = (ClusterSfPeerCap){ .valid = true,
													.bits = test_peer_capability_word,
													.generation = test_peer_capability_generation };
	UT_ASSERT(cluster_sf_peer_cap_invalidate_gen(&test_terminal_peer_record,
												 test_peer_capability_generation));
	test_terminal_peer_eligible = false;
	UT_ASSERT_EQ(cluster_semantic_normal_stop_match(&test_serving_expected_open,
													test_serving_expected_root, incarnations, NULL),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(semantic_activation_ack_complete_image_check_internal(
					 &complete, 15, 0, 7, 0, cluster_node_id, test_local_capability_word,
					 &test_serving_expected_open, test_serving_expected_root, NULL),
				 CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_IMAGE_OBSERVATION_PENDING);
	test_terminal_peer_eligible = true;
	UT_ASSERT_EQ(semantic_activation_ack_complete_image_check_internal(
					 &complete, 15, 0, 7, 0, cluster_node_id, test_local_capability_word,
					 &test_serving_expected_open, test_serving_expected_root, NULL),
				 CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_MATCH);

	shared = *SemanticControlRebindState;
	memcpy(sends, test_send_calls, sizeof(sends));
	read_seq = pg_atomic_read_u64(&SemanticActivationShmem->record_cas_request_seq);
	UT_ASSERT_EQ(semantic_open_proof_cut_check(&SemanticControlRebindState->progress, true),
				 SEMANTIC_CONTROL_REBIND_SAME);
	UT_ASSERT_EQ(semantic_open_proof_snapshot_mode(&complete, &after, true),
				 SEMANTIC_OPEN_PROOF_REBOUND);
	UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
	UT_ASSERT_EQ(semantic_normal_stop_match_impl(&test_serving_expected_open,
												 test_serving_expected_root, incarnations, &reason,
												 &trace),
				 CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_STR_EQ(reason, "SEMANTIC_STOP_IDENTITY_READY");
	UT_ASSERT(trace.have_before && trace.have_after);
	for (unsigned node = 0; node < 4; node++)
		UT_ASSERT_EQ(incarnations[node], expected_members[node]);
	UT_ASSERT(memcmp(&ordinary, SemanticActivationAckTable, sizeof(ordinary)) == 0);
	UT_ASSERT(memcmp(&shared, SemanticControlRebindState, sizeof(shared)) == 0);
	UT_ASSERT(memcmp(sends, test_send_calls, sizeof(sends)) == 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->record_cas_request_seq), read_seq);
	UT_ASSERT(!test_terminal_peer_record.valid);
	UT_ASSERT_EQ(test_terminal_peer_record.bits, 0);
	UT_ASSERT_EQ(test_terminal_peer_record.generation, 20);

	test_terminal_peer_record_enabled = test_terminal_peer_eligible = false;
	MyAuxProcType = NotAnAuxProcess;
	test_first_open_finish();
}

/* Same-OPEN terminal-stop observations retain every original refusal. */
static void
rebind_stop_tail_fixture(bool complete)
{
	if (complete)
		rebind_finish_proof();
	else
		rebind_start();
	test_terminal_stop = true;
	IsUnderPostmaster = true;
	MyAuxProcType = CheckpointerProcess;
	test_terminal_peer_open = test_serving_expected_open;
	memcpy(test_terminal_peer_root, test_serving_expected_root, sizeof(test_terminal_peer_root));
	test_terminal_peer_record_enabled = test_terminal_peer_eligible = true;
	test_terminal_peer_record = (ClusterSfPeerCap){ .valid = true,
													.bits = test_peer_capability_word,
													.generation = test_peer_capability_generation };
	UT_ASSERT(cluster_sf_peer_cap_invalidate_gen(&test_terminal_peer_record,
												 test_peer_capability_generation));
}

static void
rebind_stop_tail_finish(void)
{
	test_terminal_peer_record_enabled = test_terminal_peer_eligible = false;
	MyAuxProcType = NotAnAuxProcess;
	test_first_open_finish();
}

UT_TEST(rebind_stop_tail_never_covers_missing_evidence_or_identity_drift)
{
	const char *faults[] = { "missing-tail",
							 "changed-generation",
							 "nonzero-bits",
							 "zero-generation",
							 "exhausted-generation",
							 "admitted-boot",
							 "root-bytes",
							 "owner-generation",
							 "round-nonce",
							 "completion-binding",
							 "formation",
							 "epoch",
							 "observation-busy",
							 "busy-with-known-boot-drift" };
	for (unsigned fault = 0; fault < lengthof(faults); fault++) {
		ClusterSemanticActivationAckTableV1 ordinary;
		SemanticControlRebindShmem shared;
		SemanticControlRebindRuntime owner;
		ClusterSfPeerCap capability;
		uint64 incarnations[4], read_seq;
		int sends[CLUSTER_MAX_NODES];
		ClusterNormalStopPollResult expected
			= fault < 5 || fault == 12 ? CLUSTER_NORMAL_STOP_PENDING : CLUSTER_NORMAL_STOP_INVALID;
		ClusterNormalStopPollResult actual;
		rebind_stop_tail_fixture(true);
		switch (fault) {
		case 0:
			test_terminal_peer_eligible = false;
			break;
		case 1:
			test_terminal_peer_record.generation++;
			break;
		case 2:
			test_terminal_peer_record.bits = 1;
			break;
		case 3:
			test_terminal_peer_record.generation = 0;
			break;
		case 4:
			test_terminal_peer_record.generation = UINT32_MAX;
			break;
		case 5:
			test_remote_admitted_incarnations[3]++;
			break;
		case 6:
			SemanticActivationPgrdSnapshot->descriptor_bytes[100] ^= 1;
			break;
		case 7:
			test_rebind_proc.cluster_grd_generation++;
			break;
		case 8:
			pg_atomic_fetch_add_u64(&SemanticControlRebindState->nonce, 1);
			break;
		case 9:
			SemanticControlRebindState->completion.proof_binding[0] ^= 1;
			break;
		case 10:
			test_serving_formation++;
			break;
		case 11:
			test_current_epoch++;
			break;
		case 12:
			test_open_proof_caps_busy = true;
			break;
		case 13:
			test_open_proof_caps_busy = true;
			test_remote_admitted_incarnations[3]++;
			break;
		}
		ordinary = *SemanticActivationAckTable;
		shared = *SemanticControlRebindState;
		owner = semantic_control_rebind;
		capability = test_terminal_peer_record;
		memcpy(sends, test_send_calls, sizeof(sends));
		read_seq = pg_atomic_read_u64(&SemanticActivationShmem->record_cas_request_seq);
		memset(incarnations, 0xa5, sizeof(incarnations));
		actual = cluster_semantic_normal_stop_match(&test_serving_expected_open,
													test_serving_expected_root, incarnations, NULL);
		printf("# CC2130 boundary %s result=%u expected=%u\n", faults[fault], actual, expected);
		UT_ASSERT_EQ(actual, expected);
		for (unsigned node = 0; node < 4; node++)
			UT_ASSERT_EQ(incarnations[node], 0);
		UT_ASSERT(memcmp(&ordinary, SemanticActivationAckTable, sizeof(ordinary)) == 0);
		UT_ASSERT(memcmp(&shared, SemanticControlRebindState, sizeof(shared)) == 0);
		UT_ASSERT(memcmp(&owner, &semantic_control_rebind, sizeof(owner)) == 0);
		UT_ASSERT(memcmp(&capability, &test_terminal_peer_record, sizeof(capability)) == 0);
		UT_ASSERT(memcmp(sends, test_send_calls, sizeof(sends)) == 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->record_cas_request_seq),
					 read_seq);
		rebind_stop_tail_finish();
	}
}

UT_TEST(rebind_unfinished_stop_tail_cannot_complete_a_round)
{
	SemanticControlRebindShmem shared;
	SemanticControlRebindRuntime owner;
	uint64 incarnations[4];
	rebind_stop_tail_fixture(false);
	shared = *SemanticControlRebindState;
	owner = semantic_control_rebind;
	UT_ASSERT_EQ(cluster_semantic_normal_stop_match(&test_serving_expected_open,
													test_serving_expected_root, incarnations, NULL),
				 CLUSTER_NORMAL_STOP_PENDING);
	for (unsigned node = 0; node < 4; node++)
		UT_ASSERT_EQ(incarnations[node], 0);
	UT_ASSERT(memcmp(&shared, SemanticControlRebindState, sizeof(shared)) == 0);
	UT_ASSERT(memcmp(&owner, &semantic_control_rebind, sizeof(owner)) == 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticControlRebindState->completion_seq), 0);
	rebind_stop_tail_finish();
}

UT_TEST(rebind_terminal_stop_tail_never_reopens_online_consumers)
{
	ClusterSemanticAdmissionToken token = rebind_resource_x_token();
	ClusterSemanticActivationAckTableV1 ordinary;
	SemanticControlRebindShmem shared;
	rebind_stop_tail_fixture(true);
	ordinary = *SemanticActivationAckTable;
	shared = *SemanticControlRebindState;
	UT_ASSERT(cluster_semantic_activation_resource_x_peer_open_check(&token, 3, 20)
			  != CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_MATCH);
	token.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	UT_ASSERT(!cluster_semantic_activation_peer_open_matches(
		&token, 3, CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS, 20));
	UT_ASSERT(!test_terminal_peer_record.valid);
	UT_ASSERT_EQ(test_terminal_peer_record.bits, 0);
	UT_ASSERT(memcmp(&ordinary, SemanticActivationAckTable, sizeof(ordinary)) == 0);
	UT_ASSERT(memcmp(&shared, SemanticControlRebindState, sizeof(shared)) == 0);
	rebind_stop_tail_finish();
}

/* Existing membership snapshot seam fires after the first proof cut. */
static void
rebind_stop_tail_republish_completion(void)
{
	pg_atomic_fetch_add_u64(&SemanticControlRebindState->completion_seq, 2);
}

UT_TEST(rebind_stop_tail_rechecks_late_observation_and_publication)
{
	for (unsigned sample = 2; sample <= 4; sample++) {
		uint64 incarnations[4];
		rebind_stop_tail_fixture(true);
		test_open_proof_caps_samples = 0;
		test_open_proof_caps_busy_at = sample;
		UT_ASSERT_EQ(cluster_semantic_normal_stop_match(&test_serving_expected_open,
														test_serving_expected_root, incarnations,
														NULL),
					 CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT_EQ(test_open_proof_caps_samples, sample == 2 ? 2 : 4);
		for (unsigned node = 0; node < 4; node++)
			UT_ASSERT_EQ(incarnations[node], 0);
		test_open_proof_caps_busy_at = 0;
		UT_ASSERT_EQ(cluster_semantic_normal_stop_match(&test_serving_expected_open,
														test_serving_expected_root, incarnations,
														NULL),
					 CLUSTER_NORMAL_STOP_READY);
		rebind_stop_tail_finish();
	}
	{
		uint64 incarnations[4], seq;
		rebind_stop_tail_fixture(true);
		seq = pg_atomic_read_u64(&SemanticControlRebindState->completion_seq);
		test_membership_snapshot_hook = rebind_stop_tail_republish_completion;
		UT_ASSERT_EQ(cluster_semantic_normal_stop_match(&test_serving_expected_open,
														test_serving_expected_root, incarnations,
														NULL),
					 CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT(test_membership_snapshot_hook == NULL);
		UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticControlRebindState->completion_seq), seq + 2);
		for (unsigned node = 0; node < 4; node++)
			UT_ASSERT_EQ(incarnations[node], 0);
		UT_ASSERT_EQ(cluster_semantic_normal_stop_match(&test_serving_expected_open,
														test_serving_expected_root, incarnations,
														NULL),
					 CLUSTER_NORMAL_STOP_READY);
		rebind_stop_tail_finish();
	}
}
int
main(void)
{
	UT_PLAN(95);
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
	UT_RUN(rebind_current_completion_authorizes_r4_and_resource_x_consumers);
	UT_RUN(rebind_reply_only_preserves_the_unchanged_ordinary_proof);
	UT_RUN(rebind_partial_proof_waits_only_for_same_key_live_owner);
	UT_RUN(rebind_completion_requires_exact_provenance_and_current_owner);
	UT_RUN(rebind_proof_survives_only_original_carrier_invalidation_shape);
	UT_RUN(rebind_terminal_reference_binds_completion_and_all_data_lifetimes);
	UT_RUN(rebind_serving_requires_new_original_owner_root_publication);
	UT_RUN(rebind_postmaster_view_never_calls_the_owned_hash_provider);
	UT_RUN(rebind_capability_contention_waits_without_hiding_known_contradictions);
	UT_RUN(rebind_stack_hash_keeps_the_same_sha256_bytes);
	UT_RUN(rebind_final_observation_contention_is_not_identity_drift);
	UT_RUN(test_145a_restore_open_proof_reconstructs_table);
	UT_RUN(test_145b_restore_open_proof_fail_closed_on_missing_open);
	UT_RUN(test_145c_restore_open_proof_fail_closed_on_root_refusal);
	UT_RUN(test_145d_restore_open_proof_fail_closed_on_membership_drift);
	UT_RUN(test_145e_restore_open_proof_is_idempotent);
	UT_RUN(test_145f_restore_open_proof_requires_active_latch);
	UT_RUN(test_145g_peer_open_matches_consumes_open_proof);
	UT_RUN(test_145h_resource_x_open_carrier_survives_unavailable_snapshot);
	UT_RUN(test_145i_resource_x_open_carrier_invalidates_formation_drift);
	UT_RUN(test_145j_resource_x_peer_match_reports_first_failed_predicate);
	UT_RUN(test_145l_epoch_zero_resource_x_open_carrier_survives_unavailable_snapshot);
	UT_RUN(test_145m_resource_x_prepare_carrier_survives_unavailable_snapshot);
	UT_RUN(test_terminal_peer_stale_origin_has_only_inquiry_qualification);
	UT_RUN(test_terminal_peer_missing_authority_clears_output);
	UT_RUN(test_terminal_peer_aba_invalidates_original_snapshot);
	UT_RUN(test_terminal_peer_mid_sample_drift_is_refused);
	UT_RUN(test_terminal_peer_uses_actual_recovered_open_carrier);
	UT_RUN(rebind_stop_consumes_complete_proof_with_online_closed);
	UT_RUN(rebind_stop_partial_wait_cannot_hide_nontransport_drift);
	UT_RUN(rebind_stop_request_preserves_original_collection);
	UT_RUN(rebind_stop_census_tracks_collection_and_reply_retirement);
	UT_RUN(rebind_stop_census_bad_geometry_overrides_other_pending);
	UT_RUN(rebind_live_lmon_and_dispatch_use_original_entries);
	UT_RUN(rebind_stop_completed_read_yields_to_original_rebind_owner);
	UT_RUN(rebind_control_seal_preserves_completed_proof_without_new_work);
	UT_RUN(rebind_capability_gap_retains_original_pending_reply_and_round);
	UT_RUN(rebind_sealed_control_refuses_ingress_and_new_round);
	UT_RUN(test_a148_stop_identity_matches_full_record_root_and_current_members);
	UT_RUN(test_stop_identity_trace_off_is_silent);
	UT_RUN(test_stop_identity_trace_keeps_first_predicate_and_full_identity);
	UT_RUN(test_stop_identity_pending_does_not_consume_first_rejection);
	UT_RUN(test_stop_identity_trace_names_service_binding);
	UT_RUN(test_stop_identity_trace_defers_original_snapshot_until_lock_free);
	UT_RUN(test_stop_identity_trace_does_not_relabel_before_tuple_as_after);
	UT_RUN(test_stop_identity_trace_keeps_before_and_after_at_late_rejection);
	UT_RUN(test_stop_identity_trace_names_original_clean_writer_rejection);
	UT_RUN(test_a148_stop_identity_rejects_record_and_namespace_contradictions);
	UT_RUN(test_a148_stop_identity_observation_gaps_do_not_publish_partial_identity);
	UT_RUN(test_a148_stop_identity_rechecks_each_original_publication);
	UT_RUN(test_a148_stop_read_uses_original_mailbox_before_identity);
	UT_RUN(test_a148_stop_read_preserves_every_existing_mailbox_owner);
	UT_RUN(test_a148_stop_read_keeps_completion_through_observation_gap);
	UT_RUN(test_a148_stop_read_never_accepts_failed_or_changed_durable_identity);
	UT_RUN(test_a148_stop_read_has_one_lmon_owner_and_no_activation_interleave);
	UT_RUN(test_a148_semantic_stop_poll_requires_original_owner_and_shared_state);
	UT_RUN(test_a148_semantic_stop_poll_all_admission_cells_and_original_release);
	UT_RUN(test_a148_semantic_stop_poll_mailbox_completion_is_not_consumption);
	UT_RUN(test_a148_semantic_stop_poll_ack_responsibilities_use_original_retirement);
	UT_RUN(test_a148_semantic_stop_poll_restart_active_and_terminal_caches);
	UT_RUN(test_a148_semantic_stop_poll_invalid_not_hidden_by_pending);
	UT_RUN(test_a148_postmaster_retention_hint_preserves_unknown_and_publication_history);
	UT_RUN(test_a148_stop_poll_includes_original_phase3_handoff);
	UT_RUN(rebind_ingress_without_original_success_has_no_responsibility);
	UT_RUN(rebind_completed_stop_observes_original_terminal_peer_receipt_tail);
	UT_RUN(rebind_stop_tail_never_covers_missing_evidence_or_identity_drift);
	UT_RUN(rebind_unfinished_stop_tail_cannot_complete_a_round);
	UT_RUN(rebind_terminal_stop_tail_never_reopens_online_consumers);
	UT_RUN(rebind_stop_tail_rechecks_late_observation_and_publication);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
