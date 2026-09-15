/*-------------------------------------------------------------------------
 *
 * test_cluster_replacement_wire.c
 *    Pure tests for spec-5.15A opcode-18 replacement wire codec.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_replacement_wire.h"

#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();


static uint32
test_get_le32(const uint8 *p)
{
	return (uint32)p[0] | ((uint32)p[1] << 8) | ((uint32)p[2] << 16)
		   | ((uint32)p[3] << 24);
}


static uint64
test_get_le64(const uint8 *p)
{
	uint64 value = 0;
	int i;

	for (i = 0; i < 8; i++)
		value |= (uint64)p[i] << (i * 8);
	return value;
}


static ClusterReplacementWireMessage
make_valid_message(uint32 phase)
{
	ClusterReplacementWireMessage message;
	int i;

	memset(&message, 0, sizeof(message));
	message.phase = phase;
	message.target_node_id = 3;
	message.epoch = UINT64_C(0x1112131415161718);
	message.request_nonce = UINT64_C(0x2122232425262728);
	message.identity0 = UINT64_C(0x3132333435363738);
	message.identity1 = UINT64_C(0x4142434445464748);
	message.grammar_fingerprint
		= CANDIDATE2_CORRECTED_A1_GRAMMAR_FINGERPRINT;
	if (phase == CLUSTER_REPLACEMENT_WIRE_PHASE_TARGET_RECOVERY_READY) {
		message.body.phase3.jcmk_generation = UINT64_C(0x5152535455565758);
		message.body.phase3.episode_state_generation = UINT32_C(0x61626364);
	} else {
		for (i = 0; i < CLUSTER_REPLACEMENT_WIRE_BITMAP_BYTES; i++)
			message.body.bitmap[i] = (uint8)(0x70 + i);
		if (phase == CLUSTER_REPLACEMENT_WIRE_PHASE_FAILSTOP_APPLICATION_ACK)
			message.target_node_id = -1;
	}
	return message;
}


static ClusterR4PrerequisiteSnapshot
make_ready_snapshot(void)
{
	ClusterR4PrerequisiteSnapshot snapshot;

	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.status = CLUSTER_R4_PREREQUISITE_R4A_READY;
	snapshot.ready = true;
	snapshot.target_node_id = 3;
	snapshot.episode_state_generation = UINT32_C(0x61626364);
	snapshot.jcmk_generation = UINT64_C(0x5152535455565758);
	snapshot.request_nonce = UINT64_C(0x2122232425262728);
	snapshot.old_admitted_incarnation = UINT64_C(0x3132333435363738);
	snapshot.fresh_incarnation = UINT64_C(0x4142434445464748);
	snapshot.committed_epoch = UINT64_C(71);
	snapshot.grammar_fingerprint
		= CANDIDATE2_CORRECTED_A1_GRAMMAR_FINGERPRINT;
	return snapshot;
}


static ClusterICEnvelope
make_phase3_envelope(int32 source_node_id, int32 dest_node_id,
					 uint64 epoch)
{
	ClusterICEnvelope envelope;

	memset(&envelope, 0, sizeof(envelope));
	envelope.msg_type = PGRAC_IC_MSG_GES_REQUEST;
	envelope.source_node_id = (uint32)source_node_id;
	envelope.dest_node_id = (uint32)dest_node_id;
	envelope.epoch = epoch;
	envelope.payload_length = CLUSTER_REPLACEMENT_WIRE_BYTES;
	return envelope;
}


static ClusterReplacementPhase3IngressResult
ingress_phase3(ClusterReplacementPhase3Handoff *handoff,
			   ClusterICEnvelope *envelope, uint8 *bytes,
			   int32 authenticated_source, int32 local_node,
			   uint64 current_epoch, uint32 connection_generation)
{
	return cluster_replacement_wire_phase3_ingress(
		handoff, envelope, bytes, CLUSTER_REPLACEMENT_WIRE_BYTES,
		authenticated_source, local_node, current_epoch,
		connection_generation);
}


UT_TEST(test_codec_is_exact_72_byte_little_endian)
{
	ClusterReplacementWireMessage in;
	ClusterReplacementWireMessage out;
	uint8 bytes[CLUSTER_REPLACEMENT_WIRE_BYTES];

	in = make_valid_message(CLUSTER_REPLACEMENT_WIRE_PHASE_PURGE_REQUEST);
	memset(bytes, 0xA5, sizeof(bytes));
	UT_ASSERT(cluster_replacement_wire_encode(&in, bytes));
	UT_ASSERT_EQ((int)sizeof(bytes), 72);
	UT_ASSERT_EQ((int)test_get_le32(bytes + 0),
				 (int)GES_REQ_OPCODE_REPLACEMENT_EPISODE);
	UT_ASSERT_EQ((int)test_get_le32(bytes + 4),
				 (int)CLUSTER_REPLACEMENT_WIRE_PHASE_PURGE_REQUEST);
	UT_ASSERT_EQ((int)test_get_le32(bytes + 8), 3);
	UT_ASSERT_EQ((int)test_get_le32(bytes + 12),
				 (int)CLUSTER_REPLACEMENT_WIRE_VERSION);
	UT_ASSERT(test_get_le64(bytes + 16) == in.epoch);
	UT_ASSERT(test_get_le64(bytes + 24) == in.request_nonce);
	UT_ASSERT(test_get_le64(bytes + 32) == in.identity0);
	UT_ASSERT(test_get_le64(bytes + 40) == in.identity1);
	UT_ASSERT_EQ(memcmp(bytes + 48, in.body.bitmap, 16), 0);
	UT_ASSERT(test_get_le64(bytes + 64) == in.grammar_fingerprint);

	memset(&out, 0, sizeof(out));
	UT_ASSERT(cluster_replacement_wire_decode(bytes, &out));
	UT_ASSERT_EQ(memcmp(&out, &in, sizeof(in)), 0);
}


UT_TEST(test_codec_round_trips_all_four_phase_layouts)
{
	ClusterReplacementWireMessage in;
	ClusterReplacementWireMessage out;
	uint8 bytes[CLUSTER_REPLACEMENT_WIRE_BYTES];
	uint32 phase;

	for (phase = CLUSTER_REPLACEMENT_WIRE_PHASE_PURGE_REQUEST;
		 phase <= CLUSTER_REPLACEMENT_WIRE_PHASE_FAILSTOP_APPLICATION_ACK;
		 phase++) {
		in = make_valid_message(phase);
		memset(&out, 0, sizeof(out));
		UT_ASSERT(cluster_replacement_wire_encode(&in, bytes));
		UT_ASSERT(cluster_replacement_wire_decode(bytes, &out));
		UT_ASSERT_EQ(memcmp(&out, &in, sizeof(in)), 0);
	}
}


UT_TEST(test_phase3_maps_exact_generation_ready_and_zero_reserved)
{
	ClusterReplacementWireMessage in;
	ClusterReplacementWireMessage out;
	ClusterReplacementWireMessage before;
	uint8 bytes[CLUSTER_REPLACEMENT_WIRE_BYTES];
	uint8 unchanged[CLUSTER_REPLACEMENT_WIRE_BYTES];

	in = make_valid_message(
		CLUSTER_REPLACEMENT_WIRE_PHASE_TARGET_RECOVERY_READY);
	UT_ASSERT(cluster_replacement_wire_encode(&in, bytes));
	UT_ASSERT(test_get_le64(bytes + 48) == in.body.phase3.jcmk_generation);
	UT_ASSERT_EQ((int)test_get_le32(bytes + 56),
				 (int)in.body.phase3.episode_state_generation);
	UT_ASSERT_EQ((int)test_get_le32(bytes + 60), 0);
	memset(bytes, 0xA5, sizeof(bytes));
	memcpy(unchanged, bytes, sizeof(unchanged));
	in.body.phase3.reserved = 1;
	UT_ASSERT(!cluster_replacement_wire_encode(&in, bytes));
	UT_ASSERT_EQ(memcmp(bytes, unchanged, sizeof(bytes)), 0);
	in.body.phase3.reserved = 0;
	UT_ASSERT(cluster_replacement_wire_encode(&in, bytes));

	memset(&out, 0x5A, sizeof(out));
	before = out;
	bytes[60] = 1;
	UT_ASSERT(!cluster_replacement_wire_decode(bytes, &out));
	UT_ASSERT_EQ(memcmp(&out, &before, sizeof(out)), 0);
	bytes[60] = 0;
	memset(bytes + 56, 0, 4);
	UT_ASSERT(!cluster_replacement_wire_decode(bytes, &out));
	UT_ASSERT_EQ(memcmp(&out, &before, sizeof(out)), 0);
}


/* The encoder is the sole consumer of the instantaneous READY snapshot.  It
 * must preserve every identity while translating committed E to wire baseline
 * E-1; it cannot infer or retain readiness of its own. */
UT_TEST(test_phase3_snapshot_encoder_maps_exact_positive_snapshot)
{
	ClusterR4PrerequisiteSnapshot snapshot = make_ready_snapshot();
	ClusterReplacementWireMessage decoded;
	uint8 bytes[CLUSTER_REPLACEMENT_WIRE_BYTES];

	memset(bytes, 0xA5, sizeof(bytes));
	UT_ASSERT(cluster_replacement_wire_encode_phase3_snapshot(&snapshot, bytes));
	UT_ASSERT(cluster_replacement_wire_decode(bytes, &decoded));
	UT_ASSERT_EQ((int)decoded.phase,
				 (int)CLUSTER_REPLACEMENT_WIRE_PHASE_TARGET_RECOVERY_READY);
	UT_ASSERT_EQ(decoded.target_node_id, snapshot.target_node_id);
	UT_ASSERT(decoded.epoch == snapshot.committed_epoch - 1);
	UT_ASSERT(decoded.request_nonce == snapshot.request_nonce);
	UT_ASSERT(decoded.identity0 == snapshot.old_admitted_incarnation);
	UT_ASSERT(decoded.identity1 == snapshot.fresh_incarnation);
	UT_ASSERT(decoded.body.phase3.jcmk_generation == snapshot.jcmk_generation);
	UT_ASSERT_EQ((int)decoded.body.phase3.episode_state_generation,
				 (int)snapshot.episode_state_generation);
	UT_ASSERT_EQ((int)decoded.body.phase3.reserved, 0);
	UT_ASSERT(decoded.grammar_fingerprint == snapshot.grammar_fingerprint);
}


/* No non-canonical or non-positive snapshot may manufacture a wire proof, and
 * refusal must leave caller bytes untouched. */
UT_TEST(test_phase3_snapshot_encoder_rejects_without_output_mutation)
{
	ClusterR4PrerequisiteSnapshot snapshot = make_ready_snapshot();
	uint8 bytes[CLUSTER_REPLACEMENT_WIRE_BYTES];
	uint8 before[CLUSTER_REPLACEMENT_WIRE_BYTES];

#define ASSERT_SNAPSHOT_REJECTED()                                                        \
	do {                                                                                    \
		memset(bytes, 0xA5, sizeof(bytes));                                                   \
		memcpy(before, bytes, sizeof(before));                                                \
		UT_ASSERT(!cluster_replacement_wire_encode_phase3_snapshot(&snapshot, bytes));        \
		UT_ASSERT_EQ(memcmp(bytes, before, sizeof(bytes)), 0);                                \
	} while (0)

	snapshot.ready = false;
	ASSERT_SNAPSHOT_REJECTED();
	snapshot = make_ready_snapshot();
	snapshot.status = CLUSTER_R4_PREREQUISITE_RF_DEFERRED;
	ASSERT_SNAPSHOT_REJECTED();
	snapshot = make_ready_snapshot();
	snapshot.reserved0[1] = 1;
	ASSERT_SNAPSHOT_REJECTED();
	snapshot = make_ready_snapshot();
	snapshot.episode_state_generation = 0;
	ASSERT_SNAPSHOT_REJECTED();
	snapshot = make_ready_snapshot();
	snapshot.jcmk_generation = 0;
	ASSERT_SNAPSHOT_REJECTED();
	snapshot = make_ready_snapshot();
	snapshot.request_nonce = 0;
	ASSERT_SNAPSHOT_REJECTED();
	snapshot = make_ready_snapshot();
	snapshot.fresh_incarnation = snapshot.old_admitted_incarnation;
	ASSERT_SNAPSHOT_REJECTED();
	snapshot = make_ready_snapshot();
	snapshot.committed_epoch = 0;
	ASSERT_SNAPSHOT_REJECTED();
	snapshot = make_ready_snapshot();
	snapshot.grammar_fingerprint ^= UINT64_C(1);
	ASSERT_SNAPSHOT_REJECTED();
	UT_ASSERT(!cluster_replacement_wire_encode_phase3_snapshot(NULL, bytes));
	UT_ASSERT(!cluster_replacement_wire_encode_phase3_snapshot(&snapshot, NULL));

#undef ASSERT_SNAPSHOT_REJECTED
}


UT_TEST(test_decoder_rejects_unknown_layout_without_output)
{
	ClusterReplacementWireMessage in;
	ClusterReplacementWireMessage out;
	ClusterReplacementWireMessage before;
	uint8 bytes[CLUSTER_REPLACEMENT_WIRE_BYTES];

	in = make_valid_message(CLUSTER_REPLACEMENT_WIRE_PHASE_PURGE_ACK);
	UT_ASSERT(cluster_replacement_wire_encode(&in, bytes));
	memset(&out, 0xC3, sizeof(out));
	before = out;

	bytes[0] = 17;
	UT_ASSERT(!cluster_replacement_wire_decode(bytes, &out));
	UT_ASSERT_EQ(memcmp(&out, &before, sizeof(out)), 0);
	bytes[0] = 18;
	bytes[4] = 5;
	UT_ASSERT(!cluster_replacement_wire_decode(bytes, &out));
	UT_ASSERT_EQ(memcmp(&out, &before, sizeof(out)), 0);
	bytes[4] = CLUSTER_REPLACEMENT_WIRE_PHASE_PURGE_ACK;
	bytes[12] = 2;
	UT_ASSERT(!cluster_replacement_wire_decode(bytes, &out));
	UT_ASSERT_EQ(memcmp(&out, &before, sizeof(out)), 0);
	UT_ASSERT(!cluster_replacement_wire_decode(NULL, &out));
	UT_ASSERT_EQ(memcmp(&out, &before, sizeof(out)), 0);
	UT_ASSERT(!cluster_replacement_wire_decode(bytes, NULL));
}


UT_TEST(test_codec_rejects_grammar_fingerprint_drift)
{
	ClusterReplacementWireMessage in;
	ClusterReplacementWireMessage out;
	ClusterReplacementWireMessage before;
	uint8 bytes[CLUSTER_REPLACEMENT_WIRE_BYTES];

	in = make_valid_message(CLUSTER_REPLACEMENT_WIRE_PHASE_PURGE_REQUEST);
	in.grammar_fingerprint ^= UINT64_C(1);
	UT_ASSERT(!cluster_replacement_wire_encode(&in, bytes));

	in.grammar_fingerprint
		= CANDIDATE2_CORRECTED_A1_GRAMMAR_FINGERPRINT;
	UT_ASSERT(cluster_replacement_wire_encode(&in, bytes));
	bytes[64] ^= 1;
	memset(&out, 0xD4, sizeof(out));
	before = out;
	UT_ASSERT(!cluster_replacement_wire_decode(bytes, &out));
	UT_ASSERT_EQ(memcmp(&out, &before, sizeof(out)), 0);
}


/* Break caught: an opcode-18 frame from the wrong authenticated CONTROL
 * endpoint must never consume a bounded handoff slot. */
UT_TEST(test_phase3_ingress_rejects_wrong_endpoint_without_handoff_mutation)
{
	ClusterReplacementPhase3Handoff handoff;
	ClusterReplacementPhase3Handoff before;
	ClusterReplacementWireMessage message;
	ClusterICEnvelope envelope;
	uint8 bytes[CLUSTER_REPLACEMENT_WIRE_BYTES];

	cluster_replacement_phase3_handoff_init(&handoff);
	message = make_valid_message(
		CLUSTER_REPLACEMENT_WIRE_PHASE_TARGET_RECOVERY_READY);
	envelope = make_phase3_envelope(message.target_node_id, 1, message.epoch + 1);
	UT_ASSERT(cluster_replacement_wire_encode(&message, bytes));
	before = handoff;

	envelope.msg_type = PGRAC_IC_MSG_GES_REPLY;
	UT_ASSERT_EQ((int)ingress_phase3(&handoff, &envelope, bytes,
								 message.target_node_id, 1, message.epoch + 1, 7),
				 (int)CLUSTER_REPLACEMENT_PHASE3_INGRESS_REJECTED);
	UT_ASSERT_EQ(memcmp(&handoff, &before, sizeof(handoff)), 0);
	envelope.msg_type = PGRAC_IC_MSG_GES_REQUEST;

	UT_ASSERT_EQ((int)ingress_phase3(&handoff, &envelope, bytes,
								 message.target_node_id + 1, 1, message.epoch + 1, 7),
				 (int)CLUSTER_REPLACEMENT_PHASE3_INGRESS_REJECTED);
	UT_ASSERT_EQ(memcmp(&handoff, &before, sizeof(handoff)), 0);
	envelope.dest_node_id = 2;
	UT_ASSERT_EQ((int)ingress_phase3(&handoff, &envelope, bytes,
								 message.target_node_id, 1, message.epoch + 1, 7),
				 (int)CLUSTER_REPLACEMENT_PHASE3_INGRESS_REJECTED);
	UT_ASSERT_EQ(memcmp(&handoff, &before, sizeof(handoff)), 0);
}


/* Break caught: phase/layout drift must be rejected before the producer
 * sequence advances, including a valid non-phase-3 opcode-18 body. */
UT_TEST(test_phase3_ingress_rejects_wire_drift_without_handoff_mutation)
{
	ClusterReplacementPhase3Handoff handoff;
	ClusterReplacementPhase3Handoff before;
	ClusterReplacementWireMessage message;
	ClusterICEnvelope envelope;
	uint8 bytes[CLUSTER_REPLACEMENT_WIRE_BYTES];

	cluster_replacement_phase3_handoff_init(&handoff);
	message = make_valid_message(
		CLUSTER_REPLACEMENT_WIRE_PHASE_TARGET_RECOVERY_READY);
	envelope = make_phase3_envelope(message.target_node_id, 1, message.epoch + 1);
	UT_ASSERT(cluster_replacement_wire_encode(&message, bytes));
	before = handoff;

	bytes[12] = 2;
	UT_ASSERT_EQ((int)ingress_phase3(&handoff, &envelope, bytes,
								 message.target_node_id, 1, message.epoch + 1, 7),
				 (int)CLUSTER_REPLACEMENT_PHASE3_INGRESS_REJECTED);
	UT_ASSERT_EQ(memcmp(&handoff, &before, sizeof(handoff)), 0);
	bytes[12] = CLUSTER_REPLACEMENT_WIRE_VERSION;
	bytes[64] ^= 1;
	UT_ASSERT_EQ((int)ingress_phase3(&handoff, &envelope, bytes,
								 message.target_node_id, 1, message.epoch + 1, 7),
				 (int)CLUSTER_REPLACEMENT_PHASE3_INGRESS_REJECTED);
	UT_ASSERT_EQ(memcmp(&handoff, &before, sizeof(handoff)), 0);
	bytes[64] ^= 1;
	bytes[60] = 1;
	UT_ASSERT_EQ((int)ingress_phase3(&handoff, &envelope, bytes,
								 message.target_node_id, 1, message.epoch + 1, 7),
				 (int)CLUSTER_REPLACEMENT_PHASE3_INGRESS_REJECTED);
	UT_ASSERT_EQ(memcmp(&handoff, &before, sizeof(handoff)), 0);

	message = make_valid_message(CLUSTER_REPLACEMENT_WIRE_PHASE_PURGE_ACK);
	UT_ASSERT(cluster_replacement_wire_encode(&message, bytes));
	UT_ASSERT_EQ((int)ingress_phase3(&handoff, &envelope, bytes,
								 message.target_node_id, 1, message.epoch + 1, 7),
				 (int)CLUSTER_REPLACEMENT_PHASE3_INGRESS_REJECTED);
	UT_ASSERT_EQ(memcmp(&handoff, &before, sizeof(handoff)), 0);
}


/* Break caught: a valid phase-3 frame must preserve the authenticated source,
 * local receiver and CONTROL connection generation for the formation owner. */
UT_TEST(test_phase3_ingress_enqueues_only_observation_handoff)
{
	ClusterReplacementPhase3Handoff handoff;
	ClusterReplacementPhase3HandoffItem item;
	ClusterReplacementWireMessage message;
	ClusterICEnvelope envelope;
	uint8 bytes[CLUSTER_REPLACEMENT_WIRE_BYTES];

	cluster_replacement_phase3_handoff_init(&handoff);
	message = make_valid_message(
		CLUSTER_REPLACEMENT_WIRE_PHASE_TARGET_RECOVERY_READY);
	envelope = make_phase3_envelope(message.target_node_id, 1, message.epoch + 1);
	UT_ASSERT(cluster_replacement_wire_encode(&message, bytes));

	UT_ASSERT_EQ((int)ingress_phase3(&handoff, &envelope, bytes,
								 message.target_node_id, 1, message.epoch + 1, 7),
				 (int)CLUSTER_REPLACEMENT_PHASE3_INGRESS_ENQUEUED);
	UT_ASSERT_EQ((int)cluster_replacement_phase3_handoff_pending(&handoff), 1);
	memset(&item, 0xA5, sizeof(item));
	UT_ASSERT(cluster_replacement_phase3_handoff_poll(&handoff, &item));
	UT_ASSERT_EQ(memcmp(&item.message, &message, sizeof(message)), 0);
	UT_ASSERT_EQ(item.authenticated_source_node_id, message.target_node_id);
	UT_ASSERT_EQ(item.local_receiver_node_id, 1);
	UT_ASSERT_EQ((int)item.control_connection_generation, 7);
	UT_ASSERT_EQ((int)cluster_replacement_phase3_handoff_pending(&handoff), 0);
}


/* Break caught: phase 3 is sent after the replacement COMMIT has advanced the
 * cluster to baseline+1, while wire byte 16 still carries the episode's exact
 * baseline_epoch.  Treating that field as the outer/current epoch makes the
 * first normal-rollout READY observation unreachable. */
UT_TEST(test_phase3_ingress_maps_wire_baseline_to_outer_committed_epoch)
{
	ClusterReplacementPhase3Handoff handoff;
	ClusterReplacementWireMessage message;
	ClusterICEnvelope envelope;
	uint8 bytes[CLUSTER_REPLACEMENT_WIRE_BYTES];
	const uint64 baseline_epoch = UINT64_C(70);
	const uint64 committed_epoch = UINT64_C(71);

	cluster_replacement_phase3_handoff_init(&handoff);
	message = make_valid_message(
		CLUSTER_REPLACEMENT_WIRE_PHASE_TARGET_RECOVERY_READY);
	message.epoch = baseline_epoch;
	envelope = make_phase3_envelope(
		message.target_node_id, 1, committed_epoch);
	UT_ASSERT(cluster_replacement_wire_encode(&message, bytes));

	UT_ASSERT_EQ((int)ingress_phase3(
					 &handoff, &envelope, bytes, message.target_node_id, 1,
					 committed_epoch, 7),
				 (int)CLUSTER_REPLACEMENT_PHASE3_INGRESS_ENQUEUED);
	UT_ASSERT_EQ((int)cluster_replacement_phase3_handoff_pending(&handoff), 1);
}


/* Break caught: producer saturation must withhold without overwriting the
 * oldest observation or changing FIFO order. */
UT_TEST(test_phase3_handoff_is_bounded_fifo_without_overwrite)
{
	ClusterReplacementPhase3Handoff handoff;
	ClusterReplacementPhase3Handoff before_full;
	ClusterReplacementPhase3HandoffItem item;
	ClusterReplacementWireMessage message;
	ClusterICEnvelope envelope;
	uint8 bytes[CLUSTER_REPLACEMENT_WIRE_BYTES];
	uint32 i;

	cluster_replacement_phase3_handoff_init(&handoff);
	message = make_valid_message(
		CLUSTER_REPLACEMENT_WIRE_PHASE_TARGET_RECOVERY_READY);
	envelope = make_phase3_envelope(message.target_node_id, 1, message.epoch + 1);
	for (i = 0; i < CLUSTER_REPLACEMENT_PHASE3_HANDOFF_CAPACITY; i++) {
		message.request_nonce = UINT64_C(1000) + i;
		UT_ASSERT(cluster_replacement_wire_encode(&message, bytes));
		UT_ASSERT_EQ((int)ingress_phase3(&handoff, &envelope, bytes,
									 message.target_node_id, 1, message.epoch + 1, 7),
					 (int)CLUSTER_REPLACEMENT_PHASE3_INGRESS_ENQUEUED);
	}
	UT_ASSERT_EQ((int)cluster_replacement_phase3_handoff_pending(&handoff),
				 (int)CLUSTER_REPLACEMENT_PHASE3_HANDOFF_CAPACITY);
	before_full = handoff;
	message.request_nonce = UINT64_C(9999);
	UT_ASSERT(cluster_replacement_wire_encode(&message, bytes));
	UT_ASSERT_EQ((int)ingress_phase3(&handoff, &envelope, bytes,
								 message.target_node_id, 1, message.epoch + 1, 7),
				 (int)CLUSTER_REPLACEMENT_PHASE3_INGRESS_FULL);
	UT_ASSERT_EQ(memcmp(&handoff, &before_full, sizeof(handoff)), 0);

	UT_ASSERT(cluster_replacement_phase3_handoff_poll(&handoff, &item));
	UT_ASSERT(item.message.request_nonce == UINT64_C(1000));
}


UT_TEST(test_stop_observation_cannot_hide_malformed_or_wrapped_queue)
{
	ClusterReplacementPhase3Handoff handoff;
	cluster_replacement_phase3_handoff_init(&handoff);
	UT_ASSERT_EQ(cluster_replacement_phase3_handoff_observed_count(&handoff), 0);
	UT_ASSERT(cluster_replacement_phase3_handoff_observed_count(NULL)
			  > CLUSTER_REPLACEMENT_PHASE3_HANDOFF_CAPACITY);
	handoff.consumer_seq = UINT64_MAX;
	handoff.producer_seq = 0;
	UT_ASSERT_EQ(cluster_replacement_phase3_handoff_observed_count(&handoff), 1);
	handoff.consumer_seq = 0;
	handoff.producer_seq = CLUSTER_REPLACEMENT_PHASE3_HANDOFF_CAPACITY + 1;
	UT_ASSERT_EQ(cluster_replacement_phase3_handoff_pending(&handoff), 0);
	UT_ASSERT_EQ(cluster_replacement_phase3_handoff_observed_count(&handoff),
				 CLUSTER_REPLACEMENT_PHASE3_HANDOFF_CAPACITY + 1);
	UT_ASSERT_EQ(handoff.consumer_seq, 0);
}

int
main(void)
{
	UT_PLAN(13);
	UT_RUN(test_codec_is_exact_72_byte_little_endian);
	UT_RUN(test_codec_round_trips_all_four_phase_layouts);
	UT_RUN(test_phase3_maps_exact_generation_ready_and_zero_reserved);
	UT_RUN(test_phase3_snapshot_encoder_maps_exact_positive_snapshot);
	UT_RUN(test_phase3_snapshot_encoder_rejects_without_output_mutation);
	UT_RUN(test_decoder_rejects_unknown_layout_without_output);
	UT_RUN(test_codec_rejects_grammar_fingerprint_drift);
	UT_RUN(test_phase3_ingress_rejects_wrong_endpoint_without_handoff_mutation);
	UT_RUN(test_phase3_ingress_rejects_wire_drift_without_handoff_mutation);
	UT_RUN(test_phase3_ingress_enqueues_only_observation_handoff);
	UT_RUN(test_phase3_ingress_maps_wire_baseline_to_outer_committed_epoch);
	UT_RUN(test_phase3_handoff_is_bounded_fifo_without_overwrite);
	UT_RUN(test_stop_observation_cannot_hide_malformed_or_wrapped_queue);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
