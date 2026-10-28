/*
 * Actual protected GCS CLOSE ingress and shared delegation lifecycle.
 * Socket, HELLO, membership and R4 providers are controlled process boundaries.
 * No native transport, concurrency or physical disk durability claim.
 */
#define CTRC_DISPATCH_MAIN ctrc_dispatch_original_main
#include "test_cluster_ctrc_dispatch.c"
#undef CTRC_DISPATCH_MAIN
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_ic_tier1.h"

#define CLOSE_TEST_BYTES 216
static unsigned close_replies;
static uint8 close_reply[GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE];
static uint32 close_source_generation = 29;
static int32 close_expected_destination = 1;

int
cluster_ic_tier1_my_data_channel(void)
{
	return 0;
}

bool
cluster_sf_peer_multixact_current_capability_generation(int32 peer pg_attribute_unused(),
	uint32 *generation)
{
	*generation = close_source_generation;
	return close_source_generation != 0;
}

ClusterICSendResult
cluster_ic_send_envelope(uint8 type, int32 destination, const void *payload, uint32 length)
{
	if (type != PGRAC_IC_MSG_GCS_BLOCK_REPLY || destination != close_expected_destination
		|| length != sizeof(close_reply) || held_count != 0)
		abort();
	memcpy(close_reply, payload, length);
	close_replies++;
	return CLUSTER_IC_SEND_DONE;
}

void *
palloc0(Size size)
{
	return palloc_extended(size, MCXT_ALLOC_ZERO);
}

static ClusterICSendResult
gcs_block_send_envelope_or_loopback(uint8 type, int32 destination,
	const void *payload, uint32 length)
{
	return cluster_ic_send_envelope(type, destination, payload, length);
}

void
cluster_gcs_block_note_send_outcome(GcsBlockSendFamily family pg_attribute_unused(),
	ClusterICSendResult result pg_attribute_unused())
{
}

#include "test_cluster_ctrc_delegation_close.inc"
#include "test_cluster_ctrc_delegation_forward.inc"
#include "test_cluster_ctrc_delegation_reply.inc"

static void
close_test_identity(const ClusterCtrcParticipantIdentity *identity, uint8 *bytes)
{
	ctrc_put_u16_le(bytes, identity->node_id);
	ctrc_put_u16_le(bytes + 2, identity->reserved16);
	ctrc_put_u32_le(bytes + 4, identity->capability_record_generation);
	ctrc_put_u64_le(bytes + 8, identity->boot_incarnation);
	ctrc_put_u64_le(bytes + 16, identity->formation_epoch);
	ctrc_put_u64_le(bytes + 24, identity->admission_record_generation);
}

/* Build the declared new wire independently with the original V2 encoder.
 * This lets the exact parent GCS function run the new behavioral RED. */
static bool
close_test_setup(uint8 bytes[CLOSE_TEST_BYTES], ClusterICEnvelope *env,
	ClusterCtrcParticipantEntry **receiver_out)
{
	ClusterCtrcReceiptHandle source;
	ClusterCtrcParticipantIdentity receiver;
	ClusterItlSlotData slot;
	uint8 record[224];
	uint64 index;

	reset_dispatch_fixture();
	if (!seed_delegation_receive(&source, &receiver, &slot, record, &index)
		|| !cluster_ctrc_itl_delegate_accept_v1(&slot, &source.participant->identity,
			&receiver, record, sizeof(record)))
		return false;
	*receiver_out = &ctrc_participant_entries()[index];
	MemSet(bytes, 0, CLOSE_TEST_BYTES);
	if (!cluster_ctrc_seal_request_encode(&source.key, 17001,
		source.participant->grant_generation, 41, receiver.capability_record_generation,
		CTRC_SEAL_CLOSE_AND_CLEAN, bytes, CLUSTER_CTRC_SEAL_REQUEST_BYTES))
		return false;
	ctrc_put_u32_le(bytes + 36, source.participant->identity.node_id);
	ctrc_put_u16_le(bytes + 68, 3);
	ctrc_put_u16_le(bytes + 70, CLOSE_TEST_BYTES);
	close_test_identity(&source.participant->identity, bytes + 136);
	close_test_identity(&receiver, bytes + 168);
	ctrc_put_u64_le(bytes + 200, (*receiver_out)->delegation_parent.sequence);
	ctrc_put_u64_le(bytes + 208, U64FromFullTransactionId(source.original_full_xid));
	MemSet(env, 0, sizeof(*env));
	env->msg_type = PGRAC_IC_MSG_GCS_BLOCK_FORWARD;
	env->source_node_id = source.participant->identity.node_id;
	env->dest_node_id = receiver.node_id;
	env->epoch = source.key.cluster_epoch;
	env->payload_length = CLOSE_TEST_BYTES;
	dispatch_self_boot = receiver.boot_incarnation;
	close_replies = 0;
	close_source_generation = 29;
	close_expected_destination = 1;
	MemSet(close_reply, 0, sizeof(close_reply));
	return true;
}

UT_TEST(test_actual_delegated_close_enters_drain_and_retains_parent)
{
	uint8 bytes[CLOSE_TEST_BYTES];
	ClusterICEnvelope env;
	ClusterCtrcParticipantEntry *receiver, before;
	bool handled;

	UT_ASSERT(close_test_setup(bytes, &env, &receiver));
	before = *receiver;
	handled = gcs_block_try_ctrc_forward136(&env, bytes);
	UT_ASSERT(handled);
	UT_ASSERT_EQ(receiver->state, CTRC_PARTICIPANT_CLOSED_DRAINING);
	UT_ASSERT_EQ(receiver->seal_generation, 41);
	UT_ASSERT(memcmp(&receiver->delegation_parent, &before.delegation_parent,
		sizeof(before.delegation_parent)) == 0);
	UT_ASSERT(FullTransactionIdEquals(receiver->original_full_xid, before.original_full_xid));
	UT_ASSERT_EQ(close_replies, 1);
	UT_ASSERT_EQ(enters, leaves);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(allocated, 0);
}

UT_TEST(test_actual_delegated_close_malformed_is_consumed_without_mutation)
{
	uint8 bytes[CLOSE_TEST_BYTES];
	ClusterICEnvelope env;
	ClusterCtrcParticipantEntry *receiver, before;

	UT_ASSERT(close_test_setup(bytes, &env, &receiver));
	bytes[68] = 99;
	before = *receiver;
	UT_ASSERT(gcs_block_try_ctrc_forward136(&env, bytes));
	UT_ASSERT(memcmp(receiver, &before, sizeof(before)) == 0);
	UT_ASSERT_EQ(close_replies, 0);
	UT_ASSERT_EQ(enters, 0);
}

UT_TEST(test_delegated_close_codec_is_explicit_and_roundtrips_all_binding_fields)
{
	uint8 bytes[CLOSE_TEST_BYTES], encoded[CLOSE_TEST_BYTES];
	ClusterICEnvelope env;
	ClusterCtrcParticipantEntry *receiver;
	ClusterCtrcDelegationCloseV1 decoded;
	ClusterCtrcSealRequestV1 old_request;
	ClusterCtrcTxnKeyV1 old_key;

	UT_ASSERT_EQ(CLOSE_TEST_BYTES, CLUSTER_CTRC_DELEGATION_CLOSE_V1_BYTES);
	UT_ASSERT(close_test_setup(bytes, &env, &receiver));
	UT_ASSERT(cluster_ctrc_delegation_close_decode_v1(bytes, sizeof(bytes), 42, 1, 2, 23, &decoded));
	UT_ASSERT(cluster_ctrc_delegation_close_encode_v1(&decoded, encoded, sizeof(encoded)));
	UT_ASSERT(memcmp(bytes, encoded, sizeof(bytes)) == 0);
	UT_ASSERT(memcmp(&decoded.key, &receiver->key, sizeof(decoded.key)) == 0);
	UT_ASSERT(memcmp(&decoded.receiver, &receiver->identity, sizeof(decoded.receiver)) == 0);
	UT_ASSERT(memcmp(&decoded.parent, &receiver->delegation_parent.receiver, sizeof(decoded.parent)) == 0);
	UT_ASSERT_EQ(decoded.delegation_sequence, receiver->delegation_parent.sequence);
	UT_ASSERT(FullTransactionIdEquals(decoded.original_full_xid, receiver->original_full_xid));
	UT_ASSERT(!cluster_ctrc_seal_request_decode(bytes, sizeof(bytes), 42, 1, 2, 23,
		&old_request, &old_key));
	UT_ASSERT(ctrc_bytes_zero(&old_request, sizeof(old_request)));
	UT_ASSERT(ctrc_bytes_zero(&old_key, sizeof(old_key)));
	decoded.parent.reserved16 = 1;
	MemSet(encoded, 0x5a, sizeof(encoded));
	UT_ASSERT(!cluster_ctrc_delegation_close_encode_v1(&decoded, encoded, sizeof(encoded)));
	UT_ASSERT(ctrc_bytes_zero(encoded, sizeof(encoded)));
}

UT_TEST(test_delegated_close_codec_rejects_malformed_without_partial_output)
{
	/* Unknown versions, the old length, reserved fields, identity and finite
	 * FullXid shape are rejected before any participant lookup or mutation. */
	static const unsigned zero_offsets[] = {0, 18, 20, 28, 40, 48, 54, 62,
		63, 64, 72, 80, 88, 96, 104, 112, 120, 128, 140, 144, 172, 176, 200, 208};
	for (unsigned fault = 0; fault < lengthof(zero_offsets) + 22; fault++) {
		uint8 bytes[CLOSE_TEST_BYTES + 1];
		ClusterICEnvelope env;
		ClusterCtrcParticipantEntry *receiver, before;
		ClusterCtrcDelegationCloseV1 decoded;
		Size length = CLOSE_TEST_BYTES;
		int source = 1, local = 2;
		uint64 epoch = 23, system = 42;

		UT_ASSERT(close_test_setup(bytes, &env, &receiver));
		before = *receiver;
		if (fault < lengthof(zero_offsets))
			MemSet(bytes + zero_offsets[fault], 0, 2);
		else switch (fault - lengthof(zero_offsets)) {
			case 0: bytes[32] = 1; break;
			case 1: bytes[44] = 1; break;
			case 2: bytes[60] = 1; break;
			case 3: bytes[132] = 1; break;
			case 4: bytes[138] = 1; break;
			case 5: bytes[170] = 1; break;
			case 6: bytes[68] = 2; break;
			case 7: bytes[68] = 4; break;
			case 8: bytes[70]--; break;
			case 9: bytes[55] = CTRC_SEAL_CERTIFICATE_COMMITTED; break;
			case 10: bytes[36]++; break;
			case 11: bytes[136]++; break;
			case 12: bytes[168]++; break;
			case 13: bytes[152]++; break;
			case 14: bytes[192]++; break;
			case 15: ctrc_put_u64_le(bytes + 208, UINT64_MAX); break;
			case 16: bytes[200] = CLUSTER_CTRC_MAX_PARTICIPANTS; break;
			case 17: length--; break;
			case 18: length++; break;
			case 19: source = 3; break;
			case 20: local = 3; break;
			case 21: epoch++; break;
		}
		MemSet(&decoded, 0x5a, sizeof(decoded));
		UT_ASSERT(!cluster_ctrc_delegation_close_decode_v1(bytes, length, system, source, local,
			epoch, &decoded));
		UT_ASSERT(ctrc_bytes_zero(&decoded, sizeof(decoded)));
		UT_ASSERT(memcmp(receiver, &before, sizeof(before)) == 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_delegated_close_shared_requires_the_accepted_parent_not_a_merge_or_raw_key)
{
	for (unsigned fault = 0; fault < 17; fault++) {
		uint8 bytes[CLOSE_TEST_BYTES];
		ClusterICEnvelope env;
		ClusterCtrcParticipantEntry *receiver, before;
		ClusterCtrcDelegationCloseV1 request;
		uint16 reason;

		UT_ASSERT(close_test_setup(bytes, &env, &receiver));
		UT_ASSERT(cluster_ctrc_delegation_close_decode_v1(bytes, sizeof(bytes), 42, 1, 2, 23, &request));
		switch (fault) {
			case 0: request.parent.boot_incarnation++; break;
			case 1: request.parent.capability_record_generation++; break;
			case 2: request.parent.node_id = 3; break;
			case 3: request.delegation_sequence++; break;
			case 4: request.receiver.boot_incarnation++; break;
			case 5: request.grant_generation++; break;
			case 6: request.key.root_generation++; break;
			case 7: request.key.system_identifier++; break;
			case 8: request.original_full_xid = FullTransactionIdFromEpochAndXid(8, request.key.xid); break;
			case 9: receiver->original_full_xid = InvalidFullTransactionId; break;
			case 10: MemSet(receiver->original_identity_v2, 0, 144); break;
			case 11: MemSet(&receiver->delegation_parent, 0, sizeof(receiver->delegation_parent)); break;
			case 12: MemSet(receiver, 0, sizeof(*receiver)); break;
			case 13: request.reserved = 1; break;
			case 14: cluster_node_id = 3; break;
			case 15: request.key.slot_wrap = 0; break;
			case 16: request.key.segment_generation = 0; break;
		}
		before = *receiver;
		UT_ASSERT_EQ(cluster_ctrc_participant_delegated_close_shared(&request, &reason),
			CTRC_SEAL_REPLY_BLOCKED_RETAIN);
		UT_ASSERT_EQ(reason, CTRC_SEAL_REASON_IDENTITY);
		UT_ASSERT(memcmp(receiver, &before, sizeof(before)) == 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_actual_delegated_close_current_sources_and_target_are_required)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		uint8 bytes[CLOSE_TEST_BYTES];
		ClusterICEnvelope env;
		ClusterCtrcParticipantEntry *receiver, before;

		UT_ASSERT(close_test_setup(bytes, &env, &receiver));
		switch (fault) {
			case 0: close_source_generation = 0; break;
			case 1: close_source_generation++; break;
			case 2: bytes[144]++; break;
			case 3: bytes[176]++; break;
			case 4: bytes[140]++; break;
			case 5: bytes[172]++; ctrc_put_u32_le(bytes + 128, 30); break;
			case 6: root_exact = false; break;
			case 7: fence_ok = false; break;
			case 8: fail_recheck = 1; break;
			case 9: data_plane = false; break;
		}
		before = *receiver;
		UT_ASSERT(gcs_block_try_ctrc_forward136(&env, bytes));
		UT_ASSERT(memcmp(receiver, &before, sizeof(before)) == 0);
		UT_ASSERT_EQ(enters, leaves);
		UT_ASSERT_EQ(held_count, 0);
		UT_ASSERT_EQ(allocated, 0);
	}
}

/* Exercise the same locked pending-custody kernel without claiming a new
 * physical IMAGE was shipped from this zero-receipt receiver. */
static bool
close_test_pending_child(ClusterCtrcParticipantEntry *participant,
	const ClusterCtrcParticipantIdentity *child, uint8 *path, Size *length)
{
	uint64 index;
	bool prepared;

	if (!ctrc_participant_index(&participant->key, participant->identity.node_id, &index))
		return false;
	ctrc_participant_locks_acquire(ctrc_participant_lock_mask(index));
	prepared = ctrc_participant_delegate_path_prepare_locked(participant,
		&participant->identity, child, path, CLUSTER_CTRC_DELEGATION_V2_MAX_BYTES, length);
	ctrc_participant_locks_release(ctrc_participant_lock_mask(index));
	return prepared;
}

UT_TEST(test_delegated_close_duplicate_keeps_downstream_stub_and_blocks_all_release)
{
	uint8 bytes[CLOSE_TEST_BYTES], path[CLUSTER_CTRC_DELEGATION_V2_MAX_BYTES];
	ClusterICEnvelope env;
	ClusterCtrcParticipantEntry *receiver, closed;
	ClusterCtrcParticipantIdentity child;
	ClusterCtrcDurability durability = { 0 };
	ClusterCtrcLocalReleaseAckV1 ack;
	uint16 reason;
	Size path_length;

	UT_ASSERT(close_test_setup(bytes, &env, &receiver));
	child = receiver->identity;
	child.node_id = 3;
	child.boot_incarnation = 1104;
	UT_ASSERT(close_test_pending_child(receiver, &child, path, &path_length));
	UT_ASSERT(gcs_block_try_ctrc_forward136(&env, bytes));
	closed = *receiver;
	UT_ASSERT_EQ(closed.state, CTRC_PARTICIPANT_CLOSED_DRAINING);
	UT_ASSERT_EQ(closed.delegation_count, 1);
	UT_ASSERT(gcs_block_try_ctrc_forward136(&env, bytes));
	UT_ASSERT(memcmp(receiver, &closed, sizeof(closed)) == 0);
	UT_ASSERT(!close_test_pending_child(receiver, &child, path, &path_length));
	UT_ASSERT(memcmp(receiver, &closed, sizeof(closed)) == 0);
	UT_ASSERT_EQ(cluster_ctrc_participant_request_shared(&receiver->key, &receiver->identity,
		receiver->grant_generation, 41, CTRC_SEAL_CERTIFICATE_COMMITTED, &reason, &ack),
		CTRC_SEAL_REPLY_BLOCKED_RETAIN);
	UT_ASSERT(memcmp(receiver, &closed, sizeof(closed)) == 0);
	UT_ASSERT_EQ(cluster_ctrc_participant_build_ack(receiver, &durability, &ack), CLUSTER_CTRC_ACK_DENIED);
	UT_ASSERT(ctrc_bytes_zero(&ack, sizeof(ack)));
	UT_ASSERT_EQ(enters, leaves);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_delegated_close_replies_bind_the_whole_request_and_never_grant_release)
{
	uint8 bytes[CLOSE_TEST_BYTES], page[BLCKSZ];
	ClusterICEnvelope env;
	ClusterCtrcParticipantEntry *receiver;
	ClusterCtrcSealReplyHeaderV1 header;
	ClusterCtrcLocalReleaseAckV1 ack;

	UT_ASSERT(close_test_setup(bytes, &env, &receiver));
	UT_ASSERT(gcs_block_try_ctrc_forward136(&env, bytes));
	memcpy(page, close_reply + sizeof(GcsBlockReplyHeader), sizeof(page));
	UT_ASSERT(cluster_ctrc_seal_reply_decode(page, sizeof(page), bytes, sizeof(bytes), 2, 1,
		&header, &ack));
	UT_ASSERT_EQ(header.wire_version, CLUSTER_CTRC_DELEGATION_CLOSE_WIRE_VERSION);
	UT_ASSERT_EQ(header.result, CTRC_SEAL_REPLY_PENDING_DRAIN);
	UT_ASSERT_EQ(header.ack_length, 0);
	UT_ASSERT(ctrc_bytes_zero(&ack, sizeof(ack)));
	bytes[208] ^= 1;
	UT_ASSERT(!cluster_ctrc_seal_reply_decode(page, sizeof(page), bytes, sizeof(bytes), 2, 1,
		&header, &ack));
	bytes[208] ^= 1;
	page[40] = CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK;
	ctrc_put_u16_le(page + 42, 0);
	ctrc_put_u16_le(page + 46, CLUSTER_CTRC_LOCAL_ACK_BYTES);
	ctrc_put_u32_le(page + 60, ctrc_crc32c_exact(page, 60));
	UT_ASSERT(!cluster_ctrc_seal_reply_decode(page, sizeof(page), bytes, sizeof(bytes), 2, 1,
		&header, &ack));
	UT_ASSERT(!cluster_ctrc_seal_reply_encode(bytes, sizeof(bytes), 2, 1,
		CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK, 0, &ack, page, sizeof(page)));
	UT_ASSERT(!cluster_ctrc_seal_reply_encode(bytes, sizeof(bytes), 2, 1,
		CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED, 0, NULL, page, sizeof(page)));
	UT_ASSERT(ctrc_bytes_zero(page, sizeof(page)));
}

UT_TEST(test_actual_delegated_close_error_leaves_original_admission_and_retains_custody)
{
	for (unsigned phase = 0; phase < 2; phase++) {
		uint8 bytes[CLOSE_TEST_BYTES];
		ClusterICEnvelope env;
		ClusterCtrcParticipantEntry *receiver, before;
		volatile bool caught = false;

		UT_ASSERT(close_test_setup(bytes, &env, &receiver));
		before = *receiver;
		if (phase == 0)
			throw_resolve = 1;
		else
			wakeup_hook = throw_from_participant_wakeup;
		PG_TRY();
		{
			(void)gcs_block_try_ctrc_forward136(&env, bytes);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		wakeup_hook = NULL;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(enters, leaves);
		UT_ASSERT_EQ(held_count, 0);
		UT_ASSERT_EQ(allocated, 0);
		UT_ASSERT(memcmp(&receiver->delegation_parent, &before.delegation_parent,
			sizeof(before.delegation_parent)) == 0);
		if (phase == 0)
			UT_ASSERT(memcmp(receiver, &before, sizeof(before)) == 0);
		else
			UT_ASSERT_EQ(receiver->state, CTRC_PARTICIPANT_CLOSED_DRAINING);
	}
}


UT_TEST(test_actual_multihop_close_keeps_the_complete_accepted_path)
{
	ClusterCtrcReceiptHandle first, middle;
	ClusterCtrcParticipantIdentity destination;
	ClusterCtrcItlTargetIdentity target;
	ClusterItlSlotData slot;
	ClusterCtrcParticipantEntry *participant, before;
	ClusterCtrcDelegationCloseV1 request = { 0 };
	ClusterICEnvelope env = { 0 };
	uint8 record[CLUSTER_CTRC_DELEGATION_V2_MAX_BYTES], bytes[CLOSE_TEST_BYTES];
	Size length = 0;
	uint64 index;

	reset_dispatch_fixture();
	UT_ASSERT(seed_multihop_source(&first, &middle, &destination, &slot, &target));
	UT_ASSERT(cluster_ctrc_itl_delegate_prepare_v2(&target, &slot,
		&middle.participant->identity, &destination, record, sizeof(record), &length));
	if (length == 0)
		return;
	cluster_node_id = destination.node_id;
	UT_ASSERT(cluster_ctrc_itl_delegate_accept_v2(&slot, &middle.participant->identity,
		&destination, record, length));
	UT_ASSERT(ctrc_participant_index(&middle.key, destination.node_id, &index));
	participant = &ctrc_participant_entries()[index];
	UT_ASSERT_EQ(participant->delegation_path.hop_count, 2);
	before = *participant;
	request.key = participant->key;
	request.parent = participant->delegation_parent.receiver;
	request.receiver = participant->identity;
	request.original_full_xid = participant->original_full_xid;
	request.request_id = 17002;
	request.grant_generation = participant->grant_generation;
	request.seal_generation = 41;
	request.delegation_sequence = participant->delegation_parent.sequence;
	UT_ASSERT(cluster_ctrc_delegation_close_encode_v1(&request, bytes, sizeof(bytes)));
	env.msg_type = PGRAC_IC_MSG_GCS_BLOCK_FORWARD;
	env.source_node_id = request.parent.node_id;
	env.dest_node_id = request.receiver.node_id;
	env.epoch = request.key.cluster_epoch;
	env.payload_length = sizeof(bytes);
	dispatch_self_boot = request.receiver.boot_incarnation;
	close_source_generation = request.parent.capability_record_generation;
	close_expected_destination = request.parent.node_id;
	close_replies = 0;
	UT_ASSERT(gcs_block_try_ctrc_forward136(&env, bytes));
	UT_ASSERT_EQ(participant->state, CTRC_PARTICIPANT_CLOSED_DRAINING);
	UT_ASSERT_EQ(close_replies, 1);
	UT_ASSERT(memcmp(&participant->delegation_path, &before.delegation_path,
		sizeof(before.delegation_path)) == 0);
	UT_ASSERT(memcmp(participant->original_identity_v2, before.original_identity_v2, 144) == 0);
	UT_ASSERT(memcmp(&participant->delegation_parent, &before.delegation_parent,
		sizeof(before.delegation_parent)) == 0);
	UT_ASSERT_EQ(enters, leaves);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(allocated, 0);
}

UT_TEST(test_actual_delegated_close_keeps_connection_and_admission_domains_separate)
{
	uint8 bytes[CLOSE_TEST_BYTES];
	ClusterICEnvelope env;
	ClusterCtrcParticipantEntry *receiver;

	UT_ASSERT(close_test_setup(bytes, &env, &receiver));
	UT_ASSERT_EQ(receiver->delegation_parent.receiver.capability_record_generation, 29);
	/* The actual Resource-X publisher uses OPEN admission 29 for custody,
	 * independently of the current HELLO connection generation 7. */
	dispatch_peer_connection_generation = close_source_generation = 7;
	UT_ASSERT(gcs_block_try_ctrc_forward136(&env, bytes));
	UT_ASSERT_EQ(receiver->state, CTRC_PARTICIPANT_CLOSED_DRAINING);
	UT_ASSERT_EQ(close_replies, 1);
	UT_ASSERT_EQ(enters, leaves);
	UT_ASSERT_EQ(held_count, 0);
}

static ClusterCtrcDelegationCloseV1 forward_captured;
static uint32 forward_connection;
static bool forward_accept = true;
static bool forward_throw;

static bool
close_test_capture_outbound(int worker, uint8 type, uint32 destination,
	const void *payload, uint16 bytes, uint32 required, uint32 connection)
{
	if (held_count != 0 || worker != 0 || type != PGRAC_IC_MSG_GCS_BLOCK_FORWARD
		|| destination != 3 || bytes != CLUSTER_CTRC_DELEGATION_CLOSE_V1_BYTES
		|| (required & PGRAC_IC_HELLO_CAP_MULTIXACT_CTRC_V1) == 0
		|| !cluster_ctrc_delegation_close_decode_v1(payload, bytes, 42, 2, 3, 23, &forward_captured))
		abort();
	forward_connection = connection;
	enqueues++;
	if (forward_throw)
		pg_re_throw();
	return forward_accept;
}

UT_TEST(test_original_background_pass_forwards_closed_child_custody)
{
	uint8 bytes[CLOSE_TEST_BYTES], path[CLUSTER_CTRC_DELEGATION_V2_MAX_BYTES];
	ClusterICEnvelope env;
	ClusterCtrcParticipantEntry *receiver;
	ClusterCtrcParticipantIdentity child;
	Size length = 0;
	uint32 first;

	UT_ASSERT(close_test_setup(bytes, &env, &receiver));
	child = receiver->identity;
	child.node_id = 3;
	child.boot_incarnation = 1104;
	UT_ASSERT(close_test_pending_child(receiver, &child, path, &length));
	UT_ASSERT(gcs_block_try_ctrc_forward136(&env, bytes));
	first = ((uint32)receiver->key.owner_instance - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	UT_ASSERT(cluster_ctrc_cleaner_bind_worker(
		(receiver->key.segment_id - first) % CLUSTER_CTRC_CLEANER_WORKERS));
	enqueues = 0;
	dispatch_enqueue_hook = close_test_capture_outbound;
	driver_delegation_dispatch = cluster_gcs_ctrc_dispatch_delegated_close;
	(void)ctrc_fixture_run_pass();
	UT_ASSERT_EQ(enqueues, 1);
	UT_ASSERT_EQ(receiver->state, CTRC_PARTICIPANT_CLOSED_DRAINING);
	UT_ASSERT_EQ(receiver->delegation_count, 1);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT(!CtrcBatch.active);
}


/* Controlled original queue/HELLO/OS boundaries; the selector, identity
 * matcher, dispatcher, admission and cleaner finally below are production C. */
static bool
close_forward_setup(ClusterCtrcParticipantEntry **participant)
{
	uint8 bytes[CLOSE_TEST_BYTES], path[CLUSTER_CTRC_DELEGATION_V2_MAX_BYTES];
	ClusterICEnvelope env;
	ClusterCtrcParticipantIdentity child;
	Size length = 0;
	uint32 first;

	fixture_declared_nodes = 4;
	if (!close_test_setup(bytes, &env, participant))
		return false;
	child = (*participant)->identity;
	child.node_id = 3;
	child.boot_incarnation = 1104;
	if (!close_test_pending_child(*participant, &child, path, &length)
		|| !gcs_block_try_ctrc_forward136(&env, bytes))
		return false;
	first = ((uint32)(*participant)->key.owner_instance - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	if (!cluster_ctrc_cleaner_bind_worker(
		((*participant)->key.segment_id - first) % CLUSTER_CTRC_CLEANER_WORKERS))
		return false;
	enqueues = 0;
	forward_connection = 0;
	forward_accept = true;
	forward_throw = false;
	MemSet(&forward_captured, 0, sizeof(forward_captured));
	dispatch_enqueue_hook = close_test_capture_outbound;
	driver_delegation_dispatch = cluster_gcs_ctrc_dispatch_delegated_close;
	driver_other_progress = driver_interrupt = false;
	return true;
}

UT_TEST(test_background_close_refusal_retries_the_same_request_without_retiring_custody)
{
	ClusterCtrcParticipantEntry *participant, expected;
	ClusterCtrcDelegationCloseV1 first;
	uint64 next_request, progress;

	UT_ASSERT(close_forward_setup(&participant));
	expected = *participant;
	next_request = CtrcShared->global_request_generation;
	progress = cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS);
	forward_accept = false;
	(void)ctrc_fixture_run_pass();
	UT_ASSERT_EQ(enqueues, 1);
	first = forward_captured;
	UT_ASSERT_EQ(first.request_id, next_request);
	UT_ASSERT_EQ(first.parent.node_id, 2);
	UT_ASSERT_EQ(first.receiver.node_id, 3);
	UT_ASSERT_EQ(first.seal_generation, 41);
	UT_ASSERT_EQ(first.delegation_sequence, participant->delegations[3].sequence);
	expected.delegation_close_request_id[3] = next_request;
	UT_ASSERT(memcmp(participant, &expected, sizeof(expected)) == 0);
	forward_accept = true;
	(void)ctrc_fixture_run_pass();
	UT_ASSERT_EQ(enqueues, 2);
	UT_ASSERT(memcmp(&forward_captured, &first, sizeof(first)) == 0);
	UT_ASSERT_EQ(CtrcShared->global_request_generation, next_request + 1);
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS), progress);
	UT_ASSERT(memcmp(participant, &expected, sizeof(expected)) == 0);
	UT_ASSERT_EQ(enters, leaves);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT(!CtrcBatch.active);
}

UT_TEST(test_background_selector_rejects_unqualified_or_exhausted_rows_without_partial_output)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		ClusterCtrcParticipantEntry *participant, before;
		ClusterCtrcDelegationCloseV1 request;
		uint64 next_request;

		UT_ASSERT(close_forward_setup(&participant));
		switch (fault) {
			case 0: participant->state = CTRC_PARTICIPANT_OPEN; break;
			case 1: participant->original_full_xid = InvalidFullTransactionId; break;
			case 2: participant->original_identity_v2[143] ^= 1; break;
			case 3: participant->delegations[3].sequence = 0; break;
			case 4: participant->delegation_close_request_id[3] = UINT64_MAX; break;
			case 5: participant->delegation_close_request_id[0] = 7; break;
			case 6: participant->state = CTRC_PARTICIPANT_BLOCKED; break;
			case 7: CtrcShared->global_request_generation = UINT64_MAX; break;
			case 8: cluster_node_id = 3; break;
			case 9: participant->key.reserved[0] = 1; break;
		}
		before = *participant;
		next_request = CtrcShared->global_request_generation;
		MemSet(&request, 0xa5, sizeof(request));
		UT_ASSERT(!cluster_ctrc_participant_next_delegated_close_shared(&request));
		UT_ASSERT(ctrc_bytes_zero(&request, sizeof(request)));
		UT_ASSERT(memcmp(participant, &before, sizeof(before)) == 0);
		UT_ASSERT_EQ(CtrcShared->global_request_generation, next_request);
		UT_ASSERT_EQ(enqueues, 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_background_dispatch_requires_current_hello_admission_root_and_exact_custody)
{
	for (unsigned fault = 0; fault < 13; fault++) {
		ClusterCtrcParticipantEntry *participant, before;
		ClusterCtrcDelegationCloseV1 request;

		UT_ASSERT(close_forward_setup(&participant));
		UT_ASSERT(cluster_ctrc_participant_next_delegated_close_shared(&request));
		switch (fault) {
			case 0: close_source_generation = 0; break;
			case 1: close_source_generation = 7; break;
			case 2: dispatch_self_boot++; break;
			case 3: request.receiver.boot_incarnation++; break;
			case 4: request.parent.capability_record_generation++; break;
			case 5: request.receiver.capability_record_generation++; break;
			case 6: root_exact = false; break;
			case 7: fence_ok = false; break;
			case 8: fail_recheck = rechecks + 1; break;
			case 9: fail_recheck = rechecks + 2; break;
			case 10: data_plane = false; break;
			case 11: participant->delegations[3].receiver.boot_incarnation++; break;
			case 12: request.request_id++; break;
		}
		before = *participant;
		UT_ASSERT(!cluster_gcs_ctrc_dispatch_delegated_close(&request));
		UT_ASSERT_EQ(enqueues, 0);
		UT_ASSERT(memcmp(participant, &before, sizeof(before)) == 0);
		UT_ASSERT_EQ(enters, leaves);
		UT_ASSERT_EQ(held_count, 0);
		UT_ASSERT_EQ(allocated, 0);
	}
}

UT_TEST(test_background_dispatch_uses_hello_connection_and_preserves_open_custody_generation)
{
	ClusterCtrcParticipantEntry *participant, before;
	ClusterCtrcDelegationCloseV1 request;

	UT_ASSERT(close_forward_setup(&participant));
	UT_ASSERT(cluster_ctrc_participant_next_delegated_close_shared(&request));
	before = *participant;
	dispatch_peer_connection_generation = close_source_generation = 7;
	UT_ASSERT(cluster_gcs_ctrc_dispatch_delegated_close(&request));
	UT_ASSERT_EQ(enqueues, 1);
	UT_ASSERT_EQ(forward_connection, 7);
	UT_ASSERT_EQ(forward_captured.parent.capability_record_generation, 29);
	UT_ASSERT_EQ(forward_captured.receiver.capability_record_generation, 29);
	UT_ASSERT(memcmp(&forward_captured, &request, sizeof(request)) == 0);
	UT_ASSERT(memcmp(participant, &before, sizeof(before)) == 0);
	UT_ASSERT_EQ(enters, leaves);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_background_error_cleans_original_batch_and_admission_but_keeps_retry_identity)
{
	for (unsigned phase = 0; phase < 2; phase++) {
		ClusterCtrcParticipantEntry *participant, expected;
		uint64 request_id;
		volatile bool caught = false;

		UT_ASSERT(close_forward_setup(&participant));
		expected = *participant;
		request_id = CtrcShared->global_request_generation;
		if (phase == 0)
			throw_resolve = resolves + 1;
		else
			forward_throw = true;
		PG_TRY();
		{
			(void)ctrc_fixture_run_pass();
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		expected.delegation_close_request_id[3] = request_id;
		UT_ASSERT(memcmp(participant, &expected, sizeof(expected)) == 0);
		UT_ASSERT_EQ(enters, leaves);
		UT_ASSERT_EQ(held_count, 0);
		UT_ASSERT_EQ(allocated, 0);
		UT_ASSERT(!CtrcBatch.active);
		throw_resolve = 0;
		forward_throw = false;
		enqueues = 0;
		(void)ctrc_fixture_run_pass();
		UT_ASSERT_EQ(enqueues, 1);
		UT_ASSERT_EQ(forward_captured.request_id, request_id);
		UT_ASSERT(memcmp(participant, &expected, sizeof(expected)) == 0);
		UT_ASSERT_EQ(enters, leaves);
		UT_ASSERT(!CtrcBatch.active);
	}
}

UT_TEST(test_background_selector_has_one_circumference_budget_and_fair_child_rotation)
{
	uint8 bytes[CLOSE_TEST_BYTES], path[CLUSTER_CTRC_DELEGATION_V2_MAX_BYTES];
	ClusterICEnvelope env;
	ClusterCtrcParticipantEntry *participant;
	ClusterCtrcParticipantIdentity child;
	ClusterCtrcDelegationCloseV1 request;
	Size length;
	uint32 selected = 0;
	uint64 ids[CLUSTER_CTRC_MAX_PARTICIPANTS] = { 0 };

	fixture_declared_nodes = 16;
	UT_ASSERT(close_test_setup(bytes, &env, &participant));
	for (unsigned node = 3; node < CLUSTER_CTRC_MAX_PARTICIPANTS; node++) {
		child = participant->identity;
		child.node_id = node;
		child.boot_incarnation = 1101 + node;
		UT_ASSERT(close_test_pending_child(participant, &child, path, &length));
	}
	UT_ASSERT(gcs_block_try_ctrc_forward136(&env, bytes));
	for (unsigned pass = 0; pass < CLUSTER_CTRC_MAX_PARTICIPANTS; pass++) {
		CtrcBatch.active = true;
		CtrcBatch.remaining[CTRC_SCAN_DELEGATION] = CtrcShared->participant_key_entries;
		UT_ASSERT(cluster_ctrc_participant_next_delegated_close_shared(&request));
		UT_ASSERT(request.receiver.node_id >= 3);
		selected |= UINT32_C(1) << request.receiver.node_id;
		if (ids[request.receiver.node_id] == 0)
			ids[request.receiver.node_id] = request.request_id;
		UT_ASSERT_EQ(request.request_id, ids[request.receiver.node_id]);
		UT_ASSERT(!cluster_ctrc_participant_next_delegated_close_shared(&request));
		UT_ASSERT(ctrc_bytes_zero(&request, sizeof(request)));
		UT_ASSERT_EQ(CtrcBatch.remaining[CTRC_SCAN_DELEGATION], 0);
		UT_ASSERT(!cluster_ctrc_participant_next_delegated_close_shared(&request));
		UT_ASSERT_EQ(held_count, 0);
		MemSet(&CtrcBatch, 0, sizeof(CtrcBatch));
	}
	UT_ASSERT_EQ(selected, UINT32_C(0xfff8));
	UT_ASSERT_EQ(participant->delegation_count, 13);
	fixture_declared_nodes = 4;
}

static ClusterCtrcParticipantEntry *forward_race_participant;
static unsigned forward_race;
static uint64 forward_race_id;

/* Serial controlled interleaving in the new lock gap, using the original
 * locks/sequence domain. This is not a native concurrency test. */
static bool
close_forward_origin_gap(LWLock *lock, LWLockMode mode pg_attribute_unused(), bool acquire)
{
	uint64 index;
	unsigned action = forward_race;

	if (!acquire || lock != &CtrcShared->origin_lock || action == 0)
		return false;
	if (held_count != 0)
		abort();
	forward_race = 0;
	if (!ctrc_participant_index(&forward_race_participant->key,
		forward_race_participant->identity.node_id, &index))
		abort();
	if (action == 2) {
		LWLockAcquire(&CtrcShared->origin_lock, LW_EXCLUSIVE);
		forward_race_id = CtrcShared->global_request_generation++;
		LWLockRelease(&CtrcShared->origin_lock);
	}
	ctrc_participant_locks_acquire(ctrc_participant_lock_mask(index));
	if (action == 1)
		forward_race_participant->delegations[3].receiver.boot_incarnation++;
	else
		forward_race_participant->delegation_close_request_id[3] = forward_race_id;
	ctrc_participant_locks_release(ctrc_participant_lock_mask(index));
	return false;
}

UT_TEST(test_background_sequence_allocation_rechecks_the_exact_row_after_original_lock_gap)
{
	for (unsigned action = 1; action <= 2; action++) {
		ClusterCtrcParticipantEntry *participant, expected;
		ClusterCtrcDelegationCloseV1 request;
		uint64 next_request;

		UT_ASSERT(close_forward_setup(&participant));
		expected = *participant;
		next_request = CtrcShared->global_request_generation;
		forward_race_participant = participant;
		forward_race = action;
		external_lock_hook = close_forward_origin_gap;
		if (action == 1) {
			UT_ASSERT(!cluster_ctrc_participant_next_delegated_close_shared(&request));
			expected.delegations[3].receiver.boot_incarnation++;
			UT_ASSERT(ctrc_bytes_zero(&request, sizeof(request)));
			UT_ASSERT_EQ(CtrcShared->global_request_generation, next_request + 1);
		} else {
			UT_ASSERT(cluster_ctrc_participant_next_delegated_close_shared(&request));
			UT_ASSERT_EQ(request.request_id, forward_race_id);
			expected.delegation_close_request_id[3] = forward_race_id;
			UT_ASSERT_EQ(CtrcShared->global_request_generation, next_request + 2);
		}
		external_lock_hook = NULL;
		UT_ASSERT(memcmp(participant, &expected, sizeof(expected)) == 0);
		UT_ASSERT_EQ(held_count, 0);
		UT_ASSERT_EQ(enqueues, 0);
	}
}

UT_TEST(test_qualified_existing_receiver_reports_merge_without_changing_its_parent)
{
	for (unsigned delegated = 0; delegated < 2; delegated++) {
		ClusterCtrcReceiptHandle source;
		ClusterCtrcParticipantIdentity receiver;
		ClusterItlSlotData slot;
		ClusterCtrcParticipantEntry before;
		ClusterCtrcDelegationCloseV1 request = {0};
		ClusterCtrcSealReplyHeaderV1 header;
		ClusterCtrcLocalReleaseAckV1 ack;
		ClusterICEnvelope env = {0};
		uint8 record[224], bytes[CLOSE_TEST_BYTES];
		uint64 index;

		reset_dispatch_fixture();
		UT_ASSERT(seed_merge_receiver(delegated, &source, &receiver, &slot, record, &index));
		before = ctrc_participant_entries()[index];
		request.key = source.key;
		request.parent = source.participant->identity;
		request.receiver = receiver;
		request.original_full_xid = source.original_full_xid;
		request.request_id = 17001;
		request.seal_generation = 41;
		request.delegation_sequence = source.participant->delegations[receiver.node_id].sequence;
		request.grant_generation = source.participant->grant_generation;
		UT_ASSERT(cluster_ctrc_delegation_close_encode_v1(&request, bytes, sizeof(bytes)));
		env.msg_type = PGRAC_IC_MSG_GCS_BLOCK_FORWARD;
		env.source_node_id = request.parent.node_id;
		env.dest_node_id = receiver.node_id;
		env.epoch = source.key.cluster_epoch;
		env.payload_length = sizeof(bytes);
		dispatch_self_boot = receiver.boot_incarnation;
		close_source_generation = dispatch_peer_connection_generation = 29;
		close_expected_destination = request.parent.node_id;
		close_replies = 0;
		UT_ASSERT(gcs_block_try_ctrc_forward136(&env, bytes));
		UT_ASSERT_EQ(close_replies, 1);
		UT_ASSERT(cluster_ctrc_seal_reply_decode(close_reply + sizeof(GcsBlockReplyHeader),
			BLCKSZ, bytes, sizeof(bytes), receiver.node_id, request.parent.node_id, &header, &ack));
		/* Explicit wire3 result5 is merge-only, never a release ACK. */
		UT_ASSERT_EQ(header.result, 5);
		UT_ASSERT_EQ(header.first_reason, 0);
		UT_ASSERT_EQ(header.ack_length, 0);
		UT_ASSERT(ctrc_bytes_zero(&ack, sizeof(ack)));
		UT_ASSERT(memcmp(&ctrc_participant_entries()[index], &before, sizeof(before)) == 0);
		UT_ASSERT_EQ(source.participant->delegation_count, 1);
		UT_ASSERT_EQ(enters, leaves);
		UT_ASSERT_EQ(held_count, 0);
		UT_ASSERT_EQ(allocated, 0);
	}
}

/* Actual accepted tree 0->1->{2,3}, plus redundant pending 2->3.
 * Original acceptance keeps 3's parent1; only 2's pending can be removed. */
static bool
close_merge_roundtrip_setup(bool other_child, ClusterCtrcParticipantEntry **sender,
	ClusterCtrcParticipantEntry **receiver, ClusterCtrcDelegationCloseV1 *request,
	uint8 reply[GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE])
{
	ClusterCtrcReceiptHandle first, middle;
	ClusterCtrcParticipantIdentity destination, other;
	ClusterCtrcItlTargetIdentity target;
	ClusterCtrcDelegationCloseV1 upstream = {0};
	ClusterICEnvelope env = {0};
	ClusterItlSlotData slot;
	uint8 path[CLUSTER_CTRC_DELEGATION_V2_MAX_BYTES], bytes[CLOSE_TEST_BYTES];
	uint64 index;
	Size length = 0;
	uint32 first_segment;

	reset_dispatch_fixture();
	if (!seed_multihop_source(&first, &middle, &destination, &slot, &target)
		|| !close_test_pending_child(first.participant, &destination, path, &length))
		return false;
	cluster_node_id = destination.node_id;
	if (!cluster_ctrc_itl_delegate_accept_v2(&slot, &first.participant->identity,
		&destination, path, length)
		|| !ctrc_participant_index(&first.key, destination.node_id, &index))
		return false;
	*receiver = &ctrc_participant_entries()[index];
	cluster_node_id = middle.participant->identity.node_id;
	if (!close_test_pending_child(middle.participant, &destination, path, &length))
		return false;
	cluster_node_id = destination.node_id;
	if (!cluster_ctrc_itl_delegate_accept_v2(&slot, &middle.participant->identity,
		&destination, path, length))
		return false;
	*sender = middle.participant;
	cluster_node_id = (*sender)->identity.node_id;
	if (other_child) {
		other = destination;
		other.node_id = 4;
		other.boot_incarnation = 1105;
		if (!close_test_pending_child(*sender, &other, path, &length))
			return false;
	}
	upstream.key = middle.key;
	upstream.parent = (*sender)->delegation_parent.receiver;
	upstream.receiver = (*sender)->identity;
	upstream.original_full_xid = middle.original_full_xid;
	upstream.request_id = 17001;
	upstream.seal_generation = 41;
	upstream.delegation_sequence = (*sender)->delegation_parent.sequence;
	upstream.grant_generation = (*sender)->grant_generation;
	if (!cluster_ctrc_delegation_close_encode_v1(&upstream, bytes, sizeof(bytes)))
		return false;
	env.msg_type = PGRAC_IC_MSG_GCS_BLOCK_FORWARD;
	env.source_node_id = upstream.parent.node_id;
	env.dest_node_id = upstream.receiver.node_id;
	env.epoch = upstream.key.cluster_epoch;
	env.payload_length = sizeof(bytes);
	dispatch_self_boot = upstream.receiver.boot_incarnation;
	close_source_generation = dispatch_peer_connection_generation = 29;
	close_expected_destination = upstream.parent.node_id;
	close_replies = 0;
	if (!gcs_block_try_ctrc_forward136(&env, bytes))
		return false;
	first_segment = ((uint32)middle.key.owner_instance - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	if (!cluster_ctrc_cleaner_bind_worker((middle.key.segment_id - first_segment)
		% CLUSTER_CTRC_CLEANER_WORKERS)
		|| !cluster_ctrc_participant_next_delegated_close_shared(request)
		|| request->receiver.node_id != destination.node_id
		|| !cluster_ctrc_delegation_close_encode_v1(request, bytes, sizeof(bytes)))
		return false;
	cluster_node_id = destination.node_id;
	dispatch_self_boot = destination.boot_incarnation;
	env.source_node_id = request->parent.node_id;
	env.dest_node_id = request->receiver.node_id;
	close_expected_destination = request->parent.node_id;
	close_replies = 0;
	if (!gcs_block_try_ctrc_forward136(&env, bytes) || close_replies != 1)
		return false;
	memcpy(reply, close_reply, sizeof(close_reply));
	cluster_node_id = request->parent.node_id;
	dispatch_self_boot = request->parent.boot_incarnation;
	return true;
}

static bool
close_merge_land(const uint8 *reply)
{
	const GcsBlockReplyHeader *outer = (const GcsBlockReplyHeader *)reply;
	ClusterICEnvelope env = {0};

	env.msg_type = PGRAC_IC_MSG_GCS_BLOCK_REPLY;
	env.source_node_id = outer->sender_node;
	env.dest_node_id = cluster_node_id;
	env.epoch = outer->epoch;
	env.payload_length = GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE;
	return gcs_block_try_land_ctrc_reply(&env, reply);
}

UT_TEST(test_actual_merge_reply_retires_only_the_exact_redundant_pending)
{
	ClusterCtrcParticipantEntry *sender, *receiver, expected, receiver_before;
	ClusterCtrcDelegationCloseV1 request;
	uint8 reply[GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE];

	UT_ASSERT(close_merge_roundtrip_setup(false, &sender, &receiver, &request, reply));
	expected = *sender;
	receiver_before = *receiver;
	MemSet(&expected.delegations[request.receiver.node_id], 0, sizeof(expected.delegations[0]));
	expected.delegation_close_request_id[request.receiver.node_id] = 0;
	expected.delegation_count = expected.last_delegation_sequence = 0;
	UT_ASSERT(close_merge_land(reply));
	UT_ASSERT(memcmp(sender, &expected, sizeof(expected)) == 0);
	UT_ASSERT(memcmp(receiver, &receiver_before, sizeof(receiver_before)) == 0);
	UT_ASSERT_EQ(sender->applied_count, 1);
	UT_ASSERT_EQ(sender->state, CTRC_PARTICIPANT_CLOSED_DRAINING);
	UT_ASSERT(sender->delegation_parent.sequence != 0);
	UT_ASSERT(!ctrc_participant_delegations_empty(sender));
	UT_ASSERT_EQ(enters, leaves);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(allocated, 0);
}

UT_TEST(test_merge_pending_retirement_never_renumbers_other_children)
{
	ClusterCtrcParticipantEntry *sender, *receiver, expected;
	ClusterCtrcDelegationCloseV1 request, next;
	uint8 reply[GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE];

	UT_ASSERT(close_merge_roundtrip_setup(true, &sender, &receiver, &request, reply));
	expected = *sender;
	MemSet(&expected.delegations[request.receiver.node_id], 0, sizeof(expected.delegations[0]));
	expected.delegation_close_request_id[request.receiver.node_id] = 0;
	expected.delegation_count--;
	UT_ASSERT(close_merge_land(reply));
	UT_ASSERT(memcmp(sender, &expected, sizeof(expected)) == 0);
	UT_ASSERT(ctrc_participant_delegations_valid(sender));
	UT_ASSERT(cluster_ctrc_participant_next_delegated_close_shared(&next));
	UT_ASSERT_EQ(next.receiver.node_id, 4);
	UT_ASSERT_EQ(next.delegation_sequence, 2);
	UT_ASSERT_EQ(enters, leaves);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_merge_result_is_only_explicit_wire3_without_any_release_payload)
{
	ClusterCtrcParticipantEntry *sender, *receiver;
	ClusterCtrcDelegationCloseV1 request;
	ClusterCtrcSealReplyHeaderV1 header;
	ClusterCtrcLocalReleaseAckV1 ack;
	uint8 reply[GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE], bytes[CLOSE_TEST_BYTES], page[BLCKSZ], old[136];
	const uint8 *valid;

	UT_ASSERT(close_merge_roundtrip_setup(false, &sender, &receiver, &request, reply));
	UT_ASSERT(cluster_ctrc_delegation_close_encode_v1(&request, bytes, sizeof(bytes)));
	valid = reply + sizeof(GcsBlockReplyHeader);
	UT_ASSERT(cluster_ctrc_seal_reply_decode(valid, BLCKSZ, bytes, sizeof(bytes), 3, 2, &header, &ack));
	UT_ASSERT_EQ(header.result, CTRC_SEAL_REPLY_DELEGATION_MERGED);
	UT_ASSERT(ctrc_bytes_zero(valid + CLUSTER_CTRC_REPLY_ACK_OFFSET,
		BLCKSZ - CLUSTER_CTRC_REPLY_ACK_OFFSET));
	UT_ASSERT(cluster_ctrc_seal_request_encode(&request.key, request.request_id,
		request.grant_generation, request.seal_generation, 29, CTRC_SEAL_CLOSE_AND_CLEAN, old, sizeof(old)));
	UT_ASSERT(!cluster_ctrc_seal_reply_encode(old, sizeof(old), 3, 0,
		CTRC_SEAL_REPLY_DELEGATION_MERGED, 0, NULL, page, sizeof(page)));
	UT_ASSERT(ctrc_bytes_zero(page, sizeof(page)));
	UT_ASSERT(!cluster_ctrc_seal_reply_encode(bytes, sizeof(bytes), 3, 2,
		CTRC_SEAL_REPLY_DELEGATION_MERGED, CTRC_SEAL_REASON_IDENTITY, NULL, page, sizeof(page)));
	UT_ASSERT(!cluster_ctrc_seal_reply_encode(bytes, sizeof(bytes), 3, 2,
		CTRC_SEAL_REPLY_DELEGATION_MERGED, 0, &ack, page, sizeof(page)));
	for (unsigned fault = 0; fault < 9; fault++) {
		memcpy(page, valid, sizeof(page));
		switch (fault) {
			case 0: page[4] = 2; break;
			case 1: page[40] = CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK; break;
			case 2: page[40] = CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED; break;
			case 3: page[40] = 6; break;
			case 4: page[41] = CTRC_SEAL_CERTIFICATE_COMMITTED; break;
			case 5: page[42] = CTRC_SEAL_REASON_IDENTITY; break;
			case 6: page[46] = 1; break;
			case 7: page[CLUSTER_CTRC_REPLY_ACK_OFFSET] = 1; break;
			case 8: page[48] ^= 1; break;
		}
		ctrc_put_u32_le(page + 60, ctrc_crc32c_exact(page, 60));
		UT_ASSERT(!cluster_ctrc_seal_reply_decode(page, sizeof(page), bytes, sizeof(bytes), 3, 2, &header, &ack));
		UT_ASSERT(ctrc_bytes_zero(&header, sizeof(header)));
		UT_ASSERT(ctrc_bytes_zero(&ack, sizeof(ack)));
	}
}

UT_TEST(test_merge_observation_never_qualifies_unknown_closed_or_conflicting_receivers)
{
	for (unsigned fault = 0; fault < 14; fault++) {
		ClusterCtrcParticipantEntry *sender, *receiver, before;
		ClusterCtrcDelegationCloseV1 request;
		uint8 reply[GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE];

		UT_ASSERT(close_merge_roundtrip_setup(false, &sender, &receiver, &request, reply));
		cluster_node_id = request.receiver.node_id;
		switch (fault) {
			case 0: MemSet(receiver, 0, sizeof(*receiver)); break;
			case 1: receiver->state = CTRC_PARTICIPANT_CLOSED_DRAINING; receiver->seal_generation = 41; break;
			case 2: MemSet(receiver->original_identity_v2, 0, sizeof(receiver->original_identity_v2)); break;
			case 3: receiver->original_full_xid = InvalidFullTransactionId; break;
			case 4: receiver->original_full_xid = FullTransactionIdFromU64(U64FromFullTransactionId(request.original_full_xid) + (UINT64_C(1) << 32)); break;
			case 5: receiver->grant_generation++; break;
			case 6: receiver->identity.boot_incarnation++; break;
			case 7: receiver->original_identity_v2[64] ^= 1; break;
			case 8: receiver->delegation_path.hops[0].sequence++; break;
			case 9: receiver->reserved8[0] = 1; break;
			case 10: request.parent = receiver->delegation_parent.receiver; break;
			case 11: request.parent = receiver->delegation_parent.receiver; request.parent.boot_incarnation++; break;
			case 12: request.reserved = 1; break;
			case 13: cluster_node_id = 1; break;
		}
		before = *receiver;
		UT_ASSERT(!cluster_ctrc_participant_delegation_merge_observed_shared(&request));
		UT_ASSERT(memcmp(receiver, &before, sizeof(before)) == 0);
		UT_ASSERT_EQ(sender->delegation_count, 1);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_actual_merge_reply_rejects_framing_digest_and_stale_request_without_mutation)
{
	for (unsigned fault = 0; fault < 20; fault++) {
		ClusterCtrcParticipantEntry *sender, *receiver, before;
		ClusterCtrcDelegationCloseV1 request;
		uint8 reply[GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE];
		GcsBlockReplyHeader *outer = (GcsBlockReplyHeader *)reply;
		uint8 *page = reply + sizeof(*outer);

		UT_ASSERT(close_merge_roundtrip_setup(false, &sender, &receiver, &request, reply));
		switch (fault) {
			case 0: outer->reserved_0[0] = 1; break;
			case 1: outer->requester_backend_id++; break;
			case 2: outer->transition_id = 1; break;
			case 3: outer->page_lsn = 1; break;
			case 4: outer->epoch++; break;
			case 5: outer->sender_node = 1; break;
			case 6: outer->request_id++; break;
			case 7: GcsBlockReplyHeaderSetForwardingMasterNode(outer, 1); break;
			case 8: page[4] = 2; break;
			case 9: page[40] = CTRC_SEAL_REPLY_PENDING_DRAIN; page[42] = CTRC_SEAL_REASON_CLEANOUT; break;
			case 10: page[48] ^= 1; break;
			case 11: page[CLUSTER_CTRC_REPLY_ACK_OFFSET] = 1; break;
			case 12: sender->delegation_close_request_id[3] = 0; break;
			case 13: sender->delegation_close_request_id[3]++; break;
			case 14: sender->grant_generation++; break;
			case 15: sender->seal_generation++; break;
			case 16: sender->delegations[3].receiver.boot_incarnation++; break;
			case 17: sender->original_identity_v2[64] ^= 1; break;
			case 18: sender->original_full_xid = InvalidFullTransactionId; break;
			case 19: sender->state = CTRC_PARTICIPANT_OPEN; sender->seal_generation = 0; break;
		}
		ctrc_put_u32_le(page + 60, ctrc_crc32c_exact(page, 60));
		outer->checksum = gcs_block_compute_checksum((const char *)page);
		before = *sender;
		UT_ASSERT(close_merge_land(reply));
		UT_ASSERT(memcmp(sender, &before, sizeof(before)) == 0);
		UT_ASSERT_EQ(enters, leaves);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_actual_merge_reply_requires_live_boot_hello_root_and_current_admission)
{
	for (unsigned fault = 0; fault < 11; fault++) {
		ClusterCtrcParticipantEntry *sender, *receiver, before;
		ClusterCtrcDelegationCloseV1 request;
		uint8 reply[GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE];
		volatile bool caught = false;

		UT_ASSERT(close_merge_roundtrip_setup(false, &sender, &receiver, &request, reply));
		before = *sender;
		rechecks = resolves = 0;
		switch (fault) {
			case 0: dispatch_excluded_member = 3; break;
			case 1: dispatch_observed_boot_delta = 1; break;
			case 2: dispatch_self_boot++; break;
			case 3: close_source_generation = 0; break;
			case 4: close_source_generation++; break;
			case 5: root_exact = false; break;
			case 6: fail_recheck = 1; break;
			case 7: fail_recheck = 2; break;
			case 8: fence_ok = false; break;
			case 9: data_plane = false; break;
			case 10: throw_resolve = 1; break;
		}
		PG_TRY();
		{
			UT_ASSERT(close_merge_land(reply));
		}
		PG_CATCH();
		{
			caught = true;
			UT_ASSERT_EQ(fault, 10);
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 10);
		UT_ASSERT(memcmp(sender, &before, sizeof(before)) == 0);
		UT_ASSERT_EQ(enters, leaves);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_actual_merge_reply_keeps_hello_separate_and_duplicate_is_a_noop)
{
	ClusterCtrcParticipantEntry *sender, *receiver, before, expected;
	ClusterCtrcDelegationCloseV1 request, ignored;
	uint8 reply[GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE];
	uint64 progress;

	UT_ASSERT(close_merge_roundtrip_setup(false, &sender, &receiver, &request, reply));
	close_source_generation = dispatch_peer_connection_generation = 7;
	UT_ASSERT(close_merge_land(reply));
	UT_ASSERT_EQ(sender->delegation_count, 0);
	before = *sender;
	progress = cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS);
	UT_ASSERT(close_merge_land(reply));
	UT_ASSERT(memcmp(sender, &before, sizeof(before)) == 0);
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS), progress);
	MemSet(&ignored, 0x5a, sizeof(ignored));
	UT_ASSERT(!cluster_ctrc_participant_delegated_close_snapshot_shared(request.request_id, 3, &ignored));
	UT_ASSERT(ctrc_bytes_zero(&ignored, sizeof(ignored)));
	/* Even with no redundant children, own APPLIED and the incoming parent
	 * still forbid any upstream release. */
	expected = *sender;
	UT_ASSERT(!cluster_ctrc_participant_delegation_merge_land_shared(&request));
	UT_ASSERT(memcmp(sender, &expected, sizeof(expected)) == 0);
	UT_ASSERT_EQ(sender->applied_count, 1);
	UT_ASSERT(sender->delegation_parent.sequence != 0);
	UT_ASSERT_EQ(enters, leaves);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_receiver_merge_observation_is_rechecked_before_the_existing_reply)
{
	for (unsigned phase = 1; phase <= 2; phase++) {
		ClusterCtrcParticipantEntry *sender, *receiver, before;
		ClusterCtrcDelegationCloseV1 request;
		ClusterCtrcSealReplyHeaderV1 header;
		ClusterCtrcLocalReleaseAckV1 ack;
		ClusterICEnvelope env = {0};
		uint8 reply[GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE], bytes[CLOSE_TEST_BYTES];

		UT_ASSERT(close_merge_roundtrip_setup(false, &sender, &receiver, &request, reply));
		before = *receiver;
		UT_ASSERT(cluster_ctrc_delegation_close_encode_v1(&request, bytes, sizeof(bytes)));
		cluster_node_id = request.receiver.node_id;
		dispatch_self_boot = request.receiver.boot_incarnation;
		env.msg_type = PGRAC_IC_MSG_GCS_BLOCK_FORWARD;
		env.source_node_id = request.parent.node_id;
		env.dest_node_id = request.receiver.node_id;
		env.epoch = request.key.cluster_epoch;
		env.payload_length = sizeof(bytes);
		rechecks = 0;
		fail_recheck = phase;
		UT_ASSERT(gcs_block_try_ctrc_forward136(&env, bytes));
		UT_ASSERT(cluster_ctrc_seal_reply_decode(close_reply + sizeof(GcsBlockReplyHeader),
			BLCKSZ, bytes, sizeof(bytes), 3, 2, &header, &ack));
		UT_ASSERT_EQ(header.result, CTRC_SEAL_REPLY_BLOCKED_RETAIN);
		UT_ASSERT_EQ(header.first_reason, CTRC_SEAL_REASON_IDENTITY);
		UT_ASSERT(memcmp(receiver, &before, sizeof(before)) == 0);
		UT_ASSERT_EQ(sender->delegation_count, 1);
		UT_ASSERT_EQ(enters, leaves);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_merge_landing_error_after_exact_retirement_leaves_admission_once)
{
	ClusterCtrcParticipantEntry *sender, *receiver, expected, receiver_before;
	ClusterCtrcDelegationCloseV1 request;
	uint8 reply[GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE];
	volatile bool caught = false;

	UT_ASSERT(close_merge_roundtrip_setup(false, &sender, &receiver, &request, reply));
	expected = *sender;
	receiver_before = *receiver;
	MemSet(&expected.delegations[3], 0, sizeof(expected.delegations[3]));
	expected.delegation_close_request_id[3] = 0;
	expected.delegation_count = expected.last_delegation_sequence = 0;
	wakeup_hook = throw_from_participant_wakeup;
	PG_TRY();
	{
		(void)close_merge_land(reply);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	wakeup_hook = NULL;
	UT_ASSERT(caught);
	UT_ASSERT(memcmp(sender, &expected, sizeof(expected)) == 0);
	UT_ASSERT(memcmp(receiver, &receiver_before, sizeof(receiver_before)) == 0);
	UT_ASSERT_EQ(enters, leaves);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(allocated, 0);
	UT_ASSERT(close_merge_land(reply));
	UT_ASSERT(memcmp(sender, &expected, sizeof(expected)) == 0);
}

/* The original roundtrip fixtures model several nodes in one pool. Build
 * the carrier in the sender, then start a fresh receiver pool and execute
 * its actual accept/prepare/CLOSE paths. No remote receipt belongs to the
 * local ACK scanner. Network, process startup and disk remain controlled. */
static bool
close_local_leaf_setup(ClusterCtrcParticipantEntry **receiver_out,
	ClusterCtrcDelegationCloseV1 *request, ClusterCtrcReceiptHandle *local_receipt, bool cancel_receipt)
{
	uint8 bytes[CLOSE_TEST_BYTES], record[CLUSTER_CTRC_DELEGATION_V1_BYTES];
	ClusterICEnvelope env = {0};
	ClusterCtrcReceiptHandle source;
	ClusterCtrcParticipantIdentity identity;
	ClusterCtrcParticipantEntry *receiver;
	ClusterCtrcTargetV1 pending;
	ClusterItlSlotData slot;
	uint64 index;
	uint32 first;

	fixture_declared_nodes = 4;
	reset_dispatch_fixture();
	if (!seed_delegation_receive(&source, &identity, &slot, record, &index))
		return false;
	MemSet(request, 0, sizeof(*request));
	request->key = source.key;
	request->parent = source.participant->identity;
	request->receiver = identity;
	request->original_full_xid = source.original_full_xid;
	request->grant_generation = source.participant->grant_generation;
	request->delegation_sequence = source.participant->delegations[identity.node_id].sequence;
	request->request_id = 17001;
	request->seal_generation = 41;
	if (!cluster_ctrc_delegation_close_encode_v1(request, bytes, sizeof(bytes)))
		return false;
	reset_dispatch_fixture();
	cluster_node_id = identity.node_id;
	dispatch_self_boot = identity.boot_incarnation;
	close_replies = 0;
	close_source_generation = 29;
	close_expected_destination = request->parent.node_id;
	MemSet(close_reply, 0, sizeof(close_reply));
	if (!cluster_ctrc_itl_delegate_accept_v1(&slot, &request->parent,
		&identity, record, sizeof(record)))
		return false;
	receiver = &ctrc_participant_entries()[index];
	if (local_receipt != NULL) {
		image_pending_target(&pending);
		if (cluster_ctrc_itl_image_prepare_v1(&slot, &request->parent,
			&identity, record, sizeof(record), &pending, local_receipt) != CLUSTER_CTRC_PREPARE_READY
			|| !local_receipt->valid)
			return false;
		if (cancel_receipt && !cluster_ctrc_receipt_cancel_shared(local_receipt))
			return false;
	}
	env.msg_type = PGRAC_IC_MSG_GCS_BLOCK_FORWARD;
	env.source_node_id = request->parent.node_id;
	env.dest_node_id = identity.node_id;
	env.epoch = request->key.cluster_epoch;
	env.payload_length = sizeof(bytes);
	if (!gcs_block_try_ctrc_forward136(&env, bytes))
		return false;
	first = ((uint32)receiver->key.owner_instance - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	if (!cluster_ctrc_cleaner_bind_worker(
		(receiver->key.segment_id - first) % CLUSTER_CTRC_CLEANER_WORKERS))
		return false;
	*receiver_out = receiver;
	return receiver->state == CTRC_PARTICIPANT_CLOSED_DRAINING;
}

UT_TEST(test_original_cleaner_freezes_local_leaf_proof_without_releasing_accepted_parent)
{
	ClusterCtrcParticipantEntry *receiver, before;
	ClusterCtrcDelegationCloseV1 request;
	uint64 index;

	UT_ASSERT(close_local_leaf_setup(&receiver, &request, NULL, false));
	UT_ASSERT_EQ(receiver->state, CTRC_PARTICIPANT_CLOSED_DRAINING);
	UT_ASSERT_EQ(receiver->receipt_count, 0);
	before = *receiver;
	UT_ASSERT(ctrc_participant_index(&receiver->key, receiver->identity.node_id, &index));
	/* Missing producer: the original cleaner currently skips all parent rows. */
	UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(1), 1);
	UT_ASSERT(memcmp(receiver, &before, sizeof(before)) == 0);
	UT_ASSERT_EQ(ctrc_participant_ack_entries()[index].flags,
		CTRC_ACK_FLAG_ZERO_RANGE | CTRC_ACK_FLAG_ALL_DURABLE);
	UT_ASSERT_EQ(ctrc_participant_ack_entries()[index].total_receipt_count, 0);
	UT_ASSERT_EQ(durability_calls, 2);
	UT_ASSERT(!ctrc_frozen_ack_summary_exact(receiver, &ctrc_participant_ack_entries()[index]));
	UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(1), 0);
	UT_ASSERT(memcmp(receiver, &before, sizeof(before)) == 0);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(allocated, 0);
}

UT_TEST(test_delegated_nonempty_local_freeze_keeps_parent_and_ordinary_ack_closed)
{
	ClusterCtrcParticipantEntry *receiver, before, expected;
	ClusterCtrcDelegationCloseV1 request;
	ClusterCtrcReceiptHandle receipt;
	ClusterCtrcLocalReleaseAckV1 ack, denied;
	ClusterCtrcDurability durability = {0};
	uint16 reason;

	UT_ASSERT(close_local_leaf_setup(&receiver, &request, &receipt, true));
	UT_ASSERT_EQ(receipt.receipt->state, CTRC_RECEIPT_CANCELLED);
	before = *receiver;
	UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(1), 1);
	expected = before;
	expected.ack_frozen_count = 1;
	UT_ASSERT(memcmp(receiver, &expected, sizeof(expected)) == 0);
	UT_ASSERT_EQ(receipt.receipt->state, CTRC_RECEIPT_ACK_FROZEN);
	UT_ASSERT(cluster_ctrc_participant_delegated_local_ack_shared(&request, &ack));
	UT_ASSERT_EQ(ack.total_receipt_count, 1);
	UT_ASSERT_EQ(ack.cancelled_count, 1);
	UT_ASSERT_EQ(ack.cleaned_count, 0);
	UT_ASSERT_EQ(ack.first_key_sequence, 1);
	UT_ASSERT_EQ(ack.last_key_sequence, 1);
	UT_ASSERT_EQ(ack.ack_frozen_count, 1);
	UT_ASSERT(ctrc_ack_bytes_exact(&ack));
	UT_ASSERT_EQ(cluster_ctrc_participant_ack_from_snapshot(receiver, NULL, 0,
		&durability, &denied), CLUSTER_CTRC_ACK_DENIED);
	UT_ASSERT(ctrc_bytes_zero(&denied, sizeof(denied)));
	UT_ASSERT(!ctrc_frozen_ack_summary_exact(receiver, &ack));
	UT_ASSERT_EQ(cluster_ctrc_participant_delegated_close_shared(&request, &reason),
		CTRC_SEAL_REPLY_PENDING_DRAIN);
	UT_ASSERT(memcmp(receiver, &expected, sizeof(expected)) == 0);
	UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(1), 0);
	UT_ASSERT(cluster_ctrc_participant_delegated_local_ack_shared(&request, &denied));
	UT_ASSERT(memcmp(&ack, &denied, sizeof(ack)) == 0);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(allocated, 0);
}

UT_TEST(test_delegated_local_freeze_waits_for_original_prepared_and_child_custody)
{
	ClusterCtrcParticipantEntry *receiver, before;
	ClusterCtrcDelegationCloseV1 request;
	ClusterCtrcReceiptHandle receipt;
	uint64 index;

	UT_ASSERT(close_local_leaf_setup(&receiver, &request, &receipt, false));
	before = *receiver;
	UT_ASSERT_EQ(receiver->prepared_count, 1);
	UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(1), 0);
	UT_ASSERT(memcmp(receiver, &before, sizeof(before)) == 0);
	UT_ASSERT_EQ(receipt.receipt->state, CTRC_RECEIPT_PREPARED);
	UT_ASSERT_EQ(durability_calls, 0);
	UT_ASSERT(close_forward_setup(&receiver));
	before = *receiver;
	UT_ASSERT(ctrc_participant_index(&receiver->key, receiver->identity.node_id, &index));
	UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(1), 0);
	UT_ASSERT_EQ(receiver->delegation_count, 1);
	UT_ASSERT(memcmp(receiver, &before, sizeof(before)) == 0);
	UT_ASSERT(ctrc_bytes_zero(&ctrc_participant_ack_entries()[index], sizeof(ClusterCtrcLocalReleaseAckV1)));
	UT_ASSERT_EQ(durability_calls, 0);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_delegated_local_proof_never_qualifies_unknown_original_or_partial_inventory)
{
	for (unsigned fault = 0; fault < 14; fault++) {
		ClusterCtrcParticipantEntry *receiver;
		ClusterCtrcDelegationCloseV1 request;
		ClusterCtrcDelegationCustodyV1 parent;
		uint64 index;

		UT_ASSERT(close_local_leaf_setup(&receiver, &request, NULL, false));
		UT_ASSERT(ctrc_participant_index(&receiver->key, receiver->identity.node_id, &index));
		switch (fault) {
			case 0: MemSet(receiver->original_identity_v2, 0, sizeof(receiver->original_identity_v2)); break;
			case 1: receiver->original_full_xid = InvalidFullTransactionId; break;
			case 2: receiver->original_full_xid = FullTransactionIdFromU64(U64FromFullTransactionId(request.original_full_xid) + (UINT64_C(1) << 32)); break;
			case 3: receiver->original_identity_v2[64] ^= 1; break;
			case 4: receiver->delegation_parent.sequence = 0; break;
			case 5: receiver->delegation_parent.receiver.boot_incarnation++; break;
			case 6: receiver->delegation_path.reserved = 1; break;
			case 7: receiver->reserved8[0] = 1; break;
			case 8: receiver->cancelled_count = 1; break;
			case 9: receiver->cleaned_count = 1; break;
			case 10: receiver->ack_frozen_count = 1; break;
			case 11: receiver->delegation_close_request_id[3] = 19; break;
			case 12: receiver->delegations[3].sequence = 1; break;
			case 13: receiver->state = CTRC_PARTICIPANT_OPEN; receiver->seal_generation = 0; break;
		}
		parent = receiver->delegation_parent;
		UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(1), 0);
		UT_ASSERT(ctrc_bytes_zero(&ctrc_participant_ack_entries()[index], sizeof(ClusterCtrcLocalReleaseAckV1)));
		UT_ASSERT(memcmp(&receiver->delegation_parent, &parent, sizeof(parent)) == 0);
		UT_ASSERT_EQ(durability_calls, 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

static ClusterCtrcParticipantEntry *local_drift_participant;
static unsigned local_drift_capture;

static void
close_local_drift_capture(unsigned call)
{
	if (call == local_drift_capture)
		local_drift_participant->original_identity_v2[64] ^= 1;
}

static void
close_local_drift_barrier(void)
{
	local_drift_participant->delegation_parent.receiver.boot_incarnation++;
}

UT_TEST(test_delegated_local_proof_rechecks_full_original_row_after_both_samples)
{
	for (unsigned phase = 0; phase < 3; phase++) {
		ClusterCtrcParticipantEntry *receiver;
		ClusterCtrcDelegationCloseV1 request;
		uint64 index;

		UT_ASSERT(close_local_leaf_setup(&receiver, &request, NULL, false));
		UT_ASSERT(ctrc_participant_index(&receiver->key, receiver->identity.node_id, &index));
		local_drift_participant = receiver;
		local_drift_capture = phase + 1;
		if (phase < 2)
			durability_hook = close_local_drift_capture;
		else {
			barrier_hook = close_local_drift_barrier;
			UT_ASSERT(cluster_ctrc_test_barrier_control(CTRC_TEST_BARRIER_ACK_DURABLE, true));
		}
		UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(1), 0);
		UT_ASSERT(ctrc_bytes_zero(&ctrc_participant_ack_entries()[index], sizeof(ClusterCtrcLocalReleaseAckV1)));
		UT_ASSERT_EQ(receiver->state, CTRC_PARTICIPANT_BLOCKED);
		UT_ASSERT_EQ(receiver->delegation_parent.sequence, request.delegation_sequence);
		UT_ASSERT_EQ(held_count, 0);
		UT_ASSERT_EQ(allocated, 0);
	}
}

static XLogRecPtr
close_local_remote_durable(int origin)
{
	return origin == 0 && durability_calls == 2 ? 0 : 100;
}

UT_TEST(test_delegated_local_receipt_proof_requires_both_original_durable_frontiers)
{
	ClusterCtrcParticipantEntry *receiver, before;
	ClusterCtrcDelegationCloseV1 request;
	ClusterCtrcReceiptHandle receipt;
	ClusterCtrcLocalReleaseAckV1 ack;

	UT_ASSERT(close_local_leaf_setup(&receiver, &request, &receipt, true));
	UT_ASSERT_EQ(receipt.receipt->state, CTRC_RECEIPT_CANCELLED);
	/* Controlled WAL/frontier inputs exercise the unchanged receipt proof. */
	receipt.receipt->highest_local_wal_lsn = 64;
	receipt.receipt->required_lsn[0] = 80;
	test_flush_lsn = 100;
	origin_durability_hook = close_local_remote_durable;
	before = *receiver;
	UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(1), 0);
	UT_ASSERT(memcmp(receiver, &before, sizeof(before)) == 0);
	UT_ASSERT_EQ(receipt.receipt->state, CTRC_RECEIPT_CANCELLED);
	UT_ASSERT(!cluster_ctrc_participant_delegated_local_ack_shared(&request, &ack));
	UT_ASSERT(ctrc_bytes_zero(&ack, sizeof(ack)));
	origin_durability_hook = NULL;
	UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(1), 1);
	UT_ASSERT(cluster_ctrc_participant_delegated_local_ack_shared(&request, &ack));
	UT_ASSERT_EQ(ack.highest_local_cleanout_lsn, 64);
	UT_ASSERT_EQ(ack.required_lsn_vector[0], 80);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(allocated, 0);
}

UT_TEST(test_delegated_local_freeze_errors_and_oom_keep_all_original_custody)
{
	for (unsigned phase = 0; phase < 3; phase++) {
		ClusterCtrcParticipantEntry *receiver, before;
		ClusterCtrcDelegationCloseV1 request;
		ClusterCtrcReceiptHandle receipt;
		ClusterCtrcReceipt before_receipt;
		volatile bool caught = false;
		uint64 index;

		UT_ASSERT(close_local_leaf_setup(&receiver, &request, &receipt, true));
		UT_ASSERT_EQ(receipt.receipt->state, CTRC_RECEIPT_CANCELLED);
		UT_ASSERT(ctrc_participant_index(&receiver->key, receiver->identity.node_id, &index));
		before = *receiver;
		before_receipt = *receipt.receipt;
		if (phase == 0)
			durability_hook = throw_during_capture;
		else if (phase == 1) {
			barrier_hook = throw_during_barrier;
			UT_ASSERT(cluster_ctrc_test_barrier_control(CTRC_TEST_BARRIER_ACK_DURABLE, true));
		} else
			allocation_failure = true;
		PG_TRY();
		{
			UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(1), 0);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, phase != 2);
		UT_ASSERT(memcmp(receiver, &before, sizeof(before)) == 0);
		UT_ASSERT(memcmp(receipt.receipt, &before_receipt, sizeof(before_receipt)) == 0);
		UT_ASSERT(ctrc_bytes_zero(&ctrc_participant_ack_entries()[index], sizeof(ClusterCtrcLocalReleaseAckV1)));
		UT_ASSERT_EQ(held_count, 0);
		UT_ASSERT_EQ(allocated, 0);
	}
}

UT_TEST(test_delegated_local_reader_rejects_stale_parent_fullxid_and_frozen_chain)
{
	for (unsigned fault = 0; fault < 16; fault++) {
		ClusterCtrcParticipantEntry *receiver, before;
		ClusterCtrcDelegationCloseV1 request;
		ClusterCtrcReceiptHandle receipt;
		ClusterCtrcLocalReleaseAckV1 ack;
		uint64 index;

		UT_ASSERT(close_local_leaf_setup(&receiver, &request, &receipt, true));
		UT_ASSERT_EQ(receipt.receipt->state, CTRC_RECEIPT_CANCELLED);
		UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(1), 1);
		UT_ASSERT(ctrc_participant_index(&receiver->key, receiver->identity.node_id, &index));
		switch (fault) {
			case 0: request.parent.boot_incarnation++; break;
			case 1: request.receiver.admission_record_generation++; break;
			case 2: request.grant_generation++; break;
			case 3: request.seal_generation++; break;
			case 4: request.delegation_sequence++; break;
			case 5: request.original_full_xid = FullTransactionIdFromU64(U64FromFullTransactionId(request.original_full_xid) + (UINT64_C(1) << 32)); break;
			case 6: request.key.slot_wrap++; break;
			case 7: request.reserved = 1; break;
			case 8: request.request_id = 0; break;
			case 9: ctrc_participant_ack_entries()[index].crc32c ^= 1; break;
			case 10: receipt.receipt->state = CTRC_RECEIPT_CANCELLED; break;
			case 11: ctrc_receipt_links()[receipt.receipt_index].next_plus_one = receipt.receipt_index + 1; break;
			case 12: receiver->original_identity_v2[64] ^= 1; break;
			case 13: receiver->ack_frozen_count = 0; break;
			case 14: receiver->delegation_close_request_id[3] = 1; break;
			case 15: cluster_node_id = 1; break;
		}
		before = *receiver;
		MemSet(&ack, 0x5a, sizeof(ack));
		UT_ASSERT(!cluster_ctrc_participant_delegated_local_ack_shared(&request, &ack));
		UT_ASSERT(ctrc_bytes_zero(&ack, sizeof(ack)));
		UT_ASSERT(memcmp(receiver, &before, sizeof(before)) == 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_normal_stop_reports_retained_local_proof_pending_and_corruption_invalid)
{
	ClusterCtrcParticipantEntry *receiver;
	ClusterCtrcDelegationCloseV1 request;
	ClusterCtrcReceiptHandle receipt;
	ClusterCtrcNormalStopObservation observed;
	uint64 index;

	UT_ASSERT(close_local_leaf_setup(&receiver, &request, &receipt, true));
	UT_ASSERT_EQ(receipt.receipt->state, CTRC_RECEIPT_CANCELLED);
	UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(1), 1);
	UT_ASSERT(ctrc_participant_index(&receiver->key, receiver->identity.node_id, &index));
	UT_ASSERT_EQ(cluster_ctrc_normal_stop_poll(&observed), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(receiver->state, CTRC_PARTICIPANT_CLOSED_DRAINING);
	ctrc_participant_ack_entries()[index].crc32c ^= 1;
	UT_ASSERT_EQ(cluster_ctrc_normal_stop_poll(&observed), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(observed.domain, CTRC_STOP_DOMAIN_PARTICIPANT_ACK);
	UT_ASSERT_EQ(receiver->state, CTRC_PARTICIPANT_CLOSED_DRAINING);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_actual_delegated_close_returns_explicit_subtree_proof_after_local_freeze)
{
	ClusterCtrcParticipantEntry *receiver, before;
	ClusterCtrcDelegationCloseV1 request;
	ClusterCtrcReceiptHandle receipt;
	ClusterICEnvelope env = {0};
	uint8 bytes[CLOSE_TEST_BYTES];
	const uint8 *page = close_reply + sizeof(GcsBlockReplyHeader);

	UT_ASSERT(close_local_leaf_setup(&receiver, &request, &receipt, true));
	UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(1), 1);
	before = *receiver;
	UT_ASSERT(cluster_ctrc_delegation_close_encode_v1(&request, bytes, sizeof(bytes)));
	env.msg_type = PGRAC_IC_MSG_GCS_BLOCK_FORWARD;
	env.source_node_id = request.parent.node_id;
	env.dest_node_id = request.receiver.node_id;
	env.epoch = request.key.cluster_epoch;
	env.payload_length = sizeof(bytes);
	close_replies = 0;
	UT_ASSERT(gcs_block_try_ctrc_forward136(&env, bytes));
	UT_ASSERT_EQ(close_replies, 1);
	/* Explicit new reply domain, never old LOCAL_RELEASE_ACK or certificate. */
	UT_ASSERT_EQ(ctrc_get_u16_le(page + 4), 4);
	UT_ASSERT_EQ(page[40], 6);
	UT_ASSERT_EQ(ctrc_get_u16_le(page + 46), 592);
	UT_ASSERT(memcmp(receiver, &before, sizeof(before)) == 0);
	UT_ASSERT_EQ(receipt.receipt->state, CTRC_RECEIPT_ACK_FROZEN);
	UT_ASSERT_EQ(enters, leaves);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(allocated, 0);
}

static bool
subtree_send_close(const ClusterCtrcDelegationCloseV1 *request)
{
	uint8 bytes[CLOSE_TEST_BYTES];
	ClusterICEnvelope env = {0};

	if (!cluster_ctrc_delegation_close_encode_v1(request, bytes, sizeof(bytes)))
		return false;
	cluster_node_id = request->receiver.node_id;
	dispatch_self_boot = request->receiver.boot_incarnation;
	close_source_generation = dispatch_peer_connection_generation = 29;
	close_expected_destination = request->parent.node_id;
	close_replies = 0;
	env.msg_type = PGRAC_IC_MSG_GCS_BLOCK_FORWARD;
	env.source_node_id = request->parent.node_id;
	env.dest_node_id = request->receiver.node_id;
	env.epoch = request->key.cluster_epoch;
	env.payload_length = sizeof(bytes);
	return gcs_block_try_ctrc_forward136(&env, bytes) && close_replies == 1;
}

/* Independent controlled node pools; only the encoded original carriers
 * and replies cross between them. Membership, scheduling and durable I/O
 * remain fixture providers, not native or concurrency evidence. */
static ClusterCtrcSharedHeader *subtree_leaf_pool;

static bool
subtree_roundtrip_setup(ClusterCtrcParticipantEntry **parent,
	ClusterCtrcParticipantEntry **leaf, ClusterCtrcDelegationCloseV1 *upstream,
	ClusterCtrcDelegationCloseV1 *child_request, uint8 reply[GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE])
{
	ClusterCtrcReceiptHandle source, receipt;
	ClusterCtrcParticipantIdentity original_source, middle, child;
	ClusterCtrcSharedHeader *parent_pool;
	ClusterItlSlotData slot;
	ClusterCtrcTargetV1 pending;
	uint8 record[224], path[CLUSTER_CTRC_DELEGATION_V2_MAX_BYTES];
	uint64 index;
	Size length;
	uint32 first;
	bool ready = false;

	free(subtree_leaf_pool);
	subtree_leaf_pool = NULL;
	fixture_declared_nodes = 4;
	reset_dispatch_fixture();
	if (!seed_delegation_receive(&source, &middle, &slot, record, &index))
		return false;
	original_source = source.participant->identity;
	/* The foreign source inventory must not exist in the receiver pool. */
	reset_dispatch_fixture();
	cluster_node_id = middle.node_id;
	if (!cluster_ctrc_itl_delegate_accept_v1(&slot, &original_source,
		&middle, record, sizeof(record)))
		return false;
	*parent = &ctrc_participant_entries()[index];
	child = middle;
	child.node_id = 3;
	child.boot_incarnation = 1104;
	if (!close_test_pending_child(*parent, &child, path, &length))
		return false;
	MemSet(upstream, 0, sizeof(*upstream));
	upstream->key = (*parent)->key;
	upstream->parent = (*parent)->delegation_parent.receiver;
	upstream->receiver = middle;
	upstream->original_full_xid = (*parent)->original_full_xid;
	upstream->grant_generation = (*parent)->grant_generation;
	upstream->delegation_sequence = (*parent)->delegation_parent.sequence;
	upstream->seal_generation = 41;
	upstream->request_id = 17001;
	if (!subtree_send_close(upstream))
		return false;
	first = ((uint32)(*parent)->key.owner_instance - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	if (!cluster_ctrc_cleaner_bind_worker(((*parent)->key.segment_id - first)
		% CLUSTER_CTRC_CLEANER_WORKERS)
		|| !cluster_ctrc_participant_next_delegated_close_shared(child_request)
		|| child_request->receiver.node_id != child.node_id)
		return false;
	parent_pool = CtrcShared;
	CtrcShared = NULL;
	cluster_ctrc_shmem_init();
	subtree_leaf_pool = CtrcShared;
	cluster_node_id = child.node_id;
	image_pending_target(&pending);
	if (cluster_ctrc_itl_image_prepare_v2(&slot, &middle, &child, path, length,
		&pending, &receipt) == CLUSTER_CTRC_PREPARE_READY
		&& cluster_ctrc_receipt_cancel_shared(&receipt)) {
		*leaf = receipt.participant;
		ready = subtree_send_close(child_request)
			&& ctrc_participant_freeze_ack_batch_shared(1) == 1
			&& subtree_send_close(child_request);
	}
	if (ready)
		memcpy(reply, close_reply, sizeof(close_reply));
	CtrcShared = parent_pool;
	cluster_node_id = middle.node_id;
	dispatch_self_boot = middle.boot_incarnation;
	return ready && ctrc_get_u16_le(reply + sizeof(GcsBlockReplyHeader) + 4) == 4;
}

UT_TEST(test_actual_subtree_reply_lands_once_and_parent_aggregates_without_retiring_custody)
{
	ClusterCtrcParticipantEntry *parent, *leaf, before, expected, leaf_before;
	ClusterCtrcDelegationCloseV1 upstream, request, ignored;
	ClusterCtrcDelegatedSubtreeAckV1 child_ack, aggregate, decoded;
	ClusterCtrcNormalStopObservation observed;
	uint8 reply[GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE];
	uint8 page[BLCKSZ];
	uint64 progress;

	UT_ASSERT(subtree_roundtrip_setup(&parent, &leaf, &upstream, &request, reply));
	UT_ASSERT(cluster_ctrc_delegated_ack_reply_decode(reply + sizeof(GcsBlockReplyHeader),
		BLCKSZ, &request, &child_ack));
	UT_ASSERT_EQ(child_ack.local_ack.cancelled_count, 1);
	UT_ASSERT_EQ(child_ack.subtree_bitmap, UINT64_C(1) << 3);
	UT_ASSERT(!cluster_ctrc_participant_delegated_subtree_ack_shared(&upstream, &aggregate));
	UT_ASSERT(ctrc_bytes_zero(&aggregate, sizeof(aggregate)));
	UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(1), 0);
	before = *parent;
	leaf_before = *leaf;
	UT_ASSERT(close_merge_land(reply));
	expected = before;
	expected.delegation_ack_bitmap = UINT64_C(1) << 3;
	expected.delegation_subtree_bitmap[3] = child_ack.subtree_bitmap;
	UT_ASSERT(ctrc_subtree_ack_digest(&request, &child_ack, expected.delegation_ack_digest[3]));
	memcpy(expected.delegation_required_lsn, child_ack.required_lsn, sizeof(child_ack.required_lsn));
	UT_ASSERT(memcmp(parent, &expected, sizeof(expected)) == 0);
	UT_ASSERT(memcmp(leaf, &leaf_before, sizeof(leaf_before)) == 0);
	progress = cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS);
	UT_ASSERT(close_merge_land(reply));
	UT_ASSERT(memcmp(parent, &expected, sizeof(expected)) == 0);
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS), progress);
	UT_ASSERT(!cluster_ctrc_participant_next_delegated_close_shared(&ignored));
	UT_ASSERT(ctrc_bytes_zero(&ignored, sizeof(ignored)));
	UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(1), 1);
	UT_ASSERT(cluster_ctrc_participant_delegated_subtree_ack_shared(&upstream, &aggregate));
	UT_ASSERT_EQ(aggregate.subtree_bitmap, (UINT64_C(1) << 2) | (UINT64_C(1) << 3));
	UT_ASSERT_EQ(aggregate.local_ack.total_receipt_count, 0);
	UT_ASSERT(memcmp(aggregate.children_digest_sha256, cluster_ctrc_empty_sha256, 32) != 0);
	for (unsigned node = 0; node < CLUSTER_SF_DEP_MAX_ORIGINS; node++)
		UT_ASSERT(aggregate.required_lsn[node] >= child_ack.required_lsn[node]);
	UT_ASSERT(cluster_ctrc_delegated_ack_reply_encode(&upstream, &aggregate, page, sizeof(page)));
	UT_ASSERT(cluster_ctrc_delegated_ack_reply_decode(page, sizeof(page), &upstream, &decoded));
	UT_ASSERT(memcmp(&aggregate, &decoded, sizeof(decoded)) == 0);
	UT_ASSERT(subtree_send_close(&upstream));
	UT_ASSERT(memcmp(page, close_reply + sizeof(GcsBlockReplyHeader), sizeof(page)) == 0);
	UT_ASSERT_EQ(parent->delegation_count, 1);
	UT_ASSERT_EQ(parent->delegation_close_request_id[3], request.request_id);
	UT_ASSERT(memcmp(parent, &expected, sizeof(expected)) == 0);
	UT_ASSERT_EQ(cluster_ctrc_normal_stop_poll(&observed), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(enters, leaves);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(allocated, 0);
}

UT_TEST(test_subtree_codec_rejects_stale_request_and_malformed_proof_without_partial_output)
{
	ClusterCtrcParticipantEntry *parent, *leaf;
	ClusterCtrcDelegationCloseV1 upstream, request;
	uint8 reply[GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE];
	uint8 page[BLCKSZ];
	ClusterCtrcDelegatedSubtreeAckV1 ack;

	UT_ASSERT(subtree_roundtrip_setup(&parent, &leaf, &upstream, &request, reply));
	for (unsigned fault = 0; fault < 18; fault++) {
		ClusterCtrcDelegationCloseV1 stale = request;
		memcpy(page, reply + sizeof(GcsBlockReplyHeader), sizeof(page));
		switch (fault) {
			case 0: page[4] = 3; break;
			case 1: page[40] = CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK; break;
			case 2: page[46] = 0; break;
			case 3: page[48] ^= 1; break;
			case 4: page[CLUSTER_CTRC_REPLY_ACK_OFFSET + 64] ^= 1; break;
			case 5: page[CLUSTER_CTRC_REPLY_ACK_OFFSET + 416] = 0; break;
			case 6: page[CLUSTER_CTRC_REPLY_ACK_OFFSET + 424] ^= 1; break;
			case 7: page[CLUSTER_CTRC_REPLY_ACK_OFFSET + 456] ^= 1; break;
			case 8: page[CLUSTER_CTRC_REPLY_ACK_OFFSET + 584] ^= 1; break;
			case 9: page[CLUSTER_CTRC_REPLY_ACK_OFFSET + 588] = 1; break;
			case 10: page[CLUSTER_CTRC_REPLY_ACK_OFFSET + CLUSTER_CTRC_SUBTREE_ACK_BYTES] = 1; break;
			case 11: stale.original_full_xid = FullTransactionIdFromEpochAndXid(8, request.key.xid); break;
			case 12: stale.request_id++; break;
			case 13: stale.delegation_sequence++; break;
			case 14: stale.receiver.boot_incarnation++; break;
			case 15: stale.parent.admission_record_generation++; break;
			case 16: stale.seal_generation++; break;
			case 17: stale.grant_generation++; break;
		}
		ctrc_put_u32_le(page + 60, ctrc_crc32c_exact(page, 60));
		MemSet(&ack, 0x5a, sizeof(ack));
		UT_ASSERT(!cluster_ctrc_delegated_ack_reply_decode(page, sizeof(page), &stale, &ack));
		UT_ASSERT(ctrc_bytes_zero(&ack, sizeof(ack)));
	}
}

UT_TEST(test_actual_subtree_landing_rejects_old_wire_correlation_and_current_source_failures)
{
	for (unsigned fault = 0; fault < 20; fault++) {
		ClusterCtrcParticipantEntry *parent, *leaf, before;
		ClusterCtrcDelegationCloseV1 upstream, request;
		uint8 reply[GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE];
		GcsBlockReplyHeader *outer = (GcsBlockReplyHeader *)reply;
		uint8 *page = reply + sizeof(*outer);
		volatile bool caught = false;

		UT_ASSERT(subtree_roundtrip_setup(&parent, &leaf, &upstream, &request, reply));
		rechecks = resolves = 0;
		switch (fault) {
			case 0: page[4] = 3; break;
			case 1: page[40] = CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK; break;
			case 2: page[48] ^= 1; break;
			case 3: page[CLUSTER_CTRC_REPLY_ACK_OFFSET + 584] ^= 1; break;
			case 4: outer->request_id++; break;
			case 5: parent->delegation_close_request_id[3]++; break;
			case 6: parent->delegations[3].receiver.boot_incarnation++; break;
			case 7: parent->seal_generation++; break;
			case 8: parent->original_full_xid = FullTransactionIdFromEpochAndXid(8, request.key.xid); break;
			case 9: dispatch_excluded_member = 3; break;
			case 10: dispatch_observed_boot_delta = 1; break;
			case 11: dispatch_self_boot++; break;
			case 12: close_source_generation = 0; break;
			case 13: close_source_generation++; break;
			case 14: root_exact = false; break;
			case 15: fail_recheck = 1; break;
			case 16: fail_recheck = 2; break;
			case 17: fence_ok = false; break;
			case 18: data_plane = false; break;
			case 19: throw_resolve = 1; break;
		}
		ctrc_put_u32_le(page + 60, ctrc_crc32c_exact(page, 60));
		outer->checksum = gcs_block_compute_checksum((const char *)page);
		before = *parent;
		PG_TRY();
		{
			UT_ASSERT(close_merge_land(reply));
		}
		PG_CATCH();
		{
			caught = true;
			UT_ASSERT_EQ(fault, 19);
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 19);
		UT_ASSERT(memcmp(parent, &before, sizeof(before)) == 0);
		UT_ASSERT_EQ(enters, leaves);
		UT_ASSERT_EQ(held_count, 0);
		UT_ASSERT_EQ(allocated, 0);
	}
}

UT_TEST(test_subtree_proof_cannot_import_ancestors_or_replace_already_retained_evidence)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		ClusterCtrcParticipantEntry *parent, *leaf, before;
		ClusterCtrcDelegationCloseV1 upstream, request;
		ClusterCtrcDelegatedSubtreeAckV1 ack;
		uint8 reply[GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE], body[CLUSTER_CTRC_SUBTREE_ACK_BYTES];

		UT_ASSERT(subtree_roundtrip_setup(&parent, &leaf, &upstream, &request, reply));
		UT_ASSERT(cluster_ctrc_delegated_ack_reply_decode(reply + sizeof(GcsBlockReplyHeader),
			BLCKSZ, &request, &ack));
		if (fault == 6 || fault == 7)
			UT_ASSERT(close_merge_land(reply));
		switch (fault) {
			case 0:
				/* A framed, checksummed child cannot claim an ancestor as a descendant. */
				ack.subtree_bitmap |= UINT64_C(1) << upstream.parent.node_id;
				ack.children_digest_sha256[0] ^= 1;
				break;
			case 1: ack.subtree_bitmap |= UINT64_C(1) << request.key.origin_node_id; break;
			case 2: ack.subtree_bitmap |= UINT64_C(1) << request.parent.node_id; break;
			case 3: ack.subtree_bitmap |= UINT64_C(1) << CLUSTER_CTRC_MAX_PARTICIPANTS; break;
			case 4: ack.subtree_bitmap = 0; break;
			case 5: MemSet(ack.children_digest_sha256, 0, 32); break;
			case 6: ack.required_lsn[0]++; break;
			case 7: request.delegation_sequence++; break;
			case 8: request.original_full_xid = FullTransactionIdFromEpochAndXid(8, request.key.xid); break;
		}
		/* Refresh framing where the malformed semantic proof is encodable;
		 * do not let a checksum failure stand in for the parent/tree guard. */
		if (ctrc_subtree_ack_encode_body(&request, &ack, body))
			ack.crc32c = ctrc_get_u32_le(body + 584);
		before = *parent;
		UT_ASSERT(!cluster_ctrc_participant_delegated_ack_land_shared(&request, &ack));
		UT_ASSERT(memcmp(parent, &before, sizeof(before)) == 0);
		UT_ASSERT_EQ(parent->delegation_count, 1);
		UT_ASSERT_EQ(enters, leaves);
		UT_ASSERT_EQ(held_count, 0);
		UT_ASSERT_EQ(allocated, 0);
	}
}

/* The original144 is granted directly by origin0 to root1. Root and child
 * use independent local pools; only the original path and protected reply
 * cross between them. This does not manufacture an accepted parent. */
static bool
direct_root_roundtrip_setup(ClusterCtrcReceiptHandle *root,
	ClusterCtrcDelegationCloseV1 *child_request,
	uint8 reply[GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE], bool cancel_local)
{
	ClusterCtrcParticipantIdentity child;
	ClusterCtrcReceiptHandle receipt;
	ClusterCtrcSharedHeader *root_pool;
	ClusterCtrcLocalReleaseAckV1 denied;
	ClusterCtrcTargetV1 pending;
	ClusterItlSlotData slot = {0};
	uint8 original[144], path[CLUSTER_CTRC_DELEGATION_V2_MAX_BYTES];
	Size length;
	uint16 reason;
	uint32 first;
	bool ready;

	free(subtree_leaf_pool);
	subtree_leaf_pool = NULL;
	fixture_declared_nodes = 4;
	reset_dispatch_fixture();
	if (!seed_delegation_sender(root, &child, original)
		|| !close_test_pending_child(root->participant, &child, path, &length)
		|| (cancel_local && !cluster_ctrc_receipt_cancel_shared(root)))
		return false;
	cluster_node_id = root->participant->identity.node_id;
	dispatch_self_boot = root->participant->identity.boot_incarnation;
	if (cluster_ctrc_participant_request_shared(&root->key, &root->participant->identity,
		root->participant->grant_generation, 41, CTRC_SEAL_CLOSE_AND_CLEAN,
		&reason, &denied) != CTRC_SEAL_REPLY_PENDING_DRAIN)
		return false;
	first = ((uint32)root->key.owner_instance - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	if (!cluster_ctrc_cleaner_bind_worker((root->key.segment_id - first)
		% CLUSTER_CTRC_CLEANER_WORKERS)
		|| !cluster_ctrc_participant_next_delegated_close_shared(child_request))
		return false;
	root_pool = CtrcShared;
	CtrcShared = NULL;
	cluster_ctrc_shmem_init();
	subtree_leaf_pool = CtrcShared;
	cluster_node_id = child.node_id;
	slot.xid = root->key.xid;
	slot.wrap = 3;
	slot.flags = ITL_FLAG_ACTIVE;
	slot.undo_segment_head = uba_encode(root->key.segment_id, 9, root->key.slot_offset, 8);
	image_pending_target(&pending);
	ready = cluster_ctrc_itl_image_prepare_v2(&slot, &root->participant->identity,
		&child, path, length, &pending, &receipt) == CLUSTER_CTRC_PREPARE_READY
		&& cluster_ctrc_receipt_cancel_shared(&receipt)
		&& subtree_send_close(child_request)
		&& ctrc_participant_freeze_ack_batch_shared(1) == 1
		&& subtree_send_close(child_request);
	if (ready)
		memcpy(reply, close_reply, GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE);
	CtrcShared = root_pool;
	cluster_node_id = root->participant->identity.node_id;
	dispatch_self_boot = root->participant->identity.boot_incarnation;
	return ready;
}

UT_TEST(test_direct_root_consumes_child_proof_before_freezing_local_inventory)
{
	ClusterCtrcReceiptHandle root;
	ClusterCtrcParticipantEntry before, expected;
	ClusterCtrcDelegationCloseV1 child, ignored;
	ClusterCtrcLocalReleaseAckV1 denied, ack;
	ClusterCtrcNormalStopObservation observed;
	uint8 reply[GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE];
	uint64 index;
	uint16 reason;

	UT_ASSERT(direct_root_roundtrip_setup(&root, &child, reply, true));
	UT_ASSERT(ctrc_bytes_zero(&root.participant->delegation_parent,
		sizeof(root.participant->delegation_parent)));
	UT_ASSERT(ctrc_bytes_zero(&root.participant->delegation_path,
		sizeof(root.participant->delegation_path)));
	UT_ASSERT_EQ(root.receipt->state, CTRC_RECEIPT_CANCELLED);
	UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(1), 0);
	UT_ASSERT(close_merge_land(reply));
	UT_ASSERT(!cluster_ctrc_participant_next_delegated_close_shared(&ignored));
	before = *root.participant;
	UT_ASSERT(ctrc_participant_index(&root.key, root.participant->identity.node_id, &index));
	/* The original cleaner must now consume the completed child inventory. */
	UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(1), 1);
	expected = before;
	expected.ack_frozen_count = 1;
	UT_ASSERT(memcmp(root.participant, &expected, sizeof(expected)) == 0);
	UT_ASSERT_EQ(root.receipt->state, CTRC_RECEIPT_ACK_FROZEN);
	ack = ctrc_participant_ack_entries()[index];
	UT_ASSERT(ctrc_ack_bytes_exact(&ack));
	UT_ASSERT_EQ(ack.total_receipt_count, 1);
	UT_ASSERT_EQ(ack.cancelled_count, 1);
	UT_ASSERT(ctrc_delegated_local_ack_summary_exact(root.participant, &ack));
	UT_ASSERT(!ctrc_frozen_ack_summary_exact(root.participant, &ack));
	UT_ASSERT_EQ(cluster_ctrc_participant_request_shared(&root.key, &root.participant->identity,
		root.participant->grant_generation, 41, CTRC_SEAL_CLOSE_AND_CLEAN, &reason, &denied),
		CTRC_SEAL_REPLY_PENDING_DRAIN);
	UT_ASSERT(ctrc_bytes_zero(&denied, sizeof(denied)));
	UT_ASSERT_EQ(cluster_ctrc_participant_request_shared(&root.key, &root.participant->identity,
		root.participant->grant_generation, 41, CTRC_SEAL_CERTIFICATE_COMMITTED, &reason, &denied),
		CTRC_SEAL_REPLY_BLOCKED_RETAIN);
	UT_ASSERT(memcmp(root.participant, &expected, sizeof(expected)) == 0);
	UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(1), 0);
	UT_ASSERT_EQ(cluster_ctrc_normal_stop_poll(&observed), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(enters, leaves);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(allocated, 0);
}

UT_TEST(test_direct_root_local_proof_requires_its_original_full_identity)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		ClusterCtrcReceiptHandle root;
		ClusterCtrcParticipantEntry before;
		ClusterCtrcReceipt before_receipt;
		ClusterCtrcDelegationCloseV1 child;
		uint8 reply[GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE];
		uint64 index;
		unsigned captures;

		UT_ASSERT(direct_root_roundtrip_setup(&root, &child, reply, true));
		UT_ASSERT(close_merge_land(reply));
		UT_ASSERT(ctrc_participant_index(&root.key, root.participant->identity.node_id, &index));
		switch (fault) {
			case 0: MemSet(root.participant->original_identity_v2, 0,
				sizeof(root.participant->original_identity_v2)); break;
			case 1: root.participant->original_full_xid = InvalidFullTransactionId; break;
			case 2: root.participant->original_full_xid = FullTransactionIdFromU64(
				U64FromFullTransactionId(root.original_full_xid) + (UINT64_C(1) << 32)); break;
			case 3: root.participant->original_identity_v2[64] ^= 1; break;
			case 4: root.participant->identity.boot_incarnation++; break;
			case 5: root.participant->original_identity_v2[6] = 1; break;
			case 6: root.participant->grant_generation++; break;
		}
		before = *root.participant;
		before_receipt = *root.receipt;
		captures = durability_calls;
		UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(1), 0);
		UT_ASSERT_EQ(durability_calls, captures);
		UT_ASSERT(memcmp(root.participant, &before, sizeof(before)) == 0);
		UT_ASSERT(memcmp(root.receipt, &before_receipt, sizeof(before_receipt)) == 0);
		UT_ASSERT(ctrc_bytes_zero(&ctrc_participant_ack_entries()[index], sizeof(ClusterCtrcLocalReleaseAckV1)));
		UT_ASSERT_EQ(held_count, 0);
		UT_ASSERT_EQ(allocated, 0);
	}
}

UT_TEST(test_direct_root_local_proof_waits_for_original_local_drain_and_durability)
{
	for (unsigned phase = 0; phase < 2; phase++) {
		ClusterCtrcReceiptHandle root;
		ClusterCtrcParticipantEntry before;
		ClusterCtrcReceipt before_receipt;
		ClusterCtrcDelegationCloseV1 child;
		uint8 reply[GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE];
		uint64 index;

		UT_ASSERT(direct_root_roundtrip_setup(&root, &child, reply, phase != 0));
		UT_ASSERT(close_merge_land(reply));
		UT_ASSERT(ctrc_participant_index(&root.key, root.participant->identity.node_id, &index));
		/* Count the root's original local captures separately from the child. */
		durability_calls = 0;
		if (phase != 0) {
			root.receipt->highest_local_wal_lsn = 64;
			root.receipt->required_lsn[0] = 80;
			test_flush_lsn = 100;
			origin_durability_hook = close_local_remote_durable;
		}
		before = *root.participant;
		before_receipt = *root.receipt;
		UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(1), 0);
		UT_ASSERT(memcmp(root.participant, &before, sizeof(before)) == 0);
		UT_ASSERT(memcmp(root.receipt, &before_receipt, sizeof(before_receipt)) == 0);
		UT_ASSERT(ctrc_bytes_zero(&ctrc_participant_ack_entries()[index], sizeof(ClusterCtrcLocalReleaseAckV1)));
		if (phase == 0) {
			UT_ASSERT_EQ(root.receipt->state, CTRC_RECEIPT_PREPARED);
			UT_ASSERT_EQ(durability_calls, 0);
		} else {
			UT_ASSERT_EQ(durability_calls, 2);
			origin_durability_hook = NULL;
			UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(1), 1);
			UT_ASSERT_EQ(root.receipt->state, CTRC_RECEIPT_ACK_FROZEN);
			UT_ASSERT_EQ(ctrc_participant_ack_entries()[index].required_lsn_vector[0], 80);
			UT_ASSERT_EQ(ctrc_participant_ack_entries()[index].highest_local_cleanout_lsn, 64);
		}
		UT_ASSERT_EQ(held_count, 0);
		UT_ASSERT_EQ(allocated, 0);
	}
}

static void
direct_root_child_proof_drift(void)
{
	local_drift_participant->delegation_ack_digest[2][0] ^= 1;
}

UT_TEST(test_direct_root_local_freeze_rechecks_original_and_child_evidence)
{
	for (unsigned phase = 0; phase < 3; phase++) {
		ClusterCtrcReceiptHandle root;
		ClusterCtrcReceipt before_receipt;
		ClusterCtrcDelegationCloseV1 child;
		ClusterCtrcDelegationCustodyV1 custody;
		uint8 reply[GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE];
		uint64 index;

		UT_ASSERT(direct_root_roundtrip_setup(&root, &child, reply, true));
		UT_ASSERT(close_merge_land(reply));
		UT_ASSERT(ctrc_participant_index(&root.key, root.participant->identity.node_id, &index));
		before_receipt = *root.receipt;
		custody = root.participant->delegations[child.receiver.node_id];
		local_drift_participant = root.participant;
		local_drift_capture = durability_calls + phase + 1;
		if (phase < 2)
			durability_hook = close_local_drift_capture;
		else {
			UT_ASSERT_EQ(child.receiver.node_id, 2);
			barrier_hook = direct_root_child_proof_drift;
			UT_ASSERT(cluster_ctrc_test_barrier_control(CTRC_TEST_BARRIER_ACK_DURABLE, true));
		}
		UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(1), 0);
		UT_ASSERT_EQ(root.participant->state, CTRC_PARTICIPANT_BLOCKED);
		UT_ASSERT(memcmp(root.receipt, &before_receipt, sizeof(before_receipt)) == 0);
		UT_ASSERT(memcmp(&root.participant->delegations[child.receiver.node_id], &custody, sizeof(custody)) == 0);
		UT_ASSERT_EQ(root.participant->delegation_close_request_id[child.receiver.node_id], child.request_id);
		UT_ASSERT(ctrc_bytes_zero(&ctrc_participant_ack_entries()[index], sizeof(ClusterCtrcLocalReleaseAckV1)));
		UT_ASSERT_EQ(held_count, 0);
		UT_ASSERT_EQ(allocated, 0);
	}
}

int
main(void)
{
	UT_PLAN(47);
	UT_RUN(test_actual_delegated_close_enters_drain_and_retains_parent);
	UT_RUN(test_actual_delegated_close_malformed_is_consumed_without_mutation);
	UT_RUN(test_delegated_close_codec_is_explicit_and_roundtrips_all_binding_fields);
	UT_RUN(test_delegated_close_codec_rejects_malformed_without_partial_output);
	UT_RUN(test_delegated_close_shared_requires_the_accepted_parent_not_a_merge_or_raw_key);
	UT_RUN(test_actual_delegated_close_current_sources_and_target_are_required);
	UT_RUN(test_delegated_close_duplicate_keeps_downstream_stub_and_blocks_all_release);
	UT_RUN(test_delegated_close_replies_bind_the_whole_request_and_never_grant_release);
	UT_RUN(test_actual_delegated_close_error_leaves_original_admission_and_retains_custody);
	UT_RUN(test_actual_multihop_close_keeps_the_complete_accepted_path);
	UT_RUN(test_actual_delegated_close_keeps_connection_and_admission_domains_separate);
	UT_RUN(test_original_background_pass_forwards_closed_child_custody);
	UT_RUN(test_background_close_refusal_retries_the_same_request_without_retiring_custody);
	UT_RUN(test_background_selector_rejects_unqualified_or_exhausted_rows_without_partial_output);
	UT_RUN(test_background_dispatch_requires_current_hello_admission_root_and_exact_custody);
	UT_RUN(test_background_dispatch_uses_hello_connection_and_preserves_open_custody_generation);
	UT_RUN(test_background_error_cleans_original_batch_and_admission_but_keeps_retry_identity);
	UT_RUN(test_background_selector_has_one_circumference_budget_and_fair_child_rotation);
	UT_RUN(test_background_sequence_allocation_rechecks_the_exact_row_after_original_lock_gap);
	UT_RUN(test_qualified_existing_receiver_reports_merge_without_changing_its_parent);
	UT_RUN(test_actual_merge_reply_retires_only_the_exact_redundant_pending);
	UT_RUN(test_merge_pending_retirement_never_renumbers_other_children);
	UT_RUN(test_merge_result_is_only_explicit_wire3_without_any_release_payload);
	UT_RUN(test_merge_observation_never_qualifies_unknown_closed_or_conflicting_receivers);
	UT_RUN(test_actual_merge_reply_rejects_framing_digest_and_stale_request_without_mutation);
	UT_RUN(test_actual_merge_reply_requires_live_boot_hello_root_and_current_admission);
	UT_RUN(test_actual_merge_reply_keeps_hello_separate_and_duplicate_is_a_noop);
	UT_RUN(test_receiver_merge_observation_is_rechecked_before_the_existing_reply);
	UT_RUN(test_merge_landing_error_after_exact_retirement_leaves_admission_once);
	UT_RUN(test_original_cleaner_freezes_local_leaf_proof_without_releasing_accepted_parent);
	UT_RUN(test_delegated_nonempty_local_freeze_keeps_parent_and_ordinary_ack_closed);
	UT_RUN(test_delegated_local_freeze_waits_for_original_prepared_and_child_custody);
	UT_RUN(test_delegated_local_proof_never_qualifies_unknown_original_or_partial_inventory);
	UT_RUN(test_delegated_local_proof_rechecks_full_original_row_after_both_samples);
	UT_RUN(test_delegated_local_receipt_proof_requires_both_original_durable_frontiers);
	UT_RUN(test_delegated_local_freeze_errors_and_oom_keep_all_original_custody);
	UT_RUN(test_delegated_local_reader_rejects_stale_parent_fullxid_and_frozen_chain);
	UT_RUN(test_normal_stop_reports_retained_local_proof_pending_and_corruption_invalid);
	UT_RUN(test_actual_delegated_close_returns_explicit_subtree_proof_after_local_freeze);
	UT_RUN(test_actual_subtree_reply_lands_once_and_parent_aggregates_without_retiring_custody);
	UT_RUN(test_subtree_codec_rejects_stale_request_and_malformed_proof_without_partial_output);
	UT_RUN(test_actual_subtree_landing_rejects_old_wire_correlation_and_current_source_failures);
	UT_RUN(test_subtree_proof_cannot_import_ancestors_or_replace_already_retained_evidence);
	UT_RUN(test_direct_root_consumes_child_proof_before_freezing_local_inventory);
	UT_RUN(test_direct_root_local_proof_requires_its_original_full_identity);
	UT_RUN(test_direct_root_local_proof_waits_for_original_local_drain_and_durability);
	UT_RUN(test_direct_root_local_freeze_rechecks_original_and_child_evidence);
	free(subtree_leaf_pool);
	free(CtrcShared);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
