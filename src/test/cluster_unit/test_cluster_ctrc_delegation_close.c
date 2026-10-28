/*
 * Actual protected GCS CLOSE ingress and shared delegation lifecycle.
 * Socket, HELLO, membership and R4 providers are controlled process boundaries.
 * No native transport, concurrency, disk durability or subtree ACK claim.
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

int
main(void)
{
	UT_PLAN(19);
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
	free(CtrcShared);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
