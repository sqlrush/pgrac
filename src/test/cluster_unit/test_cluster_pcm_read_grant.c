/* First-disk grant executes the production PCM entry-lock owner. */
int pcm_read_grant_fixture_main(void);
#define main pcm_read_grant_fixture_main
#include "test_cluster_pcm_lock.c"
#undef main

bool
cluster_gcs_block_read_source_quiesce(BufferDesc *buf pg_attribute_unused(),
									  const ClusterPcmReadSource *source pg_attribute_unused())
{
	abort();
}
ClusterPcmOwnEntry *ClusterPcmOwnArray;

UT_TEST(test_first_disk_grant_is_atomic_and_rejects_foreign_current_copies)
{
	BufferTag tag = make_tag(94);
	PcmAuthoritySnapshot before, after;
	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_disk_read_grant(tag, 2), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &before));
	UT_ASSERT_EQ(before.state, PCM_STATE_S);
	UT_ASSERT_EQ(before.s_holders_bitmap, 4);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_disk_read_grant(tag, 2), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_disk_read_grant(tag, 1), PCM_GCS_TRANSITION_INCOMPATIBLE);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ(after.state, before.state);
	UT_ASSERT_EQ(after.s_holders_bitmap, before.s_holders_bitmap);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(tag, PCM_TRANS_N_TO_S, 1),
				 PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_disk_read_grant(tag, 2), PCM_GCS_TRANSITION_INCOMPATIBLE);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(tag, PCM_TRANS_S_TO_N_RELEASE, 1),
				 PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(tag, PCM_TRANS_S_TO_N_RELEASE, 2),
				 PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(tag, PCM_TRANS_N_TO_X, 0),
				 PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_disk_read_grant(tag, 0), PCM_GCS_TRANSITION_INCOMPATIBLE);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_disk_read_grant(tag, 1), PCM_GCS_TRANSITION_INCOMPATIBLE);
}

UT_TEST(test_first_disk_grant_does_not_discharge_pi_or_conversion_responsibility)
{
	BufferTag tag = make_tag(95);
	struct StopPcmEntryLayout *entry;
	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(tag, PCM_TRANS_N_TO_X, 0),
				 PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(tag, PCM_TRANS_X_TO_N_DOWNGRADE, 0),
				 PCM_GCS_TRANSITION_APPLIED);
	entry = hash_search((HTAB *)&fake_pcm_htab_token, &tag, HASH_FIND, NULL);
	UT_ASSERT_NOT_NULL(entry);
	UT_ASSERT_EQ(pg_atomic_read_u32(&entry->pi_holders_bitmap), 1);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_disk_read_grant(tag, 1), PCM_GCS_TRANSITION_INCOMPATIBLE);
	UT_ASSERT_EQ(pg_atomic_read_u32(&entry->pi_holders_bitmap), 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&entry->master_state), PCM_STATE_N);
	tag = make_tag(96);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_disk_read_grant(tag, 0), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(tag, PCM_TRANS_S_TO_N_RELEASE, 0),
		PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(cluster_pcm_lock_set_pending_x(tag, 1, 41));
	UT_ASSERT_EQ(cluster_pcm_lock_apply_disk_read_grant(tag, 2), PCM_GCS_TRANSITION_PENDING_X);
	cluster_pcm_lock_clear_pending_x_if(tag, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_disk_read_grant(tag, 2), PCM_GCS_TRANSITION_APPLIED);
}
static ClusterPcmReadGrant
begin_read(BufferTag tag, int r, int b, uint64 id)
{
	ClusterPcmReadGrant grant;
	UT_ASSERT_EQ(cluster_pcm_lock_read_grant_begin(tag, r, b, id, 1, 10000, true, &grant),
		PCM_GCS_TRANSITION_APPLIED);
	return grant;
}

UT_TEST(test_pending_s_precedes_x_and_confirms_exactly)
{
	BufferTag tag = make_tag(111);
	ClusterPcmReadGrant grant, duplicate;
	reset_fake_pcm_runtime(4);
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S, 0));
	grant = begin_read(tag, 1, 7, 21);
	UT_ASSERT_EQ(grant.holder_node, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(tag), 1);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(tag, PCM_TRANS_S_TO_X_UPGRADE, 0),
		PCM_GCS_TRANSITION_PENDING_X);
	UT_ASSERT_EQ(cluster_pcm_lock_read_grant_begin(tag, 1, 7, 21, 1, 99999, false, &duplicate),
		PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(grant.binding_generation, duplicate.binding_generation);
	duplicate.key.backend_id++;
	UT_ASSERT(!cluster_pcm_lock_read_grant_done(&duplicate.key, 1, true));
	UT_ASSERT(!cluster_pcm_lock_read_grant_done(&grant.key, 0, true));
	UT_ASSERT(cluster_pcm_lock_read_grant_done(&grant.key, 1, true));
	UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(tag), 3);
	UT_ASSERT(!cluster_pcm_lock_read_grant_done(&grant.key, 1, true));
}

UT_TEST(test_native_resource_x_queue_waits_for_pending_s_before_arming_blockers)
{
	BufferTag tag = make_tag(115);
	ClusterPcmReadGrant grant;
	ResourceXDecodedFrame writer;
	ResourceXMasterSnapshot queued;
	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S, 0));
	grant = begin_read(tag, 1, 7, 30);
	writer = make_resource_x_master_frame(RESOURCE_X_WIRE_ASSERT_X, tag, 2, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(&writer, 2, &queued), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(queued.is_head);
	UT_ASSERT_EQ(queued.phase, RESOURCE_X_MASTER_QUEUED);
	UT_ASSERT_EQ(queued.incompatible_holders_bitmap, 0);
	UT_ASSERT(cluster_pcm_lock_read_grant_done(&grant.key, 1, true));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&writer.common.logical_assertion, &queued), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(queued.phase, RESOURCE_X_MASTER_WAIT_BLOCKERS);
	UT_ASSERT_EQ(queued.incompatible_holders_bitmap, 3);
}

UT_TEST(test_timeout_requires_revoke_and_late_done_cannot_resurrect)
{
	BufferTag tag = make_tag(112);
	ClusterPcmReadGrant grant, work;
	uint32 cursor = 0;
	reset_fake_pcm_runtime(4);
	grant = begin_read(tag, 1, 8, 22);
	UT_ASSERT_EQ(grant.holder_node, -1);
	UT_ASSERT(!cluster_pcm_lock_read_grant_revoke_ack(&grant.key));
	UT_ASSERT(cluster_pcm_lock_read_grant_work(&cursor, 1, 2, 10001, 1, &work));
	UT_ASSERT(work.revoking);
	UT_ASSERT(!cluster_pcm_lock_read_grant_done(&grant.key, 1, true));
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(tag, PCM_TRANS_S_TO_X_UPGRADE, 1),
		PCM_GCS_TRANSITION_PENDING_X);
	work.key.request_id++;
	UT_ASSERT(!cluster_pcm_lock_read_grant_revoke_ack(&work.key));
	UT_ASSERT(cluster_pcm_lock_read_grant_revoke_ack(&grant.key));
	UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(tag), 0);
	grant = begin_read(tag, 1, 8, 23);
	UT_ASSERT(!cluster_pcm_lock_read_grant_revoke_ack(&work.key));
	UT_ASSERT(cluster_pcm_lock_read_grant_done(&grant.key, 1, true));
}

UT_TEST(test_x_downgrade_confirmation_orders_independent_notify)
{
	for (int notify_first = 0; notify_first < 2; notify_first++) {
		BufferTag tag = make_tag(113);
		ClusterPcmReadGrant grant;
		PcmAuthoritySnapshot state;
		reset_fake_pcm_runtime(4);
		UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_X, 0));
		grant = begin_read(tag, 2, 9, 24);
		UT_ASSERT(grant.holder_was_x);
		if (notify_first)
			UT_ASSERT(cluster_pcm_lock_read_grant_holder_done(&grant.key, 0));
		UT_ASSERT(cluster_pcm_lock_read_grant_done(&grant.key, 2, true));
		UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &state));
		UT_ASSERT_EQ(state.state, PCM_STATE_S);
		UT_ASSERT_EQ(state.s_holders_bitmap, 5);
		UT_ASSERT_EQ(pg_atomic_read_u32(&((struct StopPcmEntryLayout *)hash_search(
			(HTAB *)&fake_pcm_htab_token, &tag, HASH_FIND, NULL))->pi_holders_bitmap), 1);
		UT_ASSERT(!cluster_pcm_lock_read_grant_holder_done(&grant.key, 0));
		UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_S_TO_N_RELEASE, 2));
		UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_S_TO_X_UPGRADE, 0));
		/* A delayed notification belongs to the retired episode, never H's new X. */
		UT_ASSERT(!cluster_pcm_lock_read_grant_holder_done(&grant.key, 0));
		UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &state));
		UT_ASSERT_EQ(state.state, PCM_STATE_X);
	}
}

UT_TEST(test_nack_membership_and_entry_lifetime_keep_pending_responsibility)
{
	BufferTag tag = make_tag(114);
	ClusterPcmReadGrant grant, work;
	uint32 cursor = 0;
	reset_fake_pcm_runtime(4);
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S, 0));
	grant = begin_read(tag, 1, 10, 25);
	UT_ASSERT(!cluster_pcm_lock_read_grant_done(&grant.key, 3, false));
	UT_ASSERT(cluster_pcm_lock_read_grant_done(&grant.key, 0, false));
	UT_ASSERT(cluster_pcm_lock_read_grant_work(&cursor, 1, 2, 1, 1, &work));
	UT_ASSERT(work.revoking);
	UT_ASSERT(!cluster_pcm_lock_read_grant_done(&grant.key, 1, true));
	UT_ASSERT(cluster_pcm_lock_read_grant_revoke_ack(&grant.key));
	grant = begin_read(tag, 1, 10, 26);
	cluster_pcm_lock_cleanup_on_node_dead(1);
	UT_ASSERT(!cluster_pcm_lock_read_grant_done(&grant.key, 1, true));
	grant = begin_read(tag, 2, 11, 27);
	cursor = 0;
	UT_ASSERT(cluster_pcm_lock_read_grant_work(&cursor, 1, 2, 1, 2, &work));
	UT_ASSERT(work.revoking); /* live requester across formation is not forgotten */
	UT_ASSERT(cluster_pcm_lock_read_grant_revoke_ack(&grant.key));
}

/* Execute the production master routing function with only transport mocked.
 * The forward stub checks native pre-registration at the actual send boundary. */
#include "cluster/cluster_gcs_block_dedup.h"
static struct { pg_atomic_uint64 block_forward_sent_count; } read_wire_counters;
static GcsBlockForwardPayload read_forward;
static GcsBlockReplyHeader read_reply;
static unsigned read_forwards, read_replies, read_local_forwards;
static bool read_admit;
static GcsBlockRequestPayload read_request;
static void
gcs_block_send_reply(int32 dest, const GcsBlockRequestPayload *req, GcsBlockReplyStatus status,
	XLogRecPtr lsn pg_attribute_unused(), const char *data pg_attribute_unused())
{
	UT_ASSERT_EQ(dest, req->sender_node);
	memset(&read_reply, 0, sizeof(read_reply));
	read_reply.status = status;
	read_replies++;
}
static bool
gcs_block_forward_send_admitted(int32 holder, const GcsBlockForwardPayload *fwd)
{
	ClusterPcmReadGrant proof;
	UT_ASSERT_EQ(cluster_pcm_lock_read_grant_begin(fwd->tag, fwd->original_requester_node,
		fwd->requester_backend_id, fwd->request_id, fwd->epoch, 99999, false, &proof),
		PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(proof.holder_node, holder);
	UT_ASSERT(GcsBlockForwardPayloadIsFirstFillS(fwd));
	read_forward = *fwd;
	read_forwards++;
	return read_admit;
}
static bool
read_mock_enqueue(uint8 type, uint32 dest, const void *payload, uint16 bytes)
{
	UT_ASSERT_EQ(type, PGRAC_IC_MSG_GCS_BLOCK_FORWARD);
	UT_ASSERT_EQ(dest, cluster_node_id);
	UT_ASSERT_EQ(bytes, sizeof(GcsBlockForwardPayload));
	read_local_forwards++;
	return gcs_block_forward_send_admitted((int32)dest, payload);
}
static ClusterICSendResult
gcs_block_send_envelope_or_loopback(uint8 type, int32 dest, const void *payload, uint32 bytes)
{
	UT_ASSERT_EQ(type, PGRAC_IC_MSG_GCS_BLOCK_REPLY);
	UT_ASSERT_EQ(dest, read_request.sender_node);
	UT_ASSERT_EQ(bytes, sizeof(GcsBlockReplyHeader) + GCS_BLOCK_DATA_SIZE);
	read_reply = *(const GcsBlockReplyHeader *)payload;
	read_replies++;
	return CLUSTER_IC_SEND_DONE;
}
static void
read_mock_install(int worker pg_attribute_unused(), const GcsBlockDedupKey *key,
	GcsBlockReplyStatus status, const GcsBlockReplyHeader *hdr,
	const char *data pg_attribute_unused())
{
	UT_ASSERT_EQ(key->request_id, hdr->request_id);
	UT_ASSERT_EQ(status, hdr->status);
}
#define ClusterGcsBlock (&read_wire_counters)
#define cluster_grd_outbound_enqueue_backend_msg read_mock_enqueue
#define cluster_gcs_block_dedup_install_reply read_mock_install
#define cluster_gcs_block_note_send_outcome(family, result) ((void)(result))
#define gcs_block_compute_checksum(data) 0
#include "test_cluster_pcm_first_fill_master.inc"
#undef gcs_block_compute_checksum
#undef cluster_gcs_block_note_send_outcome
#undef cluster_gcs_block_dedup_install_reply
#undef cluster_grd_outbound_enqueue_backend_msg
#undef ClusterGcsBlock

static GcsBlockDedupKey
read_wire_setup(BufferTag tag)
{
	GcsBlockDedupKey key = {0};
	read_forwards = read_replies = read_local_forwards = 0;
	read_admit = true;
	memset(&read_request, 0, sizeof(read_request));
	read_request.tag = tag;
	read_request.request_id = 71;
	read_request.epoch = 1;
	read_request.sender_node = 1;
	read_request.requester_backend_id = 13;
	read_request.transition_id = PCM_TRANS_N_TO_S;
	GcsBlockRequestPayloadSetReadMiss(&read_request, true);
	key.origin_node_id = 1;
	key.requester_backend_id = 13;
	key.request_id = 71;
	key.cluster_epoch = 1;
	return key;
}
UT_TEST(test_native_master_registers_before_forward_and_replays_behind_x)
{
	BufferTag tag = make_tag(120);
	GcsBlockDedupKey key;
	ClusterPcmReadGrant grant;
	reset_fake_pcm_runtime(4);
	key = read_wire_setup(tag);
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_X, 2));
	gcs_block_first_fill_request(&read_request, &key, 1, true);
	UT_ASSERT_EQ(read_forwards, 1);
	UT_ASSERT_EQ(read_replies, 0); /* R->M / M->H / H->R, no registration RPC */
	UT_ASSERT(GcsBlockForwardPayloadIsDowngradeRequest(&read_forward));
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(tag, PCM_TRANS_N_TO_X, 3),
		PCM_GCS_TRANSITION_PENDING_X);
	UT_ASSERT(cluster_pcm_lock_set_pending_x(tag, 3, 41));
	gcs_block_first_fill_request(&read_request, &key, 1, false);
	UT_ASSERT_EQ(read_forwards, 2); /* exact replay predates the later writer */
	UT_ASSERT_EQ(cluster_pcm_lock_read_grant_begin(tag, 1, 13, 71, 1, 99999, false, &grant),
		PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(cluster_pcm_lock_read_grant_done(&grant.key, 1, true));
	gcs_block_first_fill_request(&read_request, &key, 1, false);
	UT_ASSERT_EQ(read_forwards, 2);
	UT_ASSERT_EQ(read_reply.status, GCS_BLOCK_REPLY_DENIED_PENDING_X);
}
UT_TEST(test_native_master_disk_grant_has_typed_marker_and_no_forward)
{
	BufferTag tag = make_tag(121);
	GcsBlockDedupKey key;
	reset_fake_pcm_runtime(4);
	key = read_wire_setup(tag);
	gcs_block_first_fill_request(&read_request, &key, 1, true);
	UT_ASSERT_EQ(read_replies, 1);
	UT_ASSERT_EQ(read_forwards, 0);
	UT_ASSERT(GcsBlockReplyHeaderHasPendingS(&read_reply));
	UT_ASSERT_EQ(read_reply.status, GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK);
}
UT_TEST(test_native_master_failed_forward_retains_revoke_owner)
{
	BufferTag tag = make_tag(122);
	GcsBlockDedupKey key;
	ClusterPcmReadGrant work;
	uint32 cursor = 0;
	reset_fake_pcm_runtime(4);
	key = read_wire_setup(tag);
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S, 2));
	read_admit = false;
	gcs_block_first_fill_request(&read_request, &key, 1, true);
	UT_ASSERT_EQ(read_reply.status, GCS_BLOCK_REPLY_DENIED_PENDING_X);
	UT_ASSERT(cluster_pcm_lock_read_grant_work(&cursor, 1, 2, 1, 1, &work));
	UT_ASSERT(work.revoking);
	UT_ASSERT(cluster_pcm_lock_read_grant_revoke_ack(&work.key));
}
UT_TEST(test_native_master_holder_loopback_uses_existing_data_ring)
{
	BufferTag tag = make_tag(123);
	GcsBlockDedupKey key;
	reset_fake_pcm_runtime(4);
	key = read_wire_setup(tag);
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S, cluster_node_id));
	gcs_block_first_fill_request(&read_request, &key, 1, true);
	UT_ASSERT_EQ(read_local_forwards, 1);
	UT_ASSERT_EQ(read_forwards, 1);
}
UT_TEST(test_pending_done_and_revoke_echo_are_exact_and_reject_unknown_fields)
{
	GcsBlockDonePayload done = {0};
	GcsBlockInvalidatePayload inv = {0};
	GcsBlockInvalidateAckPayload ack = {0};
	ClusterPcmReadGrantKey key, decoded;
	memset(&key, 0, sizeof(key));
	key.tag = make_tag(124);
	key.request_id = 74;
	key.epoch = 0x123456789abcULL;
	key.backend_id = 16;
	key.requester_node = 3;
	done.tag = key.tag;
	done.request_id = key.request_id;
	done.epoch = key.epoch;
	done.requester_backend_id = key.backend_id;
	done.sender_node = 3;
	done.transition_id = PCM_TRANS_N_TO_S;
	done.reserved_0[0] = GCS_BLOCK_DONE_PENDING_S_CONFIRM;
	done.reserved_0[1] = 3;
	UT_ASSERT(GcsBlockDonePayloadPendingSKey(&done, &decoded));
	UT_ASSERT_EQ(memcmp(&key, &decoded, sizeof(key)), 0);
	done.sender_node = 2;
	UT_ASSERT(!GcsBlockDonePayloadPendingSKey(&done, &decoded));
	done.reserved_0[0] = GCS_BLOCK_DONE_PENDING_S_NACK;
	UT_ASSERT(GcsBlockDonePayloadPendingSKey(&done, &decoded));
	done.reserved_0[0] = GCS_BLOCK_DONE_PENDING_S_HOLDER;
	UT_ASSERT(GcsBlockDonePayloadPendingSKey(&done, &decoded));
	done.reserved_0[18] = 1;
	UT_ASSERT(!GcsBlockDonePayloadPendingSKey(&done, &decoded));
	inv.tag = key.tag;
	inv.request_id = key.request_id;
	inv.epoch = 55; /* authenticated current transport, not original request */
	GcsBlockInvalidatePayloadSetPendingS(&inv, &key);
	ack.tag = key.tag;
	ack.request_id = key.request_id;
	ack.sender_node = 3;
	GcsBlockInvalidateAckPayloadEchoPendingS(&ack, &inv);
	UT_ASSERT(GcsBlockInvalidateAckPayloadPendingSKey(&ack, &decoded));
	UT_ASSERT_EQ(memcmp(&key, &decoded, sizeof(key)), 0);
	ack.reserved_0[6] = 1;
	UT_ASSERT(!GcsBlockInvalidateAckPayloadPendingSKey(&ack, &decoded));
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(test_first_disk_grant_is_atomic_and_rejects_foreign_current_copies);
	UT_RUN(test_first_disk_grant_does_not_discharge_pi_or_conversion_responsibility);
	UT_RUN(test_pending_s_precedes_x_and_confirms_exactly);
	UT_RUN(test_native_resource_x_queue_waits_for_pending_s_before_arming_blockers);
	UT_RUN(test_timeout_requires_revoke_and_late_done_cannot_resurrect);
	UT_RUN(test_x_downgrade_confirmation_orders_independent_notify);
	UT_RUN(test_nack_membership_and_entry_lifetime_keep_pending_responsibility);
	UT_RUN(test_native_master_registers_before_forward_and_replays_behind_x);
	UT_RUN(test_native_master_disk_grant_has_typed_marker_and_no_forward);
	UT_RUN(test_native_master_failed_forward_retains_revoke_owner);
	UT_RUN(test_native_master_holder_loopback_uses_existing_data_ring);
	UT_RUN(test_pending_done_and_revoke_echo_are_exact_and_reject_unknown_fields);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
