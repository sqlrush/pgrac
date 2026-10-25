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
	UT_ASSERT(cluster_pcm_lock_read_grant_retire_notice_admitted(&grant.key));
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
	UT_ASSERT(cluster_pcm_lock_read_grant_retire_notice_admitted(&grant.key));
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
#include "cluster/cluster_membership.h"
#include "cluster/cluster_gcs_reqid.h"
static struct { pg_atomic_uint64 block_forward_sent_count; } read_wire_counters;
static GcsBlockForwardPayload read_forward;
static GcsBlockReplyHeader read_reply;
static unsigned read_forwards, read_replies, read_local_forwards;
static bool read_admit;
static ClusterICSendResult read_notice_result;
static GcsBlockDonePayload read_notice;
static unsigned read_notices, read_revokes, read_cleanup_lookups;
static bool read_cleanup_capacity = true;
static TimestampTz read_master_time;
static unsigned read_retirements;
static unsigned read_ungranted;
static bool read_managed, read_serving, read_quorum, read_member;
static bool read_join_rebuilt, read_recovering, read_home;
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
	if (type == PGRAC_IC_MSG_GCS_BLOCK_DONE) {
		UT_ASSERT_EQ(bytes, sizeof(read_notice));
		UT_ASSERT_EQ(dest, read_request.sender_node);
		read_notice = *(const GcsBlockDonePayload *)payload;
		read_notices++;
		return read_notice_result;
	}
	if (type == PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE) {
		UT_ASSERT_EQ(bytes, sizeof(GcsBlockInvalidatePayload));
		read_revokes++;
		return CLUSTER_IC_SEND_DONE;
	}
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
static bool
read_mock_ungranted(int worker, const GcsBlockDedupKey *key, const BufferTag *tag,
	const GcsBlockReplyHeader *header)
{
	UT_ASSERT(BufferTagsEqual(tag, &read_request.tag));
	read_mock_install(worker, key, (GcsBlockReplyStatus)header->status, header, NULL);
	read_ungranted++;
	return true;
}
static GcsBlockDedupResult
read_cleanup_lookup(int worker pg_attribute_unused(), const GcsBlockDedupKey *key,
	BufferTag tag, uint8 transition, uint32 hint pg_attribute_unused(),
	bool capable pg_attribute_unused(), GcsBlockDedupEntry *cached pg_attribute_unused())
{
	UT_ASSERT_EQ(transition, PCM_TRANS_N_TO_S);
	UT_ASSERT_EQ(key->request_id, read_request.request_id);
	UT_ASSERT(BufferTagsEqual(&tag, &read_request.tag));
	read_cleanup_lookups++;
	return read_cleanup_capacity ? GCS_BLOCK_DEDUP_MISS_REGISTERED : GCS_BLOCK_DEDUP_FULL;
}
static bool
read_cleanup_flags(int worker pg_attribute_unused(), const GcsBlockDedupKey *key,
	const BufferTag *tag, uint8 transition, uint8 flags)
{
	UT_ASSERT_EQ(key->request_id, read_request.request_id);
	UT_ASSERT(BufferTagsEqual(tag, &read_request.tag));
	UT_ASSERT_EQ(transition, PCM_TRANS_N_TO_S);
	UT_ASSERT_EQ(flags, GCS_BLOCK_DEDUP_REQUEST_F_READ_MISS);
	return true;
}
#define cluster_gcs_block_dedup_lookup_or_register read_cleanup_lookup
#define cluster_gcs_block_dedup_set_request_flags_exact read_cleanup_flags
#define ClusterGcsBlock (&read_wire_counters)
#define cluster_grd_outbound_enqueue_backend_msg read_mock_enqueue
#define cluster_gcs_block_dedup_install_reply read_mock_install
#define cluster_gcs_block_dedup_install_ungranted_first_fill_exact read_mock_ungranted
#define cluster_authority_readiness_managed() read_managed
#define cluster_serving_ready_check(pending, failed) ((void)(pending), (void)(failed), read_serving)
#define cluster_qvotec_in_quorum() read_quorum
#define cluster_membership_is_member(node) read_member
#define cluster_membership_get_state(node) CLUSTER_MEMBER_MEMBER
#define cluster_grd_join_remaster_active_for_shard(tag) true
#define cluster_grd_block_view_rebuilt(tag) read_join_rebuilt
#define cluster_gcs_block_phase_for_tag(tag) (read_recovering ? GCS_BLOCK_RECOVERING : GCS_BLOCK_NORMAL)
#define cluster_gcs_lookup_master(tag) (read_home ? cluster_node_id : cluster_node_id + 1)
#define cluster_gcs_block_note_send_outcome(family, result) ((void)(result))
#define gcs_block_compute_checksum(data) 0
#define cluster_block_io_trace_pending_s(key, phase, state, holder, binding, result) ((void)0)
#define cluster_gcs_block_dedup_mark_done(worker, key, tag, transition) (++read_retirements, true)
#define GetCurrentTimestamp() read_master_time
#define cluster_epoch_get_current() read_request.epoch
#define cluster_block_io_trace_master(key, event, result) ((void)0)
#define cluster_ic_suppress_gcs_done_cap false
#define cluster_sf_peer_supports_gcs_done(node) true
#define cluster_ic_tier1_my_data_channel() 1
#define gcs_block_compute_invalidate_checksum(inv) 0
#define cluster_lms_workers 2
static void gcs_first_fill_receipt_tick(int worker pg_attribute_unused()) { }
#include "test_cluster_pcm_first_fill_master.inc"
#undef cluster_lms_workers
#undef gcs_block_compute_invalidate_checksum
#undef cluster_ic_tier1_my_data_channel
#undef cluster_sf_peer_supports_gcs_done
#undef cluster_ic_suppress_gcs_done_cap
#undef cluster_block_io_trace_master
#undef cluster_epoch_get_current
#undef GetCurrentTimestamp
#undef cluster_gcs_lookup_master
#undef cluster_gcs_block_phase_for_tag
#undef cluster_grd_block_view_rebuilt
#undef cluster_grd_join_remaster_active_for_shard
#undef cluster_membership_get_state
#undef cluster_membership_is_member
#undef cluster_qvotec_in_quorum
#undef cluster_serving_ready_check
#undef cluster_authority_readiness_managed
#undef cluster_gcs_block_dedup_install_ungranted_first_fill_exact
#undef cluster_gcs_block_dedup_mark_done
#undef cluster_block_io_trace_pending_s
#undef gcs_block_compute_checksum
#undef cluster_gcs_block_note_send_outcome
#undef cluster_gcs_block_dedup_install_reply
#undef cluster_grd_outbound_enqueue_backend_msg
#undef ClusterGcsBlock
#undef cluster_gcs_block_dedup_lookup_or_register
#undef cluster_gcs_block_dedup_set_request_flags_exact

static GcsBlockDedupKey
read_wire_setup(BufferTag tag)
{
	GcsBlockDedupKey key = {0};
	read_forwards = read_replies = read_local_forwards = 0;
	read_notices = read_revokes = read_cleanup_lookups = 0;
	read_cleanup_capacity = true;
	read_notice_result = CLUSTER_IC_SEND_NOT_ADMITTED;
	read_retirements = 0;
	read_ungranted = 0;
	read_managed = read_serving = read_quorum = read_member = true;
	read_join_rebuilt = read_home = true;
	read_recovering = false;
	read_master_time = 1;
	memset(gcs_first_fill_master_parks, 0, sizeof(gcs_first_fill_master_parks));
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
	UT_ASSERT_EQ(read_reply.status, GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT);
}

UT_TEST(test_native_master_waits_behind_existing_conversion_without_reader_resend)
{
	BufferTag tag = make_tag(125);
	GcsBlockDedupKey key;
	reset_fake_pcm_runtime(4);
	key = read_wire_setup(tag);
	UT_ASSERT(cluster_pcm_lock_set_pending_x(tag, 2, 41));
	gcs_block_first_fill_request(&read_request, &key, 1, true);
	UT_ASSERT_EQ(read_forwards + read_replies, 0);
	UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
	read_master_time += 10000;
	gcs_first_fill_master_tick(1);
	UT_ASSERT_EQ(read_forwards + read_replies, 0);
	cluster_pcm_lock_clear_pending_x_if(tag, 2);
	read_master_time += 10000;
	gcs_first_fill_master_tick(1);
	UT_ASSERT_EQ(read_replies, 1);
	UT_ASSERT_EQ(read_reply.status, GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK);
	UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
}

UT_TEST(test_native_master_cancel_retires_unpublished_request_without_grant)
{
	BufferTag tag = make_tag(126);
	GcsBlockDedupKey key;
	ClusterPcmReadGrantKey cancel = {0};
	reset_fake_pcm_runtime(4);
	key = read_wire_setup(tag);
	UT_ASSERT(cluster_pcm_lock_set_pending_x(tag, 2, 41));
	gcs_block_first_fill_request(&read_request, &key, 1, true);
	cancel.tag = tag;
	cancel.request_id = read_request.request_id;
	cancel.epoch = read_request.epoch;
	cancel.requester_node = read_request.sender_node;
	cancel.backend_id = read_request.requester_backend_id + 1;
	UT_ASSERT(!gcs_first_fill_master_cancel(&cancel, 1));
	cancel.backend_id--;
	UT_ASSERT(gcs_first_fill_master_cancel(&cancel, 1));
	UT_ASSERT_EQ(read_retirements, 0);
	UT_ASSERT_EQ(read_forwards, 0);
	UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
	UT_ASSERT(gcs_first_fill_master_parks[0].cancelling);
	read_master_time += 10000;
	gcs_first_fill_master_tick(1);
	UT_ASSERT_EQ(read_revokes, 1);
	UT_ASSERT(gcs_first_fill_master_cancel_ack(&cancel));
	gcs_first_fill_master_tick(1);
	UT_ASSERT_EQ(read_notices, 1);
	UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
	read_notice_result = CLUSTER_IC_SEND_WOULD_BLOCK;
	read_master_time += 10000;
	gcs_first_fill_master_tick(1);
	UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
	UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(tag), 0);
}

UT_TEST(test_delayed_master_rechecks_each_native_admission_gate)
{
	for (int gate = 0; gate < 6; gate++) {
		BufferTag tag = make_tag(127 + gate);
		GcsBlockDedupKey key;
		reset_fake_pcm_runtime(4);
		key = read_wire_setup(tag);
		UT_ASSERT(cluster_pcm_lock_set_pending_x(tag, 2, 41));
		gcs_block_first_fill_request(&read_request, &key, 1, true);
		cluster_pcm_lock_clear_pending_x_if(tag, 2);
		switch (gate) {
		case 0: read_serving = false; break;
		case 1: read_member = false; break;
		case 2: read_join_rebuilt = false; break;
		case 3: read_recovering = true; break;
		case 4: read_home = false; break;
		case 5: read_managed = false; read_quorum = false; break;
		}
		for (int tick = 0; tick < 3; tick++) {
			read_master_time += 10000;
			gcs_first_fill_master_tick(1);
			UT_ASSERT_EQ(read_forwards + read_replies, 0);
			UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
			UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(tag), 0);
		}
		read_serving = read_member = read_join_rebuilt = read_home = read_quorum = true;
		read_recovering = false;
		read_master_time += 10000;
		gcs_first_fill_master_tick(1);
		UT_ASSERT_EQ(read_replies, 1);
		UT_ASSERT_EQ(read_reply.status, GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK);
		UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
	}
}

UT_TEST(test_master_seals_only_proven_never_granted_terminal_denial)
{
	BufferTag tag = make_tag(134);
	GcsBlockDedupKey key;
	reset_fake_pcm_runtime(4);
	key = read_wire_setup(tag);
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_X, 2));
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_X_TO_N_DOWNGRADE, 2));
	gcs_block_first_fill_request(&read_request, &key, 1, true);
	UT_ASSERT_EQ(read_forwards, 0);
	UT_ASSERT_EQ(read_reply.status, GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT);
	UT_ASSERT_EQ(read_ungranted, 1);
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

UT_TEST(test_revoke_ack_keeps_exact_consumption_notice_until_transport_handoff)
{
	BufferTag tag = make_tag(128);
	ClusterPcmReadGrant grant, work;
	uint32 cursor = 0;
	reset_fake_pcm_runtime(4);
	grant = begin_read(tag, 1, 8, 81);
	UT_ASSERT(cluster_pcm_lock_read_grant_done(&grant.key, 1, false));
	UT_ASSERT(cluster_pcm_lock_read_grant_revoke_ack(&grant.key));
	/* Consuming the ACK is not delivery of that fact to the quiet receipt.
	 * The original bounded claim must still own an exact background work item. */
	UT_ASSERT(cluster_pcm_lock_read_grant_work(&cursor, 1, 2, 10001, 1, &work));
	UT_ASSERT(work.ack_consumed);
	UT_ASSERT(!work.revoking);
	UT_ASSERT_EQ(work.key.request_id, grant.key.request_id);
	UT_ASSERT_EQ(work.key.backend_id, grant.key.backend_id);
	UT_ASSERT(BufferTagsEqual(&work.key.tag, &grant.key.tag));
	UT_ASSERT(!cluster_pcm_lock_read_grant_done(&grant.key, 1, false));
	UT_ASSERT(cluster_pcm_lock_read_grant_revoke_ack(&grant.key));
	work.key.request_id++;
	UT_ASSERT(!cluster_pcm_lock_read_grant_retire_notice_admitted(&work.key));
	UT_ASSERT(cluster_pcm_lock_read_grant_retire_notice_admitted(&grant.key));
	UT_ASSERT(!cluster_pcm_lock_read_grant_work(&cursor, 1, 2, 30001, 1, &work));
	grant = begin_read(tag, 1, 8, 82);
	UT_ASSERT(!cluster_pcm_lock_read_grant_revoke_ack(&work.key));
}

UT_TEST(test_master_not_admitted_notice_keeps_native_claim_until_real_send_takeover)
{
	for (int which = 0; which < 2; which++) {
		BufferTag tag = make_tag(129);
		ClusterPcmReadGrant grant, next;
		uint32 cursor = 0;
		reset_fake_pcm_runtime(4);
		(void)read_wire_setup(tag);
		grant = begin_read(tag, 1, 13, read_request.request_id);
		UT_ASSERT(cluster_pcm_lock_read_grant_done(&grant.key, 1, false));
		UT_ASSERT(cluster_pcm_lock_read_grant_revoke_ack(&grant.key));
		cluster_gcs_block_pending_s_tick();
		UT_ASSERT_EQ(read_notices, 1);
		UT_ASSERT_EQ(read_notice.reserved_0[0], GCS_BLOCK_DONE_PENDING_S_RETIRED);
		UT_ASSERT_EQ(read_notice.request_id, grant.key.request_id);
		UT_ASSERT_EQ(cluster_pcm_lock_read_grant_begin(tag, 1, 13, 99, 1, 10000, true, &next),
			PCM_GCS_TRANSITION_PENDING_X);
		read_master_time += 10000;
		read_notice_result = which ? CLUSTER_IC_SEND_WOULD_BLOCK : CLUSTER_IC_SEND_DONE;
		cluster_gcs_block_pending_s_tick();
		UT_ASSERT_EQ(read_notices, 2);
		UT_ASSERT(!cluster_pcm_lock_read_grant_work(&cursor, 1, 2, 30000, 1, &next));
		cluster_gcs_block_pending_s_tick();
		UT_ASSERT_EQ(read_notices, 2);
	}
}

UT_TEST(test_requester_nack_can_admit_cleanup_after_initial_master_capacity_refusal)
{
	BufferTag tag = make_tag(139);
	GcsBlockDedupKey wire;
	ClusterPcmReadGrantKey key = {0};
	reset_fake_pcm_runtime(4); wire = read_wire_setup(tag);
	wire.request_id = read_request.request_id = gcs_reqid_requester(1, 12, 71);
	key.tag = tag; key.request_id = wire.request_id; key.epoch = wire.cluster_epoch;
	key.requester_node = wire.origin_node_id; key.backend_id = wire.requester_backend_id;
	/* The original request was refused before either a native grant or park
	 * existed. The retained requester NACK, not another REQUEST, supplies the
	 * exact cleanup identity when the master's bounded capacity is available. */
	for (int i = 0; i < GCS_FIRST_FILL_PARK_MAX; i++)
		gcs_first_fill_master_parks[i].in_use = true;
	UT_ASSERT(!gcs_first_fill_master_cancel(&key, 1));
	UT_ASSERT_EQ(read_cleanup_lookups, 0);
	for (int i = 0; i < GCS_FIRST_FILL_PARK_MAX; i++)
		UT_ASSERT(gcs_first_fill_master_parks[i].in_use);
	/* The existing owners complete and make bounded capacity available. */
	memset(gcs_first_fill_master_parks, 0, sizeof(gcs_first_fill_master_parks));
	read_cleanup_capacity = false;
	UT_ASSERT(!gcs_first_fill_master_cancel(&key, 1));
	UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
	read_cleanup_capacity = true;
	UT_ASSERT(gcs_first_fill_master_cancel(&key, 1));
	UT_ASSERT_EQ(read_cleanup_lookups, 2);
	UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
	UT_ASSERT(gcs_first_fill_master_parks[0].cancelling);
	UT_ASSERT_EQ(read_forwards, 0);
	gcs_first_fill_master_tick(1);
	UT_ASSERT_EQ(read_revokes, 1);
	UT_ASSERT(gcs_first_fill_master_cancel_ack(&key));
	gcs_first_fill_master_tick(1);
	UT_ASSERT_EQ(read_notices, 1);
	UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
	read_notice_result = CLUSTER_IC_SEND_DONE; read_master_time += 10000;
	gcs_first_fill_master_tick(1);
	UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
}

UT_TEST(test_absent_old_key_cleanup_does_not_consume_successor_native_grant)
{
	BufferTag tag = make_tag(140);
	ClusterPcmReadGrant successor, replay;
	ClusterPcmReadGrantKey old = {0};
	GcsBlockDedupKey wire;
	reset_fake_pcm_runtime(4); wire = read_wire_setup(tag);
	wire.request_id = read_request.request_id = gcs_reqid_requester(1, 12, 81);
	old.tag = tag; old.request_id = wire.request_id; old.epoch = 1;
	old.requester_node = 1; old.backend_id = 13;
	successor = begin_read(tag, 1, 13, gcs_reqid_requester(1, 12, 82));
	UT_ASSERT(cluster_pcm_lock_read_grant_absent(&old));
	UT_ASSERT(!cluster_pcm_lock_read_grant_absent(&successor.key));
	UT_ASSERT(gcs_first_fill_master_cancel(&old, 1));
	UT_ASSERT(gcs_first_fill_master_cancel_ack(&old));
	read_notice_result = CLUSTER_IC_SEND_DONE;
	gcs_first_fill_master_tick(1);
	UT_ASSERT_EQ(cluster_pcm_lock_read_grant_begin(tag, 1, 13, successor.key.request_id,
		1, 10000, false, &replay), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(replay.key.request_id, successor.key.request_id);
	UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(tag), 2);
}
UT_TEST(test_native_absence_is_not_inferred_for_invalid_identity)
{
	ClusterPcmReadGrantKey good = {0};
	reset_fake_pcm_runtime(4);
	good.tag = make_tag(141); good.request_id = 17; good.epoch = 1;
	good.requester_node = 1; good.backend_id = 13;
	UT_ASSERT(cluster_pcm_lock_read_grant_absent(&good));
	for (int bad = 0; bad < 8; bad++) {
		ClusterPcmReadGrantKey key = good;
		if (bad == 0) key.request_id = 0;
		if (bad == 1) key.request_id = UINT64_MAX;
		if (bad == 2) key.epoch = 0;
		if (bad == 3) key.epoch = UINT64_MAX;
		if (bad == 4) key.requester_node = -1;
		if (bad == 5) key.requester_node = 32;
		if (bad == 6) key.backend_id = 0;
		if (bad == 7) key.backend_id = MaxBackends + 1;
		UT_ASSERT(!cluster_pcm_lock_read_grant_absent(&key));
	}
}

int
main(void)
{
	UT_PLAN(21);
	UT_RUN(test_absent_old_key_cleanup_does_not_consume_successor_native_grant);
	UT_RUN(test_native_absence_is_not_inferred_for_invalid_identity);
	UT_RUN(test_requester_nack_can_admit_cleanup_after_initial_master_capacity_refusal);
	UT_RUN(test_master_not_admitted_notice_keeps_native_claim_until_real_send_takeover);
	UT_RUN(test_revoke_ack_keeps_exact_consumption_notice_until_transport_handoff);
	UT_RUN(test_delayed_master_rechecks_each_native_admission_gate);
	UT_RUN(test_master_seals_only_proven_never_granted_terminal_denial);
	UT_RUN(test_native_master_waits_behind_existing_conversion_without_reader_resend);
	UT_RUN(test_native_master_cancel_retires_unpublished_request_without_grant);
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
