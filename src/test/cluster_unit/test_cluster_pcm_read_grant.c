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

UT_TEST(test_live_grant_ignores_age_but_exact_cancellation_cannot_resurrect)
{
	BufferTag tag = make_tag(112);
	ClusterPcmReadGrant grant, work;
	uint32 cursor = 0;
	reset_fake_pcm_runtime(4);
	grant = begin_read(tag, 1, 8, 22);
	UT_ASSERT_EQ(grant.holder_node, -1);
	UT_ASSERT(!cluster_pcm_lock_read_grant_revoke_ack(&grant.key));
	/* A response/lifetime period is not a failed acquisition. */
	UT_ASSERT(!cluster_pcm_lock_read_grant_work(&cursor, 1, 2, (INT64_MAX - 10000), 1, &work));
	UT_ASSERT_EQ(cluster_pcm_lock_read_grant_begin(tag, 1, 8, 22, 1, 20000, false, &work),
		PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(work.key.request_id, grant.key.request_id);
	UT_ASSERT(!work.revoking);
	UT_ASSERT(cluster_pcm_lock_read_grant_done(&grant.key, 1, false));
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
			UT_ASSERT(cluster_pcm_lock_read_grant_holder_done(&grant, 0));
		UT_ASSERT(cluster_pcm_lock_read_grant_done(&grant.key, 2, true));
		UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &state));
		UT_ASSERT_EQ(state.state, PCM_STATE_S);
		UT_ASSERT_EQ(state.s_holders_bitmap, 5);
		UT_ASSERT_EQ(pg_atomic_read_u32(&((struct StopPcmEntryLayout *)hash_search(
			(HTAB *)&fake_pcm_htab_token, &tag, HASH_FIND, NULL))->pi_holders_bitmap), 1);
		UT_ASSERT(!cluster_pcm_lock_read_grant_holder_done(&grant, 0));
		UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_S_TO_N_RELEASE, 2));
		UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_S_TO_X_UPGRADE, 0));
		/* A delayed notification belongs to the retired episode, never H's new X. */
		UT_ASSERT(!cluster_pcm_lock_read_grant_holder_done(&grant, 0));
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
	UT_ASSERT(cluster_pcm_lock_read_grant_holder_failed(&grant, 0));
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
static struct {
	pg_atomic_uint64 block_forward_sent_count;
	pg_atomic_uint64 block_reply_count;
} read_wire_counters;
static GcsBlockForwardPayload read_forward;
static GcsBlockReplyHeader read_reply;
static unsigned read_forwards, read_replies, read_local_forwards;
static unsigned read_local_seals;
static bool read_admit;
static ClusterICSendResult read_notice_result, read_disk_result;
static uint64 read_epoch;
static ClusterMembershipState read_requester_state, read_holder_state;
static char read_reply_frame[sizeof(GcsBlockReplyHeader) + GCS_BLOCK_DATA_SIZE];
static GcsBlockDonePayload read_notice;
static unsigned read_notices, read_revokes, read_cleanup_lookups;
static bool read_cleanup_capacity = true;
static GcsBlockDedupResult read_ingress_result;
static GcsBlockDedupEntry read_ingress_cached;
static bool read_flags_match;
static unsigned read_flag_pins, read_cached_resends, read_ingress_defers;
static const ClusterICEnvelope *read_deferred_envelope;
static TimestampTz read_master_time;
static unsigned read_retirements;
static unsigned read_ungranted;
static bool read_managed, read_serving, read_quorum, read_member;
static bool read_join_rebuilt, read_recovering, read_home;
static bool read_unknown_home;
static bool read_stop;
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
gcs_block_first_fill_forward_send_admitted(int32 holder, const GcsBlockFirstFillForwardPayload *wire)
{
	const GcsBlockForwardPayload *fwd = &wire->forward;
	ClusterPcmReadGrant proof;
	UT_ASSERT_EQ(cluster_pcm_lock_read_grant_begin(fwd->tag, fwd->original_requester_node,
		fwd->requester_backend_id, fwd->request_id, fwd->epoch, 99999, false, &proof),
		PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(proof.holder_node, holder);
	UT_ASSERT_EQ(proof.binding_generation, wire->binding_generation);
	UT_ASSERT_EQ(proof.service_generation, wire->service_generation);
	UT_ASSERT(GcsBlockFirstFillForwardPayloadValid(wire, sizeof(*wire)));
	UT_ASSERT(GcsBlockForwardPayloadIsFirstFillS(fwd));
	read_forward = *fwd;
	read_forwards++;
	return read_admit;
}
static bool
read_mock_enqueue(uint8 type, uint32 dest, const void *payload, uint16 bytes)
{
	UT_ASSERT_EQ(dest, cluster_node_id);
	if (type == PGRAC_IC_MSG_GCS_BLOCK_DONE) {
		UT_ASSERT_EQ(bytes, sizeof(read_notice));
		read_notice = *(const GcsBlockDonePayload *)payload;
		UT_ASSERT_EQ(read_notice.reserved_0[0], GCS_BLOCK_DONE_PENDING_S_SEAL);
		read_local_seals++;
		return read_notice_result == CLUSTER_IC_SEND_DONE || read_notice_result == CLUSTER_IC_SEND_WOULD_BLOCK;
	}
	UT_ASSERT_EQ(type, PGRAC_IC_MSG_GCS_BLOCK_FORWARD);
	UT_ASSERT_EQ(bytes, sizeof(GcsBlockFirstFillForwardPayload));
	read_local_forwards++;
	return gcs_block_first_fill_forward_send_admitted((int32)dest, payload);
}
static ClusterICSendResult
gcs_block_send_envelope_or_loopback(uint8 type, int32 dest, const void *payload, uint32 bytes)
{
	if (type == PGRAC_IC_MSG_GCS_BLOCK_DONE) {
		UT_ASSERT_EQ(bytes, sizeof(read_notice));
		UT_ASSERT_EQ(dest, (((const GcsBlockDonePayload *)payload)->reserved_0[0] == GCS_BLOCK_DONE_PENDING_S_RETURNED
			|| ((const GcsBlockDonePayload *)payload)->reserved_0[0] == GCS_BLOCK_DONE_PENDING_S_SEAL)
			? (int)((const GcsBlockDonePayload *)payload)->reserved_0[15] - 1 : read_request.sender_node);
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
	memcpy(read_reply_frame, payload, bytes);
	read_replies++;
	return read_disk_result;
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
	BufferTag tag, uint8 transition, uint32 hint,
	bool capable, GcsBlockDedupEntry *cached);
static bool read_lifetime_guard;
static uint32 read_last_lifetime_hint;
static GcsBlockDedupResult
read_cleanup_lookup(int worker pg_attribute_unused(), const GcsBlockDedupKey *key,
	BufferTag tag, uint8 transition, uint32 hint,
	bool capable, GcsBlockDedupEntry *cached)
{
	UT_ASSERT_EQ(transition, PCM_TRANS_N_TO_S);
	UT_ASSERT_EQ(key->request_id, read_request.request_id);
	UT_ASSERT(BufferTagsEqual(&tag, &read_request.tag));
	read_cleanup_lookups++;
	read_last_lifetime_hint = hint;
	*cached = read_ingress_cached;
	/* Registration boundary: a real DONE-capable MISS rejects a zero or
	 * illegal lifetime before creating a dedup entry. Most legacy tests
	 * control replay directly; this arm covers fresh queued cancellation. */
	if (read_lifetime_guard && read_ingress_result == GCS_BLOCK_DEDUP_MISS_REGISTERED
		&& capable && (hint == 0 || (int64)hint > GCS_BLOCK_DEDUP_MAX_PROTOCOL_LIFETIME_MS))
		return GCS_BLOCK_DEDUP_VALIDATION_FAIL;
	return read_cleanup_capacity ? read_ingress_result : GCS_BLOCK_DEDUP_FULL;
}
static bool
read_cleanup_flags(int worker pg_attribute_unused(), const GcsBlockDedupKey *key,
	const BufferTag *tag, uint8 transition, uint8 flags)
{
	UT_ASSERT_EQ(key->request_id, read_request.request_id);
	UT_ASSERT(BufferTagsEqual(tag, &read_request.tag));
	UT_ASSERT_EQ(transition, PCM_TRANS_N_TO_S);
	UT_ASSERT_EQ(flags, GCS_BLOCK_DEDUP_REQUEST_F_READ_MISS);
	read_flag_pins++;
	return read_flags_match;
}
static bool
gcs_block_resend_cached_reply(int32 dest, const GcsBlockDedupEntry *cached)
{
	UT_ASSERT_EQ(dest, read_request.sender_node);
	read_cached_resends++;
	read_reply = cached->reply_header;
	return true;
}
static void
read_ingress_defer(const ClusterICEnvelope *env)
{
	read_ingress_defers++;
	read_deferred_envelope = env;
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
#define cluster_membership_get_state(node) ((node) == read_request.sender_node ? read_requester_state : read_holder_state)
#define cluster_grd_join_remaster_active_for_shard(tag) true
#define cluster_grd_block_view_rebuilt(tag) read_join_rebuilt
#define cluster_gcs_block_phase_for_tag(tag) (read_recovering ? GCS_BLOCK_RECOVERING : GCS_BLOCK_NORMAL)
#define cluster_gcs_lookup_master(tag) (read_unknown_home ? -1 : (read_home ? cluster_node_id : cluster_node_id + 1))
#define cluster_gcs_block_note_send_outcome(family, result) ((void)(result))
#define gcs_block_compute_checksum(data) 0
#define cluster_block_io_trace_pending_s(key, phase, state, holder, binding, result) ((void)0)
#define cluster_gcs_block_dedup_mark_done(worker, key, tag, transition) (++read_retirements, true)
#define GetCurrentTimestamp() read_master_time
#define cluster_epoch_get_current() read_epoch
#define cluster_normal_stop_requested() read_stop
#define cluster_block_io_trace_master(key, event, result) ((void)0)
#define cluster_ic_suppress_gcs_done_cap false
#define cluster_sf_peer_supports_gcs_done(node) true
#define cluster_ic_tier1_my_data_channel() 1
#define gcs_block_compute_invalidate_checksum(inv) 0
#define cluster_lms_workers 2
static void gcs_first_fill_receipt_tick(int worker pg_attribute_unused()) { }
static bool read_done_session = true;
static uint64 read_done_incarnation = 9;
static bool
read_done_auth(int node pg_attribute_unused(), uint64 epoch, uint64 *session)
{
	*session = read_done_incarnation;
	return read_done_session && epoch == read_epoch;
}
#define gcs_block_try_resource_x_frame(env, payload) false
#define cluster_lms_shard_for_tag(tag, workers) 1
#define cluster_gcs_block_dedup_note_misroute() abort()
#define cluster_gcs_block_dedup_note_done_mismatch(worker) ((void)0)
#define gcs_first_fill_receipt_consumed(done) false
#define gcs_first_fill_holder_returned(done, session) false
#define gcs_first_fill_holder_seal(done, session) false
#define cluster_ic_dispatch_defer read_ingress_defer
#define gcs_block_pcm_x_authenticated_session read_done_auth
#define MemoryContextAllocZero(context, size) calloc(1, (size))
#define pfree free
#include "test_cluster_pcm_first_fill_master.inc"
#undef pfree
#undef MemoryContextAllocZero
#undef gcs_block_pcm_x_authenticated_session
#undef cluster_ic_dispatch_defer
#undef gcs_first_fill_holder_returned
#undef gcs_first_fill_holder_seal
#undef gcs_first_fill_receipt_consumed
#undef cluster_gcs_block_dedup_note_done_mismatch
#undef cluster_gcs_block_dedup_note_misroute
#undef cluster_lms_shard_for_tag
#undef gcs_block_try_resource_x_frame
#undef cluster_lms_workers
#undef gcs_block_compute_invalidate_checksum
#undef cluster_ic_tier1_my_data_channel
#undef cluster_sf_peer_supports_gcs_done
#undef cluster_ic_suppress_gcs_done_cap
#undef cluster_block_io_trace_master
#undef cluster_epoch_get_current
#undef cluster_normal_stop_requested
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
	while (gcs_first_fill_ingress_head != NULL)
		gcs_first_fill_ingress_forget(gcs_first_fill_ingress_head);
	memset(gcs_first_fill_return_receipts, 0, sizeof(gcs_first_fill_return_receipts));
	gcs_first_fill_return_receipt_count = 0;
	read_forwards = read_replies = read_local_forwards = 0;
	read_local_seals = 0;
	read_notices = read_revokes = read_cleanup_lookups = 0;
	read_cleanup_capacity = true;
	read_lifetime_guard = false;
	read_last_lifetime_hint = 0;
	read_ingress_result = GCS_BLOCK_DEDUP_MISS_REGISTERED;
	memset(&read_ingress_cached, 0, sizeof(read_ingress_cached));
	read_flags_match = true;
	read_flag_pins = read_cached_resends = read_ingress_defers = 0;
	read_deferred_envelope = NULL;
	memset(&read_wire_counters, 0, sizeof(read_wire_counters));
	read_notice_result = CLUSTER_IC_SEND_NOT_ADMITTED;
	read_disk_result = CLUSTER_IC_SEND_DONE;
	read_epoch = 1;
	read_done_session = true;
	read_done_incarnation = 9;
	read_requester_state = read_holder_state = CLUSTER_MEMBER_MEMBER;
	memset(read_reply_frame, 0, sizeof(read_reply_frame));
	read_retirements = 0;
	read_ungranted = 0;
	read_managed = read_serving = read_quorum = read_member = true;
	read_join_rebuilt = read_home = true;
	read_unknown_home = false;
	read_recovering = false;
	read_stop = false;
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
	GcsBlockRequestPayloadSetLifetimeHintMs(&read_request, 5000);
	key.origin_node_id = 1;
	key.requester_backend_id = 13;
	key.request_id = 71;
	key.cluster_epoch = 1;
	return key;
}

static void
read_deliver_original_confirmation(void)
{
	ClusterPcmReadGrant grant = {0};
	GcsBlockDonePayload done = {0};
	ClusterICEnvelope env = {0};
	UT_ASSERT_EQ(cluster_pcm_lock_read_grant_begin(read_request.tag, read_request.sender_node,
		read_request.requester_backend_id, read_request.request_id, read_request.epoch,
		99999, false, &grant), PCM_GCS_TRANSITION_APPLIED);
	done.tag = grant.key.tag; done.request_id = grant.key.request_id;
	done.epoch = grant.key.epoch; done.sender_node = grant.key.requester_node;
	done.requester_backend_id = grant.key.backend_id; done.transition_id = PCM_TRANS_N_TO_S;
	done.reserved_0[0] = GCS_BLOCK_DONE_PENDING_S_CONFIRM;
	done.reserved_0[1] = (uint8)grant.key.requester_node;
	env.source_node_id = (uint32)done.sender_node; env.dest_node_id = (uint32)cluster_node_id;
	env.epoch = done.epoch; env.payload_length = sizeof(done);
	cluster_gcs_handle_block_done_envelope(&env, &done);
	UT_ASSERT(cluster_pcm_lock_read_grant_absent(&grant.key));
}

static void
read_confirm_original_service(void)
{
	read_deliver_original_confirmation();
	read_master_time += 10000;
	gcs_first_fill_master_tick(1);
}

UT_TEST(test_native_master_keeps_original_service_behind_later_x)
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
	UT_ASSERT_EQ(read_forwards, 1); /* Original service already precedes the later writer. */
	UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
	UT_ASSERT_EQ(cluster_pcm_lock_read_grant_begin(tag, 1, 13, 71, 1, 99999, false, &grant),
		PCM_GCS_TRANSITION_APPLIED);
	read_deliver_original_confirmation();
	gcs_block_first_fill_request(&read_request, &key, 1, false);
	UT_ASSERT_EQ(read_forwards, 1);
	UT_ASSERT_EQ(read_replies, 0);
	read_master_time += 10000; gcs_first_fill_master_tick(1);
	UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
	gcs_block_first_fill_request(&read_request, &key, 1, false);
	UT_ASSERT_EQ(read_forwards + read_replies, 1);
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
	UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
	read_confirm_original_service();
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
		UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
		read_confirm_original_service();
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
UT_TEST(test_native_master_retains_refused_forward_until_transport_takeover)
{
	for (int local = 0; local < 2; local++) {
		BufferTag tag = make_tag(122);
		GcsBlockDedupKey key;
		GcsBlockForwardPayload first;
		ClusterPcmReadGrant grant, work;
		uint32 cursor = 0;
		reset_fake_pcm_runtime(4);
		key = read_wire_setup(tag);
		UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S,
			local ? cluster_node_id : 2));
		read_admit = false;
		gcs_block_first_fill_request(&read_request, &key, 1, true);
		first = read_forward;
		UT_ASSERT_EQ(read_forwards, 1);
		UT_ASSERT_EQ(read_replies, 0);
		UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
		for (int tick = 0; tick < 3; tick++) {
			gcs_block_first_fill_request(&read_request, &key, 1, false);
			UT_ASSERT_EQ(read_forwards, tick + 1);
			read_master_time += 1000000;
			gcs_first_fill_master_tick(1);
			UT_ASSERT_EQ(read_replies, 0);
			UT_ASSERT_EQ(read_forwards, tick + 2);
			UT_ASSERT_EQ(memcmp(&first, &read_forward, sizeof(first)), 0);
			UT_ASSERT(!cluster_pcm_lock_read_grant_work(&cursor, 1, 2, read_master_time, 1, &work));
			UT_ASSERT_EQ(cluster_pcm_lock_read_grant_begin(tag, 1, 13, 71, 1, 99999, false, &grant),
				PCM_GCS_TRANSITION_APPLIED);
		}
		read_admit = true;
		read_master_time += 10000;
		gcs_first_fill_master_tick(1);
		UT_ASSERT_EQ(read_forwards, 5);
		UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
		gcs_block_first_fill_request(&read_request, &key, 1, false);
		read_master_time += 10000;
		gcs_first_fill_master_tick(1);
		UT_ASSERT_EQ(read_forwards, 5);
		read_confirm_original_service();
		UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
	}
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
	UT_ASSERT(!GcsBlockDonePayloadPendingSKey(&done, &decoded));
	{
		ClusterPcmReadGrant service = {0};
		service.binding_generation = 7; service.service_generation = 1;
		GcsBlockDonePayloadSetPendingSService(&done, &service);
	}
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
		ClusterPcmReadGrant grant = {0}, next = {0};
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
	ClusterICEnvelope env = {0};
	reset_fake_pcm_runtime(4); wire = read_wire_setup(tag);
	wire.request_id = read_request.request_id = gcs_reqid_requester(1, 12, 71);
	key.tag = tag; key.request_id = wire.request_id; key.epoch = wire.cluster_epoch;
	key.requester_node = wire.origin_node_id; key.backend_id = wire.requester_backend_id;
	/* The original request waits before either a native grant or park
	 * exists. The retained requester NACK, not another REQUEST, supplies the
	 * exact cleanup identity when the master's bounded capacity is available. */
	for (int i = 0; i < GCS_FIRST_FILL_PARK_MAX; i++)
		gcs_first_fill_master_parks[i].in_use = true;
	gcs_block_first_fill_ingress(&env, &read_request, &wire, 1);
	UT_ASSERT_EQ(read_ingress_defers, 0);
	UT_ASSERT_EQ(gcs_first_fill_ingress_count, 1);
	UT_ASSERT_EQ(read_forwards + read_replies, 0);
	UT_ASSERT(!gcs_first_fill_master_cancel(&key, 1));
	UT_ASSERT(gcs_first_fill_ingress_head->cancelling);
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
	UT_ASSERT_EQ(gcs_first_fill_ingress_count, 0);
	UT_ASSERT(gcs_first_fill_ingress_head == NULL);
	UT_ASSERT_EQ(read_cleanup_lookups, 2);
	UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
	UT_ASSERT(gcs_first_fill_master_parks[0].cancelling);
	read_ingress_result = GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE;
	gcs_block_first_fill_ingress(&env, &read_request, &wire, 1);
	UT_ASSERT(gcs_first_fill_master_parks[0].cancelling);
	UT_ASSERT(cluster_pcm_lock_read_grant_absent(&key));
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
	read_ingress_result = GCS_BLOCK_DEDUP_CACHED_REPLY;
	read_ingress_cached.reply_header.status = GCS_BLOCK_REPLY_DENIED_PENDING_X;
	gcs_block_first_fill_ingress(&env, &read_request, &wire, 1);
	UT_ASSERT_EQ(read_cached_resends, 1);
	UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
	UT_ASSERT(cluster_pcm_lock_read_grant_absent(&key));
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

UT_TEST(test_unconfirmed_or_cancelling_grantee_is_never_an_image_source)
{
	for (int cancel = 0; cancel < 2; cancel++) {
		BufferTag tag = make_tag(142);
		ClusterPcmReadGrant a, b;
		reset_fake_pcm_runtime(4);
		a = begin_read(tag, 1, 7, 31);
		UT_ASSERT_EQ(a.holder_node, -1);
		if (cancel)
			UT_ASSERT(cluster_pcm_lock_read_grant_done(&a.key, 1, false));
		UT_ASSERT_EQ(cluster_pcm_lock_read_grant_begin(tag, 2, 8, 32, 1, 10000, true, &b),
			PCM_GCS_TRANSITION_PENDING_X);
		if (cancel) {
			UT_ASSERT(cluster_pcm_lock_read_grant_revoke_ack(&a.key));
			UT_ASSERT(cluster_pcm_lock_read_grant_retire_notice_admitted(&a.key));
		} else {
			UT_ASSERT(!cluster_pcm_lock_read_grant_revoke_ack(&a.key));
			UT_ASSERT(cluster_pcm_lock_read_grant_done(&a.key, 1, true));
		}
		b = begin_read(tag, 2, 8, 32);
		UT_ASSERT_EQ(b.holder_node, cancel ? -1 : 1);
		UT_ASSERT(cluster_pcm_lock_read_grant_done(&b.key, 2, true));
	}
}

UT_TEST(test_source_selection_skips_pending_copy_and_uses_proven_shared_holder)
{
	BufferTag tag = make_tag(143);
	ClusterPcmReadGrant a, b;
	reset_fake_pcm_runtime(4);
	a = begin_read(tag, 0, 7, 33);
	UT_ASSERT_EQ(a.holder_node, -1);
	/* The fixture publishes a distinct native, already-resident S holder. */
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S, 3));
	b = begin_read(tag, 2, 8, 34);
	UT_ASSERT_EQ(b.holder_node, 3);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(tag, PCM_TRANS_S_TO_X_UPGRADE, 3),
		PCM_GCS_TRANSITION_PENDING_X);
	UT_ASSERT(cluster_pcm_lock_read_grant_done(&a.key, 0, false));
	UT_ASSERT(cluster_pcm_lock_read_grant_revoke_ack(&a.key));
	UT_ASSERT(cluster_pcm_lock_read_grant_retire_notice_admitted(&a.key));
	UT_ASSERT(cluster_pcm_lock_read_grant_done(&b.key, 2, true));
}

UT_TEST(test_master_retains_exact_disk_reply_until_actual_handoff)
{
	for (int blocked = 0; blocked < 2; blocked++) {
		BufferTag tag = make_tag(144);
		GcsBlockDedupKey key;
		char first[sizeof(read_reply_frame)];
		ClusterPcmReadGrant grant;
		reset_fake_pcm_runtime(4); key = read_wire_setup(tag);
		read_disk_result = blocked ? CLUSTER_IC_SEND_HARD_ERROR : CLUSTER_IC_SEND_NOT_ADMITTED;
		gcs_block_first_fill_request(&read_request, &key, 1, true);
		memcpy(first, read_reply_frame, sizeof(first));
		UT_ASSERT(GcsBlockReplyHeaderHasPendingS(&read_reply));
		UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
		for (int tick = 0; tick < 3; tick++) {
			gcs_block_first_fill_request(&read_request, &key, 1, false);
			UT_ASSERT_EQ(read_replies, tick + 1);
			read_master_time += 1000000;
			gcs_first_fill_master_tick(1);
			UT_ASSERT_EQ(read_replies, tick + 2);
			UT_ASSERT_EQ(memcmp(first, read_reply_frame, sizeof(first)), 0);
			UT_ASSERT_EQ(read_forwards, 0);
		}
		read_disk_result = blocked ? CLUSTER_IC_SEND_DONE : CLUSTER_IC_SEND_WOULD_BLOCK;
		read_master_time += 10000; gcs_first_fill_master_tick(1);
		UT_ASSERT_EQ(read_replies, 5);
		UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
		gcs_block_first_fill_request(&read_request, &key, 1, false);
		read_master_time += 10000; gcs_first_fill_master_tick(1);
		UT_ASSERT_EQ(read_replies, 5);
		UT_ASSERT_EQ(cluster_pcm_lock_read_grant_begin(tag, 1, 13, 71, 1, 10000, false, &grant),
			PCM_GCS_TRANSITION_APPLIED);
		UT_ASSERT(cluster_pcm_lock_read_grant_done(&grant.key, 1, true));
		read_master_time += 10000; gcs_first_fill_master_tick(1);
		UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
	}
}

UT_TEST(test_registered_outbound_cancel_keeps_native_revoke_owner)
{
	for (int scenario = 0; scenario < 4; scenario++) {
		BufferTag tag = make_tag(145);
		GcsBlockDedupKey key;
		ClusterPcmReadGrant grant, work;
		uint32 cursor = 0;
		reset_fake_pcm_runtime(4); key = read_wire_setup(tag);
		if (!(scenario & 1)) UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S, 2));
		read_admit = (scenario & 2) != 0;
		read_disk_result = read_admit ? CLUSTER_IC_SEND_DONE : CLUSTER_IC_SEND_NOT_ADMITTED;
		gcs_block_first_fill_request(&read_request, &key, 1, true);
		UT_ASSERT_EQ(cluster_pcm_lock_read_grant_begin(tag, 1, 13, 71, 1, 10000, false, &grant),
			PCM_GCS_TRANSITION_APPLIED);
		work = grant; work.key.backend_id++;
		UT_ASSERT(!gcs_first_fill_master_cancel(&work.key, 1));
		UT_ASSERT(gcs_first_fill_master_cancel(&grant.key, 1));
		UT_ASSERT_EQ(read_ungranted, 0);
		UT_ASSERT(cluster_pcm_lock_read_grant_work(&cursor, 1, 2, 1, 1, &work));
		UT_ASSERT(work.revoking);
		UT_ASSERT(!gcs_first_fill_master_cancel_ack(&grant.key));
		UT_ASSERT(cluster_pcm_lock_read_grant_revoke_ack(&grant.key));
		read_admit = true; read_disk_result = CLUSTER_IC_SEND_DONE;
		read_master_time += 10000; gcs_first_fill_master_tick(1);
		UT_ASSERT_EQ(read_forwards + read_replies, 1);
		UT_ASSERT(cluster_pcm_lock_read_grant_retire_notice_admitted(&grant.key));
	}
}

UT_TEST(test_registered_outbound_cancel_preserves_consumed_ack_notice)
{
	BufferTag tag = make_tag(150);
	GcsBlockDedupKey key;
	ClusterPcmReadGrantKey native = {0};
	reset_fake_pcm_runtime(4); key = read_wire_setup(tag);
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S, 2));
	read_admit = false;
	gcs_block_first_fill_request(&read_request, &key, 1, true);
	native.tag = tag; native.request_id = read_request.request_id;
	native.epoch = read_request.epoch; native.requester_node = read_request.sender_node;
	native.backend_id = read_request.requester_backend_id;
	UT_ASSERT(cluster_pcm_lock_read_grant_done(&native, native.requester_node, false));
	UT_ASSERT(cluster_pcm_lock_read_grant_revoke_ack(&native));
	UT_ASSERT(gcs_first_fill_master_cancel(&native, 1));
	UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
	UT_ASSERT(gcs_first_fill_master_parks[0].cancelling);
	UT_ASSERT(!gcs_first_fill_master_cancel_ack(&native));
	read_admit = true;
	read_master_time += 10000; gcs_first_fill_master_tick(1);
	UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
	UT_ASSERT_EQ(read_forwards + read_replies, 1);
	UT_ASSERT_EQ(read_ungranted, 0);
	UT_ASSERT(cluster_pcm_lock_read_grant_retire_notice_admitted(&native));
	read_master_time += 10000; gcs_first_fill_master_tick(1);
	UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
	UT_ASSERT_EQ(read_forwards + read_replies, 1);
}

UT_TEST(test_registered_outbound_rechecks_all_authority_gates)
{
	for (int gate = 0; gate < 6; gate++) {
		BufferTag tag = make_tag(146);
		GcsBlockDedupKey key;
		reset_fake_pcm_runtime(4); key = read_wire_setup(tag);
		UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S, 2));
		read_admit = false;
		gcs_block_first_fill_request(&read_request, &key, 1, true);
		UT_ASSERT_EQ(read_forwards, 1);
		read_admit = true;
		switch (gate) {
		case 0: read_serving = false; break;
		case 1: read_member = false; break;
		case 2: read_join_rebuilt = false; break;
		case 3: read_recovering = true; break;
		case 4: read_home = false; break;
		case 5: read_managed = false; read_quorum = false; break;
		}
		read_master_time += 10000; gcs_first_fill_master_tick(1);
		UT_ASSERT_EQ(read_forwards, 1);
		UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
		read_serving = read_member = read_join_rebuilt = read_home = read_quorum = true;
		read_recovering = false;
		read_master_time += 10000; gcs_first_fill_master_tick(1);
		UT_ASSERT_EQ(read_forwards, 2);
		UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
		read_confirm_original_service();
		UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
	}
}

UT_TEST(test_registered_outbound_waits_for_committed_member_cleanup)
{
	for (int scenario = 0; scenario < 4; scenario++) {
		BufferTag tag = make_tag(147);
		GcsBlockDedupKey key;
		reset_fake_pcm_runtime(4); key = read_wire_setup(tag);
		read_disk_result = scenario & 2 ? CLUSTER_IC_SEND_DONE : CLUSTER_IC_SEND_NOT_ADMITTED;
		gcs_block_first_fill_request(&read_request, &key, 1, true);
		read_requester_state = scenario & 1 ? CLUSTER_MEMBER_REMOVED : CLUSTER_MEMBER_DEAD;
		read_disk_result = CLUSTER_IC_SEND_DONE;
		read_master_time += 10000; gcs_first_fill_master_tick(1);
		UT_ASSERT_EQ(read_replies, 1);
		UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
		cluster_pcm_lock_cleanup_on_node_dead(1);
		read_master_time += 10000; gcs_first_fill_master_tick(1);
		UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
		UT_ASSERT_EQ(read_replies, 1);
	}
}

UT_TEST(test_registered_service_epoch_or_stop_cancels_without_redelivery)
{
	for (int scenario = 0; scenario < 4; scenario++) {
		BufferTag tag = make_tag(148);
		GcsBlockDedupKey key;
		ClusterPcmReadGrant work = {0};
		uint32 cursor = 0;
		reset_fake_pcm_runtime(4); key = read_wire_setup(tag);
		read_disk_result = scenario & 2 ? CLUSTER_IC_SEND_DONE : CLUSTER_IC_SEND_NOT_ADMITTED;
		gcs_block_first_fill_request(&read_request, &key, 1, true);
		read_disk_result = CLUSTER_IC_SEND_DONE;
		if (scenario & 1) read_stop = true; else read_epoch = 2;
		read_master_time += 10000; gcs_first_fill_master_tick(1);
		UT_ASSERT_EQ(read_replies, 1);
		UT_ASSERT(cluster_pcm_lock_read_grant_work(&cursor, 1, 2, read_master_time, read_epoch, &work));
		UT_ASSERT(work.revoking);
		UT_ASSERT(cluster_pcm_lock_read_grant_revoke_ack(&work.key));
		UT_ASSERT(cluster_pcm_lock_read_grant_retire_notice_admitted(&work.key));
	}
}

UT_TEST(test_master_capacity_precedes_native_grant_and_never_evicts)
{
	BufferTag tag = make_tag(149);
	GcsBlockDedupKey key;
	ClusterPcmReadGrantKey native = {0};
	GcsFirstFillMasterPark before[GCS_FIRST_FILL_PARK_MAX];
	reset_fake_pcm_runtime(4); key = read_wire_setup(tag);
	for (int i = 0; i < GCS_FIRST_FILL_PARK_MAX; i++) gcs_first_fill_master_parks[i].in_use = true;
	memcpy(before, gcs_first_fill_master_parks, sizeof(before));
	gcs_block_first_fill_request(&read_request, &key, 1, true);
	UT_ASSERT_EQ(memcmp(before, gcs_first_fill_master_parks, sizeof(before)), 0);
	UT_ASSERT_EQ(read_replies, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(tag), 0);
	native.tag = tag; native.request_id = 71; native.epoch = 1;
	native.requester_node = 1; native.backend_id = 13;
	UT_ASSERT(cluster_pcm_lock_read_grant_absent(&native));
}

UT_TEST(test_handed_off_master_waits_without_replaying_image_or_disk_grant)
{
	for (int source = 0; source < 3; source++) {
		BufferTag tag = make_tag(151);
		GcsBlockDedupKey key;
		reset_fake_pcm_runtime(4); key = read_wire_setup(tag);
		if (source)
			UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S,
				source == 1 ? 2 : cluster_node_id));
		gcs_block_first_fill_request(&read_request, &key, 1, true);
		UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
		for (int duplicate = 0; duplicate < 12; duplicate++) {
			gcs_block_first_fill_request(&read_request, &key, 1, false);
			read_master_time += 30000000;
			gcs_first_fill_master_tick(1);
			UT_ASSERT_EQ(read_forwards + read_replies, 1);
			UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
		}
		/* Closing sending admission cannot delay retirement of a native
		 * confirmation already accepted for the original exact claim. */
		read_deliver_original_confirmation();
		read_serving = read_member = read_home = false;
		read_recovering = true;
		read_master_time += 10000; gcs_first_fill_master_tick(1);
		UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
		UT_ASSERT_EQ(read_forwards + read_replies, 1);
	}
}

UT_TEST(test_master_service_retirement_cannot_consume_same_node_successor)
{
	BufferTag tag = make_tag(152);
	GcsBlockDedupKey key;
	ClusterPcmReadGrant original = {0}, successor = {0}, replay = {0};
	reset_fake_pcm_runtime(4); key = read_wire_setup(tag);
	gcs_block_first_fill_request(&read_request, &key, 1, true);
	UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
	UT_ASSERT_EQ(cluster_pcm_lock_read_grant_begin(tag, 1, 13, 71, 1, 99999, false, &original),
		PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(cluster_pcm_lock_read_grant_done(&original.key, 1, true));
	successor = begin_read(tag, 1, 13, 72);
	read_master_time += 10000; gcs_first_fill_master_tick(1);
	UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
	UT_ASSERT_EQ(read_forwards + read_replies, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_read_grant_begin(tag, 1, 13, 72, 1, 99999, false, &replay),
		PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(replay.binding_generation, successor.binding_generation);
	UT_ASSERT_EQ(replay.key.request_id, successor.key.request_id);
	UT_ASSERT(!cluster_pcm_lock_read_grant_revoke_ack(&original.key));
	UT_ASSERT(cluster_pcm_lock_read_grant_done(&successor.key, 1, true));
}

UT_TEST(test_master_service_slots_return_after_each_native_confirmation)
{
	BufferTag tag = make_tag(153);
	GcsBlockDedupKey key;
	reset_fake_pcm_runtime(4); key = read_wire_setup(tag);
	for (int episode = 0; episode < 2 * GCS_FIRST_FILL_PARK_MAX + 1; episode++) {
		unsigned used = 0;
		read_request.request_id = key.request_id = gcs_reqid_requester(1, 12, episode + 81);
		gcs_block_first_fill_request(&read_request, &key, 1, true);
		for (int slot = 0; slot < GCS_FIRST_FILL_PARK_MAX; slot++)
			used += gcs_first_fill_master_parks[slot].in_use;
		UT_ASSERT_EQ(used, 1);
		UT_ASSERT_EQ(read_replies, episode + 1);
		read_confirm_original_service();
		for (int slot = 0; slot < GCS_FIRST_FILL_PARK_MAX; slot++)
			UT_ASSERT(!gcs_first_fill_master_parks[slot].in_use);
	}
}

UT_TEST(test_cached_replay_without_local_owner_never_mints_another_delivery)
{
	for (int existing = 0; existing < 2; existing++) {
		BufferTag tag = make_tag(154);
		GcsBlockDedupKey key;
		ClusterPcmReadGrant native = {0}, replay = {0};
		reset_fake_pcm_runtime(4); key = read_wire_setup(tag);
		if (existing) {
			UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S, 2));
			native = begin_read(tag, 1, 13, 71);
		}
		gcs_block_first_fill_request(&read_request, &key, 1, false);
		UT_ASSERT_EQ(read_forwards + read_replies, 0);
		UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
		if (existing) {
			UT_ASSERT_EQ(cluster_pcm_lock_read_grant_begin(tag, 1, 13, 71, 1, 99999, false, &replay),
				PCM_GCS_TRANSITION_APPLIED);
			UT_ASSERT_EQ(replay.binding_generation, native.binding_generation);
			UT_ASSERT(cluster_pcm_lock_read_grant_done(&native.key, 1, false));
			UT_ASSERT(cluster_pcm_lock_read_grant_revoke_ack(&native.key));
			UT_ASSERT(cluster_pcm_lock_read_grant_retire_notice_admitted(&native.key));
		} else
			UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(tag), 0);
	}
}

UT_TEST(test_unbound_holder_failure_cannot_revoke_native_claim)
{
	BufferTag tag = make_tag(157);
	ClusterPcmReadGrant grant, current;
	reset_fake_pcm_runtime(4);
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_X, 0));
	grant = begin_read(tag, 1, 13, 157);
	UT_ASSERT(!cluster_pcm_lock_read_grant_done(&grant.key, 0, false));
	UT_ASSERT_EQ(cluster_pcm_lock_read_grant_begin(tag, 1, 13, 157, 1,
		10000, false, &current), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(!current.revoking);
	UT_ASSERT(cluster_pcm_lock_read_grant_done(&grant.key, 1, false));
}

UT_TEST(test_holder_service_rejects_stale_binding_generation_and_source)
{
	for (int field = 0; field < 10; field++) {
		BufferTag tag = make_tag(158);
		ClusterPcmReadGrant grant, wrong, current;
		PcmAuthoritySnapshot state;
		reset_fake_pcm_runtime(4);
		UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_X, 0));
		grant = begin_read(tag, 1, 13, 158);
		wrong = grant;
		switch (field) {
		case 0: wrong.binding_generation++; break;
		case 1: wrong.service_generation++; break;
		case 2: wrong.holder_node = 2; break;
		case 3: wrong.key.backend_id++; break;
		case 4: wrong.key.request_id++; break;
		case 5: wrong.holder_was_x = false; break;
		case 6: wrong.binding_generation = 0; break;
		case 7: wrong.binding_generation = UINT64_MAX; break;
		case 8: wrong.service_generation = 0; break;
		case 9: wrong.service_generation = UINT32_MAX; break;
		}
		UT_ASSERT(!cluster_pcm_lock_read_grant_holder_done(&wrong, 0));
		UT_ASSERT(!cluster_pcm_lock_read_grant_holder_failed(&wrong, 0));
		UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &state));
		UT_ASSERT_EQ(state.state, PCM_STATE_X);
		UT_ASSERT_EQ(cluster_pcm_lock_read_grant_begin(tag, 1, 13, 158, 1,
			10000, false, &current), PCM_GCS_TRANSITION_APPLIED);
		UT_ASSERT(!current.revoking);
		UT_ASSERT(cluster_pcm_lock_read_grant_holder_done(&grant, 0));
		UT_ASSERT(cluster_pcm_lock_read_grant_holder_failed(&grant, 0));
		UT_ASSERT(!cluster_pcm_lock_read_grant_done(&grant.key, 1, true));
	}
}

UT_TEST(test_native_service_generation_survives_replay_and_exact_retirement)
{
	BufferTag tag = make_tag(155);
	ClusterPcmReadGrant grant, replay, work;
	uint32 cursor = 0;
	reset_fake_pcm_runtime(4);
	grant = begin_read(tag, 1, 13, 155);
	UT_ASSERT_EQ(grant.service_generation, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_read_grant_begin(tag, 1, 13, 155, 1,
		INT64_MAX - 10000, false, &replay), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(replay.service_generation, grant.service_generation);
	UT_ASSERT_EQ(replay.binding_generation, grant.binding_generation);
	UT_ASSERT(cluster_pcm_lock_read_grant_blocker_snapshot(tag, 1, &work));
	UT_ASSERT_EQ(work.service_generation, grant.service_generation);
	UT_ASSERT(cluster_pcm_lock_read_grant_done(&grant.key, 1, false));
	UT_ASSERT(cluster_pcm_lock_read_grant_work(&cursor, 1, 2, 10001, 1, &work));
	UT_ASSERT(work.revoking);
	UT_ASSERT_EQ(work.service_generation, grant.service_generation);
	UT_ASSERT(cluster_pcm_lock_read_grant_revoke_ack(&grant.key));
	cursor = 0;
	UT_ASSERT(cluster_pcm_lock_read_grant_work(&cursor, 1, 2, 20001, 1, &work));
	UT_ASSERT(work.ack_consumed);
	UT_ASSERT_EQ(work.service_generation, grant.service_generation);
	UT_ASSERT(cluster_pcm_lock_read_grant_retire_notice_admitted(&grant.key));
	replay = begin_read(tag, 1, 13, 156);
	UT_ASSERT_EQ(replay.service_generation, 1);
	UT_ASSERT(!cluster_pcm_lock_read_grant_revoke_ack(&grant.key));
	UT_ASSERT(cluster_pcm_lock_read_grant_done(&replay.key, 1, true));
}

UT_TEST(test_saved_frame_cannot_cross_native_service_generation)
{
	for (int disk = 0; disk < 2; disk++) {
		BufferTag tag = make_tag(156);
		GcsBlockDedupKey key;
		GcsFirstFillMasterPark *park;
		ClusterPcmReadGrant native;
		uint32 generation;
		reset_fake_pcm_runtime(4); key = read_wire_setup(tag);
		if (!disk)
			UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S, 2));
		read_admit = false;
		read_disk_result = CLUSTER_IC_SEND_NOT_ADMITTED;
		gcs_block_first_fill_request(&read_request, &key, 1, true);
		UT_ASSERT_EQ(read_forwards + read_replies, 1);
		park = &gcs_first_fill_master_parks[0];
		UT_ASSERT(park->in_use && park->registered && !park->handed_off);
		generation = park->grant.service_generation;
		UT_ASSERT_EQ(generation, 1);
		read_admit = true; read_disk_result = CLUSTER_IC_SEND_DONE;
		for (int mismatch = 0; mismatch < 3; mismatch++) {
			park->grant.service_generation = mismatch == 0 ? 0
				: mismatch == 1 ? UINT32_MAX : generation + 1;
			read_master_time += 10000; gcs_first_fill_master_tick(1);
			UT_ASSERT_EQ(read_forwards + read_replies, 1);
			UT_ASSERT(park->in_use && !park->handed_off);
			UT_ASSERT_EQ(cluster_pcm_lock_read_grant_begin(tag, 1, 13,
				read_request.request_id, 1, 10000, false, &native), PCM_GCS_TRANSITION_APPLIED);
			UT_ASSERT_EQ(native.service_generation, generation);
			UT_ASSERT(!native.revoking && !native.ack_consumed);
		}
		park->grant.service_generation = generation;
		read_master_time += 10000; gcs_first_fill_master_tick(1);
		UT_ASSERT_EQ(read_forwards + read_replies, 2);
		UT_ASSERT(park->in_use && park->handed_off);
		read_confirm_original_service();
		UT_ASSERT(!park->in_use);
	}
}

UT_TEST(test_service_done_wire_is_canonical_and_requester_actions_stay_episode_scoped)
{
	GcsBlockDonePayload done = {0};
	ClusterPcmReadGrant service = {0}, decoded = {0};
	ClusterPcmReadGrantKey key;
	service.key.tag = make_tag(159);
	service.key.request_id = 159; service.key.epoch = 1;
	service.key.requester_node = 1; service.key.backend_id = 13;
	service.binding_generation = 7; service.service_generation = 1;
	service.holder_node = 0; service.holder_was_x = true;
	done.tag = service.key.tag; done.request_id = service.key.request_id;
	done.epoch = 1; done.sender_node = 0; done.requester_backend_id = 13;
	done.transition_id = PCM_TRANS_N_TO_S;
	done.reserved_0[0] = GCS_BLOCK_DONE_PENDING_S_HOLDER; done.reserved_0[1] = 1;
	UT_ASSERT(!GcsBlockDonePayloadPendingSKey(&done, &key));
	GcsBlockDonePayloadSetPendingSService(&done, &service);
	UT_ASSERT(GcsBlockDonePayloadPendingSService(&done, &decoded));
	UT_ASSERT_EQ(memcmp(&service.key, &decoded.key, sizeof(key)), 0);
	UT_ASSERT_EQ(decoded.binding_generation, 7);
	UT_ASSERT_EQ(decoded.service_generation, 1);
	UT_ASSERT_EQ(decoded.holder_node, 0);
	UT_ASSERT(decoded.holder_was_x);
	for (int bad = 0; bad < 4; bad++) {
		GcsBlockDonePayload wrong = done;
		wrong.reserved_0[15 + bad] = 1;
		UT_ASSERT(!GcsBlockDonePayloadPendingSService(&wrong, &decoded));
	}
	done.reserved_0[14] = 2;
	UT_ASSERT(!GcsBlockDonePayloadPendingSService(&done, &decoded));
	done.reserved_0[14] = 1; done.reserved_0[0] = GCS_BLOCK_DONE_PENDING_S_NACK;
	UT_ASSERT(GcsBlockDonePayloadPendingSService(&done, &decoded));
	done.sender_node = 1;
	UT_ASSERT(!GcsBlockDonePayloadPendingSKey(&done, &key));
	memset(&done.reserved_0[2], 0, sizeof(done.reserved_0) - 2);
	UT_ASSERT(GcsBlockDonePayloadPendingSKey(&done, &key));
	UT_ASSERT(!GcsBlockDonePayloadPendingSService(&done, &decoded));
	done.reserved_0[0] = GCS_BLOCK_DONE_PENDING_S_CONFIRM;
	UT_ASSERT(GcsBlockDonePayloadPendingSKey(&done, &key));
	done.reserved_0[0] = GCS_BLOCK_DONE_PENDING_S_RETIRED; done.sender_node = 2;
	UT_ASSERT(GcsBlockDonePayloadPendingSKey(&done, &key));
	for (int returned = 0; returned < 2; returned++) {
		done.sender_node = returned ? 1 : service.holder_node;
		done.reserved_0[0] = returned ? GCS_BLOCK_DONE_PENDING_S_RETURNED
			: GCS_BLOCK_DONE_PENDING_S_RETURN;
		GcsBlockDonePayloadSetPendingSService(&done, &service);
		UT_ASSERT(GcsBlockDonePayloadPendingSService(&done, &decoded));
		UT_ASSERT_EQ(decoded.holder_node, service.holder_node);
		UT_ASSERT_EQ(decoded.source_transition_generation, 0); /* Master-local only. */
		for (int bad = 0; bad < 7; bad++) {
			GcsBlockDonePayload wrong = done;
			if (bad < 3) wrong.reserved_0[15] = bad == 0 ? 0 : bad == 1 ? 33 : 2;
			else if (bad < 6) wrong.reserved_0[13 + bad] = 1;
			else wrong.reserved_0[14] = 2;
			UT_ASSERT(!GcsBlockDonePayloadPendingSService(&wrong, &decoded));
		}
		if (!returned) {
			GcsBlockDonePayload wrong = done;
			wrong.sender_node = 2; /* RETURN may only be sent by the encoded holder. */
			UT_ASSERT(!GcsBlockDonePayloadPendingSService(&wrong, &decoded));
		}
	}
}

UT_TEST(test_actual_done_ingress_checks_holder_service_and_preserves_requester_cancel)
{
	for (int action = 0; action < 2; action++) {
		for (int bad = 0; bad < 9; bad++) {
			BufferTag tag = make_tag(160);
			ClusterPcmReadGrant grant, current, wrong;
			GcsBlockDonePayload done = {0};
			ClusterICEnvelope env = {0};
			PcmAuthoritySnapshot state;
			reset_fake_pcm_runtime(4); (void)read_wire_setup(tag);
			UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_X, 2));
			grant = begin_read(tag, 1, 13, read_request.request_id);
			wrong = grant;
			done.tag = tag; done.request_id = grant.key.request_id;
			done.epoch = 1; done.sender_node = 2; done.requester_backend_id = 13;
			done.transition_id = PCM_TRANS_N_TO_S;
			done.reserved_0[0] = action ? GCS_BLOCK_DONE_PENDING_S_NACK : GCS_BLOCK_DONE_PENDING_S_HOLDER;
			done.reserved_0[1] = 1;
			env.source_node_id = 2; env.dest_node_id = cluster_node_id;
			env.payload_length = sizeof(done); read_done_session = true;
			switch (bad) {
			case 0: wrong.binding_generation++; break;
			case 1: wrong.service_generation++; break;
			case 2: done.sender_node = 3; env.source_node_id = 3; break;
			case 3: wrong.holder_was_x = false; break;
			case 4: done.request_id++; break;
			case 5: done.epoch++; break;
			case 6: env.source_node_id = 3; break;
			case 7: read_done_session = false; break;
			case 8: break; /* right node/key, missing service extension */
			}
			if (bad != 8) GcsBlockDonePayloadSetPendingSService(&done, &wrong);
			cluster_gcs_handle_block_done_envelope(&env, &done);
			UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &state));
			UT_ASSERT_EQ(state.state, PCM_STATE_X);
			UT_ASSERT_EQ(cluster_pcm_lock_read_grant_begin(tag, 1, 13, grant.key.request_id,
				1, 10000, false, &current), PCM_GCS_TRANSITION_APPLIED);
			UT_ASSERT(!current.revoking);
			read_done_session = true; env.source_node_id = 2;
			done.sender_node = 2; done.request_id = grant.key.request_id; done.epoch = 1;
			GcsBlockDonePayloadSetPendingSService(&done, &grant);
			cluster_gcs_handle_block_done_envelope(&env, &done);
			UT_ASSERT_EQ(cluster_pcm_lock_read_grant_begin(tag, 1, 13, grant.key.request_id,
				1, 10000, false, &current), action ? PCM_GCS_TRANSITION_PENDING_X : PCM_GCS_TRANSITION_APPLIED);
			if (action) {
				UT_ASSERT(cluster_pcm_lock_read_grant_blocker_snapshot(tag, 1, &current));
				UT_ASSERT(current.revoking);
			} else {
				UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &state));
				UT_ASSERT_EQ(state.state, PCM_STATE_S);
			}
		}
	}
	{
		BufferTag tag = make_tag(161);
		ClusterPcmReadGrant grant, current;
		GcsBlockDonePayload done = {0};
		ClusterICEnvelope env = {0};
		reset_fake_pcm_runtime(4); (void)read_wire_setup(tag);
		UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_X, 2));
		grant = begin_read(tag, 1, 13, read_request.request_id);
		done.tag = tag; done.request_id = grant.key.request_id;
		done.epoch = 1; done.sender_node = 1; done.requester_backend_id = 13;
		done.transition_id = PCM_TRANS_N_TO_S;
		done.reserved_0[0] = GCS_BLOCK_DONE_PENDING_S_NACK; done.reserved_0[1] = 1;
		env.source_node_id = 1; env.dest_node_id = cluster_node_id;
		env.payload_length = sizeof(done); read_done_session = true;
		cluster_gcs_handle_block_done_envelope(&env, &done);
		UT_ASSERT(cluster_pcm_lock_read_grant_blocker_snapshot(tag, 1, &current));
		UT_ASSERT(current.revoking);
	}
	read_done_session = true;
}

UT_TEST(returned_service_reselects_only_after_native_source_change)
{
	BufferTag tag = make_tag(170);
	ClusterPcmReadGrant grant = {0}, current = {0}, next = {0};
	reset_fake_pcm_runtime(4);
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_X, 0));
	grant = begin_read(tag, 1, 7, 71);
	UT_ASSERT_EQ(cluster_pcm_lock_read_grant_reselect_returned(&grant, 0, &next),
		PCM_GCS_TRANSITION_PENDING_X);
	UT_ASSERT_EQ(cluster_pcm_lock_read_grant_begin(tag, 1, 7, 71, 1, 10000, false, &current),
		PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(current.service_generation, 1);
	UT_ASSERT(cluster_pcm_lock_read_grant_holder_done(&grant, 0));
	UT_ASSERT_EQ(cluster_pcm_lock_read_grant_reselect_returned(&grant, 0, &next),
		PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(next.key.request_id, 71);
	UT_ASSERT_EQ(next.binding_generation, grant.binding_generation);
	UT_ASSERT_EQ(next.service_generation, 2);
	UT_ASSERT_EQ(next.holder_node, 0);
	UT_ASSERT(!next.holder_was_x);
	UT_ASSERT(!cluster_pcm_lock_read_grant_holder_failed(&grant, 0));
	UT_ASSERT(!cluster_pcm_lock_read_grant_holder_done(&grant, 0));
	UT_ASSERT_EQ(cluster_pcm_lock_read_grant_reselect_returned(&grant, 0, &current),
		PCM_GCS_TRANSITION_INCOMPATIBLE);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(tag, PCM_TRANS_S_TO_X_UPGRADE, 0),
		PCM_GCS_TRANSITION_PENDING_X);
	UT_ASSERT(cluster_pcm_lock_read_grant_done(&next.key, 1, true));
}

UT_TEST(returned_service_cannot_cross_identity_cancel_or_pi)
{
	BufferTag tag = make_tag(171);
	ClusterPcmReadGrant grant = {0}, bad = {0}, next = {0};
	reset_fake_pcm_runtime(4);
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S, 0));
	grant = begin_read(tag, 1, 7, 71);
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_S_TO_N_RELEASE, 0));
	for (int i = 0; i < 9; i++) {
		bad = grant;
		switch (i) {
		case 0: bad.binding_generation++; break;
		case 1: bad.service_generation++; break;
		case 2: bad.holder_node++; break;
		case 3: bad.holder_was_x = !bad.holder_was_x; break;
		case 4: bad.key.request_id++; break;
		case 5: bad.key.backend_id++; break;
		case 6: bad.key.epoch++; break;
		case 7: bad.key.tag.blockNum++; break;
		case 8: bad.key.requester_node++; break;
		}
		UT_ASSERT_EQ(cluster_pcm_lock_read_grant_reselect_returned(&bad, 0, &next),
			PCM_GCS_TRANSITION_INCOMPATIBLE);
	}
	UT_ASSERT_EQ(cluster_pcm_lock_read_grant_reselect_returned(&grant, 0, &next),
		PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(next.holder_node, -1);
	UT_ASSERT_EQ(next.service_generation, 2);
	UT_ASSERT(cluster_pcm_lock_read_grant_done(&next.key, 1, false));
	UT_ASSERT_EQ(cluster_pcm_lock_read_grant_reselect_returned(&next, 0, &bad),
		PCM_GCS_TRANSITION_INCOMPATIBLE);
	reset_fake_pcm_runtime(4);
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_X, 0));
	grant = begin_read(tag, 1, 7, 71);
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_X_TO_N_DOWNGRADE, 0));
	UT_ASSERT_EQ(cluster_pcm_lock_read_grant_reselect_returned(&grant, 0, &next),
		PCM_GCS_TRANSITION_PENDING_X);
	UT_ASSERT(cluster_pcm_lock_read_grant_done(&grant.key, 1, false));
	UT_ASSERT_EQ(cluster_pcm_lock_read_grant_reselect_returned(&grant, 0, &next),
		PCM_GCS_TRANSITION_INCOMPATIBLE);
}

static void
read_return_wire(const ClusterPcmReadGrant *service)
{
	GcsBlockDonePayload done = {0};
	ClusterICEnvelope env = {0};
	done.request_id = service->key.request_id; done.epoch = service->key.epoch;
	done.tag = service->key.tag; done.sender_node = service->holder_node;
	done.requester_backend_id = service->key.backend_id;
	done.transition_id = PCM_TRANS_N_TO_S;
	done.reserved_0[0] = GCS_BLOCK_DONE_PENDING_S_RETURN;
	done.reserved_0[1] = service->key.requester_node;
	GcsBlockDonePayloadSetPendingSService(&done, service);
	env.source_node_id = done.sender_node; env.dest_node_id = cluster_node_id;
	env.payload_length = sizeof(done);
	cluster_gcs_handle_block_done_envelope(&env, &done);
}

UT_TEST(orphan_return_does_not_retain_the_receive_head_at_master_capacity)
{
	BufferTag tag = make_tag(198);
	ClusterPcmReadGrant service = {0};
	GcsFirstFillMasterPark before[GCS_FIRST_FILL_PARK_MAX];
	reset_fake_pcm_runtime(4); (void)read_wire_setup(tag);
	service.key.tag = tag; service.key.request_id = 71; service.key.epoch = 1;
	service.key.requester_node = 1; service.key.backend_id = 13;
	service.binding_generation = 7; service.service_generation = 1; service.holder_node = 0;
	UT_ASSERT(cluster_pcm_lock_read_grant_absent(&service.key));
	for (int i = 0; i < GCS_FIRST_FILL_PARK_MAX; i++) {
		gcs_first_fill_master_parks[i].in_use = true;
		gcs_first_fill_master_parks[i].key.request_id = 1000 + i;
		gcs_first_fill_master_parks[i].retry_at = INT64_MAX;
	}
	memcpy(before, gcs_first_fill_master_parks, sizeof(before));
	read_return_wire(&service);
	UT_ASSERT_EQ(read_ingress_defers, 0);
	UT_ASSERT_EQ(memcmp(before, gcs_first_fill_master_parks, sizeof(before)), 0);
	read_master_time += 10000;
	gcs_first_fill_master_tick(1);
	UT_ASSERT_EQ(read_notices, 1);
	UT_ASSERT_EQ(read_forwards + read_replies + read_revokes, 0);
	UT_ASSERT(cluster_pcm_lock_read_grant_absent(&service.key));
}

static ClusterPcmReadGrant
read_orphan_return_setup(BlockNumber block)
{
	ClusterPcmReadGrant service = {0};
	reset_fake_pcm_runtime(4); (void)read_wire_setup(make_tag(block));
	service.key.tag = read_request.tag; service.key.request_id = 71; service.key.epoch = 1;
	service.key.requester_node = 1; service.key.backend_id = 13;
	service.binding_generation = 7; service.service_generation = 1; service.holder_node = 0;
	return service;
}

UT_TEST(orphan_return_receipts_are_bounded_per_original_sealed_holder)
{
	ClusterPcmReadGrant service = read_orphan_return_setup(199);
	const char *reason;
	for (int i = 0; i < GCS_FIRST_FILL_PARK_MAX; i++) {
		service.key.request_id = 71 + i;
		read_return_wire(&service);
	}
	UT_ASSERT_EQ(gcs_first_fill_return_receipt_count, 64);
	UT_ASSERT_EQ(read_ingress_defers, 0);
	read_return_wire(&service); /* Exact duplicate shares a receipt at capacity. */
	UT_ASSERT_EQ(gcs_first_fill_return_receipt_count, 64);
	service.service_generation++;
	read_return_wire(&service);
	UT_ASSERT_EQ(read_ingress_defers, 1);
	UT_ASSERT_EQ(gcs_first_fill_return_receipt_count, 64);
	UT_ASSERT_EQ(gcs_first_fill_return_receipts[0][63].service.service_generation, 1);
	/* A second source holder owns a separate set of at most64 sealed parks. */
	service.holder_node = 2;
	read_return_wire(&service);
	UT_ASSERT_EQ(gcs_first_fill_return_receipt_count, 65);
	UT_ASSERT_EQ(gcs_first_fill_return_receipt_poll(&reason), CLUSTER_NORMAL_STOP_PENDING);
	read_notice_result = CLUSTER_IC_SEND_DONE;
	gcs_first_fill_master_tick(1);
	UT_ASSERT_EQ(read_notices, 65);
	UT_ASSERT_EQ(gcs_first_fill_return_receipt_count, 0);
	UT_ASSERT_EQ(gcs_first_fill_return_receipt_poll(&reason), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(read_forwards + read_replies + read_revokes, 0);
}

UT_TEST(new_authenticated_holder_instance_can_retire_its_full_old_receipt_row)
{
	ClusterPcmReadGrant service = read_orphan_return_setup(205);
	for (int i = 0; i < GCS_FIRST_FILL_PARK_MAX; i++) {
		service.key.request_id = 71 + i;
		read_return_wire(&service);
	}
	UT_ASSERT_EQ(gcs_first_fill_return_receipt_count, 64);
	read_done_incarnation++;
	service.key.request_id++;
	read_return_wire(&service);
	UT_ASSERT_EQ(gcs_first_fill_return_receipt_count, 1);
	UT_ASSERT_EQ(gcs_first_fill_return_receipts[0][0].holder_session, read_done_incarnation);
	UT_ASSERT_EQ(read_ingress_defers, 0);
	UT_ASSERT_EQ(read_notices + read_forwards + read_replies + read_revokes, 0);
}

UT_TEST(orphan_receipt_waits_through_age_unknown_session_send_refusal_and_stop)
{
	ClusterPcmReadGrant service = read_orphan_return_setup(200), echoed;
	const char *reason;
	read_return_wire(&service);
	gcs_first_fill_return_receipt_tick(0); /* Not the routed worker. */
	UT_ASSERT_EQ(read_notices, 0);
	read_master_time += 1000000000;
	gcs_first_fill_return_receipt_tick(1);
	UT_ASSERT_EQ(read_notices, 1);
	UT_ASSERT_EQ(gcs_first_fill_return_receipt_count, 1);
	read_unknown_home = true;
	read_master_time += 10000;
	gcs_first_fill_return_receipt_tick(1);
	UT_ASSERT_EQ(read_notices, 1);
	UT_ASSERT_EQ(gcs_first_fill_return_receipt_count, 1);
	read_unknown_home = false;
	read_done_session = false;
	read_master_time += 1000000000;
	gcs_first_fill_return_receipt_tick(1);
	UT_ASSERT_EQ(read_notices, 1);
	read_done_session = true;
	read_stop = true;
	read_serving = read_quorum = read_member = read_join_rebuilt = false;
	read_recovering = true;
	read_notice_result = CLUSTER_IC_SEND_HARD_ERROR;
	gcs_first_fill_return_receipt_tick(1);
	UT_ASSERT_EQ(read_notices, 2);
	UT_ASSERT_EQ(gcs_first_fill_return_receipt_poll(&reason), CLUSTER_NORMAL_STOP_PENDING);
	read_notice_result = CLUSTER_IC_SEND_WOULD_BLOCK;
	read_master_time += 10000;
	gcs_first_fill_return_receipt_tick(1);
	UT_ASSERT_EQ(gcs_first_fill_return_receipt_count, 0);
	UT_ASSERT(GcsBlockDonePayloadPendingSService(&read_notice, &echoed));
	UT_ASSERT(gcs_first_fill_return_same_service(&service, &echoed));
	UT_ASSERT_EQ(gcs_first_fill_return_receipt_poll(&reason), CLUSTER_NORMAL_STOP_READY);
	gcs_first_fill_return_receipt_tick(1);
	UT_ASSERT_EQ(read_notices, 3);
	UT_ASSERT_EQ(read_forwards + read_replies + read_revokes, 0);
}

UT_TEST(orphan_receipt_retires_only_for_proved_master_or_holder_reconfiguration)
{
	for (int why = 0; why < 5; why++) {
		ClusterPcmReadGrant service = read_orphan_return_setup(201);
		read_return_wire(&service);
		if (why == 0) read_epoch++;
		if (why == 1) read_home = false;
		if (why == 2) read_holder_state = CLUSTER_MEMBER_DEAD;
		if (why == 3) read_holder_state = CLUSTER_MEMBER_REMOVED;
		if (why == 4) read_done_incarnation++;
		gcs_first_fill_return_receipt_tick(1);
		UT_ASSERT_EQ(gcs_first_fill_return_receipt_count, 0);
		UT_ASSERT_EQ(read_notices + read_forwards + read_replies + read_revokes, 0);
	}
}

UT_TEST(orphan_receipt_does_not_borrow_or_retire_a_native_successor)
{
	ClusterPcmReadGrant service = read_orphan_return_setup(202), next, current;
	read_return_wire(&service);
	next = begin_read(service.key.tag, 1, 13, service.key.request_id);
	UT_ASSERT(next.binding_generation != service.binding_generation);
	read_return_wire(&service); /* Same receipt is independent of the successor. */
	UT_ASSERT_EQ(read_ingress_defers, 0);
	UT_ASSERT_EQ(gcs_first_fill_return_receipt_count, 1);
	read_notice_result = CLUSTER_IC_SEND_DONE;
	gcs_first_fill_return_receipt_tick(1);
	UT_ASSERT_EQ(gcs_first_fill_return_receipt_count, 0);
	UT_ASSERT(cluster_pcm_lock_read_grant_blocker_snapshot(service.key.tag, 1, &current));
	UT_ASSERT_EQ(current.binding_generation, next.binding_generation);
	UT_ASSERT_EQ(current.service_generation, next.service_generation);
	UT_ASSERT(!current.revoking);
	UT_ASSERT(cluster_pcm_lock_read_grant_done(&next.key, 1, true));
}

UT_TEST(orphan_receipt_poll_rejects_corrupt_credit_and_identity)
{
	ClusterPcmReadGrant service = read_orphan_return_setup(203);
	GcsFirstFillReturnReceipt saved;
	const char *reason;
	read_return_wire(&service);
	saved = gcs_first_fill_return_receipts[0][0];
	gcs_first_fill_return_receipt_count++;
	UT_ASSERT_EQ(gcs_first_fill_return_receipt_poll(&reason), CLUSTER_NORMAL_STOP_INVALID);
	gcs_first_fill_return_receipt_count--;
	for (int bad = 0; bad < 4; bad++) {
		GcsFirstFillReturnReceipt *receipt = &gcs_first_fill_return_receipts[0][0];
		*receipt = saved;
		if (bad == 0) receipt->service.holder_node = 2;
		if (bad == 1) receipt->holder_session = 0;
		if (bad == 2) receipt->worker = 2;
		if (bad == 3) receipt->service.binding_generation = 0;
		UT_ASSERT_EQ(gcs_first_fill_return_receipt_poll(&reason), CLUSTER_NORMAL_STOP_INVALID);
	}
	gcs_first_fill_return_receipts[0][0] = saved;
	UT_ASSERT_EQ(gcs_first_fill_return_receipt_poll(&reason), CLUSTER_NORMAL_STOP_PENDING);
}

UT_TEST(unproved_orphan_return_keeps_original_carrier_and_native_owner)
{
	ClusterPcmReadGrant service = read_orphan_return_setup(204), active, current;
	active = begin_read(service.key.tag, 1, 13, service.key.request_id);
	read_return_wire(&service);
	UT_ASSERT_EQ(read_ingress_defers, 1);
	UT_ASSERT_EQ(gcs_first_fill_return_receipt_count, 0);
	UT_ASSERT(cluster_pcm_lock_read_grant_blocker_snapshot(service.key.tag, 1, &current));
	UT_ASSERT_EQ(current.binding_generation, active.binding_generation);
	UT_ASSERT(!current.revoking);
	UT_ASSERT_EQ(read_notices + read_forwards + read_replies + read_revokes, 0);
	UT_ASSERT(cluster_pcm_lock_read_grant_done(&active.key, 1, true));
}

UT_TEST(master_consumes_quiet_return_before_reselecting_original_request)
{
	BufferTag tag = make_tag(172);
	GcsBlockDedupKey key;
	ClusterPcmReadGrant grant, current;
	reset_fake_pcm_runtime(4); key = read_wire_setup(tag);
	read_done_session = true;
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_X, 0));
	gcs_block_first_fill_request(&read_request, &key, 1, true);
	grant = gcs_first_fill_master_parks[0].grant;
	UT_ASSERT_EQ(read_forwards, 1);
	read_return_wire(&grant);
	UT_ASSERT(gcs_first_fill_master_parks[0].returned);
	UT_ASSERT(gcs_first_fill_master_parks[0].return_ack_pending);
	/* Native downgrade is not permission to replace the old holder receipt. */
	UT_ASSERT(cluster_pcm_lock_read_grant_holder_done(&grant, 0));
	for (int i = 0; i < 20; i++) {
		read_master_time += 10000; gcs_first_fill_master_tick(1);
	}
	UT_ASSERT_EQ(read_forwards, 1);
	UT_ASSERT_EQ(read_replies + read_revokes, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_read_grant_begin(tag, 1, 13, 71, 1, 99999, false, &current),
		PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(current.service_generation, 1);
	read_notice_result = CLUSTER_IC_SEND_WOULD_BLOCK;
	read_master_time += 10000; gcs_first_fill_master_tick(1);
	UT_ASSERT_EQ(read_notices, 21);
	UT_ASSERT_EQ(read_notice.reserved_0[0], GCS_BLOCK_DONE_PENDING_S_RETURNED);
	UT_ASSERT_EQ(read_forwards, 2);
	UT_ASSERT_EQ(read_forward.request_id, 71);
	UT_ASSERT_EQ(gcs_first_fill_master_parks[0].grant.service_generation, 2);
	UT_ASSERT(!gcs_first_fill_master_parks[0].return_ack_pending);
	read_return_wire(&grant); /* exact older service cannot reset generation2 */
	read_master_time += 10000; gcs_first_fill_master_tick(1);
	UT_ASSERT_EQ(read_forwards, 2);
	UT_ASSERT(!gcs_first_fill_master_parks[0].returned);
	read_confirm_original_service();
	UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
}

UT_TEST(master_return_waits_for_native_release_and_keeps_later_writer_queued)
{
	BufferTag tag = make_tag(173);
	GcsBlockDedupKey key;
	ClusterPcmReadGrant grant;
	ResourceXDecodedFrame writer;
	ResourceXMasterSnapshot queued;
	reset_fake_pcm_runtime(4); key = read_wire_setup(tag);
	read_done_session = true;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S, 0));
	gcs_block_first_fill_request(&read_request, &key, 1, true);
	grant = gcs_first_fill_master_parks[0].grant;
	writer = make_resource_x_master_frame(RESOURCE_X_WIRE_ASSERT_X, tag, 2, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(&writer, 2, &queued), RESOURCE_X_APPLY_APPLIED);
	read_return_wire(&grant);
	read_notice_result = CLUSTER_IC_SEND_DONE;
	for (int i = 0; i < 20; i++) {
		read_master_time += 10000; gcs_first_fill_master_tick(1);
	}
	UT_ASSERT_EQ(read_forwards, 1);
	UT_ASSERT_EQ(read_replies + read_revokes, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&writer.common.logical_assertion, &queued), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(queued.phase, RESOURCE_X_MASTER_QUEUED);
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_S_TO_N_RELEASE, 0));
	read_master_time += 10000; gcs_first_fill_master_tick(1);
	UT_ASSERT_EQ(read_replies, 1);
	UT_ASSERT_EQ(read_reply.status, GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK);
	UT_ASSERT_EQ(read_reply.request_id, 71);
	UT_ASSERT_EQ(gcs_first_fill_master_parks[0].grant.service_generation, 2);
	UT_ASSERT_EQ(read_revokes, 0);
	read_confirm_original_service();
}

UT_TEST(master_quiet_return_preserves_exact_cancel_and_late_consumption)
{
	BufferTag tag = make_tag(174);
	GcsBlockDedupKey key;
	ClusterPcmReadGrant grant, work;
	reset_fake_pcm_runtime(4); key = read_wire_setup(tag);
	read_done_session = true;
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S, 0));
	gcs_block_first_fill_request(&read_request, &key, 1, true);
	grant = gcs_first_fill_master_parks[0].grant;
	read_return_wire(&grant);
	UT_ASSERT(gcs_first_fill_master_cancel(&grant.key, 1));
	UT_ASSERT(gcs_first_fill_master_parks[0].return_ack_pending);
	UT_ASSERT(cluster_pcm_lock_read_grant_blocker_snapshot(tag, 1, &work));
	UT_ASSERT(work.revoking);
	read_notice_result = CLUSTER_IC_SEND_DONE;
	read_master_time += 10000; gcs_first_fill_master_tick(1);
	UT_ASSERT(gcs_first_fill_master_parks[0].in_use); /* R's native quiet notice still owns cleanup. */
	UT_ASSERT_EQ(read_forwards, 1);
	UT_ASSERT(cluster_pcm_lock_read_grant_revoke_ack(&grant.key));
	UT_ASSERT(cluster_pcm_lock_read_grant_retire_notice_admitted(&grant.key));
	read_master_time += 10000; gcs_first_fill_master_tick(1);
	UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
	read_return_wire(&grant); /* Original native owner is already retired. */
	UT_ASSERT_EQ(gcs_first_fill_return_receipt_count, 1);
	UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
	read_master_time += 10000; gcs_first_fill_master_tick(1);
	UT_ASSERT_EQ(gcs_first_fill_return_receipt_count, 0);
	UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
	UT_ASSERT_EQ(read_forwards, 1);
}

UT_TEST(returned_service_observes_a_native_release_and_reacquire_of_same_holder)
{
	BufferTag tag = make_tag(175);
	ClusterPcmReadGrant grant = {0}, next = {0};
	reset_fake_pcm_runtime(4);
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S, 0));
	grant = begin_read(tag, 1, 7, 71);
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_S_TO_N_RELEASE, 0));
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S, 0));
	UT_ASSERT_EQ(cluster_pcm_lock_read_grant_reselect_returned(&grant, 0, &next),
		PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(next.key.request_id, 71);
	UT_ASSERT_EQ(next.holder_node, 0);
	UT_ASSERT_EQ(next.service_generation, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_read_grant_reselect_returned(&next, 0, &grant),
		PCM_GCS_TRANSITION_PENDING_X);
}

UT_TEST(return_from_a_different_holder_incarnation_cannot_reset_service)
{
	BufferTag tag = make_tag(176);
	GcsBlockDedupKey key;
	ClusterPcmReadGrant grant;
	reset_fake_pcm_runtime(4); key = read_wire_setup(tag);
	read_done_session = true;
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S, 0));
	gcs_block_first_fill_request(&read_request, &key, 1, true);
	grant = gcs_first_fill_master_parks[0].grant;
	read_done_incarnation++;
	read_return_wire(&grant);
	UT_ASSERT(!gcs_first_fill_master_parks[0].returned);
	UT_ASSERT(!gcs_first_fill_master_parks[0].return_ack_pending);
	read_done_incarnation--;
	read_return_wire(&grant);
	UT_ASSERT(gcs_first_fill_master_parks[0].return_ack_pending);
}

UT_TEST(master_handed_off_service_ends_only_on_proved_holder_death_or_new_incarnation)
{
	for (int why = 0; why < 3; why++) {
		BufferTag tag = make_tag(177);
		GcsBlockDedupKey key;
		ClusterPcmReadGrant grant, current;
		reset_fake_pcm_runtime(4); key = read_wire_setup(tag);
		read_done_session = true;
		UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S, 2));
		gcs_block_first_fill_request(&read_request, &key, 1, true);
		grant = gcs_first_fill_master_parks[0].grant;
		if (why == 0) read_done_incarnation++;
		if (why == 1) read_holder_state = CLUSTER_MEMBER_DEAD;
		if (why == 2) read_done_session = false; /* unknown is not death */
		read_master_time += 10000; gcs_first_fill_master_tick(1);
		UT_ASSERT(cluster_pcm_lock_read_grant_blocker_snapshot(tag, 1, &current));
		UT_ASSERT_EQ(current.revoking, why < 2);
		UT_ASSERT_EQ(read_forwards, 1);
		UT_ASSERT_EQ(current.key.request_id, grant.key.request_id);
	}
	read_done_session = true;
}

/* One real native service owns slot zero. Other occupied slots are bounded
 * capacity sentinels; their unchanged bytes are checked, not native claims. */
static void
read_fill_other_master_slots(void)
{
	for (int i = 1; i < GCS_FIRST_FILL_PARK_MAX; i++) {
		GcsFirstFillMasterPark *park = &gcs_first_fill_master_parks[i];
		park->in_use = true;
		park->key.request_id = 1000 + i;
		park->retry_at = INT64_MAX;
	}
}

UT_TEST(master_full_ingress_waits_before_dedup_then_admits_the_same_frame)
{
	GcsBlockDedupKey key;
	GcsBlockRequestPayload original;
	ClusterICEnvelope env = {0}, original_env;
	ClusterPcmReadGrant old;
	GcsFirstFillMasterPark before[GCS_FIRST_FILL_PARK_MAX];
	reset_fake_pcm_runtime(4); key = read_wire_setup(make_tag(180));
	gcs_block_first_fill_request(&read_request, &key, 1, true);
	old = gcs_first_fill_master_parks[0].grant;
	read_fill_other_master_slots();
	memcpy(before, gcs_first_fill_master_parks, sizeof(before));
	read_request.tag = make_tag(181);
	read_request.request_id = key.request_id = gcs_reqid_requester(1, 12, 72);
	original = read_request;
	env.payload_length = sizeof(read_request);
	original_env = env;
	for (int dispatch = 0; dispatch < 3; dispatch++) {
		gcs_block_first_fill_ingress(&env, &read_request, &key, 1);
		UT_ASSERT_EQ(read_ingress_defers, 0);
		UT_ASSERT(read_deferred_envelope == NULL);
		UT_ASSERT_EQ(gcs_first_fill_ingress_count, 1);
		UT_ASSERT_EQ(memcmp(&gcs_first_fill_ingress_head->request, &original, sizeof(original)), 0);
		UT_ASSERT_EQ(read_cleanup_lookups + read_flag_pins, 0);
		UT_ASSERT_EQ(read_forwards + read_replies, 1);
		UT_ASSERT_EQ(memcmp(before, gcs_first_fill_master_parks, sizeof(before)), 0);
		UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(read_request.tag), 0);
		read_master_time += 10000;
	}
	UT_ASSERT(cluster_pcm_lock_read_grant_done(&old.key, 1, true));
	gcs_first_fill_master_tick(1);
	UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
	UT_ASSERT_EQ(gcs_first_fill_ingress_count, 0);
	UT_ASSERT_EQ(read_ingress_defers, 0);
	UT_ASSERT_EQ(read_cleanup_lookups, 1);
	UT_ASSERT_EQ(read_flag_pins, 1);
	UT_ASSERT_EQ(read_forwards + read_replies, 2);
	UT_ASSERT_EQ(gcs_first_fill_master_parks[0].request.request_id, original.request_id);
	UT_ASSERT_EQ(memcmp(before + 1, gcs_first_fill_master_parks + 1,
		sizeof(before) - sizeof(before[0])), 0);
	UT_ASSERT_EQ(memcmp(&original, &read_request, sizeof(original)), 0);
	UT_ASSERT_EQ(memcmp(&original_env, &env, sizeof(env)), 0);
	read_confirm_original_service();
	UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
}

UT_TEST(dedup_full_ingress_keeps_the_frame_without_grant_or_local_owner)
{
	GcsBlockDedupKey key;
	ClusterICEnvelope env = {0};
	GcsFirstFillMasterPark before[GCS_FIRST_FILL_PARK_MAX];
	reset_fake_pcm_runtime(4); key = read_wire_setup(make_tag(182));
	read_request.request_id = key.request_id = gcs_reqid_requester(1, 12, 73);
	memcpy(before, gcs_first_fill_master_parks, sizeof(before));
	read_cleanup_capacity = false;
	for (int dispatch = 0; dispatch < 3; dispatch++) {
		gcs_block_first_fill_ingress(&env, &read_request, &key, 1);
		UT_ASSERT_EQ(read_cleanup_lookups, 1);
		UT_ASSERT_EQ(read_ingress_defers, 0);
		UT_ASSERT(read_deferred_envelope == NULL);
		UT_ASSERT_EQ(gcs_first_fill_ingress_count, 1);
		UT_ASSERT_EQ(read_flag_pins + read_forwards + read_replies, 0);
		UT_ASSERT_EQ(memcmp(before, gcs_first_fill_master_parks, sizeof(before)), 0);
		UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(read_request.tag), 0);
	}
	read_master_time += 1000000000;
	gcs_first_fill_master_tick(1);
	UT_ASSERT_EQ(read_cleanup_lookups, 2);
	UT_ASSERT_EQ(gcs_first_fill_ingress_count, 1);
	read_cleanup_capacity = true;
	gcs_first_fill_master_tick(1);
	UT_ASSERT_EQ(read_ingress_defers, 0);
	UT_ASSERT_EQ(read_cleanup_lookups, 3);
	UT_ASSERT_EQ(gcs_first_fill_ingress_count, 0);
	UT_ASSERT_EQ(read_flag_pins, 1);
	UT_ASSERT_EQ(read_replies, 1);
	UT_ASSERT(gcs_first_fill_master_parks[0].registered);
	UT_ASSERT_EQ(gcs_first_fill_master_parks[0].request.request_id, key.request_id);
	read_confirm_original_service();
	UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
}

static GcsBlockDedupKey
read_queue_at_capacity(BlockNumber block)
{
	GcsBlockDedupKey key = read_wire_setup(make_tag(block));
	read_request.request_id = key.request_id = gcs_reqid_requester(1, 12, block);
	for (int i = 0; i < GCS_FIRST_FILL_PARK_MAX; i++) {
		gcs_first_fill_master_parks[i].in_use = true;
		gcs_first_fill_master_parks[i].retry_at = INT64_MAX;
	}
	return key;
}

UT_TEST(queued_ingress_uses_existing_backend_credit_without_evicting_originals)
{
	GcsBlockDedupKey key;
	ClusterICEnvelope env = {0};
	GcsBlockRequestPayload original;
	const char *reason;
	reset_fake_pcm_runtime(4); key = read_queue_at_capacity(190);
	for (int i = 0; i < CLUSTER_GCS_BLOCK_MAX_OUTSTANDING_PER_BACKEND; i++) {
		read_request.request_id = key.request_id = gcs_reqid_requester(1, 12, 100 + i);
		gcs_block_first_fill_ingress(&env, &read_request, &key, 1);
	}
	UT_ASSERT_EQ(gcs_first_fill_ingress_count, 8);
	original = read_request;
	/* A duplicate at the credit limit shares its existing owner. A changed
	 * payload with that same identity cannot replace its immutable bytes. */
	gcs_block_first_fill_ingress(&env, &read_request, &key, 1);
	UT_ASSERT_EQ(read_ingress_defers, 0);
	read_request.tag.blockNum++;
	gcs_block_first_fill_ingress(&env, &read_request, &key, 1);
	UT_ASSERT_EQ(read_ingress_defers, 1);
	UT_ASSERT_EQ(memcmp(&gcs_first_fill_ingress_tail->request, &original, sizeof(original)), 0);
	read_request = original;
	read_request.request_id = key.request_id = gcs_reqid_requester(1, 12, 108);
	gcs_block_first_fill_ingress(&env, &read_request, &key, 1);
	UT_ASSERT_EQ(gcs_first_fill_ingress_count, 8);
	UT_ASSERT_EQ(read_ingress_defers, 2);
	/* Another backend has its own original shared-slot credit. */
	read_request.requester_backend_id = key.requester_backend_id = 14;
	read_request.request_id = key.request_id = gcs_reqid_requester(1, 13, 109);
	gcs_block_first_fill_ingress(&env, &read_request, &key, 1);
	UT_ASSERT_EQ(gcs_first_fill_ingress_count, 9);
	UT_ASSERT_EQ(read_ingress_defers, 2);
	UT_ASSERT_EQ(read_cleanup_lookups + read_flag_pins + read_replies + read_forwards, 0);
	UT_ASSERT_EQ(gcs_first_fill_ingress_poll(&reason), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(strcmp(reason, "GCS_FIRST_FILL_INGRESS_WAIT") == 0);
	/* Test teardown does not stand in for protocol completion. */
	while (gcs_first_fill_ingress_head != NULL)
		gcs_first_fill_ingress_forget(gcs_first_fill_ingress_head);
	UT_ASSERT_EQ(gcs_first_fill_ingress_poll(&reason), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(queued_ingress_rechecks_each_authority_gate_without_fabricated_admission)
{
	for (int gate = 0; gate < 7; gate++) {
		GcsBlockDedupKey key;
		ClusterICEnvelope env = {0};
		reset_fake_pcm_runtime(4); key = read_queue_at_capacity(191);
		gcs_block_first_fill_ingress(&env, &read_request, &key, 1);
		memset(gcs_first_fill_master_parks, 0, sizeof(gcs_first_fill_master_parks));
		if (gate == 0) read_serving = false;
		if (gate == 1) { read_managed = false; read_quorum = false; }
		if (gate == 2) read_member = false;
		if (gate == 3) read_join_rebuilt = false;
		if (gate == 4) read_recovering = true;
		if (gate == 5) read_done_session = false;
		/* An unrelated worker cannot consume the routed owner. */
		read_master_time += 1000000000;
		gcs_first_fill_master_tick(gate == 6 ? 0 : 1);
		UT_ASSERT_EQ(gcs_first_fill_ingress_count, 1);
		UT_ASSERT_EQ(read_cleanup_lookups + read_flag_pins + read_replies + read_forwards, 0);
		read_serving = read_quorum = read_member = read_join_rebuilt = read_done_session = true;
		read_recovering = false;
		gcs_first_fill_master_tick(1);
		UT_ASSERT_EQ(gcs_first_fill_ingress_count, 0);
		UT_ASSERT_EQ(read_cleanup_lookups, 1);
		read_confirm_original_service();
	}
}

UT_TEST(queued_ingress_retires_only_on_proved_reconfiguration_or_incarnation_change)
{
	for (int why = 0; why < 5; why++) {
		GcsBlockDedupKey key;
		ClusterICEnvelope env = {0};
		const char *reason;
		reset_fake_pcm_runtime(4); key = read_queue_at_capacity(192);
		gcs_block_first_fill_ingress(&env, &read_request, &key, 1);
		if (why == 0) read_epoch++;
		if (why == 1) read_requester_state = CLUSTER_MEMBER_DEAD;
		if (why == 2) read_requester_state = CLUSTER_MEMBER_REMOVED;
		if (why == 3) read_done_incarnation++;
		if (why == 4) read_home = false;
		gcs_first_fill_master_tick(1);
		UT_ASSERT_EQ(gcs_first_fill_ingress_count, 0);
		UT_ASSERT_EQ(read_cleanup_lookups + read_flag_pins + read_replies + read_forwards, 0);
		UT_ASSERT_EQ(gcs_first_fill_ingress_poll(&reason), CLUSTER_NORMAL_STOP_READY);
		UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(read_request.tag), 0);
	}
}

UT_TEST(queued_ingress_normal_stop_drives_exact_cleanup_and_never_grants)
{
	GcsBlockDedupKey key;
	ClusterPcmReadGrantKey native = {0};
	ClusterICEnvelope env = {0};
	const char *reason;
	reset_fake_pcm_runtime(4); key = read_queue_at_capacity(193);
	native.tag = read_request.tag; native.request_id = key.request_id; native.epoch = 1;
	native.requester_node = 1; native.backend_id = 13;
	gcs_block_first_fill_ingress(&env, &read_request, &key, 1);
	read_stop = true;
	gcs_first_fill_master_tick(1);
	UT_ASSERT(gcs_first_fill_ingress_head->cancelling);
	UT_ASSERT_EQ(gcs_first_fill_ingress_poll(&reason), CLUSTER_NORMAL_STOP_PENDING);
	memset(gcs_first_fill_master_parks, 0, sizeof(gcs_first_fill_master_parks));
	read_cleanup_capacity = false;
	gcs_first_fill_master_tick(1);
	UT_ASSERT_EQ(gcs_first_fill_ingress_count, 1);
	read_cleanup_capacity = true;
	gcs_first_fill_master_tick(1);
	UT_ASSERT_EQ(gcs_first_fill_ingress_count, 0);
	UT_ASSERT(gcs_first_fill_master_parks[0].cancelling);
	UT_ASSERT(cluster_pcm_lock_read_grant_absent(&native));
	UT_ASSERT_EQ(read_forwards + read_replies, 0);
	/* Its original REQUEST copy is gone before the quiet ACK can retire
	 * the terminal replay history or release original requester credit. */
	UT_ASSERT(gcs_first_fill_master_cancel_ack(&native));
	read_notice_result = CLUSTER_IC_SEND_DONE;
	gcs_first_fill_master_tick(1);
	UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
	UT_ASSERT_EQ(gcs_first_fill_ingress_poll(&reason), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(queued_cancel_preserves_original_lifetime_for_fresh_dedup_registration)
{
	GcsBlockDedupKey wire;
	ClusterPcmReadGrantKey key = {0};
	ClusterICEnvelope env = {0};
	reset_fake_pcm_runtime(4); wire = read_queue_at_capacity(196);
	GcsBlockRequestPayloadSetLifetimeHintMs(&read_request, 23456);
	key.tag = read_request.tag; key.request_id = wire.request_id; key.epoch = wire.cluster_epoch;
	key.requester_node = wire.origin_node_id; key.backend_id = wire.requester_backend_id;
	gcs_block_first_fill_ingress(&env, &read_request, &wire, 1);
	memset(gcs_first_fill_master_parks, 0, sizeof(gcs_first_fill_master_parks));
	read_lifetime_guard = true;
	UT_ASSERT(gcs_first_fill_master_cancel(&key, 1));
	UT_ASSERT_EQ(read_last_lifetime_hint, 23456);
	UT_ASSERT_EQ(gcs_first_fill_ingress_count, 0);
	UT_ASSERT(gcs_first_fill_ingress_head == NULL);
	UT_ASSERT(gcs_first_fill_master_parks[0].cancelling);
	UT_ASSERT_EQ(read_forwards + read_replies, 0);
	UT_ASSERT(cluster_pcm_lock_read_grant_absent(&key));
}

UT_TEST(queued_ingress_keeps_carrier_when_identity_or_session_is_unprovable)
{
	for (int bad = 0; bad < 6; bad++) {
		GcsBlockDedupKey key;
		ClusterICEnvelope env = {0};
		reset_fake_pcm_runtime(4); key = read_queue_at_capacity(194);
		if (bad == 0) read_done_session = false;
		if (bad == 1) read_done_incarnation = 0;
		if (bad == 2) read_request.request_id = key.request_id = 71;
		if (bad == 3) key.requester_backend_id++;
		if (bad == 4) read_request.requester_backend_id = key.requester_backend_id = MaxBackends + 1;
		if (bad == 5) GcsBlockRequestPayloadSetCleanEligible(&read_request, true);
		gcs_block_first_fill_ingress(&env, &read_request, &key, 1);
		UT_ASSERT_EQ(gcs_first_fill_ingress_count, 0);
		UT_ASSERT_EQ(read_ingress_defers, 1);
		UT_ASSERT(read_deferred_envelope == &env);
		UT_ASSERT_EQ(read_cleanup_lookups + read_flag_pins + read_replies + read_forwards, 0);
	}
}

UT_TEST(queued_ingress_poll_rejects_malformed_count_credit_and_chain)
{
	GcsBlockDedupKey key;
	ClusterICEnvelope env = {0};
	const char *reason;
	reset_fake_pcm_runtime(4); key = read_queue_at_capacity(195);
	gcs_block_first_fill_ingress(&env, &read_request, &key, 1);
	gcs_first_fill_ingress_count++;
	UT_ASSERT_EQ(gcs_first_fill_ingress_poll(&reason), CLUSTER_NORMAL_STOP_INVALID);
	gcs_first_fill_ingress_count--;
	gcs_first_fill_ingress_credit[1][12]++;
	UT_ASSERT_EQ(gcs_first_fill_ingress_poll(&reason), CLUSTER_NORMAL_STOP_INVALID);
	gcs_first_fill_ingress_credit[1][12]--;
	gcs_first_fill_ingress_head->next = gcs_first_fill_ingress_head;
	UT_ASSERT_EQ(gcs_first_fill_ingress_poll(&reason), CLUSTER_NORMAL_STOP_INVALID);
	gcs_first_fill_ingress_head->next = NULL;
	UT_ASSERT_EQ(gcs_first_fill_ingress_poll(&reason), CLUSTER_NORMAL_STOP_PENDING);
}

UT_TEST(full_master_accepts_exact_duplicates_without_reoffering_service)
{
	const GcsBlockDedupResult results[] = {
		GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE, GCS_BLOCK_DEDUP_FORWARDED_DUPLICATE,
		GCS_BLOCK_DEDUP_CACHED_REPLY
	};
	GcsBlockDedupKey key;
	ClusterICEnvelope env = {0};
	GcsFirstFillMasterPark before[GCS_FIRST_FILL_PARK_MAX];
	reset_fake_pcm_runtime(4); key = read_wire_setup(make_tag(183));
	gcs_block_first_fill_request(&read_request, &key, 1, true);
	read_fill_other_master_slots();
	memcpy(before, gcs_first_fill_master_parks, sizeof(before));
	read_ingress_cached.reply_header = read_reply;
	UT_ASSERT(GcsBlockReplyHeaderHasPendingS(&read_ingress_cached.reply_header));
	for (unsigned duplicate = 0; duplicate < lengthof(results); duplicate++) {
		read_ingress_result = results[duplicate];
		gcs_block_first_fill_ingress(&env, &read_request, &key, 1);
		UT_ASSERT_EQ(read_cleanup_lookups, duplicate + 1);
		UT_ASSERT_EQ(read_flag_pins, duplicate + 1);
		UT_ASSERT_EQ(read_ingress_defers + read_cached_resends, 0);
		UT_ASSERT_EQ(read_forwards + read_replies, 1);
		UT_ASSERT_EQ(memcmp(before, gcs_first_fill_master_parks, sizeof(before)), 0);
	}
}

UT_TEST(first_fill_ingress_preserves_validation_and_immutable_flag_rejections)
{
	for (int collision = 0; collision < 2; collision++) {
		GcsBlockDedupKey key;
		ClusterICEnvelope env = {0};
		reset_fake_pcm_runtime(4); key = read_wire_setup(make_tag(184));
		if (collision) read_flags_match = false;
		else read_ingress_result = GCS_BLOCK_DEDUP_VALIDATION_FAIL;
		gcs_block_first_fill_ingress(&env, &read_request, &key, 1);
		UT_ASSERT_EQ(read_replies, 1);
		UT_ASSERT_EQ(read_reply.status, GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT);
		UT_ASSERT_EQ(read_flag_pins, collision);
		UT_ASSERT_EQ(read_ingress_defers + read_forwards, 0);
		UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
		UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(read_request.tag), 0);
	}
}

UT_TEST(first_fill_replays_without_local_ownership_cannot_create_service)
{
	for (int replay = 0; replay < 4; replay++) {
		GcsBlockDedupKey key;
		ClusterICEnvelope env = {0};
		reset_fake_pcm_runtime(4); key = read_wire_setup(make_tag(185));
		read_ingress_result = replay == 0 ? GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE
			: replay == 1 ? GCS_BLOCK_DEDUP_FORWARDED_DUPLICATE : GCS_BLOCK_DEDUP_CACHED_REPLY;
		if (replay == 2)
			read_ingress_cached.reply_header.reserved_0[5] = GCS_BLOCK_PENDING_S_REPLY_MARK;
		read_ingress_cached.reply_header.transition_id = PCM_TRANS_N_TO_S;
		read_ingress_cached.reply_header.status = replay == 2
			? GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK : GCS_BLOCK_REPLY_DENIED_PENDING_X;
		UT_ASSERT_EQ(GcsBlockReplyHeaderHasPendingS(&read_ingress_cached.reply_header), replay == 2);
		gcs_block_first_fill_ingress(&env, &read_request, &key, 1);
		UT_ASSERT_EQ(read_cleanup_lookups, 1);
		UT_ASSERT_EQ(read_flag_pins, 1);
		UT_ASSERT_EQ(read_cached_resends, replay == 3 ? 1 : 0);
		UT_ASSERT_EQ(read_ingress_defers + read_forwards + read_replies, 0);
		UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
		UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(read_request.tag), 0);
	}
}

UT_TEST(cancelled_forward_retains_master_credit_after_requester_is_quiet)
{
	BufferTag tag = make_tag(193);
	GcsBlockDedupKey key;
	ClusterPcmReadGrant original;
	reset_fake_pcm_runtime(4); key = read_wire_setup(tag);
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S, 2));
	gcs_block_first_fill_request(&read_request, &key, 1, true);
	original = gcs_first_fill_master_parks[0].grant;
	UT_ASSERT(gcs_first_fill_master_parks[0].handed_off);
	UT_ASSERT(gcs_first_fill_master_cancel(&original.key, 1));
	UT_ASSERT(cluster_pcm_lock_read_grant_revoke_ack(&original.key));
	UT_ASSERT(cluster_pcm_lock_read_grant_retire_notice_admitted(&original.key));
	UT_ASSERT(cluster_pcm_lock_read_grant_absent(&original.key));
	read_master_time += 10000; gcs_first_fill_master_tick(1);
	UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
	UT_ASSERT(gcs_first_fill_master_parks[0].cancelling);
	UT_ASSERT_EQ(read_forwards, 1);
}

static ClusterPcmReadGrant
read_cancelled_forward_on_holder(unsigned block, bool requester_quiet, int holder)
{
	BufferTag tag = make_tag(block);
	GcsBlockDedupKey key;
	ClusterPcmReadGrant original;
	reset_fake_pcm_runtime(4); key = read_wire_setup(tag);
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S, holder));
	gcs_block_first_fill_request(&read_request, &key, 1, true);
	original = gcs_first_fill_master_parks[0].grant;
	UT_ASSERT(gcs_first_fill_master_cancel(&original.key, 1));
	if (requester_quiet) {
		UT_ASSERT(cluster_pcm_lock_read_grant_revoke_ack(&original.key));
		UT_ASSERT(cluster_pcm_lock_read_grant_retire_notice_admitted(&original.key));
	}
	return original;
}

static ClusterPcmReadGrant
read_cancelled_forward(unsigned block, bool requester_quiet)
{
	return read_cancelled_forward_on_holder(block, requester_quiet, 2);
}

static GcsBlockDonePayload
read_holder_notice(const ClusterPcmReadGrant *service, uint8 action)
{
	GcsBlockDonePayload done = {0};
	done.tag = service->key.tag; done.request_id = service->key.request_id;
	done.epoch = service->key.epoch; done.sender_node = service->holder_node;
	done.requester_backend_id = service->key.backend_id; done.transition_id = PCM_TRANS_N_TO_S;
	done.reserved_0[0] = action; done.reserved_0[1] = (uint8)service->key.requester_node;
	GcsBlockDonePayloadSetPendingSService(&done, service);
	return done;
}

static void
read_deliver_holder_notice(const GcsBlockDonePayload *done)
{
	ClusterICEnvelope env = {0};
	env.source_node_id = (uint32)done->sender_node; env.dest_node_id = (uint32)cluster_node_id;
	env.epoch = done->epoch; env.payload_length = sizeof(*done);
	cluster_gcs_handle_block_done_envelope(&env, done);
}

UT_TEST(master_seal_retains_transport_and_full_service_until_exact_consumption)
{
	for (int accepted = 0; accepted < 2; accepted++) {
		ClusterPcmReadGrant original = read_cancelled_forward(195, true);
		GcsBlockDonePayload done = read_holder_notice(&original, GCS_BLOCK_DONE_PENDING_S_SEALED);
		GcsBlockDonePayload first;
		/* An unsolicited SEALED cannot consume a service credit. */
		read_deliver_holder_notice(&done);
		UT_ASSERT(gcs_first_fill_master_parks[0].holder_outstanding);
		for (int refused = 0; refused < 3; refused++) {
			read_notice_result = refused == 1 ? CLUSTER_IC_SEND_HARD_ERROR : CLUSTER_IC_SEND_NOT_ADMITTED;
			read_master_time += 10000; gcs_first_fill_master_tick(1);
			if (refused == 0) first = read_notice;
			UT_ASSERT_EQ(memcmp(&first, &read_notice, sizeof(first)), 0);
			UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
			UT_ASSERT(!gcs_first_fill_master_parks[0].holder_seal_admitted);
		}
		read_notice_result = accepted ? CLUSTER_IC_SEND_WOULD_BLOCK : CLUSTER_IC_SEND_DONE;
		read_master_time += 10000; gcs_first_fill_master_tick(1);
		UT_ASSERT(gcs_first_fill_master_parks[0].holder_seal_admitted);
		for (int bad = 0; bad < 14; bad++) {
			GcsBlockDonePayload wrong = done;
			ClusterICEnvelope env = {0};
			env.source_node_id = (uint32)wrong.sender_node; env.dest_node_id = (uint32)cluster_node_id;
			env.epoch = wrong.epoch; env.payload_length = sizeof(wrong);
			switch (bad) {
			case 0: wrong.request_id++; break;
			case 1: wrong.tag.blockNum++; break;
			case 2: wrong.requester_backend_id++; break;
			case 3: wrong.reserved_0[1] = 3; break;
			case 4: wrong.reserved_0[2] ^= 1; break;
			case 5: wrong.reserved_0[10] ^= 1; break;
			case 6: wrong.reserved_0[14] ^= 1; break;
			case 7: wrong.reserved_0[15] = 4; break;
			case 8: wrong.reserved_0[16] = 1; break;
			case 9: env.source_node_id = 3; break;
			case 10: env.dest_node_id++; break;
			case 11: env.epoch++; break;
			case 12: read_done_incarnation = 10; break;
			case 13: read_done_incarnation = 0; break;
			}
			cluster_gcs_handle_block_done_envelope(&env, &wrong);
			read_done_incarnation = 9;
			UT_ASSERT(gcs_first_fill_master_parks[0].holder_outstanding);
		}
		for (int tick = 0; tick < 3; tick++) {
			read_master_time += 10000000; gcs_first_fill_master_tick(1);
			UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
		}
		UT_ASSERT_EQ(read_notices, 5); /* Initial refusal plus four offers; takeover is never resubmitted. */
		read_deliver_holder_notice(&done);
		UT_ASSERT(!gcs_first_fill_master_parks[0].holder_outstanding);
		read_master_time += 10000; gcs_first_fill_master_tick(1);
		UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
		read_deliver_holder_notice(&done); /* Duplicate cannot create another owner. */
		UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
		UT_ASSERT_EQ(read_forwards, 1);
	}
}

UT_TEST(holder_sealed_does_not_retire_native_revoke_or_crossing_return)
{
	ClusterPcmReadGrant original = read_cancelled_forward(196, false);
	GcsBlockDonePayload done;
	read_notice_result = CLUSTER_IC_SEND_DONE;
	UT_ASSERT(!gcs_first_fill_master_holder_quiet(&gcs_first_fill_master_parks[0]));
	done = read_holder_notice(&original, GCS_BLOCK_DONE_PENDING_S_NACK);
	read_deliver_holder_notice(&done);
	UT_ASSERT(gcs_first_fill_master_parks[0].holder_outstanding);
	done = read_holder_notice(&original, GCS_BLOCK_DONE_PENDING_S_RETURN);
	read_deliver_holder_notice(&done);
	read_master_time += 10000; gcs_first_fill_master_tick(1);
	UT_ASSERT(gcs_first_fill_master_parks[0].holder_outstanding);
	UT_ASSERT(!cluster_pcm_lock_read_grant_absent(&original.key));
	done = read_holder_notice(&original, GCS_BLOCK_DONE_PENDING_S_SEALED);
	read_deliver_holder_notice(&done);
	read_master_time += 10000; gcs_first_fill_master_tick(1);
	UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
	UT_ASSERT(!cluster_pcm_lock_read_grant_absent(&original.key));
	UT_ASSERT(cluster_pcm_lock_read_grant_revoke_ack(&original.key));
	read_master_time += 10000; gcs_first_fill_master_tick(1);
	UT_ASSERT(gcs_first_fill_master_parks[0].in_use); /* 60302 still owns its notice. */
	UT_ASSERT(cluster_pcm_lock_read_grant_retire_notice_admitted(&original.key));
	read_master_time += 10000; gcs_first_fill_master_tick(1);
	UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
}

UT_TEST(unadmitted_seal_can_finish_by_exact_holder_terminal_notice)
{
	for (int returned = 0; returned < 2; returned++) {
		ClusterPcmReadGrant original = read_cancelled_forward(197, true);
		GcsBlockDonePayload done = read_holder_notice(&original, returned
			? GCS_BLOCK_DONE_PENDING_S_RETURN : GCS_BLOCK_DONE_PENDING_S_NACK);
		read_master_time += 10000; gcs_first_fill_master_tick(1);
		UT_ASSERT(!gcs_first_fill_master_parks[0].holder_seal_admitted);
		read_deliver_holder_notice(&done);
		UT_ASSERT(!gcs_first_fill_master_parks[0].holder_outstanding);
		read_notice_result = CLUSTER_IC_SEND_DONE;
		read_master_time += 10000; gcs_first_fill_master_tick(1);
		UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
		UT_ASSERT_EQ(read_notices, returned ? 3 : 2);
	}
}

UT_TEST(master_cancel_barrier_retains_unknown_and_ends_only_on_proved_change)
{
	for (int end = 0; end < 5; end++) {
		unsigned notices;
		(void)read_cancelled_forward(198, true);
		notices = read_notices;
		for (int unknown = 0; unknown < 3; unknown++) {
			read_unknown_home = unknown == 0; read_done_session = unknown != 1;
			read_done_incarnation = unknown == 2 ? 0 : 9;
			read_stop = true;
			read_master_time += 1000000000; gcs_first_fill_master_tick(1);
			UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
			UT_ASSERT_EQ(read_notices, notices);
		}
		read_unknown_home = false; read_done_session = true; read_done_incarnation = 9;
		switch (end) {
		case 0: read_holder_state = CLUSTER_MEMBER_DEAD; break;
		case 1: read_holder_state = CLUSTER_MEMBER_REMOVED; break;
		case 2: read_done_incarnation = 10; break;
		case 3: read_home = false; break;
		case 4: read_epoch++; break;
		}
		read_master_time += 10000; gcs_first_fill_master_tick(1);
		UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
		UT_ASSERT_EQ(read_forwards, 1);
	}
}

UT_TEST(master_cancelled_forward_history_is_bounded_by_retained_service_credits)
{
	GcsBlockDedupKey key;
	ClusterPcmReadGrant first = {0};
	reset_fake_pcm_runtime(4); key = read_wire_setup(make_tag(200));
	read_notice_result = CLUSTER_IC_SEND_DONE;
	for (int i = 0; i < GCS_FIRST_FILL_PARK_MAX; i++) {
		ClusterPcmReadGrant grant;
		/* Serial native claims on one tag, cancelled before each successor:
		 * all historical FORWARDs still consume distinct master credits. */
		read_request.tag = make_tag(200);
		read_request.request_id = key.request_id = gcs_reqid_requester(1, 12, 1000 + i);
		UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(read_request.tag, PCM_TRANS_N_TO_S, 2));
		gcs_block_first_fill_request(&read_request, &key, 1, true);
		grant = gcs_first_fill_master_parks[i].grant;
		if (i == 0) first = grant;
		UT_ASSERT(gcs_first_fill_master_cancel(&grant.key, 1));
		UT_ASSERT(cluster_pcm_lock_read_grant_revoke_ack(&grant.key));
		UT_ASSERT(cluster_pcm_lock_read_grant_retire_notice_admitted(&grant.key));
		read_master_time += 10000; gcs_first_fill_master_tick(1);
		UT_ASSERT(gcs_first_fill_master_parks[i].holder_seal_admitted);
	}
	read_request.tag = make_tag(300); read_request.request_id = ++key.request_id;
	gcs_block_first_fill_request(&read_request, &key, 1, true);
	UT_ASSERT_EQ(read_forwards, GCS_FIRST_FILL_PARK_MAX);
	for (int i = 0; i < GCS_FIRST_FILL_PARK_MAX; i++)
		UT_ASSERT(gcs_first_fill_master_parks[i].in_use);
	{
		GcsBlockDonePayload done = read_holder_notice(&first, GCS_BLOCK_DONE_PENDING_S_SEALED);
		read_deliver_holder_notice(&done);
	}
	read_master_time += 10000; gcs_first_fill_master_tick(1);
	UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
	gcs_block_first_fill_request(&read_request, &key, 1, true);
	UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
	UT_ASSERT_EQ(read_forwards + read_replies, GCS_FIRST_FILL_PARK_MAX + 1);
	{
		ClusterPcmReadGrant successor = gcs_first_fill_master_parks[0].grant, current;
		GcsBlockDonePayload done = read_holder_notice(&first, GCS_BLOCK_DONE_PENDING_S_SEALED);
		read_deliver_holder_notice(&done);
		UT_ASSERT_EQ(cluster_pcm_lock_read_grant_begin(successor.key.tag,
			successor.key.requester_node, successor.key.backend_id, successor.key.request_id,
			successor.key.epoch, 99999, false, &current), PCM_GCS_TRANSITION_APPLIED);
		UT_ASSERT_EQ(current.key.request_id, successor.key.request_id);
		UT_ASSERT(gcs_first_fill_master_parks[0].in_use);
	}
}

UT_TEST(local_master_holder_seal_uses_the_same_ring_as_original_forward)
{
	BufferTag tag = make_tag(301);
	GcsBlockDedupKey key;
	ClusterPcmReadGrant original;
	reset_fake_pcm_runtime(4); key = read_wire_setup(tag);
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S, cluster_node_id));
	gcs_block_first_fill_request(&read_request, &key, 1, true);
	original = gcs_first_fill_master_parks[0].grant;
	UT_ASSERT_EQ(read_local_forwards, 1);
	UT_ASSERT(gcs_first_fill_master_cancel(&original.key, 1));
	UT_ASSERT(cluster_pcm_lock_read_grant_revoke_ack(&original.key));
	UT_ASSERT(cluster_pcm_lock_read_grant_retire_notice_admitted(&original.key));
	read_master_time += 10000; gcs_first_fill_master_tick(1);
	UT_ASSERT_EQ(read_local_seals, 2);
	UT_ASSERT(!gcs_first_fill_master_parks[0].holder_seal_admitted);
	read_notice_result = CLUSTER_IC_SEND_WOULD_BLOCK;
	read_master_time += 10000; gcs_first_fill_master_tick(1);
	UT_ASSERT_EQ(read_local_seals, 3);
	UT_ASSERT(gcs_first_fill_master_parks[0].holder_seal_admitted);
	UT_ASSERT_EQ(read_notices, 0); /* No direct self-dispatch ahead of queued FORWARD. */
	{
		GcsBlockDonePayload done = read_holder_notice(&original, GCS_BLOCK_DONE_PENDING_S_SEALED);
		read_deliver_holder_notice(&done);
	}
	read_master_time += 10000; gcs_first_fill_master_tick(1);
	UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
}

UT_TEST(one_shot_seal_carrier_survives_unknown_identity_at_either_endpoint)
{
	for (int sealed = 0; sealed < 2; sealed++) {
		ClusterPcmReadGrant original = read_cancelled_forward_on_holder(302, true,
			sealed ? 2 : cluster_node_id);
		GcsBlockDonePayload done;
		ClusterICEnvelope env = {0};
		done = read_holder_notice(&original, sealed
			? GCS_BLOCK_DONE_PENDING_S_SEALED : GCS_BLOCK_DONE_PENDING_S_SEAL);
		if (!sealed) done.sender_node = cluster_node_id;
		env.source_node_id = (uint32)done.sender_node; env.dest_node_id = (uint32)cluster_node_id;
		env.epoch = done.epoch; env.payload_length = sizeof(done);
		for (int unknown = 0; unknown < 3; unknown++) {
			read_unknown_home = unknown == 0; read_done_session = unknown != 1;
			read_done_incarnation = unknown == 2 ? 0 : 9;
			read_deferred_envelope = NULL;
			cluster_gcs_handle_block_done_envelope(&env, &done);
			UT_ASSERT_EQ(read_ingress_defers, unknown + 1);
			UT_ASSERT(read_deferred_envelope == &env);
			UT_ASSERT(gcs_first_fill_master_parks[0].holder_outstanding);
		}
		read_unknown_home = false; read_done_session = true; read_done_incarnation = 9;
		/* A forged carrier is rejected even while authority is unknown. */
		read_unknown_home = true; env.source_node_id++;
		cluster_gcs_handle_block_done_envelope(&env, &done);
		UT_ASSERT_EQ(read_ingress_defers, 3);
		read_unknown_home = false; env.source_node_id = (uint32)done.sender_node;
		if (sealed) {
			read_notice_result = CLUSTER_IC_SEND_DONE;
			read_master_time += 10000; gcs_first_fill_master_tick(1);
			cluster_gcs_handle_block_done_envelope(&env, &done);
			read_master_time += 10000; gcs_first_fill_master_tick(1);
			UT_ASSERT(!gcs_first_fill_master_parks[0].in_use);
		}
	}
}

UT_TEST(holder_cancel_seal_wire_requires_explicit_holder_and_service)
{
	ClusterPcmReadGrant service = {0}, decoded;
	GcsBlockDonePayload done = {0};
	service.key.tag = make_tag(194); service.key.request_id = 71;
	service.key.epoch = 1; service.key.requester_node = 1; service.key.backend_id = 13;
	service.holder_node = 2; service.binding_generation = 7; service.service_generation = 3;
	done.tag = service.key.tag; done.request_id = 71; done.epoch = 1;
	done.requester_backend_id = 13; done.transition_id = PCM_TRANS_N_TO_S;
	done.sender_node = 0; done.reserved_0[1] = 1;
	for (int action = 0xa7; action <= 0xa8; action++) {
		done.reserved_0[0] = (uint8)action;
		done.sender_node = action == 0xa7 ? 0 : 2;
		GcsBlockDonePayloadSetPendingSService(&done, &service);
		UT_ASSERT(GcsBlockDonePayloadPendingSService(&done, &decoded));
		UT_ASSERT_EQ(decoded.holder_node, 2);
		UT_ASSERT_EQ(decoded.binding_generation, 7);
		UT_ASSERT_EQ(decoded.service_generation, 3);
		for (int bad = 0; bad < 8; bad++) {
			GcsBlockDonePayload wrong = done;
			if (bad == 0) wrong.reserved_0[15] = 0;
			else if (bad == 1) wrong.reserved_0[15] = 33;
			else if (bad == 2) wrong.reserved_0[15] = 2;
			else if (bad == 3) wrong.reserved_0[14] = 2;
			else if (bad == 4) memset(&wrong.reserved_0[2], 0, sizeof(uint64));
			else if (bad == 5) memset(&wrong.reserved_0[10], 0xff, sizeof(uint32));
			else if (bad == 6) wrong.reserved_0[16] = 1;
			else wrong.reserved_0[1] = 32;
			UT_ASSERT(!GcsBlockDonePayloadPendingSService(&wrong, &decoded));
		}
		if (action == GCS_BLOCK_DONE_PENDING_S_SEAL) {
			done.sender_node = service.key.requester_node; /* M may also be R. */
			UT_ASSERT(GcsBlockDonePayloadPendingSService(&done, &decoded));
		}
	}
}

int
main(void)
{
	UT_PLAN(77);
	UT_RUN(one_shot_seal_carrier_survives_unknown_identity_at_either_endpoint);
	UT_RUN(local_master_holder_seal_uses_the_same_ring_as_original_forward);
	UT_RUN(master_seal_retains_transport_and_full_service_until_exact_consumption);
	UT_RUN(holder_sealed_does_not_retire_native_revoke_or_crossing_return);
	UT_RUN(unadmitted_seal_can_finish_by_exact_holder_terminal_notice);
	UT_RUN(master_cancel_barrier_retains_unknown_and_ends_only_on_proved_change);
	UT_RUN(master_cancelled_forward_history_is_bounded_by_retained_service_credits);
	UT_RUN(cancelled_forward_retains_master_credit_after_requester_is_quiet);
	UT_RUN(holder_cancel_seal_wire_requires_explicit_holder_and_service);
	UT_RUN(new_authenticated_holder_instance_can_retire_its_full_old_receipt_row);
	UT_RUN(orphan_return_receipts_are_bounded_per_original_sealed_holder);
	UT_RUN(orphan_receipt_waits_through_age_unknown_session_send_refusal_and_stop);
	UT_RUN(orphan_receipt_retires_only_for_proved_master_or_holder_reconfiguration);
	UT_RUN(orphan_receipt_does_not_borrow_or_retire_a_native_successor);
	UT_RUN(orphan_receipt_poll_rejects_corrupt_credit_and_identity);
	UT_RUN(unproved_orphan_return_keeps_original_carrier_and_native_owner);
	UT_RUN(orphan_return_does_not_retain_the_receive_head_at_master_capacity);
	UT_RUN(queued_cancel_preserves_original_lifetime_for_fresh_dedup_registration);
	UT_RUN(queued_ingress_uses_existing_backend_credit_without_evicting_originals);
	UT_RUN(queued_ingress_rechecks_each_authority_gate_without_fabricated_admission);
	UT_RUN(queued_ingress_retires_only_on_proved_reconfiguration_or_incarnation_change);
	UT_RUN(queued_ingress_normal_stop_drives_exact_cleanup_and_never_grants);
	UT_RUN(queued_ingress_keeps_carrier_when_identity_or_session_is_unprovable);
	UT_RUN(queued_ingress_poll_rejects_malformed_count_credit_and_chain);
	UT_RUN(master_full_ingress_waits_before_dedup_then_admits_the_same_frame);
	UT_RUN(dedup_full_ingress_keeps_the_frame_without_grant_or_local_owner);
	UT_RUN(full_master_accepts_exact_duplicates_without_reoffering_service);
	UT_RUN(first_fill_ingress_preserves_validation_and_immutable_flag_rejections);
	UT_RUN(first_fill_replays_without_local_ownership_cannot_create_service);
	UT_RUN(master_handed_off_service_ends_only_on_proved_holder_death_or_new_incarnation);
	UT_RUN(return_from_a_different_holder_incarnation_cannot_reset_service);
	UT_RUN(returned_service_observes_a_native_release_and_reacquire_of_same_holder);
	UT_RUN(master_consumes_quiet_return_before_reselecting_original_request);
	UT_RUN(master_return_waits_for_native_release_and_keeps_later_writer_queued);
	UT_RUN(master_quiet_return_preserves_exact_cancel_and_late_consumption);
	UT_RUN(returned_service_reselects_only_after_native_source_change);
	UT_RUN(returned_service_cannot_cross_identity_cancel_or_pi);
	UT_RUN(test_service_done_wire_is_canonical_and_requester_actions_stay_episode_scoped);
	UT_RUN(test_actual_done_ingress_checks_holder_service_and_preserves_requester_cancel);
	UT_RUN(test_holder_service_rejects_stale_binding_generation_and_source);
	UT_RUN(test_unbound_holder_failure_cannot_revoke_native_claim);
	UT_RUN(test_native_service_generation_survives_replay_and_exact_retirement);
	UT_RUN(test_saved_frame_cannot_cross_native_service_generation);
	UT_RUN(test_handed_off_master_waits_without_replaying_image_or_disk_grant);
	UT_RUN(test_master_service_retirement_cannot_consume_same_node_successor);
	UT_RUN(test_master_service_slots_return_after_each_native_confirmation);
	UT_RUN(test_cached_replay_without_local_owner_never_mints_another_delivery);
	UT_RUN(test_master_retains_exact_disk_reply_until_actual_handoff);
	UT_RUN(test_registered_outbound_cancel_keeps_native_revoke_owner);
	UT_RUN(test_registered_outbound_cancel_preserves_consumed_ack_notice);
	UT_RUN(test_registered_outbound_rechecks_all_authority_gates);
	UT_RUN(test_registered_outbound_waits_for_committed_member_cleanup);
	UT_RUN(test_registered_service_epoch_or_stop_cancels_without_redelivery);
	UT_RUN(test_master_capacity_precedes_native_grant_and_never_evicts);
	UT_RUN(test_unconfirmed_or_cancelling_grantee_is_never_an_image_source);
	UT_RUN(test_source_selection_skips_pending_copy_and_uses_proven_shared_holder);
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
	UT_RUN(test_live_grant_ignores_age_but_exact_cancellation_cannot_resurrect);
	UT_RUN(test_x_downgrade_confirmation_orders_independent_notify);
	UT_RUN(test_nack_membership_and_entry_lifetime_keep_pending_responsibility);
	UT_RUN(test_native_master_keeps_original_service_behind_later_x);
	UT_RUN(test_native_master_disk_grant_has_typed_marker_and_no_forward);
	UT_RUN(test_native_master_retains_refused_forward_until_transport_takeover);
	UT_RUN(test_native_master_holder_loopback_uses_existing_data_ring);
	UT_RUN(test_pending_done_and_revoke_echo_are_exact_and_reject_unknown_fields);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
