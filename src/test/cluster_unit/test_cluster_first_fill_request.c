/* Execute the production first-fill enqueue and original-slot CV wait.
 * Native reservation, exact cleanup and receipt consumption bodies are real;
 * ring admission, wakes and authority events are controlled boundaries.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#include "postgres.h"
#include "cluster/cluster_lms.h"
int first_fill_request_retire_fixture_main(void);
#define FIRST_FILL_RETIRE_MAIN first_fill_request_retire_fixture_main
#include "test_cluster_first_fill_retire.c"

static ClusterGcsBlockOutstandingSlot *request_slot;
static ClusterPcmReadSource request_source;
static GcsBlockRequestPayload request_payload;
static unsigned request_tries, request_admitted, request_full, request_capacity_waits;
static unsigned request_prepares, request_cancels, request_sleeps, request_finish_at;
static int request_event;
static uint8 request_final;
static bool request_early, request_unavailable, request_cancel_capacity;

static void
request_exact_native(void)
{
	UT_ASSERT(request_slot->in_use);
	UT_ASSERT_EQ(request_slot->request_id, request_source.request_id);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_source.base_generation);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), request_source.reservation_token);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT(pg_atomic_read_u32(&transition_buf->state) & BM_IO_IN_PROGRESS);
}

static void
request_deliver(uint8 status)
{
	request_slot->reply_header.status = status;
	request_slot->reply_received = true;
}

static ClusterLmsEnqueueResult
request_enqueue(int worker, uint8 type, uint32 destination, const void *payload, uint16 len)
{
	UT_ASSERT_EQ(worker, 0);
	UT_ASSERT_EQ(type, PGRAC_IC_MSG_GCS_BLOCK_REQUEST);
	UT_ASSERT_EQ(destination, 2);
	UT_ASSERT_EQ(len, sizeof(request_payload));
	UT_ASSERT_EQ(memcmp(payload, &request_payload, len), 0);
	request_exact_native();
	request_tries++;
	if (request_unavailable) return CLUSTER_LMS_ENQUEUE_UNAVAILABLE;
	if (request_full) {
		request_full--;
		return CLUSTER_LMS_ENQUEUE_FULL;
	}
	request_admitted++;
	if (request_early) request_deliver(request_final);
	return CLUSTER_LMS_ENQUEUE_ADMITTED;
}

static bool
request_capacity(int worker, uint64 epoch)
{
	UT_ASSERT_EQ(worker, 0);
	UT_ASSERT_EQ(epoch, request_source.request_epoch);
	UT_ASSERT_EQ(request_admitted, 0);
	request_capacity_waits++;
	request_exact_native();
	if (request_cancel_capacity) pg_re_throw();
	return true;
}

static int
request_sleep(void)
{
	request_sleeps++;
	request_exact_native();
	UT_ASSERT_EQ(request_admitted, 1);
	UT_ASSERT(!request_slot->reply_received);
	retire_now += 10000000; /* Time alone cannot retire or rearm the request. */
	if (request_sleeps >= request_finish_at) {
		switch (request_event) {
		case 1: retire_cancel = true; break;
		case 2: retire_epoch++; break;
		case 3: retire_route = 4; break;
		case 4: retire_master_state = CLUSTER_MEMBER_DEAD; break;
		case 5: retire_master_state = CLUSTER_MEMBER_REMOVED; break;
		case 6: retire_peer_session++; break;
		case 7: request_slot->stale = true; break;
		case 8: request_slot->first_fill_revoked = true; break;
		default:
			retire_session = true;
			request_deliver(request_final);
			break;
		}
	} else if (request_event == 9) {
		static const uint8 busy[] = { GCS_BLOCK_REPLY_DENIED_PENDING_X,
			GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER, GCS_BLOCK_REPLY_DENIED_DEDUP_FULL };
		request_deliver(busy[(request_sleeps - 1) % lengthof(busy)]);
	}
	return 0;
}

#undef ConditionVariablePrepareToSleep
#undef ConditionVariableCancelSleep
#undef ConditionVariableTimedSleep
#define ConditionVariablePrepareToSleep(cv) (++request_prepares)
#define ConditionVariableCancelSleep() (++request_cancels)
#define ConditionVariableTimedSleep(...) request_sleep()
#define cluster_lms_outbound_try_enqueue request_enqueue
#define cluster_lms_outbound_wait_capacity request_capacity
#include "test_cluster_first_fill_request.inc"

/* The codec is a controlled boundary; the whole real receive handler still
 * decides domain, authenticated sender chain, identity and publication order. */
static bool request_decode_valid = true;
static bool
request_decode(const ClusterICEnvelope *env pg_attribute_unused(), const void *payload,
	const GcsBlockReplyHeader **hdr, const char **data, bool *sf, uint8 *flags,
	ClusterSfDepVec *deps, const ClusterGcsUndoAuthTrailer **undo)
{
	*hdr = payload;
	*data = (const char *)payload + sizeof(**hdr);
	*sf = false; *flags = 0; *undo = NULL;
	cluster_sf_dep_vec_reset(deps);
	return request_decode_valid;
}
#define gcs_block_try_resource_x_frame(env, payload) false
#define gcs_block_try_land_ctrc_reply(env, payload) false
#define gcs_block_try_land_current_mx_reply(env, payload) false
#define gcs_block_try_land_r4_terminal_reply(env, payload) false
#define gcs_block_decode_reply_payload request_decode
#define cluster_shared_config false
#define cluster_cr_server_r4_land_foreign_undo(...) false
#define gcs_block_compute_checksum(data) 0
#undef gcs_block_wake_local_pending_s_request
#include "test_cluster_first_fill_reply.inc"

static void
request_receive(uint8 status, unsigned char image, int sender)
{
	struct {
		GcsBlockReplyHeader hdr;
		char block[GCS_BLOCK_DATA_SIZE];
	} frame;
	ClusterICEnvelope env = {0};
	memset(&frame, 0, sizeof(frame));
	frame.hdr.request_id = request_source.request_id;
	frame.hdr.epoch = request_source.request_epoch;
	frame.hdr.sender_node = sender;
	frame.hdr.requester_backend_id = MyBackendId;
	frame.hdr.transition_id = PCM_TRANS_N_TO_S;
	frame.hdr.status = status;
	GcsBlockReplyHeaderSetForwardingMasterNode(&frame.hdr, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	memset(frame.block, image, sizeof(frame.block));
	env.source_node_id = sender; env.dest_node_id = cluster_node_id;
	cluster_gcs_handle_block_reply_envelope(&env, &frame);
}

static void
request_reset(BufferDesc *buf, ClusterPcmOwnEntry *entry)
{
	retire_reset(buf, entry, true);
	request_source = retire_arm(buf, false);
	request_slot = &retire_blocks[0].slots[0];
	memset(&request_payload, 0, sizeof(request_payload));
	request_payload.request_id = request_source.request_id;
	request_payload.epoch = request_source.request_epoch;
	request_payload.tag = request_source.tag;
	request_payload.sender_node = cluster_node_id;
	request_payload.requester_backend_id = MyBackendId;
	request_payload.transition_id = PCM_TRANS_N_TO_S;
	GcsBlockRequestPayloadSetReadMiss(&request_payload, true);
	request_tries = request_admitted = request_full = request_capacity_waits = 0;
	request_prepares = request_cancels = request_sleeps = 0;
	request_finish_at = 1;
	request_event = 0;
	request_final = GCS_BLOCK_REPLY_GRANTED;
	request_early = request_unavailable = request_cancel_capacity = false;
	request_decode_valid = true;
}

static bool
request_run(void)
{
	volatile bool caught = false;
	PG_TRY();
	{
		gcs_first_fill_send_wait(request_slot, &request_payload, &request_source);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	return !caught;
}

static void
request_retire_consumed(BufferDesc *buf)
{
	GcsBlockInvalidatePayload inv;
	GcsBlockDonePayload done;
	retire_abort(buf, &request_source);
	UT_ASSERT(request_slot->first_fill.quiet);
	inv = retire_invalidate(&request_source);
	retire_result = CLUSTER_IC_SEND_DONE;
	UT_ASSERT_EQ(gcs_first_fill_receipt_revoke(&inv), 1);
	UT_ASSERT(request_slot->first_fill.ack_handed);
	done = retire_notice(&request_source);
	UT_ASSERT(retire_consume(&done));
	UT_ASSERT(!request_slot->in_use);
	UT_ASSERT_EQ(retire_receipts, 0);
}

UT_TEST(test_final_outcomes_use_one_original_admission)
{
	const uint8 final[] = { GCS_BLOCK_REPLY_GRANTED, GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER,
		GCS_BLOCK_REPLY_DENIED_INCOMPATIBLE };
	for (unsigned i = 0; i < lengthof(final); i++) {
		BufferDesc buf; ClusterPcmOwnEntry entry;
		request_reset(&buf, &entry); request_final = final[i];
		UT_ASSERT(request_run());
		UT_ASSERT_EQ(request_slot->reply_header.status, final[i]);
		UT_ASSERT_EQ(request_tries, 1); UT_ASSERT_EQ(request_admitted, 1);
		UT_ASSERT_EQ(request_prepares, 1); UT_ASSERT_EQ(request_cancels, 1);
		request_exact_native(); /* The common decoder still owns final validation/publication. */
	}
}

UT_TEST(test_busy_replies_wait_on_original_episode_without_rearm)
{
	BufferDesc buf; ClusterPcmOwnEntry entry;
	request_reset(&buf, &entry); request_event = 9; request_finish_at = 10;
	UT_ASSERT(request_run());
	UT_ASSERT_EQ(request_sleeps, 10);
	UT_ASSERT_EQ(request_tries, 1); UT_ASSERT_EQ(request_admitted, 1);
	UT_ASSERT_EQ(request_prepares, 1); UT_ASSERT_EQ(request_cancels, 1);
	request_exact_native();
}

UT_TEST(test_timer_and_spurious_wakes_cannot_expire_service)
{
	BufferDesc buf; ClusterPcmOwnEntry entry;
	request_reset(&buf, &entry); request_finish_at = 150;
	UT_ASSERT(request_run());
	UT_ASSERT_EQ(request_sleeps, 150); UT_ASSERT_EQ(request_tries, 1);
	UT_ASSERT_EQ(request_cancels, 1); request_exact_native();
}

UT_TEST(test_reply_before_cv_registration_is_not_lost)
{
	BufferDesc buf; ClusterPcmOwnEntry entry;
	request_reset(&buf, &entry); request_early = true;
	UT_ASSERT(request_run()); UT_ASSERT_EQ(request_sleeps, 0);
	UT_ASSERT_EQ(request_prepares, 1); UT_ASSERT_EQ(request_cancels, 1);
	UT_ASSERT_EQ(request_tries, 1);
}

UT_TEST(test_full_local_ring_waits_with_identical_bytes_until_one_takeover)
{
	BufferDesc buf; ClusterPcmOwnEntry entry;
	request_reset(&buf, &entry); request_full = 4;
	UT_ASSERT(request_run());
	UT_ASSERT_EQ(request_capacity_waits, 4); UT_ASSERT_EQ(request_tries, 5);
	UT_ASSERT_EQ(request_admitted, 1); request_exact_native();
}

UT_TEST(test_cancel_before_admission_has_no_remote_receipt)
{
	BufferDesc buf; ClusterPcmOwnEntry entry;
	request_reset(&buf, &entry); request_full = 1; request_cancel_capacity = true;
	UT_ASSERT(!request_run()); UT_ASSERT_EQ(request_admitted, 0);
	UT_ASSERT_EQ(request_prepares, 0); UT_ASSERT_EQ(request_cancels, 0);
	request_exact_native(); retire_abort(&buf, &request_source);
	UT_ASSERT(!request_slot->in_use); UT_ASSERT_EQ(retire_receipts, 0);
	UT_ASSERT(pg_atomic_read_u32(&buf.state) & BM_IO_IN_PROGRESS);
}

UT_TEST(test_unavailable_before_admission_cannot_fabricate_handoff)
{
	BufferDesc buf; ClusterPcmOwnEntry entry;
	request_reset(&buf, &entry); request_unavailable = true;
	UT_ASSERT(!request_run()); UT_ASSERT_EQ(request_admitted, 0);
	UT_ASSERT(!request_slot->first_fill.request_admitted);
	request_exact_native(); retire_abort(&buf, &request_source);
	UT_ASSERT(!request_slot->in_use); UT_ASSERT_EQ(request_cancels, 0);
}

UT_TEST(test_cancel_after_admission_keeps_exact_quiet_ack_responsibility)
{
	BufferDesc buf; ClusterPcmOwnEntry entry;
	request_reset(&buf, &entry); request_event = 1;
	UT_ASSERT(!request_run()); UT_ASSERT_EQ(request_cancels, 1);
	UT_ASSERT_EQ(request_tries, 1); request_exact_native();
	request_retire_consumed(&buf);
	UT_ASSERT(pg_atomic_read_u32(&buf.state) & BM_IO_IN_PROGRESS);
}

UT_TEST(test_proved_authority_changes_end_original_wait_without_resend)
{
	for (int event = 2; event <= 8; event++) {
		BufferDesc buf; ClusterPcmOwnEntry entry;
		request_reset(&buf, &entry); request_event = event;
		UT_ASSERT(!request_run()); UT_ASSERT_EQ(request_tries, 1);
		UT_ASSERT_EQ(request_cancels, 1); request_exact_native();
		/* Reconfiguration cleanup has its own native tests. Do not invent an
		 * ACK from the changed authority here; original receipt is still armed. */
		UT_ASSERT(request_slot->first_fill.armed);
	}
}

UT_TEST(test_unknown_session_retains_wait_and_wrong_owner_never_sends)
{
	BufferDesc buf; ClusterPcmOwnEntry entry;
	request_reset(&buf, &entry); retire_session = false; request_finish_at = 6;
	UT_ASSERT(request_run()); UT_ASSERT_EQ(request_sleeps, 6);
	UT_ASSERT_EQ(request_tries, 1);
	for (int mismatch = 0; mismatch < 6; mismatch++) {
		request_reset(&buf, &entry);
		switch (mismatch) {
		case 0: request_payload.request_id++; break;
		case 1: request_payload.tag.blockNum++; break;
		case 2: request_source.reservation_token++; break;
		case 3: retire_start++; break;
		case 4: request_slot->first_fill.delivery_closed = true; break;
		default: request_payload.requester_backend_id++; break;
		}
		UT_ASSERT(!request_run()); UT_ASSERT_EQ(request_tries, 0);
		UT_ASSERT_EQ(request_prepares, 0); UT_ASSERT_EQ(request_cancels, 0);
	}
}

UT_TEST(test_actual_receiver_keeps_final_completion_behind_busy_before_waiter_runs)
{
	for (int local = 0; local < 2; local++) {
		BufferDesc buf; ClusterPcmOwnEntry entry;
		request_reset(&buf, &entry);
		if (local) {
			GcsBlockInvalidatePayload inv = retire_invalidate(&request_source);
			UT_ASSERT(gcs_block_wake_local_pending_s_request(&inv));
		} else
			request_receive(GCS_BLOCK_REPLY_DENIED_PENDING_X, 11, 2);
		UT_ASSERT(request_slot->reply_received);
		/* A rejected sender must not replace even a nonterminal result. */
		request_receive(GCS_BLOCK_REPLY_GRANTED, 77, 4);
		UT_ASSERT_EQ(request_slot->reply_header.status, GCS_BLOCK_REPLY_DENIED_PENDING_X);
		request_receive(GCS_BLOCK_REPLY_GRANTED, 22, 2);
		UT_ASSERT_EQ(request_slot->reply_header.status, GCS_BLOCK_REPLY_GRANTED);
		for (unsigned i = 0; i < GCS_BLOCK_DATA_SIZE; i++)
			UT_ASSERT_EQ((unsigned char)request_slot->reply_block_data[i], 22);
		request_receive(GCS_BLOCK_REPLY_DENIED_PENDING_X, 33, 2);
		request_receive(GCS_BLOCK_REPLY_GRANTED, 44, 2);
		UT_ASSERT_EQ(request_slot->reply_header.status, GCS_BLOCK_REPLY_GRANTED);
		UT_ASSERT_EQ((unsigned char)request_slot->reply_block_data[0], 22);
		UT_ASSERT_EQ(request_tries, 0); request_exact_native();
	}
	/* Legacy first-reply immutability remains in force for its old waiter. */
	{
		BufferDesc buf; ClusterPcmOwnEntry entry;
		request_reset(&buf, &entry); request_slot->first_fill.armed = false;
		request_receive(GCS_BLOCK_REPLY_DENIED_PENDING_X, 11, 2);
		request_receive(GCS_BLOCK_REPLY_GRANTED, 22, 2);
		UT_ASSERT_EQ(request_slot->reply_header.status, GCS_BLOCK_REPLY_DENIED_PENDING_X);
		UT_ASSERT_EQ((unsigned char)request_slot->reply_block_data[0], 11);
	}
}

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_final_outcomes_use_one_original_admission);
	UT_RUN(test_busy_replies_wait_on_original_episode_without_rearm);
	UT_RUN(test_timer_and_spurious_wakes_cannot_expire_service);
	UT_RUN(test_reply_before_cv_registration_is_not_lost);
	UT_RUN(test_full_local_ring_waits_with_identical_bytes_until_one_takeover);
	UT_RUN(test_cancel_before_admission_has_no_remote_receipt);
	UT_RUN(test_unavailable_before_admission_cannot_fabricate_handoff);
	UT_RUN(test_cancel_after_admission_keeps_exact_quiet_ack_responsibility);
	UT_RUN(test_proved_authority_changes_end_original_wait_without_resend);
	UT_RUN(test_unknown_session_retains_wait_and_wrong_owner_never_sends);
	UT_RUN(test_actual_receiver_keeps_final_completion_behind_busy_before_waiter_runs);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
