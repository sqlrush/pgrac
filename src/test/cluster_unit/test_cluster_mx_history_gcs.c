/* Execute original ingress, context, drain and cancellation. The runtime
 * leaf deliberately refuses proof; it never fabricates TT40 or a grant. */
#define HISTORY_BACKEND_TEST_MAIN history_backend_fixture_main
int history_backend_fixture_main(void);
#define cluster_semantic_activation_resolve_shared_undo_root history_fixture_root
#define cluster_undo_block0_current_acquire_begin_admitted history_fixture_acquire
#define cluster_ic_send_envelope history_fixture_send
#define cluster_ic_dispatch_defer history_fixture_defer
#define cluster_lms_shard_for_tag history_fixture_shard
#define cluster_ic_tier1_resource_x_stream_generation history_fixture_stream
#include "test_cluster_mx_history_backend.c"
#undef cluster_semantic_activation_resolve_shared_undo_root
#undef cluster_undo_block0_current_acquire_begin_admitted
#undef cluster_ic_send_envelope
#undef cluster_ic_dispatch_defer
#undef cluster_lms_shard_for_tag
#undef cluster_ic_tier1_resource_x_stream_generation

static int history_stream_worker;
int cluster_lms_shard_for_tag(const BufferTag *, int);
uint64 cluster_ic_tier1_resource_x_stream_generation(int32, int);
int
cluster_lms_shard_for_tag(const BufferTag *tag pg_attribute_unused(), int workers)
{
	return workers > 1 ? 3 : 0;
}
uint64
cluster_ic_tier1_resource_x_stream_generation(int32 peer pg_attribute_unused(), int worker)
{
	history_stream_worker = worker;
	return route_history_stream_generation;
}

static GcsBlockReplyHeader history_sent_header;
static bool history_root_error;
static int history_deferred;
void cluster_ic_dispatch_defer(const ClusterICEnvelope *env);
void
cluster_ic_dispatch_defer(const ClusterICEnvelope *env)
{
	UT_ASSERT_EQ(env->payload_length, 424);
	history_deferred++;
}
extern bool cluster_gcs_block_test_history_canonical_matches(const ClusterTxResolution *,
															 const ClusterTTSlotPhysicalLocator *,
															 const ClusterUndoBlock0Generation *);
bool cluster_semantic_activation_resolve_shared_undo_root(const ClusterSemanticAdmissionToken *,
														  ClusterUndoPathIntent, uint32, uint32,
														  ClusterUndoBlock0ResolvedRoot *);
ClusterUndoBlock0CurrentStep cluster_undo_block0_current_acquire_begin_admitted(
	const ClusterUndoBlock0LogicalKey *, ClusterUndoBlock0CurrentMode, int,
	const ClusterSemanticAdmissionToken *, ClusterUndoBlock0CurrentGuard *,
	ClusterUndoBlock0Result *);
ClusterICSendResult cluster_ic_send_envelope(uint8, int32, const void *, uint32);

bool
cluster_semantic_activation_resolve_shared_undo_root(const ClusterSemanticAdmissionToken *a,
													 ClusterUndoPathIntent intent, uint32 owner,
													 uint32 segment,
													 ClusterUndoBlock0ResolvedRoot *out)
{
	if (history_root_error)
		siglongjmp(*PG_exception_stack, 1);
	return history_fixture_root(a, intent, owner, segment == 257 ? 5 : segment, out);
}
ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_begin_admitted(const ClusterUndoBlock0LogicalKey *key,
												   ClusterUndoBlock0CurrentMode mode, int timeout,
												   const ClusterSemanticAdmissionToken *a,
												   ClusterUndoBlock0CurrentGuard *guard,
												   ClusterUndoBlock0Result *failure)
{
	ClusterUndoBlock0LogicalKey mapped = *key;
	if (mapped.segment_id == 257)
		mapped.segment_id = 5;
	return history_fixture_acquire(&mapped, mode, timeout, a, guard, failure);
}
ClusterICSendResult
cluster_ic_send_envelope(uint8 type, int32 dest, const void *payload, uint32 length)
{
	if (type == PGRAC_IC_MSG_GCS_BLOCK_REPLY && length >= sizeof(history_sent_header))
		memcpy(&history_sent_header, payload, sizeof(history_sent_header));
	return history_fixture_send(type, dest, payload, length);
}
static ClusterICEnvelope history_env;
static ClusterMxHistoryRequestWire history_origin_request;
static void
history_origin_prepare(void)
{
	ClusterMxHistoryRequest r = history_input();
	r.forward.request_id = 51;
	r.requester_capability_generation = 42;
	r.origin_capability_generation = 12;
	route_seam_reset();
	cluster_node_id = 1;
	route_seam.admission_result = CLUSTER_SEMANTIC_ADMISSION_OK;
	UT_ASSERT(cluster_mx_history_request_encode(&r, &history_origin_request));
	history_env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, 2, 1, 424);
	memset(&history_sent_header, 0, sizeof(history_sent_header));
}
static void
history_origin_drain(void)
{
	for (int i = 0; i < 5; i++)
		cluster_gcs_block_test_r4_tx_origin_drain();
}
UT_TEST(history_origin_accepts_exact_once_and_refuses_unproven_data)
{
	history_origin_prepare();
	cluster_gcs_handle_block_forward_envelope(&history_env, &history_origin_request);
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 1);
	cluster_gcs_handle_block_forward_envelope(&history_env, &history_origin_request);
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	history_origin_drain();
	UT_ASSERT_EQ(route_seam.candidate_acquire_begin_calls, 1);
	UT_ASSERT_EQ(route_seam.candidate_release_begin_calls, 1);
	UT_ASSERT_EQ(route_seam.raw_send_payload_len, 48);
	UT_ASSERT_EQ(history_sent_header.status, GCS_BLOCK_REPLY_R4_DENIED);
	UT_ASSERT_EQ(history_sent_header.requester_backend_id, 1);
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
}
UT_TEST(history_origin_cancel_before_acquire_is_idempotent)
{
	ClusterMxHistoryRequestWire cancel;
	history_origin_prepare();
	cluster_gcs_handle_block_forward_envelope(&history_env, &history_origin_request);
	UT_ASSERT(cluster_mx_history_request_cancel(&history_origin_request, &cancel));
	cluster_gcs_handle_block_forward_envelope(&history_env, &cancel);
	history_origin_drain();
	UT_ASSERT_EQ(route_seam.candidate_acquire_begin_calls, 0);
	UT_ASSERT_EQ(route_seam.raw_send_calls, 0);
	cluster_gcs_handle_block_forward_envelope(&history_env, &cancel);
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
}
UT_TEST(history_origin_cancel_waits_for_pending_release)
{
	ClusterMxHistoryRequestWire cancel;
	history_origin_prepare();
	mx_origin_sample_enabled = true;
	mx_origin_release_pending = true;
	cluster_gcs_handle_block_forward_envelope(&history_env, &history_origin_request);
	history_origin_drain();
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 1);
	UT_ASSERT_EQ(mx_origin_guard_held, 1);
	UT_ASSERT(cluster_mx_history_request_cancel(&history_origin_request, &cancel));
	cluster_gcs_handle_block_forward_envelope(&history_env, &cancel);
	history_origin_drain();
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 1);
	UT_ASSERT_EQ(route_seam.candidate_cancel_calls, 0);
	UT_ASSERT_EQ(route_seam.candidate_release_begin_calls, 1);
	mx_origin_release_pending = false;
	history_origin_drain();
	UT_ASSERT_EQ(mx_origin_guard_held, 0);
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
	UT_ASSERT_EQ(route_seam.raw_send_calls, 0);
	mx_origin_sample_enabled = false;
}
UT_TEST(history_origin_changed_body_never_reuses_staged_work)
{
	history_origin_prepare();
	cluster_gcs_handle_block_forward_envelope(&history_env, &history_origin_request);
	history_origin_request.bytes[104] ^= 1; /* same authenticated key, bad CRC */
	cluster_gcs_handle_block_forward_envelope(&history_env, &history_origin_request);
	history_origin_drain();
	UT_ASSERT_EQ(route_seam.candidate_acquire_begin_calls, 0);
	UT_ASSERT_EQ(route_seam.raw_send_payload_len, 48);
	UT_ASSERT_EQ(history_sent_header.status, GCS_BLOCK_REPLY_R4_DENIED);
}
UT_TEST(history_origin_backpressure_preserves_context_then_forgets)
{
	history_origin_prepare();
	route_seam.raw_send_result = CLUSTER_IC_SEND_NOT_ADMITTED;
	cluster_gcs_handle_block_forward_envelope(&history_env, &history_origin_request);
	history_origin_drain();
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 1);
	UT_ASSERT_EQ(route_seam.candidate_acquire_begin_calls, 1);
	route_seam.raw_send_result = CLUSTER_IC_SEND_DONE;
	history_origin_drain();
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
	UT_ASSERT_EQ(route_seam.candidate_acquire_begin_calls, 1);
}
UT_TEST(history_origin_bad_crc_never_enters_admission)
{
	history_origin_prepare();
	history_origin_request.bytes[416] ^= 1;
	cluster_gcs_handle_block_forward_envelope(&history_env, &history_origin_request);
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
	UT_ASSERT_EQ(route_seam.enter_calls, 0);
}
UT_TEST(history_origin_capacity_defers_to_original_receive_owner)
{
	ClusterMxHistoryRequest r;
	ClusterMxHistoryRequestWire requests[129], cancel;
	history_origin_prepare();
	history_deferred = 0;
	UT_ASSERT(cluster_mx_history_request_decode(&history_origin_request, 424, 2, 1, 9, &r));
	for (int i = 0; i < 129; i++) {
		r.forward.request_id = 100 + i;
		UT_ASSERT(cluster_mx_history_request_encode(&r, &requests[i]));
		cluster_gcs_handle_block_forward_envelope(&history_env, &requests[i]);
	}
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 128);
	UT_ASSERT_EQ(history_deferred, 1);
	UT_ASSERT_EQ(route_seam.raw_send_calls, 0);
	UT_ASSERT_EQ(route_seam.enter_calls, 128);
	for (int i = 0; i < 128; i++) {
		UT_ASSERT(cluster_mx_history_request_cancel(&requests[i], &cancel));
		cluster_gcs_handle_block_forward_envelope(&history_env, &cancel);
	}
	history_origin_drain();
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
	cluster_gcs_handle_block_forward_envelope(&history_env, &requests[128]);
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 1);
	UT_ASSERT(cluster_mx_history_request_cancel(&requests[128], &cancel));
	cluster_gcs_handle_block_forward_envelope(&history_env, &cancel);
	history_origin_drain();
	UT_ASSERT_EQ(route_seam.candidate_acquire_begin_calls, 0);
}
UT_TEST(history_origin_canonical_generation_is_not_rollover_count)
{
	ClusterTxResolution top = { 0 };
	ClusterTTSlotPhysicalLocator canonical
		= { .segment_id = 258, .xid = 100, .slot_offset = 3, .wrap = 2 };
	ClusterUndoBlock0Generation generation = { true, 23 };
	top.local_canonical_valid = true;
	top.local_canonical = canonical;
	top.local_canonical_generation = 23;
	top.authority.tt_generation = 0; /* unrelated rollover observation */
	UT_ASSERT(cluster_gcs_block_test_history_canonical_matches(&top, &canonical, &generation));
	top.authority.tt_generation = 23;
	top.local_canonical_generation = 24;
	UT_ASSERT(!cluster_gcs_block_test_history_canonical_matches(&top, &canonical, &generation));
	top.local_canonical_generation = 23;
	canonical.wrap++;
	UT_ASSERT(!cluster_gcs_block_test_history_canonical_matches(&top, &canonical, &generation));
	canonical.wrap--;
	generation.known = false;
	UT_ASSERT(!cluster_gcs_block_test_history_canonical_matches(&top, &canonical, &generation));
}
UT_TEST(history_requester_uses_original_origin_data0)
{
	ClusterMxHistoryRequestWire request;
	int workers = cluster_lms_workers;
	cluster_lms_workers = 4;
	request = history_reset_prepare();
	UT_ASSERT(cluster_gcs_block_mx_history_submit(&request));
	UT_ASSERT_EQ(history_sent_worker, 0);
	UT_ASSERT_EQ(history_stream_worker, 0);
	cluster_gcs_block_mx_history_cancel(&request);
	UT_ASSERT_EQ(history_sent_worker, 0);
	cluster_lms_workers = workers;
}
UT_TEST(history_origin_deadline_releases_before_retry)
{
	for (int failed_release = 0; failed_release < 2; failed_release++) {
		history_origin_prepare();
		mx_origin_sample_enabled = true;
		mx_origin_release_pending = true;
		cluster_gcs_handle_block_forward_envelope(&history_env, &history_origin_request);
		history_origin_drain();
		route_test_now += (TimestampTz)cluster_gcs_reply_timeout_ms * 1000 + 1;
		history_origin_drain();
		UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 1);
		UT_ASSERT_EQ(mx_origin_guard_held, 1);
		UT_ASSERT_EQ(route_seam.raw_send_calls, 0);
		mx_origin_release_failed = failed_release != 0;
		mx_origin_release_pending = false;
		history_origin_drain();
		UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
		UT_ASSERT_EQ(route_seam.candidate_cancel_calls, 0);
		UT_ASSERT_EQ(history_sent_header.status, GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED);
		UT_ASSERT_EQ(route_seam.raw_send_payload_len, 48);
		mx_origin_sample_enabled = false;
		mx_origin_release_failed = false;
	}
}
UT_TEST(history_origin_admission_drift_waits_for_release)
{
	history_origin_prepare();
	mx_origin_sample_enabled = true;
	mx_origin_release_pending = true;
	cluster_gcs_handle_block_forward_envelope(&history_env, &history_origin_request);
	history_origin_drain();
	route_seam.recheck_ok = false;
	history_origin_drain();
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 1);
	UT_ASSERT_EQ(mx_origin_guard_held, 1);
	mx_origin_release_pending = false;
	history_origin_drain();
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
	UT_ASSERT_EQ(mx_origin_guard_held, 0);
	UT_ASSERT_EQ(route_seam.candidate_cancel_calls, 0);
	UT_ASSERT_EQ(route_seam.raw_send_calls, 0);
	mx_origin_sample_enabled = false;
}
UT_TEST(history_origin_same_numeric_key_keeps_legacy_domain)
{
	ClusterR4InternalForwardPayload legacy;
	ClusterICEnvelope env;
	ClusterMxHistoryRequestWire cancel;
	history_origin_prepare();
	cluster_gcs_handle_block_forward_envelope(&history_env, &history_origin_request);
	legacy = route_test_tx_forward(9);
	legacy.base.request_id = 51;
	legacy.base.requester_backend_id = 1;
	env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, 2, 1, sizeof(legacy));
	cluster_gcs_handle_block_forward_envelope(&env, &legacy);
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 2);
	UT_ASSERT_EQ(route_seam.enter_calls, 2);
	UT_ASSERT(cluster_mx_history_request_cancel(&history_origin_request, &cancel));
	cluster_gcs_handle_block_forward_envelope(&history_env, &cancel);
	history_origin_drain();
	UT_ASSERT_EQ(route_seam.candidate_acquire_begin_calls, 1);
	UT_ASSERT_EQ(route_seam.raw_send_payload_len, GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE);
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
}
UT_TEST(history_origin_ingress_error_releases_untransferred_admission)
{
	volatile bool caught = false;
	history_origin_prepare();
	history_root_error = true;
	PG_TRY();
	{
		cluster_gcs_handle_block_forward_envelope(&history_env, &history_origin_request);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	history_root_error = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
}
int
main(void)
{
	UT_PLAN(13);
	UT_RUN(history_origin_accepts_exact_once_and_refuses_unproven_data);
	UT_RUN(history_origin_cancel_before_acquire_is_idempotent);
	UT_RUN(history_origin_cancel_waits_for_pending_release);
	UT_RUN(history_origin_changed_body_never_reuses_staged_work);
	UT_RUN(history_origin_backpressure_preserves_context_then_forgets);
	UT_RUN(history_origin_bad_crc_never_enters_admission);
	UT_RUN(history_origin_capacity_defers_to_original_receive_owner);
	UT_RUN(history_origin_canonical_generation_is_not_rollover_count);
	UT_RUN(history_requester_uses_original_origin_data0);
	UT_RUN(history_origin_deadline_releases_before_retry);
	UT_RUN(history_origin_admission_drift_waits_for_release);
	UT_RUN(history_origin_same_numeric_key_keeps_legacy_domain);
	UT_RUN(history_origin_ingress_error_releases_untransferred_admission);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
