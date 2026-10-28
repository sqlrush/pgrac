/* Actual native capture + ownership object + requester ring/PCM handoff.
 * PGPROC, pin/provider, shmem-lock and OS-wakeup leaves are explicit fixtures.
 * Transport takeover here is neither authority, disk permission nor native C. */
int native_request_pcm_main(void);
#define main native_request_pcm_main
#define cluster_semantic_activation_recheck native_request_legacy_recheck
#include "test_cluster_pcm_lock.c"
#undef cluster_semantic_activation_recheck
#undef main

bool cluster_semantic_activation_recheck(const ClusterSemanticAdmissionToken *token);
bool cluster_shared_catalog = false;
static BufferDesc request_buf;
static ClusterPcmOwnEntry request_own;
static ClusterPcmOwnSnapshot request_base;
static ClusterSemanticAdmissionToken request_admission;
static PGPROC request_proc;
static ResourceXDecodedFrame request_prepared;
static uint64 request_token;
static int request_pins;
static unsigned request_rechecks;
static void (*request_hook)(void);

bool
cluster_semantic_activation_recheck(const ClusterSemanticAdmissionToken *token)
{
	UT_ASSERT((pg_atomic_read_u32(&request_buf.state) & BM_LOCKED) == 0);
	if (++request_rechecks == 2 && request_hook != NULL) request_hook();
	return native_request_legacy_recheck(token);
}

static int
request_private_pin(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, request_buf.buf_id + 1);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	UT_ASSERT((pg_atomic_read_u32(&request_buf.state) & BM_LOCKED) == 0);
	return request_pins;
}

static uint32
request_header_lock(BufferDesc *buf)
{
	uint32 state = pg_atomic_read_u32(&buf->state);
	UT_ASSERT((state & BM_LOCKED) == 0);
	pg_atomic_write_u32(&buf->state, state | BM_LOCKED);
	return state;
}

#include "test_cluster_pcm_snapshot_owner.inc"
#define GetPrivateRefCount(buffer) request_private_pin(buffer)
#define LockBufHdr request_header_lock
#include "test_cluster_native_acquire_intent.inc"
#undef LockBufHdr
#undef GetPrivateRefCount
#include "test_cluster_native_request_ring.inc"
#include "test_cluster_native_request_send.inc"

static ClusterLmsOutboundSharedState request_shared;
static LWLock request_ring_locks[CLUSTER_LMS_MAX_WORKERS];

static struct StopPcmEntryLayout *
request_entry(void)
{
	return hash_search((HTAB *)&fake_pcm_htab_token,
		&request_prepared.common.logical_assertion.resource, HASH_FIND, NULL);
}

static void
request_start_kind(unsigned profile, bool local, bool direct_init)
{
	ResourceXAssertion a;
	ResourceXAcquisitionRef terminal;
	BufferTag tag = make_tag(940);
	uint32 state;

	tag.relNumber = 16392;
	tag.dbOid = 5;
	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	fake_pcm_clock_us = 100;
	ut_master_session = 101;
	master_durable_admission = true;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &a));
	if (direct_init) {
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_exact(&a,
			local ? 1 : 0, 17, local ? 101 : 31, 77, 51, 61, 9000000, 8900000, 100,
			10000, 48, 49, false, 0, &request_prepared, &terminal),
			RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	} else {
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_exact(&a, local ? 1 : 0,
			17, local ? 101 : 31, 77, 51, 61, 9000000, 8900000, 100, 10000, false, 0,
			&request_prepared, &terminal), RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	}
	memset(&request_shared, 0, sizeof(request_shared));
	pg_atomic_init_u64(&request_shared.resource_x_transport_mutation_sequence, 1);
	cluster_lms_outbound_shared = &request_shared;
	cluster_lms_outbound_rings = request_shared.rings;
	for (unsigned i = 0; i < CLUSTER_LMS_MAX_WORKERS; i++)
		cluster_lms_outbound_locks[i] = &request_ring_locks[i];
	memset(&request_proc, 0, sizeof(request_proc));
	MyProc = &request_proc;
	request_proc.pid = MyProcPid;
	request_proc.backendId = MyBackendId = 4;
	request_proc.pgprocno = 3;
	request_proc.cluster_grd_generation = 71;
	request_pins = 1; request_rechecks = 0; request_hook = NULL;
	memset(&request_admission, 0, sizeof(request_admission));
	request_admission.entered = true;
	request_admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	request_admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1;
	request_admission.record_generation = 77;
	request_admission.formation_epoch = 17;
	memset(&request_buf, 0, sizeof(request_buf));
	memset(&request_own, 0, sizeof(request_own));
	ClusterPcmOwnArray = &request_own;
	request_buf.tag = tag;
	request_buf.pcm_state = profile < 3 ? PCM_STATE_N : profile == 3 ? PCM_STATE_S : PCM_STATE_X;
	request_buf.buffer_type = profile == 2 ? BUF_TYPE_PI : profile == 3 ? BUF_TYPE_SCUR
		: profile == 4 ? BUF_TYPE_XCUR : BUF_TYPE_CURRENT;
	pg_atomic_init_u32(&request_buf.state, BM_TAG_VALID | BUF_REFCOUNT_ONE
		| (profile == 0 ? BM_IO_IN_PROGRESS : BM_VALID));
	pg_atomic_init_u64(&request_own.generation, 48);
	pg_atomic_init_u64(&request_own.reservation_token, 48);
	state = request_header_lock(&request_buf);
	cluster_pcm_own_snapshot_locked(&request_buf, &request_base);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 48,
		PCM_OWN_FLAG_GRANT_PENDING, &request_token), CLUSTER_PCM_OWN_OK);
	UnlockBufHdr(&request_buf, state);
	fake_pcm_clock_us = 101;
}

static void
request_start(unsigned profile, bool local)
{
	request_start_kind(profile, local, false);
}

static ClusterLmsEnqueueResult
request_send(uint8 mode)
{
	return cluster_lms_outbound_enqueue_resource_x_acquire(
		request_entry()->resource_x_bootstrap_round.current_master_node, &request_prepared,
		&request_buf, &request_base, request_token, request_base.pcm_state == PCM_STATE_N
			&& (request_base.semantic_buf_state & BM_VALID) == 0
			? RESOURCE_X_ACQUIRE_FIRST_FILL : RESOURCE_X_ACQUIRE_RESIDENT,
		mode, &request_admission);
}

static void
request_native_unchanged(const ClusterPcmOwnEntry *own, uint32 state)
{
	UT_ASSERT(memcmp(own, &request_own, sizeof(*own)) == 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state), state);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
}

static void
request_refused(ClusterLmsEnqueueResult wanted)
{
	ClusterPcmResourceXBootstrapRound before = request_entry()->resource_x_bootstrap_round;
	ClusterPcmOwnEntry own = request_own;
	ResourceXDecodedFrame prepared = request_prepared;
	uint32 state = pg_atomic_read_u32(&request_buf.state);
	UT_ASSERT_EQ(request_send(PCM_STATE_S), wanted);
	UT_ASSERT_EQ(request_shared.rings[1].count, 0);
	UT_ASSERT(memcmp(&before, &request_entry()->resource_x_bootstrap_round, sizeof(before)) == 0);
	UT_ASSERT(memcmp(&prepared, &request_prepared, sizeof(prepared)) == 0);
	request_native_unchanged(&own, state);
}

static ResourceXIntentSlot
request_ticket(ResourceXDecodedFrame *decoded)
{
	ClusterLmsOutboundSlot *slot = &request_shared.rings[1].ring[0];
	ResourceXIntentSlot ticket;
	ResourceXWireReject reject;
	memcpy(&ticket, slot->payload, sizeof(ticket));
	UT_ASSERT_EQ(slot->kind, CLUSTER_LMS_OUTBOUND_RESOURCE_X_REQUESTER);
	UT_ASSERT_EQ(slot->payload_len, sizeof(ticket) + RESOURCE_X_ACQUIRE_V3_BYTES);
	UT_ASSERT_EQ(slot->msg_type, RESOURCE_X_MSG_ASSERT_X);
	UT_ASSERT_EQ(slot->required_capability, PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1);
	UT_ASSERT(cluster_resource_x_wire_decode(slot->msg_type, slot->payload + sizeof(ticket),
		RESOURCE_X_ACQUIRE_V3_BYTES, decoded, &reject));
	UT_ASSERT(cluster_pcm_lock_resource_x_requester_send_current_exact(&ticket));
	return ticket;
}

UT_TEST(test_native_request_real_ring_retains_first_fill_and_resident_s_x)
{
	for (unsigned profile = 0; profile < 5; profile++)
		for (uint8 mode = PCM_STATE_S; mode <= PCM_STATE_X; mode++) {
			ResourceXDecodedFrame decoded;
			ResourceXIntentSlot ticket;
			ClusterPcmOwnEntry before;
			uint32 state;
			request_start(profile, false); before = request_own;
			state = pg_atomic_read_u32(&request_buf.state);
			UT_ASSERT_EQ(request_send(mode), CLUSTER_LMS_ENQUEUE_ADMITTED);
			ticket = request_ticket(&decoded);
			UT_ASSERT(decoded.has_acquire_intent && !decoded.has_acquire_decision);
			UT_ASSERT_EQ(decoded.common.observed_mode, request_base.pcm_state);
			UT_ASSERT_EQ(decoded.common.target_mode, mode);
			UT_ASSERT_EQ(decoded.body.acquire_intent.backend_id, MyBackendId);
			UT_ASSERT_EQ(decoded.body.acquire_intent.backend_generation, 71);
			UT_ASSERT_EQ(decoded.body.acquire_intent.requester_incarnation, 101);
			UT_ASSERT_EQ(decoded.body.acquire_intent.base_generation, 48);
			UT_ASSERT_EQ(decoded.body.acquire_intent.reservation_token, 49);
			UT_ASSERT_EQ(decoded.body.acquire_intent.buffer_id, 0);
			UT_ASSERT_EQ(decoded.body.acquire_intent.r4_generation, 77);
			UT_ASSERT_EQ(decoded.body.acquire_intent.cluster_epoch, 17);
			UT_ASSERT_EQ(decoded.body.acquire_intent.path, profile == 0
				? RESOURCE_X_ACQUIRE_FIRST_FILL : RESOURCE_X_ACQUIRE_RESIDENT);
			UT_ASSERT_EQ(ticket.send_episode, 1); UT_ASSERT_EQ(ticket.logical_generation, 1);
			UT_ASSERT_EQ(request_shared.rings[1].count, 1);
			request_native_unchanged(&before, state);
		}
}

UT_TEST(test_native_request_local_master_uses_the_same_physical_owner)
{
	ResourceXDecodedFrame decoded;
	ResourceXIntentSlot ticket;
	request_start(0, true);
	UT_ASSERT_EQ(request_send(PCM_STATE_S), CLUSTER_LMS_ENQUEUE_ADMITTED);
	ticket = request_ticket(&decoded);
	UT_ASSERT_EQ(ticket.destination_node, cluster_node_id);
	UT_ASSERT_EQ(decoded.common.master_session_incarnation, 101);
	UT_ASSERT_EQ(decoded.common.assertion_sequence, request_prepared.common.assertion_sequence);
}

UT_TEST(test_native_request_preserves_prepared_input_and_has_no_grant_or_native_completion)
{
	ResourceXDecodedFrame decoded, before, grant, image;
	ClusterPcmResourceXBootstrapRound *round;
	ResourceXRequesterJoinSnapshot join;
	request_start(0, false); before = request_prepared;
	UT_ASSERT_EQ(request_send(PCM_STATE_X), CLUSTER_LMS_ENQUEUE_ADMITTED);
	(void)request_ticket(&decoded);
	round = &request_entry()->resource_x_bootstrap_round;
	UT_ASSERT(memcmp(&before, &request_prepared, sizeof(before)) == 0);
	UT_ASSERT_EQ(round->accepted_base, 0); UT_ASSERT_EQ(round->terminal_authority_generation, 0);
	UT_ASSERT_EQ(round->cached_ownership_generation, 0);
	UT_ASSERT_EQ(round->phase, 1); /* original REQUEST_DISPATCHED */
	UT_ASSERT_EQ(round->send.pending, 1); UT_ASSERT_EQ(round->send.sent, 0);
	UT_ASSERT_EQ(decoded.common.base_authority_generation, 0);
	UT_ASSERT_EQ(decoded.common.authority_generation, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_frames_exact(
		&request_prepared.common.logical_assertion, &grant, &image, &join), RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(join.flags, 0);
}

UT_TEST(test_native_request_full_ring_preserves_unbound_episode_then_uses_same_attempt)
{
	ResourceXDecodedFrame decoded;
	ClusterPcmResourceXBootstrapRound before;
	request_start(0, false); before = request_entry()->resource_x_bootstrap_round;
	request_shared.rings[1].count = PGRAC_LMS_OUTBOUND_CAPACITY;
	UT_ASSERT_EQ(request_send(PCM_STATE_S), CLUSTER_LMS_ENQUEUE_FULL);
	UT_ASSERT(memcmp(&before, &request_entry()->resource_x_bootstrap_round, sizeof(before)) == 0);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), request_token);
	request_shared.rings[1].count = 0; /* explicit environmental consumer release */
	UT_ASSERT_EQ(request_send(PCM_STATE_S), CLUSTER_LMS_ENQUEUE_ADMITTED);
	(void)request_ticket(&decoded);
	UT_ASSERT_EQ(decoded.common.assertion_sequence, before.request.assertion_sequence);
}

UT_TEST(test_native_request_draining_capacity_is_not_available_capacity)
{
	request_start(0, false);
	request_shared.rings[1].draining = PGRAC_LMS_OUTBOUND_CAPACITY;
	request_refused(CLUSTER_LMS_ENQUEUE_FULL);
}

UT_TEST(test_native_request_second_enqueue_never_creates_another_physical_handoff)
{
	request_start(0, false);
	UT_ASSERT_EQ(request_send(PCM_STATE_S), CLUSTER_LMS_ENQUEUE_ADMITTED);
	UT_ASSERT_EQ(request_send(PCM_STATE_S), CLUSTER_LMS_ENQUEUE_NOT_DUE);
	UT_ASSERT_EQ(request_shared.rings[1].count, 1);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.episode, 1);
}

UT_TEST(test_native_request_positive_and_negative_callbacks_cannot_rearm_e)
{
	for (unsigned sent = 0; sent < 2; sent++) {
		ResourceXDecodedFrame decoded;
		ResourceXIntentSlot ticket;
		request_start(0, false);
		UT_ASSERT_EQ(request_send(PCM_STATE_S), CLUSTER_LMS_ENQUEUE_ADMITTED);
		ticket = request_ticket(&decoded);
		fake_pcm_clock_us = 102;
		cluster_lms_outbound_resource_x_send_complete(&ticket, sent != 0);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.pending, 0);
		fake_pcm_clock_us = 10000001;
		UT_ASSERT_EQ(request_send(PCM_STATE_S), CLUSTER_LMS_ENQUEUE_NOT_DUE);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.episode, 1);
		UT_ASSERT(!cluster_pcm_lock_resource_x_requester_send_complete_exact(&ticket, true, 10000002));
		UT_ASSERT_EQ(request_shared.rings[1].count, 1);
	}
}

UT_TEST(test_native_request_misbound_callback_preserves_original_send_responsibility)
{
	ResourceXDecodedFrame decoded;
	ResourceXIntentSlot ticket;
	request_start(0, false);
	UT_ASSERT_EQ(request_send(PCM_STATE_S), CLUSTER_LMS_ENQUEUE_ADMITTED);
	ticket = request_ticket(&decoded); ticket.send_episode++;
	cluster_lms_outbound_resource_x_send_complete(&ticket, true);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.pending, 1);
	UT_ASSERT_EQ(request_send(PCM_STATE_S), CLUSTER_LMS_ENQUEUE_NOT_DUE);
}

UT_TEST(test_native_request_native_tag_generation_and_token_must_match)
{
	for (unsigned field = 0; field < 4; field++) {
		request_start(0, false);
		if (field == 0) request_base.generation++;
		if (field == 1) request_token++;
		if (field == 2) request_base.tag.blockNum++;
		if (field == 3) request_buf.tag.blockNum++;
		request_refused(CLUSTER_LMS_ENQUEUE_INVALID);
	}
}

UT_TEST(test_native_request_busy_owners_remain_owned_without_a_request)
{
	for (unsigned field = 0; field < 4; field++) {
		request_start(0, false);
		if (field == 0) pg_atomic_write_u64(&request_own.delivery_attempt, 5);
		if (field == 1) pg_atomic_write_u64(&request_own.writer_activation_token, request_token);
		if (field == 2) pg_atomic_write_u64(&request_own.write_io_generation, 1);
		if (field == 3) pg_atomic_write_u32(&request_own.flags, PCM_OWN_FLAG_REVOKING);
		request_refused(CLUSTER_LMS_ENQUEUE_UNAVAILABLE);
	}
}

UT_TEST(test_native_request_requires_original_backend_and_pin)
{
	for (unsigned field = 0; field < 4; field++) {
		request_start(0, false);
		if (field == 0) MyProc = NULL;
		if (field == 1) request_proc.backendId++;
		if (field == 2) request_proc.cluster_grd_generation = 0;
		if (field == 3) request_pins = 0;
		request_refused(CLUSTER_LMS_ENQUEUE_INVALID);
	}
}

UT_TEST(test_native_request_source_side_or_closed_admission_cannot_send)
{
	for (unsigned field = 0; field < 3; field++) {
		request_start(0, false);
		if (field == 0) request_admission.side = CLUSTER_SEMANTIC_SOURCE_SIDE;
		if (field == 1) request_admission.entered = false;
		if (field == 2) master_durable_admission = false;
		request_refused(CLUSTER_LMS_ENQUEUE_UNAVAILABLE);
	}
}

static void request_change_domain(void) { request_admission.record_generation++; }
static void request_provider_error(void) { pg_re_throw(); }

UT_TEST(test_native_request_domain_turnover_between_samples_cannot_bind)
{
	request_start(0, false); request_hook = request_change_domain;
	request_refused(CLUSTER_LMS_ENQUEUE_UNAVAILABLE);
}

UT_TEST(test_native_request_provider_error_preserves_both_original_owners)
{
	ClusterPcmResourceXBootstrapRound before;
	volatile bool caught = false;
	request_start(0, false); request_hook = request_provider_error;
	before = request_entry()->resource_x_bootstrap_round;
	PG_TRY(); { (void)request_send(PCM_STATE_S); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT_EQ(fake_lwlock_depth, 0);
	UT_ASSERT_EQ(request_shared.rings[1].count, 0);
	UT_ASSERT(memcmp(&before, &request_entry()->resource_x_bootstrap_round, sizeof(before)) == 0);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), request_token);
	UT_ASSERT((pg_atomic_read_u32(&request_buf.state) & BM_LOCKED) == 0);
}

UT_TEST(test_native_request_prepared_legacy_proof_authority_and_flags_are_rejected)
{
	for (unsigned field = 0; field < 8; field++) {
		request_start(0, false);
		if (field == 0) request_prepared.kind = RESOURCE_X_WIRE_ASSERT_X;
		if (field == 1) request_prepared.has_acquire_intent = true;
		if (field == 2) request_prepared.has_acquire_decision = true;
		if (field == 3) request_prepared.blocked_has_remote_proof = true;
		if (field == 4) request_prepared.common.flags = RESOURCE_X_COMMON_FLAG_PI_ESTABLISHED;
		if (field == 5) request_prepared.common.authority_generation = 1;
		if (field == 6) request_prepared.common.base_authority_generation = 1;
		if (field == 7) request_prepared.payload_bytes--;
		request_refused(CLUSTER_LMS_ENQUEUE_INVALID);
	}
}

UT_TEST(test_native_request_original_round_full_key_remains_authoritative)
{
	for (unsigned field = 0; field < 4; field++) {
		request_start(0, false);
		if (field == 0) request_prepared.common.master_session_incarnation++;
		if (field == 1) request_prepared.common.assertion_sequence++;
		if (field == 2) request_prepared.common.sender_connection_generation++;
		if (field == 3) request_prepared.common.resource_formation++;
		request_refused(CLUSTER_LMS_ENQUEUE_INVALID);
	}
}

UT_TEST(test_native_request_old_physical_owner_and_closed_gate_are_not_reinterpreted)
{
	ResourceXIntentSlot old;
	ResourceXGateSnapshot gate;
	request_start(0, false);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_send_begin_exact(
		&request_prepared, 0, 1, fake_pcm_clock_us, &old), RESOURCE_X_INTENT_STAGED);
	request_refused(CLUSTER_LMS_ENQUEUE_INVALID);
	UT_ASSERT(cluster_pcm_lock_resource_x_requester_send_current_exact(&old));
	request_start(0, false);
	UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&gate));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_fail_closed_exact(&gate), RESOURCE_X_APPLY_APPLIED);
	request_refused(CLUSTER_LMS_ENQUEUE_INVALID);
}

UT_TEST(test_native_request_transport_exhaustion_refuses_without_wrap_or_lost_owner)
{
	request_start(0, false);
	pg_atomic_write_u64(&request_shared.resource_x_transport_mutation_sequence, UINT64_MAX);
	request_refused(CLUSTER_LMS_ENQUEUE_UNAVAILABLE);
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_shared.resource_x_transport_mutation_sequence), UINT64_MAX);
}

UT_TEST(test_native_request_invalid_arguments_cannot_allocate_or_bind)
{
	for (unsigned field = 0; field < 9; field++) {
		ClusterPcmResourceXBootstrapRound before;
		request_start(0, false); before = request_entry()->resource_x_bootstrap_round;
		UT_ASSERT_EQ(cluster_lms_outbound_enqueue_resource_x_acquire(
			field == 0 ? -1 : field == 1 ? RESOURCE_X_PROTOCOL_NODE_LIMIT : 0,
			field == 2 ? NULL : &request_prepared, field == 3 ? NULL : &request_buf,
			field == 4 ? NULL : &request_base, request_token,
			field == 5 ? 0 : RESOURCE_X_ACQUIRE_FIRST_FILL,
			field == 6 ? PCM_STATE_N : field == 7 ? UINT8_MAX : PCM_STATE_S,
			field == 8 ? NULL : &request_admission), CLUSTER_LMS_ENQUEUE_INVALID);
		UT_ASSERT_EQ(request_shared.rings[1].count, 0);
		UT_ASSERT(memcmp(&before, &request_entry()->resource_x_bootstrap_round, sizeof(before)) == 0);
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
	}
}

UT_TEST(test_native_request_direct_init_round_cannot_become_an_ordinary_request)
{
	request_start_kind(0, false, true);
	request_refused(CLUSTER_LMS_ENQUEUE_INVALID);
}

int
main(void)
{
	UT_PLAN(20);
	UT_RUN(test_native_request_real_ring_retains_first_fill_and_resident_s_x);
	UT_RUN(test_native_request_local_master_uses_the_same_physical_owner);
	UT_RUN(test_native_request_preserves_prepared_input_and_has_no_grant_or_native_completion);
	UT_RUN(test_native_request_full_ring_preserves_unbound_episode_then_uses_same_attempt);
	UT_RUN(test_native_request_draining_capacity_is_not_available_capacity);
	UT_RUN(test_native_request_second_enqueue_never_creates_another_physical_handoff);
	UT_RUN(test_native_request_positive_and_negative_callbacks_cannot_rearm_e);
	UT_RUN(test_native_request_misbound_callback_preserves_original_send_responsibility);
	UT_RUN(test_native_request_native_tag_generation_and_token_must_match);
	UT_RUN(test_native_request_busy_owners_remain_owned_without_a_request);
	UT_RUN(test_native_request_requires_original_backend_and_pin);
	UT_RUN(test_native_request_source_side_or_closed_admission_cannot_send);
	UT_RUN(test_native_request_domain_turnover_between_samples_cannot_bind);
	UT_RUN(test_native_request_provider_error_preserves_both_original_owners);
	UT_RUN(test_native_request_prepared_legacy_proof_authority_and_flags_are_rejected);
	UT_RUN(test_native_request_original_round_full_key_remains_authoritative);
	UT_RUN(test_native_request_old_physical_owner_and_closed_gate_are_not_reinterpreted);
	UT_RUN(test_native_request_transport_exhaustion_refuses_without_wrap_or_lost_owner);
	UT_RUN(test_native_request_invalid_arguments_cannot_allocate_or_bind);
	UT_RUN(test_native_request_direct_init_round_cannot_become_an_ordinary_request);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
