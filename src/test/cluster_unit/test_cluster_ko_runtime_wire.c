/*-------------------------------------------------------------------------
 * test_cluster_ko_runtime_wire.c
 *    Original KO transport owners keep committed-discard wire identity.
 *
 * Spec: 2026-10-16-pre2-p014-am05-shared-ddl-autocommit.md
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * The handlers, queues and owner transitions are production code. CONTROL,
 * GES, transport and physical operations retain explicit fixture boundaries.
 *-------------------------------------------------------------------------
 */
int ko_stop_fixture_main(void);
#define main ko_stop_fixture_main
#include "test_cluster_ko_stop.c"
#undef main

static uint32
committed_capabilities(void)
{
	return (native_real_capability_word() | PGRAC_IC_HELLO_CAP_KO_COMMITTED_DISCARD_V1)
		& ~PGRAC_IC_HELLO_CAP_KO_OBJECT_DRAIN_V3;
}

static ClusterKoSharedMessageV4
committed_peer(uint64 batch)
{
	ClusterKoSharedMessageV4 message;

	cluster_interconnect_tier = CLUSTER_IC_TIER_1;
	message.object = peer_fence_fixture(batch);

	native_capability_hook = committed_capabilities;
	return message;
}

static void
receive_committed(const ClusterKoSharedMessageV4 *message, unsigned corrupt)
{
	const ClusterKoSharedMessageV3 *m = &message->object;
	ClusterICEnvelope env = { .source_node_id = m->base.origin_node,
		.payload_length = CLUSTER_KO_SHARED_V4_BYTES, .epoch = m->base.epoch };
	uint8 wire[CLUSTER_KO_SHARED_V4_BYTES];
	BackendType saved = MyBackendType;
	AuxProcType saved_aux = MyAuxProcType;

	UT_ASSERT(cluster_ko_shared_encode_v4(message, wire, sizeof(wire)));
	if (corrupt != 0) wire[corrupt - 1] ^= 1;
	v3_ingress = (ClusterICControlIngress){ .serial = 77, .epoch = m->base.epoch,
		.owner_pid = MyProcPid, .source_node = m->base.origin_node,
		.carrier = CLUSTER_IC_CONTROL_TCP };
	MyBackendType = B_LMON;
	MyAuxProcType = LmonProcess;
	v3_ingress_env = &env;
	cluster_ko_flush_request_handler(&env, wire);
	v3_ingress_env = NULL;
	MyBackendType = saved;
	MyAuxProcType = saved_aux;
}

UT_TEST(test_committed_request_registers_exact_version_without_legacy_physical_work)
{
	ClusterKoSharedMessageV4 request = committed_peer(1001);
	ClusterKoPeerFenceHandleV3 old_handle = { .slot = 0, .serial = 1 };

	local_relation_scan_result = CLUSTER_PCM_PI_RELATION_PENDING;
	receive_committed(&request, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&storage.inbound_tail), 1);
	UT_ASSERT_EQ(storage.peer_fences[0].state, CLUSTER_KO_PEER_FENCE_ACTIVE);
	UT_ASSERT(memcmp(storage.peer_fences[0].encoded, "PKO4", 4) == 0);
	UT_ASSERT(memcmp(&storage.peer_fences[0].request, &request.object,
		sizeof(request.object)) == 0);
	fixture_peer_drain();
	UT_ASSERT_EQ(flush_count + sync_count + drop_count + buffer_drain_calls, 0);
	UT_ASSERT_EQ(storage.peer_fences[0].state, CLUSTER_KO_PEER_FENCE_ACTIVE);
	UT_ASSERT(!cluster_ko_peer_fence_ready_v3(&request.object, &old_handle));
	UT_ASSERT_EQ(storage.send_v3_count, 0);
}

UT_TEST(test_committed_retransmit_keeps_one_owner_and_rejects_legacy_same_key)
{
	ClusterKoSharedMessageV4 request = committed_peer(1002);

	receive_committed(&request, 0);
	receive_committed(&request, 0);
	enqueue_shared_v3(&request.object);
	UT_ASSERT_EQ(pg_atomic_read_u32(&storage.inbound_tail), 1);
	UT_ASSERT_EQ(storage.peer_fence_serial, 1);
	UT_ASSERT(memcmp(storage.peer_fences[0].encoded, "PKO4", 4) == 0);
	fixture_peer_drain();
	receive_committed(&request, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&storage.inbound_tail), 1);
	UT_ASSERT_EQ(storage.peer_fence_serial, 1);
}

UT_TEST(test_legacy_owner_cannot_be_reinterpreted_as_committed)
{
	ClusterKoSharedMessageV4 request = committed_peer(1003);

	native_capability_hook = native_legacy_v3_capability_word;
	enqueue_shared_v3(&request.object);
	native_capability_hook = committed_capabilities;
	receive_committed(&request, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&storage.inbound_tail), 1);
	UT_ASSERT_EQ(storage.peer_fence_serial, 1);
	UT_ASSERT(memcmp(storage.peer_fences[0].encoded, "PKO3", 4) == 0);
}

UT_TEST(test_new_capability_and_runtime_gates_precede_registration)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		ClusterKoSharedMessageV4 request = committed_peer(1010 + fault);
		switch (fault) {
		case 0: native_capability_hook = NULL; break;
		case 1: object_wire_missing_capabilities = PGRAC_IC_HELLO_CAP_KO_COMMITTED_DISCARD_V1; break;
		case 2: marker_capable = false; break;
		case 3: cluster_interconnect_tier = CLUSTER_IC_TIER_2; break;
		case 4: cluster_interconnect_tier = CLUSTER_IC_TIER_3; break;
		case 5: fixture_recovering = true; break;
		case 6: current_epoch++; break;
		case 7: v3_ingress_fault = 1; break;
		}
		receive_committed(&request, 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&storage.inbound_tail), 0);
		UT_ASSERT_EQ(storage.peer_fence_serial, 0);
		UT_ASSERT_EQ(flush_count + sync_count + drop_count, 0);
	}
}

UT_TEST(test_corrupt_version_and_identity_are_not_normalized_into_legacy)
{
	const unsigned offsets[] = { 1, 6, 8, 13, 33, 169, 185, 193, 221, 227, 229 };
	for (unsigned i = 0; i < lengthof(offsets); i++) {
		ClusterKoSharedMessageV4 request = committed_peer(1020 + i);
		receive_committed(&request, offsets[i]);
		UT_ASSERT_EQ(pg_atomic_read_u32(&storage.inbound_tail), 0);
		UT_ASSERT_EQ(storage.peer_fence_serial, 0);
	}
}

static ClusterKoSharedMessageV4 sent_committed;
static uint8 sent_type;

static ClusterICSendResult
capture_committed(uint8 type, int32 peer, const void *bytes, uint32 length)
{
	UT_ASSERT(SpinLockFree(&storage.shared_lock));
	UT_ASSERT(cluster_ko_shared_decode_v4(bytes, length, &sent_committed));
	UT_ASSERT_EQ(peer, type == PGRAC_IC_MSG_KO_FLUSH
		? sent_committed.object.base.peer_node : sent_committed.object.base.origin_node);
	sent_type = type;
	send_calls++;
	if (v3_send_throw) {
		v3_send_throw = false;
		pg_re_throw();
	}
	return v3_send_result;
}

/* The commit-window producer is still unconnected. Select its immutable
 * purpose at this module boundary; reserve/queue/send/ACK consumers are real. */
static ClusterKoSharedMessageV4
committed_origin(void)
{
	ClusterKoSharedMessageV4 request;

	cluster_interconnect_tier = CLUSTER_IC_TIER_1;
	request.object = origin_retained_send_setup();

	storage.contexts[0].committed_discard = true;
	storage.contexts[0].committed_drain_pending = true;
	native_capability_hook = committed_capabilities;
	object_wire_send_hook = capture_committed;
	sent_type = 0;
	memset(&sent_committed, 0, sizeof(sent_committed));
	return request;
}

static void
receive_result(const ClusterKoSharedMessageV4 *request, bool committed, uint16 status,
			   unsigned fault)
{
	ClusterKoSharedMessageV4 result = *request;
	ClusterICEnvelope env = { .source_node_id = request->object.base.peer_node,
		.payload_length = CLUSTER_KO_SHARED_V4_BYTES };
	uint8 wire[CLUSTER_KO_SHARED_V4_BYTES];

	result.object.base.verb = CLUSTER_KO_SHARED_ACK;
	result.object.base.status = status;
	switch (fault) {
	case 1: result.object.base.batch_id++; break;
	case 2: result.object.base.peer_boot++; break;
	case 3: result.object.space_mutation_token++; break;
	case 4: result.object.ax_holder.procno++; break;
	case 5: result.object.ax_master_generation++; break;
	case 6: result.object.base.key.locator.relNumber++; break;
	case 7: result.object.base.incarnation[0] ^= 1; break;
	case 8: result.object.base.origin_boot++; break;
	case 9: env.source_node_id++; break;
	}
	UT_ASSERT(committed ? cluster_ko_shared_encode_v4(&result, wire, sizeof(wire))
		: cluster_ko_shared_encode_v3(&result.object, wire, sizeof(wire)));
	cluster_ko_flush_ack_handler(&env, wire);
}

UT_TEST(test_committed_sender_preserves_version_through_backpressure_and_acceptance)
{
	ClusterKoSharedMessageV4 request = committed_origin();

	UT_ASSERT(ko_shared_enqueue_object(&request.object, request.object.ax_master_generation, true));
	v3_send_result = CLUSTER_IC_SEND_NOT_ADMITTED;
	shared_send_tick();
	UT_ASSERT_EQ(send_calls, 1);
	UT_ASSERT_EQ(storage.send_v3_count, 1);
	UT_ASSERT(storage.send_committed_discard[storage.send_v3_head]);
	UT_ASSERT_EQ(storage.contexts[0].request_uncertain_peers | storage.contexts[0].request_sent_peers, 0);
	v3_send_result = CLUSTER_IC_SEND_WOULD_BLOCK;
	shared_send_tick();
	UT_ASSERT_EQ(send_calls, 2);
	UT_ASSERT_EQ(sent_type, PGRAC_IC_MSG_KO_FLUSH);
	UT_ASSERT(memcmp(&sent_committed, &request, sizeof(request)) == 0);
	UT_ASSERT_EQ(storage.send_v3_count, 0);
	UT_ASSERT_EQ(storage.contexts[0].request_sent_peers, 2);
	UT_ASSERT_EQ(storage.contexts[0].request_done_peers, 0);
	UT_ASSERT(ko_shared_enqueue_object(&request.object, request.object.ax_master_generation, true));
	shared_send_tick();
	UT_ASSERT_EQ(send_calls, 2);
}

UT_TEST(test_ack_must_match_version_full_identity_and_original_claim)
{
	ClusterKoSharedMessageV4 request = committed_origin();

	receive_result(&request, true, CLUSTER_KO_SHARED_DONE, 0);
	UT_ASSERT_EQ(record_count, 0);
	UT_ASSERT(ko_shared_enqueue_object(&request.object, request.object.ax_master_generation, true));
	shared_send_tick();
	receive_result(&request, false, CLUSTER_KO_SHARED_DONE, 0);
	UT_ASSERT_EQ(record_count, 0);
	for (unsigned fault = 1; fault <= 9; fault++) {
		receive_result(&request, true, CLUSTER_KO_SHARED_DONE, fault);
		UT_ASSERT_EQ(record_count, 0);
		UT_ASSERT_EQ(storage.contexts[0].request_done_peers, 0);
	}
	receive_result(&request, true, CLUSTER_KO_SHARED_DONE, 0);
	UT_ASSERT_EQ(record_count, 1);
	UT_ASSERT_EQ(storage.contexts[0].request_done_peers, 2);
	UT_ASSERT(storage.contexts[0].committed_drain_pending && !storage.contexts[0].complete);
}

UT_TEST(test_new_ack_does_not_complete_legacy_origin)
{
	ClusterKoSharedMessageV4 request = committed_origin();

	storage.contexts[0].committed_discard = false;
	storage.contexts[0].committed_drain_pending = false;
	storage.contexts[0].request_sent_peers = 2;
	receive_result(&request, true, CLUSTER_KO_SHARED_DONE, 0);
	UT_ASSERT_EQ(record_count, 0);
	receive_result(&request, false, CLUSTER_KO_SHARED_DONE, 0);
	UT_ASSERT_EQ(record_count, 0);
	native_capability_hook = native_legacy_v3_capability_word;
	receive_result(&request, false, CLUSTER_KO_SHARED_DONE, 0);
	UT_ASSERT_EQ(record_count, 1);
	UT_ASSERT_EQ(storage.contexts[0].request_done_peers, 2);
}

UT_TEST(test_failed_ack_is_negative_and_unknown_requires_real_result)
{
	ClusterKoSharedMessageV4 request = committed_origin();

	v3_send_result = CLUSTER_IC_SEND_HARD_ERROR;
	UT_ASSERT(ko_shared_enqueue_object(&request.object, request.object.ax_master_generation, true));
	shared_send_tick();
	UT_ASSERT_EQ(storage.contexts[0].request_uncertain_peers, 2);
	receive_result(&request, false, CLUSTER_KO_SHARED_FAILED, 0);
	UT_ASSERT_EQ(failure_records, 0);
	receive_result(&request, true, CLUSTER_KO_SHARED_FAILED, 0);
	UT_ASSERT_EQ(failure_records, 1);
	UT_ASSERT_EQ(storage.contexts[0].request_uncertain_peers, 0);
	UT_ASSERT_EQ(storage.contexts[0].request_failed_peers, 2);
	UT_ASSERT_EQ(record_count + storage.contexts[0].request_done_peers, 0);
	UT_ASSERT(!storage.contexts[0].complete && storage.contexts[0].committed_drain_pending);
}

UT_TEST(test_queued_purpose_or_capability_drift_cannot_escape)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterKoSharedMessageV4 request = committed_origin();
		UT_ASSERT(ko_shared_enqueue_object(&request.object, request.object.ax_master_generation, true));
		if (fault == 0) storage.send_committed_discard[storage.send_v3_head] = false;
		if (fault == 1) native_capability_hook = NULL;
		if (fault == 2) object_wire_missing_capabilities = PGRAC_IC_HELLO_CAP_KO_COMMITTED_DISCARD_V1;
		shared_send_tick();
		UT_ASSERT_EQ(send_calls, 0);
		UT_ASSERT_EQ(storage.contexts[0].request_uncertain_peers | storage.contexts[0].request_sent_peers, 0);
		UT_ASSERT(storage.contexts[0].used && storage.contexts[0].committed_drain_pending);
	}
}

UT_TEST(test_sender_error_keeps_unknown_and_does_not_repeat_transport)
{
	ClusterKoSharedMessageV4 request = committed_origin();
	volatile bool caught = false;

	UT_ASSERT(ko_shared_enqueue_object(&request.object, request.object.ax_master_generation, true));
	v3_send_throw = true;
	PG_TRY(); { shared_send_tick(); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(storage.contexts[0].request_uncertain_peers, 2);
	UT_ASSERT_EQ(storage.contexts[0].request_sending_peers, 0);
	shared_send_tick();
	UT_ASSERT_EQ(send_calls, 1);
	UT_ASSERT_EQ(storage.send_v3_count, 0);
	UT_ASSERT(storage.contexts[0].used && storage.contexts[0].committed_drain_pending);
}

UT_TEST(test_peer_failure_reply_uses_original_version_and_exact_send_owner)
{
	ClusterKoSharedMessageV4 request = committed_peer(1100);
	ClusterKoSharedMessageV3 failure = request.object;

	local_relation_scan_result = CLUSTER_PCM_PI_RELATION_PENDING;
	receive_committed(&request, 0);
	fixture_peer_drain();
	ko_peer_attempt = (ClusterKoPeerFenceHandleV3){ .slot = 0, .serial = storage.peer_fences[0].serial };
	ko_peer_attempt_active = true;
	cluster_ko_peer_apply_error_v3(ERRCODE_IO_ERROR);
	failure.base.verb = CLUSTER_KO_SHARED_ACK;
	failure.base.status = CLUSTER_KO_SHARED_FAILED;
	UT_ASSERT(!ko_shared_enqueue_v3(&failure, v3_route_generation));
	fixture_peer_drain();
	UT_ASSERT_EQ(storage.send_v3_count, 1);
	UT_ASSERT(storage.send_committed_discard[storage.send_v3_head]);
	object_wire_send_hook = capture_committed;
	v3_send_result = CLUSTER_IC_SEND_NOT_ADMITTED;
	shared_send_tick();
	UT_ASSERT_EQ(storage.peer_fences[0].reply_state, KO_PEER_REPLY_QUEUED);
	UT_ASSERT(storage.send_committed_discard[storage.send_v3_head]);
	v3_send_result = CLUSTER_IC_SEND_DONE;
	shared_send_tick();
	UT_ASSERT_EQ(sent_type, PGRAC_IC_MSG_KO_FLUSH_ACK);
	UT_ASSERT_EQ(sent_committed.object.base.status, CLUSTER_KO_SHARED_FAILED);
	UT_ASSERT(cluster_ko_shared_result_matches_v4(&request, &sent_committed));
	UT_ASSERT_EQ(storage.peer_fences[0].reply_state, KO_PEER_REPLY_TRANSPORT_OWNED);
	UT_ASSERT_EQ(storage.peer_fences[0].state, CLUSTER_KO_PEER_FENCE_ACTIVE);
	UT_ASSERT_EQ(flush_count + sync_count + drop_count, 0);
}

UT_TEST(test_pending_new_peer_is_not_ready_or_normal_stop_complete)
{
	ClusterKoSharedMessageV4 request = committed_peer(1101);
	const char *reason = NULL;

	local_relation_scan_result = CLUSTER_PCM_PI_RELATION_PENDING;
	receive_committed(&request, 0);
	fixture_peer_drain();
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(storage.peer_fences[0].state, CLUSTER_KO_PEER_FENCE_ACTIVE);
	UT_ASSERT_EQ(storage.send_v3_count, 0);
	UT_ASSERT_EQ(flush_count + sync_count + drop_count + buffer_drain_calls, 0);
}

UT_TEST(test_committed_peer_closes_entered_io_before_discard_and_ready)
{
	ClusterKoSharedMessageV4 request = committed_peer(1200);
	ClusterKoDirectoryIoV3 entered, refused;
	RelFileLocator locator = request.object.base.key.locator;

	UT_ASSERT(cluster_ko_directory_io_begin_v3(locator, &entered));
	receive_committed(&request, 0);
	fixture_peer_drain();
	UT_ASSERT(cluster_ko_directory_drop_committed_v3(locator));
	UT_ASSERT(!cluster_ko_directory_drop_retired_v3(locator));
	UT_ASSERT(!cluster_ko_directory_io_begin_v3(locator, &refused));
	UT_ASSERT_EQ(committed_discard_calls + storage.send_v3_count, 0);
	UT_ASSERT(cluster_ko_directory_io_end_v3(&entered));
	fixture_peer_drain();
	UT_ASSERT(cluster_ko_directory_drop_retired_v3(locator));
	UT_ASSERT_EQ(committed_discard_calls, 1);
	UT_ASSERT_EQ(storage.peer_fences[0].state, CLUSTER_KO_PEER_FENCE_READY);
	UT_ASSERT_EQ(storage.send_v3_count, 1);
	UT_ASSERT(storage.send_committed_discard[storage.send_v3_head]);
	UT_ASSERT_EQ(flush_count + sync_count + drop_count + space_reads + buffer_drain_calls, 0);
}

UT_TEST(test_committed_discard_resumes_bounded_cursor_without_live_proof)
{
	ClusterKoSharedMessageV4 request = committed_peer(1201);
	removal_ready = false; /* Dead data is not a CTRC live-object DATA proof. */
	space_ok = false;
	committed_discard_result = CLUSTER_BUFFER_DRAIN_MORE;
	receive_committed(&request, 0);
	fixture_peer_drain();
	UT_ASSERT_EQ(committed_discard_calls, 1);
	UT_ASSERT_EQ(storage.peer_fences[0].physical_cursor, 128);
	fixture_peer_drain();
	UT_ASSERT_EQ(committed_discard_input, 128);
	UT_ASSERT_EQ(storage.peer_fences[0].physical_cursor, 256);
	committed_discard_result = CLUSTER_BUFFER_DRAIN_EMPTY;
	fixture_peer_drain();
	UT_ASSERT_EQ(storage.peer_fences[0].state, CLUSTER_KO_PEER_FENCE_READY);
	UT_ASSERT_EQ(flush_count + sync_count + drop_count + space_reads + buffer_drain_calls, 0);
}

UT_TEST(test_committed_discard_keeps_local_and_writeback_responsibility)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterKoSharedMessageV4 request = committed_peer(1202 + fault);
		if (fault == 0) committed_discard_result = CLUSTER_BUFFER_DRAIN_WAIT;
		if (fault == 1) local_relation_scan_result = CLUSTER_PCM_PI_RELATION_PENDING;
		if (fault == 2) local_writeback_closed = false;
		receive_committed(&request, 0);
		fixture_peer_drain();
		UT_ASSERT_EQ(committed_discard_calls, 1);
		UT_ASSERT_EQ(storage.peer_fences[0].state, CLUSTER_KO_PEER_FENCE_ACTIVE);
		UT_ASSERT_EQ(storage.send_v3_count, 0);
		committed_discard_result = CLUSTER_BUFFER_DRAIN_EMPTY;
		local_relation_scan_result = CLUSTER_PCM_PI_RELATION_EMPTY;
		local_writeback_closed = true;
		fixture_peer_drain();
		UT_ASSERT_EQ(storage.peer_fences[0].state, CLUSTER_KO_PEER_FENCE_READY);
	}
}

static void committed_cut_drift(void) { relation_gate_freeze++; }
static void committed_discard_error(void) { pg_re_throw(); }

UT_TEST(test_committed_discard_drift_and_error_never_publish_ready)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterKoSharedMessageV4 request = committed_peer(1210 + fault);
		volatile bool caught = false;
		committed_discard_hook = fault == 0 ? committed_cut_drift : committed_discard_error;
		receive_committed(&request, 0);
		PG_TRY(); { fixture_peer_drain(); }
		PG_CATCH(); {
			caught = true;
			cluster_ko_peer_apply_error_v3(ERRCODE_IO_ERROR);
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault != 0);
		UT_ASSERT(!storage.peer_fences[0].physical_applied);
		UT_ASSERT_EQ(storage.send_v3_count, 0);
		UT_ASSERT(cluster_ko_directory_drop_retired_v3(request.object.base.key.locator));
		committed_discard_hook = NULL;
		fixture_peer_drain();
		UT_ASSERT_EQ(storage.send_v3_count, 1);
		UT_ASSERT_EQ(storage.send_v3[storage.send_v3_head].base.status,
			fault == 0 ? CLUSTER_KO_SHARED_DONE : CLUSTER_KO_SHARED_FAILED);
	}
}

UT_TEST(test_rewrite_old_uses_committed_identity_and_old_wire_cannot_claim_it)
{
	ClusterKoSharedMessageV4 request = committed_peer(1220);
	request.object.operation = CLUSTER_KO_SHARED_V3_OPERATION_REWRITE_OLD;
	receive_committed(&request, 0);
	fixture_peer_drain();
	UT_ASSERT(cluster_ko_directory_drop_retired_v3(request.object.base.key.locator));
	UT_ASSERT_EQ(storage.peer_fences[0].state, CLUSTER_KO_PEER_FENCE_READY);
	UT_ASSERT_EQ(storage.send_v3_count, 1);
	enqueue_shared_v3(&request.object);
	UT_ASSERT_EQ(pg_atomic_read_u32(&storage.inbound_tail), 1);
	UT_ASSERT_EQ(storage.peer_fence_serial, 1);
}

UT_TEST(test_committed_gc_requires_accepted_ready_and_keeps_negative_identity)
{
	ClusterKoSharedMessageV4 request = committed_peer(1300), first = request;
	ClusterKoDirectoryIoV3 refused;

	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		request.object.base.batch_id = 1300 + i;
		request.object.base.key.locator.relNumber = 99 + i;
		local_scan_rel_number = 99 + i;
		receive_committed(&request, 0);
		fixture_peer_drain();
		UT_ASSERT_EQ(storage.peer_fences[i].state, CLUSTER_KO_PEER_FENCE_READY);
	}
	UT_ASSERT(!cluster_ko_peer_fence_gc_tick_v3());
	object_wire_send_hook = capture_committed;
	v3_send_result = CLUSTER_IC_SEND_NOT_ADMITTED;
	shared_send_tick();
	UT_ASSERT(!cluster_ko_peer_fence_gc_tick_v3());
	v3_send_result = CLUSTER_IC_SEND_DONE;
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) shared_send_tick();
	UT_ASSERT_EQ(storage.send_v3_count, 0);
	UT_ASSERT(cluster_ko_peer_fence_gc_tick_v3());
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++)
		UT_ASSERT_EQ(storage.peer_fences[i].state, CLUSTER_KO_PEER_FENCE_EMPTY);
	UT_ASSERT(cluster_ko_directory_drop_retired_v3(first.object.base.key.locator));
	UT_ASSERT(!cluster_ko_directory_io_begin_v3(first.object.base.key.locator, &refused));
	receive_committed(&first, 0);
	UT_ASSERT_EQ(storage.peer_fences[0].state, CLUSTER_KO_PEER_FENCE_EMPTY);
	request.object.base.batch_id++;
	request.object.base.key.locator.relNumber++;
	local_scan_rel_number++;
	receive_committed(&request, 0);
	fixture_peer_drain();
	UT_ASSERT_EQ(storage.peer_fences[0].state, CLUSTER_KO_PEER_FENCE_READY);
}

UT_TEST(test_committed_closure_capacity_and_finite_owner_never_become_empty)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterKoSharedMessageV4 request = committed_peer(1400 + fault);
		RelFileLocator locator = request.object.base.key.locator;
		if (fault == 0) storage.drop_free = 0;
		if (fault == 1) {
			storage.cleaner_work[0].used = true;
			storage.cleaner_work[0].locator = locator;
		}
		if (fault == 2) {
			storage.extend_work[0].used = true;
			storage.extend_work[0].locator = locator;
		}
		receive_committed(&request, 0);
		fixture_peer_drain();
		UT_ASSERT_EQ(committed_discard_calls + storage.send_v3_count, 0);
		UT_ASSERT_EQ(storage.peer_fences[0].state, CLUSTER_KO_PEER_FENCE_ACTIVE);
		UT_ASSERT(!cluster_ko_directory_drop_retired_v3(locator));
	}
}

UT_TEST(test_committed_local_pi_uses_discard_consumer_but_keeps_writeback_gate)
{
	ClusterKoSharedMessageV4 request = committed_peer(1500);

	local_relation_scan_result = CLUSTER_PCM_PI_RELATION_PENDING;
	local_pi_only_pending = true;
	local_writeback_closed = false;
	receive_committed(&request, 0);
	fixture_peer_drain();
	UT_ASSERT_EQ(local_discard_calls, 1);
	UT_ASSERT_EQ(storage.peer_fences[0].state, CLUSTER_KO_PEER_FENCE_ACTIVE);
	UT_ASSERT_EQ(storage.send_v3_count, 0);
	local_writeback_closed = true;
	fixture_peer_drain();
	UT_ASSERT_EQ(storage.peer_fences[0].state, CLUSTER_KO_PEER_FENCE_READY);
	UT_ASSERT_EQ(storage.send_v3_count, 1);
	UT_ASSERT_EQ(flush_count + sync_count + drop_count + space_reads, 0);
}

UT_TEST(test_live_object_never_uses_committed_local_pi_discard)
{
	ClusterKoSharedMessageV3 request = peer_fence_fixture(1501);

	local_relation_scan_result = CLUSTER_PCM_PI_RELATION_PENDING;
	local_pi_only_pending = true;
	enqueue_shared_v3(&request);
	fixture_peer_drain();
	UT_ASSERT_EQ(local_discard_calls, 0);
	UT_ASSERT_EQ(storage.peer_fences[0].state, CLUSTER_KO_PEER_FENCE_ACTIVE);
	UT_ASSERT_EQ(storage.send_v3_count, 0);
}

UT_TEST(test_production_capability_admits_committed_peer_and_ready)
{
	ClusterKoSharedMessageV4 request = committed_peer(1600);
	ClusterKoDirectoryIoV3 denied;

	native_capability_hook = native_real_capability_word;
	receive_committed(&request, 0);
	fixture_peer_drain();
	UT_ASSERT_EQ(storage.peer_fences[0].state, CLUSTER_KO_PEER_FENCE_READY);
	UT_ASSERT_EQ(storage.send_v3_count, 1);
	UT_ASSERT(cluster_ko_directory_drop_retired_v3(request.object.base.key.locator));
	UT_ASSERT(!cluster_ko_directory_io_begin_v3(request.object.base.key.locator, &denied));
	UT_ASSERT_EQ(committed_discard_calls, 1);
	UT_ASSERT_EQ(flush_count + sync_count + drop_count + space_reads + buffer_drain_calls, 0);
	object_wire_send_hook = capture_committed;
	shared_send_tick();
	UT_ASSERT_EQ(sent_type, PGRAC_IC_MSG_KO_FLUSH_ACK);
	UT_ASSERT(cluster_ko_shared_ack_matches_v4(&request, &sent_committed));
}

UT_TEST(test_production_capability_rejects_old_requests_before_any_owner)
{
	for (unsigned version = 2; version <= 3; version++) {
		ClusterKoSharedMessageV3 request = peer_fence_fixture(1601 + version);

		native_capability_hook = native_real_capability_word;
		if (version == 2)
			enqueue_shared(&request.base);
		else
			enqueue_shared_v3(&request);
		UT_ASSERT_EQ(pg_atomic_read_u32(&storage.inbound_tail), 0);
		UT_ASSERT_EQ(storage.peer_fence_serial, 0);
		UT_ASSERT_EQ(storage.send_v3_count, 0);
		UT_ASSERT_EQ(committed_discard_calls + space_reads + flush_count + sync_count + drop_count, 0);
	}
}

int
main(void)
{
	UT_PLAN(24);
	UT_RUN(test_production_capability_admits_committed_peer_and_ready);
	UT_RUN(test_production_capability_rejects_old_requests_before_any_owner);
	UT_RUN(test_committed_request_registers_exact_version_without_legacy_physical_work);
	UT_RUN(test_committed_retransmit_keeps_one_owner_and_rejects_legacy_same_key);
	UT_RUN(test_legacy_owner_cannot_be_reinterpreted_as_committed);
	UT_RUN(test_new_capability_and_runtime_gates_precede_registration);
	UT_RUN(test_corrupt_version_and_identity_are_not_normalized_into_legacy);
	UT_RUN(test_committed_sender_preserves_version_through_backpressure_and_acceptance);
	UT_RUN(test_ack_must_match_version_full_identity_and_original_claim);
	UT_RUN(test_new_ack_does_not_complete_legacy_origin);
	UT_RUN(test_failed_ack_is_negative_and_unknown_requires_real_result);
	UT_RUN(test_queued_purpose_or_capability_drift_cannot_escape);
	UT_RUN(test_sender_error_keeps_unknown_and_does_not_repeat_transport);
	UT_RUN(test_peer_failure_reply_uses_original_version_and_exact_send_owner);
	UT_RUN(test_pending_new_peer_is_not_ready_or_normal_stop_complete);
	UT_RUN(test_committed_peer_closes_entered_io_before_discard_and_ready);
	UT_RUN(test_committed_discard_resumes_bounded_cursor_without_live_proof);
	UT_RUN(test_committed_discard_keeps_local_and_writeback_responsibility);
	UT_RUN(test_committed_discard_drift_and_error_never_publish_ready);
	UT_RUN(test_rewrite_old_uses_committed_identity_and_old_wire_cannot_claim_it);
	UT_RUN(test_committed_gc_requires_accepted_ready_and_keeps_negative_identity);
	UT_RUN(test_committed_closure_capacity_and_finite_owner_never_become_empty);
	UT_RUN(test_committed_local_pi_uses_discard_consumer_but_keeps_writeback_gate);
	UT_RUN(test_live_object_never_uses_committed_local_pi_discard);
	UT_DONE();
	return ut_failed_count != 0;
}
