/* Actual S-to-X caller ERROR cleanup and an independent master. The S being
 * converted is installed by a preceding ordinary acquisition, not a fixture
 * grant. Socket, peer, scheduling and storage leaves remain controlled.
 * Author: SqlRush <sqlrush@gmail.com> */
#define NATIVE_MASTER_RESIDENT_WRITE_COMPOSITION
#include "test_cluster_native_master_fixture.inc"

static unsigned sx_cut;
static bool sx_dirty, sx_late_callback, sx_successor;
static ResourceXDecodedFrame sx_old_consumed, sx_seed_consumed;
static ResourceXIntentSlot sx_seed_callback;
static bool sx_exiting;
static sigjmp_buf sx_exit_jump;
static bool sx_late_seed(void);
static void sx_capture_seed(const ResourceXIntentSlot *slot, const ResourceXDecodedFrame *receipt)
{
	sx_seed_callback = *slot; sx_seed_consumed = *receipt;
}
static void sx_interrupt(void)
{
	if (!sx_exiting) pg_re_throw();
	/* Real PG_ENSURE callbacks, using native ipc.c's pop-before-call order.
	 * FATAL does not return through the abandoned PG_TRY frames. */
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	while (disk_exit_count != 0) {
		unsigned n = --disk_exit_count;
		disk_exit_callbacks[n](1, disk_exit_arguments[n]);
	}
	siglongjmp(sx_exit_jump, 1);
}

static bool sx_decode(const uint8 *bytes, uint16 type, ResourceXDecodedFrame *out)
{
	NM_REQUIRE(cluster_resource_x_wire_decode(type, bytes, RESOURCE_X_SOURCE_COMPLETE_V11_BYTES,
		out, &(ResourceXWireReject){0}));
	return true;
}
static bool sx_master(int fd, int mode)
{
	/* This original first-fill owns and publishes the initial S provenance. */
	NM_REQUIRE(nm_master(fd, 0));
	for (unsigned n = 0; n < (sx_successor ? 2u : 1u); n++) {
		PublishFixture f;
		ResourceXDecodedFrame request, grant, done, consumed;
		uint64 old_grant;
		bool installed = n != 0 || sx_cut >= 2;
		NM_REQUIRE(nm_receive(fd, RESOURCE_X_MSG_ASSERT_X, &request));
		NM_REQUIRE(nm_master_admit(&f, &request));
		grant = publish_decode(f.state->grant_intent.payload, false);
		UT_ASSERT_EQ(grant.body.acquire_decision.source_kind, RESOURCE_X_ACQUIRE_SOURCE_LOCAL_CURRENT);
		UT_ASSERT_EQ(grant.body.acquire_decision.source_node, 1);
		UT_ASSERT_EQ(grant.body.acquire_decision.required_holders_bitmap, 0);
		UT_ASSERT_EQ(grant.body.acquire_decision.holders_bitmap, 2);
		UT_ASSERT_EQ(grant.body.acquire_decision.directory_mode, PCM_STATE_S);
		UT_ASSERT_EQ(grant.body.acquire_decision.episode.path, RESOURCE_X_ACQUIRE_RESIDENT);
		UT_ASSERT_EQ(grant.common.target_mode, PCM_STATE_X);
		old_grant = f.state->holder_grants[1].grant_generation;
		if (mode != 1) NM_REQUIRE(nm_master_callback(&f.state->grant_intent.slot, true));
		NM_REQUIRE(nm_send(fd, RESOURCE_X_MSG_IMAGE_OR_GRANT, &grant));
		NM_REQUIRE(nm_receive(fd, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &done));
		UT_ASSERT_EQ(done.common.outcome, installed ? RESOURCE_X_OUTCOME_OK
			: sx_cut == 0 ? RESOURCE_X_OUTCOME_QUIET_NO_IMAGE : RESOURCE_X_OUTCOME_QUIET);
		UT_ASSERT(memcmp(&done.body.acquire_completion.decision, &grant.body.acquire_decision,
			sizeof(grant.body.acquire_decision)) == 0);
		NM_REQUIRE(nm_master_done(&done) == RESOURCE_X_APPLY_APPLIED);
		if (mode == 1) {
			UT_ASSERT(f.state->deferred_settlement.present != 0);
			UT_ASSERT(f.state->requests[1].phase != RESOURCE_X_MASTER_NONE);
			NM_REQUIRE(nm_master_callback(&f.state->grant_intent.slot, true));
			NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.work) == RESOURCE_X_APPLY_APPLIED);
		}
		UT_ASSERT_EQ(f.state->deferred_settlement.present, 3);
		if (mode == 2) NM_REQUIRE(nm_master_done(&done) == RESOURCE_X_APPLY_DUPLICATE);
		NM_REQUIRE(sx_decode(f.state->grant_intent.payload, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &consumed));
		if (mode == 3) {
			NM_REQUIRE(nm_master_callback(&f.state->grant_intent.slot, false));
			UT_ASSERT_EQ(f.state->deferred_settlement.present, 3);
			UT_ASSERT_EQ(f.state->grant_intent.slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
			fake_pcm_clock_us += 1000000;
		}
		NM_REQUIRE(nm_send(fd, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &consumed));
		NM_REQUIRE(nm_master_callback(&f.state->grant_intent.slot, true));
		UT_ASSERT_EQ(f.state->deferred_settlement.present, 0);
		UT_ASSERT_EQ(f.state->requests[1].phase, RESOURCE_X_MASTER_NONE);
		UT_ASSERT_EQ(f.state->requester_settlement_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
		UT_ASSERT_EQ(f.state->bootstrap_receipts[1].highest_attempt_floor, request.common.assertion_sequence);
		UT_ASSERT_EQ(f.state->holder_grants[1].grant_generation,
			installed ? done.common.authority_generation : old_grant);
		UT_ASSERT_EQ(pg_atomic_read_u32(&((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->s_holders_bitmap), installed ? 0 : 2);
		UT_ASSERT_EQ(((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->x_holder_node, installed ? 1 : -1);
	}
	complete_idle(); return true;
}
static void sx_pre_C_error(void)
{
	if (request_entry()->resource_x_bootstrap_round.delivery_executor_purpose
		== RESOURCE_X_DELIVERY_ACQUIRE_EXECUTING) {
		stage_after_content = NULL; UT_ASSERT(sx_late_seed()); sx_interrupt();
	}
}
static void sx_native_error(void)
{
	if ((sx_cut == 1 && request_entry()->resource_x_bootstrap_round.delivery_executor_purpose
			== RESOURCE_X_DELIVERY_ACQUIRE_READ_ARMED && request_buf.pcm_state == PCM_STATE_S)
		|| (sx_cut == 2 && request_buf.pcm_state == PCM_STATE_X
			&& cluster_pcm_own_gen_get(0) == prepared_episode.base_generation + 1)) {
		rx_native_boundary = NULL; UT_ASSERT(sx_late_seed()); sx_interrupt();
	}
}
/* The original separate LMS drain is qualified elsewhere. This explicit
 * transport-leaf release occurs only after the real send callback and its
 * consumed reply have retired the previous episode; it clears no PCM state. */
static void sx_transport_idle(void)
{
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.pending, 0);
	memset(&request_shared, 0, sizeof(request_shared));
	pg_atomic_init_u64(&request_shared.resource_x_transport_mutation_sequence, 1);
}
static bool sx_probe(ResourceXIntentSlot *out)
{
	uint8 bytes[RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES];
	ResourceXAcquisitionRef ref; uint32 examined;
	fake_pcm_clock_us += 1000000;
	for (int n = 0; n < 256; n++) {
		ResourceXIntentProbeResult result = cluster_pcm_lock_resource_x_outbound_work_probe_exact(
			4, out, bytes, sizeof(bytes), &examined, &ref);
		NM_REQUIRE(result != RESOURCE_X_INTENT_PROBE_CORRUPT);
		if (result == RESOURCE_X_INTENT_PROBE_FOUND) return true;
		if (result == RESOURCE_X_INTENT_PROBE_IDLE) break;
	}
	return false;
}
static bool sx_accept(const ResourceXDecodedFrame *frame, ResourceXApplyResult expected)
{
	ClusterICEnvelope env = {0}; ResourceXRequesterJoinSnapshot joined;
	env.msg_type = RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE; env.source_node_id = 0; env.dest_node_id = 1;
	env.epoch = 17; env.payload_length = frame->payload_bytes; auth_peer_boot = 31;
	NM_REQUIRE(gcs_block_resource_x_requester_join_ingress(&env, frame, 61, &joined) == expected);
	return true;
}
static bool sx_late_seed(void)
{
	ClusterPcmResourceXBootstrapRound before = request_entry()->resource_x_bootstrap_round;
	ClusterPcmOwnEntry native = request_own;
	uint32 header = pg_atomic_read_u32(&request_buf.state);
	NM_REQUIRE(sx_accept(&sx_seed_consumed, RESOURCE_X_APPLY_STALE));
	NM_REQUIRE(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(
		&sx_seed_callback, true, ++fake_pcm_clock_us));
	UT_ASSERT(memcmp(&before, &request_entry()->resource_x_bootstrap_round, sizeof(before)) == 0);
	UT_ASSERT(memcmp(&native, &request_own, sizeof(native)) == 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state), header);
	return true;
}
static bool sx_late_old(void)
{
	ClusterPcmResourceXBootstrapRound before = request_entry()->resource_x_bootstrap_round;
	ClusterPcmOwnEntry native = request_own;
	uint32 header = pg_atomic_read_u32(&request_buf.state);
	NM_REQUIRE(sx_accept(&sx_old_consumed, RESOURCE_X_APPLY_STALE));
	UT_ASSERT(memcmp(&before, &request_entry()->resource_x_bootstrap_round, sizeof(before)) == 0);
	UT_ASSERT(memcmp(&native, &request_own, sizeof(native)) == 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state), header);
	return true;
}
static bool sx_parent(int fd, int mode)
{
	PGIOAlignedBlock before;
	rx_native_boundary = NULL;
	nm_settled_observer = sx_capture_seed;
	NM_REQUIRE(nm_parent_as(fd, 0, 1));
	nm_settled_observer = NULL;
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.resident_s_authorized, 1);
	/* Preserve the actual S owner, bytes and master binding across the new
	 * operation. Only event leaves/counters and the consumed transport reset. */
	sx_transport_idle(); nm_fd = fd; nm_mode = mode == 2 ? 2 : 0;
	ordinary_delivery_transport = nm_deliver;
	cluster_enabled = cluster_shared_config = cluster_gcs_block_local_cache = true;
	wc_object_enabled = false; wc_object_valid = true;
	wc_r4 = 77; wc_session_id = 31; wc_master = 0; wc_path = RESOURCE_X_WRITER_TARGET;
	wc_legacy = wc_failures = wc_waits = wc_object_checks = wc_unavailable = wc_pauses = 0;
	wc_cancel_pause = rx_barrier = rx_cancel_wait = false;
	if (sx_dirty) pg_atomic_fetch_or_u32(&request_buf.state, BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED);
	memcpy(before.data, disk_shared.data, BLCKSZ);
	for (unsigned n = 0; n < (sx_successor ? 2u : 1u); n++) {
		ClusterPcmResourceXMasterState *state = NULL;
		ResourceXDecodedFrame done, consumed;
		ResourceXIntentSlot observed, staged;
		ClusterPcmXWriterLedgerEntry *entry;
		uint32 semantic = pg_atomic_read_u32(&request_buf.state) & CLUSTER_PCM_OWN_SEMANTIC_BUF_STATE_MASK;
		bool installed = n != 0 || sx_cut >= 2;
		ordinary_active = NULL; ordinary_sends = ordinary_pauses = ordinary_leaves = 0;
		ordinary_current_calls = ordinary_peer_samples = 0;
		disk_reads = complete_commits = complete_io_calls = complete_broadcasts = 0;
		MyBackendType = B_BACKEND;
		stage_after_content = n == 0 && sx_cut == 0 ? sx_pre_C_error : NULL;
		rx_native_boundary = n == 0 && sx_cut != 0 ? sx_native_error : NULL;
		if (sx_exiting && n == 0) {
			sigjmp_buf *saved = PG_exception_stack;
			ErrorContextCallback *context = error_context_stack;
			entry = NULL;
			if (sigsetjmp(sx_exit_jump, 0) == 0) { (void)rx_prepare(NULL); UT_ASSERT(false); }
			PG_exception_stack = saved; error_context_stack = context;
			UT_ASSERT_EQ(disk_exit_count, 0); UT_ASSERT_EQ(disk_exit_lifo_errors, 0);
		} else entry = rx_prepare(NULL);
		NM_REQUIRE(n == 0 && sx_cut < 3 ? entry == NULL : entry != NULL);
		UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(wc_legacy, 0); UT_ASSERT_EQ(ordinary_panics, 0);
		UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT_EQ(complete_io_calls, 0);
		UT_ASSERT_EQ(complete_commits, 0); UT_ASSERT_EQ(complete_broadcasts, 0);
		UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + installed);
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0); UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
		UT_ASSERT_EQ(request_buf.pcm_state, installed ? PCM_STATE_X : PCM_STATE_S);
		UT_ASSERT_EQ(request_buf.buffer_type, installed ? BUF_TYPE_XCUR : BUF_TYPE_SCUR);
		UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & CLUSTER_PCM_OWN_SEMANTIC_BUF_STATE_MASK, semantic);
		UT_ASSERT(memcmp(before.data, disk_shared.data, BLCKSZ) == 0);
		UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1)); UT_ASSERT(!fake_cv_prepared);
		if (entry != NULL) {
			NM_REQUIRE(sx_late_seed());
			if (n != 0) NM_REQUIRE(sx_late_old());
			LWLockAcquire(entry->content_lock, LW_EXCLUSIVE);
			NM_REQUIRE(cluster_bufmgr_pcm_x_writer_activate(entry, true));
			cluster_bufmgr_pcm_x_writer_mark_releasing(entry); LWLockRelease(entry->content_lock);
			cluster_bufmgr_pcm_x_writer_release(entry);
		}
		MyBackendType = B_LMS;
		NM_REQUIRE(sx_probe(&observed)); NM_REQUIRE(nm_requester_notice(&state));
		NM_REQUIRE(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(
			&observed, ++fake_pcm_clock_us, &staged) == RESOURCE_X_INTENT_STAGED);
		NM_REQUIRE(sx_decode(state->requester_settlement_intent.payload, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &done));
		UT_ASSERT_EQ(done.common.outcome, installed ? RESOURCE_X_OUTCOME_OK
			: sx_cut == 0 ? RESOURCE_X_OUTCOME_QUIET_NO_IMAGE : RESOURCE_X_OUTCOME_QUIET);
		UT_ASSERT_EQ(done.body.acquire_completion.native_generation, request_base.generation + installed);
		if (!sx_late_callback) NM_REQUIRE(nm_master_callback(&staged, true));
		NM_REQUIRE(nm_send(fd, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &done));
		NM_REQUIRE(nm_receive(fd, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &consumed));
		NM_REQUIRE(sx_accept(&consumed, RESOURCE_X_APPLY_APPLIED));
		if (sx_late_callback) {
			UT_ASSERT_EQ(state->requester_settlement_intent.slot.state, RESOURCE_X_INTENT_SLOT_STAGED);
			if (mode == 2) NM_REQUIRE(sx_accept(&consumed, RESOURCE_X_APPLY_DUPLICATE));
			NM_REQUIRE(nm_master_callback(&staged, true));
		}
		UT_ASSERT_EQ(state->requester_settlement_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.pending, 0);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
			installed ? RESOURCE_X_DELIVERY_ACQUIRE_READ_SETTLED : 0);
		if (!installed) UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.cancelled_attempt_floor, done.common.assertion_sequence);
		if (mode == 2) {
			sx_old_consumed = consumed;
			if (installed) NM_REQUIRE(sx_accept(&consumed, RESOURCE_X_APPLY_DUPLICATE));
			else NM_REQUIRE(sx_late_old());
		}
		if (n == 0) sx_old_consumed = consumed;
		cluster_bufmgr_pcm_x_writer_exception_cleanup_all();
		if (LWLockHeldByMe(BufferDescriptorGetContentLock(&request_buf))) LWLockRelease(BufferDescriptorGetContentLock(&request_buf));
		complete_idle(); sx_transport_idle();
	}
	return true;
}
static void sx_run(unsigned cut, int mode, bool late, bool dirty, bool successor)
{
	sx_cut = cut; sx_dirty = dirty; sx_late_callback = late; sx_successor = successor;
	nm_run_roles(mode, sx_master, sx_parent);
}
/* Exercise the reservation-to-round handoff separately from M's service.
 * The read successor is cancelled before REQUEST: M's old S authorization
 * stays intact, and no delivery can target this unsent reservation. */
#define LockBufHdr stage_header_lock
#include "test_cluster_native_s_release.inc"
#undef LockBufHdr
static bool sx_identity_parent(int fd, int mode)
{
	ClusterPcmResourceXBootstrapRound old, before;
	ClusterPcmOwnSnapshot base, released;
	ClusterSemanticAdmissionToken admission = {0};
	ResourceXDecodedAcquireIntent episode;
	ResourceXDecodedFrame dispatch;
	ResourceXAcquisitionRef terminal;
	ResourceXAssertion assertion;
	ResourceXInstallClaimJoinObservation observation, issued;
	uint64 token;
	(void)mode;
	rx_native_boundary = NULL; nm_settled_observer = sx_capture_seed;
	NM_REQUIRE(nm_parent_as(fd, 0, 1)); nm_settled_observer = NULL;
	old = request_entry()->resource_x_bootstrap_round;
	NM_REQUIRE(cluster_bufmgr_pcm_own_snapshot(&request_buf, &base) == CLUSTER_PCM_OWN_OK);
	if (mode == 1) {
		LWLockAcquire(BufferDescriptorGetContentLock(&request_buf), LW_EXCLUSIVE);
		NM_REQUIRE(cluster_bufmgr_pcm_own_finish_s_release_to_n(&request_buf, &base, &released) == CLUSTER_PCM_OWN_OK);
		LWLockRelease(BufferDescriptorGetContentLock(&request_buf)); base = released;
		NM_REQUIRE(cluster_bufmgr_pcm_own_begin_x_reservation(&request_buf, &base, &token) == CLUSTER_PCM_OWN_OK);
	} else NM_REQUIRE(cluster_bufmgr_pcm_begin_s_x_reservation(&request_buf, &base, &token) == CLUSTER_PCM_OWN_OK);
	NM_REQUIRE(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1,
		CLUSTER_SEMANTIC_TARGET_SIDE, &admission) == CLUSTER_SEMANTIC_ADMISSION_OK);
	NM_REQUIRE(cluster_bufmgr_pcm_own_acquire_intent_exact(&request_buf, &base, token,
		RESOURCE_X_ACQUIRE_RESIDENT, &admission, NULL, &episode) == CLUSTER_PCM_OWN_OK);
	NM_REQUIRE(resource_x_assertion_init(&base.tag, cluster_node_id, &assertion));
	for (unsigned cut = 0; cut < 14; cut++) {
		ClusterPcmOwnSnapshot wrong = base;
		ResourceXDecodedAcquireIntent bad = episode;
		ClusterPcmResourceXMasterState *state = NULL;
		uint64 r4 = old.r4_record_generation;
		uint32 connection = old.master_ingress_connection_generation;
		NM_REQUIRE(nm_requester_notice(&state));
		if (cut == 0) bad.buffer_id++;
		if (cut == 1) bad.base_generation--;
		if (cut == 2) bad.reservation_token = base.reservation_token;
		if (cut == 3) bad.requester_incarnation++;
		if (cut == 4) bad.r4_generation++;
		if (cut == 5) bad.backend_generation = 0;
		if (cut == 6) wrong.reservation_token--;
		if (cut == 7) wrong.tag.blockNum++;
		if (cut == 8) wrong.flags = PCM_OWN_FLAG_REVOKING;
		if (cut == 9) connection++;
		if (cut == 10) r4++;
		if (cut == 11) { state->requester_settlement_intent.payload[16] ^= 1; }
		if (cut == 12) request_entry()->resource_x_bootstrap_round.resident_s_authorized = 2;
		if (cut == 13) bad.reservation_token++;
		before = request_entry()->resource_x_bootstrap_round;
		NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_round_step_exact(&assertion,
			old.current_master_node, old.resource_formation, old.master_session_incarnation, r4,
			old.requester_sender_connection_generation, connection, fake_pcm_clock_us + 1000000,
			1000000, fake_pcm_clock_us, 1000, &wrong, &bad, &dispatch, &terminal)
			== RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED);
		UT_ASSERT(memcmp(&before, &request_entry()->resource_x_bootstrap_round, sizeof(before)) == 0);
		if (cut == 11) state->requester_settlement_intent.payload[16] ^= 1;
		if (cut == 12) request_entry()->resource_x_bootstrap_round.resident_s_authorized = 1;
	}
	NM_REQUIRE(cluster_pcm_lock_resource_x_bootstrap_round_step_exact(&assertion,
		old.current_master_node, old.resource_formation, old.master_session_incarnation, old.r4_record_generation,
		old.requester_sender_connection_generation, old.master_ingress_connection_generation,
		fake_pcm_clock_us + 1000000, 1000000, fake_pcm_clock_us, 1000, false, 0, &dispatch, &terminal)
		== RESOURCE_X_BOOTSTRAP_ROUND_WAIT);
	NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_round_step_exact(&assertion,
		old.current_master_node, old.resource_formation, old.master_session_incarnation, old.r4_record_generation,
		old.requester_sender_connection_generation, old.master_ingress_connection_generation,
		fake_pcm_clock_us + 1000000, 1000000, fake_pcm_clock_us, 1000, &base, &episode, &dispatch, &terminal)
		== RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(dispatch.common.assertion_sequence, old.highest_attempt_floor + 1);
	NM_REQUIRE(sx_late_seed());
	NM_REQUIRE(cluster_pcm_lock_resource_x_install_claim_join_observe_exact(&assertion,
		old.current_master_node, old.resource_formation, old.master_session_incarnation, old.r4_record_generation,
		old.requester_sender_connection_generation, old.master_ingress_connection_generation, &observation)
		== RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(cluster_pcm_lock_resource_x_first_fill_retire_unsent_exact(&observation, &episode, &issued)
		== RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(cluster_pcm_own_reservation_abort_exact(0, base.generation, token, PCM_OWN_FLAG_GRANT_PENDING) == CLUSTER_PCM_OWN_OK);
	cluster_semantic_activation_leave(&admission);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.phase, 0 /* EMPTY */);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(request_buf.pcm_state, mode == 1 ? PCM_STATE_N : PCM_STATE_S);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), base.generation);
	complete_idle(); return true;
}
UT_TEST(test_resident_S_exact_successor_handoff_rejects_wrong_identity) { nm_run_roles(0, nm_master, sx_identity_parent); }
UT_TEST(test_resident_S_read_successor_cancelled_before_REQUEST) { nm_run_roles(1, nm_master, sx_identity_parent); }
UT_TEST(test_resident_S_to_X_completes_new_request_once) { sx_run(3, 0, false, false, false); }
UT_TEST(test_resident_S_successor_exit_runs_original_LIFO_cleanup)
{
	sx_exiting = true;
	for (unsigned cut = 0; cut < 3; cut++) sx_run(cut, 0, false, false, false);
	sx_exiting = false;
}
UT_TEST(test_S_X_unarmed_ERROR_preserves_installed_S) { sx_run(0, 0, false, false, false); }
UT_TEST(test_S_X_armed_ERROR_preserves_installed_S) { sx_run(1, 0, false, false, false); }
UT_TEST(test_S_X_post_C_ERROR_preserves_exact_X) { sx_run(2, 0, false, false, false); }
UT_TEST(test_S_X_quiet_waits_master_grant_callback) { sx_run(1, 1, false, false, false); }
UT_TEST(test_S_X_installed_waits_master_grant_callback) { sx_run(2, 1, false, false, false); }
UT_TEST(test_S_X_quiet_consumed_before_requester_callback) { sx_run(1, 0, true, false, false); }
UT_TEST(test_S_X_installed_consumed_before_requester_callback) { sx_run(2, 0, true, false, false); }
UT_TEST(test_S_X_failed_consumed_send_retains_master_owner) { sx_run(1, 3, false, false, false); }
UT_TEST(test_S_X_duplicate_quiet_and_installed_receipts) { sx_run(1, 2, true, false, false); sx_run(2, 2, true, false, false); }
UT_TEST(test_S_X_dirty_current_survives_all_ERROR_cuts) { for (unsigned cut = 0; cut < 3; cut++) sx_run(cut, 0, false, true, false); }
UT_TEST(test_S_X_old_unarmed_receipt_cannot_cancel_successor_X) { sx_run(0, 0, false, false, true); }
UT_TEST(test_S_X_old_armed_receipt_cannot_cancel_dirty_successor_X) { sx_run(1, 0, true, true, true); }
int main(void)
{
	UT_PLAN(16);
	UT_RUN(test_S_X_unarmed_ERROR_preserves_installed_S);
	UT_RUN(test_S_X_armed_ERROR_preserves_installed_S);
	UT_RUN(test_S_X_post_C_ERROR_preserves_exact_X);
	UT_RUN(test_S_X_quiet_waits_master_grant_callback);
	UT_RUN(test_S_X_installed_waits_master_grant_callback);
	UT_RUN(test_S_X_quiet_consumed_before_requester_callback);
	UT_RUN(test_S_X_installed_consumed_before_requester_callback);
	UT_RUN(test_S_X_failed_consumed_send_retains_master_owner);
	UT_RUN(test_S_X_duplicate_quiet_and_installed_receipts);
	UT_RUN(test_S_X_dirty_current_survives_all_ERROR_cuts);
	UT_RUN(test_S_X_old_unarmed_receipt_cannot_cancel_successor_X);
	UT_RUN(test_S_X_old_armed_receipt_cannot_cancel_dirty_successor_X);
	UT_RUN(test_resident_S_exact_successor_handoff_rejects_wrong_identity);
	UT_RUN(test_resident_S_read_successor_cancelled_before_REQUEST);
	UT_RUN(test_resident_S_to_X_completes_new_request_once);
	UT_RUN(test_resident_S_successor_exit_runs_original_LIFO_cleanup);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
