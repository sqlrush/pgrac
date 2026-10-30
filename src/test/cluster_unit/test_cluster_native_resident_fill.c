/* Full resident read: actual requester, native IO, disk/image and PCM C.
 * Transport peers, process interrupts and OS lock events are leaves. */
#define NATIVE_RESIDENT_FILL_COMPOSITION
#include "test_cluster_native_resident_fixture.inc"
#undef NATIVE_RESIDENT_FILL_COMPOSITION

static bool rf_enlarge_error;
static void
rf_enlarge(ResourceOwner owner)
{
	ResourceOwnerEnlargeBufferIOs(owner);
	if (rf_enlarge_error) { rf_enlarge_error = false; pg_re_throw(); }
}
#ifndef RESIDENT_FILL_PARENT_RED
#define LockBufHdr stage_header_lock
#define GetPrivateRefCount(buffer) request_private_pin(buffer)
#define LWLockAcquire stage_content_acquire
#define LWLockRelease stage_content_release
#define ereport(level, rest) ordinary_report(level)
#define ResourceOwnerEnlargeBufferIOs rf_enlarge
#include "test_cluster_native_resident_fill.inc"
#undef ResourceOwnerEnlargeBufferIOs
#undef ereport
#undef LWLockRelease
#undef LWLockAcquire
#undef GetPrivateRefCount
#undef LockBufHdr
#endif

static bool
rf_start(ClusterGcsFirstFill *request, unsigned native, int source)
{
	resident_start(request, native); ordinary_profile = source;
	rf_enlarge_error = false;
	/* The request-only prefix resets shared mode after its IO setup.
	 * Physical fill exercises the real shared WAL/KO and image providers. */
	cluster_enabled = cluster_shared_config = true;
	cluster_page_wal_shmem_init();
	if (cluster_gcs_block_resident_read_begin(&request_buf, &request_base, request_token, request)
		!= RESOURCE_X_APPLY_APPLIED || cluster_gcs_block_first_fill_wait(request) != RESOURCE_X_APPLY_APPLIED
		|| claim_get() != RESOURCE_X_APPLY_APPLIED)
		return false;
	/* The independently built authenticated peer resets fixture globals. */
	cluster_enabled = cluster_shared_config = true;
	return cluster_bufmgr_pcm_own_acquire_stage_begin_exact(&request_buf, &request_base,
		request_token, RESOURCE_X_ACQUIRE_RESIDENT, &request_admission, &acquire_claim, &prepared_claim)
		== RESOURCE_X_APPLY_APPLIED;
}
static ResourceXApplyResult
rf_run(void)
{
#ifdef RESIDENT_FILL_PARENT_RED
	return cluster_bufmgr_pcm_own_acquire_fill_image_exact(&request_buf, &request_base,
		&prepared_episode, &request_admission, &prepared_claim);
#else
	return cluster_bufmgr_pcm_own_acquire_fill_resident_exact(&request_buf, &request_base,
		&prepared_episode, &request_admission, &prepared_claim);
#endif
}
static bool
rf_catches(void)
{
	volatile bool caught = false;
	PG_TRY(); { (void)rf_run(); } PG_CATCH(); { caught = true; } PG_END_TRY();
	return caught;
}
static void
rf_installed(int source, unsigned io_attempts)
{
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + 1);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S); UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_SCUR);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0); UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_ERROR | BM_IO_IN_PROGRESS), BM_VALID);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT(memcmp(disk_shared.data, disk_bytes.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(disk_reads, source == 0 ? 1 : 0);
	UT_ASSERT_EQ(complete_io_calls, io_attempts); UT_ASSERT_EQ(complete_commits, 1);
	UT_ASSERT_EQ(complete_broadcasts, 1);
	UT_ASSERT_EQ(ordinary_sends, 1);
}

static ResourceXApplyResult
rf_ack(bool quiet)
{
	ResourceXDecodedFrame frame = done_frame();
	ResourceXWireReject reject;
	uint8 bytes[RESOURCE_X_SOURCE_COMPLETE_V11_BYTES]; uint16 len;
	ClusterICEnvelope env = {0};

	frame.payload_bytes = input_grant.body.acquire_decision.source_kind == RESOURCE_X_ACQUIRE_SOURCE_DISK
		? RESOURCE_X_COMPLETE_V5_BYTES : RESOURCE_X_SOURCE_COMPLETE_V11_BYTES;
	frame.common.outcome = quiet ? RESOURCE_X_OUTCOME_QUIET_NO_IMAGE_RETIRED : RESOURCE_X_OUTCOME_DUPLICATE;
	frame.common.action_node = frame.body.acquire_completion.decision.master_node;
	frame.common.sender_connection_generation = 71;
	if (quiet) frame.body.acquire_completion.native_generation = prepared_episode.base_generation;
	if (!cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &frame,
		bytes, sizeof(bytes), &len, &reject)
		|| !cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
			bytes, len, &frame, &reject)) return RESOURCE_X_APPLY_INVALID;
	env.msg_type = RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE;
	env.source_node_id = 0; env.dest_node_id = 1; env.epoch = 17; env.payload_length = len;
	auth_peer_boot = 31;
	return settle_gcs(&frame, &env);
}

static void
rf_error_after_copy(void)
{
	if ((unsigned char)disk_shared.data[0] != 0xA7
		&& request_entry()->resource_x_bootstrap_round.delivery_executor_purpose == RESOURCE_X_DELIVERY_ACQUIRE_EXECUTING) {
		stage_after_content = NULL;
		pg_re_throw();
	}
}

UT_TEST(test_resident_all_read_sources_complete_current_and_retired_PI_once)
{
	for (unsigned native = 1; native <= 2; native++) for (int source = 0; source <= 3; source += source == 0 ? 2 : 1) {
		ClusterGcsFirstFill request;
		UT_ASSERT(rf_start(&request, native, source));
		RESIDENT_REQUIRE_EQ(rf_run(), RESOURCE_X_APPLY_APPLIED);
		rf_installed(source, 1); cluster_gcs_block_first_fill_end(&request); complete_idle();
	}
}
UT_TEST(test_resident_completion_replay_never_copies_or_completes_IO_twice)
{
	ClusterGcsFirstFill request;
	UT_ASSERT(rf_start(&request, 2, 2)); RESIDENT_REQUIRE_EQ(rf_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(rf_run(), RESOURCE_X_APPLY_DUPLICATE); rf_installed(2, 1);
	cluster_gcs_block_first_fill_end(&request); complete_idle();
}
UT_TEST(test_resident_uncovered_PI_preserves_prepared_owner_and_old_bytes)
{
	ClusterGcsFirstFill request; ClusterPageWalBindingV1 wal;
	ClusterPcmOwnEntry own; uint32 state;
	PGIOAlignedBlock before;
	wal = local_pi_setup(make_tag(850));
	UT_ASSERT(rf_start(&request, 2, 2));
	wal.identity.locator = BufTagGetRelFileLocator(&request_base.tag);
	wal.identity.forknum = request_base.tag.forkNum;
	wal.identity.blockno = request_base.tag.blockNum;
	wal.source.claim.identity.origin_node_id = cluster_node_id;
	wal.source.claim.identity.origin_thread_id = cluster_node_id + 1;
	wal.source.claim.identity.origin_owner_incarnation = cluster_qvotec_get_self_incarnation();
	local_pi_writer = wal.source; local_pi_writer_ready = true;
	if (!cluster_pcm_local_pi_record_first_v1(request_base.tag, &wal, &wal)) {
		UT_ASSERT(false); complete_idle(); return;
	}
	own = request_own; state = pg_atomic_read_u32(&request_buf.state);
	memcpy(before.data, disk_shared.data, BLCKSZ);
	UT_ASSERT_EQ(rf_run(), RESOURCE_X_APPLY_LOCAL_WAIT);
	request_native_unchanged(&own, state);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, RESOURCE_X_DELIVERY_ACQUIRE_PREPARED);
	UT_ASSERT(memcmp(before.data, disk_shared.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), prepared_claim.executor_sequence);
	UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT_EQ(complete_commits, 0);
	cluster_gcs_block_first_fill_end(&request); complete_idle();
}
UT_TEST(test_resident_storage_error_aborts_only_original_new_input_IO)
{
	ClusterGcsFirstFill request;
	UT_ASSERT(rf_start(&request, 2, 0)); disk_read_error = true;
	UT_ASSERT(rf_catches());
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0); UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_ERROR | BM_IO_IN_PROGRESS), BM_IO_ERROR);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, RESOURCE_X_DELIVERY_ACQUIRE_UNARMED_QUIET);
	cluster_gcs_block_first_fill_end(&request); complete_idle();
}
UT_TEST(test_resident_ERROR_after_native_C_rolls_forward_without_abort)
{
	ClusterGcsFirstFill request;
	UT_ASSERT(rf_start(&request, 2, 2)); complete_after_io_error = true;
	UT_ASSERT(rf_catches()); rf_installed(2, 1);
	cluster_gcs_block_first_fill_end(&request); complete_idle();
}
UT_TEST(test_resident_native_refusals_never_change_old_page_or_publish_C)
{
	for (int n = 0; n < 6; n++) {
		ClusterGcsFirstFill request; ClusterPcmOwnEntry own; uint32 state;
		UT_ASSERT(rf_start(&request, 2, 2));
		if (n == 0) request_pins = 0;
		if (n == 1) request_buf.tag.blockNum++;
		if (n == 2) pg_atomic_write_u64(&request_own.reservation_token, request_token + 1);
		if (n == 3) pg_atomic_write_u64(&request_own.write_io_generation, 1);
		if (n == 4) request_proc.cluster_grd_generation++;
		if (n == 5) pg_atomic_fetch_or_u32(&request_buf.state, BM_IO_ERROR);
		own = request_own; state = pg_atomic_read_u32(&request_buf.state);
		UT_ASSERT(rf_run() != RESOURCE_X_APPLY_APPLIED);
		request_native_unchanged(&own, state);
		for (int i = 0; i < BLCKSZ; i++) UT_ASSERT_EQ((unsigned char)disk_shared.data[i], 0xA7);
		UT_ASSERT_EQ(complete_commits, 0);
		cluster_gcs_block_first_fill_end(&request); complete_idle();
	}
}

UT_TEST(test_resident_ERROR_before_input_IO_preserves_old_bytes_and_seals_owner)
{
	for (int source = 0; source <= 3; source += source == 0 ? 2 : 1) {
		ClusterGcsFirstFill request;
		UT_ASSERT(rf_start(&request, 2, source)); rf_enlarge_error = true;
		UT_ASSERT(rf_catches()); resident_unchanged_bytes();
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
			RESOURCE_X_DELIVERY_ACQUIRE_UNARMED_QUIET);
		cluster_gcs_block_first_fill_end(&request); complete_idle();
	}
}

UT_TEST(test_resident_ERROR_after_copy_before_I_leaves_unusable_original_IO_error)
{
	for (int source = 0; source <= 3; source += source == 0 ? 2 : 1) {
		ClusterGcsFirstFill request;
		UT_ASSERT(rf_start(&request, 2, source)); stage_after_content = rf_error_after_copy;
		UT_ASSERT(rf_catches());
		UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation);
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0); UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_IN_PROGRESS | BM_IO_ERROR), BM_IO_ERROR);
		UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
		UT_ASSERT_EQ(complete_commits, 0);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
			RESOURCE_X_DELIVERY_ACQUIRE_UNARMED_QUIET);
		cluster_gcs_block_first_fill_end(&request); complete_idle();
	}
}

UT_TEST(test_resident_ERROR_after_C_before_IO_end_only_rolls_forward)
{
	ClusterGcsFirstFill request;
	UT_ASSERT(rf_start(&request, 2, 0)); complete_before_io_error = true;
	UT_ASSERT(rf_catches()); rf_installed(0, 2);
	cluster_gcs_block_first_fill_end(&request); complete_idle();
}

UT_TEST(test_resident_C_real_LMS_callback_and_authenticated_ACK_retire_in_either_order)
{
	for (int source = 0; source <= 3; source += source == 0 ? 2 : 1) for (int order = 0; order < 2; order++) {
		ClusterGcsFirstFill request; ResourceXIntentSlot slot;
		UT_ASSERT(rf_start(&request, 2, source)); RESIDENT_REQUIRE_EQ(rf_run(), RESOURCE_X_APPLY_APPLIED);
		slot = qn_stage();
		if (order == 0) {
			UT_ASSERT_EQ(rf_ack(false), RESOURCE_X_APPLY_APPLIED);
			UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 1);
			UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
		} else {
			UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
			UT_ASSERT_EQ(rf_ack(false), RESOURCE_X_APPLY_APPLIED);
		}
		/* Notification/receipt debt ends; the bounded resident S provenance
		 * remains until the real release/revoke owner terminates it. */
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 1);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
			RESOURCE_X_DELIVERY_ACQUIRE_READ_SETTLED);
		UT_ASSERT(!qn_probe(&slot));
		rf_installed(source, 1); cluster_gcs_block_first_fill_end(&request); complete_idle();
	}
}

UT_TEST(test_resident_pre_IO_quiet_never_samples_successor_during_real_retirement)
{
	ClusterGcsFirstFill request; ResourceXIntentSlot slot; ClusterPcmOwnEntry successor; uint32 state;
	UT_ASSERT(rf_start(&request, 2, 0)); rf_enlarge_error = true;
	UT_ASSERT(rf_catches()); resident_unchanged_bytes(); slot = qn_stage();
	pg_atomic_write_u64(&request_own.generation, 100);
	pg_atomic_write_u64(&request_own.reservation_token, 101); request_buf.tag.blockNum++;
	successor = request_own; state = pg_atomic_read_u32(&request_buf.state);
	UT_ASSERT_EQ(rf_ack(true), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 0);
	request_native_unchanged(&successor, state);
	cluster_gcs_block_first_fill_end(&request); complete_idle();
}

int
main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
	UT_PLAN(11);
	UT_RUN(test_resident_all_read_sources_complete_current_and_retired_PI_once);
	UT_RUN(test_resident_completion_replay_never_copies_or_completes_IO_twice);
	UT_RUN(test_resident_uncovered_PI_preserves_prepared_owner_and_old_bytes);
	UT_RUN(test_resident_storage_error_aborts_only_original_new_input_IO);
	UT_RUN(test_resident_ERROR_after_native_C_rolls_forward_without_abort);
	UT_RUN(test_resident_native_refusals_never_change_old_page_or_publish_C);
	UT_RUN(test_resident_ERROR_before_input_IO_preserves_old_bytes_and_seals_owner);
	UT_RUN(test_resident_ERROR_after_copy_before_I_leaves_unusable_original_IO_error);
	UT_RUN(test_resident_ERROR_after_C_before_IO_end_only_rolls_forward);
	UT_RUN(test_resident_C_real_LMS_callback_and_authenticated_ACK_retire_in_either_order);
	UT_RUN(test_resident_pre_IO_quiet_never_samples_successor_during_real_retirement);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
