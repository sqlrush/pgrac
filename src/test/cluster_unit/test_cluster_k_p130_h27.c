/* H27: original requester/native refusal owners, with explicit peer, process
 * and storage leaves from the existing resident/ordinary composition.
 * These host components do not qualify the native cluster matrix. */
#include "test_cluster_k_p130_h27_resident.inc"

#if defined(K_P130_EXPECT_ASSERT_OFF) && defined(USE_ASSERT_CHECKING)
#error "H27 assertion-OFF requires an assertion-disabled configuration"
#endif

/* Catch removal of the native guard, an early C, a hidden disk fallback,
 * or retirement of a responsibility which the rejected input still owns. */
static void
h27_trace(const char *cut, unsigned variant)
{
	printf("# H27 %s/%u gen=%llu token=%llu flags=%u delivery=%llu "
		"io=%llu state=%u reads=%u C=%u finish=%u ordinary-sends=%u ring=%u "
		"round=%u purpose=%llu E=%llu epoch=%llu page-crc=%u\n",
		cut, variant, (unsigned long long)cluster_pcm_own_gen_get(0),
		(unsigned long long)cluster_pcm_own_reservation_token_get(0), cluster_pcm_own_flags_get(0),
		(unsigned long long)cluster_pcm_own_delivery_attempt_get(0),
		(unsigned long long)pg_atomic_read_u64(&request_own.write_io_generation),
		pg_atomic_read_u32(&request_buf.state), disk_reads, complete_commits,
		complete_io_calls, ordinary_sends,
		request_shared.rings[1].count,
		request_entry()->resource_x_bootstrap_round.has_acquire_intent,
		(unsigned long long)request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
		(unsigned long long)request_prepared.common.assertion_sequence,
		(unsigned long long)request_entry()->resource_x_bootstrap_round.acquire_intent.cluster_epoch,
		cluster_gcs_block_compute_checksum(disk_shared.data));
}

static void
h27_no_publication(const PGIOAlignedBlock *before)
{
	UT_ASSERT(memcmp(before->data, disk_shared.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT_EQ(complete_commits, 0);
	UT_ASSERT_EQ(complete_io_calls, 0);
}

static void
h27_owner_trace(const char *cut, unsigned variant)
{
	/* The native-only exhaustion cases have no live PCM registry entry. */
	printf("# H27 %s/%u gen=%llu token=%llu flags=%u io=%llu waiters=%u\n",
		cut, variant, (unsigned long long)cluster_pcm_own_gen_get(0),
		(unsigned long long)cluster_pcm_own_reservation_token_get(0),
		cluster_pcm_own_flags_get(0),
		(unsigned long long)pg_atomic_read_u64(&request_own.write_io_generation),
		request_own.service_waiters);
}

UT_TEST(test_h27_PI_without_source_waits_across_elapsed_response_periods)
{
	ClusterGcsFirstFill request;
	ClusterPcmResourceXBootstrapRound before;
	PGIOAlignedBlock page;
	resident_start(&request, 2);
	RESIDENT_REQUIRE_EQ(cluster_gcs_block_resident_read_begin(&request_buf,
		&request_base, request_token, &request), RESOURCE_X_APPLY_APPLIED);
	fake_cv_prepare_hook = NULL;
	before = request_entry()->resource_x_bootstrap_round;
	memcpy(page.data, disk_shared.data, BLCKSZ);
	for (unsigned n = 0; n < 8; n++) {
		fake_pcm_clock_us += UINT64_C(60000000);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_wait_exact(
			&request.observation, &request.episode), RESOURCE_X_APPLY_NOT_FOUND);
		UT_ASSERT(memcmp(&before, &request_entry()->resource_x_bootstrap_round, sizeof(before)) == 0);
		h27_no_publication(&page);
		UT_ASSERT_EQ(ordinary_sends, 1);
	}
	h27_trace("PI/passive-wait", 8);
	/* End the original scope; its admitted shared obligation is retained. */
	cluster_gcs_block_first_fill_end(&request);
	complete_idle();
}

UT_TEST(test_h27_current_and_PI_grant_only_keep_native_bytes_and_owner)
{
	for (unsigned native = 1; native <= 2; native++) {
		EvidenceFixture f;
		ResourceXRequesterJoinSnapshot out;
		PGIOAlignedBlock page;
		UT_ASSERT(resident_seed(&f, 2, native));
		memcpy(page.data, disk_shared.data, BLCKSZ);
		UT_ASSERT_EQ(evidence_offer(&f.decision, &out), RESOURCE_X_APPLY_APPLIED);
		input_refused(RESOURCE_X_APPLY_BAD_STATE);
		h27_no_publication(&page);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 1);
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
		UT_ASSERT_EQ(request_shared.rings[1].count, 1);
		h27_trace("grant-only", native);
		complete_idle();
	}
}

UT_TEST(test_h27_transport_checksum_rejects_before_join_or_native_copy)
{
	EvidenceFixture f;
	ResourceXDecodedFrame decoded, canary;
	ResourceXWireReject reject;
	uint8 bytes[RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES];
	uint16 length = 0;
	PGIOAlignedBlock page;
	ClusterPcmResourceXBootstrapRound round;
	UT_ASSERT(resident_seed(&f, 2, 2));
	memcpy(page.data, disk_shared.data, BLCKSZ);
	round = request_entry()->resource_x_bootstrap_round;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT,
		&f.image, bytes, sizeof(bytes), &length, &reject));
	memset(&decoded, 0xA5, sizeof(decoded)); canary = decoded;
	bytes[length - 1] ^= 1;
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT,
		bytes, length, &decoded, &reject));
	UT_ASSERT(memcmp(&decoded, &canary, sizeof(decoded)) == 0);
	UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
	h27_no_publication(&page);
	h27_trace("wire-checksum", (unsigned)reject);
	complete_idle();
}

UT_TEST(test_h27_full_source_rejects_wrong_native_tag_generation_and_token)
{
	for (unsigned native = 1; native <= 2; native++) for (unsigned field = 0; field < 3; field++) {
		EvidenceFixture f;
		PGIOAlignedBlock page;
		UT_ASSERT(resident_seed(&f, 2, native)); input_complete(&f, 2);
		memcpy(page.data, disk_shared.data, BLCKSZ);
		if (field == 0) request_buf.tag.blockNum++;
		if (field == 1) pg_atomic_write_u64(&request_own.generation, 49);
		if (field == 2) request_token++;
		input_refused(RESOURCE_X_APPLY_STALE);
		h27_no_publication(&page);
		h27_trace("native-identity", native * 10 + field);
		complete_idle();
	}
}

UT_TEST(test_h27_full_source_rejects_attempt_formation_and_connection_drift)
{
	for (unsigned field = 0; field < 9; field++) {
		EvidenceFixture f;
		PGIOAlignedBlock page;
		UT_ASSERT(resident_seed(&f, 2, 2)); input_complete(&f, 2);
		memcpy(page.data, disk_shared.data, BLCKSZ);
		if (field == 0) input_observation.entry_binding_generation++;
		if (field == 1) input_observation.request.assertion_sequence++;
		if (field == 2) input_observation.request.master_session_incarnation++;
		if (field == 3) input_observation.request.resource_formation++;
		if (field == 4) input_observation.r4_record_generation++;
		if (field == 5) input_observation.request.sender_connection_generation++;
		if (field == 6) input_observation.master_ingress_connection_generation++;
		if (field == 7) input_observation.master_node++;
		if (field == 8) input_observation.request.target_mode = PCM_STATE_X;
		input_refused(RESOURCE_X_APPLY_STALE);
		h27_no_publication(&page);
		h27_trace("observation-domain", field);
		complete_idle();
	}
}

UT_TEST(test_h27_native_completion_rechecks_identity_before_input_IO)
{
	static const ResourceXApplyResult wanted[] = {
		RESOURCE_X_APPLY_RECOVERY_BLOCKED, RESOURCE_X_APPLY_RECOVERY_BLOCKED,
		RESOURCE_X_APPLY_LOCAL_WAIT, RESOURCE_X_APPLY_BAD_STATE,
		RESOURCE_X_APPLY_RECOVERY_BLOCKED, RESOURCE_X_APPLY_BAD_STATE,
		RESOURCE_X_APPLY_BAD_STATE
	};
	for (unsigned field = 0; field < lengthof(wanted); field++) {
		ClusterGcsFirstFill request;
		ClusterPcmOwnEntry own;
		PGIOAlignedBlock page;
		uint32 state;
		ResourceXApplyResult result;
		UT_ASSERT(rf_start(&request, 2, 2));
		if (field == 0) request_buf.tag.blockNum++;
		if (field == 1) pg_atomic_write_u64(&request_own.reservation_token, request_token + 1);
		if (field == 2) pg_atomic_write_u64(&request_own.write_io_generation, 1);
		if (field == 3) request_proc.cluster_grd_generation++;
		if (field == 4) pg_atomic_fetch_or_u32(&request_buf.state, BM_IO_ERROR);
		if (field == 5) request_admission.formation_epoch++;
		if (field == 6) prepared_episode.cluster_epoch++;
		own = request_own; state = pg_atomic_read_u32(&request_buf.state);
		memcpy(page.data, disk_shared.data, BLCKSZ);
		result = rf_run();
		/* Failed physical capture retains the prepared owner; odd IO waits.
		 * Wrong process/admission/episode fails before physical capture. */
		UT_ASSERT_EQ(result, wanted[field]);
		request_native_unchanged(&own, state);
		h27_no_publication(&page);
		h27_trace("completion-pre-IO", field);
		cluster_gcs_block_first_fill_end(&request); complete_idle();
	}
}

static void
h27_bad_image(unsigned shape)
{
	ClusterBufmgrFirstFill read;
	PGIOAlignedBlock page;
	ordinary_start(&read, 2);
	memcpy(page.data, disk_shared.data, BLCKSZ);
	if (shape == 0) ((PageHeader)disk_bytes.data)->pd_lower = 1;
	if (shape == 1) ((PageHeader)disk_bytes.data)->pd_upper = BLCKSZ + 1;
	if (shape == 2) ((PageHeader)disk_bytes.data)->pd_special--;
	if (shape == 3) ((PageHeader)disk_bytes.data)->pd_pagesize_version = 0;
	if (shape == 4) ((PageHeader)disk_bytes.data)->pd_flags = PD_SPACE_METADATA;
	if (shape == 5) ((PageHeader)disk_bytes.data)->pd_flags = PD_UNDO_SEG_HEADER;
	if (shape == 6) ((PageHeader)disk_bytes.data)->pd_flags = (uint16)~PD_VALID_FLAG_BITS;
	/* The ordinary peer leaf computes a valid transport checksum over these
	 * malformed bytes, so the real native page-shape guard must reject. */
	UT_ASSERT(!ordinary_run(&read));
	h27_no_publication(&page);
	UT_ASSERT_EQ(ordinary_sends, 1);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_IN_PROGRESS | BM_IO_ERROR), BM_IO_ERROR);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	h27_trace("image-shape", shape); complete_idle();
}

UT_TEST(test_h27_ordinary_remote_image_rejects_malformed_page_with_valid_wire_CRC)
{
	for (unsigned shape = 0; shape < 7; shape++) h27_bad_image(shape);
}

static void
h27_bad_disk(bool checksum)
{
	ClusterBufmgrFirstFill read;
	PGIOAlignedBlock page;
	ordinary_start(&read, 0);
	memcpy(page.data, disk_shared.data, BLCKSZ);
	if (checksum) disk_bytes.data[BLCKSZ - 1] ^= 1;
	else {
		((PageHeader)disk_bytes.data)->pd_upper = 1;
		((PageHeader)disk_bytes.data)->pd_checksum = pg_checksum_page(disk_bytes.data, request_base.tag.blockNum);
	}
	disk_store();
	UT_ASSERT(!ordinary_run(&read));
	/* This guard necessarily follows one authorized disk read. No retry,
	 * shared-page copy, C or native successful finish may follow refusal. */
	UT_ASSERT_EQ(disk_reads, 1); UT_ASSERT_EQ(disk_ends, 1);
	UT_ASSERT_EQ(disk_bad_pages, checksum ? 1 : 0);
	UT_ASSERT_EQ(disk_traces, 1);
	UT_ASSERT(memcmp(page.data, disk_shared.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(complete_commits, 0); UT_ASSERT_EQ(complete_io_calls, 0);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation);
	UT_ASSERT_EQ(ordinary_sends, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_IN_PROGRESS | BM_IO_ERROR), BM_IO_ERROR);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	h27_trace(checksum ? "disk-checksum" : "disk-shape", 0); complete_idle();
}
UT_TEST(test_h27_ordinary_disk_checksum_refusal_cannot_publish_C) { h27_bad_disk(true); }
UT_TEST(test_h27_ordinary_disk_shape_refusal_cannot_publish_C) { h27_bad_disk(false); }

UT_TEST(test_h27_remote_S_and_X_current_keep_legitimate_old_disk_checksum)
{
	for (unsigned source = 2; source <= 3; source++) {
		ClusterBufmgrFirstFill read;
		ordinary_start(&read, source);
		((PageHeader)disk_bytes.data)->pd_checksum ^= UINT16_MAX;
		UT_ASSERT(ordinary_run(&read));
		UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT_EQ(disk_bad_pages, 0);
		UT_ASSERT_EQ(complete_commits, 1); UT_ASSERT_EQ(complete_io_calls, 1);
		UT_ASSERT_EQ(ordinary_sends, 1);
		UT_ASSERT(memcmp(disk_bytes.data, disk_shared.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + 1);
		h27_trace("memory-old-disk-checksum", source); complete_idle();
	}
}

UT_TEST(test_h27_reservation_token_and_generation_exhaustion_have_no_side_effects)
{
	for (unsigned field = 0; field < 2; field++) {
		ClusterBufmgrFirstFill unused;
		ClusterPcmOwnEntry own;
		PGIOAlignedBlock page;
		uint32 state;
		uint64 token = 99;
		resident_environment(&unused);
		pg_atomic_write_u32(&request_own.flags, 0);
		pg_atomic_write_u64(&request_own.generation, field == 0 ? 48 : UINT64_MAX);
		pg_atomic_write_u64(&request_own.reservation_token, field == 0 ? UINT64_MAX : 17);
		own = request_own; state = pg_atomic_read_u32(&request_buf.state);
		memcpy(page.data, disk_shared.data, BLCKSZ);
		/* Actual owner API, under its required native header lock. */
		state = request_header_lock(&request_buf);
		UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, field == 0 ? 48 : UINT64_MAX,
			PCM_OWN_FLAG_GRANT_PENDING, &token), CLUSTER_PCM_OWN_EXHAUSTED);
		UnlockBufHdr(&request_buf, state);
		UT_ASSERT_EQ(token, 0);
		request_native_unchanged(&own, state);
		h27_no_publication(&page);
		h27_owner_trace("reservation-exhaustion", field); complete_idle();
	}
}

UT_TEST(test_h27_write_IO_exhaustion_keeps_original_service_responsibility)
{
	ClusterBufmgrFirstFill unused;
	ClusterPcmOwnEntry own;
	PGIOAlignedBlock page;
	uint32 state;
	resident_environment(&unused);
	pg_atomic_write_u64(&request_own.write_io_generation, UINT64_MAX - 1);
	request_own.service_waiters = 5;
	own = request_own; memcpy(page.data, disk_shared.data, BLCKSZ);
	state = request_header_lock(&request_buf);
	UT_ASSERT_EQ(cluster_pcm_own_write_io_begin(0), CLUSTER_PCM_OWN_EXHAUSTED);
	UnlockBufHdr(&request_buf, state);
	request_native_unchanged(&own, state);
	UT_ASSERT_EQ(request_own.service_waiters, 5);
	h27_no_publication(&page);
	h27_owner_trace("write-IO-exhaustion", 0); complete_idle();
}

int
main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
	UT_PLAN(12);
	UT_RUN(test_h27_PI_without_source_waits_across_elapsed_response_periods);
	UT_RUN(test_h27_current_and_PI_grant_only_keep_native_bytes_and_owner);
	UT_RUN(test_h27_transport_checksum_rejects_before_join_or_native_copy);
	UT_RUN(test_h27_full_source_rejects_wrong_native_tag_generation_and_token);
	UT_RUN(test_h27_full_source_rejects_attempt_formation_and_connection_drift);
	UT_RUN(test_h27_native_completion_rechecks_identity_before_input_IO);
	UT_RUN(test_h27_ordinary_remote_image_rejects_malformed_page_with_valid_wire_CRC);
	UT_RUN(test_h27_ordinary_disk_checksum_refusal_cannot_publish_C);
	UT_RUN(test_h27_ordinary_disk_shape_refusal_cannot_publish_C);
	UT_RUN(test_h27_remote_S_and_X_current_keep_legitimate_old_disk_checksum);
	UT_RUN(test_h27_reservation_token_and_generation_exhaustion_have_no_side_effects);
	UT_RUN(test_h27_write_IO_exhaustion_keeps_original_service_responsibility);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
