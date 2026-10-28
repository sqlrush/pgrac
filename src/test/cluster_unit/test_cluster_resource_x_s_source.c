/* Real master publication, S-source pair producer, codec and send owners.
 * Native snapshot/page capture is an explicit caller seam; these tests do
 * not qualify a BufferDesc transition, source pin, or complete H row. */
#include "test_cluster_resource_x_master_ack_fixture.inc"

typedef struct SSourceFixture {
	PublishFixture master;
	ResourceXDecodedFrame block;
	ClusterPcmOwnSnapshot prepared, finished;
	PGAlignedBlock page;
	int32 source_master;
} SSourceFixture;

static bool
ss_start(SSourceFixture *f)
{
	memset(f, 0, sizeof(*f));
	if (!publish_start(2, PCM_STATE_X, &f->master)) return false;
	publish_apply(&f->master);
	f->block = publish_decode(f->master.state->block_intents[0].payload, true);
	f->prepared.tag = make_tag(850); f->prepared.generation = 901;
	f->prepared.reservation_token = 77; f->prepared.flags = PCM_OWN_FLAG_REVOKING;
	f->prepared.pcm_state = PCM_STATE_S; f->prepared.buffer_type = BUF_TYPE_SCUR;
	f->prepared.semantic_buf_state = BM_VALID;
	f->finished = f->prepared; f->finished.generation++;
	f->finished.pcm_state = PCM_STATE_N; f->finished.buffer_type = BUF_TYPE_PI;
	/* The native page provider is a declared seam, not PageInit/redo. */
	((PageHeader)f->page.data)->pd_lower = SizeOfPageHeaderData;
	((PageHeader)f->page.data)->pd_upper = BLCKSZ;
	((PageHeader)f->page.data)->pd_special = BLCKSZ;
	((PageHeader)f->page.data)->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	PageXLogRecPtrSet(((PageHeader)f->page.data)->pd_lsn, (XLogRecPtr)144);
	((PageHeader)f->page.data)->pd_block_scn = 236;
	return true;
}

static ResourceXApplyResult
ss_prepare(SSourceFixture *f)
{
	ResourceXApplyResult result = cluster_pcm_lock_resource_x_acquire_s_source_prepare_exact(
		&f->block, f->source_master, 7, &f->prepared, f->page.data, NULL, 83, 61);
	UT_ASSERT_EQ(fake_lwlock_depth, 0); return result;
}

static ResourceXApplyResult
ss_publish(SSourceFixture *f)
{
	ResourceXApplyResult result = cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(
		&f->block, f->source_master, 7, &f->prepared, &f->finished);
	UT_ASSERT_EQ(fake_lwlock_depth, 0); return result;
}

static void
ss_unchanged(SSourceFixture *f, bool publish, ResourceXApplyResult expected)
{
	ClusterPcmResourceXMasterState *before = malloc(sizeof(*before));
	UT_ASSERT(before != NULL); if (!before) return;
	memcpy(before, f->master.state, sizeof(*before));
	UT_ASSERT_EQ(publish ? ss_publish(f) : ss_prepare(f), expected);
	UT_ASSERT(memcmp(before, f->master.state, sizeof(*before)) == 0); free(before);
}

static ResourceXDecodedFrame
ss_read(SSourceFixture *f, bool image)
{
	ResourceXDecodedFrame frame = {0};
	ResourceXApplyResult result = image
		? cluster_pcm_lock_resource_x_holder_image_exact(&f->block.common.logical_assertion, &frame)
		: cluster_pcm_lock_resource_x_holder_status_exact(&f->block.common.logical_assertion, &frame);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED); UT_ASSERT(frame.has_acquire_decision);
	return frame;
}

UT_TEST(test_s_source_reserves_complete_pair_before_native_finish)
{
	SSourceFixture f; ResourceXDecodedFrame image, ack; ClusterPcmResourceXMasterState *s;
	if (!ss_start(&f)) return;
	UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED); s = f.master.state;
	UT_ASSERT_EQ(s->holder_status.valid, 1); UT_ASSERT_EQ(s->holder_image.valid, 1);
	UT_ASSERT_EQ(s->holder_status.payload_bytes, RESOURCE_X_SOURCE_WITNESS_V3_BYTES);
	UT_ASSERT_EQ(s->holder_image.payload_bytes, RESOURCE_X_ACQUIRE_IMAGE_V3_BYTES);
	UT_ASSERT_EQ(s->holder_status_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(s->holder_image_intent.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	ack = ss_read(&f, false); image = ss_read(&f, true);
	UT_ASSERT_EQ(ack.body.acquire_witness.original_ack.observed_mode, PCM_STATE_S);
	UT_ASSERT_EQ(ack.body.acquire_witness.original_ack.target_mode, PCM_STATE_N);
	UT_ASSERT_EQ(ack.body.acquire_witness.original_proof.source_carrier_generation, 902);
	UT_ASSERT_EQ(ack.body.acquire_witness.original_ack.sender_connection_generation, 61);
	UT_ASSERT(memcmp(image.body.acquire_image.original_image.page_bytes, f.page.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(image.body.acquire_image.original_image.page_checksum, cluster_gcs_block_compute_checksum(f.page.data));
	UT_ASSERT_EQ(s->requests[1].final_authority_generation, 0);
}

UT_TEST(test_s_source_exact_prepare_duplicate_is_not_a_second_native_commit)
{
	SSourceFixture f; if (!ss_start(&f)) return;
	UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
	ss_unchanged(&f, false, RESOURCE_X_APPLY_DUPLICATE);
}

UT_TEST(test_s_source_changed_token_descriptor_or_page_cannot_replace_pending)
{
	for (int n = 0; n < 3; n++) {
		SSourceFixture f; if (!ss_start(&f)) return;
		UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
		if (n == 0) f.prepared.reservation_token++;
		else if (n == 1) f.page.data[BLCKSZ - 1]++;
		else f.prepared.generation++;
		ss_unchanged(&f, false, RESOURCE_X_APPLY_STALE);
	}
}

UT_TEST(test_s_source_bad_native_shape_is_refused_before_capacity_changes)
{
	for (int n = 0; n < 10; n++) {
		SSourceFixture f; if (!ss_start(&f)) return;
		switch (n) {
		case 0: f.prepared.flags = 0; break;
		case 1: f.prepared.reservation_token = 0; break;
		case 2: f.prepared.reservation_token = UINT64_MAX; break;
		case 3: f.prepared.generation = UINT64_MAX - 1; break;
		case 4: f.prepared.writer_activation_token = 1; break;
		case 5: f.prepared.resource_x_activation_generation = 1; break;
		case 6: f.prepared.semantic_buf_state |= BM_IO_IN_PROGRESS; break;
		case 7: f.prepared.semantic_buf_state |= BM_DIRTY; break;
		case 8: f.prepared.buffer_type = BUF_TYPE_PI; break;
		case 9: f.prepared._reserved[0] = 1; break;
		}
		ss_unchanged(&f, false, RESOURCE_X_APPLY_INVALID);
	}
}

UT_TEST(test_s_source_does_not_relabel_x_downgrade)
{
	for (int scenario = 1; scenario <= 1; scenario++) {
		SSourceFixture f; if (!ss_start(&f)) return;
		if (!publish_start(scenario, PCM_STATE_S, &f.master)) return;
		publish_apply(&f.master);
		f.block = publish_decode(f.master.state->block_intents[0].payload, true);
		ss_unchanged(&f, false, RESOURCE_X_APPLY_INVALID);
	}
}

UT_TEST(test_s_source_full_a_and_native_tag_are_bound)
{
	SSourceFixture f; ResourceXWireReject reject; uint16 len = 0; uint8 bytes[736];
	if (!ss_start(&f)) return;
	f.block.body.acquire_decision.episode.reservation_token++;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCK_TO_N, &f.block, bytes, sizeof(bytes), &len, &reject));
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCK_TO_N, bytes, len, &f.block, &reject));
	ss_unchanged(&f, false, RESOURCE_X_APPLY_STALE);
	if (!ss_start(&f)) return;
	f.prepared.tag.blockNum++; ss_unchanged(&f, false, RESOURCE_X_APPLY_INVALID);
}

UT_TEST(test_s_source_crc_and_original_master_authentication_are_not_optional)
{
	SSourceFixture f; if (!ss_start(&f)) return;
	f.block.common.semantic_crc32c++; ss_unchanged(&f, false, RESOURCE_X_APPLY_INVALID);
	if (!ss_start(&f)) return;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_prepare_exact(&f.block, 2, 7,
		&f.prepared, f.page.data, NULL, 83, 61), RESOURCE_X_APPLY_INVALID);
	UT_ASSERT_EQ(f.master.state->holder_status.valid, 0);
}

UT_TEST(test_s_source_full_original_holder_capacity_refusal_preserves_it)
{
	SSourceFixture f; if (!ss_start(&f)) return;
	f.master.state->holder_status.valid = 1;
	ss_unchanged(&f, false, RESOURCE_X_APPLY_BAD_STATE);
}

UT_TEST(test_s_source_cannot_publish_without_exact_native_transition)
{
	for (int n = 0; n < 7; n++) {
		SSourceFixture f; if (!ss_start(&f)) return;
		UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
		switch (n) {
		case 0: f.finished = f.prepared; break;
		case 1: f.finished.generation++; break;
		case 2: f.finished.reservation_token++; break;
		case 3: f.finished.flags = 0; break;
		case 4: f.finished.buffer_type = BUF_TYPE_CURRENT; break;
		case 5: f.finished.semantic_buf_state |= BM_IO_ERROR; break;
		case 6: f.finished.writer_activation_token = 1; break;
		}
		ss_unchanged(&f, true, RESOURCE_X_APPLY_STALE);
	}
}

UT_TEST(test_s_source_publish_arms_both_original_send_owners_once)
{
	SSourceFixture f; ResourceXIntentSlot status, image;
	if (!ss_start(&f)) return; UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ss_publish(&f), RESOURCE_X_APPLY_APPLIED);
	status = f.master.state->holder_status_intent.slot; image = f.master.state->holder_image_intent;
	UT_ASSERT_EQ(status.state, RESOURCE_X_INTENT_SLOT_ARMED); UT_ASSERT_EQ(image.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(status.destination_node, 0); UT_ASSERT_EQ(image.destination_node, 1);
	UT_ASSERT_EQ(status.payload_bytes, 1048); UT_ASSERT_EQ(image.payload_bytes, 9800);
	ss_unchanged(&f, true, RESOURCE_X_APPLY_DUPLICATE);
	ss_unchanged(&f, false, RESOURCE_X_APPLY_DUPLICATE);
}

UT_TEST(test_s_source_old_publish_and_drain_remain_closed_for_full_a)
{
	SSourceFixture f; ClusterPcmResourceXMasterState *before; uint64 generation = 99;
	if (!ss_start(&f)) return; UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
	before = malloc(sizeof(*before)); UT_ASSERT(before != NULL); if (!before) return;
	memcpy(before, f.master.state, sizeof(*before));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_publish_exact(&f.block.common.logical_assertion,
		f.block.common.assertion_sequence, 0, f.block.common.master_session_incarnation), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_drain_prepare_exact(&f.block.common.logical_assertion,
		f.block.common.assertion_sequence, 0, f.block.common.master_session_incarnation, &generation), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(generation, 0);
	UT_ASSERT(memcmp(before, f.master.state, sizeof(*before)) == 0); free(before);
}

UT_TEST(test_s_source_actual_transport_callback_does_not_retire_semantic_pair)
{
	SSourceFixture f; ResourceXIntentSlot sent, staged;
	if (!ss_start(&f)) return; UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ss_publish(&f), RESOURCE_X_APPLY_APPLIED);
	for (int half = 0; half < 2; half++) {
		sent = half ? f.master.state->holder_image_intent : f.master.state->holder_status_intent.slot;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(&sent, 100001, &staged), RESOURCE_X_INTENT_STAGED);
		UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(&staged));
	}
	UT_ASSERT_EQ(f.master.state->holder_status.valid, 2); UT_ASSERT_EQ(f.master.state->holder_image.valid, 2);
	ss_unchanged(&f, true, RESOURCE_X_APPLY_DUPLICATE);
}

UT_TEST(test_s_source_master_consumes_the_produced_original_proof_independently)
{
	SSourceFixture f; ResourceXDecodedFrame ack; ResourceXMasterSnapshot out;
	if (!ss_start(&f)) return; UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ss_publish(&f), RESOURCE_X_APPLY_APPLIED); ack = ss_read(&f, false);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(&ack, 0, &out), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.master.state->requests[1].proof_kind, RESOURCE_X_PROOF_REMOTE_CARRIER);
	UT_ASSERT_EQ(f.master.state->requests[1].final_authority_generation, 0);
	UT_ASSERT_EQ(f.master.state->requests[1].blocked_holders_bitmap, 1);
}

UT_TEST(test_s_source_cancellation_cannot_clear_no_cancel_through_pair)
{
	for (int published = 0; published < 2; published++) {
		SSourceFixture f; ClusterPcmResourceXMasterState *before;
		if (!ss_start(&f)) return; UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
		if (published) UT_ASSERT_EQ(ss_publish(&f), RESOURCE_X_APPLY_APPLIED);
		before = malloc(sizeof(*before)); UT_ASSERT(before != NULL); if (!before) return;
		memcpy(before, f.master.state, sizeof(*before));
		cluster_pcm_lock_resource_x_bootstrap_forget_exact(&f.master.work);
		UT_ASSERT(memcmp(before, f.master.state, sizeof(*before)) == 0); free(before);
	}
}

UT_TEST(test_s_source_membership_freeze_retains_exact_native_debt)
{
	for (int remote = 0; remote < 2; remote++) for (int published = 0; published < 2; published++) {
		SSourceFixture f; ResourceXReconfigToken token; ResourceXReconfigBatch batch;
		ResourceXReconfigResult result = RESOURCE_X_RECONFIG_MORE;
		if (!ss_start(&f)) return;
		if (remote) {
			ResourceXWireReject reject; uint16 len = 0; uint8 bytes[RESOURCE_X_DECISION_V3_BYTES];
			/* Authenticated remote-M forward is the input seam. Remove the
			 * colocated canonical entirely: R8 must inspect this source debt. */
			f.source_master = 1; f.block.body.acquire_decision.master_node = 1;
			f.block.common.master_session_incarnation = 777;
			UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCK_TO_N, &f.block, bytes, sizeof(bytes), &len, &reject));
			UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCK_TO_N, bytes, len, &f.block, &reject));
			acquire_reset(); fake_gcs_master_node = 1;
		}
		UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
		if (remote) {
			ResourceXDecodedFrame ack = ss_read(&f, false); ResourceXWireReject reject;
			uint16 len = 0; uint8 bytes[RESOURCE_X_SOURCE_WITNESS_V3_BYTES]; unsigned matches = 0;
			unsigned char *memory = (unsigned char *)fake_pcm_header.data;
			Size offset = offsetof(ClusterPcmResourceXMasterState, holder_status) + offsetof(ClusterPcmResourceXHolderStatus, payload);
			UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCKED_TO_N, &ack, bytes, sizeof(bytes), &len, &reject));
			f.master.state = NULL;
			for (Size n = offset; n + len <= fake_pcm_header_requested_size; n += 8)
				if (memcmp(memory + n, bytes, len) == 0) { matches++; f.master.state = (ClusterPcmResourceXMasterState *)(memory + n - offset); }
			UT_ASSERT_EQ(matches, 1); if (matches != 1) return;
			UT_ASSERT_EQ(f.master.state->requests[1].phase, RESOURCE_X_MASTER_NONE);
		}
		if (published) UT_ASSERT_EQ(ss_publish(&f), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &token));
		for (int scan = 0; scan < 8 && (result == RESOURCE_X_RECONFIG_MORE || result == RESOURCE_X_RECONFIG_RETRY); scan++)
			result = cluster_resource_x_reconfig_sweep(&token, 4, &batch);
		UT_ASSERT_EQ(result, RESOURCE_X_RECONFIG_ORPHAN);
		UT_ASSERT_EQ(f.master.state->holder_status.valid, published ? 2 : 1);
		UT_ASSERT_EQ(f.master.state->holder_image.valid, published ? 2 : 1);
		ss_unchanged(&f, true, RESOURCE_X_APPLY_RECOVERY_BLOCKED);
		ss_unchanged(&f, false, RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	}
}

UT_TEST(test_s_source_corrupt_half_is_not_published_or_replaced)
{
	for (int n = 0; n < 3; n++) {
		SSourceFixture f; if (!ss_start(&f)) return; UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
		if (n == 0) f.master.state->holder_image.valid = 0;
		else if (n == 1) f.master.state->holder_status.payload[800]++;
		else f.master.state->holder_image.payload[9000]++;
		ss_unchanged(&f, true, RESOURCE_X_APPLY_RECOVERY_BLOCKED);
		ss_unchanged(&f, false, RESOURCE_X_APPLY_RECOVERY_BLOCKED);
		{
			ResourceXDecodedFrame out;
			UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_status_exact(&f.block.common.logical_assertion, &out), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
			UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_image_exact(&f.block.common.logical_assertion, &out), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
		}
	}
}

UT_TEST(test_s_source_ack_sender_rebind_and_descriptor_identity_are_exact)
{
	SSourceFixture f; if (!ss_start(&f)) return; UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_prepare_exact(&f.block, 0, 8,
		&f.prepared, f.page.data, NULL, 83, 61), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_prepare_exact(&f.block, 0, 7,
		&f.prepared, f.page.data, NULL, 83, 62), RESOURCE_X_APPLY_STALE);
}

UT_TEST(test_s_source_bad_ids_and_connection_generations_do_not_reserve)
{
	SSourceFixture f; if (!ss_start(&f)) return;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_prepare_exact(&f.block, 0, -1,
		&f.prepared, f.page.data, NULL, 83, 61), RESOURCE_X_APPLY_INVALID);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_prepare_exact(&f.block, 0, 7,
		&f.prepared, f.page.data, NULL, 0, 61), RESOURCE_X_APPLY_INVALID);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_prepare_exact(&f.block, 0, 7,
		&f.prepared, f.page.data, NULL, 83, UINT32_MAX), RESOURCE_X_APPLY_INVALID);
	UT_ASSERT_EQ(f.master.state->holder_status.valid, 0);
}

UT_TEST(test_s_source_master_directory_does_not_change_at_reserve_or_publish)
{
	SSourceFixture f; PcmAuthoritySnapshot before, after;
	if (!ss_start(&f)) return;
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(make_tag(850), &before));
	UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED); UT_ASSERT_EQ(ss_publish(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(make_tag(850), &after));
	UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
	UT_ASSERT_EQ(f.master.state->holder_pi_first.source_flags, 0);
}

UT_TEST(test_s_source_invalid_wal_projection_cannot_produce_a_pair)
{
	SSourceFixture f; ClusterPageWalBindingV1 wal = {0}; if (!ss_start(&f)) return;
	wal.record_start = 123;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_prepare_exact(&f.block, 0, 7,
		&f.prepared, f.page.data, &wal, 83, 61), RESOURCE_X_APPLY_INVALID);
	UT_ASSERT_EQ(f.master.state->holder_status.valid, 0);
}

static ResourceXDecodedFrame
ss_forward_reencode(ResourceXDecodedFrame frame)
{
	uint8 bytes[RESOURCE_X_DECISION_V3_BYTES]; uint16 len = 0; ResourceXWireReject reject;
	ResourceXDecodedFrame out = {0};
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCK_TO_N, &frame, bytes, sizeof(bytes), &len, &reject));
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCK_TO_N, bytes, len, &out, &reject));
	return out;
}

static ResourceXApplyResult
ss_snapshot(SSourceFixture *f, int32 *id, ClusterPcmOwnSnapshot *native,
	ResourceXDecodedFrame *image, ResourceXAcquireSSourceState *state)
{
	ResourceXApplyResult r = cluster_pcm_lock_resource_x_acquire_s_source_snapshot_exact(
		&f->block, f->source_master, id, native, image, state);
	UT_ASSERT_EQ(fake_lwlock_depth, 0); return r;
}

UT_TEST(test_s_source_snapshot_copies_original_pending_without_mutation)
{
	SSourceFixture f; ClusterPcmOwnSnapshot native; ResourceXDecodedFrame image;
	int32 id = -1; ResourceXAcquireSSourceState state = 0; ClusterPcmResourceXMasterState *before;
	if (!ss_start(&f)) return;
	UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
	before = malloc(sizeof(*before)); UT_ASSERT(before != NULL); if (!before) return;
	memcpy(before, f.master.state, sizeof(*before));
	UT_ASSERT_EQ(ss_snapshot(&f, &id, &native, &image, &state), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(id, 7); UT_ASSERT_EQ(state, RESOURCE_X_HOLDER_PAIR_PENDING);
	UT_ASSERT(cluster_pcm_own_snapshot_equal_exact(&native, &f.prepared));
	UT_ASSERT(image.has_acquire_decision);
	UT_ASSERT(memcmp(image.body.acquire_image.original_image.page_bytes, f.page.data, BLCKSZ) == 0);
	UT_ASSERT(memcmp(before, f.master.state, sizeof(*before)) == 0); free(before);
}

UT_TEST(test_s_source_snapshot_published_pair_survives_transport_drain)
{
	SSourceFixture f; ClusterPcmOwnSnapshot native; ResourceXDecodedFrame image;
	int32 id; ResourceXAcquireSSourceState state;
	if (!ss_start(&f)) return;
	UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ss_publish(&f), RESOURCE_X_APPLY_APPLIED);
	for (int half = 0; half < 2; half++) {
		ResourceXIntentSlot sent = half ? f.master.state->holder_image_intent : f.master.state->holder_status_intent.slot;
		ResourceXIntentSlot staged;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(&sent, 100001, &staged), RESOURCE_X_INTENT_STAGED);
		UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(&staged));
	}
	UT_ASSERT_EQ(f.master.state->holder_status_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(f.master.state->holder_image_intent.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(ss_snapshot(&f, &id, &native, &image, &state), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(state, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
	UT_ASSERT_EQ(native.generation, f.prepared.generation);
	UT_ASSERT_EQ(f.master.state->holder_acquire_s_native.flags, PCM_OWN_FLAG_REVOKING);
}

UT_TEST(test_s_source_snapshot_rejects_changed_forward_and_preserves_outputs)
{
	for (int n = 0; n < 3; n++) {
		SSourceFixture f; ClusterPcmOwnSnapshot native, before;
		ResourceXDecodedFrame image, before_image; int32 id = -51; ResourceXAcquireSSourceState state = 93;
		if (!ss_start(&f)) return;
		UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
		memset(&native, 0x63, sizeof(native)); before = native;
		memset(&image, 0x27, sizeof(image)); before_image = image;
		if (n == 0) f.block.common.semantic_crc32c++;
		else if (n == 1) f.source_master = 1;
		else { f.block.common.sender_connection_generation++; f.block = ss_forward_reencode(f.block); }
		UT_ASSERT(ss_snapshot(&f, &id, &native, &image, &state) != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(id, -51); UT_ASSERT_EQ(state, 93);
		UT_ASSERT(memcmp(&native, &before, sizeof(native)) == 0);
		UT_ASSERT(memcmp(&image, &before_image, sizeof(image)) == 0);
		UT_ASSERT_EQ(f.master.state->holder_status.valid, RESOURCE_X_HOLDER_PAIR_PENDING);
	}
}

UT_TEST(test_s_source_snapshot_gate_freeze_does_not_forget_native_debt)
{
	SSourceFixture f; ClusterPcmOwnSnapshot native; ResourceXDecodedFrame image;
	int32 id = -1; ResourceXAcquireSSourceState state = 0; if (!ss_start(&f)) return;
	UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
	{ ResourceXReconfigToken token; UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &token)); }
	UT_ASSERT_EQ(ss_snapshot(&f, &id, &native, &image, &state), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(id, -1); UT_ASSERT_EQ(state, 0);
	UT_ASSERT_EQ(f.master.state->holder_status.valid, RESOURCE_X_HOLDER_PAIR_PENDING);
	UT_ASSERT(cluster_pcm_own_snapshot_equal_exact(&f.master.state->holder_acquire_s_native, &f.prepared));
}

UT_TEST(test_s_source_snapshot_corrupt_pair_is_not_adopted)
{
	for (int n = 0; n < 3; n++) {
		SSourceFixture f; ClusterPcmOwnSnapshot native; ResourceXDecodedFrame image;
		int32 id = -1; ResourceXAcquireSSourceState state = 0; if (!ss_start(&f)) return;
		UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
		if (n == 0) f.master.state->holder_image.payload[700] ^= 1;
		else if (n == 1) f.master.state->holder_acquire_s_native.reservation_token = 0;
		else f.master.state->holder_acquire_s_forward_connection = 1;
		UT_ASSERT_EQ(ss_snapshot(&f, &id, &native, &image, &state), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(id, -1); UT_ASSERT_EQ(state, 0);
		UT_ASSERT_EQ(f.master.state->holder_status.valid, RESOURCE_X_HOLDER_PAIR_PENDING);
	}
}

UT_TEST(test_s_source_snapshot_requires_every_output_and_original_reservation)
{
	SSourceFixture f; ClusterPcmOwnSnapshot native; ResourceXDecodedFrame image;
	int32 id = -1; ResourceXAcquireSSourceState state = 0; if (!ss_start(&f)) return;
	UT_ASSERT_EQ(ss_snapshot(&f, &id, &native, &image, &state), RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_snapshot_exact(&f.block, 0, NULL, &native, &image, &state), RESOURCE_X_APPLY_INVALID);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_snapshot_exact(&f.block, 0, &id, NULL, &image, &state), RESOURCE_X_APPLY_INVALID);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_snapshot_exact(&f.block, 0, &id, &native, NULL, &state), RESOURCE_X_APPLY_INVALID);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_snapshot_exact(&f.block, 0, &id, &native, &image, NULL), RESOURCE_X_APPLY_INVALID);
	UT_ASSERT_EQ(id, -1); UT_ASSERT_EQ(state, 0);
}

static ResourceXApplyResult
ss_service_prepare(SSourceFixture *f, uint64 r4)
{
	return cluster_pcm_lock_resource_x_acquire_s_source_service_prepare_exact(
		&f->block, f->source_master, 7, &f->prepared, f->page.data, NULL, 83, 61, r4, 61);
}

static bool
ss_service_probe(ResourceXSourceDrainWork *work)
{
	BackendType saved = MyBackendType;
	ResourceXIntentSlot slot;
	ResourceXAcquisitionRef delivery;
	uint8 payload[RESOURCE_X_ACQUIRE_IMAGE_V3_BYTES];
	uint32 examined;
	bool found = false;

	MyBackendType = B_LMS;
	for (int calls = 0; calls < 256; calls++) {
		ResourceXIntentProbeResult r = cluster_pcm_lock_resource_x_outbound_work_probe_exact(
			4, &slot, payload, sizeof(payload), &examined, &delivery);
		UT_ASSERT(r != RESOURCE_X_INTENT_PROBE_CORRUPT);
		if (r == RESOURCE_X_INTENT_PROBE_SOURCE_DRAIN) {
			memcpy(work, payload, sizeof(*work)); found = true; break;
		}
		if (r == RESOURCE_X_INTENT_PROBE_COMPLETE
			&& cluster_pcm_lock_resource_x_outbound_wait_timeout(1000L) == 0) continue;
		if (r == RESOURCE_X_INTENT_PROBE_IDLE || r == RESOURCE_X_INTENT_PROBE_COMPLETE) break;
	}
	MyBackendType = saved; UT_ASSERT_EQ(fake_lwlock_depth, 0); return found;
}

static ResourceXSourceDrainWork *
ss_service_live(const ResourceXSourceDrainWork *work)
{
	ResourceXSourceDrainWork *found = NULL;
	unsigned matches = 0;

	for (long entry = 0; entry < fake_pcm_entry_count; entry++)
		for (Size n = 0; n + sizeof(*work) <= fake_pcm_entrysize; n += 8)
			if (memcmp(fake_pcm_entries.data[entry] + n, work, sizeof(*work)) == 0) {
				found = (ResourceXSourceDrainWork *)(fake_pcm_entries.data[entry] + n); matches++;
			}
	UT_ASSERT_EQ(matches, 1); return matches == 1 ? found : NULL;
}

UT_TEST(test_s_source_service_atomically_queues_same_full_forward)
{
	SSourceFixture f; ResourceXSourceDrainWork work; ResourceXDecodedFrame block;
	if (!ss_start(&f)) return;
	UT_ASSERT_EQ(ss_service_prepare(&f, 77), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.master.state->holder_status.valid, RESOURCE_X_HOLDER_PAIR_PENDING);
	UT_ASSERT_EQ(f.master.state->holder_status_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT(ss_service_probe(&work));
	UT_ASSERT_EQ(work.r4_record_generation, 77); UT_ASSERT_EQ(work.master_node, 0);
	UT_ASSERT_EQ(work.authenticated_connection_generation, 61);
	UT_ASSERT_EQ(work.request.sender_connection_generation, f.block.common.sender_connection_generation);
	UT_ASSERT_EQ(work.attempt_generation & 1, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_drain_snapshot_exact(&work, &block), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(block.has_acquire_decision);
	UT_ASSERT(memcmp(&block, &f.block, sizeof(block)) == 0);
}

UT_TEST(test_s_source_service_excludes_parallel_claim_and_busy_self_requeue)
{
	SSourceFixture f; ResourceXSourceDrainWork work, next;
	if (!ss_start(&f)) return;
	UT_ASSERT_EQ(ss_service_prepare(&f, 77), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(ss_service_probe(&work));
	cluster_pcm_lock_resource_x_source_drain_content_released();
	UT_ASSERT(!ss_service_probe(&next)); /* An event cannot steal a running claim. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&work, RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(ss_service_probe(&next)); /* The release during the attempt survives. */
	UT_ASSERT(next.attempt_generation > work.attempt_generation);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&next, RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(!ss_service_probe(&work)); /* No new release, no busy loop. */
}

UT_TEST(test_s_source_service_release_after_busy_resumes_without_wire)
{
	SSourceFixture f; ResourceXSourceDrainWork work, next;
	if (!ss_start(&f)) return;
	UT_ASSERT_EQ(ss_service_prepare(&f, 77), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(ss_service_probe(&work));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&work, RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(!ss_service_probe(&next));
	cluster_pcm_lock_resource_x_source_drain_content_released();
	UT_ASSERT(ss_service_probe(&next));
	UT_ASSERT(next.attempt_generation > work.attempt_generation);
	UT_ASSERT_EQ(f.master.state->holder_status_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(f.master.state->holder_image_intent.state, RESOURCE_X_INTENT_SLOT_EMPTY);
}

UT_TEST(test_s_source_service_success_needs_real_pair_publication)
{
	SSourceFixture f; ResourceXSourceDrainWork work; ResourceXSourceDrainWork *live;
	if (!ss_start(&f)) return;
	UT_ASSERT_EQ(ss_service_prepare(&f, 77), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(ss_service_probe(&work)); live = ss_service_live(&work); if (!live) return;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&work, RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(live->r4_record_generation, 77);
	UT_ASSERT_EQ(f.master.state->holder_status.valid, RESOURCE_X_HOLDER_PAIR_PENDING);
}

UT_TEST(test_s_source_service_success_retires_work_not_source_native_or_pi)
{
	SSourceFixture f; ResourceXSourceDrainWork work, next; ResourceXSourceDrainWork *live;
	if (!ss_start(&f)) return;
	UT_ASSERT_EQ(ss_service_prepare(&f, 77), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(ss_service_probe(&work)); live = ss_service_live(&work); if (!live) return;
	UT_ASSERT_EQ(ss_publish(&f), RESOURCE_X_APPLY_APPLIED); /* Native tuple is the declared seam. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&work, RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(live->r4_record_generation, 0); UT_ASSERT_EQ(live->attempt_generation & 1, 0);
	UT_ASSERT_EQ(f.master.state->holder_status.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
	UT_ASSERT_EQ(f.master.state->holder_acquire_s_native.reservation_token, f.prepared.reservation_token);
	UT_ASSERT_EQ(f.master.state->holder_status_intent.slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
	cluster_pcm_lock_resource_x_source_drain_content_released(); UT_ASSERT(!ss_service_probe(&next));
}

UT_TEST(test_s_source_service_caller_cancel_does_not_discard_lms_work)
{
	SSourceFixture f; ResourceXSourceDrainWork work; ResourceXDecodedFrame block;
	if (!ss_start(&f)) return;
	UT_ASSERT_EQ(ss_service_prepare(&f, 77), RESOURCE_X_APPLY_APPLIED);
	cluster_pcm_lock_resource_x_bootstrap_forget_exact(&f.master.work);
	UT_ASSERT(ss_service_probe(&work));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_drain_snapshot_exact(&work, &block), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&block, &f.block, sizeof(block)) == 0);
}

UT_TEST(test_s_source_service_membership_refusal_keeps_original_work)
{
	SSourceFixture f; ResourceXSourceDrainWork work; ResourceXSourceDrainWork *live;
	ResourceXDecodedFrame block; ResourceXReconfigToken token;
	if (!ss_start(&f)) return;
	UT_ASSERT_EQ(ss_service_prepare(&f, 77), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(ss_service_probe(&work)); live = ss_service_live(&work); if (!live) return;
	UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &token));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&work, RESOURCE_X_APPLY_STALE), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(live->r4_record_generation, 77);
	UT_ASSERT_EQ(f.master.state->holder_status.valid, RESOURCE_X_HOLDER_PAIR_PENDING);
	work = *live; work.attempt_generation++;
	memset(&block, 0x35, sizeof(block));
	UT_ASSERT(cluster_pcm_lock_resource_x_acquire_s_source_drain_snapshot_exact(&work, &block) != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(((uint8 *)&block)[0], 0x35);
}

UT_TEST(test_s_source_service_old_claim_cannot_complete_successor_attempt)
{
	SSourceFixture f; ResourceXSourceDrainWork work, next; ResourceXSourceDrainWork *live;
	if (!ss_start(&f)) return;
	UT_ASSERT_EQ(ss_service_prepare(&f, 77), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(ss_service_probe(&work));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&work, RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
	cluster_pcm_lock_resource_x_source_drain_content_released(); UT_ASSERT(ss_service_probe(&next));
	live = ss_service_live(&next); if (!live) return;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&work, RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(memcmp(live, &next, sizeof(next)) == 0);
}

UT_TEST(test_s_source_service_snapshot_requires_exact_claim_and_full_common)
{
	SSourceFixture f; ResourceXSourceDrainWork work, bad; ResourceXDecodedFrame block;
	if (!ss_start(&f)) return;
	UT_ASSERT_EQ(ss_service_prepare(&f, 77), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(ss_service_probe(&work));
	for (int n = 0; n < 3; n++) {
		bad = work;
		if (n == 0) bad.attempt_generation++;
		else if (n == 1) bad.request.sender_connection_generation++;
		else bad.request.ordered_lane++;
		memset(&block, 0x35, sizeof(block));
		UT_ASSERT(cluster_pcm_lock_resource_x_acquire_s_source_drain_snapshot_exact(&bad, &block) != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(((uint8 *)&block)[0], 0x35);
	}
}

UT_TEST(test_s_source_service_r4_and_duplicate_registration_are_exact)
{
	SSourceFixture f; ResourceXSourceDrainWork work;
	if (!ss_start(&f)) return;
	UT_ASSERT_EQ(ss_service_prepare(&f, 0), RESOURCE_X_APPLY_INVALID);
	UT_ASSERT_EQ(ss_service_prepare(&f, UINT64_MAX), RESOURCE_X_APPLY_INVALID);
	UT_ASSERT_EQ(f.master.state->holder_status.valid, 0);
	UT_ASSERT_EQ(ss_service_prepare(&f, 77), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ss_service_prepare(&f, 77), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(ss_service_prepare(&f, 78), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(ss_service_probe(&work)); UT_ASSERT_EQ(work.r4_record_generation, 77);
}

UT_TEST(test_s_source_service_does_not_silently_adopt_unowned_component_pair)
{
	SSourceFixture f; ResourceXSourceDrainWork work;
	if (!ss_start(&f)) return;
	UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ss_service_prepare(&f, 77), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(!ss_service_probe(&work));
	UT_ASSERT_EQ(f.master.state->holder_status.valid, RESOURCE_X_HOLDER_PAIR_PENDING);
}

UT_TEST(test_s_source_service_release_before_registration_cannot_strand_first_attempt)
{
	SSourceFixture f; ResourceXSourceDrainWork work;
	if (!ss_start(&f)) return;
	UT_ASSERT(!ss_service_probe(&work)); /* Finish an older empty scan. */
	cluster_pcm_lock_resource_x_source_drain_content_released();
	UT_ASSERT_EQ(ss_service_prepare(&f, 77), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(ss_service_probe(&work));
	UT_ASSERT_EQ(work.r4_record_generation, 77);
}

UT_TEST(test_s_source_service_error_returns_only_claim_and_preserves_native_debt)
{
	SSourceFixture f; ResourceXSourceDrainWork work, next; ResourceXSourceDrainWork *live;
	if (!ss_start(&f)) return;
	UT_ASSERT_EQ(ss_service_prepare(&f, 77), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(ss_service_probe(&work)); live = ss_service_live(&work); if (!live) return;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&work, RESOURCE_X_APPLY_RECOVERY_BLOCKED), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(live->r4_record_generation, 77); UT_ASSERT_EQ(live->attempt_generation & 1, 0);
	UT_ASSERT_EQ(f.master.state->holder_status.valid, RESOURCE_X_HOLDER_PAIR_PENDING);
	UT_ASSERT(cluster_pcm_own_snapshot_equal_exact(&f.master.state->holder_acquire_s_native, &f.prepared));
	UT_ASSERT(!ss_service_probe(&next));
	cluster_pcm_lock_resource_x_source_drain_content_released();
	UT_ASSERT(ss_service_probe(&next)); UT_ASSERT(next.attempt_generation > work.attempt_generation);
}

UT_TEST(test_s_source_service_reconfiguration_preserves_claimed_and_parked_work)
{
	for (int claimed = 0; claimed < 2; claimed++) {
		SSourceFixture f; ResourceXSourceDrainWork work; ResourceXSourceDrainWork *live;
		ResourceXReconfigToken token; ResourceXReconfigBatch batch;
		ResourceXReconfigResult result = RESOURCE_X_RECONFIG_MORE;
		if (!ss_start(&f)) return;
		UT_ASSERT_EQ(ss_service_prepare(&f, 77), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(ss_service_probe(&work)); live = ss_service_live(&work); if (!live) return;
		if (!claimed) {
			UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&work, RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
			work = *live;
		}
		UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &token));
		for (int scan = 0; scan < 8 && (result == RESOURCE_X_RECONFIG_MORE || result == RESOURCE_X_RECONFIG_RETRY); scan++)
			result = cluster_resource_x_reconfig_sweep(&token, 4, &batch);
		UT_ASSERT_EQ(result, RESOURCE_X_RECONFIG_ORPHAN);
		UT_ASSERT(memcmp(live, &work, sizeof(work)) == 0);
		UT_ASSERT_EQ(f.master.state->holder_status.valid, RESOURCE_X_HOLDER_PAIR_PENDING);
		UT_ASSERT(cluster_pcm_own_snapshot_equal_exact(&f.master.state->holder_acquire_s_native, &f.prepared));
	}
}

UT_TEST(test_s_source_service_two_busy_claims_cannot_manufacture_release_events)
{
	SSourceFixture f, other;
	ResourceXSourceDrainWork a, b, a_next, b_next, extra;
	if (!ss_start(&f)) return;
	/* Authenticated remote-M inputs are explicit seams. Two actual original
	 * entries/pairs/claims run below; no synthetic completion owner. */
	f.source_master = 1; f.block.body.acquire_decision.master_node = 1;
	f.block.common.master_session_incarnation = 777;
	f.block = ss_forward_reencode(f.block);
	other = f; other.block.common.logical_assertion.resource.blockNum++;
	other.prepared.tag = other.block.common.logical_assertion.resource;
	other.block = ss_forward_reencode(other.block);
	acquire_reset(); fake_gcs_master_node = 1;
	UT_ASSERT_EQ(ss_service_prepare(&f, 77), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ss_service_prepare(&other, 77), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(ss_service_probe(&a)); UT_ASSERT(ss_service_probe(&b));
	UT_ASSERT(!BufferTagsEqual(&a.request.logical_assertion.resource, &b.request.logical_assertion.resource));
	cluster_pcm_lock_resource_x_source_drain_content_released(); /* Only real event. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&a, RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(ss_service_probe(&a_next));
	UT_ASSERT(BufferTagsEqual(&a.request.logical_assertion.resource, &a_next.request.logical_assertion.resource));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&b, RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(ss_service_probe(&b_next));
	UT_ASSERT(BufferTagsEqual(&b.request.logical_assertion.resource, &b_next.request.logical_assertion.resource));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&a_next, RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&b_next, RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(!ss_service_probe(&extra)); /* Neither completion is a new release. */
}

/* Native capture remains an explicit input seam in this PCM component. */

static ResourceXApplyResult
ss_forward(SSourceFixture *f)
{
	return cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(&f->block, f->source_master, 77, 61);
}

static ResourceXApplyResult
ss_capture(SSourceFixture *f, const ResourceXSourceDrainWork *work)
{
	return cluster_pcm_lock_resource_x_acquire_s_source_capture_exact(work, 7,
		&f->prepared, f->page.data, NULL, 83, 61);
}

UT_TEST(test_s_source_forward_reserves_full_a_before_any_native_tuple)
{
	SSourceFixture f; ResourceXSourceDrainWork work; ResourceXDecodedFrame retained;
	ClusterPcmOwnSnapshot empty = {0}; ResourceXWireReject reject;
	if (!ss_start(&f)) return;
	UT_ASSERT_EQ(ss_forward(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.master.state->holder_status.valid, 3);
	UT_ASSERT_EQ(f.master.state->holder_status.kind, RESOURCE_X_WIRE_BLOCK_TO_N);
	UT_ASSERT_EQ(f.master.state->holder_status.payload_bytes, RESOURCE_X_DECISION_V3_BYTES);
	UT_ASSERT_EQ(f.master.state->holder_image.valid, 0);
	UT_ASSERT_EQ(f.master.state->holder_status_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(f.master.state->holder_image_intent.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT(memcmp(&empty, &f.master.state->holder_acquire_s_native, sizeof(empty)) == 0);
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCK_TO_N, f.master.state->holder_status.payload,
		f.master.state->holder_status.payload_bytes, &retained, &reject));
	UT_ASSERT_EQ(retained.common.semantic_crc32c, f.block.common.semantic_crc32c);
	UT_ASSERT(ss_service_probe(&work));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_drain_snapshot_exact(&work, &retained), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&retained.body.acquire_decision, &f.block.body.acquire_decision, sizeof(retained.body.acquire_decision)) == 0);
}

UT_TEST(test_s_source_forward_duplicate_and_changed_domain_cannot_replace_it)
{
	SSourceFixture f; ClusterPcmResourceXMasterState before;
	if (!ss_start(&f)) return; UT_ASSERT_EQ(ss_forward(&f), RESOURCE_X_APPLY_APPLIED);
	before = *f.master.state; UT_ASSERT_EQ(ss_forward(&f), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(&f.block, 0, 78, 61), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(&f.block, 0, 77, 62), RESOURCE_X_APPLY_STALE);
	f.block.body.acquire_decision.episode.reservation_token++;
	f.block = ss_forward_reencode(f.block);
	UT_ASSERT_EQ(ss_forward(&f), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(memcmp(&before, f.master.state, sizeof(before)) == 0);
}

UT_TEST(test_s_source_forward_bad_auth_r4_and_crc_never_reserve)
{
	for (int n = 0; n < 5; n++) {
		SSourceFixture f; if (!ss_start(&f)) return;
		if (n == 4) f.block.common.semantic_crc32c++;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(&f.block, 0,
			n == 0 ? 0 : n == 1 ? UINT64_MAX : 77,
			n == 2 ? 0 : n == 3 ? UINT32_MAX : 61), RESOURCE_X_APPLY_INVALID);
		UT_ASSERT_EQ(f.master.state->holder_status.valid, 0);
	}
}

UT_TEST(test_s_source_forward_cannot_overwrite_any_existing_holder_capacity)
{
	for (int n = 0; n < 4; n++) {
		SSourceFixture f; ClusterPcmResourceXMasterState before;
		if (!ss_start(&f)) return;
		if (n == 0) f.master.state->holder_status.valid = 1;
		else if (n == 1) f.master.state->holder_image.valid = 1;
		else if (n == 2) f.master.state->holder_status_intent.slot.state = RESOURCE_X_INTENT_SLOT_ARMED;
		else f.master.state->holder_image_intent.state = RESOURCE_X_INTENT_SLOT_STAGED;
		before = *f.master.state;
		UT_ASSERT_EQ(ss_forward(&f), RESOURCE_X_APPLY_BAD_STATE);
		UT_ASSERT(memcmp(&before, f.master.state, sizeof(before)) == 0);
	}
}

UT_TEST(test_s_source_forward_snapshot_has_explicit_no_native_phase)
{
	SSourceFixture f; ResourceXDecodedFrame image; ClusterPcmOwnSnapshot native, empty = {0};
	int32 id = 99; ResourceXAcquireSSourceState state = 0;
	if (!ss_start(&f)) return; UT_ASSERT_EQ(ss_forward(&f), RESOURCE_X_APPLY_APPLIED);
	memset(&image, 0x6a, sizeof(image)); memset(&native, 0x6a, sizeof(native));
	UT_ASSERT_EQ(ss_snapshot(&f, &id, &native, &image, &state), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(state, 3); UT_ASSERT_EQ(id, -1);
	UT_ASSERT(memcmp(&native, &empty, sizeof(native)) == 0);
	UT_ASSERT_EQ(image.kind, 0); UT_ASSERT(!image.has_acquire_decision);
	UT_ASSERT_EQ(ss_publish(&f), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(f.master.state->holder_image.valid, 0);
}

UT_TEST(test_s_source_forward_busy_waits_real_event_without_dropping_frame)
{
	SSourceFixture f; ResourceXSourceDrainWork work, next;
	if (!ss_start(&f)) return; UT_ASSERT_EQ(ss_forward(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(ss_service_probe(&work)); UT_ASSERT(!ss_service_probe(&next));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&work, RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(!ss_service_probe(&next)); UT_ASSERT_EQ(f.master.state->holder_status.valid, 3);
	cluster_pcm_lock_resource_x_source_drain_content_released(); UT_ASSERT(ss_service_probe(&next));
	UT_ASSERT(next.attempt_generation > work.attempt_generation);
	UT_ASSERT_EQ(next.request.semantic_crc32c, work.request.semantic_crc32c);
}

UT_TEST(test_s_source_forward_cancel_and_unsent_forget_cannot_erase_work)
{
	SSourceFixture f; ResourceXSourceDrainWork work; ClusterPcmResourceXMasterState before;
	if (!ss_start(&f)) return; UT_ASSERT_EQ(ss_forward(&f), RESOURCE_X_APPLY_APPLIED);
	before = *f.master.state;
	cluster_pcm_lock_resource_x_bootstrap_forget_exact(&f.master.work);
	UT_ASSERT(memcmp(&before, f.master.state, sizeof(before)) == 0);
	UT_ASSERT(ss_service_probe(&work)); UT_ASSERT_EQ(work.r4_record_generation, 77);
}

UT_TEST(test_s_source_forward_cannot_be_adopted_by_unclaimed_prepare)
{
	SSourceFixture f; ClusterPcmResourceXMasterState before;
	if (!ss_start(&f)) return; UT_ASSERT_EQ(ss_forward(&f), RESOURCE_X_APPLY_APPLIED);
	before = *f.master.state;
	UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(ss_service_prepare(&f, 77), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(memcmp(&before, f.master.state, sizeof(before)) == 0);
}

UT_TEST(test_s_source_forward_exact_capture_atomically_hands_native_to_same_work)
{
	SSourceFixture f; ResourceXSourceDrainWork work, *live; ResourceXDecodedFrame restored;
	if (!ss_start(&f)) return; UT_ASSERT_EQ(ss_forward(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(ss_service_probe(&work)); live = ss_service_live(&work); if (!live) return;
	UT_ASSERT_EQ(ss_capture(&f, &work), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.master.state->holder_status.valid, RESOURCE_X_HOLDER_PAIR_PENDING);
	UT_ASSERT_EQ(f.master.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PENDING);
	UT_ASSERT(memcmp(live, &work, sizeof(work)) == 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_drain_snapshot_exact(&work, &restored), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ss_capture(&f, &work), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(ss_publish(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&work, RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(live->r4_record_generation, 0);
	UT_ASSERT_EQ(f.master.state->holder_status.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
}

UT_TEST(test_s_source_forward_old_claim_cannot_capture_successor_attempt)
{
	SSourceFixture f; ResourceXSourceDrainWork old, next; ClusterPcmResourceXMasterState before;
	if (!ss_start(&f)) return; UT_ASSERT_EQ(ss_forward(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(ss_service_probe(&old));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&old, RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
	cluster_pcm_lock_resource_x_source_drain_content_released(); UT_ASSERT(ss_service_probe(&next));
	before = *f.master.state; UT_ASSERT_EQ(ss_capture(&f, &old), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(memcmp(&before, f.master.state, sizeof(before)) == 0);
	UT_ASSERT_EQ(ss_capture(&f, &next), RESOURCE_X_APPLY_APPLIED);
}

UT_TEST(test_s_source_forward_stale_domain_cannot_handoff_or_forget)
{
	SSourceFixture f; ResourceXSourceDrainWork work; ClusterPcmResourceXMasterState before;
	if (!ss_start(&f)) return; UT_ASSERT_EQ(ss_forward(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(ss_service_probe(&work)); before = *f.master.state; fake_gcs_master_node = 2;
	UT_ASSERT(ss_capture(&f, &work) != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&before, f.master.state, sizeof(before)) == 0);
}

UT_TEST(test_s_source_forward_corruption_is_not_a_pair_or_claim)
{
	for (int n = 0; n < 5; n++) {
		SSourceFixture f; ResourceXSourceDrainWork work; ResourceXDecodedFrame restored;
		if (!ss_start(&f)) return; UT_ASSERT_EQ(ss_forward(&f), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(ss_service_probe(&work));
		switch (n) {
		case 0: f.master.state->holder_status.payload[500] ^= 1; break;
		case 1: f.master.state->holder_acquire_s_native.generation = 9; break;
		case 2: f.master.state->holder_image.valid = 1; break;
		case 3: f.master.state->holder_status.destination_node++; break;
		case 4: f.master.state->holder_status_intent.slot.state = RESOURCE_X_INTENT_SLOT_ARMED; break;
		}
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_drain_snapshot_exact(&work, &restored), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
		UT_ASSERT(ss_capture(&f, &work) != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(f.master.state->holder_status.valid, 3);
	}
}

UT_TEST(test_s_source_forward_remote_master_r8_keeps_full_frame_orphan)
{
	for (int claimed = 0; claimed < 2; claimed++) {
		SSourceFixture f; ResourceXSourceDrainWork work, *live; ResourceXReconfigToken token;
		ResourceXReconfigBatch batch; ResourceXReconfigResult result = RESOURCE_X_RECONFIG_MORE;
		if (!ss_start(&f)) return;
		f.source_master = 1; f.block.body.acquire_decision.master_node = 1;
		f.block.common.master_session_incarnation = 777; f.block = ss_forward_reencode(f.block);
		acquire_reset(); fake_gcs_master_node = 1;
		UT_ASSERT_EQ(ss_forward(&f), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(ss_service_probe(&work)); live = ss_service_live(&work); if (!live) return;
		if (!claimed) {
			UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&work, RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
			work = *live;
		}
		UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &token));
		for (int n = 0; n < 8 && (result == RESOURCE_X_RECONFIG_MORE || result == RESOURCE_X_RECONFIG_RETRY); n++)
			result = cluster_resource_x_reconfig_sweep(&token, 4, &batch);
		UT_ASSERT_EQ(result, RESOURCE_X_RECONFIG_ORPHAN);
		UT_ASSERT(memcmp(live, &work, sizeof(work)) == 0);
	}
}

UT_TEST(test_s_source_forward_capture_bad_native_or_descriptor_preserves_original)
{
	for (int n = 0; n < 3; n++) {
		SSourceFixture f; ResourceXSourceDrainWork work; ClusterPcmResourceXMasterState before;
		if (!ss_start(&f)) return; UT_ASSERT_EQ(ss_forward(&f), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(ss_service_probe(&work)); before = *f.master.state;
		if (n == 0) f.prepared.reservation_token = 0;
		else if (n == 1) f.prepared.semantic_buf_state |= BM_DIRTY;
		else f.prepared.tag.blockNum++;
		UT_ASSERT(ss_capture(&f, &work) != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&before, f.master.state, sizeof(before)) == 0);
	}
}

/* Full-A/native inputs are explicit seams; actual original record, claim,
 * codec, pair and independent send owners run for this compatible mode. */
static bool
ss_compatible_start(SSourceFixture *f)
{
	if (!ss_start(f) || !publish_start(2, PCM_STATE_S, &f->master)) return false;
	publish_apply(&f->master);
	f->block = publish_decode(f->master.state->block_intents[0].payload, true);
	f->finished = f->prepared; f->finished.flags = 0;
	return true;
}

UT_TEST(test_compatible_s_source_keeps_s_generation_and_seals_real_pair)
{
	SSourceFixture f; ResourceXSourceDrainWork work; ResourceXDecodedFrame ack, image;
	if (!ss_compatible_start(&f)) return;
	UT_ASSERT_EQ(ss_forward(&f), RESOURCE_X_APPLY_APPLIED); UT_ASSERT(ss_service_probe(&work));
	UT_ASSERT_EQ(work.request.target_mode, PCM_STATE_S);
	UT_ASSERT_EQ(ss_capture(&f, &work), RESOURCE_X_APPLY_APPLIED);
	ack = ss_read(&f, false); image = ss_read(&f, true);
	UT_ASSERT_EQ(ack.body.acquire_witness.original_ack.target_mode, PCM_STATE_S);
	UT_ASSERT_EQ(ack.body.acquire_witness.original_ack.flags, 0);
	UT_ASSERT_EQ(ack.body.acquire_witness.original_proof.source_carrier_generation, f.prepared.generation);
	UT_ASSERT_EQ(image.body.acquire_image.original_image.source_carrier_generation, f.prepared.generation);
	UT_ASSERT(memcmp(image.body.acquire_image.original_image.page_bytes, f.page.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(ss_publish(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.master.state->holder_status_intent.slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(f.master.state->holder_image_intent.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&work, RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.master.state->holder_acquire_s_native.generation, f.prepared.generation);
	UT_ASSERT_EQ(f.master.state->holder_acquire_s_native.pcm_state, PCM_STATE_S);
}

UT_TEST(test_compatible_s_source_rejects_wrong_native_completion_without_mutation)
{
	for (int n = 0; n < 7; n++) {
		SSourceFixture f; if (!ss_compatible_start(&f)) return;
		UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
		if (n == 0) f.finished = f.prepared;
		else if (n == 1) f.finished.generation++;
		else if (n == 2) f.finished.reservation_token++;
		else if (n == 3) f.finished.pcm_state = PCM_STATE_N;
		else if (n == 4) f.finished.buffer_type = BUF_TYPE_PI;
		else if (n == 5) f.finished.semantic_buf_state |= BM_IO_ERROR;
		else f.finished.writer_activation_token = 1;
		ss_unchanged(&f, true, RESOURCE_X_APPLY_STALE);
	}
}

UT_TEST(test_compatible_s_source_exact_replay_never_rearms_or_demotes)
{
	SSourceFixture f; if (!ss_compatible_start(&f)) return;
	UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ss_publish(&f), RESOURCE_X_APPLY_APPLIED);
	ss_unchanged(&f, false, RESOURCE_X_APPLY_DUPLICATE);
	ss_unchanged(&f, true, RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(f.master.state->requests[1].final_authority_generation, 0);
}

UT_TEST(test_compatible_s_source_busy_preserves_same_forward_until_real_event)
{
	SSourceFixture f; ResourceXSourceDrainWork work, next; ResourceXDecodedFrame block;
	if (!ss_compatible_start(&f)) return;
	UT_ASSERT_EQ(ss_forward(&f), RESOURCE_X_APPLY_APPLIED); UT_ASSERT(ss_service_probe(&work));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&work, RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(!ss_service_probe(&next)); cluster_pcm_lock_resource_x_source_drain_content_released();
	UT_ASSERT(ss_service_probe(&next)); UT_ASSERT(next.attempt_generation > work.attempt_generation);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_drain_snapshot_exact(&next, &block), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&block, &f.block, sizeof(block)) == 0);
	UT_ASSERT_EQ(ss_capture(&f, &work), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(ss_capture(&f, &next), RESOURCE_X_APPLY_APPLIED);
}

UT_TEST(test_compatible_s_source_bad_native_shape_and_holder_capacity_stay_closed)
{
	SSourceFixture f; if (!ss_compatible_start(&f)) return;
	f.prepared.semantic_buf_state |= BM_IO_ERROR; ss_unchanged(&f, false, RESOURCE_X_APPLY_INVALID);
	if (!ss_compatible_start(&f)) return;
	f.master.state->holder_image.valid = 1; ss_unchanged(&f, false, RESOURCE_X_APPLY_BAD_STATE);
}

UT_TEST(test_compatible_s_source_reconfiguration_retains_original_pair_and_work)
{
	SSourceFixture f; ResourceXSourceDrainWork work; ResourceXReconfigToken token;
	ResourceXReconfigBatch batch; ResourceXReconfigResult result = RESOURCE_X_RECONFIG_MORE;
	if (!ss_compatible_start(&f)) return;
	UT_ASSERT_EQ(ss_service_prepare(&f, 77), RESOURCE_X_APPLY_APPLIED); UT_ASSERT(ss_service_probe(&work));
	UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &token));
	for (int n = 0; n < 8 && (result == RESOURCE_X_RECONFIG_MORE || result == RESOURCE_X_RECONFIG_RETRY); n++)
		result = cluster_resource_x_reconfig_sweep(&token, 4, &batch);
	UT_ASSERT_EQ(result, RESOURCE_X_RECONFIG_ORPHAN);
	UT_ASSERT_EQ(f.master.state->holder_status.valid, RESOURCE_X_HOLDER_PAIR_PENDING);
	UT_ASSERT(cluster_pcm_own_snapshot_equal_exact(&f.master.state->holder_acquire_s_native, &f.prepared));
}

UT_TEST(test_compatible_s_component_crc_does_not_enable_standalone_v2_s)
{
	SSourceFixture f; ResourceXDecodedFrame full, legacy = {0}; ResourceXWireReject reject;
	uint8 bytes[RESOURCE_X_IMAGE_V2_BYTES]; uint16 len = 0; uint32 crc = 0;
	if (!ss_compatible_start(&f)) return;
	UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED); full = ss_read(&f, true);
	UT_ASSERT(cluster_resource_x_wire_acquire_image_crc(&full.body.acquire_image, &crc, &reject));
	UT_ASSERT_EQ(crc, full.body.acquire_image.original_common.semantic_crc32c);
	legacy.kind = RESOURCE_X_WIRE_IMAGE_ENVELOPE;
	legacy.common = full.body.acquire_image.original_common;
	legacy.body.image_envelope = full.body.acquire_image.original_image;
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &legacy,
		bytes, sizeof(bytes), &len, &reject));
	full.body.acquire_image.original_common.target_mode = PCM_STATE_N; crc = 123;
	UT_ASSERT(!cluster_resource_x_wire_acquire_image_crc(&full.body.acquire_image, &crc, &reject));
	UT_ASSERT_EQ(crc, 0);
}

UT_TEST(test_compatible_s_completion_preserves_later_hints_but_not_io_error)
{
	SSourceFixture f; if (!ss_compatible_start(&f)) return;
	UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
	f.finished.semantic_buf_state |= BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED;
	UT_ASSERT_EQ(ss_publish(&f), RESOURCE_X_APPLY_APPLIED);
	ss_unchanged(&f, true, RESOURCE_X_APPLY_DUPLICATE);
	f.finished.semantic_buf_state |= BM_IO_ERROR;
	ss_unchanged(&f, true, RESOURCE_X_APPLY_STALE);
}


/* The native capture input is the same declared seam as the source cases.
 * Full A is produced by the real master, then delivered to its other holder. */
static bool
sh_start(SSourceFixture *f)
{
	if (!ss_start(f)) return false;
	f->block = publish_decode(f->master.state->block_intents[2].payload, true);
	/* A different instance has its own original GRD/owner, not M's slots. */
	acquire_reset();
	cluster_node_id = 2; fake_gcs_master_node = 0;
	ut_master_session = f->block.body.acquire_decision.holders[2].boot_incarnation;
	return true;
}

static ResourceXDecodedFrame
sh_direct(SSourceFixture *f)
{
	int32 id = -1; ClusterPcmOwnSnapshot native; ResourceXAcquireSSourceState state;
	ResourceXDecodedFrame direct = {0};
	UT_ASSERT_EQ(ss_snapshot(f, &id, &native, &direct, &state), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(id, 7); UT_ASSERT_EQ(direct.kind, RESOURCE_X_WIRE_BLOCKED_TO_N);
	return direct;
}

UT_TEST(test_s_holder_retains_original_forward_and_no_source_image)
{
	SSourceFixture f; ResourceXSourceDrainWork work; ResourceXDecodedFrame frame;
	if (!sh_start(&f)) return;
	UT_ASSERT_EQ(ss_forward(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(ss_service_probe(&work));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_drain_snapshot_exact(&work, &frame), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(frame.body.acquire_decision.source_node, 0);
	UT_ASSERT_EQ(frame.common.action_node, 2);
	UT_ASSERT_EQ(f.master.state->holder_image.valid, 0);
}

UT_TEST(test_s_holder_reserves_two_control_witnesses_before_native_finish)
{
	SSourceFixture f; ResourceXSourceDrainWork work; ResourceXDecodedFrame ack, direct;
	if (!sh_start(&f)) return;
	UT_ASSERT_EQ(ss_forward(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(ss_service_probe(&work));
	UT_ASSERT_EQ(ss_capture(&f, &work), RESOURCE_X_APPLY_APPLIED);
	ack = ss_read(&f, false); direct = sh_direct(&f);
	UT_ASSERT_EQ(f.master.state->holder_status.payload_bytes, RESOURCE_X_WITNESS_V3_BYTES);
	UT_ASSERT_EQ(f.master.state->holder_image.payload_bytes, RESOURCE_X_WITNESS_V3_BYTES);
	UT_ASSERT_EQ(direct.kind, RESOURCE_X_WIRE_BLOCKED_TO_N);
	UT_ASSERT(!ack.body.acquire_witness.has_source_proof);
	UT_ASSERT_EQ(ack.body.acquire_witness.original_ack.target_mode, PCM_STATE_N);
	UT_ASSERT_EQ(ack.body.acquire_witness.original_ack.flags, 0);
	UT_ASSERT(memcmp(&ack.body.acquire_witness, &direct.body.acquire_witness, sizeof(ack.body.acquire_witness)) == 0);
	UT_ASSERT_EQ(f.master.state->holder_status_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(f.master.state->holder_image_intent.state, RESOURCE_X_INTENT_SLOT_EMPTY);
}

UT_TEST(test_s_holder_publish_requires_exact_n_pi_and_never_a_source_proof)
{
	SSourceFixture f; if (!sh_start(&f)) return;
	UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
	f.finished = f.prepared; ss_unchanged(&f, true, RESOURCE_X_APPLY_STALE);
	f.finished.generation++; f.finished.pcm_state = PCM_STATE_N; f.finished.buffer_type = BUF_TYPE_PI;
	UT_ASSERT_EQ(ss_publish(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.master.state->holder_status_intent.slot.payload_bytes, RESOURCE_X_WITNESS_V3_BYTES);
	UT_ASSERT_EQ(f.master.state->holder_image_intent.payload_bytes, RESOURCE_X_WITNESS_V3_BYTES);
	ss_unchanged(&f, true, RESOURCE_X_APPLY_DUPLICATE);
}

UT_TEST(test_s_holder_snapshot_rejects_original_forward_connection_drift)
{
	SSourceFixture f; int32 id; ClusterPcmOwnSnapshot native;
	ResourceXDecodedFrame frame; ResourceXAcquireSSourceState state;
	if (!sh_start(&f)) return; UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ss_snapshot(&f, &id, &native, &frame, &state), RESOURCE_X_APPLY_APPLIED);
	f.block.common.sender_connection_generation++; f.block = ss_forward_reencode(f.block);
	UT_ASSERT_EQ(ss_snapshot(&f, &id, &native, &frame, &state), RESOURCE_X_APPLY_STALE);
	ss_unchanged(&f, false, RESOURCE_X_APPLY_STALE);
}

UT_TEST(test_s_holder_transport_callbacks_leave_original_native_and_pair_debt)
{
	SSourceFixture f; int32 id; ClusterPcmOwnSnapshot native;
	ResourceXDecodedFrame frame; ResourceXAcquireSSourceState state;
	if (!sh_start(&f)) return; UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ss_publish(&f), RESOURCE_X_APPLY_APPLIED);
	for (int half = 0; half < 2; half++) {
		ResourceXIntentSlot sent = half ? f.master.state->holder_image_intent : f.master.state->holder_status_intent.slot;
		ResourceXIntentSlot staged;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(&sent, 100001, &staged), RESOURCE_X_INTENT_STAGED);
		UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(&staged));
	}
	UT_ASSERT_EQ(ss_snapshot(&f, &id, &native, &frame, &state), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(state, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
	UT_ASSERT_EQ(native.flags, PCM_OWN_FLAG_REVOKING);
}

UT_TEST(test_s_holder_busy_service_keeps_same_work_until_actual_event)
{
	SSourceFixture f; ResourceXSourceDrainWork work, next;
	if (!sh_start(&f)) return; UT_ASSERT_EQ(ss_forward(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(ss_service_probe(&work)); UT_ASSERT_EQ(ss_capture(&f, &work), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&work, RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(!ss_service_probe(&next));
	cluster_pcm_lock_resource_x_source_drain_content_released();
	UT_ASSERT(ss_service_probe(&next)); UT_ASSERT_EQ(ss_publish(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&next, RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(sh_direct(&f).kind, RESOURCE_X_WIRE_BLOCKED_TO_N);
}

UT_TEST(test_s_holder_wrong_role_or_native_identity_cannot_reserve)
{
	SSourceFixture f;
	if (!sh_start(&f)) return;
	f.prepared.flags = 0; ss_unchanged(&f, false, RESOURCE_X_APPLY_INVALID);
	if (!sh_start(&f)) return;
	f.block.common.action_node = 3; f.block = ss_forward_reencode(f.block);
	ss_unchanged(&f, false, RESOURCE_X_APPLY_INVALID);
}

UT_TEST(test_s_holder_membership_freeze_preserves_pair_as_orphan)
{
	SSourceFixture f; ResourceXReconfigToken token; ResourceXReconfigBatch batch;
	ResourceXReconfigResult result = RESOURCE_X_RECONFIG_MORE;
	if (!sh_start(&f)) return; UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &token));
	for (int n = 0; n < 8 && (result == RESOURCE_X_RECONFIG_MORE || result == RESOURCE_X_RECONFIG_RETRY); n++)
		result = cluster_resource_x_reconfig_sweep(&token, 4, &batch);
	UT_ASSERT_EQ(result, RESOURCE_X_RECONFIG_ORPHAN);
	UT_ASSERT_EQ(f.master.state->holder_status.valid, RESOURCE_X_HOLDER_PAIR_PENDING);
}


UT_TEST(test_s_holder_ack_is_consumed_only_by_original_master_independently)
{
	SSourceFixture holder, master; ResourceXDecodedFrame ack; ResourceXMasterSnapshot out;
	if (!sh_start(&holder)) return;
	UT_ASSERT_EQ(ss_prepare(&holder), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ss_publish(&holder), RESOURCE_X_APPLY_APPLIED); ack = ss_read(&holder, false);
	/* Recreate the deterministic original M fixture, not its H local GRD. */
	if (!ss_start(&master)) return;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(&ack, 2, &out), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(master.master.state->requests[1].blocked_holders_bitmap, UINT32_C(1) << 2);
	UT_ASSERT_EQ(master.master.state->requests[1].proof_kind, 0);
	UT_ASSERT_EQ(master.master.state->requests[1].final_authority_generation, 0);
}

UT_TEST(test_s_holder_cancellation_cannot_erase_native_or_direct_witness)
{
	SSourceFixture f; ClusterPcmResourceXMasterState *before;
	if (!sh_start(&f)) return; UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
	before = malloc(sizeof(*before)); UT_ASSERT(before != NULL); if (!before) return;
	*before = *f.master.state; cluster_pcm_lock_resource_x_bootstrap_forget_exact(&f.master.work);
	UT_ASSERT(memcmp(before, f.master.state, sizeof(*before)) == 0); free(before);
	UT_ASSERT_EQ(sh_direct(&f).kind, RESOURCE_X_WIRE_BLOCKED_TO_N);
}

/* X-to-S uses the real conditional producer and native tuple input seam. */
static bool
xs_start(SSourceFixture *f)
{
	if (!ss_start(f) || !publish_start(1, PCM_STATE_S, &f->master)) return false;
	publish_apply(&f->master);
	f->block = publish_decode(f->master.state->block_intents[0].payload, true);
	f->prepared.pcm_state = PCM_STATE_X; f->prepared.buffer_type = BUF_TYPE_XCUR;
	f->finished = f->prepared; f->finished.generation++;
	f->finished.pcm_state = PCM_STATE_S; f->finished.buffer_type = BUF_TYPE_SCUR; f->finished.flags = 0;
	return true;
}
UT_TEST(test_x_to_s_pair_retained_before_physical_commit)
{
	SSourceFixture f; ResourceXDecodedFrame ack, image;
	if (!xs_start(&f)) return;
	UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
	ack = ss_read(&f, false); image = ss_read(&f, true);
	UT_ASSERT_EQ(ack.body.acquire_witness.original_ack.observed_mode, PCM_STATE_X);
	UT_ASSERT_EQ(ack.body.acquire_witness.original_ack.target_mode, PCM_STATE_S);
	UT_ASSERT_EQ(ack.body.acquire_witness.original_ack.flags, RESOURCE_X_COMMON_FLAG_PI_ESTABLISHED);
	UT_ASSERT_EQ(ack.body.acquire_witness.original_proof.source_carrier_generation, f.prepared.generation + 1);
	UT_ASSERT_EQ(image.body.acquire_image.original_image.source_fence[28], PCM_STATE_X);
	UT_ASSERT(memcmp(image.body.acquire_image.original_image.page_bytes, f.page.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(f.master.state->holder_image_intent.state, RESOURCE_X_INTENT_SLOT_EMPTY);
}
UT_TEST(test_x_to_s_pair_publish_and_replay_are_exact)
{
	SSourceFixture f; if (!xs_start(&f)) return;
	UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
	ss_unchanged(&f, false, RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(ss_publish(&f), RESOURCE_X_APPLY_APPLIED);
	ss_unchanged(&f, true, RESOURCE_X_APPLY_DUPLICATE);
}
UT_TEST(test_x_to_s_pair_rejects_mode_activation_dirt_or_wrong_native_finish)
{
	for (int n = 0; n < 5; n++) {
		SSourceFixture f; if (!xs_start(&f)) return;
		if (n == 0) f.prepared.pcm_state = PCM_STATE_S;
		if (n == 1) f.prepared.writer_activation_token = 19;
		if (n == 2) f.prepared.resource_x_activation_generation = 19;
		if (n == 3) f.prepared.semantic_buf_state |= BM_DIRTY;
		if (n < 4) ss_unchanged(&f, false, RESOURCE_X_APPLY_INVALID);
		else {
			UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED);
			f.finished.generation--; ss_unchanged(&f, true, RESOURCE_X_APPLY_STALE);
		}
	}
}
UT_TEST(test_x_to_s_original_forward_and_lms_claim_survive_capture)
{
	SSourceFixture f; ResourceXSourceDrainWork work; ResourceXDecodedFrame block;
	if (!xs_start(&f)) return;
	UT_ASSERT_EQ(ss_forward(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(ss_service_probe(&work));
	UT_ASSERT_EQ(ss_capture(&f, &work), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_drain_snapshot_exact(&work, &block), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(block.body.acquire_decision.directory_mode, PCM_STATE_X);
	UT_ASSERT_EQ(ss_publish(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&work, RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_APPLIED);
}
UT_TEST(test_x_to_s_original_master_independently_consumes_source_proof)
{
	SSourceFixture f; ResourceXDecodedFrame ack; ResourceXMasterSnapshot out;
	if (!xs_start(&f)) return;
	UT_ASSERT_EQ(ss_prepare(&f), RESOURCE_X_APPLY_APPLIED); UT_ASSERT_EQ(ss_publish(&f), RESOURCE_X_APPLY_APPLIED);
	ack = ss_read(&f, false);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(&ack, 0, &out), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.master.state->requests[1].final_authority_generation, 0);
}


/* An X current source transfers to a different X requester under the same
 * full-A pair owner. Native snapshots/pages remain explicit input seams. */
static bool
xx_start(SSourceFixture *f)
{
	if (!ss_start(f) || !publish_start(1, PCM_STATE_X, &f->master)) return false;
	publish_apply(&f->master);
	f->block = publish_decode(f->master.state->block_intents[0].payload, true);
	f->prepared.pcm_state = PCM_STATE_X; f->prepared.buffer_type = BUF_TYPE_XCUR;
	f->finished = f->prepared; f->finished.generation++;
	f->finished.pcm_state = PCM_STATE_N; f->finished.buffer_type = BUF_TYPE_PI;
	return true;
}

UT_TEST(test_x_current_source_reserves_bound_image_before_native_n_pi)
{
	SSourceFixture f; ResourceXDecodedFrame ack, image; ResourceXApplyResult rc;
	if (!xx_start(&f)) return;
	rc = ss_prepare(&f); UT_ASSERT_EQ(rc, RESOURCE_X_APPLY_APPLIED);
	if (rc != RESOURCE_X_APPLY_APPLIED) return;
	ack = ss_read(&f, false); image = ss_read(&f, true);
	UT_ASSERT_EQ(ack.body.acquire_witness.original_ack.observed_mode, PCM_STATE_X);
	UT_ASSERT_EQ(ack.body.acquire_witness.original_ack.target_mode, PCM_STATE_N);
	UT_ASSERT_EQ(ack.body.acquire_witness.original_ack.flags, RESOURCE_X_COMMON_FLAG_PI_ESTABLISHED);
	UT_ASSERT_EQ(ack.body.acquire_witness.original_proof.source_carrier_generation, 902);
	UT_ASSERT_EQ(image.body.acquire_image.original_image.source_fence[28], PCM_STATE_X);
	UT_ASSERT(memcmp(image.body.acquire_image.original_image.page_bytes, f.page.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(f.master.state->holder_status_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(f.master.state->holder_image_intent.state, RESOURCE_X_INTENT_SLOT_EMPTY);
}

UT_TEST(test_x_current_source_original_forward_busy_then_same_claim_capture)
{
	SSourceFixture f; ResourceXSourceDrainWork work, next; ResourceXDecodedFrame restored;
	ResourceXApplyResult rc;
	if (!xx_start(&f)) return;
	rc = ss_forward(&f); UT_ASSERT_EQ(rc, RESOURCE_X_APPLY_APPLIED);
	if (rc != RESOURCE_X_APPLY_APPLIED) return;
	UT_ASSERT(ss_service_probe(&work));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&work, RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(!ss_service_probe(&next));
	cluster_pcm_lock_resource_x_source_drain_content_released();
	UT_ASSERT(ss_service_probe(&next));
	UT_ASSERT_EQ(ss_capture(&f, &next), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_drain_snapshot_exact(&next, &restored), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(restored.common.target_mode, PCM_STATE_X);
	UT_ASSERT_EQ(restored.body.acquire_decision.directory_mode, PCM_STATE_X);
	UT_ASSERT(resource_x_assertion_equal(&restored.common.logical_assertion, &f.block.common.logical_assertion));
	UT_ASSERT_EQ(ss_publish(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&next, RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_APPLIED);
}

UT_TEST(test_x_current_source_publication_requires_exact_n_pi_once)
{
	SSourceFixture f; ClusterPcmOwnSnapshot n; ResourceXApplyResult rc;
	if (!xx_start(&f)) return;
	rc = ss_prepare(&f); UT_ASSERT_EQ(rc, RESOURCE_X_APPLY_APPLIED);
	if (rc != RESOURCE_X_APPLY_APPLIED) return;
	n = f.finished; f.finished.pcm_state = PCM_STATE_S; f.finished.buffer_type = BUF_TYPE_SCUR;
	ss_unchanged(&f, true, RESOURCE_X_APPLY_STALE);
	f.finished = n; UT_ASSERT_EQ(ss_publish(&f), RESOURCE_X_APPLY_APPLIED);
	ss_unchanged(&f, true, RESOURCE_X_APPLY_DUPLICATE);
	ss_unchanged(&f, false, RESOURCE_X_APPLY_DUPLICATE);
}

UT_TEST(test_x_current_source_wrong_native_mode_or_activation_preserves_owner)
{
	for (int n = 0; n < 5; n++) {
		SSourceFixture f; if (!xx_start(&f)) return;
		if (n == 0) { f.prepared.pcm_state = PCM_STATE_S; f.prepared.buffer_type = BUF_TYPE_SCUR; }
		if (n == 1) f.prepared.writer_activation_token = 19;
		if (n == 2) f.prepared.resource_x_activation_generation = 19;
		if (n == 3) f.prepared.semantic_buf_state |= BM_IO_ERROR;
		if (n == 4) f.prepared.semantic_buf_state |= BM_DIRTY;
		ss_unchanged(&f, false, RESOURCE_X_APPLY_INVALID);
	}
}

UT_TEST(test_x_current_source_original_master_ack_is_independent_of_delivery)
{
	SSourceFixture f; ResourceXDecodedFrame ack; ResourceXMasterSnapshot out; ResourceXApplyResult rc;
	if (!xx_start(&f)) return;
	rc = ss_prepare(&f); UT_ASSERT_EQ(rc, RESOURCE_X_APPLY_APPLIED);
	if (rc != RESOURCE_X_APPLY_APPLIED) return;
	UT_ASSERT_EQ(ss_publish(&f), RESOURCE_X_APPLY_APPLIED); ack = ss_read(&f, false);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(&ack, 0, &out), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.master.state->requests[1].final_authority_generation, 0);
	UT_ASSERT_EQ(f.master.state->holder_status.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
	UT_ASSERT_EQ(f.master.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
}

UT_TEST(test_x_current_source_member_change_keeps_original_pair_native_debt)
{
	SSourceFixture f; ResourceXSourceDrainWork work; ResourceXReconfigToken token;
	ResourceXReconfigBatch batch; ResourceXReconfigResult result = RESOURCE_X_RECONFIG_MORE;
	ResourceXApplyResult rc;
	if (!xx_start(&f)) return;
	rc = ss_service_prepare(&f, 77); UT_ASSERT_EQ(rc, RESOURCE_X_APPLY_APPLIED);
	if (rc != RESOURCE_X_APPLY_APPLIED) return;
	UT_ASSERT(ss_service_probe(&work)); UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &token));
	for (int n = 0; n < 8 && (result == RESOURCE_X_RECONFIG_MORE || result == RESOURCE_X_RECONFIG_RETRY); n++)
		result = cluster_resource_x_reconfig_sweep(&token, 4, &batch);
	UT_ASSERT_EQ(result, RESOURCE_X_RECONFIG_ORPHAN);
	UT_ASSERT_EQ(f.master.state->holder_status.valid, RESOURCE_X_HOLDER_PAIR_PENDING);
	UT_ASSERT(cluster_pcm_own_snapshot_equal_exact(&f.master.state->holder_acquire_s_native, &f.prepared));
}

int
main(void)
{
	UT_PLAN(84);
	UT_RUN(test_s_source_reserves_complete_pair_before_native_finish);
	UT_RUN(test_s_source_exact_prepare_duplicate_is_not_a_second_native_commit);
	UT_RUN(test_s_source_changed_token_descriptor_or_page_cannot_replace_pending);
	UT_RUN(test_s_source_bad_native_shape_is_refused_before_capacity_changes);
	UT_RUN(test_s_source_does_not_relabel_x_downgrade);
	UT_RUN(test_s_source_full_a_and_native_tag_are_bound);
	UT_RUN(test_s_source_crc_and_original_master_authentication_are_not_optional);
	UT_RUN(test_s_source_full_original_holder_capacity_refusal_preserves_it);
	UT_RUN(test_s_source_cannot_publish_without_exact_native_transition);
	UT_RUN(test_s_source_publish_arms_both_original_send_owners_once);
	UT_RUN(test_s_source_old_publish_and_drain_remain_closed_for_full_a);
	UT_RUN(test_s_source_actual_transport_callback_does_not_retire_semantic_pair);
	UT_RUN(test_s_source_master_consumes_the_produced_original_proof_independently);
	UT_RUN(test_s_source_cancellation_cannot_clear_no_cancel_through_pair);
	UT_RUN(test_s_source_membership_freeze_retains_exact_native_debt);
	UT_RUN(test_s_source_corrupt_half_is_not_published_or_replaced);
	UT_RUN(test_s_source_ack_sender_rebind_and_descriptor_identity_are_exact);
	UT_RUN(test_s_source_bad_ids_and_connection_generations_do_not_reserve);
	UT_RUN(test_s_source_master_directory_does_not_change_at_reserve_or_publish);
	UT_RUN(test_s_source_invalid_wal_projection_cannot_produce_a_pair);
	UT_RUN(test_s_source_snapshot_copies_original_pending_without_mutation);
	UT_RUN(test_s_source_snapshot_published_pair_survives_transport_drain);
	UT_RUN(test_s_source_snapshot_rejects_changed_forward_and_preserves_outputs);
	UT_RUN(test_s_source_snapshot_gate_freeze_does_not_forget_native_debt);
	UT_RUN(test_s_source_snapshot_corrupt_pair_is_not_adopted);
	UT_RUN(test_s_source_snapshot_requires_every_output_and_original_reservation);
	UT_RUN(test_s_source_service_atomically_queues_same_full_forward);
	UT_RUN(test_s_source_service_excludes_parallel_claim_and_busy_self_requeue);
	UT_RUN(test_s_source_service_release_after_busy_resumes_without_wire);
	UT_RUN(test_s_source_service_success_needs_real_pair_publication);
	UT_RUN(test_s_source_service_success_retires_work_not_source_native_or_pi);
	UT_RUN(test_s_source_service_caller_cancel_does_not_discard_lms_work);
	UT_RUN(test_s_source_service_membership_refusal_keeps_original_work);
	UT_RUN(test_s_source_service_old_claim_cannot_complete_successor_attempt);
	UT_RUN(test_s_source_service_snapshot_requires_exact_claim_and_full_common);
	UT_RUN(test_s_source_service_r4_and_duplicate_registration_are_exact);
	UT_RUN(test_s_source_service_does_not_silently_adopt_unowned_component_pair);
	UT_RUN(test_s_source_service_release_before_registration_cannot_strand_first_attempt);
	UT_RUN(test_s_source_service_error_returns_only_claim_and_preserves_native_debt);
	UT_RUN(test_s_source_service_reconfiguration_preserves_claimed_and_parked_work);
	UT_RUN(test_s_source_service_two_busy_claims_cannot_manufacture_release_events);
	UT_RUN(test_s_source_forward_reserves_full_a_before_any_native_tuple);
	UT_RUN(test_s_source_forward_duplicate_and_changed_domain_cannot_replace_it);
	UT_RUN(test_s_source_forward_bad_auth_r4_and_crc_never_reserve);
	UT_RUN(test_s_source_forward_cannot_overwrite_any_existing_holder_capacity);
	UT_RUN(test_s_source_forward_snapshot_has_explicit_no_native_phase);
	UT_RUN(test_s_source_forward_busy_waits_real_event_without_dropping_frame);
	UT_RUN(test_s_source_forward_cancel_and_unsent_forget_cannot_erase_work);
	UT_RUN(test_s_source_forward_cannot_be_adopted_by_unclaimed_prepare);
	UT_RUN(test_s_source_forward_exact_capture_atomically_hands_native_to_same_work);
	UT_RUN(test_s_source_forward_old_claim_cannot_capture_successor_attempt);
	UT_RUN(test_s_source_forward_stale_domain_cannot_handoff_or_forget);
	UT_RUN(test_s_source_forward_corruption_is_not_a_pair_or_claim);
	UT_RUN(test_s_source_forward_remote_master_r8_keeps_full_frame_orphan);
	UT_RUN(test_s_source_forward_capture_bad_native_or_descriptor_preserves_original);
	UT_RUN(test_compatible_s_source_keeps_s_generation_and_seals_real_pair);
	UT_RUN(test_compatible_s_source_rejects_wrong_native_completion_without_mutation);
	UT_RUN(test_compatible_s_source_exact_replay_never_rearms_or_demotes);
	UT_RUN(test_compatible_s_source_busy_preserves_same_forward_until_real_event);
	UT_RUN(test_compatible_s_source_bad_native_shape_and_holder_capacity_stay_closed);
	UT_RUN(test_compatible_s_source_reconfiguration_retains_original_pair_and_work);
	UT_RUN(test_compatible_s_component_crc_does_not_enable_standalone_v2_s);
	UT_RUN(test_compatible_s_completion_preserves_later_hints_but_not_io_error);
	UT_RUN(test_s_holder_retains_original_forward_and_no_source_image);
	UT_RUN(test_s_holder_reserves_two_control_witnesses_before_native_finish);
	UT_RUN(test_s_holder_publish_requires_exact_n_pi_and_never_a_source_proof);
	UT_RUN(test_s_holder_snapshot_rejects_original_forward_connection_drift);
	UT_RUN(test_s_holder_transport_callbacks_leave_original_native_and_pair_debt);
	UT_RUN(test_s_holder_busy_service_keeps_same_work_until_actual_event);
	UT_RUN(test_s_holder_wrong_role_or_native_identity_cannot_reserve);
	UT_RUN(test_s_holder_membership_freeze_preserves_pair_as_orphan);
	UT_RUN(test_s_holder_ack_is_consumed_only_by_original_master_independently);
	UT_RUN(test_s_holder_cancellation_cannot_erase_native_or_direct_witness);
	UT_RUN(test_x_to_s_pair_retained_before_physical_commit);
	UT_RUN(test_x_to_s_pair_publish_and_replay_are_exact);
	UT_RUN(test_x_to_s_pair_rejects_mode_activation_dirt_or_wrong_native_finish);
	UT_RUN(test_x_to_s_original_forward_and_lms_claim_survive_capture);
	UT_RUN(test_x_to_s_original_master_independently_consumes_source_proof);
	UT_RUN(test_x_current_source_reserves_bound_image_before_native_n_pi);
	UT_RUN(test_x_current_source_original_forward_busy_then_same_claim_capture);
	UT_RUN(test_x_current_source_publication_requires_exact_n_pi_once);
	UT_RUN(test_x_current_source_wrong_native_mode_or_activation_preserves_owner);
	UT_RUN(test_x_current_source_original_master_ack_is_independent_of_delivery);
	UT_RUN(test_x_current_source_member_change_keeps_original_pair_native_debt);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
