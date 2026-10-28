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

UT_TEST(test_s_source_does_not_relabel_compatible_s_or_x_downgrade)
{
	for (int scenario = 1; scenario <= 2; scenario++) {
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
		else f.master.state->holder_acquire_s_reserved = 1;
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

int
main(void)
{
	UT_PLAN(26);
	UT_RUN(test_s_source_reserves_complete_pair_before_native_finish);
	UT_RUN(test_s_source_exact_prepare_duplicate_is_not_a_second_native_commit);
	UT_RUN(test_s_source_changed_token_descriptor_or_page_cannot_replace_pending);
	UT_RUN(test_s_source_bad_native_shape_is_refused_before_capacity_changes);
	UT_RUN(test_s_source_does_not_relabel_compatible_s_or_x_downgrade);
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
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
