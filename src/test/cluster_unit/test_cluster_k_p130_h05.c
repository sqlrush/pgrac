/* Bounded PI body ownership through original producer and completion owners. */
#include "test_cluster_resource_x_pi_fixture.inc"

#if defined(K_P130_EXPECT_ASSERT_OFF) && defined(USE_ASSERT_CHECKING)
#error "H05 assertion-OFF requires an assertion-disabled configuration"
#endif

static ResourceXDecodedFrame
h05_successor(PiFixture *f)
{
	ResourceXDecodedFrame block = f->source.block;
	ResourceXWireReject reject;
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES];
	uint16 length = 0;
	block.common.assertion_sequence++;
	block.common.base_authority_generation = block.common.authority_generation++;
	block.body.acquire_decision.enqueue_order++;
	block.body.acquire_decision.source_service_generation++;
	block.body.acquire_decision.episode.reservation_token++;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCK_TO_N,
		&block, bytes, sizeof(bytes), &length, &reject));
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCK_TO_N,
		bytes, length, &block, &reject));
	return block;
}

static void
h05_census(PiFixture *f, const char *phase)
{
	ClusterPcmResourceXMasterState *s = f->source.m.state;
	ClusterPcmLocalPiFloorV1 floor;
	UT_ASSERT(cluster_pcm_local_pi_floor_v1(&f->first.source, &floor));
	printf("# H05 mode=%d phase=%s E=" UINT64_FORMAT " A=" UINT64_FORMAT
		" pair=%u PI=%u transport=%u ACKstate=%u ACKkind=%u lower=%u floor=" UINT64_FORMAT
		" broadcasts=%d sleeps=%d pairA=" UINT64_FORMAT " native=" UINT64_FORMAT
		" crc=%u\n", f->source.prepared.pcm_state, phase,
		f->source.block.common.assertion_sequence, f->source.block.common.authority_generation,
		s->holder_image.valid, s->pi_carrier.pi_refs, s->pi_carrier.transport_refs,
		s->holder_status_intent.slot.state, s->holder_status_intent.slot.kind,
		floor.bounded, floor.floor, fake_cv_broadcast_count, fake_cv_sleep_count,
		s->holder_status.authority_generation, s->pi_carrier.completed.generation,
		cluster_gcs_block_compute_checksum(f->source.page.data));
}

static void
h05_wait(PiFixture *f, const ResourceXDecodedFrame *block)
{
	ClusterPcmResourceXMasterState before = *f->source.m.state;
	ClusterPcmLocalPiSnapshotV1 local, after;
	ResourceXApplyResult prepared, forwarded;
	int sleeps = fake_cv_sleep_count;
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(f->local.resource, &local));
	prepared = cluster_pcm_lock_resource_x_acquire_s_source_prepare_exact(block,
		2, 7, &f->source.prepared, f->source.page.data, &f->last, 83, 61);
	UT_ASSERT_EQ(prepared, RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT(memcmp(&before, f->source.m.state, sizeof(before)) == 0);
	forwarded = cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(block, 2, 83, 61);
	UT_ASSERT_EQ(forwarded, RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT(memcmp(&before, f->source.m.state, sizeof(before)) == 0);
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(f->local.resource, &after));
	UT_ASSERT(memcmp(&local, &after, sizeof(local)) == 0);
	/* These are nonblocking owner steps, not a replacement foreground wait. */
	UT_ASSERT_EQ(fake_cv_sleep_count, sleeps);
	printf("# H05 capacity prepare=%d forward=%d unchanged=%d\n", prepared, forwarded,
		memcmp(&before, f->source.m.state, sizeof(before)) == 0);
}

static bool
h05_start(PiFixture *f, PcmState mode, ResourceXDecodedFrame *block)
{
	if (!pi_start_mode(f, true, 2, mode)) { UT_ASSERT(false); return false; }
	UT_ASSERT_EQ(f->source.m.state->pi_carrier.pi_refs, 1);
	UT_ASSERT_EQ(f->source.m.state->pi_carrier.transport_refs, 1);
	UT_ASSERT_EQ(f->source.m.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
	*block = h05_successor(f);
	h05_census(f, "full");
	return true;
}

static void
h05_settle(PiFixture *f)
{
	ResourceXDecodedFrame request = pi_request(f);
	int wakes = fake_cv_broadcast_count;
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(fake_cv_broadcast_count > wakes);
	UT_ASSERT_EQ(f->source.m.state->pi_carrier.transport_refs, 0);
	UT_ASSERT_EQ(f->source.m.state->holder_status_intent.slot.kind,
		RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
}

static void
h05_data(PiFixture *f)
{
	int wakes = fake_cv_broadcast_count;
	/* Original typed DATA/physical ACK providers are explicit fixture inputs;
	 * actual local/retained PI and WAL reference consumers execute here. */
	local_pi_covered = true;
	UT_ASSERT(pi_retire(f, false));
	UT_ASSERT(fake_cv_broadcast_count > wakes);
	UT_ASSERT_EQ(f->source.m.state->pi_carrier.pi_refs, 0);
}

static void
h05_empty(PiFixture *f)
{
	static const ClusterPcmResourceXPiCarrier empty;
	ClusterPcmLocalPiSnapshotV1 local;
	ClusterPcmLocalPiFloorV1 floor;
	UT_ASSERT(memcmp(&empty, &f->source.m.state->pi_carrier, sizeof(empty)) == 0);
	UT_ASSERT_EQ(f->source.m.state->holder_status.valid, 0);
	UT_ASSERT_EQ(f->source.m.state->holder_image.valid, 0);
	UT_ASSERT_EQ(f->source.m.state->holder_status_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(f->source.m.state->holder_image_intent.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(f->source.m.state->settlement_retry.pending, 0);
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(f->local.resource, &local));
	UT_ASSERT_EQ(local.first.record_start, 0); UT_ASSERT_EQ(local.last.record_start, 0);
	UT_ASSERT(cluster_pcm_local_pi_floor_v1(&f->first.source, &floor));
	UT_ASSERT_EQ(floor.bounded, 0); UT_ASSERT_EQ(floor.floor, 0);
	h05_census(f, "old-empty");
}

static void
h05_full(PcmState mode)
{
	PiFixture f; ResourceXDecodedFrame block;
	if (!h05_start(&f, mode, &block)) { pi_end(); return; }
	/* Finite repeated observations are not a timeout or a retrying caller. */
	for (int i = 0; i < 3; i++) h05_wait(&f, &block);
	local_pi_covered = false;
	UT_ASSERT(!pi_retire(&f, false));
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.pi_refs, 1);
	h05_wait(&f, &block);
	h05_census(&f, "retained");
	pi_end();
}

static void
h05_data_first(PcmState mode)
{
	PiFixture f; ResourceXDecodedFrame block;
	if (!h05_start(&f, mode, &block)) { pi_end(); return; }
	h05_data(&f);
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.transport_refs, 1);
	UT_ASSERT_EQ(f.source.m.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
	h05_wait(&f, &block);
	h05_census(&f, "DATA-only");
	h05_settle(&f);
	clean_send(&f.source.m.state->holder_status_intent.slot);
	h05_empty(&f); pi_end();
}

static void
h05_ack_pending(PcmState mode)
{
	PiFixture f; ResourceXDecodedFrame block;
	ResourceXIntentSlot staged;
	if (!h05_start(&f, mode, &block)) { pi_end(); return; }
	h05_settle(&f);
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.pi_refs, 1);
	h05_wait(&f, &block);
	staged = clean_stage(&f.source.m.state->holder_status_intent.slot);
	/* Physical completion has not arrived. Keep the actual staged ACK owner
	 * while DATA retires the independent logical responsibility. */
	h05_data(&f);
	UT_ASSERT_EQ(f.source.m.state->holder_image.valid, 0);
	UT_ASSERT_EQ(f.source.m.state->holder_status_intent.slot.state, RESOURCE_X_INTENT_SLOT_STAGED);
	h05_wait(&f, &block);
	h05_census(&f, "ACK-pending");
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(
		&staged, true, ++fake_pcm_clock_us));
	h05_empty(&f); pi_end();
}

static void
h05_reuse(PcmState mode)
{
	PiFixture f; ResourceXDecodedFrame block;
	ClusterPcmOwnSnapshot prepared;
	if (!h05_start(&f, mode, &block)) { pi_end(); return; }
	h05_settle(&f);
	clean_send(&f.source.m.state->holder_status_intent.slot);
	h05_wait(&f, &block);
	h05_data(&f);
	h05_empty(&f);
	prepared = f.source.prepared; prepared.generation += 2; prepared.reservation_token += 2;
	f.source.page.data[BLCKSZ - 1] ^= 0x5a;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_prepare_exact(&block,
		2, 7, &prepared, f.source.page.data, &f.last, 83, 61), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.source.m.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PENDING);
	UT_ASSERT_EQ(f.source.m.state->holder_status.authority_generation, block.common.authority_generation);
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.pi_refs, 0);
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.transport_refs, 0);
	h05_census(&f, "successor-pending");
	/* The new unpublished source pair is still owned. No terminal-zero claim. */
	pi_end();
}

UT_TEST(test_k_h05_S_full_body_retains_capacity) { h05_full(PCM_STATE_S); }
UT_TEST(test_k_h05_X_full_body_retains_capacity) { h05_full(PCM_STATE_X); }
UT_TEST(test_k_h05_S_DATA_first_keeps_transport_wait) { h05_data_first(PCM_STATE_S); }
UT_TEST(test_k_h05_X_DATA_first_keeps_transport_wait) { h05_data_first(PCM_STATE_X); }
UT_TEST(test_k_h05_S_ACK_pending_keeps_local_wait) { h05_ack_pending(PCM_STATE_S); }
UT_TEST(test_k_h05_X_ACK_pending_keeps_local_wait) { h05_ack_pending(PCM_STATE_X); }
UT_TEST(test_k_h05_S_original_cleanup_releases_capacity) { h05_reuse(PCM_STATE_S); }
UT_TEST(test_k_h05_X_original_cleanup_releases_capacity) { h05_reuse(PCM_STATE_X); }
int main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
#ifdef USE_ASSERT_CHECKING
	printf("# H05 USE_ASSERT_CHECKING=1\n");
#else
	printf("# H05 USE_ASSERT_CHECKING=0\n");
#endif
	UT_PLAN(8);
	UT_RUN(test_k_h05_S_full_body_retains_capacity);
	UT_RUN(test_k_h05_X_full_body_retains_capacity);
	UT_RUN(test_k_h05_S_DATA_first_keeps_transport_wait);
	UT_RUN(test_k_h05_X_DATA_first_keeps_transport_wait);
	UT_RUN(test_k_h05_S_ACK_pending_keeps_local_wait);
	UT_RUN(test_k_h05_X_ACK_pending_keeps_local_wait);
	UT_RUN(test_k_h05_S_original_cleanup_releases_capacity);
	UT_RUN(test_k_h05_X_original_cleanup_releases_capacity);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
