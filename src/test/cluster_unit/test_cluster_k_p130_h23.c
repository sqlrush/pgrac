/* Independent source transport and PI lifetimes through original owners.
 * Native and lifecycle environments are separate host executables.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifdef K_P130_H23_LIFECYCLE
#include "postgres.h"
#include "cluster/cluster_pcm_lock.h"
/* Keep the original PI fixture's provider inputs, but give its prepare to
 * the production service owner so the actual LMS work can be claimed. */
#define cluster_pcm_lock_resource_x_acquire_s_source_prepare_exact(...) \
	cluster_pcm_lock_resource_x_acquire_s_source_service_prepare_exact(__VA_ARGS__, 77, 61)
#include "test_cluster_resource_x_pi_fixture.inc"
#undef cluster_pcm_lock_resource_x_acquire_s_source_prepare_exact

static bool
h23_probe(ResourceXSourceDrainWork *work)
{
	ResourceXIntentSlot slot;
	ResourceXAcquisitionRef ref;
	uint8 payload[RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES];
	uint32 examined;
	for (int n = 0; n < 256; n++) {
		ResourceXIntentProbeResult r = cluster_pcm_lock_resource_x_outbound_work_probe_exact(
			4, &slot, payload, sizeof(payload), &examined, &ref);
		UT_ASSERT(r != RESOURCE_X_INTENT_PROBE_CORRUPT);
		if (r == RESOURCE_X_INTENT_PROBE_SOURCE_DRAIN) { memcpy(work, payload, sizeof(*work)); return true; }
		if (r == RESOURCE_X_INTENT_PROBE_IDLE) break;
	}
	return false;
}

static void
h23_lifecycle(PcmState mode, bool published)
{
	PiFixture f;
	ResourceXDecodedFrame request, next;
	ResourceXSourceDrainWork old, current;
	ClusterPcmOwnSnapshot prepared, finished;
	ClusterPcmResourceXMasterState before;
	struct StopPcmEntryLayout *entry;
	ClusterPcmLocalPiFloorV1 floor = {0};
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES] = {0};
	uint16 length = 0;
	ResourceXWireReject reject;
	bool found;

	if (!pi_start_mode(&f, true, 2, mode)) { UT_ASSERT(false); pi_end(); return; }
	found = h23_probe(&old); UT_ASSERT(found); if (!found) { pi_end(); return; }
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&old,
		RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_APPLIED);
	request = pi_request(&f);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.transport_refs, 0);
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.pi_refs, 1);
	clean_send(&f.source.m.state->holder_status_intent.slot);
	/* This is the existing typed DATA/physical-ACK provider seam, not a
	 * storage persistence test. Actual PCM/WAL retirement runs here. */
	local_pi_covered = true;
	UT_ASSERT(pi_retire(&f, false));
	UT_ASSERT_EQ(f.source.m.state->holder_image.valid, 0);
	UT_ASSERT(cluster_pcm_local_pi_floor_v1(&f.last.source, &floor));
	UT_ASSERT_EQ(floor.bounded, 0);

	next = f.source.block;
	next.common.assertion_sequence++;
	next.common.base_authority_generation = next.common.authority_generation++;
	next.body.acquire_decision.enqueue_order++;
	next.body.acquire_decision.source_service_generation++;
	next.body.acquire_decision.episode.reservation_token++;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCK_TO_N, &next,
		bytes, sizeof(bytes), &length, &reject));
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCK_TO_N, bytes, length, &next, &reject));
	prepared = f.source.prepared; prepared.generation += 4; prepared.reservation_token += 4;
	finished = prepared; finished.flags = 0;
	if (mode == PCM_STATE_X) { finished.generation++; finished.pcm_state = PCM_STATE_S; finished.buffer_type = BUF_TYPE_SCUR; }
	f.source.page.data[BLCKSZ - 1] ^= 0x5a;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_service_prepare_exact(&next,
		2, 7, &prepared, f.source.page.data, NULL, 83, 61, 77, 61), RESOURCE_X_APPLY_APPLIED);
	found = h23_probe(&current); UT_ASSERT(found); if (!found) { pi_end(); return; }
	UT_ASSERT(current.attempt_generation > old.attempt_generation);
	if (published) UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(
		&next, 2, 7, &prepared, &finished), RESOURCE_X_APPLY_APPLIED);
	entry = (struct StopPcmEntryLayout *)fake_pcm_entries.data[0];
	UT_ASSERT(memcmp(&entry->resource_x_source_drain, &current, sizeof(current)) == 0);
	before = *f.source.m.state;
	for (int n = 0; n < 3; n++) {
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&old,
			n == 1 ? RESOURCE_X_APPLY_BAD_STATE : RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_STALE);
		/* The bounded old receipt remains exact, but cannot arm its ACK
		 * while a different B pair owns the slot. The original owner parks it. */
		UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_LOCAL_WAIT);
		UT_ASSERT(memcmp(&before, f.source.m.state, sizeof(before)) == 0);
		UT_ASSERT(memcmp(&current, &entry->resource_x_source_drain, sizeof(current)) == 0);
	}
	UT_ASSERT_EQ(f.source.m.state->holder_image.authority_generation, next.common.authority_generation);
	UT_ASSERT_EQ(f.source.m.state->holder_image.valid, published ? RESOURCE_X_HOLDER_PAIR_PUBLISHED : RESOURCE_X_HOLDER_PAIR_PENDING);
	printf("# H23 lifecycle mode=%d published=%d E=%llu/%llu A=%llu/%llu attempt=%llu/%llu PI=%u transport=%u old_lower=%llu pair=%u running=1 late=3 bytes=%u/%u crc=%u\n",
		mode, published, (unsigned long long)request.common.assertion_sequence, (unsigned long long)next.common.assertion_sequence,
		(unsigned long long)request.common.authority_generation, (unsigned long long)next.common.authority_generation,
		(unsigned long long)old.attempt_generation, (unsigned long long)current.attempt_generation,
		f.source.m.state->pi_carrier.pi_refs, f.source.m.state->pi_carrier.transport_refs, (unsigned long long)floor.bounded,
		f.source.m.state->holder_image.valid, f.source.m.state->holder_status.payload_bytes,
		f.source.m.state->holder_image.payload_bytes, cluster_gcs_block_compute_checksum(f.source.page.data));
	if (!published) UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(
		&next, 2, 7, &prepared, &finished), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&current,
		RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(entry->resource_x_source_drain.r4_record_generation, 0);
	/* New B pair and transport debt deliberately remain; no terminal-zero claim. */
	pi_end();
}
UT_TEST(h23_S_late_DRAIN_preserves_pending_successor) { h23_lifecycle(PCM_STATE_S, false); }
UT_TEST(h23_X_late_DRAIN_preserves_pending_successor) { h23_lifecycle(PCM_STATE_X, false); }
UT_TEST(h23_S_late_DRAIN_preserves_published_successor) { h23_lifecycle(PCM_STATE_S, true); }
UT_TEST(h23_X_late_DRAIN_preserves_published_successor) { h23_lifecycle(PCM_STATE_X, true); }
#else
#include "test_cluster_k_p130_h23_native.inc"

static void
h23_native(PcmState target, bool image_first)
{
	HolderFixture f;
	ClusterPageWalBindingV1 wal;
	ClusterPcmLocalPiSnapshotV1 pi = {0}, after = {0};
	ClusterPcmLocalPiFloorV1 floor = {0}, later = {0};
	ClusterPcmResourceXHolderImage image;
	ClusterPcmResourceXHolderStatus status;
	ClusterPcmResourceXPiCarrier carrier;
	ResourceXDecodedFrame request = {0}, grant = {0}, proof = {0};
	BufferDesc native;
	uint8 bytes[RESOURCE_X_SOURCE_COMPLETE_V11_BYTES] = {0};
	uint16 length = 0;
	ResourceXWireReject reject;
	WRITEBACK_START(&f, target);
	wal = writeback_install_wal(); writeback_dirty();
	HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(request_buf.tag, &pi));
	UT_ASSERT(cluster_pcm_local_pi_floor_v1(&wal.source, &floor));
	UT_ASSERT_EQ(floor.bounded, 2); UT_ASSERT_EQ(floor.floor, wal.record_start);
	image = f.master.state->holder_image; status = f.master.state->holder_status;
	carrier = f.master.state->pi_carrier; native = request_buf;
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCKED_TO_N, status.payload,
		status.payload_bytes, &proof, &reject));
	grant = publish_decode(f.master.state->grant_intent.payload, false);
	memset(&request, 0, sizeof(request));
	request.kind = RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2; request.has_acquire_decision = true;
	request.payload_bytes = RESOURCE_X_SOURCE_COMPLETE_V11_BYTES;
	request.common = f.block.common; request.common.outcome = RESOURCE_X_OUTCOME_OK;
	request.body.acquire_completion.decision = grant.body.acquire_decision;
	request.body.acquire_completion.decision_crc32c = grant.common.semantic_crc32c;
	request.body.acquire_completion.native_generation = grant.body.acquire_decision.episode.base_generation + 1;
	request.body.acquire_completion.page_lsn = PageGetLSN((Page)disk_shared.data);
	request.body.acquire_completion.page_scn = proof.body.acquire_witness.original_proof.page_scn_lsn;
	request.body.acquire_completion.page_crc32c = proof.body.acquire_witness.original_proof.page_checksum;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCK_TO_N, &request, bytes, sizeof(bytes), &length, &reject));
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCK_TO_N, bytes, length, &request, &reject));
	HOLDER_EQ(cluster_pcm_lock_resource_x_acquire_read_source_settlement_exact(&request, 0, 61), RESOURCE_X_APPLY_LOCAL_WAIT);
	HOLDER_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f.work, RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_APPLIED);
	/* The original fixture's M FORWARD has been consumed; physical callbacks
	 * for the holder pair are still independently required. */
	holder_send_control(&f, image_first, true);
	HOLDER_EQ(cluster_pcm_lock_resource_x_acquire_read_source_settlement_exact(&request, 0, 61), RESOURCE_X_APPLY_LOCAL_WAIT);
	holder_send_control(&f, !image_first, false);
	HOLDER_EQ(cluster_pcm_lock_resource_x_acquire_read_source_settlement_exact(&request, 0, 61), RESOURCE_X_APPLY_LOCAL_WAIT);
	holder_send_control(&f, !image_first, true);
	HOLDER_EQ(cluster_pcm_lock_resource_x_acquire_read_source_settlement_exact(&request, 0, 61), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.master.state->pi_carrier.transport_refs, 0);
	UT_ASSERT_EQ(f.master.state->pi_carrier.pi_refs, 1);
	carrier.transport_refs = 0;
	UT_ASSERT(memcmp(&carrier, &f.master.state->pi_carrier, sizeof(carrier)) == 0);
	UT_ASSERT(memcmp(&image, &f.master.state->holder_image, sizeof(image)) == 0);
	UT_ASSERT(memcmp(&status, &f.master.state->holder_status, sizeof(status)) == 0);
	UT_ASSERT(memcmp(&native, &request_buf, sizeof(native)) == 0);
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(request_buf.tag, &after));
	UT_ASSERT(memcmp(&pi, &after, sizeof(pi)) == 0);
	UT_ASSERT(!cluster_pcm_local_pi_retire_v1(&after, NULL, NULL, NULL, 0, NULL));
	UT_ASSERT(cluster_pcm_local_pi_floor_v1(&wal.source, &later));
	UT_ASSERT(memcmp(&floor, &later, sizeof(floor)) == 0);
	UT_ASSERT_EQ(f.master.state->holder_status_intent.slot.kind, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
	holder_send_control(&f, false, true);
	for (int n = 0; n < 3; n++) {
		HOLDER_EQ(cluster_pcm_lock_resource_x_acquire_read_source_settlement_exact(&request, 0, 61), RESOURCE_X_APPLY_DUPLICATE);
		holder_send_control(&f, false, true);
	}
	UT_ASSERT_EQ(f.master.state->requests[1].blocked_holders_bitmap, 0);
	UT_ASSERT_EQ(f.master.state->pi_carrier.pi_refs, 1);
	UT_ASSERT_EQ(f.master.state->pi_carrier.transport_refs, 0);
	UT_ASSERT_EQ(f.master.state->holder_image_intent.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	writeback_file_matches(); writeback_idle();
	printf("# H23 native target=%d image_first=%d E=%llu A=%llu gen=%llu writes=%u PI=1 transport=0 lower=%llu floor=%llu pair=%u ack=0 DATA=0 bytes=%u/%u crc=%u\n",
		target, image_first, (unsigned long long)request.common.assertion_sequence,
		(unsigned long long)request.common.authority_generation, (unsigned long long)cluster_pcm_own_gen_get(0),
		holder_writes, (unsigned long long)later.bounded, (unsigned long long)later.floor, f.master.state->holder_image.valid,
		status.payload_bytes, image.payload_bytes, request.body.acquire_completion.page_crc32c);
}
UT_TEST(h23_native_S_settlement_before_DATA_status_first) { h23_native(PCM_STATE_S, false); }
UT_TEST(h23_native_X_settlement_before_DATA_status_first) { h23_native(PCM_STATE_X, false); }
UT_TEST(h23_native_S_settlement_before_DATA_image_first) { h23_native(PCM_STATE_S, true); }
UT_TEST(h23_native_X_settlement_before_DATA_image_first) { h23_native(PCM_STATE_X, true); }
#endif

#if defined(K_P130_EXPECT_ASSERT_OFF) && defined(USE_ASSERT_CHECKING)
#error "H23 assertion-OFF requires an assertion-disabled configuration"
#endif
int main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
#ifdef USE_ASSERT_CHECKING
	printf("# H23 USE_ASSERT_CHECKING=1\n");
#else
	printf("# H23 USE_ASSERT_CHECKING=0\n");
#endif
	UT_PLAN(4);
#ifdef K_P130_H23_LIFECYCLE
	UT_RUN(h23_S_late_DRAIN_preserves_pending_successor);
	UT_RUN(h23_X_late_DRAIN_preserves_pending_successor);
	UT_RUN(h23_S_late_DRAIN_preserves_published_successor);
	UT_RUN(h23_X_late_DRAIN_preserves_published_successor);
#else
	UT_RUN(h23_native_S_settlement_before_DATA_status_first);
	UT_RUN(h23_native_X_settlement_before_DATA_status_first);
	UT_RUN(h23_native_S_settlement_before_DATA_image_first);
	UT_RUN(h23_native_X_settlement_before_DATA_image_first);
#endif
	UT_DONE(); return ut_failed_count ? 1 : 0;
}
