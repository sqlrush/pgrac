/*-------------------------------------------------------------------------
 * test_cluster_resource_x_pi_carrier.c
 *    Actual source transport and independent local-PI responsibility owners.
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_resource_x_pi_carrier.c
 * NOTES
 *    Native page completion and typed DATA proof are explicit provider seams.
 *    These component tests do not qualify ordinary native callers.
 *-------------------------------------------------------------------------
 */
#include "test_cluster_resource_x_clean_fixture.inc"

typedef struct PiFixture {
	CleanFixture source;
	ClusterPageWalBindingV1 first, last;
	ClusterPcmLocalPiSnapshotV1 local;
} PiFixture;

static bool
pi_start_target(PiFixture *f, bool publish, int32 master, PcmState mode, PcmState target)
{
	CleanFixture *s = &f->source;
	ResourceXDecodedFrame grant;
	ClusterPageWalBindingV1 seed = local_pi_setup(make_tag(850));

	memset(f, 0, sizeof(*f));
	if (!publish_start(mode == PCM_STATE_X ? 1 : 2, target, &s->m)) return false;
	/* The directory's original PI bit is retained before A is sealed. */
	publish_apply(&s->m);
	grant = publish_decode(s->m.state->grant_intent.payload, false);
	s->block = publish_decode(s->m.state->block_intents[0].payload, true);
	if (master != 0) {
		uint8 bytes[RESOURCE_X_DECISION_V4_BYTES]; uint16 length; ResourceXWireReject reject;
		/* Authenticated remote-master frames are the explicit fixture input.
		 * This holder has no local master head to manufacture a successor. */
		s->block.body.acquire_decision.master_node = master;
		s->block.common.master_session_incarnation = 777;
		UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCK_TO_N, &s->block, bytes, sizeof(bytes), &length, &reject));
		UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCK_TO_N, bytes, length, &s->block, &reject));
		grant = s->block; grant.kind = RESOURCE_X_WIRE_AUTHORITY_GRANT; grant.common.action_node = 1;
		UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &grant, bytes, sizeof(bytes), &length, &reject));
		UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, length, &grant, &reject));
		acquire_reset(); fake_gcs_master_node = master;
	}
	f->last = seed;
	f->last.source.claim.identity.origin_owner_incarnation = cluster_qvotec_get_self_incarnation();
	f->last.record_end = 144;
	f->first = f->last;
	f->first.record_start = 80; f->first.record_end = 90;
	f->first.version.mutation_token = 70;
	cluster_enabled = cluster_shared_config = true;
	cluster_page_wal_shmem_init();
	local_pi_writer = f->last.source; local_pi_writer_ready = true;
	if (!cluster_pcm_local_pi_record_first_v1(s->block.common.logical_assertion.resource,
		&f->first, &f->last)
		|| !cluster_pcm_local_pi_snapshot_v1(s->block.common.logical_assertion.resource, &f->local)) {
		UT_ASSERT(false); return false;
	}
	s->prepared.tag = s->block.common.logical_assertion.resource;
	s->prepared.generation = 901; s->prepared.reservation_token = 77;
	s->prepared.flags = PCM_OWN_FLAG_REVOKING;
	s->prepared.pcm_state = mode;
	s->prepared.buffer_type = mode == PCM_STATE_X ? BUF_TYPE_XCUR : BUF_TYPE_SCUR;
	s->prepared.semantic_buf_state = BM_VALID;
	s->finished = s->prepared; s->finished.flags = 0;
	if (target == PCM_STATE_X) {
		s->finished.generation++; s->finished.pcm_state = PCM_STATE_N;
		s->finished.buffer_type = BUF_TYPE_PI; s->finished.flags = PCM_OWN_FLAG_REVOKING;
	} else if (mode == PCM_STATE_X) {
		s->finished.generation++; s->finished.pcm_state = PCM_STATE_S;
		s->finished.buffer_type = BUF_TYPE_SCUR;
	}
	((PageHeader)s->page.data)->pd_lower = SizeOfPageHeaderData;
	((PageHeader)s->page.data)->pd_upper = BLCKSZ;
	((PageHeader)s->page.data)->pd_special = BLCKSZ;
	((PageHeader)s->page.data)->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	PageXLogRecPtrSet(((PageHeader)s->page.data)->pd_lsn, UINT64_C(144));
	UT_ASSERT(PageSetLSNOrigin(s->page.data, 0));
	((PageHeader)s->page.data)->pd_block_scn = 80;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_prepare_exact(&s->block,
		master, 7, &s->prepared, s->page.data, &f->last, 83, 61), RESOURCE_X_APPLY_APPLIED);
	if (publish) UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(
		&s->block, master, 7, &s->prepared, &s->finished), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_status_exact(&s->block.common.logical_assertion,
		&s->ack), RESOURCE_X_APPLY_APPLIED);
	if (master != 0) {
		uint8 bytes[RESOURCE_X_SOURCE_WITNESS_V4_BYTES]; uint16 length; ResourceXWireReject reject;
		unsigned matches = 0; unsigned char *memory = (unsigned char *)fake_pcm_header.data;
		Size offset = offsetof(ClusterPcmResourceXMasterState, holder_status) + offsetof(ClusterPcmResourceXHolderStatus, payload);
		UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCKED_TO_N, &s->ack, bytes, sizeof(bytes), &length, &reject));
		for (Size n = offset; n - offset + sizeof(*s->m.state) <= fake_pcm_header_requested_size; n += 8) {
			ClusterPcmResourceXMasterState *candidate = (ClusterPcmResourceXMasterState *)(memory + n - offset);
			if (memcmp(memory + n, bytes, length) == 0
				&& candidate->holder_status.valid == RESOURCE_X_HOLDER_PAIR_PUBLISHED
				&& candidate->holder_status.payload_bytes == length
				&& candidate->binding_generation == ((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->binding_generation) {
				matches++; s->m.state = candidate;
			}
		}
		UT_ASSERT_EQ(matches, 1); if (matches != 1) return false;
	}
	/* target-X requester completion has a different producer, outside this fixture. */
	if (target == PCM_STATE_X) return true;
	s->done.kind = RESOURCE_X_WIRE_INSTALL_SETTLEMENT;
	s->done.has_acquire_decision = true;
	s->done.payload_bytes = RESOURCE_X_SOURCE_COMPLETE_V11_BYTES;
	s->done.common = grant.common; s->done.common.action_node = 1;
	s->done.common.outcome = RESOURCE_X_OUTCOME_OK;
	s->done.common.sender_connection_generation = 71;
	s->done.body.acquire_completion.decision = grant.body.acquire_decision;
	s->done.body.acquire_completion.native_generation = grant.body.acquire_decision.episode.base_generation + 1;
	s->done.body.acquire_completion.decision_crc32c = grant.common.semantic_crc32c;
	s->done.body.acquire_completion.page_lsn = 144;
	s->done.body.acquire_completion.page_scn = s->ack.body.acquire_witness.original_proof.page_scn_lsn;
	s->done.body.acquire_completion.page_crc32c = s->ack.body.acquire_witness.original_proof.page_checksum;
	UT_ASSERT(source_done_encode(&s->done));
	if (publish) {
		clean_send(&s->m.state->holder_status_intent.slot);
		clean_send(&s->m.state->holder_image_intent);
		if (master == 0) {
			if (!source_done_keep(&s->m, &s->done)) return false;
			UT_ASSERT_EQ(source_apply(&s->ack, 0), RESOURCE_X_APPLY_APPLIED);
			source_done_callback(&s->m, true);
		}
	}
	return true;
}

static bool
pi_start_mode(PiFixture *f, bool publish, int32 master, PcmState mode)
{
	return pi_start_target(f, publish, master, mode, PCM_STATE_S);
}

static bool
pi_start_master(PiFixture *f, bool publish, int32 master)
{
	return pi_start_mode(f, publish, master, PCM_STATE_S);
}

static bool
pi_start(PiFixture *f, bool publish)
{
	return pi_start_master(f, publish, 0);
}

static ResourceXDecodedFrame
pi_request(PiFixture *f)
{
	ResourceXDecodedFrame request = f->source.done;
	request.kind = RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2;
	request.common.action_node = 0;
	UT_ASSERT(clean_encode(&request));
	return request;
}

static bool
pi_retire(PiFixture *f, bool structural)
{
	pi_receipt_valid = true; pi_storage_receipt_valid = false;
	memset(&pi_receipt_cut, 0, sizeof(pi_receipt_cut)); pi_ack_cut = pi_receipt_cut;
	pi_ack_available = 1; pi_ack_imported = 0;
	structural_ack_valid = true;
	return structural
		? cluster_pcm_local_pi_retire_structural_v2(&f->local,
			(const void *)&structural_receipt_fixture, (const void *)&structural_ack_fixture)
		: cluster_pcm_local_pi_retire_v1(&f->local, (const void *)&pi_receipt_fixture,
			NULL, NULL, 0, pi_acks[0]);
}

static void
pi_end(void)
{
	cluster_shared_config = false; local_pi_writer_ready = false; local_pi_covered = false;
	local_pi_coverage_filter = NULL;
	structural_ack_valid = false;
}

UT_TEST(test_pi_master_arms_settlement_without_retiring_pi)
{
	PiFixture f; ResourceXDecodedFrame request = {0};
	if (!pi_start(&f, true)) return;
	UT_ASSERT(clean_arm(&f.source, &request));
	UT_ASSERT_EQ(request.body.acquire_completion.decision.pi_holders_bitmap, 1);
	UT_ASSERT_EQ(f.source.m.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
	pi_end();
}

UT_TEST(test_pi_transport_settlement_keeps_original_body_and_wal_scope)
{
	PiFixture f; ResourceXDecodedFrame request; ClusterPcmLocalPiSnapshotV1 after;
	if (!pi_start(&f, true)) return;
	request = pi_request(&f);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.source.m.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(request.common.logical_assertion.resource, &after));
	UT_ASSERT(memcmp(&f.local, &after, sizeof(after)) == 0);
	UT_ASSERT_EQ(f.source.m.state->holder_status_intent.slot.kind, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
	pi_end();
}

UT_TEST(test_pi_retirement_after_transport_releases_body_with_ack_still_owned)
{
	for (int structural = 0; structural < 2; structural++) {
		PiFixture f; ResourceXDecodedFrame request;
		if (!pi_start(&f, true)) return;
		request = pi_request(&f);
		UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
		local_pi_covered = false; UT_ASSERT(!pi_retire(&f, structural));
		UT_ASSERT_EQ(f.source.m.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
		local_pi_covered = true; UT_ASSERT(pi_retire(&f, structural));
		UT_ASSERT_EQ(f.source.m.state->holder_image.valid, 0);
		UT_ASSERT_EQ(f.source.m.state->holder_status.valid, 0);
		UT_ASSERT_EQ(f.source.m.state->holder_status_intent.slot.kind, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
		clean_send(&f.source.m.state->holder_status_intent.slot);
		pi_end();
	}
}

UT_TEST(test_pi_retirement_before_transport_keeps_original_sends_and_body)
{
	for (int structural = 0; structural < 2; structural++) {
		PiFixture f; ResourceXDecodedFrame request;
		if (!pi_start(&f, true)) return;
		local_pi_covered = true; UT_ASSERT(pi_retire(&f, structural));
		UT_ASSERT_EQ(f.source.m.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
		request = pi_request(&f);
		UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(f.source.m.state->holder_image.valid, 0);
		pi_end();
	}
}

UT_TEST(test_pi_wrong_latest_binding_cannot_publish_a_carrier)
{
	PiFixture f; ClusterPageWalBindingV1 newer;
	if (!pi_start(&f, false)) return;
	newer = f.last; newer.version.mutation_token++; newer.record_start = 160; newer.record_end = 200;
	UT_ASSERT(cluster_pcm_local_pi_record_v1(f.source.block.common.logical_assertion.resource, &newer));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(&f.source.block,
		0, 7, &f.source.prepared, &f.source.finished), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT_EQ(f.source.m.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PENDING);
	UT_ASSERT_EQ(f.source.m.state->holder_image_intent.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	pi_end();
}

UT_TEST(test_pi_complete_chain_keeps_original_pi_bitmap_and_consumes_requester)
{
	PiFixture f; ResourceXDecodedFrame request, ack, consumed; ResourceXMasterSnapshot out;
	if (!pi_start(&f, true)) return;
	if (!clean_arm(&f.source, &request)) return;
	clean_send(&f.source.m.state->source_settlement.intent.slot);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	ack = clean_decode(f.source.m.state->holder_status_intent.payload, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_ack_exact(&ack, 0, &out), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.source.m.work), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(pg_atomic_read_u32(&((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->pi_holders_bitmap), 1);
	UT_ASSERT_EQ(f.source.m.state->deferred_settlement.present, 3);
	UT_ASSERT_EQ(f.source.m.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
	clean_send(&f.source.m.state->holder_status_intent.slot);
	consumed = clean_decode(f.source.m.state->grant_intent.payload, RESOURCE_X_WIRE_INSTALL_SETTLEMENT);
	UT_ASSERT_EQ(consumed.common.outcome, RESOURCE_X_OUTCOME_DUPLICATE);
	clean_send(&f.source.m.state->grant_intent.slot);
	UT_ASSERT_EQ(f.source.m.state->deferred_settlement.present, 0);
	pi_end();
}

static uint64 pi_old_revision;
static unsigned pi_coverage_calls;
static bool
pi_only_new_scope(const ClusterPcmLocalPiSnapshotV1 *scope)
{
	pi_coverage_calls++;
	return scope->revision != pi_old_revision;
}

UT_TEST(test_pi_data_and_structural_proof_must_cover_both_scopes)
{
	for (int structural = 0; structural < 2; structural++) {
		PiFixture f; ClusterPcmResourceXMasterState before; ClusterPcmLocalPiSnapshotV1 after;
		ClusterPageWalBindingV1 newer;
		if (!pi_start(&f, true)) return;
		pi_old_revision = f.local.revision;
		newer = f.last; newer.version.mutation_token++; newer.record_start = 160; newer.record_end = 200;
		UT_ASSERT(cluster_pcm_local_pi_record_v1(f.source.block.common.logical_assertion.resource, &newer));
		UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(f.local.resource, &f.local));
		before = *f.source.m.state;
		pi_coverage_calls = 0; local_pi_coverage_filter = pi_only_new_scope; local_pi_covered = true;
		UT_ASSERT(!pi_retire(&f, structural));
		UT_ASSERT_EQ(pi_coverage_calls, 2);
		UT_ASSERT(memcmp(&before, f.source.m.state, sizeof(before)) == 0);
		UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(f.local.resource, &after));
		UT_ASSERT(memcmp(&f.local, &after, sizeof(after)) == 0);
		local_pi_coverage_filter = NULL;
		UT_ASSERT(pi_retire(&f, structural));
		pi_end();
	}
}

UT_TEST(test_pi_transport_replay_cannot_release_a_carrier_or_republish_old_a)
{
	PiFixture f; ResourceXDecodedFrame request; ClusterPcmResourceXPiCarrier carrier;
	if (!pi_start(&f, true)) return;
	request = pi_request(&f); carrier = f.source.m.state->pi_carrier;
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	carrier.transport_refs = 0;
	UT_ASSERT(memcmp(&carrier, &f.source.m.state->pi_carrier, sizeof(carrier)) == 0);
	clean_send(&f.source.m.state->holder_status_intent.slot);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&carrier, &f.source.m.state->pi_carrier, sizeof(carrier)) == 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(&f.source.block,
		0, 7, &f.source.prepared, &f.source.finished), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(memcmp(&carrier, &f.source.m.state->pi_carrier, sizeof(carrier)) == 0);
	pi_end();
}

UT_TEST(test_pi_membership_freeze_keeps_independent_carrier)
{
	PiFixture f; ResourceXDecodedFrame request; ResourceXReconfigToken token;
	ResourceXReconfigBatch batch; ResourceXReconfigResult result = RESOURCE_X_RECONFIG_MORE;
	ClusterPcmResourceXPiCarrier before;
	if (!pi_start(&f, true)) return;
	request = pi_request(&f); UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	before = f.source.m.state->pi_carrier;
	UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &token));
	for (int scan = 0; scan < 8 && (result == RESOURCE_X_RECONFIG_MORE || result == RESOURCE_X_RECONFIG_RETRY); scan++)
		result = cluster_resource_x_reconfig_sweep(&token, 4, &batch);
	UT_ASSERT_EQ(result, RESOURCE_X_RECONFIG_ORPHAN);
	UT_ASSERT(memcmp(&before, &f.source.m.state->pi_carrier, sizeof(before)) == 0);
	UT_ASSERT_EQ(f.source.m.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
	pi_end();
}

UT_TEST(test_pi_malformed_carrier_is_invalid_in_original_stop_census)
{
	PiFixture f; PcmGrdProtocolDebtStats debt;
	if (!pi_start(&f, true)) return;
	f.source.m.state->pi_carrier.reserved[0] = 1;
	cluster_pcm_grd_protocol_debt_snapshot(&debt);
	UT_ASSERT(debt.invalid_entry_count != 0);
	pi_end();
}

UT_TEST(test_pi_successor_waits_for_original_body_capacity)
{
	PiFixture f; ResourceXDecodedFrame request, block, retained;
	ResourceXWireReject reject; uint8 bytes[RESOURCE_X_DECISION_V4_BYTES]; uint16 length;
	ClusterPcmResourceXMasterState before, after;
	if (!pi_start_master(&f, true, 2)) return;
	request = pi_request(&f);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	clean_send(&f.source.m.state->holder_status_intent.slot);
	block = f.source.block; block.common.logical_assertion.requester_node = 3;
	block.common.assertion_sequence = 1;
	block.common.base_authority_generation = block.common.authority_generation++;
	block.body.acquire_decision.enqueue_order++;
	block.body.acquire_decision.source_service_generation++;
	block.body.acquire_decision.episode.requester_incarnation = block.body.acquire_decision.holders[3].boot_incarnation;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCK_TO_N, &block, bytes, sizeof(bytes), &length, &reject));
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCK_TO_N, bytes, length, &block, &reject));
	before = *f.source.m.state;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_prepare_exact(&block, 2, 7,
		&f.source.prepared, f.source.page.data, &f.last, 83, 61), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT(memcmp(&before, f.source.m.state, sizeof(before)) == 0);
	/* Native capture still waits. The new control-only service may park in
	 * the original entry while the old body and both WAL owners stay exact. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(&block, 2, 83, 61), RESOURCE_X_APPLY_APPLIED);
	after = *f.source.m.state;
	UT_ASSERT_EQ(after.holder_coverage_forward.valid, RESOURCE_X_HOLDER_SOURCE_FORWARD);
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCK_TO_N,
		after.holder_coverage_forward.payload, after.holder_coverage_forward.payload_bytes, &retained, &reject));
	UT_ASSERT(memcmp(&retained, &block, sizeof(block)) == 0);
	memset(&after.holder_coverage_forward, 0, sizeof(after.holder_coverage_forward));
	UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
	pi_end();
}

UT_TEST(test_pi_original_wal_floor_counts_both_owners_and_releases_exact_refs)
{
	PiFixture f; ResourceXDecodedFrame request; ClusterPcmLocalPiFloorV1 floor;
	static const ClusterPcmResourceXPiCarrier empty;
	ClusterPcmLocalPiSnapshotV1 after;
	if (!pi_start(&f, true)) return;
	UT_ASSERT(cluster_pcm_local_pi_floor_v1(&f.first.source, &floor));
	UT_ASSERT_EQ(floor.floor, 80); UT_ASSERT_EQ(floor.bounded, 2);
	request = pi_request(&f); UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_local_pi_floor_v1(&f.first.source, &floor));
	UT_ASSERT_EQ(floor.floor, 80); UT_ASSERT_EQ(floor.bounded, 2);
	local_pi_covered = true; UT_ASSERT(pi_retire(&f, false));
	UT_ASSERT(cluster_pcm_local_pi_floor_v1(&f.first.source, &floor));
	UT_ASSERT_EQ(floor.floor, 0); UT_ASSERT_EQ(floor.bounded, 0);
	UT_ASSERT(memcmp(&empty, &f.source.m.state->pi_carrier, sizeof(empty)) == 0);
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(f.local.resource, &after));
	UT_ASSERT_EQ(after.first.record_start, 0); UT_ASSERT_EQ(after.last.record_start, 0);
	pi_end();
}

UT_TEST(test_pi_quiet_and_no_image_cleanup_preserve_independent_wal_duty)
{
	for (int no_image = 0; no_image < 2; no_image++) {
		PiFixture f; ResourceXDecodedFrame request; ClusterPcmLocalPiSnapshotV1 after;
		if (!pi_start_master(&f, true, 2)) return;
		request = pi_request(&f);
		request.common.outcome = no_image ? RESOURCE_X_OUTCOME_QUIET_NO_IMAGE : RESOURCE_X_OUTCOME_QUIET;
		request.body.acquire_completion.native_generation = request.body.acquire_completion.decision.episode.base_generation;
		if (no_image) {
			request.body.acquire_completion.page_lsn = request.body.acquire_completion.page_scn = 0;
			request.body.acquire_completion.page_crc32c = 0;
		}
		UT_ASSERT(clean_encode(&request));
		UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(f.source.m.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
		UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(f.local.resource, &after));
		UT_ASSERT(memcmp(&after, &f.local, sizeof(after)) == 0);
		local_pi_covered = true; UT_ASSERT(pi_retire(&f, false));
		UT_ASSERT_EQ(f.source.m.state->holder_image.valid, 0);
		clean_send(&f.source.m.state->holder_status_intent.slot);
		pi_end();
	}
}

static PiFixture *pi_racing_fixture;
static bool
pi_advance_during_proof(const ClusterPcmLocalPiSnapshotV1 *scope)
{
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	if (++pi_coverage_calls == 2) {
		ClusterPageWalBindingV1 newer = pi_racing_fixture->last;
		newer.version.mutation_token++; newer.record_start = 160; newer.record_end = 200;
		UT_ASSERT(cluster_pcm_local_pi_record_v1(scope->resource, &newer));
	}
	return true;
}

UT_TEST(test_pi_proof_window_change_keeps_both_owned_responsibilities)
{
	for (int structural = 0; structural < 2; structural++) {
		PiFixture f; ClusterPcmResourceXMasterState before; ClusterPcmLocalPiSnapshotV1 after;
		if (!pi_start(&f, true)) return;
		before = *f.source.m.state;
		pi_racing_fixture = &f; pi_coverage_calls = 0;
		local_pi_coverage_filter = pi_advance_during_proof; local_pi_covered = true;
		UT_ASSERT(!pi_retire(&f, structural));
		UT_ASSERT_EQ(pi_coverage_calls, 2);
		UT_ASSERT(memcmp(&before, f.source.m.state, sizeof(before)) == 0);
		UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(f.local.resource, &after));
		UT_ASSERT(after.revision > f.local.revision);
		UT_ASSERT_EQ(after.last.version.mutation_token, f.last.version.mutation_token + 1);
		f.local = after; local_pi_coverage_filter = NULL;
		UT_ASSERT(pi_retire(&f, structural));
		pi_racing_fixture = NULL;
		pi_end();
	}
}

static bool
pi_resident_start(PiFixture *f, PcmState mode, ClusterPcmOwnSnapshot *base)
{
	ClusterPcmOwnSnapshot pending;
	int32 id = -1;

	if (!pi_start_target(f, true, 0, mode, PCM_STATE_X)) return false;
	if (cluster_pcm_lock_resource_x_acquire_s_source_handoff_snapshot_exact(
		&f->source.block, 0, &id, &pending) != RESOURCE_X_APPLY_APPLIED) return false;
	*base = pending; base->flags = 0;
	return cluster_pcm_lock_resource_x_acquire_s_source_handoff_exact(
		&f->source.block, 0, id, &pending, base) == RESOURCE_X_APPLY_APPLIED;
}

UT_TEST(test_resident_PI_requires_independent_exact_body_without_retiring_owners)
{
	for (int x = 0; x < 2; x++) {
		PiFixture f; ClusterPcmOwnSnapshot base; ClusterPcmResourceXMasterState before;
		ClusterPcmLocalPiSnapshotV1 after;
		UT_ASSERT(pi_resident_start(&f, x ? PCM_STATE_X : PCM_STATE_S, &base));
		before = *f.source.m.state;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_resident_image_preserved_exact(&base,
			f.source.page.data), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&before, f.source.m.state, sizeof(before)) == 0);
		UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(base.tag, &after));
		UT_ASSERT(memcmp(&after, &f.local, sizeof(after)) == 0);
		pi_end();
	}
}

UT_TEST(test_resident_PI_rejects_uncovered_bytes_identity_and_malformed_carrier)
{
	for (int fault = 0; fault < 7; fault++) {
		PiFixture f; ClusterPcmOwnSnapshot base; ClusterPcmResourceXMasterState before;
		PGIOAlignedBlock page;
		UT_ASSERT(pi_resident_start(&f, PCM_STATE_X, &base));
		memcpy(page.data, f.source.page.data, BLCKSZ);
		if (fault == 0) page.data[BLCKSZ - 1] ^= 1;
		if (fault == 1) base.generation++;
		if (fault == 2) base.reservation_token++;
		if (fault == 3) base.tag.blockNum++;
		if (fault == 4) base.writer_activation_token = 1;
		if (fault == 5) f.source.m.state->pi_carrier.reserved[0] = 1;
		if (fault == 6) {
			ClusterPageWalBindingV1 newer = f.last;
			newer.record_start = newer.record_end; newer.record_end += 10;
			newer.version.mutation_token++;
			UT_ASSERT(cluster_pcm_local_pi_record_v1(base.tag, &newer));
		}
		before = *f.source.m.state;
		UT_ASSERT(cluster_pcm_lock_resource_x_resident_image_preserved_exact(&base,
			page.data) != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&before, f.source.m.state, sizeof(before)) == 0);
		pi_end();
	}
}

UT_TEST(test_resident_PI_DATA_retirement_keeps_original_transport_and_allows_refill)
{
	PiFixture f; ClusterPcmOwnSnapshot base; ClusterPcmResourceXMasterState before;
	UT_ASSERT(pi_resident_start(&f, PCM_STATE_X, &base));
	local_pi_covered = true; UT_ASSERT(pi_retire(&f, false));
	before = *f.source.m.state;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_resident_image_preserved_exact(&base,
		f.source.page.data), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&before, f.source.m.state, sizeof(before)) == 0);
	UT_ASSERT_EQ(before.pi_carrier.transport_refs, 1);
	UT_ASSERT_EQ(before.pi_carrier.pi_refs, 0);
	pi_end();
}


/* The old body and its completion ACK have separate bounded owners. */
static ResourceXDecodedFrame
pi_capacity_successor(PiFixture *f)
{
	ResourceXDecodedFrame block = f->source.block;
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES];
	uint16 length = 0;
	ResourceXWireReject reject;
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
pi_capacity_observe(PiFixture *f, const ResourceXDecodedFrame *block,
	ResourceXApplyResult wanted)
{
	ClusterPcmResourceXMasterState before = *f->source.m.state;
	ClusterPcmLocalPiSnapshotV1 local, after;
	PGAlignedBlock page = f->source.page;
	int sleeps = fake_cv_sleep_count;
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(f->local.resource, &local));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_prepare_exact(block,
		2, 7, &f->source.prepared, f->source.page.data, &f->last, 83, 61), wanted);
	UT_ASSERT(memcmp(&before, f->source.m.state, sizeof(before)) == 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_service_prepare_exact(block,
		2, 7, &f->source.prepared, f->source.page.data, &f->last, 83, 61, 83, 61), wanted);
	UT_ASSERT(memcmp(&before, f->source.m.state, sizeof(before)) == 0);
	if (wanted == RESOURCE_X_APPLY_LOCAL_WAIT
		&& before.holder_status_intent.slot.kind == RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2) {
		ClusterPcmResourceXMasterState retained;
		ResourceXDecodedFrame forward; ResourceXWireReject reject;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(block, 2, 83, 61),
			before.holder_coverage_forward.valid ? RESOURCE_X_APPLY_DUPLICATE : RESOURCE_X_APPLY_APPLIED);
		retained = *f->source.m.state;
		UT_ASSERT_EQ(retained.holder_coverage_forward.valid, RESOURCE_X_HOLDER_SOURCE_FORWARD);
		UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCK_TO_N,
			retained.holder_coverage_forward.payload, retained.holder_coverage_forward.payload_bytes, &forward, &reject));
		UT_ASSERT(memcmp(&forward, block, sizeof(forward)) == 0);
		retained.holder_coverage_forward = before.holder_coverage_forward;
		UT_ASSERT(memcmp(&before, &retained, sizeof(before)) == 0);
	} else {
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(block, 2, 83, 61), wanted);
		UT_ASSERT(memcmp(&before, f->source.m.state, sizeof(before)) == 0);
	}
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(f->local.resource, &after));
	UT_ASSERT(memcmp(&local, &after, sizeof(local)) == 0);
	UT_ASSERT(memcmp(page.data, f->source.page.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(fake_cv_sleep_count, sleeps);
}
static bool
pi_capacity_claim(ResourceXSourceDrainWork *work)
{
	BackendType saved = MyBackendType;
	ResourceXIntentSlot slot; ResourceXAcquisitionRef delivery;
	uint8 payload[RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES]; uint32 examined; bool found = false;
	MyBackendType = B_LMS;
	for (int n = 0; n < 256; n++) {
		ResourceXIntentProbeResult result = cluster_pcm_lock_resource_x_outbound_work_probe_exact(
			4, &slot, payload, sizeof(payload), &examined, &delivery);
		UT_ASSERT(result != RESOURCE_X_INTENT_PROBE_CORRUPT);
		if (result == RESOURCE_X_INTENT_PROBE_SOURCE_DRAIN) { memcpy(work, payload, sizeof(*work)); found = true; break; }
		if (result == RESOURCE_X_INTENT_PROBE_IDLE || result == RESOURCE_X_INTENT_PROBE_COMPLETE) break;
	}
	MyBackendType = saved; return found;
}
static void
pi_capacity_lifetimes(PcmState mode, bool data_first)
{
	PiFixture f;
	ResourceXDecodedFrame block, request;
	ResourceXIntentSlot staged;
	ClusterPcmOwnSnapshot next;
	int wakes;
	if (!pi_start_mode(&f, true, 2, mode)) { UT_ASSERT(false); return; }
	block = pi_capacity_successor(&f); request = pi_request(&f);
	pi_capacity_observe(&f, &block, RESOURCE_X_APPLY_LOCAL_WAIT);
	if (data_first) {
		local_pi_covered = true; UT_ASSERT(pi_retire(&f, false));
		UT_ASSERT_EQ(f.source.m.state->pi_carrier.pi_refs, 0);
		UT_ASSERT_EQ(f.source.m.state->pi_carrier.transport_refs, 1);
		pi_capacity_observe(&f, &block, RESOURCE_X_APPLY_LOCAL_WAIT);
	}
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	if (!data_first) {
		local_pi_covered = true; UT_ASSERT(pi_retire(&f, false));
	}
	UT_ASSERT_EQ(f.source.m.state->holder_status_intent.slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
	pi_capacity_observe(&f, &block, RESOURCE_X_APPLY_LOCAL_WAIT);
	staged = clean_stage(&f.source.m.state->holder_status_intent.slot);
	UT_ASSERT_EQ(f.source.m.state->holder_image.valid, 0);
	UT_ASSERT_EQ(f.source.m.state->holder_status_intent.slot.state, RESOURCE_X_INTENT_SLOT_STAGED);
	pi_capacity_observe(&f, &block, RESOURCE_X_APPLY_LOCAL_WAIT);
	wakes = fake_cv_broadcast_count;
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged,
		true, ++fake_pcm_clock_us));
	UT_ASSERT(fake_cv_broadcast_count > wakes);
	UT_ASSERT_EQ(f.source.m.state->holder_status_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.pi_refs, 0);
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.transport_refs, 0);
	next = f.source.prepared; next.generation += 2; next.reservation_token += 2;
	{
		ResourceXSourceDrainWork work;
		if (!pi_capacity_claim(&work)) UT_ASSERT(pi_capacity_claim(&work));
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_coverage_claim_exact(&work, &block, true, false), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_capture_exact(&work,
			7, &next, f.source.page.data, &f.last, 83, 61), RESOURCE_X_APPLY_APPLIED);
	}
	UT_ASSERT_EQ(f.source.m.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PENDING);
	UT_ASSERT_EQ(f.source.m.state->holder_status.authority_generation, block.common.authority_generation);
	printf("# capacity mode=%d DATA-first=%d old-PI=0 old-transport=0 old-ACK=0 successor=owned\n", mode, data_first);
	pi_end();
}
UT_TEST(test_pi_capacity_S_transport_and_ACK_wait_for_original_owners)
{
	pi_capacity_lifetimes(PCM_STATE_S, true);
	pi_capacity_lifetimes(PCM_STATE_S, false);
}
UT_TEST(test_pi_capacity_X_transport_and_ACK_wait_for_original_owners)
{
	pi_capacity_lifetimes(PCM_STATE_X, true);
	pi_capacity_lifetimes(PCM_STATE_X, false);
}


UT_TEST(test_pi_capacity_does_not_turn_invalid_ACK_or_old_A_into_wait)
{
	PiFixture f;
	ResourceXDecodedFrame block, request;
	ResourceXIntentSlot staged;
	ClusterPcmResourceXMasterState before;
	int wakes;
	if (!pi_start_mode(&f, true, 2, PCM_STATE_S)) { UT_ASSERT(false); return; }
	block = pi_capacity_successor(&f); request = pi_request(&f);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	local_pi_covered = true; UT_ASSERT(pi_retire(&f, false));
	staged = clean_stage(&f.source.m.state->holder_status_intent.slot);
	before = *f.source.m.state;
	for (int cut = 0; cut < 5; cut++) {
		ResourceXApplyResult result;
		ClusterPcmResourceXMasterState changed;
		switch (cut) {
			case 0: f.source.m.state->holder_status_intent.payload[0] ^= 1; break;
			case 1: f.source.m.state->holder_status_intent.slot.state = RESOURCE_X_INTENT_SLOT_STAGED + 1; break;
			case 2: f.source.m.state->holder_status_intent.slot.authority_generation++; break;
			case 3: f.source.m.state->holder_status_intent.slot.body.owner_generation++; break;
			case 4: f.source.m.state->settlement_retry.pending = 1; break;
		}
		changed = *f.source.m.state;
		result = cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(&block, 2, 83, 61);
		UT_ASSERT(result != RESOURCE_X_APPLY_LOCAL_WAIT && result != RESOURCE_X_APPLY_APPLIED
			&& result != RESOURCE_X_APPLY_DUPLICATE);
		UT_ASSERT(memcmp(&changed, f.source.m.state, sizeof(changed)) == 0);
		result = cluster_pcm_lock_resource_x_acquire_s_source_service_prepare_exact(&block,
			2, 7, &f.source.prepared, f.source.page.data, &f.last, 83, 61, 83, 61);
		UT_ASSERT(result != RESOURCE_X_APPLY_LOCAL_WAIT && result != RESOURCE_X_APPLY_APPLIED
			&& result != RESOURCE_X_APPLY_DUPLICATE);
		UT_ASSERT(memcmp(&changed, f.source.m.state, sizeof(changed)) == 0);
		*f.source.m.state = before;
	}
	pi_capacity_observe(&f, &f.source.block, RESOURCE_X_APPLY_STALE);
	wakes = fake_cv_broadcast_count;
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged,
		true, ++fake_pcm_clock_us));
	UT_ASSERT(fake_cv_broadcast_count > wakes);
	wakes = fake_cv_broadcast_count;
	UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged,
		true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(fake_cv_broadcast_count, wakes);
	pi_end();
}

/* This adapter enters the actual ordinary round owner. Native completion
 * and peer receipt remain explicit provider inputs, not a network run. */
static ResourceXBootstrapRoundAction
pi_resident_round_domain(const ClusterPcmOwnSnapshot *base, int32 master, uint64 session)
{
	ResourceXAssertion assertion;
	ResourceXDecodedAcquireIntent episode = {0};
	ResourceXDecodedFrame dispatch;
	ResourceXAcquisitionRef terminal;
	UT_ASSERT(resource_x_assertion_init(&base->tag, cluster_node_id, &assertion));
	episode.requester_incarnation = cluster_qvotec_get_self_incarnation();
	episode.backend_generation = 44; episode.backend_id = 1; episode.buffer_id = 7;
	episode.base_generation = base->generation; episode.reservation_token = base->reservation_token + 1;
	episode.cluster_epoch = 17; episode.r4_generation = 83; episode.path = RESOURCE_X_ACQUIRE_RESIDENT;
	return cluster_pcm_lock_resource_x_acquire_round_step_exact(&assertion, master, 17, session,
		83, 51, 61, 1000, 900, 115, 50, base, &episode, &dispatch, &terminal);
}
static ResourceXBootstrapRoundAction
pi_resident_round(const ClusterPcmOwnSnapshot *base)
{
	return pi_resident_round_domain(base, 2, 777);
}
static bool
pi_resident_remote(PiFixture *f, ClusterPcmOwnSnapshot *base)
{
	if (!pi_start_target(f, true, 2, PCM_STATE_X, PCM_STATE_X)) return false;
	*base = f->source.finished; base->flags = 0;
	return cluster_pcm_lock_resource_x_acquire_s_source_handoff_exact(&f->source.block,
		2, 7, &f->source.finished, base) == RESOURCE_X_APPLY_APPLIED;
}
static ResourceXDecodedFrame
pi_resident_settlement(PiFixture *f)
{
	ResourceXDecodedFrame grant = f->source.block, request = {0};
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES]; uint16 length;
	ResourceXWireReject reject;
	grant.kind = RESOURCE_X_WIRE_AUTHORITY_GRANT;
	grant.common.action_node = grant.common.logical_assertion.requester_node;
	grant.common.sender_connection_generation = 1; /* Original retained canonical grant. */
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &grant,
		bytes, sizeof(bytes), &length, &reject));
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes,
		length, &grant, &reject));
	request.kind = RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2; request.has_acquire_decision = true;
	request.payload_bytes = RESOURCE_X_SOURCE_COMPLETE_V11_BYTES;
	request.common = grant.common; request.common.action_node = cluster_node_id;
	request.common.outcome = RESOURCE_X_OUTCOME_OK; request.common.sender_connection_generation = 61;
	request.body.acquire_completion.decision = grant.body.acquire_decision;
	request.body.acquire_completion.native_generation = grant.body.acquire_decision.episode.base_generation + 1;
	request.body.acquire_completion.decision_crc32c = grant.common.semantic_crc32c;
	request.body.acquire_completion.page_lsn = 144;
	request.body.acquire_completion.page_scn = f->source.ack.body.acquire_witness.original_proof.page_scn_lsn;
	request.body.acquire_completion.page_crc32c = f->source.ack.body.acquire_witness.original_proof.page_checksum;
	UT_ASSERT(clean_encode(&request));
	return request;
}
static bool
pi_resident_quiet(PiFixture *f, ClusterPcmOwnSnapshot *base, ResourceXIntentSlot *staged)
{
	ResourceXDecodedFrame request;
	if (!pi_resident_remote(f, base)) return false;
	clean_send(&f->source.m.state->holder_status_intent.slot);
	clean_send(&f->source.m.state->holder_image_intent);
	request = pi_resident_settlement(f);
	if (clean_receive(&request) != RESOURCE_X_APPLY_APPLIED) { UT_ASSERT(false); return false; }
	*staged = clean_stage(&f->source.m.state->holder_status_intent.slot);
	return true;
}
UT_TEST(test_resident_PI_waits_before_request_until_real_transport_and_ACK_quiet)
{
	PiFixture f; ClusterPcmOwnSnapshot base; ResourceXDecodedFrame request;
	ResourceXIntentSlot staged; ClusterPcmResourceXMasterState before;
	struct StopPcmEntryLayout *entry;
	if (!pi_resident_remote(&f, &base)) { UT_ASSERT(false); return; }
	entry = (struct StopPcmEntryLayout *)fake_pcm_entries.data[0]; before = *f.source.m.state;
	UT_ASSERT_EQ(pi_resident_round(&base), RESOURCE_X_BOOTSTRAP_ROUND_PREDECESSOR_WAIT);
	UT_ASSERT_EQ(entry->resource_x_bootstrap_round.highest_attempt_floor, 0);
	UT_ASSERT(memcmp(&before, f.source.m.state, sizeof(before)) == 0);
	clean_send(&f.source.m.state->holder_status_intent.slot);
	clean_send(&f.source.m.state->holder_image_intent);
	request = pi_resident_settlement(&f);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	staged = clean_stage(&f.source.m.state->holder_status_intent.slot);
	before = *f.source.m.state;
	UT_ASSERT_EQ(pi_resident_round(&base), RESOURCE_X_BOOTSTRAP_ROUND_PREDECESSOR_WAIT);
	UT_ASSERT_EQ(entry->resource_x_bootstrap_round.highest_attempt_floor, 0);
	UT_ASSERT(memcmp(&before, f.source.m.state, sizeof(before)) == 0);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged, true, ++fake_pcm_clock_us));
	before = *f.source.m.state;
	UT_ASSERT_EQ(pi_resident_round(&base), RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(pi_resident_round(&base), RESOURCE_X_BOOTSTRAP_ROUND_WAIT);
	UT_ASSERT(memcmp(&before, f.source.m.state, sizeof(before)) == 0);
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.pi_refs, 1);
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.transport_refs, 0);
	UT_ASSERT_EQ(entry->resource_x_bootstrap_round.highest_attempt_floor, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_resident_image_preserved_exact(&base,
		f.source.page.data), RESOURCE_X_APPLY_APPLIED);
	pi_end();
}
UT_TEST(test_resident_PI_pre_request_scope_native_and_receipt_guards)
{
	for (int fault = 0; fault < 9; fault++) {
		PiFixture f; ClusterPcmOwnSnapshot base; ResourceXIntentSlot staged;
		ClusterPcmResourceXMasterState before;
		if (!pi_resident_quiet(&f, &base, &staged)) { UT_ASSERT(false); return; }
		UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged, true, ++fake_pcm_clock_us));
		switch (fault) {
		case 0: base.generation++; break;
		case 1: base.reservation_token++; break;
		case 2: {
			ClusterPageWalBindingV1 later = f.last;
			later.record_start = later.record_end; later.record_end += 10; later.version.mutation_token++;
			UT_ASSERT(cluster_pcm_local_pi_record_v1(base.tag, &later)); break;
		}
		case 3: f.source.m.state->pi_carrier.reserved[0] = 1; break;
		case 4: f.source.m.state->settlement_retry.retired_authority++; break;
		case 5: f.source.m.state->settlement_retry.retired_resource_binding++; break;
		case 6: f.source.m.state->settlement_retry.retired_master_session++; break;
		case 7: f.source.m.state->settlement_retry.payload[0] ^= 1; break;
		case 8: fake_gcs_master_node = 3; break;
		}
		before = *f.source.m.state;
		UT_ASSERT_EQ(pi_resident_round(&base), fault < 3
			? RESOURCE_X_BOOTSTRAP_ROUND_BACKPRESSURE : RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED);
		UT_ASSERT(memcmp(&before, f.source.m.state, sizeof(before)) == 0);
		UT_ASSERT_EQ(((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->resource_x_bootstrap_round.highest_attempt_floor, 0);
		pi_end();
	}
}

UT_TEST(test_resident_PI_ACK_wait_keeps_exact_master_session)
{
	{
		PiFixture f; ClusterPcmOwnSnapshot base; ResourceXIntentSlot staged;
		struct StopPcmEntryLayout *entry;
		ClusterPcmResourceXMasterState before;
		if (!pi_resident_quiet(&f, &base, &staged)) { UT_ASSERT(false); return; }
		entry = (struct StopPcmEntryLayout *)fake_pcm_entries.data[0];
		{
			ResourceXAssertion assertion; ResourceXDecodedAcquireIntent episode = {0};
			ResourceXDecodedFrame dispatch; ResourceXAcquisitionRef terminal;
			UT_ASSERT(resource_x_assertion_init(&base.tag, cluster_node_id, &assertion));
			episode.requester_incarnation = cluster_qvotec_get_self_incarnation();
			episode.backend_generation = 44; episode.backend_id = 1; episode.buffer_id = 7;
			episode.base_generation = base.generation; episode.reservation_token = base.reservation_token + 1;
			episode.cluster_epoch = 17; episode.r4_generation = 83; episode.path = RESOURCE_X_ACQUIRE_RESIDENT;
			before = *f.source.m.state;
			UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_round_step_exact(&assertion, 2, 17, 778,
				83, 51, 61, 1000, 900, 115, 50, &base, &episode, &dispatch, &terminal),
				RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED);
			UT_ASSERT(memcmp(&before, f.source.m.state, sizeof(before)) == 0);
			UT_ASSERT_EQ(entry->resource_x_bootstrap_round.highest_attempt_floor, 0);
		}
		pi_end();
	}
}

UT_TEST(test_resident_PI_local_master_uses_retired_full_receipt_after_DONE)
{
	PiFixture f; ClusterPcmOwnSnapshot base;
	ResourceXDecodedFrame done, request, ack; ResourceXMasterSnapshot out;
	ClusterPcmResourceXMasterState before;
	if (!pi_start_target(&f, true, 0, PCM_STATE_X, PCM_STATE_X)) { UT_ASSERT(false); return; }
	base = f.source.finished; base.flags = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_handoff_exact(&f.source.block,
		0, 7, &f.source.finished, &base), RESOURCE_X_APPLY_APPLIED);
	clean_send(&f.source.m.state->holder_status_intent.slot);
	clean_send(&f.source.m.state->holder_image_intent);
	done = pi_resident_settlement(&f); done.kind = RESOURCE_X_WIRE_INSTALL_SETTLEMENT;
	done.common.action_node = done.common.logical_assertion.requester_node;
	done.common.sender_connection_generation = 71;
	UT_ASSERT(source_done_encode(&done));
	if (!source_done_keep(&f.source.m, &done)) { pi_end(); return; }
	UT_ASSERT_EQ(source_apply(&f.source.ack, 0), RESOURCE_X_APPLY_APPLIED);
	source_done_callback(&f.source.m, true);
	if (!clean_arm(&f.source, &request)) { pi_end(); return; }
	clean_send(&f.source.m.state->source_settlement.intent.slot);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	ack = clean_decode(f.source.m.state->holder_status_intent.payload, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_ack_exact(&ack, 0, &out), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.source.m.work), RESOURCE_X_APPLY_APPLIED);
	clean_send(&f.source.m.state->holder_status_intent.slot);
	clean_send(&f.source.m.state->grant_intent.slot);
	{
		uint8 empty[sizeof(f.source.m.state->requests[1])] = {0};
		UT_ASSERT(memcmp(&f.source.m.state->requests[1], empty, sizeof(empty)) == 0);
	}
	UT_ASSERT_EQ(f.source.m.state->deferred_settlement.present, 0);
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.pi_refs, 1);
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.transport_refs, 0);
	before = *f.source.m.state;
	UT_ASSERT_EQ(pi_resident_round_domain(&base, 0, f.source.block.common.master_session_incarnation),
		RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT(memcmp(&before, f.source.m.state, sizeof(before)) == 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_resident_image_preserved_exact(&base, f.source.page.data), RESOURCE_X_APPLY_APPLIED);
	pi_end();
}

/* No source pair or A exists for this original local PI. Native bytes and
 * the existing typed DATA receipt remain explicit fixture inputs. */
static bool
pi_bodyless_start(PiFixture *f, ClusterPcmOwnSnapshot *base)
{
	BufferTag tag = make_tag(850);
	memset(f, 0, sizeof(*f)); memset(base, 0, sizeof(*base));
	f->last = local_pi_setup(tag); f->first = f->last;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17), RESOURCE_X_APPLY_APPLIED);
	f->first.record_start = 80; f->first.record_end = 90;
	f->first.version.mutation_token = 70;
	if (!cluster_pcm_local_pi_record_first_v1(tag, &f->first, &f->last)
		|| !cluster_pcm_local_pi_snapshot_v1(tag, &f->local)) return false;
	base->tag = tag; base->generation = 901; base->reservation_token = 77;
	base->pcm_state = PCM_STATE_N; base->buffer_type = BUF_TYPE_PI;
	base->semantic_buf_state = BM_TAG_VALID | BM_VALID;
	((PageHeader)f->source.page.data)->pd_lower = SizeOfPageHeaderData;
	((PageHeader)f->source.page.data)->pd_upper = BLCKSZ;
	((PageHeader)f->source.page.data)->pd_special = BLCKSZ;
	((PageHeader)f->source.page.data)->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	PageXLogRecPtrSet(((PageHeader)f->source.page.data)->pd_lsn, f->last.record_end);
	if (!PageSetLSNOrigin(f->source.page.data, 0)) return false;
	((PageHeader)f->source.page.data)->pd_block_scn = f->last.version.mutation_token;
	return true;
}
UT_TEST(test_local_PI_without_A_seals_before_resident_overwrite)
{
	PiFixture f; ClusterPcmOwnSnapshot base; ClusterPcmLocalPiSnapshotV1 after;
	UT_ASSERT(pi_bodyless_start(&f, &base));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_resident_image_preserved_exact(&base,
		f.source.page.data), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(base.tag, &after));
	UT_ASSERT(memcmp(&f.local, &after, sizeof(after)) == 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_resident_image_preserved_exact(&base,
		f.source.page.data), RESOURCE_X_APPLY_APPLIED);
	pi_end();
}
UT_TEST(test_local_PI_cannot_be_sent_as_an_authorized_holder_image)
{
	PiFixture f; ClusterPcmOwnSnapshot base; ResourceXAssertion assertion;
	ResourceXDecodedFrame out = {0}, before = out;
	UT_ASSERT(pi_bodyless_start(&f, &base));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_resident_image_preserved_exact(&base,
		f.source.page.data), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&base.tag, 1, &assertion));
	UT_ASSERT(cluster_pcm_lock_resource_x_holder_image_exact(&assertion, &out) != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&before, &out, sizeof(out)) == 0);
	pi_end();
}
UT_TEST(test_local_PI_late_native_or_bytes_cannot_replace_original_body)
{
	for (unsigned cut = 0; cut < 5; cut++) {
		PiFixture f; ClusterPcmOwnSnapshot base, wrong; PGIOAlignedBlock page;
		UT_ASSERT(pi_bodyless_start(&f, &base));
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_resident_image_preserved_exact(&base,
			f.source.page.data), RESOURCE_X_APPLY_APPLIED);
		wrong = base; memcpy(page.data, f.source.page.data, BLCKSZ);
		if (cut == 0) wrong.generation++;
		else if (cut == 1) wrong.reservation_token++;
		else if (cut == 2) page.data[BLCKSZ-1] ^= 1;
		else if (cut == 3) wrong.tag.blockNum++;
		else wrong.writer_activation_token = 1;
		UT_ASSERT(cluster_pcm_lock_resource_x_resident_image_preserved_exact(&wrong,
			page.data) != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_resident_image_preserved_exact(&base,
			f.source.page.data), RESOURCE_X_APPLY_APPLIED);
		pi_end();
	}
}
UT_TEST(test_local_PI_requires_original_page_WAL_before_sealing)
{
	for (unsigned cut = 0; cut < 3; cut++) {
		PiFixture f; ClusterPcmOwnSnapshot base; PGIOAlignedBlock page;
		UT_ASSERT(pi_bodyless_start(&f, &base)); memcpy(page.data, f.source.page.data, BLCKSZ);
		if (cut == 0) ((PageHeader)page.data)->pd_block_scn++;
		else if (cut == 1) PageXLogRecPtrSet(((PageHeader)page.data)->pd_lsn, f.last.record_end+1);
		else UT_ASSERT(PageSetLSNOrigin(page.data, 1));
		UT_ASSERT(cluster_pcm_lock_resource_x_resident_image_preserved_exact(&base,
			page.data) != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_resident_image_preserved_exact(&base,
			f.source.page.data), RESOURCE_X_APPLY_APPLIED);
		pi_end();
	}
}
UT_TEST(test_local_PI_only_typed_DATA_can_retire_the_sealed_body)
{
	for (unsigned structural = 0; structural < 2; structural++) {
		PiFixture f; ClusterPcmOwnSnapshot base;
		UT_ASSERT(pi_bodyless_start(&f, &base));
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_resident_image_preserved_exact(&base,
			f.source.page.data), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(!pi_retire(&f, structural != 0));
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_resident_image_preserved_exact(&base,
			f.source.page.data), RESOURCE_X_APPLY_APPLIED);
		local_pi_covered = true; UT_ASSERT(pi_retire(&f, structural != 0));
		base.generation++; base.reservation_token++;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_resident_image_preserved_exact(&base,
			f.source.page.data), RESOURCE_X_APPLY_APPLIED);
		pi_end();
	}
}
UT_TEST(test_local_PI_membership_freeze_preserves_the_original_debt)
{
	PiFixture f; ClusterPcmOwnSnapshot base; ResourceXGateSnapshot gate;
	UT_ASSERT(pi_bodyless_start(&f, &base));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_resident_image_preserved_exact(&base,
		f.source.page.data), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&gate));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_fail_closed_exact(&gate), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_resident_image_preserved_exact(&base,
		f.source.page.data), RESOURCE_X_APPLY_APPLIED);
	local_pi_covered = true; UT_ASSERT(pi_retire(&f, false));
	pi_end();
}
UT_TEST(test_local_PI_stop_census_keeps_valid_recovery_debt)
{
	PiFixture f; ClusterPcmOwnSnapshot base; PcmGrdProtocolDebtStats debt;
	UT_ASSERT(pi_bodyless_start(&f, &base));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_resident_image_preserved_exact(&base,
		f.source.page.data), RESOURCE_X_APPLY_APPLIED);
	cluster_pcm_grd_protocol_debt_snapshot(&debt);
	UT_ASSERT_EQ(debt.invalid_entry_count, 0);
	UT_ASSERT(debt.retained_entry_count != 0);
	pi_end();
}

int
main(void)
{
	UT_PLAN(31);
	UT_RUN(test_pi_master_arms_settlement_without_retiring_pi);
	UT_RUN(test_pi_transport_settlement_keeps_original_body_and_wal_scope);
	UT_RUN(test_pi_retirement_after_transport_releases_body_with_ack_still_owned);
	UT_RUN(test_pi_retirement_before_transport_keeps_original_sends_and_body);
	UT_RUN(test_pi_wrong_latest_binding_cannot_publish_a_carrier);
	UT_RUN(test_pi_complete_chain_keeps_original_pi_bitmap_and_consumes_requester);
	UT_RUN(test_pi_data_and_structural_proof_must_cover_both_scopes);
	UT_RUN(test_pi_transport_replay_cannot_release_a_carrier_or_republish_old_a);
	UT_RUN(test_pi_membership_freeze_keeps_independent_carrier);
	UT_RUN(test_pi_malformed_carrier_is_invalid_in_original_stop_census);
	UT_RUN(test_pi_successor_waits_for_original_body_capacity);
	UT_RUN(test_pi_original_wal_floor_counts_both_owners_and_releases_exact_refs);
	UT_RUN(test_pi_quiet_and_no_image_cleanup_preserve_independent_wal_duty);
	UT_RUN(test_pi_proof_window_change_keeps_both_owned_responsibilities);
	UT_RUN(test_resident_PI_requires_independent_exact_body_without_retiring_owners);
	UT_RUN(test_resident_PI_rejects_uncovered_bytes_identity_and_malformed_carrier);
	UT_RUN(test_resident_PI_DATA_retirement_keeps_original_transport_and_allows_refill);
	UT_RUN(test_pi_capacity_S_transport_and_ACK_wait_for_original_owners);
	UT_RUN(test_pi_capacity_X_transport_and_ACK_wait_for_original_owners);
	UT_RUN(test_pi_capacity_does_not_turn_invalid_ACK_or_old_A_into_wait);
	UT_RUN(test_resident_PI_waits_before_request_until_real_transport_and_ACK_quiet);
	UT_RUN(test_resident_PI_pre_request_scope_native_and_receipt_guards);
	UT_RUN(test_resident_PI_ACK_wait_keeps_exact_master_session);
	UT_RUN(test_resident_PI_local_master_uses_retired_full_receipt_after_DONE);
	UT_RUN(test_local_PI_without_A_seals_before_resident_overwrite);
	UT_RUN(test_local_PI_cannot_be_sent_as_an_authorized_holder_image);
	UT_RUN(test_local_PI_late_native_or_bytes_cannot_replace_original_body);
	UT_RUN(test_local_PI_requires_original_page_WAL_before_sealing);
	UT_RUN(test_local_PI_only_typed_DATA_can_retire_the_sealed_body);
	UT_RUN(test_local_PI_membership_freeze_preserves_the_original_debt);
	UT_RUN(test_local_PI_stop_census_keeps_valid_recovery_debt);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
