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
pi_start_master(PiFixture *f, bool publish, int32 master)
{
	CleanFixture *s = &f->source;
	ResourceXDecodedFrame grant;
	ClusterPageWalBindingV1 seed = local_pi_setup(make_tag(850));

	memset(f, 0, sizeof(*f));
	if (!publish_start(2, PCM_STATE_S, &s->m)) return false;
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
	s->prepared.pcm_state = PCM_STATE_S; s->prepared.buffer_type = BUF_TYPE_SCUR;
	s->prepared.semantic_buf_state = BM_VALID;
	s->finished = s->prepared; s->finished.flags = 0;
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
	PiFixture f; ResourceXDecodedFrame request, block;
	ResourceXWireReject reject; uint8 bytes[RESOURCE_X_DECISION_V4_BYTES]; uint16 length;
	ClusterPcmResourceXMasterState before;
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
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(&block, 2, 83, 61), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT(memcmp(&before, f.source.m.state, sizeof(before)) == 0);
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

int
main(void)
{
	UT_PLAN(14);
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
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
