/*-------------------------------------------------------------------------
 * test_cluster_resource_x_x_source_carrier.c
 *    X-to-S native receipts and independent PI/transport owners.
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_resource_x_x_source_carrier.c
 * NOTES
 *    Exact native completion, authenticated peers and typed DATA proofs are
 *    explicit input seams. This does not qualify an ordinary native caller.
 *-------------------------------------------------------------------------
 */
#include "test_cluster_resource_x_pi_fixture.inc"

static bool
xs_publish(PiFixture *f)
{
	CleanFixture *s = &f->source;
	ResourceXApplyResult result = cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(
		&s->block, 0, 7, &s->prepared, &s->finished);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED);
	return result == RESOURCE_X_APPLY_APPLIED;
}

static bool
xs_start(PiFixture *f, bool send)
{
	CleanFixture *s = &f->source;
	if (!pi_start_mode(f, false, 0, PCM_STATE_X) || !xs_publish(f)) return false;
	if (send) {
		clean_send(&s->m.state->holder_status_intent.slot);
		clean_send(&s->m.state->holder_image_intent);
	}
	return true;
}

static bool
xs_master_ready(PiFixture *f, int outcome, ResourceXDecodedFrame *request)
{
	CleanFixture *s = &f->source;
	s->done.common.outcome = outcome;
	if (outcome != RESOURCE_X_OUTCOME_OK)
		s->done.body.acquire_completion.native_generation = s->done.body.acquire_completion.decision.episode.base_generation;
	if (outcome == RESOURCE_X_OUTCOME_QUIET_NO_IMAGE) {
		s->done.body.acquire_completion.page_lsn = s->done.body.acquire_completion.page_scn = 0;
		s->done.body.acquire_completion.page_crc32c = 0;
	}
	UT_ASSERT(source_done_encode(&s->done));
	if (!source_done_keep(&s->m, &s->done)) return false;
	UT_ASSERT_EQ(source_apply(&s->ack, 0), RESOURCE_X_APPLY_APPLIED);
	source_done_callback(&s->m, true);
	return clean_arm(s, request);
}

UT_TEST(test_x_source_publishes_the_original_native_handoff_and_wal_lower)
{
	PiFixture f; ClusterPcmResourceXPiCarrier before; ClusterPageWalBindingV1 first;
	if (!xs_start(&f, false)) return;
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.transport_refs, 1);
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.pi_refs, 1);
	UT_ASSERT(cluster_pcm_own_snapshot_equal_exact(&f.source.m.state->pi_carrier.completed, &f.source.finished));
	UT_ASSERT_EQ(f.source.prepared.pcm_state, PCM_STATE_X);
	UT_ASSERT_EQ(f.source.finished.generation, f.source.prepared.generation + 1);
	UT_ASSERT_EQ(f.source.ack.body.acquire_witness.original_ack.flags, RESOURCE_X_COMMON_FLAG_PI_ESTABLISHED);
	UT_ASSERT(cluster_page_wal_ref_read_v1(&f.source.m.state->pi_carrier.first,
		BufTagGetRelFileLocator(&f.local.resource), f.local.resource.forkNum, f.local.resource.blockNum, &first));
	UT_ASSERT(cluster_page_wal_same_mutation_v1(&first, &f.first));
	before = f.source.m.state->pi_carrier;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(&f.source.block, 0, 7,
		&f.source.prepared, &f.source.finished), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&before, &f.source.m.state->pi_carrier, sizeof(before)) == 0);
	pi_end();
}

UT_TEST(test_x_source_only_exact_x_to_s_can_publish)
{
	for (int fault = 0; fault < 5; fault++) {
		PiFixture f; ClusterPcmResourceXMasterState before;
		if (!pi_start_mode(&f, false, 0, PCM_STATE_X)) return;
		before = *f.source.m.state;
		switch (fault) {
		case 0: f.source.finished.generation--; break;
		case 1: f.source.finished.reservation_token++; break;
		case 2: f.source.finished.pcm_state = PCM_STATE_X; break;
		case 3: f.source.finished.flags = PCM_OWN_FLAG_REVOKING; break;
		case 4: f.source.finished.semantic_buf_state |= BM_IO_ERROR; break;
		}
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(&f.source.block, 0, 7,
			&f.source.prepared, &f.source.finished), RESOURCE_X_APPLY_STALE);
		UT_ASSERT(memcmp(&before, f.source.m.state, sizeof(before)) == 0);
		pi_end();
	}
}

UT_TEST(test_x_source_pi_and_transport_retire_in_both_orders)
{
	for (int structural = 0; structural < 2; structural++) for (int pi_first = 0; pi_first < 2; pi_first++) {
		PiFixture f; ResourceXDecodedFrame request; ClusterPcmLocalPiSnapshotV1 after;
		if (!xs_start(&f, true)) return;
		request = pi_request(&f);
		if (pi_first) {
			local_pi_covered = true; UT_ASSERT(pi_retire(&f, structural));
			UT_ASSERT_EQ(f.source.m.state->pi_carrier.pi_refs, 0);
			UT_ASSERT_EQ(f.source.m.state->pi_carrier.transport_refs, 1);
		}
		UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
		if (!pi_first) {
			UT_ASSERT_EQ(f.source.m.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
			UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(f.local.resource, &after));
			UT_ASSERT(memcmp(&after, &f.local, sizeof(after)) == 0);
			local_pi_covered = true; UT_ASSERT(pi_retire(&f, structural));
		}
		UT_ASSERT_EQ(f.source.m.state->holder_image.valid, 0);
		UT_ASSERT_EQ(f.source.m.state->holder_status_intent.slot.kind, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
		clean_send(&f.source.m.state->holder_status_intent.slot);
		pi_end();
	}
}

UT_TEST(test_x_source_complete_master_chain_records_one_downgrade_for_every_outcome)
{
	for (int result = 0; result < 3; result++) {
		PiFixture f; ResourceXDecodedFrame request = {0}, ack; ResourceXMasterSnapshot out;
		ClusterPcmResourceXPiCarrier carrier; ResourceXHolderGrantProvenance source_grant;
		struct StopPcmEntryLayout *entry;
		uint64 transitions;
		int outcome = result == 0 ? RESOURCE_X_OUTCOME_OK : result == 1 ? RESOURCE_X_OUTCOME_QUIET : RESOURCE_X_OUTCOME_QUIET_NO_IMAGE;
		if (!xs_start(&f, true) || !xs_master_ready(&f, outcome, &request)) return;
		entry = (struct StopPcmEntryLayout *)fake_pcm_entries.data[0];
		source_grant = f.source.m.state->holder_grants[0];
		UT_ASSERT_EQ(pg_atomic_read_u32(&entry->master_state), PCM_STATE_X);
		UT_ASSERT_EQ(entry->x_holder_node, 0);
		transitions = pg_atomic_read_u64(&entry->transition_count_local);
		clean_send(&f.source.m.state->source_settlement.intent.slot);
		UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
		carrier = f.source.m.state->pi_carrier;
		ack = clean_decode(f.source.m.state->holder_status_intent.payload, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_ack_exact(&ack, 0, &out), RESOURCE_X_APPLY_APPLIED);
		clean_send(&f.source.m.state->holder_status_intent.slot);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.source.m.work), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(pg_atomic_read_u32(&entry->master_state), PCM_STATE_S);
		UT_ASSERT_EQ(entry->x_holder_node, -1);
		UT_ASSERT_EQ(pg_atomic_read_u32(&entry->s_holders_bitmap), result == 0 ? 3 : 1);
		UT_ASSERT_EQ(pg_atomic_read_u32(&entry->pi_holders_bitmap), 1);
		UT_ASSERT_EQ(pg_atomic_read_u64(&entry->transition_count_local), transitions + (result == 0 ? 2 : 1));
		UT_ASSERT(memcmp(&source_grant, &f.source.m.state->holder_grants[0], sizeof(source_grant)) == 0);
		UT_ASSERT(memcmp(&carrier, &f.source.m.state->pi_carrier, sizeof(carrier)) == 0);
		UT_ASSERT_EQ(f.source.m.state->acquire_ack_connections[0], 0);
		clean_send(&f.source.m.state->grant_intent.slot);
		UT_ASSERT_EQ(f.source.m.state->deferred_settlement.present, 0);
		UT_ASSERT_EQ(source_done_receive(&f.source.done), RESOURCE_X_APPLY_DUPLICATE);
		UT_ASSERT_EQ(pg_atomic_read_u64(&entry->transition_count_local), transitions + (result == 0 ? 2 : 1));
		pi_end();
	}
}

UT_TEST(test_x_source_original_sends_and_ack_gate_directory_downgrade)
{
	PiFixture f; ResourceXDecodedFrame request = {0}, ack; ResourceXMasterSnapshot out;
	struct StopPcmEntryLayout *entry;
	if (!xs_start(&f, false) || !xs_master_ready(&f, RESOURCE_X_OUTCOME_OK, &request)) return;
	entry = (struct StopPcmEntryLayout *)fake_pcm_entries.data[0];
	clean_send(&f.source.m.state->source_settlement.intent.slot);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_LOCAL_WAIT);
	clean_send(&f.source.m.state->holder_status_intent.slot);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.source.m.work), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT_EQ(entry->x_holder_node, 0);
	clean_send(&f.source.m.state->holder_image_intent);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.source.m.work), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT_EQ(entry->x_holder_node, 0);
	ack = clean_decode(f.source.m.state->holder_status_intent.payload, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_ack_exact(&ack, 0, &out), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.source.m.work), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(entry->x_holder_node, -1);
	pi_end();
}

UT_TEST(test_x_source_retired_pi_still_requires_an_exact_native_transport_receipt)
{
	PiFixture f; ResourceXDecodedFrame request;
	if (!pi_start_mode(&f, false, 0, PCM_STATE_X)) return;
	local_pi_covered = true; UT_ASSERT(pi_retire(&f, false));
	if (!xs_publish(&f)) return;
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.pi_refs, 0);
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.transport_refs, 1);
	UT_ASSERT(cluster_pcm_own_snapshot_equal_exact(&f.source.m.state->pi_carrier.completed, &f.source.finished));
	clean_send(&f.source.m.state->holder_status_intent.slot);
	clean_send(&f.source.m.state->holder_image_intent);
	request = pi_request(&f);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.source.m.state->holder_image.valid, 0);
	pi_end();
}

UT_TEST(test_x_source_late_old_publication_cannot_revive_transport)
{
	PiFixture f; ResourceXDecodedFrame request; ClusterPcmResourceXPiCarrier before;
	if (!xs_start(&f, true)) return;
	request = pi_request(&f);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	before = f.source.m.state->pi_carrier;
	clean_send(&f.source.m.state->holder_status_intent.slot);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(&f.source.block, 0, 7,
		&f.source.prepared, &f.source.finished), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(memcmp(&before, &f.source.m.state->pi_carrier, sizeof(before)) == 0);
	pi_end();
}

UT_TEST(test_x_source_pi_only_body_remains_visible_to_membership_and_wal_floor)
{
	PiFixture f; ResourceXDecodedFrame request; ClusterPcmLocalPiFloorV1 floor;
	ResourceXReconfigToken token; ResourceXReconfigBatch batch;
	ResourceXReconfigResult result = RESOURCE_X_RECONFIG_MORE;
	ClusterPcmResourceXPiCarrier before;
	if (!xs_start(&f, true)) return;
	request = pi_request(&f); UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_local_pi_floor_v1(&f.first.source, &floor));
	UT_ASSERT_EQ(floor.floor, 80); UT_ASSERT_EQ(floor.bounded, 2);
	before = f.source.m.state->pi_carrier;
	UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &token));
	for (int scan = 0; scan < 8 && (result == RESOURCE_X_RECONFIG_MORE || result == RESOURCE_X_RECONFIG_RETRY); scan++)
		result = cluster_resource_x_reconfig_sweep(&token, 4, &batch);
	UT_ASSERT_EQ(result, RESOURCE_X_RECONFIG_ORPHAN);
	UT_ASSERT(memcmp(&before, &f.source.m.state->pi_carrier, sizeof(before)) == 0);
	pi_end();
}

UT_TEST(test_x_source_both_directory_edges_fit_before_either_mutates)
{
	PiFixture f; ResourceXDecodedFrame request = {0}, ack; ResourceXMasterSnapshot out;
	ClusterPcmResourceXMasterState before; struct StopPcmEntryLayout *entry;
	if (!xs_start(&f, true) || !xs_master_ready(&f, RESOURCE_X_OUTCOME_OK, &request)) return;
	clean_send(&f.source.m.state->source_settlement.intent.slot);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	ack = clean_decode(f.source.m.state->holder_status_intent.payload, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_ack_exact(&ack, 0, &out), RESOURCE_X_APPLY_APPLIED);
	clean_send(&f.source.m.state->holder_status_intent.slot);
	entry = (struct StopPcmEntryLayout *)fake_pcm_entries.data[0];
	/* Exhaustion is an explicit input boundary, matched to the original head. */
	pg_atomic_write_u64(&entry->transition_count_local, UINT64_MAX - 2);
	f.source.m.state->requests[1].head_transition_generation = UINT64_MAX - 2;
	before = *f.source.m.state;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.source.m.work), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT(memcmp(&before, f.source.m.state, sizeof(before)) == 0);
	UT_ASSERT_EQ(entry->x_holder_node, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&entry->s_holders_bitmap), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&entry->transition_count_local), UINT64_MAX - 2);
	pg_atomic_write_u64(&entry->transition_count_local, UINT64_MAX - 3);
	f.source.m.state->requests[1].head_transition_generation = UINT64_MAX - 3;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.source.m.work), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(entry->x_holder_node, -1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&entry->transition_count_local), UINT64_MAX - 1);
	pi_end();
}

UT_TEST(test_clean_x_source_keeps_native_transport_receipt_without_wal_debt)
{
	CleanFixture f; ResourceXDecodedFrame request;
	if (!clean_start_mode(&f, RESOURCE_X_OUTCOME_OK, true, true, PCM_STATE_X)
		|| !clean_arm(&f, &request)) return;
	UT_ASSERT_EQ(f.m.state->pi_carrier.pi_refs, 0);
	UT_ASSERT_EQ(f.m.state->pi_carrier.transport_refs, 1);
	UT_ASSERT_EQ(f.m.state->pi_carrier.pi_revision, 0);
	UT_ASSERT(cluster_pcm_own_snapshot_equal_exact(&f.m.state->pi_carrier.completed, &f.finished));
	clean_send(&f.m.state->source_settlement.intent.slot);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.m.state->holder_image.valid, 0);
	UT_ASSERT_EQ(f.m.state->holder_status_intent.slot.kind, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(test_x_source_publishes_the_original_native_handoff_and_wal_lower);
	UT_RUN(test_x_source_only_exact_x_to_s_can_publish);
	UT_RUN(test_x_source_pi_and_transport_retire_in_both_orders);
	UT_RUN(test_x_source_complete_master_chain_records_one_downgrade_for_every_outcome);
	UT_RUN(test_x_source_original_sends_and_ack_gate_directory_downgrade);
	UT_RUN(test_x_source_retired_pi_still_requires_an_exact_native_transport_receipt);
	UT_RUN(test_x_source_late_old_publication_cannot_revive_transport);
	UT_RUN(test_x_source_pi_only_body_remains_visible_to_membership_and_wal_floor);
	UT_RUN(test_x_source_both_directory_edges_fit_before_either_mutates);
	UT_RUN(test_clean_x_source_keeps_native_transport_receipt_without_wal_debt);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
