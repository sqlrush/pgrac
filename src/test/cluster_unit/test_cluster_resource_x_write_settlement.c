/*-------------------------------------------------------------------------
 * test_cluster_resource_x_write_settlement.c
 *    Full-A write-source settlement and independent master completion.
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_resource_x_write_settlement.c
 * NOTES
 *    Native completion, authenticated peers and typed DATA are explicit
 *    provider inputs. No ordinary requester-X native qualification here.
 *-------------------------------------------------------------------------
 */
#include "test_cluster_resource_x_pi_fixture.inc"

static bool
write_start(PiFixture *f, PcmState mode, bool quiet)
{
	CleanFixture *s = &f->source;
	ClusterPcmOwnSnapshot finished;
	if (!pi_start_target(f, false, 0, mode, PCM_STATE_X)) return false;
	if (cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(&s->block,
		0, 7, &s->prepared, &s->finished) != RESOURCE_X_APPLY_APPLIED) { UT_ASSERT(false); return false; }
	if (quiet) {
		finished = s->finished; finished.flags = 0;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_handoff_exact(&s->block,
			0, 7, &s->finished, &finished), RESOURCE_X_APPLY_APPLIED);
	}
	return true;
}

static bool
write_done(PiFixture *f, int outcome)
{
	CleanFixture *s = &f->source;
	ResourceXDecodedFrame grant = publish_decode(s->m.state->grant_intent.payload, false);
	ResourceXDecodedAcquireCompletion *c;
	bool valid;
	s->done.kind = RESOURCE_X_WIRE_INSTALL_SETTLEMENT;
	s->done.has_acquire_decision = true;
	s->done.payload_bytes = RESOURCE_X_SOURCE_COMPLETE_V11_BYTES;
	s->done.common = grant.common; s->done.common.action_node = 1;
	s->done.common.outcome = outcome; s->done.common.sender_connection_generation = 71;
	c = &s->done.body.acquire_completion;
	c->decision = grant.body.acquire_decision;
	c->native_generation = c->decision.episode.base_generation
		+ (outcome == RESOURCE_X_OUTCOME_OK || outcome == RESOURCE_X_OUTCOME_DUPLICATE ? 1 : 0);
	c->decision_crc32c = grant.common.semantic_crc32c;
	if (outcome != RESOURCE_X_OUTCOME_QUIET_NO_IMAGE && outcome != RESOURCE_X_OUTCOME_QUIET_NO_IMAGE_RETIRED) {
		c->page_lsn = 144;
		c->page_scn = s->ack.body.acquire_witness.original_proof.page_scn_lsn;
		c->page_crc32c = s->ack.body.acquire_witness.original_proof.page_checksum;
	}
	if (outcome == RESOURCE_X_OUTCOME_DUPLICATE || outcome == RESOURCE_X_OUTCOME_QUIET_RETIRED
		|| outcome == RESOURCE_X_OUTCOME_QUIET_NO_IMAGE_RETIRED) s->done.common.action_node = 0;
	valid = source_done_encode(&s->done);
	UT_ASSERT(valid);
	return valid;
}

static ResourceXDecodedFrame
write_request(PiFixture *f)
{
	ResourceXDecodedFrame request = f->source.done;
	request.kind = RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2;
	request.common.action_node = 0;
	UT_ASSERT(clean_encode(&request));
	return request;
}

UT_TEST(test_write_source_all_six_complete_receipts_retain_full_authority)
{
	const int outcomes[] = {RESOURCE_X_OUTCOME_OK, RESOURCE_X_OUTCOME_DUPLICATE,
		RESOURCE_X_OUTCOME_QUIET, RESOURCE_X_OUTCOME_QUIET_RETIRED,
		RESOURCE_X_OUTCOME_QUIET_NO_IMAGE, RESOURCE_X_OUTCOME_QUIET_NO_IMAGE_RETIRED};
	for (int x = 0; x < 2; x++) for (int n = 0; n < 6; n++) {
		PiFixture f;
		if (!write_start(&f, x ? PCM_STATE_X : PCM_STATE_S, true)) return;
		UT_ASSERT(write_done(&f, outcomes[n]));
		UT_ASSERT_EQ(f.source.done.common.target_mode, PCM_STATE_X);
		UT_ASSERT_EQ(f.source.done.body.acquire_completion.decision.holders_bitmap, x ? 1 : 13);
		UT_ASSERT_EQ(f.source.done.body.acquire_completion.decision.holders[0].boot_incarnation,
			f.source.block.body.acquire_decision.holders[0].boot_incarnation);
		pi_end();
	}
}

UT_TEST(test_write_source_settlement_waits_exact_native_quiet_receipt)
{
	PiFixture f; ResourceXDecodedFrame request; ClusterPcmResourceXPiCarrier before;
	if (!write_start(&f, PCM_STATE_X, false)) return;
	if (!write_done(&f, RESOURCE_X_OUTCOME_OK)) { UT_ASSERT(false); return; }
	request = write_request(&f); before = f.source.m.state->pi_carrier;
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT(memcmp(&before, &f.source.m.state->pi_carrier, sizeof(before)) == 0);
	UT_ASSERT_EQ(f.source.m.state->settlement_retry.pending, 0);
	pi_end();
}

UT_TEST(test_write_source_settlement_keeps_pi_and_independent_quiet_receipt)
{
	PiFixture f; ResourceXDecodedFrame request; ClusterPcmOwnSnapshot quiet = {0}; int32 id = -1;
	if (!write_start(&f, PCM_STATE_S, true)) return;
	if (!write_done(&f, RESOURCE_X_OUTCOME_OK)) { UT_ASSERT(false); return; }
	clean_send(&f.source.m.state->holder_status_intent.slot);
	clean_send(&f.source.m.state->holder_image_intent);
	request = write_request(&f);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.pi_refs, 1);
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.transport_refs, 0);
	UT_ASSERT_EQ(f.source.m.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_handoff_snapshot_exact(
		&f.source.block, 0, &id, &quiet), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(quiet.flags, 0); UT_ASSERT_EQ(id, 7);
	pi_end();
}

static bool
write_master_receipts(PiFixture *f, int outcome, bool callbacks)
{
	CleanFixture *s = &f->source;
	uint32 required = s->block.body.acquire_decision.required_holders_bitmap;
	if (!write_done(f, outcome)) { UT_ASSERT(false); return false; }
	if (!source_done_keep(&s->m, &s->done)) return false;
	for (int h = 0; h < RESOURCE_X_PROTOCOL_NODE_LIMIT; h++) {
		ResourceXDecodedFrame ack;
		if ((required & (UINT32_C(1) << h)) == 0) continue;
		ack = h == 0 ? s->ack : source_ack(&s->m, h);
		UT_ASSERT_EQ(source_apply(&ack, h), RESOURCE_X_APPLY_APPLIED);
		if (callbacks) clean_send(&s->m.state->block_intents[h].slot);
	}
	return true;
}

static bool
write_arm(PiFixture *f, int outcome, ResourceXDecodedFrame *request)
{
	if (!write_master_receipts(f, outcome, true)) return false;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f->source.m.work), RESOURCE_X_APPLY_LOCAL_WAIT);
	if (f->source.m.state->source_settlement.state != RESOURCE_X_SOURCE_SETTLEMENT_PENDING) {
		UT_ASSERT(false); return false;
	}
	*request = clean_decode(f->source.m.state->source_settlement.intent.payload, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2);
	return true;
}

static bool
write_settle(PiFixture *f, ResourceXDecodedFrame *request)
{
	ResourceXDecodedFrame ack; ResourceXMasterSnapshot out;
	clean_send(&f->source.m.state->source_settlement.intent.slot);
	clean_send(&f->source.m.state->holder_status_intent.slot);
	clean_send(&f->source.m.state->holder_image_intent);
	UT_ASSERT_EQ(clean_receive(request), RESOURCE_X_APPLY_APPLIED);
	ack = clean_decode(f->source.m.state->holder_status_intent.payload, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_ack_exact(&ack, 0, &out), RESOURCE_X_APPLY_APPLIED);
	clean_send(&f->source.m.state->holder_status_intent.slot);
	return true;
}

UT_TEST(test_write_master_requires_every_holder_receipt_and_callback)
{
	PiFixture f; ClusterPcmResourceXMasterState before; ResourceXDecodedFrame out;
	if (!write_start(&f, PCM_STATE_S, true) || !write_master_receipts(&f, RESOURCE_X_OUTCOME_OK, false)) return;
	before = *f.source.m.state;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_source_done_snapshot_exact(&f.source.m.work, &out), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT(memcmp(&before, f.source.m.state, sizeof(before)) == 0);
	for (int h = 0; h < 4; h++) if (h == 0 || h == 2 || h == 3) clean_send(&f.source.m.state->block_intents[h].slot);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.source.m.work), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT_EQ(f.source.m.state->source_settlement.state, RESOURCE_X_SOURCE_SETTLEMENT_PENDING);
	pi_end();
}

UT_TEST(test_write_master_completion_records_all_revocations_and_only_successful_x)
{
	const int outcomes[] = {RESOURCE_X_OUTCOME_OK, RESOURCE_X_OUTCOME_QUIET, RESOURCE_X_OUTCOME_QUIET_NO_IMAGE};
	for (int x = 0; x < 2; x++) for (int n = 0; n < 3; n++) {
		PiFixture f; ResourceXDecodedFrame request; struct StopPcmEntryLayout *entry;
		uint64 before;
		if (!write_start(&f, x ? PCM_STATE_X : PCM_STATE_S, true)
			|| !write_arm(&f, outcomes[n], &request) || !write_settle(&f, &request)) return;
		entry = (struct StopPcmEntryLayout *)fake_pcm_entries.data[0]; before = pg_atomic_read_u64(&entry->transition_count_local);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.source.m.work), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(entry->x_holder_node, n ? -1 : 1);
		UT_ASSERT_EQ(pg_atomic_read_u32(&entry->master_state), n ? PCM_STATE_N : PCM_STATE_X);
		UT_ASSERT_EQ(pg_atomic_read_u32(&entry->s_holders_bitmap), 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&entry->pi_holders_bitmap), 1);
		UT_ASSERT_EQ(pg_atomic_read_u64(&entry->transition_count_local), before + (x ? 1 : 3) + (n ? 0 : 1));
		for (int h = 0; h < 4; h++) UT_ASSERT_EQ(f.source.m.state->acquire_ack_connections[h], 0);
		UT_ASSERT_EQ(f.source.m.state->holder_grants[1].grant_generation, n ? 0 : request.common.authority_generation);
		UT_ASSERT_EQ(f.source.m.state->pi_carrier.pi_refs, 1);
		/* The canonical work has retired; only its exact DONE may replay. */
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.source.m.work), RESOURCE_X_APPLY_STALE);
		UT_ASSERT_EQ(pg_atomic_read_u64(&entry->transition_count_local), before + (x ? 1 : 3) + (n ? 0 : 1));
		clean_send(&f.source.m.state->grant_intent.slot);
		UT_ASSERT_EQ(f.source.m.state->deferred_settlement.present, 0);
		UT_ASSERT_EQ(source_done_receive(&f.source.done), RESOURCE_X_APPLY_DUPLICATE);
		pi_end();
	}
}

UT_TEST(test_write_master_checks_whole_transition_budget_before_any_revocation)
{
	PiFixture f; ResourceXDecodedFrame request; struct StopPcmEntryLayout *entry;
	ClusterPcmResourceXMasterState before;
	if (!write_start(&f, PCM_STATE_S, true) || !write_arm(&f, RESOURCE_X_OUTCOME_OK, &request)
		|| !write_settle(&f, &request)) return;
	entry = (struct StopPcmEntryLayout *)fake_pcm_entries.data[0];
	pg_atomic_write_u64(&entry->transition_count_local, UINT64_MAX - 4);
	f.source.m.state->requests[1].head_transition_generation = UINT64_MAX - 4;
	before = *f.source.m.state;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.source.m.work), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT(memcmp(&before, f.source.m.state, sizeof(before)) == 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&entry->s_holders_bitmap), 13);
	UT_ASSERT_EQ(pg_atomic_read_u64(&entry->transition_count_local), UINT64_MAX - 4);
	pg_atomic_write_u64(&entry->transition_count_local, UINT64_MAX - 5);
	f.source.m.state->requests[1].head_transition_generation = UINT64_MAX - 5;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.source.m.work), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(pg_atomic_read_u64(&entry->transition_count_local), UINT64_MAX - 1);
	pi_end();
}

UT_TEST(test_write_source_inflight_and_failed_callbacks_keep_original_carrier)
{
	PiFixture f; ResourceXDecodedFrame request; ResourceXIntentSlot pending;
	if (!write_start(&f, PCM_STATE_X, true) || !write_done(&f, RESOURCE_X_OUTCOME_QUIET)) return;
	request = write_request(&f);
	pending = clean_stage(&f.source.m.state->holder_image_intent);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT_EQ(f.source.m.state->settlement_retry.pending, 1);
	clean_send(&f.source.m.state->holder_status_intent.slot);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&pending, false, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.transport_refs, 1);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_LOCAL_WAIT);
	fake_pcm_clock_us += 500000; /* Existing bounded transport retry cadence. */
	clean_send(&f.source.m.state->holder_image_intent);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.transport_refs, 0);
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.pi_refs, 1);
	pi_end();
}

UT_TEST(test_write_source_data_retirement_is_independent_in_both_orders)
{
	for (int data_first = 0; data_first < 2; data_first++) {
		PiFixture f; ResourceXDecodedFrame request;
		if (!write_start(&f, PCM_STATE_X, true) || !write_done(&f, RESOURCE_X_OUTCOME_OK)) return;
		request = write_request(&f); local_pi_covered = true;
		if (data_first) UT_ASSERT(pi_retire(&f, false));
		clean_send(&f.source.m.state->holder_status_intent.slot);
		clean_send(&f.source.m.state->holder_image_intent);
		UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(f.source.m.state->holder_image.valid, data_first ? 0 : RESOURCE_X_HOLDER_PAIR_PUBLISHED);
		if (!data_first) UT_ASSERT(pi_retire(&f, true));
		UT_ASSERT_EQ(f.source.m.state->holder_image.valid, 0);
		UT_ASSERT_EQ(f.source.m.state->holder_status_intent.slot.kind, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
		pi_end();
	}
}

UT_TEST(test_write_source_late_replay_never_reopens_transport_or_native_fence)
{
	PiFixture f; ResourceXDecodedFrame request; ClusterPcmResourceXPiCarrier before;
	if (!write_start(&f, PCM_STATE_S, true) || !write_done(&f, RESOURCE_X_OUTCOME_OK)) return;
	request = write_request(&f);
	clean_send(&f.source.m.state->holder_status_intent.slot);
	clean_send(&f.source.m.state->holder_image_intent);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	before = f.source.m.state->pi_carrier;
	clean_send(&f.source.m.state->holder_status_intent.slot);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(&f.source.block, 0,
		7, &f.source.prepared, &f.source.finished), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(memcmp(&before, &f.source.m.state->pi_carrier, sizeof(before)) == 0);
	pi_end();
}

UT_TEST(test_write_source_mismatched_full_receipt_cannot_retire_original_pair)
{
	for (int fault = 0; fault < 6; fault++) {
		PiFixture f; ResourceXDecodedFrame request; ClusterPcmResourceXMasterState before;
		ResourceXApplyResult result;
		if (!write_start(&f, PCM_STATE_X, true) || !write_done(&f, RESOURCE_X_OUTCOME_OK)) return;
		request = write_request(&f);
		switch (fault) {
		case 0: request.body.acquire_completion.page_crc32c++; break;
		case 1: request.body.acquire_completion.decision.episode.reservation_token++; break;
		case 2: request.common.semantic_crc32c++; break;
		case 3: request.common.master_session_incarnation++; break;
		case 4: request.body.acquire_completion.decision_crc32c++; break;
		case 5: request.body.acquire_completion.decision.holders[0].boot_incarnation++; break;
		}
		if (fault != 2) UT_ASSERT(clean_encode(&request));
		before = *f.source.m.state;
		result = clean_receive(&request);
		UT_ASSERT(result != RESOURCE_X_APPLY_APPLIED && result != RESOURCE_X_APPLY_DUPLICATE);
		UT_ASSERT(memcmp(&before, f.source.m.state, sizeof(before)) == 0);
		pi_end();
	}
}

UT_TEST(test_write_master_source_ack_and_physical_callback_are_independent)
{
	PiFixture f; ResourceXDecodedFrame request, ack; ResourceXMasterSnapshot out;
	ResourceXIntentSlot pending; ClusterPcmResourceXMasterState before;
	if (!write_start(&f, PCM_STATE_S, true) || !write_arm(&f, RESOURCE_X_OUTCOME_QUIET, &request)) return;
	pending = clean_stage(&f.source.m.state->source_settlement.intent.slot);
	clean_send(&f.source.m.state->holder_status_intent.slot);
	clean_send(&f.source.m.state->holder_image_intent);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	ack = clean_decode(f.source.m.state->holder_status_intent.payload, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
	ack.body.acquire_completion.page_scn++; UT_ASSERT(clean_encode(&ack));
	before = *f.source.m.state;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_ack_exact(&ack, 0, &out), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(memcmp(&before, f.source.m.state, sizeof(before)) == 0);
	ack = clean_decode(f.source.m.state->holder_status_intent.payload, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_ack_exact(&ack, 0, &out), RESOURCE_X_APPLY_APPLIED);
	before = *f.source.m.state;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.source.m.work), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT(memcmp(&before, f.source.m.state, sizeof(before)) == 0);
	UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_intent_complete_exact(&pending));
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&pending, false, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.source.m.work), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->x_holder_node, -1);
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.pi_refs, 1);
	clean_send(&f.source.m.state->holder_status_intent.slot);
	clean_send(&f.source.m.state->grant_intent.slot);
	UT_ASSERT_EQ(f.source.m.state->deferred_settlement.present, 0);
	pi_end();
}

UT_TEST(test_write_source_ack_send_refusal_keeps_its_exact_receipt)
{
	PiFixture f; ResourceXDecodedFrame request; ResourceXIntentSlot pending;
	ClusterPcmResourceXSettlementRetry before;
	if (!write_start(&f, PCM_STATE_X, true) || !write_done(&f, RESOURCE_X_OUTCOME_OK)) return;
	request = write_request(&f);
	clean_send(&f.source.m.state->holder_status_intent.slot);
	clean_send(&f.source.m.state->holder_image_intent);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	before = f.source.m.state->settlement_retry;
	pending = f.source.m.state->holder_status_intent.slot;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_not_admitted_exact(&pending,
		++fake_pcm_clock_us), RESOURCE_X_INTENT_NOT_ADMITTED);
	fake_pcm_clock_us += 500000;
	pending = clean_stage(&f.source.m.state->holder_status_intent.slot);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&pending, false, ++fake_pcm_clock_us));
	UT_ASSERT(memcmp(&before, &f.source.m.state->settlement_retry, sizeof(before)) == 0);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_DUPLICATE);
	fake_pcm_clock_us += 500000;
	clean_send(&f.source.m.state->holder_status_intent.slot);
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.pi_refs, 1);
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.transport_refs, 0);
	pi_end();
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(test_write_source_all_six_complete_receipts_retain_full_authority);
	UT_RUN(test_write_source_settlement_waits_exact_native_quiet_receipt);
	UT_RUN(test_write_source_settlement_keeps_pi_and_independent_quiet_receipt);
	UT_RUN(test_write_master_requires_every_holder_receipt_and_callback);
	UT_RUN(test_write_master_completion_records_all_revocations_and_only_successful_x);
	UT_RUN(test_write_master_checks_whole_transition_budget_before_any_revocation);
	UT_RUN(test_write_source_inflight_and_failed_callbacks_keep_original_carrier);
	UT_RUN(test_write_source_data_retirement_is_independent_in_both_orders);
	UT_RUN(test_write_source_late_replay_never_reopens_transport_or_native_fence);
	UT_RUN(test_write_source_mismatched_full_receipt_cannot_retire_original_pair);
	UT_RUN(test_write_master_source_ack_and_physical_callback_are_independent);
	UT_RUN(test_write_source_ack_send_refusal_keeps_its_exact_receipt);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
