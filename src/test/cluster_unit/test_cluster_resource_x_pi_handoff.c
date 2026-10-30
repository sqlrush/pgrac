/*-------------------------------------------------------------------------
 * test_cluster_resource_x_pi_handoff.c
 *    Original S/X-to-N native fence and independent PI ownership.
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_resource_x_pi_handoff.c
 * NOTES
 *    Native completion and typed DATA are explicit provider inputs here.
 *-------------------------------------------------------------------------
 */
#include "test_cluster_resource_x_pi_fixture.inc"

static bool
handoff_start(PiFixture *f, PcmState mode)
{
	return pi_start_target(f, false, 0, mode, PCM_STATE_X);
}

UT_TEST(test_s_to_n_retains_original_image_and_wal_refs)
{
	PiFixture f;
	if (!handoff_start(&f, PCM_STATE_S)) return;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(&f.source.block,
		0, 7, &f.source.prepared, &f.source.finished), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.pi_refs, 1);
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.transport_refs, 1);
	UT_ASSERT(cluster_pcm_own_snapshot_equal_exact(&f.source.m.state->pi_carrier.completed, &f.source.finished));
	UT_ASSERT_EQ(f.source.m.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
	pi_end();
}

UT_TEST(test_x_to_n_retains_original_image_and_wal_refs)
{
	PiFixture f;
	if (!handoff_start(&f, PCM_STATE_X)) return;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(&f.source.block,
		0, 7, &f.source.prepared, &f.source.finished), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.pi_refs, 1);
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.transport_refs, 1);
	UT_ASSERT(cluster_pcm_own_snapshot_equal_exact(&f.source.m.state->pi_carrier.completed, &f.source.finished));
	pi_end();
}

static bool
handoff_published(PiFixture *f, PcmState mode)
{
	if (!handoff_start(f, mode)) return false;
	return cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(&f->source.block,
		0, 7, &f->source.prepared, &f->source.finished) == RESOURCE_X_APPLY_APPLIED;
}

UT_TEST(test_handoff_receipt_commits_once_without_retiring_pi_or_transport)
{
	for (int x = 0; x < 2; x++) {
		PiFixture f; ClusterPcmOwnSnapshot pending, quiet; int32 id = -1;
		ClusterPcmResourceXPiCarrier before;
		if (!handoff_published(&f, x ? PCM_STATE_X : PCM_STATE_S)) { UT_ASSERT(false); return; }
		before = f.source.m.state->pi_carrier;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_handoff_snapshot_exact(
			&f.source.block, 0, &id, &pending), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(id, 7); UT_ASSERT(cluster_pcm_own_snapshot_equal_exact(&pending, &f.source.finished));
		quiet = pending; quiet.flags = 0;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_handoff_exact(
			&f.source.block, 0, id, &pending, &quiet), RESOURCE_X_APPLY_APPLIED);
		before.completed = quiet;
		UT_ASSERT(memcmp(&before, &f.source.m.state->pi_carrier, sizeof(before)) == 0);
		UT_ASSERT_EQ(f.source.m.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
		UT_ASSERT_EQ(f.source.m.state->holder_image_intent.state, RESOURCE_X_INTENT_SLOT_ARMED);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_handoff_exact(
			&f.source.block, 0, id, &pending, &quiet), RESOURCE_X_APPLY_DUPLICATE);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_handoff_snapshot_exact(
			&f.source.block, 0, &id, &pending), RESOURCE_X_APPLY_DUPLICATE);
		UT_ASSERT(cluster_pcm_own_snapshot_equal_exact(&pending, &quiet));
		pi_end();
	}
}

UT_TEST(test_handoff_rejects_wrong_original_tuple_without_mutation)
{
	for (int fault = 0; fault < 7; fault++) {
		PiFixture f; ClusterPcmResourceXMasterState before;
		ClusterPcmOwnSnapshot pending, quiet; int32 id = 7;
		if (!handoff_published(&f, PCM_STATE_X)) { UT_ASSERT(false); return; }
		pending = f.source.finished; quiet = pending; quiet.flags = 0;
		before = *f.source.m.state;
		switch (fault) {
		case 0: id++; break;
		case 1: pending.reservation_token++; break;
		case 2: quiet.generation++; break;
		case 3: quiet.tag.blockNum++; break;
		case 4: quiet.writer_activation_token++; break;
		case 5: quiet.semantic_buf_state |= BM_IO_ERROR; break;
		case 6: quiet.flags = PCM_OWN_FLAG_REVOKING; break;
		}
		UT_ASSERT(cluster_pcm_lock_resource_x_acquire_s_source_handoff_exact(
			&f.source.block, 0, id, &pending, &quiet) != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&before, f.source.m.state, sizeof(before)) == 0);
		pi_end();
	}
}

UT_TEST(test_handoff_preserves_outputs_until_original_publication)
{
	PiFixture f; ClusterPcmOwnSnapshot output, before; int32 id = 31;
	if (!handoff_start(&f, PCM_STATE_S)) return;
	memset(&output, 0x5c, sizeof(output)); before = output;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_handoff_snapshot_exact(
		&f.source.block, 0, &id, &output), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT_EQ(id, 31); UT_ASSERT(memcmp(&before, &output, sizeof(output)) == 0);
	pi_end();
}

UT_TEST(test_handoff_reconfiguration_keeps_original_body_and_receipt)
{
	PiFixture f; ClusterPcmOwnSnapshot output; int32 id = 31;
	ClusterPcmResourceXMasterState before;
	if (!handoff_published(&f, PCM_STATE_X)) { UT_ASSERT(false); return; }
	before = *f.source.m.state; fake_gcs_master_node = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_handoff_snapshot_exact(
		&f.source.block, 0, &id, &output), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(id, 31); UT_ASSERT(memcmp(&before, f.source.m.state, sizeof(before)) == 0);
	fake_gcs_master_node = 0; pi_end();
}

UT_TEST(test_data_retirement_does_not_supply_native_handoff_proof)
{
	PiFixture f; ClusterPcmOwnSnapshot output; int32 id = -1;
	if (!handoff_published(&f, PCM_STATE_X)) { UT_ASSERT(false); return; }
	local_pi_covered = true;
	UT_ASSERT(pi_retire(&f, false));
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.pi_refs, 0);
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.transport_refs, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_handoff_snapshot_exact(
		&f.source.block, 0, &id, &output), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(output.flags, PCM_OWN_FLAG_REVOKING);
	UT_ASSERT_EQ(f.source.m.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
	pi_end();
}

UT_TEST(test_native_handoff_does_not_supply_data_retirement_proof)
{
	PiFixture f; ClusterPcmOwnSnapshot quiet; ClusterPcmLocalPiSnapshotV1 local;
	if (!handoff_published(&f, PCM_STATE_S)) { UT_ASSERT(false); return; }
	quiet = f.source.finished; quiet.flags = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_handoff_exact(
		&f.source.block, 0, 7, &f.source.finished, &quiet), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(f.local.resource, &local));
	UT_ASSERT(memcmp(&f.local, &local, sizeof(local)) == 0);
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.pi_refs, 1);
	local_pi_covered = true;
	UT_ASSERT(pi_retire(&f, true));
	UT_ASSERT_EQ(f.source.m.state->pi_carrier.pi_refs, 0);
	UT_ASSERT_EQ(f.source.m.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
	pi_end();
}

UT_TEST(test_pi_retired_before_publication_still_retains_native_transport_receipt)
{
	for (int x = 0; x < 2; x++) {
		PiFixture f; ClusterPcmOwnSnapshot quiet, output; int32 id = -1;
		if (!handoff_start(&f, x ? PCM_STATE_X : PCM_STATE_S)) return;
		local_pi_covered = true; UT_ASSERT(pi_retire(&f, false));
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(&f.source.block,
			0, 7, &f.source.prepared, &f.source.finished), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(f.source.m.state->pi_carrier.pi_refs, 0);
		UT_ASSERT_EQ(f.source.m.state->pi_carrier.transport_refs, 1);
		quiet = f.source.finished; quiet.flags = 0;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_handoff_exact(
			&f.source.block, 0, 7, &f.source.finished, &quiet), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_handoff_snapshot_exact(
			&f.source.block, 0, &id, &output), RESOURCE_X_APPLY_DUPLICATE);
		UT_ASSERT(cluster_pcm_own_snapshot_equal_exact(&quiet, &output));
		UT_ASSERT_EQ(f.source.m.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
		pi_end();
	}
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(test_s_to_n_retains_original_image_and_wal_refs);
	UT_RUN(test_x_to_n_retains_original_image_and_wal_refs);
	UT_RUN(test_handoff_receipt_commits_once_without_retiring_pi_or_transport);
	UT_RUN(test_handoff_rejects_wrong_original_tuple_without_mutation);
	UT_RUN(test_handoff_preserves_outputs_until_original_publication);
	UT_RUN(test_handoff_reconfiguration_keeps_original_body_and_receipt);
	UT_RUN(test_data_retirement_does_not_supply_native_handoff_proof);
	UT_RUN(test_native_handoff_does_not_supply_data_retirement_proof);
	UT_RUN(test_pi_retired_before_publication_still_retains_native_transport_receipt);
	UT_DONE();
	return ut_failed_count != 0;
}
