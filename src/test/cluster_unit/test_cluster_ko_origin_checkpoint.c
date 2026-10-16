/* Native origin checkpoint through the actual relation KO producer.
 * Buffer IO, PCM/WB census, GES and transport are explicit boundaries.
 * Author: SqlRush <sqlrush@gmail.com> */
int ko_stop_fixture_main(void);
#define main ko_stop_fixture_main
#include "test_cluster_ko_stop.c"
#undef main

static RelationData origin_relation;
static bool origin_cursor_drift;

/* The AM04 diagnostic surface is intentionally queried by reason name so the
 * unit test proves that the production loop records the predicate which sent
 * it back to the latch.  These declarations are RED until the shared dump
 * accessors are added. */
extern uint64 cluster_ko_origin_checkpoint_restart_count(const char *reason);
extern uint64 cluster_ko_origin_checkpoint_wait_count(const char *reason);

static void
prepare_origin(void)
{
	v3_native_structure_setup(&origin_relation);
	MyDatabaseId = origin_relation.rd_locator.dbOid;
}

static void
run_origin(uint16 operation)
{
	cluster_ko_flush_and_wait_ack_relation(&origin_relation, RELPERSISTENCE_PERMANENT, operation);
}

UT_TEST(test_origin_joins_checkpoint_before_native_producer_returns)
{
	prepare_origin();
	run_origin(CLUSTER_KO_SHARED_V3_OPERATION_DROP);
	UT_ASSERT_EQ(origin_flush_calls, 1);
	UT_ASSERT_EQ(origin_invalidate_calls, 1);
	UT_ASSERT_EQ(origin_synced_forks, existing_forks);
	UT_ASSERT(local_relation_scan_calls > 0);
	UT_ASSERT(ko_completions != NULL && ko_completions->native_pending);
	UT_ASSERT_EQ(sent_v3_request.base.origin_node, cluster_node_id);
	UT_ASSERT_NE(sent_v3_request.base.peer_node, cluster_node_id);
	UT_ASSERT_EQ(marker_registrations, 1);
	UT_ASSERT_EQ(native_fork_unlinks, 0);
	native_fixture_shared_memory_end();
}

static ClusterBufferDrainResult
check_origin_admission(bool flush, uint32 *budget, uint32 *cursor)
{
	ClusterKoPeerAdmissionV3 sample;
	RelFileLocator other = origin_relation.rd_locator;
	uint64 serial = 0, revision = 0;
	bool retry = false;
	UT_ASSERT(!cluster_ko_peer_admission_begin_v3(origin_relation.rd_locator, &sample));
	UT_ASSERT(!cluster_ko_create_begin_v3(origin_relation.rd_locator, &serial));
	UT_ASSERT(!cluster_ko_extend_begin_v3(origin_relation.rd_locator, MAIN_FORKNUM,
		&serial, &retry));
	UT_ASSERT(retry);
	UT_ASSERT_EQ(serial, 0);
	UT_ASSERT(!cluster_ko_peer_read_begin_v3(origin_relation.rd_locator, SPACE_FORKNUM, 0,
		&sample));
	/* Only already retained protocol work may finish; no self-peer request. */
	UT_ASSERT(cluster_ko_peer_delivery_begin_v3(origin_relation.rd_locator, &revision));
	other.relNumber++;
	UT_ASSERT(cluster_ko_peer_admission_begin_v3(other, &sample));
	return CLUSTER_BUFFER_DRAIN_EMPTY;
}

UT_TEST(test_origin_fence_blocks_new_work_while_old_work_drains)
{
	prepare_origin();
	origin_drain_hook = check_origin_admission;
	run_origin(CLUSTER_KO_SHARED_V3_OPERATION_DROP);
	UT_ASSERT_EQ(origin_drain_calls, 2);
	native_fixture_shared_memory_end();
}

static ClusterBufferDrainResult
wait_origin_buffer(bool flush, uint32 *budget, uint32 *cursor)
{
	return CLUSTER_BUFFER_DRAIN_WAIT;
}

static void
cancel_origin_wait(void)
{
	pg_re_throw();
}

UT_TEST(test_origin_cancel_retains_accepted_operation_without_closed_result)
{
	volatile bool caught = false;
	prepare_origin();
	origin_drain_hook = wait_origin_buffer;
	origin_wait_hook = cancel_origin_wait;
	PG_TRY(); { run_origin(CLUSTER_KO_SHARED_V3_OPERATION_DROP); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(origin_waits, 1);
	UT_ASSERT_EQ(origin_sync_calls + origin_invalidate_calls + local_relation_scan_calls, 0);
	UT_ASSERT(ko_completions != NULL && !ko_completions->native_pending);
	UT_ASSERT(storage.contexts[0].used && storage.contexts[0].request_sent_peers != 0);
	native_fixture_shared_memory_end();
}

static void
fail_origin_sync(ForkNumber fork)
{
	pg_re_throw();
}

UT_TEST(test_origin_sync_error_cannot_invalidate_or_publish_local_closed)
{
	volatile bool caught = false;
	prepare_origin();
	origin_sync_hook = fail_origin_sync;
	PG_TRY(); { run_origin(CLUSTER_KO_SHARED_V3_OPERATION_DROP); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(origin_sync_calls, 1);
	UT_ASSERT_EQ(origin_invalidate_calls + local_relation_scan_calls, 0);
	UT_ASSERT(ko_completions != NULL && !ko_completions->native_pending);
	native_fixture_shared_memory_end();
}

static void
finish_origin_local_owners(void)
{
	local_relation_scan_result = CLUSTER_PCM_PI_RELATION_EMPTY;
	local_writeback_closed = true;
}

static void
advance_origin_pi_scan_after_pending(void)
{
	if (origin_waits == 1) {
		/* The first bounded pass made progress; the next pass is pending. */
		local_relation_scan_result = CLUSTER_PCM_PI_RELATION_PENDING;
		local_relation_scan_advance = 0;
	} else if (origin_waits == 2) {
		/* Once the pending owner settles, the same bounded scan must resume. */
		local_relation_scan_result = CLUSTER_PCM_PI_RELATION_EMPTY;
		local_relation_scan_advance = 0;
	}
}

UT_TEST(test_origin_pi_pending_resumes_from_bounded_cursor)
{
	prepare_origin();
	local_relation_scan_result = CLUSTER_PCM_PI_RELATION_MORE;
	local_relation_scan_advance = 128;
	local_writeback_closed = true;
	origin_wait_hook = advance_origin_pi_scan_after_pending;
	run_origin(CLUSTER_KO_SHARED_V3_OPERATION_DROP);
	UT_ASSERT_EQ(local_relation_scan_calls, 3);
	/* A PENDING result must not restart the complete registry scan at zero. */
	UT_ASSERT_EQ(local_relation_scan_input, 128);
	native_fixture_shared_memory_end();
}

UT_TEST(test_origin_waits_for_local_protocol_and_physical_ack_owners)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		prepare_origin();
		local_relation_scan_result = fault == 0 ? CLUSTER_PCM_PI_RELATION_PENDING
			: CLUSTER_PCM_PI_RELATION_EMPTY;
		local_writeback_closed = fault == 0;
		origin_wait_hook = finish_origin_local_owners;
		run_origin(CLUSTER_KO_SHARED_V3_OPERATION_DROP);
		UT_ASSERT_EQ(origin_waits, 1);
		UT_ASSERT_EQ(origin_flush_calls, 2);
		UT_ASSERT_EQ(local_relation_scan_calls, 2);
		native_fixture_shared_memory_end();
	}
}

UT_TEST(test_origin_checkpoint_publishes_pi_scan_restart_reason)
{
	uint64 restart_before, wait_before;

	prepare_origin();
	restart_before = cluster_ko_origin_checkpoint_restart_count("pi_scan_pending");
	wait_before = cluster_ko_origin_checkpoint_wait_count("pi_scan_pending");
	local_relation_scan_result = CLUSTER_PCM_PI_RELATION_PENDING;
	local_writeback_closed = false;
	origin_wait_hook = finish_origin_local_owners;
	run_origin(CLUSTER_KO_SHARED_V3_OPERATION_DROP);
	UT_ASSERT(cluster_ko_origin_checkpoint_restart_count("pi_scan_pending") > restart_before);
	UT_ASSERT(cluster_ko_origin_checkpoint_wait_count("pi_scan_pending") > wait_before);
	native_fixture_shared_memory_end();
}

static ClusterBufferDrainResult
resume_origin_buffer(bool flush, uint32 *budget, uint32 *cursor)
{
	if (origin_drain_calls == 1) {
		UT_ASSERT(flush && *cursor == 0 && *budget == 128);
		*cursor += *budget;
		*budget = 0;
		return CLUSTER_BUFFER_DRAIN_MORE;
	}
	if (origin_drain_calls == 2)
		UT_ASSERT_EQ(*cursor, origin_cursor_drift ? 0 : 128);
	return CLUSTER_BUFFER_DRAIN_EMPTY;
}

static void
drift_origin_gate(void)
{
	if (origin_cursor_drift)
		relation_gate_freeze++;
}

UT_TEST(test_origin_resumes_bounded_prefix_only_under_the_same_gate)
{
	for (unsigned drift = 0; drift < 2; drift++) {
		prepare_origin();
		origin_cursor_drift = drift != 0;
		origin_drain_hook = resume_origin_buffer;
		origin_wait_hook = drift_origin_gate;
		run_origin(CLUSTER_KO_SHARED_V3_OPERATION_DROP);
		UT_ASSERT_EQ(origin_waits, 1);
		UT_ASSERT_EQ(origin_flush_calls, 2);
		UT_ASSERT_EQ(origin_invalidate_calls, 1);
		native_fixture_shared_memory_end();
	}
}

UT_TEST(test_only_exact_native_space_writer_can_enter_after_old_data_closed)
{
	ClusterKoPeerAdmissionV3 sample, rejected;
	prepare_origin();
	run_origin(CLUSTER_KO_SHARED_V3_OPERATION_DROP);
	UT_ASSERT(!cluster_ko_peer_admission_begin_v3(origin_relation.rd_locator, &rejected));
	UT_ASSERT(cluster_ko_peer_read_begin_v3(origin_relation.rd_locator, SPACE_FORKNUM, 0, &sample));
	UT_ASSERT(!cluster_ko_peer_admission_current_v3(&sample, origin_relation.rd_locator));
	UT_ASSERT(cluster_ko_peer_read_current_v3(&sample, origin_relation.rd_locator, SPACE_FORKNUM, 0));
	UT_ASSERT(!cluster_ko_peer_read_current_v3(&sample, origin_relation.rd_locator, MAIN_FORKNUM, 0));
	MyProcPid++;
	UT_ASSERT(!cluster_ko_peer_read_begin_v3(origin_relation.rd_locator, SPACE_FORKNUM, 0, &rejected));
	UT_ASSERT(!cluster_ko_peer_read_current_v3(&sample, origin_relation.rd_locator, SPACE_FORKNUM, 0));
	MyProcPid--;
	(void)pg_atomic_fetch_add_u64(&storage.peer_admission_revision, 1);
	UT_ASSERT(!cluster_ko_peer_read_current_v3(&sample, origin_relation.rd_locator, SPACE_FORKNUM, 0));
	UT_ASSERT(!cluster_ko_peer_read_begin_v3(origin_relation.rd_locator, SPACE_FORKNUM, 2, &rejected));
	native_fixture_shared_memory_end();
}

static unsigned origin_wb_checks;
static void
drift_at_origin_closed_publish(void)
{
	if (++origin_wb_checks == 2) {
		relation_gate_freeze++;
		local_writeback_sample_hook = NULL;
	}
}

UT_TEST(test_origin_rechecks_atomic_gate_at_closed_publication)
{
	prepare_origin();
	origin_wb_checks = 0;
	local_writeback_sample_hook = drift_at_origin_closed_publish;
	run_origin(CLUSTER_KO_SHARED_V3_OPERATION_DROP);
	UT_ASSERT_EQ(origin_waits, 1);
	UT_ASSERT_EQ(origin_flush_calls, 2);
	UT_ASSERT_EQ(local_relation_scan_calls, 2);
	native_fixture_shared_memory_end();
}

UT_TEST(test_origin_scan_error_keeps_the_exact_accepted_owner)
{
	volatile bool caught = false;
	ClusterKoSharedContext proof;
	ClusterKoPeerAdmissionV3 admission;
	prepare_origin();
	local_relation_scan_drift = 4;
	PG_TRY(); { run_origin(CLUSTER_KO_SHARED_V3_OPERATION_DROP); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(origin_invalidate_calls, 1);
	UT_ASSERT_EQ(local_relation_scan_calls, 1);
	UT_ASSERT(!ko_completion_snapshot(ko_completions, &proof));
	UT_ASSERT(!cluster_ko_peer_admission_begin_v3(origin_relation.rd_locator, &admission));
	UT_ASSERT(!ko_completions->native_pending);
	native_fixture_shared_memory_end();
}

static ClusterBufferDrainResult
change_origin_authority(bool flush, uint32 *budget, uint32 *cursor)
{
	v3_local_ax = false;
	return CLUSTER_BUFFER_DRAIN_EMPTY;
}

UT_TEST(test_origin_lost_ax_cannot_sync_or_publish_closed)
{
	volatile bool caught = false;
	ClusterKoSharedContext proof;
	ClusterKoPeerAdmissionV3 admission;
	prepare_origin();
	origin_drain_hook = change_origin_authority;
	expecting_error = true;
	PG_TRY(); { run_origin(CLUSTER_KO_SHARED_V3_OPERATION_DROP); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(origin_sync_calls + origin_invalidate_calls + local_relation_scan_calls, 0);
	UT_ASSERT(!ko_completion_snapshot(ko_completions, &proof) && !ko_completions->native_pending);
	UT_ASSERT(!cluster_ko_peer_admission_begin_v3(origin_relation.rd_locator, &admission));
	v3_local_ax = true;
	native_fixture_shared_memory_end();
}

UT_TEST(test_origin_space_owner_expires_on_claim_scope_or_error_cleanup)
{
	ClusterKoPeerAdmissionV3 sample;
	prepare_origin();
	run_origin(CLUSTER_KO_SHARED_V3_OPERATION_DROP);
	UT_ASSERT(cluster_ko_peer_read_begin_v3(origin_relation.rd_locator, SPACE_FORKNUM, 1, &sample));
	v3_local_ax = false;
	UT_ASSERT(!cluster_ko_peer_read_begin_v3(origin_relation.rd_locator, SPACE_FORKNUM, 1, &sample));
	v3_local_ax = true;
	UT_ASSERT(cluster_ko_peer_read_begin_v3(origin_relation.rd_locator, SPACE_FORKNUM, 1, &sample));
	CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)12;
	UT_ASSERT(!cluster_ko_peer_read_current_v3(&sample, origin_relation.rd_locator, SPACE_FORKNUM, 1));
	CurrentResourceOwner = CurTransactionResourceOwner = TopTransactionResourceOwner;
	resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	UT_ASSERT(!cluster_ko_peer_read_current_v3(&sample, origin_relation.rd_locator, SPACE_FORKNUM, 1));
	UT_ASSERT(storage.contexts[0].used && storage.contexts[0].origin_detached);
	UT_ASSERT(!cluster_ko_peer_admission_begin_v3(origin_relation.rd_locator, &sample));
	native_fixture_shared_memory_end();
}

UT_TEST(test_in_place_truncate_keeps_its_original_origin_path)
{
	prepare_origin();
	run_origin(CLUSTER_KO_SHARED_V3_OPERATION_TRUNCATE);
	UT_ASSERT_EQ(origin_drain_calls + origin_sync_calls + local_relation_scan_calls, 0);
	native_fixture_shared_memory_end();
}

int
main(void)
{
	UT_PLAN(14);
	UT_RUN(test_origin_joins_checkpoint_before_native_producer_returns);
	UT_RUN(test_origin_fence_blocks_new_work_while_old_work_drains);
	UT_RUN(test_origin_cancel_retains_accepted_operation_without_closed_result);
	UT_RUN(test_origin_sync_error_cannot_invalidate_or_publish_local_closed);
	UT_RUN(test_origin_pi_pending_resumes_from_bounded_cursor);
	UT_RUN(test_origin_waits_for_local_protocol_and_physical_ack_owners);
	UT_RUN(test_origin_checkpoint_publishes_pi_scan_restart_reason);
	UT_RUN(test_origin_resumes_bounded_prefix_only_under_the_same_gate);
	UT_RUN(test_only_exact_native_space_writer_can_enter_after_old_data_closed);
	UT_RUN(test_origin_rechecks_atomic_gate_at_closed_publication);
	UT_RUN(test_origin_scan_error_keeps_the_exact_accepted_owner);
	UT_RUN(test_origin_lost_ax_cannot_sync_or_publish_closed);
	UT_RUN(test_origin_space_owner_expires_on_claim_scope_or_error_cleanup);
	UT_RUN(test_in_place_truncate_keeps_its_original_origin_path);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
