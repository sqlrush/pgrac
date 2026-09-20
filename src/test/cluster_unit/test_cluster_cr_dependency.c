/* Author: SqlRush <sqlrush@gmail.com>
 * Exercise the actual worker drain and production slot/ship owners. Only
 * clock, origin activity and transport boundaries are controlled fixtures.
 * Portions Copyright (c) 2026, pgrac contributors
 */
int reservation_fixture_main(void);
#define main reservation_fixture_main
#include "test_cluster_r4_slot_reservation.c"
#undef main
#include "cluster/cluster_update_trace.h"

static bool dependency_origin_pending;
static int dependency_legacy_calls;
static uint32 cluster_lms_cr_legacy_drain_cursor;
static UtClusterCrServerShared *dependency_shared;

void
cluster_gcs_block_r4_tx_resolve_drain(void)
{}

bool
cluster_gcs_block_r4_tx_resolve_active(void)
{
	return dependency_origin_pending;
}

static void
dependency_noop(void)
{}

/* This fixture exercises drain ownership, with the observer disabled. */
static void
cr_server_trace_origin_gate(bool active pg_attribute_unused())
{
	UT_ASSERT(!cluster_update_trace_enabled);
}

static void
dependency_legacy_serve(ClusterLmsCrSlot *slot)
{
	(void)slot;
	dependency_legacy_calls++;
}

extern void cluster_cr_server_test_r4_maintain_dependencies(void);
void dependency_real_drain(void);
#define CrServerShared dependency_shared
#define cluster_lms_cr_drain dependency_real_drain
#define cr_server_r4_claim_queued cluster_cr_server_test_r4_claim_queued
#define cr_server_r4_build_step cluster_cr_server_test_r4_build_step
#define cr_server_r4_ship_terminal cluster_cr_server_test_r4_ship_terminal
#define cr_server_r4_send_foreign_undo cluster_cr_server_test_r4_send_foreign_undo
#define cr_server_r4_maintain_dependencies cluster_cr_server_test_r4_maintain_dependencies
#define cr_server_r4_note_origin_deferral dependency_noop
#define cr_serve_slot dependency_legacy_serve
#define cluster_lmon_duty_mark_dirty(duty) ((void)0)
#define cluster_lmon_wakeup dependency_noop
#include "test_cluster_cr_dependency_drain.inc"

UT_TEST(test_lost_dependency_retires_while_origin_busy_and_stop_sealed)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot = prepare_worker0_undo_inflight(&state);
	const char *reason;
	int index;
	int builds = ut_builder_step_calls;

	dependency_origin_pending = true;
	ut_stop_new_work_allowed = false;
	ut_now += (int64)cluster_gcs_reply_timeout_ms * 1000 - 1;
	dependency_real_drain();
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_UNDO_INFLIGHT);
	UT_ASSERT_EQ(ut_send_calls, 1);
	ut_now++;
	dependency_real_drain();
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_FREE);
	UT_ASSERT_EQ(ut_builder_step_calls, builds);
	UT_ASSERT_EQ(ut_send_calls, 2);
	UT_ASSERT_EQ(((GcsBlockReplyHeader *)ut_send_payload)->status,
				 GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED);
	UT_ASSERT(bytes_are(ut_send_payload + sizeof(GcsBlockReplyHeader), BLCKSZ, 0));
	UT_ASSERT_EQ(ut_forget_calls, 1);
	UT_ASSERT_EQ(ut_leave_calls, 1);
	UT_ASSERT_EQ(cluster_cr_server_normal_stop_poll(&index, &reason), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(dependency_legacy_calls, 0);
}

UT_TEST(test_dependency_terminal_backpressure_retains_owner_until_send)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot = prepare_worker0_undo_inflight(&state);
	GcsBlockReplyHeader header;
	ClusterGcsUndoAuthTrailer auth;
	ClusterICEnvelope env;
	char page[BLCKSZ];

	make_foreign_undo_reply(&header, &auth, &env, page);
	dependency_origin_pending = true;
	ut_send_result = CLUSTER_IC_SEND_NOT_ADMITTED;
	ut_now += (int64)cluster_gcs_reply_timeout_ms * 1000;
	dependency_real_drain();
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_READY_RETRY);
	UT_ASSERT_EQ(ut_forget_calls, 0);
	UT_ASSERT_EQ(ut_leave_calls, 0);
	UT_ASSERT(!cluster_cr_server_r4_land_foreign_undo(&env, &header, page, &auth));
	ut_send_result = CLUSTER_IC_SEND_DONE;
	dependency_real_drain();
	UT_ASSERT(slot_is_canonical_free_with_generation(slot, 1));
	UT_ASSERT_EQ(ut_forget_calls, 1);
	UT_ASSERT_EQ(ut_leave_calls, 1);
	UT_ASSERT(!cluster_cr_server_r4_land_foreign_undo(&env, &header, page, &auth));
}

UT_TEST(test_reply_wins_and_maintenance_never_builds_under_origin_guard)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot = prepare_worker0_undo_inflight(&state);
	GcsBlockReplyHeader header;
	ClusterGcsUndoAuthTrailer auth;
	ClusterICEnvelope env;
	char page[BLCKSZ];
	int builds = ut_builder_step_calls;

	make_foreign_undo_reply(&header, &auth, &env, page);
	UT_ASSERT(cluster_cr_server_r4_land_foreign_undo(&env, &header, page, &auth));
	dependency_origin_pending = true;
	ut_now += (int64)cluster_gcs_reply_timeout_ms * 1000;
	dependency_real_drain();
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->state), CLUSTER_LMS_CR_R4_UNDO_READY);
	UT_ASSERT_EQ(ut_builder_step_calls, builds);
	UT_ASSERT_EQ(ut_send_calls, 1);
}

UT_TEST(test_dependency_wrong_worker_or_reused_generation_never_retires)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot = prepare_worker0_undo_inflight(&state);
	ClusterLmsCrSlot before;

	dependency_origin_pending = true;
	ut_now += (int64)cluster_gcs_reply_timeout_ms * 1000;
	ut_data_worker_id = 1;
	before = *slot;
	dependency_real_drain();
	UT_ASSERT_EQ(memcmp(slot, &before, sizeof(before)), 0);
	ut_data_worker_id = 0;
	slot->r4.slot_generation++;
	before = *slot;
	dependency_real_drain();
	UT_ASSERT_EQ(memcmp(slot, &before, sizeof(before)), 0);
	UT_ASSERT_EQ(ut_send_calls, 1);
}

UT_TEST(test_full_image_fence_loss_returns_zero_body_retry)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot = prepare_worker0_claim(&state);

	UT_ASSERT(cluster_cr_server_test_r4_claim_queued(0));
	UT_ASSERT(cluster_cr_server_test_r4_build_step(0));
	ut_write_fence_enforcing = true;
	ut_write_fence_allowed = false;
	UT_ASSERT(cluster_cr_server_test_r4_ship_terminal(0));
	UT_ASSERT_EQ(ut_send_calls, 1);
	UT_ASSERT_EQ(((GcsBlockReplyHeader *)ut_send_payload)->status,
				 GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED);
	UT_ASSERT_EQ(((GcsBlockReplyHeader *)ut_send_payload)->page_lsn, 0);
	UT_ASSERT(bytes_are(ut_send_payload + sizeof(GcsBlockReplyHeader), BLCKSZ, 0));
	UT_ASSERT(slot_is_canonical_free_with_generation(slot, 1));
}

UT_TEST(test_admission_loss_never_sends_image_or_unproved_reply)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot *slot = prepare_worker0_claim(&state);

	UT_ASSERT(cluster_cr_server_test_r4_claim_queued(0));
	UT_ASSERT(cluster_cr_server_test_r4_build_step(0));
	ut_recheck_ok = false;
	UT_ASSERT(cluster_cr_server_test_r4_ship_terminal(0));
	UT_ASSERT_EQ(ut_send_calls, 0);
	UT_ASSERT(slot_is_canonical_free_with_generation(slot, 1));
}

int
main(void)
{
	dependency_shared = &ut_cr_server_shared;
	UT_PLAN(6);
	UT_RUN(test_lost_dependency_retires_while_origin_busy_and_stop_sealed);
	UT_RUN(test_dependency_terminal_backpressure_retains_owner_until_send);
	UT_RUN(test_reply_wins_and_maintenance_never_builds_under_origin_guard);
	UT_RUN(test_dependency_wrong_worker_or_reused_generation_never_retires);
	UT_RUN(test_full_image_fence_loss_returns_zero_body_retry);
	UT_RUN(test_admission_loss_never_sends_image_or_unproved_reply);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
