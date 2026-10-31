/*-------------------------------------------------------------------------
 * test_cluster_mx_operation_origin.c
 *   Exact MX context through the real original three-current lifecycle.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Current/storage/admission and the original FullXid source are explicit
 * leaves of the existing complete origin fixture; no native TT40 claim.
 *-------------------------------------------------------------------------
 */
int mx_unused_origin_main(int argc, char **argv);
#define main mx_unused_origin_main
#include "test_cluster_r4_tx_outcome.c"
#undef main

typedef struct MxOriginFixture {
	OperationPlanFixture origin;
	ClusterSavepointOperationKey key;
	ClusterRuntimeVisibilityOperationRead read;
	ClusterSavepointEffectProof effect;
} MxOriginFixture;

static MxOriginFixture *mx_origin_observed;
static int mx_origin_action;
static int mx_origin_action_release;

static void
mx_origin_release(void)
{
	ClusterSavepointEffectProof zero = { 0 };
	UT_ASSERT_EQ(memcmp(&mx_origin_observed->effect, &zero, sizeof(zero)), 0);
	UT_ASSERT_EQ(test_operation_pin_depth, 0);
	if (test_candidate_release_calls != mx_origin_action_release)
		return;
	switch (mx_origin_action) {
	case 1:
		operation_plan_effect(CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
		break;
	case 2:
		operation_plan_effect(CLUSTER_SAVEPOINT_EFFECT_LIVE);
		break;
	case 3:
		test_candidate_data_generation++;
		break;
	case 4:
		test_admission_revoked = true;
		break;
	case 5:
		test_candidate_root_drift = true;
		/* The shared root leaf selects drift after its regular-call fence;
		 * this test deliberately uses terminal-census root dispatch. */
		test_regular_root_resolve_calls = 2;
		break;
	case 6:
		((UndoRecordHeader *)(test_operation_page.data + sizeof(UndoBlockHeader)))->target_offset++;
		break;
	case 7:
		operation_plan_tail()[offsetof(UndoOperationTrailer, command_id)]++;
		break;
	case 8:
		operation_plan_tail()[offsetof(UndoOperationTrailer, operation_sequence)]++;
		break;
	case 9:
		pg_re_throw();
		break;
	}
}

static void
mx_origin_init(MxOriginFixture *f, bool same)
{
	memset(f, 0, sizeof(*f));
	cluster_node_id = 0;
	operation_plan_init(&f->origin, same, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	f->key = (ClusterSavepointOperationKey){
		.locator = test_origin_locator,
		.target
		= { .locator = { 1663, 42, 77 }, .forknum = MAIN_FORKNUM, .blockno = 23, .offnum = 3 },
		.top_fxid = FullTransactionIdFromEpochAndXid(9, TEST_ORIGIN_XID),
		.operation_fxid = FullTransactionIdFromEpochAndXid(9, TEST_ORIGIN_XID + 1)
	};
	test_original_available = true;
	test_original_full_xid = f->key.top_fxid;
	test_original_key
		= (ClusterCtrcTxnKeyV1){ .format_version = CLUSTER_CTRC_FORMAT_VERSION,
								 .owner_instance = 1,
								 .origin_node_id = 0,
								 .segment_id = same ? TEST_RECORD_SEGMENT : TEST_TT_SEGMENT,
								 .segment_generation = same ? 17 : 23,
								 .slot_offset = TEST_TT_OFFSET,
								 .slot_wrap = TEST_ORIGIN_WRAP,
								 .xid = TEST_ORIGIN_XID,
								 .cluster_epoch = 42,
								 .system_identifier = UINT64_C(0x1122334455667788),
								 .origin_boot_incarnation = 9001,
								 .formation_epoch = 42,
								 .admission_record_generation = 5,
								 .root_descriptor_incarnation = 7,
								 .root_id = same ? 91 : 92,
								 .root_generation = 7 };
	mx_origin_observed = f;
	mx_origin_action = mx_origin_action_release = 0;
	test_operation_release_observer = mx_origin_release;
}

static bool
mx_origin_begin(MxOriginFixture *f)
{
	bool ok = cluster_runtime_visibility_mx_operation_origin_read_begin(
		&f->read, &f->key, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &f->origin.admission,
		&f->origin.reason);
	UT_ASSERT(ok);
	return ok;
}

static void
mx_origin_empty(MxOriginFixture *f)
{
	ClusterSavepointEffectProof zero = { 0 };
	ClusterRuntimeVisibilityOperationRead empty = { 0 };
	UT_ASSERT_EQ(memcmp(&f->effect, &zero, sizeof(zero)), 0);
	UT_ASSERT_EQ(memcmp(&f->read, &empty, sizeof(empty)), 0);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(test_operation_pin_depth, 0);
}

static ClusterRuntimeVisibilityOperationStep
mx_origin_step(MxOriginFixture *f)
{
	int calls = test_candidate_acquire_calls + test_candidate_poll_calls
				+ test_candidate_release_calls + test_operation_release_poll_count;
	ClusterRuntimeVisibilityOperationStep step;
	ClusterSavepointEffectProof zero = { 0 };
	memset(&f->effect, 0xa5, sizeof(f->effect));
	step = cluster_runtime_visibility_mx_operation_origin_read_poll(&f->read, &f->effect,
																	&f->origin.reason);
	UT_ASSERT(test_candidate_acquire_calls + test_candidate_poll_calls
				  + test_candidate_release_calls + test_operation_release_poll_count - calls
			  <= 1);
	UT_ASSERT_EQ(test_candidate_wait_calls, 0);
	UT_ASSERT_EQ(test_operation_release_wait_count, 0);
	UT_ASSERT_EQ(test_candidate_data_pin_calls, 0);
	if (step != CLUSTER_RUNTIME_VISIBILITY_OPERATION_COMPLETE)
		UT_ASSERT_EQ(memcmp(&f->effect, &zero, sizeof(zero)), 0);
	if (step == CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED)
		UT_ASSERT(f->origin.reason != CLUSTER_TX_RESOLVE_NONE);
	return step;
}

static ClusterRuntimeVisibilityOperationStep
mx_origin_finish(MxOriginFixture *f)
{
	ClusterRuntimeVisibilityOperationStep step = CLUSTER_RUNTIME_VISIBILITY_OPERATION_PENDING;
	for (int i = 0; i < 64 && step == CLUSTER_RUNTIME_VISIBILITY_OPERATION_PENDING; i++)
		step = mx_origin_step(f);
	UT_ASSERT(step != CLUSTER_RUNTIME_VISIBILITY_OPERATION_PENDING);
	return step;
}

static bool
mx_origin_blocking(MxOriginFixture *f)
{
	return cluster_runtime_visibility_resolve_mx_operation_origin_admitted(
		&f->key, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &f->origin.admission, &f->effect,
		&f->origin.reason);
}

UT_TEST(mx_origin_same_and_cross_segment_three_current_phases)
{
	for (int same = 0; same < 2; same++) {
		MxOriginFixture f;
		mx_origin_init(&f, same);
		if (!mx_origin_begin(&f))
			return;
		UT_ASSERT_EQ(test_candidate_acquire_calls, 0);
		UT_ASSERT_EQ(mx_origin_finish(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_COMPLETE);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
		UT_ASSERT_EQ(test_candidate_release_calls, 3);
		UT_ASSERT_EQ(test_candidate_acquire_segments[0], 1);
		UT_ASSERT_EQ(test_candidate_acquire_segments[1], same ? 1 : 2);
		UT_ASSERT_EQ(test_candidate_acquire_segments[2], 1);
		UT_ASSERT_EQ(test_candidate_data_copy_calls, 2);
		UT_ASSERT_EQ(test_candidate_max_held_count, 1);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(test_original_calls, 1);
		UT_ASSERT_EQ(f.effect.top_outcome, CLUSTER_TX_COMMITTED);
		UT_ASSERT_EQ(f.effect.top_commit_scn, test_commit_scn);
		UT_ASSERT_EQ(f.effect.effect_state, CLUSTER_SAVEPOINT_EFFECT_LIVE);
		UT_ASSERT_EQ(memcmp(&f.effect.key, &f.key, sizeof(f.key)), 0);
		UT_ASSERT(FullTransactionIdEquals(f.effect.identity.top_fxid, f.key.top_fxid));
		UT_ASSERT_EQ(f.effect.identity.canonical_generation, same ? 17 : 23);
		UT_ASSERT(cluster_savepoint_reader_mx_effect_valid(&f.effect));
	}
}

UT_TEST(mx_origin_stages_until_final_release)
{
	MxOriginFixture f;
	mx_origin_init(&f, false);
	if (!mx_origin_begin(&f))
		return;
	for (int i = 0; i < 9; i++)
		UT_ASSERT_EQ(mx_origin_step(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_PENDING);
	UT_ASSERT_EQ(test_candidate_release_calls, 3);
	UT_ASSERT_EQ(mx_origin_step(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_COMPLETE);
	UT_ASSERT_EQ(mx_origin_step(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
	mx_origin_empty(&f);
}

UT_TEST(mx_origin_copies_begin_key_and_admission)
{
	MxOriginFixture f;
	ClusterSavepointOperationKey saved;
	mx_origin_init(&f, false);
	saved = f.key;
	if (!mx_origin_begin(&f))
		return;
	memset(&f.key, 0xa5, sizeof(f.key));
	memset(&f.origin.admission, 0, sizeof(f.origin.admission));
	UT_ASSERT_EQ(mx_origin_finish(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_COMPLETE);
	UT_ASSERT_EQ(memcmp(&f.effect.key, &saved, sizeof(saved)), 0);
}

UT_TEST(mx_origin_consumes_post_top_discard)
{
	MxOriginFixture f;
	mx_origin_init(&f, false);
	mx_origin_action = 1;
	mx_origin_action_release = 2;
	if (!mx_origin_begin(&f))
		return;
	UT_ASSERT_EQ(mx_origin_finish(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_COMPLETE);
	UT_ASSERT_EQ(f.effect.top_outcome, CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(f.effect.effect_state, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
}

UT_TEST(mx_origin_discard_cannot_revert_to_live)
{
	MxOriginFixture f;
	mx_origin_init(&f, false);
	operation_plan_effect(CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
	mx_origin_action = 2;
	mx_origin_action_release = 2;
	if (!mx_origin_begin(&f))
		return;
	UT_ASSERT_EQ(mx_origin_finish(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
	mx_origin_empty(&f);
}

UT_TEST(mx_origin_changed_immutable_record_refuses)
{
	for (int action = 6; action <= 8; action++) {
		MxOriginFixture f;
		mx_origin_init(&f, false);
		mx_origin_action = action;
		mx_origin_action_release = 2;
		if (!mx_origin_begin(&f))
			return;
		UT_ASSERT_EQ(mx_origin_finish(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
		mx_origin_empty(&f);
	}
}

UT_TEST(mx_origin_original_full_xid_is_not_the_tail)
{
	for (int fault = 0; fault < 5; fault++) {
		MxOriginFixture f;
		mx_origin_init(&f, false);
		if (fault == 0)
			test_original_available = false;
		if (fault == 1)
			test_original_full_xid = FullTransactionIdFromEpochAndXid(10, TEST_ORIGIN_XID);
		if (fault == 2)
			test_original_key.slot_wrap++;
		if (fault == 3)
			test_original_key.segment_generation++;
		if (fault == 4)
			test_original_key.origin_boot_incarnation++;
		if (!mx_origin_begin(&f))
			return;
		UT_ASSERT_EQ(mx_origin_finish(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 2);
		UT_ASSERT_EQ(test_candidate_release_calls, 2);
		mx_origin_empty(&f);
	}
}

UT_TEST(mx_origin_unavailable_active_abort_and_visibility_identity_refuse)
{
	for (int kind = 0; kind < 3; kind++) {
		MxOriginFixture f;
		mx_origin_init(&f, false);
		if (kind == 0) {
			test_tt_slot.status = TT_SLOT_ACTIVE;
			test_tt_slot.commit_scn = InvalidScn;
			test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
			test_twophase_xid = TEST_ORIGIN_XID;
		}
		if (kind == 1) {
			test_tt_slot.status = TT_SLOT_ABORTED;
			test_tt_slot.commit_scn = InvalidScn;
			test_native_status = TRANSACTION_STATUS_ABORTED;
		}
		if (kind == 2) {
			bool ok = cluster_runtime_visibility_mx_operation_origin_read_begin(
				&f.read, &f.key, CLUSTER_TX_RESOLVE_VISIBILITY, &f.origin.admission,
				&f.origin.reason);
			UT_ASSERT(ok);
			if (!ok)
				return;
		} else if (!mx_origin_begin(&f))
			return;
		UT_ASSERT_EQ(mx_origin_finish(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
		UT_ASSERT_EQ(test_original_calls, 0);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 2);
		mx_origin_empty(&f);
	}
}

UT_TEST(mx_origin_pending_acquire_never_waits)
{
	MxOriginFixture f;
	mx_origin_init(&f, false);
	test_candidate_acquire_step = CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
	test_candidate_poll_step = CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
	if (!mx_origin_begin(&f))
		return;
	for (int i = 0; i < 5; i++)
		UT_ASSERT_EQ(mx_origin_step(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_PENDING);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 1);
	test_candidate_poll_step = CLUSTER_UNDO_BLOCK0_CURRENT_HELD;
	UT_ASSERT_EQ(mx_origin_finish(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_COMPLETE);
}

UT_TEST(mx_origin_pending_release_never_publishes)
{
	for (int release = 1; release <= 3; release++) {
		MxOriginFixture f;
		mx_origin_init(&f, false);
		test_operation_release_pending_call = release;
		if (!mx_origin_begin(&f))
			return;
		UT_ASSERT_EQ(mx_origin_finish(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_COMPLETE);
		UT_ASSERT_EQ(test_operation_release_poll_count, 2);
		UT_ASSERT_EQ(test_candidate_release_calls, 3);
	}
}

UT_TEST(mx_origin_cancel_and_rebegin_preserve_guard_ownership)
{
	for (int stop = 0; stop < 9; stop++) {
		MxOriginFixture f;
		int cancels;
		mx_origin_init(&f, false);
		if (!mx_origin_begin(&f))
			return;
		for (int i = 0; i < stop; i++)
			UT_ASSERT_EQ(mx_origin_step(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_PENDING);
		UT_ASSERT(!cluster_runtime_visibility_mx_operation_origin_read_begin(
			&f.read, &f.key, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &f.origin.admission,
			&f.origin.reason));
		cancels = test_candidate_cancel_calls + (test_candidate_held_count > 0 ? 1 : 0);
		cluster_runtime_visibility_operation_origin_read_cancel(&f.read);
		cluster_runtime_visibility_operation_origin_read_cancel(&f.read);
		UT_ASSERT_EQ(test_candidate_cancel_calls, cancels);
		UT_ASSERT_EQ(mx_origin_step(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
		mx_origin_empty(&f);
	}
}

UT_TEST(mx_origin_current_failures_clear_proof_and_detach)
{
	for (int kind = 0; kind < 3; kind++)
		for (int phase = 1; phase <= 3; phase++) {
			MxOriginFixture f;
			mx_origin_init(&f, false);
			if (kind == 0)
				test_operation_acquire_failure_call = phase;
			if (kind == 1) {
				test_candidate_release_failed = true;
				test_candidate_release_fault_call = phase;
			}
			if (kind == 2) {
				test_operation_release_pending_call = phase;
				test_operation_release_poll_failed = true;
			}
			if (!mx_origin_begin(&f))
				return;
			UT_ASSERT_EQ(mx_origin_finish(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
			mx_origin_empty(&f);
		}
}

UT_TEST(mx_origin_generation_root_and_admission_fences)
{
	for (int action = 3; action <= 5; action++)
		for (int release = 1; release <= 3; release++) {
			MxOriginFixture f;
			mx_origin_init(&f, false);
			mx_origin_action = action;
			mx_origin_action_release = release;
			if (!mx_origin_begin(&f))
				return;
			if (release == 3 && action != 4) {
				/* DATA/root changes after the final protected observation do
				 * not rewrite that qualified private snapshot. Admission must
				 * still remain current, and no fourth fetch is introduced. */
				UT_ASSERT_EQ(mx_origin_finish(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_COMPLETE);
				UT_ASSERT_EQ(f.effect.effect_state, CLUSTER_SAVEPOINT_EFFECT_LIVE);
				UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
				UT_ASSERT_EQ(test_candidate_held_count, 0);
			} else {
				UT_ASSERT_EQ(mx_origin_finish(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
				mx_origin_empty(&f);
			}
		}
}

UT_TEST(mx_origin_error_cleanup_detaches_each_phase)
{
	for (int phase = 1; phase <= 3; phase++) {
		MxOriginFixture f;
		volatile bool caught = false;
		mx_origin_init(&f, false);
		mx_origin_action = 9;
		mx_origin_action_release = phase;
		if (!mx_origin_begin(&f))
			return;
		PG_TRY();
		{
			(void)mx_origin_finish(&f);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(test_candidate_cancel_calls, 1);
		mx_origin_empty(&f);
	}
}

UT_TEST(mx_origin_blocking_cold_data_and_misses)
{
	for (int mask = 1; mask <= 3; mask++) {
		MxOriginFixture f;
		mx_origin_init(&f, false);
		test_candidate_cold_copy_mask = mask;
		UT_ASSERT(mx_origin_blocking(&f));
		UT_ASSERT_EQ(test_candidate_data_pin_calls, mask == 3 ? 2 : 1);
		UT_ASSERT_EQ(test_candidate_data_pin_calls, test_candidate_data_unpin_calls);
		UT_ASSERT_EQ(test_candidate_max_held_count, 1);
		UT_ASSERT_EQ(test_operation_pin_depth, 0);
	}
	for (int mask = 1; mask <= 2; mask++) {
		MxOriginFixture f;
		mx_origin_init(&f, false);
		test_candidate_cold_copy_mask = mask;
		test_candidate_data_pool_disabled = true;
		UT_ASSERT(!mx_origin_blocking(&f));
		UT_ASSERT_EQ(test_candidate_data_pin_calls, 1);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(test_operation_pin_depth, 0);
	}
}

UT_TEST(mx_origin_blocking_waits_and_error_cleanup)
{
	for (int release = 0; release <= 3; release++) {
		MxOriginFixture f;
		mx_origin_init(&f, false);
		if (release == 0) {
			test_candidate_acquire_step = CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
			test_candidate_poll_step = CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
			test_candidate_after_wait_step = CLUSTER_UNDO_BLOCK0_CURRENT_HELD;
			test_candidate_wakes_before_terminal = 2;
		} else
			test_operation_release_pending_call = release;
		UT_ASSERT(mx_origin_blocking(&f));
		UT_ASSERT_EQ(test_candidate_wait_calls, release == 0 ? 2 : 0);
		UT_ASSERT_EQ(test_operation_release_wait_count, release == 0 ? 0 : 1);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
	for (int fault = 0; fault < 3; fault++) {
		MxOriginFixture f;
		volatile bool caught = false;
		mx_origin_init(&f, false);
		if (fault < 2) {
			test_candidate_cold_copy_mask = fault == 0 ? 1 : 2;
			test_candidate_data_io_throw = true;
		} else {
			mx_origin_action = 9;
			mx_origin_action_release = 3;
		}
		PG_TRY();
		{
			(void)mx_origin_blocking(&f);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(test_candidate_cancel_calls, 1);
		/* The storage leaf throws before allocating a pin. */
		UT_ASSERT_EQ(test_candidate_data_pin_calls, fault < 2 ? 1 : 0);
		UT_ASSERT_EQ(test_candidate_data_unpin_calls, 0);
		mx_origin_empty(&f);
	}
}

UT_TEST(mx_origin_bad_inputs_refuse_without_losing_active_guard)
{
	MxOriginFixture f;
	mx_origin_init(&f, false);
	if (!mx_origin_begin(&f))
		return;
	UT_ASSERT(!cluster_runtime_visibility_mx_operation_origin_read_begin(
		NULL, &f.key, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &f.origin.admission, &f.origin.reason));
	UT_ASSERT(!cluster_runtime_visibility_mx_operation_origin_read_begin(
		&f.read, NULL, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &f.origin.admission, &f.origin.reason));
	UT_ASSERT_EQ(mx_origin_step(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_PENDING);
	UT_ASSERT_EQ(
		cluster_runtime_visibility_mx_operation_origin_read_poll(&f.read, NULL, &f.origin.reason),
		CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	cluster_node_id = 1;
	UT_ASSERT(!cluster_runtime_visibility_mx_operation_origin_read_begin(
		&f.read, &f.key, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &f.origin.admission,
		&f.origin.reason));
	mx_origin_empty(&f);
}

UT_TEST(mx_origin_provisional_key_is_only_an_exact_challenge)
{
	MxOriginFixture f;
	ClusterSavepointReaderIdentity identity, zero = { 0 };
	char unaligned[BLCKSZ + 1];
	mx_origin_init(&f, false);
	if (!mx_origin_begin(&f))
		return;
	cluster_runtime_visibility_operation_origin_read_cancel(&f.read);
	memcpy(unaligned + 1, test_operation_page.data, BLCKSZ);
	UT_ASSERT(cluster_savepoint_reader_discover_mx_operation(&f.key, unaligned + 1, &identity));
	UT_ASSERT_EQ(identity.canonical.xid, TEST_ORIGIN_XID);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 0);
	UT_ASSERT_EQ(test_original_calls, 0);
	for (int fault = 0; fault < 7; fault++) {
		ClusterSavepointOperationKey bad = f.key;
		if (fault == 0)
			bad.target.blockno++;
		if (fault == 1)
			bad.target.offnum++;
		if (fault == 2)
			bad.target._pad = 1;
		if (fault == 3)
			bad.operation_fxid = FullTransactionIdFromEpochAndXid(10, TEST_ORIGIN_XID + 1);
		if (fault == 4)
			bad.top_fxid = FullTransactionIdFromEpochAndXid(10, TEST_ORIGIN_XID);
		if (fault == 5)
			bad.locator.tt_wrap++;
		if (fault == 6)
			bad.locator.xid++;
		memset(&identity, 0xa5, sizeof(identity));
		UT_ASSERT(!cluster_savepoint_reader_discover_mx_operation(&bad, unaligned + 1, &identity));
		UT_ASSERT_EQ(memcmp(&identity, &zero, sizeof(zero)), 0);
	}
}

int
main(void)
{
	UT_PLAN(18);
	UT_RUN(mx_origin_same_and_cross_segment_three_current_phases);
	UT_RUN(mx_origin_stages_until_final_release);
	UT_RUN(mx_origin_copies_begin_key_and_admission);
	UT_RUN(mx_origin_consumes_post_top_discard);
	UT_RUN(mx_origin_discard_cannot_revert_to_live);
	UT_RUN(mx_origin_changed_immutable_record_refuses);
	UT_RUN(mx_origin_original_full_xid_is_not_the_tail);
	UT_RUN(mx_origin_unavailable_active_abort_and_visibility_identity_refuse);
	UT_RUN(mx_origin_pending_acquire_never_waits);
	UT_RUN(mx_origin_pending_release_never_publishes);
	UT_RUN(mx_origin_cancel_and_rebegin_preserve_guard_ownership);
	UT_RUN(mx_origin_current_failures_clear_proof_and_detach);
	UT_RUN(mx_origin_generation_root_and_admission_fences);
	UT_RUN(mx_origin_error_cleanup_detaches_each_phase);
	UT_RUN(mx_origin_blocking_cold_data_and_misses);
	UT_RUN(mx_origin_blocking_waits_and_error_cleanup);
	UT_RUN(mx_origin_bad_inputs_refuse_without_losing_active_guard);
	UT_RUN(mx_origin_provisional_key_is_only_an_exact_challenge);
	UT_DONE();
	return ut_failed_count != 0;
}
