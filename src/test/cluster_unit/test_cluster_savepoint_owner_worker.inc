/*-------------------------------------------------------------------------
 *
 * test_cluster_savepoint_owner_worker.inc
 *    Exercise admitted owner FULL construction and exact cleanup.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_savepoint_owner_worker.inc
 *
 * NOTES
 *    This is a pgrac-original test fixture. Origin I/O, native membership
 *    and backend publication remain explicit fixture boundaries.
 *
 *-------------------------------------------------------------------------
 */
static void
owner_worker_init(OperationWorkerFixture *f)
{
	const ClusterSavepointOperation *expected;
	ClusterR4CrOwnerChallenge *owner;

	operation_worker_init(f, false);
	operation_plan_effect(CLUSTER_SAVEPOINT_EFFECT_LIVE);
	ClusterPageGetItlSlots((Page)f->cr.source.data)[2].flags = ITL_FLAG_ACTIVE;
	ClusterPageGetItlSlots((Page)f->cr.source.data)[2].commit_scn = InvalidScn;
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
	test_current_owner_available = true;
	expected = &f->cr.origin.expected;
	owner = &f->extension.route_proof.requester_owner;
	owner->top_fxid = expected->top_fxid;
	owner->active_lsn = expected->top.active_lsn;
	owner->segment_id = expected->top.segment_id;
	owner->segment_generation = expected->top.segment_generation;
	owner->top_xid = expected->top.xid;
	owner->slot_offset = expected->top.slot_offset;
	owner->slot_wrap = expected->top.slot_wrap;
	owner->origin_instance = expected->top.origin_instance;
	owner->publish_state = expected->top.publish_state;
	owner->flags = CLUSTER_R4_CR_OWNER_PRESENT;
	UT_ASSERT(ClusterR4CrOwnerValid(owner));
}

UT_TEST(test_owner_worker_active_live_keeps_own_full)
{
	OperationWorkerFixture f;
	PGAlignedBlock original;
	int allocations = operation_worker_allocations;

	owner_worker_init(&f);
	memcpy(original.data, f.cr.source.data, BLCKSZ);
	UT_ASSERT_EQ(operation_worker_finish(&f), CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT(operation_cr_has_child(f.cr.source.data));
	UT_ASSERT_EQ(memcmp(original.data, f.cr.source.data, BLCKSZ), 0);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
	UT_ASSERT_EQ(test_candidate_release_calls, 3);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(test_operation_pin_depth, 0);
	cluster_cr_build_on_holder_forget(0, 1000);
	UT_ASSERT_EQ(operation_worker_allocations, allocations);
}

UT_TEST(test_owner_worker_discarded_and_other_active_are_inversed)
{
	for (int other = 0; other < 2; other++) {
		OperationWorkerFixture f;

		owner_worker_init(&f);
		if (other)
			memset(&f.extension.route_proof.requester_owner, 0,
				   sizeof(f.extension.route_proof.requester_owner));
		else
			operation_plan_effect(CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
		UT_ASSERT_EQ(operation_worker_finish(&f), CLUSTER_R4_CR_STEP_FULL);
		UT_ASSERT(!operation_cr_has_child(f.cr.source.data));
		cluster_cr_build_on_holder_forget(0, 1000);
	}
}

UT_TEST(test_owner_worker_raw_alias_is_explicit_refusal)
{
	for (int fault = 0; fault < 8; fault++) {
		OperationWorkerFixture f;
		ClusterR4CrOwnerChallenge *owner;
		PGAlignedBlock original;

		owner_worker_init(&f);
		owner = &f.extension.route_proof.requester_owner;
		switch (fault) {
		case 0:
			owner->top_fxid = FullTransactionIdFromEpochAndXid(8, owner->top_xid);
			break;
		case 1:
			owner->segment_generation++;
			break;
		case 2:
			owner->segment_id++;
			break;
		case 3:
			owner->slot_offset++;
			break;
		case 4:
			owner->slot_wrap++;
			break;
		case 5:
			owner->origin_instance++;
			break;
		case 6:
			test_tt_slot.xid++;
			break;
		case 7:
			operation_plan_effect(0 /* unknown is not a valid operation state */);
			break;
		}
		memcpy(original.data, f.cr.source.data, BLCKSZ);
		UT_ASSERT_EQ(operation_worker_finish(&f), CLUSTER_R4_CR_STEP_FAIL);
		UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
		UT_ASSERT_EQ(memcmp(original.data, f.cr.source.data, BLCKSZ), 0);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(test_operation_pin_depth, 0);
		cluster_cr_build_on_holder_forget(0, 1000);
	}
}

UT_TEST(test_owner_worker_challenge_drift_cannot_finish_old_builder)
{
	OperationWorkerFixture f;

	owner_worker_init(&f);
	UT_ASSERT_EQ(operation_worker_step(&f), CLUSTER_R4_CR_STEP_PENDING);
	f.extension.route_proof.requester_owner.segment_generation++;
	UT_ASSERT_EQ(operation_worker_step(&f), CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_PROTOCOL);
	cluster_cr_build_on_holder_forget(0, 1000);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(test_operation_pin_depth, 0);
}

/* A pipe lets independently owned origin and native-consumer fixtures use
 * the same produced bytes without merging their unrelated boundary doubles. */
static int
owner_worker_export_full(FILE *stream)
{
	TestOwnerFullFixture fixture = { 0 };
	OperationWorkerFixture f;
	HeapTupleHeader tuple;
	ClusterR4CrBuildStepResult step;

	owner_worker_init(&f);
	tuple = (HeapTupleHeader)PageGetItem((Page)f.cr.source.data,
										 PageGetItemId((Page)f.cr.source.data, 3));
	tuple->t_itl_slot_idx = 2;
	tuple->t_choice.t_heap.t_field3.t_cid = f.cr.origin.expected.command_id;
	step = operation_worker_finish(&f);
	fixture.magic = TEST_OWNER_FULL_MAGIC;
	fixture.expected = f.cr.origin.expected;
	fixture.tag = f.cr.tag;
	fixture.read_scn = f.extension.route_proof.read_scn;
	memcpy(fixture.full.data, f.cr.source.data, BLCKSZ);
	memcpy(fixture.undo.data, test_operation_page.data, BLCKSZ);
	cluster_cr_build_on_holder_forget(0, 1000);
	if (step != CLUSTER_R4_CR_STEP_FULL || f.reason != CLUSTER_CR_BUILD_NONE
		|| !operation_cr_has_child(fixture.full.data) || ut_failed_count != 0)
		return 1;
	return fwrite(&fixture, sizeof(fixture), 1, stream) == 1 ? 0 : 1;
}
