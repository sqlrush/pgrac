/* Author: SqlRush <sqlrush@gmail.com> */
/*
 * Real local abort consumer plus the existing durable-TT fixture.  Only the
 * external admission, storage and status/hint sinks are doubles.  Including
 * the source permits invalid backend-local states without a production API.
 * Copyright (c) 2026, pgrac contributors
 */
int durable_fixture_main(int argc, char **argv);
#define main durable_fixture_main
#include "test_cluster_tt_durable.c"
#undef main
#include "../../backend/cluster/cluster_tt_local.c"

static int abort_marks;
static int status_installs;
static int hint_emits;
bool cluster_enabled = true;

ClusterJoinGateVerdict
cluster_reconfig_self_join_gate_verdict(void)
{
	return CLUSTER_JOIN_GATE_ALLOW;
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_modifier_enter(bool writable, ClusterSemanticAdmissionToken *token)
{
	UT_ASSERT(writable);
	*token = target_modifier_token();
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	token->entered = false;
}

void
cluster_tt_slot_mark_aborted(uint32 segment_id, uint16 slot_offset, TransactionId xid,
							 uint16 expected_wrap)
{
	UT_ASSERT(segment_id == 1 && slot_offset < TT_SLOTS_PER_SEGMENT && xid >= 100);
	UT_ASSERT_EQ(expected_wrap, 5);
	abort_marks++;
}

void
cluster_tt_slot_mark_committed(uint32 segment_id pg_attribute_unused(),
							   uint16 slot_offset pg_attribute_unused(),
							   TransactionId xid pg_attribute_unused(),
							   uint16 expected_wrap pg_attribute_unused(),
							   SCN scn pg_attribute_unused())
{
	UT_ASSERT(false);
}

ClusterSemanticAdmissionResult
cluster_tt_status_source_dispatch(ClusterTTStatusSourceOp operation,
								  const ClusterTTStatusSourceRequest *request pg_attribute_unused(),
								  ClusterTTStatusSourceResult *result)
{
	memset(result, 0, sizeof(*result));
	if (operation == CLUSTER_TT_SOURCE_INSTALL_LOCAL)
		status_installs++;
	result->bool_value = true;
	result->lookup.authoritative = true;
	result->lookup.status = CLUSTER_TT_STATUS_ABORTED;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

ClusterSemanticAdmissionResult
cluster_tt_status_hint_source_dispatch(ClusterTTStatusHintSourceOp operation,
									   const ClusterTTStatusHintSourceRequest *request)
{
	UT_ASSERT(operation == CLUSTER_TT_HINT_SOURCE_EMIT);
	UT_ASSERT(request->status == CLUSTER_TT_STATUS_ABORTED);
	hint_emits++;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

static void
seed_binding(ClusterTTLocalBinding *binding, uint8 publication, uint8 terminal)
{
	memset(binding, 0, sizeof(*binding));
	binding->top_xid = 100;
	binding->segment_id = 1;
	binding->segment_generation = 4;
	binding->slot_offset = 7;
	binding->wrap = 5;
	binding->cluster_epoch = 17;
	binding->publish_state = publication;
	binding->terminal_state = terminal;
	cluster_tt_local_bindings = binding;
	cluster_tt_local_binding_count = 1;
	abort_marks = status_installs = hint_emits = 0;
}

UT_TEST(test_unproved_publication_cannot_skip_durable_abort)
{
	const uint8 states[] = { CLUSTER_CANONICAL_TXN_PUBLISHING, CLUSTER_CANONICAL_TXN_FAILED,
							 CLUSTER_CANONICAL_TXN_PUBLISHED, 255 };
	volatile unsigned i;

	for (i = 0; i < lengthof(states); i++) {
		ClusterTTLocalBinding binding;
		volatile bool rejected = false;

		seed_binding(&binding, states[i],
					 states[i] == CLUSTER_CANONICAL_TXN_PUBLISHED
						 ? CLUSTER_TT_LOCAL_TERMINAL_COMMIT_STAGED
						 : CLUSTER_TT_LOCAL_TERMINAL_NONE);
		PG_TRY();
		{
			(void)cluster_tt_local_preabort_durable_finish(100);
		}
		PG_CATCH();
		{
			rejected = true;
		}
		PG_END_TRY();
		UT_ASSERT(rejected);
		UT_ASSERT_EQ(binding.publish_state, states[i]);
	}
	cluster_tt_local_reset_binding();
}

UT_TEST(test_batch_rejected_before_any_hint_or_allocator_mutation)
{
	ClusterTTLocalBinding bindings[2];
	volatile bool rejected = false;

	seed_binding(&bindings[0], CLUSTER_CANONICAL_TXN_PUBLISHED,
				 CLUSTER_TT_LOCAL_TERMINAL_ABORT_DURABLE);
	bindings[1] = bindings[0];
	bindings[1].top_xid = 101;
	bindings[1].slot_offset = 8;
	bindings[1].publish_state = CLUSTER_CANONICAL_TXN_FAILED;
	bindings[1].terminal_state = CLUSTER_TT_LOCAL_TERMINAL_NONE;
	cluster_tt_local_binding_count = 2;
	PG_TRY();
	{
		cluster_tt_local_record_abort(100);
	}
	PG_CATCH();
	{
		rejected = true;
	}
	PG_END_TRY();
	UT_ASSERT(rejected);
	UT_ASSERT_EQ(abort_marks, 0);
	UT_ASSERT_EQ(status_installs, 0);
	UT_ASSERT_EQ(hint_emits, 0);
	UT_ASSERT(cluster_tt_local_bindings == bindings);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 2);
	cluster_tt_local_reset_binding();
}

UT_TEST(test_reservation_and_proved_abort_keep_existing_cleanup)
{
	ClusterTTLocalBinding binding;

	cluster_tt_local_reset_binding();
	UT_ASSERT(!cluster_tt_local_preabort_durable_finish(100));
	seed_binding(&binding, CLUSTER_CANONICAL_TXN_RESERVED, CLUSTER_TT_LOCAL_TERMINAL_NONE);
	UT_ASSERT(!cluster_tt_local_preabort_durable_finish(100));
	cluster_tt_local_record_abort(100);
	UT_ASSERT_EQ(abort_marks, 1);
	UT_ASSERT_EQ(hint_emits, 0);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
	seed_binding(&binding, CLUSTER_CANONICAL_TXN_PUBLISHED,
				 CLUSTER_TT_LOCAL_TERMINAL_ABORT_DURABLE);
	UT_ASSERT(cluster_tt_local_preabort_durable_finish(100));
	cluster_tt_local_record_abort(100);
	UT_ASSERT_EQ(abort_marks, 1);
	UT_ASSERT_EQ(status_installs, 1);
	UT_ASSERT_EQ(hint_emits, 1);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
}

UT_TEST(test_post_wal_install_failure_preserves_unfinished_binding)
{
	ClusterTTLocalBinding binding;
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;
	TTSlot before;
	volatile int rejected = 0;

	reset_current_write_mock();
	seed_current_exact_active(7, 100, 5);
	before = resident->tt_slots[7];
	seed_binding(&binding, CLUSTER_CANONICAL_TXN_PUBLISHED, CLUSTER_TT_LOCAL_TERMINAL_NONE);
	/* Real abort emits/flushed its carrier; dependency fails the data install. */
	g_write_hdr_ok = false;
	PG_TRY();
	{
		(void)cluster_tt_local_preabort_durable_finish(100);
	}
	PG_CATCH();
	{
		rejected++;
	}
	PG_END_TRY();
	UT_ASSERT_EQ(rejected, 1);
	UT_ASSERT_EQ(g_abort_exact_emit_calls, 1);
	UT_ASSERT(g_abort_flush_seen);
	UT_ASSERT_EQ(memcmp(&before, &resident->tt_slots[7], sizeof(before)), 0);
	UT_ASSERT_EQ(binding.terminal_state, CLUSTER_TT_LOCAL_TERMINAL_NONE);
	PG_TRY();
	{
		cluster_tt_local_record_abort(100);
	}
	PG_CATCH();
	{
		rejected++;
	}
	PG_END_TRY();
	UT_ASSERT_EQ(rejected, 2);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 1);
	UT_ASSERT_EQ(abort_marks, 0);
	UT_ASSERT_EQ(hint_emits, 0);
	cluster_tt_local_reset_binding();
	g_write_hdr_ok = true;
}

/*
 * F-D-29: an ERROR between the commit stamp's stage and the commit record
 * (e.g. the commit brake) leaves nothing on storage; the abort releases the
 * stage and makes the ordinary abort durable.
 */
UT_TEST(test_abort_after_unwritten_commit_stage_aborts_durably)
{
	ClusterTTLocalBinding binding;
	ClusterSemanticAdmissionToken admission = target_modifier_token();
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;
	TTSlot staged;

	reset_current_write_mock();
	seed_current_exact_active(7, 100, 5);
	seed_binding(&binding, CLUSTER_CANONICAL_TXN_PUBLISHED,
				 CLUSTER_TT_LOCAL_TERMINAL_COMMIT_STAGED);
	(void)cluster_tt_slot_durable_commit_stage(1, 4, 7, 100, 5, scn_encode(1, 42), &admission,
											   &staged);
	UT_ASSERT_EQ(g_write_hdr_calls, 0);
	g_require_abort_flush_before_write = true;

	UT_ASSERT(cluster_tt_local_preabort_durable_finish(100));

	UT_ASSERT(!cluster_tt_slot_durable_commit_staged(1, 7, 100, 5));
	UT_ASSERT_EQ(g_reservation_release_calls, 1);
	UT_ASSERT_EQ(g_abort_exact_emit_calls, 1);
	UT_ASSERT(g_abort_flush_seen);
	UT_ASSERT_EQ(g_write_hdr_calls, 1);
	UT_ASSERT_EQ(g_last_written_slot.status, TT_SLOT_ABORTED);
	UT_ASSERT_EQ(resident->tt_slots[7].status, TT_SLOT_ABORTED);
	UT_ASSERT_EQ(binding.terminal_state, CLUSTER_TT_LOCAL_TERMINAL_ABORT_DURABLE);
	cluster_tt_local_record_abort(100);
	UT_ASSERT_EQ(abort_marks, 1);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
}

/* A staged stamp of another slot is no proof for this binding. */
UT_TEST(test_commit_staged_binding_without_its_own_stage_is_refused)
{
	ClusterTTLocalBinding binding;
	ClusterSemanticAdmissionToken admission = target_modifier_token();
	TTSlot staged;
	volatile bool rejected = false;

	reset_current_write_mock();
	seed_current_exact_active(8, 100, 5);
	seed_binding(&binding, CLUSTER_CANONICAL_TXN_PUBLISHED,
				 CLUSTER_TT_LOCAL_TERMINAL_COMMIT_STAGED);
	(void)cluster_tt_slot_durable_commit_stage(1, 4, 8, 100, 5, scn_encode(1, 42), &admission,
											   &staged);
	PG_TRY();
	{
		(void)cluster_tt_local_preabort_durable_finish(100);
	}
	PG_CATCH();
	{
		rejected = true;
	}
	PG_END_TRY();
	UT_ASSERT(rejected);
	UT_ASSERT(cluster_tt_slot_durable_commit_staged(1, 8, 100, 5));
	UT_ASSERT_EQ(binding.terminal_state, CLUSTER_TT_LOCAL_TERMINAL_COMMIT_STAGED);
	UT_ASSERT_EQ(g_abort_exact_emit_calls, 0);
	cluster_tt_local_reset_binding();
}

UT_TEST(test_local_abort_accepts_retained_successor_without_losing_head)
{
	ClusterTTLocalBinding binding;
	TTSlot before = retained_abort_seed(2);
	volatile bool success = false;

	seed_binding(&binding, CLUSTER_CANONICAL_TXN_PUBLISHED, CLUSTER_TT_LOCAL_TERMINAL_NONE);
	PG_TRY();
	{
		success = cluster_tt_local_preabort_durable_finish(100);
	}
	PG_CATCH();
	{}
	PG_END_TRY();
	UT_ASSERT(success);
	UT_ASSERT_EQ(binding.terminal_state, CLUSTER_TT_LOCAL_TERMINAL_ABORT_DURABLE);
	UT_ASSERT_EQ(g_abort_exact_emit_calls, 1);
	UT_ASSERT_EQ(
		memcmp(&g_last_written_slot.first_undo_block, &before.first_undo_block, sizeof(UBA)), 0);
	if (success) {
		UT_ASSERT(cluster_tt_local_preabort_durable_finish(100));
		UT_ASSERT_EQ(g_abort_exact_emit_calls, 1);
		cluster_tt_local_record_abort(100);
		UT_ASSERT_EQ(abort_marks, 1);
		UT_ASSERT_EQ(hint_emits, 1);
		UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
		UT_ASSERT_EQ(
			memcmp(&((UndoSegmentHeaderData *)g_canned_block)->tt_slots[7].first_undo_block,
				   &before.first_undo_block, sizeof(UBA)),
			0);
	}
	cluster_tt_local_reset_binding();
}

UT_TEST(test_local_retained_abort_write_failure_cannot_publish_cleanup)
{
	ClusterTTLocalBinding binding;
	TTSlot before = retained_abort_seed(2);
	volatile bool rejected = false;

	seed_binding(&binding, CLUSTER_CANONICAL_TXN_PUBLISHED, CLUSTER_TT_LOCAL_TERMINAL_NONE);
	g_write_hdr_ok = false;
	PG_TRY();
	{
		(void)cluster_tt_local_preabort_durable_finish(100);
	}
	PG_CATCH();
	{
		rejected = true;
	}
	PG_END_TRY();
	UT_ASSERT(rejected);
	UT_ASSERT_EQ(g_abort_exact_emit_calls, 1);
	UT_ASSERT(g_abort_flush_seen);
	UT_ASSERT_EQ(binding.terminal_state, CLUSTER_TT_LOCAL_TERMINAL_NONE);
	UT_ASSERT_EQ(abort_marks + hint_emits + status_installs, 0);
	UT_ASSERT_EQ(
		memcmp(&((UndoSegmentHeaderData *)g_canned_block)->tt_slots[7], &before, sizeof(before)),
		0);
	cluster_tt_local_reset_binding();
	g_write_hdr_ok = true;
}

static bool
retained_local_precommit(xl_xact_tt_commit *fold)
{
	volatile bool success = false;

	PG_TRY();
	{
		success = cluster_tt_local_precommit_durable_finish(100, scn_encode(1, 42), fold);
	}
	PG_CATCH();
	{}
	PG_END_TRY();
	return success;
}

UT_TEST(test_local_commit_retains_head_and_original_32_byte_fold)
{
	ClusterTTLocalBinding binding;
	xl_xact_tt_commit fold = { 0 };
	xl_xact_tt_commit expected = { 0 };
	TTSlot successor = retained_commit_seed();

	seed_binding(&binding, CLUSTER_CANONICAL_TXN_PUBLISHED, CLUSTER_TT_LOCAL_TERMINAL_NONE);
	binding.active_lsn = (XLogRecPtr)0x1000;
	expected.segment_id = 1;
	expected.segment_generation = 4;
	expected.xid = 100;
	expected.slot_offset = 7;
	expected.wrap = 5;
	expected.instance = 1;
	expected.format_version = CLUSTER_XACT_TT_COMMIT_VERSION;
	expected.commit_scn = scn_encode(1, 42);
	if (!retained_local_precommit(&fold)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT_EQ(memcmp(&fold, &expected, sizeof(fold)), 0);
	UT_ASSERT_EQ(sizeof(fold), 32);
	UT_ASSERT_EQ(binding.terminal_state, CLUSTER_TT_LOCAL_TERMINAL_COMMIT_STAGED);
	UT_ASSERT_EQ(g_write_hdr_calls + status_installs + hint_emits + abort_marks, 0);
	g_flush_ptr = (XLogRecPtr)0x5000;
	MyProc->delayChkptFlags |= DELAY_CHKPT_START;
	CritSectionCount++;
	cluster_tt_local_commit_durable_apply(g_flush_ptr);
	CritSectionCount--;
	MyProc->delayChkptFlags &= ~DELAY_CHKPT_START;
	successor.status = TT_SLOT_COMMITTED;
	successor.commit_scn = scn_encode(1, 42);
	UT_ASSERT_EQ(memcmp(&g_last_written_slot, &successor, sizeof(successor)), 0);
	UT_ASSERT(!cluster_tt_local_commit_admission.entered);
	UT_ASSERT_EQ(status_installs + hint_emits + abort_marks, 0);
}

UT_TEST(test_local_commit_refusal_keeps_binding_without_terminal_cleanup)
{
	ClusterTTLocalBinding binding;
	xl_xact_tt_commit fold = { 0 };
	TTSlot before = retained_commit_seed();

	before.first_undo_block = uba_encode(257, 9, 7, 3);
	retained_abort_store(&before);
	seed_binding(&binding, CLUSTER_CANONICAL_TXN_PUBLISHED, CLUSTER_TT_LOCAL_TERMINAL_NONE);
	binding.active_lsn = (XLogRecPtr)0x1000;
	UT_ASSERT(!retained_local_precommit(&fold));
	UT_ASSERT_EQ(binding.terminal_state, CLUSTER_TT_LOCAL_TERMINAL_NONE);
	UT_ASSERT_EQ(binding.publish_state, CLUSTER_CANONICAL_TXN_PUBLISHED);
	UT_ASSERT_EQ(g_write_hdr_calls + status_installs + hint_emits + abort_marks, 0);
	UT_ASSERT_EQ(g_writer_fd_open, 0);
	UT_ASSERT(!cluster_tt_local_commit_admission.entered);
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(test_local_commit_retains_head_and_original_32_byte_fold);
	UT_RUN(test_local_commit_refusal_keeps_binding_without_terminal_cleanup);
	UT_RUN(test_local_abort_accepts_retained_successor_without_losing_head);
	UT_RUN(test_local_retained_abort_write_failure_cannot_publish_cleanup);
	UT_RUN(test_unproved_publication_cannot_skip_durable_abort);
	UT_RUN(test_batch_rejected_before_any_hint_or_allocator_mutation);
	UT_RUN(test_reservation_and_proved_abort_keep_existing_cleanup);
	UT_RUN(test_post_wal_install_failure_preserves_unfinished_binding);
	UT_RUN(test_abort_after_unwritten_commit_stage_aborts_durably);
	UT_RUN(test_commit_staged_binding_without_its_own_stage_is_refused);
	UT_DONE();
	return ut_failed_count != 0;
}
