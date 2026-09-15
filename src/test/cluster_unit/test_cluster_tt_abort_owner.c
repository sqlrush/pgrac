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

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

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
cluster_tt_slot_mark_aborted(uint32 segment_id, uint16 slot_offset, TransactionId xid)
{
	UT_ASSERT(segment_id == 1 && slot_offset < TT_SLOTS_PER_SEGMENT && xid >= 100);
	abort_marks++;
}

void
cluster_tt_slot_mark_committed(uint32 segment_id pg_attribute_unused(),
							   uint16 slot_offset pg_attribute_unused(),
							   TransactionId xid pg_attribute_unused(),
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

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_unproved_publication_cannot_skip_durable_abort);
	UT_RUN(test_batch_rejected_before_any_hint_or_allocator_mutation);
	UT_RUN(test_reservation_and_proved_abort_keep_existing_cleanup);
	UT_RUN(test_post_wal_install_failure_preserves_unfinished_binding);
	UT_DONE();
	return ut_failed_count != 0;
}
