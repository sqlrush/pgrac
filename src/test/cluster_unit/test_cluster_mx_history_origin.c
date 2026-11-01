/* Typed history through the real original current/release continuation.
 * Storage/admission/native status/original-source leaves are from the
 * original fixture; no TT40, grant or complete native warm claim. */
int history_unused_mx_origin_main(void);
#define MX_OPERATION_ORIGIN_TEST_MAIN history_unused_mx_origin_main
#define TransactionIdGetStatus creator_fixture_xid_status
#include "test_cluster_mx_operation_origin.c"
#undef TransactionIdGetStatus
#undef MX_OPERATION_ORIGIN_TEST_MAIN
#include "cluster/cluster_mx_history_record.h"

typedef struct HistoryOriginFixture {
	MxOriginFixture base;
	ClusterMxHistoryRequest request;
	ClusterMxHistoryRequestWire wire;
	ClusterTxResolution top;
	ClusterMxHistoryRecord record;
	uint32 data_generation;
	PGAlignedBlock output;
} HistoryOriginFixture;

static bool creator_commit_needs_flush;
XidStatus TransactionIdGetStatus(TransactionId xid, XLogRecPtr *lsn);
XidStatus
TransactionIdGetStatus(TransactionId xid, XLogRecPtr *lsn)
{
	XidStatus status = creator_fixture_xid_status(xid, lsn);
	if (creator_commit_needs_flush && lsn != NULL)
		*lsn = test_flush_lsn + 1;
	return status;
}
static HistoryOriginFixture *history_observed;
static int history_action, history_release;
static bool history_cold_cache_race;
static int history_cold_fault;

/* Only the disk/cache scheduling leaf is controlled. All held qualification,
 * record decoding, generation fences and cancellation remain production. */
char *
cluster_undo_buf_pin_observed(uint32 segment, uint8 owner, uint32 block, ClusterUndoBufMode mode,
							  ClusterUndoBufPin *pin,
							  volatile ClusterUndoBufReadObservation *observation)
{
	char *bytes;
	observation->read_started = !history_cold_cache_race && !test_candidate_data_pool_disabled;
	observation->read_ns = observation->read_started ? 123000 : 0;
	bytes = cluster_undo_buf_pin(segment, owner, block, mode, pin);
	if (history_cold_fault == 1)
		test_candidate_data_generation++;
	if (history_cold_fault == 2)
		test_admission_revoked = true;
	if (history_cold_fault == 3) {
		test_candidate_root_drift = true;
		test_regular_root_resolve_calls = 2;
	}
	return bytes;
}

static bool
history_is_zero(const void *p, size_t n)
{
	const uint8 *b = p;
	for (size_t i = 0; i < n; i++)
		if (b[i])
			return false;
	return true;
}

static UndoRecordHeader *
history_header(void)
{
	return (UndoRecordHeader *)(test_operation_page.data + sizeof(UndoBlockHeader));
}

static void
history_repack_length(void)
{
	UndoRecordHeader *h = history_header();
	UndoSlotDirEntry *slot
		= (UndoSlotDirEntry *)(test_operation_page.data + BLCKSZ - sizeof(*slot));
	slot->record_length = sizeof(*h) + h->payload_length;
	slot->record_type = h->record_type;
	slot->flags = h->flags;
	((UndoBlockHeader *)test_operation_page.data)->free_offset
		= sizeof(UndoBlockHeader) + slot->record_length;
}

static void
history_origin_release(void)
{
	HistoryOriginFixture *f = history_observed;
	UT_ASSERT(history_is_zero(&f->top, sizeof(f->top)));
	UT_ASSERT(history_is_zero(&f->record, sizeof(f->record)));
	UT_ASSERT_EQ(f->data_generation, 0);
	UT_ASSERT_EQ(test_operation_pin_depth, 0);
	if (test_candidate_release_calls != history_release)
		return;
	if (history_action == 1)
		operation_plan_effect(CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
	if (history_action == 2)
		operation_plan_tail()[offsetof(UndoOperationTrailer, command_id)]++;
	if (history_action == 3)
		test_candidate_data_generation++;
	if (history_action == 4)
		test_admission_revoked = true;
	if (history_action == 5) {
		test_candidate_root_drift = true;
		test_regular_root_resolve_calls = 2;
	}
	if (history_action == 6) {
		UndoSavepointRollbackPayload *c = (UndoSavepointRollbackPayload *)(history_header() + 1);
		c->phase = UNDO_SAVEPOINT_ROLLBACK_COMPLETE;
		c->next_uba = c->stop_uba;
		c->visited_count = 1;
		c->last_effect_wal_lsn = 100;
	}
	if (history_action == 7)
		pg_re_throw();
}

static void
history_origin_init(HistoryOriginFixture *f, bool same)
{
	uint32 segment, block;
	uint16 tt, row;
	memset(f, 0, sizeof(*f));
	mx_origin_init(&f->base, same);
	UT_ASSERT(uba_decode_record(f->base.key.locator.uba, &segment, &block, &tt, &row));
	f->request.locator = f->base.key.locator;
	f->request.forward
		= (GcsBlockForwardPayload){ .request_id = 51,
									.epoch = 42,
									.original_requester_node = 1,
									.requester_backend_id = 7,
									.master_node = 0,
									.transition_id = PCM_TRANS_N_TO_S,
									.tag = GcsBlockUndoFetchTagMake(segment, block) };
	f->request.edge_kind = CMXH_ROOT;
	f->request.root_ordinal = f->request.edge_itl_slot = f->request.locator.itl_slot_index;
	f->request.collector_id = 1;
	f->request.capture_generation = 2;
	f->request.backend_incarnation = 3;
	f->request.requester_capability_generation = 4;
	f->request.origin_capability_generation = 5;
	f->request.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	f->request.admission_generation = 5;
	f->request.read_scn = scn_encode(0, 1000);
	f->request.read_epoch = 42;
	f->request.retained_floor = scn_encode(0, 60);
	f->request.snapshot_identity = 8;
	f->request.descriptor_hash = 9;
	f->request.multixact_id = 10;
	f->request.member_count = 2;
	f->request.target_locator = f->base.key.target.locator;
	f->request.target_fork = MAIN_FORKNUM;
	f->request.target_block = 23;
	f->request.target_offset = 3;
	f->request.root = (ClusterItlSlotData){ .xid = TEST_ORIGIN_XID,
											.wrap = 8,
											.flags = ITL_FLAG_ACTIVE,
											.write_scn = scn_encode(0, 70),
											.undo_segment_head = f->request.locator.uba };
	f->request.edge = f->request.root;
	history_observed = f;
	history_action = history_release = 0;
	test_operation_release_observer = history_origin_release;
	memset(f->output.data, 0x5a, BLCKSZ);
}

static void
history_origin_global(HistoryOriginFixture *f)
{
	f->request.edge_kind = CMXH_GLOBAL_PREV;
	f->request.flags |= CLUSTER_MX_HISTORY_EXPECT_OWNER;
	f->request.expected_owner
		= (ClusterTTSlotPhysicalLocator){ .segment_id = f->base.origin.expected.top.segment_id,
										  .xid = TEST_ORIGIN_XID,
										  .slot_offset = TEST_TT_OFFSET,
										  .wrap = TEST_ORIGIN_WRAP };
	f->request.expected_owner_generation = f->base.origin.expected.top.segment_generation;
	f->request.expected_top_fxid = f->base.key.top_fxid;
	f->request.parent = uba_encode(TEST_RECORD_SEGMENT, 8, TEST_TT_OFFSET, 0);
	f->request.parent_data_generation = 17;
	f->request.parent_record_crc = 123;
	f->request.locator.itl_kind = 0;
	f->request.locator.itl_slot_index = 255;
	f->request.edge_itl_slot = 255;
	memset(&f->request.edge, 0, sizeof(f->request.edge));
}

static void
history_origin_v1(void)
{
	UndoRecordHeader *h = history_header();
	h->flags &= ~UNDO_REC_FLAG_HAS_OPERATION;
	h->xid = TEST_ORIGIN_XID;
	h->payload_length -= sizeof(UndoOperationTrailer);
	history_repack_length();
}

static void
history_origin_control(HistoryOriginFixture *f)
{
	UndoRecordHeader *h = history_header();
	UndoOperationTrailer operation;
	UndoSavepointRollbackPayload c
		= { .version = UNDO_SAVEPOINT_ROLLBACK_VERSION,
			.length = UNDO_SAVEPOINT_ROLLBACK_LENGTH,
			.phase = UNDO_SAVEPOINT_ROLLBACK_PENDING,
			.rollback_sequence = 3,
			.start_uba = h->prev_uba,
			.next_uba = h->prev_uba,
			.scope_child_fxid = U64FromFullTransactionId(f->base.key.operation_fxid) };
	memcpy(&operation, operation_plan_tail(), sizeof(operation));
	history_origin_global(f);
	h->record_type = UNDO_RECORD_SAVEPOINT_ROLLBACK;
	h->flags = UNDO_REC_FLAG_HAS_OPERATION;
	memset(&h->target_locator, 0, sizeof(h->target_locator));
	h->target_fork = h->target_block = h->target_offset = 0;
	h->payload_length = sizeof(c) + sizeof(operation);
	memcpy(h + 1, &c, sizeof(c));
	memcpy((char *)(h + 1) + sizeof(c), &operation, sizeof(operation));
	history_repack_length();
}

static bool
history_begin(HistoryOriginFixture *f)
{
	bool ok = cluster_mx_history_request_encode(&f->request, &f->wire);
	UT_ASSERT(ok);
	if (!ok)
		return false;
	ok = cluster_runtime_visibility_history_origin_read_begin(
		&f->base.read, &f->wire, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &f->base.origin.admission,
		&f->base.origin.reason);
	UT_ASSERT(ok);
	return ok;
}

static ClusterRuntimeVisibilityOperationStep
history_step(HistoryOriginFixture *f)
{
	int calls = test_candidate_acquire_calls + test_candidate_poll_calls
				+ test_candidate_release_calls + test_operation_release_poll_count;
	ClusterRuntimeVisibilityOperationStep step;
	memset(&f->top, 0xa5, sizeof(f->top));
	memset(&f->record, 0xa5, sizeof(f->record));
	f->data_generation = UINT32_MAX;
	step = cluster_runtime_visibility_history_origin_read_poll(&f->base.read, &f->top, &f->record,
															   &f->data_generation, f->output.data,
															   &f->base.origin.reason);
	UT_ASSERT(test_candidate_acquire_calls + test_candidate_poll_calls
				  + test_candidate_release_calls + test_operation_release_poll_count - calls
			  <= 1);
	UT_ASSERT_EQ(test_candidate_wait_calls, 0);
	UT_ASSERT_EQ(test_operation_release_wait_count, 0);
	UT_ASSERT_EQ(test_candidate_data_pin_calls, 0);
	if (step != CLUSTER_RUNTIME_VISIBILITY_OPERATION_COMPLETE) {
		bool unchanged = true;
		UT_ASSERT(history_is_zero(&f->top, sizeof(f->top)));
		UT_ASSERT(history_is_zero(&f->record, sizeof(f->record)));
		UT_ASSERT_EQ(f->data_generation, 0);
		for (int i = 0; i < BLCKSZ; i++)
			if ((uint8)f->output.data[i] != 0x5a) {
				unchanged = false;
				break;
			}
		UT_ASSERT(unchanged);
	}
	return step;
}

static ClusterRuntimeVisibilityOperationStep
history_finish(HistoryOriginFixture *f)
{
	ClusterRuntimeVisibilityOperationStep step = CLUSTER_RUNTIME_VISIBILITY_OPERATION_PENDING;
	for (int i = 0; i < 80 && step == CLUSTER_RUNTIME_VISIBILITY_OPERATION_PENDING; i++)
		step = history_step(f);
	UT_ASSERT(step != CLUSTER_RUNTIME_VISIBILITY_OPERATION_PENDING);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(test_operation_pin_depth, 0);
	return step;
}

UT_TEST(history_origin_root_three_phases_same_and_cross_segment)
{
	for (int same = 0; same < 2; same++) {
		HistoryOriginFixture f;
		history_origin_init(&f, same);
		if (!history_begin(&f))
			return;
		UT_ASSERT_EQ(history_finish(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_COMPLETE);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
		UT_ASSERT_EQ(test_candidate_release_calls, 3);
		UT_ASSERT_EQ(test_candidate_max_held_count, 1);
		UT_ASSERT_EQ(f.data_generation, 17);
		UT_ASSERT_EQ(f.record.record_class, CMXH_OPERATION);
		UT_ASSERT_EQ(f.top.outcome, CLUSTER_TX_COMMITTED);
		UT_ASSERT(FullTransactionIdEquals(f.record.identity.top_fxid, f.base.key.top_fxid));
		UT_ASSERT_EQ(memcmp(f.output.data, test_operation_page.data, BLCKSZ), 0);
	}
}

UT_TEST(history_origin_global_and_v1_keep_exact_identity)
{
	for (int v1 = 0; v1 < 2; v1++) {
		HistoryOriginFixture f;
		history_origin_init(&f, false);
		history_origin_global(&f);
		f.request.target_offset++;
		if (v1)
			history_origin_v1();
		if (!history_begin(&f))
			return;
		UT_ASSERT_EQ(history_finish(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_COMPLETE);
		UT_ASSERT(!f.record.matches_target);
		UT_ASSERT_EQ(f.record.record_class, v1 ? CMXH_V1_TOP : CMXH_OPERATION);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
		UT_ASSERT(FullTransactionIdEquals(f.record.identity.top_fxid, f.base.key.top_fxid));
		if (v1)
			UT_ASSERT(
				FullTransactionIdEquals(f.record.identity.operation_fxid, f.base.key.top_fxid));
	}
}

UT_TEST(history_origin_control_returns_fresh_snapshot)
{
	HistoryOriginFixture f;
	history_origin_init(&f, false);
	history_origin_control(&f);
	history_action = 6;
	history_release = 2;
	if (!history_begin(&f))
		return;
	UT_ASSERT_EQ(history_finish(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_COMPLETE);
	UT_ASSERT_EQ(f.record.record_class, CMXH_ROLLBACK_CONTROL);
	UT_ASSERT_EQ(f.record.control.phase, UNDO_SAVEPOINT_ROLLBACK_COMPLETE);
	UT_ASSERT_EQ(f.record.effect, 0);
	UT_ASSERT(!f.record.matches_target);
	UT_ASSERT_EQ(test_ctrc_touch_calls, 0);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
}

UT_TEST(history_origin_missing_original_stops_after_tt_release)
{
	for (int v1 = 0; v1 < 2; v1++) {
		HistoryOriginFixture f;
		history_origin_init(&f, false);
		if (v1)
			history_origin_v1();
		test_original_available = false;
		if (!history_begin(&f))
			return;
		UT_ASSERT_EQ(history_finish(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 2);
		UT_ASSERT_EQ(test_candidate_release_calls, 2);
	}
}

UT_TEST(history_origin_generation_challenges_and_fresh_effect)
{
	for (int fault = 0; fault < 3; fault++) {
		HistoryOriginFixture f;
		history_origin_init(&f, false);
		if (fault == 0) {
			f.request.flags |= CLUSTER_MX_HISTORY_EXPECT_DATA_GENERATION;
			f.request.expected_data_generation = 18;
		}
		if (fault == 1) {
			history_origin_global(&f);
			f.request.expected_owner_generation++;
		}
		if (fault == 2) {
			history_action = 1;
			history_release = 2;
		}
		if (!history_begin(&f))
			return;
		UT_ASSERT_EQ(history_finish(&f), fault == 2 ? CLUSTER_RUNTIME_VISIBILITY_OPERATION_COMPLETE
													: CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
		UT_ASSERT_EQ(test_candidate_acquire_calls, fault == 2 ? 3 : 1);
		if (fault == 2)
			UT_ASSERT_EQ(f.record.effect, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
	}
}

UT_TEST(history_origin_pending_release_and_acquire_never_publish)
{
	for (int phase = 0; phase < 4; phase++) {
		HistoryOriginFixture f;
		history_origin_init(&f, false);
		if (phase)
			test_operation_release_pending_call = phase;
		else {
			test_candidate_acquire_step = CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
			test_candidate_poll_step = CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
		}
		if (!history_begin(&f))
			return;
		if (!phase) {
			for (int i = 0; i < 5; i++)
				UT_ASSERT_EQ(history_step(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_PENDING);
			test_candidate_poll_step = CLUSTER_UNDO_BLOCK0_CURRENT_HELD;
		}
		UT_ASSERT_EQ(history_finish(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_COMPLETE);
	}
}

UT_TEST(history_origin_begin_copies_request_and_cancel_is_idempotent)
{
	for (int stop = 0; stop < 10; stop++) {
		HistoryOriginFixture f;
		history_origin_init(&f, false);
		if (!history_begin(&f))
			return;
		memset(&f.wire, 0xa5, sizeof(f.wire));
		UT_ASSERT(!cluster_runtime_visibility_history_origin_read_begin(
			&f.base.read, &f.wire, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &f.base.origin.admission,
			&f.base.origin.reason));
		for (int i = 0; i < stop; i++)
			UT_ASSERT_EQ(history_step(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_PENDING);
		cluster_runtime_visibility_operation_origin_read_cancel(&f.base.read);
		cluster_runtime_visibility_operation_origin_read_cancel(&f.base.read);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT(history_is_zero(&f.base.read, sizeof(f.base.read)));
	}
}

UT_TEST(history_origin_fresh_identity_and_admission_fences)
{
	for (int action = 2; action <= 5; action++) {
		HistoryOriginFixture f;
		history_origin_init(&f, false);
		history_action = action;
		history_release = 2;
		if (!history_begin(&f))
			return;
		UT_ASSERT_EQ(history_finish(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
	}
}

UT_TEST(history_origin_error_cleanup_and_cold_miss)
{
	for (int phase = 1; phase <= 3; phase++) {
		HistoryOriginFixture f;
		volatile bool caught = false;
		history_origin_init(&f, false);
		history_action = 7;
		history_release = phase;
		if (!history_begin(&f))
			return;
		PG_TRY();
		{
			(void)history_finish(&f);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT(history_is_zero(&f.base.read, sizeof(f.base.read)));
	}
	for (int mask = 1; mask < 3; mask++) {
		HistoryOriginFixture f;
		history_origin_init(&f, false);
		test_candidate_cold_copy_mask = mask;
		if (!history_begin(&f))
			return;
		UT_ASSERT_EQ(history_finish(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
		UT_ASSERT_EQ(test_candidate_data_pin_calls, 0);
	}
}

UT_TEST(history_origin_v1_root_uses_independent_tt_generation)
{
	HistoryOriginFixture f;
	history_origin_init(&f, false);
	history_origin_v1();
	if (!history_begin(&f))
		return;
	UT_ASSERT_EQ(history_finish(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_COMPLETE);
	UT_ASSERT_EQ(f.record.record_class, CMXH_V1_TOP);
	UT_ASSERT(f.record.canonical_generation_known);
	UT_ASSERT_EQ(f.record.identity.canonical_generation, 23);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
	UT_ASSERT_EQ(test_original_calls, 1);
}

UT_TEST(history_origin_unavailable_active_abort_visibility_fail_closed)
{
	for (int kind = 0; kind < 3; kind++) {
		HistoryOriginFixture f;
		history_origin_init(&f, false);
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
			UT_ASSERT(cluster_mx_history_request_encode(&f.request, &f.wire));
			UT_ASSERT(cluster_runtime_visibility_history_origin_read_begin(
				&f.base.read, &f.wire, CLUSTER_TX_RESOLVE_VISIBILITY, &f.base.origin.admission,
				&f.base.origin.reason));
		} else if (!history_begin(&f))
			return;
		UT_ASSERT_EQ(history_finish(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
		UT_ASSERT_EQ(test_original_calls, 0);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 2);
	}
}

UT_TEST(history_origin_final_release_rechecks_admission_and_consumes_once)
{
	HistoryOriginFixture f;
	history_origin_init(&f, false);
	history_action = 4;
	history_release = 3;
	if (!history_begin(&f))
		return;
	UT_ASSERT_EQ(history_finish(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
	UT_ASSERT_EQ(test_candidate_release_calls, 3);
	history_origin_init(&f, false);
	if (!history_begin(&f))
		return;
	UT_ASSERT_EQ(history_finish(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_COMPLETE);
	UT_ASSERT(history_is_zero(&f.base.read, sizeof(f.base.read)));
	memset(f.output.data, 0x5a, BLCKSZ);
	UT_ASSERT_EQ(history_step(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
}

UT_TEST(history_origin_held_plan_never_falls_back_to_untyped_copy)
{
	for (int kind = 0; kind < 3; kind++) {
		HistoryOriginFixture f;
		ClusterUndoBlock0LogicalKey data
			= { .owner_instance = 1, .segment_id = TEST_RECORD_SEGMENT },
			tt;
		ClusterUndoBlock0Result failure;
		ClusterR4PeerOperationProof old_peer;
		history_origin_init(&f, false);
		if (kind == 1)
			history_origin_v1();
		if (kind == 2)
			history_origin_control(&f);
		test_operation_release_observer = NULL;
		UT_ASSERT(cluster_mx_history_request_encode(&f.request, &f.wire));
		UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin_admitted(
						 &data, CLUSTER_UNDO_BLOCK0_SCUR, 0, &f.base.origin.admission,
						 &f.base.origin.guard, &failure),
					 CLUSTER_UNDO_BLOCK0_CURRENT_HELD);
		UT_ASSERT_EQ(cluster_runtime_visibility_origin_plan_freeze_history_held(
						 &f.request, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &f.base.origin.admission,
						 &f.base.origin.guard, &f.base.origin.data_root, &f.base.origin.plan,
						 &f.record, &f.base.origin.reason),
					 CLUSTER_RUNTIME_VISIBILITY_ORIGIN_NEEDS_CANONICAL);
		UT_ASSERT_EQ(f.record.record_class,
					 kind == 0 ? CMXH_OPERATION
							   : (kind == 1 ? CMXH_V1_TOP : CMXH_ROLLBACK_CONTROL));
		UT_ASSERT(
			cluster_runtime_visibility_origin_plan_canonical_logical(&f.base.origin.plan, &tt));
		UT_ASSERT_EQ(cluster_undo_block0_current_release_begin(&f.base.origin.guard, &failure),
					 CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED);
		UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin_admitted(
						 &tt, CLUSTER_UNDO_BLOCK0_SCUR, 0, &f.base.origin.admission,
						 &f.base.origin.guard, &failure),
					 CLUSTER_UNDO_BLOCK0_CURRENT_HELD);
		UT_ASSERT(cluster_runtime_visibility_origin_plan_sample_history_held(
			&f.base.origin.plan, &f.request, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
			&f.base.origin.admission, &f.base.origin.guard, &f.base.origin.tt_root,
			&f.base.origin.top, &f.base.origin.reason));
		UT_ASSERT_EQ(cluster_undo_block0_current_release_begin(&f.base.origin.guard, &failure),
					 CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED);
		UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin_admitted(
						 &data, CLUSTER_UNDO_BLOCK0_SCUR, 0, &f.base.origin.admission,
						 &f.base.origin.guard, &failure),
					 CLUSTER_UNDO_BLOCK0_CURRENT_HELD);
		UT_ASSERT_EQ(cluster_runtime_visibility_origin_plan_copy_data_held(
						 &f.base.origin.plan, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
						 &f.base.origin.admission, &f.base.origin.guard, &f.base.origin.data_root,
						 &f.top, f.output.data, &f.base.origin.reason),
					 CLUSTER_TX_UNKNOWN);
		UT_ASSERT_EQ(cluster_runtime_visibility_origin_plan_copy_operation_held(
						 &f.base.origin.plan, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
						 &f.base.origin.admission, &f.base.origin.guard, &f.base.origin.data_root,
						 &f.top, &f.base.origin.operation, f.output.data, &f.base.origin.reason),
					 CLUSTER_TX_UNKNOWN);
		UT_ASSERT_EQ(cluster_runtime_visibility_origin_plan_copy_peer_held(
						 &f.base.origin.plan, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
						 &f.base.origin.admission, &f.base.origin.guard, &f.base.origin.data_root,
						 &f.top, &old_peer, f.output.data, &f.base.origin.reason),
					 CLUSTER_TX_UNKNOWN);
		UT_ASSERT_EQ(cluster_runtime_visibility_origin_plan_copy_history_held(
						 &f.base.origin.plan, &f.request, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
						 &f.base.origin.admission, &f.base.origin.guard, &f.base.origin.data_root,
						 &f.top, &f.record, &f.data_generation, f.output.data,
						 &f.base.origin.reason),
					 CLUSTER_TX_COMMITTED);
		UT_ASSERT_EQ(cluster_undo_block0_current_release_begin(&f.base.origin.guard, &failure),
					 CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
		UT_ASSERT_EQ(test_candidate_release_calls, 3);
		UT_ASSERT_EQ(test_candidate_max_held_count, 1);
	}
}

UT_TEST(history_origin_current_failures_cancel_each_phase)
{
	for (int phase = 1; phase <= 3; phase++)
		for (int release = 0; release < 2; release++) {
			HistoryOriginFixture f;
			history_origin_init(&f, false);
			if (release) {
				test_candidate_release_failed = true;
				test_candidate_release_fault_call = phase;
			} else
				test_operation_acquire_failure_call = phase;
			if (!history_begin(&f))
				return;
			UT_ASSERT_EQ(history_finish(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
			UT_ASSERT(history_is_zero(&f.base.read, sizeof(f.base.read)));
		}
}

UT_TEST(history_origin_bad_wire_scope_and_poll_arguments)
{
	for (int fault = 0; fault < 4; fault++) {
		HistoryOriginFixture f;
		ClusterMxHistoryRequestWire cancel;
		history_origin_init(&f, false);
		UT_ASSERT(cluster_mx_history_request_encode(&f.request, &f.wire));
		if (fault == 0)
			f.wire.bytes[200]++;
		if (fault == 1) {
			UT_ASSERT(cluster_mx_history_request_cancel(&f.wire, &cancel));
			f.wire = cancel;
		}
		if (fault == 2)
			f.base.origin.admission.record_generation++;
		if (fault == 3)
			test_admission_revoked = true;
		UT_ASSERT(!cluster_runtime_visibility_history_origin_read_begin(
			&f.base.read, &f.wire, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &f.base.origin.admission,
			&f.base.origin.reason));
		UT_ASSERT_EQ(test_candidate_acquire_calls, 0);
		UT_ASSERT(history_is_zero(&f.base.read, sizeof(f.base.read)));
	}
	{
		HistoryOriginFixture f;
		history_origin_init(&f, false);
		if (!history_begin(&f))
			return;
		UT_ASSERT_EQ(history_step(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_PENDING);
		UT_ASSERT_EQ(test_candidate_held_count, 1);
		UT_ASSERT_EQ(cluster_runtime_visibility_history_origin_read_poll(
						 &f.base.read, NULL, &f.record, &f.data_generation, f.output.data,
						 &f.base.origin.reason),
					 CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT(history_is_zero(&f.base.read, sizeof(f.base.read)));
	}
}

static void
history_cold_acquire(HistoryOriginFixture *f)
{
	ClusterUndoBlock0LogicalKey key = { .segment_id = TEST_RECORD_SEGMENT, .owner_instance = 1 };
	ClusterUndoBlock0Result failure;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin_admitted(
					 &key, CLUSTER_UNDO_BLOCK0_SCUR, 0, &f->base.origin.admission,
					 &f->base.origin.guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_HELD);
}

static ClusterHistoryDataStep
history_cold_load(HistoryOriginFixture *f, bool *available)
{
	return cluster_runtime_visibility_history_prepare_data_held(
		&f->request, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &f->base.origin.admission,
		&f->base.origin.guard, &f->base.origin.data_root, available);
}

UT_TEST(history_cold_loads_under_same_guard_and_preserves_record_gates)
{
	HistoryOriginFixture f;
	ClusterHistoryColdStats before, after;
	bool available = true;
	history_origin_init(&f, false);
	history_cold_cache_race = false;
	history_cold_acquire(&f);
	test_candidate_cold_copy_mask = 1;
	cluster_runtime_visibility_history_cold_snapshot(&before);
	UT_ASSERT_EQ(history_cold_load(&f, &available), CLUSTER_HISTORY_DATA_READY);
	UT_ASSERT(!available);
	UT_ASSERT_EQ(test_candidate_data_pin_calls, 1);
	UT_ASSERT_EQ(test_candidate_data_unpin_calls, 1);
	UT_ASSERT_EQ(test_operation_pin_depth, 0);
	UT_ASSERT_EQ(test_candidate_held_count, 1);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 1);
	cluster_runtime_visibility_history_cold_snapshot(&after);
	UT_ASSERT_EQ(after.reads - before.reads, 1);
	UT_ASSERT_EQ(after.read_ns - before.read_ns, 123000);
	UT_ASSERT_EQ(after.read_failures, before.read_failures);
	/* Loaded bytes remain untrusted until the original decoder. */
	history_header()->xid++;
	UT_ASSERT_EQ(cluster_runtime_visibility_origin_plan_freeze_history_held(
					 &f.request, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &f.base.origin.admission,
					 &f.base.origin.guard, &f.base.origin.data_root, &f.base.origin.plan, &f.record,
					 &f.base.origin.reason),
				 CLUSTER_RUNTIME_VISIBILITY_ORIGIN_FAILED);
	UT_ASSERT(history_is_zero(&f.record, sizeof(f.record)));
	cluster_undo_block0_current_cancel(&f.base.origin.guard);
}

UT_TEST(history_cold_budget_defers_miss_but_allows_warm)
{
	HistoryOriginFixture f;
	bool available = false;
	history_origin_init(&f, false);
	history_cold_acquire(&f);
	UT_ASSERT_EQ(history_cold_load(&f, &available), CLUSTER_HISTORY_DATA_READY);
	UT_ASSERT_EQ(test_candidate_data_pin_calls, 0);
	/* Force the next resident lookup to miss, without changing the guard. */
	test_candidate_data_copy_calls = 0;
	test_candidate_cold_copy_mask = 1;
	UT_ASSERT_EQ(history_cold_load(&f, &available), CLUSTER_HISTORY_DATA_DEFERRED);
	UT_ASSERT_EQ(test_candidate_data_pin_calls, 0);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 1);
	UT_ASSERT_EQ(test_candidate_held_count, 1);
	cluster_undo_block0_current_cancel(&f.base.origin.guard);
}

UT_TEST(history_cold_rejects_wrong_scope_namespace_and_generation_before_io)
{
	for (int fault = 0; fault < 5; fault++) {
		HistoryOriginFixture f;
		bool available = true;
		history_origin_init(&f, false);
		history_cold_acquire(&f);
		test_candidate_cold_copy_mask = 1;
		if (fault == 0)
			f.request.forward.master_node = 1;
		if (fault == 1)
			f.base.origin.data_root.root_id++;
		if (fault == 2)
			test_admission_revoked = true;
		if (fault == 3) {
			f.request.flags |= CLUSTER_MX_HISTORY_EXPECT_DATA_GENERATION;
			f.request.expected_data_generation = 18;
		}
		if (fault == 4)
			CritSectionCount = 1;
		UT_ASSERT_EQ(history_cold_load(&f, &available), CLUSTER_HISTORY_DATA_FAILED);
		CritSectionCount = 0;
		UT_ASSERT(available);
		UT_ASSERT_EQ(test_candidate_data_pin_calls, 0);
		cluster_undo_block0_current_cancel(&f.base.origin.guard);
	}
}

UT_TEST(history_cold_error_and_disabled_pool_preserve_unknown_and_account_reads)
{
	for (int fault = 0; fault < 3; fault++) {
		HistoryOriginFixture f;
		ClusterHistoryColdStats before, after;
		volatile bool caught = false;
		bool available = true;
		history_origin_init(&f, false);
		history_cold_cache_race = fault == 2;
		history_cold_acquire(&f);
		test_candidate_cold_copy_mask = 1;
		test_candidate_data_io_throw = fault == 0;
		test_candidate_data_pool_disabled = fault == 1;
		cluster_runtime_visibility_history_cold_snapshot(&before);
		PG_TRY();
		{
			UT_ASSERT_EQ(history_cold_load(&f, &available),
						 fault == 2 ? CLUSTER_HISTORY_DATA_READY : CLUSTER_HISTORY_DATA_FAILED);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		cluster_runtime_visibility_history_cold_snapshot(&after);
		UT_ASSERT_EQ(caught, fault == 0);
		UT_ASSERT_EQ(after.reads - before.reads, fault == 0 ? 1 : 0);
		UT_ASSERT_EQ(after.read_failures - before.read_failures, fault == 0 ? 1 : 0);
		UT_ASSERT_EQ(test_operation_pin_depth, 0);
		UT_ASSERT_EQ(test_candidate_held_count, 1);
		cluster_undo_block0_current_cancel(&f.base.origin.guard);
	}
	history_cold_cache_race = false;
}

UT_TEST(history_cold_post_io_drift_never_becomes_proof)
{
	for (int fault = 1; fault <= 3; fault++) {
		HistoryOriginFixture f;
		bool available = true;
		history_origin_init(&f, false);
		history_cold_acquire(&f);
		test_candidate_cold_copy_mask = 1;
		history_cold_fault = fault;
		UT_ASSERT_EQ(history_cold_load(&f, &available), CLUSTER_HISTORY_DATA_FAILED);
		UT_ASSERT_EQ(test_candidate_data_pin_calls, 1);
		UT_ASSERT_EQ(test_candidate_data_unpin_calls, 1);
		UT_ASSERT_EQ(test_operation_pin_depth, 0);
		UT_ASSERT(history_is_zero(&f.record, sizeof(f.record)));
		cluster_undo_block0_current_cancel(&f.base.origin.guard);
	}
	history_cold_fault = 0;
}

UT_TEST(history_cold_first_and_fresh_data_use_only_original_three_currents)
{
	HistoryOriginFixture f;
	ClusterUndoBlock0LogicalKey tt = { .segment_id = TEST_TT_SEGMENT, .owner_instance = 1 };
	ClusterUndoBlock0Result failure;
	bool available = true;
	char dump[384];
	history_origin_init(&f, false);
	test_operation_release_observer = NULL;
	history_cold_acquire(&f);
	test_candidate_cold_copy_mask = 1;
	UT_ASSERT_EQ(history_cold_load(&f, &available), CLUSTER_HISTORY_DATA_READY);
	UT_ASSERT_EQ(cluster_runtime_visibility_origin_plan_freeze_history_held(
					 &f.request, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &f.base.origin.admission,
					 &f.base.origin.guard, &f.base.origin.data_root, &f.base.origin.plan, &f.record,
					 &f.base.origin.reason),
				 CLUSTER_RUNTIME_VISIBILITY_ORIGIN_NEEDS_CANONICAL);
	UT_ASSERT_EQ(cluster_undo_block0_current_release_begin(&f.base.origin.guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED);
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin_admitted(&tt, CLUSTER_UNDO_BLOCK0_SCUR,
																	0, &f.base.origin.admission,
																	&f.base.origin.guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_HELD);
	UT_ASSERT(cluster_runtime_visibility_origin_plan_sample_history_held(
		&f.base.origin.plan, &f.request, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
		&f.base.origin.admission, &f.base.origin.guard, &f.base.origin.tt_root, &f.top,
		&f.base.origin.reason));
	UT_ASSERT_EQ(cluster_undo_block0_current_release_begin(&f.base.origin.guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED);
	operation_plan_effect(CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
	history_cold_acquire(&f);
	available = true; /* a later drain */
	test_candidate_data_copy_calls = 0;
	UT_ASSERT_EQ(history_cold_load(&f, &available), CLUSTER_HISTORY_DATA_READY);
	UT_ASSERT_EQ(cluster_runtime_visibility_origin_plan_copy_history_held(
					 &f.base.origin.plan, &f.request, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
					 &f.base.origin.admission, &f.base.origin.guard, &f.base.origin.data_root,
					 &f.top, &f.record, &f.data_generation, f.output.data, &f.base.origin.reason),
				 CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(f.record.effect, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
	UT_ASSERT_EQ(cluster_undo_block0_current_release_begin(&f.base.origin.guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
	UT_ASSERT_EQ(test_candidate_release_calls, 3);
	UT_ASSERT_EQ(test_candidate_data_pin_calls, 2);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	cluster_runtime_visibility_history_cold_dump(dump, sizeof(dump));
	UT_ASSERT(strstr(dump, "p50_le_us=123") != NULL);
	UT_ASSERT(strstr(dump, "p99_le_us=123") != NULL);
	UT_ASSERT(strstr(dump, "max_read_ns=123000") != NULL);
}


static void
creator_origin_init(HistoryOriginFixture *f)
{
	history_origin_init(f, false);
	history_origin_v1();
	f->request.edge_kind = CMXH_CREATOR_ROOT;
	f->request.retained_floor = 0;
	f->request.descriptor_hash = 0;
	f->request.multixact_id = 0;
	f->request.member_count = 0;
}
UT_TEST(creator_origin_reuses_three_phases_without_live_grant)
{
	HistoryOriginFixture f;
	creator_origin_init(&f);
	if (!history_begin(&f))
		return;
	UT_ASSERT_EQ(history_finish(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_COMPLETE);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
	UT_ASSERT_EQ(test_candidate_release_calls, 3);
	UT_ASSERT_EQ(f.top.outcome, CLUSTER_TX_COMMITTED);
	UT_ASSERT(
		FullTransactionIdEquals(f.record.identity.top_fxid, f.record.identity.operation_fxid));
}
UT_TEST(creator_origin_refuses_missing_identity_and_unflushed_commit)
{
	for (int fault = 0; fault < 3; fault++) {
		HistoryOriginFixture f;
		creator_origin_init(&f);
		if (fault == 0)
			test_original_available = false;
		if (fault == 1) {
			test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
			test_twophase_xid = TEST_ORIGIN_XID;
		}
		if (fault == 2)
			creator_commit_needs_flush = true;
		if (!history_begin(&f))
			return;
		UT_ASSERT_EQ(history_finish(&f), CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
		UT_ASSERT(history_is_zero(&f.top, sizeof(f.top)));
		UT_ASSERT_EQ(test_candidate_acquire_calls, 2);
		UT_ASSERT_EQ(test_candidate_release_calls, 2);
		creator_commit_needs_flush = false;
	}
}

int
main(void)
{
	UT_PLAN(23);
	UT_RUN(history_origin_root_three_phases_same_and_cross_segment);
	UT_RUN(history_origin_global_and_v1_keep_exact_identity);
	UT_RUN(history_origin_control_returns_fresh_snapshot);
	UT_RUN(history_origin_missing_original_stops_after_tt_release);
	UT_RUN(history_origin_generation_challenges_and_fresh_effect);
	UT_RUN(history_origin_pending_release_and_acquire_never_publish);
	UT_RUN(history_origin_begin_copies_request_and_cancel_is_idempotent);
	UT_RUN(history_origin_fresh_identity_and_admission_fences);
	UT_RUN(history_origin_error_cleanup_and_cold_miss);
	UT_RUN(history_origin_v1_root_uses_independent_tt_generation);
	UT_RUN(history_origin_unavailable_active_abort_visibility_fail_closed);
	UT_RUN(history_origin_final_release_rechecks_admission_and_consumes_once);
	UT_RUN(history_origin_held_plan_never_falls_back_to_untyped_copy);
	UT_RUN(history_origin_current_failures_cancel_each_phase);
	UT_RUN(history_origin_bad_wire_scope_and_poll_arguments);
	UT_RUN(history_cold_loads_under_same_guard_and_preserves_record_gates);
	UT_RUN(history_cold_budget_defers_miss_but_allows_warm);
	UT_RUN(history_cold_rejects_wrong_scope_namespace_and_generation_before_io);
	UT_RUN(history_cold_error_and_disabled_pool_preserve_unknown_and_account_reads);
	UT_RUN(history_cold_post_io_drift_never_becomes_proof);
	UT_RUN(history_cold_first_and_fresh_data_use_only_original_three_currents);
	UT_RUN(creator_origin_reuses_three_phases_without_live_grant);
	UT_RUN(creator_origin_refuses_missing_identity_and_unflushed_commit);
	UT_DONE();
	return ut_failed_count != 0;
}
