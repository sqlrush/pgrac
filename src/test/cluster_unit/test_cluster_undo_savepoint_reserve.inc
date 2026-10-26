/* Real record-owner tests. Boundary services are explicit fixture inputs.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#ifndef CLUSTER_UNDO_SAVEPOINT_RESERVE_PRESENT
typedef struct ClusterUndoSavepointControl {
	int missing;
} ClusterUndoSavepointControl;
static bool
cluster_undo_record_reserve_savepoint_controls(const ClusterSavepointOperation *operations,
											   uint32 count, uint64 deadline,
											   ClusterUndoSavepointControl **out)
{
	(void)operations;
	(void)deadline;
	for (uint32 i = 0; i < count; i++)
		out[i] = NULL;
	return false;
}
static void
cluster_undo_record_release_savepoint_control(ClusterUndoSavepointControl *control)
{
	(void)control;
}
static bool
cluster_undo_record_savepoint_control_owned(const ClusterUndoSavepointControl *control)
{
	(void)control;
	return false;
}
static void
cluster_undo_record_release_savepoint_controls_at_xact_end(void)
{}
#endif

char *
cluster_undo_buf_pin(uint32 segment_id, uint8 owner, uint32 block_no, ClusterUndoBufMode mode,
					 ClusterUndoBufPin *pin)
{
	(void)mode;
	UT_ASSERT(reserve_test_mode);
	UT_ASSERT_EQ(segment_id, 1);
	UT_ASSERT_EQ(owner, 1);
	UT_ASSERT(block_no >= 1 && block_no <= 2);
	pin->slot = 7 + block_no - 1;
	reserve_test_pins++;
	if (reserve_test_expire_at_pin)
		receipt_clock_us = 100;
	reserve_test_io++;
	return reserve_test_images[block_no - 1].data;
}
void
cluster_undo_buf_addref(const ClusterUndoBufPin *pin)
{
	UT_ASSERT(reserve_test_pins > 0);
	UT_ASSERT(pin->slot == 7 || pin->slot == 8);
	reserve_test_refs++;
}
void
cluster_undo_buf_unpin(ClusterUndoBufPin *pin)
{
	UT_ASSERT(pin->slot == 7 || pin->slot == 8);
	reserve_test_pins--;
	pin->slot = -1;
}
void
cluster_undo_buf_mark_dirty(const ClusterUndoBufPin *pin, XLogRecPtr lsn)
{
	(void)pin;
	(void)lsn;
}
bool
cluster_undo_buf_writeback_allowed(void)
{
	return false;
}
void
cluster_undo_smgr_fd_cache_reset(void)
{}

MemoryContext TopTransactionContext = (MemoryContext)1;
static void *reserve_test_allocations[32];
static unsigned reserve_test_allocation_count;

void *
MemoryContextAllocZero(MemoryContext context, Size size)
{
	void *ptr = calloc(1, size);

	UT_ASSERT(context == TopTransactionContext);
	UT_ASSERT_NOT_NULL(ptr);
	UT_ASSERT(reserve_test_allocation_count < 32);
	reserve_test_allocations[reserve_test_allocation_count++] = ptr;
	return ptr;
}
void
pfree(void *pointer)
{
	for (unsigned i = 0; i < reserve_test_allocation_count; i++)
		if (reserve_test_allocations[i] == pointer)
			reserve_test_allocations[i] = NULL;
	free(pointer);
}
static uint32
reserve_test_active_segment(int node)
{
	if (!reserve_test_mode)
		return cluster_undo_active_segment_for_node_or_create(node);
	UT_ASSERT_EQ(node, 0);
	return 1;
}
ClusterNormalStartState
cluster_semantic_normal_start_state(void)
{
	return CLUSTER_NORMAL_START_TARGET_READY;
}
bool
cluster_semantic_activation_recovered_open_current(void)
{
	return true;
}
ClusterSemanticAdmissionResult
cluster_semantic_activation_modifier_enter(bool writable, ClusterSemanticAdmissionToken *token)
{
	memset(token, 0, sizeof(*token));
	if (!writable || !receipt_modifier_valid)
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	token->entered = true;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}
ClusterUndoBlock0Result
cluster_undo_block0_current_live_owner_ensure_resident_exact(
	const ClusterUndoBlock0LogicalKey *key, int timeout,
	ClusterUndoBlock0LiveOwnerPublication *publication)
{
	(void)key;
	(void)timeout;
	(void)publication;
	UT_ASSERT(false); /* The exact resident publication is an explicit input. */
	return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
}
ClusterUndoBlock0Result
cluster_undo_block0_current_live_owner_provision(const ClusterUndoBlock0LogicalKey *key,
												 int timeout)
{
	(void)key;
	(void)timeout;
	UT_ASSERT(false);
	return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
}
ClusterUndoBlock0Result
cluster_undo_block0_current_live_owner_reuse_exact(const ClusterUndoBlock0LogicalKey *key,
												   const ClusterUndoBlock0Generation *expected,
												   const char successor[BLCKSZ], int timeout)
{
	(void)key;
	(void)expected;
	(void)successor;
	(void)timeout;
	UT_ASSERT(false);
	return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
}
void
cluster_undo_buf_invalidate_segment(uint32 segment, uint8 owner)
{
	(void)segment;
	(void)owner;
	UT_ASSERT(false);
}
void
cluster_undo_cleaner_wakeup(void)
{}

bool
cluster_savepoint_operation_matches_frame(const ClusterSavepointOperation *operation)
{
	if (!reserve_test_scope_valid)
		return false;
	for (unsigned i = 0; i < RESERVE_TEST_MAX; i++)
		if (memcmp(operation, &reserve_test_operations[i], sizeof(*operation)) == 0)
			return true;
	return false;
}

static void
reserve_test_fixture(void)
{
	ClusterUndoRecordPrepareReceipt receipt;

	receipt_fixture_ready(UNDO_RECORD_INSERT, &receipt);
	cluster_undo_record_cancel_prepared(&receipt);
	receipt_unref_calls = receipt_leave_calls = 0;
	reserve_test_mode = reserve_test_scope_valid = true;
	reserve_test_refs = reserve_test_pins = reserve_test_io = 0;
	reserve_test_wal = reserve_test_installs = 0;
	reserve_test_fail_lock = reserve_test_throw_wal = false;
	reserve_test_throw_wal_at = 0;
	reserve_test_invalidate_scope_after_lock = reserve_test_expire_at_pin = false;
	memset(reserve_test_images, 0, sizeof(reserve_test_images));
	memset(reserve_test_wal_slots, 0, sizeof(reserve_test_wal_slots));
	memset(&reserve_test_proc, 0, sizeof(reserve_test_proc));
	MyProc = &reserve_test_proc;
	cluster_shared_config = cluster_enabled = true;
	UndoRecordShared->active_segment_id = 1;
	cluster_undo_residual_validated_this_xact = true;
	cluster_undo_current_extent.nblocks = 2;
	cluster_undo_record_block0_publication_segment_id = 1;
	for (unsigned i = 0; i < RESERVE_TEST_MAX; i++) {
		reserve_test_operations[i] = receipt_operation;
		reserve_test_operations[i].operation_fxid = FullTransactionIdFromEpochAndXid(5, 701 + i);
		reserve_test_operations[i].subid = 2 + i;
		reserve_test_operations[i].operation_sequence = 31 + i;
	}
}

static void
reserve_test_done(void)
{
	cluster_undo_record_release_savepoint_controls_at_xact_end();
	cluster_undo_record_xact_reset();
	UT_ASSERT_EQ(reserve_test_refs, 0);
	UT_ASSERT_EQ(reserve_test_pins, 0);
	for (unsigned i = 0; i < reserve_test_allocation_count; i++)
		free(reserve_test_allocations[i]);
	reserve_test_allocation_count = 0;
	reserve_test_mode = false;
	cluster_enabled = cluster_shared_config = false;
	MyProc = NULL;
}

UT_TEST(test_reserved_control_is_physical_but_not_top_head)
{
	ClusterUndoSavepointControl *control = NULL;
	UndoSavepointRollbackPayload body;
	UndoOperationTrailer operation;
	UndoRecordHeader record;
	UBA original = uba_encode(1, 1, 0, 9);

	reserve_test_fixture();
	cluster_undo_local_heads[0].tt_slot_segment_id = 1;
	cluster_undo_local_heads[0].tt_slot_offset = 0;
	cluster_undo_local_heads[0].head = original;
	cluster_undo_local_head_count = 1;
	UT_ASSERT(
		cluster_undo_record_reserve_savepoint_controls(reserve_test_operations, 1, 100, &control));
	UT_ASSERT_NOT_NULL(control);
	memcpy(&record, reserve_test_images[0].data + 48, 64);
	UT_ASSERT_EQ(record.record_type, 5);
	UT_ASSERT_EQ(record.xid, 701);
	UT_ASSERT_EQ(record.payload_length, 144);
	UT_ASSERT(UBA_is_invalid(record.prev_uba));
	UT_ASSERT(cluster_undo_record_decode_rollback_payload(
		&record, reserve_test_images[0].data + 112, 144, &body, &operation));
	UT_ASSERT_EQ(body.phase, 1);
	UT_ASSERT_EQ(body.rollback_sequence, 31);
	UT_ASSERT_EQ(body.visited_count, 0);
	UT_ASSERT(UBA_is_invalid(body.start_uba) && UBA_is_invalid(body.stop_uba)
			  && UBA_is_invalid(body.next_uba));
	UT_ASSERT_EQ(operation.operation_fxid, UINT64CONST(21474837181));
	UT_ASSERT(memcmp(&original, &cluster_undo_local_heads[0].head, sizeof(original)) == 0);
	UT_ASSERT_EQ(cluster_undo_local_head_count, 1);
	UT_ASSERT_EQ(reserve_test_refs, 1);
	UT_ASSERT_EQ(receipt_apply_calls + receipt_prepare_calls, 0);
	UT_ASSERT_EQ(reserve_test_wal, 1);
	cluster_undo_record_release_savepoint_control(control);
	UT_ASSERT_EQ(reserve_test_refs, 0);
	cluster_undo_record_release_savepoint_control(control);
	UT_ASSERT_EQ(reserve_test_refs, 0);
	reserve_test_done();
}

UT_TEST(test_reserved_controls_batch_with_independent_refs)
{
	ClusterUndoSavepointControl *controls[3] = { 0 };
	UndoBlockHeader *header;

	reserve_test_fixture();
	UT_ASSERT(
		cluster_undo_record_reserve_savepoint_controls(reserve_test_operations, 3, 100, controls));
	header = (UndoBlockHeader *)reserve_test_images[0].data;
	UT_ASSERT_EQ(header->slot_count, 3);
	UT_ASSERT_EQ(header->free_offset, 672);
	UT_ASSERT_EQ(reserve_test_wal, 1);
	UT_ASSERT_EQ(reserve_test_wal_slots[0], 24);
	UT_ASSERT_EQ(reserve_test_refs, 3);
	cluster_undo_record_release_savepoint_control(controls[1]);
	UT_ASSERT_EQ(reserve_test_refs, 2);
	cluster_undo_record_release_savepoint_control(controls[0]);
	UT_ASSERT_EQ(reserve_test_refs, 1);
	UT_ASSERT_EQ(header->slot_count, 3);
	cluster_undo_record_release_savepoint_control(controls[2]);
	UT_ASSERT_EQ(reserve_test_refs, 0);
	reserve_test_done();
}

UT_TEST(test_reserved_controls_refuse_existing_data_reservation)
{
	ClusterUndoSavepointControl *out = (void *)1;
	ClusterUndoRecordPrepareReceipt original;

	reserve_test_fixture();
	reserve_test_mode = false;
	receipt_fixture_ready(UNDO_RECORD_INSERT, &original);
	reserve_test_mode = true;
	reserve_test_refs = 1;
	UT_ASSERT(
		!cluster_undo_record_reserve_savepoint_controls(reserve_test_operations, 1, 100, &out));
	UT_ASSERT(out == NULL);
	UT_ASSERT(memcmp(&original, &cluster_undo_record_reservation.receipt, sizeof(original)) == 0);
	UT_ASSERT_EQ(reserve_test_io + reserve_test_wal, 0);
	cluster_undo_record_cancel_prepared(&original);
	reserve_test_done();
}

UT_TEST(test_reserved_controls_refuse_unproved_scope_before_storage)
{
	ClusterUndoSavepointControl *out[2] = { (void *)1, (void *)1 };

	reserve_test_fixture();
	reserve_test_scope_valid = false;
	UT_ASSERT(
		!cluster_undo_record_reserve_savepoint_controls(reserve_test_operations, 2, 100, out));
	UT_ASSERT(out[0] == NULL && out[1] == NULL);
	UT_ASSERT_EQ(reserve_test_io + reserve_test_wal + reserve_test_refs, 0);
	reserve_test_done();
}

UT_TEST(test_reserved_controls_fail_before_install_without_leaking_refs)
{
	ClusterUndoSavepointControl *out = NULL;

	for (unsigned leg = 0; leg < 5; leg++) {
		reserve_test_fixture();
		if (leg == 0)
			reserve_test_fail_lock = true;
		if (leg == 1)
			receipt_modifier_valid = false;
		if (leg == 2)
			reserve_test_invalidate_scope_after_lock = true;
		if (leg == 3)
			receipt_block0_conditional_available = false;
		if (leg == 4)
			reserve_test_expire_at_pin = true;
		UT_ASSERT(
			!cluster_undo_record_reserve_savepoint_controls(reserve_test_operations, 1, 100, &out));
		UT_ASSERT(out == NULL);
		UT_ASSERT_EQ(reserve_test_installs + reserve_test_wal + reserve_test_refs, 0);
		UT_ASSERT(!cluster_undo_record_reservation.active);
		reserve_test_done();
	}
}

UT_TEST(test_reserved_controls_wal_error_preserves_original_cleanup)
{
	ClusterUndoSavepointControl *out = NULL;
	volatile bool caught = false;

	reserve_test_fixture();
	reserve_test_proc.delayChkptFlags = DELAY_CHKPT_COMPLETE;
	reserve_test_throw_wal = true;
	PG_TRY();
	{
		(void)cluster_undo_record_reserve_savepoint_controls(reserve_test_operations, 1, 100, &out);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(out == NULL);
	UT_ASSERT_EQ(reserve_test_proc.delayChkptFlags, DELAY_CHKPT_COMPLETE);
	UT_ASSERT_EQ(reserve_test_installs + reserve_test_refs + reserve_test_pins, 0);
	UT_ASSERT(!cluster_undo_record_reservation.active);
	reserve_test_done();
}

UT_TEST(test_reserved_controls_cross_block_and_release_only_owned_refs)
{
	ClusterUndoSavepointControl *controls[38] = { 0 };

	reserve_test_fixture();
	UT_ASSERT(
		cluster_undo_record_reserve_savepoint_controls(reserve_test_operations, 38, 100, controls));
	UT_ASSERT_EQ(reserve_test_wal, 2);
	UT_ASSERT_EQ(((UndoBlockHeader *)reserve_test_images[0].data)->slot_count, 37);
	UT_ASSERT_EQ(((UndoBlockHeader *)reserve_test_images[1].data)->slot_count, 1);
	UT_ASSERT_EQ(reserve_test_refs, 38);
	for (unsigned i = 0; i < 38; i++)
		cluster_undo_record_release_savepoint_control(controls[i]);
	UT_ASSERT_EQ(reserve_test_refs, 0);
	UT_ASSERT_EQ(reserve_test_installs, 2);
	reserve_test_done();
}

UT_TEST(test_reserved_controls_top_teardown_retires_all_unused_refs)
{
	ClusterUndoSavepointControl *controls[2] = { 0 };

	reserve_test_fixture();
	UT_ASSERT(
		cluster_undo_record_reserve_savepoint_controls(reserve_test_operations, 2, 100, controls));
	UT_ASSERT_EQ(reserve_test_refs, 2);
	cluster_undo_record_release_savepoint_controls_at_xact_end();
	cluster_undo_record_xact_commit_release();
	UT_ASSERT_EQ(reserve_test_refs, 0);
	cluster_undo_record_release_savepoint_control(controls[0]);
	UT_ASSERT_EQ(reserve_test_refs, 0);
	reserve_test_done();
}

UT_TEST(test_reserved_controls_later_batch_error_keeps_prior_parent)
{
	ClusterUndoSavepointControl *parent = NULL;
	ClusterUndoSavepointControl *out[38] = { 0 };
	volatile bool caught = false;

	reserve_test_fixture();
	UT_ASSERT(
		cluster_undo_record_reserve_savepoint_controls(reserve_test_operations, 1, 100, &parent));
	reserve_test_throw_wal_at = 3;
	PG_TRY();
	{
		(void)cluster_undo_record_reserve_savepoint_controls(reserve_test_operations + 1, 38, 100,
															 out);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	for (unsigned i = 0; i < 38; i++)
		UT_ASSERT(out[i] == NULL);
	UT_ASSERT_EQ(reserve_test_refs, 1);
	UT_ASSERT_EQ(reserve_test_wal, 3);
	UT_ASSERT_EQ(reserve_test_installs, 2);
	UT_ASSERT(cluster_undo_record_savepoint_control_owned(parent));
	UT_ASSERT_EQ(cluster_undo_local_head_count, 0);
	cluster_undo_record_release_savepoint_control(parent);
	reserve_test_done();
}

UT_TEST(test_reserved_controls_nonshared_pending_keeps_its_original_ref)
{
	ClusterUndoSavepointControl *out[3] = { 0 };

	reserve_test_fixture();
	cluster_shared_config = false;
	UT_ASSERT(cluster_undo_record_reserve_savepoint_controls(reserve_test_operations, 3, 100, out));
	UT_ASSERT_EQ(reserve_test_refs, 4);
	UT_ASSERT_EQ(reserve_test_wal, 0);
	UT_ASSERT(cluster_undo_pending.active);
	UT_ASSERT_EQ(cluster_undo_pending.nrecords, 3);
	for (unsigned i = 0; i < 3; i++)
		cluster_undo_record_release_savepoint_control(out[i]);
	UT_ASSERT_EQ(reserve_test_refs, 1);
	UT_ASSERT(cluster_undo_pending_flush_internal(false));
	UT_ASSERT_EQ(reserve_test_refs, 0);
	UT_ASSERT_EQ(reserve_test_wal, 1);
	UT_ASSERT_EQ(reserve_test_wal_slots[0], 24);
	UT_ASSERT(!cluster_undo_pending.active);
	reserve_test_done();
}
