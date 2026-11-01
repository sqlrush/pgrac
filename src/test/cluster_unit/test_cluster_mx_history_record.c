/* Real record decoders and exact comparisons, without authority stubs. */
int history_unused_reader_main(void);
#define main history_unused_reader_main
#include "test_cluster_savepoint_reader.c"
#undef main
#include "cluster/cluster_mx_history_record.h"

typedef struct HistoryFixture {
	ReaderFixture reader;
	ClusterMxHistoryRequest request;
	PGAlignedBlock data, bytes, fresh_bytes;
	ClusterMxHistoryRecord captured, fresh;
} HistoryFixture;

static bool
history_zero(const void *p, size_t n)
{
	const unsigned char *b = p;
	for (size_t i = 0; i < n; i++)
		if (b[i] != 0)
			return false;
	return true;
}

static void
history_pack(HistoryFixture *f)
{
	UndoBlockHeader h = { .magic = PGRAC_UNDO_BLOCK_MAGIC,
						  .block_version = UNDO_BLOCK_VERSION_1,
						  .slot_count = 1,
						  .free_offset = sizeof(UndoBlockHeader) + sizeof(UndoRecordHeader)
										 + f->reader.record.payload_length };
	UndoSlotDirEntry s
		= { .record_offset = sizeof(h),
			.record_length = sizeof(UndoRecordHeader) + f->reader.record.payload_length,
			.record_type = f->reader.record.record_type,
			.flags = f->reader.record.flags };
	memset(f->data.data, 0, BLCKSZ);
	memcpy(f->data.data, &h, sizeof(h));
	memcpy(f->data.data + s.record_offset, &f->reader.record, sizeof(f->reader.record));
	memcpy(f->data.data + s.record_offset + sizeof(f->reader.record), f->reader.body,
		   f->reader.record.payload_length);
	memcpy(f->data.data + BLCKSZ - sizeof(s), &s, sizeof(s));
}

static void
history_init(HistoryFixture *f)
{
	memset(f, 0, sizeof(*f));
	init_fixture(&f->reader);
	f->reader.operation.effect_state = CLUSTER_SAVEPOINT_EFFECT_LIVE;
	seal_fixture(&f->reader);
	f->request.locator = f->reader.locator;
	f->request.edge_kind = CMXH_ROOT;
	f->request.target_locator = f->reader.record.target_locator;
	f->request.target_fork = MAIN_FORKNUM;
	f->request.target_block = 17;
	f->request.target_offset = 1;
	f->request.edge = *ClusterPageGetItlSlots((Page)f->reader.page.data);
	f->request.root = f->request.edge;
	history_pack(f);
}

static bool
history_capture(HistoryFixture *f)
{
	return cluster_mx_history_record_capture(&f->request, f->data.data, f->bytes.data,
											 &f->captured);
}

static bool
history_recheck(HistoryFixture *f)
{
	return cluster_mx_history_record_recheck(&f->request, &f->captured, f->bytes.data,
											 f->captured.record_length, f->data.data,
											 f->fresh_bytes.data, &f->fresh);
}

static void
history_global(HistoryFixture *f)
{
	f->request.edge_kind = CMXH_GLOBAL_PREV;
	f->request.flags |= CLUSTER_MX_HISTORY_EXPECT_OWNER;
	f->request.expected_owner = (ClusterTTSlotPhysicalLocator){ .segment_id = 1,
																.xid = 100,
																.slot_offset = 2,
																.wrap = 3 };
	f->request.expected_owner_generation = 7;
	f->request.expected_top_fxid = f->reader.expected.top_fxid;
	f->request.locator.itl_kind = 0;
	f->request.locator.itl_slot_index = f->request.edge_itl_slot = 255;
	memset(&f->request.edge, 0, sizeof(f->request.edge));
}

static void
history_v1(HistoryFixture *f)
{
	f->reader.record.flags &= ~UNDO_REC_FLAG_HAS_OPERATION;
	f->reader.record.xid = 100;
	f->reader.record.payload_length -= sizeof(UndoOperationTrailer);
	history_pack(f);
}

static void
history_control(HistoryFixture *f, uint8 phase)
{
	UndoSavepointRollbackPayload c
		= { .version = UNDO_SAVEPOINT_ROLLBACK_VERSION,
			.length = UNDO_SAVEPOINT_ROLLBACK_LENGTH,
			.phase = phase,
			.rollback_sequence = 3,
			.start_uba = f->reader.record.prev_uba,
			.next_uba = f->reader.record.prev_uba,
			.scope_child_fxid = U64FromFullTransactionId(f->reader.expected.operation_fxid) };
	history_global(f);
	f->reader.record.record_type = UNDO_RECORD_SAVEPOINT_ROLLBACK;
	f->reader.record.flags = UNDO_REC_FLAG_HAS_OPERATION;
	memset(&f->reader.record.target_locator, 0, sizeof(RelFileLocator));
	f->reader.record.target_fork = f->reader.record.target_block = f->reader.record.target_offset
		= 0;
	f->reader.record.payload_length = sizeof(c) + sizeof(f->reader.operation);
	memcpy(f->reader.body, &c, sizeof(c));
	memcpy(f->reader.body + sizeof(c), &f->reader.operation, sizeof(f->reader.operation));
	history_pack(f);
}

static ClusterTxResolution
history_top(HistoryFixture *f)
{
	ClusterTxResolution top = { 0 };
	top.locator_echo = f->request.locator;
	top.top_xid = 100;
	top.outcome = CLUSTER_TX_COMMITTED;
	top.commit_scn = scn_encode(0, 2000);
	top.proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
	top.local_canonical_valid = true;
	top.local_canonical = (ClusterTTSlotPhysicalLocator){ .segment_id = 1,
														  .xid = 100,
														  .slot_offset = 2,
														  .wrap = 3 };
	top.local_canonical_generation = 7;
	top.local_original_full_xid = f->reader.expected.top_fxid;
	return top;
}

UT_TEST(history_root_operation_and_prior)
{
	HistoryFixture f;
	history_init(&f);
	UT_ASSERT(history_capture(&f));
	UT_ASSERT_EQ(f.captured.record_class, CMXH_OPERATION);
	UT_ASSERT(f.captured.matches_target);
	UT_ASSERT_EQ(f.captured.identity.operation_sequence, 19);
	f.request.edge_kind = CMXH_PAGE_PRIOR;
	UT_ASSERT(history_capture(&f));
	f.request.edge.write_scn++;
	UT_ASSERT(!history_capture(&f));
	UT_ASSERT(history_zero(&f.captured, sizeof(f.captured)));
	UT_ASSERT(history_zero(f.bytes.data, BLCKSZ));
}

UT_TEST(history_global_keeps_non_target_record)
{
	HistoryFixture f;
	history_init(&f);
	history_global(&f);
	f.request.target_offset = 2;
	UT_ASSERT(history_capture(&f));
	UT_ASSERT(!f.captured.matches_target);
	UT_ASSERT_EQ(f.captured.header.prev_uba.raw[0], f.reader.record.prev_uba.raw[0]);
	f.request.expected_owner_generation++;
	UT_ASSERT(!history_capture(&f));
}

UT_TEST(history_physical_binding_and_slot_directory)
{
	HistoryFixture f;
	for (int i = 0; i < 6; i++) {
		history_init(&f);
		if (i == 0)
			f.reader.record.tt_slot_id++;
		if (i == 1)
			f.reader.record.tt_slot_segment_id = CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
		if (i == 2)
			f.reader.record.origin_node_id++;
		if (i == 3)
			f.reader.record.tt_wrap_plus1++;
		if (i == 4)
			f.reader.record.prev_uba = f.reader.current;
		history_pack(&f);
		if (i == 5)
			((UndoSlotDirEntry *)(f.data.data + BLCKSZ - sizeof(UndoSlotDirEntry)))
				->record_length++;
		UT_ASSERT(!history_capture(&f));
		UT_ASSERT(history_zero(&f.captured, sizeof(f.captured)));
	}
}

UT_TEST(history_v1_requires_independent_original_full_xid)
{
	HistoryFixture f;
	ClusterSavepointReaderIdentity identity;
	ClusterTxResolution top;
	history_init(&f);
	history_v1(&f);
	UT_ASSERT(history_capture(&f));
	UT_ASSERT_EQ(f.captured.record_class, CMXH_V1_TOP);
	UT_ASSERT_EQ(U64FromFullTransactionId(f.captured.identity.top_fxid), 0);
	UT_ASSERT(!f.captured.canonical_generation_known);
	top = history_top(&f);
	UT_ASSERT(cluster_mx_history_record_bind_top(&f.request, &f.captured, &top, &identity));
	UT_ASSERT(FullTransactionIdEquals(identity.top_fxid, top.local_original_full_xid));
	UT_ASSERT(FullTransactionIdEquals(identity.operation_fxid, identity.top_fxid));
	UT_ASSERT_EQ(identity.operation_sequence, 0);
	top.local_original_full_xid = InvalidFullTransactionId;
	UT_ASSERT(!cluster_mx_history_record_bind_top(&f.request, &f.captured, &top, &identity));
	UT_ASSERT(history_zero(&identity, sizeof(identity)));
}

UT_TEST(history_operation_tail_never_proves_its_own_epoch)
{
	HistoryFixture f;
	ClusterSavepointReaderIdentity identity;
	ClusterTxResolution top;
	history_init(&f);
	UT_ASSERT(history_capture(&f));
	top = history_top(&f);
	UT_ASSERT(cluster_mx_history_record_bind_top(&f.request, &f.captured, &top, &identity));
	for (int i = 0; i < 5; i++) {
		top = history_top(&f);
		if (i == 0)
			top.local_original_full_xid = InvalidFullTransactionId;
		if (i == 1)
			top.local_original_full_xid = FullTransactionIdFromEpochAndXid(2, 100);
		if (i == 2)
			top.local_canonical_generation++;
		if (i == 3)
			top.local_canonical.wrap++;
		if (i == 4)
			top.proof_kind = CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP;
		UT_ASSERT(!cluster_mx_history_record_bind_top(&f.request, &f.captured, &top, &identity));
	}
}

UT_TEST(history_fresh_effect_is_one_way)
{
	HistoryFixture f;
	history_init(&f);
	UT_ASSERT(history_capture(&f));
	f.reader.operation.effect_state = CLUSTER_SAVEPOINT_EFFECT_DISCARDED;
	seal_fixture(&f.reader);
	history_pack(&f);
	UT_ASSERT(history_recheck(&f));
	UT_ASSERT_EQ(f.fresh.effect, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
	f.captured = f.fresh;
	memcpy(f.bytes.data, f.fresh_bytes.data, BLCKSZ);
	f.reader.operation.effect_state = CLUSTER_SAVEPOINT_EFFECT_LIVE;
	seal_fixture(&f.reader);
	history_pack(&f);
	UT_ASSERT(!history_recheck(&f));
	UT_ASSERT(history_zero(&f.fresh, sizeof(f.fresh)));
}

UT_TEST(history_fresh_identity_and_v1_bytes_immutable)
{
	HistoryFixture f;
	history_init(&f);
	UT_ASSERT(history_capture(&f));
	f.reader.operation.command_id++;
	seal_fixture(&f.reader);
	history_pack(&f);
	UT_ASSERT(!history_recheck(&f));
	history_init(&f);
	history_v1(&f);
	UT_ASSERT(history_capture(&f));
	UT_ASSERT(history_recheck(&f));
	f.reader.record.target_offset++;
	history_pack(&f);
	UT_ASSERT(!history_recheck(&f));
}

UT_TEST(history_control_progress_is_snapshot_only)
{
	HistoryFixture f;
	UndoSavepointRollbackPayload c;
	history_init(&f);
	history_control(&f, UNDO_SAVEPOINT_ROLLBACK_PENDING);
	UT_ASSERT(history_capture(&f));
	UT_ASSERT_EQ(f.captured.record_class, CMXH_ROLLBACK_CONTROL);
	UT_ASSERT_EQ(f.captured.effect, 0);
	UT_ASSERT(!f.captured.matches_target);
	memcpy(&c, f.reader.body, sizeof(c));
	c.phase = UNDO_SAVEPOINT_ROLLBACK_COMPLETE;
	c.next_uba = c.stop_uba;
	c.visited_count = 1;
	c.last_effect_wal_lsn = 100;
	memcpy(f.reader.body, &c, sizeof(c));
	history_pack(&f);
	UT_ASSERT(history_recheck(&f));
	UT_ASSERT_EQ(f.fresh.control.phase, UNDO_SAVEPOINT_ROLLBACK_COMPLETE);
}

UT_TEST(history_control_regression_and_reserved_refused)
{
	HistoryFixture f;
	UndoSavepointRollbackPayload c;
	history_init(&f);
	history_control(&f, UNDO_SAVEPOINT_ROLLBACK_MARKING);
	memcpy(&c, f.reader.body, sizeof(c));
	c.next_uba = uba_encode(1, 5, 2, 0);
	c.visited_count = 2;
	c.last_effect_wal_lsn = 101;
	memcpy(f.reader.body, &c, sizeof(c));
	history_pack(&f);
	UT_ASSERT(history_capture(&f));
	c.visited_count = 1;
	c.last_effect_wal_lsn = 100;
	memcpy(f.reader.body, &c, sizeof(c));
	history_pack(&f);
	UT_ASSERT(!history_recheck(&f));
	history_init(&f);
	history_control(&f, UNDO_SAVEPOINT_ROLLBACK_RESERVED);
	UT_ASSERT(!history_capture(&f));
}

UT_TEST(history_generation_zero_and_v1_owner_challenge)
{
	HistoryFixture f;
	ClusterTxResolution top;
	ClusterSavepointReaderIdentity identity;
	history_init(&f);
	history_global(&f);
	f.request.expected_owner_generation = f.reader.operation.canonical_segment_generation = 0;
	seal_fixture(&f.reader);
	history_pack(&f);
	UT_ASSERT(history_capture(&f));
	UT_ASSERT(f.captured.canonical_generation_known);
	top = history_top(&f);
	top.local_canonical_generation = 0;
	UT_ASSERT(cluster_mx_history_record_bind_top(&f.request, &f.captured, &top, &identity));
	history_v1(&f);
	UT_ASSERT(history_capture(&f));
	UT_ASSERT(f.captured.canonical_generation_known);
	UT_ASSERT(cluster_mx_history_record_bind_top(&f.request, &f.captured, &top, &identity));
	top.local_canonical_generation = 1;
	UT_ASSERT(!cluster_mx_history_record_bind_top(&f.request, &f.captured, &top, &identity));
	top.local_canonical_generation = 0;
	top.local_original_full_xid = FullTransactionIdFromEpochAndXid(2, 100);
	UT_ASSERT(!cluster_mx_history_record_bind_top(&f.request, &f.captured, &top, &identity));
}

UT_TEST(history_update_new_target_and_unrelated_global)
{
	HistoryFixture f;
	history_init(&f);
	make_update(&f.reader, true);
	f.reader.operation.effect_state = CLUSTER_SAVEPOINT_EFFECT_LIVE;
	seal_fixture(&f.reader);
	history_pack(&f);
	f.request.target_block = 18;
	f.request.target_offset = 2;
	UT_ASSERT(history_capture(&f));
	UT_ASSERT(f.captured.matches_target);
	history_global(&f);
	f.request.target_block = 19;
	UT_ASSERT(history_capture(&f));
	UT_ASSERT(!f.captured.matches_target);
}

UT_TEST(history_fresh_relocated_slot_and_bounded_first_bytes)
{
	HistoryFixture f;
	UndoSlotDirEntry *slot;
	UndoBlockHeader *h;
	history_init(&f);
	UT_ASSERT(history_capture(&f));
	slot = (UndoSlotDirEntry *)(f.data.data + BLCKSZ - sizeof(*slot));
	h = (UndoBlockHeader *)f.data.data;
	memmove(f.data.data + slot->record_offset + 8, f.data.data + slot->record_offset,
			slot->record_length);
	slot->record_offset += 8;
	h->free_offset += 8;
	UT_ASSERT(!history_recheck(&f));
	UT_ASSERT(history_zero(f.fresh_bytes.data, BLCKSZ));
	UT_ASSERT(!cluster_mx_history_record_recheck(&f.request, &f.captured, f.bytes.data,
												 sizeof(UndoRecordHeader) - 1, f.data.data,
												 f.fresh_bytes.data, &f.fresh));
	UT_ASSERT(!cluster_mx_history_record_capture(NULL, f.data.data, f.fresh_bytes.data, &f.fresh));
	UT_ASSERT(history_zero(&f.fresh, sizeof(f.fresh)));
}

UT_TEST(history_control_mutable_fields_and_terminal_freeze)
{
	HistoryFixture f;
	UndoSavepointRollbackPayload c;
	for (int i = 0; i < 7; i++) {
		history_init(&f);
		history_control(&f, UNDO_SAVEPOINT_ROLLBACK_MARKING);
		memcpy(&c, f.reader.body, sizeof(c));
		c.next_uba = uba_encode(1, 5, 2, 0);
		c.visited_count = 2;
		c.last_effect_wal_lsn = 101;
		memcpy(f.reader.body, &c, sizeof(c));
		history_pack(&f);
		UT_ASSERT(history_capture(&f));
		if (i == 0) {
			c.visited_count--;
			c.last_effect_wal_lsn--;
		}
		if (i == 1)
			c.next_uba = uba_encode(1, 4, 2, 0);
		if (i == 2)
			c.last_effect_wal_lsn++;
		if (i == 3)
			c.rollback_sequence++;
		if (i == 4)
			c.phase = UNDO_SAVEPOINT_ROLLBACK_PENDING;
		if (i == 5) {
			f.reader.operation.operation_sequence++;
			memcpy(f.reader.body + sizeof(c), &f.reader.operation, sizeof(f.reader.operation));
		}
		if (i == 6) {
			c.scope_child_fxid++;
			f.reader.operation.operation_fxid++;
			f.reader.record.xid++;
			memcpy(f.reader.body + sizeof(c), &f.reader.operation, sizeof(f.reader.operation));
		}
		memcpy(f.reader.body, &c, sizeof(c));
		history_pack(&f);
		UT_ASSERT(!history_recheck(&f));
	}
	history_init(&f);
	history_control(&f, UNDO_SAVEPOINT_ROLLBACK_PENDING);
	memcpy(&c, f.reader.body, sizeof(c));
	c.phase = UNDO_SAVEPOINT_ROLLBACK_COMPLETE;
	c.next_uba = c.stop_uba;
	c.visited_count = 1;
	c.last_effect_wal_lsn = 100;
	memcpy(f.reader.body, &c, sizeof(c));
	history_pack(&f);
	UT_ASSERT(history_capture(&f));
	UT_ASSERT(history_recheck(&f));
	c.visited_count++;
	c.last_effect_wal_lsn++;
	memcpy(f.reader.body, &c, sizeof(c));
	history_pack(&f);
	UT_ASSERT(!history_recheck(&f));
}

UT_TEST(history_valid_marking_progress_and_active_stays_active)
{
	HistoryFixture f;
	UndoSavepointRollbackPayload c;
	ClusterTxResolution top;
	ClusterSavepointReaderIdentity identity;
	history_init(&f);
	history_control(&f, UNDO_SAVEPOINT_ROLLBACK_MARKING);
	UT_ASSERT(history_capture(&f));
	memcpy(&c, f.reader.body, sizeof(c));
	c.visited_count = 1;
	c.last_effect_wal_lsn = 100;
	c.next_uba = uba_encode(1, 5, 2, 0);
	memcpy(f.reader.body, &c, sizeof(c));
	history_pack(&f);
	UT_ASSERT(history_recheck(&f));
	top = history_top(&f);
	top.outcome = CLUSTER_TX_IN_PROGRESS;
	top.commit_scn = InvalidScn;
	UT_ASSERT(cluster_mx_history_record_bind_top(&f.request, &f.fresh, &top, &identity));
	UT_ASSERT_EQ(top.outcome, CLUSTER_TX_IN_PROGRESS);
	UT_ASSERT_EQ(f.fresh.effect, 0);
}

UT_TEST(history_control_count_needs_new_cursor_and_effect_wal)
{
	HistoryFixture f;
	UndoSavepointRollbackPayload c;
	for (int i = 0; i < 3; i++) {
		history_init(&f);
		history_control(&f, UNDO_SAVEPOINT_ROLLBACK_MARKING);
		memcpy(&c, f.reader.body, sizeof(c));
		c.visited_count = 2;
		c.last_effect_wal_lsn = 101;
		c.next_uba = uba_encode(1, 5, 2, 0);
		memcpy(f.reader.body, &c, sizeof(c));
		history_pack(&f);
		UT_ASSERT(history_capture(&f));
		c.visited_count++;
		if (i == 1)
			c.next_uba = uba_encode(1, 4, 2, 0);
		if (i == 2)
			c.last_effect_wal_lsn++;
		memcpy(f.reader.body, &c, sizeof(c));
		history_pack(&f);
		UT_ASSERT(!history_recheck(&f));
	}
}

UT_TEST(history_unknown_request_wrap_still_binds_exact_tt_echo)
{
	HistoryFixture f;
	ClusterTxResolution top;
	ClusterSavepointReaderIdentity identity;
	history_init(&f);
	f.request.locator.tt_wrap = TT_WRAP_INVALID;
	UT_ASSERT(history_capture(&f));
	top = history_top(&f);
	top.locator_echo.tt_wrap = 3;
	UT_ASSERT(cluster_mx_history_record_bind_top(&f.request, &f.captured, &top, &identity));
	top.locator_echo.tt_wrap++;
	UT_ASSERT(!cluster_mx_history_record_bind_top(&f.request, &f.captured, &top, &identity));
}

int
main(int argc pg_attribute_unused(), char **argv pg_attribute_unused())
{
	UT_PLAN(16);
	printf("# history record view: %zu bytes\n", sizeof(ClusterMxHistoryRecord));
	UT_RUN(history_root_operation_and_prior);
	UT_RUN(history_global_keeps_non_target_record);
	UT_RUN(history_physical_binding_and_slot_directory);
	UT_RUN(history_v1_requires_independent_original_full_xid);
	UT_RUN(history_operation_tail_never_proves_its_own_epoch);
	UT_RUN(history_fresh_effect_is_one_way);
	UT_RUN(history_fresh_identity_and_v1_bytes_immutable);
	UT_RUN(history_control_progress_is_snapshot_only);
	UT_RUN(history_control_regression_and_reserved_refused);
	UT_RUN(history_generation_zero_and_v1_owner_challenge);
	UT_RUN(history_update_new_target_and_unrelated_global);
	UT_RUN(history_fresh_relocated_slot_and_bounded_first_bytes);
	UT_RUN(history_control_mutable_fields_and_terminal_freeze);
	UT_RUN(history_valid_marking_progress_and_active_stays_active);
	UT_RUN(history_control_count_needs_new_cursor_and_effect_wal);
	UT_RUN(history_unknown_request_wrap_still_binds_exact_tt_echo);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
