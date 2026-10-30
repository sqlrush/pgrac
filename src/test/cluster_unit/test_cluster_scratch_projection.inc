/* Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors.
 */
/* The holder and consumer are production code. The independently typed
 * origin outcome remains a fixture boundary until TT40 integration. */
UT_TEST(test_peer_projection_full_reaches_real_tuple_consumer)
{
	HeapTupleData tuple = { 0 };
	SnapshotData snapshot = { 0 };
	ClusterR4HotScratchTestContext context = { 0 };
	Page page = (Page)ut_visibility_page.data;
	PGAlignedBlock before;
	volatile bool caught = false, visible = false;

	{
		FILE *producer = popen("./test_cluster_r4_tx_outcome --peer-full-proof-fixture | "
							   "./test_cluster_r4_slot_reservation --peer-full-frame-fixture | "
							   "./test_cluster_r4_route_policy --peer-full-land-fixture",
							   "r");
		bool received = producer != NULL
						&& fread(&self_owner_full, sizeof(self_owner_full), 1, producer) == 1;
		int rc = producer != NULL ? pclose(producer) : -1;
		UT_ASSERT(received && rc == 0);
		if (!received || rc != 0)
			return;
	}
	self_child = true;
	self_reset_case(false, 0);
	self_binding_reads = 0;
	self_current = false;
	self_owner_full_pipe = self_peer_full_pipe = true;
	cluster_node_id = 2;
	memcpy(page, self_owner_full.full.data, BLCKSZ);
	memcpy(before.data, page, BLCKSZ);
	tuple.t_data = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 3));
	tuple.t_len = ItemIdGetLength(PageGetItemId(page, 3));
	tuple.t_self = tuple.t_data->t_ctid;
	tuple.t_tableOid = FirstNormalObjectId;
	snapshot.snapshot_type = SNAPSHOT_MVCC;
	snapshot.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
	snapshot.read_epoch = ClusterR4OperationRead(self_owner_full.projection.wire.bytes + 52, 8);
	snapshot.read_scn = self_owner_full.read_scn;
	context.scratch_page = page;
	context.already_full = true;
	context.read_scn = snapshot.read_scn;
	context.logical_root = tuple.t_self;
	context.tag = self_owner_full.tag;
#ifdef CLUSTER_R4_PROJECTION_PRESENT
	context.projection = &self_owner_full.projection;
#endif
	ut_error_armed = true;
	PG_TRY();
	{
		visible = HeapTupleSatisfiesMVCCScratch(&tuple, &snapshot, &context);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	ut_error_armed = false;
	if (caught)
		printf("# typed peer FULL consumer refused: %s\n", ut_error_message);
	UT_ASSERT(!caught && visible);
	UT_ASSERT_EQ(ut_origin_asks, 0);
	UT_ASSERT_EQ(ut_calls.wire, 0);
	UT_ASSERT_EQ(self_operation_calls, 0);
	UT_ASSERT_EQ(self_binding_reads, 0);
	UT_ASSERT_EQ(self_cid_calls, 0);
	UT_ASSERT_EQ(memcmp(before.data, page, BLCKSZ), 0);
	UT_ASSERT(PG_exception_stack == NULL && ut_snapshot_scope == NULL);
	self_owner_full_pipe = self_peer_full_pipe = false;
	self_child = false;
	cluster_node_id = UT_SELF_NODE;
}

/* Each corruption is evaluated by the real whole-page guard before any
 * tuple status or requester raw-history fallback can be consumed. */
UT_TEST(test_peer_projection_whole_page_rejects_incomplete_or_changed_receipts)
{
	TestOwnerFullFixture saved = self_owner_full;
	for (unsigned fault = 0; fault < 16; fault++) {
		HeapTupleData tuple = { 0 };
		SnapshotData snapshot = { 0 };
		ClusterR4HotScratchTestContext context = { 0 };
		ClusterR4ProjectionBinding binding;
		ClusterR4ProjectionEntry entry;
		volatile bool caught = false;
		unsigned before = ut_vis_unknown_count;
		unsigned count;
		self_owner_full = saved;
		cluster_node_id = 2;
		memcpy(ut_visibility_page.data, saved.full.data, BLCKSZ);
		context.scratch_page = ut_visibility_page.data;
		context.already_full = true;
		context.read_scn = saved.read_scn;
		context.tag = saved.tag;
		context.projection = &self_owner_full.projection;
		snapshot.snapshot_type = SNAPSHOT_MVCC;
		snapshot.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
		snapshot.read_scn = saved.read_scn;
		snapshot.read_epoch = 9;
		tuple.t_data = (HeapTupleHeader)PageGetItem(context.scratch_page,
													PageGetItemId(context.scratch_page, 3));
		tuple.t_len = ItemIdGetLength(PageGetItemId(context.scratch_page, 3));
		tuple.t_self = tuple.t_data->t_ctid;
		context.logical_root = tuple.t_self;
		UT_ASSERT(ClusterR4ProjectionDecode(&saved.projection.wire, saved.projection.reply_header,
											saved.full.data, saved.projection.wire.bytes + 140,
											&binding));
		count = ClusterR4OperationRead(saved.projection.wire.bytes + 8, 2);
		UT_ASSERT(count >= 2);
		if (fault == 0)
			self_owner_full.projection.valid = false;
		if (fault == 1)
			self_owner_full.projection.wire.bytes[12] ^= 1;
		if (fault == 2)
			tuple.t_data->t_choice.t_heap.t_xmin++;
		if (fault == 3)
			context.tag.relNumber++;
		if (fault == 4)
			snapshot.read_scn = ++context.read_scn;
		if (fault == 5)
			snapshot.read_epoch++;
		if (fault == 6)
			binding.requester++;
		if (fault == 7) {
			/* Remove the last receipt while querying an earlier intact row. */
			count--;
		}
		if (fault == 8) {
			UT_ASSERT(!ClusterR4ProjectionSeal(&self_owner_full.projection.wire, 33, &binding,
											   self_owner_full.projection.reply_header,
											   ut_visibility_page.data));
			ClusterR4OperationWrite(self_owner_full.projection.wire.bytes + 8, 2, 33);
			ClusterR4OperationWrite(
				self_owner_full.projection.wire.bytes + 12, 4,
				ClusterR4ProjectionFrameCrc(
					&self_owner_full.projection.wire, self_owner_full.projection.reply_header,
					ut_visibility_page.data, self_owner_full.projection.wire.bytes + 140));
		}
		if (fault == 9)
			ClusterR4ProjectionForget(&self_owner_full.projection);
		if (fault >= 10) {
			UT_ASSERT(ClusterR4ProjectionEntryGet(&self_owner_full.projection.wire, 0, &entry));
			if (fault == 10)
				entry.operation.effect = CLUSTER_SAVEPOINT_EFFECT_DISCARDED;
			if (fault == 11)
				entry.operation.identity.top_fxid = InvalidFullTransactionId;
			if (fault == 12) {
				entry.role = CLUSTER_R4_PROJECTION_XMAX;
				entry.record_type = UNDO_RECORD_UPDATE;
			}
			if (fault == 13)
				entry.operation.admission_generation++;
			if (fault == 14)
				entry.operation.commit_scn = saved.read_scn + 1;
			if (fault == 15)
				entry.tuple_crc++;
			/* Missing full identity is a deliberately malformed wire. */
			if (fault == 11)
				memset(self_owner_full.projection.wire.bytes + CLUSTER_R4_PROJECTION_HEADER_BYTES,
					   0, 96);
			else
				UT_ASSERT(ClusterR4ProjectionEntryPut(&self_owner_full.projection.wire, 0, &entry));
		}
		if (fault == 6 || fault == 7 || fault >= 10)
			UT_ASSERT(ClusterR4ProjectionSeal(&self_owner_full.projection.wire, count, &binding,
											  self_owner_full.projection.reply_header,
											  ut_visibility_page.data));
		ut_error_armed = true;
		PG_TRY();
		{
			(void)HeapTupleSatisfiesMVCCScratch(&tuple, &snapshot, &context);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		ut_error_armed = false;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(ut_error_code, ERRCODE_CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT_EQ(ut_vis_unknown_count, before + 1);
	}
	self_owner_full = saved;
	self_owner_full_pipe = self_peer_full_pipe = false;
	self_child = false;
	cluster_node_id = UT_SELF_NODE;
}

UT_TEST(test_peer_projection_modifier_roles_reach_real_consumer)
{
	for (unsigned kind = 1; kind <= 4; kind++) {
		char command[512];
		FILE *producer;
		TestOwnerFullFixture fixture;
		HeapTupleData tuple = { 0 };
		SnapshotData snapshot = { 0 };
		ClusterR4HotScratchTestContext context = { 0 };
		volatile bool caught = false, visible = true;
		bool received;
		int rc;
		snprintf(command, sizeof(command),
				 "./test_cluster_r4_tx_outcome --peer-full-proof-fixture %u | "
				 "./test_cluster_r4_slot_reservation --peer-full-frame-fixture | "
				 "./test_cluster_r4_route_policy --peer-full-land-fixture",
				 kind);
		producer = popen(command, "r");
		received = producer != NULL && fread(&fixture, sizeof(fixture), 1, producer) == 1;
		rc = producer != NULL ? pclose(producer) : -1;
		UT_ASSERT(received && rc == 0);
		if (!received || rc != 0)
			continue;
		self_child = true;
		self_reset_case(false, 0);
		cluster_node_id = 2;
		self_current = false;
		self_owner_full = fixture;
		self_owner_full_pipe = self_peer_full_pipe = true;
		memcpy(ut_visibility_page.data, fixture.full.data, BLCKSZ);
		context.scratch_page = ut_visibility_page.data;
		context.already_full = true;
		context.read_scn = fixture.read_scn;
		context.tag = fixture.tag;
		context.projection = &fixture.projection;
		snapshot.snapshot_type = SNAPSHOT_MVCC;
		snapshot.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
		snapshot.read_scn = fixture.read_scn;
		snapshot.read_epoch = 9;
		for (OffsetNumber off = 1; off <= PageGetMaxOffsetNumber(context.scratch_page); off++) {
			if (!ItemIdIsNormal(PageGetItemId(context.scratch_page, off)))
				continue;
			tuple.t_data = (HeapTupleHeader)PageGetItem(context.scratch_page,
														PageGetItemId(context.scratch_page, off));
			tuple.t_len = ItemIdGetLength(PageGetItemId(context.scratch_page, off));
			tuple.t_self = tuple.t_data->t_ctid;
			/* Physical tuple identity remains its own offset for projection. */
			ItemPointerSet(&tuple.t_self, fixture.tag.blockNum, off);
			context.logical_root = tuple.t_self;
			ut_error_armed = true;
			PG_TRY();
			{
				visible = HeapTupleSatisfiesMVCCScratch(&tuple, &snapshot, &context);
			}
			PG_CATCH();
			{
				caught = true;
			}
			PG_END_TRY();
			ut_error_armed = false;
			if (caught || (off == 1 ? visible : !visible))
				printf("# modifier kind=%u off=%u caught=%d visible=%d: %s\n", kind, off, caught,
					   visible, ut_error_message);
			UT_ASSERT(!caught && (off == 1 ? !visible : visible));
		}
	}
	self_owner_full_pipe = self_peer_full_pipe = false;
	self_child = false;
	cluster_node_id = UT_SELF_NODE;
}
