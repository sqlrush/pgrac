/* Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors.
 */
/* Real holder FULL bytes -> production resolver/evaluator. The producer's
 * peer TT outcome is typed fixture input, not an integrated TT40 proof. */
extern ClusterUndoVerdictResult cluster_undo_verdict_resolve(int, uint32, TransactionId, uint32,
															 SCN, bool);
extern int cluster_xid_origin_slot(TransactionId);

ClusterUndoVerdictResult
cluster_undo_verdict_resolve(int origin, uint32 segment, TransactionId xid, uint32 slot,
							 SCN read_scn, bool authoritative)
{
	if (self_peer_full_pipe) {
		ClusterUndoTTSlotRef ref;

		ut_calls.wire++;
		ut_origin_asks++;
		UT_ASSERT_EQ(origin, self_owner_full.expected.top.origin_instance - 1);
		UT_ASSERT(self_real_itl_ref(ut_visibility_page.data, 2, &ref));
		UT_ASSERT_EQ(segment, ref.undo_segment_id);
		UT_ASSERT_EQ(xid, XidFromFullTransactionId(self_owner_full.expected.operation_fxid));
		UT_ASSERT_EQ(slot, 0);
		UT_ASSERT_EQ(read_scn, self_owner_full.read_scn);
		UT_ASSERT(!authoritative);
		return self_peer_raw_verdict;
	}
	return self_unused_history(origin, segment, xid, slot, read_scn, authoritative);
}

int
cluster_xid_origin_slot(TransactionId xid)
{
	return self_peer_full_pipe ? self_owner_full.expected.top.origin_instance - 1
							   : self_unused_xid_origin(xid);
}

static bool
peer_full_read(void)
{
	FILE *producer = popen("./test_cluster_r4_tx_outcome --peer-full-fixture", "r");
	bool received;
	int status;

	UT_ASSERT(producer != NULL);
	if (producer == NULL)
		return false;
	received = fread(&self_owner_full, sizeof(self_owner_full), 1, producer) == 1;
	status = pclose(producer);
	UT_ASSERT(received && status == 0 && self_owner_full.magic == TEST_PEER_FULL_MAGIC);
	return received && status == 0 && self_owner_full.magic == TEST_PEER_FULL_MAGIC;
}

/* A raw child terminal answer lacks both canonical top and operation role.
 * It cannot replace an ordered peer receipt even after FULL construction. */
UT_TEST(test_peer_full_raw_child_verdict_never_authorizes_operation)
{
	if (!peer_full_read())
		return;
	for (unsigned answer = 0; answer < 4; answer++) {
		HeapTupleData tuple = { 0 };
		SnapshotData snapshot = { 0 };
		ClusterR4HotScratchTestContext context = { 0 };
		Page page = (Page)ut_visibility_page.data;
		PGAlignedBlock before;
		volatile bool caught = false, visible = false;

		self_child = true;
		self_reset_case(false, 0);
		self_binding_reads = 0;
		self_current = false;
		self_owner_full_pipe = self_peer_full_pipe = true;
		cluster_node_id = 1;
		self_peer_raw_verdict = (ClusterUndoVerdictResult){
			.kind = answer == 0	  ? CLUSTER_UNDO_VERDICT_COMMITTED_EXACT
					: answer == 1 ? CLUSTER_UNDO_VERDICT_ABORTED
					: answer == 2 ? CLUSTER_UNDO_VERDICT_COMMITTED_BOUND
								  : CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED,
			.commit_scn = answer == 0 || answer == 2 ? self_owner_full.read_scn - 1 : InvalidScn
		};
		memcpy(page, self_owner_full.full.data, BLCKSZ);
		memcpy(before.data, page, BLCKSZ);
		tuple.t_data = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 3));
		tuple.t_len = ItemIdGetLength(PageGetItemId(page, 3));
		tuple.t_self = tuple.t_data->t_ctid;
		tuple.t_tableOid = FirstNormalObjectId;
		snapshot.snapshot_type = SNAPSHOT_MVCC;
		snapshot.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
		snapshot.read_epoch = ut_current_epoch;
		snapshot.read_scn = self_owner_full.read_scn;
		context.scratch_page = page;
		context.already_full = true;
		context.read_scn = snapshot.read_scn;
		context.logical_root = tuple.t_self;
		context.tag = self_owner_full.tag;
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
		if (!caught || ut_origin_asks != 0)
			printf("# peer FULL raw answer=%u caught=%d visible=%d origin_asks=%d error=%s\n",
				   answer, caught, visible, ut_origin_asks, ut_error_message);
		UT_ASSERT(caught);
		UT_ASSERT_EQ(ut_origin_asks, 0);
		UT_ASSERT_EQ(ut_error_code, ERRCODE_CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT_EQ(self_cid_calls, 0);
		UT_ASSERT_EQ(self_operation_calls, 0);
		UT_ASSERT_EQ(self_binding_reads, 0);
		UT_ASSERT_EQ(ut_calls.wire, 0);
		UT_ASSERT_EQ(ut_hint_mutations, 0);
		UT_ASSERT_EQ(memcmp(before.data, page, BLCKSZ), 0);
		UT_ASSERT(!cluster_vis_resolve_in_flight());
		UT_ASSERT(PG_exception_stack == NULL && ut_snapshot_scope == NULL);
		self_owner_full_pipe = self_peer_full_pipe = false;
		self_child = false;
		cluster_node_id = UT_SELF_NODE;
	}
}

UT_TEST(test_peer_full_possible_child_refusals_cover_both_tuple_roles)
{
	if (!peer_full_read())
		return;
	for (unsigned which = 0; which < 2; which++) {
		Page page = (Page)ut_visibility_page.data;
		HeapTupleData tuple = { 0 };
		ClusterVisResolve resolved;
		PGAlignedBlock before;

		self_child = true;
		self_reset_case(false, 0);
		self_binding_reads = 0;
		self_current = false;
		self_owner_full_pipe = self_peer_full_pipe = true;
		cluster_node_id = 1;
		memcpy(page, self_owner_full.full.data, BLCKSZ);
		tuple.t_data = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 3));
		tuple.t_len = ItemIdGetLength(PageGetItemId(page, 3));
		tuple.t_self = tuple.t_data->t_ctid;
		HeapTupleHeaderSetXmax(tuple.t_data,
							   XidFromFullTransactionId(self_owner_full.expected.operation_fxid));
		memcpy(before.data, page, BLCKSZ);
		cluster_visibility_resolve_scratch_tuple_scn(page, &tuple, &self_owner_full.tag, 2,
													 which == 0 ? CLUSTER_VIS_XMIN
																: CLUSTER_VIS_XMAX_UPDATE,
													 self_owner_full.read_scn, &resolved);
		UT_ASSERT_EQ(resolved.evidence, CLUSTER_VIS_EVIDENCE_STALE_OR_AMBIGUOUS);
		UT_ASSERT_EQ(resolved.status, CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT_EQ(strcmp(resolved.diagnostic_reason, "PEER_OPERATION_UNPROVABLE"), 0);
		UT_ASSERT_EQ(ut_origin_asks, 0);
		UT_ASSERT_EQ(ut_calls.wire, 0);
		UT_ASSERT_EQ(self_operation_calls, 0);
		UT_ASSERT_EQ(self_binding_reads, 0);
		UT_ASSERT_EQ(self_cid_calls, 0);
		UT_ASSERT_EQ(memcmp(before.data, page, BLCKSZ), 0);
		self_owner_full_pipe = self_peer_full_pipe = false;
		self_child = false;
		cluster_node_id = UT_SELF_NODE;
	}
}
