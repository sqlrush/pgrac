/*-------------------------------------------------------------------------
 *
 * test_cluster_scratch_nonowner.inc
 *   Exact local non-owner operation proofs for FULL scratch tuples.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_scratch_nonowner.inc
 *
 * NOTES
 *   Pgrac-original tests; origin I/O/publication/native leaves are doubled.
 *   Tx admission, decoder, resolver and FULL evaluator are production bodies.
 *-------------------------------------------------------------------------
 */
static ClusterTxOutcome nonowner_outcome;
static ClusterTxOutcome nonowner_later_outcome;
static int nonowner_defect;
static int nonowner_cases;

extern ClusterTxOutcome cluster_runtime_visibility_resolve_retained_operation_head_origin_admitted(
	const ClusterTxLocator *locator, const ClusterItlSlotData *head, const BufferTag *tag,
	const ClusterSavepointReaderIdentity *retained, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission, ClusterSavepointReaderIdentity *identity,
	ClusterTxResolution *top, ClusterSavepointReaderRecord *operation, char data[BLCKSZ],
	ClusterTxResolveReason *reason);

ClusterTxOutcome
cluster_runtime_visibility_resolve_retained_operation_head_origin_admitted(
	const ClusterTxLocator *locator, const ClusterItlSlotData *head, const BufferTag *tag,
	const ClusterSavepointReaderIdentity *retained, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission, ClusterSavepointReaderIdentity *identity,
	ClusterTxResolution *top, ClusterSavepointReaderRecord *operation, char data[BLCKSZ],
	ClusterTxResolveReason *reason)
{
	PGAlignedBlock record;
	ClusterTxLocator exact = *locator;
	ClusterTxLocator sampled = *locator;
	HeapTupleHeader tuple = (HeapTupleHeader)(ut_visibility_page.data + 1024);
	size_t length;

	self_operation_calls++;
	UT_ASSERT(!self_current && !self_binding_present);
	UT_ASSERT(self_admission_held && admission->entered);
	UT_ASSERT_EQ(mode, CLUSTER_TX_RESOLVE_VISIBILITY);
	UT_ASSERT_EQ(retained == NULL, self_operation_calls == 1);
	if (self_operation_defect == 18
		|| (self_operation_calls == 2 && self_history_defect == SELF_HISTORY_ERROR))
		pg_re_throw();
	if (self_history_records != 0)
		self_history_copy(locator, head, data);
	else {
		self_operation_record(data, locator, head, tag,
							  (tuple->t_infomask & HEAP_XMAX_INVALID) != 0);
		if (self_update_layout != 0)
			self_update_record(data, (tuple->t_infomask & HEAP_XMAX_INVALID) != 0);
	}
	if (retained != NULL) {
		UT_ASSERT_EQ(retained->operation_sequence, 0);
		UT_ASSERT_EQ(retained->command_id, InvalidCommandId);
		UT_ASSERT_EQ(retained->subid, TopSubTransactionId);
		UT_ASSERT(FullTransactionIdEquals(retained->top_fxid, retained->operation_fxid));
		if (!cluster_savepoint_reader_discover_retained_head(retained, head, tag, locator, data,
															 identity))
			return CLUSTER_TX_UNKNOWN;
	} else if (!cluster_savepoint_reader_discover_head(head, tag, locator, data, identity))
		return CLUSTER_TX_UNKNOWN;
	exact.tt_wrap = identity->canonical.wrap;
	if (!cluster_savepoint_reader_extract_identity(identity, &exact, data, record.data, &length,
												   operation))
		return CLUSTER_TX_UNKNOWN;
	if (self_history_records != 0) {
		sampled.uba = self_history_heads[0].undo_segment_head;
		sampled.tt_wrap = TT_WRAP_INVALID;
	}
	(void)cluster_tx_resolve_exact(&sampled, mode, top, reason);
	top->locator_echo = exact;
	top->outcome = self_operation_calls == 1 ? nonowner_outcome : nonowner_later_outcome;
	top->commit_scn = top->outcome == CLUSTER_TX_COMMITTED ? UT_READ_SCN - 1 : InvalidScn;
	if (nonowner_defect == 1)
		top->commit_scn = UT_READ_SCN + 1;
	if (nonowner_defect == 2 || self_operation_defect == 13)
		top->local_canonical_generation++;
	if (nonowner_defect == 3)
		top->authority.origin_epoch++;
	if (nonowner_defect == 4)
		top->locator_echo.uba.raw[0]++;
	if (nonowner_defect == 5 && self_operation_calls == 2)
		top->commit_scn++;
	if (nonowner_defect == 6)
		top->local_canonical_valid = false;
	if (nonowner_defect == 7)
		ut_current_epoch++;
	return top->outcome;
}

static void
nonowner_case(bool creator, ClusterTxOutcome first, ClusterTxOutcome later, bool visible_expected,
			  bool error_expected, int calls_expected)
{
	HeapTupleData tuple = { 0 };
	SnapshotData snapshot = { 0 };
	ClusterR4HotScratchTestContext context = { 0 };
	PGAlignedBlock before;
	volatile bool caught = false;
	volatile bool visible = false;

	nonowner_cases++;
	self_child = true;
	self_reset_case(false, 0);
	self_setup_tuple(creator, false, false, 0, &tuple, &context.tag);
	self_current = false;
	self_binding_present = false;
	self_binding_reads = 0;
	nonowner_outcome = first;
	nonowner_later_outcome = later;
	/* Deliberately unusable native CID: a non-owner must not consult it. */
	tuple.t_data->t_choice.t_heap.t_field3.t_cid = InvalidCommandId;
	snapshot.snapshot_type = SNAPSHOT_MVCC;
	snapshot.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
	snapshot.read_epoch = UT_CLUSTER_EPOCH;
	snapshot.read_scn = UT_READ_SCN;
	snapshot.curcid = 0;
	context.scratch_page = ut_visibility_page.data;
	context.already_full = true;
	context.read_scn = snapshot.read_scn;
	context.logical_root = tuple.t_self;
	if (self_history_records != 0)
		self_history_build(creator, &context.tag);
	memcpy(before.data, ut_visibility_page.data, BLCKSZ);
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
	if (caught != error_expected || (!caught && visible != visible_expected)
		|| self_operation_calls != calls_expected)
		printf("# nonowner creator=%d layout=%d records=%d mixed=%d defect=%d/%d/%d "
			   "outcome=%d/%d calls=%d/%d caught=%d/%d visible=%d/%d\n",
			   creator, self_update_layout, self_history_records, self_mixed_mode, nonowner_defect,
			   self_operation_defect, self_history_defect, first, later, self_operation_calls,
			   calls_expected, caught, error_expected, visible, visible_expected);
	UT_ASSERT_EQ(caught, error_expected);
	if (!caught)
		UT_ASSERT_EQ(visible, visible_expected);
	UT_ASSERT_EQ(self_operation_calls, calls_expected);
	UT_ASSERT_EQ(self_admission_leaves, calls_expected);
	UT_ASSERT_EQ(self_admission_enters, self_operation_defect == 17 ? 0
										: self_operation_defect == 15 || self_operation_defect == 16
											? 1
											: calls_expected);
	UT_ASSERT(!self_admission_held);
	UT_ASSERT_EQ(self_binding_reads, 0);
	UT_ASSERT_EQ(self_cid_calls, 0);
	UT_ASSERT_EQ(ut_calls.clog, 0);
	UT_ASSERT_EQ(ut_calls.wire, 0);
	UT_ASSERT_EQ(ut_hint_mutations, 0);
	UT_ASSERT_EQ(memcmp(before.data, ut_visibility_page.data, BLCKSZ), 0);
	UT_ASSERT(!cluster_vis_resolve_in_flight());
	UT_ASSERT(PG_exception_stack == NULL && ut_snapshot_scope == NULL);
	self_child = false;
}

UT_TEST(test_nonowner_live_child_outcomes_and_roles)
{
	for (self_update_layout = 0; self_update_layout <= 2; self_update_layout++)
		for (int creator = 0; creator < 2; creator++) {
			nonowner_case(creator, CLUSTER_TX_COMMITTED, CLUSTER_TX_COMMITTED, creator, false, 1);
			nonowner_case(creator, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_IN_PROGRESS, !creator, false,
						  1);
			nonowner_case(creator, CLUSTER_TX_ABORTED, CLUSTER_TX_ABORTED, !creator, false, 1);
			nonowner_defect = 1;
			nonowner_case(creator, CLUSTER_TX_COMMITTED, CLUSTER_TX_COMMITTED, !creator, false, 1);
			nonowner_defect = 0;
		}
	self_update_layout = 0;
}
UT_TEST(test_nonowner_history_mixed_live_roles)
{
	for (int length = 3; length <= 24; length += 21) {
		self_history_records = length;
		for (self_mixed_mode = 0; self_mixed_mode <= 3; self_mixed_mode++) {
			if (self_mixed_mode == 2 || self_mixed_mode == 3)
				continue; /* initial V1 has no independently qualified top identity */
			for (int cross = 0; cross < 2; cross++) {
				self_history_cross_page = cross;
				for (self_update_layout = 0; self_update_layout <= 2; self_update_layout++)
					for (int creator = 0; creator < 2; creator++)
						nonowner_case(creator, CLUSTER_TX_COMMITTED, CLUSTER_TX_COMMITTED, creator,
									  false, length);
			}
		}
	}
	self_history_records = self_update_layout = self_mixed_mode = 0;
	self_history_cross_page = false;
}
UT_TEST(test_nonowner_unknown_identity_discard_and_admission_refuse)
{
	for (int creator = 0; creator < 2; creator++) {
		for (nonowner_defect = 2; nonowner_defect <= 7; nonowner_defect++) {
			if (nonowner_defect == 5)
				continue;
			nonowner_case(creator, CLUSTER_TX_COMMITTED, CLUSTER_TX_COMMITTED, false, true, 1);
		}
		nonowner_defect = 0;
		nonowner_case(creator, CLUSTER_TX_UNKNOWN, CLUSTER_TX_UNKNOWN, false, true, 1);
		nonowner_case(creator, CLUSTER_TX_PREPARED, CLUSTER_TX_PREPARED, false, true, 1);
		for (self_operation_defect = 1; self_operation_defect <= 18; self_operation_defect++) {
			if (self_operation_defect == 3 || self_operation_defect == 5
				|| (self_operation_defect >= 6 && self_operation_defect <= 9))
				continue;
			nonowner_case(creator, CLUSTER_TX_COMMITTED, CLUSTER_TX_COMMITTED, false, true,
						  self_operation_defect >= 15 && self_operation_defect <= 17 ? 0 : 1);
		}
		self_operation_defect = 0;
	}
}
UT_TEST(test_nonowner_history_identity_and_terminal_stability)
{
	self_history_records = 3;
	for (int creator = 0; creator < 2; creator++) {
		for (self_history_defect = SELF_HISTORY_SEQUENCE;
			 self_history_defect <= SELF_HISTORY_WRITER_ERA; self_history_defect++)
			nonowner_case(creator, CLUSTER_TX_COMMITTED, CLUSTER_TX_COMMITTED, false, true,
						  self_history_defect <= SELF_HISTORY_GENERATION ? 2 : 3);
		self_history_defect = SELF_HISTORY_OK;
		nonowner_case(creator, CLUSTER_TX_COMMITTED, CLUSTER_TX_IN_PROGRESS, false, true, 2);
		nonowner_case(creator, CLUSTER_TX_ABORTED, CLUSTER_TX_COMMITTED, false, true, 2);
		nonowner_defect = 5;
		nonowner_case(creator, CLUSTER_TX_COMMITTED, CLUSTER_TX_COMMITTED, false, true, 2);
		nonowner_defect = 0;
		nonowner_case(creator, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_COMMITTED, !creator, false, 3);
		self_history_defect = SELF_HISTORY_ERROR;
		nonowner_case(creator, CLUSTER_TX_COMMITTED, CLUSTER_TX_COMMITTED, false, true, 2);
		self_history_defect = SELF_HISTORY_OK;
	}
	self_history_records = 0;
}

UT_TEST(test_nonowner_initial_v1_never_fabricates_owner)
{
	self_history_records = 3;
	for (self_mixed_mode = 2; self_mixed_mode <= 3; self_mixed_mode++)
		for (int creator = 0; creator < 2; creator++)
			nonowner_case(creator, CLUSTER_TX_COMMITTED, CLUSTER_TX_COMMITTED, false, true, 1);
	self_history_records = self_mixed_mode = 0;
}

UT_TEST(test_nonowner_page_prior_and_matched_bad_image_refuse)
{
	self_history_records = 3;
	for (int cross = 0; cross < 2; cross++) {
		self_history_cross_page = cross;
		for (self_history_defect = SELF_HISTORY_PRIOR_SELF;
			 self_history_defect <= SELF_HISTORY_PRIOR_SCN; self_history_defect++)
			for (int creator = 0; creator < 2; creator++)
				nonowner_case(creator, CLUSTER_TX_COMMITTED, CLUSTER_TX_COMMITTED, false, true,
							  self_history_defect == SELF_HISTORY_PRIOR_SCN ? 2 : 1);
		self_history_defect = SELF_HISTORY_OLD_XMIN;
		nonowner_case(false, CLUSTER_TX_COMMITTED, CLUSTER_TX_COMMITTED, false, true, 1);
		self_update_layout = 1;
		nonowner_case(false, CLUSTER_TX_COMMITTED, CLUSTER_TX_COMMITTED, false, true, 1);
		self_history_defect = SELF_HISTORY_OLD_HOFF;
		nonowner_case(false, CLUSTER_TX_COMMITTED, CLUSTER_TX_COMMITTED, false, true, 1);
		self_update_layout = 0;
		self_history_defect = SELF_HISTORY_OK;
	}
	self_history_records = 0;
	self_history_cross_page = false;
}
