/* Real holder/reader/inverse; the peer receipt is an explicit typed origin
 * boundary. These tests do not claim the unintegrated TT40 full-id source. */
UT_TEST(test_peer_operation_discarded_completes_foreign_holder)
{
#ifdef CLUSTER_R4_OPERATION_RPC_PRESENT
	for (unsigned same = 0; same < 2; same++) {
		OperationWorkerFixture f;
		ClusterR4PeerOperationProof peer = { 0 };
		ClusterSavepointReaderIdentity identity;
		ClusterRuntimeVisibilityOperationRead read = { 0 };
		const ClusterItlSlotData *head;
		int acquisitions;

		operation_worker_init(&f, same);
		head = &ClusterPageGetItlSlots((Page)f.cr.source.data)[2];
		UT_ASSERT(cluster_runtime_visibility_operation_origin_read_begin(
			&read, &test_origin_locator, NULL, head, &f.cr.tag, CLUSTER_TX_RESOLVE_VISIBILITY,
			&f.cr.origin.admission, &f.cr.origin.reason));
		UT_ASSERT_EQ(operation_poll_finish(&read, &f.cr.origin, &identity),
					 CLUSTER_RUNTIME_VISIBILITY_OPERATION_COMPLETE);
		UT_ASSERT(!FullTransactionIdIsValid(f.cr.origin.top.local_original_full_xid));
		peer.identity = identity;
		peer.flags = CLUSTER_R4_OPERATION_ORDERED | CLUSTER_R4_OPERATION_CHILD;
		peer.effect = CLUSTER_SAVEPOINT_EFFECT_DISCARDED;
		peer.record_format = 2;
		peer.outcome = f.cr.origin.top.outcome;
		peer.proof_kind = f.cr.origin.top.proof_kind;
		peer.commit_scn = f.cr.origin.top.commit_scn;
		peer.admission_generation = f.cr.origin.admission.record_generation;
		peer.uba = test_origin_locator.uba;
		memcpy(f.foreign.data, f.cr.origin.output.data, BLCKSZ);
		acquisitions = test_candidate_acquire_calls;
		cluster_node_id = 1;
		UT_ASSERT_EQ(operation_worker_step(&f), CLUSTER_R4_CR_STEP_NEED_UNDO);
		UT_ASSERT(ClusterR4PeerOperationEncode(&f.extension.peer_operation, &peer));
		{
			ClusterTxLocator pending, exact;
			PGAlignedBlock record;
			size_t length;
			UT_ASSERT(cluster_cr_build_on_holder_pending_locator(0, 1000, &pending));
			UT_ASSERT(cluster_r4_peer_operation_extract(
				&f.extension.foreign_operation, &f.extension.peer_operation, &pending,
				peer.admission_generation, f.foreign.data, record.data, &length, &exact));
		}
		f.extension.foreign_wrap = identity.canonical.wrap;
		f.extension.origin_live_hwm_lsn = f.cr.origin.top.authority.live_hwm_lsn;
		f.extension.origin_tt_generation = f.cr.origin.top.authority.tt_generation;
		f.extension.origin_authority_scn = f.cr.origin.top.authority.authority_scn;
		UT_ASSERT_EQ(cluster_cr_build_on_holder_step_admitted(0, 1000, true, &f.extension,
															  f.cr.source.data, f.foreign.data,
															  &f.cr.origin.admission, &f.reason),
					 CLUSTER_R4_CR_STEP_FULL);
		UT_ASSERT(!operation_cr_has_child(f.cr.source.data));
		UT_ASSERT_EQ(test_candidate_acquire_calls, acquisitions);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(f.extension.peer_operation.bytes[0], 0);
		cluster_cr_build_on_holder_forget(0, 1000);
		UT_ASSERT_EQ(operation_worker_allocations, 0);
		cluster_node_id = 0;
	}
#else
	UT_ASSERT(false);
#endif
}

UT_TEST(test_peer_operation_real_sampler_refuses_missing_original_full_id)
{
	for (unsigned same = 0; same < 2; same++)
		for (unsigned state = 0; state < 3; state++) {
			OperationPlanFixture f;
			ClusterR4PeerOperationProof peer, zero = { 0 };
			ClusterTxResolution empty = { 0 };
			PGAlignedBlock before;
			int acquisitions;
			operation_plan_init(&f, same, CLUSTER_SAVEPOINT_EFFECT_LIVE);
			if (state == 1) {
				test_tt_slot.status = TT_SLOT_ACTIVE;
				test_tt_slot.commit_scn = InvalidScn;
				test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
				test_twophase_xid = TEST_ORIGIN_XID;
				test_current_owner_available = true;
			} else if (state == 2) {
				test_tt_slot.status = TT_SLOT_ABORTED;
				test_tt_slot.commit_scn = InvalidScn;
				test_native_status = TRANSACTION_STATUS_ABORTED;
			}
			operation_plan_sample(&f);
			memset(&peer, 0x5a, sizeof(peer));
			memset(f.output.data, 0x5a, BLCKSZ);
			before = f.output;
			acquisitions = test_candidate_acquire_calls;
			UT_ASSERT_EQ(cluster_runtime_visibility_origin_plan_copy_peer_held(
							 &f.plan, CLUSTER_TX_RESOLVE_VISIBILITY, &f.admission, &f.guard,
							 &f.data_root, &f.top, &peer, f.output.data, &f.reason),
						 CLUSTER_TX_UNKNOWN);
			UT_ASSERT_EQ(f.reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);
			UT_ASSERT_EQ(memcmp(&peer, &zero, sizeof(peer)), 0);
			UT_ASSERT_EQ(memcmp(&f.top, &empty, sizeof(empty)), 0);
			UT_ASSERT_EQ(memcmp(before.data, f.output.data, BLCKSZ), 0);
			UT_ASSERT_EQ(test_candidate_acquire_calls, acquisitions);
		}
}

/* Receipt identity/outcome is an explicit typed origin boundary. DATA,
 * retained comparison, holder WALK/RESULT, projection and cleanup are real. */
static bool
peer_fixture_supply(OperationWorkerFixture *f, unsigned mode, unsigned *old_count)
{
	ClusterR4OperationRequest request;
	ClusterR4PeerOperationProof peer = { 0 };
	ClusterTxLocator locator;
	uint32 segment, block;
	uint16 offset, row;
	UndoSlotDirEntry *directory;
	UndoRecordHeader *record;
	bool valid;
	if (!ClusterR4OperationContextDecode(&f->extension.foreign_operation, &request)
		|| !cluster_cr_build_on_holder_pending_locator(0, 1000, &locator)
		|| !uba_decode_record(locator.uba, &segment, &block, &offset, &row))
		return false;
	memcpy(f->foreign.data, test_operation_data_page(block, false), BLCKSZ);
	directory = UNDO_SLOT_DIR_PTR(f->foreign.data, row);
	record = (UndoRecordHeader *)(f->foreign.data + directory->record_offset);
	locator.tt_wrap = record->tt_wrap_plus1 - 1;
	valid = request.retained
				? cluster_savepoint_reader_discover_retained_head(&request.identity, &request.head,
																  &request.tag, &locator,
																  f->foreign.data, &peer.identity)
				: cluster_savepoint_reader_discover_head(&request.head, &request.tag, &locator,
														 f->foreign.data, &peer.identity);
	if (!valid)
		return false;
	peer.flags = CLUSTER_R4_OPERATION_ORDERED;
	peer.record_format = peer.identity.operation_sequence != 0 ? 2 : 1;
	peer.flags |= peer.record_format == 2 ? CLUSTER_R4_OPERATION_CHILD : CLUSTER_R4_OPERATION_V1;
	if (peer.record_format == 2) {
		UndoOperationTrailer tail;
		memcpy(&tail, (char *)(record + 1) + record->payload_length - sizeof(tail), sizeof(tail));
		peer.effect = tail.effect_state;
	}
	if (locator.xid == TEST_ORIGIN_XID) {
		peer.outcome
			= mode == 3 && (*old_count)++ == 0 ? CLUSTER_TX_IN_PROGRESS : CLUSTER_TX_COMMITTED;
		peer.commit_scn = peer.outcome == CLUSTER_TX_COMMITTED ? (mode == 2 ? 78 : 72) : InvalidScn;
	} else {
		peer.outcome = mode == 3 ? CLUSTER_TX_COMMITTED : CLUSTER_TX_IN_PROGRESS;
		peer.commit_scn = mode == 3 ? 100 : InvalidScn;
	}
	peer.proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
	peer.admission_generation = f->cr.origin.admission.record_generation;
	peer.uba = locator.uba;
	if (!ClusterR4PeerOperationEncode(&f->extension.peer_operation, &peer))
		return false;
	f->extension.foreign_wrap = peer.identity.canonical.wrap;
	f->extension.origin_live_hwm_lsn = 12345;
	f->extension.origin_tt_generation = 55;
	f->extension.origin_authority_scn = 1000;
	return true;
}

UT_TEST(test_peer_operation_mixed_history_keeps_only_qualified_effects)
{
	for (unsigned layout = 0; layout < 4; layout++)
		for (unsigned mode = 0; mode < 4; mode++) {
			OperationWorkerFixture f;
			PGAlignedBlock expected;
			ClusterR4CrBuildStepResult step = CLUSTER_R4_CR_STEP_PENDING;
			unsigned old_count = 0, supplied = 0;
			int allocations = operation_worker_allocations;
			holder_mixed_init(&f, layout, 0, mode);
			/* The shared mode-3 fixture normally uses two slots. A reuse
             * layout has no second live head; keep that unused slot FREE. */
			if (mode == 3 && (layout & 2))
				ClusterPageGetItlSlots((Page)f.cr.source.data)[4]
					= (ClusterItlSlotData){ .wrap = 14 };
			holder_mixed_expected(expected.data, &f, layout, 0, mode);
			cluster_node_id = 1;
			for (unsigned tick = 0; tick < 80; tick++) {
				step = operation_worker_step(&f);
				if (step != CLUSTER_R4_CR_STEP_NEED_UNDO)
					break;
				if (!peer_fixture_supply(&f, mode, &old_count)) {
					UT_ASSERT(false);
					break;
				}
				supplied++;
				step = cluster_cr_build_on_holder_step_admitted(0, 1000, true, &f.extension,
																f.cr.source.data, f.foreign.data,
																&f.cr.origin.admission, &f.reason);
				UT_ASSERT_EQ(f.extension.peer_operation.bytes[0], 0);
				if (step != CLUSTER_R4_CR_STEP_PENDING)
					break;
			}
			if (step != CLUSTER_R4_CR_STEP_FULL)
				printf("# peer mixed layout=%u mode=%u supplied=%u reason=%d step=%d\n", layout,
					   mode, supplied, f.reason, step);
			UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
			UT_ASSERT_EQ(supplied, 24);
			UT_ASSERT_EQ(memcmp(expected.data, f.cr.source.data, BLCKSZ), 0);
			UT_ASSERT_EQ(test_candidate_acquire_calls, 0);
			cluster_cr_build_on_holder_forget(0, 1000);
			UT_ASSERT_EQ(operation_worker_allocations, allocations);
			cluster_node_id = 0;
		}
}

/* Host-only fixture transport. The actual holder validates DATA and typed
 * peer proof before producing these immutable bytes; outcome is a boundary
 * double until the independent TT40 source is integrated. */
static int
peer_worker_export_full(FILE *stream)
{
	TestOwnerFullFixture fixture = { 0 };
	OperationWorkerFixture f;
	ClusterItlSlotData *head;
	HeapTupleHeader tuple;
	ClusterR4CrBuildStepResult step;
	unsigned old_count = 0;
	int allocations = operation_worker_allocations;

	operation_worker_init(&f, false);
	operation_plan_effect(CLUSTER_SAVEPOINT_EFFECT_LIVE);
	f.extension.route_proof.read_scn = 100;
	head = &ClusterPageGetItlSlots((Page)f.cr.source.data)[2];
	head->flags = ITL_FLAG_ACTIVE;
	head->commit_scn = InvalidScn;
	tuple = (HeapTupleHeader)PageGetItem((Page)f.cr.source.data,
										 PageGetItemId((Page)f.cr.source.data, 3));
	tuple->t_itl_slot_idx = 2;
	tuple->t_choice.t_heap.t_field3.t_cid = InvalidCommandId;
	cluster_node_id = 1;
	step = operation_worker_step(&f);
	if (step == CLUSTER_R4_CR_STEP_NEED_UNDO && peer_fixture_supply(&f, 0, &old_count))
		step = cluster_cr_build_on_holder_step_admitted(0, 1000, true, &f.extension,
														f.cr.source.data, f.foreign.data,
														&f.cr.origin.admission, &f.reason);
	fixture.magic = TEST_PEER_FULL_MAGIC;
	fixture.expected = f.cr.origin.expected;
	fixture.tag = f.cr.tag;
	fixture.read_scn = f.extension.route_proof.read_scn;
	memcpy(fixture.full.data, f.cr.source.data, BLCKSZ);
	memcpy(fixture.undo.data, test_operation_page.data, BLCKSZ);
	cluster_cr_build_on_holder_forget(0, 1000);
	cluster_node_id = 0;
	if (step != CLUSTER_R4_CR_STEP_FULL || f.reason != CLUSTER_CR_BUILD_NONE
		|| !operation_cr_has_child(fixture.full.data) || ut_failed_count != 0
		|| test_candidate_acquire_calls != 0 || test_candidate_held_count != 0
		|| operation_worker_allocations != allocations)
		return 1;
	return fwrite(&fixture, sizeof(fixture), 1, stream) == 1 ? 0 : 1;
}
