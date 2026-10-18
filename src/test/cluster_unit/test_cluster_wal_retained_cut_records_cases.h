/*-------------------------------------------------------------------------
 *
 * test_cluster_wal_retained_cut_records_cases.h
 *	  Cases of test_cluster_wal_retained_cut_records.c.  Self is thread 1
 *	  (history [0x1000, 0x3000), tail 0x4000); the peer is thread 2
 *	  (history [0x5000, 0x6000), tail 0x7000).
 *
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_wal_retained_cut_records_cases.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-03-shared-wal-and-checkpoint.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_WAL_RETAINED_CUT_RECORDS_CASES_H
#define TEST_CLUSTER_WAL_RETAINED_CUT_RECORDS_CASES_H

#define REC_SELF_REDO 0x3000
#define REC_PEER_REDO 0x6000

static void
rec_two_writers(uint32 *self, uint32 *peer)
{
	records_reset();
	*self = add_writer(1, 0x1000, REC_SELF_REDO, 0x4000);
	*peer = add_writer(2, 0x5000, REC_PEER_REDO, 0x7000);
}

static void
expect_cut(uint32 source, XLogRecPtr lower, ClusterWalRetainedPinV1 pin, uint32 side)
{
	ClusterWalRetainedCutV1 cut;
	RfPageProofDetailV1 detail;

	UT_ASSERT_EQ(compute_for(source, &cut, &detail), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(cut.lower, lower);
	UT_ASSERT_EQ(cut.pin, pin);
	UT_ASSERT_EQ(cut.side_classes, side);
}

/* Exercise the cold classifier and original SIDE validator together. The
 * optional SIDE capture is not the owner of native/history acceptance. */
static RealRecord *
rec_cold_tt_terminal(uint32 source, int carrier)
{
	uint16 thread = items[source].source.claim.identity.origin_thread_id;
	uint32 segment = (uint32)(thread - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	RealRecord *r;

	if (carrier == 0)
		return rec_tt_commit(source, 0x1000, 0x1100, 801, 4);
	if (carrier == 2) {
		xl_undo_tt_slot_set_head head = { 0 };

		head.segment_id = segment;
		head.slot_offset = 4;
		head.wrap = 7;
		head.xid = 801;
		head.instance = thread;
		head.first_undo_block = uba_encode(segment, 1, 4, 128);
		r = record_new(source, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_SET_HEAD, 801, 0x1000, 0x1100);
		record_append(r, &head, sizeof(head));
	} else {
		r = record_new(source, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_ABORT, 801, 0x1000, 0x1100);
		if (carrier == 3) {
			xl_undo_tt_slot_abort_exact abort = { 0 };

			abort.segment_id = segment;
			/* Generation zero is a valid original INIT identity. */
			abort.segment_generation = 0;
			abort.slot_offset = 4;
			abort.wrap = 7;
			abort.xid = 801;
			abort.instance = thread;
			abort.format_version = CLUSTER_UNDO_TT_ABORT_EXACT_VERSION;
			record_append(r, &abort, sizeof(abort));
		} else {
			xl_undo_tt_slot_abort abort = { 0 };

			abort.segment_id = segment;
			abort.slot_offset = 4;
			abort.wrap = 7;
			abort.xid = 801;
			abort.instance = thread;
			record_append(r, &abort, sizeof(abort));
		}
	}
	return r;
}

/* Feed the real decoder's output straight into the original planner. A
 * checkpoint cannot make an unsupported prepared carrier into ordinary history. */
static ClusterColdDetailV1
rec_cold_tt_feed(const ClusterColdDecodedV1 *decoded, uint16 thread, bool history)
{
	ClusterColdParticipantV1 cut = { 0 };
	ClusterColdPlanV1 *plan = NULL;
	ClusterColdDetailV1 detail;

	cut.thread_id = thread;
	cut.timeline = 1;
	cut.owner_incarnation = 11;
	cut.physical_lower = decoded->record.read_rec_ptr;
	cut.native_redo = history ? decoded->record.end_rec_ptr : cut.physical_lower;
	cut.tail_end = decoded->record.end_rec_ptr;
	detail = cluster_cold_plan_create_v1(&cut, 1, 4 * 1024 * 1024, &plan);
	if (detail == CLUSTER_COLD_OK)
		detail = cluster_cold_plan_feed_v1(plan, 0, &decoded->record);
	cluster_cold_plan_destroy_v1(&plan);
	return detail;
}

UT_TEST(test_records_cold_prepared_tt_sources_refuse_history_and_redo)
{
	for (int carrier = 0; carrier < 3; carrier++)
		for (int origin = 0; origin < 2; origin++)
			for (int foreign = 0; foreign < 2; foreign++) {
				uint32 self, peer, source;
				RealRecord *r;
				ClusterColdDecodedV1 decoded = { 0 };

				rec_two_writers(&self, &peer);
				source = origin ? peer : self;
				r = rec_cold_tt_terminal(source, carrier);
				UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(
								 &r->reader, REC_SYSID,
								 items[source].source.claim.identity.storage_uuid, false, foreign,
								 &decoded),
							 CLUSTER_COLD_OK);
				UT_ASSERT(decoded.record.record_flags & CLUSTER_COLD_RECORD_UNSUPPORTED);
				for (int history = 0; history < 2; history++)
					UT_ASSERT_EQ(rec_cold_tt_feed(&decoded, origin + 1, history),
								 CLUSTER_COLD_STRUCTURAL_UNSUPPORTED);
				cluster_cold_decoded_release_v1(&decoded);
			}
}

UT_TEST(test_records_cold_exact_abort_sources_stay_supported)
{
	for (int origin = 0; origin < 2; origin++)
		for (int foreign = 0; foreign < 2; foreign++) {
			uint32 self, peer, source;
			RealRecord *r;
			ClusterColdDecodedV1 decoded = { 0 };

			rec_two_writers(&self, &peer);
			source = origin ? peer : self;
			r = rec_cold_tt_terminal(source, 3);
			UT_ASSERT_EQ(
				cluster_cold_recovery_decode_v1(&r->reader, REC_SYSID,
												items[source].source.claim.identity.storage_uuid,
												false, foreign, &decoded),
				CLUSTER_COLD_OK);
			UT_ASSERT_EQ(decoded.record.record_flags,
						 foreign ? CLUSTER_COLD_RECORD_FOREIGN_SIDE : 0);
			for (int history = 0; history < 2; history++)
				UT_ASSERT_EQ(rec_cold_tt_feed(&decoded, origin + 1, history), CLUSTER_COLD_OK);
			cluster_cold_decoded_release_v1(&decoded);
		}
}

UT_TEST(test_records_cold_tt_terminal_carriers_require_full_payload)
{
	for (int carrier = 0; carrier < 4; carrier++)
		for (int foreign = 0; foreign < 2; foreign++)
			for (int extra = 0; extra < 2; extra++) {
				uint32 self, peer;
				RealRecord *r;
				ClusterColdDecodedV1 decoded = { 0 };

				rec_two_writers(&self, &peer);
				r = rec_cold_tt_terminal(self, carrier);
				r->u.decoded.main_data_len += extra ? 1 : -1;
				UT_ASSERT_EQ(
					cluster_cold_recovery_decode_v1(&r->reader, REC_SYSID,
													items[self].source.claim.identity.storage_uuid,
													false, foreign, &decoded),
					CLUSTER_COLD_COMPONENT_INVALID);
				cluster_cold_decoded_release_v1(&decoded);
			}
}

UT_TEST(test_records_cold_assignment_rejects_malformed_xids)
{
	for (int fault = 0; fault < 10; fault++) {
		uint32 self, peer;
		RealRecord *r;
		PGAlignedBlock payload = { 0 };
		xl_xact_assignment *a = (xl_xact_assignment *)payload.data;
		uint32 length = MinSizeOfXactAssignment + 2 * sizeof(TransactionId);

		rec_two_writers(&self, &peer);
		a->xtop = 800;
		a->nsubxacts = 2;
		a->xsub[0] = 801;
		a->xsub[1] = 802;
		switch (fault) {
		case 0:
			a->xtop = FrozenTransactionId;
			break;
		case 1:
			a->xsub[1] = a->xsub[0];
			break;
		case 2:
			a->xsub[1] = 799;
			break;
		case 3:
			a->xsub[1] = InvalidTransactionId;
			break;
		case 4:
			a->xsub[1] = a->xtop + UINT32_C(0x80000000);
			break;
		case 5:
			a->nsubxacts = 0;
			length = MinSizeOfXactAssignment;
			break;
		case 6:
			a->nsubxacts = PGPROC_MAX_CACHED_SUBXIDS + 1;
			for (int i = 0; i < a->nsubxacts; i++)
				a->xsub[i] = 801 + i;
			length = MinSizeOfXactAssignment + a->nsubxacts * sizeof(TransactionId);
			break;
		case 7:
			length--;
			break;
		case 8:
			length++;
			break;
		case 9:
			a->nsubxacts = -1;
			break;
		}
		r = record_new(self, RM_XACT_ID, XLOG_XACT_ASSIGNMENT, InvalidTransactionId, 0x2000,
					   0x2100);
		record_append(r, payload.data, length);
		UT_ASSERT(!rf_side_xact_assignment_preflight_v1(r->data, length));
		for (int foreign = 0; foreign < 2; foreign++) {
			ClusterColdDecodedV1 decoded = { 0 };
			ClusterColdDetailV1 detail = cluster_cold_recovery_decode_v1(
				&r->reader, REC_SYSID, items[self].source.claim.identity.storage_uuid, false,
				foreign, &decoded);

			cluster_cold_decoded_release_v1(&decoded);
			if (detail != CLUSTER_COLD_COMPONENT_INVALID)
				printf("# assignment fault %d foreign %d accepted as %d\n", fault, foreign, detail);
			UT_ASSERT_EQ(detail, CLUSTER_COLD_COMPONENT_INVALID);
		}
	}
}

UT_TEST(test_records_cold_assignment_keeps_valid_wrap_and_ownership)
{
	for (int wrap = 0; wrap < 2; wrap++) {
		uint32 self, peer;
		RealRecord *r;
		xl_xact_assignment a = { wrap ? UINT32_MAX - 1 : 800, 3 };
		TransactionId children[3] = { wrap ? UINT32_MAX : 801, wrap ? 3 : 802, wrap ? 4 : 803 };

		rec_two_writers(&self, &peer);
		r = record_new(self, RM_XACT_ID, XLOG_XACT_ASSIGNMENT, InvalidTransactionId, 0x2000,
					   0x2100);
		record_append(r, &a, MinSizeOfXactAssignment);
		record_append(r, children, sizeof(children));
		/* The original retained input need not be aligned. */
		memmove(r->data + 1, r->data, r->reader.record->main_data_len);
		r->reader.record->main_data = (char *)r->data + 1;
		for (int foreign = 0; foreign < 2; foreign++) {
			ClusterColdDecodedV1 decoded = { 0 };

			UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(
							 &r->reader, REC_SYSID, items[self].source.claim.identity.storage_uuid,
							 false, foreign, &decoded),
						 CLUSTER_COLD_OK);
			UT_ASSERT_EQ(decoded.record.record_flags,
						 foreign ? CLUSTER_COLD_RECORD_FOREIGN_SIDE : 0);
			cluster_cold_decoded_release_v1(&decoded);
		}
	}
}

static ClusterColdDetailV1
rec_counter_decode(RealRecord *r, bool foreign, ClusterColdDecodedV1 *decoded)
{
	r->reader.cluster_expected_thread_id
		= items[r->source].source.claim.identity.origin_thread_id;
	return cluster_cold_recovery_decode_v1(&r->reader, REC_SYSID,
										 items[r->source].source.claim.identity.storage_uuid,
										 false, foreign, decoded);
}

UT_TEST(test_records_cold_ordered_counter_candidate)
{
	for (int foreign = 0; foreign < 2; foreign++) {
		uint32 self, peer;
		ClusterColdParticipantV1 source = { 0 };
		ClusterColdXidProgressV1 progress;
		RealRecord *stream[6];
		CheckPoint checkpoint = { 0 };
		xl_xact_xinfo xinfo;
		TransactionId children[2] = { 111, 114 };
		int count = lengthof(children);
		Size offset = MinSizeOfXactCommit + sizeof(xinfo);
		Size extra = sizeof(count) + sizeof(children);
		uint64 candidate = 99;

		rec_two_writers(&self, &peer);
		stream[0] = record_new(self, RM_XLOG_ID, XLOG_NOOP, 105, 0x2000, 0x2100);
		stream[1] = rec_commit(self, 0x2100, 0x2200, 110, NULL, 0, false, 0);
		memcpy(&xinfo, stream[1]->data + offset - sizeof(xinfo), sizeof(xinfo));
		xinfo.xinfo |= XACT_XINFO_HAS_SUBXACTS;
		memcpy(stream[1]->data + offset - sizeof(xinfo), &xinfo, sizeof(xinfo));
		memmove(stream[1]->data + offset + extra, stream[1]->data + offset,
				stream[1]->u.decoded.main_data_len - offset);
		memcpy(stream[1]->data + offset, &count, sizeof(count));
		memcpy(stream[1]->data + offset + sizeof(count), children, sizeof(children));
		stream[1]->u.decoded.main_data_len += extra;
		checkpoint.nextXid = FullTransactionIdFromEpochAndXid(9, 90);
		stream[2] = record_new(self, RM_XLOG_ID, XLOG_CHECKPOINT_ONLINE, InvalidTransactionId,
							   0x2200, 0x2300);
		record_append(stream[2], &checkpoint, sizeof(checkpoint));
		stream[3] = record_new(self, RM_XLOG_ID, XLOG_NOOP, 99, 0x2300, 0x2400);
		checkpoint.nextXid = FullTransactionIdFromEpochAndXid(9, 70);
		stream[4] = record_new(self, RM_XLOG_ID, XLOG_CHECKPOINT_SHUTDOWN, InvalidTransactionId,
							   0x2400, 0x2500);
		record_append(stream[4], &checkpoint, sizeof(checkpoint));
		stream[5] = rec_abort(self, 0x2500, 0x2600, 80, NULL, 0);
		source.thread_id = 1;
		source.timeline = 7;
		source.owner_incarnation = 19;
		source.physical_lower = 0x1000;
		source.native_redo = 0x2000;
		source.tail_end = 0x2600;
		UT_ASSERT(cluster_cold_xid_begin_v1(&progress, &source,
										   U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(8, 100)),
										   lengthof(stream)));
		for (int i = 0; i < lengthof(stream); i++) {
			ClusterColdDecodedV1 decoded = { 0 };
			stream[i]->u.decoded.header.xl_prev = i == 0 ? 0x1000 : stream[i - 1]->reader.ReadRecPtr;
			UT_ASSERT_EQ(rec_counter_decode(stream[i], foreign, &decoded), CLUSTER_COLD_OK);
			UT_ASSERT(cluster_cold_xid_record_v1(&progress, &source, &decoded.record,
												 &decoded.xid_input));
			if (i == 1) {
				UT_ASSERT_EQ(progress.next_xid,
							 U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(8, 115)));
				UT_ASSERT_EQ(decoded.record.record_flags,
							 foreign ? CLUSTER_COLD_RECORD_FOREIGN_SIDE : 0);
			}
			if (i == 4)
				UT_ASSERT_EQ(progress.next_xid,
							 U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(9, 70)));
			if (i < lengthof(stream) - 1) {
				candidate = 99;
				UT_ASSERT(!cluster_cold_xid_candidate_v1(&progress, &source, &candidate));
				UT_ASSERT_EQ(candidate, 0);
			}
			cluster_cold_decoded_release_v1(&decoded);
		}
		UT_ASSERT(cluster_cold_xid_candidate_v1(&progress, &source, &candidate));
		UT_ASSERT_EQ(candidate,
					 U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(9, 81)));
	}
}

UT_TEST(test_records_cold_terminal_counter_inputs)
{
	for (int abort_record = 0; abort_record < 2; abort_record++) {
		for (int wrap = 0; wrap < 2; wrap++) {
			uint32 self, peer;
			TransactionId top = wrap ? UINT32_MAX - 2 : 800;
			TransactionId children[3] = { wrap ? 4 : 803, wrap ? UINT32_MAX : 801,
										 wrap ? 3 : 802 };
			int count = lengthof(children);
			RealRecord *r;
			xl_xact_xinfo xinfo;
			Size offset = (abort_record ? MinSizeOfXactAbort : MinSizeOfXactCommit)
						  + sizeof(xinfo);
			Size extra = sizeof(count) + sizeof(children);

			rec_two_writers(&self, &peer);
			r = abort_record ? rec_abort(self, 0x2000, 0x2100, top, NULL, 0)
							 : rec_commit(self, 0x2000, 0x2100, top, NULL, 0, false, 0);
			memcpy(&xinfo, r->data + offset - sizeof(xinfo), sizeof(xinfo));
			xinfo.xinfo |= XACT_XINFO_HAS_SUBXACTS;
			memcpy(r->data + offset - sizeof(xinfo), &xinfo, sizeof(xinfo));
			memmove(r->data + offset + extra, r->data + offset,
					r->u.decoded.main_data_len - offset);
			memcpy(r->data + offset, &count, sizeof(count));
			memcpy(r->data + offset + sizeof(count), children, sizeof(children));
			r->u.decoded.main_data_len += extra;
			/* Original readers need not supply aligned main data. */
			memmove(r->data + 1, r->data, r->u.decoded.main_data_len);
			r->u.decoded.main_data = (char *)r->data + 1;
			for (int foreign = 0; foreign < 2; foreign++) {
				ClusterColdDecodedV1 decoded = { 0 };

				UT_ASSERT_EQ(rec_counter_decode(r, foreign, &decoded), CLUSTER_COLD_OK);
				UT_ASSERT(decoded.xid_input.complete);
				UT_ASSERT_EQ(decoded.xid_input.record_xid, top);
				UT_ASSERT_EQ(decoded.xid_input.payload_xid, wrap ? 4 : 803);
				UT_ASSERT_EQ(decoded.xid_input.checkpoint_next_xid, 0);
				/* This unsorted child list fails the native terminal-tree
				 * contract; valid counter evidence cannot supply that owner. */
				UT_ASSERT_EQ(decoded.record.record_flags,
							 foreign ? CLUSTER_COLD_RECORD_SIDE_UNOWNED : 0);
				cluster_cold_decoded_release_v1(&decoded);
			}
		}
	}
}

static void
rec_check_cold_terminal_children(bool abort)
{
	for (int wrap = 0; wrap < 2; wrap++)
		for (int invalid = 0; invalid < 2; invalid++) {
			uint32 self, peer;
			TransactionId top = wrap ? UINT32_MAX - 2 : 800;
			TransactionId children[] = { wrap ? UINT32_MAX : 801, wrap ? 3 : 802, wrap ? 4 : 803 };
			int count = lengthof(children);
			RealRecord *r;
			xl_xact_xinfo xinfo;
			Size header = abort ? MinSizeOfXactAbort : MinSizeOfXactCommit;
			Size offset = header + sizeof(xinfo);
			Size added = sizeof(count) + sizeof(children);
			ClusterColdDecodedV1 decoded = { 0 };

			rec_two_writers(&self, &peer);
			r = abort ? rec_abort(self, 0x2000, 0x2100, top, NULL, 0)
					  : rec_commit(self, 0x2000, 0x2100, top, NULL, 0, false, 0);
			memcpy(&xinfo, r->data + header, sizeof(xinfo));
			xinfo.xinfo |= XACT_XINFO_HAS_SUBXACTS;
			memcpy(r->data + header, &xinfo, sizeof(xinfo));
			if (invalid)
				children[1] = children[0];
			memmove(r->data + offset + added, r->data + offset,
					r->u.decoded.main_data_len - offset);
			memcpy(r->data + offset, &count, sizeof(count));
			memcpy(r->data + offset + sizeof(count), children, sizeof(children));
			r->u.decoded.main_data_len += added;
			memmove(r->data + 1, r->data, r->u.decoded.main_data_len);
			r->u.decoded.main_data = (char *)r->data + 1;
			UT_ASSERT_EQ(rec_counter_decode(r, true, &decoded), CLUSTER_COLD_OK);
			UT_ASSERT(decoded.xid_input.complete);
			UT_ASSERT_EQ(decoded.record.record_flags, invalid ? CLUSTER_COLD_RECORD_SIDE_UNOWNED
															  : CLUSTER_COLD_RECORD_FOREIGN_SIDE);
			cluster_cold_decoded_release_v1(&decoded);
		}
}

UT_TEST(test_records_cold_abort_children_require_native_tree_shape)
{
	rec_check_cold_terminal_children(true);
}

UT_TEST(test_records_cold_commit_children_require_native_tree_shape)
{
	rec_check_cold_terminal_children(false);
}

UT_TEST(test_records_cold_multixact_counter_inputs)
{
	for (int variant = 0; variant < 4; variant++) {
		bool wrap = variant == 1;
		uint32 self, peer;
		xl_multixact_create header = { 1, 1, 3 };
		MultiXactMember members[3] = {
			{ wrap ? 4 : 903, MultiXactStatusForShare },
			{ wrap ? UINT32_MAX : 901, MultiXactStatusForKeyShare },
			{ wrap ? 3 : 902, MultiXactStatusForUpdate }
		};
		TransactionId xid = variant == 2 ? InvalidTransactionId
											: (variant == 3 ? 1000 : (wrap ? UINT32_MAX - 2 : 900));
		RealRecord *r;

		rec_two_writers(&self, &peer);
		r = record_new(self, RM_MULTIXACT_ID, XLOG_MULTIXACT_CREATE_ID, xid, 0x2000, 0x2100);
		record_append(r, &header, SizeOfMultiXactCreate);
		record_append(r, members, sizeof(members));
		memmove(r->data + 1, r->data, r->u.decoded.main_data_len);
		r->u.decoded.main_data = (char *)r->data + 1;
		for (int foreign = 0; foreign < 2; foreign++) {
			ClusterColdDecodedV1 decoded = { 0 };

			UT_ASSERT_EQ(rec_counter_decode(r, foreign, &decoded), CLUSTER_COLD_OK);
			UT_ASSERT(decoded.xid_input.complete);
			UT_ASSERT_EQ(decoded.xid_input.record_xid, xid);
			UT_ASSERT_EQ(decoded.xid_input.payload_xid, variant == 3 ? 1000 : (wrap ? 4 : 903));
			UT_ASSERT_EQ(decoded.record.record_flags,
						 foreign ? CLUSTER_COLD_RECORD_FOREIGN_SIDE : 0);
			cluster_cold_decoded_release_v1(&decoded);
		}
	}
}

UT_TEST(test_records_cold_checkpoint_counter_inputs)
{
	for (int shutdown = 0; shutdown < 2; shutdown++) {
		uint32 self, peer;
		CheckPoint checkpoint = { 0 };
		RealRecord *r;

		rec_two_writers(&self, &peer);
		checkpoint.nextXid = FullTransactionIdFromEpochAndXid(7, 50);
		r = record_new(self, RM_XLOG_ID,
					   shutdown ? XLOG_CHECKPOINT_SHUTDOWN : XLOG_CHECKPOINT_ONLINE,
					   InvalidTransactionId, 0x2000, 0x2100);
		record_append(r, &checkpoint, sizeof(checkpoint));
		memmove(r->data + 1, r->data, r->u.decoded.main_data_len);
		r->u.decoded.main_data = (char *)r->data + 1;
		for (int foreign = 0; foreign < 2; foreign++) {
			ClusterColdDecodedV1 decoded = { 0 };

			UT_ASSERT_EQ(rec_counter_decode(r, foreign, &decoded), CLUSTER_COLD_OK);
			UT_ASSERT(decoded.xid_input.complete);
			UT_ASSERT_EQ(decoded.xid_input.record_xid, InvalidTransactionId);
			UT_ASSERT_EQ(decoded.xid_input.payload_xid, InvalidTransactionId);
			UT_ASSERT_EQ(decoded.xid_input.checkpoint_next_xid,
						 U64FromFullTransactionId(checkpoint.nextXid));
			UT_ASSERT_EQ(decoded.xid_input.checkpoint_shutdown, shutdown);
			UT_ASSERT_EQ(decoded.record.record_flags,
						 foreign ? CLUSTER_COLD_RECORD_FOREIGN_CONTROL : 0);
			cluster_cold_decoded_release_v1(&decoded);
		}
	}
}

UT_TEST(test_records_cold_counter_header_and_assignment)
{
	uint32 self, peer;
	xl_xact_assignment assignment = { 800, 1 };
	TransactionId child = 900;
	RealRecord *r;
	ClusterColdDecodedV1 decoded = { 0 };

	rec_two_writers(&self, &peer);
	r = record_new(self, RM_XLOG_ID, XLOG_NOOP, 700, 0x2000, 0x2100);
	UT_ASSERT_EQ(rec_counter_decode(r, false, &decoded), CLUSTER_COLD_OK);
	UT_ASSERT(decoded.xid_input.complete);
	UT_ASSERT_EQ(decoded.xid_input.record_xid, 700);
	UT_ASSERT_EQ(decoded.xid_input.payload_xid, InvalidTransactionId);
	cluster_cold_decoded_release_v1(&decoded);
	r = record_new(self, RM_XACT_ID, XLOG_XACT_ASSIGNMENT, InvalidTransactionId, 0x2200, 0x2300);
	record_append(r, &assignment, MinSizeOfXactAssignment);
	record_append(r, &child, sizeof(child));
	for (int foreign = 0; foreign < 2; foreign++) {
		UT_ASSERT_EQ(rec_counter_decode(r, foreign, &decoded), CLUSTER_COLD_OK);
		UT_ASSERT(decoded.xid_input.complete);
		UT_ASSERT_EQ(decoded.xid_input.record_xid, InvalidTransactionId);
		/* The assignment's cache population is hot-standby-only. */
		UT_ASSERT_EQ(decoded.xid_input.payload_xid, InvalidTransactionId);
		cluster_cold_decoded_release_v1(&decoded);
	}
	{
		xl_running_xacts running = { 0 };

		running.xcnt = 1;
		running.nextXid = 5000;
		running.oldestRunningXid = 800;
		running.latestCompletedXid = 799;
		r = record_new(self, RM_STANDBY_ID, XLOG_RUNNING_XACTS, InvalidTransactionId,
					   0x2400, 0x2500);
		record_append(r, &running, offsetof(xl_running_xacts, xids));
		record_append(r, &child, sizeof(child));
		for (int foreign = 0; foreign < 2; foreign++) {
			UT_ASSERT_EQ(rec_counter_decode(r, foreign, &decoded), CLUSTER_COLD_OK);
			UT_ASSERT(decoded.xid_input.complete);
			UT_ASSERT_EQ(decoded.xid_input.record_xid, InvalidTransactionId);
			UT_ASSERT_EQ(decoded.xid_input.payload_xid, InvalidTransactionId);
			UT_ASSERT_EQ(decoded.xid_input.checkpoint_next_xid, 0);
			cluster_cold_decoded_release_v1(&decoded);
		}
	}
}

UT_TEST(test_records_cold_counter_incomplete_clears_inputs)
{
	uint32 self, peer;
	ClusterColdDecodedV1 decoded = { 0 };
	CheckPoint checkpoint = { 0 };
	RealRecord *r;

	rec_two_writers(&self, &peer);
	r = record_new(self, RM_XLOG_ID, XLOG_NOOP, 700, 0x2000, 0x2100);
	UT_ASSERT_EQ(rec_counter_decode(r, false, &decoded), CLUSTER_COLD_OK);
	UT_ASSERT(decoded.xid_input.complete);
	/* A short control frame refuses before effect classification and
	 * must never furnish complete counter inputs. */
	r = record_new(self, RM_XLOG_ID, XLOG_CHECKPOINT_ONLINE, 0, 0x2200, 0x2300);
	record_append(r, &checkpoint, sizeof(checkpoint) - 1);
	UT_ASSERT_EQ(rec_counter_decode(r, false, &decoded), CLUSTER_COLD_COMPONENT_INVALID);
	UT_ASSERT_EQ(decoded.effects.required, 0);
	UT_ASSERT(!decoded.xid_input.complete);
	UT_ASSERT_EQ(decoded.xid_input.checkpoint_next_xid, 0);
	r->u.decoded.main_data_len++;
	/* A full checkpoint with an invalid nextXid is also unavailable. */
	UT_ASSERT_EQ(rec_counter_decode(r, false, &decoded), CLUSTER_COLD_OK);
	UT_ASSERT(!decoded.xid_input.complete);
	UT_ASSERT_EQ(decoded.xid_input.record_xid, InvalidTransactionId);
	for (int fault = 0; fault < 8; fault++) {
		xl_multixact_create header = { 1, 1, 2 };
		MultiXactMember members[2] = { { 801, MultiXactStatusForShare },
									   { 802, MultiXactStatusForUpdate } };
		uint8 extra = 0;

		switch (fault) {
		case 0:
			header.nmembers = -1;
			break;
		case 1:
			header.nmembers = 0;
			break;
		case 2:
			members[1].xid = FrozenTransactionId;
			break;
		case 3:
			members[1].status = MaxMultiXactStatus + 1;
			break;
		case 5:
			header.mid = InvalidMultiXactId;
			break;
		case 6:
			header.moff = 0;
			break;
		}
		r = record_new(self, RM_MULTIXACT_ID, XLOG_MULTIXACT_CREATE_ID, 800,
					   0x2400, 0x2500);
		record_append(r, &header, SizeOfMultiXactCreate);
		record_append(r, members, sizeof(members));
		if (fault == 4)
			r->u.decoded.main_data_len--;
		if (fault == 7)
			record_append(r, &extra, sizeof(extra));
		UT_ASSERT_EQ(rec_counter_decode(r, false, &decoded), CLUSTER_COLD_OK);
		UT_ASSERT(!decoded.xid_input.complete);
		UT_ASSERT_EQ(decoded.xid_input.record_xid, InvalidTransactionId);
		UT_ASSERT_EQ(decoded.xid_input.payload_xid, InvalidTransactionId);
	}
	r = record_new(self, RM_XACT_ID, XLOG_XACT_PREPARE, 800, 0x2400, 0x2500);
	UT_ASSERT_EQ(rec_counter_decode(r, false, &decoded), CLUSTER_COLD_OK);
	UT_ASSERT(!decoded.xid_input.complete);
	UT_ASSERT_EQ(decoded.xid_input.payload_xid, InvalidTransactionId);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(NULL, REC_SYSID,
												  items[self].source.claim.identity.storage_uuid,
												  false, false, &decoded),
				 CLUSTER_COLD_INVALID_ARGUMENT);
	UT_ASSERT(!decoded.xid_input.complete);
	UT_ASSERT_EQ(decoded.xid_input.record_xid, InvalidTransactionId);
	cluster_cold_decoded_release_v1(&decoded);
}

/* A COMMIT whose typed tombstones the SIDE decoder accepts: the DROP ends an
 * incarnation, so in history it holds its source (CR20).  The same COMMIT
 * still an obligation, or a plain COMMIT in history, holds nothing (no
 * obligation needs its TT slot). */
UT_TEST(test_records_commit_tombstone_pins_its_source)
{
	static const RelFileNumber drops[] = { 16384, 16390 };

	for (int variant = 0; variant < 3; variant++) {
		uint32 self, peer;

		rec_two_writers(&self, &peer);
		if (variant == 0)
			rec_commit(self, 0x1800, 0x1900, 801, drops, 2, true, 0);
		else if (variant == 1)
			rec_commit(self, 0x3200, 0x3300, 801, drops, 2, true, 0);
		else
			rec_commit(self, 0x1800, 0x1900, 801, NULL, 0, false, 0);
		expect_cut(
			self, variant == 0 ? 0x1800 : REC_SELF_REDO,
			variant == 0 ? CLUSTER_WAL_RETAINED_PIN_STRUCTURE : CLUSTER_WAL_RETAINED_PIN_NONE, 0);
		if (ut_current_failed)
			printf("# tombstone variant %d\n", variant);
	}
}

/* The SIDE decoder does not own a COMMIT with invalidation messages yet.
 * The census classifies it instead of giving up: its TT delta keys its
 * slot (a later commit on the same slot keeps it, one on another node's
 * segment does not); with relations to drop but no typed tombstones it
 * holds its source like a DROP. */
UT_TEST(test_records_commit_with_invalidations_is_classified)
{
	static const RelFileNumber drops[] = { 16384 };

	for (int variant = 0; variant < 7; variant++) {
		uint32 self, peer;
		XLogRecPtr lower = REC_SELF_REDO;
		ClusterWalRetainedPinV1 pin = CLUSTER_WAL_RETAINED_PIN_NONE;
		uint32 side = 0;

		rec_two_writers(&self, &peer);
		if (variant == 3) { /* a standalone invalidation record */
			RealRecord *r
				= record_new(self, RM_XACT_ID, XLOG_XACT_INVALIDATIONS, 801, 0x1800, 0x1900);
			int nmsgs = 0;

			record_append(r, &nmsgs, sizeof(nmsgs));
		} else if (variant == 4) { /* a subtransaction XID assignment */
			RealRecord *r = record_new(self, RM_XACT_ID, XLOG_XACT_ASSIGNMENT, 801, 0x1800, 0x1900);
			xl_xact_assignment assignment = { 801, 1 };
			TransactionId sub = 802;

			record_append(r, &assignment, MinSizeOfXactAssignment);
			record_append(r, &sub, sizeof(sub));
		} else
			rec_commit(self, 0x1800, 0x1900, 801, variant == 2 ? drops : NULL, variant == 2 ? 1 : 0,
					   false, 3);
		if (variant == 1) /* a peer commit: its own segment's slot */
			rec_commit(peer, 0x6100, 0x6200, 902, NULL, 0, false, 0);
		else if (variant == 5) { /* a later typed commit on the same TT slot */
			rec_commit(self, 0x3200, 0x3300, 802, NULL, 0, false, 0);
			lower = 0x1800;
			pin = CLUSTER_WAL_RETAINED_PIN_SIDE;
			side = RF_SIDE_CONTRIBUTION_UNDO_HEADER;
		} else if (variant == 6) {
			/* A native COMMIT PREPARED: its TT slots are unkeyed, so as an
			 * obligation it keeps all UNDO_HEADER history. */
			rec_commit_prepared(self, 0x3200, 0x3300, 990);
			lower = 0x1800;
			pin = CLUSTER_WAL_RETAINED_PIN_SIDE;
			side = RF_SIDE_CONTRIBUTION_UNDO_HEADER;
		} else if (variant == 2) {
			lower = 0x1800;
			pin = CLUSTER_WAL_RETAINED_PIN_STRUCTURE;
		}
		expect_cut(self, lower, pin, side);
		if (ut_current_failed)
			printf("# invalidation variant %d\n", variant);
	}
}

/* ABORT that drops the relations its transaction created ends their
 * incarnation: held in history.  A plain ABORT holds nothing. */
UT_TEST(test_records_abort_with_relations_pins_its_source)
{
	static const RelFileNumber rels[] = { 16400 };

	for (int variant = 0; variant < 2; variant++) {
		uint32 self, peer;

		rec_two_writers(&self, &peer);
		rec_abort(self, 0x2000, 0x2100, 803, rels, variant == 0 ? 1 : 0);
		expect_cut(
			self, variant == 0 ? 0x2000 : REC_SELF_REDO,
			variant == 0 ? CLUSTER_WAL_RETAINED_PIN_STRUCTURE : CLUSTER_WAL_RETAINED_PIN_NONE, 0);
		if (ut_current_failed)
			printf("# abort variant %d\n", variant);
	}
}

/* Native SMGR CREATE (every CREATE TABLE) needs no older record; a native
 * SMGR TRUNCATE ends an incarnation. */
UT_TEST(test_records_native_smgr_create_and_truncate)
{
	for (int variant = 0; variant < 2; variant++) {
		uint32 self, peer;

		rec_two_writers(&self, &peer);
		rec_smgr(self, 0x1400, 0x1440, false, 16500);
		if (variant == 1)
			rec_smgr(self, 0x2400, 0x2440, true, 16500);
		expect_cut(
			self, variant == 0 ? REC_SELF_REDO : 0x2400,
			variant == 0 ? CLUSTER_WAL_RETAINED_PIN_NONE : CLUSTER_WAL_RETAINED_PIN_STRUCTURE, 0);
		if (ut_current_failed)
			printf("# smgr variant %d\n", variant);
	}
}

/* D1 coverage is observable without turning a native record into a proof:
 * CREATE, standalone invalidation/assignment and dropped-stats items are
 * counted independently of the retention bound. */
UT_TEST(test_records_native_effect_coverage_is_read_only)
{
	uint32 self, peer;
	ClusterWalRetainedCutV1 cut;
	RfPageProofDetailV1 detail;
	RealRecord *r;
	xl_xact_commit commit = { 0 };
	xl_xact_xinfo xinfo = { 0 };
	xl_xact_stats_item stat = { 1, 5, 6 };
	int nstats = 1;

	rec_two_writers(&self, &peer);
	rec_smgr(self, 0x1400, 0x1440, false, 16501);
	r = record_new(self, RM_XACT_ID, XLOG_XACT_INVALIDATIONS, 801, 0x1500, 0x1540);
	record_append(r, &(int){ 0 }, sizeof(int));
	r = record_new(self, RM_XACT_ID, XLOG_XACT_ASSIGNMENT, 801, 0x1600, 0x1640);
	{
		xl_xact_assignment assignment = { 801, 1 };
		TransactionId sub = 802;
		record_append(r, &assignment, MinSizeOfXactAssignment);
		record_append(r, &sub, sizeof(sub));
	}
	r = record_new(self, RM_XACT_ID, XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO, 803, 0x1700, 0x1740);
	commit.xact_time = 1;
	xinfo.xinfo = XACT_XINFO_HAS_DROPPED_STATS;
	record_append(r, &commit, sizeof(commit));
	record_append(r, &xinfo, sizeof(xinfo));
	record_append(r, &nstats, sizeof(nstats));
	record_append(r, &stat, sizeof(stat));
	UT_ASSERT_EQ(compute_for(self, &cut, &detail), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(cut.native_smgr_create_records, 1);
	UT_ASSERT_EQ(cut.native_xact_invalidation_records, 1);
	UT_ASSERT_EQ(cut.native_xact_assignment_records, 1);
	UT_ASSERT_EQ(cut.native_dropped_stats_records, 1);
	UT_ASSERT_EQ(cut.native_dropped_stats_items, 1);
	UT_ASSERT_EQ(cut.native_effects_incomplete, 0);
	if (ut_current_failed)
		printf("# native effect coverage must remain observational\n");
}

/* A record of another node's source is classified the same way and bounds
 * that source only. */
UT_TEST(test_records_foreign_source_pins_only_itself)
{
	static const RelFileNumber rels[] = { 16600 };
	uint32 self, peer;

	rec_two_writers(&self, &peer);
	rec_smgr(peer, 0x5100, 0x5140, false, 16600);
	rec_abort(peer, 0x5800, 0x5900, 905, rels, 1);
	rec_commit(peer, 0x5a00, 0x5b00, 906, NULL, 0, false, 2);
	expect_cut(self, REC_SELF_REDO, CLUSTER_WAL_RETAINED_PIN_NONE, 0);
	expect_cut(peer, 0x5800, CLUSTER_WAL_RETAINED_PIN_STRUCTURE, 0);
}

/* The fallback is narrow: a typed SPACE record the decoder refuses, a
 * transaction end too short to parse, one with a block reference, or one
 * the decoder rejects for its identity, still refuses the whole census. */
UT_TEST(test_records_unknown_native_record_still_refuses)
{
	for (int variant = 0; variant < 5; variant++) {
		uint32 self, peer;
		ClusterWalRetainedCutV1 cut;
		RfPageProofDetailV1 detail;
		RealRecord *r;
		uint8 garbage[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };

		rec_two_writers(&self, &peer);
		if (variant == 0) {
			r = record_new(self, RM_SMGR_ID, XLOG_SMGR_SPACE_IDENTITY, InvalidTransactionId, 0x1800,
						   0x1900);
			record_append(r, garbage, sizeof(garbage));
		} else if (variant == 1) {
			r = record_new(self, RM_XACT_ID, XLOG_XACT_ABORT, 807, 0x1800, 0x1900);
			record_append(r, garbage, 4);
		} else if (variant == 2) {
			r = rec_commit(self, 0x1800, 0x1900, 808, NULL, 0, false, 2);
			r->u.decoded.max_block_id = 0;
			r->u.decoded.blocks[0].in_use = true;
		} else if (variant == 3) {
			r = record_new(self, RM_XACT_ID, XLOG_XACT_COMMIT, 810, 0x1800, 0x1900);
			record_append(r, garbage, 4); /* shorter than xl_xact_commit */
		} else {
			static const RelFileNumber drops[] = { 16384 };

			/* Its tombstone names another database incarnation. */
			rec_drop_dbinc = REC_DBINC + 1;
			r = rec_commit(self, 0x1800, 0x1900, 809, drops, 1, true, 0);
		}
		UT_ASSERT(compute_for(self, &cut, &detail) != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(detail != RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(cut.lower, 0);
		if (ut_current_failed)
			printf("# refusal variant %d\n", variant);
	}
}

/* A integration reproducer (A hold 16c44655e9), extended: the typed decoder
 * rejects a transaction end whose sections do not lie within its data, and
 * the native fallback must reject it too -- a damaged record refuses the
 * census instead of becoming retention evidence.  Truncated xinfo, count
 * arrays longer than the data, trailing bytes and a section an ABORT cannot
 * carry, for ABORT and COMMIT alike. */
UT_TEST(test_records_short_abort_does_not_advance_retention)
{
	for (int variant = 0; variant < 7; variant++) {
		uint32 self, peer;
		ClusterWalRetainedCutV1 cut;
		ClusterWalRetainedCutDiagnosticsV1 diagnostics;
		RfPageProofDetailV1 detail;
		bool commit = variant >= 5;
		RealRecord *record;
		xl_xact_abort abort_record = { 0 };
		xl_xact_commit commit_record = { 0 };
		xl_xact_xinfo xinfo = { 0 };
		RelFileLocator locator = rec_locator(16400);
		TransactionId sub = 804;
		int count = 1000;
		uint8 junk[4] = { 9, 9, 9, 9 };

		rec_two_writers(&self, &peer);
		memset(&diagnostics, 0xa5, sizeof(diagnostics));
		record = record_new(self, RM_XACT_ID,
							(commit ? XLOG_XACT_COMMIT : XLOG_XACT_ABORT) | XLOG_XACT_HAS_INFO, 803,
							0x2000, 0x2100);
		if (commit)
			record_append(record, &commit_record, MinSizeOfXactCommit);
		else
			record_append(record, &abort_record, MinSizeOfXactAbort);
		if (variant == 0) /* A's case: no xinfo at all */
			;
		else if (variant == 1 || variant == 5) /* xinfo cut short */
			record_append(record, junk, 2);
		else if (variant == 2) { /* relation count beyond the data */
			xinfo.xinfo = XACT_XINFO_HAS_RELFILELOCATORS;
			record_append(record, &xinfo, sizeof(xinfo));
			record_append(record, &count, sizeof(count));
			record_append(record, &locator, sizeof(locator));
		} else if (variant == 3) { /* bytes after the last section */
			xinfo.xinfo = XACT_XINFO_HAS_RELFILELOCATORS;
			count = 1;
			record_append(record, &xinfo, sizeof(xinfo));
			record_append(record, &count, sizeof(count));
			record_append(record, &locator, sizeof(locator));
			record_append(record, junk, sizeof(junk));
		} else if (variant == 4) { /* a section only a COMMIT carries */
			xl_xact_tt_commit tt = { 0 };

			xinfo.xinfo = XACT_XINFO_HAS_TT_COMMIT;
			record_append(record, &xinfo, sizeof(xinfo));
			record_append(record, &tt, sizeof(tt));
		} else { /* variant 6: subtransaction count beyond the data */
			xinfo.xinfo = XACT_XINFO_HAS_SUBXACTS;
			record_append(record, &xinfo, sizeof(xinfo));
			record_append(record, &count, sizeof(count));
			record_append(record, &sub, sizeof(sub));
		}
		UT_ASSERT(compute_for_with_diagnostics(self, &cut, &detail, &diagnostics)
				  != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(detail != RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(cut.lower, InvalidXLogRecPtr);
		UT_ASSERT(diagnostics.native_effects_incomplete > 0);
		if (variant == 0) /* reaches the native fallback, which refuses it */
			UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE);
		if (ut_current_failed)
			printf("# damaged transaction end variant %d\n", variant);
	}
}

/* R-A18 with the real undo decoder: a TT slot stamped again after the
 * completion keeps its earlier stamps, another slot's do not. */
UT_TEST(test_records_tt_slot_stamps_are_keyed)
{
	for (int variant = 0; variant < 3; variant++) {
		uint32 self, peer;

		rec_two_writers(&self, &peer);
		rec_tt_commit(self, 0x1400, 0x1440, 801, 4);
		rec_tt_commit(self, 0x1800, 0x1840, 802, 5);
		if (variant == 0)
			rec_tt_commit(self, 0x3200, 0x3240, 803, 4);
		else if (variant == 1)
			rec_tt_commit(self, 0x3200, 0x3240, 803, 9);
		expect_cut(self, variant == 0 ? 0x1400 : REC_SELF_REDO,
				   variant == 0 ? CLUSTER_WAL_RETAINED_PIN_SIDE : CLUSTER_WAL_RETAINED_PIN_NONE,
				   variant == 0 ? RF_SIDE_CONTRIBUTION_UNDO_HEADER : 0);
		if (ut_current_failed)
			printf("# tt stamp variant %d\n", variant);
	}
}


/* Retiring the directory's PI first-record pin must not retire the original
 * TT or structure owner. The actual census recomputes both source cuts. */
UT_TEST(test_pi_pin_removal_keeps_independent_side_and_structure)
{
	for (int structure = 0; structure < 2; structure++) {
		uint32 self, peer;
		ClusterWalRetainedCutV1 before, after;
		RfPageProofDetailV1 detail;
		static const RelFileNumber drops[] = { 16384 };
		rec_two_writers(&self, &peer);
		rec_commit(self, 0x1800, 0x1900, 801, structure ? drops : NULL, structure ? 1 : 0,
				   structure, structure ? 0 : 3);
		if (!structure)
			rec_commit(self, 0x3200, 0x3300, 802, NULL, 0, false, 0);
		record_pi_floor.floor = 0x1400;
		record_pi_floor.bounded = 1;
		UT_ASSERT_EQ(compute_for(self, &before, &detail), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(before.lower, 0x1400);
		memset(&record_pi_floor, 0, sizeof(record_pi_floor));
		UT_ASSERT_EQ(compute_for(self, &after, &detail), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(after.local_pi_bounded, 0);
		UT_ASSERT_EQ(after.lower, 0x1800);
		UT_ASSERT_EQ(after.pin, structure ? CLUSTER_WAL_RETAINED_PIN_STRUCTURE
										  : CLUSTER_WAL_RETAINED_PIN_SIDE);
		UT_ASSERT(after.lower < after.native_redo);
	}
}

#endif /* TEST_CLUSTER_WAL_RETAINED_CUT_RECORDS_CASES_H */
