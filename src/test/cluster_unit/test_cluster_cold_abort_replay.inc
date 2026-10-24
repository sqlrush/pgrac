/*-------------------------------------------------------------------------
 * test_cluster_cold_abort_replay.inc
 *    Native ABORT capture and selected replay through the real cold plans.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_cold_abort_replay.inc
 * NOTES
 *    PGRAC-original. Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *-------------------------------------------------------------------------
 */

/* Real decode -> immutable SIDE capture -> cold sequencer composition.
 * Native CREATE/SPACE I/O, authority and TT terminal preconditions are fixture
 * boundaries. This proves callback order and counted completion, not E1 or
 * permission to delete a relation. No page or relation file is opened. */
#define ABORT_REPLAY_RECORDS 7
#define ABORT_REPLAY_LSN(i) (UINT64_C(0x1000) + (i) * UINT64_C(0x100))

sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

/* PG_TRY's selected-record preflight also links the prepared dependency
 * branches. None is an eligible consumer in this ordinary ABORT fixture. */
uint64
GetSystemIdentifier(void)
{
	UT_ASSERT(false);
	return 99;
}

int
cluster_xid_origin_slot(TransactionId xid pg_attribute_unused())
{
	UT_ASSERT(false);
	return -1;
}

bool
cluster_tt_slot_durable_read_exact_stable(uint32 segment_id pg_attribute_unused(),
										  uint16 slot_offset pg_attribute_unused(),
										  TransactionId xid pg_attribute_unused(),
										  uint16 expected_wrap pg_attribute_unused(),
										  TTSlot *slot_out pg_attribute_unused())
{
	UT_ASSERT(false);
	return false;
}

TwoPhaseRecoveryPendingResult
TwoPhaseRecoveryPendingReadExact(TransactionId xid pg_attribute_unused(),
								 Oid database pg_attribute_unused(),
								 const char *gid pg_attribute_unused(),
								 void **content_out pg_attribute_unused(),
								 uint32 *len_out pg_attribute_unused())
{
	UT_ASSERT(false);
	return TWOPHASE_RECOVERY_PENDING_BLOCKED;
}

typedef struct AbortReplayFixture {
	ClusterColdPlanV1 *cold;
	RfSideOnlinePlanV1 *side;
	ClusterColdParticipantV1 cuts[2];
	RfSideOnlineApplyOpsV1 apply;
	uint32 next;
	uint32 index;
	uint32 applied;
	uint32 preflighted;
	uint32 begins;
	uint32 ends;
	uint32 prepares;
	uint32 finals;
	uint32 scheduled;
	uint32 consumed;
	bool drained[2];
	int fault;
	EffectRecord current;
} AbortReplayFixture;

static void
abort_replay_record(EffectRecord *r, uint32 index, bool missing_creation)
{
	if (index == 5) {
		completion(r, false,
				   XACT_XINFO_HAS_SCN | XACT_XINFO_HAS_RELFILELOCATORS | XACT_XINFO_HAS_SUBXACTS
					   | XACT_XINFO_HAS_AE_LOCKS | XACT_XINFO_HAS_DROPPED_STATS);
		r->decoded.header.xl_info |= XLR_SPECIAL_REL_UPDATE;
	} else if (index >= 1 && index <= 4 && !(missing_creation && index == 4)) {
		RelFileLocator locator = { DEFAULTTABLESPACE_OID, 5, 123 + (index - 1) / 2 };

		if (index % 2) {
			xl_smgr_create create = { locator, MAIN_FORKNUM };

			record_init(r, RM_SMGR_ID, XLOG_SMGR_CREATE | XLR_SPECIAL_REL_UPDATE);
			append(r, &create, sizeof(create));
		} else {
			ClusterSpaceStructureChange create = { 0 };
			uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];

			create.identity.action = CLUSTER_SPACE_WAL_CREATE;
			create.identity.nblocks = InvalidBlockNumber;
			create.identity.result.key.system_identifier = 99;
			memcpy(create.identity.result.key.storage_uuid, uuid, sizeof(uuid));
			create.identity.result.key.database_incarnation = 4;
			create.identity.result.key.locator = locator;
			memset(create.identity.result.incarnation, 0x31, 16);
			create.identity.result.sequence = 1;
			create.identity.result.operation = 7;
			create.identity.result.state = CLUSTER_SPACE_IDENTITY_LIVE;
			create.identity.result_token = 17;
			create.reservation.action = CLUSTER_SPACE_RESERVATION_INIT;
			create.reservation.result.identity = create.identity.result;
			create.reservation.result_token = 17;
			UT_ASSERT(cluster_space_structure_wal_encode(&create, wal, sizeof(wal)));
			record_init(r, RM_SMGR_ID, XLOG_SMGR_SPACE_IDENTITY | XLR_SPECIAL_REL_UPDATE);
			append(r, wal, sizeof(wal));
		}
	} else {
		Oid next_oid = 18000 + index;

		record_init(r, RM_XLOG_ID, XLOG_NEXTOID);
		r->decoded.header.xl_xid = InvalidTransactionId;
		append(r, &next_oid, sizeof(next_oid));
	}
	r->reader.ReadRecPtr = ABORT_REPLAY_LSN(index);
	r->reader.EndRecPtr = ABORT_REPLAY_LSN(index + 1);
	r->reader.system_identifier = 99;
	r->decoded.lsn = r->reader.ReadRecPtr;
	r->decoded.next_lsn = r->reader.EndRecPtr;
	r->decoded.header.xl_prev = index ? ABORT_REPLAY_LSN(index - 1) : 0xfc0;
	r->decoded.header.xl_crc = 100 + index;
}

static RfPageOnlineRecordIdentityV1
abort_replay_identity(const EffectRecord *r)
{
	RfPageOnlineRecordIdentityV1 identity = { 0 };

	identity.record.system_identifier = 99;
	memcpy(identity.record.storage_uuid, uuid, sizeof(uuid));
	identity.record.origin_thread = 1;
	identity.record.timeline_id = 1;
	identity.record.read_rec_ptr = r->reader.ReadRecPtr;
	identity.record.end_rec_ptr = r->reader.EndRecPtr;
	identity.record.record_crc = r->decoded.header.xl_crc;
	identity.record.rmid = r->decoded.header.xl_rmid;
	identity.record.info = r->decoded.header.xl_info;
	return identity;
}

static bool
abort_replay_observe(void *arg pg_attribute_unused(),
					 const RfPageIdentityV1 *page pg_attribute_unused(),
					 ClusterColdDataV1 *out pg_attribute_unused())
{
	UT_ASSERT(false); /* This cut has no ordinary PAGE component. */
	return false;
}

/* Physical SPACE observation is the existing owner's fixture boundary. */
static bool
abort_replay_space_check(void *arg pg_attribute_unused(), const RelFileLocator *locator,
						 const ClusterColdSpaceInputV1 *inputs pg_attribute_unused(), uint32 count,
						 uint32 *order)
{
	UT_ASSERT(locator->relNumber == 123 || locator->relNumber == 124);
	UT_ASSERT_EQ(count, 1);
	for (uint32 i = 0; i < count; i++)
		order[i] = i;
	return true;
}

static void
abort_replay_capture(AbortReplayFixture *h, bool missing_creation)
{
	RfContributorStreamCutV1 cut = { .failed_thread = 1,
									 .flags = RF_CONTRIBUTOR_CUT_COMPLETE,
									 .timeline_id = 1,
									 .scan_begin_inclusive = ABORT_REPLAY_LSN(0),
									 .scan_end_exclusive = ABORT_REPLAY_LSN(ABORT_REPLAY_RECORDS),
									 .origin_owner_incarnation = 12 };
	RfSideOnlinePlanRequestV1 request = { .system_identifier = 99,
										  .physical_cuts = &cut,
										  .participant_count = 1,
										  .memory_budget = 1024 * 1024,
										  .purpose = RF_SIDE_PLAN_RECOVERY };
	EffectRecord scratch;
	ClusterColdDiagV1 diag;

	memcpy(request.storage_uuid, uuid, sizeof(uuid));
	h->cuts[0] = (ClusterColdParticipantV1){ .thread_id = 2,
											 .timeline = 1,
											 .owner_incarnation = 11,
											 .physical_lower = ABORT_REPLAY_LSN(0),
											 .native_redo = ABORT_REPLAY_LSN(0),
											 .tail_end = ABORT_REPLAY_LSN(0) };
	h->cuts[1] = (ClusterColdParticipantV1){ .thread_id = 1,
											 .timeline = 1,
											 .owner_incarnation = 12,
											 .physical_lower = cut.scan_begin_inclusive,
											 .native_redo = cut.scan_begin_inclusive,
											 .tail_end = cut.scan_end_exclusive };
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(h->cuts, 2, 8 * 1024 * 1024, &h->cold),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_set_space_check_v1(h->cold, abort_replay_space_check, NULL),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &h->side), RF_PAGE_PROOF_DETAIL_OK);
	for (uint32 i = 0; i < ABORT_REPLAY_RECORDS; i++) {
		ClusterColdDecodedV1 decoded = { 0 };
		RfPageOnlineRecordIdentityV1 identity;

		/* Reuse/overwrite the reader and decoder storage just as the scanner does. */
		abort_replay_record(&scratch, i, missing_creation);
		identity = abort_replay_identity(&scratch);
		UT_ASSERT_EQ(
			cluster_cold_recovery_decode_v1(&scratch.reader, 99, uuid, true, true, &decoded),
			CLUSTER_COLD_OK);
		if (i == 5)
			UT_ASSERT_EQ(decoded.record.record_flags, CLUSTER_COLD_RECORD_FOREIGN_SIDE);
		UT_ASSERT_EQ(cluster_cold_plan_feed_v1(h->cold, 1, &decoded.record), CLUSTER_COLD_OK);
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(h->side, &decoded.detached, &identity),
					 RF_PAGE_PROOF_DETAIL_OK);
		cluster_cold_decoded_release_v1(&decoded);
	}
	memset(&scratch, 0xa5, sizeof(scratch));
	UT_ASSERT(rf_side_online_plan_bind_database_v1(h->side, 4));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(h->side),
				 missing_creation ? RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE : RF_PAGE_PROOF_DETAIL_OK);
	if (!missing_creation) {
		UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(h->side), ABORT_REPLAY_RECORDS);
		UT_ASSERT_EQ(cluster_cold_plan_seal_v1(h->cold, abort_replay_observe, NULL, &diag),
					 CLUSTER_COLD_OK);
	}
}

static bool
abort_replay_begin(void *arg)
{
	AbortReplayFixture *h = arg;

	UT_ASSERT_EQ(h->begins, h->ends);
	h->begins++;
	h->preflighted = 0;
	return true;
}

static void
abort_replay_end(void *arg, bool complete)
{
	AbortReplayFixture *h = arg;

	UT_ASSERT(!complete && !h->drained[1]);
	h->ends++;
}

static bool
abort_replay_preflight(void *arg, const RfSideOnlineOperationV1 *op)
{
	AbortReplayFixture *h = arg;
	uint32 index = (op->identity.record.read_rec_ptr - ABORT_REPLAY_LSN(0)) / 0x100;

	UT_ASSERT(index < ABORT_REPLAY_RECORDS && op->owned_payload != NULL);
	h->preflighted |= 1U << index;
	return !(h->fault == 3 && index == 6);
}

static bool
abort_replay_prepare(void *arg)
{
	AbortReplayFixture *h = arg;

	UT_ASSERT_EQ(h->preflighted, (1U << ABORT_REPLAY_RECORDS) - 1);
	h->prepares++;
	return true;
}

static bool
abort_replay_whole_xact(void *arg pg_attribute_unused(),
						const RfSideOnlineOperationV1 *op pg_attribute_unused())
{
	UT_ASSERT(false); /* Selected ABORT cannot substitute whole-source apply. */
	return false;
}

static bool
abort_replay_xact(void *arg, uint32 index, const RfSideOnlineOperationV1 *op)
{
	AbortReplayFixture *h = arg;
	EffectRecord expected;

	abort_replay_record(&expected, 5, false);
	UT_ASSERT_EQ(index, 5);
	UT_ASSERT_EQ(h->applied, (1U << 5) - 1);
	UT_ASSERT_EQ(op->xact.kind, RF_SIDE_XACT_ABORT);
	UT_ASSERT_EQ(op->xact.relation_count, 2);
	UT_ASSERT(op->xact.cold_completion_effects);
	UT_ASSERT_EQ(op->owned_payload_length, expected.decoded.main_data_len);
	UT_ASSERT(memcmp(op->owned_payload, expected.data.bytes, op->owned_payload_length) == 0);
	UT_ASSERT(rf_side_xact_cold_completion_payload_v1(&op->xact, op->owned_payload,
													  op->owned_payload_length));
	if (h->fault == 1)
		return false;
	h->applied |= 1U << index;
	return true;
}

static bool
abort_replay_control(void *arg, const RfSideOnlineOperationV1 *op)
{
	AbortReplayFixture *h = arg;
	Oid next_oid;

	UT_ASSERT(h->index == 0 || h->index == 6);
	UT_ASSERT_EQ(op->kind, RF_SIDE_ONLINE_OPERATION_NATIVE_CONTROL);
	UT_ASSERT_EQ(op->owned_payload_length, sizeof(next_oid));
	memcpy(&next_oid, op->owned_payload, sizeof(next_oid));
	UT_ASSERT_EQ(next_oid, 18000 + h->index);
	UT_ASSERT_EQ(h->applied, (1U << h->index) - 1);
	if (h->fault == 2 && h->index == 6)
		return false;
	h->applied |= 1U << h->index;
	return true;
}

static bool
abort_replay_next(void *arg, uint32 participant, ClusterColdReplayRecordV1 *out)
{
	AbortReplayFixture *h = arg;

	if (participant == 0 || h->next == ABORT_REPLAY_RECORDS || (h->fault == 4 && h->next == 6))
		return false;
	h->index = h->next++;
	abort_replay_record(&h->current, h->index, false);
	if (h->fault == 5 && h->index == 5)
		h->current.decoded.header.xl_crc++;
	*out = (ClusterColdReplayRecordV1){ .read_rec_ptr = h->current.reader.ReadRecPtr,
										.end_rec_ptr = h->current.reader.EndRecPtr,
										.record_crc = h->current.decoded.header.xl_crc,
										.rmid = h->current.decoded.header.xl_rmid,
										.info = h->current.decoded.header.xl_info };
	return true;
}

static bool
abort_replay_consume(void *arg, uint32 participant)
{
	AbortReplayFixture *h = arg;
	ClusterColdDecodedV1 decoded = { 0 };
	RfPageOnlineRecordIdentityV1 identity = abort_replay_identity(&h->current);
	bool accepted;

	h->consumed++;
	UT_ASSERT_EQ(participant, 1);
	UT_ASSERT_EQ(
		cluster_cold_recovery_decode_v1(&h->current.reader, 99, uuid, true, true, &decoded),
		CLUSTER_COLD_OK);
	if (h->index >= 1 && h->index <= 4) {
		/* Native and typed CREATE execute at their existing I/O-owner boundary. */
		UT_ASSERT_EQ(h->applied, (1U << h->index) - 1);
		h->applied |= 1U << h->index;
		accepted = true;
	} else {
		UT_ASSERT(cluster_cold_replay_effects_v1(&decoded, NULL, false));
		accepted = rf_side_online_plan_apply_record_v1(h->side, h->index, &identity, &h->apply)
				   == RF_PAGE_PROOF_DETAIL_OK;
	}
	cluster_cold_decoded_release_v1(&decoded);
	return accepted;
}

static bool
abort_replay_scheduled(void *arg, uint32 participant, const ClusterColdStepV1 *step,
					   ClusterColdPageActionV1 action pg_attribute_unused())
{
	AbortReplayFixture *h = arg;

	UT_ASSERT_EQ(step->read_rec_ptr, ABORT_REPLAY_LSN(h->index));
	UT_ASSERT(h->index == 2 || h->index == 4);
	h->scheduled++;
	return abort_replay_consume(arg, participant);
}

static bool
abort_replay_drained(void *arg, uint32 participant, const ClusterColdReplayResultV1 *progress,
					 uint64 records)
{
	AbortReplayFixture *h = arg;

	UT_ASSERT_EQ(records, participant ? ABORT_REPLAY_RECORDS : 0);
	if (participant) {
		UT_ASSERT_EQ(h->applied, (1U << ABORT_REPLAY_RECORDS) - 1);
		UT_ASSERT_EQ(progress->last_read[1], ABORT_REPLAY_LSN(6));
		UT_ASSERT_EQ(progress->last_end[1], ABORT_REPLAY_LSN(7));
		UT_ASSERT_EQ(progress->last_crc[1], 106);
		if (h->fault == 6)
			return false;
	}
	h->drained[participant] = true;
	return true;
}

static bool
abort_replay_final(void *arg, uint32 relation pg_attribute_unused())
{
	AbortReplayFixture *h = arg;

	UT_ASSERT(h->drained[0] && h->drained[1]);
	UT_ASSERT_EQ(h->applied, (1U << ABORT_REPLAY_RECORDS) - 1);
	h->finals++;
	return true;
}

static ClusterColdReplayDetailV1
abort_replay_run(AbortReplayFixture *h, ClusterColdReplayResultV1 *result)
{
	const ClusterColdReplayOpsV1 ops = { .next = abort_replay_next,
										 .unscheduled = abort_replay_consume,
										 .scheduled = abort_replay_scheduled,
										 .source_drained = abort_replay_drained,
										 .space_final = abort_replay_final };

	h->apply = (RfSideOnlineApplyOpsV1){ .arg = h,
										 .begin_protected_set = abort_replay_begin,
										 .end_protected_set = abort_replay_end,
										 .preflight_xact = abort_replay_preflight,
										 .apply_xact = abort_replay_whole_xact,
										 .preflight_space = abort_replay_preflight,
										 .apply_space = abort_replay_whole_xact,
										 .preflight_control = abort_replay_preflight,
										 .apply_control = abort_replay_control,
										 .source_thread = 1,
										 .prepare_apply = abort_replay_prepare,
										 .apply_xact_record = abort_replay_xact };
	return cluster_cold_replay_run_v1(h->cold, h->cuts, 2, 0, &ops, h, result);
}

static void
abort_replay_release(AbortReplayFixture *h)
{
	cluster_cold_plan_destroy_v1(&h->cold);
	rf_side_online_plan_destroy_v1(&h->side);
}

UT_TEST(test_relation_abort_capture_replays_and_drains_complete_cut)
{
	AbortReplayFixture h = { 0 };
	ClusterColdReplayResultV1 result;

	abort_replay_capture(&h, false);
	UT_ASSERT_EQ(abort_replay_run(&h, &result), CLUSTER_COLD_REPLAY_OK);
	UT_ASSERT_EQ(result.steps_done, 2);
	UT_ASSERT_EQ(h.scheduled, 2);
	UT_ASSERT_EQ(h.begins, 3);
	UT_ASSERT_EQ(h.ends, 3);
	UT_ASSERT_EQ(h.prepares, 3);
	UT_ASSERT(h.drained[0] && h.drained[1]);
	UT_ASSERT_EQ(h.finals, 2);
	abort_replay_release(&h);
}

UT_TEST(test_relation_abort_capture_refuses_missing_creation)
{
	AbortReplayFixture h = { 0 };

	abort_replay_capture(&h, true);
	UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(h.side), 0);
	UT_ASSERT_EQ(h.applied | h.begins | h.finals, 0);
	UT_ASSERT(!h.drained[0] && !h.drained[1]);
	abort_replay_release(&h);
}

UT_TEST(test_relation_abort_replay_failure_keeps_final_drain_closed)
{
	for (int fault = 1; fault <= 6; fault++) {
		AbortReplayFixture h = { .fault = fault };
		ClusterColdReplayResultV1 result;
		ClusterColdReplayDetailV1 expected = CLUSTER_COLD_REPLAY_CALLBACK;

		abort_replay_capture(&h, false);
		if (fault == 4)
			expected = CLUSTER_COLD_REPLAY_CUT_DIFFERS;
		else if (fault == 5)
			expected = CLUSTER_COLD_REPLAY_IDENTITY;
		UT_ASSERT_EQ(abort_replay_run(&h, &result), expected);
		UT_ASSERT(!h.drained[1]);
		UT_ASSERT_EQ(h.finals, 0);
		UT_ASSERT_EQ(h.begins, h.ends);
		if (fault == 5) {
			/* Changed ABORT CRC is rejected before the SIDE consumer. */
			UT_ASSERT_EQ(h.consumed, 5);
			UT_ASSERT_EQ(result.participant, 1);
			UT_ASSERT_EQ(result.rec_ptr, ABORT_REPLAY_LSN(5));
			UT_ASSERT_EQ(result.last_read[1], ABORT_REPLAY_LSN(4));
			UT_ASSERT_EQ(result.last_crc[1], 104);
		}
		if (fault == 3) {
			UT_ASSERT_EQ(h.applied, 0);
			UT_ASSERT_EQ(h.prepares, 0);
		} else if (fault == 1 || fault == 5)
			UT_ASSERT_EQ(h.applied, (1U << 5) - 1);
		else if (fault == 2 || fault == 4)
			UT_ASSERT_EQ(h.applied, (1U << 6) - 1);
		else
			UT_ASSERT_EQ(h.applied, (1U << ABORT_REPLAY_RECORDS) - 1);
		abort_replay_release(&h);
	}
}
