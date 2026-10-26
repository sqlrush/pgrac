/*-------------------------------------------------------------------------
 * test_cluster_undo_rollback.inc
 *    Real rollback-control bytes in backend and FRONTEND.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef UNDO_SAVEPOINT_ROLLBACK_VERSION
/* Parent adapters only expose the missing codec as behavioral rejection. */
#define UNDO_SAVEPOINT_ROLLBACK_VERSION 1
#define UNDO_SAVEPOINT_ROLLBACK_LENGTH 96
#define UNDO_SAVEPOINT_ROLLBACK_PAYLOAD_LENGTH 144
#define UNDO_RECORD_SAVEPOINT_ROLLBACK 5
#define UNDO_SAVEPOINT_ROLLBACK_RESERVED 1
#define UNDO_SAVEPOINT_ROLLBACK_PENDING 2
#define UNDO_SAVEPOINT_ROLLBACK_MARKING 3
#define UNDO_SAVEPOINT_ROLLBACK_COMPLETE 4
typedef struct UndoSavepointRollbackPayload {
	uint16 version;
	uint16 length;
	uint8 phase;
	uint8 reserved[3];
	uint64 rollback_sequence;
	UBA start_uba;
	UBA stop_uba;
	UBA next_uba;
	uint64 scope_child_fxid;
	XLogRecPtr last_effect_wal_lsn;
	uint64 visited_count;
	uint64 reserved_end;
} UndoSavepointRollbackPayload;

static bool
rollback_codec_parent_decode(const UndoRecordHeader *record, const void *payload, size_t bytes,
							 UndoSavepointRollbackPayload *body, UndoOperationTrailer *operation)
{
	uint16 old_body;
	UndoItlHistoryTrailer history;

	if (body != NULL)
		memset(body, 0, sizeof(*body));
	if (operation != NULL)
		memset(operation, 0, sizeof(*operation));
	return cluster_undo_record_decode_payload(record, payload, bytes, &old_body, &history);
}

static bool
rollback_codec_parent_encode(const UndoRecordHeader *record pg_attribute_unused(),
							 const UndoSavepointRollbackPayload *body pg_attribute_unused(),
							 const UndoOperationTrailer *operation pg_attribute_unused(),
							 void *payload pg_attribute_unused(),
							 size_t bytes pg_attribute_unused())
{
	return false;
}
#define cluster_undo_record_decode_rollback_payload rollback_codec_parent_decode
#define cluster_undo_record_encode_rollback_payload rollback_codec_parent_encode
#endif

typedef struct RollbackCodecFixture {
	UndoRecordHeader record;
	char payload[144];
} RollbackCodecFixture;

/* Literal offsets and values, independent of the serializer and C padding. */
static void
rollback_codec_init(RollbackCodecFixture *f, uint8 phase, bool has_stop)
{
	uint16 version = 1;
	uint16 length = 96;
	uint16 operation_length = 48;
	uint64 sequence = 17;
	uint64 top = (UINT64_C(5) << 32) | 100;
	uint64 child = (UINT64_C(5) << 32) | 101;
	uint64 operation_sequence = 9;
	uint32 command = 7;
	uint32 subid = 2;
	uint64 visited = phase == 4 ? 3 : phase == 3 ? 2 : 0;
	uint64 lsn = visited == 0 ? 0 : 400;
	UBA start = InvalidUba_init;
	UBA stop = InvalidUba_init;
	UBA next = InvalidUba_init;

	memset(f, 0, sizeof(*f));
	f->record.record_type = 5;
	f->record.flags = UNDO_REC_FLAG_HAS_OPERATION | UNDO_REC_FLAG_FIRST_IN_TX;
	f->record.payload_length = 144;
	f->record.xid = 101;
	f->record.tt_slot_segment_id = 1;
	f->record.tt_slot_id = 1;
	f->record.tt_wrap_plus1 = 1;
	f->record.write_scn = 201;
	if (phase != 1) {
		start = uba_encode(1, 9, 0, 2);
		if (has_stop)
			stop = uba_encode(2, 4, 0, 0);
		next = phase == 2 ? start : phase == 3 ? uba_encode(3, 8, 0, 1) : stop;
		f->record.prev_uba = start;
		f->record.flags &= ~UNDO_REC_FLAG_FIRST_IN_TX;
	}
	memcpy(f->payload, &version, 2);
	memcpy(f->payload + 2, &length, 2);
	f->payload[4] = phase;
	memcpy(f->payload + 8, &sequence, 8);
	memcpy(f->payload + 16, &start, 16);
	memcpy(f->payload + 32, &stop, 16);
	memcpy(f->payload + 48, &next, 16);
	memcpy(f->payload + 64, &child, 8);
	memcpy(f->payload + 72, &lsn, 8);
	memcpy(f->payload + 80, &visited, 8);
	memcpy(f->payload + 96, &version, 2);
	memcpy(f->payload + 98, &operation_length, 2);
	f->payload[100] = 1;
	memcpy(f->payload + 104, &top, 8);
	memcpy(f->payload + 112, &child, 8);
	memcpy(f->payload + 120, &operation_sequence, 8);
	memcpy(f->payload + 132, &command, 4);
	memcpy(f->payload + 136, &subid, 4);
}

static void
rollback_codec_unpack(const RollbackCodecFixture *f, UndoSavepointRollbackPayload *body,
					  UndoOperationTrailer *operation)
{
	memcpy(body, f->payload, 96);
	memcpy(operation, f->payload + 96, 48);
}

static void
rollback_codec_pack(RollbackCodecFixture *f, const UndoSavepointRollbackPayload *body,
					const UndoOperationTrailer *operation)
{
	memcpy(f->payload, body, 96);
	memcpy(f->payload + 96, operation, 48);
}

static bool
rollback_codec_refused(const UndoRecordHeader *record, const void *payload, size_t bytes)
{
	UndoSavepointRollbackPayload body;
	UndoSavepointRollbackPayload zero_body = { 0 };
	UndoOperationTrailer operation;
	UndoOperationTrailer zero_operation = { 0 };
	bool result;

	memset(&body, 0xA5, sizeof(body));
	memset(&operation, 0xA5, sizeof(operation));
	result = cluster_undo_record_decode_rollback_payload(record, payload, bytes, &body, &operation);
	return !result && memcmp(&body, &zero_body, sizeof(body)) == 0
		   && memcmp(&operation, &zero_operation, sizeof(operation)) == 0;
}

UT_TEST(test_rollback_codec_reserved_unaligned_zero_progress)
{
	RollbackCodecFixture f;
	UndoSavepointRollbackPayload body;
	UndoOperationTrailer operation;
	char storage[152];

	rollback_codec_init(&f, 1, false);
	for (unsigned offset = 1; offset <= 7; offset++) {
		memset(storage, 0xA5, sizeof(storage));
		memcpy(storage + offset, f.payload, 144);
		UT_ASSERT(cluster_undo_record_decode_rollback_payload(&f.record, storage + offset, 144,
															  &body, &operation));
		UT_ASSERT_EQ(body.version, 1);
		UT_ASSERT_EQ(body.length, 96);
		UT_ASSERT_EQ(body.phase, 1);
		UT_ASSERT_EQ(body.rollback_sequence, 17);
		UT_ASSERT_EQ(body.scope_child_fxid, (UINT64_C(5) << 32) | 101);
		UT_ASSERT(UBA_is_invalid(body.start_uba));
		UT_ASSERT(UBA_is_invalid(body.stop_uba));
		UT_ASSERT(UBA_is_invalid(body.next_uba));
		UT_ASSERT_EQ(body.last_effect_wal_lsn, 0);
		UT_ASSERT_EQ(body.visited_count, 0);
		UT_ASSERT_EQ(operation.canonical_top_fxid, (UINT64_C(5) << 32) | 100);
		UT_ASSERT_EQ(operation.operation_sequence, 9);
		UT_ASSERT_EQ(operation.command_id, 7);
		UT_ASSERT_EQ(operation.subid, 2);
		UT_ASSERT_EQ(storage[0], (char)0xA5);
		UT_ASSERT_EQ(storage[151], (char)0xA5);
	}
}

UT_TEST(test_rollback_codec_active_phases_and_cross_segment_stops)
{
	for (uint8 phase = 2; phase <= 4; phase++) {
		for (unsigned has_stop = 0; has_stop < 2; has_stop++) {
			RollbackCodecFixture f;
			UndoSavepointRollbackPayload body;
			UndoOperationTrailer operation;
			UBA start = uba_encode(1, 9, 0, 2);
			UBA stop = has_stop ? uba_encode(2, 4, 0, 0) : (UBA)InvalidUba_init;
			UBA next = phase == 2 ? start : phase == 3 ? uba_encode(3, 8, 0, 1) : stop;

			rollback_codec_init(&f, phase, has_stop != 0);
			UT_ASSERT(cluster_undo_record_decode_rollback_payload(&f.record, f.payload, 144, &body,
																  &operation));
			UT_ASSERT_EQ(body.phase, phase);
			UT_ASSERT_EQ(memcmp(&body.start_uba, &start, 16), 0);
			UT_ASSERT_EQ(memcmp(&body.stop_uba, &stop, 16), 0);
			UT_ASSERT_EQ(memcmp(&body.next_uba, &next, 16), 0);
			UT_ASSERT_EQ(body.visited_count, phase == 2 ? 0 : phase == 3 ? 2 : 3);
			UT_ASSERT_EQ(body.last_effect_wal_lsn, phase == 2 ? 0 : 400);
			UT_ASSERT_EQ(operation.effect_state, CLUSTER_SAVEPOINT_EFFECT_LIVE);
		}
	}
}

UT_TEST(test_rollback_codec_marking_initial_and_last_cursor_cut)
{
	for (unsigned leg = 0; leg < 2; leg++) {
		RollbackCodecFixture f;
		UndoSavepointRollbackPayload body;
		UndoOperationTrailer operation;

		rollback_codec_init(&f, 3, false);
		rollback_codec_unpack(&f, &body, &operation);
		body.next_uba = leg == 0 ? body.start_uba : body.stop_uba;
		body.visited_count = leg == 0 ? 0 : 3;
		body.last_effect_wal_lsn = leg == 0 ? 0 : 400;
		rollback_codec_pack(&f, &body, &operation);
		UT_ASSERT(cluster_undo_record_decode_rollback_payload(&f.record, f.payload, 144, &body,
															  &operation));
		UT_ASSERT_EQ(body.phase, 3);
	}
}

UT_TEST(test_rollback_codec_full_xid_wrap_and_counter_boundaries)
{
	RollbackCodecFixture f;
	UndoSavepointRollbackPayload body;
	UndoOperationTrailer operation;

	rollback_codec_init(&f, 4, true);
	rollback_codec_unpack(&f, &body, &operation);
	operation.canonical_top_fxid = (UINT64_C(8) << 32) | PG_UINT32_MAX;
	operation.operation_fxid = (UINT64_C(9) << 32) | 3;
	operation.operation_sequence = PG_UINT64_MAX;
	operation.canonical_segment_generation = PG_UINT32_MAX - 1;
	operation.command_id = InvalidCommandId - 1;
	operation.subid = PG_UINT32_MAX;
	body.scope_child_fxid = operation.operation_fxid;
	body.rollback_sequence = PG_UINT64_MAX;
	body.visited_count = PG_UINT64_MAX;
	f.record.xid = 3;
	rollback_codec_pack(&f, &body, &operation);
	UT_ASSERT(
		cluster_undo_record_decode_rollback_payload(&f.record, f.payload, 144, &body, &operation));
	UT_ASSERT_EQ(body.rollback_sequence, PG_UINT64_MAX);
	UT_ASSERT_EQ(body.visited_count, PG_UINT64_MAX);
	UT_ASSERT_EQ(operation.operation_fxid, (UINT64_C(9) << 32) | 3);
}

UT_TEST(test_rollback_codec_rejects_body_domain_drift)
{
	for (unsigned leg = 0; leg < 20; leg++) {
		RollbackCodecFixture f;
		UndoSavepointRollbackPayload body;
		UndoOperationTrailer operation;

		rollback_codec_init(&f, 1, false);
		rollback_codec_unpack(&f, &body, &operation);
		switch (leg) {
		case 0:
			body.version = 0;
			break;
		case 1:
			body.version = 2;
			break;
		case 2:
			body.length = 95;
			break;
		case 3:
			body.length = 97;
			break;
		case 4:
			body.phase = 0;
			break;
		case 5:
			body.phase = 5;
			break;
		case 6:
			body.rollback_sequence = 0;
			break;
		case 7:
			body.scope_child_fxid = 0;
			break;
		case 8:
			body.scope_child_fxid++;
			break;
		default:
			break;
		}
		rollback_codec_pack(&f, &body, &operation);
		if (leg >= 9)
			f.payload[leg < 12 ? 5 + leg - 9 : 88 + leg - 12] = 1;
		UT_ASSERT(rollback_codec_refused(&f.record, f.payload, 144));
	}
}

UT_TEST(test_rollback_codec_rejects_owner_tail_drift)
{
	for (unsigned leg = 0; leg < 24; leg++) {
		RollbackCodecFixture f;
		UndoSavepointRollbackPayload body;
		UndoOperationTrailer operation;

		rollback_codec_init(&f, 2, true);
		rollback_codec_unpack(&f, &body, &operation);
		switch (leg) {
		case 0:
			operation.version = 0;
			break;
		case 1:
			operation.version = 2;
			break;
		case 2:
			operation.length = 47;
			break;
		case 3:
			operation.length = 49;
			break;
		case 4:
			operation.effect_state = 0;
			break;
		case 5:
			operation.effect_state = CLUSTER_SAVEPOINT_EFFECT_DISCARDED;
			break;
		case 6:
			operation.effect_state = 3;
			break;
		case 7:
			operation.reserved[0] = 1;
			break;
		case 8:
			operation.reserved[1] = 1;
			break;
		case 9:
			operation.reserved[2] = 1;
			break;
		case 10:
			operation.reserved_end = 1;
			break;
		case 11:
			operation.canonical_top_fxid = 0;
			break;
		case 12:
			operation.canonical_top_fxid = (UINT64_C(5) << 32) | 2;
			break;
		case 13:
			operation.canonical_top_fxid = operation.operation_fxid;
			break;
		case 14:
			operation.canonical_top_fxid = operation.operation_fxid + 1;
			break;
		case 15:
			operation.operation_fxid = 0;
			break;
		case 16:
			operation.operation_fxid = (UINT64_C(5) << 32) | 2;
			break;
		case 17:
			operation.operation_fxid--;
			break;
		case 18:
			operation.operation_sequence = 0;
			break;
		case 19:
			operation.canonical_segment_generation = PG_UINT32_MAX;
			break;
		case 20:
			operation.command_id = InvalidCommandId;
			break;
		case 21:
			operation.subid = TopSubTransactionId;
			break;
		case 22:
			operation.subid = 0;
			break;
		case 23:
			operation.operation_fxid += UINT64_C(1) << 32;
			break;
		}
		rollback_codec_pack(&f, &body, &operation);
		UT_ASSERT(rollback_codec_refused(&f.record, f.payload, 144));
	}
}

UT_TEST(test_rollback_codec_rejects_header_domain_drift)
{
	for (unsigned leg = 0; leg < 30; leg++) {
		RollbackCodecFixture f;

		rollback_codec_init(&f, 2, true);
		switch (leg) {
		case 0:
		case 1:
			f.record.record_type = leg;
			break;
		case 2:
			f.record.record_type = 4;
			break;
		case 3:
			f.record.record_type = 6;
			break;
		case 4:
			f.record.flags = 0;
			break;
		case 5:
			f.record.flags |= UNDO_REC_FLAG_HAS_ITL_HISTORY;
			break;
		case 6:
			f.record.flags |= UNDO_REC_FLAG_CONTINUED;
			break;
		case 7:
			f.record.flags |= UNDO_REC_FLAG_TOAST;
			break;
		case 8:
			f.record.flags |= 0x80;
			break;
		case 9:
			f.record.flags |= UNDO_REC_FLAG_FIRST_IN_TX;
			break;
		case 10:
			f.record.payload_length = 143;
			break;
		case 11:
			f.record.payload_length = 145;
			break;
		case 12:
			f.record.xid = 0;
			break;
		case 13:
			f.record.xid++;
			break;
		case 14:
			f.record.origin_node_id = SCN_MAX_VALID_NODE_ID + 1;
			break;
		case 15:
			f.record.origin_node_id = 1;
			break;
		case 16:
			f.record.tt_slot_segment_id = 0;
			break;
		case 17:
			f.record.tt_slot_segment_id = CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
			break;
		case 18:
			f.record.tt_slot_id = 0;
			break;
		case 19:
			f.record.tt_slot_id = TT_SLOTS_PER_SEGMENT + 1;
			break;
		case 20:
			f.record.tt_wrap_plus1 = 0;
			break;
		case 21:
			f.record.write_scn = InvalidScn;
			break;
		case 22:
			f.record.target_locator.spcOid = 1;
			break;
		case 23:
			f.record.target_locator.dbOid = 1;
			break;
		case 24:
			f.record.target_locator.relNumber = 1;
			break;
		case 25:
			f.record.target_fork = FSM_FORKNUM;
			break;
		case 26:
			f.record.target_block = 1;
			break;
		case 27:
			f.record.target_offset = 1;
			break;
		case 28:
			f.record.prev_uba = (UBA)InvalidUba_init;
			break;
		case 29:
			f.record.prev_uba = uba_encode(1, 8, 0, 1);
			break;
		}
		UT_ASSERT(rollback_codec_refused(&f.record, f.payload, 144));
	}
}

UT_TEST(test_rollback_codec_rejects_nonrecord_or_foreign_cursor_addresses)
{
	for (unsigned field = 0; field < 3; field++) {
		for (unsigned leg = 0; leg < 8; leg++) {
			RollbackCodecFixture f;
			UndoSavepointRollbackPayload body;
			UndoOperationTrailer operation;
			UBA bad = uba_encode(1, 8, 0, 1);

			rollback_codec_init(&f, 3, true);
			rollback_codec_unpack(&f, &body, &operation);
			switch (leg) {
			case 0:
				bad.raw[0] = 1;
				break; /* TT-only block zero */
			case 1:
				bad.raw[0] = ((uint64)UNDO_BLOCKS_PER_SEGMENT << 32) | 1;
				break;
			case 2:
				bad.raw[0] &= ~UINT64_C(0xFFFFFFFF);
				break;
			case 3:
				bad.raw[0] += CLUSTER_UNDO_SEGS_PER_INSTANCE;
				break;
			case 4:
				bad.raw[1] = (UINT64_C(1) << 16) | 1;
				break;
			case 5:
				bad.raw[1] = TT_SLOTS_PER_SEGMENT;
				break;
			case 6:
				bad.raw[1] = (uint64)((BLCKSZ - sizeof(UndoBlockHeader)) / sizeof(UndoSlotDirEntry))
							 << 16;
				break;
			case 7:
				bad.raw[1] |= UINT64_C(1) << 32;
				break;
			}
			if (field == 0)
				f.record.prev_uba = body.start_uba = bad;
			else if (field == 1)
				body.stop_uba = bad;
			else
				body.next_uba = bad;
			rollback_codec_pack(&f, &body, &operation);
			UT_ASSERT(rollback_codec_refused(&f.record, f.payload, 144));
		}
	}
}

UT_TEST(test_rollback_codec_rejects_phase_progress_contradictions)
{
	for (unsigned leg = 0; leg < 18; leg++) {
		RollbackCodecFixture f;
		UndoSavepointRollbackPayload body;
		UndoOperationTrailer operation;

		rollback_codec_init(&f, leg < 6 ? 1 : leg < 12 ? 2 : leg < 15 ? 3 : 4, true);
		rollback_codec_unpack(&f, &body, &operation);
		switch (leg) {
		case 0:
			body.start_uba = uba_encode(1, 9, 0, 2);
			break;
		case 1:
			body.stop_uba = uba_encode(1, 9, 0, 2);
			break;
		case 2:
			body.next_uba = uba_encode(1, 9, 0, 2);
			break;
		case 3:
			body.last_effect_wal_lsn = 400;
			break;
		case 4:
			body.visited_count = 1;
			break;
		case 5:
			f.record.flags &= ~UNDO_REC_FLAG_FIRST_IN_TX;
			break;
		case 6:
			body.stop_uba = body.start_uba;
			break;
		case 7:
			body.start_uba = (UBA)InvalidUba_init;
			break;
		case 8:
			body.next_uba = body.stop_uba;
			break;
		case 9:
			body.visited_count = 1;
			break;
		case 10:
			body.last_effect_wal_lsn = 400;
			break;
		case 11:
			body.visited_count = 1;
			body.last_effect_wal_lsn = 400;
			break;
		case 12:
			body.last_effect_wal_lsn = 0;
			break;
		case 13:
			body.visited_count = 0;
			break;
		case 14:
			body.next_uba = (UBA)InvalidUba_init;
			break;
		case 15:
			body.next_uba = body.start_uba;
			break;
		case 16:
			body.visited_count = 0;
			break;
		case 17:
			body.last_effect_wal_lsn = 0;
			break;
		}
		rollback_codec_pack(&f, &body, &operation);
		UT_ASSERT(rollback_codec_refused(&f.record, f.payload, 144));
	}
}

UT_TEST(test_rollback_codec_short_null_and_missing_outputs)
{
	RollbackCodecFixture f;
	UndoSavepointRollbackPayload body;
	UndoSavepointRollbackPayload zero_body = { 0 };
	UndoOperationTrailer operation;
	UndoOperationTrailer zero_operation = { 0 };

	rollback_codec_init(&f, 1, false);
	for (size_t bytes = 0; bytes < 144; bytes++) {
		UT_ASSERT(rollback_codec_refused(&f.record, f.payload, bytes));
		f.record.payload_length = bytes;
		UT_ASSERT(rollback_codec_refused(&f.record, f.payload, bytes));
		f.record.payload_length = 144;
	}
	UT_ASSERT(rollback_codec_refused(&f.record, f.payload, 145));
	UT_ASSERT(rollback_codec_refused(NULL, f.payload, 144));
	UT_ASSERT(rollback_codec_refused(&f.record, NULL, 144));
	memset(&body, 0xA5, sizeof(body));
	UT_ASSERT(!cluster_undo_record_decode_rollback_payload(&f.record, f.payload, 144, &body, NULL));
	UT_ASSERT_EQ(memcmp(&body, &zero_body, sizeof(body)), 0);
	memset(&operation, 0xA5, sizeof(operation));
	UT_ASSERT(
		!cluster_undo_record_decode_rollback_payload(&f.record, f.payload, 144, NULL, &operation));
	UT_ASSERT_EQ(memcmp(&operation, &zero_operation, sizeof(operation)), 0);
}

UT_TEST(test_rollback_encoder_exact_bytes_and_capacity_before_copy)
{
	for (uint8 phase = 1; phase <= 4; phase++) {
		RollbackCodecFixture f;
		UndoSavepointRollbackPayload body;
		UndoOperationTrailer operation;
		unsigned char output[146];
		unsigned char unchanged[146];

		rollback_codec_init(&f, phase, true);
		rollback_codec_unpack(&f, &body, &operation);
		memset(output, 0xA5, sizeof(output));
		memcpy(unchanged, output, sizeof(output));
		for (size_t capacity = 0; capacity < 144; capacity++) {
			UT_ASSERT(!cluster_undo_record_encode_rollback_payload(&f.record, &body, &operation,
																   output + 1, capacity));
			UT_ASSERT_EQ(memcmp(output, unchanged, sizeof(output)), 0);
		}
		UT_ASSERT(cluster_undo_record_encode_rollback_payload(&f.record, &body, &operation,
															  output + 1, 144));
		UT_ASSERT_EQ(memcmp(output + 1, f.payload, 144), 0);
		UT_ASSERT_EQ(output[0], 0xA5);
		UT_ASSERT_EQ(output[145], 0xA5);
		memcpy(output, unchanged, sizeof(output));
		UT_ASSERT(cluster_undo_record_encode_rollback_payload(&f.record, &body, &operation,
															  output + 1, 145));
		UT_ASSERT_EQ(memcmp(output + 1, f.payload, 144), 0);
		UT_ASSERT_EQ(output[0], 0xA5);
		UT_ASSERT_EQ(output[145], 0xA5);
		body.phase = 0;
		memcpy(output, unchanged, sizeof(output));
		UT_ASSERT(!cluster_undo_record_encode_rollback_payload(&f.record, &body, &operation,
															   output + 1, 144));
		UT_ASSERT_EQ(memcmp(output, unchanged, sizeof(output)), 0);
	}
}

UT_TEST(test_rollback_codec_legacy_data_consumers_stay_closed)
{
	RollbackCodecFixture f;
	UndoItlHistoryTrailer history;
	UndoOperationTrailer operation;
	uint16 body_length = 99;
	char output[144];
	char unchanged[144];
	UndoSavepointRollbackPayload body;

	rollback_codec_init(&f, 1, false);
	UT_ASSERT(
		!cluster_undo_record_decode_payload(&f.record, f.payload, 144, &body_length, &history));
	UT_ASSERT_EQ(body_length, 0);
	UT_ASSERT(!cluster_undo_record_decode_operation_payload(&f.record, f.payload, 144, &body_length,
															&history, &operation));
	f.record.flags &= ~UNDO_REC_FLAG_HAS_OPERATION;
	f.record.payload_length = 96;
	UT_ASSERT(
		!cluster_undo_record_decode_payload(&f.record, f.payload, 96, &body_length, &history));
	rollback_codec_init(&f, 1, false);
	rollback_codec_unpack(&f, &body, &operation);
	memset(output, 0xA5, sizeof(output));
	memcpy(unchanged, output, sizeof(output));
	UT_ASSERT(!cluster_undo_record_encode_rollback_payload(NULL, &body, &operation, output, 144));
	UT_ASSERT(
		!cluster_undo_record_encode_rollback_payload(&f.record, NULL, &operation, output, 144));
	UT_ASSERT(!cluster_undo_record_encode_rollback_payload(&f.record, &body, NULL, output, 144));
	UT_ASSERT(
		!cluster_undo_record_encode_rollback_payload(&f.record, &body, &operation, NULL, 144));
	UT_ASSERT_EQ(memcmp(output, unchanged, sizeof(output)), 0);
}
