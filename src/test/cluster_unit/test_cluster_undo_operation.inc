/*-------------------------------------------------------------------------
 * test_cluster_undo_operation.inc
 *    Same payload contract in backend and FRONTEND, without backend services.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef UNDO_OPERATION_TRAILER_VERSION
/* Execute the original decoder on the parent for a behavioral RED. */
#define UNDO_OPERATION_TRAILER_VERSION 1
#define UNDO_OPERATION_TRAILER_LENGTH 48
#define UNDO_REC_FLAG_HAS_OPERATION 0x10
#define CLUSTER_SAVEPOINT_EFFECT_LIVE 1
#define CLUSTER_SAVEPOINT_EFFECT_DISCARDED 2
typedef struct UndoOperationTrailer {
	uint16 version;
	uint16 length;
	uint8 effect_state;
	uint8 reserved[3];
	uint64 canonical_top_fxid;
	uint64 operation_fxid;
	uint64 operation_sequence;
	uint32 canonical_segment_generation;
	CommandId command_id;
	SubTransactionId subid;
	uint32 reserved_end;
} UndoOperationTrailer;

static bool
operation_codec_parent_decode(const UndoRecordHeader *record, const void *payload, size_t bytes,
							  uint16 *body, UndoItlHistoryTrailer *history,
							  UndoOperationTrailer *operation)
{
	if (operation != NULL)
		memset(operation, 0, sizeof(*operation));
	return cluster_undo_record_decode_payload(record, payload, bytes, body, history);
}
#define cluster_undo_record_decode_operation_payload operation_codec_parent_decode
#endif

typedef struct OperationCodecFixture {
	UndoRecordHeader record;
	UndoItlHistoryTrailer history;
	UndoOperationTrailer operation;
	uint16 body_length;
	char storage[512];
} OperationCodecFixture;

static void
operation_codec_pack(OperationCodecFixture *f)
{
	char *payload = f->storage + 1;

	memcpy(payload + f->body_length, &f->history, sizeof(f->history));
	memcpy(payload + f->body_length + sizeof(f->history), &f->operation, sizeof(f->operation));
}

static void
operation_codec_init(OperationCodecFixture *f, UndoRecordType type)
{
	char *payload;

	memset(f, 0, sizeof(*f));
	payload = f->storage + 1;
	f->record.record_type = type;
	f->record.flags
		= UNDO_REC_FLAG_FIRST_IN_TX | UNDO_REC_FLAG_HAS_ITL_HISTORY | UNDO_REC_FLAG_HAS_OPERATION;
	f->record.xid = 101;
	f->record.tt_slot_segment_id = 1;
	f->record.tt_slot_id = 1;
	f->record.tt_wrap_plus1 = 1;
	f->record.write_scn = 201;
	f->record.target_locator.relNumber = 20000;
	f->record.target_fork = MAIN_FORKNUM;
	f->record.target_block = 37;
	f->record.target_offset = 1;
	f->history.magic = UNDO_ITL_HISTORY_MAGIC;
	f->history.version = UNDO_ITL_HISTORY_VERSION;
	f->history.count = 1;
	f->history.entries[0].block = 37;
	f->history.entries[0].after_kind = ITL_FLAG_ACTIVE;
	f->history.entries[0].after_write_scn = 200;
	f->history.entries[0].prior.wrap = 7;
	f->operation.version = UNDO_OPERATION_TRAILER_VERSION;
	f->operation.length = UNDO_OPERATION_TRAILER_LENGTH;
	f->operation.effect_state = CLUSTER_SAVEPOINT_EFFECT_LIVE;
	f->operation.canonical_top_fxid = 100;
	f->operation.operation_fxid = 101;
	f->operation.operation_sequence = 1;
	f->operation.subid = 2;
	switch (type) {
	case UNDO_RECORD_INSERT: {
		UndoInsertPayload insert = { 32, 0 };

		f->body_length = sizeof(insert);
		memcpy(payload, &insert, sizeof(insert));
		break;
	}
	case UNDO_RECORD_UPDATE: {
		UndoUpdatePayload update = { 38, 2, 32, sizeof(UndoUpdatePayload), 0 };

		f->body_length = sizeof(update) + update.old_tuple_length;
		memcpy(payload, &update, sizeof(update));
		f->history.count = 2;
		f->history.entries[1] = f->history.entries[0];
		f->history.entries[1].block = 38;
		break;
	}
	case UNDO_RECORD_DELETE: {
		UndoDeletePayload deleted = { 32, sizeof(UndoDeletePayload), 0 };

		f->body_length = sizeof(deleted) + deleted.full_tuple_length;
		memcpy(payload, &deleted, sizeof(deleted));
		break;
	}
	case UNDO_RECORD_ITL: {
		UndoItlPayload itl = { 0 };

		itl.new_flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
		itl.lock_xid = f->record.xid;
		f->body_length = sizeof(itl);
		memcpy(payload, &itl, sizeof(itl));
		f->history.entries[0].after_kind = ITL_FLAG_LOCK_ONLY_ACTIVE;
		break;
	}
	default:
		break;
	}
	f->record.payload_length = f->body_length + sizeof(f->history) + sizeof(f->operation);
	operation_codec_pack(f);
}

static bool
operation_codec_refused(const UndoRecordHeader *record, const void *payload, size_t bytes)
{
	UndoItlHistoryTrailer history, empty_history = { 0 };
	UndoOperationTrailer operation, empty_operation = { 0 };
	uint16 body = 123;
	bool result;

	memset(&history, 0xa5, sizeof(history));
	memset(&operation, 0xa5, sizeof(operation));
	result = cluster_undo_record_decode_operation_payload(record, payload, bytes, &body, &history,
														  &operation);
	return !result && body == 0 && memcmp(&history, &empty_history, sizeof(history)) == 0
		   && memcmp(&operation, &empty_operation, sizeof(operation)) == 0;
}

UT_TEST(test_operation_codec_unaligned_live_and_discarded)
{
	OperationCodecFixture f;
	UndoItlHistoryTrailer history;
	UndoOperationTrailer operation;
	uint16 body;

	UT_ASSERT_EQ(sizeof(UndoOperationTrailer), 48);
	UT_ASSERT_EQ(offsetof(UndoOperationTrailer, canonical_top_fxid), 8);
	UT_ASSERT_EQ(offsetof(UndoOperationTrailer, operation_fxid), 16);
	UT_ASSERT_EQ(offsetof(UndoOperationTrailer, operation_sequence), 24);
	UT_ASSERT_EQ(offsetof(UndoOperationTrailer, canonical_segment_generation), 32);
	UT_ASSERT_EQ(offsetof(UndoOperationTrailer, command_id), 36);
	UT_ASSERT_EQ(offsetof(UndoOperationTrailer, subid), 40);
	UT_ASSERT_EQ(offsetof(UndoOperationTrailer, reserved_end), 44);
	for (unsigned state = 1; state <= 2; state++) {
		operation_codec_init(&f, UNDO_RECORD_INSERT);
		f.operation.effect_state = state;
		operation_codec_pack(&f);
		UT_ASSERT(cluster_undo_record_decode_operation_payload(
			&f.record, f.storage + 1, f.record.payload_length, &body, &history, &operation));
		UT_ASSERT_EQ(body, f.body_length);
		UT_ASSERT_EQ(memcmp(&history, &f.history, sizeof(history)), 0);
		UT_ASSERT_EQ(memcmp(&operation, &f.operation, sizeof(operation)), 0);
	}
}

UT_TEST(test_operation_codec_all_original_body_shapes)
{
	for (unsigned type = UNDO_RECORD_UPDATE; type <= UNDO_RECORD_ITL; type++) {
		OperationCodecFixture f;
		UndoItlHistoryTrailer history;
		UndoOperationTrailer operation;
		uint16 body;

		operation_codec_init(&f, type);
		UT_ASSERT(cluster_undo_record_decode_operation_payload(
			&f.record, f.storage + 1, f.record.payload_length, &body, &history, &operation));
		UT_ASSERT_EQ(body, f.body_length);
		UT_ASSERT_EQ(history.count, type == UNDO_RECORD_UPDATE ? 2 : 1);
		UT_ASSERT_EQ(operation.operation_fxid, f.record.xid);
	}
}

UT_TEST(test_operation_codec_epoch_and_counter_boundaries)
{
	OperationCodecFixture f;
	UndoItlHistoryTrailer history;
	UndoOperationTrailer operation;
	uint16 body;

	operation_codec_init(&f, UNDO_RECORD_INSERT);
	f.operation.canonical_top_fxid = (UINT64_C(7) << 32) | PG_UINT32_MAX;
	f.operation.operation_fxid = (UINT64_C(8) << 32) | FirstNormalTransactionId;
	f.record.xid = FirstNormalTransactionId;
	f.operation.operation_sequence = PG_UINT64_MAX;
	f.operation.command_id = InvalidCommandId - 1;
	f.operation.subid = PG_UINT32_MAX;
	for (unsigned i = 0; i < 2; i++) {
		f.operation.canonical_segment_generation = i == 0 ? 0 : PG_UINT32_MAX - 1;
		operation_codec_pack(&f);
		UT_ASSERT(cluster_undo_record_decode_operation_payload(
			&f.record, f.storage + 1, f.record.payload_length, &body, &history, &operation));
		UT_ASSERT_EQ(operation.operation_sequence, PG_UINT64_MAX);
		UT_ASSERT_EQ(operation.canonical_top_fxid, f.operation.canonical_top_fxid);
		UT_ASSERT_EQ(operation.operation_fxid, f.operation.operation_fxid);
	}
}

UT_TEST(test_operation_codec_rejects_unproved_tail_fields)
{
	for (unsigned i = 0; i < 22; i++) {
		OperationCodecFixture f;

		operation_codec_init(&f, UNDO_RECORD_INSERT);
		switch (i) {
		case 0:
			f.operation.version = 0;
			break;
		case 1:
			f.operation.version++;
			break;
		case 2:
			f.operation.length--;
			break;
		case 3:
			f.operation.length++;
			break;
		case 4:
			f.operation.effect_state = 0;
			break;
		case 5:
			f.operation.effect_state = 3;
			break;
		case 6:
			f.operation.effect_state = 255;
			break;
		case 7:
			f.operation.reserved[0] = 1;
			break;
		case 8:
			f.operation.reserved[1] = 1;
			break;
		case 9:
			f.operation.reserved[2] = 1;
			break;
		case 10:
			f.operation.reserved_end = 1;
			break;
		case 11:
			f.operation.canonical_top_fxid = 0;
			break;
		case 12:
			f.operation.canonical_top_fxid = (UINT64_C(9) << 32) | 2;
			break;
		case 13:
			f.operation.operation_fxid = 0;
			break;
		case 14:
			f.operation.operation_fxid++;
			break;
		case 15:
			f.operation.canonical_top_fxid = f.operation.operation_fxid;
			break;
		case 16:
			f.operation.canonical_top_fxid = (UINT64_C(1) << 32) | 100;
			break;
		case 17:
			f.operation.operation_sequence = 0;
			break;
		case 18:
			f.operation.canonical_segment_generation = PG_UINT32_MAX;
			break;
		case 19:
			f.operation.command_id = InvalidCommandId;
			break;
		case 20:
			f.operation.subid = InvalidSubTransactionId;
			break;
		case 21:
			f.operation.subid = TopSubTransactionId;
			break;
		}
		operation_codec_pack(&f);
		UT_ASSERT(operation_codec_refused(&f.record, f.storage + 1, f.record.payload_length));
	}
}

UT_TEST(test_operation_codec_rejects_record_domain_drift)
{
	for (unsigned i = 0; i < 15; i++) {
		OperationCodecFixture f;

		operation_codec_init(&f, UNDO_RECORD_INSERT);
		switch (i) {
		case 0:
			f.record.record_type = UNDO_RECORD_INVALID;
			break;
		case 1:
			f.record.record_type = 5;
			break;
		case 2:
			f.record.record_type = 255;
			break;
		case 3:
			f.record.flags |= UNDO_REC_FLAG_CONTINUED;
			break;
		case 4:
			f.record.flags |= UNDO_REC_FLAG_TOAST;
			break;
		case 5:
			f.record.flags |= 0x80;
			break;
		case 6:
			f.record.flags &= ~UNDO_REC_FLAG_HAS_ITL_HISTORY;
			break;
		case 7:
			f.record.flags &= ~UNDO_REC_FLAG_HAS_OPERATION;
			break;
		case 8:
			f.record.flags &= ~UNDO_REC_FLAG_FIRST_IN_TX;
			break;
		case 9:
			f.record.tt_wrap_plus1 = 0;
			break;
		case 10:
			f.record.tt_slot_segment_id = 0;
			break;
		case 11:
			f.record.tt_slot_id = TT_SLOTS_PER_SEGMENT + 1;
			break;
		case 12:
			f.record.origin_node_id = 1;
			break;
		case 13:
			f.record.target_fork = VISIBILITYMAP_FORKNUM;
			break;
		case 14:
			f.record.target_offset = InvalidOffsetNumber;
			break;
		}
		UT_ASSERT(operation_codec_refused(&f.record, f.storage + 1, f.record.payload_length));
	}
}

UT_TEST(test_operation_codec_short_null_and_missing_outputs)
{
	OperationCodecFixture f;
	UndoItlHistoryTrailer history, empty_history = { 0 };
	UndoOperationTrailer operation, empty_operation = { 0 };
	uint16 body;
	uint16 full_length;

	operation_codec_init(&f, UNDO_RECORD_INSERT);
	full_length = f.record.payload_length;
	for (unsigned n = 0; n < full_length; n++) {
		f.record.payload_length = n;
		UT_ASSERT(operation_codec_refused(&f.record, f.storage + 1, n));
	}
	f.record.payload_length = full_length;
	UT_ASSERT(operation_codec_refused(&f.record, f.storage + 1, full_length - 1));
	UT_ASSERT(operation_codec_refused(&f.record, f.storage + 1, full_length + 1));
	UT_ASSERT(operation_codec_refused(NULL, f.storage + 1, full_length));
	UT_ASSERT(operation_codec_refused(&f.record, NULL, full_length));
	for (unsigned missing = 0; missing < 3; missing++) {
		body = 123;
		memset(&history, 0xa5, sizeof(history));
		memset(&operation, 0xa5, sizeof(operation));
		UT_ASSERT(!cluster_undo_record_decode_operation_payload(
			&f.record, f.storage + 1, full_length, missing == 0 ? NULL : &body,
			missing == 1 ? NULL : &history, missing == 2 ? NULL : &operation));
		UT_ASSERT(missing == 0 || body == 0);
		UT_ASSERT(missing == 1 || memcmp(&history, &empty_history, sizeof(history)) == 0);
		UT_ASSERT(missing == 2 || memcmp(&operation, &empty_operation, sizeof(operation)) == 0);
	}
}

UT_TEST(test_operation_codec_keeps_original_body_and_history_guards)
{
	for (unsigned i = 0; i < 12; i++) {
		OperationCodecFixture f;

		operation_codec_init(&f, i < 8 ? UNDO_RECORD_INSERT : UNDO_RECORD_UPDATE);
		switch (i) {
		case 0:
			f.history.magic++;
			break;
		case 1:
			f.history.version++;
			break;
		case 2:
			f.history.reserved = 1;
			break;
		case 3:
			f.history.count = 0;
			break;
		case 4:
			f.history.entries[0].block++;
			break;
		case 5:
			f.history.entries[0].after_write_scn = 202;
			break;
		case 6:
			f.history.entries[1].block = 1;
			break;
		case 7:
			f.storage[1 + offsetof(UndoInsertPayload, flags)] = 1;
			break;
		case 8:
			f.history.count = 1;
			break;
		case 9:
			f.history.entries[1].block = 37;
			break;
		case 10:
			f.storage[1 + offsetof(UndoUpdatePayload, old_tuple_offset)]++;
			break;
		case 11:
			f.storage[1 + offsetof(UndoUpdatePayload, old_tuple_length)]++;
			break;
		}
		operation_codec_pack(&f);
		UT_ASSERT(operation_codec_refused(&f.record, f.storage + 1, f.record.payload_length));
	}
	for (unsigned type = UNDO_RECORD_DELETE; type <= UNDO_RECORD_ITL; type++) {
		OperationCodecFixture f;

		operation_codec_init(&f, type);
		if (type == UNDO_RECORD_DELETE)
			f.storage[1 + offsetof(UndoDeletePayload, full_tuple_offset)]++;
		else
			f.storage[1 + offsetof(UndoItlPayload, lock_xid)]++;
		UT_ASSERT(operation_codec_refused(&f.record, f.storage + 1, f.record.payload_length));
	}
}

UT_TEST(test_operation_codec_legacy_consumers_stay_closed)
{
	OperationCodecFixture f;
	UndoItlHistoryTrailer history;
	ClusterTxLocator owner = { 0 };
	ClusterTxResolveReason reason;
	uint16 body;

	operation_codec_init(&f, UNDO_RECORD_INSERT);
	UT_ASSERT(!cluster_undo_record_decode_payload(&f.record, f.storage + 1, f.record.payload_length,
												  &body, &history));
	owner.uba = uba_encode(1, 1, 0, 0);
	owner.xid = 100;
	owner.itl_kind = ITL_FLAG_ACTIVE;
	UT_ASSERT(!cluster_undo_record_validate_member(&owner, owner.uba, &f.record, &reason));
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_XID_MISMATCH);
	/* No-tail V1 remains exclusively the original decoder's domain. */
	f.record.flags &= ~UNDO_REC_FLAG_HAS_OPERATION;
	f.record.payload_length -= sizeof(f.operation);
	f.record.xid = 100;
	UT_ASSERT(cluster_undo_record_decode_payload(&f.record, f.storage + 1, f.record.payload_length,
												 &body, &history));
	UT_ASSERT(operation_codec_refused(&f.record, f.storage + 1, f.record.payload_length));
}
