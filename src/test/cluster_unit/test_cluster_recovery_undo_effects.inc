/* Original undo WAL retained as owned cold effect observations.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */

static uint64
undo_effect_fixture(unsigned shape, uint32 segment)
{
	uint64 required = 1;
	uint8 instance = (segment - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
#define UNDO_FIX(op_, rec_, extra_)                                                                \
	do {                                                                                           \
		fixture(RM_CLUSTER_UNDO_ID, (op_));                                                        \
		append(&(rec_), sizeof(rec_));                                                             \
		if ((extra_) != 0) {                                                                       \
			char body[BLCKSZ];                                                                     \
			memset(body, 0xa7, sizeof(body));                                                      \
			append(body, (extra_));                                                                \
		}                                                                                          \
	} while (0)

	switch (shape) {
	case 0: {
		xl_cluster_undo_segment_init r = { 0 };
		r.segment_id = segment;
		r.instance = instance;
		UNDO_FIX(XLOG_UNDO_SEGMENT_INIT, r, BLCKSZ);
		break;
	}
	case 1: {
		xl_undo_tt_slot_bind r = { 0 };
		r.segment_id = segment;
		r.instance = instance;
		r.xid = 4195136;
		r.wrap = 1;
		r.format_version = CLUSTER_UNDO_TT_BIND_VERSION;
		UNDO_FIX(XLOG_UNDO_TT_SLOT_BIND, r, 0);
		break;
	}
	case 2: {
		xl_undo_tt_slot_commit r = { 0 };
		r.segment_id = segment;
		r.instance = instance;
		r.xid = 4195136;
		r.wrap = 1;
		r.commit_scn = 42;
		UNDO_FIX(XLOG_UNDO_TT_SLOT_COMMIT, r, 0);
		break;
	}
	case 3: {
		xl_undo_segment_recycle r = { 0 };
		r.segment_id = segment;
		r.instance = instance;
		r.old_state = SEGMENT_COMMITTED;
		r.new_state = SEGMENT_RECYCLABLE;
		UNDO_FIX(XLOG_UNDO_SEGMENT_RECYCLE, r, 0);
		break;
	}
	case 4: {
		xl_undo_segment_reuse r = { 0 };
		r.segment_id = segment;
		r.instance = instance;
		r.new_generation = 1;
		UNDO_FIX(XLOG_UNDO_SEGMENT_REUSE, r, BLCKSZ);
		break;
	}
	case 5: {
		xl_undo_tt_slot_abort r = { 0 };
		r.segment_id = segment;
		r.instance = instance;
		r.xid = 4195136;
		r.wrap = 1;
		UNDO_FIX(XLOG_UNDO_TT_SLOT_ABORT, r, 0);
		break;
	}
	case 6: {
		xl_undo_tt_slot_abort_exact r = { 0 };
		r.segment_id = segment;
		r.instance = instance;
		r.xid = 4195136;
		r.wrap = 1;
		r.format_version = CLUSTER_UNDO_TT_ABORT_EXACT_VERSION;
		UNDO_FIX(XLOG_UNDO_TT_SLOT_ABORT, r, 0);
		break;
	}
	case 7: {
		xl_undo_block_write r = { 0 };
		r.segment_id = segment;
		r.instance = instance;
		r.block_no = 9;
		r.has_fpi = 1;
		UNDO_FIX(XLOG_UNDO_BLOCK_WRITE, r, BLCKSZ);
		required = 10;
		break;
	}
	case 8: {
		xl_undo_block_write_multi r = { 0 };
		r.segment_id = segment;
		r.instance = instance;
		r.block_no = 12;
		r.has_fpi = 1;
		UNDO_FIX(XLOG_UNDO_BLOCK_WRITE_MULTI, r, BLCKSZ);
		required = 13;
		break;
	}
	case 9: {
		xl_undo_tt_slot_set_head r = { 0 };
		r.segment_id = segment;
		r.instance = instance;
		r.xid = 4195136;
		r.wrap = 1;
		r.first_undo_block = uba_encode(segment, 7, 0, 0);
		UNDO_FIX(XLOG_UNDO_TT_SLOT_SET_HEAD, r, 0);
		required = 8;
		break;
	}
	default: {
		xl_undo_tt_slot_ctrc_release_v1 r = { 0 };
		uint8 encoded[CLUSTER_UNDO_TT_CTRC_RELEASE_BYTES];
		r.segment_id = segment;
		r.owner_instance = instance;
		r.xid = 4195136;
		r.slot_wrap = 1;
		r.cluster_epoch = 1;
		r.root_id = 1;
		r.root_generation = 1;
		r.formation_epoch = 1;
		r.admission_record_generation = 1;
		r.seal_generation = 1;
		r.terminal_status = TT_SLOT_COMMITTED;
		r.ack_set_digest[0] = 1;
		r.format_version = CLUSTER_UNDO_TT_CTRC_RELEASE_VERSION;
		r.flags = CLUSTER_UNDO_TT_CTRC_RELEASE_ALL_TOUCHED_ACKED;
		fixture(RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_CTRC_RELEASE);
		UT_ASSERT(cluster_undo_tt_ctrc_release_encode(&r, encoded));
		append(encoded, sizeof(encoded));
		break;
	}
	}
#undef UNDO_FIX
	return required;
}


UT_TEST(test_cold_undo_effects_own_all_original_shapes)
{
	for (unsigned shape = 0; shape < 11; shape++) {
		ClusterRecoveryNativeScan *scan = NULL;
		ClusterRecoveryNativeEffect effect;
		ClusterUndoDecoded original;
		char saved[2 * BLCKSZ];
		uint32 length;
		undo_effect_fixture(shape, 257);
		length = decoded.main_data_len;
		memcpy(saved, payload.bytes, length);
		UT_ASSERT(cluster_undo_decode(&reader, &original));
		UT_ASSERT_EQ(cluster_recovery_native_scan_begin(&reader, &source, &cut, 128 * 1024, &scan),
					 CLUSTER_RECOVERY_NATIVE_MORE);
		if (scan == NULL)
			continue;
		memset(payload.bytes, 0, sizeof(payload.bytes));
		memset(&source, 0, sizeof(source));
		memset(&cut, 0, sizeof(cut));
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
					 CLUSTER_RECOVERY_NATIVE_EFFECT);
		UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_UNDO);
		UT_ASSERT_EQ(effect.length, sizeof(ClusterUndoDecoded));
		UT_ASSERT_EQ(memcmp(effect.bytes, &original, sizeof(original)), 0);
		UT_ASSERT_EQ(effect.record->payload_length, length);
		UT_ASSERT_EQ(memcmp(effect.record->payload, saved, length), 0);
		UT_ASSERT_EQ(effect.record->source.claim.identity.origin_thread_id, 2);
		UT_ASSERT_EQ(effect.record->read_lsn, 0x3000);
		UT_ASSERT_EQ(effect.record->record_origin, 17);
		UT_ASSERT_EQ(effect.record->header.xl_xid, 4195136);
		UT_ASSERT_EQ(cluster_recovery_native_scan_bytes(scan), MAXALIGN(sizeof(*scan)) + length);
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_END);
		UT_ASSERT(effect.record == NULL && effect.bytes == NULL);
		cluster_recovery_native_scan_destroy(scan);
	}
}

UT_TEST(test_cold_undo_effects_reject_truncated_and_mixed_records)
{
	for (unsigned shape = 0; shape < 11; shape++) {
		uint32 length;
		undo_effect_fixture(shape, 257);
		length = decoded.main_data_len;
		for (unsigned bad = 0; bad < 5; bad++) {
			ClusterRecoveryNativeScan *scan = NULL;
			undo_effect_fixture(shape, 257);
			switch (bad) {
			case 0:
				decoded.main_data_len = length - 1;
				break;
			case 1:
				decoded.main_data_len = 1;
				break;
			case 2:
				decoded.main_data_len = length + 1;
				break;
			case 3:
				decoded.max_block_id = 0;
				break;
			case 4:
				decoded.has_page_version_edge = true;
				break;
			}
			UT_ASSERT(cluster_recovery_native_scan_begin(&reader, &source, &cut, 128 * 1024, &scan)
					  > CLUSTER_RECOVERY_NATIVE_END);
			UT_ASSERT(scan == NULL);
		}
	}
}

UT_TEST(test_cold_undo_effects_require_source_and_bounded_target)
{
	for (unsigned bad = 0; bad < 9; bad++) {
		xl_undo_tt_slot_bind b = { 0 };
		ClusterRecoveryNativeScan *scan = NULL;
		undo_effect_fixture(1, 257);
		memcpy(&b, payload.bytes, sizeof(b));
		switch (bad) {
		case 0:
			b.instance = 1;
			b.segment_id = 1;
			break; /* valid other-owner shape */
		case 1:
			b.segment_id = 0;
			break;
		case 2:
			b.wrap = TT_WRAP_INVALID;
			break;
		case 3:
			b.xid = InvalidTransactionId;
			break;
		case 4:
			b.slot_offset = TT_SLOTS_PER_SEGMENT;
			break;
		case 5:
			b.format_version++;
			break;
		case 6:
			b.segment_generation = UINT32_MAX;
			break;
		case 7:
			cut.scan_end_exclusive = reader.EndRecPtr - 1;
			break;
		case 8:
			source.claim.database_incarnation = 0;
			break;
		}
		memcpy(payload.bytes, &b, sizeof(b));
		UT_ASSERT(cluster_recovery_native_scan_begin(&reader, &source, &cut, 128 * 1024, &scan)
				  > CLUSTER_RECOVERY_NATIVE_END);
		UT_ASSERT(scan == NULL);
	}
	for (unsigned multi = 0; multi < 2; multi++) {
		ClusterRecoveryNativeScan *scan = NULL;
		undo_effect_fixture(7 + multi, 257);
		if (multi)
			((xl_undo_block_write_multi *)payload.bytes)->block_no = UINT32_MAX;
		else
			((xl_undo_block_write *)payload.bytes)->block_no = UINT32_MAX;
		UT_ASSERT(cluster_recovery_native_scan_begin(&reader, &source, &cut, 128 * 1024, &scan)
				  > CLUSTER_RECOVERY_NATIVE_END);
		UT_ASSERT(scan == NULL);
	}
}

UT_TEST(test_cold_undo_effects_budget_and_recheck_are_sticky)
{
	for (unsigned shape = 0; shape < 11; shape++) {
		ClusterRecoveryNativeScan *scan = NULL;
		ClusterRecoveryNativeEffect effect;
		size_t bytes;
		undo_effect_fixture(shape, 257);
		bytes = MAXALIGN(sizeof(*scan)) + decoded.main_data_len;
		UT_ASSERT_EQ(cluster_recovery_native_scan_begin(&reader, &source, &cut, bytes - 1, &scan),
					 CLUSTER_RECOVERY_NATIVE_CAPACITY);
		UT_ASSERT(scan == NULL);
		UT_ASSERT_EQ(cluster_recovery_native_scan_begin(&reader, &source, &cut, bytes, &scan),
					 CLUSTER_RECOVERY_NATIVE_MORE);
		if (scan == NULL)
			continue;
		UT_ASSERT_EQ(cluster_recovery_native_scan_recheck(scan, &reader, &source, &cut),
					 CLUSTER_RECOVERY_NATIVE_UNCHANGED);
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
					 CLUSTER_RECOVERY_NATIVE_EFFECT);
		decoded.main_data[decoded.main_data_len - 1] ^= 1;
		UT_ASSERT_EQ(cluster_recovery_native_scan_recheck(scan, &reader, &source, &cut),
					 CLUSTER_RECOVERY_NATIVE_CHANGED);
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
					 CLUSTER_RECOVERY_NATIVE_CHANGED);
		UT_ASSERT(effect.record == NULL && effect.bytes == NULL);
		undo_effect_fixture(shape, 257);
		UT_ASSERT_EQ(cluster_recovery_native_scan_recheck(scan, &reader, &source, &cut),
					 CLUSTER_RECOVERY_NATIVE_CHANGED);
		cluster_recovery_native_scan_destroy(scan);
	}
}

UT_TEST(test_cold_undo_effect_keeps_delta_and_unknown_generation)
{
	xl_undo_block_write b = { 0 };
	ClusterRecoveryNativeScan *scan = NULL;
	ClusterRecoveryNativeEffect effect;
	ClusterUndoDecoded undo;
	char bytes[UNDO_BLOCK_HDR_PREFIX_LEN + 16 + sizeof(UndoSlotDirEntry)];
	b.instance = 2;
	b.segment_id = 257;
	b.block_no = 2;
	b.rec_off = sizeof(UndoBlockHeader);
	b.rec_len = 16;
	b.slot_off = BLCKSZ - sizeof(UndoSlotDirEntry);
	memset(bytes, 0xa6, sizeof(bytes));
	fixture(RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE);
	append(&b, sizeof(b));
	append(bytes, sizeof(bytes));
	UT_ASSERT_EQ(cluster_recovery_native_scan_begin(&reader, &source, &cut, 128 * 1024, &scan),
				 CLUSTER_RECOVERY_NATIVE_MORE);
	if (scan != NULL) {
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
					 CLUSTER_RECOVERY_NATIVE_EFFECT);
		memcpy(&undo, effect.bytes, sizeof(undo));
		UT_ASSERT(!undo.has_fpi && undo.has_payload);
		UT_ASSERT_EQ(undo.payload_length, sizeof(bytes));
		UT_ASSERT_EQ(memcmp(effect.record->payload + undo.payload_offset, bytes, sizeof(bytes)), 0);
		cluster_recovery_native_scan_destroy(scan);
	}
	undo_effect_fixture(9, 257); /* legacy SET_HEAD has no canonical generation */
	scan = NULL;
	UT_ASSERT_EQ(cluster_recovery_native_scan_begin(&reader, &source, &cut, 128 * 1024, &scan),
				 CLUSTER_RECOVERY_NATIVE_MORE);
	if (scan != NULL) {
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
					 CLUSTER_RECOVERY_NATIVE_EFFECT);
		memcpy(&undo, effect.bytes, sizeof(undo));
		UT_ASSERT_EQ(undo.kind, CLUSTER_UNDO_KIND_TT_SET_HEAD);
		UT_ASSERT_EQ(undo.format_version, 0);
		UT_ASSERT_EQ(memcmp(&undo.first_undo_block,
							&((xl_undo_tt_slot_set_head *)payload.bytes)->first_undo_block,
							sizeof(UBA)),
					 0);
		cluster_recovery_native_scan_destroy(scan);
	}
}
