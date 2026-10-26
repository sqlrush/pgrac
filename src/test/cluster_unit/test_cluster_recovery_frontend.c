/* Recovery record headers must compile and link without backend services. */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"
#ifndef FRONTEND
#error "This regression must be built as a frontend"
#endif

#include "cluster/cluster_recovery_ctrc_record.h"

#undef printf
#undef fprintf
#include "unit_test.h"

/* Exact current backend pure comparator, with only its symbol renamed. */
#include "test_cluster_recovery_frontend_compare.inc"

UT_DEFINE_GLOBALS();

#include "test_cluster_undo_operation.inc"
#include "test_cluster_undo_rollback.inc"

UT_TEST(test_frontend_time_comparison_matches_original_backend)
{
	const uint64 times[] = {0, 1, 255, SCN_MAX_LOCAL - 1, SCN_MAX_LOCAL};

	for (NodeId a = 0; a <= SCN_MAX_VALID_NODE_ID; a++)
		for (NodeId b = 0; b <= SCN_MAX_VALID_NODE_ID; b++)
			for (unsigned i = 0; i < lengthof(times); i++)
				for (unsigned j = 0; j < lengthof(times); j++) {
					SCN left = scn_encode(a, times[i]);
					SCN right = scn_encode(b, times[j]);

					UT_ASSERT_EQ(scn_time_cmp(left, right),
						test_backend_scn_time_cmp(left, right));
				}
}

UT_TEST(test_frontend_time_comparison_ignores_node_tie_break)
{
	UT_ASSERT_EQ(scn_time_cmp(scn_encode(127, 100), scn_encode(0, 100)), 0);
	UT_ASSERT_EQ(scn_time_cmp(scn_encode(127, 99), scn_encode(0, 100)), -1);
	UT_ASSERT_EQ(scn_time_cmp(scn_encode(0, 101), scn_encode(127, 100)), 1);
	UT_ASSERT_EQ(scn_time_cmp(InvalidScn, scn_encode(127, 1)), -1);
}

UT_TEST(test_frontend_original_undo_history_guards_keep_time_order)
{
	UndoRecordHeader record = {0};
	UndoInsertPayload insert = {0};
	UndoItlHistoryTrailer history = {0}, decoded;
	uint8 payload[sizeof(insert) + sizeof(history)];
	uint16 body;

	record.record_type = UNDO_RECORD_INSERT;
	record.flags = UNDO_REC_FLAG_FIRST_IN_TX | UNDO_REC_FLAG_HAS_ITL_HISTORY;
	record.payload_length = sizeof(payload);
	record.xid = 700;
	record.origin_node_id = 0;
	record.tt_slot_segment_id = 1;
	record.tt_slot_id = 1;
	record.tt_wrap_plus1 = 1;
	record.target_locator.relNumber = 20000;
	record.target_fork = MAIN_FORKNUM;
	record.target_block = 5;
	record.target_offset = 1;
	record.write_scn = scn_encode(0, 100);
	history.magic = UNDO_ITL_HISTORY_MAGIC;
	history.version = UNDO_ITL_HISTORY_VERSION;
	history.count = 1;
	history.entries[0].block = record.target_block;
	history.entries[0].after_kind = ITL_FLAG_ACTIVE;
	history.entries[0].after_write_scn = scn_encode(127, 100);
	memcpy(payload, &insert, sizeof(insert));
	memcpy(payload + sizeof(insert), &history, sizeof(history));
	UT_ASSERT(cluster_undo_record_decode_payload(&record, payload, sizeof(payload), &body, &decoded));
	UT_ASSERT_EQ(body, sizeof(insert));
	history.entries[0].after_write_scn = scn_encode(0, 101);
	memcpy(payload + sizeof(insert), &history, sizeof(history));
	UT_ASSERT(!cluster_undo_record_decode_payload(&record, payload, sizeof(payload), &body, &decoded));
	UT_ASSERT_EQ(body, 0);
	/* Prior history must be strictly earlier even across different node bits. */
	history.entries[0].after_write_scn = scn_encode(127, 100);
	history.entries[0].prior.flags = ITL_FLAG_ACTIVE;
	history.entries[0].prior.xid = 699;
	history.entries[0].prior.undo_segment_head = uba_encode(1, 2, 0, 0);
	for (unsigned time = 99; time <= 101; time++) {
		history.entries[0].prior.write_scn = scn_encode(0, time);
		memcpy(payload + sizeof(insert), &history, sizeof(history));
		UT_ASSERT_EQ(cluster_undo_record_decode_payload(&record, payload, sizeof(payload), &body, &decoded), time == 99);
	}
}

int
main(void)
{
	UT_PLAN(23);
	UT_RUN(test_rollback_codec_reserved_unaligned_zero_progress);
	UT_RUN(test_rollback_codec_active_phases_and_cross_segment_stops);
	UT_RUN(test_rollback_codec_marking_initial_and_last_cursor_cut);
	UT_RUN(test_rollback_codec_full_xid_wrap_and_counter_boundaries);
	UT_RUN(test_rollback_codec_rejects_body_domain_drift);
	UT_RUN(test_rollback_codec_rejects_owner_tail_drift);
	UT_RUN(test_rollback_codec_rejects_header_domain_drift);
	UT_RUN(test_rollback_codec_rejects_nonrecord_or_foreign_cursor_addresses);
	UT_RUN(test_rollback_codec_rejects_phase_progress_contradictions);
	UT_RUN(test_rollback_codec_short_null_and_missing_outputs);
	UT_RUN(test_rollback_encoder_exact_bytes_and_capacity_before_copy);
	UT_RUN(test_rollback_codec_legacy_data_consumers_stay_closed);
	UT_RUN(test_operation_codec_unaligned_live_and_discarded);
	UT_RUN(test_operation_codec_all_original_body_shapes);
	UT_RUN(test_operation_codec_epoch_and_counter_boundaries);
	UT_RUN(test_operation_codec_rejects_unproved_tail_fields);
	UT_RUN(test_operation_codec_rejects_record_domain_drift);
	UT_RUN(test_operation_codec_short_null_and_missing_outputs);
	UT_RUN(test_operation_codec_keeps_original_body_and_history_guards);
	UT_RUN(test_operation_codec_legacy_consumers_stay_closed);
	UT_RUN(test_frontend_time_comparison_matches_original_backend);
	UT_RUN(test_frontend_time_comparison_ignores_node_tie_break);
	UT_RUN(test_frontend_original_undo_history_guards_keep_time_order);
	UT_DONE();
	return ut_failed_count != 0;
}
