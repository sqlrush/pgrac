/*-------------------------------------------------------------------------
 *
 * test_cluster_side_undo.c
 *    RF-SIDE D-SIDE-02 focused unit tests: the TT/undo decode primitive
 *    (pure parse, malformed -> BLOCKED) and the preflight gate
 *    (route + field integrity -> zero mutation on any false).
 *
 *    RED mapping (spec §5.1 U-SIDE-04/05 + §2.1):
 *      - decode extracts the exact parsed fields per opcode; apply
 *        never re-reads the raw record (the parsed struct is the only
 *        apply input);
 *      - malformed length / unknown info -> decode false (BLOCKED,
 *        pre-mutation);
 *      - preflight: route must be TT_UNDO (XLOG_HW_RESERVE stays
 *        BLOCKED under STOP-RF-SIDE-SPACE-ABI); TT slot bounds and a
 *        valid xid are required (U-SIDE-05 one-at-a-time facts).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/rmgr.h"
#include "access/xlogrecord.h"
#include "cluster/cluster_side_undo.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/storage/cluster_undo_xlog.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

#include <stdio.h>

static TTSlot apply_slot;
static uint32 apply_generation = 9;
static int apply_read_calls;
static int apply_fail_on_read;

void
cluster_tt_durable_redo_stamp_slot(uint8 instance pg_attribute_unused(),
								   uint32 segment_id pg_attribute_unused(),
								   uint16 slot_offset pg_attribute_unused(), uint16 wrap,
								   TransactionId xid, SCN commit_scn)
{
	memset(&apply_slot, 0, sizeof(apply_slot));
	apply_slot.status = TT_SLOT_COMMITTED;
	apply_slot.xid = xid;
	apply_slot.wrap = wrap;
	apply_slot.commit_scn = commit_scn;
}

ClusterTTActiveTransitionDecision
cluster_tt_durable_bind_preflight_exact(uint8 instance pg_attribute_unused(),
										uint32 segment_id pg_attribute_unused(),
										uint32 segment_generation,
										uint16 slot_offset pg_attribute_unused(), uint16 wrap,
										TransactionId xid)
{
	if (segment_generation != apply_generation)
		return segment_generation < apply_generation ? CLUSTER_TT_ACTIVE_STALE
													 : CLUSTER_TT_ACTIVE_CORRUPT;
	if (apply_slot.status == TT_SLOT_UNUSED)
		return memcmp(&apply_slot, &(TTSlot){ 0 }, sizeof(apply_slot)) == 0
				   ? CLUSTER_TT_ACTIVE_APPLY
				   : CLUSTER_TT_ACTIVE_CORRUPT;
	if (apply_slot.status == TT_SLOT_ACTIVE && apply_slot.xid == xid && apply_slot.wrap == wrap
		&& apply_slot.flags == TT_FLAGS_RESERVED && !SCN_VALID(apply_slot.commit_scn)
		&& UBA_is_invalid(apply_slot.first_undo_block))
		return CLUSTER_TT_ACTIVE_IDEMPOTENT;
	return CLUSTER_TT_ACTIVE_CONFLICT;
}

void
cluster_tt_durable_redo_bind_slot(uint8 instance pg_attribute_unused(),
								  uint32 segment_id pg_attribute_unused(),
								  uint32 segment_generation,
								  uint16 slot_offset pg_attribute_unused(), uint16 wrap,
								  TransactionId xid)
{
	if (cluster_tt_durable_bind_preflight_exact(instance, segment_id, segment_generation,
												slot_offset, wrap, xid)
		!= CLUSTER_TT_ACTIVE_APPLY)
		return;
	memset(&apply_slot, 0, sizeof(apply_slot));
	apply_slot.status = TT_SLOT_ACTIVE;
	apply_slot.xid = xid;
	apply_slot.wrap = wrap;
}

ClusterTTTerminalTransitionDecision
cluster_tt_durable_abort_preflight_exact(uint8 instance pg_attribute_unused(),
										 uint32 segment_id pg_attribute_unused(),
										 uint32 segment_generation,
										 uint16 slot_offset pg_attribute_unused(), uint16 wrap,
										 TransactionId xid)
{
	if (segment_generation != apply_generation)
		return segment_generation < apply_generation ? CLUSTER_TT_TERMINAL_STALE
													 : CLUSTER_TT_TERMINAL_CORRUPT;
	if (apply_slot.status == TT_SLOT_ACTIVE && apply_slot.xid == xid && apply_slot.wrap == wrap
		&& !SCN_VALID(apply_slot.commit_scn))
		return CLUSTER_TT_TERMINAL_APPLY;
	if (apply_slot.status == TT_SLOT_ABORTED && apply_slot.xid == xid && apply_slot.wrap == wrap
		&& !SCN_VALID(apply_slot.commit_scn) && UBA_is_invalid(apply_slot.first_undo_block))
		return CLUSTER_TT_TERMINAL_IDEMPOTENT;
	return CLUSTER_TT_TERMINAL_CONFLICT;
}

void
cluster_tt_durable_redo_abort_slot_exact(uint8 instance pg_attribute_unused(),
										 uint32 segment_id pg_attribute_unused(),
										 uint32 segment_generation,
										 uint16 slot_offset pg_attribute_unused(), uint16 wrap,
										 TransactionId xid)
{
	if (cluster_tt_durable_abort_preflight_exact(instance, segment_id, segment_generation,
												 slot_offset, wrap, xid)
		!= CLUSTER_TT_TERMINAL_APPLY)
		return;
	apply_slot.status = TT_SLOT_ABORTED;
	apply_slot.commit_scn = InvalidScn;
	memset(&apply_slot.first_undo_block, 0, sizeof(apply_slot.first_undo_block));
}

ClusterUndoTtCtrcReleaseRedoDecision
cluster_tt_durable_ctrc_release_preflight_exact(const xl_undo_tt_slot_ctrc_release_v1 *record)
{
	return cluster_undo_tt_ctrc_release_redo_decide(apply_generation, &apply_slot, record);
}

void
cluster_tt_durable_redo_ctrc_release_slot_exact(const xl_undo_tt_slot_ctrc_release_v1 *record)
{
	if (cluster_tt_durable_ctrc_release_preflight_exact(record)
		== CLUSTER_UNDO_TT_CTRC_RELEASE_REDO_APPLY)
		apply_slot.flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
}

void
cluster_tt_durable_redo_abort_slot(uint8 instance pg_attribute_unused(),
								   uint32 segment_id pg_attribute_unused(),
								   uint16 slot_offset pg_attribute_unused(), uint16 wrap,
								   TransactionId xid)
{
	memset(&apply_slot, 0, sizeof(apply_slot));
	apply_slot.status = TT_SLOT_ABORTED;
	apply_slot.xid = xid;
	apply_slot.wrap = wrap;
}

void
cluster_tt_durable_redo_set_head_slot(uint8 instance pg_attribute_unused(),
									  uint32 segment_id pg_attribute_unused(),
									  uint16 slot_offset pg_attribute_unused(), uint16 wrap,
									  TransactionId xid, UBA first_undo_block)
{
	apply_slot.status = TT_SLOT_ABORTED;
	apply_slot.xid = xid;
	apply_slot.wrap = wrap;
	apply_slot.first_undo_block = first_undo_block;
}

bool
cluster_tt_slot_durable_read_exact_stable(uint32 segment_id pg_attribute_unused(),
										  uint16 slot_offset pg_attribute_unused(),
										  TransactionId xid, uint16 expected_wrap, TTSlot *slot_out)
{
	apply_read_calls++;
	if ((apply_fail_on_read > 0 && apply_read_calls == apply_fail_on_read) || slot_out == NULL
		|| apply_slot.xid != xid || apply_slot.wrap != expected_wrap)
		return false;
	*slot_out = apply_slot;
	return true;
}

/* ----------
 * Record fabrication: a minimal XLogReaderState wrapping a
 * DecodedXLogRecord whose main_data points at an external payload.
 * ---------- */
typedef struct FakeRecord {
	XLogReaderState st;
	uint8 data[BLCKSZ + 128];
	union {
		DecodedXLogRecord dec;
		/* cppcheck-suppress unusedStructMember */
		char pad[sizeof(DecodedXLogRecord) + 2 * sizeof(DecodedBkpBlock)];
	} u;
} FakeRecord;

static XLogReaderState *
make_record(FakeRecord *fr, RmgrId rmid, uint8 info, const void *payload, uint32 payload_len)
{
	DecodedXLogRecord *dec = &fr->u.dec;

	memset(fr, 0, sizeof(*fr));
	dec->header.xl_rmid = rmid;
	dec->header.xl_info = info;
	if (payload != NULL && payload_len > 0) {
		memcpy(fr->data, payload, payload_len);
		dec->main_data = (char *)fr->data;
		dec->main_data_len = payload_len;
	}
	fr->st.record = dec;
	return &fr->st;
}

UT_TEST(test_decode_tt_commit_fields)
{
	FakeRecord fr;
	xl_undo_tt_slot_commit payload;
	ClusterUndoDecoded out;
	XLogReaderState *rec;

	memset(&payload, 0, sizeof(payload));
	payload.instance = 1;
	payload.segment_id = 7;
	payload.slot_offset = 3;
	payload.wrap = 2;
	payload.xid = 1234;
	payload.commit_scn = UINT64_C(0x123456789);

	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_COMMIT, &payload, sizeof(payload));
	UT_ASSERT(cluster_undo_decode(rec, &out));
	UT_ASSERT_EQ((int)out.kind, (int)CLUSTER_UNDO_KIND_TT_COMMIT);
	UT_ASSERT_EQ((unsigned)out.instance, 1U);
	UT_ASSERT_EQ((unsigned)out.segment_id, 7U);
	UT_ASSERT_EQ((unsigned)out.slot_offset, 3U);
	UT_ASSERT_EQ((unsigned)out.wrap, 2U);
	UT_ASSERT_EQ((unsigned)out.xid, 1234U);
	UT_ASSERT_EQ((unsigned long long)out.commit_scn, (unsigned long long)UINT64_C(0x123456789));

	/* preflight passes for a valid TT commit. */
	UT_ASSERT(cluster_undo_preflight(&out));
}

UT_TEST(test_decode_tt_bind_exact_shape_and_reserved_bytes)
{
	FakeRecord fr;
	xl_undo_tt_slot_bind payload;
	ClusterUndoDecoded out;
	XLogReaderState *rec;

	memset(&payload, 0, sizeof(payload));
	payload.instance = 1;
	payload.segment_id = 7;
	payload.segment_generation = 9;
	payload.slot_offset = 3;
	payload.wrap = 0;
	payload.xid = 1234;
	payload.format_version = CLUSTER_UNDO_TT_BIND_VERSION;
	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_BIND, &payload, sizeof(payload));
	UT_ASSERT(cluster_undo_decode(rec, &out));
	UT_ASSERT_EQ((int)out.kind, (int)CLUSTER_UNDO_KIND_TT_BIND);
	UT_ASSERT_EQ(out.instance, 1);
	UT_ASSERT_EQ(out.segment_id, 7);
	UT_ASSERT_EQ(out.expected_generation, 9);
	UT_ASSERT_EQ(out.slot_offset, 3);
	UT_ASSERT_EQ(out.wrap, 0);
	UT_ASSERT_EQ(out.xid, 1234);
	UT_ASSERT_EQ(out.format_version, CLUSTER_UNDO_TT_BIND_VERSION);
	UT_ASSERT(cluster_undo_preflight(&out));

	payload.reserved32 = 1;
	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_BIND, &payload, sizeof(payload));
	UT_ASSERT(!cluster_undo_decode(rec, &out));
	payload.reserved32 = 0;
	payload.format_version++;
	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_BIND, &payload, sizeof(payload));
	UT_ASSERT(!cluster_undo_decode(rec, &out));
	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_BIND, &payload,
					  sizeof(payload) - 1);
	UT_ASSERT(!cluster_undo_decode(rec, &out));
}

UT_TEST(test_decode_malformed_and_unknown_blocked)
{
	FakeRecord fr;
	xl_undo_tt_slot_commit payload;
	xl_undo_tt_slot_abort abort_payload;
	xl_undo_tt_slot_abort_exact exact_abort;
	xl_undo_tt_slot_set_head head_payload;
	ClusterUndoDecoded out;
	XLogReaderState *rec;

	/* Wrong rmgr. */
	memset(&payload, 0, sizeof(payload));
	rec = make_record(&fr, RM_HEAP_ID, XLOG_UNDO_TT_SLOT_COMMIT, &payload, sizeof(payload));
	UT_ASSERT(!cluster_undo_decode(rec, &out));

	/* Unknown info byte. */
	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, 0x05, &payload, sizeof(payload));
	UT_ASSERT(!cluster_undo_decode(rec, &out));

	/* Malformed length (too short) — U-SIDE-04: BLOCKED pre-mutation. */
	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_COMMIT, &payload,
					  sizeof(payload) - 1);
	UT_ASSERT(!cluster_undo_decode(rec, &out));

	/* Malformed length (too long). */
	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_COMMIT, &payload,
					  sizeof(payload) + 1);
	UT_ASSERT(!cluster_undo_decode(rec, &out));

	memset(&exact_abort, 0, sizeof(exact_abort));
	exact_abort.segment_id = 513;
	exact_abort.segment_generation = 9;
	exact_abort.xid = 801;
	exact_abort.slot_offset = 4;
	exact_abort.wrap = 0;
	exact_abort.instance = 3;
	exact_abort.format_version = CLUSTER_UNDO_TT_ABORT_EXACT_VERSION;
	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_ABORT, &exact_abort,
					  sizeof(exact_abort));
	UT_ASSERT(cluster_undo_decode(rec, &out));
	UT_ASSERT_EQ(out.expected_generation, 9);
	UT_ASSERT_EQ(out.format_version, CLUSTER_UNDO_TT_ABORT_EXACT_VERSION);
	UT_ASSERT(cluster_undo_preflight(&out));
	exact_abort.reserved = 1;
	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_ABORT, &exact_abort,
					  sizeof(exact_abort));
	UT_ASSERT(!cluster_undo_decode(rec, &out));

	/* Cold and online share this padding gate before either may mutate. */
	memset(&abort_payload, 0, sizeof(abort_payload));
	abort_payload._pad[0] = 1;
	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_ABORT, &abort_payload,
					  sizeof(abort_payload));
	UT_ASSERT(!cluster_undo_decode(rec, &out));
	memset(&head_payload, 0, sizeof(head_payload));
	head_payload._pad[2] = 1;
	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_SET_HEAD, &head_payload,
					  sizeof(head_payload));
	UT_ASSERT(!cluster_undo_decode(rec, &out));

	/* NULL inputs. */
	UT_ASSERT(!cluster_undo_decode(NULL, &out));
	UT_ASSERT(!cluster_undo_decode(rec, NULL));
}

UT_TEST(test_preflight_field_integrity_and_route)
{
	FakeRecord fr;
	xl_undo_tt_slot_commit payload;
	xl_hw_reserve hw;
	ClusterUndoDecoded out;
	XLogReaderState *rec;

	memset(&payload, 0, sizeof(payload));
	payload.instance = 1;
	payload.segment_id = 7;
	payload.slot_offset = 3;
	payload.wrap = 2;
	payload.xid = 1234;
	payload.commit_scn = 55;
	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_COMMIT, &payload, sizeof(payload));
	UT_ASSERT(cluster_undo_decode(rec, &out));
	UT_ASSERT(cluster_undo_preflight(&out));

	/* U-SIDE-05 one-at-a-time: slot offset out of range -> BLOCKED. */
	out.slot_offset = TT_SLOTS_PER_SEGMENT;
	UT_ASSERT(!cluster_undo_preflight(&out));
	out.slot_offset = 3;

	/* Invalid xid -> BLOCKED. */
	out.xid = InvalidTransactionId;
	UT_ASSERT(!cluster_undo_preflight(&out));
	out.xid = 1234;

	/* NULL / unknown -> BLOCKED. */
	UT_ASSERT(!cluster_undo_preflight(NULL));
	out.kind = CLUSTER_UNDO_KIND_UNKNOWN;
	UT_ASSERT(!cluster_undo_preflight(&out));
	out.kind = CLUSTER_UNDO_KIND_TT_COMMIT;

	/* XLOG_HW_RESERVE decodes but stays BLOCKED (STOP-RF-SIDE-SPACE-ABI). */
	memset(&hw, 0, sizeof(hw));
	hw.new_hwm = 42;
	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_HW_RESERVE, &hw, sizeof(hw));
	UT_ASSERT(cluster_undo_decode(rec, &out));
	UT_ASSERT_EQ((int)out.kind, (int)CLUSTER_UNDO_KIND_HW_RESERVE);
	UT_ASSERT(!cluster_undo_preflight(&out));
}

UT_TEST(test_decode_block_write_fields)
{
	FakeRecord fr;
	xl_undo_block_write payload;
	ClusterUndoDecoded out;
	XLogReaderState *rec;
	uint8 fpi[sizeof(payload) + BLCKSZ];
	uint8 delta[sizeof(payload) + UNDO_BLOCK_HDR_PREFIX_LEN + 32 + sizeof(UndoSlotDirEntry)];

	memset(&payload, 0, sizeof(payload));
	payload.instance = 2;
	payload.segment_id = 9;
	payload.block_no = 5;
	payload.has_fpi = 1;
	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE, &payload, sizeof(payload));
	UT_ASSERT(!cluster_undo_decode(rec, &out));
	memcpy(fpi, &payload, sizeof(payload));
	memset(fpi + sizeof(payload), 0x5a, BLCKSZ);
	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE, fpi, sizeof(fpi));
	UT_ASSERT(cluster_undo_decode(rec, &out));
	UT_ASSERT_EQ((int)out.kind, (int)CLUSTER_UNDO_KIND_BLOCK_WRITE);
	UT_ASSERT_EQ((unsigned)out.segment_id, 9U);
	UT_ASSERT_EQ((unsigned)out.block_no, 5U);
	UT_ASSERT(out.has_payload);
	UT_ASSERT(out.has_fpi);
	UT_ASSERT_EQ(out.payload_offset, sizeof(payload));
	UT_ASSERT_EQ(out.payload_length, BLCKSZ);

	memset(&payload, 0, sizeof(payload));
	payload.instance = 2;
	payload.segment_id = 257;
	payload.block_no = 6;
	payload.rec_off = sizeof(UndoBlockHeader);
	payload.rec_len = 32;
	payload.slot_off = BLCKSZ - sizeof(UndoSlotDirEntry);
	memcpy(delta, &payload, sizeof(payload));
	memset(delta + sizeof(payload), 0x6b, sizeof(delta) - sizeof(payload));
	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE, delta, sizeof(delta));
	UT_ASSERT(cluster_undo_decode(rec, &out));
	UT_ASSERT(!out.has_fpi);
	UT_ASSERT_EQ(out.rec_off, sizeof(UndoBlockHeader));
	UT_ASSERT_EQ(out.rec_len, 32);
	UT_ASSERT_EQ(out.slot_off, BLCKSZ - sizeof(UndoSlotDirEntry));
	UT_ASSERT_EQ(out.payload_length, UNDO_BLOCK_HDR_PREFIX_LEN + 32 + sizeof(UndoSlotDirEntry));
	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE, delta, sizeof(delta) - 1);
	UT_ASSERT(!cluster_undo_decode(rec, &out));
}

UT_TEST(test_decode_set_head_and_multi_exact_shape)
{
	FakeRecord fr;
	xl_undo_tt_slot_set_head set_head;
	xl_undo_block_write_multi multi;
	ClusterUndoDecoded out;
	XLogReaderState *rec;
	uint8 body[sizeof(multi) + UNDO_BLOCK_HDR_PREFIX_LEN + 24 + 2 * sizeof(UndoSlotDirEntry)];

	memset(&set_head, 0, sizeof(set_head));
	set_head.instance = 3;
	set_head.segment_id = 513;
	set_head.slot_offset = 4;
	set_head.wrap = 7;
	set_head.xid = 801;
	set_head.first_undo_block.raw[0] = UINT64_C(513) | (UINT64_C(9) << 32);
	set_head.first_undo_block.raw[1] = UINT64_C(4);
	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_SET_HEAD, &set_head,
					  sizeof(set_head));
	UT_ASSERT(cluster_undo_decode(rec, &out));
	UT_ASSERT(memcmp(&out.first_undo_block, &set_head.first_undo_block, sizeof(UBA)) == 0);

	memset(&multi, 0, sizeof(multi));
	multi.instance = 3;
	multi.segment_id = 513;
	multi.block_no = 9;
	multi.rec_off = sizeof(UndoBlockHeader);
	multi.rec_len = 24;
	multi.slot_len = 2 * sizeof(UndoSlotDirEntry);
	multi.slot_off = BLCKSZ - multi.slot_len;
	memcpy(body, &multi, sizeof(multi));
	memset(body + sizeof(multi), 0x31, sizeof(body) - sizeof(multi));
	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE_MULTI, body, sizeof(body));
	UT_ASSERT(cluster_undo_decode(rec, &out));
	UT_ASSERT_EQ((int)out.kind, (int)CLUSTER_UNDO_KIND_BLOCK_WRITE_MULTI);
	UT_ASSERT_EQ(out.slot_len, 2 * sizeof(UndoSlotDirEntry));
	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE_MULTI, body, sizeof(body) - 1);
	UT_ASSERT(!cluster_undo_decode(rec, &out));
}

UT_TEST(test_typed_tt_apply_requires_exact_post_read)
{
	ClusterUndoDecoded operation;
	UBA head = InvalidUba_init;

	memset(&operation, 0, sizeof(operation));
	operation.kind = CLUSTER_UNDO_KIND_TT_COMMIT;
	operation.opcode = XLOG_UNDO_TT_SLOT_COMMIT;
	operation.instance = 3;
	operation.segment_id = 513;
	operation.slot_offset = 4;
	operation.wrap = 7;
	operation.xid = 801;
	operation.commit_scn = 901;
	apply_read_calls = 0;
	apply_fail_on_read = 0;
	memset(&apply_slot, 0, sizeof(apply_slot));
	apply_slot.status = TT_SLOT_ACTIVE;
	apply_slot.xid = operation.xid;
	apply_slot.wrap = operation.wrap;
	UT_ASSERT_EQ(cluster_undo_preflight_tt_target_v1(&operation), CLUSTER_UNDO_TARGET_APPLY);
	UT_ASSERT_EQ(cluster_undo_apply_tt_v1(&operation), CLUSTER_UNDO_APPLY_OK);
	UT_ASSERT_EQ((int)apply_slot.status, (int)TT_SLOT_COMMITTED);
	UT_ASSERT_EQ(cluster_undo_preflight_tt_target_v1(&operation), CLUSTER_UNDO_TARGET_PROVED_NOOP);

	operation.kind = CLUSTER_UNDO_KIND_TT_ABORT;
	operation.opcode = XLOG_UNDO_TT_SLOT_ABORT;
	operation.commit_scn = InvalidScn;
	UT_ASSERT_EQ(cluster_undo_preflight_tt_target_v1(&operation), CLUSTER_UNDO_TARGET_BLOCKED);
	apply_slot.status = TT_SLOT_ACTIVE;
	apply_slot.commit_scn = InvalidScn;
	UT_ASSERT_EQ(cluster_undo_apply_tt_v1(&operation), CLUSTER_UNDO_APPLY_OK);
	UT_ASSERT_EQ((int)apply_slot.status, (int)TT_SLOT_ABORTED);

	head.raw[0] = UINT64_C(513) | (UINT64_C(9) << 32);
	head.raw[1] = UINT64_C(4);
	operation.kind = CLUSTER_UNDO_KIND_TT_SET_HEAD;
	operation.opcode = XLOG_UNDO_TT_SLOT_SET_HEAD;
	operation.first_undo_block = head;
	UT_ASSERT_EQ(cluster_undo_preflight_tt_target_v1(&operation), CLUSTER_UNDO_TARGET_APPLY);
	UT_ASSERT_EQ(cluster_undo_apply_tt_v1(&operation), CLUSTER_UNDO_APPLY_OK);
	UT_ASSERT_EQ(memcmp(&apply_slot.first_undo_block, &head, sizeof(head)), 0);
	UT_ASSERT_EQ(cluster_undo_preflight_tt_target_v1(&operation), CLUSTER_UNDO_TARGET_PROVED_NOOP);
	apply_slot.first_undo_block.raw[0]++;
	UT_ASSERT_EQ(cluster_undo_preflight_tt_target_v1(&operation), CLUSTER_UNDO_TARGET_BLOCKED);
	apply_slot.first_undo_block = head;

	apply_slot.status = TT_SLOT_ABORTED;
	apply_slot.commit_scn = InvalidScn;
	memset(&apply_slot.first_undo_block, 0, sizeof(apply_slot.first_undo_block));
	apply_read_calls = 0;
	apply_fail_on_read = 2;
	UT_ASSERT_EQ(cluster_undo_apply_tt_v1(&operation), CLUSTER_UNDO_APPLY_POST_READ_FAILED);
	apply_fail_on_read = 0;
}

UT_TEST(test_typed_tt_bind_applies_only_exact_generation_and_predecessor)
{
	ClusterUndoDecoded operation;

	memset(&operation, 0, sizeof(operation));
	operation.kind = CLUSTER_UNDO_KIND_TT_BIND;
	operation.opcode = XLOG_UNDO_TT_SLOT_BIND;
	operation.instance = 3;
	operation.segment_id = 513;
	operation.expected_generation = 9;
	operation.slot_offset = 4;
	operation.wrap = 0;
	operation.xid = 801;
	operation.format_version = CLUSTER_UNDO_TT_BIND_VERSION;
	memset(&apply_slot, 0, sizeof(apply_slot));
	apply_read_calls = 0;
	apply_fail_on_read = 0;

	UT_ASSERT_EQ(cluster_undo_preflight_tt_target_v1(&operation), CLUSTER_UNDO_TARGET_APPLY);
	UT_ASSERT_EQ(cluster_undo_apply_tt_v1(&operation), CLUSTER_UNDO_APPLY_OK);
	UT_ASSERT_EQ(apply_slot.status, TT_SLOT_ACTIVE);
	UT_ASSERT_EQ(apply_slot.xid, operation.xid);
	UT_ASSERT_EQ(apply_slot.wrap, operation.wrap);
	UT_ASSERT_EQ(cluster_undo_preflight_tt_target_v1(&operation), CLUSTER_UNDO_TARGET_PROVED_NOOP);

	operation.expected_generation--;
	UT_ASSERT_EQ(cluster_undo_preflight_tt_target_v1(&operation), CLUSTER_UNDO_TARGET_PROVED_NOOP);

	operation.expected_generation += 2;
	UT_ASSERT_EQ(cluster_undo_preflight_tt_target_v1(&operation), CLUSTER_UNDO_TARGET_BLOCKED);
}

UT_TEST(test_typed_exact_abort_requires_same_active_predecessor)
{
	ClusterUndoDecoded operation;

	memset(&operation, 0, sizeof(operation));
	operation.kind = CLUSTER_UNDO_KIND_TT_ABORT;
	operation.opcode = XLOG_UNDO_TT_SLOT_ABORT;
	operation.instance = 3;
	operation.segment_id = 513;
	operation.expected_generation = 9;
	operation.slot_offset = 4;
	operation.wrap = 7;
	operation.xid = 801;
	operation.format_version = CLUSTER_UNDO_TT_ABORT_EXACT_VERSION;
	apply_generation = operation.expected_generation;
	memset(&apply_slot, 0, sizeof(apply_slot));
	apply_slot.status = TT_SLOT_ACTIVE;
	apply_slot.xid = operation.xid;
	apply_slot.wrap = operation.wrap;
	apply_read_calls = 0;
	apply_fail_on_read = 0;

	UT_ASSERT_EQ(cluster_undo_preflight_tt_target_v1(&operation), CLUSTER_UNDO_TARGET_APPLY);
	UT_ASSERT_EQ(cluster_undo_apply_tt_v1(&operation), CLUSTER_UNDO_APPLY_OK);
	UT_ASSERT_EQ((int)apply_slot.status, (int)TT_SLOT_ABORTED);
	UT_ASSERT_EQ(cluster_undo_preflight_tt_target_v1(&operation), CLUSTER_UNDO_TARGET_PROVED_NOOP);

	operation.expected_generation--;
	UT_ASSERT_EQ(cluster_undo_preflight_tt_target_v1(&operation), CLUSTER_UNDO_TARGET_PROVED_NOOP);
	operation.expected_generation += 2;
	UT_ASSERT_EQ(cluster_undo_preflight_tt_target_v1(&operation), CLUSTER_UNDO_TARGET_BLOCKED);
	operation.expected_generation = apply_generation;
	apply_slot.status = TT_SLOT_ACTIVE;
	apply_slot.xid++;
	UT_ASSERT_EQ(cluster_undo_preflight_tt_target_v1(&operation), CLUSTER_UNDO_TARGET_BLOCKED);
}

UT_TEST(test_typed_ctrc_release_requires_exact_terminal_generation_and_post_read)
{
	ClusterUndoDecoded operation;

	memset(&operation, 0, sizeof(operation));
	operation.kind = CLUSTER_UNDO_KIND_TT_CTRC_RELEASE;
	operation.opcode = XLOG_UNDO_TT_SLOT_CTRC_RELEASE;
	operation.instance = 3;
	operation.segment_id = 513;
	operation.expected_generation = 9;
	operation.slot_offset = 4;
	operation.wrap = 7;
	operation.xid = 801;
	operation.cluster_epoch = 11;
	operation.root_id = 12;
	operation.root_generation = 13;
	operation.formation_epoch = 14;
	operation.admission_record_generation = 15;
	operation.seal_generation = 16;
	operation.touched_nodes_low = UINT64_C(0x4);
	operation.terminal_status = TT_SLOT_COMMITTED;
	operation.format_version = CLUSTER_UNDO_TT_CTRC_RELEASE_VERSION;
	operation.flags = CLUSTER_UNDO_TT_CTRC_RELEASE_ALL_TOUCHED_ACKED;
	memset(operation.ack_set_digest, 0x5a, sizeof(operation.ack_set_digest));
	apply_generation = operation.expected_generation;
	memset(&apply_slot, 0, sizeof(apply_slot));
	apply_slot.status = TT_SLOT_COMMITTED;
	apply_slot.xid = operation.xid;
	apply_slot.wrap = operation.wrap;
	apply_slot.commit_scn = 901;
	apply_read_calls = 0;
	apply_fail_on_read = 0;

	UT_ASSERT_EQ(cluster_undo_preflight_tt_target_v1(&operation), CLUSTER_UNDO_TARGET_APPLY);
	UT_ASSERT_EQ(cluster_undo_apply_tt_v1(&operation), CLUSTER_UNDO_APPLY_OK);
	UT_ASSERT_EQ(apply_slot.flags, TT_SLOT_FLAG_CTRC_RELEASE_PROVEN);
	UT_ASSERT_EQ(cluster_undo_preflight_tt_target_v1(&operation), CLUSTER_UNDO_TARGET_PROVED_NOOP);
}

int
main(void)
{
	UT_PLAN(10);

	UT_RUN(test_decode_tt_commit_fields);
	UT_RUN(test_decode_tt_bind_exact_shape_and_reserved_bytes);
	UT_RUN(test_decode_malformed_and_unknown_blocked);
	UT_RUN(test_preflight_field_integrity_and_route);
	UT_RUN(test_decode_block_write_fields);
	UT_RUN(test_decode_set_head_and_multi_exact_shape);
	UT_RUN(test_typed_tt_apply_requires_exact_post_read);
	UT_RUN(test_typed_tt_bind_applies_only_exact_generation_and_predecessor);
	UT_RUN(test_typed_exact_abort_requires_same_active_predecessor);
	UT_RUN(test_typed_ctrc_release_requires_exact_terminal_generation_and_post_read);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
