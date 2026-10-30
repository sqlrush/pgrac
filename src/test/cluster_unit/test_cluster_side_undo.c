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
#include "access/xact.h"
#include "access/xlogrecord.h"
#include "cluster/cluster_side_undo.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_undo_segment_init.h"
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
	UT_ASSERT_EQ(out.original_full_xid, 0);
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

/* Assemble the frozen wire offsets independently of the new product type. */
static void
make_bind_v2_payload(uint8 payload[32], uint64 full_xid)
{
	xl_undo_tt_slot_bind prefix = { 0 };

	prefix.instance = 1;
	prefix.segment_id = 7;
	prefix.segment_generation = 9;
	prefix.slot_offset = 3;
	prefix.wrap = 0;
	prefix.xid = 1234;
	prefix.format_version = 2;
	memcpy(payload, &prefix, sizeof(prefix));
	memcpy(payload + 24, &full_xid, sizeof(full_xid));
}

UT_TEST(test_decode_tt_commit_v2_preserves_complete_identity)
{
	const uint64 fulls[] = { 1234, (UINT64_C(7) << 32) | 1234 };
	FakeRecord fr;
	uint8 bytes[41];
	ClusterUndoDecoded out;
	SCN scn = 901;

	for (Size i = 0; i < lengthof(fulls); i++) {
		make_bind_v2_payload(bytes + 1, fulls[i]);
		memcpy(bytes + 33, &scn, sizeof(scn));
		UT_ASSERT(cluster_undo_decode(make_record(&fr, RM_CLUSTER_UNDO_ID,
			XLOG_UNDO_TT_SLOT_COMMIT, bytes + 1, 40), &out));
		UT_ASSERT_EQ(out.format_version, 2);
		UT_ASSERT_EQ(out.expected_generation, 9);
		UT_ASSERT_EQ(out.original_full_xid, fulls[i]);
		UT_ASSERT_EQ(out.xid, 1234);
		UT_ASSERT_EQ(out.commit_scn, scn);
		UT_ASSERT(!cluster_undo_preflight(&out));
		UT_ASSERT(!cluster_undo_inspect_pi(&out));
	}
}

UT_TEST(test_decode_tt_abort_v2_preserves_complete_identity)
{
	const uint64 fulls[] = { 1234, (UINT64_C(7) << 32) | 1234 };
	FakeRecord fr;
	uint8 bytes[33];
	ClusterUndoDecoded out;

	for (Size i = 0; i < lengthof(fulls); i++) {
		make_bind_v2_payload(bytes + 1, fulls[i]);
		UT_ASSERT(cluster_undo_decode(make_record(&fr, RM_CLUSTER_UNDO_ID,
			XLOG_UNDO_TT_SLOT_ABORT, bytes + 1, 32), &out));
		UT_ASSERT_EQ(out.format_version, 2);
		UT_ASSERT_EQ(out.expected_generation, 9);
		UT_ASSERT_EQ(out.original_full_xid, fulls[i]);
		UT_ASSERT_EQ(out.xid, 1234);
		UT_ASSERT_EQ(out.commit_scn, InvalidScn);
		UT_ASSERT(!cluster_undo_preflight(&out));
		UT_ASSERT(!cluster_undo_inspect_pi(&out));
	}
}

UT_TEST(test_decode_tt_head_v2_preserves_complete_identity)
{
	const uint64 fulls[] = { 1234, (UINT64_C(7) << 32) | 1234 };
	FakeRecord fr;
	uint8 bytes[49];
	ClusterUndoDecoded out;
	UBA head = InvalidUba_init;

	head.raw[0] = 7 | (UINT64_C(9) << 32);
	head.raw[1] = 3;
	for (Size i = 0; i < lengthof(fulls); i++) {
		make_bind_v2_payload(bytes + 1, fulls[i]);
		memcpy(bytes + 33, &head, sizeof(head));
		UT_ASSERT(cluster_undo_decode(make_record(&fr, RM_CLUSTER_UNDO_ID,
			XLOG_UNDO_TT_SLOT_SET_HEAD, bytes + 1, 48), &out));
		UT_ASSERT_EQ(out.format_version, 2);
		UT_ASSERT_EQ(out.expected_generation, 9);
		UT_ASSERT_EQ(out.original_full_xid, fulls[i]);
		UT_ASSERT_EQ(out.xid, 1234);
		UT_ASSERT_EQ(memcmp(&out.first_undo_block, &head, sizeof(head)), 0);
		UT_ASSERT(!cluster_undo_preflight(&out));
		UT_ASSERT(!cluster_undo_inspect_pi(&out));
	}
}

static void
make_release_v2_payload(uint8 bytes[104], uint64 full)
{
	xl_undo_tt_slot_ctrc_release_v1 legacy = { 0 };

	legacy.segment_id = 7;
	legacy.segment_generation = 9;
	legacy.xid = 1234;
	legacy.cluster_epoch = 11;
	legacy.root_id = 12;
	legacy.root_generation = 13;
	legacy.formation_epoch = 14;
	legacy.admission_record_generation = 15;
	legacy.seal_generation = 16;
	legacy.touched_nodes_low = 1;
	memset(legacy.ack_set_digest, 0x5a, sizeof(legacy.ack_set_digest));
	legacy.slot_offset = 3;
	legacy.slot_wrap = 0;
	legacy.owner_instance = 1;
	legacy.terminal_status = TT_SLOT_COMMITTED;
	legacy.format_version = CLUSTER_UNDO_TT_CTRC_RELEASE_VERSION;
	legacy.flags = CLUSTER_UNDO_TT_CTRC_RELEASE_ALL_TOUCHED_ACKED;
	if (!cluster_undo_tt_ctrc_release_encode(&legacy, bytes))
		abort();
	bytes[94] = 2;
	cluster_undo_tt_ctrc_put_u64_le(bytes + 96, full);
}

UT_TEST(test_decode_tt_release_v2_preserves_complete_identity)
{
	const uint64 fulls[] = { 1234, (UINT64_C(7) << 32) | 1234 };
	FakeRecord fr;
	uint8 bytes[105];
	ClusterUndoDecoded out;

	for (Size i = 0; i < lengthof(fulls); i++) {
		make_release_v2_payload(bytes + 1, fulls[i]);
		UT_ASSERT(cluster_undo_decode(make_record(&fr, RM_CLUSTER_UNDO_ID,
			XLOG_UNDO_TT_SLOT_CTRC_RELEASE, bytes + 1, 104), &out));
		UT_ASSERT_EQ(out.format_version, 2);
		UT_ASSERT_EQ(out.expected_generation, 9);
		UT_ASSERT_EQ(out.original_full_xid, fulls[i]);
		UT_ASSERT_EQ(out.xid, 1234);
		UT_ASSERT_EQ(out.seal_generation, 16);
		UT_ASSERT_EQ(out.root_id, 12);
		UT_ASSERT_EQ(out.ack_set_digest[0], 0x5a);
		UT_ASSERT(!cluster_undo_preflight(&out));
		UT_ASSERT(!cluster_undo_inspect_pi(&out));
	}
}

static Size
make_terminal_v2_payload(uint8 opcode, uint8 bytes[105], uint64 full)
{
	SCN scn = 901;
	UBA head = InvalidUba_init;

	memset(bytes, 0, 105);
	if (opcode == XLOG_UNDO_TT_SLOT_CTRC_RELEASE) {
		make_release_v2_payload(bytes, full);
		return 104;
	}
	make_bind_v2_payload(bytes, full);
	if (opcode == XLOG_UNDO_TT_SLOT_COMMIT) {
		memcpy(bytes + 32, &scn, sizeof(scn));
		return 40;
	}
	if (opcode == XLOG_UNDO_TT_SLOT_ABORT)
		return 32;
	head.raw[0] = 7 | (UINT64_C(9) << 32);
	head.raw[1] = 3;
	memcpy(bytes + 32, &head, sizeof(head));
	return 48;
}

UT_TEST(test_terminal_v2_rejects_malformed_versions_lengths_and_identity)
{
	const uint8 opcodes[] = { XLOG_UNDO_TT_SLOT_COMMIT, XLOG_UNDO_TT_SLOT_ABORT,
		XLOG_UNDO_TT_SLOT_SET_HEAD, XLOG_UNDO_TT_SLOT_CTRC_RELEASE };
	const uint64 invalid_fulls[] = { 0, 1, 2, UINT64_MAX, (UINT64_C(7) << 32) | 1235,
		(UINT64_C(7) << 32), (UINT64_C(7) << 32) | 1, (UINT64_C(7) << 32) | 2 };
	const uint8 bad_versions[] = { 0, 1, 3, UINT8_MAX };
	FakeRecord fr;
	uint8 bytes[105];
	ClusterUndoDecoded out;

	for (Size c = 0; c < lengthof(opcodes); c++) {
		Size len = make_terminal_v2_payload(opcodes[c], bytes, 1234);
		Size full_offset = c == 3 ? 96 : 24;
		Size version_offset = c == 3 ? 94 : 17;
		Size lengths[] = { 0, 1, len - 1, len + 1 };

		for (Size i = 0; i < lengthof(lengths); i++)
			UT_ASSERT(!cluster_undo_decode(make_record(&fr, RM_CLUSTER_UNDO_ID,
				opcodes[c], bytes, lengths[i]), &out));
		for (Size i = 0; i < lengthof(bad_versions); i++) {
			make_terminal_v2_payload(opcodes[c], bytes, 1234);
			bytes[version_offset] = bad_versions[i];
			UT_ASSERT(!cluster_undo_decode(make_record(&fr, RM_CLUSTER_UNDO_ID,
				opcodes[c], bytes, len), &out));
		}
		for (Size i = 0; i < lengthof(invalid_fulls); i++) {
			make_terminal_v2_payload(opcodes[c], bytes, 1234);
			if (c == 3)
				cluster_undo_tt_ctrc_put_u64_le(bytes + full_offset, invalid_fulls[i]);
			else
				memcpy(bytes + full_offset, &invalid_fulls[i], sizeof(uint64));
			UT_ASSERT(!cluster_undo_decode(make_record(&fr, RM_CLUSTER_UNDO_ID,
				opcodes[c], bytes, len), &out));
		}
		/* Every flags/reserved byte in the common prefix is validated. */
		for (Size byte = 18; c != 3 && byte < 24; byte++) {
			make_terminal_v2_payload(opcodes[c], bytes, 1234);
			bytes[byte] = 1;
			UT_ASSERT(!cluster_undo_decode(make_record(&fr, RM_CLUSTER_UNDO_ID,
				opcodes[c], bytes, len), &out));
		}
		make_terminal_v2_payload(opcodes[c], bytes, (UINT64_C(7) << 32) | 1234);
		make_record(&fr, RM_CLUSTER_UNDO_ID, opcodes[c], bytes, len);
		memmove(fr.data + 1, fr.data, len);
		fr.u.dec.main_data++;
		UT_ASSERT(cluster_undo_decode(&fr.st, &out));
		UT_ASSERT_EQ(out.original_full_xid, (UINT64_C(7) << 32) | 1234);
	}
	/* COMMIT must carry a finite SCN; ordinary HEAD cannot accept operation flags/tail. */
	for (Size i = 0; i < 2; i++) {
		SCN bad = i == 0 ? InvalidScn : UINT64_MAX;

		make_terminal_v2_payload(XLOG_UNDO_TT_SLOT_COMMIT, bytes, 1234);
		memcpy(bytes + 32, &bad, sizeof(bad));
		UT_ASSERT(!cluster_undo_decode(make_record(&fr, RM_CLUSTER_UNDO_ID,
			XLOG_UNDO_TT_SLOT_COMMIT, bytes, 40), &out));
	}
	make_terminal_v2_payload(XLOG_UNDO_TT_SLOT_SET_HEAD, bytes, 1234);
	UT_ASSERT(!cluster_undo_decode(make_record(&fr, RM_CLUSTER_UNDO_ID,
		XLOG_UNDO_TT_SLOT_SET_HEAD, bytes, 96), &out));
}

UT_TEST(test_terminal_v2_never_reaches_legacy_target_apply_or_header)
{
	const uint8 opcodes[] = { XLOG_UNDO_TT_SLOT_COMMIT, XLOG_UNDO_TT_SLOT_ABORT,
		XLOG_UNDO_TT_SLOT_SET_HEAD, XLOG_UNDO_TT_SLOT_CTRC_RELEASE };
	FakeRecord fr;
	uint8 bytes[105];
	ClusterUndoDecoded out;
	PGAlignedBlock base, prepared;
	TTSlot before;

	for (Size c = 0; c < lengthof(opcodes); c++) {
		Size len = make_terminal_v2_payload(opcodes[c], bytes, (UINT64_C(7) << 32) | 1234);

		UT_ASSERT(cluster_undo_decode(make_record(&fr, RM_CLUSTER_UNDO_ID,
			opcodes[c], bytes, len), &out));
		memset(&apply_slot, 0, sizeof(apply_slot));
		apply_slot.xid = 1234;
		apply_slot.status = TT_SLOT_ACTIVE;
		before = apply_slot;
		apply_read_calls = 0;
		for (Size relabel = 0; relabel < 2; relabel++) {
			if (relabel)
				out.format_version = c < 3 ? 0 : CLUSTER_UNDO_TT_CTRC_RELEASE_VERSION;
			memset(base.data, 0, BLCKSZ);
			memset(prepared.data, 0x5a, BLCKSZ);
			UT_ASSERT(!cluster_undo_preflight(&out));
			UT_ASSERT(!cluster_undo_inspect_pi(&out));
			UT_ASSERT_EQ(cluster_undo_preflight_tt_target_v1(&out), CLUSTER_UNDO_TARGET_BLOCKED);
			UT_ASSERT_EQ(cluster_undo_preflight_legacy_slot_v1(&out, &apply_slot),
				CLUSTER_UNDO_TARGET_BLOCKED);
			UT_ASSERT_EQ(cluster_undo_apply_tt_v1(&out), CLUSTER_UNDO_APPLY_BLOCKED);
			UT_ASSERT_EQ(cluster_undo_prepare_header_v1(&out, NULL, 0, base.data, prepared.data),
				CLUSTER_UNDO_HEADER_BLOCKED);
			UT_ASSERT_EQ(apply_read_calls, 0);
			UT_ASSERT_EQ(memcmp(&apply_slot, &before, sizeof(before)), 0);
			for (Size i = 0; i < BLCKSZ; i++)
				UT_ASSERT_EQ((uint8)prepared.data[i], 0x5a);
		}
	}
}

UT_TEST(test_release_v2_codec_preserves_original_fields_and_rejects_bad_inputs)
{
	uint8 bytes[105], encoded[104], before[104];
	xl_undo_tt_slot_ctrc_release_v2 decoded, saved;
	const Size wrong_lengths[] = { 0, 95, 96, 103, 105 };
	const Size invalid_bytes[] = { 8, 64, 89, 92, 93, 94, 95, 96 };
	const Size required_u64_offsets[] = { 16, 24, 40, 48 };

	make_release_v2_payload(bytes, (UINT64_C(7) << 32) | 1234);
	memcpy(before, bytes, sizeof(before));
	UT_ASSERT(cluster_undo_tt_ctrc_release_v2_decode(bytes, 104, &decoded));
	saved = decoded;
	UT_ASSERT(cluster_undo_tt_ctrc_release_v2_encode(&decoded, encoded));
	UT_ASSERT_EQ(memcmp(encoded, before, sizeof(before)), 0);
	UT_ASSERT_EQ(memcmp(bytes, before, sizeof(before)), 0);
	UT_ASSERT_EQ(sizeof(decoded), 104);
	UT_ASSERT_EQ(offsetof(xl_undo_tt_slot_ctrc_release_v2, original_full_xid), 96);
	UT_ASSERT_EQ(decoded.release.format_version, 2);
	for (Size i = 0; i < lengthof(wrong_lengths); i++) {
		memset(&decoded, 0x5a, sizeof(decoded));
		UT_ASSERT(!cluster_undo_tt_ctrc_release_v2_decode(bytes, wrong_lengths[i], &decoded));
		UT_ASSERT_EQ(memcmp(&decoded, &(xl_undo_tt_slot_ctrc_release_v2){ 0 }, sizeof(decoded)), 0);
	}
	for (Size i = 0; i < lengthof(invalid_bytes); i++) {
		memcpy(bytes, before, sizeof(before));
		bytes[invalid_bytes[i]] ^= 0xff;
		UT_ASSERT(!cluster_undo_tt_ctrc_release_v2_decode(bytes, 104, &decoded));
		UT_ASSERT_EQ(memcmp(&decoded, &(xl_undo_tt_slot_ctrc_release_v2){ 0 }, sizeof(decoded)), 0);
	}
	for (Size i = 0; i < lengthof(required_u64_offsets); i++) {
		memcpy(bytes, before, sizeof(before));
		memset(bytes + required_u64_offsets[i], 0, sizeof(uint64));
		UT_ASSERT(!cluster_undo_tt_ctrc_release_v2_decode(bytes, 104, &decoded));
		UT_ASSERT_EQ(memcmp(&decoded, &(xl_undo_tt_slot_ctrc_release_v2){ 0 }, sizeof(decoded)), 0);
	}
	UT_ASSERT(!cluster_undo_tt_ctrc_release_v2_decode(NULL, 104, &decoded));
	UT_ASSERT(!cluster_undo_tt_ctrc_release_v2_decode(before, 104, NULL));
	decoded = saved;
	decoded.original_full_xid++;
	memset(encoded, 0x5a, sizeof(encoded));
	UT_ASSERT(!cluster_undo_tt_ctrc_release_v2_encode(&decoded, encoded));
	UT_ASSERT_EQ(memcmp(encoded, (uint8[104]){ 0 }, sizeof(encoded)), 0);
	UT_ASSERT(!cluster_undo_tt_ctrc_release_v2_encode(NULL, encoded));
	UT_ASSERT(!cluster_undo_tt_ctrc_release_v2_encode(&saved, NULL));
}

UT_TEST(test_terminal_v2_descriptors_keep_exact_full_identity)
{
	const uint8 opcodes[] = { XLOG_UNDO_TT_SLOT_COMMIT, XLOG_UNDO_TT_SLOT_ABORT,
		XLOG_UNDO_TT_SLOT_SET_HEAD, XLOG_UNDO_TT_SLOT_CTRC_RELEASE };
	const uint64 fulls[] = { 1234, (UINT64_C(7) << 32) | 1234 };
	FakeRecord fr;
	uint8 bytes[105];
	char text[512], expected[64];
	StringInfoData buf = { .data = text, .maxlen = sizeof(text) };

	for (Size c = 0; c < lengthof(opcodes); c++) {
		Size len = 0;

		for (Size i = 0; i < lengthof(fulls); i++) {
			len = make_terminal_v2_payload(opcodes[c], bytes, fulls[i]);
			resetStringInfo(&buf);
			cluster_undo_desc(&buf, make_record(&fr, RM_CLUSTER_UNDO_ID, opcodes[c], bytes, len));
			snprintf(expected, sizeof(expected), "original_full_xid " UINT64_FORMAT, fulls[i]);
			UT_ASSERT(strstr(text, expected) != NULL);
			UT_ASSERT(strstr(text, "v2)") != NULL);
			UT_ASSERT(strstr(text, "generation 9") != NULL);
		}
		bytes[c == 3 ? 94 : 17] = 3;
		resetStringInfo(&buf);
		cluster_undo_desc(&buf, make_record(&fr, RM_CLUSTER_UNDO_ID, opcodes[c], bytes, len));
		UT_ASSERT(strstr(text, "malformed") != NULL);
		UT_ASSERT(strstr(text, "original_full_xid") == NULL);
		resetStringInfo(&buf);
		cluster_undo_desc(&buf, make_record(&fr, RM_CLUSTER_UNDO_ID, opcodes[c], bytes, len - 1));
		UT_ASSERT(strstr(text, "malformed") != NULL);
	}
}

UT_TEST(test_legacy_terminal_descriptions_remain_legacy)
{
	FakeRecord fr;
	xl_undo_tt_slot_commit commit = { 0 };
	xl_undo_tt_slot_abort abort_record = { 0 };
	xl_undo_tt_slot_set_head head = { 0 };
	uint8 bytes[104];
	char text[512];
	StringInfoData buf = { .data = text, .maxlen = sizeof(text) };

	commit.instance = abort_record.instance = head.instance = 1;
	commit.segment_id = abort_record.segment_id = head.segment_id = 7;
	commit.slot_offset = abort_record.slot_offset = head.slot_offset = 3;
	commit.wrap = abort_record.wrap = head.wrap = 5;
	commit.xid = abort_record.xid = head.xid = 1234;
	commit.commit_scn = 901;
	resetStringInfo(&buf);
	cluster_undo_desc(&buf, make_record(&fr, RM_CLUSTER_UNDO_ID,
		XLOG_UNDO_TT_SLOT_COMMIT, &commit, sizeof(commit)));
	UT_ASSERT(strcmp(text, "instance 1 segment 7 slot 3 wrap 5 xid 1234 commit_scn 901") == 0);
	resetStringInfo(&buf);
	cluster_undo_desc(&buf, make_record(&fr, RM_CLUSTER_UNDO_ID,
		XLOG_UNDO_TT_SLOT_ABORT, &abort_record, sizeof(abort_record)));
	UT_ASSERT(strcmp(text, "instance 1 seg 7 slot 3 wrap 5 xid 1234 (prepared abort)") == 0);
	resetStringInfo(&buf);
	cluster_undo_desc(&buf, make_record(&fr, RM_CLUSTER_UNDO_ID,
		XLOG_UNDO_TT_SLOT_SET_HEAD, &head, sizeof(head)));
	UT_ASSERT(strcmp(text, "instance 1 seg 7 slot 3 wrap 5 xid 1234 (set undo head)") == 0);
	make_release_v2_payload(bytes, 1234);
	bytes[94] = CLUSTER_UNDO_TT_CTRC_RELEASE_VERSION;
	resetStringInfo(&buf);
	cluster_undo_desc(&buf, make_record(&fr, RM_CLUSTER_UNDO_ID,
		XLOG_UNDO_TT_SLOT_CTRC_RELEASE, bytes, 96));
	UT_ASSERT(strcmp(text, "instance 1 seg 7 generation 9 slot 3 wrap 0 xid 1234 terminal 2 seal 16 touched 1") == 0);
}

UT_TEST(test_decode_tt_bind_v2_preserves_epoch_zero_and_original_epoch)
{
	const uint64 full_xids[] = { UINT64_C(1234), (UINT64_C(7) << 32) | 1234 };
	FakeRecord fr;
	uint8 payload[32];
	ClusterUndoDecoded out;

	for (Size i = 0; i < lengthof(full_xids); i++) {
		make_bind_v2_payload(payload, full_xids[i]);
		UT_ASSERT(cluster_undo_decode(make_record(&fr, RM_CLUSTER_UNDO_ID,
			XLOG_UNDO_TT_SLOT_BIND, payload, sizeof(payload)), &out));
		UT_ASSERT_EQ(out.format_version, 2);
		UT_ASSERT_EQ(out.xid, 1234);
		UT_ASSERT_EQ(out.expected_generation, 9);
		UT_ASSERT_EQ(out.original_full_xid, full_xids[i]);
		/* Reader staging must never reach a raw-only TT32 writer. */
		UT_ASSERT(!cluster_undo_preflight(&out));
		UT_ASSERT(!cluster_undo_inspect_pi(&out));
	}
}

UT_TEST(test_decode_tt_bind_v2_rejects_unknown_shape_and_raw_identity)
{
	const uint64 invalid_full[] = { 0, 1, 2, UINT64_MAX,
		(UINT64_C(7) << 32) | 1235 };
	const unsigned corrupt_bytes[] = { 17, 18, 19, 20, 23 };
	const unsigned wrong_lengths[] = { 0, 16, 24, 31, 33, 40 };
	FakeRecord fr;
	uint8 payload[40] = { 0 };
	ClusterUndoDecoded out;
	uint64 full = (UINT64_C(7) << 32) | 1234;

	for (Size i = 0; i < lengthof(invalid_full); i++) {
		make_bind_v2_payload(payload, invalid_full[i]);
		UT_ASSERT(!cluster_undo_decode(make_record(&fr, RM_CLUSTER_UNDO_ID,
			XLOG_UNDO_TT_SLOT_BIND, payload, 32), &out));
	}
	/* Even raw-exact special and exhausted values are not original IDs. */
	for (TransactionId xid = 0; xid < FirstNormalTransactionId; xid++) {
		uint64 special = (UINT64_C(7) << 32) | xid;

		make_bind_v2_payload(payload, special);
		memcpy(payload + 8, &xid, sizeof(xid));
		UT_ASSERT(!cluster_undo_decode(make_record(&fr, RM_CLUSTER_UNDO_ID,
			XLOG_UNDO_TT_SLOT_BIND, payload, 32), &out));
	}
	make_bind_v2_payload(payload, UINT64_MAX);
	memset(payload + 8, 0xff, sizeof(TransactionId));
	UT_ASSERT(!cluster_undo_decode(make_record(&fr, RM_CLUSTER_UNDO_ID,
		XLOG_UNDO_TT_SLOT_BIND, payload, 32), &out));
	for (Size i = 0; i < lengthof(corrupt_bytes); i++) {
		make_bind_v2_payload(payload, full);
		payload[corrupt_bytes[i]] ^= 1;
		UT_ASSERT(!cluster_undo_decode(make_record(&fr, RM_CLUSTER_UNDO_ID,
			XLOG_UNDO_TT_SLOT_BIND, payload, 32), &out));
	}
	make_bind_v2_payload(payload, full);
	payload[17] = CLUSTER_UNDO_TT_BIND_VERSION;
	UT_ASSERT(!cluster_undo_decode(make_record(&fr, RM_CLUSTER_UNDO_ID,
		XLOG_UNDO_TT_SLOT_BIND, payload, 32), &out));
	make_bind_v2_payload(payload, full);
	for (Size i = 0; i < lengthof(wrong_lengths); i++)
		UT_ASSERT(!cluster_undo_decode(make_record(&fr, RM_CLUSTER_UNDO_ID,
			XLOG_UNDO_TT_SLOT_BIND, payload, wrong_lengths[i]), &out));
	/* Unaligned main data must preserve the full value, without casting. */
	make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_BIND, payload, 32);
	memmove(fr.data + 1, fr.data, 32);
	fr.u.dec.main_data++;
	UT_ASSERT(cluster_undo_decode(&fr.st, &out));
	UT_ASSERT_EQ(out.original_full_xid, full);
}

UT_TEST(test_tt_bind_v2_stays_closed_before_any_legacy_mutation)
{
	FakeRecord fr;
	uint8 payload[32];
	ClusterUndoDecoded out;
	PGAlignedBlock base, prepared;
	TTSlot before;

	make_bind_v2_payload(payload, (UINT64_C(7) << 32) | 1234);
	UT_ASSERT(cluster_undo_decode(make_record(&fr, RM_CLUSTER_UNDO_ID,
		XLOG_UNDO_TT_SLOT_BIND, payload, sizeof(payload)), &out));
	memset(&apply_slot, 0, sizeof(apply_slot));
	before = apply_slot;
	apply_read_calls = 0;
	memset(base.data, 0xa5, BLCKSZ);
	memset(prepared.data, 0x5a, BLCKSZ);
	UT_ASSERT_EQ(cluster_undo_preflight_tt_target_v1(&out), CLUSTER_UNDO_TARGET_BLOCKED);
	UT_ASSERT_EQ(cluster_undo_apply_tt_v1(&out), CLUSTER_UNDO_APPLY_BLOCKED);
	UT_ASSERT_EQ(cluster_undo_prepare_header_v1(&out, NULL, 0, base.data, prepared.data),
		CLUSTER_UNDO_HEADER_BLOCKED);
	UT_ASSERT_EQ(apply_read_calls, 0);
	UT_ASSERT(memcmp(&apply_slot, &before, sizeof(before)) == 0);
	for (Size i = 0; i < BLCKSZ; i++)
		UT_ASSERT_EQ((uint8)prepared.data[i], 0x5a);
	/* Relabelling the parsed version cannot silently discard FullXid. */
	out.format_version = CLUSTER_UNDO_TT_BIND_VERSION;
	UT_ASSERT(!cluster_undo_preflight(&out));
	UT_ASSERT(!cluster_undo_inspect_pi(&out));
	UT_ASSERT_EQ(cluster_undo_apply_tt_v1(&out), CLUSTER_UNDO_APPLY_BLOCKED);
	UT_ASSERT_EQ(apply_read_calls, 0);
}

UT_TEST(test_tt_bind_descriptor_uses_exact_v1_or_v2_identity)
{
	FakeRecord fr;
	uint8 payload[40] = { 0 };
	char text[256];
	StringInfoData buf = { .data = text, .maxlen = sizeof(text) };
	XLogReaderState *record;
	const uint64 full_xids[] = { UINT64_C(1234), (UINT64_C(7) << 32) | 1234 };
	const unsigned wrong_lengths[] = { 0, 16, 24, 31, 33, 40 };

	for (Size i = 0; i < lengthof(full_xids); i++) {
		char expected[64];

		make_bind_v2_payload(payload, full_xids[i]);
		record = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_BIND, payload, 32);
		resetStringInfo(&buf);
		cluster_undo_desc(&buf, record);
		snprintf(expected, sizeof(expected), "original_full_xid " UINT64_FORMAT, full_xids[i]);
		UT_ASSERT(strstr(buf.data, expected) != NULL);
		UT_ASSERT(strstr(buf.data, "(active bind v2)") != NULL);
	}
	for (Size i = 0; i < lengthof(wrong_lengths); i++) {
		record = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_BIND,
			payload, wrong_lengths[i]);
		resetStringInfo(&buf);
		cluster_undo_desc(&buf, record);
		UT_ASSERT(strcmp(buf.data, "malformed TT BIND payload") == 0);
	}
	/* Same-sized malformed V2 must not be described as a legacy bind. */
	for (unsigned byte = 17; byte <= 24; byte++) {
		make_bind_v2_payload(payload, full_xids[1]);
		payload[byte] ^= 1;
		resetStringInfo(&buf);
		cluster_undo_desc(&buf, make_record(&fr, RM_CLUSTER_UNDO_ID,
			XLOG_UNDO_TT_SLOT_BIND, payload, 32));
		UT_ASSERT(strcmp(buf.data, "malformed TT BIND payload") == 0);
	}
	make_bind_v2_payload(payload, 1234);
	payload[17] = CLUSTER_UNDO_TT_BIND_VERSION;
	resetStringInfo(&buf);
	cluster_undo_desc(&buf, make_record(&fr, RM_CLUSTER_UNDO_ID,
		XLOG_UNDO_TT_SLOT_BIND, payload, sizeof(xl_undo_tt_slot_bind)));
	UT_ASSERT(strcmp(buf.data,
		"instance 1 segment 7 generation 9 slot 3 wrap 0 xid 1234 (active bind v1)") == 0);
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

UT_TEST(test_overlapping_undo_record_and_slot_ranges_refuse)
{
	FakeRecord fr;
	ClusterUndoDecoded out;
	xl_undo_block_write single = { 0 };
	xl_undo_block_write_multi multi = { 0 };
	char payload[sizeof(multi) + UNDO_BLOCK_HDR_PREFIX_LEN + 32 + 16] = { 0 };
	XLogReaderState *record;

	single.instance = multi.instance = 1;
	single.segment_id = multi.segment_id = 7;
	single.block_no = multi.block_no = 2;
	single.rec_off = multi.rec_off = sizeof(UndoBlockHeader);
	single.rec_len = multi.rec_len = 32;
	single.slot_off = multi.slot_off = sizeof(UndoBlockHeader) + 24;
	multi.slot_len = 2 * sizeof(UndoSlotDirEntry);
	memcpy(payload, &single, sizeof(single));
	record = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE, payload,
						 sizeof(single) + UNDO_BLOCK_HDR_PREFIX_LEN + single.rec_len
							 + sizeof(UndoSlotDirEntry));
	UT_ASSERT(!cluster_undo_decode(record, &out));
	memcpy(payload, &multi, sizeof(multi));
	record
		= make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE_MULTI, payload,
					  sizeof(multi) + UNDO_BLOCK_HDR_PREFIX_LEN + multi.rec_len + multi.slot_len);
	UT_ASSERT(!cluster_undo_decode(record, &out));
}

/* A complete original fresh producer supplies the V2 header. Only the target
 * predecessor is seeded; expectations below change one complete 40-byte slot. */
static void
make_tt40_base(PGAlignedBlock *base, uint64 full, uint8 status)
{
	UndoSegmentHeaderDataV2 *header = (UndoSegmentHeaderDataV2 *)base->data;
	TTSlotV2 *slot;

	if (!cluster_undo_segment_make_header_v2_bytes(7, 1, base->data))
		abort();
	header->wrap_count = 9;
	header->_reserved[4000] = 0x5a;
	if (status == TT_SLOT_UNUSED)
		return;
	slot = &header->tt_slots[3];
	slot->xid = 1234;
	slot->status = status;
	slot->original_full_xid = full;
	if (status == TT_SLOT_COMMITTED)
		slot->commit_scn = 901;
}

UT_TEST(test_tt40_bind_prepares_exact_full_identity_without_touching_other_bytes)
{
	const uint64 fulls[] = { 1234, (UINT64_C(7) << 32) | 1234 };
	PGAlignedBlock base, before, expected, out;
	FakeRecord fr;
	uint8 bytes[33];
	ClusterUndoDecoded decoded;

	for (Size i = 0; i < lengthof(fulls); i++) {
		TTSlotV2 *slot;

		make_tt40_base(&base, 0, TT_SLOT_UNUSED);
		before = base;
		expected = base;
		slot = &((UndoSegmentHeaderDataV2 *)expected.data)->tt_slots[3];
		slot->xid = 1234;
		slot->status = TT_SLOT_ACTIVE;
		slot->original_full_xid = fulls[i];
		make_bind_v2_payload(bytes + 1, fulls[i]);
		UT_ASSERT(cluster_undo_decode(make_record(&fr, RM_CLUSTER_UNDO_ID,
			XLOG_UNDO_TT_SLOT_BIND, bytes + 1, 32), &decoded));
		memset(out.data, 0x6a, BLCKSZ);
		UT_ASSERT_EQ(cluster_undo_prepare_header_v1(&decoded, NULL, 0, base.data, out.data),
			CLUSTER_UNDO_HEADER_APPLY);
		UT_ASSERT_EQ(memcmp(out.data, expected.data, BLCKSZ), 0);
		UT_ASSERT_EQ(memcmp(base.data, before.data, BLCKSZ), 0);
	}
}

UT_TEST(test_tt40_terminal_successors_preserve_original_full_identity)
{
	const uint64 fulls[] = { 1234, (UINT64_C(7) << 32) | 1234 };
	const uint8 opcodes[] = { XLOG_UNDO_TT_SLOT_COMMIT, XLOG_UNDO_TT_SLOT_ABORT };
	PGAlignedBlock base, before, expected;
	FakeRecord fr;
	uint8 bytes[105];
	ClusterUndoDecoded decoded;

	for (Size i = 0; i < lengthof(fulls); i++)
		for (Size c = 0; c < lengthof(opcodes); c++) {
			TTSlotV2 *slot;
			Size len = make_terminal_v2_payload(opcodes[c], bytes, fulls[i]);

			make_tt40_base(&base, fulls[i], TT_SLOT_ACTIVE);
			before = base;
			expected = base;
			slot = &((UndoSegmentHeaderDataV2 *)expected.data)->tt_slots[3];
			slot->status = c == 0 ? TT_SLOT_COMMITTED : TT_SLOT_ABORTED;
			slot->commit_scn = c == 0 ? 901 : InvalidScn;
			UT_ASSERT(cluster_undo_decode(make_record(&fr, RM_CLUSTER_UNDO_ID,
				opcodes[c], bytes, len), &decoded));
			UT_ASSERT_EQ(cluster_undo_prepare_header_v1(&decoded, NULL, 0, base.data, base.data),
				CLUSTER_UNDO_HEADER_APPLY);
			UT_ASSERT_EQ(memcmp(base.data, expected.data, BLCKSZ), 0);
			UT_ASSERT_EQ(((UndoSegmentHeaderDataV2 *)before.data)->tt_slots[3].status,
				TT_SLOT_ACTIVE);
		}
}

UT_TEST(test_tt40_head_and_release_preserve_complete_predecessor_identity)
{
	const uint64 full = (UINT64_C(7) << 32) | 1234;
	const uint8 opcodes[] = { XLOG_UNDO_TT_SLOT_SET_HEAD, XLOG_UNDO_TT_SLOT_CTRC_RELEASE };
	PGAlignedBlock base, before, expected, out;
	FakeRecord fr;
	uint8 bytes[105];
	ClusterUndoDecoded decoded;

	for (Size c = 0; c < lengthof(opcodes); c++) {
		TTSlotV2 *slot;
		Size len = make_terminal_v2_payload(opcodes[c], bytes, full);

		make_tt40_base(&base, full, c == 0 ? TT_SLOT_ABORTED : TT_SLOT_COMMITTED);
		before = base;
		expected = base;
		slot = &((UndoSegmentHeaderDataV2 *)expected.data)->tt_slots[3];
		UT_ASSERT(cluster_undo_decode(make_record(&fr, RM_CLUSTER_UNDO_ID,
			opcodes[c], bytes, len), &decoded));
		if (c == 0)
			slot->first_undo_block = decoded.first_undo_block;
		else
			slot->flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
		UT_ASSERT_EQ(cluster_undo_prepare_header_v1(&decoded, NULL, 0, base.data, out.data),
			CLUSTER_UNDO_HEADER_APPLY);
		UT_ASSERT_EQ(memcmp(out.data, expected.data, BLCKSZ), 0);
		UT_ASSERT_EQ(memcmp(base.data, before.data, BLCKSZ), 0);
	}
}

static ClusterUndoDecoded
make_tt40_decoded(uint8 opcode, uint64 full)
{
	ClusterUndoDecoded decoded;
	FakeRecord fr;
	uint8 bytes[105];
	Size len;

	if (opcode == XLOG_UNDO_TT_SLOT_BIND) {
		make_bind_v2_payload(bytes, full);
		len = 32;
	} else
		len = make_terminal_v2_payload(opcode, bytes, full);
	if (!cluster_undo_decode(make_record(&fr, RM_CLUSTER_UNDO_ID, opcode, bytes, len), &decoded))
		abort();
	return decoded;
}

static uint8
tt40_predecessor_status(uint8 opcode)
{
	return opcode == XLOG_UNDO_TT_SLOT_SET_HEAD ? TT_SLOT_ABORTED
		 : opcode == XLOG_UNDO_TT_SLOT_CTRC_RELEASE ? TT_SLOT_COMMITTED : TT_SLOT_ACTIVE;
}

static void
check_tt40_no_output(const ClusterUndoDecoded *decoded, PGAlignedBlock *base,
					ClusterUndoHeaderPrepareResultV1 want)
{
	PGAlignedBlock before = *base, out, sentinel;

	memset(out.data, 0x6a, BLCKSZ);
	sentinel = out;
	apply_read_calls = 0;
	UT_ASSERT_EQ(cluster_undo_prepare_header_v1(decoded, NULL, 0, base->data, out.data), want);
	UT_ASSERT_EQ(memcmp(out.data, sentinel.data, BLCKSZ), 0);
	UT_ASSERT_EQ(memcmp(base->data, before.data, BLCKSZ), 0);
	UT_ASSERT_EQ(cluster_undo_prepare_header_v1(decoded, NULL, 0, base->data, base->data), want);
	UT_ASSERT_EQ(memcmp(base->data, before.data, BLCKSZ), 0);
	UT_ASSERT_EQ(apply_read_calls, 0);
}

UT_TEST(test_tt40_all_slot_offsets_and_unaligned_private_copies)
{
	const uint64 full = (UINT64_C(7) << 32) | 1234;
	PGAlignedBlock base, expected;
	char unaligned_base[BLCKSZ + 1], unaligned_out[BLCKSZ + 1];

	for (uint16 offset = 0; offset < 48; offset++) {
		ClusterUndoDecoded decoded = make_tt40_decoded(XLOG_UNDO_TT_SLOT_BIND, full);
		TTSlotV2 *slot;

		decoded.slot_offset = offset;
		make_tt40_base(&base, 0, TT_SLOT_UNUSED);
		expected = base;
		slot = &((UndoSegmentHeaderDataV2 *)expected.data)->tt_slots[offset];
		slot->xid = 1234;
		slot->status = TT_SLOT_ACTIVE;
		slot->original_full_xid = full;
		memset(unaligned_base, 0x5a, sizeof(unaligned_base));
		memset(unaligned_out, 0x6a, sizeof(unaligned_out));
		memcpy(unaligned_base + 1, base.data, BLCKSZ);
		UT_ASSERT_EQ(cluster_undo_prepare_header_v1(&decoded, NULL, 0,
			unaligned_base + 1, unaligned_out + 1), CLUSTER_UNDO_HEADER_APPLY);
		UT_ASSERT_EQ(memcmp(unaligned_out + 1, expected.data, BLCKSZ), 0);
		UT_ASSERT_EQ(memcmp(unaligned_base + 1, base.data, BLCKSZ), 0);
		UT_ASSERT_EQ((uint8)unaligned_base[0], 0x5a);
		UT_ASSERT_EQ((uint8)unaligned_out[0], 0x6a);
	}
}

UT_TEST(test_tt40_raw_match_never_borrows_or_repairs_another_full_identity)
{
	const uint64 full = (UINT64_C(7) << 32) | 1234;
	const uint64 bad_fulls[] = { 0, 1, 2, UINT64_MAX, (UINT64_C(7) << 32) | 1235,
		1234, (UINT64_C(8) << 32) | 1234 };
	const uint8 opcodes[] = { XLOG_UNDO_TT_SLOT_BIND, XLOG_UNDO_TT_SLOT_COMMIT,
		XLOG_UNDO_TT_SLOT_ABORT, XLOG_UNDO_TT_SLOT_SET_HEAD, XLOG_UNDO_TT_SLOT_CTRC_RELEASE };
	PGAlignedBlock base;

	for (Size c = 0; c < lengthof(opcodes); c++) {
		ClusterUndoDecoded decoded = make_tt40_decoded(opcodes[c], full);

		for (Size i = 0; i < lengthof(bad_fulls); i++) {
			make_tt40_base(&base, bad_fulls[i], tt40_predecessor_status(opcodes[c]));
			check_tt40_no_output(&decoded, &base, CLUSTER_UNDO_HEADER_BLOCKED);
		}
		if (opcodes[c] != XLOG_UNDO_TT_SLOT_BIND) {
			make_tt40_base(&base, 0, TT_SLOT_UNUSED);
			check_tt40_no_output(&decoded, &base, CLUSTER_UNDO_HEADER_BLOCKED);
		}
	}
}

UT_TEST(test_tt40_checks_explicit_page_layout_before_shifted_target_fields)
{
	const uint64 full = (UINT64_C(7) << 32) | 1234;
	const uint8 opcodes[] = { XLOG_UNDO_TT_SLOT_BIND, XLOG_UNDO_TT_SLOT_COMMIT,
		XLOG_UNDO_TT_SLOT_ABORT, XLOG_UNDO_TT_SLOT_SET_HEAD, XLOG_UNDO_TT_SLOT_CTRC_RELEASE };
	PGAlignedBlock base;

	for (Size c = 0; c < lengthof(opcodes); c++) {
		ClusterUndoDecoded decoded = make_tt40_decoded(opcodes[c], full);

		for (unsigned fault = 0; fault < 14; fault++) {
			UndoSegmentHeaderDataV2 *header = (UndoSegmentHeaderDataV2 *)base.data;

			make_tt40_base(&base, full, tt40_predecessor_status(opcodes[c]));
			switch (fault) {
			case 0: header->tt_layout_version = 0; break;
			case 1: header->tt_layout_version = 1; break;
			case 2: header->tt_layout_version = 3; break;
			case 3: header->tt_slot_bytes = 32; break;
			case 4: header->tt_layout_reserved = 1; break;
			case 5: header->tt_slots_count = 47; break;
			case 6: header->tt_next_free_slot = 49; break;
			case 7: header->tt_active_count = 49; break;
			case 8: header->segment_id = 8; break;
			case 9: header->owner_instance = 2; break;
			case 10: header->segment_size_bytes = 0; break;
			case 11: header->pd_flags = 0; break;
			case 12: header->segment_state = UINT8_MAX; break;
			case 13:
				cluster_undo_segment_make_header_bytes(7, 1, base.data);
				/* Plausible bytes at the shifted generation cannot qualify V1. */
				header->wrap_count = 9;
				break;
			}
			check_tt40_no_output(&decoded, &base, CLUSTER_UNDO_HEADER_BLOCKED);
		}
	}
}

UT_TEST(test_tt40_physical_stale_and_exact_idempotent_results_do_not_publish_output)
{
	const uint64 full = (UINT64_C(7) << 32) | 1234;
	const uint8 opcodes[] = { XLOG_UNDO_TT_SLOT_BIND, XLOG_UNDO_TT_SLOT_COMMIT,
		XLOG_UNDO_TT_SLOT_ABORT, XLOG_UNDO_TT_SLOT_SET_HEAD, XLOG_UNDO_TT_SLOT_CTRC_RELEASE };
	PGAlignedBlock base;

	for (Size c = 0; c < lengthof(opcodes); c++) {
		ClusterUndoDecoded decoded = make_tt40_decoded(opcodes[c], full);
		UndoSegmentHeaderDataV2 *header = (UndoSegmentHeaderDataV2 *)base.data;
		TTSlotV2 *slot = &header->tt_slots[3];

		make_tt40_base(&base, full, tt40_predecessor_status(opcodes[c]));
		header->wrap_count = 10;
		check_tt40_no_output(&decoded, &base, CLUSTER_UNDO_HEADER_SKIP_STALE);
		header->wrap_count = 8;
		check_tt40_no_output(&decoded, &base, CLUSTER_UNDO_HEADER_BLOCKED);
		header->wrap_count = 9;
		slot->wrap = 1;
		/* Only physical wrap establishes staleness, not FullXid ordering. */
		slot->original_full_xid = 1234;
		check_tt40_no_output(&decoded, &base, CLUSTER_UNDO_HEADER_SKIP_STALE);
		slot->wrap = TT_WRAP_INVALID;
		check_tt40_no_output(&decoded, &base, CLUSTER_UNDO_HEADER_BLOCKED);

		make_tt40_base(&base, full, opcodes[c] == XLOG_UNDO_TT_SLOT_COMMIT ? TT_SLOT_COMMITTED
																 : tt40_predecessor_status(opcodes[c]));
		if (opcodes[c] == XLOG_UNDO_TT_SLOT_ABORT)
			slot->status = TT_SLOT_ABORTED;
		if (opcodes[c] == XLOG_UNDO_TT_SLOT_SET_HEAD)
			slot->first_undo_block = decoded.first_undo_block;
		if (opcodes[c] == XLOG_UNDO_TT_SLOT_CTRC_RELEASE)
			slot->flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
		check_tt40_no_output(&decoded, &base, CLUSTER_UNDO_HEADER_ALREADY);
		if (opcodes[c] == XLOG_UNDO_TT_SLOT_COMMIT) {
			slot->commit_scn = 902;
			check_tt40_no_output(&decoded, &base, CLUSTER_UNDO_HEADER_BLOCKED);
		}
	}
}

UT_TEST(test_tt40_bind_reuse_uses_original_wrap_transition_and_replaces_only_at_bind)
{
	const uint64 old_full = (UINT64_C(7) << 32) | 1234;
	const uint64 new_full = (UINT64_C(8) << 32) | 1234;
	const uint8 statuses[] = { TT_SLOT_COMMITTED, TT_SLOT_ABORTED, TT_SLOT_RECYCLABLE };
	PGAlignedBlock base, expected, out;
	ClusterUndoDecoded decoded = make_tt40_decoded(XLOG_UNDO_TT_SLOT_BIND, new_full);

	for (Size c = 0; c < lengthof(statuses); c++) {
		TTSlotV2 *slot = &((UndoSegmentHeaderDataV2 *)base.data)->tt_slots[3];
		TTSlotV2 *want = &((UndoSegmentHeaderDataV2 *)expected.data)->tt_slots[3];

		make_tt40_base(&base, old_full, statuses[c]);
		slot->wrap = 4;
		expected = base;
		memset(want, 0, sizeof(*want));
		want->xid = 1234;
		want->wrap = 5;
		want->status = TT_SLOT_ACTIVE;
		want->original_full_xid = new_full;
		decoded.wrap = 5;
		UT_ASSERT_EQ(cluster_undo_prepare_header_v1(&decoded, NULL, 0, base.data, out.data),
			CLUSTER_UNDO_HEADER_APPLY);
		UT_ASSERT_EQ(memcmp(out.data, expected.data, BLCKSZ), 0);
		UT_ASSERT_EQ(slot->original_full_xid, old_full);
		decoded.wrap = 6;
		check_tt40_no_output(&decoded, &base, CLUSTER_UNDO_HEADER_BLOCKED);
		decoded.wrap = 4;
		check_tt40_no_output(&decoded, &base, CLUSTER_UNDO_HEADER_BLOCKED);
		decoded.wrap = 5;
		slot->first_undo_block.raw[0] = 1;
		check_tt40_no_output(&decoded, &base, CLUSTER_UNDO_HEADER_BLOCKED);
	}
}

UT_TEST(test_folded_v2_prepares_exact_tt40_successor_without_raw_backfill)
{
	const uint64 identities[] = { 1234, (UINT64_C(7) << 32) | 1234 };
	PGAlignedBlock base, before, out, expected;
	xl_xact_tt_commit tt = { .segment_id = 7, .segment_generation = 9,
		.xid = 1234, .slot_offset = 3, .wrap = 5, .instance = 1,
		.format_version = CLUSTER_XACT_TT_COMMIT_V2_VERSION, .commit_scn = 901 };

	for (Size i = 0; i < lengthof(identities); i++) {
		TTSlotV2 *slot;

		make_tt40_base(&base, identities[i], TT_SLOT_ACTIVE);
		((UndoSegmentHeaderDataV2 *)base.data)->tt_slots[3].wrap = tt.wrap;
		before = base;
		expected = base;
		slot = &((UndoSegmentHeaderDataV2 *)expected.data)->tt_slots[3];
		slot->status = TT_SLOT_COMMITTED;
		slot->flags = TT_FLAGS_RESERVED;
		slot->commit_scn = 901;
		slot->first_undo_block = (UBA)InvalidUba_init;
		UT_ASSERT_EQ(cluster_undo_prepare_xact_commit_v1(&tt, identities[i], base.data, out.data),
			CLUSTER_UNDO_HEADER_APPLY);
		UT_ASSERT_EQ(memcmp(out.data, expected.data, BLCKSZ), 0);
		UT_ASSERT_EQ(memcmp(base.data, before.data, BLCKSZ), 0);
		UT_ASSERT_EQ(cluster_undo_prepare_xact_commit_v1(&tt, identities[i], base.data, base.data),
			CLUSTER_UNDO_HEADER_APPLY);
		UT_ASSERT_EQ(memcmp(base.data, expected.data, BLCKSZ), 0);
		UT_ASSERT_EQ(cluster_undo_prepare_xact_commit_v1(&tt, identities[i], base.data, base.data),
			CLUSTER_UNDO_HEADER_ALREADY);
		UT_ASSERT_EQ(memcmp(base.data, expected.data, BLCKSZ), 0);
		base = before;
		for (unsigned fault = 0; fault < 8; fault++) {
			xl_xact_tt_commit bad = tt;
			uint64 full = identities[i];
			PGAlignedBlock saved;

			switch (fault) {
			case 0: full ^= UINT64_C(1) << 32; break; /* same raw, other epoch */
			case 1: full = 0; break;
			case 2: full = UINT64_MAX; break;
			case 3: bad.format_version = 1; break;
			case 4: bad.format_version = 3; break;
			case 5: bad.flags = 1; break;
			case 6: bad.reserved = 1; break;
			case 7: bad.commit_scn = UINT64_MAX; break;
			}
			memset(out.data, 0x5a, BLCKSZ);
			saved = out;
			UT_ASSERT_EQ(cluster_undo_prepare_xact_commit_v1(&bad, full, base.data, out.data),
				CLUSTER_UNDO_HEADER_BLOCKED);
			UT_ASSERT_EQ(memcmp(out.data, saved.data, BLCKSZ), 0);
			UT_ASSERT_EQ(memcmp(base.data, before.data, BLCKSZ), 0);
		}
		/* Physical generation controls stale decisions, never FullXid ordering. */
		((UndoSegmentHeaderDataV2 *)base.data)->wrap_count++;
		UT_ASSERT_EQ(cluster_undo_prepare_xact_commit_v1(&tt, identities[i], base.data, out.data),
			CLUSTER_UNDO_HEADER_SKIP_STALE);
		base = before;
		((UndoSegmentHeaderDataV2 *)base.data)->tt_layout_version = 0;
		UT_ASSERT_EQ(cluster_undo_prepare_xact_commit_v1(&tt, identities[i], base.data, out.data),
			CLUSTER_UNDO_HEADER_BLOCKED);
		UT_ASSERT_EQ(cluster_undo_prepare_xact_commit_v1(&tt, 0, before.data, out.data),
			CLUSTER_UNDO_HEADER_BLOCKED);
	}
}

UT_TEST(test_tt40_refuses_malformed_decoded_fields_and_preserves_caller_buffers)
{
	const uint64 full = (UINT64_C(7) << 32) | 1234;
	const uint8 opcodes[] = { XLOG_UNDO_TT_SLOT_BIND, XLOG_UNDO_TT_SLOT_COMMIT,
		XLOG_UNDO_TT_SLOT_ABORT, XLOG_UNDO_TT_SLOT_SET_HEAD, XLOG_UNDO_TT_SLOT_CTRC_RELEASE };
	PGAlignedBlock base, out, saved;

	for (Size c = 0; c < lengthof(opcodes); c++) {
		ClusterUndoDecoded good = make_tt40_decoded(opcodes[c], full);

		make_tt40_base(&base, full, tt40_predecessor_status(opcodes[c]));
		for (unsigned fault = 0; fault < 17; fault++) {
			ClusterUndoDecoded bad = good;

			switch (fault) {
			case 0: bad.format_version = 1; break;
			case 1: bad.format_version = 3; break;
			case 2: bad.flags = UINT8_MAX; break;
			case 3: bad.expected_generation = UINT32_MAX; break;
			case 4: bad.wrap = TT_WRAP_INVALID; break;
			case 5: bad.slot_offset = 48; break;
			case 6: bad.instance = 0; break;
			case 7: bad.instance = 129; break;
			case 8: bad.segment_id = 0; break;
			case 9: bad.instance = 2; break;
			case 10: bad.xid = 2; break;
			case 11: bad.original_full_xid = 0; break;
			case 12: bad.original_full_xid = UINT64_MAX; break;
			case 13: bad.has_payload = true; break;
			case 14: bad.has_fpi = true; break;
			case 15: bad.payload_length = 1; break;
			case 16: bad.opcode = XLOG_HW_RESERVE; break;
			}
			check_tt40_no_output(&bad, &base, CLUSTER_UNDO_HEADER_BLOCKED);
		}
		if (opcodes[c] == XLOG_UNDO_TT_SLOT_COMMIT) {
			good.commit_scn = UINT64_MAX;
			check_tt40_no_output(&good, &base, CLUSTER_UNDO_HEADER_BLOCKED);
			good.commit_scn = InvalidScn;
			check_tt40_no_output(&good, &base, CLUSTER_UNDO_HEADER_BLOCKED);
		} else if (opcodes[c] == XLOG_UNDO_TT_SLOT_SET_HEAD) {
			good.first_undo_block.raw[0] = 8 | (UINT64_C(9) << 32);
			check_tt40_no_output(&good, &base, CLUSTER_UNDO_HEADER_BLOCKED);
			good.first_undo_block.raw[0] = 7 | (UINT64_C(9) << 32);
			good.first_undo_block.raw[1] = 4;
			check_tt40_no_output(&good, &base, CLUSTER_UNDO_HEADER_BLOCKED);
		} else if (opcodes[c] == XLOG_UNDO_TT_SLOT_CTRC_RELEASE) {
			good.root_id = 0;
			check_tt40_no_output(&good, &base, CLUSTER_UNDO_HEADER_BLOCKED);
			good = make_tt40_decoded(opcodes[c], full);
			good.cluster_epoch = 0;
			check_tt40_no_output(&good, &base, CLUSTER_UNDO_HEADER_BLOCKED);
			good = make_tt40_decoded(opcodes[c], full);
			good.formation_epoch = 0;
			check_tt40_no_output(&good, &base, CLUSTER_UNDO_HEADER_BLOCKED);
			good = make_tt40_decoded(opcodes[c], full);
			good.expected_generation = 0;
			((UndoSegmentHeaderDataV2 *)base.data)->wrap_count = 0;
			check_tt40_no_output(&good, &base, CLUSTER_UNDO_HEADER_BLOCKED);
			((UndoSegmentHeaderDataV2 *)base.data)->wrap_count = 9;
		}
		good = make_tt40_decoded(opcodes[c], full);
		memset(out.data, 0x6a, BLCKSZ);
		saved = out;
		UT_ASSERT_EQ(cluster_undo_prepare_header_v1(&good, NULL, 0, NULL, out.data),
			CLUSTER_UNDO_HEADER_BLOCKED);
		UT_ASSERT_EQ(cluster_undo_prepare_header_v1(&good, NULL, 0, base.data, NULL),
			CLUSTER_UNDO_HEADER_BLOCKED);
		UT_ASSERT_EQ(cluster_undo_prepare_header_v1(&good, (const uint8 *)base.data, 0,
			base.data, out.data), CLUSTER_UNDO_HEADER_BLOCKED);
		UT_ASSERT_EQ(cluster_undo_prepare_header_v1(&good, NULL, 1, base.data, out.data),
			CLUSTER_UNDO_HEADER_BLOCKED);
		UT_ASSERT_EQ(memcmp(out.data, saved.data, BLCKSZ), 0);
	}
}

int
main(void)
{
	UT_PLAN(34);

	UT_RUN(test_decode_tt_commit_fields);
	UT_RUN(test_decode_tt_bind_exact_shape_and_reserved_bytes);
	UT_RUN(test_decode_tt_commit_v2_preserves_complete_identity);
	UT_RUN(test_decode_tt_abort_v2_preserves_complete_identity);
	UT_RUN(test_decode_tt_head_v2_preserves_complete_identity);
	UT_RUN(test_decode_tt_release_v2_preserves_complete_identity);
	UT_RUN(test_terminal_v2_rejects_malformed_versions_lengths_and_identity);
	UT_RUN(test_terminal_v2_never_reaches_legacy_target_apply_or_header);
	UT_RUN(test_release_v2_codec_preserves_original_fields_and_rejects_bad_inputs);
	UT_RUN(test_terminal_v2_descriptors_keep_exact_full_identity);
	UT_RUN(test_legacy_terminal_descriptions_remain_legacy);
	UT_RUN(test_decode_tt_bind_v2_preserves_epoch_zero_and_original_epoch);
	UT_RUN(test_decode_tt_bind_v2_rejects_unknown_shape_and_raw_identity);
	UT_RUN(test_tt_bind_v2_stays_closed_before_any_legacy_mutation);
	UT_RUN(test_tt_bind_descriptor_uses_exact_v1_or_v2_identity);
	UT_RUN(test_decode_malformed_and_unknown_blocked);
	UT_RUN(test_preflight_field_integrity_and_route);
	UT_RUN(test_decode_block_write_fields);
	UT_RUN(test_decode_set_head_and_multi_exact_shape);
	UT_RUN(test_typed_tt_apply_requires_exact_post_read);
	UT_RUN(test_typed_tt_bind_applies_only_exact_generation_and_predecessor);
	UT_RUN(test_typed_exact_abort_requires_same_active_predecessor);
	UT_RUN(test_typed_ctrc_release_requires_exact_terminal_generation_and_post_read);
	UT_RUN(test_overlapping_undo_record_and_slot_ranges_refuse);
	UT_RUN(test_tt40_bind_prepares_exact_full_identity_without_touching_other_bytes);
	UT_RUN(test_tt40_terminal_successors_preserve_original_full_identity);
	UT_RUN(test_tt40_head_and_release_preserve_complete_predecessor_identity);
	UT_RUN(test_tt40_all_slot_offsets_and_unaligned_private_copies);
	UT_RUN(test_tt40_raw_match_never_borrows_or_repairs_another_full_identity);
	UT_RUN(test_tt40_checks_explicit_page_layout_before_shifted_target_fields);
	UT_RUN(test_tt40_physical_stale_and_exact_idempotent_results_do_not_publish_output);
	UT_RUN(test_tt40_bind_reuse_uses_original_wrap_transition_and_replaces_only_at_bind);
	UT_RUN(test_folded_v2_prepares_exact_tt40_successor_without_raw_backfill);
	UT_RUN(test_tt40_refuses_malformed_decoded_fields_and_preserves_caller_buffers);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
