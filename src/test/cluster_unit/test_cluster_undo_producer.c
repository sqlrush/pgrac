/*-------------------------------------------------------------------------
 * test_cluster_undo_producer.c
 *    Actual typed UNDO producers through native WAL decoding and preflight.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xloginsert.h"
#include "cluster/cluster_side_undo.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/storage/cluster_undo_buf.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "storage/proc.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static char payload[BLCKSZ + 256];
static uint32 payload_length;
static RmgrId emitted_rmid;
static uint8 emitted_info;
static bool writeback, full_page_writes;
static XLogRecPtr checkpoint_redo;
static PGPROC test_proc;
PGPROC *MyProc = &test_proc;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

/* Only insertion/runtime boundaries are substituted. Record headers, fields,
 * FPI selection and delta ranges below come from the production emitters. */
void
XLogBeginInsert(void)
{
	payload_length = 0;
	memset(payload, 0, sizeof(payload));
}

void
XLogRegisterData(char *data, uint32 length)
{
	if (length > sizeof(payload) - payload_length)
		abort();
	memcpy(payload + payload_length, data, length);
	payload_length += length;
}

XLogRecPtr
XLogInsert(RmgrId rmid, uint8 info)
{
	emitted_rmid = rmid;
	emitted_info = info;
	return 0x900;
}

bool
cluster_undo_buf_writeback_allowed(void)
{
	return writeback;
}

void
GetFullPageWriteInfo(XLogRecPtr *redo, bool *writes)
{
	*redo = 1; /* Intentionally stale: the producer must use GetRedoRecPtr. */
	*writes = full_page_writes;
}

XLogRecPtr
GetRedoRecPtr(void)
{
	return checkpoint_redo;
}

#undef ereport
#define ereport(level_, rest_) abort()
#include "test_cluster_undo_producer_owner.inc"

static ClusterUndoDecoded
decode_emitted(bool accepted)
{
	union {
		XLogRecord align;
		char bytes[BLCKSZ + 512];
	} wire;
	XLogRecord *header = (XLogRecord *)wire.bytes;
	XLogReaderState reader = {0};
	DecodedXLogRecord *decoded;
	ClusterUndoDecoded result = {0};
	char error_buffer[1024];
	char *error = NULL;
	char *p = wire.bytes + SizeOfXLogRecord;
	bool ok;

	memset(&wire, 0, sizeof(wire));
	header->xl_rmid = emitted_rmid;
	header->xl_info = emitted_info;
	header->xl_scn = 51;
	*p++ = (char)XLR_BLOCK_ID_DATA_LONG;
	memcpy(p, &payload_length, sizeof(payload_length));
	p += sizeof(payload_length);
	memcpy(p, payload, payload_length);
	header->xl_tot_len = (p - wire.bytes) + payload_length;
	decoded = calloc(1, DecodeXLogRecordRequiredSpace(header->xl_tot_len));
	if (decoded == NULL)
		abort();
	reader.errormsg_buf = error_buffer;
	ok = DecodeXLogRecord(&reader, decoded, header, 0x800, &error);
	UT_ASSERT(ok);
	if (!ok)
		abort();
	reader.record = decoded;
	UT_ASSERT_EQ(decoded->max_block_id, -1);
	UT_ASSERT(!decoded->has_page_version_edge);
	UT_ASSERT_EQ(decoded->main_data_len, payload_length);
	UT_ASSERT(memcmp(decoded->main_data, payload, payload_length) == 0);
	UT_ASSERT(cluster_undo_decode(&reader, &result));
	UT_ASSERT_EQ(cluster_undo_preflight(&result), accepted);
	free(decoded);
	return result;
}

UT_TEST(test_segment_lifecycle_producer_payloads)
{
	char image[BLCKSZ];
	ClusterUndoDecoded out;

	memset(image, 0x6b, sizeof(image));
	cluster_undo_emit_segment_init(3, 513, image);
	out = decode_emitted(true);
	UT_ASSERT_EQ(out.kind, CLUSTER_UNDO_KIND_SEGMENT_INIT);
	UT_ASSERT_EQ(out.instance, 3);
	UT_ASSERT_EQ(out.segment_id, 513);
	UT_ASSERT_EQ(out.payload_length, BLCKSZ);
	UT_ASSERT(memcmp(payload + out.payload_offset, image, BLCKSZ) == 0);
	cluster_undo_emit_segment_recycle(3, 513, 8, SEGMENT_COMMITTED, SEGMENT_RECYCLABLE);
	out = decode_emitted(true);
	UT_ASSERT_EQ(out.kind, CLUSTER_UNDO_KIND_SEGMENT_RECYCLE);
	UT_ASSERT_EQ(out.expected_generation, 8);
	UT_ASSERT_EQ(out.new_state, SEGMENT_RECYCLABLE);
	cluster_undo_emit_segment_reuse(3, 513, 8, 9, image);
	out = decode_emitted(true);
	UT_ASSERT_EQ(out.kind, CLUSTER_UNDO_KIND_SEGMENT_REUSE);
	UT_ASSERT_EQ(out.expected_generation, 8);
	UT_ASSERT_EQ(out.new_generation, 9);
	UT_ASSERT(memcmp(payload + out.payload_offset, image, BLCKSZ) == 0);
}

UT_TEST(test_tt_producer_identity_and_polarity)
{
	ClusterUndoDecoded out;
	UBA head = uba_encode(513, 2, 4, 0);

	cluster_undo_emit_tt_slot_bind(3, 513, 9, 4, 7, 801);
	out = decode_emitted(true);
	UT_ASSERT_EQ(out.kind, CLUSTER_UNDO_KIND_TT_BIND);
	UT_ASSERT_EQ(out.expected_generation, 9);
	UT_ASSERT_EQ(out.slot_offset, 4);
	UT_ASSERT_EQ(out.wrap, 7);
	UT_ASSERT_EQ(out.xid, 801);
	cluster_undo_emit_tt_slot_commit(3, 513, 4, 7, 801, 901);
	out = decode_emitted(true);
	UT_ASSERT_EQ(out.kind, CLUSTER_UNDO_KIND_TT_COMMIT);
	UT_ASSERT_EQ(out.commit_scn, 901);
	cluster_undo_emit_tt_slot_abort(3, 513, 4, 7, 801);
	out = decode_emitted(true);
	UT_ASSERT_EQ(out.kind, CLUSTER_UNDO_KIND_TT_ABORT);
	UT_ASSERT_EQ(out.format_version, 0);
	cluster_undo_emit_tt_slot_abort_exact(3, 513, 9, 4, 7, 801);
	out = decode_emitted(true);
	UT_ASSERT_EQ(out.kind, CLUSTER_UNDO_KIND_TT_ABORT);
	UT_ASSERT_EQ(out.expected_generation, 9);
	UT_ASSERT_EQ(out.format_version, CLUSTER_UNDO_TT_ABORT_EXACT_VERSION);
	cluster_undo_emit_tt_slot_set_head(3, 513, 4, 7, 801, head);
	out = decode_emitted(true);
	UT_ASSERT_EQ(out.kind, CLUSTER_UNDO_KIND_TT_SET_HEAD);
	UT_ASSERT(memcmp(&out.first_undo_block, &head, sizeof(head)) == 0);
}

UT_TEST(test_ctrc_release_producer_certificate)
{
	xl_undo_tt_slot_ctrc_release_v1 certificate = {0};
	ClusterUndoDecoded out;

	certificate.segment_id = 513;
	certificate.segment_generation = 9;
	certificate.xid = 801;
	certificate.cluster_epoch = 11;
	certificate.root_id = 12;
	certificate.root_generation = 13;
	certificate.formation_epoch = 14;
	certificate.admission_record_generation = 15;
	certificate.seal_generation = 16;
	certificate.touched_nodes_low = 4;
	certificate.slot_offset = 4;
	certificate.slot_wrap = 7;
	certificate.owner_instance = 3;
	certificate.terminal_status = TT_SLOT_COMMITTED;
	certificate.format_version = CLUSTER_UNDO_TT_CTRC_RELEASE_VERSION;
	certificate.flags = CLUSTER_UNDO_TT_CTRC_RELEASE_ALL_TOUCHED_ACKED;
	memset(certificate.ack_set_digest, 0x5a, sizeof(certificate.ack_set_digest));
	cluster_undo_xlog_insert_tt_ctrc_release(&certificate);
	out = decode_emitted(true);
	UT_ASSERT_EQ(out.kind, CLUSTER_UNDO_KIND_TT_CTRC_RELEASE);
	UT_ASSERT_EQ(out.expected_generation, 9);
	UT_ASSERT_EQ(out.formation_epoch, 14);
	UT_ASSERT_EQ(out.admission_record_generation, 15);
	UT_ASSERT_EQ(out.seal_generation, 16);
	UT_ASSERT_EQ(out.touched_nodes_low, 4);
	UT_ASSERT(memcmp(out.ack_set_digest, certificate.ack_set_digest, 16) == 0);
}

static void
check_block_producer(bool multi)
{
	char image[BLCKSZ];
	static const struct {
		bool writeback;
		bool fpw;
		XLogRecPtr before;
		bool fpi;
	} cases[] = {
		{false, false, 900, true},
		{true, true, 0, true},
		{true, true, 99, true},
		{true, true, 100, true},
		{true, true, 101, false},
		{true, false, 99, false}
	};
	uint16 rec_off = sizeof(UndoBlockHeader), rec_len = 73;
	uint16 slot_len = (multi ? 2 : 1) * sizeof(UndoSlotDirEntry);
	uint16 slot_off = BLCKSZ - slot_len;

	for (Size n = 0; n < sizeof(image); n++)
		image[n] = (char)(n * 17 + 3);
	checkpoint_redo = 100;
	test_proc.delayChkptFlags = DELAY_CHKPT_START;
	for (Size i = 0; i < lengthof(cases); i++) {
		ClusterUndoDecoded out;
		const char *body;
		writeback = cases[i].writeback;
		full_page_writes = cases[i].fpw;
		if (multi)
			cluster_undo_emit_block_write_multi(3, 513, 2, image, cases[i].before,
											  rec_off, rec_len, slot_off, slot_len);
		else
			cluster_undo_emit_block_write(3, 513, 2, image, cases[i].before,
										rec_off, rec_len, slot_off);
		out = decode_emitted(true);
		UT_ASSERT_EQ(out.kind, multi ? CLUSTER_UNDO_KIND_BLOCK_WRITE_MULTI
									: CLUSTER_UNDO_KIND_BLOCK_WRITE);
		UT_ASSERT_EQ(out.has_fpi, cases[i].fpi);
		UT_ASSERT_EQ(out.block_no, 2);
		body = payload + out.payload_offset;
		if (out.has_fpi) {
			UT_ASSERT_EQ(out.payload_length, BLCKSZ);
			UT_ASSERT(memcmp(body, image, BLCKSZ) == 0);
		} else {
			UT_ASSERT_EQ(out.payload_length, UNDO_BLOCK_HDR_PREFIX_LEN + rec_len + slot_len);
			UT_ASSERT_EQ(out.slot_len, slot_len);
			UT_ASSERT(memcmp(body, image, UNDO_BLOCK_HDR_PREFIX_LEN) == 0);
			body += UNDO_BLOCK_HDR_PREFIX_LEN;
			UT_ASSERT(memcmp(body, image + rec_off, rec_len) == 0);
			UT_ASSERT(memcmp(body + rec_len, image + slot_off, slot_len) == 0);
		}
	}
}

UT_TEST(test_block_write_producer_checkpoint_boundary)
{
	check_block_producer(false);
}

UT_TEST(test_block_write_multi_producer_checkpoint_boundary)
{
	check_block_producer(true);
}

UT_TEST(test_unowned_segment_and_hwm_do_not_gain_authority)
{
	ClusterUndoDecoded out;
	cluster_undo_emit_tt_slot_commit(2, 513, 4, 7, 801, 901);
	(void)decode_emitted(false);
	cluster_hw_emit_reserve((RelFileLocator){1, 2, 10}, MAIN_FORKNUM, 128, 16);
	out = decode_emitted(false);
	UT_ASSERT_EQ(out.kind, CLUSTER_UNDO_KIND_HW_RESERVE);
	UT_ASSERT_EQ(out.block_no, 128);
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_segment_lifecycle_producer_payloads);
	UT_RUN(test_tt_producer_identity_and_polarity);
	UT_RUN(test_ctrc_release_producer_certificate);
	UT_RUN(test_block_write_producer_checkpoint_boundary);
	UT_RUN(test_block_write_multi_producer_checkpoint_boundary);
	UT_RUN(test_unowned_segment_and_hwm_do_not_gain_authority);
	UT_DONE();
	return ut_failed_count != 0;
}
