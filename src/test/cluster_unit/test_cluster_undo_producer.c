/*-------------------------------------------------------------------------
 * test_cluster_undo_producer.c
 *    Actual typed UNDO producers through native WAL decoding and preflight.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>

#include "access/xlog.h"
#include "access/xloginsert.h"
#include "cluster/cluster_side_undo.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/storage/cluster_undo_alloc.h"
#include "cluster/storage/cluster_undo_buf.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "storage/proc.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static char payload[BLCKSZ + 256];
static uint32 payload_length;
static uint32 inserts_begun;
static RmgrId emitted_rmid;
static uint8 emitted_info;
static bool writeback, full_page_writes;
static XLogRecPtr checkpoint_redo;
static PGPROC test_proc;
PGPROC *MyProc = &test_proc;
static jmp_buf panic_jump;
static bool expect_panic;

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
	inserts_begun++;
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
#define ereport(level_, rest_)                                                                     \
	do {                                                                                           \
		if (expect_panic)                                                                          \
			longjmp(panic_jump, 1);                                                                \
		abort();                                                                                   \
	} while (0)
#include "test_cluster_undo_producer_owner.inc"

/* Execute the actual native block writer; only path and physical I/O are
 * fixture boundaries. The production dispatcher supplies decoded fields. */
static PGAlignedBlock native_disk;
static uint32 native_reads, native_writes, native_syncs, native_closes, native_applies;

static int
fixture_path(int intent, uint8 instance, uint32 segment, char *path, size_t length)
{
	UT_ASSERT_EQ(instance, 3);
	UT_ASSERT_EQ(segment, 513);
	strlcpy(path, "owned-undo-segment", length);
	return 0;
}

static int
fixture_open(uint8 instance, uint32 segment, const char *path, bool create)
{
	UT_ASSERT_EQ(instance, 3);
	UT_ASSERT_EQ(segment, 513);
	return 42;
}

static ssize_t
fixture_read(int fd, void *buffer, size_t length, off_t offset)
{
	UT_ASSERT_EQ(fd, 42);
	UT_ASSERT_EQ(length, BLCKSZ);
	UT_ASSERT_EQ(offset, 2 * BLCKSZ);
	native_reads++;
	memcpy(buffer, native_disk.data, BLCKSZ);
	return BLCKSZ;
}

static ssize_t
fixture_write(int fd, const void *buffer, size_t length, off_t offset)
{
	UT_ASSERT_EQ(fd, 42);
	UT_ASSERT_EQ(length, BLCKSZ);
	UT_ASSERT_EQ(offset, 2 * BLCKSZ);
	UT_ASSERT_EQ(native_syncs, 0);
	native_writes++;
	memcpy(native_disk.data, buffer, BLCKSZ);
	return BLCKSZ;
}

static int
fixture_sync(int fd)
{
	UT_ASSERT_EQ(fd, 42);
	UT_ASSERT_EQ(native_writes, 1);
	UT_ASSERT_EQ(native_closes, 0);
	native_syncs++;
	return 0;
}

static int
fixture_close(int fd)
{
	UT_ASSERT_EQ(fd, 42);
	native_closes++;
	return 0;
}

#define build_undo_segment_path fixture_path
#define cluster_undo_recovery_intent_for_owner(instance_) CLUSTER_UNDO_PATH_RECOVERY_SHARED
#define cluster_undo_redo_open_segment fixture_open
#undef pg_pread
#undef pg_pwrite
#define pg_pread fixture_read
#define pg_pwrite fixture_write
#define pg_fsync fixture_sync
#define close fixture_close
#define cluster_vis_bump_recovery_undo_redo_applies() (native_applies++)
#include "test_cluster_undo_block_native.inc"
#undef build_undo_segment_path
#undef cluster_undo_recovery_intent_for_owner
#undef cluster_undo_redo_open_segment
#undef pg_pread
#undef pg_pwrite
#undef pg_fsync
#undef close
#undef cluster_vis_bump_recovery_undo_redo_applies

static ClusterUndoDecoded
decode_emitted(bool accepted)
{
	union {
		XLogRecord align;
		char bytes[BLCKSZ + 512];
	} wire;
	XLogRecord *header = (XLogRecord *)wire.bytes;
	XLogReaderState reader = { 0 };
	DecodedXLogRecord *decoded;
	ClusterUndoDecoded result = { 0 };
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
	xl_undo_tt_slot_ctrc_release_v1 certificate = { 0 };
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
	} cases[]
		= { { false, false, 900, true }, { true, true, 0, true },	 { true, true, 99, true },
			{ true, true, 100, true },	 { true, true, 101, false }, { true, false, 99, false } };
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
		PGAlignedBlock base, prepared, expected;
		writeback = cases[i].writeback;
		full_page_writes = cases[i].fpw;
		if (multi)
			cluster_undo_emit_block_write_multi(3, 513, 2, image, cases[i].before, rec_off, rec_len,
												slot_off, slot_len);
		else
			cluster_undo_emit_block_write(3, 513, 2, image, cases[i].before, rec_off, rec_len,
										  slot_off);
		out = decode_emitted(true);
		UT_ASSERT_EQ(out.kind,
					 multi ? CLUSTER_UNDO_KIND_BLOCK_WRITE_MULTI : CLUSTER_UNDO_KIND_BLOCK_WRITE);
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
		memset(base.data, 0x5c, BLCKSZ);
		((UndoBlockHeader *)base.data)->block_lsn = 13;
		expected = base;
		if (out.has_fpi)
			memcpy(expected.data, image, BLCKSZ);
		else {
			memcpy(expected.data, image, UNDO_BLOCK_HDR_PREFIX_LEN);
			memcpy(expected.data + rec_off, image + rec_off, rec_len);
			memcpy(expected.data + slot_off, image + slot_off, slot_len);
		}
		((UndoBlockHeader *)expected.data)->block_lsn = 0x900;
		UT_ASSERT(cluster_undo_prepare_block_v1(&out, (const uint8 *)payload + out.payload_offset,
												out.payload_length, 0x900,
												out.has_fpi ? NULL : base.data, prepared.data));
		UT_ASSERT(memcmp(prepared.data, expected.data, BLCKSZ) == 0);
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
	cluster_hw_emit_reserve((RelFileLocator){ 1, 2, 10 }, MAIN_FORKNUM, 128, 16);
	out = decode_emitted(false);
	UT_ASSERT_EQ(out.kind, CLUSTER_UNDO_KIND_HW_RESERVE);
	UT_ASSERT_EQ(out.block_no, 128);
}

UT_TEST(test_private_block_preparation_refusal_is_atomic)
{
	PGAlignedBlock image, output, before;
	ClusterUndoDecoded decoded;

	memset(image.data, 0x4d, BLCKSZ);
	writeback = full_page_writes = true;
	checkpoint_redo = 100;
	cluster_undo_emit_block_write_multi(3, 513, 2, image.data, 101, sizeof(UndoBlockHeader), 73,
										BLCKSZ - 16, 16);
	decoded = decode_emitted(true);
	for (int variant = 0; variant < 5; variant++) {
		ClusterUndoDecoded bad = decoded;
		Size length = bad.payload_length;
		XLogRecPtr end = 0x900;
		const char *base;

		memset(output.data, 0x5c, BLCKSZ);
		((UndoBlockHeader *)output.data)->block_lsn = 13;
		base = output.data;
		if (variant == 0)
			base = NULL;
		if (variant == 1)
			((UndoBlockHeader *)output.data)->block_lsn = 0;
		if (variant == 2)
			length--;
		if (variant == 3)
			bad.slot_off = bad.rec_off + bad.rec_len - 1;
		if (variant == 4)
			end = 0;
		before = output;
		UT_ASSERT(!cluster_undo_prepare_block_v1(&bad, (const uint8 *)payload + bad.payload_offset,
												 length, end, base, output.data));
		UT_ASSERT(memcmp(output.data, before.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_native_block_writer_uses_typed_preparation_before_write_and_sync)
{
	PGAlignedBlock image, expected;
	ClusterUndoDecoded decoded;

	memset(image.data, 0x4d, BLCKSZ);
	writeback = full_page_writes = true;
	checkpoint_redo = 100;
	for (int variant = 0; variant < 3; variant++) {
		const uint8 *body;
		cluster_undo_emit_block_write_multi(3, 513, 2, image.data, variant == 0 ? 0 : 101,
											sizeof(UndoBlockHeader), 73, BLCKSZ - 16, 16);
		decoded = decode_emitted(true);
		body = (const uint8 *)payload + decoded.payload_offset;
		memset(native_disk.data, 0x5c, BLCKSZ);
		((UndoBlockHeader *)native_disk.data)->block_lsn = variant == 2 ? 0 : 13;
		expected = native_disk;
		native_reads = native_writes = native_syncs = native_closes = native_applies = 0;
		if (variant != 2) {
			UT_ASSERT(cluster_undo_prepare_block_v1(&decoded, body, decoded.payload_length, 0x900,
													native_disk.data, expected.data));
			cluster_undo_redo_block_write(&decoded, body, 0x900);
			UT_ASSERT_EQ(native_reads, variant == 0 ? 0 : 1);
			UT_ASSERT_EQ(native_writes, 1);
			UT_ASSERT_EQ(native_syncs, 1);
			UT_ASSERT_EQ(native_applies, 1);
		} else {
			expect_panic = true;
			if (setjmp(panic_jump) == 0) {
				cluster_undo_redo_block_write(&decoded, body, 0x900);
				UT_ASSERT(false);
			}
			expect_panic = false;
			UT_ASSERT_EQ(native_writes, 0);
			UT_ASSERT_EQ(native_syncs, 0);
			UT_ASSERT_EQ(native_applies, 0);
		}
		UT_ASSERT_EQ(native_closes, 1);
		UT_ASSERT(memcmp(native_disk.data, expected.data, BLCKSZ) == 0);
	}
}


/* Explicit V2 producer, through the original XLogInsert boundary and the
 * actual native decoder. An epoch-zero normal FullXid is also valid. */
UT_TEST(test_v2_bind_producer_preserves_original_fullxid_and_opcode)
{
	for (unsigned epoch = 0; epoch < 2; epoch++) {
		xl_undo_tt_identity_v2 identity = { 0 }, emitted;
		ClusterUndoDecoded decoded;

		identity.segment_id = 513;
		identity.segment_generation = 9;
		identity.xid = 801;
		identity.slot_offset = 4;
		identity.wrap = 7;
		identity.instance = 3;
		identity.format_version = CLUSTER_UNDO_TT_IDENTITY_V2_VERSION;
		identity.original_full_xid = ((uint64)(epoch ? 7 : 0) << 32) | identity.xid;
		UT_ASSERT_EQ(cluster_undo_emit_tt_slot_bind_v2(&identity), 0x900);
		UT_ASSERT_EQ(emitted_rmid, RM_CLUSTER_UNDO_ID);
		UT_ASSERT_EQ(emitted_info, XLOG_UNDO_TT_SLOT_BIND);
		UT_ASSERT_EQ(payload_length, 32);
		UT_ASSERT(memcmp(payload, &identity, sizeof(identity)) == 0);
		UT_ASSERT(cluster_undo_tt_bind_v2_decode(payload, payload_length, &emitted));
		UT_ASSERT(memcmp(&emitted, &identity, sizeof(identity)) == 0);
		/* The generic TT32 consumer stays closed to V2; its explicit typed
		 * consumer must retain the complete original identity. */
		decoded = decode_emitted(false);
		UT_ASSERT(cluster_undo_preflight_tt_v2(&decoded));
		UT_ASSERT_EQ(decoded.original_full_xid, identity.original_full_xid);
		UT_ASSERT_EQ(decoded.expected_generation, identity.segment_generation);
	}
}

UT_TEST(test_v2_bind_producer_refuses_bad_identity_before_insertion)
{
	for (unsigned fault = 0; fault < 12; fault++) {
		xl_undo_tt_identity_v2 identity = { 0 };

		identity.segment_id = 513;
		identity.segment_generation = 9;
		identity.xid = 801;
		identity.slot_offset = 4;
		identity.wrap = 7;
		identity.instance = 3;
		identity.format_version = CLUSTER_UNDO_TT_IDENTITY_V2_VERSION;
		identity.original_full_xid = (UINT64_C(7) << 32) | identity.xid;
		switch (fault) {
		case 0: identity.format_version = 1; break;
		case 1: identity.flags = 1; break;
		case 2: identity.reserved = 1; break;
		case 3: identity.original_full_xid = UINT64_MAX; break;
		case 4: identity.original_full_xid++; break;
		case 5: identity.xid = FrozenTransactionId; break;
		case 6: identity.instance = 0; break;
		case 7: identity.segment_id = 1; break;
		case 8: identity.slot_offset = TT_SLOTS_PER_SEGMENT; break;
		case 9: identity.wrap = TT_WRAP_INVALID; break;
		case 10: identity.segment_generation = UINT32_MAX; break;
		case 11: break; /* NULL payload */
		}
		payload_length = 123;
		emitted_info = 0xee;
		expect_panic = true;
		if (setjmp(panic_jump) == 0) {
			(void)cluster_undo_emit_tt_slot_bind_v2(fault == 11 ? NULL : &identity);
			UT_ASSERT(false);
		}
		expect_panic = false;
		UT_ASSERT_EQ(payload_length, 123);
		UT_ASSERT_EQ(emitted_info, 0xee);
	}
}


UT_TEST(test_v2_abort_producer_preserves_original_identity_and_rejects_before_insert)
{
	for (unsigned fault = 0; fault < 13; fault++) {
		xl_undo_tt_identity_v2 identity = { .segment_id = 513, .segment_generation = 9,
			.xid = 801, .slot_offset = 4, .wrap = 7, .instance = 3,
			.format_version = CLUSTER_UNDO_TT_IDENTITY_V2_VERSION,
			.original_full_xid = UINT64_C(0x700000321) };
		ClusterUndoDecoded decoded;
		switch (fault) {
		case 1: identity.format_version = 1; break;
		case 2: identity.flags = 1; break;
		case 3: identity.reserved = 1; break;
		case 4: identity.original_full_xid = UINT64_MAX; break;
		case 5: identity.original_full_xid++; break;
		case 6: identity.xid = FrozenTransactionId; break;
		case 7: identity.instance = 0; break;
		case 8: identity.segment_id = 1; break;
		case 9: identity.slot_offset = TT_SLOTS_PER_SEGMENT; break;
		case 10: identity.wrap = TT_WRAP_INVALID; break;
		case 11: identity.segment_generation = UINT32_MAX; break;
		case 12: break;
		}
		payload_length = 123;
		emitted_info = 0xee;
		if (fault == 0) {
			UT_ASSERT_EQ(cluster_undo_emit_tt_slot_abort_v2(&identity), 0x900);
			UT_ASSERT_EQ(emitted_rmid, RM_CLUSTER_UNDO_ID);
			UT_ASSERT_EQ(emitted_info, XLOG_UNDO_TT_SLOT_ABORT);
			UT_ASSERT_EQ(payload_length, sizeof(identity));
			UT_ASSERT_EQ(memcmp(payload, &identity, sizeof(identity)), 0);
			decoded = decode_emitted(false);
			UT_ASSERT(cluster_undo_preflight_tt_v2(&decoded));
			UT_ASSERT_EQ(decoded.original_full_xid, identity.original_full_xid);
		} else {
			expect_panic = true;
			if (setjmp(panic_jump) == 0) {
				(void)cluster_undo_emit_tt_slot_abort_v2(fault == 12 ? NULL : &identity);
				UT_ASSERT(false);
			}
			expect_panic = false;
			UT_ASSERT_EQ(payload_length, 123);
			UT_ASSERT_EQ(emitted_info, 0xee);
		}
	}
}


UT_TEST(test_v2_release_producer_preserves_104_bytes_and_rejects_before_insert)
{
	for (unsigned fault = 0; fault < 21; fault++) {
		xl_undo_tt_slot_ctrc_release_v2 record = { 0 };
		uint8 expected[CLUSTER_UNDO_TT_CTRC_RELEASE_V2_BYTES];
		ClusterUndoDecoded decoded;
		record.release.segment_id = 513; record.release.segment_generation = 9;
		record.release.xid = 801; record.release.cluster_epoch = 11;
		record.release.root_id = 12; record.release.root_generation = 13;
		record.release.formation_epoch = 14; record.release.admission_record_generation = 15;
		record.release.seal_generation = 16; record.release.touched_nodes_low = 4;
		record.release.slot_offset = 4; record.release.slot_wrap = 7;
		record.release.owner_instance = 3; record.release.terminal_status = TT_SLOT_COMMITTED;
		record.release.format_version = CLUSTER_UNDO_TT_CTRC_RELEASE_V2_VERSION;
		record.release.flags = CLUSTER_UNDO_TT_CTRC_RELEASE_ALL_TOUCHED_ACKED;
		memset(record.release.ack_set_digest, 0x5a, 16);
		record.original_full_xid = UINT64_C(0x700000321);
		switch (fault) {
		case 1: record.release.format_version = 1; break;
		case 2: record.original_full_xid = 0; break;
		case 3: record.original_full_xid = UINT64_MAX; break;
		case 4: record.original_full_xid++; break;
		case 5: record.release.xid = FrozenTransactionId; record.original_full_xid = FrozenTransactionId; break;
		case 6: record.release.slot_offset = TT_SLOTS_PER_SEGMENT; break;
		case 7: record.release.slot_wrap = TT_WRAP_INVALID; break;
		case 8: record.release.owner_instance = 0; break;
		case 9: record.release.root_id = 0; break;
		case 10: record.release.seal_generation = 0; break;
		case 11: record.release.touched_nodes_high = 1; break;
		case 12: record.release.flags = 0; break;
		case 13: record.release.terminal_status = TT_SLOT_ACTIVE; break;
		case 14: record.release.segment_generation = UINT32_MAX; break;
		case 15: record.release.segment_id = 1; break; /* Different physical owner. */
		case 16: record.release.segment_id = UINT32_MAX; break;
		case 17: break; /* NULL input. */
		case 18: record.release.segment_generation = 0; break;
		case 19: record.release.cluster_epoch = 0; break;
		case 20: record.release.formation_epoch = 0; break;
		}
		payload_length = 123; emitted_info = 0xee;
		if (fault == 0) {
			UT_ASSERT(cluster_undo_tt_ctrc_release_v2_encode(&record, expected));
			UT_ASSERT_EQ(cluster_undo_xlog_insert_tt_ctrc_release_v2(&record), 0x900);
			UT_ASSERT_EQ(payload_length, 104); UT_ASSERT_EQ(emitted_rmid, RM_CLUSTER_UNDO_ID);
			UT_ASSERT_EQ(emitted_info, XLOG_UNDO_TT_SLOT_CTRC_RELEASE);
			UT_ASSERT_EQ(memcmp(payload, expected, 104), 0);
			decoded = decode_emitted(false);
			UT_ASSERT(cluster_undo_preflight_tt_v2(&decoded));
			UT_ASSERT_EQ(decoded.original_full_xid, record.original_full_xid);
			UT_ASSERT_EQ(decoded.expected_generation, 9);
			UT_ASSERT_EQ(decoded.seal_generation, 16);
			UT_ASSERT_EQ(memcmp(decoded.ack_set_digest, record.release.ack_set_digest, 16), 0);
		} else {
			expect_panic = true;
			if (setjmp(panic_jump) == 0) {
				(void)cluster_undo_xlog_insert_tt_ctrc_release_v2(fault == 17 ? NULL : &record);
				UT_ASSERT(false);
			}
			expect_panic = false;
			UT_ASSERT_EQ(payload_length, 123); UT_ASSERT_EQ(emitted_info, 0xee);
		}
	}
}

UT_TEST(test_v2_prepared_commit_and_head_producers_reach_original_decoder)
{
	for (unsigned epoch = 0; epoch < 2; epoch++) {
		xl_undo_tt_identity_v2 identity = { .segment_id = 513, .segment_generation = 9,
			.xid = 801, .slot_offset = 4, .wrap = 7, .instance = 3,
			.format_version = CLUSTER_UNDO_TT_IDENTITY_V2_VERSION };
		xl_undo_tt_slot_commit_v2 commit;
		xl_undo_tt_slot_set_head_v2 head_record;
		ClusterUndoDecoded decoded;
		UBA head = uba_encode(513, 2, 4, 0);
		identity.original_full_xid = ((uint64)(epoch ? 7 : 0) << 32) | identity.xid;
		UT_ASSERT_EQ(cluster_undo_emit_tt_slot_commit_v2(&identity, 42), 0x900);
		UT_ASSERT_EQ(payload_length, 40);
		UT_ASSERT_EQ(emitted_info, XLOG_UNDO_TT_SLOT_COMMIT);
		UT_ASSERT(cluster_undo_tt_commit_v2_decode(payload, payload_length, &commit));
		UT_ASSERT_EQ(memcmp(&commit.identity, &identity, sizeof(identity)), 0);
		UT_ASSERT_EQ(commit.commit_scn, 42);
		decoded = decode_emitted(false);
		UT_ASSERT(cluster_undo_preflight_tt_v2(&decoded));
		UT_ASSERT_EQ(decoded.original_full_xid, identity.original_full_xid);
		UT_ASSERT_EQ(decoded.commit_scn, 42);
		UT_ASSERT_EQ(cluster_undo_emit_tt_slot_set_head_v2(&identity, head), 0x900);
		UT_ASSERT_EQ(payload_length, 48);
		UT_ASSERT_EQ(emitted_info, XLOG_UNDO_TT_SLOT_SET_HEAD);
		UT_ASSERT(cluster_undo_tt_set_head_v2_decode(payload, payload_length, &head_record));
		UT_ASSERT_EQ(memcmp(&head_record.identity, &identity, sizeof(identity)), 0);
		UT_ASSERT_EQ(memcmp(&head_record.first_undo_block, &head, sizeof(head)), 0);
		decoded = decode_emitted(false);
		UT_ASSERT(cluster_undo_preflight_tt_v2(&decoded));
		UT_ASSERT_EQ(decoded.original_full_xid, identity.original_full_xid);
		UT_ASSERT_EQ(memcmp(&decoded.first_undo_block, &head, sizeof(head)), 0);
	}
}

UT_TEST(test_v2_prepared_emitters_reject_bad_identity_scn_or_head_before_insert)
{
	for (unsigned is_head = 0; is_head < 2; is_head++) {
		for (unsigned fault = 0; fault < 15; fault++) {
			xl_undo_tt_identity_v2 identity = { .segment_id = 513, .segment_generation = 9,
				.xid = 801, .slot_offset = 4, .wrap = 7, .instance = 3,
				.format_version = CLUSTER_UNDO_TT_IDENTITY_V2_VERSION,
				.original_full_xid = UINT64_C(0x700000321) };
			UBA head = uba_encode(513, 2, 4, 0);
			SCN scn = 42;
			switch (fault) {
			case 0: identity.format_version = 1; break;
			case 1: identity.flags = 1; break;
			case 2: identity.reserved = 1; break;
			case 3: identity.original_full_xid = UINT64_MAX; break;
			case 4: identity.original_full_xid++; break;
			case 5: identity.xid = FrozenTransactionId; break;
			case 6: identity.instance = 0; break;
			case 7: identity.segment_id = 1; break;
			case 8: identity.slot_offset = TT_SLOTS_PER_SEGMENT; break;
			case 9: identity.wrap = TT_WRAP_INVALID; break;
			case 10: identity.segment_generation = UINT32_MAX; break;
			case 11: break; /* NULL identity */
			case 12: scn = InvalidScn; head = (UBA)InvalidUba_init; break;
			case 13: scn = UINT64_MAX; head = uba_encode(513, 2, 5, 0); break;
			case 14: scn = UINT64_MAX; head = uba_encode(514, 2, 4, 0); break;
			}
			payload_length = 123; emitted_info = 0xee;
			expect_panic = true;
			if (setjmp(panic_jump) == 0) {
				if (is_head)
					(void)cluster_undo_emit_tt_slot_set_head_v2(fault == 11 ? NULL : &identity, head);
				else
					(void)cluster_undo_emit_tt_slot_commit_v2(fault == 11 ? NULL : &identity, scn);
				UT_ASSERT(false);
			}
			expect_panic = false;
			UT_ASSERT_EQ(payload_length, 123);
			UT_ASSERT_EQ(emitted_info, 0xee);
		}
	}
}

static xl_undo_tt_slot_set_head_operation_v2
producer_operation(uint64 top)
{
	xl_undo_tt_slot_set_head_operation_v2 record = { 0 };

	record.head.identity.segment_id = 513;
	record.head.identity.segment_generation = 9;
	record.head.identity.xid = (TransactionId)top;
	record.head.identity.slot_offset = 4;
	record.head.identity.wrap = 7;
	record.head.identity.instance = 3;
	record.head.identity.format_version = CLUSTER_UNDO_TT_IDENTITY_V2_VERSION;
	record.head.identity.flags = CLUSTER_UNDO_TT_SET_HEAD_OPERATION_FLAG;
	record.head.identity.original_full_xid = top;
	/* A retained data segment may differ from the canonical TT segment. */
	record.head.first_undo_block = uba_encode(514, 2, 4, 0);
	record.operation.version = UNDO_OPERATION_TRAILER_VERSION;
	record.operation.length = UNDO_OPERATION_TRAILER_LENGTH;
	record.operation.effect_state = CLUSTER_SAVEPOINT_EFFECT_LIVE;
	record.operation.canonical_top_fxid = top;
	record.operation.operation_fxid = top + 1;
	record.operation.operation_sequence = 3;
	record.operation.canonical_segment_generation = 9;
	record.operation.command_id = 7;
	record.operation.subid = 2;
	return record;
}

UT_TEST(test_operation_v2_producer_preserves_96_bytes_through_native_wal_decode)
{
	const uint64 tops[] = { 801, (UINT64_C(7) << 32) | 801 };

	for (Size i = 0; i < lengthof(tops); i++) {
		xl_undo_tt_slot_set_head_operation_v2 record = producer_operation(tops[i]);
		ClusterUndoDecoded decoded;

		UT_ASSERT_EQ(cluster_undo_emit_tt_slot_set_head_operation_v2(&record), 0x900);
		UT_ASSERT_EQ(payload_length, 96);
		UT_ASSERT_EQ(emitted_info, XLOG_UNDO_TT_SLOT_SET_HEAD);
		UT_ASSERT_EQ(memcmp(payload, &record, sizeof(record)), 0);
		decoded = decode_emitted(false);
		UT_ASSERT(cluster_undo_preflight_tt_v2(&decoded));
		UT_ASSERT_EQ(decoded.original_full_xid, tops[i]);
		UT_ASSERT_EQ(decoded.head_operation.operation_fxid, tops[i] + 1);
		UT_ASSERT_EQ(memcmp(&decoded.head_operation, &record.operation, 48), 0);
	}
}

UT_TEST(test_operation_v2_producer_refuses_before_any_wal_insertion)
{
	const uint64 top = (UINT64_C(7) << 32) | 801;

	for (unsigned fault = 0; fault < 13; fault++) {
		xl_undo_tt_slot_set_head_operation_v2 record = producer_operation(top);
		uint32 prior = inserts_begun;

		switch (fault) {
		case 0: record.head.identity.original_full_xid ^= UINT64_C(1) << 32; break;
		case 1: record.operation.operation_fxid = top; break;
		case 2: record.operation.canonical_top_fxid ^= UINT64_C(1) << 32; break;
		case 3: record.head.identity.flags = 0; break;
		case 4: record.head.identity.reserved = 1; break;
		case 5: record.head.identity.format_version = 3; break;
		case 6: record.head.identity.segment_generation++; break;
		case 7: record.operation.effect_state = CLUSTER_SAVEPOINT_EFFECT_DISCARDED; break;
		case 8: record.operation.reserved_end = 1; break;
		case 9: record.head.first_undo_block = uba_encode(1, 2, 4, 0); break;
		case 10: record.operation.operation_sequence = 0; break;
		case 11: record.operation.version++; break;
		case 12: break; /* NULL */
		}
		expect_panic = true;
		if (setjmp(panic_jump) == 0) {
			cluster_undo_emit_tt_slot_set_head_operation_v2(fault == 12 ? NULL : &record);
			UT_ASSERT(false);
		}
		expect_panic = false;
		UT_ASSERT_EQ(inserts_begun, prior);
	}
}

int
main(void)
{
	UT_PLAN(16);
	UT_RUN(test_segment_lifecycle_producer_payloads);
	UT_RUN(test_tt_producer_identity_and_polarity);
	UT_RUN(test_ctrc_release_producer_certificate);
	UT_RUN(test_v2_release_producer_preserves_104_bytes_and_rejects_before_insert);
	UT_RUN(test_block_write_producer_checkpoint_boundary);
	UT_RUN(test_block_write_multi_producer_checkpoint_boundary);
	UT_RUN(test_unowned_segment_and_hwm_do_not_gain_authority);
	UT_RUN(test_private_block_preparation_refusal_is_atomic);
	UT_RUN(test_native_block_writer_uses_typed_preparation_before_write_and_sync);
	UT_RUN(test_v2_bind_producer_preserves_original_fullxid_and_opcode);
	UT_RUN(test_v2_bind_producer_refuses_bad_identity_before_insertion);
	UT_RUN(test_v2_abort_producer_preserves_original_identity_and_rejects_before_insert);
	UT_RUN(test_v2_prepared_commit_and_head_producers_reach_original_decoder);
	UT_RUN(test_v2_prepared_emitters_reject_bad_identity_scn_or_head_before_insert);
	UT_RUN(test_operation_v2_producer_preserves_96_bytes_through_native_wal_decode);
	UT_RUN(test_operation_v2_producer_refuses_before_any_wal_insertion);
	UT_DONE();
	return ut_failed_count != 0;
}
