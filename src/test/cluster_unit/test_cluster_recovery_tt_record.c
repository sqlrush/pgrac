/* Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"

#include "cluster/cluster_recovery_tt_record.h"
#include "cluster/cluster_recovery_ctrc_record.h"
#include "cluster/cluster_uba.h"

#include <stdarg.h>

#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition pg_attribute_unused(),
					 const char *file pg_attribute_unused(), int line pg_attribute_unused())
{
	abort();
}

/* Fixed externally laid-out little-endian payload, not produced by the codec. */
static const uint8 golden[152]
	= { 0x01, 0x00, 0x98, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x18, 0x17, 0x16, 0x15,
		0x14, 0x13, 0x12, 0x11, 0x28, 0x27, 0x26, 0x25, 0x24, 0x23, 0x22, 0x21, 0x04, 0x03,
		0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x02, 0x01, 0x40, 0x00, 0x07, 0x00, 0x00, 0x00,
		0x02, 0x01, 0x40, 0x00, 0x07, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
		0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14,
		0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x01, 0x01,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x01, 0x40, 0x00, 0x05, 0x00, 0x09, 0x00,
		0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x01, 0x40, 0x00, 0x09, 0x00,
		0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x01, 0x00, 0x00,
		0x03, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00 };

/* Independent canonical D0 bytes shared with the codec golden fixture. */
static const uint8 d0_golden[320] = {
	0x02, 0x00, 0x40, 0x01, 0x01, 0x00, 0x00, 0x00, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
	0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, 0x24, 0x23, 0x22, 0x21, 0x00, 0x00, 0x00, 0x00,
	0x38, 0x37, 0x36, 0x35, 0x34, 0x33, 0x32, 0x31, 0x48, 0x47, 0x46, 0x45, 0x44, 0x43, 0x42, 0x41,
	0x58, 0x57, 0x56, 0x55, 0x54, 0x53, 0x52, 0x51, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x68, 0x67, 0x66, 0x65, 0x64, 0x63, 0x62, 0x61,
	0x78, 0x77, 0x76, 0x75, 0x74, 0x73, 0x72, 0x71, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x01, 0x00, 0x03, 0x00, 0x02, 0x00, 0x05, 0x00, 0x09, 0x00, 0x02, 0x00, 0x01, 0x01, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x02, 0x01, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x02, 0x01, 0x40, 0x00, 0x09, 0x00, 0x02, 0x00, 0x88, 0x87, 0x86, 0x85, 0x84, 0x83, 0x82, 0x81,
	0x02, 0x01, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
	0x02, 0x01, 0x40, 0x00, 0x09, 0x00, 0x02, 0x01, 0x88, 0x87, 0x86, 0x85, 0x84, 0x83, 0x82, 0x81,
	0x02, 0x01, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
	0xc1, 0xc5, 0xc9, 0x76, 0x3d, 0xa1, 0x72, 0x5b, 0x44, 0xc5, 0xdc, 0x07, 0xfb, 0x67, 0x4b, 0x77,
	0x99, 0xf7, 0x3b, 0x30, 0x47, 0x84, 0xf4, 0x8e, 0x14, 0xae, 0x26, 0x7e, 0x4a, 0x02, 0xb0, 0x8b,
	0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
	0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
	0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30,
	0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, 0x40,
	0x0d, 0x3a, 0x13, 0x1b, 0x78, 0x61, 0x18, 0x76, 0xbf, 0xf3, 0xae, 0xdc, 0x17, 0xeb, 0x44, 0x96,
	0x44, 0x86, 0x18, 0x7e, 0x83, 0xe8, 0xe0, 0x22, 0xe3, 0xb6, 0xa0, 0xe0, 0xde, 0x8d, 0xab, 0xcc
};

static bool
all_zero(const void *ptr, size_t length)
{
	const uint8 *bytes = ptr;
	for (size_t i = 0; i < length; i++)
		if (bytes[i] != 0)
			return false;
	return true;
}

static ClusterRecoveryTTAbortV1
sample(void)
{
	ClusterRecoveryTTAbortV1 r = { 0 };
	r.version = 1;
	r.length = 152;
	r.flags = 1;
	r.target_thread = 2;
	r.writer_thread = 1;
	r.target_incarnation = UINT64_C(0x0102030405060708);
	r.writer_incarnation = UINT64_C(0x1112131415161718);
	r.recovery_serial = UINT64_C(0x2122232425262728);
	r.target_epoch = UINT64_C(0x01020304);
	r.target_full_xid = r.top_full_xid = UINT64_C(0x700400102);
	for (size_t i = 0; i < sizeof(r.target_claim_sha256); i++)
		r.target_claim_sha256[i] = (uint8)(i + 1);
	r.exact = (xl_undo_tt_slot_abort_exact){ .segment_id = 257,
											 .segment_generation = 0,
											 .xid = 0x400102,
											 .slot_offset = 5,
											 .wrap = 9,
											 .instance = 2,
											 .format_version = 1 };
	r.before.xid = r.exact.xid;
	r.before.wrap = r.exact.wrap;
	r.before.status = TT_SLOT_ACTIVE;
	r.before.first_undo_block.raw[0] = UINT64_C(0x300000102);
	r.before.first_undo_block.raw[1] = UINT64_C(0x1000005);
	return r;
}

static bool
reject_payload(const uint8 *bytes, size_t length)
{
	ClusterRecoveryTTAbortV1 out;
	memset(&out, 0xa5, sizeof(out));
	return !cluster_recovery_tt_abort_decode_v1(bytes, length, &out) && all_zero(&out, sizeof(out));
}

static bool
reject_record(ClusterRecoveryTTAbortV1 *record)
{
	uint8 bytes[152];
	TTSlot result;
	memset(bytes, 0xa5, sizeof(bytes));
	memset(&result, 0xa5, sizeof(result));
	return !cluster_recovery_tt_abort_encode_v1(record, bytes, sizeof(bytes))
		   && all_zero(bytes, sizeof(bytes))
		   && !cluster_recovery_tt_abort_result_v1(record, &result)
		   && all_zero(&result, sizeof(result))
		   && cluster_recovery_tt_abort_match_v1(record, 0, &record->before)
				  == CLUSTER_RECOVERY_TT_ABORT_REFUSED;
}

UT_TEST(golden_encoding)
{
	ClusterRecoveryTTAbortV1 r = sample();
	uint8 bytes[152];
	UT_ASSERT(cluster_recovery_tt_abort_encode_v1(&r, bytes, sizeof(bytes)));
	UT_ASSERT(memcmp(bytes, golden, sizeof(bytes)) == 0);
}

UT_TEST(golden_decoding_unaligned)
{
	ClusterRecoveryTTAbortV1 r, expected = sample();
	uint8 bytes[153];
	memcpy(bytes + 1, golden, sizeof(golden));
	UT_ASSERT(cluster_recovery_tt_abort_decode_v1(bytes + 1, 152, &r));
	UT_ASSERT(memcmp(&r, &expected, sizeof(r)) == 0);
}

UT_TEST(exact_lengths_and_null)
{
	ClusterRecoveryTTAbortV1 r = sample();
	uint8 bytes[153];
	memcpy(bytes, golden, 152);
	bytes[152] = 0;
	for (size_t n = 0; n < 152; n++)
		UT_ASSERT(reject_payload(bytes, n));
	UT_ASSERT(reject_payload(bytes, 153));
	UT_ASSERT(reject_payload(NULL, 152));
	UT_ASSERT(!cluster_recovery_tt_abort_decode_v1(bytes, 152, NULL));
	UT_ASSERT(!cluster_recovery_tt_abort_encode_v1(&r, NULL, 152));
	UT_ASSERT(!cluster_recovery_tt_abort_encode_v1(NULL, bytes, 152));
	UT_ASSERT(!cluster_recovery_tt_abort_result_v1(&r, NULL));
	for (size_t n = 0; n < 152; n++) {
		memset(bytes, 0xa5, sizeof(bytes));
		UT_ASSERT(!cluster_recovery_tt_abort_encode_v1(&r, bytes, n));
		UT_ASSERT(bytes[n] == 0xa5);
	}
}

UT_TEST(version_flags_reserved)
{
	static const size_t offsets[]
		= { 0, 1, 2, 3, 4, 5, 6, 7, 12, 13, 14, 15, 113, 114, 115, 116, 117, 118, 119 };
	uint8 bytes[152];
	for (size_t i = 0; i < lengthof(offsets); i++) {
		memcpy(bytes, golden, sizeof(bytes));
		bytes[offsets[i]] ^= 0x80;
		UT_ASSERT(reject_payload(bytes, sizeof(bytes)));
	}
	for (uint32 flags = 0; flags < 4; flags++) {
		ClusterRecoveryTTAbortV1 r = sample();
		if (flags == 1)
			continue;
		r.flags = flags;
		UT_ASSERT(reject_record(&r));
	}
}

UT_TEST(source_and_target_fields)
{
	ClusterRecoveryTTAbortV1 r;
	for (int k = 0; k < 10; k++) {
		r = sample();
		switch (k) {
		case 0:
			r.target_thread = 0;
			break;
		case 1:
			r.target_thread = 129;
			break;
		case 2:
			r.writer_thread = 0;
			break;
		case 3:
			r.writer_thread = 129;
			break;
		case 4:
			r.target_incarnation = 0;
			break;
		case 5:
			r.writer_incarnation = 0;
			break;
		case 6:
			r.recovery_serial = 0;
			break;
		case 7:
			r.target_epoch = 0;
			break;
		case 8:
			r.target_epoch = UINT64_C(0x100000000);
			break;
		case 9:
			memset(r.target_claim_sha256, 0, 32);
			break;
		}
		UT_ASSERT(reject_record(&r));
	}
}

UT_TEST(exact_binding_and_top_only)
{
	ClusterRecoveryTTAbortV1 r;
	for (int k = 0; k < 15; k++) {
		r = sample();
		switch (k) {
		case 0:
			r.top_full_xid = 0;
			break;
		case 1:
			r.top_full_xid++;
			break;
		case 2:
			r.top_full_xid ^= UINT64_C(1) << 32;
			break;
		case 3:
			r.target_full_xid = r.top_full_xid = UINT64_C(0x700000002);
			break;
		case 4:
			r.exact.segment_id = 0;
			break;
		case 5:
			r.exact.segment_id = 256;
			break;
		case 6:
			r.exact.segment_id = 513;
			break;
		case 7:
			r.exact.slot_offset = TT_SLOTS_PER_SEGMENT;
			break;
		case 8:
			r.exact.wrap = TT_WRAP_INVALID;
			break;
		case 9:
			r.exact.instance = 1;
			break;
		case 10:
			r.exact.xid++;
			break;
		case 11:
			r.before.xid++;
			break;
		case 12:
			r.before.wrap++;
			break;
		case 13:
			r.exact.flags = 1;
			break;
		case 14:
			r.exact.reserved = 1;
			break;
		}
		UT_ASSERT(reject_record(&r));
	}
}

UT_TEST(shape_and_full_xid_derivation)
{
	ClusterRecoveryTTAbortV1 shape = sample();
	ClusterRecoveryTTAbortDerivationV1 derived;
	uint64 next_xid = UINT64_C(0x700400200);

	shape.target_full_xid = 0;
	shape.top_full_xid = UINT64_C(0xdeadbeef);
	UT_ASSERT(cluster_recovery_tt_abort_shape_valid_v1(&shape));
	UT_ASSERT(!cluster_recovery_tt_abort_valid_v1(&shape));
	UT_ASSERT(cluster_recovery_tt_abort_derive_fields_v1(&shape, next_xid, true, false,
												 InvalidTransactionId, UINT64_C(0x12345678), &derived));
	UT_ASSERT_EQ(derived.full_xid, UINT64_C(0x700400102));
	UT_ASSERT_EQ(derived.record.target_full_xid, derived.full_xid);
	UT_ASSERT_EQ(derived.record.top_full_xid, derived.full_xid);
	UT_ASSERT_EQ(derived.record.target_epoch, UINT64_C(0x12345678));
	UT_ASSERT(derived.oldest_xid_dependency);
	UT_ASSERT(!derived.oldest_xid_checked);
	UT_ASSERT(cluster_recovery_tt_abort_valid_v1(&derived.record));

	shape = sample();
	UT_ASSERT(cluster_recovery_tt_abort_derive_fields_v1(&shape, next_xid, true, true,
												(TransactionId) 0x400050, UINT64_C(0x12345678),
												&derived));
	UT_ASSERT(derived.oldest_xid_checked);
	UT_ASSERT(!derived.oldest_xid_dependency);
	UT_ASSERT(!cluster_recovery_tt_abort_derive_fields_v1(&shape, next_xid, true, true,
												(TransactionId) 0x400150, UINT64_C(0x12345678),
												&derived));
	UT_ASSERT(!cluster_recovery_tt_abort_derive_fields_v1(&shape, 0, true, false,
												 InvalidTransactionId, UINT64_C(0x12345678), &derived));
	UT_ASSERT(!cluster_recovery_tt_abort_derive_fields_v1(&shape, next_xid, false, false,
												 InvalidTransactionId, UINT64_C(0x12345678), &derived));
}

UT_TEST(expected_active_slot)
{
	ClusterRecoveryTTAbortV1 r;
	for (unsigned status = 0; status <= 255; status++) {
		if (status == TT_SLOT_ACTIVE)
			continue;
		r = sample();
		r.before.status = (uint8)status;
		UT_ASSERT(reject_record(&r));
	}
	for (unsigned flags = 1; flags <= 255; flags++) {
		r = sample();
		r.before.flags = (uint8)flags;
		UT_ASSERT(reject_record(&r));
	}
	r = sample();
	r.before.commit_scn = 1;
	UT_ASSERT(reject_record(&r));
}

UT_TEST(head_record_shape)
{
	ClusterRecoveryTTAbortV1 r;
	for (int k = 0; k < 6; k++) {
		r = sample();
		switch (k) {
		case 0:
			r.before.first_undo_block.raw[1] |= UINT64_C(1) << 32;
			break;
		case 1:
			r.before.first_undo_block.raw[0] = 258;
			break;
		case 2:
			r.before.first_undo_block.raw[0] = ((uint64)UNDO_BLOCKS_PER_SEGMENT << 32) | 258;
			break;
		case 3:
			r.before.first_undo_block.raw[0] = UINT64_C(0x300000201);
			break;
		case 4:
			r.before.first_undo_block.raw[1]++;
			break;
		case 5:
			r.before.first_undo_block.raw[0] = 0;
			break;
		}
		UT_ASSERT(reject_record(&r));
	}
}

UT_TEST(valid_boundaries)
{
	ClusterRecoveryTTAbortV1 r = sample(), out;
	uint8 bytes[152];
	r.writer_thread = r.target_thread;
	r.exact.segment_generation = UINT32_MAX - 1;
	r.target_epoch = UINT32_MAX;
	r.exact.wrap = r.before.wrap = TT_WRAP_MAX;
	r.before.first_undo_block = (UBA){ { 0, 0 } };
	UT_ASSERT(cluster_recovery_tt_abort_encode_v1(&r, bytes, sizeof(bytes)));
	UT_ASSERT(cluster_recovery_tt_abort_decode_v1(bytes, sizeof(bytes), &out));
	UT_ASSERT(memcmp(&r, &out, sizeof(r)) == 0);
	UT_ASSERT(r.exact.wrap != (r.target_full_xid >> 32));
	r.target_thread = r.writer_thread = 128;
	r.exact.instance = 128;
	r.exact.segment_id = 128 * 256;
	UT_ASSERT(cluster_recovery_tt_abort_encode_v1(&r, bytes, sizeof(bytes)));
}

UT_TEST(generation_sentinel_refuses)
{
	ClusterRecoveryTTAbortV1 r = sample();
	uint8 bytes[152];

	r.exact.segment_generation = UINT32_MAX;
	UT_ASSERT(reject_record(&r));
	UT_ASSERT_EQ(cluster_recovery_tt_abort_match_v1(&r, UINT32_MAX, &r.before),
				 CLUSTER_RECOVERY_TT_ABORT_REFUSED);
	memcpy(bytes, golden, sizeof(bytes));
	memset(bytes + 100, 0xff, 4);
	UT_ASSERT(reject_payload(bytes, sizeof(bytes)));
}

UT_TEST(result_preserves_every_other_byte)
{
	ClusterRecoveryTTAbortV1 r = sample();
	TTSlot expected = r.before, out;
	expected.status = TT_SLOT_ABORTED;
	UT_ASSERT(cluster_recovery_tt_abort_result_v1(&r, &out));
	UT_ASSERT(memcmp(&out, &expected, sizeof(out)) == 0);
	UT_ASSERT_EQ(cluster_recovery_tt_abort_match_v1(&r, 0, &r.before),
				 CLUSTER_RECOVERY_TT_ABORT_APPLY);
	UT_ASSERT_EQ(cluster_recovery_tt_abort_match_v1(&r, 0, &out),
				 CLUSTER_RECOVERY_TT_ABORT_IDEMPOTENT);
	UT_ASSERT_EQ(r.before.status, TT_SLOT_ACTIVE);
}

UT_TEST(conflict_never_overwrites_or_skips)
{
	ClusterRecoveryTTAbortV1 r = sample();
	TTSlot slot, old;
	for (int k = 0; k < 8; k++) {
		slot = r.before;
		switch (k) {
		case 0:
			slot.xid++;
			break;
		case 1:
			slot.wrap++;
			break;
		case 2:
			slot.status = TT_SLOT_COMMITTED;
			slot.commit_scn = 99;
			break;
		case 3:
			slot.status = TT_SLOT_RECYCLABLE;
			break;
		case 4:
			slot.first_undo_block.raw[0]++;
			break;
		case 5:
			slot.flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
			break;
		case 6:
			slot.status = TT_SLOT_ABORTED;
			slot.first_undo_block.raw[0]++;
			break;
		case 7:
			slot.commit_scn = 99;
			break;
		}
		old = slot;
		UT_ASSERT_EQ(cluster_recovery_tt_abort_match_v1(&r, 0, &slot),
					 CLUSTER_RECOVERY_TT_ABORT_REFUSED);
		UT_ASSERT(memcmp(&old, &slot, sizeof(slot)) == 0);
	}
	UT_ASSERT_EQ(cluster_recovery_tt_abort_match_v1(&r, 1, &r.before),
				 CLUSTER_RECOVERY_TT_ABORT_REFUSED);
	r.exact.segment_generation = 1;
	UT_ASSERT_EQ(cluster_recovery_tt_abort_match_v1(&r, 0, &r.before),
				 CLUSTER_RECOVERY_TT_ABORT_REFUSED);
	UT_ASSERT_EQ(cluster_recovery_tt_abort_match_v1(&r, 1, NULL),
				 CLUSTER_RECOVERY_TT_ABORT_REFUSED);
}

/* The original descriptor's output boundary; no WAL/IO or authority stubs. */
void
appendStringInfo(StringInfo str, const char *fmt, ...)
{
	va_list ap;
	int n;
	va_start(ap, fmt);
	n = vsnprintf(str->data + str->len, str->maxlen - str->len, fmt, ap);
	va_end(ap);
	if (n < 0 || n >= str->maxlen - str->len)
		abort();
	str->len += n;
}

UT_TEST(real_wal_descriptor)
{
	XLogReaderState reader = { 0 };
	DecodedXLogRecord record = { 0 };
	char text[1024];
	StringInfoData buf = { .data = text, .maxlen = sizeof(text) };
	record.header.xl_info = 0xc0;
	record.main_data = (char *)golden;
	record.main_data_len = sizeof(golden);
	reader.record = &record;
	UT_ASSERT(cluster_undo_identify(0xc0) != NULL);
	if (cluster_undo_identify(0xc0) == NULL)
		return;
	UT_ASSERT(strcmp(cluster_undo_identify(0xc0), "UNDO_RECOVERY_TT_ABORT") == 0);
	cluster_undo_desc(&buf, &reader);
	UT_ASSERT(strstr(text, "target thread 2") != NULL);
	UT_ASSERT(strstr(text, "writer thread 1") != NULL);
	UT_ASSERT(strstr(text, "generation 0 slot 5 wrap 9") != NULL);
	for (size_t len = 0; len < sizeof(golden); len++) {
		buf.len = 0;
		record.main_data_len = len;
		cluster_undo_desc(&buf, &reader);
		UT_ASSERT(strstr(text, "malformed") != NULL);
	}
	UT_ASSERT(strcmp(cluster_undo_identify(XLOG_UNDO_TT_SLOT_ABORT), "UNDO_TT_SLOT_ABORT") == 0);
}

static void
describe_d0(const uint8 *payload, size_t length, char *text, size_t capacity)
{
	XLogReaderState reader = { 0 };
	DecodedXLogRecord record = { 0 };
	StringInfoData buf = { .data = text, .maxlen = capacity };

	record.header.xl_info = CLUSTER_RECOVERY_CTRC_WAL_INFO | XLR_INFO_MASK;
	record.main_data = (char *)payload;
	record.main_data_len = length;
	reader.record = &record;
	cluster_undo_desc(&buf, &reader);
}

UT_TEST(d0_opcode_identity)
{
	for (unsigned bits = 0; bits <= XLR_INFO_MASK; bits++) {
		const char *name = cluster_undo_identify(CLUSTER_RECOVERY_CTRC_WAL_INFO | bits);
		UT_ASSERT(name != NULL);
		if (name != NULL)
			UT_ASSERT(strcmp(name, "UNDO_RECOVERY_CTRC_RELEASE") == 0);
	}
	UT_ASSERT(strcmp(cluster_undo_identify(XLOG_UNDO_TT_SLOT_CTRC_RELEASE),
					 "UNDO_TT_SLOT_CTRC_RELEASE") == 0);
	UT_ASSERT(strcmp(cluster_undo_identify(XLOG_UNDO_RECOVERY_TT_ABORT),
					 "UNDO_RECOVERY_TT_ABORT") == 0);
	UT_ASSERT(cluster_undo_identify(0xe0) == NULL);
}

UT_TEST(d0_golden_description)
{
	uint8 payload[sizeof(d0_golden)];
	char text[1024];

	memcpy(payload, d0_golden, sizeof(payload));
	describe_d0(payload, sizeof(payload), text, sizeof(text));
	UT_ASSERT(strcmp(text,
		"v2 flags 0x00000001 root 72623859790382856 root generation 1230066625199609624"
		" formation 555885348 serial 3544952156018063160"
		" database incarnation 4702394921427289928 writer boot 5859837686836516696"
		" history 7017280452245743464 highest FPI end 8174723217654970232 cohort 5"
		" origin 1 writer thread 3 target owner 2 segment 257 generation 0"
		" slot 5 wrap 9 xid 4194562 terminal 2") == 0);
	UT_ASSERT(memcmp(payload, d0_golden, sizeof(payload)) == 0);
}

UT_TEST(d0_descriptor_exact_bounds)
{
	char text[1024];
	uint8 oversized[sizeof(d0_golden) + 1];

	memcpy(oversized, d0_golden, sizeof(d0_golden));
	oversized[sizeof(d0_golden)] = 0xa5;
	for (size_t length = 0; length < sizeof(d0_golden); length++) {
		describe_d0(d0_golden, length, text, sizeof(text));
		UT_ASSERT(strstr(text, "malformed recovery CTRC release payload") != NULL);
	}
	describe_d0(oversized, sizeof(oversized), text, sizeof(text));
	UT_ASSERT(strstr(text, "malformed recovery CTRC release payload") != NULL);
	describe_d0(NULL, sizeof(d0_golden), text, sizeof(text));
	UT_ASSERT(strstr(text, "malformed recovery CTRC release payload") != NULL);
}

UT_TEST(d0_descriptor_version_and_declared_length)
{
	uint8 payload[sizeof(d0_golden)];
	char text[1024];

	for (size_t offset = 0; offset < 4; offset++) {
		memcpy(payload, d0_golden, sizeof(payload));
		payload[offset] ^= 0x80;
		describe_d0(payload, sizeof(payload), text, sizeof(text));
		UT_ASSERT(strstr(text, "malformed recovery CTRC release payload") != NULL);
	}
}

UT_TEST(d0_descriptor_unaligned_full_width)
{
	uint8 unaligned[sizeof(d0_golden) + 1];
	uint8 *payload = unaligned + 1;
	char text[1024];

	memcpy(payload, d0_golden, sizeof(d0_golden));
	memset(payload + 4, 0xff, 12); /* flags and root */
	memset(payload + 96, 0xff, 11); /* five u16 fields and terminal */
	memset(payload + 108, 0xff, 12); /* segment, generation and xid */
	describe_d0(payload, sizeof(d0_golden), text, sizeof(text));
	UT_ASSERT(strstr(text, "flags 0xffffffff root 18446744073709551615") != NULL);
	UT_ASSERT(strstr(text, "origin 65535 writer thread 65535 target owner 65535") != NULL);
	UT_ASSERT(strstr(text, "segment 4294967295 generation 4294967295") != NULL);
	UT_ASSERT(strstr(text, "slot 65535 wrap 65535 xid 4294967295 terminal 255") != NULL);
}

UT_TEST(d0_diagnostic_layout_is_not_qualification)
{
	/* Reserved bytes, images and hashes are validated by the replay codec,
	 * whose ROOT-selected system identity is unavailable to this descriptor. */
	static const size_t offsets[] = { 56, 64, 107, 120, 128, 160, 192, 224, 256, 288 };
	uint8 payload[sizeof(d0_golden)];
	char text[1024], original[1024];

	describe_d0(d0_golden, sizeof(d0_golden), original, sizeof(original));
	UT_ASSERT(strstr(original, "v2 flags") != NULL);
	for (size_t i = 0; i < lengthof(offsets); i++) {
		memcpy(payload, d0_golden, sizeof(payload));
		payload[offsets[i]] ^= 0x80;
		describe_d0(payload, sizeof(payload), text, sizeof(text));
		UT_ASSERT(strcmp(text, original) == 0);
	}
}

int
main(void)
{
	UT_PLAN(20);
	UT_RUN(golden_encoding);
	UT_RUN(golden_decoding_unaligned);
	UT_RUN(exact_lengths_and_null);
	UT_RUN(version_flags_reserved);
	UT_RUN(source_and_target_fields);
	UT_RUN(exact_binding_and_top_only);
	UT_RUN(shape_and_full_xid_derivation);
	UT_RUN(expected_active_slot);
	UT_RUN(head_record_shape);
	UT_RUN(valid_boundaries);
	UT_RUN(generation_sentinel_refuses);
	UT_RUN(result_preserves_every_other_byte);
	UT_RUN(conflict_never_overwrites_or_skips);
	UT_RUN(real_wal_descriptor);
	UT_RUN(d0_opcode_identity);
	UT_RUN(d0_golden_description);
	UT_RUN(d0_descriptor_exact_bounds);
	UT_RUN(d0_descriptor_version_and_declared_length);
	UT_RUN(d0_descriptor_unaligned_full_width);
	UT_RUN(d0_diagnostic_layout_is_not_qualification);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
