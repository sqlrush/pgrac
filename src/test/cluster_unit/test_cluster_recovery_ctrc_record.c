/* Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "access/xlogrecord.h"
#include "port/pg_crc32c.h"
#include "cluster/cluster_recovery_ctrc_record.h"
#include "cluster/cluster_uba.h"
#include "common/cryptohash.h"

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

#define SYSTEM_ID UINT64_C(0x1122334455667788)

/* Independently laid out with Python struct.pack/hashlib, not this codec. */
static const uint8 golden[320] = {
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
	const uint8 *p = ptr;
	for (size_t i = 0; i < length; i++)
		if (p[i] != 0)
			return false;
	return true;
}

static ClusterRecoveryCTRCReleaseV2
sample(void)
{
	ClusterRecoveryCTRCReleaseV2 r = { 0 };
	r.version = 2;
	r.length = 320;
	r.flags = 1;
	r.root_id = UINT64_C(0x0102030405060708);
	r.root_generation = UINT64_C(0x1112131415161718);
	r.formation_epoch = UINT64_C(0x21222324);
	r.recovery_serial = UINT64_C(0x3132333435363738);
	r.database_incarnation = UINT64_C(0x4142434445464748);
	r.writer_boot = UINT64_C(0x5152535455565758);
	r.covered_history = UINT64_C(0x6162636465666768);
	r.highest_fpi_end = UINT64_C(0x7172737475767778);
	r.cohort_bitmap = 5;
	r.origin_node_id = 1;
	r.writer_thread = 3;
	r.target_owner = 2;
	r.slot_offset = 5;
	r.wrap = 9;
	r.terminal_outcome = TT_SLOT_COMMITTED;
	r.segment_id = 257;
	r.segment_generation = 0;
	r.xid = 0x400102;
	r.before
		= (TTSlot){ .xid = r.xid,
					.wrap = r.wrap,
					.status = TT_SLOT_COMMITTED,
					.commit_scn = UINT64_C(0x8182838485868788),
					.first_undo_block = { .raw = { UINT64_C(0x300000102), UINT64_C(0x1000005) } } };
	r.after = r.before;
	r.after.flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	for (unsigned i = 0; i < 32; i++) {
		r.graph_sha256[i] = i + 1;
		r.input_sha256[i] = i + 33;
	}
	return r;
}

static bool
same_fields(const ClusterRecoveryCTRCReleaseV2 *a, const ClusterRecoveryCTRCReleaseV2 *b)
{
	return a->version == b->version && a->length == b->length && a->flags == b->flags
		   && a->root_id == b->root_id && a->root_generation == b->root_generation
		   && a->formation_epoch == b->formation_epoch && a->recovery_serial == b->recovery_serial
		   && a->database_incarnation == b->database_incarnation && a->writer_boot == b->writer_boot
		   && a->covered_history == b->covered_history && a->highest_fpi_end == b->highest_fpi_end
		   && a->cohort_bitmap == b->cohort_bitmap && a->origin_node_id == b->origin_node_id
		   && a->writer_thread == b->writer_thread && a->target_owner == b->target_owner
		   && a->slot_offset == b->slot_offset && a->wrap == b->wrap
		   && a->terminal_outcome == b->terminal_outcome && a->segment_id == b->segment_id
		   && a->segment_generation == b->segment_generation && a->xid == b->xid
		   && memcmp(&a->before, &b->before, sizeof(TTSlot)) == 0
		   && memcmp(&a->after, &b->after, sizeof(TTSlot)) == 0
		   && memcmp(a->graph_sha256, b->graph_sha256, 32) == 0
		   && memcmp(a->input_sha256, b->input_sha256, 32) == 0;
}

/* Independent hash transcripts, used to make well-hashed structural
 * counterexamples. Never call a production serializer/hash helper here. */
static bool
sha256(const uint8 *bytes, size_t length, uint8 out[32])
{
	pg_cryptohash_ctx *ctx = pg_cryptohash_create(PG_SHA256);
	bool ok;
	if (ctx == NULL)
		return false;
	ok = pg_cryptohash_init(ctx) == 0 && pg_cryptohash_update(ctx, bytes, length) == 0
		 && pg_cryptohash_final(ctx, out, 32) == 0;
	pg_cryptohash_free(ctx);
	return ok;
}

static bool
reseal(uint8 p[320], bool identity)
{
	uint8 transcript[304];
	if (identity) {
		uint8 id[20 + 34 + 48 + 40 + 32];
		uint8 *next = id;
		memcpy(next, "PGRAC-D0-IDENTITY-V2", 20);
		next += 20;
		for (unsigned i = 0; i < 8; i++)
			*next++ = (uint8)(SYSTEM_ID >> (8 * i));
		memcpy(next, p + 40, 8);
		next += 8;
		memcpy(next, p + 96, 2);
		next += 2;
		memcpy(next, p + 108, 8);
		next += 8;
		memcpy(next, p + 102, 4);
		next += 4;
		memcpy(next, p + 116, 4);
		next += 4;
		memcpy(next, p + 8, 48);
		next += 48;
		memcpy(next, p + 88, 40);
		next += 40;
		memcpy(next, p + 256, 32);
		next += 32;
		if ((size_t)(next - id) != sizeof(id) || !sha256(id, sizeof(id), p + 288))
			return false;
	}
	memcpy(transcript, "PGRAC-D0-V2\0\0\0\0\0", 16);
	memcpy(transcript + 16, p, 192);
	memcpy(transcript + 208, p + 224, 96);
	return sha256(transcript, sizeof(transcript), p + 192);
}

static bool
reject_bytes(const uint8 *bytes, size_t length, uint64 system)
{
	ClusterRecoveryCTRCReleaseV2 out;
	memset(&out, 0xa5, sizeof(out));
	return !cluster_recovery_ctrc_decode_v2(bytes, length, system, &out)
		   && all_zero(&out, sizeof(out));
}

static bool
reject_record(const ClusterRecoveryCTRCReleaseV2 *record)
{
	uint8 bytes[320];
	memset(bytes, 0xa5, sizeof(bytes));
	return !cluster_recovery_ctrc_encode_v2(record, SYSTEM_ID, bytes, sizeof(bytes))
		   && all_zero(bytes, sizeof(bytes));
}

UT_TEST(golden_encoding)
{
	ClusterRecoveryCTRCReleaseV2 r = sample();
	uint8 bytes[320];
	UT_ASSERT(cluster_recovery_ctrc_encode_v2(&r, SYSTEM_ID, bytes, sizeof(bytes)));
	UT_ASSERT(memcmp(bytes, golden, sizeof(golden)) == 0);
}

UT_TEST(golden_decoding)
{
	ClusterRecoveryCTRCReleaseV2 out, r = sample();
	UT_ASSERT(cluster_recovery_ctrc_decode_v2(golden, sizeof(golden), SYSTEM_ID, &out));
	UT_ASSERT(same_fields(&out, &r));
}

UT_TEST(unaligned_buffers)
{
	uint8 bytes[322];
	ClusterRecoveryCTRCReleaseV2 r = sample(), out;
	memset(bytes, 0xa5, sizeof(bytes));
	UT_ASSERT(cluster_recovery_ctrc_encode_v2(&r, SYSTEM_ID, bytes + 1, 320));
	UT_ASSERT(bytes[0] == 0xa5 && bytes[321] == 0xa5);
	UT_ASSERT(memcmp(bytes + 1, golden, 320) == 0);
	UT_ASSERT(cluster_recovery_ctrc_decode_v2(bytes + 1, 320, SYSTEM_ID, &out));
	UT_ASSERT(same_fields(&r, &out));
}

UT_TEST(exact_length_and_clear_bounds)
{
	uint8 bytes[322];
	ClusterRecoveryCTRCReleaseV2 r = sample();
	for (size_t len = 0; len < 320; len++) {
		memset(bytes, 0xa5, sizeof(bytes));
		UT_ASSERT(!cluster_recovery_ctrc_encode_v2(&r, SYSTEM_ID, bytes + 1, len));
		UT_ASSERT(all_zero(bytes + 1, len));
		UT_ASSERT(bytes[0] == 0xa5 && bytes[len + 1] == 0xa5);
		UT_ASSERT(reject_bytes(golden, len, SYSTEM_ID));
	}
	memset(bytes, 0xa5, sizeof(bytes));
	UT_ASSERT(!cluster_recovery_ctrc_encode_v2(&r, SYSTEM_ID, bytes + 1, 321));
	UT_ASSERT(all_zero(bytes + 1, 320) && bytes[0] == 0xa5 && bytes[321] == 0xa5);
	UT_ASSERT(reject_bytes(bytes, 321, SYSTEM_ID));
}

UT_TEST(nulls_and_missing_system)
{
	ClusterRecoveryCTRCReleaseV2 r = sample();
	uint8 bytes[320];
	UT_ASSERT(reject_record(NULL));
	UT_ASSERT(reject_bytes(NULL, 320, SYSTEM_ID));
	UT_ASSERT(!cluster_recovery_ctrc_encode_v2(&r, SYSTEM_ID, NULL, 320));
	UT_ASSERT(!cluster_recovery_ctrc_decode_v2(golden, 320, SYSTEM_ID, NULL));
	memset(bytes, 0xa5, sizeof(bytes));
	UT_ASSERT(!cluster_recovery_ctrc_encode_v2(&r, 0, bytes, 320));
	UT_ASSERT(all_zero(bytes, sizeof(bytes)));
	UT_ASSERT(reject_bytes(golden, 320, 0));
}

UT_TEST(legacy_unknown_version_length_flags)
{
	ClusterRecoveryCTRCReleaseV2 r;
	const uint16 versions[] = { 0, 1, 3, UINT16_MAX };
	const uint16 lengths[] = { 0, 96, 152, 192, 319, 321, UINT16_MAX };
	const uint32 flags[] = { 0, 2, 3, UINT32_MAX };
	for (unsigned i = 0; i < lengthof(versions); i++) {
		r = sample();
		r.version = versions[i];
		UT_ASSERT(reject_record(&r));
	}
	for (unsigned i = 0; i < lengthof(lengths); i++) {
		r = sample();
		r.length = lengths[i];
		UT_ASSERT(reject_record(&r));
	}
	for (unsigned i = 0; i < lengthof(flags); i++) {
		r = sample();
		r.flags = flags[i];
		UT_ASSERT(reject_record(&r));
	}
}

UT_TEST(reserved_bytes_rejected_even_with_valid_hashes)
{
	uint8 bytes[320];
	for (unsigned i = 0; i < 320; i++) {
		if (!((i >= 56 && i < 72) || i == 107 || (i >= 120 && i < 128)))
			continue;
		memcpy(bytes, golden, sizeof(bytes));
		bytes[i] = 1;
		UT_ASSERT(reseal(bytes, true));
		UT_ASSERT(reject_bytes(bytes, sizeof(bytes), SYSTEM_ID));
	}
}

UT_TEST(missing_root_round_or_source)
{
	const unsigned offsets[] = { 8, 16, 24, 32, 40, 48, 88 };
	uint8 bytes[320];
	for (unsigned i = 0; i < lengthof(offsets); i++) {
		memcpy(bytes, golden, sizeof(bytes));
		memset(bytes + offsets[i], 0, 8);
		UT_ASSERT(reseal(bytes, true));
		UT_ASSERT(reject_bytes(bytes, sizeof(bytes), SYSTEM_ID));
	}
}

UT_TEST(owner_thread_origin_and_segment_ranges)
{
	ClusterRecoveryCTRCReleaseV2 r;
	const uint16 bad_owners[] = { 0, 3, 129, UINT16_MAX };
	const uint32 bad_segments[] = { 0, 1, 256, 513, 32769, UINT32_MAX };
	for (unsigned i = 0; i < lengthof(bad_owners); i++) {
		r = sample();
		r.target_owner = bad_owners[i];
		UT_ASSERT(reject_record(&r));
	}
	for (unsigned i = 0; i < lengthof(bad_segments); i++) {
		r = sample();
		r.segment_id = bad_segments[i];
		UT_ASSERT(reject_record(&r));
	}
	r = sample();
	r.origin_node_id = 0;
	UT_ASSERT(reject_record(&r));
	r = sample();
	r.origin_node_id = 128;
	UT_ASSERT(reject_record(&r));
	r = sample();
	r.writer_thread = 0;
	UT_ASSERT(reject_record(&r));
	r = sample();
	r.writer_thread = 129;
	UT_ASSERT(reject_record(&r));
	r = sample();
	r.segment_generation = UINT32_MAX;
	UT_ASSERT(reject_record(&r));
}

UT_TEST(slot_wrap_and_normal_xid)
{
	ClusterRecoveryCTRCReleaseV2 r = sample();
	r.slot_offset = 48;
	UT_ASSERT(reject_record(&r));
	r = sample();
	r.wrap = TT_WRAP_INVALID;
	UT_ASSERT(reject_record(&r));
	for (TransactionId xid = 0; xid < FirstNormalTransactionId; xid++) {
		r = sample();
		r.xid = r.before.xid = r.after.xid = xid;
		UT_ASSERT(reject_record(&r));
	}
}

UT_TEST(terminal_outcome_and_scn)
{
	ClusterRecoveryCTRCReleaseV2 r;
	const uint8 bad_status[] = { TT_SLOT_UNUSED, TT_SLOT_ACTIVE, TT_SLOT_RECYCLABLE, 4, 254, 255 };
	for (unsigned i = 0; i < lengthof(bad_status); i++) {
		r = sample();
		r.terminal_outcome = r.before.status = r.after.status = bad_status[i];
		UT_ASSERT(reject_record(&r));
	}
	r = sample();
	r.before.commit_scn = r.after.commit_scn = InvalidScn;
	UT_ASSERT(reject_record(&r));
	r = sample();
	r.terminal_outcome = r.before.status = r.after.status = TT_SLOT_ABORTED;
	UT_ASSERT(reject_record(&r));
	r = sample();
	r.terminal_outcome = TT_SLOT_ABORTED;
	UT_ASSERT(reject_record(&r));
}

UT_TEST(before_is_exact_unreleased_terminal)
{
	ClusterRecoveryCTRCReleaseV2 r = sample();
	r.before.xid++;
	UT_ASSERT(reject_record(&r));
	r = sample();
	r.before.wrap++;
	UT_ASSERT(reject_record(&r));
	r = sample();
	r.before.status = TT_SLOT_ABORTED;
	UT_ASSERT(reject_record(&r));
	for (unsigned flag = 1; flag < 256; flag++) {
		r = sample();
		r.before.flags = flag;
		UT_ASSERT(reject_record(&r));
	}
}

UT_TEST(after_changes_only_release_bit)
{
	uint8 bytes[320];
	for (unsigned i = 160; i < 192; i++) {
		memcpy(bytes, golden, sizeof(bytes));
		bytes[i] ^= 1;
		UT_ASSERT(reseal(bytes, true));
		UT_ASSERT(reject_bytes(bytes, sizeof(bytes), SYSTEM_ID));
	}
	for (unsigned flag = 0; flag < 256; flag++) {
		ClusterRecoveryCTRCReleaseV2 r = sample();
		if (flag == TT_SLOT_FLAG_CTRC_RELEASE_PROVEN)
			continue;
		r.after.flags = flag;
		UT_ASSERT(reject_record(&r));
	}
}

UT_TEST(undo_record_head_shape)
{
	const UBA bad_heads[]
		= { { .raw = { UINT64_C(0x300000000), UINT64_C(0x1000005) } },
			{ .raw = { UINT64_C(0x300000001), UINT64_C(0x1000005) } },
			{ .raw = { 258, UINT64_C(0x1000005) } },
			{ .raw = { ((uint64)UNDO_BLOCKS_PER_SEGMENT << 32) | 258, UINT64_C(0x1000005) } },
			{ .raw = { UINT64_C(0x300000102), UINT64_C(0x1000004) } },
			{ .raw = { UINT64_C(0x300000102), UINT64_C(0x101000005) } } };
	for (unsigned i = 0; i < lengthof(bad_heads); i++) {
		ClusterRecoveryCTRCReleaseV2 r = sample();
		r.before.first_undo_block = r.after.first_undo_block = bad_heads[i];
		UT_ASSERT(reject_record(&r));
	}
}

UT_TEST(native_boundary_identities_and_empty_head)
{
	ClusterRecoveryCTRCReleaseV2 r = sample(), out;
	uint8 bytes[320];
	r.origin_node_id = 0;
	r.target_owner = 1;
	r.writer_thread = 1;
	r.segment_id = 1;
	r.slot_offset = r.wrap = r.before.wrap = r.after.wrap = 0;
	r.xid = r.before.xid = r.after.xid = FirstNormalTransactionId;
	r.before.first_undo_block = r.after.first_undo_block = (UBA)InvalidUba_init;
	r.before.commit_scn = r.after.commit_scn = 1;
	r.covered_history = r.highest_fpi_end = 0;
	UT_ASSERT(cluster_recovery_ctrc_encode_v2(&r, SYSTEM_ID, bytes, sizeof(bytes)));
	UT_ASSERT(cluster_recovery_ctrc_decode_v2(bytes, sizeof(bytes), SYSTEM_ID, &out));
	UT_ASSERT(same_fields(&r, &out));
	r.origin_node_id = 127;
	r.target_owner = r.writer_thread = 128;
	r.segment_id = 32768;
	r.segment_generation = UINT32_MAX - 1;
	r.slot_offset = 47;
	r.wrap = r.before.wrap = r.after.wrap = TT_WRAP_MAX;
	r.xid = r.before.xid = r.after.xid = UINT32_MAX;
	r.terminal_outcome = r.before.status = r.after.status = TT_SLOT_ABORTED;
	r.before.commit_scn = r.after.commit_scn = InvalidScn;
	r.cohort_bitmap = UINT64_MAX;
	UT_ASSERT(cluster_recovery_ctrc_encode_v2(&r, SYSTEM_ID, bytes, sizeof(bytes)));
	UT_ASSERT(cluster_recovery_ctrc_decode_v2(bytes, sizeof(bytes), SYSTEM_ID, &out));
	UT_ASSERT(same_fields(&r, &out));
}

UT_TEST(every_payload_bit_is_integrity_bound)
{
	uint8 bytes[320];
	for (unsigned i = 0; i < 320; i++) {
		for (unsigned bit = 0; bit < 8; bit++) {
			memcpy(bytes, golden, sizeof(bytes));
			bytes[i] ^= 1U << bit;
			UT_ASSERT(reject_bytes(bytes, sizeof(bytes), SYSTEM_ID));
		}
	}
}

UT_TEST(external_system_identity_is_bound)
{
	UT_ASSERT(reject_bytes(golden, sizeof(golden), SYSTEM_ID ^ 1));
	UT_ASSERT(reject_bytes(golden, sizeof(golden), UINT64_MAX));
}

UT_TEST(identity_cannot_be_replaced_by_payload_checksum)
{
	uint8 bytes[320];
	const unsigned offsets[] = { 8, 16, 24, 32, 40, 48, 88, 98, 112, 256, 288 };
	for (unsigned i = 0; i < lengthof(offsets); i++) {
		memcpy(bytes, golden, sizeof(bytes));
		bytes[offsets[i]] ^= 1;
		UT_ASSERT(reseal(bytes, false));
		UT_ASSERT(reject_bytes(bytes, sizeof(bytes), SYSTEM_ID));
	}
}

UT_TEST(opaque_graph_and_input_are_full_payload_fields)
{
	ClusterRecoveryCTRCReleaseV2 r = sample();
	uint8 bytes[320];
	/* Hash binding only: no graph or input qualification is inferred. */
	r.graph_sha256[31] ^= 0x80;
	UT_ASSERT(cluster_recovery_ctrc_encode_v2(&r, SYSTEM_ID, bytes, sizeof(bytes)));
	UT_ASSERT(memcmp(bytes + 224, r.graph_sha256, 32) == 0);
	UT_ASSERT(memcmp(bytes + 192, golden + 192, 32) != 0);
	UT_ASSERT(memcmp(bytes + 288, golden + 288, 32) == 0);
	r = sample();
	r.input_sha256[31] ^= 0x80;
	UT_ASSERT(cluster_recovery_ctrc_encode_v2(&r, SYSTEM_ID, bytes, sizeof(bytes)));
	UT_ASSERT(memcmp(bytes + 256, r.input_sha256, 32) == 0);
	UT_ASSERT(memcmp(bytes + 192, golden + 192, 32) != 0);
	UT_ASSERT(memcmp(bytes + 288, golden + 288, 32) != 0);
}

UT_TEST(missing_graph_or_input_is_not_a_payload)
{
	ClusterRecoveryCTRCReleaseV2 r = sample();
	uint8 bytes[320];
	memset(r.graph_sha256, 0, 32);
	UT_ASSERT(reject_record(&r));
	r = sample();
	memset(r.input_sha256, 0, 32);
	UT_ASSERT(reject_record(&r));
	for (unsigned offset = 224; offset <= 256; offset += 32) {
		memcpy(bytes, golden, sizeof(bytes));
		memset(bytes + offset, 0, 32);
		UT_ASSERT(reseal(bytes, true));
		UT_ASSERT(reject_bytes(bytes, sizeof(bytes), SYSTEM_ID));
	}
}

UT_TEST(independent_transcript_matches_fixed_sample)
{
	uint8 bytes[320];
	memcpy(bytes, golden, sizeof(bytes));
	memset(bytes + 192, 0, 32);
	memset(bytes + 288, 0, 32);
	UT_ASSERT(reseal(bytes, true));
	UT_ASSERT(memcmp(bytes, golden, sizeof(bytes)) == 0);
}

UT_TEST(input_output_aliasing)
{
	union {
		ClusterRecoveryCTRCReleaseV2 record;
		uint8 bytes[512];
	} work;
	ClusterRecoveryCTRCReleaseV2 r = sample();
	work.record = r;
	UT_ASSERT(cluster_recovery_ctrc_encode_v2(&work.record, SYSTEM_ID, work.bytes, 320));
	UT_ASSERT(memcmp(work.bytes, golden, 320) == 0);
	UT_ASSERT(cluster_recovery_ctrc_decode_v2(work.bytes, 320, SYSTEM_ID, &work.record));
	UT_ASSERT(same_fields(&r, &work.record));
}


/* Independent Python struct/hashlib + bitwise CRC32C raw-record vectors.
 * This models a final D0 record, without registering an opcode or writer. */
#define RAW_LENGTH 357
#define RAW_START UINT64_C(0x1122334455667700)
#define RAW_END (RAW_START + RAW_LENGTH)
#ifdef WORDS_BIGENDIAN
static const uint8 raw_prefix[37] = {
	0x00, 0x00, 0x01, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03,
	0x04, 0x05, 0x00, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0xd0, 0x16,
	0x00, 0x00, 0xbc, 0xdb, 0xee, 0xdc, 0xfe, 0x00, 0x00, 0x01, 0x40,
};
static const uint8 raw_expected[32] = {
	0x79, 0xb4, 0xb3, 0xa4, 0x20, 0xdc, 0x28, 0xce, 0x0e, 0x0a, 0x4b, 0x9d, 0x4e, 0xc1, 0x27, 0x00,
	0x59, 0xaf, 0x17, 0x65, 0x3e, 0x6f, 0x61, 0x99, 0x61, 0xa9, 0xbf, 0x10, 0x84, 0x7a, 0xd0, 0x59,
};
static const uint8 raw_cross_expected[32] = {
	0xe1, 0xe5, 0xc3, 0xd8, 0x51, 0x63, 0xab, 0x2a, 0x61, 0x53, 0x25, 0x38, 0x83, 0x8d, 0x23, 0x35,
	0x83, 0x38, 0x38, 0x11, 0x23, 0xd5, 0x24, 0xeb, 0x37, 0xc3, 0xd6, 0x1e, 0x78, 0x3d, 0xeb, 0x07,
};
#else
static const uint8 raw_prefix[37] = {
	0x65, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x04, 0x03, 0x02,
	0x01, 0x00, 0x00, 0x19, 0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0xd0, 0x16,
	0x00, 0x00, 0x18, 0x45, 0x68, 0xfa, 0xfe, 0x40, 0x01, 0x00, 0x00,
};
static const uint8 raw_expected[32] = {
	0xf6, 0xf2, 0x57, 0xc1, 0x22, 0xae, 0xf9, 0x0f, 0x61, 0x53, 0x04, 0x9e, 0x4d, 0xf7, 0x60, 0x20,
	0xef, 0x39, 0x2e, 0x80, 0x8b, 0xf5, 0xf7, 0xd6, 0x12, 0x6a, 0x51, 0x95, 0x42, 0xb3, 0x8e, 0xf8,
};
static const uint8 raw_cross_expected[32] = {
	0x5c, 0xd7, 0x46, 0x11, 0x35, 0x19, 0xe4, 0x08, 0x23, 0xfa, 0xd1, 0x12, 0x08, 0xed, 0x5e, 0x2e,
	0x64, 0x84, 0x87, 0xa1, 0xa7, 0xa8, 0xc0, 0x34, 0xad, 0x6a, 0x67, 0x87, 0x07, 0x17, 0xad, 0x6d,
};
#endif
static const uint8 raw_source[32] = {
	0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e, 0x4f, 0x50,
	0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5a, 0x5b, 0x5c, 0x5d, 0x5e, 0x5f, 0x60,
};

static void
raw_sample(uint8 raw[RAW_LENGTH])
{
	memcpy(raw, raw_prefix, sizeof(raw_prefix));
	memcpy(raw + sizeof(raw_prefix), golden, sizeof(golden));
}

/* The existing native CRC order, independently invoked by the fixture to
 * build valid record mutations. Never call the product hash for resealing. */
static void
raw_reseal(uint8 raw[RAW_LENGTH])
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, raw + SizeOfXLogRecord, RAW_LENGTH - SizeOfXLogRecord);
	COMP_CRC32C(crc, raw, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(crc);
	memcpy(raw + offsetof(XLogRecord, xl_crc), &crc, sizeof(crc));
}

static bool
raw_refused(const uint8 *source, XLogRecPtr start, XLogRecPtr end, const uint8 *raw, size_t length)
{
	uint8 out[34];

	memset(out, 0xa5, sizeof(out));
	return !cluster_recovery_ctrc_record_hash_v2(source, start, end, raw, length, out + 1)
		   && all_zero(out + 1, 32) && out[0] == 0xa5 && out[33] == 0xa5;
}

UT_TEST(raw_golden_and_immutable)
{
	uint8 raw[RAW_LENGTH], saved[RAW_LENGTH], out[34];

	raw_sample(raw);
	memcpy(saved, raw, sizeof(raw));
	memset(out, 0xa5, sizeof(out));
	UT_ASSERT(cluster_recovery_ctrc_record_hash_v2(raw_source, RAW_START, RAW_END, raw, sizeof(raw),
												   out + 1));
	UT_ASSERT(memcmp(out + 1, raw_expected, 32) == 0);
	UT_ASSERT(out[0] == 0xa5 && out[33] == 0xa5);
	UT_ASSERT(memcmp(raw, saved, sizeof(raw)) == 0);
}

UT_TEST(raw_cross_page_end)
{
	uint8 raw[RAW_LENGTH], out[32];

	raw_sample(raw);
	UT_ASSERT(cluster_recovery_ctrc_record_hash_v2(raw_source, RAW_START, RAW_END + 24, raw,
												   sizeof(raw), out));
	UT_ASSERT(memcmp(out, raw_cross_expected, 32) == 0);
	UT_ASSERT(memcmp(out, raw_expected, 32) != 0);
}

UT_TEST(raw_complete_source_binding)
{
	uint8 raw[RAW_LENGTH], source[32], out[32];

	raw_sample(raw);
	for (unsigned bit = 0; bit < 256; bit++) {
		memcpy(source, raw_source, sizeof(source));
		source[bit / 8] ^= 1U << (bit % 8);
		UT_ASSERT(cluster_recovery_ctrc_record_hash_v2(source, RAW_START, RAW_END, raw, sizeof(raw),
													   out));
		UT_ASSERT(memcmp(out, raw_expected, 32) != 0);
	}
}

UT_TEST(raw_full_width_position_binding)
{
	uint8 raw[RAW_LENGTH], out[32];

	raw_sample(raw);
	for (unsigned bit = 0; bit < 64; bit++) {
		uint64 delta = UINT64_C(1) << bit;
		UT_ASSERT(cluster_recovery_ctrc_record_hash_v2(raw_source, RAW_START + delta,
													   RAW_END + delta, raw, sizeof(raw), out));
		UT_ASSERT(memcmp(out, raw_expected, 32) != 0);
	}
	UT_ASSERT(cluster_recovery_ctrc_record_hash_v2(raw_source, RAW_START - 1, RAW_END, raw,
												   sizeof(raw), out));
	UT_ASSERT(memcmp(out, raw_expected, 32) != 0);
}

UT_TEST(raw_valid_mutations_bind_header_crc_and_payload)
{
	uint8 raw[RAW_LENGTH], out[32];

	for (size_t offset = sizeof(uint32); offset < RAW_LENGTH; offset++) {
		if (offset >= offsetof(XLogRecord, xl_crc) && offset < SizeOfXLogRecord)
			continue;
		raw_sample(raw);
		raw[offset] ^= 1;
		raw_reseal(raw);
		UT_ASSERT(cluster_recovery_ctrc_record_hash_v2(raw_source, RAW_START, RAW_END, raw,
													   sizeof(raw), out));
		UT_ASSERT(memcmp(out, raw_expected, 32) != 0);
	}
}

UT_TEST(raw_every_bit_crc_refusal)
{
	uint8 raw[RAW_LENGTH];

	for (unsigned bit = 0; bit < RAW_LENGTH * 8; bit++) {
		raw_sample(raw);
		raw[bit / 8] ^= 1U << (bit % 8);
		UT_ASSERT(raw_refused(raw_source, RAW_START, RAW_END, raw, sizeof(raw)));
	}
}

UT_TEST(raw_exact_native_length)
{
	uint8 raw[RAW_LENGTH + 1];
	uint32 total;

	raw_sample(raw);
	raw[RAW_LENGTH] = 0;
	for (size_t length = 0; length < RAW_LENGTH; length++)
		UT_ASSERT(raw_refused(raw_source, RAW_START, RAW_END, raw, length));
	UT_ASSERT(raw_refused(raw_source, RAW_START, RAW_END + 1, raw, sizeof(raw)));
	for (unsigned i = 0; i < 2; i++) {
		total = i == 0 ? RAW_LENGTH - 1 : RAW_LENGTH + 1;
		memcpy(raw, &total, sizeof(total));
		raw_reseal(raw);
		UT_ASSERT(raw_refused(raw_source, RAW_START, RAW_END, raw, RAW_LENGTH));
	}
}

UT_TEST(raw_oversize_refused_before_read)
{
	uint8 byte = 0;

	UT_ASSERT(raw_refused(raw_source, RAW_START, UINT64_MAX, &byte, (size_t)XLogRecordMaxSize + 1));
	UT_ASSERT(raw_refused(raw_source, RAW_START, UINT64_MAX, &byte, SIZE_MAX));
}

UT_TEST(raw_null_and_missing_source)
{
	uint8 raw[RAW_LENGTH], zero[32] = { 0 };

	raw_sample(raw);
	UT_ASSERT(raw_refused(NULL, RAW_START, RAW_END, raw, sizeof(raw)));
	UT_ASSERT(raw_refused(zero, RAW_START, RAW_END, raw, sizeof(raw)));
	UT_ASSERT(raw_refused(raw_source, RAW_START, RAW_END, NULL, sizeof(raw)));
	UT_ASSERT(!cluster_recovery_ctrc_record_hash_v2(raw_source, RAW_START, RAW_END, raw,
													sizeof(raw), NULL));
}

UT_TEST(raw_invalid_position_domain)
{
	uint8 raw[RAW_LENGTH];

	raw_sample(raw);
	UT_ASSERT(raw_refused(raw_source, 0, RAW_END, raw, sizeof(raw)));
	UT_ASSERT(raw_refused(raw_source, RAW_START, 0, raw, sizeof(raw)));
	UT_ASSERT(raw_refused(raw_source, RAW_START, RAW_START, raw, sizeof(raw)));
	UT_ASSERT(raw_refused(raw_source, RAW_START, RAW_START - 1, raw, sizeof(raw)));
	UT_ASSERT(raw_refused(raw_source, RAW_START, RAW_END - 1, raw, sizeof(raw)));
}

UT_TEST(raw_unaligned_buffers)
{
	uint8 raw[RAW_LENGTH + 2], source[34], out[34];

	memset(raw, 0xa5, sizeof(raw));
	memset(source, 0xa5, sizeof(source));
	memset(out, 0xa5, sizeof(out));
	raw_sample(raw + 1);
	memcpy(source + 1, raw_source, 32);
	UT_ASSERT(cluster_recovery_ctrc_record_hash_v2(source + 1, RAW_START, RAW_END, raw + 1,
												   RAW_LENGTH, out + 1));
	UT_ASSERT(memcmp(out + 1, raw_expected, 32) == 0);
	UT_ASSERT(raw[0] == 0xa5 && raw[RAW_LENGTH + 1] == 0xa5);
	UT_ASSERT(source[0] == 0xa5 && source[33] == 0xa5);
	UT_ASSERT(out[0] == 0xa5 && out[33] == 0xa5);
}

UT_TEST(raw_input_output_aliasing)
{
	uint8 raw[RAW_LENGTH], source[32];

	raw_sample(raw);
	memcpy(source, raw_source, sizeof(source));
	UT_ASSERT(
		cluster_recovery_ctrc_record_hash_v2(source, RAW_START, RAW_END, raw, sizeof(raw), source));
	UT_ASSERT(memcmp(source, raw_expected, 32) == 0);
	UT_ASSERT(cluster_recovery_ctrc_record_hash_v2(raw_source, RAW_START, RAW_END, raw, sizeof(raw),
												   raw + 3));
	UT_ASSERT(memcmp(raw + 3, raw_expected, 32) == 0);
	UT_ASSERT(memcmp(raw, raw_prefix, 3) == 0);
	UT_ASSERT(memcmp(raw + 35, raw_prefix + 35, 2) == 0);
	UT_ASSERT(memcmp(raw + 37, golden, 320) == 0);
}

int
main(void)
{
	UT_PLAN(34);
	UT_RUN(golden_encoding);
	UT_RUN(golden_decoding);
	UT_RUN(unaligned_buffers);
	UT_RUN(exact_length_and_clear_bounds);
	UT_RUN(nulls_and_missing_system);
	UT_RUN(legacy_unknown_version_length_flags);
	UT_RUN(reserved_bytes_rejected_even_with_valid_hashes);
	UT_RUN(missing_root_round_or_source);
	UT_RUN(owner_thread_origin_and_segment_ranges);
	UT_RUN(slot_wrap_and_normal_xid);
	UT_RUN(terminal_outcome_and_scn);
	UT_RUN(before_is_exact_unreleased_terminal);
	UT_RUN(after_changes_only_release_bit);
	UT_RUN(undo_record_head_shape);
	UT_RUN(native_boundary_identities_and_empty_head);
	UT_RUN(every_payload_bit_is_integrity_bound);
	UT_RUN(external_system_identity_is_bound);
	UT_RUN(identity_cannot_be_replaced_by_payload_checksum);
	UT_RUN(opaque_graph_and_input_are_full_payload_fields);
	UT_RUN(missing_graph_or_input_is_not_a_payload);
	UT_RUN(independent_transcript_matches_fixed_sample);
	UT_RUN(input_output_aliasing);
	UT_RUN(raw_golden_and_immutable);
	UT_RUN(raw_cross_page_end);
	UT_RUN(raw_complete_source_binding);
	UT_RUN(raw_full_width_position_binding);
	UT_RUN(raw_valid_mutations_bind_header_crc_and_payload);
	UT_RUN(raw_every_bit_crc_refusal);
	UT_RUN(raw_exact_native_length);
	UT_RUN(raw_oversize_refused_before_read);
	UT_RUN(raw_null_and_missing_source);
	UT_RUN(raw_invalid_position_domain);
	UT_RUN(raw_unaligned_buffers);
	UT_RUN(raw_input_output_aliasing);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
