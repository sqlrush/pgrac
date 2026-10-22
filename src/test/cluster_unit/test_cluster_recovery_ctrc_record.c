/* Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "access/xlogrecord.h"
#include "access/xlog_internal.h"
#include "access/rmgr.h"
#include "port/pg_crc32c.h"
#include "cluster/cluster_recovery_ctrc_record.h"
#include "cluster/cluster_side_undo.h"
#include "cluster/cluster_uba.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "cluster/cluster_undo_segment_init.h"
#include "common/cryptohash.h"
#include "utils/memutils.h"

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

/* Real native initializer and checksum, with only the selected slot installed.
 * These images do not pretend to be an owner/source qualification receipt. */
static bool
target_sample(const ClusterRecoveryCTRCReleaseV2 *r, bool after, PGAlignedBlock *page,
			  uint8 payload[CLUSTER_RECOVERY_CTRC_BYTES])
{
	UndoSegmentHeaderData *hdr = (UndoSegmentHeaderData *)page->data;

	if (!cluster_recovery_ctrc_encode_v2(r, SYSTEM_ID, payload, CLUSTER_RECOVERY_CTRC_BYTES))
		return false;
	cluster_undo_segment_make_header_bytes(r->segment_id, r->target_owner, page->data);
	hdr->wrap_count = r->segment_generation;
	hdr->tt_slots[r->slot_offset] = after ? r->after : r->before;
	return cluster_undo_segment_header_seal(page->data);
}

static ClusterRecoveryCTRCTargetMatchV2
target_match(const uint8 payload[CLUSTER_RECOVERY_CTRC_BYTES], const PGAlignedBlock *page)
{
	return cluster_recovery_ctrc_target_match_v2(payload, CLUSTER_RECOVERY_CTRC_BYTES, SYSTEM_ID,
												 page->data, BLCKSZ);
}

UT_TEST(target_terminal_images)
{
	ClusterRecoveryCTRCReleaseV2 r = sample();
	PGAlignedBlock page, saved;
	uint8 payload[320], saved_payload[320];

	for (int aborted = 0; aborted < 2; aborted++) {
		r.terminal_outcome = aborted ? TT_SLOT_ABORTED : TT_SLOT_COMMITTED;
		r.before.status = r.terminal_outcome;
		r.before.commit_scn = aborted ? InvalidScn : UINT64_C(0x8182838485868788);
		r.after = r.before;
		r.after.flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
		for (int after = 0; after < 2; after++) {
			UT_ASSERT(target_sample(&r, after, &page, payload));
			memcpy(saved.data, page.data, BLCKSZ);
			memcpy(saved_payload, payload, sizeof(payload));
			UT_ASSERT(target_match(payload, &page)
					  == (after ? CLUSTER_RECOVERY_CTRC_TARGET_EXPECTED_AFTER
								: CLUSTER_RECOVERY_CTRC_TARGET_EXPECTED_BEFORE));
			UT_ASSERT(memcmp(saved.data, page.data, BLCKSZ) == 0);
			UT_ASSERT(memcmp(saved_payload, payload, sizeof(payload)) == 0);
		}
	}
}

UT_TEST(target_native_identity)
{
	ClusterRecoveryCTRCReleaseV2 r = sample();
	PGAlignedBlock original, page;
	uint8 payload[320];
	const uint8 states[]
		= { SEGMENT_ALLOCATED, SEGMENT_ACTIVE, SEGMENT_COMMITTED, SEGMENT_RECYCLABLE };

	UT_ASSERT(target_sample(&r, false, &original, payload));
	for (size_t i = 0; i < lengthof(states); i++) {
		memcpy(page.data, original.data, BLCKSZ);
		((UndoSegmentHeaderData *)page.data)->segment_state = states[i];
		UT_ASSERT(cluster_undo_segment_header_seal(page.data));
		UT_ASSERT(target_match(payload, &page) == CLUSTER_RECOVERY_CTRC_TARGET_EXPECTED_BEFORE);
	}
	for (int change = 0; change < 8; change++) {
		UndoSegmentHeaderData *hdr = (UndoSegmentHeaderData *)page.data;
		PageHeader ph = (PageHeader)page.data;

		memcpy(page.data, original.data, BLCKSZ);
		switch (change) {
		case 0:
			ph->pd_flags &= ~PD_UNDO_SEG_HEADER;
			break;
		case 1:
			PageSetPageSizeAndVersion(page.data, BLCKSZ / 2, PG_PAGE_LAYOUT_VERSION);
			break;
		case 2:
			PageSetPageSizeAndVersion(page.data, BLCKSZ, PG_PAGE_LAYOUT_VERSION + 1);
			break;
		case 3:
			hdr->segment_id++;
			break;
		case 4:
			hdr->segment_size_bytes--;
			break;
		case 5:
			hdr->owner_instance++;
			break;
		case 6:
			hdr->tt_slots_count--;
			break;
		case 7:
			hdr->segment_state = UINT8_MAX;
			break;
		}
		UT_ASSERT(cluster_undo_segment_header_seal(page.data));
		UT_ASSERT(cluster_undo_segment_header_integrity(page.data) == CLUSTER_UNDO_HEADER_VALID);
		UT_ASSERT(target_match(payload, &page) == CLUSTER_RECOVERY_CTRC_TARGET_REFUSED);
	}
}

UT_TEST(target_generation_and_wrap_are_exact)
{
	ClusterRecoveryCTRCReleaseV2 r = sample();
	PGAlignedBlock original, page;
	uint8 payload[320];
	const uint32 generations[] = { 0, 6, 8, UINT32_MAX };
	const uint16 wraps[] = { 0, 8, 10, TT_WRAP_MAX, TT_WRAP_INVALID };

	r.segment_generation = 7;
	UT_ASSERT(target_sample(&r, false, &original, payload));
	for (size_t i = 0; i < lengthof(generations); i++) {
		memcpy(page.data, original.data, BLCKSZ);
		((UndoSegmentHeaderData *)page.data)->wrap_count = generations[i];
		UT_ASSERT(cluster_undo_segment_header_seal(page.data));
		UT_ASSERT(target_match(payload, &page) == CLUSTER_RECOVERY_CTRC_TARGET_REFUSED);
	}
	for (size_t i = 0; i < lengthof(wraps); i++) {
		memcpy(page.data, original.data, BLCKSZ);
		((UndoSegmentHeaderData *)page.data)->tt_slots[r.slot_offset].wrap = wraps[i];
		UT_ASSERT(cluster_undo_segment_header_seal(page.data));
		UT_ASSERT(target_match(payload, &page) == CLUSTER_RECOVERY_CTRC_TARGET_REFUSED);
	}
}

UT_TEST(target_complete_slot_images)
{
	ClusterRecoveryCTRCReleaseV2 r = sample();
	PGAlignedBlock original, page, saved;
	uint8 payload[320];

	for (int after = 0; after < 2; after++) {
		UT_ASSERT(target_sample(&r, after, &original, payload));
		for (size_t bit = 0; bit < sizeof(TTSlot) * 8; bit++) {
			UndoSegmentHeaderData *hdr = (UndoSegmentHeaderData *)page.data;
			ClusterRecoveryCTRCTargetMatchV2 expected = CLUSTER_RECOVERY_CTRC_TARGET_REFUSED;

			memcpy(page.data, original.data, BLCKSZ);
			((uint8 *)&hdr->tt_slots[r.slot_offset])[bit / 8] ^= (uint8)(1U << (bit % 8));
			/* The one release bit converts exactly between before and after. */
			if (bit == offsetof(TTSlot, flags) * 8)
				expected = after ? CLUSTER_RECOVERY_CTRC_TARGET_EXPECTED_BEFORE
								 : CLUSTER_RECOVERY_CTRC_TARGET_EXPECTED_AFTER;
			UT_ASSERT(cluster_undo_segment_header_seal(page.data));
			UT_ASSERT(cluster_undo_segment_header_integrity(page.data)
					  == CLUSTER_UNDO_HEADER_VALID);
			memcpy(saved.data, page.data, BLCKSZ);
			UT_ASSERT(target_match(payload, &page) == expected);
			UT_ASSERT(memcmp(saved.data, page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(target_only_selected_slot)
{
	ClusterRecoveryCTRCReleaseV2 r = sample();
	PGAlignedBlock page;
	UndoSegmentHeaderData *hdr = (UndoSegmentHeaderData *)page.data;
	uint8 payload[320];

	UT_ASSERT(target_sample(&r, false, &page, payload));
	hdr->tt_slots[r.slot_offset + 1] = hdr->tt_slots[r.slot_offset];
	memset(&hdr->tt_slots[r.slot_offset], 0, sizeof(TTSlot));
	UT_ASSERT(cluster_undo_segment_header_seal(page.data));
	UT_ASSERT(target_match(payload, &page) == CLUSTER_RECOVERY_CTRC_TARGET_REFUSED);
	hdr->tt_slots[r.slot_offset] = r.before;
	memset(&hdr->tt_slots[r.slot_offset + 1], 0xa5, sizeof(TTSlot));
	UT_ASSERT(cluster_undo_segment_header_seal(page.data));
	UT_ASSERT(target_match(payload, &page) == CLUSTER_RECOVERY_CTRC_TARGET_EXPECTED_BEFORE);
}

UT_TEST(target_native_integrity_required)
{
	ClusterRecoveryCTRCReleaseV2 r = sample();
	PGAlignedBlock original, page, saved;
	uint8 payload[320];
	const size_t offsets[] = { 0, 32, 112 + 32 * 5, BLCKSZ - 1 };

	UT_ASSERT(target_sample(&r, false, &original, payload));
	memcpy(page.data, original.data, BLCKSZ);
	((PageHeader)page.data)->pd_checksum = 0;
	UT_ASSERT(cluster_undo_segment_header_integrity(page.data) == CLUSTER_UNDO_HEADER_UNVERIFIED);
	UT_ASSERT(target_match(payload, &page) == CLUSTER_RECOVERY_CTRC_TARGET_REFUSED);
	for (size_t i = 0; i < lengthof(offsets); i++) {
		memcpy(page.data, original.data, BLCKSZ);
		page.data[offsets[i]] ^= 1;
		memcpy(saved.data, page.data, BLCKSZ);
		UT_ASSERT(target_match(payload, &page) == CLUSTER_RECOVERY_CTRC_TARGET_REFUSED);
		UT_ASSERT(memcmp(saved.data, page.data, BLCKSZ) == 0);
	}
	memset(page.data, 0, BLCKSZ);
	UT_ASSERT(target_match(payload, &page) == CLUSTER_RECOVERY_CTRC_TARGET_REFUSED);
}

UT_TEST(target_payload_and_external_identity)
{
	ClusterRecoveryCTRCReleaseV2 r = sample();
	PGAlignedBlock page;
	uint8 original[320], payload[320];

	UT_ASSERT(target_sample(&r, false, &page, original));
	for (size_t bit = 0; bit < sizeof(payload) * 8; bit++) {
		memcpy(payload, original, sizeof(payload));
		payload[bit / 8] ^= (uint8)(1U << (bit % 8));
		UT_ASSERT(target_match(payload, &page) == CLUSTER_RECOVERY_CTRC_TARGET_REFUSED);
	}
	UT_ASSERT(
		cluster_recovery_ctrc_target_match_v2(original, sizeof(original), 0, page.data, BLCKSZ)
		== CLUSTER_RECOVERY_CTRC_TARGET_REFUSED);
	UT_ASSERT(cluster_recovery_ctrc_target_match_v2(original, sizeof(original), SYSTEM_ID + 1,
													page.data, BLCKSZ)
			  == CLUSTER_RECOVERY_CTRC_TARGET_REFUSED);
}

UT_TEST(target_exact_lengths_and_nulls)
{
	ClusterRecoveryCTRCReleaseV2 r = sample();
	PGAlignedBlock page;
	uint8 payload[320], one = 0;
	const size_t page_lengths[] = { 0, 1, BLCKSZ - 1, BLCKSZ + 1, SIZE_MAX };
	const size_t payload_lengths[] = { 0, 1, 192, 319, 321, SIZE_MAX };

	UT_ASSERT(target_sample(&r, false, &page, payload));
	for (size_t i = 0; i < lengthof(page_lengths); i++)
		UT_ASSERT(cluster_recovery_ctrc_target_match_v2(payload, sizeof(payload), SYSTEM_ID, &one,
														page_lengths[i])
				  == CLUSTER_RECOVERY_CTRC_TARGET_REFUSED);
	for (size_t i = 0; i < lengthof(payload_lengths); i++)
		UT_ASSERT(cluster_recovery_ctrc_target_match_v2(&one, payload_lengths[i], SYSTEM_ID,
														page.data, BLCKSZ)
				  == CLUSTER_RECOVERY_CTRC_TARGET_REFUSED);
	UT_ASSERT(
		cluster_recovery_ctrc_target_match_v2(NULL, sizeof(payload), SYSTEM_ID, page.data, BLCKSZ)
		== CLUSTER_RECOVERY_CTRC_TARGET_REFUSED);
	UT_ASSERT(
		cluster_recovery_ctrc_target_match_v2(payload, sizeof(payload), SYSTEM_ID, NULL, BLCKSZ)
		== CLUSTER_RECOVERY_CTRC_TARGET_REFUSED);
}

UT_TEST(target_unaligned_and_immutable)
{
	ClusterRecoveryCTRCReleaseV2 r = sample();
	PGAlignedBlock page;
	uint8 payload[322], unaligned[BLCKSZ + 2], saved[BLCKSZ + 2], saved_payload[322];

	memset(payload, 0xa5, sizeof(payload));
	memset(unaligned, 0xa5, sizeof(unaligned));
	UT_ASSERT(target_sample(&r, true, &page, payload + 1));
	memcpy(unaligned + 1, page.data, BLCKSZ);
	memcpy(saved, unaligned, sizeof(saved));
	memcpy(saved_payload, payload, sizeof(payload));
	UT_ASSERT(
		cluster_recovery_ctrc_target_match_v2(payload + 1, 320, SYSTEM_ID, unaligned + 1, BLCKSZ)
		== CLUSTER_RECOVERY_CTRC_TARGET_EXPECTED_AFTER);
	UT_ASSERT(memcmp(saved, unaligned, sizeof(saved)) == 0);
	UT_ASSERT(memcmp(saved_payload, payload, sizeof(payload)) == 0);
}

UT_TEST(target_legal_native_boundaries)
{
	ClusterRecoveryCTRCReleaseV2 r = sample();
	PGAlignedBlock page;
	uint8 payload[320];

	for (int high = 0; high < 2; high++) {
		r.origin_node_id = high ? 127 : 0;
		r.target_owner = high ? 128 : 1;
		r.writer_thread = high ? 128 : 1;
		r.segment_id = high ? 32768 : 1;
		r.segment_generation = high ? UINT32_MAX - 1 : 0;
		r.slot_offset = high ? TT_SLOTS_PER_SEGMENT - 1 : 0;
		r.wrap = high ? TT_WRAP_MAX : TT_WRAP_INITIAL;
		r.xid = high ? MaxTransactionId : FirstNormalTransactionId;
		r.before.xid = r.xid;
		r.before.wrap = r.wrap;
		r.before.first_undo_block = (UBA)InvalidUba_init;
		r.after = r.before;
		r.after.flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
		for (int after = 0; after < 2; after++) {
			UT_ASSERT(target_sample(&r, after, &page, payload));
			UT_ASSERT(target_match(payload, &page)
					  == (after ? CLUSTER_RECOVERY_CTRC_TARGET_EXPECTED_AFTER
								: CLUSTER_RECOVERY_CTRC_TARGET_EXPECTED_BEFORE));
		}
	}
}


static bool
image_make(const uint8 payload[320], const PGAlignedBlock *page, PGAlignedBlock *out)
{
	return cluster_recovery_ctrc_target_image_v2(payload, 320, SYSTEM_ID, page->data, BLCKSZ,
												 out->data, BLCKSZ);
}

static bool
image_refused(const uint8 payload[320], const PGAlignedBlock *page)
{
	PGAlignedBlock out;

	memset(out.data, 0xa5, BLCKSZ);
	return !image_make(payload, page, &out) && all_zero(out.data, BLCKSZ);
}

UT_TEST(image_terminal_pages)
{
	ClusterRecoveryCTRCReleaseV2 r = sample();
	PGAlignedBlock page, saved, out, expected;
	uint8 payload[320], saved_payload[320];

	for (int aborted = 0; aborted < 2; aborted++) {
		r.terminal_outcome = aborted ? TT_SLOT_ABORTED : TT_SLOT_COMMITTED;
		r.before.status = r.terminal_outcome;
		r.before.commit_scn = aborted ? InvalidScn : UINT64_C(0x8182838485868788);
		r.after = r.before;
		r.after.flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
		UT_ASSERT(target_sample(&r, false, &page, payload));
		/* Unrelated live slots and unused tail bytes must survive intact. */
		memset(&((UndoSegmentHeaderData *)page.data)->tt_slots[r.slot_offset + 1], 0x5a,
			   sizeof(TTSlot));
		page.data[BLCKSZ - 1] = 0x3c;
		UT_ASSERT(cluster_undo_segment_header_seal(page.data));
		memcpy(saved.data, page.data, BLCKSZ);
		memcpy(saved_payload, payload, sizeof(payload));
		memcpy(expected.data, page.data, BLCKSZ);
		((UndoSegmentHeaderData *)expected.data)->tt_slots[r.slot_offset] = r.after;
		UT_ASSERT(cluster_undo_segment_header_seal(expected.data));
		memset(out.data, 0xa5, BLCKSZ);
		UT_ASSERT(image_make(payload, &page, &out));
		UT_ASSERT(memcmp(out.data, expected.data, BLCKSZ) == 0);
		UT_ASSERT(cluster_undo_segment_header_integrity(out.data) == CLUSTER_UNDO_HEADER_VALID);
		UT_ASSERT(target_match(payload, &out) == CLUSTER_RECOVERY_CTRC_TARGET_EXPECTED_AFTER);
		UT_ASSERT(memcmp(page.data, saved.data, BLCKSZ) == 0);
		UT_ASSERT(memcmp(payload, saved_payload, sizeof(payload)) == 0);
	}
}

UT_TEST(image_requires_exact_before)
{
	ClusterRecoveryCTRCReleaseV2 r = sample();
	PGAlignedBlock page, original, saved;
	uint8 payload[320];

	UT_ASSERT(target_sample(&r, true, &page, payload));
	UT_ASSERT(image_refused(payload, &page));
	UT_ASSERT(target_sample(&r, false, &original, payload));
	for (size_t bit = 0; bit < sizeof(TTSlot) * 8; bit++) {
		TTSlot *slot = &((UndoSegmentHeaderData *)page.data)->tt_slots[r.slot_offset];

		memcpy(page.data, original.data, BLCKSZ);
		((uint8 *)slot)[bit / 8] ^= (uint8)(1U << (bit % 8));
		UT_ASSERT(cluster_undo_segment_header_seal(page.data));
		memcpy(saved.data, page.data, BLCKSZ);
		UT_ASSERT(image_refused(payload, &page));
		UT_ASSERT(memcmp(page.data, saved.data, BLCKSZ) == 0);
	}
}

UT_TEST(image_native_refusal)
{
	ClusterRecoveryCTRCReleaseV2 r = sample();
	PGAlignedBlock original, page, saved;
	uint8 payload[320];

	r.segment_generation = 7;
	UT_ASSERT(target_sample(&r, false, &original, payload));
	for (int change = 0; change < 11; change++) {
		UndoSegmentHeaderData *hdr = (UndoSegmentHeaderData *)page.data;

		memcpy(page.data, original.data, BLCKSZ);
		switch (change) {
		case 0:
			hdr->segment_id++;
			break;
		case 1:
			hdr->owner_instance++;
			break;
		case 2:
			hdr->tt_slots_count--;
			break;
		case 3:
			hdr->wrap_count--;
			break;
		case 4:
			hdr->wrap_count++;
			break;
		case 5:
			hdr->wrap_count = UINT32_MAX;
			break;
		case 6:
			((PageHeader)page.data)->pd_flags &= ~PD_UNDO_SEG_HEADER;
			break;
		case 7:
			hdr->segment_state = UINT8_MAX;
			break;
		case 8:
			break; /* legacy checksum below */
		case 9:
			break; /* damaged complete page below */
		case 10:
			memset(page.data, 0, BLCKSZ);
			break;
		}
		if (change < 8)
			UT_ASSERT(cluster_undo_segment_header_seal(page.data));
		if (change == 8)
			((PageHeader)page.data)->pd_checksum = 0;
		if (change == 9)
			page.data[BLCKSZ - 1] ^= 1;
		memcpy(saved.data, page.data, BLCKSZ);
		UT_ASSERT(image_refused(payload, &page));
		UT_ASSERT(memcmp(page.data, saved.data, BLCKSZ) == 0);
	}
}

UT_TEST(image_payload_binding)
{
	ClusterRecoveryCTRCReleaseV2 r = sample();
	PGAlignedBlock page, out;
	uint8 payload[320], original[320];

	UT_ASSERT(target_sample(&r, false, &page, original));
	for (size_t bit = 0; bit < sizeof(payload) * 8; bit++) {
		memcpy(payload, original, sizeof(payload));
		payload[bit / 8] ^= (uint8)(1U << (bit % 8));
		UT_ASSERT(image_refused(payload, &page));
	}
	for (int missing = 0; missing < 2; missing++) {
		memset(out.data, 0xa5, BLCKSZ);
		UT_ASSERT(!cluster_recovery_ctrc_target_image_v2(original, 320, missing ? 0 : SYSTEM_ID + 1,
														 page.data, BLCKSZ, out.data, BLCKSZ));
		UT_ASSERT(all_zero(out.data, BLCKSZ));
	}
}

UT_TEST(image_exact_bounds_and_nulls)
{
	ClusterRecoveryCTRCReleaseV2 r = sample();
	PGAlignedBlock page;
	uint8 payload[320], guarded[BLCKSZ + 2], one = 0;
	const size_t output_lengths[] = { 0, 1, BLCKSZ - 1, BLCKSZ + 1, SIZE_MAX };
	const size_t page_lengths[] = { 0, 1, BLCKSZ - 1, BLCKSZ + 1, SIZE_MAX };
	const size_t payload_lengths[] = { 0, 1, 192, 319, 321, SIZE_MAX };

	UT_ASSERT(target_sample(&r, false, &page, payload));
	for (size_t i = 0; i < lengthof(output_lengths); i++) {
		size_t cleared = Min(output_lengths[i], BLCKSZ);

		memset(guarded, 0xa5, sizeof(guarded));
		UT_ASSERT(!cluster_recovery_ctrc_target_image_v2(payload, 320, SYSTEM_ID, page.data, BLCKSZ,
														 guarded + 1, output_lengths[i]));
		UT_ASSERT(all_zero(guarded + 1, cleared));
		for (size_t j = cleared + 1; j < sizeof(guarded); j++)
			UT_ASSERT(guarded[j] == 0xa5);
		UT_ASSERT(guarded[0] == 0xa5);
	}
	for (size_t i = 0; i < lengthof(page_lengths); i++) {
		memset(guarded, 0xa5, sizeof(guarded));
		UT_ASSERT(!cluster_recovery_ctrc_target_image_v2(payload, 320, SYSTEM_ID, &one,
														 page_lengths[i], guarded + 1, BLCKSZ));
		UT_ASSERT(all_zero(guarded + 1, BLCKSZ));
		UT_ASSERT(guarded[0] == 0xa5 && guarded[BLCKSZ + 1] == 0xa5);
	}
	for (size_t i = 0; i < lengthof(payload_lengths); i++) {
		memset(guarded, 0xa5, sizeof(guarded));
		UT_ASSERT(!cluster_recovery_ctrc_target_image_v2(&one, payload_lengths[i], SYSTEM_ID,
														 page.data, BLCKSZ, guarded + 1, BLCKSZ));
		UT_ASSERT(all_zero(guarded + 1, BLCKSZ));
		UT_ASSERT(guarded[0] == 0xa5 && guarded[BLCKSZ + 1] == 0xa5);
	}
	for (int null_input = 0; null_input < 2; null_input++) {
		memset(guarded, 0xa5, sizeof(guarded));
		UT_ASSERT(!cluster_recovery_ctrc_target_image_v2(null_input ? payload : NULL, 320,
														 SYSTEM_ID, null_input ? NULL : page.data,
														 BLCKSZ, guarded + 1, BLCKSZ));
		UT_ASSERT(all_zero(guarded + 1, BLCKSZ));
		UT_ASSERT(guarded[0] == 0xa5 && guarded[BLCKSZ + 1] == 0xa5);
	}
	UT_ASSERT(!cluster_recovery_ctrc_target_image_v2(payload, 320, SYSTEM_ID, page.data, BLCKSZ,
													 NULL, BLCKSZ));
}

UT_TEST(image_unaligned_and_aliasing)
{
	ClusterRecoveryCTRCReleaseV2 r = sample();
	PGAlignedBlock page, expected;
	uint8 payload[322], image[BLCKSZ + 2], output[BLCKSZ + 2];

	memset(payload, 0xa5, sizeof(payload));
	UT_ASSERT(target_sample(&r, false, &page, payload + 1));
	UT_ASSERT(target_sample(&r, true, &expected, payload + 1));
	memset(image, 0xa5, sizeof(image));
	memcpy(image + 1, page.data, BLCKSZ);
	memset(output, 0xa5, sizeof(output));
	UT_ASSERT(cluster_recovery_ctrc_target_image_v2(payload + 1, 320, SYSTEM_ID, image + 1, BLCKSZ,
													output + 1, BLCKSZ));
	UT_ASSERT(memcmp(output + 1, expected.data, BLCKSZ) == 0);
	UT_ASSERT(output[0] == 0xa5 && output[BLCKSZ + 1] == 0xa5);
	UT_ASSERT(payload[0] == 0xa5 && payload[321] == 0xa5);
	UT_ASSERT(memcmp(image + 1, page.data, BLCKSZ) == 0);
	UT_ASSERT(cluster_recovery_ctrc_target_image_v2(payload + 1, 320, SYSTEM_ID, image + 1, BLCKSZ,
													image + 1, BLCKSZ));
	UT_ASSERT(memcmp(image + 1, expected.data, BLCKSZ) == 0);
	UT_ASSERT(image[0] == 0xa5 && image[BLCKSZ + 1] == 0xa5);
	memset(output, 0xa5, sizeof(output));
	memcpy(output + 17, payload + 1, 320);
	UT_ASSERT(cluster_recovery_ctrc_target_image_v2(output + 17, 320, SYSTEM_ID, page.data, BLCKSZ,
													output + 1, BLCKSZ));
	UT_ASSERT(memcmp(output + 1, expected.data, BLCKSZ) == 0);
	UT_ASSERT(output[0] == 0xa5 && output[BLCKSZ + 1] == 0xa5);
}

UT_TEST(image_native_boundaries)
{
	ClusterRecoveryCTRCReleaseV2 r = sample();
	PGAlignedBlock page, out;
	uint8 payload[320];

	for (int high = 0; high < 2; high++) {
		r.origin_node_id = high ? 127 : 0;
		r.target_owner = high ? 128 : 1;
		r.writer_thread = high ? 128 : 1;
		r.segment_id = high ? 32768 : 1;
		r.segment_generation = high ? UINT32_MAX - 1 : 0;
		r.slot_offset = high ? TT_SLOTS_PER_SEGMENT - 1 : 0;
		r.wrap = high ? TT_WRAP_MAX : TT_WRAP_INITIAL;
		r.xid = high ? MaxTransactionId : FirstNormalTransactionId;
		r.before.xid = r.xid;
		r.before.wrap = r.wrap;
		r.before.first_undo_block = (UBA)InvalidUba_init;
		r.after = r.before;
		r.after.flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
		UT_ASSERT(target_sample(&r, false, &page, payload));
		UT_ASSERT(image_make(payload, &page, &out));
		UT_ASSERT(target_match(payload, &out) == CLUSTER_RECOVERY_CTRC_TARGET_EXPECTED_AFTER);
	}
}


/* Original reader over two in-memory WAL pages. The fixture supplies bytes and
 * selected system/thread, not a replacement decoder or completion proof. */
#define NATIVE_SEG_SIZE (16 * 1024 * 1024)
#define NATIVE_BASE ((XLogRecPtr)NATIVE_SEG_SIZE)
#define NATIVE_BLOCK 1
#define NATIVE_ORIGIN 2
#define NATIVE_TOP_XID 4
#define NATIVE_EDGE 8

typedef union NativeRaw {
	XLogRecord align;
	uint8 bytes[1024];
} NativeRaw;

typedef struct NativePages {
	PGAlignedXLogBlock pages[2];
	unsigned reads;
} NativePages;

static void
native_crc(NativeRaw *wire, size_t length)
{
	pg_crc32c crc;

	wire->align.xl_tot_len = length;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, wire->bytes + SizeOfXLogRecord, length - SizeOfXLogRecord);
	COMP_CRC32C(crc, wire->bytes, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(crc);
	wire->align.xl_crc = crc;
}

static size_t
native_wire(NativeRaw *wire, const uint8 *payload, uint32 length, unsigned extra)
{
	uint8 *next;
	RepOriginId origin = 7;
	TransactionId top = 123;

	memset(wire, 0, sizeof(*wire));
	wire->align.xl_rmid = RM_CLUSTER_UNDO_ID;
	wire->align.xl_info = 0xD0; /* Frozen D2 encoding, independent of adapter. */
	wire->align.xl_scn = 41;
	next = wire->bytes + SizeOfXLogRecord;
	if (extra & NATIVE_EDGE) {
		/* One valid PageVersion edge for the following block, canonical LE. */
		memset(next, 0, 64);
		next[0] = XLR_BLOCK_ID_PAGE_VERSION_EDGE;
		next[1] = 1;
		next[2] = 1;
		next[3] = 48;
		next[8] = 99;
		next[17] = RF_PAGE_CLASS_ORDINARY;
		next[18] = next[19] = RF_PAGE_STATE_PRESENT;
		for (unsigned i = 0; i < 16; i++)
			next[24 + i] = next[48 + i] = 7 + i;
		next[40] = 41;
		next += 64;
	}
	if (extra & (NATIVE_BLOCK | NATIVE_EDGE)) {
		XLogRecordBlockHeader block = { 0 };
		RelFileLocator locator = { 1663, 5, 17 };
		BlockNumber block_number = 23;

		memcpy(next, &block, SizeOfXLogRecordBlockHeader);
		next += SizeOfXLogRecordBlockHeader;
		memcpy(next, &locator, sizeof(locator));
		next += sizeof(locator);
		memcpy(next, &block_number, sizeof(block_number));
		next += sizeof(block_number);
	}
	if (extra & NATIVE_ORIGIN) {
		*next++ = XLR_BLOCK_ID_ORIGIN;
		memcpy(next, &origin, sizeof(origin));
		next += sizeof(origin);
	}
	if (extra & NATIVE_TOP_XID) {
		*next++ = XLR_BLOCK_ID_TOPLEVEL_XID;
		memcpy(next, &top, sizeof(top));
		next += sizeof(top);
	}
	*next++ = XLR_BLOCK_ID_DATA_LONG;
	memcpy(next, &length, sizeof(length));
	next += sizeof(length);
	memcpy(next, payload, length);
	next += length;
	native_crc(wire, next - wire->bytes);
	return next - wire->bytes;
}

static int
native_page_read(XLogReaderState *reader, XLogRecPtr target, int required pg_attribute_unused(),
				 XLogRecPtr record pg_attribute_unused(), char *buffer)
{
	NativePages *pages = reader->private_data;

	pages->reads++;
	if (target != NATIVE_BASE && target != NATIVE_BASE + XLOG_BLCKSZ)
		return -1;
	memcpy(buffer, pages->pages[(target - NATIVE_BASE) / XLOG_BLCKSZ].data, XLOG_BLCKSZ);
	reader->seg.ws_tli = 1;
	return XLOG_BLCKSZ;
}

static bool
native_read(NativePages *pages, const NativeRaw *wire, size_t length, bool cross_page,
			XLogReaderState **out)
{
	XLogReaderRoutine routine = { .page_read = native_page_read };
	XLogLongPageHeader first;
	XLogPageHeader second;
	XLogRecPtr start;
	size_t offset, available;
	char *error = NULL;

	memset(pages, 0, sizeof(*pages));
	first = (XLogLongPageHeader)pages->pages[0].data;
	first->std.xlp_magic = XLOG_PAGE_MAGIC;
	first->std.xlp_info = XLP_LONG_HEADER;
	first->std.xlp_tli = 1;
	first->std.xlp_pageaddr = NATIVE_BASE;
	first->std.xlp_thread_id = 3;
	first->xlp_sysid = SYSTEM_ID;
	first->xlp_seg_size = NATIVE_SEG_SIZE;
	first->xlp_xlog_blcksz = XLOG_BLCKSZ;
	offset = cross_page
		? XLOG_BLCKSZ - Min((size_t)128, (size_t)MAXALIGN_DOWN(length / 2))
		: SizeOfXLogLongPHD;
	available = XLOG_BLCKSZ - offset;
	memcpy(pages->pages[0].data + offset, wire->bytes, Min(length, available));
	if (length > available) {
		second = (XLogPageHeader)pages->pages[1].data;
		second->xlp_magic = XLOG_PAGE_MAGIC;
		second->xlp_info = XLP_FIRST_IS_CONTRECORD;
		second->xlp_tli = 1;
		second->xlp_pageaddr = NATIVE_BASE + XLOG_BLCKSZ;
		second->xlp_thread_id = 3;
		second->xlp_rem_len = length - available;
		memcpy(pages->pages[1].data + SizeOfXLogShortPHD, wire->bytes + available,
			   length - available);
	}
	*out = XLogReaderAllocate(NATIVE_SEG_SIZE, NULL, &routine, pages);
	if (*out == NULL)
		abort();
	(*out)->system_identifier = SYSTEM_ID;
	(*out)->cluster_expected_thread_id = 3;
	start = NATIVE_BASE + offset;
	XLogBeginRead(*out, start);
	return XLogReadRecord(*out, &error) != NULL;
}

static bool
reader_refused(const XLogReaderState *reader)
{
	struct {
		uint64 before;
		ClusterRecoveryCTRCReleaseV2 value;
		uint64 after;
	} out;

	memset(&out, 0xa5, sizeof(out));
	return !cluster_recovery_ctrc_read_v2(reader, &out.value)
		   && all_zero(&out.value, sizeof(out.value)) && out.before == UINT64_C(0xa5a5a5a5a5a5a5a5)
		   && out.after == UINT64_C(0xa5a5a5a5a5a5a5a5);
}

UT_TEST(reader_terminal_payloads_and_immutability)
{
	NativeRaw wire;
	NativePages pages, saved_pages;
	XLogReaderState *reader, saved_reader;
	ClusterRecoveryCTRCReleaseV2 out, expected;
	uint8 payload[320];

	for (unsigned terminal = TT_SLOT_COMMITTED; terminal <= TT_SLOT_ABORTED; terminal++) {
		size_t length;

		expected = sample();
		expected.terminal_outcome = expected.before.status = expected.after.status = terminal;
		if (terminal == TT_SLOT_ABORTED)
			expected.before.commit_scn = expected.after.commit_scn = InvalidScn;
		UT_ASSERT(cluster_recovery_ctrc_encode_v2(&expected, SYSTEM_ID, payload, sizeof(payload)));
		length = native_wire(&wire, payload, sizeof(payload), 0);
		UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
		saved_pages = pages;
		saved_reader = *reader;
		UT_ASSERT(cluster_recovery_ctrc_read_v2(reader, &out));
		UT_ASSERT(same_fields(&out, &expected));
		UT_ASSERT(cluster_recovery_ctrc_read_v2(reader, &out));
		UT_ASSERT(memcmp(&pages, &saved_pages, sizeof(pages)) == 0);
		UT_ASSERT(memcmp(reader, &saved_reader, sizeof(*reader)) == 0);
		UT_ASSERT(memcmp(XLogRecGetData(reader), payload, sizeof(payload)) == 0);
		XLogReaderFree(reader);
	}
}

UT_TEST(reader_cross_page_actual_positions)
{
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	ClusterRecoveryCTRCReleaseV2 out, expected = sample();
	size_t length = native_wire(&wire, golden, sizeof(golden), 0);

	UT_ASSERT(native_read(&pages, &wire, length, true, &reader));
	UT_ASSERT_EQ(reader->ReadRecPtr, NATIVE_BASE + XLOG_BLCKSZ - 128);
	UT_ASSERT(reader->EndRecPtr - reader->ReadRecPtr > length);
	UT_ASSERT(reader->record->lsn == reader->ReadRecPtr);
	UT_ASSERT(reader->record->next_lsn == reader->EndRecPtr);
	UT_ASSERT(cluster_recovery_ctrc_read_v2(reader, &out));
	UT_ASSERT(same_fields(&out, &expected));
	XLogReaderFree(reader);
}

UT_TEST(reader_exact_rmgr_and_info)
{
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	ClusterRecoveryCTRCReleaseV2 out;

	for (unsigned info = 0; info <= UINT8_MAX; info++) {
		size_t length = native_wire(&wire, golden, sizeof(golden), 0);
		wire.align.xl_info = info;
		native_crc(&wire, length);
		UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
		if (info == 0xD0)
			UT_ASSERT(cluster_recovery_ctrc_read_v2(reader, &out));
		else
			UT_ASSERT(reader_refused(reader));
		XLogReaderFree(reader);
	}
	for (unsigned rmid = RM_XLOG_ID; rmid <= RM_XACT_ID; rmid++) {
		size_t length = native_wire(&wire, golden, sizeof(golden), 0);
		wire.align.xl_rmid = rmid;
		native_crc(&wire, length);
		UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
		UT_ASSERT(reader_refused(reader));
		XLogReaderFree(reader);
	}
}

UT_TEST(reader_standalone_native_frame)
{
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	const unsigned extras[] = { NATIVE_ORIGIN, NATIVE_TOP_XID, NATIVE_ORIGIN | NATIVE_TOP_XID };
	const TransactionId xids[] = { BootstrapTransactionId, FrozenTransactionId, 123, UINT32_MAX };

	for (unsigned i = 0; i < lengthof(extras); i++) {
		size_t length = native_wire(&wire, golden, sizeof(golden), extras[i]);
		UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
		UT_ASSERT(reader_refused(reader));
		XLogReaderFree(reader);
	}
	for (unsigned i = 0; i < lengthof(xids); i++) {
		size_t length = native_wire(&wire, golden, sizeof(golden), 0);
		wire.align.xl_xid = xids[i];
		native_crc(&wire, length);
		UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
		UT_ASSERT(reader_refused(reader));
		XLogReaderFree(reader);
	}
}

UT_TEST(reader_extra_block_and_page_edge)
{
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	const unsigned extras[] = { NATIVE_BLOCK, NATIVE_EDGE };

	for (unsigned i = 0; i < lengthof(extras); i++) {
		size_t length = native_wire(&wire, golden, sizeof(golden), extras[i]);
		UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
		UT_ASSERT(XLogRecHasAnyBlockRefs(reader));
		UT_ASSERT(XLogRecHasPageVersionEdge(reader) == (extras[i] == NATIVE_EDGE));
		UT_ASSERT(reader_refused(reader));
		XLogReaderFree(reader);
	}
}

UT_TEST(reader_exact_payload_and_codec)
{
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	uint8 payload[321];
	const unsigned lengths[] = { 0, 192, 319, 321 };
	const unsigned offsets[] = { 0, 56, 128, 192, 224, 256, 288 };

	memcpy(payload, golden, sizeof(golden));
	payload[320] = 0;
	for (unsigned i = 0; i < lengthof(lengths); i++) {
		size_t length = native_wire(&wire, payload, lengths[i], 0);
		UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
		UT_ASSERT(reader_refused(reader));
		XLogReaderFree(reader);
	}
	/* Native CRC is valid; payload digest and semantic/reserved checks remain
	 * the existing codec's responsibility. */
	for (unsigned i = 0; i < lengthof(offsets); i++) {
		size_t length;

		memcpy(payload, golden, sizeof(golden));
		payload[offsets[i]] ^= 1;
		length = native_wire(&wire, payload, sizeof(golden), 0);
		UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
		UT_ASSERT(reader_refused(reader));
		XLogReaderFree(reader);
	}
}

UT_TEST(reader_system_and_selected_thread)
{
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	ClusterRecoveryCTRCReleaseV2 expected = sample();
	uint8 payload[320];
	const uint16 threads[] = { 1, 2, 4 };
	size_t length;

	for (unsigned i = 0; i < lengthof(threads); i++) {
		expected.writer_thread = threads[i];
		UT_ASSERT(cluster_recovery_ctrc_encode_v2(&expected, SYSTEM_ID, payload, sizeof(payload)));
		length = native_wire(&wire, payload, sizeof(payload), 0);
		UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
		UT_ASSERT(reader_refused(reader));
		XLogReaderFree(reader);
	}
	length = native_wire(&wire, golden, sizeof(golden), 0);
	UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
	reader->system_identifier = 0;
	UT_ASSERT(reader_refused(reader));
	reader->system_identifier = SYSTEM_ID ^ UINT64_C(0x100000000);
	UT_ASSERT(reader_refused(reader));
	reader->system_identifier = SYSTEM_ID;
	reader->cluster_expected_thread_id = XLP_THREAD_ID_INVALID;
	UT_ASSERT(reader_refused(reader));
	reader->cluster_expected_thread_id = XLP_THREAD_ID_LEGACY;
	UT_ASSERT(reader_refused(reader));
	XLogReaderFree(reader);
}

UT_TEST(reader_requires_current_completed_record)
{
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	XLogReaderState empty = { 0 };
	DecodedXLogRecord *record;
	XLogRecPtr start, end;
	size_t length = native_wire(&wire, golden, sizeof(golden), 0);

	UT_ASSERT(reader_refused(NULL));
	UT_ASSERT(reader_refused(&empty));
	UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
	UT_ASSERT(!cluster_recovery_ctrc_read_v2(reader, NULL));
	record = reader->record;
	start = reader->ReadRecPtr;
	end = reader->EndRecPtr;
	reader->record = NULL;
	UT_ASSERT(reader_refused(reader));
	reader->record = record;
	reader->ReadRecPtr = InvalidXLogRecPtr;
	UT_ASSERT(reader_refused(reader));
	reader->ReadRecPtr = start;
	reader->EndRecPtr = start;
	UT_ASSERT(reader_refused(reader));
	reader->EndRecPtr = start - 1;
	UT_ASSERT(reader_refused(reader));
	reader->EndRecPtr = end;
	record->lsn++;
	UT_ASSERT(reader_refused(reader));
	record->lsn = start;
	record->next_lsn++;
	UT_ASSERT(reader_refused(reader));
	record->next_lsn = end;
	record->main_data = NULL;
	UT_ASSERT(reader_refused(reader));
	XLogReaderFree(reader);
}

UT_TEST(reader_native_crc_refusal_is_not_completion)
{
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	size_t length = native_wire(&wire, golden, sizeof(golden), 0);

	wire.bytes[length - 1] ^= 1; /* Do not reseal native CRC. */
	UT_ASSERT(!native_read(&pages, &wire, length, false, &reader));
	UT_ASSERT(reader->record == NULL);
	UT_ASSERT(reader_refused(reader));
	XLogReaderFree(reader);
}

/* Independent transcript: no production record hash or LE writer. */
static bool
reference_digest(const uint8 source[32], XLogRecPtr start, XLogRecPtr end, const NativeRaw *wire,
				 size_t length, uint8 out[32])
{
	uint8 transcript[18 + 32 + 20 + sizeof(wire->bytes)];
	uint8 *next = transcript;

	if (length > sizeof(wire->bytes))
		return false;
	memcpy(next, "PGRAC-D0-RECORD-V2", 18);
	next += 18;
	memcpy(next, source, 32);
	next += 32;
	for (unsigned i = 0; i < 8; i++)
		*next++ = (uint8)(start >> (8 * i));
	for (unsigned i = 0; i < 8; i++)
		*next++ = (uint8)(end >> (8 * i));
	for (unsigned i = 0; i < 4; i++)
		*next++ = (uint8)(length >> (8 * i));
	memcpy(next, wire->bytes, length);
	next += length;
	return sha256(transcript, next - transcript, out);
}

static bool
reference_refused(const XLogReaderState *reader, const uint8 *source, const uint8 *raw,
				  size_t length)
{
	struct {
		uint64 before;
		ClusterRecoveryCTRCReferenceV2 value;
		uint64 after;
	} out;

	memset(&out, 0xa5, sizeof(out));
	return !cluster_recovery_ctrc_reference_v2(reader, 7, source, raw, length, &out.value)
		   && all_zero(&out.value, sizeof(out.value)) && out.before == UINT64_C(0xa5a5a5a5a5a5a5a5)
		   && out.after == UINT64_C(0xa5a5a5a5a5a5a5a5);
}

UT_TEST(reference_terminal_records)
{
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	ClusterRecoveryCTRCReferenceV2 out;
	ClusterRecoveryCTRCReleaseV2 r;
	uint8 payload[320], expected[32];

	for (unsigned terminal = TT_SLOT_COMMITTED; terminal <= TT_SLOT_ABORTED; terminal++) {
		size_t length;
		uint32 index = terminal == TT_SLOT_COMMITTED ? 0 : UINT32_MAX;

		r = sample();
		r.terminal_outcome = r.before.status = r.after.status = terminal;
		if (terminal == TT_SLOT_ABORTED)
			r.before.commit_scn = r.after.commit_scn = InvalidScn;
		UT_ASSERT(cluster_recovery_ctrc_encode_v2(&r, SYSTEM_ID, payload, sizeof(payload)));
		length = native_wire(&wire, payload, sizeof(payload), 0);
		UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
		UT_ASSERT(reference_digest(raw_source, reader->ReadRecPtr, reader->EndRecPtr, &wire, length,
								   expected));
		UT_ASSERT(cluster_recovery_ctrc_reference_v2(reader, index, raw_source, wire.bytes, length,
													 &out));
		UT_ASSERT_EQ(out.source_index, index);
		UT_ASSERT_EQ(out.start, reader->ReadRecPtr);
		UT_ASSERT_EQ(out.end, reader->EndRecPtr);
		UT_ASSERT_EQ(out.raw_length, length);
		UT_ASSERT(memcmp(out.raw_record_sha256, expected, 32) == 0);
		XLogReaderFree(reader);
	}
}

UT_TEST(reference_cross_page_actual_interval)
{
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	ClusterRecoveryCTRCReferenceV2 out;
	uint8 expected[32], guessed[32];
	size_t length = native_wire(&wire, golden, sizeof(golden), 0);

	UT_ASSERT(native_read(&pages, &wire, length, true, &reader));
	UT_ASSERT(reader->EndRecPtr > reader->ReadRecPtr + length);
	UT_ASSERT(reference_digest(raw_source, reader->ReadRecPtr, reader->EndRecPtr, &wire, length,
							   expected));
	UT_ASSERT(reference_digest(raw_source, reader->ReadRecPtr, reader->ReadRecPtr + length, &wire,
							   length, guessed));
	UT_ASSERT(cluster_recovery_ctrc_reference_v2(reader, 9, raw_source, wire.bytes, length, &out));
	UT_ASSERT_EQ(out.source_index, 9);
	UT_ASSERT_EQ(out.start, NATIVE_BASE + XLOG_BLCKSZ - 128);
	UT_ASSERT_EQ(out.end, reader->EndRecPtr);
	UT_ASSERT_EQ(out.raw_length, length);
	UT_ASSERT(memcmp(out.raw_record_sha256, expected, 32) == 0);
	UT_ASSERT(memcmp(out.raw_record_sha256, guessed, 32) != 0);
	XLogReaderFree(reader);
}

UT_TEST(reference_complete_source_domain)
{
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	ClusterRecoveryCTRCReferenceV2 out, first;
	uint8 source[32], expected[32], zero[32] = { 0 };
	size_t length = native_wire(&wire, golden, sizeof(golden), 0);

	UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
	UT_ASSERT(
		cluster_recovery_ctrc_reference_v2(reader, 0, raw_source, wire.bytes, length, &first));
	for (unsigned i = 0; i < 32; i++) {
		memcpy(source, raw_source, sizeof(source));
		source[i] ^= 0x80;
		UT_ASSERT(reference_digest(source, reader->ReadRecPtr, reader->EndRecPtr, &wire, length,
								   expected));
		UT_ASSERT(cluster_recovery_ctrc_reference_v2(reader, 0, source, wire.bytes, length, &out));
		UT_ASSERT(memcmp(out.raw_record_sha256, expected, 32) == 0);
		UT_ASSERT(memcmp(out.raw_record_sha256, first.raw_record_sha256, 32) != 0);
	}
	/* R carries the original source index separately from its record SHA. */
	UT_ASSERT(cluster_recovery_ctrc_reference_v2(reader, UINT32_MAX, raw_source, wire.bytes, length,
												 &out));
	UT_ASSERT_EQ(out.source_index, UINT32_MAX);
	UT_ASSERT(memcmp(out.raw_record_sha256, first.raw_record_sha256, 32) == 0);
	UT_ASSERT(reference_refused(reader, zero, wire.bytes, length));
	UT_ASSERT(reference_refused(reader, NULL, wire.bytes, length));
	XLogReaderFree(reader);
}

UT_TEST(reference_exact_raw_bounds)
{
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	XLogReaderState empty = { 0 };
	size_t length = native_wire(&wire, golden, sizeof(golden), 0);
	size_t lengths[] = { 0, SizeOfXLogRecord, length - 1, length + 1, SIZE_MAX };

	UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
	for (unsigned i = 0; i < lengthof(lengths); i++)
		UT_ASSERT(reference_refused(reader, raw_source, wire.bytes, lengths[i]));
	UT_ASSERT(reference_refused(reader, raw_source, NULL, length));
	UT_ASSERT(reference_refused(NULL, raw_source, wire.bytes, length));
	UT_ASSERT(reference_refused(&empty, raw_source, wire.bytes, length));
	UT_ASSERT(!cluster_recovery_ctrc_reference_v2(reader, 0, raw_source, wire.bytes, length, NULL));
	XLogReaderFree(reader);
}

UT_TEST(reference_valid_crc_header_mismatch)
{
	NativeRaw wire, changed;
	NativePages pages;
	XLogReaderState *reader;
	uint8 digest[32];
	size_t length = native_wire(&wire, golden, sizeof(golden), 0);
	size_t offsets[] = { offsetof(XLogRecord, xl_prev), offsetof(XLogRecord, xl_scn),
						 offsetof(XLogRecord, xl_xid),	offsetof(XLogRecord, xl_info),
						 offsetof(XLogRecord, xl_rmid), offsetof(XLogRecord, xl_crc) - 1 };

	UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
	for (unsigned i = 0; i < lengthof(offsets); i++) {
		changed = wire;
		changed.bytes[offsets[i]] ^= 1;
		native_crc(&changed, length);
		/* Complete, CRC-valid raw input alone does not bind this current read. */
		UT_ASSERT(cluster_recovery_ctrc_record_hash_v2(
			raw_source, reader->ReadRecPtr, reader->EndRecPtr, changed.bytes, length, digest));
		UT_ASSERT(reference_refused(reader, raw_source, changed.bytes, length));
	}
	XLogReaderFree(reader);
}

UT_TEST(reference_valid_payload_mismatch)
{
	NativeRaw wire, changed;
	NativePages pages, other_pages;
	XLogReaderState *reader, *other;
	ClusterRecoveryCTRCReleaseV2 decoded;
	ClusterRecoveryCTRCReferenceV2 out;
	uint8 payload[320];
	size_t length = native_wire(&wire, golden, sizeof(golden), 0);
	unsigned offsets[] = { 48, 224, 256 };

	UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
	for (unsigned i = 0; i < lengthof(offsets); i++) {
		memcpy(payload, golden, sizeof(payload));
		payload[offsets[i]] ^= 1;
		UT_ASSERT(reseal(payload, true));
		UT_ASSERT_EQ(native_wire(&changed, payload, sizeof(payload), 0), length);
		UT_ASSERT(native_read(&other_pages, &changed, length, false, &other));
		UT_ASSERT(cluster_recovery_ctrc_read_v2(other, &decoded));
		UT_ASSERT(reference_refused(reader, raw_source, changed.bytes, length));
		UT_ASSERT(reference_refused(other, raw_source, wire.bytes, length));
		UT_ASSERT(
			cluster_recovery_ctrc_reference_v2(other, 0, raw_source, changed.bytes, length, &out));
		XLogReaderFree(other);
	}
	XLogReaderFree(reader);
}

UT_TEST(reference_exact_standalone_frame)
{
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	ClusterRecoveryCTRCReleaseV2 decoded;
	unsigned extras[] = { NATIVE_BLOCK, NATIVE_ORIGIN, NATIVE_TOP_XID, NATIVE_EDGE };
	size_t length;

	for (unsigned i = 0; i < lengthof(extras); i++) {
		length = native_wire(&wire, golden, sizeof(golden), extras[i]);
		UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
		UT_ASSERT(reference_refused(reader, raw_source, wire.bytes, length));
		XLogReaderFree(reader);
	}
	/* Even a syntactically present zero-origin frame is extra raw structure. */
	length = native_wire(&wire, golden, sizeof(golden), NATIVE_ORIGIN);
	memset(wire.bytes + SizeOfXLogRecord + 1, 0, sizeof(RepOriginId));
	native_crc(&wire, length);
	UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
	UT_ASSERT(cluster_recovery_ctrc_read_v2(reader, &decoded));
	UT_ASSERT(reference_refused(reader, raw_source, wire.bytes, length));
	XLogReaderFree(reader);
}

UT_TEST(reference_requires_current_reader)
{
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	DecodedXLogRecord *record;
	ClusterRecoveryCTRCReferenceV2 out;
	size_t length = native_wire(&wire, golden, sizeof(golden), 0);

	UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
	record = reader->record;
	reader->record = NULL;
	UT_ASSERT(reference_refused(reader, raw_source, wire.bytes, length));
	reader->record = record;
	record->lsn++;
	UT_ASSERT(reference_refused(reader, raw_source, wire.bytes, length));
	record->lsn--;
	reader->cluster_expected_thread_id++;
	UT_ASSERT(reference_refused(reader, raw_source, wire.bytes, length));
	reader->cluster_expected_thread_id--;
	reader->system_identifier ^= UINT64_C(0x100000000);
	UT_ASSERT(reference_refused(reader, raw_source, wire.bytes, length));
	reader->system_identifier = SYSTEM_ID;
	UT_ASSERT(cluster_recovery_ctrc_reference_v2(reader, 0, raw_source, wire.bytes, length, &out));
	XLogReaderFree(reader);
}

UT_TEST(reference_unaligned_immutable_and_alias)
{
	NativeRaw wire, saved_wire;
	NativePages pages, saved_pages;
	XLogReaderState *reader, saved_reader;
	ClusterRecoveryCTRCReferenceV2 out;
	union {
		ClusterRecoveryCTRCReferenceV2 value;
		uint8 bytes[128];
	} source_alias;
	uint8 raw[1025], source[33], expected[32];
	size_t length = native_wire(&wire, golden, sizeof(golden), 0);

	UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
	saved_wire = wire;
	saved_pages = pages;
	saved_reader = *reader;
	memcpy(raw + 1, wire.bytes, length);
	memcpy(source + 1, raw_source, 32);
	UT_ASSERT(reference_digest(raw_source, reader->ReadRecPtr, reader->EndRecPtr, &wire, length,
							   expected));
	UT_ASSERT(cluster_recovery_ctrc_reference_v2(reader, 6, source + 1, raw + 1, length, &out));
	UT_ASSERT(memcmp(out.raw_record_sha256, expected, 32) == 0);
	UT_ASSERT(memcmp(raw + 1, wire.bytes, length) == 0);
	UT_ASSERT(memcmp(source + 1, raw_source, 32) == 0);
	UT_ASSERT(cluster_recovery_ctrc_reference_v2(reader, 6, raw_source, wire.bytes, length, &out));
	UT_ASSERT(memcmp(&wire, &saved_wire, sizeof(wire)) == 0);
	UT_ASSERT(memcmp(&pages, &saved_pages, sizeof(pages)) == 0);
	UT_ASSERT(memcmp(reader, &saved_reader, sizeof(*reader)) == 0);
	UT_ASSERT(memcmp(XLogRecGetData(reader), golden, sizeof(golden)) == 0);
	UT_ASSERT(cluster_recovery_ctrc_reference_v2(reader, 6, raw_source, wire.bytes, length,
												 (ClusterRecoveryCTRCReferenceV2 *)wire.bytes));
	memcpy(&out, wire.bytes, sizeof(out));
	UT_ASSERT_EQ(out.source_index, 6);
	UT_ASSERT(memcmp(out.raw_record_sha256, expected, 32) == 0);
	wire = saved_wire;
	memcpy(source_alias.bytes, raw_source, 32);
	UT_ASSERT(cluster_recovery_ctrc_reference_v2(reader, 6, source_alias.bytes, wire.bytes, length,
												 &source_alias.value));
	UT_ASSERT(memcmp(source_alias.value.raw_record_sha256, expected, 32) == 0);
	XLogReaderFree(reader);
}

UT_TEST(reference_each_raw_bit_refused)
{
	NativeRaw wire, changed;
	NativePages pages;
	XLogReaderState *reader;
	size_t length = native_wire(&wire, golden, sizeof(golden), 0);

	UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
	for (size_t i = 0; i < length; i++)
		for (unsigned bit = 0; bit < 8; bit++) {
			changed = wire;
			changed.bytes[i] ^= 1U << bit;
			UT_ASSERT(reference_refused(reader, raw_source, changed.bytes, length));
		}
	XLogReaderFree(reader);
}

/* Independent frozen atom1 transcript: literal widths/offsets, not product
 * serialization helpers or the native in-memory reference struct. */
static void
wal_atom_uint(uint8 *out, uint64 value, unsigned width)
{
	for (unsigned i = 0; i < width; i++)
		out[i] = (uint8)(value >> (8 * i));
}

static bool
wal_atom_expected(const XLogReaderState *reader, uint32 index, const uint8 *source,
				  const NativeRaw *wire, size_t length, uint8 *out)
{
	memset(out, 0, CLUSTER_RECOVERY_CTRC_WAL_ATOM_BYTES);
	out[0] = 1;
	wal_atom_uint(out + 2, 56, 8);
	wal_atom_uint(out + 10, index, 4);
	wal_atom_uint(out + 14, reader->ReadRecPtr, 8);
	wal_atom_uint(out + 22, reader->EndRecPtr, 8);
	wal_atom_uint(out + 30, length, 4);
	if (!reference_digest(source, reader->ReadRecPtr, reader->EndRecPtr, wire, length, out + 34))
		return false;
	wal_atom_uint(out + 66, length, 8);
	memcpy(out + 74, wire->bytes, length);
	return 74 + length == CLUSTER_RECOVERY_CTRC_WAL_ATOM_BYTES;
}

static bool
wal_atom_refused(const XLogReaderState *reader, const uint8 *source, const uint8 *raw,
				 size_t length)
{
	uint8 out[CLUSTER_RECOVERY_CTRC_WAL_ATOM_BYTES + 2];

	memset(out, 0xa5, sizeof(out));
	return !cluster_recovery_ctrc_wal_atom_v2(reader, 7, source, raw, length, out + 1,
											  CLUSTER_RECOVERY_CTRC_WAL_ATOM_BYTES)
		   && all_zero(out + 1, sizeof(out) - 2) && out[0] == 0xa5 && out[sizeof(out) - 1] == 0xa5;
}

UT_TEST(wal_atom_canonical_terminal_records)
{
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	ClusterRecoveryCTRCReleaseV2 record;
	uint8 payload[320], out[CLUSTER_RECOVERY_CTRC_WAL_ATOM_BYTES], expected[sizeof(out)];

	for (unsigned terminal = TT_SLOT_COMMITTED; terminal <= TT_SLOT_ABORTED; terminal++) {
		size_t length;
		uint32 index = terminal == TT_SLOT_COMMITTED ? 0 : UINT32_MAX;

		record = sample();
		record.terminal_outcome = record.before.status = record.after.status = terminal;
		if (terminal == TT_SLOT_ABORTED)
			record.before.commit_scn = record.after.commit_scn = InvalidScn;
		UT_ASSERT(cluster_recovery_ctrc_encode_v2(&record, SYSTEM_ID, payload, sizeof(payload)));
		length = native_wire(&wire, payload, sizeof(payload), 0);
		UT_ASSERT_EQ(length, 357);
		UT_ASSERT_EQ(sizeof(out), 431);
		UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
		UT_ASSERT(wal_atom_expected(reader, index, raw_source, &wire, length, expected));
		UT_ASSERT(cluster_recovery_ctrc_wal_atom_v2(reader, index, raw_source, wire.bytes, length,
													out, sizeof(out)));
		UT_ASSERT(memcmp(out, expected, sizeof(out)) == 0);
		XLogReaderFree(reader);
	}
}

UT_TEST(wal_atom_cross_page_positions)
{
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	uint8 out[CLUSTER_RECOVERY_CTRC_WAL_ATOM_BYTES], expected[sizeof(out)];
	size_t length = native_wire(&wire, golden, sizeof(golden), 0);

	UT_ASSERT(native_read(&pages, &wire, length, true, &reader));
	UT_ASSERT(reader->EndRecPtr > reader->ReadRecPtr + length);
	UT_ASSERT(wal_atom_expected(reader, 0x98765432, raw_source, &wire, length, expected));
	UT_ASSERT(cluster_recovery_ctrc_wal_atom_v2(reader, 0x98765432, raw_source, wire.bytes, length,
												out, sizeof(out)));
	UT_ASSERT(memcmp(out, expected, sizeof(out)) == 0);
	/* Native page headers and alignment gaps belong to the interval, not raw. */
	UT_ASSERT(memcmp(out + 74, wire.bytes, length) == 0);
	XLogReaderFree(reader);
}

UT_TEST(wal_atom_source_domains)
{
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	uint8 out[CLUSTER_RECOVERY_CTRC_WAL_ATOM_BYTES], first[sizeof(out)], expected[sizeof(out)];
	uint8 source[32], zero[32] = { 0 };
	size_t length = native_wire(&wire, golden, sizeof(golden), 0);

	UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
	UT_ASSERT(cluster_recovery_ctrc_wal_atom_v2(reader, 0, raw_source, wire.bytes, length, first,
												sizeof(first)));
	for (unsigned i = 0; i < 32; i++) {
		memcpy(source, raw_source, sizeof(source));
		source[i] ^= 0x80;
		UT_ASSERT(wal_atom_expected(reader, UINT32_MAX, source, &wire, length, expected));
		UT_ASSERT(cluster_recovery_ctrc_wal_atom_v2(reader, UINT32_MAX, source, wire.bytes, length,
													out, sizeof(out)));
		UT_ASSERT(memcmp(out, expected, sizeof(out)) == 0);
		UT_ASSERT(memcmp(out + 34, first + 34, 32) != 0);
		UT_ASSERT(memcmp(out + 74, first + 74, length) == 0);
	}
	UT_ASSERT(wal_atom_refused(reader, zero, wire.bytes, length));
	UT_ASSERT(wal_atom_refused(reader, NULL, wire.bytes, length));
	XLogReaderFree(reader);
}

UT_TEST(wal_atom_exact_bounds_and_nulls)
{
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	uint8 out[CLUSTER_RECOVERY_CTRC_WAL_ATOM_BYTES + 2];
	size_t length = native_wire(&wire, golden, sizeof(golden), 0);
	size_t output_lengths[] = { 0, 1, 73, sizeof(out) - 3, sizeof(out) - 1, SIZE_MAX };
	size_t raw_lengths[] = { 0, SizeOfXLogRecord, length - 1, length + 1, SIZE_MAX };

	UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
	for (unsigned i = 0; i < lengthof(output_lengths); i++) {
		size_t cleared = Min(output_lengths[i], sizeof(out) - 2);

		memset(out, 0xa5, sizeof(out));
		UT_ASSERT(!cluster_recovery_ctrc_wal_atom_v2(reader, 0, raw_source, wire.bytes, length,
													 out + 1, output_lengths[i]));
		UT_ASSERT(all_zero(out + 1, cleared));
		for (size_t j = cleared + 1; j < sizeof(out); j++)
			UT_ASSERT_EQ(out[j], 0xa5);
		UT_ASSERT_EQ(out[0], 0xa5);
	}
	for (unsigned i = 0; i < lengthof(raw_lengths); i++)
		UT_ASSERT(wal_atom_refused(reader, raw_source, wire.bytes, raw_lengths[i]));
	UT_ASSERT(wal_atom_refused(reader, raw_source, NULL, length));
	UT_ASSERT(wal_atom_refused(NULL, raw_source, wire.bytes, length));
	UT_ASSERT(!cluster_recovery_ctrc_wal_atom_v2(reader, 0, raw_source, wire.bytes, length, NULL,
												 sizeof(out) - 2));
	XLogReaderFree(reader);
}

UT_TEST(wal_atom_current_reader_and_frame)
{
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	DecodedXLogRecord *decoded;
	uint8 out[CLUSTER_RECOVERY_CTRC_WAL_ATOM_BYTES];
	unsigned extras[] = { NATIVE_BLOCK, NATIVE_ORIGIN, NATIVE_TOP_XID, NATIVE_EDGE };
	size_t length = native_wire(&wire, golden, sizeof(golden), 0);

	UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
	decoded = reader->record;
	reader->record = NULL;
	UT_ASSERT(wal_atom_refused(reader, raw_source, wire.bytes, length));
	reader->record = decoded;
	decoded->next_lsn++;
	UT_ASSERT(wal_atom_refused(reader, raw_source, wire.bytes, length));
	decoded->next_lsn--;
	reader->cluster_expected_thread_id++;
	UT_ASSERT(wal_atom_refused(reader, raw_source, wire.bytes, length));
	reader->cluster_expected_thread_id--;
	reader->system_identifier++;
	UT_ASSERT(wal_atom_refused(reader, raw_source, wire.bytes, length));
	reader->system_identifier = SYSTEM_ID;
	UT_ASSERT(cluster_recovery_ctrc_wal_atom_v2(reader, 7, raw_source, wire.bytes, length, out,
												sizeof(out)));
	XLogReaderFree(reader);
	for (unsigned i = 0; i < lengthof(extras); i++) {
		length = native_wire(&wire, golden, sizeof(golden), extras[i]);
		UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
		UT_ASSERT(wal_atom_refused(reader, raw_source, wire.bytes, length));
		XLogReaderFree(reader);
	}
}

UT_TEST(wal_atom_complete_raw_binding)
{
	NativeRaw wire, changed;
	NativePages pages;
	XLogReaderState *reader;
	size_t length = native_wire(&wire, golden, sizeof(golden), 0);

	UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
	for (size_t i = 0; i < length; i++) {
		changed = wire;
		changed.bytes[i] ^= 1;
		UT_ASSERT(wal_atom_refused(reader, raw_source, changed.bytes, length));
	}
	/* A valid CRC over a different native header still is not this record. */
	changed = wire;
	changed.align.xl_prev++;
	native_crc(&changed, length);
	UT_ASSERT(wal_atom_refused(reader, raw_source, changed.bytes, length));
	/* A separately valid payload cannot replace this reader's main data. */
	changed = wire;
	changed.bytes[SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong + 48] ^= 1;
	UT_ASSERT(reseal(changed.bytes + SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong, true));
	native_crc(&changed, length);
	UT_ASSERT(wal_atom_refused(reader, raw_source, changed.bytes, length));
	XLogReaderFree(reader);
}

UT_TEST(wal_atom_unaligned_immutable_and_alias)
{
	NativeRaw wire, saved_wire;
	NativePages pages, saved_pages;
	XLogReaderState *reader, saved_reader;
	uint8 out[CLUSTER_RECOVERY_CTRC_WAL_ATOM_BYTES + 2], expected[sizeof(out) - 2];
	uint8 source[sizeof(out)], raw[sizeof(out)];
	size_t length = native_wire(&wire, golden, sizeof(golden), 0);

	UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
	saved_wire = wire;
	saved_pages = pages;
	saved_reader = *reader;
	memset(out, 0xa5, sizeof(out));
	memcpy(source + 1, raw_source, 32);
	memcpy(raw + 1, wire.bytes, length);
	UT_ASSERT(wal_atom_expected(reader, 0x12345678, raw_source, &wire, length, expected));
	UT_ASSERT(cluster_recovery_ctrc_wal_atom_v2(reader, 0x12345678, source + 1, raw + 1, length,
												out + 1, sizeof(expected)));
	UT_ASSERT(memcmp(out + 1, expected, sizeof(expected)) == 0);
	UT_ASSERT_EQ(out[0], 0xa5);
	UT_ASSERT_EQ(out[sizeof(out) - 1], 0xa5);
	UT_ASSERT(memcmp(raw + 1, wire.bytes, length) == 0);
	UT_ASSERT(memcmp(source + 1, raw_source, 32) == 0);
	UT_ASSERT(memcmp(&wire, &saved_wire, sizeof(wire)) == 0);
	UT_ASSERT(memcmp(&pages, &saved_pages, sizeof(pages)) == 0);
	UT_ASSERT(memcmp(reader, &saved_reader, sizeof(*reader)) == 0);
	UT_ASSERT(memcmp(XLogRecGetData(reader), golden, sizeof(golden)) == 0);
	UT_ASSERT(cluster_recovery_ctrc_wal_atom_v2(reader, 0x12345678, source + 1, raw + 1, length,
												raw + 1, sizeof(expected)));
	UT_ASSERT(memcmp(raw + 1, expected, sizeof(expected)) == 0);
	UT_ASSERT(cluster_recovery_ctrc_wal_atom_v2(reader, 0x12345678, source + 1, wire.bytes, length,
												source + 1, sizeof(expected)));
	UT_ASSERT(memcmp(source + 1, expected, sizeof(expected)) == 0);
	XLogReaderFree(reader);
}

/* Independent frozen kind1 transcript. The identity bytes come from the
 * encoded payload's fields; positions are literal offsets in the raw record,
 * rather than caller ordinals or a native structure's layout. */
static bool
wal_references_expected(const XLogReaderState *reader, uint32 index, const uint8 *source,
						const NativeRaw *wire, size_t length,
						ClusterRecoveryCTRCWalReferencesV2 *out)
{
	static const unsigned positions[] = { 153, 165, 197 };
	const uint8 *payload = wire->bytes + 37;
	uint8 atom[431], identity[34];

	if (!wal_atom_expected(reader, index, source, wire, length, atom))
		return false;
	wal_atom_uint(identity, SYSTEM_ID, 8);
	memcpy(identity + 8, payload + 40, 8);
	memcpy(identity + 16, payload + 96, 2);
	memcpy(identity + 18, payload + 108, 8);
	memcpy(identity + 26, payload + 102, 4);
	memcpy(identity + 30, payload + 116, 4);
	memset(out, 0, sizeof(*out));
	for (unsigned i = 0; i < 3; i++) {
		memcpy(out->keys[i], identity, sizeof(identity));
		out->keys[i][34] = 3; /* WAL carrier */
		wal_atom_uint(out->keys[i] + 36, 56, 8);
		memcpy(out->keys[i] + 44, atom + 10, 56);
		wal_atom_uint(out->keys[i] + 100, positions[i], 8);
	}
	out->evidence[0] = 1;
	wal_atom_uint(out->evidence + 8, 431, 8);
	memcpy(out->evidence + 16, atom, sizeof(atom));
	return true;
}

static bool
wal_references_refused(const XLogReaderState *reader, const uint8 *source, const uint8 *raw,
					   size_t length)
{
	uint8 out[sizeof(ClusterRecoveryCTRCWalReferencesV2) + 2];

	memset(out, 0xa5, sizeof(out));
	return !cluster_recovery_ctrc_wal_references_v2(reader, 7, source, raw, length,
													(ClusterRecoveryCTRCWalReferencesV2 *)(out + 1))
		   && all_zero(out + 1, sizeof(out) - 2) && out[0] == 0xa5 && out[sizeof(out) - 1] == 0xa5;
}

UT_TEST(wal_references_canonical_terminal_records)
{
	static const uint8 identity[34]
		= { 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x48, 0x47, 0x46, 0x45,
			0x44, 0x43, 0x42, 0x41, 0x01, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x05, 0x00, 0x09, 0x00, 0x02, 0x01, 0x40, 0x00 };
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	ClusterRecoveryCTRCReleaseV2 record;
	ClusterRecoveryCTRCWalReferencesV2 out, expected;
	uint8 payload[320];

	UT_ASSERT_EQ(CLUSTER_RECOVERY_CTRC_WAL_REFERENCE_COUNT, 3);
	UT_ASSERT_EQ(CLUSTER_RECOVERY_CTRC_WAL_REFERENCE_KEY_BYTES, 116);
	UT_ASSERT_EQ(CLUSTER_RECOVERY_CTRC_WAL_REFERENCE_EVIDENCE_BYTES, 447);
	UT_ASSERT_EQ(sizeof(out), 795);
	for (unsigned terminal = TT_SLOT_COMMITTED; terminal <= TT_SLOT_ABORTED; terminal++) {
		size_t length;

		record = sample();
		record.terminal_outcome = record.before.status = record.after.status = terminal;
		if (terminal == TT_SLOT_ABORTED)
			record.before.commit_scn = record.after.commit_scn = InvalidScn;
		UT_ASSERT(cluster_recovery_ctrc_encode_v2(&record, SYSTEM_ID, payload, sizeof(payload)));
		length = native_wire(&wire, payload, sizeof(payload), 0);
		UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
		UT_ASSERT(wal_references_expected(reader, 7, raw_source, &wire, length, &expected));
		for (unsigned i = 0; i < 3; i++)
			UT_ASSERT(memcmp(expected.keys[i], identity, sizeof(identity)) == 0);
		UT_ASSERT(cluster_recovery_ctrc_wal_references_v2(reader, 7, raw_source, wire.bytes, length,
														  &out));
		UT_ASSERT(memcmp(&out, &expected, sizeof(out)) == 0);
		XLogReaderFree(reader);
	}
}

UT_TEST(wal_references_physical_fields_not_slot_ordinals)
{
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	ClusterRecoveryCTRCReleaseV2 record = sample();
	ClusterRecoveryCTRCWalReferencesV2 out, expected;
	uint8 payload[320];
	size_t length;

	record.slot_offset = TT_SLOTS_PER_SEGMENT - 1;
	memset(&record.before.first_undo_block, 0, sizeof(UBA));
	record.after.first_undo_block = record.before.first_undo_block;
	UT_ASSERT(cluster_recovery_ctrc_encode_v2(&record, SYSTEM_ID, payload, sizeof(payload)));
	length = native_wire(&wire, payload, sizeof(payload), 0);
	UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
	UT_ASSERT(wal_references_expected(reader, 7, raw_source, &wire, length, &expected));
	UT_ASSERT(
		cluster_recovery_ctrc_wal_references_v2(reader, 7, raw_source, wire.bytes, length, &out));
	UT_ASSERT(memcmp(&out, &expected, sizeof(out)) == 0);
	UT_ASSERT_EQ(out.keys[0][100], 153);
	UT_ASSERT_EQ(out.keys[1][100], 165);
	UT_ASSERT_EQ(out.keys[2][100], 197);
	for (unsigned i = 0; i < 3; i++) {
		UT_ASSERT(all_zero(out.keys[i] + 101, 15));
		UT_ASSERT(memcmp(out.keys[i], out.keys[0], 100) == 0);
	}
	XLogReaderFree(reader);
}

UT_TEST(wal_references_complete_transaction_identity)
{
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	ClusterRecoveryCTRCReleaseV2 record;
	ClusterRecoveryCTRCWalReferencesV2 out, expected;
	uint8 payload[320], first[34];

	for (unsigned field = 0; field < 8; field++) {
		size_t length;

		record = sample();
		memset(&record.before.first_undo_block, 0, sizeof(UBA));
		record.after.first_undo_block = record.before.first_undo_block;
		switch (field) {
		case 1:
			record.database_incarnation = UINT64_MAX;
			break;
		case 2:
			record.origin_node_id = 0;
			record.target_owner = 1;
			record.segment_id = 1;
			break;
		case 3:
			record.segment_id++;
			break;
		case 4:
			record.segment_generation = UINT32_MAX - 1;
			break;
		case 5:
			record.slot_offset = TT_SLOTS_PER_SEGMENT - 1;
			break;
		case 6:
			record.wrap = record.before.wrap = record.after.wrap = TT_WRAP_INVALID - 1;
			break;
		case 7:
			record.xid = record.before.xid = record.after.xid = UINT32_MAX;
			break;
		}
		UT_ASSERT(cluster_recovery_ctrc_encode_v2(&record, SYSTEM_ID, payload, sizeof(payload)));
		length = native_wire(&wire, payload, sizeof(payload), 0);
		UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
		UT_ASSERT(
			wal_references_expected(reader, UINT32_MAX, raw_source, &wire, length, &expected));
		UT_ASSERT(cluster_recovery_ctrc_wal_references_v2(reader, UINT32_MAX, raw_source,
														  wire.bytes, length, &out));
		UT_ASSERT(memcmp(&out, &expected, sizeof(out)) == 0);
		if (field == 0)
			memcpy(first, out.keys[0], sizeof(first));
		else
			UT_ASSERT(memcmp(first, out.keys[0], sizeof(first)) != 0);
		for (unsigned i = 0; i < 3; i++)
			UT_ASSERT(memcmp(out.keys[i], out.keys[0], sizeof(first)) == 0);
		XLogReaderFree(reader);
	}
}

UT_TEST(wal_references_original_source_and_cross_page_positions)
{
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	ClusterRecoveryCTRCWalReferencesV2 out, expected, first;
	uint8 source[32];
	size_t length = native_wire(&wire, golden, sizeof(golden), 0);

	UT_ASSERT(native_read(&pages, &wire, length, true, &reader));
	UT_ASSERT(reader->EndRecPtr > reader->ReadRecPtr + length);
	UT_ASSERT(
		cluster_recovery_ctrc_wal_references_v2(reader, 0, raw_source, wire.bytes, length, &first));
	for (unsigned i = 0; i < 32; i++) {
		memcpy(source, raw_source, sizeof(source));
		source[i] ^= 0x80;
		UT_ASSERT(wal_references_expected(reader, UINT32_MAX, source, &wire, length, &expected));
		UT_ASSERT(cluster_recovery_ctrc_wal_references_v2(reader, UINT32_MAX, source, wire.bytes,
														  length, &out));
		UT_ASSERT(memcmp(&out, &expected, sizeof(out)) == 0);
		UT_ASSERT(memcmp(first.keys[0] + 44, out.keys[0] + 44, 56) != 0);
		UT_ASSERT(memcmp(first.keys[0], out.keys[0], 34) == 0);
	}
	XLogReaderFree(reader);
}

UT_TEST(wal_references_atomic_refusal_and_current_reader)
{
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	DecodedXLogRecord *decoded;
	uint8 zero[32] = { 0 };
	unsigned extras[] = { NATIVE_BLOCK, NATIVE_ORIGIN, NATIVE_TOP_XID, NATIVE_EDGE };
	size_t length = native_wire(&wire, golden, sizeof(golden), 0);
	size_t lengths[] = { 0, SizeOfXLogRecord, length - 1, length + 1, SIZE_MAX };

	UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
	for (unsigned i = 0; i < lengthof(lengths); i++)
		UT_ASSERT(wal_references_refused(reader, raw_source, wire.bytes, lengths[i]));
	UT_ASSERT(wal_references_refused(reader, raw_source, NULL, length));
	UT_ASSERT(wal_references_refused(reader, NULL, wire.bytes, length));
	UT_ASSERT(wal_references_refused(reader, zero, wire.bytes, length));
	UT_ASSERT(wal_references_refused(NULL, raw_source, wire.bytes, length));
	UT_ASSERT(
		!cluster_recovery_ctrc_wal_references_v2(reader, 7, raw_source, wire.bytes, length, NULL));
	decoded = reader->record;
	reader->record = NULL;
	UT_ASSERT(wal_references_refused(reader, raw_source, wire.bytes, length));
	reader->record = decoded;
	decoded->next_lsn++;
	UT_ASSERT(wal_references_refused(reader, raw_source, wire.bytes, length));
	decoded->next_lsn--;
	reader->cluster_expected_thread_id++;
	UT_ASSERT(wal_references_refused(reader, raw_source, wire.bytes, length));
	reader->cluster_expected_thread_id--;
	reader->system_identifier++;
	UT_ASSERT(wal_references_refused(reader, raw_source, wire.bytes, length));
	reader->system_identifier = SYSTEM_ID;
	XLogReaderFree(reader);
	for (unsigned i = 0; i < lengthof(extras); i++) {
		length = native_wire(&wire, golden, sizeof(golden), extras[i]);
		UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
		UT_ASSERT(wal_references_refused(reader, raw_source, wire.bytes, length));
		XLogReaderFree(reader);
	}
}

UT_TEST(wal_references_complete_raw_binding)
{
	NativeRaw wire, changed;
	NativePages pages;
	XLogReaderState *reader;
	size_t length = native_wire(&wire, golden, sizeof(golden), 0);

	UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
	for (size_t i = 0; i < length; i++) {
		changed = wire;
		changed.bytes[i] ^= 1;
		UT_ASSERT(wal_references_refused(reader, raw_source, changed.bytes, length));
	}
	changed = wire;
	changed.align.xl_prev++;
	native_crc(&changed, length);
	UT_ASSERT(wal_references_refused(reader, raw_source, changed.bytes, length));
	changed = wire;
	changed.bytes[37 + 48] ^= 1;
	UT_ASSERT(reseal(changed.bytes + 37, true));
	native_crc(&changed, length);
	UT_ASSERT(wal_references_refused(reader, raw_source, changed.bytes, length));
	XLogReaderFree(reader);
}

UT_TEST(wal_references_unaligned_alias_and_immutable_inputs)
{
	NativeRaw wire, saved_wire;
	NativePages pages, saved_pages;
	XLogReaderState *reader, saved_reader;
	ClusterRecoveryCTRCWalReferencesV2 expected;
	uint8 out[sizeof(expected) + 2], source[sizeof(out)], raw[sizeof(out)];
	size_t length = native_wire(&wire, golden, sizeof(golden), 0);

	UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
	saved_wire = wire;
	saved_pages = pages;
	saved_reader = *reader;
	memset(out, 0xa5, sizeof(out));
	memcpy(source + 1, raw_source, 32);
	memcpy(raw + 1, wire.bytes, length);
	UT_ASSERT(wal_references_expected(reader, 0x12345678, raw_source, &wire, length, &expected));
	UT_ASSERT(
		cluster_recovery_ctrc_wal_references_v2(reader, 0x12345678, source + 1, raw + 1, length,
												(ClusterRecoveryCTRCWalReferencesV2 *)(out + 1)));
	UT_ASSERT(memcmp(out + 1, &expected, sizeof(expected)) == 0);
	UT_ASSERT_EQ(out[0], 0xa5);
	UT_ASSERT_EQ(out[sizeof(out) - 1], 0xa5);
	UT_ASSERT(memcmp(raw + 1, wire.bytes, length) == 0);
	UT_ASSERT(memcmp(source + 1, raw_source, 32) == 0);
	UT_ASSERT(memcmp(&wire, &saved_wire, sizeof(wire)) == 0);
	UT_ASSERT(memcmp(&pages, &saved_pages, sizeof(pages)) == 0);
	UT_ASSERT(memcmp(reader, &saved_reader, sizeof(*reader)) == 0);
	UT_ASSERT(memcmp(XLogRecGetData(reader), golden, sizeof(golden)) == 0);
	UT_ASSERT(
		cluster_recovery_ctrc_wal_references_v2(reader, 0x12345678, source + 1, raw + 1, length,
												(ClusterRecoveryCTRCWalReferencesV2 *)(raw + 1)));
	UT_ASSERT(memcmp(raw + 1, &expected, sizeof(expected)) == 0);
	UT_ASSERT(cluster_recovery_ctrc_wal_references_v2(
		reader, 0x12345678, source + 1, wire.bytes, length,
		(ClusterRecoveryCTRCWalReferencesV2 *)(source + 1)));
	UT_ASSERT(memcmp(source + 1, &expected, sizeof(expected)) == 0);
	XLogReaderFree(reader);
}

/* Controlled allocation leaf only. The graph implementation is the separately
 * compiled production object, with the same sort/hash/traversal in both builds. */
static unsigned graph_alloc_calls, graph_alloc_live, graph_alloc_fail_at;
static bool graph_alloc_tracking;

/* Native-reader allocation and frontend logging must also resolve here;
 * otherwise their archive pulls in duplicate backend allocation symbols. */
void *pg_malloc_extended(size_t size, int flags);

void *
pg_malloc_extended(size_t size, int flags)
{
	void *memory;

	if (size == 0)
		size = 1;
	memory = (flags & MCXT_ALLOC_ZERO) ? calloc(1, size) : malloc(size);
	if (memory == NULL && !(flags & MCXT_ALLOC_NO_OOM))
		abort();
	return memory;
}

void *
palloc(Size size)
{
	return pg_malloc_extended(size, 0);
}

void *
palloc_extended(Size size, int flags)
{
	void *memory;

	/* Earlier native-reader cases also resolve this backend leaf. Preserve
	 * their ZERO request and do not inject/count graph failures there. */
	if (!graph_alloc_tracking)
		return pg_malloc_extended(size, flags);
	if (flags != MCXT_ALLOC_NO_OOM || size == 0 || size > MaxAllocSize)
		abort();
	graph_alloc_calls++;
	if (graph_alloc_calls == graph_alloc_fail_at)
		return NULL;
	memory = malloc(size);
	if (memory != NULL)
		graph_alloc_live++;
	return memory;
}

void
pfree(void *memory)
{
	if (graph_alloc_tracking) {
		if (memory == NULL || graph_alloc_live == 0)
			abort();
		graph_alloc_live--;
	}
	free(memory);
}

typedef struct GraphFixture {
	uint8 keys[4][128];
	uint8 evidence[4][256];
	ClusterRecoveryCTRCGraphNodeV2 nodes[4];
	ClusterRecoveryCTRCGraphEdgeV2 edges[8];
	size_t roots[4];
	ClusterRecoveryCTRCGraphV2 graph;
} GraphFixture;

/* Independent field coding. Synthetic readback/completion bytes exercise only
 * the transcript; they are not qualified native bases or actual owner proofs. */
static uint8 *
graph_uint(uint8 *next, uint64 value, unsigned width)
{
	for (unsigned i = 0; i < width; i++)
		*next++ = value >> (8 * i);
	return next;
}

static uint8 *
graph_bytes(uint8 *next, const uint8 *bytes, size_t length)
{
	next = graph_uint(next, length, 8);
	memcpy(next, bytes, length);
	return next + length;
}

static void
graph_fixture(GraphFixture *f)
{
	uint8 target[34], undo[46], subject[62], atom[240], witness[72], completion[100];
	uint8 *next;
	size_t key_length, atom_length, witness_length;

	memset(f, 0, sizeof(*f));
	next = graph_uint(target, SYSTEM_ID, 8);
	memcpy(next, golden + 40, 8);
	next += 8;
	memcpy(next, golden + 96, 2);
	next += 2;
	memcpy(next, golden + 108, 8);
	next += 8;
	memcpy(next, golden + 102, 4);
	next += 4;
	memcpy(next, golden + 116, 4);
	memcpy(undo, target, sizeof(target));
	next = graph_uint(undo + sizeof(target), UINT64_C(0x0102030405060708), 8);
	graph_uint(next, 0, 4);
	for (size_t i = 0; i < 2; i++) {
		uint8 readback[4] = { 0x80 + i, 0xff, 0, 0x7f };

		memcpy(f->keys[i], target, sizeof(target));
		next = graph_uint(f->keys[i] + sizeof(target), 2, 2);
		next = graph_bytes(next, undo, sizeof(undo));
		next = graph_uint(next, 128 + i, 8);
		next = graph_uint(next, i, 8);
		key_length = next - f->keys[i];
		memcpy(subject, undo, sizeof(undo));
		next = graph_uint(subject + sizeof(undo), 128 + i, 8);
		graph_uint(next, 4, 8);
		next = graph_uint(atom, 2, 2);
		next = graph_bytes(next, subject, sizeof(subject));
		next = graph_bytes(next, readback, sizeof(readback));
		atom_length = next - atom;
		next = graph_uint(f->evidence[i], 1, 8);
		next = graph_bytes(next, atom, atom_length);
		f->nodes[i] = (ClusterRecoveryCTRCGraphNodeV2){ 1,
														{ f->keys[i], key_length },
														{ f->evidence[i], next - f->evidence[i] } };
	}
	next = graph_uint(f->keys[2], 1, 2);
	memcpy(subject, target, sizeof(target));
	graph_uint(graph_uint(subject + sizeof(target), 0, 8), 0, 8);
	next = graph_bytes(next, subject, sizeof(target) + 16);
	key_length = next - f->keys[2];
	memcpy(subject, undo, sizeof(undo));
	graph_uint(graph_uint(subject + sizeof(undo), 128, 8), 4, 8);
	next = graph_uint(witness, 2, 2);
	next = graph_bytes(next, subject, sizeof(subject));
	witness_length = next - witness;
	next = graph_uint(completion, 3, 2);
	next = graph_uint(next, 5, 2);
	next = graph_uint(next, 1, 8);
	next = graph_bytes(next, witness, witness_length);
	atom_length = next - completion;
	next = graph_uint(atom, 3, 2);
	next = graph_bytes(next, f->keys[2], key_length);
	next = graph_bytes(next, completion, atom_length);
	atom_length = next - atom;
	next = graph_uint(f->evidence[2], 1, 8);
	next = graph_bytes(next, atom, atom_length);
	f->nodes[2] = (ClusterRecoveryCTRCGraphNodeV2){ 2,
													{ f->keys[2], key_length },
													{ f->evidence[2], next - f->evidence[2] } };
	f->edges[0] = (ClusterRecoveryCTRCGraphEdgeV2){ 2, 0, 1 };
	f->edges[1] = (ClusterRecoveryCTRCGraphEdgeV2){ 3, 1, 0 };
	f->edges[2] = (ClusterRecoveryCTRCGraphEdgeV2){ 4, 1, 2 };
	f->roots[0] = 0;
	f->graph = (ClusterRecoveryCTRCGraphV2){ f->nodes, 3, f->edges, 3, f->roots, 1 };
}

static const uint8 graph_golden[32]
	= { 0x73, 0x4b, 0x8c, 0x5a, 0xeb, 0x82, 0x76, 0x5c, 0xb4, 0x78, 0x41,
		0x8f, 0x74, 0xe8, 0xc7, 0x6e, 0x05, 0xa5, 0x9e, 0x82, 0xf4, 0x94,
		0x07, 0x18, 0x10, 0xfe, 0xd1, 0x0b, 0xf2, 0x99, 0x06, 0x9a };

static bool
graph_refused(const ClusterRecoveryCTRCReleaseV2 *record, uint64 system,
			  const ClusterRecoveryCTRCGraphV2 *graph)
{
	uint8 guarded[34];
	bool result;

	memset(guarded, 0xa5, sizeof(guarded));
	result = cluster_recovery_ctrc_graph_hash_v2(record, system, graph, guarded + 1);
	return !result && guarded[0] == 0xa5 && guarded[33] == 0xa5 && all_zero(guarded + 1, 32)
		   && graph_alloc_live == 0;
}

UT_TEST(graph_independent_golden_and_immutable)
{
	GraphFixture f, saved;
	ClusterRecoveryCTRCReleaseV2 record = sample(), copy;
	uint8 out[32];

	graph_fixture(&f);
	saved = f;
	copy = record;
	UT_ASSERT(cluster_recovery_ctrc_graph_hash_v2(&record, SYSTEM_ID, &f.graph, out));
	UT_ASSERT(memcmp(out, graph_golden, 32) == 0);
	UT_ASSERT(memcmp(&f, &saved, sizeof(f)) == 0);
	UT_ASSERT(memcmp(&record, &copy, sizeof(record)) == 0);
	UT_ASSERT_EQ(graph_alloc_live, 0);
}

UT_TEST(graph_all_node_edge_permutations)
{
	const unsigned permutations[6][3]
		= { { 0, 1, 2 }, { 0, 2, 1 }, { 1, 0, 2 }, { 1, 2, 0 }, { 2, 0, 1 }, { 2, 1, 0 } };
	GraphFixture f;
	ClusterRecoveryCTRCReleaseV2 record = sample();
	uint8 out[32];

	for (unsigned n = 0; n < 6; n++)
		for (unsigned e = 0; e < 6; e++) {
			ClusterRecoveryCTRCGraphNodeV2 nodes[3];
			ClusterRecoveryCTRCGraphEdgeV2 edges[3];
			size_t inverse[3], root;

			graph_fixture(&f);
			for (unsigned i = 0; i < 3; i++) {
				nodes[i] = f.nodes[permutations[n][i]];
				inverse[permutations[n][i]] = i;
			}
			for (unsigned i = 0; i < 3; i++) {
				edges[i] = f.edges[permutations[e][i]];
				edges[i].from = inverse[edges[i].from];
				edges[i].to = inverse[edges[i].to];
			}
			root = inverse[0];
			f.graph.nodes = nodes;
			f.graph.edges = edges;
			f.graph.roots = &root;
			UT_ASSERT(cluster_recovery_ctrc_graph_hash_v2(&record, SYSTEM_ID, &f.graph, out));
			UT_ASSERT(memcmp(out, graph_golden, 32) == 0);
		}
}

UT_TEST(graph_identical_nodes_edges_collapse)
{
	GraphFixture f;
	ClusterRecoveryCTRCReleaseV2 record = sample();
	uint8 out[32];

	graph_fixture(&f);
	f.nodes[3] = f.nodes[0];
	f.edges[3] = f.edges[0];
	f.edges[3].from = 3;
	f.graph.node_count = f.graph.edge_count = 4;
	f.roots[0] = 3;
	UT_ASSERT(cluster_recovery_ctrc_graph_hash_v2(&record, SYSTEM_ID, &f.graph, out));
	UT_ASSERT(memcmp(out, graph_golden, 32) == 0);
}

UT_TEST(graph_conflicting_node_evidence)
{
	GraphFixture f;
	ClusterRecoveryCTRCReleaseV2 record = sample();

	graph_fixture(&f);
	f.nodes[3] = f.nodes[0];
	memcpy(f.evidence[3], f.nodes[0].evidence.data, f.nodes[0].evidence.length);
	f.nodes[3].evidence.data = f.evidence[3];
	f.evidence[3][f.nodes[3].evidence.length - 1] ^= 1;
	f.graph.node_count = 4;
	UT_ASSERT(graph_refused(&record, SYSTEM_ID, &f.graph));
	f.nodes[3].evidence.length--;
	UT_ASSERT(graph_refused(&record, SYSTEM_ID, &f.graph));
}

UT_TEST(graph_roots_order_unique_and_present)
{
	GraphFixture f;
	ClusterRecoveryCTRCReleaseV2 record = sample();
	uint8 first[32], second[32];

	graph_fixture(&f);
	f.roots[1] = 2;
	f.graph.root_count = 2;
	UT_ASSERT(cluster_recovery_ctrc_graph_hash_v2(&record, SYSTEM_ID, &f.graph, first));
	f.roots[0] = 2;
	f.roots[1] = 0;
	UT_ASSERT(cluster_recovery_ctrc_graph_hash_v2(&record, SYSTEM_ID, &f.graph, second));
	UT_ASSERT(memcmp(first, second, 32) == 0 && memcmp(first, graph_golden, 32) != 0);
	f.roots[1] = 2;
	UT_ASSERT(graph_refused(&record, SYSTEM_ID, &f.graph));
	f.roots[1] = 3;
	UT_ASSERT(graph_refused(&record, SYSTEM_ID, &f.graph));
	f.nodes[3] = f.nodes[2];
	f.graph.node_count = 4;
	UT_ASSERT(graph_refused(&record, SYSTEM_ID, &f.graph));
}

UT_TEST(graph_kind_and_endpoint_domain)
{
	GraphFixture f;
	ClusterRecoveryCTRCReleaseV2 record = sample();
	const uint16 node_kinds[] = { 0, 3, UINT16_MAX };
	const uint16 edge_kinds[] = { 0, 5, UINT16_MAX };

	for (unsigned i = 0; i < lengthof(node_kinds); i++) {
		graph_fixture(&f);
		f.nodes[0].kind = node_kinds[i];
		UT_ASSERT(graph_refused(&record, SYSTEM_ID, &f.graph));
	}
	for (unsigned i = 0; i < lengthof(edge_kinds); i++) {
		graph_fixture(&f);
		f.edges[0].kind = edge_kinds[i];
		UT_ASSERT(graph_refused(&record, SYSTEM_ID, &f.graph));
	}
	graph_fixture(&f);
	f.edges[0].from = 3;
	UT_ASSERT(graph_refused(&record, SYSTEM_ID, &f.graph));
	f.edges[0].from = 0;
	f.edges[0].to = SIZE_MAX;
	UT_ASSERT(graph_refused(&record, SYSTEM_ID, &f.graph));
}

UT_TEST(graph_cycle_reachability_and_owner_required)
{
	GraphFixture f;
	ClusterRecoveryCTRCReleaseV2 record = sample();
	uint8 out[32];

	graph_fixture(&f);
	UT_ASSERT(cluster_recovery_ctrc_graph_hash_v2(&record, SYSTEM_ID, &f.graph, out));
	f.graph.edge_count = 2;
	UT_ASSERT(graph_refused(&record, SYSTEM_ID, &f.graph));
	f.graph.node_count = 2;
	UT_ASSERT(graph_refused(&record, SYSTEM_ID, &f.graph));
	/* A qualified zero-reference owner root needs no reference edge. */
	f.graph.nodes = f.nodes + 2;
	f.graph.node_count = 1;
	f.graph.edge_count = 0;
	f.graph.edges = NULL;
	UT_ASSERT(cluster_recovery_ctrc_graph_hash_v2(&record, SYSTEM_ID, &f.graph, out));
	UT_ASSERT(memcmp(out, graph_golden, 32) != 0);
}

UT_TEST(graph_payload_identity_and_no_self_digest)
{
	GraphFixture f;
	ClusterRecoveryCTRCReleaseV2 record = sample();
	uint8 out[32];

	graph_fixture(&f);
	memset(record.graph_sha256, 0, 32);
	UT_ASSERT(cluster_recovery_ctrc_graph_hash_v2(&record, SYSTEM_ID, &f.graph, out));
	UT_ASSERT(memcmp(out, graph_golden, 32) == 0);
	memset(record.graph_sha256, 0xff, 32);
	UT_ASSERT(cluster_recovery_ctrc_graph_hash_v2(&record, SYSTEM_ID, &f.graph, out));
	UT_ASSERT(memcmp(out, graph_golden, 32) == 0);
	for (unsigned i = 0; i < 32; i++) {
		record = sample();
		record.input_sha256[i] ^= 1;
		UT_ASSERT(cluster_recovery_ctrc_graph_hash_v2(&record, SYSTEM_ID, &f.graph, out));
		UT_ASSERT(memcmp(out, graph_golden, 32) != 0);
	}
	for (unsigned i = 0; i < 5; i++) {
		record = sample();
		if (i == 0)
			record.writer_boot++;
		if (i == 1)
			record.cohort_bitmap ^= 2;
		if (i == 2)
			record.covered_history++;
		if (i == 3)
			record.highest_fpi_end++;
		if (i == 4)
			record.root_generation++;
		UT_ASSERT(cluster_recovery_ctrc_graph_hash_v2(&record, SYSTEM_ID, &f.graph, out));
		UT_ASSERT(memcmp(out, graph_golden, 32) != 0);
	}
	UT_ASSERT(cluster_recovery_ctrc_graph_hash_v2(&record, SYSTEM_ID + 1, &f.graph, out));
	UT_ASSERT(memcmp(out, graph_golden, 32) != 0);
	record.after.flags = 0;
	UT_ASSERT(graph_refused(&record, SYSTEM_ID, &f.graph));
}

UT_TEST(graph_complete_bytes_edges_and_roots_bind)
{
	GraphFixture f;
	ClusterRecoveryCTRCReleaseV2 record = sample();
	uint8 out[32];

	for (unsigned n = 0; n < 3; n++) {
		graph_fixture(&f);
		for (size_t i = 0; i < f.nodes[n].key.length; i++) {
			f.keys[n][i] ^= 1;
			UT_ASSERT(cluster_recovery_ctrc_graph_hash_v2(&record, SYSTEM_ID, &f.graph, out));
			UT_ASSERT(memcmp(out, graph_golden, 32) != 0);
			f.keys[n][i] ^= 1;
		}
		for (size_t i = 0; i < f.nodes[n].evidence.length; i++) {
			f.evidence[n][i] ^= 1;
			UT_ASSERT(cluster_recovery_ctrc_graph_hash_v2(&record, SYSTEM_ID, &f.graph, out));
			UT_ASSERT(memcmp(out, graph_golden, 32) != 0);
			f.evidence[n][i] ^= 1;
		}
	}
	graph_fixture(&f);
	f.edges[0].kind = 1;
	UT_ASSERT(cluster_recovery_ctrc_graph_hash_v2(&record, SYSTEM_ID, &f.graph, out));
	UT_ASSERT(memcmp(out, graph_golden, 32) != 0);
	f.roots[0] = 1;
	UT_ASSERT(cluster_recovery_ctrc_graph_hash_v2(&record, SYSTEM_ID, &f.graph, out));
	UT_ASSERT(memcmp(out, graph_golden, 32) != 0);
}

UT_TEST(graph_null_empty_and_overflow_refusal)
{
	GraphFixture f;
	ClusterRecoveryCTRCReleaseV2 record = sample();

	graph_fixture(&f);
	UT_ASSERT(graph_refused(NULL, SYSTEM_ID, &f.graph));
	UT_ASSERT(graph_refused(&record, 0, &f.graph));
	UT_ASSERT(graph_refused(&record, SYSTEM_ID, NULL));
	UT_ASSERT(!cluster_recovery_ctrc_graph_hash_v2(&record, SYSTEM_ID, &f.graph, NULL));
	f.graph.node_count = 0;
	UT_ASSERT(graph_refused(&record, SYSTEM_ID, &f.graph));
	f.graph.node_count = SIZE_MAX;
	f.graph.nodes = (const void *)1;
	UT_ASSERT(graph_refused(&record, SYSTEM_ID, &f.graph));
	graph_fixture(&f);
	f.graph.edge_count = SIZE_MAX;
	f.graph.edges = (const void *)1;
	UT_ASSERT(graph_refused(&record, SYSTEM_ID, &f.graph));
	graph_fixture(&f);
	f.graph.root_count = SIZE_MAX;
	f.graph.roots = (const void *)1;
	UT_ASSERT(graph_refused(&record, SYSTEM_ID, &f.graph));
	graph_fixture(&f);
	f.graph.root_count = 0;
	UT_ASSERT(graph_refused(&record, SYSTEM_ID, &f.graph));
	graph_fixture(&f);
	f.nodes[0].key.length = SIZE_MAX;
	f.nodes[0].key.data = (const void *)1;
	UT_ASSERT(graph_refused(&record, SYSTEM_ID, &f.graph));
	graph_fixture(&f);
	f.nodes[0].evidence.length = SIZE_MAX;
	f.nodes[0].evidence.data = (const void *)1;
	UT_ASSERT(graph_refused(&record, SYSTEM_ID, &f.graph));
}

UT_TEST(graph_missing_containers_and_byte_views)
{
	GraphFixture f;
	ClusterRecoveryCTRCReleaseV2 record = sample();

	for (unsigned i = 0; i < 7; i++) {
		graph_fixture(&f);
		if (i == 0)
			f.graph.nodes = NULL;
		if (i == 1)
			f.graph.edges = NULL;
		if (i == 2)
			f.graph.roots = NULL;
		if (i == 3)
			f.nodes[0].key.data = NULL;
		if (i == 4)
			f.nodes[0].key.length = 0;
		if (i == 5)
			f.nodes[0].evidence.data = NULL;
		if (i == 6)
			f.nodes[0].evidence.length = 0;
		UT_ASSERT(graph_refused(&record, SYSTEM_ID, &f.graph));
	}
}

UT_TEST(graph_unaligned_byte_views_and_output_aliases)
{
	GraphFixture f;
	ClusterRecoveryCTRCReleaseV2 record = sample();
	uint8 keys[3][129], evidence[3][257], out[33];

	graph_fixture(&f);
	for (unsigned i = 0; i < 3; i++) {
		memcpy(keys[i] + 1, f.nodes[i].key.data, f.nodes[i].key.length);
		memcpy(evidence[i] + 1, f.nodes[i].evidence.data, f.nodes[i].evidence.length);
		f.nodes[i].key.data = keys[i] + 1;
		f.nodes[i].evidence.data = evidence[i] + 1;
	}
	UT_ASSERT(cluster_recovery_ctrc_graph_hash_v2(&record, SYSTEM_ID, &f.graph, out + 1));
	UT_ASSERT(memcmp(out + 1, graph_golden, 32) == 0);
	UT_ASSERT(cluster_recovery_ctrc_graph_hash_v2(&record, SYSTEM_ID, &f.graph, evidence[0] + 1));
	UT_ASSERT(memcmp(evidence[0] + 1, graph_golden, 32) == 0);
	graph_fixture(&f);
	UT_ASSERT(
		cluster_recovery_ctrc_graph_hash_v2(&record, SYSTEM_ID, &f.graph, record.graph_sha256));
	UT_ASSERT(memcmp(record.graph_sha256, graph_golden, 32) == 0);
	UT_ASSERT(cluster_recovery_ctrc_graph_hash_v2(&record, SYSTEM_ID, &f.graph, (uint8 *)&f.graph));
	UT_ASSERT(memcmp(&f.graph, graph_golden, 32) == 0);
}

UT_TEST(graph_allocation_failures_leave_no_scratch)
{
	GraphFixture f;
	ClusterRecoveryCTRCReleaseV2 record = sample();
	uint8 out[32];
	unsigned allocations;

	graph_fixture(&f);
	graph_alloc_calls = 0;
	UT_ASSERT(cluster_recovery_ctrc_graph_hash_v2(&record, SYSTEM_ID, &f.graph, out));
	allocations = graph_alloc_calls;
	UT_ASSERT(allocations > 0);
	UT_ASSERT_EQ(graph_alloc_live, 0);
	for (unsigned i = 1; i <= allocations; i++) {
		graph_alloc_calls = 0;
		graph_alloc_fail_at = i;
		UT_ASSERT(graph_refused(&record, SYSTEM_ID, &f.graph));
	}
	graph_alloc_fail_at = 0;
	UT_ASSERT(cluster_recovery_ctrc_graph_hash_v2(&record, SYSTEM_ID, &f.graph, out));
	UT_ASSERT_EQ(graph_alloc_live, 0);
	UT_ASSERT(memcmp(out, graph_golden, 32) == 0);
}

UT_TEST(graph_full_unsigned_encoded_byte_order)
{
	/* Opaque structural views, deliberately not producer-qualified keys. */
	const uint8 keys[4][2] = { { 0x80 }, { 0x7f }, { 0x80, 0 }, { 0 } };
	const uint8 evidence = 1;
	const uint8 expected[32] = { 0x9c, 0x39, 0xd1, 0x4f, 0x1f, 0x70, 0xcf, 0x1c, 0xd8, 0x3b, 0x4d,
								 0xc9, 0x25, 0xe7, 0x8f, 0xc6, 0x0a, 0xf8, 0x7a, 0x58, 0xc8, 0xec,
								 0x21, 0xe2, 0x52, 0xec, 0x1d, 0xed, 0x81, 0x46, 0x68, 0xa4 };
	ClusterRecoveryCTRCGraphNodeV2 nodes[4];
	const ClusterRecoveryCTRCGraphEdgeV2 edges[] = { { 1, 0, 1 }, { 1, 0, 2 }, { 1, 0, 3 } };
	const size_t root = 0;
	ClusterRecoveryCTRCGraphV2 graph = { nodes, 4, edges, 3, &root, 1 };
	ClusterRecoveryCTRCReleaseV2 record = sample();
	uint8 out[32];

	for (unsigned i = 0; i < 4; i++)
		nodes[i] = (ClusterRecoveryCTRCGraphNodeV2){ i == 3 ? 2 : 1,
													 { keys[i], i == 2 ? 2 : 1 },
													 { &evidence, 1 } };
	UT_ASSERT(cluster_recovery_ctrc_graph_hash_v2(&record, SYSTEM_ID, &graph, out));
	UT_ASSERT(memcmp(out, expected, 32) == 0);
}


/* Independent BIND fixture: original native reader/decoder, literal frozen
 * transcript offsets. This inventories only the payload xid field. */
static xl_undo_tt_slot_bind
bind_sample(void)
{
	xl_undo_tt_slot_bind bind = { 0 };

	bind.segment_id = 2 * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	bind.segment_generation = 17;
	bind.xid = 0x12345678;
	bind.slot_offset = 5;
	bind.wrap = 9;
	bind.instance = 3;
	bind.format_version = 1;
	return bind;
}

static size_t
bind_wire(NativeRaw *wire, const xl_undo_tt_slot_bind *bind)
{
	memset(wire, 0, sizeof(*wire));
	wire->align.xl_rmid = RM_CLUSTER_UNDO_ID;
	wire->align.xl_info = XLOG_UNDO_TT_SLOT_BIND;
	wire->align.xl_scn = 41;
	wire->bytes[32] = XLR_BLOCK_ID_DATA_SHORT;
	wire->bytes[33] = 24;
	memcpy(wire->bytes + 34, bind, 24);
	native_crc(wire, 58);
	return 58;
}

static bool
bind_expected(const XLogReaderState *reader, uint64 dbinc, uint32 index,
			  const uint8 *source, const NativeRaw *wire, ClusterRecoveryCTRCBindReferenceV2 *out)
{
	xl_undo_tt_slot_bind bind;
	uint8 *key = out->key, *atom = out->evidence + 16;

	memcpy(&bind, wire->bytes + 34, 24);
	memset(out, 0, sizeof(*out));
	wal_atom_uint(key, SYSTEM_ID, 8);
	wal_atom_uint(key + 8, dbinc, 8);
	wal_atom_uint(key + 16, bind.instance, 2);
	wal_atom_uint(key + 18, bind.segment_id, 4);
	wal_atom_uint(key + 22, bind.segment_generation, 4);
	wal_atom_uint(key + 26, bind.slot_offset, 2);
	wal_atom_uint(key + 28, bind.wrap, 2);
	wal_atom_uint(key + 30, bind.xid, 4);
	wal_atom_uint(key + 34, 3, 2);
	wal_atom_uint(key + 36, 56, 8);
	wal_atom_uint(key + 44, index, 4);
	wal_atom_uint(key + 48, reader->ReadRecPtr, 8);
	wal_atom_uint(key + 56, reader->EndRecPtr, 8);
	wal_atom_uint(key + 64, 58, 4);
	if (!reference_digest(source, reader->ReadRecPtr, reader->EndRecPtr, wire, 58, key + 68))
		return false;
	wal_atom_uint(key + 100, 42, 8);
	wal_atom_uint(out->evidence, 1, 8);
	wal_atom_uint(out->evidence + 8, 132, 8);
	wal_atom_uint(atom, 1, 2);
	wal_atom_uint(atom + 2, 56, 8);
	memcpy(atom + 10, key + 44, 56);
	wal_atom_uint(atom + 66, 58, 8);
	memcpy(atom + 74, wire->bytes, 58);
	return true;
}

static bool
bind_refused(XLogReaderState *reader, uint64 dbinc, const uint8 *source,
			 const uint8 *raw, size_t length)
{
	uint8 guarded[sizeof(ClusterRecoveryCTRCBindReferenceV2) + 2];

	memset(guarded, 0xa5, sizeof(guarded));
	return !cluster_recovery_ctrc_bind_reference_v2(reader, dbinc, 7, source, raw, length,
								 (ClusterRecoveryCTRCBindReferenceV2 *)(guarded + 1))
		&& all_zero(guarded + 1, sizeof(guarded) - 2)
		&& guarded[0] == 0xa5 && guarded[sizeof(guarded) - 1] == 0xa5;
}

UT_TEST(bind_canonical_reference)
{
	xl_undo_tt_slot_bind bind = bind_sample();
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	ClusterRecoveryCTRCBindReferenceV2 out, expected;

	UT_ASSERT_EQ(sizeof(out), 264);
	UT_ASSERT_EQ(bind_wire(&wire, &bind), 58);
	UT_ASSERT(native_read(&pages, &wire, 58, false, &reader));
	UT_ASSERT(bind_expected(reader, 99, 7, raw_source, &wire, &expected));
	UT_ASSERT(cluster_recovery_ctrc_bind_reference_v2(reader, 99, 7, raw_source,
													wire.bytes, 58, &out));
	UT_ASSERT(memcmp(&out, &expected, sizeof(out)) == 0);
	UT_ASSERT_EQ(out.key[30], 0x78);
	UT_ASSERT_EQ(out.key[100], 42);
	UT_ASSERT(all_zero(out.key + 108, 8));
	XLogReaderFree(reader);
}

UT_TEST(bind_cross_page_actual_interval)
{
	xl_undo_tt_slot_bind bind = bind_sample();
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	ClusterRecoveryCTRCBindReferenceV2 out, expected;

	bind_wire(&wire, &bind);
	UT_ASSERT(native_read(&pages, &wire, 58, true, &reader));
	UT_ASSERT(pages.reads >= 2);
	UT_ASSERT(reader->EndRecPtr > reader->ReadRecPtr + 58);
	UT_ASSERT(bind_expected(reader, 99, UINT32_MAX, raw_source, &wire, &expected));
	UT_ASSERT(cluster_recovery_ctrc_bind_reference_v2(reader, 99, UINT32_MAX,
													raw_source, wire.bytes, 58, &out));
	UT_ASSERT(memcmp(&out, &expected, sizeof(out)) == 0);
	XLogReaderFree(reader);
}

UT_TEST(bind_source_and_root_domains)
{
	xl_undo_tt_slot_bind bind = bind_sample();
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	ClusterRecoveryCTRCBindReferenceV2 base, out, expected;
	uint8 source[32];

	bind_wire(&wire, &bind);
	UT_ASSERT(native_read(&pages, &wire, 58, false, &reader));
	UT_ASSERT(cluster_recovery_ctrc_bind_reference_v2(reader, 99, 0, raw_source,
													wire.bytes, 58, &base));
	memcpy(source, raw_source, 32);
	source[31] ^= 0x80;
	UT_ASSERT(bind_expected(reader, UINT64_MAX, UINT32_MAX, source, &wire, &expected));
	UT_ASSERT(cluster_recovery_ctrc_bind_reference_v2(reader, UINT64_MAX, UINT32_MAX,
													source, wire.bytes, 58, &out));
	UT_ASSERT(memcmp(&out, &expected, sizeof(out)) == 0);
	UT_ASSERT(memcmp(base.key + 68, out.key + 68, 32) != 0);
	UT_ASSERT(bind_refused(reader, 0, raw_source, wire.bytes, 58));
	UT_ASSERT(bind_refused(reader, 99, NULL, wire.bytes, 58));
	memset(source, 0, 32);
	UT_ASSERT(bind_refused(reader, 99, source, wire.bytes, 58));
	XLogReaderFree(reader);
}

UT_TEST(bind_native_field_refusals)
{
	for (unsigned i = 0; i < 14; i++) {
		xl_undo_tt_slot_bind bind = bind_sample();
		NativeRaw wire;
		NativePages pages;
		XLogReaderState *reader;

		switch (i) {
		case 0: bind.instance = 0; break;
		case 1: bind.instance = 129; break;
		case 2: bind.instance = 2; break;
		case 3: bind.segment_id = 0; break;
		case 4: bind.segment_id = 1; break;
		case 5: bind.slot_offset = TT_SLOTS_PER_SEGMENT; break;
		case 6: bind.wrap = TT_WRAP_INVALID; break;
		case 7: bind.xid = InvalidTransactionId; break;
		case 8: bind.xid = FrozenTransactionId; break;
		case 9: bind.format_version = 2; break;
		case 10: bind.flags = 1; break;
		case 11: bind.reserved8 = 1; break;
		case 12: bind.reserved32 = 1; break;
		case 13: bind.segment_generation = UINT32_MAX; break;
		}
		bind_wire(&wire, &bind);
		UT_ASSERT(native_read(&pages, &wire, 58, false, &reader));
		UT_ASSERT(bind_refused(reader, 99, raw_source, wire.bytes, 58));
		XLogReaderFree(reader);
	}
}

UT_TEST(bind_complete_raw_binding)
{
	xl_undo_tt_slot_bind bind = bind_sample();
	NativeRaw wire, changed;
	NativePages pages;
	XLogReaderState *reader;

	bind_wire(&wire, &bind);
	UT_ASSERT(native_read(&pages, &wire, 58, false, &reader));
	for (unsigned i = 0; i < 58; i++) {
		changed = wire;
		changed.bytes[i] ^= 1;
		UT_ASSERT(bind_refused(reader, 99, raw_source, changed.bytes, 58));
	}
	changed = wire;
	changed.align.xl_scn++;
	native_crc(&changed, 58);
	UT_ASSERT(bind_refused(reader, 99, raw_source, changed.bytes, 58));
	changed = wire;
	changed.bytes[42] ^= 1;
	native_crc(&changed, 58);
	UT_ASSERT(bind_refused(reader, 99, raw_source, changed.bytes, 58));
	/* Even matching decoded bytes cannot substitute for final raw CRC. */
	reader->record->header = changed.align;
	memcpy(XLogRecGetData(reader), changed.bytes + 34, 24);
	changed.bytes[58 - 1] ^= 1;
	memcpy(XLogRecGetData(reader), changed.bytes + 34, 24);
	UT_ASSERT(bind_refused(reader, 99, raw_source, changed.bytes, 58));
	XLogReaderFree(reader);
}

UT_TEST(bind_current_reader_and_bounds)
{
	xl_undo_tt_slot_bind bind = bind_sample();
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader, saved;

	bind_wire(&wire, &bind);
	UT_ASSERT(native_read(&pages, &wire, 58, false, &reader));
	UT_ASSERT(bind_refused(NULL, 99, raw_source, wire.bytes, 58));
	UT_ASSERT(bind_refused(reader, 99, raw_source, NULL, 58));
	for (size_t len = 0; len < 58; len++)
		UT_ASSERT(bind_refused(reader, 99, raw_source, wire.bytes, len));
	UT_ASSERT(bind_refused(reader, 99, raw_source, wire.bytes, 59));
	UT_ASSERT(bind_refused(reader, 99, raw_source, wire.bytes, SIZE_MAX));
	UT_ASSERT(!cluster_recovery_ctrc_bind_reference_v2(reader, 99, 7, raw_source,
													 wire.bytes, 58, NULL));
	saved = *reader;
	reader->record = NULL;
	UT_ASSERT(bind_refused(reader, 99, raw_source, wire.bytes, 58));
	*reader = saved;
	reader->system_identifier = 0;
	UT_ASSERT(bind_refused(reader, 99, raw_source, wire.bytes, 58));
	*reader = saved;
	reader->ReadRecPtr++;
	UT_ASSERT(bind_refused(reader, 99, raw_source, wire.bytes, 58));
	*reader = saved;
	reader->EndRecPtr++;
	UT_ASSERT(bind_refused(reader, 99, raw_source, wire.bytes, 58));
	*reader = saved;
	reader->cluster_expected_thread_id = 2;
	UT_ASSERT(bind_refused(reader, 99, raw_source, wire.bytes, 58));
	*reader = saved;
	reader->cluster_expected_thread_id = 0;
	UT_ASSERT(bind_refused(reader, 99, raw_source, wire.bytes, 58));
	*reader = saved;
	XLogReaderFree(reader);
}

UT_TEST(bind_exact_native_frame)
{
	xl_undo_tt_slot_bind bind = bind_sample();
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;

	for (unsigned extra = 0; extra <= NATIVE_EDGE; extra = extra ? extra * 2 : 1) {
		size_t length = native_wire(&wire, (const uint8 *)&bind, 24, extra);

		wire.align.xl_info = XLOG_UNDO_TT_SLOT_BIND;
		native_crc(&wire, length);
		UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
		UT_ASSERT(bind_refused(reader, 99, raw_source, wire.bytes, length));
		XLogReaderFree(reader);
	}
	for (unsigned i = 0; i < 3; i++) {
		bind_wire(&wire, &bind);
		if (i == 0) wire.align.xl_rmid = RM_XLOG_ID;
		if (i == 1) wire.align.xl_info = XLOG_UNDO_TT_SLOT_COMMIT;
		if (i == 2) wire.align.xl_info |= XLR_SPECIAL_REL_UPDATE;
		native_crc(&wire, 58);
		UT_ASSERT(native_read(&pages, &wire, 58, false, &reader));
		UT_ASSERT(bind_refused(reader, 99, raw_source, wire.bytes, 58));
		XLogReaderFree(reader);
	}
}

UT_TEST(bind_immutable_unaligned_and_aliases)
{
	xl_undo_tt_slot_bind bind = bind_sample();
	NativeRaw wire;
	NativePages pages, saved_pages;
	XLogReaderState *reader, saved_reader;
	ClusterRecoveryCTRCBindReferenceV2 expected;
	uint8 buffer[sizeof(expected) + 2];

	bind_wire(&wire, &bind);
	UT_ASSERT(native_read(&pages, &wire, 58, false, &reader));
	UT_ASSERT(bind_expected(reader, 99, 7, raw_source, &wire, &expected));
	saved_pages = pages;
	saved_reader = *reader;
	memset(buffer, 0xa5, sizeof(buffer));
	memcpy(buffer + 1, wire.bytes, 58);
	UT_ASSERT(cluster_recovery_ctrc_bind_reference_v2(reader, 99, 7, raw_source,
				buffer + 1, 58, (ClusterRecoveryCTRCBindReferenceV2 *)(buffer + 1)));
	UT_ASSERT(memcmp(buffer + 1, &expected, sizeof(expected)) == 0);
	UT_ASSERT(buffer[0] == 0xa5 && buffer[sizeof(buffer) - 1] == 0xa5);
	memcpy(buffer + 1, raw_source, 32);
	UT_ASSERT(cluster_recovery_ctrc_bind_reference_v2(reader, 99, 7, buffer + 1,
				wire.bytes, 58, (ClusterRecoveryCTRCBindReferenceV2 *)(buffer + 1)));
	UT_ASSERT(memcmp(buffer + 1, &expected, sizeof(expected)) == 0);
	UT_ASSERT(memcmp(&pages, &saved_pages, sizeof(pages)) == 0);
	UT_ASSERT(memcmp(reader, &saved_reader, sizeof(*reader)) == 0);
	UT_ASSERT(memcmp(XLogRecGetData(reader), &bind, 24) == 0);
	XLogReaderFree(reader);
}

UT_TEST(bind_native_boundary_identities)
{
	for (unsigned i = 0; i < 2; i++) {
		xl_undo_tt_slot_bind bind = bind_sample();
		NativeRaw wire;
		NativePages pages;
		XLogReaderState *reader;
		ClusterRecoveryCTRCBindReferenceV2 out, expected;

		bind.segment_generation = i ? UINT32_MAX - 1 : 0;
		bind.slot_offset = i ? TT_SLOTS_PER_SEGMENT - 1 : 0;
		bind.wrap = i ? TT_WRAP_INVALID - 1 : 0;
		bind.xid = i ? MaxTransactionId : FirstNormalTransactionId;
		bind_wire(&wire, &bind);
		UT_ASSERT(native_read(&pages, &wire, 58, false, &reader));
		UT_ASSERT(bind_expected(reader, 1, 0, raw_source, &wire, &expected));
		UT_ASSERT(cluster_recovery_ctrc_bind_reference_v2(reader, 1, 0, raw_source,
													 wire.bytes, 58, &out));
		UT_ASSERT(memcmp(&out, &expected, sizeof(out)) == 0);
		XLogReaderFree(reader);
	}
}

UT_TEST(bind_header_xid_is_evidence_not_inventory)
{
	xl_undo_tt_slot_bind bind = bind_sample();
	NativeRaw wire;
	NativePages pages;
	XLogReaderState *reader;
	ClusterRecoveryCTRCBindReferenceV2 out, expected;

	bind_wire(&wire, &bind);
	wire.align.xl_xid = bind.xid + 1;
	native_crc(&wire, 58);
	UT_ASSERT(native_read(&pages, &wire, 58, false, &reader));
	UT_ASSERT(bind_expected(reader, 99, 7, raw_source, &wire, &expected));
	UT_ASSERT(cluster_recovery_ctrc_bind_reference_v2(reader, 99, 7, raw_source,
													wire.bytes, 58, &out));
	UT_ASSERT(memcmp(&out, &expected, sizeof(out)) == 0);
	UT_ASSERT_EQ(out.key[30], 0x78);
	UT_ASSERT_EQ(out.key[100], 42);
	UT_ASSERT(memcmp(out.evidence + 16 + 74, wire.bytes, 58) == 0);
	XLogReaderFree(reader);
}


/* Generation-bearing original TT payloads share one singleton field shape.
 * Expected bytes use the frozen transcript, not the production encoder. */
static size_t
tt_wire(NativeRaw *wire, unsigned kind, const xl_undo_tt_slot_bind *identity)
{
	size_t length = 58;

	memset(wire, 0, sizeof(*wire));
	wire->align.xl_rmid = RM_CLUSTER_UNDO_ID;
	wire->align.xl_scn = 41;
	wire->align.xl_xid = identity->xid + 1;
	wire->bytes[32] = XLR_BLOCK_ID_DATA_SHORT;
	wire->bytes[33] = 24;
	if (kind == 0) {
		wire->align.xl_info = XLOG_UNDO_TT_SLOT_BIND;
		memcpy(wire->bytes + 34, identity, 24);
	} else if (kind == 1) {
		xl_undo_tt_slot_abort_exact abort = { 0 };

		abort.segment_id = identity->segment_id;
		abort.segment_generation = identity->segment_generation;
		abort.xid = identity->xid;
		abort.slot_offset = identity->slot_offset;
		abort.wrap = identity->wrap;
		abort.instance = identity->instance;
		abort.format_version = CLUSTER_UNDO_TT_ABORT_EXACT_VERSION;
		wire->align.xl_info = XLOG_UNDO_TT_SLOT_ABORT;
		memcpy(wire->bytes + 34, &abort, 24);
	} else {
		xl_undo_tt_slot_ctrc_release_v1 release = { 0 };

		release.segment_id = identity->segment_id;
		release.segment_generation = identity->segment_generation;
		release.xid = identity->xid;
		release.cluster_epoch = 7;
		release.root_id = 11;
		release.root_generation = 13;
		release.formation_epoch = 17;
		release.admission_record_generation = 19;
		release.seal_generation = 23;
		release.touched_nodes_low = 5;
		release.ack_set_digest[0] = 0x80;
		release.ack_set_digest[15] = 7;
		release.slot_offset = identity->slot_offset;
		release.slot_wrap = identity->wrap;
		release.owner_instance = identity->instance;
		release.terminal_status = kind == 2 ? TT_SLOT_COMMITTED : TT_SLOT_ABORTED;
		release.format_version = CLUSTER_UNDO_TT_CTRC_RELEASE_VERSION;
		release.flags = CLUSTER_UNDO_TT_CTRC_RELEASE_ALL_TOUCHED_ACKED;
		if (!cluster_undo_tt_ctrc_release_encode(&release, wire->bytes + 34))
			return 0;
		wire->align.xl_info = XLOG_UNDO_TT_SLOT_CTRC_RELEASE;
		wire->bytes[33] = 96;
		length = 130;
	}
	native_crc(wire, length);
	return length;
}

static bool
tt_expected(const XLogReaderState *reader, uint64 dbinc, uint32 index,
			const uint8 *source, const NativeRaw *wire, size_t length,
			const xl_undo_tt_slot_bind *identity, ClusterRecoveryCTRCTTReferenceV2 *out)
{
	uint8 *key = out->key, *atom = out->evidence + 16;

	memset(out, 0, sizeof(*out));
	wal_atom_uint(key, SYSTEM_ID, 8);
	wal_atom_uint(key + 8, dbinc, 8);
	wal_atom_uint(key + 16, identity->instance, 2);
	wal_atom_uint(key + 18, identity->segment_id, 4);
	wal_atom_uint(key + 22, identity->segment_generation, 4);
	wal_atom_uint(key + 26, identity->slot_offset, 2);
	wal_atom_uint(key + 28, identity->wrap, 2);
	wal_atom_uint(key + 30, identity->xid, 4);
	wal_atom_uint(key + 34, 3, 2);
	wal_atom_uint(key + 36, 56, 8);
	wal_atom_uint(key + 44, index, 4);
	wal_atom_uint(key + 48, reader->ReadRecPtr, 8);
	wal_atom_uint(key + 56, reader->EndRecPtr, 8);
	wal_atom_uint(key + 64, length, 4);
	if (!reference_digest(source, reader->ReadRecPtr, reader->EndRecPtr, wire, length, key + 68))
		return false;
	wal_atom_uint(key + 100, 42, 8);
	wal_atom_uint(out->evidence, 1, 8);
	wal_atom_uint(out->evidence + 8, 74 + length, 8);
	wal_atom_uint(atom, 1, 2);
	wal_atom_uint(atom + 2, 56, 8);
	memcpy(atom + 10, key + 44, 56);
	wal_atom_uint(atom + 66, length, 8);
	memcpy(atom + 74, wire->bytes, length);
	out->evidence_length = 90 + length;
	return true;
}

static bool
tt_refused(XLogReaderState *reader, uint64 dbinc, const uint8 *source,
		   const uint8 *raw, size_t length)
{
	uint8 guarded[sizeof(ClusterRecoveryCTRCTTReferenceV2) + 2];

	memset(guarded, 0xa5, sizeof(guarded));
	return !cluster_recovery_ctrc_tt_reference_v2(reader, dbinc, 7, source, raw, length,
								 (ClusterRecoveryCTRCTTReferenceV2 *)(guarded + 1))
		&& all_zero(guarded + 1, sizeof(guarded) - 2)
		&& guarded[0] == 0xa5 && guarded[sizeof(guarded) - 1] == 0xa5;
}

UT_TEST(tt_canonical_generation_bearing_payloads)
{
	for (unsigned kind = 0; kind < 4; kind++) {
		xl_undo_tt_slot_bind identity = bind_sample();
		NativeRaw wire;
		NativePages pages;
		XLogReaderState *reader;
		ClusterRecoveryCTRCTTReferenceV2 out, expected;
		size_t length = tt_wire(&wire, kind, &identity);

		UT_ASSERT_EQ(length, kind < 2 ? 58 : 130);
		UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
		UT_ASSERT(tt_expected(reader, 99, 7, raw_source, &wire, length, &identity, &expected));
		UT_ASSERT(cluster_recovery_ctrc_tt_reference_v2(reader, 99, 7, raw_source,
												   wire.bytes, length, &out));
		UT_ASSERT(memcmp(&out, &expected, sizeof(out)) == 0);
		UT_ASSERT_EQ(out.evidence_length, kind < 2 ? 148 : 220);
		UT_ASSERT_EQ(out.key[30], 0x78);
		UT_ASSERT_EQ(out.key[100], 42);
		UT_ASSERT(all_zero(out.key + 108, 8));
		XLogReaderFree(reader);
	}
}

UT_TEST(tt_actual_interval_and_source_domains)
{
	for (unsigned kind = 1; kind < 3; kind++) {
		xl_undo_tt_slot_bind identity = bind_sample();
		NativeRaw wire;
		NativePages pages;
		XLogReaderState *reader;
		ClusterRecoveryCTRCTTReferenceV2 out, base, expected;
		uint8 source[32];
		size_t length = tt_wire(&wire, kind, &identity);

		UT_ASSERT(native_read(&pages, &wire, length, true, &reader));
		UT_ASSERT(pages.reads >= 2);
		UT_ASSERT(reader->EndRecPtr > reader->ReadRecPtr + length);
		UT_ASSERT(cluster_recovery_ctrc_tt_reference_v2(reader, 99, 0, raw_source,
												   wire.bytes, length, &base));
		memcpy(source, raw_source, 32);
		source[31] ^= 0x80;
		UT_ASSERT(tt_expected(reader, UINT64_MAX, UINT32_MAX, source, &wire, length,
							  &identity, &expected));
		UT_ASSERT(cluster_recovery_ctrc_tt_reference_v2(reader, UINT64_MAX, UINT32_MAX,
												   source, wire.bytes, length, &out));
		UT_ASSERT(memcmp(&out, &expected, sizeof(out)) == 0);
		UT_ASSERT(memcmp(base.key + 68, out.key + 68, 32) != 0);
		XLogReaderFree(reader);
	}
}

UT_TEST(tt_original_payload_validation)
{
	/* Mutate original field encodings, then supply a valid full WAL CRC. */
	for (unsigned kind = 1; kind < 3; kind++) {
		const unsigned common_offsets[] = { 0, 4, 8 };
		const unsigned abort_offsets[] = { 12, 14, 16, 17, 18, 20 };
		const unsigned release_offsets[] = { 12, 16, 24, 32, 40, 48, 64, 72,
											88, 90, 92, 93, 94, 95 };
		unsigned count = kind == 1 ? 9 : 17;

		for (unsigned i = 0; i < count; i++) {
			xl_undo_tt_slot_bind identity = bind_sample();
			NativeRaw wire;
			NativePages pages;
			XLogReaderState *reader;
			size_t length = tt_wire(&wire, kind, &identity);
			uint8 *payload = wire.bytes + 34;
			unsigned offset = i < 3 ? common_offsets[i]
				: kind == 1 ? abort_offsets[i - 3] : release_offsets[i - 3];

			if (i < 3) memset(payload + offset, i == 1 ? 0xff : 0, 4);
			else if (offset == (kind == 1 ? 12 : 88)) memset(payload + offset, 0xff, 2);
			else if (offset == (kind == 1 ? 14 : 90)) memset(payload + offset, 0xff, 2);
			else if (kind == 1) payload[offset] = offset == 18 || offset == 20 ? 1 : 0;
			else if (offset == 64) payload[offset] = 1;
			else if (offset == 72) memset(payload + offset, 0, 16);
			else memset(payload + offset, 0, offset <= 48 ? (offset == 12 ? 4 : 8) : 1);
			native_crc(&wire, length);
			UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
			UT_ASSERT(tt_refused(reader, 99, raw_source, wire.bytes, length));
			XLogReaderFree(reader);
		}
	}
}

UT_TEST(tt_records_without_generation_are_not_inferred)
{
	const uint8 opcodes[] = { XLOG_UNDO_TT_SLOT_ABORT, XLOG_UNDO_TT_SLOT_COMMIT,
							 XLOG_UNDO_TT_SLOT_SET_HEAD };
	const size_t lengths[] = { 16, 24, 32 };
	xl_undo_tt_slot_bind identity = bind_sample();
	xl_undo_tt_slot_abort legacy = { 0 };
	xl_undo_tt_slot_commit commit = { 0 };
	xl_undo_tt_slot_set_head head = { 0 };
	const void *payloads[] = { &legacy, &commit, &head };

	legacy.segment_id = identity.segment_id;
	legacy.slot_offset = identity.slot_offset;
	legacy.wrap = identity.wrap;
	legacy.xid = identity.xid;
	legacy.instance = identity.instance;
	memcpy(&commit, &legacy, sizeof(legacy));
	commit.commit_scn = 41;
	memcpy(&head, &legacy, sizeof(legacy));
	head.first_undo_block.raw[0] = identity.segment_id | (UINT64_C(9) << 32);
	head.first_undo_block.raw[1] = identity.slot_offset;

	for (unsigned i = 0; i < 4; i++) {
		NativeRaw wire;
		NativePages pages;
		XLogReaderState *reader;
		ClusterUndoDecoded decoded;
		size_t length;

		if (i < 3) {
			memset(&wire, 0, sizeof(wire));
			wire.align.xl_rmid = RM_CLUSTER_UNDO_ID;
			wire.align.xl_info = opcodes[i];
			wire.align.xl_scn = 41;
			wire.bytes[32] = XLR_BLOCK_ID_DATA_SHORT;
			wire.bytes[33] = lengths[i];
			memcpy(wire.bytes + 34, payloads[i], lengths[i]);
			length = 34 + lengths[i];
			native_crc(&wire, length);
		} else length = native_wire(&wire, golden, sizeof(golden), 0);
		UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
		if (i < 3) {
			UT_ASSERT(cluster_undo_decode(reader, &decoded));
			UT_ASSERT(cluster_undo_inspect_pi(&decoded));
		}
		UT_ASSERT(tt_refused(reader, 99, raw_source, wire.bytes, length));
		XLogReaderFree(reader);
	}
}

UT_TEST(tt_complete_raw_binding)
{
	for (unsigned kind = 1; kind < 3; kind++) {
		xl_undo_tt_slot_bind identity = bind_sample();
		NativeRaw wire, changed;
		NativePages pages;
		XLogReaderState *reader;
		size_t length = tt_wire(&wire, kind, &identity);

		UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
		for (size_t i = 0; i < length; i++) {
			changed = wire;
			changed.bytes[i] ^= 1;
			UT_ASSERT(tt_refused(reader, 99, raw_source, changed.bytes, length));
		}
		changed = wire;
		changed.align.xl_scn++;
		native_crc(&changed, length);
		UT_ASSERT(tt_refused(reader, 99, raw_source, changed.bytes, length));
		changed = wire;
		changed.bytes[42] ^= 1;
		native_crc(&changed, length);
		UT_ASSERT(tt_refused(reader, 99, raw_source, changed.bytes, length));
		/* Matching decoded bytes still require the complete raw CRC. */
		changed.bytes[42] ^= 2;
		reader->record->header = changed.align;
		memcpy(XLogRecGetData(reader), changed.bytes + 34, length - 34);
		UT_ASSERT(tt_refused(reader, 99, raw_source, changed.bytes, length));
		XLogReaderFree(reader);
	}
}

UT_TEST(tt_current_reader_bounds_and_frame)
{
	for (unsigned kind = 1; kind < 3; kind++) {
		xl_undo_tt_slot_bind identity = bind_sample();
		NativeRaw wire;
		NativePages pages;
		XLogReaderState *reader, saved;
		size_t length = tt_wire(&wire, kind, &identity);
		uint8 source[32] = { 0 };

		UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
		UT_ASSERT(tt_refused(NULL, 99, raw_source, wire.bytes, length));
		UT_ASSERT(tt_refused(reader, 0, raw_source, wire.bytes, length));
		UT_ASSERT(tt_refused(reader, 99, NULL, wire.bytes, length));
		UT_ASSERT(tt_refused(reader, 99, source, wire.bytes, length));
		UT_ASSERT(tt_refused(reader, 99, raw_source, NULL, length));
		for (size_t len = 0; len < length; len++)
			UT_ASSERT(tt_refused(reader, 99, raw_source, wire.bytes, len));
		UT_ASSERT(tt_refused(reader, 99, raw_source, wire.bytes, length + 1));
		UT_ASSERT(tt_refused(reader, 99, raw_source, wire.bytes, SIZE_MAX));
		UT_ASSERT(!cluster_recovery_ctrc_tt_reference_v2(reader, 99, 7, raw_source,
													wire.bytes, length, NULL));
		saved = *reader;
		for (unsigned i = 0; i < 7; i++) {
			*reader = saved;
			if (i == 0) reader->record = NULL;
			if (i == 1) reader->system_identifier = 0;
			if (i == 2) reader->ReadRecPtr++;
			if (i == 3) reader->EndRecPtr++;
			if (i == 4) reader->cluster_expected_thread_id = 2;
			if (i == 5) reader->cluster_expected_thread_id = 0;
			if (i == 6) reader->cluster_expected_thread_id = 129;
			UT_ASSERT(tt_refused(reader, 99, raw_source, wire.bytes, length));
		}
		*reader = saved;
		XLogReaderFree(reader);
		for (unsigned extra = NATIVE_BLOCK; extra <= NATIVE_EDGE; extra *= 2) {
			NativeRaw extra_wire;
			size_t extra_len = native_wire(&extra_wire, wire.bytes + 34, length - 34, extra);

			extra_wire.align.xl_info = wire.align.xl_info;
			native_crc(&extra_wire, extra_len);
			UT_ASSERT(native_read(&pages, &extra_wire, extra_len, false, &reader));
			UT_ASSERT(tt_refused(reader, 99, raw_source, extra_wire.bytes, extra_len));
			XLogReaderFree(reader);
		}
		wire.align.xl_info |= XLR_SPECIAL_REL_UPDATE;
		native_crc(&wire, length);
		UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
		UT_ASSERT(tt_refused(reader, 99, raw_source, wire.bytes, length));
		XLogReaderFree(reader);
	}
}

UT_TEST(tt_unaligned_aliases_and_immutable_inputs)
{
	for (unsigned kind = 1; kind < 3; kind++) {
		xl_undo_tt_slot_bind identity = bind_sample();
		NativeRaw wire, saved_wire;
		NativePages pages, saved_pages;
		XLogReaderState *reader, saved_reader;
		ClusterRecoveryCTRCTTReferenceV2 expected;
		uint8 buffer[sizeof(expected) + 2];
		size_t length = tt_wire(&wire, kind, &identity);

		UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
		UT_ASSERT(tt_expected(reader, 99, 7, raw_source, &wire, length, &identity, &expected));
		saved_wire = wire;
		saved_pages = pages;
		saved_reader = *reader;
		memset(buffer, 0xa5, sizeof(buffer));
		memcpy(buffer + 1, wire.bytes, length);
		UT_ASSERT(cluster_recovery_ctrc_tt_reference_v2(reader, 99, 7, raw_source,
			buffer + 1, length, (ClusterRecoveryCTRCTTReferenceV2 *)(buffer + 1)));
		UT_ASSERT(memcmp(buffer + 1, &expected, sizeof(expected)) == 0);
		UT_ASSERT(buffer[0] == 0xa5 && buffer[sizeof(buffer) - 1] == 0xa5);
		memcpy(buffer + 1, raw_source, 32);
		UT_ASSERT(cluster_recovery_ctrc_tt_reference_v2(reader, 99, 7, buffer + 1,
			wire.bytes, length, (ClusterRecoveryCTRCTTReferenceV2 *)(buffer + 1)));
		UT_ASSERT(memcmp(buffer + 1, &expected, sizeof(expected)) == 0);
		UT_ASSERT(buffer[0] == 0xa5 && buffer[sizeof(buffer) - 1] == 0xa5);
		UT_ASSERT(memcmp(&wire, &saved_wire, sizeof(wire)) == 0);
		UT_ASSERT(memcmp(&pages, &saved_pages, sizeof(pages)) == 0);
		UT_ASSERT(memcmp(reader, &saved_reader, sizeof(*reader)) == 0);
		UT_ASSERT(memcmp(XLogRecGetData(reader), wire.bytes + 34, length - 34) == 0);
		XLogReaderFree(reader);
	}
}

UT_TEST(tt_bind_api_remains_bind_only)
{
	for (unsigned kind = 0; kind < 3; kind++) {
		xl_undo_tt_slot_bind identity = bind_sample();
		NativeRaw wire;
		NativePages pages;
		XLogReaderState *reader;
		ClusterRecoveryCTRCTTReferenceV2 out;
		ClusterRecoveryCTRCBindReferenceV2 bind;
		size_t length = tt_wire(&wire, kind, &identity);

		UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
		UT_ASSERT(cluster_recovery_ctrc_tt_reference_v2(reader, 99, 7, raw_source,
												   wire.bytes, length, &out));
		if (kind == 0) {
			UT_ASSERT(cluster_recovery_ctrc_bind_reference_v2(reader, 99, 7, raw_source,
													 wire.bytes, length, &bind));
			UT_ASSERT(memcmp(bind.key, out.key, sizeof(bind.key)) == 0);
			UT_ASSERT(memcmp(bind.evidence, out.evidence, sizeof(bind.evidence)) == 0);
		} else UT_ASSERT(bind_refused(reader, 99, raw_source, wire.bytes, length));
		XLogReaderFree(reader);
	}
}

UT_TEST(tt_native_boundary_identities)
{
	for (unsigned kind = 1; kind < 3; kind++) {
		for (unsigned boundary = 0; boundary < 2; boundary++) {
			xl_undo_tt_slot_bind identity = bind_sample();
			NativeRaw wire;
			NativePages pages;
			XLogReaderState *reader;
			ClusterRecoveryCTRCTTReferenceV2 out, expected;
			size_t length;

			identity.segment_generation = boundary ? UINT32_MAX - 1 : 0;
			identity.slot_offset = boundary ? TT_SLOTS_PER_SEGMENT - 1 : 0;
			identity.wrap = boundary ? TT_WRAP_INVALID - 1 : 0;
			identity.xid = boundary ? MaxTransactionId : FirstNormalTransactionId;
			length = tt_wire(&wire, kind, &identity);
			UT_ASSERT(native_read(&pages, &wire, length, false, &reader));
			UT_ASSERT(tt_expected(reader, 1, 0, raw_source, &wire, length, &identity, &expected));
			UT_ASSERT(cluster_recovery_ctrc_tt_reference_v2(reader, 1, 0, raw_source,
													wire.bytes, length, &out));
			UT_ASSERT(memcmp(&out, &expected, sizeof(out)) == 0);
			XLogReaderFree(reader);
		}
	}
}

int
main(void)
{
	UT_PLAN(117);
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
	UT_RUN(target_terminal_images);
	UT_RUN(target_native_identity);
	UT_RUN(target_generation_and_wrap_are_exact);
	UT_RUN(target_complete_slot_images);
	UT_RUN(target_only_selected_slot);
	UT_RUN(target_native_integrity_required);
	UT_RUN(target_payload_and_external_identity);
	UT_RUN(target_exact_lengths_and_nulls);
	UT_RUN(target_unaligned_and_immutable);
	UT_RUN(target_legal_native_boundaries);
	UT_RUN(image_terminal_pages);
	UT_RUN(image_requires_exact_before);
	UT_RUN(image_native_refusal);
	UT_RUN(image_payload_binding);
	UT_RUN(image_exact_bounds_and_nulls);
	UT_RUN(image_unaligned_and_aliasing);
	UT_RUN(image_native_boundaries);
	UT_RUN(reader_terminal_payloads_and_immutability);
	UT_RUN(reader_cross_page_actual_positions);
	UT_RUN(reader_exact_rmgr_and_info);
	UT_RUN(reader_standalone_native_frame);
	UT_RUN(reader_extra_block_and_page_edge);
	UT_RUN(reader_exact_payload_and_codec);
	UT_RUN(reader_system_and_selected_thread);
	UT_RUN(reader_requires_current_completed_record);
	UT_RUN(reader_native_crc_refusal_is_not_completion);
	UT_RUN(reference_terminal_records);
	UT_RUN(reference_cross_page_actual_interval);
	UT_RUN(reference_complete_source_domain);
	UT_RUN(reference_exact_raw_bounds);
	UT_RUN(reference_valid_crc_header_mismatch);
	UT_RUN(reference_valid_payload_mismatch);
	UT_RUN(reference_exact_standalone_frame);
	UT_RUN(reference_requires_current_reader);
	UT_RUN(reference_unaligned_immutable_and_alias);
	UT_RUN(reference_each_raw_bit_refused);
	UT_RUN(wal_atom_canonical_terminal_records);
	UT_RUN(wal_atom_cross_page_positions);
	UT_RUN(wal_atom_source_domains);
	UT_RUN(wal_atom_exact_bounds_and_nulls);
	UT_RUN(wal_atom_current_reader_and_frame);
	UT_RUN(wal_atom_complete_raw_binding);
	UT_RUN(wal_atom_unaligned_immutable_and_alias);
	UT_RUN(wal_references_canonical_terminal_records);
	UT_RUN(wal_references_physical_fields_not_slot_ordinals);
	UT_RUN(wal_references_complete_transaction_identity);
	UT_RUN(wal_references_original_source_and_cross_page_positions);
	UT_RUN(wal_references_atomic_refusal_and_current_reader);
	UT_RUN(wal_references_complete_raw_binding);
	UT_RUN(wal_references_unaligned_alias_and_immutable_inputs);
	UT_RUN(bind_canonical_reference);
	UT_RUN(bind_cross_page_actual_interval);
	UT_RUN(bind_source_and_root_domains);
	UT_RUN(bind_native_field_refusals);
	UT_RUN(bind_complete_raw_binding);
	UT_RUN(bind_current_reader_and_bounds);
	UT_RUN(bind_exact_native_frame);
	UT_RUN(bind_immutable_unaligned_and_aliases);
	UT_RUN(bind_native_boundary_identities);
	UT_RUN(bind_header_xid_is_evidence_not_inventory);
	UT_RUN(tt_canonical_generation_bearing_payloads);
	UT_RUN(tt_actual_interval_and_source_domains);
	UT_RUN(tt_original_payload_validation);
	UT_RUN(tt_records_without_generation_are_not_inferred);
	UT_RUN(tt_complete_raw_binding);
	UT_RUN(tt_current_reader_bounds_and_frame);
	UT_RUN(tt_unaligned_aliases_and_immutable_inputs);
	UT_RUN(tt_bind_api_remains_bind_only);
	UT_RUN(tt_native_boundary_identities);
	graph_alloc_tracking = true;
	UT_RUN(graph_independent_golden_and_immutable);
	UT_RUN(graph_all_node_edge_permutations);
	UT_RUN(graph_identical_nodes_edges_collapse);
	UT_RUN(graph_conflicting_node_evidence);
	UT_RUN(graph_roots_order_unique_and_present);
	UT_RUN(graph_kind_and_endpoint_domain);
	UT_RUN(graph_cycle_reachability_and_owner_required);
	UT_RUN(graph_payload_identity_and_no_self_digest);
	UT_RUN(graph_complete_bytes_edges_and_roots_bind);
	UT_RUN(graph_null_empty_and_overflow_refusal);
	UT_RUN(graph_missing_containers_and_byte_views);
	UT_RUN(graph_unaligned_byte_views_and_output_aliases);
	UT_RUN(graph_allocation_failures_leave_no_scratch);
	UT_RUN(graph_full_unsigned_encoded_byte_order);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
