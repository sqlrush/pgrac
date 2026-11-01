/* Independent survivor wire vectors; parse success never means service proof.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "cluster/cluster_semantic_survivor.h"
#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

#ifdef USE_ASSERT_CHECKING
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
 (void)condition; (void)file; (void)line;
 abort();
}
#endif
static void put(uint8 *p, uint64 value, unsigned count)
{
	for (unsigned i = 0; i < count; i++) { p[i] = value & 255; value >>= 8; }
}
static uint32 crc(const uint8 *p, Size n)
{
	uint32 value = UINT32_MAX;
	for (Size i = 0; i < n; i++) {
		value ^= i >= 12 && i < 16 ? 0 : p[i];
		for (unsigned b = 0; b < 8; b++)
			value = (value >> 1) ^ ((value & 1) ? UINT32_C(0x82f63b78) : 0);
	}
	return value ^ UINT32_MAX;
}
static Size vector(uint8 *p)
{
	const Size n = 192 + 1536 + 80 + 3 * 48;
	uint8 *basis = p + 192, *row = basis + 1536 + 80;
	memset(p, 0, n);
	put(p, 4, 2); put(p + 2, 1, 2); put(p + 4, 1, 2); put(p + 6, 1, 2);
	put(p + 8, n, 4);
	for (unsigned i = 0; i < 6; i++) put(p + 16 + i * 8, 6 + i, 8);
	put(p + 64, 4, 4); put(p + 68, 4, 4);
	put(p + 72, 18, 8); put(p + 80, UINT64_C(1) << 63, 8);
	memset(p + 88, 0x11, 32); memset(p + 120, 0x22, 32); memset(p + 152, 0x33, 32);
	put(p + 188, 1, 2);
	memset(basis, 0xa5, 512); memset(basis + 512, 0xb6, 512);
	memset(basis + 1104, 0x33, 32);
	put(basis + 1136, 1, 8); put(basis + 1144, 8, 8);
	put(basis + 1152, 9, 8); put(basis + 1160, 10, 8); put(basis + 1168, 6, 8);
	put(basis + 1176, 18, 8); put(basis + 1184, UINT64_C(1) << 63, 8);
	put(basis + 1192, 1, 8);
	for (unsigned i = 0; i < 3; i++, row += 48) {
		put(row, i == 0 ? 1 : i == 1 ? 4 : 127, 4); put(row + 4, 0x71b000, 4);
		for (unsigned f = 0; f < 5; f++) put(row + 8 + 8 * f, 100 + i + f, 8);
	}
	put(p + 12, crc(p, n), 4);
	return n;
}
UT_TEST(independent_manifest_is_a_borrowed_syntax_view)
{
	uint8 bytes[7952], format[32];
	ClusterSurvivorMessageV4 out = { 0 };
	Size n = vector(bytes);
	memset(format, 0x33, sizeof(format));
	UT_ASSERT(cluster_semantic_survivor_v4_decode(bytes, n, format, &out));
	UT_ASSERT_EQ(out.body_bytes, n - 192);
	UT_ASSERT(out.body == bytes + 192);
	UT_ASSERT_EQ(out.survivors[1], UINT64_C(1) << 63);
	UT_ASSERT_EQ(out.coordinator, 4);
}

static void
refresh(uint8 *bytes, Size n)
{
	put(bytes + 8, n, 4); put(bytes + 12, crc(bytes, n), 4);
}
static void
reject(const uint8 *bytes, Size n)
{
	uint8 format[32];
	ClusterSurvivorMessageV4 out, before;
	memset(format, 0x33, 32); memset(&out, 0xa5, sizeof(out)); before = out;
	UT_ASSERT(!cluster_semantic_survivor_v4_decode(bytes, n, format, &out));
	UT_ASSERT_EQ(memcmp(&out, &before, sizeof(out)), 0);
}
static void
roundtrip(const uint8 *bytes, Size n)
{
	uint8 format[32], output[10240];
	ClusterSurvivorMessageV4 out = { 0 };
	Size written = 0;
	memset(format, 0x33, 32);
	UT_ASSERT(cluster_semantic_survivor_v4_decode(bytes, n, format, &out));
	UT_ASSERT(cluster_semantic_survivor_v4_encode(&out, format, output, sizeof(output), &written));
	UT_ASSERT_EQ(written, n); UT_ASSERT_EQ(memcmp(bytes, output, n), 0);
}
static Size
full_vector(uint8 *p, unsigned count)
{
	uint8 sparse[7952];
	Size n = 192 + 1536 + 80 + count * 48;
	uint64 lo = count >= 64 ? UINT64_MAX : (UINT64_C(1) << count) - 1;
	uint64 hi = count <= 64 ? 0 : count == 128 ? UINT64_MAX : (UINT64_C(1) << (count - 64)) - 1;
	(void)vector(sparse); memset(p, 0, n); memcpy(p, sparse, 192 + 1536 + 80);
	put(p + 64, count - 1, 4); put(p + 68, count - 1, 4);
	put(p + 72, lo, 8); put(p + 80, hi, 8);
	put(p + 192 + 1176, lo, 8); put(p + 192 + 1184, hi, 8);
	put(p + 192 + 1192, 0, 8);
	for (unsigned node = 0; node < count; node++) {
		uint8 *row = p + 192 + 1536 + 80 + 48 * node;
		put(row, node, 4); put(row + 4, 0x71b000, 4);
		for (unsigned i = 0; i < 5; i++) put(row + 8 + 8 * i, 200 + node + i, 8);
	}
	refresh(p, n); return n;
}
UT_TEST(manifest_counts_cover_both_bitmap_halves_and_capacity)
{
	const unsigned counts[] = { 1, 2, 63, 64, 65, 127, 128 };
	uint8 bytes[7952];
	for (unsigned i = 0; i < lengthof(counts); i++) {
		Size n = full_vector(bytes, counts[i]); roundtrip(bytes, n);
		UT_ASSERT(n <= 7952);
	}
}
UT_TEST(all_truncations_and_oversize_leave_output_unchanged)
{
	uint8 bytes[10241]; Size n = vector(bytes);
	for (Size cut = 0; cut < n; cut++) reject(bytes, cut);
	memset(bytes + n, 0, sizeof(bytes) - n);
	reject(bytes, n + 1); reject(bytes, 10240); reject(bytes, 10241);
	reject(NULL, 192);
}
UT_TEST(header_and_format_contradictions_reject_even_with_correct_crc)
{
	uint8 bytes[7952], bad[7952], wrong[32];
	ClusterSurvivorMessageV4 out = { 0 };
	Size n = vector(bytes);
	for (unsigned fault = 0; fault < 29; fault++) {
		memcpy(bad, bytes, n);
		switch (fault) {
		case 0: put(bad, 3, 2); break;
		case 1: put(bad + 2, 0, 2); break;
		case 2: put(bad + 2, 4, 2); break;
		case 3: put(bad + 4, 0, 2); break;
		case 4: put(bad + 4, 3, 2); break;
		case 5: put(bad + 6, 0, 2); break;
		case 6: put(bad + 6, 5, 2); break;
		case 7: put(bad + 190, 1, 2); break;
		case 8: put(bad + 16, 0, 8); break;
		case 9: put(bad + 16, UINT64_MAX, 8); break;
		case 10: put(bad + 24, 0, 8); break;
		case 11: put(bad + 32, 0, 8); break;
		case 12: put(bad + 40, 0, 8); break;
		case 13: put(bad + 48, 0, 8); break;
		case 14: put(bad + 64, 128, 4); break;
		case 15: put(bad + 68, 128, 4); break;
		case 16: put(bad + 64, 2, 4); break;
		case 17: put(bad + 68, 1, 4); break;
		case 18: memset(bad + 72, 0, 16); break;
		case 19: memset(bad + 88, 0, 32); break;
		case 20: memset(bad + 120, 0, 32); break;
		case 21: memset(bad + 152, 0, 32); break;
		case 22: bad[152] ^= 1; break;
		case 23: put(bad + 184, 1, 2); break;
		case 24: put(bad + 186, 1, 2); break;
		case 25: put(bad + 188, 0, 2); break;
		case 26: put(bad + 188, 3, 2); break;
		case 27: put(bad, 2, 2); break;
		case 28: put(bad, 1, 2); break;
		}
		refresh(bad, n); reject(bad, n);
	}
	memset(wrong, 0x34, 32);
	UT_ASSERT(!cluster_semantic_survivor_v4_decode(bytes, n, wrong, &out));
	memset(wrong, 0, 32);
	UT_ASSERT(!cluster_semantic_survivor_v4_decode(bytes, n, wrong, &out));
	UT_ASSERT(!cluster_semantic_survivor_v4_decode(bytes, n, NULL, &out));
}
UT_TEST(immutable_basis_and_complete_identity_rows_are_required)
{
	uint8 bytes[7952], bad[7952]; Size n = vector(bytes);
	const unsigned offsets[] = { 0, 512, 1104, 1136, 1144, 1152, 1160, 1168,
		1176, 1184, 1192, 1208, 1240, 1272, 1304, 1535 };
	for (unsigned i = 0; i < lengthof(offsets); i++) {
		memcpy(bad, bytes, n);
		if (offsets[i] == 0 || offsets[i] == 512) memset(bad + 192 + offsets[i], 0, 512);
		else if (offsets[i] == 1136 || offsets[i] == 1160) put(bad + 192 + offsets[i], 0, 8);
		else if (offsets[i] == 1192) put(bad + 192 + 1192, 2, 8); /* S/F overlap. */
		else bad[192 + offsets[i]] ^= 1;
		refresh(bad, n); reject(bad, n);
	}
	for (unsigned row = 0; row < 3; row++)
		for (unsigned field = 0; field < 7; field++) {
			uint8 *at;
			memcpy(bad, bytes, n); at = bad + 192 + 1616 + 48 * row;
			if (field == 0) put(at, 128, 4);
			else if (field == 1) put(at + 4, 0, 4);
			else put(at + 8 + (field - 2) * 8, 0, 8);
			refresh(bad, n); reject(bad, n);
		}
}
UT_TEST(ack_receipt_bytes_are_syntax_only_and_keep_all_identity_fields)
{
	uint8 manifest[7952], bytes[272], bad[272];
	(void)vector(manifest);
	for (unsigned stage = 1; stage <= 4; stage++) {
		memcpy(bytes, manifest, 192); put(bytes + 4, 2, 2); put(bytes + 6, stage, 2);
		put(bytes + 68, 127, 4); put(bytes + 188, 2, 2);
		memcpy(bytes + 192, manifest + 192 + 1616 + 2 * 48, 48);
		memset(bytes + 240, 0x5a, 32); refresh(bytes, sizeof(bytes));
		roundtrip(bytes, sizeof(bytes));
		memcpy(bad, bytes, sizeof(bytes)); put(bad + 192, 4, 4);
		refresh(bad, sizeof(bad)); reject(bad, sizeof(bad));
		memcpy(bad, bytes, sizeof(bytes)); memset(bad + 240, 0, 32);
		refresh(bad, sizeof(bad)); reject(bad, sizeof(bad));
	}
}
static Size
open_chunk(uint8 *bytes, unsigned start)
{
	uint8 all[7952]; Size n = 192 + 1616 + 64 * 80;
	(void)full_vector(all, 128); memcpy(bytes, all, 192 + 1616);
	put(bytes + 6, 4, 2); put(bytes + 184, start, 2); put(bytes + 186, 64, 2);
	put(bytes + 188, 3, 2); memset(bytes + 192 + 1208, 0x44, 96);
	for (unsigned i = 0; i < 64; i++) {
		memcpy(bytes + 192 + 1616 + i * 80, all + 192 + 1616 + (start + i) * 48, 48);
		memset(bytes + 192 + 1616 + i * 80 + 48, 0x55, 32);
	}
	refresh(bytes, n); return n;
}
UT_TEST(open_chunks_have_exact_member_intervals_without_implying_completion)
{
	uint8 bytes[7952], bad[7952];
	for (unsigned start = 0; start <= 64; start += 64) {
		Size n = open_chunk(bytes, start); roundtrip(bytes, n);
		for (unsigned fault = 0; fault < 6; fault++) {
			memcpy(bad, bytes, n);
			if (fault == 0) put(bad + 186, 0, 2);
			if (fault == 1) put(bad + 186, 65, 2);
			if (fault == 2) put(bad + 184, 128, 2);
			if (fault == 3) put(bad + 184, 127, 2);
			if (fault == 4) put(bad + 192 + 1616, 128, 4);
			if (fault == 5) memset(bad + 192 + 1616 + 48, 0, 32);
			refresh(bad, n); reject(bad, n);
		}
	}
}
UT_TEST(executor_rows_preserve_signed_process_shape_without_granting_claims)
{
	uint8 manifest[7952], bytes[288], bad[288];
	(void)vector(manifest);
	for (unsigned purpose = 2; purpose <= 3; purpose++)
		for (unsigned kind = 1; kind <= 2; kind++) {
			uint8 *row = bytes + 192;
			memcpy(bytes, manifest, 192); memset(row, 0, 96);
			put(bytes + 2, purpose, 2); put(bytes + 4, kind, 2); put(bytes + 188, 4, 2);
			put(row, 127, 4); put(row + 4, 7, 4); put(row + 8, 17, 8);
			put(row + 16, 4321, 4); put(row + 20, 1, 2); put(row + 22, 1, 2);
			put(row + 24, 123, 8); put(row + 32, 124, 8); memset(row + 40, 0x66, 32);
			put(row + 72, 125, 8); put(row + 88, 126, 8); refresh(bytes, sizeof(bytes));
			roundtrip(bytes, sizeof(bytes));
			for (unsigned fault = 0; fault < 10; fault++) {
				memcpy(bad, bytes, sizeof(bytes)); row = bad + 192;
				switch (fault) {
				case 0: put(row, 128, 4); break;
				case 1: put(row + 4, UINT32_MAX, 4); break;
				case 2: put(row + 8, 0, 8); break;
				case 3: put(row + 16, 0, 4); break;
				case 4: put(row + 16, UINT32_MAX, 4); break;
				case 5: put(row + 24, 0, 8); break;
				case 6: put(row + 32, 0, 8); break;
				case 7: memset(row + 40, 0, 32); break;
				case 8: put(row + 72, 0, 8); break;
				case 9: put(row + 88, 0, 8); break;
				}
				refresh(bad, sizeof(bad)); reject(bad, sizeof(bad));
			}
		}
}
UT_TEST(crc_covers_the_whole_header_and_body)
{
	uint8 bytes[7952], bad[7952]; Size n = vector(bytes);
	UT_ASSERT_EQ(crc((const uint8 *)"123456789", 9), UINT32_C(0xe3069283));
	for (Size i = 0; i < n; i++) {
		memcpy(bad, bytes, n); bad[i] ^= 1; reject(bad, n);
	}
}
UT_TEST(encoder_refusal_preserves_all_output_and_supports_in_place_body)
{
	uint8 bytes[7952], output[7952], before[7952], format[32];
	ClusterSurvivorMessageV4 m = { 0 }; Size n = vector(bytes), written;
	memset(format, 0x33, 32);
	UT_ASSERT(cluster_semantic_survivor_v4_decode(bytes, n, format, &m));
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterSurvivorMessageV4 bad = m;
		memset(output, 0xa5, sizeof(output)); memcpy(before, output, sizeof(output)); written = 123;
		if (fault == 1) bad.nonce = 0;
		if (fault == 2) bad.body_bytes = SIZE_MAX;
		if (fault == 3) bad.body = NULL;
		UT_ASSERT(!cluster_semantic_survivor_v4_encode(&bad, format, output, fault ? sizeof(output) : n - 1, &written));
		UT_ASSERT_EQ(written, 123); UT_ASSERT_EQ(memcmp(output, before, sizeof(output)), 0);
	}
	written = 0;
	UT_ASSERT(cluster_semantic_survivor_v4_encode(&m, format, bytes, sizeof(bytes), &written));
	UT_ASSERT_EQ(written, n); roundtrip(bytes, n);
	UT_ASSERT(!cluster_semantic_survivor_v4_encode(NULL, format, output, sizeof(output), &written));
	UT_ASSERT(!cluster_semantic_survivor_v4_encode(&m, format, NULL, sizeof(output), &written));
	UT_ASSERT(!cluster_semantic_survivor_v4_encode(&m, format, output, sizeof(output), NULL));
	UT_ASSERT(!cluster_semantic_survivor_v4_decode(bytes, n, format, NULL));
}
int main(void)
{
	UT_PLAN(10);
	UT_RUN(independent_manifest_is_a_borrowed_syntax_view);
	UT_RUN(manifest_counts_cover_both_bitmap_halves_and_capacity);
	UT_RUN(all_truncations_and_oversize_leave_output_unchanged);
	UT_RUN(header_and_format_contradictions_reject_even_with_correct_crc);
	UT_RUN(immutable_basis_and_complete_identity_rows_are_required);
	UT_RUN(ack_receipt_bytes_are_syntax_only_and_keep_all_identity_fields);
	UT_RUN(open_chunks_have_exact_member_intervals_without_implying_completion);
	UT_RUN(executor_rows_preserve_signed_process_shape_without_granting_claims);
	UT_RUN(crc_covers_the_whole_header_and_body);
	UT_RUN(encoder_refusal_preserves_all_output_and_supports_in_place_body);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
