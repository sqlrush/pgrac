/* Fixed native window codec, not a live owner/base qualification. */
#include "postgres.h"
#include "cluster/cluster_relmap_checkpoint.h"
#include "unit_test.h"
#include "port/pg_crc32c.h"
UT_DEFINE_GLOBALS();
static CheckPoint cp;
static ClusterControlRootIdentity id;
static ClusterRelmapCheckpointWindow w;
static void
setup(void)
{
	memset(&cp, 0, sizeof(cp));
	memset(&id, 0, sizeof(id));
	memset(&w, 0, sizeof(w));
	cp.redo = 100;
	cp.ThisTimeLineID = 7;
	cp.PrevTimeLineID = 6;
	cp.nextXid = FullTransactionIdFromU64(UINT64CONST(0x100000104));
	id.system_identifier = 9;
	id.origin_node_id = 2;
	id.origin_owner_incarnation = 12;
	w.system_identifier = 9;
	w.database_incarnation = 11;
	w.epoch = 13;
	w.admitted_incarnation = 12;
	w.boot_incarnation = 14;
	w.origin = 2;
	w.native_floor = UINT64CONST(0xfffffffe);
	w.last_issued = UINT64CONST(0x100000102);
	w.allocation_next = UINT64CONST(0x100000103);
}
UT_TEST(roundtrip_binds_native_checkpoint)
{
	uint8 b[160];
	ClusterRelmapCheckpointWindow out;
	setup();
	UT_ASSERT(cluster_relmap_checkpoint_window_encode_v1(&w, &cp, 200, 240, b));
	UT_ASSERT(cluster_relmap_checkpoint_window_decode_v1(b, &cp, 200, 240, &id, 11, &out));
	UT_ASSERT_EQ(out.native_floor, w.native_floor);
	UT_ASSERT_EQ(out.last_issued, w.last_issued);
	UT_ASSERT_EQ(out.boot_incarnation, 14);
	UT_ASSERT_EQ(out.admitted_incarnation, 12);
	UT_ASSERT_EQ(out.allocation_next, w.allocation_next);
}
UT_TEST(rejects_corruption_and_all_zero_legacy)
{
	uint8 b[160], bad[160];
	ClusterRelmapCheckpointWindow out, zero = { 0 };
	setup();
	UT_ASSERT(cluster_relmap_checkpoint_window_encode_v1(&w, &cp, 200, 240, b));
	for (size_t i = 0; i < sizeof(b); i++) {
		memcpy(bad, b, 160);
		bad[i] ^= 1;
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(!cluster_relmap_checkpoint_window_decode_v1(bad, &cp, 200, 240, &id, 11, &out));
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	}
	memset(bad, 0, 160);
	UT_ASSERT(!cluster_relmap_checkpoint_window_decode_v1(bad, &cp, 200, 240, &id, 11, &out));
}
UT_TEST(rejects_wrong_source_position_and_complete_body)
{
	uint8 b[160];
	ClusterRelmapCheckpointWindow out;
	setup();
	UT_ASSERT(cluster_relmap_checkpoint_window_encode_v1(&w, &cp, 200, 240, b));
	UT_ASSERT(!cluster_relmap_checkpoint_window_decode_v1(b, &cp, 201, 240, &id, 11, &out));
	UT_ASSERT(!cluster_relmap_checkpoint_window_decode_v1(b, &cp, 200, 241, &id, 11, &out));
	UT_ASSERT(!cluster_relmap_checkpoint_window_decode_v1(b, &cp, 200, 240, &id, 12, &out));
	id.origin_owner_incarnation++;
	UT_ASSERT(!cluster_relmap_checkpoint_window_decode_v1(b, &cp, 200, 240, &id, 11, &out));
	id.origin_owner_incarnation--;
	cp.oldestMulti++;
	UT_ASSERT(!cluster_relmap_checkpoint_window_decode_v1(b, &cp, 200, 240, &id, 11, &out));
}
UT_TEST(rejects_guessed_issuance_and_halfspace)
{
	uint8 b[160], zero[160] = { 0 };
	setup();
	w.last_issued = w.allocation_next;
	UT_ASSERT(!cluster_relmap_checkpoint_window_encode_v1(&w, &cp, 200, 240, b));
	UT_ASSERT(memcmp(b, zero, 160) == 0);
	setup();
	w.native_floor = w.last_issued - UINT64CONST(0x80000000);
	UT_ASSERT(!cluster_relmap_checkpoint_window_encode_v1(&w, &cp, 200, 240, b));
	setup();
	w.allocation_next = U64FromFullTransactionId(cp.nextXid) + 1;
	UT_ASSERT(!cluster_relmap_checkpoint_window_encode_v1(&w, &cp, 200, 240, b));
	setup();
	UT_ASSERT(!cluster_relmap_checkpoint_window_encode_v1(&w, &cp, 200, 200, b));
}
UT_TEST(overlap_preserves_input_and_null_refuses)
{
	union {
		uint8 b[512];
		ClusterRelmapCheckpointWindow w;
	} u;
	uint8 saved[512];
	setup();
	memset(&u, 0, sizeof(u));
	u.w = w;
	memcpy(saved, &u, sizeof(u));
	UT_ASSERT(!cluster_relmap_checkpoint_window_encode_v1(&u.w, &cp, 200, 240, u.b));
	UT_ASSERT(memcmp(saved, &u, sizeof(u)) == 0);
	UT_ASSERT(!cluster_relmap_checkpoint_window_encode_v1(&w, &cp, 200, 240, NULL));
}
static void
rewrite_field(uint8 b[160], unsigned offset, uint64 value)
{
	pg_crc32c crc;
	for (unsigned i = 0; i < 8; i++)
		b[offset + i] = (uint8)(value >> (8 * i));
	memset(b + 136, 0, 4);
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, b, 160);
	FIN_CRC32C(crc);
	for (unsigned i = 0; i < 4; i++)
		b[136 + i] = (uint8)((uint32)crc >> (8 * i));
}
UT_TEST(v2_preserves_actual_postflush_issuance_without_ceiling_inference)
{
	uint8 b[160], legacy[160];
	uint64 issued = UINT64CONST(0x100001000), out_issued = 0;
	ClusterRelmapCheckpointWindow out;
	setup();
	UT_ASSERT(cluster_relmap_checkpoint_window_encode_v2(&w, &cp, 200, 240, issued, b));
	UT_ASSERT(memcmp(b, "RMWIN002", 8) == 0 && b[8] == 2);
	UT_ASSERT(
		cluster_relmap_checkpoint_window_decode_v2(b, &cp, 200, 240, &id, 11, &out, &out_issued));
	UT_ASSERT_EQ(out_issued, issued);
	UT_ASSERT_EQ(out.last_issued, w.last_issued);
	UT_ASSERT(out_issued > U64FromFullTransactionId(cp.nextXid));
	UT_ASSERT(cluster_relmap_checkpoint_window_decode_v1(b, &cp, 200, 240, &id, 11, &out));
	UT_ASSERT(cluster_relmap_checkpoint_window_encode_v1(&w, &cp, 200, 240, legacy));
	UT_ASSERT(memcmp(legacy, "RMWIN001", 8) == 0 && legacy[8] == 1);
	for (unsigned i = 140; i < 160; i++)
		UT_ASSERT_EQ(legacy[i], 0);
}
UT_TEST(v2_rejects_valid_crc_wrong_issued_and_reserved_fields)
{
	uint8 b[160], saved[160], zero_bytes[160] = { 0 };
	uint64 bad_values[]
		= { 0, 1, UINT64CONST(0x100000101), UINT64CONST(0x17ffffffe), PG_UINT64_MAX };
	ClusterRelmapCheckpointWindow out, zero_window = { 0 };
	uint64 out_issued;
	setup();
	UT_ASSERT(cluster_relmap_checkpoint_window_encode_v2(&w, &cp, 200, 240, w.last_issued, saved));
	for (unsigned i = 0; i < lengthof(bad_values); i++) {
		memset(b, 0xa5, sizeof(b));
		UT_ASSERT(!cluster_relmap_checkpoint_window_encode_v2(&w, &cp, 200, 240, bad_values[i], b));
		UT_ASSERT(memcmp(b, zero_bytes, sizeof(b)) == 0);
		memcpy(b, saved, sizeof(b));
		rewrite_field(b, 140, bad_values[i]);
		memset(&out, 0xa5, sizeof(out));
		out_issued = 99;
		UT_ASSERT(!cluster_relmap_checkpoint_window_decode_v2(b, &cp, 200, 240, &id, 11, &out,
															  &out_issued));
		UT_ASSERT(memcmp(&out, &zero_window, sizeof(out)) == 0 && out_issued == 0);
		UT_ASSERT(!cluster_relmap_checkpoint_window_decode_v1(b, &cp, 200, 240, &id, 11, &out));
	}
	memcpy(b, saved, sizeof(b));
	rewrite_field(b, 148, 1);
	out_issued = 99;
	UT_ASSERT(
		!cluster_relmap_checkpoint_window_decode_v2(b, &cp, 200, 240, &id, 11, &out, &out_issued));
	UT_ASSERT(out_issued == 0 && memcmp(&out, &zero_window, sizeof(out)) == 0);
	for (unsigned i = 0; i < 160; i++) {
		memcpy(b, saved, sizeof(b));
		b[i] ^= 1;
		out_issued = 99;
		UT_ASSERT(!cluster_relmap_checkpoint_window_decode_v2(b, &cp, 200, 240, &id, 11, &out,
															  &out_issued));
		UT_ASSERT(out_issued == 0 && memcmp(&out, &zero_window, sizeof(out)) == 0);
	}
}
UT_TEST(v2_legacy_refusal_and_alias_keep_inputs)
{
	union {
		uint8 b[160];
		uint64 issued;
		ClusterRelmapCheckpointWindow w;
	} u;
	uint8 saved[160];
	ClusterRelmapCheckpointWindow out, zero_window = { 0 };
	uint64 issued = 99;
	setup();
	UT_ASSERT(cluster_relmap_checkpoint_window_encode_v1(&w, &cp, 200, 240, u.b));
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(
		!cluster_relmap_checkpoint_window_decode_v2(u.b, &cp, 200, 240, &id, 11, &out, &issued));
	UT_ASSERT(issued == 0 && memcmp(&out, &zero_window, sizeof(out)) == 0);
	UT_ASSERT(cluster_relmap_checkpoint_window_encode_v2(&w, &cp, 200, 240, w.last_issued, u.b));
	memcpy(saved, u.b, 160);
	UT_ASSERT(
		!cluster_relmap_checkpoint_window_decode_v2(u.b, &cp, 200, 240, &id, 11, &out, &u.issued));
	UT_ASSERT(memcmp(saved, u.b, 160) == 0);
	UT_ASSERT(
		!cluster_relmap_checkpoint_window_decode_v2(u.b, &cp, 200, 240, &id, 11, &u.w, &issued));
	UT_ASSERT(memcmp(saved, u.b, 160) == 0);
	UT_ASSERT(!cluster_relmap_checkpoint_window_decode_v2(u.b, &cp, 200, 240, &id, 11, &out,
														  (uint64 *)&out));
	UT_ASSERT(!cluster_relmap_checkpoint_window_decode_v2(u.b, &cp, 200, 240, &id, 11, &out, NULL));
	UT_ASSERT(memcmp(&out, &zero_window, sizeof(out)) == 0);
}
int
main(void)
{
	UT_PLAN(8);
	UT_RUN(v2_preserves_actual_postflush_issuance_without_ceiling_inference);
	UT_RUN(v2_rejects_valid_crc_wrong_issued_and_reserved_fields);
	UT_RUN(v2_legacy_refusal_and_alias_keep_inputs);
	UT_RUN(roundtrip_binds_native_checkpoint);
	UT_RUN(rejects_corruption_and_all_zero_legacy);
	UT_RUN(rejects_wrong_source_position_and_complete_body);
	UT_RUN(rejects_guessed_issuance_and_halfspace);
	UT_RUN(overlap_preserves_input_and_null_refuses);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
