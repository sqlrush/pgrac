/* Fixed native window codec, not a live owner/base qualification. */
#include "postgres.h"
#include "cluster/cluster_relmap_checkpoint.h"
#include "unit_test.h"
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
int
main(void)
{
	UT_PLAN(5);
	UT_RUN(roundtrip_binds_native_checkpoint);
	UT_RUN(rejects_corruption_and_all_zero_legacy);
	UT_RUN(rejects_wrong_source_position_and_complete_body);
	UT_RUN(rejects_guessed_issuance_and_halfspace);
	UT_RUN(overlap_preserves_input_and_null_refuses);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
