/* Original TT capture plus real sealed SIDE, native TT/UNDO folds and parser.
 * Only temporary path resolution, allocation and the inherited native-runtime
 * boundaries are fixtures. This does not execute Startup or classify losers.
 * Author: SqlRush <sqlrush@gmail.com>
 */
int side_xact_fixture_main(void);
#define main side_xact_fixture_main
#include "test_cluster_side_xact.c"
#undef main

#include <fcntl.h>
#include "cluster/cluster_cold_recovery.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "cluster/storage/cluster_undo_alloc.h"
#include "storage/fd.h"
#include "utils/memutils.h"

static char tt_redo_dir[MAXPGPATH];

void *
palloc_extended(Size size, int flags)
{
	return (flags & MCXT_ALLOC_ZERO) ? calloc(1, size) : malloc(size);
}

void
pfree(void *ptr)
{
	free(ptr);
}

int
cluster_shared_fs_undo_path_resolve(uint8 owner, uint32 segment, char *buf, size_t size)
{
	uint32 first = (owner - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;

	UT_ASSERT(owner >= 1 && owner <= 128 && segment >= first && segment < first + 256);
	return snprintf(buf, size, "%s/%u", tt_redo_dir, segment) >= size ? -1 : 0;
}

int
BasicOpenFile(const char *path, int flags)
{
	return open(path, flags);
}

#include "../../backend/cluster/cluster_cold_recovery_tt.c"

typedef struct TtRedoFixture {
	ClusterColdTypedV1 typed;
	RfContributorStreamCutV1 cut;
	PGAlignedBlock base;
} TtRedoFixture;

static void
tt_redo_fixture(TtRedoFixture *f, XLogRecPtr end, RfSidePlanPurposeV1 purpose)
{
	RfSideOnlinePlanRequestV1 request = { 0 };
	UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)f->base.data;
	char path[MAXPGPATH];
	int fd;

	memset(f, 0, sizeof(*f));
	strlcpy(tt_redo_dir, "/tmp/pgrac-tt-redo-XXXXXX", sizeof(tt_redo_dir));
	UT_ASSERT_NOT_NULL(mkdtemp(tt_redo_dir));
	cluster_undo_segment_make_header_bytes(513, 3, f->base.data);
	header->tt_slots[4].xid = 802;
	header->tt_slots[4].wrap = 7;
	header->tt_slots[4].status = TT_SLOT_ACTIVE;
	header->tt_slots[4].first_undo_block = (UBA)InvalidUba_init;
	header->tt_slots[47].xid = 804;
	header->tt_slots[47].wrap = 5;
	header->tt_slots[47].status = TT_SLOT_ACTIVE;
	header->tt_slots[47].first_undo_block.raw[0] = (UINT64_C(5) << 32) | 513;
	header->tt_slots[47].first_undo_block.raw[1] = 47;
	snprintf(path, sizeof(path), "%s/513", tt_redo_dir);
	fd = open(path, O_RDWR | O_CREAT | O_EXCL, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(ftruncate(fd, UNDO_SEGMENT_SIZE_BYTES), 0);
	UT_ASSERT_EQ(pwrite(fd, f->base.data, BLCKSZ, 0), BLCKSZ);
	UT_ASSERT_EQ(close(fd), 0);

	f->typed.system_identifier = UINT64_C(0x11223344);
	memset(f->typed.observer.storage_uuid, 0x44, 16);
	f->typed.observer.database_incarnation = 42;
	f->typed.participant_count = 2;
	f->typed.replay_count = 1;
	f->typed.participants[0] = (ClusterColdParticipantV1){ .thread_id = 3,
														   .timeline = 7,
														   .owner_incarnation = 33,
														   .physical_lower = 100,
														   .native_redo = 100,
														   .tail_end = end };
	f->typed.participants[1] = (ClusterColdParticipantV1){ .thread_id = 2,
														   .timeline = 7,
														   .owner_incarnation = 22,
														   .physical_lower = 100,
														   .native_redo = 100,
														   .tail_end = 100 };
	for (uint32 i = 0; i < 2; i++) {
		ClusterWalThreadClaimRefV2 *ref = &f->typed.sources[i].claim;

		ref->identity.system_identifier = f->typed.system_identifier;
		memcpy(ref->identity.storage_uuid, f->typed.observer.storage_uuid, 16);
		memset(ref->identity.authority_uuid, 0x55, 16);
		ref->identity.origin_thread_id = f->typed.participants[i].thread_id;
		ref->identity.origin_node_id = ref->identity.origin_thread_id - 1;
		ref->identity.origin_owner_incarnation = f->typed.participants[i].owner_incarnation;
		ref->identity.root_lineage_seq = 1;
		ref->identity.thread_claim_created_at = 42;
		ref->database_incarnation = 42;
		ref->max_config_generation = 9;
		memset(ref->claim_sha256, 0x20 + i, 32);
		f->typed.sources[i].timeline = 7;
	}
	UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&f->typed, (Size)1024 * 1024), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&f->typed), 1);
	f->cut = (RfContributorStreamCutV1){
		.failed_thread = 3,
		.timeline_id = 7,
		.origin_owner_incarnation = 33,
		.scan_begin_inclusive = 100,
		.scan_end_exclusive = end,
		.flags = RF_CONTRIBUTOR_CUT_COMPLETE | (end == 100 ? RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY : 0)
	};
	request.system_identifier = f->typed.system_identifier;
	memcpy(request.storage_uuid, f->typed.observer.storage_uuid, 16);
	request.physical_cuts = &f->cut;
	request.participant_count = 1;
	request.purpose = purpose;
	request.redo_starts
		= purpose == RF_SIDE_PLAN_RECOVERY ? &f->typed.participants[0].native_redo : NULL;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &f->typed.side_plans[0]),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_bind_sources_v1(f->typed.side_plans[0], f->typed.sources, 2));
	UT_ASSERT(rf_side_online_plan_bind_database_v1(f->typed.side_plans[0], 42));
}

static void
tt_redo_destroy(TtRedoFixture *f)
{
	char path[MAXPGPATH];

	rf_side_online_plan_destroy_v1(&f->typed.side_plans[0]);
	cluster_cold_tt_base_destroy_v1(&f->typed);
	snprintf(path, sizeof(path), "%s/513", tt_redo_dir);
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT_EQ(rmdir(tt_redo_dir), 0);
	tt_redo_dir[0] = '\0';
}

static void
tt_redo_seal(TtRedoFixture *f)
{
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(f->typed.side_plans[0]), RF_PAGE_PROOF_DETAIL_OK);
}

static void
tt_redo_refuses(TtRedoFixture *f, uint32 participant, uint32 segment)
{
	RfSideUndoHeaderImageV1 out, saved;

	memset(&saved, 0xa9, sizeof(saved));
	out = saved;
	UT_ASSERT(!cluster_cold_typed_tt_redo_header_v1(&f->typed, participant, segment, &out));
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
}

UT_TEST(tt_redo_commit_without_bind_keeps_original_all_slots_and_heads)
{
	TtRedoFixture f;
	FakeXactRecord fake;
	RfSideUndoHeaderImageV1 out, again;
	PGAlignedBlock expected;
	const UndoSegmentHeaderData *captured;
	uint8 owner;
	uint32 segment;

	tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
	undo_header_commit(&fake, 0, 802);
	undo_header_feed(f.typed.side_plans[0], &fake, 100);
	tt_redo_seal(&f);
	expected = f.base;
	((UndoSegmentHeaderData *)expected.data)->tt_slots[4].status = TT_SLOT_COMMITTED;
	((UndoSegmentHeaderData *)expected.data)->tt_slots[4].commit_scn = 999;
	UT_ASSERT(cluster_cold_typed_tt_redo_header_v1(&f.typed, 0, 0, &out));
	UT_ASSERT(memcmp(out.page.data, expected.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(out.operation_count, 1);
	UT_ASSERT_EQ(out.source_index, 0);
	UT_ASSERT(!out.has_full_image);
	captured = cluster_cold_tt_base_header_v1(&f.typed, 0, &owner, &segment);
	UT_ASSERT_NOT_NULL(captured);
	UT_ASSERT(memcmp(captured, f.base.data, BLCKSZ) == 0);
	out.page.data[100] ^= 1;
	UT_ASSERT(cluster_cold_typed_tt_redo_header_v1(&f.typed, 0, 0, &again));
	UT_ASSERT(memcmp(again.page.data, expected.data, BLCKSZ) == 0);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_unrelated_terminal_preserves_active_input)
{
	TtRedoFixture f;
	FakeXactRecord fake;
	xl_xact_tt_commit delta;
	RfSideUndoHeaderImageV1 out;
	uint32 offset;

	tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
	undo_header_commit(&fake, 0, 802);
	offset = fake.u.decoded.main_data_len - sizeof(delta);
	memcpy(&delta, fake.data + offset, sizeof(delta));
	delta.segment_id = 514;
	memcpy(fake.data + offset, &delta, sizeof(delta));
	undo_header_feed(f.typed.side_plans[0], &fake, 100);
	tt_redo_seal(&f);
	UT_ASSERT(cluster_cold_typed_tt_redo_header_v1(&f.typed, 0, 0, &out));
	UT_ASSERT(memcmp(out.page.data, f.base.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(out.operation_count, 0);
	UT_ASSERT_EQ(out.source_index, UINT32_MAX);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_empty_cut_preserves_input_and_requires_canonical_flags)
{
	TtRedoFixture f;
	RfSideUndoHeaderImageV1 out;
	RfContributorStreamCutV1 bad;

	tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
	tt_redo_seal(&f);
	UT_ASSERT(rf_side_online_plan_recovery_source_matches_v1(
		f.typed.side_plans[0], f.typed.system_identifier, f.typed.observer.storage_uuid, &f.cut));
	UT_ASSERT(cluster_cold_typed_tt_redo_header_v1(&f.typed, 0, 0, &out));
	UT_ASSERT(memcmp(out.page.data, f.base.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(out.operation_count, 0);
	UT_ASSERT_EQ(out.source_index, UINT32_MAX);
	for (int fault = 0; fault < 4; fault++) {
		bad = f.cut;
		if (fault == 0)
			bad.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		if (fault == 1)
			bad.flags |= UINT16_C(0x8000);
		if (fault == 2)
			bad.scan_end_exclusive++;
		if (fault == 3)
			bad.flags = RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY;
		UT_ASSERT(!rf_side_online_plan_recovery_source_matches_v1(
			f.typed.side_plans[0], f.typed.system_identifier, f.typed.observer.storage_uuid, &bad));
	}
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_exact_abort_uses_original_active_slot_without_bind)
{
	TtRedoFixture f;
	FakeXactRecord fake;
	RfSideUndoHeaderImageV1 out;
	PGAlignedBlock expected;

	tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
	undo_header_abort(&fake, 4);
	undo_header_feed(f.typed.side_plans[0], &fake, 100);
	tt_redo_seal(&f);
	expected = f.base;
	((UndoSegmentHeaderData *)expected.data)->tt_slots[4].status = TT_SLOT_ABORTED;
	UT_ASSERT(cluster_cold_typed_tt_redo_header_v1(&f.typed, 0, 0, &out));
	UT_ASSERT(memcmp(out.page.data, expected.data, BLCKSZ) == 0);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_rejects_each_source_and_checkpoint_identity_drift)
{
	TtRedoFixture f;
	FakeXactRecord fake;
	ClusterColdTypedV1 saved;

	tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
	undo_header_commit(&fake, 0, 802);
	undo_header_feed(f.typed.side_plans[0], &fake, 100);
	tt_redo_seal(&f);
	saved = f.typed;
	for (int fault = 0; fault < 10; fault++) {
		f.typed = saved;
		if (fault == 0)
			f.typed.system_identifier++;
		if (fault == 1)
			f.typed.observer.storage_uuid[0]++;
		if (fault == 2)
			f.typed.observer.database_incarnation++;
		if (fault == 3)
			f.typed.participants[0].timeline++;
		if (fault == 4)
			f.typed.participants[0].owner_incarnation++;
		if (fault == 5)
			f.typed.participants[0].physical_lower++;
		if (fault == 6)
			f.typed.participants[0].tail_end++;
		if (fault == 7)
			f.typed.participants[0].native_redo++;
		if (fault == 8)
			f.typed.participants[0].thread_id = 2;
		if (fault == 9)
			f.typed.participants[0].tail_end = 100;
		tt_redo_refuses(&f, 0, 0);
	}
	f.typed = saved;
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_rejects_incomplete_capture_plan_and_invalid_bounds)
{
	TtRedoFixture f;
	ClusterColdTypedV1 saved;
	RfSideUndoHeaderImageV1 out;

	tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
	tt_redo_refuses(&f, 0, 0); /* Unsealed even for zero records. */
	tt_redo_seal(&f);
	tt_redo_refuses(&f, 1, 0); /* History is not a selected replay source. */
	tt_redo_refuses(&f, UINT32_MAX, 0);
	tt_redo_refuses(&f, 0, 1);
	UT_ASSERT(!cluster_cold_typed_tt_redo_header_v1(NULL, 0, 0, &out));
	UT_ASSERT(!cluster_cold_typed_tt_redo_header_v1(&f.typed, 0, 0, NULL));
	saved = f.typed;
	for (int fault = 0; fault < 7; fault++) {
		f.typed = saved;
		if (fault == 0)
			f.typed.tt_base = NULL;
		if (fault == 1)
			f.typed.side_plans[0] = NULL;
		if (fault == 2)
			f.typed.side_capture_detail[0] = 1;
		if (fault == 3)
			f.typed.participant_count = 0;
		if (fault == 4)
			f.typed.replay_count = CLUSTER_COLD_MAX_PARTICIPANTS + 1;
		if (fault == 5)
			f.typed.participant_count = CLUSTER_COLD_MAX_PARTICIPANTS + 1;
		if (fault == 6)
			f.typed.refusal = CLUSTER_COLD_CAPACITY;
		tt_redo_refuses(&f, 0, 0);
	}
	f.typed = saved;
	f.typed.tt_base->detail = CLUSTER_COLD_CAPACITY;
	tt_redo_refuses(&f, 0, 0);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_refuses_native_commit_identity_mismatch_without_output)
{
	for (int fault = 0; fault < 2; fault++) {
		TtRedoFixture f;
		FakeXactRecord fake;

		tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
		undo_header_commit(&fake, fault == 0 ? 1 : 0, fault == 1 ? 803 : 802);
		undo_header_feed(f.typed.side_plans[0], &fake, 100);
		tt_redo_seal(&f);
		tt_redo_refuses(&f, 0, 0);
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_redo_refuses_pi_purpose)
{
	TtRedoFixture f;

	tt_redo_fixture(&f, 100, RF_SIDE_PLAN_PI_CONTRIBUTIONS);
	tt_redo_seal(&f);
	tt_redo_refuses(&f, 0, 0);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_refuses_prepared_even_without_matching_header_effect)
{
	TtRedoFixture f;
	FakeXactRecord fake;

	tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
	make_commit_prepared(&fake, 802, 16384, 901, 123456);
	undo_header_feed(f.typed.side_plans[0], &fake, 100);
	tt_redo_seal(&f);
	tt_redo_refuses(&f, 0, 0);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_refuses_c0_in_ordinary_only_input)
{
	TtRedoFixture f;
	FakeXactRecord fake;
	ClusterRecoveryTTAbortV1 c0 = { 0 };
	uint8 payload[152];

	tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
	c0.version = 1;
	c0.length = sizeof(payload);
	c0.flags = CLUSTER_RECOVERY_TT_ABORT_TOP_KNOWN;
	c0.writer_thread = 3;
	c0.writer_incarnation = 33;
	c0.target_thread = 2;
	c0.target_incarnation = 22;
	c0.recovery_serial = 19;
	c0.target_epoch = 4;
	c0.target_full_xid = c0.top_full_xid = UINT64_C(0x700400102);
	memcpy(c0.target_claim_sha256, f.typed.sources[1].claim.claim_sha256, 32);
	c0.exact = (xl_undo_tt_slot_abort_exact){ .segment_id = 257,
											  .xid = 0x400102,
											  .slot_offset = 5,
											  .wrap = 9,
											  .instance = 2,
											  .format_version = 1 };
	c0.before.xid = c0.exact.xid;
	c0.before.wrap = 9;
	c0.before.status = TT_SLOT_ACTIVE;
	c0.before.first_undo_block.raw[0] = UINT64_C(0x300000102);
	c0.before.first_undo_block.raw[1] = UINT64_C(0x1000005);
	UT_ASSERT(cluster_recovery_tt_abort_encode_v1(&c0, payload, sizeof(payload)));
	make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_RECOVERY_TT_ABORT, payload,
						   sizeof(payload));
	fake.u.decoded.max_block_id = -1;
	undo_header_feed(f.typed.side_plans[0], &fake, 100);
	tt_redo_seal(&f);
	tt_redo_refuses(&f, 0, 0);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_retains_native_refusal_of_unretired_active_head)
{
	for (int terminal = 0; terminal < 2; terminal++) {
		TtRedoFixture f;
		FakeXactRecord fake;
		UndoSegmentHeaderData *header;
		const UndoSegmentHeaderData *captured;
		char path[MAXPGPATH];
		uint8 owner;
		uint32 segment;
		int fd;

		tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
		header = (UndoSegmentHeaderData *)f.base.data;
		header->tt_slots[4].first_undo_block = header->tt_slots[47].first_undo_block;
		snprintf(path, sizeof(path), "%s/513", tt_redo_dir);
		fd = open(path, O_RDWR);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(pwrite(fd, f.base.data, BLCKSZ, 0), BLCKSZ);
		UT_ASSERT_EQ(close(fd), 0);
		cluster_cold_tt_base_destroy_v1(&f.typed);
		UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&f.typed, (Size)1024 * 1024), CLUSTER_COLD_OK);
		if (terminal == 0)
			undo_header_commit(&fake, 0, 802);
		else
			undo_header_abort(&fake, 4);
		undo_header_feed(f.typed.side_plans[0], &fake, 100);
		tt_redo_seal(&f);
		tt_redo_refuses(&f, 0, 0);
		captured = cluster_cold_tt_base_header_v1(&f.typed, 0, &owner, &segment);
		UT_ASSERT_NOT_NULL(captured);
		UT_ASSERT(memcmp(captured, f.base.data, BLCKSZ) == 0);
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_redo_refuses_standalone_prepared_tt_without_xact_terminal)
{
	for (int kind = 0; kind < 3; kind++) {
		TtRedoFixture f;
		FakeXactRecord fake;
		UBA head = InvalidUba_init;

		tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
		if (kind == 0)
			make_tt_commit_delta(&fake, 802, 999);
		else if (kind == 1)
			make_tt_abort_delta(&fake, 802);
		else {
			head.raw[0] = (UINT64_C(5) << 32) | 513;
			head.raw[1] = 4;
			make_tt_set_head_delta(&fake, 802, &head);
		}
		undo_header_feed(f.typed.side_plans[0], &fake, 100);
		tt_redo_seal(&f);
		tt_redo_refuses(&f, 0, 0);
		tt_redo_destroy(&f);
	}
}

static void
tt_redo_slot_refuses(TtRedoFixture *f, uint32 participant, uint32 segment, uint32 slot)
{
	ClusterColdTtSlotInputV1 out, zero = { 0 };

	memset(&out, 0xa9, sizeof(out));
	UT_ASSERT(!cluster_cold_typed_tt_redo_slot_v1(&f->typed, participant, segment, slot, &out));
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
}

UT_TEST(tt_redo_slots_enumerate_every_status_and_preserve_generation_zero)
{
	TtRedoFixture f;
	ClusterColdTtSlotInputV1 out;
	const UndoSegmentHeaderData *base;
	uint8 owner;
	uint32 segment;

	tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
	tt_redo_seal(&f);
	base = cluster_cold_tt_base_header_v1(&f.typed, 0, &owner, &segment);
	UT_ASSERT_NOT_NULL(base);
	for (uint32 i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		UT_ASSERT(cluster_cold_typed_tt_redo_slot_v1(&f.typed, 0, 0, i, &out));
		UT_ASSERT_EQ(out.key.owner_instance, 3);
		UT_ASSERT_EQ(out.key.segment_id, 513);
		UT_ASSERT_EQ(out.key.segment_generation, 0);
		UT_ASSERT_EQ(out.key.slot_offset, i);
		UT_ASSERT_EQ(out.key.xid, base->tt_slots[i].xid);
		UT_ASSERT_EQ(out.key.slot_wrap, base->tt_slots[i].wrap);
		UT_ASSERT(memcmp(&out.slot, &base->tt_slots[i], sizeof(TTSlot)) == 0);
		memset(&out, 0xa9, sizeof(out));
	}
	UT_ASSERT(memcmp(base, f.base.data, BLCKSZ) == 0);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_slot_reads_commit_and_matches_after_image_independently)
{
	TtRedoFixture f;
	FakeXactRecord fake;
	ClusterColdTtSlotInputV1 before, out;

	tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
	UT_ASSERT(cluster_cold_tt_base_slot_v1(&f.typed, 0, 4, &before));
	undo_header_commit(&fake, 0, 802);
	undo_header_feed(f.typed.side_plans[0], &fake, 100);
	tt_redo_seal(&f);
	UT_ASSERT(cluster_cold_typed_tt_redo_slot_v1(&f.typed, 0, 0, 4, &out));
	UT_ASSERT_EQ(before.slot.status, TT_SLOT_ACTIVE);
	UT_ASSERT_EQ(out.slot.status, TT_SLOT_COMMITTED);
	UT_ASSERT_EQ(out.slot.commit_scn, 999);
	UT_ASSERT(cluster_cold_typed_tt_redo_match_v1(&f.typed, 0, 0, &out.key, &out));
	UT_ASSERT_EQ(out.slot.status, TT_SLOT_COMMITTED);
	UT_ASSERT_EQ(out.slot.commit_scn, 999);
	memset(&out.slot, 0xa9, sizeof(out.slot));
	UT_ASSERT(cluster_cold_typed_tt_redo_match_v1(&f.typed, 0, 0, &before.key, &out));
	UT_ASSERT_EQ(out.slot.status, TT_SLOT_COMMITTED);
	UT_ASSERT(cluster_cold_tt_base_slot_v1(&f.typed, 0, 4, &before));
	UT_ASSERT_EQ(before.slot.status, TT_SLOT_ACTIVE);
	UT_ASSERT(!SCN_VALID(before.slot.commit_scn));
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_slot_reads_exact_abort_without_loser_inference)
{
	TtRedoFixture f;
	FakeXactRecord fake;
	ClusterColdTtSlotInputV1 out;

	tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
	undo_header_abort(&fake, 4);
	undo_header_feed(f.typed.side_plans[0], &fake, 100);
	tt_redo_seal(&f);
	UT_ASSERT(cluster_cold_typed_tt_redo_slot_v1(&f.typed, 0, 0, 4, &out));
	UT_ASSERT_EQ(out.slot.status, TT_SLOT_ABORTED);
	UT_ASSERT_EQ(out.key.xid, 802);
	UT_ASSERT_EQ(out.key.slot_wrap, 7);
	UT_ASSERT(cluster_cold_typed_tt_redo_match_v1(&f.typed, 0, 0, &out.key, &out));
	UT_ASSERT_EQ(out.slot.status, TT_SLOT_ABORTED);
	UT_ASSERT(cluster_cold_typed_tt_redo_slot_v1(&f.typed, 0, 0, 47, &out));
	UT_ASSERT_EQ(out.slot.status, TT_SLOT_ACTIVE);
	UT_ASSERT(memcmp(&out.slot.first_undo_block,
					 &((UndoSegmentHeaderData *)f.base.data)->tt_slots[47].first_undo_block,
					 sizeof(UBA))
			  == 0);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_match_refuses_each_stale_key_field_and_clears_output)
{
	TtRedoFixture f;
	ClusterColdTtSlotInputV1 input, out, zero = { 0 };
	ClusterColdTtSlotKeyV1 key;

	tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
	tt_redo_seal(&f);
	UT_ASSERT(cluster_cold_typed_tt_redo_slot_v1(&f.typed, 0, 0, 47, &input));
	for (int fault = 0; fault < 10; fault++) {
		key = input.key;
		if (fault == 0)
			key.owner_instance = 2;
		if (fault == 1)
			key.segment_id++;
		if (fault == 2)
			key.segment_generation++;
		if (fault == 3)
			key.slot_offset--;
		if (fault == 4)
			key.xid++;
		if (fault == 5)
			key.slot_wrap++;
		if (fault == 6)
			key.owner_instance = 0;
		if (fault == 7)
			key.slot_offset = TT_SLOTS_PER_SEGMENT;
		if (fault == 8)
			key.xid = InvalidTransactionId;
		if (fault == 9)
			key.slot_wrap = TT_WRAP_INVALID;
		memset(&out, 0xa9, sizeof(out));
		UT_ASSERT(!cluster_cold_typed_tt_redo_match_v1(&f.typed, 0, 0, &key, &out));
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	}
	UT_ASSERT(cluster_cold_typed_tt_redo_match_v1(&f.typed, 0, 0, &input.key, &out));
	UT_ASSERT_EQ(out.slot.status, TT_SLOT_ACTIVE);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_slots_reject_unsealed_history_bounds_and_invalid_arguments)
{
	TtRedoFixture f;
	ClusterColdTtSlotInputV1 out, zero = { 0 };
	ClusterColdTtSlotKeyV1 key = { 0 };

	tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
	tt_redo_slot_refuses(&f, 0, 0, 4);
	tt_redo_seal(&f);
	tt_redo_slot_refuses(&f, 1, 0, 4);
	tt_redo_slot_refuses(&f, UINT32_MAX, 0, 4);
	tt_redo_slot_refuses(&f, 0, UINT32_MAX, 4);
	tt_redo_slot_refuses(&f, 0, 0, TT_SLOTS_PER_SEGMENT);
	UT_ASSERT(cluster_cold_tt_base_slot_v1(&f.typed, 0, 4, &out));
	key = out.key;
	UT_ASSERT(!cluster_cold_typed_tt_redo_match_v1(&f.typed, 0, 1, &key, &out));
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	memset(&out, 0xa9, sizeof(out));
	UT_ASSERT(!cluster_cold_typed_tt_redo_slot_v1(NULL, 0, 0, 4, &out));
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	UT_ASSERT(!cluster_cold_typed_tt_redo_slot_v1(&f.typed, 0, 0, 4, NULL));
	memset(&out, 0xa9, sizeof(out));
	UT_ASSERT(!cluster_cold_typed_tt_redo_match_v1(NULL, 0, 0, &key, &out));
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	memset(&out, 0xa9, sizeof(out));
	UT_ASSERT(!cluster_cold_typed_tt_redo_match_v1(&f.typed, 0, 0, NULL, &out));
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	UT_ASSERT(!cluster_cold_typed_tt_redo_match_v1(&f.typed, 0, 0, &key, NULL));
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_slots_refuse_source_drift_and_unsupported_prepared_input)
{
	TtRedoFixture f;
	FakeXactRecord fake;
	ClusterColdTtSlotInputV1 input, out, zero = { 0 };

	tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
	make_commit_prepared(&fake, 802, 16384, 901, 123456);
	undo_header_feed(f.typed.side_plans[0], &fake, 100);
	tt_redo_seal(&f);
	tt_redo_slot_refuses(&f, 0, 0, 4);
	UT_ASSERT(cluster_cold_tt_base_slot_v1(&f.typed, 0, 4, &input));
	memset(&out, 0xa9, sizeof(out));
	UT_ASSERT(!cluster_cold_typed_tt_redo_match_v1(&f.typed, 0, 0, &input.key, &out));
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	tt_redo_destroy(&f);
	tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
	tt_redo_seal(&f);
	UT_ASSERT(cluster_cold_tt_base_slot_v1(&f.typed, 0, 4, &input));
	f.typed.participants[0].native_redo++;
	tt_redo_slot_refuses(&f, 0, 0, 4);
	memset(&out, 0xa9, sizeof(out));
	UT_ASSERT(!cluster_cold_typed_tt_redo_match_v1(&f.typed, 0, 0, &input.key, &out));
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_slots_use_reused_binding_instead_of_captured_key)
{
	TtRedoFixture f;
	FakeXactRecord fake;
	xl_undo_tt_slot_bind bind;
	ClusterColdTtSlotInputV1 before, out, zero = { 0 };
	UndoSegmentHeaderData *header;
	char path[MAXPGPATH];
	int fd;

	tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
	header = (UndoSegmentHeaderData *)f.base.data;
	header->tt_slots[4].status = TT_SLOT_RECYCLABLE;
	snprintf(path, sizeof(path), "%s/513", tt_redo_dir);
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pwrite(fd, f.base.data, BLCKSZ, 0), BLCKSZ);
	UT_ASSERT_EQ(close(fd), 0);
	cluster_cold_tt_base_destroy_v1(&f.typed);
	UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&f.typed, (Size)1024 * 1024), CLUSTER_COLD_OK);
	UT_ASSERT(cluster_cold_tt_base_slot_v1(&f.typed, 0, 4, &before));
	undo_header_bind(&fake, 0);
	memcpy(&bind, fake.data, sizeof(bind));
	bind.wrap = 8;
	bind.xid = 806;
	memcpy(fake.data, &bind, sizeof(bind));
	undo_header_feed(f.typed.side_plans[0], &fake, 100);
	tt_redo_seal(&f);
	UT_ASSERT(cluster_cold_typed_tt_redo_slot_v1(&f.typed, 0, 0, 4, &out));
	UT_ASSERT_EQ(out.key.xid, 806);
	UT_ASSERT_EQ(out.key.slot_wrap, 8);
	UT_ASSERT_EQ(out.slot.status, TT_SLOT_ACTIVE);
	UT_ASSERT(cluster_cold_typed_tt_redo_match_v1(&f.typed, 0, 0, &out.key, &out));
	UT_ASSERT_EQ(out.slot.xid, 806);
	UT_ASSERT(!cluster_cold_typed_tt_redo_match_v1(&f.typed, 0, 0, &before.key, &out));
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	UT_ASSERT(cluster_cold_tt_base_slot_v1(&f.typed, 0, 4, &out));
	UT_ASSERT_EQ(out.slot.xid, 802);
	UT_ASSERT_EQ(out.slot.status, TT_SLOT_RECYCLABLE);
	tt_redo_destroy(&f);
}

static UBA
tt_redo_record_address(uint32 segment, uint32 block, uint16 row)
{
	UBA address = { { ((uint64)block << 32) | segment, ((uint64)row << 16) | (4 + row) } };

	return address;
}

static void
tt_redo_record_page(PGAlignedBlock *page, TransactionId xid)
{
	UndoBlockHeader block = { 0 };

	memset(page, 0, sizeof(*page));
	block.magic = PGRAC_UNDO_BLOCK_MAGIC;
	block.block_version = UNDO_BLOCK_VERSION_1;
	block.free_offset = sizeof(block);
	for (uint16 row = 0; row < 2; row++) {
		UndoRecordHeader header = { 0 };
		UndoSlotDirEntry directory = { 0 };
		UndoInsertPayload payload = { 32 + row, 0 };

		header.record_type = UNDO_RECORD_INSERT;
		header.flags = UNDO_REC_FLAG_FIRST_IN_TX;
		header.payload_length = sizeof(payload);
		header.xid = xid + row;
		header.origin_node_id = 2;
		header.tt_slot_segment_id = 513 + row;
		header.tt_slot_id = 5 + row;
		header.tt_wrap_plus1 = row == 0 ? 8 : 0;
		header.target_locator = (RelFileLocator){ 1663, 5, 16385 };
		header.target_offset = 1;
		directory.record_offset = block.free_offset;
		directory.record_length = sizeof(header) + sizeof(payload);
		directory.record_type = header.record_type;
		directory.flags = header.flags;
		memcpy(page->data + block.free_offset, &header, sizeof(header));
		memcpy(page->data + block.free_offset + sizeof(header), &payload, sizeof(payload));
		memcpy(page->data + UNDO_SLOT_DIR_OFFSET(row), &directory, sizeof(directory));
		block.slot_count++;
		block.free_offset += directory.record_length;
	}
	memcpy(page->data, &block, sizeof(block));
}

static void
tt_redo_feed_record_page(TtRedoFixture *f, XLogRecPtr begin, const PGAlignedBlock *page,
						 bool full_image)
{
	FakeXactRecord fake;
	xl_undo_block_write carrier = { 0 };
	uint8 bytes[sizeof(carrier) + BLCKSZ];
	uint32 length = sizeof(carrier);

	carrier.instance = 3;
	carrier.segment_id = 513;
	carrier.block_no = 9;
	carrier.has_fpi = full_image;
	if (full_image) {
		memcpy(bytes + length, page->data, BLCKSZ);
		length += BLCKSZ;
	} else {
		UndoSlotDirEntry directory;

		memcpy(&directory, page->data + UNDO_SLOT_DIR_OFFSET(0), sizeof(directory));
		carrier.rec_off = directory.record_offset;
		carrier.rec_len = directory.record_length;
		carrier.slot_off = UNDO_SLOT_DIR_OFFSET(0);
		memcpy(bytes + length, page->data, UNDO_BLOCK_HDR_PREFIX_LEN);
		length += UNDO_BLOCK_HDR_PREFIX_LEN;
		memcpy(bytes + length, page->data + directory.record_offset, directory.record_length);
		length += directory.record_length;
		memcpy(bytes + length, &directory, sizeof(directory));
		length += sizeof(directory);
	}
	memcpy(bytes, &carrier, sizeof(carrier));
	make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE, bytes, length);
	undo_header_feed(f->typed.side_plans[0], &fake, begin);
}

static void
tt_redo_record_refuses(const ClusterColdTypedV1 *typed, uint32 participant, uint32 segment,
					   UBA address)
{
	ClusterRecoveryUndoRecord out, zero = { 0 };
	uint32 generation = UINT32_MAX;

	memset(&out, 0xa9, sizeof(out));
	UT_ASSERT(!cluster_cold_typed_undo_redo_record_v1(typed, participant, segment, address, &out,
													  &generation));
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	UT_ASSERT_EQ(generation, 0);
}

UT_TEST(tt_redo_undo_uses_latest_fpi_and_delta_without_installing_or_borrowing)
{
	TtRedoFixture f;
	PGAlignedBlock page, disk, zero = { 0 };
	ClusterRecoveryUndoRecord out, saved;
	UBA address = tt_redo_record_address(513, 9, 0);
	uint32 generation = UINT32_MAX;
	uint32 segment;
	uint8 owner;
	char path[MAXPGPATH];
	int fd;

	tt_redo_fixture(&f, 400, RF_SIDE_PLAN_RECOVERY);
	tt_redo_record_page(&page, 802);
	tt_redo_feed_record_page(&f, 100, &page, true);
	tt_redo_record_page(&page, 882);
	tt_redo_feed_record_page(&f, 200, &page, true);
	tt_redo_record_page(&page, 892);
	tt_redo_feed_record_page(&f, 300, &page, false);
	tt_redo_seal(&f);
	UT_ASSERT(cluster_cold_typed_undo_redo_record_v1(&f.typed, 0, 0, address, &out, &generation));
	UT_ASSERT_EQ(generation, 0);
	UT_ASSERT_EQ(out.header.xid, 892);
	UT_ASSERT_EQ(out.block_header.block_lsn, 400);
	UT_ASSERT_EQ(out.header.tt_wrap_plus1, 8);
	saved = out;
	memset(&out, 0x6b, sizeof(out));
	UT_ASSERT(cluster_cold_typed_undo_redo_record_v1(&f.typed, 0, 0, address, &out, &generation));
	UT_ASSERT_EQ(memcmp(&out, &saved, sizeof(out)), 0);
	address = tt_redo_record_address(513, 9, 1);
	UT_ASSERT(cluster_cold_typed_undo_redo_record_v1(&f.typed, 0, 0, address, &out, &generation));
	UT_ASSERT_EQ(out.header.xid, 883); /* The row untouched by the final delta. */
	UT_ASSERT_EQ(out.header.tt_slot_segment_id, 514);
	UT_ASSERT_EQ(out.header.tt_slot_id, 6);
	UT_ASSERT_EQ(out.header.tt_wrap_plus1, 0); /* Unknown stays unknown. */
	UT_ASSERT_EQ(
		memcmp(cluster_cold_tt_base_header_v1(&f.typed, 0, &owner, &segment), f.base.data, BLCKSZ),
		0);
	snprintf(path, sizeof(path), "%s/513", tt_redo_dir);
	fd = open(path, O_RDONLY);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pread(fd, disk.data, BLCKSZ, (off_t)9 * BLCKSZ), BLCKSZ);
	UT_ASSERT_EQ(memcmp(disk.data, zero.data, BLCKSZ), 0);
	UT_ASSERT_EQ(close(fd), 0);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_undo_refuses_missing_or_late_anchor_even_with_valid_data)
{
	for (int fault = 0; fault < 3; fault++) {
		TtRedoFixture f;
		PGAlignedBlock page;
		char path[MAXPGPATH];
		int fd;

		tt_redo_fixture(&f, 100 + fault * 100, RF_SIDE_PLAN_RECOVERY);
		tt_redo_record_page(&page, 802);
		snprintf(path, sizeof(path), "%s/513", tt_redo_dir);
		fd = open(path, O_RDWR);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(pwrite(fd, page.data, BLCKSZ, (off_t)9 * BLCKSZ), BLCKSZ);
		UT_ASSERT_EQ(close(fd), 0);
		if (fault > 0)
			tt_redo_feed_record_page(&f, 100, &page, false);
		if (fault > 1)
			tt_redo_feed_record_page(&f, 200, &page, true);
		tt_redo_seal(&f);
		tt_redo_record_refuses(&f.typed, 0, 0, tt_redo_record_address(513, 9, 0));
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_redo_undo_refuses_bad_bounds_source_and_arguments_with_cleared_outputs)
{
	TtRedoFixture f;
	PGAlignedBlock page;
	UBA address = tt_redo_record_address(513, 9, 0);
	ClusterRecoveryUndoRecord out, zero = { 0 };
	uint32 generation = UINT32_MAX;

	tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
	tt_redo_record_page(&page, 802);
	tt_redo_feed_record_page(&f, 100, &page, true);
	tt_redo_record_refuses(&f.typed, 0, 0, address); /* Unsealed. */
	tt_redo_seal(&f);
	tt_redo_record_refuses(NULL, 0, 0, address);
	tt_redo_record_refuses(&f.typed, 1, 0, address); /* History only. */
	tt_redo_record_refuses(&f.typed, 0, 1, address);
	tt_redo_record_refuses(&f.typed, 0, 0, (UBA)InvalidUba_init);
	tt_redo_record_refuses(&f.typed, 0, 0, tt_redo_record_address(514, 9, 0));
	tt_redo_record_refuses(&f.typed, 0, 0, tt_redo_record_address(513, 0, 0));
	tt_redo_record_refuses(&f.typed, 0, 0, tt_redo_record_address(513, 9, 2));
	f.typed.observer.database_incarnation++;
	tt_redo_record_refuses(&f.typed, 0, 0, address);
	f.typed.observer.database_incarnation--;
	UT_ASSERT(!cluster_cold_typed_undo_redo_record_v1(&f.typed, 0, 0, address, NULL, &generation));
	UT_ASSERT_EQ(generation, 0);
	memset(&out, 0xa9, sizeof(out));
	UT_ASSERT(!cluster_cold_typed_undo_redo_record_v1(&f.typed, 0, 0, address, &out, NULL));
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_undo_refuses_malformed_fpi_record)
{
	TtRedoFixture f;
	PGAlignedBlock page;
	UndoSlotDirEntry directory;

	tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
	tt_redo_record_page(&page, 802);
	memcpy(&directory, page.data + UNDO_SLOT_DIR_OFFSET(0), sizeof(directory));
	directory.record_offset = sizeof(UndoBlockHeader) - 1;
	memcpy(page.data + UNDO_SLOT_DIR_OFFSET(0), &directory, sizeof(directory));
	tt_redo_feed_record_page(&f, 100, &page, true);
	tt_redo_seal(&f);
	tt_redo_record_refuses(&f.typed, 0, 0, tt_redo_record_address(513, 9, 0));
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_undo_requires_new_fpi_after_segment_reuse_and_returns_new_generation)
{
	for (int fresh = 0; fresh < 2; fresh++) {
		TtRedoFixture f;
		FakeXactRecord fake;
		PGAlignedBlock page;
		RfSideUndoHeaderImageV1 header;
		ClusterRecoveryUndoRecord out;
		uint32 generation = UINT32_MAX;
		char path[MAXPGPATH];
		int fd;
		struct {
			xl_undo_segment_reuse record;
			char page[BLCKSZ];
		} reuse = { 0 };

		tt_redo_fixture(&f, fresh ? 400 : 300, RF_SIDE_PLAN_RECOVERY);
		/* Capture a genuinely unused base, before starting this source. */
		cluster_cold_tt_base_destroy_v1(&f.typed);
		cluster_undo_segment_make_header_bytes(513, 3, f.base.data);
		snprintf(path, sizeof(path), "%s/513", tt_redo_dir);
		fd = open(path, O_RDWR);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(pwrite(fd, f.base.data, BLCKSZ, 0), BLCKSZ);
		UT_ASSERT_EQ(close(fd), 0);
		UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&f.typed, (Size)1024 * 1024), CLUSTER_COLD_OK);
		tt_redo_record_page(&page, 802);
		tt_redo_feed_record_page(&f, 100, &page, true);
		reuse.record.instance = 3;
		reuse.record.segment_id = 513;
		reuse.record.new_generation = 1;
		cluster_undo_segment_make_header_bytes(513, 3, reuse.page);
		((UndoSegmentHeaderData *)reuse.page)->wrap_count = 1;
		make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_REUSE, &reuse,
							   sizeof(reuse));
		undo_header_feed(f.typed.side_plans[0], &fake, 200);
		if (fresh) {
			tt_redo_record_page(&page, 882);
			tt_redo_feed_record_page(&f, 300, &page, true);
		}
		tt_redo_seal(&f);
		UT_ASSERT(cluster_cold_typed_tt_redo_header_v1(&f.typed, 0, 0, &header));
		UT_ASSERT_EQ(((UndoSegmentHeaderData *)header.page.data)->wrap_count, 1);
		if (fresh) {
			UT_ASSERT(cluster_cold_typed_undo_redo_record_v1(
				&f.typed, 0, 0, tt_redo_record_address(513, 9, 0), &out, &generation));
			UT_ASSERT_EQ(generation, 1);
			UT_ASSERT_EQ(out.header.xid, 882);
		} else
			tt_redo_record_refuses(&f.typed, 0, 0, tt_redo_record_address(513, 9, 0));
		tt_redo_destroy(&f);
	}
}

static ClusterRecoveryTxnKey
tt_redo_graph_key(TtRedoFixture *f)
{
	ClusterColdTtSlotInputV1 input;

	UT_ASSERT(cluster_cold_typed_tt_redo_slot_v1(&f->typed, 0, 0, 4, &input));
	return (ClusterRecoveryTxnKey){ .system_identifier = f->typed.system_identifier,
									.database_incarnation = f->typed.observer.database_incarnation,
									.origin_node_id = input.key.owner_instance - 1,
									.segment_id = input.key.segment_id,
									.segment_generation = input.key.segment_generation,
									.slot_offset = input.key.slot_offset,
									.xid = input.key.xid,
									.slot_wrap = input.key.slot_wrap };
}

static void
tt_redo_graph_refuses(ClusterColdTtRedoGraphInputV1 *input, const ClusterRecoveryTxnKey *key,
					  UBA address, ClusterRecoveryGraphResult expected)
{
	ClusterRecoveryUndoRecord out, zero = { 0 };
	uint32 generation = UINT32_MAX;

	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT_EQ(cluster_cold_typed_tt_redo_graph_read_v1(input, key, address, &out, &generation),
				 expected);
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	UT_ASSERT_EQ(generation, 0);
}

static void
tt_redo_graph_walk(TtRedoFixture *f, const ClusterRecoveryTxnKey *key, UBA head,
				   uint32 physical_generation, bool drift_before_verify)
{
	ClusterColdTtRedoGraphInputV1 input = { &f->typed, 0 };
	ClusterRecoveryReferenceGraph *graph = NULL;
	ClusterRecoveryGraphRecord out;
	ClusterRecoveryGraphResult result = CLUSTER_RECOVERY_GRAPH_MORE;
	unsigned records = 0;

	UT_ASSERT_EQ(cluster_recovery_reference_graph_begin(
					 1024 * 1024, cluster_cold_typed_tt_redo_graph_read_v1, &input, &graph),
				 CLUSTER_RECOVERY_GRAPH_MORE);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_add(graph, key, head),
				 CLUSTER_RECOVERY_GRAPH_MORE);
	for (unsigned i = 0; i < 32; i++) {
		result = cluster_recovery_reference_graph_step(graph, &out);
		if (result == CLUSTER_RECOVERY_GRAPH_RECORD) {
			UT_ASSERT_NOT_NULL(out.record);
			UT_ASSERT_EQ(out.record->header.xid, key->xid);
			UT_ASSERT_EQ(out.record_segment_generation, physical_generation);
			records++;
			if (drift_before_verify)
				f->typed.system_identifier++;
		} else if (result != CLUSTER_RECOVERY_GRAPH_MORE)
			break;
	}
	UT_ASSERT_EQ(records, 1);
	UT_ASSERT_EQ(result, drift_before_verify ? CLUSTER_RECOVERY_GRAPH_IDENTITY
											 : CLUSTER_RECOVERY_GRAPH_DRAINED);
	UT_ASSERT_NULL(out.record);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_observed(graph), !drift_before_verify);
	if (drift_before_verify)
		f->typed.system_identifier--;
	UT_ASSERT_EQ(cluster_recovery_reference_graph_step(graph, &out), result);
	cluster_recovery_reference_graph_destroy(graph);
}

UT_TEST(tt_redo_graph_uses_rebound_slot_and_owned_fpi_bytes_at_generation_zero)
{
	TtRedoFixture f;
	FakeXactRecord fake;
	xl_undo_tt_slot_bind bind;
	PGAlignedBlock page;
	UndoRecordHeader record;
	UndoSlotDirEntry directory;
	ClusterRecoveryTxnKey key, stale;
	ClusterColdTtRedoGraphInputV1 input = { &f.typed, 0 };
	ClusterRecoveryUndoRecord out;
	uint32 generation = UINT32_MAX;
	char path[MAXPGPATH];
	int fd;

	tt_redo_fixture(&f, 300, RF_SIDE_PLAN_RECOVERY);
	((UndoSegmentHeaderData *)f.base.data)->tt_slots[4].status = TT_SLOT_RECYCLABLE;
	snprintf(path, sizeof(path), "%s/513", tt_redo_dir);
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pwrite(fd, f.base.data, BLCKSZ, 0), BLCKSZ);
	UT_ASSERT_EQ(close(fd), 0);
	cluster_cold_tt_base_destroy_v1(&f.typed);
	UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&f.typed, (Size)1024 * 1024), CLUSTER_COLD_OK);
	undo_header_bind(&fake, 0);
	memcpy(&bind, fake.data, sizeof(bind));
	bind.wrap = 8;
	bind.xid = 806;
	memcpy(fake.data, &bind, sizeof(bind));
	undo_header_feed(f.typed.side_plans[0], &fake, 100);
	tt_redo_record_page(&page, 806);
	memcpy(&directory, page.data + UNDO_SLOT_DIR_OFFSET(0), sizeof(directory));
	memcpy(&record, page.data + directory.record_offset, sizeof(record));
	record.tt_wrap_plus1 = 9;
	memcpy(page.data + directory.record_offset, &record, sizeof(record));
	tt_redo_feed_record_page(&f, 200, &page, true);
	tt_redo_seal(&f);
	key = tt_redo_graph_key(&f);
	UT_ASSERT_EQ(key.segment_generation, 0);
	UT_ASSERT_EQ(key.xid, 806);
	UT_ASSERT_EQ(key.slot_wrap, 8);
	UT_ASSERT_EQ(cluster_cold_typed_tt_redo_graph_read_v1(
					 &input, &key, tt_redo_record_address(513, 9, 0), &out, &generation),
				 CLUSTER_RECOVERY_GRAPH_RECORD);
	UT_ASSERT_EQ(generation, 0);
	memset(out.payload, 0xa5, sizeof(out.payload));
	memset(page.data, 0, BLCKSZ);
	tt_redo_graph_walk(&f, &key, tt_redo_record_address(513, 9, 0), 0, false);
	stale = key;
	stale.xid = 802;
	stale.slot_wrap = 7;
	tt_redo_graph_refuses(&input, &stale, tt_redo_record_address(513, 9, 0),
						  CLUSTER_RECOVERY_GRAPH_IDENTITY);
	UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&f.typed), CLUSTER_COLD_OK);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_graph_cross_segment_record_keeps_physical_generation)
{
	TtRedoFixture f;
	PGAlignedBlock page, physical;
	FakeXactRecord fake;
	xl_undo_block_write carrier = { 0 };
	uint8 bytes[sizeof(carrier) + BLCKSZ];
	ClusterRecoveryTxnKey key;
	char path[MAXPGPATH];
	int fd;

	tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
	cluster_undo_segment_make_header_bytes(514, 3, physical.data);
	((UndoSegmentHeaderData *)physical.data)->wrap_count = 91;
	snprintf(path, sizeof(path), "%s/514", tt_redo_dir);
	fd = open(path, O_RDWR | O_CREAT | O_EXCL, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(ftruncate(fd, UNDO_SEGMENT_SIZE_BYTES), 0);
	UT_ASSERT_EQ(pwrite(fd, physical.data, BLCKSZ, 0), BLCKSZ);
	UT_ASSERT_EQ(close(fd), 0);
	cluster_cold_tt_base_destroy_v1(&f.typed);
	UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&f.typed, (Size)1024 * 1024), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&f.typed), 2);
	tt_redo_record_page(&page, 802);
	carrier.instance = 3;
	carrier.segment_id = 514;
	carrier.block_no = 9;
	carrier.has_fpi = true;
	memcpy(bytes, &carrier, sizeof(carrier));
	memcpy(bytes + sizeof(carrier), page.data, BLCKSZ);
	make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE, bytes, sizeof(bytes));
	undo_header_feed(f.typed.side_plans[0], &fake, 100);
	tt_redo_seal(&f);
	key = tt_redo_graph_key(&f);
	tt_redo_graph_walk(&f, &key, tt_redo_record_address(514, 9, 0), 91, false);
	UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&f.typed), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(unlink(path), 0);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_graph_refuses_namespace_key_source_and_argument_drift)
{
	TtRedoFixture f;
	PGAlignedBlock page;
	ClusterRecoveryTxnKey key, changed;
	ClusterColdTtRedoGraphInputV1 input = { &f.typed, 0 };
	ClusterRecoveryUndoRecord out, zero = { 0 };
	uint32 generation;
	UBA address = tt_redo_record_address(513, 9, 0);

	tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
	tt_redo_record_page(&page, 802);
	tt_redo_feed_record_page(&f, 100, &page, true);
	tt_redo_seal(&f);
	key = tt_redo_graph_key(&f);
	for (int fault = 0; fault < 11; fault++) {
		changed = key;
		switch (fault) {
		case 0:
			changed.system_identifier++;
			break;
		case 1:
			changed.database_incarnation++;
			break;
		case 2:
			changed.origin_node_id++;
			break;
		case 3:
			changed.segment_id++;
			break;
		case 4:
			changed.segment_generation++;
			break;
		case 5:
			changed.slot_offset++;
			break;
		case 6:
			changed.xid++;
			break;
		case 7:
			changed.slot_wrap++;
			break;
		case 8:
			changed.system_identifier = ++f.typed.system_identifier;
			break;
		case 9:
			changed.database_incarnation = ++f.typed.observer.database_incarnation;
			break;
		case 10:
			f.typed.system_identifier = changed.system_identifier = 0;
			break;
		}
		tt_redo_graph_refuses(&input, &changed, address, CLUSTER_RECOVERY_GRAPH_IDENTITY);
		if (fault == 0 || fault == 1 || fault >= 8)
			UT_ASSERT(!cluster_cold_tt_base_namespace_matches_v1(
				&f.typed, changed.system_identifier, changed.database_incarnation));
		f.typed.system_identifier = key.system_identifier;
		f.typed.observer.database_incarnation = key.database_incarnation;
	}
	f.typed.participants[1].thread_id = 1;
	tt_redo_graph_refuses(&input, &key, address, CLUSTER_RECOVERY_GRAPH_IDENTITY);
	f.typed.participants[1].thread_id = 2;
	for (int fault = 0; fault < 3; fault++) {
		input.participant = fault == 0 ? 1 : (fault == 1 ? 2 : UINT32_MAX);
		tt_redo_graph_refuses(&input, &key, address, CLUSTER_RECOVERY_GRAPH_IDENTITY);
	}
	input.participant = 0;
	f.typed.participants[0].native_redo++;
	tt_redo_graph_refuses(&input, &key, address, CLUSTER_RECOVERY_GRAPH_IDENTITY);
	f.typed.participants[0].native_redo--;
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT_EQ(cluster_cold_typed_tt_redo_graph_read_v1(&input, &key, address, &out, NULL),
				 CLUSTER_RECOVERY_GRAPH_INVALID);
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	generation = UINT32_MAX;
	UT_ASSERT_EQ(cluster_cold_typed_tt_redo_graph_read_v1(&input, &key, address, NULL, &generation),
				 CLUSTER_RECOVERY_GRAPH_INVALID);
	UT_ASSERT_EQ(generation, 0);
	tt_redo_graph_refuses(NULL, &key, address, CLUSTER_RECOVERY_GRAPH_INVALID);
	input.typed = NULL;
	tt_redo_graph_refuses(&input, &key, address, CLUSTER_RECOVERY_GRAPH_INVALID);
	input.typed = &f.typed;
	tt_redo_graph_refuses(&input, NULL, address, CLUSTER_RECOVERY_GRAPH_INVALID);
	tt_redo_graph_refuses(&input, &key, (UBA)InvalidUba_init, CLUSTER_RECOVERY_GRAPH_INVALID);
	tt_redo_graph_refuses(&input, &key, tt_redo_record_address(514, 9, 0),
						  CLUSTER_RECOVERY_GRAPH_IDENTITY);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_graph_second_read_refuses_drift_and_keeps_graph_failure)
{
	TtRedoFixture f;
	PGAlignedBlock page;
	ClusterRecoveryTxnKey key;

	tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
	tt_redo_record_page(&page, 802);
	tt_redo_feed_record_page(&f, 100, &page, true);
	tt_redo_seal(&f);
	key = tt_redo_graph_key(&f);
	tt_redo_graph_walk(&f, &key, tt_redo_record_address(513, 9, 0), 0, true);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_graph_no_fpi_never_falls_back_to_valid_data)
{
	TtRedoFixture f;
	PGAlignedBlock page;
	ClusterRecoveryTxnKey key;
	ClusterColdTtRedoGraphInputV1 input = { &f.typed, 0 };
	char path[MAXPGPATH];
	int fd;

	tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
	tt_redo_record_page(&page, 802);
	snprintf(path, sizeof(path), "%s/513", tt_redo_dir);
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pwrite(fd, page.data, BLCKSZ, 9 * BLCKSZ), BLCKSZ);
	UT_ASSERT_EQ(close(fd), 0);
	tt_redo_seal(&f);
	key = tt_redo_graph_key(&f);
	tt_redo_graph_refuses(&input, &key, tt_redo_record_address(513, 9, 0),
						  CLUSTER_RECOVERY_GRAPH_INVALID);
	UT_ASSERT_NULL(f.typed.tt_base->blocks);
	UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&f.typed), CLUSTER_COLD_OK);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_graph_refuses_raw_record_without_exact_transaction_identity)
{
	for (int fault = 0; fault < 4; fault++) {
		TtRedoFixture f;
		PGAlignedBlock page;
		UndoSlotDirEntry directory;
		UndoRecordHeader record;
		ClusterRecoveryTxnKey key;
		ClusterColdTtRedoGraphInputV1 input = { &f.typed, 0 };
		ClusterRecoveryUndoRecord raw;
		uint32 generation;
		UBA address = tt_redo_record_address(513, 9, 0);

		tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
		tt_redo_record_page(&page, 802);
		memcpy(&directory, page.data + UNDO_SLOT_DIR_OFFSET(0), sizeof(directory));
		memcpy(&record, page.data + directory.record_offset, sizeof(record));
		switch (fault) {
		case 0:
			record.xid++;
			break;
		case 1:
			record.tt_wrap_plus1 = 0;
			break;
		case 2:
			record.tt_wrap_plus1++;
			break;
		case 3:
			record.tt_slot_segment_id++;
			break;
		}
		memcpy(page.data + directory.record_offset, &record, sizeof(record));
		tt_redo_feed_record_page(&f, 100, &page, true);
		tt_redo_seal(&f);
		key = tt_redo_graph_key(&f);
		UT_ASSERT(
			cluster_cold_typed_undo_redo_record_v1(&f.typed, 0, 0, address, &raw, &generation));
		tt_redo_graph_refuses(&input, &key, address, CLUSTER_RECOVERY_GRAPH_IDENTITY);
		tt_redo_destroy(&f);
	}
}

static void
tt_redo_seed_header(uint32 segment, const PGAlignedBlock *page)
{
	char path[MAXPGPATH];
	int fd;

	snprintf(path, sizeof(path), "%s/%u", tt_redo_dir, segment);
	fd = open(path, O_RDWR | O_CREAT, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(ftruncate(fd, UNDO_SEGMENT_SIZE_BYTES), 0);
	UT_ASSERT_EQ(pwrite(fd, page->data, BLCKSZ, 0), BLCKSZ);
	UT_ASSERT_EQ(close(fd), 0);
}

static void
tt_redo_seed_recapture(TtRedoFixture *f)
{
	cluster_cold_tt_base_destroy_v1(&f->typed);
	UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&f->typed, (Size)1024 * 1024), CLUSTER_COLD_OK);
}

static UBA
tt_redo_seed_address(uint32 segment, uint32 block, uint16 slot)
{
	UBA address = tt_redo_record_address(segment, block, 0);

	address.raw[1] = slot;
	return address;
}

static void
tt_redo_seed_record(TtRedoFixture *f, XLogRecPtr begin, uint32 segment, uint32 block, uint16 slot,
					TransactionId xid, uint16 wrap)
{
	PGAlignedBlock page;
	UndoSlotDirEntry directory;
	UndoRecordHeader header;
	xl_undo_block_write carrier = { 0 };
	uint8 bytes[sizeof(carrier) + BLCKSZ];
	FakeXactRecord fake;

	tt_redo_record_page(&page, xid);
	memcpy(&directory, page.data + UNDO_SLOT_DIR_OFFSET(0), sizeof(directory));
	memcpy(&header, page.data + directory.record_offset, sizeof(header));
	header.tt_slot_segment_id = segment;
	header.tt_slot_id = slot + 1;
	header.tt_wrap_plus1 = wrap + 1;
	memcpy(page.data + directory.record_offset, &header, sizeof(header));
	carrier.instance = 3;
	carrier.segment_id = segment;
	carrier.block_no = block;
	carrier.has_fpi = true;
	memcpy(bytes, &carrier, sizeof(carrier));
	memcpy(bytes + sizeof(carrier), page.data, BLCKSZ);
	make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE, bytes, sizeof(bytes));
	undo_header_feed(f->typed.side_plans[0], &fake, begin);
}

static void
tt_redo_seed_refuses(ClusterColdTtRedoGraphInputV1 *input, size_t budget,
					 ClusterRecoveryGraphResult expected)
{
	ClusterRecoveryReferenceGraph *graph = (ClusterRecoveryReferenceGraph *)(uintptr_t)1;

	UT_ASSERT_EQ(cluster_cold_typed_tt_redo_graph_begin_v1(input, budget, &graph), expected);
	UT_ASSERT_NULL(graph);
}

UT_TEST(tt_redo_seed_uses_all_segments_first_and_last_slots_and_terminal_heads)
{
	TtRedoFixture f;
	PGAlignedBlock second, foreign;
	UndoSegmentHeaderData *header;
	FakeXactRecord fake;
	RfSideUndoHeaderImageV1 after;
	ClusterColdTtRedoGraphInputV1 input = { &f.typed, 0 };
	ClusterRecoveryReferenceGraph *graph = NULL;
	ClusterRecoveryGraphRecord record;
	ClusterRecoveryGraphResult result = CLUSTER_RECOVERY_GRAPH_MORE;
	unsigned seen = 0;
	char path[MAXPGPATH];

	tt_redo_fixture(&f, 500, RF_SIDE_PLAN_RECOVERY);
	header = (UndoSegmentHeaderData *)f.base.data;
	header->tt_slots[0] = (TTSlot){ .xid = 800,
									.wrap = 0,
									.status = TT_SLOT_ACTIVE,
									.first_undo_block = tt_redo_seed_address(513, 9, 0) };
	header->tt_slots[47].status = TT_SLOT_ABORTED;
	header->tt_slots[47].first_undo_block = tt_redo_seed_address(513, 10, 47);
	tt_redo_seed_header(513, &f.base);
	cluster_undo_segment_make_header_bytes(514, 3, second.data);
	header = (UndoSegmentHeaderData *)second.data;
	header->wrap_count = 91;
	header->tt_slots[47] = (TTSlot){ .xid = 900,
									 .wrap = 9,
									 .status = TT_SLOT_COMMITTED,
									 .commit_scn = 777,
									 .first_undo_block = tt_redo_seed_address(514, 9, 47) };
	tt_redo_seed_header(514, &second);
	cluster_undo_segment_make_header_bytes(257, 2, foreign.data);
	((UndoSegmentHeaderData *)foreign.data)->tt_slots[47] = header->tt_slots[47];
	((UndoSegmentHeaderData *)foreign.data)->tt_slots[47].first_undo_block
		= tt_redo_seed_address(257, 9, 47);
	tt_redo_seed_header(257, &foreign);
	tt_redo_seed_recapture(&f);
	UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&f.typed), 3);
	tt_redo_seed_record(&f, 100, 513, 9, 0, 800, 0);
	tt_redo_seed_record(&f, 200, 513, 10, 47, 804, 5);
	tt_redo_seed_record(&f, 300, 514, 9, 47, 900, 9);
	undo_header_commit(&fake, 0, 802);
	undo_header_feed(f.typed.side_plans[0], &fake, 400);
	tt_redo_seal(&f);
	UT_ASSERT(cluster_cold_typed_tt_redo_header_v1(&f.typed, 0, 1, &after));
	UT_ASSERT_EQ(((UndoSegmentHeaderData *)after.page.data)->tt_slots[4].status, TT_SLOT_COMMITTED);
	UT_ASSERT_EQ(cluster_cold_typed_tt_redo_graph_begin_v1(&input, 1024 * 1024, &graph),
				 CLUSTER_RECOVERY_GRAPH_MORE);
	UT_ASSERT_NOT_NULL(graph);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_count(graph), 3);
	for (unsigned i = 0; i < 64; i++) {
		result = cluster_recovery_reference_graph_step(graph, &record);
		if (result == CLUSTER_RECOVERY_GRAPH_RECORD) {
			UT_ASSERT_EQ(record.transaction.origin_node_id, 2);
			if (record.transaction.xid == 800) {
				UT_ASSERT_EQ(record.transaction.slot_offset, 0);
				UT_ASSERT_EQ(record.transaction.segment_generation, 0);
				seen |= 1;
			} else if (record.transaction.xid == 804) {
				UT_ASSERT_EQ(record.transaction.slot_offset, 47);
				seen |= 2;
			} else {
				UT_ASSERT_EQ(record.transaction.xid, 900);
				UT_ASSERT_EQ(record.transaction.segment_id, 514);
				UT_ASSERT_EQ(record.transaction.segment_generation, 91);
				UT_ASSERT_EQ(record.transaction.slot_offset, 47);
				seen |= 4;
			}
		} else if (result != CLUSTER_RECOVERY_GRAPH_MORE)
			break;
	}
	UT_ASSERT_EQ(seen, 7);
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_GRAPH_DRAINED);
	UT_ASSERT(cluster_recovery_reference_graph_observed(graph));
	UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&f.typed), CLUSTER_COLD_OK);
	cluster_recovery_reference_graph_destroy(graph);
	for (uint32 segment = 257; segment <= 514; segment += 257) {
		snprintf(path, sizeof(path), "%s/%u", tt_redo_dir, segment);
		UT_ASSERT_EQ(unlink(path), 0);
	}
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_seed_preserves_pre_cut_head_when_another_slot_is_rebound)
{
	TtRedoFixture f;
	FakeXactRecord fake;
	xl_undo_tt_slot_bind bind;
	ClusterColdTtSlotInputV1 slot;
	ClusterColdTtRedoGraphInputV1 input = { &f.typed, 0 };
	ClusterRecoveryReferenceGraph *graph = NULL;
	ClusterRecoveryGraphRecord record;
	ClusterRecoveryGraphResult result = CLUSTER_RECOVERY_GRAPH_MORE;
	unsigned records = 0;

	tt_redo_fixture(&f, 300, RF_SIDE_PLAN_RECOVERY);
	((UndoSegmentHeaderData *)f.base.data)->tt_slots[4].status = TT_SLOT_RECYCLABLE;
	((UndoSegmentHeaderData *)f.base.data)->tt_slots[47].first_undo_block
		= tt_redo_seed_address(513, 9, 47);
	tt_redo_seed_header(513, &f.base);
	tt_redo_seed_recapture(&f);
	undo_header_bind(&fake, 0);
	memcpy(&bind, fake.data, sizeof(bind));
	bind.xid = 806;
	bind.wrap = 8;
	memcpy(fake.data, &bind, sizeof(bind));
	undo_header_feed(f.typed.side_plans[0], &fake, 100);
	tt_redo_seed_record(&f, 200, 513, 9, 47, 804, 5);
	tt_redo_seal(&f);
	UT_ASSERT(cluster_cold_typed_tt_redo_slot_v1(&f.typed, 0, 0, 4, &slot));
	UT_ASSERT_EQ(slot.key.xid, 806);
	UT_ASSERT_EQ(cluster_cold_typed_tt_redo_graph_begin_v1(&input, 1024 * 1024, &graph),
				 CLUSTER_RECOVERY_GRAPH_MORE);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_count(graph), 1);
	for (unsigned i = 0; i < 32; i++) {
		result = cluster_recovery_reference_graph_step(graph, &record);
		if (result == CLUSTER_RECOVERY_GRAPH_RECORD) {
			UT_ASSERT_EQ(record.transaction.xid, 804);
			UT_ASSERT_EQ(record.transaction.slot_offset, 47);
			records++;
		} else if (result != CLUSTER_RECOVERY_GRAPH_MORE)
			break;
	}
	UT_ASSERT_EQ(records, 1);
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_GRAPH_DRAINED);
	cluster_recovery_reference_graph_destroy(graph);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_seed_empty_inventory_still_requires_sealed_exact_source)
{
	TtRedoFixture f;
	ClusterColdTtRedoGraphInputV1 input = { &f.typed, 0 };
	ClusterRecoveryReferenceGraph *graph = NULL;
	ClusterRecoveryGraphRecord record;
	char path[MAXPGPATH];

	tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
	snprintf(path, sizeof(path), "%s/513", tt_redo_dir);
	UT_ASSERT_EQ(unlink(path), 0);
	tt_redo_seed_recapture(&f);
	UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&f.typed), 0);
	tt_redo_seed_refuses(&input, 1024 * 1024, CLUSTER_RECOVERY_GRAPH_IDENTITY);
	tt_redo_seal(&f);
	UT_ASSERT_EQ(cluster_cold_typed_tt_redo_graph_begin_v1(&input, 1024 * 1024, &graph),
				 CLUSTER_RECOVERY_GRAPH_MORE);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_count(graph), 0);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_step(graph, &record),
				 CLUSTER_RECOVERY_GRAPH_DRAINED);
	cluster_recovery_reference_graph_destroy(graph);
	f.typed.participants[0].native_redo++;
	tt_redo_seed_refuses(&input, 1024 * 1024, CLUSTER_RECOVERY_GRAPH_IDENTITY);
	f.typed.participants[0].native_redo--;
	tt_redo_seed_header(513, &f.base);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_seed_empty_inventory_refuses_prepared_input)
{
	TtRedoFixture f;
	FakeXactRecord fake;
	ClusterColdTtRedoGraphInputV1 input = { &f.typed, 0 };
	char path[MAXPGPATH];

	tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
	snprintf(path, sizeof(path), "%s/513", tt_redo_dir);
	UT_ASSERT_EQ(unlink(path), 0);
	tt_redo_seed_recapture(&f);
	make_commit_prepared(&fake, 802, 16384, 901, 123456);
	undo_header_feed(f.typed.side_plans[0], &fake, 100);
	tt_redo_seal(&f);
	tt_redo_seed_refuses(&input, 1024 * 1024, CLUSTER_RECOVERY_GRAPH_IDENTITY);
	tt_redo_seed_header(513, &f.base);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_seed_zero_heads_supply_no_roots)
{
	TtRedoFixture f;
	ClusterColdTtRedoGraphInputV1 input = { &f.typed, 0 };
	ClusterRecoveryReferenceGraph *graph = NULL;
	ClusterRecoveryGraphRecord record;

	tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
	((UndoSegmentHeaderData *)f.base.data)->tt_slots[47].first_undo_block = (UBA)InvalidUba_init;
	tt_redo_seed_header(513, &f.base);
	tt_redo_seed_recapture(&f);
	tt_redo_seal(&f);
	UT_ASSERT_EQ(cluster_cold_typed_tt_redo_graph_begin_v1(&input, 1024 * 1024, &graph),
				 CLUSTER_RECOVERY_GRAPH_MORE);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_count(graph), 0);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_step(graph, &record),
				 CLUSTER_RECOVERY_GRAPH_DRAINED);
	cluster_recovery_reference_graph_destroy(graph);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_seed_refuses_bad_arguments_namespace_source_and_budget)
{
	TtRedoFixture f;
	ClusterColdTtRedoGraphInputV1 input = { &f.typed, 0 };

	tt_redo_seed_refuses(NULL, 1024 * 1024, CLUSTER_RECOVERY_GRAPH_INVALID);
	input.typed = NULL;
	tt_redo_seed_refuses(&input, 1024 * 1024, CLUSTER_RECOVERY_GRAPH_INVALID);
	input.typed = &f.typed;
	tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
	tt_redo_seal(&f);
	UT_ASSERT_EQ(cluster_cold_typed_tt_redo_graph_begin_v1(&input, 1024 * 1024, NULL),
				 CLUSTER_RECOVERY_GRAPH_INVALID);
	tt_redo_seed_refuses(&input, 0, CLUSTER_RECOVERY_GRAPH_CAPACITY);
	f.typed.system_identifier++;
	tt_redo_seed_refuses(&input, 1024 * 1024, CLUSTER_RECOVERY_GRAPH_IDENTITY);
	f.typed.system_identifier--;
	for (uint32 participant = 1; participant <= 2; participant++) {
		input.participant = participant;
		tt_redo_seed_refuses(&input, 1024 * 1024, CLUSTER_RECOVERY_GRAPH_IDENTITY);
	}
	input.participant = UINT32_MAX;
	tt_redo_seed_refuses(&input, 1024 * 1024, CLUSTER_RECOVERY_GRAPH_IDENTITY);
	input.participant = 0;
	f.typed.side_capture_detail[0] = RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	tt_redo_seed_refuses(&input, 1024 * 1024, CLUSTER_RECOVERY_GRAPH_IDENTITY);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_seed_late_header_refusal_discards_partial_graph)
{
	TtRedoFixture f;
	PGAlignedBlock second;
	FakeXactRecord fake;
	xl_xact_tt_commit delta;
	ClusterColdTtRedoGraphInputV1 input = { &f.typed, 0 };
	char path[MAXPGPATH];
	uint32 offset;

	tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
	second = f.base;
	((UndoSegmentHeaderData *)second.data)->segment_id = 514;
	((UndoSegmentHeaderData *)second.data)->tt_slots[4].first_undo_block
		= tt_redo_record_address(514, 9, 0);
	tt_redo_seed_header(514, &second);
	tt_redo_seed_recapture(&f);
	undo_header_commit(&fake, 0, 802);
	offset = fake.u.decoded.main_data_len - sizeof(delta);
	memcpy(&delta, fake.data + offset, sizeof(delta));
	delta.segment_id = 514;
	memcpy(fake.data + offset, &delta, sizeof(delta));
	undo_header_feed(f.typed.side_plans[0], &fake, 100);
	tt_redo_seal(&f);
	tt_redo_seed_refuses(&input, 1024 * 1024, CLUSTER_RECOVERY_GRAPH_IDENTITY);
	snprintf(path, sizeof(path), "%s/514", tt_redo_dir);
	UT_ASSERT_EQ(unlink(path), 0);
	tt_redo_destroy(&f);
}

static void
tt_redo_base_page(const PGAlignedBlock *page)
{
	char path[MAXPGPATH];
	int fd;

	snprintf(path, sizeof(path), "%s/513", tt_redo_dir);
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pwrite(fd, page->data, BLCKSZ, 9 * BLCKSZ), BLCKSZ);
	UT_ASSERT_EQ(close(fd), 0);
}

static void
tt_redo_base_refuses(ClusterColdTypedV1 *typed, uint32 participant, uint32 segment, UBA address)
{
	ClusterRecoveryUndoRecord out, zero = { 0 };
	uint32 generation = UINT32_MAX;

	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_cold_typed_undo_base_redo_record_v1(typed, participant, segment, address,
														   &out, &generation));
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	UT_ASSERT_EQ(generation, 0);
}

UT_TEST(tt_redo_base_only_retains_precheckpoint_records_and_owned_bytes)
{
	TtRedoFixture f;
	PGAlignedBlock page;
	ClusterRecoveryUndoRecord out, saved;
	uint32 generation = UINT32_MAX;
	Size used;
	UBA address = tt_redo_record_address(513, 9, 0);

	tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
	tt_redo_record_page(&page, 802);
	((UndoBlockHeader *)page.data)->block_lsn = 90;
	tt_redo_base_page(&page);
	tt_redo_seal(&f);
	UT_ASSERT(
		cluster_cold_typed_undo_base_redo_record_v1(&f.typed, 0, 0, address, &out, &generation));
	UT_ASSERT_EQ(out.header.xid, 802);
	UT_ASSERT_EQ(out.block_header.block_lsn, 90);
	UT_ASSERT_EQ(generation, 0);
	saved = out;
	used = f.typed.tt_base->used;
	memset(&out, 0x5a, sizeof(out));
	UT_ASSERT(
		cluster_cold_typed_undo_base_redo_record_v1(&f.typed, 0, 0, address, &out, &generation));
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	UT_ASSERT_EQ(f.typed.tt_base->used, used);
	UT_ASSERT_NOT_NULL(f.typed.tt_base->blocks);
	UT_ASSERT(memcmp(f.typed.tt_base->blocks->bytes.data, page.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&f.typed), CLUSTER_COLD_OK);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_base_delta_and_fpi_preserve_order_untouched_rows_and_disk)
{
	for (int fpi = 0; fpi < 2; fpi++) {
		TtRedoFixture f;
		PGAlignedBlock base, page, disk;
		ClusterRecoveryUndoRecord out;
		uint32 generation;
		char path[MAXPGPATH];
		int fd;

		tt_redo_fixture(&f, fpi ? 400 : 200, RF_SIDE_PLAN_RECOVERY);
		tt_redo_record_page(&base, 802);
		((UndoBlockHeader *)base.data)->block_lsn = 100;
		tt_redo_base_page(&base);
		tt_redo_record_page(&page, 812);
		tt_redo_feed_record_page(&f, 100, &page, false);
		if (fpi) {
			tt_redo_record_page(&page, 822);
			tt_redo_feed_record_page(&f, 200, &page, true);
			tt_redo_record_page(&page, 832);
			tt_redo_feed_record_page(&f, 300, &page, false);
		}
		tt_redo_seal(&f);
		UT_ASSERT(cluster_cold_typed_undo_base_redo_record_v1(
			&f.typed, 0, 0, tt_redo_record_address(513, 9, 0), &out, &generation));
		UT_ASSERT_EQ(out.header.xid, fpi ? 832 : 812);
		UT_ASSERT_EQ(out.block_header.block_lsn, fpi ? 400 : 200);
		UT_ASSERT_EQ(generation, 0);
		UT_ASSERT(cluster_cold_typed_undo_base_redo_record_v1(
			&f.typed, 0, 0, tt_redo_record_address(513, 9, 1), &out, &generation));
		UT_ASSERT_EQ(out.header.xid, fpi ? 823 : 803);
		snprintf(path, sizeof(path), "%s/513", tt_redo_dir);
		fd = open(path, O_RDONLY);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(pread(fd, disk.data, BLCKSZ, 9 * BLCKSZ), BLCKSZ);
		UT_ASSERT_EQ(close(fd), 0);
		UT_ASSERT(memcmp(disk.data, base.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&f.typed), CLUSTER_COLD_OK);
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_redo_base_refuses_zero_ahead_malformed_and_missing_original)
{
	for (int fault = 0; fault < 4; fault++) {
		TtRedoFixture f;
		PGAlignedBlock page;

		tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
		tt_redo_record_page(&page, 802);
		((UndoBlockHeader *)page.data)->block_lsn = fault == 0 ? 0 : (fault == 1 ? 101 : 90);
		if (fault == 2)
			((UndoBlockHeader *)page.data)->magic = 0;
		if (fault != 3)
			tt_redo_base_page(&page);
		tt_redo_seal(&f);
		tt_redo_base_refuses(&f.typed, 0, 0, tt_redo_record_address(513, 9, 0));
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_redo_base_delta_introduces_record_in_captured_nonzero_generation)
{
	TtRedoFixture f;
	PGAlignedBlock base, page;
	UndoBlockHeader *header = (UndoBlockHeader *)base.data;
	UndoSlotDirEntry directory;
	ClusterRecoveryUndoRecord out;
	uint32 generation;
	UBA address = tt_redo_record_address(513, 9, 0);

	tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
	((UndoSegmentHeaderData *)f.base.data)->wrap_count = 17;
	tt_redo_seed_header(513, &f.base);
	tt_redo_seed_recapture(&f);
	memset(&base, 0, sizeof(base));
	header->magic = PGRAC_UNDO_BLOCK_MAGIC;
	header->block_version = UNDO_BLOCK_VERSION_1;
	header->block_lsn = 90;
	header->free_offset = sizeof(*header);
	tt_redo_base_page(&base);
	UT_ASSERT(cluster_recovery_undo_record_read(base.data, BLCKSZ, 513, 9, 3, address, &out)
			  != CLUSTER_RECOVERY_UNDO_ENTRY);
	tt_redo_record_page(&page, 802);
	memcpy(&directory, page.data + UNDO_SLOT_DIR_OFFSET(0), sizeof(directory));
	((UndoBlockHeader *)page.data)->slot_count = 1;
	((UndoBlockHeader *)page.data)->free_offset = directory.record_offset + directory.record_length;
	tt_redo_feed_record_page(&f, 100, &page, false);
	tt_redo_seal(&f);
	UT_ASSERT(
		cluster_cold_typed_undo_base_redo_record_v1(&f.typed, 0, 0, address, &out, &generation));
	UT_ASSERT_EQ(out.header.xid, 802);
	UT_ASSERT_EQ(generation, 17);
	UT_ASSERT_EQ(out.block_header.block_lsn, 200);
	UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&f.typed), CLUSTER_COLD_OK);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_base_rechecks_full_bytes_on_every_read_and_final_recheck)
{
	for (int final = 0; final < 2; final++) {
		TtRedoFixture f;
		PGAlignedBlock page;
		ClusterRecoveryUndoRecord out;
		uint32 generation;
		UBA address = tt_redo_record_address(513, 9, 0);

		tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
		tt_redo_record_page(&page, 802);
		((UndoBlockHeader *)page.data)->block_lsn = 90;
		tt_redo_base_page(&page);
		tt_redo_feed_record_page(&f, 100, &page, false);
		tt_redo_seal(&f);
		UT_ASSERT(cluster_cold_typed_undo_base_redo_record_v1(&f.typed, 0, 0, address, &out,
															  &generation));
		page.data[BLCKSZ / 2] ^= 1; /* Not a requested record or redo delta byte. */
		tt_redo_base_page(&page);
		if (!final)
			tt_redo_base_refuses(&f.typed, 0, 0, address);
		UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&f.typed), CLUSTER_COLD_SOURCE_GAP);
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_redo_base_refuses_capacity_and_source_drift)
{
	for (int fault = 0; fault < 3; fault++) {
		TtRedoFixture f;
		PGAlignedBlock page;

		tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
		tt_redo_record_page(&page, 802);
		((UndoBlockHeader *)page.data)->block_lsn = 90;
		tt_redo_base_page(&page);
		tt_redo_seal(&f);
		if (fault == 0)
			f.typed.tt_base->budget = f.typed.tt_base->used;
		else if (fault == 1)
			f.typed.participants[0].native_redo++;
		else
			f.typed.participants[0].owner_incarnation++;
		tt_redo_base_refuses(&f.typed, 0, 0, tt_redo_record_address(513, 9, 0));
		if (fault == 0)
			UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&f.typed), CLUSTER_COLD_CAPACITY);
		else
			UT_ASSERT_NULL(f.typed.tt_base->blocks);
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_redo_base_refuses_lifecycle_even_with_a_later_fpi)
{
	TtRedoFixture f;
	PGAlignedBlock page;
	FakeXactRecord fake;
	struct {
		xl_undo_segment_reuse record;
		char page[BLCKSZ];
	} reuse = { 0 };

	tt_redo_fixture(&f, 300, RF_SIDE_PLAN_RECOVERY);
	cluster_undo_segment_make_header_bytes(513, 3, f.base.data);
	tt_redo_seed_header(513, &f.base);
	tt_redo_seed_recapture(&f);
	tt_redo_record_page(&page, 802);
	((UndoBlockHeader *)page.data)->block_lsn = 90;
	tt_redo_base_page(&page);
	reuse.record.instance = 3;
	reuse.record.segment_id = 513;
	reuse.record.new_generation = 1;
	cluster_undo_segment_make_header_bytes(513, 3, reuse.page);
	((UndoSegmentHeaderData *)reuse.page)->wrap_count = 1;
	make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_REUSE, &reuse,
						   sizeof(reuse));
	undo_header_feed(f.typed.side_plans[0], &fake, 100);
	tt_redo_feed_record_page(&f, 200, &page, true);
	tt_redo_seal(&f);
	tt_redo_base_refuses(&f.typed, 0, 0, tt_redo_record_address(513, 9, 0));
	UT_ASSERT_NULL(f.typed.tt_base->blocks);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_base_refuses_bad_arguments_with_cleared_outputs)
{
	TtRedoFixture f;
	ClusterRecoveryUndoRecord out, zero = { 0 };
	uint32 generation;
	UBA address = tt_redo_record_address(513, 9, 0);

	tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
	tt_redo_base_refuses(&f.typed, 0, 0, address); /* Unsealed. */
	tt_redo_seal(&f);
	tt_redo_base_refuses(NULL, 0, 0, address);
	tt_redo_base_refuses(&f.typed, 1, 0, address); /* History. */
	tt_redo_base_refuses(&f.typed, 0, UINT32_MAX, address);
	tt_redo_base_refuses(&f.typed, 0, 0, (UBA)InvalidUba_init);
	tt_redo_base_refuses(&f.typed, 0, 0, tt_redo_record_address(514, 9, 0));
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_cold_typed_undo_base_redo_record_v1(&f.typed, 0, 0, address, &out, NULL));
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	generation = UINT32_MAX;
	UT_ASSERT(
		!cluster_cold_typed_undo_base_redo_record_v1(&f.typed, 0, 0, address, NULL, &generation));
	UT_ASSERT_EQ(generation, 0);
	tt_redo_destroy(&f);
}

int
main(void)
{
	UT_PLAN(45);
	UT_RUN(tt_redo_base_only_retains_precheckpoint_records_and_owned_bytes);
	UT_RUN(tt_redo_base_delta_and_fpi_preserve_order_untouched_rows_and_disk);
	UT_RUN(tt_redo_base_refuses_zero_ahead_malformed_and_missing_original);
	UT_RUN(tt_redo_base_delta_introduces_record_in_captured_nonzero_generation);
	UT_RUN(tt_redo_base_rechecks_full_bytes_on_every_read_and_final_recheck);
	UT_RUN(tt_redo_base_refuses_capacity_and_source_drift);
	UT_RUN(tt_redo_base_refuses_lifecycle_even_with_a_later_fpi);
	UT_RUN(tt_redo_base_refuses_bad_arguments_with_cleared_outputs);
	UT_RUN(tt_redo_commit_without_bind_keeps_original_all_slots_and_heads);
	UT_RUN(tt_redo_unrelated_terminal_preserves_active_input);
	UT_RUN(tt_redo_empty_cut_preserves_input_and_requires_canonical_flags);
	UT_RUN(tt_redo_exact_abort_uses_original_active_slot_without_bind);
	UT_RUN(tt_redo_rejects_each_source_and_checkpoint_identity_drift);
	UT_RUN(tt_redo_rejects_incomplete_capture_plan_and_invalid_bounds);
	UT_RUN(tt_redo_refuses_native_commit_identity_mismatch_without_output);
	UT_RUN(tt_redo_refuses_pi_purpose);
	UT_RUN(tt_redo_refuses_prepared_even_without_matching_header_effect);
	UT_RUN(tt_redo_refuses_c0_in_ordinary_only_input);
	UT_RUN(tt_redo_retains_native_refusal_of_unretired_active_head);
	UT_RUN(tt_redo_refuses_standalone_prepared_tt_without_xact_terminal);
	UT_RUN(tt_redo_slots_enumerate_every_status_and_preserve_generation_zero);
	UT_RUN(tt_redo_slot_reads_commit_and_matches_after_image_independently);
	UT_RUN(tt_redo_slot_reads_exact_abort_without_loser_inference);
	UT_RUN(tt_redo_match_refuses_each_stale_key_field_and_clears_output);
	UT_RUN(tt_redo_slots_reject_unsealed_history_bounds_and_invalid_arguments);
	UT_RUN(tt_redo_slots_refuse_source_drift_and_unsupported_prepared_input);
	UT_RUN(tt_redo_slots_use_reused_binding_instead_of_captured_key);
	UT_RUN(tt_redo_undo_uses_latest_fpi_and_delta_without_installing_or_borrowing);
	UT_RUN(tt_redo_undo_refuses_missing_or_late_anchor_even_with_valid_data);
	UT_RUN(tt_redo_undo_refuses_bad_bounds_source_and_arguments_with_cleared_outputs);
	UT_RUN(tt_redo_undo_refuses_malformed_fpi_record);
	UT_RUN(tt_redo_undo_requires_new_fpi_after_segment_reuse_and_returns_new_generation);
	UT_RUN(tt_redo_graph_uses_rebound_slot_and_owned_fpi_bytes_at_generation_zero);
	UT_RUN(tt_redo_graph_cross_segment_record_keeps_physical_generation);
	UT_RUN(tt_redo_graph_refuses_namespace_key_source_and_argument_drift);
	UT_RUN(tt_redo_graph_second_read_refuses_drift_and_keeps_graph_failure);
	UT_RUN(tt_redo_graph_no_fpi_never_falls_back_to_valid_data);
	UT_RUN(tt_redo_graph_refuses_raw_record_without_exact_transaction_identity);
	UT_RUN(tt_redo_seed_uses_all_segments_first_and_last_slots_and_terminal_heads);
	UT_RUN(tt_redo_seed_preserves_pre_cut_head_when_another_slot_is_rebound);
	UT_RUN(tt_redo_seed_empty_inventory_still_requires_sealed_exact_source);
	UT_RUN(tt_redo_seed_empty_inventory_refuses_prepared_input);
	UT_RUN(tt_redo_seed_zero_heads_supply_no_roots);
	UT_RUN(tt_redo_seed_refuses_bad_arguments_namespace_source_and_budget);
	UT_RUN(tt_redo_seed_late_header_refusal_discards_partial_graph);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
