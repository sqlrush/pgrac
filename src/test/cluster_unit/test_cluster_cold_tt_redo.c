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
tt_redo_history_plan(TtRedoFixture *f, XLogRecPtr begin, XLogRecPtr end, XLogRecPtr redo,
					 RfSidePlanPurposeV1 purpose, bool seal)
{
	RfContributorStreamCutV1 cut = { 0 };
	RfSideOnlinePlanRequestV1 request = { 0 };
	ClusterColdParticipantV1 *source = &f->typed.participants[1];

	rf_side_online_plan_destroy_v1(&f->typed.side_plans[1]);
	source->physical_lower = begin;
	source->native_redo = redo;
	source->tail_end = end;
	cut.failed_thread = source->thread_id;
	cut.timeline_id = source->timeline;
	cut.origin_owner_incarnation = source->owner_incarnation;
	cut.scan_begin_inclusive = begin;
	cut.scan_end_exclusive = end;
	cut.flags
		= RF_CONTRIBUTOR_CUT_COMPLETE | (begin == end ? RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY : 0);
	request.system_identifier = f->typed.system_identifier;
	memcpy(request.storage_uuid, f->typed.observer.storage_uuid, 16);
	request.physical_cuts = &cut;
	request.participant_count = 1;
	request.purpose = purpose;
	request.redo_starts = purpose == RF_SIDE_PLAN_RECOVERY ? &source->native_redo : NULL;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &f->typed.side_plans[1]),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_bind_sources_v1(f->typed.side_plans[1], f->typed.sources, 2));
	UT_ASSERT(rf_side_online_plan_bind_database_v1(f->typed.side_plans[1], 42));
	if (seal)
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(f->typed.side_plans[1]), RF_PAGE_PROOF_DETAIL_OK);
}

static void
tt_redo_history_feed(TtRedoFixture *f, FakeXactRecord *fake)
{
	RfPageOnlineRecordIdentityV1 identity = make_identity(fake, f->typed.observer.storage_uuid);
	RfDetachedRecordPlanV1 record;

	identity.record.origin_thread = f->typed.participants[1].thread_id;
	set_identity_range(fake, &identity, 100, 200);
	record = XLogRecGetRmid(&fake->reader) == RM_CLUSTER_UNDO_ID ? make_undo_record_plan(fake)
																 : make_record_plan(fake);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(f->typed.side_plans[1], &record, &identity),
				 RF_PAGE_PROOF_DETAIL_OK);
}

static void
tt_redo_fixture_at(TtRedoFixture *f, XLogRecPtr end, RfSidePlanPurposeV1 purpose, XLogRecPtr redo)
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
														   .native_redo = redo,
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
	tt_redo_history_plan(f, 100, 100, 100, RF_SIDE_PLAN_RECOVERY, true);
}

static void
tt_redo_fixture(TtRedoFixture *f, XLogRecPtr end, RfSidePlanPurposeV1 purpose)
{
	tt_redo_fixture_at(f, end, purpose, 100);
}

static void
tt_redo_destroy(TtRedoFixture *f)
{
	char path[MAXPGPATH];

	rf_side_online_plan_destroy_v1(&f->typed.side_plans[0]);
	rf_side_online_plan_destroy_v1(&f->typed.side_plans[1]);
	cluster_cold_tt_base_destroy_v1(&f->typed);
	cluster_cold_plan_destroy_v1(&f->typed.plan);
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

UT_TEST(tt_redo_rejects_exact_root_claim_drift_with_unchanged_cut)
{
	TtRedoFixture f;
	ClusterWalSourceRef saved;

	tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
	tt_redo_seal(&f);
	saved = f.typed.sources[0];
	for (int fault = 0; fault < 15; fault++) {
		ClusterWalThreadClaimRefV2 *ref = &f.typed.sources[0].claim;

		f.typed.sources[0] = saved;
		if (fault == 0)
			ref->identity.system_identifier++;
		if (fault == 1)
			ref->identity.storage_uuid[0]++;
		if (fault == 2)
			ref->identity.authority_uuid[0]++;
		if (fault == 3)
			ref->identity.origin_thread_id = 2;
		if (fault == 4)
			ref->identity.reserved42 = 1;
		if (fault == 5)
			ref->identity.origin_node_id++;
		if (fault == 6)
			ref->identity.thread_claim_created_at++;
		if (fault == 7)
			ref->identity.thread_claim_crc32c++;
		if (fault == 8)
			ref->identity.reserved60 = 1;
		if (fault == 9)
			ref->identity.origin_owner_incarnation++;
		if (fault == 10)
			ref->identity.root_lineage_seq++;
		if (fault == 11)
			ref->database_incarnation++;
		if (fault == 12)
			ref->max_config_generation++;
		if (fault == 13)
			ref->claim_sha256[0]++;
		if (fault == 14)
			f.typed.sources[0].timeline++;
		tt_redo_refuses(&f, 0, 0);
	}
	f.typed.sources[0] = saved;
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_claim_matches_bound_copy_in_any_order_not_history)
{
	TtRedoFixture f;
	RfSideOnlinePlanRequestV1 request = { 0 };
	RfSideOnlinePlanV1 *plan = NULL;
	ClusterWalSourceRef bound[2];
	RfSideUndoHeaderImageV1 out;

	tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
	bound[0] = f.typed.sources[1];
	bound[1] = f.typed.sources[0];
	request.system_identifier = f.typed.system_identifier;
	memcpy(request.storage_uuid, f.typed.observer.storage_uuid, 16);
	request.physical_cuts = &f.cut;
	request.participant_count = 1;
	request.purpose = RF_SIDE_PLAN_RECOVERY;
	request.redo_starts = &f.typed.participants[0].native_redo;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_bind_sources_v1(plan, bound, 2));
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT(!rf_side_online_plan_recovery_claim_matches_v1(plan, &f.typed.sources[0]));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	bound[1].claim.claim_sha256[0]++; /* The plan must own its original copy. */
	UT_ASSERT(rf_side_online_plan_recovery_claim_matches_v1(plan, &f.typed.sources[0]));
	UT_ASSERT(!rf_side_online_plan_recovery_claim_matches_v1(plan, &bound[1]));
	UT_ASSERT(!rf_side_online_plan_recovery_claim_matches_v1(plan, &f.typed.sources[1]));
	rf_side_online_plan_destroy_v1(&f.typed.side_plans[0]);
	f.typed.side_plans[0] = plan;
	UT_ASSERT(cluster_cold_typed_tt_redo_header_v1(&f.typed, 0, 0, &out));
	UT_ASSERT(memcmp(out.page.data, f.base.data, BLCKSZ) == 0);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_claim_match_refuses_unbound_pi_and_bad_arguments)
{
	for (int scenario = 0; scenario < 2; scenario++) {
		TtRedoFixture f;
		RfSideOnlinePlanRequestV1 request = { 0 };
		RfSideOnlinePlanV1 *plan = NULL;

		tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
		request.system_identifier = f.typed.system_identifier;
		memcpy(request.storage_uuid, f.typed.observer.storage_uuid, 16);
		request.physical_cuts = &f.cut;
		request.participant_count = 1;
		request.purpose = scenario == 0 ? RF_SIDE_PLAN_RECOVERY : RF_SIDE_PLAN_PI_CONTRIBUTIONS;
		request.redo_starts = scenario == 0 ? &f.typed.participants[0].native_redo : NULL;
		UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
		if (scenario == 1)
			UT_ASSERT(rf_side_online_plan_bind_sources_v1(plan, f.typed.sources, 2));
		UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
		UT_ASSERT(!rf_side_online_plan_recovery_claim_matches_v1(plan, &f.typed.sources[0]));
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(!rf_side_online_plan_recovery_claim_matches_v1(plan, &f.typed.sources[0]));
		UT_ASSERT(!rf_side_online_plan_recovery_claim_matches_v1(NULL, &f.typed.sources[0]));
		UT_ASSERT(!rf_side_online_plan_recovery_claim_matches_v1(plan, NULL));
		if (scenario == 0) {
			/* An unchanged, complete cut and redo cannot replace the claim. */
			UT_ASSERT(rf_side_online_plan_recovery_source_matches_v1(
				plan, f.typed.system_identifier, f.typed.observer.storage_uuid, &f.cut));
			UT_ASSERT(rf_side_online_plan_replay_start_matches_v1(plan, 3, 33, 42, 100));
		}
		rf_side_online_plan_destroy_v1(&f.typed.side_plans[0]);
		f.typed.side_plans[0] = plan;
		tt_redo_refuses(&f, 0, 0);
		tt_redo_destroy(&f);
	}
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

UT_TEST(tt_redo_active_identity_requires_post_redo_active_physical_slot)
{
	TtRedoFixture f;
	FakeXactRecord fake;
	ClusterColdTtSlotInputV1 input;
	ClusterColdTtActiveIdentityV1 identity, zero = { 0 };

	/* An ACTIVE slot yields only its physical key. No FullXid or epoch input
	 * exists on this path, so a later TopTxProof can add those gates. */
	tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
	tt_redo_seal(&f);
	UT_ASSERT(cluster_cold_typed_tt_redo_slot_v1(&f.typed, 0, 0, 47, &input));
	memset(&identity, 0xa9, sizeof(identity));
	UT_ASSERT(cluster_cold_typed_tt_active_identity_v1(&f.typed, 0, 0, &input.key,
										 &identity));
	UT_ASSERT_EQ(identity.key.owner_instance, input.key.owner_instance);
	UT_ASSERT_EQ(identity.key.segment_id, input.key.segment_id);
	UT_ASSERT_EQ(identity.key.segment_generation, input.key.segment_generation);
	UT_ASSERT_EQ(identity.key.slot_offset, input.key.slot_offset);
	UT_ASSERT_EQ(identity.key.xid, input.key.xid);
	UT_ASSERT_EQ(identity.key.slot_wrap, input.key.slot_wrap);
	tt_redo_destroy(&f);

	/* Terminal status is a physical refusal, regardless of the raw key still
	 * matching the captured slot identity. */
	for (int terminal = 0; terminal < 2; terminal++) {
		tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
		UT_ASSERT(cluster_cold_tt_base_slot_v1(&f.typed, 0, 4, &input));
		if (terminal == 0)
			undo_header_commit(&fake, 0, 802);
		else
			undo_header_abort(&fake, 4);
		undo_header_feed(f.typed.side_plans[0], &fake, 100);
		tt_redo_seal(&f);
		memset(&identity, 0xa9, sizeof(identity));
		UT_ASSERT(!cluster_cold_typed_tt_active_identity_v1(&f.typed, 0, 0, &input.key,
											&identity));
		UT_ASSERT(memcmp(&identity, &zero, sizeof(identity)) == 0);
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_top_tx_loser_requires_all_proof_gates_and_ignores_full_xid)
{
	TtRedoFixture f;
	ClusterColdTtSlotInputV1 input;
	ClusterColdTtActiveIdentityV1 active;
	ClusterColdTopTxProofV1 proof = { 0 };
	ClusterColdLoserIdentityV1 loser, zero = { 0 };

	tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
	tt_redo_seal(&f);
	UT_ASSERT(cluster_cold_typed_tt_redo_slot_v1(&f.typed, 0, 0, 47, &input));
	UT_ASSERT(cluster_cold_typed_tt_active_identity_v1(&f.typed, 0, 0, &input.key,
									 &active));
	proof.key = active.key;
	proof.producer_top = true;
	proof.qualified_base = true;
	proof.sealed_redo = true;
	proof.ordinary_effects = true;
	proof.old_writer_isolated = true;
	proof.terminal_match = true;
	/* No FullXid, nextXid or epoch is an input to the physical loser gate. */
	UT_ASSERT(cluster_cold_top_tx_loser_v1(&active, &proof, &loser));
	UT_ASSERT(memcmp(&loser.key, &active.key, sizeof(loser.key)) == 0);

	for (int gate = 0; gate < 6; gate++) {
		ClusterColdTopTxProofV1 rejected = proof;

		switch (gate) {
			case 0:
				rejected.producer_top = false;
				break;
			case 1:
				rejected.qualified_base = false;
				break;
			case 2:
				rejected.sealed_redo = false;
				break;
			case 3:
				rejected.ordinary_effects = false;
				break;
			case 4:
				rejected.old_writer_isolated = false;
				break;
			case 5:
				rejected.terminal_match = false;
				break;
		}
		memset(&loser, 0xa9, sizeof(loser));
		UT_ASSERT(!cluster_cold_top_tx_loser_v1(&active, &rejected, &loser));
		UT_ASSERT(memcmp(&loser, &zero, sizeof(loser)) == 0);
	}
	proof.key.slot_offset++;
	memset(&loser, 0xa9, sizeof(loser));
	UT_ASSERT(!cluster_cold_top_tx_loser_v1(&active, &proof, &loser));
	UT_ASSERT(memcmp(&loser, &zero, sizeof(loser)) == 0);
	tt_redo_destroy(&f);
}

UT_TEST(tt_top_tx_c0_requires_physical_loser_and_complete_endpoints)
{
	TtRedoFixture f;
	ClusterColdTtSlotInputV1 input;
	ClusterColdTtActiveIdentityV1 active;
	ClusterColdTopTxProofV1 proof = { 0 };
	ClusterColdLoserIdentityV1 loser;
	ClusterRecoveryTTAbortV1 shape = { 0 };
	ClusterRecoveryTTAbortDerivationV1 derived, zero = { 0 };
	uint64 next_xid = UINT64_C(0x700400200);

	tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
	tt_redo_seal(&f);
	UT_ASSERT(cluster_cold_typed_tt_redo_slot_v1(&f.typed, 0, 0, 47, &input));
	UT_ASSERT(cluster_cold_typed_tt_active_identity_v1(&f.typed, 0, 0, &input.key,
									 &active));
	proof.key = active.key;
	proof.producer_top = true;
	proof.qualified_base = true;
	proof.sealed_redo = true;
	proof.ordinary_effects = true;
	proof.old_writer_isolated = true;
	proof.terminal_match = true;
	UT_ASSERT(cluster_cold_top_tx_loser_v1(&active, &proof, &loser));

	shape.version = CLUSTER_RECOVERY_TT_ABORT_VERSION;
	shape.length = CLUSTER_RECOVERY_TT_ABORT_BYTES;
	shape.flags = CLUSTER_RECOVERY_TT_ABORT_TOP_KNOWN;
	shape.target_thread = loser.key.owner_instance;
	shape.writer_thread = 1;
	shape.target_incarnation = 11;
	shape.writer_incarnation = 12;
	shape.recovery_serial = 13;
	shape.target_epoch = 1;
	memset(shape.target_claim_sha256, 0x5a, sizeof(shape.target_claim_sha256));
	shape.exact.segment_id = loser.key.segment_id;
	shape.exact.segment_generation = loser.key.segment_generation;
	shape.exact.xid = loser.key.xid;
	shape.exact.slot_offset = loser.key.slot_offset;
	shape.exact.wrap = loser.key.slot_wrap;
	shape.exact.instance = loser.key.owner_instance;
	shape.exact.format_version = CLUSTER_UNDO_TT_ABORT_EXACT_VERSION;
	shape.before.xid = loser.key.xid;
	shape.before.wrap = loser.key.slot_wrap;
	shape.before.status = TT_SLOT_ACTIVE;
	shape.before.flags = TT_FLAGS_RESERVED;
	shape.before.first_undo_block = (UBA) InvalidUba_init;
	UT_ASSERT(cluster_recovery_tt_abort_shape_valid_v1(&shape));
	UT_ASSERT(cluster_cold_top_tx_c0_v1(&loser, &shape, next_xid, true, false,
								 InvalidTransactionId, UINT64_C(0x12345678), &derived));
	UT_ASSERT_EQ(derived.record.target_epoch, UINT64_C(0x12345678));
	UT_ASSERT_EQ(derived.record.target_full_xid, derived.record.top_full_xid);
	UT_ASSERT_EQ((uint32) derived.record.target_full_xid, loser.key.xid);
	UT_ASSERT(derived.oldest_xid_dependency);

	memset(&derived, 0xa9, sizeof(derived));
	UT_ASSERT(!cluster_cold_top_tx_c0_v1(&loser, &shape, next_xid, false, false,
								  InvalidTransactionId, UINT64_C(0x12345678), &derived));
	UT_ASSERT(memcmp(&derived, &zero, sizeof(derived)) == 0);
	shape.exact.slot_offset++;
	UT_ASSERT(!cluster_cold_top_tx_c0_v1(&loser, &shape, next_xid, true, false,
								  InvalidTransactionId, UINT64_C(0x12345678), &derived));
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
tt_redo_feed_record_page_at(TtRedoFixture *f, XLogRecPtr begin, uint32 segment, uint32 block,
							const PGAlignedBlock *page, bool full_image)
{
	FakeXactRecord fake;
	xl_undo_block_write carrier = { 0 };
	uint8 bytes[sizeof(carrier) + BLCKSZ];
	uint32 length = sizeof(carrier);

	carrier.instance = 3;
	carrier.segment_id = segment;
	carrier.block_no = block;
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
tt_redo_feed_record_page(TtRedoFixture *f, XLogRecPtr begin, const PGAlignedBlock *page,
						 bool full_image)
{
	tt_redo_feed_record_page_at(f, begin, 513, 9, page, full_image);
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

static void
tt_redo_base_prefix_lifecycle(bool reused)
{
	for (int replay = 0; replay < 3; replay++) {
		TtRedoFixture f;
		PGAlignedBlock base, page, folded;
		FakeXactRecord fake;
		RfSideOnlineOperationV1 operation;
		RfSideUndoHeaderImageV1 header;
		ClusterRecoveryUndoRecord out;
		uint32 generation;
		struct {
			xl_cluster_undo_segment_init record;
			char page[BLCKSZ];
		} init = { 0 };
		struct {
			xl_undo_segment_reuse record;
			char page[BLCKSZ];
		} reuse = { 0 };

		tt_redo_fixture_at(&f, replay == 0 ? 300 : 400, RF_SIDE_PLAN_RECOVERY, 300);
		if (reused) {
			((UndoSegmentHeaderData *)f.base.data)->wrap_count = 1;
			reuse.record.instance = 3;
			reuse.record.segment_id = 513;
			reuse.record.new_generation = 1;
			cluster_undo_segment_make_header_bytes(513, 3, reuse.page);
			((UndoSegmentHeaderData *)reuse.page)->wrap_count = 1;
			make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_REUSE, &reuse,
								   sizeof(reuse));
		} else {
			init.record.instance = 3;
			init.record.segment_id = 513;
			cluster_undo_segment_make_header_bytes(513, 3, init.page);
			make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_INIT, &init,
								   sizeof(init));
		}
		undo_header_feed(f.typed.side_plans[0], &fake, 100);
		/* Retain the full native prefix required by the existing TT fold.
		 * This DATA ancestry test does not grant checkpoint coverage. */
		cluster_undo_segment_make_header_bytes(513, 3, f.base.data);
		((UndoSegmentHeaderData *)f.base.data)->wrap_count = reused ? 1 : 0;
		((UndoSegmentHeaderData *)f.base.data)->tt_slots[4].xid = 802;
		((UndoSegmentHeaderData *)f.base.data)->tt_slots[4].wrap = 7;
		((UndoSegmentHeaderData *)f.base.data)->tt_slots[4].status = TT_SLOT_ACTIVE;
		((UndoSegmentHeaderData *)f.base.data)->tt_slots[4].first_undo_block = (UBA)InvalidUba_init;
		tt_redo_seed_header(513, &f.base);
		tt_redo_seed_recapture(&f);
		undo_header_bind(&fake, reused ? 1 : 0);
		undo_header_feed(f.typed.side_plans[0], &fake, 200);
		tt_redo_record_page(&base, 802);
		((UndoBlockHeader *)base.data)->block_lsn = 280;
		tt_redo_base_page(&base);
		page = base;
		if (replay != 0) {
			tt_redo_record_page(&page, 812);
			tt_redo_feed_record_page(&f, 300, &page, replay == 2);
		}
		tt_redo_seal(&f);
		UT_ASSERT(rf_side_online_plan_operation_v1(f.typed.side_plans[0], 0, &operation));
		UT_ASSERT(operation.history_only); /* The native redo boundary owns this split. */
		if (replay != 0) {
			UT_ASSERT(rf_side_online_plan_operation_v1(f.typed.side_plans[0], 2, &operation));
			UT_ASSERT(!operation.history_only);
		}
		UT_ASSERT(cluster_cold_typed_tt_redo_header_v1(&f.typed, 0, 0, &header));
		UT_ASSERT(memcmp(header.page.data, f.base.data, BLCKSZ) == 0);
		UT_ASSERT(cluster_cold_typed_undo_base_redo_block_v1(&f.typed, 0, 0, 9, &base, &folded));
		UT_ASSERT_EQ(((UndoBlockHeader *)folded.data)->block_lsn, replay == 0 ? 280 : 400);
		UT_ASSERT(cluster_cold_typed_undo_base_redo_record_v1(
			&f.typed, 0, 0, tt_redo_record_address(513, 9, 0), &out, &generation));
		UT_ASSERT_EQ(out.header.xid, replay == 0 ? 802 : 812);
		UT_ASSERT_EQ(generation, reused ? 1 : 0);
		UT_ASSERT(cluster_cold_typed_undo_base_redo_record_v1(
			&f.typed, 0, 0, tt_redo_record_address(513, 9, 1), &out, &generation));
		UT_ASSERT_EQ(out.header.xid, replay == 2 ? 813 : 803);
		UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&f.typed), CLUSTER_COLD_OK);
		tt_redo_destroy(&f);
	}
}


UT_TEST(tt_redo_base_refuses_prefix_without_captured_slot_history)
{
	for (int reused = 0; reused < 2; reused++) {
		TtRedoFixture f;
		FakeXactRecord fake;
		RfSideUndoHeaderImageV1 header, saved;
		PGAlignedBlock base, out;
		struct {
			xl_cluster_undo_segment_init record;
			char page[BLCKSZ];
		} init = { 0 };
		struct {
			xl_undo_segment_reuse record;
			char page[BLCKSZ];
		} reuse = { 0 };

		tt_redo_fixture_at(&f, 200, RF_SIDE_PLAN_RECOVERY, 200);
		if (reused) {
			((UndoSegmentHeaderData *)f.base.data)->wrap_count = 1;
			tt_redo_seed_header(513, &f.base);
			tt_redo_seed_recapture(&f);
			reuse.record.instance = 3;
			reuse.record.segment_id = 513;
			reuse.record.new_generation = 1;
			cluster_undo_segment_make_header_bytes(513, 3, reuse.page);
			((UndoSegmentHeaderData *)reuse.page)->wrap_count = 1;
			make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_REUSE, &reuse,
								   sizeof(reuse));
		} else {
			init.record.instance = 3;
			init.record.segment_id = 513;
			cluster_undo_segment_make_header_bytes(513, 3, init.page);
			make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_INIT, &init,
								   sizeof(init));
		}
		undo_header_feed(f.typed.side_plans[0], &fake, 100);
		tt_redo_record_page(&base, 802);
		((UndoBlockHeader *)base.data)->block_lsn = 180;
		tt_redo_base_page(&base);
		tt_redo_seal(&f);
		memset(&header, 0xa7, sizeof(header));
		saved = header;
		UT_ASSERT(!cluster_cold_typed_tt_redo_header_v1(&f.typed, 0, 0, &header));
		UT_ASSERT(memcmp(&header, &saved, sizeof(header)) == 0);
		memset(&out, 0xa7, sizeof(out));
		UT_ASSERT(!cluster_cold_typed_undo_base_redo_block_v1(&f.typed, 0, 0, 9, &base, &out));
		UT_ASSERT(memcmp(&out, &(PGAlignedBlock){ 0 }, sizeof(out)) == 0);
		tt_redo_base_refuses(&f.typed, 0, 0, tt_redo_record_address(513, 9, 0));
		UT_ASSERT_NULL(f.typed.tt_base->blocks);
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_redo_base_keeps_ancestry_after_prefix_init)
{
	tt_redo_base_prefix_lifecycle(false);
}

UT_TEST(tt_redo_base_keeps_ancestry_after_prefix_reuse)
{
	tt_redo_base_prefix_lifecycle(true);
}

UT_TEST(tt_redo_base_refuses_current_init_even_with_a_later_fpi)
{
	TtRedoFixture f;
	PGAlignedBlock page;
	FakeXactRecord fake;
	struct {
		xl_cluster_undo_segment_init record;
		char page[BLCKSZ];
	} init = { 0 };

	tt_redo_fixture_at(&f, 300, RF_SIDE_PLAN_RECOVERY, 100);
	init.record.instance = 3;
	init.record.segment_id = 513;
	cluster_undo_segment_make_header_bytes(513, 3, init.page);
	make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_INIT, &init, sizeof(init));
	undo_header_feed(f.typed.side_plans[0], &fake, 100);
	tt_redo_record_page(&page, 802);
	((UndoBlockHeader *)page.data)->block_lsn = 90;
	tt_redo_base_page(&page);
	tt_redo_feed_record_page(&f, 200, &page, true);
	tt_redo_seal(&f);
	tt_redo_base_refuses(&f.typed, 0, 0, tt_redo_record_address(513, 9, 0));
	UT_ASSERT_NULL(f.typed.tt_base->blocks);
	tt_redo_destroy(&f);
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


/* Explicit original-base graph fixtures retain real DATA and sealed redo. */
static void
tt_redo_base_graph_page(uint32 segment, uint32 block, const PGAlignedBlock *page)
{
	char path[MAXPGPATH];
	int fd;

	snprintf(path, sizeof(path), "%s/%u", tt_redo_dir, segment);
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pwrite(fd, page->data, BLCKSZ, (off_t)block * BLCKSZ), BLCKSZ);
	UT_ASSERT_EQ(close(fd), 0);
}

static void
tt_redo_base_graph_record(PGAlignedBlock *page, TransactionId xid, uint32 segment, uint16 slot,
						  uint16 wrap, UBA previous)
{
	UndoSlotDirEntry directory;
	UndoRecordHeader header;

	tt_redo_record_page(page, xid);
	((UndoBlockHeader *)page->data)->block_lsn = 90;
	memcpy(&directory, page->data + UNDO_SLOT_DIR_OFFSET(0), sizeof(directory));
	memcpy(&header, page->data + directory.record_offset, sizeof(header));
	header.tt_slot_segment_id = segment;
	header.tt_slot_id = slot + 1;
	header.tt_wrap_plus1 = wrap + 1;
	header.prev_uba = previous;
	if (!UBA_is_invalid(previous))
		header.flags &= ~UNDO_REC_FLAG_FIRST_IN_TX;
	directory.flags = header.flags;
	memcpy(page->data + directory.record_offset, &header, sizeof(header));
	memcpy(page->data + UNDO_SLOT_DIR_OFFSET(0), &directory, sizeof(directory));
}

static void
tt_redo_base_graph_fixture(TtRedoFixture *f, XLogRecPtr end, PGAlignedBlock *page)
{
	UndoSegmentHeaderData *header;

	tt_redo_fixture(f, end, RF_SIDE_PLAN_RECOVERY);
	header = (UndoSegmentHeaderData *)f->base.data;
	header->tt_slots[4].first_undo_block = tt_redo_record_address(513, 9, 0);
	header->tt_slots[47].first_undo_block = (UBA)InvalidUba_init;
	tt_redo_seed_header(513, &f->base);
	tt_redo_seed_recapture(f);
	tt_redo_base_graph_record(page, 802, 513, 4, 7, (UBA)InvalidUba_init);
	tt_redo_base_page(page);
}

static void
tt_redo_base_graph_refuses(ClusterColdTtBaseRedoGraphInputV1 *input,
						   const ClusterRecoveryTxnKey *key, UBA address,
						   ClusterRecoveryGraphResult expected)
{
	ClusterRecoveryUndoRecord out, zero = { 0 };
	uint32 generation = UINT32_MAX;

	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT_EQ(
		cluster_cold_typed_tt_base_redo_graph_read_v1(input, key, address, &out, &generation),
		expected);
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	UT_ASSERT_EQ(generation, 0);
}

UT_TEST(tt_redo_base_graph_walks_original_chain_across_physical_generations)
{
	TtRedoFixture f;
	PGAlignedBlock page, second;
	ClusterColdTtBaseRedoGraphInputV1 input = { &f.typed, 0 };
	ClusterColdTtRedoGraphInputV1 fpi = { &f.typed, 0 };
	ClusterRecoveryReferenceGraph *graph = NULL;
	ClusterRecoveryGraphRecord out;
	ClusterRecoveryGraphResult result = CLUSTER_RECOVERY_GRAPH_MORE;
	ClusterRecoveryTxnKey key;
	unsigned seen = 0;
	char path[MAXPGPATH];

	tt_redo_base_graph_fixture(&f, 100, &page);
	cluster_undo_segment_make_header_bytes(514, 3, second.data);
	((UndoSegmentHeaderData *)second.data)->wrap_count = 91;
	tt_redo_seed_header(514, &second);
	tt_redo_seed_recapture(&f);
	tt_redo_base_graph_record(&page, 802, 513, 4, 7, tt_redo_record_address(514, 9, 0));
	tt_redo_base_page(&page);
	tt_redo_base_graph_record(&page, 802, 513, 4, 7, (UBA)InvalidUba_init);
	tt_redo_base_graph_page(514, 9, &page);
	tt_redo_seal(&f);
	key = tt_redo_graph_key(&f);
	tt_redo_graph_refuses(&fpi, &key, tt_redo_record_address(513, 9, 0),
						  CLUSTER_RECOVERY_GRAPH_INVALID);
	UT_ASSERT_EQ(cluster_cold_typed_tt_base_redo_graph_begin_v1(&input, 1024 * 1024, &graph),
				 CLUSTER_RECOVERY_GRAPH_MORE);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_count(graph), 1);
	for (unsigned i = 0; i < 64; i++) {
		result = cluster_recovery_reference_graph_step(graph, &out);
		if (result == CLUSTER_RECOVERY_GRAPH_RECORD) {
			UT_ASSERT_EQ(out.transaction.segment_id, 513);
			UT_ASSERT_EQ(out.transaction.segment_generation, 0);
			UT_ASSERT_EQ(out.record->header.xid, 802);
			UT_ASSERT_EQ(out.record_segment_generation, seen == 0 ? 0 : 91);
			seen++;
		} else if (result != CLUSTER_RECOVERY_GRAPH_MORE)
			break;
	}
	UT_ASSERT_EQ(seen, 2);
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_GRAPH_DRAINED);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_count(graph), 2);
	UT_ASSERT(cluster_recovery_reference_graph_observed(graph));
	UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&f.typed), CLUSTER_COLD_OK);
	cluster_recovery_reference_graph_destroy(graph);
	snprintf(path, sizeof(path), "%s/514", tt_redo_dir);
	UT_ASSERT_EQ(unlink(path), 0);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_base_graph_seeds_all_segments_slot_edges_and_terminal_heads_after_redo)
{
	TtRedoFixture f;
	PGAlignedBlock page, second, original;
	UndoSegmentHeaderData *header;
	UndoSlotDirEntry directory;
	UndoRecordHeader changed;
	FakeXactRecord fake;
	ClusterColdTtBaseRedoGraphInputV1 input = { &f.typed, 0 };
	ClusterRecoveryReferenceGraph *graph = NULL;
	ClusterRecoveryGraphRecord out;
	ClusterRecoveryGraphResult result = CLUSTER_RECOVERY_GRAPH_MORE;
	unsigned seen = 0;
	uint32 generation = UINT32_MAX;
	char path[MAXPGPATH];

	tt_redo_fixture(&f, 300, RF_SIDE_PLAN_RECOVERY);
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
	tt_redo_seed_recapture(&f);
	tt_redo_base_graph_record(&page, 804, 513, 47, 5, (UBA)InvalidUba_init);
	tt_redo_base_graph_page(513, 10, &page);
	tt_redo_base_graph_record(&page, 900, 514, 47, 9, (UBA)InvalidUba_init);
	tt_redo_base_graph_page(514, 9, &page);
	tt_redo_base_graph_record(&page, 800, 513, 0, 0, (UBA)InvalidUba_init);
	tt_redo_base_page(&page);
	original = page;
	memcpy(&directory, page.data + UNDO_SLOT_DIR_OFFSET(0), sizeof(directory));
	memcpy(&changed, page.data + directory.record_offset, sizeof(changed));
	changed.target_offset = 2;
	memcpy(page.data + directory.record_offset, &changed, sizeof(changed));
	tt_redo_feed_record_page(&f, 100, &page, false);
	undo_header_commit(&fake, 0, 802);
	undo_header_feed(f.typed.side_plans[0], &fake, 200);
	tt_redo_seal(&f);
	UT_ASSERT_EQ(cluster_cold_typed_tt_base_redo_graph_begin_v1(&input, 1024 * 1024, &graph),
				 CLUSTER_RECOVERY_GRAPH_MORE);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_count(graph), 3);
	for (unsigned i = 0; i < 64; i++) {
		result = cluster_recovery_reference_graph_step(graph, &out);
		if (result == CLUSTER_RECOVERY_GRAPH_RECORD) {
			if (out.transaction.xid == 800) {
				UT_ASSERT_EQ(out.transaction.slot_offset, 0);
				UT_ASSERT_EQ(out.record->header.target_offset, 2);
				UT_ASSERT_EQ(out.record->block_header.block_lsn, 200);
				seen |= 1;
			} else if (out.transaction.xid == 804) {
				UT_ASSERT_EQ(out.transaction.slot_offset, 47);
				seen |= 2;
			} else {
				UT_ASSERT_EQ(out.transaction.xid, 900);
				UT_ASSERT_EQ(out.transaction.segment_id, 514);
				UT_ASSERT_EQ(out.record_segment_generation, 91);
				seen |= 4;
			}
		} else if (result != CLUSTER_RECOVERY_GRAPH_MORE)
			break;
	}
	UT_ASSERT_EQ(seen, 7);
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_GRAPH_DRAINED);
	UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&f.typed), CLUSTER_COLD_OK);
	/* Full original DATA still matches after graph traversal and verification. */
	UT_ASSERT(cluster_cold_tt_base_block_v1(&f.typed, 0, 9, &page, &generation));
	UT_ASSERT_EQ(generation, 0);
	UT_ASSERT(memcmp(page.data, original.data, BLCKSZ) == 0);
	cluster_recovery_reference_graph_destroy(graph);
	snprintf(path, sizeof(path), "%s/514", tt_redo_dir);
	UT_ASSERT_EQ(unlink(path), 0);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_base_graph_rejects_exact_key_source_and_bad_arguments)
{
	TtRedoFixture f;
	PGAlignedBlock page;
	ClusterRecoveryTxnKey key, changed;
	ClusterColdTtBaseRedoGraphInputV1 input = { &f.typed, 0 };
	ClusterRecoveryUndoRecord out, zero = { 0 };
	uint32 generation = UINT32_MAX;
	UBA address = tt_redo_record_address(513, 9, 0);

	tt_redo_base_graph_fixture(&f, 100, &page);
	tt_redo_seal(&f);
	key = tt_redo_graph_key(&f);
	for (int fault = 0; fault < 8; fault++) {
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
		}
		tt_redo_base_graph_refuses(&input, &changed, address, CLUSTER_RECOVERY_GRAPH_IDENTITY);
	}
	f.typed.participants[0].native_redo++;
	tt_redo_base_graph_refuses(&input, &key, address, CLUSTER_RECOVERY_GRAPH_IDENTITY);
	f.typed.participants[0].native_redo--;
	input.participant = 1;
	tt_redo_base_graph_refuses(&input, &key, address, CLUSTER_RECOVERY_GRAPH_IDENTITY);
	input.participant = 0;
	tt_redo_base_graph_refuses(NULL, &key, address, CLUSTER_RECOVERY_GRAPH_INVALID);
	tt_redo_base_graph_refuses(&input, NULL, address, CLUSTER_RECOVERY_GRAPH_INVALID);
	tt_redo_base_graph_refuses(&input, &key, (UBA)InvalidUba_init, CLUSTER_RECOVERY_GRAPH_INVALID);
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT_EQ(cluster_cold_typed_tt_base_redo_graph_read_v1(&input, &key, address, &out, NULL),
				 CLUSTER_RECOVERY_GRAPH_INVALID);
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	UT_ASSERT_EQ(
		cluster_cold_typed_tt_base_redo_graph_read_v1(&input, &key, address, NULL, &generation),
		CLUSTER_RECOVERY_GRAPH_INVALID);
	UT_ASSERT_EQ(generation, 0);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_base_graph_verification_detects_full_block_drift_and_stays_failed)
{
	TtRedoFixture f;
	PGAlignedBlock page;
	ClusterColdTtBaseRedoGraphInputV1 input = { &f.typed, 0 };
	ClusterRecoveryReferenceGraph *graph = NULL;
	ClusterRecoveryGraphRecord out;
	ClusterRecoveryGraphResult result = CLUSTER_RECOVERY_GRAPH_MORE;
	unsigned records = 0;

	tt_redo_base_graph_fixture(&f, 100, &page);
	tt_redo_seal(&f);
	UT_ASSERT_EQ(cluster_cold_typed_tt_base_redo_graph_begin_v1(&input, 1024 * 1024, &graph),
				 CLUSTER_RECOVERY_GRAPH_MORE);
	for (unsigned i = 0; i < 32; i++) {
		result = cluster_recovery_reference_graph_step(graph, &out);
		if (result == CLUSTER_RECOVERY_GRAPH_RECORD) {
			records++;
			page.data[BLCKSZ / 2] ^= 1;
			tt_redo_base_page(&page);
		} else if (result != CLUSTER_RECOVERY_GRAPH_MORE)
			break;
	}
	UT_ASSERT_EQ(records, 1);
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_GRAPH_INVALID);
	UT_ASSERT_NULL(out.record);
	UT_ASSERT(!cluster_recovery_reference_graph_observed(graph));
	UT_ASSERT_EQ(cluster_recovery_reference_graph_step(graph, &out), result);
	UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&f.typed), CLUSTER_COLD_SOURCE_GAP);
	cluster_recovery_reference_graph_destroy(graph);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_base_graph_rejects_claim_drift_during_verification)
{
	TtRedoFixture f;
	PGAlignedBlock page;
	ClusterWalSourceRef saved;
	ClusterColdTtBaseRedoGraphInputV1 input = { &f.typed, 0 };
	ClusterRecoveryReferenceGraph *graph = NULL;
	ClusterRecoveryGraphRecord out;
	ClusterRecoveryGraphResult result = CLUSTER_RECOVERY_GRAPH_MORE;
	unsigned records = 0;

	tt_redo_base_graph_fixture(&f, 100, &page);
	tt_redo_seal(&f);
	saved = f.typed.sources[0];
	UT_ASSERT_EQ(cluster_cold_typed_tt_base_redo_graph_begin_v1(&input, 1024 * 1024, &graph),
				 CLUSTER_RECOVERY_GRAPH_MORE);
	for (unsigned i = 0; i < 32; i++) {
		result = cluster_recovery_reference_graph_step(graph, &out);
		if (result == CLUSTER_RECOVERY_GRAPH_RECORD) {
			records++;
			f.typed.sources[0].claim.claim_sha256[0]++;
		} else if (result != CLUSTER_RECOVERY_GRAPH_MORE)
			break;
	}
	UT_ASSERT_EQ(records, 1);
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_GRAPH_IDENTITY);
	UT_ASSERT_NULL(out.record);
	UT_ASSERT(!cluster_recovery_reference_graph_observed(graph));
	f.typed.sources[0] = saved;
	UT_ASSERT_EQ(cluster_recovery_reference_graph_step(graph, &out), result);
	cluster_recovery_reference_graph_destroy(graph);
	tt_redo_destroy(&f);
}

UT_TEST(tt_redo_base_graph_never_treats_refused_record_as_empty_chain)
{
	for (int fault = 0; fault < 5; fault++) {
		TtRedoFixture f;
		PGAlignedBlock page;
		ClusterColdTtBaseRedoGraphInputV1 input = { &f.typed, 0 };
		ClusterRecoveryReferenceGraph *graph = NULL;
		ClusterRecoveryGraphRecord out;
		ClusterRecoveryGraphResult result = CLUSTER_RECOVERY_GRAPH_MORE;

		tt_redo_base_graph_fixture(&f, 100, &page);
		if (fault == 0)
			f.typed.tt_base->budget = f.typed.tt_base->used;
		else if (fault == 1)
			((UndoBlockHeader *)page.data)->block_lsn = 0;
		else if (fault == 2)
			((UndoBlockHeader *)page.data)->block_lsn = 101;
		else if (fault == 3)
			memset(&page, 0, sizeof(page));
		else
			tt_redo_base_graph_record(&page, 999, 513, 4, 7, (UBA)InvalidUba_init);
		tt_redo_base_page(&page);
		tt_redo_seal(&f);
		UT_ASSERT_EQ(cluster_cold_typed_tt_base_redo_graph_begin_v1(&input, 1024 * 1024, &graph),
					 CLUSTER_RECOVERY_GRAPH_MORE);
		for (unsigned i = 0; i < 16 && result == CLUSTER_RECOVERY_GRAPH_MORE; i++)
			result = cluster_recovery_reference_graph_step(graph, &out);
		UT_ASSERT_EQ(result,
					 fault == 4 ? CLUSTER_RECOVERY_GRAPH_IDENTITY : CLUSTER_RECOVERY_GRAPH_INVALID);
		UT_ASSERT_NULL(out.record);
		UT_ASSERT(!cluster_recovery_reference_graph_observed(graph));
		UT_ASSERT_EQ(cluster_recovery_reference_graph_step(graph, &out), result);
		if (fault == 0)
			UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&f.typed), CLUSTER_COLD_CAPACITY);
		cluster_recovery_reference_graph_destroy(graph);
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_redo_base_graph_factory_clears_failures_and_requires_source_with_empty_inventory)
{
	TtRedoFixture f;
	PGAlignedBlock page;
	ClusterColdTtBaseRedoGraphInputV1 input = { &f.typed, 0 };
	ClusterRecoveryReferenceGraph *graph = (ClusterRecoveryReferenceGraph *)(uintptr_t)1;
	char path[MAXPGPATH];

	tt_redo_base_graph_fixture(&f, 100, &page);
	tt_redo_seal(&f);
	UT_ASSERT_EQ(cluster_cold_typed_tt_base_redo_graph_begin_v1(NULL, 1024 * 1024, &graph),
				 CLUSTER_RECOVERY_GRAPH_INVALID);
	UT_ASSERT_NULL(graph);
	UT_ASSERT_EQ(cluster_cold_typed_tt_base_redo_graph_begin_v1(&input, 0, &graph),
				 CLUSTER_RECOVERY_GRAPH_CAPACITY);
	UT_ASSERT_NULL(graph);
	UT_ASSERT_EQ(cluster_cold_typed_tt_base_redo_graph_begin_v1(&input, 1024 * 1024, NULL),
				 CLUSTER_RECOVERY_GRAPH_INVALID);
	snprintf(path, sizeof(path), "%s/513", tt_redo_dir);
	UT_ASSERT_EQ(unlink(path), 0);
	tt_redo_seed_recapture(&f);
	UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&f.typed), 0);
	f.typed.participants[0].owner_incarnation++;
	UT_ASSERT_EQ(cluster_cold_typed_tt_base_redo_graph_begin_v1(&input, 1024 * 1024, &graph),
				 CLUSTER_RECOVERY_GRAPH_IDENTITY);
	UT_ASSERT_NULL(graph);
	f.typed.participants[0].owner_incarnation--;
	UT_ASSERT_EQ(cluster_cold_typed_tt_base_redo_graph_begin_v1(&input, 1024 * 1024, &graph),
				 CLUSTER_RECOVERY_GRAPH_MORE);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_count(graph), 0);
	cluster_recovery_reference_graph_destroy(graph);
	/* Restore only for the existing fixture cleanup, after dropping capture. */
	cluster_cold_tt_base_destroy_v1(&f.typed);
	tt_redo_seed_header(513, &f.base);
	tt_redo_destroy(&f);
}

static void
tt_readback_write(uint32 segment, const PGAlignedBlock *image, bool create)
{
	char path[MAXPGPATH];
	int fd;

	snprintf(path, sizeof(path), "%s/%u", tt_redo_dir, segment);
	fd = open(path, O_WRONLY | (create ? O_CREAT | O_EXCL : 0), 0600);
	UT_ASSERT(fd >= 0);
	if (create)
		UT_ASSERT_EQ(ftruncate(fd, UNDO_SEGMENT_SIZE_BYTES), 0);
	UT_ASSERT_EQ(pwrite(fd, image->data, BLCKSZ, 0), BLCKSZ);
	UT_ASSERT_EQ(close(fd), 0);
}

static void
tt_readback_refuses(const ClusterColdTypedV1 *typed, uint32 participant, uint32 segment)
{
	RfSideUndoHeaderImageV1 out, zero = { 0 };

	memset(&out, 0xa9, sizeof(out));
	UT_ASSERT(!cluster_cold_typed_tt_redo_readback_v1(typed, participant, segment, &out));
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
}

UT_TEST(tt_readback_matches_physical_commit_and_preserves_original)
{
	TtRedoFixture f;
	FakeXactRecord fake;
	PGAlignedBlock expected;
	RfSideUndoHeaderImageV1 out;
	const UndoSegmentHeaderData *observed, *original;
	uint8 owner;
	uint32 segment;

	tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
	undo_header_commit(&fake, 0, 802);
	undo_header_feed(f.typed.side_plans[0], &fake, 100);
	tt_redo_seal(&f);
	expected = f.base;
	((UndoSegmentHeaderData *)expected.data)->tt_slots[4].status = TT_SLOT_COMMITTED;
	((UndoSegmentHeaderData *)expected.data)->tt_slots[4].commit_scn = 999;
	tt_readback_write(513, &expected, false);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
	observed = cluster_cold_tt_after_redo_header_v1(&f.typed, 0, &owner, &segment);
	UT_ASSERT_NOT_NULL(observed);
	UT_ASSERT_EQ(owner, 3);
	UT_ASSERT_EQ(segment, 513);
	if (observed != NULL)
		UT_ASSERT(memcmp(observed, expected.data, BLCKSZ) == 0);
	UT_ASSERT(cluster_cold_typed_tt_redo_readback_v1(&f.typed, 0, 0, &out));
	UT_ASSERT(memcmp(out.page.data, expected.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(out.operation_count, 1);
	UT_ASSERT_EQ(out.source_index, 0);
	original = cluster_cold_tt_base_header_v1(&f.typed, 0, &owner, &segment);
	UT_ASSERT_NOT_NULL(original);
	UT_ASSERT(memcmp(original, f.base.data, BLCKSZ) == 0);
	out.page.data[BLCKSZ - 1] ^= 1;
	UT_ASSERT(cluster_cold_typed_tt_redo_readback_v1(&f.typed, 0, 0, &out));
	UT_ASSERT(memcmp(out.page.data, expected.data, BLCKSZ) == 0);
	tt_redo_destroy(&f);
}

UT_TEST(tt_readback_matches_exact_abort_and_unchanged_active_slots)
{
	for (int terminal = 0; terminal < 2; terminal++) {
		TtRedoFixture f;
		FakeXactRecord fake;
		PGAlignedBlock expected;
		RfSideUndoHeaderImageV1 out;

		tt_redo_fixture(&f, terminal ? 200 : 100, RF_SIDE_PLAN_RECOVERY);
		expected = f.base;
		if (terminal) {
			undo_header_abort(&fake, 4);
			undo_header_feed(f.typed.side_plans[0], &fake, 100);
			((UndoSegmentHeaderData *)expected.data)->tt_slots[4].status = TT_SLOT_ABORTED;
		}
		tt_redo_seal(&f);
		tt_readback_write(513, &expected, false);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
		UT_ASSERT(cluster_cold_typed_tt_redo_readback_v1(&f.typed, 0, 0, &out));
		UT_ASSERT(memcmp(out.page.data, expected.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(((UndoSegmentHeaderData *)out.page.data)->tt_slots[47].status, TT_SLOT_ACTIVE);
		UT_ASSERT_EQ(out.operation_count, terminal ? 1 : 0);
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_readback_refuses_every_slot_and_non_slot_byte_mismatch)
{
	TtRedoFixture f;
	FakeXactRecord fake;
	PGAlignedBlock expected, changed;

	tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
	undo_header_commit(&fake, 0, 802);
	undo_header_feed(f.typed.side_plans[0], &fake, 100);
	tt_redo_seal(&f);
	expected = f.base;
	((UndoSegmentHeaderData *)expected.data)->tt_slots[4].status = TT_SLOT_COMMITTED;
	((UndoSegmentHeaderData *)expected.data)->tt_slots[4].commit_scn = 999;
	for (uint32 fault = 0; fault < TT_SLOTS_PER_SEGMENT + 3; fault++) {
		UndoSegmentHeaderData *header;

		changed = expected;
		header = (UndoSegmentHeaderData *)changed.data;
		if (fault < TT_SLOTS_PER_SEGMENT)
			header->tt_slots[fault].commit_scn++;
		else if (fault == TT_SLOTS_PER_SEGMENT)
			header->wrap_count++;
		else if (fault == TT_SLOTS_PER_SEGMENT + 1)
			changed = f.base; /* A missed physical COMMIT is not readback. */
		else
			changed.data[BLCKSZ - 1] ^= 1;
		tt_readback_write(513, &changed, false);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
		tt_readback_refuses(&f.typed, 0, 0);
		cluster_cold_tt_after_redo_reset_v1(&f.typed);
	}
	tt_redo_destroy(&f);
}

UT_TEST(tt_readback_looks_up_physical_identity_in_independent_inventory)
{
	TtRedoFixture f;
	PGAlignedBlock extra;
	RfSideUndoHeaderImageV1 out;
	uint8 owner;
	uint32 segment;
	char path[MAXPGPATH];

	tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
	tt_redo_seal(&f);
	cluster_undo_segment_make_header_bytes(257, 2, extra.data);
	tt_readback_write(257, &extra, true);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_count_v1(&f.typed), 2);
	UT_ASSERT_NOT_NULL(cluster_cold_tt_after_redo_header_v1(&f.typed, 0, &owner, &segment));
	UT_ASSERT_EQ(segment, 257);
	UT_ASSERT(cluster_cold_typed_tt_redo_readback_v1(&f.typed, 0, 0, &out));
	UT_ASSERT(memcmp(out.page.data, f.base.data, BLCKSZ) == 0);
	cluster_cold_tt_after_redo_reset_v1(&f.typed);
	snprintf(path, sizeof(path), "%s/513", tt_redo_dir);
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_count_v1(&f.typed), 1);
	tt_readback_refuses(&f.typed, 0, 0); /* Same count is not identity. */
	tt_readback_write(513, &f.base, true);
	snprintf(path, sizeof(path), "%s/257", tt_redo_dir);
	UT_ASSERT_EQ(unlink(path), 0);
	tt_redo_destroy(&f);
}

UT_TEST(tt_readback_requires_original_source_namespace_and_valid_snapshots)
{
	TtRedoFixture f;
	ClusterColdTypedV1 saved;

	tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
	tt_redo_seal(&f);
	tt_readback_refuses(&f.typed, 0, 0);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
	saved = f.typed;
	for (int fault = 0; fault < 8; fault++) {
		f.typed = saved;
		if (fault == 0)
			f.typed.system_identifier++;
		if (fault == 1)
			f.typed.observer.database_incarnation++;
		if (fault == 2)
			f.typed.sources[0].claim.claim_sha256[0] ^= 1;
		if (fault == 3)
			f.typed.participants[0].native_redo++;
		if (fault == 4)
			f.typed.participants[0].owner_incarnation++;
		if (fault == 5)
			f.typed.refusal = CLUSTER_COLD_SOURCE_GAP;
		if (fault == 6)
			f.typed.tt_base = NULL;
		if (fault == 7)
			f.typed.side_plans[0] = NULL;
		tt_readback_refuses(&f.typed, 0, 0);
	}
	f.typed = saved;
	cluster_cold_tt_after_redo_reset_v1(&f.typed);
	tt_readback_refuses(&f.typed, 0, 0);
	tt_redo_destroy(&f);
}

UT_TEST(tt_readback_retains_final_recheck_failure_and_clears_bad_outputs)
{
	TtRedoFixture f;
	PGAlignedBlock changed;
	RfSideUndoHeaderImageV1 out;
	uint8 owner = 99;
	uint32 segment = 99;

	tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
	tt_redo_seal(&f);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
	tt_readback_refuses(NULL, 0, 0);
	tt_readback_refuses(&f.typed, 1, 0); /* History cannot authorize this fold. */
	tt_readback_refuses(&f.typed, UINT32_MAX, 0);
	tt_readback_refuses(&f.typed, 0, UINT32_MAX);
	UT_ASSERT(!cluster_cold_typed_tt_redo_readback_v1(&f.typed, 0, 0, NULL));
	UT_ASSERT_NULL(cluster_cold_tt_after_redo_header_v1(&f.typed, UINT32_MAX, &owner, &segment));
	UT_ASSERT_EQ(owner, 0);
	UT_ASSERT_EQ(segment, 0);
	UT_ASSERT_NULL(cluster_cold_tt_after_redo_header_v1(&f.typed, 0, NULL, &segment));
	changed = f.base;
	changed.data[BLCKSZ - 1] ^= 1;
	tt_readback_write(513, &changed, false);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_recheck_v1(&f.typed), CLUSTER_COLD_SOURCE_GAP);
	tt_readback_refuses(&f.typed, 0, 0);
	tt_readback_write(513, &f.base, false);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_recheck_v1(&f.typed), CLUSTER_COLD_SOURCE_GAP);
	tt_readback_refuses(&f.typed, 0, 0);
	UT_ASSERT(!cluster_cold_typed_tt_redo_readback_v1(&f.typed, 0, 0, &out));
	tt_redo_destroy(&f);
}

static void
tt_post_readback_empty_original(TtRedoFixture *f)
{
	char path[MAXPGPATH];

	cluster_cold_tt_base_destroy_v1(&f->typed);
	snprintf(path, sizeof(path), "%s/513", tt_redo_dir);
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&f->typed, (Size)1024 * 1024), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&f->typed), 0);
}

static void
tt_post_readback_init(TtRedoFixture *f, XLogRecPtr begin)
{
	FakeXactRecord fake;
	struct {
		xl_cluster_undo_segment_init record;
		uint8 page[BLCKSZ];
	} init = { 0 };

	init.record.instance = 3;
	init.record.segment_id = 513;
	cluster_undo_segment_make_header_bytes(513, 3, (char *)init.page);
	make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_INIT, &init, sizeof(init));
	undo_header_feed(f->typed.side_plans[0], &fake, begin);
}

static void
tt_post_readback_refuses(const ClusterColdTypedV1 *typed, uint32 participant, uint32 index)
{
	RfSideUndoHeaderImageV1 out, zero = { 0 };

	memset(&out, 0xa9, sizeof(out));
	UT_ASSERT(!cluster_cold_typed_tt_after_redo_readback_v1(typed, participant, index, &out));
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
}

static void
tt_post_readback_new(TtRedoFixture *f, PGAlignedBlock *expected, bool terminal)
{
	FakeXactRecord fake;
	UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)expected->data;

	tt_redo_fixture(f, terminal ? 400 : 300, RF_SIDE_PLAN_RECOVERY);
	tt_post_readback_empty_original(f);
	tt_post_readback_init(f, 100);
	undo_header_bind(&fake, 0);
	undo_header_feed(f->typed.side_plans[0], &fake, 200);
	cluster_undo_segment_make_header_bytes(513, 3, expected->data);
	header->tt_slots[4].xid = 802;
	header->tt_slots[4].wrap = 7;
	header->tt_slots[4].status = terminal ? TT_SLOT_COMMITTED : TT_SLOT_ACTIVE;
	header->tt_slots[4].first_undo_block = (UBA)InvalidUba_init;
	if (terminal) {
		undo_header_commit(&fake, 0, 802);
		undo_header_feed(f->typed.side_plans[0], &fake, 300);
		header->tt_slots[4].commit_scn = 999;
	}
	tt_redo_seal(f);
	tt_readback_write(513, expected, true);
}

static void
tt_top_tx_c0_inputs(const TtRedoFixture *f, const ClusterColdTtSlotInputV1 *input,
					ClusterColdTopTxProofV1 *proof, ClusterRecoveryTTAbortV1 *shape)
{
	memset(proof, 0, sizeof(*proof));
	memset(shape, 0, sizeof(*shape));
	proof->key = input->key;
	proof->producer_top = true;
	proof->qualified_base = true;
	proof->sealed_redo = true;
	proof->ordinary_effects = true;
	proof->old_writer_isolated = true;
	proof->terminal_match = true;
	shape->version = CLUSTER_RECOVERY_TT_ABORT_VERSION;
	shape->length = CLUSTER_RECOVERY_TT_ABORT_BYTES;
	shape->flags = CLUSTER_RECOVERY_TT_ABORT_TOP_KNOWN;
	shape->target_thread = input->key.owner_instance;
	shape->writer_thread = 1;
	shape->target_incarnation = f->typed.participants[0].owner_incarnation;
	shape->writer_incarnation = 12;
	shape->recovery_serial = 13;
	shape->target_epoch = 1;
	memcpy(shape->target_claim_sha256, f->typed.sources[0].claim.claim_sha256,
		   sizeof(shape->target_claim_sha256));
	shape->exact.segment_id = input->key.segment_id;
	shape->exact.segment_generation = input->key.segment_generation;
	shape->exact.xid = input->key.xid;
	shape->exact.slot_offset = input->key.slot_offset;
	shape->exact.wrap = input->key.slot_wrap;
	shape->exact.instance = input->key.owner_instance;
	shape->exact.format_version = CLUSTER_UNDO_TT_ABORT_EXACT_VERSION;
	shape->before = input->slot;
}

static bool
tt_top_tx_unexpected_page(void *arg, const RfPageIdentityV1 *page, ClusterColdDataV1 *out)
{
	(void)arg;
	(void)page;
	(void)out;
	UT_ASSERT(false);
	return false;
}

/* These TT-only streams have no ordinary page/SPACE steps. Feed their real
 * sealed record identities to the ordinary plan and coverage accumulator;
 * physical application and the original TT readback remain separate. */
static void
tt_top_tx_effects_complete(TtRedoFixture *f)
{
	ClusterColdEffectsProofV1 proof = { 0 };
	ClusterColdReplayResultV1 result = { .detail = CLUSTER_COLD_REPLAY_OK };
	ClusterColdDiagV1 diag;

	UT_ASSERT_NULL(f->typed.plan);
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(f->typed.participants, f->typed.participant_count,
												  (Size)1024 * 1024, &f->typed.plan), CLUSTER_COLD_OK);
	for (uint32 p = 0; p < f->typed.participant_count; p++) {
		XLogRecPtr prev = InvalidXLogRecPtr;
		uint32 count = rf_side_online_plan_operation_count_v1(f->typed.side_plans[p]);

		UT_ASSERT(count != UINT32_MAX);
		for (uint32 i = 0; i < count; i++) {
			RfSideOnlineOperationV1 op;
			ClusterColdDecodedV1 decoded = { 0 };

			UT_ASSERT(rf_side_online_plan_operation_v1(f->typed.side_plans[p], i, &op));
			decoded.record.read_rec_ptr = op.identity.record.read_rec_ptr;
			decoded.record.end_rec_ptr = op.identity.record.end_rec_ptr;
			decoded.record.prev_rec_ptr = prev;
			decoded.record.record_crc = op.identity.record.record_crc;
			decoded.record.rmid = op.identity.record.rmid;
			decoded.record.info = op.identity.record.info;
			decoded.record.scn = op.control_scn;
			UT_ASSERT_EQ(cluster_cold_plan_feed_v1(f->typed.plan, p, &decoded.record), CLUSTER_COLD_OK);
			prev = decoded.record.read_rec_ptr;
			if (op.history_only)
				continue;
			/* TT-only fixture: native application is a boundary. The complete
			 * decoder/effect matrix is exercised by the effects suite. */
			UT_ASSERT_EQ(p, f->typed.own_participant);
			decoded.effects.required = CLUSTER_COLD_EFFECT_RECORD;
			decoded.effects.native_apply = decoded.effects.required;
			UT_ASSERT(cluster_cold_effects_proof_record_v1(&proof, p, &decoded, NULL, true));
		}
	}
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(f->typed.plan, tt_top_tx_unexpected_page, NULL, &diag),
				 CLUSTER_COLD_OK);
	UT_ASSERT(cluster_cold_typed_effects_complete_v1(&f->typed, &proof, &result));
}

/* Counter completion is a boundary for this TT-only fixture. The Startup
 * suite publishes and checks this receipt through the actual original owner. */
static void
tt_top_tx_endpoints_complete(TtRedoFixture *f)
{
	ClusterColdXidReceiptV1 *receipt = &f->typed.xid_receipt;

	UT_ASSERT_NOT_NULL(f->typed.plan);
	memset(receipt, 0, sizeof(*receipt));
	receipt->plan = f->typed.plan;
	receipt->system_identifier = f->typed.system_identifier;
	receipt->participant_count = f->typed.participant_count;
	receipt->replay_count = f->typed.replay_count;
	for (uint32 p = 0; p < receipt->participant_count; p++) {
		receipt->sources[p] = f->typed.sources[p];
		receipt->next_xid[p] = f->typed.xid_next_after_redo;
	}
}

UT_TEST(tt_top_tx_c0_producer_refuses_unproved_endpoint_coverage)
{
	TtRedoFixture f;
	ClusterColdTtSlotInputV1 input;
	ClusterColdTopTxProofV1 proof;
	ClusterRecoveryTTAbortV1 shape;
	ClusterColdTopTxC0ProducerResultV1 out, zero = { 0 };

	tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
	tt_redo_seal(&f);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
	UT_ASSERT(cluster_cold_tt_after_redo_slot_v1(&f.typed, 0, 47, &input));
	f.typed.xid_next_after_redo = UINT64_C(0x700400200);
	tt_top_tx_c0_inputs(&f, &input, &proof, &shape);
	tt_top_tx_effects_complete(&f);
	/* A nonzero aggregate cannot replace the original owner's complete per-source counter endpoints. */
	memset(&out, 0xa9, sizeof(out));
	UT_ASSERT(!cluster_cold_typed_top_tx_c0_producer_v1(
		&f.typed, 0, &proof, &shape, false, InvalidTransactionId, UINT64_C(0x12345678), &out));
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	tt_top_tx_endpoints_complete(&f);
	UT_ASSERT(cluster_cold_typed_top_tx_c0_producer_v1(
		&f.typed, 0, &proof, &shape, false, InvalidTransactionId, UINT64_C(0x12345678), &out));
	for (int fault = 0; fault < 8; fault++) {
		ClusterColdXidReceiptV1 saved = f.typed.xid_receipt;
		uint32 last = f.typed.participant_count - 1;

		switch (fault) {
		case 0:
			f.typed.xid_receipt.plan = NULL;
			break;
		case 1:
			f.typed.xid_receipt.participant_count--;
			break;
		case 2:
			f.typed.xid_receipt.replay_count++;
			break;
		case 3:
			f.typed.xid_receipt.next_xid[last] = 0;
			break;
		case 4:
			f.typed.xid_receipt.sources[last].claim.claim_sha256[0] ^= 1;
			break;
		case 5:
			f.typed.xid_receipt.sources[last].timeline++;
			break;
		case 6:
			f.typed.xid_receipt.next_xid[0]++;
			break;
		case 7:
			f.typed.xid_receipt.system_identifier++;
			break;
		}
		memset(&out, 0xa9, sizeof(out));
		UT_ASSERT(!cluster_cold_typed_top_tx_c0_producer_v1(
			&f.typed, 0, &proof, &shape, false, InvalidTransactionId, UINT64_C(0x12345678), &out));
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		f.typed.xid_receipt = saved;
	}
	tt_redo_destroy(&f);
}

UT_TEST(tt_top_tx_c0_producer_refuses_unproved_effect_coverage)
{
	TtRedoFixture f;
	ClusterColdTtSlotInputV1 input;
	ClusterColdTopTxProofV1 proof;
	ClusterRecoveryTTAbortV1 shape;
	ClusterColdTopTxC0ProducerResultV1 out, zero = { 0 };

	tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
	tt_redo_seal(&f);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
	UT_ASSERT(cluster_cold_tt_after_redo_slot_v1(&f.typed, 0, 47, &input));
	f.typed.xid_next_after_redo = UINT64_C(0x700400200);
	tt_top_tx_c0_inputs(&f, &input, &proof, &shape);
	/* A caller's true ordinary_effects flag cannot replace pass-2 coverage. */
	UT_ASSERT_NULL(f.typed.plan);
	memset(&out, 0xa9, sizeof(out));
	UT_ASSERT(!cluster_cold_typed_top_tx_c0_producer_v1(
		&f.typed, 0, &proof, &shape, false, InvalidTransactionId, UINT64_C(0x12345678), &out));
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	tt_top_tx_effects_complete(&f);
	tt_top_tx_endpoints_complete(&f);
	UT_ASSERT(cluster_cold_typed_top_tx_c0_producer_v1(
		&f.typed, 0, &proof, &shape, false, InvalidTransactionId, UINT64_C(0x12345678), &out));
	for (int fault = 0; fault < 4; fault++) {
		ClusterColdEffectsReceiptV1 saved = f.typed.effects_receipt;

		if (fault == 0)
			f.typed.effects_receipt.plan = NULL;
		if (fault == 1)
			f.typed.effects_receipt.proof.invalid = true;
		if (fault == 2)
			f.typed.effects_receipt.proof.records_seen[1]++;
		if (fault == 3)
			f.typed.effects_receipt.result.space_relations_finished++;
		memset(&out, 0xa9, sizeof(out));
		UT_ASSERT(!cluster_cold_typed_top_tx_c0_producer_v1(
			&f.typed, 0, &proof, &shape, false, InvalidTransactionId, UINT64_C(0x12345678), &out));
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		f.typed.effects_receipt = saved;
	}
	tt_redo_destroy(&f);
}

UT_TEST(tt_top_tx_c0_producer_requires_original_owner_and_exact_post_redo)
{
	for (int new_header = 0; new_header < 2; new_header++) {
		TtRedoFixture f;
		PGAlignedBlock expected;
		ClusterColdTtSlotInputV1 input;
		ClusterColdTopTxProofV1 proof = { 0 };
		ClusterRecoveryTTAbortV1 shape = { 0 };
		ClusterColdTopTxC0ProducerResultV1 out, zero = { 0 };
		uint64 next_xid = UINT64_C(0x700400200);
		uint32 offset = new_header ? 4 : 47;

		if (new_header)
			tt_post_readback_new(&f, &expected, false);
		else {
			tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
			tt_redo_seal(&f);
		}
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
		UT_ASSERT(cluster_cold_tt_after_redo_slot_v1(&f.typed, 0, offset, &input));
		f.typed.xid_next_after_redo = next_xid;
		tt_top_tx_c0_inputs(&f, &input, &proof, &shape);
		tt_top_tx_effects_complete(&f);
		tt_top_tx_endpoints_complete(&f);
		UT_ASSERT(cluster_recovery_tt_abort_shape_valid_v1(&shape));
		UT_ASSERT(cluster_cold_typed_top_tx_c0_producer_v1(
			&f.typed, 0, &proof, &shape, false, InvalidTransactionId, UINT64_C(0x12345678), &out));
		UT_ASSERT(memcmp(&out.active.key, &input.key, sizeof(input.key)) == 0);
		UT_ASSERT(memcmp(&out.loser.key, &input.key, sizeof(input.key)) == 0);
		UT_ASSERT_EQ(out.c0.record.target_epoch, UINT64_C(0x12345678));
		UT_ASSERT_EQ(out.c0.record.target_full_xid, out.c0.record.top_full_xid);
		UT_ASSERT_EQ((uint32)out.c0.record.target_full_xid, input.key.xid);
		UT_ASSERT(memcmp(&out.c0.record.before, &input.slot, sizeof(input.slot)) == 0);

		for (int fault = 0; fault < 7; fault++) {
			ClusterRecoveryTTAbortV1 rejected = shape;
			ClusterColdTopTxProofV1 missing_proof = proof;
			uint32 participant = 0;

			f.typed.xid_next_after_redo = fault == 1 ? 0 : next_xid;
			f.typed.xid_receipt.plan = fault == 0 ? NULL : f.typed.plan;
			if (fault == 2)
				participant = 1; /* A different/history owner cannot produce this C0. */
			if (fault == 3)
				rejected.target_incarnation++;
			if (fault == 4)
				rejected.target_claim_sha256[0] ^= 1;
			if (fault == 5) {
				if (UBA_is_invalid(input.slot.first_undo_block)) {
					rejected.before.first_undo_block.raw[0] = (UINT64_C(5) << 32) | 513;
					rejected.before.first_undo_block.raw[1] = offset;
				} else
					rejected.before.first_undo_block = (UBA)InvalidUba_init;
				/* Both heads are valid shapes, but only one is the real before-image. */
				UT_ASSERT(cluster_recovery_tt_abort_shape_valid_v1(&rejected));
			}
			if (fault == 6)
				missing_proof.ordinary_effects = false;
			memset(&out, 0xa9, sizeof(out));
			UT_ASSERT(!cluster_cold_typed_top_tx_c0_producer_v1(
				&f.typed, participant, &missing_proof, &rejected, false, InvalidTransactionId,
				UINT64_C(0x12345678), &out));
			UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		}
		f.typed.xid_next_after_redo = next_xid;
		cluster_cold_tt_after_redo_reset_v1(&f.typed);
		memset(&out, 0xa9, sizeof(out));
		UT_ASSERT(!cluster_cold_typed_top_tx_c0_producer_v1(
			&f.typed, 0, &proof, &shape, false, InvalidTransactionId, UINT64_C(0x12345678), &out));
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_post_readback_reconstructs_new_init_bind_and_commit_without_base)
{
	for (int mode = 0; mode < 3; mode++) {
		TtRedoFixture f;
		PGAlignedBlock expected, foreign;
		RfSideUndoHeaderImageV1 out;
		bool terminal = mode != 0;
		uint32 post_index = mode == 2 ? 1 : 0;
		char path[MAXPGPATH];

		tt_post_readback_new(&f, &expected, terminal);
		if (mode == 2) {
			/* An unrelated captured original is not a base for this new file. */
			cluster_cold_tt_base_destroy_v1(&f.typed);
			snprintf(path, sizeof(path), "%s/513", tt_redo_dir);
			UT_ASSERT_EQ(unlink(path), 0);
			cluster_undo_segment_make_header_bytes(257, 2, foreign.data);
			tt_readback_write(257, &foreign, true);
			UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&f.typed, (Size)1024 * 1024),
						 CLUSTER_COLD_OK);
			tt_readback_write(513, &expected, true);
		}
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
		UT_ASSERT(cluster_cold_typed_tt_after_redo_readback_v1(&f.typed, 0, post_index, &out));
		UT_ASSERT(memcmp(out.page.data, expected.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(out.has_full_image, true);
		UT_ASSERT_EQ(out.operation_count, terminal ? 3 : 2);
		UT_ASSERT_EQ(out.source_index, terminal ? 2 : 1);
		UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&f.typed), mode == 2 ? 1 : 0);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_recheck_v1(&f.typed), CLUSTER_COLD_OK);
		if (mode == 2) {
			snprintf(path, sizeof(path), "%s/257", tt_redo_dir);
			UT_ASSERT_EQ(unlink(path), 0);
		}
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_post_readback_selects_existing_base_by_physical_identity)
{
	TtRedoFixture f;
	PGAlignedBlock foreign, expected;
	FakeXactRecord fake;
	RfSideUndoHeaderImageV1 out;
	char path[MAXPGPATH];

	tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
	undo_header_commit(&fake, 0, 802);
	undo_header_feed(f.typed.side_plans[0], &fake, 100);
	tt_redo_seal(&f);
	expected = f.base;
	((UndoSegmentHeaderData *)expected.data)->tt_slots[4].status = TT_SLOT_COMMITTED;
	((UndoSegmentHeaderData *)expected.data)->tt_slots[4].commit_scn = 999;
	tt_readback_write(513, &expected, false);
	cluster_undo_segment_make_header_bytes(257, 2, foreign.data);
	tt_readback_write(257, &foreign, true);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_count_v1(&f.typed), 2);
	tt_post_readback_refuses(&f.typed, 0, 0); /* Another source cannot authorize owner 2. */
	UT_ASSERT(cluster_cold_typed_tt_after_redo_readback_v1(&f.typed, 0, 1, &out));
	UT_ASSERT(memcmp(out.page.data, expected.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(out.has_full_image, false);
	UT_ASSERT_EQ(out.operation_count, 1);
	UT_ASSERT(memcmp(cluster_cold_tt_base_header_v1(&f.typed, 0, &(uint8){ 0 }, &(uint32){ 0 }),
					 f.base.data, BLCKSZ)
			  == 0);
	snprintf(path, sizeof(path), "%s/257", tt_redo_dir);
	UT_ASSERT_EQ(unlink(path), 0);
	tt_redo_destroy(&f);
}

UT_TEST(tt_post_readback_never_seeds_missing_base_from_observed_bytes)
{
	for (int carrier = 0; carrier < 3; carrier++) {
		TtRedoFixture f;
		FakeXactRecord fake;

		tt_redo_fixture(&f, carrier == 0 ? 100 : 200, RF_SIDE_PLAN_RECOVERY);
		tt_post_readback_empty_original(&f);
		if (carrier != 0) {
			if (carrier == 1)
				undo_header_bind(&fake, 0);
			else
				undo_header_commit(&fake, 0, 802);
			undo_header_feed(f.typed.side_plans[0], &fake, 100);
		}
		tt_redo_seal(&f);
		tt_readback_write(513, &f.base, true);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
		tt_post_readback_refuses(&f.typed, 0, 0);
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_post_readback_new_header_requires_every_slot_and_untouched_byte)
{
	TtRedoFixture f;
	PGAlignedBlock expected, changed;

	tt_post_readback_new(&f, &expected, true);
	for (uint32 fault = 0; fault < TT_SLOTS_PER_SEGMENT + 3; fault++) {
		UndoSegmentHeaderData *header;

		changed = expected;
		header = (UndoSegmentHeaderData *)changed.data;
		if (fault < TT_SLOTS_PER_SEGMENT)
			header->tt_slots[fault].commit_scn++;
		else if (fault == TT_SLOTS_PER_SEGMENT)
			header->wrap_count++;
		else if (fault == TT_SLOTS_PER_SEGMENT + 1)
			header->tt_slots[4].status = TT_SLOT_ACTIVE;
		else
			changed.data[BLCKSZ - 1] ^= 1;
		tt_readback_write(513, &changed, false);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
		tt_post_readback_refuses(&f.typed, 0, 0);
		cluster_cold_tt_after_redo_reset_v1(&f.typed);
	}
	tt_redo_destroy(&f);
}

UT_TEST(tt_post_readback_new_header_retains_original_source_and_namespace_guards)
{
	for (int fault = 0; fault < 8; fault++) {
		TtRedoFixture f;
		PGAlignedBlock expected;

		tt_post_readback_new(&f, &expected, true);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
		switch (fault) {
		case 0:
			f.typed.system_identifier++;
			break;
		case 1:
			f.typed.observer.database_incarnation++;
			break;
		case 2:
			f.typed.participants[0].native_redo++;
			break;
		case 3:
			f.typed.participants[0].tail_end++;
			break;
		case 4:
			f.typed.sources[0].claim.claim_sha256[0] ^= 1;
			break;
		case 5:
			f.typed.side_capture_detail[0] = RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
			break;
		case 6:
			f.typed.refusal = CLUSTER_COLD_SOURCE_GAP;
			break;
		case 7:
			f.typed.replay_count = 0;
			break;
		}
		tt_post_readback_refuses(&f.typed, 0, 0);
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_post_readback_new_header_retains_native_lifecycle_refusals)
{
	for (int fault = 0; fault < 3; fault++) {
		TtRedoFixture f;
		FakeXactRecord fake;
		PGAlignedBlock observed;

		tt_redo_fixture(&f, 300, RF_SIDE_PLAN_RECOVERY);
		tt_post_readback_empty_original(&f);
		if (fault == 0) {
			undo_header_bind(&fake, 0);
			undo_header_feed(f.typed.side_plans[0], &fake, 100);
			tt_post_readback_init(&f, 200);
		} else {
			tt_post_readback_init(&f, 100);
			if (fault == 1)
				tt_post_readback_init(&f, 200);
			else {
				undo_header_bind(&fake, 1);
				undo_header_feed(f.typed.side_plans[0], &fake, 200);
			}
		}
		tt_redo_seal(&f);
		cluster_undo_segment_make_header_bytes(513, 3, observed.data);
		tt_readback_write(513, &observed, true);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
		tt_post_readback_refuses(&f.typed, 0, 0);
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_post_readback_requires_valid_post_capture_and_keeps_final_refusal)
{
	TtRedoFixture f;
	PGAlignedBlock expected, changed;

	tt_post_readback_new(&f, &expected, true);
	tt_post_readback_refuses(&f.typed, 0, 0);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
	tt_post_readback_refuses(NULL, 0, 0);
	tt_post_readback_refuses(&f.typed, 1, 0);
	tt_post_readback_refuses(&f.typed, UINT32_MAX, 0);
	tt_post_readback_refuses(&f.typed, 0, UINT32_MAX);
	UT_ASSERT(!cluster_cold_typed_tt_after_redo_readback_v1(&f.typed, 0, 0, NULL));
	changed = expected;
	changed.data[BLCKSZ - 1] ^= 1;
	tt_readback_write(513, &changed, false);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_recheck_v1(&f.typed), CLUSTER_COLD_SOURCE_GAP);
	tt_post_readback_refuses(&f.typed, 0, 0);
	tt_readback_write(513, &expected, false);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_recheck_v1(&f.typed), CLUSTER_COLD_SOURCE_GAP);
	tt_post_readback_refuses(&f.typed, 0, 0);
	tt_redo_destroy(&f);
}

static void
tt_inventory_extra(uint32 segment, uint8 owner, PGAlignedBlock *page)
{
	cluster_undo_segment_make_header_bytes(segment, owner, page->data);
	((UndoSegmentHeaderData *)page->data)->tt_slots[47].xid = 900;
	((UndoSegmentHeaderData *)page->data)->tt_slots[47].wrap = 9;
	((UndoSegmentHeaderData *)page->data)->tt_slots[47].status = TT_SLOT_ACTIVE;
	tt_readback_write(segment, page, true);
}

static void
tt_inventory_remove(uint32 segment)
{
	char path[MAXPGPATH];

	snprintf(path, sizeof(path), "%s/%u", tt_redo_dir, segment);
	UT_ASSERT_EQ(unlink(path), 0);
}

UT_TEST(tt_inventory_checks_untouched_current_and_history_headers)
{
	TtRedoFixture f;
	PGAlignedBlock second, history;

	tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
	tt_inventory_extra(514, 3, &second);
	tt_inventory_extra(257, 2, &history);
	tt_redo_seed_recapture(&f);
	tt_redo_seal(&f);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
	UT_ASSERT(cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
	UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&f.typed), 3);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_count_v1(&f.typed), 3);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_recheck_v1(&f.typed), CLUSTER_COLD_OK);
	tt_inventory_remove(257);
	tt_inventory_remove(514);
	tt_redo_destroy(&f);
}

static void
tt_inventory_bad_layout(bool history)
{
	for (int fault = 0; fault < 9; fault++) {
		TtRedoFixture f;
		PGAlignedBlock extra;
		UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)extra.data;
		uint32 segment = history ? 257 : 514;

		tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
		tt_inventory_extra(segment, history ? 2 : 3, &extra);
		switch (fault) {
		case 0:
			header->tt_slots[47].status = TT_SLOT_RECYCLABLE + 1;
			break;
		case 1:
			header->tt_slots[0].status = TT_SLOT_INVALID;
			break;
		case 2:
			header->tt_slots[47].flags = 0x80;
			break;
		case 3:
			header->segment_flags = 0x80;
			break;
		case 4:
			header->pd_lower++;
			break;
		case 5:
			header->pd_upper--;
			break;
		case 6:
			header->pd_special--;
			break;
		case 7:
			header->pd_flags |= PD_HAS_ITL;
			break;
		case 8:
			header->pd_flags |= PD_SPACE_METADATA;
			break;
		}
		tt_readback_write(segment, &extra, false);
		tt_redo_seed_recapture(&f);
		tt_redo_seal(&f);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
		/* The original and post-redo bytes agree exactly. That cannot make
		 * an uninterpretable last slot or header into a qualified input. */
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_recheck_v1(&f.typed), CLUSTER_COLD_OK);
		UT_ASSERT(!cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
		tt_inventory_remove(segment);
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_inventory_refuses_unchanged_current_header_with_bad_layout)
{
	tt_inventory_bad_layout(false);
}

UT_TEST(tt_inventory_refuses_unchanged_history_header_with_bad_layout)
{
	tt_inventory_bad_layout(true);
}

UT_TEST(tt_inventory_accepts_known_layout_for_all_slots_and_generation_zero)
{
	TtRedoFixture f;
	PGAlignedBlock extra;
	UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)extra.data;

	tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
	for (uint8 owner = 2; owner <= 3; owner++) {
		uint32 segment = owner == 2 ? 257 : 514;

		tt_inventory_extra(segment, owner, &extra);
		for (uint32 slot = 0; slot < TT_SLOTS_PER_SEGMENT; slot++) {
			TTSlot *entry = &header->tt_slots[slot];

			entry->status = slot % (TT_SLOT_RECYCLABLE + 1);
			if (entry->status != TT_SLOT_UNUSED) {
				entry->xid = 900 + slot;
				entry->wrap = 1;
			}
			if (entry->status >= TT_SLOT_COMMITTED) {
				entry->flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
				entry->commit_scn = 123456;
			}
		}
		UT_ASSERT_EQ(header->wrap_count, 0);
		tt_readback_write(segment, &extra, false);
	}
	tt_redo_seed_recapture(&f);
	tt_redo_seal(&f);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
	UT_ASSERT(cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
	tt_inventory_remove(257);
	tt_inventory_remove(514);
	tt_redo_destroy(&f);
}

UT_TEST(tt_inventory_matches_new_native_full_image_and_terminal)
{
	for (int terminal = 0; terminal < 2; terminal++) {
		TtRedoFixture f;
		PGAlignedBlock expected;

		tt_post_readback_new(&f, &expected, terminal != 0);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
		UT_ASSERT(cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_inventory_refuses_disappeared_original_even_when_selected_header_matches)
{
	for (int history = 0; history < 2; history++) {
		TtRedoFixture f;
		PGAlignedBlock extra;
		RfSideUndoHeaderImageV1 out;
		uint32 segment = history ? 257 : 514;

		tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
		tt_inventory_extra(segment, history ? 2 : 3, &extra);
		tt_redo_seed_recapture(&f);
		tt_redo_seal(&f);
		tt_inventory_remove(segment);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
		UT_ASSERT(cluster_cold_typed_tt_after_redo_readback_v1(&f.typed, 0, 0, &out));
		UT_ASSERT(!cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_inventory_requires_current_wal_targets_even_with_empty_observed_inventory)
{
	TtRedoFixture f;
	PGAlignedBlock expected;

	tt_post_readback_new(&f, &expected, true);
	tt_inventory_remove(513);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&f.typed), 0);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_count_v1(&f.typed), 0);
	UT_ASSERT(!cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
	tt_readback_write(513, &expected, true);
	tt_redo_destroy(&f);
}

UT_TEST(tt_inventory_refuses_unexplained_new_current_or_history_segment)
{
	for (int history = 0; history < 2; history++) {
		TtRedoFixture f;
		PGAlignedBlock extra;
		uint32 segment = history ? 257 : 514;

		tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
		tt_redo_seal(&f);
		tt_inventory_extra(segment, history ? 2 : 3, &extra);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
		UT_ASSERT(!cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
		tt_inventory_remove(segment);
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_inventory_compares_every_untouched_current_and_history_byte)
{
	for (int history = 0; history < 2; history++) {
		TtRedoFixture f;
		PGAlignedBlock extra, changed;
		uint32 segment = history ? 257 : 514;

		tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
		tt_inventory_extra(segment, history ? 2 : 3, &extra);
		tt_redo_seed_recapture(&f);
		tt_redo_seal(&f);
		changed = extra;
		changed.data[BLCKSZ - 1] ^= 1;
		tt_readback_write(segment, &changed, false);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
		UT_ASSERT(!cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
		tt_inventory_remove(segment);
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_inventory_empty_capture_still_requires_exact_current_source)
{
	TtRedoFixture f;
	PGAlignedBlock saved;

	tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
	saved = f.base;
	tt_post_readback_empty_original(&f);
	UT_ASSERT(!cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
	UT_ASSERT(!cluster_cold_typed_tt_redo_inventory_v1(&f.typed)); /* unsealed */
	tt_redo_seal(&f);
	UT_ASSERT(cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
	f.typed.sources[0].claim.claim_sha256[0] ^= 1;
	UT_ASSERT(!cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
	f.typed.sources[0].claim.claim_sha256[0] ^= 1;
	f.typed.participants[0].native_redo++;
	UT_ASSERT(!cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
	f.typed.participants[0].native_redo--;
	f.typed.refusal = CLUSTER_COLD_SOURCE_GAP;
	UT_ASSERT(!cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
	f.typed.refusal = CLUSTER_COLD_OK;
	f.typed.replay_count = 0;
	UT_ASSERT(!cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
	f.typed.replay_count = 1;
	UT_ASSERT(!cluster_cold_typed_tt_redo_inventory_v1(NULL));
	tt_readback_write(513, &saved, true);
	tt_redo_destroy(&f);
}

UT_TEST(tt_inventory_refuses_ambiguous_current_origin_and_sticky_post_failure)
{
	TtRedoFixture f;
	PGAlignedBlock changed;
	ClusterColdParticipantV1 history;
	ClusterWalSourceRef source;
	RfSideOnlinePlanV1 *history_plan;

	tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
	tt_redo_seal(&f);
	history = f.typed.participants[1];
	source = f.typed.sources[1];
	history_plan = f.typed.side_plans[1];
	f.typed.participants[1] = f.typed.participants[0];
	f.typed.sources[1] = f.typed.sources[0];
	f.typed.side_plans[1] = f.typed.side_plans[0];
	f.typed.replay_count = 2;
	tt_redo_seed_recapture(&f);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
	UT_ASSERT(!cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
	f.typed.replay_count = 1;
	f.typed.participants[1] = history;
	f.typed.sources[1] = source;
	f.typed.side_plans[1] = history_plan;
	tt_redo_seed_recapture(&f);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
	UT_ASSERT(cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
	changed = f.base;
	changed.data[BLCKSZ - 1] ^= 1;
	tt_readback_write(513, &changed, false);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_recheck_v1(&f.typed), CLUSTER_COLD_SOURCE_GAP);
	tt_readback_write(513, &f.base, false);
	UT_ASSERT(!cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
	tt_redo_destroy(&f);
}

static void
tt_undo_readback_write(uint32 segment, uint32 block, const PGAlignedBlock *page)
{
	char path[MAXPGPATH];
	int fd;

	snprintf(path, sizeof(path), "%s/%u", tt_redo_dir, segment);
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pwrite(fd, page->data, BLCKSZ, (off_t)block * BLCKSZ), BLCKSZ);
	UT_ASSERT_EQ(close(fd), 0);
}

static void
tt_undo_readback_cache(TtRedoFixture *f, uint8 owner, uint32 segment, uint32 block,
					   const PGAlignedBlock *page)
{
	PGAlignedBlock saved;
	uint32 index, generation;

	tt_undo_readback_write(segment, block, page);
	UT_ASSERT_NOT_NULL(cluster_cold_tt_header_find_v1(&f->typed, false, owner, segment, &index));
	UT_ASSERT(cluster_cold_tt_base_block_v1(&f->typed, index, block, &saved, &generation));
	UT_ASSERT(memcmp(saved.data, page->data, BLCKSZ) == 0);
}

static void
tt_undo_readback_delta(TtRedoFixture *f, bool fpi, PGAlignedBlock *base, PGAlignedBlock *expected)
{
	PGAlignedBlock page;
	RfSideOnlineOperationV1 operation;

	tt_redo_fixture(f, 200, RF_SIDE_PLAN_RECOVERY);
	tt_redo_record_page(base, 802);
	((UndoBlockHeader *)base->data)->block_lsn = 90;
	tt_undo_readback_cache(f, 3, 513, 9, base);
	tt_redo_record_page(&page, 900);
	tt_redo_feed_record_page(f, 100, &page, fpi);
	tt_redo_seal(f);
	UT_ASSERT(rf_side_online_plan_operation_v1(f->typed.side_plans[0], 0, &operation));
	UT_ASSERT(cluster_undo_prepare_block_v1(
		&operation.undo, operation.owned_payload, operation.owned_payload_length,
		operation.identity.record.end_rec_ptr, base->data, expected->data));
}

UT_TEST(tt_inventory_undo_matches_native_delta_and_fpi_without_changing_base)
{
	for (int fpi = 0; fpi < 2; fpi++) {
		TtRedoFixture f;
		PGAlignedBlock base, expected;

		tt_undo_readback_delta(&f, fpi, &base, &expected);
		tt_undo_readback_write(513, 9, &expected);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
		UT_ASSERT(cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
		UT_ASSERT(memcmp(f.typed.tt_base->blocks->bytes.data, base.data, BLCKSZ) == 0);
		UT_ASSERT(memcmp(f.typed.tt_after_redo->blocks->bytes.data, expected.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_recheck_v1(&f.typed), CLUSTER_COLD_OK);
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_inventory_undo_refuses_missing_redo_and_any_post_block_byte_mismatch)
{
	for (int fault = 0; fault < 4; fault++) {
		TtRedoFixture f;
		PGAlignedBlock base, expected;

		tt_undo_readback_delta(&f, false, &base, &expected);
		if (fault == 0)
			expected = base;
		else if (fault == 1)
			expected.data[BLCKSZ - 1] ^= 1;
		else if (fault == 2)
			((UndoBlockHeader *)expected.data)->block_lsn++;
		else {
			UndoSlotDirEntry directory;

			memcpy(&directory, expected.data + UNDO_SLOT_DIR_OFFSET(1), sizeof(directory));
			expected.data[directory.record_offset] ^= 1;
		}
		tt_undo_readback_write(513, 9, &expected);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_recheck_v1(&f.typed), CLUSTER_COLD_OK);
		UT_ASSERT(!cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_inventory_undo_checks_all_cached_current_and_history_blocks)
{
	for (int fault = -1; fault < 3; fault++) {
		TtRedoFixture f;
		PGAlignedBlock page, history, changed;

		tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
		tt_inventory_extra(257, 2, &history);
		tt_redo_seed_recapture(&f);
		tt_redo_record_page(&page, 802);
		((UndoBlockHeader *)page.data)->block_lsn = 90;
		tt_undo_readback_cache(&f, 3, 513, 9, &page);
		tt_undo_readback_cache(&f, 3, 513, 10, &page);
		tt_undo_readback_cache(&f, 2, 257, 9, &page);
		tt_redo_seal(&f);
		if (fault >= 0) {
			changed = page;
			changed.data[BLCKSZ - 1] ^= 1;
			tt_undo_readback_write(fault == 2 ? 257 : 513, fault == 1 ? 10 : 9, &changed);
		}
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_recheck_v1(&f.typed), CLUSTER_COLD_OK);
		UT_ASSERT(cluster_cold_typed_tt_redo_inventory_v1(&f.typed) == (fault == -1));
		tt_inventory_remove(257);
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_inventory_undo_refuses_unusable_base_or_missing_post_pair)
{
	for (int fault = 0; fault < 3; fault++) {
		TtRedoFixture f;
		PGAlignedBlock page;

		tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
		tt_redo_record_page(&page, 802);
		((UndoBlockHeader *)page.data)->block_lsn = fault == 0 ? 0 : (fault == 1 ? 101 : 90);
		tt_undo_readback_cache(&f, 3, 513, 9, &page);
		tt_redo_seal(&f);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
		if (fault == 2) {
			pfree(f.typed.tt_after_redo->blocks);
			f.typed.tt_after_redo->blocks = NULL;
		}
		UT_ASSERT(!cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
		tt_redo_destroy(&f);
	}
}

static void
tt_undo_uncached_expected(TtRedoFixture *f, uint32 segment, uint32 block, PGAlignedBlock *expected)
{
	RfSideUndoBlockImageV1 image;

	UT_ASSERT_EQ(rf_side_online_plan_prepare_undo_block_v1(f->typed.side_plans[0], 3, segment,
														   block, &image),
				 RF_PAGE_PROOF_DETAIL_OK);
	*expected = image.page;
}

UT_TEST(tt_inventory_undo_retains_uncached_fpi_target_once)
{
	TtRedoFixture f;
	PGAlignedBlock page, expected;
	Size used;

	tt_redo_fixture(&f, 300, RF_SIDE_PLAN_RECOVERY);
	tt_redo_record_page(&page, 900);
	tt_redo_feed_record_page(&f, 100, &page, true);
	tt_redo_record_page(&page, 901);
	tt_redo_feed_record_page(&f, 200, &page, false);
	tt_redo_seal(&f);
	tt_undo_uncached_expected(&f, 513, 9, &expected);
	tt_undo_readback_write(513, 9, &expected);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
	UT_ASSERT(cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
	UT_ASSERT(f.typed.tt_base->blocks == NULL);
	UT_ASSERT_NOT_NULL(f.typed.tt_after_redo->blocks);
	if (f.typed.tt_after_redo->blocks == NULL) {
		tt_redo_destroy(&f);
		return;
	}
	UT_ASSERT(f.typed.tt_after_redo->blocks->next == NULL);
	UT_ASSERT(memcmp(f.typed.tt_after_redo->blocks->bytes.data, expected.data, BLCKSZ) == 0);
	used = f.typed.tt_after_redo->used;
	UT_ASSERT(cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
	UT_ASSERT_EQ(f.typed.tt_after_redo->used, used);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_recheck_v1(&f.typed), CLUSTER_COLD_OK);
	tt_redo_destroy(&f);
}

UT_TEST(tt_inventory_undo_refuses_uncached_missing_or_corrupt_fpi_effects)
{
	for (int fault = 0; fault < 4; fault++) {
		TtRedoFixture f;
		PGAlignedBlock page, expected;

		tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
		tt_redo_record_page(&page, 900);
		tt_redo_feed_record_page(&f, 100, &page, true);
		tt_redo_seal(&f);
		tt_undo_uncached_expected(&f, 513, 9, &expected);
		if (fault == 0)
			memset(expected.data, 0, BLCKSZ);
		else if (fault == 1)
			expected = page; /* Original FPI bytes lack the native redo LSN. */
		else
			expected.data[fault == 2 ? sizeof(UndoBlockHeader) : BLCKSZ - 1] ^= 1;
		tt_undo_readback_write(513, 9, &expected);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
		UT_ASSERT(!cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
		UT_ASSERT(f.typed.tt_base->blocks == NULL);
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_inventory_undo_refuses_uncached_delta_before_any_fpi)
{
	for (int later_fpi = 0; later_fpi < 2; later_fpi++) {
		TtRedoFixture f;
		PGAlignedBlock base, page, expected;
		RfSideOnlineOperationV1 operation;

		tt_redo_fixture(&f, later_fpi ? 300 : 200, RF_SIDE_PLAN_RECOVERY);
		tt_redo_record_page(&base, 802);
		((UndoBlockHeader *)base.data)->block_lsn = 90;
		tt_redo_record_page(&page, 900);
		tt_redo_feed_record_page(&f, 100, &page, false);
		if (later_fpi)
			tt_redo_feed_record_page(&f, 200, &page, true);
		tt_redo_seal(&f);
		UT_ASSERT(rf_side_online_plan_operation_v1(f.typed.side_plans[0], later_fpi, &operation));
		UT_ASSERT(cluster_undo_prepare_block_v1(
			&operation.undo, operation.owned_payload, operation.owned_payload_length,
			operation.identity.record.end_rec_ptr, later_fpi ? NULL : base.data, expected.data));
		tt_undo_readback_write(513, 9, &expected);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
		UT_ASSERT(!cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_inventory_undo_checks_uncached_targets_in_every_segment)
{
	for (int fault = -1; fault < 2; fault++) {
		TtRedoFixture f;
		PGAlignedBlock extra, page, expected;

		tt_redo_fixture(&f, 300, RF_SIDE_PLAN_RECOVERY);
		tt_inventory_extra(514, 3, &extra);
		tt_redo_seed_recapture(&f);
		tt_redo_record_page(&page, 900);
		tt_redo_feed_record_page_at(&f, 100, 513, 9, &page, true);
		tt_redo_feed_record_page_at(&f, 200, 514, 10, &page, true);
		tt_redo_seal(&f);
		for (int i = 0; i < 2; i++) {
			tt_undo_uncached_expected(&f, 513 + i, 9 + i, &expected);
			if (fault == i)
				expected.data[BLCKSZ - 1] ^= 1;
			tt_undo_readback_write(513 + i, 9 + i, &expected);
		}
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
		UT_ASSERT(cluster_cold_typed_tt_redo_inventory_v1(&f.typed) == (fault == -1));
		tt_inventory_remove(514);
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_inventory_undo_history_is_not_an_actual_target_but_can_anchor_native_fold)
{
	for (int replayed = 0; replayed < 2; replayed++) {
		TtRedoFixture f;
		PGAlignedBlock page, expected;
		RfSideOnlineOperationV1 operation;

		tt_redo_fixture_at(&f, replayed ? 300 : 200, RF_SIDE_PLAN_RECOVERY, 200);
		tt_redo_record_page(&page, 900);
		tt_redo_feed_record_page(&f, 100, &page, true);
		if (replayed)
			tt_redo_feed_record_page(&f, 200, &page, false);
		tt_redo_seal(&f);
		UT_ASSERT(rf_side_online_plan_operation_v1(f.typed.side_plans[0], 0, &operation));
		UT_ASSERT(operation.history_only);
		if (replayed) {
			tt_undo_uncached_expected(&f, 513, 9, &expected);
			tt_undo_readback_write(513, 9, &expected);
		}
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
		UT_ASSERT(cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
		UT_ASSERT((f.typed.tt_after_redo->blocks != NULL) == (replayed != 0));
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_inventory_undo_uncached_readback_preserves_budget_and_sticky_reread_failure)
{
	for (int capacity = 0; capacity < 2; capacity++) {
		TtRedoFixture f;
		PGAlignedBlock page, expected;

		tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
		tt_redo_record_page(&page, 900);
		tt_redo_feed_record_page(&f, 100, &page, true);
		tt_redo_seal(&f);
		tt_undo_uncached_expected(&f, 513, 9, &expected);
		tt_undo_readback_write(513, 9, &expected);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
		if (capacity) {
			f.typed.tt_after_redo->budget = f.typed.tt_after_redo->used;
			UT_ASSERT(!cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
			UT_ASSERT(f.typed.tt_after_redo->blocks == NULL);
		} else {
			UT_ASSERT(cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
			page = expected;
			page.data[BLCKSZ - 1] ^= 1;
			tt_undo_readback_write(513, 9, &page);
			UT_ASSERT(!cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
			tt_undo_readback_write(513, 9, &expected);
			UT_ASSERT(!cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
		}
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_inventory_requires_sealed_exact_history_sources_even_without_headers)
{
	for (int fault = 0; fault < 12; fault++) {
		TtRedoFixture f;

		tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
		tt_post_readback_empty_original(&f);
		tt_redo_seal(&f);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
		UT_ASSERT(cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
		switch (fault) {
		case 0:
			rf_side_online_plan_destroy_v1(&f.typed.side_plans[1]);
			break;
		case 1:
			f.typed.side_capture_detail[1] = RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED;
			break;
		case 2:
			f.typed.sources[1].claim.claim_sha256[0] ^= 1;
			break;
		case 3:
			f.typed.participants[1].owner_incarnation++;
			break;
		case 4:
			f.typed.participants[1].timeline++;
			break;
		case 5:
			f.typed.participants[1].physical_lower--;
			break;
		case 6:
			f.typed.participants[1].tail_end++;
			break;
		case 7:
			f.typed.participants[1].native_redo++;
			break;
		case 8:
			f.typed.sources[1].claim.database_incarnation++;
			break;
		case 9:
			tt_redo_history_plan(&f, 100, 100, 100, RF_SIDE_PLAN_RECOVERY, false);
			break;
		case 10:
			tt_redo_history_plan(&f, 100, 100, 100, RF_SIDE_PLAN_PI_CONTRIBUTIONS, true);
			break;
		case 11: {
			FakeXactRecord fake;
			tt_redo_history_plan(&f, 100, 200, 100, RF_SIDE_PLAN_RECOVERY, false);
			make_abort(&fake, 702);
			tt_redo_history_feed(&f, &fake);
			UT_ASSERT_EQ(rf_side_online_plan_seal_v1(f.typed.side_plans[1]),
						 RF_PAGE_PROOF_DETAIL_OK);
			break;
		}
		}
		UT_ASSERT(!cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
		tt_readback_write(513, &f.base, true);
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_inventory_accepts_ordinary_history_with_reused_or_absent_old_target)
{
	for (int terminal = 0; terminal < 2; terminal++) {
		TtRedoFixture f;
		FakeXactRecord fake;
		PGAlignedBlock reused;
		RfSideOnlineOperationV1 operation;

		tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
		tt_inventory_extra(257, 2, &reused);
		tt_redo_seed_recapture(&f);
		tt_redo_history_plan(&f, 100, 200, 200, RF_SIDE_PLAN_RECOVERY, false);
		if (terminal) {
			xl_xact_tt_commit delta;
			uint32 offset;

			undo_header_commit(&fake, 0, 702);
			offset = fake.u.decoded.main_data_len - sizeof(delta);
			memcpy(&delta, fake.data + offset, sizeof(delta));
			delta.instance = 2;
			delta.segment_id = 258; /* This old target is no longer present. */
			delta.wrap = 6;
			memcpy(fake.data + offset, &delta, sizeof(delta));
		} else {
			xl_undo_tt_slot_abort_exact abort = { 0 };

			abort.instance = 2;
			abort.segment_id = 257;
			abort.slot_offset = 47;
			abort.wrap = 6;
			abort.xid = 702;
			abort.format_version = CLUSTER_UNDO_TT_ABORT_EXACT_VERSION;
			make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_ABORT, &abort,
								   sizeof(abort));
		}
		tt_redo_history_feed(&f, &fake);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(f.typed.side_plans[1]), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(rf_side_online_plan_operation_v1(f.typed.side_plans[1], 0, &operation));
		UT_ASSERT(operation.history_only);
		tt_redo_seal(&f);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
		UT_ASSERT(cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_recheck_v1(&f.typed), CLUSTER_COLD_OK);
		tt_inventory_remove(257);
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_inventory_refuses_unsupported_historical_tt_carriers)
{
	for (int carrier = 0; carrier < 4; carrier++) {
		TtRedoFixture f;
		FakeXactRecord fake;
		xl_undo_tt_slot_commit legacy = { 0 };

		tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
		tt_redo_history_plan(&f, 100, 200, 200, RF_SIDE_PLAN_RECOVERY, false);
		if (carrier == 0)
			make_commit_prepared(&fake, 702, 16384, 901, 123456);
		else if (carrier == 1)
			make_abort_prepared(&fake, 702, 16384, 901);
		else {
			legacy.instance = 2;
			legacy.segment_id = 257;
			legacy.slot_offset = 4;
			legacy.wrap = 6;
			legacy.xid = 702;
			legacy.commit_scn = 999;
			make_projection_record(
				&fake, RM_CLUSTER_UNDO_ID,
				carrier == 2 ? XLOG_UNDO_TT_SLOT_COMMIT : XLOG_UNDO_TT_SLOT_ABORT, &legacy,
				carrier == 2 ? sizeof(legacy) : sizeof(xl_undo_tt_slot_abort));
		}
		tt_redo_history_feed(&f, &fake);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(f.typed.side_plans[1]), RF_PAGE_PROOF_DETAIL_OK);
		tt_redo_seal(&f);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
		UT_ASSERT(!cluster_cold_typed_tt_redo_inventory_v1(&f.typed));
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_top_tx_c0_producer_requires_complete_tt_inventory)
{
	for (int history = 0; history < 2; history++)
		for (int fault = 0; fault < 5; fault++) {
			TtRedoFixture f;
			PGAlignedBlock extra;
			ClusterColdTtSlotInputV1 input;
			ClusterColdTopTxProofV1 proof;
			ClusterRecoveryTTAbortV1 shape;
			ClusterColdTopTxC0ProducerResultV1 out, zero = { 0 };
			RfSideUndoHeaderImageV1 target;
			uint32 segment = history ? 257 : 514, index = UINT32_MAX;

			tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
			if (fault != 4) {
				tt_inventory_extra(segment, history ? 2 : 3, &extra);
				if (fault == 3) {
					/* Corrupt untouched bytes in both captures: equality is insufficient. */
					((UndoSegmentHeaderData *)extra.data)->tt_slots[47].flags = 0x80;
					tt_readback_write(segment, &extra, false);
				}
				tt_redo_seed_recapture(&f);
			}
			tt_redo_seal(&f);
			if (fault == 1)
				tt_inventory_remove(segment);
			else if (fault == 2) {
				extra.data[BLCKSZ - 1] ^= 1;
				tt_readback_write(segment, &extra, false);
			} else if (fault == 4)
				tt_inventory_extra(segment, history ? 2 : 3, &extra);
			UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
			UT_ASSERT_NOT_NULL(cluster_cold_tt_header_find_v1(&f.typed, true, 3, 513, &index));
			UT_ASSERT(cluster_cold_tt_after_redo_slot_v1(&f.typed, index, 47, &input));
			/* Every negative case still has an exact, fully folded target header. */
			UT_ASSERT(cluster_cold_typed_tt_after_redo_readback_v1(&f.typed, 0, index, &target));
			f.typed.xid_next_after_redo = UINT64_C(0x700400200);
			tt_top_tx_c0_inputs(&f, &input, &proof, &shape);
			tt_top_tx_effects_complete(&f);
			tt_top_tx_endpoints_complete(&f);
			memset(&out, 0xa9, sizeof(out));
			UT_ASSERT_EQ(cluster_cold_typed_top_tx_c0_producer_v1(&f.typed, 0, &proof, &shape,
																  false, InvalidTransactionId,
																  UINT64_C(0x12345678), &out),
						 fault == 0);
			if (fault != 0)
				UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
			if (fault != 1)
				tt_inventory_remove(segment);
			tt_redo_destroy(&f);
		}
}

UT_TEST(tt_top_tx_c0_producer_requires_all_original_sources)
{
	for (int fault = 0; fault < 6; fault++) {
		TtRedoFixture f;
		ClusterColdTtSlotInputV1 input;
		ClusterColdTopTxProofV1 proof;
		ClusterRecoveryTTAbortV1 shape;
		ClusterColdTopTxC0ProducerResultV1 out, zero = { 0 };
		RfSideUndoHeaderImageV1 target;

		tt_redo_fixture(&f, 100, RF_SIDE_PLAN_RECOVERY);
		tt_redo_seal(&f);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
		UT_ASSERT(cluster_cold_tt_after_redo_slot_v1(&f.typed, 0, 47, &input));
		f.typed.xid_next_after_redo = UINT64_C(0x700400200);
		tt_top_tx_c0_inputs(&f, &input, &proof, &shape);
		tt_top_tx_effects_complete(&f);
		tt_top_tx_endpoints_complete(&f);
		/* Retained generations matter even when none of their TT files survive. */
		switch (fault) {
		case 1:
			rf_side_online_plan_destroy_v1(&f.typed.side_plans[1]);
			break;
		case 2:
			f.typed.sources[1].claim.claim_sha256[0] ^= 1;
			break;
		case 3:
			f.typed.side_capture_detail[1] = RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED;
			break;
		case 4:
			tt_redo_history_plan(&f, 100, 100, 100, RF_SIDE_PLAN_RECOVERY, false);
			break;
		case 5:
			f.typed.participants[1].owner_incarnation++;
			break;
		}
		UT_ASSERT(cluster_cold_typed_tt_after_redo_readback_v1(&f.typed, 0, 0, &target));
		memset(&out, 0xa9, sizeof(out));
		UT_ASSERT_EQ(cluster_cold_typed_top_tx_c0_producer_v1(&f.typed, 0, &proof, &shape, false,
															  InvalidTransactionId,
															  UINT64_C(0x12345678), &out),
					 fault == 0);
		if (fault != 0)
			UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		tt_redo_destroy(&f);
	}
}

UT_TEST(tt_top_tx_c0_producer_checks_unselected_undo_effects)
{
	for (int fault = 0; fault < 3; fault++) {
		TtRedoFixture f;
		PGAlignedBlock page, expected;
		ClusterColdTtSlotInputV1 input;
		ClusterColdTopTxProofV1 proof;
		ClusterRecoveryTTAbortV1 shape;
		ClusterColdTopTxC0ProducerResultV1 out, zero = { 0 };
		RfSideUndoHeaderImageV1 target;

		tt_redo_fixture(&f, 200, RF_SIDE_PLAN_RECOVERY);
		tt_redo_record_page(&page, 900);
		tt_redo_feed_record_page(&f, 100, &page, true);
		tt_redo_seal(&f);
		tt_undo_uncached_expected(&f, 513, 9, &expected);
		if (fault == 1)
			expected = page; /* Missing native replay LSN, despite an exact TT header. */
		else if (fault == 2)
			expected.data[BLCKSZ - 1] ^= 1;
		tt_undo_readback_write(513, 9, &expected);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&f.typed), CLUSTER_COLD_OK);
		UT_ASSERT(f.typed.tt_base->blocks == NULL);
		UT_ASSERT(f.typed.tt_after_redo->blocks == NULL);
		UT_ASSERT(cluster_cold_tt_after_redo_slot_v1(&f.typed, 0, 47, &input));
		UT_ASSERT(cluster_cold_typed_tt_after_redo_readback_v1(&f.typed, 0, 0, &target));
		f.typed.xid_next_after_redo = UINT64_C(0x700400200);
		tt_top_tx_c0_inputs(&f, &input, &proof, &shape);
		tt_top_tx_effects_complete(&f);
		tt_top_tx_endpoints_complete(&f);
		memset(&out, 0xa9, sizeof(out));
		UT_ASSERT_EQ(cluster_cold_typed_top_tx_c0_producer_v1(&f.typed, 0, &proof, &shape, false,
															  InvalidTransactionId,
															  UINT64_C(0x12345678), &out),
					 fault == 0);
		if (fault != 0)
			UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		tt_redo_destroy(&f);
	}
}

int
main(void)
{
	UT_PLAN(105);
	UT_RUN(tt_top_tx_c0_producer_refuses_unproved_endpoint_coverage);
	UT_RUN(tt_top_tx_c0_producer_refuses_unproved_effect_coverage);
	UT_RUN(tt_top_tx_c0_producer_requires_complete_tt_inventory);
	UT_RUN(tt_top_tx_c0_producer_requires_all_original_sources);
	UT_RUN(tt_top_tx_c0_producer_checks_unselected_undo_effects);
	UT_RUN(tt_inventory_requires_sealed_exact_history_sources_even_without_headers);
	UT_RUN(tt_inventory_accepts_ordinary_history_with_reused_or_absent_old_target);
	UT_RUN(tt_inventory_refuses_unsupported_historical_tt_carriers);
	UT_RUN(tt_inventory_undo_retains_uncached_fpi_target_once);
	UT_RUN(tt_inventory_undo_refuses_uncached_missing_or_corrupt_fpi_effects);
	UT_RUN(tt_inventory_undo_refuses_uncached_delta_before_any_fpi);
	UT_RUN(tt_inventory_undo_checks_uncached_targets_in_every_segment);
	UT_RUN(tt_inventory_undo_history_is_not_an_actual_target_but_can_anchor_native_fold);
	UT_RUN(tt_inventory_undo_uncached_readback_preserves_budget_and_sticky_reread_failure);
	UT_RUN(tt_inventory_undo_matches_native_delta_and_fpi_without_changing_base);
	UT_RUN(tt_inventory_undo_refuses_missing_redo_and_any_post_block_byte_mismatch);
	UT_RUN(tt_inventory_undo_checks_all_cached_current_and_history_blocks);
	UT_RUN(tt_inventory_undo_refuses_unusable_base_or_missing_post_pair);
	UT_RUN(tt_inventory_checks_untouched_current_and_history_headers);
	UT_RUN(tt_inventory_refuses_unchanged_current_header_with_bad_layout);
	UT_RUN(tt_inventory_refuses_unchanged_history_header_with_bad_layout);
	UT_RUN(tt_inventory_accepts_known_layout_for_all_slots_and_generation_zero);
	UT_RUN(tt_inventory_matches_new_native_full_image_and_terminal);
	UT_RUN(tt_inventory_refuses_disappeared_original_even_when_selected_header_matches);
	UT_RUN(tt_inventory_requires_current_wal_targets_even_with_empty_observed_inventory);
	UT_RUN(tt_inventory_refuses_unexplained_new_current_or_history_segment);
	UT_RUN(tt_inventory_compares_every_untouched_current_and_history_byte);
	UT_RUN(tt_inventory_empty_capture_still_requires_exact_current_source);
	UT_RUN(tt_inventory_refuses_ambiguous_current_origin_and_sticky_post_failure);
	UT_RUN(tt_post_readback_reconstructs_new_init_bind_and_commit_without_base);
	UT_RUN(tt_post_readback_selects_existing_base_by_physical_identity);
	UT_RUN(tt_post_readback_never_seeds_missing_base_from_observed_bytes);
	UT_RUN(tt_post_readback_new_header_requires_every_slot_and_untouched_byte);
	UT_RUN(tt_post_readback_new_header_retains_original_source_and_namespace_guards);
	UT_RUN(tt_post_readback_new_header_retains_native_lifecycle_refusals);
	UT_RUN(tt_post_readback_requires_valid_post_capture_and_keeps_final_refusal);
	UT_RUN(tt_readback_matches_physical_commit_and_preserves_original);
	UT_RUN(tt_readback_matches_exact_abort_and_unchanged_active_slots);
	UT_RUN(tt_readback_refuses_every_slot_and_non_slot_byte_mismatch);
	UT_RUN(tt_readback_looks_up_physical_identity_in_independent_inventory);
	UT_RUN(tt_readback_requires_original_source_namespace_and_valid_snapshots);
	UT_RUN(tt_readback_retains_final_recheck_failure_and_clears_bad_outputs);
	UT_RUN(tt_redo_base_graph_walks_original_chain_across_physical_generations);
	UT_RUN(tt_redo_base_graph_seeds_all_segments_slot_edges_and_terminal_heads_after_redo);
	UT_RUN(tt_redo_base_graph_rejects_exact_key_source_and_bad_arguments);
	UT_RUN(tt_redo_base_graph_verification_detects_full_block_drift_and_stays_failed);
	UT_RUN(tt_redo_base_graph_rejects_claim_drift_during_verification);
	UT_RUN(tt_redo_base_graph_never_treats_refused_record_as_empty_chain);
	UT_RUN(tt_redo_base_graph_factory_clears_failures_and_requires_source_with_empty_inventory);
	UT_RUN(tt_redo_base_only_retains_precheckpoint_records_and_owned_bytes);
	UT_RUN(tt_redo_base_delta_and_fpi_preserve_order_untouched_rows_and_disk);
	UT_RUN(tt_redo_base_refuses_zero_ahead_malformed_and_missing_original);
	UT_RUN(tt_redo_base_delta_introduces_record_in_captured_nonzero_generation);
	UT_RUN(tt_redo_base_rechecks_full_bytes_on_every_read_and_final_recheck);
	UT_RUN(tt_redo_base_refuses_capacity_and_source_drift);
	UT_RUN(tt_redo_base_refuses_prefix_without_captured_slot_history);
	UT_RUN(tt_redo_base_keeps_ancestry_after_prefix_init);
	UT_RUN(tt_redo_base_keeps_ancestry_after_prefix_reuse);
	UT_RUN(tt_redo_base_refuses_current_init_even_with_a_later_fpi);
	UT_RUN(tt_redo_base_refuses_lifecycle_even_with_a_later_fpi);
	UT_RUN(tt_redo_base_refuses_bad_arguments_with_cleared_outputs);
	UT_RUN(tt_redo_commit_without_bind_keeps_original_all_slots_and_heads);
	UT_RUN(tt_redo_unrelated_terminal_preserves_active_input);
	UT_RUN(tt_redo_empty_cut_preserves_input_and_requires_canonical_flags);
	UT_RUN(tt_redo_exact_abort_uses_original_active_slot_without_bind);
	UT_RUN(tt_redo_rejects_each_source_and_checkpoint_identity_drift);
	UT_RUN(tt_redo_rejects_exact_root_claim_drift_with_unchanged_cut);
	UT_RUN(tt_redo_claim_matches_bound_copy_in_any_order_not_history);
	UT_RUN(tt_redo_claim_match_refuses_unbound_pi_and_bad_arguments);
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
	UT_RUN(tt_redo_active_identity_requires_post_redo_active_physical_slot);
	UT_RUN(tt_top_tx_loser_requires_all_proof_gates_and_ignores_full_xid);
	UT_RUN(tt_top_tx_c0_requires_physical_loser_and_complete_endpoints);
	UT_RUN(tt_top_tx_c0_producer_requires_original_owner_and_exact_post_redo);
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
