/*-------------------------------------------------------------------------
 * test_cluster_page_image_coverage.c
 *    Exact in-memory full-image coverage of a retained PI WAL range.
 *
 * The real immutable plan queue/seal and coverage consumer run here. Detached
 * redo and identity leaves use the original online-plan component fixture;
 * this does not qualify native current authority or physical durability.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "test_cluster_page_online_plan_fixture.inc"
#include "cluster/cluster_page_wal.h"

/* A new-interface baseline must fail as TAP, rather than fail to link. Never
 * substitute a permissive coverage implementation for the original owner. */
#ifndef CLUSTER_PAGE_IMAGE_COVERAGE_V1
/* The baseline has no such query. This adapter is fail-stop and is never
 * used after the real interface exists, including assertion-OFF builds. */
static bool
rf_page_online_plan_image_covers_v1(const RfPageOnlinePlanV1 *plan,
	const ClusterPageWalBindingV1 *image, const char page[BLCKSZ],
	const ClusterPageWalBindingV1 *first, const ClusterPageWalBindingV1 *last)
{
	UT_ASSERT(false);
	return false;
}
#endif

typedef struct ImageCoverageFixture {
	RfPageOnlinePlanV1 *plan;
	ClusterWalSourceRef sources[2];
	ClusterPageWalBindingV1 bindings[4];
	PGAlignedBlock image;
} ImageCoverageFixture;

static bool
image_coverage_available(void)
{
#ifndef CLUSTER_PAGE_IMAGE_COVERAGE_V1
	UT_ASSERT(false); /* Missing original-owner coverage interface. */
	return false;
#else
	return true;
#endif
}

static void
image_source(ClusterWalSourceRef *source, uint16 thread)
{
	ClusterControlRootIdentity *id;
	memset(source, 0, sizeof(*source));
	id = &source->claim.identity;
	id->system_identifier = 99;
	memset(id->storage_uuid, 3, 16);
	memset(id->authority_uuid, thread, 16);
	id->origin_thread_id = thread;
	id->origin_node_id = thread - 1;
	id->thread_claim_created_at = 100 + thread;
	id->thread_claim_crc32c = 200 + thread;
	id->origin_owner_incarnation = 70 + thread;
	id->root_lineage_seq = 300 + thread;
	source->claim.database_incarnation = 5;
	source->claim.max_config_generation = 8;
	memset(source->claim.claim_sha256, thread + 4, 32);
	source->timeline = 1;
}

static bool
image_start_internal(ImageCoverageFixture *f, bool bind, bool seal, ForkNumber forknum,
	bool history_only, uint8 incarnation)
{
	RfContributorStreamCutV1 cuts[2] = {{0}};
	XLogRecPtr redo[2] = {0xc00, 0x200};
	RfPageOnlinePlanRequestV1 request = {0};
	static const uint64 tokens[5] = {10, 900, 7, 20, 3};
	static const XLogRecPtr starts[4] = {0x900, 0x100, 0xa00, 0xb00};
	static const XLogRecPtr ends[4] = {0xa00, 0x200, 0xb00, 0xc00};
	/* Arrival and version/LSN magnitudes deliberately disagree with ancestry. */
	static const unsigned arrival[4] = {1, 0, 2, 3};
	RfPageOnlineTargetViewV1 target = {0};
	memset(f, 0, sizeof(*f));
	for (unsigned i = 0; i < 2; i++) {
		image_source(&f->sources[i], i + 1);
		cuts[i].failed_thread = i + 1;
		cuts[i].timeline_id = 1;
		cuts[i].flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		cuts[i].origin_owner_incarnation = 71 + i;
		cuts[i].scan_begin_inclusive = i == 0 ? 0x900 : 0x100;
		cuts[i].scan_end_exclusive = i == 0 ? 0xc00 : 0x200;
	}
	request.system_identifier = 99;
	memset(request.storage_uuid, 3, 16);
	request.physical_cuts = cuts;
	request.participant_count = 2;
	request.retention_binding_cookie = 41;
	if (history_only) request.redo_starts = redo;
	UT_ASSERT_EQ(rf_page_online_plan_create_v1(&request, &f->plan), RF_PAGE_PROOF_DETAIL_OK);
	if (f->plan == NULL) return false;
	if (bind) UT_ASSERT(rf_page_online_plan_bind_sources_v1(f->plan, f->sources, 2));
	for (unsigned i = 0; i < 4; i++) {
		unsigned n = history_only ? i : arrival[i];
		unsigned participant = n == 1 ? 1 : 0;
		FakeRecord record;
		RfPageOnlineRecordIdentityV1 id;
		RfPageProofDetailV1 detail;
		ClusterPageWalBindingV1 *binding = &f->bindings[n];
		init_record(&record, starts[n], ends[n], 1);
		set_component(&record, 0, 10, 4, tokens[n], tokens[n + 1], n == 0);
		if (n >= 2) memset(record.plan.components[0].before.segment_incarnation, incarnation, 16);
		if (n >= 1) memset(record.plan.components[0].result.segment_incarnation, incarnation, 16);
		record.storage.decoded.blocks[0].forknum = forknum;
		id = record_identity(&record);
		id.participant_index = participant;
		id.record.origin_thread = participant + 1;
		/* Ordered history remains inspectable in the original feed API;
		 * the closed queue correctly removes unrelated completed targets. */
		detail = history_only ? rf_page_online_plan_feed_record_v1(f->plan, &record.plan, &id)
			: rf_page_online_plan_queue_record_v1(f->plan, &record.plan, &id);
		UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
		if (detail != RF_PAGE_PROOF_DETAIL_OK) {
			rf_page_online_plan_destroy_v1(&f->plan);
			return false;
		}
		binding->source = f->sources[participant];
		binding->identity.system_identifier = 99;
		memset(binding->identity.storage_uuid, 3, 16);
		binding->identity.locator = record.storage.decoded.blocks[0].rlocator;
		binding->identity.forknum = forknum;
		binding->identity.blockno = 4;
		binding->version = record.plan.components[0].result;
		binding->record_start = starts[n];
		binding->record_end = ends[n];
		binding->record_crc = id.record.record_crc;
		binding->rmid = id.record.rmid;
		binding->info = id.record.info;
	}
	if (!seal) return true;
	UT_ASSERT_EQ(rf_page_online_plan_seal_v1(f->plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_page_online_plan_target_v1(f->plan, 0, &target));
	if (target.canonical_page == NULL) return false;
	memcpy(f->image.data, target.canonical_page, BLCKSZ);
	return true;
}

static bool
image_start(ImageCoverageFixture *f, bool bind, bool seal, ForkNumber forknum)
{
	return image_start_internal(f, bind, seal, forknum, false, 7);
}

static bool
image_covers(const ImageCoverageFixture *f, const ClusterPageWalBindingV1 *first,
	const ClusterPageWalBindingV1 *last)
{
	return rf_page_online_plan_image_covers_v1(f->plan, &f->bindings[3], f->image.data,
		first, last);
}

UT_TEST(test_full_image_covers_complete_cross_source_range)
{
	ImageCoverageFixture f;
	if (!image_coverage_available() || !image_start(&f, true, true, MAIN_FORKNUM)) return;
	UT_ASSERT(image_covers(&f, &f.bindings[0], &f.bindings[2]));
	UT_ASSERT(image_covers(&f, &f.bindings[1], &f.bindings[2]));
	UT_ASSERT(image_covers(&f, &f.bindings[0], &f.bindings[3]));
	rf_page_online_plan_destroy_v1(&f.plan);
}

UT_TEST(test_same_exact_version_is_coverage_without_durability)
{
	ImageCoverageFixture f;
	if (!image_coverage_available() || !image_start(&f, true, true, MAIN_FORKNUM)) return;
	UT_ASSERT_EQ(f.bindings[3].flags, 0);
	UT_ASSERT(image_covers(&f, &f.bindings[3], &f.bindings[3]));
	/* Flush certification is independent of the mutation/ancestry claim. */
	f.bindings[3].flags = CLUSTER_PAGE_WAL_NATIVE_FLUSHED;
	UT_ASSERT(image_covers(&f, &f.bindings[0], &f.bindings[2]));
	rf_page_online_plan_destroy_v1(&f.plan);
}

UT_TEST(test_full_original_source_claim_must_match_at_every_endpoint)
{
	ImageCoverageFixture f;
	if (!image_coverage_available() || !image_start(&f, true, true, MAIN_FORKNUM)) return;
	for (unsigned n = 0; n < 4; n++) {
		ClusterPageWalBindingV1 valid = f.bindings[n];
		ClusterPageWalBindingV1 changed;
		ClusterPageWalBindingV1 *b = &f.bindings[n];
		const ClusterPageWalBindingV1 *last = &f.bindings[n == 1 ? 1 : 2];
#define REJECT_SOURCE(statement_) \
		do { changed = valid; statement_; *b = changed; \
			UT_ASSERT(!image_covers(&f, &f.bindings[0], last)); } while (0)
		REJECT_SOURCE(changed.source.claim.identity.authority_uuid[0]++);
		REJECT_SOURCE(changed.source.claim.identity.origin_owner_incarnation++);
		REJECT_SOURCE(changed.source.claim.identity.origin_node_id++);
		REJECT_SOURCE(changed.source.claim.identity.origin_thread_id++);
		REJECT_SOURCE(changed.source.claim.identity.thread_claim_created_at++);
		REJECT_SOURCE(changed.source.claim.identity.thread_claim_crc32c++);
		REJECT_SOURCE(changed.source.claim.identity.root_lineage_seq++);
		REJECT_SOURCE(changed.source.claim.database_incarnation++);
		REJECT_SOURCE(changed.source.claim.claim_sha256[0]++);
		REJECT_SOURCE(changed.source.claim.max_config_generation++);
		REJECT_SOURCE(changed.source.timeline++);
		*b = valid;
#undef REJECT_SOURCE
		/* The selected immutable claim may carry a later decoder ceiling. */
		b->source.claim.max_config_generation--;
		UT_ASSERT(image_covers(&f, &f.bindings[0], last));
		*b = valid;
	}
	UT_ASSERT(image_covers(&f, &f.bindings[0], &f.bindings[2]));
	rf_page_online_plan_destroy_v1(&f.plan);
}

UT_TEST(test_record_tuple_and_physical_identity_must_match)
{
	ImageCoverageFixture f;
	if (!image_coverage_available() || !image_start(&f, true, true, MAIN_FORKNUM)) return;
	for (unsigned n = 0; n < 4; n++) {
		ClusterPageWalBindingV1 valid = f.bindings[n], changed;
#define REJECT_BINDING(statement_) \
		do { changed = valid; statement_; f.bindings[n] = changed; \
			UT_ASSERT(!image_covers(&f, &f.bindings[0], &f.bindings[n == 1 ? 1 : 2])); } while (0)
		REJECT_BINDING(changed.record_start++);
		REJECT_BINDING(changed.record_end++);
		REJECT_BINDING(changed.record_crc++);
		REJECT_BINDING(changed.rmid++);
		REJECT_BINDING(changed.info++);
		REJECT_BINDING(changed.version.segment_incarnation[0]++);
		REJECT_BINDING(changed.version.mutation_token++);
		REJECT_BINDING(changed.identity.blockno++);
		REJECT_BINDING(changed.identity.locator.relNumber++);
		REJECT_BINDING(changed.identity.storage_uuid[0]++);
		REJECT_BINDING(changed.identity.reserved_zero++);
		REJECT_BINDING(changed.flags = 2);
		f.bindings[n] = valid;
#undef REJECT_BINDING
	}
	rf_page_online_plan_destroy_v1(&f.plan);
}

UT_TEST(test_reversed_range_and_unknown_version_are_not_coverage)
{
	ImageCoverageFixture f;
	ClusterPageWalBindingV1 unknown;
	if (!image_coverage_available() || !image_start(&f, true, true, MAIN_FORKNUM)) return;
	UT_ASSERT(!image_covers(&f, &f.bindings[2], &f.bindings[0]));
	UT_ASSERT(!image_covers(&f, &f.bindings[3], &f.bindings[2]));
	unknown = f.bindings[2]; unknown.version.mutation_token = UINT64_MAX;
	UT_ASSERT(!image_covers(&f, &f.bindings[0], &unknown));
	unknown = f.bindings[0]; unknown.version.mutation_token = 1;
	UT_ASSERT(!image_covers(&f, &unknown, &f.bindings[2]));
	rf_page_online_plan_destroy_v1(&f.plan);
}

UT_TEST(test_earlier_image_cannot_claim_later_terminal_bytes)
{
	ImageCoverageFixture f;
	ClusterPageWalBindingV1 terminal;
	if (!image_coverage_available() || !image_start(&f, true, true, MAIN_FORKNUM)) return;
	terminal = f.bindings[3]; f.bindings[3] = f.bindings[2];
	UT_ASSERT(!image_covers(&f, &f.bindings[0], &f.bindings[2]));
	f.bindings[3] = terminal;
	UT_ASSERT(image_covers(&f, &f.bindings[0], &f.bindings[2]));
	rf_page_online_plan_destroy_v1(&f.plan);
}

UT_TEST(test_all_image_bytes_and_page_coordinates_are_bound)
{
	ImageCoverageFixture f;
	PGAlignedBlock saved;
	if (!image_coverage_available() || !image_start(&f, true, true, MAIN_FORKNUM)) return;
	saved = f.image;
	for (unsigned offset = 0; offset < BLCKSZ; offset++) {
		f.image.data[offset] ^= 1;
		UT_ASSERT(!image_covers(&f, &f.bindings[0], &f.bindings[2]));
		f.image.data[offset] ^= 1;
	}
	UT_ASSERT(memcmp(f.image.data, saved.data, BLCKSZ) == 0);
	UT_ASSERT(image_covers(&f, &f.bindings[0], &f.bindings[2]));
	rf_page_online_plan_destroy_v1(&f.plan);
}

UT_TEST(test_unsealed_or_unbound_plan_is_not_a_proof)
{
	ImageCoverageFixture f;
	if (!image_coverage_available() || !image_start(&f, true, false, MAIN_FORKNUM)) return;
	UT_ASSERT(!image_covers(&f, &f.bindings[0], &f.bindings[2]));
	rf_page_online_plan_destroy_v1(&f.plan);
	f.plan = create_plan(0x100, 0x200);
	UT_ASSERT(!image_covers(&f, &f.bindings[0], &f.bindings[2]));
	rf_page_online_plan_destroy_v1(&f.plan);
	if (!image_start(&f, false, true, MAIN_FORKNUM)) return;
	UT_ASSERT(!image_covers(&f, &f.bindings[0], &f.bindings[2]));
	rf_page_online_plan_destroy_v1(&f.plan);
}

UT_TEST(test_null_or_malformed_input_never_creates_coverage)
{
	ImageCoverageFixture f;
	if (!image_coverage_available() || !image_start(&f, true, true, MAIN_FORKNUM)) return;
	UT_ASSERT(!rf_page_online_plan_image_covers_v1(NULL, &f.bindings[3], f.image.data,
		&f.bindings[0], &f.bindings[2]));
	UT_ASSERT(!rf_page_online_plan_image_covers_v1(f.plan, NULL, f.image.data,
		&f.bindings[0], &f.bindings[2]));
	UT_ASSERT(!rf_page_online_plan_image_covers_v1(f.plan, &f.bindings[3], NULL,
		&f.bindings[0], &f.bindings[2]));
	UT_ASSERT(!image_covers(&f, NULL, &f.bindings[2]));
	UT_ASSERT(!image_covers(&f, &f.bindings[0], NULL));
	memset(&f.bindings[0], 0, sizeof(f.bindings[0]));
	UT_ASSERT(!image_covers(&f, &f.bindings[0], &f.bindings[2]));
	rf_page_online_plan_destroy_v1(&f.plan);
}

UT_TEST(test_history_only_target_cannot_publish_a_current_image)
{
	ImageCoverageFixture f;
	RfPageOnlineTargetViewV1 target = {0};
	if (!image_coverage_available()
		|| !image_start_internal(&f, true, true, MAIN_FORKNUM, true, 7)) return;
	UT_ASSERT(rf_page_online_plan_target_v1(f.plan, 0, &target));
	UT_ASSERT(target.history_only);
	UT_ASSERT(!image_covers(&f, &f.bindings[0], &f.bindings[2]));
	rf_page_online_plan_destroy_v1(&f.plan);
}

UT_TEST(test_incarnation_change_requires_its_separate_structural_proof)
{
	ImageCoverageFixture f;
	if (!image_coverage_available()
		|| !image_start_internal(&f, true, true, MAIN_FORKNUM, false, 8)) return;
	UT_ASSERT_NE(f.bindings[0].version.segment_incarnation[0],
		f.bindings[3].version.segment_incarnation[0]);
	/* A PAGE-only chain is not the structural/SIDE proof for the old
	 * incarnation, even with exact records and terminal bytes. */
	UT_ASSERT(!image_covers(&f, &f.bindings[0], &f.bindings[2]));
	UT_ASSERT(image_covers(&f, &f.bindings[1], &f.bindings[2]));
	rf_page_online_plan_destroy_v1(&f.plan);
}

UT_TEST(test_vm_uses_the_same_full_page_dependency_proof)
{
	ImageCoverageFixture f;
	if (!image_coverage_available() || !image_start(&f, true, true, VISIBILITYMAP_FORKNUM)) return;
	UT_ASSERT(image_covers(&f, &f.bindings[0], &f.bindings[2]));
	f.bindings[0].identity.forknum = MAIN_FORKNUM;
	UT_ASSERT(!image_covers(&f, &f.bindings[0], &f.bindings[2]));
	rf_page_online_plan_destroy_v1(&f.plan);
}

UT_TEST(test_space_cannot_borrow_a_page_only_coverage_proof)
{
	ImageCoverageFixture f;
	if (!image_coverage_available() || !image_start(&f, true, true, MAIN_FORKNUM)) return;
	for (unsigned i = 0; i < 4; i++) {
		f.bindings[i].identity.forknum = SPACE_FORKNUM;
		f.bindings[i].identity.blockno = 0;
	}
	UT_ASSERT(!image_covers(&f, &f.bindings[0], &f.bindings[2]));
	rf_page_online_plan_destroy_v1(&f.plan);
}

UT_TEST(test_queries_preserve_first_last_bytes_and_plan)
{
	ImageCoverageFixture f;
	ClusterPageWalBindingV1 bindings[4];
	PGAlignedBlock image;
	RfPageOnlineTargetViewV1 before, after;
	if (!image_coverage_available() || !image_start(&f, true, true, MAIN_FORKNUM)) return;
	memcpy(bindings, f.bindings, sizeof(bindings)); image = f.image;
	UT_ASSERT(rf_page_online_plan_target_v1(f.plan, 0, &before));
	UT_ASSERT(image_covers(&f, &f.bindings[0], &f.bindings[2]));
	UT_ASSERT(!image_covers(&f, &f.bindings[2], &f.bindings[0]));
	UT_ASSERT(rf_page_online_plan_target_v1(f.plan, 0, &after));
	UT_ASSERT(memcmp(bindings, f.bindings, sizeof(bindings)) == 0);
	UT_ASSERT(memcmp(image.data, f.image.data, BLCKSZ) == 0);
	UT_ASSERT(memcmp(before.canonical_page, after.canonical_page, BLCKSZ) == 0);
	UT_ASSERT_EQ(before.contributors->edge_count, after.contributors->edge_count);
	UT_ASSERT_EQ(before.expected_result.mutation_token, after.expected_result.mutation_token);
	rf_page_online_plan_destroy_v1(&f.plan);
}

int
main(void)
{
	UT_PLAN(14);
	UT_RUN(test_full_image_covers_complete_cross_source_range);
	UT_RUN(test_same_exact_version_is_coverage_without_durability);
	UT_RUN(test_full_original_source_claim_must_match_at_every_endpoint);
	UT_RUN(test_record_tuple_and_physical_identity_must_match);
	UT_RUN(test_reversed_range_and_unknown_version_are_not_coverage);
	UT_RUN(test_earlier_image_cannot_claim_later_terminal_bytes);
	UT_RUN(test_all_image_bytes_and_page_coordinates_are_bound);
	UT_RUN(test_unsealed_or_unbound_plan_is_not_a_proof);
	UT_RUN(test_null_or_malformed_input_never_creates_coverage);
	UT_RUN(test_history_only_target_cannot_publish_a_current_image);
	UT_RUN(test_incarnation_change_requires_its_separate_structural_proof);
	UT_RUN(test_vm_uses_the_same_full_page_dependency_proof);
	UT_RUN(test_space_cannot_borrow_a_page_only_coverage_proof);
	UT_RUN(test_queries_preserve_first_last_bytes_and_plan);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
