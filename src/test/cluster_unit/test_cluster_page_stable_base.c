/*-------------------------------------------------------------------------
 *
 * test_cluster_page_stable_base.c
 *    STOP-06 stable-base graph/selector semantic capability RED.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/xlogreader.h"

#if defined(__has_include)
#if __has_include("cluster/cluster_page_stable_base.h")
#include "cluster/cluster_external_fence.h"
#include "cluster/cluster_page_stable_base.h"
#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_wal_retention.h"
#define TEST_HAVE_CLUSTER_PAGE_STABLE_BASE 1
#endif
#endif

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# Assert failed: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

int
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

bool
errstart_cold(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

#ifndef TEST_HAVE_CLUSTER_PAGE_STABLE_BASE

UT_TEST(test_stable_base_capability_red)
{
	printf("# JIT_SEMANTIC_RED:D6-STABLE-BASE-GRAPH-SELECTOR\n");
	UT_ASSERT(false);
}

int
main(void)
{
	UT_PLAN(1);
	UT_RUN(test_stable_base_capability_red);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}

#else

static const ClusterControlRootReadToken *expected_root_tokens;
static const ClusterRecoveryDutyKey *expected_duties;
static const ClusterFormationWitnessV1 *expected_formation;
static const PgracExternalFenceNeedSetV1 *expected_needs;
static const PgracExternalFenceAdmissionSetV1 *expected_admissions;
static ClusterWalRetentionPin *expected_pin;
static bool canonical_root_current;
static bool bound_pin_current;
static int preflight_pin_checks;
static int bound_pin_checks;

ClusterControlRootResult
cluster_control_root_revalidate(const ClusterControlRootReadToken *token,
								const ClusterControlRootIdentity *identity,
								ClusterControlRootSnapshot *snapshot)
{
	if (!canonical_root_current || token != expected_root_tokens || identity != expected_duties)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (snapshot != NULL) {
		memset(snapshot, 0, sizeof(*snapshot));
		snapshot->identity = *identity;
		snapshot->lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
		snapshot->root_flags = CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID;
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterFormationWitnessResult
cluster_formation_witness_revalidate_nowait(const ClusterFormationWitnessV1 *formation)
{
	return formation == expected_formation ? CLUSTER_FORMATION_WITNESS_READY
										   : CLUSTER_FORMATION_WITNESS_UNSTABLE;
}

bool
cluster_external_fence_need_set_revalidate_nowait(const PgracExternalFenceNeedSetV1 *needs,
												  const ClusterFormationWitnessV1 *formation,
												  PgracExternalFenceDenyReason *reason)
{
	if (reason != NULL)
		*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	return needs == expected_needs && formation == expected_formation;
}

bool
cluster_external_fence_revalidate_set_nowait(const PgracExternalFenceAdmissionSetV1 *admissions,
											 const PgracExternalFenceNeedSetV1 *needs,
											 const ClusterFormationWitnessV1 *formation,
											 PgracExternalFenceDenyReason *reason)
{
	if (reason != NULL)
		*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	return admissions == expected_admissions && needs == expected_needs
		   && formation == expected_formation;
}

ClusterWalPinResult
cluster_wal_retention_pin_preflight_revalidate_wait_v1(ClusterWalRetentionPin *pin)
{
	preflight_pin_checks++;
	return pin == expected_pin ? CLUSTER_WAL_PIN_OK : CLUSTER_WAL_PIN_STALE;
}

ClusterWalPinResult
cluster_wal_retention_pin_revalidate(ClusterWalRetentionPin *pin)
{
	bound_pin_checks++;
	return bound_pin_current && pin == expected_pin ? CLUSTER_WAL_PIN_OK : CLUSTER_WAL_PIN_STALE;
}

typedef struct GraphFixture {
	RfPageIdentityV1 identity;
	RfContributorStreamCutV1 cuts[4];
	RfPageStableEdgeInputV1 edges[8];
	RfContributorVectorV1 vector;
	RfPagePinnedSourceV1 source;
	RfPageStableGraphRequestV1 request;
	ClusterRecoveryDutyKey duties[4];
	ClusterControlRootReadToken root_tokens[4];
	char formation_object;
	char needs_object;
	char admission_object;
	char pin_object;
	uint32 chain[16];
	RfPageStableSelectionV1 selection;
} GraphFixture;

static void
set_incarnation(uint8 incarnation[16], uint8 seed)
{
	int i;

	for (i = 0; i < 16; i++)
		incarnation[i] = seed + i;
}

static RfPageVersionV1
make_version(uint8 seed, uint64 token)
{
	RfPageVersionV1 version;

	memset(&version, 0, sizeof(version));
	set_incarnation(version.segment_incarnation, seed);
	version.mutation_token = token;
	return version;
}

static RfPageIdentityV1
make_identity(BlockNumber blockno)
{
	RfPageIdentityV1 identity;

	memset(&identity, 0, sizeof(identity));
	identity.system_identifier = 9001;
	set_incarnation(identity.storage_uuid, 0x20);
	identity.locator.spcOid = 1663;
	identity.locator.dbOid = 5;
	identity.locator.relNumber = 17;
	identity.forknum = MAIN_FORKNUM;
	identity.blockno = blockno;
	return identity;
}

static void
set_edge(RfPageStableEdgeInputV1 *edge, const RfPageIdentityV1 *identity, uint8 before_kind,
		 const RfPageVersionV1 *before, const RfPageVersionV1 *result, uint16 flags,
		 uint64 record_identity, uint16 participant_index)
{
	memset(edge, 0, sizeof(*edge));
	edge->page_identity = *identity;
	edge->edge.block_id = 0;
	edge->edge.page_class = RF_PAGE_CLASS_ORDINARY;
	edge->edge.before_kind = before_kind;
	edge->edge.result_kind = RF_PAGE_STATE_PRESENT;
	edge->edge.edge_flags = flags;
	edge->edge.component_ordinal = 0;
	if (before != NULL)
		edge->edge.before = *before;
	memcpy(edge->edge.result_incarnation, result->segment_incarnation, 16);
	edge->result_token = result->mutation_token;
	edge->record_identity.system_identifier = identity->system_identifier;
	memcpy(edge->record_identity.storage_uuid, identity->storage_uuid, 16);
	edge->record_identity.origin_thread = participant_index + 1;
	edge->record_identity.timeline_id = 1;
	edge->record_identity.read_rec_ptr = 100 + record_identity;
	edge->record_identity.end_rec_ptr = 101 + record_identity;
	edge->record_identity.record_crc = (uint32)record_identity;
	edge->record_identity.rmid = RM_XLOG_ID;
	edge->record_identity.info = (uint8)(record_identity & 0xf0);
	edge->participant_index = participant_index;
	edge->component_count = 1;
	memset(edge->anchor_digest, (int)(record_identity & 0xff), sizeof(edge->anchor_digest));
	edge->record_complete = true;
	edge->opcode_supported = true;
	edge->side_complete = true;
	edge->image_integrity_ok = true;
}

static void
graph_recount(GraphFixture *fixture, uint32 participant_count, uint32 edge_count)
{
	uint32 i;

	memset(fixture->cuts, 0, sizeof(fixture->cuts));
	for (i = 0; i < participant_count; i++) {
		fixture->cuts[i].failed_thread = (uint16)(i + 1);
		fixture->cuts[i].timeline_id = 1;
		fixture->cuts[i].flags = RF_CONTRIBUTOR_CUT_KNOWN_MASK;
		fixture->cuts[i].scan_begin_inclusive = 100;
		fixture->cuts[i].scan_end_exclusive = 100;
	}
	for (i = 0; i < edge_count; i++) {
		uint16 participant = fixture->edges[i].participant_index;

		UT_ASSERT(participant < participant_count);
		fixture->cuts[participant].contributor_count++;
		fixture->cuts[participant].component_count += fixture->edges[i].component_count;
		fixture->cuts[participant].flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		fixture->cuts[participant].scan_end_exclusive = 200;
	}
	fixture->vector.participant_count = participant_count;
	fixture->vector.edge_count = edge_count;
	fixture->request.participant_count = participant_count;
}

static void
graph_init(GraphFixture *fixture)
{
	RfPageVersionV1 result = make_version(1, 11);

	memset(fixture, 0, sizeof(*fixture));
	fixture->identity = make_identity(7);
	set_edge(&fixture->edges[0], &fixture->identity, RF_PAGE_STATE_ABSENT, NULL, &result,
			 RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE, 1, 0);
	fixture->vector.system_identifier = fixture->identity.system_identifier;
	memcpy(fixture->vector.storage_uuid, fixture->identity.storage_uuid, 16);
	fixture->vector.cuts = fixture->cuts;
	fixture->vector.edges = fixture->edges;
	fixture->source.page_identity = fixture->identity;
	fixture->source.binding_cookie = 41;
	fixture->source.current_binding_cookie = 41;
	fixture->source.identity_verified = true;
	fixture->source.integrity_verified = true;
	fixture->request.page_identity = fixture->identity;
	fixture->request.expected_result = result;
	fixture->request.contributors = &fixture->vector;
	fixture->request.source = &fixture->source;
	fixture->request.retention_binding_cookie = 51;
	fixture->request.current_retention_binding_cookie = 51;
	fixture->request.root_current = true;
	fixture->request.duty_current = true;
	fixture->request.fence_current = true;
	fixture->request.retention_current = true;
	graph_recount(fixture, 1, 1);
	memset(fixture->root_tokens, 0, sizeof(fixture->root_tokens));
	memset(fixture->duties, 0, sizeof(fixture->duties));
	memset(fixture->root_tokens[0].authority_uuid, 0x31,
		   sizeof(fixture->root_tokens[0].authority_uuid));
	fixture->root_tokens[0].origin_thread_id = 1;
	fixture->root_tokens[0].root_lineage_seq = 9;
	fixture->root_tokens[0].file_txn_seq = 10;
	fixture->root_tokens[0].root_publish_seq = 11;
	fixture->root_tokens[0].record_crc32c = 12;
	fixture->duties[0].origin_thread_id = 1;
	fixture->duties[0].root_lineage_seq = 9;
	memset(fixture->duties[0].authority_uuid, 0x31, sizeof(fixture->duties[0].authority_uuid));
}

static RfPageProofDetailV1
graph_select(GraphFixture *fixture)
{
	return rf_page_stable_base_select_v1(&fixture->request, fixture->chain,
										 lengthof(fixture->chain), &fixture->selection);
}

static void
proof_request(GraphFixture *fixture, RfPageStableBaseProofRequestV1 *request)
{
	memset(request, 0, sizeof(*request));
	request->graph = &fixture->request;
	request->duties = fixture->duties;
	request->root_tokens = fixture->root_tokens;
	request->formation = (const ClusterFormationWitnessV1 *)&fixture->formation_object;
	request->fence_need_set = (const PgracExternalFenceNeedSetV1 *)&fixture->needs_object;
	request->fence_admission_set
		= (const PgracExternalFenceAdmissionSetV1 *)&fixture->admission_object;
	request->retention_pin = (ClusterWalRetentionPin *)&fixture->pin_object;
	expected_root_tokens = fixture->root_tokens;
	expected_duties = fixture->duties;
	expected_formation = request->formation;
	expected_needs = request->fence_need_set;
	expected_admissions = request->fence_admission_set;
	expected_pin = request->retention_pin;
	canonical_root_current = true;
	bound_pin_current = true;
	preflight_pin_checks = 0;
	bound_pin_checks = 0;
}

UT_TEST(test_stable_proof_binds_exact_borrowed_owners)
{
	GraphFixture fixture;
	RfPageStableBaseProofRequestV1 request;
	RfPageStableBaseProofV1 *proof = NULL;

	graph_init(&fixture);
	proof_request(&fixture, &request);
	UT_ASSERT_EQ(rf_page_stable_base_proof_build_wait_v1(&request, fixture.chain,
														 lengthof(fixture.chain), 1000, &proof),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_page_stable_base_proof_matches_v1(
		proof, &fixture.identity, &fixture.request.expected_result, fixture.duties,
		fixture.root_tokens, request.formation, request.fence_need_set, request.fence_admission_set,
		request.retention_pin, &fixture.source, &fixture.vector, 1));
	rf_page_stable_base_proof_destroy_v1(&proof);
	UT_ASSERT(proof == NULL);
	UT_ASSERT_EQ(preflight_pin_checks, 1);
	UT_ASSERT_EQ(bound_pin_checks, 0);
}

UT_TEST(test_stable_proof_accepts_only_current_bound_pin)
{
	GraphFixture fixture;
	RfPageStableBaseProofRequestV1 request;
	RfPageStableBaseProofV1 *proof = NULL;

	graph_init(&fixture);
	proof_request(&fixture, &request);
	UT_ASSERT_EQ(rf_page_stable_base_proof_build_bound_v1(&request, fixture.chain,
														  lengthof(fixture.chain), &proof),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(proof != NULL);
	UT_ASSERT_EQ(preflight_pin_checks, 0);
	UT_ASSERT_EQ(bound_pin_checks, 1);
	rf_page_stable_base_proof_destroy_v1(&proof);
	bound_pin_current = false;
	UT_ASSERT_EQ(rf_page_stable_base_proof_build_bound_v1(&request, fixture.chain,
														  lengthof(fixture.chain), &proof),
				 RF_PAGE_PROOF_DETAIL_RETENTION_STALE);
	UT_ASSERT(proof == NULL);
	UT_ASSERT_EQ(preflight_pin_checks, 0);
	UT_ASSERT_EQ(bound_pin_checks, 2);
}

UT_TEST(test_stable_proof_rejects_duplicate_owner_scalars)
{
	GraphFixture fixture;
	RfPageStableBaseProofRequestV1 request;
	RfPageStableBaseProofV1 *proof = NULL;
	ClusterControlRootReadToken copied_root[4];
	RfContributorVectorV1 copied_vector;
	RfPagePinnedSourceV1 copied_source;

	graph_init(&fixture);
	proof_request(&fixture, &request);
	UT_ASSERT_EQ(rf_page_stable_base_proof_build_wait_v1(&request, fixture.chain,
														 lengthof(fixture.chain), 1000, &proof),
				 RF_PAGE_PROOF_DETAIL_OK);
	memcpy(copied_root, fixture.root_tokens, sizeof(copied_root));
	copied_vector = fixture.vector;
	copied_source = fixture.source;
	UT_ASSERT(!rf_page_stable_base_proof_matches_v1(
		proof, &fixture.identity, &fixture.request.expected_result, fixture.duties, copied_root,
		request.formation, request.fence_need_set, request.fence_admission_set,
		request.retention_pin, &fixture.source, &fixture.vector, 1));
	UT_ASSERT(!rf_page_stable_base_proof_matches_v1(
		proof, &fixture.identity, &fixture.request.expected_result, fixture.duties,
		fixture.root_tokens, request.formation, request.fence_need_set, request.fence_admission_set,
		request.retention_pin, &copied_source, &fixture.vector, 1));
	UT_ASSERT(!rf_page_stable_base_proof_matches_v1(
		proof, &fixture.identity, &fixture.request.expected_result, fixture.duties,
		fixture.root_tokens, request.formation, request.fence_need_set, request.fence_admission_set,
		request.retention_pin, &fixture.source, &copied_vector, 1));
	rf_page_stable_base_proof_destroy_v1(&proof);
}

UT_TEST(test_stable_proof_rejects_forged_root_current_boolean)
{
	GraphFixture fixture;
	RfPageStableBaseProofRequestV1 request;
	RfPageStableBaseProofV1 *proof = NULL;

	graph_init(&fixture);
	proof_request(&fixture, &request);
	UT_ASSERT(fixture.request.root_current);
	canonical_root_current = false;
	UT_ASSERT_EQ(rf_page_stable_base_proof_build_wait_v1(&request, fixture.chain,
														 lengthof(fixture.chain), 1000, &proof),
				 RF_PAGE_PROOF_DETAIL_ROOT_STALE);
	UT_ASSERT(proof == NULL);
}

UT_TEST(test_stable_proof_rejects_root_cut_order_mismatch)
{
	GraphFixture fixture;
	RfPageStableBaseProofRequestV1 request;
	RfPageStableBaseProofV1 *proof = NULL;

	graph_init(&fixture);
	proof_request(&fixture, &request);
	fixture.root_tokens[0].origin_thread_id = 2;
	UT_ASSERT_EQ(rf_page_stable_base_proof_build_wait_v1(&request, fixture.chain,
														 lengthof(fixture.chain), 1000, &proof),
				 RF_PAGE_PROOF_DETAIL_ROOT_STALE);
	UT_ASSERT(proof == NULL);
}

UT_TEST(test_one_stream_chain)
{
	GraphFixture fixture;

	graph_init(&fixture);
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(fixture.selection.chain_length, 1);
}

UT_TEST(test_two_stream_positive)
{
	GraphFixture fixture;
	RfPageVersionV1 v1 = make_version(1, 11);
	RfPageVersionV1 v2 = make_version(1, 12);

	graph_init(&fixture);
	set_edge(&fixture.edges[1], &fixture.identity, RF_PAGE_STATE_PRESENT, &v1, &v2, 0, 2, 1);
	fixture.request.expected_result = v2;
	graph_recount(&fixture, 2, 2);
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(fixture.selection.chain_length, 2);
}

UT_TEST(test_explicit_empty_participant)
{
	GraphFixture fixture;

	graph_init(&fixture);
	graph_recount(&fixture, 2, 1);
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(fixture.cuts[1].flags, RF_CONTRIBUTOR_CUT_KNOWN_MASK);
}

UT_TEST(test_missing_participant)
{
	GraphFixture fixture;

	graph_init(&fixture);
	fixture.request.participant_count = 2;
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_PARTICIPANT_MISSING);
}

UT_TEST(test_edge_gap)
{
	GraphFixture fixture;
	RfPageVersionV1 before = make_version(1, 12);
	RfPageVersionV1 result = make_version(1, 13);

	graph_init(&fixture);
	set_edge(&fixture.edges[0], &fixture.identity, RF_PAGE_STATE_PRESENT, &before, &result, 0, 1,
			 0);
	fixture.request.expected_result = result;
	graph_recount(&fixture, 1, 1);
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_EDGE_GAP);
}

UT_TEST(test_edge_branch)
{
	GraphFixture fixture;
	RfPageVersionV1 v1 = make_version(1, 11);
	RfPageVersionV1 v2 = make_version(1, 12);
	RfPageVersionV1 v3 = make_version(1, 13);

	graph_init(&fixture);
	set_edge(&fixture.edges[1], &fixture.identity, RF_PAGE_STATE_PRESENT, &v1, &v2, 0, 2, 0);
	set_edge(&fixture.edges[2], &fixture.identity, RF_PAGE_STATE_PRESENT, &v1, &v3, 0, 3, 0);
	fixture.request.expected_result = v3;
	graph_recount(&fixture, 1, 3);
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_EDGE_BRANCH);
}

UT_TEST(test_join_ambiguity)
{
	GraphFixture fixture;
	RfPageVersionV1 v1 = make_version(1, 11);
	RfPageVersionV1 v2 = make_version(1, 12);
	RfPageVersionV1 v3 = make_version(1, 13);

	graph_init(&fixture);
	set_edge(&fixture.edges[1], &fixture.identity, RF_PAGE_STATE_PRESENT, &v1, &v3, 0, 2, 0);
	set_edge(&fixture.edges[2], &fixture.identity, RF_PAGE_STATE_PRESENT, &v2, &v3, 0, 3, 0);
	fixture.request.expected_result = v3;
	graph_recount(&fixture, 1, 3);
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_TERMINAL_AMBIGUOUS);
}

UT_TEST(test_edge_cycle)
{
	GraphFixture fixture;
	RfPageVersionV1 v1 = make_version(1, 11);
	RfPageVersionV1 v2 = make_version(1, 12);

	graph_init(&fixture);
	set_edge(&fixture.edges[0], &fixture.identity, RF_PAGE_STATE_PRESENT, &v1, &v2, 0, 1, 0);
	set_edge(&fixture.edges[1], &fixture.identity, RF_PAGE_STATE_PRESENT, &v2, &v1, 0, 2, 0);
	fixture.request.expected_result = v2;
	graph_recount(&fixture, 1, 2);
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_EDGE_CYCLE);
}

UT_TEST(test_duplicate_exact_record)
{
	GraphFixture fixture;

	graph_init(&fixture);
	fixture.edges[1] = fixture.edges[0];
	graph_recount(&fixture, 1, 2);
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(fixture.selection.chain_length, 1);
}

UT_TEST(test_conflicting_duplicate_record)
{
	GraphFixture fixture;

	graph_init(&fixture);
	fixture.edges[1] = fixture.edges[0];
	fixture.edges[1].result_token++;
	graph_recount(&fixture, 1, 2);
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_ANCHOR_AMBIGUOUS);
}

UT_TEST(test_duplicate_requires_full_record_identity)
{
	GraphFixture fixture;

	graph_init(&fixture);
	fixture.edges[1] = fixture.edges[0];
	fixture.edges[1].record_identity.info++;
	graph_recount(&fixture, 1, 2);
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_ANCHOR_AMBIGUOUS);
}

UT_TEST(test_unique_terminal)
{
	GraphFixture fixture;
	RfPageVersionV1 v1 = make_version(1, 11);
	RfPageVersionV1 v2 = make_version(1, 12);

	graph_init(&fixture);
	set_edge(&fixture.edges[1], &fixture.identity, RF_PAGE_STATE_PRESENT, &v1, &v2, 0, 2, 0);
	fixture.request.expected_result = v2;
	graph_recount(&fixture, 1, 2);
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_page_version_equal_v1(&fixture.selection.terminal_version, &v2));
}

UT_TEST(test_multiple_terminals)
{
	GraphFixture fixture;
	RfPageVersionV1 before = make_version(2, 0);
	RfPageVersionV1 result = make_version(2, 12);

	graph_init(&fixture);
	set_edge(&fixture.edges[1], &fixture.identity, RF_PAGE_STATE_UNFORMATTED, &before, &result,
			 RF_PAGE_EDGE_WILL_INIT | RF_PAGE_EDGE_FULL_COVERAGE, 2, 0);
	graph_recount(&fixture, 1, 2);
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_TERMINAL_AMBIGUOUS);
}

UT_TEST(test_nearest_anchor)
{
	GraphFixture fixture;
	RfPageVersionV1 v1 = make_version(1, 11);
	RfPageVersionV1 v2 = make_version(1, 12);
	RfPageVersionV1 v3 = make_version(1, 13);

	graph_init(&fixture);
	set_edge(&fixture.edges[1], &fixture.identity, RF_PAGE_STATE_PRESENT, &v1, &v2,
			 RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE, 2, 0);
	set_edge(&fixture.edges[2], &fixture.identity, RF_PAGE_STATE_PRESENT, &v2, &v3, 0, 3, 0);
	fixture.request.expected_result = v3;
	graph_recount(&fixture, 1, 3);
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(fixture.selection.anchor_edge_index, 1);
	UT_ASSERT_EQ(fixture.selection.chain_length, 2);
}

UT_TEST(test_off_chain_anchor)
{
	GraphFixture fixture;
	RfPageVersionV1 before = make_version(2, 0);
	RfPageVersionV1 result = make_version(2, 12);

	graph_init(&fixture);
	set_edge(&fixture.edges[1], &fixture.identity, RF_PAGE_STATE_UNFORMATTED, &before, &result,
			 RF_PAGE_EDGE_WILL_INIT | RF_PAGE_EDGE_FULL_COVERAGE, 2, 0);
	graph_recount(&fixture, 1, 2);
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_TERMINAL_AMBIGUOUS);
}

UT_TEST(test_ambiguous_image)
{
	GraphFixture fixture;

	graph_init(&fixture);
	fixture.edges[1] = fixture.edges[0];
	fixture.edges[1].record_identity.record_crc = 2;
	memset(fixture.edges[1].anchor_digest, 0x77, sizeof(fixture.edges[1].anchor_digest));
	graph_recount(&fixture, 1, 2);
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_ANCHOR_AMBIGUOUS);
}

UT_TEST(test_full_init_anchor)
{
	GraphFixture fixture;
	RfPageVersionV1 before = make_version(1, 0);
	RfPageVersionV1 result = make_version(1, 11);

	graph_init(&fixture);
	set_edge(&fixture.edges[0], &fixture.identity, RF_PAGE_STATE_UNFORMATTED, &before, &result,
			 RF_PAGE_EDGE_WILL_INIT | RF_PAGE_EDGE_FULL_COVERAGE, 1, 0);
	graph_recount(&fixture, 1, 1);
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_OK);
}

UT_TEST(test_partial_init_reject)
{
	GraphFixture fixture;
	RfPageVersionV1 before = make_version(1, 0);
	RfPageVersionV1 result = make_version(1, 11);

	graph_init(&fixture);
	set_edge(&fixture.edges[0], &fixture.identity, RF_PAGE_STATE_UNFORMATTED, &before, &result,
			 RF_PAGE_EDGE_WILL_INIT, 1, 0);
	graph_recount(&fixture, 1, 1);
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_IMAGE_DECODE_FAILED);
}

UT_TEST(test_consistency_image_reject)
{
	GraphFixture fixture;
	RfPageVersionV1 result = make_version(1, 11);

	graph_init(&fixture);
	set_edge(&fixture.edges[0], &fixture.identity, RF_PAGE_STATE_ABSENT, NULL, &result, 0, 1, 0);
	graph_recount(&fixture, 1, 1);
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_ANCHOR_MISSING);
}

UT_TEST(test_source_drift)
{
	GraphFixture fixture;

	graph_init(&fixture);
	fixture.source.current_binding_cookie++;
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
}

UT_TEST(test_pin_drift)
{
	GraphFixture fixture;

	graph_init(&fixture);
	fixture.request.current_retention_binding_cookie++;
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_RETENTION_STALE);
}

UT_TEST(test_root_drift)
{
	GraphFixture fixture;

	graph_init(&fixture);
	fixture.request.root_current = false;
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_ROOT_STALE);
}

UT_TEST(test_duty_stale)
{
	GraphFixture fixture;

	graph_init(&fixture);
	fixture.request.duty_current = false;
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_DUTY_STALE);
}

UT_TEST(test_incarnation_mismatch)
{
	GraphFixture fixture;
	RfPageVersionV1 before = make_version(1, 11);
	RfPageVersionV1 result = make_version(2, 12);

	graph_init(&fixture);
	set_edge(&fixture.edges[0], &fixture.identity, RF_PAGE_STATE_PRESENT, &before, &result, 0, 1,
			 0);
	fixture.request.expected_result = result;
	graph_recount(&fixture, 1, 1);
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_INCARNATION_MISMATCH);
}

UT_TEST(test_foreign_identity)
{
	GraphFixture fixture;

	graph_init(&fixture);
	fixture.edges[0].page_identity.blockno++;
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
}

UT_TEST(test_unsupported_opcode)
{
	GraphFixture fixture;

	graph_init(&fixture);
	fixture.edges[0].opcode_supported = false;
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED);
}

UT_TEST(test_side_incomplete)
{
	GraphFixture fixture;

	graph_init(&fixture);
	fixture.edges[0].side_complete = false;
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
}

typedef struct FakeInstallState {
	char *targets;
	uint32 component_count;
	char log[256];
	uint32 log_length;
	uint32 write_calls;
	uint32 sync_calls;
	uint32 postread_calls;
	char fail_kind;
	uint32 fail_call;
	bool corrupt_postread;
	bool promoted;
	bool canonicalize_after_promote;
} FakeInstallState;

typedef struct InstallFixture {
	RfPageInstallComponentV1 components[RF_PAGE_STABLE_MAX_COMPONENTS];
	RfPageInstallOpsV1 ops;
	RfPageInstallRequestV1 request;
	RfPageInstallProofV1 proof;
	FakeInstallState state;
	char *canonical;
	char *prepared;
} InstallFixture;

static void
fake_log(FakeInstallState *state, char operation)
{
	UT_ASSERT(state->log_length + 1 < sizeof(state->log));
	state->log[state->log_length++] = operation;
	state->log[state->log_length] = '\0';
}

static bool
fake_canonicalize(void *arg, uint32 index, bool checksums_enabled, char page[BLCKSZ])
{
	FakeInstallState *state = arg;

	(void)index;
	fake_log(state, 'C');
	if (state->promoted)
		state->canonicalize_after_promote = true;
	if (checksums_enabled)
		page[8] = (char)0x5a;
	return true;
}

static bool
fake_promote(void *arg)
{
	FakeInstallState *state = arg;

	fake_log(state, 'P');
	state->promoted = true;
	return true;
}

static bool
fake_write(void *arg, uint32 index, const char page[BLCKSZ])
{
	FakeInstallState *state = arg;

	fake_log(state, 'W');
	state->write_calls++;
	if (state->fail_kind == 'W' && state->write_calls == state->fail_call)
		return false;
	memcpy(state->targets + (Size)index * BLCKSZ, page, BLCKSZ);
	return true;
}

static bool
fake_sync(void *arg, uint32 index)
{
	FakeInstallState *state = arg;

	(void)index;
	fake_log(state, 'S');
	state->sync_calls++;
	return !(state->fail_kind == 'S' && state->sync_calls == state->fail_call);
}

static bool
fake_postread(void *arg, uint32 index, char page[BLCKSZ])
{
	FakeInstallState *state = arg;

	fake_log(state, 'R');
	state->postread_calls++;
	if (state->fail_kind == 'R' && state->postread_calls == state->fail_call)
		return false;
	memcpy(page, state->targets + (Size)index * BLCKSZ, BLCKSZ);
	if (state->corrupt_postread)
		page[BLCKSZ - 1] ^= 0x01;
	return true;
}

static bool
fake_publish(void *arg)
{
	FakeInstallState *state = arg;

	fake_log(state, 'V');
	return true;
}

static bool
fake_release(void *arg)
{
	FakeInstallState *state = arg;

	fake_log(state, 'L');
	state->promoted = false;
	return true;
}

static void
install_init(InstallFixture *fixture, uint32 component_count)
{
	uint32 i;
	Size bytes = (Size)component_count * BLCKSZ;

	memset(fixture, 0, sizeof(*fixture));
	fixture->canonical = calloc(1, bytes);
	fixture->prepared = calloc(1, bytes);
	fixture->state.targets = calloc(1, bytes);
	UT_ASSERT(fixture->canonical != NULL);
	UT_ASSERT(fixture->prepared != NULL);
	UT_ASSERT(fixture->state.targets != NULL);
	fixture->state.component_count = component_count;
	for (i = 0; i < component_count; i++) {
		char *canonical = fixture->canonical + (Size)i * BLCKSZ;

		memset(canonical, (int)(0x10 + i), BLCKSZ);
		memset(fixture->state.targets + (Size)i * BLCKSZ, 0x44, BLCKSZ);
		fixture->components[i].page_identity = make_identity(i + 1);
		fixture->components[i].expected_before = make_version(1, 100 + i);
		fixture->components[i].expected_result = make_version(1, 200 + i);
		fixture->components[i].canonical_page = canonical;
		fixture->components[i].target_state = RF_PAGE_INSTALL_TARGET_EXPECTED;
		fixture->components[i].route_preflight_ok = true;
		fixture->components[i].side_preflight_ok = true;
		fixture->components[i].scratch_ready = true;
		fixture->components[i].identity_authority_ok = true;
		fixture->components[i].canonical_layout_ok = true;
	}
	fixture->ops.arg = &fixture->state;
	fixture->ops.canonicalize = fake_canonicalize;
	fixture->ops.promote = fake_promote;
	fixture->ops.write = fake_write;
	fixture->ops.sync = fake_sync;
	fixture->ops.postread = fake_postread;
	fixture->ops.publish = fake_publish;
	fixture->ops.release = fake_release;
	fixture->request.components = fixture->components;
	fixture->request.component_count = component_count;
	fixture->request.prepared_pages = fixture->prepared;
	fixture->request.prepared_capacity = bytes;
	fixture->request.ops = &fixture->ops;
	fixture->request.global_preflight_ok = true;
}

static void
install_reset_attempt(InstallFixture *fixture)
{
	fixture->state.log_length = 0;
	fixture->state.log[0] = '\0';
	fixture->state.write_calls = 0;
	fixture->state.sync_calls = 0;
	fixture->state.postread_calls = 0;
	fixture->state.fail_kind = '\0';
	fixture->state.fail_call = 0;
	fixture->state.corrupt_postread = false;
	fixture->state.promoted = false;
	memset(&fixture->proof, 0, sizeof(fixture->proof));
}

static void
install_destroy(InstallFixture *fixture)
{
	free(fixture->canonical);
	free(fixture->prepared);
	free(fixture->state.targets);
}

static RfPageProofDetailV1
install_run(InstallFixture *fixture)
{
	return rf_page_stable_install_test_v1(&fixture->request, &fixture->proof);
}

UT_TEST(test_no_mutation_before_preflight)
{
	InstallFixture fixture;

	install_init(&fixture, 1);
	fixture.components[0].side_preflight_ok = false;
	UT_ASSERT_EQ(install_run(&fixture), RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE);
	UT_ASSERT_EQ(fixture.state.log_length, 0);
	install_destroy(&fixture);
}

UT_TEST(test_thirty_three_scratch_pages)
{
	InstallFixture fixture;

	install_init(&fixture, RF_PAGE_STABLE_MAX_COMPONENTS);
	UT_ASSERT_EQ(install_run(&fixture), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(fixture.proof.component_count, RF_PAGE_STABLE_MAX_COMPONENTS);
	install_destroy(&fixture);
}

UT_TEST(test_edge_capacity)
{
	GraphFixture fixture;

	graph_init(&fixture);
	fixture.vector.edge_count = RF_PAGE_STABLE_MAX_EDGES + 1;
	UT_ASSERT_EQ(graph_select(&fixture), RF_PAGE_PROOF_DETAIL_CAPACITY);
}

UT_TEST(test_result_skip)
{
	InstallFixture fixture;

	install_init(&fixture, 1);
	fixture.components[0].target_state = RF_PAGE_INSTALL_TARGET_RESULT;
	memcpy(fixture.state.targets, fixture.canonical, BLCKSZ);
	UT_ASSERT_EQ(install_run(&fixture), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(fixture.state.write_calls, 0);
	UT_ASSERT_EQ(fixture.state.postread_calls, 1);
	install_destroy(&fixture);
}

UT_TEST(test_before_apply)
{
	InstallFixture fixture;

	install_init(&fixture, 1);
	UT_ASSERT_EQ(install_run(&fixture), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(fixture.state.write_calls, 1);
	UT_ASSERT(memcmp(fixture.state.targets, fixture.canonical, BLCKSZ) == 0);
	install_destroy(&fixture);
}

UT_TEST(test_torn_overwrite)
{
	InstallFixture fixture;

	install_init(&fixture, 1);
	fixture.components[0].target_state = RF_PAGE_INSTALL_TARGET_TORN;
	UT_ASSERT_EQ(install_run(&fixture), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(fixture.state.write_calls, 1);
	install_destroy(&fixture);
}

UT_TEST(test_unrelated_version_reject)
{
	InstallFixture fixture;

	install_init(&fixture, 1);
	fixture.components[0].target_state = RF_PAGE_INSTALL_TARGET_UNRELATED;
	UT_ASSERT_EQ(install_run(&fixture), RF_PAGE_PROOF_DETAIL_VERSION_MISMATCH);
	UT_ASSERT_EQ(fixture.state.log_length, 0);
	install_destroy(&fixture);
}

UT_TEST(test_crash_after_first_sibling)
{
	InstallFixture fixture;

	install_init(&fixture, 2);
	fixture.state.fail_kind = 'W';
	fixture.state.fail_call = 2;
	UT_ASSERT_EQ(install_run(&fixture), RF_PAGE_PROOF_DETAIL_INTERNAL);
	UT_ASSERT(memcmp(fixture.state.targets, fixture.canonical, BLCKSZ) == 0);
	fixture.components[0].target_state = RF_PAGE_INSTALL_TARGET_RESULT;
	install_reset_attempt(&fixture);
	UT_ASSERT_EQ(install_run(&fixture), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(fixture.state.write_calls, 1);
	install_destroy(&fixture);
}

UT_TEST(test_crash_after_last_write_before_sync)
{
	InstallFixture fixture;

	install_init(&fixture, 2);
	fixture.state.fail_kind = 'S';
	fixture.state.fail_call = 1;
	UT_ASSERT_EQ(install_run(&fixture), RF_PAGE_PROOF_DETAIL_POSTREAD_FAILED);
	fixture.components[0].target_state = RF_PAGE_INSTALL_TARGET_RESULT;
	fixture.components[1].target_state = RF_PAGE_INSTALL_TARGET_RESULT;
	install_reset_attempt(&fixture);
	UT_ASSERT_EQ(install_run(&fixture), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(fixture.state.write_calls, 0);
	install_destroy(&fixture);
}

UT_TEST(test_crash_after_sync_before_postread)
{
	InstallFixture fixture;

	install_init(&fixture, 1);
	fixture.state.fail_kind = 'R';
	fixture.state.fail_call = 1;
	UT_ASSERT_EQ(install_run(&fixture), RF_PAGE_PROOF_DETAIL_POSTREAD_FAILED);
	fixture.components[0].target_state = RF_PAGE_INSTALL_TARGET_RESULT;
	install_reset_attempt(&fixture);
	UT_ASSERT_EQ(install_run(&fixture), RF_PAGE_PROOF_DETAIL_OK);
	install_destroy(&fixture);
}

UT_TEST(test_checksum_on_recompute)
{
	InstallFixture fixture;

	install_init(&fixture, 1);
	fixture.components[0].checksums_enabled = true;
	UT_ASSERT_EQ(install_run(&fixture), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ((uint8)fixture.state.targets[8], 0x5a);
	install_destroy(&fixture);
}

UT_TEST(test_checksum_off_full_page_compare)
{
	InstallFixture fixture;

	install_init(&fixture, 1);
	fixture.state.corrupt_postread = true;
	UT_ASSERT_EQ(install_run(&fixture), RF_PAGE_PROOF_DETAIL_POSTREAD_FAILED);
	install_destroy(&fixture);
}

UT_TEST(test_zero_page_reject)
{
	InstallFixture fixture;

	install_init(&fixture, 1);
	memset(fixture.canonical, 0, BLCKSZ);
	UT_ASSERT_EQ(install_run(&fixture), RF_PAGE_PROOF_DETAIL_IMAGE_INTEGRITY_FAILED);
	UT_ASSERT_EQ(fixture.state.log_length, 0);
	install_destroy(&fixture);
}

UT_TEST(test_ignore_checksum_bypass_forbidden)
{
	InstallFixture fixture;

	install_init(&fixture, 1);
	fixture.components[0].checksums_enabled = true;
	fixture.state.corrupt_postread = true;
	UT_ASSERT_EQ(install_run(&fixture), RF_PAGE_PROOF_DETAIL_POSTREAD_FAILED);
	UT_ASSERT(!fixture.proof.proof_published);
	install_destroy(&fixture);
}

UT_TEST(test_lock_order_noalloc)
{
	InstallFixture fixture;

	install_init(&fixture, 2);
	UT_ASSERT_EQ(install_run(&fixture), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(!fixture.state.canonicalize_after_promote);
	UT_ASSERT(strncmp(fixture.state.log, "CCP", 3) == 0);
	install_destroy(&fixture);
}

UT_TEST(test_complete_release_order)
{
	InstallFixture fixture;

	install_init(&fixture, 2);
	UT_ASSERT_EQ(install_run(&fixture), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(strcmp(fixture.state.log, "CCPWWSSRRVL") == 0);
	UT_ASSERT(fixture.proof.authority_released);
	install_destroy(&fixture);
}

int
main(void)
{
	UT_PLAN(48);
	UT_RUN(test_stable_proof_binds_exact_borrowed_owners);
	UT_RUN(test_stable_proof_accepts_only_current_bound_pin);
	UT_RUN(test_stable_proof_rejects_duplicate_owner_scalars);
	UT_RUN(test_stable_proof_rejects_root_cut_order_mismatch);
	UT_RUN(test_stable_proof_rejects_forged_root_current_boolean);
	UT_RUN(test_one_stream_chain);
	UT_RUN(test_two_stream_positive);
	UT_RUN(test_explicit_empty_participant);
	UT_RUN(test_missing_participant);
	UT_RUN(test_edge_gap);
	UT_RUN(test_edge_branch);
	UT_RUN(test_join_ambiguity);
	UT_RUN(test_edge_cycle);
	UT_RUN(test_duplicate_exact_record);
	UT_RUN(test_conflicting_duplicate_record);
	UT_RUN(test_duplicate_requires_full_record_identity);
	UT_RUN(test_unique_terminal);
	UT_RUN(test_multiple_terminals);
	UT_RUN(test_nearest_anchor);
	UT_RUN(test_off_chain_anchor);
	UT_RUN(test_ambiguous_image);
	UT_RUN(test_full_init_anchor);
	UT_RUN(test_partial_init_reject);
	UT_RUN(test_consistency_image_reject);
	UT_RUN(test_source_drift);
	UT_RUN(test_pin_drift);
	UT_RUN(test_root_drift);
	UT_RUN(test_duty_stale);
	UT_RUN(test_incarnation_mismatch);
	UT_RUN(test_foreign_identity);
	UT_RUN(test_unsupported_opcode);
	UT_RUN(test_side_incomplete);
	UT_RUN(test_no_mutation_before_preflight);
	UT_RUN(test_thirty_three_scratch_pages);
	UT_RUN(test_edge_capacity);
	UT_RUN(test_result_skip);
	UT_RUN(test_before_apply);
	UT_RUN(test_torn_overwrite);
	UT_RUN(test_unrelated_version_reject);
	UT_RUN(test_crash_after_first_sibling);
	UT_RUN(test_crash_after_last_write_before_sync);
	UT_RUN(test_crash_after_sync_before_postread);
	UT_RUN(test_checksum_on_recompute);
	UT_RUN(test_checksum_off_full_page_compare);
	UT_RUN(test_zero_page_reject);
	UT_RUN(test_ignore_checksum_bypass_forbidden);
	UT_RUN(test_lock_order_noalloc);
	UT_RUN(test_complete_release_order);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}

#endif
