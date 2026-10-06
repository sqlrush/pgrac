/*-------------------------------------------------------------------------
 *
 * test_cluster_page_stable_base.c
 *    STOP-06 stable-base graph/selector semantic capability RED.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/xlogreader.h"
#include "cluster/cluster_page_authority.h"
#include "cluster/cluster_thread_recovery_authority.h"

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
bool cluster_shared_config = true;

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
static uint32 source_count;
static const ClusterThreadRecoveryAuthorityV1 *source_owners;
static uint16 stale_source;
static uint32 source_serial_checks[4];

ClusterRecoveryDutyCompare
cluster_recovery_duty_key_compare_for_claim(const ClusterRecoveryDutyKey *expected,
											const ClusterRecoveryDutyKey *observed, bool claim_v2)
{
	return memcmp(expected, observed, sizeof(*expected)) == 0
			   ? CLUSTER_RECOVERY_DUTY_COMPARE_EXACT
			   : CLUSTER_RECOVERY_DUTY_COMPARE_DIFFERENT;
}

ClusterRecoverySerialRevalidateResult
cluster_recovery_serial_revalidate(ClusterRecoverySerialGuard *guard)
{
	for (uint32 i = 0; i < source_count; i++)
		if (guard == source_owners[i].serial_guard) {
			source_serial_checks[i]++;
			return guard->held && guard->mode == CLUSTER_RECOVERY_SERIAL_COLD_FORMED
						   && guard->duty.origin_thread_id != stale_source
					   ? CLUSTER_RECOVERY_SERIAL_CURRENT
					   : CLUSTER_RECOVERY_SERIAL_NOT_HELD;
		}
	return CLUSTER_RECOVERY_SERIAL_NOT_HELD;
}

ClusterControlRootResult
cluster_control_root_revalidate(const ClusterControlRootReadToken *token,
								const ClusterControlRootIdentity *identity,
								ClusterControlRootSnapshot *snapshot)
{
	for (uint32 i = 0; i < source_count; i++)
		if (token == source_owners[i].root_token && identity == source_owners[i].duty) {
			if (!canonical_root_current || identity->origin_thread_id == stale_source)
				return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
			*snapshot = *source_owners[i].root_snapshot;
			return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
		}
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
	for (uint32 i = 0; i < source_count; i++)
		if (formation == source_owners[i].formation)
			return CLUSTER_FORMATION_WITNESS_READY;
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
	for (uint32 i = 0; i < source_count; i++)
		if (needs == source_owners[i].serial_guard->fence_need_set
			&& formation == source_owners[i].serial_guard->formation)
			return source_owners[i].duty->origin_thread_id != stale_source;
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
	for (uint32 i = 0; i < source_count; i++)
		if (admissions == source_owners[i].serial_guard->fence_admission_set
			&& needs == source_owners[i].serial_guard->fence_need_set
			&& formation == source_owners[i].serial_guard->formation)
			return source_owners[i].duty->origin_thread_id != stale_source;
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
		fixture->cuts[i].origin_owner_incarnation = 19;
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
	source_count = 0;
	source_owners = NULL;
	stale_source = 0;
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
	fixture->duties[0].origin_owner_incarnation = 19;
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

typedef struct SourceFixture {
	GraphFixture graph;
	ClusterThreadRecoveryAuthorityV1 authorities[3];
	ClusterControlRootSnapshot roots[3];
	ClusterRecoverySerialGuard serials[3];
	char formations[3], needs[3], admissions[3];
} SourceFixture;

static void
sources_init(SourceFixture *fixture)
{
	GraphFixture *g = &fixture->graph;
	RfPageStableBaseProofRequestV1 legacy;
	RfPageVersionV1 first = make_version(1, 900);
	RfPageVersionV1 middle = make_version(1, 60);
	RfPageVersionV1 last = make_version(1, 7);

	memset(fixture, 0, sizeof(*fixture));
	graph_init(g);
	proof_request(g, &legacy);
	set_edge(&g->edges[0], &g->identity, RF_PAGE_STATE_ABSENT, NULL, &first,
			 RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE, 1, 0);
	set_edge(&g->edges[1], &g->identity, RF_PAGE_STATE_PRESENT, &first, &middle, 0, 1, 1);
	set_edge(&g->edges[2], &g->identity, RF_PAGE_STATE_PRESENT, &middle, &last, 0, 1, 2);
	g->request.expected_result = last;
	graph_recount(g, 3, 3);
	for (uint32 i = 0; i < 3; i++) {
		ClusterThreadRecoveryAuthorityV1 *a = &fixture->authorities[i];
		ClusterControlRootSnapshot *r = &fixture->roots[i];
		ClusterRecoverySerialGuard *s = &fixture->serials[i];
		ClusterRecoveryDutyKey *d = &g->duties[i];
		ClusterControlRootReadToken *t = &g->root_tokens[i];

		*d = g->duties[0];
		d->system_identifier = g->identity.system_identifier;
		memcpy(d->storage_uuid, g->identity.storage_uuid, 16);
		d->origin_thread_id = i + 1;
		r->identity = *d;
		r->lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
		r->root_flags
			= CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID;
		r->checkpoint_tli = r->tail_tli = 1;
		r->checkpoint_lower_lsn = 100;
		r->validated_tail_lsn_exclusive = 200;
		*t = g->root_tokens[0];
		t->origin_thread_id = i + 1;
		t->lifecycle = r->lifecycle;
		t->root_flags = r->root_flags;
		a->duty = d;
		a->root_snapshot = r;
		a->root_token = t;
		a->formation = (const ClusterFormationWitnessV1 *)&fixture->formations[i];
		a->fence_need_set = (const PgracExternalFenceNeedSetV1 *)&fixture->needs[i];
		a->fence_admission_set = (const PgracExternalFenceAdmissionSetV1 *)&fixture->admissions[i];
		a->retention_pin = expected_pin;
		a->serial_guard = s;
		s->held = true;
		s->mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
		s->duty = *d;
		s->root_read_token = *t;
		s->formation = a->formation;
		s->fence_need_set = a->fence_need_set;
		s->fence_admission_set = a->fence_admission_set;
	}
	source_owners = fixture->authorities;
	source_count = 3;
	memset(source_serial_checks, 0, sizeof(source_serial_checks));
}

static RfPageProofDetailV1
sources_build(SourceFixture *fixture, RfPageStableBaseProofV1 **proof)
{
	return rf_page_stable_base_proof_build_sources_v1(&fixture->graph.request, fixture->authorities,
													  3, fixture->graph.chain,
													  lengthof(fixture->graph.chain), proof);
}

static bool
sources_match(SourceFixture *fixture, RfPageStableBaseProofV1 *proof)
{
	return rf_page_stable_base_proof_matches_sources_v1(
		proof, &fixture->graph.identity, &fixture->graph.request.expected_result,
		fixture->authorities, 3, &fixture->graph.source, &fixture->graph.vector);
}

UT_TEST(test_sources_bind_each_original_owner)
{
	SourceFixture f;
	RfPageStableBaseProofV1 *proof = NULL;

	sources_init(&f);
	UT_ASSERT_EQ(sources_build(&f, &proof), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(sources_match(&f, proof));
	for (uint32 i = 0; i < 3; i++)
		UT_ASSERT(source_serial_checks[i] > 0);
	UT_ASSERT_EQ(f.graph.chain[0], 0);
	UT_ASSERT_EQ(f.graph.chain[1], 1);
	UT_ASSERT_EQ(f.graph.chain[2], 2);
	UT_ASSERT(rf_page_stable_base_proof_covers_version_v1(proof, &f.graph.identity,
														  &f.graph.edges[2].edge.before));
	rf_page_stable_base_proof_destroy_v1(&proof);
	UT_ASSERT_NULL(proof);
}

UT_TEST(test_sources_do_not_borrow_first_fence_for_later_origin)
{
	SourceFixture f;
	RfPageStableBaseProofV1 *proof = NULL;

	sources_init(&f);
	f.authorities[2].formation = f.authorities[0].formation;
	f.authorities[2].fence_need_set = f.authorities[0].fence_need_set;
	f.authorities[2].fence_admission_set = f.authorities[0].fence_admission_set;
	UT_ASSERT(sources_build(&f, &proof) != RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_NULL(proof);
}

UT_TEST(test_sources_reject_stale_later_member_and_pin)
{
	SourceFixture f;
	RfPageStableBaseProofV1 *proof = NULL;

	sources_init(&f);
	stale_source = 3;
	UT_ASSERT(sources_build(&f, &proof) != RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_NULL(proof);
	stale_source = 0;
	bound_pin_current = false;
	UT_ASSERT_EQ(sources_build(&f, &proof), RF_PAGE_PROOF_DETAIL_RETENTION_STALE);
	UT_ASSERT_NULL(proof);
}

UT_TEST(test_sources_require_exact_cuts_and_namespace)
{
	SourceFixture f;
	RfPageStableBaseProofV1 *proof = NULL;

	for (int variant = 0; variant < 8; variant++) {
		sources_init(&f);
		switch (variant) {
		case 0:
			f.graph.cuts[1].scan_begin_inclusive = 99;
			break;
		case 1:
			f.graph.cuts[1].scan_end_exclusive = 201;
			break;
		case 2:
			f.graph.cuts[1].timeline_id++;
			break;
		case 3:
			f.graph.cuts[1].failed_thread = 1;
			break;
		case 4:
			f.graph.identity.storage_uuid[0]++;
			f.graph.request.page_identity = f.graph.identity;
			break;
		case 5:
			f.authorities[1].retention_pin = (ClusterWalRetentionPin *)&f;
			break;
		case 6:
			f.roots[1].tail_tli++;
			break;
		case 7:
			f.graph.cuts[1].flags = RF_CONTRIBUTOR_CUT_KNOWN_MASK;
			break;
		}
		UT_ASSERT(sources_build(&f, &proof) != RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_NULL(proof);
	}
}

UT_TEST(test_sources_proof_rejects_rebound_original_objects)
{
	SourceFixture f;
	RfPageStableBaseProofV1 *proof = NULL;
	ClusterControlRootSnapshot copied_root;
	ClusterThreadRecoveryAuthorityV1 copied[3];

	sources_init(&f);
	UT_ASSERT_EQ(sources_build(&f, &proof), RF_PAGE_PROOF_DETAIL_OK);
	memcpy(copied, f.authorities, sizeof(copied));
	UT_ASSERT(!rf_page_stable_base_proof_matches_sources_v1(
		proof, &f.graph.identity, &f.graph.request.expected_result, copied, 3, &f.graph.source,
		&f.graph.vector));
	copied_root = f.roots[2];
	f.authorities[2].root_snapshot = &copied_root;
	UT_ASSERT(!sources_match(&f, proof));
	f.authorities[2].root_snapshot = &f.roots[2];
	UT_ASSERT(sources_match(&f, proof));
	stale_source = 2;
	UT_ASSERT(!sources_match(&f, proof));
	rf_page_stable_base_proof_destroy_v1(&proof);
}

UT_TEST(test_sources_reject_incomplete_input_and_readonly_ir)
{
	SourceFixture f;
	RfPageStableBaseProofV1 *proof = NULL;

	sources_init(&f);
	f.graph.edges[2].edge.before.mutation_token++;
	UT_ASSERT(sources_build(&f, &proof) != RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_NULL(proof);
	sources_init(&f);
	f.serials[2].mode = CLUSTER_RECOVERY_SERIAL_INPUT_SEAL;
	UT_ASSERT(sources_build(&f, &proof) != RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_NULL(proof);
}

UT_TEST(test_sources_keep_authority_for_projected_empty_origin)
{
	SourceFixture f;
	RfPageStableBaseProofV1 *proof = NULL;

	sources_init(&f);
	f.graph.request.expected_result = make_version(1, 60);
	graph_recount(&f.graph, 3, 2);
	/* Target edges need only occupy a subinterval of the original cut. */
	f.graph.cuts[0].scan_begin_inclusive = f.graph.cuts[1].scan_begin_inclusive = 101;
	f.graph.cuts[0].scan_end_exclusive = f.graph.cuts[1].scan_end_exclusive = 102;
	UT_ASSERT_EQ(sources_build(&f, &proof), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(sources_match(&f, proof));
	stale_source = 3;
	UT_ASSERT(!sources_match(&f, proof));
	rf_page_stable_base_proof_destroy_v1(&proof);
}

UT_TEST(test_sources_page_adapter_rechecks_all_members)
{
	SourceFixture f;
	RfPageStableBaseProofV1 *proof = NULL;
	RfPageAuthorityBatchRequestV1 request = { 0 };
	RfPageAuthorityTargetV1 target = { 0 };
	RfPageAuthorityPreflightV1 *preflight = NULL;
	RfPageInstallAuthorityAdapterV1 adapter = { 0 };

	sources_init(&f);
	UT_ASSERT_EQ(sources_build(&f, &proof), RF_PAGE_PROOF_DETAIL_OK);
	target.page_identity = f.graph.identity;
	target.expected_result = f.graph.request.expected_result;
	target.stable_base = proof;
	target.source = &f.graph.source;
	target.contributors = &f.graph.vector;
	request.targets = &target;
	request.target_count = 1;
	request.participant_count = 3;
	request.source_authorities = f.authorities;
	request.formation = f.authorities[0].formation;
	UT_ASSERT_EQ(rf_page_authority_batch_preflight_wait_v1(&request, 0, &preflight),
				 RF_PAGE_AUTHORITY_INVALID_ARGUMENT);
	UT_ASSERT_NULL(preflight);
	request.formation = NULL;
	UT_ASSERT_EQ(rf_page_authority_batch_preflight_wait_v1(&request, 0, &preflight),
				 RF_PAGE_AUTHORITY_OK);
	UT_ASSERT(rf_page_install_authority_adapter_init_v1(preflight, &f.serials[0], &adapter));
	if (ut_current_failed)
		goto cleanup;
	adapter.serial_guard = &f.serials[1];
	UT_ASSERT(!adapter.ops.promote(adapter.ops.arg));
	UT_ASSERT_NULL(adapter.guard);
	adapter.serial_guard = &f.serials[0];
	stale_source = 2;
	UT_ASSERT(!adapter.ops.promote(adapter.ops.arg));
	UT_ASSERT_NULL(adapter.guard);
	stale_source = 0;
	UT_ASSERT(adapter.ops.promote(adapter.ops.arg));
	UT_ASSERT(adapter.ops.validate_identity(adapter.ops.arg, &target.page_identity,
											target.expected_result.segment_incarnation));
	stale_source = 3;
	UT_ASSERT(!adapter.ops.validate_identity(adapter.ops.arg, &target.page_identity,
											 target.expected_result.segment_incarnation));
	UT_ASSERT(!adapter.ops.publish(adapter.ops.arg));
	UT_ASSERT(adapter.ops.release(adapter.ops.arg));
	rf_page_authority_preflight_destroy_v1(&preflight);
	UT_ASSERT_NULL(preflight);
	rf_page_stable_base_proof_destroy_v1(&proof);

	/* A failed publication must not strand the original PAGE guard. */
	sources_init(&f);
	UT_ASSERT_EQ(sources_build(&f, &proof), RF_PAGE_PROOF_DETAIL_OK);
	target.stable_base = proof;
	UT_ASSERT_EQ(rf_page_authority_batch_preflight_wait_v1(&request, 0, &preflight),
				 RF_PAGE_AUTHORITY_OK);
	UT_ASSERT(rf_page_install_authority_adapter_init_v1(preflight, &f.serials[0], &adapter));
	if (ut_current_failed)
		goto cleanup;
	UT_ASSERT(adapter.ops.promote(adapter.ops.arg));
	UT_ASSERT(adapter.ops.publish(adapter.ops.arg));
	UT_ASSERT(adapter.ops.release(adapter.ops.arg));
cleanup:
	if (adapter.guard != NULL)
		rf_page_authority_guard_release_v1(&adapter.guard);
	rf_page_authority_preflight_destroy_v1(&preflight);
	rf_page_stable_base_proof_destroy_v1(&proof);
}

UT_TEST(test_shared_proofs_reject_unspecified_writer_generation)
{
	for (int shared = 0; shared < 2; shared++) {
		SourceFixture f;
		GraphFixture g;
		RfPageStableBaseProofRequestV1 request;
		RfPageStableBaseProofV1 *proof = NULL;
		RfPageProofDetailV1 result;

		sources_init(&f);
		cluster_shared_config = shared;
		f.graph.cuts[1].origin_owner_incarnation = 0;
		result = sources_build(&f, &proof);
		UT_ASSERT_EQ(result == RF_PAGE_PROOF_DETAIL_OK, !shared);
		rf_page_stable_base_proof_destroy_v1(&proof);
		graph_init(&g);
		proof_request(&g, &request);
		g.cuts[0].origin_owner_incarnation = 0;
		result = rf_page_stable_base_proof_build_wait_v1(&request, g.chain, lengthof(g.chain), 1000,
														 &proof);
		UT_ASSERT_EQ(result == RF_PAGE_PROOF_DETAIL_OK, !shared);
		rf_page_stable_base_proof_destroy_v1(&proof);
	}
	cluster_shared_config = true;
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

UT_TEST(test_stable_proof_covers_exact_ancestors_before_nearest_anchor)
{
	GraphFixture fixture;
	RfPageStableBaseProofRequestV1 request;
	RfPageStableBaseProofV1 *proof = NULL;
	RfPageVersionV1 first = make_version(1, 11);
	RfPageVersionV1 middle = make_version(1, 30);
	RfPageVersionV1 last = make_version(1, 10);
	RfPageVersionV1 unknown = make_version(1, 29);
	RfPageIdentityV1 wrong;

	graph_init(&fixture);
	set_edge(&fixture.edges[1], &fixture.identity, RF_PAGE_STATE_PRESENT, &first, &middle, 0, 2, 0);
	set_edge(&fixture.edges[2], &fixture.identity, RF_PAGE_STATE_PRESENT, &middle, &last,
			 RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE, 3, 0);
	fixture.request.expected_result = last;
	graph_recount(&fixture, 1, 3);
	proof_request(&fixture, &request);
	UT_ASSERT_EQ(rf_page_stable_base_proof_build_bound_v1(&request, fixture.chain,
														  lengthof(fixture.chain), &proof),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(proof != NULL);
	UT_ASSERT(rf_page_stable_base_proof_covers_version_v1(proof, &fixture.identity, &first));
	UT_ASSERT(rf_page_stable_base_proof_covers_version_v1(proof, &fixture.identity, &middle));
	UT_ASSERT(!rf_page_stable_base_proof_covers_version_v1(proof, &fixture.identity, &last));
	UT_ASSERT(!rf_page_stable_base_proof_covers_version_v1(proof, &fixture.identity, &unknown));
	wrong = fixture.identity;
	wrong.blockno++;
	UT_ASSERT(!rf_page_stable_base_proof_covers_version_v1(proof, &wrong, &middle));
	middle.segment_incarnation[0]++;
	UT_ASSERT(!rf_page_stable_base_proof_covers_version_v1(proof, &fixture.identity, &middle));
	rf_page_stable_base_proof_destroy_v1(&proof);
	UT_ASSERT(!rf_page_stable_base_proof_covers_version_v1(proof, &fixture.identity, &first));
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
	UT_PLAN(58);
	UT_RUN(test_shared_proofs_reject_unspecified_writer_generation);
	UT_RUN(test_sources_bind_each_original_owner);
	UT_RUN(test_sources_do_not_borrow_first_fence_for_later_origin);
	UT_RUN(test_sources_reject_stale_later_member_and_pin);
	UT_RUN(test_sources_require_exact_cuts_and_namespace);
	UT_RUN(test_sources_proof_rejects_rebound_original_objects);
	UT_RUN(test_sources_reject_incomplete_input_and_readonly_ir);
	UT_RUN(test_sources_keep_authority_for_projected_empty_origin);
	UT_RUN(test_sources_page_adapter_rechecks_all_members);
	UT_RUN(test_stable_proof_binds_exact_borrowed_owners);
	UT_RUN(test_stable_proof_accepts_only_current_bound_pin);
	UT_RUN(test_stable_proof_covers_exact_ancestors_before_nearest_anchor);
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
