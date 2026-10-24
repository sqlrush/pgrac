/* Actual GCS observation wrappers and publisher; native service is the boundary. */
int content_trace_fixture_main(void);
#define main content_trace_fixture_main
#include "test_cluster_content_trace.c"
#undef main
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_resource_x_node_wire.h"
#include "cluster/cluster_semantic_activation.h"

sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static bool service_throws, service_cleans;
static unsigned source_calls, finish_calls;
static ResourceXDecodedFrame frame;
static ClusterWaitPcmAdmission admission, expected_admission;
static bool nested_finish;
static ResourceXApplyResult run_nested_finish(void);

void pg_re_throw(void)
{
	if (PG_exception_stack) siglongjmp(*PG_exception_stack, 1);
	abort();
}

static ResourceXApplyResult
service_boundary(const ResourceXDecodedFrame *block, int32 master, uint64 r4, int site)
{
	UT_ASSERT(block == &frame); UT_ASSERT_EQ(master, 2); UT_ASSERT_EQ(r4, 103);
	if (cluster_trace_tx_identity) {
		cluster_content_trace_capture(NULL, &cut);
		UT_ASSERT_EQ(cut.nowners, 1); UT_ASSERT(cut.owners[0].pcm_serving);
		UT_ASSERT_EQ(cut.owners[0].pcm_service.site, site);
		UT_ASSERT_EQ(cut.owners[0].pcm_service.tag.forkNum, SPACE_FORKNUM);
		UT_ASSERT_EQ(cut.owners[0].pcm_service.requester, 3);
		UT_ASSERT_EQ(cut.owners[0].pcm_service.source, 0);
		UT_ASSERT_EQ(cut.owners[0].pcm_service.request_seq, 100);
		UT_ASSERT_EQ(cut.owners[0].pcm_service.formation, 101);
		UT_ASSERT_EQ(cut.owners[0].pcm_service.master_session, 102);
		UT_ASSERT_EQ(cut.owners[0].pcm_service.r4, 103);
		UT_ASSERT_EQ(cut.owners[0].pcm_service.base, 0);
		UT_ASSERT_EQ(cut.owners[0].pcm_service.wire_sender_connection, 104);
		UT_ASSERT_EQ(cut.owners[0].pcm_service.requester_connection, 0);
		UT_ASSERT_EQ(cut.owners[0].pcm_service.authority, 300);
		UT_ASSERT_EQ(cut.owners[0].pcm_service.ordered_lane, 7);
		UT_ASSERT_EQ(cut.owners[0].pcm_service.semantic_crc32c, 301);
		UT_ASSERT_EQ(cut.owners[0].pcm_service.common_flags, 1);
		UT_ASSERT_EQ(cut.owners[0].pcm_service.source_candidate, 1);
		UT_ASSERT_EQ(cut.owners[0].pcm_service.retain_pi_if_dirty, 1);
		UT_ASSERT(memcmp(&cut.owners[0].pcm_service.service_admission,
			&expected_admission, sizeof(expected_admission)) == 0);
		if (nested_finish && site == CLWAIT_SITE_PCM_SOURCE_SERVICE)
			UT_ASSERT_EQ(run_nested_finish(), RESOURCE_X_APPLY_STALE);
	} else UT_ASSERT(!local_owner.pcm_serving);
	if (service_cleans) cluster_content_trace_error_cleanup();
	if (service_throws) pg_re_throw();
	return RESOURCE_X_APPLY_STALE;
}

static ResourceXApplyResult
gcs_block_pcm_x_resource_x_source_block_to_n(const ResourceXDecodedFrame *block,
	int32 master, uint64 r4)
{
	source_calls++;
	if (block == NULL) {
		UT_ASSERT(!local_owner.pcm_serving);
		return RESOURCE_X_APPLY_INVALID;
	}
	return service_boundary(block, master, r4, CLWAIT_SITE_PCM_SOURCE_SERVICE);
}

static ResourceXApplyResult
gcs_block_resource_x_source_finish_owned(
	const ResourceXDecodedFrame *block, int32 master, uint64 r4, BufferDesc *buffer,
	ClusterPcmOwnSnapshot revoking, const ResourceXDecodedFrame *image,
	ClusterPcmOwnHeldXRevoke held, ResourceXLocalOwnerHandle owner, volatile bool held_active,
	bool owner_held, bool tagless, bool drop, ResourceXApplyResult result)
{
	finish_calls++;
	UT_ASSERT(buffer == buf(2)); UT_ASSERT(image == &frame);
	UT_ASSERT_EQ(revoking.generation, 201); UT_ASSERT_EQ(held.buffer_id, 202);
	UT_ASSERT_EQ(owner.owner_generation, 203); UT_ASSERT(held_active);
	UT_ASSERT(owner_held); UT_ASSERT(tagless); UT_ASSERT(!drop);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED);
	return service_boundary(block, master, r4, CLWAIT_SITE_PCM_SOURCE_FINISH);
}

#include "test_cluster_pcm_service_trace.inc"

static void service_reset(void)
{
	reset(); memset(&frame, 0, sizeof(frame));
	frame.common.logical_assertion.resource = buf(2)->tag;
	frame.common.logical_assertion.resource.forkNum = SPACE_FORKNUM;
	frame.common.logical_assertion.requester_node = 3;
	frame.common.assertion_sequence = 100; frame.common.resource_formation = 101;
	frame.common.master_session_incarnation = 102;
	frame.common.sender_connection_generation = 104; frame.common.action_node = 0;
	frame.common.authority_generation = 300; frame.common.ordered_lane = 7;
	frame.common.semantic_crc32c = 301; frame.common.flags = 1;
	frame.common.source_candidate = frame.common.retain_pi_if_dirty = 1;
	{
		ClusterSemanticAdmissionToken token = {
			.feature_bit = CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1,
			.record_generation = 103, .formation_epoch = 101,
			.side = CLUSTER_SEMANTIC_TARGET_SIDE, .entered = true};
		ResourceXGateSnapshot gate = {.phase = RESOURCE_X_GATE_OPEN,
			.formation = 101, .freeze_generation = 105};
		gcs_block_pcm_service_admission(&admission, &token, &gate,
			CLWAIT_PCM_SERVICE_INGRESS, 2, 102, 61);
		expected_admission = admission;
	}
	nested_finish = false;
	service_throws = service_cleans = false; source_calls = finish_calls = 0;
}

static ResourceXApplyResult run_nested_finish(void)
{
	ClusterPcmOwnSnapshot revoking = {.generation = 201};
	ClusterPcmOwnHeldXRevoke held = {.buffer_id = 202};
	ResourceXLocalOwnerHandle owner = {.owner_generation = 203};
	return gcs_block_resource_x_source_finish_owned_observed(&frame, 2, 103, buf(2),
		revoking, &frame, held, owner, true, true, true, false,
		RESOURCE_X_APPLY_APPLIED, NULL);
}

UT_TEST(source_wrapper_preserves_result_and_retires)
{
	service_reset();
	UT_ASSERT_EQ(gcs_block_pcm_x_resource_x_source_block_to_n_observed(&frame, 2, 103, &admission), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(source_calls, 1); UT_ASSERT(!local_owner.pcm_serving);
	UT_ASSERT_EQ(gcs_block_pcm_x_resource_x_source_block_to_n_observed(NULL, 2, 103, &admission), RESOURCE_X_APPLY_INVALID);
	cluster_trace_tx_identity = false;
	UT_ASSERT_EQ(gcs_block_pcm_x_resource_x_source_block_to_n_observed(&frame, 2, 103, &admission), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(source_calls, 3); UT_ASSERT_EQ(logs, 0);
}

UT_TEST(source_wrapper_error_retires_without_resurrection)
{
	ClusterContentTraceServiceScope outer;
	ClusterWaitTrace previous = {0};
	volatile bool caught = false;
	service_reset(); previous.request_seq = 999;
	cluster_content_trace_service_begin(&outer, &previous);
	service_throws = service_cleans = true;
	PG_TRY(); { (void)gcs_block_pcm_x_resource_x_source_block_to_n_observed(&frame, 2, 103, &admission); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT(!local_owner.pcm_serving);
	cluster_content_trace_service_end(&outer); UT_ASSERT(!local_owner.pcm_serving);
	cluster_content_trace_capture(NULL, &cut); UT_ASSERT(!cut.owners[0].pcm_serving);
}

UT_TEST(finish_wrapper_preserves_native_arguments_and_error)
{
	ClusterPcmOwnSnapshot revoking = {0};
	ClusterPcmOwnHeldXRevoke held = {0};
	ResourceXLocalOwnerHandle owner = {0};
	volatile bool caught = false;
	service_reset(); revoking.generation = 201; held.buffer_id = 202; owner.owner_generation = 203;
	UT_ASSERT_EQ(gcs_block_resource_x_source_finish_owned_observed(&frame, 2, 103, buf(2),
		revoking, &frame, held, owner, true, true, true, false, RESOURCE_X_APPLY_APPLIED, &admission), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(!local_owner.pcm_serving); UT_ASSERT_EQ(finish_calls, 1);
	service_throws = true;
	PG_TRY(); {
		(void)gcs_block_resource_x_source_finish_owned_observed(&frame, 2, 103, buf(2),
			revoking, &frame, held, owner, true, true, true, false, RESOURCE_X_APPLY_APPLIED, &admission);
	}
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT(!local_owner.pcm_serving); UT_ASSERT_EQ(finish_calls, 2);
}

UT_TEST(native_admission_preserves_receiver_generation_and_nested_finish)
{
	service_reset();
	UT_ASSERT(admission.supplied); UT_ASSERT(admission.known); UT_ASSERT(admission.entered);
	UT_ASSERT_EQ(admission.peer_capability_generation, 61);
	UT_ASSERT_EQ(frame.common.sender_connection_generation, 104);
	UT_ASSERT_EQ(admission.gate_freeze, 105);
	for (int origin = CLWAIT_PCM_SERVICE_INGRESS; origin <= CLWAIT_PCM_SERVICE_FINISH; origin++) {
		admission.origin = expected_admission.origin = origin;
		nested_finish = true;
		UT_ASSERT_EQ(gcs_block_pcm_x_resource_x_source_block_to_n_observed(&frame, 2, 103,
			&admission), RESOURCE_X_APPLY_STALE);
		UT_ASSERT(!local_owner.pcm_serving);
	}
	UT_ASSERT_EQ(source_calls, 3); UT_ASSERT_EQ(finish_calls, 3);
}

UT_TEST(native_admission_rejects_incomplete_or_mismatched_context)
{
	for (int drift = 0; drift < 15; drift++) {
		ClusterSemanticAdmissionToken token = {
			.feature_bit = CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1,
			.record_generation = 103, .formation_epoch = 101,
			.side = CLUSTER_SEMANTIC_TARGET_SIDE, .entered = true};
		ResourceXGateSnapshot gate = {.phase = RESOURCE_X_GATE_OPEN, .formation = 101};
		int origin = CLWAIT_PCM_SERVICE_INGRESS, master = 2;
		uint64 session = 102;
		uint32 generation = 61;
		service_reset();
		switch (drift) {
		case 0: token.entered = false; break;
		case 1: token.feature_bit = 1; break;
		case 2: token.side = CLUSTER_SEMANTIC_SOURCE_SIDE; break;
		case 3: token.record_generation = 0; break;
		case 4: token.record_generation = UINT64_MAX; break;
		case 5: token.formation_epoch = 0; break;
		case 6: gate.phase = RESOURCE_X_GATE_OPEN + 1; break;
		case 7: gate.formation++; break;
		case 8: origin = 0; break;
		case 9: master = -1; break;
		case 10: master = 32; break;
		case 11: session = 0; break;
		case 12: session = UINT64_MAX; break;
		case 13: generation = 0; break;
		case 14: origin = CLWAIT_PCM_SERVICE_FINISH + 1; break;
		}
		gcs_block_pcm_service_admission(&admission, &token, &gate, origin,
			master, session, generation);
		UT_ASSERT(admission.supplied); UT_ASSERT(!admission.known);
		UT_ASSERT_EQ(admission.origin, origin);
	}
	service_reset();
	admission.record_generation++; expected_admission = admission;
	expected_admission.known = false;
	UT_ASSERT_EQ(gcs_block_pcm_x_resource_x_source_block_to_n_observed(&frame, 2, 103,
		&admission), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(!local_owner.pcm_serving);
}

int main(void)
{
	UT_PLAN(5);
	UT_RUN(source_wrapper_preserves_result_and_retires);
	UT_RUN(source_wrapper_error_retires_without_resurrection);
	UT_RUN(finish_wrapper_preserves_native_arguments_and_error);
	UT_RUN(native_admission_preserves_receiver_generation_and_nested_finish);
	UT_RUN(native_admission_rejects_incomplete_or_mismatched_context);
	UT_DONE(); free(content_trace); return ut_failed_count ? 1 : 0;
}
