/* Actual GCS observation wrappers and publisher; native service is the boundary. */
int content_trace_fixture_main(void);
#define main content_trace_fixture_main
#include "test_cluster_content_trace.c"
#undef main
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_resource_x_node_wire.h"

sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static bool service_throws, service_cleans;
static unsigned source_calls, finish_calls;
static ResourceXDecodedFrame frame;

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
	service_throws = service_cleans = false; source_calls = finish_calls = 0;
}

UT_TEST(source_wrapper_preserves_result_and_retires)
{
	service_reset();
	UT_ASSERT_EQ(gcs_block_pcm_x_resource_x_source_block_to_n_observed(&frame, 2, 103), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(source_calls, 1); UT_ASSERT(!local_owner.pcm_serving);
	UT_ASSERT_EQ(gcs_block_pcm_x_resource_x_source_block_to_n_observed(NULL, 2, 103), RESOURCE_X_APPLY_INVALID);
	cluster_trace_tx_identity = false;
	UT_ASSERT_EQ(gcs_block_pcm_x_resource_x_source_block_to_n_observed(&frame, 2, 103), RESOURCE_X_APPLY_STALE);
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
	PG_TRY(); { (void)gcs_block_pcm_x_resource_x_source_block_to_n_observed(&frame, 2, 103); }
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
		revoking, &frame, held, owner, true, true, true, false, RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(!local_owner.pcm_serving); UT_ASSERT_EQ(finish_calls, 1);
	service_throws = true;
	PG_TRY(); {
		(void)gcs_block_resource_x_source_finish_owned_observed(&frame, 2, 103, buf(2),
			revoking, &frame, held, owner, true, true, true, false, RESOURCE_X_APPLY_APPLIED);
	}
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT(!local_owner.pcm_serving); UT_ASSERT_EQ(finish_calls, 2);
}

int main(void)
{
	UT_PLAN(3);
	UT_RUN(source_wrapper_preserves_result_and_retires);
	UT_RUN(source_wrapper_error_retires_without_resurrection);
	UT_RUN(finish_wrapper_preserves_native_arguments_and_error);
	UT_DONE(); free(content_trace); return ut_failed_count ? 1 : 0;
}
