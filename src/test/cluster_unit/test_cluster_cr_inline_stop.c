/* Author: SqlRush <sqlrush@gmail.com> */
/* Actual inline serve entry; constructor/transport, service-seal state and
 * scratch allocation are boundaries. This proves admission precedes every
 * inline read context, not remote CR correctness or native shutdown. */
#include "postgres.h"
#include "miscadmin.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_gcs_block_dedup.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_tier1.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_lms_shard.h"
#include "cluster/cluster_write_fence.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#undef printf
#undef fprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
int cluster_node_id = 1, cluster_lms_workers = 4, MaxBackends = 128;
int cluster_injection_armed_count;
MemoryContext CurrentMemoryContext, TopMemoryContext;
static MemoryContext CrServeScratchCtx;
static char caller_context, scratch_context;
static bool new_work_allowed, fence_allowed, seal_during_serve;
static int gate_calls, scratch_calls, serve_calls, reply_calls, reset_calls;
static int direct_replies, serve_observations, fence_refusals;
static ClusterLmsCrSlot last_served, last_reply;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d %s\n", file, line, condition);
	abort();
}
bool
errstart(int level, const char *domain)
{
	return false;
}
bool
errstart_cold(int level, const char *domain)
{
	return false;
}
int
errmsg_internal(const char *fmt, ...)
{
	return 0;
}
void
errfinish(const char *file, int line, const char *func)
{}

bool
cluster_normal_stop_service_new_work(bool modifies_data)
{
	Assert(!modifies_data);
	gate_calls++;
	return new_work_allowed;
}
int
cluster_ic_tier1_my_data_channel(void)
{
	return 3;
}
int
cluster_lms_shard_for_tag(const BufferTag *tag, int count)
{
	return 3;
}
void
cluster_gcs_block_dedup_note_misroute(void)
{
	abort();
}
bool
cluster_write_fence_enforcing(void)
{
	return true;
}
bool
cluster_write_fence_allowed(void)
{
	return fence_allowed;
}
void
cluster_injection_run(const char *name)
{
	abort();
}
bool
cluster_injection_should_skip(const char *name)
{
	return false;
}
uint64
cluster_epoch_get_current(void)
{
	return 7;
}
TimestampTz
GetCurrentTimestamp(void)
{
	static TimestampTz now;
	return ++now;
}
void
cluster_lms_obs_note_direct_reply(void)
{
	direct_replies++;
}
void
cluster_lms_obs_note_inline_serve(uint64 elapsed)
{
	serve_observations++;
}
void
cluster_cr_server_stat_bump(ClusterCrServerStat which)
{
	Assert(which == CLUSTER_CR_SERVER_STAT_FENCE_REFUSED);
	fence_refusals++;
}
bool
cluster_cr_server_freshref_c1b_pair_request_decode(const GcsBlockForwardPayload *fwd, int32 source,
												   int32 local, uint64 epoch, int backends,
												   uint32 *segment, TransactionId *xid,
												   uint32 *slot, SCN *scn)
{
	abort(); /* Freshref decode is outside these ordinary inline cases. */
	return false;
}
static MemoryContext
cr_serve_scratch_context(void)
{
	scratch_calls++;
	CrServeScratchCtx = (MemoryContext)&scratch_context;
	return CrServeScratchCtx;
}
void
MemoryContextReset(MemoryContext context)
{
	Assert(context == CrServeScratchCtx);
	reset_calls++;
}
static void
cr_serve_slot(ClusterLmsCrSlot *slot)
{
	Assert(CurrentMemoryContext == CrServeScratchCtx);
	serve_calls++;
	last_served = *slot;
	slot->result_status = GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER;
	if (seal_during_serve)
		new_work_allowed = false;
}
static void
cr_build_and_send_reply(const ClusterLmsCrSlot *slot)
{
	Assert(CurrentMemoryContext == CrServeScratchCtx);
	reply_calls++;
	last_reply = *slot;
}

#include "test_cluster_cr_inline_stop.inc"

static GcsBlockForwardPayload
reset_test(void)
{
	GcsBlockForwardPayload fwd = { 0 };
	new_work_allowed = fence_allowed = true;
	seal_during_serve = false;
	gate_calls = scratch_calls = serve_calls = reply_calls = reset_calls = 0;
	direct_replies = serve_observations = fence_refusals = 0;
	memset(&last_served, 0, sizeof(last_served));
	memset(&last_reply, 0, sizeof(last_reply));
	CurrentMemoryContext = (MemoryContext)&caller_context;
	CrServeScratchCtx = NULL;
	fwd.request_id = 19;
	fwd.epoch = 7;
	fwd.original_requester_node = 2;
	fwd.master_node = 0;
	fwd.requester_backend_id = 5;
	fwd.tag.spcOid = 1663;
	fwd.tag.dbOid = 5;
	fwd.tag.relNumber = 0;
	return fwd;
}
UT_TEST(test_seal_two_forbids_all_inline_read_contexts)
{
	for (int kind = CLUSTER_LMS_SLOT_KIND_CR; kind <= CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT; kind++) {
		GcsBlockForwardPayload fwd = reset_test(), before = fwd;
		new_work_allowed = false;
		cluster_gcs_block_forward_serve_inline(&fwd, kind);
		UT_ASSERT_EQ(gate_calls, 1);
		UT_ASSERT_EQ(scratch_calls + serve_calls + reply_calls + reset_calls, 0);
		UT_ASSERT_EQ(direct_replies + serve_observations, 0);
		UT_ASSERT_EQ(memcmp(&fwd, &before, sizeof(fwd)), 0);
		UT_ASSERT(CurrentMemoryContext == (MemoryContext)&caller_context);
	}
}
UT_TEST(test_seal_one_allows_original_read_and_reply)
{
	for (int kind = CLUSTER_LMS_SLOT_KIND_CR; kind <= CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT; kind++) {
		GcsBlockForwardPayload fwd = reset_test();
		cluster_gcs_block_forward_serve_inline(&fwd, kind);
		UT_ASSERT_EQ(gate_calls, 1);
		UT_ASSERT_EQ(serve_calls, 1);
		UT_ASSERT_EQ(reply_calls, 1);
		UT_ASSERT_EQ(last_served.request_id, fwd.request_id);
		UT_ASSERT_EQ(last_served.req_kind, kind);
		UT_ASSERT_EQ(last_served.undo_owner, -1);
		UT_ASSERT_EQ(last_reply.result_status, GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER);
		UT_ASSERT_EQ(reset_calls, 1);
		UT_ASSERT(CurrentMemoryContext == (MemoryContext)&caller_context);
	}
}
UT_TEST(test_existing_admission_keeps_its_original_completion)
{
	GcsBlockForwardPayload fwd = reset_test();
	seal_during_serve = true;
	cluster_gcs_block_forward_serve_inline(&fwd, CLUSTER_LMS_SLOT_KIND_CR);
	UT_ASSERT_EQ(gate_calls, 1);
	UT_ASSERT(!new_work_allowed);
	UT_ASSERT_EQ(serve_calls, 1);
	UT_ASSERT_EQ(reply_calls, 1);
	UT_ASSERT_EQ(reset_calls, 1);
}
UT_TEST(test_fence_denial_is_preserved_but_does_not_bypass_final_seal)
{
	GcsBlockForwardPayload fwd = reset_test();
	fence_allowed = false;
	cluster_gcs_block_forward_serve_inline(&fwd, CLUSTER_LMS_SLOT_KIND_CR);
	UT_ASSERT_EQ(serve_calls, 0);
	UT_ASSERT_EQ(reply_calls, 1);
	UT_ASSERT_EQ(last_reply.result_status, GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER);
	UT_ASSERT_EQ(fence_refusals, 1);
	fwd = reset_test();
	new_work_allowed = fence_allowed = false;
	cluster_gcs_block_forward_serve_inline(&fwd, CLUSTER_LMS_SLOT_KIND_CR);
	UT_ASSERT_EQ(gate_calls, 1);
	UT_ASSERT_EQ(scratch_calls + serve_calls + reply_calls, 0);
}
UT_TEST(test_park_only_kinds_never_enter_generic_serve)
{
	for (int kind = CLUSTER_LMS_SLOT_KIND_UNDO_MULTI_VERDICT;
		 kind <= CLUSTER_LMS_SLOT_KIND_R4_CR_BUILD; kind++) {
		GcsBlockForwardPayload fwd = reset_test();
		cluster_gcs_block_forward_serve_inline(&fwd, kind);
		UT_ASSERT_EQ(serve_calls, 0);
		UT_ASSERT_EQ(reply_calls, 1);
		UT_ASSERT_EQ(last_reply.result_status, GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER);
		fwd = reset_test();
		new_work_allowed = false;
		cluster_gcs_block_forward_serve_inline(&fwd, kind);
		UT_ASSERT_EQ(scratch_calls + serve_calls + reply_calls, 0);
	}
}
UT_TEST(test_null_does_not_allocate_or_use_service_gate)
{
	reset_test();
	cluster_gcs_block_forward_serve_inline(NULL, CLUSTER_LMS_SLOT_KIND_CR);
	UT_ASSERT_EQ(gate_calls + scratch_calls + serve_calls + reply_calls, 0);
}
int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_seal_two_forbids_all_inline_read_contexts);
	UT_RUN(test_seal_one_allows_original_read_and_reply);
	UT_RUN(test_existing_admission_keeps_its_original_completion);
	UT_RUN(test_fence_denial_is_preserved_but_does_not_bypass_final_seal);
	UT_RUN(test_park_only_kinds_never_enter_generic_serve);
	UT_RUN(test_null_does_not_allocate_or_use_service_gate);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
