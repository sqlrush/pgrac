/* Actual GCS terminal ingress with explicit dependency doubles. This proves
 * routing/call boundaries; PCM tests separately execute real T1/T2/T3. */
#include "postgres.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_semantic_activation.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static int master;
static int scenario;
static int peer_checks;
static int rechecks;
static int installs;
static int publishes;
static int leaves;
int cluster_node_id;
static int kind9_requests;
static int ack_sends;
static int assert_sends;
static int joins;
static bool fused_reply;
enum { GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_KIND9_REQUEST, GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_KIND9_ACK };

bool errstart(int level, const char *domain);
bool
errstart(int level, const char *domain)
{
	(void)domain;
	return level >= ERROR;
}

int
errmsg_internal(const char *fmt, ...)
{
	(void)fmt;
	return 0;
}

int
errdetail(const char *fmt, ...)
{
	(void)fmt;
	return 0;
}

void
errfinish(const char *file, int line, const char *func)
{
	(void)file;
	(void)line;
	(void)func;
	abort();
}

static bool
gcs_block_resource_x_diagnostic_should_log(int kind, int result)
{
	(void)kind;
	(void)result;
	return false;
}

static bool
gcs_block_pcm_x_resource_x_peer_ready_exact(int node, uint32 *connection)
{
	(void)node;
	*connection = 61;
	return scenario != 14;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_requester_join_current_exact(const ResourceXDecodedFrame *frame,
														 int32 source, uint32 source_ingress,
														 int32 current_master,
														 uint32 master_ingress, uint64 r4,
														 ResourceXRequesterJoinSnapshot *out)
{
	(void)frame;
	(void)out;
	UT_ASSERT_EQ(source, 1);
	UT_ASSERT_EQ(source_ingress, 61);
	UT_ASSERT_EQ(current_master, master);
	UT_ASSERT_EQ(master_ingress, 61);
	UT_ASSERT_EQ(r4, 77);
	joins++;
	return RESOURCE_X_APPLY_APPLIED;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_bootstrap_request_exact(const ResourceXDecodedFrame *request,
													int32 source, uint32 ingress, uint64 r4,
													uint64 session, uint32 outbound,
													ResourceXDecodedFrame *reply)
{
	UT_ASSERT_EQ(source, 1);
	UT_ASSERT_EQ(ingress, 61);
	UT_ASSERT_EQ(r4, 77);
	UT_ASSERT_EQ(session, 31);
	UT_ASSERT_EQ(outbound, 61);
	kind9_requests++;
	*reply = *request;
	reply->kind = fused_reply ? RESOURCE_X_WIRE_ASSERT_X : RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP;
	reply->common.flags = 0;
	reply->common.base_authority_generation = 2;
	reply->common.authority_generation = fused_reply ? 2 : 0;
	return RESOURCE_X_APPLY_APPLIED;
}

static bool
gcs_block_resource_x_bootstrap_ack_stage_exact(int node, const ResourceXDecodedFrame *reply)
{
	(void)node;
	(void)reply;
	ack_sends++;
	return true;
}

ResourceXBootstrapRoundAction
cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(const ResourceXDecodedFrame *ack,
															 int32 source, uint32 ingress,
															 uint64 r4, uint64 now,
															 ResourceXDecodedFrame *assertion)
{
	(void)source;
	(void)ingress;
	(void)r4;
	(void)now;
	*assertion = *ack;
	return RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT;
}

static ResourceXApplyResult
gcs_block_resource_x_native_assert_stage_exact(int node, const ResourceXDecodedFrame *frame)
{
	(void)node;
	(void)frame;
	assert_sends++;
	return RESOURCE_X_APPLY_APPLIED;
}

void
cluster_pcm_lock_resource_x_trace_frame(uint16 kind, const ResourceXDecodedFrame *frame, int32 peer,
										int32 detail)
{
	(void)kind;
	(void)frame;
	(void)peer;
	(void)detail;
}

void
pg_re_throw(void)
{
	abort();
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	(void)condition;
	(void)file;
	(void)line;
	abort();
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_enter(uint64 feature, ClusterSemanticAdmissionSide side,
								  ClusterSemanticAdmissionToken *token)
{
	memset(token, 0, sizeof(*token));
	token->feature_bit = feature;
	token->side = side;
	token->record_generation = 77;
	token->entered = true;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

bool
cluster_semantic_activation_recheck(const ClusterSemanticAdmissionToken *token)
{
	rechecks++;
	return token->entered && scenario != 10 && !(scenario == 11 && rechecks == 2);
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	leaves++;
	token->entered = false;
}

static bool
gcs_block_resource_x_target_peer_matches_exact(const ClusterSemanticAdmissionToken *token,
											   int32 source, uint32 connection)
{
	peer_checks++;
	return token->entered && source >= 0 && connection == 61 && scenario != 7
		   && !(scenario == 13 && peer_checks == 2);
}

static bool
gcs_block_resource_x_gate_session_snapshot(const BufferTag *tag, ResourceXGateSnapshot *gate,
										   int32 *node, uint64 *session)
{
	(void)tag;
	memset(gate, 0, sizeof(*gate));
	gate->formation = scenario == 9 ? 18 : 17;
	*node = master;
	*session = 31;
	return true;
}

static bool
gcs_block_resource_x_gate_session_recheck(const BufferTag *tag, const ResourceXGateSnapshot *gate,
										  int32 node, uint64 session)
{
	(void)tag;
	return scenario != 12 && gate->formation == 17 && node == master && session == 31;
}

static ResourceXApplyResult
gcs_block_pcm_x_resource_x_join_terminal_try(const ResourceXAssertion *assertion,
											 bool scheduled_retry, ResourceXAcquisitionRef *ref,
											 uint64 *ownership, uint64 *authority)
{
	(void)scheduled_retry;
	installs++;
	ref->assertion = *assertion;
	ref->formation = 17;
	ref->acquisition_generation = 41;
	*ownership = 9;
	*authority = 4;
	return RESOURCE_X_APPLY_APPLIED;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_bootstrap_round_publish_terminal_exact(
	const ResourceXAcquisitionRef *ref, uint64 session, uint64 record, uint64 ownership,
	uint64 authority, uint64 now)
{
	publishes++;
	UT_ASSERT_EQ(ref->acquisition_generation, UINT64_C(41));
	UT_ASSERT_EQ(session, UINT64_C(31));
	UT_ASSERT_EQ(record, UINT64_C(77));
	UT_ASSERT_EQ(ownership, UINT64_C(9));
	UT_ASSERT_EQ(authority, UINT64_C(4));
	UT_ASSERT_EQ(now, UINT64_C(200));
	return RESOURCE_X_APPLY_APPLIED;
}

static uint64
gcs_block_pcm_x_monotonic_us(void)
{
	return 200;
}

#include "test_cluster_resource_x_terminal_owner.inc"

UT_TEST(test_actual_terminal_ingress_keeps_master_and_physical_source_distinct)
{
	for (scenario = 0; scenario < 14; scenario++) {
		ClusterICEnvelope env = { 0 };
		ResourceXDecodedFrame frame = { 0 };
		bool reaches_install = scenario <= 2 || scenario == 4 || scenario >= 11;
		bool completes = scenario <= 2 || scenario == 4;

		master = scenario == 1 ? 2 : scenario == 2 ? 1 : 0;
		peer_checks = rechecks = installs = publishes = leaves = 0;
		env.source_node_id = scenario == 4 ? 0 : 1;
		frame.kind = scenario == 4 || scenario == 5 ? RESOURCE_X_WIRE_AUTHORITY_GRANT
					 : scenario == 6				? RESOURCE_X_WIRE_BLOCKED_TO_N
													: RESOURCE_X_WIRE_IMAGE_ENVELOPE;
		frame.common.flags = scenario == 3 || scenario == 4 || scenario == 5
								 ? 0
								 : RESOURCE_X_COMMON_FLAG_AUTHORITY_WITH_IMAGE;
		frame.common.logical_assertion.requester_node = 2;
		frame.common.resource_formation = 17;
		frame.common.master_session_incarnation = scenario == 8 ? 32 : 31;
		UT_ASSERT_EQ(gcs_block_resource_x_requester_terminal_try(&env, &frame, 61, false),
					 completes ? RESOURCE_X_APPLY_APPLIED : RESOURCE_X_APPLY_STALE);
		UT_ASSERT_EQ(installs, reaches_install ? 1 : 0);
		UT_ASSERT_EQ(publishes, completes ? 1 : 0);
		UT_ASSERT_EQ(leaves, 1);
	}
}

UT_TEST(test_actual_kind9_ingress_does_not_send_ack_for_fused_admission)
{
	int fused;

	for (fused = 0; fused < 2; fused++) {
		ClusterICEnvelope env = { 0 };
		ResourceXDecodedFrame frame = { 0 };
		master = cluster_node_id = scenario = 0;
		peer_checks = rechecks = leaves = kind9_requests = ack_sends = assert_sends = 0;
		fused_reply = fused != 0;
		env.source_node_id = 1;
		env.msg_type = RESOURCE_X_MSG_ASSERT_X;
		frame.kind = RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP;
		frame.common.logical_assertion.requester_node = 1;
		frame.common.flags = RESOURCE_X_COMMON_FLAG_REMOTE_ADMISSION;
		frame.common.resource_formation = 17;
		frame.common.master_session_incarnation = 31;
		gcs_block_resource_x_kind9_ingress(&env, &frame, 61);
		UT_ASSERT_EQ(kind9_requests, 1);
		UT_ASSERT_EQ(ack_sends, fused ? 0 : 1);
		UT_ASSERT_EQ(assert_sends, 0);
		UT_ASSERT_EQ(leaves, 1);
	}
}

UT_TEST(test_actual_join_ingress_carries_current_authority_coordinates)
{
	int test_case;
	const int cases[] = { 0, 0, 0, 7, 9, 8, 10, 13, 14, 12 };

	for (test_case = 0; test_case < lengthof(cases); test_case++) {
		ClusterICEnvelope env = { 0 };
		ResourceXDecodedFrame frame = { 0 };
		ResourceXRequesterJoinSnapshot out;
		master = test_case == 1 ? 1 : test_case == 2 ? 2 : 0;
		scenario = cases[test_case];
		peer_checks = rechecks = leaves = joins = 0;
		env.source_node_id = 1;
		frame.kind = RESOURCE_X_WIRE_IMAGE_ENVELOPE;
		frame.common.flags = RESOURCE_X_COMMON_FLAG_AUTHORITY_WITH_IMAGE;
		frame.common.resource_formation = 17;
		frame.common.master_session_incarnation = scenario == 8 ? 32 : 31;
		UT_ASSERT_EQ(gcs_block_resource_x_requester_join_ingress(&env, &frame, 61, &out),
					 test_case < 3 ? RESOURCE_X_APPLY_APPLIED : RESOURCE_X_APPLY_STALE);
		UT_ASSERT_EQ(joins, test_case < 3 ? 1 : 0);
		UT_ASSERT_EQ(leaves, 1);
	}
}

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(test_actual_terminal_ingress_keeps_master_and_physical_source_distinct);
	UT_RUN(test_actual_kind9_ingress_does_not_send_ack_for_fused_admission);
	UT_RUN(test_actual_join_ingress_carries_current_authority_coordinates);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
