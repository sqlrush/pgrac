/* Actual GCS terminal ingress with explicit dependency doubles. This proves
 * routing/call boundaries; PCM tests separately execute real T1/T2/T3. */
#include "postgres.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_semantic_activation.h"
#include "miscadmin.h"
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
BackendType MyBackendType = B_LMS;
int NBuffers = 2;
static BufferDescPadded delivery_buffers[2];
BufferDescPadded *BufferDescriptors = delivery_buffers;
static bool delivery_enabled, delivery_expired, delivery_active, delivery_ready;
static bool delivery_terminal, delivery_hold, delivery_throw;
static int delivery_claims, delivery_ends, delivery_completes, delivery_retries, cleanup_joins;
static int delivery_drift;
static uint64 delivery_sequence;
static ResourceXAcquisitionRef delivery_ref;
static uint64 delivery_metrics[PCM_RX_METRIC_COUNT];
static int delivery_trace_callbacks;
enum { GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_KIND9_REQUEST, GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_KIND9_ACK };

void
cluster_pcm_rx_metric_note(PcmRxMetric metric)
{
	delivery_metrics[metric]++;
}

void
cluster_pcm_rx_delivery_max_note(PcmRxMetric metric, uint64 elapsed_us)
{
	delivery_metrics[metric] = Max(delivery_metrics[metric], elapsed_us);
}

void
cluster_pcm_lock_resource_x_trace_note(const ResourceXTraceEvent *event)
{
	UT_ASSERT_EQ(event->kind, RESOURCE_X_TRACE_APPLY);
	UT_ASSERT_EQ(event->detail, RESOURCE_X_DELIVERY_TRACE_CALLBACK);
	UT_ASSERT_EQ(event->attempt, UINT64_C(41));
	delivery_trace_callbacks++;
}

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
	if (delivery_enabled)
		return RESOURCE_X_APPLY_STALE;
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
	if (delivery_enabled)
		return RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
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
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
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
gcs_block_pcm_x_resource_x_join_terminal_owned_try(const ResourceXAcquisitionRef *input,
												   bool scheduled_retry,
												   ResourceXAcquisitionRef *ref, uint64 *ownership,
												   uint64 *authority)
{
	(void)scheduled_retry;
	installs++;
	ref->assertion = input->assertion;
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

void
cluster_lms_wakeup(int worker_id)
{
	UT_ASSERT_EQ(worker_id, 0);
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_delivery_target_snapshot_exact(
	const ResourceXAcquisitionRef *ref, ResourceXInstallClaimJoinObservation *out,
	ResourceXDeliveryTarget *target)
{
	if (!delivery_enabled)
		return RESOURCE_X_APPLY_NOT_FOUND;
	memset(out, 0, sizeof(*out));
	memset(target, 0, sizeof(*target));
	out->request.logical_assertion = ref->assertion;
	out->request.resource_formation = ref->formation;
	out->request.assertion_sequence = ref->acquisition_generation;
	out->request.master_session_incarnation = 31;
	out->request.sender_connection_generation = 61;
	out->r4_record_generation = delivery_drift == 1 ? 78 : 77;
	out->master_node = master;
	out->master_ingress_connection_generation = delivery_drift == 2 ? 62 : 61;
	target->buffer_id_plus_one = 1;
	target->generation = 7;
	return RESOURCE_X_APPLY_APPLIED;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_delivery_claim_begin_exact(const ResourceXAcquisitionRef *ref,
													   const ResourceXDeliveryTarget *target,
													   uint8 purpose, uint64 now,
													   ResourceXDeliveryClaim *claim)
{
	ResourceXDeliveryTarget sampled;

	UT_ASSERT_EQ(now, UINT64_C(200));
	if (delivery_active || ((purpose == RESOURCE_X_DELIVERY_CLEANUP) != delivery_expired))
		return RESOURCE_X_APPLY_BAD_STATE;
	memset(claim, 0, sizeof(*claim));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_delivery_target_snapshot_exact(
					 ref, &claim->observation, &sampled),
				 RESOURCE_X_APPLY_APPLIED);
	claim->target = *target;
	claim->purpose = purpose;
	claim->executor_sequence = ++delivery_sequence;
	delivery_active = true;
	delivery_claims++;
	return RESOURCE_X_APPLY_APPLIED;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_delivery_claim_end_exact(const ResourceXDeliveryClaim *claim)
{
	UT_ASSERT_EQ(claim->executor_sequence, delivery_sequence);
	delivery_ends++;
	delivery_active = false;
	return RESOURCE_X_APPLY_APPLIED;
}

bool
cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(const ResourceXAcquisitionRef *ref,
																uint64 session, uint64 r4,
																uint64 generation)
{
	UT_ASSERT_EQ(ref->acquisition_generation, UINT64_C(41));
	UT_ASSERT_EQ(session, UINT64_C(31));
	UT_ASSERT_EQ(r4, UINT64_C(77));
	UT_ASSERT_EQ(generation, UINT64_C(8));
	return delivery_terminal;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_install_claim_snapshot_exact(const ResourceXAcquisitionRef *ref,
														 uint8 *source, uint64 *generation,
														 uint64 *token)
{
	(void)ref;
	*source = RESOURCE_X_INSTALL_CLAIM_ORDINARY_T1;
	*generation = 7;
	*token = 9;
	return RESOURCE_X_APPLY_APPLIED;
}

ClusterPcmOwnResult
cluster_bufmgr_pcm_own_snapshot(BufferDesc *buf, ClusterPcmOwnSnapshot *out)
{
	UT_ASSERT(buf == GetBufferDescriptor(0));
	memset(out, 0, sizeof(*out));
	out->tag = delivery_ref.assertion.resource;
	out->generation = delivery_terminal ? 8 : 7;
	out->reservation_token = delivery_drift == 3   ? 10
							 : delivery_drift == 4 ? 8
							 : delivery_drift == 5 ? UINT64_MAX
												   : 9;
	out->pcm_state = delivery_terminal ? PCM_STATE_X : PCM_STATE_N;
	out->buffer_type = delivery_terminal ? BUF_TYPE_XCUR : BUF_TYPE_CURRENT;
	out->semantic_buf_state = BM_TAG_VALID | BM_VALID;
	return CLUSTER_PCM_OWN_OK;
}

ClusterPcmOwnResult
cluster_bufmgr_pcm_own_delivery_snapshot_exact(BufferDesc *buf, const BufferTag *tag,
											   uint64 attempt, ClusterPcmOwnSnapshot *out)
{
	UT_ASSERT_EQ(attempt, UINT64_C(41));
	UT_ASSERT(BufferTagsEqual(tag, &delivery_ref.assertion.resource));
	if (!delivery_hold)
		return CLUSTER_PCM_OWN_STALE;
	return cluster_bufmgr_pcm_own_snapshot(buf, out);
}

ClusterPcmOwnResult
cluster_bufmgr_pcm_own_delivery_hold_release_exact(BufferDesc *buf,
												   const ClusterPcmOwnSnapshot *terminal,
												   uint64 attempt, uint64 original_install_token)
{
	UT_ASSERT(delivery_active && delivery_terminal);
	UT_ASSERT(buf == GetBufferDescriptor(0));
	UT_ASSERT_EQ(attempt, UINT64_C(41));
	UT_ASSERT_EQ(terminal->generation, UINT64_C(8));
	UT_ASSERT_EQ(original_install_token, UINT64_C(9));
	if (terminal->reservation_token < original_install_token
		|| terminal->reservation_token == UINT64_MAX
		|| (delivery_hold && terminal->reservation_token != original_install_token))
		return CLUSTER_PCM_OWN_STALE;
	delivery_hold = false;
	return CLUSTER_PCM_OWN_OK;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_delivery_complete_exact(const ResourceXDeliveryClaim *claim,
													const ClusterPcmOwnSnapshot *terminal)
{
	UT_ASSERT(delivery_active && delivery_terminal && !delivery_hold);
	UT_ASSERT_EQ(claim->executor_sequence, delivery_sequence);
	UT_ASSERT_EQ(terminal->generation, UINT64_C(8));
	delivery_completes++;
	return RESOURCE_X_APPLY_APPLIED;
}

static ResourceXApplyResult
gcs_block_pcm_x_resource_x_join_terminal_try(const ResourceXAssertion *assertion, bool retry,
											 const ResourceXDeliveryClaim *claim,
											 ResourceXAcquisitionRef *out, uint64 *generation,
											 uint64 *authority)
{
	UT_ASSERT(retry && delivery_active);
	UT_ASSERT_EQ(claim->executor_sequence, delivery_sequence);
	UT_ASSERT(resource_x_assertion_equal(assertion, &delivery_ref.assertion));
	installs++;
	if (delivery_throw)
		siglongjmp(*PG_exception_stack, 1);
	if (!delivery_ready)
		return RESOURCE_X_APPLY_NOT_FOUND;
	*out = delivery_ref;
	*generation = 8;
	*authority = 4;
	delivery_terminal = true;
	return RESOURCE_X_APPLY_APPLIED;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_delivery_dispatch_exact(const ResourceXDeliveryClaim *claim,
													ResourceXDecodedFrame *out)
{
	UT_ASSERT(delivery_active && !delivery_ready);
	memset(out, 0, sizeof(*out));
	out->kind = RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP;
	out->common = claim->observation.request;
	return RESOURCE_X_APPLY_APPLIED;
}

static bool
gcs_block_resource_x_bootstrap_request_stage_exact(int32 node, const ResourceXDecodedFrame *frame)
{
	UT_ASSERT_EQ(node, master);
	UT_ASSERT_EQ(frame->common.assertion_sequence, UINT64_C(41));
	delivery_retries++;
	return true;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_requester_join_delivery_exact(const ResourceXDecodedFrame *frame,
														  int32 source, uint32 ingress,
														  int32 current_master,
														  uint32 master_ingress, uint64 r4,
														  const ResourceXDeliveryClaim *claim,
														  ResourceXRequesterJoinSnapshot *out)
{
	UT_ASSERT_EQ(source, 1);
	UT_ASSERT_EQ(ingress, 61);
	UT_ASSERT_EQ(current_master, master);
	UT_ASSERT_EQ(master_ingress, 61);
	UT_ASSERT_EQ(r4, UINT64_C(77));
	UT_ASSERT_EQ(claim->purpose, RESOURCE_X_DELIVERY_CLEANUP);
	UT_ASSERT(delivery_active);
	UT_ASSERT_EQ(frame->common.assertion_sequence, UINT64_C(41));
	memset(out, 0, sizeof(*out));
	out->flags = RESOURCE_X_REQUESTER_JOIN_READY;
	cleanup_joins++;
	delivery_ready = true;
	return RESOURCE_X_APPLY_APPLIED;
}

ResourceXBootstrapRoundAction
cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_delivery_exact(
	const ResourceXDecodedFrame *ack, int32 source, uint32 ingress, uint64 r4, uint64 now,
	const ResourceXDeliveryClaim *claim, ResourceXDecodedFrame *assertion)
{
	UT_ASSERT_EQ(source, master);
	UT_ASSERT_EQ(ingress, 61);
	UT_ASSERT_EQ(r4, UINT64_C(77));
	UT_ASSERT_EQ(now, UINT64_C(200));
	UT_ASSERT_EQ(claim->purpose, RESOURCE_X_DELIVERY_CLEANUP);
	*assertion = *ack;
	assertion->kind = RESOURCE_X_WIRE_ASSERT_X;
	return RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT;
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

static void
reset_delivery_fixture(void)
{
	memset(delivery_metrics, 0, sizeof(delivery_metrics));
	delivery_trace_callbacks = 0;
	memset(&delivery_ref, 0, sizeof(delivery_ref));
	delivery_ref.assertion.requester_node = 2;
	delivery_ref.assertion.resource.spcOid = 1663;
	delivery_ref.assertion.resource.dbOid = 5;
	delivery_ref.assertion.resource.relNumber = 99;
	delivery_ref.assertion.resource.blockNum = 4;
	delivery_ref.formation = 17;
	delivery_ref.acquisition_generation = 41;
	master = scenario = 0;
	cluster_node_id = 2;
	MyBackendType = B_LMS;
	delivery_enabled = delivery_expired = delivery_hold = true;
	delivery_active = delivery_ready = delivery_terminal = delivery_throw = false;
	delivery_claims = delivery_ends = delivery_completes = delivery_retries = cleanup_joins = 0;
	delivery_drift = 0;
	delivery_sequence = 0;
	installs = publishes = leaves = joins = peer_checks = rechecks = assert_sends = 0;
}

UT_TEST(test_actual_quiet_tick_installs_or_redrives_without_foreground)
{
	int leg;

	for (leg = 0; leg < 5; leg++) {
		reset_delivery_fixture();
		delivery_ready = leg != 0;
		delivery_terminal = leg >= 2;
		if (leg >= 3)
			delivery_hold = false; /* Physical release happened before an unwind. */
		if (leg == 4)
			delivery_drift = 3; /* Reversible eviction consumed the next token. */
		UT_ASSERT_EQ(cluster_gcs_block_resource_x_delivery_tick(&delivery_ref),
					 leg == 0 ? RESOURCE_X_APPLY_NOT_FOUND : RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(delivery_claims, 1);
		UT_ASSERT_EQ(delivery_ends, 1);
		UT_ASSERT(!delivery_active);
		UT_ASSERT_EQ(delivery_retries, leg == 0 ? 1 : 0);
		UT_ASSERT_EQ(installs, leg < 2 ? 1 : 0);
		UT_ASSERT_EQ(delivery_completes, leg == 0 ? 0 : 1);
		UT_ASSERT_EQ(leaves, 1);
		UT_ASSERT_EQ(publishes, 0); /* No synthetic foreground terminal publish. */
		UT_ASSERT_EQ(delivery_metrics[PCM_RX_DELIVERY_CALLBACK], UINT64_C(1));
		UT_ASSERT_EQ(delivery_metrics[PCM_RX_DELIVERY_REQUEST_RETRY], leg == 0 ? 1 : 0);
		UT_ASSERT_EQ(delivery_trace_callbacks, 1);
	}
}

UT_TEST(test_actual_cleanup_ingress_retains_exact_late_frame_then_quiet_tick_finishes)
{
	ClusterICEnvelope env = { 0 };
	ResourceXDecodedFrame frame = { 0 };
	ResourceXRequesterJoinSnapshot out;

	reset_delivery_fixture();
	env.source_node_id = 1;
	frame.kind = RESOURCE_X_WIRE_IMAGE_ENVELOPE;
	frame.common.logical_assertion = delivery_ref.assertion;
	frame.common.resource_formation = 17;
	frame.common.assertion_sequence = 41;
	frame.common.master_session_incarnation = 31;
	frame.common.flags = RESOURCE_X_COMMON_FLAG_AUTHORITY_WITH_IMAGE;
	UT_ASSERT_EQ(gcs_block_resource_x_requester_join_ingress(&env, &frame, 61, &out),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(joins, 1); /* Ordinary admission ran first and returned STALE. */
	UT_ASSERT_EQ(cleanup_joins, 1);
	UT_ASSERT_EQ(delivery_metrics[PCM_RX_DELIVERY_LATE_FRAME], UINT64_C(1));
	UT_ASSERT_EQ(installs, 0);
	UT_ASSERT_EQ(delivery_ends, 1);
	UT_ASSERT_EQ(cluster_gcs_block_resource_x_delivery_tick(&delivery_ref),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(delivery_completes, 1);
	UT_ASSERT_EQ(delivery_retries, 0);
	UT_ASSERT_EQ(leaves, 2);

	reset_delivery_fixture();
	env.source_node_id = 0;
	env.msg_type = RESOURCE_X_MSG_IMAGE_OR_GRANT;
	frame.kind = RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP;
	frame.common.flags = 0;
	gcs_block_resource_x_kind9_ingress(&env, &frame, 61);
	UT_ASSERT_EQ(delivery_claims, 1);
	UT_ASSERT_EQ(delivery_ends, 1);
	UT_ASSERT_EQ(assert_sends, 1);
	UT_ASSERT_EQ(installs, 0);
}

UT_TEST(test_actual_delivery_tick_fences_drift_active_owner_and_unwind)
{
	int leg;
	volatile bool caught = false;

	for (leg = 1; leg <= 5; leg++) {
		reset_delivery_fixture();
		delivery_drift = leg;
		delivery_terminal = leg >= 3;
		UT_ASSERT_EQ(cluster_gcs_block_resource_x_delivery_tick(&delivery_ref),
					 RESOURCE_X_APPLY_STALE);
		UT_ASSERT_EQ(delivery_completes, 0);
		UT_ASSERT_EQ(delivery_retries, 0);
		UT_ASSERT(delivery_hold);
		UT_ASSERT_EQ(delivery_claims, leg >= 3 ? 1 : 0);
	}
	reset_delivery_fixture();
	delivery_active = true;
	UT_ASSERT_EQ(cluster_gcs_block_resource_x_delivery_tick(&delivery_ref),
				 RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(delivery_claims, 0);
	UT_ASSERT_EQ(delivery_ends, 0);
	UT_ASSERT_EQ(installs, 0);
	UT_ASSERT(delivery_active);

	reset_delivery_fixture();
	delivery_throw = true;
	PG_TRY();
	{
		(void)cluster_gcs_block_resource_x_delivery_tick(&delivery_ref);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(delivery_claims, 1);
	UT_ASSERT_EQ(delivery_ends, 1);
	UT_ASSERT_EQ(leaves, 1);
	UT_ASSERT(!delivery_active && delivery_hold);
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_actual_terminal_ingress_keeps_master_and_physical_source_distinct);
	UT_RUN(test_actual_kind9_ingress_does_not_send_ack_for_fused_admission);
	UT_RUN(test_actual_join_ingress_carries_current_authority_coordinates);
	UT_RUN(test_actual_quiet_tick_installs_or_redrives_without_foreground);
	UT_RUN(test_actual_cleanup_ingress_retains_exact_late_frame_then_quiet_tick_finishes);
	UT_RUN(test_actual_delivery_tick_fences_drift_active_owner_and_unwind);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
