/* Actual GCS terminal ingress with explicit dependency doubles. This proves
 * routing/call boundaries; PCM tests separately execute real T1/T2/T3. */
#include "postgres.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_gcs.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_semantic_activation.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "unit_test.h"

/* These verbatim consumer slices sink only non-authoritative age logging.
 * The real age/reason state machine is exercised in test_cluster_pcm_lock. */
#define gcs_block_resource_x_requester_wait_note(context, reason) ((void)(reason))

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
static int ready_tag_notifications;
static BufferTag ready_tag;
BackendType MyBackendType = B_LMS;
int NBuffers = 2;
static BufferDescPadded delivery_buffers[2];
BufferDescPadded *BufferDescriptors = delivery_buffers;
static bool delivery_enabled, delivery_expired, delivery_active, delivery_ready;
static bool delivery_terminal, delivery_hold, delivery_throw;
static bool session_gap, session_gap_during_claim, session_gap_after_install;
static int delivery_claims, delivery_ends, delivery_completes, delivery_retries, cleanup_joins;
static int delivery_drift;
static uint64 delivery_sequence;
static ResourceXAcquisitionRef delivery_ref;
static uint64 delivery_metrics[PCM_RX_METRIC_COUNT];
static int delivery_trace_callbacks;
static int context_cover_checks;
static ClusterSemanticResourceXPeerOpenResult peer_observation_gap;
static bool peer_gap_during_claim, peer_gap_after_install;
static int authority_mutations;
static PGPROC recycle_proc;
PGPROC *MyProc = &recycle_proc;
static bool recycle_gap_after_arm, recycle_gap_after_context;
static int recycle_arms, recycle_cancels, recycle_finishes;
static ResourceXApplyResult recycle_cancel_result = RESOURCE_X_APPLY_APPLIED;
static bool eviction_admission_closed;
int cluster_gcs_block_retransmit_initial_backoff_ms = 1;
enum { GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_KIND9_REQUEST, GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_KIND9_ACK };

static PcmXSessionAuthResult
gcs_block_resource_x_gate_session_snapshot_result(const BufferTag *tag, ResourceXGateSnapshot *gate,
												  int32 *node, uint64 *session);
static ResourceXApplyResult gcs_block_resource_x_gate_session_recheck_result(
	const BufferTag *tag, const ResourceXGateSnapshot *gate, int32 node, uint64 session);

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

/* Scheduler boundary double only. The real ingress must notify this after
 * releasing its semantic admission; it must not wait for a registry sweep.
 * Separate PCM/ring tests prove the owner and transport mutations. */
static void
gcs_block_resource_x_stage_ready_tag(const BufferTag *tag)
{
	ready_tag_notifications++;
	ready_tag = *tag;
	UT_ASSERT(leaves > 0);
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
	if (eviction_admission_closed) {
		token->entered = false;
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	}
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

bool
cluster_semantic_activation_recheck(const ClusterSemanticAdmissionToken *token)
{
	rechecks++;
	return token->entered && scenario != 10 && !(scenario == 11 && installs > 0);
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	leaves++;
	token->entered = false;
}

uint64
cluster_epoch_get_current(void)
{
	return 0;
}

uint32
cluster_ic_local_capability_word(void)
{
	return PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
}

ClusterSemanticResourceXPeerOpenResult
cluster_semantic_activation_resource_x_peer_open_check(const ClusterSemanticAdmissionToken *token,
													   int32 node, uint32 generation)
{
	peer_checks++;
	if (!token->entered || node < 0 || generation != 61 || scenario == 7
		|| (scenario == 13 && peer_checks == 2))
		return CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_ADMISSION_DRIFT;
	if (peer_observation_gap != CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_NOT_CHECKED
		&& (!peer_gap_during_claim || delivery_active) && (!peer_gap_after_install || installs > 0))
		return peer_observation_gap;
	return CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_MATCH;
}

bool
cluster_semantic_activation_resource_x_peer_open_matches(const ClusterSemanticAdmissionToken *token,
														 int32 node, uint32 generation)
{
	return cluster_semantic_activation_resource_x_peer_open_check(token, node, generation)
		   == CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_MATCH;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_bootstrap_round_discard_pre_assert_authority_drift_exact(
	const ResourceXDecodedFrame *frame pg_attribute_unused(), int32 node pg_attribute_unused(),
	uint64 r4 pg_attribute_unused(), uint32 ingress pg_attribute_unused(),
	uint64 retry pg_attribute_unused(), uint64 deadline pg_attribute_unused(),
	uint64 generation pg_attribute_unused(), uint64 token pg_attribute_unused())
{
	UT_ASSERT(false); /* Unavailable observation cannot discard a binding. */
	return RESOURCE_X_APPLY_STALE;
}

void
cluster_pcm_rx_dispatch_note(bool retry pg_attribute_unused())
{}

ResourceXWriterPath
cluster_resource_x_writer_path_snapshot(uint64 *r4)
{
	*r4 = 77;
	return RESOURCE_X_WRITER_TARGET;
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
	return !session_gap && !(session_gap_during_claim && delivery_active)
		   && !(session_gap_after_install && installs > 0);
}

static bool
gcs_block_resource_x_gate_session_recheck(const BufferTag *tag, const ResourceXGateSnapshot *gate,
										  int32 node, uint64 session)
{
	(void)tag;
	return !session_gap && !(session_gap_during_claim && delivery_active)
		   && !(session_gap_after_install && installs > 0) && scenario != 12
		   && gate->formation == 17 && node == master && session == 31;
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
	context_cover_checks++;
	UT_ASSERT_EQ(ref->acquisition_generation, UINT64_C(41));
	UT_ASSERT_EQ(session, UINT64_C(31));
	UT_ASSERT_EQ(r4, UINT64_C(77));
	UT_ASSERT_EQ(generation, UINT64_C(8));
	if (recycle_gap_after_context)
		session_gap = true;
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

int
cluster_gcs_lookup_master(BufferTag tag pg_attribute_unused())
{
	return master;
}

bool
cluster_pcm_lock_resource_x_gate_snapshot(ResourceXGateSnapshot *gate)
{
	memset(gate, 0, sizeof(*gate));
	gate->formation = 17;
	gate->phase = RESOURCE_X_GATE_OPEN;
	return true;
}

static PcmXSessionAuthResult
gcs_block_pcm_x_authenticated_session_result(int32 node pg_attribute_unused(),
											 uint64 epoch pg_attribute_unused(), uint64 *session,
											 ClusterGcsPcmXAuthSample *sample)
{
	memset(sample, 0, sizeof(*sample));
	*session = session_gap ? 0 : 31;
	return session_gap ? PCM_X_SESSION_AUTH_FRESH_NOT_READY : PCM_X_SESSION_AUTH_OK;
}

int
errdetail_log(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

static ResourceXApplyResult
gcs_block_pcm_x_resource_x_source_block_to_n(
	const ResourceXDecodedFrame *frame pg_attribute_unused(), int32 node pg_attribute_unused(),
	uint64 r4 pg_attribute_unused())
{
	authority_mutations++;
	return RESOURCE_X_APPLY_APPLIED;
}

static ResourceXApplyResult
gcs_block_pcm_x_resource_x_remote_s_holder_block_to_n(
	const ResourceXDecodedFrame *frame, int32 node, uint32 ingress pg_attribute_unused(), uint64 r4,
	const ClusterSemanticAdmissionToken *admission pg_attribute_unused())
{
	return gcs_block_pcm_x_resource_x_source_block_to_n(frame, node, r4);
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_block_to_n_exact(const ResourceXDecodedFrame *frame, int32 node)
{
	return gcs_block_pcm_x_resource_x_source_block_to_n(frame, node, 77);
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_assert_bootstrapped_exact(
	const ResourceXDecodedFrame *frame, int32 node, uint32 ingress pg_attribute_unused(), uint64 r4,
	uint64 session pg_attribute_unused(), uint32 outbound pg_attribute_unused(),
	ResourceXMasterSnapshot *out pg_attribute_unused())
{
	return gcs_block_pcm_x_resource_x_source_block_to_n(frame, node, r4);
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_itl_recycle_begin_exact(const ResourceXAcquisitionRef *ref,
													uint64 session, uint64 r4, uint64 generation,
													uint64 token, int32 proc pg_attribute_unused(),
													uint64 now pg_attribute_unused(),
													ResourceXLocalOwnerHandle *handle)
{
	recycle_arms++;
	memset(handle, 0, sizeof(*handle));
	handle->ref = *ref;
	handle->master_session_incarnation = session;
	handle->r4_record_generation = r4;
	handle->buffer_ownership_generation = generation;
	handle->reservation_token = token;
	handle->owner_generation = 19;
	if (recycle_gap_after_arm)
		session_gap = true;
	return RESOURCE_X_APPLY_APPLIED;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_itl_recycle_cancel_exact(const ResourceXLocalOwnerHandle *handle)
{
	UT_ASSERT_EQ(handle->owner_generation, 19);
	UT_ASSERT_EQ(handle->ref.acquisition_generation, 41);
	recycle_cancels++;
	return recycle_cancel_result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_itl_recycle_finish_exact(const ResourceXLocalOwnerHandle *handle,
													 uint64 now pg_attribute_unused())
{
	UT_ASSERT_EQ(handle->owner_generation, 19);
	recycle_finishes++;
	return RESOURCE_X_APPLY_APPLIED;
}

#include "test_cluster_resource_x_terminal_owner.inc"

/* The exact foreground terminal predicate is compiled below. Only its two
 * observation dependencies are controlled. This is not a complete live
 * transport replay and does not replace the physical T1/T2/T3 tests. */
static PcmXSessionAuthResult terminal_pending_kind;
static int terminal_pending_remaining;
static int terminal_sample_calls;
static int terminal_identity_conflict;
static int terminal_pause_calls;
static bool pause_clears_gap;

static void
gcs_block_resource_x_observation_pause(void)
{
	terminal_pause_calls++;
	if (pause_clears_gap)
		session_gap = false;
}

static PcmXSessionAuthResult
gcs_block_resource_x_gate_session_snapshot_result(const BufferTag *tag, ResourceXGateSnapshot *gate,
												  int32 *node, uint64 *session)
{
	ClusterGcsPcmXAuthSample sample = { .session_before = 31,
										.session_after = 31,
										.slot_generation_before = 5,
										.slot_generation_after = 5,
										.observed_epoch_before = 2,
										.observed_epoch_after = 2,
										.connection_generation_before = 61,
										.connection_generation_after = 61,
										.connection_before_valid = true,
										.connection_after_valid = true,
										.slot_before_valid = true,
										.slot_after_valid = true,
										.fresh_before = true,
										.fresh_after = true };
	PcmXSessionAuthResult result;

	terminal_sample_calls++;
	(void)gcs_block_resource_x_gate_session_snapshot(tag, gate, node, session);
	/* The former bool-recheck refusal represents a real namespace change,
	 * not an unavailable sample. Supply that contradiction to the new typed
	 * consumer at the same post-install / post-peer-check boundary. */
	if (scenario == 12 && (installs > 0 || peer_checks > 1))
		gate->freeze_generation++;
	*session = 0;
	if (session_gap || (session_gap_during_claim && delivery_active)
		|| (session_gap_after_install && installs > 0))
		sample.fresh_before = sample.fresh_after = false;
	if (terminal_pending_remaining > 0) {
		terminal_pending_remaining--;
		switch (terminal_pending_kind) {
		case PCM_X_SESSION_AUTH_CONNECTION_NOT_READY:
			sample.connection_before_valid = false;
			break;
		case PCM_X_SESSION_AUTH_SLOT_NOT_READY:
			sample.slot_before_valid = false;
			break;
		case PCM_X_SESSION_AUTH_EPOCH_NOT_READY:
			sample.observed_epoch_before = sample.observed_epoch_after = 0;
			break;
		case PCM_X_SESSION_AUTH_FRESH_NOT_READY:
			sample.fresh_before = sample.fresh_after = false;
			break;
		case PCM_X_SESSION_AUTH_SLOT_TORN:
			sample.slot_generation_after++;
			break;
		case PCM_X_SESSION_AUTH_EPOCH_TORN:
			sample.observed_epoch_after++;
			break;
		case PCM_X_SESSION_AUTH_CONNECTION_TORN:
			sample.connection_generation_after++;
			break;
		default:
			return PCM_X_SESSION_AUTH_INVALID;
		}
	}
	result = cluster_gcs_pcm_x_auth_sample_classify(&sample, 2);
	if (result != PCM_X_SESSION_AUTH_OK)
		return result;
	*session = sample.session_before;
	if (terminal_identity_conflict == 1)
		gate->freeze_generation++;
	else if (terminal_identity_conflict == 2)
		(*node)++;
	else if (terminal_identity_conflict == 3)
		(*session)++;
	return PCM_X_SESSION_AUTH_OK;
}

#include "test_cluster_resource_x_session_observation.inc"

static bool remote_s_cap_valid = true, remote_s_gate_open = true;
static uint32 remote_s_cap_generation = 61;
static int remote_s_cancels, remote_s_aborts, remote_s_fuses, remote_s_rollback_failure;

bool
cluster_sf_peer_capability_record_snapshot(int32 peer, ClusterSfPeerCap *out)
{
	UT_ASSERT_EQ(peer, 1);
	memset(out, 0, sizeof(*out));
	out->valid = remote_s_cap_valid;
	out->generation = remote_s_cap_generation;
	out->bits = PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
	return true;
}

bool
cluster_sf_peer_capability_generation_matches(int32 peer, uint32 bits, uint32 generation)
{
	UT_ASSERT_EQ(peer, 1);
	UT_ASSERT_EQ(bits, PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1);
	return remote_s_cap_valid && generation == remote_s_cap_generation;
}

bool
cluster_pcm_lock_resource_x_gate_open_exact(uint64 formation)
{
	return remote_s_gate_open && formation == 17;
}

#include "test_cluster_resource_x_remote_s_types.inc"

static ClusterPcmOwnResult
remote_s_cancel(const ClusterLmsRemoteSStatusHandle *handle)
{
	UT_ASSERT_EQ(handle->slot_cookie, 19);
	UT_ASSERT_EQ(handle->own_generation, 7);
	UT_ASSERT_EQ(handle->reservation_token, 9);
	UT_ASSERT_EQ(remote_s_aborts, 0);
	remote_s_cancels++;
	return remote_s_rollback_failure == 1 ? CLUSTER_PCM_OWN_STALE : CLUSTER_PCM_OWN_OK;
}

static ClusterPcmOwnResult
remote_s_abort(BufferDesc *buf, const ClusterPcmOwnSnapshot *revoking)
{
	UT_ASSERT(buf == GetBufferDescriptor(0));
	UT_ASSERT_EQ(revoking->generation, 7);
	UT_ASSERT_EQ(revoking->reservation_token, 9);
	UT_ASSERT_EQ(remote_s_cancels, 1);
	remote_s_aborts++;
	return remote_s_rollback_failure == 2 ? CLUSTER_PCM_OWN_STALE : CLUSTER_PCM_OWN_OK;
}

static ResourceXApplyResult
run_actual_remote_s_staged_gate(void)
{
	ClusterSemanticAdmissionToken token = { .entered = true, .record_generation = 77 };
	const ClusterSemanticAdmissionToken *admission pg_attribute_unused() = &token;
	ResourceXDecodedFrame request = { 0 };
	const ResourceXDecodedFrame *block = &request;
	int32 authenticated_master_node = 1;
	uint32 authenticated_capability_generation = 61;
	uint64 r4_record_generation = 77;
	BufferDesc *buf = GetBufferDescriptor(0);
	ClusterPcmOwnSnapshot revoking = { .generation = 7, .reservation_token = 9 };
	ClusterPcmOwnSnapshot current = { .generation = 6 };
	ClusterLmsRemoteSStatusHandle status_handle
		= { .slot_cookie = 19, .own_generation = 7, .reservation_token = 9 };
	ClusterPcmOwnResult cancel_result, abort_result;
	bool rollback_cancel_ok = false, rollback_abort_ok = false;
	ResourceXRemoteSStage remote_s_stage = RESOURCE_X_REMOTE_S_STAGE_PENDING_STAGED;
	ResourceXRemoteSFailureDecision failure_decision;
	ResourceXFirstFailureEvidence first_failure;
	ResourceXApplyResult mapped_result pg_attribute_unused();
	ResourceXFailureDomain failure_domain pg_attribute_unused();

	request.common.logical_assertion = delivery_ref.assertion;
	request.common.master_session_incarnation = 31;
	request.common.resource_formation = 17;
#define cluster_lms_outbound_cancel_resource_x_remote_s_status_exact remote_s_cancel
#define cluster_bufmgr_pcm_own_abort_s_revoke remote_s_abort
#define gcs_block_resource_x_first_failure_from_block(evidence, ...)                               \
	memset(evidence, 0, sizeof(*(evidence)))
#define gcs_block_resource_x_first_failure_record(evidence) ((void)(evidence))
#define gcs_block_resource_x_failure_decision_apply(decision)                                      \
	(remote_s_fuses += (decision)->global_fail_closed ? 1 : 0)
#include "test_cluster_resource_x_remote_s_staged_gate.inc"
#undef gcs_block_resource_x_failure_decision_apply
#undef gcs_block_resource_x_first_failure_record
#undef gcs_block_resource_x_first_failure_from_block
#undef cluster_bufmgr_pcm_own_abort_s_revoke
#undef cluster_lms_outbound_cancel_resource_x_remote_s_status_exact
	return RESOURCE_X_APPLY_APPLIED; /* Reached the unchanged physical-finish branch. */
}

static ResourceXApplyResult
run_actual_terminal_gate_consumer(bool *reached_export)
{
	ClusterSemanticAdmissionToken admission = { 0 };
	BufferTag resource = { 0 };
	ResourceXGateSnapshot gate = { 0 }, terminal_gate;
	ResourceXApplyResult result = RESOURCE_X_APPLY_INVALID;
	PcmXSessionAuthResult terminal_session_check = PCM_X_SESSION_AUTH_INVALID;
	bool terminal_admission_current = false, terminal_gate_session_current = false;
	int32 master_node = 1, terminal_master_node = -1;
	uint64 master_session = 31, terminal_master_session = 0;
	int iteration;

	admission.entered = true;
	gate.formation = 17;
	*reached_export = false;
	for (iteration = 0; iteration < 8; iteration++) {
#include "test_cluster_resource_x_terminal_gate_consumer.inc"
		/* The production predicate must fall through before export is legal.
		 * No test replacement of that predicate decides this branch. */
		*reached_export = true;
		return RESOURCE_X_APPLY_APPLIED;
	}
	return result;
}

UT_TEST(test_actual_terminal_gate_reobserves_missing_and_torn_session)
{
	static const PcmXSessionAuthResult pending[] = { PCM_X_SESSION_AUTH_CONNECTION_NOT_READY,
													 PCM_X_SESSION_AUTH_SLOT_NOT_READY,
													 PCM_X_SESSION_AUTH_EPOCH_NOT_READY,
													 PCM_X_SESSION_AUTH_FRESH_NOT_READY,
													 PCM_X_SESSION_AUTH_SLOT_TORN,
													 PCM_X_SESSION_AUTH_EPOCH_TORN,
													 PCM_X_SESSION_AUTH_CONNECTION_TORN };
	int leg;

	master = 1;
	scenario = 0;
	terminal_identity_conflict = 0;
	for (leg = 0; leg < lengthof(pending); leg++) {
		bool exported;

		terminal_pending_kind = pending[leg];
		terminal_pending_remaining = 3;
		terminal_sample_calls = 0;
		terminal_pause_calls = 0;
		UT_ASSERT_EQ(run_actual_terminal_gate_consumer(&exported), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(exported);
		UT_ASSERT_EQ(terminal_sample_calls, 4);
		UT_ASSERT_EQ(terminal_pending_remaining, 0);
		UT_ASSERT_EQ(terminal_pause_calls, 3);
	}
}

UT_TEST(test_actual_terminal_gate_rejects_proved_identity_or_admission_change)
{
	int leg;

	master = 1;
	terminal_pending_remaining = 0;
	for (leg = 1; leg <= 4; leg++) {
		bool exported;

		terminal_identity_conflict = leg;
		scenario = leg == 4 ? 10 : 0;
		terminal_sample_calls = 0;
		terminal_pause_calls = 0;
		UT_ASSERT_EQ(run_actual_terminal_gate_consumer(&exported), RESOURCE_X_APPLY_STALE);
		UT_ASSERT(!exported);
		UT_ASSERT_EQ(terminal_sample_calls, leg == 4 ? 0 : 1);
		UT_ASSERT_EQ(terminal_pause_calls, 0);
	}
	terminal_identity_conflict = 0;
	scenario = 0;
}

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

UT_TEST(test_actual_fused_admission_notifies_ready_resource_without_registry_tick)
{
	ClusterICEnvelope env = { 0 };
	ResourceXDecodedFrame frame = { 0 };

	scenario = 0;
	master = cluster_node_id = 0;
	peer_checks = rechecks = leaves = kind9_requests = ack_sends = assert_sends = 0;
	ready_tag_notifications = 0;
	fused_reply = true;
	env.source_node_id = 1;
	env.msg_type = RESOURCE_X_MSG_ASSERT_X;
	frame.kind = RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP;
	frame.common.flags = RESOURCE_X_COMMON_FLAG_REMOTE_ADMISSION;
	frame.common.resource_formation = 17;
	frame.common.master_session_incarnation = 31;
	frame.common.logical_assertion.resource.blockNum = 761;
	gcs_block_resource_x_kind9_ingress(&env, &frame, 61);
	UT_ASSERT_EQ(kind9_requests, 1);
	UT_ASSERT_EQ(ack_sends, 0);
	UT_ASSERT_EQ(ready_tag_notifications, 1);
	UT_ASSERT(BufferTagsEqual(&ready_tag, &frame.common.logical_assertion.resource));
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
		/* These two boundaries require two actual remote peer checks. */
		if (scenario == 12 || scenario == 13)
			master = 2;
		peer_checks = rechecks = leaves = joins = installs = publishes = 0;
		env.source_node_id = 1;
		frame.kind = RESOURCE_X_WIRE_IMAGE_ENVELOPE;
		frame.common.flags = RESOURCE_X_COMMON_FLAG_AUTHORITY_WITH_IMAGE;
		frame.common.resource_formation = 17;
		frame.common.master_session_incarnation = scenario == 8 ? 32 : 31;
		UT_ASSERT_EQ(gcs_block_resource_x_requester_join_ingress(&env, &frame, 61, &out),
					 test_case < 3	  ? RESOURCE_X_APPLY_APPLIED
					 : scenario == 14 ? RESOURCE_X_APPLY_BAD_STATE
									  : RESOURCE_X_APPLY_STALE);
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
	session_gap = session_gap_during_claim = session_gap_after_install = false;
	peer_observation_gap = CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_NOT_CHECKED;
	peer_gap_during_claim = peer_gap_after_install = false;
	delivery_sequence = 0;
	ready_tag_notifications = 0;
	installs = publishes = leaves = joins = peer_checks = rechecks = assert_sends = 0;
	kind9_requests = ack_sends = 0;
	authority_mutations = 0;
	recycle_gap_after_arm = recycle_gap_after_context = false;
	recycle_arms = recycle_cancels = recycle_finishes = 0;
	recycle_cancel_result = RESOURCE_X_APPLY_APPLIED;
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
		UT_ASSERT_EQ(ready_tag_notifications, leg == 0 ? 0 : 1);
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
		UT_ASSERT_EQ(ready_tag_notifications, 0);
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
	UT_ASSERT_EQ(ready_tag_notifications, 0); /* Never stage from ERROR FINALLY. */
}

UT_TEST(test_actual_delivery_observation_gap_retains_owner_then_finishes)
{
	int leg;

	for (leg = 0; leg < 2; leg++) {
		reset_delivery_fixture();
		delivery_ready = true;
		session_gap = leg == 0;
		session_gap_during_claim = leg == 1;
		UT_ASSERT_EQ(cluster_gcs_block_resource_x_delivery_tick(&delivery_ref),
					 RESOURCE_X_APPLY_BAD_STATE);
		UT_ASSERT_EQ(delivery_claims, leg);
		UT_ASSERT_EQ(delivery_ends, leg);
		UT_ASSERT(!delivery_active && delivery_hold);
		UT_ASSERT_EQ(installs, 0);
		UT_ASSERT_EQ(delivery_completes, 0);
		UT_ASSERT_EQ(delivery_retries, 0);
		UT_ASSERT_EQ(ready_tag_notifications, 0);
		session_gap = session_gap_during_claim = false;
		UT_ASSERT_EQ(cluster_gcs_block_resource_x_delivery_tick(&delivery_ref),
					 RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(delivery_claims, leg + 1);
		UT_ASSERT_EQ(delivery_ends, leg + 1);
		UT_ASSERT(!delivery_active && !delivery_hold);
		UT_ASSERT_EQ(delivery_completes, 1);
		UT_ASSERT_EQ(installs, 1);
	}
}

UT_TEST(test_actual_image_ingress_yields_before_join_and_after_t3)
{
	int leg;

	for (leg = 0; leg < 3; leg++) {
		ClusterICEnvelope env = { 0 };
		ResourceXDecodedFrame frame = { 0 };
		ResourceXRequesterJoinSnapshot out;

		reset_delivery_fixture();
		delivery_enabled = false;
		env.source_node_id = 1;
		frame.kind = RESOURCE_X_WIRE_IMAGE_ENVELOPE;
		frame.common.logical_assertion = delivery_ref.assertion;
		frame.common.resource_formation = 17;
		frame.common.assertion_sequence = 41;
		frame.common.master_session_incarnation = 31;
		frame.common.flags = RESOURCE_X_COMMON_FLAG_AUTHORITY_WITH_IMAGE;
		session_gap = leg < 2;
		session_gap_after_install = leg == 2;
		if (leg == 0) {
			UT_ASSERT_EQ(gcs_block_resource_x_requester_join_ingress(&env, &frame, 61, &out),
						 RESOURCE_X_APPLY_BAD_STATE);
			UT_ASSERT_EQ(joins, 0);
		} else {
			UT_ASSERT_EQ(gcs_block_resource_x_requester_terminal_try(&env, &frame, 61, false),
						 RESOURCE_X_APPLY_BAD_STATE);
			UT_ASSERT_EQ(installs, leg == 2 ? 1 : 0);
		}
		UT_ASSERT_EQ(publishes, 0);
		UT_ASSERT_EQ(leaves, 1);
		UT_ASSERT(delivery_hold);
		UT_ASSERT_EQ(delivery_completes, 0);
		session_gap = session_gap_after_install = false;
		if (leg == 0) {
			UT_ASSERT_EQ(gcs_block_resource_x_requester_join_ingress(&env, &frame, 61, &out),
						 RESOURCE_X_APPLY_APPLIED);
			UT_ASSERT_EQ(joins, 1);
		} else {
			UT_ASSERT_EQ(gcs_block_resource_x_requester_terminal_try(&env, &frame, 61, true),
						 RESOURCE_X_APPLY_APPLIED);
			UT_ASSERT_EQ(publishes, 1);
		}
		UT_ASSERT_EQ(leaves, 2);
	}
}

static int source_final_fuses, source_final_rollbacks, source_final_continues;

UT_TEST(test_actual_itl_recycle_pending_keeps_exact_cancel_and_no_finish_permission)
{
	for (int leg = 0; leg < 4; leg++) {
		ResourceXWriterUseContext context = { 0 };
		ClusterPcmOwnSnapshot observed = { 0 };
		ResourceXLocalOwnerHandle handle = { 0 }, before;

		reset_delivery_fixture();
		master = 1;
		delivery_terminal = true;
		context.ref = delivery_ref;
		context.r4_record_generation = 77;
		context.buffer_ownership_generation = 8;
		observed.tag = delivery_ref.assertion.resource;
		observed.pcm_state = PCM_STATE_X;
		observed.generation = 8;
		observed.reservation_token = 9;
		if (leg >= 2)
			UT_ASSERT_EQ(
				cluster_gcs_resource_x_target_itl_recycle_begin_exact(&context, &observed, &handle),
				RESOURCE_X_APPLY_APPLIED);
		before = handle;
		session_gap = leg == 0 || leg == 2;
		recycle_gap_after_arm = leg == 1;
		recycle_gap_after_context = leg == 3;
		UT_ASSERT_EQ(leg < 2 ? cluster_gcs_resource_x_target_itl_recycle_begin_exact(
								   &context, &observed, &handle)
							 : cluster_gcs_resource_x_target_itl_recycle_finish_exact(
								   &context, &observed, &handle),
					 RESOURCE_X_APPLY_BAD_STATE);
		UT_ASSERT_EQ(recycle_arms, leg == 0 ? 0 : 1);
		UT_ASSERT_EQ(recycle_cancels, leg == 1 ? 1 : 0);
		UT_ASSERT_EQ(recycle_finishes, 0);
		if (leg < 2)
			UT_ASSERT_EQ(handle.owner_generation, 0);
		else
			UT_ASSERT_EQ(memcmp(&handle, &before, sizeof(handle)), 0);
		session_gap = recycle_gap_after_arm = recycle_gap_after_context = false;
		if (leg < 2)
			UT_ASSERT_EQ(
				cluster_gcs_resource_x_target_itl_recycle_begin_exact(&context, &observed, &handle),
				RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(
			cluster_gcs_resource_x_target_itl_recycle_finish_exact(&context, &observed, &handle),
			RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(recycle_finishes, 1);
		/* A different exact handle is not rescued by a temporarily missing session. */
		handle.ref.acquisition_generation++;
		session_gap = true;
		UT_ASSERT_EQ(
			cluster_gcs_resource_x_target_itl_recycle_finish_exact(&context, &observed, &handle),
			RESOURCE_X_APPLY_STALE);
		UT_ASSERT_EQ(recycle_finishes, 1);
	}
	reset_delivery_fixture();
}

UT_TEST(test_actual_remote_s_observation_gap_cancels_exact_status_and_revoke)
{
	for (int cause = 0; cause < 2; cause++) {
		for (int rollback_failure = 0; rollback_failure < 3; rollback_failure++) {
			reset_delivery_fixture();
			master = 1;
			session_gap = cause == 0;
			remote_s_cap_valid = cause != 1;
			remote_s_cap_generation = 61;
			remote_s_gate_open = true;
			remote_s_cancels = remote_s_aborts = remote_s_fuses = 0;
			remote_s_rollback_failure = rollback_failure;
			UT_ASSERT_EQ(run_actual_remote_s_staged_gate(),
						 rollback_failure == 0 ? RESOURCE_X_APPLY_BAD_STATE
											   : RESOURCE_X_APPLY_RECOVERY_BLOCKED);
			UT_ASSERT_EQ(remote_s_cancels, 1);
			UT_ASSERT_EQ(remote_s_aborts, 1);
			UT_ASSERT_EQ(remote_s_fuses, rollback_failure == 0 ? 0 : 1);
		}
	}
	reset_delivery_fixture();
	master = 1;
	remote_s_cap_valid = remote_s_gate_open = true;
	remote_s_rollback_failure = 0;
	remote_s_cap_generation = 62;
	remote_s_cancels = remote_s_aborts = remote_s_fuses = 0;
	UT_ASSERT_EQ(run_actual_remote_s_staged_gate(), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(remote_s_cancels, 1);
	UT_ASSERT_EQ(remote_s_aborts, 1);
	remote_s_cap_generation = 61;
	remote_s_cancels = remote_s_aborts = 0;
	UT_ASSERT_EQ(run_actual_remote_s_staged_gate(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(remote_s_cancels, 0);
	UT_ASSERT_EQ(remote_s_aborts, 0);
	reset_delivery_fixture();
}

UT_TEST(test_actual_type17_and_assert_admission_keep_unproven_work_pending)
{
	for (int role = 0; role < 2; role++) {
		for (int cause = 0; cause < 3; cause++) {
			ClusterICEnvelope env = { .source_node_id = 1 };
			ResourceXDecodedFrame frame = { 0 };
			ResourceXMasterSnapshot out;

			reset_delivery_fixture();
			master = role == 0 ? 1 : cluster_node_id;
			frame.kind = role == 0 ? RESOURCE_X_WIRE_BLOCK_TO_N : RESOURCE_X_WIRE_ASSERT_X;
			frame.common.logical_assertion = delivery_ref.assertion;
			frame.common.resource_formation = 17;
			frame.common.master_session_incarnation = 31;
			frame.common.observed_mode = PCM_STATE_X;
			frame.common.source_candidate = frame.common.retain_pi_if_dirty = 1;
			session_gap = cause == 0;
			peer_observation_gap
				= cause == 1 ? CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_CAPABILITY_NOT_READY : 0;
			scenario = cause == 2 && role == 1 ? 14 : 0;
			/* Type17 has an authenticated inbound connection and needs no
			 * separate outbound sample in this admission boundary. */
			if (cause == 2 && role == 0)
				continue;
			UT_ASSERT_EQ(role == 0 ? gcs_block_resource_x_type17_ingress(&env, &frame, 61)
								   : gcs_block_resource_x_bootstrapped_assert_ingress(&env, &frame,
																					  61, &out),
						 RESOURCE_X_APPLY_BAD_STATE);
			UT_ASSERT_EQ(authority_mutations, 0);
			UT_ASSERT_EQ(leaves, 1);
			session_gap = false;
			peer_observation_gap = 0;
			scenario = 0;
			UT_ASSERT_EQ(role == 0 ? gcs_block_resource_x_type17_ingress(&env, &frame, 61)
								   : gcs_block_resource_x_bootstrapped_assert_ingress(&env, &frame,
																					  61, &out),
						 RESOURCE_X_APPLY_APPLIED);
			UT_ASSERT_EQ(authority_mutations, 1);
			/* A coherent wrong session remains rejected before canonical mutation. */
			frame.common.master_session_incarnation++;
			UT_ASSERT_EQ(role == 0 ? gcs_block_resource_x_type17_ingress(&env, &frame, 61)
								   : gcs_block_resource_x_bootstrapped_assert_ingress(&env, &frame,
																					  61, &out),
						 RESOURCE_X_APPLY_STALE);
			UT_ASSERT_EQ(authority_mutations, 1);
		}
	}
	reset_delivery_fixture();
}

UT_TEST(test_actual_peer_observation_gap_keeps_delivery_and_ingress_owned)
{
	const ClusterSemanticResourceXPeerOpenResult pending[]
		= { CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_AUTHORITY_UNAVAILABLE,
			CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_CAPABILITY_NOT_READY,
			CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_TABLE_TORN,
			CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_IMAGE_OBSERVATION_PENDING };

	for (int reason = 0; reason < lengthof(pending); reason++) {
		for (int leg = 0; leg < 4; leg++) {
			ClusterICEnvelope env = { .source_node_id = 1 };
			ResourceXDecodedFrame frame = { 0 };
			ResourceXRequesterJoinSnapshot joined;

			reset_delivery_fixture();
			delivery_ready = true;
			peer_observation_gap = pending[reason];
			peer_gap_during_claim = leg == 1;
			peer_gap_after_install = leg == 2;
			frame.kind = RESOURCE_X_WIRE_IMAGE_ENVELOPE;
			frame.common.logical_assertion = delivery_ref.assertion;
			frame.common.resource_formation = 17;
			frame.common.assertion_sequence = 41;
			frame.common.master_session_incarnation = 31;
			frame.common.flags = RESOURCE_X_COMMON_FLAG_AUTHORITY_WITH_IMAGE;
			if (leg < 2) {
				UT_ASSERT_EQ(cluster_gcs_block_resource_x_delivery_tick(&delivery_ref),
							 RESOURCE_X_APPLY_BAD_STATE);
				UT_ASSERT_EQ(delivery_claims, leg);
				UT_ASSERT_EQ(delivery_ends, leg);
			} else {
				delivery_enabled = false;
				if (leg == 2)
					UT_ASSERT_EQ(
						gcs_block_resource_x_requester_terminal_try(&env, &frame, 61, false),
						RESOURCE_X_APPLY_BAD_STATE);
				else
					UT_ASSERT_EQ(
						gcs_block_resource_x_requester_join_ingress(&env, &frame, 61, &joined),
						RESOURCE_X_APPLY_BAD_STATE);
			}
			UT_ASSERT_EQ(installs, leg == 2 ? 1 : 0);
			UT_ASSERT_EQ(publishes, 0);
			UT_ASSERT(delivery_hold && !delivery_active);
			UT_ASSERT_EQ(delivery_completes, 0);
			peer_observation_gap = CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_NOT_CHECKED;
			if (leg < 2) {
				UT_ASSERT_EQ(cluster_gcs_block_resource_x_delivery_tick(&delivery_ref),
							 RESOURCE_X_APPLY_APPLIED);
				UT_ASSERT_EQ(delivery_completes, 1);
				UT_ASSERT(!delivery_hold);
			}
		}
	}
	reset_delivery_fixture();
}

static ResourceXApplyResult
run_actual_source_final_gate(bool semantic_retained)
{
	ResourceXDecodedFrame frame = { 0 };
	ResourceXDecodedFrame *block = &frame;
	ResourceXGateSnapshot resource_gate = { 0 };
	ResourceXApplyResult failure_result = RESOURCE_X_APPLY_INVALID;
	ResourceXApplyResult gate_result;
	int resource_master_node = 0;
	uint64 resource_master_session = 31;
	const char *failure_stage;

	resource_gate.formation = 17;
#define gcs_block_resource_x_fail_closed_current() (source_final_fuses++)
#include "test_cluster_resource_x_source_final_gate.inc"
#undef gcs_block_resource_x_fail_closed_current
	source_final_continues++;
	return RESOURCE_X_APPLY_APPLIED;

pre_retained_failure:
	UT_ASSERT(strcmp(failure_stage, "source-final-gate") == 0);
	UT_ASSERT(!semantic_retained);
	source_final_rollbacks++;
	return failure_result;
}

UT_TEST(test_actual_source_gate_never_rolls_back_armed_pair_for_sample_gap)
{
	int retained;

	for (retained = 0; retained <= 1; retained++) {
		reset_delivery_fixture();
		source_final_fuses = source_final_rollbacks = source_final_continues = 0;
		session_gap = true;
		UT_ASSERT_EQ(run_actual_source_final_gate(retained != 0), RESOURCE_X_APPLY_BAD_STATE);
		UT_ASSERT_EQ(source_final_fuses, 0);
		UT_ASSERT_EQ(source_final_continues, 0);
		UT_ASSERT_EQ(source_final_rollbacks, retained ? 0 : 1);
		session_gap = false;
		UT_ASSERT_EQ(run_actual_source_final_gate(retained != 0), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(source_final_continues, 1);
		/* A complete gate contradiction retains the original hard behavior. */
		scenario = 12;
		installs = 1;
		UT_ASSERT_EQ(run_actual_source_final_gate(retained != 0),
					 retained ? RESOURCE_X_APPLY_RECOVERY_BLOCKED : RESOURCE_X_APPLY_STALE);
		UT_ASSERT_EQ(source_final_fuses, retained ? 1 : 0);
		UT_ASSERT_EQ(source_final_rollbacks, retained ? 0 : 2);
	}
	scenario = 0;
}

static ResourceXApplyResult
run_actual_source_before_mutation_gate(bool at_entry)
{
	ResourceXDecodedFrame frame = { 0 };
	ResourceXDecodedFrame *block = &frame;
	ResourceXGateSnapshot resource_gate = { 0 };
	ResourceXApplyResult failure_result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	ResourceXApplyResult gate_result pg_attribute_unused();
	PcmXSessionAuthResult session_result pg_attribute_unused();
	int resource_master_node = 0, authenticated_master_node = 0;
	uint64 resource_master_session = 31;
	const char *failure_stage = "entry";

	resource_gate.formation = frame.common.resource_formation = 17;
	frame.common.master_session_incarnation = 31;
	if (at_entry) {
#include "test_cluster_resource_x_source_entry_gate.inc"
	} else {
#include "test_cluster_resource_x_source_prepare_gate.inc"
	}
	return RESOURCE_X_APPLY_APPLIED;
pre_retained_failure:
	UT_ASSERT(strcmp(failure_stage, "gate-session") == 0
			  || strcmp(failure_stage, "source-gate-recheck") == 0);
	return failure_result;
}

UT_TEST(test_actual_source_before_mutation_waits_without_exporting_session)
{
	int phase, kind;

	for (phase = 0; phase < 2; phase++) {
		for (kind = PCM_X_SESSION_AUTH_CONNECTION_NOT_READY;
			 kind <= PCM_X_SESSION_AUTH_CONNECTION_TORN; kind++) {
			reset_delivery_fixture();
			session_gap = true; /* Also drives the old bool snapshot for RED. */
			terminal_pending_kind = (PcmXSessionAuthResult)kind;
			terminal_pending_remaining = 1;
			UT_ASSERT_EQ(run_actual_source_before_mutation_gate(phase == 0),
						 RESOURCE_X_APPLY_BAD_STATE);
			/* Even while unavailable, the independently observed master may
			 * contradict the bound request and must win over pending. */
			master = 1;
			terminal_pending_remaining = 1;
			UT_ASSERT_EQ(run_actual_source_before_mutation_gate(phase == 0),
						 RESOURCE_X_APPLY_STALE);
			master = 0;
			session_gap = false;
			terminal_pending_remaining = 0;
			UT_ASSERT_EQ(run_actual_source_before_mutation_gate(phase == 0),
						 RESOURCE_X_APPLY_APPLIED);
			UT_ASSERT_EQ(installs, 0);
			UT_ASSERT_EQ(publishes, 0);
			UT_ASSERT(delivery_hold);
		}
	}
}

static ResourceXApplyResult
run_actual_foreground_wait_gate(int phase, bool *reached_wait)
{
	ClusterSemanticAdmissionToken admission = { .entered = true };
	BufferTag resource = { 0 };
	ResourceXGateSnapshot gate = { 0 };
	ResourceXApplyResult result = RESOURCE_X_APPLY_INVALID;
	ResourceXApplyResult wait_result pg_attribute_unused();
	int32 master_node = 0;
	uint64 master_session = 31;
	bool target_retained_release_post_mutation = phase == 1;
	const char *diagnostic_stage pg_attribute_unused();
	int iteration;

	gate.formation = 17;
	*reached_wait = false;
	for (iteration = 0; iteration < 4; iteration++) {
		if (phase < 2) {
#define gcs_block_resource_x_fail_closed_current() (source_final_fuses++)
#include "test_cluster_resource_x_retained_wait_gate.inc"
#undef gcs_block_resource_x_fail_closed_current
		} else {
#include "test_cluster_resource_x_predecessor_wait_gate.inc"
		}
		*reached_wait = true;
		return RESOURCE_X_APPLY_APPLIED;
	}
	return result;
}

UT_TEST(test_actual_foreground_retained_and_predecessor_wait_reobserve_without_fuse)
{
	int phase;

	for (phase = 0; phase < 3; phase++) {
		bool reached_wait;

		reset_delivery_fixture();
		source_final_fuses = terminal_pause_calls = 0;
		pause_clears_gap = true;
		session_gap = true;
		UT_ASSERT_EQ(run_actual_foreground_wait_gate(phase, &reached_wait),
					 RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(reached_wait);
		UT_ASSERT_EQ(terminal_pause_calls, 1);
		UT_ASSERT_EQ(source_final_fuses, 0);
		UT_ASSERT_EQ(installs, 0);
		UT_ASSERT_EQ(publishes, 0);
		/* Independently revoked admission still terminates the old wait. */
		scenario = 10;
		UT_ASSERT_EQ(run_actual_foreground_wait_gate(phase, &reached_wait),
					 phase == 1 ? RESOURCE_X_APPLY_RECOVERY_BLOCKED : RESOURCE_X_APPLY_STALE);
		UT_ASSERT(!reached_wait);
		UT_ASSERT_EQ(source_final_fuses, phase == 1 ? 1 : 0);
	}
	pause_clears_gap = false;
	session_gap = false;
	scenario = 0;
}

static ResourceXApplyResult
run_actual_dispatch_observation(ResourceXBootstrapRoundAction action, bool join_only)
{
	ClusterSemanticAdmissionToken admission = { .entered = true };
	BufferTag resource = { 0 };
	ResourceXGateSnapshot gate = { 0 }, rebound_gate;
	ResourceXDecodedFrame dispatch = { 0 };
	ResourceXCallerWitness caller_witness, saved_witness;
	ResourceXApplyResult dispatch_gate_session_result pg_attribute_unused();
	ResourceXApplyResult discard_result, result = RESOURCE_X_APPLY_INVALID;
	ClusterSemanticResourceXPeerOpenResult peer_open_result;
	PcmXSessionAuthResult dispatch_session_check;
	int32 master_node = 0;
	int32 rebound_master_node;
	uint64 master_session = 31;
	uint64 rebound_master_session;
	uint64 absolute_deadline_us = 3000000, retry_slice_us = 10000, remaining_us;
	uint64 now_us pg_attribute_unused();
	uint64 direct_init_ownership_generation = 0, direct_init_reservation_token = 0;
	uint32 requester_sender_connection_generation = 61;
	uint32 master_ingress_connection_generation = 61;
	uint32 requester_sender_recheck, master_ingress_recheck, dispatch_recheck_failure_mask;
	uint32 rebound_requester_connection_generation, rebound_master_connection_generation;
	bool dispatch_admission_current, dispatch_gate_session_current;
	bool dispatch_requester_sampled, dispatch_master_sampled;
	bool rebound_requester_sampled, rebound_master_sampled, rebound_peer_matches, stage_ok;
	long timeout_ms;
	const char *diagnostic_stage pg_attribute_unused();
	int iteration;

	/* Both actions and local-follower shape reach the same actual fence.
	 * Its pending branch must not be restricted to REQUEST/non-follower. */
	UT_ASSERT(action == RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST
			  || action == RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	memset(&caller_witness, 0x2a, sizeof(caller_witness));
	saved_witness = caller_witness;
	dispatch.common.assertion_sequence = 41;
	gate.formation = 17;
	for (iteration = 0; iteration < 4; iteration++) {
		if (delivery_retries != 0 || assert_sends != 0) {
			UT_ASSERT_EQ(memcmp(&caller_witness, &saved_witness, sizeof(caller_witness)), 0);
			UT_ASSERT_EQ(dispatch.common.assertion_sequence, UINT64_C(41));
			UT_ASSERT_EQ(absolute_deadline_us, UINT64_C(3000000));
			return RESOURCE_X_APPLY_APPLIED;
		}
#undef CHECK_FOR_INTERRUPTS
#define CHECK_FOR_INTERRUPTS() ((void)0)
#define pg_usleep(usec) ((void)(usec), gcs_block_resource_x_observation_pause())
#include "test_cluster_resource_x_dispatch_observation.inc"
#undef pg_usleep
#undef CHECK_FOR_INTERRUPTS
	}
	return result;
}

UT_TEST(test_actual_dispatch_reobserves_request_assert_and_follower)
{
	int leg;

	for (leg = 0; leg < 4; leg++) {
		reset_delivery_fixture();
		session_gap = pause_clears_gap = true;
		terminal_pause_calls = 0;
		UT_ASSERT_EQ(
			run_actual_dispatch_observation(leg < 2 ? RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST
													: RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT,
											(leg & 1) != 0),
			RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(terminal_pause_calls, 1);
		UT_ASSERT_EQ(kind9_requests + ack_sends, 0);
		UT_ASSERT_EQ(delivery_retries + assert_sends, 1);
		delivery_retries = assert_sends = 0;
		scenario = 10;
		UT_ASSERT_EQ(
			run_actual_dispatch_observation(RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT, true),
			RESOURCE_X_APPLY_STALE);
		UT_ASSERT_EQ(terminal_pause_calls, 1);
	}
	pause_clears_gap = session_gap = false;
	scenario = 0;
}

UT_TEST(test_actual_context_check_preserves_typed_observation_and_terminal_cover)
{
	int kind;
	ResourceXWriterUseContext context = { 0 };

	reset_delivery_fixture();
	context.ref = delivery_ref;
	context.r4_record_generation = 77;
	context.buffer_ownership_generation = 8;
	delivery_terminal = true;
	for (kind = PCM_X_SESSION_AUTH_CONNECTION_NOT_READY; kind <= PCM_X_SESSION_AUTH_CONNECTION_TORN;
		 kind++) {
		terminal_pending_kind = (PcmXSessionAuthResult)kind;
		terminal_pending_remaining = 1;
		context_cover_checks = 0;
		UT_ASSERT_EQ(cluster_gcs_resource_x_target_context_recheck_result_exact(&context),
					 RESOURCE_X_APPLY_BAD_STATE);
		UT_ASSERT_EQ(context_cover_checks, 0);
		UT_ASSERT_EQ(cluster_gcs_resource_x_target_context_recheck_result_exact(&context),
					 RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(context_cover_checks, 1);
	}
	delivery_terminal = false;
	UT_ASSERT_EQ(cluster_gcs_resource_x_target_context_recheck_result_exact(&context),
				 RESOURCE_X_APPLY_STALE);
	context.writer_activation_token = 1;
	UT_ASSERT_EQ(cluster_gcs_resource_x_target_context_recheck_result_exact(&context),
				 RESOURCE_X_APPLY_INVALID);
	context.writer_activation_token = 0;
	scenario = 9;
	terminal_pending_remaining = 1;
	UT_ASSERT_EQ(cluster_gcs_resource_x_target_context_recheck_result_exact(&context),
				 RESOURCE_X_APPLY_STALE);
	terminal_pending_remaining = 0;
	scenario = 0;
}

/* Actual GCS PREPARE/PUBLISH/ABORT consumers.  The entry mutation, codec and
 * transport dependencies are controlled below; real PCM/codec suites supply
 * their independent lifecycle/wire coverage. */
static int evict_claims, evict_aborts, evict_commits, evict_enqueues;
static bool evict_after_prepare_gap, evict_after_enqueue_gap, evict_queue_full;
static ResourceXApplyResult evict_abort_result, evict_commit_result;

ResourceXApplyResult
cluster_pcm_lock_resource_x_target_evict_prepare_exact(const BufferTag *tag, int32 node,
													   uint64 formation, uint64 session, uint64 r4,
													   uint64 generation, uint64 token,
													   uint32 connection, int32 procno,
													   ResourceXDecodedFrame *release,
													   ResourceXLocalOwnerHandle *owner)
{
	memset(release, 0, sizeof(*release));
	memset(owner, 0, sizeof(*owner));
	release->kind = RESOURCE_X_WIRE_RELEASE_X;
	release->payload_bytes = RESOURCE_X_CONTROL_V1_BYTES;
	release->common.logical_assertion.resource = *tag;
	release->common.master_session_incarnation = session;
	release->common.resource_formation = formation;
	owner->owner_generation = 19;
	owner->buffer_ownership_generation = generation;
	owner->reservation_token = token;
	evict_claims++;
	if (evict_after_prepare_gap)
		session_gap = true;
	return RESOURCE_X_APPLY_APPLIED;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_target_evict_abort_exact(const ResourceXLocalOwnerHandle *owner)
{
	UT_ASSERT_EQ(owner->owner_generation, 19);
	evict_aborts++;
	return evict_abort_result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_target_evict_commit_exact(const ResourceXDecodedFrame *release,
													  int32 node, uint64 r4, uint64 generation,
													  const ResourceXLocalOwnerHandle *owner)
{
	UT_ASSERT_EQ(owner->owner_generation, 19);
	UT_ASSERT_EQ(evict_enqueues, 1);
	evict_commits++;
	return evict_commit_result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_release_x_exact(const ResourceXDecodedFrame *release, int32 source,
											ResourceXMasterSnapshot *out)
{
	UT_ASSERT(false); /* All cases deliberately exercise remote publication. */
	return RESOURCE_X_APPLY_INVALID;
}

static bool
evict_encode(uint8 kind, const ResourceXDecodedFrame *frame, void *payload, uint16 capacity,
			 uint16 *bytes, ResourceXWireReject *reject)
{
	UT_ASSERT_EQ(kind, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE);
	UT_ASSERT_EQ(frame->kind, RESOURCE_X_WIRE_RELEASE_X);
	UT_ASSERT_EQ(capacity, RESOURCE_X_CONTROL_V1_BYTES);
	*bytes = capacity;
	*reject = RESOURCE_X_WIRE_REJECT_NONE;
	memset(payload, 0xa5, capacity);
	return true;
}

static bool
evict_enqueue(uint8 kind, uint32 destination, const void *payload, uint16 bytes)
{
	UT_ASSERT_EQ(kind, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE);
	UT_ASSERT_EQ(destination, 1);
	UT_ASSERT_EQ(bytes, RESOURCE_X_CONTROL_V1_BYTES);
	UT_ASSERT_EQ(((const uint8 *)payload)[0], 0xa5);
	if (evict_queue_full)
		return false;
	evict_enqueues++;
	if (evict_after_enqueue_gap)
		session_gap = true;
	return true;
}

static bool
pg_attribute_unused() evict_session_snapshot(const BufferTag *tag, ResourceXGateSnapshot *gate,
											 int32 *node, uint64 *session)
{
	return gcs_block_resource_x_gate_session_snapshot_result(tag, gate, node, session)
		   == PCM_X_SESSION_AUTH_OK;
}

static bool
pg_attribute_unused() evict_session_recheck(const BufferTag *tag, const ResourceXGateSnapshot *gate,
											int32 node, uint64 session)
{
	return gcs_block_resource_x_gate_session_recheck_result(tag, gate, node, session)
		   == RESOURCE_X_APPLY_APPLIED;
}

#define cluster_resource_x_wire_encode evict_encode
#define cluster_grd_outbound_enqueue_backend_msg evict_enqueue
#define gcs_block_resource_x_gate_session_snapshot evict_session_snapshot
#define gcs_block_resource_x_gate_session_recheck evict_session_recheck
#define gcs_block_resource_x_fail_closed_current() ((void)0)
#include "test_cluster_resource_x_eviction_owner.inc"
#undef gcs_block_resource_x_fail_closed_current
#undef gcs_block_resource_x_gate_session_recheck
#undef gcs_block_resource_x_gate_session_snapshot
#undef cluster_grd_outbound_enqueue_backend_msg
#undef cluster_resource_x_wire_encode

static void
evict_fixture(ResourceXTargetEvictionPlan *plan)
{
	reset_delivery_fixture();
	master = 1;
	terminal_identity_conflict = terminal_pending_remaining = 0;
	eviction_admission_closed = false;
	evict_claims = evict_aborts = evict_commits = evict_enqueues = 0;
	evict_after_prepare_gap = evict_after_enqueue_gap = evict_queue_full = false;
	evict_abort_result = evict_commit_result = RESOURCE_X_APPLY_APPLIED;
	memset(plan, 0, sizeof(*plan));
	plan->prepared = plan->local_n_committed = true;
	plan->master_node = 1;
	plan->gate.formation = 17;
	plan->r4_record_generation = 77;
	plan->cached_ownership_generation = 7;
	plan->sender_connection_generation = 61;
	plan->tag = delivery_ref.assertion.resource;
	plan->release.common.logical_assertion = delivery_ref.assertion;
	plan->release.common.master_session_incarnation = 31;
	plan->release.kind = RESOURCE_X_WIRE_RELEASE_X;
	plan->release.payload_bytes = plan->payload_bytes = RESOURCE_X_CONTROL_V1_BYTES;
	plan->owner.owner_generation = 19;
	memset(plan->release_payload, 0xa5, sizeof(plan->release_payload));
}

UT_TEST(test_actual_eviction_publication_pending_preserves_one_frozen_release)
{
	int leg;

	for (leg = 0; leg < 8; leg++) {
		ResourceXTargetEvictionPlan plan;
		ResourceXDecodedFrame frozen_release;
		bool retry_pending = false;

		evict_fixture(&plan);
		frozen_release = plan.release;
		if (leg < 7) {
			terminal_pending_kind
				= (PcmXSessionAuthResult)(PCM_X_SESSION_AUTH_CONNECTION_NOT_READY + leg);
			terminal_pending_remaining = 1;
		} else
			peer_observation_gap = CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_CAPABILITY_NOT_READY;
		UT_ASSERT_EQ(cluster_gcs_resource_x_target_evict_publish_exact(&plan, &retry_pending),
					 RESOURCE_X_APPLY_BAD_STATE);
		UT_ASSERT(retry_pending && plan.prepared && !plan.release_admitted);
		UT_ASSERT_EQ(evict_enqueues, 0);
		terminal_pending_remaining = 0;
		peer_observation_gap = CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_NOT_CHECKED;
		evict_after_enqueue_gap = true;
		UT_ASSERT_EQ(cluster_gcs_resource_x_target_evict_publish_exact(&plan, &retry_pending),
					 RESOURCE_X_APPLY_BAD_STATE);
		UT_ASSERT(retry_pending && plan.prepared && plan.release_admitted);
		UT_ASSERT_EQ(evict_enqueues, 1);
		UT_ASSERT_EQ(evict_commits, 0);
		session_gap = evict_after_enqueue_gap = false;
		UT_ASSERT_EQ(cluster_gcs_resource_x_target_evict_publish_exact(&plan, &retry_pending),
					 RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(!retry_pending && !plan.prepared && plan.release_admitted);
		UT_ASSERT_EQ(evict_enqueues, 1);
		UT_ASSERT_EQ(evict_commits, 1);
		UT_ASSERT_EQ(memcmp(&frozen_release, &plan.release, sizeof(frozen_release)), 0);
	}
}

UT_TEST(test_actual_eviction_hard_refusal_and_capacity_have_distinct_retry_cause)
{
	int leg;

	for (leg = 0; leg < 5; leg++) {
		ResourceXTargetEvictionPlan plan;
		bool retry_pending = true;
		ResourceXApplyResult result;

		evict_fixture(&plan);
		if (leg == 0)
			eviction_admission_closed = true;
		if (leg == 1)
			terminal_identity_conflict = 3;
		if (leg == 2)
			peer_observation_gap = CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_CAPABILITY_DRIFT;
		if (leg == 3)
			evict_commit_result = RESOURCE_X_APPLY_BAD_STATE;
		if (leg == 4)
			evict_queue_full = true;
		result = cluster_gcs_resource_x_target_evict_publish_exact(&plan, &retry_pending);
		UT_ASSERT(result != RESOURCE_X_APPLY_APPLIED && result != RESOURCE_X_APPLY_DUPLICATE);
		UT_ASSERT(retry_pending == (leg == 4));
		UT_ASSERT(plan.prepared);
		UT_ASSERT_EQ(evict_enqueues, leg == 3 ? 1 : 0);
	}
	eviction_admission_closed = false;
	terminal_identity_conflict = 0;
}

UT_TEST(test_actual_eviction_prepare_pending_proves_exact_reversible_cleanup)
{
	int leg;

	for (leg = 0; leg < 3; leg++) {
		ResourceXTargetEvictionPlan plan;
		ClusterPcmOwnSnapshot exact;

		evict_fixture(&plan);
		memset(&exact, 0, sizeof(exact));
		exact.tag = plan.tag;
		exact.generation = 7;
		exact.reservation_token = 9;
		exact.flags = PCM_OWN_FLAG_REVOKING;
		exact.pcm_state = PCM_STATE_X;
		session_gap = leg == 0;
		evict_after_prepare_gap = leg != 0;
		if (leg == 2)
			evict_abort_result = RESOURCE_X_APPLY_STALE;
		UT_ASSERT_EQ(
			cluster_gcs_resource_x_target_evict_prepare_exact(&exact.tag, &exact, 77, 9, &plan),
			leg == 2 ? RESOURCE_X_APPLY_RECOVERY_BLOCKED : RESOURCE_X_APPLY_BAD_STATE);
		UT_ASSERT(!plan.prepared);
		UT_ASSERT_EQ(evict_aborts, leg == 0 ? 0 : 1);
		UT_ASSERT_EQ(evict_claims, leg == 0 ? 0 : 1);
	}
	session_gap = false;
}

static ClusterPcmOwnSnapshot failed_round_live;
static ResourceXApplyResult failed_round_entry_result;
static bool failed_round_pair;

static ClusterPcmOwnResult
failed_round_snapshot(BufferDesc *buf, ClusterPcmOwnSnapshot *out)
{
	*out = failed_round_live;
	return CLUSTER_PCM_OWN_OK;
}

static bool
run_actual_failed_round_observation(bool direct_init, bool join_only,
									ResourceXApplyResult *result_out)
{
	ClusterSemanticAdmissionToken admission
		= { .entered = true,
			.record_generation = 77,
			.feature_bit = CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1,
			.side = CLUSTER_SEMANTIC_TARGET_SIDE };
	ResourceXAssertion assertion = delivery_ref.assertion;
	BufferTag resource = assertion.resource;
	BufferDesc *buf = GetBufferDescriptor(0);
	ClusterPcmOwnSnapshot own = failed_round_live, failure_live;
	ClusterPcmOwnResult own_result;
	ResourceXBootstrapRoundAction action = RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
	ResourceXBootstrapRoundFailureSnapshot failure_round;
	ResourceXApplyResult result = RESOURCE_X_APPLY_INVALID, failure_snapshot_result;
	ResourceXApplyResult dispatch_gate_session_result pg_attribute_unused();
	ResourceXGateSnapshot gate = { .formation = 17 };
	ClusterSemanticResourceXPeerOpenResult peer_open_result pg_attribute_unused();
	int32 master_node = 1;
	uint64 master_session = 31, absolute_deadline_us = 3000000, now_us;
	uint64 retry_slice_us pg_attribute_unused() = 10000;
	uint32 requester_sender_connection_generation = 61, master_ingress_connection_generation = 61;
	uint32 requester_sender_recheck, master_ingress_recheck, dispatch_recheck_failure_mask;
	bool dispatch_admission_current, dispatch_gate_session_current;
	bool dispatch_requester_sampled, dispatch_master_sampled;
	bool round_drift_authority_current, round_drift_retained_pair_exact,
		round_drift_retained_buffer_exact;
	const char *diagnostic_stage pg_attribute_unused();
	int iteration;

	own.pcm_state = PCM_STATE_X;
	own.flags = 0;
	own.generation = 7;
	own.reservation_token = 9;
	for (iteration = 0; iteration < 2; iteration++) {
		if (iteration != 0) {
			/* Only a fresh enclosing-driver iteration was reached. This is
			 * not a grant and does not emulate the next bootstrap round. */
			UT_ASSERT_EQ(own.generation, 7);
			UT_ASSERT_EQ(own.reservation_token, 9);
			UT_ASSERT_EQ(absolute_deadline_us, UINT64_C(3000000));
			*result_out = result;
			return true;
		}
		{
#define cluster_bufmgr_pcm_own_snapshot failed_round_snapshot
#define cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(...)                    \
	failed_round_entry_result
#define cluster_pcm_lock_resource_x_holder_pair_retained_fence_exact(...) failed_round_pair
#define cluster_bufmgr_pcm_own_n_retained_release_inflight_exact(...) failed_round_pair
#define gcs_block_resource_x_gate_session_recheck evict_session_recheck
#include "test_cluster_resource_x_failed_round_observation.inc"
#undef gcs_block_resource_x_gate_session_recheck
#undef cluster_bufmgr_pcm_own_n_retained_release_inflight_exact
#undef cluster_pcm_lock_resource_x_holder_pair_retained_fence_exact
#undef cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact
#undef cluster_bufmgr_pcm_own_snapshot
		}
	}
	*result_out = result;
	return false;
}

UT_TEST(test_actual_failed_round_observation_retries_only_exact_predecessor_shape)
{
	int path, leg;

	for (path = 0; path < 2; path++) {
		for (leg = 0; leg < 8; leg++) {
			ResourceXTargetEvictionPlan ignored;
			ResourceXApplyResult result;

			evict_fixture(&ignored);
			memset(&failed_round_live, 0, sizeof(failed_round_live));
			failed_round_live.tag = delivery_ref.assertion.resource;
			failed_round_live.pcm_state = PCM_STATE_N;
			failed_round_live.generation = 8;
			failed_round_live.reservation_token = 10;
			failed_round_pair = path != 0;
			failed_round_live.flags = path ? PCM_OWN_FLAG_REVOKING : 0;
			failed_round_entry_result = RESOURCE_X_APPLY_NOT_FOUND;
			terminal_pause_calls = 0;
			if (leg < 7) {
				terminal_pending_kind
					= (PcmXSessionAuthResult)(PCM_X_SESSION_AUTH_CONNECTION_NOT_READY + leg);
				terminal_pending_remaining = 1;
			} else
				peer_observation_gap
					= CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_IMAGE_OBSERVATION_PENDING;
			UT_ASSERT(run_actual_failed_round_observation(false, false, &result));
			UT_ASSERT_EQ(terminal_pause_calls, 1);
			UT_ASSERT_EQ(installs + publishes + evict_enqueues, 0);
		}
	}
	for (leg = 0; leg < 5; leg++) {
		ResourceXTargetEvictionPlan ignored;
		ResourceXApplyResult result;

		evict_fixture(&ignored);
		failed_round_pair = false;
		failed_round_live.flags = 0;
		failed_round_live.generation = 8;
		failed_round_entry_result = RESOURCE_X_APPLY_NOT_FOUND;
		session_gap = true;
		terminal_pause_calls = 0;
		if (leg == 0)
			failed_round_entry_result = RESOURCE_X_APPLY_APPLIED;
		if (leg == 1)
			failed_round_live.generation++;
		if (leg == 2)
			scenario = 10;
		UT_ASSERT(!run_actual_failed_round_observation(leg == 3, leg == 4, &result));
		UT_ASSERT_EQ(result, RESOURCE_X_APPLY_STALE);
		UT_ASSERT_EQ(terminal_pause_calls, 0);
	}
	session_gap = false;
	scenario = 0;
}

int
main(void)
{
	UT_PLAN(24);
	UT_RUN(test_actual_terminal_ingress_keeps_master_and_physical_source_distinct);
	UT_RUN(test_actual_kind9_ingress_does_not_send_ack_for_fused_admission);
	UT_RUN(test_actual_fused_admission_notifies_ready_resource_without_registry_tick);
	UT_RUN(test_actual_join_ingress_carries_current_authority_coordinates);
	UT_RUN(test_actual_quiet_tick_installs_or_redrives_without_foreground);
	UT_RUN(test_actual_cleanup_ingress_retains_exact_late_frame_then_quiet_tick_finishes);
	UT_RUN(test_actual_delivery_tick_fences_drift_active_owner_and_unwind);
	UT_RUN(test_actual_terminal_gate_reobserves_missing_and_torn_session);
	UT_RUN(test_actual_terminal_gate_rejects_proved_identity_or_admission_change);
	UT_RUN(test_actual_delivery_observation_gap_retains_owner_then_finishes);
	UT_RUN(test_actual_image_ingress_yields_before_join_and_after_t3);
	UT_RUN(test_actual_source_gate_never_rolls_back_armed_pair_for_sample_gap);
	UT_RUN(test_actual_source_before_mutation_waits_without_exporting_session);
	UT_RUN(test_actual_foreground_retained_and_predecessor_wait_reobserve_without_fuse);
	UT_RUN(test_actual_dispatch_reobserves_request_assert_and_follower);
	UT_RUN(test_actual_context_check_preserves_typed_observation_and_terminal_cover);
	UT_RUN(test_actual_peer_observation_gap_keeps_delivery_and_ingress_owned);
	UT_RUN(test_actual_type17_and_assert_admission_keep_unproven_work_pending);
	UT_RUN(test_actual_remote_s_observation_gap_cancels_exact_status_and_revoke);
	UT_RUN(test_actual_itl_recycle_pending_keeps_exact_cancel_and_no_finish_permission);
	UT_RUN(test_actual_eviction_publication_pending_preserves_one_frozen_release);
	UT_RUN(test_actual_eviction_hard_refusal_and_capacity_have_distinct_retry_cause);
	UT_RUN(test_actual_eviction_prepare_pending_proves_exact_reversible_cleanup);
	UT_RUN(test_actual_failed_round_observation_retries_only_exact_predecessor_shape);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
