/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_route_policy.c
 *	  Stage 8 R4 canonical current-holder route policy.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>
#include <sys/mman.h>
#include <unistd.h>

#include "cluster/cluster_conf.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_cssd.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_gcs.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_gcs_block_dedup.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_grd_outbound.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_ic_tier1.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_lms_shard.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_multixact_current_wire.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_gcs.h"
#include "cluster/cluster_ic_rdma.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_xnode_profile.h"
#include "cluster/cluster_xnode_lever.h"
#include "cluster/cluster_startup_phase.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_multixact_current_stats.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_r4_observe.h"
#include "cluster/cluster_runtime_visibility.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_touched_peers.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/storage/cluster_undo_block0_current.h"
#include "miscadmin.h"
#include "storage/latch.h"
#include "storage/buf_internals.h"

#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* The two symbols exist only in the USE_CLUSTER_UNIT special object.  They
 * call the same static exact-length branches used by the production envelope
 * handlers; neither symbol is present in a production build. */
extern bool cluster_gcs_block_test_r4_request80(const ClusterICEnvelope *env,
											 const void *payload);
extern bool cluster_gcs_block_test_r4_forward96(const ClusterICEnvelope *env,
											 const void *payload);
extern int cluster_gcs_block_test_r4_tx_origin_context_count(void);
extern void cluster_gcs_block_test_r4_tx_origin_drain(void);
extern int cluster_gcs_block_r4_tx_resolve_pending_detail(char *out, Size capacity);
extern bool cluster_gcs_block_test_current_mx_forward128(
	const ClusterICEnvelope *env, const void *payload);
extern bool cluster_gcs_block_test_r4_refusal_status(ClusterCrBuildResult result,
											  ClusterCrBuildReason reason,
											  bool admitted_forward,
											  GcsBlockReplyStatus *status_out);
extern bool cluster_gcs_block_test_decode_r4_reply(
	const ClusterICEnvelope *env, const void *payload, uint64 expected_request_id,
	uint64 expected_epoch, int32 expected_requester_backend_id, uint8 expected_transition_id,
	int32 expected_sender_node, int32 expected_forwarding_master_node,
	uint8 expected_reply_domain);
extern bool cluster_gcs_block_test_arm_r4_reply_slot(uint64 request_id,
												 uint64 request_epoch,
												 int32 requester_backend_id,
												 uint8 transition_id,
												 int32 expected_master_node);
extern bool cluster_gcs_block_test_arm_legacy_reply_slot(uint64 request_id,
													 uint64 request_epoch,
													 int32 requester_backend_id,
													 uint8 transition_id,
													 int32 expected_master_node);
extern bool cluster_gcs_block_test_snapshot_r4_reply_slot(
	GcsBlockReplyHeader *header_out, char block_out[GCS_BLOCK_DATA_SIZE],
	bool *reply_received_out, uint64 *stale_drop_count_out);
extern bool cluster_gcs_block_test_r4_requester_arm(
	BufferTag tag, uint64 request_epoch, int32 expected_master_node,
	uint64 next_sequence, uint64 *request_id_out);
extern bool cluster_gcs_block_test_snapshot_r4_requester_slot(
	bool *in_use_out, uint8 *reply_domain_out, uint64 *request_id_out,
	uint8 *transition_id_out, BufferTag *tag_out, uint64 *request_epoch_out,
	int32 *expected_master_node_out, ClusterGcsBlockDirectState *direct_state_out,
	bool *direct_target_prepared_out);
extern bool cluster_gcs_block_test_release_r4_requester_slot(void);
extern uint64 cluster_gcs_block_r4_requester_count(void);
extern bool cluster_gcs_block_test_r4_fetch_and_wait(BufferTag tag, SCN read_scn,
											 int32 real_master_node,
											 char dst_page[GCS_BLOCK_DATA_SIZE]);
extern ClusterCrBuildResult cluster_gcs_block_cr_fetch_and_wait(
	BufferTag tag, SCN read_scn, char dst_page[BLCKSZ],
	ClusterCrBuildReason *reason_out);

/* Backend globals reached by the narrow production route section. */
sigjmp_buf *PG_exception_stack = NULL;
MemoryContext CurrentMemoryContext = (MemoryContext)0x1;
ErrorContextCallback *error_context_stack = NULL;
volatile sig_atomic_t InterruptPending = false;
static Latch route_test_latch;
Latch *MyLatch = &route_test_latch;
static int retry_latch_wait_calls;
static bool retry_latch_cancel;
static bool retry_latch_epoch_drift;
static bool reply_wait_epoch_drift;
static bool reply_wait_admission_drift;
static int process_interrupt_calls;
static uint64 route_test_epoch = UINT64_C(9);
static bool route_ereport_armed;
static bool route_ereport_caught;
static int route_ereport_sqlstate;
int MaxBackends = 32;
bool cluster_enabled = true;
int cluster_node_id = 1;
int cluster_pcm_grd_max_entries = 0;
bool cluster_recmerge_window_active = false;
bool cluster_online_join = true;
int cluster_lms_workers = 4;
ClusterConf *ClusterConfShmem = NULL;
bool cluster_smart_fusion = false;
BackendId MyBackendId = 1;
int cluster_gcs_reply_timeout_ms = 5000;
int cluster_gcs_block_retransmit_max_retries = 4;
int cluster_gcs_block_retransmit_initial_backoff_ms = 10;
bool cluster_ic_suppress_gcs_done_cap = false;
int cluster_injection_armed_count = 0;
char *BufferBlocks = NULL;
Block *LocalBufferBlockPointers = NULL;
int NBuffers = 0;
int NLocBuffer = 0;
ClusterXnodeProfileShared *ClusterXnodeProfileCtl = NULL;
bool cluster_xnode_profile_enabled = false;
int cluster_gcs_block_lost_write_action = 0;
int cluster_gcs_block_starvation_backoff_ms = 1;
bool cluster_ges_bast = false;
bool cluster_gcs_block_local_cache = true;
static BufferDesc capacity_buffer;
static bool capacity_content_held;
static bool capacity_gate_open = true;
static bool capacity_wait_active;

/* Runtime seams for the real ordinary-read endpoint.  A capacity refusal
 * has no image/grant/storage side effect: reaching one is a fixture failure,
 * not a simulated successful acquisition.  Transport replies still enter
 * the real reply decoder and the real outstanding-slot state machine. */
bool
cluster_authority_readiness_managed(void)
{
	return false;
}
bool
cluster_serving_ready_is_current(void)
{
	return true;
}
bool
cluster_clean_leave_block_serve_gate_allows(void)
{
	return true;
}
bool
cluster_xp_relkind_hint_is_index_for(const BufferTag *tag pg_attribute_unused())
{
	return false;
}
bool
cluster_ic_rdma_block_reply_lane_connected(int32 peer pg_attribute_unused(),
										   const char **reason pg_attribute_unused())
{
	return false;
}
void
cluster_lmon_wakeup(void)
{}
void
cluster_grd_inc_block_path_failclosed(void)
{
	abort();
}
void
cluster_lever_a_note_remote_ack_degraded(void)
{
	abort();
}
void
cluster_xp_note_read(bool reship pg_attribute_unused())
{
	abort();
}
bool
cluster_bufmgr_block_is_extension_for_gcs(BufferTag tag pg_attribute_unused())
{
	abort();
}
SCN
cluster_bufmgr_read_block_scn_for_gcs(BufferDesc *buf pg_attribute_unused())
{
	abort();
}
bool
cluster_bufmgr_refresh_block_from_storage_for_gcs(BufferDesc *buf pg_attribute_unused(),
												  SCN *scn pg_attribute_unused())
{
	abort();
}
void
cluster_bufmgr_finish_direct_land_target_for_gcs(BufferDesc *buf pg_attribute_unused(),
												 bool valid pg_attribute_unused(),
												 XLogRecPtr lsn pg_attribute_unused())
{
	abort();
}
bool
cluster_bufmgr_prepare_direct_land_target_for_gcs(BufferDesc *buf pg_attribute_unused(),
												  BufferTag tag pg_attribute_unused(),
												  void **page pg_attribute_unused())
{
	abort();
}
void
cluster_bufmgr_pcm_own_republish_grant_pending_image(BufferDesc *buf pg_attribute_unused())
{
	abort();
}
bool
cluster_bufmgr_pcm_x_content_write_permitted(BufferDesc *buf pg_attribute_unused())
{
	abort();
}
void
cluster_gcs_send_transition_and_wait(BufferTag tag pg_attribute_unused(),
									 PcmLockTransition transition pg_attribute_unused(),
									 int master pg_attribute_unused())
{
	abort();
}
bool
cluster_gcs_try_send_transition_and_wait(BufferTag tag pg_attribute_unused(),
										 PcmLockTransition transition pg_attribute_unused(),
										 int master pg_attribute_unused())
{
	abort();
}
bool
cluster_pcm_lock_pi_watermark_prov_query(BufferTag tag pg_attribute_unused(),
										 ClusterPcmWmProv *out pg_attribute_unused())
{
	abort();
}
SCN
cluster_pcm_lock_pi_watermark_scn_query(BufferTag tag pg_attribute_unused())
{
	abort();
}
int32
cluster_pcm_master_holder_node_by_tag(BufferTag tag pg_attribute_unused())
{
	return -1;
}
const char *
cluster_pcm_wm_src_text(ClusterPcmWmSrc src pg_attribute_unused())
{
	abort();
}
void
cluster_sf_dep_install_vec(BufferTag tag pg_attribute_unused(),
						   const ClusterSfDepVec *vec pg_attribute_unused())
{
	abort();
}
void
cluster_sf_note_dep_touched(Buffer buffer pg_attribute_unused())
{
	abort();
}

void
cluster_injection_run(const char *name pg_attribute_unused())
{}

bool
cluster_injection_should_skip(const char *name pg_attribute_unused())
{
	return false;
}

uint64
GetSystemIdentifier(void)
{
	return UINT64_C(0x524f555445);
}

int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

bool
cluster_multixact_current_member_proof_bind_ctrc(
	ClusterCurrentMemberProof *proof, const ClusterCtrcTxnKeyV1 *ctrc_key)
{
	if (proof == NULL || ctrc_key == NULL)
		return false;
	ClusterCurrentMemberProofSetCtrcBinding(
		proof, ctrc_key->segment_generation, ctrc_key->slot_wrap);
	return true;
}

ClusterMxResolveResult
cluster_cr_server_current_mx_build_proof_page(
	uint16 source_node_id pg_attribute_unused(),
	const ClusterCurrentMxProofForwardV2 *request pg_attribute_unused(),
	ClusterMxResolveResult result,
	uint32 requester_capability_generation pg_attribute_unused(),
	const ClusterCurrentMemberProof *proofs pg_attribute_unused(),
	uint16 proof_count pg_attribute_unused(),
	const ClusterCurrentUpdaterProof *updater_proof pg_attribute_unused(),
	ClusterCurrentMxProofReplyPage *page pg_attribute_unused())
{
	return result;
}

bool
cluster_ctrc_origin_ack_land_shared(uint64 request_id pg_attribute_unused(),
								const ClusterCtrcLocalReleaseAckV1 *ack pg_attribute_unused())
{
	return false;
}

bool
cluster_ctrc_origin_grant_publishable(
	const ClusterCtrcTxnKeyV1 *key pg_attribute_unused(),
	const ClusterCtrcParticipantIdentity *participant pg_attribute_unused(),
	uint32 grant_generation pg_attribute_unused())
{
	return false;
}

bool
cluster_ctrc_origin_note_certificate_reply_shared(
	uint64 request_id pg_attribute_unused(),
	uint16 participant_node_id pg_attribute_unused(),
	ClusterCtrcSealReplyResult result pg_attribute_unused())
{
	return false;
}

bool
cluster_ctrc_origin_note_close_reply_shared(
	uint64 request_id pg_attribute_unused(),
	uint16 participant_node_id pg_attribute_unused(),
	ClusterCtrcSealReplyResult result pg_attribute_unused())
{
	return false;
}

bool
cluster_ctrc_origin_request_snapshot_shared(
	uint64 request_id pg_attribute_unused(),
	uint16 participant_node_id pg_attribute_unused(),
	ClusterCtrcTxnKeyV1 *key_out pg_attribute_unused(),
	ClusterCtrcParticipantIdentity *identity_out pg_attribute_unused(),
	uint32 *grant_generation_out pg_attribute_unused(),
	uint64 *seal_generation_out pg_attribute_unused(),
	ClusterCtrcSealSuboperation *suboperation_out pg_attribute_unused())
{
	return false;
}

ClusterCtrcTouchResult
cluster_ctrc_origin_touch_exact(
	const ClusterCtrcTxnKeyV1 *key pg_attribute_unused(),
	const ClusterCtrcParticipantIdentity *participant pg_attribute_unused(),
	ClusterCtrcProofClass proof_class pg_attribute_unused(),
	uint32 *grant_out pg_attribute_unused())
{
	return CLUSTER_CTRC_TOUCH_REFUSED;
}

bool
cluster_ctrc_seal_reply_decode(
	const uint8 *page pg_attribute_unused(), Size page_length pg_attribute_unused(),
	const uint8 *request_bytes pg_attribute_unused(),
	Size request_length pg_attribute_unused(),
	int32 expected_source_node pg_attribute_unused(),
	int32 expected_destination_node pg_attribute_unused(),
	ClusterCtrcSealReplyHeaderV1 *header_out pg_attribute_unused(),
	ClusterCtrcLocalReleaseAckV1 *ack_out pg_attribute_unused())
{
	return false;
}

bool
cluster_ctrc_seal_request_encode(
	const ClusterCtrcTxnKeyV1 *key pg_attribute_unused(),
	uint64 request_id pg_attribute_unused(),
	uint32 grant_generation pg_attribute_unused(),
	uint64 seal_generation pg_attribute_unused(),
	uint32 participant_capability_record_generation pg_attribute_unused(),
	ClusterCtrcSealSuboperation suboperation pg_attribute_unused(),
	uint8 *bytes pg_attribute_unused(), Size length pg_attribute_unused())
{
	return false;
}

void
cluster_ctrc_stat_bump(ClusterCtrcStatId stat pg_attribute_unused())
{}

void
cluster_ctrc_test_barrier_wait(ClusterCtrcTestBarrierPhase phase pg_attribute_unused())
{}

uint64
cluster_membership_get_last_admitted_incarnation(int32 node_id pg_attribute_unused())
{
	return UINT64_C(1);
}

bool
cluster_membership_is_member(int32 node_id pg_attribute_unused())
{
	return true;
}

bool
cluster_multixact_current_resolve_origin_member_proof(
	TransactionId member_xid pg_attribute_unused(),
	uint8 member_status pg_attribute_unused(),
	uint16 member_ordinal pg_attribute_unused(),
	uint16 member_origin_node pg_attribute_unused(),
	uint32 current_epoch pg_attribute_unused(),
	bool requester_self pg_attribute_unused(),
	const ClusterTTStatusKey *initial_key pg_attribute_unused(),
	const ClusterTTStatusResult *initial_result pg_attribute_unused(),
	ClusterCurrentMxExactLookupFn exact_lookup pg_attribute_unused(),
	void *exact_lookup_arg pg_attribute_unused(),
	ClusterCurrentMemberProof *proof pg_attribute_unused())
{
	return false;
}

void
cluster_multixact_current_stats_bump(ClusterCurrentMxStatId stat pg_attribute_unused())
{}

bool
cluster_multixact_current_successor_provenance_well_formed(
	const ClusterCurrentMxSuccessorAlias *alias,
	const ClusterTxLocator *locator, TransactionId updater_xid,
	uint16 updater_origin_node, uint32 current_epoch)
{
	return alias != NULL && locator != NULL
		&& alias->local_xid == updater_xid
		&& alias->origin_node_id == updater_origin_node
		&& alias->cluster_epoch == current_epoch
		&& locator->xid == updater_xid
		&& !UBA_is_invalid(locator->uba);
}

uint64
cluster_qvotec_get_self_incarnation(void)
{
	return UINT64_C(1);
}

uint64
cluster_reconfig_get_observed_epoch(int32 node_id pg_attribute_unused())
{
	return UINT64_C(9);
}

bool
cluster_reconfig_get_observed_slot(int32 node_id pg_attribute_unused(),
							   uint64 *incarnation, uint64 *generation)
{
	if (incarnation != NULL)
		*incarnation = UINT64_C(1);
	if (generation != NULL)
		*generation = UINT64_C(1);
	return true;
}

bool
cluster_runtime_visibility_origin_plan_canonical_diagnostic(
	const ClusterRuntimeVisibilityOriginPlan *plan pg_attribute_unused(),
	ClusterRuntimeVisibilityCanonicalDiagnostic *out)
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	return false;
}

bool
cluster_runtime_visibility_origin_plan_canonical_physical(
	const ClusterRuntimeVisibilityOriginPlan *plan pg_attribute_unused(),
	ClusterTTSlotPhysicalLocator *locator_out pg_attribute_unused(),
	bool *same_segment_out pg_attribute_unused())
{
	return false;
}

bool
cluster_runtime_visibility_physical_locator_sample_held(
	const ClusterTTSlotPhysicalLocator *locator pg_attribute_unused(),
	const ClusterSemanticAdmissionToken *admission pg_attribute_unused(),
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
	const ClusterUndoBlock0ResolvedRoot *root pg_attribute_unused(),
	ClusterTTStatusKey *key_out pg_attribute_unused(),
	ClusterTTStatusResult *result_out pg_attribute_unused(),
	bool *ctrc_physical_active_out pg_attribute_unused())
{
	return false;
}

bool
cluster_sf_peer_capability_generation_matches(
	int32 peer_id pg_attribute_unused(),
	uint32 required_capabilities pg_attribute_unused(),
	uint32 expected_generation pg_attribute_unused())
{
	return true;
}

bool
cluster_tt_slot_current_owner_by_xid(
	int node_id pg_attribute_unused(), TransactionId xid pg_attribute_unused(),
	ClusterTTSlotCurrentOwner *out pg_attribute_unused())
{
	return false;
}

ClusterTTDurableLocate
cluster_tt_slot_durable_locate_any_by_xid_origin(
	int origin_node pg_attribute_unused(), TransactionId xid pg_attribute_unused(),
	uint16 *out_seg pg_attribute_unused(), uint16 *out_slot pg_attribute_unused(),
	uint16 *out_wrap pg_attribute_unused(), uint8 *out_status pg_attribute_unused())
{
	return CLUSTER_TT_DURABLE_LOCATE_MISSING;
}

static bool origin_generation_sample_enabled;
static uint32 origin_generation_sample_value;
static int origin_generation_sample_calls;
static bool origin_generation_sample_known;
static bool origin_generation_sample_throw;

ClusterUndoBlock0Result
cluster_undo_block0_current_sample_generation(ClusterUndoBlock0CurrentGuard *guard,
											  const ClusterUndoBlock0ResolvedRoot *root,
											  ClusterUndoBlock0Generation *observed)
{
	origin_generation_sample_calls++;
	if (origin_generation_sample_throw) {
		UT_ASSERT_NOT_NULL(PG_exception_stack);
		siglongjmp(*PG_exception_stack, 1);
	}
	if (!origin_generation_sample_enabled || guard == NULL || root == NULL
		|| root->root_id != UINT64_C(0x8000) || observed == NULL)
		return CLUSTER_UNDO_BLOCK0_NOT_FOUND;
	observed->known = origin_generation_sample_known;
	observed->value = origin_generation_sample_value;
	return CLUSTER_UNDO_BLOCK0_OK;
}

bool
cluster_undo_block0_root_matches(
	const ClusterUndoBlock0ResolvedRoot *observed,
	const ClusterUndoBlock0ResolvedRoot *expected)
{
	return observed != NULL && expected != NULL
		&& memcmp(observed, expected, sizeof(*observed)) == 0;
}

int
cluster_xid_origin_slot(TransactionId xid pg_attribute_unused())
{
	return cluster_node_id;
}

static int reply_lock_acquire_calls;
static int reply_lock_release_calls;
static int reply_cv_signal_calls;

#define REQUESTER_REPLY_SCRIPT_CAPACITY 12

static ConditionVariable *reply_cv_signaled[REQUESTER_REPLY_SCRIPT_CAPACITY];

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode)
{
	UT_ASSERT(mode == LW_EXCLUSIVE || mode == LW_SHARED);
	reply_lock_acquire_calls++;
	return true;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{
	reply_lock_release_calls++;
}

void
ConditionVariableSignal(ConditionVariable *cv pg_attribute_unused())
{
	if (reply_cv_signal_calls < REQUESTER_REPLY_SCRIPT_CAPACITY)
		reply_cv_signaled[reply_cv_signal_calls] = cv;
	reply_cv_signal_calls++;
}

static int reply_cv_prepare_calls;
static int reply_cv_timed_sleep_calls;
static int reply_cv_cancel_calls;
static ConditionVariable *reply_cv_prepared[REQUESTER_REPLY_SCRIPT_CAPACITY];
static TimestampTz route_test_now;
static bool reply_cv_timed_sleep_raise;

void
ResetLatch(Latch *latch)
{
	UT_ASSERT(latch == MyLatch);
}

int
WaitLatch(Latch *latch, int wake_events, long timeout, uint32 wait_event)
{
	UT_ASSERT(latch == MyLatch);
	UT_ASSERT((wake_events & (WL_TIMEOUT | WL_EXIT_ON_PM_DEATH))
			  == (WL_TIMEOUT | WL_EXIT_ON_PM_DEATH));
	if (capacity_wait_active) {
		UT_ASSERT_EQ(timeout, 2);
		UT_ASSERT_EQ(wait_event, WAIT_EVENT_PCM_BLOCK_CONVERT_WAIT);
		UT_ASSERT(!capacity_content_held);
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&capacity_buffer.state) & BM_LOCKED, 0);
	} else
		UT_ASSERT_EQ(timeout, cluster_gcs_block_retransmit_initial_backoff_ms);
	UT_ASSERT(wait_event != 0);
	retry_latch_wait_calls++;
	if (retry_latch_cancel)
		InterruptPending = true;
	if (retry_latch_epoch_drift)
		route_test_epoch++;
	return WL_TIMEOUT;
}

void
ProcessInterrupts(void)
{
	process_interrupt_calls++;
	InterruptPending = false;
	UT_ASSERT_NOT_NULL(PG_exception_stack);
	siglongjmp(*PG_exception_stack, 1);
}

void
ConditionVariablePrepareToSleep(ConditionVariable *cv pg_attribute_unused())
{
	if (reply_cv_prepare_calls < REQUESTER_REPLY_SCRIPT_CAPACITY)
		reply_cv_prepared[reply_cv_prepare_calls] = cv;
	reply_cv_prepare_calls++;
}

bool
ConditionVariableTimedSleep(ConditionVariable *cv pg_attribute_unused(),
								long timeout pg_attribute_unused(),
								uint32 wait_event_info pg_attribute_unused())
{
	reply_cv_timed_sleep_calls++;
	if (reply_cv_timed_sleep_raise)
		siglongjmp(*PG_exception_stack, 1);
	if (reply_wait_epoch_drift)
		route_test_epoch++;
	return false;
}

bool
ConditionVariableCancelSleep(void)
{
	reply_cv_cancel_calls++;
	return false;
}

void
cluster_sf_dep_note_lost_failclosed(void)
{}

#define UT_FORMATION_EPOCH UINT64_C(9)
#define UT_ACTIVATION_GENERATION UINT64_C(12)
#define UT_LOCAL_OPEN_GENERATION UINT64_C(0xffffffff)
#define UT_LOCAL_OPEN_GENERATION_OVERFLOW UINT64_C(0x100000000)
#define UT_REQUESTER_CAPABILITY_GENERATION UINT32_C(42)
#define UT_MASTER_CAPABILITY_GENERATION UINT32_C(43)
#define UT_HOLDER_CAPABILITY_GENERATION UINT32_C(44)
#define UT_CURRENT_MX_CAPABILITY_GENERATION UINT32_C(77)
#define UT_REQUESTER_NODE 2
#define UT_MASTER_NODE 1
#define UT_HOLDER_NODE 3
#define UT_REQUESTER_BACKEND 7
#define UT_REQUEST_ID UINT64_C(0x0102030405060708)
#define UT_READ_SCN ((SCN)UINT64_C(0x1234))
#define UT_EXPECTED_PAGE_SCN ((SCN)UINT64_C(0x2222))
#define UT_MASTER_GENERATION ((UT_FORMATION_EPOCH << 32) | UINT64_C(4))
#define UT_MASTER_TRANSITION UINT64_C(7)
#define UT_REPLY_DOMAIN_LEGACY_ACQUIRE ((uint8)0)
#define UT_REPLY_DOMAIN_R4_CR ((uint8)1)
#define UT_REPLY_DOMAIN_CURRENT_MX ((uint8)2)
#define UT_R4_INTERNAL_ENDPOINT (-2)
#define UT_R4_REQUIRED_CAPABILITIES                                                          \
	(PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1 | PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1            \
	 | PGRAC_IC_HELLO_CAP_CANDIDATE2_CORRECTED_A1_V1                                        \
	 | PGRAC_IC_HELLO_CAP_UNDO_ROOT_DESCRIPTOR_V1)

typedef struct RouteSeamCapture {
	ClusterSemanticAdmissionResult admission_result;
	uint64 activation_generation;
	bool capability_ok;
	bool peer_open_ok;
	bool recheck_ok;
	int recheck_fail_at;
	bool snapshot_ok;
	bool local_dispatch_ok;
	bool current_mx_capability_ok;
	uint32 current_mx_capability_generation;
	bool current_mx_reply_enabled;
	ClusterMxDescribeResult current_mx_validate_result;
	bool current_mx_proof_reply_enabled;
	ClusterMxResolveResult current_mx_proof_validate_result;
	GcsBlockR4RouteArmResult arm_result;
	bool enqueue_ok;
	bool refusal_enqueue_ok;
	GcsBlockR4RouteSendResult finish_result;
	ClusterCrBuildResult holder_submit_result;
	ClusterCrBuildReason holder_submit_reason;
	int lookup_master_node;

	int enter_calls;
	int leave_calls;
	int lookup_calls;
	int capability_calls;
	int peer_open_calls;
	int snapshot_calls;
	int arm_calls;
	int enqueue_calls;
	int refusal_enqueue_calls;
	int finish_calls;
	int recheck_calls;
	int holder_submit_calls;
	int terminal_census_enter_calls;
	int terminal_census_recheck_calls;
	int terminal_census_root_calls;
	int candidate_acquire_begin_calls;
	int candidate_release_begin_calls;
	int candidate_resolve_calls;
	int candidate_canonical_sample_calls;
	int candidate_data_recheck_calls;
	bool candidate_cross_segment;
	bool candidate_throw_on_copy;
	int candidate_cancel_calls;
	int configured_node_count;
	ClusterTxOutcome candidate_outcome;
	ClusterTxProofKind candidate_proof;
	int observe_calls;
	int refusal_observe_calls;
	ClusterR4RefusalStage refusal_stage;
	ClusterCrBuildReason refusal_reason;
	BufferTag refusal_tag;
	uint64 refusal_request_id;
	uint64 refusal_epoch;
	SCN refusal_read_scn;
	int envelope_build_calls;
	int local_dispatch_calls;
	int raw_send_calls;
	ClusterICSendResult raw_send_result;
	uint8 raw_send_msg_type;
	int32 raw_send_dest;
	uint32 raw_send_payload_len;
	GcsBlockReplyHeader raw_send_header;
	char raw_send_page[GCS_BLOCK_DATA_SIZE];
	int foreign_undo_land_calls;
	int current_mx_capability_calls;
	int32 current_mx_capability_peer;
	int current_mx_validate_calls;
	int current_mx_proof_validate_calls;
	int current_mx_describe_serve_calls;
	int current_mx_proof_serve_calls;
	bool current_mx_slot_armed;
	uint8 current_mx_slot_domain;
	bool local_request_slot_armed;
	ClusterR4Event observed_event;
	ClusterTxResolveReason observed_tx_reason;
	ClusterCrBuildReason observed_cr_reason;

	int sequence;
	int snapshot_sequence;
	int arm_sequence;
	int enqueue_sequence;
	int refusal_enqueue_sequence;
	int finish_sequence;
	int recheck_sequence;
	int leave_sequence;
	int enter_sequence;
	int lookup_sequence;
	int capability_sequence[4];
	int peer_open_sequence[4];
	int holder_submit_sequence;
	int envelope_build_sequence;
	int local_dispatch_sequence;

	int32 capability_peers[4];
	uint32 capability_required[4];
	uint32 capability_optional[4];
	int32 peer_open_peers[4];
	uint32 peer_open_required[4];
	uint32 peer_open_generation[4];

	GcsBlockR4RouteIdentity armed_identity;
	ClusterR4CrRouteProof armed_proof;
	uint8 armed_transition;
	uint32 armed_lifetime_hint_ms;
	bool armed_lifetime_hint_trusted;

	GcsBlockR4RouteIdentity finished_identity;
	ClusterR4CrRouteProof finished_proof;
	uint8 finished_transition;
	bool finished_outbound_admitted;

	int enqueue_worker;
	uint8 enqueue_msg_type;
	uint32 enqueue_dest;
	uint16 enqueue_payload_len;
	uint32 enqueue_required_capability;
	uint32 enqueue_connection_generation;
	uint8 enqueue_payload[128];

	int refusal_enqueue_worker;
	uint32 refusal_enqueue_dest;
	uint32 refusal_enqueue_required_capability;
	uint32 refusal_enqueue_connection_generation;
	GcsBlockReplyHeader refusal_header;
	ClusterICEnvelope local_dispatch_envelope;
	int32 local_dispatch_peer;
	uint32 local_dispatch_payload_len;
	uint8 local_dispatch_payload[128];
	ClusterICEnvelope foreign_undo_env;
	GcsBlockReplyHeader foreign_undo_header;
	char foreign_undo_page[BLCKSZ];
	ClusterGcsUndoAuthTrailer foreign_undo_auth;
	ClusterICEnvelope current_mx_describe_env;
	ClusterCurrentMxDescribeForwardV2 current_mx_describe_request;
	ClusterICEnvelope current_mx_proof_env;
	ClusterCurrentMxProofForwardV2 current_mx_proof_request;

	ClusterR4CrForwardPayload submitted_forward;
	ClusterSemanticAdmissionToken submitted_admission;
	ClusterSemanticAdmissionToken rechecked_admission;
	ClusterSemanticAdmissionToken left_admission;
	const ClusterSemanticAdmissionToken *submitted_admission_address;
	const ClusterSemanticAdmissionToken *rechecked_admission_address;
	const ClusterSemanticAdmissionToken *left_admission_address;
	uint32 submitted_requester_capability_generation;
	uint32 submitted_master_capability_generation;
	bool submitted_reason_out_present;
} RouteSeamCapture;

static RouteSeamCapture route_seam;
static bool stop_new_read_allowed = true;
static int stop_new_read_calls;

bool
cluster_normal_stop_service_new_work(bool modifies_data)
{
	Assert(!modifies_data);
	stop_new_read_calls++;
	return stop_new_read_allowed;
}

bool
cluster_cr_server_r4_land_foreign_undo(
	const ClusterICEnvelope *env, const GcsBlockReplyHeader *header,
	const char undo_page[BLCKSZ], const ClusterGcsUndoAuthTrailer *undo_auth)
{
	route_seam.foreign_undo_land_calls++;
	if (env == NULL || header == NULL || undo_page == NULL || undo_auth == NULL)
		return false;
	route_seam.foreign_undo_env = *env;
	route_seam.foreign_undo_header = *header;
	memcpy(route_seam.foreign_undo_page, undo_page,
		   sizeof(route_seam.foreign_undo_page));
	route_seam.foreign_undo_auth = *undo_auth;
	return true;
}

void
cluster_gcs_current_mx_describe_serve_inline(
	const ClusterICEnvelope *env, const void *payload)
{
	route_seam.current_mx_describe_serve_calls++;
	if (env != NULL)
		route_seam.current_mx_describe_env = *env;
	if (payload != NULL && env != NULL
		&& env->payload_length
			   == sizeof(route_seam.current_mx_describe_request))
		memcpy(&route_seam.current_mx_describe_request, payload,
			   sizeof(route_seam.current_mx_describe_request));
}

void
cluster_gcs_current_mx_member_proof_serve_inline(
	const ClusterICEnvelope *env, const void *payload)
{
	route_seam.current_mx_proof_serve_calls++;
	if (env != NULL)
		route_seam.current_mx_proof_env = *env;
	if (payload != NULL && env != NULL
		&& env->payload_length == sizeof(route_seam.current_mx_proof_request))
		memcpy(&route_seam.current_mx_proof_request, payload,
			   sizeof(route_seam.current_mx_proof_request));
}

typedef struct RequesterReplyStep {
	GcsBlockReplyStatus status;
	uint32 envelope_source_node;
	int32 header_sender_node;
	int32 forwarding_master_node;
	uint64 page_lsn;
	uint8 block_fill;
} RequesterReplyStep;

typedef struct RequesterSendCapture {
	int calls;
	int send_sequence;
	uint8 msg_type;
	uint32 dest_node;
	uint16 payload_len;
	ClusterR4CrRequestPayload request;
	ClusterR4CrForwardPayload tx_forward;
	bool tx_kind2;
	bool legacy_read;
	bool suppress_reply;
	int suppress_reply_calls;
	int refuse_enqueue_calls;
	bool corrupt_reply_checksum;
	bool corrupt_reply_identity;
	bool suppress_done_cap;
	bool refuse_done;
	int done_calls;
	int done_cap_calls;
	int32 done_cap_peer;
	GcsBlockDonePayload done[REQUESTER_REPLY_SCRIPT_CAPACITY];
	uint32 done_dest[REQUESTER_REPLY_SCRIPT_CAPACITY];
	bool done_after_release[REQUESTER_REPLY_SCRIPT_CAPACITY];
	bool slot_armed;
	uint8 slot_domain;
	ClusterGcsBlockDirectState direct_state;
	bool direct_target_prepared;
	char reply_page[GCS_BLOCK_DATA_SIZE];
	int reply_step_count;
	RequesterReplyStep reply_steps[REQUESTER_REPLY_SCRIPT_CAPACITY];
	uint64 request_ids[REQUESTER_REPLY_SCRIPT_CAPACITY];
	bool slot_armed_by_call[REQUESTER_REPLY_SCRIPT_CAPACITY];
	uint8 slot_domain_by_call[REQUESTER_REPLY_SCRIPT_CAPACITY];
} RequesterSendCapture;

static RequesterSendCapture requester_send;

/* The real bufmgr rearm and header snapshots below are generated verbatim.
 * Only unrelated admission/lock runtime is external: no competing X head,
 * one descriptor, and the ordinary gate can close during the test. */

static bool
capacity_gate_snapshot(ResourceXGateSnapshot *out)
{
	memset(out, 0, sizeof(*out));
	out->phase = capacity_gate_open ? RESOURCE_X_GATE_OPEN : RESOURCE_X_GATE_FROZEN;
	return true;
}

static bool
capacity_local_barrier(BufferTag tag pg_attribute_unused())
{
	return false;
}
static bool
capacity_lock_held(LWLock *lock pg_attribute_unused())
{
	return capacity_content_held;
}
static void
cluster_pcm_own_report_bump_failure(BufferDesc *buf pg_attribute_unused(),
									ClusterPcmOwnResult result pg_attribute_unused(),
									uint64 generation pg_attribute_unused(),
									uint32 flags pg_attribute_unused(),
									const char *context pg_attribute_unused())
{
	abort();
}
static void
cluster_bufmgr_resource_x_writer_report_failure(ResourceXApplyResult result pg_attribute_unused(),
												BufferDesc *buf pg_attribute_unused(),
												const char *context pg_attribute_unused())
{
	abort();
}

#include "test_cluster_pcm_snapshot_owner.inc"
#define cluster_pcm_lock_resource_x_gate_snapshot capacity_gate_snapshot
#define cluster_gcs_block_resource_x_local_s_barrier_active capacity_local_barrier
#define LWLockHeldByMe capacity_lock_held
#define GetBufferDescriptor(id) ((void)(id), &capacity_buffer)
#include "test_cluster_gcs_read_rearm_owner.inc"
#undef GetBufferDescriptor
#undef LWLockHeldByMe
#undef cluster_gcs_block_resource_x_local_s_barrier_active
#undef cluster_pcm_lock_resource_x_gate_snapshot

/* These branches are outside an ordinary empty-N reservation rearm. */
bool
LWLockAcquireOrWait(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	abort();
}
ClusterPcmOwnResult
cluster_pcm_own_reclaim_read_image_exact(BufferDesc *buf pg_attribute_unused(),
										 const ClusterPcmOwnSnapshot *snapshot
											 pg_attribute_unused(),
										 uint64 *generation pg_attribute_unused())
{
	abort();
}
void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{
	abort();
}
void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size pg_attribute_unused(),
				bool *found pg_attribute_unused())
{
	abort();
}
Size
mul_size(Size s1 pg_attribute_unused(), Size s2 pg_attribute_unused())
{
	/* The real PCM sidecar is fixture-owned; backend shmem setup is not run. */
	abort();
}
void
perform_spin_delay(SpinDelayStatus *status pg_attribute_unused())
{
	abort();
}
void
finish_spin_delay(SpinDelayStatus *status pg_attribute_unused())
{}

bool
cluster_sf_peer_supports_gcs_done(int32 peer_id)
{
	requester_send.done_cap_calls++;
	requester_send.done_cap_peer = peer_id;
	return !requester_send.suppress_done_cap && peer_id != cluster_node_id;
}

static uint32
route_test_capability_generation(int32 peer_id)
{
	if (peer_id == UT_REQUESTER_NODE)
		return UT_REQUESTER_CAPABILITY_GENERATION;
	if (peer_id == UT_MASTER_NODE)
		return UT_MASTER_CAPABILITY_GENERATION;
	if (peer_id == UT_HOLDER_NODE)
		return UT_HOLDER_CAPABILITY_GENERATION;
	return 0;
}

static void
route_seam_reset(void)
{
	stop_new_read_allowed = true;
	stop_new_read_calls = 0;
	memset(&route_seam, 0, sizeof(route_seam));
	origin_generation_sample_enabled = false;
	origin_generation_sample_value = 9;
	origin_generation_sample_calls = 0;
	origin_generation_sample_known = true;
	origin_generation_sample_throw = false;
	route_seam.admission_result = CLUSTER_SEMANTIC_ADMISSION_OK;
	route_seam.configured_node_count = 1;
	route_seam.activation_generation = UT_ACTIVATION_GENERATION;
	route_seam.capability_ok = true;
	route_seam.peer_open_ok = true;
	route_seam.recheck_ok = true;
	route_seam.snapshot_ok = true;
	route_seam.local_dispatch_ok = true;
	route_seam.current_mx_capability_ok = true;
	route_seam.current_mx_capability_generation
		= UT_CURRENT_MX_CAPABILITY_GENERATION;
	route_seam.current_mx_validate_result = CMX_DESC_UNKNOWN;
	route_seam.current_mx_proof_validate_result = CMX_RESOLVE_UNKNOWN;
	route_seam.arm_result = GCS_BLOCK_R4_ROUTE_ARM_NEW;
	route_seam.enqueue_ok = true;
	route_seam.refusal_enqueue_ok = true;
	route_seam.finish_result = GCS_BLOCK_R4_ROUTE_SEND_FORWARDED;
	route_seam.holder_submit_result = CLUSTER_CR_BUILD_FULL;
	route_seam.holder_submit_reason = CLUSTER_CR_BUILD_NONE;
	route_seam.lookup_master_node = UT_MASTER_NODE;
	route_seam.raw_send_result = CLUSTER_IC_SEND_HARD_ERROR;
	route_seam.candidate_outcome = CLUSTER_TX_COMMITTED;
	route_seam.candidate_proof = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
	route_test_now = 0;
	route_test_epoch = UT_FORMATION_EPOCH;
	reply_cv_timed_sleep_raise = false;
	retry_latch_wait_calls = 0;
	retry_latch_cancel = false;
	retry_latch_epoch_drift = false;
	reply_wait_epoch_drift = false;
	reply_wait_admission_drift = false;
	process_interrupt_calls = 0;
	InterruptPending = false;
}

static BufferTag
route_test_tag(void)
{
	BufferTag tag;

	memset(&tag, 0, sizeof(tag));
	tag.spcOid = 1663;
	tag.dbOid = 5;
	tag.relNumber = 20000;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = 37;
	return tag;
}

static ClusterICEnvelope
route_test_envelope(uint8 msg_type, uint32 source, uint32 dest, uint32 payload_length)
{
	ClusterICEnvelope env;

	memset(&env, 0, sizeof(env));
	env.magic = PGRAC_IC_ENVELOPE_MAGIC;
	env.version = PGRAC_IC_ENVELOPE_VERSION_V1;
	env.msg_type = msg_type;
	env.source_node_id = source;
	env.dest_node_id = dest;
	env.epoch = UT_FORMATION_EPOCH;
	env.payload_length = payload_length;
	return env;
}

bool
cluster_ic_envelope_build(ClusterICEnvelope *out_env, uint8 msg_type,
						  uint32 source_node_id, uint32 dest_node_id,
						  const void *payload, uint32 payload_length)
{
	route_seam.envelope_build_calls++;
	route_seam.envelope_build_sequence = ++route_seam.sequence;
	if (!route_seam.local_dispatch_ok || out_env == NULL || payload == NULL)
		return false;
	*out_env = route_test_envelope(msg_type, source_node_id, dest_node_id,
							   payload_length);
	return true;
}

bool
cluster_ic_dispatch_envelope(const ClusterICEnvelope *env, const void *payload,
						 int32 peer_id)
{
	typedef struct TestR4Reply8240 {
		GcsBlockReplyHeader header;
		char block_data[GCS_BLOCK_DATA_SIZE];
	} TestR4Reply8240;
	const ClusterR4CrRequestPayload *request;
	TestR4Reply8240 reply;
	ClusterICEnvelope reply_env;
	BufferTag slot_tag;
	bool in_use = false;
	uint8 slot_domain = UINT8_MAX;
	uint64 slot_request_id = 0;
	uint64 slot_epoch = 0;
	int32 slot_master = -1;
	uint8 transition_id = 0;
	ClusterGcsBlockDirectState direct_state = GCS_BLOCK_DIRECT_ABORTED;
	bool direct_target_prepared = true;

	route_seam.local_dispatch_calls++;
	route_seam.local_dispatch_sequence = ++route_seam.sequence;
	if (env != NULL)
		route_seam.local_dispatch_envelope = *env;
	route_seam.local_dispatch_peer = peer_id;
	route_seam.local_dispatch_payload_len = env != NULL ? env->payload_length : 0;
	if (payload != NULL && route_seam.local_dispatch_payload_len
						   <= sizeof(route_seam.local_dispatch_payload))
		memcpy(route_seam.local_dispatch_payload, payload,
			   route_seam.local_dispatch_payload_len);
	if (route_seam.local_dispatch_ok && env != NULL && payload != NULL
		&& env->msg_type == PGRAC_IC_MSG_GCS_BLOCK_REQUEST
		&& env->payload_length == sizeof(ClusterR4CrRequestPayload)) {
		request = (const ClusterR4CrRequestPayload *)payload;
		route_seam.local_request_slot_armed
			= cluster_gcs_block_test_snapshot_r4_requester_slot(
				&in_use, &slot_domain, &slot_request_id, &transition_id,
				&slot_tag, &slot_epoch, &slot_master, &direct_state,
				&direct_target_prepared)
			  && in_use && slot_domain == UT_REPLY_DOMAIN_R4_CR
			  && slot_request_id == request->base.request_id
			  && transition_id == request->base.transition_id
			  && memcmp(&slot_tag, &request->base.tag, sizeof(slot_tag)) == 0
			  && slot_epoch == request->base.epoch
			  && slot_master == (int32)env->dest_node_id
			  && direct_state == GCS_BLOCK_DIRECT_UNARMED
			  && !direct_target_prepared;
		if (!route_seam.local_request_slot_armed)
			return false;
		memset(&reply, 0, sizeof(reply));
		reply.header.request_id = request->base.request_id;
		reply.header.epoch = request->base.epoch;
		reply.header.page_lsn = UINT64_C(0xabcdef);
		reply.header.sender_node = UT_HOLDER_NODE;
		reply.header.requester_backend_id
			= request->base.requester_backend_id;
		reply.header.transition_id = request->base.transition_id;
		reply.header.status = GCS_BLOCK_REPLY_R4_CR_FULL;
		GcsBlockReplyHeaderSetForwardingMasterNode(
			&reply.header, (int32)env->dest_node_id);
		memset(reply.block_data, 0x6d, sizeof(reply.block_data));
		reply.header.checksum
			= cluster_gcs_block_compute_checksum(reply.block_data);
		reply_env = route_test_envelope(
			PGRAC_IC_MSG_GCS_BLOCK_REPLY, UT_HOLDER_NODE,
			(uint32)cluster_node_id, sizeof(reply));
		cluster_gcs_handle_block_reply_envelope(&reply_env, &reply);
	}
	return route_seam.local_dispatch_ok;
}

ClusterICSendResult
cluster_ic_send_envelope(uint8 msg_type, int32 dest_node_id,
					 const void *payload, uint32 payload_len)
{
	route_seam.raw_send_calls++;
	route_seam.raw_send_msg_type = msg_type;
	route_seam.raw_send_dest = dest_node_id;
	route_seam.raw_send_payload_len = payload_len;
	if (payload != NULL
		&& (payload_len == GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE
			|| payload_len
				   == GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE + sizeof(ClusterGcsUndoAuthTrailer))) {
		memcpy(&route_seam.raw_send_header, payload,
			   sizeof(route_seam.raw_send_header));
		memcpy(route_seam.raw_send_page,
			   ((const char *)payload) + sizeof(GcsBlockReplyHeader),
			   sizeof(route_seam.raw_send_page));
	}
	return route_seam.raw_send_result;
}

uint32
cluster_gcs_block_dedup_lifetime_ms(int initial_backoff_ms, int max_retries,
									int reply_timeout_ms)
{
	int64 lifetime_ms;

	if (initial_backoff_ms <= 0)
		initial_backoff_ms = 100;
	if (max_retries < 0)
		max_retries = 4;
	if (max_retries > 30)
		max_retries = 30;
	if (reply_timeout_ms <= 0)
		reply_timeout_ms = 5000;
	lifetime_ms = (int64)initial_backoff_ms * ((int64)((1u << max_retries) - 1))
				  + (int64)(max_retries + 1) * reply_timeout_ms;
	if (lifetime_ms > (int64)PG_UINT32_MAX)
		lifetime_ms = (int64)PG_UINT32_MAX;
	return (uint32)lifetime_ms;
}

void
cluster_gcs_block_dedup_register_backend_exit_hook(void)
{}

bool
cluster_grd_outbound_enqueue_backend_msg(uint8 msg_type, uint32 dest_node_id,
										  const void *payload, uint16 payload_len)
{
	typedef struct TestR4Reply8240 {
		GcsBlockReplyHeader header;
		char block_data[GCS_BLOCK_DATA_SIZE];
	} TestR4Reply8240;
	TestR4Reply8240 reply;
	ClusterICEnvelope env;
	bool in_use = false;
	uint64 slot_request_id = 0;
	uint64 slot_epoch = 0;
	int32 slot_master = -1;
	uint8 transition_id = 0;
	BufferTag slot_tag;
	RequesterReplyStep reply_step = {
		.status = GCS_BLOCK_REPLY_R4_CR_FULL,
		.envelope_source_node = UT_HOLDER_NODE,
		.header_sender_node = UT_HOLDER_NODE,
		.forwarding_master_node = UT_MASTER_NODE,
		.page_lsn = UINT64_C(0xabcdef),
		.block_fill = 0x6d
	};
	int call_index = requester_send.calls;

	if (msg_type == PGRAC_IC_MSG_GCS_BLOCK_DONE) {
		int i = requester_send.done_calls++;
		uint8 domain;
		ClusterGcsBlockDirectState direct_state;
		bool direct_prepared;

		if (payload == NULL || payload_len != sizeof(GcsBlockDonePayload)
			|| i >= REQUESTER_REPLY_SCRIPT_CAPACITY)
			return false;
		memcpy(&requester_send.done[i], payload, payload_len);
		requester_send.done_dest[i] = dest_node_id;
		requester_send.done_after_release[i]
			= cluster_gcs_block_test_snapshot_r4_requester_slot(
				  &in_use, &domain, &slot_request_id, &transition_id, &slot_tag, &slot_epoch,
				  &slot_master, &direct_state, &direct_prepared)
			  && !in_use && slot_request_id == 0 && slot_epoch == 0
			  && reply_cv_cancel_calls >= i + 1;
		return !requester_send.refuse_done;
	}
	requester_send.calls++;
	requester_send.send_sequence = ++route_seam.sequence;
	requester_send.msg_type = msg_type;
	requester_send.dest_node = dest_node_id;
	requester_send.payload_len = payload_len;
	if (payload != NULL && msg_type == PGRAC_IC_MSG_GCS_BLOCK_FORWARD
		&& payload_len == sizeof(requester_send.tx_forward)) {
		ClusterTxLocator locator;
		ClusterTxResolution resolution;
		uint32 physical_generation = UINT32_MAX;

		memcpy(&requester_send.tx_forward, payload,
			   sizeof(requester_send.tx_forward));
		requester_send.tx_kind2
			= ClusterR4ForwardExtensionGetLocatorGeneration(
				&requester_send.tx_forward.extension,
				CLUSTER_R4_WIRE_TX_RESOLVE, &locator,
				&physical_generation);
		requester_send.slot_armed
			= cluster_gcs_block_test_snapshot_r4_requester_slot(
				&in_use, &requester_send.slot_domain, &slot_request_id,
				&transition_id, &slot_tag, &slot_epoch, &slot_master,
				&requester_send.direct_state,
				&requester_send.direct_target_prepared)
			  && in_use
			  && slot_request_id
					 == requester_send.tx_forward.base.request_id
			  && transition_id
					 == requester_send.tx_forward.base.transition_id
			  && memcmp(&slot_tag, &requester_send.tx_forward.base.tag,
						 sizeof(slot_tag)) == 0
			  && slot_epoch == requester_send.tx_forward.base.epoch
			  && slot_master == (int32)dest_node_id;
		if (!requester_send.tx_kind2
			|| (physical_generation != 9 && physical_generation != UINT32_MAX)
			|| !requester_send.slot_armed)
			return false;
		if (requester_send.suppress_reply)
			return true;
		memset(&resolution, 0, sizeof(resolution));
		resolution.locator_echo = locator;
		resolution.locator_echo.tt_wrap = 19;
		resolution.top_xid = locator.xid;
		resolution.outcome = CLUSTER_TX_COMMITTED;
		resolution.proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
		resolution.commit_scn = (SCN)101;
		resolution.horizon_scn = (SCN)89;
		resolution.authority.origin_epoch
			= requester_send.tx_forward.base.epoch;
		resolution.authority.tt_generation = UINT64_C(17);
		resolution.authority.authority_scn = (SCN)103;
		memset(&reply, 0, sizeof(reply));
		reply.header.request_id
			= requester_send.tx_forward.base.request_id;
		reply.header.epoch = requester_send.tx_forward.base.epoch;
		reply.header.page_lsn = UINT64_C(0xabcdef);
		reply.header.sender_node = (int32)dest_node_id;
		reply.header.requester_backend_id
			= requester_send.tx_forward.base.requester_backend_id;
		reply.header.transition_id
			= requester_send.tx_forward.base.transition_id;
		reply.header.status = GCS_BLOCK_REPLY_R4_TX_RESOLVE_RESULT;
		GcsBlockReplyHeaderSetForwardingMasterNode(
			&reply.header, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
		if (!ClusterR4TxVerdictPageEncode(
				(uint8 *)reply.block_data, &resolution))
			return false;
		reply.header.checksum
			= cluster_gcs_block_compute_checksum(reply.block_data);
		env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY,
						  dest_node_id, (uint32)cluster_node_id,
						  sizeof(reply));
		cluster_gcs_handle_block_reply_envelope(&env, &reply);
		return true;
	}
	if (payload == NULL
		|| (payload_len != sizeof(requester_send.request)
			&& !(requester_send.legacy_read && payload_len == sizeof(GcsBlockRequestPayload))))
		return false;
	memset(&requester_send.request, 0, sizeof(requester_send.request));
	memcpy(&requester_send.request, payload, payload_len);
	requester_send.slot_armed = cluster_gcs_block_test_snapshot_r4_requester_slot(
		&in_use, &requester_send.slot_domain, &slot_request_id, &transition_id,
		&slot_tag, &slot_epoch, &slot_master, &requester_send.direct_state,
		&requester_send.direct_target_prepared)
		&& in_use && slot_request_id == requester_send.request.base.request_id
		&& transition_id == requester_send.request.base.transition_id
		&& memcmp(&slot_tag, &requester_send.request.base.tag, sizeof(slot_tag)) == 0
		&& slot_epoch == requester_send.request.base.epoch
		&& slot_master == (int32)dest_node_id;
	if (call_index >= 0 && call_index < REQUESTER_REPLY_SCRIPT_CAPACITY) {
		requester_send.request_ids[call_index] = requester_send.request.base.request_id;
		requester_send.slot_armed_by_call[call_index] = requester_send.slot_armed;
		requester_send.slot_domain_by_call[call_index] = requester_send.slot_domain;
		if (call_index < requester_send.reply_step_count)
			reply_step = requester_send.reply_steps[call_index];
	}
	if (call_index < requester_send.refuse_enqueue_calls)
		return false;
	if (requester_send.suppress_reply || call_index < requester_send.suppress_reply_calls)
		return true;

	memset(&reply, 0, sizeof(reply));
	reply.header.request_id = requester_send.request.base.request_id;
	reply.header.epoch = requester_send.request.base.epoch;
	reply.header.page_lsn = reply_step.page_lsn;
	reply.header.sender_node = reply_step.header_sender_node;
	reply.header.requester_backend_id = requester_send.request.base.requester_backend_id;
	reply.header.transition_id = requester_send.request.base.transition_id;
	reply.header.status = (uint8)reply_step.status;
	GcsBlockReplyHeaderSetForwardingMasterNode(
		&reply.header, reply_step.forwarding_master_node);
	memset(reply.block_data, reply_step.block_fill, sizeof(reply.block_data));
	memcpy(requester_send.reply_page, reply.block_data, sizeof(reply.block_data));
	reply.header.checksum = cluster_gcs_block_compute_checksum(reply.block_data);
	if (requester_send.corrupt_reply_checksum)
		reply.header.checksum ^= UINT32_C(1);
	if (requester_send.corrupt_reply_identity)
		reply.header.request_id++;
	env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY,
						  reply_step.envelope_source_node,
						  (uint32)cluster_node_id, sizeof(reply));
	cluster_gcs_handle_block_reply_envelope(&env, &reply);
	return true;
}

/* Build wire fixtures from literal bytes, independently of the production
 * encoder under test. */
static ClusterR4CrRequestPayload
route_test_request80(void)
{
	ClusterR4CrRequestPayload request;
	uint8 *extension;

	memset(&request, 0, sizeof(request));
	request.base.request_id = UT_REQUEST_ID;
	request.base.epoch = UT_FORMATION_EPOCH;
	request.base.tag = route_test_tag();
	request.base.sender_node = UT_REQUESTER_NODE;
	request.base.requester_backend_id = UT_REQUESTER_BACKEND;
	request.base.transition_id = PCM_TRANS_N_TO_S;
	/* A negotiated requester supplies a legal 1000 ms inherited lifetime. */
	request.base.reserved_0[2] = 0xe8;
	request.base.reserved_0[3] = 0x03;
	extension = (uint8 *)&request.extension;
	extension[0] = 1;
	extension[1] = 1;
	extension[4] = 0x34;
	extension[5] = 0x12;
	return request;
}

static ClusterR4CrForwardPayload
route_test_forward96(void)
{
	static const uint8 extension_bytes[32] = {
		0x01, 0x01, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
		0x09, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x22, 0x22, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
	};
	ClusterR4CrForwardPayload forward;

	memset(&forward, 0, sizeof(forward));
	forward.base.request_id = UT_REQUEST_ID;
	forward.base.epoch = UT_FORMATION_EPOCH;
	forward.base.tag = route_test_tag();
	forward.base.original_requester_node = UT_REQUESTER_NODE;
	forward.base.requester_backend_id = UT_REQUESTER_BACKEND;
	forward.base.master_node = UT_MASTER_NODE;
	forward.base.transition_id = PCM_TRANS_N_TO_S;
	forward.base.expected_pi_watermark_scn_bytes[0] = 0x34;
	forward.base.expected_pi_watermark_scn_bytes[1] = 0x12;
	forward.base.reserved_0[4] = 1;
	memcpy(&forward.extension, extension_bytes, sizeof(extension_bytes));
	return forward;
}

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(), int lineNumber pg_attribute_unused())
{
	abort();
}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

void
FlushErrorState(void)
{}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	return route_ereport_armed && elevel >= ERROR;
}

bool
errstart_cold(int elevel, const char *domain pg_attribute_unused())
{
	return route_ereport_armed && elevel >= ERROR;
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errcode(int sqlerrcode)
{
	if (route_ereport_armed)
		route_ereport_sqlstate = sqlerrcode;
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	if (route_ereport_armed) {
		route_ereport_caught = true;
		UT_ASSERT_NOT_NULL(PG_exception_stack);
		if (PG_exception_stack != NULL)
			siglongjmp(*PG_exception_stack, 1);
		abort();
	}
}

uint64
cluster_epoch_get_current(void)
{
	return route_test_epoch;
}

void
cluster_scn_observe(SCN remote_scn pg_attribute_unused())
{}

int
scn_time_cmp(SCN a, SCN b)
{
	uint64 left = a & SCN_LOCAL_MASK;
	uint64 right = b & SCN_LOCAL_MASK;
	return (left > right) - (left < right);
}

TimestampTz
GetCurrentTimestamp(void)
{
	route_test_now += 2000;
	return route_test_now;
}

bool
cluster_touched_peers_stamp(int32 node_id pg_attribute_unused(),
							ClusterTouchKind kind pg_attribute_unused())
{
	return true;
}

const ClusterICMsgTypeInfo *
cluster_ic_get_msg_type_info(uint8 msg_type)
{
	static const ClusterICMsgTypeInfo data_info = {
		.msg_type = PGRAC_IC_MSG_GCS_BLOCK_REPLY,
		.name = "test_gcs_block_reply",
		.allowed_producer_mask = 0,
		.broadcast_ok = false,
		.handler = NULL,
		.plane = CLUSTER_IC_PLANE_DATA
	};

	return msg_type == PGRAC_IC_MSG_GCS_BLOCK_REPLY ? &data_info : NULL;
}

int
cluster_gcs_lookup_master(BufferTag tag pg_attribute_unused())
{
	route_seam.lookup_calls++;
	route_seam.lookup_sequence = ++route_seam.sequence;
	return route_seam.lookup_master_node;
}

int
cluster_gcs_lookup_master_static(BufferTag tag pg_attribute_unused())
{
	return route_seam.lookup_master_node;
}

int
cluster_conf_node_count(void)
{
	return route_seam.configured_node_count;
}

bool
cluster_grd_join_remaster_active_for_shard(BufferTag tag pg_attribute_unused())
{
	return false;
}

bool
cluster_grd_block_view_rebuilt(BufferTag tag pg_attribute_unused())
{
	return true;
}

void
cluster_grd_inc_join_block_failclosed(void)
{}

bool
cluster_grd_offpath_boot_decided(void)
{
	return true;
}

bool
cluster_grd_recovery_in_progress(void)
{
	return false;
}

ClusterCssdPeerState
cluster_cssd_get_peer_state(int node_id pg_attribute_unused())
{
	return CLUSTER_CSSD_PEER_ALIVE;
}

bool
cluster_merged_instance_is_materialized(int origin_node pg_attribute_unused())
{
	return true;
}

uint64
cluster_merged_instance_recovered_through(int origin_node pg_attribute_unused())
{
	return UINT64_MAX;
}

XLogRecPtr
cluster_pcm_lock_pi_watermark_lsn_query(BufferTag tag pg_attribute_unused())
{
	return InvalidXLogRecPtr;
}

int
cluster_ic_tier1_my_data_channel(void)
{
	return 0;
}

int
cluster_lms_shard_for_tag(const BufferTag *tag pg_attribute_unused(), int n_workers)
{
	return n_workers > 0 ? 0 : -1;
}

int
cluster_gcs_block_payload_shard(uint8 msg_type, const void *payload, uint16 payload_len,
								int n_workers)
{
	if (payload == NULL || n_workers <= 0)
		return -1;
	if (msg_type == PGRAC_IC_MSG_GCS_BLOCK_FORWARD
		&& payload_len == sizeof(ClusterR4CrForwardPayload))
		return 0;
	return -1;
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_enter(uint64 feature_bit, ClusterSemanticAdmissionSide side,
								  ClusterSemanticAdmissionToken *token)
{
	route_seam.enter_calls++;
	route_seam.enter_sequence = ++route_seam.sequence;
	if (token != NULL)
		memset(token, 0, sizeof(*token));
	if (route_seam.admission_result != CLUSTER_SEMANTIC_ADMISSION_OK)
		return route_seam.admission_result;
	if (feature_bit != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		|| side != CLUSTER_SEMANTIC_TARGET_SIDE || token == NULL)
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	token->feature_bit = feature_bit;
	token->record_generation = route_seam.activation_generation;
	token->formation_epoch = route_test_epoch;
	token->side = (uint8)side;
	token->entered = true;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

bool
cluster_semantic_activation_recheck(const ClusterSemanticAdmissionToken *token)
{
	route_seam.recheck_calls++;
	route_seam.recheck_sequence = ++route_seam.sequence;
	route_seam.rechecked_admission_address = token;
	if (token != NULL)
		route_seam.rechecked_admission = *token;
	return route_seam.recheck_ok && token != NULL && token->entered
		   && (route_seam.recheck_fail_at == 0
			   || route_seam.recheck_calls < route_seam.recheck_fail_at)
		   && !(reply_wait_admission_drift && reply_cv_timed_sleep_calls > 0);
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	if (token == NULL || !token->entered)
		return;
	route_seam.leave_calls++;
	route_seam.leave_sequence = ++route_seam.sequence;
	route_seam.left_admission_address = token;
	route_seam.left_admission = *token;
	memset(token, 0, sizeof(*token));
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_enter_r4_terminal_census(
	ClusterSemanticAdmissionToken *token)
{
	route_seam.terminal_census_enter_calls++;
	if (token == NULL)
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	memset(token, 0, sizeof(*token));
	token->feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	token->record_generation = route_seam.activation_generation;
	token->formation_epoch = UT_FORMATION_EPOCH;
	token->side = CLUSTER_SEMANTIC_TARGET_SIDE;
	token->entered = true;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

bool
cluster_semantic_activation_recheck_r4_terminal_census(
	const ClusterSemanticAdmissionToken *token)
{
	route_seam.terminal_census_recheck_calls++;
	return route_seam.recheck_ok && token != NULL && token->entered
		   && token->formation_epoch == UT_FORMATION_EPOCH;
}

bool
cluster_semantic_activation_resolve_shared_undo_root(
	const ClusterSemanticAdmissionToken *token, ClusterUndoPathIntent intent,
	uint32 owner_instance, uint32 segment_id,
	ClusterUndoBlock0ResolvedRoot *out)
{
	if (token == NULL || !token->entered || out == NULL
		|| intent != CLUSTER_UNDO_PATH_RUNTIME_SHARED
		|| owner_instance != (uint32)UT_MASTER_NODE + 1
		|| (segment_id != 5 && segment_id != 6))
		return false;
	memset(out, 0, sizeof(*out));
	out->intent = intent;
	out->root_id = UINT64_C(0x8000) + segment_id - 5;
	out->root_generation = UINT64_C(3);
	return true;
}

bool
cluster_semantic_activation_resolve_shared_undo_root_r4_terminal_census(
	const ClusterSemanticAdmissionToken *token, ClusterUndoPathIntent intent,
	uint32 owner_instance, uint32 segment_id,
	ClusterUndoBlock0ResolvedRoot *out)
{
	route_seam.terminal_census_root_calls++;
	if (token == NULL || !token->entered || out == NULL
		|| intent != CLUSTER_UNDO_PATH_RUNTIME_SHARED
		|| owner_instance != (uint32)UT_MASTER_NODE + 1
		|| (segment_id != 5 && segment_id != 6))
		return false;
	memset(out, 0, sizeof(*out));
	out->intent = intent;
	out->root_id = UINT64_C(0x8000) + segment_id - 5;
	out->root_generation = UINT64_C(3);
	return true;
}

NodeId
uba_origin_node_id(UBA uba pg_attribute_unused())
{
	return (NodeId)UT_MASTER_NODE;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_begin_admitted(
	const ClusterUndoBlock0LogicalKey *key, ClusterUndoBlock0CurrentMode mode,
	int timeout_ms pg_attribute_unused(),
	const ClusterSemanticAdmissionToken *admission,
	ClusterUndoBlock0CurrentGuard *guard, ClusterUndoBlock0Result *failure)
{
	route_seam.candidate_acquire_begin_calls++;
	if (failure != NULL)
		*failure = CLUSTER_UNDO_BLOCK0_OK;
	if (key == NULL || key->owner_instance != (uint8)(UT_MASTER_NODE + 1)
		|| (key->segment_id != 5 && key->segment_id != 6)
		|| mode != CLUSTER_UNDO_BLOCK0_SCUR
		|| admission == NULL || !admission->entered || guard == NULL)
		return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
	return CLUSTER_UNDO_BLOCK0_CURRENT_HELD;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_poll(
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
	ClusterUndoBlock0Result *failure pg_attribute_unused())
{
	return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
}

void
cluster_undo_block0_current_cancel(
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused())
{
	route_seam.candidate_cancel_calls++;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_release_begin(
	ClusterUndoBlock0CurrentGuard *guard,
	ClusterUndoBlock0Result *failure)
{
	route_seam.candidate_release_begin_calls++;
	if (failure != NULL)
		*failure = CLUSTER_UNDO_BLOCK0_OK;
	return guard != NULL ? CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED
						 : CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_release_poll(
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
	ClusterUndoBlock0Result *failure pg_attribute_unused())
{
	return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
}

static ClusterTxOutcome
route_test_fill_origin_resolution(
	const ClusterTxLocator *locator,
	const ClusterSemanticAdmissionToken *admission,
	ClusterTxResolution *out, ClusterTxResolveReason *reason_out)
{
	if (locator == NULL || admission == NULL || !admission->entered
		|| out == NULL || reason_out == NULL)
		return CLUSTER_TX_UNKNOWN;
	memset(out, 0, sizeof(*out));
	out->locator_echo = *locator;
	out->locator_echo.tt_wrap = 19;
	out->top_xid = locator->xid;
	out->outcome = route_seam.candidate_outcome;
	out->proof_kind = route_seam.candidate_proof;
	out->commit_scn = route_seam.candidate_outcome == CLUSTER_TX_COMMITTED
						  ? (SCN)101
						  : InvalidScn;
	out->horizon_scn = (SCN)89;
	out->authority.origin_epoch = admission->formation_epoch;
	out->authority.live_hwm_lsn = (XLogRecPtr)UINT64_C(0xabcdef);
	out->authority.tt_generation = UINT64_C(17);
	out->authority.authority_scn = (SCN)103;
	*reason_out = CLUSTER_TX_RESOLVE_NONE;
	return route_seam.candidate_outcome;
}

ClusterRuntimeVisibilityOriginStep
cluster_runtime_visibility_origin_plan_freeze_data_held(
	const ClusterTxLocator *locator, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission,
	const ClusterUndoBlock0Generation *expected_generation,
	ClusterUndoBlock0CurrentGuard *guard,
	const ClusterUndoBlock0ResolvedRoot *root,
	ClusterRuntimeVisibilityOriginPlan *plan, ClusterTxResolution *out,
	ClusterTxResolveReason *reason_out)
{
	route_seam.candidate_resolve_calls++;
	if (locator == NULL
		|| (mode != CLUSTER_TX_RESOLVE_TERMINAL_CENSUS && mode != CLUSTER_TX_RESOLVE_VISIBILITY)
		|| admission == NULL || !admission->entered || expected_generation == NULL
		|| !expected_generation->known
		|| expected_generation->value
			   != (origin_generation_sample_enabled ? origin_generation_sample_value : UINT32_C(9))
		|| guard == NULL || root == NULL || root->root_id != UINT64_C(0x8000) || plan == NULL
		|| out == NULL || reason_out == NULL)
		return CLUSTER_RUNTIME_VISIBILITY_ORIGIN_FAILED;
	memset(plan, 0, sizeof(*plan));
	plan->opaque[0] = 1;
	memcpy(plan->opaque + 8, locator, sizeof(*locator));
	if (route_seam.candidate_cross_segment)
		return CLUSTER_RUNTIME_VISIBILITY_ORIGIN_NEEDS_CANONICAL;
	(void)route_test_fill_origin_resolution(locator, admission, out, reason_out);
	return CLUSTER_RUNTIME_VISIBILITY_ORIGIN_COMPLETE;
}

bool
cluster_runtime_visibility_origin_plan_canonical_logical(
	const ClusterRuntimeVisibilityOriginPlan *plan,
	ClusterUndoBlock0LogicalKey *logical_out)
{
	if (plan == NULL || logical_out == NULL || plan->opaque[0] != 1)
		return false;
	logical_out->owner_instance = (uint8)(UT_MASTER_NODE + 1);
	logical_out->segment_id = 6;
	return true;
}

bool
cluster_runtime_visibility_origin_plan_sample_canonical_held(
	ClusterRuntimeVisibilityOriginPlan *plan, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission,
	ClusterUndoBlock0CurrentGuard *guard,
	const ClusterUndoBlock0ResolvedRoot *root,
	ClusterTxResolveReason *reason_out)
{
	route_seam.candidate_canonical_sample_calls++;
	if (plan == NULL || plan->opaque[0] != 1
		|| (mode != CLUSTER_TX_RESOLVE_TERMINAL_CENSUS && mode != CLUSTER_TX_RESOLVE_VISIBILITY)
		|| admission == NULL || !admission->entered || guard == NULL || root == NULL
		|| root->root_id != UINT64_C(0x8001) || reason_out == NULL)
		return false;
	plan->opaque[1] = 1;
	*reason_out = CLUSTER_TX_RESOLVE_NONE;
	return true;
}

ClusterTxOutcome
cluster_runtime_visibility_origin_plan_recheck_data_held(
	ClusterRuntimeVisibilityOriginPlan *plan, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission,
	ClusterUndoBlock0CurrentGuard *guard,
	const ClusterUndoBlock0ResolvedRoot *root, ClusterTxResolution *out,
	ClusterTxResolveReason *reason_out)
{
	ClusterTxLocator locator;

	route_seam.candidate_data_recheck_calls++;
	if (plan == NULL || plan->opaque[0] != 1 || plan->opaque[1] != 1
		|| mode != CLUSTER_TX_RESOLVE_TERMINAL_CENSUS
		|| admission == NULL || !admission->entered || guard == NULL
		|| root == NULL || root->root_id != UINT64_C(0x8000))
		return CLUSTER_TX_UNKNOWN;
	memcpy(&locator, plan->opaque + 8, sizeof(locator));
	return route_test_fill_origin_resolution(
		&locator, admission, out, reason_out);
}

ClusterTxOutcome
cluster_runtime_visibility_origin_plan_copy_data_held(
	ClusterRuntimeVisibilityOriginPlan *plan, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission, ClusterUndoBlock0CurrentGuard *guard,
	const ClusterUndoBlock0ResolvedRoot *root, ClusterTxResolution *out, char data_out[BLCKSZ],
	ClusterTxResolveReason *reason_out)
{
	ClusterTxLocator locator;

	route_seam.candidate_data_recheck_calls++;
	if (route_seam.candidate_throw_on_copy) {
		UT_ASSERT_NOT_NULL(PG_exception_stack);
		siglongjmp(*PG_exception_stack, 1);
	}
	if (plan == NULL || plan->opaque[0] != 1
		|| (route_seam.candidate_cross_segment && plan->opaque[1] != 1)
		|| mode != CLUSTER_TX_RESOLVE_VISIBILITY || admission == NULL || !admission->entered
		|| guard == NULL || data_out == NULL || root == NULL || root->root_id != UINT64_C(0x8000))
		return CLUSTER_TX_UNKNOWN;
	memcpy(&locator, plan->opaque + 8, sizeof(locator));
	memset(data_out, 0x5a, BLCKSZ);
	return route_test_fill_origin_resolution(&locator, admission, out, reason_out);
}

ClusterTxOutcome
cluster_runtime_visibility_resolve_exact_origin_held(
	const ClusterTxLocator *locator, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission,
	const ClusterUndoBlock0Generation *expected_generation,
	ClusterUndoBlock0CurrentGuard *guard,
	const ClusterUndoBlock0ResolvedRoot *root, ClusterTxResolution *out,
	ClusterTxResolveReason *reason_out)
{
	route_seam.candidate_resolve_calls++;
	if (locator == NULL || mode != CLUSTER_TX_RESOLVE_TERMINAL_CENSUS
		|| admission == NULL || !admission->entered
		|| expected_generation == NULL || !expected_generation->known
		|| expected_generation->value != UINT32_C(9) || guard == NULL
		|| root == NULL || root->root_id != UINT64_C(0x8000)
		|| out == NULL || reason_out == NULL)
		return CLUSTER_TX_UNKNOWN;
	memset(out, 0, sizeof(*out));
	out->locator_echo = *locator;
	out->locator_echo.tt_wrap = 19;
	out->top_xid = locator->xid;
	out->outcome = route_seam.candidate_outcome;
	out->proof_kind = route_seam.candidate_proof;
	out->commit_scn = route_seam.candidate_outcome == CLUSTER_TX_COMMITTED
						  ? (SCN)101
						  : InvalidScn;
	out->horizon_scn = (SCN)89;
	out->authority.origin_epoch = admission->formation_epoch;
	out->authority.live_hwm_lsn = (XLogRecPtr)UINT64_C(0xabcdef);
	out->authority.tt_generation = UINT64_C(17);
	out->authority.authority_scn = (SCN)103;
	*reason_out = CLUSTER_TX_RESOLVE_NONE;
	return route_seam.candidate_outcome;
}

void
cluster_lms_data_plane_close_peer_now(int32 peer_id pg_attribute_unused())
{}

void
cluster_r4_observe(ClusterR4Event event, ClusterTxResolveReason tx_reason,
				   ClusterCrBuildReason cr_reason)
{
	route_seam.observe_calls++;
	route_seam.observed_event = event;
	route_seam.observed_tx_reason = tx_reason;
	route_seam.observed_cr_reason = cr_reason;
}

void
cluster_r4_observe_refusal(ClusterR4RefusalStage stage, ClusterCrBuildReason reason,
						   const BufferTag *tag, uint64 request_id, uint64 epoch, int32 requester,
						   int32 master, SCN read_scn)
{
	route_seam.refusal_observe_calls++;
	route_seam.refusal_stage = stage;
	route_seam.refusal_reason = reason;
	route_seam.refusal_tag = *tag;
	route_seam.refusal_request_id = request_id;
	route_seam.refusal_epoch = epoch;
	route_seam.refusal_read_scn = read_scn;
	UT_ASSERT_EQ(requester, UT_REQUESTER_NODE);
	UT_ASSERT(master >= -1 && master < CLUSTER_MAX_NODES);
}

bool
cluster_sf_peer_capability_family_sample(int32 peer_id, uint32 required_bits,
									 uint32 optional_bits, bool *optional_out,
									 uint32 *generation_out)
{
	int slot = route_seam.capability_calls++;
	int sequence = ++route_seam.sequence;

	if (optional_out != NULL)
		*optional_out = false;
	if (generation_out != NULL)
		*generation_out = 0;
	if (slot < lengthof(route_seam.capability_peers)) {
		route_seam.capability_peers[slot] = peer_id;
		route_seam.capability_required[slot] = required_bits;
		route_seam.capability_optional[slot] = optional_bits;
		route_seam.capability_sequence[slot] = sequence;
	}
	if (!route_seam.capability_ok || generation_out == NULL)
		return false;
	if (optional_out != NULL)
		*optional_out = true;
	*generation_out = route_test_capability_generation(peer_id);
	return *generation_out != 0;
}

bool
cluster_sf_peer_multixact_current_capability_generation(
	int32 peer_id, uint32 *generation_out)
{
	route_seam.current_mx_capability_calls++;
	route_seam.current_mx_capability_peer = peer_id;
	if (generation_out != NULL)
		*generation_out = 0;
	if (!route_seam.current_mx_capability_ok || generation_out == NULL)
		return false;
	*generation_out = route_seam.current_mx_capability_generation;
	return true;
}

bool
cluster_semantic_activation_peer_open_matches(const ClusterSemanticAdmissionToken *token,
										  int32 authenticated_peer,
										  uint32 required_capabilities,
										  uint32 sampled_generation)
{
	int slot = route_seam.peer_open_calls++;
	int sequence = ++route_seam.sequence;

	if (slot < lengthof(route_seam.peer_open_peers)) {
		route_seam.peer_open_peers[slot] = authenticated_peer;
		route_seam.peer_open_required[slot] = required_capabilities;
		route_seam.peer_open_generation[slot] = sampled_generation;
		route_seam.peer_open_sequence[slot] = sequence;
	}
	return route_seam.peer_open_ok && token != NULL && token->entered
		   && required_capabilities == UT_R4_REQUIRED_CAPABILITIES
		   && sampled_generation == route_test_capability_generation(authenticated_peer);
}

bool
cluster_pcm_lock_r4_route_snapshot(BufferTag tag, PcmAuthoritySnapshot *authority_out,
								   uint64 *master_authority_generation_out,
								   SCN *expected_page_scn_out)
{
	route_seam.snapshot_calls++;
	route_seam.snapshot_sequence = ++route_seam.sequence;
	if (!route_seam.snapshot_ok || authority_out == NULL
		|| master_authority_generation_out == NULL || expected_page_scn_out == NULL)
		return false;
	memset(authority_out, 0, sizeof(*authority_out));
	authority_out->state = PCM_STATE_S;
	authority_out->x_holder_node = -1;
	authority_out->pending_x_requester_node = -1;
	authority_out->transition_count = UT_MASTER_TRANSITION;
	authority_out->master_holder.node_id = UT_HOLDER_NODE;
	authority_out->s_holders_bitmap = UINT32_C(1) << UT_HOLDER_NODE;
	*master_authority_generation_out = UT_MASTER_GENERATION;
	*expected_page_scn_out = UT_EXPECTED_PAGE_SCN;
	(void)tag;
	return true;
}

GcsBlockR4RouteArmResult
cluster_gcs_block_dedup_r4_route_arm_or_match(
	int worker_id, const GcsBlockR4RouteIdentity *identity, uint8 transition_id,
	const ClusterR4CrRouteProof *fresh_proof, uint32 requester_lifetime_hint_ms,
	bool lifetime_hint_trusted, GcsBlockR4RouteRecord *record_out)
{
	(void)worker_id;
	route_seam.arm_calls++;
	route_seam.arm_sequence = ++route_seam.sequence;
	if (identity != NULL)
		route_seam.armed_identity = *identity;
	if (fresh_proof != NULL)
		route_seam.armed_proof = *fresh_proof;
	route_seam.armed_transition = transition_id;
	route_seam.armed_lifetime_hint_ms = requester_lifetime_hint_ms;
	route_seam.armed_lifetime_hint_trusted = lifetime_hint_trusted;
	if ((route_seam.arm_result == GCS_BLOCK_R4_ROUTE_ARM_NEW
		 || route_seam.arm_result == GCS_BLOCK_R4_ROUTE_ARM_REPLAY)
		&& fresh_proof != NULL && record_out != NULL) {
		memset(record_out, 0, sizeof(*record_out));
		record_out->proof = *fresh_proof;
		record_out->state = GCS_BLOCK_R4_ROUTE_ROUTING;
	}
	return route_seam.arm_result;
}

GcsBlockR4RouteSendResult
cluster_gcs_block_dedup_r4_route_finish_send(
	int worker_id, const GcsBlockR4RouteIdentity *identity, uint8 transition_id,
	const ClusterR4CrRouteProof *armed_proof, bool outbound_admitted)
{
	(void)worker_id;
	route_seam.finish_calls++;
	route_seam.finish_sequence = ++route_seam.sequence;
	if (identity != NULL)
		route_seam.finished_identity = *identity;
	if (armed_proof != NULL)
		route_seam.finished_proof = *armed_proof;
	route_seam.finished_transition = transition_id;
	route_seam.finished_outbound_admitted = outbound_admitted;
	return route_seam.finish_result;
}

bool
cluster_lms_outbound_enqueue_cap_bound(int worker_id, uint8 msg_type, uint32 dest_node_id,
									   const void *payload, uint16 payload_len,
									   uint32 required_capability,
									   uint32 connection_generation)
{
	route_seam.enqueue_calls++;
	route_seam.enqueue_sequence = ++route_seam.sequence;
	route_seam.enqueue_worker = worker_id;
	route_seam.enqueue_msg_type = msg_type;
	route_seam.enqueue_dest = dest_node_id;
	route_seam.enqueue_payload_len = payload_len;
	route_seam.enqueue_required_capability = required_capability;
	route_seam.enqueue_connection_generation = connection_generation;
	if (payload != NULL && payload_len <= sizeof(route_seam.enqueue_payload))
		memcpy(route_seam.enqueue_payload, payload, payload_len);
	if (payload != NULL
		&& payload_len == sizeof(ClusterCurrentMxDescribeForwardV2)) {
		const ClusterCurrentMxDescribeForwardV2 *request = payload;
		typedef struct TestCurrentMxDescribeReply {
			GcsBlockReplyHeader header;
			ClusterCurrentMxDescribeReplyPage page;
		} TestCurrentMxDescribeReply;
		BufferTag slot_tag;
		TestCurrentMxDescribeReply reply;
		ClusterICEnvelope reply_env;
		bool in_use = false;
		uint64 slot_request_id = 0;
		uint64 slot_epoch = 0;
		int32 slot_master = -1;
		uint8 transition_id = UINT8_MAX;
		ClusterGcsBlockDirectState direct_state = GCS_BLOCK_DIRECT_ABORTED;
		bool direct_target_prepared = true;

		route_seam.current_mx_slot_armed
			= cluster_gcs_block_test_snapshot_r4_requester_slot(
				&in_use, &route_seam.current_mx_slot_domain,
				&slot_request_id, &transition_id, &slot_tag, &slot_epoch,
				&slot_master, &direct_state, &direct_target_prepared)
			  && in_use
			  && route_seam.current_mx_slot_domain
					 == UT_REPLY_DOMAIN_CURRENT_MX
			  && slot_request_id == request->prefix.request_id
			  && transition_id == 0
			  && slot_epoch == request->prefix.epoch
			  && slot_master == (int32)dest_node_id
			  && direct_state == GCS_BLOCK_DIRECT_UNARMED
			  && !direct_target_prepared;
		if (route_seam.current_mx_reply_enabled
			&& route_seam.current_mx_slot_armed) {
			memset(&reply, 0, sizeof(reply));
			reply.header.request_id = request->prefix.request_id;
			reply.header.epoch = request->prefix.epoch;
			reply.header.sender_node = (int32)dest_node_id;
			reply.header.requester_backend_id
				= request->prefix.requester_backend_id;
			reply.header.status
				= (uint8)GCS_BLOCK_REPLY_CURRENT_MX_DESCRIBE_RESULT;
			GcsBlockReplyHeaderSetForwardingMasterNode(
				&reply.header, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
			reply.page.header.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
			reply.page.header.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
			reply.page.header.kind
				= GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE;
			reply.page.header.result
				= (uint8)route_seam.current_mx_validate_result;
			reply.page.header.source_node_id = dest_node_id;
			reply.page.header.request_id = request->prefix.request_id;
			reply.page.header.mxkey = request->prefix.mxkey;
			reply.page.header.wire_length
				= sizeof(ClusterCurrentMxDescribeReplyHeader);
			reply.header.checksum = cluster_gcs_block_compute_checksum(
				(const char *)&reply.page);
			reply_env = route_test_envelope(
				PGRAC_IC_MSG_GCS_BLOCK_REPLY, dest_node_id,
				(uint32)cluster_node_id, sizeof(reply));
			cluster_gcs_handle_block_reply_envelope(&reply_env, &reply);
		}
	}
	if (payload != NULL
		&& payload_len == sizeof(ClusterCurrentMxProofForwardV2)
		&& ((const ClusterCurrentMxProofForwardV2 *)payload)->prefix.kind
			   == GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF
		&& route_seam.current_mx_proof_reply_enabled
		&& route_seam.current_mx_slot_armed) {
		const ClusterCurrentMxProofForwardV2 *request = payload;
		typedef struct TestCurrentMxProofReply {
			GcsBlockReplyHeader header;
			ClusterCurrentMxProofReplyPage page;
		} TestCurrentMxProofReply;
		TestCurrentMxProofReply reply;
		ClusterICEnvelope reply_env;

		memset(&reply, 0, sizeof(reply));
		reply.header.request_id = request->prefix.request_id;
		reply.header.epoch = request->prefix.epoch;
		reply.header.sender_node = (int32)dest_node_id;
		reply.header.requester_backend_id
			= request->prefix.requester_backend_id;
		reply.header.status
			= (uint8)GCS_BLOCK_REPLY_CURRENT_MX_MEMBER_PROOF_RESULT;
		GcsBlockReplyHeaderSetForwardingMasterNode(
			&reply.header, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
		reply.page.header.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
		reply.page.header.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
		reply.page.header.kind
			= GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF;
		reply.page.header.result
			= (uint8)route_seam.current_mx_proof_validate_result;
		reply.page.header.source_node_id = dest_node_id;
		reply.page.header.request_id = request->prefix.request_id;
		reply.page.header.mxkey = request->prefix.mxkey;
		reply.page.header.descriptor_hash
			= ClusterCurrentMxProofPrefixGetDescriptorHash(&request->prefix);
		reply.page.header.total_count = request->prefix.total_count;
		reply.page.header.chunk_ordinal = request->prefix.chunk_ordinal;
		reply.page.header.chunk_count_minus_one
			= request->prefix.chunk_count_minus_one;
		reply.page.header.wire_length
			= sizeof(ClusterCurrentMxProofReplyHeader);
		reply.header.checksum = cluster_gcs_block_compute_checksum(
			(const char *)&reply.page);
		reply_env = route_test_envelope(
			PGRAC_IC_MSG_GCS_BLOCK_REPLY, dest_node_id,
			(uint32)cluster_node_id, sizeof(reply));
		cluster_gcs_handle_block_reply_envelope(&reply_env, &reply);
	}
	return route_seam.enqueue_ok;
}

ClusterMxDescribeResult
cluster_multixact_current_wire_validate_describe_reply(
	const void *payload, uint32 payload_length, int32 expected_source,
	uint64 current_epoch, uint64 expected_request_id,
	const ClusterCurrentMxKey *expected_key,
	ClusterCurrentMxMemberDesc *members pg_attribute_unused(),
	uint16 members_cap pg_attribute_unused(),
	uint16 *members_count, uint32 *reported_total_members)
{
	const ClusterCurrentMxDescribeReplyPage *page = payload;

	route_seam.current_mx_validate_calls++;
	if (members_count != NULL)
		*members_count = 0;
	if (reported_total_members != NULL)
		*reported_total_members = 0;
	if (payload == NULL || payload_length != sizeof(*page)
		|| expected_key == NULL
		|| page->header.magic != CLUSTER_CURRENT_MX_WIRE_MAGIC
		|| page->header.version != CLUSTER_CURRENT_MX_WIRE_VERSION
		|| page->header.kind != GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE
		|| page->header.source_node_id != (uint32)expected_source
		|| page->header.request_id != expected_request_id
		|| page->header.mxkey.origin_node_id
			   != expected_key->origin_node_id
		|| page->header.mxkey.multixact_id != expected_key->multixact_id
		|| page->header.mxkey.cluster_epoch != (uint32)current_epoch)
		return CMX_DESC_UNKNOWN;
	return route_seam.current_mx_validate_result;
}

bool
cluster_multixact_current_wire_validate_proof_forward(
	const void *payload, uint32 payload_length, int32 envelope_source,
	int32 local_node pg_attribute_unused(), uint64 current_epoch,
	ClusterCurrentMxProofForwardV2 *decoded)
{
	const ClusterCurrentMxProofForwardV2 *request = payload;

	if (decoded != NULL)
		memset(decoded, 0, sizeof(*decoded));
	if (request == NULL || decoded == NULL
		|| payload_length != sizeof(*request)
		|| request->prefix.request_id == 0
		|| request->prefix.epoch != current_epoch
		|| request->prefix.original_requester_node != envelope_source
		|| request->prefix.requester_backend_id <= 0
		|| request->prefix.kind
			   != GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF
		|| request->trailer.magic != CLUSTER_CURRENT_MX_WIRE_MAGIC
		|| request->trailer.version != CLUSTER_CURRENT_MX_WIRE_VERSION)
		return false;
	*decoded = *request;
	return true;
}

bool
cluster_multixact_current_wire_validate_proof_reply_frame(
	const void *payload, uint32 payload_length, int32 expected_source,
	uint64 current_epoch,
	const ClusterCurrentMxProofForwardV2 *expected_request,
	ClusterMxResolveResult *result,
	ClusterCurrentMemberProof *proofs pg_attribute_unused(),
	uint16 proofs_cap pg_attribute_unused(), uint16 *proof_count,
	ClusterCurrentUpdaterProof *updater_proof,
	uint32 *requester_capability_generation_out)
{
	const ClusterCurrentMxProofReplyPage *page = payload;

	route_seam.current_mx_proof_validate_calls++;
	if (result != NULL)
		*result = CMX_RESOLVE_UNKNOWN;
	if (proof_count != NULL)
		*proof_count = 0;
	if (updater_proof != NULL) {
		memset(updater_proof, 0, sizeof(*updater_proof));
		updater_proof->verdict = CUCP_UNKNOWN;
	}
	if (requester_capability_generation_out != NULL)
		*requester_capability_generation_out = 0;
	if (payload == NULL || payload_length != sizeof(*page)
		|| expected_request == NULL || result == NULL
		|| page->header.magic != CLUSTER_CURRENT_MX_WIRE_MAGIC
		|| page->header.version != CLUSTER_CURRENT_MX_WIRE_VERSION
		|| page->header.kind
			   != GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF
		|| page->header.source_node_id != (uint32)expected_source
		|| page->header.request_id != expected_request->prefix.request_id
		|| page->header.mxkey.origin_node_id
			   != expected_request->prefix.mxkey.origin_node_id
		|| page->header.mxkey.multixact_id
			   != expected_request->prefix.mxkey.multixact_id
		|| page->header.mxkey.cluster_epoch != (uint32)current_epoch)
		return false;
	*result = route_seam.current_mx_proof_validate_result;
	if (*result == CMX_RESOLVE_OK
		&& requester_capability_generation_out != NULL)
		*requester_capability_generation_out
			= page->header.requester_capability_generation;
	return true;
}

bool
cluster_lms_outbound_enqueue_zero_block_reply_cap_bound(
	int worker_id, uint32 dest_node_id, const GcsBlockReplyHeader *header,
	uint32 required_capability, uint32 connection_generation)
{
	route_seam.refusal_enqueue_calls++;
	route_seam.refusal_enqueue_sequence = ++route_seam.sequence;
	route_seam.refusal_enqueue_worker = worker_id;
	route_seam.refusal_enqueue_dest = dest_node_id;
	route_seam.refusal_enqueue_required_capability = required_capability;
	route_seam.refusal_enqueue_connection_generation = connection_generation;
	if (header != NULL)
		route_seam.refusal_header = *header;
	return route_seam.refusal_enqueue_ok;
}

static void
route_assert_refusal(GcsBlockReplyStatus status, uint64 page_lsn)
{
	int i;

	UT_ASSERT_EQ(route_seam.refusal_enqueue_calls, 1);
	UT_ASSERT_EQ(route_seam.refusal_enqueue_worker, 0);
	UT_ASSERT_EQ(route_seam.refusal_enqueue_dest, UT_REQUESTER_NODE);
	UT_ASSERT_EQ(route_seam.refusal_enqueue_required_capability, UT_R4_REQUIRED_CAPABILITIES);
	UT_ASSERT_EQ(route_seam.refusal_enqueue_connection_generation,
				 UT_REQUESTER_CAPABILITY_GENERATION);
	UT_ASSERT_EQ(route_seam.refusal_header.request_id, UT_REQUEST_ID);
	UT_ASSERT_EQ(route_seam.refusal_header.page_lsn, page_lsn);
	UT_ASSERT_EQ(route_seam.refusal_header.checksum, 0);
	UT_ASSERT_EQ(route_seam.refusal_header.sender_node, UT_MASTER_NODE);
	UT_ASSERT_EQ(route_seam.refusal_header.requester_backend_id, UT_REQUESTER_BACKEND);
	UT_ASSERT_EQ(route_seam.refusal_header.transition_id, PCM_TRANS_N_TO_S);
	UT_ASSERT_EQ(route_seam.refusal_header.status, status);
	UT_ASSERT_EQ(GcsBlockReplyHeaderGetForwardingMasterNode(&route_seam.refusal_header),
				 GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	for (i = 0; i < (int)sizeof(route_seam.refusal_header.reserved_0); i++)
		UT_ASSERT_EQ(route_seam.refusal_header.reserved_0[i], 0);
}

static void
route_assert_holder_refusal(GcsBlockReplyStatus status)
{
	int i;

	UT_ASSERT_EQ(route_seam.refusal_enqueue_calls, 1);
	UT_ASSERT_EQ(route_seam.refusal_enqueue_worker, 0);
	UT_ASSERT_EQ(route_seam.refusal_enqueue_dest, UT_REQUESTER_NODE);
	UT_ASSERT_EQ(route_seam.refusal_enqueue_required_capability,
				 UT_R4_REQUIRED_CAPABILITIES);
	UT_ASSERT_EQ(route_seam.refusal_enqueue_connection_generation,
				 UT_REQUESTER_CAPABILITY_GENERATION);
	UT_ASSERT_EQ(route_seam.refusal_header.request_id, UT_REQUEST_ID);
	UT_ASSERT_EQ(route_seam.refusal_header.epoch, UT_FORMATION_EPOCH);
	UT_ASSERT_EQ(route_seam.refusal_header.page_lsn, 0);
	UT_ASSERT_EQ(route_seam.refusal_header.checksum, 0);
	UT_ASSERT_EQ(route_seam.refusal_header.sender_node, UT_HOLDER_NODE);
	UT_ASSERT_EQ(route_seam.refusal_header.requester_backend_id, UT_REQUESTER_BACKEND);
	UT_ASSERT_EQ(route_seam.refusal_header.transition_id, PCM_TRANS_N_TO_S);
	UT_ASSERT_EQ(route_seam.refusal_header.status, status);
	UT_ASSERT_EQ(GcsBlockReplyHeaderGetForwardingMasterNode(&route_seam.refusal_header),
				 UT_MASTER_NODE);
	for (i = 0; i < (int)sizeof(route_seam.refusal_header.reserved_0); i++)
		UT_ASSERT_EQ(route_seam.refusal_header.reserved_0[i], 0);
}

ClusterCrBuildResult
cluster_lms_cr_submit_r4(const ClusterR4CrForwardPayload *forward,
						 const ClusterSemanticAdmissionToken *receive_admission,
						 uint32 requester_capability_generation,
						 uint32 master_capability_generation,
						 ClusterCrBuildReason *reason_out)
{
	route_seam.holder_submit_calls++;
	route_seam.holder_submit_sequence = ++route_seam.sequence;
	route_seam.submitted_admission_address = receive_admission;
	if (forward != NULL)
		route_seam.submitted_forward = *forward;
	if (receive_admission != NULL)
		route_seam.submitted_admission = *receive_admission;
	route_seam.submitted_requester_capability_generation
		= requester_capability_generation;
	route_seam.submitted_master_capability_generation = master_capability_generation;
	route_seam.submitted_reason_out_present = reason_out != NULL;
	if (reason_out != NULL)
		*reason_out = route_seam.holder_submit_reason;
	return route_seam.holder_submit_result;
}

static PcmAuthoritySnapshot
route_snapshot(PcmState state)
{
	PcmAuthoritySnapshot authority;

	memset(&authority, 0, sizeof(authority));
	authority.master_holder.node_id = UINT32_MAX;
	authority.transition_count = 7;
	authority.state = state;
	authority.x_holder_node = -1;
	authority.pending_x_requester_node = -1;
	return authority;
}

static ClusterCrBuildReason
classify(const PcmAuthoritySnapshot *authority, uint64 epoch, uint64 generation,
		 int32 *holder_out)
{
	return cluster_r4_route_policy_classify(authority, epoch, generation, holder_out);
}

#define DEFINE_REASON_TEST(test_name, setup_code, expected_reason, expected_holder)                \
	UT_TEST(test_name)                                                                             \
	{                                                                                              \
		PcmAuthoritySnapshot authority = route_snapshot(PCM_STATE_N);                               \
		PcmAuthoritySnapshot *authority_ptr = &authority;                                           \
		uint64 epoch = 9;                                                                           \
		uint64 generation = (epoch << 32) | 3;                                                      \
		int32 holder = -1;                                                                          \
		setup_code;                                                                                  \
		UT_ASSERT_EQ(classify(authority_ptr, epoch, generation, &holder), (expected_reason));        \
		UT_ASSERT_EQ(holder, (expected_holder));                                                     \
	}

DEFINE_REASON_TEST(test_01_null_authority_is_protocol, authority_ptr = NULL,
			   CLUSTER_CR_BUILD_PROTOCOL, -1)

UT_TEST(test_02_null_output_is_protocol)
{
	PcmAuthoritySnapshot authority = route_snapshot(PCM_STATE_N);

	UT_ASSERT_EQ(classify(&authority, 9, (UINT64_C(9) << 32) | 3, NULL),
				 CLUSTER_CR_BUILD_PROTOCOL);
}

DEFINE_REASON_TEST(test_03_canonical_n_has_no_holder, (void)0, CLUSTER_CR_BUILD_NO_HOLDER, -1)
DEFINE_REASON_TEST(test_04_n_with_x_is_ambiguous, authority.x_holder_node = 1,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_05_n_with_s_is_ambiguous, authority.s_holders_bitmap = 1,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_06_n_with_master_is_ambiguous, authority.master_holder.node_id = 1,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_07_x_node_zero_is_selected,
			   authority.state = PCM_STATE_X; authority.x_holder_node = 0;
			   authority.master_holder.node_id = 0,
			   CLUSTER_CR_BUILD_NONE, 0)
DEFINE_REASON_TEST(test_08_x_node_31_is_selected,
			   authority.state = PCM_STATE_X; authority.x_holder_node = 31;
			   authority.master_holder.node_id = 31,
			   CLUSTER_CR_BUILD_NONE, 31)
DEFINE_REASON_TEST(test_09_x_without_holder_is_ambiguous, authority.state = PCM_STATE_X,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_10_x_negative_holder_is_ambiguous,
			   authority.state = PCM_STATE_X; authority.x_holder_node = -2,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_11_x_out_of_range_is_ambiguous,
			   authority.state = PCM_STATE_X; authority.x_holder_node = 32;
			   authority.master_holder.node_id = 32,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_12_x_master_mismatch_is_ambiguous,
			   authority.state = PCM_STATE_X; authority.x_holder_node = 4;
			   authority.master_holder.node_id = 5,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_13_x_with_s_bitmap_is_ambiguous,
			   authority.state = PCM_STATE_X; authority.x_holder_node = 4;
			   authority.master_holder.node_id = 4; authority.s_holders_bitmap = 1,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_14_s_node_zero_is_selected,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 0;
			   authority.s_holders_bitmap = 1,
			   CLUSTER_CR_BUILD_NONE, 0)
DEFINE_REASON_TEST(test_15_s_node_31_is_selected,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 31;
			   authority.s_holders_bitmap = UINT32_C(1) << 31,
			   CLUSTER_CR_BUILD_NONE, 31)
DEFINE_REASON_TEST(test_16_s_multiple_selects_canonical,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 3;
			   authority.s_holders_bitmap = (UINT32_C(1) << 2) | (UINT32_C(1) << 3),
			   CLUSTER_CR_BUILD_NONE, 3)
DEFINE_REASON_TEST(test_17_s_zero_bitmap_is_ambiguous,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 3,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_18_s_with_x_is_ambiguous,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 3;
			   authority.s_holders_bitmap = UINT32_C(1) << 3; authority.x_holder_node = 3,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_19_s_without_master_is_ambiguous,
			   authority.state = PCM_STATE_S; authority.s_holders_bitmap = 1,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_20_s_out_of_range_master_is_ambiguous,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 33;
			   authority.s_holders_bitmap = 1,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_21_s_missing_canonical_bit_is_ambiguous,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 3;
			   authority.s_holders_bitmap = UINT32_C(1) << 2,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_22_unknown_state_is_ambiguous, authority.state = (PcmState)99,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_23_reserved_zero_is_required, authority.reserved[0] = 1,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_24_reserved_one_is_required, authority.reserved[1] = 1,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_25_pending_destructive_convert_is_recovering,
			   authority.pending_x_requester_node = 4,
			   CLUSTER_CR_BUILD_RECOVERING, -1)
DEFINE_REASON_TEST(test_26_zero_master_generation_is_rejected,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 0;
			   authority.s_holders_bitmap = 1; generation = 0,
			   CLUSTER_CR_BUILD_GENERATION_MISMATCH, -1)
DEFINE_REASON_TEST(test_27_zero_restart_half_is_rejected,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 0;
			   authority.s_holders_bitmap = 1; generation = epoch << 32,
			   CLUSTER_CR_BUILD_GENERATION_MISMATCH, -1)
DEFINE_REASON_TEST(test_28_wrong_epoch_half_is_rejected,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 0;
			   authority.s_holders_bitmap = 1; generation = ((epoch + 1) << 32) | 3,
			   CLUSTER_CR_BUILD_GENERATION_MISMATCH, -1)
DEFINE_REASON_TEST(test_29_zero_transition_count_is_rejected,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 0;
			   authority.s_holders_bitmap = 1; authority.transition_count = 0,
			   CLUSTER_CR_BUILD_GENERATION_MISMATCH, -1)
DEFINE_REASON_TEST(test_30_exhausted_transition_count_is_rejected,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 0;
			   authority.s_holders_bitmap = 1; authority.transition_count = UINT64_MAX,
			   CLUSTER_CR_BUILD_GENERATION_MISMATCH, -1)

static ClusterR4CrRouteProof
route_proof(void)
{
	ClusterR4CrRouteProof proof;

	memset(&proof, 0, sizeof(proof));
	proof.formation_epoch = 9;
	proof.master_authority_generation = (UINT64_C(9) << 32) | 3;
	proof.master_resource_transition_count = 7;
	proof.expected_page_scn = (SCN)11;
	proof.selected_holder_node = 4;
	return proof;
}

UT_TEST(test_31_exact_duplicate_route_matches)
{
	ClusterR4CrRouteProof proof = route_proof();

	UT_ASSERT(cluster_r4_route_proof_matches(&proof, 9, (UINT64_C(9) << 32) | 3, 4, 7,
										   (SCN)11));
}

UT_TEST(test_32_transition_drift_closes_duplicate)
{
	ClusterR4CrRouteProof proof = route_proof();

	UT_ASSERT(!cluster_r4_route_proof_matches(&proof, 9, (UINT64_C(9) << 32) | 3, 4, 8,
											(SCN)11));
}

static const ClusterCrBuildResult reason_result[18] = {
	[CLUSTER_CR_BUILD_NONE] = CLUSTER_CR_BUILD_FULL,
	[CLUSTER_CR_BUILD_TARGET_DISABLED] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_RF_DEFERRED] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_WRONG_MASTER] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_NO_HOLDER] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_HOLDER_MOVED] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_RECOVERING] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_GENERATION_MISMATCH] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_CAPACITY] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_BAD_LOCATOR] = CLUSTER_CR_BUILD_FAIL_CLOSED,
	[CLUSTER_CR_BUILD_BAD_UNDO] = CLUSTER_CR_BUILD_FAIL_CLOSED,
	[CLUSTER_CR_BUILD_CHAIN_LIMIT] = CLUSTER_CR_BUILD_FAIL_CLOSED,
	[CLUSTER_CR_BUILD_SNAPSHOT_TOO_OLD] = CLUSTER_CR_BUILD_FAIL_CLOSED,
	[CLUSTER_CR_BUILD_EPOCH_MISMATCH] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_CANCELLED] = CLUSTER_CR_BUILD_FAIL_CLOSED,
	[CLUSTER_CR_BUILD_IO_ERROR] = CLUSTER_CR_BUILD_FAIL_CLOSED,
	[CLUSTER_CR_BUILD_PROTOCOL] = CLUSTER_CR_BUILD_FAIL_CLOSED,
};

static void
run_reason_polarity(int reason)
{
	UT_ASSERT_EQ(cluster_cr_build_result_for_reason((ClusterCrBuildReason)reason),
				 reason_result[reason]);
}

#define DEFINE_POLARITY_TEST(n) \
	UT_TEST(test_reason_polarity_##n) { run_reason_polarity(n); }

DEFINE_POLARITY_TEST(0)
DEFINE_POLARITY_TEST(1)
DEFINE_POLARITY_TEST(2)
DEFINE_POLARITY_TEST(3)
DEFINE_POLARITY_TEST(4)
DEFINE_POLARITY_TEST(5)
DEFINE_POLARITY_TEST(6)
DEFINE_POLARITY_TEST(7)
DEFINE_POLARITY_TEST(8)
DEFINE_POLARITY_TEST(9)
DEFINE_POLARITY_TEST(10)
DEFINE_POLARITY_TEST(11)
DEFINE_POLARITY_TEST(12)
DEFINE_POLARITY_TEST(13)
DEFINE_POLARITY_TEST(14)
DEFINE_POLARITY_TEST(15)
DEFINE_POLARITY_TEST(16)
DEFINE_POLARITY_TEST(17)

UT_TEST(test_unknown_reason_fails_closed)
{
	UT_ASSERT_EQ(cluster_cr_build_result_for_reason((ClusterCrBuildReason)-1),
				 CLUSTER_CR_BUILD_FAIL_CLOSED);
	UT_ASSERT_EQ(cluster_cr_build_result_for_reason((ClusterCrBuildReason)18),
				 CLUSTER_CR_BUILD_FAIL_CLOSED);
}

UT_TEST(test_d3_result_reason_mapping_is_closed)
{
	static const GcsBlockReplyStatus expected[18] = {
		GCS_BLOCK_REPLY_R4_DENIED,
		GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED,
		GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED,
		GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED,
		GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED,
		GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED,
		GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED,
		GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED,
		GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED,
		GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED,
		GCS_BLOCK_REPLY_R4_DENIED,
		GCS_BLOCK_REPLY_R4_DENIED,
		GCS_BLOCK_REPLY_R4_DENIED,
		GCS_BLOCK_REPLY_R4_DENIED,
		GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED,
		GCS_BLOCK_REPLY_R4_DENIED,
		GCS_BLOCK_REPLY_R4_DENIED,
		GCS_BLOCK_REPLY_R4_DENIED
	};
	int reason;

	for (reason = CLUSTER_CR_BUILD_TARGET_DISABLED; reason <= CLUSTER_CR_BUILD_PROTOCOL;
		 reason++) {
		ClusterCrBuildResult result
			= cluster_cr_build_result_for_reason((ClusterCrBuildReason)reason);
		GcsBlockReplyStatus status = GCS_BLOCK_REPLY_GRANTED;

		UT_ASSERT(cluster_gcs_block_test_r4_refusal_status(
			result, (ClusterCrBuildReason)reason, false, &status));
		UT_ASSERT_EQ(status, expected[reason]);
	}
	{
		GcsBlockReplyStatus status = GCS_BLOCK_REPLY_GRANTED;

		UT_ASSERT(!cluster_gcs_block_test_r4_refusal_status(
			CLUSTER_CR_BUILD_FULL, CLUSTER_CR_BUILD_NONE, true, &status));
		UT_ASSERT(cluster_gcs_block_test_r4_refusal_status(
			CLUSTER_CR_BUILD_FULL, CLUSTER_CR_BUILD_NONE, false, &status));
		UT_ASSERT_EQ(status, GCS_BLOCK_REPLY_R4_DENIED);
		UT_ASSERT(cluster_gcs_block_test_r4_refusal_status(
			CLUSTER_CR_BUILD_FAIL_CLOSED, CLUSTER_CR_BUILD_HOLDER_MOVED, false, &status));
		UT_ASSERT_EQ(status, GCS_BLOCK_REPLY_R4_DENIED);
		UT_ASSERT(cluster_gcs_block_test_r4_refusal_status(
			(ClusterCrBuildResult)99, (ClusterCrBuildReason)99, false, &status));
		UT_ASSERT_EQ(status, GCS_BLOCK_REPLY_R4_DENIED);
	}
}

UT_TEST(test_r4_refusal_decoder_requires_exact_domain_identity_and_zero_body)
{
	typedef struct TestR4Reply {
		GcsBlockReplyHeader header;
		char block_data[GCS_BLOCK_DATA_SIZE];
	} TestR4Reply;
	TestR4Reply reply;
	ClusterICEnvelope env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY, UT_REQUESTER_NODE, UT_MASTER_NODE,
							  sizeof(reply));

	memset(&reply, 0, sizeof(reply));
	reply.header.request_id = UT_REQUEST_ID;
	reply.header.epoch = UT_FORMATION_EPOCH;
	reply.header.sender_node = UT_REQUESTER_NODE;
	reply.header.requester_backend_id = UT_REQUESTER_BACKEND;
	reply.header.transition_id = PCM_TRANS_N_TO_S;
	reply.header.status = GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED;
	reply.header.page_lsn = 1;
	GcsBlockReplyHeaderSetForwardingMasterNode(
		&reply.header, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	reply.header.checksum = cluster_gcs_block_compute_checksum(reply.block_data);
	UT_ASSERT(cluster_gcs_block_test_decode_r4_reply(
		&env, &reply, UT_REQUEST_ID, UT_FORMATION_EPOCH, UT_REQUESTER_BACKEND,
		PCM_TRANS_N_TO_S, UT_REQUESTER_NODE, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER,
		UT_REPLY_DOMAIN_R4_CR));

	/* Only the D3 master-refusal role may carry WRONG_MASTER in page_lsn.
	 * A holder refusal names the real master in forwarding_master_node and
	 * therefore requires page_lsn=0. */
	env.source_node_id = UT_HOLDER_NODE;
	reply.header.sender_node = UT_HOLDER_NODE;
	GcsBlockReplyHeaderSetForwardingMasterNode(&reply.header, UT_MASTER_NODE);
	UT_ASSERT(!cluster_gcs_block_test_decode_r4_reply(
		&env, &reply, UT_REQUEST_ID, UT_FORMATION_EPOCH, UT_REQUESTER_BACKEND,
		PCM_TRANS_N_TO_S, UT_HOLDER_NODE, UT_MASTER_NODE,
		UT_REPLY_DOMAIN_R4_CR));
	env.source_node_id = UT_REQUESTER_NODE;
	reply.header.sender_node = UT_REQUESTER_NODE;
	GcsBlockReplyHeaderSetForwardingMasterNode(
		&reply.header, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);

	reply.header.status = GCS_BLOCK_REPLY_DENIED_PENDING_X;
	UT_ASSERT(!cluster_gcs_block_test_decode_r4_reply(
		&env, &reply, UT_REQUEST_ID, UT_FORMATION_EPOCH, UT_REQUESTER_BACKEND,
		PCM_TRANS_N_TO_S, UT_REQUESTER_NODE, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER,
		UT_REPLY_DOMAIN_R4_CR));
	reply.header.status = GCS_BLOCK_REPLY_R4_DENIED;
	reply.header.page_lsn = 1;
	UT_ASSERT(!cluster_gcs_block_test_decode_r4_reply(
		&env, &reply, UT_REQUEST_ID, UT_FORMATION_EPOCH, UT_REQUESTER_BACKEND,
		PCM_TRANS_N_TO_S, UT_REQUESTER_NODE, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER,
		UT_REPLY_DOMAIN_R4_CR));
	reply.header.page_lsn = 0;
	reply.header.reserved_0[0] = 1;
	UT_ASSERT(!cluster_gcs_block_test_decode_r4_reply(
		&env, &reply, UT_REQUEST_ID, UT_FORMATION_EPOCH, UT_REQUESTER_BACKEND,
		PCM_TRANS_N_TO_S, UT_REQUESTER_NODE, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER,
		UT_REPLY_DOMAIN_R4_CR));
	reply.header.reserved_0[0] = 0;
	reply.block_data[0] = 1;
	reply.header.checksum = cluster_gcs_block_compute_checksum(reply.block_data);
	UT_ASSERT(!cluster_gcs_block_test_decode_r4_reply(
		&env, &reply, UT_REQUEST_ID, UT_FORMATION_EPOCH, UT_REQUESTER_BACKEND,
		PCM_TRANS_N_TO_S, UT_REQUESTER_NODE, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER,
		UT_REPLY_DOMAIN_R4_CR));
}

/* Removing the expected R4 domain gate, accepting a status at the other
 * physical length, or treating status 24 as an ordinary backend reply makes
 * at least one row below fail.  The fixtures use literal 8240/8256 shapes and
 * call the production decoder seam, not the production encoder. */
UT_TEST(test_r4_reply_decoder_binds_domain_status_length_and_undo_authority)
{
	typedef struct TestR4Reply8240 {
		GcsBlockReplyHeader header;
		char block_data[GCS_BLOCK_DATA_SIZE];
	} TestR4Reply8240;
	typedef struct TestR4Reply8256 {
		TestR4Reply8240 base;
		ClusterGcsUndoAuthTrailer auth;
	} TestR4Reply8256;
	TestR4Reply8240 reply8240;
	TestR4Reply8256 reply8256;
	ClusterICEnvelope env;

	UT_ASSERT_EQ(sizeof(reply8240), 8240);
	UT_ASSERT_EQ(sizeof(reply8256), 8256);

	/* A holder's finished CR is an exact 8240-byte R4_CR-domain reply. */
	memset(&reply8240, 0, sizeof(reply8240));
	reply8240.header.request_id = UT_REQUEST_ID;
	reply8240.header.epoch = UT_FORMATION_EPOCH;
	reply8240.header.sender_node = UT_HOLDER_NODE;
	reply8240.header.requester_backend_id = UT_REQUESTER_BACKEND;
	reply8240.header.transition_id = PCM_TRANS_N_TO_S;
	reply8240.header.status = GCS_BLOCK_REPLY_R4_CR_FULL;
	GcsBlockReplyHeaderSetForwardingMasterNode(&reply8240.header, UT_MASTER_NODE);
	reply8240.block_data[0] = 0x5a;
	reply8240.header.checksum = cluster_gcs_block_compute_checksum(reply8240.block_data);
	env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY, UT_HOLDER_NODE, cluster_node_id,
						  sizeof(reply8240));
	UT_ASSERT(cluster_gcs_block_test_decode_r4_reply(
		&env, &reply8240, UT_REQUEST_ID, UT_FORMATION_EPOCH, UT_REQUESTER_BACKEND,
		PCM_TRANS_N_TO_S, UT_HOLDER_NODE, UT_MASTER_NODE, UT_REPLY_DOMAIN_R4_CR));
	UT_ASSERT(!cluster_gcs_block_test_decode_r4_reply(
		&env, &reply8240, UT_REQUEST_ID, UT_FORMATION_EPOCH, UT_REQUESTER_BACKEND,
		PCM_TRANS_N_TO_S, UT_HOLDER_NODE, UT_MASTER_NODE,
		UT_REPLY_DOMAIN_LEGACY_ACQUIRE));
	UT_ASSERT(!cluster_gcs_block_test_decode_r4_reply(
		&env, &reply8240, UT_REQUEST_ID, UT_FORMATION_EPOCH, UT_REQUESTER_BACKEND,
		PCM_TRANS_N_TO_S, UT_HOLDER_NODE, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER,
		UT_REPLY_DOMAIN_R4_CR));

	/* The same status must not alias the authenticated 8256-byte undo shape. */
	memset(&reply8256, 0, sizeof(reply8256));
	reply8256.base = reply8240;
	env.payload_length = sizeof(reply8256);
	UT_ASSERT(!cluster_gcs_block_test_decode_r4_reply(
		&env, &reply8256, UT_REQUEST_ID, UT_FORMATION_EPOCH, UT_REQUESTER_BACKEND,
		PCM_TRANS_N_TO_S, UT_HOLDER_NODE, UT_MASTER_NODE, UT_REPLY_DOMAIN_R4_CR));

	/* Status 24 is the endpoint -2 internal result.  Its reply supplies the
	 * first nonzero live-HWM/TT-generation/authority-SCN co-sample. */
	memset(&reply8256, 0, sizeof(reply8256));
	reply8256.base.header.request_id = UT_REQUEST_ID;
	reply8256.base.header.epoch = UT_FORMATION_EPOCH;
	reply8256.base.header.sender_node = UT_HOLDER_NODE;
	reply8256.base.header.requester_backend_id = UT_R4_INTERNAL_ENDPOINT;
	reply8256.base.header.transition_id = PCM_TRANS_N_TO_S;
	reply8256.base.header.status = GCS_BLOCK_REPLY_R4_UNDO_DATA_RESULT;
	reply8256.base.header.page_lsn = UINT64_C(0x4000);
	UT_ASSERT(GcsBlockReplyHeaderSetR4UndoGeneration(
		&reply8256.base.header, UINT32_C(0x01020304)));
	GcsBlockReplyHeaderSetForwardingMasterNode(
		&reply8256.base.header, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	reply8256.base.block_data[0] = 0xa5;
	reply8256.base.header.checksum
		= cluster_gcs_block_compute_checksum(reply8256.base.block_data);
	ClusterGcsUndoAuthTrailerSetTtGeneration(&reply8256.auth, UINT64_C(3));
	ClusterGcsUndoAuthTrailerSetAuthorityScn(&reply8256.auth, (uint64)UT_READ_SCN);
	env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY, UT_HOLDER_NODE, cluster_node_id,
						  sizeof(reply8256));
	UT_ASSERT(cluster_gcs_block_test_decode_r4_reply(
		&env, &reply8256, UT_REQUEST_ID, UT_FORMATION_EPOCH, UT_R4_INTERNAL_ENDPOINT,
		PCM_TRANS_N_TO_S, UT_HOLDER_NODE, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER,
		UT_REPLY_DOMAIN_R4_CR));
	UT_ASSERT(!cluster_gcs_block_test_decode_r4_reply(
		&env, &reply8256, UT_REQUEST_ID, UT_FORMATION_EPOCH, UT_R4_INTERNAL_ENDPOINT,
		PCM_TRANS_N_TO_S, UT_HOLDER_NODE, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER,
		UT_REPLY_DOMAIN_LEGACY_ACQUIRE));
	reply8256.base.header.reserved_0[4] = 1;
	UT_ASSERT(!cluster_gcs_block_test_decode_r4_reply(
		&env, &reply8256, UT_REQUEST_ID, UT_FORMATION_EPOCH, UT_R4_INTERNAL_ENDPOINT,
		PCM_TRANS_N_TO_S, UT_HOLDER_NODE, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER,
		UT_REPLY_DOMAIN_R4_CR));
	reply8256.base.header.reserved_0[4] = 0;
	UT_ASSERT(GcsBlockReplyHeaderSetR4UndoGeneration(
		&reply8256.base.header, UINT32_MAX) == false);
	memset(reply8256.base.header.reserved_0, 0xff, 4);
	UT_ASSERT(!cluster_gcs_block_test_decode_r4_reply(
		&env, &reply8256, UT_REQUEST_ID, UT_FORMATION_EPOCH, UT_R4_INTERNAL_ENDPOINT,
		PCM_TRANS_N_TO_S, UT_HOLDER_NODE, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER,
		UT_REPLY_DOMAIN_R4_CR));
	UT_ASSERT(GcsBlockReplyHeaderSetR4UndoGeneration(
		&reply8256.base.header, UINT32_C(0x01020304)));

	/* A short frame is not status 24. Initial rollover generation zero is
	 * valid when the complete authenticated authority trailer is present. */
	env.payload_length = sizeof(reply8240);
	UT_ASSERT(!cluster_gcs_block_test_decode_r4_reply(
		&env, &reply8256, UT_REQUEST_ID, UT_FORMATION_EPOCH, UT_R4_INTERNAL_ENDPOINT,
		PCM_TRANS_N_TO_S, UT_HOLDER_NODE, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER,
		UT_REPLY_DOMAIN_R4_CR));
	env.payload_length = sizeof(reply8256);
	ClusterGcsUndoAuthTrailerSetTtGeneration(&reply8256.auth, 0);
	UT_ASSERT(cluster_gcs_block_test_decode_r4_reply(
		&env, &reply8256, UT_REQUEST_ID, UT_FORMATION_EPOCH, UT_R4_INTERNAL_ENDPOINT,
		PCM_TRANS_N_TO_S, UT_HOLDER_NODE, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER,
		UT_REPLY_DOMAIN_R4_CR));
	ClusterGcsUndoAuthTrailerSetTtGeneration(&reply8256.auth, UINT64_C(3));
	ClusterGcsUndoAuthTrailerSetAuthorityScn(&reply8256.auth, 0);
	UT_ASSERT(!cluster_gcs_block_test_decode_r4_reply(
		&env, &reply8256, UT_REQUEST_ID, UT_FORMATION_EPOCH, UT_R4_INTERNAL_ENDPOINT,
		PCM_TRANS_N_TO_S, UT_HOLDER_NODE, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER,
		UT_REPLY_DOMAIN_R4_CR));

	/* The two existing typed refusal statuses retain the exact 8240 shape. */
	memset(&reply8240, 0, sizeof(reply8240));
	reply8240.header.request_id = UT_REQUEST_ID;
	reply8240.header.epoch = UT_FORMATION_EPOCH;
	reply8240.header.sender_node = UT_MASTER_NODE;
	reply8240.header.requester_backend_id = UT_REQUESTER_BACKEND;
	reply8240.header.transition_id = PCM_TRANS_N_TO_S;
	GcsBlockReplyHeaderSetForwardingMasterNode(
		&reply8240.header, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	reply8240.header.checksum = cluster_gcs_block_compute_checksum(reply8240.block_data);
	env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY, UT_MASTER_NODE, cluster_node_id,
						  sizeof(reply8240));
	reply8240.header.status = GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED;
	UT_ASSERT(cluster_gcs_block_test_decode_r4_reply(
		&env, &reply8240, UT_REQUEST_ID, UT_FORMATION_EPOCH, UT_REQUESTER_BACKEND,
		PCM_TRANS_N_TO_S, UT_MASTER_NODE, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER,
		UT_REPLY_DOMAIN_R4_CR));
	reply8240.header.status = GCS_BLOCK_REPLY_R4_DENIED;
	UT_ASSERT(cluster_gcs_block_test_decode_r4_reply(
		&env, &reply8240, UT_REQUEST_ID, UT_FORMATION_EPOCH, UT_REQUESTER_BACKEND,
		PCM_TRANS_N_TO_S, UT_MASTER_NODE, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER,
		UT_REPLY_DOMAIN_R4_CR));
}

/* Removing the status-24 internal landing call, or deriving a backend index
 * before it, makes the positive row fail.  The malformed rows prove that the
 * handler does not widen this endpoint beyond its exact authenticated shape. */
UT_TEST(test_r4_status24_routes_only_to_internal_foreign_undo_landing)
{
	typedef struct TestR4Reply8256 {
		GcsBlockReplyHeader header;
		char block_data[GCS_BLOCK_DATA_SIZE];
		ClusterGcsUndoAuthTrailer auth;
	} TestR4Reply8256;
	TestR4Reply8256 reply;
	ClusterICEnvelope env;

	memset(&reply, 0, sizeof(reply));
	reply.header.request_id = UT_REQUEST_ID;
	reply.header.epoch = UT_FORMATION_EPOCH;
	reply.header.page_lsn = UINT64_C(0x4000);
	reply.header.sender_node = UT_HOLDER_NODE;
	reply.header.requester_backend_id = UT_R4_INTERNAL_ENDPOINT;
	reply.header.transition_id = PCM_TRANS_N_TO_S;
	reply.header.status = GCS_BLOCK_REPLY_R4_UNDO_DATA_RESULT;
	GcsBlockReplyHeaderSetForwardingMasterNode(
		&reply.header, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	UT_ASSERT(GcsBlockReplyHeaderSetR4UndoGeneration(
		&reply.header, UINT32_C(0x01020304)));
	memset(reply.block_data, 0xa5, sizeof(reply.block_data));
	reply.header.checksum = cluster_gcs_block_compute_checksum(reply.block_data);
	ClusterGcsUndoAuthTrailerSetTtGeneration(&reply.auth, UINT64_C(3));
	ClusterGcsUndoAuthTrailerSetAuthorityScn(&reply.auth, (uint64)UT_READ_SCN);
	env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY, UT_HOLDER_NODE,
						  cluster_node_id, sizeof(reply));

	route_seam_reset();
	reply_lock_acquire_calls = 0;
	cluster_gcs_handle_block_reply_envelope(&env, &reply);
	UT_ASSERT_EQ(route_seam.foreign_undo_land_calls, 1);
	UT_ASSERT_EQ(reply_lock_acquire_calls, 0);
	UT_ASSERT_EQ(memcmp(&route_seam.foreign_undo_env, &env, sizeof(env)), 0);
	UT_ASSERT_EQ(memcmp(&route_seam.foreign_undo_header, &reply.header,
					 sizeof(reply.header)), 0);
	UT_ASSERT_EQ(memcmp(route_seam.foreign_undo_page, reply.block_data,
					 sizeof(reply.block_data)), 0);
	UT_ASSERT_EQ(memcmp(&route_seam.foreign_undo_auth, &reply.auth,
					 sizeof(reply.auth)), 0);

	route_seam_reset();
	reply.header.requester_backend_id = UT_REQUESTER_BACKEND;
	cluster_gcs_handle_block_reply_envelope(&env, &reply);
	UT_ASSERT_EQ(route_seam.foreign_undo_land_calls, 0);

	route_seam_reset();
	reply.header.requester_backend_id = UT_R4_INTERNAL_ENDPOINT;
	env.payload_length--;
	cluster_gcs_handle_block_reply_envelope(&env, &reply);
	UT_ASSERT_EQ(route_seam.foreign_undo_land_calls, 0);

	route_seam_reset();
	env.payload_length = sizeof(reply);
	env.source_node_id = UT_MASTER_NODE;
	cluster_gcs_handle_block_reply_envelope(&env, &reply);
	UT_ASSERT_EQ(route_seam.foreign_undo_land_calls, 0);
	UT_ASSERT_EQ(reply_lock_acquire_calls, 0);
}

/* Put the one advertised payload byte immediately before a protected page.
 * A decoder that samples any header field before checking the minimum header
 * length faults here instead of returning a typed rejection. */
UT_TEST(test_r4_reply_decoder_checks_minimum_length_before_header_read)
{
	long page_size = sysconf(_SC_PAGESIZE);
	char *mapping;
	const void *truncated_payload;
	ClusterICEnvelope env;
	int rc;

	UT_ASSERT(page_size > 0);
	if (page_size <= 0)
		return;
	mapping = mmap(NULL, (size_t)page_size * 2, PROT_READ | PROT_WRITE,
				   MAP_PRIVATE | MAP_ANON, -1, 0);
	UT_ASSERT(mapping != MAP_FAILED);
	if (mapping == MAP_FAILED)
		return;
	rc = mprotect(mapping + page_size, (size_t)page_size, PROT_NONE);
	UT_ASSERT_EQ(rc, 0);
	if (rc != 0) {
		(void)munmap(mapping, (size_t)page_size * 2);
		return;
	}

	truncated_payload = mapping + page_size - 1;
	mapping[page_size - 1] = 0;
	env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY, UT_MASTER_NODE,
						  cluster_node_id, 1);
	UT_ASSERT(!cluster_gcs_block_test_decode_r4_reply(
		&env, truncated_payload, UT_REQUEST_ID, UT_FORMATION_EPOCH, UT_REQUESTER_BACKEND,
		PCM_TRANS_N_TO_S, UT_MASTER_NODE, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER,
		UT_REPLY_DOMAIN_R4_CR));

	rc = mprotect(mapping + page_size, (size_t)page_size, PROT_READ | PROT_WRITE);
	UT_ASSERT_EQ(rc, 0);
	rc = munmap(mapping, (size_t)page_size * 2);
	UT_ASSERT_EQ(rc, 0);
}

/* The registered reply handler must select the R4 decoder from the armed
 * slot domain before the legacy 8240-byte decoder rejects status 21.  Once
 * landed, an authenticated duplicate is first-reply-wins and cannot tear the
 * page already visible to the waiting backend. */
UT_TEST(test_r4_full_reply_lands_once_in_armed_r4_slot)
{
	typedef struct TestR4Reply8240 {
		GcsBlockReplyHeader header;
		char block_data[GCS_BLOCK_DATA_SIZE];
	} TestR4Reply8240;
	TestR4Reply8240 first;
	TestR4Reply8240 duplicate;
	ClusterICEnvelope env;
	GcsBlockReplyHeader landed_header;
	char landed_block[GCS_BLOCK_DATA_SIZE];
	bool reply_received = false;
	uint64 stale_drop_count = 0;

	memset(&first, 0, sizeof(first));
	first.header.request_id = UT_REQUEST_ID;
	first.header.epoch = UT_FORMATION_EPOCH;
	first.header.page_lsn = UINT64_C(0x12345678);
	first.header.sender_node = UT_HOLDER_NODE;
	first.header.requester_backend_id = 1;
	first.header.transition_id = PCM_TRANS_N_TO_S;
	first.header.status = GCS_BLOCK_REPLY_R4_CR_FULL;
	GcsBlockReplyHeaderSetForwardingMasterNode(&first.header, UT_MASTER_NODE);
	memset(first.block_data, 0x5a, sizeof(first.block_data));
	first.header.checksum = cluster_gcs_block_compute_checksum(first.block_data);
	env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY, UT_HOLDER_NODE,
						  cluster_node_id, sizeof(first));

	reply_lock_acquire_calls = 0;
	reply_lock_release_calls = 0;
	reply_cv_signal_calls = 0;
	UT_ASSERT(cluster_gcs_block_test_arm_r4_reply_slot(
		UT_REQUEST_ID, UT_FORMATION_EPOCH, 1, PCM_TRANS_N_TO_S, UT_MASTER_NODE));
	cluster_gcs_handle_block_reply_envelope(&env, &first);
	UT_ASSERT(cluster_gcs_block_test_snapshot_r4_reply_slot(
		&landed_header, landed_block, &reply_received, &stale_drop_count));
	UT_ASSERT(reply_received);
	UT_ASSERT_EQ(memcmp(&landed_header, &first.header, sizeof(first.header)), 0);
	UT_ASSERT_EQ(memcmp(landed_block, first.block_data, sizeof(landed_block)), 0);
	UT_ASSERT_EQ(reply_lock_acquire_calls, 1);
	UT_ASSERT_EQ(reply_lock_release_calls, 1);
	UT_ASSERT_EQ(reply_cv_signal_calls, 1);
	UT_ASSERT_EQ(stale_drop_count, 0);

	duplicate = first;
	duplicate.header.page_lsn++;
	memset(duplicate.block_data, 0xa5, sizeof(duplicate.block_data));
	duplicate.header.checksum
		= cluster_gcs_block_compute_checksum(duplicate.block_data);
	cluster_gcs_handle_block_reply_envelope(&env, &duplicate);
	UT_ASSERT(cluster_gcs_block_test_snapshot_r4_reply_slot(
		&landed_header, landed_block, &reply_received, &stale_drop_count));
	UT_ASSERT(reply_received);
	UT_ASSERT_EQ(memcmp(&landed_header, &first.header, sizeof(first.header)), 0);
	UT_ASSERT_EQ(memcmp(landed_block, first.block_data, sizeof(landed_block)), 0);
	UT_ASSERT_EQ(reply_lock_acquire_calls, 2);
	UT_ASSERT_EQ(reply_lock_release_calls, 2);
	UT_ASSERT_EQ(reply_cv_signal_calls, 1);
	UT_ASSERT_EQ(stale_drop_count, 1);
}

/* A holder-side conditional BufferContent miss is pre-mutation backpressure.
 * Its exact forwarded identity must reach the requester so the existing
 * DENIED_PENDING_X boundary can abort GRANT_PENDING and mint a fresh request;
 * treating it as an unauthorized holder status loses the only reply and makes
 * a same-id retransmit observe an unrelated terminal master refusal. */
UT_TEST(test_forwarded_holder_pending_x_lands_in_legacy_slot)
{
	typedef struct TestLegacyReply8240 {
		GcsBlockReplyHeader header;
		char block_data[GCS_BLOCK_DATA_SIZE];
	} TestLegacyReply8240;
	TestLegacyReply8240 reply;
	ClusterICEnvelope env;
	GcsBlockReplyHeader landed_header;
	char landed_block[GCS_BLOCK_DATA_SIZE];
	bool reply_received = false;
	uint64 stale_drop_count = 0;

	memset(&reply, 0, sizeof(reply));
	reply.header.request_id = UT_REQUEST_ID;
	reply.header.epoch = UT_FORMATION_EPOCH;
	reply.header.sender_node = UT_HOLDER_NODE;
	reply.header.requester_backend_id = 1;
	reply.header.transition_id = PCM_TRANS_N_TO_S;
	reply.header.status = GCS_BLOCK_REPLY_DENIED_PENDING_X;
	GcsBlockReplyHeaderSetForwardingMasterNode(&reply.header, UT_MASTER_NODE);
	reply.header.checksum = cluster_gcs_block_compute_checksum(reply.block_data);
	env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY, UT_HOLDER_NODE,
						  cluster_node_id, sizeof(reply));

	reply_lock_acquire_calls = 0;
	reply_lock_release_calls = 0;
	reply_cv_signal_calls = 0;
	UT_ASSERT(cluster_gcs_block_test_arm_legacy_reply_slot(
		UT_REQUEST_ID, UT_FORMATION_EPOCH, 1, PCM_TRANS_N_TO_S,
		UT_MASTER_NODE));
	cluster_gcs_handle_block_reply_envelope(&env, &reply);
	UT_ASSERT(cluster_gcs_block_test_snapshot_r4_reply_slot(
		&landed_header, landed_block, &reply_received, &stale_drop_count));
	UT_ASSERT(reply_received);
	UT_ASSERT_EQ(memcmp(&landed_header, &reply.header, sizeof(reply.header)), 0);
	UT_ASSERT_EQ(reply_lock_acquire_calls, 1);
	UT_ASSERT_EQ(reply_lock_release_calls, 1);
	UT_ASSERT_EQ(reply_cv_signal_calls, 1);
	UT_ASSERT_EQ(stale_drop_count, 0);
}

/* The requester arm consumes one exact sequence only as it publishes a free
 * slot in the R4 domain.  It must never inherit the legacy direct-land path,
 * and the existing slot owner must canonicalize byte 1 on release. */
UT_TEST(test_r4_requester_arm_selects_closed_domain_and_releases_cleanly)
{
	BufferTag tag = route_test_tag();
	BufferTag armed_tag;
	uint64 request_id = 0;
	uint64 armed_request_id = 0;
	uint64 armed_epoch = 0;
	int32 armed_master = -1;
	uint8 reply_domain = UINT8_MAX;
	uint8 transition_id = UINT8_MAX;
	ClusterGcsBlockDirectState direct_state = GCS_BLOCK_DIRECT_ABORTED;
	bool in_use = false;
	bool direct_target_prepared = true;

	UT_ASSERT(cluster_gcs_block_test_r4_requester_arm(
		tag, UT_FORMATION_EPOCH, UT_MASTER_NODE, UINT64_C(1), &request_id));
	UT_ASSERT_EQ(request_id,
				 gcs_reqid_requester(cluster_node_id, (int)MyBackendId - 1, UINT64_C(1)));
	UT_ASSERT(cluster_gcs_block_test_snapshot_r4_requester_slot(
		&in_use, &reply_domain, &armed_request_id, &transition_id, &armed_tag,
		&armed_epoch, &armed_master, &direct_state, &direct_target_prepared));
	UT_ASSERT(in_use);
	UT_ASSERT_EQ(reply_domain, UT_REPLY_DOMAIN_R4_CR);
	UT_ASSERT_EQ(armed_request_id, request_id);
	UT_ASSERT_EQ(transition_id, PCM_TRANS_N_TO_S);
	UT_ASSERT_EQ(memcmp(&armed_tag, &tag, sizeof(tag)), 0);
	UT_ASSERT_EQ(armed_epoch, UT_FORMATION_EPOCH);
	UT_ASSERT_EQ(armed_master, UT_MASTER_NODE);
	UT_ASSERT_EQ(direct_state, GCS_BLOCK_DIRECT_UNARMED);
	UT_ASSERT(!direct_target_prepared);

	UT_ASSERT(cluster_gcs_block_test_release_r4_requester_slot());
	UT_ASSERT(cluster_gcs_block_test_snapshot_r4_requester_slot(
		&in_use, &reply_domain, &armed_request_id, &transition_id, &armed_tag,
		&armed_epoch, &armed_master, &direct_state, &direct_target_prepared));
	UT_ASSERT(!in_use);
	UT_ASSERT_EQ(reply_domain, UT_REPLY_DOMAIN_LEGACY_ACQUIRE);
	UT_ASSERT_EQ(armed_request_id, 0);
	UT_ASSERT_EQ(armed_epoch, 0);
	UT_ASSERT_EQ(armed_master, -1);
	UT_ASSERT_EQ(direct_state, GCS_BLOCK_DIRECT_UNARMED);
	UT_ASSERT(!direct_target_prepared);
}

UT_TEST(test_current_mx_describe_requester_is_capability_bound_and_times_out_cleanly)
{
	ClusterCurrentMxKey key;
	ClusterCurrentMxMemberDesc members[2];
	const ClusterCurrentMxDescribeForwardV2 *request;
	BufferTag reset_tag = route_test_tag();
	BufferTag released_tag;
	uint16 members_count = UINT16_MAX;
	uint32 reported_total = UINT32_MAX;
	uint64 reset_request_id = 0;
	uint64 released_request_id = UINT64_MAX;
	uint64 released_epoch = UINT64_MAX;
	int32 released_master = INT32_MIN;
	uint8 released_domain = UINT8_MAX;
	uint8 released_transition = UINT8_MAX;
	ClusterGcsBlockDirectState released_direct_state
		= GCS_BLOCK_DIRECT_ABORTED;
	bool released_in_use = true;
	bool released_direct_target_prepared = true;
	int saved_node_id = cluster_node_id;
	int saved_reply_timeout_ms = cluster_gcs_reply_timeout_ms;

	memset(&key, 0, sizeof(key));
	key.origin_node_id = UT_MASTER_NODE;
	key.multixact_id = (MultiXactId)42;
	key.cluster_epoch = (uint32)UT_FORMATION_EPOCH;

	cluster_node_id = UT_REQUESTER_NODE;
	cluster_gcs_reply_timeout_ms = 1;
	route_seam_reset();
	route_seam.current_mx_capability_ok = false;
	memset(members, 0xa5, sizeof(members));
	UT_ASSERT_EQ(cluster_gcs_current_mx_describe_fetch_and_wait(
					 UT_MASTER_NODE, &key, members, lengthof(members),
					 &members_count, &reported_total),
			 CMX_DESC_UNKNOWN);
	UT_ASSERT_EQ(route_seam.current_mx_capability_calls, 1);
	UT_ASSERT_EQ(route_seam.current_mx_capability_peer, UT_MASTER_NODE);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
	UT_ASSERT_EQ(members_count, 0);
	UT_ASSERT_EQ(reported_total, (uint32)0);
	UT_ASSERT_EQ(memcmp(members,
					  (ClusterCurrentMxMemberDesc[lengthof(members)]){{0}},
					  sizeof(members)),
			 0);

	/* Seed the single-backend product fixture, then let the real Current-MX
	 * requester own and release the same slot. */
	UT_ASSERT(cluster_gcs_block_test_r4_requester_arm(
		reset_tag, UT_FORMATION_EPOCH, UT_MASTER_NODE, UINT64_C(1),
		&reset_request_id));
	UT_ASSERT(cluster_gcs_block_test_release_r4_requester_slot());
	route_seam_reset();
	members_count = UINT16_MAX;
	reported_total = UINT32_MAX;
	memset(members, 0xa5, sizeof(members));
	UT_ASSERT_EQ(cluster_gcs_current_mx_describe_fetch_and_wait(
					 UT_MASTER_NODE, &key, members, lengthof(members),
					 &members_count, &reported_total),
			 CMX_DESC_TIMEOUT);
	UT_ASSERT_EQ(route_seam.current_mx_capability_calls, 1);
	UT_ASSERT_EQ(route_seam.current_mx_capability_peer, UT_MASTER_NODE);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 1);
	UT_ASSERT_EQ(route_seam.enqueue_msg_type, PGRAC_IC_MSG_GCS_BLOCK_FORWARD);
	UT_ASSERT_EQ(route_seam.enqueue_dest, (uint32)UT_MASTER_NODE);
	UT_ASSERT_EQ(route_seam.enqueue_payload_len,
				 sizeof(ClusterCurrentMxDescribeForwardV2));
	UT_ASSERT_EQ(route_seam.enqueue_required_capability,
				 PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1);
	UT_ASSERT_EQ(route_seam.enqueue_connection_generation,
				 UT_CURRENT_MX_CAPABILITY_GENERATION);
	UT_ASSERT(route_seam.current_mx_slot_armed);
	UT_ASSERT_EQ(route_seam.current_mx_slot_domain,
				 UT_REPLY_DOMAIN_CURRENT_MX);

	request = (const ClusterCurrentMxDescribeForwardV2 *)route_seam.enqueue_payload;
	UT_ASSERT_EQ(request->prefix.epoch, UT_FORMATION_EPOCH);
	UT_ASSERT_EQ(request->prefix.mxkey.origin_node_id, UT_MASTER_NODE);
	UT_ASSERT_EQ(request->prefix.mxkey.multixact_id, (MultiXactId)42);
	UT_ASSERT_EQ(request->prefix.original_requester_node, UT_REQUESTER_NODE);
	UT_ASSERT_EQ(request->prefix.requester_backend_id, (int32)MyBackendId);
	UT_ASSERT_EQ(request->prefix.kind,
				 GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE);
	UT_ASSERT_EQ(request->trailer.magic, CLUSTER_CURRENT_MX_WIRE_MAGIC);
	UT_ASSERT_EQ(request->trailer.version, CLUSTER_CURRENT_MX_WIRE_VERSION);
	UT_ASSERT_EQ(request->trailer.flags, CLUSTER_CURRENT_MX_WIRE_FLAGS_NONE);
	UT_ASSERT_EQ(members_count, 0);
	UT_ASSERT_EQ(reported_total, (uint32)0);

	UT_ASSERT(cluster_gcs_block_test_snapshot_r4_requester_slot(
		&released_in_use, &released_domain, &released_request_id,
		&released_transition, &released_tag, &released_epoch,
		&released_master, &released_direct_state,
		&released_direct_target_prepared));
	UT_ASSERT(!released_in_use);
	UT_ASSERT_EQ(released_domain, UT_REPLY_DOMAIN_LEGACY_ACQUIRE);
	UT_ASSERT_EQ(released_request_id, 0);
	UT_ASSERT_EQ(released_epoch, 0);
	UT_ASSERT_EQ(released_master, -1);
	UT_ASSERT_EQ(released_direct_state, GCS_BLOCK_DIRECT_UNARMED);
	UT_ASSERT(!released_direct_target_prepared);

	cluster_gcs_reply_timeout_ms = saved_reply_timeout_ms;
	cluster_node_id = saved_node_id;
}

UT_TEST(test_current_mx_describe_reply_lands_in_current_domain)
{
	ClusterCurrentMxKey key;
	ClusterCurrentMxMemberDesc members[2];
	BufferTag reset_tag = route_test_tag();
	uint16 members_count = UINT16_MAX;
	uint32 reported_total = UINT32_MAX;
	uint64 reset_request_id = 0;
	int saved_node_id = cluster_node_id;
	int saved_reply_timeout_ms = cluster_gcs_reply_timeout_ms;

	memset(&key, 0, sizeof(key));
	key.origin_node_id = UT_MASTER_NODE;
	key.multixact_id = (MultiXactId)43;
	key.cluster_epoch = (uint32)UT_FORMATION_EPOCH;
	cluster_node_id = UT_REQUESTER_NODE;
	cluster_gcs_reply_timeout_ms = 1;
	UT_ASSERT(cluster_gcs_block_test_r4_requester_arm(
		reset_tag, UT_FORMATION_EPOCH, UT_MASTER_NODE, UINT64_C(1),
		&reset_request_id));
	UT_ASSERT(cluster_gcs_block_test_release_r4_requester_slot());
	route_seam_reset();
	route_seam.current_mx_reply_enabled = true;
	route_seam.current_mx_validate_result = CMX_DESC_DENIED;
	memset(members, 0xa5, sizeof(members));
	reply_cv_signal_calls = 0;
	reply_cv_timed_sleep_calls = 0;

	UT_ASSERT_EQ(cluster_gcs_current_mx_describe_fetch_and_wait(
					 UT_MASTER_NODE, &key, members, lengthof(members),
					 &members_count, &reported_total),
			 CMX_DESC_DENIED);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 1);
	UT_ASSERT(route_seam.current_mx_slot_armed);
	UT_ASSERT_EQ(route_seam.current_mx_slot_domain,
				 UT_REPLY_DOMAIN_CURRENT_MX);
	UT_ASSERT_EQ(route_seam.current_mx_validate_calls, 2);
	UT_ASSERT_EQ(reply_cv_signal_calls, 1);
	UT_ASSERT_EQ(reply_cv_timed_sleep_calls, 0);
	UT_ASSERT_EQ(members_count, 0);
	UT_ASSERT_EQ(reported_total, (uint32)0);

	cluster_gcs_reply_timeout_ms = saved_reply_timeout_ms;
	cluster_node_id = saved_node_id;
}

UT_TEST(test_current_mx_describe_forward128_routes_to_origin_serve)
{
	ClusterCurrentMxDescribeForwardV2 request;
	ClusterICEnvelope env;
	int saved_node_id = cluster_node_id;

	cluster_node_id = UT_MASTER_NODE;
	route_seam_reset();
	memset(&request, 0, sizeof(request));
	request.prefix.request_id = UT_REQUEST_ID;
	request.prefix.epoch = UT_FORMATION_EPOCH;
	request.prefix.mxkey.origin_node_id = UT_MASTER_NODE;
	request.prefix.mxkey.multixact_id = (MultiXactId)44;
	request.prefix.mxkey.cluster_epoch = (uint32)UT_FORMATION_EPOCH;
	request.prefix.original_requester_node = UT_REQUESTER_NODE;
	request.prefix.requester_backend_id = UT_REQUESTER_BACKEND;
	request.prefix.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE;
	request.trailer.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	request.trailer.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	request.trailer.flags = CLUSTER_CURRENT_MX_WIRE_FLAGS_NONE;
	env = route_test_envelope(
		PGRAC_IC_MSG_GCS_BLOCK_FORWARD, UT_REQUESTER_NODE,
		UT_MASTER_NODE, sizeof(request));

	UT_ASSERT(cluster_gcs_block_test_current_mx_forward128(&env, &request));
	UT_ASSERT_EQ(route_seam.current_mx_describe_serve_calls, 1);
	UT_ASSERT_EQ(route_seam.current_mx_describe_env.source_node_id,
				 (uint32)UT_REQUESTER_NODE);
	UT_ASSERT_EQ(route_seam.current_mx_describe_env.dest_node_id,
				 (uint32)UT_MASTER_NODE);
	UT_ASSERT_EQ(route_seam.current_mx_describe_request.prefix.request_id,
				 UT_REQUEST_ID);
	UT_ASSERT_EQ(route_seam.current_mx_describe_request.prefix.kind,
				 GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE);

	cluster_node_id = saved_node_id;
}

UT_TEST(test_current_mx_member_proof_requester_is_capability_bound_and_times_out_cleanly)
{
	ClusterCurrentMxProofForwardV2 request;
	const ClusterCurrentMxProofForwardV2 *sent_request;
	ClusterCurrentMemberProof proofs[1];
	ClusterCurrentUpdaterProof updater;
	BufferTag reset_tag = route_test_tag();
	uint16 proof_count = UINT16_MAX;
	uint32 requester_capability_generation = UINT32_MAX;
	uint64 reset_request_id = 0;
	int saved_node_id = cluster_node_id;
	int saved_reply_timeout_ms = cluster_gcs_reply_timeout_ms;

	cluster_node_id = UT_REQUESTER_NODE;
	cluster_gcs_reply_timeout_ms = 1;
	memset(&request, 0, sizeof(request));
	request.prefix.request_id = UINT64_C(1);
	request.prefix.epoch = UT_FORMATION_EPOCH;
	request.prefix.mxkey.origin_node_id = 1;
	request.prefix.mxkey.multixact_id = (MultiXactId)45;
	request.prefix.mxkey.cluster_epoch = (uint32)UT_FORMATION_EPOCH;
	request.prefix.original_requester_node = UT_REQUESTER_NODE;
	request.prefix.requester_backend_id = (int32)MyBackendId;
	request.prefix.total_count = 2;
	ClusterCurrentMxProofPrefixSetDescriptorHash(
		&request.prefix, UINT64_C(0x12345678));
	request.prefix.entry_count = 1;
	request.prefix.body_kind = CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS;
	request.prefix.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF;
	request.trailer.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	request.trailer.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	request.trailer.body.asks[0].xid = (TransactionId)501;
	request.trailer.body.asks[0].member_ordinal = 0;
	request.trailer.body.asks[0].member_status = MultiXactStatusForShare;
	UT_ASSERT(cluster_gcs_block_test_r4_requester_arm(
		reset_tag, UT_FORMATION_EPOCH, 4, UINT64_C(1),
		&reset_request_id));
	UT_ASSERT(cluster_gcs_block_test_release_r4_requester_slot());
	route_seam_reset();
	memset(proofs, 0xa5, sizeof(proofs));
	memset(&updater, 0xa5, sizeof(updater));

	UT_ASSERT_EQ(cluster_gcs_current_mx_member_proof_fetch_and_wait(
					 4, &request, proofs, lengthof(proofs),
					 &proof_count, &updater,
					 &requester_capability_generation,
					 GetCurrentTimestamp()
					 + (TimestampTz)cluster_gcs_reply_timeout_ms * 10000),
				 CMX_RESOLVE_TIMEOUT);
	UT_ASSERT_EQ(requester_capability_generation, (uint32)0);
	UT_ASSERT_EQ(route_seam.current_mx_capability_calls, 1);
	UT_ASSERT_EQ(route_seam.current_mx_capability_peer, 4);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 1);
	UT_ASSERT_EQ(route_seam.enqueue_msg_type,
				 PGRAC_IC_MSG_GCS_BLOCK_FORWARD);
	UT_ASSERT_EQ(route_seam.enqueue_dest, (uint32)4);
	UT_ASSERT_EQ(route_seam.enqueue_payload_len,
				 sizeof(ClusterCurrentMxProofForwardV2));
	UT_ASSERT_EQ(route_seam.enqueue_required_capability,
				 PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1);
	UT_ASSERT_EQ(route_seam.enqueue_connection_generation,
				 UT_CURRENT_MX_CAPABILITY_GENERATION);
	UT_ASSERT(route_seam.current_mx_slot_armed);
	UT_ASSERT_EQ(route_seam.current_mx_slot_domain,
				 UT_REPLY_DOMAIN_CURRENT_MX);
	sent_request
		= (const ClusterCurrentMxProofForwardV2 *)route_seam.enqueue_payload;
	UT_ASSERT_EQ(sent_request->prefix.kind,
				 GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF);
	UT_ASSERT_EQ(sent_request->prefix.epoch, UT_FORMATION_EPOCH);
	UT_ASSERT_EQ(sent_request->prefix.original_requester_node,
				 UT_REQUESTER_NODE);
	UT_ASSERT_EQ(proof_count, (uint16)0);
	UT_ASSERT_EQ(proofs[0].state, CCM_UNKNOWN);
	UT_ASSERT_EQ(updater.verdict, CUCP_UNKNOWN);

	cluster_gcs_reply_timeout_ms = saved_reply_timeout_ms;
	cluster_node_id = saved_node_id;
}

UT_TEST(test_current_mx_member_proof_forward128_routes_to_cooperative_origin)
{
	ClusterCurrentMxProofForwardV2 request;
	ClusterICEnvelope env;
	int saved_node_id = cluster_node_id;

	cluster_node_id = UT_MASTER_NODE;
	route_seam_reset();
	memset(&request, 0, sizeof(request));
	request.prefix.request_id = UT_REQUEST_ID;
	request.prefix.epoch = UT_FORMATION_EPOCH;
	request.prefix.mxkey.origin_node_id = UT_MASTER_NODE;
	request.prefix.mxkey.multixact_id = (MultiXactId)46;
	request.prefix.mxkey.cluster_epoch = (uint32)UT_FORMATION_EPOCH;
	request.prefix.original_requester_node = UT_REQUESTER_NODE;
	request.prefix.requester_backend_id = UT_REQUESTER_BACKEND;
	request.prefix.total_count = 1;
	request.prefix.entry_count = 1;
	request.prefix.body_kind = CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS;
	request.prefix.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF;
	request.trailer.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	request.trailer.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	request.trailer.body.asks[0].xid = (TransactionId)503;
	request.trailer.body.asks[0].member_status = MultiXactStatusForShare;
	env = route_test_envelope(
		PGRAC_IC_MSG_GCS_BLOCK_FORWARD, UT_REQUESTER_NODE,
		UT_MASTER_NODE, sizeof(request));

	UT_ASSERT(cluster_gcs_block_test_current_mx_forward128(&env, &request));
	UT_ASSERT_EQ(route_seam.current_mx_proof_serve_calls, 0);
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 1);
	/* An admitted context is not a new read: duplicate and original drain
	 * remain available after the seal. */
	stop_new_read_allowed = false;
	stop_new_read_calls = 0;
	UT_ASSERT(cluster_gcs_block_test_current_mx_forward128(&env, &request));
	UT_ASSERT_EQ(stop_new_read_calls, 0);
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 1);
	cluster_gcs_block_test_r4_tx_origin_drain();
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
	/* Once the original owner retired it, this frame starts a new context. */
	UT_ASSERT(cluster_gcs_block_test_current_mx_forward128(&env, &request));
	UT_ASSERT_EQ(stop_new_read_calls, 1);
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
	UT_ASSERT_EQ(route_seam.current_mx_proof_serve_calls, 0);
	cluster_gcs_block_test_r4_tx_origin_drain(); /* RED-only original cleanup. */
	stop_new_read_allowed = true;

	cluster_node_id = saved_node_id;
}

UT_TEST(test_current_mx_member_proof_reply_lands_in_current_domain)
{
	ClusterCurrentMxProofForwardV2 request;
	ClusterCurrentMemberProof proofs[1];
	ClusterCurrentUpdaterProof updater;
	BufferTag reset_tag = route_test_tag();
	uint16 proof_count = UINT16_MAX;
	uint32 requester_capability_generation = UINT32_MAX;
	uint64 reset_request_id = 0;
	int saved_node_id = cluster_node_id;
	int saved_reply_timeout_ms = cluster_gcs_reply_timeout_ms;

	cluster_node_id = UT_REQUESTER_NODE;
	cluster_gcs_reply_timeout_ms = 1;
	memset(&request, 0, sizeof(request));
	request.prefix.request_id = UINT64_C(1);
	request.prefix.epoch = UT_FORMATION_EPOCH;
	request.prefix.mxkey.origin_node_id = 1;
	request.prefix.mxkey.multixact_id = (MultiXactId)46;
	request.prefix.mxkey.cluster_epoch = (uint32)UT_FORMATION_EPOCH;
	request.prefix.original_requester_node = UT_REQUESTER_NODE;
	request.prefix.requester_backend_id = (int32)MyBackendId;
	request.prefix.total_count = 2;
	ClusterCurrentMxProofPrefixSetDescriptorHash(
		&request.prefix, UINT64_C(0x12345678));
	request.prefix.entry_count = 1;
	request.prefix.body_kind = CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS;
	request.prefix.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF;
	request.trailer.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	request.trailer.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	request.trailer.body.asks[0].xid = (TransactionId)502;
	request.trailer.body.asks[0].member_ordinal = 0;
	request.trailer.body.asks[0].member_status = MultiXactStatusForShare;
	UT_ASSERT(cluster_gcs_block_test_r4_requester_arm(
		reset_tag, UT_FORMATION_EPOCH, 4, UINT64_C(1),
		&reset_request_id));
	UT_ASSERT(cluster_gcs_block_test_release_r4_requester_slot());
	route_seam_reset();
	route_seam.current_mx_proof_reply_enabled = true;
	route_seam.current_mx_proof_validate_result = CMX_RESOLVE_DENIED;
	memset(proofs, 0xa5, sizeof(proofs));
	memset(&updater, 0xa5, sizeof(updater));
	reply_cv_signal_calls = 0;
	reply_cv_timed_sleep_calls = 0;

	UT_ASSERT_EQ(cluster_gcs_current_mx_member_proof_fetch_and_wait(
					 4, &request, proofs, lengthof(proofs),
					 &proof_count, &updater,
					 &requester_capability_generation,
					 GetCurrentTimestamp()
					 + (TimestampTz)cluster_gcs_reply_timeout_ms * 10000),
				 CMX_RESOLVE_DENIED);
	UT_ASSERT_EQ(requester_capability_generation, (uint32)0);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 1);
	UT_ASSERT(route_seam.current_mx_slot_armed);
	UT_ASSERT_EQ(route_seam.current_mx_slot_domain,
				 UT_REPLY_DOMAIN_CURRENT_MX);
	UT_ASSERT_EQ(route_seam.current_mx_proof_validate_calls, 2);
	UT_ASSERT_EQ(reply_cv_signal_calls, 1);
	UT_ASSERT_EQ(reply_cv_timed_sleep_calls, 0);
	UT_ASSERT_EQ(proof_count, (uint16)0);
	UT_ASSERT_EQ(proofs[0].state, CCM_UNKNOWN);
	UT_ASSERT_EQ(updater.verdict, CUCP_UNKNOWN);

	cluster_gcs_reply_timeout_ms = saved_reply_timeout_ms;
	cluster_node_id = saved_node_id;
}

UT_TEST(test_r4_requester_count_tracks_only_live_r4_domain_slots)
{
	BufferTag tag = route_test_tag();
	uint64 request_id = 0;
	int saved_max_backends = MaxBackends;

	MaxBackends = 1;
	UT_ASSERT(cluster_gcs_block_test_r4_requester_arm(
		tag, UT_FORMATION_EPOCH, UT_MASTER_NODE, UINT64_C(1), &request_id));
	UT_ASSERT_EQ(cluster_gcs_block_r4_requester_count(), UINT64_C(1));
	UT_ASSERT(cluster_gcs_block_test_release_r4_requester_slot());
	UT_ASSERT_EQ(cluster_gcs_block_r4_requester_count(), UINT64_C(0));
	MaxBackends = saved_max_backends;
}

/* The first executable requester leg is deliberately remote: an exact R4
 * slot is visible before REQUEST80 staging, a remote holder's status 21 is
 * consumed into scratch only, and the slot domain is cleared on return. */
UT_TEST(test_r4_remote_requester_arms_request80_waits_full_and_releases)
{
	BufferTag tag = route_test_tag();
	char dst_page[GCS_BLOCK_DATA_SIZE];
	SCN decoded_read_scn = InvalidScn;
	bool in_use = true;
	uint8 reply_domain = UINT8_MAX;
	uint64 request_id = UINT64_MAX;
	uint64 request_epoch = UINT64_MAX;
	int32 expected_master = INT32_MIN;
	uint8 transition_id = UINT8_MAX;
	BufferTag released_tag;
	ClusterGcsBlockDirectState direct_state = GCS_BLOCK_DIRECT_ABORTED;
	bool direct_target_prepared = true;
	int saved_node_id = cluster_node_id;

	cluster_node_id = UT_REQUESTER_NODE;
	memset(&requester_send, 0, sizeof(requester_send));
	memset(dst_page, 0, sizeof(dst_page));
	reply_cv_prepare_calls = 0;
	reply_cv_timed_sleep_calls = 0;
	reply_cv_cancel_calls = 0;
	reply_cv_signal_calls = 0;
	UT_ASSERT(cluster_gcs_block_test_r4_fetch_and_wait(
		tag, UT_READ_SCN, UT_MASTER_NODE, dst_page));
	UT_ASSERT_EQ(requester_send.calls, 1);
	UT_ASSERT_EQ(requester_send.msg_type, PGRAC_IC_MSG_GCS_BLOCK_REQUEST);
	UT_ASSERT_EQ(requester_send.dest_node, UT_MASTER_NODE);
	UT_ASSERT_EQ(requester_send.payload_len, sizeof(ClusterR4CrRequestPayload));
	UT_ASSERT(requester_send.slot_armed);
	UT_ASSERT_EQ(requester_send.slot_domain, UT_REPLY_DOMAIN_R4_CR);
	UT_ASSERT_EQ(requester_send.direct_state, GCS_BLOCK_DIRECT_UNARMED);
	UT_ASSERT(!requester_send.direct_target_prepared);
	UT_ASSERT_EQ(requester_send.request.base.epoch, UT_FORMATION_EPOCH);
	UT_ASSERT_EQ(requester_send.request.base.tag.relNumber, tag.relNumber);
	UT_ASSERT_EQ(requester_send.request.base.tag.blockNum, tag.blockNum);
	UT_ASSERT_EQ(requester_send.request.base.sender_node, UT_REQUESTER_NODE);
	UT_ASSERT_EQ(requester_send.request.base.requester_backend_id, (int32)MyBackendId);
	UT_ASSERT_EQ(requester_send.request.base.transition_id, PCM_TRANS_N_TO_S);
	UT_ASSERT(!GcsBlockRequestPayloadIsCleanEligible(&requester_send.request.base));
	UT_ASSERT(!GcsBlockRequestPayloadIsDirectLandArmed(&requester_send.request.base));
	UT_ASSERT(ClusterR4RequestExtensionGetCr(
		&requester_send.request.extension, &decoded_read_scn));
	UT_ASSERT_EQ(decoded_read_scn, UT_READ_SCN);
	UT_ASSERT_EQ(memcmp(dst_page, requester_send.reply_page, sizeof(dst_page)), 0);
	UT_ASSERT_EQ(reply_cv_prepare_calls, 1);
	UT_ASSERT_EQ(reply_cv_timed_sleep_calls, 0);
	UT_ASSERT_EQ(reply_cv_cancel_calls, 1);
	UT_ASSERT_EQ(reply_cv_signal_calls, 1);

	UT_ASSERT(cluster_gcs_block_test_snapshot_r4_requester_slot(
		&in_use, &reply_domain, &request_id, &transition_id, &released_tag,
		&request_epoch, &expected_master, &direct_state, &direct_target_prepared));
	UT_ASSERT(!in_use);
	UT_ASSERT_EQ(reply_domain, UT_REPLY_DOMAIN_LEGACY_ACQUIRE);
	UT_ASSERT_EQ(request_id, 0);
	UT_ASSERT_EQ(request_epoch, 0);
	UT_ASSERT_EQ(expected_master, -1);
	UT_ASSERT_EQ(direct_state, GCS_BLOCK_DIRECT_UNARMED);
	UT_ASSERT(!direct_target_prepared);
	cluster_node_id = saved_node_id;
}

/* Status 25 is the closed RETRYABLE/HOLDER_MOVED requester outcome already
 * pinned by the result/reason table above.  It closes the old operation: the
 * requester must release that slot, allocate a genuinely fresh id and route
 * the same logical read again.  The two attempts intentionally use the same
 * physical CV, proving the first slot became reusable before the second arm. */
UT_TEST(test_r4_requester_status25_releases_then_retries_with_fresh_id)
{
	BufferTag tag = route_test_tag();
	char dst_page[GCS_BLOCK_DATA_SIZE];
	char expected_page[GCS_BLOCK_DATA_SIZE];
	bool in_use = true;
	uint8 reply_domain = UINT8_MAX;
	uint64 request_id = UINT64_MAX;
	uint64 request_epoch = UINT64_MAX;
	int32 expected_master = INT32_MIN;
	uint8 transition_id = UINT8_MAX;
	BufferTag released_tag;
	ClusterGcsBlockDirectState direct_state = GCS_BLOCK_DIRECT_ABORTED;
	bool direct_target_prepared = true;
	int saved_node_id = cluster_node_id;
	int saved_reply_timeout_ms = cluster_gcs_reply_timeout_ms;
	int saved_max_retries = cluster_gcs_block_retransmit_max_retries;

	cluster_node_id = UT_REQUESTER_NODE;
	cluster_gcs_reply_timeout_ms = 1;
	cluster_gcs_block_retransmit_max_retries = 1;
	memset(&requester_send, 0, sizeof(requester_send));
	requester_send.reply_step_count = 2;
	requester_send.reply_steps[0].status
		= GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED;
	requester_send.reply_steps[0].envelope_source_node = UT_MASTER_NODE;
	requester_send.reply_steps[0].header_sender_node = UT_MASTER_NODE;
	requester_send.reply_steps[0].forwarding_master_node
		= GCS_BLOCK_REPLY_NO_FORWARDING_MASTER;
	requester_send.reply_steps[0].page_lsn = 0;
	requester_send.reply_steps[0].block_fill = 0;
	requester_send.reply_steps[1].status = GCS_BLOCK_REPLY_R4_CR_FULL;
	requester_send.reply_steps[1].envelope_source_node = UT_HOLDER_NODE;
	requester_send.reply_steps[1].header_sender_node = UT_HOLDER_NODE;
	requester_send.reply_steps[1].forwarding_master_node = UT_MASTER_NODE;
	requester_send.reply_steps[1].page_lsn = UINT64_C(0xabcdef);
	requester_send.reply_steps[1].block_fill = 0x6d;
	memset(dst_page, 0, sizeof(dst_page));
	memset(expected_page, 0x6d, sizeof(expected_page));
	reply_cv_prepare_calls = 0;
	reply_cv_timed_sleep_calls = 0;
	reply_cv_cancel_calls = 0;
	reply_cv_signal_calls = 0;
	memset(reply_cv_prepared, 0, sizeof(reply_cv_prepared));
	memset(reply_cv_signaled, 0, sizeof(reply_cv_signaled));

	UT_ASSERT(cluster_gcs_block_test_r4_fetch_and_wait(
		tag, UT_READ_SCN, UT_MASTER_NODE, dst_page));
	UT_ASSERT_EQ(requester_send.calls, 2);
	UT_ASSERT_EQ(requester_send.request_ids[0], UINT64_C(0x0200000000000001));
	UT_ASSERT_EQ(requester_send.request_ids[1], UINT64_C(0x0200000000000002));
	UT_ASSERT_NE(requester_send.request_ids[0], requester_send.request_ids[1]);
	UT_ASSERT(requester_send.slot_armed_by_call[0]);
	UT_ASSERT(requester_send.slot_armed_by_call[1]);
	UT_ASSERT_EQ(requester_send.slot_domain_by_call[0], UT_REPLY_DOMAIN_R4_CR);
	UT_ASSERT_EQ(requester_send.slot_domain_by_call[1], UT_REPLY_DOMAIN_R4_CR);
	UT_ASSERT_EQ(memcmp(dst_page, expected_page, sizeof(dst_page)), 0);
	UT_ASSERT_EQ(reply_cv_prepare_calls, 2);
	UT_ASSERT_EQ(reply_cv_timed_sleep_calls, 0);
	UT_ASSERT_EQ(reply_cv_cancel_calls, 2);
	UT_ASSERT_EQ(reply_cv_signal_calls, 2);
	UT_ASSERT_NOT_NULL(reply_cv_prepared[0]);
	UT_ASSERT(reply_cv_prepared[0] == reply_cv_signaled[0]);
	UT_ASSERT(reply_cv_prepared[1] == reply_cv_signaled[1]);
	UT_ASSERT(reply_cv_prepared[0] == reply_cv_prepared[1]);

	UT_ASSERT(cluster_gcs_block_test_snapshot_r4_requester_slot(
		&in_use, &reply_domain, &request_id, &transition_id, &released_tag,
		&request_epoch, &expected_master, &direct_state, &direct_target_prepared));
	UT_ASSERT(!in_use);
	UT_ASSERT_EQ(reply_domain, UT_REPLY_DOMAIN_LEGACY_ACQUIRE);
	UT_ASSERT_EQ(request_id, 0);
	UT_ASSERT_EQ(request_epoch, 0);
	UT_ASSERT_EQ(expected_master, -1);
	UT_ASSERT_EQ(direct_state, GCS_BLOCK_DIRECT_UNARMED);
	UT_ASSERT(!direct_target_prepared);

	cluster_gcs_block_retransmit_max_retries = saved_max_retries;
	cluster_gcs_reply_timeout_ms = saved_reply_timeout_ms;
	cluster_node_id = saved_node_id;
}

/* Status 26 is the closed FAIL_CLOSED/PROTOCOL requester outcome.  Unlike
 * status 25 it cannot consume retry budget or arm a fresh identity; its exact
 * zero-body holder reply wakes once, leaves caller scratch untouched and the
 * sole requester slot returns to the canonical legacy-zero image. */
UT_TEST(test_r4_requester_status26_fails_closed_without_retry_and_cleans_slot)
{
	BufferTag tag = route_test_tag();
	char dst_page[GCS_BLOCK_DATA_SIZE];
	char original_page[GCS_BLOCK_DATA_SIZE];
	bool in_use = true;
	uint8 reply_domain = UINT8_MAX;
	uint64 request_id = UINT64_MAX;
	uint64 request_epoch = UINT64_MAX;
	int32 expected_master = INT32_MIN;
	uint8 transition_id = UINT8_MAX;
	BufferTag released_tag;
	ClusterGcsBlockDirectState direct_state = GCS_BLOCK_DIRECT_ABORTED;
	bool direct_target_prepared = true;
	int saved_node_id = cluster_node_id;
	int saved_reply_timeout_ms = cluster_gcs_reply_timeout_ms;
	int saved_max_retries = cluster_gcs_block_retransmit_max_retries;

	cluster_node_id = UT_REQUESTER_NODE;
	cluster_gcs_reply_timeout_ms = 1;
	cluster_gcs_block_retransmit_max_retries = 1;
	memset(&requester_send, 0, sizeof(requester_send));
	requester_send.reply_step_count = 1;
	requester_send.reply_steps[0].status = GCS_BLOCK_REPLY_R4_DENIED;
	requester_send.reply_steps[0].envelope_source_node = UT_HOLDER_NODE;
	requester_send.reply_steps[0].header_sender_node = UT_HOLDER_NODE;
	requester_send.reply_steps[0].forwarding_master_node = UT_MASTER_NODE;
	requester_send.reply_steps[0].page_lsn = 0;
	requester_send.reply_steps[0].block_fill = 0;
	memset(dst_page, 0xa7, sizeof(dst_page));
	memcpy(original_page, dst_page, sizeof(original_page));
	reply_cv_prepare_calls = 0;
	reply_cv_timed_sleep_calls = 0;
	reply_cv_cancel_calls = 0;
	reply_cv_signal_calls = 0;
	memset(reply_cv_prepared, 0, sizeof(reply_cv_prepared));
	memset(reply_cv_signaled, 0, sizeof(reply_cv_signaled));

	UT_ASSERT(!cluster_gcs_block_test_r4_fetch_and_wait(
		tag, UT_READ_SCN, UT_MASTER_NODE, dst_page));
	UT_ASSERT_EQ(requester_send.calls, 1);
	UT_ASSERT_EQ(requester_send.request_ids[0], UINT64_C(0x0200000000000001));
	UT_ASSERT(requester_send.slot_armed_by_call[0]);
	UT_ASSERT_EQ(requester_send.slot_domain_by_call[0], UT_REPLY_DOMAIN_R4_CR);
	UT_ASSERT_EQ(memcmp(dst_page, original_page, sizeof(dst_page)), 0);
	UT_ASSERT_EQ(reply_cv_prepare_calls, 1);
	UT_ASSERT_EQ(reply_cv_timed_sleep_calls, 0);
	UT_ASSERT_EQ(reply_cv_cancel_calls, 1);
	UT_ASSERT_EQ(reply_cv_signal_calls, 1);
	UT_ASSERT_NOT_NULL(reply_cv_prepared[0]);
	UT_ASSERT(reply_cv_prepared[0] == reply_cv_signaled[0]);

	UT_ASSERT(cluster_gcs_block_test_snapshot_r4_requester_slot(
		&in_use, &reply_domain, &request_id, &transition_id, &released_tag,
		&request_epoch, &expected_master, &direct_state, &direct_target_prepared));
	UT_ASSERT(!in_use);
	UT_ASSERT_EQ(reply_domain, UT_REPLY_DOMAIN_LEGACY_ACQUIRE);
	UT_ASSERT_EQ(request_id, 0);
	UT_ASSERT_EQ(request_epoch, 0);
	UT_ASSERT_EQ(expected_master, -1);
	UT_ASSERT_EQ(direct_state, GCS_BLOCK_DIRECT_UNARMED);
	UT_ASSERT(!direct_target_prepared);

	cluster_gcs_block_retransmit_max_retries = saved_max_retries;
	cluster_gcs_reply_timeout_ms = saved_reply_timeout_ms;
	cluster_node_id = saved_node_id;
}

static void
route_test_reset_public_target_requester(void)
{
	BufferTag tag = route_test_tag();
	uint64 request_id = 0;
	bool armed;

	armed = cluster_gcs_block_test_r4_requester_arm(
		tag, UT_FORMATION_EPOCH, UT_MASTER_NODE, UINT64_C(1), &request_id);
	UT_ASSERT(armed);
	if (armed)
		UT_ASSERT(cluster_gcs_block_test_release_r4_requester_slot());
	route_seam_reset();
	memset(&requester_send, 0, sizeof(requester_send));
	reply_lock_acquire_calls = 0;
	reply_lock_release_calls = 0;
	reply_cv_prepare_calls = 0;
	reply_cv_timed_sleep_calls = 0;
	reply_cv_cancel_calls = 0;
	reply_cv_signal_calls = 0;
}

static void
route_test_assert_public_target_slot_is_canonical(void)
{
	BufferTag tag;
	bool in_use = true;
	uint8 reply_domain = UINT8_MAX;
	uint64 request_id = UINT64_MAX;
	uint64 request_epoch = UINT64_MAX;
	int32 expected_master = INT32_MIN;
	uint8 transition_id = UINT8_MAX;
	ClusterGcsBlockDirectState direct_state = GCS_BLOCK_DIRECT_ABORTED;
	bool direct_target_prepared = true;

	UT_ASSERT(cluster_gcs_block_test_snapshot_r4_requester_slot(
		&in_use, &reply_domain, &request_id, &transition_id, &tag,
		&request_epoch, &expected_master, &direct_state, &direct_target_prepared));
	UT_ASSERT(!in_use);
	UT_ASSERT_EQ(reply_domain, UT_REPLY_DOMAIN_LEGACY_ACQUIRE);
	UT_ASSERT_EQ(request_id, 0);
	UT_ASSERT_EQ(transition_id, 0);
	UT_ASSERT_EQ(request_epoch, 0);
	UT_ASSERT_EQ(expected_master, -1);
	UT_ASSERT_EQ(direct_state, GCS_BLOCK_DIRECT_UNARMED);
	UT_ASSERT(!direct_target_prepared);
}

/* A valid retry is a terminal physical reply, not a failed logical read.
 * Nine replies exceed the old eight-retry limit; only the later exact FULL
 * may publish scratch, under the same admitted logical read. */
UT_TEST(test_target_wrapper_status25_beyond_old_limit_waits_for_full)
{
	BufferTag tag = route_test_tag();
	char dst_page[BLCKSZ];
	char original_page[BLCKSZ];
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	sigjmp_buf error_jump;
	sigjmp_buf *saved_exception_stack = PG_exception_stack;
	int saved_node_id = cluster_node_id;
	int saved_reply_timeout_ms = cluster_gcs_reply_timeout_ms;
	int saved_max_retries = cluster_gcs_block_retransmit_max_retries;
	volatile ClusterCrBuildResult result = CLUSTER_CR_BUILD_FAIL_CLOSED;
	int i;

	cluster_node_id = UT_REQUESTER_NODE;
	cluster_gcs_reply_timeout_ms = 1;
	cluster_gcs_block_retransmit_max_retries = 8;
	route_test_reset_public_target_requester();
	requester_send.reply_step_count = 10;
	requester_send.reply_steps[0].status
		= GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED;
	requester_send.reply_steps[0].envelope_source_node = UT_MASTER_NODE;
	requester_send.reply_steps[0].header_sender_node = UT_MASTER_NODE;
	requester_send.reply_steps[0].forwarding_master_node
		= GCS_BLOCK_REPLY_NO_FORWARDING_MASTER;
	requester_send.reply_steps[0].page_lsn = 0;
	requester_send.reply_steps[0].block_fill = 0;
	for (i = 1; i < 9; i++)
		requester_send.reply_steps[i] = requester_send.reply_steps[0];
	requester_send.reply_steps[9].status = GCS_BLOCK_REPLY_R4_CR_FULL;
	requester_send.reply_steps[9].envelope_source_node = UT_HOLDER_NODE;
	requester_send.reply_steps[9].header_sender_node = UT_HOLDER_NODE;
	requester_send.reply_steps[9].forwarding_master_node = UT_MASTER_NODE;
	requester_send.reply_steps[9].page_lsn = UINT64_C(0xabcdef);
	requester_send.reply_steps[9].block_fill = 0x6d;
	memset(dst_page, 0xa7, sizeof(dst_page));
	memcpy(original_page, dst_page, sizeof(original_page));
	route_ereport_armed = true;
	route_ereport_caught = false;
	route_ereport_sqlstate = 0;

	if (sigsetjmp(error_jump, 1) == 0) {
		PG_exception_stack = &error_jump;
		result = cluster_gcs_block_cr_fetch_and_wait(tag, UT_READ_SCN, dst_page, &reason);
	} else
		PG_exception_stack = saved_exception_stack;
	PG_exception_stack = saved_exception_stack;
	route_ereport_armed = false;

	UT_ASSERT(!route_ereport_caught);
	UT_ASSERT_EQ(result, CLUSTER_CR_BUILD_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(requester_send.calls, 10);
	for (i = 0; i < 10; i++) {
		UT_ASSERT_EQ(requester_send.request_ids[i], UINT64_C(0x0200000000000002) + i);
		UT_ASSERT(requester_send.slot_armed_by_call[i]);
		UT_ASSERT_EQ(requester_send.slot_domain_by_call[i], UT_REPLY_DOMAIN_R4_CR);
	}
	UT_ASSERT_EQ(reply_cv_prepare_calls, 10);
	UT_ASSERT_EQ(reply_cv_timed_sleep_calls, 0);
	UT_ASSERT_EQ(reply_cv_cancel_calls, 10);
	UT_ASSERT_EQ(reply_cv_signal_calls, 10);
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT(route_seam.recheck_calls >= 1);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	memset(original_page, 0x6d, sizeof(original_page));
	UT_ASSERT_EQ(memcmp(dst_page, original_page, sizeof(dst_page)), 0);
	route_test_assert_public_target_slot_is_canonical();

	cluster_gcs_block_retransmit_max_retries = saved_max_retries;
	cluster_gcs_reply_timeout_ms = saved_reply_timeout_ms;
	cluster_node_id = saved_node_id;
}

static void
route_test_first_reply_retry(void)
{
	RequesterReplyStep *step = &requester_send.reply_steps[0];

	requester_send.reply_step_count = 1;
	step->status = GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED;
	step->envelope_source_node = UT_MASTER_NODE;
	step->header_sender_node = UT_MASTER_NODE;
	step->forwarding_master_node = GCS_BLOCK_REPLY_NO_FORWARDING_MASTER;
}

/* The real requester must finish each physical identity before its existing
 * outbound seam sees DONE.  Neither a successful logical read nor a retry
 * may keep the master's completed route alive for the full fallback TTL. */
UT_TEST(test_r4_terminal_done_remote_and_self_master_preserves_exact_identity)
{
	BufferTag tag = route_test_tag();
	int saved_node = cluster_node_id;
	int local;
	int terminal;

	cluster_node_id = UT_REQUESTER_NODE;
	for (local = 0; local < 2; local++) {
		for (terminal = 0; terminal < 3; terminal++) {
			char page[BLCKSZ];
			ClusterCrBuildReason reason = CLUSTER_CR_BUILD_NONE;
			int master = local ? UT_REQUESTER_NODE : UT_MASTER_NODE;
			int expected = terminal == 1 ? 2 : 1;
			int i;

			route_test_reset_public_target_requester();
			route_seam.lookup_master_node = master;
			requester_send.reply_step_count = expected;
			for (i = 0; i < expected; i++) {
				RequesterReplyStep *step = &requester_send.reply_steps[i];

				step->status = (terminal == 1 && i == 0) ? GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED
							   : terminal == 2			 ? GCS_BLOCK_REPLY_R4_DENIED
														 : GCS_BLOCK_REPLY_R4_CR_FULL;
				step->envelope_source_node = UT_HOLDER_NODE;
				step->header_sender_node = UT_HOLDER_NODE;
				step->forwarding_master_node = master;
				if (step->status == GCS_BLOCK_REPLY_R4_CR_FULL) {
					step->page_lsn = UINT64_C(0xabcdef);
					step->block_fill = 0x6d;
				}
			}
			memset(page, 0xa7, sizeof(page));
			UT_ASSERT_EQ(cluster_gcs_block_cr_fetch_and_wait(tag, UT_READ_SCN, page, &reason),
						 terminal == 2 ? CLUSTER_CR_BUILD_FAIL_CLOSED : CLUSTER_CR_BUILD_FULL);
			UT_ASSERT_EQ(requester_send.done_calls, expected);
			for (i = 0; i < requester_send.done_calls; i++) {
				GcsBlockDonePayload literal;

				memset(&literal, 0, sizeof(literal));
				literal.request_id = UINT64_C(0x0200000000000002) + i;
				literal.epoch = UT_FORMATION_EPOCH;
				literal.tag = tag;
				literal.sender_node = UT_REQUESTER_NODE;
				literal.requester_backend_id = (int32)MyBackendId;
				literal.transition_id = (uint8)PCM_TRANS_N_TO_S;
				UT_ASSERT_EQ(memcmp(&literal, &requester_send.done[i], sizeof(literal)), 0);
				UT_ASSERT_EQ(requester_send.done_dest[i], master);
				UT_ASSERT(requester_send.done_after_release[i]);
			}
			UT_ASSERT_EQ(requester_send.done_cap_calls, local ? 0 : expected);
			UT_ASSERT_EQ(cluster_gcs_get_block_done_sent_count(), expected);
			route_test_assert_public_target_slot_is_canonical();
		}
	}
	cluster_node_id = saved_node;
}

UT_TEST(test_r4_done_missing_capability_or_queue_space_keeps_terminal_result)
{
	BufferTag tag = route_test_tag();
	int saved_node = cluster_node_id;
	int mode;

	cluster_node_id = UT_REQUESTER_NODE;
	for (mode = 0; mode < 3; mode++) {
		char page[BLCKSZ];
		ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;

		route_test_reset_public_target_requester();
		requester_send.suppress_done_cap = mode == 0;
		requester_send.refuse_done = mode == 1;
		cluster_ic_suppress_gcs_done_cap = mode == 2;
		UT_ASSERT_EQ(cluster_gcs_block_cr_fetch_and_wait(tag, UT_READ_SCN, page, &reason),
					 CLUSTER_CR_BUILD_FULL);
		UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
		UT_ASSERT_EQ(requester_send.done_calls, mode == 1 ? 1 : 0);
		UT_ASSERT_EQ(cluster_gcs_get_block_done_sent_count(), 0);
		UT_ASSERT_EQ(cluster_gcs_get_block_done_enqueue_drop_count(), mode == 1 ? 1 : 0);
		route_test_assert_public_target_slot_is_canonical();
		cluster_ic_suppress_gcs_done_cap = false;
	}
	cluster_node_id = saved_node;
}

UT_TEST(test_direct_s_capacity_refusal_returns_owned_retry_without_done)
{
	BufferDesc buffer;
	BufferDesc before;
	sigjmp_buf jump;
	sigjmp_buf *saved_stack = PG_exception_stack;
	int saved_node = cluster_node_id;
	int saved_retries = cluster_gcs_block_retransmit_max_retries;
	volatile bool caught = false;
	bool retry_denied = false;
	volatile bool granted = true;

	cluster_node_id = UT_REQUESTER_NODE;
	cluster_gcs_block_retransmit_max_retries = 0;
	route_test_reset_public_target_requester();
	requester_send.legacy_read = true;
	route_test_first_reply_retry();
	requester_send.reply_steps[0].status = GCS_BLOCK_REPLY_DENIED_DEDUP_FULL;
	memset(&buffer, 0, sizeof(buffer));
	buffer.tag = route_test_tag();
	memcpy(&before, &buffer, sizeof(before));
	route_ereport_armed = true;
	if (sigsetjmp(jump, 1) == 0) {
		PG_exception_stack = &jump;
		granted = cluster_gcs_send_block_request_and_wait(&buffer, PCM_TRANS_N_TO_S, UT_MASTER_NODE,
														  false, &retry_denied);
	} else
		caught = true;
	PG_exception_stack = saved_stack;
	route_ereport_armed = false;
	UT_ASSERT(!caught);
	UT_ASSERT(!granted);
	UT_ASSERT(retry_denied);
	UT_ASSERT_EQ(requester_send.calls, 1);
	UT_ASSERT_EQ(requester_send.done_calls, 0);
	UT_ASSERT_EQ(memcmp(&buffer, &before, sizeof(buffer)), 0);
	route_test_assert_public_target_slot_is_canonical();
	cluster_gcs_block_retransmit_max_retries = saved_retries;
	cluster_node_id = saved_node;
}

UT_TEST(test_capacity_retry_excludes_write_clean_forwarded_and_unverified_reply)
{
	int saved_node = cluster_node_id;
	int saved_retries = cluster_gcs_block_retransmit_max_retries;
	int mode;

	cluster_node_id = UT_REQUESTER_NODE;
	cluster_gcs_block_retransmit_max_retries = 0;
	for (mode = 0; mode < 5; mode++) {
		BufferDesc buffer, before;
		sigjmp_buf jump;
		sigjmp_buf *saved_stack = PG_exception_stack;
		volatile bool caught = false;
		bool retry_denied = false;

		route_test_reset_public_target_requester();
		requester_send.legacy_read = true;
		route_test_first_reply_retry();
		requester_send.reply_steps[0].status = GCS_BLOCK_REPLY_DENIED_DEDUP_FULL;
		if (mode == 2) {
			requester_send.reply_steps[0].header_sender_node = UT_HOLDER_NODE;
			requester_send.reply_steps[0].envelope_source_node = UT_HOLDER_NODE;
			requester_send.reply_steps[0].forwarding_master_node = UT_MASTER_NODE;
		}
		if (mode == 3)
			requester_send.corrupt_reply_identity = true;
		if (mode == 4)
			requester_send.suppress_reply = true;
		reply_cv_timed_sleep_raise = mode >= 3;
		memset(&buffer, 0, sizeof(buffer));
		buffer.tag = route_test_tag();
		memcpy(&before, &buffer, sizeof(before));
		route_ereport_armed = true;
		if (sigsetjmp(jump, 1) == 0) {
			PG_exception_stack = &jump;
			(void)cluster_gcs_send_block_request_and_wait(
				&buffer, mode == 0 ? PCM_TRANS_N_TO_X : PCM_TRANS_N_TO_S, UT_MASTER_NODE, mode == 1,
				&retry_denied);
		} else
			caught = true;
		PG_exception_stack = saved_stack;
		route_ereport_armed = false;
		UT_ASSERT(caught);
		UT_ASSERT(!retry_denied);
		UT_ASSERT_EQ(requester_send.done_calls, 0);
		UT_ASSERT_EQ(memcmp(&buffer, &before, sizeof(buffer)), 0);
		route_test_assert_public_target_slot_is_canonical();
	}
	route_test_reset_public_target_requester();
	cluster_gcs_block_retransmit_max_retries = saved_retries;
	cluster_node_id = saved_node;
}

UT_TEST(test_real_capacity_refusal_to_exact_rearm_cancel_gate_and_successor)
{
	ClusterPcmOwnEntry *saved_own = ClusterPcmOwnArray;
	int saved_buffers = NBuffers;
	int saved_node = cluster_node_id;
	int saved_retries = cluster_gcs_block_retransmit_max_retries;
	int mode;

	cluster_node_id = UT_REQUESTER_NODE;
	cluster_gcs_block_retransmit_max_retries = 0;
	NBuffers = 1;
	for (mode = 0; mode < 5; mode++) {
		ClusterPcmOwnEntry entry, before;
		ClusterPcmOwnSnapshot base;
		ClusterPcmGrantBeginWaitReason wait_reason;
		uint64 token = 0, covered_generation = 0;
		bool covered = false, barrier = false, retry_denied = false;
		sigjmp_buf jump;
		sigjmp_buf *saved_stack = PG_exception_stack;
		volatile bool caught = false;
		volatile ClusterBufmgrPcmRetryRearmResult result = CLUSTER_BUFMGR_PCM_RETRY_REARMED;

		route_test_reset_public_target_requester();
		requester_send.legacy_read = true;
		route_test_first_reply_retry();
		requester_send.reply_steps[0].status = GCS_BLOCK_REPLY_DENIED_DEDUP_FULL;
		requester_send.reply_steps[1] = requester_send.reply_steps[0];
		requester_send.reply_step_count = 2;
		memset(&capacity_buffer, 0, sizeof(capacity_buffer));
		capacity_buffer.tag = route_test_tag();
		capacity_buffer.pcm_state = PCM_STATE_N;
		capacity_buffer.buffer_type = BUF_TYPE_CURRENT;
		pg_atomic_init_u32(&capacity_buffer.state, BM_VALID | BM_TAG_VALID);
		memset(&entry, 0, sizeof(entry));
		pg_atomic_init_u64(&entry.generation, 5);
		pg_atomic_init_u64(&entry.reservation_token, 7);
		ClusterPcmOwnArray = &entry;
		capacity_gate_open = true;
		capacity_content_held = false;
		UT_ASSERT_EQ(cluster_pcm_own_begin_grant_reservation(&capacity_buffer, PCM_LOCK_MODE_S,
															 &base, &token, &covered, &wait_reason),
					 CLUSTER_PCM_OWN_OK);
		UT_ASSERT_EQ(token, 8);
		UT_ASSERT(!covered);
		memcpy(&before, &entry, sizeof(before));
		UT_ASSERT(!cluster_gcs_send_block_request_and_wait(&capacity_buffer, PCM_TRANS_N_TO_S,
														   UT_MASTER_NODE, false, &retry_denied));
		UT_ASSERT(retry_denied);
		UT_ASSERT_EQ(memcmp(&entry, &before, sizeof(entry)), 0);
		route_test_assert_public_target_slot_is_canonical();
		if (mode == 3) {
			pg_atomic_write_u64(&entry.generation, 6);
			pg_atomic_write_u64(&entry.reservation_token, 9);
			pg_atomic_write_u32(&entry.flags, 0);
			capacity_buffer.pcm_state = PCM_STATE_S;
			capacity_buffer.buffer_type = BUF_TYPE_SCUR;
			memcpy(&before, &entry, sizeof(before));
		}
		capacity_gate_open = mode != 1;
		retry_latch_cancel = mode == 2;
		capacity_content_held = mode == 4;
		capacity_wait_active = true;
		route_ereport_armed = true;
		if (sigsetjmp(jump, 1) == 0) {
			PG_exception_stack = &jump;
			result = cluster_bufmgr_pcm_retry_denied_rearm(
				&capacity_buffer, PCM_LOCK_MODE_S, &base, &token, 0, &barrier, &covered_generation);
		} else
			caught = true;
		PG_exception_stack = saved_stack;
		route_ereport_armed = false;
		capacity_wait_active = false;
		UT_ASSERT_EQ(caught, mode == 2 || mode == 4);
		UT_ASSERT_EQ(retry_latch_wait_calls, mode == 0 || mode == 2 ? 1 : 0);
		if (mode == 0) {
			UT_ASSERT_EQ(result, CLUSTER_BUFMGR_PCM_RETRY_REARMED);
			UT_ASSERT_EQ(token, 9);
			UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
			UT_ASSERT_EQ(base.generation, 5);
			UT_ASSERT(!cluster_gcs_send_block_request_and_wait(
				&capacity_buffer, PCM_TRANS_N_TO_S, UT_MASTER_NODE, false, &retry_denied));
			UT_ASSERT(retry_denied);
			UT_ASSERT_EQ(requester_send.calls, 2);
			UT_ASSERT_EQ(requester_send.request_ids[0], UINT64_C(0x0200000000000002));
			UT_ASSERT_EQ(requester_send.request_ids[1], UINT64_C(0x0200000000000003));
			UT_ASSERT_EQ(cluster_pcm_own_abort_grant_reservation(&capacity_buffer, &base, token),
						 CLUSTER_PCM_OWN_OK);
		} else if (mode == 1) {
			UT_ASSERT_EQ(result, CLUSTER_BUFMGR_PCM_RETRY_BARRIER_REFUSED);
			UT_ASSERT(barrier);
		} else if (mode == 3) {
			UT_ASSERT_EQ(result, CLUSTER_BUFMGR_PCM_RETRY_COVERED);
			UT_ASSERT_EQ(covered_generation, 6);
			UT_ASSERT_EQ(memcmp(&entry, &before, sizeof(entry)), 0);
		}
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
		UT_ASSERT_EQ(requester_send.done_calls, 0);
		route_test_assert_public_target_slot_is_canonical();
	}
	capacity_content_held = false;
	capacity_gate_open = true;
	ClusterPcmOwnArray = saved_own;
	NBuffers = saved_buffers;
	cluster_node_id = saved_node;
	cluster_gcs_block_retransmit_max_retries = saved_retries;
	route_test_reset_public_target_requester();
}

/* Neither lost responses nor outbound backpressure authorize parallel
 * physical requests.  All three sends must carry exactly the same ID. */
UT_TEST(test_target_wrapper_lost_reply_and_backpressure_redrive_same_id)
{
	BufferTag tag = route_test_tag();
	int saved_node = cluster_node_id;
	int saved_timeout = cluster_gcs_reply_timeout_ms;
	int mode;

	cluster_node_id = UT_REQUESTER_NODE;
	cluster_gcs_reply_timeout_ms = 1;
	for (mode = 0; mode < 2; mode++) {
		char page[BLCKSZ];
		char expected[BLCKSZ];
		ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
		int i;

		route_test_reset_public_target_requester();
		if (mode == 0)
			requester_send.suppress_reply_calls = 2;
		else
			requester_send.refuse_enqueue_calls = 2;
		memset(page, 0xa7, sizeof(page));
		memset(expected, 0x6d, sizeof(expected));
		UT_ASSERT_EQ(cluster_gcs_block_cr_fetch_and_wait(tag, UT_READ_SCN, page, &reason),
					 CLUSTER_CR_BUILD_FULL);
		UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
		UT_ASSERT_EQ(requester_send.calls, 3);
		for (i = 0; i < 3; i++) {
			UT_ASSERT_EQ(requester_send.request_ids[i], UINT64_C(0x0200000000000002));
			UT_ASSERT(requester_send.slot_armed_by_call[i]);
			UT_ASSERT_EQ(requester_send.slot_domain_by_call[i], UT_REPLY_DOMAIN_R4_CR);
		}
		UT_ASSERT_EQ(reply_cv_prepare_calls, mode == 0 ? 3 : 1);
		UT_ASSERT_EQ(reply_cv_cancel_calls, reply_cv_prepare_calls);
		UT_ASSERT_EQ(reply_cv_signal_calls, 1);
		UT_ASSERT_EQ(retry_latch_wait_calls, mode == 0 ? 0 : 2);
		UT_ASSERT_EQ(route_seam.enter_calls, 1);
		UT_ASSERT_EQ(route_seam.leave_calls, 1);
		UT_ASSERT_EQ(memcmp(page, expected, sizeof(page)), 0);
		UT_ASSERT_EQ(requester_send.done_calls, 1);
		route_test_assert_public_target_slot_is_canonical();
	}
	cluster_gcs_reply_timeout_ms = saved_timeout;
	cluster_node_id = saved_node;
}

/* Exercise cancellation in the CV, after a terminal reply released its
 * slot, and while an unqueued request still owns its slot. */
UT_TEST(test_target_wrapper_wait_cancellation_leaves_admission_and_slot)
{
	BufferTag tag = route_test_tag();
	int saved_node = cluster_node_id;
	int mode;

	cluster_node_id = UT_REQUESTER_NODE;
	for (mode = 0; mode < 3; mode++) {
		char page[BLCKSZ];
		char before[BLCKSZ];
		ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
		sigjmp_buf jump;
		sigjmp_buf *saved_stack = PG_exception_stack;
		volatile bool caught = false;

		route_test_reset_public_target_requester();
		if (mode == 0) {
			requester_send.suppress_reply = true;
			reply_cv_timed_sleep_raise = true;
		} else {
			retry_latch_cancel = true;
			if (mode == 1)
				route_test_first_reply_retry();
			else
				requester_send.refuse_enqueue_calls = 1;
		}
		memset(page, 0xa7, sizeof(page));
		memcpy(before, page, sizeof(before));
		if (sigsetjmp(jump, 1) == 0) {
			PG_exception_stack = &jump;
			(void)cluster_gcs_block_cr_fetch_and_wait(tag, UT_READ_SCN, page, &reason);
		} else
			caught = true;
		PG_exception_stack = saved_stack;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(requester_send.calls, 1);
		UT_ASSERT_EQ(process_interrupt_calls, mode == 0 ? 0 : 1);
		UT_ASSERT_EQ(reply_cv_cancel_calls, 1);
		UT_ASSERT_EQ(route_seam.enter_calls, 1);
		UT_ASSERT_EQ(route_seam.leave_calls, 1);
		UT_ASSERT_EQ(memcmp(page, before, sizeof(page)), 0);
		/* Only mode 1 consumed an exact terminal 25 before caller cancel. */
		UT_ASSERT_EQ(requester_send.done_calls, mode == 1 ? 1 : 0);
		route_test_assert_public_target_slot_is_canonical();
	}
	route_test_reset_public_target_requester();
	cluster_node_id = saved_node;
}

/* Original epoch/admission cannot be replaced during this logical read,
 * including a wake which has not received any response yet. */
UT_TEST(test_target_wrapper_wait_drift_never_adopts_new_identity)
{
	BufferTag tag = route_test_tag();
	int saved_node = cluster_node_id;
	int mode;

	cluster_node_id = UT_REQUESTER_NODE;
	for (mode = 0; mode < 4; mode++) {
		char page[BLCKSZ];
		char before[BLCKSZ];
		ClusterCrBuildReason reason = CLUSTER_CR_BUILD_NONE;

		route_test_reset_public_target_requester();
		if (mode == 0) {
			route_test_first_reply_retry();
			retry_latch_epoch_drift = true;
		} else if (mode == 3)
			route_seam.recheck_ok = false;
		else {
			requester_send.suppress_reply = true;
			reply_wait_epoch_drift = mode == 1;
			reply_wait_admission_drift = mode == 2;
		}
		memset(page, 0xa7, sizeof(page));
		memcpy(before, page, sizeof(before));
		UT_ASSERT_EQ(cluster_gcs_block_cr_fetch_and_wait(tag, UT_READ_SCN, page, &reason),
					 CLUSTER_CR_BUILD_RETRYABLE);
		UT_ASSERT_EQ(reason,
					 mode < 2 ? CLUSTER_CR_BUILD_EPOCH_MISMATCH : CLUSTER_CR_BUILD_RF_DEFERRED);
		UT_ASSERT_EQ(requester_send.calls, mode == 3 ? 0 : 1);
		UT_ASSERT_EQ(reply_cv_cancel_calls, mode == 3 ? 0 : 1);
		UT_ASSERT_EQ(route_seam.enter_calls, 1);
		UT_ASSERT_EQ(route_seam.leave_calls, 1);
		UT_ASSERT_EQ(route_seam.left_admission.formation_epoch, UT_FORMATION_EPOCH);
		UT_ASSERT_EQ(requester_send.done_calls, mode == 0 ? 1 : 0);
		UT_ASSERT_EQ(memcmp(page, before, sizeof(page)), 0);
		route_test_assert_public_target_slot_is_canonical();
	}
	route_test_reset_public_target_requester();
	cluster_node_id = saved_node;
}

UT_TEST(test_target_wrapper_denied_and_corrupt_full_never_publish)
{
	BufferTag tag = route_test_tag();
	int saved_node = cluster_node_id;
	int mode;

	cluster_node_id = UT_REQUESTER_NODE;
	for (mode = 0; mode < 3; mode++) {
		char page[BLCKSZ];
		char before[BLCKSZ];
		ClusterCrBuildReason reason = CLUSTER_CR_BUILD_NONE;
		sigjmp_buf jump;
		sigjmp_buf *saved_stack = PG_exception_stack;
		volatile bool caught = false;
		volatile ClusterCrBuildResult result = CLUSTER_CR_BUILD_FULL;

		route_test_reset_public_target_requester();
		if (mode == 0) {
			route_test_first_reply_retry();
			requester_send.reply_steps[0].status = GCS_BLOCK_REPLY_R4_DENIED;
		} else {
			requester_send.corrupt_reply_checksum = mode == 1;
			requester_send.corrupt_reply_identity = mode == 2;
			reply_cv_timed_sleep_raise = true;
		}
		memset(page, 0xa7, sizeof(page));
		memcpy(before, page, sizeof(before));
		if (sigsetjmp(jump, 1) == 0) {
			PG_exception_stack = &jump;
			result = cluster_gcs_block_cr_fetch_and_wait(tag, UT_READ_SCN, page, &reason);
		} else
			caught = true;
		PG_exception_stack = saved_stack;
		UT_ASSERT_EQ(caught, mode != 0);
		if (mode == 0) {
			UT_ASSERT_EQ(result, CLUSTER_CR_BUILD_FAIL_CLOSED);
			UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_PROTOCOL);
		}
		UT_ASSERT_EQ(requester_send.calls, 1);
		UT_ASSERT_EQ(requester_send.done_calls, mode == 0 ? 1 : 0);
		UT_ASSERT_EQ(route_seam.enter_calls, 1);
		UT_ASSERT_EQ(route_seam.leave_calls, 1);
		UT_ASSERT_EQ(memcmp(page, before, sizeof(page)), 0);
		route_test_assert_public_target_slot_is_canonical();
	}
	route_test_reset_public_target_requester();
	cluster_node_id = saved_node;
}

/* TARGET admission is the outermost gate.  Even an invalid read SCN cannot
 * reach validation, master lookup, slot reservation or REQUEST80 staging when
 * the feature is CLOSED, and the public ABI must preserve its typed refusal. */
UT_TEST(test_target_wrapper_disabled_returns_typed_before_lookup_or_wire)
{
	BufferTag tag = route_test_tag();
	char dst_page[BLCKSZ];
	char original_page[BLCKSZ];
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	ClusterCrBuildResult result;
	int saved_node_id = cluster_node_id;

	cluster_node_id = UT_REQUESTER_NODE;
	route_test_reset_public_target_requester();
	route_seam.admission_result = CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED;
	memset(dst_page, 0xa7, sizeof(dst_page));
	memcpy(original_page, dst_page, sizeof(original_page));

	result = cluster_gcs_block_cr_fetch_and_wait(
		tag, InvalidScn, dst_page, &reason);

	UT_ASSERT_EQ(result, CLUSTER_CR_BUILD_RETRYABLE);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_TARGET_DISABLED);
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.lookup_calls, 0);
	UT_ASSERT_EQ(requester_send.calls, 0);
	UT_ASSERT_EQ(reply_lock_acquire_calls, 0);
	UT_ASSERT_EQ(reply_lock_release_calls, 0);
	UT_ASSERT_EQ(route_seam.leave_calls, 0);
	UT_ASSERT_EQ(memcmp(dst_page, original_page, sizeof(dst_page)), 0);
	route_test_assert_public_target_slot_is_canonical();
	cluster_node_id = saved_node_id;
}

/* A transported status 21 is received into wrapper-private scratch.  The
 * caller receives the verified page only after the one TARGET token passes
 * its final recheck, and the raw requester slot is canonical before return. */
UT_TEST(test_target_wrapper_status21_rechecks_private_scratch_then_copies_full)
{
	BufferTag tag = route_test_tag();
	char dst_page[BLCKSZ];
	char expected_page[BLCKSZ];
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	ClusterCrBuildResult result;
	int saved_node_id = cluster_node_id;

	cluster_node_id = UT_REQUESTER_NODE;
	route_test_reset_public_target_requester();
	memset(dst_page, 0xa7, sizeof(dst_page));
	memset(expected_page, 0x6d, sizeof(expected_page));

	result = cluster_gcs_block_cr_fetch_and_wait(
		tag, UT_READ_SCN, dst_page, &reason);

	UT_ASSERT_EQ(result, CLUSTER_CR_BUILD_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(requester_send.calls, 1);
	UT_ASSERT(requester_send.slot_armed);
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.lookup_calls, 1);
	UT_ASSERT_EQ(route_seam.recheck_calls, 4);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT(route_seam.enter_sequence < route_seam.lookup_sequence);
	UT_ASSERT(route_seam.lookup_sequence < requester_send.send_sequence);
	UT_ASSERT(requester_send.send_sequence < route_seam.recheck_sequence);
	UT_ASSERT(route_seam.recheck_sequence < route_seam.leave_sequence);
	UT_ASSERT(route_seam.rechecked_admission_address
			  == route_seam.left_admission_address);
	UT_ASSERT_EQ(memcmp(dst_page, expected_page, sizeof(dst_page)), 0);
	route_test_assert_public_target_slot_is_canonical();
	cluster_node_id = saved_node_id;
}

/* When this backend is the resource master, REQUEST80 must still enter the
 * tag-owning LMS DATA ring.  A backend-side registered dispatch runs in the
 * CONTROL process, whose production plane gate silently drops DATA frames;
 * only the DATA worker may perform the authenticated self loopback after
 * dequeue. */
UT_TEST(test_target_wrapper_local_master_stages_request80_to_data_owner_and_returns_full)
{
	BufferTag tag = route_test_tag();
	ClusterR4CrRequestPayload request;
	char dst_page[BLCKSZ];
	char expected_page[BLCKSZ];
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	ClusterCrBuildResult result;
	SCN decoded_read_scn = InvalidScn;
	int saved_node_id = cluster_node_id;

	cluster_node_id = UT_MASTER_NODE;
	route_test_reset_public_target_requester();
	route_seam.lookup_master_node = UT_MASTER_NODE;
	memset(dst_page, 0xa7, sizeof(dst_page));
	memset(expected_page, 0x6d, sizeof(expected_page));
	memset(&request, 0, sizeof(request));

	result = cluster_gcs_block_cr_fetch_and_wait(
		tag, UT_READ_SCN, dst_page, &reason);
	if (requester_send.payload_len == sizeof(request))
		memcpy(&request, &requester_send.request, sizeof(request));

	UT_ASSERT_EQ(result, CLUSTER_CR_BUILD_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(requester_send.calls, 1);
	UT_ASSERT_EQ(requester_send.msg_type, PGRAC_IC_MSG_GCS_BLOCK_REQUEST);
	UT_ASSERT_EQ(requester_send.dest_node, UT_MASTER_NODE);
	UT_ASSERT_EQ(requester_send.payload_len, sizeof(request));
	UT_ASSERT(requester_send.slot_armed);
	UT_ASSERT_EQ(route_seam.raw_send_calls, 0);
	UT_ASSERT_EQ(route_seam.envelope_build_calls, 0);
	UT_ASSERT_EQ(route_seam.local_dispatch_calls, 0);
	UT_ASSERT(!route_seam.local_request_slot_armed);
	UT_ASSERT_EQ(request.base.epoch, UT_FORMATION_EPOCH);
	UT_ASSERT_EQ(request.base.sender_node, UT_MASTER_NODE);
	UT_ASSERT_EQ(request.base.requester_backend_id, (int32)MyBackendId);
	UT_ASSERT_EQ(request.base.transition_id, PCM_TRANS_N_TO_S);
	UT_ASSERT_EQ(memcmp(&request.base.tag, &tag, sizeof(tag)), 0);
	UT_ASSERT(ClusterR4RequestExtensionGetCr(
		&request.extension, &decoded_read_scn));
	UT_ASSERT_EQ(decoded_read_scn, UT_READ_SCN);
	UT_ASSERT_EQ(route_seam.capability_calls, 0);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 0);
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.lookup_calls, 1);
	UT_ASSERT_EQ(route_seam.recheck_calls, 4);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT(route_seam.enter_sequence < route_seam.lookup_sequence);
	UT_ASSERT(route_seam.lookup_sequence < requester_send.send_sequence);
	UT_ASSERT(requester_send.send_sequence < route_seam.recheck_sequence);
	UT_ASSERT(route_seam.recheck_sequence < route_seam.leave_sequence);
	UT_ASSERT_EQ(reply_cv_prepare_calls, 1);
	UT_ASSERT_EQ(reply_cv_timed_sleep_calls, 0);
	UT_ASSERT_EQ(reply_cv_cancel_calls, 1);
	UT_ASSERT_EQ(reply_cv_signal_calls, 1);
	UT_ASSERT_EQ(memcmp(dst_page, expected_page, sizeof(dst_page)), 0);
	route_test_assert_public_target_slot_is_canonical();
	cluster_node_id = saved_node_id;
}

/* A FULL reply cannot escape its private scratch if TARGET drifts before the
 * final fence.  Drift has its own typed retry reason, still releases the raw
 * requester slot and leaves exactly the token that was rechecked. */
UT_TEST(test_target_wrapper_recheck_drift_keeps_dst_and_canonicalizes_slot)
{
	BufferTag tag = route_test_tag();
	char dst_page[BLCKSZ];
	char original_page[BLCKSZ];
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_NONE;
	ClusterCrBuildResult result;
	int saved_node_id = cluster_node_id;

	cluster_node_id = UT_REQUESTER_NODE;
	route_test_reset_public_target_requester();
	route_seam.recheck_fail_at = 4; /* After the verified FULL, at final publication. */
	memset(dst_page, 0xa7, sizeof(dst_page));
	memcpy(original_page, dst_page, sizeof(original_page));

	result = cluster_gcs_block_cr_fetch_and_wait(
		tag, UT_READ_SCN, dst_page, &reason);

	UT_ASSERT_EQ(result, CLUSTER_CR_BUILD_RETRYABLE);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_RF_DEFERRED);
	UT_ASSERT_EQ(requester_send.calls, 1);
	UT_ASSERT(requester_send.slot_armed);
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.lookup_calls, 1);
	UT_ASSERT_EQ(route_seam.recheck_calls, 4);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT(requester_send.send_sequence < route_seam.recheck_sequence);
	UT_ASSERT(route_seam.recheck_sequence < route_seam.leave_sequence);
	UT_ASSERT(route_seam.rechecked_admission_address
			  == route_seam.left_admission_address);
	UT_ASSERT_EQ(memcmp(dst_page, original_page, sizeof(dst_page)), 0);
	route_test_assert_public_target_slot_is_canonical();
	cluster_node_id = saved_node_id;
}

/* Removing the real request80 branch, taking a second TARGET admission,
 * re-snapshotting PCM, encoding fresh rather than stored proof bytes, using an
 * unbound enqueue, or failing to feed the exact forward96 branch all break
 * this one request -> holder behavior test. */
UT_TEST(test_request80_routes_stored_proof_to_real_forward96_handler)
{
	static const uint8 expected_extension[32] = {
		0x01, 0x01, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
		0x09, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x22, 0x22, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
	};
	ClusterR4CrRequestPayload request = route_test_request80();
	ClusterICEnvelope request_env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, UT_REQUESTER_NODE, UT_MASTER_NODE,
							  sizeof(request));
	ClusterR4CrForwardPayload forwarded;
	ClusterICEnvelope forward_env;
	int saved_node_id = cluster_node_id;

	route_seam_reset();
	UT_ASSERT(cluster_gcs_block_test_r4_request80(&request_env, &request));
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT_EQ(route_seam.capability_calls, 2);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 2);
	UT_ASSERT_EQ(route_seam.capability_peers[0], UT_REQUESTER_NODE);
	UT_ASSERT_EQ(route_seam.capability_peers[1], UT_HOLDER_NODE);
	UT_ASSERT_EQ(route_seam.capability_required[0], UT_R4_REQUIRED_CAPABILITIES);
	UT_ASSERT_EQ(route_seam.capability_required[1], UT_R4_REQUIRED_CAPABILITIES);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 1);
	UT_ASSERT_EQ(route_seam.arm_calls, 1);
	UT_ASSERT_EQ(route_seam.observe_calls, 1);
	UT_ASSERT_EQ(route_seam.observed_event, CLUSTER_R4_EVENT_CR_ROUTE_STARTED);
	UT_ASSERT_EQ(route_seam.observed_tx_reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(route_seam.observed_cr_reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(route_seam.armed_lifetime_hint_ms, 1000);
	UT_ASSERT(route_seam.armed_lifetime_hint_trusted);
	UT_ASSERT_EQ(route_seam.armed_identity.legacy_key.origin_node_id, UT_REQUESTER_NODE);
	UT_ASSERT_EQ(route_seam.armed_identity.legacy_key.requester_backend_id,
				 UT_REQUESTER_BACKEND);
	UT_ASSERT_EQ(route_seam.armed_identity.legacy_key.request_id, UT_REQUEST_ID);
	UT_ASSERT_EQ(route_seam.armed_identity.legacy_key.cluster_epoch, UT_FORMATION_EPOCH);
	UT_ASSERT_EQ(route_seam.armed_identity.read_scn, UT_READ_SCN);
	UT_ASSERT_EQ(route_seam.armed_identity.activation_generation, UT_ACTIVATION_GENERATION);
	UT_ASSERT_EQ(route_seam.armed_proof.formation_epoch, UT_FORMATION_EPOCH);
	UT_ASSERT_EQ(route_seam.armed_proof.activation_generation, UT_ACTIVATION_GENERATION);
	UT_ASSERT_EQ(route_seam.armed_proof.master_authority_generation, UT_MASTER_GENERATION);
	UT_ASSERT_EQ(route_seam.armed_proof.master_resource_transition_count, UT_MASTER_TRANSITION);
	UT_ASSERT_EQ(route_seam.armed_proof.expected_page_scn, UT_EXPECTED_PAGE_SCN);
	UT_ASSERT_EQ(route_seam.armed_proof.real_master_node, UT_MASTER_NODE);
	UT_ASSERT_EQ(route_seam.armed_proof.selected_holder_node, UT_HOLDER_NODE);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 1);
	UT_ASSERT_EQ(route_seam.finish_calls, 1);
	UT_ASSERT_EQ(route_seam.refusal_enqueue_calls, 0);
	UT_ASSERT(route_seam.finished_outbound_admitted);
	UT_ASSERT_EQ(memcmp(&route_seam.finished_identity, &route_seam.armed_identity,
					 sizeof(route_seam.armed_identity)), 0);
	UT_ASSERT_EQ(memcmp(&route_seam.finished_proof, &route_seam.armed_proof,
					 sizeof(route_seam.armed_proof)), 0);
	UT_ASSERT_EQ(route_seam.recheck_calls, 1);
	UT_ASSERT(route_seam.snapshot_sequence < route_seam.arm_sequence);
	UT_ASSERT(route_seam.arm_sequence < route_seam.enqueue_sequence);
	UT_ASSERT(route_seam.enqueue_sequence < route_seam.finish_sequence);
	UT_ASSERT(route_seam.finish_sequence < route_seam.recheck_sequence);
	UT_ASSERT(route_seam.recheck_sequence < route_seam.leave_sequence);

	UT_ASSERT_EQ(route_seam.enqueue_msg_type, PGRAC_IC_MSG_GCS_BLOCK_FORWARD);
	UT_ASSERT_EQ(route_seam.enqueue_dest, UT_HOLDER_NODE);
	UT_ASSERT_EQ(route_seam.enqueue_payload_len, sizeof(ClusterR4CrForwardPayload));
	UT_ASSERT_EQ(route_seam.enqueue_required_capability, UT_R4_REQUIRED_CAPABILITIES);
	UT_ASSERT_EQ(route_seam.enqueue_connection_generation, UT_HOLDER_CAPABILITY_GENERATION);
	memcpy(&forwarded, route_seam.enqueue_payload, sizeof(forwarded));
	UT_ASSERT_EQ(forwarded.base.request_id, UT_REQUEST_ID);
	UT_ASSERT_EQ(forwarded.base.epoch, UT_FORMATION_EPOCH);
	UT_ASSERT_EQ(memcmp(&forwarded.base.tag, &request.base.tag, sizeof(BufferTag)), 0);
	UT_ASSERT_EQ(forwarded.base.original_requester_node, UT_REQUESTER_NODE);
	UT_ASSERT_EQ(forwarded.base.requester_backend_id, UT_REQUESTER_BACKEND);
	UT_ASSERT_EQ(forwarded.base.master_node, UT_MASTER_NODE);
	UT_ASSERT_EQ(forwarded.base.transition_id, PCM_TRANS_N_TO_S);
	UT_ASSERT_EQ(forwarded.base.expected_pi_watermark_scn_bytes[0], 0x34);
	UT_ASSERT_EQ(forwarded.base.expected_pi_watermark_scn_bytes[1], 0x12);
	UT_ASSERT_EQ(forwarded.base.reserved_0[4], 1);
	UT_ASSERT_EQ(memcmp(&forwarded.extension, expected_extension, sizeof(expected_extension)), 0);
	/* Absolute bytes 76..83 are the master-resource transition count. */
	UT_ASSERT_EQ(memcmp(((const uint8 *)&forwarded) + 76, expected_extension + 12, 8), 0);

	forward_env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, UT_MASTER_NODE,
								  UT_HOLDER_NODE, sizeof(forwarded));
	cluster_node_id = UT_HOLDER_NODE;
	route_seam_reset();
	UT_ASSERT(cluster_gcs_block_test_r4_forward96(&forward_env, &forwarded));
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT_EQ(route_seam.capability_calls, 2);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 2);
	UT_ASSERT_EQ(route_seam.capability_peers[0], UT_MASTER_NODE);
	UT_ASSERT_EQ(route_seam.capability_peers[1], UT_REQUESTER_NODE);
	UT_ASSERT_EQ(route_seam.capability_required[0], UT_R4_REQUIRED_CAPABILITIES);
	UT_ASSERT_EQ(route_seam.capability_required[1], UT_R4_REQUIRED_CAPABILITIES);
	UT_ASSERT_EQ(route_seam.peer_open_peers[0], UT_MASTER_NODE);
	UT_ASSERT_EQ(route_seam.peer_open_peers[1], UT_REQUESTER_NODE);
	UT_ASSERT_EQ(route_seam.peer_open_generation[0], UT_MASTER_CAPABILITY_GENERATION);
	UT_ASSERT_EQ(route_seam.peer_open_generation[1], UT_REQUESTER_CAPABILITY_GENERATION);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 0);
	UT_ASSERT_EQ(route_seam.arm_calls, 0);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.refusal_enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 1);
	UT_ASSERT_EQ(memcmp(&route_seam.submitted_forward, &forwarded, sizeof(forwarded)), 0);
	UT_ASSERT(route_seam.submitted_admission.entered);
	UT_ASSERT_EQ(route_seam.submitted_admission.feature_bit,
				 CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(route_seam.submitted_admission.record_generation,
				 UT_ACTIVATION_GENERATION);
	UT_ASSERT_EQ(route_seam.submitted_admission.formation_epoch, UT_FORMATION_EPOCH);
	UT_ASSERT_EQ(route_seam.submitted_admission.side, CLUSTER_SEMANTIC_TARGET_SIDE);
	UT_ASSERT_EQ(route_seam.submitted_requester_capability_generation,
				 UT_REQUESTER_CAPABILITY_GENERATION);
	UT_ASSERT_EQ(route_seam.submitted_master_capability_generation,
				 UT_MASTER_CAPABILITY_GENERATION);
	UT_ASSERT(route_seam.submitted_reason_out_present);
	UT_ASSERT(route_seam.enter_sequence < route_seam.capability_sequence[0]);
	UT_ASSERT(route_seam.capability_sequence[0] < route_seam.capability_sequence[1]);
	UT_ASSERT(route_seam.capability_sequence[1] < route_seam.peer_open_sequence[0]);
	UT_ASSERT(route_seam.peer_open_sequence[0] < route_seam.peer_open_sequence[1]);
	UT_ASSERT(route_seam.peer_open_sequence[1] < route_seam.holder_submit_sequence);
	UT_ASSERT(route_seam.holder_submit_sequence < route_seam.recheck_sequence);
	UT_ASSERT(route_seam.recheck_sequence < route_seam.leave_sequence);
	cluster_node_id = saved_node_id;
}

/* A requester that is also the real master has no peer HELLO record.  It
 * proves the compiled R4 capability under the one entered TARGET token, then
 * samples/binds only the selected remote holder before the cap-bound
 * FORWARD96 enqueue. */
UT_TEST(test_request80_local_master_uses_compiled_capability_and_routes_remote_holder)
{
	ClusterR4CrRequestPayload request = route_test_request80();
	ClusterICEnvelope env;
	ClusterR4CrForwardPayload forwarded;
	int saved_node_id = cluster_node_id;

	cluster_node_id = UT_MASTER_NODE;
	request.base.sender_node = UT_MASTER_NODE;
	env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, UT_MASTER_NODE,
						  UT_MASTER_NODE, sizeof(request));
	route_seam_reset();

	UT_ASSERT(cluster_gcs_block_test_r4_request80(&env, &request));
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT(route_seam.capability_calls == 1
			  && route_seam.capability_peers[0] == UT_HOLDER_NODE
			  && route_seam.capability_required[0] == UT_R4_REQUIRED_CAPABILITIES
			  && route_seam.peer_open_calls == 1
			  && route_seam.peer_open_peers[0] == UT_HOLDER_NODE
			  && route_seam.peer_open_required[0] == UT_R4_REQUIRED_CAPABILITIES
			  && route_seam.peer_open_generation[0] == UT_HOLDER_CAPABILITY_GENERATION
			  && route_seam.enter_sequence < route_seam.snapshot_sequence
			  && route_seam.snapshot_sequence < route_seam.capability_sequence[0]
			  && route_seam.capability_sequence[0] < route_seam.peer_open_sequence[0]);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 1);
	UT_ASSERT_EQ(route_seam.arm_calls, 1);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 1);
	UT_ASSERT_EQ(route_seam.finish_calls, 1);
	UT_ASSERT_EQ(route_seam.refusal_enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.recheck_calls, 1);
	UT_ASSERT(route_seam.finished_outbound_admitted);
	UT_ASSERT_EQ(route_seam.armed_identity.legacy_key.origin_node_id, UT_MASTER_NODE);
	UT_ASSERT_EQ(route_seam.armed_identity.activation_generation,
				 UT_ACTIVATION_GENERATION);
	UT_ASSERT_EQ(route_seam.armed_proof.activation_generation,
				 UT_ACTIVATION_GENERATION);
	UT_ASSERT_EQ(route_seam.armed_proof.real_master_node, UT_MASTER_NODE);
	UT_ASSERT_EQ(route_seam.armed_proof.selected_holder_node, UT_HOLDER_NODE);
	UT_ASSERT_EQ(route_seam.armed_lifetime_hint_ms, 1000);
	UT_ASSERT(route_seam.armed_lifetime_hint_trusted);
	UT_ASSERT_EQ(route_seam.enqueue_msg_type, PGRAC_IC_MSG_GCS_BLOCK_FORWARD);
	UT_ASSERT_EQ(route_seam.enqueue_dest, UT_HOLDER_NODE);
	UT_ASSERT_EQ(route_seam.enqueue_payload_len, sizeof(ClusterR4CrForwardPayload));
	UT_ASSERT_EQ(route_seam.enqueue_required_capability, UT_R4_REQUIRED_CAPABILITIES);
	UT_ASSERT_EQ(route_seam.enqueue_connection_generation,
				 UT_HOLDER_CAPABILITY_GENERATION);
	memcpy(&forwarded, route_seam.enqueue_payload, sizeof(forwarded));
	UT_ASSERT_EQ(forwarded.base.original_requester_node, UT_MASTER_NODE);
	UT_ASSERT_EQ(forwarded.base.master_node, UT_MASTER_NODE);
	UT_ASSERT_EQ(forwarded.base.request_id, UT_REQUEST_ID);
	UT_ASSERT_EQ(forwarded.base.epoch, UT_FORMATION_EPOCH);
	UT_ASSERT_EQ(memcmp(&forwarded.base.tag, &request.base.tag, sizeof(BufferTag)), 0);
	UT_ASSERT(route_seam.arm_sequence < route_seam.enqueue_sequence);
	UT_ASSERT(route_seam.enqueue_sequence < route_seam.finish_sequence);
	UT_ASSERT(route_seam.finish_sequence < route_seam.recheck_sequence);
	UT_ASSERT(route_seam.recheck_sequence < route_seam.leave_sequence);
	cluster_node_id = saved_node_id;
}

/* With requester, real master and selected holder all local, both capability
 * identities are compiled under the master's one TARGET token.  The stored
 * FORWARD96 proof must enter the registered local envelope dispatcher; the
 * peer sampler, cap-bound ring and generic IC self-send have no local edge. */
UT_TEST(test_request80_all_local_routes_forward96_through_registered_dispatch)
{
	ClusterR4CrRequestPayload request = route_test_request80();
	ClusterICEnvelope env;
	ClusterR4CrForwardPayload forwarded;
	uint64 master_authority_generation = 0;
	uint64 master_resource_transition_count = 0;
	SCN expected_page_scn = InvalidScn;
	bool proof_ok;
	int saved_node_id = cluster_node_id;

	cluster_node_id = UT_HOLDER_NODE;
	request.base.sender_node = UT_HOLDER_NODE;
	env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, UT_HOLDER_NODE,
						  UT_HOLDER_NODE, sizeof(request));
	route_seam_reset();
	route_seam.lookup_master_node = UT_HOLDER_NODE;

	UT_ASSERT(cluster_gcs_block_test_r4_request80(&env, &request));
	memcpy(&forwarded, route_seam.local_dispatch_payload, sizeof(forwarded));
	proof_ok = ClusterR4ForwardExtensionGetCrProof(
		&forwarded.extension, UT_FORMATION_EPOCH,
		&master_authority_generation, &master_resource_transition_count,
		&expected_page_scn);

	UT_ASSERT(route_seam.capability_calls == 0
			  && route_seam.peer_open_calls == 0
			  && route_seam.enqueue_calls == 0
			  && route_seam.raw_send_calls == 0
			  && route_seam.envelope_build_calls == 1
			  && route_seam.local_dispatch_calls == 1
			  && route_seam.local_dispatch_peer == UT_HOLDER_NODE
			  && route_seam.local_dispatch_envelope.msg_type
				 == PGRAC_IC_MSG_GCS_BLOCK_FORWARD
			  && route_seam.local_dispatch_envelope.source_node_id == UT_HOLDER_NODE
			  && route_seam.local_dispatch_envelope.dest_node_id == UT_HOLDER_NODE
			  && route_seam.local_dispatch_envelope.epoch == UT_FORMATION_EPOCH
			  && route_seam.local_dispatch_payload_len
				 == sizeof(ClusterR4CrForwardPayload)
			  && forwarded.base.request_id == UT_REQUEST_ID
			  && forwarded.base.epoch == UT_FORMATION_EPOCH
			  && memcmp(&forwarded.base.tag, &request.base.tag, sizeof(BufferTag)) == 0
			  && forwarded.base.original_requester_node == UT_HOLDER_NODE
			  && forwarded.base.requester_backend_id == UT_REQUESTER_BACKEND
			  && forwarded.base.master_node == UT_HOLDER_NODE
			  && forwarded.base.transition_id == PCM_TRANS_N_TO_S
			  && proof_ok
			  && master_authority_generation == UT_MASTER_GENERATION
			  && master_resource_transition_count == UT_MASTER_TRANSITION
			  && expected_page_scn == UT_EXPECTED_PAGE_SCN
			  && route_seam.enter_sequence < route_seam.snapshot_sequence
			  && route_seam.snapshot_sequence < route_seam.arm_sequence
			  && route_seam.arm_sequence < route_seam.envelope_build_sequence
			  && route_seam.envelope_build_sequence < route_seam.local_dispatch_sequence
			  && route_seam.local_dispatch_sequence < route_seam.finish_sequence);
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 1);
	UT_ASSERT_EQ(route_seam.arm_calls, 1);
	UT_ASSERT_EQ(route_seam.finish_calls, 1);
	UT_ASSERT_EQ(route_seam.refusal_enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.recheck_calls, 1);
	UT_ASSERT(route_seam.finished_outbound_admitted);
	UT_ASSERT_EQ(route_seam.armed_identity.legacy_key.origin_node_id, UT_HOLDER_NODE);
	UT_ASSERT_EQ(route_seam.armed_identity.activation_generation,
				 UT_ACTIVATION_GENERATION);
	UT_ASSERT_EQ(route_seam.armed_proof.activation_generation,
				 UT_ACTIVATION_GENERATION);
	UT_ASSERT_EQ(route_seam.armed_proof.real_master_node, UT_HOLDER_NODE);
	UT_ASSERT_EQ(route_seam.armed_proof.selected_holder_node, UT_HOLDER_NODE);
	UT_ASSERT_EQ(memcmp(&route_seam.finished_identity, &route_seam.armed_identity,
					 sizeof(route_seam.armed_identity)), 0);
	UT_ASSERT_EQ(memcmp(&route_seam.finished_proof, &route_seam.armed_proof,
					 sizeof(route_seam.armed_proof)), 0);
	UT_ASSERT(route_seam.finish_sequence < route_seam.recheck_sequence);
	UT_ASSERT(route_seam.recheck_sequence < route_seam.leave_sequence);
	cluster_node_id = saved_node_id;
}

/* The all-local holder decoder has no peer HELLO identity to sample.  Both
 * authenticated local actors prove the compiled family under the one entered
 * TARGET token, whose checked OPEN generation is the exact uint32 carrier for
 * both sides of the typed holder submit. */
UT_TEST(test_forward96_all_local_uses_checked_open_generation_for_both_submit_identities)
{
	ClusterR4CrForwardPayload forward = route_test_forward96();
	ClusterICEnvelope env;
	int saved_node_id = cluster_node_id;

	cluster_node_id = UT_HOLDER_NODE;
	forward.base.original_requester_node = UT_HOLDER_NODE;
	forward.base.master_node = UT_HOLDER_NODE;
	env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, UT_HOLDER_NODE,
							  UT_HOLDER_NODE, sizeof(forward));
	route_seam_reset();
	route_seam.activation_generation = UT_LOCAL_OPEN_GENERATION;
	route_seam.lookup_master_node = UT_HOLDER_NODE;

	UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, &forward));
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.capability_calls, 0);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 0);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 0);
	UT_ASSERT_EQ(route_seam.arm_calls, 0);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.raw_send_calls, 0);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 1);
	UT_ASSERT_EQ(route_seam.refusal_enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.recheck_calls, 1);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT_EQ(memcmp(&route_seam.submitted_forward, &forward, sizeof(forward)), 0);
	UT_ASSERT(route_seam.submitted_reason_out_present);
	UT_ASSERT(route_seam.submitted_admission.entered);
	UT_ASSERT_EQ(route_seam.submitted_admission.feature_bit,
				 CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(route_seam.submitted_admission.record_generation,
				 UT_LOCAL_OPEN_GENERATION);
	UT_ASSERT_EQ(route_seam.submitted_admission.formation_epoch, UT_FORMATION_EPOCH);
	UT_ASSERT_EQ(route_seam.submitted_admission.side, CLUSTER_SEMANTIC_TARGET_SIDE);
	UT_ASSERT_EQ(route_seam.submitted_requester_capability_generation,
				 (uint32)UT_LOCAL_OPEN_GENERATION);
	UT_ASSERT_EQ(route_seam.submitted_master_capability_generation,
				 (uint32)UT_LOCAL_OPEN_GENERATION);
	UT_ASSERT(route_seam.submitted_admission_address != NULL);
	UT_ASSERT(route_seam.submitted_admission_address
			  == route_seam.rechecked_admission_address);
	UT_ASSERT(route_seam.submitted_admission_address == route_seam.left_admission_address);
	UT_ASSERT_EQ(memcmp(&route_seam.submitted_admission,
					 &route_seam.rechecked_admission,
					 sizeof(route_seam.submitted_admission)), 0);
	UT_ASSERT_EQ(memcmp(&route_seam.submitted_admission, &route_seam.left_admission,
					 sizeof(route_seam.submitted_admission)), 0);
	UT_ASSERT(route_seam.enter_sequence < route_seam.holder_submit_sequence);
	UT_ASSERT(route_seam.holder_submit_sequence < route_seam.recheck_sequence);
	UT_ASSERT(route_seam.recheck_sequence < route_seam.leave_sequence);
	cluster_node_id = saved_node_id;
}

/* A local OPEN generation that cannot be represented by the frozen uint32
 * slot carrier is refused without truncation before typed submit can mutate a
 * slot.  The exact FORWARD96 frame remains consumed and the entered token is
 * still left once by its holder decoder. */
UT_TEST(test_forward96_all_local_refuses_open_generation_overflow_before_submit)
{
	ClusterR4CrForwardPayload forward = route_test_forward96();
	ClusterICEnvelope env;
	int saved_node_id = cluster_node_id;

	cluster_node_id = UT_HOLDER_NODE;
	forward.base.original_requester_node = UT_HOLDER_NODE;
	forward.base.master_node = UT_HOLDER_NODE;
	env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, UT_HOLDER_NODE,
							  UT_HOLDER_NODE, sizeof(forward));
	route_seam_reset();
	route_seam.activation_generation = UT_LOCAL_OPEN_GENERATION_OVERFLOW;
	route_seam.lookup_master_node = UT_HOLDER_NODE;

	UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, &forward));
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.capability_calls, 0);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 0);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
	UT_ASSERT(!route_seam.submitted_reason_out_present);
	UT_ASSERT(route_seam.submitted_admission_address == NULL);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 0);
	UT_ASSERT_EQ(route_seam.arm_calls, 0);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT(route_seam.enter_sequence < route_seam.leave_sequence);
	cluster_node_id = saved_node_id;
}

/* A holder-side typed failure is consumed only after the submit result and
 * the decoder's final token recheck.  Reusing the master-side refusal shape,
 * replying to env.source, or publishing after leave breaks this boundary. */
UT_TEST(test_forward96_holder_submit_failure_publishes_typed_remote_refusal)
{
	static const struct {
		ClusterCrBuildResult result;
		ClusterCrBuildReason reason;
		GcsBlockReplyStatus status;
	} cases[] = {
		{ CLUSTER_CR_BUILD_RETRYABLE, CLUSTER_CR_BUILD_HOLDER_MOVED,
		  GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED },
		{ CLUSTER_CR_BUILD_FAIL_CLOSED, CLUSTER_CR_BUILD_PROTOCOL,
		  GCS_BLOCK_REPLY_R4_DENIED },
		/* A polarity-invalid pair must never inherit FULL or retry semantics. */
		{ CLUSTER_CR_BUILD_FULL, CLUSTER_CR_BUILD_HOLDER_MOVED,
		  GCS_BLOCK_REPLY_R4_DENIED }
	};
	ClusterR4CrForwardPayload forward = route_test_forward96();
	ClusterICEnvelope env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, UT_MASTER_NODE,
							  UT_HOLDER_NODE, sizeof(forward));
	int saved_node_id = cluster_node_id;
	int i;

	cluster_node_id = UT_HOLDER_NODE;
	for (i = 0; i < lengthof(cases); i++) {
		route_seam_reset();
		route_seam.holder_submit_result = cases[i].result;
		route_seam.holder_submit_reason = cases[i].reason;

		UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, &forward));
		UT_ASSERT_EQ(route_seam.enter_calls, 1);
		UT_ASSERT_EQ(route_seam.leave_calls, 1);
		UT_ASSERT_EQ(route_seam.capability_calls, 2);
		UT_ASSERT_EQ(route_seam.peer_open_calls, 2);
		UT_ASSERT_EQ(route_seam.holder_submit_calls, 1);
		UT_ASSERT(route_seam.submitted_reason_out_present);
		UT_ASSERT_EQ(route_seam.submitted_requester_capability_generation,
					 UT_REQUESTER_CAPABILITY_GENERATION);
		UT_ASSERT_EQ(route_seam.submitted_master_capability_generation,
					 UT_MASTER_CAPABILITY_GENERATION);
		UT_ASSERT_EQ(route_seam.recheck_calls, 1);
		UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
		route_assert_holder_refusal(cases[i].status);
		UT_ASSERT(route_seam.holder_submit_sequence < route_seam.recheck_sequence);
		UT_ASSERT(route_seam.recheck_sequence < route_seam.refusal_enqueue_sequence);
		UT_ASSERT(route_seam.refusal_enqueue_sequence < route_seam.leave_sequence);
	}
	cluster_node_id = saved_node_id;
}

/* These hooks are exact-extended-shape classifiers: an exact R4 frame is
 * consumed even when its capability gate refuses it, while legacy and
 * malformed lengths remain for the outer dispatcher/drop path. */
UT_TEST(test_r4_try_handlers_consume_only_exact_extended_lengths)
{
	uint8 bytes[sizeof(ClusterR4CrForwardPayload)];
	ClusterICEnvelope env;

	memset(bytes, 0, sizeof(bytes));
	route_seam_reset();
	route_seam.capability_ok = false;

	env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, UT_REQUESTER_NODE, UT_MASTER_NODE,
						  sizeof(ClusterR4CrRequestPayload));
	UT_ASSERT(cluster_gcs_block_test_r4_request80(&env, bytes));
	UT_ASSERT_EQ(route_seam.refusal_enqueue_calls, 0);
	env.payload_length = sizeof(GcsBlockRequestPayload);
	UT_ASSERT(!cluster_gcs_block_test_r4_request80(&env, bytes));
	env.payload_length = sizeof(ClusterR4CrRequestPayload) - 1;
	UT_ASSERT(!cluster_gcs_block_test_r4_request80(&env, bytes));
	env.payload_length = sizeof(ClusterR4CrRequestPayload) + 1;
	UT_ASSERT(!cluster_gcs_block_test_r4_request80(&env, bytes));

	env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, UT_MASTER_NODE, UT_HOLDER_NODE,
						  sizeof(ClusterR4CrForwardPayload));
	UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, bytes));
	env.payload_length = sizeof(GcsBlockForwardPayload);
	UT_ASSERT(!cluster_gcs_block_test_r4_forward96(&env, bytes));
	env.payload_length = sizeof(ClusterR4CrForwardPayload) - 1;
	UT_ASSERT(!cluster_gcs_block_test_r4_forward96(&env, bytes));
	env.payload_length = sizeof(ClusterR4CrForwardPayload) + 1;
	UT_ASSERT(!cluster_gcs_block_test_r4_forward96(&env, bytes));
}

/* A negotiated-capability sample that cannot be joined to the same committed
 * OPEN generation is consumed fail-closed before PCM/dedup/ring/holder state.
 * Deleting either matcher call or moving mutation ahead of it breaks this. */
UT_TEST(test_r4_same_open_mismatch_has_zero_route_or_holder_mutation)
{
	ClusterR4CrRequestPayload request = route_test_request80();
	ClusterR4CrForwardPayload forward = route_test_forward96();
	ClusterICEnvelope env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, UT_REQUESTER_NODE, UT_MASTER_NODE,
							  sizeof(request));
	int saved_node_id = cluster_node_id;

	route_seam_reset();
	route_seam.peer_open_ok = false;
	UT_ASSERT(cluster_gcs_block_test_r4_request80(&env, &request));
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 1);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 0);
	UT_ASSERT_EQ(route_seam.arm_calls, 0);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.finish_calls, 0);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
	UT_ASSERT_EQ(route_seam.recheck_calls, 1);
	route_assert_refusal(GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED, 0);
	UT_ASSERT_EQ(route_seam.refusal_observe_calls, 1);
	UT_ASSERT_EQ(route_seam.refusal_stage, CLUSTER_R4_REFUSAL_MASTER);
	UT_ASSERT_EQ(route_seam.refusal_reason, CLUSTER_CR_BUILD_RF_DEFERRED);
	UT_ASSERT_EQ(route_seam.refusal_request_id, UT_REQUEST_ID);
	UT_ASSERT_EQ(route_seam.refusal_epoch, UT_FORMATION_EPOCH);
	UT_ASSERT_EQ(route_seam.refusal_read_scn, UT_READ_SCN);
	UT_ASSERT_EQ(memcmp(&route_seam.refusal_tag, &request.base.tag, sizeof(BufferTag)), 0);

	env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, UT_MASTER_NODE, UT_HOLDER_NODE,
						  sizeof(forward));
	cluster_node_id = UT_HOLDER_NODE;
	route_seam_reset();
	route_seam.peer_open_ok = false;
	UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, &forward));
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.capability_calls, 2);
	UT_ASSERT_EQ(route_seam.capability_peers[0], UT_MASTER_NODE);
	UT_ASSERT_EQ(route_seam.capability_peers[1], UT_REQUESTER_NODE);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 1);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 0);
	UT_ASSERT_EQ(route_seam.arm_calls, 0);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.finish_calls, 0);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
	UT_ASSERT_EQ(route_seam.recheck_calls, 0);
	UT_ASSERT_EQ(route_seam.refusal_enqueue_calls, 0);
	cluster_node_id = saved_node_id;
}

/* Once the requester capability generation has been sampled, admission
 * refusal is an authenticated pre-token status-25 outcome.  It must not
 * acquire/recheck/leave a token or touch PCM, dedup, forwarding, or holder
 * state. */
UT_TEST(test_request80_admission_refusals_publish_without_token)
{
	static const ClusterSemanticAdmissionResult refusals[] = {
		CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED,
		CLUSTER_SEMANTIC_ADMISSION_CLOSED,
		CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED
	};
	ClusterR4CrRequestPayload request = route_test_request80();
	ClusterICEnvelope env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, UT_REQUESTER_NODE, UT_MASTER_NODE,
							  sizeof(request));
	int i;

	for (i = 0; i < lengthof(refusals); i++) {
		route_seam_reset();
		route_seam.admission_result = refusals[i];
		UT_ASSERT(cluster_gcs_block_test_r4_request80(&env, &request));
		UT_ASSERT_EQ(route_seam.capability_calls, 1);
		UT_ASSERT_EQ(route_seam.capability_peers[0], UT_REQUESTER_NODE);
		UT_ASSERT_EQ(route_seam.enter_calls, 1);
		UT_ASSERT_EQ(route_seam.peer_open_calls, 0);
		UT_ASSERT_EQ(route_seam.leave_calls, 0);
		UT_ASSERT_EQ(route_seam.recheck_calls, 0);
		UT_ASSERT_EQ(route_seam.snapshot_calls, 0);
		UT_ASSERT_EQ(route_seam.arm_calls, 0);
		UT_ASSERT_EQ(route_seam.observe_calls, 0);
		UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
		UT_ASSERT_EQ(route_seam.finish_calls, 0);
		UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
		route_assert_refusal(GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED, 0);
	}
}

UT_TEST(test_request80_sender_mismatch_is_consumed_without_route_mutation)
{
	ClusterR4CrRequestPayload request = route_test_request80();
	ClusterICEnvelope env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, UT_REQUESTER_NODE, UT_MASTER_NODE,
							  sizeof(request));

	request.base.sender_node = UT_HOLDER_NODE;
	route_seam_reset();
	UT_ASSERT_EQ(env.payload_length, 80);
	UT_ASSERT(cluster_gcs_block_test_r4_request80(&env, &request));
	UT_ASSERT_EQ(route_seam.capability_calls, 1);
	UT_ASSERT_EQ(route_seam.capability_peers[0], UT_REQUESTER_NODE);
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 1);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 0);
	UT_ASSERT_EQ(route_seam.arm_calls, 0);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.finish_calls, 0);
	UT_ASSERT_EQ(route_seam.recheck_calls, 1);
	route_assert_refusal(GCS_BLOCK_REPLY_R4_DENIED, 0);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
}

UT_TEST(test_request80_reserved_extension_is_consumed_without_route_mutation)
{
	ClusterR4CrRequestPayload request = route_test_request80();
	ClusterICEnvelope env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, UT_REQUESTER_NODE, UT_MASTER_NODE,
							  sizeof(request));

	request.extension.reserved[0] = 1;
	route_seam_reset();
	UT_ASSERT_EQ(env.payload_length, 80);
	UT_ASSERT(cluster_gcs_block_test_r4_request80(&env, &request));
	UT_ASSERT_EQ(route_seam.capability_calls, 1);
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 1);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 0);
	UT_ASSERT_EQ(route_seam.arm_calls, 0);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.finish_calls, 0);
	UT_ASSERT_EQ(route_seam.recheck_calls, 1);
	route_assert_refusal(GCS_BLOCK_REPLY_R4_DENIED, 0);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
}

UT_TEST(test_request80_epoch_mismatch_is_consumed_without_route_mutation)
{
	ClusterR4CrRequestPayload request = route_test_request80();
	ClusterICEnvelope env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, UT_REQUESTER_NODE, UT_MASTER_NODE,
							  sizeof(request));

	request.base.epoch = UT_FORMATION_EPOCH + 1;
	route_seam_reset();
	UT_ASSERT_EQ(env.payload_length, 80);
	UT_ASSERT(cluster_gcs_block_test_r4_request80(&env, &request));
	UT_ASSERT_EQ(route_seam.capability_calls, 1);
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 1);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 0);
	UT_ASSERT_EQ(route_seam.arm_calls, 0);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.finish_calls, 0);
	UT_ASSERT_EQ(route_seam.recheck_calls, 1);
	route_assert_refusal(GCS_BLOCK_REPLY_R4_DENIED, 0);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
}

UT_TEST(test_request80_wrong_master_is_consumed_before_snapshot)
{
	ClusterR4CrRequestPayload request = route_test_request80();
	ClusterICEnvelope env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, UT_REQUESTER_NODE, UT_MASTER_NODE,
							  sizeof(request));

	route_seam_reset();
	route_seam.lookup_master_node = 0;
	UT_ASSERT_EQ(env.payload_length, 80);
	UT_ASSERT(cluster_gcs_block_test_r4_request80(&env, &request));
	UT_ASSERT_EQ(route_seam.capability_calls, 1);
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 1);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 0);
	UT_ASSERT_EQ(route_seam.arm_calls, 0);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.finish_calls, 0);
	UT_ASSERT_EQ(route_seam.recheck_calls, 1);
	route_assert_refusal(GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED, 1);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
}

UT_TEST(test_request80_snapshot_failure_stops_before_route_arm)
{
	ClusterR4CrRequestPayload request = route_test_request80();
	ClusterICEnvelope env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, UT_REQUESTER_NODE, UT_MASTER_NODE,
							  sizeof(request));

	route_seam_reset();
	route_seam.snapshot_ok = false;
	UT_ASSERT_EQ(env.payload_length, 80);
	UT_ASSERT(cluster_gcs_block_test_r4_request80(&env, &request));
	UT_ASSERT_EQ(route_seam.capability_calls, 1);
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 1);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 1);
	UT_ASSERT_EQ(route_seam.arm_calls, 0);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.finish_calls, 0);
	UT_ASSERT_EQ(route_seam.recheck_calls, 1);
	route_assert_refusal(GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED, 0);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT(route_seam.snapshot_sequence < route_seam.leave_sequence);
}

UT_TEST(test_request80_arm_full_stops_before_publication)
{
	ClusterR4CrRequestPayload request = route_test_request80();
	ClusterICEnvelope env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, UT_REQUESTER_NODE, UT_MASTER_NODE,
							  sizeof(request));

	route_seam_reset();
	route_seam.arm_result = GCS_BLOCK_R4_ROUTE_ARM_FULL;
	UT_ASSERT_EQ(env.payload_length, 80);
	UT_ASSERT(cluster_gcs_block_test_r4_request80(&env, &request));
	UT_ASSERT_EQ(route_seam.capability_calls, 2);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 2);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 1);
	UT_ASSERT_EQ(route_seam.arm_calls, 1);
	UT_ASSERT_EQ(route_seam.observe_calls, 0);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.finish_calls, 0);
	UT_ASSERT_EQ(route_seam.recheck_calls, 1);
	route_assert_refusal(GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED, 0);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT(route_seam.snapshot_sequence < route_seam.arm_sequence);
	UT_ASSERT(route_seam.arm_sequence < route_seam.leave_sequence);
}

UT_TEST(test_request80_enqueue_refusal_finishes_unadmitted_once)
{
	ClusterR4CrRequestPayload request = route_test_request80();
	ClusterICEnvelope env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, UT_REQUESTER_NODE, UT_MASTER_NODE,
							  sizeof(request));

	route_seam_reset();
	route_seam.enqueue_ok = false;
	route_seam.finish_result = GCS_BLOCK_R4_ROUTE_SEND_RETRYABLE;
	UT_ASSERT_EQ(env.payload_length, 80);
	UT_ASSERT(cluster_gcs_block_test_r4_request80(&env, &request));
	UT_ASSERT_EQ(route_seam.capability_calls, 2);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 2);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 1);
	UT_ASSERT_EQ(route_seam.arm_calls, 1);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 1);
	UT_ASSERT_EQ(route_seam.finish_calls, 1);
	UT_ASSERT(!route_seam.finished_outbound_admitted);
	UT_ASSERT_EQ(route_seam.recheck_calls, 1);
	route_assert_refusal(GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED, 0);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT(route_seam.arm_sequence < route_seam.enqueue_sequence);
	UT_ASSERT(route_seam.enqueue_sequence < route_seam.finish_sequence);
	UT_ASSERT(route_seam.finish_sequence < route_seam.recheck_sequence);
	UT_ASSERT(route_seam.recheck_sequence < route_seam.refusal_enqueue_sequence);
	UT_ASSERT(route_seam.refusal_enqueue_sequence < route_seam.leave_sequence);
	UT_ASSERT(route_seam.recheck_sequence < route_seam.leave_sequence);
}

UT_TEST(test_request80_final_recheck_failure_leaves_after_one_publication)
{
	ClusterR4CrRequestPayload request = route_test_request80();
	ClusterICEnvelope env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, UT_REQUESTER_NODE, UT_MASTER_NODE,
							  sizeof(request));

	route_seam_reset();
	route_seam.recheck_ok = false;
	UT_ASSERT_EQ(env.payload_length, 80);
	UT_ASSERT(cluster_gcs_block_test_r4_request80(&env, &request));
	UT_ASSERT_EQ(route_seam.capability_calls, 2);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 2);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 1);
	UT_ASSERT_EQ(route_seam.arm_calls, 1);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 1);
	UT_ASSERT_EQ(route_seam.finish_calls, 1);
	UT_ASSERT(route_seam.finished_outbound_admitted);
	UT_ASSERT_EQ(route_seam.recheck_calls, 1);
	route_assert_refusal(GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED, 0);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT(route_seam.enqueue_sequence < route_seam.finish_sequence);
	UT_ASSERT(route_seam.finish_sequence < route_seam.recheck_sequence);
	UT_ASSERT(route_seam.recheck_sequence < route_seam.refusal_enqueue_sequence);
	UT_ASSERT(route_seam.refusal_enqueue_sequence < route_seam.leave_sequence);
	UT_ASSERT(route_seam.recheck_sequence < route_seam.leave_sequence);
}

UT_TEST(test_forward96_master_mismatch_is_consumed_without_holder_submit)
{
	ClusterR4CrForwardPayload forward = route_test_forward96();
	ClusterICEnvelope env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, UT_MASTER_NODE, UT_HOLDER_NODE,
							  sizeof(forward));
	int saved_node_id = cluster_node_id;

	forward.base.master_node = UT_HOLDER_NODE;
	cluster_node_id = UT_HOLDER_NODE;
	route_seam_reset();
	UT_ASSERT_EQ(env.payload_length, 96);
	UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, &forward));
	UT_ASSERT_EQ(route_seam.capability_calls, 2);
	UT_ASSERT_EQ(route_seam.capability_peers[0], UT_MASTER_NODE);
	UT_ASSERT_EQ(route_seam.capability_peers[1], UT_REQUESTER_NODE);
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 2);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
	UT_ASSERT_EQ(route_seam.recheck_calls, 0);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 0);
	UT_ASSERT_EQ(route_seam.arm_calls, 0);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.finish_calls, 0);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	cluster_node_id = saved_node_id;
}

UT_TEST(test_forward96_proof_epoch_mismatch_is_consumed_without_holder_submit)
{
	ClusterR4CrForwardPayload forward = route_test_forward96();
	ClusterICEnvelope env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, UT_MASTER_NODE, UT_HOLDER_NODE,
							  sizeof(forward));
	int saved_node_id = cluster_node_id;

	/* Corrupt the proof's embedded formation-epoch half while keeping the base
	 * and authenticated envelope epochs valid. */
	forward.extension.kind.cr.master_authority_generation_le[4]
		= (uint8)(UT_FORMATION_EPOCH + 1);
	cluster_node_id = UT_HOLDER_NODE;
	route_seam_reset();
	UT_ASSERT_EQ(env.payload_length, 96);
	UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, &forward));
	UT_ASSERT_EQ(route_seam.capability_calls, 2);
	UT_ASSERT_EQ(route_seam.capability_peers[0], UT_MASTER_NODE);
	UT_ASSERT_EQ(route_seam.capability_peers[1], UT_REQUESTER_NODE);
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 2);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
	UT_ASSERT_EQ(route_seam.recheck_calls, 0);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 0);
	UT_ASSERT_EQ(route_seam.arm_calls, 0);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.finish_calls, 0);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	cluster_node_id = saved_node_id;
}

static void
test_kind2_requester_generation_roundtrip(uint32 requested_generation)
{
	BufferTag seed_tag = route_test_tag();
	BufferTag released_tag;
	ClusterTxLocator locator;
	ClusterTxLocator wire_locator;
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterGcsBlockDirectState direct_state = GCS_BLOCK_DIRECT_ABORTED;
	uint64 seed_request_id = 0;
	uint64 released_request_id = UINT64_MAX;
	uint64 released_epoch = UINT64_MAX;
	uint32 wire_generation = UINT32_MAX;
	int32 released_master = INT32_MIN;
	uint8 released_domain = UINT8_MAX;
	uint8 released_transition = UINT8_MAX;
	bool released_in_use = true;
	bool direct_target_prepared = true;
	int saved_node_id = cluster_node_id;

	cluster_node_id = UT_REQUESTER_NODE;
	UT_ASSERT(cluster_gcs_block_test_r4_requester_arm(
		seed_tag, UT_FORMATION_EPOCH, UT_MASTER_NODE, UINT64_C(1),
		&seed_request_id));
	UT_ASSERT(cluster_gcs_block_test_release_r4_requester_slot());
	memset(&requester_send, 0, sizeof(requester_send));
	memset(&locator, 0, sizeof(locator));
	locator.uba = uba_encode(5, 408, 6, 1);
	locator.xid = (TransactionId)798;
	locator.tt_wrap = TT_WRAP_INVALID;
	locator.itl_kind = ITL_FLAG_ACTIVE;
	locator.itl_slot_index = 3;
	memset(&resolution, 0xa5, sizeof(resolution));

	UT_ASSERT_EQ(cluster_gcs_block_r4_tx_resolve_fetch_and_wait(
					 UT_MASTER_NODE, &locator, requested_generation, UT_FORMATION_EPOCH,
					 &resolution, &reason),
				 CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(resolution.locator_echo.tt_wrap, 19);
	UT_ASSERT_EQ(resolution.authority.origin_epoch, UT_FORMATION_EPOCH);
	UT_ASSERT_EQ(resolution.authority.live_hwm_lsn,
				 (XLogRecPtr)UINT64_C(0xabcdef));
	UT_ASSERT_EQ(requester_send.calls, 1);
	UT_ASSERT(requester_send.tx_kind2);
	UT_ASSERT(requester_send.slot_armed);
	UT_ASSERT_EQ(requester_send.payload_len,
				 sizeof(ClusterR4CrForwardPayload));
	UT_ASSERT_EQ(requester_send.dest_node, UT_MASTER_NODE);
	UT_ASSERT_EQ(requester_send.tx_forward.base.original_requester_node,
				 UT_REQUESTER_NODE);
	UT_ASSERT_EQ(requester_send.tx_forward.base.master_node,
				 UT_MASTER_NODE);
	UT_ASSERT_EQ(memcmp(requester_send.tx_forward.base.reserved_0,
					  (const uint8[7]){ 0 },
					  sizeof(requester_send.tx_forward.base.reserved_0)), 0);
	UT_ASSERT(ClusterR4ForwardExtensionGetLocatorGeneration(
		&requester_send.tx_forward.extension,
		CLUSTER_R4_WIRE_TX_RESOLVE, &wire_locator, &wire_generation));
	UT_ASSERT_EQ(wire_generation, requested_generation);
	UT_ASSERT_EQ(memcmp(&wire_locator, &locator, sizeof(locator)), 0);
	UT_ASSERT(cluster_gcs_block_test_snapshot_r4_requester_slot(
		&released_in_use, &released_domain, &released_request_id,
		&released_transition, &released_tag, &released_epoch,
		&released_master, &direct_state, &direct_target_prepared));
	UT_ASSERT(!released_in_use);
	UT_ASSERT_EQ(released_domain, UT_REPLY_DOMAIN_LEGACY_ACQUIRE);
	UT_ASSERT_EQ(released_request_id, 0);
	UT_ASSERT_EQ(released_epoch, 0);
	UT_ASSERT_EQ(released_master, -1);
	cluster_node_id = saved_node_id;
}

UT_TEST(test_kind2_requester_uses_existing_r4_slot_and_lands_status22)
{
	test_kind2_requester_generation_roundtrip(9);
}

UT_TEST(test_kind2_requester_asks_origin_to_select_and_lands_exact_status22)
{
	test_kind2_requester_generation_roundtrip(UINT32_MAX);
}

UT_TEST(test_kind2_requester_sleep_error_cancels_cv_before_slot_release)
{
	BufferTag seed_tag = route_test_tag();
	BufferTag released_tag;
	ClusterTxLocator locator;
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterGcsBlockDirectState direct_state = GCS_BLOCK_DIRECT_ABORTED;
	uint64 seed_request_id = 0;
	uint64 released_request_id = UINT64_MAX;
	uint64 released_epoch = UINT64_MAX;
	int32 released_master = INT32_MIN;
	uint8 released_domain = UINT8_MAX;
	uint8 released_transition = UINT8_MAX;
	bool released_in_use = true;
	bool direct_target_prepared = true;
	volatile bool caught = false;
	int saved_node_id = cluster_node_id;

	cluster_node_id = UT_REQUESTER_NODE;
	UT_ASSERT(cluster_gcs_block_test_r4_requester_arm(
		seed_tag, UT_FORMATION_EPOCH, UT_MASTER_NODE, UINT64_C(1),
		&seed_request_id));
	UT_ASSERT(cluster_gcs_block_test_release_r4_requester_slot());
	memset(&requester_send, 0, sizeof(requester_send));
	requester_send.suppress_reply = true;
	memset(&locator, 0, sizeof(locator));
	locator.uba = uba_encode(5, 408, 6, 1);
	locator.xid = (TransactionId)798;
	locator.tt_wrap = TT_WRAP_INVALID;
	locator.itl_kind = ITL_FLAG_ACTIVE;
	locator.itl_slot_index = 3;
	reply_cv_prepare_calls = 0;
	reply_cv_timed_sleep_calls = 0;
	reply_cv_cancel_calls = 0;
	reply_cv_timed_sleep_raise = true;

	PG_TRY();
	{
		(void)cluster_gcs_block_r4_tx_resolve_fetch_and_wait(
			UT_MASTER_NODE, &locator, 9, UT_FORMATION_EPOCH,
			&resolution, &reason);
	}
	PG_CATCH();
	{
		caught = true;
		FlushErrorState();
	}
	PG_END_TRY();
	reply_cv_timed_sleep_raise = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(reply_cv_prepare_calls, 1);
	UT_ASSERT_EQ(reply_cv_timed_sleep_calls, 1);
	UT_ASSERT_EQ(reply_cv_cancel_calls, 1);
	UT_ASSERT(cluster_gcs_block_test_snapshot_r4_requester_slot(
		&released_in_use, &released_domain, &released_request_id,
		&released_transition, &released_tag, &released_epoch,
		&released_master, &direct_state, &direct_target_prepared));
	UT_ASSERT(!released_in_use);
	UT_ASSERT_EQ(released_domain, UT_REPLY_DOMAIN_LEGACY_ACQUIRE);
	UT_ASSERT_EQ(released_request_id, 0);
	UT_ASSERT_EQ(released_epoch, 0);
	UT_ASSERT_EQ(released_master, -1);
	cluster_node_id = saved_node_id;
}

UT_TEST(test_kind2_origin_runs_candidate2_cooperatively_and_ships_exact_status22)
{
	ClusterR4CrForwardPayload forward;
	ClusterICEnvelope env;
	ClusterTxLocator locator;
	ClusterTxResolution decoded;
	int saved_node_id = cluster_node_id;
	int i;

	cluster_node_id = UT_MASTER_NODE;
	memset(&locator, 0, sizeof(locator));
	locator.uba = uba_encode(5, 408, 6, 1);
	locator.xid = (TransactionId)798;
	locator.tt_wrap = TT_WRAP_INVALID;
	locator.itl_kind = ITL_FLAG_ACTIVE;
	locator.itl_slot_index = 3;
	memset(&forward, 0, sizeof(forward));
	forward.base.request_id = UT_REQUEST_ID;
	forward.base.epoch = UT_FORMATION_EPOCH;
	forward.base.tag = GcsBlockUndoFetchTagMake(5, 408);
	forward.base.original_requester_node = UT_REQUESTER_NODE;
	forward.base.requester_backend_id = UT_REQUESTER_BACKEND;
	forward.base.master_node = UT_MASTER_NODE;
	forward.base.transition_id = (uint8)PCM_TRANS_N_TO_S;
	UT_ASSERT(ClusterR4ForwardExtensionSetLocatorGeneration(
		&forward.extension, CLUSTER_R4_WIRE_TX_RESOLVE, &locator, 9));
	UT_ASSERT_EQ(memcmp(forward.base.reserved_0,
					  (const uint8[7]){ 0 },
					  sizeof(forward.base.reserved_0)), 0);
	env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_FORWARD,
						  UT_REQUESTER_NODE, UT_MASTER_NODE, sizeof(forward));

	route_seam_reset();
	route_seam.admission_result
		= CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED;
	route_seam.raw_send_result = CLUSTER_IC_SEND_DONE;
	UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, &forward));
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 1);
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.terminal_census_enter_calls, 1);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 0);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
	UT_ASSERT_EQ(route_seam.capability_calls, 1);

	cluster_gcs_block_test_r4_tx_origin_drain();
	UT_ASSERT_EQ(route_seam.candidate_acquire_begin_calls, 1);
	UT_ASSERT_EQ(route_seam.candidate_resolve_calls, 1);
	UT_ASSERT_EQ(route_seam.candidate_release_begin_calls, 1);
	UT_ASSERT_EQ(route_seam.raw_send_calls, 1);
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
	UT_ASSERT_EQ(route_seam.raw_send_msg_type, PGRAC_IC_MSG_GCS_BLOCK_REPLY);
	UT_ASSERT_EQ(route_seam.raw_send_dest, UT_REQUESTER_NODE);
	UT_ASSERT_EQ(route_seam.raw_send_payload_len,
				 GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE);
	UT_ASSERT_EQ(route_seam.raw_send_header.request_id, UT_REQUEST_ID);
	UT_ASSERT_EQ(route_seam.raw_send_header.epoch, UT_FORMATION_EPOCH);
	UT_ASSERT_EQ(route_seam.raw_send_header.sender_node, UT_MASTER_NODE);
	UT_ASSERT_EQ(route_seam.raw_send_header.requester_backend_id,
				 UT_REQUESTER_BACKEND);
	UT_ASSERT_EQ(route_seam.raw_send_header.transition_id,
				 (uint8)PCM_TRANS_N_TO_S);
	UT_ASSERT_EQ(route_seam.raw_send_header.status,
				 GCS_BLOCK_REPLY_R4_TX_RESOLVE_RESULT);
	UT_ASSERT_EQ(GcsBlockReplyHeaderGetForwardingMasterNode(
				 &route_seam.raw_send_header),
				 GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	UT_ASSERT_EQ(route_seam.raw_send_header.page_lsn, UINT64_C(0xabcdef));
	UT_ASSERT_EQ(route_seam.raw_send_header.checksum,
				 cluster_gcs_block_compute_checksum(route_seam.raw_send_page));
	memset(&decoded, 0, sizeof(decoded));
	UT_ASSERT(ClusterR4TxVerdictPageDecode(
		(const uint8 *)route_seam.raw_send_page, &locator, &decoded));
	UT_ASSERT_EQ(decoded.outcome, CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(decoded.locator_echo.tt_wrap, 19);
	UT_ASSERT_EQ(decoded.authority.origin_epoch, UT_FORMATION_EPOCH);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT(route_seam.terminal_census_recheck_calls > 0);

	/* The existing extension kind is the only discriminator.  Any base
	 * reserved byte remains canonical-zero and is consumed without work. */
	forward.base.reserved_0[6] = 1;
	route_seam_reset();
	UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, &forward));
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
	UT_ASSERT_EQ(route_seam.enter_calls, 0);
	UT_ASSERT_EQ(route_seam.terminal_census_enter_calls, 0);
	UT_ASSERT_EQ(route_seam.capability_calls, 0);

	/* A fully resolved but nonterminal PREPARED result is deliberately
	 * non-stamping: status 26 with a canonical zero page. */
	forward.base.reserved_0[6] = 0;
	route_seam_reset();
	route_seam.admission_result
		= CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED;
	route_seam.raw_send_result = CLUSTER_IC_SEND_DONE;
	route_seam.candidate_outcome = CLUSTER_TX_PREPARED;
	route_seam.candidate_proof = CLUSTER_TX_PROOF_ORIGIN_TWOPHASE;
	UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, &forward));
	for (i = 0; i < 4; i++)
		cluster_gcs_block_test_r4_tx_origin_drain();
	UT_ASSERT_EQ(route_seam.raw_send_calls, 1);
	UT_ASSERT_EQ(route_seam.raw_send_header.status,
				 GCS_BLOCK_REPLY_R4_DENIED);
	UT_ASSERT_EQ(route_seam.raw_send_header.page_lsn, 0);
	for (i = 0; i < GCS_BLOCK_DATA_SIZE; i++)
		UT_ASSERT_EQ((uint8)route_seam.raw_send_page[i], 0);

	/* A capability-generation/connection drift before publication sends no
	 * verdict on the replacement connection and leaves the sole token. */
	route_seam_reset();
	route_seam.admission_result
		= CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED;
	route_seam.raw_send_result = CLUSTER_IC_SEND_DONE;
	route_seam.candidate_cross_segment = true;
	UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, &forward));
	cluster_gcs_block_test_r4_tx_origin_drain();
	UT_ASSERT_EQ(route_seam.candidate_acquire_begin_calls, 2);
	UT_ASSERT_EQ(route_seam.candidate_release_begin_calls, 2);
	UT_ASSERT_EQ(route_seam.candidate_canonical_sample_calls, 1);
	UT_ASSERT_EQ(route_seam.candidate_data_recheck_calls, 0);
	UT_ASSERT_EQ(route_seam.raw_send_calls, 0);
	route_seam.capability_ok = false;
	cluster_gcs_block_test_r4_tx_origin_drain();
	UT_ASSERT_EQ(route_seam.candidate_acquire_begin_calls, 3);
	UT_ASSERT_EQ(route_seam.candidate_release_begin_calls, 3);
	UT_ASSERT_EQ(route_seam.candidate_data_recheck_calls, 1);
	UT_ASSERT_EQ(route_seam.raw_send_calls, 0);
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	cluster_node_id = saved_node_id;
}

UT_TEST(test_kind2_origin_capacity_covers_three_remote_c32_bursts)
{
	ClusterR4CrForwardPayload forward;
	ClusterICEnvelope env;
	ClusterTxLocator locator;
	int saved_max_backends = MaxBackends;
	int saved_node_id = cluster_node_id;
	int i;

	/* Stage-8 PRE has three remote members with C=32.  Every authenticated
	 * exact request already owns its original reply deadline; the bounded
	 * process-local continuation pool must retain the whole 3*32 burst rather
	 * than silently consuming request 5 and later without a reply. */
	MaxBackends = 96;
	cluster_node_id = UT_MASTER_NODE;
	memset(&locator, 0, sizeof(locator));
	locator.uba = uba_encode(5, 408, 6, 1);
	locator.xid = (TransactionId)798;
	locator.tt_wrap = TT_WRAP_INVALID;
	locator.itl_kind = ITL_FLAG_ACTIVE;
	locator.itl_slot_index = 3;
	memset(&forward, 0, sizeof(forward));
	forward.base.epoch = UT_FORMATION_EPOCH;
	forward.base.tag = GcsBlockUndoFetchTagMake(5, 408);
	forward.base.original_requester_node = UT_REQUESTER_NODE;
	forward.base.master_node = UT_MASTER_NODE;
	forward.base.transition_id = (uint8)PCM_TRANS_N_TO_S;
	UT_ASSERT(ClusterR4ForwardExtensionSetLocatorGeneration(
		&forward.extension, CLUSTER_R4_WIRE_TX_RESOLVE, &locator, 9));
	env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_FORWARD,
						  UT_REQUESTER_NODE, UT_MASTER_NODE, sizeof(forward));

	route_seam_reset();
	route_seam.admission_result
		= CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED;
	route_seam.raw_send_result = CLUSTER_IC_SEND_DONE;
	for (i = 0; i < 96; i++) {
		forward.base.request_id = UT_REQUEST_ID + (uint64)i;
		forward.base.requester_backend_id = i + 1;
		UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, &forward));
	}
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 96);
	UT_ASSERT_EQ(route_seam.enter_calls, 96);
	UT_ASSERT_EQ(route_seam.raw_send_calls, 0);
	{
		char detail[768];
		char truncated[1] = { 'x' };

		UT_ASSERT_EQ(cluster_gcs_block_r4_tx_resolve_pending_detail(detail, sizeof(detail)), 96);
		UT_ASSERT(strstr(detail, "active=96") != NULL);
		UT_ASSERT(strstr(detail, "guards=0") != NULL);
		UT_ASSERT_EQ(cluster_gcs_block_r4_tx_resolve_pending_detail(truncated, sizeof(truncated)),
					 96);
		UT_ASSERT_EQ(truncated[0], '\0');
		UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 96);
		UT_ASSERT_EQ(route_seam.enter_calls, 96);
		UT_ASSERT_EQ(route_seam.raw_send_calls, 0);
	}

	cluster_gcs_block_test_r4_tx_origin_drain();
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
	UT_ASSERT_EQ(route_seam.raw_send_calls, 96);
	UT_ASSERT_EQ(route_seam.leave_calls, 96);
	{
		char detail[768];

		UT_ASSERT_EQ(cluster_gcs_block_r4_tx_resolve_pending_detail(detail, sizeof(detail)), 0);
		UT_ASSERT(strstr(detail, "active=0") != NULL);
	}
	MaxBackends = saved_max_backends;
	cluster_node_id = saved_node_id;
}

static ClusterR4CrForwardPayload
route_test_undo_forward(uint16 wrap)
{
	ClusterR4CrForwardPayload forward;
	ClusterTxLocator locator;
	memset(&locator, 0, sizeof(locator));
	locator.uba = uba_encode(5, 408, 6, 1);
	locator.xid = (TransactionId)798;
	locator.tt_wrap = wrap;
	locator.itl_kind = ITL_FLAG_ACTIVE;
	locator.itl_slot_index = 3;
	memset(&forward, 0, sizeof(forward));
	forward.base.request_id = UINT64_C(262145);
	forward.base.epoch = UT_FORMATION_EPOCH;
	forward.base.tag = GcsBlockUndoFetchTagMake(5, 408);
	forward.base.original_requester_node = UT_REQUESTER_NODE;
	forward.base.requester_backend_id = UT_R4_INTERNAL_ENDPOINT;
	forward.base.master_node = UT_REQUESTER_NODE;
	forward.base.transition_id = (uint8)PCM_TRANS_N_TO_S;
	forward.base.reserved_0[6] = CLUSTER_R4_FORWARD_EXTENDED;
	GcsBlockForwardPayloadSetExpectedPiWatermarkScn(&forward.base, (SCN)100);
	UT_ASSERT(ClusterR4ForwardExtensionSetLocatorGeneration(
		&forward.extension, CLUSTER_R4_WIRE_UNDO_DATA_FETCH, &locator, 9));
	return forward;
}

static ClusterR4CrForwardPayload
route_test_tx_forward(uint32 generation)
{
	ClusterR4CrForwardPayload forward = route_test_undo_forward(TT_WRAP_INVALID);

	forward.base.requester_backend_id = UT_REQUESTER_BACKEND;
	forward.base.master_node = UT_MASTER_NODE;
	memset(forward.base.reserved_0, 0, sizeof(forward.base.reserved_0));
	memset(forward.base.expected_pi_watermark_scn_bytes, 0,
		   sizeof(forward.base.expected_pi_watermark_scn_bytes));
	forward.extension.r4_kind = CLUSTER_R4_WIRE_TX_RESOLVE;
	/* Build bytes independently of the codec under test, so old-product RED
	 * reaches the real origin's admission branch rather than its test helper. */
	ClusterR4WireWriteU32(forward.extension.subject_id_le, generation);
	return forward;
}

UT_TEST(test_seal_two_blocks_new_tx_and_undo_contexts_but_not_original_drain)
{
	int saved_node = cluster_node_id;

	cluster_node_id = UT_MASTER_NODE;
	for (int domain = 0; domain < 2; domain++) {
		ClusterR4CrForwardPayload forward = domain == 0 ? route_test_tx_forward(UINT32_MAX)
														: route_test_undo_forward(TT_WRAP_INVALID);
		ClusterICEnvelope env = route_test_envelope(
			PGRAC_IC_MSG_GCS_BLOCK_FORWARD, UT_REQUESTER_NODE, UT_MASTER_NODE, sizeof(forward));

		route_seam_reset();
		route_seam.raw_send_result = CLUSTER_IC_SEND_DONE;
		stop_new_read_allowed = false;
		UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, &forward));
		UT_ASSERT_EQ(stop_new_read_calls, 1);
		UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
		UT_ASSERT_EQ(route_seam.enter_calls, 0);
		UT_ASSERT_EQ(route_seam.raw_send_calls, 0);
		for (int i = 0; i < 4; i++)
			cluster_gcs_block_test_r4_tx_origin_drain(); /* RED-only cleanup. */
		route_seam_reset();
		route_seam.raw_send_result = CLUSTER_IC_SEND_DONE;
		UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, &forward));
		UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 1);
		UT_ASSERT_EQ(stop_new_read_calls, 1);
		stop_new_read_calls = 0;
		stop_new_read_allowed = false;
		UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, &forward));
		UT_ASSERT_EQ(stop_new_read_calls, 0);
		UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 1);
		for (int i = 0; i < 4; i++)
			cluster_gcs_block_test_r4_tx_origin_drain();
		UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
		UT_ASSERT_EQ(route_seam.leave_calls, 1);
	}
	stop_new_read_allowed = true;
	cluster_node_id = saved_node;
}

UT_TEST(test_kind2_origin_generation_selection_and_strict_known_negatives)
{
	int saved_node = cluster_node_id;
	int variant;

	cluster_node_id = UT_MASTER_NODE;
	for (variant = 0; variant < 10; variant++) {
		bool known = variant == 1 || variant == 2;
		bool positive = variant < 5 && variant != 2;
		ClusterR4CrForwardPayload forward = route_test_tx_forward(known ? 0 : UINT32_MAX);
		ClusterICEnvelope env = route_test_envelope(
			PGRAC_IC_MSG_GCS_BLOCK_FORWARD, UT_REQUESTER_NODE, UT_MASTER_NODE, sizeof(forward));
		ClusterTxLocator locator;
		ClusterTxResolution decoded;
		uint32 generation;
		int i;

		route_seam_reset();
		route_seam.admission_result = CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED;
		route_seam.raw_send_result = CLUSTER_IC_SEND_DONE;
		route_seam.candidate_cross_segment = positive;
		origin_generation_sample_enabled = variant != 5;
		origin_generation_sample_value
			= (variant == 1 || variant == 3)
				  ? 0
				  : (variant == 4 ? UINT32_MAX - 1 : (variant == 7 ? UINT32_MAX : 3));
		origin_generation_sample_known = variant != 6;
		origin_generation_sample_throw = variant == 8;
		if (variant == 9) {
			/* The non-census OPEN mode also accepts cold requests. The
			 * single-segment stub proves selection, not the real TT sample. */
			route_seam.admission_result = CLUSTER_SEMANTIC_ADMISSION_OK;
			positive = true;
		}
		UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, &forward));
		UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 1);
		for (i = 0; i < 4; i++)
			cluster_gcs_block_test_r4_tx_origin_drain();
		UT_ASSERT_EQ(origin_generation_sample_calls, known ? 0 : 1);
		UT_ASSERT_EQ(route_seam.raw_send_calls, 1);
		UT_ASSERT_EQ(route_seam.raw_send_header.status,
					 positive ? GCS_BLOCK_REPLY_R4_TX_RESOLVE_RESULT : GCS_BLOCK_REPLY_R4_DENIED);
		UT_ASSERT_EQ(route_seam.candidate_cancel_calls, variant == 8 ? 1 : 0);
		UT_ASSERT_EQ(route_seam.candidate_release_begin_calls,
					 variant == 8 ? 0 : (route_seam.candidate_cross_segment ? 3 : 1));
		UT_ASSERT_EQ(route_seam.candidate_canonical_sample_calls,
					 route_seam.candidate_cross_segment ? 1 : 0);
		UT_ASSERT_EQ(route_seam.candidate_data_recheck_calls,
					 route_seam.candidate_cross_segment ? 1 : 0);
		if (positive) {
			UT_ASSERT(ClusterR4ForwardExtensionGetLocatorGeneration(
				&forward.extension, CLUSTER_R4_WIRE_TX_RESOLVE, &locator, &generation));
			UT_ASSERT(ClusterR4TxVerdictPageDecode((const uint8 *)route_seam.raw_send_page,
												   &locator, &decoded));
			UT_ASSERT_EQ(decoded.outcome, CLUSTER_TX_COMMITTED);
		} else {
			for (i = 0; i < BLCKSZ; i++)
				UT_ASSERT_EQ(route_seam.raw_send_page[i], 0);
			UT_ASSERT_EQ(route_seam.raw_send_header.page_lsn, 0);
		}
		UT_ASSERT_EQ(route_seam.leave_calls, 1);
		UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
	}
	cluster_node_id = saved_node;
}

UT_TEST(test_kind2_origin_backpressure_does_not_reselect_or_duplicate)
{
	ClusterR4CrForwardPayload forward = route_test_tx_forward(UINT32_MAX);
	ClusterICEnvelope env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, UT_REQUESTER_NODE,
												UT_MASTER_NODE, sizeof(forward));
	GcsBlockReplyHeader header;
	char page[BLCKSZ];
	int saved_node = cluster_node_id;
	int i;
	int acquires;

	cluster_node_id = UT_MASTER_NODE;
	route_seam_reset();
	route_seam.admission_result = CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED;
	route_seam.raw_send_result = CLUSTER_IC_SEND_NOT_ADMITTED;
	route_seam.candidate_cross_segment = true;
	origin_generation_sample_enabled = true;
	UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, &forward));
	for (i = 0; i < 4; i++)
		cluster_gcs_block_test_r4_tx_origin_drain();
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 1);
	UT_ASSERT_EQ(route_seam.candidate_release_begin_calls, 3);
	UT_ASSERT_EQ(origin_generation_sample_calls, 1);
	header = route_seam.raw_send_header;
	memcpy(page, route_seam.raw_send_page, BLCKSZ);
	acquires = route_seam.candidate_acquire_begin_calls;
	UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, &forward));
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 1);
	origin_generation_sample_value++;
	route_seam.raw_send_result = CLUSTER_IC_SEND_DONE;
	cluster_gcs_block_test_r4_tx_origin_drain();
	UT_ASSERT_EQ(origin_generation_sample_calls, 1);
	UT_ASSERT_EQ(route_seam.candidate_acquire_begin_calls, acquires);
	UT_ASSERT_EQ(memcmp(page, route_seam.raw_send_page, BLCKSZ), 0);
	UT_ASSERT_EQ(memcmp(&header, &route_seam.raw_send_header, sizeof(header)), 0);
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	cluster_node_id = saved_node;
}

UT_TEST(test_kind4_origin_selects_generation_for_cold_holder)
{
	ClusterR4CrForwardPayload forward = route_test_undo_forward(TT_WRAP_INVALID);
	ClusterICEnvelope env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, UT_REQUESTER_NODE,
												UT_MASTER_NODE, sizeof(forward));
	uint32 generation = 0;
	int saved_node = cluster_node_id;
	int i;

	cluster_node_id = UT_MASTER_NODE;
	route_seam_reset();
	route_seam.raw_send_result = CLUSTER_IC_SEND_DONE;
	route_seam.candidate_cross_segment = true;
	origin_generation_sample_enabled = true;
	/* Unknown is a request to the exact origin, never an actual generation. */
	memset(forward.extension.subject_id_le, 0xff, sizeof(forward.extension.subject_id_le));
	UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, &forward));
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 1);
	for (i = 0; i < 4; i++)
		cluster_gcs_block_test_r4_tx_origin_drain();
	UT_ASSERT_EQ(route_seam.raw_send_calls, 1);
	UT_ASSERT_EQ(route_seam.raw_send_header.status, GCS_BLOCK_REPLY_R4_UNDO_DATA_RESULT);
	UT_ASSERT(GcsBlockReplyHeaderGetR4UndoGeneration(&route_seam.raw_send_header, &generation));
	UT_ASSERT_EQ(generation, 9);
	UT_ASSERT_EQ(route_seam.candidate_acquire_begin_calls, 3);
	UT_ASSERT_EQ(route_seam.candidate_release_begin_calls, 3);
	UT_ASSERT_EQ(route_seam.candidate_canonical_sample_calls, 1);
	UT_ASSERT_EQ(route_seam.candidate_data_recheck_calls, 1);
	UT_ASSERT_EQ(origin_generation_sample_calls, 1);
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	cluster_node_id = saved_node;
}

UT_TEST(test_kind4_origin_generation_sample_failure_never_publishes_data)
{
	int saved_node = cluster_node_id;
	int variant;

	cluster_node_id = UT_MASTER_NODE;
	for (variant = 0; variant < 6; variant++) {
		ClusterR4CrForwardPayload forward = route_test_undo_forward(TT_WRAP_INVALID);
		ClusterICEnvelope env = route_test_envelope(
			PGRAC_IC_MSG_GCS_BLOCK_FORWARD, UT_REQUESTER_NODE, UT_MASTER_NODE, sizeof(forward));
		uint32 generation = UINT32_MAX;
		int i;

		route_seam_reset();
		route_seam.raw_send_result = CLUSTER_IC_SEND_DONE;
		origin_generation_sample_enabled = variant != 2;
		origin_generation_sample_value
			= variant == 0 ? 0 : (variant == 1 ? UINT32_MAX - 1 : (variant == 3 ? UINT32_MAX : 9));
		origin_generation_sample_known = variant != 4;
		origin_generation_sample_throw = variant == 5;
		memset(forward.extension.subject_id_le, 0xff, sizeof(forward.extension.subject_id_le));
		UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, &forward));
		for (i = 0; i < 4; i++)
			cluster_gcs_block_test_r4_tx_origin_drain();
		UT_ASSERT_EQ(origin_generation_sample_calls, 1);
		UT_ASSERT_EQ(route_seam.raw_send_calls, 1);
		UT_ASSERT_EQ(route_seam.raw_send_header.request_id, forward.base.request_id);
		UT_ASSERT_EQ(route_seam.candidate_acquire_begin_calls, 1);
		UT_ASSERT_EQ(route_seam.candidate_release_begin_calls, variant == 5 ? 0 : 1);
		UT_ASSERT_EQ(route_seam.candidate_cancel_calls, variant == 5 ? 1 : 0);
		UT_ASSERT_EQ(route_seam.candidate_resolve_calls, variant < 2 ? 1 : 0);
		UT_ASSERT_EQ(route_seam.raw_send_header.status,
					 variant < 2 ? GCS_BLOCK_REPLY_R4_UNDO_DATA_RESULT : GCS_BLOCK_REPLY_R4_DENIED);
		if (variant < 2) {
			UT_ASSERT(
				GcsBlockReplyHeaderGetR4UndoGeneration(&route_seam.raw_send_header, &generation));
			UT_ASSERT_EQ(generation, origin_generation_sample_value);
		} else {
			for (i = 0; i < BLCKSZ; i++)
				UT_ASSERT_EQ(route_seam.raw_send_page[i], 0);
			UT_ASSERT_EQ(route_seam.raw_send_header.page_lsn, 0);
		}
		UT_ASSERT_EQ(route_seam.leave_calls, 1);
		UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
	}
	cluster_node_id = saved_node;
}

UT_TEST(test_kind4_origin_error_releases_guard_and_returns_correlated_refusal)
{
	ClusterR4CrForwardPayload forward = route_test_undo_forward(TT_WRAP_INVALID);
	ClusterICEnvelope env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, UT_REQUESTER_NODE,
												UT_MASTER_NODE, sizeof(forward));
	int saved_node = cluster_node_id;
	int i;
	cluster_node_id = UT_MASTER_NODE;
	route_seam_reset();
	route_seam.raw_send_result = CLUSTER_IC_SEND_DONE;
	route_seam.candidate_throw_on_copy = true;
	UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, &forward));
	for (i = 0; i < 4; i++)
		cluster_gcs_block_test_r4_tx_origin_drain();
	UT_ASSERT_EQ(route_seam.candidate_cancel_calls, 1);
	UT_ASSERT_EQ(route_seam.raw_send_calls, 1);
	UT_ASSERT_EQ(route_seam.raw_send_payload_len, GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE);
	UT_ASSERT_EQ(route_seam.raw_send_header.status, GCS_BLOCK_REPLY_R4_DENIED);
	UT_ASSERT_EQ(route_seam.raw_send_header.request_id, forward.base.request_id);
	UT_ASSERT_EQ(route_seam.raw_send_header.requester_backend_id, UT_R4_INTERNAL_ENDPOINT);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
	cluster_node_id = saved_node;
}

UT_TEST(test_kind4_origin_backpressure_retains_same_proven_page_without_reacquiring)
{
	ClusterR4CrForwardPayload forward = route_test_undo_forward(TT_WRAP_INVALID);
	ClusterICEnvelope env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, UT_REQUESTER_NODE,
												UT_MASTER_NODE, sizeof(forward));
	GcsBlockReplyHeader first_header;
	char first_page[BLCKSZ];
	int saved_node = cluster_node_id;
	int i;
	int acquires;
	int sent;

	cluster_node_id = UT_MASTER_NODE;
	route_seam_reset();
	route_seam.raw_send_result = CLUSTER_IC_SEND_NOT_ADMITTED;
	route_seam.candidate_cross_segment = true;
	origin_generation_sample_enabled = true;
	memset(forward.extension.subject_id_le, 0xff, sizeof(forward.extension.subject_id_le));
	UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, &forward));
	for (i = 0; i < 4; i++)
		cluster_gcs_block_test_r4_tx_origin_drain();
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 1);
	UT_ASSERT_EQ(route_seam.leave_calls, 0);
	UT_ASSERT_EQ(route_seam.candidate_release_begin_calls, 3);
	UT_ASSERT_EQ(origin_generation_sample_calls, 1);
	first_header = route_seam.raw_send_header;
	memcpy(first_page, route_seam.raw_send_page, BLCKSZ);
	acquires = route_seam.candidate_acquire_begin_calls;
	sent = route_seam.raw_send_calls;
	route_seam.raw_send_result = CLUSTER_IC_SEND_WOULD_BLOCK;
	/* A later local observation is not permission to replace the proof. */
	origin_generation_sample_value++;
	cluster_gcs_block_test_r4_tx_origin_drain();
	UT_ASSERT_EQ(route_seam.raw_send_calls, sent + 1);
	UT_ASSERT_EQ(route_seam.candidate_acquire_begin_calls, acquires);
	UT_ASSERT_EQ(origin_generation_sample_calls, 1);
	UT_ASSERT_EQ(memcmp(first_page, route_seam.raw_send_page, BLCKSZ), 0);
	UT_ASSERT_EQ(memcmp(&first_header, &route_seam.raw_send_header, sizeof(first_header)), 0);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
	cluster_node_id = saved_node;
}

UT_TEST(test_kind4_origin_requires_exact_current_open_and_internal_shape)
{
	int saved_node = cluster_node_id;
	int variant;
	cluster_node_id = UT_MASTER_NODE;
	for (variant = 0; variant < 12; variant++) {
		ClusterR4CrForwardPayload forward = route_test_undo_forward(TT_WRAP_INVALID);
		ClusterICEnvelope env = route_test_envelope(
			PGRAC_IC_MSG_GCS_BLOCK_FORWARD, UT_REQUESTER_NODE, UT_MASTER_NODE, sizeof(forward));
		int i;
		route_seam_reset();
		route_seam.raw_send_result = CLUSTER_IC_SEND_DONE;
		switch (variant) {
		case 0: /* The ordinary four-member OPEN epoch zero is valid. */
			forward.base.epoch = env.epoch = route_test_epoch = 0;
			route_seam.configured_node_count = 4;
			break;
		case 1:
			forward.base.epoch = env.epoch = route_test_epoch = 0;
			break;
		case 2:
			route_seam.admission_result = CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED;
			break;
		case 3:
			route_seam.peer_open_ok = false;
			break;
		case 4:
			route_seam.capability_ok = false;
			break;
		case 5:
			forward.base.requester_backend_id = UT_REQUESTER_BACKEND;
			break;
		case 6:
			forward.base.master_node = UT_MASTER_NODE;
			break;
		case 7:
			forward.base.reserved_0[0] = 1;
			break;
		case 8:
			forward.base.reserved_0[6] = 0;
			break;
		case 9:
			forward.base.tag.blockNum++;
			break;
		case 10:
			forward.base.epoch++;
			break;
		case 11:
			GcsBlockForwardPayloadSetExpectedPiWatermarkScn(&forward.base, InvalidScn);
			break;
		}
		UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, &forward));
		UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), variant == 0 ? 1 : 0);
		UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
		UT_ASSERT_EQ(route_seam.terminal_census_enter_calls, 0);
		for (i = 0; i < 4; i++)
			cluster_gcs_block_test_r4_tx_origin_drain();
		if (variant == 0) {
			UT_ASSERT_EQ(route_seam.raw_send_calls, 1);
			UT_ASSERT_EQ(route_seam.raw_send_header.status, GCS_BLOCK_REPLY_R4_UNDO_DATA_RESULT);
			UT_ASSERT_EQ(route_seam.raw_send_header.epoch, 0);
		} else
			UT_ASSERT_EQ(route_seam.raw_send_calls, 0);
		UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
	}
	cluster_node_id = saved_node;
}

UT_TEST(test_kind4_origin_reuses_owned_data_tt_data_and_returns_internal_page)
{
	ClusterR4CrForwardPayload forward;
	ClusterICEnvelope env;
	ClusterTxLocator locator;
	int saved_node_id = cluster_node_id;
	int variant;
	int i;
	char expected_page[BLCKSZ];

	memset(expected_page, 0x5a, sizeof(expected_page));
	cluster_node_id = UT_MASTER_NODE;
	for (variant = 0; variant < 4; variant++) {
		route_seam_reset();
		route_seam.raw_send_result = CLUSTER_IC_SEND_DONE;
		route_seam.candidate_cross_segment = (variant & 1) != 0;
		memset(&locator, 0, sizeof(locator));
		locator.uba = uba_encode(5, 408, 6, 1);
		locator.xid = (TransactionId)798;
		locator.tt_wrap = (variant & 2) ? 19 : TT_WRAP_INVALID;
		locator.itl_kind = ITL_FLAG_ACTIVE;
		locator.itl_slot_index = 3;
		memset(&forward, 0, sizeof(forward));
		forward.base.request_id = UINT64_C(262145);
		forward.base.epoch = UT_FORMATION_EPOCH;
		forward.base.tag = GcsBlockUndoFetchTagMake(5, 408);
		forward.base.original_requester_node = UT_REQUESTER_NODE;
		forward.base.requester_backend_id = UT_R4_INTERNAL_ENDPOINT;
		forward.base.master_node = UT_REQUESTER_NODE;
		forward.base.transition_id = (uint8)PCM_TRANS_N_TO_S;
		forward.base.reserved_0[6] = CLUSTER_R4_FORWARD_EXTENDED;
		GcsBlockForwardPayloadSetExpectedPiWatermarkScn(&forward.base, (SCN)100);
		UT_ASSERT(ClusterR4ForwardExtensionSetLocatorGeneration(
			&forward.extension, CLUSTER_R4_WIRE_UNDO_DATA_FETCH, &locator, 9));
		env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, UT_REQUESTER_NODE, UT_MASTER_NODE,
								  sizeof(forward));
		UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, &forward));
		UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 1);
		UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
		UT_ASSERT_EQ(route_seam.terminal_census_enter_calls, 0);
		/* Exact retransmission retains the original owner, not a second job. */
		UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, &forward));
		UT_ASSERT_EQ(route_seam.enter_calls, 1);
		for (i = 0; i < 4; i++)
			cluster_gcs_block_test_r4_tx_origin_drain();
		UT_ASSERT_EQ(route_seam.candidate_acquire_begin_calls, (variant & 1) ? 3 : 1);
		UT_ASSERT_EQ(route_seam.candidate_release_begin_calls,
					 route_seam.candidate_acquire_begin_calls);
		UT_ASSERT_EQ(route_seam.raw_send_calls, 1);
		UT_ASSERT_EQ(route_seam.raw_send_payload_len,
					 GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE + sizeof(ClusterGcsUndoAuthTrailer));
		UT_ASSERT_EQ(route_seam.raw_send_header.status, GCS_BLOCK_REPLY_R4_UNDO_DATA_RESULT);
		UT_ASSERT_EQ(route_seam.raw_send_header.request_id, forward.base.request_id);
		UT_ASSERT_EQ(route_seam.raw_send_header.requester_backend_id, UT_R4_INTERNAL_ENDPOINT);
		UT_ASSERT_EQ(route_seam.raw_send_header.page_lsn, UINT64_C(0xabcdef));
		UT_ASSERT_EQ(memcmp(route_seam.raw_send_page, expected_page, BLCKSZ), 0);
		UT_ASSERT_EQ(route_seam.leave_calls, 1);
		UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
	}
	cluster_node_id = saved_node_id;
}

UT_TEST(test_internal_origin_refusals_do_not_enter_backend_reply_table)
{
	struct {
		GcsBlockReplyHeader header;
		char block_data[GCS_BLOCK_DATA_SIZE];
	} reply;
	ClusterICEnvelope env;
	int variant;

	for (variant = 0; variant < 2; variant++) {
		memset(&reply, 0, sizeof(reply));
		reply.header.request_id = UINT64_C(262144);
		reply.header.epoch = UT_FORMATION_EPOCH;
		reply.header.sender_node = UT_HOLDER_NODE;
		reply.header.requester_backend_id = UT_R4_INTERNAL_ENDPOINT;
		reply.header.transition_id = PCM_TRANS_N_TO_S;
		reply.header.status
			= variant ? GCS_BLOCK_REPLY_R4_DENIED : GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED;
		GcsBlockReplyHeaderSetForwardingMasterNode(&reply.header,
												   GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
		reply.header.checksum = cluster_gcs_block_compute_checksum(reply.block_data);
		env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY, UT_HOLDER_NODE, cluster_node_id,
								  sizeof(reply));
		route_seam_reset();
		reply_lock_acquire_calls = 0;
		cluster_gcs_handle_block_reply_envelope(&env, &reply);
		UT_ASSERT_EQ(route_seam.foreign_undo_land_calls, 1);
		UT_ASSERT_EQ(reply_lock_acquire_calls, 0);
	}
}

int
main(void)
{
	UT_PLAN(117);
	UT_RUN(test_seal_two_blocks_new_tx_and_undo_contexts_but_not_original_drain);
	UT_RUN(test_kind2_requester_asks_origin_to_select_and_lands_exact_status22);
	UT_RUN(test_kind2_origin_generation_selection_and_strict_known_negatives);
	UT_RUN(test_kind2_origin_backpressure_does_not_reselect_or_duplicate);
	UT_RUN(test_kind4_origin_selects_generation_for_cold_holder);
	UT_RUN(test_kind4_origin_generation_sample_failure_never_publishes_data);
	UT_RUN(test_01_null_authority_is_protocol);
	UT_RUN(test_02_null_output_is_protocol);
	UT_RUN(test_03_canonical_n_has_no_holder);
	UT_RUN(test_04_n_with_x_is_ambiguous);
	UT_RUN(test_05_n_with_s_is_ambiguous);
	UT_RUN(test_06_n_with_master_is_ambiguous);
	UT_RUN(test_07_x_node_zero_is_selected);
	UT_RUN(test_08_x_node_31_is_selected);
	UT_RUN(test_09_x_without_holder_is_ambiguous);
	UT_RUN(test_10_x_negative_holder_is_ambiguous);
	UT_RUN(test_11_x_out_of_range_is_ambiguous);
	UT_RUN(test_12_x_master_mismatch_is_ambiguous);
	UT_RUN(test_13_x_with_s_bitmap_is_ambiguous);
	UT_RUN(test_14_s_node_zero_is_selected);
	UT_RUN(test_15_s_node_31_is_selected);
	UT_RUN(test_16_s_multiple_selects_canonical);
	UT_RUN(test_17_s_zero_bitmap_is_ambiguous);
	UT_RUN(test_18_s_with_x_is_ambiguous);
	UT_RUN(test_19_s_without_master_is_ambiguous);
	UT_RUN(test_20_s_out_of_range_master_is_ambiguous);
	UT_RUN(test_21_s_missing_canonical_bit_is_ambiguous);
	UT_RUN(test_22_unknown_state_is_ambiguous);
	UT_RUN(test_23_reserved_zero_is_required);
	UT_RUN(test_24_reserved_one_is_required);
	UT_RUN(test_25_pending_destructive_convert_is_recovering);
	UT_RUN(test_26_zero_master_generation_is_rejected);
	UT_RUN(test_27_zero_restart_half_is_rejected);
	UT_RUN(test_28_wrong_epoch_half_is_rejected);
	UT_RUN(test_29_zero_transition_count_is_rejected);
	UT_RUN(test_30_exhausted_transition_count_is_rejected);
	UT_RUN(test_31_exact_duplicate_route_matches);
	UT_RUN(test_32_transition_drift_closes_duplicate);
	UT_RUN(test_reason_polarity_0);
	UT_RUN(test_reason_polarity_1);
	UT_RUN(test_reason_polarity_2);
	UT_RUN(test_reason_polarity_3);
	UT_RUN(test_reason_polarity_4);
	UT_RUN(test_reason_polarity_5);
	UT_RUN(test_reason_polarity_6);
	UT_RUN(test_reason_polarity_7);
	UT_RUN(test_reason_polarity_8);
	UT_RUN(test_reason_polarity_9);
	UT_RUN(test_reason_polarity_10);
	UT_RUN(test_reason_polarity_11);
	UT_RUN(test_reason_polarity_12);
	UT_RUN(test_reason_polarity_13);
	UT_RUN(test_reason_polarity_14);
	UT_RUN(test_reason_polarity_15);
	UT_RUN(test_reason_polarity_16);
	UT_RUN(test_reason_polarity_17);
	UT_RUN(test_unknown_reason_fails_closed);
	UT_RUN(test_d3_result_reason_mapping_is_closed);
	UT_RUN(test_r4_refusal_decoder_requires_exact_domain_identity_and_zero_body);
	UT_RUN(test_r4_reply_decoder_binds_domain_status_length_and_undo_authority);
	UT_RUN(test_r4_status24_routes_only_to_internal_foreign_undo_landing);
	UT_RUN(test_r4_reply_decoder_checks_minimum_length_before_header_read);
	UT_RUN(test_r4_full_reply_lands_once_in_armed_r4_slot);
	UT_RUN(test_forwarded_holder_pending_x_lands_in_legacy_slot);
	UT_RUN(test_r4_requester_arm_selects_closed_domain_and_releases_cleanly);
	UT_RUN(test_current_mx_describe_requester_is_capability_bound_and_times_out_cleanly);
	UT_RUN(test_current_mx_describe_reply_lands_in_current_domain);
	UT_RUN(test_current_mx_describe_forward128_routes_to_origin_serve);
	UT_RUN(test_current_mx_member_proof_requester_is_capability_bound_and_times_out_cleanly);
	UT_RUN(test_current_mx_member_proof_forward128_routes_to_cooperative_origin);
	UT_RUN(test_current_mx_member_proof_reply_lands_in_current_domain);
	UT_RUN(test_r4_requester_count_tracks_only_live_r4_domain_slots);
	UT_RUN(test_r4_remote_requester_arms_request80_waits_full_and_releases);
	UT_RUN(test_r4_requester_status25_releases_then_retries_with_fresh_id);
	UT_RUN(test_r4_requester_status26_fails_closed_without_retry_and_cleans_slot);
	UT_RUN(test_r4_terminal_done_remote_and_self_master_preserves_exact_identity);
	UT_RUN(test_r4_done_missing_capability_or_queue_space_keeps_terminal_result);
	UT_RUN(test_direct_s_capacity_refusal_returns_owned_retry_without_done);
	UT_RUN(test_capacity_retry_excludes_write_clean_forwarded_and_unverified_reply);
	UT_RUN(test_real_capacity_refusal_to_exact_rearm_cancel_gate_and_successor);
	UT_RUN(test_target_wrapper_status25_beyond_old_limit_waits_for_full);
	UT_RUN(test_target_wrapper_lost_reply_and_backpressure_redrive_same_id);
	UT_RUN(test_target_wrapper_wait_cancellation_leaves_admission_and_slot);
	UT_RUN(test_target_wrapper_wait_drift_never_adopts_new_identity);
	UT_RUN(test_target_wrapper_denied_and_corrupt_full_never_publish);
	UT_RUN(test_target_wrapper_disabled_returns_typed_before_lookup_or_wire);
	UT_RUN(test_target_wrapper_status21_rechecks_private_scratch_then_copies_full);
	UT_RUN(test_target_wrapper_local_master_stages_request80_to_data_owner_and_returns_full);
	UT_RUN(test_target_wrapper_recheck_drift_keeps_dst_and_canonicalizes_slot);
	UT_RUN(test_request80_routes_stored_proof_to_real_forward96_handler);
	UT_RUN(test_request80_local_master_uses_compiled_capability_and_routes_remote_holder);
	UT_RUN(test_request80_all_local_routes_forward96_through_registered_dispatch);
	UT_RUN(test_forward96_all_local_uses_checked_open_generation_for_both_submit_identities);
	UT_RUN(test_forward96_all_local_refuses_open_generation_overflow_before_submit);
	UT_RUN(test_forward96_holder_submit_failure_publishes_typed_remote_refusal);
	UT_RUN(test_r4_try_handlers_consume_only_exact_extended_lengths);
	UT_RUN(test_r4_same_open_mismatch_has_zero_route_or_holder_mutation);
	UT_RUN(test_request80_admission_refusals_publish_without_token);
	UT_RUN(test_request80_sender_mismatch_is_consumed_without_route_mutation);
	UT_RUN(test_request80_reserved_extension_is_consumed_without_route_mutation);
	UT_RUN(test_request80_epoch_mismatch_is_consumed_without_route_mutation);
	UT_RUN(test_request80_wrong_master_is_consumed_before_snapshot);
	UT_RUN(test_request80_snapshot_failure_stops_before_route_arm);
	UT_RUN(test_request80_arm_full_stops_before_publication);
	UT_RUN(test_request80_enqueue_refusal_finishes_unadmitted_once);
	UT_RUN(test_request80_final_recheck_failure_leaves_after_one_publication);
	UT_RUN(test_forward96_master_mismatch_is_consumed_without_holder_submit);
	UT_RUN(test_forward96_proof_epoch_mismatch_is_consumed_without_holder_submit);
	UT_RUN(test_kind2_requester_uses_existing_r4_slot_and_lands_status22);
	UT_RUN(test_kind2_requester_sleep_error_cancels_cv_before_slot_release);
	UT_RUN(test_kind2_origin_runs_candidate2_cooperatively_and_ships_exact_status22);
	UT_RUN(test_kind2_origin_capacity_covers_three_remote_c32_bursts);
	UT_RUN(test_kind4_origin_reuses_owned_data_tt_data_and_returns_internal_page);
	UT_RUN(test_internal_origin_refusals_do_not_enter_backend_reply_table);
	UT_RUN(test_kind4_origin_error_releases_guard_and_returns_correlated_refusal);
	UT_RUN(test_kind4_origin_backpressure_retains_same_proven_page_without_reacquiring);
	UT_RUN(test_kind4_origin_requires_exact_current_open_and_internal_shape);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
