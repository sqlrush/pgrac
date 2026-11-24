/* Real retained FORWARD -> original writeback slot -> sealed PAGE -> carrier.
 * WAL input and native descriptor acquisition are explicit provider seams.
 * No network, physical DATA completion or native qualification is simulated.
 * Author: SqlRush <sqlrush@gmail.com> */
#define PGRAC_TEST_REAL_SOURCE_COVERAGE 1
#include <unistd.h>
#include "test_cluster_source_coverage_replace.inc"
#include "cluster/cluster_pi_writeback.h"
#include "cluster/cluster_wal_inputs.h"
#include "utils/resowner.h"

ResourceOwner CurrentResourceOwner = (ResourceOwner)1;
static ReplaceFixture *coverage_fixture;
static BufferDesc coverage_buffer;
bool cluster_normal_stop_continuation_allowed(void) { return true; }
static bool coverage_input_wait, coverage_input_current = true, coverage_input_pinned;
static unsigned coverage_plans, coverage_releases, coverage_aborts;
static bool coverage_capture_error, coverage_corrupt_page;
static ClusterControlRootResult coverage_begin_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
void cluster_lmon_wakeup(void) { }
static void *coverage_zero(Size n) { void *p = palloc(n); memset(p, 0, n); return p; }
#define palloc0 coverage_zero
#define GetBufferDescriptor(id) (&coverage_buffer)
ClusterPcmOwnResult cluster_bufmgr_pcm_own_snapshot_by_tag(const BufferTag *tag, int *id, ClusterPcmOwnSnapshot *out)
{
	UT_ASSERT_EQ(fake_lwlock_depth, 0); UT_ASSERT(BufferTagsEqual(tag, &coverage_fixture->prepared.tag));
	*id = 7; *out = coverage_fixture->prepared; out->flags = 0; return CLUSTER_PCM_OWN_OK;
}
ClusterPcmOwnResult cluster_bufmgr_pcm_own_prepare_x_source_image(BufferDesc *buf,
	const ClusterPcmOwnSnapshot *current, ClusterPcmOwnSnapshot *out, char page[BLCKSZ], ClusterPageWalBindingV1 *wal)
{
	UT_ASSERT_EQ(fake_lwlock_depth, 0); UT_ASSERT(coverage_input_pinned);
	if (coverage_capture_error) { coverage_capture_error = false; pg_re_throw(); }
	*out = coverage_fixture->prepared; memcpy(page, coverage_fixture->page.data, BLCKSZ);
	if (coverage_corrupt_page) page[BLCKSZ - 1] ^= 1;
	*wal = coverage_fixture->last; return CLUSTER_PCM_OWN_OK;
}
ClusterPcmOwnResult cluster_bufmgr_pcm_own_prepare_s_source_image(BufferDesc *buf,
	const ClusterPcmOwnSnapshot *current, SCN requested, ClusterPcmOwnSnapshot *out,
	char page[BLCKSZ], XLogRecPtr *lsn, uint64 *scn, ClusterPcmOwnSourcePrepareRefusal *refusal,
	ClusterPageWalBindingV1 *wal)
{
	return cluster_bufmgr_pcm_own_prepare_x_source_image(buf, current, out, page, wal);
}
ClusterPcmOwnResult cluster_bufmgr_pcm_own_abort_x_revoke(BufferDesc *buf, const ClusterPcmOwnSnapshot *exact)
{ coverage_aborts++; return CLUSTER_PCM_OWN_OK; }
ClusterPcmOwnResult cluster_bufmgr_pcm_own_abort_s_revoke(BufferDesc *buf, const ClusterPcmOwnSnapshot *exact)
{ return cluster_bufmgr_pcm_own_abort_x_revoke(buf, exact); }
/* Failure reporting is a leaf, not an admission decision. Frozen-gate
 * cases expect this original refusal and still execute all product guards. */
static bool coverage_expected_fail_closed;
static unsigned coverage_refusals;
static void gcs_block_resource_x_fail_closed_current(void)
{ UT_ASSERT(coverage_expected_fail_closed); coverage_refusals++; }
/* Authenticated remote-master observation is an environment leaf; local
 * boot remains independent, as it is in separate instances. */
static PcmXSessionAuthResult coverage_session_sample(const BufferTag *tag,
	ResourceXGateSnapshot *gate, int32 *master, uint64 *session)
{
	if (!cluster_pcm_lock_resource_x_gate_snapshot(gate)) return PCM_X_SESSION_AUTH_INVALID;
	*master = fake_gcs_master_node; *session = coverage_fixture->block.common.master_session_incarnation;
	return PCM_X_SESSION_AUTH_OK;
}
static ResourceXApplyResult coverage_session_recheck(const BufferTag *tag,
	const ResourceXGateSnapshot *expected, int32 master, uint64 session)
{
	ResourceXGateSnapshot current; int32 node; uint64 actual;
	return coverage_session_sample(tag, &current, &node, &actual) == PCM_X_SESSION_AUTH_OK
		&& memcmp(expected, &current, sizeof(current)) == 0 && node == master && actual == session
		? RESOURCE_X_APPLY_APPLIED : RESOURCE_X_APPLY_STALE;
}
#define gcs_block_resource_x_gate_session_snapshot_result coverage_session_sample
#define gcs_block_resource_x_gate_session_recheck_result coverage_session_recheck
#include "test_cluster_source_coverage_gcs.inc"
#undef gcs_block_resource_x_gate_session_snapshot_result
#undef gcs_block_resource_x_gate_session_recheck_result
#undef GetBufferDescriptor

static void wb_plan_rejected(ClusterControlRootResult r, RfPageProofDetailV1 d,
	const BufferTag *tag, int32 peer, uint64 epoch, uint64 boot)
{ UT_ASSERT_EQ(fake_lwlock_depth, 0); }
#include "test_cluster_source_coverage_types.inc"
static WritebackBatch *wb_batch;
static ResourceOwner wb_scan_owner;
static void wb_cancel(ClusterPiWritebackJobV1 *job) { UT_ASSERT(job == NULL); }
/* No DATA batch is created by these source-job cases. The original exit
 * hook still calls this independent owner even when its pointer is NULL. */
static void wb_forget_batch(void) { UT_ASSERT(wb_batch == NULL); }
#include "test_cluster_source_coverage_job.inc"
ClusterControlRootResult cluster_wal_inputs_begin_v1(const uint8 uuid[16], uint64 sysid, ClusterWalInputsV1 **out)
{
	UT_ASSERT_EQ(MyBackendType, B_BG_WRITER); UT_ASSERT_EQ(fake_lwlock_depth, 0);
	if (coverage_begin_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) return coverage_begin_result;
	*out = (void *)1; coverage_input_pinned = true; return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
ClusterControlRootResult cluster_wal_inputs_resume_v1(ClusterWalInputsV1 *in)
{ coverage_input_pinned = true; return CLUSTER_CONTROL_ROOT_OK_PRIMARY; }
ClusterControlRootResult cluster_wal_inputs_revalidate_v1(ClusterWalInputsV1 *in)
{ return coverage_input_current && coverage_input_pinned ? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_STALE_TOKEN; }
ClusterControlRootResult cluster_wal_inputs_suspend_v1(ClusterWalInputsV1 *in)
{ coverage_input_pinned = false; return CLUSTER_CONTROL_ROOT_OK_PRIMARY; }
void cluster_wal_inputs_release_v1(ClusterWalInputsV1 **in)
{ if (*in) coverage_releases++; *in = NULL; coverage_input_pinned = false; }
ClusterControlRootResult cluster_wal_inputs_contributions_v1(ClusterWalInputsV1 *in, bool space,
	ClusterThreadRecoveryFabricPlanV1 **out, uint64 *records, RfPageProofDetailV1 *detail)
{
	UT_ASSERT_EQ(MyBackendType, B_BG_WRITER); UT_ASSERT(coverage_input_pinned); UT_ASSERT_EQ(fake_lwlock_depth, 0);
	*detail = RF_PAGE_PROOF_DETAIL_OK;
	if (coverage_input_wait) return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	coverage_plans++;
	if (!replace_plan(coverage_fixture)) return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	*out = (void *)coverage_fixture->plan; *records = 3; return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
const RfPageOnlinePlanV1 *cluster_thread_recovery_fabric_page_plan_v1(const ClusterThreadRecoveryFabricPlanV1 *plan)
{ return (const void *)plan; }
void cluster_thread_recovery_fabric_plan_destroy_v1(ClusterThreadRecoveryFabricPlanV1 **plan)
{
	if (*plan) rf_page_online_plan_destroy_v1(&coverage_fixture->plan);
	*plan = NULL;
}
static bool coverage_start_phase(ReplaceFixture *f, ResourceXSourceDrainWork *work, bool quiet, bool retired)
{
	ResourceXApplyResult r;
	if (!replace_start(f, quiet)) return false;
	rf_page_online_plan_destroy_v1(&f->plan);
	coverage_fixture = f; coverage_input_wait = false; coverage_input_current = true;
	coverage_input_pinned = false; coverage_plans = coverage_releases = coverage_aborts = 0;
	coverage_capture_error = coverage_corrupt_page = false; coverage_begin_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	NBuffers = 8;
	wb_shared = calloc(1, sizeof(*wb_shared)); SpinLockInit(&wb_shared->lock);
	pg_atomic_init_u64(&wb_shared->local_revision, 1); wb_shared->bgwriter_procno = -1;
	wb_notice = NULL;
	if (retired) { local_pi_covered = true; UT_ASSERT(pi_retire(&f->old, false)); }
	r = cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(&f->block, 2, 77, 61);
	UT_ASSERT_EQ(r, RESOURCE_X_APPLY_APPLIED);
	return r == RESOURCE_X_APPLY_APPLIED && replace_claim(work);
}
static bool coverage_start(ReplaceFixture *f, ResourceXSourceDrainWork *work)
{ return coverage_start_phase(f, work, true, false); }
static void coverage_end(ReplaceFixture *f)
{
	if (wb_notice) {
		cluster_thread_recovery_fabric_plan_destroy_v1(&wb_notice->plan);
		cluster_wal_inputs_release_v1(&wb_notice->inputs); pfree(wb_notice); wb_notice = NULL;
	}
	free(wb_shared); wb_shared = NULL; replace_end(f); coverage_fixture = NULL;
}
UT_TEST(test_original_holder_park_job_carrier_and_LMS_resume)
{
	ReplaceFixture f; ResourceXSourceDrainWork work, next = {0}; ResourceXGateSnapshot gate;
	ClusterSemanticAdmissionToken admission = {0}; ResourceXDecodedFrame block;
	int refs;
	if (!coverage_start(&f, &work)) { UT_ASSERT(false); return; }
	refs = replace_references;
	UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&gate));
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1,
		CLUSTER_SEMANTIC_TARGET_SIDE, &admission), CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT_EQ(gcs_block_resource_x_acquire_s_source_capture_owned(&work, &f.block, &admission, &gate), RESOURCE_X_APPLY_BAD_STATE);
	cluster_semantic_activation_leave(&admission);
	UT_ASSERT_EQ(wb_shared->inbound_state, WB_QUEUED); UT_ASSERT_EQ(coverage_plans, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&work, RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_coverage_snapshot_exact(&work, &block), RESOURCE_X_APPLY_APPLIED);
	MyBackendType = B_BG_WRITER;
	UT_ASSERT(wb_source_tick());
	UT_ASSERT_EQ(coverage_plans, 1); UT_ASSERT_EQ(coverage_releases, 1);
	UT_ASSERT_EQ(wb_shared->inbound_state, WB_EMPTY); UT_ASSERT(wb_notice == NULL);
	UT_ASSERT_EQ(f.old.source.m.state->pi_carrier.pi_refs, 1);
	UT_ASSERT_EQ(f.old.source.m.state->pi_carrier.transport_refs, 1);
	UT_ASSERT_EQ(replace_references, refs);
	MyBackendType = B_LMS;
	if (!replace_claim(&next)) UT_ASSERT(replace_claim(&next));
	UT_ASSERT_EQ(next.attempt_generation, work.attempt_generation + 2);
	UT_ASSERT(resource_x_assertion_equal(&next.request.logical_assertion, &work.request.logical_assertion));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(&f.block, 2, 7,
		&f.prepared, &f.finished), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&next, RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_APPLIED);
	coverage_end(&f);
}
static bool coverage_enqueue(ReplaceFixture *f, ResourceXSourceDrainWork *work)
{
	ClusterControlRootResult r = cluster_pi_writeback_source_begin_v1(work, &f->block);
	UT_ASSERT_EQ(r, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(work,
		RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
	return r == CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
UT_TEST(test_ROOT_lock_contention_keeps_the_same_service_and_PI)
{
	ReplaceFixture f; ResourceXSourceDrainWork work; ResourceXDecodedFrame block;
	int refs;
	if (!coverage_start(&f, &work)) { UT_ASSERT(false); return; }
	refs = replace_references;
	if (!coverage_enqueue(&f, &work)) { coverage_end(&f); return; }
	coverage_begin_result = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE; MyBackendType = B_BG_WRITER;
	UT_ASSERT(wb_source_tick());
	UT_ASSERT_EQ(wb_shared->inbound_state, WB_RUNNING);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_coverage_snapshot_exact(&work, &block), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.old.source.m.state->pi_carrier.pi_refs, 1); UT_ASSERT_EQ(replace_references, refs);
	coverage_end(&f);
}
UT_TEST(test_old_DATA_completion_does_not_hide_the_new_FORWARD)
{
	ReplaceFixture f; ResourceXSourceDrainWork work;
	ResourceXDecodedFrame image; ResourceXAcquireSSourceState phase = 0;
	ClusterPcmOwnSnapshot native; int32 id;
	if (!coverage_start(&f, &work)) { UT_ASSERT(false); return; }
	local_pi_covered = true; UT_ASSERT(pi_retire(&f.old, false));
	UT_ASSERT_EQ(f.old.source.m.state->pi_carrier.pi_refs, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_snapshot_exact(&f.block, 2,
		&id, &native, &image, &phase), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(phase, RESOURCE_X_ACQUIRE_S_SOURCE_FORWARD);
	coverage_end(&f);
}
UT_TEST(test_ordinary_job_capacity_release_wakes_the_parked_source)
{
	ReplaceFixture f; ResourceXSourceDrainWork work, next = {0};
	if (!coverage_start(&f, &work)) { UT_ASSERT(false); return; }
	wb_shared->inbound_state = WB_RUNNING; wb_shared->inbound_pid = MyProcPid;
	wb_shared->inbound_version = 1; wb_shared->inbound_revision = 11;
	UT_ASSERT_EQ(cluster_pi_writeback_source_begin_v1(&work, &f.block), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&work,
		RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(!replace_claim(&next));
	wb_notice = palloc0(sizeof(*wb_notice)); wb_notice->revision = 11; wb_notice->version = 1;
	wb_server_finish(NULL);
	if (!replace_claim(&next)) UT_ASSERT(replace_claim(&next));
	UT_ASSERT_EQ(next.attempt_generation, work.attempt_generation + 2);
	coverage_end(&f);
}
UT_TEST(test_incomplete_input_waits_then_finishes_once)
{
	ReplaceFixture f; ResourceXSourceDrainWork work; ResourceXDecodedFrame block;
	int refs;
	if (!coverage_start(&f, &work)) { UT_ASSERT(false); return; }
	refs = replace_references; coverage_enqueue(&f, &work);
	coverage_input_wait = true; MyBackendType = B_BG_WRITER;
	UT_ASSERT(wb_source_tick()); UT_ASSERT(!coverage_input_pinned); UT_ASSERT_EQ(coverage_plans, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_coverage_snapshot_exact(&work, &block), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(replace_references, refs);
	coverage_input_wait = false; UT_ASSERT(wb_source_tick());
	UT_ASSERT_EQ(coverage_plans, 1); UT_ASSERT_EQ(coverage_releases, 1);
	UT_ASSERT_EQ(wb_shared->inbound_state, WB_EMPTY); UT_ASSERT_EQ(replace_references, refs);
	coverage_end(&f);
}
UT_TEST(test_unproved_current_retains_service_and_old_PI_then_rebuilds)
{
	ReplaceFixture f; ResourceXSourceDrainWork work; ResourceXDecodedFrame block;
	ClusterPcmResourceXPiCarrier carrier; int refs;
	if (!coverage_start(&f, &work)) { UT_ASSERT(false); return; }
	carrier = f.old.source.m.state->pi_carrier; refs = replace_references;
	coverage_enqueue(&f, &work); coverage_corrupt_page = true; MyBackendType = B_BG_WRITER;
	UT_ASSERT(wb_source_tick()); UT_ASSERT_EQ(wb_shared->inbound_state, WB_QUEUED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_coverage_snapshot_exact(&work, &block), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&carrier, &f.old.source.m.state->pi_carrier, sizeof(carrier)) == 0);
	UT_ASSERT_EQ(coverage_aborts, 1); UT_ASSERT_EQ(replace_references, refs);
	coverage_corrupt_page = false; UT_ASSERT(wb_source_tick());
	UT_ASSERT_EQ(wb_shared->inbound_state, WB_EMPTY); UT_ASSERT_EQ(coverage_plans, 2);
	coverage_end(&f);
}
UT_TEST(test_input_revalidation_failure_does_not_cancel_master_service)
{
	ReplaceFixture f; ResourceXSourceDrainWork work; ResourceXDecodedFrame block;
	if (!coverage_start(&f, &work)) { UT_ASSERT(false); return; }
	coverage_enqueue(&f, &work); coverage_input_current = false; MyBackendType = B_BG_WRITER;
	UT_ASSERT(wb_source_tick()); UT_ASSERT_EQ(wb_shared->inbound_state, WB_QUEUED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_coverage_snapshot_exact(&work, &block), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.old.source.m.state->pi_carrier.pi_refs, 1);
	coverage_input_current = true; UT_ASSERT(wb_source_tick());
	UT_ASSERT_EQ(wb_shared->inbound_state, WB_EMPTY); coverage_end(&f);
}
UT_TEST(test_BG_ERROR_requeues_the_original_job_and_claim)
{
	ReplaceFixture f; ResourceXSourceDrainWork work; ResourceXDecodedFrame block;
	ClusterPiWritebackNoticeV1 *aborted; volatile bool caught = false;
	if (!coverage_start(&f, &work)) { UT_ASSERT(false); return; }
	coverage_enqueue(&f, &work); coverage_capture_error = true; MyBackendType = B_BG_WRITER;
	PG_TRY(); { (void)wb_source_tick(); } PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught); aborted = wb_notice;
	wb_resource_release(RESOURCE_RELEASE_BEFORE_LOCKS, false, false, NULL);
	UT_ASSERT(wb_notice == NULL); UT_ASSERT_EQ(wb_shared->inbound_state, WB_QUEUED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_coverage_snapshot_exact(&work, &block), RESOURCE_X_APPLY_APPLIED);
	/* Native input ResourceOwner callbacks are a provider seam here. */
	cluster_thread_recovery_fabric_plan_destroy_v1(&aborted->plan);
	cluster_wal_inputs_release_v1(&aborted->inputs); pfree(aborted);
	UT_ASSERT(wb_source_tick()); UT_ASSERT_EQ(wb_shared->inbound_state, WB_EMPTY);
	UT_ASSERT_EQ(coverage_plans, 2); coverage_end(&f);
}
UT_TEST(test_exact_cancel_preserves_PI_and_late_job_cannot_touch_successor)
{
	ReplaceFixture f; ResourceXSourceDrainWork work, next = {0}; ResourceXDecodedFrame block, forged;
	ClusterPcmResourceXPiCarrier carrier; int refs;
	if (!coverage_start(&f, &work)) { UT_ASSERT(false); return; }
	carrier = f.old.source.m.state->pi_carrier; refs = replace_references;
	coverage_enqueue(&f, &work);
	forged = f.block; forged.body.acquire_decision.source_service_generation++;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_coverage_claim_exact(&work, &forged, false, true), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_coverage_claim_exact(&work, &f.block, false, true), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&carrier, &f.old.source.m.state->pi_carrier, sizeof(carrier)) == 0);
	f.block.common.base_authority_generation = f.block.common.authority_generation++;
	f.block.body.acquire_decision.source_service_generation++;
	UT_ASSERT(replace_block_encode(&f));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(&f.block, 2, 77, 61), RESOURCE_X_APPLY_APPLIED);
	MyBackendType = B_BG_WRITER; UT_ASSERT(wb_source_tick()); UT_ASSERT_EQ(coverage_plans, 0);
	MyBackendType = B_LMS; if (!replace_claim(&next)) UT_ASSERT(replace_claim(&next));
	UT_ASSERT(next.attempt_generation > work.attempt_generation);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_coverage_snapshot_exact(&next, &block), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(block.common.authority_generation, f.block.common.authority_generation);
	UT_ASSERT_EQ(replace_references, refs); coverage_end(&f);
}
UT_TEST(test_duplicate_begin_cannot_unclaim_an_existing_job)
{
	ReplaceFixture f; ResourceXSourceDrainWork work; ResourceXDecodedFrame block;
	ClusterWalSourceRef saved;
	if (!coverage_start(&f, &work)) { UT_ASSERT(false); return; }
	coverage_enqueue(&f, &work); saved = local_pi_writer;
	local_pi_writer.claim.database_incarnation++;
	UT_ASSERT_EQ(cluster_pi_writeback_source_begin_v1(&work, &f.block), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&work, RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_coverage_snapshot_exact(&work, &block), RESOURCE_X_APPLY_APPLIED);
	local_pi_writer = saved; MyBackendType = B_BG_WRITER; UT_ASSERT(wb_source_tick());
	UT_ASSERT_EQ(wb_shared->inbound_state, WB_EMPTY); coverage_end(&f);
}
/* The remote master may have consumed the ACK before its sender callback.
 * The real old settlement, ACK transport, PCM work and background job run. */
static bool capacity_start(ReplaceFixture *f, ResourceXSourceDrainWork *work)
{ return coverage_start_phase(f, work, false, false); }
static void capacity_wait(ReplaceFixture *f, const ResourceXSourceDrainWork *work)
{
	UT_ASSERT_EQ(cluster_pi_writeback_source_begin_v1(work, &f->block), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(wb_shared->inbound_state, WB_EMPTY);
	UT_ASSERT_EQ(f->old.source.m.state->holder_coverage_claimed, 0);
	UT_ASSERT_EQ(coverage_plans, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(work,
		RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
}
static void capacity_finish(ReplaceFixture *f)
{
	ResourceXSourceDrainWork next;
	if (!replace_claim(&next)) UT_ASSERT(replace_claim(&next));
	UT_ASSERT(coverage_enqueue(f, &next));
	MyBackendType = B_BG_WRITER; UT_ASSERT(wb_source_tick());
	UT_ASSERT_EQ(coverage_plans, 1); UT_ASSERT_EQ(wb_shared->inbound_state, WB_EMPTY);
	MyBackendType = B_LMS;
	if (!replace_claim(&next)) UT_ASSERT(replace_claim(&next));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(&f->block, 2, 7,
		&f->prepared, &f->finished), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&next,
		RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_APPLIED);
}
UT_TEST(test_remote_successor_waits_for_original_ACK_callback)
{
	for (int staged = 0; staged < 2; staged++) {
		ReplaceFixture f; ResourceXSourceDrainWork work, next; ResourceXIntentSlot ack;
		ClusterPcmResourceXPiCarrier carrier; int refs;
		if (!capacity_start(&f, &work)) { coverage_end(&f); return; }
		if (staged) ack = clean_stage(&f.old.source.m.state->holder_status_intent.slot);
		else ack = f.old.source.m.state->holder_status_intent.slot;
		carrier = f.old.source.m.state->pi_carrier; refs = replace_references;
		capacity_wait(&f, &work);
		UT_ASSERT(memcmp(&ack, &f.old.source.m.state->holder_status_intent.slot, sizeof(ack)) == 0);
		UT_ASSERT(memcmp(&carrier, &f.old.source.m.state->pi_carrier, sizeof(carrier)) == 0);
		UT_ASSERT_EQ(replace_references, refs); UT_ASSERT(!replace_claim(&next));
		if (!staged) ack = clean_stage(&f.old.source.m.state->holder_status_intent.slot);
		UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&ack, true, ++fake_pcm_clock_us));
		capacity_finish(&f); UT_ASSERT_EQ(replace_references, refs); coverage_end(&f);
	}
}
UT_TEST(test_failed_ACK_keeps_old_send_and_new_service)
{
	ReplaceFixture f; ResourceXSourceDrainWork work, next; ResourceXIntentSlot ack;
	if (!capacity_start(&f, &work)) { coverage_end(&f); return; }
	ack = clean_stage(&f.old.source.m.state->holder_status_intent.slot); capacity_wait(&f, &work);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&ack, false, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(f.old.source.m.state->holder_status_intent.slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT(!replace_claim(&next));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(
		&f.old.source.m.state->holder_status_intent.slot, ++fake_pcm_clock_us, &ack), RESOURCE_X_INTENT_NOT_DUE);
	/* Advance the clock leaf through the unchanged 500ms transport backoff. */
	fake_pcm_clock_us += 500000;
	ack = clean_stage(&f.old.source.m.state->holder_status_intent.slot);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&ack, true, ++fake_pcm_clock_us));
	capacity_finish(&f); coverage_end(&f);
}
UT_TEST(test_DATA_before_ACK_keeps_successor_and_finishes_without_old_body)
{
	ReplaceFixture f; ResourceXSourceDrainWork work, next; ResourceXIntentSlot ack;
	if (!capacity_start(&f, &work)) { coverage_end(&f); return; }
	ack = clean_stage(&f.old.source.m.state->holder_status_intent.slot); capacity_wait(&f, &work);
	local_pi_covered = true; UT_ASSERT(pi_retire(&f.old, false));
	UT_ASSERT_EQ(f.old.source.m.state->pi_carrier.pi_refs, 0); UT_ASSERT(!replace_claim(&next));
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&ack, true, ++fake_pcm_clock_us));
	capacity_finish(&f); coverage_end(&f);
}
UT_TEST(test_duplicate_FORWARD_and_old_settlement_preserve_parked_service)
{
	ReplaceFixture f; ResourceXSourceDrainWork work; ClusterPcmResourceXMasterState before;
	if (!capacity_start(&f, &work)) { coverage_end(&f); return; }
	before = *f.old.source.m.state;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(&f.block, 2, 77, 61), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(&f.block, 2, 78, 61), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(&f.block, 2, 77, 62), RESOURCE_X_APPLY_STALE);
	f.block.body.acquire_decision.source_service_generation++; UT_ASSERT(replace_block_encode(&f));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(&f.block, 2, 77, 61), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(clean_receive(&f.request), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&before, f.old.source.m.state, sizeof(before)) == 0); coverage_end(&f);
}
UT_TEST(test_cancel_preserves_old_ACK_PI_and_rejects_late_callback)
{
	ReplaceFixture f; ResourceXSourceDrainWork work; ResourceXIntentSlot ack;
	ClusterPcmResourceXPiCarrier carrier; ResourceXDecodedFrame block;
	if (!capacity_start(&f, &work)) { coverage_end(&f); return; }
	ack = clean_stage(&f.old.source.m.state->holder_status_intent.slot); carrier = f.old.source.m.state->pi_carrier;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_coverage_claim_exact(&work, &f.block, false, true), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&carrier, &f.old.source.m.state->pi_carrier, sizeof(carrier)) == 0);
	UT_ASSERT(memcmp(&ack, &f.old.source.m.state->holder_status_intent.slot, sizeof(ack)) == 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_coverage_snapshot_exact(&work, &block), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&ack, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(&f.block, 2, 77, 61), RESOURCE_X_APPLY_APPLIED);
	capacity_finish(&f);
	UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&ack, true, ++fake_pcm_clock_us));
	coverage_end(&f);
}
UT_TEST(test_ACK_release_between_busy_and_rearm_is_not_lost)
{
	ReplaceFixture f; ResourceXSourceDrainWork work; ResourceXIntentSlot ack;
	if (!capacity_start(&f, &work)) { coverage_end(&f); return; }
	ack = clean_stage(&f.old.source.m.state->holder_status_intent.slot);
	UT_ASSERT_EQ(cluster_pi_writeback_source_begin_v1(&work, &f.block), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&ack, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&work,
		RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
	capacity_finish(&f); coverage_end(&f);
}
UT_TEST(test_malformed_ACK_cannot_admit_successor)
{
	ReplaceFixture f; ClusterPcmResourceXMasterState before;
	if (!replace_start(&f, false)) return;
	f.old.source.m.state->holder_status_intent.slot.authority_generation++; before = *f.old.source.m.state;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(&f.block, 2, 77, 61), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT(memcmp(&before, f.old.source.m.state, sizeof(before)) == 0); replace_end(&f);
}
UT_TEST(test_unsettled_transport_cannot_be_displaced_by_successor)
{
	ReplaceFixture f; ClusterPcmResourceXMasterState before;
	if (!replace_start_phase(&f, 0)) return;
	before = *f.old.source.m.state;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(&f.block, 2, 77, 61), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT(memcmp(&before, f.old.source.m.state, sizeof(before)) == 0);
	UT_ASSERT_EQ(clean_receive(&f.request), RESOURCE_X_APPLY_APPLIED);
	clean_send(&f.old.source.m.state->holder_status_intent.slot); replace_end(&f);
}

UT_TEST(test_DATA_already_retired_before_FORWARD_keeps_ACK_owner)
{
	ReplaceFixture f; ResourceXSourceDrainWork work; ResourceXIntentSlot ack;
	if (!coverage_start_phase(&f, &work, false, true)) { coverage_end(&f); return; }
	UT_ASSERT_EQ(f.old.source.m.state->pi_carrier.pi_refs, 0);
	ack = clean_stage(&f.old.source.m.state->holder_status_intent.slot); capacity_wait(&f, &work);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&ack, true, ++fake_pcm_clock_us));
	capacity_finish(&f); coverage_end(&f);
}
UT_TEST(test_original_intent_completion_also_releases_capacity)
{
	ReplaceFixture f; ResourceXSourceDrainWork work; ResourceXIntentSlot ack;
	if (!capacity_start(&f, &work)) { coverage_end(&f); return; }
	ack = clean_stage(&f.old.source.m.state->holder_status_intent.slot); capacity_wait(&f, &work);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(&ack));
	capacity_finish(&f); coverage_end(&f);
}
UT_TEST(test_unproved_prior_domain_cannot_use_ACK_capacity)
{
	for (int domain = 0; domain < 4; domain++) {
		ReplaceFixture f; ClusterPcmResourceXMasterState before; ResourceXApplyResult result;
		if (!replace_start(&f, false)) return;
		if (domain == 0) f.block.body.acquire_decision.resource_binding++;
		else if (domain == 1) f.block.common.master_session_incarnation++;
		else if (domain == 2) f.block.common.resource_formation++;
		else { f.block.common.authority_generation--; f.block.common.base_authority_generation--; }
		UT_ASSERT(replace_block_encode(&f)); before = *f.old.source.m.state;
		result = cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(&f.block, 2, 77, 61);
		UT_ASSERT(result != RESOURCE_X_APPLY_APPLIED && result != RESOURCE_X_APPLY_DUPLICATE);
		UT_ASSERT(memcmp(&before, f.old.source.m.state, sizeof(before)) == 0); replace_end(&f);
	}
}
/* Original cleanup bodies run below. Native ROOT/WALR ResourceOwner
 * callbacks are explicit provider leaves, as in the existing ERROR case. */
static unsigned lifecycle_scenarios;
static ClusterPiWritebackNoticeV1 *
lifecycle_pause(ReplaceFixture *f, ResourceXSourceDrainWork *work, unsigned cut)
{
	volatile bool caught = false;
	CurrentResourceOwner = (ResourceOwner)1;
	if (!coverage_start(f, work)) { UT_ASSERT(false); return NULL; }
	lifecycle_scenarios++;
	UT_ASSERT(coverage_enqueue(f, work));
	MyBackendType = B_BG_WRITER;
	if (cut == 0) coverage_begin_result = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	else if (cut == 1) coverage_input_wait = true;
	else coverage_capture_error = true;
	PG_TRY(); { UT_ASSERT(wb_source_tick()); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT_EQ(caught, cut == 2); UT_ASSERT(wb_notice != NULL);
	UT_ASSERT_EQ(wb_shared->inbound_state, WB_RUNNING);
	UT_ASSERT_EQ(wb_shared->inbound_pid, MyProcPid);
	UT_ASSERT_EQ(f->old.source.m.state->holder_coverage_claimed, 1);
	return wb_notice;
}
static void lifecycle_native_release(ClusterPiWritebackNoticeV1 *old)
{
	UT_ASSERT(wb_notice != old);
	cluster_thread_recovery_fabric_plan_destroy_v1(&old->plan);
	cluster_wal_inputs_release_v1(&old->inputs); pfree(old);
}
static void lifecycle_service_current(ReplaceFixture *f, const ResourceXSourceDrainWork *work,
	const WritebackSourceRequest *saved, const ClusterPcmResourceXPiCarrier *pi, int refs)
{
	ResourceXDecodedFrame retained;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_coverage_snapshot_exact(work, &retained), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&retained, &f->block, sizeof(retained)) == 0);
	UT_ASSERT(memcmp(&wb_shared->source_request, saved, sizeof(*saved)) == 0);
	UT_ASSERT(memcmp(pi, &f->old.source.m.state->pi_carrier, sizeof(*pi)) == 0);
	UT_ASSERT_EQ(replace_references, refs);
}
static void lifecycle_finish(ReplaceFixture *f)
{
	ResourceXSourceDrainWork next = {0};
	coverage_begin_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	coverage_input_wait = coverage_capture_error = false; coverage_input_current = true;
	MyBackendType = B_BG_WRITER;
	if (!wb_source_tick()) { UT_ASSERT(false); return; }
	UT_ASSERT_EQ(wb_shared->inbound_state, WB_EMPTY); UT_ASSERT(wb_notice == NULL);
	MyBackendType = B_LMS;
	if (!replace_claim(&next) && !replace_claim(&next)) { UT_ASSERT(false); return; }
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(&f->block, 2, 7,
		&f->prepared, &f->finished), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&next,
		RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_APPLIED);
}
static void lifecycle_owner_release(bool commit)
{
	for (unsigned cut = 0; cut < 3; cut++) {
		ReplaceFixture f; ResourceXSourceDrainWork work; WritebackSourceRequest saved;
		ClusterPcmResourceXPiCarrier pi; int refs;
		ClusterPiWritebackNoticeV1 *old = lifecycle_pause(&f, &work, cut);
		if (!old) { coverage_end(&f); return; }
		saved = wb_shared->source_request; pi = f.old.source.m.state->pi_carrier; refs = replace_references;
		wb_resource_release(RESOURCE_RELEASE_BEFORE_LOCKS, commit, true, NULL);
		UT_ASSERT(wb_notice == NULL); UT_ASSERT_EQ(wb_shared->inbound_state, WB_QUEUED);
		UT_ASSERT_EQ(wb_shared->inbound_pid, 0);
		lifecycle_service_current(&f, &work, &saved, &pi, refs); lifecycle_native_release(old);
		lifecycle_finish(&f); coverage_end(&f);
	}
}
UT_TEST(test_source_job_abort_cleanup_preserves_original_service)
{ lifecycle_owner_release(false); }
UT_TEST(test_source_job_commit_cleanup_preserves_original_service)
{ lifecycle_owner_release(true); }
UT_TEST(test_unrelated_ResourceOwner_cannot_take_running_source_job)
{
	for (unsigned cut = 0; cut < 3; cut++) {
		ReplaceFixture f; ResourceXSourceDrainWork work; ClusterPiWritebackNoticeV1 *old = lifecycle_pause(&f, &work, cut);
		if (!old) { coverage_end(&f); return; }
		CurrentResourceOwner = (ResourceOwner)2;
		wb_resource_release(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		UT_ASSERT(wb_notice == old); UT_ASSERT_EQ(wb_shared->inbound_state, WB_RUNNING);
		CurrentResourceOwner = (ResourceOwner)1; lifecycle_finish(&f); coverage_end(&f);
	}
}
UT_TEST(test_late_release_phases_cannot_take_source_job)
{
	for (unsigned cut = 0; cut < 3; cut++) {
		ReplaceFixture f; ResourceXSourceDrainWork work; ClusterPiWritebackNoticeV1 *old = lifecycle_pause(&f, &work, cut);
		if (!old) { coverage_end(&f); return; }
		wb_resource_release(RESOURCE_RELEASE_LOCKS, false, true, NULL);
		UT_ASSERT(wb_notice == old); UT_ASSERT_EQ(wb_shared->inbound_state, WB_RUNNING);
		wb_resource_release(RESOURCE_RELEASE_AFTER_LOCKS, false, true, NULL);
		UT_ASSERT(wb_notice == old); UT_ASSERT_EQ(wb_shared->inbound_state, WB_RUNNING);
		lifecycle_finish(&f); coverage_end(&f);
	}
}
UT_TEST(test_original_BG_exit_requeues_and_replacement_owner_finishes)
{
	for (unsigned cut = 0; cut < 3; cut++) {
		ReplaceFixture f; ResourceXSourceDrainWork work; WritebackSourceRequest saved;
		ClusterPcmResourceXPiCarrier pi; int refs, pid = MyProcPid;
		ClusterPiWritebackNoticeV1 *old = lifecycle_pause(&f, &work, cut);
		if (!old) { coverage_end(&f); return; }
		saved = wb_shared->source_request; pi = f.old.source.m.state->pi_carrier; refs = replace_references;
		wb_shared->bgwriter_pid = MyProcPid; wb_shared->bgwriter_procno = 3;
		wb_exit(0, (Datum)0);
		UT_ASSERT(wb_notice == NULL); UT_ASSERT_EQ(wb_shared->inbound_state, WB_QUEUED);
		UT_ASSERT_EQ(wb_shared->inbound_pid, 0); UT_ASSERT_EQ(wb_shared->bgwriter_pid, 0);
		UT_ASSERT_EQ(wb_shared->bgwriter_procno, -1);
		lifecycle_service_current(&f, &work, &saved, &pi, refs); lifecycle_native_release(old);
		/* Process identity/ResourceOwner are native-provider inputs; the new
		 * job still has to acquire the real original shared request. */
		MyProcPid++; CurrentResourceOwner = (ResourceOwner)2;
		lifecycle_finish(&f); MyProcPid = pid; CurrentResourceOwner = (ResourceOwner)1; coverage_end(&f);
	}
}
UT_TEST(test_BG_exit_does_not_remove_another_worker_registration)
{
	for (unsigned cut = 0; cut < 3; cut++) {
		ReplaceFixture f; ResourceXSourceDrainWork work; ClusterPiWritebackNoticeV1 *old = lifecycle_pause(&f, &work, cut);
		if (!old) { coverage_end(&f); return; }
		wb_shared->bgwriter_pid = MyProcPid + 1; wb_shared->bgwriter_procno = 3;
		wb_exit(0, (Datum)0); UT_ASSERT(wb_notice == NULL);
		UT_ASSERT_EQ(wb_shared->bgwriter_pid, MyProcPid + 1); UT_ASSERT_EQ(wb_shared->bgwriter_procno, 3);
		UT_ASSERT_EQ(wb_shared->inbound_state, WB_QUEUED);
		lifecycle_native_release(old); lifecycle_finish(&f); coverage_end(&f);
	}
}
UT_TEST(test_exact_cancel_during_BG_wait_keeps_old_PI_and_WAL)
{
	for (unsigned cut = 0; cut < 3; cut++) {
		ReplaceFixture f; ResourceXSourceDrainWork work; ResourceXDecodedFrame block;
		ClusterPcmResourceXPiCarrier pi; int refs;
		if (!lifecycle_pause(&f, &work, cut)) { coverage_end(&f); return; }
		pi = f.old.source.m.state->pi_carrier; refs = replace_references;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_coverage_claim_exact(&work, &f.block, false, true), RESOURCE_X_APPLY_APPLIED);
		coverage_begin_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY; coverage_input_wait = coverage_capture_error = false;
		UT_ASSERT(wb_source_tick()); UT_ASSERT_EQ(wb_shared->inbound_state, WB_EMPTY); UT_ASSERT(wb_notice == NULL);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_coverage_snapshot_exact(&work, &block), RESOURCE_X_APPLY_STALE);
		UT_ASSERT(memcmp(&pi, &f.old.source.m.state->pi_carrier, sizeof(pi)) == 0);
		UT_ASSERT_EQ(replace_references, refs); coverage_end(&f);
	}
}
UT_TEST(test_cancel_before_BG_claim_never_starts_new_image_capture)
{
	for (unsigned queued = 0; queued < 2; queued++) {
		ReplaceFixture f; ResourceXSourceDrainWork work; int refs;
		if (!coverage_start(&f, &work)) { UT_ASSERT(false); return; }
		lifecycle_scenarios++; refs = replace_references;
		if (queued) UT_ASSERT(coverage_enqueue(&f, &work));
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_coverage_claim_exact(&work, &f.block, false, true), RESOURCE_X_APPLY_APPLIED);
		MyBackendType = B_BG_WRITER; UT_ASSERT_EQ(wb_source_tick(), queued != 0);
		UT_ASSERT_EQ(coverage_plans, 0); UT_ASSERT_EQ(wb_shared->inbound_state, WB_EMPTY);
		UT_ASSERT_EQ(replace_references, refs); UT_ASSERT_EQ(f.old.source.m.state->pi_carrier.pi_refs, 1);
		coverage_end(&f);
	}
}
UT_TEST(test_late_cancel_cannot_undo_transferred_carrier_or_native_work)
{
	ReplaceFixture f; ResourceXSourceDrainWork work, next; ClusterPcmResourceXMasterState before;
	if (!coverage_start(&f, &work)) { UT_ASSERT(false); return; }
	lifecycle_scenarios++; UT_ASSERT(coverage_enqueue(&f, &work));
	MyBackendType = B_BG_WRITER; UT_ASSERT(wb_source_tick()); before = *f.old.source.m.state;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_coverage_claim_exact(&work, &f.block, false, true), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(memcmp(&before, f.old.source.m.state, sizeof(before)) == 0);
	MyBackendType = B_LMS; if (!replace_claim(&next)) UT_ASSERT(replace_claim(&next));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(&f.block, 2, 7,
		&f.prepared, &f.finished), RESOURCE_X_APPLY_APPLIED);
	before = *f.old.source.m.state;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_coverage_claim_exact(&work, &f.block, false, true), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(memcmp(&before, f.old.source.m.state, sizeof(before)) == 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&next, RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_APPLIED);
	coverage_end(&f);
}
UT_TEST(test_frozen_gate_keeps_unproved_source_debt)
{
	for (unsigned cut = 0; cut < 3; cut++) {
		ReplaceFixture f; ResourceXSourceDrainWork work; ResourceXGateSnapshot gate;
		ClusterPcmResourceXPiCarrier pi; int refs;
		if (!lifecycle_pause(&f, &work, cut)) { coverage_end(&f); return; }
		pi = f.old.source.m.state->pi_carrier; refs = replace_references;
		UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&gate));
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_fail_closed_exact(&gate), RESOURCE_X_APPLY_APPLIED);
		coverage_begin_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY; coverage_input_wait = coverage_capture_error = false;
		coverage_expected_fail_closed = true; coverage_refusals = 0;
		UT_ASSERT(wb_source_tick()); coverage_expected_fail_closed = false;
		UT_ASSERT_EQ(coverage_refusals, 1); UT_ASSERT_EQ(wb_shared->inbound_state, WB_QUEUED);
		UT_ASSERT(!cluster_pcm_lock_resource_x_gate_open_exact(work.request.resource_formation));
		UT_ASSERT(memcmp(&pi, &f.old.source.m.state->pi_carrier, sizeof(pi)) == 0);
		UT_ASSERT_EQ(replace_references, refs); coverage_end(&f);
	}
}
UT_TEST(test_forged_cancel_identity_cannot_clear_original_source_job)
{
	for (unsigned cut = 0; cut < 7; cut++) {
		ReplaceFixture f; ResourceXSourceDrainWork work, forged; ResourceXDecodedFrame block;
		ClusterPcmResourceXMasterState before; ResourceXApplyResult result;
		if (!coverage_start(&f, &work)) { UT_ASSERT(false); return; }
		lifecycle_scenarios++; UT_ASSERT(coverage_enqueue(&f, &work)); forged = work; block = f.block;
		if (cut == 0) forged.r4_record_generation++;
		else if (cut == 1) forged.authenticated_connection_generation++;
		else if (cut == 2) forged.attempt_generation += 2;
		else if (cut == 3) forged.request.master_session_incarnation++;
		else if (cut == 4) forged.request.logical_assertion.resource.blockNum++;
		else if (cut == 5) forged.request.resource_formation++;
		else block.body.acquire_decision.source_service_generation++;
		before = *f.old.source.m.state;
		result = cluster_pcm_lock_resource_x_source_coverage_claim_exact(&forged, &block, false, true);
		UT_ASSERT(result != RESOURCE_X_APPLY_APPLIED && result != RESOURCE_X_APPLY_DUPLICATE);
		UT_ASSERT(memcmp(&before, f.old.source.m.state, sizeof(before)) == 0);
		lifecycle_finish(&f); coverage_end(&f);
	}
}
UT_TEST(test_old_owner_cleanup_cannot_consume_replacement_BG_notice)
{
	ReplaceFixture f; ResourceXSourceDrainWork work; ClusterPiWritebackNoticeV1 *old, *next;
	old = lifecycle_pause(&f, &work, 1); if (!old) { coverage_end(&f); return; }
	wb_resource_release(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	UT_ASSERT(wb_notice == NULL); lifecycle_native_release(old);
	CurrentResourceOwner = (ResourceOwner)2; MyBackendType = B_BG_WRITER;
	UT_ASSERT(wb_source_tick()); next = wb_notice; UT_ASSERT(next != NULL);
	CurrentResourceOwner = (ResourceOwner)1;
	wb_resource_release(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	UT_ASSERT(wb_notice == next); UT_ASSERT_EQ(wb_shared->inbound_state, WB_RUNNING);
	CurrentResourceOwner = (ResourceOwner)2; lifecycle_finish(&f);
	CurrentResourceOwner = (ResourceOwner)1; coverage_end(&f);
	printf("# source lifecycle scenarios=%u; native input cleanup is a provider boundary\n", lifecycle_scenarios);
}

int main(void)
{
	UT_PLAN(33); UT_RUN(test_original_holder_park_job_carrier_and_LMS_resume);
	UT_RUN(test_ROOT_lock_contention_keeps_the_same_service_and_PI);
	UT_RUN(test_old_DATA_completion_does_not_hide_the_new_FORWARD);
	UT_RUN(test_ordinary_job_capacity_release_wakes_the_parked_source);
	UT_RUN(test_incomplete_input_waits_then_finishes_once);
	UT_RUN(test_unproved_current_retains_service_and_old_PI_then_rebuilds);
	UT_RUN(test_input_revalidation_failure_does_not_cancel_master_service);
	UT_RUN(test_BG_ERROR_requeues_the_original_job_and_claim);
	UT_RUN(test_exact_cancel_preserves_PI_and_late_job_cannot_touch_successor);
	UT_RUN(test_duplicate_begin_cannot_unclaim_an_existing_job);
	UT_RUN(test_remote_successor_waits_for_original_ACK_callback);
	UT_RUN(test_failed_ACK_keeps_old_send_and_new_service);
	UT_RUN(test_DATA_before_ACK_keeps_successor_and_finishes_without_old_body);
	UT_RUN(test_duplicate_FORWARD_and_old_settlement_preserve_parked_service);
	UT_RUN(test_cancel_preserves_old_ACK_PI_and_rejects_late_callback);
	UT_RUN(test_ACK_release_between_busy_and_rearm_is_not_lost);
	UT_RUN(test_malformed_ACK_cannot_admit_successor);
	UT_RUN(test_unsettled_transport_cannot_be_displaced_by_successor);
	UT_RUN(test_DATA_already_retired_before_FORWARD_keeps_ACK_owner);
	UT_RUN(test_original_intent_completion_also_releases_capacity);
	UT_RUN(test_unproved_prior_domain_cannot_use_ACK_capacity);
	UT_RUN(test_source_job_abort_cleanup_preserves_original_service);
	UT_RUN(test_source_job_commit_cleanup_preserves_original_service);
	UT_RUN(test_unrelated_ResourceOwner_cannot_take_running_source_job);
	UT_RUN(test_late_release_phases_cannot_take_source_job);
	UT_RUN(test_original_BG_exit_requeues_and_replacement_owner_finishes);
	UT_RUN(test_BG_exit_does_not_remove_another_worker_registration);
	UT_RUN(test_exact_cancel_during_BG_wait_keeps_old_PI_and_WAL);
	UT_RUN(test_cancel_before_BG_claim_never_starts_new_image_capture);
	UT_RUN(test_late_cancel_cannot_undo_transferred_carrier_or_native_work);
	UT_RUN(test_frozen_gate_keeps_unproved_source_debt);
	UT_RUN(test_forged_cancel_identity_cannot_clear_original_source_job);
	UT_RUN(test_old_owner_cleanup_cannot_consume_replacement_BG_notice);
	UT_DONE(); return ut_failed_count ? 1 : 0;
}
