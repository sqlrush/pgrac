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
static void gcs_block_resource_x_fail_closed_current(void) { UT_ASSERT(false); }
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
static void wb_forget_batch(void) { UT_ASSERT(false); }
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
static bool coverage_start(ReplaceFixture *f, ResourceXSourceDrainWork *work)
{
	ResourceXApplyResult r;
	if (!replace_start(f, true)) return false;
	rf_page_online_plan_destroy_v1(&f->plan);
	coverage_fixture = f; coverage_input_wait = false; coverage_input_current = true;
	coverage_input_pinned = false; coverage_plans = coverage_releases = coverage_aborts = 0;
	coverage_capture_error = coverage_corrupt_page = false; coverage_begin_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	NBuffers = 8;
	wb_shared = calloc(1, sizeof(*wb_shared)); SpinLockInit(&wb_shared->lock);
	pg_atomic_init_u64(&wb_shared->local_revision, 1); wb_shared->bgwriter_procno = -1;
	wb_notice = NULL;
	r = cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(&f->block, 2, 77, 61);
	UT_ASSERT_EQ(r, RESOURCE_X_APPLY_APPLIED);
	return r == RESOURCE_X_APPLY_APPLIED && replace_claim(work);
}
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
int main(void)
{
	UT_PLAN(10); UT_RUN(test_original_holder_park_job_carrier_and_LMS_resume);
	UT_RUN(test_ROOT_lock_contention_keeps_the_same_service_and_PI);
	UT_RUN(test_old_DATA_completion_does_not_hide_the_new_FORWARD);
	UT_RUN(test_ordinary_job_capacity_release_wakes_the_parked_source);
	UT_RUN(test_incomplete_input_waits_then_finishes_once);
	UT_RUN(test_unproved_current_retains_service_and_old_PI_then_rebuilds);
	UT_RUN(test_input_revalidation_failure_does_not_cancel_master_service);
	UT_RUN(test_BG_ERROR_requeues_the_original_job_and_claim);
	UT_RUN(test_exact_cancel_preserves_PI_and_late_job_cannot_touch_successor);
	UT_RUN(test_duplicate_begin_cannot_unclaim_an_existing_job);
	UT_DONE(); return ut_failed_count ? 1 : 0;
}
