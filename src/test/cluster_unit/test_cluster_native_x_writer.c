/* Actual physical disk X, original cached target and writer ledger/use.
 * Authentication, object events and OS process leaves are controlled.
 * The unrelated legacy acquisition is a rejection sentinel, never success.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_native_x_disk_fixture.inc"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_xnode_profile.h"
#include "miscadmin.h"

BufferDescPadded *BufferDescriptors = (BufferDescPadded *)&request_buf;
static unsigned wc_legacy, wc_failures, wc_waits, wc_object_checks;
static bool wc_object_enabled, wc_object_valid = true;
static uint64 wc_r4 = 77, wc_session_id = 31;
static int32 wc_master;
static unsigned wc_unavailable, wc_pauses;
static bool wc_cancel_pause;
static ResourceXWriterPath wc_path = RESOURCE_X_WRITER_TARGET;
ResourceXWriterPath cluster_resource_x_writer_path_snapshot(uint64 *r4)
{
	*r4 = wc_r4; return wc_path;
}
static PcmXSessionAuthResult
wc_session(const BufferTag *tag, ResourceXGateSnapshot *gate, int32 *master, uint64 *session)
{
	/* Existing pre-use recheck also samples this local domain under content-X. */
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & BM_LOCKED, 0);
	UT_ASSERT(BufferTagsEqual(tag, &request_base.tag));
	if (wc_unavailable != 0) { wc_unavailable--; return PCM_X_SESSION_AUTH_FRESH_NOT_READY; }
	if (!cluster_pcm_lock_resource_x_gate_snapshot(gate) || gate->phase != RESOURCE_X_GATE_OPEN)
		return PCM_X_SESSION_AUTH_INVALID;
	*master = wc_master; *session = wc_session_id;
	return PCM_X_SESSION_AUTH_OK;
}
static ResourceXApplyResult
gcs_block_resource_x_target_acquire_internal_trace_impl(
	BufferDesc *buf, const BufferTag *tag, uint64 r4, uint64 native, uint64 token,
	bool join, uint64 *deadline, ResourceXAuxiliaryAcquireContext *aux,
	bool *creation, ResourceXAcquisitionRef *ref)
{
	(void)buf; (void)tag; (void)r4; (void)native; (void)token; (void)join;
	(void)deadline; (void)aux; (void)creation; (void)ref;
	wc_legacy++;
	/* Preserve the original driver's input rejection at this leaf. */
	if (deadline != NULL && *deadline == UINT64_MAX)
		return RESOURCE_X_APPLY_INVALID;
	if (aux != NULL) {
		UT_ASSERT(aux->active && !aux->reobserve);
		UT_ASSERT(BufferTagsEqual(tag, &aux->resource));
		UT_ASSERT(deadline == &aux->absolute_deadline_us);
	}
	return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
}
#define gcs_block_resource_x_gate_session_snapshot_result wc_session
static void wc_pause(long usecs)
{
	UT_ASSERT_EQ(fake_lwlock_depth, 0); UT_ASSERT_EQ(usecs, 1000);
	if (++wc_pauses > 2 || wc_cancel_pause) pg_re_throw();
}
#define pg_usleep wc_pause
#ifdef X_WRITER_PARENT_SOURCE
#include X_WRITER_PARENT_SOURCE
#else
#include "test_cluster_native_x_writer_gcs.inc"
#endif
#undef pg_usleep
#undef gcs_block_resource_x_gate_session_snapshot_result

static void wc_fail(ResourceXApplyResult result, BufferDesc *buf, const char *operation)
{
	(void)result; (void)buf; (void)operation;
	wc_failures++; pg_re_throw();
}
static void wc_close(const ResourceXGateSnapshot *gate)
{
	(void)cluster_pcm_lock_resource_x_gate_fail_closed_exact(gate);
}
static bool wc_wait(LWLock *lock, int32 id, uint32 n, bool *barrier)
{
	(void)lock; (void)id; (void)n; (void)barrier;
	if (++wc_waits > 2) pg_re_throw();
	return true;
}
static bool wc_object_begin(RelFileLocator locator, uint64 *revision)
{
	(void)locator; UT_ASSERT_EQ(fake_lwlock_depth, 0);
	*revision = 7; return true;
}
static bool wc_object_current(uint64 revision)
{
	UT_ASSERT_EQ(revision, 7); wc_object_checks++;
	return wc_object_valid;
}
#define cluster_bufmgr_should_pcm_track(buf) cluster_pcm_x_buffer_tag_tracked(&(buf)->tag, cluster_shared_catalog)
#define LockBufHdr stage_header_lock
#define ereport(level, rest) wc_fail(RESOURCE_X_APPLY_RECOVERY_BLOCKED, NULL, "ledger error report")
#define cluster_bufmgr_resource_x_writer_report_failure wc_fail
#define cluster_bufmgr_resource_x_fail_closed_exact wc_close
#define cluster_bufmgr_resource_x_wait_retry wc_wait
#define cluster_content_trace_pair_event(...) ((void)0)
#define cluster_enabled wc_object_enabled
#define cluster_shared_config wc_object_enabled
#define cluster_interconnect_tier CLUSTER_IC_TIER_1
#define cluster_smgr_which_for(...) 1
#define cluster_ko_peer_delivery_begin_v3 wc_object_begin
#define cluster_ko_peer_delivery_current_v3 wc_object_current
/* This component starts after physical C; the resident N entrance is
 * exercised with its real body by test_cluster_native_resident_x. */
#define cluster_bufmgr_pcm_acquire_resident_x(...) RESOURCE_X_APPLY_NOT_FOUND
#include "test_cluster_native_x_writer_ledger.inc"
#undef cluster_bufmgr_pcm_acquire_resident_x
#undef cluster_ko_peer_delivery_current_v3
#undef cluster_ko_peer_delivery_begin_v3
#undef cluster_smgr_which_for
#undef cluster_interconnect_tier
#undef cluster_shared_config
#undef cluster_enabled
#undef cluster_content_trace_pair_event
#undef cluster_bufmgr_resource_x_wait_retry
#undef cluster_bufmgr_resource_x_fail_closed_exact
#undef cluster_bufmgr_resource_x_writer_report_failure
#undef cluster_bufmgr_should_pcm_track
#undef LockBufHdr
#undef ereport

static bool
wc_start(EvidenceFixture *f)
{
	if (!xd_start(f) || xd_run() != RESOURCE_X_APPLY_APPLIED) return false;
	wc_legacy = wc_failures = wc_waits = wc_object_checks = 0;
	wc_object_enabled = false; wc_object_valid = true;
	wc_r4 = 77; wc_path = RESOURCE_X_WRITER_TARGET;
	wc_unavailable = wc_pauses = 0; wc_cancel_pause = false;
	cluster_xnode_profile_enabled = cluster_update_trace_enabled = false;
	wc_session_id = 31; wc_master = request_entry()->resource_x_bootstrap_round.current_master_node;
	MemSet(cluster_bufmgr_pcm_x_writer_ledger, 0, sizeof(cluster_bufmgr_pcm_x_writer_ledger));
	return true;
}
static ResourceXApplyResult
wc_acquire(ResourceXAcquisitionRef *ref)
{
	uint64 deadline = 0;
	return cluster_gcs_resource_x_target_acquire_until_exact(&request_buf, &request_base.tag,
		77, &deadline, ref);
}
static ClusterPcmXWriterLedgerEntry *wc_prepare(void)
{
	uint64 deadline = 0;
	ClusterPcmXWriterLedgerEntry *volatile entry = NULL;
	PG_TRY();
	{
		entry = cluster_bufmgr_pcm_x_writer_prepare_target(&request_buf, &request_base.tag,
			PCM_LOCK_MODE_X, 77, &deadline, NULL, NULL, NULL);
	}
	PG_CATCH(); { /* Keep an original ERROR as a failed method, not an abort. */ }
	PG_END_TRY();
	return entry;
}
static void wc_unchanged(void)
{
	UT_ASSERT_EQ(wc_legacy, 0); UT_ASSERT_EQ(wc_failures, 0);
	UT_ASSERT_EQ(disk_reads, 1); UT_ASSERT_EQ(complete_io_calls, 1);
	UT_ASSERT_EQ(request_shared.rings[1].count, 1);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + 1);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
		RESOURCE_X_DELIVERY_ACQUIRE_READ_NOTICE);
	UT_ASSERT_EQ(quiet_aborts + ua_aborts, 0); dg_stack_empty();
}

UT_TEST(test_cached_X_reuses_exact_completed_full_E_without_legacy_acquire)
{
	EvidenceFixture f; ResourceXAcquisitionRef ref = {0};
	XD_REQUIRE(wc_start(&f));
	for (int trace = 0; trace < 2; trace++) {
		/* Exercise both original wrapper branches; no profile region is
		 * attached in this host component. */
		cluster_xnode_profile_enabled = trace;
		UT_ASSERT_EQ(wc_acquire(&ref), RESOURCE_X_APPLY_APPLIED);
	}
	cluster_xnode_profile_enabled = false;
	UT_ASSERT_EQ(ref.acquisition_generation, prepared_claim.observation.request.assertion_sequence);
	UT_ASSERT(resource_x_assertion_equal(&ref.assertion, &prepared_claim.observation.request.logical_assertion));
	wc_unchanged();
}
UT_TEST(test_cached_X_original_ledger_activates_and_unlocks_without_retiring_residency)
{
	EvidenceFixture f; ClusterPcmXWriterLedgerEntry *entry;
	XD_REQUIRE(wc_start(&f));
	for (int n = 0; n < 2; n++) {
		/* Resident node authority survives the original executor backend. */
		if (n == 1) {
			request_proc.cluster_grd_generation++;
			/* Protected resident X can contain later dirty bytes, whose disk
			 * checksum is not refreshed until actual writeback. */
			pg_atomic_fetch_or_u32(&request_buf.state, BM_DIRTY);
			disk_shared.data[256] ^= 1;
		}
		entry = wc_prepare(); XD_REQUIRE(entry != NULL);
		UT_ASSERT_EQ(entry->phase, PCM_X_WRITER_LEDGER_ACQUIRING);
		LWLockAcquire(entry->content_lock, LW_EXCLUSIVE);
		UT_ASSERT(cluster_bufmgr_pcm_x_writer_activate(entry, true));
		UT_ASSERT_EQ(entry->phase, PCM_X_WRITER_LEDGER_ACTIVE);
		cluster_bufmgr_pcm_x_writer_mark_releasing(entry);
		LWLockRelease(entry->content_lock);
		cluster_bufmgr_pcm_x_writer_release(entry);
		UT_ASSERT(cluster_bufmgr_pcm_x_writer_find(&request_buf) == NULL);
	}
	wc_unchanged();
}
UT_TEST(test_cached_X_wrong_native_generation_never_exports_or_rearms)
{
	EvidenceFixture f; ResourceXAcquisitionRef ref = {0};
	XD_REQUIRE(wc_start(&f));
	pg_atomic_fetch_add_u64(&request_own.generation, 1);
	UT_ASSERT(wc_acquire(&ref) != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(wc_legacy, 0); UT_ASSERT_EQ(request_shared.rings[1].count, 1);
}
UT_TEST(test_cached_X_domain_loss_never_falls_into_legacy_acquire)
{
	for (int n = 0; n < 3; n++) {
		EvidenceFixture f; ResourceXAcquisitionRef ref = {0};
		XD_REQUIRE(wc_start(&f));
		if (n == 0) wc_r4++;
		if (n == 1) wc_session_id++;
		if (n == 2) wc_master = (wc_master + 1) % 4;
		UT_ASSERT(wc_acquire(&ref) != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(wc_legacy, 0); UT_ASSERT_EQ(request_shared.rings[1].count, 1);
	}
}
UT_TEST(test_cached_X_full_E_receipt_or_phase_mismatch_retains_responsibility)
{
	for (int n = 0; n < 3; n++) {
		EvidenceFixture f; ResourceXAcquisitionRef ref = {0};
		XD_REQUIRE(wc_start(&f));
		if (n == 0) request_entry()->resource_x_bootstrap_round.acquire_read_receipt.authority_generation++;
		if (n == 1) request_entry()->resource_x_bootstrap_round.delivery_executor_purpose = RESOURCE_X_DELIVERY_ACQUIRE_READ_ARMED;
		if (n == 2) request_entry()->resource_x_bootstrap_round.cached_ownership_generation++;
		UT_ASSERT(wc_acquire(&ref) != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(wc_legacy, 0); UT_ASSERT_EQ(request_shared.rings[1].count, 1);
		UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + 1);
	}
}
UT_TEST(test_cached_X_content_boundary_native_drift_clears_only_local_use)
{
	EvidenceFixture f; ClusterPcmXWriterLedgerEntry *entry;
	XD_REQUIRE(wc_start(&f)); entry = wc_prepare(); XD_REQUIRE(entry != NULL);
	LWLockAcquire(entry->content_lock, LW_EXCLUSIVE);
	pg_atomic_fetch_add_u64(&request_own.generation, 1);
	UT_ASSERT(!cluster_bufmgr_pcm_x_writer_activate(entry, true));
	LWLockRelease(BufferDescriptorGetContentLock(&request_buf));
	UT_ASSERT(cluster_bufmgr_pcm_x_writer_find(&request_buf) == NULL);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + 2);
	UT_ASSERT_EQ(wc_legacy, 0); UT_ASSERT_EQ(quiet_aborts + ua_aborts, 0);
}
UT_TEST(test_cached_X_object_revision_loss_keeps_node_cover_and_notification)
{
	EvidenceFixture f; ClusterPcmXWriterLedgerEntry *entry;
	XD_REQUIRE(wc_start(&f)); wc_object_enabled = true;
	entry = wc_prepare(); XD_REQUIRE(entry != NULL);
	wc_object_valid = false;
	LWLockAcquire(entry->content_lock, LW_EXCLUSIVE);
	UT_ASSERT(!cluster_bufmgr_pcm_x_writer_activate(entry, true));
	LWLockRelease(BufferDescriptorGetContentLock(&request_buf));
	UT_ASSERT(cluster_bufmgr_pcm_x_writer_find(&request_buf) == NULL);
	UT_ASSERT_EQ(wc_object_checks, 1); wc_unchanged();
}
UT_TEST(test_cached_X_exception_cleanup_clears_unheld_local_ledger_only)
{
	EvidenceFixture f;
	XD_REQUIRE(wc_start(&f)); XD_REQUIRE(wc_prepare() != NULL);
	cluster_bufmgr_pcm_x_writer_exception_cleanup_all();
	UT_ASSERT(cluster_bufmgr_pcm_x_writer_find(&request_buf) == NULL);
	wc_unchanged();
}
UT_TEST(test_cached_X_observation_gap_waits_locally_and_cancel_keeps_original_X)
{
	for (int cancel = 0; cancel < 2; cancel++) {
		EvidenceFixture f; ResourceXAcquisitionRef ref = {0};
		volatile bool caught = false;
		volatile ResourceXApplyResult result = RESOURCE_X_APPLY_INVALID;
		XD_REQUIRE(wc_start(&f));
		wc_unavailable = 1; wc_cancel_pause = cancel;
		PG_TRY(); { result = wc_acquire(&ref); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT_EQ(caught, cancel);
		if (!cancel) UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(wc_pauses, 1); wc_unchanged();
	}
}
UT_TEST(test_cached_X_does_not_replace_non_X_or_known_new_owners)
{
	for (int n = 0; n < 2; n++) {
		EvidenceFixture f; ResourceXAcquisitionRef ref = {0}; uint64 deadline = 0;
		XD_REQUIRE(wc_start(&f));
		if (n == 0) request_buf.pcm_state = PCM_STATE_N;
		UT_ASSERT_EQ(gcs_block_resource_x_target_acquire_internal(&request_buf,
			&request_base.tag, 77, n ? request_base.generation : 0,
			n ? prepared_episode.reservation_token : 0, false, &deadline, NULL, NULL, &ref),
			RESOURCE_X_APPLY_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(wc_legacy, 1);
		UT_ASSERT_EQ(request_shared.rings[1].count, 1);
	}
}
UT_TEST(test_cached_X_public_deadline_guard_precedes_cached_success)
{
	for (int trace = 0; trace < 2; trace++) {
		EvidenceFixture f; ResourceXAcquisitionRef ref = {0}; uint64 deadline = UINT64_MAX;
		ResourceXAuxiliaryAcquireContext aux = {0}; BufferTag old;
		XD_REQUIRE(wc_start(&f)); cluster_xnode_profile_enabled = trace;
		UT_ASSERT_EQ(cluster_gcs_resource_x_target_acquire_until_exact(&request_buf,
			&request_base.tag, 77, &deadline, &ref), RESOURCE_X_APPLY_INVALID);
		old = request_base.tag; old.forkNum = VISIBILITYMAP_FORKNUM;
		aux.active = true; aux.resource = old; aux.r4_record_generation = 77;
		aux.absolute_deadline_us = UINT64_MAX;
		UT_ASSERT_EQ(cluster_gcs_resource_x_target_acquire_reobserve_exact(&request_buf,
			&old, 77, &aux, &ref), RESOURCE_X_APPLY_INVALID);
		UT_ASSERT_EQ(wc_legacy, 0); UT_ASSERT_EQ(ref.acquisition_generation, 0);
		UT_ASSERT_EQ(request_shared.rings[1].count, 1);
	}
}
UT_TEST(test_cached_X_auxiliary_retag_returns_to_original_classification_owner)
{
	for (int trace = 0; trace < 2; trace++) {
		EvidenceFixture f; ResourceXAcquisitionRef ref = {0};
		ResourceXAuxiliaryAcquireContext aux = {0}; BufferTag old;
		XD_REQUIRE(wc_start(&f)); cluster_xnode_profile_enabled = trace;
		/* The unpinned auxiliary handle is A; this descriptor now caches X
		 * for B. Only the original driver classifies caller-owned repin. */
		old = request_base.tag; old.forkNum = FSM_FORKNUM;
		UT_ASSERT_EQ(cluster_gcs_resource_x_target_acquire_reobserve_exact(&request_buf,
			&old, 77, &aux, &ref), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(wc_legacy, 1); UT_ASSERT_EQ(ref.acquisition_generation, 0);
		UT_ASSERT_EQ(request_shared.rings[1].count, 1);
	}
}
int main(void)
{
	UT_PLAN(12);
	UT_RUN(test_cached_X_reuses_exact_completed_full_E_without_legacy_acquire);
	UT_RUN(test_cached_X_original_ledger_activates_and_unlocks_without_retiring_residency);
	UT_RUN(test_cached_X_wrong_native_generation_never_exports_or_rearms);
	UT_RUN(test_cached_X_domain_loss_never_falls_into_legacy_acquire);
	UT_RUN(test_cached_X_full_E_receipt_or_phase_mismatch_retains_responsibility);
	UT_RUN(test_cached_X_content_boundary_native_drift_clears_only_local_use);
	UT_RUN(test_cached_X_object_revision_loss_keeps_node_cover_and_notification);
	UT_RUN(test_cached_X_exception_cleanup_clears_unheld_local_ledger_only);
	UT_RUN(test_cached_X_observation_gap_waits_locally_and_cancel_keeps_original_X);
	UT_RUN(test_cached_X_does_not_replace_non_X_or_known_new_owners);
	UT_RUN(test_cached_X_public_deadline_guard_precedes_cached_success);
	UT_RUN(test_cached_X_auxiliary_retag_returns_to_original_classification_owner);
	UT_DONE(); return ut_failed_count ? 1 : 0;
}
