/* Retained-cut caller lifecycle; real job/CF/WALR/owner/ACK bodies.
 * Immutable I/O, business census, native allocator/lock/interrupt/clock are
 * explicit boundaries. No installed checkpoint or ROOT publish is claimed.
 * Author: SqlRush <sqlrush@gmail.com> */
#define PGRAC_WAL_INPUTS_JOB_PIN_EMBEDDED
#include "test_cluster_wal_inputs_job_pin.c"
#define begin_job retained_inputs_job
#include "test_cluster_wal_inputs_job_begin_fixture.h"
#undef begin_job
#include "cluster_wal_retained_cut_internal.h"
#include "test_cluster_wal_inputs_retained_claim.inc"

static unsigned retained_dirty_calls, retained_scan_calls, retained_scratch;
static bool retained_throw, retained_defer_release, retained_throw_release;
static ClusterControlRootResult retained_scan_result;

static void *
retained_alloc(Size size)
{
	++retained_scratch;
	return calloc(1, size);
}
static void
retained_free(void *pointer)
{
	UT_ASSERT(retained_scratch > 0);
	--retained_scratch;
	free(pointer);
}
void BufFileClose(BufFile *file) { (void)file; abort(); }
bool
cluster_page_wal_dirty_floor_v1(const ClusterWalSourceRef *source, ClusterPageWalDirtyFloorV1 *out)
{
	(void)source;
	++retained_dirty_calls;
	memset(out, 0, sizeof(*out));
	return true;
}
bool
cluster_pcm_local_pi_floor_v1(const ClusterWalSourceRef *source, ClusterPcmLocalPiFloorV1 *out)
{
	(void)source;
	memset(out, 0, sizeof(*out));
	return true;
}
static void retained_dirty_merge_presync(ClusterPageWalDirtyFloorV1 *dirty) { (void)dirty; }

/* The business scan is a configured boundary here; its full old algorithm,
 * record decoders and assertions run separately in the retained-cut tests. */
static ClusterControlRootResult
retained_census(RetainedCutWork *work, ClusterWalInputsV1 *inputs,
	const ClusterWalSourceRef *self, ClusterWalRetainedCutV1 *out, RfPageProofDetailV1 *detail)
{
	ClusterControlRootResult result;

	(void)work; (void)self; (void)detail;
	++retained_scan_calls;
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(inputs), 2);
	UT_ASSERT(retained_inputs_job != NULL);
	UT_ASSERT_EQ(retained_inputs_job->state, WAL_INPUT_JOB_STATE_USABLE);
	if (retained_throw)
		pg_re_throw();
	out->lower = 300;
	out->old_lower = 100;
	if (retained_scan_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		if (retained_defer_release)
			owner_ack_on_wait = false;
		return retained_scan_result;
	}
	result = cluster_wal_inputs_job_revalidate_v1(retained_inputs_job);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		result = cluster_wal_inputs_root_token_v1(inputs, &out->root_token);
	if (retained_defer_release)
		owner_ack_on_wait = false;
	if (retained_throw_release) {
		owner_ack_on_wait = false;
		job_throw_on_wait = true;
	}
	return result;
}

/* A raw entry cannot qualify this fixture's original job. */
ClusterControlRootResult
cluster_wal_inputs_begin_v1(const uint8 uuid[16], uint64 system_id, ClusterWalInputsV1 **out)
{
	(void)uuid; (void)system_id; (void)out;
	fprintf(stderr, "raw retained input entry bypassed the original job\n");
	abort();
}

#define palloc0 retained_alloc
#define pfree retained_free
#include "test_cluster_wal_inputs_retained_caller.inc"
#undef palloc0
#undef pfree

static ClusterWalSourceRef
retained_setup(void)
{
	ClusterWalSourceRef self = {0};

	begin_setup();
	foundation_finish(&retained_inputs_job);
	retained_dirty_calls = retained_scan_calls = retained_scratch = 0;
	retained_throw = retained_defer_release = retained_throw_release = false;
	retained_scan_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	self.claim.identity.storage_uuid[0] = 1;
	self.claim.identity.system_identifier = 1234;
	self.claim.identity.authority_uuid[0] = 2;
	self.claim.identity.thread_claim_created_at = 1;
	self.claim.identity.root_lineage_seq = 1;
	self.claim.identity.origin_thread_id = 1;
	self.claim.identity.origin_owner_incarnation = 7;
	self.claim.database_incarnation = 3;
	self.claim.max_config_generation = 5;
	self.claim.claim_sha256[0] = 1;
	self.timeline = 1;
	return self;
}

static void
retained_cleanup(void)
{
	if (retained_inputs_job != NULL)
		begin_cleanup();
	else
		foundation_end();
	if (inputs_jobs_process.caller_lifetime != NULL) {
		MemoryContextDelete(inputs_jobs_process.caller_lifetime);
		inputs_jobs_process.caller_lifetime = NULL;
	}
	cluster_controlfile_shared_authority = false;
	UT_ASSERT_EQ(retained_scratch, 0);
}

UT_TEST(retained_actual_caller_publishes_only_after_synchronous_retirement)
{
	ClusterWalSourceRef self = retained_setup();
	ClusterWalRetainedCutV1 out;
	RfPageProofDetailV1 detail;

	UT_ASSERT_EQ(cluster_wal_retained_cut_compute_v1(&self, &out, &detail, NULL),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(out.lower, 300);
	UT_ASSERT(retained_inputs_job == NULL);
	UT_ASSERT_EQ(begin_roots, 4);
	UT_ASSERT_EQ(retained_scan_calls, 1);
	UT_ASSERT_EQ(pin_native_refs, 0);
	retained_cleanup();
}

static void
retained_ack_all(void)
{
	while (retained_inputs_job->pin != NULL) {
		ClusterWalReadPinV1 *pin;

		cluster_wal_inputs_jobs_service_poll_v1();
		pin = retained_inputs_job->pin;
		if (pin == NULL)
			break;
		pin_ack_member(&pin->locks[pin->cursor - 1].request, false);
		cluster_wal_inputs_jobs_service_poll_v1();
	}
}

UT_TEST(retained_exact_ack_completion_allows_a_fresh_scope)
{
	ClusterWalSourceRef self = retained_setup();
	ClusterWalRetainedCutV1 out;
	RfPageProofDetailV1 detail;
	uint64 generation;

	retained_defer_release = true;
	UT_ASSERT_EQ(cluster_wal_retained_cut_compute_v1(&self, &out, &detail, NULL),
		CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	generation = retained_inputs_job->generation;
	retained_ack_all();
	UT_ASSERT_EQ(retained_inputs_job->state, WAL_INPUT_JOB_STATE_DONE);
	UT_ASSERT_EQ(retained_dirty_calls, 1);
	retained_defer_release = false;
	owner_ack_on_wait = true;
	UT_ASSERT_EQ(cluster_wal_retained_cut_compute_v1(&self, &out, &detail, NULL),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(retained_inputs_job == NULL);
	UT_ASSERT(inputs_jobs_process.generation > generation);
	UT_ASSERT_EQ(retained_dirty_calls, 2);
	UT_ASSERT_EQ(retained_scan_calls, 2);
	UT_ASSERT_EQ(out.lower, 300);
	retained_cleanup();
}

UT_TEST(retained_business_error_is_not_replaced_by_cleanup_wait)
{
	ClusterWalSourceRef self = retained_setup();
	ClusterWalRetainedCutV1 out;
	RfPageProofDetailV1 detail;

	retained_scan_result = CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
	owner_ack_on_wait = true;
	/* A typed business error remains the returned verdict, including when
	 * its original release cannot yet finish. */
	retained_defer_release = true;
	UT_ASSERT_EQ(cluster_wal_retained_cut_compute_v1(&self, &out, &detail, NULL),
		CLUSTER_CONTROL_ROOT_HASH_MISMATCH);
	UT_ASSERT_EQ(out.lower, 0);
	UT_ASSERT_EQ(retained_scan_calls, 1);
	UT_ASSERT(retained_inputs_job != NULL);
	UT_ASSERT_EQ(retained_inputs_job->original_result, CLUSTER_CONTROL_ROOT_HASH_MISMATCH);
	UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_PENDING);
	retained_cleanup();
}

UT_TEST(retained_interrupt_in_first_retirement_rethrows_without_published_lower)
{
	ClusterWalSourceRef self = retained_setup();
	ClusterWalRetainedCutV1 out;
	RfPageProofDetailV1 detail;
	volatile bool caught = false;

	retained_throw_release = true;
	PG_TRY(); { (void)cluster_wal_retained_cut_compute_v1(&self, &out, &detail, NULL); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught && !job_throw_on_wait);
	UT_ASSERT_EQ(out.lower, 0);
	UT_ASSERT_EQ(retained_scratch, 0);
	UT_ASSERT(retained_inputs_job != NULL);
	UT_ASSERT_EQ(pin_native_refs, 2);
	UT_ASSERT((retained_inputs_job->flags & WAL_INPUT_JOB_FLAG_ERROR) != 0);
	retained_cleanup();
}

UT_TEST(retained_census_error_revokes_without_wait_and_survives_work_reset)
{
	ClusterWalSourceRef self = retained_setup();
	ClusterWalRetainedCutV1 out;
	RfPageProofDetailV1 detail;
	unsigned waits;
	volatile bool caught = false;

	retained_throw = true;
	PG_TRY(); { (void)cluster_wal_retained_cut_compute_v1(&self, &out, &detail, NULL); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(out.lower, 0);
	UT_ASSERT_EQ(retained_scratch, 0);
	UT_ASSERT_EQ(retained_inputs_job->state, WAL_INPUT_JOB_STATE_RETIRING_CF);
	UT_ASSERT_EQ(retained_inputs_job->original_result, UINT32_MAX);
	UT_ASSERT((retained_inputs_job->flags & WAL_INPUT_JOB_FLAG_ERROR) != 0);
	UT_ASSERT_EQ(pin_native_refs, 2);
	waits = owner_wait_calls;
	job_resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, job_resource_argument);
	MemoryContextReset(work_context);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT_EQ(cluster_wal_retained_cut_compute_v1(&self, &out, &detail, NULL),
		CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(retained_scan_calls, 1);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	retained_cleanup();
}

UT_TEST(retained_each_cf_phase_keeps_debt_and_blocks_new_root_reads)
{
	for (unsigned phase = 1; phase <= 4; ++phase) {
		ClusterWalSourceRef self = retained_setup();
		ClusterWalRetainedCutV1 out;
		RfPageProofDetailV1 detail;
		unsigned waits, roots;

		begin_pending_phase = phase;
		UT_ASSERT_EQ(cluster_wal_retained_cut_compute_v1(&self, &out, &detail, NULL),
			CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		UT_ASSERT_EQ(out.lower, 0);
		UT_ASSERT(retained_inputs_job != NULL);
		UT_ASSERT_EQ(retained_inputs_job->cf.state, CF_JOB_RECEIPT_RETIRING);
		waits = owner_wait_calls;
		roots = begin_roots;
		MemoryContextReset(work_context);
		UT_ASSERT_EQ(cluster_wal_retained_cut_compute_v1(&self, &out, &detail, NULL),
			CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		UT_ASSERT_EQ(owner_wait_calls, waits);
		UT_ASSERT_EQ(begin_roots, roots);
		UT_ASSERT_EQ(retained_dirty_calls, 1);
		retained_cleanup();
	}
}

UT_TEST(retained_foreign_owner_cannot_consume_or_replace_old_debt)
{
	ClusterWalSourceRef self = retained_setup();
	ClusterWalRetainedCutV1 out;
	RfPageProofDetailV1 detail;
	ResourceOwner owner;
	unsigned roots;
	volatile bool caught = false;

	retained_defer_release = true;
	UT_ASSERT_EQ(cluster_wal_retained_cut_compute_v1(&self, &out, &detail, NULL),
		CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	owner = CurrentResourceOwner;
	roots = begin_roots;
	CurrentResourceOwner = (ResourceOwner)1;
	job_expect_fatal = true;
	PG_TRY(); { (void)cluster_wal_retained_cut_compute_v1(&self, &out, &detail, NULL); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	job_expect_fatal = false;
	CurrentResourceOwner = owner;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(begin_roots, roots);
	UT_ASSERT_EQ(retained_dirty_calls, 1);
	UT_ASSERT_EQ(pin_native_refs, 2);
	retained_cleanup();
}

UT_TEST(retained_pending_retirement_clears_lower_and_blocks_new_scans)
{
	ClusterWalSourceRef self = retained_setup();
	ClusterWalRetainedCutV1 out;
	RfPageProofDetailV1 detail;
	unsigned roots, waits;

	retained_defer_release = true;
	UT_ASSERT_EQ(cluster_wal_retained_cut_compute_v1(&self, &out, &detail, NULL),
		CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(out.lower, 0);
	UT_ASSERT(retained_inputs_job != NULL);
	UT_ASSERT_EQ(pin_native_refs, 2);
	MemoryContextReset(work_context);
	roots = begin_roots;
	waits = owner_wait_calls;
	UT_ASSERT_EQ(cluster_wal_retained_cut_compute_v1(&self, &out, &detail, NULL),
		CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(retained_dirty_calls, 1);
	UT_ASSERT_EQ(retained_scan_calls, 1);
	UT_ASSERT_EQ(begin_roots, roots);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_PENDING);
	retained_cleanup();
}

UT_TEST(retained_original_aux_parent_is_stable_and_role_bound)
{
	ClusterWalInputsJobV1 *job = NULL;
	MemoryContext parent;

	pin_begin();
	UT_ASSERT_EQ(cluster_wal_inputs_job_open_aux_v1(WAL_INPUT_JOB_RETAINED_CUT, &job),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	parent = MemoryContextGetParent(job->context);
	UT_ASSERT(parent != caller_context && parent != work_context);
	UT_ASSERT_EQ(sizeof(ClusterWalInputsJobControlBudgetV1), 2040);
	UT_ASSERT(MemoryContextGetParent(parent) == TopMemoryContext);
	UT_ASSERT(strcmp(parent->name, "Checkpointer WAL input jobs") == 0);
	foundation_finish(&job);
	UT_ASSERT_EQ(cluster_wal_inputs_job_open_aux_v1(WAL_INPUT_JOB_PI_BATCH, &job),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(MemoryContextGetParent(job->context) == parent);
	foundation_finish(&job);
	UT_ASSERT_EQ(cluster_wal_inputs_job_open_aux_v1(WAL_INPUT_JOB_PI_NOTICE, &job),
		CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(job == NULL);
	MemoryContextDelete(parent);
	inputs_jobs_process.caller_lifetime = NULL;
	foundation_end();
}

int
main(void)
{
	MyProc = &job_procs[42];
	MyProc->pgprocno = 42;
	MyLatch = &MyProc->procLatch;
	job_proc_global.allProcs = job_procs;
	job_proc_global.allProcCount = lengthof(job_procs);
	pg_atomic_init_u32(&MyProc->cluster_grd_registered_count, 0);
	pg_atomic_init_u64(&MyProc->cluster_grd_redeclare_acked, 0);
	pg_atomic_init_u64(&MyProc->cluster_grd_redeclare_acked_epoch, 0);
	MyProc->cluster_grd_generation = 1;
	cluster_shared_config = true;
	cluster_control_request_shmem_init();
	control_driver = retire_driver = cluster_control_request_driver_start();
	setvbuf(stdout, NULL, _IONBF, 0);
	UT_PLAN(9);
	UT_RUN(retained_original_aux_parent_is_stable_and_role_bound);
	UT_RUN(retained_actual_caller_publishes_only_after_synchronous_retirement);
	UT_RUN(retained_pending_retirement_clears_lower_and_blocks_new_scans);
	UT_RUN(retained_exact_ack_completion_allows_a_fresh_scope);
	UT_RUN(retained_business_error_is_not_replaced_by_cleanup_wait);
	UT_RUN(retained_census_error_revokes_without_wait_and_survives_work_reset);
	UT_RUN(retained_each_cf_phase_keeps_debt_and_blocks_new_root_reads);
	UT_RUN(retained_foreign_owner_cannot_consume_or_replace_old_debt);
	UT_RUN(retained_interrupt_in_first_retirement_rethrows_without_published_lower);
	UT_DONE();
	return ut_failed_count != 0;
}
