/* Original positive merge projection, canonical reader and CF/CONTROL.
 * Plan publication, stream validation, reporting and same-token WAL source
 * are leaves.
 * No automatic retry or Startup continuation is implemented by this fixture.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "lib/stringinfo.h"
#include "cluster/cluster_recovery_worker.h"
#include "cluster/storage/cluster_shared_fs.h"

bool cluster_merged_recovery = true;
bool fullPageWrites = true;
int cluster_shared_storage_backend = CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS;
char *cluster_shared_data_dir = "projection-fixture";
static ClusterRecoveryPlan project_census;
static unsigned project_sources, project_validations;

static void
project_blockers_init(StringInfo out)
{
	out->maxlen = 2048;
	out->data = palloc0(out->maxlen);
	out->len = out->cursor = 0;
}

static void project_blockers_append(StringInfo out, const char *format, ...)
	pg_attribute_printf(2, 3);
static void
project_blockers_append(StringInfo out, const char *format, ...)
{
	va_list args;
	int remaining = out->maxlen - out->len;
	int written;

	va_start(args, format);
	written = vsnprintf(out->data + out->len, remaining, format, args);
	va_end(args);
	UT_ASSERT(written >= 0 && written < remaining);
	if (written >= 0 && written < remaining)
		out->len += written;
}

static bool
project_snapshot(ClusterRecoveryPlan *out)
{
	*out = project_census;
	return project_census.generated;
}

static bool
project_pool(ClusterRecoveryWorkerPool *out)
{
	memset(out, 0, sizeof(*out));
	return false;
}

static ClusterRecoveryStreamVerdict
project_stream(uint16 thread)
{
	UT_ASSERT_EQ(thread, 2);
	++project_validations;
	return CLUSTER_RECOVERY_STREAM_OK;
}

static bool
project_participant(int node)
{
	UT_ASSERT_EQ(node, 1);
	return true;
}

static ClusterControlRootResult
project_source(const ClusterControlRootSnapshot *root, const ClusterControlRootReadToken *token,
			   ClusterWalSourceRef *out, XLogRecPtr *redo)
{
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	UT_ASSERT_EQ(root->identity.origin_thread_id, 2);
	UT_ASSERT_EQ(root->root_publish_seq, token->root_publish_seq);
	++project_sources;
	memset(out, 0, sizeof(*out));
	*redo = 150;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

#define cluster_recovery_plan_snapshot project_snapshot
#define cluster_recovery_worker_pool_snapshot project_pool
#define cluster_recovery_worker_revalidate project_stream
#define cluster_shared_fs_sentinel_has_participant project_participant
#define cluster_control_root_recovery_source_v1 project_source
#define cluster_recovery_duty_key_compare_for_claim rejoin_key_compare_for_claim
#define initStringInfo project_blockers_init
#define appendStringInfo project_blockers_append
#include "test_cluster_merge_project_cf.inc"
#undef appendStringInfo
#undef initStringInfo
#undef cluster_recovery_duty_key_compare_for_claim
#undef cluster_control_root_recovery_source_v1
#undef cluster_shared_fs_sentinel_has_participant
#undef cluster_recovery_worker_revalidate
#undef cluster_recovery_worker_pool_snapshot
#undef cluster_recovery_plan_snapshot

static void
project_fixture(void)
{
	ir_cf_fixture(CLUSTER_RECOVERY_SERIAL_COLD_FORMED, 0);
	memset(&project_census, 0, sizeof(project_census));
	project_census.generated = true;
	project_census.own_thread = 1;
	project_census.n_crashed_candidate = 1;
	cluster_recovery_plan_candidate_set(&project_census, 2);
	project_sources = project_validations = 0;
	for (unsigned i = 0; i < 2; ++i) {
		retention_image.records[i].checkpoint_lower_lsn = 100;
		retention_image.records[i].validated_tail_lsn_exclusive = 300;
		retention_image.records[i].tail_tli = 1;
	}
	entry_on_wait = NULL;
}

static bool
project_call(ClusterMergeEngage *engage, uint64 replay[2], XLogRecPtr *start)
{
	volatile bool completed = false;

	PG_TRY();
	{
		*engage = cluster_recovery_merge_project_readonly(1, 90, replay, start);
		completed = true;
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
	return completed;
}

UT_TEST(startup_projection_uses_the_original_canonical_cut)
{
	ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;
	uint64 replay[2] = { 0, 0 };
	XLogRecPtr start[CLUSTER_WAL_STATE_SLOT_COUNT + 1] = { 0 };

	project_fixture();
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT(project_call(&engage, replay, start));
	UT_ASSERT_EQ(engage, CLUSTER_MERGE_ENGAGE);
	UT_ASSERT_EQ(replay[0], 3);
	UT_ASSERT_EQ(replay[1], 0);
	UT_ASSERT_EQ(start[1], 90);
	UT_ASSERT_EQ(start[2], 150);
	UT_ASSERT_EQ(retention_controls, 1);
	UT_ASSERT_EQ(retention_threads, 1);
	UT_ASSERT_EQ(project_sources, 1);
	UT_ASSERT_EQ(project_validations, 1);
	UT_ASSERT_EQ(ir_cf_held_count(), 0);
	UT_ASSERT(cluster_control_request_empty());
	ir_cf_cleanup();
}

UT_TEST(startup_projection_cf_slice_keeps_exact_cleanup_without_reading_roots)
{
	ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;
	uint64 replay[2] = { 0, 0 };
	XLogRecPtr start[CLUSTER_WAL_STATE_SLOT_COUNT + 1] = { 0 };
	ClusterCfRetirementSnapshot pending = { 0 };
	ClusterCfAttemptObservation attempt = { 0 };
	bool completed;

	project_fixture();
	g_seven_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	completed = project_call(&engage, replay, start);
	UT_ASSERT(!completed);
	UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	UT_ASSERT(cluster_cf_last_attempt_since(0, &attempt));
	UT_ASSERT(cluster_cf_attempt_wait_slice(&attempt));
	UT_ASSERT_EQ(attempt.mode, ShareLock);
	UT_ASSERT(cluster_cf_retirement_snapshot(ShareLock, NULL, &pending));
	UT_ASSERT_EQ(pending.cookie, attempt.control_owner_id);
	UT_ASSERT_EQ(pending.registry.message.key.holder.request_id, attempt.request_id);
	UT_ASSERT_EQ(pending.registry.message.key.holder.cluster_epoch, attempt.request_epoch);
	UT_ASSERT_EQ(ir_cf_held_count(), 0);
	UT_ASSERT_EQ(entry_waits, 0);
	UT_ASSERT_EQ(retention_attempts, 1);
	UT_ASSERT_EQ(retention_controls | retention_threads | project_sources, 0);
	UT_ASSERT_EQ(start[2], 0);
	ir_cf_trace("projection-before-IR", pending.cookie, &pending.registry);
	printf("# projection completed=%d sqlstate=%d attempt_result=%d waits=%u reads=%u IR=%u\n",
		   completed, entry_code, (int)attempt.result, entry_waits,
		   retention_controls + retention_threads, ir_cf_held_count());
	/* This opt-in assertion is deliberately RED until the original caller
	 * owns a frozen continuation. Safety characterization is not liveness. */
	if (getenv("PGRAC_PRE2_TEST_PROJECT_CF_LIVENESS") != NULL)
		UT_ASSERT(completed && engage == CLUSTER_MERGE_ENGAGE);
	ir_cf_cleanup();
}

UT_TEST(startup_projection_under_ir_does_not_adopt_another_wait_policy)
{
	ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;
	uint64 replay[2] = { 0, 0 };
	XLogRecPtr start[CLUSTER_WAL_STATE_SLOT_COUNT + 1] = { 0 };
	ClusterCfRetirementSnapshot pending = { 0 };
	ClusterRecoverySerialGuard before;
	unsigned waits;

	project_fixture();
	UT_ASSERT_EQ(cluster_recovery_serial_acquire(&ir_cf_requests[0], &ir_cf_guard),
				 CLUSTER_RECOVERY_SERIAL_GRANTED);
	UT_ASSERT_EQ(ir_cf_held_count(), 1);
	before = ir_cf_guard;
	waits = entry_waits;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	UT_ASSERT(!project_call(&engage, replay, start));
	UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(entry_waits, waits);
	UT_ASSERT_EQ(ir_cf_held_count(), 1);
	UT_ASSERT_EQ(memcmp(&before, &ir_cf_guard, sizeof(before)), 0);
	UT_ASSERT(cluster_cf_retirement_snapshot(ShareLock, NULL, &pending));
	UT_ASSERT_EQ(retention_attempts, 2);
	UT_ASSERT_EQ(retention_controls, 0);
	UT_ASSERT_EQ(retention_threads, 1); /* Original IR preflight only. */
	UT_ASSERT_EQ(project_sources, 0);
	ir_cf_trace("projection-with-IR", pending.cookie, &pending.registry);
	ir_cf_cleanup();
}

UT_TEST(startup_projection_preserves_real_root_and_existing_slot_refusals)
{
	for (unsigned existing = 0; existing < 2; ++existing) {
		ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;
		uint64 replay[2] = { 0, 0 };
		XLogRecPtr start[CLUSTER_WAL_STATE_SLOT_COUNT + 1] = { 0 };
		unsigned attempts;

		project_fixture();
		if (existing) {
			(void)entry_timeout(ShareLock);
			attempts = retention_attempts;
		} else {
			g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
			retention_root_result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
			attempts = retention_attempts + 1;
		}
		UT_ASSERT(!project_call(&engage, replay, start));
		UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(entry_waits, 0);
		UT_ASSERT_EQ(retention_attempts, attempts);
		UT_ASSERT_EQ(project_sources, 0);
		UT_ASSERT_EQ(ir_cf_held_count(), 0);
		UT_ASSERT_EQ(cluster_cf_held(ShareLock), existing != 0);
		ir_cf_cleanup();
	}
}
