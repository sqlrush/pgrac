/* PGRAC: actual cold-merge admission body; CF/native source owners and the
 * crash census are fixture boundaries. No physical redo or live crash test.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <setjmp.h>
#include <stdarg.h>
#include "access/xlog.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_recovery_plan.h"
#include "cluster/cluster_recovery_worker.h"
#include "cluster/cluster_wal_source.h"
#include "cluster/cluster_wal_tail.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "lib/stringinfo.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_merged_recovery = true, cluster_shared_config = true, fullPageWrites = true;
char *cluster_wal_threads_dir = "/fixture/wal", *cluster_shared_data_dir = "/fixture/data";
int cluster_shared_storage_backend = CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS;
static ClusterRecoveryPlan census;
static ClusterControlRootSnapshot roots[5];
static ClusterControlRootReadToken tokens[5];
static XLogRecPtr redo[5];
static ClusterControlRootResult source_result;
static int source_calls;
static jmp_buf fatal_jump;
static char details[4096];

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s %s:%d\n", condition, file, line);
	abort();
}
void
initStringInfo(StringInfo s)
{
	memset(s, 0, sizeof(*s));
	s->data = details;
	s->maxlen = sizeof(details);
	details[0] = 0;
}
void
appendStringInfo(StringInfo s, const char *fmt, ...)
{
	va_list args;
	int n;
	va_start(args, fmt);
	n = vsnprintf(s->data + s->len, s->maxlen - s->len, fmt, args);
	va_end(args);
	if (n < 0 || n >= s->maxlen - s->len)
		abort();
	s->len += n;
}
void
pfree(void *p)
{
	UT_ASSERT(p == details);
}
bool
cluster_recovery_plan_snapshot(ClusterRecoveryPlan *out)
{
	*out = census;
	return true;
}
bool
cluster_recovery_worker_pool_snapshot(ClusterRecoveryWorkerPool *out)
{
	(void)out;
	return false;
}
ClusterRecoveryStreamVerdict
cluster_recovery_worker_revalidate(uint16 thread)
{
	UT_ASSERT(thread > 1 && thread <= 4);
	return CLUSTER_RECOVERY_STREAM_OK;
}
bool
cluster_shared_fs_sentinel_has_participant(int node)
{
	return node >= 0 && node < 4;
}
ClusterRecoveryDutyCompare
cluster_recovery_duty_key_compare(const ClusterRecoveryDutyKey *a, const ClusterRecoveryDutyKey *b)
{
	return memcmp(a, b, sizeof(*a)) == 0 ? CLUSTER_RECOVERY_DUTY_COMPARE_EXACT
										 : CLUSTER_RECOVERY_DUTY_COMPARE_DIFFERENT;
}
ClusterControlRootResult
cluster_control_root_lookup_owner_by_node_runtime(int32 node, ClusterControlRootIdentity *id,
												  ClusterControlRootSnapshot *root,
												  ClusterControlRootReadToken *token)
{
	UT_ASSERT(node >= 0 && node < 4);
	*root = roots[node + 1];
	*id = root->identity;
	*token = tokens[node + 1];
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
ClusterControlRootResult
cluster_control_root_recovery_source_v1(const ClusterControlRootSnapshot *root,
										const ClusterControlRootReadToken *token,
										ClusterWalSourceRef *source, XLogRecPtr *native_redo)
{
	uint16 tid = root->identity.origin_thread_id;
	source_calls++;
	UT_ASSERT_EQ(memcmp(root, &roots[tid], sizeof(*root)), 0);
	UT_ASSERT_EQ(memcmp(token, &tokens[tid], sizeof(*token)), 0);
	memset(source, 0, sizeof(*source));
	source->claim.identity = root->identity;
	source->timeline = root->checkpoint_tli;
	*native_redo = redo[tid];
	return source_result;
}

#undef ereport
#define ereport(level, rest) longjmp(fatal_jump, 1)
#include "test_cluster_recovery_cold_cut.inc"

static void
fixture(void)
{
	memset(&census, 0, sizeof(census));
	census.generated = true;
	census.own_thread = 1;
	census.n_crashed_candidate = 3;
	census.candidate_bitmap[0] = 14; /* all four instances crashed */
	census.dbstate_at_startup = DB_IN_PRODUCTION;
	census.local_recovery_needed = true;
	memset(roots, 0, sizeof(roots));
	memset(tokens, 0, sizeof(tokens));
	for (uint16 tid = 1; tid <= 4; tid++) {
		ClusterControlRootIdentity *id = &roots[tid].identity;
		ClusterWalThreadClaim claim;
		id->system_identifier = 9;
		id->storage_uuid[0] = 1;
		id->authority_uuid[6] = 0x40;
		id->authority_uuid[8] = 0x80;
		id->origin_thread_id = tid;
		id->origin_node_id = tid - 1;
		id->thread_claim_created_at = 42;
		id->origin_owner_incarnation = 11;
		id->root_lineage_seq = 3;
		cluster_wal_thread_claim_fill(&claim, tid, tid - 1, 42);
		id->thread_claim_crc32c = claim.crc;
		roots[tid].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
		roots[tid].checkpoint_tli = roots[tid].tail_tli = 1;
		roots[tid].checkpoint_lower_lsn = redo[tid] = 0x800;
		roots[tid].validated_tail_lsn_exclusive = 0x1000;
		tokens[tid].origin_thread_id = tid;
		tokens[tid].file_txn_seq = 77;
	}
	source_calls = 0;
	source_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	cluster_shared_config = true;
}

/* Retained history before the native redo is ancestry for the typed cold
 * plan: the projection engages from the same-token native redo and never
 * hands the physical lower to replay as a redo start. */
UT_TEST(all_crashed_retained_history_engages_from_native_redo)
{
	uint64 bitmap[2];
	XLogRecPtr starts[CLUSTER_WAL_STATE_SLOT_COUNT + 1] = { 0 };
	fixture();
	roots[3].checkpoint_lower_lsn = 0x100; /* old peer FPI precedes real redo */
	if (setjmp(fatal_jump) != 0) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT_EQ(cluster_recovery_merge_project_readonly(1, 0x900, bitmap, starts),
				 CLUSTER_MERGE_ENGAGE);
	UT_ASSERT_EQ(starts[3], 0x800);
	UT_ASSERT_EQ(source_calls, 3);
}

UT_TEST(native_redo_outside_retained_cut_refused)
{
	uint64 bitmap[2];
	XLogRecPtr starts[CLUSTER_WAL_STATE_SLOT_COUNT + 1] = { 0 };
	fixture();
	roots[2].checkpoint_lower_lsn = 0x900; /* lower beyond the native redo */
	if (setjmp(fatal_jump) == 0) {
		(void)cluster_recovery_merge_project_readonly(1, 0x900, bitmap, starts);
		UT_ASSERT(false);
	}
	UT_ASSERT(strstr(details, "native checkpoint") != NULL);
	UT_ASSERT_EQ(starts[2], 0);
	fixture();
	redo[4] = 0x1800; /* native redo beyond the validated tail */
	if (setjmp(fatal_jump) == 0) {
		(void)cluster_recovery_merge_project_readonly(1, 0x900, bitmap, starts);
		UT_ASSERT(false);
	}
	UT_ASSERT(strstr(details, "native checkpoint") != NULL);
}

UT_TEST(all_crashed_equal_native_cuts_preserve_original_gate)
{
	uint64 bitmap[2];
	XLogRecPtr starts[CLUSTER_WAL_STATE_SLOT_COUNT + 1] = { 0 };
	fixture();
	if (setjmp(fatal_jump) != 0) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT_EQ(cluster_recovery_merge_project_readonly(1, 0x900, bitmap, starts),
				 CLUSTER_MERGE_ENGAGE);
	UT_ASSERT_EQ(bitmap[0], 15);
	UT_ASSERT_EQ(starts[1], 0x900);
	UT_ASSERT_EQ(starts[3], 0x800);
	UT_ASSERT_EQ(source_calls, 3);
}

UT_TEST(unproven_native_anchor_cannot_fall_back_to_lower)
{
	uint64 bitmap[2];
	XLogRecPtr starts[CLUSTER_WAL_STATE_SLOT_COUNT + 1] = { 0 };
	fixture();
	source_result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (setjmp(fatal_jump) == 0) {
		(void)cluster_recovery_merge_project_readonly(1, 0x900, bitmap, starts);
		UT_ASSERT(false);
	}
	UT_ASSERT(strstr(details, "native checkpoint") != NULL);
	UT_ASSERT_EQ(starts[2], 0);
}

UT_TEST(warm_and_legacy_paths_do_not_borrow_canonical_authority)
{
	uint64 bitmap[2];
	XLogRecPtr starts[CLUSTER_WAL_STATE_SLOT_COUNT + 1] = { 0 };
	fixture();
	if (setjmp(fatal_jump) != 0) {
		UT_ASSERT(false);
		return;
	}
	census.n_alive = 1;
	UT_ASSERT_EQ(cluster_recovery_merge_project_readonly(1, 0x900, bitmap, starts),
				 CLUSTER_MERGE_NO_NOT_COLD);
	UT_ASSERT_EQ(source_calls, 0);
	census.n_alive = 0;
	cluster_shared_config = false;
	UT_ASSERT_EQ(cluster_recovery_merge_project_readonly(1, 0x900, bitmap, starts),
				 CLUSTER_MERGE_ENGAGE);
	UT_ASSERT_EQ(source_calls, 0);
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(all_crashed_retained_history_engages_from_native_redo);
	UT_RUN(native_redo_outside_retained_cut_refused);
	UT_RUN(all_crashed_equal_native_cuts_preserve_original_gate);
	UT_RUN(unproven_native_anchor_cannot_fall_back_to_lower);
	UT_RUN(warm_and_legacy_paths_do_not_borrow_canonical_authority);
	UT_DONE();
	return ut_failed_count != 0;
}
