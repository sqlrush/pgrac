/* Original Startup barrier body; lock/participant leaves are controlled.
 * This tests order and failure custody, not native file/ROOT qualification. */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"
#include "access/xlogreader.h"
#include "access/xlogrecovery.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_cold_recovery_plan_internal.h"
#include "cluster/cluster_cold_relmap.h"
#include "cluster/cluster_control_relmap_recovery.h"
#include "cluster/cluster_relmap_control.h"
#include "cluster/cluster_relmap_lock.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_recovery_merge.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
void pg_re_throw(void) { siglongjmp(*PG_exception_stack, 1); }
void ExceptionalCondition(const char *condition, const char *file, int line)
{ fprintf(stderr, "%s %s:%d\n", condition, file, line); abort(); }

static bool serial_held, x_held, cf_held, failed;
static unsigned fault, applied, checked;
static char events[64];
static unsigned event_count;
static uint8 fence_storage;
static ClusterRecoveryFencePlan *fence = (void *)&fence_storage;
static void event(char c) { events[event_count++] = c; events[event_count] = 0; }
bool cluster_recovery_merge_fence_plan_deadline_v1(const ClusterRecoveryFencePlan *p, uint64 *out)
{ UT_ASSERT(p == fence); *out = 123; return true; }
bool cluster_recovery_merge_fence_plan_suspend_serial_v1(ClusterRecoveryFencePlan *p, uint64 *id)
{
	UT_ASSERT(p == fence && serial_held && !cf_held && !x_held && !*id);
	event('S'); if (fault == 1) return false;
	serial_held = false; *id = 73; return true;
}
bool cluster_recovery_merge_fence_plan_resume_serial_v1(ClusterRecoveryFencePlan *p, uint64 id)
{
	UT_ASSERT(p == fence && id == 73 && !serial_held && !cf_held && !x_held);
	event('R'); if (fault == 7) return false;
	serial_held = true; return true;
}
bool cluster_relmap_authority_x_held(void) { return x_held; }
bool cluster_relmap_authority_x_lock(void)
{
	UT_ASSERT(!serial_held && !x_held && !cf_held); event('X'); x_held = fault != 2;
	if (fault == 9) pg_re_throw();
	return x_held;
}
void cluster_relmap_authority_x_unlock(void)
{ UT_ASSERT(!serial_held && x_held && !cf_held); event('x'); x_held = false; }
bool cluster_cf_held(LOCKMODE mode) { (void)mode; return cf_held; }
bool cluster_cf_held_is_clusterwide(LOCKMODE mode) { return mode == ExclusiveLock && cf_held; }
bool cluster_cf_lock_deadline_v1(LOCKMODE mode, uint64 deadline)
{
	UT_ASSERT(mode == ExclusiveLock && deadline == 123 && !serial_held && x_held && !cf_held);
	event('C'); cf_held = fault != 3;
	if (fault == 10) pg_re_throw();
	return cf_held;
}
ClusterCfReleaseResult cluster_cf_unlock_confirmed(LOCKMODE mode)
{
	UT_ASSERT(mode == ExclusiveLock && !serial_held && x_held);
	event('c'); if (fault == 6) return CLUSTER_CF_RELEASE_UNCONFIRMED;
	if (!cf_held) return CLUSTER_CF_RELEASE_NOT_HELD;
	cf_held = false; return CLUSTER_CF_RELEASE_CONFIRMED;
}
bool cluster_cold_typed_relmap_create_barrier_current_v2(ClusterColdTypedV1 *t,
	ClusterRecoveryFencePlan *p, uint64 id)
{ (void)t; UT_ASSERT(p == fence && id == 73 && !serial_held && cf_held && x_held); event('V'); return fault != 4; }
bool cluster_cold_typed_relmap_create_apply_barrier_v2(ClusterColdTypedV1 *t,
	ClusterRecoveryFencePlan *p, uint64 id, const ClusterColdStepV1 *s, XLogReaderState *r, Size budget)
{
	(void)t; UT_ASSERT(p == fence && id == 73 && s && r && budget && !serial_held && cf_held && x_held);
	event('A'); ++applied;
	if (fault == 8) pg_re_throw();
	return fault != 5;
}
bool cluster_cold_typed_relmap_create_ready_barrier_v2(ClusterColdTypedV1 *t,
	ClusterRecoveryFencePlan *p, uint64 id)
{ (void)t; UT_ASSERT(p == fence && id == 73 && !serial_held && cf_held && x_held); event('Q'); ++checked; return fault != 5; }
void cluster_cold_typed_relmap_create_failed_v2(ClusterColdTypedV1 *t)
{ failed = true; t->refusal = CLUSTER_COLD_SOURCE_GAP; }

static uint8 start_storage;
static const struct ClusterRecoveryStartV1 *start = (void *)&start_storage;
static uint32 terminal_total, terminal_fault, terminal_reads;
static bool terminal_changed;
bool ClusterColdRecoveryRelmapCurrent(const struct ClusterRecoveryStartV1 *s)
{ return s == start; }
bool cluster_cold_relmap_pending_terminal_recheck_segment_v1(
	const struct ClusterRecoveryStartV1 *s, uint32 offset, ClusterColdRelmapTerminalCheckV1 *out)
{
	UT_ASSERT(s == start && offset < terminal_total);
	++terminal_reads; memset(out, 0, sizeof(*out));
	out->offset = offset; out->total_count = terminal_total;
	out->count = Min(terminal_total - offset, CLUSTER_COLD_RELMAP_MAX_INPUTS);
	out->digest[0] = 1;
	if (terminal_fault == 1 && offset) out->digest[0]++;
	if (terminal_fault == 2 && offset) out->total_count++;
	if (terminal_fault == 3 && offset) out->offset--;
	if (terminal_fault == 4 && offset) out->count--;
	if (terminal_fault == 5 && offset) return false;
	for (uint32 i = 0; i < out->count; ++i)
		out->entries[i].top_full_xid = 1000 + offset + i;
	if (terminal_changed && offset == 64) out->entries[0].top_full_xid++;
	return true;
}

#include "test_cluster_startup_create_barrier_body.inc"

/* Original fatal-cleanup body; only the external owner/fatal services are
 * leaves. Unconfirmed ownership must keep every borrowed receipt alive. */
static unsigned fail_cleanup_fault, typed_destroyed, fence_destroyed;
static ClusterColdTypedV1 *fail_typed;
static ClusterRecoveryFencePlan *fail_external;
bool cluster_recovery_merge_fence_plan_release_serial(ClusterRecoveryFencePlan *p)
{
	UT_ASSERT(p == fence);
	if (fail_cleanup_fault == 3) pg_re_throw();
	return fail_cleanup_fault != 1;
}
void cluster_recovery_merge_fence_plan_destroy(ClusterRecoveryFencePlan **p)
{
	UT_ASSERT(p && *p == fence);
	++fence_destroyed;
	if (fail_cleanup_fault != 2) *p = NULL;
}
void cluster_cold_typed_destroy_v1(ClusterColdTypedV1 **typed)
{ ++typed_destroyed; *typed = NULL; }
const char *cluster_cold_refusal_hint_v1(ClusterColdDetailV1 detail)
{ (void)detail; return "original refusal"; }
bool errstart(int level, const char *domain)
{ (void)domain; UT_ASSERT_EQ(level, FATAL); return true; }
bool errstart_cold(int level, const char *domain) { return errstart(level, domain); }
void errfinish(const char *file, int line, const char *function)
{ (void)file; (void)line; (void)function; pg_re_throw(); }
int errcode(int code) { (void)code; return 0; }
int errmsg(const char *format, ...) { (void)format; return 0; }
int errdetail(const char *format, ...) { (void)format; return 0; }
int errdetail_log(const char *format, ...) { (void)format; return 0; }
int errhint(const char *format, ...) { (void)format; return 0; }
#define pstrdup strdup
#include "test_cluster_startup_create_fail_body.inc"
#undef pstrdup

static ClusterColdTypedReplay setup(ClusterColdTypedV1 *typed, ClusterColdPlanV1 *plan)
{
	memset(typed, 0, sizeof(*typed)); memset(plan, 0, sizeof(*plan));
	typed->plan = plan; plan->relmap_create_count = 1;
	plan->memory_budget = 65536; plan->memory_used = 4096;
	serial_held = true; x_held = cf_held = failed = false;
	fault = applied = checked = event_count = 0; events[0] = 0;
	return (ClusterColdTypedReplay){.typed = typed, .fence_plan = &fence};
}
UT_TEST(original_barrier_releases_ir_before_x_cf_and_resumes_after_release)
{
	ClusterColdTypedV1 typed; ClusterColdPlanV1 plan; ClusterColdStepV1 step = {0};
	XLogReaderState reader = {0}; ClusterColdTypedReplay r = setup(&typed, &plan);
	UT_ASSERT(cluster_typed_replay_create_barrier(&r, &step, &reader));
	UT_ASSERT(!strcmp(events, "SXCVAVcxR"));
	UT_ASSERT(serial_held && !x_held && !cf_held && !failed && applied == 1);
	events[event_count = 0] = 0;
	UT_ASSERT(cluster_typed_replay_create_barrier(&r, NULL, NULL));
	UT_ASSERT(!strcmp(events, "SXCVQVcxR"));
	UT_ASSERT_EQ(applied, 1); UT_ASSERT_EQ(checked, 1);
}
UT_TEST(any_barrier_failure_revokes_without_resuming_or_reexecution)
{
	for (unsigned f = 1; f <= 7; ++f) {
		ClusterColdTypedV1 typed; ClusterColdPlanV1 plan; ClusterColdStepV1 step = {0};
		XLogReaderState reader = {0}; ClusterColdTypedReplay r = setup(&typed, &plan);
		unsigned once;
		fault = f;
		UT_ASSERT(!cluster_typed_replay_create_barrier(&r, &step, &reader));
		UT_ASSERT(failed && typed.refusal != CLUSTER_COLD_OK);
		UT_ASSERT(f == 7 || !strchr(events, 'R'));
		if (f == 6) UT_ASSERT(cf_held && x_held);
		once = applied;
		UT_ASSERT(!cluster_typed_replay_create_barrier(&r, &step, &reader));
		UT_ASSERT_EQ(applied, once);
	}
}
UT_TEST(original_barrier_error_preserves_attempt_and_never_resumes)
{
	ClusterColdTypedV1 typed; ClusterColdPlanV1 plan; ClusterColdStepV1 step = {0};
	XLogReaderState reader = {0}; ClusterColdTypedReplay r = setup(&typed, &plan);
	volatile bool caught = false; fault = 8;
	PG_TRY(); { (void)cluster_typed_replay_create_barrier(&r, &step, &reader); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught && failed && !strchr(events, 'R'));
	UT_ASSERT_EQ(applied, 1);
	UT_ASSERT(!cluster_typed_replay_create_barrier(&r, &step, &reader));
	UT_ASSERT_EQ(applied, 1);
}
UT_TEST(owner_graph_checks_every_terminal_page_before_and_after)
{
	for (uint32 count = 65; count <= 129; count += 64) {
		ClusterColdRelmapTerminalCheckV1 *pages = calloc(4, sizeof(*pages));
		terminal_total = count; terminal_fault = terminal_reads = 0; terminal_changed = false;
		UT_ASSERT(cluster_recovery_cold_relmap_terminals(start, count, pages, pages + 3, true));
		UT_ASSERT(cluster_recovery_cold_relmap_terminals(start, count, pages, pages + 3, false));
		UT_ASSERT_EQ(terminal_reads, 2 * ((count + 63) / 64));
		terminal_changed = true;
		UT_ASSERT(!cluster_recovery_cold_relmap_terminals(start, count, pages, pages + 3, false));
		free(pages);
	}
}
UT_TEST(barrier_error_during_acquisition_cleans_only_actual_owned_holds)
{
	for (unsigned f = 9; f <= 10; ++f) {
		ClusterColdTypedV1 typed; ClusterColdPlanV1 plan; ClusterColdStepV1 step = {0};
		XLogReaderState reader = {0}; ClusterColdTypedReplay r = setup(&typed, &plan);
		volatile bool caught = false;
		fault = f;
		PG_TRY(); { (void)cluster_typed_replay_create_barrier(&r, &step, &reader); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(caught && failed && !serial_held && !strchr(events, 'R'));
		UT_ASSERT(!x_held && !cf_held);
		UT_ASSERT_EQ(applied, 0);
	}
}
UT_TEST(owner_graph_refuses_missing_or_inconsistent_whole_batch_pages)
{
	for (unsigned f = 1; f <= 5; ++f) {
		ClusterColdRelmapTerminalCheckV1 *pages = calloc(4, sizeof(*pages));
		terminal_total = 129; terminal_fault = f; terminal_reads = 0; terminal_changed = false;
		UT_ASSERT(!cluster_recovery_cold_relmap_terminals(start, 129, pages, pages + 3, true));
		free(pages);
	}
}
UT_TEST(fatal_cleanup_keeps_receipts_until_external_fence_is_destroyed)
{
	for (unsigned f = 0; f < 4; ++f) {
		ClusterColdTypedV1 storage = {0};
		volatile bool caught = false;
		fail_typed = &storage; fail_external = fence;
		fail_cleanup_fault = f; typed_destroyed = fence_destroyed = 0;
		PG_TRY(); { cluster_recovery_typed_fail(&fail_external, &fail_typed, "refused", NULL); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(typed_destroyed, f == 0 ? 1 : 0);
		UT_ASSERT_EQ(fail_typed == NULL, f == 0);
		UT_ASSERT_EQ(fail_external == NULL, f == 0);
		UT_ASSERT_EQ(fence_destroyed, f == 0 || f == 2 ? 1 : 0);
	}
}
int main(void)
{
	UT_PLAN(7);
	UT_RUN(original_barrier_releases_ir_before_x_cf_and_resumes_after_release);
	UT_RUN(any_barrier_failure_revokes_without_resuming_or_reexecution);
	UT_RUN(original_barrier_error_preserves_attempt_and_never_resumes);
	UT_RUN(owner_graph_checks_every_terminal_page_before_and_after);
	UT_RUN(owner_graph_refuses_missing_or_inconsistent_whole_batch_pages);
	UT_RUN(barrier_error_during_acquisition_cleans_only_actual_owned_holds);
	UT_RUN(fatal_cleanup_keeps_receipts_until_external_fence_is_destroyed);
	UT_DONE(); return ut_failed_count != 0;
}
