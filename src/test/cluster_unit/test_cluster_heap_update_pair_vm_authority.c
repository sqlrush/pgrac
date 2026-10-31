/*-------------------------------------------------------------------------
 * test_cluster_heap_update_pair_vm_authority.c
 *    Actual UPDATE pair bracket retains original VM writers across reentry.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Transport, exact writer/KO validation, target selection and MX planning are
 * controlled leaves. The complete VM bracket, proof helpers and heap repin
 * execute; this is not a full UPDATE/ResourceOwner or distributed ACK test.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#ifdef FIXTURE_NO_ASSERTS
#undef USE_ASSERT_CHECKING
#undef Assert
#define Assert(condition) ((void)0)
#undef AssertMacro
#define AssertMacro(condition) ((void)0)
#undef PG_USED_FOR_ASSERTS_ONLY
#define PG_USED_FOR_ASSERTS_ONLY
#endif
#include "access/heapam.h"
#include "access/visibilitymap.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_semantic_activation.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"
#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

typedef int ClusterCurrentMxStampPlan;
static PGAlignedBlock pages[4];
static BufferTag tags[4];
static int pins[4];
static bool held[4];
static RelationData fixture_relation;
static uint64 revision[2], generation;
static Buffer old_map, new_map;
static bool shared, same_heap, reverse_order, omit_old, omit_new;
static unsigned nested, warms, captures, resumes, writes, entries, posts, reentries;
static unsigned cancels, frees, checks, l2_visits, reacquire_visits, starts;
static ClusterCurrentMxStampPlan *old_plan_address, *new_plan_address;
static bool old_plan_cancelled, new_plan_cancelled;
static unsigned order_count, order[128];
static unsigned refuse_warm, refuse_resume, resume_error, capture_error;
static Buffer refuse_target;
static bool replace_alias, missing_writer, rotate_other, clear_hint, cancel_after_warm;
static bool do_repick, repicked, temp_lock;
static bool seen_cursor, fail_history;
static unsigned warm_error;
volatile sig_atomic_t InterruptPending;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
int NBuffers = 4, NLocBuffer;
char *BufferBlocks = (char *)pages;
Block *LocalBufferBlockPointers;

bool errstart(int level, const char *domain) { return level >= ERROR; }
bool errstart_cold(int level, const char *domain) { return errstart(level, domain); }
int errcode(int code) { return 0; }
int errmsg(const char *fmt, ...) { return 0; }
int errmsg_internal(const char *fmt, ...) { return 0; }
int errdetail(const char *fmt, ...) { return 0; }
void
errfinish(const char *file, int line, const char *function)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
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
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "assertion %s at %s:%d\n", condition, file, line);
	abort();
}

static bool
any_content(void)
{
	return held[0] || held[1] || held[2] || held[3];
}

void
LockBuffer(Buffer buffer, int mode)
{
	UT_ASSERT(buffer >= 1 && buffer <= 4 && pins[buffer - 1] > 0);
	UT_ASSERT_EQ(held[buffer - 1], mode == BUFFER_LOCK_UNLOCK);
	if (mode == BUFFER_LOCK_EXCLUSIVE && buffer >= 3)
		UT_ASSERT_EQ(pins[0] + pins[1], 0);
	held[buffer - 1] = mode != BUFFER_LOCK_UNLOCK;
}
void
ReleaseBuffer(Buffer buffer)
{
	UT_ASSERT(buffer >= 1 && buffer <= 4 && pins[buffer - 1] > 0);
	UT_ASSERT(!held[buffer - 1]);
	pins[buffer - 1]--;
}
void
BufferGetTag(Buffer buffer, RelFileLocator *locator, ForkNumber *fork, BlockNumber *block)
{
	UT_ASSERT(buffer >= 1 && buffer <= 4 && pins[buffer - 1] > 0);
	*locator = BufTagGetRelFileLocator(&tags[buffer - 1]);
	*fork = tags[buffer - 1].forkNum;
	*block = tags[buffer - 1].blockNum;
}

bool
ClusterLockBufferExclusiveAuxiliaryAliasAware(Buffer *buffer, Buffer *alias,
	const ClusterBufferBarrierSiteId *site, ResourceXAuxiliaryAcquireContext *context)
{
	Buffer target = *buffer;
	bool equal_alias = alias != NULL && alias != buffer && *alias == target;

	UT_ASSERT(target >= 1 && target <= 2 && !held[target - 1]);
	UT_ASSERT_EQ(pins[target - 1], equal_alias ? 2 : 1);
	if (shared && any_content()) {
		nested++;
		elog(ERROR, "ordinary VM acquire beneath content locks");
	}
	if (!context->active) {
		context->active = true;
		context->resource = tags[target - 1];
		context->r4_record_generation = generation;
		context->absolute_deadline_us = 4242;
		starts++;
	} else {
		seen_cursor = true;
		UT_ASSERT_EQ(context->absolute_deadline_us, 4242);
		if (!BufferTagsEqual(&context->resource, &tags[target - 1]))
			elog(ERROR, "old auxiliary cursor cannot authorize a new map");
		if (fail_history)
			UT_ASSERT_EQ(context->caller.failed_attempt, 7);
	}
	if (any_content()) {
		UT_ASSERT(site != NULL);
		order[order_count++] = target;
	} else {
		UT_ASSERT(site == NULL);
		warms++;
		if (warm_error)
			elog(ERROR, "controlled ordinary VM error");
		if (refuse_warm > 0 && (refuse_target == 0 || refuse_target == target)) {
			refuse_warm--;
			context->reobserve = true;
			if (fail_history)
				context->caller.failed_attempt = 7;
			if (replace_alias) {
				ReleaseBuffer(target);
				*buffer = InvalidBuffer;
				if (equal_alias) {
					ReleaseBuffer(target);
					*alias = InvalidBuffer;
				}
			}
			return false;
		}
		if (rotate_other && target == 2) {
			revision[0]++;
			rotate_other = false;
		}
		if (clear_hint)
			PageClearAllVisible(pages[2].data);
	}
	LockBuffer(target, BUFFER_LOCK_EXCLUSIVE);
	memset(context, 0, sizeof(*context));
	return true;
}

bool
ClusterBufferCaptureMutationContext(Buffer buffer, ResourceXWriterUseContext *out,
	uint64 *ko_revision, bool *checked)
{
	UT_ASSERT(!held[2] && !held[3] && held[buffer - 1]);
	captures++;
	if (capture_error)
		elog(ERROR, "controlled capture ERROR");
	if (missing_writer)
		return false;
	memset(out, 0, sizeof(*out));
	out->ref.assertion.resource = tags[buffer - 1];
	out->writer_activation_token = 700 + buffer;
	out->r4_record_generation = generation;
	*ko_revision = revision[buffer - 1];
	*checked = true;
	if (cancel_after_warm)
		InterruptPending = true;
	return true;
}

bool
ClusterConditionalLockBufferWithMutationContext(Buffer buffer,
	const ResourceXWriterUseContext *expected, uint64 ko_revision, bool checked)
{
	UT_ASSERT(held[2] && (same_heap || held[3]));
	UT_ASSERT(buffer >= 1 && buffer <= 2 && pins[buffer - 1] > 0 && !held[buffer - 1]);
	resumes++;
	order[order_count++] = buffer;
	if (resume_error && buffer == (Buffer)resume_error)
		elog(ERROR, "controlled resume ERROR");
	if (refuse_resume > 0 && (refuse_target == 0 || refuse_target == buffer)) {
		refuse_resume--;
		return false;
	}
	if (!checked || ko_revision != revision[buffer - 1]
		|| expected->writer_activation_token != (uint64)(700 + buffer)
		|| expected->r4_record_generation != generation
		|| !BufferTagsEqual(&expected->ref.assertion.resource, &tags[buffer - 1]))
		return false;
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	return true;
}

ResourceXWriterPath
cluster_resource_x_writer_path_snapshot(uint64 *out)
{
	*out = generation;
	return RESOURCE_X_WRITER_TARGET;
}

void
ProcessInterrupts(void)
{
	InterruptPending = false;
	checks++;
	UT_ASSERT(!any_content() && pins[0] + pins[1] == 0);
	elog(ERROR, "controlled cancellation");
}

static void
cancel_plan(ClusterCurrentMxStampPlan *plan)
{
	if (plan != NULL) {
		if (plan == old_plan_address)
			old_plan_cancelled = true;
		else if (plan == new_plan_address)
			new_plan_cancelled = true;
		else
			UT_ASSERT(false);
		*plan = 0;
		cancels++;
	}
}

/* The unchanged F helper's service boundary, also used by the original
 * bracket for RED. This does not replace F's production plan implementation. */
static bool
cluster_current_mx_stamp_lock_buffer(Buffer buffer, bool barrier,
	ClusterBufferBarrierSiteId site, bool *replaced,
	ClusterCurrentMxStampPlan *first, ClusterCurrentMxStampPlan *second,
	ResourceXAuxiliaryAcquireContext *context, Buffer *alias)
{
	bool result = false;

	UT_ASSERT(barrier);
	*replaced = false;
	PG_TRY();
	{
		result = ClusterLockBufferExclusiveAuxiliaryAliasAware(&buffer, alias, &site, context);
		*replaced = !BufferIsValid(buffer);
	}
	PG_CATCH();
	{
		cancel_plan(first);
		cancel_plan(second);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return result;
}

#define cluster_current_mx_stamp_cancel cancel_plan
#include "test_cluster_heap_insert_vm_authority.inc"
#include "test_cluster_heap_vm_pair_writer.inc"

void
visibilitymap_pin(Relation relation, BlockNumber block, Buffer *map)
{
	UT_ASSERT(relation == &fixture_relation && block == 0 && !any_content());
	UT_ASSERT(!BufferIsValid(*map));
	*map = old_map;
	UT_ASSERT_EQ(pins[*map - 1], 0);
	pins[*map - 1]++;
}
bool
visibilitymap_pin_recent(Relation relation, BlockNumber block, Buffer recent, Buffer *map)
{
	UT_ASSERT(relation == &fixture_relation && block == 0 && held[2]);
	UT_ASSERT(!BufferIsValid(*map) && recent == old_map && pins[recent - 1] == 0);
	*map = recent;
	pins[recent - 1]++;
	return true;
}
void cluster_heap_lock_with_vm_repin(Relation relation, BlockNumber heap, Buffer buffer, Buffer *map);
#include "test_cluster_heap_vm_unwind_helpers.inc"

void
heap_freetuple(HeapTuple tuple)
{
	UT_ASSERT(tuple == (HeapTuple)(uintptr_t)1 || tuple == (HeapTuple)(uintptr_t)2);
	frees++;
}
void
ClusterObserveBufferBarrierReceipt(ClusterBufferBarrierSiteId site,
	ClusterBufferBarrierPhase phase, RelFileLocator locator,
	ForkNumber fork, BlockNumber block, ClusterBufferBarrierOutcome outcome, uint64 proof)
{
	UT_ASSERT(fork == VISIBILITYMAP_FORKNUM);
	UT_ASSERT(RelFileLocatorEquals(locator, fixture_relation.rd_locator));
	if (phase == CLUSTER_BUFFER_BARRIER_PHASE_CALLER_POST) {
		UT_ASSERT(!any_content());
		posts++;
	} else {
		UT_ASSERT_EQ(phase, CLUSTER_BUFFER_BARRIER_PHASE_REENTRY);
		UT_ASSERT_EQ(held[2], !temp_lock);
		reentries++;
	}
}

static void
reset_pair(bool alias, bool reverse, bool temporary, bool nonshared)
{
	HeapTupleHeaderData tuple = { 0 };

	memset(pins, 0, sizeof(pins));
	memset(held, 0, sizeof(held));
	memset(&fixture_relation, 0, sizeof(fixture_relation));
	fixture_relation.rd_locator = (RelFileLocator){ 1663, 5, 16393 };
	old_map = reverse ? 2 : 1;
	new_map = alias ? old_map : (reverse ? 1 : 2);
	revision[0] = 33; revision[1] = 34; generation = 80;
	shared = !nonshared; same_heap = omit_old = omit_new = false;
	temp_lock = temporary; reverse_order = reverse;
	nested = warms = captures = resumes = writes = entries = posts = reentries = 0;
	cancels = frees = checks = l2_visits = reacquire_visits = starts = order_count = 0;
	refuse_warm = refuse_resume = resume_error = capture_error = warm_error = 0;
	refuse_target = 0;
	replace_alias = missing_writer = rotate_other = clear_hint = cancel_after_warm = false;
	do_repick = repicked = seen_cursor = fail_history = false;
	InterruptPending = false;
	for (int i = 0; i < 4; i++) {
		PageInit(pages[i].data, BLCKSZ, 0);
		InitBufferTag(&tags[i], &fixture_relation.rd_locator,
			i < 2 ? VISIBILITYMAP_FORKNUM : MAIN_FORKNUM, i);
		if (i >= 2) {
			tuple.t_hoff = SizeofHeapTupleHeader;
			UT_ASSERT_EQ(PageAddItem(pages[i].data, (Item)&tuple,
				SizeofHeapTupleHeader, 1, false, true), 1);
			PageSetAllVisible(pages[i].data);
		}
	}
}

/* Only original target selection/replanning is controlled. Both original
 * l2/reacquire jumps, CID/key cleanup, receipt and heap/VM reentry execute. */
static void
execute_pair(void)
{
	Relation relation = &fixture_relation;
	Buffer buffer = 3, newbuf = same_heap ? 3 : 4;
	Buffer vmbuffer = old_map, vmbuffer_new = same_heap ? InvalidBuffer : new_map;
	bool vm_locked = false, vm_locked_new = false, old_tuple_temp_locked = temp_lock;
	bool cluster_page_versioned = shared;
	ClusterHeapVmMutationState cluster_vm_state = { 0 }, cluster_vm_new_state = { 0 };
	ResourceXAuxiliaryAcquireContext cluster_vm_context = { 0 }, cluster_vm_new_context = { 0 };
	ClusterCurrentMxStampPlan cluster_current_mx_old_plan = 1, cluster_current_mx_new_plan = 1;
	HeapTupleData tuple = { 0 }, oldtup = { 0 };
	HeapTuple newtup = &tuple, heaptup = (HeapTuple)(uintptr_t)2;
	HeapTuple old_key_tuple = (HeapTuple)(uintptr_t)1;
	bool old_key_copied = true, iscombo = true;
	CommandId cid = 2, pgrac_entry_cid = 1;
	ItemPointerData tid;
	ItemPointer otid = &tid;
	BlockNumber block = 0;
	Page page;
	ItemId lp;

	(void)cluster_vm_state; (void)cluster_vm_new_state; (void)cluster_page_versioned;
	old_plan_address = &cluster_current_mx_old_plan;
	new_plan_address = &cluster_current_mx_new_plan;
	ItemPointerSet(otid, 0, 1);
	pins[2] = 1; held[2] = true;
	pins[vmbuffer - 1]++;
	if (!same_heap) {
		pins[3] = 1; held[3] = true;
		pins[vmbuffer_new - 1]++;
	}
	if (omit_old) PageClearAllVisible(pages[2].data);
	if (omit_new) PageClearAllVisible(pages[3].data);
reenter:
	old_plan_cancelled = new_plan_cancelled = false;
	if (++entries > 40)
		elog(ERROR, "fixture bounded retry exhausted");
#include "test_cluster_heap_vm_update.inc"
	UT_ASSERT(!PageIsAllVisible(pages[2].data) || vm_locked);
	UT_ASSERT(same_heap || !PageIsAllVisible(pages[3].data)
		|| (old_map == new_map ? vm_locked : vm_locked_new));
	UT_ASSERT_EQ(cluster_current_mx_old_plan + cluster_current_mx_new_plan, 2);
	writes++;
	if (vm_locked_new) LockBuffer(vmbuffer_new, BUFFER_LOCK_UNLOCK);
	if (vm_locked) LockBuffer(vmbuffer, BUFFER_LOCK_UNLOCK);
	return;
l2:
	l2_visits++;
	UT_ASSERT(!vm_locked && !vm_locked_new && !held[0] && !held[1] && held[2] && !held[3]);
	UT_ASSERT_EQ(cid, pgrac_entry_cid);
	UT_ASSERT(!iscombo && old_key_tuple == NULL && !old_key_copied);
	UT_ASSERT(oldtup.t_data == (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 1)));
	UT_ASSERT_EQ(oldtup.t_len, SizeofHeapTupleHeader);
	UT_ASSERT_EQ(pins[3], 0);
	UT_ASSERT_EQ(vmbuffer_new, InvalidBuffer);
	goto plan_again;
l_pgrac_reacquire:
	reacquire_visits++;
	UT_ASSERT(!any_content() && pins[0] + pins[1] == 0 && pins[3] == 0);
	UT_ASSERT(vmbuffer == InvalidBuffer && vmbuffer_new == InvalidBuffer);
	UT_ASSERT(old_key_tuple == NULL && !old_key_copied);
	cluster_heap_lock_with_vm_repin(relation, block, buffer, &vmbuffer);
plan_again:
	UT_ASSERT_EQ(cluster_current_mx_old_plan + cluster_current_mx_new_plan, 0);
	if (do_repick && !repicked && warms > 0 && cluster_vm_new_context.reobserve) {
		tags[new_map - 1].blockNum += 10;
		repicked = true;
	}
	if (!same_heap) {
		pins[3] = 1; held[3] = true;
		vmbuffer_new = new_map; pins[new_map - 1]++;
	}
	cluster_current_mx_old_plan = cluster_current_mx_new_plan = 1;
	heaptup = (HeapTuple)(uintptr_t)2;
	old_key_tuple = (HeapTuple)(uintptr_t)1; old_key_copied = true;
	iscombo = true; cid = 2;
	goto reenter;
}

static bool
run_pair(void)
{
	volatile bool caught = false;

	PG_TRY();
	{
		execute_pair();
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	return !caught;
}

UT_TEST(cold_distinct_maps_prepare_outside_all_content_and_resume_in_order)
{
	for (int bits = 0; bits < 4; bits++) {
		reset_pair(false, bits & 1, bits & 2, false);
		UT_ASSERT(run_pair());
		UT_ASSERT_EQ(writes, 1); UT_ASSERT_EQ(nested, 0);
		UT_ASSERT_EQ(warms, 2); UT_ASSERT_EQ(captures, 2);
		UT_ASSERT(order_count >= 2);
		if (order_count >= 2) {
			UT_ASSERT_EQ(order[order_count - 2], 1);
			UT_ASSERT_EQ(order[order_count - 1], 2);
		}
		UT_ASSERT_EQ(posts, 2); UT_ASSERT_EQ(reentries, 2);
		UT_ASSERT_EQ(temp_lock ? reacquire_visits : l2_visits, 2);
		UT_ASSERT_EQ(frees, temp_lock ? 2 : 4);
	}
}
UT_TEST(equal_map_two_pins_take_one_writer_and_one_content_lock)
{
	for (int bits = 0; bits < 4; bits++) {
		reset_pair(true, bits & 1, bits & 2, false);
		UT_ASSERT(run_pair()); UT_ASSERT_EQ(writes, 1);
		UT_ASSERT_EQ(warms, 1); UT_ASSERT_EQ(captures, 1); UT_ASSERT_EQ(resumes, 1);
		UT_ASSERT_EQ(nested, 0); UT_ASSERT_EQ(pins[old_map - 1], 2);
	}
}
UT_TEST(same_heap_uses_only_old_map_and_original_l2_retry)
{
	reset_pair(true, false, false, false); same_heap = true;
	UT_ASSERT(run_pair()); UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(warms, 1); UT_ASSERT_EQ(captures, 1); UT_ASSERT_EQ(resumes, 1);
}
UT_TEST(first_or_second_resume_refusal_releases_both_heaps_before_prepare)
{
	for (int bits = 0; bits < 8; bits++) {
		reset_pair(false, bits & 1, bits & 2, false);
		refuse_target = bits & 4 ? 2 : 1; refuse_resume = 1;
		UT_ASSERT(run_pair()); UT_ASSERT_EQ(writes, 1);
		UT_ASSERT_EQ(nested, 0); UT_ASSERT_EQ(warms, 3);
		UT_ASSERT_EQ(cancels, 6); UT_ASSERT_EQ(posts, 3);
	}
}
UT_TEST(other_map_ko_change_discards_only_original_proof_and_requalifies)
{
	reset_pair(false, false, false, false); rotate_other = true;
	UT_ASSERT(run_pair()); UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(warms, 3); UT_ASSERT_EQ(captures, 3); UT_ASSERT_EQ(nested, 0);
}
UT_TEST(warm_refusal_preserves_caller_failure_history_and_age)
{
	reset_pair(true, false, false, false);
	refuse_warm = 2; fail_history = true;
	UT_ASSERT(run_pair()); UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(warms, 3); UT_ASSERT_EQ(starts, 1); UT_ASSERT(seen_cursor);
}
UT_TEST(warm_alias_replacement_drops_both_declared_pins_before_reentry)
{
	for (int temporary = 0; temporary < 2; temporary++) {
		reset_pair(true, false, temporary, false);
		refuse_warm = 1; replace_alias = true;
		UT_ASSERT(run_pair()); UT_ASSERT_EQ(writes, 1);
		UT_ASSERT_EQ(warms, 2); UT_ASSERT_EQ(posts, 2); UT_ASSERT_EQ(nested, 0);
	}
}
UT_TEST(new_map_repick_resets_only_clean_same_relation_observation)
{
	reset_pair(false, false, false, false);
	refuse_target = 2; refuse_warm = 1; do_repick = true;
	UT_ASSERT(run_pair()); UT_ASSERT_EQ(writes, 1);
	UT_ASSERT(repicked); UT_ASSERT_EQ(nested, 0); UT_ASSERT_EQ(warms, 3);
}
UT_TEST(capture_unknown_or_error_unlocks_vm_and_never_reaches_mutation)
{
	for (int mode = 0; mode < 2; mode++) {
		reset_pair(true, false, false, false);
		missing_writer = mode == 0; capture_error = mode == 1;
		UT_ASSERT(!run_pair()); UT_ASSERT_EQ(writes, 0);
		UT_ASSERT(!any_content()); UT_ASSERT_EQ(cancels, 2);
		UT_ASSERT_EQ(nested, 0); UT_ASSERT_EQ(captures, 1);
	}
}
UT_TEST(resume_error_cancels_both_original_plans_for_either_leg)
{
	for (int bits = 0; bits < 4; bits++) {
		reset_pair(false, bits & 1, bits & 2, false);
		resume_error = bits & 1 ? 1 : 2;
		UT_ASSERT(!run_pair()); UT_ASSERT_EQ(writes, 0);
		UT_ASSERT(cancels >= 4); UT_ASSERT_EQ(nested, 0);
		UT_ASSERT(old_plan_cancelled && new_plan_cancelled);
	}
}
UT_TEST(cancellation_after_prepare_owns_no_vm_pins_or_content)
{
	for (int temporary = 0; temporary < 2; temporary++) {
		reset_pair(true, false, temporary, false); cancel_after_warm = true;
		UT_ASSERT(!run_pair()); UT_ASSERT_EQ(writes, 0);
		UT_ASSERT_EQ(checks, 1); UT_ASSERT_EQ(cancels, 2);
		UT_ASSERT(!any_content()); UT_ASSERT_EQ(pins[0] + pins[1], 0);
	}
}
UT_TEST(unlocked_pd_change_requalifies_without_reusing_vm_requirement)
{
	reset_pair(true, false, false, false); same_heap = true; clear_hint = true;
	UT_ASSERT(run_pair()); UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(resumes, 0); UT_ASSERT_EQ(warms, 1); UT_ASSERT_EQ(nested, 0);
}
UT_TEST(old_only_new_only_or_no_hint_uses_only_required_authority)
{
	for (int mode = 0; mode < 3; mode++) {
		reset_pair(false, true, false, false);
		omit_old = mode != 0; omit_new = mode != 1;
		UT_ASSERT(run_pair()); UT_ASSERT_EQ(writes, 1);
		UT_ASSERT_EQ(warms, mode == 2 ? 0 : 1);
		UT_ASSERT_EQ(captures, mode == 2 ? 0 : 1); UT_ASSERT_EQ(nested, 0);
	}
}
UT_TEST(native_pair_keeps_original_ascending_order_and_zero_shared_services)
{
	for (int bits = 0; bits < 4; bits++) {
		reset_pair(bits & 1, bits & 2, false, true);
		UT_ASSERT(run_pair()); UT_ASSERT_EQ(writes, 1);
		UT_ASSERT_EQ(warms + captures + resumes, 0);
		UT_ASSERT_EQ(order_count, bits & 1 ? 1 : 2);
		UT_ASSERT_EQ(order[0], bits == 3 ? 2 : 1);
	}
}

int
main(void)
{
	UT_PLAN(14);
	UT_RUN(cold_distinct_maps_prepare_outside_all_content_and_resume_in_order);
	UT_RUN(equal_map_two_pins_take_one_writer_and_one_content_lock);
	UT_RUN(same_heap_uses_only_old_map_and_original_l2_retry);
	UT_RUN(first_or_second_resume_refusal_releases_both_heaps_before_prepare);
	UT_RUN(other_map_ko_change_discards_only_original_proof_and_requalifies);
	UT_RUN(warm_refusal_preserves_caller_failure_history_and_age);
	UT_RUN(warm_alias_replacement_drops_both_declared_pins_before_reentry);
	UT_RUN(new_map_repick_resets_only_clean_same_relation_observation);
	UT_RUN(capture_unknown_or_error_unlocks_vm_and_never_reaches_mutation);
	UT_RUN(resume_error_cancels_both_original_plans_for_either_leg);
	UT_RUN(cancellation_after_prepare_owns_no_vm_pins_or_content);
	UT_RUN(unlocked_pd_change_requalifies_without_reusing_vm_requirement);
	UT_RUN(old_only_new_only_or_no_hint_uses_only_required_authority);
	UT_RUN(native_pair_keeps_original_ascending_order_and_zero_shared_services);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
