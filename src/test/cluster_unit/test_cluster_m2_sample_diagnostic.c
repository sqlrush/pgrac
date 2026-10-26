/* Execute the real read-only physical probe, with runtime boundaries supplied
 * by this fixture. These checks do not establish native TT/CLOG authority.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_runtime_visibility.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/storage/cluster_undo_block0_current.h"

#undef printf
#undef fprintf
#undef snprintf
#undef vsnprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

int cluster_node_id = 0;
volatile sig_atomic_t InterruptPending;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

static char calls[64], message[512];
static int call_count, owner_calls, root_calls, provider_calls;
static int fault, acquire_pending, release_pending, sleeps;
static bool guard_held;
static VariableCacheData variable_cache;
VariableCache ShmemVariableCache = &variable_cache;
static LWLockPadded test_lwlocks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = test_lwlocks;
static bool retired_owner;
static int locate_calls, scope_reads, scope_fault, scope_fault_read, native_lock_depth;
static int locate_fault;
static ClusterTTDurableLocate locate_result;
static ClusterTTStatus provider_status;
static uint64 formation_epoch = 17;
static void note_call(char call);

enum {
	SCOPE_INVALID_NEXT = 1,
	SCOPE_RAW_WRAP,
	SCOPE_FUTURE_XID,
	SCOPE_INVALID_HORIZON,
	SCOPE_TRUNCATED,
	SCOPE_EPOCH,
	SCOPE_LARGE_EPOCH,
	SCOPE_SYSTEM,
	SCOPE_BOOT,
	SCOPE_SEGMENT,
	SCOPE_ADMISSION
};

static bool
scope_fault_now(int selected)
{
	return scope_fault == selected && scope_reads == scope_fault_read;
}

FullTransactionId
ReadNextFullTransactionId(void)
{
	scope_reads++;
	variable_cache.oldestClogXid = scope_fault_now(SCOPE_INVALID_HORIZON) ? InvalidTransactionId
								   : scope_fault_now(SCOPE_TRUNCATED) ? 102
																	  : FirstNormalTransactionId;
	if (scope_fault_now(SCOPE_INVALID_NEXT))
		return InvalidFullTransactionId;
	return FullTransactionIdFromEpochAndXid(scope_fault_now(SCOPE_RAW_WRAP) ? 1 : 0,
											scope_fault_now(SCOPE_FUTURE_XID) ? 101 : 201);
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == XactTruncationLock && mode == LW_SHARED);
	UT_ASSERT_EQ(native_lock_depth, 0);
	native_lock_depth++;
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	UT_ASSERT(lock == XactTruncationLock);
	UT_ASSERT_EQ(native_lock_depth, 1);
	native_lock_depth--;
}

uint64
cluster_epoch_get_current(void)
{
	return scope_fault_now(SCOPE_LARGE_EPOCH) ? UINT64_MAX
		   : scope_fault_now(SCOPE_EPOCH)	  ? formation_epoch + 1
											  : formation_epoch;
}

uint64
GetSystemIdentifier(void)
{
	return scope_fault_now(SCOPE_SYSTEM) ? (scope_reads == 1 ? 0 : 43) : 42;
}

uint64
cluster_qvotec_get_self_incarnation(void)
{
	return scope_fault_now(SCOPE_BOOT) ? (scope_reads == 1 ? 0 : 4) : 3;
}

uint32
cluster_tt_slot_current_segment(int node)
{
	UT_ASSERT_EQ(node, 0);
	return scope_fault_now(SCOPE_SEGMENT) ? (scope_reads == 1 ? 0 : 2) : 1;
}

ClusterTTDurableLocate
cluster_tt_slot_durable_locate_any_by_xid_origin(int node, TransactionId xid, uint16 *segment,
												 uint16 *slot, uint16 *wrap, uint8 *status)
{
	note_call('D');
	locate_calls++;
	UT_ASSERT_EQ(node, 0);
	UT_ASSERT_EQ(xid, 101);
	UT_ASSERT_EQ(native_lock_depth, 0);
	UT_ASSERT(status == NULL);
	*segment = locate_fault == 1 ? 0 : 2;
	*slot = locate_fault == 2 ? TT_SLOTS_PER_SEGMENT : 5;
	*wrap = locate_fault == 3 ? TT_WRAP_INVALID : 7;
	return locate_result;
}

enum {
	ORIGIN = 1,
	ADMISSION,
	OWNER,
	ROOT,
	ACQUIRE,
	PROVIDER,
	FINAL_OWNER,
	FINAL_ROOT,
	FINAL_ADMISSION,
	RELEASE,
	PROVIDER_ERROR
};

static void
note_call(char call)
{
	UT_ASSERT(call_count < sizeof(calls) - 1);
	calls[call_count++] = call;
	calls[call_count] = '\0';
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	abort();
}

void
pg_re_throw(void)
{
	siglongjmp(*PG_exception_stack, 1);
}

void
ProcessInterrupts(void)
{
	abort();
}

void
pg_usleep(long usec)
{
	UT_ASSERT_EQ(usec, 1000);
	sleeps++;
}

bool
errstart(int level, const char *domain)
{
	UT_ASSERT_EQ(level, LOG);
	return true;
}

bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}

int
errmsg_internal(const char *format, ...)
{
	va_list args;
	va_start(args, format);
	vsnprintf(message, sizeof(message), format, args);
	va_end(args);
	return 0;
}

void
errfinish(const char *file, int line, const char *function)
{}

int
cluster_xid_origin_slot(TransactionId xid)
{
	UT_ASSERT_EQ(xid, 101);
	return fault == ORIGIN ? 1 : 0;
}

void
cluster_runtime_visibility_ensure_exit_hooks(void)
{}

void
cluster_undo_block0_current_ensure_exit_hooks(void)
{}

/* Retain the separately linked runtime call boundary across PG_TRY. */
ClusterSemanticAdmissionResult __attribute__((noinline))
cluster_semantic_activation_enter(uint64 feature, ClusterSemanticAdmissionSide side,
								  ClusterSemanticAdmissionToken *token)
{
	note_call('A');
	UT_ASSERT_EQ(feature, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(side, CLUSTER_SEMANTIC_TARGET_SIDE);
	token->entered = fault != ADMISSION;
	token->formation_epoch = formation_epoch;
	return fault == ADMISSION ? CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED
							  : CLUSTER_SEMANTIC_ADMISSION_OK;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	note_call('E');
	UT_ASSERT(token->entered);
	token->entered = false;
}

bool
cluster_semantic_activation_recheck(const ClusterSemanticAdmissionToken *token)
{
	note_call('C');
	UT_ASSERT(token->entered);
	return fault != FINAL_ADMISSION && !scope_fault_now(SCOPE_ADMISSION);
}

bool
cluster_tt_slot_current_owner_by_xid(int node, TransactionId xid, ClusterTTSlotCurrentOwner *owner)
{
	note_call('O');
	UT_ASSERT_EQ(node, 0);
	UT_ASSERT_EQ(xid, 101);
	owner_calls++;
	memset(owner, 0, sizeof(*owner));
	if (fault == OWNER || retired_owner)
		return false;
	owner->segment_id = 1;
	owner->xid = 101;
	owner->wrap = fault == FINAL_OWNER && owner_calls == 2 ? 3 : 2;
	owner->slot_offset = 4;
	return true;
}

bool
cluster_semantic_activation_resolve_shared_undo_root(const ClusterSemanticAdmissionToken *token,
													 ClusterUndoPathIntent intent, uint32 owner,
													 uint32 segment,
													 ClusterUndoBlock0ResolvedRoot *root)
{
	note_call('R');
	UT_ASSERT(token->entered);
	UT_ASSERT_EQ(intent, CLUSTER_UNDO_PATH_RUNTIME_SHARED);
	UT_ASSERT_EQ(owner, 1);
	UT_ASSERT_EQ(segment, retired_owner ? 2 : 1);
	UT_ASSERT_EQ(native_lock_depth, 0);
	root_calls++;
	memset(root, 0, sizeof(*root));
	root->intent = intent;
	root->root_id = fault == FINAL_ROOT && root_calls == 2 ? 20 : 19;
	root->root_generation = 1;
	return fault != ROOT;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_begin_admitted(const ClusterUndoBlock0LogicalKey *key,
												   ClusterUndoBlock0CurrentMode mode, int timeout,
												   const ClusterSemanticAdmissionToken *token,
												   ClusterUndoBlock0CurrentGuard *guard,
												   ClusterUndoBlock0Result *failure)
{
	note_call('X');
	UT_ASSERT_EQ(key->owner_instance, 1);
	UT_ASSERT_EQ(key->segment_id, retired_owner ? 2 : 1);
	UT_ASSERT_EQ(native_lock_depth, 0);
	UT_ASSERT_EQ(mode, CLUSTER_UNDO_BLOCK0_SCUR);
	UT_ASSERT_EQ(timeout, 0);
	UT_ASSERT(token->entered);
	if (fault == ACQUIRE)
		return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
	guard_held = acquire_pending == 0;
	return guard_held ? CLUSTER_UNDO_BLOCK0_CURRENT_HELD : CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_poll(ClusterUndoBlock0CurrentGuard *guard,
										 ClusterUndoBlock0Result *failure)
{
	guard_held = --acquire_pending == 0;
	return guard_held ? CLUSTER_UNDO_BLOCK0_CURRENT_HELD : CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
}

bool
cluster_undo_block0_current_wait_reply(ClusterUndoBlock0CurrentGuard *guard,
									   ClusterUndoBlock0ReplyWaitSite site)
{
	UT_ASSERT(site == CLUSTER_UNDO_BLOCK0_WAIT_RUNTIME_VIS_ACQUIRE
			  || site == CLUSTER_UNDO_BLOCK0_WAIT_RUNTIME_VIS_RELEASE);
	return false;
}

bool
cluster_runtime_visibility_physical_locator_sample_held(
	const ClusterTTSlotPhysicalLocator *locator, const ClusterSemanticAdmissionToken *token,
	ClusterUndoBlock0CurrentGuard *guard, const ClusterUndoBlock0ResolvedRoot *root,
	ClusterTTStatusKey *key, ClusterTTStatusResult *result, bool *active, bool *precommit)
{
	note_call('P');
	provider_calls++;
	UT_ASSERT(guard_held && token->entered);
	UT_ASSERT_EQ(locator->segment_id, retired_owner ? 2 : 1);
	UT_ASSERT_EQ(locator->xid, 101);
	UT_ASSERT_EQ(locator->slot_offset, retired_owner ? 5 : 4);
	UT_ASSERT_EQ(locator->wrap, retired_owner ? 7 : 2);
	UT_ASSERT_EQ(root->root_id, 19);
	UT_ASSERT(precommit == NULL);
	if (fault == PROVIDER_ERROR)
		siglongjmp(*PG_exception_stack, 1);
	result->authoritative = fault != PROVIDER;
	result->status = provider_status;
	result->commit_scn = provider_status == CLUSTER_TT_STATUS_COMMITTED ? 777 : InvalidScn;
	return fault != PROVIDER;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_release_begin(ClusterUndoBlock0CurrentGuard *guard,
										  ClusterUndoBlock0Result *failure)
{
	note_call('L');
	UT_ASSERT(guard_held);
	if (fault == RELEASE)
		return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
	guard_held = false;
	return release_pending ? CLUSTER_UNDO_BLOCK0_CURRENT_PENDING
						   : CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_release_poll(ClusterUndoBlock0CurrentGuard *guard,
										 ClusterUndoBlock0Result *failure)
{
	return --release_pending ? CLUSTER_UNDO_BLOCK0_CURRENT_PENDING
							 : CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED;
}

void
cluster_undo_block0_current_cancel(ClusterUndoBlock0CurrentGuard *guard)
{
	note_call('Z');
	guard_held = false;
}

#include "test_cluster_m2_sample_diagnostic.inc"

static void
reset(int selected_fault)
{
	fault = selected_fault;
	call_count = owner_calls = root_calls = provider_calls = 0;
	acquire_pending = release_pending = sleeps = 0;
	guard_held = false;
	retired_owner = false;
	locate_calls = scope_reads = scope_fault = scope_fault_read = locate_fault = 0;
	native_lock_depth = 0;
	locate_result = CLUSTER_TT_DURABLE_LOCATE_MISSING;
	provider_status = CLUSTER_TT_STATUS_IN_PROGRESS;
	formation_epoch = 17;
	calls[0] = message[0] = '\0';
}

static void
test_refusal_reports_first_actual_boundary_without_bypassing_later_fences(void)
{
	static const struct {
		int fault;
		const char *stage, *calls;
		int providers;
	} cases[] = { { ORIGIN, "stage=origin ", "", 0 },
				  { ADMISSION, "stage=admission ", "A", 0 },
				  { OWNER, "stage=locate ", "AOCCDE", 0 },
				  { ROOT, "stage=root ", "AORE", 0 },
				  { ACQUIRE, "stage=acquire ", "AORXZE", 0 },
				  { PROVIDER, "stage=provider ", "AORXPLZE", 1 },
				  { RELEASE, "stage=release ", "AORXPLZE", 1 },
				  { FINAL_OWNER, "stage=final_owner ", "AORXPLOZE", 1 },
				  { FINAL_ROOT, "stage=final_root ", "AORXPLORZE", 1 },
				  { FINAL_ADMISSION, "stage=final_admission ", "AORXPLORCZE", 1 } };
	for (int i = 0; i < lengthof(cases); i++) {
		ClusterTTStatusResult result = { 0 };
		reset(cases[i].fault);
		UT_ASSERT(!sample_physical(101, &result));
		UT_ASSERT_STR_EQ(calls, cases[i].calls);
		UT_ASSERT_EQ(provider_calls, cases[i].providers);
		UT_ASSERT(!guard_held);
		UT_ASSERT(strstr(message, "M2_PROBE path=physical xid=101 ") != NULL);
		UT_ASSERT(strstr(message, cases[i].stage) != NULL);
		UT_ASSERT(strstr(message, "sampled=0") != NULL);
	}
}

static void
test_success_keeps_original_pending_waits_and_final_checks(void)
{
	ClusterTTStatusResult result = { 0 };
	reset(0);
	acquire_pending = release_pending = 2;
	UT_ASSERT(sample_physical(101, &result));
	UT_ASSERT_STR_EQ(calls, "AORXPLORCZE");
	UT_ASSERT_EQ(sleeps, 2);
	UT_ASSERT(!guard_held);
	UT_ASSERT(strstr(message, "stage=complete sampled=1") != NULL);
}

static void
test_provider_error_still_cancels_guard_and_leaves_admission(void)
{
	ClusterTTStatusResult result = { 0 };
	volatile bool caught = false;
	reset(PROVIDER_ERROR);
	PG_TRY();
	{
		(void)sample_physical(101, &result);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_STR_EQ(calls, "AORXPZE");
	UT_ASSERT(!guard_held);
	/* Normal-return diagnostics never stand in for an ERROR result. */
	UT_ASSERT_STR_EQ(message, "");
}

static void
test_retired_owner_uses_exact_physical_provider_with_original_waits(void)
{
	static const ClusterTTStatus statuses[]
		= { CLUSTER_TT_STATUS_COMMITTED, CLUSTER_TT_STATUS_ABORTED, CLUSTER_TT_STATUS_IN_PROGRESS };
	for (int i = 0; i < lengthof(statuses) * 2; i++) {
		ClusterTTStatusResult result = { 0 };
		reset(0);
		formation_epoch = i < lengthof(statuses) ? 0 : 17;
		retired_owner = true;
		locate_result = CLUSTER_TT_DURABLE_LOCATE_FOUND;
		provider_status = statuses[i % lengthof(statuses)];
		acquire_pending = release_pending = 2;
		UT_ASSERT(sample_physical(101, &result));
		UT_ASSERT_EQ(result.status, statuses[i % lengthof(statuses)]);
		UT_ASSERT_EQ(result.commit_scn, i % lengthof(statuses) == 0 ? 777 : InvalidScn);
		UT_ASSERT_EQ(provider_calls, 1);
		UT_ASSERT_EQ(locate_calls, 1);
		UT_ASSERT_EQ(owner_calls, 1);
		UT_ASSERT_EQ(scope_reads, 4);
		UT_ASSERT_EQ(sleeps, 2);
		UT_ASSERT(!guard_held && native_lock_depth == 0);
		UT_ASSERT(strstr(message, "stage=complete sampled=1") != NULL);
	}
}

static void
test_missing_ambiguous_unavailable_or_malformed_locator_never_samples(void)
{
	for (int i = 0; i < 6; i++) {
		ClusterTTStatusResult result = { 0 };
		reset(0);
		retired_owner = true;
		locate_result
			= i < 3 ? CLUSTER_TT_DURABLE_LOCATE_MISSING + i : CLUSTER_TT_DURABLE_LOCATE_FOUND;
		locate_fault = i < 3 ? 0 : i - 2;
		UT_ASSERT(!sample_physical(101, &result));
		UT_ASSERT_EQ(locate_calls, 1);
		UT_ASSERT_EQ(provider_calls, 0);
		UT_ASSERT_EQ(root_calls, 0);
		UT_ASSERT(!guard_held && native_lock_depth == 0);
		UT_ASSERT(strstr(message, "stage=locate sampled=0") != NULL);
	}
}

static void
test_native_interval_and_all_incarnation_fences_surround_the_scan_and_guard(void)
{
	static const char *stages[]
		= { "locate_scope", "locate_recheck", "sample_scope", "final_scope" };
	for (int read = 1; read <= 4; read++) {
		for (int selected = SCOPE_INVALID_NEXT; selected <= SCOPE_ADMISSION; selected++) {
			ClusterTTStatusResult result = { 0 };
			char expected[64];
			reset(0);
			retired_owner = true;
			locate_result = CLUSTER_TT_DURABLE_LOCATE_FOUND;
			scope_fault = selected;
			scope_fault_read = read;
			UT_ASSERT(!sample_physical(101, &result));
			UT_ASSERT_EQ(locate_calls, read > 1 ? 1 : 0);
			UT_ASSERT_EQ(provider_calls, read > 2 ? 1 : 0);
			UT_ASSERT(!guard_held && native_lock_depth == 0);
			snprintf(expected, sizeof(expected), "stage=%s sampled=0", stages[read - 1]);
			UT_ASSERT(strstr(message, expected) != NULL);
		}
	}
}

static void
test_retired_owner_does_not_turn_provider_refusal_into_a_terminal_result(void)
{
	ClusterTTStatusResult result = { 0 };
	reset(PROVIDER);
	retired_owner = true;
	locate_result = CLUSTER_TT_DURABLE_LOCATE_FOUND;
	UT_ASSERT(!sample_physical(101, &result));
	UT_ASSERT_EQ(provider_calls, 1);
	UT_ASSERT(!guard_held && native_lock_depth == 0);
	UT_ASSERT(strstr(message, "stage=provider sampled=0") != NULL);
}

static void
test_retired_owner_provider_error_keeps_original_cleanup(void)
{
	ClusterTTStatusResult result = { 0 };
	volatile bool caught = false;
	reset(PROVIDER_ERROR);
	retired_owner = true;
	locate_result = CLUSTER_TT_DURABLE_LOCATE_FOUND;
	PG_TRY();
	{
		(void)sample_physical(101, &result);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(provider_calls, 1);
	UT_ASSERT(!guard_held && native_lock_depth == 0);
	UT_ASSERT(call_count >= 2 && strcmp(calls + call_count - 2, "ZE") == 0);
	UT_ASSERT_STR_EQ(message, "");
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_refusal_reports_first_actual_boundary_without_bypassing_later_fences);
	UT_RUN(test_success_keeps_original_pending_waits_and_final_checks);
	UT_RUN(test_provider_error_still_cancels_guard_and_leaves_admission);
	UT_RUN(test_retired_owner_uses_exact_physical_provider_with_original_waits);
	UT_RUN(test_missing_ambiguous_unavailable_or_malformed_locator_never_samples);
	UT_RUN(test_native_interval_and_all_incarnation_fences_surround_the_scan_and_guard);
	UT_RUN(test_retired_owner_does_not_turn_provider_refusal_into_a_terminal_result);
	UT_RUN(test_retired_owner_provider_error_keeps_original_cleanup);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
