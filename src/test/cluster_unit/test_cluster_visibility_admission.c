/*
 * Execute owned visibility error unwinding and the production debt primitives.
 * Admission policy, control roots, I/O and transport are counted boundaries.
 * These fixtures prove lifetime cleanup only; they grant no native authority.
 */
#include "postgres.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_runtime_visibility.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_tt_local.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_tx_resolve.h"
#include "cluster/storage/cluster_undo_block0_current.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;
volatile uint32 InterruptHoldoffCount = 0;
int cluster_node_id = 0;

#include "test_cluster_visibility_admission_types.inc"

static ClusterSemanticActivationShmem state;
static ClusterSemanticActivationShmem *SemanticActivationShmem = &state;
static uint32 semantic_activation_local_inflight[2][64];
static const ClusterSemanticActivationDescriptor fixture_descriptor = { 0 };
static const ClusterSemanticActivationDescriptor *SemanticActivationDescriptors[64] = { &fixture_descriptor };

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

void
ExceptionalCondition(const char *condition pg_attribute_unused(),
					 const char *file pg_attribute_unused(), int line pg_attribute_unused())
{
	abort();
}

/* Unexpected production PANIC is fatal to this fixture, never a passing case. */
bool errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused()) { return true; }
bool errstart_cold(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused()) { return true; }
int errcode(int code pg_attribute_unused()) { return 0; }
int errmsg(const char *fmt pg_attribute_unused(), ...) { return 0; }
int errhint(const char *fmt pg_attribute_unused(), ...) { return 0; }
void errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(), const char *func pg_attribute_unused()) { abort(); }

#include "test_cluster_visibility_admission_debt.inc"

static unsigned callback_depth, guards, cancels, leaves, roots, leaves_with_guard;
static bool allow_admission = true;
static int fault;
enum { AT_NONE, AT_OWNER, AT_LOCATOR, AT_ROOT, AT_ACQUIRE, AT_COPY, AT_RELEASE, AT_RECHECK };
static void (*callbacks[4])(int, Datum);
static Datum callback_args[4];

void
before_shmem_exit(pg_on_exit_callback function, Datum arg)
{
	if (callback_depth >= lengthof(callbacks)) abort();
	callbacks[callback_depth] = function;
	callback_args[callback_depth++] = arg;
}

void
cancel_before_shmem_exit(pg_on_exit_callback function, Datum arg)
{
	if (callback_depth == 0 || callbacks[callback_depth - 1] != function ||
		callback_args[callback_depth - 1] != arg) abort();
	callback_depth--;
}

static void
raise_at(int point)
{
	if (fault == point) pg_re_throw();
}

static ClusterSemanticAdmissionResult
fixture_enter(uint64 bit, ClusterSemanticAdmissionSide side, ClusterSemanticAdmissionToken *token)
{
	memset(token, 0, sizeof(*token));
	if (!allow_admission) return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	if (bit != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1 || side != CLUSTER_SEMANTIC_TARGET_SIDE) abort();
	if (!semantic_activation_counter_increment(&state.inflight[side][0])) abort();
	semantic_activation_local_inflight[side][0]++;
	token->feature_bit = bit;
	token->side = side;
	token->record_generation = 6;
	token->formation_epoch = 1;
	token->entered = true;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

static void
fixture_leave(ClusterSemanticAdmissionToken *token)
{
	if (token->entered) {
		leaves++;
		if (guards != 0) leaves_with_guard++;
	}
	cluster_semantic_activation_leave(token);
}

void
cluster_undo_block0_current_cancel(ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused())
{
	if (guards == 0) abort();
	guards--;
	cancels++;
}

static bool
fixture_owner(ClusterTTSlotCurrentOwner *owner)
{
	raise_at(AT_OWNER);
	memset(owner, 0, sizeof(*owner));
	owner->segment_id = 1;
	return true;
}

static ClusterTTDurableLocate
fixture_locator(uint16 *segment, uint16 *slot, uint16 *wrap)
{
	raise_at(AT_LOCATOR);
	*segment = 1; *slot = 0; *wrap = 1;
	return CLUSTER_TT_DURABLE_LOCATE_FOUND;
}

static bool
fixture_root(void)
{
	roots++;
	if (roots == 1) raise_at(AT_ROOT);
	return true;
}

static ClusterUndoBlock0CurrentStep
fixture_acquire(ClusterRuntimeCandidateCleanup *cleanup)
{
	cleanup->active = true;
	guards++;
	raise_at(AT_ACQUIRE);
	return CLUSTER_UNDO_BLOCK0_CURRENT_HELD;
}

static bool
fixture_sample(void)
{
	raise_at(AT_COPY);
	return true; /* Only counted lifetime is judged, not provider authority. */
}

static ClusterUndoBlock0CurrentStep
fixture_release(void)
{
	raise_at(AT_RELEASE);
	if (guards != 1) abort();
	guards--;
	return CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED;
}

static bool
fixture_recheck(void)
{
	raise_at(AT_RECHECK);
	return true;
}

static bool
fixture_decode(uint32 *segment, uint32 *block, uint16 *slot, uint16 *row)
{
	*segment = 1; *block = 1; *slot = 0; *row = 0;
	return true;
}

static ClusterRuntimeVisibilityOriginStep
fixture_freeze(void)
{
	raise_at(AT_COPY);
	return CLUSTER_RUNTIME_VISIBILITY_ORIGIN_FAILED;
}

/* Counted external gates/providers; the ownership/error-control body is real.
 * E has no locate-scope type: AT_RECHECK targets its actual admitted freshness
 * check after the first root lookup, before the borrowed guard. */
#define cluster_semantic_activation_enter fixture_enter
#define cluster_semantic_activation_leave fixture_leave
#define cluster_runtime_visibility_recovered_open_admitted() true
#define cluster_xid_origin_slot(xid) 0
#define cluster_tt_local_get_published_binding(...) false
#define cluster_tt_slot_current_owner_by_xid(node, xid, owner) fixture_owner(owner)
#define cluster_runtime_visibility_current_owner_shape_valid(...) true
#define cluster_runtime_visibility_current_owner_from_local_binding(...) false
#define cluster_runtime_visibility_locate_scope_capture(xid,admission,scope) ((void)(scope), true)
#define cluster_runtime_visibility_locate_scope_current(...) fixture_recheck()
#define cluster_tt_slot_durable_locate_any_by_xid_origin(node,xid,segment,slot,wrap,generation) fixture_locator(segment,slot,wrap)
#define cluster_runtime_visibility_physical_locator_shape_valid(...) true
#define cluster_runtime_visibility_resolve_root_admitted(...) fixture_root()
#define cluster_runtime_visibility_admission_current(...) fixture_recheck()
#define cluster_runtime_visibility_ensure_exit_hooks() ((void)0)
#define cluster_runtime_visibility_candidate_acquire(logical,admission,guard,cleanup,result) ((void)(result), fixture_acquire(cleanup))
#define cluster_runtime_visibility_current_owner_sample_held_internal(xid,owner,binding,...) ((void)(binding), fixture_sample())
#define cluster_runtime_visibility_physical_locator_sample_held(...) fixture_sample()
#define cluster_undo_block0_current_sample_generation(...) CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED
#define cluster_runtime_visibility_ctrc_key_fill(...) false
#define cluster_ctrc_origin_touch_exact(...) CLUSTER_CTRC_TOUCH_RECORDED
#define cluster_runtime_visibility_candidate_root_matches(...) true
#define cluster_qvotec_get_self_incarnation() UINT64_C(1)
#define cluster_runtime_visibility_candidate_release(guard,result) ((void)(result), fixture_release())
#define cluster_runtime_visibility_current_owner_recheck(...) fixture_recheck()
#define GetCurrentTimestamp() ((TimestampTz)0)
#define cluster_epoch_get_current() UINT64_C(1)
#define uba_decode(uba,segment,block,slot,row) fixture_decode(segment,block,slot,row)
#define uba_origin_node_id(uba) ((NodeId)0)
#define cluster_runtime_visibility_candidate_acquire_until(logical,admission,guard,cleanup,result,deadline) ((void)(result), fixture_acquire(cleanup))
#define cluster_runtime_visibility_candidate_release_until(guard,result,deadline) ((void)(result), fixture_release())
#define cluster_runtime_visibility_origin_plan_freeze_data_held(locator,mode,admission,expected,guard,root,plan,resolution,reason) ((void)(reason), fixture_freeze())
#define cluster_runtime_visibility_origin_plan_canonical_physical(...) false
#define cluster_runtime_visibility_origin_plan_canonical_logical(...) false
#define cluster_runtime_visibility_origin_plan_sample_canonical_held(...) false
#define cluster_runtime_visibility_origin_plan_recheck_data_held(...) CLUSTER_TX_UNKNOWN
#define cluster_tx_locator_reply_matches(...) false
#define cluster_ctrc_origin_grant_publishable(...) false

#include "test_cluster_visibility_admission_callers.inc"

static bool
invoke(unsigned caller)
{
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;
	ClusterTxLocator locator = { 0 }, canonical;
	ClusterCtrcTxnKeyV1 ctrc;
	uint32 grant, capability;
	bool cross_segment;

	if (caller == 0)
		return cluster_runtime_visibility_current_owner_lookup_internal(10, &key, &result, false, NULL, NULL, NULL);
	if (caller == 1)
		return cluster_runtime_visibility_local_terminal_lookup_exact(10, &key, &result, NULL);
	locator.xid = 10;
	locator.tt_wrap = TT_WRAP_INVALID;
	return cluster_runtime_visibility_current_mx_updater_provenance_exact(&locator, 0, &key, &result,
		&grant, &capability, &ctrc, &canonical, &cross_segment, NULL);
}

static void
reset_fixture(void)
{
	memset(&state, 0, sizeof(state));
	memset(semantic_activation_local_inflight, 0, sizeof(semantic_activation_local_inflight));
	callback_depth = guards = cancels = leaves = roots = leaves_with_guard = 0;
	allow_admission = true;
	fault = AT_NONE;
}

static void
assert_debt(unsigned expected)
{
	UT_ASSERT_EQ(pg_atomic_read_u32(&state.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][0]), expected);
	UT_ASSERT_EQ(semantic_activation_local_inflight[CLUSTER_SEMANTIC_TARGET_SIDE][0], expected);
	UT_ASSERT_EQ(callback_depth, 0);
	UT_ASSERT_EQ(guards, 0);
	UT_ASSERT_EQ(leaves_with_guard, 0);
}

static void
run_fault(unsigned caller, int point, unsigned outer_debt)
{
	volatile bool caught = false;

	fault = point;
	PG_TRY();
	{
		(void)invoke(caller);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	assert_debt(outer_debt);
	UT_ASSERT_EQ(leaves, 1);
	UT_ASSERT_EQ(cancels, point == AT_ACQUIRE || point == AT_COPY || point == AT_RELEASE ? 1 : 0);
}

UT_TEST(test_owner_errors_retire_owned_admission)
{
	const int points[] = { AT_OWNER, AT_ROOT, AT_ACQUIRE, AT_COPY, AT_RELEASE, AT_RECHECK };
	for (unsigned i = 0; i < lengthof(points); i++) {
		reset_fixture();
		run_fault(0, points[i], 0);
	}
}

UT_TEST(test_terminal_errors_retire_owned_admission)
{
	/* E performs the locator before admission; it is covered separately. */
	const int points[] = { AT_ROOT, AT_ACQUIRE, AT_COPY, AT_RELEASE, AT_RECHECK };
	for (unsigned i = 0; i < lengthof(points); i++) {
		reset_fixture();
		run_fault(1, points[i], 0);
	}
}

UT_TEST(test_mx_errors_retire_owned_admission)
{
	const int points[] = { AT_ROOT, AT_ACQUIRE, AT_COPY, AT_RELEASE };
	for (unsigned i = 0; i < lengthof(points); i++) {
		reset_fixture();
		run_fault(2, points[i], 0);
	}
}

UT_TEST(test_normal_paths_retire_exactly_once)
{
	for (unsigned caller = 0; caller < 3; caller++) {
		reset_fixture();
		(void)invoke(caller);
		assert_debt(0);
		UT_ASSERT_EQ(leaves, 1);
		UT_ASSERT_EQ(cancels, 0);
	}
}

UT_TEST(test_refused_admission_has_no_leave)
{
	for (unsigned caller = 0; caller < 3; caller++) {
		reset_fixture();
		allow_admission = false;
		UT_ASSERT(!invoke(caller));
		assert_debt(0);
		UT_ASSERT_EQ(leaves, 0);
	}
}

UT_TEST(test_error_preserves_outer_admission)
{
	for (unsigned caller = 0; caller < 3; caller++) {
		ClusterSemanticAdmissionToken outer;
		reset_fixture();
		UT_ASSERT_EQ(fixture_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
			CLUSTER_SEMANTIC_TARGET_SIDE, &outer), CLUSTER_SEMANTIC_ADMISSION_OK);
		run_fault(caller, AT_COPY, 1);
		UT_ASSERT(outer.entered);
		fixture_leave(&outer);
		assert_debt(0);
	}
}

UT_TEST(test_borrowed_guard_cleanup_preserves_caller_token)
{
	ClusterSemanticAdmissionToken outer;
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterRuntimeCandidateCleanup cleanup = { &guard, true };
	reset_fixture();
	(void)fixture_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, CLUSTER_SEMANTIC_TARGET_SIDE, &outer);
	guards = 1;
	cluster_runtime_visibility_candidate_cleanup(0, PointerGetDatum(&cleanup));
	assert_debt(1);
	UT_ASSERT(!cleanup.active);
	UT_ASSERT_EQ(cancels, 1);
	UT_ASSERT_EQ(leaves, 0);
	UT_ASSERT(outer.entered);
	fixture_leave(&outer);
	assert_debt(0);
}

UT_TEST(test_caught_error_allows_next_request_in_same_process)
{
	for (unsigned caller = 0; caller < 3; caller++) {
		reset_fixture();
		run_fault(caller, AT_COPY, 0);
		fault = AT_NONE;
		(void)invoke(caller);
		assert_debt(0);
		UT_ASSERT_EQ(leaves, 2);
	}
}

UT_TEST(test_terminal_locator_error_before_admission_has_no_debt)
{
	volatile bool caught = false;
	reset_fixture();
	fault = AT_LOCATOR;
	PG_TRY();
	{
		(void)invoke(1);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	assert_debt(0);
	UT_ASSERT_EQ(leaves, 0);
	UT_ASSERT_EQ(cancels, 0);
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(test_owner_errors_retire_owned_admission);
	UT_RUN(test_terminal_errors_retire_owned_admission);
	UT_RUN(test_mx_errors_retire_owned_admission);
	UT_RUN(test_normal_paths_retire_exactly_once);
	UT_RUN(test_refused_admission_has_no_leave);
	UT_RUN(test_error_preserves_outer_admission);
	UT_RUN(test_borrowed_guard_cleanup_preserves_caller_token);
	UT_RUN(test_caught_error_allows_next_request_in_same_process);
	UT_RUN(test_terminal_locator_error_before_admission_has_no_debt);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
