/* Execute actual native frame/name APIs with narrow backend boundaries.
 * This fixture does not perform DML, WAL, TT or distributed recovery.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"
#include <setjmp.h>
#include "access/xact.h"
#include "access/parallel.h"
#include "cluster/cluster_savepoint.h"
#include "cluster/cluster_tt_local.h"
#include "cluster/cluster_tt_slot.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#undef printf
#undef snprintf
#undef elog
#undef ereport
#include "unit_test.h"
UT_DEFINE_GLOBALS();

#define CLUSTER_SAVEPOINT_TEST_TYPES
#include "test_cluster_savepoint_native.inc"
#undef CLUSTER_SAVEPOINT_TEST_TYPES

static TransactionStateData top;
static TransactionState CurrentTransactionState = &top;
static SubTransactionId currentSubTransactionId = TopSubTransactionId;
static CommandId currentCommandId;
static bool currentCommandIdUsed;
FullTransactionId XactTopFullTransactionId;
static FullTransactionId next_fxid;
static int xid_assignments;
static int binding_lookups;
static int bad_binding;
static bool parallel_mode;
static TransactionId last_binding_xid;
static void AssignTransactionId(TransactionState state);
MemoryContext TopTransactionContext = (MemoryContext) 1;
MemoryContext CurTransactionContext = (MemoryContext) 1;
MemoryContext CurrentMemoryContext = (MemoryContext) 1;
ResourceOwner CurTransactionResourceOwner;
ResourceOwner CurrentResourceOwner;
bool XactReadOnly;
int ParallelWorkerNumber = -1;
bool cluster_enabled = true;
int cluster_node_id = 0;
static XactCallback end_callback;
static void *end_callback_arg;
static jmp_buf error_jump;
static bool expect_error;
static int error_code;
static int fail_allocation;
static int allocations;

static void fixture_error(void) pg_attribute_noreturn();
static void
fixture_error(void)
{
	if (expect_error)
		longjmp(error_jump, 1);
	abort();
}
#define elog(level, ...) fixture_error()
#define ereport(level, rest) do { (void) (rest); fixture_error(); } while (0)
#define MemoryContextSwitchTo(context) (CurrentMemoryContext = (context))

int
errcode(int code)
{
	error_code = code;
	return 0;
}
int
errmsg(const char *format, ...)
{
	(void) format;
	return 0;
}
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	(void) condition;
	(void) file;
	(void) line;
	abort();
}
void *
MemoryContextAllocZero(MemoryContext context, Size size)
{
	void *p;
	(void) context;
	if (fail_allocation > 0 && --fail_allocation == 0)
	{
		error_code = ERRCODE_OUT_OF_MEMORY;
		fixture_error();
	}
	p = calloc(1, size);
	if (p == NULL)
		abort();
	allocations++;
	return p;
}
char *
MemoryContextStrdup(MemoryContext context, const char *text)
{
	char *p = MemoryContextAllocZero(context, strlen(text) + 1);
	memcpy(p, text, strlen(text) + 1);
	return p;
}
void
pfree(void *p)
{
	if (p != NULL)
	{
		allocations--;
		free(p);
	}
}
void
RegisterXactCallback(XactCallback callback, void *arg)
{
	end_callback = callback;
	end_callback_arg = arg;
}
bool
IsInParallelMode(void)
{
	return parallel_mode;
}
int
NewGUCNestLevel(void)
{
	return CurrentTransactionState->nestingLevel + 1;
}
void
GetUserIdAndSecContext(Oid *userid, int *seccontext)
{
	*userid = 10;
	*seccontext = 0;
}

static void StartSubTransaction(void);
static void CommitSubTransaction(void);
static void AbortSubTransaction(void);
static void CleanupSubTransaction(void);
#include "../../backend/cluster/cluster_savepoint.c"
#include "test_cluster_savepoint_native.inc"

/* Only xid allocation and the published-binding boundary are simulated.
 * Frame stack, native accessors and the capture implementation are actual code. */
static void
AssignTransactionId(TransactionState state)
{
	if (state->parent != NULL && !FullTransactionIdIsValid(state->parent->fullTransactionId))
		AssignTransactionId(state->parent);
	state->fullTransactionId = next_fxid;
	FullTransactionIdAdvance(&next_fxid);
	xid_assignments++;
	if (state->parent == NULL)
		XactTopFullTransactionId = state->fullTransactionId;
}

bool
cluster_tt_local_get_published_binding(TransactionId xid, ClusterCanonicalTxnBinding *out)
{
	binding_lookups++;
	last_binding_xid = xid;
	memset(out, 0, sizeof(*out));
	out->segment_id = 1;
	out->segment_generation = 42;
	out->xid = xid;
	out->slot_wrap = 7;
	out->origin_instance = 1;
	out->publish_state = CLUSTER_CANONICAL_TXN_PUBLISHED;
	out->active_lsn = 100;
	switch (bad_binding)
	{
		case 1: return false;
		case 2: out->xid++; break;
		case 3: out->segment_id = 0; break;
		case 4: out->segment_id = PG_UINT16_MAX + 1U; break;
		case 5: out->segment_generation = PG_UINT32_MAX; break;
		case 6: out->slot_offset = TT_SLOTS_PER_SEGMENT; break;
		case 7: out->slot_wrap = TT_WRAP_INVALID; break;
		case 8: out->origin_instance++; break;
		case 9: out->publish_state = CLUSTER_CANONICAL_TXN_PUBLISHING; break;
		case 10: out->reserved16 = 1; break;
		case 11: out->active_lsn = InvalidXLogRecPtr; break;
		case 12: out->segment_generation = 0; break;
	}
	return true;
}

#ifndef CLUSTER_SAVEPOINT_OPERATION_PRESENT
/* Compile the parent implementation: missing capture is an observed refusal. */
typedef struct ClusterSavepointOperation {
	ClusterCanonicalTxnBinding top;
	FullTransactionId top_fxid;
	FullTransactionId operation_fxid;
	uint64 operation_sequence;
	CommandId command_id;
	SubTransactionId subid;
} ClusterSavepointOperation;
static bool
cluster_savepoint_capture_operation(ClusterSavepointOperation *out)
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	return false;
}
static bool
cluster_savepoint_operation_matches_current(const ClusterSavepointOperation *operation)
{
	(void) operation;
	return false;
}
#endif

#ifndef CLUSTER_SAVEPOINT_FRAME_OPERATION_PRESENT
/* Execute the existing current-only producer on the parent, exposing its
 * wrong scope rather than pretending ancestor capture already exists. */
static bool
cluster_savepoint_capture_frame_operation(SubTransactionId subid, ClusterSavepointOperation *out)
{
	(void)subid;
	return cluster_savepoint_capture_operation(out);
}
static bool
cluster_savepoint_operation_matches_frame(const ClusterSavepointOperation *operation)
{
	return cluster_savepoint_operation_matches_current(operation);
}
static FullTransactionId
GetFullTransactionIdForSubTransactionIfAny(SubTransactionId subid)
{
	(void)subid;
	return GetCurrentFullTransactionIdIfAny();
}
#endif

static void
StartSubTransaction(void)
{
	CurrentTransactionState->state = TRANS_INPROGRESS;
}
static void
CommitSubTransaction(void)
{
	CurrentTransactionState->state = TRANS_DEFAULT;
	PopTransaction();
}
static void
AbortSubTransaction(void)
{
	CurrentTransactionState->state = TRANS_ABORT;
}
static void
CleanupSubTransaction(void)
{
	CurrentTransactionState->state = TRANS_DEFAULT;
	PopTransaction();
}
void
CommitTransactionCommand(void)
{
	for (;;)
	{
		switch (CurrentTransactionState->blockState)
		{
			case TBLOCK_SUBBEGIN:
				StartSubTransaction();
				CurrentTransactionState->blockState = TBLOCK_SUBINPROGRESS;
				return;
			case TBLOCK_SUBRELEASE:
				CommitSubTransaction();
				break;
			case TBLOCK_SUBABORT_PENDING:
			case TBLOCK_SUBABORT_END:
				CleanupSubTransaction();
				break;
			case TBLOCK_SUBRESTART:
				test_native_restart_live();
				return;
			case TBLOCK_SUBABORT_RESTART:
				test_native_restart_aborted();
				return;
			default:
				return;
		}
	}
}
void
StartTransactionCommand(void)
{
	CommitTransactionCommand();
}
static void
fresh_transaction(void)
{
	expect_error = false;
	fail_allocation = 0;
	while (CurrentTransactionState != &top)
		CleanupSubTransaction();
	if (end_callback != NULL)
		end_callback(XACT_EVENT_ABORT, end_callback_arg);
	memset(&top, 0, sizeof(top));
	top.subTransactionId = TopSubTransactionId;
	top.nestingLevel = 1;
	top.state = TRANS_INPROGRESS;
	top.blockState = TBLOCK_INPROGRESS;
	top.curTransactionContext = TopTransactionContext;
	currentSubTransactionId = TopSubTransactionId;
	XactTopFullTransactionId = InvalidFullTransactionId;
	next_fxid = FullTransactionIdFromEpochAndXid(5, 700);
	currentCommandId = 11;
	currentCommandIdUsed = false;
	xid_assignments = binding_lookups = bad_binding = 0;
	parallel_mode = false;
	last_binding_xid = InvalidTransactionId;
	cluster_enabled = true;
	cluster_node_id = 0;
}
static void
savepoint(const char *name)
{
	DefineSavepoint(name);
	CommitTransactionCommand();
}

UT_TEST(test_same_name_replaces_name_but_retains_parent_frame)
{
	TransactionState old;
	fresh_transaction();
	savepoint("s");
	old = CurrentTransactionState;
	savepoint("s");
	UT_ASSERT_NULL(old->name);
	UT_ASSERT_EQ(CurrentTransactionState->parent, old);
	UT_ASSERT_STR_EQ(CurrentTransactionState->name, "s");
	UT_ASSERT(!FullTransactionIdIsValid(old->fullTransactionId));
}
UT_TEST(test_release_does_not_reveal_replaced_name)
{
	fresh_transaction();
	savepoint("s");
	savepoint("s");
	ReleaseSavepoint("s");
	CommitTransactionCommand();
	expect_error = true;
	if (setjmp(error_jump) == 0)
	{
		RollbackToSavepoint("s");
		UT_ASSERT(false);
	}
	expect_error = false;
	UT_ASSERT_EQ(error_code, ERRCODE_S_E_INVALID_SPECIFICATION);
}
UT_TEST(test_earlier_name_and_later_points)
{
	fresh_transaction();
	savepoint("before");
	savepoint("s");
	savepoint("later");
	savepoint("s");
	RollbackToSavepoint("before");
	CommitTransactionCommand();
	UT_ASSERT_STR_EQ(CurrentTransactionState->name, "before");
	UT_ASSERT_EQ(CurrentTransactionState->parent, &top);
}
UT_TEST(test_restart_live_then_replace_again)
{
	TransactionState restarted;
	fresh_transaction();
	savepoint("s");
	RollbackToSavepoint("s");
	CommitTransactionCommand();
	restarted = CurrentTransactionState;
	savepoint("s");
	UT_ASSERT_NULL(restarted->name);
}
UT_TEST(test_restart_aborted_then_replace_again)
{
	TransactionState restarted;
	fresh_transaction();
	savepoint("s");
	CurrentTransactionState->state = TRANS_ABORT;
	CurrentTransactionState->blockState = TBLOCK_SUBABORT;
	RollbackToSavepoint("s");
	CommitTransactionCommand();
	restarted = CurrentTransactionState;
	savepoint("s");
	UT_ASSERT_NULL(restarted->name);
}
UT_TEST(test_anonymous_release_and_exception_preserve_parent)
{
	TransactionState parent;
	fresh_transaction();
	savepoint("outer");
	parent = CurrentTransactionState;
	BeginInternalSubTransaction(NULL);
	UT_ASSERT_NULL(CurrentTransactionState->name);
	ReleaseCurrentSubTransaction();
	UT_ASSERT_EQ(CurrentTransactionState, parent);
	BeginInternalSubTransaction(NULL);
	BeginInternalSubTransaction(NULL);
	RollbackAndReleaseCurrentSubTransaction();
	RollbackAndReleaseCurrentSubTransaction();
	UT_ASSERT_EQ(CurrentTransactionState, parent);
	UT_ASSERT_STR_EQ(parent->name, "outer");
}
UT_TEST(test_scope_boundary_keeps_outer_name)
{
	TransactionState outer;
	fresh_transaction();
	savepoint("s");
	outer = CurrentTransactionState;
	BeginInternalSubTransaction(NULL);
	CurrentTransactionState->savepointLevel = 1;
	savepoint("s");
	UT_ASSERT_STR_EQ(outer->name, "s");
	ReleaseSavepoint("s");
	CommitTransactionCommand();
	RollbackAndReleaseCurrentSubTransaction();
	UT_ASSERT_EQ(CurrentTransactionState, outer);
}
UT_TEST(test_disabled_mode_keeps_native_names)
{
	TransactionState old;
	fresh_transaction();
	cluster_enabled = false;
	savepoint("s");
	old = CurrentTransactionState;
	savepoint("s");
	UT_ASSERT_STR_EQ(old->name, "s");
}
static void
check_failed_allocation(int allocation)
{
	TransactionState old;
	fresh_transaction();
	savepoint("s");
	old = CurrentTransactionState;
	fail_allocation = allocation;
	expect_error = true;
	if (setjmp(error_jump) == 0)
	{
		savepoint("s");
		UT_ASSERT(false);
	}
	expect_error = false;
	while (CurrentTransactionState != old)
		CleanupSubTransaction();
	UT_ASSERT_STR_EQ(old->name, "s");
	RollbackToSavepoint("s");
	CommitTransactionCommand();
	UT_ASSERT_STR_EQ(CurrentTransactionState->name, "s");
}
UT_TEST(test_failed_native_frame_keeps_old_point)
{
	check_failed_allocation(1);
}
UT_TEST(test_failed_metadata_keeps_parent_frame)
{
	check_failed_allocation(2);
}
UT_TEST(test_failed_name_keeps_old_point)
{
	check_failed_allocation(3);
}
UT_TEST(test_top_end_retires_frame_memory)
{
	fresh_transaction();
	savepoint("outer");
	BeginInternalSubTransaction(NULL);
	ReleaseCurrentSubTransaction();
	ReleaseSavepoint("outer");
	CommitTransactionCommand();
	UT_ASSERT_EQ(CurrentTransactionState, &top);
	if (end_callback != NULL)
		end_callback(XACT_EVENT_COMMIT, end_callback_arg);
	UT_ASSERT_EQ(allocations, 0);
}
static bool
operation_empty(const ClusterSavepointOperation *operation)
{
	ClusterSavepointOperation empty = { 0 };
	return memcmp(operation, &empty, sizeof(empty)) == 0;
}

UT_TEST(test_capture_top_keeps_full_writer_and_exact_canonical)
{
	ClusterSavepointOperation operation;
	fresh_transaction();
	UT_ASSERT(cluster_savepoint_capture_operation(&operation));
	UT_ASSERT_EQ(U64FromFullTransactionId(operation.top_fxid), (UINT64CONST(5) << 32) + 700);
	UT_ASSERT(FullTransactionIdEquals(operation.top_fxid, operation.operation_fxid));
	UT_ASSERT_EQ(operation.top.xid, 700);
	UT_ASSERT_EQ(operation.top.segment_generation, 42);
	UT_ASSERT_EQ(operation.top.slot_wrap, 7);
	UT_ASSERT_EQ(operation.operation_sequence, 1);
	UT_ASSERT_EQ(operation.command_id, 11);
	UT_ASSERT_EQ(operation.subid, TopSubTransactionId);
	UT_ASSERT(!currentCommandIdUsed);
	UT_ASSERT_EQ(xid_assignments, 1);
	UT_ASSERT_EQ(binding_lookups, 1);
	UT_ASSERT_EQ(last_binding_xid, 700);
	UT_ASSERT(cluster_savepoint_operation_matches_current(&operation));
}

UT_TEST(test_capture_child_allocates_native_parents_but_looks_up_only_top)
{
	ClusterSavepointOperation operation;
	fresh_transaction();
	savepoint("outer");
	BeginInternalSubTransaction(NULL);
	UT_ASSERT_EQ(xid_assignments + binding_lookups, 0);
	UT_ASSERT(cluster_savepoint_capture_operation(&operation));
	UT_ASSERT_EQ(xid_assignments, 3);
	UT_ASSERT_EQ(binding_lookups, 1);
	UT_ASSERT_EQ(last_binding_xid, 700);
	UT_ASSERT_EQ(operation.top.xid, 700);
	UT_ASSERT_EQ(XidFromFullTransactionId(operation.operation_fxid), 702);
	UT_ASSERT_EQ(operation.subid, CurrentTransactionState->subTransactionId);
	UT_ASSERT(FullTransactionIdEquals(operation.operation_fxid,
		CurrentTransactionState->fullTransactionId));
	UT_ASSERT(cluster_savepoint_operation_matches_current(&operation));
}

UT_TEST(test_operation_sequence_survives_release_and_rollback_restart)
{
	ClusterSavepointOperation operation;
	ClusterSavepointOperation old;
	fresh_transaction();
	UT_ASSERT(cluster_savepoint_capture_operation(&operation));
	savepoint("s");
	UT_ASSERT(cluster_savepoint_capture_operation(&old));
	UT_ASSERT_EQ(old.operation_sequence, 2);
	BeginInternalSubTransaction(NULL);
	UT_ASSERT(cluster_savepoint_capture_operation(&operation));
	UT_ASSERT_EQ(operation.operation_sequence, 3);
	RollbackAndReleaseCurrentSubTransaction();
	UT_ASSERT(cluster_savepoint_operation_matches_current(&old));
	UT_ASSERT(cluster_savepoint_capture_operation(&operation));
	UT_ASSERT_EQ(operation.operation_sequence, 4);
	RollbackToSavepoint("s");
	CommitTransactionCommand();
	UT_ASSERT(!cluster_savepoint_operation_matches_current(&old));
	UT_ASSERT(cluster_savepoint_capture_operation(&operation));
	UT_ASSERT_EQ(operation.operation_sequence, 5);
	UT_ASSERT(XidFromFullTransactionId(operation.operation_fxid)
		> XidFromFullTransactionId(old.operation_fxid));
	ReleaseSavepoint("s");
	CommitTransactionCommand();
	UT_ASSERT(cluster_savepoint_capture_operation(&operation));
	UT_ASSERT_EQ(operation.operation_sequence, 6);
}

UT_TEST(test_capture_counter_resets_at_top_end_without_frames)
{
	ClusterSavepointOperation operation;
	fresh_transaction();
	UT_ASSERT(cluster_savepoint_capture_operation(&operation));
	UT_ASSERT(cluster_savepoint_capture_operation(&operation));
	UT_ASSERT_EQ(operation.operation_sequence, 2);
	fresh_transaction();
	UT_ASSERT(cluster_savepoint_capture_operation(&operation));
	UT_ASSERT_EQ(operation.operation_sequence, 1);
}

UT_TEST(test_capture_refuses_invalid_entry_without_xid_or_binding_work)
{
	ClusterSavepointOperation operation;
	for (int variant = 0; variant < 5; variant++)
	{
		fresh_transaction();
		memset(&operation, 0xa5, sizeof(operation));
		switch (variant)
		{
			case 0: cluster_enabled = false; break;
			case 1: cluster_node_id = -1; break;
			case 2: cluster_node_id = SCN_MAX_VALID_NODE_ID + 1; break;
			case 3: top.state = TRANS_ABORT; break;
			case 4: parallel_mode = true; break;
		}
		UT_ASSERT(!cluster_savepoint_capture_operation(&operation));
		UT_ASSERT(operation_empty(&operation));
		UT_ASSERT_EQ(xid_assignments + binding_lookups, 0);
	}
	fresh_transaction();
	UT_ASSERT(!cluster_savepoint_capture_operation(NULL));
	UT_ASSERT_EQ(xid_assignments + binding_lookups, 0);
}

UT_TEST(test_capture_refuses_missing_or_malformed_published_owner)
{
	ClusterSavepointOperation operation;
	for (int variant = 1; variant <= 11; variant++)
	{
		fresh_transaction();
		bad_binding = variant;
		memset(&operation, 0xa5, sizeof(operation));
		UT_ASSERT(!cluster_savepoint_capture_operation(&operation));
		UT_ASSERT(operation_empty(&operation));
		bad_binding = 0;
		UT_ASSERT(cluster_savepoint_capture_operation(&operation));
		UT_ASSERT_EQ(operation.operation_sequence, 1);
	}
	/* Generation zero is a real fresh canonical segment, not UNKNOWN. */
	fresh_transaction();
	bad_binding = 12;
	UT_ASSERT(cluster_savepoint_capture_operation(&operation));
	UT_ASSERT_EQ(operation.top.segment_generation, 0);
}

UT_TEST(test_capture_rejects_alias_child_and_invalid_command_identity)
{
	ClusterSavepointOperation operation;
	fresh_transaction();
	(void) GetCurrentFullTransactionId();
	savepoint("s");
	CurrentTransactionState->fullTransactionId = XactTopFullTransactionId;
	UT_ASSERT(!cluster_savepoint_capture_operation(&operation));
	UT_ASSERT(operation_empty(&operation));
	fresh_transaction();
	currentCommandId = InvalidCommandId;
	UT_ASSERT(!cluster_savepoint_capture_operation(&operation));
	UT_ASSERT(operation_empty(&operation));
	fresh_transaction();
	next_fxid = FullTransactionIdFromEpochAndXid(5, FrozenTransactionId);
	UT_ASSERT(!cluster_savepoint_capture_operation(&operation));
	UT_ASSERT(operation_empty(&operation));
}

UT_TEST(test_operation_recheck_never_assigns_and_keeps_captured_command)
{
	ClusterSavepointOperation operation;
	int assignments;
	fresh_transaction();
	UT_ASSERT(cluster_savepoint_capture_operation(&operation));
	assignments = xid_assignments;
	currentCommandId++;
	UT_ASSERT(cluster_savepoint_operation_matches_current(&operation));
	UT_ASSERT_EQ(operation.command_id, 11);
	savepoint("s");
	UT_ASSERT(!cluster_savepoint_operation_matches_current(&operation));
	UT_ASSERT_EQ(xid_assignments, assignments);
	UT_ASSERT(!FullTransactionIdIsValid(CurrentTransactionState->fullTransactionId));
}

UT_TEST(test_operation_recheck_rejects_full_epoch_owner_and_scope_drift)
{
	ClusterSavepointOperation operation;
	fresh_transaction();
	UT_ASSERT(cluster_savepoint_capture_operation(&operation));
	top.fullTransactionId = FullTransactionIdFromEpochAndXid(6, 700);
	UT_ASSERT(!cluster_savepoint_operation_matches_current(&operation));
	top.fullTransactionId = operation.operation_fxid;
	XactTopFullTransactionId = FullTransactionIdFromEpochAndXid(6, 700);
	UT_ASSERT(!cluster_savepoint_operation_matches_current(&operation));
	XactTopFullTransactionId = operation.top_fxid;
	for (int variant = 1; variant <= 12; variant++)
	{
		bad_binding = variant;
		UT_ASSERT(!cluster_savepoint_operation_matches_current(&operation));
	}
	bad_binding = 0;
	operation.subid++;
	UT_ASSERT(!cluster_savepoint_operation_matches_current(&operation));
	operation.subid--;
	operation.operation_sequence = 0;
	UT_ASSERT(!cluster_savepoint_operation_matches_current(&operation));
}

UT_TEST(test_operation_sequence_exhaustion_cannot_wrap_or_allocate)
{
#ifdef CLUSTER_SAVEPOINT_OPERATION_PRESENT
	ClusterSavepointOperation operation;
	fresh_transaction();
	operation_sequence_floor = PG_UINT64_MAX - 1;
	UT_ASSERT(cluster_savepoint_capture_operation(&operation));
	UT_ASSERT_EQ(operation.operation_sequence, PG_UINT64_MAX);
	UT_ASSERT(!cluster_savepoint_capture_operation(&operation));
	UT_ASSERT(operation_empty(&operation));
	UT_ASSERT_EQ(xid_assignments, 1);
	UT_ASSERT_EQ(binding_lookups, 1);
#else
	UT_ASSERT(false);
#endif
}

UT_TEST(test_frame_capture_keeps_ancestor_and_data_writer_distinct)
{
	ClusterSavepointOperation ancestor;
	ClusterSavepointOperation data;
	SubTransactionId outer;

	fresh_transaction();
	savepoint("outer");
	outer = CurrentTransactionState->subTransactionId;
	BeginInternalSubTransaction(NULL);
	UT_ASSERT(cluster_savepoint_capture_frame_operation(outer, &ancestor));
	UT_ASSERT_EQ(ancestor.subid, outer);
	UT_ASSERT_EQ(XidFromFullTransactionId(ancestor.operation_fxid), 701);
	UT_ASSERT_EQ(ancestor.top.xid, 700);
	UT_ASSERT_EQ(ancestor.operation_sequence, 1);
	UT_ASSERT_EQ(xid_assignments, 3);
	UT_ASSERT_EQ(binding_lookups, 1);
	UT_ASSERT_EQ(last_binding_xid, 700);
	UT_ASSERT(cluster_savepoint_operation_matches_frame(&ancestor));
	UT_ASSERT(!cluster_savepoint_operation_matches_current(&ancestor));
	UT_ASSERT(cluster_savepoint_capture_operation(&data));
	UT_ASSERT_EQ(data.subid, CurrentTransactionState->subTransactionId);
	UT_ASSERT_EQ(XidFromFullTransactionId(data.operation_fxid), 702);
	UT_ASSERT_EQ(data.operation_sequence, 2);
	UT_ASSERT(cluster_savepoint_operation_matches_current(&data));
	UT_ASSERT(cluster_savepoint_operation_matches_frame(&data));
	UT_ASSERT(cluster_savepoint_capture_frame_operation(outer, &ancestor));
	UT_ASSERT_EQ(ancestor.operation_sequence, 3);
	UT_ASSERT_EQ(XidFromFullTransactionId(ancestor.operation_fxid), 701);
}

UT_TEST(test_frame_capture_keeps_replaced_and_anonymous_ancestors)
{
	ClusterSavepointOperation first;
	ClusterSavepointOperation second;
	ClusterSavepointOperation anonymous;
	TransactionState older;
	SubTransactionId newer;

	fresh_transaction();
	savepoint("same");
	older = CurrentTransactionState;
	savepoint("same");
	newer = CurrentTransactionState->subTransactionId;
	BeginInternalSubTransaction(NULL);
	UT_ASSERT(older->name == NULL);
	UT_ASSERT(cluster_savepoint_capture_frame_operation(older->subTransactionId, &first));
	UT_ASSERT(cluster_savepoint_capture_frame_operation(newer, &second));
	UT_ASSERT(cluster_savepoint_capture_frame_operation(CurrentTransactionState->subTransactionId,
														&anonymous));
	UT_ASSERT_EQ(XidFromFullTransactionId(first.operation_fxid), 701);
	UT_ASSERT_EQ(XidFromFullTransactionId(second.operation_fxid), 702);
	UT_ASSERT_EQ(XidFromFullTransactionId(anonymous.operation_fxid), 703);
	UT_ASSERT_EQ(anonymous.operation_sequence, 3);
	UT_ASSERT_EQ(xid_assignments, 4);
	UT_ASSERT_EQ(last_binding_xid, 700);
	ReleaseSavepoint("same");
	CommitTransactionCommand();
	UT_ASSERT(CurrentTransactionState == older);
	UT_ASSERT(cluster_savepoint_operation_matches_frame(&first));
	UT_ASSERT(!cluster_savepoint_operation_matches_frame(&second));
	UT_ASSERT(!cluster_savepoint_operation_matches_frame(&anonymous));
}

UT_TEST(test_native_frame_lookup_never_assigns_or_guesses)
{
	TransactionState outer;
	SubTransactionId current;

	fresh_transaction();
	savepoint("outer");
	outer = CurrentTransactionState;
	BeginInternalSubTransaction(NULL);
	current = CurrentTransactionState->subTransactionId;
	UT_ASSERT(!FullTransactionIdIsValid(
		GetFullTransactionIdForSubTransactionIfAny(outer->subTransactionId)));
	UT_ASSERT(!FullTransactionIdIsValid(GetFullTransactionIdForSubTransactionIfAny(current)));
	UT_ASSERT_EQ(xid_assignments + binding_lookups, 0);
	(void)GetCurrentFullTransactionId();
	UT_ASSERT_EQ(XidFromFullTransactionId(
					 GetFullTransactionIdForSubTransactionIfAny(outer->subTransactionId)),
				 701);
	UT_ASSERT_EQ(XidFromFullTransactionId(GetFullTransactionIdForSubTransactionIfAny(current)),
				 702);
	UT_ASSERT(!FullTransactionIdIsValid(GetFullTransactionIdForSubTransactionIfAny(0)));
	UT_ASSERT(
		!FullTransactionIdIsValid(GetFullTransactionIdForSubTransactionIfAny(TopSubTransactionId)));
	UT_ASSERT(!FullTransactionIdIsValid(GetFullTransactionIdForSubTransactionIfAny(current + 1)));
	outer->state = TRANS_ABORT;
	UT_ASSERT(!FullTransactionIdIsValid(
		GetFullTransactionIdForSubTransactionIfAny(outer->subTransactionId)));
	outer->state = TRANS_INPROGRESS;
	outer->fullTransactionId = FullTransactionIdFromEpochAndXid(5, FrozenTransactionId);
	UT_ASSERT(!FullTransactionIdIsValid(
		GetFullTransactionIdForSubTransactionIfAny(outer->subTransactionId)));
	CurrentTransactionState->state = TRANS_ABORT;
	UT_ASSERT(!FullTransactionIdIsValid(GetFullTransactionIdForSubTransactionIfAny(current)));
	UT_ASSERT_EQ(xid_assignments, 3);
	UT_ASSERT_EQ(binding_lookups, 0);
}

UT_TEST(test_frame_recheck_does_not_assign_readonly_descendants)
{
	ClusterSavepointOperation ancestor;
	SubTransactionId outer;

	fresh_transaction();
	savepoint("outer");
	outer = CurrentTransactionState->subTransactionId;
	UT_ASSERT(cluster_savepoint_capture_frame_operation(outer, &ancestor));
	UT_ASSERT_EQ(xid_assignments, 2);
	BeginInternalSubTransaction(NULL);
	currentCommandId++;
	UT_ASSERT(cluster_savepoint_operation_matches_frame(&ancestor));
	UT_ASSERT(!cluster_savepoint_operation_matches_current(&ancestor));
	UT_ASSERT_EQ(ancestor.command_id, 11);
	UT_ASSERT(!FullTransactionIdIsValid(GetCurrentFullTransactionIdIfAny()));
	UT_ASSERT_EQ(xid_assignments, 2);
	RollbackAndReleaseCurrentSubTransaction();
	UT_ASSERT(cluster_savepoint_operation_matches_frame(&ancestor));
	UT_ASSERT(cluster_savepoint_operation_matches_current(&ancestor));
	UT_ASSERT_EQ(xid_assignments, 2);
}

UT_TEST(test_frame_capture_refuses_invalid_or_incomplete_scope_before_work)
{
	ClusterSavepointOperation operation;
	SubTransactionId outer;

	for (unsigned leg = 0; leg < 7; leg++) {
		SubTransactionId requested;

		fresh_transaction();
		savepoint("outer");
		requested = CurrentTransactionState->subTransactionId;
		switch (leg) {
		case 0:
			requested = InvalidSubTransactionId;
			break;
		case 1:
			requested = TopSubTransactionId;
			break;
		case 2:
			requested++;
			break;
		case 3:
			parallel_mode = true;
			break;
		case 4:
			cluster_enabled = false;
			break;
		case 5:
			CurrentTransactionState->state = TRANS_ABORT;
			break;
		case 6:
			operation_sequence_floor = PG_UINT64_MAX;
			break;
		}
		memset(&operation, 0xA5, sizeof(operation));
		UT_ASSERT(!cluster_savepoint_capture_frame_operation(requested, &operation));
		UT_ASSERT(operation_empty(&operation));
		UT_ASSERT_EQ(xid_assignments + binding_lookups, 0);
	}
	fresh_transaction();
	savepoint("outer");
	outer = CurrentTransactionState->subTransactionId;
	fail_allocation = 2;
	expect_error = true;
	if (setjmp(error_jump) == 0) {
		BeginInternalSubTransaction(NULL);
		UT_ASSERT(false);
	}
	expect_error = false;
	UT_ASSERT(!cluster_savepoint_capture_frame_operation(outer, &operation));
	UT_ASSERT(operation_empty(&operation));
	UT_ASSERT_EQ(xid_assignments + binding_lookups, 0);
	CleanupSubTransaction();
	UT_ASSERT(cluster_savepoint_capture_frame_operation(outer, &operation));
	UT_ASSERT(!cluster_savepoint_capture_frame_operation(outer, NULL));
}

UT_TEST(test_frame_recheck_retires_released_and_restarted_identity)
{
	ClusterSavepointOperation ancestor;
	ClusterSavepointOperation child;
	ClusterSavepointOperation restarted;
	SubTransactionId outer;

	fresh_transaction();
	savepoint("s");
	outer = CurrentTransactionState->subTransactionId;
	BeginInternalSubTransaction(NULL);
	UT_ASSERT(cluster_savepoint_capture_frame_operation(outer, &ancestor));
	UT_ASSERT(cluster_savepoint_capture_frame_operation(CurrentTransactionState->subTransactionId,
														&child));
	ReleaseCurrentSubTransaction();
	UT_ASSERT(cluster_savepoint_operation_matches_frame(&ancestor));
	UT_ASSERT(!cluster_savepoint_operation_matches_frame(&child));
	RollbackToSavepoint("s");
	CommitTransactionCommand();
	UT_ASSERT(!cluster_savepoint_operation_matches_frame(&ancestor));
	UT_ASSERT(cluster_savepoint_capture_frame_operation(CurrentTransactionState->subTransactionId,
														&restarted));
	UT_ASSERT(restarted.subid != ancestor.subid);
	UT_ASSERT(U64FromFullTransactionId(restarted.operation_fxid)
			  > U64FromFullTransactionId(child.operation_fxid));
	UT_ASSERT_EQ(restarted.operation_sequence, 3);
	UT_ASSERT(!cluster_savepoint_operation_matches_frame(&ancestor));
	UT_ASSERT(!cluster_savepoint_operation_matches_frame(&child));
}

UT_TEST(test_frame_recheck_refuses_full_identity_and_owner_drift)
{
	ClusterSavepointOperation operation;
	ClusterSavepointOperation changed;
	TransactionState outer;

	fresh_transaction();
	savepoint("outer");
	outer = CurrentTransactionState;
	BeginInternalSubTransaction(NULL);
	UT_ASSERT(cluster_savepoint_capture_frame_operation(outer->subTransactionId, &operation));
	/* Metadata alone must not authorize a frame absent from the native chain. */
	CurrentTransactionState->parent = &top;
	UT_ASSERT(!cluster_savepoint_operation_matches_frame(&operation));
	UT_ASSERT(!cluster_savepoint_capture_frame_operation(outer->subTransactionId, &changed));
	UT_ASSERT(operation_empty(&changed));
	CurrentTransactionState->parent = outer;
	outer->fullTransactionId = FullTransactionIdFromEpochAndXid(6, 701);
	UT_ASSERT(!cluster_savepoint_operation_matches_frame(&operation));
	outer->fullTransactionId = operation.operation_fxid;
	XactTopFullTransactionId = FullTransactionIdFromEpochAndXid(6, 700);
	UT_ASSERT(!cluster_savepoint_operation_matches_frame(&operation));
	XactTopFullTransactionId = operation.top_fxid;
	for (int variant = 1; variant <= 12; variant++) {
		bad_binding = variant;
		UT_ASSERT(!cluster_savepoint_operation_matches_frame(&operation));
	}
	bad_binding = 0;
	for (unsigned leg = 0; leg < 6; leg++) {
		changed = operation;
		switch (leg) {
		case 0:
			changed.subid = TopSubTransactionId;
			break;
		case 1:
			changed.subid = CurrentTransactionState->subTransactionId;
			break;
		case 2:
			changed.operation_sequence = 0;
			break;
		case 3:
			changed.operation_sequence += 100;
			break;
		case 4:
			changed.command_id = InvalidCommandId;
			break;
		case 5:
			changed.operation_fxid = changed.top_fxid;
			break;
		}
		UT_ASSERT(!cluster_savepoint_operation_matches_frame(&changed));
	}
	UT_ASSERT(!cluster_savepoint_operation_matches_frame(NULL));
	UT_ASSERT(cluster_savepoint_operation_matches_frame(&operation));
	UT_ASSERT_EQ(xid_assignments, 3);
}

UT_TEST(test_frame_sequence_exhaustion_keeps_ancestor_proof_without_wrapping)
{
	ClusterSavepointOperation operation;
	ClusterSavepointOperation refused;
	SubTransactionId outer;
	int lookups;

	fresh_transaction();
	savepoint("outer");
	outer = CurrentTransactionState->subTransactionId;
	BeginInternalSubTransaction(NULL);
	operation_sequence_floor = PG_UINT64_MAX - 1;
	UT_ASSERT(cluster_savepoint_capture_frame_operation(outer, &operation));
	UT_ASSERT_EQ(operation.operation_sequence, PG_UINT64_MAX);
	ReleaseCurrentSubTransaction();
	UT_ASSERT(cluster_savepoint_operation_matches_frame(&operation));
	lookups = binding_lookups;
	UT_ASSERT(!cluster_savepoint_capture_frame_operation(outer, &refused));
	UT_ASSERT(operation_empty(&refused));
	UT_ASSERT_EQ(binding_lookups, lookups);
	UT_ASSERT_EQ(xid_assignments, 3);
	fresh_transaction();
	savepoint("new");
	UT_ASSERT(cluster_savepoint_capture_frame_operation(CurrentTransactionState->subTransactionId,
														&operation));
	UT_ASSERT_EQ(operation.operation_sequence, 1);
}

int
main(void)
{
	UT_PLAN(30);
	UT_RUN(test_frame_capture_keeps_ancestor_and_data_writer_distinct);
	UT_RUN(test_frame_capture_keeps_replaced_and_anonymous_ancestors);
	UT_RUN(test_native_frame_lookup_never_assigns_or_guesses);
	UT_RUN(test_frame_recheck_does_not_assign_readonly_descendants);
	UT_RUN(test_frame_capture_refuses_invalid_or_incomplete_scope_before_work);
	UT_RUN(test_frame_recheck_retires_released_and_restarted_identity);
	UT_RUN(test_frame_recheck_refuses_full_identity_and_owner_drift);
	UT_RUN(test_frame_sequence_exhaustion_keeps_ancestor_proof_without_wrapping);
	UT_RUN(test_capture_top_keeps_full_writer_and_exact_canonical);
	UT_RUN(test_capture_child_allocates_native_parents_but_looks_up_only_top);
	UT_RUN(test_operation_sequence_survives_release_and_rollback_restart);
	UT_RUN(test_capture_counter_resets_at_top_end_without_frames);
	UT_RUN(test_capture_refuses_invalid_entry_without_xid_or_binding_work);
	UT_RUN(test_capture_refuses_missing_or_malformed_published_owner);
	UT_RUN(test_capture_rejects_alias_child_and_invalid_command_identity);
	UT_RUN(test_operation_recheck_never_assigns_and_keeps_captured_command);
	UT_RUN(test_operation_recheck_rejects_full_epoch_owner_and_scope_drift);
	UT_RUN(test_operation_sequence_exhaustion_cannot_wrap_or_allocate);
	UT_RUN(test_same_name_replaces_name_but_retains_parent_frame);
	UT_RUN(test_release_does_not_reveal_replaced_name);
	UT_RUN(test_earlier_name_and_later_points);
	UT_RUN(test_restart_live_then_replace_again);
	UT_RUN(test_restart_aborted_then_replace_again);
	UT_RUN(test_anonymous_release_and_exception_preserve_parent);
	UT_RUN(test_scope_boundary_keeps_outer_name);
	UT_RUN(test_disabled_mode_keeps_native_names);
	UT_RUN(test_failed_native_frame_keeps_old_point);
	UT_RUN(test_failed_metadata_keeps_parent_frame);
	UT_RUN(test_failed_name_keeps_old_point);
	UT_RUN(test_top_end_retires_frame_memory);
	fresh_transaction();
	UT_DONE();
	return ut_failed_count != 0;
}
