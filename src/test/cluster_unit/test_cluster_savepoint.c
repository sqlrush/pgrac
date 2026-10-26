/* Execute actual native frame/name APIs with narrow backend boundaries.
 * This fixture does not perform DML, WAL, TT or distributed recovery.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"
#include <setjmp.h>
#include "access/xact.h"
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
MemoryContext TopTransactionContext = (MemoryContext) 1;
MemoryContext CurTransactionContext = (MemoryContext) 1;
MemoryContext CurrentMemoryContext = (MemoryContext) 1;
ResourceOwner CurTransactionResourceOwner;
ResourceOwner CurrentResourceOwner;
bool XactReadOnly;
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
	return false;
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
int
main(void)
{
	UT_PLAN(12);
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
