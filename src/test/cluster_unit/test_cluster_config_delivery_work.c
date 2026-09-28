/*-------------------------------------------------------------------------
 *
 * test_cluster_config_delivery_work.c
 *    Execute native delivery/retry with retained asynchronous owner inputs.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_config_delivery_work.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "nodes/memnodes.h"
#include "../../backend/cluster/cluster_shared_config_delivery.c"
#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

/* Real delivery and retry bodies; allocation, native policy/application and
 * owner observations are boundaries. Native TAP covers the actual policy and
 * assignments; PGSA/GCS/CR/cleaner tests cover the actual owner lifetimes.
 * These fixture roles are not a native-process or distributed certificate. */
bool IsUnderPostmaster = true, IsPostmasterEnvironment = true;
int MyProcPid = 101;
pid_t PostmasterPid = 100;
BackendType MyBackendType = B_LMS;
AuxProcType MyAuxProcType = LmsProcess;
volatile sig_atomic_t ConfigReloadPending;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
MemoryContext CurrentMemoryContext;
ResourceOwner CurrentResourceOwner;
static MemoryContextData parent_context, child_context;
static char test_owner;
static ConfigDeliveryFamily family;
static bool transaction, transaction_block, admission, cr_idle, cleaner_idle;
static bool session_locks;
static bool common_change, available, classification_ok;
static ClusterNormalStopPollResult gcs_result;
static uint64 target, consumed;
static unsigned assignments, reads, gcs_polls, cr_polls, cleaner_polls;
static unsigned contexts, deletes, owners, releases;

/* Native producer lifecycle is exercised by the real-postmaster TAP test.
 * This unit isolates the retained asynchronous service delivery boundary. */
void
cluster_shared_config_use_enter(void)
{}

MemoryContext
AllocSetContextCreateInternal(MemoryContext parent, const char *name, Size minsize, Size initsize,
							  Size maxsize)
{
	UT_ASSERT(parent == &parent_context);
	contexts++;
	return &child_context;
}
void
MemoryContextDelete(MemoryContext context)
{
	UT_ASSERT(context == &child_context);
	deletes++;
}
ResourceOwner
ResourceOwnerCreate(ResourceOwner parent, const char *name)
{
	UT_ASSERT(parent == NULL);
	owners++;
	return (ResourceOwner)&test_owner;
}
void
ResourceOwnerRelease(ResourceOwner owner, ResourceReleasePhase phase, bool commit, bool top)
{
	UT_ASSERT(owner == (ResourceOwner)&test_owner && top);
	releases++;
}
void
ResourceOwnerDelete(ResourceOwner owner)
{
	UT_ASSERT(owner == (ResourceOwner)&test_owner);
}
bool
IsTransactionState(void)
{
	return transaction;
}
bool
IsTransactionOrTransactionBlock(void)
{
	return transaction || transaction_block;
}
bool
cluster_shared_config_use_session_owned(void)
{
	return session_locks;
}
bool
cluster_semantic_activation_backend_has_admission(void)
{
	return admission;
}
bool
cluster_cr_server_r4_worker0_drained(void)
{
	cr_polls++;
	return cr_idle;
}
bool
cluster_ctrc_cleaner_local_idle(void)
{
	cleaner_polls++;
	return cleaner_idle;
}
ClusterNormalStopPollResult
cluster_gcs_block_normal_stop_local_poll(int *slot, const char **reason)
{
	UT_ASSERT(AmLmsProcess() || AmLmsWorkerProcess() || AmLmonProcess());
	gcs_polls++;
	*slot = -1;
	*reason = "fixture";
	return gcs_result;
}
bool
cluster_shared_config_delivery_slot_read(ClusterSharedConfigDeliverySlot *slot, uint64 generation,
										 ClusterSharedConfigRef *ref,
										 ClusterSharedConfigImage *image)
{
	UT_ASSERT(slot == &family.accepted && generation == 1);
	reads++;
	if (!available)
		return false;
	memset(ref, 0, sizeof(*ref));
	ref->identity.generation = target;
	image->bytes = "actual delivery, isolated image boundary";
	image->len = strlen(image->bytes);
	return true;
}
ClusterControlRootResult
cluster_shared_config_process_reload_needs_idle(const char *bytes, size_t len,
												const ClusterSharedConfigRef *ref, bool *needs_idle,
												ClusterSharedConfigPolicyReport *report)
{
	UT_ASSERT(ref->identity.generation == target && strlen(bytes) == len);
	*needs_idle = common_change;
	return classification_ok ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
							 : CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
}
ClusterControlRootResult
cluster_shared_config_process_reload(const char *bytes, size_t len,
									 const ClusterSharedConfigRef *ref,
									 ClusterSharedConfigProcess *out,
									 ClusterSharedConfigPolicyReport *report)
{
	UT_ASSERT(ref->identity.generation == target && strlen(bytes) == len);
	assignments++;
	consumed = target;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
bool
cluster_shared_config_process_copy(ClusterSharedConfigProcess *out, ClusterSharedConfigImage *image)
{
	abort(); /* Child delivery must never use parent publication. */
}
bool
cluster_shared_config_delivery_slot_write(ClusterSharedConfigDeliverySlot *slot, uint64 generation,
										  int32 pid, const ClusterSharedConfigRef *ref,
										  const char *bytes, size_t len)
{
	abort();
}
void
FlushErrorState(void)
{
	abort();
}
void
pg_re_throw(void)
{
	abort();
}
int
errmsg(const char *fmt, ...)
{
	abort();
}
int
errcode(int code)
{
	abort();
}
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# %s %s:%d\n", condition, file, line);
	abort();
}

static void
reset(void)
{
	memset(&family, 0, sizeof(family));
	family.postmaster_pid = PostmasterPid;
	pg_atomic_init_u64(&family.generation, 1);
	delivery_family = &family;
	delivery_generation = 1;
	delivery_work_depth = 0;
	delivery_waiting_for_idle = false;
	parent_context.type = child_context.type = T_AllocSetContext;
	CurrentMemoryContext = &parent_context;
	CurrentResourceOwner = NULL;
	MyBackendType = B_LMS;
	MyAuxProcType = LmsProcess;
	transaction = transaction_block = admission = session_locks = false;
	cr_idle = cleaner_idle = common_change = available = classification_ok = true;
	gcs_result = CLUSTER_NORMAL_STOP_READY;
	target = 2;
	consumed = 1;
	assignments = reads = gcs_polls = cr_polls = cleaner_polls = 0;
	contexts = deletes = owners = releases = 0;
	ConfigReloadPending = false;
}

static void
expect_deferred(void)
{
	UT_ASSERT(!cluster_shared_config_delivery_reload());
	UT_ASSERT_EQ(assignments, 0);
	UT_ASSERT_EQ(consumed, 1);
	UT_ASSERT(ConfigReloadPending && delivery_waiting_for_idle);
	UT_ASSERT_EQ(contexts, deletes);
	UT_ASSERT_EQ(releases, owners * 3);
	UT_ASSERT(CurrentMemoryContext == &parent_context && CurrentResourceOwner == NULL);
}

UT_TEST(retained_admission_without_transaction_or_command)
{
	reset();
	admission = true;
	expect_deferred();
	cluster_shared_config_delivery_retry_idle();
	UT_ASSERT_EQ(reads, 1); /* No repeated allocation or target read while owned. */
	UT_ASSERT_EQ(assignments, 0);
	UT_ASSERT(delivery_waiting_for_idle);
	admission = false;
	target = 7;
	cluster_shared_config_delivery_retry_idle();
	UT_ASSERT_EQ(assignments, 1);
	UT_ASSERT_EQ(consumed, 7); /* Reread newest accepted image, not old target2. */
	UT_ASSERT(!delivery_waiting_for_idle);
}
UT_TEST(gcs_private_work_without_admission)
{
	AuxProcType roles[] = { LmonProcess, LmsProcess, LmsWorker1Process, LmsWorker7Process };
	for (size_t i = 0; i < lengthof(roles); i++) {
		reset();
		MyAuxProcType = roles[i];
		gcs_result = CLUSTER_NORMAL_STOP_PENDING;
		expect_deferred();
		gcs_result = CLUSTER_NORMAL_STOP_READY;
		cluster_shared_config_delivery_retry_idle();
		UT_ASSERT_EQ(consumed, 2);
	}
}
UT_TEST(invalid_gcs_owner_is_not_idle)
{
	reset();
	gcs_result = CLUSTER_NORMAL_STOP_INVALID;
	expect_deferred();
	cluster_shared_config_delivery_retry_idle();
	UT_ASSERT_EQ(assignments, 0);
}
UT_TEST(cr_context_or_terminal_shipping_is_not_idle)
{
	reset();
	cr_idle = false;
	expect_deferred();
	cr_idle = true;
	cluster_shared_config_delivery_retry_idle();
	UT_ASSERT_EQ(consumed, 2);
}
UT_TEST(cleaner_batch_remains_owned)
{
	for (int worker = 0; worker < 8; worker++) {
		reset();
		MyAuxProcType = ClusterUndoCleanerTypeForWorker(worker);
		cleaner_idle = false;
		expect_deferred();
		cleaner_idle = true;
		cluster_shared_config_delivery_retry_idle();
		UT_ASSERT_EQ(consumed, 2);
	}
}
UT_TEST(incomplete_release_does_not_apply)
{
	reset();
	admission = true;
	gcs_result = CLUSTER_NORMAL_STOP_PENDING;
	expect_deferred();
	admission = false;
	cluster_shared_config_delivery_retry_idle();
	UT_ASSERT_EQ(assignments, 0);
	UT_ASSERT(delivery_waiting_for_idle);
}
UT_TEST(default_only_reload_is_not_blocked)
{
	reset();
	common_change = false;
	admission = true;
	gcs_result = CLUSTER_NORMAL_STOP_PENDING;
	UT_ASSERT(cluster_shared_config_delivery_reload());
	UT_ASSERT_EQ(consumed, 2);
	UT_ASSERT(!delivery_waiting_for_idle);
}
UT_TEST(other_native_role_does_not_wait_for_unrelated_owner)
{
	reset();
	MyAuxProcType = WalWriterProcess;
	cr_idle = cleaner_idle = false;
	gcs_result = CLUSTER_NORMAL_STOP_INVALID;
	UT_ASSERT(cluster_shared_config_delivery_reload());
	UT_ASSERT_EQ(gcs_polls + cr_polls + cleaner_polls, 0);
}
UT_TEST(other_role_still_retains_own_admission)
{
	reset();
	MyAuxProcType = WalWriterProcess;
	admission = true;
	expect_deferred();
}
UT_TEST(transaction_and_command_boundary_retained)
{
	reset();
	transaction = true;
	expect_deferred();
	transaction = false;
	delivery_work_depth = 1;
	cluster_shared_config_delivery_retry_idle();
	UT_ASSERT_EQ(assignments, 0);
	delivery_work_depth = 0;
	cluster_shared_config_delivery_retry_idle();
	UT_ASSERT_EQ(consumed, 2);
}
UT_TEST(missing_new_image_retains_retry_owner)
{
	reset();
	admission = true;
	expect_deferred();
	admission = false;
	available = false;
	cluster_shared_config_delivery_retry_idle();
	UT_ASSERT(delivery_waiting_for_idle && assignments == 0);
	available = true;
	cluster_shared_config_delivery_retry_idle();
	UT_ASSERT_EQ(consumed, 2);
}
UT_TEST(policy_failure_cannot_apply_while_owned)
{
	reset();
	classification_ok = false;
	admission = true;
	UT_ASSERT(!cluster_shared_config_delivery_reload());
	UT_ASSERT_EQ(assignments, 0);
}
UT_TEST(session_owner_survives_native_idle)
{
	reset();
	MyBackendType = B_BACKEND;
	MyAuxProcType = NotAnAuxProcess;
	session_locks = true;
	expect_deferred();
	cluster_shared_config_delivery_retry_idle();
	UT_ASSERT_EQ(reads, 1);
	UT_ASSERT_EQ(assignments, 0);
	session_locks = false;
	target = 9;
	cluster_shared_config_delivery_retry_idle();
	UT_ASSERT_EQ(consumed, 9);
}
UT_TEST(session_lock_does_not_hold_default_only)
{
	reset();
	session_locks = true;
	common_change = false;
	UT_ASSERT(cluster_shared_config_delivery_reload());
	UT_ASSERT_EQ(consumed, 2);
}
int
main(void)
{
	UT_PLAN(14);
	UT_RUN(retained_admission_without_transaction_or_command);
	UT_RUN(gcs_private_work_without_admission);
	UT_RUN(invalid_gcs_owner_is_not_idle);
	UT_RUN(cr_context_or_terminal_shipping_is_not_idle);
	UT_RUN(cleaner_batch_remains_owned);
	UT_RUN(incomplete_release_does_not_apply);
	UT_RUN(default_only_reload_is_not_blocked);
	UT_RUN(other_native_role_does_not_wait_for_unrelated_owner);
	UT_RUN(other_role_still_retains_own_admission);
	UT_RUN(transaction_and_command_boundary_retained);
	UT_RUN(missing_new_image_retains_retry_owner);
	UT_RUN(policy_failure_cannot_apply_while_owned);
	UT_RUN(session_owner_survives_native_idle);
	UT_RUN(session_lock_does_not_hold_default_only);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
