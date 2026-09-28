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
#include "storage/proc.h"
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
static PGPROC background_proc;
PGPROC *MyProc = &background_proc;
static ClusterSharedConfigRegistration background_actual;
static bool registration_available, registration_changed;

bool
cluster_shared_config_registration_read(ClusterSharedConfigSlot *slot,
										ClusterSharedConfigRegistration *out)
{
	UT_ASSERT(slot == &background_proc.cluster_config);
	*out = background_actual;
	if (registration_changed)
		pg_atomic_fetch_add_u64(&background_proc.cluster_config.sequence, 2);
	return registration_available;
}

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
	cluster_config_use_gate_init(&family.cleaner_producer);
	pg_atomic_init_u32(&family.cleaner_failed, 0);
	delivery_family = &family;
	delivery_generation = 1;
	delivery_work_depth = 0;
	delivery_waiting_for_idle = false;
	delivery_cleaner_owned = false;
	delivery_background_owned = CLUSTER_CONFIG_BACKGROUND_COUNT;
	for (int i = 0; i < CLUSTER_CONFIG_BACKGROUND_COUNT; i++) {
		cluster_config_use_gate_init(&family.background[i]);
		pg_atomic_init_u32(&family.background_failed[i], 0);
	}
	memset(&background_proc, 0, sizeof(background_proc));
	pg_atomic_init_u64(&background_proc.cluster_config.sequence, 2);
	memset(&background_actual, 0, sizeof(background_actual));
	registration_available = true;
	registration_changed = false;
	MyProc = &background_proc;
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
	UT_ASSERT(!cluster_shared_config_delivery_retry_idle());
	UT_ASSERT_EQ(reads, 1); /* No repeated allocation or target read while owned. */
	UT_ASSERT_EQ(assignments, 0);
	UT_ASSERT(delivery_waiting_for_idle);
	admission = false;
	target = 7;
	UT_ASSERT(cluster_shared_config_delivery_retry_idle());
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
UT_TEST(cleaner_producer_cut_retains_exact_original_pass)
{
	uint32 cut;
	bool failed;
	ClusterConfigUseGate *gate;
	reset();
	MyBackendType = B_UNDO_CLEANER;
	MyAuxProcType = UndoCleanerProcess;
	gate = cluster_shared_config_delivery_cleaner_gate(&failed);
	UT_ASSERT(gate != NULL && !failed);
	UT_ASSERT(cluster_shared_config_cleaner_begin());
	UT_ASSERT(!cluster_shared_config_cleaner_begin());
	UT_ASSERT_EQ(cluster_config_use_gate_read(gate).owners, 1);
	UT_ASSERT(cluster_config_use_gate_close(gate, &cut));
	UT_ASSERT(!cluster_config_use_gate_open(gate, cut));
	expect_deferred();
	cluster_shared_config_cleaner_end(true);
	UT_ASSERT_EQ(cluster_config_use_gate_read(gate).owners, 0);
	UT_ASSERT(!cluster_shared_config_cleaner_begin());
	cluster_shared_config_delivery_retry_idle();
	UT_ASSERT_EQ(consumed, 2);
	UT_ASSERT(cluster_config_use_gate_open(gate, cut));
	UT_ASSERT(cluster_shared_config_cleaner_begin());
	cluster_shared_config_cleaner_end(true);
	UT_ASSERT_EQ(cluster_config_use_gate_read(gate).owners, 0);
}

UT_TEST(cleaner_producer_failure_is_not_retirement)
{
	uint32 cut;
	bool failed;
	ClusterConfigUseGate *gate;
	reset();
	MyBackendType = B_UNDO_CLEANER;
	MyAuxProcType = UndoCleanerProcess;
	UT_ASSERT(cluster_shared_config_cleaner_begin());
	cluster_shared_config_cleaner_end(false);
	gate = cluster_shared_config_delivery_cleaner_gate(&failed);
	UT_ASSERT(gate != NULL && failed);
	UT_ASSERT_EQ(cluster_config_use_gate_read(gate).owners, 1);
	cluster_shared_config_cleaner_end(true);
	UT_ASSERT_EQ(cluster_config_use_gate_read(gate).owners, 1);
	UT_ASSERT(cluster_config_use_gate_close(gate, &cut));
	UT_ASSERT(!cluster_config_use_gate_open(gate, cut));
	UT_ASSERT(!cluster_shared_config_cleaner_begin());
	expect_deferred();
	/* A replacement process's empty private scope is not original retirement. */
	delivery_cleaner_owned = false;
	UT_ASSERT(!cluster_shared_config_cleaner_begin());
	UT_ASSERT_EQ(cluster_config_use_gate_read(gate).owners, 1);
}

UT_TEST(cleaner_producer_requires_actual_family_and_worker)
{
	bool failed;
	reset();
	UT_ASSERT(!cluster_shared_config_cleaner_begin());
	MyBackendType = B_UNDO_CLEANER;
	MyAuxProcType = ClusterUndoCleanerTypeForWorker(1);
	UT_ASSERT(!cluster_shared_config_cleaner_begin());
	MyAuxProcType = UndoCleanerProcess;
	pg_atomic_write_u64(&family.generation, 2);
	UT_ASSERT(cluster_shared_config_delivery_cleaner_gate(&failed) == NULL && failed);
	UT_ASSERT(!cluster_shared_config_cleaner_begin());
	delivery_family = NULL;
	UT_ASSERT(cluster_shared_config_cleaner_begin());
	cluster_shared_config_cleaner_end(true);
	UT_ASSERT(!delivery_cleaner_owned);
}

static void
background_role(int kind)
{
	BackendType roles[] = { B_CHECKPOINTER, B_BG_WRITER, B_WAL_WRITER };
	AuxProcType auxiliary[] = { CheckpointerProcess, BgWriterProcess, WalWriterProcess };
	MyBackendType = roles[kind];
	MyAuxProcType = auxiliary[kind];
}

UT_TEST(background_close_preserves_original_owner)
{
	for (int i = 0; i < CLUSTER_CONFIG_BACKGROUND_COUNT; i++) {
		uint32 cut;
		bool failed;
		ClusterConfigUseGate *gate;
		reset();
		background_role(i);
		gate = cluster_shared_config_delivery_background_gate(i, &failed);
		UT_ASSERT(gate != NULL && !failed);
		UT_ASSERT(cluster_shared_config_background_begin());
		UT_ASSERT(!cluster_shared_config_background_begin());
		UT_ASSERT_EQ(cluster_config_use_gate_read(gate).owners, 1);
		UT_ASSERT(cluster_config_use_gate_close(gate, &cut));
		UT_ASSERT(!cluster_config_use_gate_open(gate, cut));
		expect_deferred();
		cluster_shared_config_background_end(true);
		UT_ASSERT_EQ(cluster_config_use_gate_read(gate).owners, 0);
		UT_ASSERT(!cluster_shared_config_background_begin());
		UT_ASSERT(cluster_shared_config_delivery_retry_idle());
		UT_ASSERT_EQ(consumed, 2);
		UT_ASSERT(cluster_config_use_gate_open(gate, cut));
		UT_ASSERT(cluster_shared_config_background_begin());
		cluster_shared_config_background_end(true);
		UT_ASSERT_EQ(cluster_config_use_gate_read(gate).owners, 0);
	}
}

UT_TEST(background_error_is_not_retirement_or_respawn)
{
	for (int i = 0; i < CLUSTER_CONFIG_BACKGROUND_COUNT; i++) {
		bool failed;
		uint32 cut;
		ClusterConfigUseGate *gate;
		reset();
		background_role(i);
		UT_ASSERT(cluster_shared_config_background_begin());
		cluster_shared_config_background_end(false);
		gate = cluster_shared_config_delivery_background_gate(i, &failed);
		UT_ASSERT(gate != NULL && failed);
		cluster_shared_config_background_end(true);
		UT_ASSERT_EQ(cluster_config_use_gate_read(gate).owners, 1);
		UT_ASSERT(cluster_config_use_gate_close(gate, &cut));
		UT_ASSERT(!cluster_config_use_gate_open(gate, cut));
		UT_ASSERT(!cluster_shared_config_background_begin());
		expect_deferred();
		/* Empty process-local state is not the old worker's completion. */
		delivery_background_owned = CLUSTER_CONFIG_BACKGROUND_COUNT;
		UT_ASSERT(!cluster_shared_config_background_begin());
		/* A different original role must still be able to finish its work. */
		background_role((i + 1) % CLUSTER_CONFIG_BACKGROUND_COUNT);
		UT_ASSERT(cluster_shared_config_background_begin());
		cluster_shared_config_background_end(true);
		UT_ASSERT_EQ(cluster_config_use_gate_read(gate).owners, 1);
	}
}

UT_TEST(background_actual_role_and_family_required)
{
	bool failed;
	reset();
	UT_ASSERT(!cluster_shared_config_background_begin());
	for (int i = 0; i < CLUSTER_CONFIG_BACKGROUND_COUNT; i++) {
		background_role(i);
		MyAuxProcType = NotAnAuxProcess;
		UT_ASSERT(!cluster_shared_config_background_begin());
	}
	UT_ASSERT(cluster_shared_config_delivery_background_gate(-1, &failed) == NULL && failed);
	UT_ASSERT(
		cluster_shared_config_delivery_background_gate(CLUSTER_CONFIG_BACKGROUND_COUNT, &failed)
			== NULL
		&& failed);
	background_role(0);
	pg_atomic_write_u64(&family.generation, 2);
	UT_ASSERT(!cluster_shared_config_background_begin());
	UT_ASSERT(cluster_shared_config_delivery_background_target(0) == NULL);
	delivery_family = NULL;
	UT_ASSERT(cluster_shared_config_background_begin());
	cluster_shared_config_background_end(true);
	UT_ASSERT_EQ(delivery_background_owned, CLUSTER_CONFIG_BACKGROUND_COUNT);
}

static void
background_bind(int kind)
{
	ClusterConfigUseGate *gate;
	ClusterConfigUseTarget *bound;
	bool failed;
	uint32 cut;
	background_role(kind);
	background_actual.pid = MyProcPid;
	background_actual.role = MyBackendType;
	background_actual.registration = 2;
	background_actual.observed = true;
	background_actual.process.applier_pid = MyProcPid;
	background_actual.process.ref.identity.system_identifier = 11;
	background_actual.process.ref.identity.database_incarnation = 12;
	background_actual.process.ref.identity.generation = 2;
	background_actual.process.ref.identity.configured[0] = 1;
	background_actual.process.ref.identity.storage_uuid[0] = 1;
	background_actual.process.ref.identity.authority_uuid[0] = 2;
	background_actual.process.ref.sha256[0] = 3;
	background_actual.common.version = CLUSTER_SHARED_CONFIG_COMMON_VERSION;
	background_actual.common.static_entries = 1;
	background_actual.common.dynamic_entries = 1;
	background_actual.common.static_sha256[0] = 4;
	background_actual.common.dynamic_sha256[0] = 5;
	gate = cluster_shared_config_delivery_background_gate(kind, &failed);
	bound = cluster_shared_config_delivery_background_target(kind);
	UT_ASSERT(gate != NULL && bound != NULL && !failed);
	UT_ASSERT(cluster_config_use_gate_close(gate, &cut));
	UT_ASSERT(cluster_config_use_target_bind(gate, bound, cut, 0, &background_actual.process.ref,
											 &background_actual.common));
	UT_ASSERT(cluster_config_use_gate_open(gate, cut));
}

UT_TEST(background_bound_target_uses_actual_native_values)
{
	for (int i = 0; i < CLUSTER_CONFIG_BACKGROUND_COUNT; i++) {
		reset();
		background_bind(i);
		UT_ASSERT(cluster_shared_config_background_begin());
		cluster_shared_config_background_end(true);
		background_actual.common.dynamic_sha256[0]++;
		UT_ASSERT(!cluster_shared_config_background_begin());
		UT_ASSERT_EQ(cluster_config_use_gate_read(&family.background[i]).owners, 0);
		UT_ASSERT(ConfigReloadPending && delivery_waiting_for_idle);
		UT_ASSERT_EQ(assignments, 0); /* No assignment inside begin/end. */
		background_actual.common.dynamic_sha256[0]--;
		UT_ASSERT(cluster_shared_config_background_begin());
		cluster_shared_config_background_end(true);
	}
}

UT_TEST(background_bound_target_rejects_unproven_registration)
{
	for (int bad = 0; bad < 7; bad++) {
		reset();
		background_bind(0);
		switch (bad) {
		case 0:
			registration_available = false;
			break;
		case 1:
			registration_changed = true;
			break;
		case 2:
			background_actual.pid++;
			break;
		case 3:
			background_actual.role = B_BACKEND;
			break;
		case 4:
			background_actual.process.failed = true;
			break;
		case 5:
			background_actual.process.parallel_snapshot = true;
			break;
		case 6:
			MyProc = NULL;
			break;
		}
		UT_ASSERT(!cluster_shared_config_background_begin());
		UT_ASSERT_EQ(cluster_config_use_gate_read(&family.background[0]).owners, 0);
		UT_ASSERT_EQ(assignments, 0);
	}
}

UT_TEST(background_default_only_reload_is_allowed)
{
	reset();
	background_role(1);
	UT_ASSERT(cluster_shared_config_background_begin());
	common_change = false;
	UT_ASSERT(cluster_shared_config_delivery_reload());
	UT_ASSERT_EQ(consumed, 2);
	cluster_shared_config_background_end(true);
}

int
main(void)
{
	UT_PLAN(23);
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
	UT_RUN(cleaner_producer_cut_retains_exact_original_pass);
	UT_RUN(cleaner_producer_failure_is_not_retirement);
	UT_RUN(cleaner_producer_requires_actual_family_and_worker);
	UT_RUN(background_close_preserves_original_owner);
	UT_RUN(background_error_is_not_retirement_or_respawn);
	UT_RUN(background_actual_role_and_family_required);
	UT_RUN(background_bound_target_uses_actual_native_values);
	UT_RUN(background_bound_target_rejects_unproven_registration);
	UT_RUN(background_default_only_reload_is_allowed);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
