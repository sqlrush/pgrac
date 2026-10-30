/*-------------------------------------------------------------------------
 *
 * test_cluster_wal_retention.c
 *	  RF-ROOT P6 / STOP-05 WAL-retention foundation tests.
 *
 * This first RED-to-GREEN slice covers only the process-local, non-authority
 * foundation used by every later reuse guard: exact WAL identity, half-open
 * intervals, and conservative folding of already validated control roots.
 * No destructive filesystem caller is exercised or authorized here.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog_internal.h"
#include "cluster/cluster_control_root.h"
#include "cluster/cluster_external_fence.h"
#include "cluster/cluster_ir.h"
#include "cluster/cluster_lock_acquire.h"
#include "cluster/cluster_lock_owner.h"
#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_wal_retention.h"
#include "cluster/cluster_wal_source.h"
#include "common/cryptohash.h"
#include "miscadmin.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "utils/resowner.h"

#include "unit_test.h"
#include "../../backend/cluster/cluster_control_root_private.h"
#include "../../backend/cluster/cluster_wal_inputs_job_private.h"

UT_DEFINE_GLOBALS();

/* This legacy suite creates only non-job pins. Job custody, memory and ACK
 * progression run with real bodies in test_cluster_wal_inputs_job_pin.
 * Reject every unsupported job entry rather than supplying ownership proof. */
Latch *MyLatch;

void
SetLatch(Latch *latch pg_attribute_unused())
{
	abort();
}

void *
MemoryContextAllocZero(MemoryContext context pg_attribute_unused(), Size size pg_attribute_unused())
{
	abort();
}

MemoryContext
cluster_wal_inputs_job_context_v1(ClusterWalInputsJobV1 *job pg_attribute_unused())
{
	abort();
}

bool
cluster_wal_inputs_job_pin_bind_v1(ClusterWalInputsJobV1 *job pg_attribute_unused(),
								   ClusterWalReadPinV1 *pin pg_attribute_unused(),
								   uint64 generation pg_attribute_unused())
{
	abort();
}

bool
cluster_wal_inputs_job_pin_matches_v1(ClusterWalInputsJobV1 *job pg_attribute_unused(),
									  ClusterWalReadPinV1 *pin pg_attribute_unused(),
									  uint64 generation pg_attribute_unused(),
									  bool owner_cleanup pg_attribute_unused())
{
	abort();
}

bool
cluster_wal_inputs_job_pin_detach_v1(ClusterWalInputsJobV1 *job pg_attribute_unused(),
									 ClusterWalReadPinV1 *pin pg_attribute_unused(),
									 uint64 generation pg_attribute_unused())
{
	abort();
}

bool
cluster_wal_inputs_job_pin_cleanup_v1(ClusterWalInputsJobV1 *job pg_attribute_unused(),
									  ClusterWalReadPinV1 *pin pg_attribute_unused(),
									  uint64 generation pg_attribute_unused(),
									  bool error pg_attribute_unused(),
									  bool owner_cleanup pg_attribute_unused())
{
	abort();
}

bool
cluster_wal_inputs_scope_job_suspended_v1(ClusterWalInputsJobV1 *job pg_attribute_unused())
{
	abort();
}

ClusterLockAcquireResult
cluster_lock_owner_request_acquire_retained(
	ClusterLockAcquireRequest *request pg_attribute_unused(),
	ClusterControlNativeFinish finish pg_attribute_unused(), void *argument pg_attribute_unused())
{
	abort();
}

ClusterLockAcquireResult
cluster_lock_owner_request_release_poll(
	const ClusterLockAcquireRequest *request pg_attribute_unused())
{
	abort();
}

#define TEST_WAL_SEG_SIZE (16 * 1024 * 1024)
#define UT_ASSERT_TRUE(condition) UT_ASSERT(condition)
#define UT_ASSERT_FALSE(condition) UT_ASSERT(!(condition))

int MyProcPid = 42;
int wal_segment_size = TEST_WAL_SEG_SIZE;
char *cluster_wal_threads_dir;
bool cluster_shared_config;
bool cluster_trace_control_config;
volatile uint32 CritSectionCount;
BackendType MyBackendType = B_INVALID;
bool proc_exit_inprogress;
static bool fake_control_owner_current = true;
static LOCKMODE fake_control_owner_mode = NoLock;
static uint16 fake_control_owner_stale_thread;
bool
cluster_lock_owner_request_usable(const ClusterLockAcquireRequest *request)
{
	return fake_control_owner_current && request->resid.field1 != fake_control_owner_stale_thread
		   && (fake_control_owner_mode == NoLock || request->lockmode == fake_control_owner_mode);
}
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

/* PGRAC: this legacy geometry/guard suite retains its S1-S6 fixture boundary.
 * Actual PRE2 ownership/native unwind is test_cluster_control_walr, not these
 * scalar fixtures. Author: SqlRush <sqlrush@gmail.com> */
ClusterLockAcquireResult
cluster_lock_owner_request_acquire(ClusterLockAcquireRequest *request,
								   ClusterControlNativeFinish finish, void *argument)
{
	ClusterLockAcquireResult result = cluster_lock_acquire_seven_step(request);

	if (request->control_owner_id == 0)
		request->control_owner_id = request->request_id;
	return result == CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK ? finish(request, argument) : result;
}
ClusterLockAcquireResult
cluster_lock_owner_request_convert(ClusterLockAcquireRequest *request, uint64 preserve_request_id,
								   ClusterControlNativeFinish finish, void *argument)
{
	if (preserve_request_id != 0)
		request->request_id = request->holder.request_id = preserve_request_id;
	return cluster_lock_owner_request_acquire(request, finish, argument);
}
ClusterLockAcquireResult
cluster_lock_owner_request_release(const ClusterLockAcquireRequest *request)
{
	return cluster_lock_acquire_s6_release(request);
}
static bool fake_v2_ref_ready;
static ClusterWalSourceRef fake_v2_ref;
ResourceOwner CurrentResourceOwner = (ResourceOwner)(uintptr_t)0x1;

bool
cluster_wal_thread_current_v2_ref(ClusterWalSourceRef *out)
{
	memset(out, 0, sizeof(*out));
	if (!fake_v2_ref_ready)
		return false;
	*out = fake_v2_ref;
	return true;
}

static ResourceReleaseCallback fake_resource_release_callback;
static void *fake_resource_release_arg;

static ClusterLockAcquireResult fake_acquire_results[16];
static int fake_acquire_result_count;
static int fake_acquire_call_count;
static int fake_acquire_error_at;
static ClusterLockAcquireRequest fake_acquire_requests[16];
static ClusterLockAcquireResult fake_release_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
static int fake_release_call_count;
static uint16 fake_release_threads[4];
static ClusterLockAcquireResult fake_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
static int fake_s5_call_count;
static int fake_s7_call_count;
static LockAcquireResult fake_native_acquire_result = LOCKACQUIRE_OK;
static int fake_native_acquire_call_count;
static int fake_native_release_call_count;
static LOCKTAG fake_native_locktag;
static bool fake_root_current = true;
static bool fake_formation_current = true;
static bool fake_need_current = true;
static bool fake_admission_current = true;
static uint32 fake_need_count = 1;
static uint32 fake_admission_count = 1;
static PgracExternalFenceWriterSetDigest fake_need_digest;
static PgracExternalFenceWriterSetDigest fake_admission_digest;
static ClusterControlRootSnapshot fake_preflight_root;
static ClusterControlRootReadToken fake_preflight_token;
static ClusterControlRecoverySubject fake_pending_subject;
static bool fake_preflight_root_ready;
static bool fake_terminal_history_blocked;
static bool fake_extra_configured_thread;
static PgracExternalFenceNeedSetResult fake_need_build_result = PGRAC_EXTERNAL_FENCE_NEED_SET_OK;
static ClusterRecoverySerialRevalidateResult fake_serial_result = CLUSTER_RECOVERY_SERIAL_CURRENT;
static bool capture_release_log;
static unsigned release_log_count;
static char release_log_detail[1024];
static ClusterLockAcquireRequest released_request;
static char read_release_observations[4][2048];
static unsigned read_release_observation_count;
static bool expect_read_cleanup_fatal, finishing_read_cleanup_fatal;
static char read_cleanup_fatal[128];

void
RegisterResourceReleaseCallback(ResourceReleaseCallback callback, void *arg)
{
	fake_resource_release_callback = callback;
	fake_resource_release_arg = arg;
}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	if (expect_read_cleanup_fatal && elevel == FATAL) {
		finishing_read_cleanup_fatal = true;
		return true;
	}
	if (!capture_release_log || elevel != LOG)
		return false;
	release_log_count++;
	return true;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	if (finishing_read_cleanup_fatal) {
		finishing_read_cleanup_fatal = false;
		pg_re_throw();
	}
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	if (expect_read_cleanup_fatal) {
		va_list args;
		va_start(args, fmt);
		vsnprintf(read_cleanup_fatal, sizeof(read_cleanup_fatal), fmt, args);
		va_end(args);
	}
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	if (strstr(fmt, "PGRAC_FAMILY=WALR_RELEASE") != NULL) {
		va_list args;
		unsigned index = read_release_observation_count++;
		va_start(args, fmt);
		if (index < lengthof(read_release_observations))
			vsnprintf(read_release_observations[index], sizeof(read_release_observations[index]),
					  fmt, args);
		va_end(args);
	}
	return 0;
}

int
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
}

int
errdetail(const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	vsnprintf(release_log_detail, sizeof(release_log_detail), fmt, args);
	va_end(args);
	return 0;
}

/* Force the same allocation address for the native borrowed-owner ABA case. */
static void *read_owner_reuse_storage;
static Size read_owner_allocation_bytes;
void *
palloc0(Size size)
{
	read_owner_allocation_bytes = size;
	if (read_owner_reuse_storage != NULL) {
		if (size > 4096) abort();
		memset(read_owner_reuse_storage, 0, size);
		return read_owner_reuse_storage;
	}
	return calloc(1, size);
}

void
pfree(void *pointer)
{
	if (pointer != read_owner_reuse_storage)
		free(pointer);
}

TimestampTz
GetCurrentTimestamp(void)
{
	return INT64_C(123456789000);
}

ClusterLockAcquireResult
cluster_lock_acquire_seven_step(const ClusterLockAcquireRequest *request)
{
	ClusterLockAcquireRequest *mutable_request = (ClusterLockAcquireRequest *)request;
	ClusterLockAcquireResult result = CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK;

	if (fake_acquire_call_count < lengthof(fake_acquire_requests))
		fake_acquire_requests[fake_acquire_call_count] = *request;
	if (fake_acquire_call_count < fake_acquire_result_count)
		result = fake_acquire_results[fake_acquire_call_count];
	fake_acquire_call_count++;
	if (fake_acquire_call_count == fake_acquire_error_at)
		pg_re_throw();
	if (result == CLUSTER_LOCK_ACQUIRE_OK_GRANTED
		|| result == CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK) {
		mutable_request->request_id = (uint64)fake_acquire_call_count;
		mutable_request->holder.node_id = 0;
		mutable_request->holder.procno = 1;
		mutable_request->holder.cluster_epoch = 1;
		mutable_request->holder.request_id = mutable_request->request_id;
	}
	return result;
}

ClusterLockAcquireResult
cluster_lock_acquire_s5_promote(const ClusterLockAcquireRequest *request)
{
	ClusterLockAcquireRequest *mutable_request = (ClusterLockAcquireRequest *)request;

	fake_s5_call_count++;
	if (fake_s5_result == CLUSTER_LOCK_ACQUIRE_OK_GRANTED
		|| fake_s5_result == CLUSTER_LOCK_ACQUIRE_OK_CONVERTED) {
		mutable_request->request_id = (uint64)fake_s5_call_count;
		mutable_request->holder.node_id = 0;
		mutable_request->holder.procno = 1;
		mutable_request->holder.cluster_epoch = 1;
		mutable_request->holder.request_id = mutable_request->request_id;
	}
	return fake_s5_result == CLUSTER_LOCK_ACQUIRE_OK_GRANTED
				   && request->op == CLUSTER_LOCK_OP_CONVERT
			   ? CLUSTER_LOCK_ACQUIRE_OK_CONVERTED
			   : fake_s5_result;
}

ClusterLockAcquireResult
cluster_lock_acquire_s7_cleanup(const ClusterLockAcquireRequest *request pg_attribute_unused())
{
	fake_s7_call_count++;
	return CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
}

LockAcquireResult
LockAcquire(const LOCKTAG *locktag, LOCKMODE lockmode pg_attribute_unused(),
			bool sessionLock pg_attribute_unused(), bool dontWait pg_attribute_unused())
{
	fake_native_acquire_call_count++;
	fake_native_locktag = *locktag;
	return fake_native_acquire_result;
}

bool
LockRelease(const LOCKTAG *locktag, LOCKMODE lockmode pg_attribute_unused(),
			bool sessionLock pg_attribute_unused())
{
	fake_native_release_call_count++;
	fake_native_locktag = *locktag;
	return true;
}

ClusterLockAcquireResult
cluster_lock_acquire_s6_release(const ClusterLockAcquireRequest *request)
{
	released_request = *request;
	if (fake_release_call_count < lengthof(fake_release_threads))
		fake_release_threads[fake_release_call_count] = (uint16)request->resid.field1;
	fake_release_call_count++;
	return fake_release_result;
}

ClusterControlRootResult
cluster_control_root_revalidate(const ClusterControlRootReadToken *token,
								const ClusterControlRootIdentity *expected_identity,
								ClusterControlRootSnapshot *out_snapshot)
{
	if (!fake_root_current)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	memset(out_snapshot, 0, sizeof(*out_snapshot));
	out_snapshot->identity = *expected_identity;
	out_snapshot->lifecycle = token->lifecycle;
	out_snapshot->root_flags = token->root_flags;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_control_root_read_recovery_subject(uint16 thread,
										   const ClusterControlRootIdentity *expected,
										   ClusterControlRecoverySubject *out)
{
	UT_ASSERT(thread == fake_pending_subject.duty.origin_thread_id && expected != NULL);
	*out = fake_pending_subject;
	return fake_root_current ? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}

ClusterControlRootResult
cluster_control_root_read_canonical(uint16 origin_thread_id,
									const ClusterControlRootIdentity *expected_identity,
									ClusterControlRootReadMode mode,
									ClusterControlRootSnapshot *out_snapshot,
									ClusterControlRootReadToken *out_token)
{
	if (cluster_shared_config)
		return CLUSTER_CONTROL_ROOT_BAD_VERSION;
	if (!fake_preflight_root_ready || origin_thread_id != 1)
		return CLUSTER_CONTROL_ROOT_ABSENT;
	if (mode == CLUSTER_CONTROL_ROOT_READ_BOOTSTRAP_VALIDATE && expected_identity == NULL) {
		*out_snapshot = fake_preflight_root;
		return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	}
	if (expected_identity == NULL
		|| memcmp(expected_identity, &fake_preflight_root.identity, sizeof(*expected_identity))
			   != 0)
		return CLUSTER_CONTROL_ROOT_ABSENT;
	*out_snapshot = fake_preflight_root;
	if (out_token != NULL)
		*out_token = fake_preflight_token;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

/* The root reader's real filesystem/authority body is tested by the root
 * suite. Here it is the explicit authority seam for the real retention .o. */
ClusterControlRootResult
cluster_control_root_v3_read_retention_current(const ClusterControlRootIdentity *self,
											   ClusterControlRootSnapshot *out,
											   ClusterControlRootReadToken *token)
{
	memset(out, 0, sizeof(*out));
	memset(token, 0, sizeof(*token));
	if (!fake_preflight_root_ready || !fake_root_current || self == NULL
		|| memcmp(self, &fake_preflight_root.identity, sizeof(*self)) != 0)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	*out = fake_preflight_root;
	*token = fake_preflight_token;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_control_root_v3_terminal_history_blocked(
	const ClusterControlRootIdentity *expected_identity, bool *out_blocked)
{
	(void)expected_identity;
	if (out_blocked != NULL)
		*out_blocked = fake_terminal_history_blocked;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterFormationWitnessResult
cluster_formation_witness_build_live_wait(uint16 origin_thread, int timeout_ms,
										  ClusterFormationWitnessV1 **out)
{
	if (!fake_formation_current || origin_thread != 1 || timeout_ms < 1 || out == NULL
		|| *out != NULL)
		return CLUSTER_FORMATION_WITNESS_UNSTABLE;
	*out = (ClusterFormationWitnessV1 *)(uintptr_t)0x1000;
	return CLUSTER_FORMATION_WITNESS_READY;
}

void
cluster_formation_witness_destroy(ClusterFormationWitnessV1 **witness)
{
	*witness = NULL;
}

ClusterFormationWitnessResult
cluster_formation_witness_revalidate_nowait(
	const ClusterFormationWitnessV1 *witness pg_attribute_unused())
{
	return fake_formation_current ? CLUSTER_FORMATION_WITNESS_READY
								  : CLUSTER_FORMATION_WITNESS_UNSTABLE;
}

bool
cluster_formation_witness_copy_classification_v1(
	const ClusterFormationWitnessV1 *witness pg_attribute_unused(), uint16 *origin_thread,
	ClusterFenceAuthorityProof *authority, ClusterFormationSnapshotV1 *snapshot)
{
	memset(authority, 0, sizeof(*authority));
	memset(snapshot, 0, sizeof(*snapshot));
	*origin_thread = 1;
	snapshot->membership.membership_state[0] = CLUSTER_MEMBER_MEMBER;
	if (fake_extra_configured_thread)
		snapshot->membership.membership_state[1] = CLUSTER_MEMBER_DEAD;
	return true;
}

bool
cluster_external_fence_need_set_revalidate_nowait(
	const PgracExternalFenceNeedSetV1 *needs pg_attribute_unused(),
	const ClusterFormationWitnessV1 *formation pg_attribute_unused(),
	PgracExternalFenceDenyReason *reason)
{
	*reason = fake_need_current ? PGRAC_EXTERNAL_FENCE_DENY_NONE
								: PGRAC_EXTERNAL_FENCE_DENY_MAPPING_CHANGED;
	return fake_need_current;
}

bool
cluster_external_fence_revalidate_set_nowait(
	const PgracExternalFenceAdmissionSetV1 *admissions pg_attribute_unused(),
	const PgracExternalFenceNeedSetV1 *needs pg_attribute_unused(),
	const ClusterFormationWitnessV1 *formation pg_attribute_unused(),
	PgracExternalFenceDenyReason *reason)
{
	*reason = fake_admission_current ? PGRAC_EXTERNAL_FENCE_DENY_NONE
									 : PGRAC_EXTERNAL_FENCE_DENY_EXPIRED;
	return fake_admission_current;
}

PgracExternalFenceNeedSetResult
cluster_external_fence_need_set_build(const ClusterRecoveryDutyKey *duty pg_attribute_unused(),
									  const ClusterFormationWitnessV1 *formation
										  pg_attribute_unused(),
									  PgracExternalFenceNeedSetV1 **out)
{
	if (fake_need_build_result == PGRAC_EXTERNAL_FENCE_NEED_SET_OK)
		*out = (PgracExternalFenceNeedSetV1 *)(uintptr_t)0x4000;
	return fake_need_build_result;
}

void
cluster_external_fence_need_set_release(PgracExternalFenceNeedSetV1 **set)
{
	*set = NULL;
}

uint32
cluster_external_fence_need_set_count(const PgracExternalFenceNeedSetV1 *set pg_attribute_unused())
{
	return fake_need_count;
}

const PgracExternalFenceWriterSetDigest *
cluster_external_fence_need_set_digest(const PgracExternalFenceNeedSetV1 *set pg_attribute_unused())
{
	return &fake_need_digest;
}

uint32
cluster_external_fence_admission_set_count(
	const PgracExternalFenceAdmissionSetV1 *set pg_attribute_unused())
{
	return fake_admission_count;
}

const PgracExternalFenceWriterSetDigest *
cluster_external_fence_admission_set_digest(
	const PgracExternalFenceAdmissionSetV1 *set pg_attribute_unused())
{
	return &fake_admission_digest;
}

PgracExternalFenceVerdict
cluster_external_fence_admit_set_wait(
	const PgracExternalFenceNeedSetV1 *needs pg_attribute_unused(),
	const ClusterFormationWitnessV1 *formation pg_attribute_unused(),
	int timeout_ms pg_attribute_unused(), PgracExternalFenceAdmissionSetV1 **out)
{
	*out = (PgracExternalFenceAdmissionSetV1 *)(uintptr_t)0x5000;
	return PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED;
}

void
cluster_external_fence_admission_set_release(PgracExternalFenceAdmissionSetV1 **set)
{
	*set = NULL;
}

ClusterRecoverySerialRevalidateResult
cluster_recovery_serial_initializer_revalidate(ClusterRecoverySerialGuard *guard)
{
	return guard->mode == CLUSTER_RECOVERY_SERIAL_INITIALIZER
			   ? fake_serial_result
			   : CLUSTER_RECOVERY_SERIAL_CAPABILITY_STALE;
}

ClusterRecoverySerialRevalidateResult
cluster_recovery_serial_revalidate(ClusterRecoverySerialGuard *guard pg_attribute_unused())
{
	return fake_serial_result;
}

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

static void
reset_pin_fakes(void)
{
	cluster_trace_control_config = false;
	CritSectionCount = 0;
	proc_exit_inprogress = false;
	MyBackendType = B_INVALID;
	read_release_observation_count = 0;
	memset(read_release_observations, 0, sizeof(read_release_observations));
	expect_read_cleanup_fatal = finishing_read_cleanup_fatal = false;
	memset(read_cleanup_fatal, 0, sizeof(read_cleanup_fatal));
	capture_release_log = false;
	release_log_count = 0;
	memset(release_log_detail, 0, sizeof(release_log_detail));
	memset(&released_request, 0, sizeof(released_request));
	cluster_shared_config = false;
	fake_control_owner_current = true;
	fake_control_owner_mode = NoLock;
	fake_control_owner_stale_thread = 0;
	fake_v2_ref_ready = false;
	memset(&fake_v2_ref, 0, sizeof(fake_v2_ref));
	memset(fake_acquire_results, 0, sizeof(fake_acquire_results));
	memset(fake_acquire_requests, 0, sizeof(fake_acquire_requests));
	memset(fake_release_threads, 0, sizeof(fake_release_threads));
	fake_acquire_result_count = 0;
	fake_acquire_call_count = 0;
	fake_acquire_error_at = 0;
	fake_release_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	fake_release_call_count = 0;
	fake_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	fake_s5_call_count = 0;
	fake_s7_call_count = 0;
	fake_native_acquire_result = LOCKACQUIRE_OK;
	fake_native_acquire_call_count = 0;
	fake_native_release_call_count = 0;
	memset(&fake_native_locktag, 0, sizeof(fake_native_locktag));
	fake_root_current = true;
	fake_formation_current = true;
	fake_need_current = true;
	fake_admission_current = true;
	fake_need_count = 1;
	fake_admission_count = 1;
	memset(&fake_need_digest, 0, sizeof(fake_need_digest));
	memset(&fake_admission_digest, 0, sizeof(fake_admission_digest));
	memset(&fake_preflight_root, 0, sizeof(fake_preflight_root));
	memset(&fake_preflight_token, 0, sizeof(fake_preflight_token));
	fake_preflight_root_ready = false;
	fake_terminal_history_blocked = false;
	fake_extra_configured_thread = false;
	fake_need_build_result = PGRAC_EXTERNAL_FENCE_NEED_SET_OK;
	fake_serial_result = CLUSTER_RECOVERY_SERIAL_CURRENT;
}

static ClusterRecoveryDutyKey
make_duty(uint16 thread_id)
{
	ClusterRecoveryDutyKey duty;
	ClusterWalThreadClaim claim;

	memset(&duty, 0, sizeof(duty));
	duty.system_identifier = UINT64_C(0x1122334455667788);
	duty.storage_uuid[0] = 1;
	duty.authority_uuid[0] = 2;
	duty.authority_uuid[6] = 0x40;
	duty.authority_uuid[8] = 0x80;
	duty.origin_thread_id = thread_id;
	duty.origin_node_id = thread_id - 1;
	duty.thread_claim_created_at = 1000 + thread_id;
	cluster_wal_thread_claim_fill(&claim, thread_id, duty.origin_node_id,
								  duty.thread_claim_created_at);
	duty.thread_claim_crc32c = claim.crc;
	duty.origin_owner_incarnation = 10 + thread_id;
	duty.root_lineage_seq = 20 + thread_id;
	return duty;
}

static ClusterControlRootReadToken
make_root_token(const ClusterRecoveryDutyKey *duty)
{
	ClusterControlRootReadToken token;

	memset(&token, 0, sizeof(token));
	memcpy(token.authority_uuid, duty->authority_uuid, sizeof(token.authority_uuid));
	token.origin_thread_id = duty->origin_thread_id;
	token.source = 1;
	token.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	token.root_lineage_seq = duty->root_lineage_seq;
	token.file_txn_seq = 1;
	token.root_publish_seq = 1;
	token.record_crc32c = 1;
	token.root_flags
		= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
		  | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID;
	return token;
}

static ClusterWalRetentionPinThreadRequest
make_pin_request(uint16 thread_id, const ClusterWalRetentionInterval *intervals, uint32 nintervals)
{
	ClusterWalRetentionPinThreadRequest request;

	memset(&request, 0, sizeof(request));
	request.intervals = intervals;
	request.nintervals = nintervals;
	request.duty = make_duty(thread_id);
	request.root_read = make_root_token(&request.duty);
	request.formation = (const ClusterFormationWitnessV1 *)(uintptr_t)0x1000;
	request.needs = (const PgracExternalFenceNeedSetV1 *)(uintptr_t)0x2000;
	request.admissions = (const PgracExternalFenceAdmissionSetV1 *)(uintptr_t)0x3000;
	return request;
}

static ClusterRecoverySerialGuard
make_serial_guard(const ClusterWalRetentionPinThreadRequest *request)
{
	ClusterRecoverySerialGuard guard;

	memset(&guard, 0, sizeof(guard));
	guard.held = true;
	guard.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
	guard.duty = request->duty;
	guard.root_read_token = request->root_read;
	guard.formation = request->formation;
	guard.fence_need_set = request->needs;
	guard.fence_admission_set = request->admissions;
	return guard;
}

static void
write_test_wal_segment(const char *directory, TimeLineID tli, XLogSegNo segno, char *out_path,
					   Size out_path_size)
{
	char wal_name[MAXFNAMELEN];
	XLogLongPageHeaderData header;
	int fd;

	XLogFileName(wal_name, tli, segno, TEST_WAL_SEG_SIZE);
	snprintf(out_path, out_path_size, "%s/%s", directory, wal_name);
	fd = open(out_path, O_RDWR | O_CREAT | O_EXCL, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(ftruncate(fd, TEST_WAL_SEG_SIZE), 0);
	memset(&header, 0, sizeof(header));
	header.std.xlp_magic = XLOG_PAGE_MAGIC;
	header.std.xlp_info = XLP_LONG_HEADER;
	header.std.xlp_tli = tli;
	header.std.xlp_pageaddr = (XLogRecPtr)segno * TEST_WAL_SEG_SIZE;
	header.std.xlp_thread_id = 1;
	header.xlp_sysid = UINT64_C(0x1122334455667788);
	header.xlp_seg_size = TEST_WAL_SEG_SIZE;
	header.xlp_xlog_blcksz = XLOG_BLCKSZ;
	UT_ASSERT_EQ(pwrite(fd, &header, sizeof(header), 0), sizeof(header));
	close(fd);
}

/* PGRAC: actual retention object + real files; authority is the existing
 * fixture seam, not a claim that root-v2 GC admission is implemented.
 * Author: SqlRush <sqlrush@gmail.com>
 */
typedef struct V2ReuseFixture {
	char root[MAXPGPATH];
	char thread[MAXPGPATH];
	char generation[MAXPGPATH];
	char claim[MAXPGPATH];
	char flat_wal[MAXPGPATH];
	char generation_wal[MAXPGPATH];
	uint8 claim_bytes[CLUSTER_WAL_CLAIM_V2_BYTES];
	ClusterWalReuseGuardRequest request;
} V2ReuseFixture;

static void
v2_reuse_fixture(V2ReuseFixture *f)
{
	ClusterWalThreadClaimV2 claim;
	pg_cryptohash_ctx *hash;
	int fd;

	reset_pin_fakes();
	memset(f, 0, sizeof(*f));
	strlcpy(f->root, "/tmp/pgrac-wal-reuse-v2-XXXXXX", sizeof(f->root));
	UT_ASSERT_NOT_NULL(mkdtemp(f->root));
	snprintf(f->thread, sizeof(f->thread), "%s/thread_1", f->root);
	snprintf(f->generation, sizeof(f->generation), "%s/generation_11", f->thread);
	snprintf(f->claim, sizeof(f->claim), "%s/pgrac_thread.claim", f->generation);
	UT_ASSERT_EQ(mkdir(f->thread, 0700), 0);
	UT_ASSERT_EQ(mkdir(f->generation, 0700), 0);
	write_test_wal_segment(f->thread, 1, 1, f->flat_wal, sizeof(f->flat_wal));
	write_test_wal_segment(f->generation, 1, 1, f->generation_wal, sizeof(f->generation_wal));
	memset(&claim, 0, sizeof(claim));
	claim.identity = make_duty(1);
	claim.identity.thread_claim_crc32c = 0;
	claim.database_incarnation = 7;
	claim.config_generation = 3;
	claim.claim_generation = 1;
	UT_ASSERT_EQ(cluster_wal_claim_v2_encode(&claim, f->claim_bytes),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	claim.identity.thread_claim_crc32c
		= (uint32)f->claim_bytes[104] | ((uint32)f->claim_bytes[105] << 8)
		  | ((uint32)f->claim_bytes[106] << 16) | ((uint32)f->claim_bytes[107] << 24);
	fd = open(f->claim, O_WRONLY | O_CREAT | O_EXCL, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(write(fd, f->claim_bytes, sizeof(f->claim_bytes)), sizeof(f->claim_bytes));
	UT_ASSERT_EQ(close(fd), 0);
	fake_v2_ref.claim.identity = claim.identity;
	fake_v2_ref.claim.database_incarnation = claim.database_incarnation;
	fake_v2_ref.claim.max_config_generation = claim.config_generation;
	fake_v2_ref.timeline = 1;
	hash = pg_cryptohash_create(PG_SHA256);
	UT_ASSERT_NOT_NULL(hash);
	UT_ASSERT_EQ(pg_cryptohash_init(hash), 0);
	UT_ASSERT_EQ(pg_cryptohash_update(hash, f->claim_bytes, sizeof(f->claim_bytes)), 0);
	UT_ASSERT_EQ(pg_cryptohash_final(hash, fake_v2_ref.claim.claim_sha256, 32), 0);
	pg_cryptohash_free(hash);
	fake_v2_ref_ready = true;
	cluster_shared_config = true;
	cluster_wal_threads_dir = f->root;
	f->request.file.thread_id = 1;
	f->request.file.kind = CLUSTER_WAL_FILE_NORMAL;
	f->request.file.tli = 1;
	f->request.file.segno = 1;
	f->request.entry = CLUSTER_WAL_REUSE_E1_CHECKPOINT_RESTARTPOINT;
	f->request.action = CLUSTER_WAL_ACTION_RETIRE_RECYCLE_OR_REMOVE;
	f->request.duty = claim.identity;
	f->request.root_read = make_root_token(&claim.identity);
	f->request.root_read.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	f->request.formation = (const ClusterFormationWitnessV1 *)(uintptr_t)0x1000;
	fake_preflight_root.identity = claim.identity;
	fake_preflight_root.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	fake_preflight_root.root_flags = f->request.root_read.root_flags;
	fake_preflight_root.root_publish_seq = 1;
	fake_preflight_root.checkpoint_tli = fake_preflight_root.tail_tli = 1;
	fake_preflight_root.checkpoint_lower_lsn = TEST_WAL_SEG_SIZE * 4;
	fake_preflight_root.validated_tail_lsn_exclusive = TEST_WAL_SEG_SIZE * 5;
	fake_preflight_token = f->request.root_read;
	fake_preflight_root_ready = true;
}

static void
v2_reuse_fixture_cleanup(V2ReuseFixture *f)
{
	UT_ASSERT_EQ(unlink(f->flat_wal), 0);
	UT_ASSERT_EQ(unlink(f->generation_wal), 0);
	UT_ASSERT_EQ(unlink(f->claim), 0);
	UT_ASSERT_EQ(rmdir(f->generation), 0);
	UT_ASSERT_EQ(rmdir(f->thread), 0);
	UT_ASSERT_EQ(rmdir(f->root), 0);
	cluster_wal_threads_dir = NULL;
	reset_pin_fakes();
}

UT_TEST(test_v2_reuse_selects_exact_generation_not_flat_decoy)
{
	V2ReuseFixture f;
	ClusterWalReuseActionGuard guard = { 0 };
	ClusterWalReuseDenyReason reason;
	ClusterWalTerminalOutcome outcome;
	PgracExternalFenceNeedSetV1 *needs = NULL;
	struct stat selected;

	v2_reuse_fixture(&f);
	UT_ASSERT_EQ(stat(f.generation_wal, &selected), 0);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_init(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_preflight(&guard, &f.request, &needs, &reason),
				 CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(guard.pre_action_stamp.file_id_lo, selected.st_ino);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_finish(&guard, &outcome, &reason),
				 CLUSTER_WALR_RELEASE_NOT_HELD);
	UT_ASSERT_EQ(outcome, CLUSTER_WAL_TERMINAL_UNCHANGED);
	v2_reuse_fixture_cleanup(&f);
}

/* PRE2: a selected checkpoint-less terminal remains a physical retention
 * input until a typed consumer closes it; a new reservation must not prune it
 * merely because the current root is otherwise reusable. */
UT_TEST(test_v2_reuse_terminal_history_blocks_until_consumer_closes)
{
	V2ReuseFixture f;
	ClusterWalReuseActionGuard guard = { 0 };
	ClusterWalReuseDenyReason reason;
	ClusterWalTerminalOutcome outcome;
	PgracExternalFenceNeedSetV1 *needs = NULL;

	v2_reuse_fixture(&f);
	fake_terminal_history_blocked = true;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_init(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_preflight(&guard, &f.request, &needs, &reason),
				 CLUSTER_WAL_GUARD_BLOCKED);
	UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_ROOT_REQUIRED);
	UT_ASSERT_NULL(needs);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_finish(&guard, &outcome, &reason),
				 CLUSTER_WALR_RELEASE_NOT_HELD);
	UT_ASSERT_EQ(outcome, CLUSTER_WAL_TERMINAL_UNCHANGED);
	v2_reuse_fixture_cleanup(&f);
}

UT_TEST(test_v2_e1_consumes_current_root_and_preserves_exact_floor)
{
	V2ReuseFixture f;
	ClusterWalRetentionE1Context context = { 0 };
	ClusterWalRootFoldResult fold;
	ClusterWalReuseDenyReason reason;
	XLogSegNo floor;

	v2_reuse_fixture(&f);
	UT_ASSERT_EQ(cluster_wal_retention_e1_coarse_begin(&context, 1, &fold, &floor, &reason),
				 CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(fold, CLUSTER_WAL_FOLD_BOUNDED);
	UT_ASSERT_EQ(floor, 4);
	UT_ASSERT(context.coarse_walr.held);
	if (context.coarse_walr.held)
		UT_ASSERT_EQ(cluster_wal_retention_e1_coarse_release(&context, &reason),
					 CLUSTER_WALR_RELEASE_CONFIRMED);
	cluster_wal_retention_e1_finish(&context);
	UT_ASSERT_EQ(context.magic, 0);
	v2_reuse_fixture_cleanup(&f);
}

UT_TEST(test_v2_reuse_missing_native_ref_cannot_fallback_flat)
{
	V2ReuseFixture f;
	ClusterWalReuseActionGuard guard = { 0 };
	ClusterWalReuseDenyReason reason;
	ClusterWalTerminalOutcome outcome;
	PgracExternalFenceNeedSetV1 *needs = NULL;

	v2_reuse_fixture(&f);
	fake_v2_ref_ready = false;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_init(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_preflight(&guard, &f.request, &needs, &reason),
				 CLUSTER_WAL_GUARD_BLOCKED);
	UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_OBJECT_STALE);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_finish(&guard, &outcome, &reason),
				 CLUSTER_WALR_RELEASE_NOT_HELD);
	v2_reuse_fixture_cleanup(&f);
}

UT_TEST(test_v2_retention_zero_crc_is_not_absent_authority)
{
	V2ReuseFixture f;
	ClusterWalRetentionE1Context context = { 0 };
	ClusterWalRootFoldResult fold;
	ClusterWalReuseDenyReason reason;
	ClusterWalRootPublishGuard *publish = NULL;
	XLogSegNo floor;

	v2_reuse_fixture(&f);
	fake_preflight_token.record_crc32c = 0;
	UT_ASSERT_EQ(cluster_wal_retention_e1_coarse_begin(&context, 1, &fold, &floor, &reason),
				 CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(floor, 4);
	if (context.coarse_walr.held)
		UT_ASSERT_EQ(cluster_wal_retention_e1_coarse_release(&context, &reason),
					 CLUSTER_WALR_RELEASE_CONFIRMED);
	cluster_wal_retention_e1_finish(&context);
	UT_ASSERT_EQ(
		cluster_wal_retention_root_publish_begin_exact(&fake_preflight_token, false, &publish),
		CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&publish), CLUSTER_WALR_RELEASE_CONFIRMED);
	v2_reuse_fixture_cleanup(&f);
}

UT_TEST(test_v2_retention_refuses_checkpoint_future_and_wrong_purpose)
{
	for (int which = 0; which < 6; ++which) {
		V2ReuseFixture f;
		ClusterWalReuseActionGuard guard = { 0 };
		ClusterWalReuseDenyReason reason;
		ClusterWalTerminalOutcome outcome;
		PgracExternalFenceNeedSetV1 *needs = NULL;
		char extra[MAXPGPATH];

		v2_reuse_fixture(&f);
		if (which < 2) {
			f.request.file.segno = which == 0 ? 4 : 9;
			write_test_wal_segment(f.generation, 1, f.request.file.segno, extra, sizeof(extra));
		} else if (which == 2)
			f.request.entry = CLUSTER_WAL_REUSE_E2_APPLY_TIMELINE_SWITCH;
		else if (which == 3)
			f.request.root_read.file_txn_seq++;
		else if (which == 4)
			fake_preflight_root.validated_tail_lsn_exclusive
				= fake_preflight_root.checkpoint_lower_lsn;
		else
			fake_preflight_root.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
		UT_ASSERT_EQ(cluster_wal_reuse_guard_init(&guard, &reason), CLUSTER_WAL_GUARD_OK);
		UT_ASSERT_EQ(cluster_wal_reuse_guard_preflight(&guard, &f.request, &needs, &reason),
					 CLUSTER_WAL_GUARD_BLOCKED);
		UT_ASSERT_EQ(reason, which == 2	  ? CLUSTER_WAL_DENY_THREAD_SCOPE
							 : which == 3 ? CLUSTER_WAL_DENY_ROOT_STALE
										  : CLUSTER_WAL_DENY_ROOT_REQUIRED);
		UT_ASSERT_EQ(cluster_wal_reuse_guard_finish(&guard, &outcome, &reason),
					 CLUSTER_WALR_RELEASE_NOT_HELD);
		UT_ASSERT_EQ(access(f.generation_wal, F_OK), 0);
		if (which < 2)
			UT_ASSERT_EQ(unlink(extra), 0);
		v2_reuse_fixture_cleanup(&f);
	}
}

UT_TEST(test_v2_retention_revalidates_root_after_walr_x)
{
	V2ReuseFixture f;
	ClusterWalReuseActionGuard guard = { 0 };
	ClusterWalReuseDenyReason reason;
	ClusterWalTerminalOutcome outcome;
	PgracExternalFenceNeedSetV1 *needs = NULL;

	v2_reuse_fixture(&f);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_init(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_preflight(&guard, &f.request, &needs, &reason),
				 CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_fence_admitted_nowait(&guard, NULL, &reason),
				 CLUSTER_WAL_GUARD_OK);
	fake_acquire_result_count = 1;
	fake_acquire_results[0] = CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK;
	fake_preflight_token.root_publish_seq++;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_arm(&guard, NULL, NULL, &reason),
				 CLUSTER_WAL_GUARD_BLOCKED);
	UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_ROOT_STALE);
	UT_ASSERT_FALSE(guard.walr.held);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_finish(&guard, &outcome, &reason),
				 CLUSTER_WALR_RELEASE_NOT_HELD);
	UT_ASSERT_EQ(access(f.generation_wal, F_OK), 0);
	v2_reuse_fixture_cleanup(&f);
}

static void
v2_reuse_write_claim(const V2ReuseFixture *f, const uint8 *bytes)
{
	int fd = open(f->claim, O_WRONLY | O_TRUNC | O_CREAT, 0600);

	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(write(fd, bytes, CLUSTER_WAL_CLAIM_V2_BYTES), CLUSTER_WAL_CLAIM_V2_BYTES);
	UT_ASSERT_EQ(close(fd), 0);
}

UT_TEST(test_v2_reuse_rejects_unproven_namespace_and_claim)
{
	for (int which = 0; which < 9; ++which) {
		V2ReuseFixture f;
		ClusterWalReuseActionGuard guard = { 0 };
		ClusterWalReuseDenyReason reason;
		ClusterWalTerminalOutcome outcome;
		PgracExternalFenceNeedSetV1 *needs = NULL;
		char held[MAXPGPATH];
		uint8 changed[CLUSTER_WAL_CLAIM_V2_BYTES];

		v2_reuse_fixture(&f);
		memcpy(changed, f.claim_bytes, sizeof(changed));
		changed[0] ^= 1;
		snprintf(held, sizeof(held), "%s.held", which == 3 ? f.generation : f.claim);
		switch (which) {
		case 0:
			fake_v2_ref.claim.identity.origin_owner_incarnation++;
			break;
		case 1:
			fake_v2_ref.claim.claim_sha256[0] ^= 1;
			break;
		case 2:
			v2_reuse_write_claim(&f, changed);
			break;
		case 3:
			UT_ASSERT_EQ(rename(f.generation, held), 0);
			UT_ASSERT_EQ(symlink(held, f.generation), 0);
			break;
		case 4:
			UT_ASSERT_EQ(rename(f.claim, held), 0);
			UT_ASSERT_EQ(symlink(held, f.claim), 0);
			break;
		case 5:
			UT_ASSERT_EQ(unlink(f.claim), 0);
			UT_ASSERT_EQ(mkfifo(f.claim, 0600), 0);
			break;
		case 6:
			UT_ASSERT_EQ(unlink(f.generation_wal), 0);
			UT_ASSERT_EQ(mkfifo(f.generation_wal, 0600), 0);
			break;
		case 7:
			UT_ASSERT_EQ(chmod(f.generation, 0770), 0);
			break;
		case 8:
			fake_v2_ref.timeline++;
			break;
		}
		UT_ASSERT_EQ(cluster_wal_reuse_guard_init(&guard, &reason), CLUSTER_WAL_GUARD_OK);
		alarm(10); /* Test-only watchdog: malformed filesystem input cannot hang. */
		UT_ASSERT_EQ(cluster_wal_reuse_guard_preflight(&guard, &f.request, &needs, &reason),
					 CLUSTER_WAL_GUARD_BLOCKED);
		alarm(0);
		UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_OBJECT_STALE);
		UT_ASSERT_EQ(guard.source_dir_handle, -1);
		UT_ASSERT_EQ(guard.source_handle, -1);
		UT_ASSERT_EQ(cluster_wal_reuse_guard_finish(&guard, &outcome, &reason),
					 CLUSTER_WALR_RELEASE_NOT_HELD);
		UT_ASSERT_EQ(access(f.flat_wal, F_OK), 0);
		if (which == 3) {
			UT_ASSERT_EQ(unlink(f.generation), 0);
			UT_ASSERT_EQ(rename(held, f.generation), 0);
		} else if (which == 4) {
			UT_ASSERT_EQ(unlink(f.claim), 0);
			UT_ASSERT_EQ(rename(held, f.claim), 0);
		} else if (which == 5) {
			UT_ASSERT_EQ(unlink(f.claim), 0);
			v2_reuse_write_claim(&f, f.claim_bytes);
		} else if (which == 7)
			UT_ASSERT_EQ(chmod(f.generation, 0700), 0);
		v2_reuse_fixture_cleanup(&f);
	}
}

static void
v2_reuse_arm(V2ReuseFixture *f, ClusterWalReuseActionGuard *guard)
{
	ClusterWalReuseDenyReason reason;
	PgracExternalFenceNeedSetV1 *needs = NULL;

	UT_ASSERT_EQ(cluster_wal_reuse_guard_init(guard, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_preflight(guard, &f->request, &needs, &reason),
				 CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(guard->pre_action_stamp.generation_claim_length, CLUSTER_WAL_CLAIM_V2_BYTES);
	UT_ASSERT(
		memcmp(guard->pre_action_stamp.generation_claim, f->claim_bytes, sizeof(f->claim_bytes))
		== 0);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_fence_admitted_nowait(guard, NULL, &reason),
				 CLUSTER_WAL_GUARD_OK);
	fake_acquire_result_count = 1;
	fake_acquire_results[0] = CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_arm(guard, NULL, NULL, &reason), CLUSTER_WAL_GUARD_OK);
}

UT_TEST(test_v2_reuse_claim_change_blocks_every_physical_recheck)
{
	for (int which = 0; which < 4; ++which) {
		V2ReuseFixture f;
		ClusterWalReuseActionGuard guard = { 0 };
		ClusterWalReuseDenyReason reason;
		ClusterWalTerminalOutcome outcome;
		ClusterWalFileIdentity destination;
		ClusterWalReusePhysicalAction physical
			= which == 2 ? CLUSTER_WAL_PHYSICAL_RECYCLE : CLUSTER_WAL_PHYSICAL_REMOVE;
		uint8 changed[CLUSTER_WAL_CLAIM_V2_BYTES];

		v2_reuse_fixture(&f);
		v2_reuse_arm(&f, &guard);
		if (which != 0)
			UT_ASSERT_EQ(cluster_wal_reuse_guard_l3_begin(&guard, physical, &reason),
						 CLUSTER_WAL_GUARD_OK);
		memcpy(changed, f.claim_bytes, sizeof(changed));
		changed[32] ^= 1;
		v2_reuse_write_claim(&f, changed);
		if (which == 0)
			UT_ASSERT_EQ(cluster_wal_reuse_guard_l3_begin(&guard, physical, &reason),
						 CLUSTER_WAL_GUARD_BLOCKED);
		else if (which == 1)
			UT_ASSERT_EQ(cluster_wal_reuse_guard_remove(&guard, &reason),
						 CLUSTER_WAL_GUARD_BLOCKED);
		else if (which == 2) {
			destination = f.request.file;
			destination.segno = 8;
			UT_ASSERT_EQ(cluster_wal_reuse_guard_recycle(&guard, &destination, &reason),
						 CLUSTER_WAL_GUARD_BLOCKED);
		} else
			UT_ASSERT_EQ(cluster_wal_reuse_guard_confirm_zero_mutation(&guard, physical, &reason),
						 CLUSTER_WAL_GUARD_BLOCKED);
		UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_OBJECT_STALE);
		UT_ASSERT_EQ(access(f.generation_wal, F_OK), 0);
		UT_ASSERT_EQ(access(f.flat_wal, F_OK), 0);
		v2_reuse_write_claim(&f, f.claim_bytes);
		if (which != 0)
			UT_ASSERT_EQ(cluster_wal_reuse_guard_confirm_zero_mutation(&guard, physical, &reason),
						 CLUSTER_WAL_GUARD_OK);
		UT_ASSERT_EQ(cluster_wal_reuse_guard_finish(&guard, &outcome, &reason),
					 CLUSTER_WALR_RELEASE_CONFIRMED);
		UT_ASSERT_EQ(outcome, CLUSTER_WAL_TERMINAL_UNCHANGED);
		v2_reuse_fixture_cleanup(&f);
	}
}

UT_TEST(test_v2_reuse_removes_only_held_generation_object)
{
	V2ReuseFixture f;
	ClusterWalReuseActionGuard guard = { 0 };
	ClusterWalReuseDenyReason reason;
	ClusterWalTerminalOutcome outcome;
	char held[MAXPGPATH], held_wal[MAXPGPATH], held_claim[MAXPGPATH];
	char wal_name[MAXFNAMELEN];

	v2_reuse_fixture(&f);
	v2_reuse_arm(&f, &guard);
	XLogFileName(wal_name, 1, 1, TEST_WAL_SEG_SIZE);
	snprintf(held, sizeof(held), "%s.held", f.generation);
	snprintf(held_wal, sizeof(held_wal), "%s/%s", held, wal_name);
	snprintf(held_claim, sizeof(held_claim), "%s/pgrac_thread.claim", held);
	UT_ASSERT_EQ(rename(f.generation, held), 0);
	UT_ASSERT_EQ(mkdir(f.generation, 0700), 0);
	write_test_wal_segment(f.generation, 1, 1, f.generation_wal, sizeof(f.generation_wal));
	v2_reuse_write_claim(&f, f.claim_bytes);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_l3_begin(&guard, CLUSTER_WAL_PHYSICAL_REMOVE, &reason),
				 CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_remove(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(access(held_wal, F_OK), -1);
	UT_ASSERT_EQ(errno, ENOENT);
	UT_ASSERT_EQ(access(f.generation_wal, F_OK), 0);
	UT_ASSERT_EQ(access(f.flat_wal, F_OK), 0);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_bookkeep(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_finish(&guard, &outcome, &reason),
				 CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(outcome, CLUSTER_WAL_TERMINAL_REMOVED);
	UT_ASSERT_EQ(unlink(held_claim), 0);
	UT_ASSERT_EQ(rmdir(held), 0);
	v2_reuse_fixture_cleanup(&f);
}

static bool
parse_fixture_u64(const char *text, uint64 *out)
{
	char *end = NULL;
	unsigned long long value;

	if (text == NULL || text[0] == '\0' || out == NULL)
		return false;
	errno = 0;
	value = strtoull(text, &end, 0);
	if (errno != 0 || end == text || *end != '\0')
		return false;
	*out = (uint64)value;
	return true;
}

static int
write_fixture_wal_segment(int argc, char **argv)
{
	XLogLongPageHeaderData header;
	char expected_name[MAXFNAMELEN];
	const char *basename;
	uint64 system_identifier;
	uint64 thread_value;
	uint64 tli_value;
	uint64 segno_value;
	uint64 segsize_value;
	int fd;
	bool created = false;
	bool ok = false;

	if (argc != 8 || !parse_fixture_u64(argv[3], &system_identifier)
		|| !parse_fixture_u64(argv[4], &thread_value) || !parse_fixture_u64(argv[5], &tli_value)
		|| !parse_fixture_u64(argv[6], &segno_value) || !parse_fixture_u64(argv[7], &segsize_value)
		|| system_identifier == 0 || thread_value == 0
		|| thread_value > CLUSTER_WAL_RETENTION_MAX_THREADS || tli_value == 0
		|| tli_value > UINT32_MAX || segsize_value > INT_MAX
		|| !IsValidWalSegSize((int)segsize_value) || segno_value > UINT64_MAX / segsize_value)
		return 2;
	XLogFileName(expected_name, (TimeLineID)tli_value, (XLogSegNo)segno_value, (int)segsize_value);
	basename = strrchr(argv[2], '/');
	basename = basename == NULL ? argv[2] : basename + 1;
	if (strcmp(basename, expected_name) != 0)
		return 2;
	fd = open(argv[2], O_RDWR | O_CREAT | O_EXCL, 0600);
	if (fd < 0)
		return 2;
	created = true;
	if (ftruncate(fd, (off_t)segsize_value) != 0)
		goto done;
	memset(&header, 0, sizeof(header));
	header.std.xlp_magic = XLOG_PAGE_MAGIC;
	header.std.xlp_info = XLP_LONG_HEADER;
	header.std.xlp_tli = (TimeLineID)tli_value;
	header.std.xlp_pageaddr = (XLogRecPtr)segno_value * segsize_value;
	header.std.xlp_thread_id = (uint16)thread_value;
	header.xlp_sysid = system_identifier;
	header.xlp_seg_size = (uint32)segsize_value;
	header.xlp_xlog_blcksz = XLOG_BLCKSZ;
	if (pwrite(fd, &header, sizeof(header), 0) != sizeof(header) || fsync(fd) != 0)
		goto done;
	ok = true;

done:
	if (close(fd) != 0)
		ok = false;
	if (created && !ok)
		(void)unlink(argv[2]);
	return ok ? 0 : 2;
}

static void
prepare_bound_recovery_guard(ClusterWalReuseActionGuard *guard,
							 const ClusterWalRetentionPinThreadRequest *request)
{
	ClusterWalReuseDenyReason reason;

	memset(guard, 0, sizeof(*guard));
	UT_ASSERT_EQ(cluster_wal_reuse_guard_init(guard, &reason), CLUSTER_WAL_GUARD_OK);
	guard->state = CLUSTER_WAL_GUARD_FENCE_ADMITTED_OR_NA;
	guard->entry = CLUSTER_WAL_REUSE_E1_CHECKPOINT_RESTARTPOINT;
	guard->action = CLUSTER_WAL_ACTION_RETIRE_RECYCLE_OR_REMOVE;
	guard->file.thread_id = request->duty.origin_thread_id;
	guard->file.kind = CLUSTER_WAL_FILE_NORMAL;
	guard->file.tli = 1;
	guard->file.segno = 1;
	guard->duty = request->duty;
	guard->root_read = request->root_read;
	guard->formation = request->formation;
	guard->needs_or_null = request->needs;
	guard->admissions_or_null = request->admissions;
}

static ClusterControlRootSnapshot
make_root(uint16 thread_id, ClusterControlRootLifecycle lifecycle, TimeLineID tli, XLogRecPtr lower,
		  XLogRecPtr tail)
{
	ClusterControlRootSnapshot root;

	memset(&root, 0, sizeof(root));
	root.identity.system_identifier = UINT64_C(0x1122334455667788);
	root.identity.origin_thread_id = thread_id;
	root.identity.origin_node_id = thread_id - 1;
	root.identity.thread_claim_created_at = 1;
	root.identity.thread_claim_crc32c = 1;
	root.identity.origin_owner_incarnation = 1;
	root.identity.root_lineage_seq = 1;
	root.lifecycle = lifecycle;
	root.root_publish_seq = 1;
	root.root_flags
		= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID;
	root.checkpoint_tli = tli;
	root.checkpoint_source_kind = CLUSTER_CONTROL_ROOT_CHECKPOINT_NATIVE_V1;
	root.checkpoint_lower_lsn = lower;
	root.checkpoint_record_crc32c = 1;
	if (tail != 0) {
		root.root_flags |= CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID;
		root.tail_tli = tli;
		root.tail_validation_kind = CLUSTER_CONTROL_ROOT_TAIL_WAL_RECORD_SCAN_V1;
		root.validated_tail_lsn_exclusive = tail;
		if (tail > lower) {
			root.root_flags |= CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID;
			root.tail_last_record_lsn = tail - 1;
			root.tail_last_record_crc32c = 1;
		}
	}
	return root;
}

static ClusterWalRootFoldInput
make_fold_input(bool configured, ClusterControlRootResult read_result,
				ClusterControlRootSnapshot root)
{
	ClusterWalRootFoldInput input;

	memset(&input, 0, sizeof(input));
	input.configured = configured;
	input.read_result = read_result;
	input.snapshot = root;
	return input;
}

UT_TEST(test_layout_contracts)
{
	UT_ASSERT_EQ(sizeof(ClusterWalFileIdentity), 16);
	UT_ASSERT_EQ(offsetof(ClusterWalFileIdentity, thread_id), 0);
	UT_ASSERT_EQ(offsetof(ClusterWalFileIdentity, kind), 2);
	UT_ASSERT_EQ(offsetof(ClusterWalFileIdentity, reserved_zero), 3);
	UT_ASSERT_EQ(offsetof(ClusterWalFileIdentity, tli), 4);
	UT_ASSERT_EQ(offsetof(ClusterWalFileIdentity, segno), 8);
	UT_ASSERT_EQ(sizeof(ClusterWalRetentionInterval), 24);
	UT_ASSERT_EQ(offsetof(ClusterWalRetentionInterval, thread_id), 0);
	UT_ASSERT_EQ(offsetof(ClusterWalRetentionInterval, tli), 4);
	UT_ASSERT_EQ(offsetof(ClusterWalRetentionInterval, start_lsn), 8);
	UT_ASSERT_EQ(offsetof(ClusterWalRetentionInterval, end_lsn), 16);
}

UT_TEST(test_thread_directory_exact_parse)
{
	uint16 thread_id = 999;

	UT_ASSERT_TRUE(cluster_wal_thread_directory_parse("thread_1", &thread_id));
	UT_ASSERT_EQ(thread_id, 1);
	UT_ASSERT_TRUE(cluster_wal_thread_directory_parse("thread_128", &thread_id));
	UT_ASSERT_EQ(thread_id, 128);
	UT_ASSERT_FALSE(cluster_wal_thread_directory_parse("thread_0", &thread_id));
	UT_ASSERT_EQ(thread_id, 0);
	UT_ASSERT_FALSE(cluster_wal_thread_directory_parse("thread_129", &thread_id));
	UT_ASSERT_FALSE(cluster_wal_thread_directory_parse("thread_01", &thread_id));
	UT_ASSERT_FALSE(cluster_wal_thread_directory_parse("thread_1/", &thread_id));
	UT_ASSERT_FALSE(cluster_wal_thread_directory_parse("THREAD_1", &thread_id));
	UT_ASSERT_FALSE(cluster_wal_thread_directory_parse(NULL, &thread_id));
}

UT_TEST(test_wal_basename_exact_parse)
{
	ClusterWalFileIdentity file;

	memset(&file, 0x5a, sizeof(file));
	UT_ASSERT_TRUE(
		cluster_wal_file_identity_parse("000000020000000100000003", 7, TEST_WAL_SEG_SIZE, &file));
	UT_ASSERT_EQ(file.thread_id, 7);
	UT_ASSERT_EQ(file.kind, CLUSTER_WAL_FILE_NORMAL);
	UT_ASSERT_EQ(file.reserved_zero, 0);
	UT_ASSERT_EQ(file.tli, 2);
	UT_ASSERT_EQ(file.segno, 259);
	UT_ASSERT_TRUE(cluster_wal_file_identity_valid(&file, TEST_WAL_SEG_SIZE));

	UT_ASSERT_TRUE(cluster_wal_file_identity_parse("000000020000000100000003.partial", 7,
												   TEST_WAL_SEG_SIZE, &file));
	UT_ASSERT_EQ(file.kind, CLUSTER_WAL_FILE_PARTIAL);
	UT_ASSERT_EQ(file.segno, 259);
}

UT_TEST(test_wal_basename_refuses_aliases)
{
	ClusterWalFileIdentity file;

	memset(&file, 0x5a, sizeof(file));
	UT_ASSERT_FALSE(cluster_wal_file_identity_parse("000000020000000100000003.done", 7,
													TEST_WAL_SEG_SIZE, &file));
	UT_ASSERT_EQ(file.thread_id, 0);
	UT_ASSERT_FALSE(cluster_wal_file_identity_parse("000000020000000100000003.partial.extra", 7,
													TEST_WAL_SEG_SIZE, &file));
	UT_ASSERT_FALSE(
		cluster_wal_file_identity_parse("00000002000000010000000a", 7, TEST_WAL_SEG_SIZE, &file));
	UT_ASSERT_FALSE(
		cluster_wal_file_identity_parse("000000000000000100000003", 7, TEST_WAL_SEG_SIZE, &file));
	UT_ASSERT_FALSE(
		cluster_wal_file_identity_parse("000000020000000100000100", 7, TEST_WAL_SEG_SIZE, &file));
	UT_ASSERT_FALSE(
		cluster_wal_file_identity_parse("000000020000000100000003", 0, TEST_WAL_SEG_SIZE, &file));
	UT_ASSERT_FALSE(
		cluster_wal_file_identity_parse("000000020000000100000003", 129, TEST_WAL_SEG_SIZE, &file));
	UT_ASSERT_FALSE(
		cluster_wal_file_identity_parse("000000020000000100000003", 7, 3 * 1024 * 1024, &file));
}

UT_TEST(test_long_header_exact_identity)
{
	ClusterWalFileIdentity file;
	XLogLongPageHeaderData header;
	uint64 system_identifier = UINT64_C(0x1122334455667788);

	UT_ASSERT_TRUE(
		cluster_wal_file_identity_parse("000000020000000100000003", 7, TEST_WAL_SEG_SIZE, &file));
	memset(&header, 0, sizeof(header));
	header.std.xlp_magic = XLOG_PAGE_MAGIC;
	header.std.xlp_info = XLP_LONG_HEADER;
	header.std.xlp_tli = file.tli;
	header.std.xlp_pageaddr = file.segno * (uint64)TEST_WAL_SEG_SIZE;
	header.std.xlp_thread_id = file.thread_id;
	header.xlp_sysid = system_identifier;
	header.xlp_seg_size = TEST_WAL_SEG_SIZE;
	header.xlp_xlog_blcksz = XLOG_BLCKSZ;
	UT_ASSERT_TRUE(
		cluster_wal_file_long_header_matches(&file, &header, system_identifier, TEST_WAL_SEG_SIZE));

	header.std.xlp_thread_id++;
	UT_ASSERT_FALSE(
		cluster_wal_file_long_header_matches(&file, &header, system_identifier, TEST_WAL_SEG_SIZE));
	header.std.xlp_thread_id--;
	header.std.xlp_tli++;
	UT_ASSERT_FALSE(
		cluster_wal_file_long_header_matches(&file, &header, system_identifier, TEST_WAL_SEG_SIZE));
	header.std.xlp_tli--;
	header.std.xlp_pageaddr += XLOG_BLCKSZ;
	UT_ASSERT_FALSE(
		cluster_wal_file_long_header_matches(&file, &header, system_identifier, TEST_WAL_SEG_SIZE));
	header.std.xlp_pageaddr -= XLOG_BLCKSZ;
	header.xlp_sysid++;
	UT_ASSERT_FALSE(
		cluster_wal_file_long_header_matches(&file, &header, system_identifier, TEST_WAL_SEG_SIZE));
}

UT_TEST(test_interval_half_open_segment_bounds)
{
	ClusterWalRetentionInterval interval = { .thread_id = 7,
											 .tli = 2,
											 .start_lsn = (XLogRecPtr)TEST_WAL_SEG_SIZE + 1,
											 .end_lsn = (XLogRecPtr)TEST_WAL_SEG_SIZE * 3 };
	XLogSegNo first = 999;
	XLogSegNo last = 999;

	UT_ASSERT_TRUE(
		cluster_wal_retention_interval_segment_bounds(&interval, TEST_WAL_SEG_SIZE, &first, &last));
	UT_ASSERT_EQ(first, 1);
	UT_ASSERT_EQ(last, 2);
	interval.end_lsn = interval.start_lsn;
	UT_ASSERT_FALSE(
		cluster_wal_retention_interval_segment_bounds(&interval, TEST_WAL_SEG_SIZE, &first, &last));
	UT_ASSERT_EQ(first, 0);
	UT_ASSERT_EQ(last, 0);
	interval.start_lsn = 0;
	interval.end_lsn = 1;
	UT_ASSERT_FALSE(
		cluster_wal_retention_interval_segment_bounds(&interval, TEST_WAL_SEG_SIZE, &first, &last));
}

UT_TEST(test_interval_intersection_requires_thread_and_tli)
{
	ClusterWalRetentionInterval interval = { .thread_id = 7,
											 .tli = 2,
											 .start_lsn = (XLogRecPtr)TEST_WAL_SEG_SIZE,
											 .end_lsn = (XLogRecPtr)TEST_WAL_SEG_SIZE * 3 };
	ClusterWalFileIdentity file
		= { .thread_id = 7, .kind = CLUSTER_WAL_FILE_NORMAL, .tli = 2, .segno = 2 };

	UT_ASSERT_TRUE(
		cluster_wal_retention_interval_intersects_file(&interval, &file, TEST_WAL_SEG_SIZE));
	file.segno = 3;
	UT_ASSERT_FALSE(
		cluster_wal_retention_interval_intersects_file(&interval, &file, TEST_WAL_SEG_SIZE));
	file.segno = 2;
	file.tli = 3;
	UT_ASSERT_FALSE(
		cluster_wal_retention_interval_intersects_file(&interval, &file, TEST_WAL_SEG_SIZE));
	file.tli = 2;
	file.thread_id = 8;
	UT_ASSERT_FALSE(
		cluster_wal_retention_interval_intersects_file(&interval, &file, TEST_WAL_SEG_SIZE));
}

UT_TEST(test_walr_resource_encoding_exact)
{
	ClusterResId resid;

	memset(&resid, 0x5a, sizeof(resid));
	UT_ASSERT_TRUE(cluster_wal_retention_resid_encode(1, &resid));
	UT_ASSERT_EQ(resid.field1, 1);
	UT_ASSERT_EQ(resid.field2, 0);
	UT_ASSERT_EQ(resid.field3, 0);
	UT_ASSERT_EQ(resid.field4, 0);
	UT_ASSERT_EQ(resid.type, 0xfa);
	UT_ASSERT_EQ(resid.lockmethodid, DEFAULT_LOCKMETHOD);
	UT_ASSERT_TRUE(cluster_wal_retention_resid_encode(128, &resid));
	UT_ASSERT_EQ(resid.field1, 128);
}

UT_TEST(test_walr_resource_encoding_refuses_invalid_thread)
{
	ClusterResId resid;

	memset(&resid, 0x5a, sizeof(resid));
	UT_ASSERT_FALSE(cluster_wal_retention_resid_encode(0, &resid));
	UT_ASSERT_EQ(resid.field1, 0);
	UT_ASSERT_EQ(resid.type, 0);
	memset(&resid, 0x5a, sizeof(resid));
	UT_ASSERT_FALSE(cluster_wal_retention_resid_encode(129, &resid));
	UT_ASSERT_EQ(resid.field1, 0);
	UT_ASSERT_EQ(resid.type, 0);
	UT_ASSERT_FALSE(cluster_wal_retention_resid_encode(1, NULL));
}

/* PGRAC: a v2 claim CRC does not prove a usable retention pin. Actual root
 * revalidation remains required after the grant. Author: SqlRush. */
UT_TEST(test_v2_pin_key_preserves_root_revalidation)
{
	ClusterWalRetentionInterval interval = { .thread_id = 1,
											 .tli = 1,
											 .start_lsn = TEST_WAL_SEG_SIZE,
											 .end_lsn = TEST_WAL_SEG_SIZE * 2 };
	ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);
	ClusterWalRetentionPin *pin = NULL;

	request.duty.thread_claim_crc32c = 0;
	reset_pin_fakes();
	cluster_shared_config = true;
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_NOT_NULL(pin);
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	fake_root_current = false;
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_STALE);
	UT_ASSERT_NULL(pin);
	cluster_shared_config = false;
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_INVALID);
	UT_ASSERT_NULL(pin);
	reset_pin_fakes();
}

UT_TEST(test_v2_pin_and_borrowed_publisher_refuse_rebuilding_walr)
{
	for (int sealed = 0; sealed < 2; sealed++) {
		ClusterWalRetentionInterval interval = { .thread_id = 1,
												 .tli = 1,
												 .start_lsn = TEST_WAL_SEG_SIZE,
												 .end_lsn = TEST_WAL_SEG_SIZE * 2 };
		ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);
		ClusterRecoverySerialGuard serial = make_serial_guard(&request);
		ClusterWalRetentionPin *pin = NULL;
		ClusterWalRootPublishGuard *publisher = NULL;

		reset_pin_fakes();
		cluster_shared_config = true;
		UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
		UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &serial), CLUSTER_WAL_PIN_OK);
		if (sealed)
			UT_ASSERT_EQ(cluster_wal_retention_pin_seal_for_root_publish(pin), CLUSTER_WAL_PIN_OK);
		fake_control_owner_current = false;
		if (sealed) {
			UT_ASSERT_EQ(cluster_wal_retention_root_publish_begin_exact(&request.root_read, true,
																		&publisher),
						 CLUSTER_WAL_PIN_STALE);
			if (publisher != NULL)
				(void)cluster_wal_retention_root_publish_end(&publisher);
		} else
			UT_ASSERT_EQ(cluster_wal_retention_pin_revalidate(pin), CLUSTER_WAL_PIN_STALE);
		UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	}
	reset_pin_fakes();
}

UT_TEST(test_v2_physical_reuse_requires_current_walr_not_held_boolean)
{
	V2ReuseFixture f;
	ClusterWalReuseActionGuard guard = { 0 };
	ClusterWalReuseDenyReason reason;
	ClusterWalTerminalOutcome outcome;
	ClusterWalReuseGuardResult l3_result;
	PgracExternalFenceNeedSetV1 *needs = NULL;

	v2_reuse_fixture(&f);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_init(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_preflight(&guard, &f.request, &needs, &reason),
				 CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_fence_admitted_nowait(&guard, NULL, &reason),
				 CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_arm(&guard, NULL, NULL, &reason), CLUSTER_WAL_GUARD_OK);
	fake_control_owner_current = false;
	l3_result = cluster_wal_reuse_guard_l3_begin(&guard, CLUSTER_WAL_PHYSICAL_REMOVE, &reason);
	UT_ASSERT_EQ(l3_result, CLUSTER_WAL_GUARD_BLOCKED);
	UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_GES_UNAVAILABLE);
	UT_ASSERT_EQ(access(f.generation_wal, F_OK), 0);
	if (l3_result == CLUSTER_WAL_GUARD_OK)
		(void)cluster_wal_reuse_guard_confirm_zero_mutation(&guard, CLUSTER_WAL_PHYSICAL_REMOVE,
															&reason);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_finish(&guard, &outcome, &reason),
				 CLUSTER_WALR_RELEASE_CONFIRMED);
	v2_reuse_fixture_cleanup(&f);
}

UT_TEST(test_v2_upgraded_pin_requires_its_exact_current_x_owner)
{
	ClusterWalRetentionInterval interval = { .thread_id = 1,
											 .tli = 1,
											 .start_lsn = TEST_WAL_SEG_SIZE,
											 .end_lsn = TEST_WAL_SEG_SIZE * 2 };
	ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);
	ClusterRecoverySerialGuard serial = make_serial_guard(&request);
	ClusterWalRetentionPin *pin = NULL;
	ClusterWalReuseActionGuard guard;
	ClusterWalReuseDenyReason reason;

	reset_pin_fakes();
	cluster_shared_config = true;
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &serial), CLUSTER_WAL_PIN_OK);
	prepare_bound_recovery_guard(&guard, &request);
	fake_s5_result = CLUSTER_LOCK_ACQUIRE_OK_CONVERTED;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_arm(&guard, &serial, pin, &reason), CLUSTER_WAL_GUARD_OK);
	fake_control_owner_mode = ExclusiveLock;
	UT_ASSERT_EQ(cluster_wal_retention_pin_revalidate(pin), CLUSTER_WAL_PIN_OK);
	guard.walr.acquire_request.control_owner_id++;
	UT_ASSERT_EQ(cluster_wal_retention_pin_revalidate(pin), CLUSTER_WAL_PIN_STALE);
	guard.walr.acquire_request.control_owner_id--;
	if (fake_resource_release_callback != NULL)
		fake_resource_release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false,
									   fake_resource_release_arg);
	reset_pin_fakes();
}

UT_TEST(test_root_publisher_requires_resource_owner)
{
	ClusterWalRetentionInterval interval = { .thread_id = 1,
											 .tli = 1,
											 .start_lsn = TEST_WAL_SEG_SIZE,
											 .end_lsn = TEST_WAL_SEG_SIZE * 2 };
	ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);
	ClusterWalRootPublishGuard *publisher = NULL;
	ResourceOwner saved_owner = CurrentResourceOwner;

	reset_pin_fakes();
	CurrentResourceOwner = NULL;
	UT_ASSERT_EQ(
		cluster_wal_retention_root_publish_begin_exact(&request.root_read, false, &publisher),
		CLUSTER_WAL_PIN_INVALID);
	UT_ASSERT_NULL(publisher);
	UT_ASSERT_EQ(fake_acquire_call_count, 0);
	CurrentResourceOwner = saved_owner;
	UT_ASSERT_EQ(
		cluster_wal_retention_root_publish_begin_exact(&request.root_read, false, &publisher),
		CLUSTER_WAL_PIN_OK);
	UT_ASSERT_NOT_NULL(publisher);
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&publisher),
				 CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_NULL(publisher);
}

/* PGRAC: uncertain cleanup must name its actual original WALR owner; an
 * unvalidated guard must not invent that identity or be dereferenced. */
UT_TEST(test_root_release_diagnostic_preserves_original_identity)
{
	ClusterWalRetentionInterval interval = { .thread_id = 1,
											 .tli = 1,
											 .start_lsn = TEST_WAL_SEG_SIZE,
											 .end_lsn = TEST_WAL_SEG_SIZE * 2 };
	ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);
	ClusterWalRootPublishGuard *publisher = NULL;
	ClusterWalRootPublishGuard *foreign = (ClusterWalRootPublishGuard *)(uintptr_t)1;
	char expected[80];

	reset_pin_fakes();
	UT_ASSERT_EQ(
		cluster_wal_retention_root_publish_begin_exact(&request.root_read, false, &publisher),
		CLUSTER_WAL_PIN_OK);
	fake_release_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	capture_release_log = true;
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&publisher),
				 CLUSTER_WALR_RELEASE_UNCONFIRMED);
	UT_ASSERT_NOT_NULL(publisher);
	UT_ASSERT_EQ(release_log_count, 1);
	UT_ASSERT(strstr(release_log_detail, "action=release_walr") != NULL);
	snprintf(expected, sizeof(expected), "request=" UINT64_FORMAT, released_request.request_id);
	UT_ASSERT(strstr(release_log_detail, expected) != NULL);
	snprintf(expected, sizeof(expected), "epoch=" UINT64_FORMAT,
			 released_request.holder.cluster_epoch);
	UT_ASSERT(strstr(release_log_detail, expected) != NULL);
	snprintf(expected, sizeof(expected), "control_owner_id=" UINT64_FORMAT,
			 released_request.control_owner_id);
	UT_ASSERT(strstr(release_log_detail, expected) != NULL);
	snprintf(expected, sizeof(expected), "result=%u", CLUSTER_WALR_RELEASE_UNCONFIRMED);
	UT_ASSERT(strstr(release_log_detail, expected) != NULL);
	UT_ASSERT_EQ(fake_release_call_count, 1);
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&publisher),
				 CLUSTER_WALR_RELEASE_UNCONFIRMED);
	UT_ASSERT_EQ(release_log_count, 1);
	UT_ASSERT_EQ(fake_release_call_count, 2);
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&foreign), CLUSTER_WALR_RELEASE_INVALID);
	UT_ASSERT_EQ(release_log_count, 2);
	UT_ASSERT(strstr(release_log_detail, "identity=unavailable") != NULL);
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&foreign), CLUSTER_WALR_RELEASE_INVALID);
	UT_ASSERT_EQ(release_log_count, 2);
	UT_ASSERT_EQ(fake_release_call_count, 2);
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&publisher),
				 CLUSTER_WALR_RELEASE_UNCONFIRMED);
	UT_ASSERT_EQ(release_log_count, 2);
	UT_ASSERT_EQ(fake_release_call_count, 3);
	fake_release_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&publisher),
				 CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_NULL(publisher);
	UT_ASSERT_EQ(release_log_count, 2);
	UT_ASSERT_EQ(
		cluster_wal_retention_root_publish_begin_exact(&request.root_read, false, &publisher),
		CLUSTER_WAL_PIN_OK);
	fake_release_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&publisher),
				 CLUSTER_WALR_RELEASE_UNCONFIRMED);
	UT_ASSERT_EQ(release_log_count, 3);
	fake_release_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&publisher),
				 CLUSTER_WALR_RELEASE_CONFIRMED);
	capture_release_log = false;
}

UT_TEST(test_pin_one_thread_acquire_and_confirmed_release)
{
	ClusterWalRetentionInterval interval = { .thread_id = 1,
											 .tli = 1,
											 .start_lsn = TEST_WAL_SEG_SIZE,
											 .end_lsn = TEST_WAL_SEG_SIZE * 2 };
	ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);
	ClusterWalRetentionPin *pin = NULL;
	ClusterWalRootPublishGuard *publisher = NULL;

	reset_pin_fakes();
	fake_native_acquire_result = LOCKACQUIRE_NOT_AVAIL;
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_UNAVAILABLE);
	UT_ASSERT_NULL(pin);
	UT_ASSERT_EQ(fake_native_acquire_call_count, 1);
	UT_ASSERT_EQ(fake_s7_call_count, 1);

	reset_pin_fakes();
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_NOT_NULL(pin);
	UT_ASSERT_EQ(fake_acquire_call_count, 1);
	UT_ASSERT_EQ(fake_acquire_requests[0].resid.field1, 1);
	UT_ASSERT_EQ(fake_acquire_requests[0].resid.type, 0xfa);
	UT_ASSERT_EQ(fake_acquire_requests[0].lockmode, ShareLock);
	UT_ASSERT(fake_acquire_requests[0].dontwait);
	UT_ASSERT_EQ(fake_native_acquire_call_count, 1);
	UT_ASSERT_EQ(fake_s5_call_count, 1);
	UT_ASSERT_EQ(
		cluster_wal_retention_pin_acquire(&request, 1, &(ClusterWalRetentionPin *){ NULL }),
		CLUSTER_WAL_PIN_CAPACITY);
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_NULL(pin);
	UT_ASSERT_EQ(fake_release_call_count, 1);
	UT_ASSERT_EQ(fake_release_threads[0], 1);
	UT_ASSERT_EQ(fake_native_release_call_count, 1);

	reset_pin_fakes();
	fake_native_acquire_result = LOCKACQUIRE_NOT_AVAIL;
	UT_ASSERT_EQ(
		cluster_wal_retention_root_publish_begin_exact(&request.root_read, false, &publisher),
		CLUSTER_WAL_PIN_UNAVAILABLE);
	UT_ASSERT_NULL(publisher);
	UT_ASSERT_EQ(fake_native_acquire_call_count, 1);
	UT_ASSERT_EQ(fake_s7_call_count, 1);

	reset_pin_fakes();
	UT_ASSERT_EQ(
		cluster_wal_retention_root_publish_begin_exact(&request.root_read, false, &publisher),
		CLUSTER_WAL_PIN_OK);
	UT_ASSERT_NOT_NULL(publisher);
	UT_ASSERT_EQ(fake_native_acquire_call_count, 1);
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&publisher),
				 CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_NULL(publisher);
	UT_ASSERT_EQ(fake_native_release_call_count, 1);
}

UT_TEST(test_pin_acquire_is_sorted_all_or_none)
{
	ClusterWalRetentionInterval intervals[2] = { { .thread_id = 1,
												   .tli = 1,
												   .start_lsn = TEST_WAL_SEG_SIZE,
												   .end_lsn = TEST_WAL_SEG_SIZE * 2 },
												 { .thread_id = 2,
												   .tli = 1,
												   .start_lsn = TEST_WAL_SEG_SIZE * 2,
												   .end_lsn = TEST_WAL_SEG_SIZE * 3 } };
	ClusterWalRetentionPinThreadRequest requests[2]
		= { make_pin_request(1, &intervals[0], 1), make_pin_request(2, &intervals[1], 1) };
	ClusterWalRetentionPin *pin = NULL;

	reset_pin_fakes();
	fake_acquire_result_count = 2;
	fake_acquire_results[0] = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	fake_acquire_results[1] = CLUSTER_LOCK_ACQUIRE_NOT_AVAIL;
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(requests, 2, &pin), CLUSTER_WAL_PIN_UNAVAILABLE);
	UT_ASSERT_NULL(pin);
	UT_ASSERT_EQ(fake_acquire_call_count, 2);
	UT_ASSERT_EQ(fake_release_call_count, 1);
	UT_ASSERT_EQ(fake_release_threads[0], 1);

	requests[0] = make_pin_request(2, &intervals[1], 1);
	requests[1] = make_pin_request(1, &intervals[0], 1);
	reset_pin_fakes();
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(requests, 2, &pin), CLUSTER_WAL_PIN_INVALID);
	UT_ASSERT_EQ(fake_acquire_call_count, 0);
}

UT_TEST(test_pin_uncertain_rollback_remains_cleanup_only)
{
	ClusterWalRetentionInterval intervals[2] = { { .thread_id = 1,
												   .tli = 1,
												   .start_lsn = TEST_WAL_SEG_SIZE,
												   .end_lsn = TEST_WAL_SEG_SIZE * 2 },
												 { .thread_id = 2,
												   .tli = 1,
												   .start_lsn = TEST_WAL_SEG_SIZE * 2,
												   .end_lsn = TEST_WAL_SEG_SIZE * 3 } };
	ClusterWalRetentionPinThreadRequest requests[2]
		= { make_pin_request(1, &intervals[0], 1), make_pin_request(2, &intervals[1], 1) };
	ClusterWalRetentionPin *pin = NULL;
	ClusterWalRetentionPin *other = NULL;

	reset_pin_fakes();
	fake_acquire_result_count = 2;
	fake_acquire_results[0] = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	fake_acquire_results[1] = CLUSTER_LOCK_ACQUIRE_NOT_AVAIL;
	fake_release_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(requests, 2, &pin),
				 CLUSTER_WAL_PIN_RELEASE_UNCERTAIN);
	UT_ASSERT_NOT_NULL(pin);
	UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &(ClusterRecoverySerialGuard){ 0 }),
				 CLUSTER_WAL_PIN_RELEASE_UNCERTAIN);
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(requests, 2, &other), CLUSTER_WAL_PIN_CAPACITY);
	UT_ASSERT_NULL(other);
	fake_release_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_NULL(pin);
}

UT_TEST(test_pin_bind_revalidate_and_seal_closed_fsm)
{
	ClusterWalRetentionInterval interval = { .thread_id = 1,
											 .tli = 1,
											 .start_lsn = TEST_WAL_SEG_SIZE,
											 .end_lsn = TEST_WAL_SEG_SIZE * 2 };
	ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);
	ClusterRecoverySerialGuard serial = make_serial_guard(&request);
	ClusterWalRetentionPin *pin = NULL;

	reset_pin_fakes();
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &serial), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &serial), CLUSTER_WAL_PIN_INVALID);
	UT_ASSERT_EQ(cluster_wal_retention_pin_revalidate(pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_seal_for_root_publish(pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_revalidate(pin), CLUSTER_WAL_PIN_INVALID);
	UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &serial), CLUSTER_WAL_PIN_INVALID);
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
}

UT_TEST(test_unbound_pin_has_pre_ir_slow_revalidation_only)
{
	ClusterWalRetentionInterval interval = { .thread_id = 1,
											 .tli = 1,
											 .start_lsn = TEST_WAL_SEG_SIZE,
											 .end_lsn = TEST_WAL_SEG_SIZE * 2 };
	ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);
	ClusterRecoverySerialGuard serial = make_serial_guard(&request);
	ClusterWalRetentionPin *pin = NULL;

	reset_pin_fakes();
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_preflight_revalidate_wait_v1(pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_revalidate(pin), CLUSTER_WAL_PIN_INVALID);
	UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &serial), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_preflight_revalidate_wait_v1(pin),
				 CLUSTER_WAL_PIN_INVALID);
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
	fake_root_current = false;
	UT_ASSERT_EQ(cluster_wal_retention_pin_preflight_revalidate_wait_v1(pin),
				 CLUSTER_WAL_PIN_STALE);
	UT_ASSERT(cluster_wal_retention_pin_bind_one(pin, &serial) != CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
}

UT_TEST(test_cold_borrow_returns_original_complete_set_without_acquiring)
{
	for (uint16 n = 1; n <= 2; n++) {
		ClusterWalRetentionInterval intervals[2]
			= { { 1, 0, 1, TEST_WAL_SEG_SIZE, TEST_WAL_SEG_SIZE * 2 },
				{ 2, 0, 1, TEST_WAL_SEG_SIZE, TEST_WAL_SEG_SIZE * 2 } };
		ClusterWalRetentionPinThreadRequest requests[2]
			= { make_pin_request(1, &intervals[0], 1), make_pin_request(2, &intervals[1], 1) };
		ClusterRecoverySerialGuardSet set = { 0 };
		ClusterRecoverySerialGuard *borrowed[2] = { NULL, NULL };
		ClusterWalRetentionPin *pin = NULL, *borrowed_pin = NULL;
		uint16 count = 77;

		reset_pin_fakes();
		cluster_shared_config = true;
		set.count = n;
		for (uint16 i = 0; i < n; i++) {
			set.guards[i] = make_serial_guard(&requests[i]);
			set.guards[i].mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
		}
		UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(requests, n, &pin), CLUSTER_WAL_PIN_OK);
		UT_ASSERT_EQ(n == 1 ? cluster_wal_retention_pin_bind_one(pin, &set.guards[0])
							: cluster_wal_retention_pin_bind_set(pin, &set),
					 CLUSTER_WAL_PIN_OK);
		UT_ASSERT_EQ(cluster_wal_retention_pin_borrow_cold_v1(&borrowed_pin, borrowed, 2, &count),
					 CLUSTER_WAL_PIN_OK);
		UT_ASSERT(borrowed_pin == pin);
		UT_ASSERT_EQ(count, n);
		for (uint16 i = 0; i < n; i++)
			UT_ASSERT(borrowed[i] == &set.guards[i]);
		UT_ASSERT_EQ(fake_acquire_call_count, n);
		UT_ASSERT_EQ(fake_release_call_count, 0);
		UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
		UT_ASSERT_EQ(fake_release_call_count, n);
		/* Borrowing never owns or releases the original IR guards. */
		UT_ASSERT(set.guards[0].held);
	}
}

UT_TEST(test_cold_borrow_never_exports_a_partial_or_stale_set)
{
	for (int variant = 0; variant < 6; variant++) {
		ClusterWalRetentionInterval intervals[2]
			= { { 1, 0, 1, TEST_WAL_SEG_SIZE, TEST_WAL_SEG_SIZE * 2 },
				{ 2, 0, 1, TEST_WAL_SEG_SIZE, TEST_WAL_SEG_SIZE * 2 } };
		ClusterWalRetentionPinThreadRequest requests[2]
			= { make_pin_request(1, &intervals[0], 1), make_pin_request(2, &intervals[1], 1) };
		ClusterRecoverySerialGuardSet set = { 0 };
		ClusterRecoverySerialGuard *borrowed[2] = { NULL, NULL };
		ClusterWalRetentionPin *pin = NULL, *borrowed_pin = NULL;
		uint16 count = 77;

		reset_pin_fakes();
		cluster_shared_config = true;
		set.count = 2;
		for (uint16 i = 0; i < 2; i++) {
			set.guards[i] = make_serial_guard(&requests[i]);
			set.guards[i].mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
		}
		UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(requests, 2, &pin), CLUSTER_WAL_PIN_OK);
		UT_ASSERT_EQ(cluster_wal_retention_pin_bind_set(pin, &set), CLUSTER_WAL_PIN_OK);
		if (variant == 0)
			set.guards[1].held = false;
		if (variant == 1)
			set.guards[1].mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
		if (variant == 2)
			set.guards[1].root_read_token.record_crc32c++;
		if (variant == 3)
			fake_serial_result = CLUSTER_RECOVERY_SERIAL_FENCE_STALE;
		if (variant == 4)
			fake_control_owner_stale_thread = 2;
		if (variant == 5)
			set.count = 1;
		UT_ASSERT(cluster_wal_retention_pin_borrow_cold_v1(&borrowed_pin, borrowed, 2, &count)
				  != CLUSTER_WAL_PIN_OK);
		UT_ASSERT_NULL(borrowed_pin);
		UT_ASSERT_NULL(borrowed[0]);
		UT_ASSERT_NULL(borrowed[1]);
		UT_ASSERT_EQ(count, 77);
		UT_ASSERT_EQ(fake_acquire_call_count, 2);
		UT_ASSERT_EQ(fake_release_call_count, 0);
		fake_serial_result = CLUSTER_RECOVERY_SERIAL_CURRENT;
		fake_control_owner_stale_thread = 0;
		UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	}
}

UT_TEST(test_cold_borrow_preserves_the_original_owner_and_closed_phase)
{
	for (int variant = 0; variant < 7; variant++) {
		ClusterWalRetentionInterval interval
			= { 1, 0, 1, TEST_WAL_SEG_SIZE, TEST_WAL_SEG_SIZE * 2 };
		ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);
		ClusterRecoverySerialGuard guard = make_serial_guard(&request), *borrowed = NULL;
		ClusterWalRetentionPin *pin = NULL, *borrowed_pin = NULL;
		ResourceOwner owner;
		uint16 count = 77;

		reset_pin_fakes();
		cluster_shared_config = true;
		owner = CurrentResourceOwner;
		guard.mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
		UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
		if (variant != 0)
			UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &guard), CLUSTER_WAL_PIN_OK);
		if (variant == 1)
			UT_ASSERT_EQ(cluster_wal_retention_pin_seal_for_root_publish(pin), CLUSTER_WAL_PIN_OK);
		if (variant == 2)
			CurrentResourceOwner = (ResourceOwner)(uintptr_t)0x2222;
		if (variant == 3)
			MyProcPid++;
		if (variant == 4)
			UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
		if (variant == 6)
			cluster_shared_config = false;
		UT_ASSERT(cluster_wal_retention_pin_borrow_cold_v1(&borrowed_pin, &borrowed,
														   variant == 5 ? 0 : 1, &count)
				  != CLUSTER_WAL_PIN_OK);
		UT_ASSERT_NULL(borrowed_pin);
		UT_ASSERT_NULL(borrowed);
		UT_ASSERT_EQ(count, 77);
		CurrentResourceOwner = owner;
		if (variant == 3)
			MyProcPid--;
		if (pin != NULL)
			UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	}
}

UT_TEST(test_pin_revalidation_drift_poisoned_until_release)
{
	ClusterWalRetentionInterval interval = { .thread_id = 1,
											 .tli = 1,
											 .start_lsn = TEST_WAL_SEG_SIZE,
											 .end_lsn = TEST_WAL_SEG_SIZE * 2 };
	ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);
	ClusterRecoverySerialGuard serial = make_serial_guard(&request);
	ClusterWalRetentionPin *pin = NULL;

	reset_pin_fakes();
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &serial), CLUSTER_WAL_PIN_OK);
	fake_serial_result = CLUSTER_RECOVERY_SERIAL_FENCE_STALE;
	UT_ASSERT_EQ(cluster_wal_retention_pin_revalidate(pin), CLUSTER_WAL_PIN_STALE);
	UT_ASSERT_TRUE(cluster_wal_retention_active_pin_present());
	fake_serial_result = CLUSTER_RECOVERY_SERIAL_CURRENT;
	UT_ASSERT_EQ(cluster_wal_retention_pin_revalidate(pin), CLUSTER_WAL_PIN_STALE);
	UT_ASSERT_EQ(cluster_wal_retention_pin_seal_for_root_publish(pin), CLUSTER_WAL_PIN_STALE);
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_FALSE(cluster_wal_retention_active_pin_present());
}

UT_TEST(test_sealed_pin_root_publish_requires_whole_root_token)
{
	ClusterWalRetentionInterval interval = { .thread_id = 1,
											 .tli = 1,
											 .start_lsn = TEST_WAL_SEG_SIZE,
											 .end_lsn = TEST_WAL_SEG_SIZE * 2 };
	ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);
	ClusterRecoverySerialGuard serial = make_serial_guard(&request);
	ClusterControlRootReadToken drifted_root = request.root_read;
	ClusterWalRetentionPin *pin = NULL;
	ClusterWalRootPublishGuard *publisher = NULL;

	reset_pin_fakes();
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &serial), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_seal_for_root_publish(pin), CLUSTER_WAL_PIN_OK);
	drifted_root.root_publish_seq++;
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_begin_exact(&drifted_root, true, &publisher),
				 CLUSTER_WAL_PIN_STALE);
	UT_ASSERT_NULL(publisher);
	serial.held = false;
	UT_ASSERT_EQ(
		cluster_wal_retention_root_publish_begin_exact(&request.root_read, true, &publisher),
		CLUSTER_WAL_PIN_OK);
	UT_ASSERT_NOT_NULL(publisher);
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&publisher),
				 CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
}

static ClusterControlRootSnapshot
make_pin_root_snapshot(const ClusterWalRetentionPinThreadRequest *request,
					   const ClusterWalRetentionInterval *interval)
{
	ClusterControlRootSnapshot root;

	memset(&root, 0, sizeof(root));
	root.identity = request->duty;
	root.lifecycle = request->root_read.lifecycle;
	root.root_flags = request->root_read.root_flags;
	root.checkpoint_tli = interval->tli;
	root.tail_tli = interval->tli;
	root.checkpoint_source_kind = CLUSTER_CONTROL_ROOT_CHECKPOINT_NATIVE_V1;
	root.tail_validation_kind = CLUSTER_CONTROL_ROOT_TAIL_WAL_RECORD_SCAN_V1;
	root.checkpoint_lower_lsn = interval->start_lsn;
	root.validated_tail_lsn_exclusive = interval->end_lsn;
	root.checkpoint_record_crc32c = 11;
	root.tail_last_record_lsn = interval->end_lsn - 64;
	root.tail_last_record_crc32c = 12;
	return root;
}

UT_TEST(test_sealed_publisher_revalidates_only_after_exact_ir_release)
{
	ClusterWalRetentionInterval interval = { .thread_id = 1,
											 .tli = 1,
											 .start_lsn = TEST_WAL_SEG_SIZE,
											 .end_lsn = TEST_WAL_SEG_SIZE * 2 };
	ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);
	ClusterRecoverySerialGuard serial = make_serial_guard(&request);
	ClusterWalRetentionPin *pin = NULL;
	ClusterWalRootPublishGuard *publisher = NULL;
	ClusterControlRootReadToken drifted = request.root_read;
	reset_pin_fakes();
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &serial), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_seal_for_root_publish(pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(
		cluster_wal_retention_root_publish_begin_exact(&request.root_read, true, &publisher),
		CLUSTER_WAL_PIN_STALE);
	UT_ASSERT_NULL(publisher);
	UT_ASSERT_EQ(fake_acquire_call_count, 1); /* never try X while IR is held */
	if (publisher != NULL)
		(void)cluster_wal_retention_root_publish_end(&publisher);
	serial.held = false;
	UT_ASSERT_EQ(
		cluster_wal_retention_root_publish_begin_exact(&request.root_read, true, &publisher),
		CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(fake_acquire_call_count, 2);
	UT_ASSERT_EQ(fake_acquire_requests[1].op, CLUSTER_LOCK_OP_CONVERT);
	UT_ASSERT_EQ(fake_acquire_requests[1].current_mode, ShareLock);
	UT_ASSERT_EQ(fake_acquire_requests[1].lockmode, ExclusiveLock);
	UT_ASSERT(fake_acquire_requests[1].dontwait);
	UT_ASSERT_TRUE(
		cluster_wal_retention_root_publish_sealed_current(publisher, &request.root_read));
	serial.release_uncertain = true;
	UT_ASSERT_FALSE(
		cluster_wal_retention_root_publish_sealed_current(publisher, &request.root_read));
	serial.release_uncertain = false;
	drifted.file_txn_seq++;
	UT_ASSERT_FALSE(cluster_wal_retention_root_publish_sealed_current(publisher, &drifted));
	UT_ASSERT_FALSE(cluster_wal_retention_root_publish_sealed_current(NULL, &request.root_read));
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&publisher),
				 CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(fake_acquire_call_count, 3);
	UT_ASSERT_EQ(fake_acquire_requests[2].lockmode, ShareLock);
	UT_ASSERT_EQ(fake_native_release_call_count, 2); /* temporary X and extra S */
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
}

UT_TEST(test_sealed_shared_publisher_requires_its_exact_converted_owner)
{
	ClusterWalRetentionInterval interval = { .thread_id = 1,
											 .tli = 1,
											 .start_lsn = TEST_WAL_SEG_SIZE,
											 .end_lsn = TEST_WAL_SEG_SIZE * 2 };
	ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);
	ClusterRecoverySerialGuard serial = make_serial_guard(&request);
	ClusterWalRetentionPin *pin = NULL;
	ClusterWalRootPublishGuard *publisher = NULL;
	reset_pin_fakes();
	cluster_shared_config = true;
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &serial), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_seal_for_root_publish(pin), CLUSTER_WAL_PIN_OK);
	serial.held = false;
	UT_ASSERT_EQ(
		cluster_wal_retention_root_publish_begin_exact(&request.root_read, true, &publisher),
		CLUSTER_WAL_PIN_OK);
	fake_control_owner_mode = ExclusiveLock; /* the old S identity is no longer usable */
	UT_ASSERT(cluster_wal_retention_root_publish_sealed_current(publisher, &request.root_read));
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_INVALID);
	UT_ASSERT_EQ(fake_release_call_count, 0);
	fake_control_owner_current = false;
	UT_ASSERT_FALSE(
		cluster_wal_retention_root_publish_sealed_current(publisher, &request.root_read));
	fake_control_owner_current = true;
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&publisher),
				 CLUSTER_WALR_RELEASE_CONFIRMED);
	fake_control_owner_mode = ShareLock;
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(fake_release_call_count, 1);
	UT_ASSERT_EQ(fake_native_release_call_count, 3);
	reset_pin_fakes();
}

UT_TEST(test_sealed_publish_contention_preserves_pin_and_retries_original_holder)
{
	ClusterWalRetentionInterval interval = { .thread_id = 1,
											 .tli = 1,
											 .start_lsn = TEST_WAL_SEG_SIZE,
											 .end_lsn = TEST_WAL_SEG_SIZE * 2 };
	ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);
	ClusterRecoverySerialGuard serial = make_serial_guard(&request);
	ClusterWalRetentionPin *pin = NULL;
	ClusterWalRootPublishGuard *publisher = NULL;
	reset_pin_fakes();
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &serial), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_seal_for_root_publish(pin), CLUSTER_WAL_PIN_OK);
	serial.held = false;
	/* Model B retaining S across DATA after A releases IR. The actual
	 * stable-owner/native exclusion interleaving is in control_walr. */
	fake_native_acquire_result = LOCKACQUIRE_NOT_AVAIL;
	UT_ASSERT_EQ(
		cluster_wal_retention_root_publish_begin_exact(&request.root_read, true, &publisher),
		CLUSTER_WAL_PIN_UNAVAILABLE);
	UT_ASSERT_NULL(publisher);
	UT_ASSERT_EQ(fake_native_acquire_call_count, 2);
	UT_ASSERT_EQ(fake_release_call_count, 0);
	if (publisher != NULL)
		(void)cluster_wal_retention_root_publish_end(&publisher);
	fake_native_acquire_result = LOCKACQUIRE_OK;
	UT_ASSERT_EQ(
		cluster_wal_retention_root_publish_begin_exact(&request.root_read, true, &publisher),
		CLUSTER_WAL_PIN_OK);
	UT_ASSERT(cluster_wal_retention_root_publish_sealed_current(publisher, &request.root_read));
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&publisher),
				 CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(fake_release_call_count, 1); /* same coordinated holder */
	UT_ASSERT_EQ(fake_native_release_call_count, 3);
}

UT_TEST(test_sealed_publish_error_cleanup_owns_conversion_and_pin)
{
	for (int after_x = 0; after_x < 2; after_x++) {
		ClusterWalRetentionInterval interval = { .thread_id = 1,
												 .tli = 1,
												 .start_lsn = TEST_WAL_SEG_SIZE,
												 .end_lsn = TEST_WAL_SEG_SIZE * 2 };
		ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);
		ClusterRecoverySerialGuard serial = make_serial_guard(&request);
		ClusterWalRetentionPin *pin = NULL;
		ClusterWalRootPublishGuard *publisher = NULL;
		volatile bool caught = false;
		reset_pin_fakes();
		UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
		UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &serial), CLUSTER_WAL_PIN_OK);
		UT_ASSERT_EQ(cluster_wal_retention_pin_seal_for_root_publish(pin), CLUSTER_WAL_PIN_OK);
		serial.held = false;
		fake_acquire_error_at = after_x ? 0 : 2;
		PG_TRY();
		{
			UT_ASSERT_EQ(cluster_wal_retention_root_publish_begin_exact(&request.root_read, true,
																		&publisher),
						 CLUSTER_WAL_PIN_OK);
			if (after_x)
				pg_re_throw(); /* CF/CAS/readback ERROR after the exclusive handoff */
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		fake_resource_release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false,
									   fake_resource_release_arg);
		UT_ASSERT_FALSE(cluster_wal_retention_active_pin_present());
		UT_ASSERT_EQ(fake_release_call_count, 1);
		UT_ASSERT_EQ(fake_native_release_call_count, after_x ? 2 : 1);
	}
}

UT_TEST(test_sealed_publish_uncertain_downgrade_is_cleanup_only)
{
	ClusterWalRetentionInterval interval = { .thread_id = 1,
											 .tli = 1,
											 .start_lsn = TEST_WAL_SEG_SIZE,
											 .end_lsn = TEST_WAL_SEG_SIZE * 2 };
	ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);
	ClusterRecoverySerialGuard serial = make_serial_guard(&request);
	ClusterWalRetentionPin *pin = NULL;
	ClusterWalRootPublishGuard *publisher = NULL;
	reset_pin_fakes();
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &serial), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_seal_for_root_publish(pin), CLUSTER_WAL_PIN_OK);
	serial.held = false;
	UT_ASSERT_EQ(
		cluster_wal_retention_root_publish_begin_exact(&request.root_read, true, &publisher),
		CLUSTER_WAL_PIN_OK);
	fake_native_acquire_result = LOCKACQUIRE_NOT_AVAIL;
	capture_release_log = true;
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&publisher),
				 CLUSTER_WALR_RELEASE_UNCONFIRMED);
	UT_ASSERT_NOT_NULL(publisher);
	UT_ASSERT_EQ(release_log_count, 1);
	UT_ASSERT(strstr(release_log_detail, "action=downgrade_walr") != NULL);
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&publisher),
				 CLUSTER_WALR_RELEASE_UNCONFIRMED);
	UT_ASSERT_EQ(release_log_count, 1);
	capture_release_log = false;
	UT_ASSERT_FALSE(
		cluster_wal_retention_root_publish_sealed_current(publisher, &request.root_read));
	fake_resource_release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false,
								   fake_resource_release_arg);
	UT_ASSERT_FALSE(cluster_wal_retention_active_pin_present());
	UT_ASSERT_EQ(fake_release_call_count, 1);
	UT_ASSERT_EQ(fake_native_release_call_count, 2);
}

UT_TEST(test_sealed_publisher_rechecks_formation_and_fence_without_cf)
{
	ClusterWalRetentionInterval interval = { .thread_id = 1,
											 .tli = 1,
											 .start_lsn = TEST_WAL_SEG_SIZE,
											 .end_lsn = TEST_WAL_SEG_SIZE * 2 };
	ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);
	ClusterRecoverySerialGuard serial = make_serial_guard(&request);
	ClusterWalRetentionPin *pin = NULL;
	ClusterWalRootPublishGuard *publisher = NULL;
	reset_pin_fakes();
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &serial), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_seal_for_root_publish(pin), CLUSTER_WAL_PIN_OK);
	serial.held = false;
	UT_ASSERT_EQ(
		cluster_wal_retention_root_publish_begin_exact(&request.root_read, true, &publisher),
		CLUSTER_WAL_PIN_OK);
	fake_root_current = false; /* The caller holds CF; no nested root read allowed. */
	UT_ASSERT_TRUE(
		cluster_wal_retention_root_publish_sealed_current(publisher, &request.root_read));
	fake_formation_current = false;
	UT_ASSERT_FALSE(
		cluster_wal_retention_root_publish_sealed_current(publisher, &request.root_read));
	fake_formation_current = true;
	fake_admission_current = false;
	UT_ASSERT_FALSE(
		cluster_wal_retention_root_publish_sealed_current(publisher, &request.root_read));
	fake_admission_current = true;
	UT_ASSERT_TRUE(
		cluster_wal_retention_root_publish_sealed_current(publisher, &request.root_read));
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&publisher),
				 CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
}

UT_TEST(test_sealed_pin_adopts_same_immutable_root_readback)
{
	ClusterWalRetentionInterval interval = { .thread_id = 1,
											 .tli = 1,
											 .start_lsn = TEST_WAL_SEG_SIZE,
											 .end_lsn = TEST_WAL_SEG_SIZE * 2 };
	ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);
	ClusterRecoverySerialGuard serial = make_serial_guard(&request);
	ClusterControlRootSnapshot expected = make_pin_root_snapshot(&request, &interval);
	ClusterControlRootSnapshot observed = expected;
	ClusterControlRootReadToken observed_token = request.root_read;
	ClusterWalRetentionPin *pin = NULL;
	ClusterWalRootPublishGuard *publisher = NULL;

	reset_pin_fakes();
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &serial), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_seal_for_root_publish(pin), CLUSTER_WAL_PIN_OK);
	serial.held = false;
	observed.root_publish_seq++;
	observed.recovered_through_lsn_exclusive = interval.end_lsn;
	observed_token.file_txn_seq++;
	observed_token.root_publish_seq++;
	observed_token.record_crc32c++;
	UT_ASSERT_EQ(cluster_wal_retention_pin_adopt_root_readback_v1(
					 pin, &expected, &request.root_read, &observed, &observed_token),
				 CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(
		cluster_wal_retention_root_publish_begin_exact(&request.root_read, true, &publisher),
		CLUSTER_WAL_PIN_STALE);
	UT_ASSERT_NULL(publisher);
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_begin_exact(&observed_token, true, &publisher),
				 CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&publisher),
				 CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
}

UT_TEST(test_sealed_pin_rejects_immutable_root_drift)
{
	ClusterWalRetentionInterval interval = { .thread_id = 1,
											 .tli = 1,
											 .start_lsn = TEST_WAL_SEG_SIZE,
											 .end_lsn = TEST_WAL_SEG_SIZE * 2 };
	ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);
	ClusterRecoverySerialGuard serial = make_serial_guard(&request);
	ClusterControlRootSnapshot expected = make_pin_root_snapshot(&request, &interval);
	ClusterControlRootSnapshot observed = expected;
	ClusterControlRootReadToken observed_token = request.root_read;
	ClusterWalRetentionPin *pin = NULL;
	ClusterWalRootPublishGuard *publisher = NULL;

	reset_pin_fakes();
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &serial), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_seal_for_root_publish(pin), CLUSTER_WAL_PIN_OK);
	serial.held = false;
	observed.checkpoint_lower_lsn++;
	observed_token.file_txn_seq++;
	observed_token.root_publish_seq++;
	observed_token.record_crc32c++;
	UT_ASSERT_EQ(cluster_wal_retention_pin_adopt_root_readback_v1(
					 pin, &expected, &request.root_read, &observed, &observed_token),
				 CLUSTER_WAL_PIN_STALE);
	UT_ASSERT_EQ(
		cluster_wal_retention_root_publish_begin_exact(&request.root_read, true, &publisher),
		CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&publisher),
				 CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
}

UT_TEST(test_pin_resource_owner_abort_releases_live_grant)
{
	ClusterWalRetentionInterval interval = { .thread_id = 1,
											 .tli = 1,
											 .start_lsn = TEST_WAL_SEG_SIZE,
											 .end_lsn = TEST_WAL_SEG_SIZE * 2 };
	ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);
	ClusterWalRetentionPin *pin = NULL;
	ClusterWalRetentionPin *next = NULL;

	reset_pin_fakes();
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_NOT_NULL(fake_resource_release_callback);
	if (fake_resource_release_callback != NULL)
		fake_resource_release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false,
									   fake_resource_release_arg);
	UT_ASSERT_EQ(fake_release_call_count, 1);
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &next), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&next), CLUSTER_WALR_RELEASE_CONFIRMED);
}

UT_TEST(test_pin_explicit_release_prevents_owner_double_release)
{
	ClusterWalRetentionInterval interval = { .thread_id = 1,
											 .tli = 1,
											 .start_lsn = TEST_WAL_SEG_SIZE,
											 .end_lsn = TEST_WAL_SEG_SIZE * 2 };
	ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);
	ClusterWalRetentionPin *pin = NULL;

	reset_pin_fakes();
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(fake_release_call_count, 1);
	UT_ASSERT_NOT_NULL(fake_resource_release_callback);
	if (fake_resource_release_callback != NULL)
		fake_resource_release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, true, false,
									   fake_resource_release_arg);
	UT_ASSERT_EQ(fake_release_call_count, 1);
}

UT_TEST(test_recovery_guard_converts_same_pin_holder_and_poison_is_cleanup_only)
{
	ClusterWalRetentionInterval interval = { .thread_id = 1,
											 .tli = 1,
											 .start_lsn = TEST_WAL_SEG_SIZE,
											 .end_lsn = TEST_WAL_SEG_SIZE * 2 };
	ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);
	ClusterRecoverySerialGuard serial = make_serial_guard(&request);
	ClusterWalRetentionPin *pin = NULL;
	ClusterWalReuseActionGuard guard;
	ClusterWalReuseDenyReason reason;
	ClusterWalTerminalOutcome outcome;

	/* Positive: one S holder converts in place to X and back to S. */
	reset_pin_fakes();
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &serial), CLUSTER_WAL_PIN_OK);
	prepare_bound_recovery_guard(&guard, &request);
	fake_s5_result = CLUSTER_LOCK_ACQUIRE_OK_CONVERTED;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_arm(&guard, &serial, pin, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(guard.state, CLUSTER_WAL_GUARD_ARMED);
	UT_ASSERT(guard.walr.held);
	UT_ASSERT(guard.walr.coordinated);
	UT_ASSERT(guard.walr.converted_from_pin);
	UT_ASSERT_EQ(guard.serial_or_null, &serial);
	UT_ASSERT_EQ(guard.pin_or_null, pin);
	UT_ASSERT_EQ(fake_acquire_call_count, 2);
	UT_ASSERT_EQ(fake_acquire_requests[1].op, CLUSTER_LOCK_OP_CONVERT);
	UT_ASSERT_EQ(fake_acquire_requests[1].current_mode, ShareLock);
	UT_ASSERT_EQ(fake_acquire_requests[1].lockmode, ExclusiveLock);
	UT_ASSERT(fake_acquire_requests[1].dontwait);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_finish(&guard, &outcome, &reason),
				 CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(outcome, CLUSTER_WAL_TERMINAL_UNCHANGED);
	UT_ASSERT_EQ(fake_acquire_call_count, 3);
	UT_ASSERT_EQ(fake_acquire_requests[2].op, CLUSTER_LOCK_OP_CONVERT);
	UT_ASSERT_EQ(fake_acquire_requests[2].current_mode, ExclusiveLock);
	UT_ASSERT_EQ(fake_acquire_requests[2].lockmode, ShareLock);
	UT_ASSERT(fake_acquire_requests[2].dontwait);
	UT_ASSERT_EQ(cluster_wal_retention_pin_revalidate(pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(fake_release_call_count, 1);
	UT_ASSERT_EQ(fake_native_release_call_count, 3);

	/* A conflicting conversion keeps the original pin S usable. */
	reset_pin_fakes();
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &serial), CLUSTER_WAL_PIN_OK);
	prepare_bound_recovery_guard(&guard, &request);
	fake_s5_result = CLUSTER_LOCK_ACQUIRE_NOT_AVAIL;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_arm(&guard, &serial, pin, &reason),
				 CLUSTER_WAL_GUARD_BLOCKED);
	UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_GES_UNAVAILABLE);
	UT_ASSERT_EQ(guard.state, CLUSTER_WAL_GUARD_FENCE_ADMITTED_OR_NA);
	UT_ASSERT_FALSE(guard.walr.held);
	UT_ASSERT_EQ(cluster_wal_retention_pin_revalidate(pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_finish(&guard, &outcome, &reason),
				 CLUSTER_WALR_RELEASE_NOT_HELD);
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);

	/* An uncertain X->S result poisons the pin; owner cleanup releases X once. */
	reset_pin_fakes();
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &serial), CLUSTER_WAL_PIN_OK);
	prepare_bound_recovery_guard(&guard, &request);
	fake_s5_result = CLUSTER_LOCK_ACQUIRE_OK_CONVERTED;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_arm(&guard, &serial, pin, &reason), CLUSTER_WAL_GUARD_OK);
	fake_s5_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_finish(&guard, &outcome, &reason),
				 CLUSTER_WALR_RELEASE_UNCONFIRMED);
	UT_ASSERT_EQ(outcome, CLUSTER_WAL_TERMINAL_UNCHANGED);
	UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_RELEASE_UNCERTAIN);
	UT_ASSERT(guard.walr.release_uncertain);
	UT_ASSERT_EQ(cluster_wal_retention_pin_revalidate(pin), CLUSTER_WAL_PIN_STALE);
	UT_ASSERT_NOT_NULL(fake_resource_release_callback);
	if (fake_resource_release_callback != NULL)
		fake_resource_release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false,
									   fake_resource_release_arg);
	UT_ASSERT_EQ(fake_release_call_count, 1);
	UT_ASSERT_EQ(fake_native_release_call_count, 3);

	/* An uncertain S->X reply preserves R_new in a cleanup-only guard and
	 * poisons both the guard/pin relationship until ResourceOwner cleanup. */
	reset_pin_fakes();
	pin = NULL;
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &serial), CLUSTER_WAL_PIN_OK);
	prepare_bound_recovery_guard(&guard, &request);
	fake_s5_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_arm(&guard, &serial, pin, &reason),
				 CLUSTER_WAL_GUARD_RELEASE_UNCERTAIN);
	UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_RELEASE_UNCERTAIN);
	UT_ASSERT_EQ(guard.state, CLUSTER_WAL_GUARD_WALR_X_HELD);
	UT_ASSERT(guard.walr.held);
	UT_ASSERT(guard.walr.converted_from_pin);
	UT_ASSERT(guard.walr.release_uncertain);
	UT_ASSERT_EQ(guard.serial_or_null, &serial);
	UT_ASSERT_EQ(guard.pin_or_null, pin);
	UT_ASSERT_EQ(cluster_wal_retention_pin_revalidate(pin), CLUSTER_WAL_PIN_STALE);
	UT_ASSERT_NOT_NULL(fake_resource_release_callback);
	if (fake_resource_release_callback != NULL)
		fake_resource_release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false,
									   fake_resource_release_arg);
	UT_ASSERT_EQ(fake_release_call_count, 1);
	UT_ASSERT_EQ(fake_native_release_call_count, 2);
}

UT_TEST(test_recovery_guard_l3_revalidates_bound_pin_and_serial)
{
	ClusterWalRetentionInterval interval = { .thread_id = 1,
											 .tli = 1,
											 .start_lsn = TEST_WAL_SEG_SIZE,
											 .end_lsn = TEST_WAL_SEG_SIZE * 2 };
	ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);
	ClusterRecoverySerialGuard serial = make_serial_guard(&request);
	ClusterWalRetentionPin *pin = NULL;
	ClusterWalReuseActionGuard guard;
	ClusterWalReuseDenyReason reason;

	reset_pin_fakes();
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &serial), CLUSTER_WAL_PIN_OK);
	prepare_bound_recovery_guard(&guard, &request);
	fake_s5_result = CLUSTER_LOCK_ACQUIRE_OK_CONVERTED;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_arm(&guard, &serial, pin, &reason), CLUSTER_WAL_GUARD_OK);

	/* ARMED is not durable authority: serial/pin drift must be observed before
	 * the final object stamp and any destructive syscall. */
	fake_serial_result = CLUSTER_RECOVERY_SERIAL_FENCE_STALE;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_l3_begin(&guard, CLUSTER_WAL_PHYSICAL_REMOVE, &reason),
				 CLUSTER_WAL_GUARD_BLOCKED);
	UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_SERIAL_STALE);
	UT_ASSERT_EQ(cluster_wal_retention_pin_revalidate(pin), CLUSTER_WAL_PIN_STALE);
	UT_ASSERT_NOT_NULL(fake_resource_release_callback);
	if (fake_resource_release_callback != NULL)
		fake_resource_release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false,
									   fake_resource_release_arg);
}

UT_TEST(test_active_recovery_preflight_borrows_exact_pin_and_blocks_retained_file)
{
	char root_template[] = "/tmp/pgrac-wal-active-XXXXXX";
	char *root_dir;
	char thread_dir[MAXPGPATH];
	char retained_path[MAXPGPATH];
	char disjoint_path[MAXPGPATH];
	ClusterWalRetentionInterval interval = { .thread_id = 1,
											 .tli = 1,
											 .start_lsn = TEST_WAL_SEG_SIZE,
											 .end_lsn = TEST_WAL_SEG_SIZE * 2 };
	ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);
	ClusterRecoverySerialGuard serial = make_serial_guard(&request);
	ClusterWalRetentionPin *pin = NULL;
	ClusterWalRetentionPin *borrowed_pin = NULL;
	ClusterRecoverySerialGuard *borrowed_serial = NULL;
	ClusterWalReuseActionGuard guard = { 0 };
	ClusterWalFileIdentity file
		= { .thread_id = 1, .kind = CLUSTER_WAL_FILE_NORMAL, .tli = 1, .segno = 3 };
	ClusterWalReuseDenyReason reason;
	ClusterWalTerminalOutcome outcome;

	reset_pin_fakes();
	UT_ASSERT_FALSE(cluster_wal_retention_active_pin_present());
	root_dir = mkdtemp(root_template);
	UT_ASSERT_NOT_NULL(root_dir);
	snprintf(thread_dir, sizeof(thread_dir), "%s/thread_1", root_dir);
	UT_ASSERT_EQ(mkdir(thread_dir, 0700), 0);
	write_test_wal_segment(thread_dir, 1, 1, retained_path, sizeof(retained_path));
	write_test_wal_segment(thread_dir, 1, 3, disjoint_path, sizeof(disjoint_path));
	cluster_wal_threads_dir = root_dir;
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_TRUE(cluster_wal_retention_active_pin_present());
	UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &serial), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_init(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_preflight_active_recovery(
					 &guard, &file, CLUSTER_WAL_REUSE_E2_APPLY_TIMELINE_SWITCH, &borrowed_serial,
					 &borrowed_pin, &reason),
				 CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(borrowed_serial, &serial);
	UT_ASSERT_EQ(borrowed_pin, pin);
	UT_ASSERT_EQ(guard.entry, CLUSTER_WAL_REUSE_E2_APPLY_TIMELINE_SWITCH);
	UT_ASSERT_EQ(guard.state, CLUSTER_WAL_GUARD_FENCE_ADMITTED_OR_NA);
	UT_ASSERT_EQ(guard.needs_or_null, request.needs);
	UT_ASSERT_EQ(guard.admissions_or_null, request.admissions);
	fake_s5_result = CLUSTER_LOCK_ACQUIRE_OK_CONVERTED;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_arm(&guard, borrowed_serial, borrowed_pin, &reason),
				 CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_finish(&guard, &outcome, &reason),
				 CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_FALSE(cluster_wal_retention_active_pin_present());

	reset_pin_fakes();
	request = make_pin_request(1, &interval, 1);
	serial = make_serial_guard(&request);
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &serial), CLUSTER_WAL_PIN_OK);
	memset(&guard, 0, sizeof(guard));
	file.segno = 1;
	borrowed_serial = NULL;
	borrowed_pin = NULL;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_init(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_preflight_active_recovery(
					 &guard, &file, CLUSTER_WAL_REUSE_E2_APPLY_TIMELINE_SWITCH, &borrowed_serial,
					 &borrowed_pin, &reason),
				 CLUSTER_WAL_GUARD_BLOCKED);
	UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_ROOT_REQUIRED);
	UT_ASSERT_NULL(borrowed_serial);
	UT_ASSERT_NULL(borrowed_pin);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_finish(&guard, &outcome, &reason),
				 CLUSTER_WALR_RELEASE_NOT_HELD);
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);

	cluster_wal_threads_dir = NULL;
	UT_ASSERT_EQ(unlink(retained_path), 0);
	UT_ASSERT_EQ(unlink(disjoint_path), 0);
	UT_ASSERT_EQ(rmdir(thread_dir), 0);
	UT_ASSERT_EQ(rmdir(root_dir), 0);
}

UT_TEST(test_correctness_action_context_preflights_e2_without_pin)
{
	char root_template[] = "/tmp/pgrac-wal-action-XXXXXX";
	char *root_dir;
	char thread_dir[MAXPGPATH];
	char wal_path[MAXPGPATH];
	char retained_path[MAXPGPATH];
	ClusterWalRetentionE1Context context = { 0 };
	ClusterWalReuseActionGuard guard = { 0 };
	ClusterWalFileIdentity file
		= { .thread_id = 1, .kind = CLUSTER_WAL_FILE_NORMAL, .tli = 1, .segno = 1 };
	ClusterWalReuseDenyReason reason;
	ClusterWalTerminalOutcome outcome;
	PgracExternalFenceNeedSetV1 *needs = NULL;
	const PgracExternalFenceAdmissionSetV1 *admissions
		= (const PgracExternalFenceAdmissionSetV1 *)(uintptr_t)0x3000;

	reset_pin_fakes();
	root_dir = mkdtemp(root_template);
	UT_ASSERT_NOT_NULL(root_dir);
	snprintf(thread_dir, sizeof(thread_dir), "%s/thread_1", root_dir);
	UT_ASSERT_EQ(mkdir(thread_dir, 0700), 0);
	write_test_wal_segment(thread_dir, 1, 1, wal_path, sizeof(wal_path));
	cluster_wal_threads_dir = root_dir;
	fake_preflight_root = make_root(1, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED, 1,
									TEST_WAL_SEG_SIZE * 4, TEST_WAL_SEG_SIZE * 5);
	fake_preflight_root.identity = make_duty(1);
	fake_preflight_token = make_root_token(&fake_preflight_root.identity);
	fake_preflight_root.root_flags = fake_preflight_token.root_flags;
	fake_preflight_root_ready = true;

	UT_ASSERT_EQ(cluster_wal_retention_action_begin(&context, 1, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(fake_acquire_call_count, 0);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_init(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_retention_action_preflight(&context, &file,
														CLUSTER_WAL_REUSE_E2_APPLY_TIMELINE_SWITCH,
														&guard, &needs, &reason),
				 CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_NOT_NULL(needs);
	UT_ASSERT_EQ(guard.entry, CLUSTER_WAL_REUSE_E2_APPLY_TIMELINE_SWITCH);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_fence_admitted_nowait(&guard, admissions, &reason),
				 CLUSTER_WAL_GUARD_OK);
	fake_acquire_result_count = 1;
	fake_acquire_results[0] = CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_arm(&guard, NULL, NULL, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_l3_begin(&guard, CLUSTER_WAL_PHYSICAL_REMOVE, &reason),
				 CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_remove(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_bookkeep(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_finish(&guard, &outcome, &reason),
				 CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(outcome, CLUSTER_WAL_TERMINAL_REMOVED);
	UT_ASSERT_EQ(access(wal_path, F_OK), -1);
	cluster_external_fence_need_set_release(&needs);

	write_test_wal_segment(thread_dir, 1, 4, retained_path, sizeof(retained_path));
	file.segno = 4;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_init(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_retention_action_preflight(&context, &file,
														CLUSTER_WAL_REUSE_E2_APPLY_TIMELINE_SWITCH,
														&guard, &needs, &reason),
				 CLUSTER_WAL_GUARD_BLOCKED);
	UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_ROOT_REQUIRED);
	UT_ASSERT_NULL(needs);
	UT_ASSERT_EQ(access(retained_path, F_OK), 0);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_finish(&guard, &outcome, &reason),
				 CLUSTER_WALR_RELEASE_NOT_HELD);
	cluster_wal_retention_action_finish(&context);
	UT_ASSERT_EQ(context.magic, 0);

	cluster_wal_threads_dir = NULL;
	UT_ASSERT_EQ(unlink(retained_path), 0);
	UT_ASSERT_EQ(rmdir(thread_dir), 0);
	UT_ASSERT_EQ(rmdir(root_dir), 0);
}

UT_TEST(test_reuse_guard_init_and_empty_finish_exact)
{
	ClusterWalReuseActionGuard guard = { 0 };
	ClusterWalReuseDenyReason reason = CLUSTER_WAL_DENY_GUARD_STATE;
	ClusterWalTerminalOutcome outcome = CLUSTER_WAL_TERMINAL_CREATED;

	UT_ASSERT_EQ(cluster_wal_reuse_guard_init(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_NONE);
	UT_ASSERT_EQ(guard.magic, CLUSTER_WAL_REUSE_GUARD_MAGIC);
	UT_ASSERT_EQ(guard.version, CLUSTER_WAL_REUSE_GUARD_VERSION);
	UT_ASSERT_EQ(guard.state, CLUSTER_WAL_GUARD_EMPTY);
	UT_ASSERT_EQ(guard.flags, 0);
	UT_ASSERT_EQ(guard.owner_pid, MyProcPid);
	UT_ASSERT_EQ(guard.self_address, (uintptr_t)&guard);
	UT_ASSERT_EQ(guard.owner, CurrentResourceOwner);
	UT_ASSERT_EQ(guard.source_dir_handle, (intptr_t)-1);
	UT_ASSERT_EQ(guard.source_handle, (intptr_t)-1);
	UT_ASSERT_EQ(guard.fork_source_handle, (intptr_t)-1);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_finish(&guard, &outcome, &reason),
				 CLUSTER_WALR_RELEASE_NOT_HELD);
	UT_ASSERT_EQ(outcome, CLUSTER_WAL_TERMINAL_UNCHANGED);
	UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_NONE);
	UT_ASSERT_EQ(guard.magic, 0);
}

UT_TEST(test_e1_coarse_holds_x_across_strong_floor_then_releases)
{
	ClusterWalRetentionE1Context context = { 0 };
	ClusterWalRootFoldResult fold_result = CLUSTER_WAL_FOLD_UNKNOWN;
	XLogSegNo floor = 0;
	ClusterWalReuseDenyReason reason;

	reset_pin_fakes();
	fake_preflight_root = make_root(1, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN, 1,
									TEST_WAL_SEG_SIZE * 4, TEST_WAL_SEG_SIZE * 5);
	fake_preflight_root.identity = make_duty(1);
	fake_preflight_token = make_root_token(&fake_preflight_root.identity);
	fake_preflight_token.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	fake_preflight_token.root_flags &= ~CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID;
	fake_preflight_root.root_flags = fake_preflight_token.root_flags;
	fake_preflight_root_ready = true;

	UT_ASSERT_EQ(cluster_wal_retention_e1_coarse_begin(&context, 1, &fold_result, &floor, &reason),
				 CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(fold_result, CLUSTER_WAL_FOLD_BOUNDED);
	UT_ASSERT_EQ(floor, 4);
	UT_ASSERT(context.coarse_walr.held);
	UT_ASSERT_EQ(fake_acquire_call_count, 1);
	UT_ASSERT_EQ(fake_acquire_requests[0].lockmode, ExclusiveLock);
	UT_ASSERT(fake_acquire_requests[0].dontwait);
	UT_ASSERT_EQ(cluster_wal_retention_e1_coarse_release(&context, &reason),
				 CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_FALSE(context.coarse_walr.held);
	UT_ASSERT_EQ(fake_release_call_count, 1);
	cluster_wal_retention_e1_finish(&context);
	UT_ASSERT_EQ(context.magic, 0);
}

UT_TEST(test_e1_coarse_unknown_configured_root_releases_and_retains)
{
	ClusterWalRetentionE1Context context = { 0 };
	ClusterWalRootFoldResult fold_result = CLUSTER_WAL_FOLD_UNCONSTRAINED;
	XLogSegNo floor = 99;
	ClusterWalReuseDenyReason reason;

	reset_pin_fakes();
	fake_preflight_root = make_root(1, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN, 1,
									TEST_WAL_SEG_SIZE * 4, TEST_WAL_SEG_SIZE * 5);
	fake_preflight_root.identity = make_duty(1);
	fake_preflight_token = make_root_token(&fake_preflight_root.identity);
	fake_preflight_root.root_flags = fake_preflight_token.root_flags;
	fake_preflight_root_ready = true;
	fake_extra_configured_thread = true;

	UT_ASSERT_EQ(cluster_wal_retention_e1_coarse_begin(&context, 1, &fold_result, &floor, &reason),
				 CLUSTER_WAL_GUARD_BLOCKED);
	UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_ROOT_REQUIRED);
	UT_ASSERT_EQ(fold_result, CLUSTER_WAL_FOLD_UNKNOWN);
	UT_ASSERT_EQ(floor, 0);
	UT_ASSERT_EQ(fake_release_call_count, 1);
	UT_ASSERT_EQ(context.magic, 0);
}

UT_TEST(test_reuse_guard_rejects_dirty_init_and_stack_copy)
{
	ClusterWalReuseActionGuard guard = { 0 };
	ClusterWalReuseActionGuard copy;
	ClusterWalReuseDenyReason reason = CLUSTER_WAL_DENY_NONE;
	ClusterWalTerminalOutcome outcome = CLUSTER_WAL_TERMINAL_CREATED;

	guard.flags = 1;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_init(&guard, &reason), CLUSTER_WAL_GUARD_INVALID);
	UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_GUARD_STATE);
	memset(&guard, 0, sizeof(guard));
	UT_ASSERT_EQ(cluster_wal_reuse_guard_init(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	copy = guard;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_finish(&copy, &outcome, &reason),
				 CLUSTER_WALR_RELEASE_INVALID);
	UT_ASSERT_EQ(outcome, CLUSTER_WAL_TERMINAL_UNCHANGED);
	UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_GUARD_STATE);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_finish(&guard, &outcome, &reason),
				 CLUSTER_WALR_RELEASE_NOT_HELD);
}

UT_TEST(test_reuse_guard_zero_attempts_finish_unchanged)
{
	ClusterWalReuseActionGuard guard = { 0 };
	ClusterWalReuseDenyReason reason;
	ClusterWalTerminalOutcome outcome;

	UT_ASSERT_EQ(cluster_wal_reuse_guard_init(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	guard.entry = CLUSTER_WAL_REUSE_E1_CHECKPOINT_RESTARTPOINT;
	guard.action = CLUSTER_WAL_ACTION_RETIRE_RECYCLE_OR_REMOVE;
	guard.state = CLUSTER_WAL_GUARD_ARMED;
	guard.flags = CLUSTER_WAL_GUARD_F_PRIMARY_L3 | CLUSTER_WAL_GUARD_F_PRIMARY_RECYCLE;
	UT_ASSERT_EQ(
		cluster_wal_reuse_guard_note_zero_mutation(&guard, CLUSTER_WAL_PHYSICAL_RECYCLE, &reason),
		CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(
		cluster_wal_reuse_guard_terminal_durable(&guard, CLUSTER_WAL_TERMINAL_RECYCLED, &reason),
		CLUSTER_WAL_GUARD_INVALID);
	UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_GUARD_STATE);
	guard.flags |= CLUSTER_WAL_GUARD_F_FALLBACK_L3;
	UT_ASSERT_EQ(
		cluster_wal_reuse_guard_note_zero_mutation(&guard, CLUSTER_WAL_PHYSICAL_REMOVE, &reason),
		CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_finish(&guard, &outcome, &reason),
				 CLUSTER_WALR_RELEASE_NOT_HELD);
	UT_ASSERT_EQ(outcome, CLUSTER_WAL_TERMINAL_UNCHANGED);
	UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_NONE);
}

UT_TEST(test_reuse_guard_terminal_mapping_bookkeep_and_finish)
{
	ClusterWalReuseActionGuard guard = { 0 };
	ClusterWalReuseDenyReason reason;
	ClusterWalTerminalOutcome outcome;

	UT_ASSERT_EQ(cluster_wal_reuse_guard_init(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	guard.entry = CLUSTER_WAL_REUSE_E4_PARTIAL_RENAME;
	guard.action = CLUSTER_WAL_ACTION_RENAME_PARTIAL;
	guard.state = CLUSTER_WAL_GUARD_ARMED;
	guard.flags = CLUSTER_WAL_GUARD_F_PRIMARY_L3;
	UT_ASSERT_EQ(
		cluster_wal_reuse_guard_terminal_durable(&guard, CLUSTER_WAL_TERMINAL_REMOVED, &reason),
		CLUSTER_WAL_GUARD_INVALID);
	UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_GUARD_STATE);
	UT_ASSERT_EQ(guard.state, CLUSTER_WAL_GUARD_ARMED);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_terminal_durable(
					 &guard, CLUSTER_WAL_TERMINAL_RENAMED_PARTIAL, &reason),
				 CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(guard.state, CLUSTER_WAL_GUARD_TERMINAL_DURABLE);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_bookkeep(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(guard.state, CLUSTER_WAL_GUARD_BOOKKEPT);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_bookkeep(&guard, &reason), CLUSTER_WAL_GUARD_INVALID);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_finish(&guard, &outcome, &reason),
				 CLUSTER_WALR_RELEASE_NOT_HELD);
	UT_ASSERT_EQ(outcome, CLUSTER_WAL_TERMINAL_RENAMED_PARTIAL);
	UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_NONE);
}

UT_TEST(test_reuse_guard_preflight_rejects_closed_matrix_before_authority)
{
	ClusterWalReuseActionGuard guard = { 0 };
	ClusterWalReuseGuardRequest request;
	ClusterWalReuseDenyReason reason;
	PgracExternalFenceNeedSetV1 *needs = NULL;

	memset(&request, 0, sizeof(request));
	request.file.thread_id = 1;
	request.file.kind = CLUSTER_WAL_FILE_NORMAL;
	request.file.tli = 1;
	request.file.segno = 1;
	request.duty = make_duty(1);
	request.root_read = make_root_token(&request.duty);
	request.formation = (const ClusterFormationWitnessV1 *)(uintptr_t)0x1000;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_init(&guard, &reason), CLUSTER_WAL_GUARD_OK);

	request.entry = CLUSTER_WAL_REUSE_E5_RESTORE_STAGING;
	request.action = CLUSTER_WAL_ACTION_RETIRE_RECYCLE_OR_REMOVE;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_preflight(&guard, &request, &needs, &reason),
				 CLUSTER_WAL_GUARD_INVALID);
	UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_INVALID_IDENTITY);
	UT_ASSERT_NULL(needs);
	UT_ASSERT_EQ(guard.state, CLUSTER_WAL_GUARD_EMPTY);

	request.entry = CLUSTER_WAL_REUSE_E7_EXTERNAL_CLEANUP;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_preflight(&guard, &request, &needs, &reason),
				 CLUSTER_WAL_GUARD_INVALID);
	UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_INVALID_IDENTITY);

	request.entry = CLUSTER_WAL_REUSE_E4_PARTIAL_RENAME;
	request.action = CLUSTER_WAL_ACTION_RENAME_PARTIAL;
	request.source_kind = CLUSTER_WAL_INSTALL_SOURCE_ARCHIVE_STAGE;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_preflight(&guard, &request, &needs, &reason),
				 CLUSTER_WAL_GUARD_INVALID);
	UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_INVALID_IDENTITY);

	request.entry = CLUSTER_WAL_REUSE_E3_ARCHIVE_END;
	request.action = CLUSTER_WAL_ACTION_CREATE_ABSENT;
	request.source_kind = CLUSTER_WAL_INSTALL_SOURCE_NONE;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_preflight(&guard, &request, &needs, &reason),
				 CLUSTER_WAL_GUARD_INVALID);
	UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_INVALID_IDENTITY);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_finish(
					 &guard, &(ClusterWalTerminalOutcome){ CLUSTER_WAL_TERMINAL_CREATED }, &reason),
				 CLUSTER_WALR_RELEASE_NOT_HELD);
}

UT_TEST(test_reuse_guard_fence_admission_is_na_or_full_set_and)
{
	ClusterWalReuseActionGuard guard = { 0 };
	ClusterWalReuseDenyReason reason;
	ClusterWalTerminalOutcome outcome;
	const ClusterFormationWitnessV1 *formation
		= (const ClusterFormationWitnessV1 *)(uintptr_t)0x1000;
	const PgracExternalFenceNeedSetV1 *needs
		= (const PgracExternalFenceNeedSetV1 *)(uintptr_t)0x2000;
	const PgracExternalFenceAdmissionSetV1 *admissions
		= (const PgracExternalFenceAdmissionSetV1 *)(uintptr_t)0x3000;

	reset_pin_fakes();
	UT_ASSERT_EQ(cluster_wal_reuse_guard_init(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	guard.state = CLUSTER_WAL_GUARD_PREFLIGHTED;
	guard.formation = formation;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_fence_admitted_nowait(&guard, NULL, &reason),
				 CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(guard.state, CLUSTER_WAL_GUARD_FENCE_ADMITTED_OR_NA);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_finish(&guard, &outcome, &reason),
				 CLUSTER_WALR_RELEASE_NOT_HELD);

	UT_ASSERT_EQ(cluster_wal_reuse_guard_init(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	guard.state = CLUSTER_WAL_GUARD_PREFLIGHTED;
	guard.formation = formation;
	guard.needs_or_null = needs;
	fake_need_count = 2;
	fake_admission_count = 1;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_fence_admitted_nowait(&guard, admissions, &reason),
				 CLUSTER_WAL_GUARD_BLOCKED);
	UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_FENCE_UNAVAILABLE);
	UT_ASSERT_EQ(guard.state, CLUSTER_WAL_GUARD_PREFLIGHTED);
	fake_admission_count = 2;
	fake_admission_digest.bytes[0] = 1;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_fence_admitted_nowait(&guard, admissions, &reason),
				 CLUSTER_WAL_GUARD_BLOCKED);
	fake_need_digest.bytes[0] = 1;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_fence_admitted_nowait(&guard, admissions, &reason),
				 CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(guard.admissions_or_null, admissions);
	UT_ASSERT_EQ(guard.state, CLUSTER_WAL_GUARD_FENCE_ADMITTED_OR_NA);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_finish(&guard, &outcome, &reason),
				 CLUSTER_WALR_RELEASE_NOT_HELD);
}

UT_TEST(test_reuse_guard_preflight_stamps_folds_and_builds_needset)
{
	char root_template[] = "/tmp/pgrac-wal-retention-XXXXXX";
	char *root_dir;
	char thread_dir[MAXPGPATH];
	char held_thread_dir[MAXPGPATH];
	char held_wal_path[MAXPGPATH];
	char wal_name[MAXFNAMELEN];
	char wal_path[MAXPGPATH];
	char replacement_wal_path[MAXPGPATH];
	char recycle_name[MAXFNAMELEN];
	char recycle_path[MAXPGPATH];
	char held_recycle_path[MAXPGPATH];
	XLogLongPageHeaderData header;
	ClusterWalReuseActionGuard guard = { 0 };
	ClusterWalReuseGuardRequest request;
	ClusterWalFileIdentity destination;
	ClusterWalReuseDenyReason reason;
	ClusterWalTerminalOutcome outcome;
	PgracExternalFenceNeedSetV1 *needs = NULL;
	const PgracExternalFenceAdmissionSetV1 *admissions
		= (const PgracExternalFenceAdmissionSetV1 *)(uintptr_t)0x5000;
	int fd;

	reset_pin_fakes();
	root_dir = mkdtemp(root_template);
	UT_ASSERT_NOT_NULL(root_dir);
	snprintf(thread_dir, sizeof(thread_dir), "%s/thread_1", root_dir);
	UT_ASSERT_EQ(mkdir(thread_dir, 0700), 0);
	XLogFileName(wal_name, 1, 1, TEST_WAL_SEG_SIZE);
	snprintf(wal_path, sizeof(wal_path), "%s/%s", thread_dir, wal_name);
	fd = open(wal_path, O_RDWR | O_CREAT | O_EXCL, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(ftruncate(fd, TEST_WAL_SEG_SIZE), 0);
	memset(&header, 0, sizeof(header));
	header.std.xlp_magic = XLOG_PAGE_MAGIC;
	header.std.xlp_info = XLP_LONG_HEADER;
	header.std.xlp_tli = 1;
	header.std.xlp_pageaddr = TEST_WAL_SEG_SIZE;
	header.std.xlp_thread_id = 1;
	header.xlp_sysid = UINT64_C(0x1122334455667788);
	header.xlp_seg_size = TEST_WAL_SEG_SIZE;
	header.xlp_xlog_blcksz = XLOG_BLCKSZ;
	UT_ASSERT_EQ(pwrite(fd, &header, sizeof(header), 0), sizeof(header));
	close(fd);
	cluster_wal_threads_dir = root_dir;

	memset(&request, 0, sizeof(request));
	request.file.thread_id = 1;
	request.file.kind = CLUSTER_WAL_FILE_NORMAL;
	request.file.tli = 1;
	request.file.segno = 1;
	request.entry = CLUSTER_WAL_REUSE_E1_CHECKPOINT_RESTARTPOINT;
	request.action = CLUSTER_WAL_ACTION_RETIRE_RECYCLE_OR_REMOVE;
	request.source_kind = CLUSTER_WAL_INSTALL_SOURCE_NONE;
	request.duty = make_duty(1);
	request.root_read = make_root_token(&request.duty);
	request.formation = (const ClusterFormationWitnessV1 *)(uintptr_t)0x1000;
	fake_preflight_root.identity = request.duty;
	fake_preflight_root.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	fake_preflight_root.root_flags = request.root_read.root_flags;
	fake_preflight_root.root_publish_seq = 1;
	fake_preflight_root.checkpoint_tli = 1;
	fake_preflight_root.tail_tli = 1;
	fake_preflight_root.checkpoint_lower_lsn = TEST_WAL_SEG_SIZE * 4;
	fake_preflight_root.validated_tail_lsn_exclusive = TEST_WAL_SEG_SIZE * 5;
	fake_preflight_token = request.root_read;
	fake_preflight_root_ready = true;

	UT_ASSERT_EQ(cluster_wal_reuse_guard_init(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_preflight(&guard, &request, &needs, &reason),
				 CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_NONE);
	UT_ASSERT_NOT_NULL(needs);
	UT_ASSERT_EQ(guard.state, CLUSTER_WAL_GUARD_PREFLIGHTED);
	UT_ASSERT_EQ(guard.pre_action_stamp.platform, CLUSTER_WAL_OBJECT_POSIX);
	UT_ASSERT_EQ(guard.pre_action_stamp.parsed_identity.segno, 1);
	UT_ASSERT_EQ(guard.needs_or_null, needs);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_fence_admitted_nowait(&guard, admissions, &reason),
				 CLUSTER_WAL_GUARD_OK);
	fake_acquire_result_count = 1;
	fake_acquire_results[0] = CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_arm(&guard, NULL, NULL, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(guard.state, CLUSTER_WAL_GUARD_ARMED);
	UT_ASSERT(guard.walr.held);
	UT_ASSERT(guard.walr.coordinated);
	UT_ASSERT_EQ(guard.walr.mode, ExclusiveLock);
	UT_ASSERT_EQ(fake_acquire_requests[0].resid.field1, 1);
	UT_ASSERT_EQ(fake_acquire_requests[0].lockmode, ExclusiveLock);
	UT_ASSERT(fake_acquire_requests[0].dontwait);
	UT_ASSERT_EQ(fake_native_acquire_call_count, 1);
	UT_ASSERT_EQ(fake_s5_call_count, 1);
	UT_ASSERT_EQ(fake_native_locktag.locktag_type, LOCKTAG_USERLOCK);
	UT_ASSERT_NOT_NULL(fake_resource_release_callback);
	/* The preflight directory/leaf handles, not a reconstructed path, remain
	 * the terminal identity authority if the configured pathname is swapped. */
	snprintf(held_thread_dir, sizeof(held_thread_dir), "%s.held", thread_dir);
	UT_ASSERT_EQ(rename(thread_dir, held_thread_dir), 0);
	snprintf(held_wal_path, sizeof(held_wal_path), "%s/%s", held_thread_dir, wal_name);
	UT_ASSERT_EQ(mkdir(thread_dir, 0700), 0);
	snprintf(replacement_wal_path, sizeof(replacement_wal_path), "%s/%s", thread_dir, wal_name);
	fd = open(replacement_wal_path, O_RDWR | O_CREAT | O_EXCL, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(ftruncate(fd, TEST_WAL_SEG_SIZE), 0);
	UT_ASSERT_EQ(pwrite(fd, &header, sizeof(header), 0), sizeof(header));
	close(fd);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_l3_begin(&guard, CLUSTER_WAL_PHYSICAL_REMOVE, &reason),
				 CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_remove(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(access(held_wal_path, F_OK), -1);
	UT_ASSERT_EQ(errno, ENOENT);
	UT_ASSERT_EQ(access(replacement_wal_path, F_OK), 0);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_bookkeep(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_finish(&guard, &outcome, &reason),
				 CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(outcome, CLUSTER_WAL_TERMINAL_REMOVED);
	UT_ASSERT_EQ(fake_release_call_count, 1);
	UT_ASSERT_EQ(fake_native_release_call_count, 1);
	UT_ASSERT_EQ(unlink(replacement_wal_path), 0);
	UT_ASSERT_EQ(rmdir(thread_dir), 0);
	UT_ASSERT_EQ(rename(held_thread_dir, thread_dir), 0);
	fd = open(wal_path, O_RDWR | O_CREAT | O_EXCL, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(ftruncate(fd, TEST_WAL_SEG_SIZE), 0);
	UT_ASSERT_EQ(pwrite(fd, &header, sizeof(header), 0), sizeof(header));
	close(fd);
	cluster_external_fence_need_set_release(&needs);
	if (fake_resource_release_callback != NULL)
		fake_resource_release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, true, false,
									   fake_resource_release_arg);
	UT_ASSERT_EQ(fake_release_call_count, 1);

	/* Recycle must rename within the preflight dirfd and must not follow a
	 * replacement configured pathname to either source or destination. */
	memset(&guard, 0, sizeof(guard));
	fake_acquire_result_count = 2;
	fake_acquire_results[1] = CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_init(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_preflight(&guard, &request, &needs, &reason),
				 CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_fence_admitted_nowait(&guard, admissions, &reason),
				 CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_arm(&guard, NULL, NULL, &reason), CLUSTER_WAL_GUARD_OK);
	destination = request.file;
	destination.tli = 2;
	destination.segno = 2;
	XLogFileName(recycle_name, destination.tli, destination.segno, wal_segment_size);
	snprintf(recycle_path, sizeof(recycle_path), "%s/%s", thread_dir, recycle_name);
	UT_ASSERT_EQ(rename(thread_dir, held_thread_dir), 0);
	snprintf(held_wal_path, sizeof(held_wal_path), "%s/%s", held_thread_dir, wal_name);
	snprintf(held_recycle_path, sizeof(held_recycle_path), "%s/%s", held_thread_dir, recycle_name);
	UT_ASSERT_EQ(mkdir(thread_dir, 0700), 0);
	fd = open(replacement_wal_path, O_RDWR | O_CREAT | O_EXCL, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(ftruncate(fd, TEST_WAL_SEG_SIZE), 0);
	UT_ASSERT_EQ(pwrite(fd, &header, sizeof(header), 0), sizeof(header));
	close(fd);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_l3_begin(&guard, CLUSTER_WAL_PHYSICAL_RECYCLE, &reason),
				 CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_recycle(&guard, &destination, &reason),
				 CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(access(held_wal_path, F_OK), -1);
	UT_ASSERT_EQ(errno, ENOENT);
	UT_ASSERT_EQ(access(held_recycle_path, F_OK), 0);
	UT_ASSERT_EQ(access(replacement_wal_path, F_OK), 0);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_bookkeep(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_finish(&guard, &outcome, &reason),
				 CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(outcome, CLUSTER_WAL_TERMINAL_RECYCLED);
	cluster_external_fence_need_set_release(&needs);
	UT_ASSERT_EQ(unlink(replacement_wal_path), 0);
	UT_ASSERT_EQ(rmdir(thread_dir), 0);
	UT_ASSERT_EQ(rename(held_thread_dir, thread_dir), 0);
	UT_ASSERT_EQ(unlink(recycle_path), 0);
	fd = open(wal_path, O_RDWR | O_CREAT | O_EXCL, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(ftruncate(fd, TEST_WAL_SEG_SIZE), 0);
	UT_ASSERT_EQ(pwrite(fd, &header, sizeof(header), 0), sizeof(header));
	close(fd);
	UT_ASSERT_EQ(fake_release_call_count, 2);
	UT_ASSERT_EQ(fake_native_release_call_count, 2);

	/* A live first-user guard is ResourceOwner-cleaned before its stack dies. */
	memset(&guard, 0, sizeof(guard));
	fake_acquire_result_count = 3;
	fake_acquire_results[2] = CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_init(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_preflight(&guard, &request, &needs, &reason),
				 CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_fence_admitted_nowait(&guard, admissions, &reason),
				 CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_arm(&guard, NULL, NULL, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(fake_native_acquire_call_count, 3);
	if (fake_resource_release_callback != NULL)
		fake_resource_release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false,
									   fake_resource_release_arg);
	UT_ASSERT_EQ(fake_release_call_count, 3);
	UT_ASSERT_EQ(fake_native_release_call_count, 3);
	UT_ASSERT_FALSE(guard.walr.held);
	cluster_external_fence_need_set_release(&needs);

	/* A missing PG-native leg must never be relabeled coordinated. */
	memset(&guard, 0, sizeof(guard));
	fake_acquire_result_count = 4;
	fake_acquire_results[3] = CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK;
	fake_native_acquire_result = LOCKACQUIRE_NOT_AVAIL;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_init(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_preflight(&guard, &request, &needs, &reason),
				 CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_fence_admitted_nowait(&guard, admissions, &reason),
				 CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_arm(&guard, NULL, NULL, &reason),
				 CLUSTER_WAL_GUARD_BLOCKED);
	UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_GES_UNAVAILABLE);
	UT_ASSERT_EQ(guard.state, CLUSTER_WAL_GUARD_FENCE_ADMITTED_OR_NA);
	UT_ASSERT_FALSE(guard.walr.held);
	UT_ASSERT_FALSE(guard.walr.coordinated);
	UT_ASSERT_EQ(fake_native_acquire_call_count, 4);
	UT_ASSERT_EQ(fake_s5_call_count, 3);
	UT_ASSERT_EQ(fake_s7_call_count, 1);
	cluster_external_fence_need_set_release(&needs);
	fake_native_acquire_result = LOCKACQUIRE_OK;

	memset(&guard, 0, sizeof(guard));
	fake_preflight_root.checkpoint_lower_lsn = TEST_WAL_SEG_SIZE;
	fake_preflight_root.validated_tail_lsn_exclusive = TEST_WAL_SEG_SIZE * 2;
	UT_ASSERT_EQ(cluster_wal_reuse_guard_init(&guard, &reason), CLUSTER_WAL_GUARD_OK);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_preflight(&guard, &request, &needs, &reason),
				 CLUSTER_WAL_GUARD_BLOCKED);
	UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_ROOT_REQUIRED);
	UT_ASSERT_NULL(needs);
	UT_ASSERT_EQ(guard.state, CLUSTER_WAL_GUARD_EMPTY);
	UT_ASSERT_EQ(cluster_wal_reuse_guard_finish(&guard, &outcome, &reason),
				 CLUSTER_WALR_RELEASE_NOT_HELD);

	cluster_wal_threads_dir = NULL;
	unlink(wal_path);
	rmdir(thread_dir);
	rmdir(root_dir);
}

UT_TEST(test_fold_unconfigured_is_unconstrained)
{
	ClusterWalRootFoldInput inputs[2];
	ClusterWalRootFold fold;

	memset(inputs, 0, sizeof(inputs));
	UT_ASSERT_TRUE(cluster_wal_retention_fold_validated_roots(inputs, lengthof(inputs),
															  TEST_WAL_SEG_SIZE, &fold));
	UT_ASSERT_EQ(fold.result, CLUSTER_WAL_FOLD_UNCONSTRAINED);
	UT_ASSERT_EQ(fold.nintervals, 0);
}

UT_TEST(test_fold_configured_missing_is_unknown)
{
	ClusterWalRootFoldInput inputs[1];
	ClusterWalRootFold fold;

	inputs[0] = make_fold_input(true, CLUSTER_CONTROL_ROOT_ABSENT,
								make_root(1, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN, 1,
										  TEST_WAL_SEG_SIZE, TEST_WAL_SEG_SIZE * 2));
	UT_ASSERT_TRUE(cluster_wal_retention_fold_validated_roots(inputs, lengthof(inputs),
															  TEST_WAL_SEG_SIZE, &fold));
	UT_ASSERT_EQ(fold.result, CLUSTER_WAL_FOLD_UNKNOWN);
	UT_ASSERT_EQ(fold.nintervals, 0);
}

UT_TEST(test_fold_open_and_recovery_required_are_bounded)
{
	ClusterWalRootFoldInput inputs[2];
	ClusterWalRootFold fold;

	inputs[0] = make_fold_input(true, CLUSTER_CONTROL_ROOT_OK_PRIMARY,
								make_root(1, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN, 1,
										  TEST_WAL_SEG_SIZE + 7, TEST_WAL_SEG_SIZE * 3));
	inputs[1] = make_fold_input(true, CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED,
								make_root(2, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED, 2,
										  TEST_WAL_SEG_SIZE * 4, TEST_WAL_SEG_SIZE * 6));
	UT_ASSERT_TRUE(cluster_wal_retention_fold_validated_roots(inputs, lengthof(inputs),
															  TEST_WAL_SEG_SIZE, &fold));
	UT_ASSERT_EQ(fold.result, CLUSTER_WAL_FOLD_BOUNDED);
	UT_ASSERT_EQ(fold.nintervals, 2);
	UT_ASSERT_EQ(fold.intervals[0].thread_id, 1);
	UT_ASSERT_EQ(fold.intervals[0].tli, 1);
	UT_ASSERT_EQ(fold.intervals[1].thread_id, 2);
	UT_ASSERT_EQ(fold.floor_by_thread[0], 1);
	UT_ASSERT_EQ(fold.floor_by_thread[1], 4);
}

UT_TEST(test_fold_terminal_lifecycles_contribute_nothing)
{
	ClusterWalRootFoldInput inputs[3];
	ClusterWalRootFold fold;

	inputs[0] = make_fold_input(true, CLUSTER_CONTROL_ROOT_OK_PRIMARY,
								make_root(1, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE, 1,
										  TEST_WAL_SEG_SIZE, TEST_WAL_SEG_SIZE * 2));
	inputs[1] = make_fold_input(true, CLUSTER_CONTROL_ROOT_OK_PRIMARY,
								make_root(2, CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED, 1,
										  TEST_WAL_SEG_SIZE, TEST_WAL_SEG_SIZE * 2));
	inputs[2] = make_fold_input(true, CLUSTER_CONTROL_ROOT_OK_PRIMARY,
								make_root(3, CLUSTER_CONTROL_ROOT_LIFECYCLE_RETIRED, 1,
										  TEST_WAL_SEG_SIZE, TEST_WAL_SEG_SIZE * 2));
	UT_ASSERT_TRUE(cluster_wal_retention_fold_validated_roots(inputs, lengthof(inputs),
															  TEST_WAL_SEG_SIZE, &fold));
	UT_ASSERT_EQ(fold.result, CLUSTER_WAL_FOLD_UNCONSTRAINED);
	UT_ASSERT_EQ(fold.nintervals, 0);
}

UT_TEST(test_fold_refuses_invalid_root_shape)
{
	ClusterWalRootFoldInput input;
	ClusterWalRootFold fold;
	ClusterControlRootSnapshot root = make_root(1, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN, 1,
												TEST_WAL_SEG_SIZE, TEST_WAL_SEG_SIZE * 2);

	root.tail_tli = 2;
	input = make_fold_input(true, CLUSTER_CONTROL_ROOT_OK_PRIMARY, root);
	UT_ASSERT_TRUE(cluster_wal_retention_fold_validated_roots(&input, 1, TEST_WAL_SEG_SIZE, &fold));
	UT_ASSERT_EQ(fold.result, CLUSTER_WAL_FOLD_UNKNOWN);

	root = make_root(1, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN, 1, TEST_WAL_SEG_SIZE * 2,
					 TEST_WAL_SEG_SIZE);
	input = make_fold_input(true, CLUSTER_CONTROL_ROOT_OK_PRIMARY, root);
	UT_ASSERT_TRUE(cluster_wal_retention_fold_validated_roots(&input, 1, TEST_WAL_SEG_SIZE, &fold));
	UT_ASSERT_EQ(fold.result, CLUSTER_WAL_FOLD_UNKNOWN);

	root = make_root(1, CLUSTER_CONTROL_ROOT_LIFECYCLE_UNUSED, 1, TEST_WAL_SEG_SIZE,
					 TEST_WAL_SEG_SIZE * 2);
	input = make_fold_input(true, CLUSTER_CONTROL_ROOT_OK_PRIMARY, root);
	UT_ASSERT_TRUE(cluster_wal_retention_fold_validated_roots(&input, 1, TEST_WAL_SEG_SIZE, &fold));
	UT_ASSERT_EQ(fold.result, CLUSTER_WAL_FOLD_UNKNOWN);
}

UT_TEST(test_fold_empty_open_interval_is_not_bounded)
{
	ClusterWalRootFoldInput input;
	ClusterWalRootFold fold;

	input = make_fold_input(
		true, CLUSTER_CONTROL_ROOT_OK_PRIMARY,
		make_root(1, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN, 1, TEST_WAL_SEG_SIZE, TEST_WAL_SEG_SIZE));
	UT_ASSERT_TRUE(cluster_wal_retention_fold_validated_roots(&input, 1, TEST_WAL_SEG_SIZE, &fold));
	UT_ASSERT_EQ(fold.result, CLUSTER_WAL_FOLD_UNCONSTRAINED);
	UT_ASSERT_EQ(fold.nintervals, 0);
}

static ClusterWalRetentionPinThreadRequest
make_pending_pin_request(void)
{
	ClusterWalRetentionPinThreadRequest request = make_pin_request(1, NULL, 0);
	cluster_shared_config = true;
	memset(&request.root_read, 0, sizeof(request.root_read));
	request.pending.file.system_identifier = request.duty.system_identifier;
	memcpy(request.pending.file.authority_uuid, request.duty.authority_uuid, 16);
	request.pending.file.format_version = 3;
	request.pending.file.record_count = 128;
	request.pending.file.activation_state = CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE;
	request.pending.file.file_txn_seq = 77;
	memset(request.pending.file.image_sha256, 0x21, 32);
	request.pending.generation = 76;
	memset(request.pending.sha256, 0x31, 32);
	memset(request.pending.operation_uuid, 0x41, 16);
	memset(&fake_pending_subject, 0, sizeof(fake_pending_subject));
	fake_pending_subject.kind = CLUSTER_CONTROL_RECOVERY_PENDING_INITIALIZER;
	fake_pending_subject.duty = request.duty;
	fake_pending_subject.pending = request.pending;
	return request;
}

UT_TEST(test_pending_pin_protects_input_without_fake_checkpoint_interval)
{
	ClusterWalRetentionPinThreadRequest request;
	ClusterWalRetentionPin *pin = NULL;
	ClusterRecoverySerialGuard serial;
	ClusterWalRootPublishGuard *publisher = NULL;
	reset_pin_fakes();
	request = make_pending_pin_request();
	serial = make_serial_guard(&request);
	serial.mode = CLUSTER_RECOVERY_SERIAL_INITIALIZER;
	serial.pending = request.pending;
	serial.lock_request.request_id = 1234;
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(fake_acquire_call_count, 1);
	if (pin != NULL) {
		UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &serial), CLUSTER_WAL_PIN_OK);
		UT_ASSERT_EQ(cluster_wal_retention_pin_seal_for_root_publish(pin), CLUSTER_WAL_PIN_OK);
		UT_ASSERT_EQ(cluster_wal_retention_pending_publish_begin(&request.duty, &request.pending,
																 &publisher),
					 CLUSTER_WAL_PIN_STALE);
		serial.held = false;
		serial.release_uncertain = true;
		UT_ASSERT_EQ(cluster_wal_retention_pending_publish_begin(&request.duty, &request.pending,
																 &publisher),
					 CLUSTER_WAL_PIN_STALE);
		serial.release_uncertain = false;
		UT_ASSERT_EQ(cluster_wal_retention_pending_publish_begin(&request.duty, &request.pending,
																 &publisher),
					 CLUSTER_WAL_PIN_OK);
		UT_ASSERT_TRUE(cluster_wal_retention_pending_publish_current(publisher, &request.duty,
																	 &request.pending));
		UT_ASSERT_EQ(fake_acquire_call_count, 1);
		request.pending.operation_uuid[0] ^= 1;
		UT_ASSERT_FALSE(cluster_wal_retention_pending_publish_current(publisher, &request.duty,
																	  &request.pending));
		request.pending.operation_uuid[0] ^= 1;
		fake_control_owner_current = false;
		UT_ASSERT_FALSE(cluster_wal_retention_pending_publish_current(publisher, &request.duty,
																	  &request.pending));
		fake_control_owner_current = true;
		fake_admission_current = false;
		UT_ASSERT_FALSE(cluster_wal_retention_pending_publish_current(publisher, &request.duty,
																	  &request.pending));
		fake_admission_current = true;
		UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&publisher),
					 CLUSTER_WALR_RELEASE_CONFIRMED);
		UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	}
	cluster_shared_config = false;
}

UT_TEST(test_pending_pin_refuses_stale_or_mixed_inputs_before_retaining_owner)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		ClusterWalRetentionPinThreadRequest request;
		ClusterWalRetentionPin *pin = NULL;
		reset_pin_fakes();
		request = make_pending_pin_request();
		switch (fault) {
		case 0:
			request.root_read.file_txn_seq = 1;
			break;
		case 1:
			request.nintervals = 1;
			break;
		case 2:
			request.pending.operation_uuid[0] ^= 1;
			break;
		case 3:
			fake_pending_subject.kind = CLUSTER_CONTROL_RECOVERY_CURRENT_CHECKPOINT;
			break;
		case 4:
			fake_pending_subject.duty.origin_owner_incarnation++;
			break;
		case 5:
			fake_pending_subject.pending.file.image_sha256[0] ^= 1;
			break;
		case 6:
			fake_formation_current = false;
			break;
		case 7:
			fake_admission_current = false;
			break;
		}
		UT_ASSERT(cluster_wal_retention_pin_acquire(&request, 1, &pin) != CLUSTER_WAL_PIN_OK);
		UT_ASSERT_NULL(pin);
		UT_ASSERT_EQ(fake_acquire_call_count, fault < 2 ? 0 : 1);
		UT_ASSERT_EQ(fake_release_call_count, fake_acquire_call_count);
		UT_ASSERT_FALSE(cluster_wal_retention_active_pin_present());
		cluster_shared_config = false;
	}
}

static ClusterWalRetentionPinThreadRequest
input_pin_request(uint16 thread, ClusterWalRetentionInterval *interval)
{
	*interval = (ClusterWalRetentionInterval){ .thread_id = thread, .tli = 1,
		.start_lsn = TEST_WAL_SEG_SIZE, .end_lsn = TEST_WAL_SEG_SIZE * 2 };
	return make_pin_request(thread, interval, 1);
}

UT_TEST(test_input_pin_orders_union_and_keeps_original_ir_subset)
{
	const uint16 threads[] = { 1, 2, 3, 4 };
	ClusterWalRetentionInterval intervals[2];
	ClusterWalRetentionPinThreadRequest requests[2];
	ClusterRecoverySerialGuardSet serial = { 0 };
	ClusterRecoverySerialGuard *borrowed[4] = { NULL };
	ClusterWalRetentionPin *pin = NULL, *borrow = NULL;
	uint16 count = 0;

	reset_pin_fakes();
	cluster_shared_config = true;
	requests[0] = input_pin_request(2, &intervals[0]);
	requests[1] = input_pin_request(4, &intervals[1]);
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire_inputs_v1(requests, 2, threads, 4, &pin),
				 CLUSTER_WAL_PIN_OK);
	if (pin == NULL)
		return;
	UT_ASSERT_EQ(fake_acquire_call_count, 4);
	for (unsigned i = 0; i < 4; i++) {
		UT_ASSERT_EQ(fake_acquire_requests[i].resid.field1, threads[i]);
		UT_ASSERT_EQ(fake_acquire_requests[i].lockmode, ShareLock);
	}
	serial.count = 2;
	for (unsigned i = 0; i < 2; i++) {
		serial.guards[i] = make_serial_guard(&requests[i]);
		serial.guards[i].mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
	}
	UT_ASSERT_EQ(cluster_wal_retention_pin_bind_set(pin, &serial), CLUSTER_WAL_PIN_OK);
	if (pin == NULL)
		return;
	UT_ASSERT_EQ(cluster_wal_retention_pin_borrow_cold_v1(&borrow, borrowed, 4, &count),
				 CLUSTER_WAL_PIN_OK);
	if (pin == NULL)
		return;
	UT_ASSERT(borrow == pin);
	UT_ASSERT_EQ(count, 2);
	UT_ASSERT(borrowed[0] == &serial.guards[0] && borrowed[1] == &serial.guards[1]);
	UT_ASSERT_NULL(borrowed[2]);
	UT_ASSERT_EQ(cluster_wal_retention_pin_seal_for_root_publish(pin), CLUSTER_WAL_PIN_OK);
	if (pin == NULL)
		return;
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(fake_release_call_count, 4);
	for (unsigned i = 0; i < 4; i++)
		UT_ASSERT_EQ(fake_release_threads[i], threads[3 - i]);
	reset_pin_fakes();
}

UT_TEST(test_input_pin_refuses_bad_or_incomplete_sets_before_io)
{
	const uint16 invalid[][2] = { { 2, 1 }, { 2, 2 }, { 0, 2 }, { 2, 129 }, { 1, 3 } };
	ClusterWalRetentionInterval interval;
	ClusterWalRetentionPinThreadRequest request = input_pin_request(2, &interval);
	ClusterWalRetentionPin *pin = NULL;

	reset_pin_fakes();
	cluster_shared_config = true;
	for (unsigned i = 0; i < lengthof(invalid); i++)
		UT_ASSERT_EQ(cluster_wal_retention_pin_acquire_inputs_v1(&request, 1, invalid[i], 2, &pin),
					 CLUSTER_WAL_PIN_INVALID);
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire_inputs_v1(&request, 1, NULL, 0, &pin),
				 CLUSTER_WAL_PIN_INVALID);
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire_inputs_v1(&request, 1, invalid[0], 129, &pin),
				 CLUSTER_WAL_PIN_INVALID);
	UT_ASSERT_NULL(pin);
	UT_ASSERT_EQ(fake_acquire_call_count, 0);
	reset_pin_fakes();
}

UT_TEST(test_input_pin_unwinds_each_partial_union_grant)
{
	const uint16 threads[] = { 1, 2, 3, 4 };
	ClusterWalRetentionInterval interval;
	ClusterWalRetentionPinThreadRequest request = input_pin_request(2, &interval);

	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterWalRetentionPin *pin = NULL;
		reset_pin_fakes();
		cluster_shared_config = true;
		fake_acquire_result_count = 4;
		fake_acquire_results[fault] = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
		UT_ASSERT_EQ(cluster_wal_retention_pin_acquire_inputs_v1(&request, 1, threads, 4, &pin),
					 CLUSTER_WAL_PIN_UNAVAILABLE);
		UT_ASSERT_NULL(pin);
		UT_ASSERT_EQ(fake_acquire_call_count, fault + 1);
		UT_ASSERT_EQ(fake_release_call_count, fault);
		for (unsigned i = 0; i < fault; i++)
			UT_ASSERT_EQ(fake_release_threads[i], threads[fault - i - 1]);
		UT_ASSERT_FALSE(cluster_wal_retention_active_pin_present());
	}
	reset_pin_fakes();
}

UT_TEST(test_input_pin_checks_extra_owners_before_bind_and_after_ir)
{
	const uint16 threads[] = { 1, 2, 3 };
	ClusterWalRetentionInterval interval;
	ClusterWalRetentionPinThreadRequest request = input_pin_request(2, &interval);
	ClusterRecoverySerialGuard serial = make_serial_guard(&request);

	for (unsigned phase = 0; phase < 3; phase++) {
		ClusterWalRetentionPin *pin = NULL;
		reset_pin_fakes();
		cluster_shared_config = true;
		UT_ASSERT_EQ(cluster_wal_retention_pin_acquire_inputs_v1(&request, 1, threads, 3, &pin),
					 CLUSTER_WAL_PIN_OK);
		if (pin == NULL)
			return;
		if (phase == 2)
			UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &serial), CLUSTER_WAL_PIN_OK);
		fake_control_owner_stale_thread = 3;
		if (phase == 0)
			UT_ASSERT_EQ(cluster_wal_retention_pin_preflight_revalidate_wait_v1(pin),
						 CLUSTER_WAL_PIN_STALE);
		else if (phase == 1)
			UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &serial), CLUSTER_WAL_PIN_STALE);
		else
			UT_ASSERT_EQ(cluster_wal_retention_pin_revalidate(pin), CLUSTER_WAL_PIN_STALE);
		fake_control_owner_stale_thread = 0;
		UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
		UT_ASSERT_EQ(fake_release_call_count, 3);
	}
	reset_pin_fakes();
}

UT_TEST(test_input_pin_uncertain_release_keeps_entire_cleanup_owner)
{
	const uint16 threads[] = { 1, 2, 3 };
	ClusterWalRetentionInterval interval;
	ClusterWalRetentionPinThreadRequest request = input_pin_request(2, &interval);
	ClusterRecoverySerialGuard serial = make_serial_guard(&request);
	ClusterWalRetentionPin *pin = NULL, *other = NULL;

	reset_pin_fakes();
	cluster_shared_config = true;
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire_inputs_v1(&request, 1, threads, 3, &pin),
				 CLUSTER_WAL_PIN_OK);
	if (pin == NULL)
		return;
	UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(pin, &serial), CLUSTER_WAL_PIN_OK);
	if (pin == NULL)
		return;
	fake_release_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_UNCONFIRMED);
	UT_ASSERT_NOT_NULL(pin);
	UT_ASSERT_EQ(fake_release_call_count, 3);
	UT_ASSERT_EQ(cluster_wal_retention_pin_revalidate(pin), CLUSTER_WAL_PIN_STALE);
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire_inputs_v1(&request, 1, threads, 3, &other),
				 CLUSTER_WAL_PIN_CAPACITY);
	fake_release_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(fake_release_call_count, 6);
	reset_pin_fakes();
}

UT_TEST(test_input_pin_error_cleanup_releases_granted_history)
{
	const uint16 threads[] = { 1, 2, 3 };
	ClusterWalRetentionInterval interval;
	ClusterWalRetentionPinThreadRequest request = input_pin_request(2, &interval);
	ClusterWalRetentionPin *pin = NULL;
	volatile bool caught = false;

	reset_pin_fakes();
	cluster_shared_config = true;
	fake_acquire_error_at = 3;
	PG_TRY();
	{
		(void)cluster_wal_retention_pin_acquire_inputs_v1(&request, 1, threads, 3, &pin);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	if (!caught)
		return;
	fake_resource_release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false,
								   fake_resource_release_arg);
	UT_ASSERT_EQ(fake_release_call_count, 2);
	UT_ASSERT_EQ(fake_release_threads[0], 2);
	UT_ASSERT_EQ(fake_release_threads[1], 1);
	UT_ASSERT_FALSE(cluster_wal_retention_active_pin_present());
	reset_pin_fakes();
}

UT_TEST(test_read_pin_covers_sorted_threads_without_recovery_authority)
{
	uint16 threads[] = { 1, 3 };
	ClusterWalReadPinV1 *pin = NULL;

	reset_pin_fakes();
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_v1(threads, 2, &pin), CLUSTER_WAL_PIN_INVALID);
	cluster_shared_config = true;
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_v1(threads, 2, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_NOT_NULL(pin);
	UT_ASSERT_EQ(fake_acquire_call_count, 2);
	UT_ASSERT_EQ(fake_native_acquire_call_count, 2);
	UT_ASSERT_EQ(fake_acquire_requests[0].resid.field1, 1);
	UT_ASSERT_EQ(fake_acquire_requests[1].resid.field1, 3);
	UT_ASSERT_EQ(fake_acquire_requests[1].lockmode, ShareLock);
	UT_ASSERT(fake_acquire_requests[1].dontwait);
	UT_ASSERT(cluster_wal_read_pin_covers_v1(pin, 1));
	UT_ASSERT(cluster_wal_read_pin_covers_v1(pin, 3));
	UT_ASSERT_FALSE(cluster_wal_read_pin_covers_v1(pin, 2));
	UT_ASSERT_FALSE(cluster_wal_retention_active_pin_present());
	UT_ASSERT_EQ(cluster_wal_read_pin_release_v1(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_NULL(pin);
	UT_ASSERT_EQ(fake_release_call_count, 2);
	UT_ASSERT_EQ(fake_release_threads[0], 3);
	UT_ASSERT_EQ(fake_release_threads[1], 1);
	UT_ASSERT_EQ(fake_native_release_call_count, 2);
	UT_ASSERT_EQ(cluster_wal_read_pin_release_v1(&pin), CLUSTER_WALR_RELEASE_NOT_HELD);
	reset_pin_fakes();
}

UT_TEST(test_released_read_census_allows_real_e1_gc_owner_to_progress)
{
	V2ReuseFixture f;
	uint16 threads[] = { 1, 3 };
	v2_reuse_fixture(&f);
	for (int retry = 0; retry < 10; retry++) {
		ClusterWalReadPinV1 *pin = NULL;
		ClusterWalRetentionE1Context context = { 0 };
		ClusterWalRootFoldResult fold;
		ClusterWalReuseDenyReason reason;
		XLogSegNo floor;
		UT_ASSERT_EQ(cluster_wal_read_pin_acquire_v1(threads, 2, &pin), CLUSTER_WAL_PIN_OK);
		UT_ASSERT_EQ(cluster_wal_retention_e1_coarse_begin(&context, 1, &fold, &floor, &reason),
					 CLUSTER_WAL_GUARD_INVALID);
		UT_ASSERT_EQ(reason, CLUSTER_WAL_DENY_GUARD_STATE);
		UT_ASSERT_EQ(cluster_wal_read_pin_release_v1(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
		UT_ASSERT_EQ(cluster_wal_retention_e1_coarse_begin(&context, 1, &fold, &floor, &reason),
					 CLUSTER_WAL_GUARD_OK);
		UT_ASSERT(context.coarse_walr.held && context.coarse_walr.mode == ExclusiveLock);
		UT_ASSERT_EQ(fold, CLUSTER_WAL_FOLD_BOUNDED);
		UT_ASSERT_EQ(floor, 4);
		UT_ASSERT_EQ(cluster_wal_retention_e1_coarse_release(&context, &reason),
					 CLUSTER_WALR_RELEASE_CONFIRMED);
		cluster_wal_retention_e1_finish(&context);
	}
	v2_reuse_fixture_cleanup(&f);
}

UT_TEST(test_read_pin_rejects_bad_shape_and_rolls_back_partial_grants)
{
	uint16 threads[] = { 1, 3 };
	const uint16 invalid[][2] = { { 0, 3 }, { 3, 1 }, { 1, 1 }, { 1, 129 } };
	ClusterWalReadPinV1 *pin = NULL;
	ResourceOwner saved = CurrentResourceOwner;

	reset_pin_fakes();
	cluster_shared_config = true;
	for (int i = 0; i < lengthof(invalid); i++)
		UT_ASSERT_EQ(cluster_wal_read_pin_acquire_v1(invalid[i], 2, &pin), CLUSTER_WAL_PIN_INVALID);
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_v1(NULL, 2, &pin), CLUSTER_WAL_PIN_INVALID);
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_v1(threads, 0, &pin), CLUSTER_WAL_PIN_INVALID);
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_v1(threads, 129, &pin), CLUSTER_WAL_PIN_INVALID);
	CurrentResourceOwner = NULL;
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_v1(threads, 2, &pin), CLUSTER_WAL_PIN_INVALID);
	CurrentResourceOwner = saved;
	UT_ASSERT_EQ(fake_acquire_call_count, 0);
	fake_acquire_result_count = 2;
	fake_acquire_results[0] = CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK;
	fake_acquire_results[1] = CLUSTER_LOCK_ACQUIRE_NOT_AVAIL;
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_v1(threads, 2, &pin), CLUSTER_WAL_PIN_UNAVAILABLE);
	UT_ASSERT_NULL(pin);
	UT_ASSERT_EQ(fake_release_call_count, 1);
	UT_ASSERT_EQ(fake_native_release_call_count, 1);
	reset_pin_fakes();
}

UT_TEST(test_read_pin_uncertain_release_is_cleanup_only)
{
	uint16 threads[] = { 1, 3 };
	ClusterWalReadPinV1 *pin = NULL;
	ClusterWalReadPinV1 *other = NULL;

	reset_pin_fakes();
	cluster_shared_config = true;
	fake_acquire_result_count = 2;
	fake_acquire_results[0] = CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK;
	fake_acquire_results[1] = CLUSTER_LOCK_ACQUIRE_NOT_AVAIL;
	fake_release_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_v1(threads, 2, &pin),
				 CLUSTER_WAL_PIN_RELEASE_UNCERTAIN);
	UT_ASSERT_NOT_NULL(pin);
	UT_ASSERT_FALSE(cluster_wal_read_pin_covers_v1(pin, 1));
	UT_ASSERT_EQ(fake_native_release_call_count, 0);
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_v1(threads, 2, &other), CLUSTER_WAL_PIN_CAPACITY);
	UT_ASSERT_NULL(other);
	fake_release_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT_EQ(cluster_wal_read_pin_release_v1(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_NULL(pin);
	UT_ASSERT_EQ(fake_native_release_call_count, 1);
	reset_pin_fakes();
}

UT_TEST(test_read_release_trace_keeps_first_failed_member_and_all_debt)
{
	uint16 threads[] = { 1, 3 };
	ClusterWalReadPinV1 *pin = NULL;

	reset_pin_fakes();
	cluster_shared_config = true;
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_v1(threads, 2, &pin), CLUSTER_WAL_PIN_OK);
	capture_release_log = cluster_trace_control_config = true;
	fake_release_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	UT_ASSERT_EQ(cluster_wal_read_pin_release_v1(&pin), CLUSTER_WALR_RELEASE_UNCONFIRMED);
	UT_ASSERT_NOT_NULL(pin);
	UT_ASSERT_FALSE(cluster_wal_read_pin_covers_v1(pin, 1));
	UT_ASSERT_EQ(fake_release_call_count, 2);
	UT_ASSERT_EQ(fake_native_release_call_count, 0);
	UT_ASSERT_EQ(read_release_observation_count, 1);
	UT_ASSERT(strstr(read_release_observations[0], "task=read_member"));
	UT_ASSERT(strstr(read_release_observations[0], "member=1 members=2"));
	UT_ASSERT(strstr(read_release_observations[0], "thread=3 mode=5"));
	UT_ASSERT(strstr(read_release_observations[0], "request=2 epoch=1"));
	UT_ASSERT(strstr(read_release_observations[0], "holder_node=0 holder_procno=1"));
	UT_ASSERT(strstr(read_release_observations[0], "lock_result=16 held_before=1 uncertain_before=0"));
	UT_ASSERT_EQ(cluster_wal_read_pin_release_v1(&pin), CLUSTER_WALR_RELEASE_UNCONFIRMED);
	UT_ASSERT_EQ(read_release_observation_count, 2);
	UT_ASSERT(strstr(read_release_observations[1], "held_before=1 uncertain_before=1"));
	UT_ASSERT_EQ(fake_acquire_call_count, 2);
	UT_ASSERT_EQ(fake_release_call_count, 4);
	fake_release_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT_EQ(cluster_wal_read_pin_release_v1(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_NULL(pin);
	UT_ASSERT_EQ(read_release_observation_count, 2);
	UT_ASSERT_EQ(fake_native_release_call_count, 2);
	reset_pin_fakes();
}

UT_TEST(test_read_release_trace_off_and_critical_preserve_original_cleanup)
{
	uint16 threads[] = { 1, 3 };
	ClusterWalReadPinV1 *pin = NULL;

	reset_pin_fakes();
	cluster_shared_config = capture_release_log = true;
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_v1(threads, 2, &pin), CLUSTER_WAL_PIN_OK);
	fake_release_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	UT_ASSERT_EQ(cluster_wal_read_pin_release_v1(&pin), CLUSTER_WALR_RELEASE_UNCONFIRMED);
	UT_ASSERT_EQ(read_release_observation_count, 0);
	cluster_trace_control_config = true;
	CritSectionCount = 1;
	UT_ASSERT_EQ(cluster_wal_read_pin_release_v1(&pin), CLUSTER_WALR_RELEASE_UNCONFIRMED);
	UT_ASSERT_EQ(read_release_observation_count, 0);
	UT_ASSERT_EQ(fake_release_call_count, 4);
	UT_ASSERT_EQ(fake_native_release_call_count, 0);
	CritSectionCount = 0;
	fake_release_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT_EQ(cluster_wal_read_pin_release_v1(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(read_release_observation_count, 0);
	UT_ASSERT_EQ(fake_native_release_call_count, 2);
	reset_pin_fakes();
}

UT_TEST(test_read_release_callback_records_aggregate_and_keeps_original_fatal)
{
	uint16 thread = 3;
	ClusterWalReadPinV1 *pin = NULL;
	volatile bool caught = false;

	reset_pin_fakes();
	cluster_shared_config = true;
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_v1(&thread, 1, &pin), CLUSTER_WAL_PIN_OK);
	capture_release_log = cluster_trace_control_config = true;
	expect_read_cleanup_fatal = proc_exit_inprogress = true;
	MyBackendType = B_BG_WRITER;
	fake_release_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	PG_TRY();
	{
		fake_resource_release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true,
									   fake_resource_release_arg);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	expect_read_cleanup_fatal = false;
	UT_ASSERT(caught);
	UT_ASSERT(strcmp(read_cleanup_fatal, "could not confirm WALR read-scope cleanup") == 0);
	UT_ASSERT_EQ(read_release_observation_count, 2);
	UT_ASSERT(strstr(read_release_observations[0], "lock_result=13"));
	UT_ASSERT(strstr(read_release_observations[1], "task=read_scope_cleanup"));
	UT_ASSERT(strstr(read_release_observations[1], "role=6 proc_exit=1"));
	UT_ASSERT(strstr(read_release_observations[1], "release_result=2"));
	UT_ASSERT_EQ(fake_release_call_count, 1);
	UT_ASSERT_EQ(fake_native_release_call_count, 0);
	fake_release_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT_EQ(cluster_wal_read_pin_release_v1(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(fake_native_release_call_count, 1);
	reset_pin_fakes();
}

UT_TEST(test_read_pin_checks_every_actual_owner_and_never_revives)
{
	uint16 threads[] = { 1, 3 };
	ClusterWalReadPinV1 *pin = NULL;
	ResourceOwner saved = CurrentResourceOwner;

	reset_pin_fakes();
	cluster_shared_config = true;
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_v1(threads, 2, &pin), CLUSTER_WAL_PIN_OK);
	CurrentResourceOwner = (ResourceOwner)(uintptr_t)2;
	UT_ASSERT_FALSE(cluster_wal_read_pin_covers_v1(pin, 1));
	UT_ASSERT_EQ(cluster_wal_read_pin_release_v1(&pin), CLUSTER_WALR_RELEASE_INVALID);
	CurrentResourceOwner = saved;
	fake_control_owner_stale_thread = 3;
	UT_ASSERT_FALSE(cluster_wal_read_pin_covers_v1(pin, 1));
	fake_control_owner_stale_thread = 0;
	UT_ASSERT_FALSE(cluster_wal_read_pin_covers_v1(pin, 1));
	UT_ASSERT_EQ(cluster_wal_read_pin_release_v1(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	fake_control_owner_current = false;
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_v1(threads, 2, &pin), CLUSTER_WAL_PIN_STALE);
	UT_ASSERT_NULL(pin);
	reset_pin_fakes();
}

UT_TEST(test_read_pin_exact_publisher_borrow_cannot_release_or_seal_scope)
{
	uint16 threads[] = { 1, 3 };
	ClusterWalReadPinV1 *pin = NULL;
	ClusterWalRootPublishGuard *publisher = NULL;
	ClusterWalRetentionInterval interval = { .thread_id = 1,
											 .tli = 1,
											 .start_lsn = TEST_WAL_SEG_SIZE,
											 .end_lsn = TEST_WAL_SEG_SIZE * 2 };
	ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);
	ClusterControlRootReadToken token = request.root_read;

	reset_pin_fakes();
	cluster_shared_config = true;
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_v1(threads, 2, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_begin_exact(&token, true, &publisher),
				 CLUSTER_WAL_PIN_STALE);
	token.origin_thread_id = 2;
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_begin_exact(&token, false, &publisher),
				 CLUSTER_WAL_PIN_STALE);
	token = request.root_read;
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_begin_exact(&token, false, &publisher),
				 CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(fake_acquire_call_count, 2);
	UT_ASSERT_FALSE(cluster_wal_retention_root_publish_sealed_current(publisher, &token));
	UT_ASSERT_EQ(cluster_wal_read_pin_release_v1(&pin), CLUSTER_WALR_RELEASE_INVALID);
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&publisher),
				 CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(fake_release_call_count, 0);
	UT_ASSERT(cluster_wal_read_pin_covers_v1(pin, 1));
	fake_control_owner_stale_thread = 3;
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_begin_exact(&token, false, &publisher),
				 CLUSTER_WAL_PIN_STALE);
	UT_ASSERT_NULL(publisher);
	UT_ASSERT_EQ(cluster_wal_read_pin_release_v1(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	reset_pin_fakes();
}

UT_TEST(test_read_pin_cannot_overlap_other_retention_owners)
{
	uint16 thread = 1;
	ClusterWalReadPinV1 *pin = NULL;
	ClusterWalRetentionPin *recovery = NULL;
	ClusterWalRootPublishGuard *publisher = NULL;
	ClusterWalRetentionE1Context e1 = { 0 };
	ClusterWalRootFoldResult fold;
	XLogSegNo floor;
	ClusterWalReuseDenyReason reason;
	ClusterWalRetentionInterval interval = { .thread_id = 1,
											 .tli = 1,
											 .start_lsn = TEST_WAL_SEG_SIZE,
											 .end_lsn = TEST_WAL_SEG_SIZE * 2 };
	ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);

	reset_pin_fakes();
	cluster_shared_config = true;
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &recovery), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_v1(&thread, 1, &pin), CLUSTER_WAL_PIN_CAPACITY);
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&recovery), CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(
		cluster_wal_retention_root_publish_begin_exact(&request.root_read, false, &publisher),
		CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_v1(&thread, 1, &pin), CLUSTER_WAL_PIN_CAPACITY);
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&publisher),
				 CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_v1(&thread, 1, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &recovery),
				 CLUSTER_WAL_PIN_CAPACITY);
	UT_ASSERT_NULL(recovery);
	UT_ASSERT_EQ(cluster_wal_retention_e1_coarse_begin(&e1, 1, &fold, &floor, &reason),
				 CLUSTER_WAL_GUARD_INVALID);
	UT_ASSERT_EQ(fake_acquire_call_count, 3);
	UT_ASSERT_EQ(cluster_wal_read_pin_release_v1(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	reset_pin_fakes();
}

UT_TEST(test_read_pin_resource_owner_cleans_error_and_publisher_borrow)
{
	uint16 threads[] = { 1, 3 };
	ClusterWalReadPinV1 *pin = NULL;
	ClusterWalRootPublishGuard *publisher = NULL;
	ClusterWalRetentionInterval interval = { .thread_id = 1,
											 .tli = 1,
											 .start_lsn = TEST_WAL_SEG_SIZE,
											 .end_lsn = TEST_WAL_SEG_SIZE * 2 };
	ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);
	volatile bool caught = false;

	reset_pin_fakes();
	cluster_shared_config = true;
	fake_acquire_error_at = 2;
	PG_TRY();
	{
		(void)cluster_wal_read_pin_acquire_v1(threads, 2, &pin);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_NOT_NULL(fake_resource_release_callback);
	fake_resource_release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false,
								   fake_resource_release_arg);
	UT_ASSERT_EQ(fake_release_call_count, 1);
	UT_ASSERT_EQ(fake_native_release_call_count, 1);
	reset_pin_fakes();
	cluster_shared_config = true;
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_v1(threads, 2, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(
		cluster_wal_retention_root_publish_begin_exact(&request.root_read, false, &publisher),
		CLUSTER_WAL_PIN_OK);
	fake_resource_release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, true, false,
								   fake_resource_release_arg);
	UT_ASSERT_EQ(fake_release_call_count, 2);
	UT_ASSERT_EQ(fake_native_release_call_count, 2);
	/* Abort cleanup owns the released objects; caller copies cannot revive them. */
	UT_ASSERT_FALSE(cluster_wal_read_pin_covers_v1(pin, 1));
	pin = NULL;
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_v1(threads, 2, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_read_pin_release_v1(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	reset_pin_fakes();
}

/* PGRAC: real WALR lifecycle, with only GES/native/ROOT authority leaves
 * doubled. A single original cold pin owns both COMPLETE and CLOSED inputs.
 * Author: SqlRush <sqlrush@gmail.com> */
typedef struct RootPublishCohortFixture {
	ClusterWalRetentionInterval intervals[2];
	ClusterWalRetentionPinThreadRequest requests[2];
	ClusterRecoverySerialGuardSet serial;
	ClusterControlRootReadToken roots[3];
	ClusterWalRetentionPin *pin;
	uint16 nroots;
} RootPublishCohortFixture;

static void
root_publish_cohort_fixture(RootPublishCohortFixture *f, bool mixed)
{
	uint16 threads[] = { 1, 2, 3 };

	memset(f, 0, sizeof(*f));
	reset_pin_fakes();
	cluster_shared_config = true;
	f->nroots = mixed ? 3 : 2;
	f->requests[0] = input_pin_request(1, &f->intervals[0]);
	f->requests[1] = input_pin_request(mixed ? 3 : 2, &f->intervals[1]);
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire_inputs_v1(f->requests, 2, threads,
															f->nroots, &f->pin), CLUSTER_WAL_PIN_OK);
	f->serial.count = 2;
	for (unsigned i = 0; i < 2; i++) {
		f->serial.guards[i] = make_serial_guard(&f->requests[i]);
		f->serial.guards[i].mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
	}
	UT_ASSERT_EQ(cluster_wal_retention_pin_bind_set(f->pin, &f->serial), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_seal_for_root_publish(f->pin), CLUSTER_WAL_PIN_OK);
	for (unsigned i = 0; i < f->nroots; i++) {
		ClusterRecoveryDutyKey duty = make_duty(threads[i]);
		f->roots[i] = make_root_token(&duty);
		f->roots[i].file_txn_seq = 10;
		f->roots[i].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	}
	for (unsigned i = 0; i < 2; i++) {
		unsigned index = f->requests[i].duty.origin_thread_id - 1;
		ClusterControlRootSnapshot before = make_pin_root_snapshot(&f->requests[i], &f->intervals[i]);
		ClusterControlRootSnapshot after = before;
		ClusterControlRootReadToken token = f->requests[i].root_read;

		f->serial.guards[i].held = false;
		after.lifecycle = token.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
		token.file_txn_seq = 10;
		UT_ASSERT_EQ(cluster_wal_retention_pin_adopt_root_readback_v1(f->pin, &before,
								 &f->requests[i].root_read, &after, &token), CLUSTER_WAL_PIN_OK);
		f->roots[index] = token;
	}
}

UT_TEST(test_root_publish_singleton_still_refuses_a_second_guard)
{
	RootPublishCohortFixture f;
	ClusterWalRootPublishGuard *first = NULL, *second = NULL;

	root_publish_cohort_fixture(&f, false);
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_begin_exact(&f.roots[0], true, &first),
				 CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_begin_exact(&f.roots[1], true, &second),
				 CLUSTER_WAL_PIN_CAPACITY);
	UT_ASSERT_NULL(second);
	UT_ASSERT_EQ(fake_acquire_call_count, 3);
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&first), CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&f.pin), CLUSTER_WALR_RELEASE_CONFIRMED);
}

UT_TEST(test_root_publish_cohort_holds_every_converted_owner_until_end)
{
	RootPublishCohortFixture f;
	ClusterWalRootPublishGuard *guard = NULL, *other = NULL;

	root_publish_cohort_fixture(&f, false);
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_begin_set_v1(f.roots, f.nroots, &guard),
				 CLUSTER_WAL_PIN_OK);
	if (guard == NULL) {
		(void)cluster_wal_retention_pin_release(&f.pin);
		return;
	}
	UT_ASSERT_EQ(fake_acquire_call_count, 4);
	for (unsigned i = 0; i < 2; i++) {
		UT_ASSERT_EQ(fake_acquire_requests[2 + i].resid.field1, i + 1);
		UT_ASSERT_EQ(fake_acquire_requests[2 + i].op, CLUSTER_LOCK_OP_CONVERT);
		UT_ASSERT_EQ(fake_acquire_requests[2 + i].lockmode, ExclusiveLock);
		UT_ASSERT(fake_acquire_requests[2 + i].dontwait);
		UT_ASSERT(cluster_wal_retention_root_publish_sealed_current(guard, &f.roots[i]));
	}
	fake_root_current = false; /* No recursive CF acquisition/current-root read. */
	fake_control_owner_mode = ExclusiveLock;
	UT_ASSERT(cluster_wal_retention_root_publish_set_current_v1(guard));
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_begin_exact(&f.roots[1], true, &other),
				 CLUSTER_WAL_PIN_CAPACITY);
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&f.pin), CLUSTER_WALR_RELEASE_INVALID);
	UT_ASSERT_EQ(fake_release_call_count, 0);
	fake_formation_current = false;
	UT_ASSERT_FALSE(cluster_wal_retention_root_publish_set_current_v1(guard));
	fake_formation_current = true;
	fake_admission_current = false;
	UT_ASSERT_FALSE(cluster_wal_retention_root_publish_set_current_v1(guard));
	fake_admission_current = true;
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&guard), CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_NULL(guard);
	UT_ASSERT_EQ(fake_acquire_call_count, 6);
	UT_ASSERT_EQ(fake_acquire_requests[4].resid.field1, 2);
	UT_ASSERT_EQ(fake_acquire_requests[5].resid.field1, 1);
	UT_ASSERT_EQ(fake_native_release_call_count, 4);
	fake_control_owner_mode = ShareLock;
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&f.pin), CLUSTER_WALR_RELEASE_CONFIRMED);
}

UT_TEST(test_root_publish_cohort_keeps_closed_inputs_as_readers)
{
	RootPublishCohortFixture f;
	ClusterWalRootPublishGuard *guard = NULL;

	root_publish_cohort_fixture(&f, true);
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_begin_set_v1(f.roots, f.nroots, &guard),
				 CLUSTER_WAL_PIN_OK);
	if (guard == NULL) {
		(void)cluster_wal_retention_pin_release(&f.pin);
		return;
	}
	UT_ASSERT_EQ(fake_acquire_call_count, 5);
	UT_ASSERT_EQ(fake_acquire_requests[3].resid.field1, 1);
	UT_ASSERT_EQ(fake_acquire_requests[4].resid.field1, 3);
	UT_ASSERT(cluster_wal_retention_root_publish_set_current_v1(guard));
	UT_ASSERT_FALSE(cluster_wal_retention_root_publish_sealed_current(guard, &f.roots[1]));
	fake_control_owner_stale_thread = 2;
	UT_ASSERT_FALSE(cluster_wal_retention_root_publish_set_current_v1(guard));
	fake_control_owner_stale_thread = 0;
	UT_ASSERT_FALSE(cluster_wal_retention_root_publish_set_current_v1(guard)); /* sticky poison */
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&guard), CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&f.pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(fake_release_call_count, 3);
}

UT_TEST(test_root_publish_cohort_refuses_incomplete_cut_and_unreleased_ir)
{
	RootPublishCohortFixture f;
	ClusterWalRootPublishGuard *guard = NULL;
	ClusterControlRootReadToken changed[3];

	root_publish_cohort_fixture(&f, true);
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_begin_set_v1(f.roots, 0, &guard),
				 CLUSTER_WAL_PIN_INVALID);
	UT_ASSERT(cluster_wal_retention_root_publish_begin_set_v1(f.roots, 2, &guard)
			  != CLUSTER_WAL_PIN_OK);
	memcpy(changed, f.roots, sizeof(changed));
	changed[2] = changed[0];
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_begin_set_v1(changed, 3, &guard),
				 CLUSTER_WAL_PIN_INVALID);
	memcpy(changed, f.roots, sizeof(changed));
	changed[2].file_txn_seq++;
	UT_ASSERT(cluster_wal_retention_root_publish_begin_set_v1(changed, 3, &guard)
			  != CLUSTER_WAL_PIN_OK);
	changed[2] = f.roots[2];
	changed[2].record_crc32c++;
	UT_ASSERT(cluster_wal_retention_root_publish_begin_set_v1(changed, 3, &guard)
			  != CLUSTER_WAL_PIN_OK);
	f.serial.guards[1].held = true;
	UT_ASSERT(cluster_wal_retention_root_publish_begin_set_v1(f.roots, 3, &guard)
			  != CLUSTER_WAL_PIN_OK);
	f.serial.guards[1].held = false;
	f.serial.guards[1].release_uncertain = true;
	UT_ASSERT(cluster_wal_retention_root_publish_begin_set_v1(f.roots, 3, &guard)
			  != CLUSTER_WAL_PIN_OK);
	f.serial.guards[1].release_uncertain = false;
	UT_ASSERT_NULL(guard);
	UT_ASSERT_EQ(fake_acquire_call_count, 3);
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&f.pin), CLUSTER_WALR_RELEASE_CONFIRMED);
}

UT_TEST(test_root_publish_cohort_partial_conversion_requires_cleanup_before_retry)
{
	RootPublishCohortFixture f;
	ClusterWalRootPublishGuard *guard = NULL, *other = NULL;

	root_publish_cohort_fixture(&f, false);
	for (unsigned i = 0; i < 3; i++)
		fake_acquire_results[i] = CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK;
	fake_acquire_results[3] = CLUSTER_LOCK_ACQUIRE_NOT_AVAIL;
	fake_acquire_result_count = 4;
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_begin_set_v1(f.roots, 2, &guard),
				 CLUSTER_WAL_PIN_UNAVAILABLE);
	if (guard == NULL) {
		(void)cluster_wal_retention_pin_release(&f.pin);
		return;
	}
	UT_ASSERT_FALSE(cluster_wal_retention_root_publish_set_current_v1(guard));
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_begin_set_v1(f.roots, 2, &other),
				 CLUSTER_WAL_PIN_CAPACITY);
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&guard), CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(fake_acquire_call_count, 5); /* only the first member needs downgrade */
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_begin_set_v1(f.roots, 2, &guard),
				 CLUSTER_WAL_PIN_OK);
	UT_ASSERT(cluster_wal_retention_root_publish_set_current_v1(guard));
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&guard), CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&f.pin), CLUSTER_WALR_RELEASE_CONFIRMED);
}

UT_TEST(test_root_publish_cohort_error_cleans_partial_and_complete_conversions)
{
	for (int partial = 0; partial < 2; partial++) {
		RootPublishCohortFixture f;
		ClusterWalRootPublishGuard *guard = NULL;
		volatile bool caught = false;

		root_publish_cohort_fixture(&f, true);
		fake_acquire_error_at = partial ? 5 : 0;
		PG_TRY();
		{
			UT_ASSERT_EQ(cluster_wal_retention_root_publish_begin_set_v1(f.roots, 3, &guard),
						 CLUSTER_WAL_PIN_OK);
			if (!partial && guard != NULL)
				pg_re_throw(); /* original CF/CAS/readback ERROR */
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		fake_resource_release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false,
									   fake_resource_release_arg);
		UT_ASSERT_FALSE(cluster_wal_retention_active_pin_present());
		UT_ASSERT_EQ(fake_release_call_count, 3); /* one coordinated release per input */
		UT_ASSERT_EQ(fake_native_release_call_count, partial ? 4 : 5);
	}
}

UT_TEST(test_root_publish_cohort_uncertain_partial_downgrade_is_cleanup_only)
{
	RootPublishCohortFixture f;
	ClusterWalRootPublishGuard *guard = NULL;
	int acquired;

	root_publish_cohort_fixture(&f, false);
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_begin_set_v1(f.roots, 2, &guard),
				 CLUSTER_WAL_PIN_OK);
	if (guard == NULL) {
		(void)cluster_wal_retention_pin_release(&f.pin);
		return;
	}
	for (unsigned i = 0; i < 5; i++)
		fake_acquire_results[i] = CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK;
	fake_acquire_results[5] = CLUSTER_LOCK_ACQUIRE_NOT_AVAIL;
	fake_acquire_result_count = 6;
	capture_release_log = true;
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&guard), CLUSTER_WALR_RELEASE_UNCONFIRMED);
	UT_ASSERT_NOT_NULL(guard);
	acquired = fake_acquire_call_count;
	UT_ASSERT_EQ(fake_native_release_call_count, 2);
	UT_ASSERT_FALSE(cluster_wal_retention_root_publish_set_current_v1(guard));
	UT_ASSERT_EQ(cluster_wal_retention_root_publish_end(&guard), CLUSTER_WALR_RELEASE_UNCONFIRMED);
	UT_ASSERT_EQ(fake_acquire_call_count, acquired);
	UT_ASSERT_EQ(release_log_count, 1);
	capture_release_log = false;
	fake_resource_release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false,
								   fake_resource_release_arg);
	UT_ASSERT_FALSE(cluster_wal_retention_active_pin_present());
	UT_ASSERT_EQ(fake_release_call_count, 2);
	UT_ASSERT_EQ(fake_native_release_call_count, 5);
}

UT_TEST(test_pin_storage_distinct_checks_interior_without_changing_owner)
{
	ClusterWalRetentionInterval interval = {.thread_id = 1, .tli = 1,
		.start_lsn = TEST_WAL_SEG_SIZE, .end_lsn = TEST_WAL_SEG_SIZE * 2};
	ClusterWalRetentionPinThreadRequest request = make_pin_request(1, &interval, 1);
	ClusterWalRetentionPin *pin = NULL;
	char separate = 0;

	reset_pin_fakes();
	cluster_shared_config = true;
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(&request, 1, &pin), CLUSTER_WAL_PIN_OK);
	for (Size offset = 0; offset <= 4 * sizeof(void *); offset++)
		UT_ASSERT(!cluster_wal_retention_pin_output_distinct_v1(pin,
			(void *)((uintptr_t)pin + offset), 1));
	UT_ASSERT(!cluster_wal_retention_pin_output_distinct_v1(pin, (void *)((uintptr_t)pin - 4), 5));
	UT_ASSERT(cluster_wal_retention_pin_output_distinct_v1(pin, (void *)((uintptr_t)pin - 4), 4));
	UT_ASSERT(cluster_wal_retention_pin_output_distinct_v1(pin, &separate, sizeof(separate)));
	UT_ASSERT(!cluster_wal_retention_pin_output_distinct_v1(NULL, &separate, 1));
	UT_ASSERT(!cluster_wal_retention_pin_output_distinct_v1(pin, NULL, 1));
	UT_ASSERT(!cluster_wal_retention_pin_output_distinct_v1(pin, &separate, 0));
	UT_ASSERT(cluster_wal_retention_active_pin_present());
	UT_ASSERT_EQ(fake_release_call_count, 0);
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT(pin == NULL); UT_ASSERT_EQ(fake_release_call_count, 1);
	reset_pin_fakes();
}

UT_TEST(test_read_owner_borrows_all_original_native_grants_without_acquiring)
{
	uint16 threads[] = {1, 3};
	ClusterWalReadPinV1 *pin = NULL;
	ClusterWalReadPinOwnerV1 owner, before;
	int acquired, released;
	reset_pin_fakes(); cluster_shared_config = true;
	memset(&owner, 0xa5, sizeof(owner)); before = owner;
	UT_ASSERT(!cluster_wal_read_pin_owner_v1(NULL, &owner));
	UT_ASSERT_EQ(memcmp(&owner, &before, sizeof(owner)), 0);
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_v1(threads, 2, &pin), CLUSTER_WAL_PIN_OK);
	acquired = fake_acquire_call_count; released = fake_release_call_count;
	UT_ASSERT(cluster_wal_read_pin_owner_v1(pin, &owner));
	UT_ASSERT(owner.pin == pin && owner.generation != 0);
	UT_ASSERT(cluster_wal_read_pin_owner_current_v1(&owner));
	UT_ASSERT(cluster_wal_read_pin_owner_current_v1(&owner));
	UT_ASSERT(!cluster_wal_read_pin_covers_v1(owner.pin, 2));
	UT_ASSERT(!cluster_wal_read_pin_covers_v1(owner.pin, 0));
	UT_ASSERT(!cluster_wal_read_pin_owner_current_v1(NULL));
	UT_ASSERT_EQ(fake_acquire_call_count, acquired);
	UT_ASSERT_EQ(fake_release_call_count, released);
	UT_ASSERT(!cluster_wal_retention_active_pin_present());
	UT_ASSERT_EQ(cluster_wal_read_pin_release_v1(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT(!cluster_wal_read_pin_owner_current_v1(&owner));
	reset_pin_fakes();
}
UT_TEST(test_read_owner_never_rebinds_after_same_address_reacquisition)
{
	uint16 thread = 1;
	ClusterWalReadPinV1 *pin = NULL;
	ClusterWalReadPinOwnerV1 old, fresh;
	reset_pin_fakes(); cluster_shared_config = true;
	read_owner_reuse_storage = calloc(1, 4096);
	UT_ASSERT_NOT_NULL(read_owner_reuse_storage);
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_v1(&thread, 1, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT(cluster_wal_read_pin_owner_v1(pin, &old));
	UT_ASSERT_EQ(cluster_wal_read_pin_release_v1(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	UT_ASSERT(!cluster_wal_read_pin_owner_current_v1(&old));
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_v1(&thread, 1, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT(cluster_wal_read_pin_owner_v1(pin, &fresh));
	UT_ASSERT(fresh.pin == old.pin); /* actual native allocation is deliberately reused */
	UT_ASSERT(fresh.generation != old.generation);
	UT_ASSERT(!cluster_wal_read_pin_owner_current_v1(&old));
	UT_ASSERT(cluster_wal_read_pin_owner_current_v1(&fresh));
	UT_ASSERT_EQ(cluster_wal_read_pin_release_v1(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	free(read_owner_reuse_storage); read_owner_reuse_storage = NULL;
	reset_pin_fakes();
}
UT_TEST(test_read_owner_checks_every_member_and_preserves_original_cleanup)
{
	uint16 threads[] = {1, 3};
	ClusterWalReadPinV1 *pin = NULL;
	ClusterWalReadPinOwnerV1 owner, output, before;
	ResourceOwner saved = CurrentResourceOwner;
	reset_pin_fakes(); cluster_shared_config = true;
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_v1(threads, 2, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT(cluster_wal_read_pin_owner_v1(pin, &owner));
	CurrentResourceOwner = (ResourceOwner)(uintptr_t)2;
	UT_ASSERT(!cluster_wal_read_pin_owner_current_v1(&owner));
	CurrentResourceOwner = saved;
	MyProcPid++;
	UT_ASSERT(!cluster_wal_read_pin_owner_current_v1(&owner));
	MyProcPid--;
	fake_control_owner_stale_thread = 3;
	UT_ASSERT(!cluster_wal_read_pin_owner_current_v1(&owner));
	fake_control_owner_stale_thread = 0;
	UT_ASSERT(!cluster_wal_read_pin_owner_current_v1(&owner));
	memset(&output, 0xa5, sizeof(output)); before = output;
	UT_ASSERT(!cluster_wal_read_pin_owner_v1(pin, &output));
	UT_ASSERT_EQ(memcmp(&output, &before, sizeof(output)), 0);
	fake_release_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	UT_ASSERT_EQ(cluster_wal_read_pin_release_v1(&pin), CLUSTER_WALR_RELEASE_UNCONFIRMED);
	UT_ASSERT(pin != NULL && !cluster_wal_read_pin_owner_current_v1(&owner));
	fake_release_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT_EQ(cluster_wal_read_pin_release_v1(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	reset_pin_fakes();
}
UT_TEST(test_read_owner_refuses_entire_native_owner_storage_alias)
{
	uint16 threads[] = {1, 3};
	ClusterWalReadPinV1 *pin = NULL;
	ClusterWalReadPinOwnerV1 owner;
	reset_pin_fakes(); cluster_shared_config = true;
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_v1(threads, 2, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT(!cluster_wal_read_pin_owner_v1(pin, (ClusterWalReadPinOwnerV1 *)pin));
	UT_ASSERT(!cluster_wal_read_pin_owner_v1(pin, (ClusterWalReadPinOwnerV1 *)((char *)pin + 1)));
	UT_ASSERT(!cluster_wal_read_pin_owner_v1(pin,
		(ClusterWalReadPinOwnerV1 *)((char *)pin + read_owner_allocation_bytes - 1)));
	UT_ASSERT(cluster_wal_read_pin_owner_v1(pin, &owner));
	UT_ASSERT(cluster_wal_read_pin_owner_current_v1(&owner));
	fake_resource_release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false,
		fake_resource_release_arg);
	UT_ASSERT(!cluster_wal_read_pin_owner_current_v1(&owner));
	UT_ASSERT_EQ(fake_release_call_count, 2);
	reset_pin_fakes();
}

int
main(int argc, char **argv)
{
	setvbuf(stdout, NULL, _IONBF, 0);
	if (argc > 1 && strcmp(argv[1], "--fixture-wal") == 0)
		return write_fixture_wal_segment(argc, argv);
	if (argc != 1)
		return 2;
	UT_PLAN(95);
	UT_RUN(test_read_owner_borrows_all_original_native_grants_without_acquiring);
	UT_RUN(test_read_owner_never_rebinds_after_same_address_reacquisition);
	UT_RUN(test_read_owner_checks_every_member_and_preserves_original_cleanup);
	UT_RUN(test_read_owner_refuses_entire_native_owner_storage_alias);
	UT_RUN(test_read_release_trace_keeps_first_failed_member_and_all_debt);
	UT_RUN(test_read_release_trace_off_and_critical_preserve_original_cleanup);
	UT_RUN(test_read_release_callback_records_aggregate_and_keeps_original_fatal);
	UT_RUN(test_root_publish_singleton_still_refuses_a_second_guard);
	UT_RUN(test_root_publish_cohort_holds_every_converted_owner_until_end);
	UT_RUN(test_root_publish_cohort_keeps_closed_inputs_as_readers);
	UT_RUN(test_root_publish_cohort_refuses_incomplete_cut_and_unreleased_ir);
	UT_RUN(test_root_publish_cohort_partial_conversion_requires_cleanup_before_retry);
	UT_RUN(test_root_publish_cohort_error_cleans_partial_and_complete_conversions);
	UT_RUN(test_root_publish_cohort_uncertain_partial_downgrade_is_cleanup_only);
	UT_RUN(test_input_pin_orders_union_and_keeps_original_ir_subset);
	UT_RUN(test_input_pin_refuses_bad_or_incomplete_sets_before_io);
	UT_RUN(test_input_pin_unwinds_each_partial_union_grant);
	UT_RUN(test_input_pin_checks_extra_owners_before_bind_and_after_ir);
	UT_RUN(test_input_pin_uncertain_release_keeps_entire_cleanup_owner);
	UT_RUN(test_input_pin_error_cleanup_releases_granted_history);
	UT_RUN(test_cold_borrow_returns_original_complete_set_without_acquiring);
	UT_RUN(test_cold_borrow_never_exports_a_partial_or_stale_set);
	UT_RUN(test_cold_borrow_preserves_the_original_owner_and_closed_phase);
	UT_RUN(test_released_read_census_allows_real_e1_gc_owner_to_progress);
	UT_RUN(test_read_pin_covers_sorted_threads_without_recovery_authority);
	UT_RUN(test_read_pin_rejects_bad_shape_and_rolls_back_partial_grants);
	UT_RUN(test_read_pin_uncertain_release_is_cleanup_only);
	UT_RUN(test_read_pin_checks_every_actual_owner_and_never_revives);
	UT_RUN(test_read_pin_exact_publisher_borrow_cannot_release_or_seal_scope);
	UT_RUN(test_read_pin_cannot_overlap_other_retention_owners);
	UT_RUN(test_read_pin_resource_owner_cleans_error_and_publisher_borrow);
	UT_RUN(test_v2_e1_consumes_current_root_and_preserves_exact_floor);
	UT_RUN(test_v2_retention_zero_crc_is_not_absent_authority);
	UT_RUN(test_v2_retention_refuses_checkpoint_future_and_wrong_purpose);
	UT_RUN(test_v2_retention_revalidates_root_after_walr_x);
	UT_RUN(test_v2_reuse_selects_exact_generation_not_flat_decoy);
	UT_RUN(test_v2_reuse_terminal_history_blocks_until_consumer_closes);
	UT_RUN(test_v2_reuse_missing_native_ref_cannot_fallback_flat);
	UT_RUN(test_v2_reuse_rejects_unproven_namespace_and_claim);
	UT_RUN(test_v2_reuse_claim_change_blocks_every_physical_recheck);
	UT_RUN(test_v2_reuse_removes_only_held_generation_object);
	UT_RUN(test_layout_contracts);
	UT_RUN(test_thread_directory_exact_parse);
	UT_RUN(test_wal_basename_exact_parse);
	UT_RUN(test_wal_basename_refuses_aliases);
	UT_RUN(test_long_header_exact_identity);
	UT_RUN(test_interval_half_open_segment_bounds);
	UT_RUN(test_interval_intersection_requires_thread_and_tli);
	UT_RUN(test_walr_resource_encoding_exact);
	UT_RUN(test_walr_resource_encoding_refuses_invalid_thread);
	UT_RUN(test_reuse_guard_preflight_stamps_folds_and_builds_needset);
	UT_RUN(test_pin_one_thread_acquire_and_confirmed_release);
	UT_RUN(test_root_publisher_requires_resource_owner);
	UT_RUN(test_root_release_diagnostic_preserves_original_identity);
	UT_RUN(test_v2_pin_key_preserves_root_revalidation);
	UT_RUN(test_v2_pin_and_borrowed_publisher_refuse_rebuilding_walr);
	UT_RUN(test_v2_physical_reuse_requires_current_walr_not_held_boolean);
	UT_RUN(test_v2_upgraded_pin_requires_its_exact_current_x_owner);
	UT_RUN(test_pin_acquire_is_sorted_all_or_none);
	UT_RUN(test_pin_uncertain_rollback_remains_cleanup_only);
	UT_RUN(test_pin_bind_revalidate_and_seal_closed_fsm);
	UT_RUN(test_unbound_pin_has_pre_ir_slow_revalidation_only);
	UT_RUN(test_pin_revalidation_drift_poisoned_until_release);
	UT_RUN(test_sealed_pin_root_publish_requires_whole_root_token);
	UT_RUN(test_sealed_publisher_revalidates_only_after_exact_ir_release);
	UT_RUN(test_sealed_shared_publisher_requires_its_exact_converted_owner);
	UT_RUN(test_sealed_publish_contention_preserves_pin_and_retries_original_holder);
	UT_RUN(test_sealed_publish_error_cleanup_owns_conversion_and_pin);
	UT_RUN(test_sealed_publish_uncertain_downgrade_is_cleanup_only);
	UT_RUN(test_sealed_publisher_rechecks_formation_and_fence_without_cf);
	UT_RUN(test_sealed_pin_adopts_same_immutable_root_readback);
	UT_RUN(test_sealed_pin_rejects_immutable_root_drift);
	UT_RUN(test_pin_resource_owner_abort_releases_live_grant);
	UT_RUN(test_pin_explicit_release_prevents_owner_double_release);
	UT_RUN(test_recovery_guard_converts_same_pin_holder_and_poison_is_cleanup_only);
	UT_RUN(test_recovery_guard_l3_revalidates_bound_pin_and_serial);
	UT_RUN(test_active_recovery_preflight_borrows_exact_pin_and_blocks_retained_file);
	UT_RUN(test_correctness_action_context_preflights_e2_without_pin);
	UT_RUN(test_reuse_guard_init_and_empty_finish_exact);
	UT_RUN(test_e1_coarse_holds_x_across_strong_floor_then_releases);
	UT_RUN(test_e1_coarse_unknown_configured_root_releases_and_retains);
	UT_RUN(test_reuse_guard_rejects_dirty_init_and_stack_copy);
	UT_RUN(test_reuse_guard_zero_attempts_finish_unchanged);
	UT_RUN(test_reuse_guard_terminal_mapping_bookkeep_and_finish);
	UT_RUN(test_reuse_guard_preflight_rejects_closed_matrix_before_authority);
	UT_RUN(test_reuse_guard_fence_admission_is_na_or_full_set_and);
	UT_RUN(test_fold_unconfigured_is_unconstrained);
	UT_RUN(test_fold_configured_missing_is_unknown);
	UT_RUN(test_fold_open_and_recovery_required_are_bounded);
	UT_RUN(test_fold_terminal_lifecycles_contribute_nothing);
	UT_RUN(test_fold_refuses_invalid_root_shape);
	UT_RUN(test_fold_empty_open_interval_is_not_bounded);
	UT_RUN(test_pending_pin_protects_input_without_fake_checkpoint_interval);
	UT_RUN(test_pending_pin_refuses_stale_or_mixed_inputs_before_retaining_owner);
	UT_RUN(test_pin_storage_distinct_checks_interior_without_changing_owner);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
