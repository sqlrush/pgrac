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
#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_wal_retention.h"
#include "storage/lock.h"
#include "utils/resowner.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

#define TEST_WAL_SEG_SIZE (16 * 1024 * 1024)
#define UT_ASSERT_TRUE(condition) UT_ASSERT(condition)
#define UT_ASSERT_FALSE(condition) UT_ASSERT(!(condition))

int MyProcPid = 42;
int wal_segment_size = TEST_WAL_SEG_SIZE;
char *cluster_wal_threads_dir;
ResourceOwner CurrentResourceOwner = (ResourceOwner)(uintptr_t)0x1;

static ResourceReleaseCallback fake_resource_release_callback;
static void *fake_resource_release_arg;

static ClusterLockAcquireResult fake_acquire_results[4];
static int fake_acquire_result_count;
static int fake_acquire_call_count;
static ClusterLockAcquireRequest fake_acquire_requests[4];
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
static bool fake_preflight_root_ready;
static bool fake_extra_configured_thread;
static PgracExternalFenceNeedSetResult fake_need_build_result = PGRAC_EXTERNAL_FENCE_NEED_SET_OK;
static ClusterRecoverySerialRevalidateResult fake_serial_result = CLUSTER_RECOVERY_SERIAL_CURRENT;

void
RegisterResourceReleaseCallback(ResourceReleaseCallback callback, void *arg)
{
	fake_resource_release_callback = callback;
	fake_resource_release_arg = arg;
}

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void *
palloc0(Size size)
{
	return calloc(1, size);
}

void
pfree(void *pointer)
{
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
	return fake_s5_result;
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
cluster_control_root_read_canonical(uint16 origin_thread_id,
									const ClusterControlRootIdentity *expected_identity,
									ClusterControlRootReadMode mode,
									ClusterControlRootSnapshot *out_snapshot,
									ClusterControlRootReadToken *out_token)
{
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
	memset(fake_acquire_results, 0, sizeof(fake_acquire_results));
	memset(fake_acquire_requests, 0, sizeof(fake_acquire_requests));
	memset(fake_release_threads, 0, sizeof(fake_release_threads));
	fake_acquire_result_count = 0;
	fake_acquire_call_count = 0;
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

int
main(int argc, char **argv)
{
	if (argc > 1 && strcmp(argv[1], "--fixture-wal") == 0)
		return write_fixture_wal_segment(argc, argv);
	if (argc != 1)
		return 2;
	UT_PLAN(39);
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
	UT_RUN(test_pin_acquire_is_sorted_all_or_none);
	UT_RUN(test_pin_uncertain_rollback_remains_cleanup_only);
	UT_RUN(test_pin_bind_revalidate_and_seal_closed_fsm);
	UT_RUN(test_unbound_pin_has_pre_ir_slow_revalidation_only);
	UT_RUN(test_pin_revalidation_drift_poisoned_until_release);
	UT_RUN(test_sealed_pin_root_publish_requires_whole_root_token);
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
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
