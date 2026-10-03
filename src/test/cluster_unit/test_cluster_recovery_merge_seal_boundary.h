/*-------------------------------------------------------------------------
 *
 * test_cluster_recovery_merge_seal_boundary.h
 *	  Boundary of the merge module's cold preflight and completion tests
 *	  (test_cluster_recovery_merge_seal.c, ..._complete.c): a four-instance
 *	  cluster whose control roots, external fence, recovery plan, WAL stream
 *	  checks and completion handoff are fixtures.  The root publishers model
 *	  the failed-writer input transitions (OPEN -> RECOVERY_REQUIRED, then
 *	  the validated tail) and the recovery completion on the fixture roots;
 *	  every other backend service the module links against aborts.
 *
 *	  Included once by each test, after the product source.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_recovery_merge_seal_boundary.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_RECOVERY_MERGE_SEAL_BOUNDARY_H
#define TEST_CLUSTER_RECOVERY_MERGE_SEAL_BOUNDARY_H

#define NODES 4
#define SEAL_FLAGS                                                                                 \
	(CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID)
#define TAIL_FLAGS                                                                                 \
	(CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID       \
	 | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID)

/* ---- configuration the module reads ---- */
bool cluster_merged_recovery = true;
bool cluster_shared_config = true;
bool fullPageWrites = true;
char *cluster_wal_threads_dir = "/fixture/wal";
char *cluster_shared_data_dir = "/fixture/data";
int cluster_shared_storage_backend = CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS;
int cluster_external_fence_acquire_timeout_ms = 5000;
int cluster_cssd_heartbeat_interval_ms = 1000;
int cluster_cssd_dead_deadband_factor = 3;
int cluster_node_id = 0;
int wal_segment_size = 16 * 1024 * 1024;
int MyProcPid = 4242;
char *DataDir = "/fixture/local";
static uint32 wait_event_sink;
uint32 *my_wait_event_info = &wait_event_sink;

/* ---- fixture state ---- */
static ClusterRecoveryPlan census;
static ClusterRecoveryWorkerPool pool;
static bool pool_present;
static ClusterControlRootSnapshot roots[NODES + 1];
static ClusterControlRootReadToken tokens[NODES + 1];
static XLogRecPtr redo[NODES + 1];
static XLogRecPtr tail_end[NODES + 1];
static ClusterControlRootResult open_result[NODES + 1];
static ClusterControlRootResult tail_result[NODES + 1];
static bool fence_current[NODES + 1];
static int open_calls[NODES + 1];
static int tail_calls[NODES + 1];
static int source_calls[NODES + 1];
static int revalidate_calls[NODES + 1];
static int formations_built, formations_destroyed;
/* Faults: flags missing from a read-back root; a generation another
 * founder recovers while this one seals thread `tid`. */
static uint32 readback_cleared[NODES + 1];
static uint16 recover_on_open[NODES + 1];
static char events[2048];

/* Opaque fence objects: one byte per thread, identified by address. */
static char formation_obj[NODES + 1];
static char needs_obj[NODES + 1];
static char admissions_obj[NODES + 1];
static char pin_obj;

/* ---- ereport capture ---- */
static jmp_buf fatal_jump;
static bool fatal_armed;
static int last_elevel;
static int last_sqlstate;
static char last_message[512];
static char last_detail[1024];

static void event(const char *fmt, ...) pg_attribute_printf(1, 2);

static void
event(const char *fmt, ...)
{
	size_t used = strlen(events);
	va_list args;

	va_start(args, fmt);
	vsnprintf(events + used, sizeof(events) - used, fmt, args);
	va_end(args);
}

static void
test_ereport_begin(int elevel)
{
	last_elevel = elevel;
	if (elevel >= ERROR) {
		last_sqlstate = 0;
		last_message[0] = '\0';
		last_detail[0] = '\0';
	}
}

static void
test_ereport_end(void)
{
	if (last_elevel < ERROR)
		return;
	event("FATAL;");
	if (!fatal_armed)
		abort();
	fatal_armed = false;
	longjmp(fatal_jump, 1);
}

int
errcode(int sqlerrcode)
{
	if (last_elevel >= ERROR)
		last_sqlstate = sqlerrcode;
	return 0;
}

int
errcode_for_file_access(void)
{
	return 0;
}

int
errmsg(const char *fmt, ...)
{
	va_list args;

	if (last_elevel < ERROR)
		return 0;
	va_start(args, fmt);
	vsnprintf(last_message, sizeof(last_message), fmt, args);
	va_end(args);
	return 0;
}

int
errdetail(const char *fmt, ...)
{
	va_list args;

	if (last_elevel < ERROR)
		return 0;
	va_start(args, fmt);
	vsnprintf(last_detail, sizeof(last_detail), fmt, args);
	va_end(args);
	return 0;
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

/* ---- memory and strings ---- */
void *
palloc0(Size size)
{
	void *p = calloc(1, size);

	if (p == NULL)
		abort();
	return p;
}

void
pfree(void *pointer)
{
	free(pointer);
}

void
initStringInfo(StringInfo str)
{
	str->maxlen = 4096;
	str->data = palloc0(str->maxlen);
	str->len = 0;
	str->cursor = 0;
}

void
appendStringInfo(StringInfo str, const char *fmt, ...)
{
	va_list args;
	int n;

	va_start(args, fmt);
	n = vsnprintf(str->data + str->len, str->maxlen - str->len, fmt, args);
	va_end(args);
	if (n < 0 || n >= str->maxlen - str->len)
		abort();
	str->len += n;
}

/* ---- recovery plan, stream checks, shared root membership ---- */
bool
cluster_recovery_plan_snapshot(ClusterRecoveryPlan *out)
{
	*out = census;
	return true;
}

bool
cluster_recovery_worker_pool_snapshot(ClusterRecoveryWorkerPool *out)
{
	if (pool_present)
		*out = pool;
	return pool_present;
}

/* Like the product check: a stream without a validated tail is unreadable. */
ClusterRecoveryStreamVerdict
cluster_recovery_worker_revalidate(uint16 thread_id)
{
	UT_ASSERT(thread_id > 1 && thread_id <= NODES);
	revalidate_calls[thread_id]++;
	return cluster_recovery_worker_root_anchor_valid(&roots[thread_id])
			   ? CLUSTER_RECOVERY_STREAM_OK
			   : CLUSTER_RECOVERY_STREAM_UNREADABLE;
}

bool
cluster_shared_fs_sentinel_has_participant(int node_id)
{
	return node_id >= 0 && node_id < NODES;
}

uint16
cluster_wal_thread_id(void)
{
	return 1;
}

uint64
GetSystemIdentifier(void)
{
	return 9;
}

/* ---- control roots ---- */
ClusterRecoveryDutyCompare
cluster_recovery_duty_key_compare(const ClusterRecoveryDutyKey *expected,
								  const ClusterRecoveryDutyKey *observed)
{
	return memcmp(expected, observed, sizeof(*expected)) == 0
			   ? CLUSTER_RECOVERY_DUTY_COMPARE_EXACT
			   : CLUSTER_RECOVERY_DUTY_COMPARE_DIFFERENT;
}

ClusterControlRootResult
cluster_control_root_lookup_owner_by_node_runtime(int32 old_node_id,
												  ClusterControlRootIdentity *out_identity,
												  ClusterControlRootSnapshot *out_snapshot,
												  ClusterControlRootReadToken *out_token)
{
	UT_ASSERT(old_node_id >= 1 && old_node_id < NODES);
	*out_snapshot = roots[old_node_id + 1];
	*out_identity = out_snapshot->identity;
	*out_token = tokens[old_node_id + 1];
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_control_root_read_canonical(uint16 origin_thread_id,
									const ClusterControlRootIdentity *expected_identity,
									ClusterControlRootReadMode mode,
									ClusterControlRootSnapshot *out_snapshot,
									ClusterControlRootReadToken *out_token)
{
	UT_ASSERT(origin_thread_id > 1 && origin_thread_id <= NODES);
	UT_ASSERT_EQ(mode, CLUSTER_CONTROL_ROOT_READ_STRONG);
	UT_ASSERT_EQ(
		memcmp(expected_identity, &roots[origin_thread_id].identity, sizeof(*expected_identity)),
		0);
	event("read:%u;", (unsigned)origin_thread_id);
	*out_snapshot = roots[origin_thread_id];
	out_snapshot->root_flags &= ~readback_cleared[origin_thread_id];
	*out_token = tokens[origin_thread_id];
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

/* The source proof needs a sealed, tail-validated generation. */
ClusterControlRootResult
cluster_control_root_recovery_source_v1(const ClusterControlRootSnapshot *expected,
										const ClusterControlRootReadToken *token,
										ClusterWalSourceRef *out, XLogRecPtr *native_redo)
{
	uint16 tid = expected->identity.origin_thread_id;

	UT_ASSERT(tid > 1 && tid <= NODES);
	source_calls[tid]++;
	memset(out, 0, sizeof(*out));
	*native_redo = InvalidXLogRecPtr;
	if (expected->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
		|| (expected->root_flags & CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID) == 0
		|| memcmp(expected, &roots[tid], sizeof(*expected)) != 0
		|| memcmp(token, &tokens[tid], sizeof(*token)) != 0)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	out->claim.identity = expected->identity;
	out->timeline = expected->checkpoint_tli;
	*native_redo = redo[tid];
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static uint16
fence_thread(const void *object, const char *base)
{
	ptrdiff_t index = (const char *)object - base;

	UT_ASSERT(index > 1 && index <= NODES);
	return (uint16)index;
}

/* A publisher sees exactly the origin's admitted evidence and the root it
 * expects (compare-and-swap on the full read token). */
static uint16
seal_request_check(const ClusterRecoverySerialRequest *request)
{
	uint16 tid = request->duty.origin_thread_id;

	UT_ASSERT(tid > 1 && tid <= NODES);
	UT_ASSERT_EQ(request->mode, CLUSTER_RECOVERY_SERIAL_INPUT_SEAL);
	UT_ASSERT_EQ(memcmp(&request->duty, &roots[tid].identity, sizeof(request->duty)), 0);
	UT_ASSERT(request->formation == (void *)&formation_obj[tid]);
	UT_ASSERT(request->fence_need_set == (void *)&needs_obj[tid]);
	UT_ASSERT(request->fence_admission_set == (void *)&admissions_obj[tid]);
	UT_ASSERT(request->acquire_timeout_ms >= 1 && request->acquire_timeout_ms <= 600000);
	UT_ASSERT(request->release_timeout_ms >= 1 && request->release_timeout_ms <= 600000);
	return tid;
}

static void
root_publish(uint16 tid)
{
	roots[tid].root_publish_seq++;
	tokens[tid].root_publish_seq = roots[tid].root_publish_seq;
	tokens[tid].lifecycle = roots[tid].lifecycle;
	tokens[tid].root_flags = roots[tid].root_flags;
	tokens[tid].file_txn_seq++;
}

ClusterControlRootResult
cluster_control_root_v3_failure_open_publish(const ClusterRecoverySerialRequest *request,
											 ClusterControlRootSnapshot *out,
											 ClusterControlRootReadToken *out_token)
{
	uint16 tid = seal_request_check(request);

	open_calls[tid]++;
	event("open:%u;", (unsigned)tid);
	memset(out, 0, sizeof(*out));
	memset(out_token, 0, sizeof(*out_token));
	if (open_result[tid] != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return open_result[tid];
	if (memcmp(&request->expected_root_token, &tokens[tid], sizeof(tokens[tid])) != 0)
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	if (roots[tid].lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	if (recover_on_open[tid] != 0) {
		uint16 other = recover_on_open[tid];

		roots[other].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
		root_publish(other);
	}
	roots[tid].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	roots[tid].root_flags
		= (roots[tid].root_flags & CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF) | SEAL_FLAGS;
	roots[tid].tail_tli = 0;
	roots[tid].validated_tail_lsn_exclusive = 0;
	root_publish(tid);
	*out = roots[tid];
	*out_token = tokens[tid];
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_control_root_v3_failure_tail_publish(const ClusterRecoverySerialRequest *request,
											 ClusterControlRootSnapshot *out,
											 ClusterControlRootReadToken *out_token)
{
	uint16 tid = seal_request_check(request);

	tail_calls[tid]++;
	event("tail:%u;", (unsigned)tid);
	memset(out, 0, sizeof(*out));
	memset(out_token, 0, sizeof(*out_token));
	if (tail_result[tid] != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return tail_result[tid];
	if (memcmp(&request->expected_root_token, &tokens[tid], sizeof(tokens[tid])) != 0)
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	if (roots[tid].lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
		|| (roots[tid].root_flags & ~CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF) != SEAL_FLAGS)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	roots[tid].root_flags |= TAIL_FLAGS;
	roots[tid].tail_tli = roots[tid].checkpoint_tli;
	roots[tid].validated_tail_lsn_exclusive = tail_end[tid];
	root_publish(tid);
	*out = roots[tid];
	*out_token = tokens[tid];
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

/* ---- external write exclusion ---- */
ClusterFormationWitnessResult
cluster_formation_witness_build_wait(uint16 origin_thread, bool opening_new_duty,
									 int timeout_ms pg_attribute_unused(),
									 ClusterFormationWitnessV1 **out)
{
	UT_ASSERT(origin_thread > 1 && origin_thread <= NODES);
	UT_ASSERT(!opening_new_duty);
	formations_built++;
	event("formation:%u;", (unsigned)origin_thread);
	*out = (ClusterFormationWitnessV1 *)&formation_obj[origin_thread];
	return CLUSTER_FORMATION_WITNESS_READY;
}

void
cluster_formation_witness_destroy(ClusterFormationWitnessV1 **witness)
{
	if (*witness != NULL)
		formations_destroyed++;
	*witness = NULL;
}

ClusterFormationWitnessResult
cluster_formation_witness_revalidate_nowait(const ClusterFormationWitnessV1 *witness)
{
	return fence_current[fence_thread(witness, formation_obj)] ? CLUSTER_FORMATION_WITNESS_READY
															   : CLUSTER_FORMATION_WITNESS_UNSTABLE;
}

PgracExternalFenceNeedSetResult
cluster_external_fence_need_set_build(const ClusterRecoveryDutyKey *duty,
									  const ClusterFormationWitnessV1 *formation,
									  PgracExternalFenceNeedSetV1 **out)
{
	uint16 tid = fence_thread(formation, formation_obj);

	UT_ASSERT_EQ(duty->origin_thread_id, tid);
	event("needs:%u;", (unsigned)tid);
	*out = (PgracExternalFenceNeedSetV1 *)&needs_obj[tid];
	return PGRAC_EXTERNAL_FENCE_NEED_SET_OK;
}

void
cluster_external_fence_need_set_release(PgracExternalFenceNeedSetV1 **set)
{
	*set = NULL;
}

bool
cluster_external_fence_need_set_revalidate_nowait(const PgracExternalFenceNeedSetV1 *needs,
												  const ClusterFormationWitnessV1 *formation,
												  PgracExternalFenceDenyReason *reason)
{
	uint16 tid = fence_thread(needs, needs_obj);

	UT_ASSERT_EQ(fence_thread(formation, formation_obj), tid);
	*reason
		= fence_current[tid] ? PGRAC_EXTERNAL_FENCE_DENY_NONE : PGRAC_EXTERNAL_FENCE_DENY_TIMEOUT;
	return fence_current[tid];
}

PgracExternalFenceVerdict
cluster_external_fence_admit_set_wait(const PgracExternalFenceNeedSetV1 *needs,
									  const ClusterFormationWitnessV1 *formation,
									  int timeout_ms pg_attribute_unused(),
									  PgracExternalFenceAdmissionSetV1 **out)
{
	uint16 tid = fence_thread(needs, needs_obj);

	UT_ASSERT_EQ(fence_thread(formation, formation_obj), tid);
	event("admit:%u;", (unsigned)tid);
	*out = (PgracExternalFenceAdmissionSetV1 *)&admissions_obj[tid];
	return PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED;
}

bool
cluster_external_fence_revalidate_set_nowait(const PgracExternalFenceAdmissionSetV1 *admissions,
											 const PgracExternalFenceNeedSetV1 *needs,
											 const ClusterFormationWitnessV1 *formation,
											 PgracExternalFenceDenyReason *reason)
{
	uint16 tid = fence_thread(admissions, admissions_obj);

	UT_ASSERT_EQ(fence_thread(needs, needs_obj), tid);
	UT_ASSERT_EQ(fence_thread(formation, formation_obj), tid);
	*reason
		= fence_current[tid] ? PGRAC_EXTERNAL_FENCE_DENY_NONE : PGRAC_EXTERNAL_FENCE_DENY_TIMEOUT;
	return fence_current[tid];
}

PgracExternalFenceDenyReason
cluster_external_fence_last_deny_reason(void)
{
	return PGRAC_EXTERNAL_FENCE_DENY_NONE;
}

void
cluster_external_fence_admission_set_release(PgracExternalFenceAdmissionSetV1 **set)
{
	*set = NULL;
}

/* ---- serial set and retention pin, as held after acquisition ---- */
ClusterRecoverySerialRevalidateResult
cluster_recovery_serial_revalidate(ClusterRecoverySerialGuard *guard pg_attribute_unused())
{
	return CLUSTER_RECOVERY_SERIAL_CURRENT;
}

ClusterWalPinResult
cluster_wal_retention_pin_revalidate(ClusterWalRetentionPin *pin)
{
	UT_ASSERT(pin == (void *)&pin_obj);
	return CLUSTER_WAL_PIN_OK;
}

/* ---- the completion handoff (cold completion tests) ---- */
static ClusterThreadRecoveryAuthorityResultV1 authority_result[NODES + 1];
static bool patch_refused[NODES + 1];
static ClusterThreadRecoveryRootFinalizeResultV1 finalize_result[NODES + 1];
static ClusterWalPinResult pin_seal_result;
static ClusterRecoverySerialReleaseResult ir_release_result;
static ClusterWalrReleaseResult pin_release_result;
static bool pin_sealed;

static void
completion_reset(void)
{
	for (uint16 tid = 0; tid <= NODES; tid++) {
		authority_result[tid] = CLUSTER_THREAD_AUTHORITY_OK;
		patch_refused[tid] = false;
		finalize_result[tid] = CLUSTER_THREAD_ROOT_FINALIZE_OK;
	}
	pin_seal_result = CLUSTER_WAL_PIN_OK;
	ir_release_result = CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED;
	pin_release_result = CLUSTER_WALR_RELEASE_CONFIRMED;
	pin_sealed = false;
}

ClusterThreadRecoveryAuthorityResultV1
cluster_thread_recovery_authority_revalidate_nowait_v1(
	const ClusterThreadRecoveryAuthorityV1 *authority)
{
	return authority_result[authority->duty->origin_thread_id];
}

/* As the real builder: only under the held serial guard, only for the
 * root's whole validated cut. */
bool
cluster_thread_recovery_root_complete_patch_build_v1(
	const ClusterThreadRecoveryAuthorityV1 *authority, XLogRecPtr recovered_through,
	ClusterControlRootPatch *out_patch)
{
	uint16 tid = authority->duty->origin_thread_id;

	event("patch:%u;", (unsigned)tid);
	memset(out_patch, 0, sizeof(*out_patch));
	if (patch_refused[tid] || !authority->serial_guard->held
		|| recovered_through != authority->root_snapshot->validated_tail_lsn_exclusive)
		return false;
	out_patch->mask
		= CLUSTER_CONTROL_ROOT_PATCH_LIFECYCLE | CLUSTER_CONTROL_ROOT_PATCH_RECOVERY_PROGRESS;
	out_patch->expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	out_patch->desired.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	out_patch->desired.recovered_through_lsn_exclusive = recovered_through;
	return true;
}

ClusterWalPinResult
cluster_wal_retention_pin_seal_for_root_publish(ClusterWalRetentionPin *pin)
{
	UT_ASSERT(pin == (void *)&pin_obj);
	event("seal-pin;");
	pin_sealed = pin_seal_result == CLUSTER_WAL_PIN_OK;
	return pin_seal_result;
}

ClusterRecoverySerialReleaseResult
cluster_recovery_serial_release_set(ClusterRecoverySerialGuardSet *set)
{
	event("release-ir;");
	if (ir_release_result != CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED) {
		for (uint16 i = 0; i < set->count; i++)
			set->guards[i].release_uncertain = true;
		return ir_release_result;
	}
	memset(set->guards, 0, sizeof(set->guards));
	set->count = 0;
	return ir_release_result;
}

/* Only after confirmed IR release, through the sealed pin. */
ClusterThreadRecoveryRootFinalizeResultV1
cluster_thread_recovery_root_finalize_after_ir_v1(const ClusterThreadRecoveryAuthorityV1 *authority,
												  const ClusterControlRootPatch *patch,
												  ClusterControlRootSnapshot *out_snapshot)
{
	uint16 tid = authority->duty->origin_thread_id;
	ClusterThreadRecoveryRootFinalizeResultV1 result = finalize_result[tid];

	event("finalize:%u;", (unsigned)tid);
	UT_ASSERT(!authority->serial_guard->held && !authority->serial_guard->release_uncertain);
	UT_ASSERT(pin_sealed);
	UT_ASSERT(authority->retention_pin == (void *)&pin_obj);
	UT_ASSERT_EQ(patch->desired.recovered_through_lsn_exclusive,
				 roots[tid].validated_tail_lsn_exclusive);
	memset(out_snapshot, 0, sizeof(*out_snapshot));
	if (result == CLUSTER_THREAD_ROOT_FINALIZE_OK) {
		roots[tid].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
		root_publish(tid);
	}
	if (result == CLUSTER_THREAD_ROOT_FINALIZE_OK
		|| result == CLUSTER_THREAD_ROOT_FINALIZE_ALREADY_COMPLETE)
		*out_snapshot = roots[tid];
	return result;
}

ClusterWalrReleaseResult
cluster_wal_retention_pin_release(ClusterWalRetentionPin **pin)
{
	UT_ASSERT(*pin == (void *)&pin_obj);
	event("release-pin;");
	if (pin_release_result == CLUSTER_WALR_RELEASE_CONFIRMED)
		*pin = NULL;
	return pin_release_result;
}

/* ---- linked but never reached by these tests ---- */
#define UNREACHED() abort()

int
scn_recovery_cmp(SCN a pg_attribute_unused(), XLogRecPtr a_lsn pg_attribute_unused(),
				 NodeId a_node pg_attribute_unused(), SCN b pg_attribute_unused(),
				 XLogRecPtr b_lsn pg_attribute_unused(), NodeId b_node pg_attribute_unused())
{
	UNREACHED();
}

ClusterRecoverySerialAcquireResult
cluster_recovery_serial_acquire_set(
	const ClusterRecoverySerialRequest *requests pg_attribute_unused(),
	uint16 count pg_attribute_unused(), int overall_acquire_timeout_ms pg_attribute_unused(),
	ClusterRecoverySerialGuardSet *set pg_attribute_unused(),
	uint16 *failed_index pg_attribute_unused())
{
	UNREACHED();
}

bool
cluster_thread_recovery_pin_request_build_v1(
	uint16 dead_tid pg_attribute_unused(), const ClusterRecoveryDutyKey *duty pg_attribute_unused(),
	const ClusterControlRootSnapshot *root_snapshot pg_attribute_unused(),
	const ClusterControlRootReadToken *root_token pg_attribute_unused(),
	const ClusterFormationWitnessV1 *formation pg_attribute_unused(),
	const PgracExternalFenceNeedSetV1 *fence_need_set pg_attribute_unused(),
	const PgracExternalFenceAdmissionSetV1 *fence_admission_set pg_attribute_unused(),
	ClusterWalRetentionInterval *interval pg_attribute_unused(),
	ClusterWalRetentionPinThreadRequest *out pg_attribute_unused())
{
	UNREACHED();
}

ClusterWalPinResult
cluster_wal_retention_pin_acquire(
	const ClusterWalRetentionPinThreadRequest *requests pg_attribute_unused(),
	uint16 nthreads pg_attribute_unused(), ClusterWalRetentionPin **out_pin pg_attribute_unused())
{
	UNREACHED();
}

ClusterWalPinResult
cluster_wal_retention_pin_bind_one(ClusterWalRetentionPin *pin pg_attribute_unused(),
								   ClusterRecoverySerialGuard *held_serial pg_attribute_unused())
{
	UNREACHED();
}

ClusterWalPinResult
cluster_wal_retention_pin_bind_set(ClusterWalRetentionPin *pin pg_attribute_unused(),
								   ClusterRecoverySerialGuardSet *held_set pg_attribute_unused())
{
	UNREACHED();
}

const ClusterICPeerStateShmem *
cluster_ic_tier1_peer_get(int32 peer_id pg_attribute_unused())
{
	UNREACHED();
}

void
HandleStartupProcInterrupts(void)
{
	UNREACHED();
}

int
BasicOpenFile(const char *fileName pg_attribute_unused(), int fileFlags pg_attribute_unused())
{
	UNREACHED();
}

int
OpenTransientFile(const char *fileName pg_attribute_unused(), int fileFlags pg_attribute_unused())
{
	UNREACHED();
}

int
CloseTransientFile(int fd pg_attribute_unused())
{
	UNREACHED();
}

void
fsync_fname(const char *fname pg_attribute_unused(), bool isdir pg_attribute_unused())
{
	UNREACHED();
}

int
pg_fsync(int fd pg_attribute_unused())
{
	UNREACHED();
}

int
MakePGDirectory(const char *directoryName pg_attribute_unused())
{
	UNREACHED();
}

XLogReaderState *
XLogReaderAllocate(int wal_segment_size_arg pg_attribute_unused(),
				   const char *waldir pg_attribute_unused(),
				   XLogReaderRoutine *routine pg_attribute_unused(),
				   void *private_data pg_attribute_unused())
{
	UNREACHED();
}

void
XLogReaderFree(XLogReaderState *state pg_attribute_unused())
{
	UNREACHED();
}

void
XLogBeginRead(XLogReaderState *state pg_attribute_unused(), XLogRecPtr RecPtr pg_attribute_unused())
{
	UNREACHED();
}

XLogRecPtr
XLogFindNextRecord(XLogReaderState *state pg_attribute_unused(),
				   XLogRecPtr RecPtr pg_attribute_unused())
{
	UNREACHED();
}

XLogRecord *
XLogReadRecord(XLogReaderState *state pg_attribute_unused(), char **errormsg pg_attribute_unused())
{
	UNREACHED();
}

#endif /* TEST_CLUSTER_RECOVERY_MERGE_SEAL_BOUNDARY_H */
