/* Actual Startup consumer and original shared stamp/checkpoint owner. Native
 * binding/fence, WAL, storage and ResourceOwner services remain fixture leaves. */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"
#include <setjmp.h>
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogrecovery.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_wal_writer.h"
#include "cluster/cluster_wal_inputs.h"
#include "cluster/cluster_undo_recovery.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/storage/cluster_undo_block0.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "../../../src/backend/cluster/cluster_control_root_private.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "../../backend/cluster/cluster_recovery_plan.c"
#undef printf
#undef fprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
/* Assert-enabled libpgport formatting may call this backend boundary. */
void
ExceptionalCondition(const char *condition pg_attribute_unused(),
					 const char *file pg_attribute_unused(), int line pg_attribute_unused())
{
	abort();
}

BackendType MyBackendType;
int MyProcPid;
volatile uint32 CritSectionCount;
volatile sig_atomic_t ShutdownRequestPending;
bool cluster_enabled, cluster_shared_config;
bool IsUnderPostmaster = true;
ResourceOwner CurrentResourceOwner;
MemoryContext TopMemoryContext;
static ClusterWalStartupImage clusterStartupWriter;
static bool clusterStartupWriterSelected, clusterStartupWriterBound;
static bool clusterStartupWriterInstalled;
static ClusterWalWriterToken observed_writer;
static bool native_recovery, binding_ok, writer_ok, allocation_ok;
static bool effects_ready;
static bool xid_ready, invalidate_writer_on_xid_ready;
static unsigned scope_begins, scope_checks, scope_releases;
static unsigned fail_scope_check;
static bool scope_begin_ok;
static unsigned target_reserves, target_releases;
static bool target_reserve_ok, invalidate_writer_on_reserve;
static ClusterUndoBlock0Pin *reserved_target;
static ClusterUndoBlock0LogicalKey target_key;
static ClusterUndoBlock0AuthorityProof target_proof;
static char scope_storage;
static unsigned stamp_prepares, stamp_closes, projections;
static bool projection_ok;
static bool stamp_prepare_ok, stamp_close_ok, stamp_live, invalidate_writer_on_prepare;
static ClusterRecoveryTTAbortV1 prepared_record;
static uint64 owner_boot;
static ResourceReleaseCallback release_callback, checkpoint_release_callback;
static uint64 checkpoint_epoch;
static unsigned callback_registrations, input_destroys, allocations, frees;
static pg_on_exit_callback exit_callback;
static jmp_buf fatal_jump;
static bool fatal_armed;
static const struct ClusterRecoveryStartV1 *destroy_probe;
static ClusterRecoveryPlanShmem window_region;
static bool found_window_region;
static ClusterRecoveryStampCaptureV1 pre_cut, insert_cut, write_cut;
static unsigned wal_capacity, wal_inserts, wal_flushes, stamp_writes;
static bool stamp_write_ok, flush_error, write_error, invalidate_writer_on_capacity;
static bool invalidate_writer_in_pre, invalidate_writer_on_flush, invalidate_writer_on_write;
static uint8 wal_payload[CLUSTER_RECOVERY_TT_ABORT_BYTES];
XLogRecPtr ProcLastRecPtr;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

void pg_re_throw(void)
{
	if (PG_exception_stack == NULL) abort();
	siglongjmp(*PG_exception_stack, 1);
}
int s_lock(volatile slock_t *lock, const char *file, int line, const char *func)
{
	(void)lock; (void)file; (void)line; (void)func;
	abort(); /* A retained slot lock is illegal in this single-thread fixture. */
}
void *ShmemInitStruct(const char *name, Size size, bool *found)
{
	if (strcmp(name, "pgrac recovery plan") != 0 || size != MAXALIGN(sizeof(window_region))) abort();
	*found = found_window_region;
	found_window_region = true;
	return &window_region;
}
static void capture_pending(ClusterRecoveryStampCaptureV1 *cut)
{
	MyBackendType = B_CHECKPOINTER;
	MyProcPid = 82;
	UT_ASSERT(cluster_recovery_stamp_capture_v1(cut));
	UT_ASSERT_EQ(cut->count, 1);
	UT_ASSERT(!cut->caller_owned);
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(cut), CLUSTER_RECOVERY_STAMP_PENDING);
	MyBackendType = B_STARTUP;
	MyProcPid = 81;
}

bool RecoveryInProgress(void) { return native_recovery; }
uint64 cluster_epoch_get_current(void) { return checkpoint_epoch; }
bool cluster_wal_thread_current_v2_ref(ClusterWalSourceRef *out)
{
	memset(out, 0, sizeof(*out));
	if (!writer_ok) return false;
	*out = observed_writer.ref;
	return true;
}

uint64 cluster_qvotec_get_self_incarnation(void) { return owner_boot; }
ClusterControlRootResult
cluster_wal_writer_begin(TimeLineID timeline, ClusterWalWriterToken *out)
{
	if (window_region.thread_replay[2].stamp.phase == CLUSTER_RECOVERY_STAMP_PRE_INSERT) {
		if (!pre_cut.captured) capture_pending(&pre_cut);
		if (invalidate_writer_in_pre) {
			invalidate_writer_in_pre = false;
			observed_writer.epoch++;
		}
	}
	memset(out, 0, sizeof(*out));
	if (!writer_ok || timeline != observed_writer.ref.timeline)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	*out = observed_writer;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
bool
cluster_wal_writer_startup_matches(const ClusterControlRootIdentity *id,
								 const uint8 operation[16], XLogRecPtr first)
{
	return binding_ok && first == clusterStartupWriter.first_segment_lsn
		&& memcmp(id, &clusterStartupWriter.claim.identity, sizeof(*id)) == 0
		&& memcmp(operation, clusterStartupWriter.operation_uuid, 16) == 0;
}
void RegisterResourceReleaseCallback(ResourceReleaseCallback callback, void *arg)
{
	if (arg != NULL || checkpoint_release_callback != NULL) abort();
	if (release_callback == NULL) release_callback = callback;
	else checkpoint_release_callback = callback;
	callback_registrations++;
}
void before_shmem_exit(pg_on_exit_callback callback, Datum arg)
{
	if (arg != 0 || exit_callback != NULL) abort();
	exit_callback = callback;
}
void cancel_before_shmem_exit(pg_on_exit_callback callback, Datum arg)
{
	if (arg != 0 || exit_callback != callback) abort();
	exit_callback = NULL;
}
void *MemoryContextAllocExtended(MemoryContext context, Size size, int flags)
{
	(void)context;
	if ((flags & (MCXT_ALLOC_ZERO | MCXT_ALLOC_NO_OOM))
		!= (MCXT_ALLOC_ZERO | MCXT_ALLOC_NO_OOM)) abort();
	if (!allocation_ok) return NULL;
	allocations++;
	return calloc(1, size);
}
void pfree(void *ptr) { if (ptr != NULL) frees++; free(ptr); }
ClusterUndoBlock0Result
cluster_undo_block0_recovery_reserve(const ClusterUndoBlock0LogicalKey *logical,
	const ClusterUndoBlock0AuthorityProof *proof, ClusterUndoBlock0Pin *pin)
{
	if (reserved_target != NULL) abort();
	target_reserves++;
	target_key = *logical;
	target_proof = *proof;
	memset(pin, 0, sizeof(*pin));
	pin->slot = -1;
	if (!target_reserve_ok) return CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED;
	pin->slot = 0;
	pin->logical = *logical;
	pin->proof = *proof;
	reserved_target = pin;
	if (invalidate_writer_on_reserve) observed_writer.epoch++;
	return CLUSTER_UNDO_BLOCK0_OK;
}
void
cluster_undo_block0_release_reservation(ClusterUndoBlock0Pin *pin)
{
	if (pin->slot >= 0) {
		if (reserved_target != pin) abort();
		target_releases++;
		reserved_target = NULL;
	}
	memset(pin, 0, sizeof(*pin));
	pin->slot = -1;
}
void cluster_cold_typed_destroy_v1(ClusterColdTypedV1 **inputs)
{
	if (destroy_probe != NULL && ClusterColdRecoveryTxCurrent(destroy_probe)) abort();
	if (*inputs != NULL) input_destroys++;
	*inputs = NULL;
}
static void test_fatal(void)
{
	if (!fatal_armed) abort();
	longjmp(fatal_jump, 1);
}
#undef ereport
#define ereport(level, rest) test_fatal()

#include "test_cluster_cold_tx_writer.inc"
#include "test_cluster_cold_tx_owner.inc"
#include "test_cluster_cold_tx_xid_maximum.inc"
#include "test_cluster_cold_tx_consumer.inc"

static void actual_startup_tx_begin(EndOfWalRecoveryInfo *endOfRecoveryInfo)
{
#include "test_cluster_cold_tx_start_boundary.inc"
}

static ClusterColdTypedV1 typed;
static union {
	uint64 align;
	unsigned char bytes[offsetof(ClusterColdTopTxCensusV1, entries)
		+ 2 * sizeof(ClusterColdTopTxCensusEntryV1)];
} census_storage;
#define census (*(ClusterColdTopTxCensusV1 *)census_storage.bytes)
static EndOfWalRecoveryInfo info;

bool
cluster_cold_typed_effects_ready_v1(const ClusterColdTypedV1 *candidate)
{
	return effects_ready && candidate == &typed;
}

bool
cluster_cold_typed_xid_sources_ready_v1(const ClusterColdTypedV1 *candidate)
{
	if (invalidate_writer_on_xid_ready)
		observed_writer.epoch++;
	return xid_ready && candidate == &typed;
}

ClusterControlRootResult
cluster_wal_inputs_begin_tx_closure_v1(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdTopTxCensusEntryV1 *subject, ClusterWalInputsV1 **out)
{
	if (!ClusterColdRecoveryTxLoserCurrent(start, subject)) abort();
	scope_begins++;
	*out = (ClusterWalInputsV1 *)&scope_storage;
	return scope_begin_ok ? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}

ClusterControlRootResult
cluster_wal_inputs_revalidate_v1(ClusterWalInputsV1 *inputs)
{
	if (inputs != (ClusterWalInputsV1 *)&scope_storage) abort();
	UT_ASSERT_EQ(window_region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_EMPTY);
	scope_checks++;
	return scope_checks == fail_scope_check
		? CLUSTER_CONTROL_ROOT_STALE_TOKEN : CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

void
cluster_wal_inputs_release_v1(ClusterWalInputsV1 **inputs)
{
	if (*inputs != (ClusterWalInputsV1 *)&scope_storage) abort();
	if (reserved_target != NULL || stamp_live) abort();
	scope_releases++;
	*inputs = NULL;
}

bool
cluster_undo_smgr_recovery_abort_prepare_tx_v1(ClusterWalInputsV1 *inputs,
	const ClusterRecoveryTTAbortV1 *record, ClusterUndoSmgrRecoveryStampV1 *stamp)
{
	if (inputs != (ClusterWalInputsV1 *)&scope_storage || reserved_target == NULL
		|| scope_releases != 0 || scope_checks != 2 || stamp_live) abort();
	stamp_prepares++;
	prepared_record = *record;
	stamp->token = 0;
	if (!stamp_prepare_ok) return false;
	stamp_live = true;
	stamp->token = 91;
	if (invalidate_writer_on_prepare) observed_writer.epoch++;
	return true;
}

bool
cluster_undo_recovery_abort_project_tx_v1(ClusterWalInputsV1 *inputs,
	const ClusterRecoveryTTAbortV1 *record, ClusterUndoSmgrRecoveryStampV1 stamp)
{
	UT_ASSERT(inputs == (ClusterWalInputsV1 *)&scope_storage);
	UT_ASSERT(stamp_live && stamp.token == 91 && reserved_target != NULL);
	UT_ASSERT_EQ(scope_releases + stamp_closes + target_releases, 0);
	UT_ASSERT_EQ(wal_flushes, 1);
	UT_ASSERT_EQ(stamp_writes, 1);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(window_region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_EMPTY);
	UT_ASSERT(memcmp(record, &prepared_record, sizeof(*record)) == 0);
	projections++;
	return projection_ok;
}

bool
cluster_undo_smgr_recovery_abort_close_v1(ClusterUndoSmgrRecoveryStampV1 stamp)
{
	if (!stamp_live || stamp.token != 91 || reserved_target == NULL
		|| scope_releases != 0) abort();
	stamp_closes++;
	stamp_live = false;
	return stamp_close_ok;
}

void XLogEnsureRecordSpace(int max_block_id, int ndatas)
{
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(max_block_id, 0);
	UT_ASSERT_EQ(ndatas, 1);
	UT_ASSERT(stamp_live && reserved_target != NULL && scope_releases == 0);
	UT_ASSERT_EQ(window_region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_EMPTY);
	wal_capacity++;
	if (invalidate_writer_on_capacity) observed_writer.epoch++;
}
void XLogBeginInsert(void)
{
	UT_ASSERT_EQ(wal_capacity, 1);
	UT_ASSERT_EQ(CritSectionCount, 1);
	UT_ASSERT_EQ(scope_checks, 3);
	UT_ASSERT_EQ(window_region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_INSERTING);
	capture_pending(&insert_cut);
}
void XLogRegisterData(char *data, uint32 length)
{
	UT_ASSERT_EQ(length, sizeof(wal_payload));
	memcpy(wal_payload, data, sizeof(wal_payload));
}
XLogRecPtr XLogInsert(RmgrId rmid, uint8 record_info)
{
	ClusterRecoveryTTAbortV1 record;
	ClusterRecoveryStampSlotV1 *slot = &window_region.thread_replay[2].stamp;
	UT_ASSERT_EQ(CritSectionCount, 1);
	UT_ASSERT_EQ(rmid, RM_CLUSTER_UNDO_ID);
	UT_ASSERT_EQ(record_info, XLOG_UNDO_RECOVERY_TT_ABORT);
	UT_ASSERT_EQ(slot->phase, CLUSTER_RECOVERY_STAMP_INSERTING);
	UT_ASSERT(memcmp(slot->payload, wal_payload, sizeof(wal_payload)) == 0);
	UT_ASSERT(cluster_recovery_tt_abort_decode_v1(wal_payload, sizeof(wal_payload), &record));
	UT_ASSERT(memcmp(&record, &prepared_record, sizeof(record)) == 0);
	UT_ASSERT_EQ(slot->handle.owner.writer.epoch, observed_writer.epoch);
	UT_ASSERT_EQ(slot->handle.owner.target.identity.origin_thread_id, record.target_thread);
	UT_ASSERT_EQ(slot->handle.owner.target.identity.origin_owner_incarnation, record.target_incarnation);
	UT_ASSERT_EQ(slot->handle.owner.executor_pid, MyProcPid);
	UT_ASSERT_EQ(slot->handle.owner.executor_boot, record.writer_incarnation);
	UT_ASSERT_EQ(slot->handle.owner.recovery_serial, record.recovery_serial);
	wal_inserts++;
	ProcLastRecPtr = observed_writer.startup_first_lsn + 32;
	return ProcLastRecPtr + 200;
}
void XLogFlush(XLogRecPtr end)
{
	ClusterRecoveryStampSlotV1 *slot = &window_region.thread_replay[2].stamp;
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(wal_inserts, 1);
	UT_ASSERT_EQ(slot->phase, CLUSTER_RECOVERY_STAMP_INSERTED);
	UT_ASSERT_EQ(slot->record_start, ProcLastRecPtr);
	UT_ASSERT_EQ(slot->record_end, end);
	UT_ASSERT_EQ(stamp_writes, 0);
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&insert_cut), CLUSTER_RECOVERY_STAMP_PENDING);
	wal_flushes++;
	if (invalidate_writer_on_flush) observed_writer.epoch++;
	if (flush_error) pg_re_throw();
}
bool cluster_undo_smgr_recovery_abort_write_v1(ClusterUndoSmgrRecoveryStampV1 stamp)
{
	UT_ASSERT(stamp_live && stamp.token == 91 && reserved_target != NULL && scope_releases == 0);
	UT_ASSERT_EQ(wal_flushes, 1);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(window_region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_INSERTED);
	capture_pending(&write_cut); /* Checkpoint is still pending during target I/O. */
	stamp_writes++;
	if (invalidate_writer_on_write) observed_writer.epoch++;
	if (write_error) pg_re_throw();
	return stamp_write_ok;
}

static void reset(void)
{
	if (reserved_target != NULL || stamp_live) abort();
	/* Fault injection changes actor identity; real process exit runs as the
	 * original Startup PID, never as the deliberately forged test actor. */
	MyBackendType = B_STARTUP;
	MyProcPid = 81;
	if (exit_callback != NULL) exit_callback(0, (Datum)0);
	found_window_region = false;
	cluster_recovery_plan_shmem_init();
	memset(&pre_cut, 0, sizeof(pre_cut));
	memset(&insert_cut, 0, sizeof(insert_cut));
	memset(&write_cut, 0, sizeof(write_cut));
	wal_capacity = wal_inserts = wal_flushes = stamp_writes = 0;
	stamp_write_ok = true;
	flush_error = write_error = invalidate_writer_on_capacity = false;
	invalidate_writer_in_pre = invalidate_writer_on_flush = invalidate_writer_on_write = false;
	exit_callback = NULL;
	destroy_probe = NULL;
	memset(&typed, 0, sizeof(typed));
	memset(&census_storage, 0, sizeof(census_storage));
	memset(&info, 0, sizeof(info));
	memset(&clusterStartupWriter, 0, sizeof(clusterStartupWriter));
	memset(&observed_writer, 0, sizeof(observed_writer));
	typed.plan = (ClusterColdPlanV1 *)&typed;
	typed.top_tx_census = &census;
	census.plan = typed.plan;
	typed.participant_count = 1;
	MyBackendType = B_STARTUP;
	MyProcPid = 81;
	CurrentResourceOwner = (ResourceOwner)&info;
	CritSectionCount = ShutdownRequestPending = 0;
	cluster_enabled = cluster_shared_config = true;
	native_recovery = false;
	effects_ready = false;
	xid_ready = invalidate_writer_on_xid_ready = false;
	scope_begins = scope_checks = scope_releases = fail_scope_check = 0;
	scope_begin_ok = true;
	target_reserves = target_releases = 0;
	target_reserve_ok = true;
	invalidate_writer_on_reserve = false;
	stamp_prepares = stamp_closes = projections = 0;
	projection_ok = true;
	stamp_prepare_ok = stamp_close_ok = true;
	invalidate_writer_on_prepare = false;
	memset(&prepared_record, 0, sizeof(prepared_record));
	memset(&target_key, 0, sizeof(target_key));
	memset(&target_proof, 0, sizeof(target_proof));
	clusterStartupWriterSelected = clusterStartupWriterBound = true;
	clusterStartupWriterInstalled = binding_ok = writer_ok = allocation_ok = true;
	owner_boot = 17;
	checkpoint_epoch = 4;
	clusterStartupWriter.input_kind = CLUSTER_WAL_STARTUP_RECOVERED;
	clusterStartupWriter.generation = 7;
	clusterStartupWriter.claim.identity.system_identifier = 91;
	memset(clusterStartupWriter.claim.identity.storage_uuid, 1, 16);
	memset(clusterStartupWriter.claim.identity.authority_uuid, 2, 16);
	clusterStartupWriter.claim.identity.thread_claim_created_at = 100;
	clusterStartupWriter.claim.identity.root_lineage_seq = 1;
	clusterStartupWriter.claim.identity.origin_thread_id = 1;
	clusterStartupWriter.claim.identity.origin_owner_incarnation = owner_boot;
	clusterStartupWriter.database_incarnation = 2;
	clusterStartupWriter.config_generation = 3;
	clusterStartupWriter.formation_epoch = 4;
	clusterStartupWriter.first_segment_lsn = 0x1000000;
	clusterStartupWriter.timeline = 1;
	clusterStartupWriter.operation_uuid[0] = 19;
	observed_writer.ref.claim.identity = clusterStartupWriter.claim.identity;
	observed_writer.ref.claim.database_incarnation = clusterStartupWriter.database_incarnation;
	observed_writer.ref.claim.max_config_generation = clusterStartupWriter.config_generation;
	observed_writer.ref.claim.claim_sha256[0] = 20;
	observed_writer.ref.timeline = clusterStartupWriter.timeline;
	observed_writer.epoch = clusterStartupWriter.formation_epoch;
	observed_writer.startup_first_lsn = clusterStartupWriter.first_segment_lsn;
}
static void retain(void)
{
	ClusterColdTypedV1 *original = &typed;
	cluster_recovery_cold_inputs_retain(&original);
	UT_ASSERT(original == NULL);
	cluster_recovery_cold_inputs_handoff(&info);
}

UT_TEST(test_original_owner_binds_complete_empty_census_after_done)
{
	reset(); retain();
	UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
	UT_ASSERT(info.cluster_tx_start != NULL);
	UT_ASSERT(ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	UT_ASSERT(ClusterColdRecoveryTxInputs(info.cluster_tx_start) == &typed);
	UT_ASSERT_EQ(callback_registrations, 1);
	UT_ASSERT_EQ(census.count, 0);
	UT_ASSERT(!ClusterBeginColdRecoveryTx(&info));
	UT_ASSERT(ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	ClusterReleaseColdRecoveryInputs(&info);
	UT_ASSERT(info.cluster_tx_start == NULL && info.cluster_cold_inputs == NULL);
	UT_ASSERT(exit_callback == NULL);
}
UT_TEST(test_native_owner_rejects_uninstalled_recovery_and_writer_drift)
{
	for (int fault = 0; fault < 17; fault++) {
		ClusterWalWriterToken out;
		uint8 operation[16];
		reset(); retain();
		switch (fault) {
		case 0: clusterStartupWriterInstalled = false; break;
		case 1: native_recovery = true; break;
		case 2: clusterStartupWriterSelected = false; break;
		case 3: clusterStartupWriterBound = false; break;
		case 4: MyBackendType = B_BG_WORKER; break;
		case 5: CritSectionCount = 1; break;
		case 6: ShutdownRequestPending = 1; break;
		case 7: binding_ok = false; break;
		case 8: writer_ok = false; break;
		case 9: observed_writer.ref.claim.identity.origin_thread_id++; break;
		case 10: observed_writer.epoch++; break;
		case 11: observed_writer.ref.claim.database_incarnation++; break;
		case 12: observed_writer.startup_first_lsn++; break;
		case 13: cluster_enabled = false; break;
		case 14: cluster_shared_config = false; break;
		case 15: observed_writer.ref.timeline++; break;
		case 16: observed_writer.ref.claim.max_config_generation++; break;
		}
		memset(&out, 0xa5, sizeof(out)); memset(operation, 0xa5, sizeof(operation));
		UT_ASSERT(!ClusterStartupInstalledWriterV1(&out, operation));
		UT_ASSERT(memcmp(&out, &(ClusterWalWriterToken){0}, sizeof(out)) == 0);
		UT_ASSERT(memcmp(operation, (uint8[16]){0}, 16) == 0);
		UT_ASSERT(!ClusterBeginColdRecoveryTx(&info));
		UT_ASSERT(info.cluster_tx_start == NULL && info.cluster_cold_inputs == &typed);
	}
}
UT_TEST(test_current_rechecks_owner_process_boot_and_exact_writer)
{
	for (int fault = 0; fault < 9; fault++) {
		reset(); retain();
		UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
		switch (fault) {
		case 0: CurrentResourceOwner = (ResourceOwner)&typed; break;
		case 1: MyProcPid++; break;
		case 2: owner_boot++; break;
		case 3: clusterStartupWriter.operation_uuid[0]++; break;
		case 4: observed_writer.ref.claim.claim_sha256[1]++; break;
		case 5: observed_writer.epoch++; break;
		case 6: native_recovery = true; break;
		case 7: binding_ok = false; break;
		case 8: typed.top_tx_census = NULL; break;
		}
		UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
		UT_ASSERT(ClusterColdRecoveryTxInputs(info.cluster_tx_start) == NULL);
		UT_ASSERT(info.cluster_cold_inputs == &typed);
	}
}
UT_TEST(test_resource_error_revokes_without_completing_or_freeing_inputs)
{
	unsigned destroyed;
	reset(); retain();
	UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
	destroyed = input_destroys;
	if (release_callback != NULL) {
		release_callback(RESOURCE_RELEASE_LOCKS, false, true, NULL);
		UT_ASSERT(ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
		CurrentResourceOwner = (ResourceOwner)&typed;
		release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		CurrentResourceOwner = (ResourceOwner)&info;
		UT_ASSERT(ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
		release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	}
	UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	UT_ASSERT(ClusterColdRecoveryTxInputs(info.cluster_tx_start) == NULL);
	UT_ASSERT(info.cluster_cold_inputs == &typed && input_destroys == destroyed);
	UT_ASSERT(!ClusterBeginColdRecoveryTx(&info));
	ClusterReleaseColdRecoveryInputs(&info);
	UT_ASSERT_EQ(input_destroys, destroyed + 1);
	UT_ASSERT(info.cluster_tx_start == NULL && exit_callback == NULL);
}
UT_TEST(test_missing_census_wrong_attempt_and_oom_do_not_publish)
{
	reset(); retain();
	typed.top_tx_census = NULL;
	UT_ASSERT(!ClusterBeginColdRecoveryTx(&info));
	UT_ASSERT(info.cluster_tx_start == NULL);
	typed.top_tx_census = &census;
	allocation_ok = false;
	UT_ASSERT(!ClusterBeginColdRecoveryTx(&info));
	UT_ASSERT(info.cluster_tx_start == NULL && info.cluster_cold_inputs == &typed);
	allocation_ok = true;
	info.cluster_cold_inputs = (const ClusterColdTypedV1 *)&info;
	UT_ASSERT(!ClusterBeginColdRecoveryTx(&info));
	info.cluster_cold_inputs = &typed;
	UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
}
UT_TEST(test_no_input_startup_and_invalid_handles_do_not_acquire_resource)
{
	reset();
	UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
	UT_ASSERT(info.cluster_tx_start == NULL);
	UT_ASSERT(!ClusterBeginColdRecoveryTx(NULL));
	UT_ASSERT(!ClusterColdRecoveryTxCurrent(NULL));
	UT_ASSERT(!ClusterColdRecoveryTxCurrent((const struct ClusterRecoveryStartV1 *)&info));
	UT_ASSERT(ClusterColdRecoveryTxInputs(NULL) == NULL);
}
UT_TEST(test_original_close_rejects_another_handoff_before_releasing)
{
	EndOfWalRecoveryInfo wrong;
	volatile bool caught = false;
	unsigned destroyed;
	reset(); retain();
	UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
	memset(&wrong, 0, sizeof(wrong));
	destroyed = input_destroys;
	fatal_armed = true;
	if (setjmp(fatal_jump) == 0) ClusterReleaseColdRecoveryInputs(&wrong);
	else caught = true;
	fatal_armed = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(input_destroys, destroyed);
	UT_ASSERT(ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	ClusterReleaseColdRecoveryInputs(&info);
}
UT_TEST(test_actual_post_done_boundary_binds_or_fails_before_later_work)
{
	volatile bool caught = false;
	reset(); retain();
	actual_startup_tx_begin(&info);
	UT_ASSERT(ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	ClusterReleaseColdRecoveryInputs(&info);
	retain();
	clusterStartupWriterInstalled = false;
	fatal_armed = true;
	if (setjmp(fatal_jump) == 0) actual_startup_tx_begin(&info);
	else caught = true;
	fatal_armed = false;
	UT_ASSERT(caught);
	UT_ASSERT(info.cluster_tx_start == NULL && info.cluster_cold_inputs == &typed);
}
UT_TEST(test_process_cleanup_revokes_start_before_destroying_original_inputs)
{
	const struct ClusterRecoveryStartV1 *stale;
	unsigned destroyed;
	reset(); retain();
	UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
	stale = info.cluster_tx_start;
	destroy_probe = stale;
	destroyed = input_destroys;
	UT_ASSERT(ClusterColdRecoveryTxCurrent(stale));
	if (exit_callback != NULL) exit_callback(0, (Datum)0);
	exit_callback = NULL;
	UT_ASSERT(!ClusterColdRecoveryTxCurrent(stale));
	UT_ASSERT(ClusterColdRecoveryTxInputs(stale) == NULL);
	UT_ASSERT_EQ(input_destroys, destroyed + 1);
	UT_ASSERT(cluster_recovery_cold_inputs == NULL);
	UT_ASSERT(!ClusterBeginColdRecoveryTx(&info));
}

static void active_subject(void)
{
	ClusterColdTopTxCensusEntryV1 *entry = &census.entries[0];
	census.count = 1;
	entry->input.key.owner_instance = 1;
	entry->input.key.segment_id = 1;
	entry->input.key.segment_generation = 7;
	entry->input.key.slot_offset = 2;
	entry->input.key.xid = 100;
	entry->input.key.slot_wrap = 3;
	entry->input.slot.status = TT_SLOT_ACTIVE;
	entry->input.slot.xid = 100;
	entry->input.slot.wrap = 3;
}

UT_TEST(test_subject_requires_exact_original_census_entry_and_live_start)
{
	ClusterColdTopTxCensusEntryV1 copy;
	uint64 generation = 0, serial = 0;
	reset(); active_subject(); retain();
	UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
	/* An unproven original ACTIVE is a read-only input, never a loser grant. */
	UT_ASSERT(!census.entries[0].loser_proven);
	UT_ASSERT(ClusterColdRecoveryTxSubjectCurrent(info.cluster_tx_start,
		&census.entries[0], &generation, &serial));
	UT_ASSERT(generation != 0 && serial != 0);
	copy = census.entries[0];
	UT_ASSERT(!ClusterColdRecoveryTxSubjectCurrent(info.cluster_tx_start,
		&copy, &generation, &serial));
	UT_ASSERT(generation == 0 && serial == 0);
	/* Both opaque arguments reject invalid addresses without dereferencing. */
	UT_ASSERT(!ClusterColdRecoveryTxSubjectCurrent(info.cluster_tx_start,
		(const ClusterColdTopTxCensusEntryV1 *)(uintptr_t)1, &generation, &serial));
	UT_ASSERT(!ClusterColdRecoveryTxSubjectCurrent(
		(const struct ClusterRecoveryStartV1 *)(uintptr_t)1,
		&census.entries[0], &generation, &serial));
	UT_ASSERT(!ClusterColdRecoveryTxSubjectCurrent(info.cluster_tx_start,
		&census.entries[1], &generation, &serial));
	UT_ASSERT(!ClusterColdRecoveryTxSubjectCurrent(info.cluster_tx_start,
		NULL, &generation, &serial));
	serial = 19;
	UT_ASSERT(!ClusterColdRecoveryTxSubjectCurrent(info.cluster_tx_start,
		&census.entries[0], NULL, &serial));
	UT_ASSERT_EQ(serial, 0);
	generation = 19;
	UT_ASSERT(!ClusterColdRecoveryTxSubjectCurrent(info.cluster_tx_start,
		&census.entries[0], &generation, NULL));
	UT_ASSERT_EQ(generation, 0);
	CurrentResourceOwner = (ResourceOwner)&typed;
	UT_ASSERT(!ClusterColdRecoveryTxSubjectCurrent(info.cluster_tx_start,
		&census.entries[0], &generation, &serial));
	CurrentResourceOwner = (ResourceOwner)&info;
	UT_ASSERT(ClusterColdRecoveryTxSubjectCurrent(info.cluster_tx_start,
		&census.entries[0], &generation, &serial));
	release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	UT_ASSERT(!ClusterColdRecoveryTxSubjectCurrent(info.cluster_tx_start,
		&census.entries[0], &generation, &serial));
	UT_ASSERT(generation == 0 && serial == 0);
}

UT_TEST(test_subject_rejects_nonactive_or_conflicting_physical_identity)
{
	for (int fault = 0; fault < 12; fault++) {
		ClusterColdTopTxCensusEntryV1 *entry;
		uint64 generation = 19, serial = 20;
		reset(); active_subject(); retain();
		UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
		entry = &census.entries[0];
		switch (fault) {
		case 0: entry->input.key.owner_instance = 0; break;
		case 1: entry->input.key.owner_instance = UNDO_OWNER_INSTANCE_MAX + 1; break;
		case 2: entry->input.key.segment_id = 0; break;
		case 3: entry->input.key.owner_instance = 2; break;
		case 4: entry->input.key.segment_generation = UINT32_MAX; break;
		case 5: entry->input.key.slot_offset = TT_SLOTS_PER_SEGMENT; break;
		case 6: entry->input.key.xid = FrozenTransactionId; break;
		case 7: entry->input.key.slot_wrap = TT_WRAP_INVALID; break;
		case 8: entry->input.slot.status = TT_SLOT_UNUSED; break;
		case 9: entry->input.slot.xid++; break;
		case 10: entry->input.slot.wrap++; break;
		case 11: census.count = 0; break;
		}
		UT_ASSERT(!ClusterColdRecoveryTxSubjectCurrent(info.cluster_tx_start,
			entry, &generation, &serial));
		UT_ASSERT(generation == 0 && serial == 0);
	}
}

UT_TEST(test_loser_candidate_requires_proven_exact_physical_entry)
{
	ClusterColdTopTxCensusEntryV1 *entry;
	reset(); active_subject(); retain();
	UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
	entry = &census.entries[0];
	/* A physical ACTIVE input alone is never a loser candidate. */
	UT_ASSERT(!ClusterColdRecoveryTxLoserCurrent(info.cluster_tx_start, entry));
	entry->loser_proven = true;
	entry->loser.key = entry->input.key;
	UT_ASSERT(ClusterColdRecoveryTxLoserCurrent(info.cluster_tx_start, entry));
	entry->proof.key = entry->input.key;
	entry->proof.producer_top = true;
	entry->proof.qualified_base = true;
	entry->proof.sealed_redo = true;
	entry->proof.ordinary_effects = true;
	entry->proof.old_writer_isolated = true;
	entry->proof.terminal_match = true;
	UT_ASSERT(ClusterColdRecoveryTxTopProofCurrent(info.cluster_tx_start, entry));
	entry->proof.qualified_base = false;
	UT_ASSERT(!ClusterColdRecoveryTxTopProofCurrent(info.cluster_tx_start, entry));
	entry->proof.qualified_base = true;
	entry->proof.key.xid++;
	UT_ASSERT(!ClusterColdRecoveryTxTopProofCurrent(info.cluster_tx_start, entry));
	entry->proof.key = entry->input.key;
	/* The retained result must stay byte-for-byte tied to the physical slot. */
	entry->loser.key.xid++;
	UT_ASSERT(!ClusterColdRecoveryTxLoserCurrent(info.cluster_tx_start, entry));
	entry->loser.key = entry->input.key;
	entry->input.slot.status = TT_SLOT_UNUSED;
	UT_ASSERT(!ClusterColdRecoveryTxLoserCurrent(info.cluster_tx_start, entry));
}

UT_TEST(test_effects_accessor_requires_complete_receipt_and_live_owner)
{
	reset(); retain();
	UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
	UT_ASSERT(!ClusterColdRecoveryTxEffectsCurrent(info.cluster_tx_start));
	effects_ready = true;
	UT_ASSERT(ClusterColdRecoveryTxEffectsCurrent(info.cluster_tx_start));
	release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	UT_ASSERT(!ClusterColdRecoveryTxEffectsCurrent(info.cluster_tx_start));
}

/* Two complete endpoint inputs, with the history endpoint providing N.
 * The victim and installed writer deliberately have different identities. */
static void c0_subject(void)
{
	ClusterColdTopTxCensusEntryV1 *entry;
	reset(); active_subject();
	entry = &census.entries[0];
	entry->input.key.owner_instance = 2;
	entry->input.key.segment_id = CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	entry->input.slot.first_undo_block = uba_encode(entry->input.key.segment_id, 3, 2, 16);
	entry->loser_proven = true;
	entry->loser.key = entry->proof.key = entry->input.key;
	entry->proof.producer_top = entry->proof.qualified_base = true;
	entry->proof.sealed_redo = entry->proof.ordinary_effects = true;
	entry->proof.old_writer_isolated = entry->proof.terminal_match = true;
	typed.participant_count = 2;
	typed.replay_count = 1;
	typed.participants[0].thread_id = 2;
	typed.participants[0].owner_incarnation = 29;
	typed.sources[0].claim = observed_writer.ref.claim;
	typed.sources[0].claim.identity.origin_node_id = 1;
	typed.sources[0].claim.identity.origin_thread_id = 2;
	typed.sources[0].claim.identity.origin_owner_incarnation = 29;
	typed.sources[0].claim.claim_sha256[0] = 41;
	typed.participants[1].thread_id = 3;
	typed.participants[1].owner_incarnation = 31;
	typed.xid_receipt.next_xid[0] = (UINT64_C(7) << 32) | 110;
	typed.xid_receipt.next_xid[1] = (UINT64_C(8) << 32) | 150;
	typed.xid_next_after_redo = typed.xid_receipt.next_xid[1];
	xid_ready = effects_ready = true;
	retain();
	UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
}

UT_TEST(test_c0_candidate_uses_original_target_actual_writer_and_all_endpoints)
{
	ClusterRecoveryTTAbortDerivationV1 out;
	const ClusterRecoveryTTAbortV1 *record = &out.record;
	c0_subject();
	UT_ASSERT(ClusterColdRecoveryTxC0Candidate(info.cluster_tx_start, &census.entries[0], &out));
	UT_ASSERT_EQ(record->target_thread, 2);
	UT_ASSERT_EQ(record->target_incarnation, 29);
	UT_ASSERT_EQ(record->writer_thread, 1);
	UT_ASSERT_EQ(record->writer_incarnation, 17);
	UT_ASSERT_EQ(record->target_epoch, 4);
	UT_ASSERT_EQ(record->recovery_serial, 1);
	UT_ASSERT_EQ(record->target_claim_sha256[0], 41);
	UT_ASSERT_EQ(out.next_xid, typed.xid_receipt.next_xid[1]);
	UT_ASSERT_EQ(out.full_xid, (UINT64_C(8) << 32) | 100);
	UT_ASSERT_EQ(record->target_full_xid, out.full_xid);
	UT_ASSERT_EQ(record->top_full_xid, out.full_xid);
	UT_ASSERT(out.oldest_xid_dependency && !out.oldest_xid_checked);
	UT_ASSERT(memcmp(&record->before, &census.entries[0].input.slot, sizeof(TTSlot)) == 0);
	UT_ASSERT_EQ(cluster_recovery_tt_abort_match_v1(record, 7, &census.entries[0].input.slot),
		CLUSTER_RECOVERY_TT_ABORT_APPLY);
}

UT_TEST(test_c0_candidate_refuses_missing_conflicting_or_stale_inputs_without_reclassifying_loser)
{
	for (int fault = 0; fault < 12; fault++) {
		ClusterRecoveryTTAbortDerivationV1 out, zero;
		c0_subject();
		switch (fault) {
		case 0: xid_ready = false; break;
		case 1: typed.xid_receipt.next_xid[0] = 0; break;
		case 2: typed.xid_receipt.next_xid[1] = 2; break;
		case 3: typed.xid_next_after_redo++; break;
		case 4: typed.participants[0].thread_id = 4; break;
		case 5: typed.sources[0].claim.identity.origin_owner_incarnation++; break;
		case 6: typed.sources[0].claim.identity.origin_thread_id++; break;
		case 7: typed.replay_count = 2; typed.participants[1].thread_id = 2; break;
		case 8: effects_ready = false; break;
		case 9: census.entries[0].proof.qualified_base = false; break;
		case 10: invalidate_writer_on_xid_ready = true; break;
		case 11: observed_writer.epoch++; break;
		}
		memset(&out, 0xa5, sizeof(out));
		memset(&zero, 0, sizeof(zero));
		UT_ASSERT(!ClusterColdRecoveryTxC0Candidate(info.cluster_tx_start, &census.entries[0], &out));
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		UT_ASSERT(census.entries[0].loser_proven);
		UT_ASSERT_EQ(census.entries[0].input.slot.status, TT_SLOT_ACTIVE);
	}
}

UT_TEST(test_actual_startup_consumer_requires_numeric_candidate_under_revalidated_scope)
{
	c0_subject();
	UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	UT_ASSERT_EQ(scope_begins, 1);
	UT_ASSERT_EQ(scope_checks, 3);
	UT_ASSERT_EQ(scope_releases, 1);
	c0_subject(); xid_ready = false;
	UT_ASSERT(!cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	UT_ASSERT_EQ(scope_releases, 1);
	UT_ASSERT(census.entries[0].loser_proven);
	for (unsigned fault = 1; fault <= 2; fault++) {
		c0_subject(); fail_scope_check = fault;
		UT_ASSERT(!cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
		UT_ASSERT_EQ(scope_releases, 1);
	}
	c0_subject(); scope_begin_ok = false;
	UT_ASSERT(!cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	UT_ASSERT_EQ(scope_releases, 1);
	c0_subject(); census.entries[0].loser_proven = false; xid_ready = false;
	UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	UT_ASSERT_EQ(scope_begins, 0);
}

UT_TEST(test_actual_startup_consumer_reserves_original_target_then_releases_before_scope)
{
	c0_subject();
	UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	UT_ASSERT_EQ(target_reserves, 1);
	UT_ASSERT_EQ(target_releases, 1);
	UT_ASSERT_EQ(target_key.owner_instance, census.entries[0].input.key.owner_instance);
	UT_ASSERT_EQ(target_key.segment_id, census.entries[0].input.key.segment_id);
	UT_ASSERT_EQ(target_proof.kind, CLUSTER_UNDO_BLOCK0_STARTUP_REDO);
	UT_ASSERT_EQ(target_proof.owner_instance, 2);
	UT_ASSERT(target_proof.cluster_epoch_present);
	UT_ASSERT_EQ(target_proof.cluster_epoch, observed_writer.epoch);
	UT_ASSERT_EQ(target_proof.recovery_generation, info.cluster_tx_start->generation);
	UT_ASSERT(reserved_target == NULL);
	UT_ASSERT_EQ(scope_releases, 1);
	UT_ASSERT_EQ(census.entries[0].input.slot.status, TT_SLOT_ACTIVE);
}

UT_TEST(test_actual_startup_reservation_failure_or_stale_owner_preserves_active_and_releases)
{
	for (int fault = 0; fault < 7; fault++) {
		bool result;
		c0_subject();
		switch (fault) {
		case 0: target_reserve_ok = false; break;
		case 1: fail_scope_check = 2; break;
		case 2: invalidate_writer_on_reserve = true; break;
		case 3: xid_ready = false; break;
		case 4: census.entries[0].proof.qualified_base = false; break;
		case 5: fail_scope_check = 1; break;
		case 6: census.entries[0].loser_proven = false; break;
		}
		result = cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start);
		UT_ASSERT_EQ(result, fault == 6);
		UT_ASSERT_EQ(target_reserves, fault <= 2 ? 1 : 0);
		UT_ASSERT_EQ(target_releases, fault == 1 || fault == 2 ? 1 : 0);
		UT_ASSERT(reserved_target == NULL);
		UT_ASSERT_EQ(census.entries[0].input.slot.status, TT_SLOT_ACTIVE);
	}
}

UT_TEST(test_actual_startup_prepares_exact_fd_before_releasing_original_guards)
{
	ClusterRecoveryTTAbortDerivationV1 expected;
	c0_subject();
	UT_ASSERT(ClusterColdRecoveryTxC0Candidate(info.cluster_tx_start, &census.entries[0], &expected));
	UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	UT_ASSERT_EQ(stamp_prepares, 1);
	UT_ASSERT_EQ(stamp_closes, 1);
	UT_ASSERT(memcmp(&prepared_record, &expected.record, sizeof(prepared_record)) == 0);
	UT_ASSERT(!stamp_live && reserved_target == NULL);
	UT_ASSERT_EQ(scope_releases, 1);
	UT_ASSERT_EQ(census.entries[0].input.slot.status, TT_SLOT_ACTIVE);
}

UT_TEST(test_actual_startup_fd_or_final_close_failure_balances_original_guards)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		c0_subject();
		switch (fault) {
		case 0: stamp_prepare_ok = false; break;
		case 1: stamp_close_ok = false; break;
		case 2: fail_scope_check = 3; break;
		case 3: invalidate_writer_on_prepare = true; break;
		}
		UT_ASSERT(!cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
		UT_ASSERT_EQ(stamp_prepares, 1);
		UT_ASSERT_EQ(stamp_closes, fault == 0 ? 0 : 1);
		UT_ASSERT_EQ(target_releases, 1);
		UT_ASSERT_EQ(scope_releases, 1);
		UT_ASSERT(!stamp_live && reserved_target == NULL);
		UT_ASSERT_EQ(census.entries[0].input.slot.status, TT_SLOT_ACTIVE);
	}
}

UT_TEST(test_actual_c0_insert_flush_stamp_completes_original_checkpoint_cut)
{
	ClusterRecoveryStampSlotV1 *slot = &window_region.thread_replay[2].stamp;
	c0_subject();
	UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	UT_ASSERT_EQ(wal_capacity, 1);
	UT_ASSERT_EQ(wal_inserts, 1);
	UT_ASSERT_EQ(wal_flushes, 1);
	UT_ASSERT_EQ(stamp_writes, 1);
	UT_ASSERT(pre_cut.captured && insert_cut.captured && write_cut.captured);
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&pre_cut), CLUSTER_RECOVERY_STAMP_COMPLETE);
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&insert_cut), CLUSTER_RECOVERY_STAMP_COMPLETE);
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&write_cut), CLUSTER_RECOVERY_STAMP_COMPLETE);
	UT_ASSERT_EQ(slot->phase, CLUSTER_RECOVERY_STAMP_EMPTY);
	UT_ASSERT(!slot->failed);
	UT_ASSERT_EQ(stamp_closes, 1);
	UT_ASSERT_EQ(target_releases, 1);
	UT_ASSERT_EQ(scope_releases, 1);
	/* Retained census is input, never overwritten to manufacture completion. */
	UT_ASSERT_EQ(census.entries[0].input.slot.status, TT_SLOT_ACTIVE);
}

UT_TEST(test_before_insert_writer_drift_does_not_create_wal_or_target_mutation)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		c0_subject();
		if (fault == 0) invalidate_writer_on_capacity = true;
		else invalidate_writer_in_pre = true;
		UT_ASSERT(!cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
		UT_ASSERT_EQ(wal_capacity, 1);
		UT_ASSERT_EQ(wal_inserts, 0);
		UT_ASSERT_EQ(stamp_writes, 0);
		UT_ASSERT_EQ(window_region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_EMPTY);
		if (fault == 1) {
			UT_ASSERT(pre_cut.captured);
			UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&pre_cut), CLUSTER_RECOVERY_STAMP_COMPLETE);
		}
		UT_ASSERT_EQ(scope_releases, 1);
	}
}

UT_TEST(test_post_insert_write_or_writer_failure_retains_failed_window)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterRecoveryStampSlotV1 *slot = &window_region.thread_replay[2].stamp;
		c0_subject();
		if (fault == 0) stamp_write_ok = false;
		else if (fault == 1) invalidate_writer_on_flush = true;
		else invalidate_writer_on_write = true;
		UT_ASSERT(!cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
		UT_ASSERT_EQ(wal_inserts, 1);
		UT_ASSERT_EQ(wal_flushes, 1);
		UT_ASSERT_EQ(stamp_writes, fault == 1 ? 0 : 1);
		UT_ASSERT_EQ(slot->phase, CLUSTER_RECOVERY_STAMP_INSERTED);
		UT_ASSERT(slot->failed);
		UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&insert_cut), CLUSTER_RECOVERY_STAMP_INVALID);
		UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&slot->handle));
		UT_ASSERT(!cluster_recovery_stamp_clear_v1(&slot->handle));
		UT_ASSERT_EQ(stamp_closes, 1);
		UT_ASSERT_EQ(target_releases, 1);
		UT_ASSERT_EQ(scope_releases, 1);
	}
}

UT_TEST(test_post_insert_error_marks_debt_before_rethrow)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		volatile bool caught = false;
		ClusterRecoveryStampSlotV1 *slot = &window_region.thread_replay[2].stamp;
		c0_subject();
		if (fault == 0) flush_error = true;
		else write_error = true;
		PG_TRY();
		{
			(void)cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(slot->phase, CLUSTER_RECOVERY_STAMP_INSERTED);
		UT_ASSERT(slot->failed);
		UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&insert_cut), CLUSTER_RECOVERY_STAMP_INVALID);
		UT_ASSERT(!cluster_recovery_stamp_clear_v1(&slot->handle));
		/* Real FD/reservation ResourceOwner ERROR unwind is not exercised here;
		 * discard only these external fixture leaves before the next case. */
		stamp_live = false;
		reserved_target = NULL;
	}
}

UT_TEST(test_new_c0_projects_after_durability_before_original_guards_release)
{
	c0_subject();
	UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	UT_ASSERT_EQ(projections, 1);
	UT_ASSERT_EQ(stamp_closes, 1);
	UT_ASSERT_EQ(target_releases, 1);
	UT_ASSERT_EQ(scope_releases, 1);
}

UT_TEST(test_projection_refusal_blocks_completion_and_balances_original_guards)
{
	c0_subject();
	projection_ok = false;
	UT_ASSERT(!cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	UT_ASSERT_EQ(projections, 1);
	UT_ASSERT_EQ(stamp_closes, 1);
	UT_ASSERT_EQ(target_releases, 1);
	UT_ASSERT_EQ(scope_releases, 1);
	UT_ASSERT_EQ(window_region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_EMPTY);
}

static void checkpoint_actor(void)
{
	MyBackendType = B_CHECKPOINTER;
	MyProcPid = 82;
	CurrentResourceOwner = (ResourceOwner)&typed;
}
static void startup_actor(void)
{
	MyBackendType = B_STARTUP;
	MyProcPid = 81;
	CurrentResourceOwner = (ResourceOwner)&info;
}

UT_TEST(test_recovered_install_generation_is_original_and_not_a_clean_input)
{
	ClusterWalWriterToken writer;
	uint8 operation[16];
	uint64 generation;
	reset();
	UT_ASSERT(ClusterStartupInstalledRecoveryWriterV1(&writer, operation, &generation));
	UT_ASSERT_EQ(generation, 7);
	UT_ASSERT(memcmp(&writer, &observed_writer, sizeof(writer)) == 0);
	UT_ASSERT(memcmp(operation, clusterStartupWriter.operation_uuid, 16) == 0);
	for (int fault = 0; fault < 6; fault++) {
		reset();
		switch (fault) {
		case 0: clusterStartupWriter.input_kind = CLUSTER_WAL_STARTUP_CLEAN; break;
		case 1: clusterStartupWriter.input_kind = CLUSTER_WAL_STARTUP_INITIALIZED; break;
		case 2: clusterStartupWriter.generation = 0; break;
		case 3: clusterStartupWriterInstalled = false; break;
		case 4: native_recovery = true; break;
		case 5: checkpoint_actor(); break;
		}
		memset(&writer, 0xa5, sizeof(writer)); memset(operation, 0xa5, 16); generation = 99;
		UT_ASSERT(!ClusterStartupInstalledRecoveryWriterV1(&writer, operation, &generation));
		UT_ASSERT(memcmp(&writer, &(ClusterWalWriterToken){0}, sizeof(writer)) == 0);
		UT_ASSERT(memcmp(operation, (uint8[16]){0}, 16) == 0);
		UT_ASSERT_EQ(generation, 0);
	}
}
UT_TEST(test_original_start_publishes_only_a_checkpointer_opaque_stable_read)
{
	struct ClusterRecoveryCheckpointScopeV1 *scope = NULL, *second = NULL;
	ClusterWalWriterToken writer;
	reset(); retain();
	checkpoint_actor();
	UT_ASSERT(!cluster_recovery_checkpoint_begin_v1(&scope));
	startup_actor();
	UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
	UT_ASSERT(!cluster_recovery_checkpoint_begin_v1(&scope));
	checkpoint_actor();
	UT_ASSERT(cluster_recovery_checkpoint_begin_v1(&scope));
	UT_ASSERT(scope != NULL);
	UT_ASSERT(cluster_recovery_checkpoint_current_v1(scope, &writer));
	UT_ASSERT(memcmp(&writer, &observed_writer, sizeof(writer)) == 0);
	UT_ASSERT(!cluster_recovery_checkpoint_begin_v1(&second));
	UT_ASSERT(second == NULL);
	UT_ASSERT(!cluster_recovery_checkpoint_current_v1((void *)&typed, &writer));
	UT_ASSERT(memcmp(&writer, &(ClusterWalWriterToken){0}, sizeof(writer)) == 0);
	cluster_recovery_checkpoint_end_v1(&scope);
	UT_ASSERT(scope == NULL);
	UT_ASSERT(cluster_recovery_checkpoint_begin_v1(&scope));
	cluster_recovery_checkpoint_end_v1(&scope);
	startup_actor();
}
UT_TEST(test_checkpoint_read_rejects_current_writer_epoch_boot_role_and_resource_drift)
{
	for (int fault = 0; fault < 16; fault++) {
		struct ClusterRecoveryCheckpointScopeV1 *scope = NULL;
		ClusterWalWriterToken writer;
		reset(); retain(); UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
		checkpoint_actor(); UT_ASSERT(cluster_recovery_checkpoint_begin_v1(&scope));
		switch (fault) {
		case 0: checkpoint_epoch++; break;
		case 1: owner_boot++; break;
		case 2: observed_writer.ref.claim.claim_sha256[1]++; break;
		case 3: observed_writer.ref.claim.database_incarnation++; break;
		case 4: observed_writer.ref.timeline++; break;
		case 5: MyBackendType = B_BACKEND; break;
		case 6: MyProcPid++; break;
		case 7: CurrentResourceOwner = (ResourceOwner)&info; break;
		case 8: native_recovery = true; break;
		case 9: cluster_shared_config = false; break;
		case 10: ShutdownRequestPending = true; break;
		case 11: writer_ok = false; break;
		case 12: window_region.checkpoint_owner.generation++; break;
		case 13: window_region.checkpoint_owner.owner.operation_uuid[0]++; break;
		case 14: window_region.checkpoint_owner.owner.install_generation++; break;
		case 15: window_region.checkpoint_owner.valid = false; break;
		}
		memset(&writer, 0xa5, sizeof(writer));
		UT_ASSERT(!cluster_recovery_checkpoint_current_v1(scope, &writer));
		UT_ASSERT(memcmp(&writer, &(ClusterWalWriterToken){0}, sizeof(writer)) == 0);
		cluster_recovery_checkpoint_end_v1(&scope); startup_actor();
	}
}
UT_TEST(test_original_resource_release_and_cleanup_revoke_the_shared_read)
{
	for (int cleanup = 0; cleanup < 2; cleanup++) {
		struct ClusterRecoveryCheckpointScopeV1 *scope = NULL;
		ClusterWalWriterToken writer;
		reset(); retain(); UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
		checkpoint_actor(); UT_ASSERT(cluster_recovery_checkpoint_begin_v1(&scope));
		startup_actor();
		if (cleanup) ClusterReleaseColdRecoveryInputs(&info);
		else release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		checkpoint_actor();
		UT_ASSERT(!cluster_recovery_checkpoint_current_v1(scope, &writer));
		cluster_recovery_checkpoint_end_v1(&scope);
		UT_ASSERT(!cluster_recovery_checkpoint_begin_v1(&scope));
		startup_actor();
	}
}
UT_TEST(test_checkpointer_error_revokes_local_read_without_releasing_start)
{
	struct ClusterRecoveryCheckpointScopeV1 *scope = NULL;
	ClusterWalWriterToken writer;
	reset(); retain(); UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
	checkpoint_actor(); UT_ASSERT(cluster_recovery_checkpoint_begin_v1(&scope));
	UT_ASSERT(checkpoint_release_callback != NULL);
	if (checkpoint_release_callback != NULL)
		checkpoint_release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	UT_ASSERT(!cluster_recovery_checkpoint_current_v1(scope, &writer));
	cluster_recovery_checkpoint_end_v1(&scope);
	UT_ASSERT(cluster_recovery_checkpoint_begin_v1(&scope));
	cluster_recovery_checkpoint_end_v1(&scope); startup_actor();
	UT_ASSERT(ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
}

int main(void)
{
	UT_PLAN(31);
	UT_RUN(test_original_owner_binds_complete_empty_census_after_done);
	UT_RUN(test_native_owner_rejects_uninstalled_recovery_and_writer_drift);
	UT_RUN(test_current_rechecks_owner_process_boot_and_exact_writer);
	UT_RUN(test_resource_error_revokes_without_completing_or_freeing_inputs);
	UT_RUN(test_missing_census_wrong_attempt_and_oom_do_not_publish);
	UT_RUN(test_no_input_startup_and_invalid_handles_do_not_acquire_resource);
	UT_RUN(test_original_close_rejects_another_handoff_before_releasing);
	UT_RUN(test_actual_post_done_boundary_binds_or_fails_before_later_work);
	UT_RUN(test_process_cleanup_revokes_start_before_destroying_original_inputs);
	UT_RUN(test_subject_requires_exact_original_census_entry_and_live_start);
	UT_RUN(test_subject_rejects_nonactive_or_conflicting_physical_identity);
	UT_RUN(test_loser_candidate_requires_proven_exact_physical_entry);
	UT_RUN(test_effects_accessor_requires_complete_receipt_and_live_owner);
	UT_RUN(test_c0_candidate_uses_original_target_actual_writer_and_all_endpoints);
	UT_RUN(test_c0_candidate_refuses_missing_conflicting_or_stale_inputs_without_reclassifying_loser);
	UT_RUN(test_actual_startup_consumer_requires_numeric_candidate_under_revalidated_scope);
	UT_RUN(test_actual_startup_consumer_reserves_original_target_then_releases_before_scope);
	UT_RUN(test_actual_startup_reservation_failure_or_stale_owner_preserves_active_and_releases);
	UT_RUN(test_actual_startup_prepares_exact_fd_before_releasing_original_guards);
	UT_RUN(test_actual_startup_fd_or_final_close_failure_balances_original_guards);
	UT_RUN(test_actual_c0_insert_flush_stamp_completes_original_checkpoint_cut);
	UT_RUN(test_before_insert_writer_drift_does_not_create_wal_or_target_mutation);
	UT_RUN(test_post_insert_write_or_writer_failure_retains_failed_window);
	UT_RUN(test_post_insert_error_marks_debt_before_rethrow);
	reset();
	if (allocations != frees) { fprintf(stderr, "unreleased start resources\n"); return 1; }
	UT_RUN(test_new_c0_projects_after_durability_before_original_guards_release);
	UT_RUN(test_projection_refusal_blocks_completion_and_balances_original_guards);
	UT_RUN(test_recovered_install_generation_is_original_and_not_a_clean_input);
	UT_RUN(test_original_start_publishes_only_a_checkpointer_opaque_stable_read);
	UT_RUN(test_checkpoint_read_rejects_current_writer_epoch_boot_role_and_resource_drift);
	UT_RUN(test_original_resource_release_and_cleanup_revoke_the_shared_read);
	UT_RUN(test_checkpointer_error_revokes_local_read_without_releasing_start);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
