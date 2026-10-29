/* Actual Startup consumer and original shared stamp/checkpoint owner. Native
 * binding/fence, WAL, storage and ResourceOwner services remain fixture leaves. */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/storage/cluster_shared_fs_page.h"
#include <setjmp.h>
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogrecovery.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_ir.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_wal_writer.h"
#include "cluster/cluster_wal_inputs.h"
#include "cluster/cluster_recovery_census.h"
#include "cluster/cluster_undo_recovery.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/cluster_recovery_undo_scan.h"
#include "cluster/storage/cluster_undo_block0.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "../../../src/backend/cluster/cluster_control_root_private.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "postmaster/startup.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/wait_event.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "../../backend/cluster/cluster_recovery_plan.c"
#undef printf
#undef fprintf
#include "unit_test.h"
#include "cluster/cluster_page_install_ctrc.h"

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
static bool tx_fence_current, tx_fence_subject_current;
static unsigned tx_fence_checks;
static void (*ctrc_request_step)(void);
static bool invalidate_fence_in_pre, invalidate_fence_on_flush;
static union {
	ClusterRecoverySerialRequest alignment;
	uint8 bytes[2 * sizeof(ClusterRecoverySerialRequest)];
} tx_fence_object;
/* Original completed fence owner is a controlled leaf here; its real
 * completion/proof lifecycle is exercised by recovery_merge_complete. */
bool
cluster_recovery_merge_fence_plan_tx_current_v1(struct ClusterRecoveryFencePlan *plan,
	const ClusterWalSourceRef *source)
{
	(void)plan;
	tx_fence_checks++;
	return tx_fence_current && (source == NULL || tx_fence_subject_current);
}

bool
cluster_recovery_merge_fence_plan_tx_request_v1(struct ClusterRecoveryFencePlan *plan,
	const ClusterWalSourceRef *source, ClusterRecoverySerialRequest *out)
{
	/* This owner leaf knows the entire opaque allocation, as the real merge
	 * owner does. The Start wrapper must pass its actual output here. */
	if ((uintptr_t)out <= (uintptr_t)plan
		? (uintptr_t)plan - (uintptr_t)out < sizeof(*out)
		: (uintptr_t)out - (uintptr_t)plan < sizeof(tx_fence_object))
		return false;
	if (!cluster_recovery_merge_fence_plan_tx_current_v1(plan, source)) return false;
	memset(out, 0, sizeof(*out));
	out->mode = CLUSTER_RECOVERY_SERIAL_TX_CLOSURE;
	out->duty = source->claim.identity;
	out->formation = (const ClusterFormationWitnessV1 *)&tx_fence_object;
	out->fence_need_set = (const PgracExternalFenceNeedSetV1 *)&tx_fence_object;
	out->fence_admission_set = (const PgracExternalFenceAdmissionSetV1 *)&tx_fence_object;
	out->acquire_timeout_ms = out->release_timeout_ms = 1000;
	if (ctrc_request_step != NULL) ctrc_request_step();
	return true;
}

static bool xid_ready, invalidate_writer_on_xid_ready;
static void (*terminal_xid_step)(void);
static unsigned scope_begins, scope_checks, scope_releases;
static unsigned tx_ir_acquires, tx_ir_releases, tx_ir_waits_remaining, tx_ir_local_waits;
static unsigned tx_ir_cancel_wait;
static bool tx_ir_allowed, tx_ir_held, lose_tx_ir_pre, lose_tx_ir_flush;
ClusterControlRootResult
cluster_wal_inputs_tx_abort_acquire_v1(ClusterWalInputsV1 *inputs,
	const ClusterRecoveryTTAbortV1 *record)
{
	(void)inputs; (void)record;
	tx_ir_acquires++;
	if (tx_ir_waits_remaining > 0) {
		tx_ir_waits_remaining--;
		return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	}
	tx_ir_held = tx_ir_allowed;
	return tx_ir_allowed ? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}
bool
cluster_wal_inputs_tx_abort_authorized_v1(ClusterWalInputsV1 *inputs,
	const ClusterRecoveryTTAbortV1 *record)
{
	(void)inputs; (void)record;
	return tx_ir_held;
}
static char observation_scope_object, observation_cursor_object;
static unsigned observation_scope_begins, observation_census_begins, observation_steps,
	observation_manifests, observation_census_destroys, observation_scope_releases;
static unsigned observation_fault, observation_fault_at;
static bool observation_scope_live, observation_cursor_live;
static char observation_events[64];
static unsigned observation_event_count;
static ClusterRecoveryCensusManifest observation_manifest;
static ClusterRecoveryUndoEntry observation_physical[4];
static unsigned observation_physical_count, observation_fail_allocation;
static bool observation_release_error, observation_early_read;
static void observation_fixture_reset(void);
static void page_graph_fixture_reset(void);
static void page_prefix_fixture_reset(void);
static void page_source_fixture_reset(void);
static void observation_scope_release(void);
static bool observation_work_allocation(Size size, int flags);
int cluster_cold_recovery_plan_memory = 65536;
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
static Latch tx_wait_latch;
Latch *MyLatch = &tx_wait_latch;
static unsigned tx_waits, tx_interrupts;
static unsigned tx_wait_destroy_base;
static bool tx_wait_drift;
static void (*ctrc_wait_step)(void);
static void (*ctrc_interrupt_step)(void);
static unsigned ctrc_driver_acquires, ctrc_driver_authorizations, ctrc_driver_waits;
static unsigned ctrc_driver_busy_until, ctrc_driver_lwlocks;
static bool ctrc_driver_waiting, ctrc_driver_false_authorization, ctrc_driver_error;
static ClusterControlRootResult ctrc_driver_result;
static void (*ctrc_driver_acquire_step)(void), (*ctrc_driver_authorize_step)(void);
static void (*ctrc_driver_wait_step)(void);
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
uint64 GetSystemIdentifier(void) { return clusterStartupWriter.claim.identity.system_identifier; }
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
		if (invalidate_fence_in_pre) tx_fence_current = false;
		if (lose_tx_ir_pre) tx_ir_held = false;
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
		!= (MCXT_ALLOC_ZERO | MCXT_ALLOC_NO_OOM)
		&& !observation_work_allocation(size, flags)) abort();
	if (!allocation_ok || (observation_fail_allocation != 0
		&& allocations + 1 == observation_fail_allocation)) return NULL;
	allocations++;
	return calloc(1, size);
}
void pfree(void *ptr) { if (ptr != NULL) frees++; free(ptr); }
void *palloc0(Size size)
{
	return MemoryContextAllocExtended(TopMemoryContext, size, MCXT_ALLOC_ZERO | MCXT_ALLOC_NO_OOM);
}
volatile sig_atomic_t InterruptPending;
void ProcessInterrupts(void) { InterruptPending = false; }

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

/* Actual filesystem custody runs in merge_claim. These are its native owner
 * and retirement boundaries, controlled alongside the original writer leaves. */
static bool claim_held, claim_read_ok, claim_arbitrate_error, claim_release_error;
static uint64 claim_serial;
static unsigned claim_arbitrations, claim_releases, claim_destroys_at_release;
uint64 cluster_recovery_merge_claim_owner_v1(void);
bool cluster_recovery_merge_claim_owner_current_nowait_v1(uint64 owner);
uint64 cluster_recovery_merge_claim_owner_v1(void)
{
	return claim_held && claim_read_ok ? claim_serial : 0;
}
bool cluster_recovery_merge_claim_owner_current_nowait_v1(uint64 owner)
{
	return claim_held && owner != 0 && owner == claim_serial;
}
bool cluster_recovery_merge_claim_is_held(void) { return claim_held && claim_read_ok; }
void cluster_relmap_arbitrate_pendings(void);
void cluster_relmap_arbitrate_pendings(void)
{
	if (!claim_held) return;
	claim_arbitrations++;
	if (claim_arbitrate_error) test_fatal();
}
void cluster_recovery_merge_claim_release_if_held(void)
{
	if (!claim_held) return;
	if (!claim_read_ok || claim_release_error) test_fatal();
	claim_destroys_at_release = input_destroys;
	claim_releases++;
	claim_held = false;
}
static void actual_startup_eor_claim(EndOfWalRecoveryInfo *endOfRecoveryInfo)
{
	(void)endOfRecoveryInfo;
#include "test_cluster_cold_tx_claim_eor_boundary.inc"
}

#include "test_cluster_cold_tx_writer.inc"
#include "test_cluster_cold_tx_owner.inc"
#include "test_cluster_cold_tx_xid_maximum.inc"
#include "test_cluster_cold_tx_consumer.inc"

/* Historical semantic classification cannot suppress an independently
 * retained cold attempt bound to the actual recovered writer. */
ClusterNormalStartState cluster_semantic_normal_start_state(void)
{
	return CLUSTER_NORMAL_START_TARGET_LOADING;
}

static void actual_startup_tx_begin(EndOfWalRecoveryInfo *endOfRecoveryInfo)
{
#include "test_cluster_cold_tx_start_boundary.inc"
}

static ClusterColdTypedV1 typed;
static bool ctrc_census_ok, ctrc_census_drift;
static unsigned ctrc_census_calls;
static ClusterColdCtrcReleaseCensusV1 ctrc_census;
static bool ctrc_before_c0;
static union {
	uint64 align;
	unsigned char bytes[offsetof(ClusterColdCtrcWorkV1, entries)
		+ 48 * sizeof(ClusterColdTtSlotInputV1)];
} ctrc_work_storage;
#define native_ctrc_work (*(ClusterColdCtrcWorkV1 *)ctrc_work_storage.bytes)

/* The independent retained-native leg has its own actual TT/fold suite. */
bool
cluster_cold_typed_ctrc_work_prepare_v1(ClusterColdTypedV1 *input)
{
	if (input != &typed) abort();
	ctrc_census_calls++;
	ctrc_before_c0 = stamp_prepares == 0 && wal_inserts == 0;
	input->ctrc_work = NULL;
	if (ctrc_census_ok && input->ctrc_work_generation != UINT64_MAX) {
		memset(&ctrc_work_storage, 0, sizeof(ctrc_work_storage));
		native_ctrc_work.plan = input->plan;
		native_ctrc_work.top_tx_census = input->top_tx_census;
		native_ctrc_work.counts = ctrc_census;
		native_ctrc_work.generation = ++input->ctrc_work_generation;
		input->ctrc_work = &native_ctrc_work;
	}
	if (ctrc_census_drift) observed_writer.epoch++;
	return input->ctrc_work != NULL;
}
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
	if (terminal_xid_step != NULL) terminal_xid_step();
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
	if (*inputs == (ClusterWalInputsV1 *)&observation_scope_object) {
		observation_scope_release();
		*inputs = NULL;
		return;
	}
	if (*inputs != (ClusterWalInputsV1 *)&scope_storage) abort();
	if (reserved_target != NULL || stamp_live) abort();
	if (tx_ir_held) tx_ir_releases++;
	tx_ir_held = false;
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
	UT_ASSERT(memcmp(slot->handle.payload, wal_payload, sizeof(wal_payload)) == 0);
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
	if (invalidate_fence_on_flush) tx_fence_current = false;
	if (lose_tx_ir_flush) tx_ir_held = false;
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
	ctrc_driver_acquires = ctrc_driver_authorizations = ctrc_driver_waits = 0;
	ctrc_driver_busy_until = ctrc_driver_lwlocks = 0;
	ctrc_driver_waiting = ctrc_driver_false_authorization = ctrc_driver_error = false;
	ctrc_driver_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	ctrc_driver_acquire_step = ctrc_driver_authorize_step = ctrc_driver_wait_step = NULL;
	ctrc_census_calls = 0;
	ctrc_census_ok = true;
	ctrc_census_drift = false;
	memset(&ctrc_census, 0, sizeof(ctrc_census));
	tx_waits = tx_interrupts = 0;
	tx_wait_drift = false;
	ctrc_wait_step = NULL;
	ctrc_interrupt_step = NULL;
	if (reserved_target != NULL || stamp_live) abort();
	/* Fault injection changes actor identity; real process exit runs as the
	 * original Startup PID, never as the deliberately forged test actor. */
	MyBackendType = B_STARTUP;
	MyProcPid = 81;
	if (exit_callback != NULL) exit_callback(0, (Datum)0);
	claim_held = claim_read_ok = true;
	claim_serial = 19;
	claim_arbitrate_error = claim_release_error = false;
	claim_arbitrations = claim_releases = claim_destroys_at_release = 0;
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
	typed.tx_fence = (struct ClusterRecoveryFencePlan *)&tx_fence_object;
	typed.plan = (ClusterColdPlanV1 *)&typed;
	typed.top_tx_census = &census;
	census.plan = typed.plan;
	typed.participant_count = 1;
	MyBackendType = B_STARTUP;
	MyProcPid = 81;
	CurrentResourceOwner = (ResourceOwner)&info;
	CritSectionCount = ShutdownRequestPending = 0;
	cluster_enabled = cluster_shared_config = true;
	cluster_node_id = 0;
	native_recovery = false;
	effects_ready = false;
	tx_fence_current = tx_fence_subject_current = true;
	tx_fence_checks = 0;
	ctrc_request_step = NULL;
	invalidate_fence_in_pre = invalidate_fence_on_flush = false;
	xid_ready = invalidate_writer_on_xid_ready = false;
	terminal_xid_step = NULL;
	scope_begins = scope_checks = scope_releases = fail_scope_check = 0;
	scope_begin_ok = true;
	tx_ir_acquires = tx_ir_releases = tx_ir_waits_remaining = tx_ir_local_waits = tx_ir_cancel_wait = 0;
	tx_ir_allowed = true; tx_ir_held = lose_tx_ir_pre = lose_tx_ir_flush = false;
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
	typed.census_root_captured = true;
	typed.census_root.system_identifier = 91;
	memset(typed.census_root.image_sha256, 0x3c, sizeof(typed.census_root.image_sha256));
	memset(clusterStartupWriter.claim.identity.storage_uuid, 1, 16);
	memset(clusterStartupWriter.claim.identity.authority_uuid, 2, 16);
	clusterStartupWriter.claim.identity.thread_claim_created_at = 100;
	clusterStartupWriter.claim.identity.root_lineage_seq = 1;
	clusterStartupWriter.claim.identity.origin_node_id = cluster_node_id;
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
	observation_fixture_reset();
	page_graph_fixture_reset();
	page_prefix_fixture_reset();
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
	UT_ASSERT_EQ(window_region.checkpoint_owner.tx_result.state,
		CLUSTER_RECOVERY_TX_RESULT_COMPLETE);
	UT_ASSERT_EQ(window_region.checkpoint_owner.tx_result.subject_count, 0);
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

UT_TEST(test_c0_refuses_lost_original_isolation_before_target_or_wal)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterRecoveryTTAbortDerivationV1 candidate, zero = {0};

		c0_subject();
		if (fault == 0) tx_fence_current = false;
		else tx_fence_subject_current = false;
		memset(&candidate, 0xa5, sizeof(candidate));
		UT_ASSERT(!ClusterColdRecoveryTxC0Candidate(info.cluster_tx_start,
			&census.entries[0], &candidate));
		UT_ASSERT(memcmp(&candidate, &zero, sizeof(zero)) == 0);
		UT_ASSERT(!cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
		UT_ASSERT_EQ(target_reserves, 0);
		UT_ASSERT_EQ(stamp_prepares, 0);
		UT_ASSERT_EQ(wal_inserts, 0);
		UT_ASSERT_EQ(stamp_writes, 0);
		UT_ASSERT(census.entries[0].loser_proven);
		UT_ASSERT_EQ(census.entries[0].input.slot.status, TT_SLOT_ACTIVE);
	}
}

UT_TEST(test_c0_original_isolation_rechecked_at_pre_insert_and_post_flush)
{
	for (unsigned after_insert = 0; after_insert < 2; after_insert++) {
		c0_subject();
		invalidate_fence_in_pre = !after_insert;
		invalidate_fence_on_flush = after_insert;
		UT_ASSERT(!cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
		UT_ASSERT_EQ(stamp_prepares, 1);
		UT_ASSERT_EQ(stamp_closes, 1);
		UT_ASSERT_EQ(target_reserves, target_releases);
		UT_ASSERT_EQ(scope_releases, 1);
		UT_ASSERT_EQ(wal_inserts, after_insert);
		UT_ASSERT_EQ(wal_flushes, after_insert);
		UT_ASSERT_EQ(stamp_writes, 0);
		UT_ASSERT_EQ(projections, 0);
		if (after_insert) {
			UT_ASSERT_EQ(window_region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_INSERTED);
			UT_ASSERT(window_region.thread_replay[2].stamp.failed);
			UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&insert_cut), CLUSTER_RECOVERY_STAMP_INVALID);
			UT_ASSERT(!cluster_recovery_stamp_clear_v1(&window_region.thread_replay[2].stamp.handle));
		} else
			UT_ASSERT_EQ(window_region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_EMPTY);
	}
}

UT_TEST(test_start_binds_original_fence_pointer_and_refuses_stale_begin)
{
	struct ClusterRecoveryFencePlan *original;

	reset(); retain(); tx_fence_current = false;
	UT_ASSERT(!ClusterBeginColdRecoveryTx(&info));
	UT_ASSERT(info.cluster_tx_start == NULL);
	c0_subject();
	original = typed.tx_fence;
	typed.tx_fence = (struct ClusterRecoveryFencePlan *)&tx_fence_checks;
	UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	UT_ASSERT(!cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	UT_ASSERT_EQ(wal_inserts, 0);
	typed.tx_fence = original;
	UT_ASSERT(ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	UT_ASSERT(!cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	UT_ASSERT_EQ(wal_inserts, 0);
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
	for (int fault = 0; fault < 17; fault++) {
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
		case 16: scope->owner.writer.ref.claim.identity.system_identifier++; break;
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
		if (cleanup) cluster_recovery_cold_inputs_cleanup(1, (Datum)0);
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

static void serving_actor(void)
{
	MyBackendType = B_LMON;
	MyProcPid = 83;
	CurrentResourceOwner = (ResourceOwner)&typed;
}

UT_TEST(test_lmon_observes_only_original_recovered_install_without_a_checkpoint_scope)
{
	ClusterRecoveryCheckpointObservationV1 observation;
	struct ClusterRecoveryCheckpointScopeV1 *scope = NULL;
	reset(); retain(); serving_actor();
	UT_ASSERT(!cluster_recovery_checkpoint_observe_v1(&observation));
	UT_ASSERT(memcmp(&observation, &(ClusterRecoveryCheckpointObservationV1){0},
					 sizeof(observation)) == 0);
	startup_actor(); UT_ASSERT(ClusterBeginColdRecoveryTx(&info)); serving_actor();
	UT_ASSERT(cluster_recovery_checkpoint_observe_v1(&observation));
	UT_ASSERT_EQ(observation.slot_generation, window_region.checkpoint_owner.generation);
	UT_ASSERT(memcmp(&observation.owner, &window_region.checkpoint_owner.owner,
					 sizeof(observation.owner)) == 0);
	UT_ASSERT(!cluster_recovery_checkpoint_begin_v1(&scope));
	UT_ASSERT(scope == NULL);
	UT_ASSERT(ClusterColdRecoveryTxCurrent(info.cluster_tx_start) == false);
	startup_actor(); UT_ASSERT(ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
}

UT_TEST(test_lmon_observation_rejects_wrong_actor_or_current_input_without_leaking_a_value)
{
	for (int fault = 0; fault < 22; fault++) {
		ClusterRecoveryCheckpointObservationV1 observation;
		reset(); retain(); UT_ASSERT(ClusterBeginColdRecoveryTx(&info)); serving_actor();
		switch (fault) {
		case 0: MyBackendType = B_BACKEND; break;
		case 1: MyBackendType = B_CHECKPOINTER; break;
		case 2: MyBackendType = B_STARTUP; break;
		case 3: MyProcPid = 0; break;
		case 4: IsUnderPostmaster = false; break;
		case 5: cluster_enabled = false; break;
		case 6: cluster_shared_config = false; break;
		case 7: native_recovery = true; break;
		case 8: ShutdownRequestPending = true; break;
		case 9: CritSectionCount = 1; break;
		case 10: checkpoint_epoch++; break;
		case 11: owner_boot++; break;
		case 12: writer_ok = false; break;
		case 13: observed_writer.ref.claim.claim_sha256[0]++; break;
		case 14: observed_writer.ref.timeline++; break;
		case 15: observed_writer.ref.claim.database_incarnation++; break;
		case 16: observed_writer.ref.claim.max_config_generation++; break;
		case 17: window_region.checkpoint_owner.valid = false; break;
		case 18: observed_writer.ref.claim.identity.system_identifier++; break;
		case 19: observed_writer.ref.claim.identity.origin_node_id++; break;
		case 20: observed_writer.ref.claim.identity.origin_thread_id++; break;
		case 21: observed_writer.ref.claim.identity.origin_owner_incarnation++; break;
		}
		memset(&observation, 0xa5, sizeof(observation));
		UT_ASSERT(!cluster_recovery_checkpoint_observe_v1(&observation));
		UT_ASSERT(memcmp(&observation, &(ClusterRecoveryCheckpointObservationV1){0},
						 sizeof(observation)) == 0);
		IsUnderPostmaster = true;
		startup_actor();
	}
}

UT_TEST(test_successful_startup_release_hands_original_owner_to_lmon_and_checkpointer)
{
	ClusterRecoveryCheckpointObservationV1 before, after;
	struct ClusterRecoveryCheckpointScopeV1 *scope = NULL;
	ClusterWalWriterToken writer;
	reset(); retain(); UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
	serving_actor(); UT_ASSERT(cluster_recovery_checkpoint_observe_v1(&before));
	startup_actor(); ClusterReleaseColdRecoveryInputs(&info);
	UT_ASSERT(info.cluster_tx_start == NULL && info.cluster_cold_inputs == NULL);
	UT_ASSERT(cluster_recovery_cold_start == NULL && cluster_recovery_cold_inputs == NULL);
	serving_actor(); UT_ASSERT(cluster_recovery_checkpoint_observe_v1(&after));
	UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
	checkpoint_actor(); UT_ASSERT(cluster_recovery_checkpoint_begin_v1(&scope));
	UT_ASSERT(cluster_recovery_checkpoint_current_v1(scope, &writer));
	UT_ASSERT(memcmp(&writer, &before.owner.writer, sizeof(writer)) == 0);
	cluster_recovery_checkpoint_end_v1(&scope);
	/* The shared handoff remains bound to the current writer and boot. */
	serving_actor(); owner_boot++;
	UT_ASSERT(!cluster_recovery_checkpoint_observe_v1(&after));
	UT_ASSERT(memcmp(&after, &(ClusterRecoveryCheckpointObservationV1){0}, sizeof(after)) == 0);
	startup_actor();
}

UT_TEST(test_startup_handoff_refusal_keeps_inputs_for_original_error_cleanup)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		volatile bool caught = false;
		const struct ClusterRecoveryStartV1 *original;
		unsigned destroyed;
		reset(); retain(); UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
		original = info.cluster_tx_start;
		destroyed = input_destroys;
		if (fault == 0) owner_boot++;
		else writer_ok = false;
		fatal_armed = true;
		if (setjmp(fatal_jump) == 0) ClusterReleaseColdRecoveryInputs(&info);
		else caught = true;
		fatal_armed = false;
		UT_ASSERT(caught);
		UT_ASSERT(info.cluster_tx_start == original && info.cluster_cold_inputs == &typed);
		UT_ASSERT_EQ(input_destroys, destroyed);
		cluster_recovery_cold_inputs_cleanup(1, (Datum)0);
		UT_ASSERT_EQ(input_destroys, destroyed + 1);
	}
}

UT_TEST(test_lmon_observation_preserves_full_owner_identity_and_original_revocation)
{
	for (int fault = 0; fault < 7; fault++) {
		ClusterRecoveryCheckpointObservationV1 before, after;
		reset(); retain(); UT_ASSERT(ClusterBeginColdRecoveryTx(&info)); serving_actor();
		UT_ASSERT(cluster_recovery_checkpoint_observe_v1(&before));
		switch (fault) {
		case 0: window_region.checkpoint_owner.generation++; break;
		case 1: window_region.checkpoint_owner.owner.operation_uuid[0]++; break;
		case 2: window_region.checkpoint_owner.owner.install_generation++; break;
		case 3: window_region.checkpoint_owner.owner.start_generation++; break;
		case 4: window_region.checkpoint_owner.owner.start_serial++; break;
		case 5: window_region.checkpoint_owner.owner.owner_pid++; break;
		case 6:
			startup_actor(); release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
			serving_actor(); break;
		}
		if (fault < 6) {
			UT_ASSERT(cluster_recovery_checkpoint_observe_v1(&after));
			UT_ASSERT(memcmp(&before, &after, sizeof(before)) != 0);
		} else {
			UT_ASSERT(!cluster_recovery_checkpoint_observe_v1(&after));
			UT_ASSERT(memcmp(&after, &(ClusterRecoveryCheckpointObservationV1){0},
							 sizeof(after)) == 0);
		}
		startup_actor();
	}
}

UT_TEST(test_tx_component_records_original_c0_completion_and_survives_private_release)
{
	ClusterRecoveryTxResultValueV1 before;
	c0_subject();
	UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	before = window_region.checkpoint_owner.tx_result;
	UT_ASSERT_EQ(before.state, CLUSTER_RECOVERY_TX_RESULT_COMPLETE);
	UT_ASSERT(before.result_generation != 0);
	UT_ASSERT_EQ(before.subject_count, 1);
	UT_ASSERT_EQ(before.processed_count, 1);
	UT_ASSERT_EQ(before.completed_count, 1);
	UT_ASSERT_EQ(before.unresolved_count, 0);
	UT_ASSERT(memcmp(before.input_root_sha256, typed.census_root.image_sha256, 32) == 0);
	ClusterReleaseColdRecoveryInputs(&info);
	UT_ASSERT(memcmp(&before, &window_region.checkpoint_owner.tx_result, sizeof(before)) == 0);
	UT_ASSERT_EQ(census.entries[0].input.slot.status, TT_SLOT_ACTIVE);
}

UT_TEST(test_tx_component_retains_pending_on_original_execution_failure)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		c0_subject();
		if (fault == 0) stamp_prepare_ok = false;
		if (fault == 1) stamp_close_ok = false;
		if (fault == 2) projection_ok = false;
		UT_ASSERT(!cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
		UT_ASSERT_EQ(window_region.checkpoint_owner.tx_result.state,
			CLUSTER_RECOVERY_TX_RESULT_PENDING);
		UT_ASSERT_EQ(window_region.checkpoint_owner.tx_result.processed_count, 0);
		UT_ASSERT_EQ(window_region.checkpoint_owner.tx_result.completed_count, 0);
	}
}

UT_TEST(test_tx_component_distinguishes_complete_empty_from_unproven_active)
{
	reset(); retain(); UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
	UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	UT_ASSERT_EQ(window_region.checkpoint_owner.tx_result.state,
		CLUSTER_RECOVERY_TX_RESULT_COMPLETE);
	UT_ASSERT_EQ(window_region.checkpoint_owner.tx_result.subject_count, 0);
	c0_subject();
	census.entries[0].loser_proven = false;
	UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	UT_ASSERT_EQ(window_region.checkpoint_owner.tx_result.state,
		CLUSTER_RECOVERY_TX_RESULT_PENDING);
	UT_ASSERT_EQ(window_region.checkpoint_owner.tx_result.subject_count, 1);
	UT_ASSERT_EQ(window_region.checkpoint_owner.tx_result.processed_count, 1);
	UT_ASSERT_EQ(window_region.checkpoint_owner.tx_result.completed_count, 0);
	UT_ASSERT_EQ(window_region.checkpoint_owner.tx_result.unresolved_count, 1);
	UT_ASSERT_EQ(wal_inserts, 0);
	UT_ASSERT_EQ(census.entries[0].input.slot.status, TT_SLOT_ACTIVE);
}

UT_TEST(test_tx_result_lmon_handoff_retains_exact_value_and_revokes_on_error)
{
	for (unsigned variant = 0; variant < 3; variant++) {
		ClusterRecoveryTxResultObservationV1 before, after, zero = {0};
		if (variant == 2) {
			reset(); retain(); UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
		} else {
			c0_subject();
			if (variant == 1) census.entries[0].loser_proven = false;
		}
		serving_actor();
		UT_ASSERT(!cluster_recovery_tx_result_observe_v1(&before));
		UT_ASSERT(memcmp(&before, &zero, sizeof(before)) == 0);
		startup_actor();
		UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
		serving_actor(); UT_ASSERT(cluster_recovery_tx_result_observe_v1(&before));
		UT_ASSERT_EQ(before.result.state, variant == 1 ? CLUSTER_RECOVERY_TX_RESULT_PENDING
			: CLUSTER_RECOVERY_TX_RESULT_COMPLETE);
		UT_ASSERT(memcmp(&before.result, &window_region.checkpoint_owner.tx_result,
			sizeof(before.result)) == 0);
		startup_actor();
		if (variant == 1)
			release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		ClusterReleaseColdRecoveryInputs(&info);
		serving_actor();
		if (variant == 1) {
			UT_ASSERT(!cluster_recovery_tx_result_observe_v1(&after));
			UT_ASSERT(memcmp(&after, &zero, sizeof(after)) == 0);
			UT_ASSERT_EQ(window_region.checkpoint_owner.tx_result.state,
				CLUSTER_RECOVERY_TX_RESULT_EMPTY);
		} else {
			UT_ASSERT(cluster_recovery_tx_result_observe_v1(&after));
			UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
			owner_boot++;
			UT_ASSERT(!cluster_recovery_tx_result_observe_v1(&after));
			UT_ASSERT(memcmp(&after, &zero, sizeof(after)) == 0);
		}
		startup_actor();
	}
}

UT_TEST(test_tx_result_binds_input_root_and_cannot_rewind_ordinals)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		c0_subject();
		if (fault == 0) typed.census_root_captured = false;
		if (fault == 1) memset(typed.census_root.image_sha256, 0, 32);
		if (fault == 2) typed.census_root.system_identifier++;
		UT_ASSERT(!cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
		UT_ASSERT_EQ(wal_inserts, 0);
		UT_ASSERT_EQ(window_region.checkpoint_owner.tx_result.state,
			CLUSTER_RECOVERY_TX_RESULT_EMPTY);
	}
	{
		ClusterRecoveryTxResultValueV1 before;
		c0_subject(); census.entries[0].loser_proven = false;
		UT_ASSERT(cluster_recovery_tx_result_begin_v1(info.cluster_tx_start));
		UT_ASSERT(!cluster_recovery_tx_result_step_v1(info.cluster_tx_start, 1));
		UT_ASSERT_EQ(window_region.checkpoint_owner.tx_result.processed_count, 0);
		UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
		before = window_region.checkpoint_owner.tx_result;
		UT_ASSERT(!cluster_recovery_tx_result_step_v1(info.cluster_tx_start, 0));
		UT_ASSERT(!cluster_recovery_tx_result_finish_v1(info.cluster_tx_start));
		UT_ASSERT(!cluster_recovery_tx_result_begin_v1(info.cluster_tx_start));
		UT_ASSERT(memcmp(&before, &window_region.checkpoint_owner.tx_result, sizeof(before)) == 0);
	}
}

void
HandleStartupProcInterrupts(void)
{
	tx_interrupts++;
	if (ctrc_interrupt_step != NULL) ctrc_interrupt_step();
}

void
ResetLatch(Latch *latch)
{
	UT_ASSERT(latch == MyLatch);
}

int
WaitLatch(Latch *latch, int events, long timeout, uint32 wait_event_info)
{
	UT_ASSERT(latch == MyLatch);
	UT_ASSERT_EQ(events, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH);
	UT_ASSERT_EQ(timeout, 100);
	if (ctrc_driver_waiting) {
		UT_ASSERT_EQ(wait_event_info, WAIT_EVENT_CLUSTER_THREAD_RECOVERY);
		UT_ASSERT(observation_cursor_live && observation_scope_live);
		UT_ASSERT_EQ(ctrc_driver_lwlocks, 0);
		ctrc_driver_waits++;
		ctrc_driver_waiting = false;
		if (ctrc_driver_wait_step != NULL) ctrc_driver_wait_step();
		return WL_TIMEOUT;
	}
	if (wait_event_info == WAIT_EVENT_CLUSTER_THREAD_RECOVERY) {
		tx_ir_local_waits++;
		UT_ASSERT(reserved_target == NULL && !stamp_live && !tx_ir_held);
		UT_ASSERT_EQ(scope_checks, 1);
		if (tx_ir_cancel_wait == tx_ir_local_waits) ShutdownRequestPending = true;
		return WL_TIMEOUT;
	}
	UT_ASSERT_EQ(wait_event_info, PG_WAIT_EXTENSION);
	UT_ASSERT(info.cluster_tx_start != NULL && input_destroys == tx_wait_destroy_base);
	UT_ASSERT(ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	tx_waits++;
	if (ctrc_wait_step != NULL) {
		ctrc_wait_step();
		return WL_TIMEOUT;
	}
	if (tx_wait_drift)
		owner_boot++;
	else if (tx_waits == 3) {
		serving_actor();
		UT_ASSERT(cluster_recovery_tx_result_accept_v1());
		UT_ASSERT(cluster_recovery_ctrc_result_accept_v1());
		startup_actor();
	}
	return WL_TIMEOUT; /* Poll timeouts never become transaction errors. */
}

static void
actual_startup_finish_tx(EndOfWalRecoveryInfo *endOfRecoveryInfo)
{
#include "test_cluster_cold_tx_finish_boundary.inc"
}

UT_TEST(test_actual_startup_keeps_private_tx_input_until_lmon_accepts)
{
	c0_subject();
	tx_wait_destroy_base = input_destroys;
	UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	actual_startup_finish_tx(&info);
	UT_ASSERT_EQ(tx_waits, 3);
	UT_ASSERT(tx_interrupts >= tx_waits);
	UT_ASSERT_EQ(input_destroys, tx_wait_destroy_base + 1);
	UT_ASSERT(info.cluster_tx_start == NULL && info.cluster_cold_inputs == NULL);
	serving_actor();
	UT_ASSERT(cluster_recovery_tx_result_accept_v1());
	startup_actor();
}

UT_TEST(test_tx_handoff_refuses_pending_or_drift_without_destroying_input)
{
	UT_ASSERT(cluster_recovery_tx_result_wait_v1(NULL));
	for (unsigned drift = 0; drift < 2; drift++) {
		c0_subject();
		tx_wait_destroy_base = input_destroys;
		if (!drift)
			census.entries[0].loser_proven = false;
		UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
		tx_wait_drift = drift != 0;
		UT_ASSERT(!cluster_recovery_tx_result_wait_v1(info.cluster_tx_start));
		UT_ASSERT_EQ(tx_waits, drift ? 1 : 0);
		UT_ASSERT_EQ(input_destroys, tx_wait_destroy_base);
		UT_ASSERT(info.cluster_tx_start != NULL && info.cluster_cold_inputs != NULL);
		startup_actor();
		release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		ClusterReleaseColdRecoveryInputs(&info);
		UT_ASSERT_EQ(input_destroys, tx_wait_destroy_base + 1);
	}
}

UT_TEST(test_original_lmon_accepts_complete_tx_without_changing_its_owner_result)
{
	for (unsigned empty = 0; empty < 2; empty++) {
		ClusterRecoveryTxResultObservationV1 before, after;
		ClusterRecoveryTxResultObservationV1 unavailable, zero = { 0 };
		if (empty) {
			reset(); retain(); UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
		} else
			c0_subject();
		UT_ASSERT(!cluster_recovery_tx_result_accept_v1());
		UT_ASSERT(!cluster_recovery_tx_result_accepted_v1(info.cluster_tx_start));
		UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
		UT_ASSERT(!cluster_recovery_tx_result_accepted_v1(info.cluster_tx_start));
		serving_actor();
		UT_ASSERT(cluster_recovery_tx_result_observe_v1(&before));
		memset(&unavailable, 0xa5, sizeof(unavailable));
		UT_ASSERT(!cluster_recovery_tx_result_observe_accepted_v1(&unavailable));
		UT_ASSERT(memcmp(&unavailable, &zero, sizeof(zero)) == 0);
		UT_ASSERT(cluster_recovery_tx_result_accept_v1());
		UT_ASSERT(cluster_recovery_tx_result_accept_v1());
		UT_ASSERT(cluster_recovery_tx_result_observe_accepted_v1(&after));
		UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
		/* An old registration cannot cover a new original result generation. */
		cluster_recovery_plan_shmem->checkpoint_owner.tx_accepted_generation++;
		memset(&unavailable, 0xa5, sizeof(unavailable));
		UT_ASSERT(!cluster_recovery_tx_result_observe_accepted_v1(&unavailable));
		UT_ASSERT(memcmp(&unavailable, &zero, sizeof(zero)) == 0);
		cluster_recovery_plan_shmem->checkpoint_owner.tx_accepted_generation--;
		UT_ASSERT(cluster_recovery_tx_result_observe_accepted_v1(&after));
		startup_actor();
		memset(&unavailable, 0xa5, sizeof(unavailable));
		UT_ASSERT(!cluster_recovery_tx_result_observe_accepted_v1(&unavailable));
		UT_ASSERT(memcmp(&unavailable, &zero, sizeof(zero)) == 0);
		UT_ASSERT(cluster_recovery_tx_result_accepted_v1(info.cluster_tx_start));
		ClusterReleaseColdRecoveryInputs(&info);
		serving_actor();
		UT_ASSERT(cluster_recovery_tx_result_accept_v1());
		UT_ASSERT(cluster_recovery_tx_result_observe_accepted_v1(&after));
		UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
		startup_actor();
	}
}

UT_TEST(test_original_lmon_does_not_accept_unfinished_or_unproven_tx)
{
	for (unsigned unresolved = 0; unresolved < 2; unresolved++) {
		c0_subject();
		serving_actor(); UT_ASSERT(!cluster_recovery_tx_result_accept_v1());
		startup_actor();
		if (unresolved)
			census.entries[0].loser_proven = false;
		else
			stamp_prepare_ok = false;
		UT_ASSERT_EQ(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start),
			unresolved != 0);
		serving_actor(); UT_ASSERT(!cluster_recovery_tx_result_accept_v1());
		startup_actor();
		UT_ASSERT(!cluster_recovery_tx_result_accepted_v1(info.cluster_tx_start));
		if (!unresolved) {
			stamp_prepare_ok = true;
			/* The leaf I/O fixture counts one operation; the original start
			 * and its pending shared result remain unchanged for this retry. */
			scope_checks = scope_releases = stamp_closes = target_releases = 0;
			UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
			serving_actor(); UT_ASSERT(cluster_recovery_tx_result_accept_v1());
			startup_actor();
			UT_ASSERT(cluster_recovery_tx_result_accepted_v1(info.cluster_tx_start));
		}
		release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		UT_ASSERT(!cluster_recovery_tx_result_accepted_v1(info.cluster_tx_start));
		serving_actor(); UT_ASSERT(!cluster_recovery_tx_result_accept_v1());
		startup_actor(); ClusterReleaseColdRecoveryInputs(&info);
	}
}

UT_TEST(test_ctrc_component_is_independent_of_complete_tx_and_fresh_shmem)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterRecoveryCtrcResultObservationV1 out;
		c0_subject();
		ctrc_census.subject_count = 1;
		if (fault == 0) ctrc_census.unresolved_count = 1;
		if (fault == 1) ctrc_census.unqualified_count = 1;
		if (fault == 2) ctrc_census_ok = false;
		UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
		UT_ASSERT_EQ(window_region.checkpoint_owner.tx_result.state,
			CLUSTER_RECOVERY_TX_RESULT_COMPLETE);
		UT_ASSERT_EQ(ctrc_census_calls, 1);
		serving_actor();
		UT_ASSERT(cluster_recovery_ctrc_result_observe_v1(&out));
		UT_ASSERT_EQ(out.result.state, CLUSTER_RECOVERY_CTRC_RESULT_PENDING);
		UT_ASSERT_EQ(out.result.released_count, 0);
		startup_actor();
	}
}

UT_TEST(test_ctrc_component_publishes_only_registered_original_input)
{
	for (unsigned empty = 0; empty < 2; empty++) {
		ClusterRecoveryCtrcResultObservationV1 before, after, zero = {0};
		reset(); retain(); UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
		if (!empty) ctrc_census.subject_count = ctrc_census.released_count = 48;
		UT_ASSERT(cluster_recovery_ctrc_result_collect_v1(info.cluster_tx_start));
		serving_actor();
		UT_ASSERT(cluster_recovery_ctrc_result_observe_v1(&before));
		UT_ASSERT_EQ(before.result.state, CLUSTER_RECOVERY_CTRC_RESULT_COMPLETE);
		UT_ASSERT_EQ(before.result.subject_count, empty ? 0 : 48);
		UT_ASSERT_EQ(before.result.result_generation, 1);
		UT_ASSERT(memcmp(before.result.input_root_sha256, typed.census_root.image_sha256, 32) == 0);
		UT_ASSERT(!cluster_recovery_ctrc_result_collect_v1(info.cluster_tx_start));
		startup_actor();
		ClusterReleaseColdRecoveryInputs(&info);
		serving_actor();
		UT_ASSERT(cluster_recovery_ctrc_result_observe_v1(&after));
		UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
		owner_boot++;
		UT_ASSERT(!cluster_recovery_ctrc_result_observe_v1(&after));
		UT_ASSERT(memcmp(&after, &zero, sizeof(after)) == 0);
		startup_actor();
	}
}

UT_TEST(test_ctrc_component_rejects_original_source_and_owner_drift)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		reset(); retain(); UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
		if (fault == 0) typed.census_root_captured = false;
		if (fault == 1) memset(typed.census_root.image_sha256, 0, 32);
		if (fault == 2) typed.census_root.system_identifier++;
		if (fault == 3) census.plan = NULL;
		if (fault == 4) ctrc_census_drift = true;
		if (fault == 5) CurrentResourceOwner = NULL;
		if (fault == 6) window_region.checkpoint_owner.ctrc_result.result_generation = PG_UINT64_MAX;
		UT_ASSERT(!cluster_recovery_ctrc_result_collect_v1(info.cluster_tx_start));
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_result.state,
			CLUSTER_RECOVERY_CTRC_RESULT_EMPTY);
	}
}

UT_TEST(test_ctrc_work_is_prepared_before_actual_c0_and_keeps_pending_input)
{
	c0_subject();
	ctrc_census.subject_count = ctrc_census.unresolved_count = 1;
	UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	UT_ASSERT_EQ(ctrc_census_calls, 1);
	UT_ASSERT(ctrc_before_c0);
	UT_ASSERT(stamp_prepares != 0 && wal_inserts != 0);
	UT_ASSERT(ClusterColdRecoveryCtrcWorkCurrent(info.cluster_tx_start, &native_ctrc_work));
	UT_ASSERT_EQ(native_ctrc_work.counts.unresolved_count, 1);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_result.state,
		CLUSTER_RECOVERY_CTRC_RESULT_PENDING);
}

UT_TEST(test_ctrc_work_requires_original_pointer_and_live_startup_owner)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		const ClusterColdCtrcWorkV1 *work;
		ClusterColdCtrcWorkV1 copy;
		reset(); retain(); UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
		work = ClusterColdRecoveryCtrcWorkPrepare(info.cluster_tx_start);
		UT_ASSERT(work == &native_ctrc_work);
		UT_ASSERT(ClusterColdRecoveryCtrcWorkCurrent(info.cluster_tx_start, work));
		copy = *work;
		UT_ASSERT(!ClusterColdRecoveryCtrcWorkCurrent(info.cluster_tx_start, &copy));
		UT_ASSERT(!ClusterColdRecoveryCtrcWorkCurrent(info.cluster_tx_start,
			(const ClusterColdCtrcWorkV1 *)(uintptr_t)1));
		if (fault == 0) native_ctrc_work.plan = NULL;
		if (fault == 1) native_ctrc_work.top_tx_census = NULL;
		if (fault == 2) typed.ctrc_work = NULL;
		if (fault == 3) CurrentResourceOwner = NULL;
		if (fault == 4) owner_boot++;
		if (fault == 5) observed_writer.epoch++;
		if (fault == 6) native_recovery = true;
		if (fault == 7) release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		if (fault == 8) native_ctrc_work.generation = 0;
		if (fault == 9) typed.ctrc_work_generation++;
		UT_ASSERT(!ClusterColdRecoveryCtrcWorkCurrent(info.cluster_tx_start, work));
	}
}

UT_TEST(test_original_lmon_registers_only_the_exact_complete_ctrc_value)
{
	for (unsigned empty = 0; empty < 2; empty++) {
		ClusterRecoveryCtrcResultObservationV1 before, after, unavailable, zero = {0};
		reset(); retain(); UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
		if (!empty) ctrc_census.subject_count = ctrc_census.released_count = 48;
		UT_ASSERT(cluster_recovery_ctrc_result_collect_v1(info.cluster_tx_start));
		UT_ASSERT(!cluster_recovery_ctrc_result_accept_v1());
		serving_actor();
		UT_ASSERT(cluster_recovery_ctrc_result_observe_v1(&before));
		memset(&unavailable, 0xa5, sizeof(unavailable));
		UT_ASSERT(!cluster_recovery_ctrc_result_observe_accepted_v1(&unavailable));
		UT_ASSERT(memcmp(&unavailable, &zero, sizeof(zero)) == 0);
		UT_ASSERT(cluster_recovery_ctrc_result_accept_v1());
		UT_ASSERT(cluster_recovery_ctrc_result_accept_v1());
		UT_ASSERT(cluster_recovery_ctrc_result_observe_accepted_v1(&after));
		UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
		startup_actor();
		ClusterReleaseColdRecoveryInputs(&info);
		serving_actor();
		UT_ASSERT(cluster_recovery_ctrc_result_observe_accepted_v1(&after));
		UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
		startup_actor();
	}
}

UT_TEST(test_ctrc_registration_does_not_infer_completion_from_tx_or_an_empty_slot)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterRecoveryCtrcResultObservationV1 unavailable, zero = {0};
		c0_subject();
		serving_actor(); UT_ASSERT(!cluster_recovery_ctrc_result_accept_v1());
		startup_actor();
		ctrc_census.subject_count = 1;
		if (fault == 0) ctrc_census.unresolved_count = 1;
		if (fault == 1) ctrc_census.unqualified_count = 1;
		if (fault == 2) ctrc_census_ok = false;
		UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
		UT_ASSERT_EQ(window_region.checkpoint_owner.tx_result.state, CLUSTER_RECOVERY_TX_RESULT_COMPLETE);
		serving_actor();
		UT_ASSERT(!cluster_recovery_ctrc_result_accept_v1());
		memset(&unavailable, 0xa5, sizeof(unavailable));
		UT_ASSERT(!cluster_recovery_ctrc_result_observe_accepted_v1(&unavailable));
		UT_ASSERT(memcmp(&unavailable, &zero, sizeof(zero)) == 0);
		startup_actor();
	}
}

UT_TEST(test_new_original_ctrc_generation_invalidates_its_previous_registration)
{
	ClusterRecoveryCtrcResultObservationV1 before, after, zero = {0};
	reset(); retain(); UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
	UT_ASSERT(cluster_recovery_ctrc_result_collect_v1(info.cluster_tx_start));
	serving_actor(); UT_ASSERT(cluster_recovery_ctrc_result_accept_v1());
	UT_ASSERT(cluster_recovery_ctrc_result_observe_accepted_v1(&before));
	startup_actor();
	UT_ASSERT(cluster_recovery_ctrc_result_collect_v1(info.cluster_tx_start));
	serving_actor();
	UT_ASSERT(!cluster_recovery_ctrc_result_observe_accepted_v1(&after));
	UT_ASSERT(memcmp(&after, &zero, sizeof(zero)) == 0);
	UT_ASSERT(cluster_recovery_ctrc_result_accept_v1());
	UT_ASSERT(cluster_recovery_ctrc_result_observe_accepted_v1(&after));
	UT_ASSERT_EQ(after.result.result_generation, before.result.result_generation + 1);
	UT_ASSERT(memcmp(&before.checkpoint, &after.checkpoint, sizeof(before.checkpoint)) == 0);
	startup_actor();
	ctrc_census.subject_count = ctrc_census.unresolved_count = 1;
	UT_ASSERT(cluster_recovery_ctrc_result_collect_v1(info.cluster_tx_start));
	serving_actor();
	UT_ASSERT(!cluster_recovery_ctrc_result_accept_v1());
	UT_ASSERT(!cluster_recovery_ctrc_result_observe_accepted_v1(&after));
	UT_ASSERT(memcmp(&after, &zero, sizeof(zero)) == 0);
	startup_actor();
}

UT_TEST(test_ctrc_registration_rechecks_original_actor_boot_epoch_and_slot)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		ClusterRecoveryCtrcResultObservationV1 after, zero = {0};
		reset(); retain(); UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
		UT_ASSERT(cluster_recovery_ctrc_result_collect_v1(info.cluster_tx_start));
		serving_actor(); UT_ASSERT(cluster_recovery_ctrc_result_accept_v1());
		if (fault == 0) startup_actor();
		if (fault == 1) owner_boot++;
		if (fault == 2) checkpoint_epoch++;
		if (fault == 3) window_region.checkpoint_owner.valid = false;
		if (fault == 4) window_region.checkpoint_owner.ctrc_accepted_generation++;
		if (fault == 5) observed_writer.ref.claim.max_config_generation++;
		if (fault == 6) ShutdownRequestPending = true;
		memset(&after, 0xa5, sizeof(after));
		UT_ASSERT(!cluster_recovery_ctrc_result_observe_accepted_v1(&after));
		UT_ASSERT(memcmp(&after, &zero, sizeof(zero)) == 0);
		if (fault != 4) UT_ASSERT(!cluster_recovery_ctrc_result_accept_v1());
		startup_actor();
	}
}

UT_TEST(test_original_error_revokes_ctrc_registration_with_the_owner_slot)
{
	ClusterRecoveryCtrcResultObservationV1 after, zero = {0};
	reset(); retain(); UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
	UT_ASSERT(cluster_recovery_ctrc_result_collect_v1(info.cluster_tx_start));
	serving_actor(); UT_ASSERT(cluster_recovery_ctrc_result_accept_v1());
	startup_actor();
	release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	serving_actor();
	UT_ASSERT(!cluster_recovery_ctrc_result_accept_v1());
	UT_ASSERT(!cluster_recovery_ctrc_result_observe_accepted_v1(&after));
	UT_ASSERT(memcmp(&after, &zero, sizeof(zero)) == 0);
	startup_actor(); ClusterReleaseColdRecoveryInputs(&info);
}

#include "test_cluster_cold_tx_ctrc_subject.inc"
#include "test_cluster_cold_ctrc_handoff.inc"
#include "test_cluster_cold_ctrc_census.inc"
#include "test_cluster_cold_ctrc_wal_sources.inc"
#include "test_cluster_cold_ctrc_terminals.inc"
#include "test_cluster_cold_ctrc_source_request.inc"
#include "test_cluster_cold_ctrc_replay_source.inc"
#include "test_cluster_cold_ctrc_replay_driver.inc"

UT_TEST(test_start_serial_request_keeps_original_subject_separate_from_new_writer)
{
	ClusterRecoverySerialRequest request, saved;
	uint8 fence_before[sizeof(tx_fence_object)];
	c0_subject();
	UT_ASSERT(ClusterColdRecoveryTxSerialRequest(info.cluster_tx_start, &census.entries[0], &request));
	UT_ASSERT_EQ(request.mode, CLUSTER_RECOVERY_SERIAL_TX_CLOSURE);
	UT_ASSERT(request.tx_closure_scope == NULL);
	UT_ASSERT(memcmp(&request.duty, &typed.sources[0].claim.identity, sizeof(request.duty)) == 0);
	UT_ASSERT_NE(request.duty.origin_owner_incarnation, observed_writer.ref.claim.identity.origin_owner_incarnation);
	saved = request; tx_fence_subject_current = false;
	UT_ASSERT(!ClusterColdRecoveryTxSerialRequest(info.cluster_tx_start, &census.entries[0], &request));
	UT_ASSERT(memcmp(&saved, &request, sizeof(request)) == 0);
	tx_fence_subject_current = true;
	UT_ASSERT(!ClusterColdRecoveryTxSerialRequest(info.cluster_tx_start, &census.entries[0],
		(ClusterRecoverySerialRequest *)info.cluster_tx_start));
	UT_ASSERT(!ClusterColdRecoveryTxSerialRequest(info.cluster_tx_start, &census.entries[0],
		(ClusterRecoverySerialRequest *)&typed.sources[0]));
	UT_ASSERT(!ClusterColdRecoveryTxSerialRequest(info.cluster_tx_start, &census.entries[0],
		(ClusterRecoverySerialRequest *)&census.entries[0]));
	memcpy(fence_before, &tx_fence_object, sizeof(fence_before));
	for (size_t offset = 0; offset <= sizeof(tx_fence_object) - sizeof(request);
		 offset += MAXIMUM_ALIGNOF) {
		UT_ASSERT(!ClusterColdRecoveryTxSerialRequest(info.cluster_tx_start, &census.entries[0],
			(ClusterRecoverySerialRequest *)((char *)&tx_fence_object + offset)));
		UT_ASSERT(memcmp(fence_before, &tx_fence_object, sizeof(fence_before)) == 0);
	}
}

UT_TEST(test_tx_ir_waits_keep_original_input_and_cancel_before_target_or_wal)
{
	c0_subject(); tx_ir_waits_remaining = 193;
	UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	UT_ASSERT_EQ(tx_ir_acquires, 194);
	UT_ASSERT_EQ(tx_ir_local_waits, 193);
	UT_ASSERT_EQ(wal_inserts, 1);
	UT_ASSERT_EQ(tx_ir_releases, 1);
	c0_subject(); tx_ir_waits_remaining = 193; tx_ir_cancel_wait = 129;
	UT_ASSERT(!cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	UT_ASSERT_EQ(tx_ir_acquires, 129);
	UT_ASSERT_EQ(tx_ir_local_waits, 129);
	UT_ASSERT_EQ(target_reserves + stamp_prepares + wal_inserts, 0);
	UT_ASSERT_EQ(scope_releases, 1);
}

UT_TEST(test_actual_c0_requires_new_ir_before_target_or_wal)
{
	c0_subject();
	UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	UT_ASSERT_EQ(tx_ir_acquires, 1);
	UT_ASSERT_EQ(tx_ir_releases, 1);
	UT_ASSERT(!tx_ir_held);
	c0_subject(); tx_ir_allowed = false;
	UT_ASSERT(!cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	UT_ASSERT_EQ(target_reserves + stamp_prepares + wal_inserts + stamp_writes, 0);
	UT_ASSERT_EQ(scope_releases, 1);
}
UT_TEST(test_c0_rechecks_new_ir_at_pre_insert_and_post_flush)
{
	c0_subject(); lose_tx_ir_pre = true;
	UT_ASSERT(!cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	UT_ASSERT_EQ(wal_inserts + stamp_writes, 0);
	UT_ASSERT_EQ(stamp_closes, 1);
	UT_ASSERT_EQ(scope_releases, 1);
	c0_subject(); lose_tx_ir_flush = true;
	UT_ASSERT(!cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	UT_ASSERT_EQ(wal_inserts, 1);
	UT_ASSERT_EQ(stamp_writes, 0);
	UT_ASSERT(window_region.thread_replay[2].stamp.failed);
	UT_ASSERT_EQ(stamp_closes, 1);
	UT_ASSERT_EQ(scope_releases, 1);
}
UT_TEST(test_c0_error_closes_target_before_ir_and_walr)
{
	volatile bool caught = false;
	c0_subject(); flush_error = true;
	PG_TRY();
	{ (void)cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(stamp_closes, 1);
	UT_ASSERT_EQ(target_releases, 1);
	UT_ASSERT_EQ(tx_ir_releases, 1);
	UT_ASSERT_EQ(scope_releases, 1);
	UT_ASSERT(!tx_ir_held && !stamp_live && reserved_target == NULL);
	UT_ASSERT(window_region.thread_replay[2].stamp.failed);
	/* Discard only external fixture leaves on the failing parent. */
	stamp_live = false; reserved_target = NULL; tx_ir_held = false;
}

#include "test_cluster_cold_claim_service.inc"

UT_TEST(test_relmap_declaration_withholds_component_complete_and_is_not_success)
{
	ClusterRecoveryCtrcResultObservationV1 out;
	c0_subject();
	ctrc_census.subject_count = ctrc_census.unresolved_count = 0;
	UT_ASSERT(!ClusterColdRecoveryRelmapCurrent(info.cluster_tx_start));
	/* An existing unaccepted component observation is withdrawn, never reused. */
	UT_ASSERT(cluster_recovery_ctrc_result_collect_v1(info.cluster_tx_start));
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_result.state,
		CLUSTER_RECOVERY_CTRC_RESULT_COMPLETE);
	UT_ASSERT(ClusterColdRecoveryRelmapDeclare(info.cluster_tx_start));
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_result.state,
		CLUSTER_RECOVERY_CTRC_RESULT_PENDING);
	UT_ASSERT(ClusterColdRecoveryRelmapDeclare(info.cluster_tx_start));
	UT_ASSERT(ClusterColdRecoveryRelmapCurrent(info.cluster_tx_start));
	UT_ASSERT(cluster_recovery_ctrc_result_collect_v1(info.cluster_tx_start));
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_result.state,
		CLUSTER_RECOVERY_CTRC_RESULT_PENDING);
	serving_actor();
	UT_ASSERT(!cluster_recovery_ctrc_result_accept_v1());
	UT_ASSERT(!cluster_recovery_ctrc_result_observe_accepted_v1(&out));
	startup_actor();
	UT_ASSERT(ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	UT_ASSERT(!cluster_recovery_checkpoint_handoff_v1(info.cluster_tx_start));
}

UT_TEST(test_relmap_declaration_cannot_reopen_accepted_or_observed_episode)
{
	c0_subject();
	ctrc_census.subject_count = ctrc_census.unresolved_count = 0;
	UT_ASSERT(cluster_recovery_ctrc_result_collect_v1(info.cluster_tx_start));
	serving_actor(); UT_ASSERT(cluster_recovery_ctrc_result_accept_v1()); startup_actor();
	UT_ASSERT(!ClusterColdRecoveryRelmapDeclare(info.cluster_tx_start));
	UT_ASSERT(!ClusterColdRecoveryRelmapCurrent(info.cluster_tx_start));
	c0_subject();
	info.cluster_tx_start->ctrc_observation_started = true;
	UT_ASSERT(!ClusterColdRecoveryRelmapDeclare(info.cluster_tx_start));
	UT_ASSERT(!ClusterColdRecoveryRelmapDeclare((const struct ClusterRecoveryStartV1 *)(uintptr_t)1));
}

UT_TEST(test_relmap_failure_revokes_original_episode_without_free_or_release)
{
	for (unsigned changed_owner = 0; changed_owner < 2; changed_owner++) {
		unsigned destroyed; ResourceOwner original;
		c0_subject();
		UT_ASSERT(ClusterColdRecoveryRelmapDeclare(info.cluster_tx_start));
		destroyed = input_destroys; original = CurrentResourceOwner;
		if (changed_owner) CurrentResourceOwner = (ResourceOwner)(uintptr_t)42;
		ClusterColdRecoveryRelmapFail(info.cluster_tx_start);
		CurrentResourceOwner = original;
		UT_ASSERT(!ClusterColdRecoveryRelmapCurrent(info.cluster_tx_start));
		UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
		UT_ASSERT(!cluster_recovery_ctrc_result_collect_v1(info.cluster_tx_start));
		UT_ASSERT(!ClusterColdRecoveryRelmapDeclare(info.cluster_tx_start));
		UT_ASSERT_EQ(input_destroys, destroyed);
		UT_ASSERT(claim_held);
		UT_ASSERT(!window_region.checkpoint_owner.valid);
	}
}

#include "test_cluster_cold_ctrc_page_plan.inc"
#include "test_cluster_cold_ctrc_page_sources.inc"
#include "test_cluster_cold_ctrc_page_prefix.inc"
#include "test_cluster_cold_ctrc_unchanged_sync.inc"
#include "test_cluster_cold_ctrc_pending_window.inc"
#include "test_cluster_cold_ctrc_returned_fpi.inc"
#include "test_cluster_cold_ctrc_pending_storage.inc"
static void startup_local_empty_reset(void);
#include "test_cluster_cold_ctrc_pending_readback.inc"
#include "test_cluster_cold_ctrc_local_empty.inc"

#include "test_cluster_cold_ctrc_fpi_retirement.inc"
#include "test_cluster_cold_ctrc_completed_readback.inc"
#include "test_cluster_cold_ctrc_unchanged_custody.inc"
#include "test_cluster_cold_ctrc_page_image.inc"
#include "test_cluster_cold_ctrc_final_projection.inc"
#include "test_cluster_cold_ctrc_heap_refs.inc"
#include "test_cluster_cold_ctrc_hot_refs.inc"

#include "test_cluster_cold_ctrc_references.inc"
#include "test_cluster_cold_ctrc_reference_terminals.inc"
#include "test_cluster_cold_ctrc_reference_tuples.inc"
#include "test_cluster_cold_ctrc_census_hot.inc"

int main(void)
{
	if (getenv("PGRAC_PRE2_TEST_CENSUS_HOT") != NULL) {
		UT_PLAN(5);
		UT_RUN(test_startup_census_hot_checks_untouched_current_and_inverse_without_mutation);
		UT_RUN(test_startup_census_hot_late_refusal_current_drift_and_error_keep_debt);
		UT_RUN(test_startup_census_hot_rejects_missing_duplicate_and_gapped_items);
		UT_RUN(test_startup_census_hot_repeat_does_not_cache_topology_success);
		UT_RUN(test_startup_census_hot_zero_page_wal_still_checks_saved_topology);
		reset(); UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_REFERENCE_TUPLES") != NULL) {
		UT_PLAN(6);
		UT_RUN(test_startup_reference_tuples_consume_untouched_current_and_inverse_versions);
		UT_RUN(test_startup_reference_tuples_require_all_itls_from_the_same_page_version);
		UT_RUN(test_startup_reference_tuples_preserve_stored_item_bounds_and_unsupported_storage);
		UT_RUN(test_startup_reference_tuples_late_native_refusal_drift_and_error_keep_debt);
		UT_RUN(test_startup_reference_tuples_repeat_rechecks_without_new_page_or_wal);
		UT_RUN(test_startup_reference_tuples_zero_page_wal_still_checks_complete_view);
		reset(); UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_REFERENCE_TERMINALS") != NULL) {
		UT_PLAN(6);
		UT_RUN(test_startup_reference_terminals_include_untouched_and_inverse_versions);
		UT_RUN(test_startup_reference_terminals_reject_full_identity_drift_not_raw_xid_match);
		UT_RUN(test_startup_reference_terminals_require_original_record_and_physical_page);
		UT_RUN(test_startup_reference_terminals_repeat_rechecks_last_historical_reference);
		UT_RUN(test_startup_reference_terminals_do_not_skip_zero_page_wal_graph);
		UT_RUN(test_startup_reference_terminals_keep_free_and_tuple_candidates_unqualified);
		reset(); UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_STARTUP_HOT_REFS") != NULL) {
		UT_PLAN(4);
		UT_RUN(test_startup_final_hot_refs_use_each_actual_block_after_page_cleanup);
		UT_RUN(test_startup_final_hot_refs_late_refusal_or_input_drift_prevents_handoff);
		UT_RUN(test_startup_final_hot_refs_error_retains_original_fds_and_pins);
		UT_RUN(test_startup_final_hot_refs_repeat_never_caches_topology_success);
		reset(); UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_STARTUP_HEAP_REFS") != NULL) {
		UT_PLAN(4);
		UT_RUN(test_startup_final_heap_refs_consume_every_same_fd_image);
		UT_RUN(test_startup_final_heap_refs_refusal_owner_or_work_drift_prevents_handoff);
		UT_RUN(test_startup_final_heap_refs_error_keeps_original_fds_and_retention);
		UT_RUN(test_startup_final_heap_refs_repeat_rechecks_without_new_wal_or_fd);
		reset(); UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_STARTUP_FINAL_PROJECTION") != NULL) {
		UT_PLAN(7);
		UT_RUN(test_startup_final_projection_consumes_all_pages_after_page_cleanup);
		UT_RUN(test_startup_final_projection_refuses_unresolved_rewritten_and_byte_drift);
		UT_RUN(test_startup_final_projection_rechecks_original_owner_and_full_work);
		UT_RUN(test_startup_final_projection_error_keeps_original_fds_and_debt);
		UT_RUN(test_startup_final_projection_wait_reuses_same_fd_without_new_wal);
		UT_RUN(test_startup_final_projection_repeat_does_not_cache_success);
		UT_RUN(test_startup_final_projection_budget_is_reserved_before_first_page);
		reset(); UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_STARTUP_PAGE_IMAGE") != NULL) {
#ifdef PGRAC_TEST_OLD_IMAGE_FORWARDING
		UT_PLAN(1);
		UT_RUN(test_startup_page_image_is_from_original_completed_or_unchanged_fd);
		UT_DONE(); return ut_failed_count != 0;
#else
		UT_PLAN(5);
		UT_RUN(test_startup_page_image_is_from_original_completed_or_unchanged_fd);
		UT_RUN(test_startup_page_image_rejects_pending_missing_and_nonmember_targets);
		UT_RUN(test_startup_page_image_discards_bytes_after_page_cleanup_drift);
		UT_RUN(test_startup_page_image_wait_has_no_cached_success_or_new_wal);
		UT_RUN(test_startup_page_image_refusal_and_error_keep_original_cleanup_debt);
		reset(); UT_DONE(); return ut_failed_count != 0;
#endif
	}

	if (getenv("PGRAC_PRE2_TEST_UNCHANGED_CUSTODY_STARTUP") != NULL) {
		UT_PLAN(6);
		UT_RUN(test_startup_unchanged_targets_keep_fds_and_full_readback_before_service);
		UT_RUN(test_startup_unchanged_readback_refusal_or_error_keeps_original_cleanup);
		UT_RUN(test_startup_unchanged_wait_reuses_same_saved_targets_without_wal);
		UT_RUN(test_startup_unchanged_storage_is_saved_before_page_cleanup);
		UT_RUN(test_startup_unchanged_late_saved_result_drift_prevents_service);
		UT_RUN(test_startup_unchanged_binding_is_original_start_slot_and_same_fd);
		reset(); UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_COMPLETED_PAGE_STARTUP") != NULL) {
		UT_PLAN(6);
		UT_RUN(test_startup_completed_pages_are_reread_before_independent_service);
		UT_RUN(test_startup_completed_page_refusal_keeps_all_fds_and_shared_debt);
		UT_RUN(test_startup_completed_page_error_preserves_original_cleanup);
		UT_RUN(test_startup_completed_page_wait_reuses_original_result_and_fds);
		UT_RUN(test_startup_completed_result_drift_after_page_cleanup_prevents_service);
		UT_RUN(test_startup_completed_binding_refuses_copies_other_fds_and_retired_observation);
		reset(); UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_STARTUP_FPI_RETIREMENT") != NULL) {
		UT_PLAN(7);
		UT_RUN(test_startup_retires_exact_fpi_windows_before_next_target_and_service);
		UT_RUN(test_startup_retired_fpi_progress_does_not_repeat_wal_or_readback);
		UT_RUN(test_startup_retired_fpi_fd_loss_refuses_without_reopening_or_releasing_scope);
		UT_RUN(test_startup_fpi_retirement_refuses_unproved_readback_or_local_directory);
		UT_RUN(test_startup_later_fpi_refusal_keeps_prior_target_custody_and_shared_debt);
		UT_RUN(test_startup_fpi_completion_capacity_is_reserved_before_first_window);
		UT_RUN(test_startup_retained_raw_handle_peak_precedes_first_page_or_window);
		reset(); UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_STARTUP_LOCAL_EMPTY") != NULL) {
		UT_PLAN(6);
		UT_RUN(test_startup_local_empty_is_after_page_cleanup_without_discharging);
		UT_RUN(test_startup_local_empty_wait_retains_operation_and_rechecks_readback);
		UT_RUN(test_startup_local_empty_refusal_keeps_original_cleanup_debt);
		UT_RUN(test_startup_local_empty_error_keeps_original_cleanup_debt);
		UT_RUN(test_startup_local_empty_cannot_change_saved_operation_or_target);
		UT_RUN(test_startup_local_empty_does_not_hide_readback_refusal);
		reset(); UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_STARTUP_PENDING_READBACK") != NULL) {
		UT_PLAN(6);
		UT_RUN(test_startup_pending_readback_uses_saved_operation_before_service);
		UT_RUN(test_startup_pending_readback_refusal_retains_fd_window_and_graph);
		UT_RUN(test_startup_pending_readback_collision_retries_without_reinserting);
		UT_RUN(test_startup_pending_readback_error_retains_original_cleanup_duty);
		UT_RUN(test_startup_pending_readback_cleanup_cannot_change_saved_operation);
		UT_RUN(test_startup_pending_readback_repeats_protection_without_new_operation);
		reset(); UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_STARTUP_PENDING_STORAGE") != NULL) {
		UT_PLAN(7);
		UT_RUN(test_startup_pending_storage_is_saved_before_page_cleanup);
		UT_RUN(test_startup_pending_storage_loss_refuses_without_reopening);
		UT_RUN(test_startup_pending_storage_late_owner_loss_retains_cleanup);
		UT_RUN(test_startup_pending_storage_page_cleanup_error_keeps_handle);
		UT_RUN(test_startup_pending_storage_requires_handle_for_durable_result);
		UT_RUN(test_startup_pending_storage_noop_and_wait_do_not_leak);
		UT_RUN(test_startup_pending_storage_checks_original_guards_before_metadata);
		reset(); UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_STARTUP_RETURNED_FPI") != NULL) {
		UT_PLAN(7);
		UT_RUN(test_startup_returned_durable_is_saved_before_page_cleanup);
		UT_RUN(test_startup_returned_durable_survives_late_owner_loss);
		UT_RUN(test_startup_returned_failed_operation_survives_late_owner_loss);
		UT_RUN(test_startup_returned_durable_survives_page_cleanup_error);
		UT_RUN(test_startup_returned_failed_operation_survives_page_cleanup_error);
		UT_RUN(test_startup_returned_unchanged_cannot_advance_after_owner_loss);
		UT_RUN(test_startup_returned_unchanged_cannot_advance_after_cleanup_error);
		reset(); UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_STARTUP_PENDING_WINDOW") != NULL) {
		UT_PLAN(5);
		UT_RUN(test_startup_pending_fpi_uses_original_shared_window_without_advancing);
		UT_RUN(test_startup_pending_fpi_refuses_original_failed_window);
		UT_RUN(test_startup_pending_fpi_cannot_rebind_a_later_original_window);
		UT_RUN(test_startup_pending_fpi_requires_complete_saved_operation);
		UT_RUN(test_actual_startup_pending_fpi_failure_interrupts_service_wait);
		reset(); UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_STARTUP_UNCHANGED_SYNC") != NULL) {
		UT_PLAN(2);
		UT_RUN(test_startup_unchanged_sync_advances_without_service_completion);
		UT_RUN(test_startup_legacy_unchanged_cannot_advance_or_complete);
		reset(); UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_STARTUP_PAGE_PREFIX") != NULL) {
		UT_PLAN(14);
		UT_RUN(test_startup_page_prefix_durable_retains_window_and_service_debt);
		UT_RUN(test_startup_page_prefix_legacy_synced_cannot_supply_durable);
		UT_RUN(test_actual_startup_retains_first_inserted_page_before_service);
		UT_RUN(test_actual_startup_refuses_changed_page_target_before_wal);
		UT_RUN(test_startup_page_prefix_repeat_retains_exact_pending_result);
		UT_RUN(test_startup_page_prefix_unchanged_is_not_completion);
		UT_RUN(test_startup_page_prefix_wait_retains_original_target_and_owners);
		UT_RUN(test_startup_page_prefix_refuses_ambiguous_or_unbound_terminal_edge);
		UT_RUN(test_startup_page_prefix_error_or_retirement_cleans_only_private_proof);
		UT_RUN(test_startup_page_prefix_inserted_refusal_retains_original_debt);

				UT_RUN(test_startup_page_prefix_uses_graph_ordinal_not_input_or_replay_index);
		UT_RUN(test_startup_page_prefix_refuses_duplicate_physical_and_terminal_targets);
		UT_RUN(test_startup_page_prefix_wait_rechecks_owner_before_second_attempt);
		UT_RUN(test_startup_page_prefix_capacity_and_caller_locks_precede_execution);
		reset(); UT_DONE(); return ut_failed_count != 0;
	}

	if (getenv("PGRAC_PRE2_TEST_STARTUP_PAGE_SOURCES") != NULL) {
		UT_PLAN(9);
		UT_RUN(test_startup_page_sources_refuse_missing_or_changed_binding);
		UT_RUN(test_startup_page_sources_reserve_vector_budget_before_graph);
		UT_RUN(test_startup_page_sources_error_revokes_original_episode);
		UT_RUN(test_startup_page_sources_revalidate_native_owner_on_repeat);
		UT_RUN(test_startup_page_sources_bind_three_original_indexes_in_graph_order);
		UT_RUN(test_startup_page_sources_reject_changed_full_value_and_original_cut);
		UT_RUN(test_startup_page_sources_callback_retirement_preserves_pointer_lifetime);
		UT_RUN(test_startup_page_sources_failure_before_allocation_keeps_original_debt);
		UT_RUN(test_startup_page_sources_repeat_error_revokes_without_early_release);
		reset(); UT_DONE(); return ut_failed_count != 0;
	}

	if (getenv("PGRAC_PRE2_TEST_STARTUP_PAGE_PLAN") != NULL) {
		UT_PLAN(5);
	UT_RUN(test_actual_startup_prepares_original_page_graph_before_service);
	UT_RUN(test_startup_page_graph_is_original_bounded_and_never_rebuilt);
	UT_RUN(test_startup_page_graph_preconditions_leave_original_custody);
	UT_RUN(test_startup_page_graph_late_refusal_discards_only_private_plan);
	UT_RUN(test_startup_page_graph_error_revokes_episode_and_preserves_cleanup);
		reset(); UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_PAGE_GRAPH_PARENT") != NULL) {
		UT_PLAN(1);
		UT_RUN(test_actual_startup_prepares_original_page_graph_before_service);
		reset(); UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_CTRC_WAL_SOURCES") != NULL) {
		UT_PLAN(4);
		UT_RUN(test_ctrc_wal_sources_bind_complete_original_start_and_do_not_complete_service);
		UT_RUN(test_ctrc_wal_sources_missing_count_drift_and_error_revoke_original_observation);
		UT_RUN(test_ctrc_wal_sources_require_exact_start_work_owner_root_and_accepted_tx);
		UT_RUN(test_ctrc_wal_sources_no_page_input_is_not_a_missing_source_set);
		reset(); UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_CTRC_PAGE_WAL") != NULL) {
		UT_PLAN(4);
		UT_RUN(test_ctrc_page_wal_requires_complete_original_start_and_keeps_service_pending);
		UT_RUN(test_ctrc_page_wal_changed_missing_or_error_revokes_original_observation);
		UT_RUN(test_ctrc_page_wal_requires_exact_work_owner_root_and_accepted_tx);
		UT_RUN(test_ctrc_page_wal_complete_empty_is_not_a_missing_input);
		reset(); UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_RELMAP_PARTICIPANT") != NULL) {
		UT_PLAN(3);
		UT_RUN(test_relmap_declaration_withholds_component_complete_and_is_not_success);
		UT_RUN(test_relmap_declaration_cannot_reopen_accepted_or_observed_episode);
		UT_RUN(test_relmap_failure_revokes_original_episode_without_free_or_release);
		reset(); UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_CTRC_PAGE_INVENTORY") != NULL) {
		UT_PLAN(5);
		UT_RUN(test_ctrc_pages_belong_to_the_complete_original_start_observation);
		UT_RUN(test_ctrc_pages_missing_or_rebound_view_revokes_the_original_observation);
		UT_RUN(test_ctrc_pages_require_original_work_owner_and_accepted_tx);
		UT_RUN(test_ctrc_pages_complete_empty_view_is_distinct_from_missing);
		UT_RUN(test_ctrc_pages_native_error_retires_cursor_before_original_scope);
		reset(); UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_START_REFERENCES") != NULL) {
		UT_PLAN(4);
		UT_RUN(test_ctrc_references_missing_view_refuses_before_observation_publication);
		UT_RUN(test_ctrc_references_cannot_rebind_same_start_to_another_complete_view);
		UT_RUN(test_ctrc_references_error_uses_original_failure_and_reverse_cleanup);
		UT_RUN(test_ctrc_references_after_getter_owner_drift_refuses_without_new_scope);
		UT_DONE();
		return ut_failed_count == 0 ? 0 : 1;
	}
	UT_PLAN(265);
	UT_RUN(test_startup_census_hot_checks_untouched_current_and_inverse_without_mutation);
	UT_RUN(test_startup_census_hot_late_refusal_current_drift_and_error_keep_debt);
	UT_RUN(test_startup_census_hot_rejects_missing_duplicate_and_gapped_items);
	UT_RUN(test_startup_census_hot_repeat_does_not_cache_topology_success);
	UT_RUN(test_startup_census_hot_zero_page_wal_still_checks_saved_topology);

	UT_RUN(test_startup_reference_tuples_consume_untouched_current_and_inverse_versions);
	UT_RUN(test_startup_reference_tuples_require_all_itls_from_the_same_page_version);
	UT_RUN(test_startup_reference_tuples_preserve_stored_item_bounds_and_unsupported_storage);
	UT_RUN(test_startup_reference_tuples_late_native_refusal_drift_and_error_keep_debt);
	UT_RUN(test_startup_reference_tuples_repeat_rechecks_without_new_page_or_wal);
	UT_RUN(test_startup_reference_tuples_zero_page_wal_still_checks_complete_view);
	UT_RUN(test_startup_reference_terminals_include_untouched_and_inverse_versions);
	UT_RUN(test_startup_reference_terminals_reject_full_identity_drift_not_raw_xid_match);
	UT_RUN(test_startup_reference_terminals_require_original_record_and_physical_page);
	UT_RUN(test_startup_reference_terminals_repeat_rechecks_last_historical_reference);
	UT_RUN(test_startup_reference_terminals_do_not_skip_zero_page_wal_graph);
	UT_RUN(test_startup_reference_terminals_keep_free_and_tuple_candidates_unqualified);
	UT_RUN(test_startup_final_hot_refs_use_each_actual_block_after_page_cleanup);
	UT_RUN(test_startup_final_hot_refs_late_refusal_or_input_drift_prevents_handoff);
	UT_RUN(test_startup_final_hot_refs_error_retains_original_fds_and_pins);
	UT_RUN(test_startup_final_hot_refs_repeat_never_caches_topology_success);

	UT_RUN(test_startup_final_heap_refs_consume_every_same_fd_image);
	UT_RUN(test_startup_final_heap_refs_refusal_owner_or_work_drift_prevents_handoff);
	UT_RUN(test_startup_final_heap_refs_error_keeps_original_fds_and_retention);
	UT_RUN(test_startup_final_heap_refs_repeat_rechecks_without_new_wal_or_fd);

	UT_RUN(test_startup_final_projection_consumes_all_pages_after_page_cleanup);
	UT_RUN(test_startup_final_projection_refuses_unresolved_rewritten_and_byte_drift);
	UT_RUN(test_startup_final_projection_rechecks_original_owner_and_full_work);
	UT_RUN(test_startup_final_projection_error_keeps_original_fds_and_debt);
	UT_RUN(test_startup_final_projection_wait_reuses_same_fd_without_new_wal);
	UT_RUN(test_startup_final_projection_repeat_does_not_cache_success);
	UT_RUN(test_startup_final_projection_budget_is_reserved_before_first_page);
	UT_RUN(test_startup_page_image_is_from_original_completed_or_unchanged_fd);
	UT_RUN(test_startup_page_image_rejects_pending_missing_and_nonmember_targets);
	UT_RUN(test_startup_page_image_discards_bytes_after_page_cleanup_drift);
	UT_RUN(test_startup_page_image_wait_has_no_cached_success_or_new_wal);
	UT_RUN(test_startup_page_image_refusal_and_error_keep_original_cleanup_debt);

	UT_RUN(test_startup_unchanged_targets_keep_fds_and_full_readback_before_service);
	UT_RUN(test_startup_unchanged_readback_refusal_or_error_keeps_original_cleanup);
	UT_RUN(test_startup_unchanged_wait_reuses_same_saved_targets_without_wal);
	UT_RUN(test_startup_unchanged_storage_is_saved_before_page_cleanup);
	UT_RUN(test_startup_unchanged_late_saved_result_drift_prevents_service);
	UT_RUN(test_startup_unchanged_binding_is_original_start_slot_and_same_fd);
	UT_RUN(test_startup_completed_pages_are_reread_before_independent_service);
	UT_RUN(test_startup_completed_page_refusal_keeps_all_fds_and_shared_debt);
	UT_RUN(test_startup_completed_page_error_preserves_original_cleanup);
	UT_RUN(test_startup_completed_page_wait_reuses_original_result_and_fds);
	UT_RUN(test_startup_completed_result_drift_after_page_cleanup_prevents_service);
	UT_RUN(test_startup_completed_binding_refuses_copies_other_fds_and_retired_observation);
	UT_RUN(test_startup_retires_exact_fpi_windows_before_next_target_and_service);
	UT_RUN(test_startup_retired_fpi_progress_does_not_repeat_wal_or_readback);
	UT_RUN(test_startup_retired_fpi_fd_loss_refuses_without_reopening_or_releasing_scope);
	UT_RUN(test_startup_fpi_retirement_refuses_unproved_readback_or_local_directory);
	UT_RUN(test_startup_later_fpi_refusal_keeps_prior_target_custody_and_shared_debt);
	UT_RUN(test_startup_fpi_completion_capacity_is_reserved_before_first_window);
	UT_RUN(test_startup_retained_raw_handle_peak_precedes_first_page_or_window);
	UT_RUN(test_startup_local_empty_is_after_page_cleanup_without_discharging);
	UT_RUN(test_startup_local_empty_wait_retains_operation_and_rechecks_readback);
	UT_RUN(test_startup_local_empty_refusal_keeps_original_cleanup_debt);
	UT_RUN(test_startup_local_empty_error_keeps_original_cleanup_debt);
	UT_RUN(test_startup_local_empty_cannot_change_saved_operation_or_target);
	UT_RUN(test_startup_local_empty_does_not_hide_readback_refusal);

	UT_RUN(test_startup_pending_readback_uses_saved_operation_before_service);
	UT_RUN(test_startup_pending_readback_refusal_retains_fd_window_and_graph);
	UT_RUN(test_startup_pending_readback_collision_retries_without_reinserting);
	UT_RUN(test_startup_pending_readback_error_retains_original_cleanup_duty);
	UT_RUN(test_startup_pending_readback_cleanup_cannot_change_saved_operation);
	UT_RUN(test_startup_pending_readback_repeats_protection_without_new_operation);

	UT_RUN(test_startup_pending_storage_is_saved_before_page_cleanup);
	UT_RUN(test_startup_pending_storage_loss_refuses_without_reopening);
	UT_RUN(test_startup_pending_storage_late_owner_loss_retains_cleanup);
	UT_RUN(test_startup_pending_storage_page_cleanup_error_keeps_handle);
	UT_RUN(test_startup_pending_storage_requires_handle_for_durable_result);
	UT_RUN(test_startup_pending_storage_noop_and_wait_do_not_leak);
	UT_RUN(test_startup_pending_storage_checks_original_guards_before_metadata);
	UT_RUN(test_startup_returned_durable_is_saved_before_page_cleanup);
	UT_RUN(test_startup_returned_durable_survives_late_owner_loss);
	UT_RUN(test_startup_returned_failed_operation_survives_late_owner_loss);
	UT_RUN(test_startup_returned_durable_survives_page_cleanup_error);
	UT_RUN(test_startup_returned_failed_operation_survives_page_cleanup_error);
	UT_RUN(test_startup_returned_unchanged_cannot_advance_after_owner_loss);
	UT_RUN(test_startup_returned_unchanged_cannot_advance_after_cleanup_error);
	UT_RUN(test_startup_pending_fpi_uses_original_shared_window_without_advancing);
	UT_RUN(test_startup_pending_fpi_refuses_original_failed_window);
	UT_RUN(test_startup_pending_fpi_cannot_rebind_a_later_original_window);
	UT_RUN(test_startup_pending_fpi_requires_complete_saved_operation);
	UT_RUN(test_actual_startup_pending_fpi_failure_interrupts_service_wait);
	UT_RUN(test_startup_unchanged_sync_advances_without_service_completion);
	UT_RUN(test_startup_legacy_unchanged_cannot_advance_or_complete);
	UT_RUN(test_startup_page_prefix_durable_retains_window_and_service_debt);
	UT_RUN(test_startup_page_prefix_legacy_synced_cannot_supply_durable);
	UT_RUN(test_startup_page_prefix_uses_graph_ordinal_not_input_or_replay_index);
	UT_RUN(test_startup_page_prefix_refuses_duplicate_physical_and_terminal_targets);
	UT_RUN(test_startup_page_prefix_wait_rechecks_owner_before_second_attempt);
	UT_RUN(test_startup_page_prefix_capacity_and_caller_locks_precede_execution);

	UT_RUN(test_startup_page_prefix_repeat_retains_exact_pending_result);
	UT_RUN(test_startup_page_prefix_unchanged_is_not_completion);
	UT_RUN(test_startup_page_prefix_wait_retains_original_target_and_owners);
	UT_RUN(test_startup_page_prefix_refuses_ambiguous_or_unbound_terminal_edge);
	UT_RUN(test_startup_page_prefix_error_or_retirement_cleans_only_private_proof);
	UT_RUN(test_startup_page_prefix_inserted_refusal_retains_original_debt);

	UT_RUN(test_actual_startup_retains_first_inserted_page_before_service);
	UT_RUN(test_actual_startup_refuses_changed_page_target_before_wal);

	UT_RUN(test_startup_page_sources_bind_three_original_indexes_in_graph_order);
	UT_RUN(test_startup_page_sources_reject_changed_full_value_and_original_cut);
	UT_RUN(test_startup_page_sources_callback_retirement_preserves_pointer_lifetime);
	UT_RUN(test_startup_page_sources_failure_before_allocation_keeps_original_debt);
	UT_RUN(test_startup_page_sources_repeat_error_revokes_without_early_release);

	UT_RUN(test_startup_page_sources_refuse_missing_or_changed_binding);
	UT_RUN(test_startup_page_sources_reserve_vector_budget_before_graph);
	UT_RUN(test_startup_page_sources_error_revokes_original_episode);
	UT_RUN(test_startup_page_sources_revalidate_native_owner_on_repeat);
	UT_RUN(test_actual_startup_prepares_original_page_graph_before_service);
	UT_RUN(test_startup_page_graph_is_original_bounded_and_never_rebuilt);
	UT_RUN(test_startup_page_graph_preconditions_leave_original_custody);
	UT_RUN(test_startup_page_graph_late_refusal_discards_only_private_plan);
	UT_RUN(test_startup_page_graph_error_revokes_episode_and_preserves_cleanup);
	UT_RUN(test_ctrc_wal_sources_bind_complete_original_start_and_do_not_complete_service);
	UT_RUN(test_ctrc_wal_sources_missing_count_drift_and_error_revoke_original_observation);
	UT_RUN(test_ctrc_wal_sources_require_exact_start_work_owner_root_and_accepted_tx);
	UT_RUN(test_ctrc_wal_sources_no_page_input_is_not_a_missing_source_set);
	UT_RUN(test_ctrc_page_wal_requires_complete_original_start_and_keeps_service_pending);
	UT_RUN(test_ctrc_page_wal_changed_missing_or_error_revokes_original_observation);
	UT_RUN(test_ctrc_page_wal_requires_exact_work_owner_root_and_accepted_tx);
	UT_RUN(test_ctrc_page_wal_complete_empty_is_not_a_missing_input);
	UT_RUN(test_ctrc_pages_belong_to_the_complete_original_start_observation);
	UT_RUN(test_ctrc_pages_missing_or_rebound_view_revokes_the_original_observation);
	UT_RUN(test_ctrc_pages_require_original_work_owner_and_accepted_tx);
	UT_RUN(test_ctrc_pages_complete_empty_view_is_distinct_from_missing);
	UT_RUN(test_ctrc_pages_native_error_retires_cursor_before_original_scope);

	UT_RUN(test_relmap_declaration_withholds_component_complete_and_is_not_success);
	UT_RUN(test_relmap_declaration_cannot_reopen_accepted_or_observed_episode);
	UT_RUN(test_relmap_failure_revokes_original_episode_without_free_or_release);
	UT_RUN(test_startup_acquires_native_replay_set_before_independent_service);
	UT_RUN(test_startup_replay_acquisition_requires_original_complete_observation);
	UT_RUN(test_startup_replay_wait_retains_partial_owners_without_deadline);
	UT_RUN(test_startup_replay_rechecks_originals_after_interrupts);
	UT_RUN(test_startup_replay_refuses_late_acquisition_and_authorization_drift);
	UT_RUN(test_startup_replay_refuses_native_failure_without_inline_release);
	UT_RUN(test_startup_replay_error_preserves_original_cleanup_responsibility);
	UT_RUN(test_startup_replay_revalidates_manifest_after_all_native_grants);
	UT_RUN(test_startup_replay_refuses_lwlocks_before_original_owner_snapshots);
	UT_RUN(test_startup_replay_repeat_keeps_original_scope_and_empty_work);
	UT_RUN(test_ctrc_replay_request_rejects_stale_start_and_critical_section);
	UT_RUN(test_ctrc_replay_request_covers_original_source_without_a_tt_subject);
	UT_RUN(test_ctrc_replay_request_allows_empty_work_only_after_complete_observation);
	UT_RUN(test_ctrc_replay_request_refuses_nonreplay_ambiguous_and_changed_originals);
	UT_RUN(test_ctrc_replay_current_checks_all_terminals_without_io_or_cleanup);
	UT_RUN(test_ctrc_replay_current_rejects_other_source_and_changed_request_bindings);
	UT_RUN(test_ctrc_replay_request_withholds_result_after_original_late_drift);
	UT_RUN(test_ctrc_replay_request_error_never_publishes_or_retires_original_owner);
	UT_RUN(test_ctrc_serial_current_borrows_only_the_original_complete_scope);
	UT_RUN(test_ctrc_serial_current_rechecks_without_manifest_io_or_scope_cleanup);
	UT_RUN(test_ctrc_serial_current_refuses_every_changed_request_binding);
	UT_RUN(test_ctrc_serial_current_late_drift_never_closes_borrowed_native_scope);
	UT_RUN(test_ctrc_source_request_keeps_original_source_and_provisional_bindings);
	UT_RUN(test_ctrc_source_request_accepts_existing_terminals_without_a_c0_subject);
	UT_RUN(test_ctrc_source_request_requires_original_complete_observation_and_source);
	UT_RUN(test_ctrc_source_request_refuses_history_only_and_uncovered_sources);
	UT_RUN(test_ctrc_source_request_withholds_private_result_after_late_drift);
	UT_RUN(test_ctrc_source_request_propagates_error_without_publishing_or_retiring);
	UT_RUN(test_ctrc_source_request_rejects_stale_owner_and_critical_section);
	UT_RUN(test_original_recovered_claim_spans_eor_pending_service_and_handoff);
	UT_RUN(test_claim_eor_preserves_original_clean_no_attempt_behavior);
	UT_RUN(test_start_requires_the_original_live_claim_incarnation);
	UT_RUN(test_failed_pending_ctrc_keeps_original_claim_and_inputs);
	UT_RUN(test_final_claim_validation_or_arbitration_failure_retains_inputs);
	UT_RUN(test_claim_release_failure_never_returns_startup_success);
	UT_RUN(test_ctrc_observation_requires_every_original_c0_terminal);
	UT_RUN(test_startup_refuses_unusable_ctrc_terminal_before_independent_wait);
	UT_RUN(test_ctrc_observation_qualifies_the_last_subject_without_rewriting_inputs);
	UT_RUN(test_ctrc_borrowed_physical_view_rechecks_original_terminal_proofs);
	UT_RUN(test_ctrc_terminal_recheck_preserves_exact_existing_terminals);
	UT_RUN(test_ctrc_terminal_recheck_cleans_original_children_on_error_and_late_drift);
	UT_RUN(test_ctrc_physical_input_keeps_work_order_and_raw_c0_successor);
	UT_RUN(test_ctrc_physical_input_refuses_reuse_duplicates_and_ambiguous_originals);
	UT_RUN(test_ctrc_physical_input_is_private_until_full_census_and_original_rechecks);
	UT_RUN(test_ctrc_physical_input_requires_exact_owner_work_and_valid_empty_observation);
	UT_RUN(test_ctrc_physical_input_reserves_storage_before_scope_and_preserves_results);
	UT_RUN(test_ctrc_physical_input_retains_storage_on_original_scope_release_error);
	UT_RUN(test_ctrc_observation_refuses_missing_required_current_physical_slot);
	UT_RUN(test_actual_startup_observes_full_ctrc_input_before_independent_handoff);
	UT_RUN(test_ctrc_observation_retains_original_cursor_and_never_completes_service);
	UT_RUN(test_ctrc_observation_refusal_is_sticky_and_preserves_first_stage_and_results);
	UT_RUN(test_ctrc_observation_rejects_original_work_root_owner_and_tx_drift_mid_census);
	UT_RUN(test_ctrc_observation_propagates_native_error_after_child_before_scope_cleanup);
	UT_RUN(test_ctrc_observation_requires_accepted_tx_and_reserves_budget_before_scope);
	UT_RUN(test_ctrc_observation_process_cleanup_retires_children_once_without_handoff);
	UT_RUN(test_startup_retains_inputs_until_independent_ctrc_acceptance);
	UT_RUN(test_ctrc_handoff_pending_timeouts_keep_original_input_and_responsibility);
	UT_RUN(test_ctrc_handoff_refuses_wrong_original_components_without_releasing_input);
	UT_RUN(test_ctrc_handoff_revalidates_after_startup_interrupts);
	UT_RUN(test_ctrc_handoff_propagates_error_without_private_input_release);
	UT_RUN(test_ctrc_terminal_subject_uses_actual_c0_and_exact_accepted_result);
	UT_RUN(test_ctrc_terminal_subject_rejects_changed_physical_and_original_identity);
	UT_RUN(test_ctrc_terminal_subject_requires_complete_current_original_components);
	UT_RUN(test_ctrc_terminal_subject_preserves_preexisting_terminal_bytes);
	UT_RUN(test_ctrc_terminal_subject_refuses_aliases_and_late_owner_change);
	UT_RUN(test_original_lmon_registers_only_the_exact_complete_ctrc_value);
	UT_RUN(test_ctrc_registration_does_not_infer_completion_from_tx_or_an_empty_slot);
	UT_RUN(test_new_original_ctrc_generation_invalidates_its_previous_registration);
	UT_RUN(test_ctrc_registration_rechecks_original_actor_boot_epoch_and_slot);
	UT_RUN(test_original_error_revokes_ctrc_registration_with_the_owner_slot);
	UT_RUN(test_ctrc_work_is_prepared_before_actual_c0_and_keeps_pending_input);
	UT_RUN(test_ctrc_work_requires_original_pointer_and_live_startup_owner);
	UT_RUN(test_ctrc_component_is_independent_of_complete_tx_and_fresh_shmem);
	UT_RUN(test_ctrc_component_publishes_only_registered_original_input);
	UT_RUN(test_ctrc_component_rejects_original_source_and_owner_drift);
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
	UT_RUN(test_c0_refuses_lost_original_isolation_before_target_or_wal);
	UT_RUN(test_c0_original_isolation_rechecked_at_pre_insert_and_post_flush);
	UT_RUN(test_start_binds_original_fence_pointer_and_refuses_stale_begin);
	UT_RUN(test_actual_startup_consumer_requires_numeric_candidate_under_revalidated_scope);
	UT_RUN(test_actual_startup_consumer_reserves_original_target_then_releases_before_scope);
	UT_RUN(test_actual_startup_reservation_failure_or_stale_owner_preserves_active_and_releases);
	UT_RUN(test_actual_startup_prepares_exact_fd_before_releasing_original_guards);
	UT_RUN(test_actual_startup_fd_or_final_close_failure_balances_original_guards);
	UT_RUN(test_actual_c0_insert_flush_stamp_completes_original_checkpoint_cut);
	UT_RUN(test_before_insert_writer_drift_does_not_create_wal_or_target_mutation);
	UT_RUN(test_post_insert_write_or_writer_failure_retains_failed_window);
	UT_RUN(test_post_insert_error_marks_debt_before_rethrow);
	UT_RUN(test_start_serial_request_keeps_original_subject_separate_from_new_writer);
	UT_RUN(test_tx_ir_waits_keep_original_input_and_cancel_before_target_or_wal);
	UT_RUN(test_actual_c0_requires_new_ir_before_target_or_wal);
	UT_RUN(test_c0_rechecks_new_ir_at_pre_insert_and_post_flush);
	UT_RUN(test_c0_error_closes_target_before_ir_and_walr);
	reset();
	if (allocations != frees) { fprintf(stderr, "unreleased start resources\n"); return 1; }
	UT_RUN(test_new_c0_projects_after_durability_before_original_guards_release);
	UT_RUN(test_projection_refusal_blocks_completion_and_balances_original_guards);
	UT_RUN(test_recovered_install_generation_is_original_and_not_a_clean_input);
	UT_RUN(test_original_start_publishes_only_a_checkpointer_opaque_stable_read);
	UT_RUN(test_checkpoint_read_rejects_current_writer_epoch_boot_role_and_resource_drift);
	UT_RUN(test_original_resource_release_and_cleanup_revoke_the_shared_read);
	UT_RUN(test_checkpointer_error_revokes_local_read_without_releasing_start);
	UT_RUN(test_lmon_observes_only_original_recovered_install_without_a_checkpoint_scope);
	UT_RUN(test_lmon_observation_rejects_wrong_actor_or_current_input_without_leaking_a_value);
	UT_RUN(test_lmon_observation_preserves_full_owner_identity_and_original_revocation);
	UT_RUN(test_successful_startup_release_hands_original_owner_to_lmon_and_checkpointer);
	UT_RUN(test_startup_handoff_refusal_keeps_inputs_for_original_error_cleanup);
	UT_RUN(test_tx_component_records_original_c0_completion_and_survives_private_release);
	UT_RUN(test_tx_component_retains_pending_on_original_execution_failure);
	UT_RUN(test_tx_component_distinguishes_complete_empty_from_unproven_active);
	UT_RUN(test_tx_result_lmon_handoff_retains_exact_value_and_revokes_on_error);
	UT_RUN(test_tx_result_binds_input_root_and_cannot_rewind_ordinals);
	UT_RUN(test_original_lmon_accepts_complete_tx_without_changing_its_owner_result);
	UT_RUN(test_original_lmon_does_not_accept_unfinished_or_unproven_tx);
	UT_RUN(test_actual_startup_keeps_private_tx_input_until_lmon_accepts);
	UT_RUN(test_tx_handoff_refuses_pending_or_drift_without_destroying_input);
	UT_RUN(test_ctrc_references_missing_view_refuses_before_observation_publication);
	UT_RUN(test_ctrc_references_cannot_rebind_same_start_to_another_complete_view);
	UT_RUN(test_ctrc_references_error_uses_original_failure_and_reverse_cleanup);
	UT_RUN(test_ctrc_references_after_getter_owner_drift_refuses_without_new_scope);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
