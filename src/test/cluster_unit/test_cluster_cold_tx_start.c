/* Original Startup input resource and installed-writer predicate. Native
 * binding/fence, resource manager and memory are leaves; no WAL or I/O grant. */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"
#include <setjmp.h>
#include "access/xlog.h"
#include "access/xlogrecovery.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_wal_writer.h"
#include "../../../src/backend/cluster/cluster_control_root_private.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
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
ResourceOwner CurrentResourceOwner;
MemoryContext TopMemoryContext;
static ClusterWalStartupImage clusterStartupWriter;
static bool clusterStartupWriterSelected, clusterStartupWriterBound;
static bool clusterStartupWriterInstalled;
static ClusterWalWriterToken observed_writer;
static bool native_recovery, binding_ok, writer_ok, allocation_ok;
static bool effects_ready;
static uint64 owner_boot;
static ResourceReleaseCallback release_callback;
static unsigned callback_registrations, input_destroys, allocations, frees;
static pg_on_exit_callback exit_callback;
static jmp_buf fatal_jump;
static bool fatal_armed;
static const struct ClusterRecoveryStartV1 *destroy_probe;

bool RecoveryInProgress(void) { return native_recovery; }
uint64 cluster_qvotec_get_self_incarnation(void) { return owner_boot; }
ClusterControlRootResult
cluster_wal_writer_begin(TimeLineID timeline, ClusterWalWriterToken *out)
{
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
	if (arg != NULL || release_callback != NULL) abort();
	release_callback = callback;
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

static void reset(void)
{
	if (exit_callback != NULL) exit_callback(0, (Datum)0);
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
	clusterStartupWriterSelected = clusterStartupWriterBound = true;
	clusterStartupWriterInstalled = binding_ok = writer_ok = allocation_ok = true;
	owner_boot = 17;
	clusterStartupWriter.claim.identity.system_identifier = 91;
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

int main(void)
{
	UT_PLAN(13);
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
	reset();
	if (allocations != frees) { fprintf(stderr, "unreleased start resources\n"); return 1; }
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
