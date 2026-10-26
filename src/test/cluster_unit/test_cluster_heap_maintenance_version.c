/*-------------------------------------------------------------------------
 *
 * test_cluster_heap_maintenance_version.c
 *    Native maintenance changes carry the exact heap page version.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_heap_maintenance_version.c
 *
 * NOTES
 *    PGRAC test of native mutation/WAL sections, freeze planning and page
 *    helpers. Prune eligibility and transaction/WAL I/O are fixture inputs;
 *    this is not a full VACUUM or distributed recovery test.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>
#include <stdarg.h>
#include <unistd.h>

#include "access/generic_xlog.h"
#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/htup_details.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/catalog.h"
#include "catalog/pg_class.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_heap_freeze.h"
#include "cluster/cluster_itl_touch.h"
#include "cluster/cluster_itl.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/memdebug.h"
#include "utils/hsearch.h"
#include "common/hashfn.h"
#include "storage/lock.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_pcm_own.h"
#include "cluster/cluster_ko.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_heap_horizon.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/cluster_undo_cleaner.h"
#include "cluster/cluster_undo_horizon.h"
#include "cluster/cluster_undo_retention.h"
#include "cluster/cluster_mrp.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_smgr.h"
#include "commands/vacuum.h"
#include "executor/instrument.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/standby.h"
#include "storage/proc.h"
#include "utils/wait_event.h"
#include "storage/proclist.h"
#include "storage/s_lock.h"
#include "utils/rel.h"
#include "unit_test.h"
#include "cluster/cluster_mode.h"
#include "access/visibilitymap.h"
#include "test_cluster_heap_maintenance_policy.inc"
#include "test_cluster_vacuum_observed_bounds.inc"

UT_DEFINE_GLOBALS();
#include "test_cluster_heap_small_scn.inc"
#include "test_cluster_vacuum_freeze_locators.inc"
bool cluster_enabled = true, cluster_shared_config = true;
/* Native maintenance here uses the ordinary online TARGET_READY role;
 * the authority fixtures own cold/recovered admission coverage. */
BackendType MyBackendType = B_BACKEND;
ClusterNormalStartState
cluster_semantic_normal_start_state(void)
{
	return CLUSTER_NORMAL_START_TARGET_READY;
}
bool
cluster_semantic_activation_phase1_pristine(void)
{
	return false;
}
bool
cluster_semantic_activation_recovered_open_current(void)
{
	return false;
}
int cluster_node_id = 0, wal_level = WAL_LEVEL_REPLICA, NBuffers = 1, NLocBuffer;
static BufferDescPadded descriptors[1];
BufferDescPadded *BufferDescriptors = descriptors;
char *BufferBlocks;
Block *LocalBufferBlockPointers;
volatile uint32 CritSectionCount;
HotStandbyState standbyState = STANDBY_DISABLED;
bool cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;

static PGAlignedBlock current_page, before_page, wal_image;
static RelationData relation_data;
static FormData_pg_class relform;
static ClusterSpaceIdentity space_identity;
static RfPageVersionEdgeEntryV1 edge;
static uint64 next_token, edge_token;
static unsigned inserts, edges, dirties, hints;
static unsigned tx_checks, redo_dirties, redo_releases, freeze_data_calls;
static int expected_nplans;
static PGAlignedBlock freeze_main_data, freeze_block_data;
static Size freeze_block_length;
static uint8 expected_info;
static bool begun, expecting_error, tx_committed;
static bool redoing, redo_locked;
static jmp_buf error_jump;
static bool local_x, writer_current, route_current, authority_current, admission_current;
static bool local_share, resume_allowed, resume_error, resumed_writer;
static bool node_grant_current, object_current, error_after_local_lock, error_after_ledger_bind;
static uint64 current_object_revision;
static unsigned local_lock_attempts, local_unlocks;
int cluster_interconnect_tier;
static bool fixture_node_grant_matches(void);
static unsigned conditional_resumes, conditional_unlocks;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static int32 backend_pin_count;
static unsigned pin_checks;
static unsigned recent_pin_calls, recent_unpins;
static void (*recent_pin_observer)(void);
static bool recent_pin_allowed, recent_pin_error;
static bool recent_caller_local_x, recent_caller_writer, recent_caller_share;
static bool recent_pin_held;
static bool phase_pin_released;
static bool phase_use_original_resident;
static bool phase_use_original_recent_pin;
/* Retain earlier resident observations when the same owner enters native apply. */
static unsigned phase_prior_lock_activity, phase_prior_authority_activity;
static unsigned phase_prior_writer_checks, phase_prior_route_checks, phase_prior_authority_checks;
static bool phase_resident_completed;
static unsigned phase_resident_expected_resumes;
static int32 recent_caller_pins;

/* The original path owns one pin. The recent entry temporarily adds exactly
 * one to the caller's saved count; these assertions must cover both cases. */
static int32
fixture_expected_backend_pins(void)
{
	return phase_pin_released ? 0 : recent_pin_held ? recent_caller_pins + 1 : 1;
}
static const ClusterSemanticAdmissionToken *recent_pin_admission;
static bool flush_error;
static bool space_buffer_permanent, space_permanence_error;
static uint8 writer_state;
static uint64 route_holder, route_master;
static unsigned flushes;
static unsigned writer_checks, route_checks, authority_checks, admission_checks;
static SCN fixture_wal_clock, inserted_wal_clock;
static unsigned wal_clock_reads;
static bool full_target_current, target_recheck_error, revoke_target_census;
static bool native_original_admission_enabled;
static bool bounds_start_original_enabled;
static bool bounds_start_cluster_enabled;
static const char *bounds_start_shared_dir;
static unsigned native_start_change_at, native_start_fault;
static void bounds_start_publish_state(ClusterNormalStartState);
static const ClusterSemanticAdmissionToken *native_admission_expected_token;
static unsigned native_admission_stage, native_admission_fault_at, native_admission_fault;
static unsigned native_admission_checks[7];
static bool fixture_native_admission_current(const ClusterSemanticAdmissionToken *, unsigned);

static unsigned full_target_checks;
static unsigned target_preflight_checks, census_preflight_checks;
static unsigned policy_current_checks, cohort_current_checks;
static unsigned allocation_fence_checks;
static bool allocation_fence_error;
bool cluster_xid_striping;
static bool saved_policy_current;
static ClusterHeapFreezeCohortStatus saved_cohort_status;
/* Worker/admission/capability providers and end-clock remain fixture leaves. */
static const ClusterHeapFreezeCohortObservation *cohort_source_input;
static bool cohort_sample_available, cohort_sample_error, cohort_revoke_ingress;
static int cohort_sample_fault;
static unsigned cohort_sample_reads, cohort_backend_checks;
static bool cohort_worker_live, cohort_admission_live, cohort_clock_error;
static bool cohort_peer_revoke_worker, cohort_clock_revoke_worker, cohort_clock_revoke_admission;
static unsigned cohort_worker_current_checks, cohort_semantic_current_checks;
static unsigned cohort_peer_current_checks, cohort_end_clock_reads;
static uint32 cohort_live_generation[CLUSTER_MAX_NODES];
static int cohort_interval_ms;
static TimestampTz cohort_end_clock;
static int source_current_error_stage, source_current_revoke_census;
static const ClusterHeapFreezePolicySampleV1 *expected_current_policy;
static const ClusterHeapFreezeCohortObservation *expected_current_cohort;
static bool preflight_target_error, preflight_revoke_census, allow_preflight_target;
static ClusterHeapFreezeCohortObservation *retire_bounds_after_target;
static ClusterHeapFreezePolicySampleV1 *advance_legal_next_after_target;
static unsigned registration_checks;
static bool advance_registration_after_target, advance_registration_in_clock;
static bool advance_registration_in_space;
static ClusterHeapFreezeCohortObservation *replace_bounds_in_space;
static ClusterHeapFreezePolicySampleV1 *replace_policy_in_space;
static int replace_bound_kind;
static unsigned space_observation_checks;
/* Keep pre-SPACE fixture counters distinct from final same-token checks. */
static unsigned post_space_target_checks, post_space_census_checks, post_space_error_seen;
static int revoke_admission_in_space, change_source_after_space_target;
static bool post_space_census_error;
static ClusterHeapFreezeCohortObservation *post_space_bounds;
static ClusterHeapFreezePolicySampleV1 *post_space_policy;

/* Original shared-header layout, ingress producer and current-cut predicate.
 * Publication and scheduling remain fixture leaves; full readiness executes
 * the original predicates only in the explicit full-allocation cases. */
#include "test_cluster_heap_ctrc_types.inc"
typedef struct NativeCensusArrayFixture {
	ClusterCtrcSharedHeader header;
	ClusterCtrcOriginEntry origins[CLUSTER_CTRC_ACTIVE_SCAN_SLICE + 1];
	ClusterCtrcParticipantEntry participants[CLUSTER_CTRC_ACTIVE_SCAN_SLICE + 1];
} NativeCensusArrayFixture;
static NativeCensusArrayFixture registration_arrays;
#define registration_shared registration_arrays.header
static ClusterCtrcSharedHeader *CtrcShared = &registration_shared;
static bool registration_attached;
static bool census_runtime_ready_requested;
static bool census_runtime_initialize_requested;

#define ctrc_runtime_attached original_census_runtime_attached
#include "test_cluster_heap_ctrc_attachment.inc"
#undef ctrc_runtime_attached

static bool
ctrc_runtime_attached(void)
{
	registration_checks++;
	UT_ASSERT_EQ(CritSectionCount, 0);
	/* The false-only leaf models an unavailable attachment; successful identity
	 * checks execute the original bounded production predicate. */
	return registration_attached && original_census_runtime_attached();
}

#include "test_cluster_heap_ctrc_registration.inc"

/* The real native apply reads this clock before mutation. WAL insertion's
 * existing production xl_scn stamp uses the same clock; this leaf captures
 * that value without simulating physical WAL insertion or persistence. */
SCN
cluster_scn_current(void)
{
	wal_clock_reads++;
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
	UT_ASSERT_EQ(admission_checks, 1);
	if (advance_registration_in_clock) ctrc_note_unqualified_registration();
	return fixture_wal_clock;
}

/* Only the process-local private refcount is a leaf double. Compile the
 * actual pin reader as well as the native apply bodies below. */
static int32
GetPrivateRefCount(Buffer buffer)
{
	pin_checks++;
	UT_ASSERT_EQ(buffer, 1);
	return backend_pin_count;
}
#include "test_cluster_heap_borrowed_pin.inc"
static bool phase_original_recent_pin(RelFileLocator locator, ForkNumber fork,
									 BlockNumber block, Buffer recent_buffer);
static void phase_original_recent_release(Buffer buffer);
/* Legacy tests retain their pin leaf; the opt-in resident chain below uses
 * the original recent lookup, private refs and ResourceOwner operations. */
bool
ReadRecentBuffer(RelFileLocator locator, ForkNumber fork, BlockNumber block,
				 Buffer recent_buffer)
{
	if (phase_use_original_recent_pin)
		return phase_original_recent_pin(locator, fork, block, recent_buffer);
	recent_pin_calls++;
	UT_ASSERT(RelFileLocatorEquals(locator, relation_data.rd_locator));
	UT_ASSERT_EQ(fork, MAIN_FORKNUM); UT_ASSERT_EQ(block, 7);
	UT_ASSERT_EQ(recent_buffer, 1);
	if (recent_pin_error) pg_re_throw();
	if (!recent_pin_allowed) return false;
	UT_ASSERT(!recent_pin_held);
	UT_ASSERT_EQ(backend_pin_count, recent_caller_pins);
	backend_pin_count++;
	recent_pin_held = true;
	if (recent_pin_observer != NULL) recent_pin_observer();
	return true;
}

void
ReleaseBuffer(Buffer buffer)
{
	if (phase_use_original_recent_pin) {
		phase_original_recent_release(buffer);
		return;
	}
	UT_ASSERT_EQ(buffer, 1); UT_ASSERT(recent_pin_held);
	UT_ASSERT_EQ(backend_pin_count, recent_caller_pins + 1);
	UT_ASSERT_EQ(local_x, recent_caller_local_x);
	UT_ASSERT_EQ(local_share, recent_caller_share);
	UT_ASSERT_EQ(resumed_writer, recent_caller_writer);
	UT_ASSERT(recent_pin_admission != NULL && recent_pin_admission->entered);
	recent_unpins++;
	backend_pin_count--;
	recent_pin_held = false;
	UT_ASSERT_EQ(backend_pin_count, recent_caller_pins);
}


bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == BufferDescriptorGetContentLock(GetBufferDescriptor(0)));
	UT_ASSERT_EQ(mode, LW_EXCLUSIVE);
	return local_x;
}
bool
LWLockHeldByMe(LWLock *lock)
{
	UT_ASSERT(lock == BufferDescriptorGetContentLock(GetBufferDescriptor(0)));
	return local_x || local_share;
}

static ResourceXWriterUseContext original_writer_context;

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	longjmp(error_jump, 1);
}

ClusterPcmOwnResult
cluster_bufmgr_pcm_own_snapshot(BufferDesc *buf, ClusterPcmOwnSnapshot *out)
{
	UT_ASSERT(buf == GetBufferDescriptor(0) && !local_x && backend_pin_count == fixture_expected_backend_pins());
	if (resume_error)
		ereport(ERROR, (errmsg("pre-lock snapshot ERROR fixture")));
	memset(out, 0, sizeof(*out)); out->tag = buf->tag;
	out->generation = original_writer_context.buffer_ownership_generation;
	out->pcm_state = writer_state;
	return node_grant_current ? CLUSTER_PCM_OWN_OK : CLUSTER_PCM_OWN_STALE;
}

bool
cluster_ko_peer_delivery_begin_v3(RelFileLocator locator, uint64 *revision)
{
	UT_ASSERT(RelFileLocatorEquals(locator, relation_data.rd_locator));
	UT_ASSERT(!local_x); *revision = current_object_revision;
	return object_current;
}

bool
cluster_ko_peer_delivery_current_v3(uint64 revision)
{
	return object_current && revision == current_object_revision;
}

bool
ConditionalLockBuffer(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(backend_pin_count == fixture_expected_backend_pins() && !local_x && !local_share);
	local_lock_attempts++; local_x = resume_allowed;
	if (error_after_local_lock)
		ereport(ERROR, (errmsg("post-local-lock ERROR fixture")));
	return local_x;
}

void
LWLockRelease(LWLock *lock)
{
	UT_ASSERT(lock == BufferDescriptorGetContentLock(GetBufferDescriptor(0)));
	UT_ASSERT(local_x && backend_pin_count == fixture_expected_backend_pins());
	local_unlocks++; local_x = resumed_writer = false;
}

bool
cluster_bufmgr_terminal_stamp_authority(Buffer buffer, const BufferTag *tag,
	uint64 *generation, uint64 *epoch, uint8 *state)
{
	writer_checks++;
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(RelFileLocatorEquals(BufTagGetRelFileLocator(tag), relation_data.rd_locator));
	UT_ASSERT_EQ(tag->forkNum, MAIN_FORKNUM);
	UT_ASSERT_EQ(tag->blockNum, 7);
	UT_ASSERT(local_x);
	*generation = 14;
	*epoch = 15;
	*state = writer_state;
	if (error_after_ledger_bind)
		ereport(ERROR, (errmsg("post-ledger-bind ERROR fixture")));
	return writer_current && node_grant_current && fixture_node_grant_matches();
}

/* Compile the actual bufmgr ledger, pin-independent capture/match, original
 * writer resumer and both paired cleanups into the same native WAL fixture.
 * Only these API counters wrap the real functions; no authority is created. */
static bool fixture_actual_resume(Buffer, const ResourceXWriterUseContext *, uint64, bool);
static void fixture_actual_unlock(Buffer);
#define ClusterConditionalLockBufferWithMutationContext fixture_actual_resume
#define ClusterUnlockBufferAfterConditionalMutation fixture_actual_unlock
#include "test_cluster_freeze_writer_context.inc"
#undef ClusterUnlockBufferAfterConditionalMutation
#undef ClusterConditionalLockBufferWithMutationContext

static bool
fixture_node_grant_matches(void)
{
	ClusterPcmXWriterLedgerEntry *e = cluster_bufmgr_pcm_x_writer_find(GetBufferDescriptor(0));
	return e != NULL && e->phase == PCM_X_WRITER_LEDGER_ACTIVE
		&& resource_x_assertion_equal(&e->authority.ref.assertion,
			&original_writer_context.ref.assertion)
		&& e->authority.ref.formation == original_writer_context.ref.formation
		&& e->authority.ref.acquisition_generation == original_writer_context.ref.acquisition_generation
		&& e->authority.r4_record_generation == original_writer_context.r4_record_generation
		&& e->authority.buffer_ownership_generation == original_writer_context.buffer_ownership_generation;
}

bool
ClusterConditionalLockBufferWithMutationContext(Buffer buffer,
	const ResourceXWriterUseContext *expected, uint64 revision, bool checked)
{
	conditional_resumes++;
	resumed_writer = fixture_actual_resume(buffer, expected, revision, checked);
	return resumed_writer;
}

void ClusterUnlockBufferAfterConditionalMutation(Buffer buffer);
void
ClusterUnlockBufferAfterConditionalMutation(Buffer buffer)
{
	conditional_unlocks++;
	fixture_actual_unlock(buffer);
	UT_ASSERT(!local_x && backend_pin_count == fixture_expected_backend_pins());
	UT_ASSERT(cluster_bufmgr_pcm_x_writer_find(GetBufferDescriptor(0)) == NULL);
}

static void
fixture_release_original_writer(void)
{
	/* Initial unlock is a controlled seam; actual release/resume bodies and
	 * the node's original T3 are checked independently and below. */
	memset(cluster_bufmgr_pcm_x_writer_ledger, 0, sizeof(cluster_bufmgr_pcm_x_writer_ledger));
	local_x = false;
}

bool
cluster_pcm_lock_r4_route_snapshot(BufferTag tag, PcmAuthoritySnapshot *out,
	uint64 *master, SCN *scn)
{
	route_checks++;
	UT_ASSERT_EQ(tag.blockNum, 7);
	memset(out, 0, sizeof(*out));
	out->transition_count = route_holder;
	*master = route_master;
	*scn = 20;
	return route_current;
}
bool
cluster_pcm_lock_authority_matches(BufferTag tag, const PcmAuthoritySnapshot *expected)
{
	authority_checks++;
	UT_ASSERT_EQ(tag.blockNum, 7);
	UT_ASSERT_EQ(expected->transition_count, 11);
	return authority_current;
}
bool
cluster_semantic_activation_recheck_r4_terminal_census(
	const ClusterSemanticAdmissionToken *token)
{
	if (!local_x) {
		census_preflight_checks++;
		UT_ASSERT(token != NULL && token->entered && backend_pin_count == fixture_expected_backend_pins());
		UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + local_lock_attempts, phase_prior_lock_activity);
		return admission_current && fixture_native_admission_current(token, 2);
	}
	if (space_observation_checks != 0) {
		post_space_census_checks++;
		UT_ASSERT(token != NULL && token->entered && local_x && backend_pin_count == fixture_expected_backend_pins());
		UT_ASSERT_EQ(post_space_target_checks, 1);
		UT_ASSERT_EQ(CritSectionCount, 0);
		UT_ASSERT_EQ(inserts + dirties + edges + flushes, 0);
		if (post_space_census_error) {
			post_space_error_seen = 2;
			elog(ERROR, "injected final census ERROR after SPACE");
		}
		return admission_current && fixture_native_admission_current(token, 6);
	}
	admission_checks++;
	UT_ASSERT(token != NULL && token->entered);
	return admission_current && fixture_native_admission_current(token, 4);
}
/* Compile the original policy-current predicate; its lease observation
 * below also executes the original retained stripe comparison. */
#define cluster_heap_freeze_policy_sample_current_v1 actual_saved_policy_current
#include "test_cluster_heap_freeze_policy_current.inc"
#undef cluster_heap_freeze_policy_sample_current_v1

/* Execute the original retained stripe comparison. Only external slot /
 * QVOTEC observations and local lock scheduling remain controlled. */
static ClusterXidStripeSampleFence retained_lease_fence;
static bool lease_cluster_on, lease_catalog_on, lease_quorum, lease_lock_held;
static uint64 lease_incarnation;
static int lease_slot, lease_after_policy_fault, lease_revoke_after_unlock, lease_revoke_kind;
static unsigned lease_lock_acquires, lease_lock_releases, lease_incarnation_reads, lease_quorum_reads;
static unsigned lease_error_read;
static void retained_lease_lock(LWLock *, LWLockMode);
static void retained_lease_unlock(LWLock *);
static int retained_lease_slot(void);
static uint64 retained_lease_incarnation(void);
static bool retained_lease_quorum(void);
#define cluster_enabled lease_cluster_on
#define cluster_shared_catalog lease_catalog_on
#define cluster_xid_allocation_slot retained_lease_slot
#define cluster_qvotec_get_self_incarnation retained_lease_incarnation
#define cluster_qvotec_in_quorum retained_lease_quorum
#define LWLockAcquire retained_lease_lock
#define LWLockRelease retained_lease_unlock
#define cluster_xid_stripe_sample_fence_current original_retained_lease_current
#include "test_cluster_heap_freeze_lease_current.inc"
#undef cluster_xid_stripe_sample_fence_current
#undef LWLockRelease
#undef LWLockAcquire
#undef cluster_qvotec_in_quorum
#undef cluster_qvotec_get_self_incarnation
#undef cluster_xid_allocation_slot
#undef cluster_shared_catalog
#undef cluster_enabled
static ClusterXidStripeBootShmem native_lease_shared;

static int retained_lease_slot(void) { return lease_slot; }
static bool retained_lease_quorum(void) { lease_quorum_reads++; return lease_quorum; }
static uint64
retained_lease_incarnation(void)
{
	lease_incarnation_reads++;
	if (lease_error_read == lease_incarnation_reads)
		elog(ERROR, "injected original lease incarnation observer ERROR");
	return lease_incarnation;
}

static void
retained_lease_fault(int fault)
{
	switch (fault) {
	case 0: StripeBootShmem = NULL; break;
	case 1: StripeLease = NULL; break;
	case 2: native_lease_shared.disk_state = CLUSTER_XID_STRIPE_DISK_UNKNOWN; break;
	case 3: native_lease_shared.floor_full++; break;
	case 4: native_lease_shared.epoch++; break;
	case 5: native_lease_shared.generation++; break;
	case 6: native_lease_shared.slot_state = CLUSTER_XID_STRIPE_SLOT_UNKNOWN; break;
	case 7: native_lease_shared.my_slot_claimed = false; break;
	case 8: native_lease_shared.my_slot_floor_full++; break;
	case 9: native_lease_shared.my_owner_incarnation++; break;
	case 10: pg_atomic_write_u64(&native_lease_shared.lease.limit_full, retained_lease_fence.lease_limit_full - 1); break;
	case 11: pg_atomic_fetch_add_u64(&native_lease_shared.lease.floor_full, 1); break;
	case 12: pg_atomic_fetch_add_u64(&native_lease_shared.lease.published_incarnation, 1); break;
	case 13: pg_atomic_write_u32(&native_lease_shared.lease.failed, 1); break;
	}
}

static void
retained_lease_lock(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == &native_lease_shared.lock && mode == LW_SHARED && !lease_lock_held);
	lease_lock_held = true; lease_lock_acquires++;
}

static void
retained_lease_unlock(LWLock *lock)
{
	UT_ASSERT(lock == &native_lease_shared.lock && lease_lock_held);
	lease_lock_held = false; lease_lock_releases++;
	if (lease_lock_releases == 1 && lease_after_policy_fault >= 0)
		retained_lease_fault(lease_after_policy_fault);
	if (lease_lock_releases == lease_revoke_after_unlock) {
		if (lease_revoke_kind == 1) lease_incarnation++;
		else if (lease_revoke_kind == 2) lease_quorum = false;
	}
}

static void
retained_lease_publish_fixture(ClusterHeapFreezePolicySampleV1 *policy)
{
	/* Fixture image only: no lease grant, IO or production publisher. */
	memset(&native_lease_shared, 0, sizeof(native_lease_shared));
	StripeBootShmem = &native_lease_shared; StripeLease = &native_lease_shared.lease;
	retained_lease_fence = (ClusterXidStripeSampleFence){
		.activation_floor_full = UINT64CONST(0x100000003), .activation_epoch = 21,
		.activation_generation = 13, .slot_floor_full = UINT64CONST(0x100000003),
		.slot_owner_incarnation = 9, .incarnation = 9,
		.lease_floor_full = UINT64CONST(0x100000003),
		.lease_limit_full = U64FromFullTransactionId(policy->legal_next) + CLUSTER_XID_STRIDE,
		.slot = XidFromFullTransactionId(policy->legal_next) % CLUSTER_XID_STRIDE };
	/* A real future must be in the leased slot and strictly below its limit. */
	lease_slot = retained_lease_fence.slot;
	native_lease_shared.disk_state = CLUSTER_XID_STRIPE_DISK_PUBLISHED;
	native_lease_shared.floor_full = retained_lease_fence.activation_floor_full;
	native_lease_shared.epoch = retained_lease_fence.activation_epoch;
	native_lease_shared.generation = retained_lease_fence.activation_generation;
	native_lease_shared.slot_state = CLUSTER_XID_STRIPE_SLOT_MINE;
	native_lease_shared.my_slot_claimed = true;
	native_lease_shared.my_slot_floor_full = retained_lease_fence.slot_floor_full;
	native_lease_shared.my_owner_incarnation = retained_lease_fence.slot_owner_incarnation;
	pg_atomic_init_u64(&StripeLease->floor_full, retained_lease_fence.lease_floor_full);
	pg_atomic_init_u64(&StripeLease->limit_full, retained_lease_fence.lease_limit_full);
	pg_atomic_init_u64(&StripeLease->published_incarnation, retained_lease_fence.incarnation);
	pg_atomic_init_u32(&StripeLease->failed, 0);
	pg_atomic_init_u64(&StripeBootShmem->herding_floor_full, 0);
	pg_atomic_init_u64(&StripeBootShmem->cluster_min_active_hwm, 0);
	pg_atomic_init_u32(&StripeBootShmem->wrap_barrier_done, 1);
	policy->allocation_fence = retained_lease_fence;
}

bool
cluster_xid_stripe_sample_fence_current(const ClusterXidStripeSampleFence *fence)
{
	UT_ASSERT(!local_x && !local_share && backend_pin_count == fixture_expected_backend_pins());
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + local_lock_attempts, phase_prior_lock_activity);
	UT_ASSERT_EQ(policy_current_checks, 1);
	UT_ASSERT_EQ(cohort_current_checks + census_preflight_checks, 0);
	if (expected_current_policy != NULL)
		UT_ASSERT(fence == &expected_current_policy->allocation_fence);
	allocation_fence_checks++;
	if (allocation_fence_error) elog(ERROR, "injected original stripe fence error");
	return saved_policy_current && original_retained_lease_current(fence);
}

/* Instrument the original unlocked call without replacing its predicate. */
bool
cluster_heap_freeze_policy_sample_current_v1(const ClusterHeapFreezePolicySampleV1 *sample)
{
	UT_ASSERT(!local_x && !local_share && backend_pin_count == fixture_expected_backend_pins());
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + local_lock_attempts, phase_prior_lock_activity);
	UT_ASSERT_EQ(inserts + dirties + flushes + wal_clock_reads, 0);
	UT_ASSERT_EQ(target_preflight_checks, 1);
	UT_ASSERT_EQ(census_preflight_checks, 0);
	if (expected_current_policy != NULL) UT_ASSERT(sample == expected_current_policy);
	policy_current_checks++;
	if (source_current_error_stage == 1) elog(ERROR, "injected saved-policy current error");
	if (source_current_revoke_census == 1) admission_current = false;
	return actual_saved_policy_current(sample);
}

/* Return a structured worker publication to the original current entry. Its
 * acquisition/lease authority is controlled; independent mismatches are injected
 * below, so this never certifies the all-producer or native lifetime chain. */
static bool original_worker_publication_get(ClusterUndoCleanerActiveSample *);
static void worker_original_publish_fixture(const ClusterHeapFreezeCohortObservation *);
static void worker_original_sample_pass_fixture(const ClusterHeapFreezeCohortObservation *);
static void worker_native_sample_fixture(const ClusterHeapFreezeCohortObservation *);

bool
cluster_undo_cleaner_active_sample(ClusterUndoCleanerActiveSample *out)
{
	UT_ASSERT(!local_x && !local_share && backend_pin_count == fixture_expected_backend_pins());
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + local_lock_attempts, phase_prior_lock_activity);
	UT_ASSERT_EQ(cohort_current_checks, 1);
	cohort_sample_reads++;
	if (cohort_sample_error) elog(ERROR, "injected worker publication read ERROR");
	if (!cohort_sample_available) return false;
	UT_ASSERT(cohort_source_input != NULL);
	if (!original_worker_publication_get(out)) return false;
	switch (cohort_sample_fault) {
	case 1: out->sample_id++; break;
	case 2: out->scan.min_full = FullTransactionIdFromU64(U64FromFullTransactionId(out->scan.min_full) + 1); break;
	case 3: out->admission_fence.record_generation++; break;
	case 4: out->scan.status = CTRC_ACTIVE_SCAN_INCOMPLETE; break;
	}
	return true;
}

/* Keep provider identity/current decisions explicit while executing the
 * original peer/H loops, local wrapper and end-clock ordering. */
/* Original worker publication-current; scheduling, lease and admission
 * providers remain controlled, and cannot manufacture a cleaner publication. */
static UndoCleanerSharedState native_worker_shared;
static UndoCleanerSharedState *undo_cleaner_state;
static bool worker_current_enabled, worker_storage_ok, worker_stop_cut;
static bool worker_lease_ok, worker_admission_ok, worker_lease_error, worker_admission_error;
static bool worker_lock_held, worker_register_on_unlock;
static unsigned worker_lock_acquires, worker_lock_releases;
static unsigned worker_lease_checks, worker_admission_checks;

static bool
worker_current_storage(void)
{
	return worker_storage_ok;
}

static bool
worker_current_stop(void)
{
	return worker_stop_cut;
}

static bool
worker_current_lease(const ClusterXidStripeSampleFence *fence)
{
	UT_ASSERT(fence != NULL && !worker_lock_held);
	worker_lease_checks++;
	if (worker_lease_error) elog(ERROR, "injected original worker lease ERROR");
	return worker_lease_ok && original_retained_lease_current(fence);
}

/* Original admission seqlock, policy, capture and complete-fence comparison.
 * Membership/QVOTEC and lifecycle observations remain controlled providers. */
#include "test_cluster_active_sample_types.inc"
static ClusterSemanticActivationShmem native_admission_shared;
static ClusterSemanticActivationShmem *SemanticActivationShmem;
static ClusterR4MembershipSnapshot admission_members;
static uint64 admission_system_id, admission_epoch, admission_incarnation, admission_cut;
static int admission_self, admission_membership_fault;
static bool admission_recovering, admission_stopping, admission_serving;
static bool admission_join_allowed, admission_quorum, admission_membership_available;
static unsigned admission_membership_reads;

static bool admission_source_recovery(void) { return admission_recovering; }
static bool admission_source_stop(void) { return admission_stopping; }
static bool admission_source_serving(void) { return admission_serving; }
static int admission_source_join(void) { return admission_join_allowed ? CLUSTER_JOIN_GATE_ALLOW : -1; }
static bool admission_source_quorum(void) { return admission_quorum; }
static uint64 admission_source_system_id(void) { return admission_system_id; }
static uint64 admission_source_epoch(void) { return admission_epoch; }
static uint64 admission_source_incarnation(void) { return admission_incarnation; }
static uint64 admission_source_cut(void) { return admission_cut; }
static bool admission_source_cut_current(uint64 expected) { return expected == admission_cut; }

static bool
admission_source_membership(ClusterR4MembershipSnapshot *out)
{
	UT_ASSERT(out != NULL && !worker_lock_held);
	admission_membership_reads++;
	if (!admission_membership_available) return false;
	*out = admission_members;
	switch (admission_membership_fault) {
	case 1: pg_atomic_fetch_add_u64(&native_admission_shared.admission_seq, 2); break;
	case 2: admission_cut++; break;
	case 3: admission_epoch++; break;
	case 4: admission_incarnation++; break;
	case 5: admission_serving = false; break;
	case 6: admission_quorum = false; break;
	case 7: admission_stopping = true; break;
	case 8: admission_self++; break;
	case 9: elog(ERROR, "injected original admission membership ERROR"); break;
	}
	return true;
}

static bool original_admission_source_capture(ClusterSemanticActiveSampleFence *);
static bool original_admission_source_current(const ClusterSemanticActiveSampleFence *);
#define cluster_node_id admission_self
#define RecoveryInProgress admission_source_recovery
#define cluster_normal_stop_maintenance_cut admission_source_stop
#define normal_start_serving_allowed admission_source_serving
#define cluster_reconfig_self_join_gate_verdict admission_source_join
#define cluster_membership_cut_generation admission_source_cut
#define GetSystemIdentifier admission_source_system_id
#define cluster_reconfig_lmon_snapshot_r4_membership admission_source_membership
#define cluster_qvotec_in_quorum admission_source_quorum
#define cluster_qvotec_get_self_incarnation admission_source_incarnation
#define cluster_epoch_get_current admission_source_epoch
#define cluster_membership_cut_generation_current admission_source_cut_current
#define cluster_semantic_activation_active_sample_capture original_admission_source_capture
#define cluster_semantic_activation_active_sample_current original_admission_source_current
#include "test_cluster_active_sample_fence.inc"
#undef cluster_semantic_activation_active_sample_current
#undef cluster_semantic_activation_active_sample_capture
#undef cluster_membership_cut_generation_current
#undef cluster_epoch_get_current
#undef cluster_qvotec_get_self_incarnation
#undef cluster_qvotec_in_quorum
#undef cluster_reconfig_lmon_snapshot_r4_membership
#undef GetSystemIdentifier
#undef cluster_membership_cut_generation
#undef cluster_reconfig_self_join_gate_verdict
#undef normal_start_serving_allowed
#undef cluster_normal_stop_maintenance_cut
#undef RecoveryInProgress
#undef cluster_node_id

static void
admission_current_publish_fixture(ClusterHeapFreezeCohortObservation *cohort)
{
	/* Controlled fixture publication, not a production R4 activation. */
	memset(&native_admission_shared, 0, sizeof(native_admission_shared));
	SemanticActivationShmem = &native_admission_shared;
	pg_atomic_init_u64(&native_admission_shared.admission_seq, 2);
	pg_atomic_init_u64(&native_admission_shared.active_bits, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	pg_atomic_init_u64(&native_admission_shared.record_generation, cohort->record_generation);
	pg_atomic_init_u64(&native_admission_shared.formation_epoch, cohort->formation_epoch);
	pg_atomic_init_u32(&native_admission_shared.transition_closed, 0);
	memset(&admission_members, 0, sizeof(admission_members));
	admission_self = 0; admission_system_id = 12345;
	admission_epoch = admission_members.formation_epoch = cohort->formation_epoch;
	admission_incarnation = admission_members.local_self_boot_incarnation = 9;
	admission_cut = 4;
	admission_members.admitted_members_lo = cohort->admitted_members_lo;
	admission_members.admitted_members_hi = cohort->admitted_members_hi;
	for (int peer = 0; peer < CLUSTER_MAX_NODES; peer++) {
		if ((peer < 64 ? cohort->admitted_members_lo : cohort->admitted_members_hi)
			& (UINT64CONST(1) << (peer % 64))) {
			admission_members.admitted_incarnation[peer] = admission_incarnation + peer;
			admission_members.observed_generation[peer] = 12;
		}
	}
	UT_ASSERT(original_admission_source_capture(&cohort->admission_fence));
	admission_membership_reads = 0;
}

static bool
worker_current_admission(const ClusterSemanticActiveSampleFence *fence)
{
	UT_ASSERT(fence != NULL && !worker_lock_held);
	worker_admission_checks++;
	if (worker_admission_error) elog(ERROR, "injected original worker admission ERROR");
	return worker_admission_ok && original_admission_source_current(fence);
}

static void
worker_current_lock(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == &native_worker_shared.lwlock && mode == LW_SHARED);
	UT_ASSERT(!worker_lock_held);
	worker_lock_held = true; worker_lock_acquires++;
}

static void
worker_current_unlock(LWLock *lock)
{
	UT_ASSERT(lock == &native_worker_shared.lwlock && worker_lock_held);
	worker_lock_held = false; worker_lock_releases++;
	if (worker_register_on_unlock) ctrc_note_unqualified_registration();
}

#define cluster_undo_cleaner_enabled worker_current_enabled
#define cluster_storage_mode_enabled worker_current_storage
#define cluster_normal_stop_maintenance_cut worker_current_stop
#define cluster_xid_stripe_sample_fence_current worker_current_lease
#define cluster_semantic_activation_active_sample_current worker_current_admission
#define LWLockAcquire worker_current_lock
#define LWLockRelease worker_current_unlock
#define cluster_undo_cleaner_active_sample_current original_worker_publication_current
#include "test_cluster_heap_freeze_worker_current.inc"
#undef cluster_undo_cleaner_active_sample_current
#undef LWLockRelease
#undef LWLockAcquire
#undef cluster_semantic_activation_active_sample_current
#undef cluster_xid_stripe_sample_fence_current
#undef cluster_normal_stop_maintenance_cut
#undef cluster_storage_mode_enabled
#undef cluster_undo_cleaner_enabled

/* Getter copy and current comparison use the same original shared lock;
 * keep separate trace counts so each critical section remains observable. */
static unsigned getter_copy_acquires, getter_copy_releases;
static int getter_copy_fault;
static void
worker_getter_lock(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == &native_worker_shared.lwlock && mode == LW_SHARED && !worker_lock_held);
	worker_lock_held = true; getter_copy_acquires++;
}

static void
worker_getter_unlock(LWLock *lock)
{
	UT_ASSERT(lock == &native_worker_shared.lwlock && worker_lock_held);
	worker_lock_held = false; getter_copy_releases++;
	switch (getter_copy_fault) {
	case 1: native_worker_shared.active_sample.sample_id++; break;
	case 2: native_worker_shared.shutdown_requested = true; break;
	case 3: native_worker_shared.workers[0].status = UNDO_CLEANER_SPAWNING; break;
	case 4: native_worker_shared.workers[0].pid++; break;
	case 5: ctrc_note_unqualified_registration(); break;
	case 6: pg_atomic_write_u32(&StripeLease->failed, 1); break;
	case 7: pg_atomic_fetch_add_u64(&native_admission_shared.admission_seq, 2); break;
	case 8: worker_lease_error = true; break;
	case 9: worker_admission_error = true; break;
	}
}

#define LWLockAcquire worker_getter_lock
#define LWLockRelease worker_getter_unlock
#define cluster_undo_cleaner_active_sample_current original_worker_publication_current
#define cluster_undo_cleaner_active_sample original_worker_publication_get
#include "test_cluster_heap_freeze_worker_getter.inc"
#undef cluster_undo_cleaner_active_sample
#undef cluster_undo_cleaner_active_sample_current
#undef LWLockRelease
#undef LWLockAcquire

/* Original publisher inputs remain controlled; publication itself uses its
 * original exclusive shared lock, independently traced from both readers. */
static int publisher_worker, publisher_pid, publisher_unlock_fault;
static unsigned publisher_lock_acquires, publisher_lock_releases;
static ClusterCtrcActiveScan publisher_scan;
static ClusterXidStripeSampleFence publisher_lease;
static ClusterSemanticActiveSampleFence publisher_admission;

static void
worker_publisher_lock(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == &native_worker_shared.lwlock && mode == LW_EXCLUSIVE);
	UT_ASSERT(!worker_lock_held);
	worker_lock_held = true; publisher_lock_acquires++;
}

static void
worker_publisher_unlock(LWLock *lock)
{
	UT_ASSERT(lock == &native_worker_shared.lwlock && worker_lock_held);
	worker_lock_held = false; publisher_lock_releases++;
	switch (publisher_unlock_fault) {
	case 1: ctrc_note_unqualified_registration(); break;
	case 2: pg_atomic_write_u32(&StripeLease->failed, 1); break;
	case 3: pg_atomic_fetch_add_u64(&native_admission_shared.admission_seq, 2); break;
	case 4: native_worker_shared.shutdown_requested = true; break;
	case 5: native_worker_shared.workers[0].status = UNDO_CLEANER_SPAWNING; break;
	case 6: native_worker_shared.workers[0].pid++; break;
	}
}

#define undo_cleaner_worker publisher_worker
#define undo_cleaner_active_scan publisher_scan
#define undo_cleaner_active_lease_fence publisher_lease
#define undo_cleaner_active_admission_fence publisher_admission
#define MyProcPid publisher_pid
#define LWLockAcquire worker_publisher_lock
#define LWLockRelease worker_publisher_unlock
#define undo_cleaner_publish_active_sample original_worker_publication_publish
#include "test_cluster_heap_freeze_worker_publisher.inc"
#undef undo_cleaner_publish_active_sample
#undef LWLockRelease
#undef LWLockAcquire
#undef MyProcPid
#undef undo_cleaner_active_admission_fence
#undef undo_cleaner_active_lease_fence
#undef undo_cleaner_active_scan
#undef undo_cleaner_worker

static void
worker_original_publish_fixture(const ClusterHeapFreezeCohortObservation *cohort)
{
	/* Controlled completed input and process identity; no census run-pass or
	 * qualification is supplied by this helper. Keep the real sequence. */
	publisher_worker = 0; publisher_pid = 42;
	publisher_scan = cohort->local_registration_cut;
	publisher_lease = retained_lease_fence;
	publisher_admission = cohort->admission_fence;
	original_worker_publication_publish();
}

/* Original coordinator, native window and census traversal. Publication,
 * attachment and lock scheduling supply no producer/freeze authority. */
static bool pass_capture_allowed, pass_native_allowed, pass_begin_allowed, pass_pending;
static unsigned pass_capture_reads, pass_native_reads, pass_begin_reads, pass_step_reads;
static unsigned pass_census_ready_reads;
static bool census_step_lock_held;
static LWLock *census_step_held_lock;
static unsigned census_step_lock_reads, census_step_lock_acquires, census_step_lock_releases;
static unsigned census_step_origin_locks, census_step_participant_locks[CLUSTER_CTRC_CLEANER_WORKERS];
static unsigned census_step_block_at, census_step_revoke_at;
static unsigned pass_lease_checks, pass_admission_checks, pass_incomplete_slices;
static int pass_step_fault;
static FullTransactionId pass_reference, pass_future, pass_active_min;
static ClusterSemanticActiveSampleFence pass_captured_admission;
static bool sampler_lock_held;

static void
worker_pass_assert_unlocked(void)
{
	UT_ASSERT(!census_step_lock_held);
	UT_ASSERT(!sampler_lock_held && !worker_lock_held && !lease_lock_held && !local_x && !local_share);
	UT_ASSERT_EQ(CritSectionCount, 0);
}

static bool
worker_pass_capture(ClusterSemanticActiveSampleFence *out)
{
	worker_pass_assert_unlocked(); pass_capture_reads++;
	memset(out, 0, sizeof(*out));
	if (!pass_capture_allowed || !original_admission_source_capture(out)) return false;
	pass_captured_admission = *out;
	return true;
}

/* Original compact native membership layout and sampler. Registry contents,
 * lock scheduling and stripe-future are fixture inputs, not live provenance. */
#include "test_cluster_heap_freeze_pgproc_types.inc"
static VariableCacheData sampler_variables;
static VariableCache sampler_cache = &sampler_variables;
static PROC_HDR sampler_procs;
static PROC_HDR *sampler_proc_global = &sampler_procs;
static PGPROC sampler_slots[3];
static TransactionId sampler_xids[3];
static char sampler_array_storage[offsetof(ProcArrayStruct, pgprocnos) + 3 * sizeof(int)]
	pg_attribute_aligned(MAXIMUM_ALIGNOF);
static ProcArrayStruct *sampler_proc_array = (ProcArrayStruct *) sampler_array_storage;
static LWLockPadded sampler_locks[NUM_INDIVIDUAL_LWLOCKS];
#define MainLWLockArray sampler_locks
static LWLock *const sampler_xid_lock = XidGenLock;
static LWLock *const sampler_proc_lock = ProcArrayLock;
#undef MainLWLockArray
static LWLock *sampler_held_lock;
static unsigned sampler_xid_locks, sampler_proc_locks;
static unsigned sampler_lock_acquires, sampler_lock_releases, sampler_future_reads;
static bool sampler_recovering, sampler_future_allowed, sampler_advance_on_xid_unlock;

static bool
sampler_recovery_in_progress(void)
{
	return sampler_recovering;
}

static void
sampler_lock_acquire(LWLock *lock, LWLockMode mode)
{
	worker_pass_assert_unlocked();
	UT_ASSERT(mode == LW_SHARED && (lock == sampler_xid_lock || lock == sampler_proc_lock));
	if (lock == sampler_xid_lock) sampler_xid_locks++;
	else {
		UT_ASSERT_EQ(sampler_xid_locks, sampler_proc_locks + 1);
		sampler_proc_locks++;
	}
	sampler_lock_held = true; sampler_held_lock = lock; sampler_lock_acquires++;
}

static void
sampler_lock_release(LWLock *lock)
{
	UT_ASSERT(sampler_lock_held && lock == sampler_held_lock);
	sampler_lock_held = false; sampler_held_lock = NULL; sampler_lock_releases++;
	if (lock == sampler_xid_lock && sampler_advance_on_xid_unlock)
		sampler_variables.nextXid = FullTransactionIdFromEpochAndXid(2, 300);
}

static int sampler_herding_slack;
static bool original_stripe_window_exceeded(FullTransactionId);
static bool original_stripe_wrap_barrier_passed(void);
static bool sampler_lock_held_by_me(LWLock *lock)
{
	return sampler_lock_held && sampler_held_lock == lock;
}
#define MainLWLockArray sampler_locks
#define LWLockHeldByMe sampler_lock_held_by_me
#define LWLockAcquire retained_lease_lock
#define LWLockRelease retained_lease_unlock
#define cluster_enabled lease_cluster_on
#define cluster_shared_catalog lease_catalog_on
#define cluster_xid_allocation_slot retained_lease_slot
#define cluster_qvotec_get_self_incarnation retained_lease_incarnation
#define cluster_qvotec_in_quorum retained_lease_quorum
#define cluster_xid_herding_slack sampler_herding_slack
#define cluster_xid_next_striped_full original_stripe_next_full
#define cluster_xid_stripe_window_exceeded original_stripe_window_exceeded
#define cluster_xid_wrap_barrier_passed original_stripe_wrap_barrier_passed
#define cluster_xid_stripe_sample_fence_current original_retained_lease_current
#define cluster_xid_stripe_sample_future original_stripe_future
#include "test_cluster_heap_freeze_stripe_future.inc"
#undef cluster_xid_stripe_sample_future
#undef cluster_xid_stripe_sample_fence_current
#undef cluster_xid_wrap_barrier_passed
#undef cluster_xid_stripe_window_exceeded
#undef cluster_xid_next_striped_full
#undef cluster_xid_herding_slack
#undef cluster_qvotec_in_quorum
#undef cluster_qvotec_get_self_incarnation
#undef cluster_xid_allocation_slot
#undef cluster_shared_catalog
#undef cluster_enabled
#undef LWLockRelease
#undef LWLockAcquire
#undef LWLockHeldByMe
#undef MainLWLockArray

static bool
sampler_stripe_future(FullTransactionId reference, FullTransactionId *future,
					  ClusterXidStripeSampleFence *fence)
{
	UT_ASSERT(sampler_lock_held && sampler_held_lock == sampler_xid_lock);
	UT_ASSERT(!worker_lock_held && !lease_lock_held && !local_x && !local_share);
	UT_ASSERT(FullTransactionIdIsValid(reference)); sampler_future_reads++;
	*future = InvalidFullTransactionId; memset(fence, 0, sizeof(*fence));
	if (!sampler_future_allowed) return false;
	return original_stripe_future(reference, future, fence);
}

#define ShmemVariableCache sampler_cache
#define ProcGlobal sampler_proc_global
#define procArray sampler_proc_array
#define MainLWLockArray sampler_locks
#define RecoveryInProgress sampler_recovery_in_progress
#define LWLockAcquire sampler_lock_acquire
#define LWLockRelease sampler_lock_release
#define cluster_xid_stripe_sample_future sampler_stripe_future
#define ProcArrayGetActiveFullXidSample original_native_fullxid_sample
#include "test_cluster_heap_freeze_pgproc_sample.inc"
#undef ProcArrayGetActiveFullXidSample
#undef cluster_xid_stripe_sample_future
#undef LWLockRelease
#undef LWLockAcquire
#undef RecoveryInProgress
#undef MainLWLockArray
#undef procArray
#undef ProcGlobal
#undef ShmemVariableCache
#undef UINT32_ACCESS_ONCE


/* Execute the original native exit on the same PGPROC used by origin reserve.
 * Legacy cases take the direct exclusive path. Opt-in cases force its first
 * conditional miss and execute the original group leader. A second queued
 * member is an explicit scheduling leaf; semaphore waiters remain outside. */
static bool source_exit_lock_held;
static bool source_exit_group_requested;
static bool source_exit_history_requested;
static unsigned source_exit_history_samples;
static ClusterUndoRetentionSample source_exit_history_report;
static bool source_exit_member_requested;
static unsigned source_exit_member_wakeups;
static FullTransactionId source_exit_member_wait_minimum, source_exit_member_wake_minimum;
static FullTransactionId source_exit_member_census_minimum;
static unsigned source_exit_locks, source_exit_unlocks;
static XidCacheStatus source_exit_subxids[3];
static uint8 source_exit_status[3];
static ProcessingMode source_exit_mode = NormalProcessing;
static void source_exit_internal(PGPROC *, TransactionId);
static void source_exit_maintain_latest(TransactionId);
static FullTransactionId source_exit_relative(FullTransactionId, TransactionId);
static bool source_exit_precedes(TransactionId, TransactionId);
static bool source_exit_precedes_or_equals(TransactionId, TransactionId);
static bool source_exit_follows_or_equals(TransactionId, TransactionId);
static void source_exit_assert_xid_range(TransactionId);
static void source_exit_group(PGPROC *, TransactionId);
static void source_exit_member_wakeup(PGSemaphore);

static bool
source_exit_try_lock(LWLock *lock, LWLockMode mode)
{
	worker_pass_assert_unlocked();
	UT_ASSERT(lock == sampler_proc_lock && mode == LW_EXCLUSIVE);
	UT_ASSERT(!source_exit_lock_held && !sampler_lock_held && !census_step_lock_held);
	if (source_exit_group_requested) return false;
	source_exit_lock_held = true; source_exit_locks++;
	return true;
}

static bool
source_exit_owned(LWLock *lock)
{
	return source_exit_lock_held && lock == sampler_proc_lock;
}

static bool
source_exit_owned_mode(LWLock *lock, LWLockMode mode)
{
	return source_exit_owned(lock) && mode == LW_EXCLUSIVE;
}

static void
source_exit_unlock(LWLock *lock)
{
	UT_ASSERT(source_exit_owned(lock)); source_exit_lock_held = false; source_exit_unlocks++;
}

static void
source_exit_group_acquire(LWLock *lock, LWLockMode mode)
{
	if (!source_exit_group_requested) { UT_ASSERT(false); abort(); }
	worker_pass_assert_unlocked();
	UT_ASSERT(lock == sampler_proc_lock && mode == LW_EXCLUSIVE);
	UT_ASSERT(!source_exit_lock_held && !sampler_lock_held && !census_step_lock_held);
	UT_ASSERT(sampler_slots[0].procArrayGroupMember);
	UT_ASSERT_EQ(pg_atomic_read_u32(&sampler_procs.procArrayGroupFirst), 0);
	if (source_exit_member_requested)
	{
		PGPROC *member = &sampler_slots[1];
		FullTransactionId reference, future;
		ClusterXidStripeSampleFence fence;
		uint32 first = 0;

		/* Queue placement is a controlled interleaving. The original leader
		 * must perform both internal clears and the real post-unlock tail. */
		UT_ASSERT(!member->procArrayGroupMember && TransactionIdIsValid(member->xid));
		member->procArrayGroupMember = true;
		member->procArrayGroupMemberXid = member->xid + 2;
		pg_atomic_write_u32(&member->procArrayGroupNext, 0);
		UT_ASSERT(pg_atomic_compare_exchange_u32(&sampler_procs.procArrayGroupFirst, &first, 1));
		UT_ASSERT(original_native_fullxid_sample(&reference, &future, &source_exit_member_wait_minimum, &fence));
		UT_ASSERT(FullTransactionIdEquals(source_exit_member_wait_minimum,
			FullTransactionIdFromU64(pg_atomic_read_u64(&sampler_slots[0].cluster_top_full_xid_atomic))));
	}
	source_exit_lock_held = true; source_exit_locks++;
}

static void source_exit_unexpected_semaphore(PGSemaphore sema pg_attribute_unused())
{ UT_ASSERT(false); abort(); }
static void source_exit_unexpected_wait_start(uint32 event pg_attribute_unused())
{ UT_ASSERT(false); abort(); }
static void source_exit_unexpected_wait_end(void)
{ UT_ASSERT(false); abort(); }

#define ShmemVariableCache sampler_cache
#define ProcGlobal sampler_proc_global
#define MainLWLockArray sampler_locks
#define Mode source_exit_mode
#define RecoveryInProgress sampler_recovery_in_progress
#define LWLockConditionalAcquire source_exit_try_lock
#define LWLockAcquire source_exit_group_acquire
#define LWLockRelease source_exit_unlock
#define LWLockHeldByMe source_exit_owned
#define LWLockHeldByMeInMode source_exit_owned_mode
#define TransactionIdPrecedes source_exit_precedes
#define TransactionIdPrecedesOrEquals source_exit_precedes_or_equals
#define TransactionIdFollowsOrEquals source_exit_follows_or_equals
#undef AssertTransactionIdInAllowableRange
#define AssertTransactionIdInAllowableRange source_exit_assert_xid_range
#define FullXidRelativeTo source_exit_relative
#define MaintainLatestCompletedXid source_exit_maintain_latest
#define ProcArrayEndTransactionInternal source_exit_internal
#define ProcArrayEndTransaction source_exit_end_transaction
#define ProcArrayGroupClearXid source_exit_group
#include "test_cluster_heap_freeze_pgproc_exit.inc"
#define allProcs (sampler_procs.allProcs)
#define MyProc (&sampler_slots[0])
#define PGSemaphoreLock source_exit_unexpected_semaphore
#define PGSemaphoreUnlock source_exit_member_wakeup
#define pgstat_report_wait_start source_exit_unexpected_wait_start
#define pgstat_report_wait_end source_exit_unexpected_wait_end
#include "test_cluster_ctrc_native_group_exit.inc"
#undef pgstat_report_wait_end
#undef pgstat_report_wait_start
#undef PGSemaphoreUnlock
#undef PGSemaphoreLock
#undef MyProc
#undef allProcs
#undef ProcArrayGroupClearXid
#undef ProcArrayEndTransaction
#undef ProcArrayEndTransactionInternal
#undef MaintainLatestCompletedXid
#undef FullXidRelativeTo
#undef AssertTransactionIdInAllowableRange
#ifndef USE_ASSERT_CHECKING
#define AssertTransactionIdInAllowableRange(xid) ((void)true)
#endif
#undef TransactionIdFollowsOrEquals
#undef TransactionIdPrecedesOrEquals
#undef TransactionIdPrecedes
#undef LWLockHeldByMeInMode
#undef LWLockHeldByMe
#undef LWLockRelease
#undef LWLockAcquire
#undef LWLockConditionalAcquire
#undef RecoveryInProgress
#undef Mode
#undef MainLWLockArray
#undef ProcGlobal
#undef ShmemVariableCache

static void
worker_native_sample_fixture(const ClusterHeapFreezeCohortObservation *cohort)
{
	/* Construct typed registry inputs; this does not run allocation/2PC. */
	memset(&sampler_variables, 0, sizeof(sampler_variables));
	memset(&sampler_procs, 0, sizeof(sampler_procs));
	memset(sampler_slots, 0, sizeof(sampler_slots));
	memset(sampler_xids, 0, sizeof(sampler_xids));
	memset(sampler_array_storage, 0, sizeof(sampler_array_storage));
	sampler_cache = &sampler_variables; sampler_proc_global = &sampler_procs;
	sampler_proc_array = (ProcArrayStruct *) sampler_array_storage;
	sampler_variables.nextXid = FullTransactionIdFromU64(cohort->future_full_xid_max);
	sampler_procs.allProcs = sampler_slots; sampler_procs.xids = sampler_xids;
	sampler_proc_array->maxProcs = 3; sampler_proc_array->numProcs = 1;
	sampler_proc_array->pgprocnos[0] = 0;
	for (int i = 0; i < 3; i++) pg_atomic_init_u64(&sampler_slots[i].cluster_top_full_xid_atomic, 0);
	sampler_xids[0] = XidFromFullTransactionId(FullTransactionIdFromU64(cohort->active_full_xid_min));
	pg_atomic_write_u64(&sampler_slots[0].cluster_top_full_xid_atomic, cohort->active_full_xid_min);
	pass_reference = pass_future = sampler_variables.nextXid;
	pass_active_min = FullTransactionIdFromU64(cohort->active_full_xid_min);
	sampler_lock_held = sampler_recovering = sampler_advance_on_xid_unlock = false;
	sampler_held_lock = NULL; sampler_future_allowed = true;
	sampler_herding_slack = 64;
	sampler_xid_locks = sampler_proc_locks = 0;
	sampler_lock_acquires = sampler_lock_releases = sampler_future_reads = 0;
}

static bool
worker_pass_native_sample(FullTransactionId *reference, FullTransactionId *future,
						 FullTransactionId *minimum, ClusterXidStripeSampleFence *fence)
{
	worker_pass_assert_unlocked(); pass_native_reads++;
	UT_ASSERT_EQ(pass_capture_reads, pass_native_reads);
	*reference = *future = *minimum = InvalidFullTransactionId;
	memset(fence, 0, sizeof(*fence));
	if (!pass_native_allowed) return false;
	return original_native_fullxid_sample(reference, future, minimum, fence);
}

/* Original capacity/layout/ready functions; only configuration values and
 * actual allocation/publication are supplied by this standalone fixture. */
static int census_runtime_buffers = 4, census_runtime_backends = 2;
static int census_runtime_nodes = 2;
static int
census_runtime_declared_nodes(void)
{
	return census_runtime_nodes;
}
static bool ctrc_bytes_zero(const void *data, Size bytes);
#define NBuffers census_runtime_buffers
#define MaxBackends census_runtime_backends
#define cluster_conf_declared_node_count_early census_runtime_declared_nodes
#define cluster_ctrc_capacity_compute original_census_capacity_compute
#define cluster_ctrc_shmem_ready original_census_shmem_ready
#include "test_cluster_heap_ctrc_ready.inc"
#undef cluster_ctrc_shmem_ready
#undef cluster_ctrc_capacity_compute
#undef cluster_conf_declared_node_count_early
#undef MaxBackends
#undef NBuffers

/* Original initializer state and lock initialization. Shared-memory lookup
 * remains an explicit malloc-backed leaf, with native publication logic intact. */
#include "test_cluster_heap_ctrc_init_state.inc"
#define LWLockInitialize original_census_lock_initialize
#include "test_cluster_heap_ctrc_lwlock_init.inc"
#undef LWLockInitialize
static void *census_initializer_memory;
static Size census_initializer_bytes;
static bool census_initializer_found;
static unsigned census_initializer_calls, census_initializer_lock_calls;
static bool census_initializer_locks[CLUSTER_CTRC_CLEANER_WORKERS + 2];
static int census_initializer_error_level, census_initializer_error_code;

static void
census_initializer_reset_trace(void)
{
	census_initializer_calls = census_initializer_lock_calls = 0;
	census_initializer_error_level = census_initializer_error_code = 0;
	memset(census_initializer_locks, 0, sizeof(census_initializer_locks));
}

static void *
census_initializer_shmem_init(const char *name, Size bytes, bool *found)
{
	worker_pass_assert_unlocked(); census_initializer_calls++;
	UT_ASSERT(strcmp(name, "pgrac cluster terminal reference census") == 0);
	UT_ASSERT(census_initializer_memory != NULL && found != NULL);
	UT_ASSERT_EQ(bytes, census_initializer_bytes);
	*found = census_initializer_found;
	return census_initializer_memory;
}

static void
census_initializer_lock_initialize(LWLock *lock, int tranche)
{
	unsigned ordinal = CLUSTER_CTRC_CLEANER_WORKERS + 2;
	int expected_tranche = -1;
	worker_pass_assert_unlocked();
	if (lock == &CtrcShared->origin_lock) {
		ordinal = 0; expected_tranche = LWTRANCHE_CLUSTER_CTRC_ORIGIN;
	} else if (lock == &CtrcShared->receipt_lock) {
		ordinal = CLUSTER_CTRC_CLEANER_WORKERS + 1;
		expected_tranche = LWTRANCHE_CLUSTER_CTRC_RECEIPT;
	} else for (unsigned i = 0; i < CLUSTER_CTRC_CLEANER_WORKERS; i++) {
		if (lock == &CtrcShared->participant_locks[i].lock) {
			ordinal = i + 1; expected_tranche = LWTRANCHE_CLUSTER_CTRC_PARTICIPANT; break;
		}
	}
	UT_ASSERT(ordinal < lengthof(census_initializer_locks));
	if (ordinal >= lengthof(census_initializer_locks)) return;
	UT_ASSERT_EQ(tranche, expected_tranche); UT_ASSERT(!census_initializer_locks[ordinal]);
	census_initializer_locks[ordinal] = true; census_initializer_lock_calls++;
	original_census_lock_initialize(lock, tranche);
}

static bool
census_initializer_errstart(int level, const char *domain)
{
	census_initializer_error_level = level;
	return errstart(level, domain);
}

static int
census_initializer_errcode(int code)
{
	census_initializer_error_code = code;
	return errcode(code);
}

#define ShmemInitStruct census_initializer_shmem_init
#define LWLockInitialize census_initializer_lock_initialize
#define errstart census_initializer_errstart
#define errstart_cold census_initializer_errstart
#define errcode census_initializer_errcode
#define cluster_ctrc_shmem_init original_census_shmem_init
#include "test_cluster_heap_ctrc_init.inc"
#undef cluster_ctrc_shmem_init
#undef errcode
#undef errstart_cold
#undef errstart
#undef LWLockInitialize
#undef ShmemInitStruct

static bool
worker_census_shmem_ready(void)
{
	worker_pass_assert_unlocked(); pass_census_ready_reads++;
	if (census_runtime_ready_requested)
		return registration_attached && original_census_shmem_ready();
	/* Legacy arithmetic/row fixtures deliberately use small controlled layouts;
	 * the new full-readiness cases allocate the complete original layout. */
	return registration_attached && CtrcShared != NULL;
}
#define cluster_ctrc_shmem_ready worker_census_shmem_ready
#define cluster_ctrc_active_scan_begin original_census_scan_begin
#include "test_cluster_heap_ctrc_begin.inc"
#undef cluster_ctrc_active_scan_begin
#undef cluster_ctrc_shmem_ready

static bool
worker_pass_census_begin(ClusterCtrcActiveScan *scan, FullTransactionId reference)
{
	worker_pass_assert_unlocked(); pass_begin_reads++;
	UT_ASSERT_EQ(pass_native_reads, pass_begin_reads);
	if (!pass_begin_allowed) return false;
	return original_census_scan_begin(scan, reference);
}

static bool
census_step_conditional_lock(LWLock *lock, LWLockMode mode)
{
	bool found = false;
	worker_pass_assert_unlocked(); UT_ASSERT(mode == LW_SHARED);
	census_step_lock_reads++;
	if (pass_incomplete_slices != 0) { pass_incomplete_slices--; return false; }
	if (census_step_block_at == census_step_lock_reads) return false;
	if (lock == &CtrcShared->origin_lock) census_step_origin_locks++;
	else {
		for (unsigned i = 0; i < CLUSTER_CTRC_CLEANER_WORKERS; i++) {
			if (lock == &CtrcShared->participant_locks[i].lock) {
				census_step_participant_locks[i]++; found = true; break;
			}
		}
		UT_ASSERT(found);
	}
	census_step_lock_held = true; census_step_held_lock = lock;
	census_step_lock_acquires++; return true;
}

static void
census_step_unlock(LWLock *lock)
{
	UT_ASSERT(census_step_lock_held && lock == census_step_held_lock);
	census_step_lock_held = false; census_step_held_lock = NULL; census_step_lock_releases++;
	if (census_step_revoke_at == census_step_lock_releases) ctrc_note_unqualified_registration();
}

#define LWLockConditionalAcquire census_step_conditional_lock
#define LWLockRelease census_step_unlock
#define cluster_xid_widen original_census_xid_widen
#define ctrc_active_scan_step original_census_step_internal
#define cluster_ctrc_active_scan_step_source original_census_step_source
#include "test_cluster_heap_ctrc_step.inc"
#undef cluster_ctrc_active_scan_step_source
#undef ctrc_active_scan_step
#undef cluster_xid_widen
#undef LWLockRelease
#undef LWLockConditionalAcquire

static void
worker_pass_census_step(ClusterCtrcActiveScan *scan, const ClusterSemanticActiveSampleFence *source)
{
	worker_pass_assert_unlocked(); pass_step_reads++;
	/* A refused capture/begin cannot be revived by a traversal leaf. */
	if (scan->status != CTRC_ACTIVE_SCAN_INCOMPLETE) return;
	UT_ASSERT(memcmp(source, &pass_captured_admission, sizeof(*source)) == 0);
	original_census_step_source(scan, source);
	switch (pass_step_fault) {
	case 1: pg_atomic_write_u32(&StripeLease->failed, 1); break;
	case 2: pg_atomic_write_u32(&native_admission_shared.transition_closed, 1); break;
	case 3: scan->status = CTRC_ACTIVE_SCAN_INVALID; break;
	case 4: ctrc_note_unqualified_registration(); break;
	}
}

static bool
worker_pass_lease_current(const ClusterXidStripeSampleFence *fence)
{
	worker_pass_assert_unlocked(); pass_lease_checks++;
	return original_retained_lease_current(fence);
}

static bool
worker_pass_admission_current(const ClusterSemanticActiveSampleFence *fence)
{
	worker_pass_assert_unlocked(); pass_admission_checks++;
	return original_admission_source_current(fence);
}

#define undo_cleaner_active_scan publisher_scan
#define undo_cleaner_active_lease_fence publisher_lease
#define undo_cleaner_active_admission_fence publisher_admission
#define undo_cleaner_active_scan_pending pass_pending
#define cluster_semantic_activation_active_sample_capture worker_pass_capture
#define ProcArrayGetActiveFullXidSample worker_pass_native_sample
#define cluster_ctrc_active_scan_begin worker_pass_census_begin
#define cluster_ctrc_active_scan_step_source worker_pass_census_step
#define cluster_xid_stripe_sample_fence_current worker_pass_lease_current
#define cluster_semantic_activation_active_sample_current worker_pass_admission_current
#define undo_cleaner_publish_active_sample original_worker_publication_publish
#define undo_cleaner_sample_active_pass original_worker_sample_active_pass
#include "test_cluster_heap_freeze_worker_pass.inc"
#undef undo_cleaner_sample_active_pass
#undef undo_cleaner_publish_active_sample
#undef cluster_semantic_activation_active_sample_current
#undef cluster_xid_stripe_sample_fence_current
#undef cluster_ctrc_active_scan_step_source
#undef cluster_ctrc_active_scan_begin
#undef ProcArrayGetActiveFullXidSample
#undef cluster_semantic_activation_active_sample_capture
#undef undo_cleaner_active_scan_pending
#undef undo_cleaner_active_admission_fence
#undef undo_cleaner_active_lease_fence
#undef undo_cleaner_active_scan

static void
worker_original_sample_pass_fixture(const ClusterHeapFreezeCohortObservation *cohort)
{
	/* Input leaves only; the original coordinator initializes its own cursor,
	 * captures source, and invokes the original shared publisher. */
	publisher_worker = 0; publisher_pid = 42;
	pass_reference = pass_future = FullTransactionIdFromU64(cohort->future_full_xid_max);
	pass_active_min = FullTransactionIdFromU64(cohort->active_full_xid_min);
	original_worker_sample_active_pass();
}

static void
worker_current_publish_fixture(const ClusterHeapFreezeCohortObservation *cohort)
{
	/* Fixture construction, not the production publisher or an authority grant. */
	ClusterUndoCleanerActiveSample *sample = &native_worker_shared.active_sample;
	memset(sample, 0, sizeof(*sample));
	sample->sample_id = cohort->source_local_sample_id;
	sample->worker_pid = 42;
	sample->lease_fence = retained_lease_fence;
	sample->scan = cohort->local_registration_cut;
	sample->admission_fence = cohort->admission_fence;
	native_worker_shared.workers[0].pid = 42;
	native_worker_shared.workers[0].status = UNDO_CLEANER_READY;
}

static bool
cohort_worker_publication_current(const ClusterUndoCleanerActiveSample *active)
{
	UT_ASSERT(!local_x && !local_share && backend_pin_count == fixture_expected_backend_pins());
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(active->sample_id, cohort_source_input->source_local_sample_id);
	UT_ASSERT(memcmp(&active->scan, &cohort_source_input->local_registration_cut, sizeof(active->scan)) == 0);
	cohort_worker_current_checks++;
	return cohort_worker_live && original_worker_publication_current(active);
}

static bool
cohort_admission_source_current(const ClusterSemanticActiveSampleFence *fence)
{
	UT_ASSERT(fence == &cohort_source_input->admission_fence);
	UT_ASSERT(!local_x && !local_share && backend_pin_count == fixture_expected_backend_pins());
	cohort_semantic_current_checks++;
	return cohort_admission_live && original_admission_source_current(fence);
}

static bool
cohort_peer_generation_current(int32 peer, uint32 capability, uint32 generation)
{
	UT_ASSERT(peer >= 0 && peer < CLUSTER_MAX_NODES);
	UT_ASSERT_EQ(capability, PGRAC_IC_HELLO_CAP_HEAP_FREEZE_REPORT_V2);
	UT_ASSERT(!local_x && !local_share && backend_pin_count == fixture_expected_backend_pins());
	cohort_peer_current_checks++;
	if (cohort_peer_revoke_worker) cohort_worker_live = false;
	return generation == cohort_live_generation[peer];
}

static TimestampTz
cohort_current_end_clock(void)
{
	UT_ASSERT(!local_x && !local_share && backend_pin_count == fixture_expected_backend_pins());
	UT_ASSERT_EQ(CritSectionCount, 0);
	cohort_end_clock_reads++;
	if (cohort_clock_error) elog(ERROR, "injected original cohort end-clock ERROR");
	if (cohort_clock_revoke_worker) cohort_worker_live = false;
	if (cohort_clock_revoke_admission) cohort_admission_live = false;
	return cohort_end_clock;
}

static bool undo_horizon_v2_required(const ClusterSemanticActiveSampleFence *, int32);
#define cluster_undo_cleaner_active_sample_current cohort_worker_publication_current
#define cluster_semantic_activation_active_sample_current cohort_admission_source_current
#define cluster_sf_peer_capability_generation_matches cohort_peer_generation_current
#define GetCurrentTimestamp cohort_current_end_clock
#define cluster_lmon_main_loop_interval cohort_interval_ms
#define undo_horizon_v2_cohort_current actual_retained_cohort_current
#include "test_cluster_heap_freeze_cohort_backend.inc"
#undef undo_horizon_v2_cohort_current
#undef cluster_lmon_main_loop_interval
#undef GetCurrentTimestamp
#undef cluster_sf_peer_capability_generation_matches
#undef cluster_semantic_activation_active_sample_current
#undef cluster_undo_cleaner_active_sample_current
static ClusterUndoHorizonShmem cohort_shared;

/* Preserve old refusal-status injection. Every READY continuation now passes
 * through the original backend; this adapter never manufactures READY. */
static ClusterHeapFreezeCohortStatus
undo_horizon_v2_cohort_current(const ClusterSemanticActiveSampleFence *fence,
	const ClusterHeapFreezeCohortPeerObservation *peers, TimestampTz started,
	uint32 interval_ms, const ClusterUndoCleanerActiveSample *active)
{
	UT_ASSERT(!local_x && !local_share && backend_pin_count == fixture_expected_backend_pins());
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT(fence == &cohort_source_input->admission_fence);
	UT_ASSERT(peers == cohort_source_input->source_peers);
	UT_ASSERT_EQ(started, cohort_source_input->source_started_at_us);
	UT_ASSERT_EQ(interval_ms, cohort_source_input->source_interval_ms);
	UT_ASSERT_EQ(active->sample_id, cohort_source_input->source_local_sample_id);
	cohort_backend_checks++;
	if (cohort_revoke_ingress) ctrc_note_unqualified_registration();
	if (saved_cohort_status != CLUSTER_HEAP_FREEZE_COHORT_READY) return saved_cohort_status;
	return actual_retained_cohort_current(fence, peers, started, interval_ms, active);
}

#define cluster_heap_freeze_cohort_current actual_saved_cohort_current
#include "test_cluster_heap_freeze_cohort_current.inc"
#undef cluster_heap_freeze_cohort_current

ClusterHeapFreezeCohortStatus
cluster_heap_freeze_cohort_current(const ClusterHeapFreezeCohortObservation *cohort)
{
	UT_ASSERT(!local_x && !local_share && backend_pin_count == fixture_expected_backend_pins());
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + local_lock_attempts, phase_prior_lock_activity);
	UT_ASSERT_EQ(inserts + dirties + flushes + wal_clock_reads, 0);
	UT_ASSERT_EQ(policy_current_checks, 1);
	UT_ASSERT_EQ(census_preflight_checks, 0);
	if (expected_current_cohort != NULL) UT_ASSERT(cohort == expected_current_cohort);
	cohort_current_checks++;
	if (source_current_error_stage == 2) elog(ERROR, "injected saved-cohort current error");
	if (source_current_revoke_census == 2) admission_current = false;
	cohort_source_input = cohort;
	return actual_saved_cohort_current(cohort);
}

/* Existing full TARGET policy is stronger than census-read permission. */
bool
cluster_semantic_activation_recheck(const ClusterSemanticAdmissionToken *token)
{
	if (!local_x) {
		target_preflight_checks++;
		UT_ASSERT(token != NULL && token->entered && backend_pin_count == fixture_expected_backend_pins());
		UT_ASSERT_EQ(CritSectionCount, 0);
		UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + local_lock_attempts, phase_prior_lock_activity);
		UT_ASSERT_EQ(writer_checks + route_checks + authority_checks + admission_checks, phase_prior_authority_activity);
		UT_ASSERT_EQ(wal_clock_reads + inserts + dirties + edges + flushes, 0);
		if (preflight_target_error) elog(ERROR, "injected unlocked TARGET preflight error");
		if (preflight_revoke_census) admission_current = false;
		return (full_target_current || allow_preflight_target) && fixture_native_admission_current(token, 1);
	}
	if (space_observation_checks != 0) {
		post_space_target_checks++;
		UT_ASSERT(token != NULL && token->entered && local_x && backend_pin_count == fixture_expected_backend_pins());
		UT_ASSERT_EQ(full_target_checks, 1);
		UT_ASSERT_EQ(admission_checks, 1);
		UT_ASSERT_EQ(wal_clock_reads, 1);
		UT_ASSERT_EQ(CritSectionCount, 0);
		UT_ASSERT_EQ(inserts + dirties + edges + flushes, 0);
		if (target_recheck_error) {
			post_space_error_seen = 1;
			elog(ERROR, "injected final TARGET ERROR after SPACE");
		}
		if (change_source_after_space_target == 1) ctrc_note_unqualified_registration();
		if (post_space_bounds != NULL) {
			post_space_bounds->horizon_scn--;
			post_space_bounds->source_local_horizon_scn--;
		}
		if (post_space_policy != NULL)
			post_space_policy->legal_next = FullTransactionIdFromU64(
				U64FromFullTransactionId(post_space_policy->legal_next) + 1);
		return full_target_current && fixture_native_admission_current(token, 5);
	}
	full_target_checks++;
	if (advance_registration_after_target) ctrc_note_unqualified_registration();
	UT_ASSERT(token != NULL && token->entered);
	UT_ASSERT(local_x && backend_pin_count == fixture_expected_backend_pins());
	UT_ASSERT_EQ(CritSectionCount, 0);
	/* Native final authority + real capture; a resumed scope checked once more. */
	UT_ASSERT_EQ(writer_checks, phase_prior_writer_checks + (resumed_writer ? 3 : 2));
	UT_ASSERT_EQ(route_checks, phase_prior_route_checks + 1);
	UT_ASSERT_EQ(authority_checks, phase_prior_authority_checks + 1);
	UT_ASSERT_EQ(admission_checks, 0);
	UT_ASSERT_EQ(wal_clock_reads + inserts + dirties + edges + flushes, 0);
	if (target_recheck_error) elog(ERROR, "injected full TARGET recheck error");
	if (revoke_target_census) admission_current = false;
	if (retire_bounds_after_target != NULL)
		retire_bounds_after_target->horizon_scn++;
	if (advance_legal_next_after_target != NULL)
		advance_legal_next_after_target->legal_next = FullTransactionIdFromU64(
			U64FromFullTransactionId(advance_legal_next_after_target->legal_next) + 1);
	return full_target_current && fixture_native_admission_current(token, 3);
}

void
XLogFlush(XLogRecPtr lsn)
{
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(lsn, UINT64_C(0x9000));
	UT_ASSERT_EQ(dirties, 1);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT(local_x && writer_current);
	flushes++;
	if (flush_error)
		elog(ERROR, "injected WAL flush failure");
}


void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# assertion %s at %s:%d\n", condition, file, line);
	abort();
}
bool
errstart(int level, const char *domain)
{
	(void)domain;
	return level >= ERROR;
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
int
errmsg_internal(const char *format, ...)
{
	(void)format;
	return 0;
}
int
errmsg(const char *format, ...)
{
	(void)format;
	return 0;
}
int
errhint(const char *format, ...)
{
	(void)format;
	return 0;
}

int
errcode(int code)
{
	(void)code;
	return 0;
}
void
errfinish(const char *file, int line, const char *function)
{
	if (!expecting_error) {
		printf("# unexpected error %s:%d %s\n", file, line, function);
		abort();
	}
	pg_re_throw();
}
bool
RecoveryInProgress(void)
{
	return false;
}
bool
IsCatalogRelation(Relation rel)
{
	UT_ASSERT(rel == &relation_data);
	return false;
}
bool
cluster_wal_thread_current_v2_ref(ClusterWalSourceRef *out)
{
	memset(out, 0, sizeof(*out));
	out->claim.identity.system_identifier = 11;
	out->claim.database_incarnation = 12;
	memset(out->claim.identity.storage_uuid, 13, 16);
	return true;
}
bool
cluster_wal_thread_restart_v2_ref(ClusterWalSourceRef *out)
{
	(void)out;
	abort();
}
int
cluster_smgr_which_for(RelFileLocator locator, BackendId backend)
{
	UT_ASSERT_EQ(locator.dbOid, 5);
	UT_ASSERT_EQ(backend, InvalidBackendId);
	return 1;
}
void
BufferGetTag(Buffer buffer, RelFileLocator *locator, ForkNumber *forknum, BlockNumber *block)
{
	UT_ASSERT_EQ(buffer, 1);
	*locator = (RelFileLocator){ 1663, 5, 900 };
	*forknum = MAIN_FORKNUM;
	*block = 7;
}
bool
BufferIsPermanent(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	space_observation_checks++;
	if (revoke_admission_in_space == 1) full_target_current = false;
	if (revoke_admission_in_space == 2) admission_current = false;
	if (revoke_admission_in_space == 3) target_recheck_error = true;
	if (revoke_admission_in_space == 4) post_space_census_error = true;
	if (space_permanence_error)
		elog(ERROR, "injected SPACE buffer observation ERROR");
	if (advance_registration_in_space) ctrc_note_unqualified_registration();
	/* The actual SPACE builder reaches this original observation leaf only
	 * after the continuation's earlier TARGET/F/H checks. */
	if (replace_bounds_in_space != NULL) {
		UT_ASSERT_EQ(CritSectionCount, 0);
		if (replace_bound_kind == 0)
			replace_bounds_in_space->active_full_xid_min--;
		else {
			replace_bounds_in_space->horizon_scn--;
			replace_bounds_in_space->source_local_horizon_scn--;
		}
	}
	if (replace_policy_in_space != NULL) {
		UT_ASSERT_EQ(CritSectionCount, 0);
		replace_policy_in_space->legal_next = FullTransactionIdFromU64(
			U64FromFullTransactionId(replace_policy_in_space->legal_next) + 1);
	}
	return space_buffer_permanent;
}
SCN
cluster_scn_advance(void)
{
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	return ++next_token;
}
bool
TransactionIdDidCommit(TransactionId xid)
{
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(xid, 77);
	tx_checks++;
	return tx_committed;
}
void
MarkBufferDirty(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	if (redoing) {
		UT_ASSERT(redo_locked);
		UT_ASSERT_EQ(CritSectionCount, 0);
		redo_dirties++;
		return;
	}
	UT_ASSERT_EQ(CritSectionCount, 1);
	dirties++;
}
void
MarkBufferDirtyHint(Buffer buffer, bool standard)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(standard);
	hints++;
}
void
XLogBeginInsert(void)
{
	UT_ASSERT(!begun && dirties == 1);
	UT_ASSERT_EQ(CritSectionCount, 1);
	begun = true;
}
void
XLogRegisterData(char *data, uint32 length)
{
	UT_ASSERT(begun);
	if (expected_info == XLOG_HEAP2_PRUNE) {
		UT_ASSERT_EQ(length, SizeOfHeapPrune);
		UT_ASSERT_EQ(((xl_heap_prune *)data)->nredirected, 1);
	} else if (expected_info == XLOG_HEAP2_FREEZE_PAGE) {
		UT_ASSERT_EQ(length, SizeOfHeapFreezePage);
		UT_ASSERT_EQ(((xl_heap_freeze_page *)data)->nplans, expected_nplans);
		memcpy(freeze_main_data.data, data, length);
	} else {
		UT_ASSERT_EQ(length, SizeOfHeapVacuum);
		UT_ASSERT_EQ(((xl_heap_vacuum *)data)->nunused, 1);
	}
}
void
XLogRegisterBuffer(uint8 id, Buffer buffer, uint8 flags)
{
	UT_ASSERT(begun);
	UT_ASSERT_EQ(id, 0);
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT_EQ(flags,
				 REGBUF_STANDARD | (expected_info == XLOG_FPI_FOR_HINT ? REGBUF_FORCE_IMAGE : 0));
	memcpy(wal_image.data, current_page.data, BLCKSZ);
}
void
XLogRegisterBufData(uint8 id, char *data, uint32 length)
{
	UT_ASSERT(begun && data != NULL && length > 0);
	UT_ASSERT_EQ(id, 0);
	if (expected_info == XLOG_HEAP2_FREEZE_PAGE) {
		UT_ASSERT(freeze_block_length + length <= BLCKSZ);
		memcpy(freeze_block_data.data + freeze_block_length, data, length);
		freeze_block_length += length;
		freeze_data_calls++;
	}
}
void
XLogRegisterPageVersionEdge(uint64 token, const RfPageVersionEdgeEntryV1 *entries, uint8 count)
{
	UT_ASSERT(begun);
	UT_ASSERT_EQ(count, 1);
	edge = entries[0];
	edge_token = token;
	edges++;
}
XLogRecPtr
XLogInsert(RmgrId rmgr, uint8 info)
{
	UT_ASSERT(begun);
	UT_ASSERT_EQ(rmgr, expected_info == XLOG_FPI_FOR_HINT ? RM_XLOG_ID : RM_HEAP2_ID);
	UT_ASSERT_EQ(info, expected_info);
	begun = false;
	inserted_wal_clock = fixture_wal_clock;
	inserts++;
	return UINT64_C(0x9000);
}

static void page_verify_redirects(Page page);
#include "test_cluster_heap_prune_helpers.inc"
#include "test_cluster_heap_freeze_helpers.inc"

/* Plans here are fixture-qualified, not shared FullXid/provenance evidence. */
static bool
heap_freeze_apply_prepared(Relation rel, Buffer buffer,
						   TransactionId snapshotConflictHorizon,
						   HeapTupleFreeze *tuples, int ntuples,
						   const ClusterSpaceIdentity *identity, bool conditional,
						   const ClusterHeapFreezePageInputV1 *freeze_input,
						   const ClusterSemanticAdmissionToken *freeze_admission,
						   const ClusterHeapFreezeObservedBoundsV1 *expected_bounds,
						   FullTransactionId expected_legal_next)
#include "test_cluster_heap_freeze_apply.inc"

#include "test_cluster_heap_freeze_xmin_apply.inc"

/* Execute the full native freeze body with the planned pre-lock identity. */
static void
run_freeze(Relation rel, Buffer buffer, TransactionId snapshotConflictHorizon,
		   HeapTupleFreeze *tuples, int ntuples,
		   const ClusterSpaceIdentity *identity pg_attribute_unused())
#include "test_cluster_heap_freeze_body.inc"

XLogRedoAction
XLogReadBufferForRedo(XLogReaderState *record, uint8 id, Buffer *buffer)
{
	UT_ASSERT(redoing && !redo_locked);
	UT_ASSERT_EQ(id, 0);
	UT_ASSERT_EQ(record->EndRecPtr, UINT64_C(0x9000));
	*buffer = 1;
	redo_locked = true;
	return BLK_NEEDS_REDO;
}

char *
XLogRecGetBlockData(XLogReaderState *record, uint8 id, Size *length)
{
	UT_ASSERT(redoing && redo_locked && record != NULL);
	UT_ASSERT_EQ(id, 0);
	if (length != NULL)
		*length = freeze_block_length;
	return freeze_block_data.data;
}

void
XLogRecGetBlockTag(XLogReaderState *record, uint8 id, RelFileLocator *locator,
				   ForkNumber *forknum, BlockNumber *block)
{
	/* Hot-standby conflict handling is outside this native replay fixture. */
	(void)record;
	(void)id;
	(void)locator;
	(void)forknum;
	(void)block;
	abort();
}

void
ResolveRecoveryConflictWithSnapshot(TransactionId xid, bool catalog, RelFileLocator locator)
{
	(void)xid;
	(void)catalog;
	(void)locator;
	abort();
}

void
UnlockReleaseBuffer(Buffer buffer)
{
	UT_ASSERT(redoing && redo_locked);
	UT_ASSERT_EQ(buffer, 1);
	redo_locked = false;
	redo_releases++;
}

#include "test_cluster_heap_freeze_redo.inc"

	/* Native eligibility/retention is covered by test_cluster_r4_lock_order.
 * Here the exact approved redirect/remove plan enters the real WAL section. */
	static void run_prune(bool change, const ClusterSpaceIdentity *identity pg_attribute_unused())
{
	Relation relation = &relation_data;
	Buffer buffer = 1;
	Page page = current_page.data;
	bool versioned pg_attribute_unused() = cluster_shared_config;
	RfPageProducerBatchV1 version_batch pg_attribute_unused();
	struct {
		int nredirected, ndead, nunused;
		OffsetNumber redirected[2], nowdead[1], nowunused[1];
		TransactionId new_prune_xid, snapshotConflictHorizon;
	} prstate = { 0 };

	if (change) {
		prstate.nredirected = prstate.nunused = 1;
		prstate.redirected[0] = 1;
		prstate.redirected[1] = 3;
		prstate.nowunused[0] = 2;
	}
#include "test_cluster_heap_prune_version.inc"
}

/* Only the native LP_DEAD mutation/WAL section is included. The later VM
 * decision is a separate producer and is deliberately not simulated here. */
static void
run_vacuum(const ClusterSpaceIdentity *identity)
{
	struct {
		int num_items;
		ItemPointerData items[1];
	} items = { 1 }, *dead_items = &items;
	struct {
		Relation rel;
		bool versioned;
		bool shared_maintenance_restricted;
		ClusterSpaceIdentity identity;
	} state = { 0 }, *vacrel = &state;
	Buffer buffer = 1;
	BlockNumber blkno = 7;
	Page page = current_page.data;
	int index = 0, nunused = 0, saved_err_info = 0;
	OffsetNumber unused[MaxHeapTuplesPerPage];
	RfPageProducerBatchV1 version_batch pg_attribute_unused();

	vacrel->rel = &relation_data;
	vacrel->versioned = cluster_shared_config;
	if (identity != NULL)
		vacrel->identity = *identity;
	ItemPointerSet(&items.items[0], 7, 3);
#define update_vacuum_error_info(a, b, c, d, e) ((void)(a), (void)(b), (void)(d), (void)(e))
#include "test_cluster_heap_vacuum_version.inc"
#undef update_vacuum_error_info
	UT_ASSERT_EQ(index, 1);
}

static void
reset(uint8 opcode, bool shared)
{
	PGAlignedBlock tuple_bytes;
	HeapTupleHeader tuple = (HeapTupleHeader)tuple_bytes.data;
	memset(&relation_data, 0, sizeof(relation_data));
	memset(&relform, 0, sizeof(relform));
	memset(&space_identity, 0, sizeof(space_identity));
	memset(&edge, 0, sizeof(edge));
	memset(tuple_bytes.data, 0, BLCKSZ);
	BufferBlocks = current_page.data;
	cluster_shared_config = shared;
	cluster_enabled = shared;
	CritSectionCount = 0;
	edges = inserts = dirties = hints = flushes = 0;
	writer_checks = route_checks = authority_checks = admission_checks = 0;
	fixture_wal_clock = scn_encode(0, 100);
	inserted_wal_clock = InvalidScn;
	wal_clock_reads = 0;
	full_target_current = true;
	native_original_admission_enabled = false; native_admission_expected_token = NULL;
	bounds_start_original_enabled = false;
	bounds_start_cluster_enabled = true; bounds_start_shared_dir = "/unit/shared/startup";
	bounds_start_publish_state(CLUSTER_NORMAL_START_TARGET_READY);
	native_start_change_at = native_start_fault = 0;
	native_admission_stage = native_admission_fault_at = native_admission_fault = 0;
	memset(native_admission_checks, 0, sizeof(native_admission_checks));
	target_recheck_error = revoke_target_census = false;
	full_target_checks = 0;
	target_preflight_checks = census_preflight_checks = 0;
	policy_current_checks = cohort_current_checks = 0;
	allocation_fence_checks = 0; allocation_fence_error = false;
	lease_cluster_on = lease_catalog_on = lease_quorum = true; lease_lock_held = false;
	lease_incarnation = 9; lease_slot = 0; lease_after_policy_fault = -1;
	lease_revoke_after_unlock = lease_revoke_kind = 0;
	lease_lock_acquires = lease_lock_releases = lease_incarnation_reads = lease_quorum_reads = 0;
	lease_error_read = 0;

	cluster_xid_striping = true;
	saved_policy_current = true; saved_cohort_status = CLUSTER_HEAP_FREEZE_COHORT_READY;
	memset(&cohort_shared, 0, sizeof(cohort_shared));
	UndoHorizonShmem = &cohort_shared; cohort_source_input = NULL;
	for (int peer = 0; peer < CLUSTER_MAX_NODES; peer++) {
		pg_atomic_init_u32(&cohort_shared.freeze_slots[peer].seq, 2);
		cohort_live_generation[peer] = 7;
	}
	memset(&native_worker_shared, 0, sizeof(native_worker_shared));
	undo_cleaner_state = &native_worker_shared;
	admission_recovering = admission_stopping = false;
	admission_serving = admission_join_allowed = admission_quorum = admission_membership_available = true;
	admission_membership_fault = 0; admission_membership_reads = 0;
	worker_current_enabled = worker_storage_ok = worker_lease_ok = worker_admission_ok = true;
	worker_stop_cut = worker_lease_error = worker_admission_error = false;
	worker_lock_held = worker_register_on_unlock = false;
	worker_lock_acquires = worker_lock_releases = worker_lease_checks = worker_admission_checks = 0;
	getter_copy_acquires = getter_copy_releases = 0; getter_copy_fault = 0;
	publisher_worker = 0; publisher_pid = 42; publisher_unlock_fault = 0;
	publisher_lock_acquires = publisher_lock_releases = 0;
	memset(&publisher_scan, 0, sizeof(publisher_scan));
	memset(&publisher_lease, 0, sizeof(publisher_lease));
	memset(&publisher_admission, 0, sizeof(publisher_admission));
	pass_capture_allowed = pass_native_allowed = pass_begin_allowed = true;
	pass_pending = false; pass_step_fault = 0; pass_incomplete_slices = 0;
	pass_capture_reads = pass_native_reads = pass_begin_reads = pass_step_reads = 0;
	pass_census_ready_reads = 0;
	census_step_lock_held = false; census_step_held_lock = NULL;
	census_step_lock_reads = census_step_lock_acquires = census_step_lock_releases = 0;
	census_step_origin_locks = census_step_block_at = census_step_revoke_at = 0;
	memset(census_step_participant_locks, 0, sizeof(census_step_participant_locks));
	pass_lease_checks = pass_admission_checks = 0;
	pass_reference = pass_future = pass_active_min = InvalidFullTransactionId;
	memset(&pass_captured_admission, 0, sizeof(pass_captured_admission));
	cohort_worker_live = cohort_admission_live = true;
	cohort_clock_error = cohort_peer_revoke_worker = false;
	cohort_clock_revoke_worker = cohort_clock_revoke_admission = false;
	cohort_worker_current_checks = cohort_semantic_current_checks = 0;
	cohort_peer_current_checks = cohort_end_clock_reads = 0;
	cohort_interval_ms = CLUSTER_UNDO_HORIZON_INTERVAL_MIN_MS;
	cohort_end_clock = 1000000;
	cohort_sample_available = true; cohort_sample_error = cohort_revoke_ingress = false;
	cohort_sample_fault = 0; cohort_sample_reads = cohort_backend_checks = 0;
	source_current_error_stage = source_current_revoke_census = 0;
	expected_current_policy = NULL; expected_current_cohort = NULL;
	preflight_target_error = preflight_revoke_census = allow_preflight_target = false;
	retire_bounds_after_target = NULL;
	advance_legal_next_after_target = NULL;
	CtrcShared = &registration_shared;
	registration_attached = true;
	census_runtime_ready_requested = census_runtime_initialize_requested = false;
	census_initializer_memory = NULL; census_initializer_bytes = 0; census_initializer_found = false;
	census_initializer_reset_trace();
	census_runtime_buffers = 4; census_runtime_backends = census_runtime_nodes = 2;
	memset(&registration_arrays, 0, sizeof(registration_arrays));
	CtrcShared->magic = CLUSTER_CTRC_SHMEM_MAGIC;
	CtrcShared->version = CLUSTER_CTRC_SHMEM_VERSION;
	CtrcShared->origin_key_entries = CtrcShared->participant_key_entries = 1;
	CtrcShared->origin_offset = offsetof(NativeCensusArrayFixture, origins);
	CtrcShared->participant_offset = offsetof(NativeCensusArrayFixture, participants);
	pg_atomic_init_u64(&CtrcShared->unqualified_registration_generation, 41);
	registration_checks = 0;
	advance_registration_after_target = advance_registration_in_clock = false;
	advance_registration_in_space = false; space_observation_checks = 0;
	post_space_target_checks = post_space_census_checks = post_space_error_seen = 0;
	revoke_admission_in_space = change_source_after_space_target = 0;
	post_space_census_error = false;
	post_space_bounds = NULL; post_space_policy = NULL;
	replace_bounds_in_space = NULL; replace_policy_in_space = NULL; replace_bound_kind = 0;
	local_x = writer_current = route_current = authority_current = admission_current = true;
	local_share = resume_allowed = resume_error = resumed_writer = false;
	node_grant_current = true;
	object_current = true; current_object_revision = 7;
	error_after_local_lock = error_after_ledger_bind = false;
	local_lock_attempts = local_unlocks = 0;
	cluster_interconnect_tier = CLUSTER_IC_TIER_1;
	memset(cluster_bufmgr_pcm_x_writer_ledger, 0, sizeof(cluster_bufmgr_pcm_x_writer_ledger));
	memset(descriptors, 0, sizeof(descriptors));
	conditional_resumes = conditional_unlocks = 0;
	backend_pin_count = 1;
	pin_checks = 0;
	recent_pin_calls = recent_unpins = 0;
	recent_pin_observer = NULL;
	recent_pin_allowed = true; recent_pin_error = false; recent_pin_admission = NULL;
	recent_caller_local_x = recent_caller_writer = recent_caller_share = recent_pin_held = false;
	recent_caller_pins = 0; phase_pin_released = false;
	phase_use_original_resident = false;
	phase_use_original_recent_pin = false;
	phase_prior_lock_activity = phase_prior_authority_activity = 0;
	phase_prior_writer_checks = phase_prior_route_checks = phase_prior_authority_checks = 0;
	writer_state = PCM_STATE_X;
	route_holder = 11;
	route_master = 12;
	flush_error = false;
	space_buffer_permanent = true; space_permanence_error = false;
	tx_checks = redo_dirties = redo_releases = freeze_data_calls = 0;
	freeze_block_length = 0;
	expected_nplans = 1;
	memset(freeze_main_data.data, 0, BLCKSZ);
	memset(freeze_block_data.data, 0, BLCKSZ);
	redoing = redo_locked = false;
	begun = expecting_error = false;
	tx_committed = true;
	next_token = 100;
	edge_token = 0;
	expected_info = opcode;
	relation_data.rd_rel = &relform;
	relation_data.rd_locator = (RelFileLocator){ 1663, 5, 900 };
	relform.relpersistence = RELPERSISTENCE_PERMANENT;
	space_identity.key.system_identifier = 11;
	space_identity.key.database_incarnation = 12;
	memset(space_identity.key.storage_uuid, 13, 16);
	space_identity.key.locator = relation_data.rd_locator;
	memset(space_identity.incarnation, 30, 16);
	space_identity.sequence = 1;
	space_identity.operation = 40;
	space_identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	PageInitHeapPage(current_page.data, BLCKSZ, 0);
	((PageHeader)current_page.data)->pd_block_scn = 20;
	tuple->t_hoff = MAXALIGN(SizeofHeapTupleHeader);
	tuple->t_infomask = HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID;
	HeapTupleHeaderSetXmin(tuple, 77);
	for (int i = 1; i <= 3; i++) {
		tuple->t_infomask2 = i == 1 ? 0 : HEAP_ONLY_TUPLE;
		ItemPointerSet(&tuple->t_ctid, 7, i);
		UT_ASSERT_EQ(PageAddItem(current_page.data, (Item)tuple, 64, i, false, true), i);
	}
	if (opcode == XLOG_HEAP2_VACUUM) {
		ItemIdSetDead(PageGetItemId(current_page.data, 3));
		PageRepairFragmentation(current_page.data);
	} else if (opcode == XLOG_HEAP2_PRUNE) {
		PageSetFull(current_page.data);
		((PageHeader)current_page.data)->pd_prune_xid = 77;
	}
	memcpy(before_page.data, current_page.data, BLCKSZ);
}

static void
execute(uint8 opcode, const ClusterSpaceIdentity *identity)
{
	if (opcode == XLOG_HEAP2_PRUNE)
		run_prune(true, identity);
	else if (opcode == XLOG_HEAP2_VACUUM)
		run_vacuum(identity);
	else {
		HeapTupleFreeze tuples[2] = { { 0 } };
		for (int i = 0; i < 2; i++) {
			tuples[i].offset = i + 1;
			tuples[i].t_infomask = HEAP_XMIN_FROZEN | HEAP_XMAX_INVALID;
			tuples[i].checkflags = HEAP_FREEZE_CHECK_XMIN_COMMITTED;
		}
		run_freeze(&relation_data, 1, 70, tuples, 2, identity);
	}
}

static void
check_publication(bool shared)
{
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(dirties, 1);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(PageGetLSN(current_page.data), UINT64_C(0x9000));
	UT_ASSERT_EQ(edges, shared ? 1 : 0);
	UT_ASSERT_EQ(next_token, shared ? 101 : 100);
	UT_ASSERT_EQ(((PageHeader)current_page.data)->pd_block_scn, shared ? 101 : 20);
	UT_ASSERT_EQ(((PageHeader)wal_image.data)->pd_block_scn, shared ? 101 : 20);
	if (shared) {
		UT_ASSERT_EQ(edge_token, 101);
		UT_ASSERT_EQ(edge.before.mutation_token, 20);
		UT_ASSERT_EQ(edge.block_id, 0);
		UT_ASSERT_EQ(edge.before_kind, RF_PAGE_STATE_PRESENT);
		UT_ASSERT_EQ(edge.result_kind, RF_PAGE_STATE_PRESENT);
		UT_ASSERT(memcmp(edge.before.segment_incarnation, space_identity.incarnation, 16) == 0);
		UT_ASSERT(memcmp(edge.result_incarnation, space_identity.incarnation, 16) == 0);
	}
}

static HeapTupleHeader
freeze_tuple(Page page, OffsetNumber offset)
{
	return (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, offset));
}

static void
replay_freeze_record(void)
{
	PGAlignedBlock written;
	XLogReaderState reader = { 0 };
	DecodedXLogRecord *decoded = calloc(1, offsetof(DecodedXLogRecord, blocks)
										 + sizeof(DecodedBkpBlock));

	UT_ASSERT_NOT_NULL(decoded);
	if (decoded == NULL)
		return;
	memcpy(written.data, current_page.data, BLCKSZ);
	memcpy(current_page.data, before_page.data, BLCKSZ);
	decoded->main_data = freeze_main_data.data;
	decoded->main_data_len = SizeOfHeapFreezePage;
	reader.record = decoded;
	reader.EndRecPtr = UINT64_C(0x9000);
	redoing = true;
	heap_xlog_freeze_page(&reader);
	redoing = false;
	UT_ASSERT_EQ(redo_dirties, 1);
	UT_ASSERT_EQ(redo_releases, 1);
	UT_ASSERT(!redo_locked);
	UT_ASSERT_EQ(CritSectionCount, 0);
	/* SPACE edge replay is an outer layer, not native heap freeze redo. */
	UT_ASSERT_EQ(((PageHeader)current_page.data)->pd_block_scn, 20);
	((PageHeader)written.data)->pd_block_scn = 20;
	UT_ASSERT(memcmp(written.data, current_page.data, BLCKSZ) == 0);
	free(decoded);
}

UT_TEST(test_qualified_freeze_kernel_changes_only_planned_headers)
{
	HeapTupleFreeze tuples[2] = { { 0 } };

	reset(XLOG_HEAP2_FREEZE_PAGE, true);
	tx_committed = false;
	for (int i = 0; i < 2; i++) {
		tuples[i].offset = i + 1;
		tuples[i].t_infomask = HEAP_XMIN_FROZEN | HEAP_XMAX_INVALID;
		tuples[i].checkflags = HEAP_FREEZE_CHECK_XMIN_COMMITTED;
	}
	heap_freeze_apply_prepared(&relation_data, 1, 70, tuples, 2, &space_identity, false, NULL, NULL, NULL, InvalidFullTransactionId);
	check_publication(true);
	UT_ASSERT_EQ(tx_checks, 0);
	UT_ASSERT_EQ(freeze_data_calls, 2);
	for (int i = 1; i <= 3; i++) {
		HeapTupleHeader tuple = freeze_tuple(current_page.data, i);
		HeapTupleHeader original = freeze_tuple(before_page.data, i);
		Size length = ItemIdGetLength(PageGetItemId(current_page.data, i));

		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(tuple), 77);
		UT_ASSERT_EQ(HeapTupleHeaderXminFrozen(tuple), i <= 2);
		UT_ASSERT(ItemPointerEquals(&tuple->t_ctid, &original->t_ctid));
		UT_ASSERT_EQ(tuple->t_hoff, original->t_hoff);
		UT_ASSERT(memcmp((char *)tuple + tuple->t_hoff,
						 (char *)original + original->t_hoff,
						 length - tuple->t_hoff) == 0);
	}
	UT_ASSERT(memcmp(freeze_tuple(current_page.data, 3),
					 freeze_tuple(before_page.data, 3), 64) == 0);
	if (inserts == 1)
		replay_freeze_record();
}

UT_TEST(test_exact_xmin_plan_preserves_xmax_through_native_wal_and_redo)
{
	const uint16 xmax_masks[] = {
		HEAP_XMAX_INVALID,
		HEAP_XMAX_COMMITTED,
		HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK,
		HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_KEYSHR_LOCK
	};

	for (unsigned variant = 0; variant < lengthof(xmax_masks); variant++) {
		ClusterHeapFreezePageBatchV1 page = { 0 };
		ClusterHeapFreezeProofBatchV1 proofs = { .proof_count = 1 };
		ClusterHeapFreezeCanonicalProofV1 *proof = &proofs.proofs[0];
		ClusterHeapFreezeFinalFenceV1 fence;
		ClusterHeapFreezeTupleInputV1 *input = &page.tuples[0];
		HeapTupleFreeze plan;
		HeapTupleHeader tuple;
		ItemId item;
		bool prepared;

		reset(XLOG_HEAP2_FREEZE_PAGE, true);
		/* The canonical final fence requires an already-persisted page LSN. */
		PageSetLSN(current_page.data, UINT64_C(0x8000));
		tuple = freeze_tuple(current_page.data, 1);
		item = PageGetItemId(current_page.data, 1);
		HeapTupleHeaderSetXmax(tuple, 88);
		tuple->t_infomask = HEAP_XMIN_COMMITTED | HEAP_UPDATED | xmax_masks[variant];
		tuple->t_infomask2 = HEAP_HOT_UPDATED | 7;
		memcpy(before_page.data, current_page.data, BLCKSZ);

		/* These identities are qualified fixture inputs, not an epoch producer. */
		fence = (ClusterHeapFreezeFinalFenceV1){
			.identity = space_identity, .block = 7, .page_lsn = UINT64_C(0x8000), .page_version = 20,
			.holder_sequence = 11, .master_sequence = 12, .admission_generation = 13 };
		proof->requested_locator.uba = uba_encode(1, 1, 0, 1);
		proof->requested_locator.xid = 77;
		proof->requested_locator.tt_wrap = TT_WRAP_INVALID;
		proof->requested_locator.itl_kind = ITL_FLAG_ACTIVE;
		proof->resolution.locator_echo = proof->requested_locator;
		proof->resolution.locator_echo.tt_wrap = 2;
		proof->resolution.outcome = CLUSTER_TX_COMMITTED;
		proof->resolution.proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
		proof->resolution.commit_scn = 9;
		proof->resolution.authority.origin_epoch = 1;
		proof->resolution.authority.live_hwm_lsn = 1;
		proof->resolution.authority.tt_generation = 1;
		proof->resolution.authority.authority_scn = 9;
		proof->resolution.local_canonical_valid = true;
		proof->resolution.local_canonical_generation = 1;
		proof->resolution.local_canonical.segment_id = 1;
		proof->resolution.local_canonical.xid = 77;
		proof->resolution.local_canonical.wrap = 2;
		proof->resolution.local_original_full_xid = FullTransactionIdFromEpochAndXid(1, 77);
		proof->resolution.local_creation_target = (ClusterTxCreationTarget){
			.locator = relation_data.rd_locator, .fork = MAIN_FORKNUM,
			.block = 7, .offset = 1, .tuple_length = ItemIdGetLength(item), .valid = true };
		proof->c1b_finalized = proof->authority_current = proof->admission_current = true;
		proof->expected_fence = proof->current_fence = fence;
		page.identity = space_identity;
		page.block = 7;
		page.page_lsn = fence.page_lsn;
		page.page_version = fence.page_version;
		page.tuple_count = page.carrier_request_count = 1;
		page.carrier_requests[0].locator = proof->requested_locator;
		input->offset = 1;
		input->tuple_offset = ItemIdGetOffset(item);
		input->tuple_length = ItemIdGetLength(item);
		input->header_length = tuple->t_hoff;
		input->raw_xmin = HeapTupleHeaderGetRawXmin(tuple);
		input->raw_xmax = HeapTupleHeaderGetRawXmax(tuple);
		input->infomask = tuple->t_infomask;
		input->infomask2 = tuple->t_infomask2;
		input->ctid = tuple->t_ctid;
		input->has_carrier = true;
		UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(&page, &proofs, &fence), 1);
		prepared = cluster_heap_freeze_prepare_xmin_v1(&page, &proofs, &fence, 0,
			FullTransactionIdFromEpochAndXid(1, 77),
			FullTransactionIdFromEpochAndXid(1, 100), 10, &plan);
		UT_ASSERT(prepared);
		if (!prepared)
			continue;
		tx_committed = false;
		heap_freeze_apply_prepared(&relation_data, 1, 70, &plan, 1, &space_identity, false, NULL, NULL, NULL, InvalidFullTransactionId);
		check_publication(true);
		UT_ASSERT_EQ(tx_checks, 0);
		UT_ASSERT_EQ(freeze_data_calls, 2);
		UT_ASSERT(HeapTupleHeaderXminFrozen(tuple));
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(tuple), 77);
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmax(tuple), 88);
		UT_ASSERT_EQ(tuple->t_infomask,
			HEAP_XMIN_FROZEN | HEAP_UPDATED | xmax_masks[variant]);
		UT_ASSERT_EQ(tuple->t_infomask2, HEAP_HOT_UPDATED | 7);
		UT_ASSERT(memcmp(freeze_tuple(current_page.data, 2),
			freeze_tuple(before_page.data, 2), 64) == 0);
		UT_ASSERT(memcmp(freeze_tuple(current_page.data, 3),
			freeze_tuple(before_page.data, 3), 64) == 0);
		if (inserts == 1)
			replay_freeze_record();
	}
}

/* Qualified identities and ownership below are leaf fixture inputs.  The real
 * capture, preparation, final apply, native WAL assembly and redo are exercised. */
typedef struct QualifiedXminFixture
{
	ClusterHeapFreezePageInputV1 input;
	ClusterHeapFreezePageBatchV1 page;
	ClusterHeapFreezeProofBatchV1 proofs;
	ClusterHeapFreezeFinalFenceV1 fence;
	PcmAuthoritySnapshot authority;
	ClusterSemanticAdmissionToken admission;
	ClusterHeapFreezePolicySampleV1 policy;
	ClusterHeapFreezeCohortObservation cohort;
} QualifiedXminFixture;

static void
qualified_xmin_setup(QualifiedXminFixture *f, uint16 xmax_mask)
{
	ClusterHeapFreezeCanonicalProofV1 *proof;
	ClusterItlSlotData *slot;
	HeapTupleHeader tuple;

	reset(XLOG_HEAP2_FREEZE_PAGE, true);
	memset(f, 0, sizeof(*f));
	PageSetLSN(current_page.data, UINT64_C(0x8000));
	slot = ClusterPageGetItlSlot(current_page.data, 0);
	memset(slot, 0, sizeof(*slot));
	slot->xid = 77;
	slot->wrap = 2;
	slot->flags = ITL_FLAG_ACTIVE;
	slot->undo_segment_head = uba_encode(1, 11, 21, 31);
	slot->write_scn = 100;
	slot->first_change_lsn = 200;
	for (int i = 1; i <= 3; i++)
		freeze_tuple(current_page.data, i)->t_itl_slot_idx = 0;
	tuple = freeze_tuple(current_page.data, 1);
	HeapTupleHeaderSetXmax(tuple, 88);
	tuple->t_infomask = HEAP_XMIN_COMMITTED | HEAP_UPDATED | xmax_mask;
	tuple->t_infomask2 = HEAP_HOT_UPDATED | 7;
	memcpy(before_page.data, current_page.data, BLCKSZ);
	UT_ASSERT(cluster_heap_freeze_capture_page_v1(&space_identity, 7,
		current_page.data, &f->input));
	memset(&original_writer_context, 0, sizeof(original_writer_context));
	InitBufferTag(&original_writer_context.ref.assertion.resource,
		&relation_data.rd_locator, MAIN_FORKNUM, 7);
	original_writer_context.ref.assertion.requester_node = 0;
	original_writer_context.ref.formation = 21;
	original_writer_context.ref.acquisition_generation = 22;
	original_writer_context.r4_record_generation = 13;
	original_writer_context.buffer_ownership_generation = 14;
	{
		BufferDesc *buf = GetBufferDescriptor(0);
		ClusterPcmXWriterLedgerEntry *e = &cluster_bufmgr_pcm_x_writer_ledger[0];
		buf->tag = original_writer_context.ref.assertion.resource;
		e->phase = PCM_X_WRITER_LEDGER_ACTIVE; e->buffer_id = 0;
		e->content_lock = BufferDescriptorGetContentLock(buf);
		e->authority = original_writer_context;
		e->granted.tag = buf->tag; e->granted.generation = 14;
		e->granted.pcm_state = PCM_STATE_X;
		e->object_checked = true; e->object_revision = 7;
	}
	f->input.writer_context = original_writer_context;
	f->input.writer_context_valid = true;
	f->input.writer_object_revision = 7;
	f->input.writer_object_checked = true;
	UT_ASSERT(cluster_heap_freeze_prepare_page_v1(&f->input, &f->page));
	UT_ASSERT_EQ(f->page.carrier_request_count, 1);
	f->fence = (ClusterHeapFreezeFinalFenceV1){
		.identity = space_identity, .block = 7, .page_lsn = UINT64_C(0x8000), .page_version = 20,
		.holder_sequence = 11, .master_sequence = 12, .admission_generation = 13 };
	f->proofs.proof_count = 1;
	proof = &f->proofs.proofs[0];
	proof->requested_locator = f->page.carrier_requests[0].locator;
	proof->resolution.locator_echo = proof->requested_locator;
	proof->resolution.locator_echo.tt_wrap = 2;
	proof->resolution.outcome = CLUSTER_TX_COMMITTED;
	proof->resolution.proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
	proof->resolution.commit_scn = 9;
	proof->resolution.authority.origin_epoch = 1;
	proof->resolution.authority.live_hwm_lsn = 1;
	proof->resolution.authority.tt_generation = 1;
	proof->resolution.authority.authority_scn = 9;
	proof->resolution.local_canonical_valid = true;
	proof->resolution.local_canonical_generation = 1;
	proof->resolution.local_canonical.segment_id = 1;
	proof->resolution.local_canonical.xid = 77;
	proof->resolution.local_canonical.wrap = 2;
	proof->resolution.local_original_full_xid = FullTransactionIdFromEpochAndXid(1, 77);
	proof->resolution.local_creation_target = (ClusterTxCreationTarget){
		.locator = relation_data.rd_locator, .fork = MAIN_FORKNUM,
		.block = 7, .offset = 1, .tuple_length = 64, .valid = true };
	proof->c1b_finalized = proof->authority_current = proof->admission_current = true;
	proof->expected_fence = proof->current_fence = f->fence;
	f->authority.transition_count = 11;
	f->admission.entered = true;
	f->admission.record_generation = 13;
	f->admission.formation_epoch = 21;
	f->policy.legal_next = FullTransactionIdFromEpochAndXid(1, 200);
	f->policy.freeze_limit = FullTransactionIdFromEpochAndXid(1, 100);
	retained_lease_publish_fixture(&f->policy);
	f->cohort.active_full_xid_min = U64FromFullTransactionId(f->policy.freeze_limit);
	f->cohort.future_full_xid_max = U64FromFullTransactionId(f->policy.legal_next);
	f->cohort.local_registration_cut.min_full = f->policy.freeze_limit;
	f->cohort.local_registration_cut.future_full = f->policy.legal_next;
	f->cohort.source_local_sample_id = 17;
	f->cohort.source_started_at_us = 1000000;
	f->cohort.source_interval_ms = CLUSTER_UNDO_HORIZON_INTERVAL_MIN_MS;
	f->cohort.horizon_scn = 10;
	f->cohort.source_local_horizon_scn = 10;
	f->cohort.source_local_history.sample_clock_scn = 10;
	f->cohort.source_local_history.contributor_kind = CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK;
	f->cohort.record_generation = 13; f->cohort.formation_epoch = 21;
	f->cohort.admitted_members_lo = 1;
	f->cohort.admission_fence.record_generation = 13;
	f->cohort.admission_fence.formation_epoch = 21;
	f->cohort.admission_fence.admitted_members_lo = 1;
	f->cohort.admission_fence.local_node_id = 0;
	f->cohort.local_registration_cut.status = CTRC_ACTIVE_SCAN_COMPLETE;
	f->cohort.local_registration_cut.total_entries =
		CtrcShared->origin_key_entries + CtrcShared->participant_key_entries;
	f->cohort.local_registration_cut.next_index = f->cohort.local_registration_cut.total_entries;
	f->cohort.local_registration_cut.unqualified_registration_generation = pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation);
	admission_current_publish_fixture(&f->cohort);
	worker_current_publish_fixture(&f->cohort);
	worker_native_sample_fixture(&f->cohort);
	f->input.freeze_policy = &f->policy; f->input.freeze_cohort = &f->cohort;
	UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(&f->page, &f->proofs, &f->fence), 1);
}

static bool
qualified_xmin_apply(QualifiedXminFixture *f, uint16 index)
{
	return cluster_heap_freeze_apply_xmin_v1(&relation_data, 1, &f->input,
		&f->page, &f->proofs, &f->fence, &f->authority, &f->admission, index,
		FullTransactionIdFromEpochAndXid(1, 77),
		FullTransactionIdFromEpochAndXid(1, 100), 10);
}

static void
qualified_xmin_batch_setup(QualifiedXminFixture *f,
	ClusterHeapFreezeXminTargetV1 targets[3])
{
	const TransactionId xids[] = { 77, UINT32_MAX - 2, 66 };
	const uint16 order[] = { 2, 0, 1 };
	ClusterHeapFreezeCanonicalProofV1 seed;
	ClusterItlSlotData carrier;

	qualified_xmin_setup(f, HEAP_XMAX_INVALID);
	seed = f->proofs.proofs[0];
	carrier = *ClusterPageGetItlSlot(current_page.data, 0);
	for (int i = 0; i < 3; i++) {
		ClusterItlSlotData *slot = ClusterPageGetItlSlot(current_page.data, i);
		HeapTupleHeader tuple = freeze_tuple(current_page.data, i + 1);

		*slot = carrier;
		slot->xid = xids[i];
		slot->wrap += i;
		slot->undo_segment_head = uba_encode(1, 11, 21 + i, 31);
		HeapTupleHeaderSetXmin(tuple, xids[i]);
		tuple->t_itl_slot_idx = i;
	}
	memcpy(before_page.data, current_page.data, BLCKSZ);
	UT_ASSERT(cluster_heap_freeze_capture_page_v1(&space_identity, 7,
		current_page.data, &f->input));
	f->input.writer_context = original_writer_context;
	f->input.writer_context_valid = true;
	f->input.writer_object_revision = 7;
	f->input.writer_object_checked = true;
	f->input.freeze_policy = &f->policy;
	f->input.freeze_cohort = &f->cohort;
	UT_ASSERT(cluster_heap_freeze_prepare_page_v1(&f->input, &f->page));
	UT_ASSERT_EQ(f->page.carrier_request_count, 3);
	f->proofs.proof_count = 3;
	for (int i = 0; i < 3; i++) {
		ClusterHeapFreezeCanonicalProofV1 *proof = &f->proofs.proofs[i];

		*proof = seed;
		proof->requested_locator = f->page.carrier_requests[i].locator;
		proof->resolution.locator_echo = proof->requested_locator;
		proof->resolution.locator_echo.tt_wrap = 2 + i;
		proof->resolution.local_canonical.xid = xids[i];
		proof->resolution.local_canonical.wrap = 2 + i;
		proof->resolution.local_original_full_xid =
			FullTransactionIdFromEpochAndXid(i == 1 ? 0 : 1, xids[i]);
		proof->resolution.local_creation_target.offset = i + 1;
		targets[i].tuple_index = order[i];
		targets[i].original_creator_full_xid =
			FullTransactionIdFromEpochAndXid(order[i] == 1 ? 0 : 1, xids[order[i]]);
	}
	UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(&f->page, &f->proofs, &f->fence), 3);
	expected_nplans = 2;
}

static bool
qualified_xmin_batch_apply(QualifiedXminFixture *f,
	const ClusterHeapFreezeXminTargetV1 *targets, uint16 count)
{
	return cluster_heap_freeze_apply_xmin_batch_v1(&relation_data, 1, &f->input,
		&f->page, &f->proofs, &f->fence, &f->authority, &f->admission, targets,
		count, FullTransactionIdFromEpochAndXid(1, 100), 10);
}

UT_TEST(test_xmin_apply_requires_original_backend_pin_before_page_or_writer_use)
{
	QualifiedXminFixture f;

	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
	backend_pin_count = 0;
	UT_ASSERT(!qualified_xmin_apply(&f, 0));
	UT_ASSERT_EQ(pin_checks, 1);
	UT_ASSERT_EQ(inserts + edges + dirties + flushes + writer_checks
		+ route_checks + authority_checks + admission_checks, 0);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	UT_ASSERT(local_x && f.admission.entered);
	UT_ASSERT_EQ(backend_pin_count, 0);
}

UT_TEST(test_xmin_batch_requires_original_backend_pin_without_repin_or_write)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];

	qualified_xmin_batch_setup(&f, targets);
	backend_pin_count = 0;
	UT_ASSERT(!qualified_xmin_batch_apply(&f, targets, 3));
	UT_ASSERT_EQ(pin_checks, 1);
	UT_ASSERT_EQ(inserts + edges + dirties + flushes + writer_checks
		+ route_checks + authority_checks + admission_checks, 0);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	UT_ASSERT(local_x && f.admission.entered);
	UT_ASSERT_EQ(backend_pin_count, 0);
}

UT_TEST(test_xmin_apply_rejects_unbounded_cutoffs_before_native_write)
{
	QualifiedXminFixture f;

	for (int variant = 0; variant < 3; variant++) {
		FullTransactionId limit = FullTransactionIdFromEpochAndXid(1, 100);
		SCN horizon = 10;

		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		if (variant != 0)
			limit = FullTransactionIdFromU64(UINT64_MAX);
		if (variant != 1)
			horizon = (SCN)UINT64_MAX;
		UT_ASSERT(!cluster_heap_freeze_apply_xmin_v1(&relation_data, 1, &f.input,
			&f.page, &f.proofs, &f.fence, &f.authority, &f.admission, 0,
			FullTransactionIdFromEpochAndXid(1, 77), limit, horizon));
		UT_ASSERT_EQ(inserts + dirties + flushes + writer_checks + authority_checks, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_batch_rejects_unbounded_cutoffs_before_native_write)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];

	for (int variant = 0; variant < 3; variant++) {
		FullTransactionId limit = FullTransactionIdFromEpochAndXid(1, 100);
		SCN horizon = 10;

		qualified_xmin_batch_setup(&f, targets);
		if (variant != 0)
			limit = FullTransactionIdFromU64(UINT64_MAX);
		if (variant != 1)
			horizon = (SCN)UINT64_MAX;
		UT_ASSERT(!cluster_heap_freeze_apply_xmin_batch_v1(&relation_data, 1, &f.input,
			&f.page, &f.proofs, &f.fence, &f.authority, &f.admission,
			targets, 3, limit, horizon));
		UT_ASSERT_EQ(inserts + dirties + flushes + writer_checks + authority_checks, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_apply_rejects_unbound_original_identity_before_wal)
{
	QualifiedXminFixture f;

	for (int variant = 0; variant < 5; variant++) {
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		switch (variant) {
		case 0: f.page.tuples[0].original_creator_full_xid = InvalidFullTransactionId; break;
		case 1: f.page.tuples[0].original_creator_full_xid = FullTransactionIdFromEpochAndXid(2, 77); break;
		case 2: f.proofs.proofs[0].resolution.local_original_full_xid = InvalidFullTransactionId; break;
		case 3: f.proofs.proofs[0].resolution.local_original_full_xid = FullTransactionIdFromEpochAndXid(2, 77); break;
		case 4:
			f.page.tuples[0].original_creator_full_xid = FullTransactionIdFromEpochAndXid(0, 77);
			f.proofs.proofs[0].resolution.local_original_full_xid = f.page.tuples[0].original_creator_full_xid;
			break;
		}
		UT_ASSERT(!qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts + dirties + flushes + writer_checks + authority_checks, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_batch_later_origin_epoch_mismatch_is_atomic)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];

	for (int variant = 0; variant < 3; variant++) {
		qualified_xmin_batch_setup(&f, targets);
		/* The first row prepares; a later target keeps the same raw XID
		 * while its observed original epoch no longer matches the caller. */
		if (variant == 0)
			f.page.tuples[1].original_creator_full_xid = InvalidFullTransactionId;
		if (variant == 1)
			f.proofs.proofs[1].resolution.local_original_full_xid =
				FullTransactionIdFromEpochAndXid(1, UINT32_MAX - 2);
		if (variant == 2) {
			f.page.tuples[0].original_creator_full_xid = FullTransactionIdFromEpochAndXid(0, 77);
			f.proofs.proofs[0].resolution.local_original_full_xid = f.page.tuples[0].original_creator_full_xid;
		}
		UT_ASSERT(!qualified_xmin_batch_apply(&f, targets, 3));
		UT_ASSERT_EQ(inserts + dirties + flushes + writer_checks + authority_checks, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_mixed_page_freezes_only_exact_selected_carriers)
{
	for (int resume = 0; resume < 2; resume++) {
		QualifiedXminFixture f;
		ClusterHeapFreezeXminTargetV1 targets[3];
		bool applied;

		qualified_xmin_batch_setup(&f, targets);
		/* Select physical rows 3 and 1; row 2 has no terminal proof. */
		f.proofs.proofs[1] = f.proofs.proofs[2];
		f.proofs.proof_count = 2;
		if (resume) {
			ClusterHeapFreezeCanonicalProofV1 swap = f.proofs.proofs[0];
			f.proofs.proofs[0] = f.proofs.proofs[1];
			f.proofs.proofs[1] = swap;
			fixture_release_original_writer(); resume_allowed = true;
		}
		UT_ASSERT_EQ(cluster_heap_freeze_validate_conditional_apply_v1(
			&f.page, &f.proofs, &f.fence), CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
		applied = qualified_xmin_batch_apply(&f, targets, 2);
		UT_ASSERT(applied);
		if (!applied)
			continue;
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT_EQ(conditional_resumes, resume);
		UT_ASSERT_EQ(conditional_unlocks, resume);
		UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(f.admission.entered);
		UT_ASSERT_EQ(((xl_heap_freeze_page *)freeze_main_data.data)->snapshotConflictHorizon, 77);
		for (int i = 1; i <= 3; i++) {
			HeapTupleHeader tuple = freeze_tuple(current_page.data, i);
			HeapTupleHeader old = freeze_tuple(before_page.data, i);
			UT_ASSERT_EQ(tuple->t_infomask,
				i == 2 ? old->t_infomask : old->t_infomask | HEAP_XMIN_FROZEN);
			UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(tuple), HeapTupleHeaderGetRawXmin(old));
			UT_ASSERT_EQ(HeapTupleHeaderGetRawXmax(tuple), HeapTupleHeaderGetRawXmax(old));
			UT_ASSERT(memcmp(ClusterPageGetItlSlot(current_page.data, i - 1),
				ClusterPageGetItlSlot(before_page.data, i - 1), sizeof(ClusterItlSlotData)) == 0);
		}
		UT_ASSERT(memcmp(freeze_tuple(current_page.data, 2),
			freeze_tuple(before_page.data, 2), 64) == 0);
		check_publication(true);
		replay_freeze_record();
	}
}

UT_TEST(test_xmin_mixed_page_refuses_missing_selected_or_invalid_supplied_proof)
{
	for (int variant = 0; variant < 3; variant++) {
		QualifiedXminFixture f;
		ClusterHeapFreezeXminTargetV1 targets[3];
		qualified_xmin_batch_setup(&f, targets);
		f.proofs.proofs[1] = f.proofs.proofs[2];
		f.proofs.proof_count = 2;
		if (variant == 1) f.proofs.proofs[1] = f.proofs.proofs[0];
		if (variant == 2) f.proofs.proofs[1].current_fence.page_version++;
		fixture_release_original_writer(); resume_allowed = true;
		UT_ASSERT(!qualified_xmin_batch_apply(&f, targets, variant == 0 ? 3 : 2));
		UT_ASSERT_EQ(inserts, 0); UT_ASSERT_EQ(flushes, 0);
		UT_ASSERT_EQ(conditional_resumes, 0);
		UT_ASSERT_EQ(conditional_unlocks, 0);
		UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_batch_one_wal_fence_fullxid_horizon_and_redo)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];

	qualified_xmin_batch_setup(&f, targets);
	UT_ASSERT(qualified_xmin_batch_apply(&f, targets, 3));
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(flushes, 1);
	UT_ASSERT_EQ(writer_checks, 2);
	UT_ASSERT_EQ(route_checks, 1);
	UT_ASSERT_EQ(authority_checks, 1);
	UT_ASSERT_EQ(admission_checks, 1);
	UT_ASSERT_EQ(((xl_heap_freeze_page *)freeze_main_data.data)->snapshotConflictHorizon, 77);
	for (int i = 1; i <= 3; i++) {
		HeapTupleHeader tuple = freeze_tuple(current_page.data, i);
		HeapTupleHeader old = freeze_tuple(before_page.data, i);

		UT_ASSERT(HeapTupleHeaderXminFrozen(tuple));
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(tuple), HeapTupleHeaderGetRawXmin(old));
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmax(tuple), HeapTupleHeaderGetRawXmax(old));
		UT_ASSERT_EQ(tuple->t_infomask, old->t_infomask | HEAP_XMIN_FROZEN);
		UT_ASSERT_EQ(tuple->t_infomask2, old->t_infomask2);
		UT_ASSERT(memcmp(ClusterPageGetItlSlot(current_page.data, i - 1),
			ClusterPageGetItlSlot(before_page.data, i - 1), sizeof(ClusterItlSlotData)) == 0);
	}
	if (inserts == 1) {
		check_publication(true);
		UT_ASSERT(!qualified_xmin_batch_apply(&f, targets, 3));
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(flushes, 1);
		replay_freeze_record();
	}
}

UT_TEST(test_xmin_batch_later_unqualified_target_changes_nothing)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];

	for (int variant = 0; variant < 4; variant++) {
		qualified_xmin_batch_setup(&f, targets);
		expected_nplans = 1; /* RED scaffold can write its first row. */
		switch (variant) {
		case 0: f.proofs.proofs[1].resolution.local_creation_target.offset = 1; break;
		case 1: f.proofs.proofs[1].resolution.commit_scn = 10; break;
		case 2: targets[2].original_creator_full_xid = FullTransactionIdFromEpochAndXid(0, 99); break;
		case 3: targets[2].original_creator_full_xid = FullTransactionIdFromEpochAndXid(2, UINT32_MAX - 2); break;
		}
		UT_ASSERT(!qualified_xmin_batch_apply(&f, targets, 3));
		UT_ASSERT_EQ(inserts + dirties + flushes, 0);
		UT_ASSERT_EQ(writer_checks + route_checks + authority_checks + admission_checks, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_batch_rejects_duplicate_physical_rows)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];

	for (int alias = 0; alias < 2; alias++) {
		qualified_xmin_batch_setup(&f, targets);
		expected_nplans = 1;
		targets[1] = targets[0];
		if (alias) {
			/* Distinct request indexes cannot authorize the same physical row. */
			f.page.tuples[1] = f.page.tuples[2];
			targets[1].tuple_index = 1;
		}
		UT_ASSERT(!qualified_xmin_batch_apply(&f, targets, 2));
		UT_ASSERT_EQ(inserts + dirties + flushes + writer_checks, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_batch_runtime_bounds_and_empty_reject)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];

	for (int variant = 0; variant < 4; variant++) {
		qualified_xmin_batch_setup(&f, targets);
		if (variant == 3)
			targets[0].tuple_index = UINT16_MAX;
		UT_ASSERT(!qualified_xmin_batch_apply(&f, variant == 1 ? NULL : targets,
			variant == 0 ? 0 : variant == 2 ? MaxHeapTuplesPerPage + 1 : 3));
		UT_ASSERT_EQ(inserts + dirties + flushes + writer_checks, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_batch_later_stale_row_rejects_before_fence)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];

	for (int variant = 0; variant < 3; variant++) {
		qualified_xmin_batch_setup(&f, targets);
		expected_nplans = 1;
		if (variant == 0)
			f.page.tuples[1].raw_xmax++;
		else if (variant == 1)
			f.page.tuples[1].tuple_offset += MAXIMUM_ALIGNOF;
		else
			f.page.tuples[1].infomask2 ^= HEAP_ONLY_TUPLE;
		UT_ASSERT(!qualified_xmin_batch_apply(&f, targets, 3));
		UT_ASSERT_EQ(inserts + dirties + flushes + writer_checks, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_batch_rejects_ambiguous_native_horizon)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];

	qualified_xmin_batch_setup(&f, targets);
	expected_nplans = 1;
	targets[0].original_creator_full_xid = FullTransactionIdFromEpochAndXid(0, 66);
	f.proofs.proofs[2].resolution.local_original_full_xid = targets[0].original_creator_full_xid;
	UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(&f.page, &f.proofs, &f.fence), 3);
	UT_ASSERT(!qualified_xmin_batch_apply(&f, targets, 3));
	UT_ASSERT_EQ(inserts + dirties + flushes + writer_checks, 0);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
}

UT_TEST(test_xmin_batch_final_authority_refusal_changes_nothing)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];

	for (int variant = 0; variant < 3; variant++) {
		qualified_xmin_batch_setup(&f, targets);
		if (variant == 0)
			admission_current = false;
		else if (variant == 1)
			authority_current = false;
		else
			route_master++;
		UT_ASSERT(!qualified_xmin_batch_apply(&f, targets, 3));
		UT_ASSERT_EQ(writer_checks, 2);
		UT_ASSERT_EQ(route_checks, 1);
		UT_ASSERT_EQ(wal_clock_reads, 0);
		UT_ASSERT_EQ(inserts + dirties + flushes, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_single_and_batch_reject_unknown_or_behind_wal_clock)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];

	for (int batch = 0; batch < 2; batch++) {
		for (int unknown = 0; unknown < 2; unknown++) {
			if (batch)
				qualified_xmin_batch_setup(&f, targets);
			else
				qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			/* Larger node bits do not make an older clock reach H. */
			fixture_wal_clock = unknown ? InvalidScn : scn_encode(7, 9);
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3)
				: qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(wal_clock_reads, 1);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			UT_ASSERT(local_x && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
		}
	}
}

UT_TEST(test_xmin_single_and_batch_wal_clock_accept_time_equal_or_ahead)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];
	const SCN horizon = scn_encode(7, 10);
	const FullTransactionId limit = FullTransactionIdFromEpochAndXid(1, 100);

	for (int batch = 0; batch < 2; batch++) {
		for (int ahead = 0; ahead < 2; ahead++) {
			bool applied;

			if (batch)
				qualified_xmin_batch_setup(&f, targets);
			else
				qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
			f.cohort.horizon_scn = horizon;
			f.cohort.source_local_horizon_scn = horizon;
			f.cohort.source_local_history.sample_clock_scn = horizon;
			/* A lower node id at the same time is still sufficient. */
			fixture_wal_clock = scn_encode(0, 10 + ahead);
			applied = batch ? cluster_heap_freeze_apply_xmin_batch_v1(
				&relation_data, 1, &f.input, &f.page, &f.proofs, &f.fence,
				&f.authority, &f.admission, targets, 3, limit, horizon)
				: cluster_heap_freeze_apply_xmin_v1(&relation_data, 1, &f.input,
					&f.page, &f.proofs, &f.fence, &f.authority, &f.admission,
					0, FullTransactionIdFromEpochAndXid(1, 77), limit, horizon);
			UT_ASSERT(applied);
			UT_ASSERT_EQ(wal_clock_reads, 1);
			UT_ASSERT_EQ(inserts, 1);
			UT_ASSERT_EQ(flushes, 1);
			UT_ASSERT(scn_time_cmp(inserted_wal_clock, horizon) >= 0);
			UT_ASSERT(local_x && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
		}
	}
}

UT_TEST(test_xmin_batch_flush_error_keeps_borrowed_resources)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];

	qualified_xmin_batch_setup(&f, targets);
	flush_error = expecting_error = true;
	if (setjmp(error_jump) == 0) {
		(void)qualified_xmin_batch_apply(&f, targets, 3);
		UT_ASSERT(false);
	} else {
		UT_ASSERT_EQ(CritSectionCount, 0);
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT_EQ(writer_checks, 2);
		UT_ASSERT_EQ(admission_checks, 1);
		for (int i = 1; i <= 3; i++)
			UT_ASSERT(HeapTupleHeaderXminFrozen(freeze_tuple(current_page.data, i)));
		UT_ASSERT(local_x && f.admission.entered);
	}
	expecting_error = false;
}

UT_TEST(test_qualified_xmin_apply_wal_flush_and_redo)
{
	const uint16 masks[] = { HEAP_XMAX_INVALID, HEAP_XMAX_COMMITTED,
		HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK,
		HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_KEYSHR_LOCK };
	QualifiedXminFixture f;

	for (unsigned i = 0; i < lengthof(masks); i++) {
		qualified_xmin_setup(&f, masks[i]);
		UT_ASSERT(qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(HeapTupleHeaderXminFrozen(freeze_tuple(current_page.data, 1)));
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(freeze_tuple(current_page.data, 1)), 77);
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmax(freeze_tuple(current_page.data, 1)), 88);
		UT_ASSERT_EQ(freeze_tuple(current_page.data, 1)->t_infomask,
			HEAP_XMIN_FROZEN | HEAP_UPDATED | masks[i]);
		UT_ASSERT_EQ(freeze_tuple(current_page.data, 1)->t_infomask2, HEAP_HOT_UPDATED | 7);
		UT_ASSERT(memcmp(ClusterPageGetItlSlot(current_page.data, 0),
			ClusterPageGetItlSlot(before_page.data, 0), sizeof(ClusterItlSlotData)) == 0);
		UT_ASSERT(memcmp(freeze_tuple(current_page.data, 2),
			freeze_tuple(before_page.data, 2), 64) == 0);
		UT_ASSERT(memcmp(freeze_tuple(current_page.data, 3),
			freeze_tuple(before_page.data, 3), 64) == 0);
		if (inserts == 1) {
			check_publication(true);
			UT_ASSERT(!qualified_xmin_apply(&f, 0));
			UT_ASSERT_EQ(inserts, 1);
			UT_ASSERT_EQ(flushes, 1);
			replay_freeze_record();
		}
	}
}

UT_TEST(test_qualified_xmin_rejects_missing_or_changed_ownership)
{
	QualifiedXminFixture f;

	for (int variant = 0; variant < 10; variant++) {
		qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
		switch (variant) {
			case 0: local_x = false; break;
			case 1: writer_current = false; break; /* ConditionalLock alone */
			case 2: writer_state = PCM_STATE_N; break;
			case 3: route_current = false; break;
			case 4: authority_current = false; break;
			case 5: route_holder++; break;
			case 6: route_master++; break;
			case 7: admission_current = false; break;
			case 8: f.admission.record_generation++; break;
			case 9: relform.relpersistence = RELPERSISTENCE_UNLOGGED; break;
		}
		UT_ASSERT(!qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts + dirties + flushes, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_qualified_xmin_rejects_stale_physical_row_and_bounds)
{
	QualifiedXminFixture f;

	for (int variant = 0; variant < 7; variant++) {
		qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
		switch (variant) {
			case 0: ClusterPageGetItlSlot(current_page.data, 0)->wrap++; break;
			case 1: f.page.tuples[0].raw_xmax++; break;
			case 2: f.page.tuples[0].tuple_offset++; break;
			case 3: f.page.tuples[0].itl_slot_index = UINT8_MAX; break;
			case 4: f.proofs.proofs[0].resolution.local_creation_target.offset++; break;
			case 5: f.proofs.proof_count = UINT8_MAX; break;
			case 6: break; /* out-of-range physical row */
		}
		UT_ASSERT(!qualified_xmin_apply(&f, variant == 6 ? UINT16_MAX : 0));
		UT_ASSERT_EQ(inserts + dirties + flushes, 0);
	}
}

UT_TEST(test_qualified_xmin_flush_error_propagates_after_native_write)
{
	QualifiedXminFixture f;

	qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
	flush_error = expecting_error = true;
	if (setjmp(error_jump) == 0) {
		(void)qualified_xmin_apply(&f, 0);
		UT_ASSERT(false);
	} else {
		UT_ASSERT_EQ(CritSectionCount, 0);
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(HeapTupleHeaderXminFrozen(freeze_tuple(current_page.data, 1)));
		UT_ASSERT(local_x); /* borrowed resources remain with the caller */
	}
	expecting_error = false;
}

UT_TEST(test_qualified_xmin_rejects_recapture_with_old_proof_version)
{
	QualifiedXminFixture f;

	for (int variant = 0; variant < 2; variant++) {
		qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
		if (variant == 0)
			PageSetLSN(current_page.data, UINT64_C(0x8001));
		else
			((PageHeader)current_page.data)->pd_block_scn++;
		/* A fresh physical capture cannot refresh the old batch/proof fence. */
		UT_ASSERT(cluster_heap_freeze_capture_page_v1(&space_identity, 7,
			current_page.data, &f.input));
		memcpy(before_page.data, current_page.data, BLCKSZ);
		UT_ASSERT(!qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts + dirties + flushes, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_freeze_native_wal_dedup_and_redo)
{
	for (int shared = 0; shared <= 1; shared++) {
		HeapTupleFreeze tuples[3] = { { 0 } };
		const OffsetNumber order[] = { 3, 1, 2 };
		xl_heap_freeze_page *header;
		xl_heap_freeze_plan *plans;
		OffsetNumber *offsets;

		reset(XLOG_HEAP2_FREEZE_PAGE, shared);
		expected_nplans = 2;
		for (int i = 1; i <= 3; i++) {
			HeapTupleHeader tuple = freeze_tuple(current_page.data, i);

			tuple->t_infomask |= i == 1 ? HEAP_MOVED_OFF : HEAP_MOVED_IN;
			HeapTupleHeaderSetXvac(tuple, 89 + i);
		}
		memcpy(before_page.data, current_page.data, BLCKSZ);
		for (int i = 0; i < 3; i++) {
			tuples[i].offset = order[i];
			tuples[i].t_infomask = HEAP_XMIN_FROZEN | HEAP_XMAX_INVALID;
			tuples[i].t_infomask2 = order[i] == 1 ? 0 : HEAP_ONLY_TUPLE;
			tuples[i].frzflags = order[i] == 1 ? XLH_INVALID_XVAC : XLH_FREEZE_XVAC;
			tuples[i].checkflags = HEAP_FREEZE_CHECK_XMIN_COMMITTED;
		}
		if (shared)
			heap_freeze_apply_prepared(&relation_data, 1, 70, tuples, 3, &space_identity, false, NULL, NULL, NULL, InvalidFullTransactionId);
		else
			run_freeze(&relation_data, 1, 70, tuples, 3, NULL);
		check_publication(shared);
		UT_ASSERT_EQ(tx_checks, shared ? 0 : 3);
		if (inserts != 1)
			continue;
		UT_ASSERT_EQ(freeze_data_calls, 2);
		UT_ASSERT_EQ(freeze_block_length,
					 2 * sizeof(xl_heap_freeze_plan) + 3 * sizeof(OffsetNumber));
		header = (xl_heap_freeze_page *)freeze_main_data.data;
		UT_ASSERT_EQ(header->snapshotConflictHorizon, 70);
		UT_ASSERT(!header->isCatalogRel);
		UT_ASSERT_EQ(header->nplans, 2);
		plans = (xl_heap_freeze_plan *)freeze_block_data.data;
		offsets = (OffsetNumber *)(freeze_block_data.data + 2 * sizeof(*plans));
		for (int i = 0; i < 2; i++) {
			UT_ASSERT_EQ(plans[i].xmax, InvalidTransactionId);
			UT_ASSERT_EQ(plans[i].t_infomask, HEAP_XMIN_FROZEN | HEAP_XMAX_INVALID);
			UT_ASSERT_EQ(plans[i].t_infomask2, i == 0 ? 0 : HEAP_ONLY_TUPLE);
			UT_ASSERT_EQ(plans[i].frzflags, i == 0 ? XLH_INVALID_XVAC : XLH_FREEZE_XVAC);
			UT_ASSERT_EQ(plans[i].ntuples, i == 0 ? 1 : 2);
		}
		for (int i = 0; i < 3; i++) {
			HeapTupleHeader tuple = freeze_tuple(current_page.data, i + 1);

			UT_ASSERT_EQ(offsets[i], i + 1);
			/* Xvac's raw field remains observable after MOVED bits clear. */
			UT_ASSERT_EQ(HeapTupleHeaderGetRawCommandId(tuple),
						 i == 0 ? InvalidTransactionId : FrozenTransactionId);
			UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(tuple), 77);
			UT_ASSERT(HeapTupleHeaderXminFrozen(tuple));
			UT_ASSERT_EQ(tuple->t_infomask & HEAP_MOVED, 0);
		}
		replay_freeze_record();
	}
}

UT_TEST(test_freeze_kernel_rejects_invalid_space_before_write)
{
	for (int bad = 0; bad < 7; bad++) {
		HeapTupleFreeze tuple = { 0 };

		reset(XLOG_HEAP2_FREEZE_PAGE, true);
		tuple.offset = 1;
		tuple.t_infomask = HEAP_XMIN_FROZEN | HEAP_XMAX_INVALID;
		switch (bad) {
		case 0:
			space_identity.key.database_incarnation++;
			break;
		case 1:
			space_identity.key.locator.relNumber++;
			break;
		case 2:
			space_identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
			break;
		case 3:
			((PageHeader)current_page.data)->pd_block_scn = 0;
			break;
		case 4:
			break; /* no pre-lock identity */
		case 5:
			memset(space_identity.incarnation, 0, 16);
			break;
		case 6:
			relation_data.rd_locator.relNumber++;
			break;
		}
		memcpy(before_page.data, current_page.data, BLCKSZ);
		expecting_error = true;
		if (setjmp(error_jump) == 0) {
			heap_freeze_apply_prepared(&relation_data, 1, 70, &tuple, 1,
									 bad == 4 ? NULL : &space_identity, false, NULL, NULL, NULL, InvalidFullTransactionId);
			UT_ASSERT(false);
		}
		expecting_error = false;
		UT_ASSERT(memcmp(before_page.data, current_page.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(tx_checks + inserts + dirties + edges + CritSectionCount, 0);
		UT_ASSERT_EQ(next_token, 100);
	}
}

UT_TEST(test_prune_native_plan_and_version)
{
	reset(XLOG_HEAP2_PRUNE, true);
	execute(XLOG_HEAP2_PRUNE, &space_identity);
	check_publication(true);
	UT_ASSERT(ItemIdIsRedirected(PageGetItemId(current_page.data, 1)));
	UT_ASSERT_EQ(ItemIdGetRedirect(PageGetItemId(current_page.data, 1)), 3);
	UT_ASSERT(!ItemIdIsUsed(PageGetItemId(current_page.data, 2)));
	UT_ASSERT(ItemIdIsNormal(PageGetItemId(current_page.data, 3)));
}
UT_TEST(test_shared_freeze_refuses_before_tuple_or_wal)
{
	reset(XLOG_HEAP2_FREEZE_PAGE, true);
	expecting_error = true;
	if (setjmp(error_jump) == 0) {
		execute(XLOG_HEAP2_FREEZE_PAGE, &space_identity);
		UT_ASSERT(false);
	}
	expecting_error = false;
	UT_ASSERT_EQ(inserts + dirties + edges + CritSectionCount, 0);
	UT_ASSERT_EQ(memcmp(before_page.data, current_page.data, BLCKSZ), 0);
	for (int i = 1; i <= 2; i++)
		UT_ASSERT(!HeapTupleHeaderXminFrozen(
			(HeapTupleHeader)PageGetItem(current_page.data, PageGetItemId(current_page.data, i))));
}
UT_TEST(test_vacuum_native_unused_and_version)
{
	reset(XLOG_HEAP2_VACUUM, true);
	execute(XLOG_HEAP2_VACUUM, &space_identity);
	check_publication(true);
	UT_ASSERT_EQ(PageGetMaxOffsetNumber(current_page.data), 2);
}
UT_TEST(test_nonshared_native_maintenance)
{
	const uint8 ops[] = { XLOG_HEAP2_PRUNE, XLOG_HEAP2_FREEZE_PAGE, XLOG_HEAP2_VACUUM };
	for (unsigned i = 0; i < lengthof(ops); i++) {
		reset(ops[i], false);
		execute(ops[i], NULL);
		check_publication(false);
	}
}
UT_TEST(test_prune_hint_only_versions_actual_change)
{
	reset(XLOG_HEAP2_PRUNE, true);
	expected_info = XLOG_FPI_FOR_HINT;
	run_prune(false, &space_identity);
	check_publication(true);
	UT_ASSERT_EQ(hints, 0);
	UT_ASSERT(!PageIsFull(current_page.data));
	UT_ASSERT_EQ(((PageHeader)current_page.data)->pd_prune_xid, InvalidTransactionId);
	for (int i = 1; i <= 3; i++)
		UT_ASSERT(ItemIdIsNormal(PageGetItemId(current_page.data, i)));
}
UT_TEST(test_prune_hint_nonshared_keeps_native_behavior)
{
	reset(XLOG_HEAP2_PRUNE, false);
	run_prune(false, NULL);
	UT_ASSERT_EQ(hints, 1);
	UT_ASSERT_EQ(dirties + inserts + edges, 0);
	UT_ASSERT_EQ(next_token, 100);
	UT_ASSERT_EQ(((PageHeader)current_page.data)->pd_block_scn, 20);
}
UT_TEST(test_prune_hint_no_change_has_no_version_or_wal)
{
	reset(XLOG_HEAP2_FREEZE_PAGE, true);
	run_prune(false, NULL);
	UT_ASSERT_EQ(hints + dirties + inserts + edges, 0);
	UT_ASSERT_EQ(next_token, 100);
	UT_ASSERT(memcmp(before_page.data, current_page.data, BLCKSZ) == 0);
}
UT_TEST(test_prune_hint_missing_identity_refuses_before_mutation)
{
	reset(XLOG_HEAP2_PRUNE, true);
	expecting_error = true;
	if (setjmp(error_jump) == 0) {
		run_prune(false, NULL);
		UT_ASSERT(false);
	}
	expecting_error = false;
	UT_ASSERT(memcmp(before_page.data, current_page.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(hints + dirties + inserts + edges + CritSectionCount, 0);
	UT_ASSERT_EQ(next_token, 100);
}

UT_TEST(test_invalid_identity_refuses_before_mutation)
{
	const uint8 ops[] = { XLOG_HEAP2_PRUNE, XLOG_HEAP2_FREEZE_PAGE, XLOG_HEAP2_VACUUM };
	for (unsigned i = 0; i < lengthof(ops); i++)
		for (int bad = 0; bad < 7; bad++) {
			reset(ops[i], true);
			switch (bad) {
			case 0:
				space_identity.key.database_incarnation++;
				break;
			case 1:
				space_identity.key.locator.relNumber++;
				break;
			case 2:
				space_identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
				break;
			case 3:
				((PageHeader)current_page.data)->pd_block_scn = 0;
				break;
			case 4:
				break; /* no pre-lock identity */
			case 5:
				memset(space_identity.incarnation, 0, 16);
				break;
			case 6:
				relation_data.rd_locator.relNumber++;
				break;
			}
			memcpy(before_page.data, current_page.data, BLCKSZ);
			expecting_error = true;
			if (setjmp(error_jump) == 0) {
				execute(ops[i], bad == 4 ? NULL : &space_identity);
				UT_ASSERT(false);
			}
			expecting_error = false;
			UT_ASSERT(memcmp(before_page.data, current_page.data, BLCKSZ) == 0);
			UT_ASSERT_EQ(inserts + dirties + edges + hints, 0);
			UT_ASSERT_EQ(CritSectionCount, 0);
			UT_ASSERT_EQ(next_token, 100);
		}
}

UT_TEST(test_freeze_preserves_native_transaction_sanity_gate)
{
	reset(XLOG_HEAP2_FREEZE_PAGE, false);
	tx_committed = false;
	expecting_error = true;
	if (setjmp(error_jump) == 0) {
		execute(XLOG_HEAP2_FREEZE_PAGE, &space_identity);
		UT_ASSERT(false);
	}
	expecting_error = false;
	UT_ASSERT(memcmp(before_page.data, current_page.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(inserts + dirties + edges, 0);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(next_token, 100);
}

static unsigned identity_reads;
static bool identity_ready;
static unsigned maintenance_requests;
static bool maintenance_request_ready;

bool
cluster_heap_freeze_request_maintenance(void)
{
	UT_ASSERT_EQ(identity_reads, 1);
	UT_ASSERT_EQ(CritSectionCount + inserts + dirties + edges, 0);
	maintenance_requests++;
	return maintenance_request_ready;
}

static bool
maintenance_identity_io(Relation rel, ClusterSpaceIdentity *out)
{
	UT_ASSERT(rel == &relation_data);
	UT_ASSERT_EQ(CritSectionCount, 0);
	identity_reads++;
	*out = space_identity;
	return identity_ready;
}

UT_TEST(test_vacuum_native_entry_copies_prelock_identity)
{
	VacuumParams options = { .options = VACOPT_VACUUM }, *params = &options;
	struct {
		Relation rel;
		bool versioned;
		bool shared_maintenance_restricted;
		ClusterSpaceIdentity identity;
	} state = { 0 }, *vacrel = &state;
	Relation rel = &relation_data;
	reset(XLOG_HEAP2_VACUUM, true);
	identity_reads = 0;
	identity_ready = true;
#define cluster_space_relation_get_identity maintenance_identity_io
#include "test_cluster_heap_maintenance_identity.inc"
#undef cluster_space_relation_get_identity
	UT_ASSERT(vacrel->versioned && vacrel->rel == rel);
	UT_ASSERT_EQ(identity_reads, 1);
	UT_ASSERT(memcmp(&vacrel->identity, &space_identity, sizeof(space_identity)) == 0);
	space_identity.incarnation[0]++;
	UT_ASSERT(vacrel->identity.incarnation[0] != space_identity.incarnation[0]);
}

UT_TEST(test_vacuum_native_entry_refuses_missing_identity)
{
	VacuumParams options = { .options = VACOPT_VACUUM | VACOPT_FREEZE }, *params = &options;
	struct {
		Relation rel;
		bool versioned;
		bool shared_maintenance_restricted;
		ClusterSpaceIdentity identity;
	} state = { 0 }, *vacrel = &state;
	Relation rel = &relation_data;
	reset(XLOG_HEAP2_VACUUM, true);
	identity_reads = 0;
	identity_ready = false;
	maintenance_requests = 0;
	expecting_error = true;
	if (setjmp(error_jump) == 0) {
#define cluster_space_relation_get_identity maintenance_identity_io
#include "test_cluster_heap_maintenance_identity.inc"
#undef cluster_space_relation_get_identity
		UT_ASSERT(false);
	}
	expecting_error = false;
	UT_ASSERT_EQ(identity_reads, 1);
	UT_ASSERT_EQ(maintenance_requests, 0);
	UT_ASSERT_EQ(CritSectionCount + inserts + dirties + edges, 0);
	UT_ASSERT(memcmp(before_page.data, current_page.data, BLCKSZ) == 0);
}

static bool
maintenance_entry(bits32 options)
{
	VacuumParams settings = { .options = options, .freeze_min_age = 0 }, *params = &settings;
	struct {
		Relation rel;
		bool versioned;
		bool shared_maintenance_restricted;
		ClusterSpaceIdentity identity;
	} state = { 0 }, *vacrel = &state;
	Relation rel = &relation_data;

	identity_reads = maintenance_requests = 0;
	identity_ready = true;
#define cluster_space_relation_get_identity maintenance_identity_io
#include "test_cluster_heap_maintenance_identity.inc"
#undef cluster_space_relation_get_identity
	UT_ASSERT_EQ(CritSectionCount + inserts + dirties + edges, 0);
	return vacrel->shared_maintenance_restricted;
}

UT_TEST(test_explicit_shared_freeze_requests_before_content_locks)
{
	reset(XLOG_HEAP2_VACUUM, true);
	maintenance_request_ready = true;
	UT_ASSERT(maintenance_entry(VACOPT_VACUUM | VACOPT_FREEZE));
	UT_ASSERT_EQ(maintenance_requests, 1);
}

UT_TEST(test_ordinary_or_nonshared_vacuum_does_not_request)
{
	for (int scope = 0; scope < 5; scope++) {
		reset(XLOG_HEAP2_VACUUM, true);
		if (scope == 1)
			cluster_shared_config = false;
		else if (scope == 2)
			cluster_enabled = false;
		else if (scope == 3)
			relform.relpersistence = RELPERSISTENCE_TEMP;
		else if (scope == 4)
			relform.relpersistence = RELPERSISTENCE_UNLOGGED;
		(void)maintenance_entry(VACOPT_VACUUM | (scope == 0 ? 0 : VACOPT_FREEZE));
		UT_ASSERT_EQ(maintenance_requests, 0);
	}
}

UT_TEST(test_deferred_request_preserves_shared_freeze_restriction)
{
	reset(XLOG_HEAP2_VACUUM, true);
	maintenance_request_ready = false;
	UT_ASSERT(maintenance_entry(VACOPT_VACUUM | VACOPT_FREEZE));
	UT_ASSERT_EQ(maintenance_requests, 1);
}

UT_TEST(test_xmin_single_and_batch_require_full_target_not_census_only)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];
	for (int batch = 0; batch < 2; batch++) {
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		full_target_current = false;
		UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3)
			: qualified_xmin_apply(&f, 0)));
		UT_ASSERT_EQ(full_target_checks, 1);
		UT_ASSERT_EQ(admission_checks + wal_clock_reads + inserts + edges + dirties + flushes, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		UT_ASSERT(local_x && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_xmin_target_admission_keeps_original_census_recheck_last)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];
	qualified_xmin_batch_setup(&f, targets);
	revoke_target_census = true;
	UT_ASSERT(!qualified_xmin_batch_apply(&f, targets, 3));
	UT_ASSERT_EQ(full_target_checks, 1);
	UT_ASSERT_EQ(admission_checks, 1);
	UT_ASSERT_EQ(wal_clock_reads + inserts + edges + dirties + flushes, 0);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	UT_ASSERT(local_x && f.admission.entered);
	UT_ASSERT_EQ(backend_pin_count, 1);
}

UT_TEST(test_xmin_full_target_error_keeps_borrowed_resources_before_wal)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];
	qualified_xmin_batch_setup(&f, targets);
	target_recheck_error = expecting_error = true;
	if (setjmp(error_jump) == 0) {
		(void)qualified_xmin_batch_apply(&f, targets, 3);
		UT_ASSERT(false);
	} else {
		UT_ASSERT_EQ(CritSectionCount, 0);
		UT_ASSERT_EQ(full_target_checks, 1);
		UT_ASSERT_EQ(wal_clock_reads + inserts + edges + dirties + flushes, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		UT_ASSERT(local_x && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
	expecting_error = false;
}

UT_TEST(test_xmin_bad_final_authority_never_checks_full_target)
{
	QualifiedXminFixture f;
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
	authority_current = false;
	UT_ASSERT(!qualified_xmin_apply(&f, 0));
	UT_ASSERT_EQ(full_target_checks, 0);
	UT_ASSERT_EQ(admission_checks + wal_clock_reads + inserts + dirties + flushes, 0);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
}

UT_TEST(test_xmin_apply_requires_original_writer_context_before_wal)
{
	QualifiedXminFixture f;
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
	f.input.writer_context_valid = false;
	UT_ASSERT(!qualified_xmin_apply(&f, 0));
	UT_ASSERT_EQ(inserts + dirties + flushes + full_target_checks, 0);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
}

UT_TEST(test_xmin_apply_rejects_replacement_t3_with_same_buffer_generation)
{
	QualifiedXminFixture f;
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
	original_writer_context.ref.acquisition_generation++;
	UT_ASSERT(!qualified_xmin_apply(&f, 0));
	UT_ASSERT_EQ(inserts + dirties + flushes + full_target_checks, 0);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
}

UT_TEST(test_page_recapture_drops_original_writer_context)
{
	QualifiedXminFixture f;
	ResourceXWriterUseContext zero = {0};
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
	UT_ASSERT(f.input.writer_context_valid);
	UT_ASSERT(cluster_heap_freeze_capture_page_v1(&space_identity, 7,
		current_page.data, &f.input));
	UT_ASSERT(!f.input.writer_context_valid);
	UT_ASSERT_EQ(f.input.writer_object_revision, 0);
	UT_ASSERT(!f.input.writer_object_checked);
	UT_ASSERT(memcmp(&f.input.writer_context, &zero, sizeof(zero)) == 0);
	UT_ASSERT(!qualified_xmin_apply(&f, 0));
	UT_ASSERT_EQ(inserts + dirties + flushes + full_target_checks, 0);
}
UT_TEST(test_xmin_batch_rejects_replacement_original_writer_before_wal)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];
	qualified_xmin_batch_setup(&f, targets);
	original_writer_context.ref.acquisition_generation++;
	UT_ASSERT(!qualified_xmin_batch_apply(&f, targets, 3));
	UT_ASSERT_EQ(inserts + dirties + flushes + full_target_checks, 0);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
}

UT_TEST(test_xmin_single_batch_keep_original_writer_object_fence)
{
	QualifiedXminFixture f;
	ClusterHeapFreezeXminTargetV1 targets[3];
	for (int batch = 0; batch < 2; batch++)
		for (int which = 0; which < 2; which++) {
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			if (which) f.input.writer_object_checked = false;
			else f.input.writer_object_revision++;
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3)
				: qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(inserts + dirties + flushes + full_target_checks, 0);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
}

UT_TEST(test_xmin_actual_writer_keeps_original_ko_revision_before_lock)
{
	QualifiedXminFixture f; qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
	fixture_release_original_writer(); resume_allowed = true;
	current_object_revision = 8;
	UT_ASSERT(!qualified_xmin_apply(&f, 0));
	UT_ASSERT_EQ(conditional_resumes, 1);
	UT_ASSERT_EQ(local_lock_attempts + local_unlocks + conditional_unlocks, 0);
	UT_ASSERT_EQ(inserts + dirties + flushes, 0); UT_ASSERT_EQ(backend_pin_count, 1);
	UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
	UT_ASSERT(cluster_bufmgr_pcm_x_writer_find(GetBufferDescriptor(0)) == NULL);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
}

UT_TEST(test_xmin_actual_resumer_inner_error_cleans_before_native_wrapper)
{
	for (int after_bind = 0; after_bind < 2; after_bind++) {
		QualifiedXminFixture f; qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		fixture_release_original_writer(); resume_allowed = expecting_error = true;
		error_after_local_lock = !after_bind; error_after_ledger_bind = after_bind;
		if (setjmp(error_jump) == 0) {
			(void) qualified_xmin_apply(&f, 0); UT_ASSERT(false);
		} else {
			UT_ASSERT_EQ(conditional_resumes, 1); UT_ASSERT_EQ(local_lock_attempts, 1);
			UT_ASSERT_EQ(local_unlocks, 1); UT_ASSERT_EQ(conditional_unlocks, 0);
			UT_ASSERT_EQ(inserts + dirties + flushes, 0); UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT(cluster_bufmgr_pcm_x_writer_find(GetBufferDescriptor(0)) == NULL);
			UT_ASSERT(PG_exception_stack == NULL);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
		expecting_error = false;
	}
}

UT_TEST(test_xmin_released_writer_requires_actual_original_node_grant)
{
	QualifiedXminFixture f; qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
	fixture_release_original_writer(); resume_allowed = true; node_grant_current = false;
	UT_ASSERT(!qualified_xmin_apply(&f, 0));
	UT_ASSERT_EQ(inserts + dirties + flushes, 0);
	UT_ASSERT_EQ(backend_pin_count, 1);
	UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
}

UT_TEST(test_xmin_released_writer_single_and_batch_reach_native_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f;
		ClusterHeapFreezeXminTargetV1 targets[3];
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		fixture_release_original_writer(); resume_allowed = true;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3)
			: qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(conditional_resumes, 1); UT_ASSERT_EQ(conditional_unlocks, 1);
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_xmin_released_writer_refusal_never_fetches_or_writes)
{
	QualifiedXminFixture f; qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
	fixture_release_original_writer();
	UT_ASSERT(!qualified_xmin_apply(&f, 0));
	UT_ASSERT_EQ(conditional_resumes, 1);
	UT_ASSERT_EQ(conditional_unlocks + full_target_checks + inserts + dirties + flushes, 0);
	UT_ASSERT_EQ(backend_pin_count, 1);
	UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
}

UT_TEST(test_xmin_plan_refusal_before_resume_keeps_original_pin)
{
	QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
	qualified_xmin_batch_setup(&f, targets);
	fixture_release_original_writer(); resume_allowed = true;
	targets[2].original_creator_full_xid = InvalidFullTransactionId;
	UT_ASSERT(!qualified_xmin_batch_apply(&f, targets, 3));
	UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
	UT_ASSERT_EQ(writer_checks + route_checks + authority_checks + full_target_checks, 0);
	UT_ASSERT_EQ(inserts + dirties + flushes, 0); UT_ASSERT_EQ(backend_pin_count, 1);
	UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
}

UT_TEST(test_xmin_missing_proof_refuses_before_writer_resume)
{
	QualifiedXminFixture f; qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
	fixture_release_original_writer(); resume_allowed = true;
	f.proofs.proof_count = 0;
	UT_ASSERT(!qualified_xmin_apply(&f, 0));
	UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
	UT_ASSERT_EQ(writer_checks + route_checks + authority_checks + full_target_checks, 0);
	UT_ASSERT_EQ(inserts + dirties + flushes, 0);
	UT_ASSERT_EQ(backend_pin_count, 1);
	UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
}

UT_TEST(test_xmin_released_writer_requires_target_and_census_before_resume)
{
	QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];

	for (int batch = 0; batch < 2; batch++) {
		for (int variant = 0; variant < 3; variant++) {
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			fixture_release_original_writer(); resume_allowed = true;
			if (variant == 0) full_target_current = false;
			else if (variant == 1) admission_current = false;
			else preflight_revoke_census = true;
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3)
				: qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(target_preflight_checks, 1);
			UT_ASSERT_EQ(census_preflight_checks, variant == 0 ? 0 : 1);
			UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
			UT_ASSERT_EQ(full_target_checks + writer_checks + route_checks + authority_checks, 0);
			UT_ASSERT_EQ(inserts + dirties + flushes, 0);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_xmin_released_writer_rejects_missing_or_different_original_token)
{
	QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];

	for (int variant = 0; variant < 4; variant++) {
		qualified_xmin_batch_setup(&f, targets);
		fixture_release_original_writer(); resume_allowed = true;
		if (variant == 0) f.admission.entered = false;
		else if (variant == 1) f.admission.record_generation++;
		else if (variant == 2) f.input.writer_context.r4_record_generation++;
		UT_ASSERT(!cluster_heap_freeze_apply_xmin_batch_v1(&relation_data, 1,
			&f.input, &f.page, &f.proofs, &f.fence, &f.authority,
			variant == 3 ? NULL : &f.admission, targets, 3,
			FullTransactionIdFromEpochAndXid(1, 100), 10));
		UT_ASSERT_EQ(target_preflight_checks + census_preflight_checks, 0);
		UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
		UT_ASSERT_EQ(inserts + dirties + flushes, 0);
		UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(!local_x && !resumed_writer);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_unlocked_target_error_never_restores_writer)
{
	QualifiedXminFixture f; qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
	fixture_release_original_writer(); resume_allowed = preflight_target_error = expecting_error = true;
	if (setjmp(error_jump) == 0) {
		(void) qualified_xmin_apply(&f, 0); UT_ASSERT(false);
	} else {
		UT_ASSERT_EQ(target_preflight_checks, 1);
		UT_ASSERT_EQ(census_preflight_checks + conditional_resumes + conditional_unlocks, 0);
		UT_ASSERT_EQ(local_lock_attempts + inserts + dirties + flushes, 0);
		UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
	expecting_error = false;
}

UT_TEST(test_xmin_successful_preflight_still_requires_final_target)
{
	QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];

	for (int batch = 0; batch < 2; batch++) {
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		fixture_release_original_writer(); resume_allowed = allow_preflight_target = true;
		full_target_current = false;
		UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3)
			: qualified_xmin_apply(&f, 0)));
		UT_ASSERT_EQ(target_preflight_checks, 1); UT_ASSERT_EQ(census_preflight_checks, 1);
		UT_ASSERT_EQ(full_target_checks, 1);
		UT_ASSERT_EQ(conditional_resumes, 1); UT_ASSERT_EQ(conditional_unlocks, 1);
		UT_ASSERT_EQ(inserts + dirties + flushes, 0); UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_successful_preflight_still_requires_final_census)
{
	QualifiedXminFixture f; qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
	fixture_release_original_writer(); resume_allowed = revoke_target_census = true;
	UT_ASSERT(!qualified_xmin_apply(&f, 0));
	UT_ASSERT_EQ(target_preflight_checks, 1); UT_ASSERT_EQ(census_preflight_checks, 1);
	UT_ASSERT_EQ(full_target_checks, 1); UT_ASSERT_EQ(admission_checks, 1);
	UT_ASSERT_EQ(conditional_resumes, 1); UT_ASSERT_EQ(conditional_unlocks, 1);
	UT_ASSERT_EQ(inserts + dirties + flushes, 0); UT_ASSERT_EQ(backend_pin_count, 1);
	UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
}

UT_TEST(test_xmin_resumed_writer_target_error_runs_finally)
{
	QualifiedXminFixture f; qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
	fixture_release_original_writer(); resume_allowed = target_recheck_error = expecting_error = true;
	if (setjmp(error_jump) == 0) {
		(void) qualified_xmin_apply(&f, 0); UT_ASSERT(false);
	} else {
		UT_ASSERT_EQ(conditional_resumes, 1); UT_ASSERT_EQ(conditional_unlocks, 1);
		UT_ASSERT_EQ(inserts + dirties + flushes, 0); UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
	}
	expecting_error = false;
}

UT_TEST(test_xmin_resumed_writer_flush_error_runs_finally_after_wal)
{
	QualifiedXminFixture f; qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
	fixture_release_original_writer(); resume_allowed = flush_error = expecting_error = true;
	if (setjmp(error_jump) == 0) {
		(void) qualified_xmin_apply(&f, 0); UT_ASSERT(false);
	} else {
		UT_ASSERT_EQ(conditional_resumes, 1); UT_ASSERT_EQ(conditional_unlocks, 1);
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
	}
	expecting_error = false;
}

UT_TEST(test_xmin_resume_error_keeps_original_pin_and_admission)
{
	QualifiedXminFixture f; qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
	fixture_release_original_writer(); resume_error = expecting_error = true;
	if (setjmp(error_jump) == 0) {
		(void) qualified_xmin_apply(&f, 0); UT_ASSERT(false);
	} else {
		UT_ASSERT_EQ(conditional_resumes, 1); UT_ASSERT_EQ(conditional_unlocks, 0);
		UT_ASSERT_EQ(inserts + dirties + flushes, 0); UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
	}
	expecting_error = false;
}

UT_TEST(test_xmin_existing_local_x_never_reacquires_or_releases_it)
{
	QualifiedXminFixture f; qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
	resume_allowed = true;
	UT_ASSERT(qualified_xmin_apply(&f, 0));
	UT_ASSERT_EQ(conditional_resumes + conditional_unlocks, 0);
	UT_ASSERT(local_x && f.admission.entered);
	UT_ASSERT(fixture_node_grant_matches()); UT_ASSERT_EQ(backend_pin_count, 1);
}


/* Keep all captured/proof inputs mutually exact, but for another physical
 * relation. The live buffer, original writer and target relation stay put. */
static void
qualified_xmin_rebind_foreign_object(QualifiedXminFixture *f, int field)
{
	RelFileLocator other = f->input.identity.key.locator;

	if (field == 0) other.spcOid++;
	if (field == 1) other.dbOid++;
	if (field == 2) other.relNumber++;
	f->input.identity.key.locator = other;
	f->fence.identity.key.locator = other;
	space_identity.key.locator = other;
	UT_ASSERT(cluster_heap_freeze_prepare_page_v1(&f->input, &f->page));
	for (uint8 i = 0; i < f->proofs.proof_count; i++) {
		ClusterHeapFreezeCanonicalProofV1 *proof = &f->proofs.proofs[i];
		uint32 wrap = proof->resolution.locator_echo.tt_wrap;

		proof->requested_locator = f->page.carrier_requests[i].locator;
		proof->resolution.locator_echo = proof->requested_locator;
		proof->resolution.locator_echo.tt_wrap = wrap;
		proof->resolution.local_creation_target.locator = other;
		proof->expected_fence = proof->current_fence = f->fence;
	}
	UT_ASSERT_EQ(cluster_heap_freeze_bind_creation_targets_v1(&f->page,
		&f->proofs, &f->fence), f->proofs.proof_count);
}

UT_TEST(test_xmin_foreign_object_refuses_before_writer_or_native_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int released = 0; released < 2; released++) {
			for (int field = 0; field < 3; field++) {
				QualifiedXminFixture f;
				ClusterHeapFreezeXminTargetV1 targets[3];
				HeapTupleFreeze plans[MaxHeapTuplesPerPage];
				TransactionId conflict_horizon;
				bool owned_x;
				int pins;

				if (batch) qualified_xmin_batch_setup(&f, targets);
				else {
					qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
					targets[0] = (ClusterHeapFreezeXminTargetV1){
						.tuple_index = 0,
						.original_creator_full_xid = FullTransactionIdFromEpochAndXid(1, 77) };
				}
				qualified_xmin_rebind_foreign_object(&f, field);
				/* This refusal must come from the live-object boundary, not an
				 * internally inconsistent proof or an unqualified row plan. */
				UT_ASSERT(cluster_heap_freeze_prepare_xmin_batch_v1(&f.page, &f.proofs,
					&f.fence, targets, batch ? 2 : 1,
					FullTransactionIdFromEpochAndXid(1, 100), 10,
					plans, lengthof(plans), &conflict_horizon));
				if (released) { fixture_release_original_writer(); resume_allowed = true; }
				owned_x = local_x;
				pins = backend_pin_count;
				expecting_error = true;
				if (setjmp(error_jump) == 0) {
					UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 2)
						: qualified_xmin_apply(&f, 0)));
				} else {
					expecting_error = false;
					UT_ASSERT(false); /* Conditional refusal must not raise ERROR. */
				}
				expecting_error = false;
				UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
				UT_ASSERT_EQ(writer_checks + route_checks + admission_checks, 0);
				UT_ASSERT_EQ(backend_pin_count, pins);
				UT_ASSERT(local_x == owned_x && !resumed_writer && f.admission.entered);
				UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			}
		}
	}
}


UT_TEST(test_xmin_locked_kernel_refuses_foreign_proof_before_native_wal)
{
	for (int field = 0; field < 3; field++) {
		QualifiedXminFixture f;
		ClusterHeapFreezeXminTargetV1 target;
		HeapTupleFreeze plan;
		TransactionId conflict_horizon;

		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		qualified_xmin_rebind_foreign_object(&f, field);
		target = (ClusterHeapFreezeXminTargetV1){ .tuple_index = 0,
			.original_creator_full_xid = FullTransactionIdFromEpochAndXid(1, 77) };
		UT_ASSERT(cluster_heap_freeze_prepare_xmin_batch_v1(&f.page, &f.proofs,
			&f.fence, &target, 1, FullTransactionIdFromEpochAndXid(1, 100), 10,
			&plan, 1, &conflict_horizon));
		expecting_error = true;
		if (setjmp(error_jump) == 0) {
			UT_ASSERT(!cluster_heap_freeze_apply_xmin_batch_locked_v1(&relation_data,
				1, &f.input, &f.page, &f.fence, &f.authority, &f.admission,
				&target, 1, &plan, conflict_horizon,
				FullTransactionIdFromEpochAndXid(1, 100), 10));
		} else {
			expecting_error = false;
			UT_ASSERT(false);
		}
		expecting_error = false;
		UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
		UT_ASSERT_EQ(writer_checks + route_checks + admission_checks, 0);
		UT_ASSERT(local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_existing_share_lock_is_not_a_resume_scope)
{
	QualifiedXminFixture f; qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
	fixture_release_original_writer(); local_share = resume_allowed = true;
	UT_ASSERT(!qualified_xmin_apply(&f, 0));
	UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + inserts + dirties + flushes, 0);
	UT_ASSERT(!local_x && local_share && f.admission.entered);
	UT_ASSERT_EQ(backend_pin_count, 1);
}


UT_TEST(test_xmin_rejects_unknown_generation_or_foreign_origin_before_writer)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int released = 0; released < 2; released++) {
			for (int variant = 0; variant < 2; variant++) {
				QualifiedXminFixture f;
				ClusterHeapFreezeXminTargetV1 targets[3];
				bool owned_x;
				int pins;

				if (batch) qualified_xmin_batch_setup(&f, targets);
				else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
				if (variant == 0) f.proofs.proofs[0].resolution.local_canonical_generation = UINT32_MAX;
				else f.proofs.proofs[0].resolution.local_canonical.segment_id = CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
				if (released) { fixture_release_original_writer(); resume_allowed = true; }
				owned_x = local_x; pins = backend_pin_count;
				UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 2)
					: qualified_xmin_apply(&f, 0)));
				UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
				UT_ASSERT_EQ(writer_checks + route_checks + admission_checks, 0);
				UT_ASSERT_EQ(backend_pin_count, pins);
				UT_ASSERT(local_x == owned_x && !resumed_writer && f.admission.entered);
				UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			}
		}
	}
}

UT_TEST(test_xmin_space_refusal_is_conditional)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int released = 0; released < 2; released++) {
			QualifiedXminFixture f;
			ClusterHeapFreezeXminTargetV1 targets[3];
			volatile bool applied = false, caught = false;

			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			if (released) { fixture_release_original_writer(); resume_allowed = true; }
			/* The real SPACE component builder consumes this buffer observation.
			 * No prepare/stamp/WAL helper is replaced by a refusal stub. */
			space_buffer_permanent = false; expecting_error = true;
			PG_TRY();
			{
				applied = batch ? qualified_xmin_batch_apply(&f, targets, 2)
					: qualified_xmin_apply(&f, 0);
			}
			PG_CATCH(); { caught = true; } PG_END_TRY();
			expecting_error = false;
			UT_ASSERT(!caught && !applied);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
			UT_ASSERT_EQ(CritSectionCount, 0);
			UT_ASSERT_EQ(next_token, 100);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(f.admission.entered && local_x == !released && !resumed_writer);
			UT_ASSERT_EQ(conditional_resumes, released);
			UT_ASSERT_EQ(conditional_unlocks, released);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_xmin_registration_cut_rechecked_after_actual_space_prepare)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int released = 0; released < 2; released++) {
			QualifiedXminFixture f;
			ClusterHeapFreezeXminTargetV1 targets[3];
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
			if (released) { fixture_release_original_writer(); resume_allowed = true; }
			/* Keep the real SPACE component/prepare path. Only its original
			 * buffer observation leaf advances the controlled ingress cut. */
			advance_registration_in_space = true;
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 2)
				: qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(space_observation_checks, 1);
			/* Getter current and the original cohort entry check the cut before resume. */
			UT_ASSERT_EQ(registration_checks, released ? 6 : 2);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
			UT_ASSERT_EQ(CritSectionCount, 0);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(f.admission.entered && local_x == !released && !resumed_writer);
			UT_ASSERT_EQ(conditional_resumes, released);
			UT_ASSERT_EQ(conditional_unlocks, released);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

/* Final input identity remains required after the original SPACE prepare.
 * The controlled leaf changes saved observations, not shared source lifetime. */
UT_TEST(test_xmin_same_bounds_rechecked_after_actual_space_prepare)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int released = 0; released < 2; released++) {
			for (int kind = 0; kind < 2; kind++) {
				QualifiedXminFixture f;
				ClusterHeapFreezeXminTargetV1 targets[3];

				if (batch) qualified_xmin_batch_setup(&f, targets);
				else qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
				if (released) { fixture_release_original_writer(); resume_allowed = true; }
				replace_bounds_in_space = &f.cohort; replace_bound_kind = kind;
				UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 2)
					: qualified_xmin_apply(&f, 0)));
				UT_ASSERT_EQ(space_observation_checks, 1);
				UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
				UT_ASSERT_EQ(CritSectionCount, 0);
				UT_ASSERT_EQ(backend_pin_count, 1);
				UT_ASSERT(f.admission.entered && local_x == !released && !resumed_writer);
				UT_ASSERT_EQ(conditional_resumes, released);
				UT_ASSERT_EQ(conditional_unlocks, released);
				UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			}
		}
	}
}

UT_TEST(test_xmin_native_window_retained_through_actual_space_prepare)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int released = 0; released < 2; released++) {
			QualifiedXminFixture f;
			ClusterHeapFreezeXminTargetV1 targets[3];
			uint64 creator;

			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
			creator = batch ? Min(U64FromFullTransactionId(targets[0].original_creator_full_xid),
				U64FromFullTransactionId(targets[1].original_creator_full_xid))
				: U64FromFullTransactionId(f.page.tuples[0].original_creator_full_xid);
			f.policy.legal_next = FullTransactionIdFromU64(creator + UINT64CONST(0x7fffffff));
			f.cohort.future_full_xid_max = U64FromFullTransactionId(f.policy.legal_next);
			f.cohort.local_registration_cut.future_full = f.policy.legal_next;
			worker_current_publish_fixture(&f.cohort);
			replace_policy_in_space = &f.policy;
			if (released) { fixture_release_original_writer(); resume_allowed = true; }
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 2)
				: qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(space_observation_checks, 1);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
			UT_ASSERT_EQ(CritSectionCount, 0);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(f.admission.entered && local_x == !released && !resumed_writer);
			UT_ASSERT_EQ(conditional_resumes, released);
			UT_ASSERT_EQ(conditional_unlocks, released);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_xmin_space_error_still_propagates)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int released = 0; released < 2; released++) {
			QualifiedXminFixture f;
			ClusterHeapFreezeXminTargetV1 targets[3];
			volatile bool caught = false;

			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
			if (released) { fixture_release_original_writer(); resume_allowed = true; }
			space_permanence_error = expecting_error = true;
			PG_TRY();
			{
				if (batch) (void)qualified_xmin_batch_apply(&f, targets, 2);
				else (void)qualified_xmin_apply(&f, 0);
			}
			PG_CATCH(); { caught = true; } PG_END_TRY();
			expecting_error = false;
			UT_ASSERT(caught);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
			UT_ASSERT_EQ(CritSectionCount, 0);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(f.admission.entered && local_x == !released && !resumed_writer);
			UT_ASSERT_EQ(conditional_unlocks, released);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_original_freeze_space_refusal_still_errors)
{
	QualifiedXminFixture f;
	HeapTupleFreeze plan;
	volatile bool caught = false;

	qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
	UT_ASSERT(cluster_heap_freeze_prepare_xmin_v1(&f.page, &f.proofs, &f.fence,
		0, FullTransactionIdFromEpochAndXid(1, 77),
		FullTransactionIdFromEpochAndXid(1, 100), 10, &plan));
	space_buffer_permanent = false; expecting_error = true;
	PG_TRY();
	{
		heap_freeze_apply_prepared(&relation_data, 1, 77, &plan, 1, &space_identity, false, NULL, NULL, NULL, InvalidFullTransactionId);
	}
	PG_CATCH(); { caught = true; } PG_END_TRY();
	expecting_error = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(backend_pin_count, 1);
	UT_ASSERT(f.admission.entered && local_x);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
}

UT_TEST(test_xmin_apply_requires_same_bound_sources)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int released = 0; released < 2; released++) {
			for (int variant = 0; variant < 10; variant++) {
				QualifiedXminFixture f;
				ClusterHeapFreezeXminTargetV1 targets[3];

				if (batch) qualified_xmin_batch_setup(&f, targets);
				else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
				if (released) { fixture_release_original_writer(); resume_allowed = true; }
				switch (variant) {
				case 0: f.input.freeze_policy = NULL; break;
				case 1: f.input.freeze_cohort = NULL; break;
				case 2: f.policy.legal_next = InvalidFullTransactionId; break;
				case 3: f.policy.freeze_limit = FullTransactionIdFromEpochAndXid(1, 99); break;
				case 4: f.cohort.horizon_scn++; break;
				case 5: f.cohort.record_generation++; break;
				case 6: f.cohort.formation_epoch++; break;
				case 7: f.cohort.admitted_members_lo = 0; break;
				case 8: f.cohort.local_unqualified_entries = 1; break;
				case 9: f.cohort.active_full_xid_min--; break;
				}
				UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 2)
					: qualified_xmin_apply(&f, 0)));
				UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
				UT_ASSERT_EQ(writer_checks + route_checks + admission_checks, 0);
				UT_ASSERT_EQ(conditional_resumes + conditional_unlocks, 0);
				UT_ASSERT_EQ(backend_pin_count, 1);
				UT_ASSERT(f.admission.entered && local_x == !released && !resumed_writer);
				UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			}
		}
	}
}

UT_TEST(test_xmin_original_h_source_refusal_precedes_writer_use)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int released = 0; released < 2; released++) {
			for (int variant = 0; variant < 7; variant++) {
				QualifiedXminFixture f;
				ClusterHeapFreezeXminTargetV1 targets[3];
				if (batch) qualified_xmin_batch_setup(&f, targets);
				else qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
				if (released) { fixture_release_original_writer(); resume_allowed = true; }
				switch (variant) {
				case 0: f.cohort.source_local_horizon_scn = 0; break;
				case 1: f.cohort.source_local_horizon_scn = UINT64_MAX; break;
				case 2: f.cohort.source_local_horizon_scn = 9; break;
				case 3: f.cohort.source_local_horizon_scn = 11; break;
				case 4: f.cohort.source_local_history.sample_clock_scn = 0; break;
				case 5: f.cohort.source_local_history.sample_clock_scn = 9; break;
				case 6: f.cohort.source_local_history.contributor_kind =
					(ClusterHeapFreezeReportV2ContributorKind)5; break;
				}
				UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 2)
					: qualified_xmin_apply(&f, 0)));
				UT_ASSERT_EQ(writer_checks + route_checks + admission_checks, 0);
				UT_ASSERT_EQ(conditional_resumes + conditional_unlocks, 0);
				UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
				UT_ASSERT_EQ(backend_pin_count, 1);
				UT_ASSERT(f.admission.entered && local_x == !released && !resumed_writer);
				UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			}
		}
	}
}

UT_TEST(test_xmin_registration_cut_refusal_precedes_writer_use)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int released = 0; released < 2; released++) {
			for (int variant = 0; variant < 5; variant++) {
				QualifiedXminFixture f;
				ClusterHeapFreezeXminTargetV1 targets[3];
				if (batch) qualified_xmin_batch_setup(&f, targets);
				else qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
				if (released) { fixture_release_original_writer(); resume_allowed = true; }
				switch (variant) {
				case 0: ctrc_note_unqualified_registration(); break;
				case 1: f.cohort.local_registration_cut.unqualified_registration_generation = 0; break;
				case 2: f.cohort.local_registration_cut.unqualified_registration_generation = UINT64_MAX; break;
				case 3: f.cohort.local_registration_cut.status = CTRC_ACTIVE_SCAN_INCOMPLETE; break;
				case 4: f.cohort.local_registration_cut.unqualified_entries = 1; break;
				}
				UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 2)
					: qualified_xmin_apply(&f, 0)));
				UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
				UT_ASSERT_EQ(writer_checks + route_checks + admission_checks, 0);
				UT_ASSERT_EQ(conditional_resumes + conditional_unlocks, 0);
				UT_ASSERT_EQ(backend_pin_count, 1);
				UT_ASSERT(f.admission.entered && local_x == !released && !resumed_writer);
				UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			}
		}
	}
}

UT_TEST(test_xmin_actual_ingress_attachment_and_saturation_refuse_before_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int released = 0; released < 2; released++) {
			for (int variant = 0; variant < 4; variant++) {
				QualifiedXminFixture f;
				ClusterHeapFreezeXminTargetV1 targets[3];
				if (batch) qualified_xmin_batch_setup(&f, targets);
				else qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
				if (released) { fixture_release_original_writer(); resume_allowed = true; }
				switch (variant) {
				case 0: registration_attached = false; break;
				case 1: CtrcShared = NULL; break;
				case 2:
					pg_atomic_write_u64(&CtrcShared->unqualified_registration_generation,
						UINT64_MAX - 1);
					f.cohort.local_registration_cut.unqualified_registration_generation =
						UINT64_MAX - 1;
					ctrc_note_unqualified_registration();
					UT_ASSERT_EQ(pg_atomic_read_u64(
						&CtrcShared->unqualified_registration_generation), UINT64_MAX);
					ctrc_note_unqualified_registration();
					UT_ASSERT_EQ(pg_atomic_read_u64(
						&CtrcShared->unqualified_registration_generation), UINT64_MAX);
					break;
				case 3:
					pg_atomic_write_u64(&CtrcShared->unqualified_registration_generation, 0);
					f.cohort.local_registration_cut.unqualified_registration_generation = 0;
					break;
				}
				UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 2)
					: qualified_xmin_apply(&f, 0)));
				UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
				UT_ASSERT_EQ(writer_checks + route_checks + admission_checks, 0);
				UT_ASSERT_EQ(conditional_resumes + conditional_unlocks, 0);
				UT_ASSERT_EQ(backend_pin_count, 1);
				UT_ASSERT(f.admission.entered && local_x == !released && !resumed_writer);
				UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			}
		}
	}
}

UT_TEST(test_xmin_registration_cut_rechecked_after_final_callbacks)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int released = 0; released < 2; released++) {
			for (int callback = 0; callback < 2; callback++) {
				QualifiedXminFixture f;
				ClusterHeapFreezeXminTargetV1 targets[3];
				if (batch) qualified_xmin_batch_setup(&f, targets);
				else qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
				if (released) { fixture_release_original_writer(); resume_allowed = true; }
				advance_registration_after_target = callback == 0;
				advance_registration_in_clock = callback == 1;
				UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 2)
					: qualified_xmin_apply(&f, 0)));
				UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
				UT_ASSERT_EQ(full_target_checks, 1);
				UT_ASSERT_EQ(conditional_unlocks, released);
				UT_ASSERT_EQ(backend_pin_count, 1);
				UT_ASSERT(f.admission.entered && local_x == !released && !resumed_writer);
				UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			}
		}
	}
}

UT_TEST(test_xmin_rechecks_same_bound_sources_after_final_target)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int released = 0; released < 2; released++) {
			QualifiedXminFixture f;
			ClusterHeapFreezeXminTargetV1 targets[3];

			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
			if (released) { fixture_release_original_writer(); resume_allowed = true; }
			retire_bounds_after_target = &f.cohort;
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 2)
				: qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
			UT_ASSERT_EQ(full_target_checks, 1);
			UT_ASSERT_EQ(conditional_unlocks, released);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(f.admission.entered && local_x == !released && !resumed_writer);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_recapture_retires_previous_bound_sources)
{
	QualifiedXminFixture f;

	qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
	UT_ASSERT(f.input.freeze_policy != NULL && f.input.freeze_cohort != NULL);
	UT_ASSERT(cluster_heap_freeze_capture_page_v1(&space_identity, 7,
		current_page.data, &f.input));
	UT_ASSERT(f.input.freeze_policy == NULL && f.input.freeze_cohort == NULL);
}

UT_TEST(test_xmin_capture_refusal_precedes_writer_resume)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int released = 0; released < 2; released++) {
			for (int variant = 0; variant < 16; variant++) {
				QualifiedXminFixture f;
				ClusterHeapFreezeXminTargetV1 targets[3];

				if (batch) qualified_xmin_batch_setup(&f, targets);
				else qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
				if (released) { fixture_release_original_writer(); resume_allowed = true; }
				switch (variant) {
				case 0: f.input.block = InvalidBlockNumber; break;
				case 1: f.input.block++; break;
				case 2: f.input.identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED; break;
				case 3: f.input.identity.sequence++; break;
				case 4: f.input.identity.key.system_identifier++; break;
				case 5: f.input.identity.key.database_incarnation++; break;
				case 6: f.input.identity.key.storage_uuid[0] ^= 1; break;
				case 7: f.input.identity.incarnation[0] ^= 1; break;
				case 8: f.input.identity.operation++; break;
				case 9: f.input.max_offset++; break;
				case 10: f.input.normal_tuples++; break;
				case 11: f.input.multi_tuples++; break;
				case 12: ((PageHeader)f.input.image.data)->pd_lower = SizeOfPageHeaderData - 1; break;
				case 13: f.input.identity.key.locator.relNumber++; break;
				case 14: PageSetLSN(f.input.image.data, f.fence.page_lsn + 1); break;
				case 15: ((PageHeader)f.input.image.data)->pd_block_scn++; break;
				}
				UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 2)
					: qualified_xmin_apply(&f, 0)));
				UT_ASSERT_EQ(conditional_resumes + conditional_unlocks, 0);
				UT_ASSERT_EQ(writer_checks + route_checks + admission_checks, 0);
				UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
				UT_ASSERT_EQ(backend_pin_count, 1);
				UT_ASSERT(f.admission.entered && local_x == !released && !resumed_writer);
				UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			}
		}
	}
}

UT_TEST(test_xmin_native_horizon_refuses_creator_outside_legal_window)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int released = 0; released < 2; released++) {
			for (int past_half = 0; past_half < 2; past_half++) {
				QualifiedXminFixture f;
				ClusterHeapFreezeXminTargetV1 targets[3];
				uint64 creator;

				if (batch) qualified_xmin_batch_setup(&f, targets);
				else qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
			creator = batch ? Min(U64FromFullTransactionId(targets[0].original_creator_full_xid),
				U64FromFullTransactionId(targets[1].original_creator_full_xid))
				: U64FromFullTransactionId(f.page.tuples[0].original_creator_full_xid);
				f.policy.legal_next = FullTransactionIdFromU64(creator + UINT64CONST(0x80000000) + past_half);
				f.cohort.future_full_xid_max = U64FromFullTransactionId(f.policy.legal_next);
				f.cohort.local_registration_cut.future_full = f.policy.legal_next;
				if (released) { fixture_release_original_writer(); resume_allowed = true; }
				UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 2)
					: qualified_xmin_apply(&f, 0)));
				UT_ASSERT_EQ(writer_checks + route_checks + admission_checks, 0);
				UT_ASSERT_EQ(conditional_resumes + conditional_unlocks, 0);
				UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
				UT_ASSERT_EQ(backend_pin_count, 1);
				UT_ASSERT(f.admission.entered && local_x == !released && !resumed_writer);
				UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			}
		}
	}
}

UT_TEST(test_xmin_native_horizon_keeps_strictly_inside_legal_window)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int released = 0; released < 2; released++) {
			for (int inside = 0; inside < 2; inside++) {
				QualifiedXminFixture f;
				ClusterHeapFreezeXminTargetV1 targets[3];
				uint64 creator;

				if (batch) qualified_xmin_batch_setup(&f, targets);
				else qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
			creator = batch ? Min(U64FromFullTransactionId(targets[0].original_creator_full_xid),
				U64FromFullTransactionId(targets[1].original_creator_full_xid))
				: U64FromFullTransactionId(f.page.tuples[0].original_creator_full_xid);
				f.policy.legal_next = FullTransactionIdFromU64(creator + UINT64CONST(0x7fffffff) - inside);
				f.cohort.future_full_xid_max = U64FromFullTransactionId(f.policy.legal_next);
				f.cohort.local_registration_cut.future_full = f.policy.legal_next;
				worker_current_publish_fixture(&f.cohort);
				if (released) { fixture_release_original_writer(); resume_allowed = true; }
				UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 2)
					: qualified_xmin_apply(&f, 0));
				UT_ASSERT_EQ(inserts, 1);
				UT_ASSERT_EQ(flushes, 1);
				UT_ASSERT_EQ(conditional_resumes, released);
				UT_ASSERT_EQ(conditional_unlocks, released);
				UT_ASSERT_EQ(backend_pin_count, 1);
				UT_ASSERT(f.admission.entered && local_x == !released && !resumed_writer);
			}
		}
	}
}

UT_TEST(test_xmin_native_horizon_rechecks_legal_window_after_final_target)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int released = 0; released < 2; released++) {
			QualifiedXminFixture f;
			ClusterHeapFreezeXminTargetV1 targets[3];
			uint64 creator;

			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
			creator = batch ? Min(U64FromFullTransactionId(targets[0].original_creator_full_xid),
				U64FromFullTransactionId(targets[1].original_creator_full_xid))
				: U64FromFullTransactionId(f.page.tuples[0].original_creator_full_xid);
			f.policy.legal_next = FullTransactionIdFromU64(creator + UINT64CONST(0x7fffffff));
			f.cohort.future_full_xid_max = U64FromFullTransactionId(f.policy.legal_next);
			f.cohort.local_registration_cut.future_full = f.policy.legal_next;
			worker_current_publish_fixture(&f.cohort);
			advance_legal_next_after_target = &f.policy;
			if (released) { fixture_release_original_writer(); resume_allowed = true; }
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 2)
				: qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(full_target_checks, 1);
			UT_ASSERT_EQ(conditional_unlocks, released);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(f.admission.entered && local_x == !released && !resumed_writer);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}


/* Real native/SPACE bodies, with only the buffer observation/status and
 * scheduling leaves controlled. No native or positive qualification claim. */
static void
assert_post_space_refusal_resources(QualifiedXminFixture *f, bool released)
{
	UT_ASSERT_EQ(space_observation_checks, 1);
	UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(backend_pin_count, 1);
	UT_ASSERT(f->admission.entered && local_x == !released && !resumed_writer);
	UT_ASSERT_EQ(conditional_resumes, released);
	UT_ASSERT_EQ(conditional_unlocks, released);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
}

UT_TEST(test_xmin_final_admission_refusal_after_actual_space_is_before_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int released = 0; released < 2; released++) {
			for (int kind = 1; kind <= 2; kind++) {
				QualifiedXminFixture f;
				ClusterHeapFreezeXminTargetV1 targets[3];

				if (batch) qualified_xmin_batch_setup(&f, targets);
				else qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
				if (released) { fixture_release_original_writer(); resume_allowed = true; }
				revoke_admission_in_space = kind;
				UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 2)
					: qualified_xmin_apply(&f, 0)));
				UT_ASSERT_EQ(post_space_target_checks, 1);
				UT_ASSERT_EQ(post_space_census_checks, kind == 2);
				assert_post_space_refusal_resources(&f, released);
			}
		}
	}
}

UT_TEST(test_xmin_final_admission_error_after_actual_space_unwinds_only_new_writer)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int released = 0; released < 2; released++) {
			for (int kind = 3; kind <= 4; kind++) {
				QualifiedXminFixture f;
				ClusterHeapFreezeXminTargetV1 targets[3];

				if (batch) qualified_xmin_batch_setup(&f, targets);
				else qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
				if (released) { fixture_release_original_writer(); resume_allowed = true; }
				revoke_admission_in_space = kind;
				expecting_error = true;
				if (setjmp(error_jump) == 0) {
					(void)(batch ? qualified_xmin_batch_apply(&f, targets, 2)
						: qualified_xmin_apply(&f, 0));
					UT_ASSERT(false);
				}
				UT_ASSERT_EQ(post_space_error_seen, kind - 2);
				UT_ASSERT_EQ(post_space_target_checks, 1);
				UT_ASSERT_EQ(post_space_census_checks, kind == 4);
				assert_post_space_refusal_resources(&f, released);
			}
		}
	}
}

UT_TEST(test_xmin_final_admission_callbacks_precede_last_source_match)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int released = 0; released < 2; released++) {
			for (int kind = 1; kind <= 3; kind++) {
				QualifiedXminFixture f;
				ClusterHeapFreezeXminTargetV1 targets[3];

				if (batch) qualified_xmin_batch_setup(&f, targets);
				else qualified_xmin_setup(&f, HEAP_XMAX_INVALID);
				if (released) { fixture_release_original_writer(); resume_allowed = true; }
				change_source_after_space_target = kind;
				if (kind == 2) post_space_bounds = &f.cohort;
				if (kind == 3) post_space_policy = &f.policy;
				UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 2)
					: qualified_xmin_apply(&f, 0)));
				UT_ASSERT_EQ(post_space_target_checks, 1);
				UT_ASSERT_EQ(post_space_census_checks, 1);
				assert_post_space_refusal_resources(&f, released);
			}
		}
	}
}

UT_TEST(test_xmin_unlocked_source_refusal_precedes_original_writer_resume)
{
	QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];

	for (int batch = 0; batch < 2; batch++) {
		for (int variant = 0; variant < 6; variant++) {
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			fixture_release_original_writer(); resume_allowed = true;
			expected_current_policy = &f.policy; expected_current_cohort = &f.cohort;
			if (variant == 0) saved_policy_current = false;
			else saved_cohort_status = (ClusterHeapFreezeCohortStatus)variant;
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3)
				: qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(policy_current_checks, 1);
			UT_ASSERT_EQ(cohort_current_checks, variant != 0);
			UT_ASSERT_EQ(census_preflight_checks + conditional_resumes + conditional_unlocks, 0);
			UT_ASSERT_EQ(local_lock_attempts + inserts + dirties + edges + flushes, 0);
			UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_xmin_unlocked_current_checks_keep_saved_inputs_and_native_wal)
{
	QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];

	for (int batch = 0; batch < 2; batch++) {
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		fixture_release_original_writer(); resume_allowed = true;
		expected_current_policy = &f.policy; expected_current_cohort = &f.cohort;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3)
			: qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(policy_current_checks, 1); UT_ASSERT_EQ(cohort_current_checks, 1);
		UT_ASSERT_EQ(target_preflight_checks, 1); UT_ASSERT_EQ(census_preflight_checks, 1);
		UT_ASSERT_EQ(conditional_resumes, 1); UT_ASSERT_EQ(conditional_unlocks, 1);
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_xmin_unlocked_source_error_keeps_caller_pin_and_admission)
{
	QualifiedXminFixture f;

	for (int stage = 1; stage <= 2; stage++) {
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		fixture_release_original_writer(); resume_allowed = expecting_error = true;
		source_current_error_stage = stage;
		if (setjmp(error_jump) == 0) {
			(void)qualified_xmin_apply(&f, 0); UT_ASSERT(false);
		} else {
			UT_ASSERT_EQ(policy_current_checks, 1);
			UT_ASSERT_EQ(cohort_current_checks, source_current_error_stage == 2);
			UT_ASSERT_EQ(census_preflight_checks + conditional_resumes + conditional_unlocks, 0);
			UT_ASSERT_EQ(local_lock_attempts + inserts + dirties + edges + flushes, 0);
			UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
		expecting_error = false;
	}
}

UT_TEST(test_xmin_original_census_recheck_follows_unlocked_source_callbacks)
{
	QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];

	for (int batch = 0; batch < 2; batch++) {
		for (int stage = 1; stage <= 2; stage++) {
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			fixture_release_original_writer(); resume_allowed = true;
			source_current_revoke_census = stage;
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3)
				: qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(policy_current_checks, 1); UT_ASSERT_EQ(cohort_current_checks, 1);
			UT_ASSERT_EQ(census_preflight_checks, 1);
			UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
			UT_ASSERT_EQ(inserts + dirties + edges + flushes, 0);
			UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_xmin_borrowed_content_lock_never_calls_unlocked_source_getters)
{
	QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];

	for (int batch = 0; batch < 2; batch++) {
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		saved_policy_current = false; saved_cohort_status = CLUSTER_HEAP_FREEZE_COHORT_INVALID;
		source_current_error_stage = 1;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3)
			: qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(policy_current_checks + cohort_current_checks, 0);
		UT_ASSERT_EQ(conditional_resumes + conditional_unlocks, 0);
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_xmin_original_policy_current_uses_retained_allocation_fence)
{
	QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];

	for (int batch = 0; batch < 2; batch++) {
		for (int current = 0; current < 2; current++) {
			ClusterHeapFreezePolicySampleV1 before;

			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			fixture_release_original_writer(); resume_allowed = true;
			expected_current_policy = &f.policy; expected_current_cohort = &f.cohort;
			before = f.policy; saved_policy_current = current != 0;
			UT_ASSERT_EQ(batch ? qualified_xmin_batch_apply(&f, targets, 3)
				: qualified_xmin_apply(&f, 0), current != 0);
			UT_ASSERT_EQ(policy_current_checks, 1); UT_ASSERT_EQ(allocation_fence_checks, 1);
			UT_ASSERT_EQ(cohort_current_checks, current);
			UT_ASSERT_EQ(census_preflight_checks, current);
			UT_ASSERT_EQ(conditional_resumes, current); UT_ASSERT_EQ(conditional_unlocks, current);
			UT_ASSERT_EQ(inserts, current); UT_ASSERT_EQ(flushes, current);
			UT_ASSERT(memcmp(&f.policy, &before, sizeof(before)) == 0);
			UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
			if (!current) UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_xmin_original_policy_current_rejects_disabled_striping_before_fence)
{
	QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];

	for (int batch = 0; batch < 2; batch++) {
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		fixture_release_original_writer(); resume_allowed = true;
		cluster_xid_striping = false;
		UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3)
			: qualified_xmin_apply(&f, 0)));
		UT_ASSERT_EQ(policy_current_checks, 1);
		UT_ASSERT_EQ(allocation_fence_checks + cohort_current_checks + census_preflight_checks, 0);
		UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
		UT_ASSERT_EQ(inserts + dirties + edges + flushes, 0);
		UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_original_allocation_fence_error_preserves_caller_resources)
{
	QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];

	for (int batch = 0; batch < 2; batch++) {
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		fixture_release_original_writer(); resume_allowed = true;
		allocation_fence_error = expecting_error = true;
		if (setjmp(error_jump) == 0) {
			if (batch) (void)qualified_xmin_batch_apply(&f, targets, 3);
			else (void)qualified_xmin_apply(&f, 0);
			UT_ASSERT(false);
		} else {
			UT_ASSERT_EQ(policy_current_checks, 1); UT_ASSERT_EQ(allocation_fence_checks, 1);
			UT_ASSERT_EQ(cohort_current_checks + census_preflight_checks, 0);
			UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
			UT_ASSERT_EQ(inserts + dirties + edges + flushes, 0);
			UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
		expecting_error = false;
	}
}

UT_TEST(test_xmin_original_cohort_entry_refuses_unknown_saved_metadata)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int fault = 0; fault < 6; fault++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			fixture_release_original_writer(); resume_allowed = true;
			switch (fault) {
			case 0: f.cohort.source_local_sample_id = 0; break;
			case 1: f.cohort.source_started_at_us = 0; break;
			case 2: f.cohort.source_started_at_us = UINT64_MAX; break;
			case 3: f.cohort.source_interval_ms = CLUSTER_UNDO_HORIZON_INTERVAL_MIN_MS - 1; break;
			case 4: f.cohort.source_interval_ms = CLUSTER_UNDO_HORIZON_INTERVAL_MAX_MS + 1; break;
			case 5: UndoHorizonShmem = NULL; break;
			}
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(policy_current_checks, 1); UT_ASSERT_EQ(cohort_current_checks, 1);
			UT_ASSERT_EQ(cohort_sample_reads + cohort_backend_checks + census_preflight_checks, 0);
			UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
			UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_xmin_original_cohort_entry_rejects_missing_or_replaced_worker)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int fault = 0; fault < 5; fault++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			fixture_release_original_writer(); resume_allowed = true;
			cohort_sample_available = fault != 0; cohort_sample_fault = fault;
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(cohort_sample_reads, 1);
			UT_ASSERT_EQ(cohort_backend_checks + census_preflight_checks, 0);
			UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
			UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_xmin_original_cohort_final_cut_follows_backend_observation)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		fixture_release_original_writer(); resume_allowed = cohort_revoke_ingress = true;
		UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
		UT_ASSERT_EQ(cohort_sample_reads, 1); UT_ASSERT_EQ(cohort_backend_checks, 1);
		UT_ASSERT_EQ(census_preflight_checks + conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
		UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
		UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_original_cohort_current_reaches_same_native_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		ClusterHeapFreezeCohortObservation before;
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		before = f.cohort;
		fixture_release_original_writer(); resume_allowed = true;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(cohort_sample_reads, 1); UT_ASSERT_EQ(cohort_backend_checks, 1);
		UT_ASSERT_EQ(census_preflight_checks, 1);
		UT_ASSERT_EQ(conditional_resumes, 1); UT_ASSERT_EQ(conditional_unlocks, 1);
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(memcmp(&f.cohort, &before, sizeof(before)) == 0);
	}
}

UT_TEST(test_xmin_original_worker_read_error_preserves_caller_resources)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		fixture_release_original_writer(); resume_allowed = cohort_sample_error = expecting_error = true;
		if (setjmp(error_jump) == 0) {
			if (batch) (void)qualified_xmin_batch_apply(&f, targets, 3);
			else (void)qualified_xmin_apply(&f, 0);
			UT_ASSERT(false);
		} else {
			UT_ASSERT_EQ(cohort_sample_reads, 1);
			UT_ASSERT_EQ(cohort_backend_checks + census_preflight_checks, 0);
			UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
			UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
		expecting_error = false;
	}
}

/* Add a fixed required peer using the same saved numeric bounds. This is
 * fixture-qualified input, not authentication or a producer-retention grant. */
static void
qualified_xmin_require_peer(QualifiedXminFixture *f, int peer)
{
	if (peer < 64) f->cohort.admitted_members_lo |= UINT64CONST(1) << peer;
	else f->cohort.admitted_members_hi |= UINT64CONST(1) << (peer - 64);
	f->cohort.admission_fence.admitted_members_lo = f->cohort.admitted_members_lo;
	f->cohort.admission_fence.admitted_members_hi = f->cohort.admitted_members_hi;
	f->cohort.source_peer_active_full_xid_min[peer] = f->cohort.active_full_xid_min;
	f->cohort.source_peer_future_full_xid[peer] = f->cohort.future_full_xid_max;
	f->cohort.source_peer_horizon_scn[peer] = f->cohort.horizon_scn;
	f->cohort.source_peer_history[peer] = f->cohort.source_local_history;
	f->cohort.source_peers[peer] = (ClusterHeapFreezeCohortPeerObservation){
		.recv_at_us = 1000000, .interval_ms = CLUSTER_UNDO_HORIZON_INTERVAL_MIN_MS,
		.generation = 7, .seq = 2 };
	admission_current_publish_fixture(&f->cohort);
	worker_current_publish_fixture(&f->cohort);
}

UT_TEST(test_xmin_original_peer_backend_continues_native_wal)
{
	const int peers[] = { 1, 65 };
	for (int batch = 0; batch < 2; batch++) {
		for (unsigned i = 0; i < lengthof(peers); i++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			ClusterHeapFreezeCohortObservation before;
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			qualified_xmin_require_peer(&f, peers[i]); before = f.cohort;
			fixture_release_original_writer(); resume_allowed = true;
			UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
			UT_ASSERT_EQ(cohort_peer_current_checks, 1); UT_ASSERT_EQ(cohort_end_clock_reads, 1);
			UT_ASSERT_EQ(cohort_worker_current_checks, 2); UT_ASSERT_EQ(cohort_semantic_current_checks, 2);
			UT_ASSERT_EQ(conditional_resumes, 1); UT_ASSERT_EQ(conditional_unlocks, 1);
			UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
			UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(&f.cohort, &before, sizeof(before)) == 0);
		}
	}
}

UT_TEST(test_xmin_original_peer_sequence_generation_and_h_refuse_before_resume)
{
	const int peers[] = { 1, 65 };
	for (int batch = 0; batch < 2; batch++) {
		for (unsigned i = 0; i < lengthof(peers); i++) {
			for (int fault = 0; fault < 4; fault++) {
				QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
				int peer = peers[i];
				if (batch) qualified_xmin_batch_setup(&f, targets);
				else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
				qualified_xmin_require_peer(&f, peer);
				fixture_release_original_writer(); resume_allowed = true;
				switch (fault) {
				case 0: pg_atomic_write_u32(&cohort_shared.freeze_slots[peer].seq, 4); break;
				case 1: cohort_live_generation[peer]++; break;
				case 2: f.cohort.source_peers[peer].recv_at_us = 699999; break;
				case 3: f.cohort.source_peers[peer].recv_at_us = 1000001; break;
				}
				UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
				UT_ASSERT_EQ(cohort_backend_checks, 1);
				UT_ASSERT_EQ(cohort_peer_current_checks, fault != 0);
				UT_ASSERT_EQ(cohort_end_clock_reads, fault >= 2);
				UT_ASSERT_EQ(census_preflight_checks + conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
				UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
				UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
				UT_ASSERT_EQ(backend_pin_count, 1);
				UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			}
		}
	}
}

UT_TEST(test_xmin_original_cohort_rechecks_local_source_around_end_clock)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int fault = 0; fault < 5; fault++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			qualified_xmin_require_peer(&f, 1);
			fixture_release_original_writer(); resume_allowed = true;
			switch (fault) {
			case 0: cohort_worker_live = false; break;
			case 1: cohort_admission_live = false; break;
			case 2: cohort_peer_revoke_worker = true; break;
			case 3: cohort_clock_revoke_worker = true; break;
			case 4: cohort_clock_revoke_admission = true; break;
			}
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(cohort_worker_current_checks, fault >= 3 ? 2 : 1);
			UT_ASSERT_EQ(cohort_end_clock_reads, fault >= 3);
			UT_ASSERT_EQ(census_preflight_checks + conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
			UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_xmin_original_cohort_clock_rejects_stale_or_rollback)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int fault = 0; fault < 2; fault++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			fixture_release_original_writer(); resume_allowed = true;
			cohort_end_clock = fault == 0 ? 999999 : 1300001;
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(cohort_peer_current_checks, 0); UT_ASSERT_EQ(cohort_end_clock_reads, 1);
			UT_ASSERT_EQ(cohort_worker_current_checks, 1);
			UT_ASSERT_EQ(census_preflight_checks + conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
			UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_xmin_original_cohort_clock_error_preserves_caller_resources)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		fixture_release_original_writer(); resume_allowed = cohort_clock_error = expecting_error = true;
		if (setjmp(error_jump) == 0) {
			if (batch) (void)qualified_xmin_batch_apply(&f, targets, 3);
			else (void)qualified_xmin_apply(&f, 0);
			UT_ASSERT(false);
		} else {
			UT_ASSERT_EQ(cohort_backend_checks, 1); UT_ASSERT_EQ(cohort_end_clock_reads, 1);
			UT_ASSERT_EQ(census_preflight_checks + conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
			UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
		expecting_error = false;
	}
}

UT_TEST(test_xmin_original_worker_publication_continues_native_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		qualified_xmin_require_peer(&f, 65);
		fixture_release_original_writer(); resume_allowed = true;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		/* Getter current plus the original two backend comparisons. */
		UT_ASSERT_EQ(worker_lock_acquires, 3); UT_ASSERT_EQ(worker_lock_releases, 3);
		UT_ASSERT_EQ(worker_lease_checks, 3); UT_ASSERT_EQ(worker_admission_checks, 3);
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT_EQ(conditional_resumes, 1); UT_ASSERT_EQ(conditional_unlocks, 1);
		UT_ASSERT(!worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_xmin_original_worker_publication_refuses_stale_shared_image)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int fault = 0; fault < 11; fault++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			fixture_release_original_writer(); resume_allowed = true;
			switch (fault) {
			case 0: undo_cleaner_state = NULL; break;
			case 1: worker_current_enabled = false; break;
			case 2: worker_storage_ok = false; break;
			case 3: worker_stop_cut = true; break;
			case 4: native_worker_shared.shutdown_requested = true; break;
			case 5: native_worker_shared.workers[0].status = UNDO_CLEANER_SPAWNING; break;
			case 6: native_worker_shared.workers[0].pid++; break;
			case 7: native_worker_shared.active_sample.sample_id++; break;
			case 8: native_worker_shared.active_sample.scan.future_full = FullTransactionIdFromU64(1); break;
			case 9: native_worker_shared.active_sample.admission_fence.record_generation++; break;
			case 10: native_worker_shared.active_sample.worker_pid++; break;
			}
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
			/* Getter refusal (or outer cohort mismatch) precedes backend current.
			 * The changed admission fence is rejected before the compare lock. */
			UT_ASSERT_EQ(getter_copy_acquires, fault != 0);
			UT_ASSERT_EQ(getter_copy_releases, getter_copy_acquires);
			UT_ASSERT_EQ(worker_lock_acquires, fault >= 4 && fault != 9);
			UT_ASSERT_EQ(worker_lock_releases, worker_lock_acquires);
			UT_ASSERT_EQ(cohort_worker_current_checks, 0); UT_ASSERT_EQ(cohort_end_clock_reads, 0);
			UT_ASSERT_EQ(census_preflight_checks + conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
			UT_ASSERT(!worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_xmin_original_worker_fence_refusals_precede_publication_lock)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int admission = 0; admission < 2; admission++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			fixture_release_original_writer(); resume_allowed = true;
			if (admission) worker_admission_ok = false; else worker_lease_ok = false;
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(worker_lease_checks, 1); UT_ASSERT_EQ(worker_admission_checks, admission);
			UT_ASSERT_EQ(worker_lock_acquires + worker_lock_releases, 0);
			UT_ASSERT_EQ(census_preflight_checks + conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
			UT_ASSERT(!worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_xmin_original_worker_fence_error_preserves_caller_resources)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int admission = 0; admission < 2; admission++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			fixture_release_original_writer(); resume_allowed = expecting_error = true;
			if (admission) worker_admission_error = true; else worker_lease_error = true;
			if (setjmp(error_jump) == 0) {
				if (batch) (void)qualified_xmin_batch_apply(&f, targets, 3);
				else (void)qualified_xmin_apply(&f, 0);
				UT_ASSERT(false);
			} else {
				UT_ASSERT_EQ(worker_lease_checks, 1); UT_ASSERT_EQ(worker_admission_checks, admission);
				UT_ASSERT_EQ(worker_lock_acquires + worker_lock_releases, 0);
				UT_ASSERT_EQ(census_preflight_checks + conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
				UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
				UT_ASSERT(!worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
				UT_ASSERT_EQ(backend_pin_count, 1);
				UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			}
			expecting_error = false;
		}
	}
}

UT_TEST(test_xmin_original_worker_unlock_rechecks_actual_registration_cut)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		fixture_release_original_writer(); resume_allowed = worker_register_on_unlock = true;
		UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
		UT_ASSERT_EQ(worker_lock_acquires, 1); UT_ASSERT_EQ(worker_lock_releases, 1);
		UT_ASSERT_EQ(worker_lease_checks, 1); UT_ASSERT_EQ(worker_admission_checks, 1);
		UT_ASSERT_EQ(cohort_end_clock_reads, 0);
		UT_ASSERT_EQ(census_preflight_checks + conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
		UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
		UT_ASSERT(!worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_original_admission_capture_current_continues_native_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		qualified_xmin_require_peer(&f, 65);
		fixture_release_original_writer(); resume_allowed = true;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(admission_membership_reads, 5);
		UT_ASSERT_EQ(worker_admission_checks, 3); UT_ASSERT_EQ(cohort_semantic_current_checks, 2);
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT_EQ(conditional_resumes, 1); UT_ASSERT_EQ(conditional_unlocks, 1);
		UT_ASSERT(!worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_xmin_original_admission_refuses_changed_publication_or_identity)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int fault = 0; fault < 12; fault++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			fixture_release_original_writer(); resume_allowed = true;
			switch (fault) {
			case 0: SemanticActivationShmem = NULL; break;
			case 1: pg_atomic_write_u64(&native_admission_shared.admission_seq, 3); break;
			case 2: pg_atomic_write_u64(&native_admission_shared.admission_seq, 4); break;
			case 3: pg_atomic_fetch_add_u64(&native_admission_shared.record_generation, 1); break;
			case 4: pg_atomic_write_u32(&native_admission_shared.transition_closed, 1); break;
			case 5: pg_atomic_write_u64(&native_admission_shared.active_bits, UINT64CONST(1) << 63); break;
			case 6: pg_atomic_fetch_add_u64(&native_admission_shared.formation_epoch, 1); break;
			case 7: admission_system_id++; break;
			case 8: admission_cut++; break;
			case 9: admission_members.admitted_members_hi = UINT64CONST(1) << 1; break;
			case 10: admission_incarnation++; break;
			case 11: admission_members.admitted_incarnation[0]++; break;
			}
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(worker_lock_acquires + worker_lock_releases, 0);
			UT_ASSERT_EQ(census_preflight_checks + conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
			UT_ASSERT(!worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_xmin_original_admission_lifecycle_prerequisites_refuse_before_writer)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int fault = 0; fault < 10; fault++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			fixture_release_original_writer(); resume_allowed = true;
			switch (fault) {
			case 0: admission_recovering = true; break;
			case 1: admission_stopping = true; break;
			case 2: admission_serving = false; break;
			case 3: admission_join_allowed = false; break;
			case 4: admission_system_id = 0; break;
			case 5: admission_epoch = 0; break;
			case 6: admission_self = -1; break;
			case 7: admission_self = CLUSTER_MAX_NODES; break;
			case 8: admission_membership_available = false; break;
			case 9: admission_quorum = false; break;
			}
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(worker_lock_acquires + worker_lock_releases, 0);
			UT_ASSERT_EQ(census_preflight_checks + conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
			UT_ASSERT(!worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_xmin_original_admission_rechecks_after_membership_callback)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int fault = 1; fault <= 8; fault++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			fixture_release_original_writer(); resume_allowed = true;
			admission_membership_fault = fault;
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(admission_membership_reads, 1);
			UT_ASSERT_EQ(worker_lock_acquires + worker_lock_releases, 0);
			UT_ASSERT_EQ(census_preflight_checks + conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
			UT_ASSERT(!worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_xmin_original_admission_membership_error_preserves_caller_resources)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		fixture_release_original_writer(); resume_allowed = expecting_error = true;
		admission_membership_fault = 9;
		if (setjmp(error_jump) == 0) {
			if (batch) (void)qualified_xmin_batch_apply(&f, targets, 3);
			else (void)qualified_xmin_apply(&f, 0);
			UT_ASSERT(false);
		} else {
			UT_ASSERT_EQ(admission_membership_reads, 1);
			UT_ASSERT_EQ(worker_lock_acquires + worker_lock_releases, 0);
			UT_ASSERT_EQ(census_preflight_checks + conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
			UT_ASSERT(!worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
		expecting_error = false;
	}
}

UT_TEST(test_xmin_original_admission_excludes_heartbeat_generation_from_identity)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		qualified_xmin_require_peer(&f, 65);
		admission_members.observed_generation[0]++;
		admission_members.observed_generation[65]++;
		fixture_release_original_writer(); resume_allowed = true;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(admission_membership_reads, 5);
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(!worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_xmin_original_retained_lease_continues_native_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		qualified_xmin_require_peer(&f, 65);
		fixture_release_original_writer(); resume_allowed = true;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(lease_lock_acquires, 4); UT_ASSERT_EQ(lease_lock_releases, 4);
		UT_ASSERT_EQ(lease_incarnation_reads, 8); UT_ASSERT_EQ(lease_quorum_reads, 8);
		UT_ASSERT_EQ(allocation_fence_checks, 1); UT_ASSERT_EQ(worker_lease_checks, 3);
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(!lease_lock_held && !worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_xmin_original_retained_lease_refuses_state_drift_at_policy_and_worker)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int worker = 0; worker < 2; worker++) {
			for (int fault = 0; fault < 14; fault++) {
				QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
				if (batch) qualified_xmin_batch_setup(&f, targets);
				else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
				fixture_release_original_writer(); resume_allowed = true;
				if (worker) lease_after_policy_fault = fault; else retained_lease_fault(fault);
				UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
				UT_ASSERT_EQ(worker_lease_checks, worker);
				UT_ASSERT_EQ(worker_lock_acquires + worker_lock_releases, 0);
				UT_ASSERT_EQ(lease_lock_acquires, lease_lock_releases);
				UT_ASSERT_EQ(census_preflight_checks + conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
				UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
				UT_ASSERT(!lease_lock_held && !worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
				UT_ASSERT_EQ(backend_pin_count, 1);
				UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			}
		}
	}
}

UT_TEST(test_xmin_original_retained_lease_prerequisites_refuse_before_lock)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int fault = 0; fault < 6; fault++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			fixture_release_original_writer(); resume_allowed = true;
			switch (fault) {
			case 0: lease_cluster_on = false; break;
			case 1: lease_catalog_on = false; break;
			case 2: lease_slot = -1; break;
			case 3: lease_slot = CLUSTER_XID_STRIDE; break;
			case 4: lease_incarnation++; break;
			case 5: lease_quorum = false; break;
			}
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(lease_lock_acquires + lease_lock_releases, 0);
			UT_ASSERT_EQ(worker_lock_acquires + worker_lock_releases, 0);
			UT_ASSERT_EQ(census_preflight_checks + conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
			UT_ASSERT(!lease_lock_held && !worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_xmin_original_retained_lease_rechecks_incarnation_quorum_after_unlock)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int stage = 1; stage <= 2; stage++) {
			for (int kind = 1; kind <= 2; kind++) {
				QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
				if (batch) qualified_xmin_batch_setup(&f, targets);
				else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
				fixture_release_original_writer(); resume_allowed = true;
				lease_revoke_after_unlock = stage; lease_revoke_kind = kind;
				UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
				UT_ASSERT_EQ(lease_lock_acquires, stage); UT_ASSERT_EQ(lease_lock_releases, stage);
				UT_ASSERT_EQ(worker_lock_acquires + worker_lock_releases, 0);
				UT_ASSERT_EQ(census_preflight_checks + conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
				UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
				UT_ASSERT(!lease_lock_held && !worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
				UT_ASSERT_EQ(backend_pin_count, 1);
				UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			}
		}
	}
}

UT_TEST(test_xmin_original_retained_lease_observer_error_preserves_caller_resources)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int worker = 0; worker < 2; worker++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			fixture_release_original_writer(); resume_allowed = expecting_error = true;
			lease_error_read = worker ? 3 : 1;
			if (setjmp(error_jump) == 0) {
				if (batch) (void)qualified_xmin_batch_apply(&f, targets, 3);
				else (void)qualified_xmin_apply(&f, 0);
				UT_ASSERT(false);
			} else {
				UT_ASSERT_EQ(lease_lock_acquires, worker); UT_ASSERT_EQ(lease_lock_releases, worker);
				UT_ASSERT_EQ(worker_lock_acquires + worker_lock_releases, 0);
				UT_ASSERT_EQ(census_preflight_checks + conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
				UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
				UT_ASSERT(!lease_lock_held && !worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
				UT_ASSERT_EQ(backend_pin_count, 1);
				UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			}
			expecting_error = false;
		}
	}
}

UT_TEST(test_xmin_original_retained_lease_allows_same_identity_renewal)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		pg_atomic_fetch_add_u64(&StripeLease->limit_full, 100);
		fixture_release_original_writer(); resume_allowed = true;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(lease_lock_acquires, 4); UT_ASSERT_EQ(lease_lock_releases, 4);
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(!lease_lock_held && !worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_xmin_original_worker_getter_continues_native_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		qualified_xmin_require_peer(&f, 65);
		fixture_release_original_writer(); resume_allowed = true;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(cohort_sample_reads, 1);
		UT_ASSERT_EQ(getter_copy_acquires, 1); UT_ASSERT_EQ(getter_copy_releases, 1);
		UT_ASSERT_EQ(worker_lock_acquires, 3); UT_ASSERT_EQ(worker_lock_releases, 3);
		UT_ASSERT_EQ(worker_lease_checks, 3); UT_ASSERT_EQ(worker_admission_checks, 3);
		UT_ASSERT_EQ(lease_lock_acquires, 4); UT_ASSERT_EQ(lease_lock_releases, 4);
		UT_ASSERT_EQ(admission_membership_reads, 5);
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(!lease_lock_held && !worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_xmin_original_worker_getter_returns_shared_image_and_clears_refusal)
{
	QualifiedXminFixture f;
	ClusterUndoCleanerActiveSample out, zero;
	memset(&zero, 0, sizeof(zero));
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
	UT_ASSERT(!original_worker_publication_get(NULL));
	UT_ASSERT_EQ(getter_copy_acquires + worker_lock_acquires, 0);
	UT_ASSERT(original_worker_publication_get(&out));
	UT_ASSERT(memcmp(&out, &native_worker_shared.active_sample, sizeof(out)) == 0);
	for (int fault = 0; fault < 10; fault++) {
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		memset(&out, 0xa5, sizeof(out));
		switch (fault) {
		case 0: undo_cleaner_state = NULL; break;
		case 1: worker_current_enabled = false; break;
		case 2: native_worker_shared.shutdown_requested = true; break;
		case 3: native_worker_shared.workers[0].status = UNDO_CLEANER_SPAWNING; break;
		case 4: native_worker_shared.active_sample.worker_pid = 0; break;
		case 5: native_worker_shared.active_sample.sample_id = 0; break;
		case 6: native_worker_shared.active_sample.scan.status = CTRC_ACTIVE_SCAN_INCOMPLETE; break;
		case 7: pg_atomic_write_u32(&StripeLease->failed, 1); break;
		case 8: pg_atomic_write_u32(&native_admission_shared.transition_closed, 1); break;
		case 9: ctrc_note_unqualified_registration(); break;
		}
		UT_ASSERT(!original_worker_publication_get(&out));
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		UT_ASSERT_EQ(getter_copy_acquires, fault != 0); UT_ASSERT_EQ(getter_copy_acquires, getter_copy_releases);
		UT_ASSERT_EQ(worker_lock_acquires, worker_lock_releases);
		UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
		UT_ASSERT(!lease_lock_held && !worker_lock_held && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_xmin_original_worker_getter_rechecks_drift_after_copy_unlock)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int fault = 1; fault <= 7; fault++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			fixture_release_original_writer(); resume_allowed = true; getter_copy_fault = fault;
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(getter_copy_acquires, 1); UT_ASSERT_EQ(getter_copy_releases, 1);
			UT_ASSERT_EQ(worker_lock_acquires, worker_lock_releases);
			UT_ASSERT_EQ(cohort_backend_checks, 0);
			UT_ASSERT_EQ(census_preflight_checks + conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
			UT_ASSERT(!lease_lock_held && !worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_xmin_original_worker_getter_error_follows_copy_unlock)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int fault = 8; fault <= 9; fault++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			fixture_release_original_writer(); resume_allowed = expecting_error = true; getter_copy_fault = fault;
			if (setjmp(error_jump) == 0) {
				if (batch) (void)qualified_xmin_batch_apply(&f, targets, 3);
				else (void)qualified_xmin_apply(&f, 0);
				UT_ASSERT(false);
			} else {
				UT_ASSERT_EQ(getter_copy_acquires, 1); UT_ASSERT_EQ(getter_copy_releases, 1);
				UT_ASSERT_EQ(worker_lock_acquires + worker_lock_releases, 0);
				UT_ASSERT_EQ(cohort_backend_checks, 0);
				UT_ASSERT_EQ(census_preflight_checks + conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
				UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
				UT_ASSERT(!lease_lock_held && !worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
				UT_ASSERT_EQ(backend_pin_count, 1);
				UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			}
			expecting_error = false;
		}
	}
}

UT_TEST(test_xmin_original_worker_getter_new_sample_does_not_refresh_old_cohort)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		ClusterHeapFreezeCohortObservation saved;
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		saved = f.cohort;
		native_worker_shared.active_sample.sample_id++;
		fixture_release_original_writer(); resume_allowed = true;
		UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
		UT_ASSERT_EQ(getter_copy_acquires, 1); UT_ASSERT_EQ(getter_copy_releases, 1);
		UT_ASSERT_EQ(worker_lock_acquires, 1); UT_ASSERT_EQ(worker_lock_releases, 1);
		UT_ASSERT_EQ(cohort_backend_checks, 0);
		UT_ASSERT(memcmp(&f.cohort, &saved, sizeof(saved)) == 0);
		UT_ASSERT_EQ(census_preflight_checks + conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
		UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
		UT_ASSERT(!lease_lock_held && !worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_original_worker_publisher_getter_continues_native_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		qualified_xmin_require_peer(&f, 65);
		native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
		memset(&native_worker_shared.active_sample, 0, sizeof(native_worker_shared.active_sample));
		worker_original_publish_fixture(&f.cohort);
		UT_ASSERT_EQ(native_worker_shared.active_sample.sample_id, f.cohort.source_local_sample_id);
		UT_ASSERT_EQ(native_worker_shared.active_sample_sequence, f.cohort.source_local_sample_id);
		UT_ASSERT_EQ(publisher_lock_acquires, 1); UT_ASSERT_EQ(publisher_lock_releases, 1);
		UT_ASSERT(memcmp(&native_worker_shared.active_sample.scan, &publisher_scan, sizeof(publisher_scan)) == 0);
		UT_ASSERT(memcmp(&native_worker_shared.active_sample.lease_fence, &publisher_lease, sizeof(publisher_lease)) == 0);
		UT_ASSERT(memcmp(&native_worker_shared.active_sample.admission_fence, &publisher_admission, sizeof(publisher_admission)) == 0);
		fixture_release_original_writer(); resume_allowed = true;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(getter_copy_acquires, 1); UT_ASSERT_EQ(getter_copy_releases, 1);
		UT_ASSERT_EQ(worker_lock_acquires, 3); UT_ASSERT_EQ(worker_lock_releases, 3);
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(!worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_original_worker_publisher_refuses_malformed_completed_cursor)
{
	for (unsigned fault = 0; fault < 4; fault++)
	{
		QualifiedXminFixture f;
		ClusterUndoCleanerActiveSample empty = {0};
		uint64 sequence;

		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		UT_ASSERT(f.cohort.local_registration_cut.total_entries > 0);
		UT_ASSERT_EQ(f.cohort.local_registration_cut.next_index,
			f.cohort.local_registration_cut.total_entries);
		sequence = f.cohort.source_local_sample_id - 1;
		native_worker_shared.active_sample_sequence = sequence;
		switch (fault)
		{
		case 0:
			f.cohort.local_registration_cut.total_entries = 0;
			f.cohort.local_registration_cut.next_index = 0;
			break;
		case 1: f.cohort.local_registration_cut.next_index--; break;
		case 2: f.cohort.local_registration_cut.next_index++; break;
		case 3: f.cohort.local_registration_cut.lock_busy = true; break;
		}
		worker_original_publish_fixture(&f.cohort);
		UT_ASSERT(memcmp(&native_worker_shared.active_sample, &empty, sizeof(empty)) == 0);
		UT_ASSERT_EQ(native_worker_shared.active_sample_sequence, sequence);
		UT_ASSERT_EQ(publisher_lock_acquires, 1); UT_ASSERT_EQ(publisher_lock_releases, 1);
		UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
	}
}

UT_TEST(test_xmin_original_worker_publisher_incomplete_preserves_last_image)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		ClusterUndoCleanerActiveSample before;
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
		worker_original_publish_fixture(&f.cohort);
		before = native_worker_shared.active_sample;
		publisher_scan.status = CTRC_ACTIVE_SCAN_INCOMPLETE;
		publisher_scan.min_full = InvalidFullTransactionId;
		publisher_scan.future_full = InvalidFullTransactionId;
		publisher_lease.activation_generation++;
		publisher_admission.record_generation++;
		original_worker_publication_publish();
		UT_ASSERT(memcmp(&before, &native_worker_shared.active_sample, sizeof(before)) == 0);
		UT_ASSERT_EQ(native_worker_shared.active_sample_sequence, before.sample_id);
		UT_ASSERT_EQ(publisher_lock_acquires, 1); UT_ASSERT_EQ(publisher_lock_releases, 1);
		fixture_release_original_writer(); resume_allowed = true;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(!worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_xmin_original_worker_publisher_invalid_clears_before_native_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int fault = 0; fault < 5; fault++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			ClusterUndoCleanerActiveSample zero;
			uint64 sequence;
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
			worker_original_publish_fixture(&f.cohort);
			switch (fault) {
			case 0: publisher_scan.status = CTRC_ACTIVE_SCAN_INVALID; break;
			case 1: native_worker_shared.shutdown_requested = true; break;
			case 2: native_worker_shared.workers[0].status = UNDO_CLEANER_SPAWNING; break;
			case 3: native_worker_shared.workers[0].pid++; break;
			case 4: native_worker_shared.active_sample_sequence = UINT64_MAX; break;
			}
			sequence = native_worker_shared.active_sample_sequence;
			original_worker_publication_publish();
			memset(&zero, 0, sizeof(zero));
			UT_ASSERT(memcmp(&zero, &native_worker_shared.active_sample, sizeof(zero)) == 0);
			UT_ASSERT_EQ(native_worker_shared.active_sample_sequence, sequence);
			UT_ASSERT_EQ(publisher_lock_acquires, 2); UT_ASSERT_EQ(publisher_lock_releases, 2);
			fixture_release_original_writer(); resume_allowed = true;
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + inserts + edges + dirties + flushes, 0);
			UT_ASSERT(!worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_xmin_original_worker_publisher_nonowner_preserves_image)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int missing = 0; missing < 2; missing++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			ClusterUndoCleanerActiveSample before;
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
			worker_original_publish_fixture(&f.cohort);
			before = native_worker_shared.active_sample;
			publisher_scan.status = CTRC_ACTIVE_SCAN_INVALID;
			if (missing) undo_cleaner_state = NULL;
			else publisher_worker = 1;
			original_worker_publication_publish();
			UT_ASSERT(memcmp(&before, &native_worker_shared.active_sample, sizeof(before)) == 0);
			UT_ASSERT_EQ(native_worker_shared.active_sample_sequence, before.sample_id);
			UT_ASSERT_EQ(publisher_lock_acquires, 1); UT_ASSERT_EQ(publisher_lock_releases, 1);
			undo_cleaner_state = &native_worker_shared;
			fixture_release_original_writer(); resume_allowed = true;
			UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
			UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
			UT_ASSERT(!worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
		}
	}
}

UT_TEST(test_xmin_original_worker_publisher_sequence_exhaustion_does_not_wrap)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		ClusterUndoCleanerActiveSample out, zero;
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		f.cohort.source_local_sample_id = UINT64_MAX;
		native_worker_shared.active_sample_sequence = UINT64_MAX - 1;
		worker_original_publish_fixture(&f.cohort);
		UT_ASSERT(original_worker_publication_get(&out));
		UT_ASSERT_EQ(out.sample_id, UINT64_MAX);
		UT_ASSERT_EQ(native_worker_shared.active_sample_sequence, UINT64_MAX);
		original_worker_publication_publish();
		memset(&out, 0xa5, sizeof(out)); memset(&zero, 0, sizeof(zero));
		UT_ASSERT(!original_worker_publication_get(&out));
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		UT_ASSERT(memcmp(&native_worker_shared.active_sample, &zero, sizeof(zero)) == 0);
		UT_ASSERT_EQ(native_worker_shared.active_sample_sequence, UINT64_MAX);
		UT_ASSERT_EQ(publisher_lock_acquires, 2); UT_ASSERT_EQ(publisher_lock_releases, 2);
		fixture_release_original_writer(); resume_allowed = true;
		UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
		UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + inserts + edges + dirties + flushes, 0);
		UT_ASSERT(!worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_original_worker_publisher_unlock_revocation_blocks_native_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int fault = 1; fault <= 6; fault++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			ClusterUndoCleanerActiveSample out, zero;
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
			publisher_unlock_fault = fault;
			worker_original_publish_fixture(&f.cohort);
			UT_ASSERT_EQ(native_worker_shared.active_sample.sample_id, f.cohort.source_local_sample_id);
			UT_ASSERT_EQ(publisher_lock_acquires, 1); UT_ASSERT_EQ(publisher_lock_releases, 1);
			memset(&out, 0xa5, sizeof(out)); memset(&zero, 0, sizeof(zero));
			UT_ASSERT(!original_worker_publication_get(&out));
			UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
			fixture_release_original_writer(); resume_allowed = true;
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + inserts + edges + dirties + flushes, 0);
			UT_ASSERT_EQ(getter_copy_acquires, getter_copy_releases);
			UT_ASSERT_EQ(worker_lock_acquires, worker_lock_releases);
			UT_ASSERT(!worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_xmin_original_worker_pass_continues_native_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		qualified_xmin_require_peer(&f, 65);
		native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
		memset(&native_worker_shared.active_sample, 0, sizeof(native_worker_shared.active_sample));
		fixture_release_original_writer(); resume_allowed = true;
		worker_original_sample_pass_fixture(&f.cohort);
		UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
		UT_ASSERT(!pass_pending);
		UT_ASSERT_EQ(pass_capture_reads, 1); UT_ASSERT_EQ(pass_native_reads, 1);
		UT_ASSERT_EQ(pass_begin_reads, 1); UT_ASSERT_EQ(pass_step_reads, 1);
		UT_ASSERT_EQ(pass_lease_checks, 1); UT_ASSERT_EQ(pass_admission_checks, 1);
		UT_ASSERT(memcmp(&publisher_scan, &f.cohort.local_registration_cut, sizeof(publisher_scan)) != 0);
		UT_ASSERT(FullTransactionIdEquals(publisher_scan.min_full, f.cohort.local_registration_cut.min_full));
		UT_ASSERT(FullTransactionIdEquals(publisher_scan.future_full, f.cohort.local_registration_cut.future_full));
		/* The full original scan cursor must be retained by the cohort too. */
		f.cohort.local_registration_cut = publisher_scan;
		UT_ASSERT_EQ(native_worker_shared.active_sample.sample_id, f.cohort.source_local_sample_id);
		UT_ASSERT_EQ(publisher_lock_acquires, 1); UT_ASSERT_EQ(publisher_lock_releases, 1);
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(getter_copy_acquires, 1); UT_ASSERT_EQ(getter_copy_releases, 1);
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(!worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_xmin_original_worker_pass_capture_refusals_do_not_publish)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int fault = 0; fault < 3; fault++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			ClusterUndoCleanerActiveSample zero;
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
			pass_capture_allowed = fault != 0; pass_native_allowed = fault != 1; pass_begin_allowed = fault != 2;
			fixture_release_original_writer(); resume_allowed = true;
			worker_original_sample_pass_fixture(&f.cohort);
			memset(&zero, 0, sizeof(zero));
			UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_INVALID); UT_ASSERT(!pass_pending);
			UT_ASSERT(memcmp(&native_worker_shared.active_sample, &zero, sizeof(zero)) == 0);
			UT_ASSERT_EQ(native_worker_shared.active_sample_sequence, f.cohort.source_local_sample_id - 1);
			UT_ASSERT_EQ(pass_capture_reads, 1); UT_ASSERT_EQ(pass_native_reads, fault != 0);
			UT_ASSERT_EQ(pass_begin_reads, fault == 2); UT_ASSERT_EQ(pass_step_reads, 1);
			UT_ASSERT_EQ(pass_lease_checks + pass_admission_checks, 0);
			UT_ASSERT_EQ(publisher_lock_acquires, 1); UT_ASSERT_EQ(publisher_lock_releases, 1);
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + inserts + edges + dirties + flushes, 0);
			UT_ASSERT(!worker_lock_held && !lease_lock_held && !local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_xmin_original_worker_pass_continuation_does_not_resample)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		ClusterCtrcActiveScan first;
		ClusterXidStripeSampleFence lease;
		ClusterSemanticActiveSampleFence admission;
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
		memset(&native_worker_shared.active_sample, 0, sizeof(native_worker_shared.active_sample));
		fixture_release_original_writer(); resume_allowed = true;
		pass_incomplete_slices = 1;
		worker_original_sample_pass_fixture(&f.cohort);
		UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_INCOMPLETE);
		UT_ASSERT(pass_pending && publisher_scan.lock_busy);
		UT_ASSERT_EQ(publisher_lock_acquires + publisher_lock_releases, 0);
		first = publisher_scan; lease = publisher_lease; admission = publisher_admission;
		UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
		UT_ASSERT_EQ(conditional_resumes + inserts + edges + dirties + flushes, 0);
		pass_reference = pass_future = pass_active_min = InvalidFullTransactionId;
		pass_capture_allowed = pass_native_allowed = pass_begin_allowed = false;
		original_worker_sample_active_pass();
		UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_COMPLETE); UT_ASSERT(!pass_pending);
		UT_ASSERT(FullTransactionIdEquals(publisher_scan.reference_full, first.reference_full));
		UT_ASSERT(FullTransactionIdEquals(publisher_scan.min_full, first.min_full));
		UT_ASSERT(FullTransactionIdEquals(publisher_scan.future_full, first.future_full));
		UT_ASSERT(memcmp(&publisher_lease, &lease, sizeof(lease)) == 0);
		UT_ASSERT(memcmp(&publisher_admission, &admission, sizeof(admission)) == 0);
		UT_ASSERT_EQ(pass_capture_reads, 1); UT_ASSERT_EQ(pass_native_reads, 1);
		UT_ASSERT_EQ(pass_begin_reads, 1); UT_ASSERT_EQ(pass_step_reads, 2);
		UT_ASSERT_EQ(pass_lease_checks, 2); UT_ASSERT_EQ(pass_admission_checks, 2);
		UT_ASSERT_EQ(publisher_lock_acquires, 1); UT_ASSERT_EQ(publisher_lock_releases, 1);
		/* Start another apply attempt: reset only the three ordering observers,
		 * retaining every original shared/cursor/fence/resource state. */
		UT_ASSERT_EQ(target_preflight_checks, 1); UT_ASSERT_EQ(policy_current_checks, 1);
		UT_ASSERT_EQ(cohort_current_checks, 1); UT_ASSERT_EQ(census_preflight_checks, 0);
		target_preflight_checks = policy_current_checks = cohort_current_checks = 0;
		f.cohort.local_registration_cut = publisher_scan;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(!worker_lock_held && !lease_lock_held && !local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_xmin_original_worker_pass_rechecks_source_after_slice)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int fault = 1; fault <= 3; fault++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			ClusterUndoCleanerActiveSample zero;
			ClusterXidStripeSampleFence zero_lease;
			ClusterSemanticActiveSampleFence zero_admission;
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
			fixture_release_original_writer(); resume_allowed = true; pass_step_fault = fault;
			worker_original_sample_pass_fixture(&f.cohort);
			memset(&zero, 0, sizeof(zero)); memset(&zero_lease, 0, sizeof(zero_lease));
			memset(&zero_admission, 0, sizeof(zero_admission));
			UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_INVALID); UT_ASSERT(!pass_pending);
			UT_ASSERT(memcmp(&native_worker_shared.active_sample, &zero, sizeof(zero)) == 0);
			UT_ASSERT_EQ(native_worker_shared.active_sample_sequence, f.cohort.source_local_sample_id - 1);
			UT_ASSERT_EQ(pass_lease_checks, fault != 3); UT_ASSERT_EQ(pass_admission_checks, fault == 2);
			if (fault != 3) {
				UT_ASSERT(memcmp(&publisher_lease, &zero_lease, sizeof(zero_lease)) == 0);
				UT_ASSERT(memcmp(&publisher_admission, &zero_admission, sizeof(zero_admission)) == 0);
			}
			UT_ASSERT_EQ(publisher_lock_acquires, 1); UT_ASSERT_EQ(publisher_lock_releases, 1);
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + inserts + edges + dirties + flushes, 0);
			UT_ASSERT(!worker_lock_held && !lease_lock_held && !local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_xmin_original_worker_pass_registration_drift_stays_unproven)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		ClusterUndoCleanerActiveSample out, zero;
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
		fixture_release_original_writer(); resume_allowed = true; pass_step_fault = 4;
		worker_original_sample_pass_fixture(&f.cohort);
		/* This deliberately controlled COMPLETE leaf does not implement census
		 * requalification. The original later getter must still revoke it. */
		UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
		UT_ASSERT_EQ(native_worker_shared.active_sample.sample_id, f.cohort.source_local_sample_id);
		memset(&out, 0xa5, sizeof(out)); memset(&zero, 0, sizeof(zero));
		UT_ASSERT(!original_worker_publication_get(&out));
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		f.cohort.local_registration_cut = publisher_scan;
		UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
		UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + inserts + edges + dirties + flushes, 0);
		UT_ASSERT_EQ(publisher_lock_acquires, publisher_lock_releases);
		UT_ASSERT_EQ(getter_copy_acquires, getter_copy_releases);
		UT_ASSERT_EQ(worker_lock_acquires, worker_lock_releases);
		UT_ASSERT(!worker_lock_held && !lease_lock_held && !local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_original_worker_pass_new_round_revokes_old_cohort)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		uint64 first_id;
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
		fixture_release_original_writer(); resume_allowed = true;
		worker_original_sample_pass_fixture(&f.cohort);
		first_id = native_worker_shared.active_sample.sample_id;
		f.cohort.local_registration_cut = publisher_scan;
		original_worker_sample_active_pass();
		UT_ASSERT_EQ(native_worker_shared.active_sample.sample_id, first_id + 1);
		UT_ASSERT_EQ(pass_capture_reads, 2); UT_ASSERT_EQ(pass_native_reads, 2);
		UT_ASSERT_EQ(pass_begin_reads, 2); UT_ASSERT_EQ(pass_step_reads, 2);
		UT_ASSERT_EQ(publisher_lock_acquires, 2); UT_ASSERT_EQ(publisher_lock_releases, 2);
		UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
		UT_ASSERT_EQ(conditional_resumes + inserts + edges + dirties + flushes, 0);
		/* Start another apply attempt: reset only the three ordering observers,
		 * retaining every original shared/cursor/fence/resource state. */
		UT_ASSERT_EQ(target_preflight_checks, 1); UT_ASSERT_EQ(policy_current_checks, 1);
		UT_ASSERT_EQ(cohort_current_checks, 1); UT_ASSERT_EQ(census_preflight_checks, 0);
		target_preflight_checks = policy_current_checks = cohort_current_checks = 0;
		f.cohort.source_local_sample_id = native_worker_shared.active_sample.sample_id;
		f.cohort.local_registration_cut = publisher_scan;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(!worker_lock_held && !lease_lock_held && !local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_xmin_original_native_sampler_continues_worker_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		qualified_xmin_require_peer(&f, 65);
		native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
		memset(&native_worker_shared.active_sample, 0, sizeof(native_worker_shared.active_sample));
		fixture_release_original_writer(); resume_allowed = true;
		worker_original_sample_pass_fixture(&f.cohort);
		UT_ASSERT_EQ(sampler_xid_locks, 1); UT_ASSERT_EQ(sampler_proc_locks, 1);
		UT_ASSERT_EQ(sampler_lock_acquires, 2); UT_ASSERT_EQ(sampler_lock_releases, 2);
		UT_ASSERT_EQ(sampler_future_reads, 1); UT_ASSERT(!sampler_lock_held);
		UT_ASSERT(FullTransactionIdEquals(publisher_scan.reference_full, sampler_variables.nextXid));
		UT_ASSERT(FullTransactionIdEquals(publisher_scan.min_full, f.cohort.local_registration_cut.min_full));
		f.cohort.local_registration_cut = publisher_scan;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(!worker_lock_held && !lease_lock_held && !local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_xmin_original_native_sampler_refuses_unknown_identity_and_window)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int fault = 0; fault < 11; fault++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			ClusterUndoCleanerActiveSample zero;
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
			switch (fault) {
			case 0: sampler_xids[0] = FrozenTransactionId; break;
			case 1: pg_atomic_write_u64(&sampler_slots[0].cluster_top_full_xid_atomic, 0); break;
			case 2: pg_atomic_fetch_add_u64(&sampler_slots[0].cluster_top_full_xid_atomic, 1); break;
			case 3: pg_atomic_write_u64(&sampler_slots[0].cluster_top_full_xid_atomic, UINT64_MAX); break;
			case 4: sampler_proc_array->numProcs = -1; break;
			case 5: sampler_proc_array->numProcs = 4; break;
			case 6: sampler_variables.nextXid = InvalidFullTransactionId; break;
			case 7: sampler_future_allowed = false; break;
			case 8: sampler_recovering = true; break;
			case 9: sampler_proc_global->xids = NULL; break;
			case 10: sampler_cache = NULL; break;
			}
			fixture_release_original_writer(); resume_allowed = true;
			worker_original_sample_pass_fixture(&f.cohort);
			memset(&zero, 0, sizeof(zero));
			UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_INVALID); UT_ASSERT(!pass_pending);
			UT_ASSERT(memcmp(&zero, &native_worker_shared.active_sample, sizeof(zero)) == 0);
			UT_ASSERT_EQ(pass_native_reads, 1); UT_ASSERT_EQ(pass_begin_reads, 0);
			UT_ASSERT_EQ(sampler_lock_acquires, sampler_lock_releases); UT_ASSERT(!sampler_lock_held);
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + inserts + edges + dirties + flushes, 0);
			UT_ASSERT(!worker_lock_held && !lease_lock_held && !local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_xmin_original_native_sampler_uses_compact_membership)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
		sampler_proc_array->numProcs = 2;
		sampler_proc_array->pgprocnos[0] = 2; sampler_proc_array->pgprocnos[1] = 0;
		sampler_xids[0] = 150; sampler_xids[1] = 100;
		pg_atomic_write_u64(&sampler_slots[2].cluster_top_full_xid_atomic, U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(1, 150)));
		/* PID0 is a controlled dummy shape, not a full PREPARE lifecycle. */
		UT_ASSERT_EQ(sampler_slots[0].pid, 0);
		fixture_release_original_writer(); resume_allowed = true;
		worker_original_sample_pass_fixture(&f.cohort);
		UT_ASSERT(FullTransactionIdEquals(publisher_scan.min_full, FullTransactionIdFromEpochAndXid(1, 100)));
		UT_ASSERT_EQ(sampler_xid_locks, 1); UT_ASSERT_EQ(sampler_proc_locks, 1);
		UT_ASSERT_EQ(sampler_lock_acquires, sampler_lock_releases);
		f.cohort.local_registration_cut = publisher_scan;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(!sampler_lock_held && !worker_lock_held && !local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_xmin_original_native_sampler_retains_xidgen_snapshot)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		FullTransactionId before;
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
		before = sampler_variables.nextXid; sampler_advance_on_xid_unlock = true;
		fixture_release_original_writer(); resume_allowed = true;
		worker_original_sample_pass_fixture(&f.cohort);
		UT_ASSERT(FullTransactionIdEquals(publisher_scan.reference_full, before));
		UT_ASSERT(!FullTransactionIdEquals(publisher_scan.reference_full, sampler_variables.nextXid));
		UT_ASSERT(FullTransactionIdEquals(publisher_scan.future_full, f.policy.legal_next));
		UT_ASSERT_EQ(sampler_xid_locks, 1); UT_ASSERT_EQ(sampler_proc_locks, 1);
		UT_ASSERT_EQ(sampler_future_reads, 1); UT_ASSERT(!sampler_lock_held);
		f.cohort.local_registration_cut = publisher_scan;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(!worker_lock_held && !lease_lock_held && !local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_xmin_original_native_sampler_fullxid_fold_and_atomic_refusal)
{
	QualifiedXminFixture f;
	FullTransactionId reference, future, minimum;
	ClusterXidStripeSampleFence fence, zero;
	unsigned locks;
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); fixture_release_original_writer();
	sampler_variables.nextXid = pass_future = FullTransactionIdFromEpochAndXid(2, 200);
	/* This API-only epoch-fold case also needs an actual covering lease. */
	pg_atomic_write_u64(&StripeLease->limit_full, U64FromFullTransactionId(pass_future) + CLUSTER_XID_STRIDE);
	sampler_proc_array->numProcs = 2;
	sampler_proc_array->pgprocnos[0] = 0; sampler_proc_array->pgprocnos[1] = 2;
	sampler_xids[0] = 100; sampler_xids[1] = 3;
	pg_atomic_write_u64(&sampler_slots[2].cluster_top_full_xid_atomic, U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(2, 3)));
	UT_ASSERT(original_native_fullxid_sample(&reference, &future, &minimum, &fence));
	UT_ASSERT(FullTransactionIdEquals(minimum, FullTransactionIdFromEpochAndXid(1, 100)));
	UT_ASSERT(FullTransactionIdEquals(reference, sampler_variables.nextXid));
	UT_ASSERT(FullTransactionIdEquals(future, pass_future));
	locks = sampler_lock_acquires;
	memset(&fence, 0xa5, sizeof(fence)); memset(&zero, 0, sizeof(zero));
	UT_ASSERT(!original_native_fullxid_sample(&reference, &reference, &minimum, &fence));
	UT_ASSERT(!FullTransactionIdIsValid(reference) && !FullTransactionIdIsValid(minimum));
	UT_ASSERT(memcmp(&fence, &zero, sizeof(fence)) == 0);
	UT_ASSERT_EQ(sampler_lock_acquires, locks); UT_ASSERT_EQ(sampler_lock_acquires, sampler_lock_releases);
	UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
	UT_ASSERT(!sampler_lock_held && !worker_lock_held && !local_x && f.admission.entered);
	UT_ASSERT_EQ(backend_pin_count, 1);
}

UT_TEST(test_xmin_original_stripe_future_refuses_exhausted_lease)
{
	QualifiedXminFixture f;
	FullTransactionId future = FullTransactionIdFromU64(1);
	ClusterXidStripeSampleFence fence;

	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); fixture_release_original_writer();
	memset(&fence, 0xa5, sizeof(fence));
	sampler_lock_acquire(sampler_xid_lock, LW_SHARED);
	/* The reference equals the lease limit: no future may be issued. */
	UT_ASSERT(!sampler_stripe_future(FullTransactionIdFromU64(retained_lease_fence.lease_limit_full), &future, &fence));
	sampler_lock_release(sampler_xid_lock);
	UT_ASSERT(!FullTransactionIdIsValid(future));
	UT_ASSERT_EQ(fence.incarnation, 0);
	UT_ASSERT_EQ(sampler_lock_acquires, sampler_lock_releases);
}

UT_TEST(test_xmin_original_stripe_future_continues_native_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		qualified_xmin_require_peer(&f, 65);
		native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
		fixture_release_original_writer(); resume_allowed = true;
		worker_original_sample_pass_fixture(&f.cohort);
		UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
		UT_ASSERT_EQ(U64FromFullTransactionId(publisher_scan.future_full) % CLUSTER_XID_STRIDE, (uint64) lease_slot);
		UT_ASSERT(U64FromFullTransactionId(publisher_scan.future_full) < pg_atomic_read_u64(&StripeLease->limit_full));
		UT_ASSERT_EQ(sampler_xid_locks, 1); UT_ASSERT_EQ(sampler_proc_locks, 1);
		UT_ASSERT_EQ(sampler_future_reads, 1);
		f.cohort.local_registration_cut = publisher_scan;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT_EQ(lease_lock_acquires, lease_lock_releases);
		UT_ASSERT(lease_lock_acquires >= 3);
		UT_ASSERT(!sampler_lock_held && !lease_lock_held && !worker_lock_held && !local_x && !resumed_writer);
		UT_ASSERT(f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_xmin_original_stripe_future_refuses_unavailable_inputs)
{
	for (int fault = 0; fault < 27; fault++) {
		QualifiedXminFixture f; ClusterUndoCleanerActiveSample zero;
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
		switch (fault) {
		case 0: lease_cluster_on = false; break;
		case 1: lease_catalog_on = false; break;
		case 2: cluster_xid_striping = false; break;
		case 3: StripeBootShmem = NULL; break;
		case 4: StripeLease = NULL; break;
		case 5: lease_quorum = false; break;
		case 6: lease_slot = -1; break;
		case 7: lease_slot = CLUSTER_XID_STRIDE; break;
		case 8: lease_incarnation = 0; break;
		case 9: native_lease_shared.disk_state = CLUSTER_XID_STRIPE_DISK_UNKNOWN; break;
		case 10: native_lease_shared.floor_full = 0; break;
		case 11: native_lease_shared.epoch = 0; break;
		case 12: native_lease_shared.generation = 0; break;
		case 13: native_lease_shared.slot_state = CLUSTER_XID_STRIPE_SLOT_UNKNOWN; break;
		case 14: native_lease_shared.my_slot_claimed = false; break;
		case 15: native_lease_shared.my_slot_floor_full = 0; break;
		case 16: native_lease_shared.my_owner_incarnation = 0; break;
		case 17: pg_atomic_write_u64(&StripeLease->floor_full, 0); break;
		case 18: pg_atomic_write_u64(&StripeLease->limit_full, retained_lease_fence.lease_floor_full); break;
		case 19: pg_atomic_fetch_add_u64(&StripeLease->published_incarnation, 1); break;
		case 20: pg_atomic_write_u32(&StripeLease->failed, 1); break;
		case 21: pg_atomic_write_u32(&native_lease_shared.wrap_barrier_done, 0); break;
		case 22:
			pg_atomic_write_u64(&native_lease_shared.cluster_min_active_hwm, retained_lease_fence.lease_floor_full);
			sampler_herding_slack = 1; break;
		case 23: pg_atomic_write_u64(&native_lease_shared.herding_floor_full, retained_lease_fence.lease_limit_full); break;
		case 24: sampler_variables.nextXid = FullTransactionIdFromEpochAndXid(1, FrozenTransactionId); break;
		case 25: sampler_variables.nextXid = InvalidFullTransactionId; break;
		case 26: pg_atomic_write_u64(&StripeLease->limit_full, U64FromFullTransactionId(sampler_variables.nextXid)); break;
		}
		fixture_release_original_writer(); resume_allowed = true;
		worker_original_sample_pass_fixture(&f.cohort);
		memset(&zero, 0, sizeof(zero));
		UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_INVALID);
		UT_ASSERT_EQ(sampler_proc_locks, 0); UT_ASSERT_EQ(pass_begin_reads, 0);
		UT_ASSERT(memcmp(&zero, &native_worker_shared.active_sample, sizeof(zero)) == 0);
		UT_ASSERT(!qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes + conditional_unlocks, 0);
		UT_ASSERT_EQ(sampler_lock_acquires, sampler_lock_releases);
		UT_ASSERT_EQ(lease_lock_acquires, lease_lock_releases);
		UT_ASSERT(!sampler_lock_held && !lease_lock_held && !worker_lock_held && !local_x && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_original_stripe_future_rounding_and_wrap_are_readonly)
{
	static const struct { uint64 reference, herd, limit, expected; int slot; } cases[] = {
		{ UINT64CONST(0x1000000c3), 0, UINT64CONST(0x100000100), UINT64CONST(0x1000000c8), 8 },
		{ UINT64CONST(0x1000000c3), UINT64CONST(0x1000000c9), UINT64CONST(0x100000100), UINT64CONST(0x1000000d8), 8 },
		{ UINT64CONST(0x1ffffffff), 0, UINT64CONST(0x200000020), UINT64CONST(0x200000010), 0 },
		{ UINT64_MAX, 0, UINT64_MAX, 0, 8 }
	};
	for (unsigned i = 0; i < lengthof(cases); i++) {
		QualifiedXminFixture f; FullTransactionId future;
		ClusterXidStripeSampleFence fence;
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); fixture_release_original_writer();
		lease_slot = cases[i].slot;
		pg_atomic_write_u64(&StripeLease->limit_full, cases[i].limit);
		pg_atomic_write_u64(&native_lease_shared.herding_floor_full, cases[i].herd);
		memset(&fence, 0xa5, sizeof(fence));
		sampler_lock_acquire(sampler_xid_lock, LW_SHARED);
		UT_ASSERT_EQ(sampler_stripe_future(FullTransactionIdFromU64(cases[i].reference), &future, &fence), cases[i].expected != 0);
		sampler_lock_release(sampler_xid_lock);
		UT_ASSERT_EQ(U64FromFullTransactionId(future), cases[i].expected);
		if (cases[i].expected != 0) {
			UT_ASSERT_EQ(fence.lease_limit_full, cases[i].limit); UT_ASSERT_EQ(fence.slot, cases[i].slot);
		} else UT_ASSERT_EQ(fence.incarnation, 0);
		UT_ASSERT_EQ(inserts + edges + dirties + flushes + pass_native_reads, 0);
		UT_ASSERT_EQ(lease_lock_acquires, lease_lock_releases);
		UT_ASSERT(!sampler_lock_held && !lease_lock_held && !local_x && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
	{
		QualifiedXminFixture f; FullTransactionId future;
		ClusterXidStripeSampleFence fence;
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); fixture_release_original_writer();
		memset(&fence, 0xa5, sizeof(fence));
		UT_ASSERT(!original_stripe_future(f.policy.legal_next, &future, &fence));
		UT_ASSERT(!FullTransactionIdIsValid(future)); UT_ASSERT_EQ(fence.incarnation, 0);
		UT_ASSERT(!original_stripe_future(f.policy.legal_next, &future, NULL));
		UT_ASSERT(!original_stripe_future(f.policy.legal_next, NULL, &fence));
		UT_ASSERT_EQ(lease_lock_acquires + sampler_lock_acquires, 0);
	}
}

UT_TEST(test_xmin_original_stripe_future_rechecks_after_snapshot_unlock)
{
	for (int fault = 0; fault < 4; fault++) {
		QualifiedXminFixture f;
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
		if (fault < 2) lease_after_policy_fault = fault == 0 ? 5 : 13;
		else { lease_revoke_after_unlock = 2; lease_revoke_kind = fault == 2 ? 1 : 2; }
		fixture_release_original_writer(); resume_allowed = true;
		worker_original_sample_pass_fixture(&f.cohort);
		UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_INVALID);
		UT_ASSERT_EQ(sampler_future_reads, 1); UT_ASSERT_EQ(sampler_proc_locks, 0);
		UT_ASSERT_EQ(pass_begin_reads, 0);
		UT_ASSERT(!qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes + conditional_unlocks, 0);
		UT_ASSERT_EQ(sampler_lock_acquires, sampler_lock_releases);
		UT_ASSERT_EQ(lease_lock_acquires, lease_lock_releases);
		UT_ASSERT(!sampler_lock_held && !lease_lock_held && !worker_lock_held && !local_x && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_original_census_begin_refuses_invalid_reference)
{
	for (uint32 raw = InvalidTransactionId; raw < FirstNormalTransactionId; raw++) {
		QualifiedXminFixture f; ClusterCtrcActiveScan scan, expected;
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); fixture_release_original_writer();
		/* Direct begin API only; no native sample is fabricated or published. */
		pass_native_reads = 1;
		memset(&scan, 0xa5, sizeof(scan)); memset(&expected, 0, sizeof(expected));
		expected.status = CTRC_ACTIVE_SCAN_INVALID;
		UT_ASSERT(!worker_pass_census_begin(&scan, raw == 0 ? InvalidFullTransactionId : FullTransactionIdFromEpochAndXid(1, raw)));
		UT_ASSERT(memcmp(&scan, &expected, sizeof(scan)) == 0);
		UT_ASSERT(!original_census_scan_begin(NULL, f.policy.legal_next));
		UT_ASSERT_EQ(pass_census_ready_reads, 0);
		UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
		UT_ASSERT(!sampler_lock_held && !lease_lock_held && !worker_lock_held && !local_x && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_xmin_original_census_begin_continues_native_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		qualified_xmin_require_peer(&f, 65);
		/* Only traversal remains controlled: these are header inputs, not rows. */
		CtrcShared->origin_key_entries = 2; CtrcShared->participant_key_entries = 3;
		native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
		fixture_release_original_writer(); resume_allowed = true;
		worker_original_sample_pass_fixture(&f.cohort);
		UT_ASSERT_EQ(pass_begin_reads, 1); UT_ASSERT_EQ(pass_census_ready_reads, 1);
		UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
		UT_ASSERT_EQ(publisher_scan.total_entries, 5); UT_ASSERT_EQ(publisher_scan.next_index, 5);
		UT_ASSERT_EQ(publisher_scan.unqualified_registration_generation, 41);
		UT_ASSERT(FullTransactionIdEquals(publisher_scan.reference_full, sampler_variables.nextXid));
		UT_ASSERT_EQ(U64FromFullTransactionId(publisher_scan.min_full), f.cohort.active_full_xid_min);
		f.cohort.local_registration_cut = publisher_scan;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(!sampler_lock_held && !lease_lock_held && !worker_lock_held && !local_x && !resumed_writer);
		UT_ASSERT(f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_xmin_original_census_begin_refuses_unavailable_header)
{
	for (int fault = 0; fault < 8; fault++) {
		QualifiedXminFixture f; ClusterUndoCleanerActiveSample zero;
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
		switch (fault) {
		case 0: CtrcShared = NULL; break;
		case 1: registration_attached = false; break;
		case 2: pg_atomic_write_u64(&CtrcShared->unqualified_registration_generation, 0); break;
		case 3: pg_atomic_write_u64(&CtrcShared->unqualified_registration_generation, UINT64_MAX); break;
		case 4: CtrcShared->origin_key_entries = UINT64_MAX; break;
		case 5: CtrcShared->origin_key_entries = UINT64_MAX - 1; CtrcShared->participant_key_entries = 2; break;
		case 6: CtrcShared->participant_key_entries = UINT64_MAX; break;
		case 7: CtrcShared->origin_key_entries = CtrcShared->participant_key_entries = UINT64_MAX; break;
		}
		fixture_release_original_writer(); resume_allowed = true;
		worker_original_sample_pass_fixture(&f.cohort);
		memset(&zero, 0, sizeof(zero));
		UT_ASSERT_EQ(pass_begin_reads, 1); UT_ASSERT_EQ(pass_census_ready_reads, 1);
		UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_INVALID); UT_ASSERT(!pass_pending);
		UT_ASSERT(memcmp(&zero, &native_worker_shared.active_sample, sizeof(zero)) == 0);
		UT_ASSERT(!qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes + conditional_unlocks, 0);
		UT_ASSERT_EQ(sampler_lock_acquires, sampler_lock_releases);
		UT_ASSERT_EQ(lease_lock_acquires, lease_lock_releases);
		UT_ASSERT(!sampler_lock_held && !lease_lock_held && !worker_lock_held && !local_x && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_xmin_original_census_begin_count_boundary_is_api_only)
{
	for (int shape = 0; shape < 2; shape++) {
		QualifiedXminFixture f; ClusterCtrcActiveScan scan;
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); fixture_release_original_writer();
		/* Exercise sum arithmetic only; no array of this size is allocated/scanned. */
		CtrcShared->origin_key_entries = shape ? UINT64_MAX : UINT64_MAX - 1;
		CtrcShared->participant_key_entries = shape ? 0 : 1;
		memset(&scan, 0xa5, sizeof(scan));
		UT_ASSERT(original_census_scan_begin(&scan, f.policy.legal_next));
		UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_INCOMPLETE);
		UT_ASSERT_EQ(scan.total_entries, UINT64_MAX); UT_ASSERT_EQ(scan.next_index, 0);
		UT_ASSERT_EQ(scan.unqualified_registration_generation, 41);
		UT_ASSERT(FullTransactionIdEquals(scan.reference_full, f.policy.legal_next));
		UT_ASSERT(FullTransactionIdEquals(scan.min_full, f.policy.legal_next));
		UT_ASSERT(!FullTransactionIdIsValid(scan.future_full) && !scan.lock_busy);
		UT_ASSERT_EQ(pass_census_ready_reads, 1);
		UT_ASSERT_EQ(pass_capture_reads + pass_native_reads + pass_begin_reads + pass_step_reads, 0);
		UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
		UT_ASSERT(!sampler_lock_held && !lease_lock_held && !worker_lock_held && !local_x && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_xmin_original_census_begin_retains_registration_across_continuation)
{
	QualifiedXminFixture f; ClusterCtrcActiveScan first;
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
	qualified_xmin_require_peer(&f, 65);
	native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
	fixture_release_original_writer(); resume_allowed = true; pass_incomplete_slices = 1;
	worker_original_sample_pass_fixture(&f.cohort);
	first = publisher_scan;
	UT_ASSERT_EQ(first.status, CTRC_ACTIVE_SCAN_INCOMPLETE); UT_ASSERT(pass_pending);
	UT_ASSERT_EQ(first.total_entries, 2); UT_ASSERT_EQ(first.unqualified_registration_generation, 41);
	ctrc_note_unqualified_registration();
	UT_ASSERT(!cluster_ctrc_active_scan_registration_current(&first));
	original_worker_sample_active_pass();
	UT_ASSERT_EQ(pass_capture_reads, 1); UT_ASSERT_EQ(pass_native_reads, 1);
	UT_ASSERT_EQ(pass_begin_reads, 1); UT_ASSERT_EQ(pass_census_ready_reads, 1);
	UT_ASSERT_EQ(pass_step_reads, 2);
	UT_ASSERT_EQ(publisher_scan.unqualified_registration_generation, first.unqualified_registration_generation);
	UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation), 42);
	f.cohort.local_registration_cut = publisher_scan;
	UT_ASSERT(!qualified_xmin_apply(&f, 0));
	UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes + conditional_unlocks, 0);
	UT_ASSERT(!sampler_lock_held && !lease_lock_held && !worker_lock_held && !local_x && f.admission.entered);
	UT_ASSERT_EQ(backend_pin_count, 1);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
}

static ClusterCtrcTxnKeyV1
census_key_for_full(const ClusterSemanticActiveSampleFence *source, FullTransactionId full)
{
	ClusterCtrcTxnKeyV1 key = {0};
	key.format_version = CLUSTER_CTRC_FORMAT_VERSION;
	key.origin_node_id = source->local_node_id; key.owner_instance = key.origin_node_id + 1;
	key.segment_id = key.segment_generation = key.slot_wrap = 1;
	key.xid = XidFromFullTransactionId(full);
	key.system_identifier = source->system_identifier;
	key.origin_boot_incarnation = source->local_boot_incarnation;
	key.admission_record_generation = source->record_generation;
	key.formation_epoch = key.cluster_epoch = source->formation_epoch;
	key.root_descriptor_incarnation = key.root_id = key.root_generation = 1;
	return key;
}

static void
census_seed_retained_rows(QualifiedXminFixture *f, unsigned origin, unsigned participant)
{
	ClusterCtrcOriginEntry *o = &registration_arrays.origins[origin];
	ClusterCtrcParticipantEntry *p = &registration_arrays.participants[participant];
	/* Typed input rows, not execution of the original producers. */
	o->original_full_xid = FullTransactionIdFromEpochAndXid(1, 100);
	o->key = census_key_for_full(&f->cohort.admission_fence, o->original_full_xid);
	o->state = CTRC_ORIGIN_OPEN;
	p->original_full_xid = FullTransactionIdFromEpochAndXid(1, 120);
	p->key = census_key_for_full(&f->cohort.admission_fence, p->original_full_xid);
	p->state = CTRC_PARTICIPANT_BLOCKED;
	/* The actual census must lower the actual native sample from152 to100. */
	sampler_xids[0] = 152;
	pg_atomic_write_u64(&sampler_slots[0].cluster_top_full_xid_atomic,
		U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(1, 152)));
}

UT_TEST(test_xmin_original_census_step_rejects_registration_drift)
{
	QualifiedXminFixture f;
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); fixture_release_original_writer();
	pass_incomplete_slices = 1;
	worker_original_sample_pass_fixture(&f.cohort);
	UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_INCOMPLETE);
	ctrc_note_unqualified_registration();
	worker_pass_census_step(&publisher_scan, &pass_captured_admission);
	UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_INVALID);
	UT_ASSERT(!FullTransactionIdIsValid(publisher_scan.min_full));
	UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
	UT_ASSERT(!sampler_lock_held && !lease_lock_held && !worker_lock_held && !local_x && f.admission.entered);
	UT_ASSERT_EQ(backend_pin_count, 1);
}

UT_TEST(test_xmin_original_census_rows_continue_native_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		qualified_xmin_require_peer(&f, 65); census_seed_retained_rows(&f, 0, 0);
		native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
		fixture_release_original_writer(); resume_allowed = true;
		worker_original_sample_pass_fixture(&f.cohort);
		UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
		UT_ASSERT_EQ(U64FromFullTransactionId(publisher_scan.min_full), f.cohort.active_full_xid_min);
		UT_ASSERT_EQ(publisher_scan.unqualified_entries, 0);
		UT_ASSERT_EQ(census_step_origin_locks, 1); UT_ASSERT_EQ(census_step_participant_locks[0], 1);
		UT_ASSERT_EQ(census_step_lock_acquires, 2); UT_ASSERT_EQ(census_step_lock_releases, 2);
		f.cohort.local_registration_cut = publisher_scan;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(!census_step_lock_held && !sampler_lock_held && !worker_lock_held && !lease_lock_held && !local_x && !resumed_writer);
		UT_ASSERT(f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_xmin_original_census_busy_and_slice_continuations_keep_original_cut)
{
	for (int wide = 0; wide < 2; wide++) {
		QualifiedXminFixture f; unsigned passes = 1;
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); qualified_xmin_require_peer(&f, 65);
		if (wide) CtrcShared->origin_key_entries = CtrcShared->participant_key_entries = CLUSTER_CTRC_ACTIVE_SCAN_SLICE + 1;
		census_seed_retained_rows(&f, wide ? CLUSTER_CTRC_ACTIVE_SCAN_SLICE : 0, wide ? CLUSTER_CTRC_ACTIVE_SCAN_SLICE : 0);
		if (!wide) census_step_block_at = 2;
		native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
		memset(&native_worker_shared.active_sample, 0, sizeof(native_worker_shared.active_sample));
		fixture_release_original_writer(); resume_allowed = true;
		worker_original_sample_pass_fixture(&f.cohort);
		UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_INCOMPLETE); UT_ASSERT(pass_pending);
		UT_ASSERT_EQ(publisher_scan.next_index, wide ? CLUSTER_CTRC_ACTIVE_SCAN_SLICE : 1);
		UT_ASSERT_EQ(publisher_scan.lock_busy, !wide);
		UT_ASSERT_EQ(publisher_scan.unqualified_registration_generation, 41);
		UT_ASSERT(!census_step_lock_held);
		while (pass_pending && passes < 5) { original_worker_sample_active_pass(); passes++; }
		UT_ASSERT_EQ(passes, wide ? 3 : 2); UT_ASSERT(!pass_pending);
		UT_ASSERT_EQ(pass_capture_reads, 1); UT_ASSERT_EQ(pass_native_reads, 1); UT_ASSERT_EQ(pass_begin_reads, 1);
		UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
		UT_ASSERT_EQ(publisher_scan.next_index, publisher_scan.total_entries);
		UT_ASSERT_EQ(U64FromFullTransactionId(publisher_scan.min_full), f.cohort.active_full_xid_min);
		UT_ASSERT_EQ(census_step_origin_locks, wide ? CLUSTER_CTRC_ACTIVE_SCAN_SLICE + 1 : 1);
		UT_ASSERT_EQ(census_step_lock_acquires, publisher_scan.total_entries);
		UT_ASSERT_EQ(census_step_lock_acquires, census_step_lock_releases);
		f.cohort.local_registration_cut = publisher_scan;
		UT_ASSERT(qualified_xmin_apply(&f, 0)); UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(!census_step_lock_held && !sampler_lock_held && !worker_lock_held && !lease_lock_held && !local_x && !resumed_writer);
		UT_ASSERT(f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_xmin_original_census_malformed_rows_refuse_before_native_wal)
{
	for (int participant = 0; participant < 2; participant++) {
		for (int fault = 0; fault < 8; fault++) {
			QualifiedXminFixture f; ClusterCtrcTxnKeyV1 *key; FullTransactionId *full;
			qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); census_seed_retained_rows(&f, 0, 0);
			key = participant ? &registration_arrays.participants[0].key : &registration_arrays.origins[0].key;
			full = participant ? &registration_arrays.participants[0].original_full_xid : &registration_arrays.origins[0].original_full_xid;
			switch (fault) {
			case 0:
				if (participant) registration_arrays.participants[0].state = CTRC_PARTICIPANT_BLOCKED + 1;
				else registration_arrays.origins[0].state = CTRC_ORIGIN_BLOCKED + 1;
				break;
			case 1: key->format_version++; break;
			case 2: key->owner_instance++; break;
			case 3: key->slot_offset = TT_SLOTS_PER_SEGMENT; break;
			case 4: key->reserved[0] = 1; break;
			case 5: key->xid = FrozenTransactionId; break;
			case 6: *full = FullTransactionIdFromU64(U64FromFullTransactionId(*full) + 1); break;
			case 7: *full = participant ? FullTransactionIdFromEpochAndXid(1, FrozenTransactionId) : InvalidFullTransactionId; break;
			}
			native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
			fixture_release_original_writer(); resume_allowed = true;
			worker_original_sample_pass_fixture(&f.cohort);
			UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_INVALID);
			UT_ASSERT(!FullTransactionIdIsValid(publisher_scan.min_full));
			UT_ASSERT(!qualified_xmin_apply(&f, 0));
			UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes + conditional_unlocks, 0);
			UT_ASSERT_EQ(census_step_lock_acquires, census_step_lock_releases);
			UT_ASSERT(!census_step_lock_held && !sampler_lock_held && !worker_lock_held && !lease_lock_held && !local_x && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1); UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_xmin_original_census_unqualified_rows_remain_observations)
{
	for (int fault = 0; fault < 7; fault++) {
		QualifiedXminFixture f; ClusterCtrcActiveScan scan; ClusterCtrcTxnKeyV1 *key;
		FullTransactionId low = FullTransactionIdFromEpochAndXid(1, 80);
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); census_seed_retained_rows(&f, 0, 0);
		if ((fault & 1) != 0) {
			registration_arrays.origins[0].original_full_xid = low;
			key = &registration_arrays.origins[0].key;
		} else {
			registration_arrays.participants[0].original_full_xid = low;
			key = &registration_arrays.participants[0].key;
		}
		*key = census_key_for_full(&f.cohort.admission_fence, low);
		switch (fault) {
		case 0: registration_arrays.participants[0].original_full_xid = InvalidFullTransactionId; break;
		case 1: key->system_identifier++; break;
		case 2: key->admission_record_generation++; break;
		case 3: key->formation_epoch++; break;
		case 4: key->cluster_epoch++; break;
		case 5: key->origin_boot_incarnation++; break;
		case 6:
			for (unsigned node = 0; node < CLUSTER_MAX_NODES; node++) {
				uint64 members = node < 64 ? f.cohort.admission_fence.admitted_members_lo : f.cohort.admission_fence.admitted_members_hi;
				if ((members & (UINT64_C(1) << (node % 64))) == 0) {
					key->origin_node_id = node; key->owner_instance = node + 1;
					key->origin_boot_incarnation = 123; break;
				}
			}
			break;
		}
		fixture_release_original_writer();
		UT_ASSERT(original_census_scan_begin(&scan, f.policy.legal_next));
		original_census_step_source(&scan, &f.cohort.admission_fence);
		UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE); UT_ASSERT_EQ(scan.unqualified_entries, 1);
		UT_ASSERT(FullTransactionIdEquals(scan.min_full, low));
		UT_ASSERT_EQ(scan.next_index, 2); UT_ASSERT_EQ(census_step_lock_acquires, census_step_lock_releases);
		/* API observation only; never submit these rows as a qualified bound. */
		UT_ASSERT_EQ(pass_native_reads + inserts + edges + dirties + flushes, 0);
		UT_ASSERT(!census_step_lock_held && !sampler_lock_held && !worker_lock_held && !local_x && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_xmin_original_census_preflight_and_final_cut_refusals)
{
	for (int fault = 0; fault < 16; fault++) {
		QualifiedXminFixture f; ClusterCtrcActiveScan scan; ClusterSemanticActiveSampleFence source;
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); fixture_release_original_writer();
		source = f.cohort.admission_fence;
		UT_ASSERT(original_census_scan_begin(&scan, f.policy.legal_next));
		switch (fault) {
		case 0: break; /* NULL source */
		case 1: source.system_identifier = 0; break;
		case 2: source.record_generation = 0; break;
		case 3: source.formation_epoch = 0; break;
		case 4: source.formation_epoch = UINT64_MAX; break;
		case 5: source.local_boot_incarnation = 0; break;
		case 6: source.local_node_id = -1; break;
		case 7: source.local_node_id = CLUSTER_MAX_NODES; break;
		case 8:
			if (source.local_node_id < 64) source.admitted_members_lo &= ~(UINT64_C(1) << source.local_node_id);
			else source.admitted_members_hi &= ~(UINT64_C(1) << (source.local_node_id - 64));
			break;
		case 9: source.admitted_incarnation[source.local_node_id]++; break;
		case 10: CtrcShared->origin_key_entries++; break;
		case 11: CtrcShared->origin_key_entries = UINT64_MAX; break;
		case 12: scan.next_index = scan.total_entries; break;
		case 13: scan.reference_full = InvalidFullTransactionId; break;
		case 14: scan.min_full = InvalidFullTransactionId; break;
		case 15: registration_attached = false; break;
		}
		original_census_step_source(&scan, fault ? &source : NULL);
		UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_INVALID); UT_ASSERT(!FullTransactionIdIsValid(scan.min_full));
		UT_ASSERT_EQ(census_step_lock_reads, 0); UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
		UT_ASSERT(!census_step_lock_held && !sampler_lock_held && !worker_lock_held && !local_x && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
	{
		QualifiedXminFixture f;
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); census_seed_retained_rows(&f, 0, 0);
		census_step_revoke_at = 2;
		native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
		fixture_release_original_writer(); resume_allowed = true;
		worker_original_sample_pass_fixture(&f.cohort);
		UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_INVALID);
		UT_ASSERT_EQ(publisher_scan.next_index, publisher_scan.total_entries);
		UT_ASSERT(!FullTransactionIdIsValid(publisher_scan.min_full));
		UT_ASSERT_EQ(census_step_lock_acquires, 2); UT_ASSERT_EQ(census_step_lock_releases, 2);
		UT_ASSERT(!qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes + conditional_unlocks, 0);
		UT_ASSERT(!census_step_lock_held && !sampler_lock_held && !worker_lock_held && !lease_lock_held && !local_x && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1); UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

/* The real attachment predicate must reject a different shared object before
 * a saved registration cut can authorize the qualified native continuation. */
UT_TEST(test_census_runtime_attachment_identity)
{
	QualifiedXminFixture f;
	uint32 saved_magic, saved_version;

	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
	UT_ASSERT(cluster_ctrc_active_scan_registration_current(&f.cohort.local_registration_cut));
	saved_magic = CtrcShared->magic;
	saved_version = CtrcShared->version;
	CtrcShared->magic ^= 1;
	UT_ASSERT(!cluster_ctrc_active_scan_registration_current(&f.cohort.local_registration_cut));
	CtrcShared->magic = saved_magic;
	CtrcShared->version ^= 1;
	UT_ASSERT(!cluster_ctrc_active_scan_registration_current(&f.cohort.local_registration_cut));
	CtrcShared->version = saved_version;
	UT_ASSERT(cluster_ctrc_active_scan_registration_current(&f.cohort.local_registration_cut));
}

UT_TEST(test_census_bad_attachment_invalidates_original_worker_pass)
{
	for (int field = 0; field < 2; field++) {
		QualifiedXminFixture f;
		unsigned char zero[sizeof(native_worker_shared.active_sample)] = {0};
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		if (field) CtrcShared->version ^= 1;
		else CtrcShared->magic ^= 1;
		fixture_release_original_writer(); resume_allowed = true;
		worker_original_sample_pass_fixture(&f.cohort);
		UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_INVALID);
		UT_ASSERT(!pass_pending);
		UT_ASSERT(memcmp(zero, &native_worker_shared.active_sample, sizeof(zero)) == 0);
		UT_ASSERT_EQ(census_step_lock_reads, 0);
		UT_ASSERT(!qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes + conditional_unlocks, 0);
		UT_ASSERT_EQ(sampler_lock_acquires, sampler_lock_releases);
		UT_ASSERT_EQ(lease_lock_acquires, lease_lock_releases);
		UT_ASSERT(!census_step_lock_held && !sampler_lock_held && !lease_lock_held && !worker_lock_held && !local_x && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_census_attachment_drift_refuses_native_writer_continuation)
{
	for (int field = 0; field < 2; field++) for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		qualified_xmin_require_peer(&f, 65); census_seed_retained_rows(&f, 0, 0);
		native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
		fixture_release_original_writer(); resume_allowed = true;
		worker_original_sample_pass_fixture(&f.cohort);
		UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
		UT_ASSERT_EQ(publisher_scan.unqualified_entries, 0);
		f.cohort.local_registration_cut = publisher_scan;
		if (field) CtrcShared->version ^= 1;
		else CtrcShared->magic ^= 1;
		UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
		UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes + conditional_unlocks, 0);
		UT_ASSERT_EQ(census_step_lock_acquires, census_step_lock_releases);
		UT_ASSERT(!census_step_lock_held && !sampler_lock_held && !worker_lock_held && !lease_lock_held && !local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_census_bad_attachment_keeps_original_registration_producer)
{
	for (int field = 0; field < 2; field++) {
		QualifiedXminFixture f;
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		if (field) CtrcShared->version ^= 1;
		else CtrcShared->magic ^= 1;
		/* The original producer still advances the cut; attachment identity is
		 * checked by the reader, not by this saturating counter producer. */
		ctrc_note_unqualified_registration();
		UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation), 42);
		UT_ASSERT(!cluster_ctrc_active_scan_registration_current(&f.cohort.local_registration_cut));
		if (field) CtrcShared->version ^= 1;
		else CtrcShared->magic ^= 1;
		ctrc_note_unqualified_registration();
		UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation), 43);
		UT_ASSERT(!cluster_ctrc_active_scan_registration_current(&f.cohort.local_registration_cut));
		UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
	}
}

typedef struct CensusRuntimeAllocation {
	ClusterCtrcSharedHeader *saved_header;
	ClusterCtrcSharedHeader *allocation;
	ClusterCtrcCapacity capacity;
} CensusRuntimeAllocation;

static bool
census_full_layout_setup(CensusRuntimeAllocation *fixture, int nodes)
{
	memset(fixture, 0, sizeof(*fixture));
	fixture->saved_header = CtrcShared;
	census_runtime_nodes = nodes;
	if (!ctrc_runtime_capacity(&fixture->capacity)) return false;
	fixture->allocation = census_runtime_initialize_requested
		? malloc(fixture->capacity.total_bytes) : calloc(1, fixture->capacity.total_bytes);
	if (fixture->allocation == NULL) return false;
	if (census_runtime_initialize_requested) {
		/* Poison the complete new region so zeroing cannot come from calloc. */
		memset(fixture->allocation, 0xa5, fixture->capacity.total_bytes);
		census_initializer_memory = fixture->allocation;
		census_initializer_bytes = fixture->capacity.total_bytes;
		census_initializer_found = false;
		census_initializer_reset_trace();
		original_census_shmem_init();
		census_runtime_ready_requested = true;
		return true;
	}
	CtrcShared = fixture->allocation;
	/* This is controlled publication of a real complete allocation, not
	 * execution of ShmemInitStruct/LWLockInitialize or a producer certificate. */
	CtrcShared->magic = CLUSTER_CTRC_SHMEM_MAGIC;
	CtrcShared->version = CLUSTER_CTRC_SHMEM_VERSION;
	CtrcShared->total_bytes = fixture->capacity.total_bytes;
	CtrcShared->origin_key_entries = fixture->capacity.origin_key_entries;
	CtrcShared->participant_key_entries = fixture->capacity.participant_key_entries;
	CtrcShared->receipt_entries = fixture->capacity.receipt_entries;
	CtrcShared->participant_ack_summary_entries = fixture->capacity.participant_ack_summary_entries;
	CtrcShared->origin_ack_inbox_entries = fixture->capacity.origin_ack_inbox_entries;
	CtrcShared->global_grant_generation = 1;
	CtrcShared->global_reservation_generation = CtrcShared->global_journal_generation = 1;
	CtrcShared->global_seal_generation = CtrcShared->global_request_generation = 1;
	ctrc_shared_fill_layout(CtrcShared, &fixture->capacity);
	pg_atomic_init_u64(&CtrcShared->unqualified_registration_generation, 41);
	census_runtime_ready_requested = true;
	return true;
}

static void
census_full_layout_release(CensusRuntimeAllocation *fixture)
{
	UT_ASSERT(!census_step_lock_held && !sampler_lock_held && !lease_lock_held && !worker_lock_held);
	CtrcShared = fixture->saved_header;
	free(fixture->allocation);
	fixture->allocation = NULL;
	census_initializer_memory = NULL; census_initializer_bytes = 0;
	census_initializer_found = false;
	census_runtime_ready_requested = census_runtime_initialize_requested = false;
}

UT_TEST(test_census_full_ready_rejects_partial_layout)
{
	QualifiedXminFixture f;
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
	fixture_release_original_writer();
	/* The small legacy row fixture has no complete runtime allocation. */
	census_runtime_ready_requested = true;
	UT_ASSERT(!worker_census_shmem_ready());
	census_runtime_ready_requested = false;
}

UT_TEST(test_census_original_capacity_rejects_overflow_and_preserves_publish_boundary)
{
	ClusterCtrcCapacity one, clamped, invalid, saved, zero = {0};
	UT_ASSERT(original_census_capacity_compute(4, 2, 1, &one));
	UT_ASSERT_EQ(one.origin_key_entries, (uint64)CLUSTER_UNDO_SEGS_PER_INSTANCE * TT_SLOTS_PER_SEGMENT);
	UT_ASSERT_EQ(one.participant_key_entries, one.origin_key_entries);
	UT_ASSERT_EQ(one.receipt_entries, 4 + 2 * CLUSTER_CURRENT_MX_MAX_MEMBERS);
	UT_ASSERT(original_census_capacity_compute(4, 2, -1, &clamped));
	UT_ASSERT(memcmp(&one, &clamped, sizeof(one)) == 0);
	UT_ASSERT(original_census_capacity_compute(4, 2, CLUSTER_CTRC_MAX_PARTICIPANTS, &one));
	UT_ASSERT(original_census_capacity_compute(4, 2, INT_MAX, &clamped));
	UT_ASSERT(memcmp(&one, &clamped, sizeof(one)) == 0);
	memset(&invalid, 0xa5, sizeof(invalid)); saved = invalid;
	UT_ASSERT(!original_census_capacity_compute(4, -1, 2, &invalid));
	UT_ASSERT(memcmp(&invalid, &saved, sizeof(invalid)) == 0);
	UT_ASSERT(!original_census_capacity_compute(4, 2, 2, NULL));
	UT_ASSERT(!original_census_capacity_compute(SIZE_MAX, 2, 2, &invalid));
	UT_ASSERT(memcmp(&invalid, &zero, sizeof(invalid)) == 0);
	UT_ASSERT(!original_census_capacity_compute(SIZE_MAX - 1, 0, 2, &invalid));
	UT_ASSERT(memcmp(&invalid, &zero, sizeof(invalid)) == 0);
}

UT_TEST(test_census_original_ready_accepts_complete_allocated_layout)
{
	for (int nodes = 1; nodes <= 2; nodes++) {
		QualifiedXminFixture f; CensusRuntimeAllocation runtime;
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); fixture_release_original_writer();
		if (!census_full_layout_setup(&runtime, nodes)) { UT_ASSERT(false); return; }
		UT_ASSERT(worker_census_shmem_ready());
		UT_ASSERT(ctrc_shared_header_exact(CtrcShared, &runtime.capacity));
		UT_ASSERT(!ctrc_shared_header_exact(NULL, &runtime.capacity));
		UT_ASSERT(!ctrc_shared_header_exact(CtrcShared, NULL));
		UT_ASSERT((char *)ctrc_origin_entries() >= (char *)runtime.allocation + sizeof(*CtrcShared));
		UT_ASSERT((char *)(ctrc_origin_entries() + runtime.capacity.origin_key_entries) <= (char *)ctrc_participant_entries());
		UT_ASSERT((char *)(ctrc_participant_entries() + runtime.capacity.participant_key_entries) <= (char *)runtime.allocation + CtrcShared->receipt_offset);
		UT_ASSERT_EQ(CtrcShared->origin_ack_offset + MAXALIGN(runtime.capacity.origin_ack_inbox_entries * sizeof(ClusterCtrcLocalReleaseAckV1)), runtime.capacity.total_bytes);
		UT_ASSERT_EQ(CtrcShared->participant_key_entries, CtrcShared->origin_key_entries * nodes);
		UT_ASSERT_EQ(inserts + edges + dirties + flushes + census_step_lock_reads, 0);
		census_full_layout_release(&runtime);
	}
}

UT_TEST(test_census_original_ready_rejects_header_and_runtime_drift_before_scan)
{
	for (int fault = 0; fault < 29; fault++) {
		QualifiedXminFixture f; CensusRuntimeAllocation runtime;
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); fixture_release_original_writer(); resume_allowed = true;
		if (!census_full_layout_setup(&runtime, 2)) { UT_ASSERT(false); return; }
		switch (fault) {
		case 0: CtrcShared = NULL; break;
		case 1: CtrcShared->magic = 0; break;
		case 2: CtrcShared->version++; break;
		case 3: CtrcShared->total_bytes--; break;
		case 4: CtrcShared->origin_key_entries++; break;
		case 5: CtrcShared->participant_key_entries++; break;
		case 6: CtrcShared->receipt_entries++; break;
		case 7: CtrcShared->participant_ack_summary_entries++; break;
		case 8: CtrcShared->origin_ack_inbox_entries++; break;
		case 9: CtrcShared->origin_offset++; break;
		case 10: CtrcShared->participant_offset++; break;
		case 11: CtrcShared->receipt_offset++; break;
		case 12: CtrcShared->receipt_probe_offset++; break;
		case 13: CtrcShared->receipt_link_offset++; break;
		case 14: CtrcShared->participant_receipt_head_offset++; break;
		case 15: CtrcShared->participant_ack_offset++; break;
		case 16: CtrcShared->origin_ack_offset++; break;
		case 17: CtrcShared->global_grant_generation = 0; break;
		case 18: CtrcShared->reserved32 = 1; break;
		case 19: CtrcShared->global_reservation_generation = 0; break;
		case 20: CtrcShared->global_journal_generation = 0; break;
		case 21: CtrcShared->global_seal_generation = 0; break;
		case 22: CtrcShared->global_request_generation = 0; break;
		case 23: CtrcShared->reserved[0] = 1; break;
		case 24: census_runtime_buffers++; break;
		case 25: census_runtime_backends++; break;
		case 26: census_runtime_nodes = 1; break;
		case 27: census_runtime_backends = -1; break;
		case 28: census_runtime_buffers = -1; break;
		}
		UT_ASSERT(!worker_census_shmem_ready());
		worker_original_sample_pass_fixture(&f.cohort);
		UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_INVALID);
		UT_ASSERT(!pass_pending); UT_ASSERT_EQ(census_step_lock_reads, 0);
		UT_ASSERT(!qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes + conditional_unlocks, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		UT_ASSERT(!local_x && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
		census_full_layout_release(&runtime);
	}
}

UT_TEST(test_census_complete_runtime_layout_reaches_original_native_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3]; CensusRuntimeAllocation runtime;
		ClusterCtrcOriginEntry *origin;
		ClusterCtrcParticipantEntry *participant;
		uint64 passes = 1, expected_passes, total;
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		qualified_xmin_require_peer(&f, 65);
		if (!census_full_layout_setup(&runtime, 2)) { UT_ASSERT(false); return; }
		origin = ctrc_origin_entries();
		participant = ctrc_participant_entries();
		origin[0].original_full_xid = FullTransactionIdFromEpochAndXid(1, 100);
		origin[0].key = census_key_for_full(&f.cohort.admission_fence, origin[0].original_full_xid);
		origin[0].state = CTRC_ORIGIN_OPEN;
		participant[0].original_full_xid = FullTransactionIdFromEpochAndXid(1, 120);
		participant[0].key = census_key_for_full(&f.cohort.admission_fence, participant[0].original_full_xid);
		participant[0].state = CTRC_PARTICIPANT_BLOCKED;
		sampler_xids[0] = 152;
		pg_atomic_write_u64(&sampler_slots[0].cluster_top_full_xid_atomic,
			U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(1, 152)));
		total = runtime.capacity.origin_key_entries + runtime.capacity.participant_key_entries;
		expected_passes = (total + CLUSTER_CTRC_ACTIVE_SCAN_SLICE - 1) / CLUSTER_CTRC_ACTIVE_SCAN_SLICE;
		native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
		fixture_release_original_writer(); resume_allowed = true;
		worker_original_sample_pass_fixture(&f.cohort);
		while (pass_pending && passes <= expected_passes) { original_worker_sample_active_pass(); passes++; }
		UT_ASSERT_EQ(passes, expected_passes); UT_ASSERT(!pass_pending);
		UT_ASSERT_EQ(pass_capture_reads, 1); UT_ASSERT_EQ(pass_native_reads, 1); UT_ASSERT_EQ(pass_begin_reads, 1);
		UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
		UT_ASSERT_EQ(publisher_scan.total_entries, total); UT_ASSERT_EQ(publisher_scan.next_index, total);
		UT_ASSERT_EQ(publisher_scan.unqualified_entries, 0);
		UT_ASSERT_EQ(U64FromFullTransactionId(publisher_scan.min_full), f.cohort.active_full_xid_min);
		UT_ASSERT_EQ(census_step_lock_acquires, total); UT_ASSERT_EQ(census_step_lock_releases, total);
		f.cohort.local_registration_cut = publisher_scan;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(!local_x && !resumed_writer && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
		census_full_layout_release(&runtime);
	}
}

UT_TEST(test_census_original_initializer_publishes_first_registration_cut)
{
	QualifiedXminFixture f; CensusRuntimeAllocation runtime;
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); fixture_release_original_writer();
	census_runtime_initialize_requested = true;
	if (!census_full_layout_setup(&runtime, 2)) { UT_ASSERT(false); return; }
	UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation), 1);
	UT_ASSERT(worker_census_shmem_ready());
	census_full_layout_release(&runtime);
	census_runtime_initialize_requested = false;
}

UT_TEST(test_census_original_initializer_clears_new_memory_and_local_state)
{
	QualifiedXminFixture f; CensusRuntimeAllocation runtime;
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); fixture_release_original_writer();
	/* Only byte-writing/clearing touches this poisoned previous local state. */
	memset(&CtrcBatch, 0xa5, sizeof(CtrcBatch)); memset(CtrcOriginHint, 0xa5, sizeof(CtrcOriginHint));
	CtrcDispatchCursor = CtrcLocalProgress = CtrcLocalPasses = 91; CtrcWorkerId = 3;
	census_runtime_initialize_requested = true;
	if (!census_full_layout_setup(&runtime, 2)) { UT_ASSERT(false); return; }
	UT_ASSERT_EQ(census_initializer_calls, 1);
	UT_ASSERT_EQ(census_initializer_lock_calls, CLUSTER_CTRC_CLEANER_WORKERS + 2);
	UT_ASSERT(ctrc_bytes_zero(&CtrcBatch, sizeof(CtrcBatch)));
	UT_ASSERT(ctrc_bytes_zero(CtrcOriginHint, sizeof(CtrcOriginHint)));
	UT_ASSERT_EQ(CtrcDispatchCursor + CtrcLocalProgress + CtrcLocalPasses, 0); UT_ASSERT_EQ(CtrcWorkerId, -1);
	UT_ASSERT(ctrc_bytes_zero((char *)CtrcShared + CtrcShared->origin_offset,
		runtime.capacity.total_bytes - CtrcShared->origin_offset));
	UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation), 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->test_barrier_hit_count), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&CtrcShared->test_barrier_phase), CTRC_TEST_BARRIER_NONE);
	for (int i = 0; i < CTRC_STAT_COUNT; i++) UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->stats[i]), 0);
	for (unsigned i = 0; i < CLUSTER_CTRC_CLEANER_WORKERS + 2; i++) {
		LWLock *lock = i == 0 ? &CtrcShared->origin_lock
			: i == CLUSTER_CTRC_CLEANER_WORKERS + 1 ? &CtrcShared->receipt_lock
			: &CtrcShared->participant_locks[i - 1].lock;
		UT_ASSERT(census_initializer_locks[i]);
		UT_ASSERT_EQ(pg_atomic_read_u32(&lock->state), LW_FLAG_RELEASE_OK);
		UT_ASSERT(proclist_is_empty(&lock->waiters));
	}
	for (unsigned i = 0; i < CLUSTER_CTRC_CLEANER_WORKERS; i++) {
		UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->workers[i].dispatch_backlog), 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->workers[i].certificate_backlog), 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->workers[i].pending_observed_age_ms), 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->workers[i].observed_at_us), 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&CtrcShared->workers[i].reason), CTRC_CLEANER_REASON_NONE);
	}
	UT_ASSERT(worker_census_shmem_ready()); UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
	census_full_layout_release(&runtime);
}

UT_TEST(test_census_original_initializer_reattaches_without_resetting_existing_state)
{
	QualifiedXminFixture f; CensusRuntimeAllocation runtime; CtrcCleanerBatch saved_batch;
	unsigned char *saved_region, *saved_hints;
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); fixture_release_original_writer();
	census_runtime_initialize_requested = true;
	if (!census_full_layout_setup(&runtime, 2)) { UT_ASSERT(false); return; }
	ctrc_note_unqualified_registration(); pg_atomic_write_u64(&CtrcShared->stats[0], 7);
	((unsigned char *)CtrcShared)[runtime.capacity.total_bytes - 1] = 0x77;
	memset(&CtrcBatch, 0, sizeof(CtrcBatch)); CtrcBatch.active = true; CtrcBatch.remaining[0] = 17;
	memset(CtrcOriginHint, 0x5a, sizeof(CtrcOriginHint));
	CtrcDispatchCursor = CtrcLocalProgress = CtrcLocalPasses = 91; CtrcWorkerId = 3;
	memcpy(&saved_batch, &CtrcBatch, sizeof(saved_batch));
	saved_region = malloc(runtime.capacity.total_bytes); saved_hints = malloc(sizeof(CtrcOriginHint));
	UT_ASSERT(saved_region != NULL && saved_hints != NULL);
	if (saved_region == NULL || saved_hints == NULL) { free(saved_region); free(saved_hints); census_full_layout_release(&runtime); return; }
	memcpy(saved_region, CtrcShared, runtime.capacity.total_bytes); memcpy(saved_hints, CtrcOriginHint, sizeof(CtrcOriginHint));
	census_initializer_found = true; census_initializer_reset_trace();
	original_census_shmem_init();
	UT_ASSERT(CtrcShared == runtime.allocation); UT_ASSERT_EQ(census_initializer_calls, 1);
	UT_ASSERT_EQ(census_initializer_lock_calls, 0);
	UT_ASSERT(memcmp(saved_region, CtrcShared, runtime.capacity.total_bytes) == 0);
	UT_ASSERT(memcmp(saved_hints, CtrcOriginHint, sizeof(CtrcOriginHint)) == 0);
	UT_ASSERT(memcmp(&saved_batch, &CtrcBatch, sizeof(saved_batch)) == 0);
	UT_ASSERT_EQ(CtrcDispatchCursor, 91); UT_ASSERT_EQ(CtrcLocalProgress, 91); UT_ASSERT_EQ(CtrcLocalPasses, 91); UT_ASSERT_EQ(CtrcWorkerId, 3);
	UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation), 2);
	UT_ASSERT(worker_census_shmem_ready()); UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
	free(saved_region); free(saved_hints); census_full_layout_release(&runtime);
}

UT_TEST(test_census_original_initializer_refuses_capacity_and_existing_header)
{
	for (int fault = 0; fault < 3; fault++) {
		QualifiedXminFixture f; CensusRuntimeAllocation runtime;
		unsigned char *saved_region; volatile bool caught = false;
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); fixture_release_original_writer();
		census_runtime_initialize_requested = true;
		if (!census_full_layout_setup(&runtime, 2)) { UT_ASSERT(false); return; }
		if (fault == 0) census_runtime_backends = -1;
		else if (fault == 1) CtrcShared->magic ^= 1;
		else CtrcShared->reserved32 = 1;
		saved_region = malloc(runtime.capacity.total_bytes); UT_ASSERT(saved_region != NULL);
		if (saved_region == NULL) { census_full_layout_release(&runtime); return; }
		memcpy(saved_region, CtrcShared, runtime.capacity.total_bytes);
		census_initializer_found = true; census_initializer_reset_trace(); expecting_error = true;
		UT_ASSERT(PG_exception_stack == NULL);
		/* FATAL is a synthetic longjmp in this fixture, not a real server exit. */
		if (setjmp(error_jump) == 0) { original_census_shmem_init(); UT_ASSERT(false); }
		else caught = true;
		expecting_error = false;
		UT_ASSERT(caught); UT_ASSERT_EQ(census_initializer_error_level, FATAL);
		UT_ASSERT_EQ(census_initializer_error_code, fault == 0 ? ERRCODE_PROGRAM_LIMIT_EXCEEDED : ERRCODE_DATA_CORRUPTED);
		UT_ASSERT_EQ(census_initializer_calls, fault == 0 ? 0 : 1); UT_ASSERT_EQ(census_initializer_lock_calls, 0);
		UT_ASSERT(CtrcShared == runtime.allocation);
		UT_ASSERT(memcmp(saved_region, CtrcShared, runtime.capacity.total_bytes) == 0);
		UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
		UT_ASSERT(!local_x && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
		free(saved_region); census_full_layout_release(&runtime);
	}
}

UT_TEST(test_census_original_initializer_then_complete_walk_continues_native_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3]; CensusRuntimeAllocation runtime;
		ClusterCtrcOriginEntry *origin; ClusterCtrcParticipantEntry *participant;
		uint64 passes = 1, expected_passes, total;
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		qualified_xmin_require_peer(&f, 65); fixture_release_original_writer();
		census_runtime_initialize_requested = true;
		if (!census_full_layout_setup(&runtime, 2)) { UT_ASSERT(false); return; }
		origin = ctrc_origin_entries(); participant = ctrc_participant_entries();
		origin[0].original_full_xid = FullTransactionIdFromEpochAndXid(1, 100);
		origin[0].key = census_key_for_full(&f.cohort.admission_fence, origin[0].original_full_xid); origin[0].state = CTRC_ORIGIN_OPEN;
		participant[0].original_full_xid = FullTransactionIdFromEpochAndXid(1, 120);
		participant[0].key = census_key_for_full(&f.cohort.admission_fence, participant[0].original_full_xid); participant[0].state = CTRC_PARTICIPANT_BLOCKED;
		ctrc_note_unqualified_registration();
		UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation), 2);
		sampler_xids[0] = 152;
		pg_atomic_write_u64(&sampler_slots[0].cluster_top_full_xid_atomic, U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(1, 152)));
		total = runtime.capacity.origin_key_entries + runtime.capacity.participant_key_entries;
		expected_passes = (total + CLUSTER_CTRC_ACTIVE_SCAN_SLICE - 1) / CLUSTER_CTRC_ACTIVE_SCAN_SLICE;
		native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1; resume_allowed = true;
		worker_original_sample_pass_fixture(&f.cohort);
		while (pass_pending && passes <= expected_passes) { original_worker_sample_active_pass(); passes++; }
		UT_ASSERT_EQ(passes, expected_passes); UT_ASSERT(!pass_pending);
		UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
		UT_ASSERT_EQ(publisher_scan.unqualified_registration_generation, 2);
		UT_ASSERT_EQ(publisher_scan.next_index, total); UT_ASSERT_EQ(publisher_scan.unqualified_entries, 0);
		UT_ASSERT_EQ(U64FromFullTransactionId(publisher_scan.min_full), f.cohort.active_full_xid_min);
		UT_ASSERT_EQ(census_step_lock_acquires, total); UT_ASSERT_EQ(census_step_lock_releases, total);
		f.cohort.local_registration_cut = publisher_scan;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(!local_x && !resumed_writer && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
		census_full_layout_release(&runtime);
	}
}

/* Only the transaction-state leaf and backend publication are controlled here. */
static PGPROC census_origin_backend;
static PGPROC *census_origin_proc;
static FullTransactionId census_origin_top_full;
static unsigned census_origin_top_reads;
static bool census_origin_lock_held;
static unsigned census_origin_shared_locks, census_origin_exclusive_locks, census_origin_unlocks;

static FullTransactionId
census_origin_top_full_read(void)
{
	worker_pass_assert_unlocked(); UT_ASSERT(!census_origin_lock_held);
	census_origin_top_reads++;
	return census_origin_top_full;
}

static bool
census_origin_lock_acquire(LWLock *lock, LWLockMode mode)
{
	worker_pass_assert_unlocked(); UT_ASSERT(!census_origin_lock_held);
	UT_ASSERT(lock == &CtrcShared->origin_lock);
	UT_ASSERT(mode == LW_SHARED || mode == LW_EXCLUSIVE);
	census_origin_lock_held = true;
	if (mode == LW_SHARED) census_origin_shared_locks++;
	else census_origin_exclusive_locks++;
	return true;
}

static void
census_origin_lock_release(LWLock *lock)
{
	UT_ASSERT(census_origin_lock_held && lock == &CtrcShared->origin_lock);
	census_origin_lock_held = false; census_origin_unlocks++;
}

#ifdef CLUSTER_CTRC_UNIT_TEST
#error Shared origin coverage must compile the original production branch
#endif
static ClusterCtrcOriginOpenResult original_census_origin_open_active(ClusterCtrcOriginEntry *, const ClusterCtrcTxnKeyV1 *, uint32);
#define MyProc census_origin_proc
#define GetTopFullTransactionIdIfAny census_origin_top_full_read
#define LWLockAcquire census_origin_lock_acquire
#define LWLockRelease census_origin_lock_release
#define cluster_ctrc_shmem_ready original_census_shmem_ready
#define ctrc_origin_index original_census_origin_index
#define cluster_ctrc_stat_bump original_census_stat_bump
#define cluster_ctrc_origin_original_full_xid_exact original_census_origin_full_exact
#define cluster_ctrc_origin_original_full_xid_shared original_census_origin_full_shared
#define cluster_ctrc_origin_reserve_entry original_census_origin_reserve_entry
#define cluster_ctrc_origin_reserve_active original_census_origin_reserve_active
#define cluster_ctrc_origin_open_reserved original_census_origin_open_reserved
#define cluster_ctrc_origin_open_active original_census_origin_open_active
#include "test_cluster_heap_ctrc_origin.inc"
#undef cluster_ctrc_origin_open_active
#undef cluster_ctrc_origin_open_reserved
#undef cluster_ctrc_origin_reserve_active
#undef cluster_ctrc_origin_reserve_entry
#undef cluster_ctrc_origin_original_full_xid_shared
#undef cluster_ctrc_origin_original_full_xid_exact
#undef cluster_ctrc_stat_bump
#undef ctrc_origin_index
#undef cluster_ctrc_shmem_ready
#undef LWLockRelease
#undef LWLockAcquire
#undef GetTopFullTransactionIdIfAny
#undef MyProc

static void
census_origin_publish_backend(const ClusterCtrcTxnKeyV1 *key, FullTransactionId full)
{
	UT_ASSERT(!census_origin_lock_held);
	census_origin_shared_locks = census_origin_exclusive_locks = census_origin_unlocks = 0;
	census_origin_top_reads = 0;
	memset(&census_origin_backend, 0, sizeof(census_origin_backend));
	census_origin_backend.xid = key->xid;
	pg_atomic_init_u64(&census_origin_backend.cluster_top_full_xid_atomic, U64FromFullTransactionId(full));
	census_origin_proc = &census_origin_backend;
	census_origin_top_full = full;
}


static ClusterCtrcOriginReserveResult
census_origin_reserve_from_backend(const ClusterCtrcTxnKeyV1 *key, FullTransactionId full,
								  ClusterCtrcOriginReservation *reservation)
{
	census_origin_publish_backend(key, full);
	return original_census_origin_reserve_active(key, reservation);
}
UT_TEST(test_census_shared_origin_reservation_retains_original_full_identity)
{
	QualifiedXminFixture f; CensusRuntimeAllocation runtime;
	ClusterCtrcTxnKeyV1 key; ClusterCtrcOriginReservation reservation;
	FullTransactionId full = FullTransactionIdFromEpochAndXid(1, 100);
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); fixture_release_original_writer();
	census_runtime_initialize_requested = true;
	if (!census_full_layout_setup(&runtime, 2)) { UT_ASSERT(false); return; }
	key = census_key_for_full(&f.cohort.admission_fence, full);
	key.segment_id = key.origin_node_id * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	UT_ASSERT_EQ(census_origin_reserve_from_backend(&key, full, &reservation), CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
	UT_ASSERT(reservation.valid && FullTransactionIdEquals(reservation.original_full_xid, full));
	UT_ASSERT_EQ(ctrc_origin_entries()[0].state, CTRC_ORIGIN_EMPTY);
	UT_ASSERT(FullTransactionIdEquals(ctrc_origin_entries()[0].original_full_xid, full));
	census_full_layout_release(&runtime);
}


UT_TEST(test_census_shared_origin_exact_open_and_replay_use_physical_slot)
{
	QualifiedXminFixture f; CensusRuntimeAllocation runtime;
	ClusterCtrcTxnKeyV1 key; ClusterCtrcOriginReservation reservation, replay;
	ClusterCtrcOriginEntry before; FullTransactionId observed;
	FullTransactionId full = FullTransactionIdFromEpochAndXid(1, 100);
	uint32 grant = UINT32_MAX, replay_grant = UINT32_MAX;
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); fixture_release_original_writer();
	census_runtime_initialize_requested = true;
	if (!census_full_layout_setup(&runtime, 2)) { UT_ASSERT(false); return; }
	key = census_key_for_full(&f.cohort.admission_fence, full);
	key.segment_id = 2; key.slot_offset = 3;
	UT_ASSERT_EQ(census_origin_reserve_from_backend(&key, full, &reservation), CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
	UT_ASSERT_EQ(reservation.origin_index, TT_SLOTS_PER_SEGMENT + 3);
	UT_ASSERT(ctrc_bytes_zero(&ctrc_origin_entries()[0], sizeof(ClusterCtrcOriginEntry)));
	UT_ASSERT_EQ(census_origin_top_reads, 1);
	observed = full;
	UT_ASSERT(!original_census_origin_full_shared(&key, &observed));
	UT_ASSERT(FullTransactionIdEquals(observed, InvalidFullTransactionId));
	UT_ASSERT(original_census_origin_open_reserved(&reservation, &grant));
	UT_ASSERT_EQ(grant, 1); UT_ASSERT_EQ(CtrcShared->global_grant_generation, 2);
	UT_ASSERT(original_census_origin_full_shared(&key, &observed));
	UT_ASSERT(FullTransactionIdEquals(observed, full));
	UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->stats[CTRC_STAT_GRANT_ISSUED]), 1);
	before = ctrc_origin_entries()[reservation.origin_index];
	UT_ASSERT_EQ(original_census_origin_reserve_active(&key, &replay), CLUSTER_CTRC_ORIGIN_RESERVED_EXISTING_OPEN);
	UT_ASSERT_EQ(replay.kind, CTRC_ORIGIN_RESERVATION_EXISTING_OPEN);
	UT_ASSERT_EQ(replay.reservation_generation, reservation.reservation_generation);
	UT_ASSERT(FullTransactionIdEquals(replay.original_full_xid, full));
	UT_ASSERT(original_census_origin_open_reserved(&replay, &replay_grant));
	UT_ASSERT_EQ(replay_grant, grant);
	UT_ASSERT_EQ(CtrcShared->global_reservation_generation, 2); UT_ASSERT_EQ(CtrcShared->global_grant_generation, 2);
	UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->stats[CTRC_STAT_GRANT_ISSUED]), 1);
	UT_ASSERT(memcmp(&before, &ctrc_origin_entries()[reservation.origin_index], sizeof(before)) == 0);
	UT_ASSERT_EQ(census_origin_shared_locks, 2); UT_ASSERT_EQ(census_origin_exclusive_locks, 4);
	UT_ASSERT_EQ(census_origin_unlocks, 6); UT_ASSERT(!census_origin_lock_held);
	UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
	census_full_layout_release(&runtime);
}

UT_TEST(test_census_shared_origin_backend_preflight_refuses_without_publication)
{
	for (int fault = 0; fault < 12; fault++) {
		QualifiedXminFixture f; CensusRuntimeAllocation runtime;
		ClusterCtrcTxnKeyV1 key; const ClusterCtrcTxnKeyV1 *input;
		ClusterCtrcOriginReservation reservation; unsigned char *before;
		FullTransactionId full = FullTransactionIdFromEpochAndXid(1, 100);
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); fixture_release_original_writer();
		census_runtime_initialize_requested = true;
		if (!census_full_layout_setup(&runtime, 2)) { UT_ASSERT(false); return; }
		key = census_key_for_full(&f.cohort.admission_fence, full); input = &key;
		census_origin_publish_backend(&key, full);
		switch (fault) {
			case 0: input = NULL; break;
			case 1: CtrcShared->magic ^= 1; break;
			case 2: key.owner_instance = 2; break;
			case 3: key.segment_id = 0; break;
			case 4: key.slot_offset = TT_SLOTS_PER_SEGMENT; break;
			case 5: census_origin_proc = NULL; break;
			case 6: key.origin_node_id = 1; key.owner_instance = 2; key.segment_id = CLUSTER_UNDO_SEGS_PER_INSTANCE + 1; break;
			case 7: census_origin_backend.xid++; break;
			case 8: census_origin_top_full = InvalidFullTransactionId; break;
			case 9: census_origin_top_full = FullTransactionIdFromU64(UINT64_MAX); break;
			case 10: census_origin_top_full = FullTransactionIdFromEpochAndXid(1, 101); break;
			case 11: pg_atomic_write_u64(&census_origin_backend.cluster_top_full_xid_atomic, U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(2, 100))); break;
		}
		before = malloc(runtime.capacity.total_bytes); UT_ASSERT(before != NULL);
		if (before == NULL) { census_full_layout_release(&runtime); return; }
		memcpy(before, CtrcShared, runtime.capacity.total_bytes); memset(&reservation, 0xa5, sizeof(reservation));
		UT_ASSERT_EQ(original_census_origin_reserve_active(input, &reservation), CLUSTER_CTRC_ORIGIN_RESERVE_REFUSED);
		UT_ASSERT(ctrc_bytes_zero(&reservation, sizeof(reservation)));
		UT_ASSERT(memcmp(before, CtrcShared, runtime.capacity.total_bytes) == 0);
		UT_ASSERT_EQ(census_origin_shared_locks + census_origin_exclusive_locks + census_origin_unlocks, 0);
		UT_ASSERT_EQ(census_origin_top_reads, fault >= 8 ? 1 : 0);
		UT_ASSERT(!census_origin_lock_held && !local_x && f.admission.entered);
		UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0); UT_ASSERT_EQ(backend_pin_count, 1);
		free(before); census_full_layout_release(&runtime);
	}
}

UT_TEST(test_census_shared_origin_raw_reuse_cannot_replace_original_full_identity)
{
	QualifiedXminFixture f; CensusRuntimeAllocation runtime;
	ClusterCtrcTxnKeyV1 key, different; ClusterCtrcOriginReservation reservation, refused;
	ClusterCtrcOriginEntry before; FullTransactionId observed;
	FullTransactionId full = FullTransactionIdFromEpochAndXid(1, 100);
	uint32 grant;
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); fixture_release_original_writer();
	census_runtime_initialize_requested = true;
	if (!census_full_layout_setup(&runtime, 2)) { UT_ASSERT(false); return; }
	key = census_key_for_full(&f.cohort.admission_fence, full);
	UT_ASSERT_EQ(census_origin_reserve_from_backend(&key, full, &reservation), CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
	UT_ASSERT(original_census_origin_open_reserved(&reservation, &grant));
	before = ctrc_origin_entries()[0];
	UT_ASSERT_EQ(census_origin_reserve_from_backend(&key, FullTransactionIdFromEpochAndXid(2, 100), &refused), CLUSTER_CTRC_ORIGIN_RESERVE_REFUSED);
	UT_ASSERT(ctrc_bytes_zero(&refused, sizeof(refused)));
	UT_ASSERT(memcmp(&before, &ctrc_origin_entries()[0], sizeof(before)) == 0);
	UT_ASSERT_EQ(CtrcShared->global_reservation_generation, 2); UT_ASSERT_EQ(CtrcShared->global_grant_generation, 2);
	UT_ASSERT_EQ(CtrcShared->full_refusal_count, 1);
	UT_ASSERT(original_census_origin_full_shared(&key, &observed)); UT_ASSERT(FullTransactionIdEquals(observed, full));
	different = key; different.slot_wrap++;
	observed = full; UT_ASSERT(!original_census_origin_full_shared(&different, &observed));
	UT_ASSERT(FullTransactionIdEquals(observed, InvalidFullTransactionId));
	UT_ASSERT(memcmp(&before, &ctrc_origin_entries()[0], sizeof(before)) == 0);
	/* A controlled released row is only an API refusal fixture, not a certificate. */
	ctrc_origin_entries()[0].state = CTRC_ORIGIN_RELEASE_PROVEN; before = ctrc_origin_entries()[0];
	UT_ASSERT_EQ(original_census_origin_reserve_active(&different, &refused), CLUSTER_CTRC_ORIGIN_RESERVE_RETRY_RELEASED);
	UT_ASSERT(ctrc_bytes_zero(&refused, sizeof(refused)));
	UT_ASSERT(memcmp(&before, &ctrc_origin_entries()[0], sizeof(before)) == 0);
	UT_ASSERT_EQ(CtrcShared->global_reservation_generation, 2); UT_ASSERT_EQ(CtrcShared->global_grant_generation, 2);
	UT_ASSERT_EQ(census_origin_unlocks, census_origin_exclusive_locks + census_origin_shared_locks);
	UT_ASSERT(!census_origin_lock_held); UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
	census_full_layout_release(&runtime);
}

UT_TEST(test_census_shared_origin_open_refuses_stale_reservation_without_grant)
{
	for (int fault = 0; fault < 11; fault++) {
		QualifiedXminFixture f; CensusRuntimeAllocation runtime;
		ClusterCtrcTxnKeyV1 key; ClusterCtrcOriginReservation reservation;
		ClusterCtrcOriginEntry before; uint32 grant = UINT32_MAX;
		FullTransactionId full = FullTransactionIdFromEpochAndXid(1, 100);
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); fixture_release_original_writer();
		census_runtime_initialize_requested = true;
		if (!census_full_layout_setup(&runtime, 2)) { UT_ASSERT(false); return; }
		key = census_key_for_full(&f.cohort.admission_fence, full);
		UT_ASSERT_EQ(census_origin_reserve_from_backend(&key, full, &reservation), CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
		switch (fault) {
			case 0: reservation.valid = false; break;
			case 1: reservation.kind = CTRC_ORIGIN_RESERVATION_INVALID; break;
			case 2: reservation.reservation_generation = 0; break;
			case 3: reservation.original_full_xid = InvalidFullTransactionId; break;
			case 4: reservation.original_full_xid = FullTransactionIdFromEpochAndXid(2, 100); break;
			case 5: reservation.origin_index++; break;
			case 6: reservation.reserved8[0] = 1; break;
			case 7: reservation.key.slot_offset++; break;
			case 8: reservation.reservation_generation++; break;
			case 9: CtrcShared->global_grant_generation = UINT32_MAX; break;
			case 10: CtrcShared->magic ^= 1; break;
		}
		before = ctrc_origin_entries()[0];
		UT_ASSERT(!original_census_origin_open_reserved(&reservation, &grant)); UT_ASSERT_EQ(grant, 0);
		UT_ASSERT(memcmp(&before, &ctrc_origin_entries()[0], sizeof(before)) == 0);
		UT_ASSERT_EQ(CtrcShared->global_reservation_generation, 2);
		UT_ASSERT_EQ(CtrcShared->global_grant_generation, fault == 9 ? UINT32_MAX : 1);
		UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->stats[CTRC_STAT_GRANT_ISSUED]), 0);
		UT_ASSERT_EQ(census_origin_unlocks, census_origin_exclusive_locks + census_origin_shared_locks);
		UT_ASSERT(!census_origin_lock_held && !local_x && f.admission.entered);
		UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0); UT_ASSERT_EQ(backend_pin_count, 1);
		census_full_layout_release(&runtime);
	}
}

UT_TEST(test_census_shared_origin_producer_then_complete_walk_continues_native_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3]; CensusRuntimeAllocation runtime;
		ClusterCtrcTxnKeyV1 key; ClusterCtrcOriginReservation reservation; uint32 grant;
		FullTransactionId full = FullTransactionIdFromEpochAndXid(1, 100);
		uint64 passes = 1, expected_passes, total;
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		qualified_xmin_require_peer(&f, 65); fixture_release_original_writer();
		census_runtime_initialize_requested = true;
		if (!census_full_layout_setup(&runtime, 2)) { UT_ASSERT(false); return; }
		key = census_key_for_full(&f.cohort.admission_fence, full);
		UT_ASSERT_EQ(census_origin_reserve_from_backend(&key, full, &reservation), CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
		UT_ASSERT(original_census_origin_open_reserved(&reservation, &grant));
		UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation), 1);
		UT_ASSERT_EQ(census_origin_exclusive_locks, 2); UT_ASSERT_EQ(census_origin_unlocks, 2);
		sampler_xids[0] = 152;
		pg_atomic_write_u64(&sampler_slots[0].cluster_top_full_xid_atomic, U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(1, 152)));
		total = runtime.capacity.origin_key_entries + runtime.capacity.participant_key_entries;
		expected_passes = (total + CLUSTER_CTRC_ACTIVE_SCAN_SLICE - 1) / CLUSTER_CTRC_ACTIVE_SCAN_SLICE;
		native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1; resume_allowed = true;
		worker_original_sample_pass_fixture(&f.cohort);
		while (pass_pending && passes <= expected_passes) { original_worker_sample_active_pass(); passes++; }
		UT_ASSERT_EQ(passes, expected_passes); UT_ASSERT(!pass_pending);
		UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
		UT_ASSERT_EQ(publisher_scan.unqualified_registration_generation, 1);
		UT_ASSERT_EQ(publisher_scan.next_index, total); UT_ASSERT_EQ(publisher_scan.unqualified_entries, 0);
		UT_ASSERT(FullTransactionIdEquals(publisher_scan.min_full, full));
		UT_ASSERT_EQ(U64FromFullTransactionId(publisher_scan.min_full), f.cohort.active_full_xid_min);
		UT_ASSERT_EQ(census_step_lock_acquires, total); UT_ASSERT_EQ(census_step_lock_releases, total);
		f.cohort.local_registration_cut = publisher_scan;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(!census_origin_lock_held && !local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1); census_full_layout_release(&runtime);
	}
}

/* The complete original publisher is used below. Only physical lock scheduling
 * and the external object-admission predicates remain explicit controlled leaves. */
static LWLock *participant_lock_stack[CLUSTER_CTRC_CLEANER_WORKERS + 2];
static LWLockMode participant_lock_modes[CLUSTER_CTRC_CLEANER_WORKERS + 2];
static unsigned participant_lock_depth, participant_lock_acquires, participant_lock_releases;
static unsigned participant_origin_guards, participant_guard_publications;
static unsigned participant_admission_samples, participant_admission_current_reads;
static bool participant_admission_sample_ok = true, participant_admission_current_ok = true;
static ClusterCtrcReceiptHandle *participant_watched_handle;
static ClusterCtrcParticipantIdentity participant_identity;
static ClusterCtrcPublicationIdV1 participant_publication;
static ClusterCtrcTargetV1 participant_target;
static ClusterCtrcPreparePath participant_prepare_path;

static bool
participant_publisher_lock_acquire(LWLock *lock, LWLockMode mode)
{
	worker_pass_assert_unlocked(); UT_ASSERT(!census_origin_lock_held);
	UT_ASSERT(participant_lock_depth < lengthof(participant_lock_stack));
	if (lock == &CtrcShared->origin_lock) {
		UT_ASSERT_EQ(participant_lock_depth, 0);
		UT_ASSERT(mode == LW_SHARED || mode == LW_EXCLUSIVE);
		if (mode == LW_SHARED) participant_origin_guards++;
	} else if (lock == &CtrcShared->receipt_lock) {
		UT_ASSERT(mode == LW_SHARED || (mode == LW_EXCLUSIVE && participant_lock_depth >= 1));
		/* Original ACK kernels can inspect an empty eligible set under pool S. */
		if (participant_lock_depth != 0)
			UT_ASSERT(participant_lock_stack[participant_lock_depth - 1] != &CtrcShared->origin_lock);
	} else {
		unsigned shard;
		for (shard = 0; shard < CLUSTER_CTRC_CLEANER_WORKERS; shard++)
			if (lock == &CtrcShared->participant_locks[shard].lock) break;
		UT_ASSERT(shard < CLUSTER_CTRC_CLEANER_WORKERS && mode == LW_EXCLUSIVE);
		if (participant_lock_depth != 0 && participant_lock_stack[participant_lock_depth - 1] != &CtrcShared->origin_lock) {
			unsigned previous;
			for (previous = 0; previous < CLUSTER_CTRC_CLEANER_WORKERS; previous++)
				if (participant_lock_stack[participant_lock_depth - 1] == &CtrcShared->participant_locks[previous].lock) break;
			UT_ASSERT(previous < shard);
		}
	}
	participant_lock_stack[participant_lock_depth] = lock;
	participant_lock_modes[participant_lock_depth++] = mode; participant_lock_acquires++;
	return true;
}

static void
participant_publisher_lock_release(LWLock *lock)
{
	UT_ASSERT(participant_lock_depth != 0 && participant_lock_stack[participant_lock_depth - 1] == lock);
	if (lock == &CtrcShared->origin_lock && participant_lock_modes[participant_lock_depth - 1] == LW_SHARED
		&& participant_watched_handle != NULL && participant_watched_handle->valid) {
		const ClusterCtrcReceiptHandle *handle = participant_watched_handle;
		UT_ASSERT_EQ(participant_lock_depth, 1);
		UT_ASSERT(handle->participant->state == CTRC_PARTICIPANT_OPEN);
		UT_ASSERT_EQ(handle->receipt->state, CTRC_RECEIPT_PREPARED);
		UT_ASSERT(FullTransactionIdEquals(handle->original_full_xid, handle->participant->original_full_xid));
		participant_guard_publications++;
	}
	participant_lock_depth--; participant_lock_releases++;
}

static bool
participant_object_admission_begin(RelFileLocator locator, ClusterKoPeerAdmissionV3 *admission)
{
	worker_pass_assert_unlocked(); UT_ASSERT_EQ(participant_lock_depth, 0);
	UT_ASSERT(locator.spcOid != InvalidOid && admission != NULL);
	memset(admission, 0, sizeof(*admission)); participant_admission_samples++;
	return participant_admission_sample_ok;
}

static bool
participant_object_admission_current(const ClusterKoPeerAdmissionV3 *admission, RelFileLocator locator)
{
	UT_ASSERT(participant_lock_depth >= 2 && participant_lock_stack[participant_lock_depth - 1] == &CtrcShared->receipt_lock);
	UT_ASSERT(admission != NULL && locator.spcOid != InvalidOid); participant_admission_current_reads++;
	return participant_admission_current_ok;
}

static bool
participant_object_read_current(const ClusterKoPeerAdmissionV3 *admission, RelFileLocator locator,
							   ForkNumber fork, BlockNumber block)
{
	(void)fork; (void)block;
	return participant_object_admission_current(admission, locator);
}

#define cluster_ctrc_shmem_ready original_census_shmem_ready
#define cluster_ctrc_stat_bump original_census_stat_bump
#define ctrc_origin_index original_census_origin_index
#define LWLockAcquire participant_publisher_lock_acquire
#define LWLockRelease participant_publisher_lock_release
#define cluster_ko_peer_admission_begin_v3 participant_object_admission_begin
#define cluster_ko_peer_admission_current_v3 participant_object_admission_current
#define cluster_ko_peer_read_current_v3 participant_object_read_current
#define cluster_ctrc_origin_has_exact_touch original_participant_origin_has_exact_touch
#define cluster_ctrc_origin_record_touched original_participant_origin_record_touched
#define cluster_ctrc_participant_index_compute original_participant_participant_index_compute
#define cluster_ctrc_participant_open original_participant_participant_open
#define cluster_ctrc_receipt_prepare_shared_observed original_participant_receipt_prepare_shared_observed
#define ctrc_allocate_journal_sequence original_participant_allocate_journal_sequence
#define ctrc_origin_protects_local_receipt_locked original_participant_origin_protects_local_receipt_locked
#define ctrc_participant_identity_valid original_participant_participant_identity_valid
#define ctrc_participant_index original_participant_participant_index
#define ctrc_participant_locks_acquire original_participant_participant_locks_acquire
#define ctrc_participant_locks_release original_participant_participant_locks_release
#define ctrc_participant_receipt_heads original_participant_participant_receipt_heads
#define ctrc_publication_prepare_valid original_participant_publication_prepare_valid
#define ctrc_publication_request_equal original_participant_publication_request_equal
#define ctrc_receipt_chain_exact_locked original_participant_receipt_chain_exact_locked
#define ctrc_receipt_entries original_participant_receipt_entries
#define ctrc_receipt_handle_fill original_participant_receipt_handle_fill
#define ctrc_receipt_hash_bytes original_participant_receipt_hash_bytes
#define ctrc_receipt_links original_participant_receipt_links
#define ctrc_receipt_prepare_existing_locked original_participant_receipt_prepare_existing_locked
#define ctrc_receipt_prepare_locators_valid_locked original_participant_receipt_prepare_locators_valid_locked
#define ctrc_receipt_prepare_refused original_participant_receipt_prepare_refused
#define ctrc_receipt_prepare_shared_internal original_participant_receipt_prepare_shared_internal
#define ctrc_receipt_prepare_table_observed original_participant_receipt_prepare_table_observed
#define ctrc_receipt_prepare_with_sequence original_participant_receipt_prepare_with_sequence
#define ctrc_receipt_probe_hash original_participant_receipt_probe_hash
#define ctrc_receipt_probe_states original_participant_receipt_probe_states
#define ctrc_receipt_publication_matches original_participant_receipt_publication_matches
#define ctrc_relation_namespace_valid original_participant_relation_namespace_valid
#define ctrc_target_pending_itl_valid original_participant_target_pending_itl_valid
#define ctrc_target_pending_offnum_valid original_participant_target_pending_offnum_valid
#define ctrc_target_predecessor_version_valid original_participant_target_predecessor_version_valid
#include "test_cluster_heap_ctrc_participant.inc"
#undef ctrc_target_predecessor_version_valid
#undef ctrc_target_pending_offnum_valid
#undef ctrc_target_pending_itl_valid
#undef ctrc_relation_namespace_valid
#undef ctrc_receipt_publication_matches
#undef ctrc_receipt_probe_states
#undef ctrc_receipt_probe_hash
#undef ctrc_receipt_prepare_with_sequence
#undef ctrc_receipt_prepare_table_observed
#undef ctrc_receipt_prepare_shared_internal
#undef ctrc_receipt_prepare_refused
#undef ctrc_receipt_prepare_locators_valid_locked
#undef ctrc_receipt_prepare_existing_locked
#undef ctrc_receipt_links
#undef ctrc_receipt_hash_bytes
#undef ctrc_receipt_handle_fill
#undef ctrc_receipt_entries
#undef ctrc_receipt_chain_exact_locked
#undef ctrc_publication_request_equal
#undef ctrc_publication_prepare_valid
#undef ctrc_participant_receipt_heads
#undef ctrc_participant_locks_release
#undef ctrc_participant_locks_acquire
#undef ctrc_participant_index
#undef ctrc_participant_identity_valid
#undef ctrc_origin_protects_local_receipt_locked
#undef ctrc_allocate_journal_sequence
#undef cluster_ctrc_receipt_prepare_shared_observed
#undef cluster_ctrc_participant_open
#undef cluster_ctrc_participant_index_compute
#undef cluster_ctrc_origin_record_touched
#undef cluster_ctrc_origin_has_exact_touch
#undef cluster_ko_peer_read_current_v3
#undef cluster_ko_peer_admission_current_v3
#undef cluster_ko_peer_admission_begin_v3
#undef LWLockRelease
#undef LWLockAcquire
#undef ctrc_origin_index
#undef cluster_ctrc_stat_bump
#undef cluster_ctrc_shmem_ready

static void
participant_publisher_inputs(const ClusterCtrcTxnKeyV1 *key, uint32 grant)
{
	UT_ASSERT_EQ(participant_lock_depth, 0);
	participant_lock_acquires = participant_lock_releases = participant_origin_guards = participant_guard_publications = 0;
	participant_admission_samples = participant_admission_current_reads = 0;
	participant_admission_sample_ok = participant_admission_current_ok = true;
	participant_watched_handle = NULL;
	memset(&participant_identity, 0, sizeof(participant_identity));
	participant_identity.node_id = (uint16)cluster_node_id;
	participant_identity.boot_incarnation = key->origin_boot_incarnation;
	participant_identity.formation_epoch = key->formation_epoch;
	participant_identity.admission_record_generation = key->admission_record_generation;
	participant_identity.capability_record_generation = 1;
	memset(&participant_publication, 0, sizeof(participant_publication));
	participant_publication.requester_node_id = participant_identity.node_id;
	participant_publication.requester_boot_incarnation = participant_identity.boot_incarnation;
	participant_publication.capability_record_generation = participant_identity.capability_record_generation;
	participant_publication.requester_backend_id = 11;
	participant_publication.wire_request_id = 101; participant_publication.operation_id = 201;
	participant_publication.attempt_generation = 1; participant_publication.member_ordinal = UINT16_MAX;
	participant_publication.reference_kind = CTRC_REF_HEAP_ITL_UBA;
	participant_publication.target_kind = CTRC_TARGET_PAGE_PENDING_ITL_SLOT;
	participant_publication.grant_generation = grant;
	memset(&participant_target, 0, sizeof(participant_target));
	participant_target.kind = CTRC_TARGET_PAGE_PENDING_ITL_SLOT;
	participant_target.spc_oid = 1663; participant_target.db_oid = 5; participant_target.rel_number = 9001;
	participant_target.block_number = 44;
	participant_target.predecessor_page_lsn_origin_node_id = CLUSTER_CTRC_PAGE_LSN_ORIGIN_INVALID;
	participant_target.publication_own_generation = 17; participant_target.publication_acquisition_epoch = 19;
	participant_target.relation_persistence = RELPERSISTENCE_PERMANENT;
	participant_target.needs_wal = true; participant_target.page_operation_kind = 1;
}

static ClusterCtrcPrepareResult
participant_publisher_prepare(const ClusterCtrcTxnKeyV1 *key, uint32 grant, ClusterCtrcReceiptHandle *handle)
{
	ClusterCtrcPrepareResult result;
	participant_watched_handle = handle;
	result = original_participant_receipt_prepare_shared_observed(key, &participant_identity, grant,
		&participant_publication, &participant_target, handle, &participant_prepare_path);
	participant_watched_handle = NULL;
	UT_ASSERT_EQ(participant_lock_depth, 0); UT_ASSERT_EQ(participant_lock_acquires, participant_lock_releases);
	return result;
}

static void
participant_publisher_touch(const ClusterCtrcTxnKeyV1 *key, uint32 grant)
{
	uint64 index; uint32 touched_grant;
	UT_ASSERT(original_census_origin_index(key, &index));
	census_origin_lock_acquire(&CtrcShared->origin_lock, LW_EXCLUSIVE);
	UT_ASSERT_EQ(original_participant_origin_record_touched(&ctrc_origin_entries()[index], &participant_identity,
		CTRC_PROOF_ACTIVE, &touched_grant), CLUSTER_CTRC_TOUCH_RECORDED);
	UT_ASSERT_EQ(touched_grant, grant);
	census_origin_lock_release(&CtrcShared->origin_lock);
}

static ClusterCtrcPrepareResult
census_participant_prepare_from_origin(const ClusterCtrcTxnKeyV1 *key, uint32 grant, ClusterCtrcReceiptHandle *handle)
{
	participant_publisher_inputs(key, grant); participant_publisher_touch(key, grant);
	return participant_publisher_prepare(key, grant, handle);
}

UT_TEST(test_census_participant_publisher_retains_original_origin_full_identity)
{
	QualifiedXminFixture f; CensusRuntimeAllocation runtime;
	ClusterCtrcTxnKeyV1 key; ClusterCtrcOriginReservation reservation; ClusterCtrcReceiptHandle handle;
	FullTransactionId full = FullTransactionIdFromEpochAndXid(1, 100); uint32 grant;
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); fixture_release_original_writer();
	census_runtime_initialize_requested = true;
	if (!census_full_layout_setup(&runtime, 2)) { UT_ASSERT(false); return; }
	key = census_key_for_full(&f.cohort.admission_fence, full);
	UT_ASSERT_EQ(census_origin_reserve_from_backend(&key, full, &reservation), CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
	UT_ASSERT(original_census_origin_open_reserved(&reservation, &grant));
	UT_ASSERT_EQ(census_participant_prepare_from_origin(&key, grant, &handle), CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT_EQ(ctrc_participant_entries()[0].state, CTRC_PARTICIPANT_OPEN);
	UT_ASSERT(FullTransactionIdEquals(ctrc_participant_entries()[0].original_full_xid, full));
	census_full_layout_release(&runtime);
}


UT_TEST(test_census_participant_original_publish_and_duplicate_keep_chain_and_full_identity)
{
	QualifiedXminFixture f; CensusRuntimeAllocation runtime;
	ClusterCtrcTxnKeyV1 key; ClusterCtrcOriginReservation reservation;
	ClusterCtrcReceiptHandle handle, replay; ClusterCtrcParticipantEntry before; ClusterCtrcReceipt receipt_before;
	FullTransactionId full = FullTransactionIdFromEpochAndXid(1, 100); uint32 grant;
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); fixture_release_original_writer();
	census_runtime_initialize_requested = true;
	if (!census_full_layout_setup(&runtime, 2)) { UT_ASSERT(false); return; }
	key = census_key_for_full(&f.cohort.admission_fence, full); key.segment_id = 2; key.slot_offset = 3;
	UT_ASSERT_EQ(census_origin_reserve_from_backend(&key, full, &reservation), CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
	UT_ASSERT(original_census_origin_open_reserved(&reservation, &grant));
	UT_ASSERT_EQ(census_participant_prepare_from_origin(&key, grant, &handle), CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT(handle.valid && FullTransactionIdEquals(handle.original_full_xid, full));
	UT_ASSERT(handle.participant == &ctrc_participant_entries()[handle.participant_index]);
	UT_ASSERT(handle.participant_index != 0); UT_ASSERT(ctrc_bytes_zero(&ctrc_participant_entries()[0], sizeof(before)));
	UT_ASSERT_EQ(handle.participant->receipt_count, 1); UT_ASSERT_EQ(handle.participant->prepared_count, 1);
	UT_ASSERT_EQ(original_participant_participant_receipt_heads()[handle.participant_index], handle.receipt_index + 1);
	original_participant_participant_locks_acquire(ctrc_participant_lock_mask(handle.participant_index));
	participant_publisher_lock_acquire(&CtrcShared->receipt_lock, LW_EXCLUSIVE);
	UT_ASSERT(original_participant_receipt_chain_exact_locked(handle.participant_index, false));
	participant_publisher_lock_release(&CtrcShared->receipt_lock);
	original_participant_participant_locks_release(ctrc_participant_lock_mask(handle.participant_index));
	UT_ASSERT_EQ(participant_guard_publications, 1); UT_ASSERT_EQ(participant_admission_samples, 1);
	UT_ASSERT_EQ(participant_admission_current_reads, 1);
	before = *handle.participant; receipt_before = *handle.receipt;
	UT_ASSERT_EQ(participant_publisher_prepare(&key, grant, &replay), CLUSTER_CTRC_PREPARE_DUPLICATE);
	UT_ASSERT(replay.valid && FullTransactionIdEquals(replay.original_full_xid, full));
	UT_ASSERT_EQ(replay.receipt_index, handle.receipt_index); UT_ASSERT_EQ(replay.journal_slot_generation, handle.journal_slot_generation);
	UT_ASSERT(memcmp(&before, handle.participant, sizeof(before)) == 0);
	UT_ASSERT(memcmp(&receipt_before, handle.receipt, sizeof(receipt_before)) == 0);
	UT_ASSERT_EQ(CtrcShared->global_journal_generation, 3); /* Duplicate may consume an unused scalar. */
	UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->stats[CTRC_STAT_RECEIPT_PREPARED]), 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation), 1);
	UT_ASSERT_EQ(participant_origin_guards, 2); UT_ASSERT_EQ(participant_guard_publications, 2);
	UT_ASSERT_EQ(participant_lock_depth, 0); UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
	census_full_layout_release(&runtime);
}

UT_TEST(test_census_participant_unknown_or_foreign_origin_stays_unqualified_on_later_replay)
{
	for (int foreign = 0; foreign < 2; foreign++) {
		QualifiedXminFixture f; CensusRuntimeAllocation runtime;
		ClusterCtrcTxnKeyV1 key; ClusterCtrcOriginReservation reservation; ClusterCtrcReceiptHandle handle, replay;
		FullTransactionId full = FullTransactionIdFromEpochAndXid(1, 100); uint32 grant;
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); fixture_release_original_writer();
		census_runtime_initialize_requested = true;
		if (!census_full_layout_setup(&runtime, 2)) { UT_ASSERT(false); return; }
		key = census_key_for_full(&f.cohort.admission_fence, full);
		UT_ASSERT_EQ(census_origin_reserve_from_backend(&key, full, &reservation), CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
		UT_ASSERT(original_census_origin_open_reserved(&reservation, &grant));
		participant_publisher_inputs(&key, grant); participant_publisher_touch(&key, grant);
		if (foreign) {
			/* Foreign grant authentication is a controlled input, not a remote protocol run. */
			key.origin_node_id = 1; key.owner_instance = 2; key.segment_id = CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
		} else ctrc_origin_entries()[0].original_full_xid = InvalidFullTransactionId;
		UT_ASSERT_EQ(participant_publisher_prepare(&key, grant, &handle), CLUSTER_CTRC_PREPARE_READY);
		UT_ASSERT(handle.valid && FullTransactionIdEquals(handle.original_full_xid, InvalidFullTransactionId));
		UT_ASSERT(FullTransactionIdEquals(handle.participant->original_full_xid, InvalidFullTransactionId));
		UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation), 2);
		UT_ASSERT_EQ(participant_origin_guards, foreign ? 0 : 1);
		if (!foreign) ctrc_origin_entries()[0].original_full_xid = full; /* Controlled metadata restoration. */
		UT_ASSERT_EQ(participant_publisher_prepare(&key, grant, &replay), CLUSTER_CTRC_PREPARE_DUPLICATE);
		UT_ASSERT(FullTransactionIdEquals(replay.original_full_xid, InvalidFullTransactionId));
		UT_ASSERT(FullTransactionIdEquals(replay.participant->original_full_xid, InvalidFullTransactionId));
		UT_ASSERT_EQ(replay.participant->receipt_count, 1);
		UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation), 2);
		UT_ASSERT_EQ(participant_lock_depth, 0); UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
		census_full_layout_release(&runtime);
	}
}

UT_TEST(test_census_participant_shared_preflight_refuses_before_new_receiver)
{
	for (int fault = 0; fault < 11; fault++) {
		QualifiedXminFixture f; CensusRuntimeAllocation runtime;
		ClusterCtrcTxnKeyV1 key; ClusterCtrcOriginReservation reservation; ClusterCtrcReceiptHandle handle;
		FullTransactionId full = FullTransactionIdFromEpochAndXid(1, 100); uint32 grant;
		ClusterCtrcPreparePath expected = CLUSTER_CTRC_PREPARE_PATH_UNKNOWN;
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); fixture_release_original_writer();
		census_runtime_initialize_requested = true;
		if (!census_full_layout_setup(&runtime, 2)) { UT_ASSERT(false); return; }
		key = census_key_for_full(&f.cohort.admission_fence, full);
		UT_ASSERT_EQ(census_origin_reserve_from_backend(&key, full, &reservation), CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
		UT_ASSERT(original_census_origin_open_reserved(&reservation, &grant));
		participant_publisher_inputs(&key, grant); participant_publisher_touch(&key, grant);
		switch (fault) {
			case 0: participant_target.spc_oid = InvalidOid; expected = CLUSTER_CTRC_PREPARE_PATH_NAMESPACE; break;
			case 1: participant_identity.node_id = 1; expected = CLUSTER_CTRC_PREPARE_PATH_INPUT; break;
			case 2: key.segment_id = 0; expected = CLUSTER_CTRC_PREPARE_PATH_INPUT; break;
			case 3: CtrcShared->magic ^= 1; expected = CLUSTER_CTRC_PREPARE_PATH_INPUT; break;
			case 4: expected = CLUSTER_CTRC_PREPARE_PATH_INPUT; break;
			case 5: CtrcShared->global_journal_generation = UINT64_MAX; expected = CLUSTER_CTRC_PREPARE_PATH_JOURNAL; break;
			case 6: ctrc_origin_entries()[0].state = CTRC_ORIGIN_RELEASE_PROVEN; break;
			case 7: grant++; break;
			case 8: ctrc_origin_entries()[0].touched_bitmap = 0; break;
			case 9: participant_admission_sample_ok = false; expected = CLUSTER_CTRC_PREPARE_PATH_OBJECT_EXISTING; break;
			case 10: participant_admission_current_ok = false; expected = CLUSTER_CTRC_PREPARE_PATH_OBJECT_EXISTING; break;
		}
		memset(&handle, 0xa5, sizeof(handle));
		UT_ASSERT_EQ(participant_publisher_prepare(&key, grant, fault == 4 ? NULL : &handle), CLUSTER_CTRC_PREPARE_REFUSED);
		if (fault != 4) UT_ASSERT(ctrc_bytes_zero(&handle, sizeof(handle)));
		UT_ASSERT_EQ(participant_prepare_path, expected);
		UT_ASSERT(ctrc_bytes_zero(ctrc_participant_entries(), runtime.capacity.participant_key_entries * sizeof(ClusterCtrcParticipantEntry)));
		UT_ASSERT(ctrc_bytes_zero(original_participant_receipt_entries(), runtime.capacity.receipt_entries * sizeof(ClusterCtrcReceipt)));
		UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation), 1);
		UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->stats[CTRC_STAT_RECEIPT_PREPARED]), 0);
		UT_ASSERT_EQ(participant_lock_depth, 0); UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
		UT_ASSERT(!local_x && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
		census_full_layout_release(&runtime);
	}
}

UT_TEST(test_census_participant_known_full_conflict_precedes_chain_refusal_without_mutation)
{
	for (int fault = 0; fault < 3; fault++) {
		QualifiedXminFixture f; CensusRuntimeAllocation runtime;
		ClusterCtrcTxnKeyV1 key; ClusterCtrcOriginReservation reservation; ClusterCtrcReceiptHandle handle, refused;
		ClusterCtrcParticipantEntry before; ClusterCtrcReceipt receipt_before; CtrcReceiptLink link_before;
		FullTransactionId full = FullTransactionIdFromEpochAndXid(1, 100); uint32 grant; uint64 head;
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); fixture_release_original_writer();
		census_runtime_initialize_requested = true;
		if (!census_full_layout_setup(&runtime, 2)) { UT_ASSERT(false); return; }
		key = census_key_for_full(&f.cohort.admission_fence, full);
		UT_ASSERT_EQ(census_origin_reserve_from_backend(&key, full, &reservation), CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
		UT_ASSERT(original_census_origin_open_reserved(&reservation, &grant));
		UT_ASSERT_EQ(census_participant_prepare_from_origin(&key, grant, &handle), CLUSTER_CTRC_PREPARE_READY);
		ctrc_origin_entries()[0].original_full_xid = FullTransactionIdFromEpochAndXid(2, 100);
		if (fault == 1) handle.participant->receipt_count++;
		if (fault == 2) original_participant_participant_receipt_heads()[handle.participant_index] = runtime.capacity.receipt_entries + 1;
		before = *handle.participant; receipt_before = *handle.receipt;
		link_before = original_participant_receipt_links()[handle.receipt_index];
		head = original_participant_participant_receipt_heads()[handle.participant_index];
		UT_ASSERT_EQ(participant_publisher_prepare(&key, grant, &refused), CLUSTER_CTRC_PREPARE_REFUSED);
		UT_ASSERT_EQ(participant_prepare_path, CLUSTER_CTRC_PREPARE_PATH_ORIGINAL_FULL_XID);
		UT_ASSERT(ctrc_bytes_zero(&refused, sizeof(refused)));
		UT_ASSERT(memcmp(&before, handle.participant, sizeof(before)) == 0);
		UT_ASSERT(memcmp(&receipt_before, handle.receipt, sizeof(receipt_before)) == 0);
		UT_ASSERT(memcmp(&link_before, &original_participant_receipt_links()[handle.receipt_index], sizeof(link_before)) == 0);
		UT_ASSERT_EQ(original_participant_participant_receipt_heads()[handle.participant_index], head);
		UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation), 1);
		UT_ASSERT_EQ(participant_lock_depth, 0); UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
		census_full_layout_release(&runtime);
	}
}

UT_TEST(test_census_participant_original_publisher_then_complete_walk_continues_native_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3]; CensusRuntimeAllocation runtime;
		ClusterCtrcTxnKeyV1 key; ClusterCtrcOriginReservation reservation; ClusterCtrcReceiptHandle handle;
		FullTransactionId full = FullTransactionIdFromEpochAndXid(1, 100); uint32 grant;
		uint64 passes = 1, expected_passes, total;
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		qualified_xmin_require_peer(&f, 65); fixture_release_original_writer();
		census_runtime_initialize_requested = true;
		if (!census_full_layout_setup(&runtime, 2)) { UT_ASSERT(false); return; }
		key = census_key_for_full(&f.cohort.admission_fence, full);
		UT_ASSERT_EQ(census_origin_reserve_from_backend(&key, full, &reservation), CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
		UT_ASSERT(original_census_origin_open_reserved(&reservation, &grant));
		UT_ASSERT_EQ(census_participant_prepare_from_origin(&key, grant, &handle), CLUSTER_CTRC_PREPARE_READY);
		UT_ASSERT(handle.valid && FullTransactionIdEquals(handle.original_full_xid, full));
		UT_ASSERT_EQ(participant_guard_publications, 1); UT_ASSERT_EQ(participant_lock_depth, 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation), 1);
		sampler_xids[0] = 152;
		pg_atomic_write_u64(&sampler_slots[0].cluster_top_full_xid_atomic, U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(1, 152)));
		total = runtime.capacity.origin_key_entries + runtime.capacity.participant_key_entries;
		expected_passes = (total + CLUSTER_CTRC_ACTIVE_SCAN_SLICE - 1) / CLUSTER_CTRC_ACTIVE_SCAN_SLICE;
		native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1; resume_allowed = true;
		worker_original_sample_pass_fixture(&f.cohort);
		while (pass_pending && passes <= expected_passes) { original_worker_sample_active_pass(); passes++; }
		UT_ASSERT_EQ(passes, expected_passes); UT_ASSERT(!pass_pending);
		UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
		UT_ASSERT_EQ(publisher_scan.unqualified_registration_generation, 1);
		UT_ASSERT_EQ(publisher_scan.next_index, total); UT_ASSERT_EQ(publisher_scan.unqualified_entries, 0);
		UT_ASSERT(FullTransactionIdEquals(publisher_scan.min_full, full));
		UT_ASSERT_EQ(census_step_lock_acquires, total); UT_ASSERT_EQ(census_step_lock_releases, total);
		f.cohort.local_registration_cut = publisher_scan;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(!local_x && !resumed_writer && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
		census_full_layout_release(&runtime);
	}
}

/* Receipt disposition is bookkeeping, not physical mutation or release proof. */
static unsigned receipt_terminal_wakeups;
static const ClusterCtrcReceiptHandle *receipt_terminal_watched;
static void
receipt_terminal_cleaner_wakeup(void)
{
	worker_pass_assert_unlocked(); UT_ASSERT_EQ(participant_lock_depth, 0);
	UT_ASSERT(receipt_terminal_watched != NULL && receipt_terminal_watched->valid);
	UT_ASSERT_EQ(receipt_terminal_watched->receipt->state, CTRC_RECEIPT_CANCELLED);
	receipt_terminal_wakeups++;
}

static bool receipt_discharge_wakeup_active;
static void
receipt_terminal_publication_wakeup(void)
{
	if (!receipt_discharge_wakeup_active)
	{
		receipt_terminal_cleaner_wakeup();
		return;
	}
	worker_pass_assert_unlocked(); UT_ASSERT_EQ(participant_lock_depth, 0);
	UT_ASSERT(receipt_terminal_watched != NULL && receipt_terminal_watched->valid);
	UT_ASSERT_EQ(receipt_terminal_watched->receipt->state, CTRC_RECEIPT_CLEANED);
	receipt_terminal_wakeups++;
}

#define cluster_ctrc_shmem_ready original_census_shmem_ready
#define cluster_ctrc_stat_bump original_census_stat_bump
#define cluster_undo_cleaner_wakeup receipt_terminal_publication_wakeup
#define cluster_ctrc_origin_has_exact_touch original_participant_origin_has_exact_touch
#define cluster_ctrc_origin_record_touched original_participant_origin_record_touched
#define cluster_ctrc_participant_index_compute original_participant_participant_index_compute
#define cluster_ctrc_participant_open original_participant_participant_open
#define cluster_ctrc_receipt_prepare_shared_observed original_participant_receipt_prepare_shared_observed
#define ctrc_allocate_journal_sequence original_participant_allocate_journal_sequence
#define ctrc_origin_protects_local_receipt_locked original_participant_origin_protects_local_receipt_locked
#define ctrc_participant_identity_valid original_participant_participant_identity_valid
#define ctrc_participant_index original_participant_participant_index
#define ctrc_participant_locks_acquire original_participant_participant_locks_acquire
#define ctrc_participant_locks_release original_participant_participant_locks_release
#define ctrc_participant_receipt_heads original_participant_participant_receipt_heads
#define ctrc_publication_prepare_valid original_participant_publication_prepare_valid
#define ctrc_publication_request_equal original_participant_publication_request_equal
#define ctrc_receipt_chain_exact_locked original_participant_receipt_chain_exact_locked
#define ctrc_receipt_entries original_participant_receipt_entries
#define ctrc_receipt_handle_fill original_participant_receipt_handle_fill
#define ctrc_receipt_hash_bytes original_participant_receipt_hash_bytes
#define ctrc_receipt_links original_participant_receipt_links
#define ctrc_receipt_prepare_existing_locked original_participant_receipt_prepare_existing_locked
#define ctrc_receipt_prepare_locators_valid_locked original_participant_receipt_prepare_locators_valid_locked
#define ctrc_receipt_prepare_refused original_participant_receipt_prepare_refused
#define ctrc_receipt_prepare_shared_internal original_participant_receipt_prepare_shared_internal
#define ctrc_receipt_prepare_table_observed original_participant_receipt_prepare_table_observed
#define ctrc_receipt_prepare_with_sequence original_participant_receipt_prepare_with_sequence
#define ctrc_receipt_probe_hash original_participant_receipt_probe_hash
#define ctrc_receipt_probe_states original_participant_receipt_probe_states
#define ctrc_receipt_publication_matches original_participant_receipt_publication_matches
#define ctrc_relation_namespace_valid original_participant_relation_namespace_valid
#define ctrc_target_pending_itl_valid original_participant_target_pending_itl_valid
#define ctrc_target_pending_offnum_valid original_participant_target_pending_offnum_valid
#define ctrc_target_predecessor_version_valid original_participant_target_predecessor_version_valid
#define cluster_ctrc_page_version_order original_terminal_page_version_order
#define cluster_ctrc_pending_itl_target_recheck original_terminal_pending_itl_target_recheck
#define cluster_ctrc_receipt_apply_prepared original_terminal_receipt_apply_prepared
#define cluster_ctrc_receipt_apply_shared original_terminal_receipt_apply_shared
#define cluster_ctrc_receipt_cancel_prepared original_terminal_receipt_cancel_prepared
#define cluster_ctrc_receipt_cancel_shared original_terminal_receipt_cancel_shared
#define ctrc_reference_durability_covers original_terminal_reference_durability_covers
#define cluster_ctrc_itl_target_identity_matches original_terminal_itl_target_identity_matches
#define cluster_ctrc_receipt_discharge_itl original_terminal_receipt_discharge_itl
#define cluster_ctrc_receipt_discharge_itl_shared original_terminal_receipt_discharge_itl_shared
#define ctrc_bytes_nonzero original_terminal_bytes_nonzero
#define ctrc_receipt_handle_exact original_terminal_receipt_handle_exact
#define ctrc_target_exact_itl_valid original_terminal_target_exact_itl_valid
#define ctrc_target_exact_tid_valid original_terminal_target_exact_tid_valid
#define ctrc_target_itl_finalizes_exact original_terminal_target_itl_finalizes_exact
#define ctrc_target_itl_finalizes_prepared original_terminal_target_itl_finalizes_prepared
#define ctrc_target_offnum_finalizes_exact original_terminal_target_offnum_finalizes_exact
#define LWLockAcquire participant_publisher_lock_acquire
#define LWLockRelease participant_publisher_lock_release
#include "test_cluster_heap_ctrc_terminal.inc"
#undef LWLockRelease
#undef LWLockAcquire
#undef ctrc_target_predecessor_version_valid
#undef ctrc_target_pending_offnum_valid
#undef ctrc_target_pending_itl_valid
#undef ctrc_relation_namespace_valid
#undef ctrc_receipt_publication_matches
#undef ctrc_receipt_probe_states
#undef ctrc_receipt_probe_hash
#undef ctrc_receipt_prepare_with_sequence
#undef ctrc_receipt_prepare_table_observed
#undef ctrc_receipt_prepare_shared_internal
#undef ctrc_receipt_prepare_refused
#undef ctrc_receipt_prepare_locators_valid_locked
#undef ctrc_receipt_prepare_existing_locked
#undef ctrc_receipt_links
#undef ctrc_receipt_hash_bytes
#undef ctrc_receipt_handle_fill
#undef ctrc_receipt_entries
#undef ctrc_receipt_chain_exact_locked
#undef ctrc_publication_request_equal
#undef ctrc_publication_prepare_valid
#undef ctrc_participant_receipt_heads
#undef ctrc_participant_locks_release
#undef ctrc_participant_locks_acquire
#undef ctrc_participant_index
#undef ctrc_participant_identity_valid
#undef ctrc_origin_protects_local_receipt_locked
#undef ctrc_allocate_journal_sequence
#undef cluster_ctrc_receipt_prepare_shared_observed
#undef cluster_ctrc_participant_open
#undef cluster_ctrc_participant_index_compute
#undef cluster_ctrc_origin_record_touched
#undef cluster_ctrc_origin_has_exact_touch
#undef ctrc_target_offnum_finalizes_exact
#undef ctrc_target_itl_finalizes_prepared
#undef ctrc_target_itl_finalizes_exact
#undef ctrc_target_exact_tid_valid
#undef ctrc_target_exact_itl_valid
#undef ctrc_receipt_handle_exact
#undef ctrc_bytes_nonzero
#undef cluster_ctrc_receipt_cancel_shared
#undef cluster_ctrc_receipt_cancel_prepared
#undef cluster_ctrc_receipt_discharge_itl_shared
#undef cluster_ctrc_receipt_discharge_itl
#undef cluster_ctrc_itl_target_identity_matches
#undef ctrc_reference_durability_covers
#undef cluster_ctrc_receipt_apply_shared
#undef cluster_ctrc_receipt_apply_prepared
#undef cluster_ctrc_pending_itl_target_recheck
#undef cluster_ctrc_page_version_order
#undef cluster_undo_cleaner_wakeup
#undef cluster_ctrc_stat_bump
#undef cluster_ctrc_shmem_ready

static bool
census_receipt_cancel_retained(const ClusterCtrcReceiptHandle *handle)
{
	bool result;
	receipt_terminal_watched = handle;
	result = original_terminal_receipt_cancel_shared(handle);
	receipt_terminal_watched = NULL;
	return result;
}

static ClusterCtrcTargetV1
receipt_terminal_final_target(const ClusterCtrcReceiptHandle *handle)
{
	ClusterCtrcTargetV1 target = handle->receipt->target;
	target.kind = CTRC_TARGET_EXACT_ITL_SLOT;
	target.itl_slot_index = target.itl_slot_wrap = 1;
	target.itl_xid = handle->key.xid; target.itl_class = 1;
	target.uba[0] = 1; target.planned_predecessor_sha256[0] = 1; target.planned_successor_sha256[0] = 2;
	return target;
}

static bool
receipt_terminal_fixture(QualifiedXminFixture *f, CensusRuntimeAllocation *runtime,
						 ClusterHeapFreezeXminTargetV1 *targets, bool batch, ClusterCtrcReceiptHandle *handle)
{
	ClusterCtrcTxnKeyV1 key; ClusterCtrcOriginReservation reservation; uint32 grant;
	FullTransactionId full = FullTransactionIdFromEpochAndXid(1, 100);
	if (batch) qualified_xmin_batch_setup(f, targets);
	else qualified_xmin_setup(f, HEAP_XMAX_IS_MULTI);
	qualified_xmin_require_peer(f, 65); fixture_release_original_writer();
	census_runtime_initialize_requested = true; receipt_terminal_wakeups = 0;
	if (!census_full_layout_setup(runtime, 2)) return false;
	key = census_key_for_full(&f->cohort.admission_fence, full);
	UT_ASSERT_EQ(census_origin_reserve_from_backend(&key, full, &reservation), CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
	UT_ASSERT(original_census_origin_open_reserved(&reservation, &grant));
	UT_ASSERT_EQ(census_participant_prepare_from_origin(&key, grant, handle), CLUSTER_CTRC_PREPARE_READY);
	return true;
}

UT_TEST(test_census_original_receipt_cancel_keeps_participant_full_identity)
{
	QualifiedXminFixture f; CensusRuntimeAllocation runtime;
	ClusterCtrcTxnKeyV1 key; ClusterCtrcOriginReservation reservation; ClusterCtrcReceiptHandle handle;
	FullTransactionId full = FullTransactionIdFromEpochAndXid(1, 100); uint32 grant;
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); fixture_release_original_writer();
	census_runtime_initialize_requested = true;
	if (!census_full_layout_setup(&runtime, 2)) { UT_ASSERT(false); return; }
	key = census_key_for_full(&f.cohort.admission_fence, full);
	UT_ASSERT_EQ(census_origin_reserve_from_backend(&key, full, &reservation), CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
	UT_ASSERT(original_census_origin_open_reserved(&reservation, &grant));
	UT_ASSERT_EQ(census_participant_prepare_from_origin(&key, grant, &handle), CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT(census_receipt_cancel_retained(&handle));
	UT_ASSERT_EQ(handle.receipt->state, CTRC_RECEIPT_CANCELLED);
	UT_ASSERT_EQ(handle.participant->prepared_count, 0); UT_ASSERT_EQ(handle.participant->cancelled_count, 1);
	UT_ASSERT(FullTransactionIdEquals(handle.participant->original_full_xid, full));
	census_full_layout_release(&runtime);
}


UT_TEST(test_census_original_receipt_apply_is_idempotent_and_retains_full_identity)
{
	QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterCtrcReceiptHandle handle;
	ClusterCtrcTargetV1 final, changed; ClusterCtrcApplyToken token;
	ClusterCtrcParticipantEntry before; ClusterCtrcReceipt receipt_before;
	if (!receipt_terminal_fixture(&f, &runtime, NULL, false, &handle)) { UT_ASSERT(false); return; }
	final = receipt_terminal_final_target(&handle);
	UT_ASSERT_EQ(original_terminal_receipt_apply_shared(&handle, &final, &token), CLUSTER_CTRC_APPLY_APPLIED);
	UT_ASSERT(token.valid); UT_ASSERT_EQ(token.journal_slot_generation, handle.journal_slot_generation);
	UT_ASSERT_EQ(handle.receipt->state, CTRC_RECEIPT_APPLIED);
	UT_ASSERT_EQ(handle.participant->receipt_count, 1); UT_ASSERT_EQ(handle.participant->prepared_count, 0);
	UT_ASSERT_EQ(handle.participant->applied_count, 1); UT_ASSERT_EQ(handle.participant->cancelled_count, 0);
	UT_ASSERT(FullTransactionIdEquals(handle.participant->original_full_xid, handle.original_full_xid));
	before = *handle.participant; receipt_before = *handle.receipt;
	UT_ASSERT_EQ(original_terminal_receipt_apply_shared(&handle, &final, &token), CLUSTER_CTRC_APPLY_APPLIED);
	UT_ASSERT(token.valid); UT_ASSERT(!census_receipt_cancel_retained(&handle));
	changed = final; changed.itl_slot_wrap++;
	memset(&token, 0xa5, sizeof(token));
	UT_ASSERT_EQ(original_terminal_receipt_apply_shared(&handle, &changed, &token), CLUSTER_CTRC_APPLY_FAIL_CLOSED);
	UT_ASSERT(ctrc_bytes_zero(&token, sizeof(token)));
	UT_ASSERT(memcmp(&before, handle.participant, sizeof(before)) == 0);
	UT_ASSERT(memcmp(&receipt_before, handle.receipt, sizeof(receipt_before)) == 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->stats[CTRC_STAT_RECEIPT_APPLIED]), 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation), 1);
	UT_ASSERT_EQ(receipt_terminal_wakeups, 0); UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
	census_full_layout_release(&runtime);
}

UT_TEST(test_census_mixed_receipt_dispositions_keep_original_chain_and_protection)
{
	QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterCtrcReceiptHandle first, second;
	ClusterCtrcTargetV1 final; ClusterCtrcApplyToken token;
	if (!receipt_terminal_fixture(&f, &runtime, NULL, false, &first)) { UT_ASSERT(false); return; }
	participant_publication.operation_id++;
	UT_ASSERT_EQ(participant_publisher_prepare(&first.key, first.participant->grant_generation, &second), CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT(first.participant == second.participant && first.receipt != second.receipt);
	UT_ASSERT_EQ(first.participant->prepared_count, 2);
	final = receipt_terminal_final_target(&first);
	UT_ASSERT_EQ(original_terminal_receipt_apply_shared(&first, &final, &token), CLUSTER_CTRC_APPLY_APPLIED);
	UT_ASSERT(census_receipt_cancel_retained(&second)); UT_ASSERT(!census_receipt_cancel_retained(&second));
	UT_ASSERT_EQ(second.receipt->disposition, CTRC_RELEASE_CANCELLED_PREMUTATION);
	UT_ASSERT_EQ(first.participant->receipt_count, 2); UT_ASSERT_EQ(first.participant->prepared_count, 0);
	UT_ASSERT_EQ(first.participant->applied_count, 1); UT_ASSERT_EQ(first.participant->cancelled_count, 1);
	UT_ASSERT_EQ(first.participant->state, CTRC_PARTICIPANT_OPEN);
	UT_ASSERT(FullTransactionIdEquals(first.participant->original_full_xid, first.original_full_xid));
	original_participant_participant_locks_acquire(ctrc_participant_lock_mask(first.participant_index));
	participant_publisher_lock_acquire(&CtrcShared->receipt_lock, LW_EXCLUSIVE);
	UT_ASSERT(original_participant_receipt_chain_exact_locked(first.participant_index, false));
	UT_ASSERT_EQ(original_participant_participant_receipt_heads()[first.participant_index], second.receipt_index + 1);
	UT_ASSERT_EQ(original_participant_receipt_links()[second.receipt_index].next_plus_one, first.receipt_index + 1);
	participant_publisher_lock_release(&CtrcShared->receipt_lock);
	original_participant_participant_locks_release(ctrc_participant_lock_mask(first.participant_index));
	UT_ASSERT_EQ(receipt_terminal_wakeups, 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->stats[CTRC_STAT_RECEIPT_APPLIED]), 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->stats[CTRC_STAT_RECEIPT_CANCELLED]), 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation), 1);
	UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0); census_full_layout_release(&runtime);
}

UT_TEST(test_census_receipt_terminal_stale_handles_refuse_before_disposition_change)
{
	for (int fault = 0; fault < 12; fault++) {
		QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterCtrcReceiptHandle handle, bad;
		ClusterCtrcTargetV1 final; ClusterCtrcApplyToken token;
		ClusterCtrcParticipantEntry before; ClusterCtrcReceipt receipt_before;
		if (!receipt_terminal_fixture(&f, &runtime, NULL, false, &handle)) { UT_ASSERT(false); return; }
		bad = handle; final = receipt_terminal_final_target(&handle);
		switch (fault) {
			case 0: bad.valid = false; break;
			case 1: bad.reserved8[0] = 1; break;
			case 2: bad.participant_index = runtime.capacity.participant_key_entries; break;
			case 3: bad.receipt_index = runtime.capacity.receipt_entries; break;
			case 4: bad.participant = &ctrc_participant_entries()[1]; break;
			case 5: bad.receipt = &original_participant_receipt_entries()[(handle.receipt_index + 1) % runtime.capacity.receipt_entries]; break;
			case 6: bad.journal_slot_generation++; break;
			case 7: bad.key.slot_wrap++; break;
			case 8: bad.original_full_xid = FullTransactionIdFromEpochAndXid(2, 100); break;
			case 9: bad.original_full_xid = FullTransactionIdFromU64(2); break;
			case 10: handle.participant->original_full_xid = InvalidFullTransactionId; break;
			case 11: CtrcShared->magic ^= 1; break;
		}
		before = *handle.participant; receipt_before = *handle.receipt;
		UT_ASSERT(!census_receipt_cancel_retained(&bad));
		memset(&token, 0xa5, sizeof(token));
		UT_ASSERT_EQ(original_terminal_receipt_apply_shared(&bad, &final, &token), CLUSTER_CTRC_APPLY_FAIL_CLOSED);
		UT_ASSERT(ctrc_bytes_zero(&token, sizeof(token)));
		UT_ASSERT(memcmp(&before, handle.participant, sizeof(before)) == 0);
		UT_ASSERT(memcmp(&receipt_before, handle.receipt, sizeof(receipt_before)) == 0);
		UT_ASSERT_EQ(receipt_terminal_wakeups, 0); UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
		UT_ASSERT(!local_x && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
		census_full_layout_release(&runtime);
	}
}

UT_TEST(test_census_receipt_apply_invalid_final_targets_keep_pending_protection)
{
	for (int fault = 0; fault < 6; fault++) {
		QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterCtrcReceiptHandle handle;
		ClusterCtrcTargetV1 final; ClusterCtrcApplyToken token;
		ClusterCtrcParticipantEntry before; ClusterCtrcReceipt receipt_before;
		if (!receipt_terminal_fixture(&f, &runtime, NULL, false, &handle)) { UT_ASSERT(false); return; }
		final = receipt_terminal_final_target(&handle);
		if (fault == 1) final.itl_xid++;
		if (fault == 2) memset(final.uba, 0, sizeof(final.uba));
		if (fault == 3) memset(final.planned_predecessor_sha256, 0, sizeof(final.planned_predecessor_sha256));
		if (fault == 4) final.block_number++;
		if (fault == 5) final.rel_number++;
		before = *handle.participant; receipt_before = *handle.receipt; memset(&token, 0xa5, sizeof(token));
		UT_ASSERT_EQ(original_terminal_receipt_apply_shared(&handle, fault == 0 ? NULL : &final, &token),
			fault >= 4 ? CLUSTER_CTRC_APPLY_RETRY_REQUIRED : CLUSTER_CTRC_APPLY_FAIL_CLOSED);
		UT_ASSERT(ctrc_bytes_zero(&token, sizeof(token)));
		UT_ASSERT(memcmp(&before, handle.participant, sizeof(before)) == 0);
		UT_ASSERT(memcmp(&receipt_before, handle.receipt, sizeof(receipt_before)) == 0);
		UT_ASSERT_EQ(handle.participant->prepared_count, 1);
		UT_ASSERT_EQ(receipt_terminal_wakeups, 0); UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
		census_full_layout_release(&runtime);
	}
}

UT_TEST(test_census_terminal_receipts_remain_in_complete_walk_before_native_wal)
{
	for (int applied = 0; applied < 2; applied++) for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterHeapFreezeXminTargetV1 targets[3];
		ClusterCtrcReceiptHandle handle; ClusterCtrcTargetV1 final; ClusterCtrcApplyToken token;
		uint64 passes = 1, expected_passes, total;
		if (!receipt_terminal_fixture(&f, &runtime, targets, batch, &handle)) { UT_ASSERT(false); return; }
		if (applied) {
			final = receipt_terminal_final_target(&handle);
			UT_ASSERT_EQ(original_terminal_receipt_apply_shared(&handle, &final, &token), CLUSTER_CTRC_APPLY_APPLIED);
		} else UT_ASSERT(census_receipt_cancel_retained(&handle));
		UT_ASSERT_EQ(handle.receipt->state, applied ? CTRC_RECEIPT_APPLIED : CTRC_RECEIPT_CANCELLED);
		UT_ASSERT_EQ(handle.participant->receipt_count, 1); UT_ASSERT_EQ(handle.participant->prepared_count, 0);
		UT_ASSERT_EQ(handle.participant->state, CTRC_PARTICIPANT_OPEN);
		UT_ASSERT(FullTransactionIdEquals(handle.participant->original_full_xid, FullTransactionIdFromEpochAndXid(1, 100)));
		sampler_xids[0] = 152;
		pg_atomic_write_u64(&sampler_slots[0].cluster_top_full_xid_atomic, U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(1, 152)));
		total = runtime.capacity.origin_key_entries + runtime.capacity.participant_key_entries;
		expected_passes = (total + CLUSTER_CTRC_ACTIVE_SCAN_SLICE - 1) / CLUSTER_CTRC_ACTIVE_SCAN_SLICE;
		native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1; resume_allowed = true;
		worker_original_sample_pass_fixture(&f.cohort);
		while (pass_pending && passes <= expected_passes) { original_worker_sample_active_pass(); passes++; }
		UT_ASSERT_EQ(passes, expected_passes); UT_ASSERT(!pass_pending);
		UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
		UT_ASSERT_EQ(publisher_scan.next_index, total); UT_ASSERT_EQ(publisher_scan.unqualified_entries, 0);
		UT_ASSERT(FullTransactionIdEquals(publisher_scan.min_full, handle.original_full_xid));
		UT_ASSERT_EQ(publisher_scan.unqualified_registration_generation, 1);
		UT_ASSERT_EQ(census_step_lock_acquires, total); UT_ASSERT_EQ(census_step_lock_releases, total);
		f.cohort.local_registration_cut = publisher_scan;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(!local_x && !resumed_writer && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
		census_full_layout_release(&runtime);
	}
}

/* Certificate authentication, frozen-ACK hashing/durability and cleaner scheduling
 * are controlled inputs here. The unchanged consumer, codec, shared-pool census,
 * lock upgrade and destructive reclaim execute their production bodies. */
#include "port/pg_crc32c.h"
static int certificate_worker_id = -1;
static uint64 certificate_local_progress, certificate_table_visits;
static unsigned certificate_wakeups, certificate_denials, certificate_upgrades;
static const ClusterCtrcReceiptHandle *certificate_upgrade_handle;
static unsigned certificate_upgrade_fault;
static ClusterCtrcSealReplyResult certificate_last_result;
static uint16 certificate_last_reason;

static void
certificate_cleaner_wakeup(void)
{
	UT_ASSERT_EQ(participant_lock_depth, 0); worker_pass_assert_unlocked(); certificate_wakeups++;
}

static void
certificate_cleaner_reason(ClusterCtrcCleanerReason reason)
{
	UT_ASSERT_EQ(participant_lock_depth, 0); worker_pass_assert_unlocked();
	UT_ASSERT_EQ(reason, CTRC_CLEANER_REASON_BLOCKED); certificate_denials++;
}

static ClusterCtrcAckResult
certificate_unreached_empty_ack(ClusterCtrcParticipantEntry *participant,
	const ClusterCtrcDurability *durability, ClusterCtrcLocalReleaseAckV1 *ack)
{
	(void)participant; (void)durability; (void)ack;
	UT_ASSERT(false); /* This slice submits CERTIFICATE, never empty CLOSE. */
	return CLUSTER_CTRC_ACK_DENIED;
}

static void
certificate_pool_upgrade(void)
{
	UT_ASSERT(participant_lock_depth != 0);
	UT_ASSERT(participant_lock_stack[participant_lock_depth - 1] != &CtrcShared->receipt_lock);
	certificate_upgrades++;
	/* Controlled damage at the original unlock/relock seam, not a legal
	 * concurrent mutation while the selected participant shard is held. */
	if (certificate_upgrade_fault != 0) {
		const ClusterCtrcReceiptHandle *handle = certificate_upgrade_handle;
		UT_ASSERT(handle != NULL);
		if (certificate_upgrade_fault == 1) handle->receipt->state = CTRC_RECEIPT_CANCELLED;
		else original_participant_participant_receipt_heads()[handle->participant_index] = 0;
	}
}
#define CLUSTER_CTRC_TEST_POOL_UPGRADE() certificate_pool_upgrade()
#define CLUSTER_CTRC_TEST_TABLE_VISIT(kind) ((void)(kind), certificate_table_visits++)
#define ctrc_put_u16_le original_certificate_put_u16_le
#define ctrc_put_u32_le original_certificate_put_u32_le
#define ctrc_put_u64_le original_certificate_put_u64_le
#define ctrc_get_u32_le original_certificate_get_u32_le
#define ctrc_crc32c_exact original_certificate_crc32c_exact
#define ctrc_txn_key_encode original_certificate_txn_key_encode
#define ctrc_seal_suboperation_valid original_certificate_seal_suboperation_valid
#define ctrc_local_ack_shape_valid original_certificate_local_ack_shape_valid
#define cluster_ctrc_local_release_ack_encode original_certificate_local_release_ack_encode
#define ctrc_semantic_progress original_certificate_semantic_progress
#define ctrc_participant_ack_entries original_certificate_participant_ack_entries
#define ctrc_reclaim_work_find original_certificate_reclaim_work_find
#define ctrc_receipts_reclaim_validate original_certificate_receipts_reclaim_validate
#define ctrc_receipt_collapse_deleted_probe original_certificate_receipt_collapse_deleted_probe
#define ctrc_receipts_reclaim_shared_locked original_certificate_receipts_reclaim_shared_locked
#define ctrc_ack_bytes_exact original_certificate_ack_bytes_exact
#define ctrc_frozen_ack_summary_exact original_certificate_frozen_ack_summary_exact
#define cluster_ctrc_participant_request_apply original_certificate_participant_request_apply
#define ctrc_participant_certificate_batch_apply_shared original_certificate_participant_certificate_batch_apply_shared
#define cluster_ctrc_participant_certificate_batch_shared original_certificate_participant_certificate_batch_shared
#define cluster_ctrc_participant_close_or_tombstone original_certificate_participant_close_or_tombstone
#define cluster_ctrc_participant_close original_certificate_participant_close
#define ctrc_receipt_entries original_participant_receipt_entries
#define ctrc_receipt_probe_states original_participant_receipt_probe_states
#define ctrc_receipt_links original_participant_receipt_links
#define ctrc_participant_receipt_heads original_participant_participant_receipt_heads
#define ctrc_receipt_chain_exact_locked original_participant_receipt_chain_exact_locked
#define ctrc_participant_index original_participant_participant_index
#define ctrc_participant_locks_acquire original_participant_participant_locks_acquire
#define ctrc_participant_locks_release original_participant_participant_locks_release
#define ctrc_participant_identity_valid original_participant_participant_identity_valid
#define cluster_ctrc_participant_open original_participant_participant_open
#define cluster_ctrc_empty_sha256 certificate_empty_sha256
#define cluster_ctrc_shmem_ready original_census_shmem_ready
#define cluster_ctrc_stat_bump original_census_stat_bump
#define cluster_undo_cleaner_wakeup certificate_cleaner_wakeup
#define cluster_ctrc_cleaner_reason_set certificate_cleaner_reason
#define cluster_ctrc_participant_build_ack certificate_unreached_empty_ack
#define CtrcWorkerId certificate_worker_id
#define CtrcLocalProgress certificate_local_progress
#define LWLockAcquire participant_publisher_lock_acquire
#define LWLockRelease participant_publisher_lock_release
#include "test_cluster_heap_ctrc_certificate.inc"
#undef LWLockRelease
#undef LWLockAcquire
#undef CtrcLocalProgress
#undef CtrcWorkerId
#undef cluster_ctrc_participant_build_ack
#undef cluster_ctrc_cleaner_reason_set
#undef cluster_undo_cleaner_wakeup
#undef cluster_ctrc_stat_bump
#undef cluster_ctrc_shmem_ready
#undef cluster_ctrc_empty_sha256
#undef cluster_ctrc_participant_open
#undef ctrc_participant_identity_valid
#undef ctrc_participant_locks_release
#undef ctrc_participant_locks_acquire
#undef ctrc_participant_index
#undef ctrc_receipt_chain_exact_locked
#undef ctrc_participant_receipt_heads
#undef ctrc_receipt_links
#undef ctrc_receipt_probe_states
#undef ctrc_receipt_entries
#undef cluster_ctrc_participant_close
#undef cluster_ctrc_participant_close_or_tombstone
#undef cluster_ctrc_participant_certificate_batch_shared
#undef ctrc_participant_certificate_batch_apply_shared
#undef cluster_ctrc_participant_request_apply
#undef ctrc_frozen_ack_summary_exact
#undef ctrc_ack_bytes_exact
#undef ctrc_receipts_reclaim_shared_locked
#undef ctrc_receipt_collapse_deleted_probe
#undef ctrc_receipts_reclaim_validate
#undef ctrc_reclaim_work_find
#undef ctrc_participant_ack_entries
#undef ctrc_semantic_progress
#undef cluster_ctrc_local_release_ack_encode
#undef ctrc_local_ack_shape_valid
#undef ctrc_seal_suboperation_valid
#undef ctrc_txn_key_encode
#undef ctrc_crc32c_exact
#undef ctrc_get_u32_le
#undef ctrc_put_u64_le
#undef ctrc_put_u32_le
#undef ctrc_put_u16_le
#undef CLUSTER_CTRC_TEST_TABLE_VISIT
#undef CLUSTER_CTRC_TEST_POOL_UPGRADE

static ClusterCtrcCloseDispatch
certificate_dispatch(const ClusterCtrcReceiptHandle *handle)
{
	ClusterCtrcCloseDispatch dispatch;
	memset(&dispatch, 0, sizeof(dispatch)); dispatch.key = handle->key;
	dispatch.participant = handle->participant->identity;
	dispatch.grant_generation = handle->participant->grant_generation; dispatch.seal_generation = 17;
	dispatch.suboperation = CTRC_SEAL_CERTIFICATE_COMMITTED;
	return dispatch;
}

static bool
census_receipt_certificate_submit(const ClusterCtrcReceiptHandle *handle)
{
	ClusterCtrcCloseDispatch dispatch = certificate_dispatch(handle);
	UT_ASSERT(original_certificate_participant_certificate_batch_apply_shared(
		&dispatch, 1, &certificate_last_result, &certificate_last_reason));
	return certificate_last_result == CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED;
}

static void
certificate_observers_reset(void)
{
	certificate_table_visits = certificate_wakeups = certificate_denials = certificate_upgrades = 0;
	certificate_upgrade_handle = NULL; certificate_upgrade_fault = 0;
	certificate_last_result = CTRC_SEAL_REPLY_DENIED; certificate_last_reason = UINT16_MAX;
}

/* A deliberately controlled frozen-ACK input. It is shape/CRC checked by the
 * original codec; this fixture does not produce or authenticate its row digest,
 * durable ACK or the certificate. Only original CANCELLED receipts are used. */
static bool
certificate_frozen_input(const ClusterCtrcReceiptHandle *handle)
{
	ClusterCtrcParticipantEntry *row = handle->participant;
	ClusterCtrcLocalReleaseAckV1 *ack = &original_certificate_participant_ack_entries()[handle->participant_index];
	uint8 encoded[CLUSTER_CTRC_LOCAL_ACK_BYTES];
	if (!census_receipt_cancel_retained(handle)) return false;
	if (original_certificate_participant_close(row, &row->identity, row->grant_generation, 17)
		!= CLUSTER_CTRC_CLOSE_ACK_READY) return false;
	UT_ASSERT_EQ(row->cancelled_count, 1); UT_ASSERT_EQ(row->receipt_count, 1);
	memset(ack, 0, sizeof(*ack)); ack->transaction_key = row->key;
	ack->grant_generation = row->grant_generation; ack->result = CTRC_ACK_RELEASED;
	ack->flags = CTRC_ACK_FLAG_ALL_DURABLE; ack->seal_generation = row->seal_generation;
	ack->participant_node_id = row->identity.node_id; ack->dependency_entry_count = CLUSTER_SF_DEP_MAX_ORIGINS;
	ack->capability_record_generation = row->identity.capability_record_generation;
	ack->participant_boot_incarnation = row->identity.boot_incarnation;
	ack->formation_epoch = row->identity.formation_epoch; ack->admission_record_generation = row->identity.admission_record_generation;
	ack->first_key_sequence = ack->last_key_sequence = ack->total_receipt_count = ack->cancelled_count = ack->ack_frozen_count = 1;
	ack->minimum_journal_sequence = ack->maximum_journal_sequence = handle->receipt->publication.journal_sequence;
	ack->row_digest_sha256[0] = 0x42; /* Controlled digest, not an actual cleaner hash. */
	if (!original_certificate_local_release_ack_encode(ack, encoded)) return false;
	ack->crc32c = original_certificate_get_u32_le(encoded + 412);
	row->state = CTRC_PARTICIPANT_ACK_FROZEN; row->ack_frozen_count = 1;
	handle->receipt->state = CTRC_RECEIPT_ACK_FROZEN;
	certificate_observers_reset(); return true;
}

UT_TEST(test_census_premature_certificate_cannot_reclaim_retained_receipt)
{
	QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterCtrcReceiptHandle handle;
	ClusterCtrcParticipantEntry before; ClusterCtrcReceipt receipt_before;
	if (!receipt_terminal_fixture(&f, &runtime, NULL, false, &handle)) { UT_ASSERT(false); return; }
	UT_ASSERT(census_receipt_cancel_retained(&handle)); certificate_observers_reset();
	before = *handle.participant; receipt_before = *handle.receipt;
	UT_ASSERT(!census_receipt_certificate_submit(&handle));
	UT_ASSERT(memcmp(&before, handle.participant, sizeof(before)) == 0);
	UT_ASSERT(memcmp(&receipt_before, handle.receipt, sizeof(receipt_before)) == 0);
	UT_ASSERT(FullTransactionIdEquals(handle.participant->original_full_xid, handle.original_full_xid));
	census_full_layout_release(&runtime);
}

UT_TEST(test_census_certificate_reclaims_only_frozen_cancelled_receiver)
{
	QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterCtrcReceiptHandle handle;
	ClusterCtrcOriginEntry origin_before; uint64 origin_index;
	if (!receipt_terminal_fixture(&f, &runtime, NULL, false, &handle)) { UT_ASSERT(false); return; }
	UT_ASSERT(original_census_origin_index(&handle.key, &origin_index));
	origin_before = ctrc_origin_entries()[origin_index];
	UT_ASSERT(certificate_frozen_input(&handle));
	UT_ASSERT(census_receipt_certificate_submit(&handle));
	UT_ASSERT_EQ(certificate_last_reason, 0); UT_ASSERT_EQ(certificate_upgrades, 1);
	UT_ASSERT_EQ(certificate_table_visits, runtime.capacity.receipt_entries);
	UT_ASSERT(ctrc_bytes_zero(handle.participant, sizeof(*handle.participant)));
	UT_ASSERT(ctrc_bytes_zero(handle.receipt, sizeof(*handle.receipt)));
	UT_ASSERT(ctrc_bytes_zero(&original_participant_receipt_links()[handle.receipt_index], sizeof(CtrcReceiptLink)));
	UT_ASSERT_EQ(original_participant_participant_receipt_heads()[handle.participant_index], 0);
	UT_ASSERT(ctrc_bytes_zero(&original_certificate_participant_ack_entries()[handle.participant_index], sizeof(ClusterCtrcLocalReleaseAckV1)));
	UT_ASSERT_EQ(original_participant_receipt_probe_states()[handle.receipt_index], CTRC_RECEIPT_PROBE_EMPTY);
	UT_ASSERT(memcmp(&origin_before, &ctrc_origin_entries()[origin_index], sizeof(origin_before)) == 0);
	UT_ASSERT_EQ(certificate_wakeups, 1); UT_ASSERT_EQ(certificate_denials, 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->stats[CTRC_STAT_SEMANTIC_PROGRESS]), 1);
	UT_ASSERT_EQ(participant_lock_depth, 0); UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
	UT_ASSERT(!census_receipt_cancel_retained(&handle)); /* Reclaimed handle remains stale. */
	census_full_layout_release(&runtime);
}

UT_TEST(test_census_certificate_refuses_stale_summary_or_incomplete_chain)
{
	for (int fault = 0; fault < 14; fault++) {
		QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterCtrcReceiptHandle handle;
		ClusterCtrcParticipantEntry before; ClusterCtrcReceipt receipt_before;
		ClusterCtrcLocalReleaseAckV1 *ack, ack_before; ClusterCtrcCloseDispatch dispatch;
		ClusterCtrcSealReplyResult result; uint16 reason; uint64 other;
		if (!receipt_terminal_fixture(&f, &runtime, NULL, false, &handle)) { UT_ASSERT(false); return; }
		UT_ASSERT(certificate_frozen_input(&handle)); dispatch = certificate_dispatch(&handle);
		ack = &original_certificate_participant_ack_entries()[handle.participant_index];
		other = (handle.receipt_index + 1) % runtime.capacity.receipt_entries;
		switch (fault) {
			case 0: dispatch.participant.boot_incarnation++; break;
			case 1: dispatch.grant_generation++; break;
			case 2: dispatch.seal_generation++; break;
			case 3: ack->crc32c ^= 1; break;
			case 4: ack->grant_generation++; break;
			case 5: handle.participant->state = CTRC_PARTICIPANT_ACK_READY; break;
			case 6: handle.participant->receipt_count++; break;
			case 7: handle.participant->ack_frozen_count++; break;
			case 8: handle.receipt->state = CTRC_RECEIPT_CANCELLED; break;
			case 9: original_participant_participant_receipt_heads()[handle.participant_index] = 0; break;
			case 10: original_participant_receipt_links()[handle.receipt_index].journal_generation++; break;
			case 11: original_participant_receipt_probe_states()[handle.receipt_index] = CTRC_RECEIPT_PROBE_EMPTY; break;
			case 12: original_participant_receipt_entries()[other].state = CTRC_RECEIPT_PREPARED; break;
			case 13: original_participant_receipt_links()[other].journal_generation = 1; break;
		}
		before = *handle.participant; receipt_before = *handle.receipt; ack_before = *ack;
		UT_ASSERT(original_certificate_participant_certificate_batch_apply_shared(&dispatch, 1, &result, &reason));
		UT_ASSERT_EQ(result, CTRC_SEAL_REPLY_BLOCKED_RETAIN);
		UT_ASSERT_EQ(reason, fault < 6 ? CTRC_SEAL_REASON_IDENTITY : CTRC_SEAL_REASON_ACK_UNAVAILABLE);
		if (fault >= 6) before.state = CTRC_PARTICIPANT_BLOCKED;
		UT_ASSERT(memcmp(&before, handle.participant, sizeof(before)) == 0);
		UT_ASSERT(memcmp(&receipt_before, handle.receipt, sizeof(receipt_before)) == 0);
		UT_ASSERT(memcmp(&ack_before, ack, sizeof(ack_before)) == 0);
		UT_ASSERT(FullTransactionIdEquals(handle.participant->original_full_xid, handle.original_full_xid));
		UT_ASSERT_EQ(certificate_upgrades, 0); UT_ASSERT_EQ(certificate_wakeups, 0);
		UT_ASSERT_EQ(certificate_denials, 1); UT_ASSERT_EQ(participant_lock_depth, 0);
		UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0); census_full_layout_release(&runtime);
	}
}

UT_TEST(test_census_certificate_preflight_keeps_all_slots_on_bad_batch)
{
	QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterCtrcReceiptHandle handle;
	ClusterCtrcCloseDispatch dispatch[2]; ClusterCtrcSealReplyResult results[2];
	ClusterCtrcParticipantEntry before; ClusterCtrcReceipt receipt_before; unsigned locks;
	if (!receipt_terminal_fixture(&f, &runtime, NULL, false, &handle)) { UT_ASSERT(false); return; }
	UT_ASSERT(certificate_frozen_input(&handle)); dispatch[0] = dispatch[1] = certificate_dispatch(&handle);
	before = *handle.participant; receipt_before = *handle.receipt; locks = participant_lock_acquires;
	UT_ASSERT(!original_certificate_participant_certificate_batch_shared(dispatch, 2, results));
	UT_ASSERT(!original_certificate_participant_certificate_batch_shared(dispatch, 0, results));
	UT_ASSERT(!original_certificate_participant_certificate_batch_shared(dispatch, CLUSTER_CTRC_RECLAIM_BATCH_MAX + 1, results));
	UT_ASSERT(!original_certificate_participant_certificate_batch_shared(NULL, 1, results));
	UT_ASSERT(!original_certificate_participant_certificate_batch_shared(dispatch, 1, NULL));
	dispatch[0].suboperation = CTRC_SEAL_CLOSE_AND_CLEAN;
	UT_ASSERT(!original_certificate_participant_certificate_batch_shared(dispatch, 1, results));
	dispatch[0] = certificate_dispatch(&handle); dispatch[0].participant.node_id++;
	UT_ASSERT(!original_certificate_participant_certificate_batch_shared(dispatch, 1, results));
	UT_ASSERT_EQ(participant_lock_acquires, locks);
	UT_ASSERT(memcmp(&before, handle.participant, sizeof(before)) == 0);
	UT_ASSERT(memcmp(&receipt_before, handle.receipt, sizeof(receipt_before)) == 0);
	UT_ASSERT_EQ(certificate_table_visits, 0); UT_ASSERT_EQ(certificate_wakeups + certificate_denials, 0);
	census_full_layout_release(&runtime);
}

UT_TEST(test_census_certificate_upgrade_rechecks_before_first_delete)
{
	for (unsigned fault = 1; fault <= 2; fault++) {
		QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterCtrcReceiptHandle handle;
		ClusterCtrcLocalReleaseAckV1 before;
		if (!receipt_terminal_fixture(&f, &runtime, NULL, false, &handle)) { UT_ASSERT(false); return; }
		UT_ASSERT(certificate_frozen_input(&handle));
		before = original_certificate_participant_ack_entries()[handle.participant_index];
		certificate_upgrade_handle = &handle; certificate_upgrade_fault = fault;
		UT_ASSERT(!census_receipt_certificate_submit(&handle));
		UT_ASSERT_EQ(certificate_last_result, CTRC_SEAL_REPLY_BLOCKED_RETAIN);
		UT_ASSERT_EQ(certificate_last_reason, CTRC_SEAL_REASON_ACK_UNAVAILABLE);
		UT_ASSERT_EQ(certificate_upgrades, 1); UT_ASSERT_EQ(certificate_table_visits, runtime.capacity.receipt_entries);
		UT_ASSERT_EQ(handle.participant->state, CTRC_PARTICIPANT_BLOCKED);
		UT_ASSERT_EQ(handle.receipt->state, fault == 1 ? CTRC_RECEIPT_CANCELLED : CTRC_RECEIPT_ACK_FROZEN);
		UT_ASSERT_EQ(original_participant_receipt_probe_states()[handle.receipt_index], CTRC_RECEIPT_PROBE_OCCUPIED);
		UT_ASSERT(memcmp(&before, &original_certificate_participant_ack_entries()[handle.participant_index], sizeof(before)) == 0);
		UT_ASSERT(FullTransactionIdEquals(handle.participant->original_full_xid, handle.original_full_xid));
		UT_ASSERT_EQ(certificate_wakeups, 0); UT_ASSERT_EQ(certificate_denials, 1);
		UT_ASSERT_EQ(participant_lock_depth, 0); certificate_upgrade_handle = NULL; certificate_upgrade_fault = 0;
		census_full_layout_release(&runtime);
	}
}

UT_TEST(test_census_certificate_receiver_retirement_keeps_origin_through_native_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterCtrcReceiptHandle handle;
		ClusterHeapFreezeXminTargetV1 targets[3]; ClusterCtrcOriginEntry origin_before;
		uint64 origin_index, total, expected_passes, passes = 1;
		if (!receipt_terminal_fixture(&f, &runtime, targets, batch != 0, &handle)) { UT_ASSERT(false); return; }
		UT_ASSERT(original_census_origin_index(&handle.key, &origin_index)); origin_before = ctrc_origin_entries()[origin_index];
		UT_ASSERT(certificate_frozen_input(&handle)); UT_ASSERT(census_receipt_certificate_submit(&handle));
		UT_ASSERT(memcmp(&origin_before, &ctrc_origin_entries()[origin_index], sizeof(origin_before)) == 0);
		sampler_xids[0] = 152;
		pg_atomic_write_u64(&sampler_slots[0].cluster_top_full_xid_atomic, U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(1, 152)));
		total = runtime.capacity.origin_key_entries + runtime.capacity.participant_key_entries;
		expected_passes = (total + CLUSTER_CTRC_ACTIVE_SCAN_SLICE - 1) / CLUSTER_CTRC_ACTIVE_SCAN_SLICE;
		native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1; resume_allowed = true;
		worker_original_sample_pass_fixture(&f.cohort);
		while (pass_pending && passes <= expected_passes) { original_worker_sample_active_pass(); passes++; }
		UT_ASSERT_EQ(passes, expected_passes); UT_ASSERT(!pass_pending);
		UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_COMPLETE); UT_ASSERT_EQ(publisher_scan.next_index, total);
		UT_ASSERT_EQ(publisher_scan.unqualified_entries, 0);
		UT_ASSERT(FullTransactionIdEquals(publisher_scan.min_full, handle.original_full_xid));
		UT_ASSERT(ctrc_bytes_zero(handle.participant, sizeof(*handle.participant)));
		UT_ASSERT_EQ(census_step_lock_acquires, total); UT_ASSERT_EQ(census_step_lock_releases, total);
		f.cohort.local_registration_cut = publisher_scan;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT(!local_x && !resumed_writer && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT_EQ(participant_lock_depth, 0); census_full_layout_release(&runtime);
	}
}

/* Original ACK producer runs with real canonical row hashing and two fresh
 * snapshots. OS allocation, durability I/O and scheduler/barrier are leaves. */
#include "../../common/sha2_int.h"
#include "utils/memutils.h"
static unsigned ack_alloc_calls, ack_alloc_live, ack_alloc_fail_at;
static unsigned ack_capture_calls, ack_wakeups, ack_flushes, ack_barriers;
static uint64 ack_visits[4];
static unsigned ack_capture_fault, ack_barrier_fault;
static unsigned ack_capture_error_at;
static XLogRecPtr ack_local_flush = 1000, ack_origin_durable = 1000;
static const ClusterCtrcReceiptHandle *ack_watched;
static void *ack_allocations[17];

static void *
ack_alloc(Size bytes, int flags)
{
	void *memory;
	worker_pass_assert_unlocked(); UT_ASSERT_EQ(participant_lock_depth, 0);
	ack_alloc_calls++; if (ack_alloc_fail_at == ack_alloc_calls) return NULL;
	UT_ASSERT(ack_alloc_live < lengthof(ack_allocations));
	memory = (flags & MCXT_ALLOC_ZERO) != 0 ? calloc(1, bytes) : malloc(bytes);
	UT_ASSERT(memory != NULL); ack_allocations[ack_alloc_live++] = memory; return memory;
}

static void
ack_free(void *memory)
{
	unsigned i; worker_pass_assert_unlocked(); UT_ASSERT_EQ(participant_lock_depth, 0);
	for (i = 0; i < ack_alloc_live; i++) if (ack_allocations[i] == memory) break;
	UT_ASSERT(i < ack_alloc_live); free(memory);
	ack_allocations[i] = ack_allocations[--ack_alloc_live];
}

static XLogRecPtr
ack_get_flush(TimeLineID *timeline)
{
	worker_pass_assert_unlocked(); UT_ASSERT_EQ(participant_lock_depth, 0); UT_ASSERT(timeline == NULL);
	ack_capture_calls++;
	if (ack_capture_calls == ack_capture_error_at) ereport(ERROR, (errmsg("ACK capture ERROR fixture")));
	/* Injection occurs after the first actual receipt copy, before the
	 * second original range capture. It is not native concurrency evidence. */
	if (ack_capture_calls == 1 && ack_capture_fault == 1) ack_watched->receipt->disposition = CTRC_RELEASE_CLEANED_ABSENT;
	if (ack_capture_calls == 1 && ack_capture_fault == 2) ack_watched->participant->original_full_xid = InvalidFullTransactionId;
	return ack_local_flush;
}

static XLogRecPtr
ack_remote_durable(int origin)
{
	worker_pass_assert_unlocked(); UT_ASSERT_EQ(participant_lock_depth, 0);
	UT_ASSERT(origin >= 0 && origin < CLUSTER_SF_DEP_MAX_ORIGINS); return ack_origin_durable;
}

static void
ack_flush(XLogRecPtr lsn)
{
	worker_pass_assert_unlocked(); UT_ASSERT_EQ(participant_lock_depth, 0);
	UT_ASSERT(lsn != InvalidXLogRecPtr); ack_flushes++;
}

static void
ack_cleaner_reason(ClusterCtrcCleanerReason reason)
{
	worker_pass_assert_unlocked(); UT_ASSERT_EQ(participant_lock_depth, 0);
	UT_ASSERT(reason == CTRC_CLEANER_REASON_WAL_DURABILITY || reason == CTRC_CLEANER_REASON_PARTICIPANT_ACK
		|| reason == CTRC_CLEANER_REASON_NONE || reason == CTRC_CLEANER_REASON_BLOCKED);
}

static void
ack_cleaner_wakeup(void)
{
	worker_pass_assert_unlocked(); UT_ASSERT_EQ(participant_lock_depth, 0); ack_wakeups++;
}

static void
ack_test_barrier(ClusterCtrcTestBarrierPhase barrier)
{
	worker_pass_assert_unlocked(); UT_ASSERT_EQ(participant_lock_depth, 0);
	UT_ASSERT_EQ(barrier, CTRC_TEST_BARRIER_ACK_DURABLE); ack_barriers++;
	if (ack_barrier_fault == 1) ack_watched->receipt->publication.journal_sequence++;
	if (ack_barrier_fault == 2) original_certificate_participant_ack_entries()[ack_watched->participant_index].reserved[0] = 1;
}
#define CLUSTER_CTRC_TEST_TABLE_VISIT(kind) (ack_visits[(kind)]++)
#define ctrc_tlv_bytes original_ack_tlv_bytes
#define ctrc_tlv_u8 original_ack_tlv_u8
#define ctrc_tlv_u16 original_ack_tlv_u16
#define ctrc_tlv_u32 original_ack_tlv_u32
#define ctrc_tlv_u64 original_ack_tlv_u64
#define ctrc_publication_id_encode original_ack_publication_id_encode
#define ctrc_target_encode original_ack_target_encode
#define ctrc_cleaner_owns_key original_ack_cleaner_owns_key
#define ctrc_scan_limit original_ack_scan_limit
#define ctrc_scan_charge original_ack_scan_charge
#define ctrc_participant_scan_lock_mask original_ack_participant_scan_lock_mask
#define ctrc_receipt_snapshot_valid original_ack_receipt_snapshot_valid
#define ctrc_receipt_row_encode original_ack_receipt_row_encode
#define ctrc_receipt_key_sequence_cmp original_ack_receipt_key_sequence_cmp
#define ctrc_receipt_durability_covered original_ack_receipt_durability_covered
#define cluster_ctrc_participant_ack_from_snapshot original_ack_participant_ack_from_snapshot
#define ctrc_participant_find_ack_ready original_ack_participant_find_ack_ready
#define ctrc_participant_capture_durability original_ack_participant_capture_durability
#define ctrc_ack_batch_inputs original_ack_ack_batch_inputs
#define ctrc_ack_participant_exact original_ack_ack_participant_exact
#define ctrc_ack_block_if_same original_ack_ack_block_if_same
#define ctrc_ack_work_find original_ack_ack_work_find
#define ctrc_participant_copy_ack_batch original_ack_participant_copy_ack_batch
#define ctrc_participant_freeze_ack_batch_exact original_ack_participant_freeze_ack_batch_exact
#define ctrc_participant_freeze_ack_batch_shared original_ack_participant_freeze_ack_batch_shared
#define ctrc_put_u16_le original_certificate_put_u16_le
#define ctrc_put_u32_le original_certificate_put_u32_le
#define ctrc_put_u64_le original_certificate_put_u64_le
#define ctrc_get_u32_le original_certificate_get_u32_le
#define ctrc_txn_key_encode original_certificate_txn_key_encode
#define ctrc_participant_ack_entries original_certificate_participant_ack_entries
#define cluster_ctrc_local_release_ack_encode original_certificate_local_release_ack_encode
#define cluster_ctrc_participant_close original_certificate_participant_close
#define ctrc_receipt_entries original_participant_receipt_entries
#define ctrc_receipt_probe_states original_participant_receipt_probe_states
#define ctrc_receipt_links original_participant_receipt_links
#define ctrc_participant_receipt_heads original_participant_participant_receipt_heads
#define ctrc_receipt_chain_exact_locked original_participant_receipt_chain_exact_locked
#define ctrc_participant_index original_participant_participant_index
#define ctrc_participant_locks_acquire original_participant_participant_locks_acquire
#define ctrc_participant_locks_release original_participant_participant_locks_release
#define ctrc_participant_identity_valid original_participant_participant_identity_valid
#define ctrc_publication_request_equal original_participant_publication_request_equal
#define ctrc_target_pending_itl_valid original_participant_target_pending_itl_valid
#define ctrc_target_pending_offnum_valid original_participant_target_pending_offnum_valid
#define ctrc_target_exact_itl_valid original_terminal_target_exact_itl_valid
#define ctrc_target_exact_tid_valid original_terminal_target_exact_tid_valid
#define cluster_ctrc_empty_sha256 certificate_empty_sha256
#define cluster_ctrc_shmem_ready original_census_shmem_ready
#define cluster_ctrc_stat_bump original_census_stat_bump
#define cluster_undo_cleaner_wakeup ack_cleaner_wakeup
#define cluster_ctrc_cleaner_reason_set ack_cleaner_reason
#define LWLockAcquire participant_publisher_lock_acquire
#define LWLockRelease participant_publisher_lock_release
#define GetFlushRecPtr ack_get_flush
#define cluster_sf_observed_origin_durable_lsn ack_remote_durable
#define XLogFlush ack_flush
#define palloc_extended ack_alloc
#define pfree ack_free
#define cluster_ctrc_test_barrier_wait ack_test_barrier
#define InterruptPending ack_interrupt_pending
#define ProcessInterrupts ack_process_interrupts
static volatile sig_atomic_t ack_interrupt_pending;
static void ack_process_interrupts(void) { UT_ASSERT(false); }
#define ctrc_scan_participant_relock original_ack_scan_participant_relock
#include "test_cluster_heap_ctrc_g_scan_relock.inc"
#include "test_cluster_heap_ctrc_ack.inc"
#undef ctrc_scan_participant_relock
#undef ProcessInterrupts
#undef InterruptPending
#undef cluster_ctrc_test_barrier_wait
#undef pfree
#undef palloc_extended
#undef XLogFlush
#undef cluster_sf_observed_origin_durable_lsn
#undef GetFlushRecPtr
#undef LWLockRelease
#undef LWLockAcquire
#undef cluster_ctrc_cleaner_reason_set
#undef cluster_undo_cleaner_wakeup
#undef cluster_ctrc_stat_bump
#undef cluster_ctrc_shmem_ready
#undef cluster_ctrc_empty_sha256
#undef ctrc_target_exact_tid_valid
#undef ctrc_target_exact_itl_valid
#undef ctrc_target_pending_offnum_valid
#undef ctrc_target_pending_itl_valid
#undef ctrc_publication_request_equal
#undef ctrc_participant_identity_valid
#undef ctrc_participant_locks_release
#undef ctrc_participant_locks_acquire
#undef ctrc_participant_index
#undef ctrc_receipt_chain_exact_locked
#undef ctrc_participant_receipt_heads
#undef ctrc_receipt_links
#undef ctrc_receipt_probe_states
#undef ctrc_receipt_entries
#undef cluster_ctrc_participant_close
#undef cluster_ctrc_local_release_ack_encode
#undef ctrc_participant_ack_entries
#undef ctrc_txn_key_encode
#undef ctrc_get_u32_le
#undef ctrc_put_u64_le
#undef ctrc_put_u32_le
#undef ctrc_put_u16_le
#undef ctrc_participant_freeze_ack_batch_shared
#undef ctrc_participant_freeze_ack_batch_exact
#undef ctrc_participant_copy_ack_batch
#undef ctrc_ack_work_find
#undef ctrc_ack_block_if_same
#undef ctrc_ack_participant_exact
#undef ctrc_ack_batch_inputs
#undef ctrc_participant_capture_durability
#undef ctrc_participant_find_ack_ready
#undef cluster_ctrc_participant_ack_from_snapshot
#undef ctrc_receipt_durability_covered
#undef ctrc_receipt_key_sequence_cmp
#undef ctrc_receipt_row_encode
#undef ctrc_receipt_snapshot_valid
#undef ctrc_participant_scan_lock_mask
#undef ctrc_scan_charge
#undef ctrc_scan_limit
#undef ctrc_cleaner_owns_key
#undef ctrc_target_encode
#undef ctrc_publication_id_encode
#undef ctrc_tlv_u64
#undef ctrc_tlv_u32
#undef ctrc_tlv_u16
#undef ctrc_tlv_u8
#undef ctrc_tlv_bytes
#undef CLUSTER_CTRC_TEST_TABLE_VISIT

static void
ack_observers_reset(const ClusterCtrcReceiptHandle *handle)
{
	UT_ASSERT_EQ(ack_alloc_live, 0); ack_watched = handle;
	ack_alloc_calls = ack_alloc_fail_at = ack_capture_calls = ack_wakeups = ack_flushes = ack_barriers = 0;
	ack_capture_fault = ack_barrier_fault = 0; ack_capture_error_at = 0;
	ack_local_flush = ack_origin_durable = 1000; memset(ack_visits, 0, sizeof(ack_visits));
}

static bool
census_original_nonempty_ack_freeze(const ClusterCtrcReceiptHandle *handle)
{
	ack_watched = handle;
	return original_ack_participant_freeze_ack_batch_shared(1) == 1;
}

UT_TEST(test_census_original_nonempty_ack_snapshot_can_freeze_cancelled_receipt)
{
	QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterCtrcReceiptHandle handle;
	if (!receipt_terminal_fixture(&f, &runtime, NULL, false, &handle)) { UT_ASSERT(false); return; }
	UT_ASSERT(census_receipt_cancel_retained(&handle));
	UT_ASSERT_EQ(original_certificate_participant_close(handle.participant, &handle.participant->identity,
		handle.participant->grant_generation, 17), CLUSTER_CTRC_CLOSE_ACK_READY);
	ack_observers_reset(&handle);
	UT_ASSERT(census_original_nonempty_ack_freeze(&handle));
	UT_ASSERT_EQ(handle.participant->state, CTRC_PARTICIPANT_ACK_FROZEN);
	UT_ASSERT_EQ(handle.receipt->state, CTRC_RECEIPT_ACK_FROZEN);
	UT_ASSERT_EQ(ack_capture_calls, 2); UT_ASSERT_EQ(ack_barriers, 1); UT_ASSERT_EQ(ack_wakeups, 1);
	UT_ASSERT_EQ(ack_alloc_live, 0); UT_ASSERT_EQ(ack_alloc_calls, 2);
	UT_ASSERT_EQ(ack_visits[1], 2); UT_ASSERT_EQ(ack_visits[2], runtime.capacity.receipt_entries);
	UT_ASSERT_EQ(ack_visits[3], 1); UT_ASSERT_EQ(participant_lock_depth, 0);
	UT_ASSERT(original_certificate_frozen_ack_summary_exact(handle.participant,
		&original_certificate_participant_ack_entries()[handle.participant_index]));
	UT_ASSERT(FullTransactionIdEquals(handle.participant->original_full_xid, handle.original_full_xid));
	census_full_layout_release(&runtime);
}

static bool
ack_ready_fixture(QualifiedXminFixture *f, CensusRuntimeAllocation *runtime,
				  ClusterHeapFreezeXminTargetV1 *targets, bool batch, ClusterCtrcReceiptHandle *handle)
{
	if (!receipt_terminal_fixture(f, runtime, targets, batch, handle)) return false;
	if (!census_receipt_cancel_retained(handle)) return false;
	if (original_certificate_participant_close(handle->participant, &handle->participant->identity,
		handle->participant->grant_generation, 17) != CLUSTER_CTRC_CLOSE_ACK_READY) return false;
	ack_observers_reset(handle); return true;
}

UT_TEST(test_census_original_ack_hashes_full_sorted_receipt_chain_before_reclaim)
{
	QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterCtrcReceiptHandle first, second;
	ClusterCtrcParticipantEntry origin_participant; ClusterCtrcLocalReleaseAckV1 *ack;
	if (!receipt_terminal_fixture(&f, &runtime, NULL, false, &first)) { UT_ASSERT(false); return; }
	participant_publication.wire_request_id++; participant_publication.operation_id++;
	UT_ASSERT_EQ(participant_publisher_prepare(&first.key, first.participant->grant_generation, &second), CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT(first.receipt_index != second.receipt_index && first.participant_index == second.participant_index);
	UT_ASSERT(census_receipt_cancel_retained(&first)); UT_ASSERT(census_receipt_cancel_retained(&second));
	UT_ASSERT_EQ(original_certificate_participant_close(first.participant, &first.participant->identity,
		first.participant->grant_generation, 17), CLUSTER_CTRC_CLOSE_ACK_READY);
	origin_participant = *first.participant; ack_observers_reset(&first);
	UT_ASSERT(census_original_nonempty_ack_freeze(&first));
	ack = &original_certificate_participant_ack_entries()[first.participant_index];
	UT_ASSERT_EQ(ack->total_receipt_count, 2); UT_ASSERT_EQ(ack->cancelled_count, 2);
	UT_ASSERT_EQ(ack->first_key_sequence, 1); UT_ASSERT_EQ(ack->last_key_sequence, 2);
	UT_ASSERT_EQ(ack->minimum_journal_sequence, first.receipt->publication.journal_sequence);
	UT_ASSERT_EQ(ack->maximum_journal_sequence, second.receipt->publication.journal_sequence);
	UT_ASSERT(!ctrc_bytes_zero(ack->row_digest_sha256, sizeof(ack->row_digest_sha256)));
	UT_ASSERT(memcmp(ack->row_digest_sha256, certificate_empty_sha256, sizeof(ack->row_digest_sha256)) != 0);
	UT_ASSERT_EQ(ack_capture_calls, 2); UT_ASSERT_EQ(ack_visits[1], 4); UT_ASSERT_EQ(ack_visits[3], 2);
	UT_ASSERT(FullTransactionIdEquals(first.participant->original_full_xid, origin_participant.original_full_xid));
	certificate_observers_reset(); UT_ASSERT(census_receipt_certificate_submit(&first));
	UT_ASSERT(ctrc_bytes_zero(first.receipt, sizeof(*first.receipt)) && ctrc_bytes_zero(second.receipt, sizeof(*second.receipt)));
	UT_ASSERT_EQ(ack_alloc_live, 0); UT_ASSERT_EQ(participant_lock_depth, 0); census_full_layout_release(&runtime);
}

UT_TEST(test_census_original_ack_refuses_durability_or_two_capture_drift)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterCtrcReceiptHandle handle;
		ClusterCtrcOriginEntry origin_before; uint64 origin_index;
		if (!ack_ready_fixture(&f, &runtime, NULL, false, &handle)) { UT_ASSERT(false); return; }
		UT_ASSERT(original_census_origin_index(&handle.key, &origin_index)); origin_before = ctrc_origin_entries()[origin_index];
		/* Nonzero WAL/dependency values are controlled durability inputs;
		 * actual capture/coverage uses them, with no physical WAL assertion. */
		if (fault == 0) { handle.receipt->highest_local_wal_lsn = 200; ack_local_flush = 199; }
		if (fault == 1) { handle.receipt->required_lsn[1] = 300; ack_origin_durable = 299; }
		if (fault == 2) handle.receipt->disposition = CTRC_RELEASE_CLEANED_ABSENT;
		if (fault == 3) ack_capture_fault = 1;
		if (fault == 4) ack_capture_fault = 2;
		if (fault == 5) ack_barrier_fault = 1;
		if (fault == 6) ack_barrier_fault = 2;
		UT_ASSERT(!census_original_nonempty_ack_freeze(&handle));
		UT_ASSERT_EQ(handle.receipt->state, CTRC_RECEIPT_CANCELLED);
		UT_ASSERT_EQ(handle.participant->state, fault >= 4 ? CTRC_PARTICIPANT_BLOCKED : CTRC_PARTICIPANT_ACK_READY);
		UT_ASSERT_EQ(handle.participant->ack_frozen_count, 0); UT_ASSERT_EQ(ack_wakeups, 0);
		UT_ASSERT_EQ(ack_alloc_live, 0); UT_ASSERT_EQ(participant_lock_depth, 0);
		if (fault != 6) UT_ASSERT(ctrc_bytes_zero(&original_certificate_participant_ack_entries()[handle.participant_index], sizeof(ClusterCtrcLocalReleaseAckV1)));
		UT_ASSERT(memcmp(&origin_before, &ctrc_origin_entries()[origin_index], sizeof(origin_before)) == 0);
		UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0); census_full_layout_release(&runtime);
	}
}

UT_TEST(test_census_original_ack_allocation_refusal_retains_ready_receiver)
{
	for (unsigned fail = 1; fail <= 2; fail++) {
		QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterCtrcReceiptHandle handle;
		ClusterCtrcParticipantEntry before; ClusterCtrcReceipt receipt_before;
		if (!ack_ready_fixture(&f, &runtime, NULL, false, &handle)) { UT_ASSERT(false); return; }
		before = *handle.participant; receipt_before = *handle.receipt; ack_alloc_fail_at = fail;
		UT_ASSERT_EQ(original_ack_participant_freeze_ack_batch_shared(0), 0);
		UT_ASSERT_EQ(original_ack_participant_freeze_ack_batch_shared(CTRC_CLEANER_ACK_BATCH + 1), 0);
		UT_ASSERT_EQ(ack_alloc_calls, 0);
		UT_ASSERT(!census_original_nonempty_ack_freeze(&handle));
		UT_ASSERT(memcmp(&before, handle.participant, sizeof(before)) == 0);
		UT_ASSERT(memcmp(&receipt_before, handle.receipt, sizeof(receipt_before)) == 0);
		UT_ASSERT_EQ(ack_alloc_live, 0); UT_ASSERT_EQ(ack_capture_calls, 0); UT_ASSERT_EQ(ack_wakeups, 0);
		UT_ASSERT_EQ(participant_lock_depth, 0); census_full_layout_release(&runtime);
	}
}

UT_TEST(test_census_original_ack_error_unwinds_private_allocations_before_caller)
{
	for (unsigned capture = 1; capture <= 2; capture++) {
		QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterCtrcReceiptHandle handle;
		ClusterCtrcParticipantEntry before; ClusterCtrcReceipt receipt_before;
		if (!ack_ready_fixture(&f, &runtime, NULL, false, &handle)) { UT_ASSERT(false); return; }
		before = *handle.participant; receipt_before = *handle.receipt;
		ack_capture_error_at = capture; expecting_error = true;
		if (setjmp(error_jump) == 0) { (void)census_original_nonempty_ack_freeze(&handle); UT_ASSERT(false); }
		else {
			UT_ASSERT_EQ(ack_capture_calls, capture); UT_ASSERT_EQ(ack_alloc_live, 0);
			UT_ASSERT_EQ(participant_lock_depth, 0); UT_ASSERT(PG_exception_stack == NULL);
			UT_ASSERT(memcmp(&before, handle.participant, sizeof(before)) == 0);
			UT_ASSERT(memcmp(&receipt_before, handle.receipt, sizeof(receipt_before)) == 0);
			UT_ASSERT_EQ(ack_wakeups, 0); UT_ASSERT(!local_x && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
		}
		expecting_error = false; ack_capture_error_at = 0; census_full_layout_release(&runtime);
	}
}

UT_TEST(test_census_original_ack_then_certificate_keeps_origin_before_native_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterCtrcReceiptHandle handle;
		ClusterHeapFreezeXminTargetV1 targets[3]; ClusterCtrcOriginEntry origin_before;
		uint64 origin_index, total, expected_passes, passes = 1;
		if (!ack_ready_fixture(&f, &runtime, targets, batch != 0, &handle)) { UT_ASSERT(false); return; }
		UT_ASSERT(original_census_origin_index(&handle.key, &origin_index)); origin_before = ctrc_origin_entries()[origin_index];
		UT_ASSERT(census_original_nonempty_ack_freeze(&handle));
		certificate_observers_reset(); UT_ASSERT(census_receipt_certificate_submit(&handle));
		UT_ASSERT(memcmp(&origin_before, &ctrc_origin_entries()[origin_index], sizeof(origin_before)) == 0);
		sampler_xids[0] = 152;
		pg_atomic_write_u64(&sampler_slots[0].cluster_top_full_xid_atomic, U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(1, 152)));
		total = runtime.capacity.origin_key_entries + runtime.capacity.participant_key_entries;
		expected_passes = (total + CLUSTER_CTRC_ACTIVE_SCAN_SLICE - 1) / CLUSTER_CTRC_ACTIVE_SCAN_SLICE;
		native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1; resume_allowed = true;
		worker_original_sample_pass_fixture(&f.cohort);
		while (pass_pending && passes <= expected_passes) { original_worker_sample_active_pass(); passes++; }
		UT_ASSERT_EQ(passes, expected_passes); UT_ASSERT(!pass_pending);
		UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_COMPLETE); UT_ASSERT_EQ(publisher_scan.next_index, total);
		UT_ASSERT_EQ(publisher_scan.unqualified_entries, 0);
		UT_ASSERT(FullTransactionIdEquals(publisher_scan.min_full, handle.original_full_xid));
		f.cohort.local_registration_cut = publisher_scan;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1); UT_ASSERT_EQ(ack_capture_calls, 2);
		UT_ASSERT_EQ(ack_alloc_live, 0); UT_ASSERT_EQ(participant_lock_depth, 0);
		UT_ASSERT(!local_x && !resumed_writer && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
		census_full_layout_release(&runtime);
	}
}

/* Origin volatile lifetime edge only. The canonical block0 release-proof
 * publisher is an explicit controlled precondition of commit_shared below. */
static uint64 origin_reply_table_visits;
#define CLUSTER_CTRC_TEST_TABLE_VISIT(kind) ((void)(kind), origin_reply_table_visits++)
#define ctrc_origin_ack_entries original_origin_reply_origin_ack_entries
#define ctrc_origin_ack_index original_origin_reply_origin_ack_index
#define ctrc_origin_frozen_touch_set_valid original_origin_reply_origin_frozen_touch_set_valid
#define cluster_ctrc_origin_begin_seal_entry original_origin_reply_origin_begin_seal_entry
#define cluster_ctrc_origin_arm_close_entry original_origin_reply_origin_arm_close_entry
#define cluster_ctrc_origin_note_close_reply_entry original_origin_reply_origin_note_close_reply_entry
#define cluster_ctrc_origin_begin_cleaning_entry original_origin_reply_origin_begin_cleaning_entry
#define cluster_ctrc_origin_ack_land_entry original_origin_reply_origin_ack_land_entry
#define ctrc_origin_ack_land_locked original_origin_reply_origin_ack_land_locked
#define cluster_ctrc_origin_ack_land_shared original_origin_reply_origin_ack_land_shared
#define cluster_ctrc_origin_certificate_validate original_origin_reply_origin_certificate_validate
#define cluster_ctrc_origin_certificate_snapshot_entry original_origin_reply_origin_certificate_snapshot_entry
#define cluster_ctrc_origin_certificate_commit_entry original_origin_reply_origin_certificate_commit_entry
#define ctrc_origin_certificate_snapshot_index_locked original_origin_reply_origin_certificate_snapshot_index_locked
#define ctrc_origin_certificate_commit_shared original_origin_reply_origin_certificate_commit_shared
#define cluster_ctrc_origin_arm_certificate_entry original_origin_reply_origin_arm_certificate_entry
#define cluster_ctrc_origin_note_certificate_reply_entry original_origin_reply_origin_note_certificate_reply_entry
#define ctrc_origin_release_reclaim_locked original_origin_reply_origin_release_reclaim_locked
#define ctrc_origin_note_certificate_match_locked original_origin_reply_origin_note_certificate_match_locked
#define cluster_ctrc_origin_note_certificate_reply_shared original_origin_reply_origin_note_certificate_reply_shared
#define ctrc_origin_index original_census_origin_index
#define ctrc_participant_identity_valid original_participant_participant_identity_valid
#define ctrc_ack_bytes_exact original_certificate_ack_bytes_exact
#define cluster_ctrc_shmem_ready original_census_shmem_ready
#define ctrc_semantic_progress original_certificate_semantic_progress
#define LWLockAcquire participant_publisher_lock_acquire
#define LWLockRelease participant_publisher_lock_release
#include "test_cluster_heap_ctrc_origin_reply_trace_types.inc"
#if defined(CLUSTER_CTRC_ORIGIN_REPLY_TRACE_PRESENT) && defined(CLUSTER_CTRC_LOCAL_BATCH_REPLY_TRACE_PRESENT)
/* Execute the original diagnostic bodies with their default-OFF input.
 * An unexpected ON traversal fails at these explicit runtime/clock leaves. */
static bool original_origin_reply_trace_enabled;
static PGPROC *original_origin_reply_trace_proc;
static void
original_origin_reply_trace_locks(void (*callback)(LWLock *, LWLockMode, void *),
								void *context)
{
	(void)callback; (void)context; UT_ASSERT(false);
}
static TimestampTz
original_origin_reply_trace_clock(void)
{
	UT_ASSERT(false);
	return 0;
}
#define cluster_trace_normal_stop original_origin_reply_trace_enabled
#define MyProc original_origin_reply_trace_proc
#define ForEachLWLockHeldByMe original_origin_reply_trace_locks
#define GetCurrentTimestamp original_origin_reply_trace_clock
#endif
#define ctrc_origin_required_ack_snapshot_entry original_origin_reply_required_ack_snapshot_entry
#include "test_cluster_heap_ctrc_g_required_ack_decl.inc"
#include "test_cluster_heap_ctrc_origin_reply.inc"
#include "test_cluster_heap_ctrc_g_required_ack_body.inc"
#undef ctrc_origin_required_ack_snapshot_entry
#if defined(CLUSTER_CTRC_ORIGIN_REPLY_TRACE_PRESENT) && defined(CLUSTER_CTRC_LOCAL_BATCH_REPLY_TRACE_PRESENT)
#undef GetCurrentTimestamp
#undef ForEachLWLockHeldByMe
#undef MyProc
#undef cluster_trace_normal_stop
#endif
#undef CLUSTER_CTRC_LOCAL_BATCH_REPLY_TRACE_PRESENT
#undef CLUSTER_CTRC_ORIGIN_REPLY_TRACE_PRESENT
#undef LWLockRelease
#undef LWLockAcquire
#undef ctrc_semantic_progress
#undef cluster_ctrc_shmem_ready
#undef ctrc_ack_bytes_exact
#undef ctrc_participant_identity_valid
#undef ctrc_origin_index
#undef cluster_ctrc_origin_note_certificate_reply_shared
#undef ctrc_origin_note_certificate_match_locked
#undef ctrc_origin_release_reclaim_locked
#undef cluster_ctrc_origin_note_certificate_reply_entry
#undef cluster_ctrc_origin_arm_certificate_entry
#undef ctrc_origin_certificate_commit_shared
#undef ctrc_origin_certificate_snapshot_index_locked
#undef cluster_ctrc_origin_certificate_commit_entry
#undef cluster_ctrc_origin_certificate_snapshot_entry
#undef cluster_ctrc_origin_certificate_validate
#undef cluster_ctrc_origin_ack_land_shared
#undef ctrc_origin_ack_land_locked
#undef cluster_ctrc_origin_ack_land_entry
#undef cluster_ctrc_origin_begin_cleaning_entry
#undef cluster_ctrc_origin_note_close_reply_entry
#undef cluster_ctrc_origin_arm_close_entry
#undef cluster_ctrc_origin_begin_seal_entry
#undef ctrc_origin_frozen_touch_set_valid
#undef ctrc_origin_ack_index
#undef ctrc_origin_ack_entries
#undef CLUSTER_CTRC_TEST_TABLE_VISIT

static bool
census_origin_certificate_reply(uint64 request_id, uint16 participant)
{
	return original_origin_reply_origin_note_certificate_reply_shared(request_id, participant,
		CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED);
}

/* The real publisher/canonical durable block0 proof is not supplied by this
 * fixture. Only its already-canonical precondition is controlled here; every
 * volatile FSM, ACK inbox, snapshot identity and reclamation check is real. */
static bool
origin_reply_ready_fixture(QualifiedXminFixture *f, CensusRuntimeAllocation *runtime,
	ClusterHeapFreezeXminTargetV1 *targets, bool batch, ClusterCtrcReceiptHandle *handle,
	uint64 *origin_index, ClusterCtrcOriginCertificateSnapshot *snapshot)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcLocalReleaseAckV1 *ack;
	bool snapped;
	if (!ack_ready_fixture(f, runtime, targets, batch, handle)) return false;
	if (!census_original_nonempty_ack_freeze(handle) || !original_census_origin_index(&handle->key, origin_index)) return false;
	origin = &ctrc_origin_entries()[*origin_index];
	ack = &original_certificate_participant_ack_entries()[handle->participant_index];
	participant_publisher_lock_acquire(&CtrcShared->origin_lock, LW_EXCLUSIVE);
	UT_ASSERT(original_origin_reply_origin_begin_seal_entry(origin, 17));
	UT_ASSERT(original_origin_reply_origin_arm_close_entry(origin, ack->participant_node_id, 11));
	UT_ASSERT(original_origin_reply_origin_note_close_reply_entry(origin, ack->participant_node_id, 11, CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK));
	UT_ASSERT(original_origin_reply_origin_begin_cleaning_entry(origin));
	participant_publisher_lock_release(&CtrcShared->origin_lock);
	if (!original_origin_reply_origin_ack_land_shared(11, ack)) return false;
	UT_ASSERT_EQ(origin->state, CTRC_ORIGIN_CERTIFYING);
	participant_publisher_lock_acquire(&CtrcShared->origin_lock, LW_EXCLUSIVE);
	snapped = original_origin_reply_origin_certificate_snapshot_index_locked(*origin_index, snapshot);
	participant_publisher_lock_release(&CtrcShared->origin_lock);
	if (!snapped) return false;
	/* Controlled durable-publication boundary; this call does not write or
	 * prove canonical block0. Its original caller requires that beforehand. */
	if (!original_origin_reply_origin_certificate_commit_shared(snapshot)) return false;
	UT_ASSERT_EQ(origin->state, CTRC_ORIGIN_RELEASE_PROVEN);
	participant_publisher_lock_acquire(&CtrcShared->origin_lock, LW_EXCLUSIVE);
	UT_ASSERT(original_origin_reply_origin_arm_certificate_entry(origin, ack->participant_node_id, 123));
	participant_publisher_lock_release(&CtrcShared->origin_lock);
	UT_ASSERT(FullTransactionIdEquals(origin->original_full_xid, handle->original_full_xid));
	certificate_observers_reset(); origin_reply_table_visits = 0; return true;
}

static bool
origin_reply_reclaim_receiver(const ClusterCtrcReceiptHandle *handle)
{
	return census_receipt_certificate_submit(handle);
}

UT_TEST(test_census_origin_rejects_unarmed_certificate_reply_without_retiring)
{
	QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterCtrcReceiptHandle handle;
	ClusterCtrcOriginEntry before; uint64 index;
	if (!receipt_terminal_fixture(&f, &runtime, NULL, false, &handle)) { UT_ASSERT(false); return; }
	UT_ASSERT(original_census_origin_index(&handle.key, &index)); before = ctrc_origin_entries()[index];
	UT_ASSERT(!census_origin_certificate_reply(123, handle.participant->identity.node_id));
	UT_ASSERT(memcmp(&before, &ctrc_origin_entries()[index], sizeof(before)) == 0);
	UT_ASSERT(FullTransactionIdEquals(ctrc_origin_entries()[index].original_full_xid, handle.original_full_xid));
	census_full_layout_release(&runtime);
}

UT_TEST(test_census_original_certificate_reply_retires_origin_and_inbox_once)
{
	QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterCtrcReceiptHandle handle;
	ClusterCtrcOriginCertificateSnapshot snapshot; ClusterCtrcOriginEntry empty_origin = {0};
	ClusterCtrcLocalReleaseAckV1 empty_ack = {0}; uint64 index; uint16 node;
	if (!origin_reply_ready_fixture(&f, &runtime, NULL, false, &handle, &index, &snapshot)) { UT_ASSERT(false); return; }
	node = handle.participant->identity.node_id;
	UT_ASSERT(origin_reply_reclaim_receiver(&handle));
	UT_ASSERT(FullTransactionIdEquals(ctrc_origin_entries()[index].original_full_xid, handle.original_full_xid));
	UT_ASSERT(census_origin_certificate_reply(123, node));
	UT_ASSERT(memcmp(&ctrc_origin_entries()[index], &empty_origin, sizeof(empty_origin)) == 0);
	for (uint64 i = 0; i < CtrcShared->origin_ack_inbox_entries; i++)
		UT_ASSERT(memcmp(&original_origin_reply_origin_ack_entries()[i], &empty_ack, sizeof(empty_ack)) == 0);
	UT_ASSERT_EQ(origin_reply_table_visits, runtime.capacity.origin_key_entries);
	UT_ASSERT_EQ(participant_lock_depth, 0);
	UT_ASSERT(!census_origin_certificate_reply(123, node));
	UT_ASSERT(memcmp(&ctrc_origin_entries()[index], &empty_origin, sizeof(empty_origin)) == 0);
	census_full_layout_release(&runtime);
}

UT_TEST(test_census_original_origin_reply_rejects_stale_request_or_result)
{
	for (int fault = 0; fault < 7; fault++) {
		QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterCtrcReceiptHandle handle;
		ClusterCtrcOriginCertificateSnapshot snapshot; ClusterCtrcOriginEntry before;
		ClusterCtrcLocalReleaseAckV1 inbox_before[2]; uint64 index, ack_index, request = 123;
		uint16 node; ClusterCtrcSealReplyResult result = CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED;
		if (!origin_reply_ready_fixture(&f, &runtime, NULL, false, &handle, &index, &snapshot)) { UT_ASSERT(false); return; }
		node = handle.participant->identity.node_id;
		UT_ASSERT(original_origin_reply_origin_ack_index(&handle.key, 0, &ack_index));
		UT_ASSERT(origin_reply_reclaim_receiver(&handle));
		before = ctrc_origin_entries()[index];
		memcpy(inbox_before, &original_origin_reply_origin_ack_entries()[ack_index], sizeof(inbox_before));
		switch (fault) {
			case 0: request = 0; break;
			case 1: request++; break;
			case 2: node ^= 1; break;
			case 3: node = UINT16_MAX; break;
			case 4: result = CTRC_SEAL_REPLY_DENIED; break;
			case 5: result = CTRC_SEAL_REPLY_PENDING_DRAIN; break;
			case 6: result = CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK; break;
		}
		UT_ASSERT(!original_origin_reply_origin_note_certificate_reply_shared(request, node, result));
		UT_ASSERT(memcmp(&before, &ctrc_origin_entries()[index], sizeof(before)) == 0);
		UT_ASSERT(memcmp(inbox_before, &original_origin_reply_origin_ack_entries()[ack_index], sizeof(inbox_before)) == 0);
		UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0); UT_ASSERT_EQ(participant_lock_depth, 0);
		census_full_layout_release(&runtime);
	}
}

UT_TEST(test_census_original_origin_reclaim_rechecks_exact_ack_inbox)
{
	for (int fault = 0; fault < 8; fault++) {
		QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterCtrcReceiptHandle handle;
		ClusterCtrcOriginCertificateSnapshot snapshot; ClusterCtrcLocalReleaseAckV1 before[2];
		ClusterCtrcOriginEntry *origin; ClusterCtrcLocalReleaseAckV1 *inbox;
		uint64 index, base; uint16 node;
		if (!origin_reply_ready_fixture(&f, &runtime, NULL, false, &handle, &index, &snapshot)) { UT_ASSERT(false); return; }
		node = handle.participant->identity.node_id;
		UT_ASSERT(original_origin_reply_origin_ack_index(&handle.key, 0, &base));
		UT_ASSERT(origin_reply_reclaim_receiver(&handle));
		origin = &ctrc_origin_entries()[index]; inbox = &original_origin_reply_origin_ack_entries()[base];
		switch (fault) {
			case 0: inbox[node].crc32c ^= 1; break;
			case 1: inbox[node].reserved[0] = 1; break;
			case 2: inbox[node].grant_generation++; break;
			case 3: origin->grant_generation++; break;
			case 4: origin->touched_count++; break;
			case 5: origin->touched[node].boot_incarnation++; break;
			case 6: origin->ack_bitmap = 0; break;
			case 7: inbox[node ^ 1].result = CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK; break;
		}
		memcpy(before, inbox, sizeof(before));
		UT_ASSERT(!census_origin_certificate_reply(123, node));
		UT_ASSERT_EQ(origin->state, CTRC_ORIGIN_BLOCKED);
		UT_ASSERT(FullTransactionIdEquals(origin->original_full_xid, handle.original_full_xid));
		UT_ASSERT(memcmp(before, inbox, sizeof(before)) == 0);
		UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0); UT_ASSERT_EQ(participant_lock_depth, 0);
		census_full_layout_release(&runtime);
	}
}

UT_TEST(test_census_original_origin_duplicate_reply_id_retains_both_origins)
{
	QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterCtrcReceiptHandle handle;
	ClusterCtrcOriginCertificateSnapshot snapshot; ClusterCtrcOriginReservation reservation;
	ClusterCtrcTxnKeyV1 key; ClusterCtrcParticipantIdentity participant;
	FullTransactionId second_full = FullTransactionIdFromEpochAndXid(1, 101);
	uint64 index, second_index; uint32 grant, touched_grant; uint16 node;
	if (!origin_reply_ready_fixture(&f, &runtime, NULL, false, &handle, &index, &snapshot)) { UT_ASSERT(false); return; }
	participant = handle.participant->identity; node = participant.node_id;
	UT_ASSERT(origin_reply_reclaim_receiver(&handle));
	key = handle.key; key.slot_offset++; key.xid++;
	UT_ASSERT_EQ(census_origin_reserve_from_backend(&key, second_full, &reservation), CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
	UT_ASSERT(original_census_origin_open_reserved(&reservation, &grant));
	UT_ASSERT(original_census_origin_index(&key, &second_index)); UT_ASSERT(index != second_index);
	participant_publisher_lock_acquire(&CtrcShared->origin_lock, LW_EXCLUSIVE);
	UT_ASSERT_EQ(original_participant_origin_record_touched(&ctrc_origin_entries()[second_index], &participant,
		CTRC_PROOF_ACTIVE, &touched_grant), CLUSTER_CTRC_TOUCH_RECORDED);
	UT_ASSERT_EQ(touched_grant, grant);
	UT_ASSERT(original_origin_reply_origin_begin_seal_entry(&ctrc_origin_entries()[second_index], 17));
	UT_ASSERT(original_origin_reply_origin_arm_close_entry(&ctrc_origin_entries()[second_index], node, 123));
	participant_publisher_lock_release(&CtrcShared->origin_lock);
	UT_ASSERT(!census_origin_certificate_reply(123, node));
	UT_ASSERT_EQ(ctrc_origin_entries()[index].state, CTRC_ORIGIN_BLOCKED);
	UT_ASSERT_EQ(ctrc_origin_entries()[second_index].state, CTRC_ORIGIN_BLOCKED);
	UT_ASSERT(FullTransactionIdEquals(ctrc_origin_entries()[index].original_full_xid, handle.original_full_xid));
	UT_ASSERT(FullTransactionIdEquals(ctrc_origin_entries()[second_index].original_full_xid, second_full));
	UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0); UT_ASSERT_EQ(participant_lock_depth, 0);
	census_full_layout_release(&runtime);
}

UT_TEST(test_census_original_origin_retirement_advances_walk_and_rejects_old_candidate)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterCtrcReceiptHandle handle;
		ClusterHeapFreezeXminTargetV1 targets[3]; ClusterCtrcOriginCertificateSnapshot snapshot;
		uint64 index, total, expected_passes, passes = 1; uint16 node;
		if (!origin_reply_ready_fixture(&f, &runtime, targets, batch != 0, &handle, &index, &snapshot)) { UT_ASSERT(false); return; }
		node = handle.participant->identity.node_id;
		UT_ASSERT(origin_reply_reclaim_receiver(&handle)); UT_ASSERT(census_origin_certificate_reply(123, node));
		UT_ASSERT(ctrc_bytes_zero(&ctrc_origin_entries()[index], sizeof(ClusterCtrcOriginEntry)));
		sampler_xids[0] = 152;
		pg_atomic_write_u64(&sampler_slots[0].cluster_top_full_xid_atomic, U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(1, 152)));
		total = runtime.capacity.origin_key_entries + runtime.capacity.participant_key_entries;
		expected_passes = (total + CLUSTER_CTRC_ACTIVE_SCAN_SLICE - 1) / CLUSTER_CTRC_ACTIVE_SCAN_SLICE;
		/* A later publication gets a new ID; never reuse the old candidate's
		 * sample ID or replace its saved cut with this later observation. */
		native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id; resume_allowed = true;
		worker_original_sample_pass_fixture(&f.cohort);
		while (pass_pending && passes <= expected_passes) { original_worker_sample_active_pass(); passes++; }
		UT_ASSERT_EQ(passes, expected_passes); UT_ASSERT(!pass_pending);
		UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_COMPLETE); UT_ASSERT_EQ(publisher_scan.next_index, total);
		UT_ASSERT_EQ(publisher_scan.unqualified_entries, 0);
		UT_ASSERT(FullTransactionIdEquals(publisher_scan.min_full, FullTransactionIdFromEpochAndXid(1, 152)));
		UT_ASSERT_EQ(native_worker_shared.active_sample.sample_id, f.cohort.source_local_sample_id + 1);
		UT_ASSERT(memcmp(&f.cohort.local_registration_cut, &publisher_scan, sizeof(publisher_scan)) != 0);
		/* The new observation neither rebinds the saved old candidate nor
		 * grants a new freeze bound. Original current checks must reject it. */
		UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
		UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes, 0);
		UT_ASSERT_EQ(participant_lock_depth, 0); UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1); census_full_layout_release(&runtime);
	}
}

/* Original publisher control flow and record/identity helpers execute below.
 * Current-lock, physical I/O, admission and WAL insertion are explicit leaves;
 * in-memory readback here does not certify disk durability or live authority. */
#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/storage/cluster_undo_block0.h"
#include "cluster/storage/cluster_undo_block0_current.h"
#include "cluster/storage/cluster_undo_xlog.h"

typedef enum DurableCertificateFault {
	DURABLE_OK, DURABLE_ENTER_DENIED, DURABLE_ROOT_DENIED, DURABLE_ACQUIRE_FAILED,
	DURABLE_GENERATION_FAILED, DURABLE_GENERATION_UNKNOWN, DURABLE_GENERATION_CHANGED,
	DURABLE_PIN_FAILED, DURABLE_INITIAL_READ_FAILED, DURABLE_INITIAL_MISMATCH,
	DURABLE_BAD_HEADER, DURABLE_BAD_SLOT, DURABLE_ROOT_CHANGED, DURABLE_SHARED_CHANGED,
	DURABLE_WAL_INVALID, DURABLE_FINAL_READ_FAILED, DURABLE_FINAL_MISMATCH,
	DURABLE_FINAL_FLAG_MISSING, DURABLE_PIN_GENERATION_CHANGED,
	DURABLE_FINAL_ADMISSION_DENIED, DURABLE_FINAL_CURRENT_DENIED,
	DURABLE_RELEASE_FAILED, DURABLE_COMMIT_CHANGED,
	DURABLE_ERROR_ACQUIRE, DURABLE_ERROR_FLUSH, DURABLE_ERROR_READBACK, DURABLE_ERROR_RELEASE
} DurableCertificateFault;
static DurableCertificateFault durable_fault;
static const ClusterCtrcOriginCertificateSnapshot *durable_snapshot;
static PGAlignedBlock durable_disk, durable_resident;
static xl_undo_tt_slot_ctrc_release_v1 durable_record;
static bool durable_admitted, durable_current, durable_pinned;
static unsigned durable_enters, durable_leaves, durable_roots, durable_reads;
static unsigned durable_wals, durable_flushes, durable_rechecks, durable_admission_checks;
static unsigned durable_commits, durable_cancels, durable_wakeups;
static char durable_events[80]; static unsigned durable_event_count;
static int durable_node, durable_timeout = 50;
static volatile sig_atomic_t durable_interrupt_pending;
static void durable_event(char c) {
	UT_ASSERT(durable_event_count + 1 < sizeof(durable_events));
	durable_events[durable_event_count++] = c; durable_events[durable_event_count] = 0;
}
static void durable_process_interrupts(void) { UT_ASSERT(false); }
static uint64 durable_system_id(void) { return durable_snapshot->origin.key.system_identifier; }
static uint64 durable_epoch(void) { return durable_snapshot->origin.key.cluster_epoch; }
static uint64 durable_incarnation(void) { return durable_snapshot->origin.key.origin_boot_incarnation; }
static void durable_reason(ClusterCtrcCleanerReason reason) { (void)reason; }
static void durable_barrier(ClusterCtrcTestBarrierPhase phase) { UT_ASSERT_EQ(phase, CTRC_TEST_BARRIER_CERTIFICATE_READY); }
static void durable_wakeup(void) {
	UT_ASSERT(!durable_admitted && !durable_current && !durable_pinned); durable_wakeups++;
}
static ClusterSemanticAdmissionResult
durable_enter(ClusterSemanticAdmissionToken *admission)
{
	durable_enters++;
	if (durable_fault == DURABLE_ENTER_DENIED) return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	UT_ASSERT(!durable_admitted && !durable_current && !durable_pinned);
	admission->entered = true; admission->record_generation = durable_snapshot->origin.key.admission_record_generation;
	admission->formation_epoch = durable_snapshot->origin.key.formation_epoch;
	durable_admitted = true; durable_event('E'); return CLUSTER_SEMANTIC_ADMISSION_OK;
}
static void durable_leave(ClusterSemanticAdmissionToken *admission) {
	UT_ASSERT(admission->entered && durable_admitted && !durable_current && !durable_pinned);
	durable_leaves++; durable_admitted = false; admission->entered = false; durable_event('L');
	if (durable_fault == DURABLE_COMMIT_CHANGED) ctrc_origin_entries()[durable_snapshot->origin_index].grant_generation++;
}
static bool
durable_resolve(const ClusterSemanticAdmissionToken *admission, ClusterUndoPathIntent intent,
	uint32 owner, uint32 segment, ClusterUndoBlock0ResolvedRoot *root)
{
	UT_ASSERT(admission->entered && durable_admitted); UT_ASSERT_EQ(intent, CLUSTER_UNDO_PATH_RUNTIME_SHARED);
	UT_ASSERT_EQ(owner, durable_snapshot->origin.key.owner_instance); UT_ASSERT_EQ(segment, durable_snapshot->origin.key.segment_id);
	durable_roots++;
	if (durable_fault == DURABLE_ROOT_DENIED) return false;
	root->intent = intent; root->root_id = durable_snapshot->origin.key.root_id;
	root->root_generation = durable_snapshot->origin.key.root_generation;
	if (durable_roots == 2 && durable_fault == DURABLE_ROOT_CHANGED) root->root_generation++;
	if (durable_roots == 2 && durable_fault == DURABLE_SHARED_CHANGED) ctrc_origin_entries()[durable_snapshot->origin_index].grant_generation++;
	return true;
}
static ClusterUndoBlock0CurrentStep
durable_acquire(const ClusterUndoBlock0LogicalKey *key, int timeout,
	const ClusterSemanticAdmissionToken *admission, ClusterUndoBlock0CurrentGuard *guard, ClusterUndoBlock0Result *failure)
{
	(void)guard; (void)failure;
	UT_ASSERT(admission->entered && durable_admitted && !durable_current); UT_ASSERT_EQ(timeout, durable_timeout);
	UT_ASSERT_EQ(key->segment_id, durable_snapshot->origin.key.segment_id); UT_ASSERT_EQ(key->owner_instance, durable_snapshot->origin.key.owner_instance);
	if (durable_fault == DURABLE_ACQUIRE_FAILED) return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
	durable_current = true; durable_event('A');
	return durable_fault == DURABLE_ERROR_ACQUIRE ? CLUSTER_UNDO_BLOCK0_CURRENT_PENDING : CLUSTER_UNDO_BLOCK0_CURRENT_HELD;
}
static ClusterUndoBlock0CurrentStep
durable_acquire_poll(ClusterUndoBlock0CurrentGuard *guard, ClusterUndoBlock0Result *failure)
{
	(void)guard; (void)failure; UT_ASSERT(durable_current && durable_fault == DURABLE_ERROR_ACQUIRE);
	elog(ERROR, "injected original certificate acquire poll ERROR"); return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
}
static bool durable_wait(ClusterUndoBlock0CurrentGuard *guard, ClusterUndoBlock0ReplyWaitSite site) {
	(void)guard; (void)site; UT_ASSERT(false); return true;
}
static void durable_pause(long microseconds) { (void)microseconds; UT_ASSERT(false); }
static ClusterUndoBlock0Result
durable_generation(ClusterUndoBlock0CurrentGuard *guard, const ClusterUndoBlock0ResolvedRoot *root, ClusterUndoBlock0Generation *generation)
{
	(void)guard; (void)root; UT_ASSERT(durable_current && durable_admitted && !durable_pinned);
	if (durable_fault == DURABLE_GENERATION_FAILED) return CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH;
	generation->known = durable_fault != DURABLE_GENERATION_UNKNOWN;
	generation->value = durable_snapshot->origin.key.segment_generation + (durable_fault == DURABLE_GENERATION_CHANGED);
	return CLUSTER_UNDO_BLOCK0_OK;
}
static ClusterUndoBlock0Result
durable_pin(ClusterUndoBlock0CurrentGuard *guard, const ClusterUndoBlock0ResolvedRoot *root,
	const ClusterUndoBlock0Generation *generation, ClusterUndoBlock0Pin *pin, char **page)
{
	(void)guard; (void)root; UT_ASSERT(durable_current && durable_admitted && !durable_pinned);
	if (durable_fault == DURABLE_PIN_FAILED) return CLUSTER_UNDO_BLOCK0_NOT_FOUND;
	pin->slot = 0; pin->mode = CLUSTER_UNDO_BLOCK0_EXCLUSIVE; pin->observed_generation = *generation;
	*page = durable_resident.data; durable_pinned = true; durable_event('P'); return CLUSTER_UNDO_BLOCK0_OK;
}
static bool
durable_read(ClusterUndoPathIntent intent, uint32 segment, uint8 owner, uint32 block, char *out)
{
	UT_ASSERT_EQ(intent, CLUSTER_UNDO_PATH_RUNTIME_SHARED); UT_ASSERT_EQ(block, 0);
	UT_ASSERT_EQ(segment, durable_snapshot->origin.key.segment_id); UT_ASSERT_EQ(owner, durable_snapshot->origin.key.owner_instance);
	UT_ASSERT(durable_pinned && durable_current && durable_admitted); durable_reads++;
	if ((durable_reads == 1 && durable_fault == DURABLE_INITIAL_READ_FAILED)
		|| (durable_reads == 2 && durable_fault == DURABLE_FINAL_READ_FAILED)) return false;
	if (durable_reads == 2 && durable_fault == DURABLE_ERROR_READBACK) elog(ERROR, "injected certificate readback ERROR");
	if (durable_reads == 1 && (durable_fault == DURABLE_BAD_HEADER || durable_fault == DURABLE_BAD_SLOT)) {
		UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)durable_disk.data;
		if (durable_fault == DURABLE_BAD_HEADER) header->segment_id++;
		else header->tt_slots[durable_snapshot->origin.key.slot_offset].xid++;
		memcpy(durable_resident.data, durable_disk.data, BLCKSZ);
	}
	if (durable_reads == 2 && durable_fault == DURABLE_FINAL_FLAG_MISSING) {
		((UndoSegmentHeaderData *)durable_disk.data)->tt_slots[durable_snapshot->origin.key.slot_offset].flags = 0;
		memcpy(durable_resident.data, durable_disk.data, BLCKSZ);
	}
	memcpy(out, durable_disk.data, BLCKSZ);
	if ((durable_reads == 1 && durable_fault == DURABLE_INITIAL_MISMATCH)
		|| (durable_reads == 2 && durable_fault == DURABLE_FINAL_MISMATCH)) out[BLCKSZ - 1] ^= 1;
	return true;
}
static bool durable_admission_current(const ClusterSemanticAdmissionToken *admission) {
	UT_ASSERT(admission->entered && durable_admitted); durable_admission_checks++;
	return !(durable_admission_checks == 2 && durable_fault == DURABLE_FINAL_ADMISSION_DENIED);
}
static ClusterUndoBlock0Result durable_current_recheck(ClusterUndoBlock0CurrentGuard *guard) {
	(void)guard; UT_ASSERT(durable_current && durable_pinned); durable_rechecks++;
	return durable_rechecks == 2 && durable_fault == DURABLE_FINAL_CURRENT_DENIED ? CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED : CLUSTER_UNDO_BLOCK0_OK;
}
static XLogRecPtr durable_insert(const xl_undo_tt_slot_ctrc_release_v1 *record) {
	UT_ASSERT(durable_admitted && durable_current && durable_pinned); durable_wals++;
	UT_ASSERT_EQ(ctrc_origin_entries()[durable_snapshot->origin_index].state, CTRC_ORIGIN_CERTIFYING);
	durable_record = *record; durable_event('W');
	return durable_fault == DURABLE_WAL_INVALID ? InvalidXLogRecPtr : UINT64_C(0x123450);
}
static void durable_flush(ClusterUndoBlock0Pin *pin, const char *successor, XLogRecPtr lsn, bool metadata_only) {
	UT_ASSERT(durable_admitted && durable_current && durable_pinned); UT_ASSERT(!metadata_only); UT_ASSERT_EQ(lsn, UINT64_C(0x123450));
	durable_flushes++; durable_event('F');
	if (durable_fault == DURABLE_ERROR_FLUSH) elog(ERROR, "injected certificate flush ERROR");
	memcpy(durable_disk.data, successor, BLCKSZ); memcpy(durable_resident.data, successor, BLCKSZ);
	if (durable_fault == DURABLE_PIN_GENERATION_CHANGED) pin->observed_generation.value++;
}
static void durable_unpin(ClusterUndoBlock0Pin *pin) {
	UT_ASSERT(pin->slot == 0 && durable_pinned && durable_current && durable_admitted);
	durable_pinned = false; durable_event('U'); pin->slot = -1;
}
static ClusterUndoBlock0CurrentStep
durable_release(ClusterUndoBlock0CurrentGuard *guard, ClusterUndoBlock0Result *failure) {
	(void)guard; (void)failure; UT_ASSERT(!durable_pinned && durable_current && durable_admitted);
	if (durable_fault == DURABLE_ERROR_RELEASE) return CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
	durable_current = false; durable_event('R');
	return durable_fault == DURABLE_RELEASE_FAILED ? CLUSTER_UNDO_BLOCK0_CURRENT_FAILED : CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED;
}
static ClusterUndoBlock0CurrentStep
durable_release_poll(ClusterUndoBlock0CurrentGuard *guard, ClusterUndoBlock0Result *failure) {
	(void)guard; (void)failure; UT_ASSERT(durable_current && !durable_pinned && durable_fault == DURABLE_ERROR_RELEASE);
	elog(ERROR, "injected certificate release poll ERROR"); return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
}
static void durable_cancel(ClusterUndoBlock0CurrentGuard *guard) {
	(void)guard; UT_ASSERT(durable_current && !durable_pinned && durable_admitted);
	durable_cancels++; durable_current = false; durable_event('X');
}
static bool durable_commit(const ClusterCtrcOriginCertificateSnapshot *snapshot) {
	UT_ASSERT(!durable_current && !durable_pinned && !durable_admitted && durable_events[durable_event_count - 1] == 'L');
	durable_commits++; durable_event('C'); return original_origin_reply_origin_certificate_commit_shared(snapshot);
}
#define cluster_ctrc_origin_certificate_digest original_durable_certificate_digest
#define ctrc_origin_certificate_snapshot_matches_shared original_durable_snapshot_matches
#define ctrc_cleaner_publish_certificate original_durable_publish
#define cluster_undo_block0_root_valid original_durable_root_valid
#define cluster_undo_block0_root_matches original_durable_root_matches
#define cluster_undo_block0_generation_matches original_durable_generation_matches
#define cluster_ctrc_origin_certificate_validate original_origin_reply_origin_certificate_validate
#define cluster_ctrc_local_release_ack_encode original_certificate_local_release_ack_encode
#define ctrc_put_u16_le original_certificate_put_u16_le
#define ctrc_put_u32_le original_certificate_put_u32_le
#define cluster_ctrc_shmem_ready original_census_shmem_ready
#define ctrc_origin_certificate_snapshot_index_locked original_origin_reply_origin_certificate_snapshot_index_locked
#define ctrc_origin_certificate_commit_shared durable_commit
#define cluster_ctrc_stat_bump original_census_stat_bump
#define LWLockAcquire participant_publisher_lock_acquire
#define LWLockRelease participant_publisher_lock_release
#define cluster_node_id durable_node
#define cluster_ges_request_timeout_ms durable_timeout
#define GetSystemIdentifier durable_system_id
#define cluster_epoch_get_current durable_epoch
#define cluster_qvotec_get_self_incarnation durable_incarnation
#define cluster_ctrc_cleaner_reason_set durable_reason
#define cluster_ctrc_test_barrier_wait durable_barrier
#define cluster_undo_cleaner_wakeup durable_wakeup
#define cluster_semantic_activation_enter_r4_terminal_census durable_enter
#define cluster_semantic_activation_leave durable_leave
#define cluster_semantic_activation_resolve_shared_undo_root_r4_terminal_census durable_resolve
#define cluster_semantic_activation_recheck_r4_terminal_census durable_admission_current
#define cluster_undo_block0_current_acquire_begin_ctrc_release durable_acquire
#define cluster_undo_block0_current_acquire_poll durable_acquire_poll
#define cluster_undo_block0_current_wait_reply durable_wait
#define cluster_undo_block0_current_sample_generation_exclusive durable_generation
#define cluster_undo_block0_current_pin_exclusive durable_pin
#define cluster_undo_block0_current_recheck_exclusive durable_current_recheck
#define cluster_undo_block0_current_release_begin durable_release
#define cluster_undo_block0_current_release_poll durable_release_poll
#define cluster_undo_block0_current_cancel durable_cancel
#define cluster_undo_block0_unpin durable_unpin
#define cluster_undo_smgr_read_block durable_read
#define cluster_undo_xlog_insert_tt_ctrc_release durable_insert
#define cluster_undo_block0_flush_sync durable_flush
#define pg_usleep durable_pause
#define InterruptPending durable_interrupt_pending
#define ProcessInterrupts durable_process_interrupts
#include "test_cluster_heap_ctrc_durable_certificate.inc"
#undef ProcessInterrupts
#undef InterruptPending
#undef pg_usleep
#undef cluster_undo_block0_flush_sync
#undef cluster_undo_xlog_insert_tt_ctrc_release
#undef cluster_undo_smgr_read_block
#undef cluster_undo_block0_unpin
#undef cluster_undo_block0_current_cancel
#undef cluster_undo_block0_current_release_poll
#undef cluster_undo_block0_current_release_begin
#undef cluster_undo_block0_current_recheck_exclusive
#undef cluster_undo_block0_current_pin_exclusive
#undef cluster_undo_block0_current_sample_generation_exclusive
#undef cluster_undo_block0_current_wait_reply
#undef cluster_undo_block0_current_acquire_poll
#undef cluster_undo_block0_current_acquire_begin_ctrc_release
#undef cluster_semantic_activation_recheck_r4_terminal_census
#undef cluster_semantic_activation_resolve_shared_undo_root_r4_terminal_census
#undef cluster_semantic_activation_leave
#undef cluster_semantic_activation_enter_r4_terminal_census
#undef cluster_undo_cleaner_wakeup
#undef cluster_ctrc_test_barrier_wait
#undef cluster_ctrc_cleaner_reason_set
#undef cluster_qvotec_get_self_incarnation
#undef cluster_epoch_get_current
#undef GetSystemIdentifier
#undef cluster_ges_request_timeout_ms
#undef cluster_node_id
#undef LWLockRelease
#undef LWLockAcquire
#undef cluster_ctrc_stat_bump
#undef ctrc_origin_certificate_commit_shared
#undef ctrc_origin_certificate_snapshot_index_locked
#undef cluster_ctrc_shmem_ready
#undef ctrc_put_u32_le
#undef ctrc_put_u16_le
#undef cluster_ctrc_local_release_ack_encode
#undef cluster_ctrc_origin_certificate_validate
#undef cluster_undo_block0_generation_matches
#undef cluster_undo_block0_root_matches
#undef cluster_undo_block0_root_valid
#undef ctrc_cleaner_publish_certificate
#undef ctrc_origin_certificate_snapshot_matches_shared
#undef cluster_ctrc_origin_certificate_digest

static void durable_observers_reset(const ClusterCtrcOriginCertificateSnapshot *snapshot) {
	UndoSegmentHeaderData *header;
	durable_snapshot = snapshot; durable_fault = DURABLE_OK; durable_node = snapshot->origin.key.origin_node_id;
	durable_admitted = durable_current = durable_pinned = false;
	durable_enters = durable_leaves = durable_roots = durable_reads = durable_wals = durable_flushes = 0;
	durable_rechecks = durable_admission_checks = durable_commits = durable_cancels = durable_wakeups = 0;
	durable_event_count = 0; memset(durable_events, 0, sizeof(durable_events));
	memset(&durable_disk, 0, sizeof(durable_disk)); memset(&durable_record, 0, sizeof(durable_record));
	header = (UndoSegmentHeaderData *)durable_disk.data;
	header->segment_id = snapshot->origin.key.segment_id; header->owner_instance = snapshot->origin.key.owner_instance;
	header->wrap_count = snapshot->origin.key.segment_generation; header->tt_slots_count = TT_SLOTS_PER_SEGMENT;
	header->tt_slots[snapshot->origin.key.slot_offset].xid = snapshot->origin.key.xid;
	header->tt_slots[snapshot->origin.key.slot_offset].wrap = snapshot->origin.key.slot_wrap;
	header->tt_slots[snapshot->origin.key.slot_offset].status = TT_SLOT_COMMITTED;
	header->tt_slots[snapshot->origin.key.slot_offset].commit_scn = 9;
	memcpy(durable_resident.data, durable_disk.data, BLCKSZ);
}
static bool durable_snapshot_fixture(QualifiedXminFixture *f, CensusRuntimeAllocation *runtime,
	ClusterHeapFreezeXminTargetV1 *targets, bool batch, ClusterCtrcReceiptHandle *handle,
	uint64 *index, ClusterCtrcOriginCertificateSnapshot *snapshot)
{
	ClusterCtrcOriginEntry *origin; ClusterCtrcLocalReleaseAckV1 *ack; bool exact;
	if (!ack_ready_fixture(f, runtime, targets, batch, handle) || !census_original_nonempty_ack_freeze(handle)
		|| !original_census_origin_index(&handle->key, index)) return false;
	origin = &ctrc_origin_entries()[*index]; ack = &original_certificate_participant_ack_entries()[handle->participant_index];
	participant_publisher_lock_acquire(&CtrcShared->origin_lock, LW_EXCLUSIVE);
	UT_ASSERT(original_origin_reply_origin_begin_seal_entry(origin, 17));
	UT_ASSERT(original_origin_reply_origin_arm_close_entry(origin, ack->participant_node_id, 11));
	UT_ASSERT(original_origin_reply_origin_note_close_reply_entry(origin, ack->participant_node_id, 11, CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK));
	UT_ASSERT(original_origin_reply_origin_begin_cleaning_entry(origin));
	participant_publisher_lock_release(&CtrcShared->origin_lock);
	if (!original_origin_reply_origin_ack_land_shared(11, ack)) return false;
	participant_publisher_lock_acquire(&CtrcShared->origin_lock, LW_EXCLUSIVE);
	exact = original_origin_reply_origin_certificate_snapshot_index_locked(*index, snapshot);
	participant_publisher_lock_release(&CtrcShared->origin_lock);
	if (!exact) return false;
	UT_ASSERT_EQ(origin->state, CTRC_ORIGIN_CERTIFYING); durable_observers_reset(snapshot); return true;
}
static bool census_origin_durable_certificate_publish(const ClusterCtrcOriginCertificateSnapshot *snapshot) {
	return original_durable_publish(snapshot);
}

UT_TEST(test_census_durable_certificate_rejects_empty_snapshot_without_commit)
{
	ClusterCtrcOriginCertificateSnapshot empty = {0};
	UT_ASSERT(!census_origin_durable_certificate_publish(NULL));
	UT_ASSERT(!census_origin_durable_certificate_publish(&empty));
}

UT_TEST(test_census_original_durable_publisher_commits_after_readback_and_cleanup)
{
	for (int shape = 0; shape < 4; shape++) {
		QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterCtrcReceiptHandle handle;
		ClusterCtrcOriginCertificateSnapshot snapshot; PGAlignedBlock expected; uint64 index;
		UndoSegmentHeaderData *header; bool replay = (shape & 1) != 0;
		if (!durable_snapshot_fixture(&f, &runtime, NULL, false, &handle, &index, &snapshot)) { UT_ASSERT(false); return; }
		header = (UndoSegmentHeaderData *)durable_disk.data;
		if (shape & 2) { header->tt_slots[handle.key.slot_offset].status = TT_SLOT_ABORTED; header->tt_slots[handle.key.slot_offset].commit_scn = InvalidScn; }
		if (replay) header->tt_slots[handle.key.slot_offset].flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
		memcpy(durable_resident.data, durable_disk.data, BLCKSZ); expected = durable_disk;
		((UndoSegmentHeaderData *)expected.data)->tt_slots[handle.key.slot_offset].flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
		UT_ASSERT(census_origin_durable_certificate_publish(&snapshot));
		UT_ASSERT_EQ(ctrc_origin_entries()[index].state, CTRC_ORIGIN_RELEASE_PROVEN);
		UT_ASSERT(FullTransactionIdEquals(ctrc_origin_entries()[index].original_full_xid, handle.original_full_xid));
		UT_ASSERT_EQ(handle.participant->state, CTRC_PARTICIPANT_ACK_FROZEN);
		UT_ASSERT(memcmp(durable_disk.data, expected.data, BLCKSZ) == 0);
		UT_ASSERT(memcmp(durable_resident.data, expected.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(durable_wals, !replay); UT_ASSERT_EQ(durable_flushes, !replay);
		UT_ASSERT_EQ(durable_reads, 2); UT_ASSERT_EQ(durable_commits, 1); UT_ASSERT_EQ(durable_wakeups, 1);
		UT_ASSERT_EQ(durable_enters, 1); UT_ASSERT_EQ(durable_leaves, 1);
		UT_ASSERT(strcmp(durable_events, replay ? "EAPURLC" : "EAPWFURLC") == 0);
		UT_ASSERT(!durable_admitted && !durable_current && !durable_pinned);
		if (!replay) {
			UT_ASSERT(cluster_undo_tt_ctrc_release_valid(&durable_record));
			UT_ASSERT_EQ(durable_record.xid, handle.key.xid); UT_ASSERT_EQ(durable_record.seal_generation, 17);
			UT_ASSERT(!ctrc_bytes_zero(durable_record.ack_set_digest, sizeof(durable_record.ack_set_digest)));
		}
		UT_ASSERT_EQ(participant_lock_depth, 0); census_full_layout_release(&runtime);
	}
}

UT_TEST(test_census_original_durable_publisher_preflight_keeps_origin_before_wait)
{
	for (int fault = 0; fault < 10; fault++) {
		QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterCtrcReceiptHandle handle;
		ClusterCtrcOriginCertificateSnapshot snapshot, bad; ClusterCtrcOriginEntry before; uint64 index;
		if (!durable_snapshot_fixture(&f, &runtime, NULL, false, &handle, &index, &snapshot)) { UT_ASSERT(false); return; }
		bad = snapshot; before = ctrc_origin_entries()[index];
		switch (fault) {
			case 0: bad.origin.state = CTRC_ORIGIN_OPEN; break;
			case 1: bad.origin.grant_generation++; break;
			case 2: bad.origin.seal_generation = UINT64_MAX; break;
			case 3: bad.origin.key.system_identifier++; break;
			case 4: bad.origin.key.origin_boot_incarnation++; break;
			case 5: bad.origin.key.segment_id++; break;
			case 6: bad.ack_count = 0; break;
			case 7: bad.acks[0].crc32c ^= 1; break;
			case 8: bad.origin_index = CtrcShared->origin_key_entries; break;
			case 9: durable_node = 1; break;
		}
		UT_ASSERT(!census_origin_durable_certificate_publish(&bad));
		UT_ASSERT(memcmp(&before, &ctrc_origin_entries()[index], sizeof(before)) == 0);
		UT_ASSERT_EQ(durable_enters + durable_roots + durable_wals + durable_flushes + durable_commits, 0);
		UT_ASSERT(!durable_pinned && !durable_current && !durable_admitted);
		UT_ASSERT_EQ(participant_lock_depth, 0); census_full_layout_release(&runtime);
	}
}

UT_TEST(test_census_original_durable_publisher_refusals_never_commit_origin)
{
	for (DurableCertificateFault fault = DURABLE_ENTER_DENIED; fault <= DURABLE_COMMIT_CHANGED; fault++) {
		QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterCtrcReceiptHandle handle;
		ClusterCtrcOriginCertificateSnapshot snapshot; uint64 index;
		if (!durable_snapshot_fixture(&f, &runtime, NULL, false, &handle, &index, &snapshot)) { UT_ASSERT(false); return; }
		durable_fault = fault;
		UT_ASSERT(!census_origin_durable_certificate_publish(&snapshot));
		UT_ASSERT_EQ(ctrc_origin_entries()[index].state, CTRC_ORIGIN_CERTIFYING);
		UT_ASSERT(FullTransactionIdEquals(ctrc_origin_entries()[index].original_full_xid, handle.original_full_xid));
		UT_ASSERT_EQ(handle.participant->state, CTRC_PARTICIPANT_ACK_FROZEN);
		UT_ASSERT_EQ(durable_wals, fault >= DURABLE_WAL_INVALID);
		UT_ASSERT_EQ(durable_flushes, fault > DURABLE_WAL_INVALID);
		UT_ASSERT_EQ(durable_commits, fault == DURABLE_COMMIT_CHANGED);
		UT_ASSERT_EQ(durable_leaves, fault != DURABLE_ENTER_DENIED);
		UT_ASSERT_EQ(durable_wakeups, 0); UT_ASSERT_EQ(durable_cancels, 0);
		UT_ASSERT(!durable_pinned && !durable_current && !durable_admitted);
		UT_ASSERT_EQ(participant_lock_depth, 0); UT_ASSERT_EQ(inserts + dirties + flushes, 0);
		census_full_layout_release(&runtime);
	}
}

UT_TEST(test_census_original_durable_publisher_error_unpins_before_guard_and_admission)
{
	for (DurableCertificateFault fault = DURABLE_ERROR_ACQUIRE; fault <= DURABLE_ERROR_RELEASE; fault++) {
		QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterCtrcReceiptHandle handle;
		ClusterCtrcOriginCertificateSnapshot snapshot; ClusterCtrcOriginEntry before; uint64 index;
		if (!durable_snapshot_fixture(&f, &runtime, NULL, false, &handle, &index, &snapshot)) { UT_ASSERT(false); return; }
		before = ctrc_origin_entries()[index]; durable_fault = fault; expecting_error = true;
		if (setjmp(error_jump) == 0) { (void)census_origin_durable_certificate_publish(&snapshot); UT_ASSERT(false); }
		else {
			UT_ASSERT(PG_exception_stack == NULL); UT_ASSERT_EQ(durable_leaves, 1); UT_ASSERT_EQ(durable_cancels, 1);
			UT_ASSERT(!durable_pinned && !durable_current && !durable_admitted);
			UT_ASSERT(strcmp(durable_events, fault == DURABLE_ERROR_ACQUIRE ? "EAXL" : "EAPWFUXL") == 0);
			UT_ASSERT_EQ(durable_commits + durable_wakeups, 0);
			UT_ASSERT(memcmp(&before, &ctrc_origin_entries()[index], sizeof(before)) == 0);
			UT_ASSERT_EQ(participant_lock_depth, 0); UT_ASSERT(!local_x && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
		}
		expecting_error = false; census_full_layout_release(&runtime);
	}
}

UT_TEST(test_census_original_durable_publish_then_retirement_keeps_old_native_candidate_closed)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; CensusRuntimeAllocation runtime; ClusterCtrcReceiptHandle handle;
		ClusterHeapFreezeXminTargetV1 targets[3]; ClusterCtrcOriginCertificateSnapshot snapshot;
		uint64 index, total, expected_passes, passes = 1; uint16 node;
		if (!durable_snapshot_fixture(&f, &runtime, targets, batch != 0, &handle, &index, &snapshot)) { UT_ASSERT(false); return; }
		node = handle.participant->identity.node_id;
		UT_ASSERT(census_origin_durable_certificate_publish(&snapshot));
		UT_ASSERT_EQ(durable_commits, 1); UT_ASSERT_EQ(durable_wals, 1); UT_ASSERT_EQ(durable_flushes, 1);
		UT_ASSERT(FullTransactionIdEquals(ctrc_origin_entries()[index].original_full_xid, handle.original_full_xid));
		participant_publisher_lock_acquire(&CtrcShared->origin_lock, LW_EXCLUSIVE);
		UT_ASSERT(original_origin_reply_origin_arm_certificate_entry(&ctrc_origin_entries()[index], node, 123));
		participant_publisher_lock_release(&CtrcShared->origin_lock);
		certificate_observers_reset(); UT_ASSERT(origin_reply_reclaim_receiver(&handle));
		UT_ASSERT(census_origin_certificate_reply(123, node));
		UT_ASSERT(ctrc_bytes_zero(&ctrc_origin_entries()[index], sizeof(ClusterCtrcOriginEntry)));
		sampler_xids[0] = 152;
		pg_atomic_write_u64(&sampler_slots[0].cluster_top_full_xid_atomic, U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(1, 152)));
		total = runtime.capacity.origin_key_entries + runtime.capacity.participant_key_entries;
		expected_passes = (total + CLUSTER_CTRC_ACTIVE_SCAN_SLICE - 1) / CLUSTER_CTRC_ACTIVE_SCAN_SLICE;
		native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id; resume_allowed = true;
		worker_original_sample_pass_fixture(&f.cohort);
		while (pass_pending && passes <= expected_passes) { original_worker_sample_active_pass(); passes++; }
		UT_ASSERT_EQ(passes, expected_passes); UT_ASSERT_EQ(publisher_scan.next_index, total);
		UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_COMPLETE); UT_ASSERT_EQ(publisher_scan.unqualified_entries, 0);
		UT_ASSERT(FullTransactionIdEquals(publisher_scan.min_full, FullTransactionIdFromEpochAndXid(1, 152)));
		UT_ASSERT_EQ(native_worker_shared.active_sample.sample_id, f.cohort.source_local_sample_id + 1);
		UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
		UT_ASSERT_EQ(inserts + dirties + flushes + conditional_resumes, 0);
		UT_ASSERT(!local_x && !resumed_writer && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
		census_full_layout_release(&runtime);
	}
}

/* Connect the original unlocked collector observer to the original qualified
 * native apply entry. Observation providers and clock callbacks remain leaves;
 * the following writer entry still executes its original source/current gates.
 * The compact LVRelState is a fixture, not a production VACUUM lifecycle. */
typedef struct LVRelState
{
	Relation rel;
	ClusterSpaceIdentity identity;
	uint64 shared_freeze_proof_calls, shared_freeze_proof_requests;
	uint64 shared_freeze_proof_exchanges;
	bool shared_freeze_proof_cost_incomplete;
	struct VacuumCutoffs cutoffs;
	ClusterHeapFreezePageInputV1 shared_freeze_input;
	ClusterHeapFreezePageBatchV1 shared_freeze_batch;
	ClusterHeapFreezeProofBatchV1 shared_freeze_proofs;
	ClusterHeapFreezeFinalFenceV1 shared_freeze_fence;
	ClusterHeapFreezeXminCandidateBatchV1 shared_freeze_xmin_candidates;
	ClusterHeapFreezeAbortedXmaxCandidateBatchV1 shared_freeze_aborted_xmax_candidates;
	ClusterHeapFreezeCohortObservation shared_freeze_cohort;
	FullTransactionId shared_freeze_candidate_limit_full;
	bool shared_freeze_cohort_observed;
	bool shared_freeze_fence_valid;
} LVRelState;

static const QualifiedXminFixture *bounds_source;
/* Only the real outer-owner cases borrow its stack token. Legacy cases keep
 * their original fixture-owned token and all original checks. */
static const ClusterSemanticAdmissionToken *bounds_borrowed_admission;

static const ClusterSemanticAdmissionToken *
bounds_expected_admission(void)
{
	return bounds_borrowed_admission != NULL ? bounds_borrowed_admission
		: &bounds_source->admission;
}

static unsigned bounds_policy_reads, bounds_cohort_reads, bounds_census_reads;
static unsigned bounds_clock_reads, bounds_clock_observes, bounds_target_reads;
static unsigned bounds_fault;
static unsigned bounds_saved_policy_calls, bounds_saved_policy_fences;
static unsigned bounds_policy_fault, bounds_policy_fault_at;
static bool bounds_saved_policy_result;
static unsigned bounds_saved_current_calls, bounds_current_fault, bounds_current_fault_at;
static ClusterHeapFreezeCohortStatus bounds_saved_current_status;
static bool bounds_after_clock, bounds_after_target;

static void
bounds_assert_unlocked(void)
{
	UT_ASSERT(!local_x && !local_share && backend_pin_count == fixture_expected_backend_pins());
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
	UT_ASSERT_EQ(conditional_resumes,
		phase_use_original_resident && phase_resident_completed
		? phase_resident_expected_resumes : 0);
}

/* Acquisition now executes the original local fold, worker publication getter,
 * R4 capture/current, and before/after-end-clock guards. Original local history
 * sampling uses explicit clock/ProcArray-lock/MRP leaves; maintenance coverage,
 * peer acquisition and source timing remain controlled. */
static unsigned bounds_acquire_fault, bounds_acquire_gets, bounds_acquire_ticks;
static unsigned bounds_acquire_history_reads, bounds_acquire_peer_reads;
static ClusterHeapFreezeCohortStatus bounds_acquire_status;

static bool
bounds_source_capture(ClusterSemanticActiveSampleFence *out)
{
	bounds_assert_unlocked();
	return original_admission_source_capture(out);
}

static bool
bounds_source_admission_current(const ClusterSemanticActiveSampleFence *fence)
{
	bounds_assert_unlocked();
	return cohort_admission_live && original_admission_source_current(fence);
}

static bool
bounds_source_worker_current(const ClusterUndoCleanerActiveSample *active)
{
	bounds_assert_unlocked();
	return cohort_worker_live && original_worker_publication_current(active);
}

static bool
bounds_source_worker_get(ClusterUndoCleanerActiveSample *out)
{
	bounds_assert_unlocked();
	bounds_acquire_gets++;
	if (!cohort_sample_available || !original_worker_publication_get(out)) return false;
	/* Malformed returned shapes isolate the original fold's own predicates;
	 * these are test inputs, not a claim about native publication corruption. */
	switch (bounds_acquire_fault) {
	case 5: out->scan.future_full = FullTransactionIdFromU64(UINT64_MAX); break;
	case 6: out->scan.min_full = FullTransactionIdFromU64(UINT64_MAX); break;
	case 7: out->scan.min_full = FullTransactionIdFromEpochAndXid(1, InvalidTransactionId); break;
	case 8: out->scan.future_full = FullTransactionIdFromU64(U64FromFullTransactionId(out->scan.min_full) - 1); break;
	case 9: out->scan.status = CTRC_ACTIVE_SCAN_INCOMPLETE; break;
	case 10: out->sample_id = 0; break;
	case 11: out->worker_pid = 0; break;
	}
	return true;
}

/* Original history fold and ProcArray sampler, with explicit local scheduling
 * and clock inputs. This is a bounded unit interleaving, not native concurrency. */
static SCN bounds_history_clock;
static PGPROC *bounds_history_all_procs = sampler_slots;
static bool bounds_history_held, bounds_history_register_on_unlock;
static bool bounds_mrp_enabled, bounds_mrp_adg, bounds_mrp_drop_on_history_unlock;
static int bounds_mrp_role;
static ClusterMrpSharedState bounds_mrp_storage, *bounds_mrp_state;

#define scn_total_cmp bounds_original_scn_total_cmp
#include "test_cluster_heap_freeze_scn_total.inc"

#define cluster_enabled bounds_mrp_enabled
#define cluster_enable_adg bounds_mrp_adg
#define cluster_dg_role bounds_mrp_role
#define cluster_mrp_state bounds_mrp_state
#define cluster_mrp_config_allows_start bounds_original_mrp_config_allows_start
#define cluster_mrp_should_start bounds_original_mrp_should_start
#define cluster_mrp_publish_consistent_scn bounds_original_mrp_publish_consistent_scn
#define cluster_mrp_standby_consistent_scn bounds_original_mrp_consistent_scn
#include "test_cluster_heap_freeze_mrp_source.inc"
#undef cluster_mrp_standby_consistent_scn
#undef cluster_mrp_publish_consistent_scn
#undef cluster_mrp_should_start
#undef cluster_mrp_config_allows_start
#undef cluster_mrp_state
#undef cluster_dg_role
#undef cluster_enable_adg
#undef cluster_enabled
#undef scn_total_cmp

/* Direct atomic inputs model unavailable/invalid source states in old refusal
 * cases; ordinary new publication tests use the original publisher above. */
static void
bounds_mrp_test_input(bool active, SCN bound)
{
	bounds_mrp_adg = active;
	pg_atomic_write_u64(&bounds_mrp_storage.standby_consistent_scn, bound);
}
static unsigned bounds_history_locks, bounds_history_unlocks, bounds_history_clocks;

static void
bounds_history_acquire(LWLock *lock, LWLockMode mode)
{
	bounds_assert_unlocked();
	UT_ASSERT(lock == sampler_proc_lock && mode == LW_EXCLUSIVE);
	UT_ASSERT(!bounds_history_held && !sampler_lock_held);
	bounds_history_held = true; bounds_history_locks++;
}

static void
bounds_history_release(LWLock *lock)
{
	UT_ASSERT(lock == sampler_proc_lock && bounds_history_held);
	bounds_history_held = false; bounds_history_unlocks++;
	if (bounds_mrp_drop_on_history_unlock) {
		bounds_mrp_drop_on_history_unlock = false;
		bounds_mrp_state = NULL;
	}
	if (bounds_history_register_on_unlock) {
		bounds_history_register_on_unlock = false;
		ctrc_note_unqualified_registration();
	}
}

static SCN
bounds_history_clock_current(void)
{
	UT_ASSERT(bounds_history_held);
	bounds_history_clocks++;
	return bounds_history_clock;
}

static bool
bounds_history_mrp_should_start(void)
{
	UT_ASSERT(!bounds_history_held);
	return bounds_original_mrp_should_start();
}

static SCN
bounds_history_mrp_consistent_scn(void)
{
	UT_ASSERT(bounds_original_mrp_should_start() && !bounds_history_held);
	return bounds_original_mrp_consistent_scn();
}

#define cluster_undo_retention_sample_min bounds_original_retention_min
#include "test_cluster_heap_freeze_retention_min.inc"
#undef cluster_undo_retention_sample_min

static bool bounds_original_retention_sample(ClusterUndoRetentionSample *out);
static SCN bounds_original_retention_horizon(void);

#define procArray sampler_proc_array
#define allProcs bounds_history_all_procs
#define MainLWLockArray sampler_locks
#define LWLockAcquire bounds_history_acquire
#define LWLockRelease bounds_history_release
#define cluster_scn_current bounds_history_clock_current
#define UINT32_ACCESS_ONCE(x) ((uint32) (x))
#define cluster_undo_retention_sample_min bounds_original_retention_min
#define cluster_undo_retention_horizon_sample bounds_original_retention_sample
#define cluster_undo_retention_horizon bounds_original_retention_horizon
#include "test_cluster_retention_sample.inc"
#undef cluster_undo_retention_horizon
#undef cluster_undo_retention_horizon_sample
#undef cluster_undo_retention_sample_min
#undef UINT32_ACCESS_ONCE
#undef cluster_scn_current
#undef LWLockRelease
#undef LWLockAcquire
#undef MainLWLockArray
#undef allProcs
#undef procArray

#define cluster_undo_retention_horizon_sample bounds_original_retention_sample
#define cluster_mrp_should_start bounds_history_mrp_should_start
#define cluster_mrp_standby_consistent_scn bounds_history_mrp_consistent_scn
#define undo_horizon_sample_local_report bounds_original_local_report
#include "test_cluster_heap_freeze_local_history.inc"
#undef undo_horizon_sample_local_report
#undef cluster_mrp_standby_consistent_scn
#undef cluster_mrp_should_start
#undef cluster_undo_retention_horizon_sample

static bool
bounds_source_history(ClusterUndoRetentionSample *out)
{
	bounds_assert_unlocked(); bounds_acquire_history_reads++;
	if (bounds_acquire_fault == 1) return false;
	if (!bounds_original_local_report(out)) return false;
	if (bounds_acquire_fault == 2) out->clock_scn = 0;
	if (bounds_acquire_fault == 3) out->horizon_scn = out->clock_scn + 1;
	if (bounds_acquire_fault == 4) out->horizon_scn = CLUSTER_UNDO_HORIZON_REPORT_UNCONSTRAINED;
	return true;
}

static TimestampTz
bounds_source_timestamp(void)
{
	bounds_assert_unlocked();
	if (++bounds_acquire_ticks == 1) return (TimestampTz)bounds_source->cohort.source_started_at_us;
	return cohort_current_end_clock();
}

static UndoHorizonMaintenanceIntent
bounds_source_maintenance(void)
{
	UndoHorizonMaintenanceIntent none = {0};
	return none;
}

static uint64
bounds_source_coverage(int32 origin pg_attribute_unused(), uint64 future pg_attribute_unused())
{
	return 0;
}

static ClusterHeapFreezeCohortStatus
bounds_source_peer(int32 peer, const ClusterSemanticActiveSampleFence *fence,
	UndoHorizonV2PeerObservation *identity pg_attribute_unused(),
	ClusterHeapFreezeCohortObservation *candidate pg_attribute_unused())
{
	bounds_assert_unlocked(); bounds_acquire_peer_reads++;
	UT_ASSERT(peer != cluster_node_id && undo_horizon_v2_required(fence, peer));
	/* No remote acquisition provider in this local fixture: never return READY. */
	return CLUSTER_HEAP_FREEZE_COHORT_INCOMPLETE;
}

#define undo_horizon_local_source_current bounds_original_local_source_current
#define undo_horizon_v2_cohort_current bounds_original_cohort_current
#define undo_horizon_v2_cohort_observe bounds_original_cohort_observe
#define cluster_heap_freeze_cohort_observe bounds_original_cohort_observe_entry
#define cluster_undo_cleaner_active_sample bounds_source_worker_get
#define cluster_undo_cleaner_active_sample_current bounds_source_worker_current
#define cluster_semantic_activation_active_sample_capture bounds_source_capture
#define cluster_semantic_activation_active_sample_current bounds_source_admission_current
#define cluster_sf_peer_capability_generation_matches cohort_peer_generation_current
#define undo_horizon_sample_local_report bounds_source_history
#define undo_horizon_maintenance_sample bounds_source_maintenance
#define undo_horizon_maintenance_coverage bounds_source_coverage
#define undo_horizon_v2_observe_peer bounds_source_peer
#define GetCurrentTimestamp bounds_source_timestamp
#define cluster_lmon_main_loop_interval cohort_interval_ms
#include "test_cluster_heap_freeze_cohort_observe.inc"
#undef cluster_lmon_main_loop_interval
#undef GetCurrentTimestamp
#undef undo_horizon_v2_observe_peer
#undef undo_horizon_maintenance_coverage
#undef undo_horizon_maintenance_sample
#undef undo_horizon_sample_local_report
#undef cluster_sf_peer_capability_generation_matches
#undef cluster_semantic_activation_active_sample_current
#undef cluster_semantic_activation_active_sample_capture
#undef cluster_undo_cleaner_active_sample_current
#undef cluster_undo_cleaner_active_sample
#undef cluster_heap_freeze_cohort_observe
#undef undo_horizon_v2_cohort_observe
#undef undo_horizon_v2_cohort_current
#undef undo_horizon_local_source_current

/* A separate alias of the original saved-observation entry avoids the
 * native-entry fixture's independent counters while retaining its real gates. */
#define undo_horizon_v2_required bounds_saved_required
#define cluster_heap_freeze_cohort_current bounds_saved_cohort_current
#define cluster_undo_cleaner_active_sample bounds_source_worker_get
#define cluster_semantic_activation_active_sample_capture bounds_source_capture
#define undo_horizon_v2_cohort_current bounds_original_cohort_current
#include "test_cluster_heap_freeze_cohort_current.inc"
#undef undo_horizon_v2_cohort_current
#undef cluster_semantic_activation_active_sample_capture
#undef cluster_undo_cleaner_active_sample
#undef cluster_heap_freeze_cohort_current
#undef undo_horizon_v2_required

static ClusterHeapFreezeCohortStatus
bounds_cohort_observe(ClusterHeapFreezeCohortObservation *out)
{
	bounds_assert_unlocked();
	bounds_acquire_status = bounds_original_cohort_observe_entry(out);
	return bounds_acquire_status;
}

static bool
bounds_saved_allocation_fence_current(const ClusterXidStripeSampleFence *fence)
{
	bounds_assert_unlocked();
	bounds_saved_policy_fences++;
	return original_retained_lease_current(fence);
}

/* Preserve the original predicate with independent collector counters, rather
 * than routing through the native-entry adapter's unrelated assertions. */
#define cluster_xid_stripe_sample_fence_current bounds_saved_allocation_fence_current
#define cluster_heap_freeze_policy_sample_current_v1 bounds_saved_policy_current
#include "test_cluster_heap_freeze_policy_current.inc"
#undef cluster_heap_freeze_policy_sample_current_v1
#undef cluster_xid_stripe_sample_fence_current

static bool
bounds_policy_current(const ClusterHeapFreezePolicySampleV1 *sample)
{
	bounds_assert_unlocked();
	UT_ASSERT(memcmp(sample, &bounds_source->policy, sizeof(*sample)) == 0);
	bounds_policy_reads++;
	if (bounds_after_target && bounds_fault == 8)
		elog(ERROR, "injected collector final source ERROR");
	if ((bounds_after_target && bounds_fault == 1)
		|| (bounds_after_clock && bounds_fault == 5))
		return false;
	if (bounds_policy_reads == bounds_policy_fault_at) {
		if (bounds_policy_fault == 1) cluster_shared_config = false;
		else if (bounds_policy_fault == 2) cluster_enabled = false;
		else if (bounds_policy_fault == 3) cluster_xid_striping = false;
		else if (bounds_policy_fault >= 4 && bounds_policy_fault <= 17)
			retained_lease_fault(bounds_policy_fault - 4);
		else if (bounds_policy_fault == 18 || bounds_policy_fault == 19) {
			lease_revoke_after_unlock = lease_lock_releases + 1;
			lease_revoke_kind = bounds_policy_fault == 18 ? 1 : 2;
		} else if (bounds_policy_fault == 20 || bounds_policy_fault == 21)
			lease_error_read = lease_incarnation_reads + (bounds_policy_fault == 20 ? 1 : 2);
	}
	bounds_saved_policy_calls++;
	bounds_saved_policy_result = bounds_saved_policy_current(sample);
	return bounds_saved_policy_result;
}

static ClusterHeapFreezeCohortStatus
bounds_cohort_current(const ClusterHeapFreezeCohortObservation *cohort)
{
	bounds_assert_unlocked();
	UT_ASSERT(memcmp(cohort, &bounds_source->cohort, sizeof(*cohort)) == 0);
	bounds_cohort_reads++;
	if ((bounds_after_target && bounds_fault == 2)
		|| (bounds_after_clock && bounds_fault == 6))
		return CLUSTER_HEAP_FREEZE_COHORT_IDENTITY_CHANGED;
	/* Controlled serial source changes at each original current boundary;
	 * the original saved entry and providers decide the refusal themselves. */
	if (bounds_cohort_reads == bounds_current_fault_at) {
		switch (bounds_current_fault) {
		case 1: native_worker_shared.workers[0].pid = 0; break;
		case 2: native_worker_shared.active_sample.sample_id++; break;
		case 3: worker_lease_ok = false; break;
		case 4: admission_cut++; break;
		case 5: ctrc_note_unqualified_registration(); break;
		case 6: cohort_end_clock = (TimestampTz)cohort->source_started_at_us
			+ (uint64)CLUSTER_UNDO_HORIZON_FRESHNESS_FACTOR * cohort->source_interval_ms * 1000 + 1; break;
		case 7: cohort_end_clock = (TimestampTz)cohort->source_started_at_us - 1; break;
		case 8: cohort_clock_error = true; break;
		case 9: worker_lease_error = true; break;
		}
	}
	bounds_saved_current_calls++;
	bounds_saved_current_status = bounds_saved_cohort_current(cohort);
	return bounds_saved_current_status;
}

/* Original admission rechecks share the existing seqlock snapshot.  Startup,
 * epoch and descriptor publication remain controlled source providers. */
static const void *bounds_admission_descriptors[64];
static ClusterSemanticAdmissionToken *bounds_mutable_admission;
static unsigned bounds_original_target_calls, bounds_original_census_calls;
static unsigned bounds_target_fault, bounds_census_fault, bounds_census_fault_at;
static unsigned bounds_admission_error_kind;
static bool bounds_original_target_result, bounds_original_census_result;
static bool bounds_bootstrap_allowed;
static unsigned bounds_start_census_change_at, bounds_start_census_fault;

#include "test_cluster_heap_freeze_startup_types.inc"
static ClusterNormalStartCompletion bounds_start_storage;
static ClusterNormalStartCompletion *bounds_start_completion;
static ClusterNormalStartState bounds_original_start_state(void);
#define NormalStartCompletion bounds_start_completion
#define cluster_enabled bounds_start_cluster_enabled
#define cluster_shared_data_dir bounds_start_shared_dir
#define cluster_semantic_normal_start_state bounds_original_start_state
#define normal_start_shared_configured bounds_original_start_shared_configured
#define normal_start_bootstrap_allowed bounds_original_start_bootstrap
#define normal_start_serving_allowed bounds_original_start_serving
#include "test_cluster_heap_freeze_startup_source.inc"
#undef normal_start_serving_allowed
#undef normal_start_bootstrap_allowed
#undef normal_start_shared_configured
#undef cluster_semantic_normal_start_state
#undef cluster_shared_data_dir
#undef cluster_enabled
#undef NormalStartCompletion

static void
bounds_start_publish_state(ClusterNormalStartState state)
{
	/* Controlled private publication; execute original reader/predicates,
	 * without claiming original Startup classification or ownership. */
	memset(&bounds_start_storage, 0, sizeof(bounds_start_storage));
	pg_atomic_init_u32(&bounds_start_storage.state, state);
	bounds_start_completion = &bounds_start_storage;
}

static void
bounds_start_retire(unsigned fault)
{
	const ClusterNormalStartState states[] = {
		CLUSTER_NORMAL_START_UNCLASSIFIED, CLUSTER_NORMAL_START_TARGET_LOADING,
		CLUSTER_NORMAL_START_FAILED, (ClusterNormalStartState)UINT32_MAX };

	UT_ASSERT(fault <= lengthof(states));
	if (fault == 0) bounds_start_completion = NULL;
	else bounds_start_publish_state(states[fault - 1]);
}


static void
bounds_admission_assert_source_ownership(void)
{
	if (!local_x) { bounds_assert_unlocked(); return; }
	/* Only original native admission checks may call these source providers
	 * under the caller's content-X; they run before the critical section. */
	UT_ASSERT(native_original_admission_enabled && native_admission_stage >= 3 && native_admission_stage <= 6);
	UT_ASSERT(native_admission_expected_token != NULL && native_admission_expected_token->entered);
	UT_ASSERT(!local_share && backend_pin_count == 1); UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
}

static bool
bounds_admission_serving(void)
{
	bounds_admission_assert_source_ownership();
	if (bounds_admission_error_kind == 1) elog(ERROR, "injected original admission serving ERROR");
	return admission_serving && (!bounds_start_original_enabled || bounds_original_start_serving());
}

static bool
bounds_admission_bootstrap(void)
{
	bounds_admission_assert_source_ownership();
	if (bounds_admission_error_kind == 3) elog(ERROR, "injected original admission bootstrap ERROR");
	return bounds_start_original_enabled ? bounds_original_start_bootstrap() : bounds_bootstrap_allowed;
}

static uint64
bounds_admission_epoch(void)
{
	bounds_admission_assert_source_ownership();
	if (bounds_admission_error_kind == 2) elog(ERROR, "injected original admission epoch ERROR");
	return admission_epoch;
}

static bool bounds_original_target_current(const ClusterSemanticAdmissionToken *);
static bool bounds_original_census_current(const ClusterSemanticAdmissionToken *);
#define SemanticActivationDescriptors bounds_admission_descriptors
#define normal_start_serving_allowed bounds_admission_serving
#define normal_start_bootstrap_allowed bounds_admission_bootstrap
#define cluster_epoch_get_current bounds_admission_epoch
#define cluster_semantic_activation_recheck bounds_original_target_current
#define cluster_semantic_activation_recheck_r4_terminal_census bounds_original_census_current
#include "test_cluster_heap_freeze_admission_current.inc"
#undef cluster_semantic_activation_recheck_r4_terminal_census
#undef cluster_semantic_activation_recheck
#undef cluster_epoch_get_current
#undef normal_start_bootstrap_allowed
#undef normal_start_serving_allowed
#undef SemanticActivationDescriptors

static void
bounds_admission_publish_change(unsigned mode)
{
	SemanticActivationAdmissionSnapshot before, after;
	uint64 bits, generation, epoch;
	bool closed;

	UT_ASSERT(mode < 5);
	UT_ASSERT(semantic_activation_snapshot(&before));
	bits = (mode == 1 || mode == 2) ? 0 : before.active_bits;
	generation = before.record_generation + (mode == 4 ? 0 : 1);
	epoch = before.formation_epoch + (mode == 4 ? 1 : 0);
	closed = mode < 2 || mode == 4;
	/* The original private publisher supplies its full seqlock transaction.
	 * These are gate images, not a replacement for the LMON round/ACK proof. */
	UT_ASSERT(semantic_activation_lmon_publish_gate(&before, bits, generation, epoch, closed));
	UT_ASSERT(semantic_activation_snapshot(&after));
	UT_ASSERT_EQ(after.seq, before.seq + 2);
	UT_ASSERT_EQ(after.active_bits, bits);
	UT_ASSERT_EQ(after.record_generation, generation);
	UT_ASSERT_EQ(after.formation_epoch, epoch);
	UT_ASSERT_EQ(after.transition_closed, closed);
}

static void
bounds_admission_change(unsigned fault)
{
	int feature_index;

	switch (fault) {
	case 1: pg_atomic_write_u64(&native_admission_shared.active_bits, 0); break;
	case 2: pg_atomic_write_u32(&native_admission_shared.transition_closed, 1); break;
	case 3: pg_atomic_write_u64(&native_admission_shared.admission_seq, 3); break;
	case 4: pg_atomic_fetch_add_u64(&native_admission_shared.record_generation, 1); break;
	case 5: pg_atomic_fetch_add_u64(&native_admission_shared.formation_epoch, 1); break;
	case 6: admission_epoch++; break;
	case 7: bounds_mutable_admission->record_generation++; break;
	case 8: bounds_mutable_admission->formation_epoch++; break;
	case 9: bounds_mutable_admission->feature_bit = 0; break;
	case 10: bounds_mutable_admission->side = CLUSTER_SEMANTIC_SOURCE_SIDE; break;
	case 11: SemanticActivationShmem = NULL; break;
	case 12: admission_serving = false; break;
	case 13:
		UT_ASSERT(semantic_activation_feature_bit_index(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, &feature_index));
		bounds_admission_descriptors[feature_index] = NULL; break;
	case 14:
		pg_atomic_write_u64(&native_admission_shared.record_generation, 0);
		bounds_mutable_admission->record_generation = 0; break;
	case 21: bounds_admission_error_kind = 1; break;
	case 22: bounds_admission_error_kind = 2; break;
	case 23:
		pg_atomic_write_u64(&native_admission_shared.record_generation, 0);
		bounds_mutable_admission->record_generation = 0;
		bounds_admission_error_kind = 3; break;
	case 24: case 25: case 26: case 27: case 28:
		bounds_admission_publish_change(fault - 24); break;
	}
}

static bool
fixture_native_admission_current(const ClusterSemanticAdmissionToken *token, unsigned stage)
{
	if (!native_original_admission_enabled) return true;
	UT_ASSERT(token == native_admission_expected_token && token != NULL && token->entered);
	UT_ASSERT(stage >= 1 && stage <= 6); UT_ASSERT_EQ(local_x, stage >= 3);
	UT_ASSERT_EQ(backend_pin_count, 1); UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
	native_admission_stage = stage; native_admission_checks[stage]++;
	if (stage == native_admission_fault_at) bounds_admission_change(native_admission_fault);
	if (stage == native_start_change_at) bounds_start_retire(native_start_fault);
	return (stage % 2) == 0 ? bounds_original_census_current(token) : bounds_original_target_current(token);
}

static bool
bounds_census_current(const ClusterSemanticAdmissionToken *token)
{
	bounds_assert_unlocked();
	UT_ASSERT(token == bounds_expected_admission() && token->entered);
	bounds_census_reads++;
	if (bounds_after_target && bounds_fault == 4) return false;
	if (bounds_census_reads == bounds_census_fault_at) bounds_admission_change(bounds_census_fault);
	if (bounds_census_reads == bounds_start_census_change_at) bounds_start_retire(bounds_start_census_fault);
	bounds_original_census_calls++;
	bounds_original_census_result = bounds_original_census_current(token);
	return bounds_original_census_result;
}

static bool
bounds_target_current(const ClusterSemanticAdmissionToken *token)
{
	bounds_assert_unlocked();
	UT_ASSERT(token == bounds_expected_admission() && token->entered);
	bounds_target_reads++;
	bounds_after_target = true;
	if (bounds_fault == 3) return false;
	bounds_admission_change(bounds_target_fault);
	bounds_original_target_calls++;
	bounds_original_target_result = bounds_original_target_current(token);
	return bounds_original_target_result;
}

static SCN
bounds_clock_current(void)
{
	bounds_assert_unlocked();
	bounds_clock_reads++;
	return fixture_wal_clock;
}

static void
bounds_clock_observe(SCN horizon)
{
	bounds_assert_unlocked();
	bounds_clock_observes++;
	if (bounds_fault == 7) elog(ERROR, "injected collector clock ERROR");
	if (bounds_fault != 9)
	{
		if (source_exit_history_requested)
		{
			/* The new combined source uses a later clock than H. Keep this
			 * explicit clock leaf monotone, as the original observer does. */
			if (scn_time_cmp(fixture_wal_clock, horizon) < 0) fixture_wal_clock = horizon;
		}
		else fixture_wal_clock = horizon;
	}
	bounds_after_clock = true;
}

#define cluster_heap_freeze_cohort_observe bounds_cohort_observe
#define cluster_heap_freeze_policy_sample_current_v1 bounds_policy_current
#define cluster_heap_freeze_cohort_current bounds_cohort_current
#define cluster_semantic_activation_recheck_r4_terminal_census bounds_census_current
#define cluster_semantic_activation_recheck bounds_target_current
#define cluster_scn_current bounds_clock_current
#define cluster_scn_observe bounds_clock_observe
#include "test_cluster_vacuum_freeze_bound_observer.inc"
#undef cluster_scn_observe
#undef cluster_scn_current
#undef cluster_semantic_activation_recheck
#undef cluster_semantic_activation_recheck_r4_terminal_census
#undef cluster_heap_freeze_cohort_current
#undef cluster_heap_freeze_policy_sample_current_v1
#undef cluster_heap_freeze_cohort_observe

static void
bounds_observer_setup(LVRelState *v, QualifiedXminFixture *f)
{
	memset(v, 0, sizeof(*v));
	v->cutoffs.SharedFreezePolicySample = f->policy;
	v->cutoffs.SharedFreezeLimitFull = f->policy.freeze_limit;
	v->cutoffs.FreezeLimit = XidFromFullTransactionId(f->policy.freeze_limit);
	v->shared_freeze_input = f->input;
	v->shared_freeze_input.freeze_policy = NULL;
	v->shared_freeze_input.freeze_cohort = NULL;
	bounds_source = f;
	bounds_borrowed_admission = NULL;
	bounds_mutable_admission = &f->admission;
	f->admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	f->admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	memset(bounds_admission_descriptors, 0, sizeof(bounds_admission_descriptors));
	{
		int feature_index;
		UT_ASSERT(semantic_activation_feature_bit_index(f->admission.feature_bit, &feature_index));
		bounds_admission_descriptors[feature_index] = &native_admission_shared;
	}
	bounds_original_target_calls = bounds_original_census_calls = 0;
	bounds_target_fault = bounds_census_fault = bounds_census_fault_at = 0;
	bounds_admission_error_kind = 0; bounds_bootstrap_allowed = false;
	bounds_start_census_change_at = bounds_start_census_fault = 0;
	bounds_original_target_result = bounds_original_census_result = false;
	bounds_acquire_fault = bounds_acquire_gets = bounds_acquire_ticks = 0;
	bounds_acquire_history_reads = bounds_acquire_peer_reads = 0;
	bounds_acquire_status = CLUSTER_HEAP_FREEZE_COHORT_INVALID;
	bounds_history_clock = f->cohort.source_local_history.sample_clock_scn;
	memset(&bounds_mrp_storage, 0, sizeof(bounds_mrp_storage));
	pg_atomic_init_u64(&bounds_mrp_storage.standby_consistent_scn, 0);
	bounds_mrp_state = &bounds_mrp_storage;
	bounds_mrp_enabled = true; bounds_mrp_adg = bounds_mrp_drop_on_history_unlock = false;
	bounds_mrp_role = CLUSTER_DG_ROLE_STANDBY;
	bounds_history_all_procs = sampler_slots;
	bounds_history_held = bounds_history_register_on_unlock = false;
	bounds_history_locks = bounds_history_unlocks = bounds_history_clocks = 0;
	bounds_policy_reads = bounds_cohort_reads = bounds_census_reads = 0;
	bounds_saved_current_calls = bounds_current_fault = bounds_current_fault_at = 0;
	bounds_saved_policy_calls = bounds_saved_policy_fences = 0;
	bounds_policy_fault = bounds_policy_fault_at = 0; bounds_saved_policy_result = false;
	bounds_saved_current_status = CLUSTER_HEAP_FREEZE_COHORT_INVALID;
	bounds_clock_reads = bounds_clock_observes = bounds_target_reads = 0;
	bounds_fault = 0;
	bounds_after_clock = bounds_after_target = false;
	fixture_release_original_writer();
	resume_allowed = true;
}

UT_TEST(test_collector_observer_owned_sources_reach_single_and_batch_native_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0};

		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		bounds_observer_setup(&v, &f); fixture_wal_clock = 5;
		UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
			CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		UT_ASSERT(v.shared_freeze_cohort_observed);
		UT_ASSERT(v.shared_freeze_input.freeze_policy == &v.cutoffs.SharedFreezePolicySample);
		UT_ASSERT(v.shared_freeze_input.freeze_cohort == &v.shared_freeze_cohort);
		UT_ASSERT(FullTransactionIdEquals(v.shared_freeze_candidate_limit_full, f.policy.freeze_limit));
		UT_ASSERT_EQ(bounds.horizon_scn, 10);
		UT_ASSERT_EQ(bounds_clock_reads, 2); UT_ASSERT_EQ(bounds_clock_observes, 1);
		UT_ASSERT_EQ(bounds_policy_reads, 3); UT_ASSERT_EQ(bounds_cohort_reads, 3);
		UT_ASSERT_EQ(bounds_census_reads, 3); UT_ASSERT_EQ(bounds_target_reads, 1);
		f.input = v.shared_freeze_input;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1); UT_ASSERT_EQ(inserted_wal_clock, 10);
		UT_ASSERT_EQ(conditional_resumes, 1); UT_ASSERT_EQ(conditional_unlocks, 1);
		UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_collector_observer_refusals_cannot_supply_native_bound_sources)
{
	for (int batch = 0; batch < 2; batch++) {
		for (unsigned fault = 1; fault <= 9; fault++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0};
			if (fault == 7 || fault == 8) continue;
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			bounds_observer_setup(&v, &f); bounds_fault = fault; fixture_wal_clock = 5;
			UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
				CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), fault == 4
				? CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED : CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
			UT_ASSERT(!v.shared_freeze_cohort_observed);
			UT_ASSERT(v.shared_freeze_input.freeze_policy == NULL && v.shared_freeze_input.freeze_cohort == NULL);
			f.input = v.shared_freeze_input;
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes, 0);
			UT_ASSERT(!local_x && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_collector_observer_borrows_still_require_original_native_source_current)
{
	for (int batch = 0; batch < 2; batch++) {
		for (unsigned fault = 1; fault <= 5; fault++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0};
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			bounds_observer_setup(&v, &f);
			UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
				CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
			UT_ASSERT(v.shared_freeze_cohort_observed); f.input = v.shared_freeze_input;
			if (fault == 1) saved_policy_current = false;
			if (fault == 2) cohort_worker_live = false;
			if (fault == 3) cohort_sample_available = false;
			if (fault == 4) ctrc_note_unqualified_registration();
			if (fault == 5) lease_incarnation++;
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes, 0);
			UT_ASSERT(!local_x && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_collector_observer_owned_sources_are_rechecked_after_actual_space)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int fault = 1; fault <= 3; fault++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0};
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			bounds_observer_setup(&v, &f);
			UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
				CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
			UT_ASSERT(v.shared_freeze_cohort_observed); f.input = v.shared_freeze_input;
			change_source_after_space_target = fault;
			if (fault == 2) post_space_bounds = &v.shared_freeze_cohort;
			if (fault == 3) post_space_policy = &v.cutoffs.SharedFreezePolicySample;
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(space_observation_checks, 1); UT_ASSERT_EQ(post_space_target_checks, 1);
			UT_ASSERT_EQ(post_space_census_checks, 1);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
			UT_ASSERT_EQ(conditional_resumes, 1); UT_ASSERT_EQ(conditional_unlocks, 1);
			UT_ASSERT(!local_x && !resumed_writer && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_collector_observer_error_preserves_caller_pin_admission_and_page)
{
	for (unsigned fault = 7; fault <= 8; fault++) {
		QualifiedXminFixture f; LVRelState v;
		ClusterHeapFreezeObservedBoundsV1 bounds = {0};
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		bounds_observer_setup(&v, &f); bounds_fault = fault; fixture_wal_clock = 5;
		expecting_error = true;
		if (setjmp(error_jump) == 0) {
			(void)lazy_observe_shared_freeze_bounds(&v, &f.admission,
				CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds);
			UT_ASSERT(false);
		}
		UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes, 0);
		UT_ASSERT(!local_x && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		expecting_error = false;
	}
}

UT_TEST(test_collector_observer_keep_unproven_preserves_selected_native_subset)
{
	QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
	LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0};
	qualified_xmin_batch_setup(&f, targets); bounds_observer_setup(&v, &f);
	UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
		CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
	UT_ASSERT(v.shared_freeze_cohort_observed); f.input = v.shared_freeze_input;
	UT_ASSERT(qualified_xmin_batch_apply(&f, targets, 3));
	UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1); UT_ASSERT_EQ(expected_nplans, 2);
	UT_ASSERT_EQ(conditional_resumes, 1); UT_ASSERT_EQ(conditional_unlocks, 1);
	UT_ASSERT(!local_x && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
}

UT_TEST(test_collector_original_cohort_acquisition_publishes_only_after_full_local_checks)
{
	for (unsigned fault = 1; fault <= 11; fault++) {
		QualifiedXminFixture f; LVRelState v;
		ClusterHeapFreezeCohortObservation out, before;
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); bounds_observer_setup(&v, &f);
		bounds_acquire_fault = fault; memset(&out, 0xa6, sizeof(out)); before = out;
		UT_ASSERT_EQ(bounds_original_cohort_observe_entry(&out), fault == 1
			? CLUSTER_HEAP_FREEZE_COHORT_INCOMPLETE : CLUSTER_HEAP_FREEZE_COHORT_INVALID);
		UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);
		UT_ASSERT_EQ(bounds_acquire_gets, 1); UT_ASSERT_EQ(bounds_acquire_history_reads, 1);
		UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes, 0);
		UT_ASSERT(!local_x && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_collector_original_cohort_acquisition_refusal_blocks_single_and_batch_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		for (unsigned fault = 1; fault <= 11; fault++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0};
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			bounds_observer_setup(&v, &f); bounds_acquire_fault = fault;
			UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
				CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
			UT_ASSERT(bounds_acquire_status != CLUSTER_HEAP_FREEZE_COHORT_READY);
			UT_ASSERT(!v.shared_freeze_cohort_observed);
			UT_ASSERT(v.shared_freeze_input.freeze_policy == NULL && v.shared_freeze_input.freeze_cohort == NULL);
			f.input = v.shared_freeze_input;
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes, 0);
			UT_ASSERT(!local_x && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_collector_original_cohort_end_clock_rechecks_worker_admission_and_ingress)
{
	for (unsigned fault = 1; fault <= 5; fault++) {
		QualifiedXminFixture f; LVRelState v;
		ClusterHeapFreezeObservedBoundsV1 bounds = {0};
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); bounds_observer_setup(&v, &f);
		if (fault == 1) cohort_clock_revoke_worker = true;
		if (fault == 2) cohort_clock_revoke_admission = true;
		if (fault == 3) worker_register_on_unlock = true;
		if (fault == 4) cohort_end_clock = (TimestampTz)f.cohort.source_started_at_us - 1;
		if (fault == 5) cohort_end_clock = (TimestampTz)f.cohort.source_started_at_us
			+ (uint64)CLUSTER_UNDO_HORIZON_FRESHNESS_FACTOR * f.cohort.source_interval_ms * 1000 + 1;
		UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
			CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		UT_ASSERT(bounds_acquire_status != CLUSTER_HEAP_FREEZE_COHORT_READY);
		UT_ASSERT(!v.shared_freeze_cohort_observed); f.input = v.shared_freeze_input;
		UT_ASSERT(!qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes, 0);
		UT_ASSERT(!local_x && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_collector_original_cohort_missing_required_peer_never_borrows_caller_view)
{
	for (int peer = 1; peer <= 65; peer += 64) {
		QualifiedXminFixture f; LVRelState v;
		ClusterHeapFreezeObservedBoundsV1 bounds = {0};
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); qualified_xmin_require_peer(&f, peer);
		bounds_observer_setup(&v, &f);
		UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
			CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		UT_ASSERT_EQ(bounds_acquire_status, CLUSTER_HEAP_FREEZE_COHORT_INCOMPLETE);
		UT_ASSERT_EQ(bounds_acquire_peer_reads, 1);
		UT_ASSERT(!v.shared_freeze_cohort_observed); f.input = v.shared_freeze_input;
		UT_ASSERT(!qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes, 0);
		UT_ASSERT(!local_x && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_collector_original_cohort_provider_error_retains_caller_resources)
{
	for (unsigned fault = 1; fault <= 3; fault++) {
		QualifiedXminFixture f; LVRelState v;
		ClusterHeapFreezeObservedBoundsV1 bounds = {0};
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); bounds_observer_setup(&v, &f);
		if (fault == 1) worker_lease_error = true;
		if (fault == 2) cohort_clock_error = true;
		if (fault == 3) admission_membership_fault = 9;
		expecting_error = true;
		if (setjmp(error_jump) == 0) {
			(void)lazy_observe_shared_freeze_bounds(&v, &f.admission,
				CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds);
			UT_ASSERT(false);
		}
		UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes, 0);
		UT_ASSERT(!local_x && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		expecting_error = false;
	}
}

UT_TEST(test_collector_original_history_same_cut_clock_and_compact_snapshot)
{
	QualifiedXminFixture f; LVRelState v; ClusterUndoRetentionSample history;
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); bounds_observer_setup(&v, &f);
	UT_ASSERT(bounds_original_local_report(&history));
	UT_ASSERT_EQ(history.horizon_scn, 10); UT_ASSERT_EQ(history.clock_scn, 10);
	UT_ASSERT_EQ(history.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
	UT_ASSERT_EQ(history.contributor_token, 0);
	UT_ASSERT_EQ(bounds_original_retention_horizon(), 10);
	/* Visit an unknown entry first, then the equal-time known compact member.
	 * The diagnostic token is its physical slot, not the compact ordinal. */
	sampler_proc_array->numProcs = 2;
	sampler_proc_array->pgprocnos[0] = 0; sampler_proc_array->pgprocnos[1] = 2;
	sampler_slots[0].xmin = sampler_slots[2].xmin = 100;
	sampler_slots[0].pid = 0; sampler_slots[2].pid = 42;
	pg_atomic_write_u64(&sampler_slots[0].cluster_read_scn_atomic, 8);
	pg_atomic_write_u64(&sampler_slots[2].cluster_read_scn_atomic, 8);
	UT_ASSERT(bounds_original_local_report(&history));
	UT_ASSERT_EQ(history.horizon_scn, 8); UT_ASSERT_EQ(history.clock_scn, 10);
	UT_ASSERT_EQ(history.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
	UT_ASSERT_EQ(history.contributor_token, 3);
	UT_ASSERT_EQ(bounds_history_locks, 3); UT_ASSERT_EQ(bounds_history_unlocks, 3);
	UT_ASSERT_EQ(bounds_history_clocks, 3); UT_ASSERT(!bounds_history_held);
	bounds_assert_unlocked();
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
}

UT_TEST(test_collector_original_history_refusals_block_single_and_batch_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		for (unsigned fault = 0; fault < 10; fault++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0};
			ClusterUndoRetentionSample history, zero = {0};
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			bounds_observer_setup(&v, &f); fixture_wal_clock = 5;
			switch (fault) {
			case 0: sampler_slots[0].xmin = 100; break;
			case 1: sampler_proc_array->replication_slot_xmin = 100; break;
			case 2: sampler_proc_array->replication_slot_catalog_xmin = 100; break;
			case 3: pg_atomic_write_u64(&sampler_slots[0].cluster_read_scn_atomic, UINT64_MAX); break;
			case 4: bounds_history_clock = UINT64_MAX; break;
			case 5: bounds_history_clock = InvalidScn; break;
			case 6: sampler_proc_array->numProcs = -1; break;
			case 7: sampler_proc_array->numProcs = sampler_proc_array->maxProcs + 1; break;
			case 8: bounds_mrp_test_input(true, InvalidScn); break;
			case 9: bounds_mrp_test_input(true, UINT64_MAX); break;
			}
			memset(&history, 0xa5, sizeof(history));
			UT_ASSERT(!bounds_original_local_report(&history));
			UT_ASSERT(memcmp(&history, &zero, sizeof(history)) == 0);
			UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
				CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
			UT_ASSERT(bounds_acquire_status != CLUSTER_HEAP_FREEZE_COHORT_READY);
			UT_ASSERT(!v.shared_freeze_cohort_observed);
			UT_ASSERT(v.shared_freeze_input.freeze_policy == NULL && v.shared_freeze_input.freeze_cohort == NULL);
			f.input = v.shared_freeze_input;
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(bounds_history_locks, bounds_history_unlocks);
			UT_ASSERT_EQ(bounds_history_locks, bounds_history_clocks);
			UT_ASSERT(!bounds_history_held && !local_x && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes, 0);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_collector_original_history_mrp_fold_keeps_same_cut_attribution)
{
	for (unsigned variant = 0; variant < 7; variant++) {
		QualifiedXminFixture f; LVRelState v; ClusterUndoRetentionSample history, zero = {0};
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); bounds_observer_setup(&v, &f);
		bounds_mrp_test_input(true, variant == 0 ? InvalidScn : variant == 1 ? UINT64_MAX
			: variant == 2 ? 8 : variant == 4 ? 12 : 10);
		if (variant >= 5) {
			sampler_proc_array->pgprocnos[0] = 2;
			sampler_slots[2].xmin = 100; sampler_slots[2].pid = variant == 5 ? 42 : 0;
			pg_atomic_write_u64(&sampler_slots[2].cluster_read_scn_atomic, 10);
		}
		memset(&history, 0xa5, sizeof(history));
		if (variant < 2) {
			UT_ASSERT(!bounds_original_local_report(&history));
			UT_ASSERT(memcmp(&history, &zero, sizeof(history)) == 0);
		} else {
			UT_ASSERT(bounds_original_local_report(&history));
			UT_ASSERT_EQ(history.horizon_scn, variant == 2 ? 8 : 10);
			UT_ASSERT_EQ(history.clock_scn, 10);
			UT_ASSERT_EQ(history.contributor_kind, variant == 4
				? CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK : variant == 5
				? CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT
				: CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_REPLICATION);
			UT_ASSERT_EQ(history.contributor_token, variant == 5 ? 3 : 0);
		}
		UT_ASSERT_EQ(bounds_history_locks, 1); UT_ASSERT_EQ(bounds_history_unlocks, 1);
		UT_ASSERT_EQ(bounds_history_clocks, 1); UT_ASSERT(!bounds_history_held);
		bounds_assert_unlocked();
	}
}

UT_TEST(test_collector_original_history_minimum_reaches_native_creation_plan)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0};
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		f.cohort.horizon_scn = f.cohort.source_local_horizon_scn = 8;
		f.cohort.source_local_history.contributor_kind = CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT;
		f.cohort.source_local_history.contributor_token = 3;
		bounds_observer_setup(&v, &f); fixture_wal_clock = 5;
		sampler_proc_array->numProcs = 2; sampler_proc_array->pgprocnos[1] = 2;
		sampler_slots[2].xmin = 100; sampler_slots[2].pid = 42;
		pg_atomic_write_u64(&sampler_slots[2].cluster_read_scn_atomic, 8);
		UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
			CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		UT_ASSERT(v.shared_freeze_cohort_observed);
		UT_ASSERT_EQ(bounds.horizon_scn, 8);
		UT_ASSERT_EQ(v.shared_freeze_cohort.source_local_history.contributor_token, 3);
		f.input = v.shared_freeze_input;
		/* Creation SCN 9 is newer than the actually observed H=8. Do not use
		 * the convenience helpers' constant H=10 in this connection check. */
		UT_ASSERT(!(batch ? cluster_heap_freeze_apply_xmin_batch_v1(&relation_data, 1,
			&f.input, &f.page, &f.proofs, &f.fence, &f.authority, &f.admission,
			targets, 3, bounds.freeze_limit, bounds.horizon_scn)
			: cluster_heap_freeze_apply_xmin_v1(&relation_data, 1, &f.input,
			&f.page, &f.proofs, &f.fence, &f.authority, &f.admission, 0,
			FullTransactionIdFromEpochAndXid(1, 77), bounds.freeze_limit, bounds.horizon_scn)));
		UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes, 0);
		UT_ASSERT(!local_x && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_collector_original_history_unlock_registration_invalidates_cut)
{
	QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0};
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); bounds_observer_setup(&v, &f);
	bounds_history_register_on_unlock = true; fixture_wal_clock = 5;
	UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
		CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(bounds_history_locks, 1); UT_ASSERT_EQ(bounds_history_unlocks, 1);
	UT_ASSERT(!bounds_history_held && !bounds_history_register_on_unlock);
	UT_ASSERT(bounds_acquire_status != CLUSTER_HEAP_FREEZE_COHORT_READY);
	UT_ASSERT(!v.shared_freeze_cohort_observed); f.input = v.shared_freeze_input;
	UT_ASSERT(!qualified_xmin_apply(&f, 0));
	UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes, 0);
	UT_ASSERT(!local_x && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
}

UT_TEST(test_collector_original_mrp_config_and_shared_source_feed_history)
{
	for (unsigned mode = 0; mode < 8; mode++) {
		QualifiedXminFixture f; LVRelState v; ClusterUndoRetentionSample history, zero = {0};
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); bounds_observer_setup(&v, &f);
		bounds_mrp_adg = mode != 0;
		if (mode == 1) bounds_mrp_enabled = false;
		if (mode == 2) bounds_mrp_role = CLUSTER_DG_ROLE_PRIMARY;
		if (mode == 3) bounds_mrp_state = NULL;
		if (mode == 5) bounds_mrp_test_input(true, UINT64_MAX);
		if (mode == 6) bounds_original_mrp_publish_consistent_scn(8);
		if (mode == 7) bounds_mrp_role = -1;
		UT_ASSERT_EQ(bounds_original_mrp_should_start(), mode >= 3 && mode <= 6);
		memset(&history, 0xa5, sizeof(history));
		if (mode >= 3 && mode <= 5) {
			UT_ASSERT(!bounds_original_local_report(&history));
			UT_ASSERT(memcmp(&history, &zero, sizeof(history)) == 0);
		} else {
			UT_ASSERT(bounds_original_local_report(&history));
			UT_ASSERT_EQ(history.horizon_scn, mode == 6 ? 8 : 10);
			UT_ASSERT_EQ(history.clock_scn, 10);
			UT_ASSERT_EQ(history.contributor_kind, mode == 6
				? CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_REPLICATION
				: CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
			UT_ASSERT_EQ(history.contributor_token, 0);
		}
		UT_ASSERT_EQ(bounds_history_locks, 1); UT_ASSERT_EQ(bounds_history_unlocks, 1);
		bounds_assert_unlocked();
	}
}

UT_TEST(test_collector_original_mrp_publisher_preserves_monotone_watermark)
{
	QualifiedXminFixture f; LVRelState v; ClusterUndoRetentionSample history;
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); bounds_observer_setup(&v, &f);
	bounds_mrp_adg = true;
	bounds_original_mrp_publish_consistent_scn(InvalidScn);
	UT_ASSERT_EQ(bounds_original_mrp_consistent_scn(), InvalidScn);
	bounds_original_mrp_publish_consistent_scn(8);
	UT_ASSERT_EQ(bounds_original_mrp_consistent_scn(), 8);
	bounds_original_mrp_publish_consistent_scn(7);
	UT_ASSERT_EQ(bounds_original_mrp_consistent_scn(), 8);
	bounds_original_mrp_publish_consistent_scn(8);
	UT_ASSERT_EQ(bounds_original_mrp_consistent_scn(), 8);
	bounds_original_mrp_publish_consistent_scn(10);
	UT_ASSERT_EQ(bounds_original_mrp_consistent_scn(), 10);
	bounds_mrp_state = NULL;
	bounds_original_mrp_publish_consistent_scn(12);
	UT_ASSERT_EQ(bounds_original_mrp_consistent_scn(), InvalidScn);
	bounds_mrp_state = &bounds_mrp_storage;
	UT_ASSERT_EQ(bounds_original_mrp_consistent_scn(), 10);
	UT_ASSERT(bounds_original_local_report(&history));
	UT_ASSERT_EQ(history.horizon_scn, 10);
	UT_ASSERT_EQ(history.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_REPLICATION);
	UT_ASSERT_EQ(history.contributor_token, 0);
	bounds_assert_unlocked();
}

UT_TEST(test_collector_original_mrp_unknown_source_blocks_single_and_batch_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		for (unsigned mode = 0; mode < 3; mode++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0};
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			bounds_observer_setup(&v, &f); fixture_wal_clock = 5; bounds_mrp_adg = true;
			if (mode == 0) bounds_mrp_state = NULL;
			/* The original raw publisher is not a finite-H qualifier. Even a
			 * stored sentinel must fail the original local history contract. */
			if (mode == 2) bounds_original_mrp_publish_consistent_scn(UINT64_MAX);
			UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
				CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
			UT_ASSERT(bounds_acquire_status != CLUSTER_HEAP_FREEZE_COHORT_READY);
			UT_ASSERT(!v.shared_freeze_cohort_observed);
			UT_ASSERT(v.shared_freeze_input.freeze_policy == NULL && v.shared_freeze_input.freeze_cohort == NULL);
			f.input = v.shared_freeze_input;
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
			UT_ASSERT(!local_x && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes, 0);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_collector_original_mrp_publication_reaches_native_creation_and_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		for (SCN published = 8; published <= 10; published += 2) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0}; bool applied;
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			f.cohort.horizon_scn = f.cohort.source_local_horizon_scn = published;
			f.cohort.source_local_history.contributor_kind = CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_REPLICATION;
			f.cohort.source_local_history.contributor_token = 0;
			bounds_observer_setup(&v, &f); fixture_wal_clock = 5; bounds_mrp_adg = true;
			bounds_original_mrp_publish_consistent_scn(8);
			bounds_original_mrp_publish_consistent_scn(published);
			UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
				CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
			UT_ASSERT(v.shared_freeze_cohort_observed); UT_ASSERT_EQ(bounds.horizon_scn, published);
			UT_ASSERT_EQ(v.shared_freeze_cohort.source_local_history.contributor_kind,
				CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_REPLICATION);
			f.input = v.shared_freeze_input;
			applied = batch ? cluster_heap_freeze_apply_xmin_batch_v1(&relation_data, 1,
				&f.input, &f.page, &f.proofs, &f.fence, &f.authority, &f.admission,
				targets, 3, bounds.freeze_limit, bounds.horizon_scn)
				: cluster_heap_freeze_apply_xmin_v1(&relation_data, 1, &f.input,
				&f.page, &f.proofs, &f.fence, &f.authority, &f.admission, 0,
				FullTransactionIdFromEpochAndXid(1, 77), bounds.freeze_limit, bounds.horizon_scn);
			UT_ASSERT_EQ(applied, published == 10);
			UT_ASSERT_EQ(inserts, published == 10 ? 1 : 0);
			UT_ASSERT_EQ(flushes, published == 10 ? 1 : 0);
			UT_ASSERT_EQ(conditional_resumes, published == 10 ? 1 : 0);
			UT_ASSERT_EQ(conditional_unlocks, published == 10 ? 1 : 0);
			if (published == 10) UT_ASSERT_EQ(inserted_wal_clock, 10);
			else UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(backend_pin_count, 1);
		}
	}
}

UT_TEST(test_collector_original_mrp_source_lost_after_history_unlock_refuses)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0};
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		bounds_observer_setup(&v, &f); fixture_wal_clock = 5; bounds_mrp_adg = true;
		bounds_original_mrp_publish_consistent_scn(8); bounds_mrp_drop_on_history_unlock = true;
		UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
			CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		UT_ASSERT(bounds_mrp_state == NULL && !bounds_mrp_drop_on_history_unlock);
		UT_ASSERT_EQ(bounds_history_locks, 1); UT_ASSERT_EQ(bounds_history_unlocks, 1);
		UT_ASSERT(bounds_acquire_status != CLUSTER_HEAP_FREEZE_COHORT_READY);
		UT_ASSERT(!v.shared_freeze_cohort_observed); f.input = v.shared_freeze_input;
		UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
		UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes, 0);
		UT_ASSERT(!local_x && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_collector_saved_current_uses_original_sources_without_history_resample)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0};
		unsigned history_reads, native_reads, worker_gets;
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		bounds_observer_setup(&v, &f); fixture_wal_clock = 5;
		UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
			CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		UT_ASSERT(v.shared_freeze_cohort_observed);
		UT_ASSERT_EQ(bounds_saved_current_calls, 3);
		UT_ASSERT_EQ(bounds_saved_current_status, CLUSTER_HEAP_FREEZE_COHORT_READY);
		UT_ASSERT_EQ(bounds_acquire_history_reads, 1);
		history_reads = bounds_acquire_history_reads; native_reads = pass_native_reads;
		worker_gets = bounds_acquire_gets;
		UT_ASSERT_EQ(bounds_saved_cohort_current(&v.shared_freeze_cohort), CLUSTER_HEAP_FREEZE_COHORT_READY);
		UT_ASSERT_EQ(bounds_acquire_gets, worker_gets + 1);
		UT_ASSERT_EQ(bounds_acquire_history_reads, history_reads); UT_ASSERT_EQ(pass_native_reads, native_reads);
		f.input = v.shared_freeze_input;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT_EQ(conditional_resumes, 1); UT_ASSERT_EQ(conditional_unlocks, 1);
		UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_collector_saved_current_refuses_worker_lease_and_admission_drift)
{
	for (int batch = 0; batch < 2; batch++) {
		for (unsigned phase = 1; phase <= 3; phase++) {
			for (unsigned fault = 1; fault <= 4; fault++) {
				QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
				LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0};
				if (batch) qualified_xmin_batch_setup(&f, targets);
				else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
				bounds_observer_setup(&v, &f); fixture_wal_clock = 5;
				bounds_current_fault_at = phase; bounds_current_fault = fault;
				UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
					CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
				UT_ASSERT_EQ(bounds_saved_current_calls, phase);
				UT_ASSERT(bounds_saved_current_status != CLUSTER_HEAP_FREEZE_COHORT_READY);
				UT_ASSERT(!v.shared_freeze_cohort_observed);
				UT_ASSERT(v.shared_freeze_input.freeze_policy == NULL && v.shared_freeze_input.freeze_cohort == NULL);
				UT_ASSERT_EQ(bounds_acquire_history_reads, 1);
				f.input = v.shared_freeze_input;
				UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
				UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes, 0);
				UT_ASSERT(!local_x && !worker_lock_held && f.admission.entered);
				UT_ASSERT_EQ(backend_pin_count, 1);
				UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			}
		}
	}
}

UT_TEST(test_collector_saved_current_refuses_original_registration_changes)
{
	for (int batch = 0; batch < 2; batch++) {
		for (unsigned phase = 1; phase <= 3; phase++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0};
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			bounds_observer_setup(&v, &f); fixture_wal_clock = 5;
			bounds_current_fault_at = phase; bounds_current_fault = 5;
			UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
				CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
			UT_ASSERT_EQ(bounds_saved_current_calls, phase);
			UT_ASSERT(bounds_saved_current_status != CLUSTER_HEAP_FREEZE_COHORT_READY);
			UT_ASSERT(!v.shared_freeze_cohort_observed);
			UT_ASSERT(v.shared_freeze_input.freeze_policy == NULL && v.shared_freeze_input.freeze_cohort == NULL);
			UT_ASSERT_EQ(bounds_acquire_history_reads, 1);
			UT_ASSERT(!cluster_ctrc_active_scan_registration_current(&f.cohort.local_registration_cut));
			f.input = v.shared_freeze_input;
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
			UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes, 0);
			UT_ASSERT(!local_x && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_collector_saved_current_refuses_original_end_clock_staleness)
{
	for (int batch = 0; batch < 2; batch++) {
		for (unsigned phase = 1; phase <= 3; phase++) {
			for (unsigned fault = 6; fault <= 7; fault++) {
				QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
				LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0};
				if (batch) qualified_xmin_batch_setup(&f, targets);
				else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
				bounds_observer_setup(&v, &f); fixture_wal_clock = 5;
				bounds_current_fault_at = phase; bounds_current_fault = fault;
				UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
					CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
				UT_ASSERT_EQ(bounds_saved_current_calls, phase);
				UT_ASSERT_EQ(bounds_saved_current_status, CLUSTER_HEAP_FREEZE_COHORT_H_STALE);
				UT_ASSERT(!v.shared_freeze_cohort_observed); f.input = v.shared_freeze_input;
				UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
				UT_ASSERT_EQ(bounds_acquire_history_reads, 1);
				UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes, 0);
				UT_ASSERT(!local_x && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
				UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			}
		}
	}
}

UT_TEST(test_collector_saved_current_error_preserves_caller_page_pin_and_admission)
{
	for (int batch = 0; batch < 2; batch++) {
		for (unsigned phase = 1; phase <= 3; phase++) {
			for (unsigned fault = 8; fault <= 9; fault++) {
				QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
				LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0};
				if (batch) qualified_xmin_batch_setup(&f, targets);
				else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
				bounds_observer_setup(&v, &f); fixture_wal_clock = 5;
				bounds_current_fault_at = phase; bounds_current_fault = fault;
				expecting_error = true;
				if (setjmp(error_jump) == 0) {
					(void)lazy_observe_shared_freeze_bounds(&v, &f.admission,
						CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds);
					UT_ASSERT(false);
				}
				UT_ASSERT_EQ(bounds_saved_current_calls, phase);
				UT_ASSERT(!v.shared_freeze_cohort_observed);
				UT_ASSERT(v.shared_freeze_input.freeze_policy == NULL && v.shared_freeze_input.freeze_cohort == NULL);
				UT_ASSERT_EQ(bounds_acquire_history_reads, 1);
				UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes, 0);
				UT_ASSERT(!local_x && !worker_lock_held && f.admission.entered);
				UT_ASSERT_EQ(backend_pin_count, 1);
				UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
				expecting_error = false;
			}
		}
	}
}

UT_TEST(test_collector_saved_policy_original_fence_reaches_native_wal_without_resample)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0}; unsigned native_reads;
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		bounds_observer_setup(&v, &f); fixture_wal_clock = 5; native_reads = pass_native_reads;
		UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
			CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		UT_ASSERT(v.shared_freeze_cohort_observed && bounds_saved_policy_result);
		UT_ASSERT_EQ(bounds_saved_policy_calls, 3); UT_ASSERT_EQ(bounds_saved_policy_fences, 3);
		UT_ASSERT_EQ(bounds_saved_current_calls, 3); UT_ASSERT_EQ(bounds_acquire_history_reads, 1);
		UT_ASSERT_EQ(pass_native_reads, native_reads);
		UT_ASSERT_EQ(policy_current_checks + allocation_fence_checks, 0);
		f.input = v.shared_freeze_input;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(policy_current_checks, 1); UT_ASSERT_EQ(allocation_fence_checks, 1);
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT_EQ(conditional_resumes, 1); UT_ASSERT_EQ(conditional_unlocks, 1);
		UT_ASSERT_EQ(lease_lock_acquires, lease_lock_releases);
		UT_ASSERT(!lease_lock_held && !local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_collector_saved_policy_config_refusal_precedes_original_fence)
{
	for (int batch = 0; batch < 2; batch++) {
		for (unsigned phase = 1; phase <= 3; phase++) {
			for (unsigned fault = 1; fault <= 3; fault++) {
				QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
				LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0};
				if (batch) qualified_xmin_batch_setup(&f, targets);
				else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
				bounds_observer_setup(&v, &f); fixture_wal_clock = 5;
				bounds_policy_fault_at = phase; bounds_policy_fault = fault;
				UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
					CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
				UT_ASSERT_EQ(bounds_saved_policy_calls, phase);
				UT_ASSERT_EQ(bounds_saved_policy_fences, phase - 1);
				UT_ASSERT(!bounds_saved_policy_result && !v.shared_freeze_cohort_observed);
				UT_ASSERT(v.shared_freeze_input.freeze_policy == NULL && v.shared_freeze_input.freeze_cohort == NULL);
				f.input = v.shared_freeze_input;
				UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
				UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes, 0);
				UT_ASSERT_EQ(lease_lock_acquires, lease_lock_releases);
				UT_ASSERT(!lease_lock_held && !local_x && f.admission.entered);
				UT_ASSERT_EQ(backend_pin_count, 1);
				UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			}
		}
	}
}

UT_TEST(test_collector_saved_policy_original_lease_fields_reject_before_writer)
{
	for (int batch = 0; batch < 2; batch++) {
		for (unsigned phase = 1; phase <= 3; phase++) {
			for (unsigned fault = 4; fault <= 17; fault++) {
				QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
				LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0};
				if (batch) qualified_xmin_batch_setup(&f, targets);
				else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
				bounds_observer_setup(&v, &f); fixture_wal_clock = 5;
				bounds_policy_fault_at = phase; bounds_policy_fault = fault;
				UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
					CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
				UT_ASSERT_EQ(bounds_saved_policy_calls, phase); UT_ASSERT_EQ(bounds_saved_policy_fences, phase);
				UT_ASSERT(!bounds_saved_policy_result && !v.shared_freeze_cohort_observed);
				UT_ASSERT(v.shared_freeze_input.freeze_policy == NULL && v.shared_freeze_input.freeze_cohort == NULL);
				UT_ASSERT_EQ(bounds_acquire_history_reads, 1);
				f.input = v.shared_freeze_input;
				UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
				UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes, 0);
				UT_ASSERT_EQ(lease_lock_acquires, lease_lock_releases);
				UT_ASSERT(!lease_lock_held && !local_x && f.admission.entered);
				UT_ASSERT_EQ(backend_pin_count, 1);
				UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			}
		}
	}
}

UT_TEST(test_collector_saved_policy_rechecks_incarnation_and_quorum_after_unlock)
{
	for (int batch = 0; batch < 2; batch++) {
		for (unsigned phase = 1; phase <= 3; phase++) {
			for (unsigned fault = 18; fault <= 19; fault++) {
				QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
				LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0};
				if (batch) qualified_xmin_batch_setup(&f, targets);
				else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
				bounds_observer_setup(&v, &f); fixture_wal_clock = 5;
				bounds_policy_fault_at = phase; bounds_policy_fault = fault;
				UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
					CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
				UT_ASSERT_EQ(bounds_saved_policy_calls, phase); UT_ASSERT_EQ(bounds_saved_policy_fences, phase);
				UT_ASSERT(!bounds_saved_policy_result && !v.shared_freeze_cohort_observed);
				UT_ASSERT_EQ(lease_lock_releases, lease_revoke_after_unlock);
				f.input = v.shared_freeze_input;
				UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
				UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes, 0);
				UT_ASSERT_EQ(lease_lock_acquires, lease_lock_releases);
				UT_ASSERT(!lease_lock_held && !local_x && f.admission.entered);
				UT_ASSERT_EQ(backend_pin_count, 1);
				UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			}
		}
	}
}

UT_TEST(test_collector_saved_policy_error_leaves_original_lease_lock_and_caller_intact)
{
	for (int batch = 0; batch < 2; batch++) {
		for (unsigned phase = 1; phase <= 3; phase++) {
			for (unsigned fault = 20; fault <= 21; fault++) {
				QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
				LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0};
				if (batch) qualified_xmin_batch_setup(&f, targets);
				else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
				bounds_observer_setup(&v, &f); fixture_wal_clock = 5;
				bounds_policy_fault_at = phase; bounds_policy_fault = fault;
				expecting_error = true;
				if (setjmp(error_jump) == 0) {
					(void)lazy_observe_shared_freeze_bounds(&v, &f.admission,
						CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds);
					UT_ASSERT(false);
				}
				UT_ASSERT_EQ(bounds_saved_policy_calls, phase); UT_ASSERT_EQ(bounds_saved_policy_fences, phase);
				UT_ASSERT(!v.shared_freeze_cohort_observed);
				UT_ASSERT(v.shared_freeze_input.freeze_policy == NULL && v.shared_freeze_input.freeze_cohort == NULL);
				UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes, 0);
				UT_ASSERT_EQ(lease_lock_acquires, lease_lock_releases);
				UT_ASSERT(!lease_lock_held && !local_x && f.admission.entered);
				UT_ASSERT_EQ(backend_pin_count, 1);
				UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
				expecting_error = false;
			}
		}
	}
}

static void
bounds_assert_admission_refusal(const LVRelState *v, const QualifiedXminFixture *f)
{
	UT_ASSERT(!v->shared_freeze_cohort_observed);
	UT_ASSERT(v->shared_freeze_input.freeze_policy == NULL && v->shared_freeze_input.freeze_cohort == NULL);
	bounds_assert_unlocked();
	UT_ASSERT(f->admission.entered && !local_share && !resumed_writer);
	UT_ASSERT_EQ(lease_lock_acquires, lease_lock_releases); UT_ASSERT(!lease_lock_held);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
}

UT_TEST(test_collector_original_same_token_admission_reaches_native_wal)
{
	for (unsigned batch = 0; batch <= 1; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0}; unsigned native_reads;
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		bounds_observer_setup(&v, &f); fixture_wal_clock = 5; native_reads = pass_native_reads;
		UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
			CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		UT_ASSERT(v.shared_freeze_cohort_observed);
		UT_ASSERT(bounds_original_target_result && bounds_original_census_result);
		UT_ASSERT_EQ(bounds_original_target_calls, 1); UT_ASSERT_EQ(bounds_original_census_calls, 3);
		UT_ASSERT_EQ(bounds_saved_policy_calls, 3); UT_ASSERT_EQ(bounds_saved_current_calls, 3);
		UT_ASSERT_EQ(pass_native_reads, native_reads); UT_ASSERT_EQ(bounds_acquire_history_reads, 1);
		f.input = v.shared_freeze_input;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT_EQ(conditional_resumes, 1); UT_ASSERT_EQ(conditional_unlocks, 1);
		UT_ASSERT(!local_x && !resumed_writer && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

UT_TEST(test_collector_original_census_rejects_token_and_source_drift_at_each_check)
{
	for (unsigned batch = 0; batch <= 1; batch++) {
		for (unsigned phase = 1; phase <= 3; phase++) {
			for (unsigned fault = 2; fault <= 14; fault++) {
				QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
				LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0};
				if (fault == 13) continue; /* Descriptor presence belongs only to ordinary TARGET. */
				if (batch) qualified_xmin_batch_setup(&f, targets);
				else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
				bounds_observer_setup(&v, &f); fixture_wal_clock = 5;
				bounds_census_fault_at = phase; bounds_census_fault = fault;
				UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
					CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
				UT_ASSERT_EQ(bounds_original_census_calls, phase);
				UT_ASSERT(!bounds_original_census_result);
				bounds_assert_admission_refusal(&v, &f);
			}
		}
	}
}

UT_TEST(test_collector_census_read_permission_does_not_replace_original_target)
{
	const unsigned faults[] = { 1, 13 };
	for (unsigned batch = 0; batch <= 1; batch++) {
		for (unsigned i = 0; i < lengthof(faults); i++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0};
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			bounds_observer_setup(&v, &f); fixture_wal_clock = 5; bounds_target_fault = faults[i];
			UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
				CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
			UT_ASSERT_EQ(bounds_original_target_calls, 1); UT_ASSERT_EQ(bounds_original_census_calls, 3);
			UT_ASSERT(!bounds_original_target_result && bounds_original_census_result);
			UT_ASSERT_EQ(bounds_saved_policy_calls, 2); UT_ASSERT_EQ(bounds_saved_current_calls, 2);
			bounds_assert_admission_refusal(&v, &f);
		}
	}
}

UT_TEST(test_collector_original_admission_error_preserves_pin_and_entered_token)
{
	for (unsigned batch = 0; batch <= 1; batch++) {
		for (unsigned phase = 1; phase <= 4; phase++) {
			for (unsigned fault = 21; fault <= 23; fault++) {
				QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
				LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0};
				if (batch) qualified_xmin_batch_setup(&f, targets);
				else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
				bounds_observer_setup(&v, &f); fixture_wal_clock = 5;
				if (phase == 4) bounds_target_fault = fault;
				else { bounds_census_fault_at = phase; bounds_census_fault = fault; }
				expecting_error = true;
				if (setjmp(error_jump) == 0) {
					(void)lazy_observe_shared_freeze_bounds(&v, &f.admission,
						CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds);
					UT_ASSERT(false);
				}
				UT_ASSERT_EQ(bounds_original_census_calls, phase == 4 ? 2 : phase);
				UT_ASSERT_EQ(bounds_original_target_calls, phase >= 3 ? 1 : 0);
				bounds_assert_admission_refusal(&v, &f);
				expecting_error = false;
			}
		}
	}
}

UT_TEST(test_original_target_and_census_rechecks_reject_missing_token_without_source_reads)
{
	QualifiedXminFixture f; LVRelState v;
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); bounds_observer_setup(&v, &f);
	bounds_admission_error_kind = 1;
	UT_ASSERT(!bounds_original_target_current(NULL)); UT_ASSERT(!bounds_original_census_current(NULL));
	f.admission.entered = false;
	UT_ASSERT(!bounds_original_target_current(&f.admission));
	UT_ASSERT(!bounds_original_census_current(&f.admission));
	f.admission.entered = true; bounds_admission_error_kind = 0;
	UT_ASSERT(bounds_original_target_current(&f.admission));
	UT_ASSERT(bounds_original_census_current(&f.admission));
	bounds_assert_admission_refusal(&v, &f);
}

UT_TEST(test_collector_same_admission_rejects_wrong_target_side)
{
	for (unsigned batch = 0; batch <= 1; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0};
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		bounds_observer_setup(&v, &f);
		f.admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		f.admission.side = CLUSTER_SEMANTIC_SOURCE_SIDE;
		UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
			CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
		UT_ASSERT(!v.shared_freeze_cohort_observed);
		UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes, 0);
		UT_ASSERT(!local_x && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
	}
}

static void
native_original_admission_setup(QualifiedXminFixture *f, LVRelState *v,
	ClusterHeapFreezeXminTargetV1 targets[3], bool batch, bool preowned)
{
	unsigned char saved_ledger[sizeof(cluster_bufmgr_pcm_x_writer_ledger)];
	ClusterHeapFreezeObservedBoundsV1 bounds = {0};

	if (batch) qualified_xmin_batch_setup(f, targets);
	else qualified_xmin_setup(f, HEAP_XMAX_IS_MULTI);
	/* Retain the original fixture producer's ledger bytes.  Restore only the
	 * controlled preowned shape, without a new writer grant or acquisition. */
	memcpy(saved_ledger, cluster_bufmgr_pcm_x_writer_ledger, sizeof(saved_ledger));
	bounds_observer_setup(v, f); fixture_wal_clock = 5;
	UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(v, &f->admission,
		CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT(v->shared_freeze_cohort_observed); f->input = v->shared_freeze_input;
	if (preowned) {
		memcpy(cluster_bufmgr_pcm_x_writer_ledger, saved_ledger, sizeof(saved_ledger));
		local_x = true;
		UT_ASSERT(cluster_bufmgr_pcm_x_writer_find(GetBufferDescriptor(0)) != NULL);
	}
	native_original_admission_enabled = true; native_admission_expected_token = &f->admission;
}

static void
native_original_admission_assert_cleanup(const QualifiedXminFixture *f,
	bool preowned, unsigned stage)
{
	unsigned resumed = !preowned && stage >= 3;

	UT_ASSERT_EQ(conditional_resumes, resumed); UT_ASSERT_EQ(conditional_unlocks, resumed);
	UT_ASSERT_EQ(local_x, preowned); UT_ASSERT(!local_share && !resumed_writer);
	UT_ASSERT_EQ(cluster_bufmgr_pcm_x_writer_find(GetBufferDescriptor(0)) != NULL, preowned);
	UT_ASSERT(f->admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
	UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0); UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(lease_lock_acquires, lease_lock_releases); UT_ASSERT(!lease_lock_held);
	for (unsigned i = 1; i <= 6; i++)
		UT_ASSERT_EQ(native_admission_checks[i], i >= (preowned ? 3 : 1) && i <= stage);
}

UT_TEST(test_native_original_same_token_admission_preserves_new_and_preowned_writer)
{
	for (unsigned batch = 0; batch <= 1; batch++) {
		for (unsigned preowned = 0; preowned <= 1; preowned++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3]; LVRelState v;
			native_original_admission_setup(&f, &v, targets, batch, preowned);
			UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
			for (unsigned i = 1; i <= 6; i++)
				UT_ASSERT_EQ(native_admission_checks[i], !preowned || i >= 3);
			UT_ASSERT_EQ(conditional_resumes, !preowned); UT_ASSERT_EQ(conditional_unlocks, !preowned);
			UT_ASSERT_EQ(local_x, preowned); UT_ASSERT(!resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(cluster_bufmgr_pcm_x_writer_find(GetBufferDescriptor(0)) != NULL, preowned);
			UT_ASSERT_EQ(backend_pin_count, 1); UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
			UT_ASSERT_EQ(CritSectionCount, 0);
		}
	}
}

UT_TEST(test_native_original_admission_source_refusal_preserves_caller_ownership)
{
	for (unsigned batch = 0; batch <= 1; batch++) {
		for (unsigned preowned = 0; preowned <= 1; preowned++) {
			for (unsigned stage = preowned ? 3 : 1; stage <= 6; stage++) {
				for (unsigned fault = 1; fault <= 14; fault++) {
					QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3]; LVRelState v;
					if (stage % 2 == 0 && (fault == 1 || fault == 13)) continue;
					native_original_admission_setup(&f, &v, targets, batch, preowned);
					native_admission_fault_at = stage; native_admission_fault = fault;
					UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
					UT_ASSERT_EQ(space_observation_checks, stage >= 5);
					native_original_admission_assert_cleanup(&f, preowned, stage);
				}
			}
		}
	}
}

UT_TEST(test_native_original_admission_error_unwinds_only_new_writer_scope)
{
	for (unsigned batch = 0; batch <= 1; batch++) {
		for (unsigned preowned = 0; preowned <= 1; preowned++) {
			for (unsigned stage = preowned ? 3 : 1; stage <= 6; stage++) {
				for (unsigned fault = 21; fault <= 23; fault++) {
					QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3]; LVRelState v;
					native_original_admission_setup(&f, &v, targets, batch, preowned);
					native_admission_fault_at = stage; native_admission_fault = fault;
					expecting_error = true;
					if (setjmp(error_jump) == 0) {
						(void)(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
						UT_ASSERT(false);
					}
					UT_ASSERT_EQ(space_observation_checks, stage >= 5);
					native_original_admission_assert_cleanup(&f, preowned, stage);
					expecting_error = false;
				}
			}
		}
	}
}

UT_TEST(test_native_original_admission_rejects_wrong_side_before_writer_resume)
{
	for (unsigned batch = 0; batch <= 1; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0};
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		bounds_observer_setup(&v, &f); fixture_wal_clock = 5;
		UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
			CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		UT_ASSERT(v.shared_freeze_cohort_observed); f.input = v.shared_freeze_input;
		native_original_admission_enabled = true; native_admission_expected_token = &f.admission;
		f.admission.side = CLUSTER_SEMANTIC_SOURCE_SIDE;
		UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
		UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + inserts + edges + dirties + flushes, 0);
		UT_ASSERT(!local_x && !resumed_writer && f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_native_original_startup_serving_states_reach_single_native_wal)
{
	const ClusterNormalStartState states[] = { CLUSTER_NORMAL_START_EXISTING_OTHER,
		CLUSTER_NORMAL_START_SOURCE_ZERO, CLUSTER_NORMAL_START_TARGET_READY };
	for (unsigned batch = 0; batch <= 1; batch++) {
		for (unsigned preowned = 0; preowned <= 1; preowned++) {
			for (unsigned n = 0; n < lengthof(states); n++) {
				QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3]; LVRelState v;
				native_original_admission_setup(&f, &v, targets, batch, preowned);
				bounds_start_original_enabled = true; bounds_start_publish_state(states[n]);
				UT_ASSERT_EQ(bounds_original_start_state(), states[n]);
				UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
				for (unsigned i = 1; i <= 6; i++) UT_ASSERT_EQ(native_admission_checks[i], !preowned || i >= 3);
				UT_ASSERT_EQ(conditional_resumes, !preowned); UT_ASSERT_EQ(conditional_unlocks, !preowned);
				UT_ASSERT_EQ(local_x, preowned); UT_ASSERT(!resumed_writer && f.admission.entered);
				UT_ASSERT_EQ(backend_pin_count, 1); UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
				UT_ASSERT_EQ(CritSectionCount, 0);
			}
		}
	}
}

UT_TEST(test_native_original_startup_refuses_retired_or_unknown_state_at_each_cut)
{
	for (unsigned batch = 0; batch <= 1; batch++) {
		for (unsigned preowned = 0; preowned <= 1; preowned++) {
			for (unsigned stage = preowned ? 3 : 1; stage <= 6; stage++) {
				for (unsigned fault = 0; fault <= 4; fault++) {
					QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3]; LVRelState v;
					native_original_admission_setup(&f, &v, targets, batch, preowned);
					bounds_start_original_enabled = true;
					native_start_change_at = stage; native_start_fault = fault;
					UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
					UT_ASSERT_EQ(space_observation_checks, stage >= 5);
					native_original_admission_assert_cleanup(&f, preowned, stage);
				}
			}
		}
	}
}

UT_TEST(test_collector_original_startup_rechecks_state_before_borrowing_sources)
{
	for (unsigned batch = 0; batch <= 1; batch++) {
		for (unsigned phase = 1; phase <= 3; phase++) {
			for (unsigned fault = 0; fault <= 4; fault++) {
				QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
				LVRelState v; ClusterHeapFreezeObservedBoundsV1 bounds = {0};
				if (batch) qualified_xmin_batch_setup(&f, targets);
				else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
				bounds_observer_setup(&v, &f); fixture_wal_clock = 5;
				bounds_start_original_enabled = true;
				bounds_start_census_change_at = phase; bounds_start_census_fault = fault;
				UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
					CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
				UT_ASSERT_EQ(bounds_original_census_calls, phase); UT_ASSERT(!bounds_original_census_result);
				bounds_assert_admission_refusal(&v, &f);
			}
		}
	}
}

UT_TEST(test_original_startup_atomic_reader_and_generation_zero_bootstrap_contract)
{
	const ClusterNormalStartState states[] = { CLUSTER_NORMAL_START_UNCLASSIFIED,
		CLUSTER_NORMAL_START_EXISTING_OTHER, CLUSTER_NORMAL_START_SOURCE_ZERO,
		CLUSTER_NORMAL_START_TARGET_LOADING, CLUSTER_NORMAL_START_TARGET_READY,
		CLUSTER_NORMAL_START_FAILED, (ClusterNormalStartState)UINT32_MAX };
	for (unsigned n = 0; n <= lengthof(states); n++) {
		QualifiedXminFixture f; LVRelState v; ClusterNormalStartCompletion before;
		ClusterNormalStartState state = n == lengthof(states) ? CLUSTER_NORMAL_START_UNCLASSIFIED : states[n];
		bool serving = state == CLUSTER_NORMAL_START_EXISTING_OTHER || state == CLUSTER_NORMAL_START_SOURCE_ZERO
			|| state == CLUSTER_NORMAL_START_TARGET_READY;
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); bounds_observer_setup(&v, &f);
		bounds_start_original_enabled = true; bounds_start_publish_state(state);
		bounds_start_storage.system_identifier = UINT64_C(123456789);
		if (n == lengthof(states)) bounds_start_completion = NULL;
		memcpy(&before, &bounds_start_storage, sizeof(before));
		UT_ASSERT_EQ(bounds_original_start_state(), state);
		UT_ASSERT_EQ(bounds_original_start_serving(), serving);
		UT_ASSERT_EQ(bounds_original_start_bootstrap(), state == CLUSTER_NORMAL_START_SOURCE_ZERO);
		UT_ASSERT_EQ(bounds_original_target_current(&f.admission), serving);
		UT_ASSERT_EQ(bounds_original_census_current(&f.admission), serving);
		pg_atomic_write_u64(&native_admission_shared.record_generation, 0); f.admission.record_generation = 0;
		UT_ASSERT_EQ(bounds_original_target_current(&f.admission), state == CLUSTER_NORMAL_START_SOURCE_ZERO);
		UT_ASSERT_EQ(bounds_original_census_current(&f.admission), state == CLUSTER_NORMAL_START_SOURCE_ZERO);
		UT_ASSERT(memcmp(&before, &bounds_start_storage, sizeof(before)) == 0);
		bounds_assert_admission_refusal(&v, &f);
	}
}

UT_TEST(test_original_startup_nonshared_config_bypass_does_not_publish_or_freeze)
{
	for (unsigned shape = 0; shape < 3; shape++) {
		QualifiedXminFixture f; LVRelState v; ClusterNormalStartCompletion before;
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI); bounds_observer_setup(&v, &f);
		bounds_start_original_enabled = true; bounds_start_publish_state(CLUSTER_NORMAL_START_FAILED);
		memcpy(&before, &bounds_start_storage, sizeof(before));
		if (shape == 0) bounds_start_cluster_enabled = false;
		else bounds_start_shared_dir = shape == 1 ? NULL : "";
		UT_ASSERT(!bounds_original_start_shared_configured());
		UT_ASSERT(bounds_original_start_serving()); UT_ASSERT(bounds_original_start_bootstrap());
		UT_ASSERT_EQ(bounds_original_start_state(), CLUSTER_NORMAL_START_FAILED);
		UT_ASSERT(memcmp(&before, &bounds_start_storage, sizeof(before)) == 0);
		bounds_assert_admission_refusal(&v, &f);
	}
}

UT_TEST(test_native_original_startup_loading_refuses_without_releasing_caller_scope)
{
	for (unsigned batch = 0; batch <= 1; batch++) {
		for (unsigned preowned = 0; preowned <= 1; preowned++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3]; LVRelState v;
			native_original_admission_setup(&f, &v, targets, batch, preowned);
			bounds_start_original_enabled = true;
			bounds_start_publish_state(CLUSTER_NORMAL_START_TARGET_LOADING);
			UT_ASSERT(!(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0)));
			native_original_admission_assert_cleanup(&f, preowned, preowned ? 3 : 1);
		}
	}
}

/* Original admission debt and lifecycle; process registration and serial
 * publication/scheduling remain controlled leaves, not backend-exit proof. */
static uint32 semantic_activation_local_inflight[2][64];
static int semantic_activation_exit_hook_pid;
static int bounds_lifecycle_pid, bounds_lifecycle_holdoff;
static unsigned bounds_lifecycle_registrations, bounds_lifecycle_snapshots;
static unsigned bounds_lifecycle_snapshot_fault;
static void (*bounds_lifecycle_exit_callback)(int, Datum);
static Datum bounds_lifecycle_exit_arg;

static void
bounds_lifecycle_register_exit(void (*callback)(int, Datum), Datum arg)
{
	UT_ASSERT(callback != NULL); UT_ASSERT_EQ(bounds_lifecycle_holdoff, 0);
	bounds_lifecycle_exit_callback = callback; bounds_lifecycle_exit_arg = arg;
	bounds_lifecycle_registrations++;
}

static bool
bounds_lifecycle_snapshot(SemanticActivationAdmissionSnapshot *out)
{
	bounds_lifecycle_snapshots++;
	if (bounds_lifecycle_snapshots == 2 && bounds_lifecycle_snapshot_fault != 0) {
		int feature;
		UT_ASSERT(semantic_activation_feature_index(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, &feature));
		UT_ASSERT_EQ(semantic_activation_local_inflight[CLUSTER_SEMANTIC_TARGET_SIDE][feature], 1);
		UT_ASSERT_EQ(pg_atomic_read_u32(&native_admission_shared.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][feature]), 1);
		switch (bounds_lifecycle_snapshot_fault) {
		case 1: pg_atomic_fetch_add_u64(&native_admission_shared.admission_seq, 2); break;
		case 2: pg_atomic_fetch_add_u64(&native_admission_shared.record_generation, 1); break;
		case 3: pg_atomic_fetch_add_u64(&native_admission_shared.formation_epoch, 1); break;
		case 4: admission_epoch++; break;
		case 5: pg_atomic_write_u32(&native_admission_shared.transition_closed, 1); break;
		case 6: pg_atomic_write_u64(&native_admission_shared.admission_seq, 3); break;
		case 7: bounds_start_publish_state(CLUSTER_NORMAL_START_FAILED); break;
		default: UT_ASSERT(false); break;
		}
	}
	return semantic_activation_snapshot(out);
}

static bool bounds_original_backend_has_admission(void);
static ClusterSemanticAdmissionResult bounds_original_census_enter(ClusterSemanticAdmissionToken *);
static void bounds_original_census_leave(ClusterSemanticAdmissionToken *);

#define MyProcPid bounds_lifecycle_pid
#define InterruptHoldoffCount bounds_lifecycle_holdoff
#define on_shmem_exit bounds_lifecycle_register_exit
#define normal_start_serving_allowed bounds_admission_serving
#define normal_start_bootstrap_allowed bounds_admission_bootstrap
#define cluster_epoch_get_current bounds_admission_epoch
#define semantic_activation_snapshot bounds_lifecycle_snapshot
#define cluster_semantic_activation_enter_r4_terminal_census bounds_original_census_enter
#define cluster_semantic_activation_leave bounds_original_census_leave
#define cluster_semantic_activation_backend_has_admission bounds_original_backend_has_admission
#include "test_cluster_heap_freeze_admission_lifecycle.inc"
#undef cluster_semantic_activation_backend_has_admission
#undef cluster_semantic_activation_leave
#undef cluster_semantic_activation_enter_r4_terminal_census
#undef semantic_activation_snapshot
#undef cluster_epoch_get_current
#undef normal_start_bootstrap_allowed
#undef normal_start_serving_allowed
#undef on_shmem_exit
#undef InterruptHoldoffCount
#undef MyProcPid

static void
bounds_lifecycle_prepare(void)
{
	UT_ASSERT(!bounds_original_backend_has_admission()); UT_ASSERT_EQ(bounds_lifecycle_holdoff, 0);
	semantic_activation_exit_hook_pid = 0; bounds_lifecycle_pid = 4401;
	bounds_lifecycle_registrations = bounds_lifecycle_snapshots = bounds_lifecycle_snapshot_fault = 0;
	bounds_lifecycle_exit_callback = NULL; bounds_lifecycle_exit_arg = (Datum)0;
	for (unsigned side = 0; side < 2; side++)
		for (unsigned bit = 0; bit < 64; bit++)
			pg_atomic_init_u32(&native_admission_shared.inflight[side][bit], 0);
	bounds_start_original_enabled = true;
}

UT_TEST(test_original_census_enter_holds_debt_through_observer_and_native_wal)
{
	for (unsigned batch = 0; batch <= 1; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3]; LVRelState v;
		ClusterHeapFreezeObservedBoundsV1 bounds = {0}; int feature;
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		bounds_observer_setup(&v, &f); fixture_wal_clock = 5;
		bounds_lifecycle_prepare();
		memset(&f.admission, 0, sizeof(f.admission));
		UT_ASSERT_EQ(bounds_original_census_enter(&f.admission), CLUSTER_SEMANTIC_ADMISSION_OK);
		UT_ASSERT(semantic_activation_feature_index(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, &feature));
		UT_ASSERT_EQ(pg_atomic_read_u32(&native_admission_shared.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][feature]), 1);
		UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
			CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		f.input = v.shared_freeze_input;
		native_original_admission_enabled = true; native_admission_expected_token = &f.admission;
		UT_ASSERT(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
		UT_ASSERT(f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(!local_x && !resumed_writer); UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		bounds_original_census_leave(&f.admission);
		UT_ASSERT(!f.admission.entered);
		UT_ASSERT_EQ(pg_atomic_read_u32(&native_admission_shared.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][feature]), 0);
	}
}

static int
bounds_lifecycle_feature(void)
{
	int feature = -1;
	UT_ASSERT(semantic_activation_feature_bit_index(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, &feature));
	return feature;
}

static void
bounds_lifecycle_setup(QualifiedXminFixture *f, LVRelState *v,
	ClusterHeapFreezeXminTargetV1 targets[3], bool batch)
{
	if (batch) qualified_xmin_batch_setup(f, targets);
	else qualified_xmin_setup(f, HEAP_XMAX_IS_MULTI);
	bounds_observer_setup(v, f); fixture_wal_clock = 5;
	bounds_lifecycle_prepare(); memset(&f->admission, 0, sizeof(f->admission));
}

static void
bounds_lifecycle_assert_empty(const ClusterSemanticAdmissionToken *token)
{
	ClusterSemanticAdmissionToken zero = {0}; int feature = bounds_lifecycle_feature();
	UT_ASSERT(memcmp(token, &zero, sizeof(zero)) == 0);
	UT_ASSERT(!bounds_original_backend_has_admission()); UT_ASSERT_EQ(bounds_lifecycle_holdoff, 0);
	UT_ASSERT_EQ(semantic_activation_local_inflight[CLUSTER_SEMANTIC_TARGET_SIDE][feature], 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&native_admission_shared.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][feature]), 0);
}

UT_TEST(test_original_census_enter_refuses_before_debt_and_clears_output)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3];
		ClusterSemanticAdmissionResult result;
		bounds_lifecycle_setup(&f, &v, targets, false);
		memset(&f.admission, 0x5a, sizeof(f.admission));
		switch (fault) {
		case 0: bounds_lifecycle_pid = 0; break;
		case 1: SemanticActivationShmem = NULL; break;
		case 2: bounds_start_completion = NULL; break;
		case 3: bounds_start_publish_state(CLUSTER_NORMAL_START_UNCLASSIFIED); break;
		case 4: bounds_start_publish_state(CLUSTER_NORMAL_START_TARGET_LOADING); break;
		case 5: bounds_start_publish_state(CLUSTER_NORMAL_START_FAILED); break;
		case 6: bounds_admission_descriptors[bounds_lifecycle_feature()] = NULL; break;
		case 7: pg_atomic_write_u64(&native_admission_shared.record_generation, 0); break;
		case 8: admission_epoch++; break;
		case 9: pg_atomic_write_u32(&native_admission_shared.transition_closed, 1); break;
		}
		result = bounds_original_census_enter(&f.admission);
		UT_ASSERT_EQ(result, fault == 8 ? CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED : CLUSTER_SEMANTIC_ADMISSION_CLOSED);
		bounds_lifecycle_assert_empty(&f.admission);
		UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes, 0);
		UT_ASSERT_EQ(backend_pin_count, 1); UT_ASSERT(!local_x);
	}
	UT_ASSERT_EQ(bounds_original_census_enter(NULL), CLUSTER_SEMANTIC_ADMISSION_CLOSED);
}

UT_TEST(test_original_census_enter_rolls_back_increment_on_second_snapshot_refusal)
{
	for (unsigned fault = 1; fault <= 7; fault++) {
		QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3];
		bounds_lifecycle_setup(&f, &v, targets, false);
		bounds_lifecycle_snapshot_fault = fault;
		UT_ASSERT_EQ(bounds_original_census_enter(&f.admission), fault <= 4
			? CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED : CLUSTER_SEMANTIC_ADMISSION_CLOSED);
		UT_ASSERT_EQ(bounds_lifecycle_snapshots, 2); bounds_lifecycle_assert_empty(&f.admission);
		UT_ASSERT_EQ(bounds_lifecycle_registrations, 1);
		UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes, 0);
		UT_ASSERT_EQ(backend_pin_count, 1); UT_ASSERT(!local_x);
	}
}

UT_TEST(test_original_census_counter_saturation_preserves_existing_debt)
{
	for (unsigned local_full = 0; local_full <= 1; local_full++) {
		QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3]; int feature;
		bounds_lifecycle_setup(&f, &v, targets, false); feature = bounds_lifecycle_feature();
		UT_ASSERT(semantic_activation_ensure_exit_hook());
		pg_atomic_write_u32(&native_admission_shared.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][feature], UINT32_MAX);
		semantic_activation_local_inflight[CLUSTER_SEMANTIC_TARGET_SIDE][feature] = local_full ? UINT32_MAX : 0;
		UT_ASSERT_EQ(bounds_original_census_enter(&f.admission), CLUSTER_SEMANTIC_ADMISSION_CLOSED);
		UT_ASSERT(!f.admission.entered); UT_ASSERT_EQ(bounds_lifecycle_holdoff, 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&native_admission_shared.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][feature]), UINT32_MAX);
		UT_ASSERT_EQ(semantic_activation_local_inflight[CLUSTER_SEMANTIC_TARGET_SIDE][feature], local_full ? UINT32_MAX : 0);
		/* Dispose of the controlled saturation image, not a real admitted token. */
		pg_atomic_write_u32(&native_admission_shared.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][feature], 0);
		semantic_activation_local_inflight[CLUSTER_SEMANTIC_TARGET_SIDE][feature] = 0;
		bounds_lifecycle_assert_empty(&f.admission);
	}
}

UT_TEST(test_original_census_leave_is_once_and_preserves_other_admitted_debt)
{
	QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3];
	ClusterSemanticAdmissionToken other = {0}; int feature;
	bounds_lifecycle_setup(&f, &v, targets, false); feature = bounds_lifecycle_feature();
	pg_atomic_write_u32(&native_admission_shared.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][feature], 7);
	UT_ASSERT_EQ(bounds_original_census_enter(&f.admission), CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT_EQ(bounds_original_census_enter(&other), CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT_EQ(bounds_lifecycle_registrations, 1); UT_ASSERT(bounds_original_backend_has_admission());
	UT_ASSERT_EQ(semantic_activation_local_inflight[CLUSTER_SEMANTIC_TARGET_SIDE][feature], 2);
	bounds_original_census_leave(&f.admission); bounds_original_census_leave(&f.admission);
	bounds_original_census_leave(NULL);
	UT_ASSERT(!f.admission.entered && other.entered); UT_ASSERT(bounds_original_backend_has_admission());
	UT_ASSERT_EQ(semantic_activation_local_inflight[CLUSTER_SEMANTIC_TARGET_SIDE][feature], 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&native_admission_shared.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][feature]), 8);
	bounds_original_census_leave(&other); UT_ASSERT(!bounds_original_backend_has_admission());
	UT_ASSERT_EQ(pg_atomic_read_u32(&native_admission_shared.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][feature]), 7);
	UT_ASSERT_EQ(bounds_lifecycle_holdoff, 0);
}

UT_TEST(test_original_census_debt_survives_observer_and_native_error_until_owner_leave)
{
	for (unsigned batch = 0; batch <= 1; batch++) {
		for (unsigned stage = 0; stage <= 6; stage++) {
			QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3];
			ClusterHeapFreezeObservedBoundsV1 bounds = {0}; int feature;
			bounds_lifecycle_setup(&f, &v, targets, batch); feature = bounds_lifecycle_feature();
			UT_ASSERT_EQ(bounds_original_census_enter(&f.admission), CLUSTER_SEMANTIC_ADMISSION_OK);
			if (stage == 0) { bounds_census_fault_at = 1; bounds_census_fault = 21; }
			else {
				UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
					CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
				f.input = v.shared_freeze_input; native_original_admission_enabled = true;
				native_admission_expected_token = &f.admission;
				native_admission_fault_at = stage; native_admission_fault = 21;
			}
			expecting_error = true;
			if (setjmp(error_jump) == 0) {
				if (stage == 0) (void)lazy_observe_shared_freeze_bounds(&v, &f.admission,
					CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds);
				else (void)(batch ? qualified_xmin_batch_apply(&f, targets, 3) : qualified_xmin_apply(&f, 0));
				UT_ASSERT(false);
			}
			expecting_error = false;
			UT_ASSERT(f.admission.entered); UT_ASSERT(bounds_original_backend_has_admission());
			UT_ASSERT_EQ(semantic_activation_local_inflight[CLUSTER_SEMANTIC_TARGET_SIDE][feature], 1);
			UT_ASSERT_EQ(pg_atomic_read_u32(&native_admission_shared.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][feature]), 1);
			UT_ASSERT_EQ(backend_pin_count, 1); UT_ASSERT(!local_x && !resumed_writer);
			UT_ASSERT_EQ(conditional_resumes, stage >= 3); UT_ASSERT_EQ(conditional_unlocks, stage >= 3);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0); UT_ASSERT_EQ(CritSectionCount, 0);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			/* Explicit owner action: not an assertion that the real collector's
			 * PG_FINALLY or a live query-cancel path ran in this fixture. */
			bounds_original_census_leave(&f.admission); bounds_lifecycle_assert_empty(&f.admission);
		}
	}
}

UT_TEST(test_original_census_exit_callback_releases_only_registered_local_debt)
{
	QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3];
	ClusterSemanticAdmissionToken other = {0}; int feature;
	bounds_lifecycle_setup(&f, &v, targets, false); feature = bounds_lifecycle_feature();
	pg_atomic_write_u32(&native_admission_shared.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][feature], 7);
	pg_atomic_write_u32(&native_admission_shared.inflight[CLUSTER_SEMANTIC_SOURCE_SIDE][0], 11);
	UT_ASSERT_EQ(bounds_original_census_enter(&f.admission), CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT_EQ(bounds_original_census_enter(&other), CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(bounds_lifecycle_exit_callback != NULL); UT_ASSERT_EQ(bounds_lifecycle_registrations, 1);
	bounds_lifecycle_exit_callback(0, Int32GetDatum(bounds_lifecycle_pid + 1));
	bounds_lifecycle_pid++;
	bounds_lifecycle_exit_callback(0, bounds_lifecycle_exit_arg);
	UT_ASSERT_EQ(semantic_activation_local_inflight[CLUSTER_SEMANTIC_TARGET_SIDE][feature], 2);
	UT_ASSERT_EQ(pg_atomic_read_u32(&native_admission_shared.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][feature]), 9);
	bounds_lifecycle_pid--;
	bounds_lifecycle_exit_callback(0, bounds_lifecycle_exit_arg);
	bounds_lifecycle_exit_callback(0, bounds_lifecycle_exit_arg);
	UT_ASSERT(!bounds_original_backend_has_admission()); UT_ASSERT_EQ(semantic_activation_exit_hook_pid, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&native_admission_shared.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][feature]), 7);
	UT_ASSERT_EQ(pg_atomic_read_u32(&native_admission_shared.inflight[CLUSTER_SEMANTIC_SOURCE_SIDE][0]), 11);
	UT_ASSERT_EQ(bounds_lifecycle_holdoff, 0);
	/* Real backend exit discards tokens; do not leave stale copies after callback. */
	memset(&f.admission, 0, sizeof(f.admission)); memset(&other, 0, sizeof(other));
}

UT_TEST(test_original_census_entry_permission_does_not_grant_native_writer)
{
	QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3];
	ClusterHeapFreezeObservedBoundsV1 bounds = {0};
	bounds_lifecycle_setup(&f, &v, targets, false);
	pg_atomic_write_u64(&native_admission_shared.active_bits, 0);
	UT_ASSERT_EQ(bounds_original_census_enter(&f.admission), CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(bounds_original_census_current(&f.admission)); UT_ASSERT(!bounds_original_target_current(&f.admission));
	UT_ASSERT_EQ(lazy_observe_shared_freeze_bounds(&v, &f.admission,
		CLUSTER_HEAP_FREEZE_PROOF_PROVEN, &bounds), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	f.input = v.shared_freeze_input;
	UT_ASSERT(!qualified_xmin_apply(&f, 0)); UT_ASSERT(!v.shared_freeze_cohort_observed);
	UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes, 0);
	UT_ASSERT_EQ(backend_pin_count, 1); UT_ASSERT(!local_x);
	bounds_original_census_leave(&f.admission); bounds_lifecycle_assert_empty(&f.admission);
}

/* The proof transport and resident recheck are explicit leaves here. The
 * admitted phase, canonical binding/selection, observer and native writer/WAL
 * entry are original bodies. This does not qualify their production sources. */
static QualifiedXminFixture *phase_source;
static unsigned phase_preflights, phase_resolves, phase_rechecks;
static unsigned phase_error_stage, phase_finally_calls, phase_finally_debt;
static unsigned phase_finally_pins;
static bool phase_finally_owned_writer;
static bool phase_mixed_proof_subset;

static void
phase_preflight(void)
{
	UT_ASSERT(bounds_expected_admission()->entered && bounds_original_backend_has_admission());
	UT_ASSERT(!local_x && !local_share && backend_pin_count == 0);
	phase_preflights++;
	if (phase_error_stage == 1) elog(ERROR, "injected admitted preflight ERROR");
}

static ClusterTxOutcome
phase_resolve(const ClusterTxLocator *locator, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *token, ClusterTxResolution *out,
	ClusterTxResolveReason *reason)
{
	const ClusterHeapFreezeCanonicalProofV1 *proof;
	UT_ASSERT(token == bounds_expected_admission() && token->entered);
	UT_ASSERT_EQ(mode, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS);
	UT_ASSERT(phase_resolves < phase_source->proofs.proof_count);
	proof = &phase_source->proofs.proofs[phase_resolves++];
	UT_ASSERT(memcmp(locator, &proof->requested_locator, sizeof(*locator)) == 0);
	if (phase_error_stage == 2) elog(ERROR, "injected admitted resolver ERROR");
	*out = proof->resolution; *reason = CLUSTER_TX_RESOLVE_NONE;
	if (phase_mixed_proof_subset && phase_resolves == 2)
		out->outcome = CLUSTER_TX_UNKNOWN;
	return out->outcome;
}

static ClusterTxOutcome
phase_retained_resolve(const ClusterTxLocator *locator, SCN scn,
	const ClusterSemanticAdmissionToken *token, ClusterTxResolution *out,
	ClusterTxResolveReason *reason)
{
	(void)locator; (void)scn; (void)token; (void)out; (void)reason;
	UT_ASSERT(false); /* These original ACTIVE carriers never use that path. */
	return CLUSTER_TX_UNKNOWN;
}

static ClusterHeapFreezeProofStatusV1
phase_real_resident_recheck(LVRelState *, Buffer, const PcmAuthoritySnapshot *,
	const ClusterSemanticAdmissionToken *, ClusterHeapFreezeProofBatchV1 *,
	const ClusterTxResolution *, uint8, ClusterHeapFreezeFinalFenceV1 *, bool *, bool *);

static ClusterHeapFreezeProofStatusV1
phase_resident_recheck(LVRelState *v, Buffer buffer,
	const PcmAuthoritySnapshot *authority, const ClusterSemanticAdmissionToken *token,
	ClusterHeapFreezeProofBatchV1 *proofs, const ClusterTxResolution *retained,
	uint8 retained_mask, ClusterHeapFreezeFinalFenceV1 *current,
	bool *cleaned, bool *rows_current)
{
	if (phase_use_original_resident) {
		phase_rechecks++;
		return phase_real_resident_recheck(v, buffer, authority, token, proofs,
			retained, retained_mask, current, cleaned, rows_current);
	}
	(void)retained;
	UT_ASSERT(v != NULL && buffer == 1);
	UT_ASSERT(bounds_borrowed_admission != NULL
		? memcmp(authority, &phase_source->authority, sizeof(*authority)) == 0
		: authority == &phase_source->authority);
	UT_ASSERT(token == bounds_expected_admission() && token->entered);
	UT_ASSERT(!local_x && !local_share && backend_pin_count == 0);
	UT_ASSERT_EQ(retained_mask, 0); UT_ASSERT_EQ(proofs->proof_count, phase_resolves);
	phase_rechecks++;
	if (phase_error_stage == 3) elog(ERROR, "injected admitted resident check ERROR");
	for (unsigned i = 0; i < proofs->proof_count; i++) {
		ClusterHeapFreezeProofStatusV1 status =
			cluster_heap_freeze_validate_canonical_proof_v1(&proofs->proofs[i]);
		if (status != CLUSTER_HEAP_FREEZE_PROOF_PROVEN) return status;
	}
	*current = proofs->proofs[0].current_fence;
	*cleaned = false; *rows_current = true;
	return CLUSTER_HEAP_FREEZE_PROOF_PROVEN;
}

#define cluster_tx_resolve_terminal_census_batch_preflight phase_preflight
#define cluster_tx_resolve_exact_admitted phase_resolve
#define cluster_tx_resolve_terminal_census_retained_admitted phase_retained_resolve
#define lazy_recheck_shared_freeze_page phase_resident_recheck
#define lazy_collect_shared_freeze_proofs_admitted phase_actual_admitted
#include "test_cluster_vacuum_admitted_phase.inc"
#undef lazy_collect_shared_freeze_proofs_admitted
#undef lazy_recheck_shared_freeze_page
#undef cluster_tx_resolve_terminal_census_retained_admitted
#undef cluster_tx_resolve_exact_admitted
#undef cluster_tx_resolve_terminal_census_batch_preflight

/* Legacy phase/native cases borrow a controlled fixture pin around the
 * phase. Model its release for proof callbacks and restore only that model
 * before the separately tested pinned native API. No production pin API or
 * qualification is replaced: phase_actual_admitted is the complete body. */
static ClusterHeapFreezeProofStatusV1
lazy_collect_shared_freeze_proofs_admitted(LVRelState *v, Buffer buffer,
	const PcmAuthoritySnapshot *authority, uint64 master,
	const ClusterSemanticAdmissionToken *token, bool *rows_current)
{
	volatile ClusterHeapFreezeProofStatusV1 status = CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN;
	UT_ASSERT_EQ(backend_pin_count, 1);
	UT_ASSERT(!local_x && !local_share && !phase_pin_released);
	backend_pin_count = 0; phase_pin_released = true;
	PG_TRY();
	{
		status = phase_actual_admitted(v, buffer, authority, master, token, rows_current);
	}
	PG_FINALLY();
	{
		UT_ASSERT_EQ(backend_pin_count, 0);
		phase_pin_released = false; backend_pin_count = 1;
	}
	PG_END_TRY();
	return status;
}

static void
phase_setup(QualifiedXminFixture *f, LVRelState *v,
	ClusterHeapFreezeXminTargetV1 targets[3], bool batch)
{
	bounds_lifecycle_setup(f, v, targets, batch);
	phase_source = f; phase_preflights = phase_resolves = phase_rechecks = 0;
	phase_error_stage = phase_finally_calls = phase_finally_debt = phase_finally_pins = 0;
	phase_finally_owned_writer = false;
	v->shared_freeze_batch = f->page;
	UT_ASSERT_EQ(bounds_original_census_enter(&f->admission), CLUSTER_SEMANTIC_ADMISSION_OK);
}

static void
phase_copy_to_native(QualifiedXminFixture *f, const LVRelState *v)
{
	f->input = v->shared_freeze_input; f->page = v->shared_freeze_batch;
	f->proofs = v->shared_freeze_proofs; f->fence = v->shared_freeze_fence;
	native_original_admission_enabled = true; native_admission_expected_token = &f->admission;
}

/* A fixture-owned continuation, not a new production VACUUM caller. Its
 * original admission owner spans both real phases and rethrows after leave. */
static bool
phase_then_native_owned(QualifiedXminFixture *f, LVRelState *v)
{
	volatile bool applied = false;
	PG_TRY();
	{
		bool rows_current = false;
		ClusterHeapFreezeProofStatusV1 status = lazy_collect_shared_freeze_proofs_admitted(v,
			1, &f->authority, f->fence.master_sequence, &f->admission, &rows_current);
		if (status == CLUSTER_HEAP_FREEZE_PROOF_PROVEN && rows_current) {
			phase_copy_to_native(f, v);
			applied = qualified_xmin_batch_apply(f, v->shared_freeze_xmin_candidates.targets,
				v->shared_freeze_xmin_candidates.target_count);
		}
	}
	PG_FINALLY();
	{
		phase_finally_calls++;
		phase_finally_debt = pg_atomic_read_u32(&native_admission_shared.inflight
			[CLUSTER_SEMANTIC_TARGET_SIDE][bounds_lifecycle_feature()]);
		phase_finally_pins = backend_pin_count;
		phase_finally_owned_writer = local_x || resumed_writer;
		bounds_original_census_leave(&f->admission);
	}
	PG_END_TRY();
	return applied;
}

UT_TEST(test_admitted_proof_phase_keeps_original_owner_through_native_wal)
{
	for (unsigned batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3];
		bool rows_current = false;
		phase_setup(&f, &v, targets, batch != 0);
		UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs_admitted(&v, 1, &f.authority,
			f.fence.master_sequence, &f.admission, &rows_current), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		UT_ASSERT(rows_current && v.shared_freeze_fence_valid && f.admission.entered);
		UT_ASSERT_EQ(phase_preflights, 1); UT_ASSERT_EQ(phase_rechecks, 1);
		UT_ASSERT_EQ(phase_resolves, f.page.carrier_request_count);
		UT_ASSERT_EQ(pg_atomic_read_u32(&native_admission_shared.inflight
			[CLUSTER_SEMANTIC_TARGET_SIDE][bounds_lifecycle_feature()]), 1);
		UT_ASSERT(v.shared_freeze_xmin_candidates.target_count > 0);
		phase_copy_to_native(&f, &v);
		UT_ASSERT(qualified_xmin_batch_apply(&f, v.shared_freeze_xmin_candidates.targets,
			v.shared_freeze_xmin_candidates.target_count));
		UT_ASSERT(f.admission.entered); UT_ASSERT_EQ(backend_pin_count, 1);
		UT_ASSERT(!local_x && !resumed_writer); UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		bounds_original_census_leave(&f.admission); bounds_lifecycle_assert_empty(&f.admission);
	}
}

UT_TEST(test_admitted_proof_cannot_continue_native_wal_after_original_owner_leave)
{
	for (unsigned batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3];
		bool rows_current = false;
		phase_setup(&f, &v, targets, batch != 0);
		UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs_admitted(&v, 1, &f.authority,
			f.fence.master_sequence, &f.admission, &rows_current), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		UT_ASSERT(rows_current && v.shared_freeze_xmin_candidates.target_count > 0);
		phase_copy_to_native(&f, &v);
		bounds_original_census_leave(&f.admission); bounds_lifecycle_assert_empty(&f.admission);
		UT_ASSERT(!qualified_xmin_batch_apply(&f, v.shared_freeze_xmin_candidates.targets,
			v.shared_freeze_xmin_candidates.target_count));
		UT_ASSERT_EQ(inserts + edges + dirties + flushes + conditional_resumes, 0);
		UT_ASSERT_EQ(memcmp(current_page.data, before_page.data, BLCKSZ), 0);
		UT_ASSERT(!local_x && !resumed_writer); UT_ASSERT_EQ(backend_pin_count, 1);
		bounds_lifecycle_assert_empty(&f.admission);
	}
}

UT_TEST(test_admitted_proof_and_native_errors_rethrow_after_original_owner_finally)
{
	for (unsigned batch = 0; batch < 2; batch++) {
		for (unsigned stage = 1; stage <= 4; stage++) {
			QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3];
			phase_setup(&f, &v, targets, batch != 0);
			if (stage == 4) error_after_ledger_bind = true;
			else phase_error_stage = stage;
			expecting_error = true;
			if (setjmp(error_jump) == 0) {
				(void)phase_then_native_owned(&f, &v);
				UT_ASSERT(false);
			}
			UT_ASSERT_EQ(phase_finally_calls, 1); UT_ASSERT_EQ(phase_finally_debt, 1);
			UT_ASSERT_EQ(phase_finally_pins, 1); UT_ASSERT(!phase_finally_owned_writer);
			UT_ASSERT_EQ(phase_preflights, 1);
			UT_ASSERT_EQ(phase_rechecks, stage >= 3);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
			UT_ASSERT_EQ(memcmp(current_page.data, before_page.data, BLCKSZ), 0);
			UT_ASSERT(!local_x && !resumed_writer); UT_ASSERT_EQ(backend_pin_count, 1);
			UT_ASSERT(cluster_bufmgr_pcm_x_writer_find(GetBufferDescriptor(0)) == NULL);
			bounds_lifecycle_assert_empty(&f.admission);
			UT_ASSERT(PG_exception_stack == NULL && error_context_stack == NULL);
			expecting_error = false;
		}
	}
}

static bool
fixture_recent_xmin_apply(QualifiedXminFixture *f,
	const ClusterHeapFreezeXminTargetV1 *targets, uint16 count)
{
	recent_pin_admission = &f->admission;
	recent_caller_local_x = local_x; recent_caller_writer = resumed_writer;
	recent_caller_share = local_share; recent_caller_pins = backend_pin_count;
	return cluster_heap_freeze_apply_xmin_batch_recent_v1(&relation_data, 1,
		&f->input, &f->page, &f->proofs, &f->fence, &f->authority,
		&f->admission, targets, count, FullTransactionIdFromEpochAndXid(1, 100), 10);
}

static void phase_recent_native_begin(const LVRelState *v);

/* The original collector phase now forbids a caller-owned heap pin. Join
 * its complete body directly to the recent-resident native entry, without
 * the older fixture wrapper's synthetic restoration of a pinned caller. */
static bool
phase_then_recent_native_owned(QualifiedXminFixture *f, LVRelState *v)
{
	volatile bool applied = false;

	UT_ASSERT_EQ(backend_pin_count, 0);
	UT_ASSERT(!local_x && !local_share && !phase_pin_released);
	PG_TRY();
	{
		bool rows_current = false;
		ClusterHeapFreezeProofStatusV1 status;

		phase_pin_released = true;
		status = phase_actual_admitted(v, 1, &f->authority,
			f->fence.master_sequence, &f->admission, &rows_current);
		phase_pin_released = false;
		UT_ASSERT_EQ(backend_pin_count, 0);
		if ((status == CLUSTER_HEAP_FREEZE_PROOF_PROVEN
			 || (phase_mixed_proof_subset && status == CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN))
			&& rows_current && v->shared_freeze_xmin_candidates.target_count > 0)
		{
			if (phase_use_original_recent_pin && phase_use_original_resident)
				phase_recent_native_begin(v);
			phase_copy_to_native(f, v);
			applied = fixture_recent_xmin_apply(f,
				v->shared_freeze_xmin_candidates.targets,
				v->shared_freeze_xmin_candidates.target_count);
		}
	}
	PG_FINALLY();
	{
		phase_pin_released = false;
		phase_finally_calls++;
		phase_finally_debt = pg_atomic_read_u32(&native_admission_shared.inflight
			[CLUSTER_SEMANTIC_TARGET_SIDE][bounds_lifecycle_feature()]);
		phase_finally_pins = backend_pin_count;
		phase_finally_owned_writer = local_x || resumed_writer;
		bounds_original_census_leave(&f->admission);
	}
	PG_END_TRY();
	return applied;
}

UT_TEST(test_pin_free_admitted_phase_reaches_recent_native_wal_before_owner_leave)
{
	for (unsigned batch = 0; batch < 2; batch++)
	{
		QualifiedXminFixture f;
		LVRelState v;
		ClusterHeapFreezeXminTargetV1 targets[3];

		phase_setup(&f, &v, targets, batch != 0);
		/* Model the caller's already completed release, never restore it. */
		backend_pin_count = 0;
		relation_data.rd_isvalid = resume_allowed = true;
		UT_ASSERT(phase_then_recent_native_owned(&f, &v));
		UT_ASSERT_EQ(phase_preflights, 1);
		UT_ASSERT_EQ(phase_resolves, f.page.carrier_request_count);
		UT_ASSERT_EQ(phase_rechecks, 1);
		UT_ASSERT_EQ(recent_pin_calls, 1); UT_ASSERT_EQ(recent_unpins, 1);
		UT_ASSERT_EQ(conditional_resumes, 1); UT_ASSERT_EQ(conditional_unlocks, 1);
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT_EQ(phase_finally_calls, 1); UT_ASSERT_EQ(phase_finally_debt, 1);
		UT_ASSERT_EQ(phase_finally_pins, 0); UT_ASSERT(!phase_finally_owned_writer);
		UT_ASSERT_EQ(backend_pin_count, 0);
		UT_ASSERT(!recent_pin_held && !local_x && !resumed_writer);
		bounds_lifecycle_assert_empty(&f.admission);
	}
}

UT_TEST(test_pin_free_phase_recent_refusal_retires_only_its_original_owner)
{
	for (unsigned batch = 0; batch < 2; batch++)
	{
		for (unsigned fault = 0; fault < 3; fault++)
		{
			QualifiedXminFixture f;
			LVRelState v;
			ClusterHeapFreezeXminTargetV1 targets[3];

			phase_setup(&f, &v, targets, batch != 0);
			backend_pin_count = 0;
			relation_data.rd_isvalid = true;
			recent_pin_allowed = fault != 0; resume_allowed = fault != 1;
			if (fault == 2) authority_current = false;
			UT_ASSERT(!phase_then_recent_native_owned(&f, &v));
			UT_ASSERT_EQ(phase_preflights, 1); UT_ASSERT_EQ(phase_rechecks, 1);
			UT_ASSERT_EQ(recent_pin_calls, 1); UT_ASSERT_EQ(recent_unpins, fault != 0);
			UT_ASSERT_EQ(phase_finally_calls, 1); UT_ASSERT_EQ(phase_finally_debt, 1);
			UT_ASSERT_EQ(phase_finally_pins, 0); UT_ASSERT(!phase_finally_owned_writer);
			UT_ASSERT_EQ(backend_pin_count, 0);
			UT_ASSERT(!recent_pin_held && !local_x && !resumed_writer);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			bounds_lifecycle_assert_empty(&f.admission);
		}
	}
}

UT_TEST(test_pin_free_phase_recent_errors_unwind_pin_before_original_owner_leave)
{
	for (unsigned batch = 0; batch < 2; batch++)
	{
		for (unsigned stage = 1; stage <= 5; stage++)
		{
			/* The longjmp modifies pointees, not automatic fixture objects. */
			QualifiedXminFixture *f = calloc(1, sizeof(*f));
			LVRelState *v = calloc(1, sizeof(*v));
			ClusterHeapFreezeXminTargetV1 targets[3];

			UT_ASSERT(f != NULL && v != NULL);
			phase_setup(f, v, targets, batch != 0);
			backend_pin_count = 0;
			relation_data.rd_isvalid = resume_allowed = true;
			if (stage <= 3) phase_error_stage = stage;
			if (stage == 4) recent_pin_error = true;
			if (stage == 5) error_after_ledger_bind = true;
			expecting_error = true;
			if (setjmp(error_jump) == 0)
			{
				(void) phase_then_recent_native_owned(f, v);
				UT_ASSERT(false);
			}
			expecting_error = false;
			UT_ASSERT_EQ(phase_finally_calls, 1); UT_ASSERT_EQ(phase_finally_debt, 1);
			UT_ASSERT_EQ(phase_finally_pins, 0); UT_ASSERT(!phase_finally_owned_writer);
			UT_ASSERT_EQ(recent_pin_calls, stage >= 4);
			UT_ASSERT_EQ(recent_unpins, stage == 5);
			UT_ASSERT_EQ(backend_pin_count, 0);
			UT_ASSERT(!recent_pin_held && !local_x && !resumed_writer);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			UT_ASSERT(cluster_bufmgr_pcm_x_writer_find(GetBufferDescriptor(0)) == NULL);
			UT_ASSERT(PG_exception_stack == NULL && error_context_stack == NULL);
			bounds_lifecycle_assert_empty(&f->admission);
			free(v); free(f);
		}
	}
}

/* A controlled interleaving at the resident I/O leaf invokes the complete
 * original foreign receipt publisher. Its authentication/storage leaves are
 * controlled; the registration-generation change comes from the real body. */
static ClusterCtrcTxnKeyV1 recent_registration_key;
static ClusterCtrcReceiptHandle recent_registration_handle;
static ClusterCtrcActiveScan recent_registration_cut;
static bool recent_registration_refused;
static unsigned recent_registration_calls;

static void
recent_register_unknown_receiver(void)
{
	uint64 generation = pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation);

	recent_registration_calls++;
	UT_ASSERT(recent_pin_held && backend_pin_count == 1);
	UT_ASSERT(!local_x && !local_share && !resumed_writer);
	UT_ASSERT(recent_pin_admission->entered);
	UT_ASSERT(cluster_ctrc_active_scan_registration_current(&recent_registration_cut));
	participant_publisher_inputs(&recent_registration_key, 1);
	participant_admission_sample_ok = !recent_registration_refused;
	memset(&recent_registration_handle, 0, sizeof(recent_registration_handle));
	if (recent_registration_refused)
	{
		UT_ASSERT_EQ(participant_publisher_prepare(&recent_registration_key, 1,
			&recent_registration_handle), CLUSTER_CTRC_PREPARE_REFUSED);
		UT_ASSERT(!recent_registration_handle.valid);
		UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation), generation);
		UT_ASSERT(cluster_ctrc_active_scan_registration_current(&recent_registration_cut));
	}
	else
	{
		ClusterCtrcReceiptHandle replay;

		UT_ASSERT_EQ(participant_publisher_prepare(&recent_registration_key, 1,
			&recent_registration_handle), CLUSTER_CTRC_PREPARE_READY);
		UT_ASSERT(recent_registration_handle.valid);
		UT_ASSERT(FullTransactionIdEquals(recent_registration_handle.original_full_xid,
			InvalidFullTransactionId));
		UT_ASSERT_EQ(recent_registration_handle.participant->receipt_count, 1);
		UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation), generation + 1);
		UT_ASSERT(!cluster_ctrc_active_scan_registration_current(&recent_registration_cut));
		UT_ASSERT_EQ(participant_publisher_prepare(&recent_registration_key, 1, &replay),
			CLUSTER_CTRC_PREPARE_DUPLICATE);
		UT_ASSERT(replay.participant == recent_registration_handle.participant);
		UT_ASSERT_EQ(replay.participant->receipt_count, 1);
		UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation), generation + 1);
	}
	UT_ASSERT_EQ(participant_lock_depth, 0);
	UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
}

static void
recent_registration_case(bool batch, bool refuse)
{
	QualifiedXminFixture f;
	LVRelState v;
	ClusterHeapFreezeXminTargetV1 targets[3];
	CensusRuntimeAllocation runtime;

	phase_setup(&f, &v, targets, batch);
	/* Keep the existing fixture's saved cut/source publication. The complete
	 * allocation starts at that same generation; it is not a new census. */
	if (!census_full_layout_setup(&runtime, 2)) { UT_ASSERT(false); return; }
	recent_registration_cut = f.cohort.local_registration_cut;
	UT_ASSERT(cluster_ctrc_active_scan_registration_current(&recent_registration_cut));
	recent_registration_key = census_key_for_full(&f.cohort.admission_fence,
		FullTransactionIdFromEpochAndXid(1, 100));
	recent_registration_key.origin_node_id = 1;
	recent_registration_key.owner_instance = 2;
	recent_registration_key.segment_id = CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	recent_registration_refused = refuse;
	recent_registration_calls = 0;
	backend_pin_count = 0;
	relation_data.rd_isvalid = resume_allowed = true;
	recent_pin_observer = recent_register_unknown_receiver;
	UT_ASSERT_EQ(phase_then_recent_native_owned(&f, &v), refuse);
	recent_pin_observer = NULL;
	UT_ASSERT_EQ(recent_registration_calls, 1);
	UT_ASSERT_EQ(phase_preflights, 1); UT_ASSERT_EQ(phase_rechecks, 1);
	UT_ASSERT_EQ(recent_pin_calls, 1); UT_ASSERT_EQ(recent_unpins, 1);
	UT_ASSERT_EQ(conditional_resumes, refuse); UT_ASSERT_EQ(conditional_unlocks, refuse);
	UT_ASSERT_EQ(inserts, refuse); UT_ASSERT_EQ(flushes, refuse);
	if (!refuse)
	{
		UT_ASSERT_EQ(edges + dirties, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(recent_registration_handle.participant->state, CTRC_PARTICIPANT_OPEN);
		UT_ASSERT_EQ(recent_registration_handle.participant->receipt_count, 1);
	}
	UT_ASSERT_EQ(phase_finally_calls, 1); UT_ASSERT_EQ(phase_finally_debt, 1);
	UT_ASSERT_EQ(phase_finally_pins, 0); UT_ASSERT(!phase_finally_owned_writer);
	UT_ASSERT_EQ(backend_pin_count, 0);
	UT_ASSERT(!recent_pin_held && !local_x && !resumed_writer);
	UT_ASSERT(cluster_bufmgr_pcm_x_writer_find(GetBufferDescriptor(0)) == NULL);
	bounds_lifecycle_assert_empty(&f.admission);
	census_full_layout_release(&runtime);
}

/* Replace the constructed completed cut with the original initializer,
 * origin/participant publishers and every slice of the original worker pass.
 * Source authentication, process scheduling, resolver and resident I/O leaves
 * remain controlled; this does not create a production VACUUM caller. */
/* Use the original ACK/publisher/reply state machines. Canonical I/O,
 * authentication and process scheduling remain the existing controlled leaves. */
/* The projection and durability inputs are controlled leaves, not a native
 * physical cleanout proof. The original shared discharge guards and mutation
 * are executed, including exact target and durability refusal before ACK. */
/* Execute the original post-page-unlock discharge tail. The physical abort
 * stamp and storage/durable-source providers remain controlled; this is not
 * a physical cleanout certificate or a production freeze qualification. */
static ClusterItlTouchRecord *page_owner_touch_list;
static XLogRecPtr page_owner_local_flush, page_owner_remote_flush;
static unsigned page_owner_flush_calls, page_owner_local_reads;
static unsigned page_owner_remote_reads, page_owner_discharge_calls;
static int page_owner_node_id;
static bool page_owner_page_locked;
static PGAlignedBlock page_owner_stamp_live, page_owner_stamp_image, page_owner_wal_cookie;
static unsigned page_owner_stamp_case, page_owner_starts, page_owner_registers;
static unsigned page_owner_finishes, page_owner_aborts, page_owner_unlocks, page_owner_skips;
static bool page_owner_use_range;
static unsigned page_owner_range_calls;
static uint32 page_owner_touch_count;
static bool page_owner_use_xact_owner;
static unsigned page_owner_xact_finishes;
static bool page_owner_use_registration;
static unsigned page_owner_registered_records;
static PGAlignedBlock page_owner_context_cookie;
static MemoryContext page_owner_context_current;
static const ClusterItlTouchRecord *page_owner_registration_source;
static uint64 page_owner_capture_generation;
static bool page_owner_capture_ok;
static unsigned page_owner_capture_calls, page_owner_allocations, page_owner_publication_notes;
static bool page_owner_cluster_enabled;
static uint32 page_owner_touch_capacity, page_owner_subxact_depth, page_owner_subxact_capacity;
static struct ClusterItlTouchSubxactBoundary *page_owner_subxact_stack;

static void
page_owner_require_unlocked(void)
{
	worker_pass_assert_unlocked(); UT_ASSERT_EQ(participant_lock_depth, 0);
	UT_ASSERT(!local_x && !resumed_writer);
	UT_ASSERT(!page_owner_page_locked);
}

static void
page_owner_flush(XLogRecPtr lsn)
{
	page_owner_require_unlocked(); UT_ASSERT_EQ(lsn, 400);
	page_owner_flush_calls++;
}

static XLogRecPtr
page_owner_get_flush(TimeLineID *tli)
{
	page_owner_require_unlocked(); UT_ASSERT(tli == NULL);
	page_owner_local_reads++;
	return page_owner_local_flush;
}

static XLogRecPtr
page_owner_observed_durable(int origin)
{
	page_owner_require_unlocked(); UT_ASSERT(origin != page_owner_node_id);
	page_owner_remote_reads++;
	return origin == 1 ? page_owner_remote_flush : InvalidXLogRecPtr;
}

static ClusterCtrcDischargeResult
page_owner_shared_discharge(const ClusterCtrcReceiptHandle *handle,
	const ClusterCtrcItlTargetIdentity *expected, ClusterCtrcItlProjection projection,
	const ClusterCtrcDurability *durability)
{
	page_owner_require_unlocked(); page_owner_discharge_calls++;
	return original_terminal_receipt_discharge_itl_shared(handle, expected, projection, durability);
}

/* These providers model buffer locking and GenericXLog I/O. The original
 * owner performs all live-slot checks, page stamping, pending selection and
 * ordering; no production PCM/SPACE or physical WAL success is inferred. */
static GenericXLogState *
page_owner_generic_start(bool logged, GenericXLogInternalOwner owner)
{
	page_owner_require_unlocked(); UT_ASSERT(logged);
	UT_ASSERT_EQ(owner, GENERIC_XLOG_ITL_FINISH); page_owner_starts++;
	return (GenericXLogState *)&page_owner_wal_cookie;
}

static bool page_owner_use_native_resident;
static unsigned page_owner_native_resident_calls;
static Buffer page_owner_acquire_from_original(const ClusterItlTouchRecord *, ClusterItlStampSkipReason *);
static void page_owner_unlock_from_original(Buffer);

static Buffer
page_owner_stamp_acquire(const ClusterItlTouchRecord *record, ClusterItlStampSkipReason *reason)
{
	page_owner_require_unlocked();
	if (page_owner_use_range)
		UT_ASSERT(record == page_owner_touch_list || record == page_owner_touch_list + 1);
	else
		UT_ASSERT(record == page_owner_touch_list);
	UT_ASSERT(reason != NULL); *reason = (ClusterItlStampSkipReason)0;
	if (page_owner_use_native_resident)
		return page_owner_acquire_from_original(record, reason);
	if (page_owner_stamp_case == 0)
		return InvalidBuffer;
	page_owner_page_locked = true;
	return 1;
}

static void
page_owner_stamp_unlock(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1); UT_ASSERT(page_owner_page_locked);
	UT_ASSERT_EQ(page_owner_touch_list->ctrc_handle.receipt->state, CTRC_RECEIPT_APPLIED);
	if (page_owner_use_native_resident)
		page_owner_unlock_from_original(buffer);
	else
		page_owner_page_locked = false;
	page_owner_unlocks++;
}

static bool
page_owner_ship_dependencies(Buffer buffer, ClusterSfDepVec *out)
{
	UT_ASSERT_EQ(buffer, 1); UT_ASSERT(page_owner_page_locked);
	UT_ASSERT_EQ(page_owner_flush_calls + page_owner_discharge_calls, 0);
	out->required[0] = 400; out->required[1] = 700;
	return true;
}

static Page
page_owner_generic_register(GenericXLogState *state, Buffer buffer, int flags,
	const ClusterSpaceIdentity *identity)
{
	UT_ASSERT(state == (GenericXLogState *)&page_owner_wal_cookie);
	UT_ASSERT_EQ(buffer, 1); UT_ASSERT_EQ(flags, 0); UT_ASSERT(page_owner_page_locked);
	UT_ASSERT(identity == &(page_owner_touch_list[page_owner_use_range ? 1 : 0].space_identity));
	memcpy(page_owner_stamp_image.data, page_owner_stamp_live.data, BLCKSZ);
	page_owner_registers++;
	return page_owner_stamp_image.data;
}

static XLogRecPtr
page_owner_generic_finish(GenericXLogState *state)
{
	UT_ASSERT(state == (GenericXLogState *)&page_owner_wal_cookie);
	UT_ASSERT(page_owner_page_locked); UT_ASSERT_EQ(page_owner_registers, 1);
	UT_ASSERT_EQ(page_owner_touch_list->ctrc_handle.receipt->state, CTRC_RECEIPT_APPLIED);
	memcpy(page_owner_stamp_live.data, page_owner_stamp_image.data, BLCKSZ);
	PageSetLSN(page_owner_stamp_live.data, 400); page_owner_finishes++;
	return 400;
}

static void
page_owner_generic_abort(GenericXLogState *state)
{
	UT_ASSERT(state == (GenericXLogState *)&page_owner_wal_cookie);
	page_owner_require_unlocked(); UT_ASSERT_EQ(page_owner_registers, 0);
	page_owner_aborts++;
}

static void
page_owner_stamp_skip(void)
{
	page_owner_skips++;
}

/* Allocation/context and PCM observations are controlled leaves; all live
 * slot capture, exact dedupe, failed-proof invalidation and append logic below
 * are the original source bodies. Unexpected growth is outside this case. */
static MemoryContext
page_owner_context_switch(MemoryContext context)
{
	MemoryContext old = page_owner_context_current;
	UT_ASSERT(context == (MemoryContext)&page_owner_context_cookie
		|| context == (MemoryContext)&page_owner_wal_cookie);
	page_owner_context_current = context;
	return old;
}

static void *
page_owner_record_alloc(Size size)
{
	void *result;
	UT_ASSERT(page_owner_context_current == (MemoryContext)&page_owner_context_cookie);
	UT_ASSERT_EQ(size, 16 * sizeof(ClusterItlTouchRecord));
	result = calloc(1, size); UT_ASSERT(result != NULL);
	page_owner_allocations++;
	return result;
}

static void *
page_owner_record_realloc(void *address, Size size)
{
	(void)address; (void)size;
	UT_ASSERT(false);
	return NULL;
}

static bool page_owner_use_native_authority;
static unsigned page_owner_native_authority_calls;
static unsigned page_owner_native_authority_refusals;
ClusterPcmOwnEntry *ClusterPcmOwnArray;

/* The original authority body reads the real header lock and PCM sidecar.
 * Configured topology, epoch, grant acquisition and scheduling remain leaves;
 * this fixture installs an exact ACTIVE writer round, not a new live grant. */
static int page_owner_authority_node_count(void) { return 4; }
static uint64 page_owner_authority_epoch(void) { return 9; }
static bool page_owner_authority_storage(void) { return true; }
static bool
page_owner_authority_target_current(const ResourceXWriterUseContext *context)
{
	UT_ASSERT(page_owner_use_native_authority && local_x && backend_pin_count > 0);
	UT_ASSERT(resource_x_assertion_valid(&context->ref.assertion));
	UT_ASSERT_EQ(context->ref.assertion.requester_node, cluster_node_id);
	UT_ASSERT_EQ(context->buffer_ownership_generation, page_owner_capture_generation);
	return true;
}

static void
page_owner_unexpected_header_spin(SpinDelayStatus *status)
{
	(void)status;
	UT_ASSERT(false);
}
static void
page_owner_finish_header_spin(SpinDelayStatus *status)
{
	UT_ASSERT_EQ(status->spins, 0); UT_ASSERT_EQ(status->delays, 0);
	UT_ASSERT_EQ(status->cur_delay, 0);
}

#define LockBufHdr original_touch_header_lock
#define perform_spin_delay page_owner_unexpected_header_spin
#define finish_spin_delay page_owner_finish_header_spin
#define cluster_pcm_own_snapshot_post_state_locked original_touch_snapshot_post_state
#define cluster_pcm_own_snapshot_locked original_touch_snapshot_locked
#define cluster_bufmgr_pcm_x_retained_image_locked original_touch_retained_image
#define cluster_bufmgr_terminal_stamp_authority original_touch_capture_authority
#define cluster_conf_node_count page_owner_authority_node_count
#define cluster_epoch_get_current page_owner_authority_epoch
#define cluster_storage_mode_enabled page_owner_authority_storage
#define cluster_gcs_resource_x_target_context_recheck_exact page_owner_authority_target_current
#include "test_cluster_heap_touch_authority.inc"
#undef cluster_gcs_resource_x_target_context_recheck_exact
#undef cluster_storage_mode_enabled
#undef cluster_epoch_get_current
#undef cluster_conf_node_count
#undef cluster_bufmgr_terminal_stamp_authority
#undef cluster_bufmgr_pcm_x_retained_image_locked
#undef cluster_pcm_own_snapshot_locked
#undef cluster_pcm_own_snapshot_post_state_locked
#undef finish_spin_delay
#undef perform_spin_delay
#undef LockBufHdr

static bool
page_owner_capture_from_original(Buffer buffer, const BufferTag *tag,
	uint64 *generation, uint64 *epoch, uint8 *state)
{
	BufferDesc *buf = GetBufferDescriptor(0);
	ClusterPcmXWriterLedgerEntry *entry = &cluster_bufmgr_pcm_x_writer_ledger[0];
	ClusterPcmXWriterLedgerEntry saved_entry = *entry;
	ClusterPcmOwnEntry own, *saved_array = ClusterPcmOwnArray;
	BufferTag saved_tag = buf->tag;
	uint32 saved_state = pg_atomic_read_u32(&buf->state);
	uint8 saved_pcm = buf->pcm_state, saved_type = buf->buffer_type;
	bool saved_x = local_x, result;
	int32 saved_pins = backend_pin_count;

	UT_ASSERT_EQ(saved_state & BM_LOCKED, 0);
	memset(&own, 0, sizeof(own));
	pg_atomic_init_u64(&own.generation, page_owner_capture_generation);
	pg_atomic_init_u64(&own.reservation_token, page_owner_capture_ok ? 0 : 1);
	pg_atomic_init_u64(&own.writer_activation_token, 0);
	pg_atomic_init_u64(&own.resource_x_activation_generation, 0);
	pg_atomic_init_u32(&own.flags, page_owner_capture_ok ? 0 : PCM_OWN_FLAG_REVOKING);
	pg_atomic_init_u64(&own.delivery_attempt, 0);
	ClusterPcmOwnArray = &own;
	buf->tag = *tag; buf->pcm_state = PCM_STATE_X; buf->buffer_type = BUF_TYPE_CURRENT;
	pg_atomic_write_u32(&buf->state, BM_VALID);
	memset(entry, 0, sizeof(*entry));
	entry->phase = PCM_X_WRITER_LEDGER_ACTIVE; entry->buffer_id = buf->buf_id;
	entry->content_lock = BufferDescriptorGetContentLock(buf);
	entry->authority = original_writer_context;
	entry->authority.ref.assertion.resource = *tag;
	entry->authority.buffer_ownership_generation = page_owner_capture_generation;
	entry->granted.tag = *tag; entry->granted.generation = page_owner_capture_generation;
	entry->granted.pcm_state = PCM_STATE_X;
	local_x = true; backend_pin_count = saved_pins + 1;
	result = original_touch_capture_authority(buffer, tag, generation, epoch, state);
	UT_ASSERT_EQ(pg_atomic_read_u32(&buf->state), BM_VALID);
	UT_ASSERT_EQ(result, page_owner_capture_ok);
	if (!result)
	{
		UT_ASSERT_EQ(*generation, 0); UT_ASSERT_EQ(*epoch, 0); UT_ASSERT_EQ(*state, PCM_STATE_N);
		page_owner_native_authority_refusals++;
	}
	else
	{
		UT_ASSERT_EQ(*generation, page_owner_capture_generation);
		UT_ASSERT_EQ(*epoch, 9); UT_ASSERT_EQ(*state, PCM_STATE_X);
	}
	local_x = saved_x; backend_pin_count = saved_pins;
	*entry = saved_entry; ClusterPcmOwnArray = saved_array;
	buf->tag = saved_tag; buf->pcm_state = saved_pcm; buf->buffer_type = saved_type;
	pg_atomic_write_u32(&buf->state, saved_state);
	page_owner_native_authority_calls++;
	return result;
}

/* Mapping, private pin/resource owner and LWLock scheduling are controlled.
 * Original resident bodies own all live checks and refusal/success cleanup. */
#include "test_cluster_heap_resident_stamp_types.inc"
static bool page_owner_use_original_resource_owner;
static unsigned page_owner_original_owner_enlarges, page_owner_original_owner_remembers;
static unsigned page_owner_original_owner_forgets, page_owner_original_owner_allocations;
static LWLock page_owner_resident_mapping_lock;
static bool page_owner_resident_active, page_owner_resident_partition_held;
static unsigned page_owner_resident_pins, page_owner_resident_unpins;
static unsigned page_owner_resident_releases, page_owner_resident_successes;
static unsigned page_owner_resident_missing, page_owner_resident_wrap, page_owner_resident_generation;
static struct {
	BufferTag tag;
	uint32 state;
	uint8 pcm, type;
	ClusterPcmOwnEntry own, *array;
	int32 pins;
	bool local_x;
} page_owner_resident_saved;

static uint32
page_owner_resident_hash(BufferTag *tag)
{
	UT_ASSERT(page_owner_resident_active && !page_owner_resident_partition_held);
	UT_ASSERT(BufferTagsEqual(tag, &GetBufferDescriptor(0)->tag));
	return 73;
}
static LWLock *
page_owner_resident_partition(uint32 hash)
{
	UT_ASSERT_EQ(hash, 73);
	return &page_owner_resident_mapping_lock;
}
static void
page_owner_resident_reserve(void)
{
	UT_ASSERT(page_owner_resident_active && !page_owner_resident_partition_held && !page_owner_page_locked);
}
/* Original buffer ResourceOwner/array routines and complete private types.
 * Memory context allocation is a bounded provider, not a PG context lifetime. */
static void *
page_owner_resource_alloc(MemoryContext context, Size size)
{
	void *memory;
	UT_ASSERT(context == (MemoryContext)&page_owner_context_cookie);
	UT_ASSERT(page_owner_use_original_resource_owner && page_owner_resident_active);
	UT_ASSERT(!page_owner_resident_partition_held && !page_owner_page_locked && !local_x);
	UT_ASSERT_EQ(page_owner_original_owner_allocations, 0);
	UT_ASSERT_EQ(size, 16 * sizeof(Datum));
	memory = malloc(size); UT_ASSERT(memory != NULL);
	memset(memory, 0xa5, size); page_owner_original_owner_allocations++;
	return memory;
}
static void
page_owner_resource_free_unexpected(void *memory)
{
	UT_ASSERT(false); /* No growth/rehash is required for one outstanding pin. */
	free(memory);
}
#define MemoryContextAlloc page_owner_resource_alloc
#define TopMemoryContext ((MemoryContext)&page_owner_context_cookie)
#define pfree page_owner_resource_free_unexpected
#define ResourceArrayInit original_touch_owner_array_init
#define ResourceArrayEnlarge original_touch_owner_array_enlarge
#define ResourceArrayAdd original_touch_owner_array_add
#define ResourceArrayRemove original_touch_owner_array_remove
#define ResourceOwnerEnlargeBuffers original_touch_owner_enlarge
#define ResourceOwnerRememberBuffer original_touch_owner_remember
#define ResourceOwnerForgetBuffer original_touch_owner_forget
#include "test_cluster_heap_resource_owner.inc"

static Datum
page_owner_buffer_datum(Buffer buffer)
{
	return BufferGetDatum(buffer);
}
#undef BufferGetDatum
#undef ResourceOwnerForgetBuffer
#undef ResourceOwnerRememberBuffer
#undef ResourceOwnerEnlargeBuffers
#undef ResourceArrayRemove
#undef ResourceArrayAdd
#undef ResourceArrayEnlarge
#undef ResourceArrayInit
#undef pfree
#undef TopMemoryContext
#undef MemoryContextAlloc
#undef MAX_RESOWNER_LOCKS
#undef RESARRAY_MAX_ITEMS
#undef RESARRAY_IS_ARRAY
#undef RESARRAY_MAX_ARRAY
#undef RESARRAY_INIT_SIZE

static ResourceOwnerData page_owner_native_resource_owner;
static ResourceOwner
page_owner_active_resource_owner(void)
{
	return page_owner_use_original_resource_owner
		? &page_owner_native_resource_owner : (ResourceOwner)&page_owner_wal_cookie;
}

static void
page_owner_resident_enlarge(ResourceOwner owner)
{
	UT_ASSERT(owner == page_owner_active_resource_owner());
	page_owner_resident_reserve();
	if (page_owner_use_original_resource_owner)
	{
		UT_ASSERT_EQ(owner->bufferarr.nitems, 0);
		original_touch_owner_enlarge(owner); page_owner_original_owner_enlarges++;
		UT_ASSERT_EQ(owner->bufferarr.capacity, 16); UT_ASSERT_EQ(owner->bufferarr.maxitems, 16);
		if (page_owner_original_owner_enlarges == 1)
			for (unsigned i = 0; i < owner->bufferarr.capacity; i++)
				UT_ASSERT_EQ(owner->bufferarr.itemsarr[i], page_owner_buffer_datum(InvalidBuffer));
	}
}
static bool
page_owner_resident_lock(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(page_owner_resident_active);
	if (lock == &page_owner_resident_mapping_lock)
	{
		UT_ASSERT_EQ(mode, LW_SHARED);
		UT_ASSERT(!page_owner_resident_partition_held && !page_owner_page_locked);
		page_owner_resident_partition_held = true;
	}
	else
	{
		UT_ASSERT(lock == BufferDescriptorGetContentLock(GetBufferDescriptor(0)));
		UT_ASSERT_EQ(mode, LW_EXCLUSIVE);
		UT_ASSERT(!page_owner_resident_partition_held && !page_owner_page_locked && !local_x);
		UT_ASSERT_EQ(backend_pin_count, page_owner_resident_saved.pins + 1);
		page_owner_page_locked = local_x = true;
	}
	return true;
}
static void
page_owner_resident_release_lock(LWLock *lock)
{
	UT_ASSERT(page_owner_resident_active);
	if (lock == &page_owner_resident_mapping_lock)
	{
		UT_ASSERT(page_owner_resident_partition_held && !page_owner_page_locked);
		page_owner_resident_partition_held = false;
	}
	else
	{
		UT_ASSERT(lock == BufferDescriptorGetContentLock(GetBufferDescriptor(0)));
		UT_ASSERT(!page_owner_resident_partition_held && page_owner_page_locked && local_x);
		UT_ASSERT_EQ(backend_pin_count, page_owner_resident_saved.pins + 1);
		page_owner_page_locked = local_x = false;
		page_owner_resident_releases++;
	}
}
static int
page_owner_resident_lookup(BufferTag *tag, uint32 hash)
{
	UT_ASSERT(page_owner_resident_partition_held && !page_owner_page_locked);
	UT_ASSERT_EQ(hash, 73); UT_ASSERT(BufferTagsEqual(tag, &GetBufferDescriptor(0)->tag));
	return page_owner_stamp_case == 0 ? -1 : 0;
}
static bool page_owner_use_original_private_refs;
static unsigned page_owner_original_private_gets, page_owner_original_private_reserves;
static unsigned page_owner_original_private_news, page_owner_original_private_forgets;
static bool page_owner_use_original_primitive_pin;
static unsigned page_owner_original_primitive_pin_calls;
static unsigned page_owner_primitive_reserves, page_owner_primitive_new_refs;
static unsigned page_owner_primitive_remembers, page_owner_primitive_unpins;
static bool page_owner_use_original_lookup_pin;
static unsigned page_owner_original_lookup_pin_calls;
static unsigned page_owner_lookup_private_reads, page_owner_lookup_pin_leaf_calls;

/* Original native atomic pin/unpin bodies; private storage, ResourceOwner
 * and contention/last-read-image scheduling remain explicit providers. */
static PrivateRefCountEntry page_owner_primitive_ref;
static bool page_owner_primitive_ref_live, page_owner_primitive_reserved;
static unsigned page_owner_primitive_private_reads, page_owner_primitive_forgets;
static unsigned page_owner_primitive_last_pin_reads;

/* Original private-ref array and reserved-slot cache. Hash overflow remains
 * in the extracted bodies, but is outside this single-buffer fixture. */
static void *
page_owner_private_hash_unexpected(HTAB *hash, const void *key, HASHACTION action, bool *found)
{
	(void)hash; (void)key; (void)action; (void)found;
	UT_ASSERT(false);
	return NULL;
}
#define PrivateRefCountArray page_owner_original_private_array
#define PrivateRefCountHash page_owner_original_private_hash
#define PrivateRefCountOverflowed page_owner_original_private_overflowed
#define PrivateRefCountClock page_owner_original_private_clock
#define ReservedRefCountEntry page_owner_original_private_reserved
#define hash_search page_owner_private_hash_unexpected
#define ReservePrivateRefCountEntry original_touch_private_reserve
#define NewPrivateRefCountEntry original_touch_private_new
#define GetPrivateRefCountEntry original_touch_private_get
#define ForgetPrivateRefCountEntry original_touch_private_forget
#include "test_cluster_heap_private_ref.inc"
#undef ForgetPrivateRefCountEntry
#undef GetPrivateRefCountEntry
#undef NewPrivateRefCountEntry
#undef ReservePrivateRefCountEntry
#undef hash_search
#undef ReservedRefCountEntry
#undef PrivateRefCountClock
#undef PrivateRefCountOverflowed
#undef PrivateRefCountHash
#undef PrivateRefCountArray
#undef REFCOUNT_ARRAY_ENTRIES

static PrivateRefCountEntry *
page_owner_active_private_ref(void)
{
	return page_owner_use_original_private_refs
		? &page_owner_original_private_array[0] : &page_owner_primitive_ref;
}

static PrivateRefCountEntry *
page_owner_primitive_private_ref(Buffer buffer, bool move)
{
	UT_ASSERT(page_owner_use_original_primitive_pin && page_owner_resident_active);
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT_EQ(move, page_owner_resident_partition_held);
	page_owner_primitive_private_reads++;
	if (page_owner_use_original_private_refs)
	{
		PrivateRefCountEntry *ref = original_touch_private_get(buffer, move);
		page_owner_original_private_gets++;
		UT_ASSERT_EQ(ref != NULL, page_owner_primitive_ref_live);
		if (ref != NULL) UT_ASSERT(ref == page_owner_active_private_ref());
		return ref;
	}
	return page_owner_primitive_ref_live ? &page_owner_primitive_ref : NULL;
}
static void
page_owner_primitive_reserve(void)
{
	UT_ASSERT(page_owner_use_original_primitive_pin && page_owner_resident_partition_held);
	UT_ASSERT(!page_owner_primitive_reserved && !page_owner_primitive_ref_live);
	if (page_owner_use_original_private_refs)
	{
		original_touch_private_reserve(); page_owner_original_private_reserves++;
		UT_ASSERT(page_owner_original_private_reserved == page_owner_active_private_ref());
	}
	page_owner_primitive_reserved = true; page_owner_primitive_reserves++;
}
static PrivateRefCountEntry *
page_owner_primitive_new_ref(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(page_owner_primitive_reserved && !page_owner_primitive_ref_live);
	if (page_owner_use_original_private_refs)
	{
		PrivateRefCountEntry *ref = original_touch_private_new(buffer);
		page_owner_original_private_news++;
		UT_ASSERT(ref == page_owner_active_private_ref());
		UT_ASSERT(page_owner_original_private_reserved == NULL);
		UT_ASSERT_EQ(ref->buffer, buffer); UT_ASSERT_EQ(ref->refcount, 0);
		page_owner_primitive_ref_live = true; page_owner_primitive_reserved = false;
		page_owner_primitive_new_refs++;
		return ref;
	}
	memset(&page_owner_primitive_ref, 0, sizeof(page_owner_primitive_ref));
	page_owner_primitive_ref.buffer = buffer;
	page_owner_primitive_ref_live = true; page_owner_primitive_reserved = false;
	page_owner_primitive_new_refs++;
	return &page_owner_primitive_ref;
}
static void
page_owner_primitive_remember(ResourceOwner owner, Buffer buffer)
{
	uint32 state = pg_atomic_read_u32(&GetBufferDescriptor(0)->state);
	UT_ASSERT(owner == page_owner_active_resource_owner()); UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(page_owner_primitive_ref_live && !page_owner_primitive_reserved);
	UT_ASSERT_EQ(page_owner_active_private_ref()->refcount, 1);
	UT_ASSERT_EQ(state, BM_VALID | BUF_REFCOUNT_ONE | BUF_USAGECOUNT_ONE);
	UT_ASSERT_EQ(backend_pin_count, page_owner_resident_saved.pins);
	if (page_owner_use_original_resource_owner)
	{
		UT_ASSERT_EQ(owner->bufferarr.nitems, 0);
		original_touch_owner_remember(owner, buffer); page_owner_original_owner_remembers++;
		UT_ASSERT_EQ(owner->bufferarr.nitems, 1); UT_ASSERT_EQ(owner->bufferarr.lastidx, 0);
		UT_ASSERT_EQ(owner->bufferarr.itemsarr[0], page_owner_buffer_datum(buffer));
	}
	backend_pin_count++; page_owner_resident_pins++; page_owner_primitive_remembers++;
}
static void
page_owner_primitive_forget(ResourceOwner owner, Buffer buffer)
{
	UT_ASSERT(owner == page_owner_active_resource_owner()); UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(page_owner_primitive_ref_live && !page_owner_resident_partition_held);
	UT_ASSERT_EQ(page_owner_active_private_ref()->refcount, 1);
	UT_ASSERT_EQ(backend_pin_count, page_owner_resident_saved.pins + 1);
	if (page_owner_use_original_resource_owner)
	{
		UT_ASSERT_EQ(owner->bufferarr.nitems, 1);
		original_touch_owner_forget(owner, buffer); page_owner_original_owner_forgets++;
		UT_ASSERT_EQ(owner->bufferarr.nitems, 0);
	}
	backend_pin_count--; page_owner_primitive_forgets++;
}
static void
page_owner_primitive_forget_ref(PrivateRefCountEntry *ref)
{
	UT_ASSERT(ref == page_owner_active_private_ref() && page_owner_primitive_ref_live);
	UT_ASSERT_EQ(ref->refcount, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&GetBufferDescriptor(0)->state), BM_VALID | BUF_USAGECOUNT_ONE);
	UT_ASSERT_EQ(backend_pin_count, page_owner_resident_saved.pins);
	if (page_owner_use_original_private_refs)
	{
		original_touch_private_forget(ref); page_owner_original_private_forgets++;
		UT_ASSERT_EQ(ref->buffer, InvalidBuffer);
		UT_ASSERT(page_owner_original_private_reserved == ref);
	}
	page_owner_primitive_ref_live = false; page_owner_primitive_unpins++;
}
static void
page_owner_primitive_last_pin(BufferDesc *buf)
{
	UT_ASSERT(buf == GetBufferDescriptor(0));
	UT_ASSERT(!page_owner_resident_partition_held && !page_owner_page_locked && !local_x);
	UT_ASSERT_EQ(page_owner_active_private_ref()->refcount, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&buf->state), BM_VALID | BUF_REFCOUNT_ONE | BUF_USAGECOUNT_ONE);
	page_owner_primitive_last_pin_reads++;
}
static bool
page_owner_primitive_content_held(LWLock *lock)
{
	UT_ASSERT(lock == BufferDescriptorGetContentLock(GetBufferDescriptor(0)));
	return page_owner_page_locked;
}
static uint32
page_owner_primitive_wait_header(BufferDesc *buf)
{
	UT_ASSERT(false); /* No contended header or waiter is modeled here. */
	return pg_atomic_read_u32(&buf->state) & ~BM_LOCKED;
}
static void
page_owner_primitive_signal(int pgprocno)
{
	(void)pgprocno; UT_ASSERT(false);
}

#define GetPrivateRefCountEntry page_owner_primitive_private_ref
#define ReservePrivateRefCountEntry page_owner_primitive_reserve
#define NewPrivateRefCountEntry page_owner_primitive_new_ref
#define ForgetPrivateRefCountEntry page_owner_primitive_forget_ref
#define ResourceOwnerRememberBuffer page_owner_primitive_remember
#define ResourceOwnerForgetBuffer page_owner_primitive_forget
#define CurrentResourceOwner page_owner_active_resource_owner()
#define WaitBufHdrUnlocked page_owner_primitive_wait_header
#define cluster_bufmgr_pcm_read_image_last_pin page_owner_primitive_last_pin
#define LWLockHeldByMe page_owner_primitive_content_held
#define LockBufHdr original_touch_header_lock
#define ProcSendSignal page_owner_primitive_signal
#define PinBuffer original_touch_primitive_pin
#define UnpinBuffer original_touch_primitive_unpin
#include "test_cluster_heap_primitive_pin.inc"
#undef UnpinBuffer
#undef PinBuffer
#undef ProcSendSignal
#undef LockBufHdr
#undef LWLockHeldByMe
#undef cluster_bufmgr_pcm_read_image_last_pin
#undef WaitBufHdrUnlocked
#undef CurrentResourceOwner
#undef ResourceOwnerForgetBuffer
#undef ResourceOwnerRememberBuffer
#undef ForgetPrivateRefCountEntry
#undef NewPrivateRefCountEntry
#undef ReservePrivateRefCountEntry
#undef GetPrivateRefCountEntry

/* Actual lookup routing and the original PCM tracking predicate below;
 * private-refcount storage, runtime/config and primitive pin stay controlled.
 * Auxiliary publication is outside these MAIN-fork cases and fails loudly. */
static PrivateRefCountEntry *
page_owner_lookup_private_ref(Buffer buffer, bool move)
{
	UT_ASSERT(page_owner_use_original_lookup_pin && page_owner_resident_partition_held);
	UT_ASSERT_EQ(buffer, 1); UT_ASSERT(move);
	page_owner_lookup_private_reads++;
	if (page_owner_use_original_private_refs)
	{
		PrivateRefCountEntry *ref = original_touch_private_get(buffer, move);
		page_owner_original_private_gets++;
		UT_ASSERT(ref == NULL);
		return ref;
	}
	return NULL;
}
static bool
page_owner_lookup_pcm_active(void)
{
	UT_ASSERT(page_owner_use_original_lookup_pin && page_owner_resident_active && page_owner_resident_partition_held);
	return true;
}
static bool
page_owner_lookup_pin_leaf(BufferDesc *buf, BufferAccessStrategy strategy)
{
	UT_ASSERT(buf == GetBufferDescriptor(0) && strategy == NULL);
	UT_ASSERT_EQ(buf->tag.forkNum, MAIN_FORKNUM);
	UT_ASSERT(page_owner_resident_partition_held && !page_owner_page_locked);
	UT_ASSERT_EQ(backend_pin_count, page_owner_resident_saved.pins);
	if (page_owner_use_original_primitive_pin)
	{
		bool result = original_touch_primitive_pin(buf, strategy);
		page_owner_original_primitive_pin_calls++; page_owner_lookup_pin_leaf_calls++;
		return result;
	}
	backend_pin_count++; page_owner_resident_pins++;
	page_owner_lookup_pin_leaf_calls++;
	return true;
}
static void
page_owner_lookup_aux_reserve(void)
{
	UT_ASSERT(false);
}
static PrivateRefCountEntry *
page_owner_lookup_aux_ref(Buffer buffer)
{
	(void)buffer;
	UT_ASSERT(false);
	return NULL;
}
static void
page_owner_lookup_aux_remember(ResourceOwner owner, Buffer buffer)
{
	(void)owner; (void)buffer;
	UT_ASSERT(false);
}

#define GetPrivateRefCountEntry page_owner_lookup_private_ref
#define cluster_pcm_is_active page_owner_lookup_pcm_active
#define cluster_shared_catalog true
#define cluster_bufmgr_should_pcm_track original_touch_pin_tracked
#define ReservePrivateRefCountEntry page_owner_lookup_aux_reserve
#define LockBufHdr original_touch_header_lock
#define NewPrivateRefCountEntry page_owner_lookup_aux_ref
#define ResourceOwnerRememberBuffer page_owner_lookup_aux_remember
#define CurrentResourceOwner page_owner_active_resource_owner()
#define PinBuffer page_owner_lookup_pin_leaf
#define PinBufferForLookup original_touch_lookup_pin
#include "test_cluster_heap_lookup_pin.inc"
#undef PinBufferForLookup
#undef PinBuffer
#undef CurrentResourceOwner
#undef ResourceOwnerRememberBuffer
#undef NewPrivateRefCountEntry
#undef LockBufHdr
#undef ReservePrivateRefCountEntry
#undef cluster_bufmgr_should_pcm_track
#undef cluster_shared_catalog
#undef cluster_pcm_is_active
#undef GetPrivateRefCountEntry

static PinBufferLookupResult
page_owner_resident_pin(BufferDesc *buf, BufferAccessStrategy strategy)
{
	UT_ASSERT(buf == GetBufferDescriptor(0) && strategy == NULL);
	UT_ASSERT(page_owner_resident_partition_held && !page_owner_page_locked);
	UT_ASSERT_EQ(backend_pin_count, page_owner_resident_saved.pins);
	if (page_owner_use_original_lookup_pin)
	{
		PinBufferLookupResult result = original_touch_lookup_pin(buf, strategy);
		page_owner_original_lookup_pin_calls++;
		return result;
	}
	backend_pin_count++; page_owner_resident_pins++;
	return PIN_BUFFER_LOOKUP_VALID;
}
static void
page_owner_resident_unpin(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(!page_owner_resident_partition_held && !page_owner_page_locked && !local_x);
	UT_ASSERT_EQ(backend_pin_count, page_owner_resident_saved.pins + 1);
	if (page_owner_use_original_primitive_pin)
	{
		original_touch_primitive_unpin(GetBufferDescriptor(0));
		UT_ASSERT_EQ(backend_pin_count, page_owner_resident_saved.pins);
		page_owner_resident_unpins++;
		return;
	}
	backend_pin_count--; page_owner_resident_unpins++;
}

static int page_owner_resident_error_detail(const char *format, ...) pg_attribute_printf(1, 2);
static int
page_owner_resident_error_detail(const char *format, ...)
{
	va_list args;
	char detail[128];

	va_start(args, format);
	(void)vsnprintf(detail, sizeof(detail), format, args);
	va_end(args);
	fprintf(stderr, "# resident stamp detail: %s\n", detail);
	return 0;
}

#define errdetail page_owner_resident_error_detail
#define cluster_bufmgr_lock_resident_for_exact_itl_stamp original_touch_resident_acquire
#define cluster_bufmgr_unlock_resident_stamp original_touch_resident_unlock
#define cluster_pcm_own_snapshot_locked original_touch_snapshot_locked
#define cluster_bufmgr_pcm_x_retained_image_locked original_touch_retained_image
#define LockBufHdr original_touch_header_lock
#define cluster_conf_node_count page_owner_authority_node_count
#define cluster_epoch_get_current page_owner_authority_epoch
#define cluster_storage_mode_enabled page_owner_authority_storage
#define BufTableHashCode page_owner_resident_hash
#define BufMappingPartitionLock page_owner_resident_partition
#define ReservePrivateRefCountEntry page_owner_resident_reserve
#define ResourceOwnerEnlargeBuffers page_owner_resident_enlarge
#define CurrentResourceOwner page_owner_active_resource_owner()
#define LWLockAcquire page_owner_resident_lock
#define LWLockRelease page_owner_resident_release_lock
#define BufTableLookup page_owner_resident_lookup
#define PinBufferForLookup page_owner_resident_pin
#define ReleaseBuffer page_owner_resident_unpin
#include "test_cluster_heap_resident_stamp.inc"
#undef BufHdrGetBlock
#undef ReleaseBuffer
#undef PinBufferForLookup
#undef BufTableLookup
#undef LWLockRelease
#undef LWLockAcquire
#undef CurrentResourceOwner
#undef ResourceOwnerEnlargeBuffers
#undef ReservePrivateRefCountEntry
#undef BufMappingPartitionLock
#undef BufTableHashCode
#undef cluster_storage_mode_enabled
#undef cluster_epoch_get_current
#undef cluster_conf_node_count
#undef LockBufHdr
#undef cluster_bufmgr_pcm_x_retained_image_locked
#undef cluster_pcm_own_snapshot_locked
#undef cluster_bufmgr_unlock_resident_stamp
#undef cluster_bufmgr_lock_resident_for_exact_itl_stamp
#undef errdetail

static void
page_owner_resident_restore(void)
{
	BufferDesc *buf = GetBufferDescriptor(0);
	UT_ASSERT(page_owner_resident_active && !page_owner_resident_partition_held && !page_owner_page_locked);
	UT_ASSERT_EQ(backend_pin_count, page_owner_resident_saved.pins);
	UT_ASSERT_EQ(local_x, page_owner_resident_saved.local_x);
	buf->tag = page_owner_resident_saved.tag;
	buf->pcm_state = page_owner_resident_saved.pcm; buf->buffer_type = page_owner_resident_saved.type;
	pg_atomic_write_u32(&buf->state, page_owner_resident_saved.state);
	ClusterPcmOwnArray = page_owner_resident_saved.array;
	page_owner_resident_active = false;
}
static Buffer
page_owner_acquire_from_original(const ClusterItlTouchRecord *record, ClusterItlStampSkipReason *reason)
{
	BufferDesc *buf = GetBufferDescriptor(0);
	ClusterPcmOwnEntry *own = &page_owner_resident_saved.own;
	const ClusterItlTerminalProof *latest = &page_owner_touch_list[page_owner_touch_count - 1].proof;
	Buffer result;
	UT_ASSERT(page_owner_use_native_resident && !page_owner_resident_active && !local_x);
	page_owner_resident_saved.tag = buf->tag;
	page_owner_resident_saved.state = pg_atomic_read_u32(&buf->state);
	page_owner_resident_saved.pcm = buf->pcm_state; page_owner_resident_saved.type = buf->buffer_type;
	page_owner_resident_saved.array = ClusterPcmOwnArray;
	page_owner_resident_saved.pins = backend_pin_count; page_owner_resident_saved.local_x = local_x;
	UT_ASSERT_EQ(page_owner_resident_saved.state & BM_LOCKED, 0);
	pg_atomic_init_u64(&own->generation, latest->own_generation);
	pg_atomic_init_u64(&own->reservation_token, 0);
	pg_atomic_init_u64(&own->writer_activation_token, 0);
	pg_atomic_init_u64(&own->resource_x_activation_generation, 0);
	pg_atomic_init_u64(&own->delivery_attempt, 0); pg_atomic_init_u32(&own->flags, 0);
	ClusterPcmOwnArray = own;
	InitBufferTag(&buf->tag, &record->key.rloc, record->key.forknum, record->key.block);
	buf->pcm_state = PCM_STATE_X; buf->buffer_type = BUF_TYPE_CURRENT;
	pg_atomic_write_u32(&buf->state, BM_VALID);
	page_owner_resident_active = true;
	result = original_touch_resident_acquire(record, reason);
	page_owner_native_resident_calls++;
	if (BufferIsValid(result))
	{
		UT_ASSERT_EQ(result, 1); UT_ASSERT_EQ(*reason, CLUSTER_ITL_STAMP_SKIP_NONE);
		UT_ASSERT(page_owner_page_locked && local_x && !page_owner_resident_partition_held);
		page_owner_resident_successes++;
	}
	else
	{
		if (page_owner_stamp_case == 0)
		{
			UT_ASSERT_EQ(*reason, CLUSTER_ITL_STAMP_SKIP_NOT_RESIDENT);
			page_owner_resident_missing++;
		}
		else if (record == page_owner_touch_list + page_owner_touch_count - 1)
		{
			UT_ASSERT_EQ(page_owner_stamp_case, 1);
			UT_ASSERT_EQ(*reason, CLUSTER_ITL_STAMP_SKIP_WRAP_CHANGED);
			page_owner_resident_wrap++;
		}
		else
		{
			UT_ASSERT_EQ(*reason, CLUSTER_ITL_STAMP_SKIP_OWNERSHIP_GENERATION_CHANGED);
			page_owner_resident_generation++;
		}
		page_owner_resident_restore();
	}
	return result;
}
static void
page_owner_unlock_from_original(Buffer buffer)
{
	UT_ASSERT(page_owner_resident_active && page_owner_page_locked && local_x);
	original_touch_resident_unlock(buffer);
	page_owner_resident_restore();
}

static bool
page_owner_capture_authority(Buffer buffer, const BufferTag *tag,
	uint64 *generation, uint64 *epoch, uint8 *state)
{
	UT_ASSERT(page_owner_use_registration && page_owner_registration_source != NULL);
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(RelFileLocatorEquals(BufTagGetRelFileLocator(tag), page_owner_registration_source->key.rloc));
	UT_ASSERT_EQ(tag->forkNum, page_owner_registration_source->key.forknum);
	UT_ASSERT_EQ(tag->blockNum, page_owner_registration_source->key.block);
	if (page_owner_use_native_authority)
	{
		page_owner_capture_calls++;
		return page_owner_capture_from_original(buffer, tag, generation, epoch, state);
	}
	*generation = page_owner_capture_generation; *epoch = 9; *state = PCM_STATE_X;
	page_owner_capture_calls++;
	return page_owner_capture_ok;
}

static void
page_owner_publication_stat(ClusterCtrcStatId stat)
{
	UT_ASSERT_EQ(stat, CTRC_STAT_ORDINARY_PUBLICATION_AFTER_APPLY);
	page_owner_publication_notes++;
	original_census_stat_bump(stat);
}

#define cluster_ctrc_stat_bump page_owner_publication_stat
#define ctrc_receipt_entries original_participant_receipt_entries
#define cluster_ctrc_note_publication_after_apply original_page_owner_publication
#include "test_cluster_heap_itl_publication.inc"
#undef cluster_ctrc_note_publication_after_apply
#undef ctrc_receipt_entries
#undef cluster_ctrc_stat_bump

#define touch_list page_owner_touch_list
#define itl_touch_handle_matches original_page_owner_handle_matches
#define itl_touch_append original_page_owner_append
#define itl_touch_capture_proof original_page_owner_capture
#define itl_touch_register_exact_internal original_page_owner_register_internal
#define cluster_itl_touch_register_exact_ctrc original_page_owner_register
#define cluster_ctrc_note_publication_after_apply original_page_owner_publication
#define cluster_bufmgr_terminal_stamp_authority page_owner_capture_authority
#define MemoryContextSwitchTo page_owner_context_switch
#define TopTransactionContext ((MemoryContext)&page_owner_context_cookie)
#define palloc page_owner_record_alloc
#define repalloc page_owner_record_realloc
#define touch_count page_owner_touch_count
#define touch_capacity page_owner_touch_capacity
#define subxact_stack page_owner_subxact_stack
#define subxact_depth page_owner_subxact_depth
#define subxact_capacity page_owner_subxact_capacity
#define cluster_enabled page_owner_cluster_enabled
#define cluster_itl_touch_reset_at_end_xact original_page_owner_reset
#define cluster_itl_xact_precommit_finish original_page_owner_precommit
#define cluster_itl_xact_abort_finish original_page_owner_abort
#define itl_touch_record_cmp original_page_owner_record_cmp
#define itl_finish_range original_page_owner_finish_range
#define cluster_node_id page_owner_node_id
#define XLogFlush page_owner_flush
#define GetFlushRecPtr page_owner_get_flush
#define cluster_sf_observed_origin_durable_lsn page_owner_observed_durable
#define cluster_ctrc_receipt_discharge_itl_shared page_owner_shared_discharge
#define itl_finish_expected_target original_page_owner_expected_target
#define itl_finish_discharge_batch original_page_owner_discharge_batch
#define itl_finish_stamp_page original_page_owner_stamp_page
#define itl_finish_flush_batch original_page_owner_flush_batch
#define GenericXLogStartInternal page_owner_generic_start
#define GenericXLogRegisterBufferVersioned page_owner_generic_register
#define GenericXLogFinish page_owner_generic_finish
#define GenericXLogAbort page_owner_generic_abort
#define cluster_bufmgr_lock_resident_for_exact_itl_stamp page_owner_stamp_acquire
#define cluster_bufmgr_unlock_resident_stamp page_owner_stamp_unlock
#define cluster_sf_dep_vec_for_ship page_owner_ship_dependencies
#define cluster_lever_g_note_stamp_skipped page_owner_stamp_skip
#include "test_cluster_heap_itl_finish_discharge.inc"
#undef cluster_lever_g_note_stamp_skipped
#undef cluster_sf_dep_vec_for_ship
#undef cluster_bufmgr_unlock_resident_stamp
#undef cluster_bufmgr_lock_resident_for_exact_itl_stamp
#undef GenericXLogAbort
#undef GenericXLogFinish
#undef GenericXLogRegisterBufferVersioned
#undef GenericXLogStartInternal
#undef itl_finish_flush_batch
#undef itl_finish_stamp_page
#undef itl_finish_discharge_batch
#undef itl_finish_expected_target
#undef cluster_ctrc_receipt_discharge_itl_shared
#undef cluster_sf_observed_origin_durable_lsn
#undef GetFlushRecPtr
#undef XLogFlush
#undef cluster_node_id
#undef itl_finish_range
#undef itl_touch_record_cmp
#undef cluster_itl_xact_abort_finish
#undef cluster_itl_xact_precommit_finish
#undef cluster_itl_touch_reset_at_end_xact
#undef cluster_enabled
#undef subxact_capacity
#undef subxact_depth
#undef subxact_stack
#undef touch_capacity
#undef touch_count
#undef repalloc
#undef palloc
#undef TopTransactionContext
#undef MemoryContextSwitchTo
#undef cluster_bufmgr_terminal_stamp_authority
#undef cluster_ctrc_note_publication_after_apply
#undef cluster_itl_touch_register_exact_ctrc
#undef itl_touch_register_exact_internal
#undef itl_touch_capture_proof
#undef itl_touch_append
#undef itl_touch_handle_matches
#undef touch_list

/* Register both slot incarnations through the original public producer.
 * Before stamping, failed capture changes only the local touch record and
 * never consumes or retires the already APPLIED shared receipt. */
static ClusterItlTouchRecord *
page_owner_register_records(ClusterItlTouchRecord *record)
{
	ClusterItlSlotData *slot = &ClusterPageGetItlSlots(BufferBlocks)[record->key.slot_idx];
	ClusterItlSlotData saved_slot = *slot;
	ClusterSpaceIdentity identity = record->space_identity, bad_identity;
	ClusterCtrcParticipantEntry participant = *record->ctrc_handle.participant;
	ClusterCtrcReceipt receipt = *record->ctrc_handle.receipt;

	identity.state = CLUSTER_SPACE_IDENTITY_LIVE; identity.key.locator = record->key.rloc;
	bad_identity = identity; bad_identity.state = 0;
	UT_ASSERT(bad_identity.state != CLUSTER_SPACE_IDENTITY_LIVE);
	page_owner_touch_list = NULL; page_owner_touch_count = page_owner_touch_capacity = 0;
	page_owner_registration_source = record;
	page_owner_context_current = (MemoryContext)&page_owner_wal_cookie;
	page_owner_capture_ok = true; page_owner_capture_generation = record->proof.own_generation;
	page_owner_capture_calls = page_owner_allocations = page_owner_publication_notes = 0;
	original_page_owner_register(&record->key, 1, slot->xid, &record->ctrc_handle, &bad_identity);
	UT_ASSERT(page_owner_touch_list == NULL); UT_ASSERT_EQ(page_owner_touch_count, 0);
	UT_ASSERT_EQ(page_owner_allocations, 0);
	original_page_owner_register(&record->key, 1, slot->xid, &record->ctrc_handle, &identity);
	UT_ASSERT_EQ(page_owner_touch_count, 1); UT_ASSERT(page_owner_touch_list[0].proof.valid);
	page_owner_capture_ok = false;
	original_page_owner_register(&record->key, 1, slot->xid, &record->ctrc_handle, &identity);
	UT_ASSERT_EQ(page_owner_touch_count, 1); UT_ASSERT(!page_owner_touch_list[0].proof.valid);
	UT_ASSERT(!page_owner_touch_list[0].ctrc_handle.valid);
	page_owner_capture_ok = true;
	original_page_owner_register(&record->key, 1, slot->xid, &record->ctrc_handle, &identity);
	UT_ASSERT_EQ(page_owner_touch_count, 1); UT_ASSERT(page_owner_touch_list[0].proof.valid);
	UT_ASSERT(page_owner_touch_list[0].ctrc_handle.valid);
	slot->xid--; page_owner_capture_generation--;
	original_page_owner_register(&record->key, 1, slot->xid, &record->ctrc_handle, &identity);
	*slot = saved_slot;
	if (page_owner_stamp_case == 1)
		slot->wrap++;
	UT_ASSERT_EQ(page_owner_touch_count, 2); UT_ASSERT_EQ(page_owner_touch_capacity, 16);
	UT_ASSERT_EQ(page_owner_capture_calls, 5); UT_ASSERT_EQ(page_owner_publication_notes, 5);
	UT_ASSERT_EQ(page_owner_allocations, 1);
	UT_ASSERT(page_owner_context_current == (MemoryContext)&page_owner_wal_cookie);
	UT_ASSERT(memcmp(record->ctrc_handle.participant, &participant, sizeof(participant)) == 0);
	UT_ASSERT(memcmp(record->ctrc_handle.receipt, &receipt, sizeof(receipt)) == 0);
	page_owner_registration_source = NULL;
	page_owner_registered_records += page_owner_touch_count;
	return page_owner_touch_list;
}

/* Execute the original qsort/range grouping with an older proof first only
 * after sorting. The same-page stale record must not borrow the current
 * slot's proof or create a second WAL image/discharge. */
static void
page_owner_finish_original_range(ClusterItlTouchRecord *record,
	const ItlFinishCtx *finish)
{
	ClusterItlTouchRecord local_records[2];
	ClusterItlTouchRecord *records = local_records;
	ClusterItlTouchRecord *saved_list = page_owner_touch_list;

	UT_ASSERT(record->proof.xid > FirstNormalTransactionId);
	UT_ASSERT(record->proof.own_generation > 0);
	if (page_owner_use_registration)
		records = page_owner_register_records(record);
	else
	{
		records[0] = records[1] = *record;
		records[1].proof.xid--;
		records[1].proof.own_generation--;
		page_owner_touch_list = records;
		page_owner_touch_count = lengthof(local_records);
	}
	if (page_owner_use_xact_owner)
	{
		ClusterItlTouchSubxactBoundary boundary = { 77, 1 };

		page_owner_cluster_enabled = true;
		if (!page_owner_use_registration)
			page_owner_touch_capacity = 8;
		page_owner_subxact_stack = &boundary;
		page_owner_subxact_depth = 1; page_owner_subxact_capacity = 4;
		if (finish->is_commit)
			original_page_owner_precommit(record->proof.xid, finish->commit_scn);
		else
			original_page_owner_abort(record->proof.xid);
		UT_ASSERT(page_owner_touch_list == NULL && page_owner_subxact_stack == NULL);
		UT_ASSERT_EQ(page_owner_touch_count, 0); UT_ASSERT_EQ(page_owner_touch_capacity, 0);
		UT_ASSERT_EQ(page_owner_subxact_depth, 0); UT_ASSERT_EQ(page_owner_subxact_capacity, 0);
		UT_ASSERT(!page_owner_page_locked);
		page_owner_cluster_enabled = false;
		page_owner_xact_finishes++;
	}
	else
		original_page_owner_finish_range(0, page_owner_touch_count, finish);
	UT_ASSERT_EQ(records[0].proof.xid, record->proof.xid - 1);
	UT_ASSERT_EQ(records[1].proof.xid, record->proof.xid);
	if (page_owner_use_registration)
		free(records);
	page_owner_touch_count = 0;
	page_owner_touch_list = saved_list;
	page_owner_range_calls++;
}

static void
page_owner_finish_full_batch(ClusterItlTouchRecord *record)
{
	const ClusterCtrcTargetV1 *target = &record->ctrc_handle.receipt->target;
	ClusterCtrcParticipantEntry participant = *record->ctrc_handle.participant;
	ClusterCtrcReceipt receipt = *record->ctrc_handle.receipt;
	unsigned wakeups = receipt_terminal_wakeups;
	char *saved_blocks = BufferBlocks;
	uint16 wrap = target->itl_slot_wrap;

	for (page_owner_stamp_case = 0; page_owner_stamp_case < 4; page_owner_stamp_case++)
	{
		ItlFinishBatchCtx batch;
		ClusterItlSlotData *slot;

		memset(&batch, 0, sizeof(batch));
		batch.finish.is_commit = page_owner_stamp_case == 2;
		batch.finish.commit_scn = batch.finish.is_commit ? 9 : InvalidScn;
		batch.needs_wal = batch.runs[0].needs_wal = true;
		batch.nruns = 1; batch.runs[0].count = 1;
		memcpy(page_owner_stamp_live.data, saved_blocks, BLCKSZ);
		UT_ASSERT(PageHasItl(page_owner_stamp_live.data));
		slot = &ClusterPageGetItlSlots(page_owner_stamp_live.data)[record->key.slot_idx];
		memset(slot, 0, sizeof(*slot));
		slot->xid = record->proof.xid; slot->wrap = wrap;
		slot->flags = ITL_FLAG_ACTIVE; slot->undo_segment_head = record->proof.undo_segment_head;
		record->proof.valid = true; record->proof.buffer_id = 1;
		record->proof.own_generation = 7; record->proof.acquisition_epoch = 9;
		record->proof.slot_class = ITL_FLAG_ACTIVE;
		record->proof.slot_wrap = wrap + (page_owner_stamp_case == 1);
		page_owner_page_locked = false;
		page_owner_starts = page_owner_registers = page_owner_finishes = page_owner_aborts = 0;
		page_owner_unlocks = page_owner_skips = 0;
		page_owner_flush_calls = page_owner_local_reads = page_owner_remote_reads = page_owner_discharge_calls = 0;
		page_owner_local_flush = 400; page_owner_remote_flush = 700;
		BufferBlocks = page_owner_stamp_live.data;
		if (page_owner_use_range)
			page_owner_finish_original_range(record, &batch.finish);
		else
			original_page_owner_flush_batch(&batch);
		BufferBlocks = saved_blocks;
		if (!page_owner_use_range)
			UT_ASSERT_EQ(batch.nruns, 0);
		UT_ASSERT(!page_owner_page_locked);
		UT_ASSERT_EQ(page_owner_starts, 1);
		if (page_owner_use_native_resident)
			UT_ASSERT_EQ(page_owner_unlocks, page_owner_stamp_case >= 2);
		else
			UT_ASSERT_EQ(page_owner_unlocks, page_owner_stamp_case != 0);
		if (page_owner_stamp_case < 2)
		{
			UT_ASSERT_EQ(page_owner_skips, page_owner_use_range ? 2 : 1);
			UT_ASSERT_EQ(page_owner_aborts, 1);
			UT_ASSERT_EQ(page_owner_registers + page_owner_finishes, 0);
			UT_ASSERT_EQ(slot->flags, ITL_FLAG_ACTIVE);
		}
		else
		{
			UT_ASSERT_EQ(page_owner_skips, page_owner_use_range ? 1 : 0);
			UT_ASSERT_EQ(page_owner_aborts, 0);
			UT_ASSERT_EQ(page_owner_registers, 1); UT_ASSERT_EQ(page_owner_finishes, 1);
			UT_ASSERT_EQ(PageGetLSN(page_owner_stamp_live.data), 400);
			UT_ASSERT_EQ(slot->flags, page_owner_stamp_case == 2 ? ITL_FLAG_NEEDS_CLEANOUT : ITL_FLAG_ABORTED);
			UT_ASSERT_EQ(slot->commit_scn, page_owner_stamp_case == 2 ? 9 : InvalidScn);
		}
		if (page_owner_stamp_case < 3)
		{
			UT_ASSERT_EQ(page_owner_flush_calls + page_owner_local_reads + page_owner_remote_reads + page_owner_discharge_calls, 0);
			UT_ASSERT(memcmp(record->ctrc_handle.participant, &participant, sizeof(participant)) == 0);
			UT_ASSERT(memcmp(record->ctrc_handle.receipt, &receipt, sizeof(receipt)) == 0);
			UT_ASSERT_EQ(receipt_terminal_wakeups, wakeups);
		}
		else
		{
			UT_ASSERT_EQ(record->ctrc_handle.receipt->state, CTRC_RECEIPT_CLEANED);
			UT_ASSERT_EQ(page_owner_flush_calls, 1); UT_ASSERT_EQ(page_owner_local_reads, 1);
			UT_ASSERT_EQ(page_owner_remote_reads, CLUSTER_SF_DEP_MAX_ORIGINS - 1);
			UT_ASSERT_EQ(page_owner_discharge_calls, 1);
			UT_ASSERT_EQ(receipt_terminal_wakeups, wakeups + 1);
		}
	}
}

static bool page_owner_use_full_batch;
static void
page_owner_finish_discharge(const ClusterCtrcReceiptHandle *handle)
{
	const ClusterCtrcTargetV1 *target = &handle->receipt->target;
	ClusterItlTouchRecord record;
	ItlFinishDischarge pending = {0, 0};
	ClusterSfDepVec dependencies;
	ClusterCtrcParticipantEntry participant = *handle->participant;
	ClusterCtrcReceipt receipt = *handle->receipt;
	unsigned wakeups = receipt_terminal_wakeups;

	memset(&record, 0, sizeof(record)); memset(&dependencies, 0, sizeof(dependencies));
	record.key.rloc.spcOid = target->spc_oid; record.key.rloc.dbOid = target->db_oid;
	record.key.rloc.relNumber = target->rel_number; record.key.block = target->block_number;
	record.key.forknum = target->fork_number; record.key.slot_idx = target->itl_slot_index;
	record.key.flags = target->needs_wal ? CLUSTER_ITL_TOUCH_FLAG_NEEDS_WAL : 0;
	record.proof.xid = target->itl_xid; record.proof.slot_wrap = target->itl_slot_wrap;
	record.proof.slot_class = target->itl_class == 2 ? ITL_FLAG_LOCK_ONLY_ACTIVE : ITL_FLAG_ACTIVE;
	memcpy(&record.proof.undo_segment_head, target->uba, sizeof(record.proof.undo_segment_head));
	record.ctrc_handle = *handle;
	dependencies.required[0] = 400; dependencies.required[1] = 700;
	page_owner_touch_list = &record; page_owner_node_id = 0;
	page_owner_flush_calls = page_owner_local_reads = page_owner_remote_reads = page_owner_discharge_calls = 0;
	if (page_owner_use_full_batch)
	{
		page_owner_finish_full_batch(&record);
		page_owner_touch_list = NULL;
		return;
	}
	original_page_owner_discharge_batch(true, 400, &pending, 0, &dependencies);
	UT_ASSERT_EQ(page_owner_flush_calls + page_owner_local_reads + page_owner_remote_reads + page_owner_discharge_calls, 0);
	for (unsigned fault = 0; fault < 4; fault++)
	{
		unsigned calls = page_owner_flush_calls, reads = page_owner_remote_reads;
		unsigned discharges = page_owner_discharge_calls;

		record.proof.slot_wrap = target->itl_slot_wrap + (fault == 0);
		page_owner_local_flush = fault == 1 ? 399 : 400;
		page_owner_remote_flush = fault == 2 ? 699 : 700;
		original_page_owner_discharge_batch(true, fault == 3 ? InvalidXLogRecPtr : 400,
			&pending, 1, &dependencies);
		UT_ASSERT_EQ(page_owner_flush_calls, calls + (fault != 3));
		UT_ASSERT_EQ(page_owner_local_reads, page_owner_flush_calls);
		UT_ASSERT_EQ(page_owner_remote_reads, reads + (fault != 3 ? CLUSTER_SF_DEP_MAX_ORIGINS - 1 : 0));
		UT_ASSERT_EQ(page_owner_discharge_calls, discharges + (fault != 3));
		UT_ASSERT(memcmp(handle->participant, &participant, sizeof(participant)) == 0);
		UT_ASSERT(memcmp(handle->receipt, &receipt, sizeof(receipt)) == 0);
		UT_ASSERT_EQ(receipt_terminal_wakeups, wakeups);
	}
	record.proof.slot_wrap = target->itl_slot_wrap;
	page_owner_local_flush = 400; page_owner_remote_flush = 700;
	original_page_owner_discharge_batch(true, 400, &pending, 1, &dependencies);
	UT_ASSERT_EQ(handle->receipt->state, CTRC_RECEIPT_CLEANED);
	UT_ASSERT_EQ(page_owner_flush_calls, 4); UT_ASSERT_EQ(page_owner_local_reads, 4);
	UT_ASSERT_EQ(page_owner_remote_reads, 4 * (CLUSTER_SF_DEP_MAX_ORIGINS - 1));
	UT_ASSERT_EQ(page_owner_discharge_calls, 4);
	UT_ASSERT_EQ(receipt_terminal_wakeups, wakeups + 1);
	page_owner_touch_list = NULL;
}

static bool native_exit_use_page_owner_discharge;
static void
native_exit_applied_receipt_discharge(const ClusterCtrcReceiptHandle *handle)
{
	const ClusterCtrcTargetV1 *target = &handle->receipt->target;
	ClusterCtrcItlTargetIdentity expected;
	ClusterCtrcDurability durability;
	ClusterCtrcParticipantEntry saved_participant = *handle->participant;
	ClusterCtrcReceipt saved_receipt = *handle->receipt;
	unsigned wakeups = receipt_terminal_wakeups;

	UT_ASSERT_EQ(handle->receipt->state, CTRC_RECEIPT_APPLIED);
	memset(&expected, 0, sizeof(expected));
	expected.spc_oid = target->spc_oid; expected.db_oid = target->db_oid;
	expected.rel_number = target->rel_number; expected.fork_number = target->fork_number;
	expected.block_number = target->block_number; expected.itl_xid = target->itl_xid;
	expected.itl_slot_index = target->itl_slot_index; expected.itl_slot_wrap = target->itl_slot_wrap;
	expected.itl_class = target->itl_class; expected.needs_wal = target->needs_wal;
	memcpy(expected.uba, target->uba, sizeof(expected.uba));
	memset(&durability, 0, sizeof(durability));
	durability.highest_local_lsn = durability.local_flush_lsn = 400;
	durability.required_lsn[0] = durability.durable_lsn[0] = 700;
	UT_ASSERT(receipt_terminal_watched == NULL && !receipt_discharge_wakeup_active);
	receipt_terminal_watched = handle; receipt_discharge_wakeup_active = true;
	for (unsigned fault = 0; fault < 3; fault++)
	{
		ClusterCtrcItlTargetIdentity wrong_target = expected;
		ClusterCtrcDurability insufficient = durability;

		if (fault == 0) wrong_target.itl_slot_wrap++;
		if (fault == 1) insufficient.local_flush_lsn--;
		if (fault == 2) insufficient.durable_lsn[0]--;
		UT_ASSERT_EQ(original_terminal_receipt_discharge_itl_shared(handle, &wrong_target,
			CTRC_ITL_TERMINAL_INDEPENDENT, &insufficient), CLUSTER_CTRC_DISCHARGE_RETAIN);
		UT_ASSERT(memcmp(handle->participant, &saved_participant, sizeof(saved_participant)) == 0);
		UT_ASSERT(memcmp(handle->receipt, &saved_receipt, sizeof(saved_receipt)) == 0);
		UT_ASSERT_EQ(receipt_terminal_wakeups, wakeups);
		UT_ASSERT_EQ(participant_lock_depth, 0);
	}
	if (native_exit_use_page_owner_discharge)
		page_owner_finish_discharge(handle);
	else
	{
		UT_ASSERT_EQ(original_terminal_receipt_discharge_itl_shared(handle, &expected,
			CTRC_ITL_TERMINAL_INDEPENDENT, &durability), CLUSTER_CTRC_DISCHARGE_CLEANED);
	}
	receipt_terminal_watched = NULL; receipt_discharge_wakeup_active = false;
	UT_ASSERT_EQ(handle->receipt->state, CTRC_RECEIPT_CLEANED);
	UT_ASSERT_EQ(handle->receipt->disposition, CTRC_RELEASE_CLEANED_TERMINAL_REWRITE);
	UT_ASSERT_EQ(handle->receipt->highest_local_wal_lsn, 400);
	UT_ASSERT_EQ(handle->receipt->required_lsn[0], native_exit_use_page_owner_discharge ? 400 : 700);
	if (native_exit_use_page_owner_discharge)
		UT_ASSERT_EQ(handle->receipt->required_lsn[1], 700);
	UT_ASSERT_EQ(handle->participant->applied_count, 0);
	UT_ASSERT_EQ(handle->participant->cleaned_count, 1);
	UT_ASSERT_EQ(handle->participant->receipt_count, 1);
	UT_ASSERT(FullTransactionIdEquals(handle->participant->original_full_xid, handle->original_full_xid));
	UT_ASSERT_EQ(receipt_terminal_wakeups, wakeups + 1);
	UT_ASSERT_EQ(participant_lock_depth, 0);
}

static void
native_exit_certificate_retire(const ClusterCtrcReceiptHandle *handle, bool final_reply)
{
	ClusterCtrcOriginCertificateSnapshot snapshot;
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcLocalReleaseAckV1 *ack;
	uint64 index;
	uint16 node = handle->participant->identity.node_id;

	if (handle->receipt->state == CTRC_RECEIPT_APPLIED)
		native_exit_applied_receipt_discharge(handle);
	UT_ASSERT(handle->receipt->state == CTRC_RECEIPT_CANCELLED
		|| handle->receipt->state == CTRC_RECEIPT_CLEANED);
	UT_ASSERT_EQ(original_certificate_participant_close(handle->participant, &handle->participant->identity,
		handle->participant->grant_generation, 17), CLUSTER_CTRC_CLOSE_ACK_READY);
	ack_observers_reset(handle);
	UT_ASSERT(census_original_nonempty_ack_freeze(handle));
	UT_ASSERT_EQ(ack_capture_calls, 2); UT_ASSERT_EQ(ack_barriers, 1);
	UT_ASSERT_EQ(ack_alloc_live, 0); UT_ASSERT_EQ(handle->receipt->state, CTRC_RECEIPT_ACK_FROZEN);
	UT_ASSERT(original_census_origin_index(&handle->key, &index));
	origin = &ctrc_origin_entries()[index];
	ack = &original_certificate_participant_ack_entries()[handle->participant_index];
	participant_publisher_lock_acquire(&CtrcShared->origin_lock, LW_EXCLUSIVE);
	UT_ASSERT(original_origin_reply_origin_begin_seal_entry(origin, 17));
	UT_ASSERT(original_origin_reply_origin_arm_close_entry(origin, node, 11));
	UT_ASSERT(original_origin_reply_origin_note_close_reply_entry(origin, node, 11, CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK));
	UT_ASSERT(original_origin_reply_origin_begin_cleaning_entry(origin));
	participant_publisher_lock_release(&CtrcShared->origin_lock);
	UT_ASSERT(original_origin_reply_origin_ack_land_shared(11, ack));
	participant_publisher_lock_acquire(&CtrcShared->origin_lock, LW_EXCLUSIVE);
	UT_ASSERT(original_origin_reply_origin_certificate_snapshot_index_locked(index, &snapshot));
	participant_publisher_lock_release(&CtrcShared->origin_lock);
	durable_observers_reset(&snapshot);
	UT_ASSERT(census_origin_durable_certificate_publish(&snapshot));
	UT_ASSERT_EQ(durable_commits, 1); UT_ASSERT_EQ(durable_wals, 1); UT_ASSERT_EQ(durable_flushes, 1);
	UT_ASSERT_EQ(durable_enters, durable_leaves); UT_ASSERT(!durable_admitted && !durable_current && !durable_pinned);
	UT_ASSERT_EQ(origin->state, CTRC_ORIGIN_RELEASE_PROVEN);
	UT_ASSERT(FullTransactionIdEquals(origin->original_full_xid, handle->original_full_xid));
	participant_publisher_lock_acquire(&CtrcShared->origin_lock, LW_EXCLUSIVE);
	UT_ASSERT(original_origin_reply_origin_arm_certificate_entry(origin, node, 123));
	participant_publisher_lock_release(&CtrcShared->origin_lock);
	certificate_observers_reset();
	UT_ASSERT(origin_reply_reclaim_receiver(handle));
	UT_ASSERT(ctrc_bytes_zero(handle->participant, sizeof(*handle->participant)));
	UT_ASSERT(ctrc_bytes_zero(handle->receipt, sizeof(*handle->receipt)));
	UT_ASSERT(FullTransactionIdEquals(origin->original_full_xid, handle->original_full_xid));
	if (final_reply)
	{
		UT_ASSERT(census_origin_certificate_reply(123, node));
		UT_ASSERT(ctrc_bytes_zero(origin, sizeof(*origin)));
		UT_ASSERT(!census_origin_certificate_reply(123, node));
	}
	UT_ASSERT_EQ(participant_lock_depth, 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation), 1);
	UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
}

static void phase_recent_native_setup(QualifiedXminFixture *, LVRelState *,
	ClusterHeapFreezeXminTargetV1 *, bool, bool);
static void phase_recent_native_finish(const QualifiedXminFixture *, const LVRelState *, bool);
static ClusterSemanticActiveSampleFence source_exit_member_source;

static void
source_exit_member_wakeup(PGSemaphore sema)
{
	PGPROC *member = &sampler_slots[1];
	FullTransactionId reference, future;
	ClusterXidStripeSampleFence fence;
	ClusterCtrcActiveScan scan;
	uint64 census_before = census_step_lock_acquires;

	if (!source_exit_member_requested) { UT_ASSERT(false); abort(); }
	UT_ASSERT(!source_exit_lock_held && !sampler_lock_held && !census_step_lock_held);
	UT_ASSERT(!local_x && !local_share && backend_pin_count == 0);
	UT_ASSERT_EQ(source_exit_locks, 1); UT_ASSERT_EQ(source_exit_unlocks, 1);
	UT_ASSERT(sema == member->sem && source_exit_member_wakeups == 0);
	UT_ASSERT(!member->procArrayGroupMember && sampler_slots[0].procArrayGroupMember);
	UT_ASSERT_EQ(pg_atomic_read_u32(&member->procArrayGroupNext), INVALID_PGPROCNO);
	for (unsigned i = 0; i < 2; i++) {
		UT_ASSERT_EQ(sampler_slots[i].xid, InvalidTransactionId);
		UT_ASSERT_EQ(sampler_xids[i], InvalidTransactionId);
		UT_ASSERT_EQ(sampler_slots[i].xmin, InvalidTransactionId);
		UT_ASSERT_EQ(sampler_slots[i].lxid, InvalidLocalTransactionId);
		UT_ASSERT_EQ(pg_atomic_read_u64(&sampler_slots[i].cluster_top_full_xid_atomic), 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&sampler_slots[i].cluster_read_scn_atomic), 0);
		UT_ASSERT_EQ(source_exit_subxids[i].count, 0);
		UT_ASSERT_EQ(source_exit_status[i] & PROC_VACUUM_STATE_MASK, 0);
	}
	UT_ASSERT(original_native_fullxid_sample(&reference, &future, &source_exit_member_wake_minimum, &fence));
	UT_ASSERT(FullTransactionIdEquals(source_exit_member_wake_minimum, future));
	UT_ASSERT(original_census_scan_begin(&scan, reference));
	scan.future_full = future; scan.min_full = source_exit_member_wake_minimum;
	for (uint64 slices = 0; scan.status == CTRC_ACTIVE_SCAN_INCOMPLETE && slices <= scan.total_entries; slices++)
		original_census_step_source(&scan, &source_exit_member_source);
	UT_ASSERT_EQ(scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(scan.next_index, scan.total_entries); UT_ASSERT(!scan.lock_busy);
	UT_ASSERT_EQ(scan.unqualified_entries, 0);
	UT_ASSERT(cluster_ctrc_active_scan_registration_current(&scan));
	UT_ASSERT_EQ(census_step_lock_acquires - census_before, scan.total_entries);
	UT_ASSERT_EQ(census_step_lock_releases, census_step_lock_acquires);
	source_exit_member_census_minimum = scan.min_full;
	UT_ASSERT(FullTransactionIdEquals(scan.min_full, source_exit_member_wait_minimum));
	if (source_exit_history_requested)
	{
		ClusterUndoRetentionSample history, zero = {0};
		PGPROC *reader = &sampler_slots[2];

		/* The queued transactions have retired, but this independent
		 * snapshot is still installed. Unknown SCN and a replication pin
		 * each refuse the original sampler; neither may become CLOCK. */
		UT_ASSERT_EQ(reader->xmin, 90);
		pg_atomic_write_u64(&reader->cluster_read_scn_atomic, InvalidScn);
		memset(&history, 0xa5, sizeof(history));
		UT_ASSERT(!bounds_original_retention_sample(&history));
		UT_ASSERT(memcmp(&history, &zero, sizeof(history)) == 0);
		pg_atomic_write_u64(&reader->cluster_read_scn_atomic, 10);
		sampler_proc_array->replication_slot_xmin = 90;
		memset(&history, 0xa5, sizeof(history));
		UT_ASSERT(!bounds_original_retention_sample(&history));
		UT_ASSERT(memcmp(&history, &zero, sizeof(history)) == 0);
		sampler_proc_array->replication_slot_xmin = InvalidTransactionId;
		UT_ASSERT(bounds_original_local_report(&history));
		UT_ASSERT_EQ(history.clock_scn, 20); UT_ASSERT_EQ(history.horizon_scn, 10);
		UT_ASSERT_EQ(history.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
		UT_ASSERT_EQ(history.contributor_token, 3);
		UT_ASSERT_EQ(bounds_history_locks, 3); UT_ASSERT_EQ(bounds_history_unlocks, 3);
		UT_ASSERT(!bounds_history_held);
		source_exit_history_report = history;
		source_exit_history_samples++;
	}
	source_exit_member_wakeups++;
}

static void
source_exit_receiver_on_second_pin(void)
{
	if (recent_pin_calls == 2) recent_register_unknown_receiver();
}

static void
full_source_then_recent_lifecycle_case(bool batch, bool late_receiver, bool retire_native, unsigned terminal_receipt, unsigned certificate_retirement)
{
	QualifiedXminFixture f;
	LVRelState v;
	ClusterHeapFreezeXminTargetV1 targets[3];
	CensusRuntimeAllocation runtime;
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcOriginReservation reservation;
	ClusterCtrcReceiptHandle handle;
	FullTransactionId full = FullTransactionIdFromEpochAndXid(1, 100);
	uint32 grant;
	uint64 total, passes = 1, expected_passes;

	if (source_exit_group_requested)
	{
		phase_recent_native_setup(&f, &v, targets, batch, true);
		/* Source sampling precedes the admitted phase and owns no heap pin. */
		phase_pin_released = true;
	}
	else
		phase_setup(&f, &v, targets, batch);
	census_runtime_initialize_requested = true;
	if (!census_full_layout_setup(&runtime, 2)) { UT_ASSERT(false); return; }
	key = census_key_for_full(&f.cohort.admission_fence, full);
	if (retire_native)
	{
		census_origin_publish_backend(&key, full);
		sampler_slots[0].xid = key.xid;
		census_origin_proc = &sampler_slots[0];
		UT_ASSERT_EQ(original_census_origin_reserve_active(&key, &reservation),
			CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
	}
	else
		UT_ASSERT_EQ(census_origin_reserve_from_backend(&key, full, &reservation),
			CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
	UT_ASSERT(original_census_origin_open_reserved(&reservation, &grant));
	UT_ASSERT_EQ(census_participant_prepare_from_origin(&key, grant, &handle),
		CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT(handle.valid && FullTransactionIdEquals(handle.original_full_xid, full));
	if (retire_native)
	{
		FullTransactionId reference, future, minimum;
		ClusterXidStripeSampleFence fence;
		PGPROC *proc = &sampler_slots[0];

		UT_ASSERT(census_origin_proc == proc);
		UT_ASSERT_EQ(proc->xid, key.xid); UT_ASSERT_EQ(sampler_xids[0], key.xid);
		UT_ASSERT_EQ(pg_atomic_read_u64(&proc->cluster_top_full_xid_atomic), U64FromFullTransactionId(full));
		memset(source_exit_subxids, 0, sizeof(source_exit_subxids));
		memset(source_exit_status, 0, sizeof(source_exit_status));
		sampler_procs.subxidStates = source_exit_subxids;
		sampler_procs.statusFlags = source_exit_status;
		proc->pgxactoff = 0; proc->lxid = 7; proc->xmin = key.xid;
		pg_atomic_write_u64(&proc->cluster_read_scn_atomic, 9);
		sampler_variables.oldestXid = FirstNormalTransactionId;
		sampler_variables.latestCompletedXid = FullTransactionIdFromEpochAndXid(1, 99);
		source_exit_locks = source_exit_unlocks = 0; source_exit_lock_held = false;
		if (source_exit_group_requested)
		{
			proc->pgprocno = 0; proc->procArrayGroupMember = false;
			pg_atomic_init_u32(&proc->procArrayGroupNext, INVALID_PGPROCNO);
			pg_atomic_init_u32(&sampler_procs.procArrayGroupFirst, INVALID_PGPROCNO);
		}
		if (source_exit_member_requested)
		{
			PGPROC *member = &sampler_slots[1];
			FullTransactionId other = FullTransactionIdFromU64(U64FromFullTransactionId(full) + 64);

			member->pgprocno = member->pgxactoff = 1; member->pid = proc->pid + 1;
			member->xid = member->xmin = sampler_xids[1] = XidFromFullTransactionId(other);
			member->lxid = 24; member->sem = (PGSemaphore)member;
			member->subxidStatus.count = 2;
			member->subxids.xids[0] = member->xid + 1; member->subxids.xids[1] = member->xid + 2;
			source_exit_subxids[1] = member->subxidStatus;
			member->statusFlags = source_exit_status[1] = PROC_IN_VACUUM;
			pg_atomic_write_u64(&member->cluster_top_full_xid_atomic, U64FromFullTransactionId(other));
			pg_atomic_write_u64(&member->cluster_read_scn_atomic, 11);
			pg_atomic_init_u32(&member->procArrayGroupNext, INVALID_PGPROCNO);
			sampler_proc_array->numProcs = 2; sampler_proc_array->pgprocnos[1] = 1;
			source_exit_member_wakeups = 0;
			source_exit_member_wait_minimum = source_exit_member_wake_minimum = InvalidFullTransactionId;
			source_exit_member_census_minimum = InvalidFullTransactionId;
			source_exit_member_source = f.cohort.admission_fence;
		}
		if (source_exit_history_requested)
		{
			PGPROC *reader = &sampler_slots[2];

			UT_ASSERT(source_exit_group_requested && source_exit_member_requested);
			/* Snapshot publication is a controlled leaf. Both complete
			 * original samplers consume this same compact ProcArray. */
			reader->pgprocno = reader->pgxactoff = 2;
			reader->pid = proc->pid + 2; reader->xmin = 90;
			reader->xid = sampler_xids[2] = InvalidTransactionId;
			pg_atomic_write_u64(&reader->cluster_top_full_xid_atomic, 0);
			pg_atomic_write_u64(&reader->cluster_read_scn_atomic, 10);
			sampler_proc_array->numProcs = 3; sampler_proc_array->pgprocnos[2] = 2;
			bounds_history_clock = 20;
			fixture_wal_clock = 20;
		}
		source_exit_end_transaction(proc, key.xid);
		if (source_exit_history_requested)
		{
			/* The cached expected source comes from the original wake-time
			 * sampler; the admitted cohort independently samples it again. */
			f.cohort.source_local_history.sample_clock_scn = source_exit_history_report.clock_scn;
			f.cohort.source_local_history.contributor_kind = source_exit_history_report.contributor_kind;
			f.cohort.source_local_history.contributor_token = source_exit_history_report.contributor_token;
		}

		UT_ASSERT_EQ(source_exit_locks, 1); UT_ASSERT_EQ(source_exit_unlocks, 1);
		UT_ASSERT(!source_exit_lock_held);
		UT_ASSERT_EQ(proc->xid, InvalidTransactionId); UT_ASSERT_EQ(sampler_xids[0], InvalidTransactionId);
		UT_ASSERT_EQ(proc->xmin, InvalidTransactionId); UT_ASSERT_EQ(proc->lxid, InvalidLocalTransactionId);
		UT_ASSERT_EQ(pg_atomic_read_u64(&proc->cluster_top_full_xid_atomic), 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&proc->cluster_read_scn_atomic), 0);
		UT_ASSERT(FullTransactionIdEquals(sampler_variables.latestCompletedXid,
			source_exit_member_requested ? FullTransactionIdFromU64(U64FromFullTransactionId(full) + 66) : full));
		UT_ASSERT_EQ(sampler_variables.xactCompletionCount, source_exit_member_requested ? 2 : 1);
		if (source_exit_member_requested) {
			UT_ASSERT_EQ(source_exit_member_wakeups, 1);
			UT_ASSERT(FullTransactionIdEquals(source_exit_member_wait_minimum, full));
			UT_ASSERT(FullTransactionIdEquals(source_exit_member_census_minimum, full));
			UT_ASSERT(FullTransactionIdFollows(source_exit_member_wake_minimum, full));
			UT_ASSERT(!sampler_slots[1].procArrayGroupMember);
		}
		if (source_exit_group_requested)
		{
			UT_ASSERT(!proc->procArrayGroupMember);
			UT_ASSERT_EQ(pg_atomic_read_u32(&proc->procArrayGroupNext), INVALID_PGPROCNO);
			UT_ASSERT_EQ(pg_atomic_read_u32(&sampler_procs.procArrayGroupFirst), INVALID_PGPROCNO);
		}
		UT_ASSERT(original_native_fullxid_sample(&reference, &future, &minimum, &fence));
		UT_ASSERT(FullTransactionIdEquals(minimum, future));
		UT_ASSERT(FullTransactionIdFollows(minimum, full));
		UT_ASSERT(FullTransactionIdEquals(handle.participant->original_full_xid, full));
	}
	else
	{
		sampler_xids[0] = 152;
		pg_atomic_write_u64(&sampler_slots[0].cluster_top_full_xid_atomic,
			U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(1, 152)));
	}
	if (terminal_receipt)
	{
		uint64 origin_index;

		UT_ASSERT(retire_native);
		UT_ASSERT_EQ(sampler_xids[0], InvalidTransactionId);
		UT_ASSERT_EQ(pg_atomic_read_u64(&sampler_slots[0].cluster_top_full_xid_atomic), 0);
		receipt_terminal_wakeups = 0;
		if (terminal_receipt == 1)
		{
			UT_ASSERT(census_receipt_cancel_retained(&handle));
			UT_ASSERT_EQ(receipt_terminal_wakeups, 1);
		}
		else
		{
			ClusterCtrcTargetV1 final = receipt_terminal_final_target(&handle);
			ClusterCtrcApplyToken token;

			UT_ASSERT_EQ(original_terminal_receipt_apply_shared(&handle, &final, &token),
				CLUSTER_CTRC_APPLY_APPLIED);
			UT_ASSERT(token.valid);
			UT_ASSERT_EQ(token.journal_slot_generation, handle.journal_slot_generation);
			UT_ASSERT_EQ(receipt_terminal_wakeups, 0);
		}
		/* A terminal receipt remains retained after its native PGPROC source
		 * has gone. It is not a certificate or permission to advance F. */
		UT_ASSERT_EQ(handle.receipt->state, terminal_receipt == 1 ? CTRC_RECEIPT_CANCELLED : CTRC_RECEIPT_APPLIED);
		UT_ASSERT_EQ(handle.participant->state, CTRC_PARTICIPANT_OPEN);
		UT_ASSERT_EQ(handle.participant->receipt_count, 1);
		UT_ASSERT_EQ(handle.participant->prepared_count, 0);
		UT_ASSERT_EQ(handle.participant->cancelled_count, terminal_receipt == 1);
		UT_ASSERT_EQ(handle.participant->applied_count, terminal_receipt == 2);
		UT_ASSERT(FullTransactionIdEquals(handle.participant->original_full_xid, full));
		UT_ASSERT(original_census_origin_index(&key, &origin_index));
		UT_ASSERT_EQ(ctrc_origin_entries()[origin_index].state, CTRC_ORIGIN_OPEN);
		UT_ASSERT(FullTransactionIdEquals(ctrc_origin_entries()[origin_index].original_full_xid, full));
		UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation), 1);
		UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
	}
	total = runtime.capacity.origin_key_entries + runtime.capacity.participant_key_entries;
	expected_passes = (total + CLUSTER_CTRC_ACTIVE_SCAN_SLICE - 1) / CLUSTER_CTRC_ACTIVE_SCAN_SLICE;
	native_worker_shared.active_sample_sequence = f.cohort.source_local_sample_id - 1;
	worker_original_sample_pass_fixture(&f.cohort);
	while (pass_pending && passes <= expected_passes) { original_worker_sample_active_pass(); passes++; }
	UT_ASSERT_EQ(passes, expected_passes); UT_ASSERT(!pass_pending);
	UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(publisher_scan.total_entries, total); UT_ASSERT_EQ(publisher_scan.next_index, total);
	UT_ASSERT(!publisher_scan.lock_busy); UT_ASSERT_EQ(publisher_scan.unqualified_entries, 0);
	UT_ASSERT_EQ(publisher_scan.unqualified_registration_generation, 1);
	UT_ASSERT(FullTransactionIdEquals(publisher_scan.min_full, full));
	/* Preserve both full traversals in member mode: the original wake-time
	 * observation and this later complete original worker publication. */
	UT_ASSERT_EQ(census_step_lock_acquires, total * (source_exit_member_requested ? 2 : 1));
	UT_ASSERT_EQ(census_step_lock_releases, total * (source_exit_member_requested ? 2 : 1));
	UT_ASSERT_EQ(native_worker_shared.active_sample.sample_id, f.cohort.source_local_sample_id);
	f.cohort.local_registration_cut = publisher_scan;
	UT_ASSERT(cluster_ctrc_active_scan_registration_current(&f.cohort.local_registration_cut));
	UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
	if (certificate_retirement)
	{
		ClusterUndoCleanerActiveSample old_sample = native_worker_shared.active_sample;
		uint64 before_locks = census_step_lock_acquires;
		FullTransactionId expected = certificate_retirement == 2
			? publisher_scan.future_full : full;

		UT_ASSERT(retire_native && (terminal_receipt == 1 || terminal_receipt == 2) && !late_receiver);
		native_exit_certificate_retire(&handle, certificate_retirement == 2);
		UT_ASSERT_EQ(handle.participant->receipt_count, 0);
		/* A new complete original pass, not an in-place edit of the old cut. */
		passes = 1;
		worker_original_sample_pass_fixture(&f.cohort);
		while (pass_pending && passes <= expected_passes) { original_worker_sample_active_pass(); passes++; }
		UT_ASSERT_EQ(passes, expected_passes); UT_ASSERT(!pass_pending);
		UT_ASSERT_EQ(publisher_scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
		UT_ASSERT_EQ(publisher_scan.next_index, total); UT_ASSERT_EQ(publisher_scan.unqualified_entries, 0);
		UT_ASSERT(FullTransactionIdEquals(publisher_scan.min_full, expected));
		UT_ASSERT_EQ(census_step_lock_acquires, before_locks + total);
		UT_ASSERT_EQ(census_step_lock_releases, census_step_lock_acquires);
		UT_ASSERT_EQ(native_worker_shared.active_sample.sample_id, old_sample.sample_id + 1);
		UT_ASSERT(!original_worker_publication_current(&old_sample));
		/* These fields come from the new original publication. The admitted
		 * phase still acquires and checks its own full cohort before writing. */
		f.cohort.active_full_xid_min = U64FromFullTransactionId(publisher_scan.min_full);
		f.cohort.source_local_sample_id = native_worker_shared.active_sample.sample_id;
		f.cohort.local_registration_cut = publisher_scan;
		UT_ASSERT(cluster_ctrc_active_scan_registration_current(&f.cohort.local_registration_cut));
		UT_ASSERT_EQ(pg_atomic_read_u32(&native_admission_shared.inflight
			[CLUSTER_SEMANTIC_TARGET_SIDE][bounds_lifecycle_feature()]), source_exit_group_requested ? 8 : 1);
	}
	if (late_receiver)
	{
		uint64 known_index, late_index;

		recent_registration_cut = publisher_scan;
		recent_registration_key = key;
		recent_registration_key.origin_node_id = 1;
		recent_registration_key.owner_instance = 2;
		recent_registration_key.segment_id = CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
		/* The late publication uses a different physical slot. Reusing slot0
		 * would correctly refuse at the original participant owner guard. */
		recent_registration_key.slot_offset = key.slot_offset + 1;
		UT_ASSERT(original_participant_participant_index(&key, cluster_node_id, &known_index));
		UT_ASSERT(original_participant_participant_index(&recent_registration_key, cluster_node_id, &late_index));
		UT_ASSERT(known_index != late_index);
		recent_registration_refused = false; recent_registration_calls = 0;
		recent_pin_observer = source_exit_group_requested
			? source_exit_receiver_on_second_pin : recent_register_unknown_receiver;
	}
	backend_pin_count = 0;
	relation_data.rd_isvalid = resume_allowed = true;
	if (source_exit_group_requested) phase_pin_released = false;
	UT_ASSERT_EQ(phase_then_recent_native_owned(&f, &v), !late_receiver);
	if (source_exit_history_requested)
	{
		UT_ASSERT_EQ(source_exit_history_samples, 1);
		UT_ASSERT_EQ(v.shared_freeze_cohort.horizon_scn, 10);
		UT_ASSERT_EQ(v.shared_freeze_cohort.source_local_horizon_scn, 10);
		UT_ASSERT_EQ(v.shared_freeze_cohort.source_local_history.sample_clock_scn, 20);
		UT_ASSERT_EQ(v.shared_freeze_cohort.source_local_history.contributor_kind,
			CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
		UT_ASSERT_EQ(v.shared_freeze_cohort.source_local_history.contributor_token, 3);
		UT_ASSERT_EQ(bounds_history_locks, 4); UT_ASSERT_EQ(bounds_history_unlocks, 4);
		UT_ASSERT_EQ(bounds_history_clocks, 4); UT_ASSERT(!bounds_history_held);
		UT_ASSERT_EQ(sampler_slots[2].xmin, 90);
		UT_ASSERT_EQ(inserted_wal_clock, late_receiver ? InvalidScn : 20);
		UT_ASSERT_EQ(pg_atomic_read_u64(&sampler_slots[2].cluster_read_scn_atomic), 10);
	}

	recent_pin_observer = NULL;
	UT_ASSERT_EQ(phase_preflights, 1); UT_ASSERT_EQ(phase_rechecks, 1);
	UT_ASSERT_EQ(phase_resolves, f.page.carrier_request_count);
	UT_ASSERT_EQ(recent_pin_calls, source_exit_group_requested ? 2 : 1);
	UT_ASSERT_EQ(recent_unpins, source_exit_group_requested ? 2 : 1);
	UT_ASSERT_EQ(conditional_resumes, (source_exit_group_requested ? 1 : 0) + !late_receiver);
	UT_ASSERT_EQ(conditional_unlocks, !late_receiver);
	UT_ASSERT_EQ(inserts, !late_receiver); UT_ASSERT_EQ(flushes, !late_receiver);
	UT_ASSERT_EQ(phase_finally_calls, 1); UT_ASSERT_EQ(phase_finally_debt, source_exit_group_requested ? 8 : 1);
	UT_ASSERT_EQ(phase_finally_pins, 0); UT_ASSERT(!phase_finally_owned_writer);
	UT_ASSERT_EQ(backend_pin_count, 0);
	UT_ASSERT(!recent_pin_held && !local_x && !resumed_writer);
	if (!certificate_retirement)
	{
		UT_ASSERT(FullTransactionIdEquals(handle.participant->original_full_xid, full));
		UT_ASSERT_EQ(handle.participant->receipt_count, 1);
	}
	else
	{
		UT_ASSERT(ctrc_bytes_zero(handle.participant, sizeof(*handle.participant)));
		UT_ASSERT(ctrc_bytes_zero(handle.receipt, sizeof(*handle.receipt)));
	}
	if (terminal_receipt && !certificate_retirement)
	{
		UT_ASSERT_EQ(handle.receipt->state, terminal_receipt == 1 ? CTRC_RECEIPT_CANCELLED : CTRC_RECEIPT_APPLIED);
		UT_ASSERT_EQ(handle.participant->prepared_count, 0);
		UT_ASSERT_EQ(receipt_terminal_wakeups, terminal_receipt == 1);
	}
	if (late_receiver)
	{
		UT_ASSERT_EQ(recent_registration_calls, 1);
		UT_ASSERT_EQ(pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation), 2);
		UT_ASSERT(!cluster_ctrc_active_scan_registration_current(&publisher_scan));
		UT_ASSERT_EQ(edges + dirties, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
	if (source_exit_group_requested) phase_recent_native_finish(&f, &v, !late_receiver);
	else bounds_lifecycle_assert_empty(&f.admission);
	census_full_layout_release(&runtime);
}

static void
full_source_then_recent_case(bool batch, bool late_receiver)
{
	full_source_then_recent_lifecycle_case(batch, late_receiver, false, 0, 0);
}

UT_TEST(test_original_lookup_pin_feeds_resident_stamp_and_recent_freeze)
{
	page_owner_use_original_lookup_pin = true;
	page_owner_original_lookup_pin_calls = 0;
	page_owner_lookup_private_reads = page_owner_lookup_pin_leaf_calls = 0;
	page_owner_use_native_resident = page_owner_use_native_authority = true;
	page_owner_native_resident_calls = 0;
	page_owner_resident_pins = page_owner_resident_unpins = 0;
	page_owner_resident_releases = page_owner_resident_successes = 0;
	page_owner_resident_missing = page_owner_resident_wrap = page_owner_resident_generation = 0;
	page_owner_native_authority_calls = page_owner_native_authority_refusals = 0;
	page_owner_use_registration = page_owner_use_xact_owner = page_owner_use_range = page_owner_use_full_batch = native_exit_use_page_owner_discharge = true;
	page_owner_registered_records = 0;
	for (unsigned retirement = 1; retirement <= 2; retirement++)
		for (unsigned batch = 0; batch < 2; batch++)
			full_source_then_recent_lifecycle_case(batch != 0, false, true, 2, retirement);
	UT_ASSERT_EQ(page_owner_registered_records, 32);
	UT_ASSERT_EQ(page_owner_native_authority_calls, 80);
	UT_ASSERT_EQ(page_owner_native_authority_refusals, 16);
	UT_ASSERT_EQ(page_owner_native_resident_calls, 24);
	UT_ASSERT_EQ(page_owner_resident_successes, 8);
	UT_ASSERT_EQ(page_owner_resident_pins, 16); UT_ASSERT_EQ(page_owner_resident_unpins, 16);
	UT_ASSERT_EQ(page_owner_resident_releases, 16);
	UT_ASSERT_EQ(page_owner_resident_missing, 8);
	UT_ASSERT_EQ(page_owner_resident_wrap, 4); UT_ASSERT_EQ(page_owner_resident_generation, 4);
	UT_ASSERT_EQ(page_owner_original_lookup_pin_calls, 16);
	UT_ASSERT_EQ(page_owner_lookup_private_reads, 16);
	UT_ASSERT_EQ(page_owner_lookup_pin_leaf_calls, 16);
	UT_ASSERT(!page_owner_resident_active && !page_owner_resident_partition_held && !page_owner_page_locked);
	UT_ASSERT(ClusterPcmOwnArray == NULL);
	page_owner_use_registration = page_owner_use_xact_owner = page_owner_use_range = page_owner_use_full_batch = native_exit_use_page_owner_discharge = false;
	page_owner_use_native_resident = page_owner_use_native_authority = false;
	page_owner_use_original_lookup_pin = false;
}

UT_TEST(test_original_primitive_pin_feeds_resident_stamp_and_recent_freeze)
{
	page_owner_use_original_primitive_pin = true;
	page_owner_original_primitive_pin_calls = 0;
	page_owner_primitive_reserves = page_owner_primitive_new_refs = 0;
	page_owner_primitive_remembers = page_owner_primitive_unpins = 0;
	page_owner_primitive_private_reads = page_owner_primitive_forgets = page_owner_primitive_last_pin_reads = 0;
	UT_ASSERT(!page_owner_primitive_ref_live && !page_owner_primitive_reserved);
	test_original_lookup_pin_feeds_resident_stamp_and_recent_freeze();
	UT_ASSERT_EQ(page_owner_original_primitive_pin_calls, 16);
	UT_ASSERT_EQ(page_owner_primitive_reserves, 16);
	UT_ASSERT_EQ(page_owner_primitive_new_refs, 16);
	UT_ASSERT_EQ(page_owner_primitive_remembers, 16);
	UT_ASSERT_EQ(page_owner_primitive_unpins, 16);
	UT_ASSERT_EQ(page_owner_primitive_private_reads, 32);
	UT_ASSERT_EQ(page_owner_primitive_forgets, 16);
	UT_ASSERT_EQ(page_owner_primitive_last_pin_reads, 16);
	UT_ASSERT(!page_owner_primitive_ref_live && !page_owner_primitive_reserved);
	page_owner_use_original_primitive_pin = false;
}

UT_TEST(test_original_private_ref_table_feeds_pin_stamp_and_recent_freeze)
{
	page_owner_use_original_private_refs = true;
	page_owner_original_private_gets = page_owner_original_private_reserves = 0;
	page_owner_original_private_news = page_owner_original_private_forgets = 0;
	memset(page_owner_original_private_array, 0, sizeof(page_owner_original_private_array));
	page_owner_original_private_reserved = NULL;
	page_owner_original_private_overflowed = 0; page_owner_original_private_clock = 0;
	UT_ASSERT(page_owner_original_private_hash == NULL);
	test_original_primitive_pin_feeds_resident_stamp_and_recent_freeze();
	UT_ASSERT_EQ(page_owner_original_private_gets, 48);
	UT_ASSERT_EQ(page_owner_original_private_reserves, 16);
	UT_ASSERT_EQ(page_owner_original_private_news, 16);
	UT_ASSERT_EQ(page_owner_original_private_forgets, 16);
	UT_ASSERT(page_owner_original_private_reserved == &page_owner_original_private_array[0]);
	UT_ASSERT_EQ(page_owner_original_private_overflowed, 0); UT_ASSERT_EQ(page_owner_original_private_clock, 0);
	for (unsigned i = 0; i < lengthof(page_owner_original_private_array); i++)
	{
		ClusterKoCurrentAdmissionV3 zero = {0};
		UT_ASSERT_EQ(page_owner_original_private_array[i].buffer, InvalidBuffer);
		UT_ASSERT_EQ(page_owner_original_private_array[i].refcount, 0);
		UT_ASSERT(memcmp(&page_owner_original_private_array[i].ko_current, &zero, sizeof(zero)) == 0);
	}
	page_owner_original_private_reserved = NULL;
	page_owner_use_original_private_refs = false;
}

UT_TEST(test_original_resource_owner_feeds_pin_stamp_and_recent_freeze)
{
	page_owner_use_original_resource_owner = true;
	page_owner_original_owner_enlarges = page_owner_original_owner_remembers = 0;
	page_owner_original_owner_forgets = page_owner_original_owner_allocations = 0;
	memset(&page_owner_native_resource_owner, 0, sizeof(page_owner_native_resource_owner));
	page_owner_native_resource_owner.name = "freeze-maintenance-unit";
	original_touch_owner_array_init(&page_owner_native_resource_owner.bufferarr, page_owner_buffer_datum(InvalidBuffer));
	test_original_private_ref_table_feeds_pin_stamp_and_recent_freeze();
	UT_ASSERT_EQ(page_owner_original_owner_enlarges, 24);
	UT_ASSERT_EQ(page_owner_original_owner_remembers, 16);
	UT_ASSERT_EQ(page_owner_original_owner_forgets, 16);
	UT_ASSERT_EQ(page_owner_original_owner_allocations, 1);
	UT_ASSERT_EQ(page_owner_native_resource_owner.bufferarr.nitems, 0);
	UT_ASSERT_EQ(page_owner_native_resource_owner.bufferarr.capacity, 16);
	UT_ASSERT_EQ(page_owner_native_resource_owner.bufferarr.maxitems, 16);
	UT_ASSERT_EQ(page_owner_native_resource_owner.bufferarr.invalidval, page_owner_buffer_datum(InvalidBuffer));
	UT_ASSERT(page_owner_native_resource_owner.bufferarr.itemsarr != NULL);
	free(page_owner_native_resource_owner.bufferarr.itemsarr);
	memset(&page_owner_native_resource_owner, 0, sizeof(page_owner_native_resource_owner));
	page_owner_use_original_resource_owner = false;
}

UT_TEST(test_original_resident_stamp_owner_feeds_shared_retention_and_recent_freeze)
{
	page_owner_use_native_resident = page_owner_use_native_authority = true;
	page_owner_native_resident_calls = 0;
	page_owner_resident_pins = page_owner_resident_unpins = 0;
	page_owner_resident_releases = page_owner_resident_successes = 0;
	page_owner_resident_missing = page_owner_resident_wrap = page_owner_resident_generation = 0;
	page_owner_native_authority_calls = page_owner_native_authority_refusals = 0;
	page_owner_use_registration = page_owner_use_xact_owner = page_owner_use_range = page_owner_use_full_batch = native_exit_use_page_owner_discharge = true;
	page_owner_registered_records = 0;
	for (unsigned retirement = 1; retirement <= 2; retirement++)
		for (unsigned batch = 0; batch < 2; batch++)
			full_source_then_recent_lifecycle_case(batch != 0, false, true, 2, retirement);
	UT_ASSERT_EQ(page_owner_registered_records, 32);
	UT_ASSERT_EQ(page_owner_native_authority_calls, 80);
	UT_ASSERT_EQ(page_owner_native_authority_refusals, 16);
	UT_ASSERT_EQ(page_owner_native_resident_calls, 24);
	UT_ASSERT_EQ(page_owner_resident_successes, 8);
	UT_ASSERT_EQ(page_owner_resident_pins, 16); UT_ASSERT_EQ(page_owner_resident_unpins, 16);
	UT_ASSERT_EQ(page_owner_resident_releases, 16);
	UT_ASSERT_EQ(page_owner_resident_missing, 8);
	UT_ASSERT_EQ(page_owner_resident_wrap, 4); UT_ASSERT_EQ(page_owner_resident_generation, 4);
	UT_ASSERT(!page_owner_resident_active && !page_owner_resident_partition_held && !page_owner_page_locked);
	UT_ASSERT(ClusterPcmOwnArray == NULL);
	page_owner_use_registration = page_owner_use_xact_owner = page_owner_use_range = page_owner_use_full_batch = native_exit_use_page_owner_discharge = false;
	page_owner_use_native_resident = page_owner_use_native_authority = false;
}

UT_TEST(test_original_bufmgr_authority_feeds_touch_registration_and_recent_freeze)
{
	page_owner_use_native_authority = true;
	page_owner_native_authority_calls = page_owner_native_authority_refusals = 0;
	page_owner_use_registration = page_owner_use_xact_owner = page_owner_use_range = page_owner_use_full_batch = native_exit_use_page_owner_discharge = true;
	page_owner_registered_records = 0;
	for (unsigned retirement = 1; retirement <= 2; retirement++)
		for (unsigned batch = 0; batch < 2; batch++)
			full_source_then_recent_lifecycle_case(batch != 0, false, true, 2, retirement);
	UT_ASSERT_EQ(page_owner_registered_records, 32);
	UT_ASSERT_EQ(page_owner_native_authority_calls, 80);
	UT_ASSERT_EQ(page_owner_native_authority_refusals, 16);
	UT_ASSERT(ClusterPcmOwnArray == NULL);
	page_owner_use_registration = page_owner_use_xact_owner = page_owner_use_range = page_owner_use_full_batch = native_exit_use_page_owner_discharge = false;
	page_owner_use_native_authority = false;
}

UT_TEST(test_original_exact_touch_registration_feeds_finish_and_recent_freeze)
{
	page_owner_use_registration = page_owner_use_xact_owner = page_owner_use_range = page_owner_use_full_batch = native_exit_use_page_owner_discharge = true;
	page_owner_registered_records = 0;
	for (unsigned retirement = 1; retirement <= 2; retirement++)
		for (unsigned batch = 0; batch < 2; batch++)
			full_source_then_recent_lifecycle_case(batch != 0, false, true, 2, retirement);
	UT_ASSERT_EQ(page_owner_registered_records, 32);
	page_owner_use_registration = page_owner_use_xact_owner = page_owner_use_range = page_owner_use_full_batch = native_exit_use_page_owner_discharge = false;
}

UT_TEST(test_original_top_xact_finish_retires_local_touches_before_recent_freeze)
{
	page_owner_use_xact_owner = page_owner_use_range = page_owner_use_full_batch = native_exit_use_page_owner_discharge = true;
	page_owner_xact_finishes = 0;
	for (unsigned retirement = 1; retirement <= 2; retirement++)
		for (unsigned batch = 0; batch < 2; batch++)
			full_source_then_recent_lifecycle_case(batch != 0, false, true, 2, retirement);
	UT_ASSERT_EQ(page_owner_xact_finishes, 16);
	page_owner_use_xact_owner = page_owner_use_range = page_owner_use_full_batch = native_exit_use_page_owner_discharge = false;
}

UT_TEST(test_original_page_owner_range_groups_reused_slot_before_recent_freeze)
{
	page_owner_use_range = page_owner_use_full_batch = native_exit_use_page_owner_discharge = true;
	page_owner_range_calls = 0;
	for (unsigned retirement = 1; retirement <= 2; retirement++)
		for (unsigned batch = 0; batch < 2; batch++)
			full_source_then_recent_lifecycle_case(batch != 0, false, true, 2, retirement);
	UT_ASSERT_EQ(page_owner_range_calls, 16);
	page_owner_use_range = page_owner_use_full_batch = native_exit_use_page_owner_discharge = false;
}

UT_TEST(test_original_page_owner_stamp_wal_discharge_feeds_recent_freeze)
{
	page_owner_use_full_batch = native_exit_use_page_owner_discharge = true;
	for (unsigned retirement = 1; retirement <= 2; retirement++)
		for (unsigned batch = 0; batch < 2; batch++)
			full_source_then_recent_lifecycle_case(batch != 0, false, true, 2, retirement);
	page_owner_use_full_batch = native_exit_use_page_owner_discharge = false;
}

UT_TEST(test_original_page_owner_discharge_feeds_certificate_recent_wal)
{
	native_exit_use_page_owner_discharge = true;
	for (unsigned retirement = 1; retirement <= 2; retirement++)
		for (unsigned batch = 0; batch < 2; batch++)
			full_source_then_recent_lifecycle_case(batch != 0, false, true, 2, retirement);
	native_exit_use_page_owner_discharge = false;
}

UT_TEST(test_original_native_exit_applied_certificate_retirement_precedes_recent_wal)
{
	for (unsigned retirement = 1; retirement <= 2; retirement++)
		for (unsigned batch = 0; batch < 2; batch++)
			full_source_then_recent_lifecycle_case(batch != 0, false, true, 2, retirement);
}

UT_TEST(test_original_native_exit_certificate_receiver_retirement_keeps_origin_in_recent_cut)
{
	for (unsigned batch = 0; batch < 2; batch++)
		full_source_then_recent_lifecycle_case(batch != 0, false, true, 1, 1);
}

UT_TEST(test_original_native_exit_final_certificate_reply_precedes_fresh_recent_cut)
{
	for (unsigned batch = 0; batch < 2; batch++)
		full_source_then_recent_lifecycle_case(batch != 0, false, true, 1, 2);
}

UT_TEST(test_original_pgproc_exit_cancelled_receipt_keeps_source_through_recent_wal)
{
	for (unsigned batch = 0; batch < 2; batch++)
		full_source_then_recent_lifecycle_case(batch != 0, false, true, 1, 0);
}

UT_TEST(test_original_pgproc_exit_applied_receipt_keeps_source_through_recent_wal)
{
	for (unsigned batch = 0; batch < 2; batch++)
		full_source_then_recent_lifecycle_case(batch != 0, false, true, 2, 0);
}

UT_TEST(test_original_pgproc_exit_keeps_retained_source_through_recent_wal)
{
	for (unsigned batch = 0; batch < 2; batch++)
		full_source_then_recent_lifecycle_case(batch != 0, false, true, 0, 0);
}

UT_TEST(test_original_pgproc_exit_late_receiver_still_revokes_recent_wal)
{
	for (unsigned batch = 0; batch < 2; batch++)
		full_source_then_recent_lifecycle_case(batch != 0, true, true, 0, 0);
}

UT_TEST(test_full_original_source_reaches_pin_free_collector_and_recent_wal)
{
	for (unsigned batch = 0; batch < 2; batch++) full_source_then_recent_case(batch != 0, false);
}

UT_TEST(test_full_original_source_late_receiver_revokes_recent_wal)
{
	for (unsigned batch = 0; batch < 2; batch++) full_source_then_recent_case(batch != 0, true);
}

UT_TEST(test_recent_unknown_receiver_revokes_pin_free_proof_before_writer_or_wal)
{
	for (unsigned batch = 0; batch < 2; batch++) recent_registration_case(batch != 0, false);
}

UT_TEST(test_recent_refused_receiver_keeps_original_cut_and_native_continuation)
{
	for (unsigned batch = 0; batch < 2; batch++) recent_registration_case(batch != 0, true);
}

UT_TEST(test_recent_xmin_stale_registration_refuses_before_resident_pin)
{
	for (unsigned batch = 0; batch < 2; batch++)
	{
		for (unsigned fault = 0; fault < 6; fault++)
		{
			QualifiedXminFixture f;
			ClusterHeapFreezeXminTargetV1 targets[3];
			uint16 count = batch ? 3 : 1;
			int32 caller_pins;

			if (batch) qualified_xmin_batch_setup(&f, targets);
			else
			{
				qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
				targets[0] = (ClusterHeapFreezeXminTargetV1) {
					0, FullTransactionIdFromEpochAndXid(1, 77)
				};
			}
			relation_data.rd_isvalid = true;
			caller_pins = backend_pin_count;
			switch (fault)
			{
			case 0: f.input.freeze_cohort = NULL; break;
			case 1: f.cohort.local_registration_cut.status = CTRC_ACTIVE_SCAN_INCOMPLETE; break;
			case 2: f.cohort.local_registration_cut.unqualified_entries = 1; break;
			case 3: ctrc_note_unqualified_registration(); break;
			case 4: f.cohort.local_registration_cut.unqualified_registration_generation = 0; break;
			case 5: f.cohort.local_registration_cut.unqualified_registration_generation = UINT64_MAX; break;
			}
			UT_ASSERT(!fixture_recent_xmin_apply(&f, targets, count));
			UT_ASSERT_EQ(recent_pin_calls + recent_unpins, 0);
			UT_ASSERT_EQ(backend_pin_count, caller_pins);
			UT_ASSERT(local_x && !resumed_writer && f.admission.entered);
			UT_ASSERT_EQ(conditional_resumes + conditional_unlocks, 0);
			UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
			UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_recent_xmin_pin_continues_original_admission_to_native_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
		if (batch) qualified_xmin_batch_setup(&f, targets);
		else {
			qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
			targets[0] = (ClusterHeapFreezeXminTargetV1) {
				0, FullTransactionIdFromEpochAndXid(1, 77)
			};
		}
		fixture_release_original_writer(); resume_allowed = true;
		relation_data.rd_isvalid = true; backend_pin_count = 0;
		UT_ASSERT(fixture_recent_xmin_apply(&f, targets, batch ? 3 : 1));
		UT_ASSERT_EQ(backend_pin_count, 0);
		UT_ASSERT_EQ(recent_pin_calls, 1); UT_ASSERT_EQ(recent_unpins, 1);
		UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT_EQ(conditional_resumes, 1); UT_ASSERT_EQ(conditional_unlocks, 1);
	}
}

UT_TEST(test_recent_xmin_missing_resident_keeps_original_owner_without_fetch)
{
	QualifiedXminFixture f;
	const ClusterHeapFreezeXminTargetV1 target = {0, FullTransactionIdFromEpochAndXid(1, 77)};
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
	fixture_release_original_writer(); resume_allowed = true;
	relation_data.rd_isvalid = true; backend_pin_count = 0; recent_pin_allowed = false;
	UT_ASSERT(!fixture_recent_xmin_apply(&f, &target, 1));
	UT_ASSERT_EQ(recent_pin_calls, 1); UT_ASSERT_EQ(recent_unpins, 0);
	UT_ASSERT_EQ(backend_pin_count, 0); UT_ASSERT(f.admission.entered);
	UT_ASSERT_EQ(conditional_resumes + inserts + dirties + flushes, 0);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
}

UT_TEST(test_recent_xmin_bad_inputs_refuse_before_resident_pin)
{
	for (int fault = 0; fault < 16; fault++) {
		QualifiedXminFixture f;
		const ClusterHeapFreezeXminTargetV1 target = {0, FullTransactionIdFromEpochAndXid(1, 77)};
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		fixture_release_original_writer(); resume_allowed = true;
		relation_data.rd_isvalid = fault != 1; backend_pin_count = 0;
		if (fault == 10) f.admission.entered = false;
		if (fault == 14) relation_data.rd_locator.relNumber++;
		if (fault == 15) f.admission.record_generation++;
		UT_ASSERT(!cluster_heap_freeze_apply_xmin_batch_recent_v1(
			fault == 0 ? NULL : &relation_data, fault == 2 ? 0 : fault == 3 ? 2 : 1,
			fault == 4 ? NULL : &f.input, fault == 5 ? NULL : &f.page,
			fault == 6 ? NULL : &f.proofs, fault == 7 ? NULL : &f.fence,
			fault == 8 ? NULL : &f.authority, fault == 9 ? NULL : &f.admission,
			fault == 11 ? NULL : &target,
			fault == 12 ? 0 : fault == 13 ? MaxHeapTuplesPerPage + 1 : 1,
			FullTransactionIdFromEpochAndXid(1, 100), 10));
		UT_ASSERT_EQ(recent_pin_calls + recent_unpins, 0);
		UT_ASSERT_EQ(backend_pin_count, 0);
		UT_ASSERT_EQ(f.admission.entered, fault != 10);
		UT_ASSERT_EQ(conditional_resumes + inserts + dirties + flushes, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_recent_xmin_native_refusal_releases_only_new_resident_pin)
{
	for (int fault = 0; fault < 4; fault++) {
		QualifiedXminFixture f;
		const ClusterHeapFreezeXminTargetV1 target = {0, FullTransactionIdFromEpochAndXid(1, 77)};
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		fixture_release_original_writer(); resume_allowed = fault != 1;
		relation_data.rd_isvalid = true; backend_pin_count = 0;
		if (fault == 0) f.input.freeze_policy = NULL;
		if (fault == 2) authority_current = false;
		if (fault == 3) object_current = false;
		UT_ASSERT(!fixture_recent_xmin_apply(&f, &target, 1));
		/* Missing saved policy is now refused before a temporary pin. */
		UT_ASSERT_EQ(recent_pin_calls, fault == 0 ? 0 : 1);
		UT_ASSERT_EQ(recent_unpins, fault == 0 ? 0 : 1);
		UT_ASSERT_EQ(backend_pin_count, 0);
		UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT_EQ(inserts + dirties + flushes, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_recent_xmin_refusal_preserves_preowned_pin_and_content_lock)
{
	for (int owned_x = 0; owned_x < 2; owned_x++) {
		QualifiedXminFixture f;
		const ClusterHeapFreezeXminTargetV1 target = {0, FullTransactionIdFromEpochAndXid(1, 77)};
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		if (!owned_x) fixture_release_original_writer();
		relation_data.rd_isvalid = true; f.input.freeze_policy = NULL;
		UT_ASSERT(!fixture_recent_xmin_apply(&f, &target, 1));
		/* The saved policy is absent; keep only the caller's existing pin. */
		UT_ASSERT_EQ(recent_pin_calls, 0); UT_ASSERT_EQ(recent_unpins, 0);
		UT_ASSERT_EQ(backend_pin_count, 1); UT_ASSERT_EQ(local_x, owned_x);
		UT_ASSERT_EQ(cluster_bufmgr_pcm_x_writer_find(GetBufferDescriptor(0)) != NULL, owned_x);
		UT_ASSERT(f.admission.entered); UT_ASSERT_EQ(conditional_resumes + conditional_unlocks, 0);
		UT_ASSERT_EQ(inserts + dirties + flushes, 0);
	}
}

UT_TEST(test_recent_xmin_errors_unwind_writer_then_pin_without_leaving_admission)
{
	for (volatile int stage = 0; stage < 6; stage++) {
		QualifiedXminFixture f;
		const ClusterHeapFreezeXminTargetV1 target = {0, FullTransactionIdFromEpochAndXid(1, 77)};
		qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
		fixture_release_original_writer(); resume_allowed = true;
		relation_data.rd_isvalid = true; backend_pin_count = 0;
		recent_pin_error = stage == 0; resume_error = stage == 1;
		error_after_local_lock = stage == 2; error_after_ledger_bind = stage == 3;
		target_recheck_error = stage == 4; flush_error = stage == 5;
		expecting_error = true;
		if (setjmp(error_jump) == 0) {
			(void) fixture_recent_xmin_apply(&f, &target, 1); UT_ASSERT(false);
		}
		expecting_error = false;
		UT_ASSERT_EQ(recent_pin_calls, 1); UT_ASSERT_EQ(recent_unpins, stage == 0 ? 0 : 1);
		UT_ASSERT_EQ(backend_pin_count, 0);
		UT_ASSERT(!local_x && !resumed_writer && f.admission.entered);
		UT_ASSERT(cluster_bufmgr_pcm_x_writer_find(GetBufferDescriptor(0)) == NULL);
		UT_ASSERT(PG_exception_stack == NULL && error_context_stack == NULL);
		UT_ASSERT_EQ(inserts, stage == 5 ? 1 : 0); UT_ASSERT_EQ(flushes, stage == 5 ? 1 : 0);
		if (stage != 5) UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_recent_xmin_success_preserves_preowned_pin_and_writer)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int owned_x = 0; owned_x < 2; owned_x++) {
			QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
			if (batch) qualified_xmin_batch_setup(&f, targets);
			else {
				qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
				targets[0] = (ClusterHeapFreezeXminTargetV1) {
					0, FullTransactionIdFromEpochAndXid(1, 77)
				};
			}
			if (!owned_x) { fixture_release_original_writer(); resume_allowed = true; }
			relation_data.rd_isvalid = true;
			UT_ASSERT(fixture_recent_xmin_apply(&f, targets, batch ? 3 : 1));
			UT_ASSERT_EQ(backend_pin_count, 1); UT_ASSERT_EQ(local_x, owned_x);
			UT_ASSERT(f.admission.entered); UT_ASSERT(!recent_pin_held);
			UT_ASSERT_EQ(cluster_bufmgr_pcm_x_writer_find(GetBufferDescriptor(0)) != NULL, owned_x);
			UT_ASSERT_EQ(recent_pin_calls, 1); UT_ASSERT_EQ(recent_unpins, 1);
			UT_ASSERT_EQ(conditional_resumes, owned_x ? 0 : 1);
			UT_ASSERT_EQ(conditional_unlocks, owned_x ? 0 : 1);
			UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(flushes, 1);
		}
	}
}

UT_TEST(test_recent_xmin_errors_keep_borrowed_pin_and_writer_scope)
{
	for (volatile int batch = 0; batch < 2; batch++) {
		for (volatile int owned_x = 0; owned_x < 2; owned_x++) {
			for (volatile int stage = 0; stage < 3; stage++) {
				QualifiedXminFixture f; ClusterHeapFreezeXminTargetV1 targets[3];
				if (batch) qualified_xmin_batch_setup(&f, targets);
				else {
					qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
					targets[0] = (ClusterHeapFreezeXminTargetV1) {
						0, FullTransactionIdFromEpochAndXid(1, 77)
					};
				}
				if (!owned_x) { fixture_release_original_writer(); resume_allowed = true; }
				relation_data.rd_isvalid = true; expecting_error = true;
				target_recheck_error = stage == 0; space_permanence_error = stage == 1;
				flush_error = stage == 2;
				if (setjmp(error_jump) == 0) {
					(void) fixture_recent_xmin_apply(&f, targets, batch ? 3 : 1); UT_ASSERT(false);
				}
				expecting_error = false;
				UT_ASSERT_EQ(backend_pin_count, 1); UT_ASSERT_EQ(local_x, owned_x);
				UT_ASSERT(f.admission.entered); UT_ASSERT(!recent_pin_held);
				UT_ASSERT_EQ(cluster_bufmgr_pcm_x_writer_find(GetBufferDescriptor(0)) != NULL, owned_x);
				UT_ASSERT_EQ(recent_pin_calls, 1); UT_ASSERT_EQ(recent_unpins, 1);
				UT_ASSERT_EQ(conditional_unlocks, conditional_resumes);
				UT_ASSERT_EQ(inserts, stage == 2 ? 1 : 0); UT_ASSERT_EQ(flushes, stage == 2 ? 1 : 0);
				UT_ASSERT(PG_exception_stack == NULL && error_context_stack == NULL);
				if (stage != 2) UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			}
		}
	}
}

UT_TEST(test_recent_xmin_share_lock_refusal_keeps_borrowed_pin_without_upgrade)
{
	QualifiedXminFixture f;
	const ClusterHeapFreezeXminTargetV1 target = {0, FullTransactionIdFromEpochAndXid(1, 77)};
	qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
	fixture_release_original_writer(); local_share = resume_allowed = true;
	relation_data.rd_isvalid = true;
	UT_ASSERT(!fixture_recent_xmin_apply(&f, &target, 1));
	UT_ASSERT_EQ(backend_pin_count, 1); UT_ASSERT(local_share && !local_x && f.admission.entered);
	UT_ASSERT(!recent_pin_held); UT_ASSERT_EQ(recent_pin_calls, 1); UT_ASSERT_EQ(recent_unpins, 1);
	UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
	UT_ASSERT_EQ(inserts + dirties + flushes, 0);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
}

/* Join the original outer VACUUM owner to the complete admitted collector.
 * Route, resolver/resident checks and transport counters remain explicit leaves;
 * the token, registration, debt and enter/leave bodies are original. There is no
 * continuation after outer leave and no production freeze qualification here. */
static unsigned proof_owner_enters, proof_owner_leaves, proof_owner_cost_reads;
static unsigned proof_owner_expected_pins;
static ClusterSemanticAdmissionToken proof_owner_left_token;
static LVRelState *proof_owner_retirement_observer;
static unsigned proof_owner_retired_before_leave;

static ClusterSemanticAdmissionResult
proof_owner_enter(ClusterSemanticAdmissionToken *token)
{
	ClusterSemanticAdmissionResult result;

	proof_owner_enters++;
	result = bounds_original_census_enter(token);
	if (result == CLUSTER_SEMANTIC_ADMISSION_OK) {
		bounds_borrowed_admission = token;
		bounds_mutable_admission = token;
	}
	return result;
}

static void
proof_owner_leave(ClusterSemanticAdmissionToken *token)
{
	UT_ASSERT(token == bounds_borrowed_admission && token->entered);
	UT_ASSERT_EQ(proof_owner_leaves, 0);
	UT_ASSERT_EQ(backend_pin_count, proof_owner_expected_pins);
	UT_ASSERT(!local_x && !local_share && !resumed_writer);
	UT_ASSERT_EQ(pg_atomic_read_u32(&native_admission_shared.inflight
		[CLUSTER_SEMANTIC_TARGET_SIDE][bounds_lifecycle_feature()]), 8);
	if (proof_owner_retirement_observer != NULL) {
		const LVRelState *v = proof_owner_retirement_observer;
		const unsigned char *cohort = (const unsigned char *)&v->shared_freeze_cohort;
		bool retired = !v->shared_freeze_fence_valid && !v->shared_freeze_cohort_observed
			&& v->shared_freeze_input.freeze_policy == NULL
			&& v->shared_freeze_input.freeze_cohort == NULL
			&& v->shared_freeze_xmin_candidates.target_count == 0
			&& v->shared_freeze_aborted_xmax_candidates.target_count == 0
			&& !FullTransactionIdIsValid(v->shared_freeze_candidate_limit_full);

		for (unsigned i = 0; i < sizeof(v->shared_freeze_cohort); i++)
			retired = retired && cohort[i] == 0;
		proof_owner_retired_before_leave += retired ? 1 : 0;
	}
	bounds_original_census_leave(token);
	proof_owner_left_token = *token;
	proof_owner_leaves++;
	bounds_borrowed_admission = NULL;
	bounds_mutable_admission = &phase_source->admission;
}

static void
proof_owner_transport_snapshot(uint64 *out)
{
	bool before_leave = proof_owner_cost_reads++ == 0;

	UT_ASSERT(proof_owner_cost_reads <= 2);
	UT_ASSERT_EQ(proof_owner_leaves, before_leave ? 0 : 1);
	UT_ASSERT_EQ(bounds_original_backend_has_admission(), before_leave);
	for (unsigned i = 0; i < 15; i++)
		out[i] = 100 + i + (before_leave ? 0 : i % 3 + 1);
}

#define cluster_semantic_activation_enter_r4_terminal_census proof_owner_enter
#define cluster_semantic_activation_leave proof_owner_leave
#define lazy_shared_freeze_transport_snapshot proof_owner_transport_snapshot
#define lazy_collect_shared_freeze_proofs_admitted phase_actual_admitted
#define lazy_collect_shared_freeze_proofs proof_owner_collect
#include "test_cluster_vacuum_proof_owner.inc"
#undef lazy_collect_shared_freeze_proofs
#undef lazy_collect_shared_freeze_proofs_admitted
#undef lazy_shared_freeze_transport_snapshot
#undef cluster_semantic_activation_leave
#undef cluster_semantic_activation_enter_r4_terminal_census

static void
proof_owner_setup(QualifiedXminFixture *f, LVRelState *v,
	ClusterHeapFreezeXminTargetV1 targets[3], bool batch)
{
	bounds_lifecycle_setup(f, v, targets, batch);
	phase_source = f;
	phase_preflights = phase_resolves = phase_rechecks = phase_error_stage = 0;
	v->rel = &relation_data;
	v->shared_freeze_batch = f->page;
	v->shared_freeze_proof_calls = 17;
	v->shared_freeze_proof_requests = 23;
	v->shared_freeze_proof_exchanges = 31;
	fixture_release_original_writer();
	backend_pin_count = 0;
	phase_pin_released = true;
	proof_owner_enters = proof_owner_leaves = proof_owner_cost_reads = 0;
	proof_owner_expected_pins = 0;
	memset(&proof_owner_left_token, 0, sizeof(proof_owner_left_token));
	pg_atomic_write_u32(&native_admission_shared.inflight
		[CLUSTER_SEMANTIC_TARGET_SIDE][bounds_lifecycle_feature()], 7);
}

static void
proof_owner_assert_released(const LVRelState *v, bool admitted)
{
	ClusterSemanticAdmissionToken zero = {0};

	UT_ASSERT_EQ(proof_owner_leaves, admitted ? 1 : 0);
	UT_ASSERT_EQ(proof_owner_cost_reads, admitted ? 2 : 0);
	UT_ASSERT_EQ(semantic_activation_local_inflight
		[CLUSTER_SEMANTIC_TARGET_SIDE][bounds_lifecycle_feature()], 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&native_admission_shared.inflight
		[CLUSTER_SEMANTIC_TARGET_SIDE][bounds_lifecycle_feature()]), 7);
	UT_ASSERT(!bounds_original_backend_has_admission());
	UT_ASSERT(memcmp(&proof_owner_left_token, &zero, sizeof(zero)) == 0);
	UT_ASSERT(bounds_borrowed_admission == NULL);
	UT_ASSERT_EQ(v->shared_freeze_proof_calls, admitted ? 22 : 17);
	UT_ASSERT_EQ(v->shared_freeze_proof_requests, admitted ? 33 : 23);
	UT_ASSERT_EQ(v->shared_freeze_proof_exchanges, admitted ? 46 : 31);
	UT_ASSERT(!v->shared_freeze_proof_cost_incomplete);
	UT_ASSERT_EQ(backend_pin_count, proof_owner_expected_pins);
	UT_ASSERT(!local_x && !local_share && !resumed_writer);
	UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
	UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	UT_ASSERT(PG_exception_stack == NULL && error_context_stack == NULL);
	phase_pin_released = false;
}

UT_TEST(test_real_outer_collector_enters_and_leaves_around_actual_admitted_phase)
{
	for (unsigned batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3];
		proof_owner_setup(&f, &v, targets, batch != 0);
		UT_ASSERT_EQ(proof_owner_collect(&v, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		UT_ASSERT_EQ(proof_owner_enters, 1);
		UT_ASSERT_EQ(phase_preflights, 1); UT_ASSERT_EQ(phase_rechecks, 1);
		UT_ASSERT_EQ(phase_resolves, f.page.carrier_request_count);
		UT_ASSERT(!v.shared_freeze_fence_valid && !v.shared_freeze_cohort_observed);
		UT_ASSERT_EQ(v.shared_freeze_xmin_candidates.target_count, 0);
		proof_owner_assert_released(&v, true);
	}
}

UT_TEST(test_real_outer_collector_early_refusals_preserve_caller_pin_and_other_debt)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3];
		Buffer recent = 1;
		proof_owner_setup(&f, &v, targets, false);
		switch (fault) {
		case 0: v.shared_freeze_batch.carrier_request_count = 0; break;
		case 1: route_current = false; break;
		case 2: pg_atomic_write_u32(&native_admission_shared.transition_closed, 1); break;
		case 3: recent = InvalidBuffer; break;
		case 4: backend_pin_count = proof_owner_expected_pins = 1; phase_pin_released = false; break;
		}
		UT_ASSERT_EQ(proof_owner_collect(&v, recent), CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
		UT_ASSERT_EQ(proof_owner_enters, fault >= 2 ? 1 : 0);
		UT_ASSERT_EQ(phase_preflights + phase_resolves + phase_rechecks, 0);
		UT_ASSERT_EQ(v.shared_freeze_proofs.proof_count, 0);
		UT_ASSERT(!v.shared_freeze_fence_valid && !v.shared_freeze_cohort_observed);
		UT_ASSERT(v.shared_freeze_input.freeze_policy == NULL
			&& v.shared_freeze_input.freeze_cohort == NULL);
		UT_ASSERT_EQ(v.shared_freeze_xmin_candidates.target_count, 0);
		proof_owner_assert_released(&v, fault >= 3);
	}
}

UT_TEST(test_real_outer_collector_rethrows_after_original_leave_on_admitted_errors)
{
	/* Heap-owned state remains defined after the error harness longjmp. */
	QualifiedXminFixture *f = malloc(sizeof(*f));
	LVRelState *v = malloc(sizeof(*v));
	ClusterHeapFreezeXminTargetV1 targets[3];
	UT_ASSERT(f != NULL && v != NULL);
	for (unsigned stage = 1; stage <= 4; stage++) {
		proof_owner_setup(f, v, targets, true);
		if (stage == 4) bounds_fault = 7;
		else phase_error_stage = stage;
		expecting_error = true;
		if (setjmp(error_jump) == 0) {
			(void)proof_owner_collect(v, 1);
			UT_ASSERT(false);
		}
		expecting_error = false;
		UT_ASSERT_EQ(proof_owner_enters, 1);
		UT_ASSERT_EQ(phase_preflights, 1);
		UT_ASSERT_EQ(phase_rechecks, stage >= 3 ? 1 : 0);
		proof_owner_assert_released(v, true);
	}
	free(v); free(f);
}

/* An outer collector result retains read-only proofs, not its released owner.
 * A nonempty request with its zeroed token and retired inputs must stop before
 * a resident pin, writer restoration or WAL. No replacement token is made. */
UT_TEST(test_real_outer_collector_result_cannot_outlive_its_original_admission)
{
	for (unsigned batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3];
		proof_owner_setup(&f, &v, targets, batch != 0);
		UT_ASSERT_EQ(proof_owner_collect(&v, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		proof_owner_assert_released(&v, true);
		phase_copy_to_native(&f, &v);
		f.admission = proof_owner_left_token;
		UT_ASSERT(!f.admission.entered);
		phase_pin_released = true;
		UT_ASSERT(!fixture_recent_xmin_apply(&f,
			targets, batch ? 3 : 1));
		UT_ASSERT_EQ(recent_pin_calls + recent_unpins + conditional_resumes, 0);
		UT_ASSERT_EQ(backend_pin_count, 0);
		UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(proof_owner_enters, 1); UT_ASSERT_EQ(proof_owner_leaves, 1);
		UT_ASSERT_EQ(pg_atomic_read_u32(&native_admission_shared.inflight
			[CLUSTER_SEMANTIC_TARGET_SIDE][bounds_lifecycle_feature()]), 7);
		phase_pin_released = false;
	}
}

UT_TEST(test_real_outer_collector_reuse_refusal_retires_previous_observations)
{
	QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3];
	proof_owner_setup(&f, &v, targets, true);
	UT_ASSERT_EQ(proof_owner_collect(&v, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	proof_owner_assert_released(&v, true);
	UT_ASSERT(!v.shared_freeze_fence_valid && !v.shared_freeze_cohort_observed);
	UT_ASSERT(v.shared_freeze_proofs.proof_count > 0);
	route_current = false;
	UT_ASSERT_EQ(proof_owner_collect(&v, 1), CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
	UT_ASSERT_EQ(v.shared_freeze_proofs.proof_count, 0);
	UT_ASSERT(!v.shared_freeze_fence_valid && !v.shared_freeze_cohort_observed);
	UT_ASSERT(v.shared_freeze_input.freeze_policy == NULL
		&& v.shared_freeze_input.freeze_cohort == NULL);
	UT_ASSERT_EQ(v.shared_freeze_xmin_candidates.target_count, 0);
	UT_ASSERT_EQ(v.shared_freeze_aborted_xmax_candidates.target_count, 0);
	UT_ASSERT(!FullTransactionIdIsValid(v.shared_freeze_candidate_limit_full));
	for (unsigned i = 0; i < v.shared_freeze_batch.tuple_count; i++) {
		UT_ASSERT_EQ(v.shared_freeze_batch.tuples[i].creation_target_proof_index,
			CLUSTER_HEAP_FREEZE_NO_CREATION_TARGET);
		UT_ASSERT_EQ(v.shared_freeze_batch.tuples[i].aborted_xmax_target_proof_index,
			CLUSTER_HEAP_FREEZE_NO_ABORTED_XMAX_TARGET);
		UT_ASSERT(!FullTransactionIdIsValid(v.shared_freeze_batch.tuples[i].original_creator_full_xid));
	}
	UT_ASSERT_EQ(proof_owner_enters, 1); UT_ASSERT_EQ(proof_owner_leaves, 1);
	UT_ASSERT_EQ(proof_owner_cost_reads, 2);
	UT_ASSERT_EQ(backend_pin_count, 0); UT_ASSERT(!local_x && !local_share);
	UT_ASSERT_EQ(pg_atomic_read_u32(&native_admission_shared.inflight
		[CLUSTER_SEMANTIC_TARGET_SIDE][bounds_lifecycle_feature()]), 7);
	UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
}

/* Execute the original resident recheck and all three original paired releases.
 * Recent-buffer mapping/pin counts remain the existing explicit leaf. Hint
 * publication is unavailable here: its false-only provider cannot mutate/WAL.
 * Original hint publication is covered in the independent page-owner cases. */
#include "test_cluster_vacuum_resident_release.inc"
static unsigned phase_resident_releases[3], phase_resident_hint_calls;
static bool phase_resident_hint_error;

static void
phase_resident_release_read(Buffer buffer)
{
	phase_resident_releases[0]++;
	ClusterUnlockReleaseBufferAfterConditionalRead(buffer);
}

static void
phase_resident_release_hint(Buffer buffer)
{
	phase_resident_releases[1]++;
	ClusterUnlockReleaseBufferAfterConditionalHint(buffer);
}

static void
phase_resident_release_mutation(Buffer buffer)
{
	phase_resident_releases[2]++;
	ClusterUnlockReleaseBufferAfterConditionalMutation(buffer);
}

static bool
phase_resident_hint_unavailable(LVRelState *v, Buffer buffer,
	const ClusterHeapFreezeProofBatchV1 *proofs,
	const ClusterTxResolution *retained, uint8 retained_mask)
{
	(void)retained;
	UT_ASSERT(v != NULL && buffer == 1 && proofs->proof_count > 0);
	UT_ASSERT(local_x && backend_pin_count == 1);
	UT_ASSERT(bounds_expected_admission()->entered && bounds_original_backend_has_admission());
	UT_ASSERT_EQ(retained_mask, 0);
	phase_resident_hint_calls++;
	if (phase_resident_hint_error) elog(ERROR, "injected resident hint preparation ERROR");
	return false;
}

static void
phase_resident_census_error_at_pin(void)
{
	UT_ASSERT(recent_pin_held && backend_pin_count == 1 && !local_x);
	UT_ASSERT(bounds_expected_admission()->entered);
	bounds_admission_error_kind = 1;
}

static bool
phase_resident_original_census_current(const ClusterSemanticAdmissionToken *token)
{
	UT_ASSERT(token == bounds_expected_admission() && token->entered);
	UT_ASSERT(local_x && backend_pin_count == 1);
	return bounds_original_census_current(token);
}

#define lazy_cleanout_shared_freeze_page phase_resident_hint_unavailable
#define cluster_semantic_activation_recheck_r4_terminal_census phase_resident_original_census_current
#define ClusterUnlockReleaseBufferAfterConditionalRead phase_resident_release_read
#define ClusterUnlockReleaseBufferAfterConditionalHint phase_resident_release_hint
#define ClusterUnlockReleaseBufferAfterConditionalMutation phase_resident_release_mutation
#define lazy_recheck_shared_freeze_page phase_actual_resident_recheck
#include "test_cluster_vacuum_resident_recheck.inc"
#undef lazy_recheck_shared_freeze_page
#undef ClusterUnlockReleaseBufferAfterConditionalMutation
#undef ClusterUnlockReleaseBufferAfterConditionalHint
#undef ClusterUnlockReleaseBufferAfterConditionalRead
#undef cluster_semantic_activation_recheck_r4_terminal_census
#undef lazy_cleanout_shared_freeze_page

static ClusterHeapFreezeProofStatusV1
phase_real_resident_recheck(LVRelState *v, Buffer buffer,
	const PcmAuthoritySnapshot *authority, const ClusterSemanticAdmissionToken *token,
	ClusterHeapFreezeProofBatchV1 *proofs, const ClusterTxResolution *retained,
	uint8 retained_mask, ClusterHeapFreezeFinalFenceV1 *current,
	bool *cleaned, bool *rows_current)
{
	volatile ClusterHeapFreezeProofStatusV1 result = CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED;

	UT_ASSERT_EQ(backend_pin_count, 0);
	UT_ASSERT(!local_x && !local_share && !resumed_writer);
	recent_caller_pins = 0;
	recent_caller_local_x = recent_caller_share = recent_caller_writer = false;
	recent_pin_admission = token;
	native_original_admission_enabled = true;
	native_admission_expected_token = token;
	native_admission_stage = 4;
	phase_pin_released = false;
	PG_TRY();
	{
		result = phase_actual_resident_recheck(v, buffer, authority, token,
			proofs, retained, retained_mask, current, cleaned, rows_current);
	}
	PG_FINALLY();
	{
		UT_ASSERT_EQ(backend_pin_count, 0);
		UT_ASSERT(!recent_pin_held && !local_x && !resumed_writer);
		phase_resident_completed = true;
		phase_pin_released = true;
	}
	PG_END_TRY();
	return result;
}

static void
phase_real_resident_setup(QualifiedXminFixture *f, LVRelState *v,
	ClusterHeapFreezeXminTargetV1 targets[3], bool batch, bool writer)
{
	proof_owner_setup(f, v, targets, batch);
	phase_use_original_resident = true;
	phase_resident_completed = false;
	phase_resident_expected_resumes = writer ? 1 : 0;
	phase_resident_hint_error = false;
	phase_resident_hint_calls = 0;
	memset(phase_resident_releases, 0, sizeof(phase_resident_releases));
	v->identity = space_identity;
	v->shared_freeze_input.writer_context_valid = writer;
	relation_data.rd_isvalid = resume_allowed = true;
}

UT_TEST(test_original_resident_recheck_unpins_before_real_outer_admission_leave)
{
	for (unsigned writer = 0; writer < 2; writer++) {
		for (unsigned batch = 0; batch < 2; batch++) {
			QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3];
			phase_real_resident_setup(&f, &v, targets, batch != 0, writer != 0);
			UT_ASSERT_EQ(proof_owner_collect(&v, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
			UT_ASSERT_EQ(phase_rechecks, 1);
			UT_ASSERT_EQ(recent_pin_calls, 1); UT_ASSERT_EQ(recent_unpins, 1);
			UT_ASSERT_EQ(phase_resident_hint_calls, 1);
			UT_ASSERT_EQ(phase_resident_releases[0], 0);
			UT_ASSERT_EQ(phase_resident_releases[1], writer ? 0 : 1);
			UT_ASSERT_EQ(phase_resident_releases[2], writer ? 1 : 0);
			UT_ASSERT_EQ(conditional_resumes, writer ? 1 : 0);
			UT_ASSERT_EQ(conditional_unlocks, 0);
			UT_ASSERT_EQ(local_unlocks, 1);
			UT_ASSERT_EQ(v.shared_freeze_xmin_candidates.target_count, 0);
			proof_owner_assert_released(&v, true);
		}
	}
}

UT_TEST(test_original_resident_refusals_release_only_their_pin_before_outer_leave)
{
	for (unsigned writer = 0; writer < 2; writer++) {
		for (unsigned fault = 0; fault < 4; fault++) {
			QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3];
			phase_real_resident_setup(&f, &v, targets, false, writer != 0);
			if (fault == 0) recent_pin_allowed = false;
			if (fault == 1) resume_allowed = false;
			if (fault == 2) authority_current = false;
			if (fault == 3) relation_data.rd_isvalid = false;
			if (fault == 0 || fault == 3) phase_resident_expected_resumes = 0;
			UT_ASSERT(proof_owner_collect(&v, 1) != CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
			UT_ASSERT_EQ(recent_pin_calls, fault == 3 ? 0 : 1);
			UT_ASSERT_EQ(recent_unpins, fault == 0 || fault == 3 ? 0 : 1);
			UT_ASSERT_EQ(phase_resident_hint_calls, 0);
			UT_ASSERT_EQ(phase_resident_releases[0], !writer && fault == 2 ? 1 : 0);
			UT_ASSERT_EQ(phase_resident_releases[1], 0);
			UT_ASSERT_EQ(phase_resident_releases[2], writer && fault == 2 ? 1 : 0);
			UT_ASSERT(!v.shared_freeze_fence_valid);
			UT_ASSERT_EQ(v.shared_freeze_xmin_candidates.target_count, 0);
			proof_owner_assert_released(&v, true);
		}
	}
}

UT_TEST(test_original_resident_errors_unpin_before_real_outer_error_propagation)
{
	QualifiedXminFixture *f = malloc(sizeof(*f));
	LVRelState *v = malloc(sizeof(*v));
	ClusterHeapFreezeXminTargetV1 targets[3];
	UT_ASSERT(f != NULL && v != NULL);
	for (unsigned writer = 0; writer < 2; writer++) {
		for (unsigned stage = 0; stage < 3; stage++) {
			phase_real_resident_setup(f, v, targets, true, writer != 0);
			if (stage == 0) recent_pin_error = true;
			if (stage == 1) recent_pin_observer = phase_resident_census_error_at_pin;
			if (stage == 2) phase_resident_hint_error = true;
			expecting_error = true;
			if (setjmp(error_jump) == 0) {
				(void)proof_owner_collect(v, 1);
				UT_ASSERT(false);
			}
			expecting_error = false;
			recent_pin_observer = NULL;
			UT_ASSERT_EQ(phase_rechecks, 1);
			UT_ASSERT_EQ(recent_unpins, stage == 0 ? 0 : 1);
			UT_ASSERT_EQ(phase_resident_releases[0], !writer && stage == 1 ? 1 : 0);
			UT_ASSERT_EQ(phase_resident_releases[1], !writer && stage == 2 ? 1 : 0);
			UT_ASSERT_EQ(phase_resident_releases[2], writer && stage != 0 ? 1 : 0);
			proof_owner_assert_released(v, true);
		}
	}
	free(v); free(f);
}


/* First-pin recent lookup uses the existing original private array,
 * ResourceOwner and native CAS unpin implementations. Header contention,
 * local buffers and an active KO read bracket are outside this MAIN-fork
 * fixture; unsupported branches fail loudly instead of granting authority. */
static BufferUsage phase_recent_usage;
static unsigned phase_recent_fault, phase_recent_ko_samples;
static uint32 phase_recent_saved_state;
static BufferTag phase_recent_saved_tag;

static void
phase_recent_enlarge(ResourceOwner owner)
{
	UT_ASSERT(phase_use_original_recent_pin && owner == page_owner_active_resource_owner());
	UT_ASSERT_EQ(owner->bufferarr.nitems, 0);
	UT_ASSERT_EQ(backend_pin_count, 0);
	UT_ASSERT(!local_x && !local_share && recent_pin_admission->entered);
	original_touch_owner_enlarge(owner);
	page_owner_original_owner_enlarges++;
}

static void
phase_recent_reserve(void)
{
	UT_ASSERT(phase_use_original_recent_pin && page_owner_resident_active);
	UT_ASSERT(!page_owner_resident_partition_held && !page_owner_page_locked && !local_x);
	UT_ASSERT(!page_owner_primitive_reserved && !page_owner_primitive_ref_live);
	original_touch_private_reserve(); page_owner_original_private_reserves++;
	UT_ASSERT(page_owner_original_private_reserved == &page_owner_original_private_array[0]);
	page_owner_primitive_reserved = true; page_owner_primitive_reserves++;
}

static int32
phase_recent_private_count(Buffer buffer)
{
	PrivateRefCountEntry *ref = original_touch_private_get(buffer, false);
	UT_ASSERT(phase_use_original_recent_pin); UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(ref == NULL); /* The actual collector released its capture pin. */
	UT_ASSERT_EQ(backend_pin_count, 0);
	return ref == NULL ? 0 : ref->refcount;
}

static bool
phase_recent_ko_unavailable(RelFileLocator locator, ForkNumber fork,
	BlockNumber block, ClusterKoPeerAdmissionV3 *sample)
{
	UT_ASSERT(phase_use_original_recent_pin && !local_x);
	UT_ASSERT(RelFileLocatorEquals(locator, relation_data.rd_locator));
	UT_ASSERT_EQ(fork, MAIN_FORKNUM); UT_ASSERT_EQ(block, 7);
	UT_ASSERT(sample != NULL); phase_recent_ko_samples++;
	return false; /* No native KO bracket is qualified by this unit fixture. */
}

static bool
phase_recent_ko_current_unexpected(const ClusterKoPeerAdmissionV3 *sample,
	RelFileLocator locator, ForkNumber fork, BlockNumber block)
{
	(void)sample; (void)locator; (void)fork; (void)block;
	UT_ASSERT(false);
	return false;
}

static void
phase_recent_ko_remember_unexpected(BufferDesc *buf, const ClusterKoPeerAdmissionV3 *sample)
{
	(void)buf; (void)sample; UT_ASSERT(false);
	ereport(ERROR, (errmsg("unexpected KO pin publication in first-pin fixture")));
}

static bool
phase_recent_pcm_active(void)
{
	UT_ASSERT(phase_use_original_recent_pin && recent_pin_admission->entered);
	UT_ASSERT_EQ(GetBufferDescriptor(0)->tag.forkNum, MAIN_FORKNUM);
	return true;
}

static BufferDesc *
phase_recent_local_unexpected(uint32 id)
{
	(void)id; UT_ASSERT(false);
	ereport(ERROR, (errmsg("local buffer outside shared recent-pin fixture")));
	return NULL;
}

static bool
phase_recent_pin_local_unexpected(BufferDesc *buf, bool adjust)
{
	(void)buf; (void)adjust; UT_ASSERT(false);
	ereport(ERROR, (errmsg("local pin outside shared recent-pin fixture")));
	return false;
}

static void
phase_recent_unpin_local_unexpected(Buffer buffer)
{
	(void)buffer; UT_ASSERT(false);
	ereport(ERROR, (errmsg("local unpin outside shared recent-pin fixture")));
}

static void phase_actual_pin_locked(BufferDesc *buf);
#define ResourceOwnerEnlargeBuffers phase_recent_enlarge
#define ReservePrivateRefCountEntry phase_recent_reserve
#define GetPrivateRefCount phase_recent_private_count
#define GetPrivateRefCountEntry page_owner_primitive_private_ref
#define NewPrivateRefCountEntry page_owner_primitive_new_ref
#define ResourceOwnerRememberBuffer page_owner_primitive_remember
#define CurrentResourceOwner page_owner_active_resource_owner()
#define LockBufHdr original_touch_header_lock
#define cluster_bufmgr_ko_native_read_sample phase_recent_ko_unavailable
#define cluster_ko_peer_read_current_v3 phase_recent_ko_current_unexpected
#define cluster_bufmgr_ko_remember_pin phase_recent_ko_remember_unexpected
#define cluster_pcm_is_active phase_recent_pcm_active
#define cluster_bufmgr_should_pcm_track original_touch_pin_tracked
#define GetLocalBufferDescriptor phase_recent_local_unexpected
#define PinLocalBuffer phase_recent_pin_local_unexpected
#define UnpinLocalBuffer phase_recent_unpin_local_unexpected
#define pgBufferUsage phase_recent_usage
#define PinBuffer original_touch_primitive_pin
#define UnpinBuffer original_touch_primitive_unpin
#define PinBuffer_Locked phase_actual_pin_locked
#define ReadRecentBuffer phase_actual_read_recent
#define ReleaseBuffer phase_actual_release_recent
#include "test_cluster_vacuum_recent_pin.inc"
#undef ReleaseBuffer
#undef ReadRecentBuffer
#undef PinBuffer_Locked
#undef UnpinBuffer
#undef PinBuffer
#undef pgBufferUsage
#undef UnpinLocalBuffer
#undef PinLocalBuffer
#undef GetLocalBufferDescriptor
#undef cluster_bufmgr_should_pcm_track
#undef cluster_pcm_is_active
#undef cluster_bufmgr_ko_remember_pin
#undef cluster_ko_peer_read_current_v3
#undef cluster_bufmgr_ko_native_read_sample
#undef LockBufHdr
#undef CurrentResourceOwner
#undef ResourceOwnerRememberBuffer
#undef NewPrivateRefCountEntry
#undef GetPrivateRefCountEntry
#undef GetPrivateRefCount
#undef ReservePrivateRefCountEntry
#undef ResourceOwnerEnlargeBuffers

static bool
phase_original_recent_pin(RelFileLocator locator, ForkNumber fork,
	BlockNumber block, Buffer recent_buffer)
{
	bool pinned;
	BufferDesc *buf = GetBufferDescriptor(0);

	recent_pin_calls++;
	UT_ASSERT_EQ(recent_buffer, 1); UT_ASSERT_EQ(backend_pin_count, 0);
	UT_ASSERT(!recent_pin_held && !local_x && !local_share && recent_pin_admission->entered);
	/* Inject a descriptor change after proof collection, before original lookup. */
	if (phase_recent_fault == 1)
		pg_atomic_write_u32(&buf->state, phase_recent_saved_state & ~BM_VALID);
	if (phase_recent_fault == 2) buf->tag.blockNum++;
	pinned = phase_actual_read_recent(locator, fork, block, recent_buffer);
	if (!pinned) {
		UT_ASSERT_EQ(backend_pin_count, 0);
		UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&buf->state)), 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&buf->state) & BM_LOCKED, 0);
		buf->tag = phase_recent_saved_tag;
		pg_atomic_write_u32(&buf->state, phase_recent_saved_state | BUF_USAGECOUNT_ONE);
		return false;
	}
	UT_ASSERT_EQ(backend_pin_count, 1);
	UT_ASSERT_EQ(page_owner_native_resource_owner.bufferarr.nitems, 1);
	UT_ASSERT_EQ(page_owner_active_private_ref()->refcount, 1);
	recent_pin_held = true;
	if (recent_pin_observer != NULL) recent_pin_observer();
	return true;
}

static void
phase_original_recent_release(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1); UT_ASSERT(recent_pin_held);
	UT_ASSERT_EQ(backend_pin_count, 1);
	UT_ASSERT(!local_x && !local_share && !resumed_writer);
	UT_ASSERT(recent_pin_admission->entered);
	phase_actual_release_recent(buffer);
	UT_ASSERT_EQ(backend_pin_count, 0);
	UT_ASSERT_EQ(page_owner_native_resource_owner.bufferarr.nitems, 0);
	recent_unpins++; recent_pin_held = false;
}

static void
phase_recent_owner_setup(QualifiedXminFixture *f, LVRelState *v,
	ClusterHeapFreezeXminTargetV1 targets[3], bool batch, bool writer)
{
	phase_real_resident_setup(f, v, targets, batch, writer);
	phase_use_original_recent_pin = true;
	phase_recent_fault = phase_recent_ko_samples = 0;
	phase_recent_saved_tag = GetBufferDescriptor(0)->tag;
	phase_recent_saved_state = pg_atomic_read_u32(&GetBufferDescriptor(0)->state);
	UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(phase_recent_saved_state), 0);
	pg_atomic_write_u32(&GetBufferDescriptor(0)->state, BM_VALID | BUF_USAGECOUNT_ONE);
	memset(&phase_recent_usage, 0, sizeof(phase_recent_usage));
	page_owner_resident_active = true;
	page_owner_resident_partition_held = page_owner_page_locked = false;
	page_owner_resident_saved.pins = 0;
	page_owner_use_original_primitive_pin = page_owner_use_original_private_refs = true;
	page_owner_use_original_resource_owner = true;
	page_owner_primitive_ref_live = page_owner_primitive_reserved = false;
	page_owner_primitive_reserves = page_owner_primitive_new_refs = 0;
	page_owner_primitive_remembers = page_owner_primitive_forgets = page_owner_primitive_unpins = 0;
	page_owner_primitive_private_reads = page_owner_primitive_last_pin_reads = 0;
	page_owner_original_private_gets = page_owner_original_private_reserves = 0;
	page_owner_original_private_news = page_owner_original_private_forgets = 0;
	memset(page_owner_original_private_array, 0, sizeof(page_owner_original_private_array));
	page_owner_original_private_reserved = NULL;
	page_owner_original_private_overflowed = 0; page_owner_original_private_clock = 0;
	page_owner_original_owner_enlarges = page_owner_original_owner_remembers = 0;
	page_owner_original_owner_forgets = page_owner_original_owner_allocations = 0;
	memset(&page_owner_native_resource_owner, 0, sizeof(page_owner_native_resource_owner));
	page_owner_native_resource_owner.name = "actual-recent-pin-unit";
	original_touch_owner_array_init(&page_owner_native_resource_owner.bufferarr,
		page_owner_buffer_datum(InvalidBuffer));
}

static void
phase_recent_owner_finish(LVRelState *v, bool pinned)
{
	UT_ASSERT_EQ(recent_pin_calls, 1); UT_ASSERT_EQ(recent_unpins, pinned ? 1 : 0);
	UT_ASSERT_EQ(phase_recent_usage.shared_blks_hit, pinned ? 1 : 0);
	UT_ASSERT_EQ(phase_recent_usage.local_blks_hit, 0);
	UT_ASSERT_EQ(phase_recent_ko_samples, 1);
	UT_ASSERT_EQ(page_owner_original_owner_enlarges, 1);
	UT_ASSERT_EQ(page_owner_original_owner_allocations, 1);
	UT_ASSERT_EQ(page_owner_original_owner_remembers, pinned ? 1 : 0);
	UT_ASSERT_EQ(page_owner_original_owner_forgets, pinned ? 1 : 0);
	UT_ASSERT_EQ(page_owner_original_private_news, pinned ? 1 : 0);
	UT_ASSERT_EQ(page_owner_original_private_forgets, pinned ? 1 : 0);
	UT_ASSERT_EQ(page_owner_primitive_last_pin_reads, pinned ? 1 : 0);
	UT_ASSERT_EQ(page_owner_native_resource_owner.bufferarr.nitems, 0);
	UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&GetBufferDescriptor(0)->state)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&GetBufferDescriptor(0)->state) & BM_LOCKED, 0);
	for (unsigned i = 0; i < lengthof(page_owner_original_private_array); i++) {
		UT_ASSERT_EQ(page_owner_original_private_array[i].buffer, InvalidBuffer);
		UT_ASSERT_EQ(page_owner_original_private_array[i].refcount, 0);
	}
	UT_ASSERT(page_owner_original_private_reserved == &page_owner_original_private_array[0]);
	proof_owner_assert_released(v, true);
	free(page_owner_native_resource_owner.bufferarr.itemsarr);
	memset(&page_owner_native_resource_owner, 0, sizeof(page_owner_native_resource_owner));
	page_owner_original_private_reserved = NULL;
	page_owner_use_original_resource_owner = page_owner_use_original_private_refs = false;
	page_owner_use_original_primitive_pin = page_owner_resident_active = false;
	phase_use_original_recent_pin = false;
	GetBufferDescriptor(0)->tag = phase_recent_saved_tag;
	pg_atomic_write_u32(&GetBufferDescriptor(0)->state, phase_recent_saved_state);
}

UT_TEST(test_original_recent_first_pin_and_owner_release_precede_outer_leave)
{
	for (unsigned writer = 0; writer < 2; writer++) {
		for (unsigned batch = 0; batch < 2; batch++) {
			QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3];
			phase_recent_owner_setup(&f, &v, targets, batch != 0, writer != 0);
			UT_ASSERT_EQ(proof_owner_collect(&v, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
			UT_ASSERT_EQ(v.shared_freeze_xmin_candidates.target_count, 0);
			UT_ASSERT_EQ(phase_resident_releases[1], writer ? 0 : 1);
			UT_ASSERT_EQ(phase_resident_releases[2], writer ? 1 : 0);
			phase_recent_owner_finish(&v, true);
		}
	}
}

UT_TEST(test_original_recent_descriptor_misses_never_publish_pin_or_owner)
{
	for (unsigned fault = 1; fault < 3; fault++) {
		QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3];
		phase_recent_owner_setup(&f, &v, targets, true, true);
		phase_recent_fault = fault; phase_resident_expected_resumes = 0;
		UT_ASSERT(proof_owner_collect(&v, 1) != CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		UT_ASSERT(!v.shared_freeze_fence_valid);
		UT_ASSERT_EQ(v.shared_freeze_xmin_candidates.target_count, 0);
		UT_ASSERT_EQ(conditional_resumes, 0);
		phase_recent_owner_finish(&v, false);
	}
}

UT_TEST(test_original_recent_owner_unpins_before_collector_error_propagation)
{
	QualifiedXminFixture *f = malloc(sizeof(*f));
	LVRelState *v = malloc(sizeof(*v));
	ClusterHeapFreezeXminTargetV1 targets[3];
	UT_ASSERT(f != NULL && v != NULL);
	for (unsigned writer = 0; writer < 2; writer++) {
		for (unsigned stage = 0; stage < 2; stage++) {
			phase_recent_owner_setup(f, v, targets, true, writer != 0);
			if (stage == 0) recent_pin_observer = phase_resident_census_error_at_pin;
			if (stage == 1) phase_resident_hint_error = true;
			expecting_error = true;
			if (setjmp(error_jump) == 0) {
				(void)proof_owner_collect(v, 1);
				UT_ASSERT(false);
			}
			expecting_error = false; recent_pin_observer = NULL;
			UT_ASSERT_EQ(phase_resident_releases[0], !writer && stage == 0 ? 1 : 0);
			UT_ASSERT_EQ(phase_resident_releases[1], !writer && stage == 1 ? 1 : 0);
			UT_ASSERT_EQ(phase_resident_releases[2], writer ? 1 : 0);
			phase_recent_owner_finish(v, true);
		}
	}
	free(v); free(f);
}


static void
phase_recent_native_begin(const LVRelState *v)
{
	bool writer = v->shared_freeze_input.writer_context_valid;
	UT_ASSERT(phase_use_original_recent_pin && phase_use_original_resident && phase_resident_completed);
	UT_ASSERT_EQ(backend_pin_count, 0);
	UT_ASSERT(!local_x && !local_share && !resumed_writer && !recent_pin_held);
	UT_ASSERT_EQ(conditional_resumes, writer ? 1 : 0);
	UT_ASSERT_EQ(conditional_unlocks, 0); UT_ASSERT_EQ(local_lock_attempts, 1);
	UT_ASSERT_EQ(writer_checks, writer ? 1 : 0);
	UT_ASSERT_EQ(route_checks, 1); UT_ASSERT_EQ(authority_checks, 1);
	UT_ASSERT_EQ(admission_checks, 0);
	UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
	phase_prior_lock_activity = conditional_resumes + conditional_unlocks + local_lock_attempts;
	phase_prior_authority_activity = writer_checks + route_checks + authority_checks + admission_checks;
	phase_prior_writer_checks = writer_checks;
	phase_prior_route_checks = route_checks; phase_prior_authority_checks = authority_checks;
}

static void
phase_recent_native_setup(QualifiedXminFixture *f, LVRelState *v,
	ClusterHeapFreezeXminTargetV1 targets[3], bool batch, bool writer)
{
	phase_recent_owner_setup(f, v, targets, batch, writer);
	phase_finally_calls = phase_finally_debt = phase_finally_pins = 0;
	phase_finally_owned_writer = false;
	UT_ASSERT_EQ(bounds_original_census_enter(&f->admission), CLUSTER_SEMANTIC_ADMISSION_OK);
	phase_pin_released = false;
}

static void
phase_recent_native_finish(const QualifiedXminFixture *f, const LVRelState *v, bool changed)
{
	ClusterSemanticAdmissionToken zero = {0};
	UT_ASSERT_EQ(recent_pin_calls, 2); UT_ASSERT_EQ(recent_unpins, 2);
	UT_ASSERT_EQ(phase_recent_usage.shared_blks_hit, 2);
	UT_ASSERT_EQ(phase_recent_usage.local_blks_hit, 0);
	UT_ASSERT_EQ(phase_recent_ko_samples, 2);
	UT_ASSERT_EQ(page_owner_original_owner_enlarges, 2);
	UT_ASSERT_EQ(page_owner_original_owner_allocations, 1);
	UT_ASSERT_EQ(page_owner_original_owner_remembers, 2);
	UT_ASSERT_EQ(page_owner_original_owner_forgets, 2);
	UT_ASSERT_EQ(page_owner_original_private_news, 2);
	UT_ASSERT_EQ(page_owner_original_private_forgets, 2);
	UT_ASSERT_EQ(page_owner_primitive_last_pin_reads, 2);
	UT_ASSERT_EQ(page_owner_native_resource_owner.bufferarr.nitems, 0);
	UT_ASSERT_EQ(phase_finally_calls, 1); UT_ASSERT_EQ(phase_finally_debt, 8);
	UT_ASSERT_EQ(phase_finally_pins, 0); UT_ASSERT(!phase_finally_owned_writer);
	UT_ASSERT_EQ(inserts, changed ? 1 : 0); UT_ASSERT_EQ(flushes, changed ? 1 : 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&native_admission_shared.inflight
		[CLUSTER_SEMANTIC_TARGET_SIDE][bounds_lifecycle_feature()]), 7);
	UT_ASSERT_EQ(semantic_activation_local_inflight
		[CLUSTER_SEMANTIC_TARGET_SIDE][bounds_lifecycle_feature()], 0);
	UT_ASSERT(memcmp(&f->admission, &zero, sizeof(zero)) == 0);
	UT_ASSERT(!bounds_original_backend_has_admission());
	UT_ASSERT(!local_x && !local_share && !resumed_writer && !recent_pin_held);
	UT_ASSERT_EQ(backend_pin_count, 0);
	UT_ASSERT(cluster_bufmgr_pcm_x_writer_find(GetBufferDescriptor(0)) == NULL);
	UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&GetBufferDescriptor(0)->state)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&GetBufferDescriptor(0)->state) & BM_LOCKED, 0);
	UT_ASSERT(PG_exception_stack == NULL && error_context_stack == NULL);
	for (unsigned i = 0; i < lengthof(page_owner_original_private_array); i++) {
		UT_ASSERT_EQ(page_owner_original_private_array[i].buffer, InvalidBuffer);
		UT_ASSERT_EQ(page_owner_original_private_array[i].refcount, 0);
	}
	if (changed) {
		UT_ASSERT(v->shared_freeze_xmin_candidates.target_count > 0);
		for (unsigned i = 0; i < v->shared_freeze_xmin_candidates.target_count; i++) {
			uint16 tuple_index = v->shared_freeze_xmin_candidates.targets[i].tuple_index;
			OffsetNumber offset;

			UT_ASSERT(tuple_index < v->shared_freeze_batch.tuple_count);
			offset = v->shared_freeze_batch.tuples[tuple_index].offset;
			UT_ASSERT(OffsetNumberIsValid(offset));
			UT_ASSERT(HeapTupleHeaderXminFrozen(freeze_tuple(current_page.data, offset)));
		}
	} else
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
	free(page_owner_native_resource_owner.bufferarr.itemsarr);
	memset(&page_owner_native_resource_owner, 0, sizeof(page_owner_native_resource_owner));
	page_owner_original_private_reserved = NULL;
	page_owner_use_original_resource_owner = page_owner_use_original_private_refs = false;
	page_owner_use_original_primitive_pin = page_owner_resident_active = false;
	phase_use_original_recent_pin = false;
	GetBufferDescriptor(0)->tag = phase_recent_saved_tag;
	pg_atomic_write_u32(&GetBufferDescriptor(0)->state, phase_recent_saved_state);
}

UT_TEST(test_original_resident_recent_pin_chain_reaches_same_owner_native_wal)
{
	for (unsigned batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3];
		phase_recent_native_setup(&f, &v, targets, batch != 0, true);
		UT_ASSERT(phase_then_recent_native_owned(&f, &v));
		UT_ASSERT_EQ(conditional_resumes, 2); UT_ASSERT_EQ(conditional_unlocks, 1);
		UT_ASSERT_EQ(local_unlocks, 2);
		UT_ASSERT_EQ(v.shared_freeze_xmin_candidates.target_count, batch ? 3 : 1);
		phase_recent_native_finish(&f, &v, true);
	}
}

static void
phase_recent_revoke_cut_on_second_pin(void)
{
	if (recent_pin_calls != 2) return;
	UT_ASSERT(phase_resident_completed && recent_pin_held && recent_pin_admission->entered);
	UT_ASSERT(!local_x && !resumed_writer);
	ctrc_note_unqualified_registration();
}

UT_TEST(test_original_resident_recent_native_refuses_read_context_and_late_cut)
{
	for (unsigned batch = 0; batch < 2; batch++) {
		for (unsigned fault = 0; fault < 2; fault++) {
			QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3];
			phase_recent_native_setup(&f, &v, targets, batch != 0, fault != 0);
			if (fault != 0) recent_pin_observer = phase_recent_revoke_cut_on_second_pin;
			UT_ASSERT(!phase_then_recent_native_owned(&f, &v));
			recent_pin_observer = NULL;
			UT_ASSERT_EQ(conditional_resumes, fault != 0 ? 1 : 0);
			UT_ASSERT_EQ(conditional_unlocks, 0);
			UT_ASSERT_EQ(target_preflight_checks + policy_current_checks + cohort_current_checks, 0);
			phase_recent_native_finish(&f, &v, false);
		}
	}
}

UT_TEST(test_original_resident_recent_native_errors_unwind_two_pin_owner_scopes)
{
	QualifiedXminFixture *f = malloc(sizeof(*f)); LVRelState *v = malloc(sizeof(*v));
	ClusterHeapFreezeXminTargetV1 targets[3];
	UT_ASSERT(f != NULL && v != NULL);
	for (unsigned batch = 0; batch < 2; batch++) {
		for (unsigned stage = 0; stage < 2; stage++) {
			phase_recent_native_setup(f, v, targets, batch != 0, true);
			target_recheck_error = stage == 0; flush_error = stage == 1;
			expecting_error = true;
			if (setjmp(error_jump) == 0) {
				(void)phase_then_recent_native_owned(f, v); UT_ASSERT(false);
			}
			expecting_error = false;
			UT_ASSERT_EQ(conditional_resumes, 2); UT_ASSERT_EQ(conditional_unlocks, 1);
			UT_ASSERT_EQ(local_unlocks, 2);
			phase_recent_native_finish(f, v, stage == 1);
		}
	}
	free(v); free(f);
}

/* The native shim already invokes the complete original TARGET/census
 * predicates. Compose their publication/startup cuts with both real pin
 * cycles; no extra eligibility flag or replacement predicate is needed. */
static void
phase_recent_disable_target_on_second_pin(void)
{
	if (recent_pin_calls != 2) return;
	UT_ASSERT_EQ(backend_pin_count, 1); UT_ASSERT(!local_x && !local_share);
	bounds_admission_change(1);
	UT_ASSERT(bounds_original_census_current(native_admission_expected_token));
	UT_ASSERT(!bounds_original_target_current(native_admission_expected_token));
}

static void
phase_recent_assert_admission_cut(unsigned stage)
{
	UT_ASSERT(stage >= 1 && stage <= 6);
	for (unsigned i = 1; i <= 6; i++)
		UT_ASSERT_EQ(native_admission_checks[i], i <= stage ? 1 : 0);
	UT_ASSERT_EQ(conditional_resumes, stage >= 3 ? 2 : 1);
	UT_ASSERT_EQ(conditional_unlocks, stage >= 3 ? 1 : 0);
	UT_ASSERT_EQ(local_unlocks, stage >= 3 ? 2 : 1);
	UT_ASSERT_EQ(space_observation_checks, stage >= 5 ? 1 : 0);
	UT_ASSERT_EQ(CritSectionCount, 0);
}

UT_TEST(test_original_group_exit_retains_source_through_recent_wal)
{
	source_exit_group_requested = true;
	for (unsigned batch = 0; batch < 2; batch++)
		for (unsigned terminal = 0; terminal <= 2; terminal++)
			full_source_then_recent_lifecycle_case(batch != 0, false, true, terminal, 0);
	source_exit_group_requested = false;
}

UT_TEST(test_original_group_exit_late_receiver_revokes_second_pin_wal)
{
	source_exit_group_requested = true;
	for (unsigned batch = 0; batch < 2; batch++)
		for (unsigned terminal = 0; terminal <= 2; terminal++)
			full_source_then_recent_lifecycle_case(batch != 0, true, true, terminal, 0);
	source_exit_group_requested = false;
}

UT_TEST(test_original_group_exit_certificate_precedes_fresh_recent_cut)
{
	source_exit_group_requested = true;
	for (unsigned batch = 0; batch < 2; batch++)
		for (unsigned retirement = 1; retirement <= 2; retirement++)
			full_source_then_recent_lifecycle_case(batch != 0, false, true, 1, retirement);
	source_exit_group_requested = false;
}

UT_TEST(test_original_group_member_exit_retains_source_through_recent_wal)
{
	source_exit_group_requested = source_exit_member_requested = true;
	for (unsigned batch = 0; batch < 2; batch++) {
		for (unsigned terminal = 0; terminal <= 2; terminal++) {
			source_exit_member_wakeups = 0;
			full_source_then_recent_lifecycle_case(batch != 0, false, true, terminal, 0);
			UT_ASSERT_EQ(source_exit_member_wakeups, 1);
		}
	}
	source_exit_group_requested = source_exit_member_requested = false;
}

UT_TEST(test_original_group_member_exit_late_receiver_revokes_recent_wal)
{
	source_exit_group_requested = source_exit_member_requested = true;
	for (unsigned batch = 0; batch < 2; batch++)
		for (unsigned terminal = 0; terminal <= 2; terminal++)
			full_source_then_recent_lifecycle_case(batch != 0, true, true, terminal, 0);
	source_exit_group_requested = source_exit_member_requested = false;
}

UT_TEST(test_original_group_member_exit_certificate_needs_fresh_recent_cut)
{
	source_exit_group_requested = source_exit_member_requested = true;
	for (unsigned batch = 0; batch < 2; batch++)
		for (unsigned retirement = 1; retirement <= 2; retirement++)
			full_source_then_recent_lifecycle_case(batch != 0, false, true, 1, retirement);
	source_exit_group_requested = source_exit_member_requested = false;
}

UT_TEST(test_resident_recent_native_original_startup_qualification_at_each_cut)
{
	const ClusterNormalStartState serving[] = { CLUSTER_NORMAL_START_EXISTING_OTHER,
		CLUSTER_NORMAL_START_SOURCE_ZERO, CLUSTER_NORMAL_START_TARGET_READY };
	for (unsigned batch = 0; batch < 2; batch++) {
		for (unsigned n = 0; n < lengthof(serving); n++) {
			QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3];
			phase_recent_native_setup(&f, &v, targets, batch != 0, true);
			bounds_start_publish_state(serving[n]);
			UT_ASSERT(phase_then_recent_native_owned(&f, &v));
			for (unsigned i = 1; i <= 6; i++) UT_ASSERT_EQ(native_admission_checks[i], 1);
			phase_recent_native_finish(&f, &v, true);
		}
		for (unsigned stage = 1; stage <= 6; stage++) {
			for (unsigned fault = 0; fault <= 4; fault++) {
				QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3];
				phase_recent_native_setup(&f, &v, targets, batch != 0, true);
				native_start_change_at = stage; native_start_fault = fault;
				UT_ASSERT(!phase_then_recent_native_owned(&f, &v));
				phase_recent_assert_admission_cut(stage);
				phase_recent_native_finish(&f, &v, false);
			}
		}
	}
}

UT_TEST(test_resident_recent_native_original_publication_identity_at_each_cut)
{
	const unsigned snapshot_faults[] = { 2, 3, 4, 5, 6 };
	for (unsigned batch = 0; batch < 2; batch++) {
		{
			QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3];
			phase_recent_native_setup(&f, &v, targets, batch != 0, true);
			/* Read-only census does not grant writer permission. A raw bit-only
			 * fault is deliberately not a census-identity refusal condition. */
			recent_pin_observer = phase_recent_disable_target_on_second_pin;
			UT_ASSERT(!phase_then_recent_native_owned(&f, &v));
			recent_pin_observer = NULL;
			phase_recent_assert_admission_cut(1);
			phase_recent_native_finish(&f, &v, false);
		}
		for (unsigned stage = 1; stage <= 6; stage++) {
			/* Torn/closed/drifted snapshots keep the caller's token and debt. */
			for (unsigned n = 0; n < lengthof(snapshot_faults); n++) {
				QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3];
				phase_recent_native_setup(&f, &v, targets, batch != 0, true);
				native_admission_fault_at = stage; native_admission_fault = snapshot_faults[n];
				UT_ASSERT(!phase_then_recent_native_owned(&f, &v));
				phase_recent_assert_admission_cut(stage);
				phase_recent_native_finish(&f, &v, false);
			}
			/* Completed original publications: closed images, reopened newer
			 * generations with either bit value, and epoch closure all retire
			 * the old writer/census cut before the original native mutation. */
			for (unsigned mode = 0; mode < 5; mode++) {
				QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3];
				phase_recent_native_setup(&f, &v, targets, batch != 0, true);
				native_admission_fault_at = stage; native_admission_fault = 24 + mode;
				UT_ASSERT(!phase_then_recent_native_owned(&f, &v));
				phase_recent_assert_admission_cut(stage);
				phase_recent_native_finish(&f, &v, false);
			}
		}
	}
}

UT_TEST(test_resident_recent_native_original_admission_errors_retire_owned_debt_last)
{
	QualifiedXminFixture *f = malloc(sizeof(*f)); LVRelState *v = malloc(sizeof(*v));
	ClusterHeapFreezeXminTargetV1 targets[3];
	UT_ASSERT(f != NULL && v != NULL);
	for (unsigned batch = 0; batch < 2; batch++) {
		for (unsigned stage = 1; stage <= 6; stage++) {
			for (unsigned fault = 21; fault <= 22; fault++) {
				phase_recent_native_setup(f, v, targets, batch != 0, true);
				native_admission_fault_at = stage; native_admission_fault = fault;
				expecting_error = true;
				if (setjmp(error_jump) == 0) {
					(void)phase_then_recent_native_owned(f, v); UT_ASSERT(false);
				}
				expecting_error = false;
				phase_recent_assert_admission_cut(stage);
				phase_recent_native_finish(f, v, false);
			}
		}
	}
	free(v); free(f);
}


UT_TEST(test_recent_xmin_saved_bounds_refuse_before_pin)
{
	for (unsigned batch = 0; batch < 2; batch++)
		for (unsigned owned = 0; owned < 2; owned++)
			for (unsigned fault = 0; fault < 9; fault++)
			{
				QualifiedXminFixture f;
				ClusterHeapFreezeXminTargetV1 targets[3];
				if (batch) qualified_xmin_batch_setup(&f, targets);
				else {
					qualified_xmin_setup(&f, HEAP_XMAX_IS_MULTI);
					targets[0] = (ClusterHeapFreezeXminTargetV1) {
						0, FullTransactionIdFromEpochAndXid(1, 77)};
				}
				if (!owned) {
					fixture_release_original_writer();
					backend_pin_count = 0;
				}
				relation_data.rd_isvalid = true;
				switch (fault) {
				case 0: f.input.freeze_policy = NULL; break;
				case 1: f.policy.freeze_limit = FullTransactionIdFromEpochAndXid(1, 99); break;
				case 2: f.cohort.horizon_scn++; break;
				case 3: f.cohort.source_local_horizon_scn--; break;
				case 4: f.cohort.record_generation++; break;
				case 5: f.cohort.formation_epoch++; break;
				case 6: f.cohort.admission_fence.admitted_members_lo = 0; break;
				case 7: f.cohort.source_local_history.sample_clock_scn = InvalidScn; break;
				case 8: f.cohort.local_registration_cut.min_full = FullTransactionIdFromEpochAndXid(1, 99); break;
				}
				UT_ASSERT(!fixture_recent_xmin_apply(&f, targets, batch ? 3 : 1));
				UT_ASSERT_EQ(recent_pin_calls + recent_unpins, 0);
				UT_ASSERT_EQ(backend_pin_count, owned ? 1 : 0);
				UT_ASSERT_EQ(local_x, owned != 0);
				UT_ASSERT_EQ(cluster_bufmgr_pcm_x_writer_find(GetBufferDescriptor(0)) != NULL, owned != 0);
				UT_ASSERT(f.admission.entered);
				UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
				UT_ASSERT_EQ(target_preflight_checks + census_preflight_checks + full_target_checks + admission_checks, 0);
				UT_ASSERT_EQ(policy_current_checks + cohort_current_checks + wal_clock_reads, 0);
				UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
				UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
			}
}


UT_TEST(test_original_snapshot_h_survives_group_exit_to_recent_wal)
{
	source_exit_history_requested = true;
	source_exit_group_requested = source_exit_member_requested = true;
	for (unsigned receipt = 0; receipt < 3; receipt++)
		for (unsigned batch = 0; batch < 2; batch++) {
			source_exit_history_samples = 0;
			full_source_then_recent_lifecycle_case(batch != 0, false, true, receipt, 0);
			UT_ASSERT_EQ(source_exit_history_samples, 1);
		}
	source_exit_history_requested = false;
	source_exit_group_requested = source_exit_member_requested = false;
}


/* The outer owner may retain read-only proof results for reporting, but its
 * continuation fence, borrowed sources and candidates must retire before leave. */
UT_TEST(test_real_outer_collector_retires_continuation_before_admission_leave)
{
	for (unsigned batch = 0; batch < 2; batch++) {
		QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3];
		proof_owner_setup(&f, &v, targets, batch != 0);
		proof_owner_retirement_observer = &v;
		proof_owner_retired_before_leave = 0;
		UT_ASSERT_EQ(proof_owner_collect(&v, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		proof_owner_retirement_observer = NULL;
		UT_ASSERT_EQ(proof_owner_retired_before_leave, 1);
		UT_ASSERT(v.shared_freeze_proofs.proof_count > 0);
		UT_ASSERT_EQ(phase_preflights, 1); UT_ASSERT_EQ(phase_rechecks, 1);
		UT_ASSERT_EQ(phase_resolves, f.page.carrier_request_count);
		proof_owner_assert_released(&v, true);
	}
}

UT_TEST(test_real_outer_collector_errors_retire_continuation_before_admission_leave)
{
	QualifiedXminFixture *f = malloc(sizeof(*f));
	LVRelState *v = malloc(sizeof(*v));
	ClusterHeapFreezeXminTargetV1 targets[3];
	UT_ASSERT(f != NULL && v != NULL);
	for (unsigned batch = 0; batch < 2; batch++) {
		for (unsigned stage = 1; stage <= 4; stage++) {
			proof_owner_setup(f, v, targets, batch != 0);
			v->shared_freeze_fence_valid = true;
			proof_owner_retirement_observer = v;
			proof_owner_retired_before_leave = 0;
			if (stage == 4) bounds_fault = 7;
			else phase_error_stage = stage;
			expecting_error = true;
			if (setjmp(error_jump) == 0) {
				(void)proof_owner_collect(v, 1);
				UT_ASSERT(false);
			}
			expecting_error = false;
			proof_owner_retirement_observer = NULL;
			UT_ASSERT_EQ(proof_owner_retired_before_leave, 1);
			UT_ASSERT_EQ(proof_owner_enters, 1);
			UT_ASSERT_EQ(phase_preflights, 1);
			proof_owner_assert_released(v, true);
		}
	}
	free(v); free(f);
}

/* Reusing a borrowed owner does not carry a previous attempt's continuation
 * through a new preflight refusal. Caller pins and the entered owner remain. */
UT_TEST(test_admitted_reuse_refusals_retire_previous_continuation)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3];
		ClusterSemanticAdmissionToken original, rejected;
		const ClusterSemanticAdmissionToken *token;
		const PcmAuthoritySnapshot *authority;
		bool rows_current = false, *out = &rows_current;
		unsigned preflights, resolves, rechecks, proof_count;
		Buffer recent = 1;
		phase_setup(&f, &v, targets, true);
		/* Release only the fixture's legacy pin model, then call the complete
		 * production admitted body directly, including the caller-pin guard. */
		backend_pin_count = 0; phase_pin_released = true;
		UT_ASSERT_EQ(phase_actual_admitted(&v, 1, &f.authority,
			f.fence.master_sequence, &f.admission, &rows_current), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		UT_ASSERT(rows_current && v.shared_freeze_fence_valid);
		UT_ASSERT(v.shared_freeze_xmin_candidates.target_count > 0);
		preflights = phase_preflights; resolves = phase_resolves; rechecks = phase_rechecks;
		proof_count = v.shared_freeze_proofs.proof_count;
		memcpy(&original, &f.admission, sizeof(original));
		memcpy(&rejected, &f.admission, sizeof(rejected));
		token = &f.admission; authority = &f.authority;
		if (fault == 0) recent = InvalidBuffer;
		if (fault == 1) backend_pin_count = 1;
		if (fault == 2) { rejected.entered = false; token = &rejected; }
		if (fault == 3) authority = NULL;
		if (fault == 4) out = NULL;
		UT_ASSERT_EQ(phase_actual_admitted(&v, recent, authority,
			f.fence.master_sequence, token, out), CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
		UT_ASSERT(!v.shared_freeze_fence_valid && !v.shared_freeze_cohort_observed);
		UT_ASSERT(v.shared_freeze_input.freeze_policy == NULL
			&& v.shared_freeze_input.freeze_cohort == NULL);
		UT_ASSERT_EQ(v.shared_freeze_xmin_candidates.target_count, 0);
		UT_ASSERT_EQ(v.shared_freeze_aborted_xmax_candidates.target_count, 0);
		UT_ASSERT(!FullTransactionIdIsValid(v.shared_freeze_candidate_limit_full));
		for (unsigned i = 0; i < sizeof(v.shared_freeze_cohort); i++)
			UT_ASSERT_EQ(((const unsigned char *)&v.shared_freeze_cohort)[i], 0);
		UT_ASSERT_EQ(v.shared_freeze_proofs.proof_count, proof_count);
		if (out != NULL) UT_ASSERT(!rows_current);
		UT_ASSERT_EQ(phase_preflights, preflights);
		UT_ASSERT_EQ(phase_resolves, resolves); UT_ASSERT_EQ(phase_rechecks, rechecks);
		UT_ASSERT_EQ(backend_pin_count, fault == 1 ? 1 : 0);
		UT_ASSERT(memcmp(&f.admission, &original, sizeof(original)) == 0);
		UT_ASSERT(bounds_original_backend_has_admission());
		UT_ASSERT_EQ(inserts + edges + dirties + flushes, 0);
		UT_ASSERT(memcmp(current_page.data, before_page.data, BLCKSZ) == 0);
		backend_pin_count = 0;
		bounds_original_census_leave(&f.admission); bounds_lifecycle_assert_empty(&f.admission);
	}
}

/* Join the real admitted/resident path to the existing qualified subset
 * native entry. Whole-page KEEP is never upgraded to whole-page proof. */
UT_TEST(test_mixed_admitted_resident_subset_reaches_same_owner_recent_wal)
{
	QualifiedXminFixture f; LVRelState v; ClusterHeapFreezeXminTargetV1 targets[3];
	bool applied;

	phase_recent_native_setup(&f, &v, targets, true, true);
	phase_mixed_proof_subset = true;
	applied = phase_then_recent_native_owned(&f, &v);
	phase_mixed_proof_subset = false;
	UT_ASSERT(applied);
	UT_ASSERT_EQ(phase_preflights, 1); UT_ASSERT_EQ(phase_resolves, 3);
	UT_ASSERT_EQ(phase_rechecks, 1);
	UT_ASSERT_EQ(v.shared_freeze_batch.carrier_request_count, 3);
	UT_ASSERT_EQ(v.shared_freeze_proofs.proof_count, 2);
	UT_ASSERT(!v.shared_freeze_fence_valid);
	UT_ASSERT_EQ(v.shared_freeze_xmin_candidates.target_count, 2);
	UT_ASSERT_EQ(((xl_heap_freeze_page *)freeze_main_data.data)->snapshotConflictHorizon, 77);
	for (int i = 1; i <= 3; i++) {
		HeapTupleHeader tuple = freeze_tuple(current_page.data, i);
		HeapTupleHeader old = freeze_tuple(before_page.data, i);
		UT_ASSERT_EQ(tuple->t_infomask,
			i == 2 ? old->t_infomask : old->t_infomask | HEAP_XMIN_FROZEN);
		UT_ASSERT_EQ(tuple->t_infomask2, old->t_infomask2);
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(tuple), HeapTupleHeaderGetRawXmin(old));
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmax(tuple), HeapTupleHeaderGetRawXmax(old));
		UT_ASSERT(memcmp(ClusterPageGetItlSlot(current_page.data, i - 1),
			ClusterPageGetItlSlot(before_page.data, i - 1), sizeof(ClusterItlSlotData)) == 0);
	}
	UT_ASSERT(memcmp(freeze_tuple(current_page.data, 2),
		freeze_tuple(before_page.data, 2), 64) == 0);
	check_publication(true);
	replay_freeze_record();
	phase_recent_native_finish(&f, &v, true);
}

int
main(void)
{
	UT_PLAN(343);

	UT_RUN(test_pin_free_admitted_phase_reaches_recent_native_wal_before_owner_leave);
	UT_RUN(test_pin_free_phase_recent_refusal_retires_only_its_original_owner);
	UT_RUN(test_pin_free_phase_recent_errors_unwind_pin_before_original_owner_leave);
	UT_RUN(test_recent_unknown_receiver_revokes_pin_free_proof_before_writer_or_wal);
	UT_RUN(test_recent_refused_receiver_keeps_original_cut_and_native_continuation);
	UT_RUN(test_full_original_source_reaches_pin_free_collector_and_recent_wal);
	UT_RUN(test_full_original_source_late_receiver_revokes_recent_wal);
	UT_RUN(test_original_lookup_pin_feeds_resident_stamp_and_recent_freeze);
	UT_RUN(test_original_primitive_pin_feeds_resident_stamp_and_recent_freeze);
	UT_RUN(test_original_private_ref_table_feeds_pin_stamp_and_recent_freeze);
	UT_RUN(test_original_resource_owner_feeds_pin_stamp_and_recent_freeze);
	UT_RUN(test_original_resident_stamp_owner_feeds_shared_retention_and_recent_freeze);
	UT_RUN(test_original_bufmgr_authority_feeds_touch_registration_and_recent_freeze);
	UT_RUN(test_original_exact_touch_registration_feeds_finish_and_recent_freeze);
	UT_RUN(test_original_top_xact_finish_retires_local_touches_before_recent_freeze);
	UT_RUN(test_original_page_owner_range_groups_reused_slot_before_recent_freeze);
	UT_RUN(test_original_page_owner_stamp_wal_discharge_feeds_recent_freeze);
	UT_RUN(test_original_page_owner_discharge_feeds_certificate_recent_wal);
	UT_RUN(test_original_native_exit_applied_certificate_retirement_precedes_recent_wal);
	UT_RUN(test_original_native_exit_certificate_receiver_retirement_keeps_origin_in_recent_cut);
	UT_RUN(test_original_native_exit_final_certificate_reply_precedes_fresh_recent_cut);
	UT_RUN(test_original_pgproc_exit_cancelled_receipt_keeps_source_through_recent_wal);
	UT_RUN(test_original_pgproc_exit_applied_receipt_keeps_source_through_recent_wal);
	UT_RUN(test_original_pgproc_exit_keeps_retained_source_through_recent_wal);
	UT_RUN(test_original_pgproc_exit_late_receiver_still_revokes_recent_wal);
	UT_RUN(test_recent_xmin_stale_registration_refuses_before_resident_pin);
	UT_RUN(test_recent_xmin_success_preserves_preowned_pin_and_writer);
	UT_RUN(test_recent_xmin_errors_keep_borrowed_pin_and_writer_scope);
	UT_RUN(test_recent_xmin_share_lock_refusal_keeps_borrowed_pin_without_upgrade);
	UT_RUN(test_recent_xmin_pin_continues_original_admission_to_native_wal);
	UT_RUN(test_recent_xmin_missing_resident_keeps_original_owner_without_fetch);
	UT_RUN(test_recent_xmin_bad_inputs_refuse_before_resident_pin);
	UT_RUN(test_recent_xmin_native_refusal_releases_only_new_resident_pin);
	UT_RUN(test_recent_xmin_refusal_preserves_preowned_pin_and_content_lock);
	UT_RUN(test_recent_xmin_errors_unwind_writer_then_pin_without_leaving_admission);
	UT_RUN(test_admitted_proof_cannot_continue_native_wal_after_original_owner_leave);
	UT_RUN(test_admitted_proof_and_native_errors_rethrow_after_original_owner_finally);
	UT_RUN(test_admitted_proof_phase_keeps_original_owner_through_native_wal);
	UT_RUN(test_original_census_enter_refuses_before_debt_and_clears_output);
	UT_RUN(test_original_census_enter_rolls_back_increment_on_second_snapshot_refusal);
	UT_RUN(test_original_census_counter_saturation_preserves_existing_debt);
	UT_RUN(test_original_census_leave_is_once_and_preserves_other_admitted_debt);
	UT_RUN(test_original_census_debt_survives_observer_and_native_error_until_owner_leave);
	UT_RUN(test_original_census_exit_callback_releases_only_registered_local_debt);
	UT_RUN(test_original_census_entry_permission_does_not_grant_native_writer);
	UT_RUN(test_original_census_enter_holds_debt_through_observer_and_native_wal);
	UT_RUN(test_native_original_startup_loading_refuses_without_releasing_caller_scope);
	UT_RUN(test_native_original_startup_serving_states_reach_single_native_wal);
	UT_RUN(test_native_original_startup_refuses_retired_or_unknown_state_at_each_cut);
	UT_RUN(test_collector_original_startup_rechecks_state_before_borrowing_sources);
	UT_RUN(test_original_startup_atomic_reader_and_generation_zero_bootstrap_contract);
	UT_RUN(test_original_startup_nonshared_config_bypass_does_not_publish_or_freeze);
	UT_RUN(test_native_original_admission_rejects_wrong_side_before_writer_resume);
	UT_RUN(test_native_original_same_token_admission_preserves_new_and_preowned_writer);
	UT_RUN(test_native_original_admission_source_refusal_preserves_caller_ownership);
	UT_RUN(test_native_original_admission_error_unwinds_only_new_writer_scope);
	UT_RUN(test_collector_same_admission_rejects_wrong_target_side);
	UT_RUN(test_collector_original_same_token_admission_reaches_native_wal);
	UT_RUN(test_collector_original_census_rejects_token_and_source_drift_at_each_check);
	UT_RUN(test_collector_census_read_permission_does_not_replace_original_target);
	UT_RUN(test_collector_original_admission_error_preserves_pin_and_entered_token);
	UT_RUN(test_original_target_and_census_rechecks_reject_missing_token_without_source_reads);
	UT_RUN(test_collector_saved_policy_original_fence_reaches_native_wal_without_resample);
	UT_RUN(test_collector_saved_policy_config_refusal_precedes_original_fence);
	UT_RUN(test_collector_saved_policy_original_lease_fields_reject_before_writer);
	UT_RUN(test_collector_saved_policy_rechecks_incarnation_and_quorum_after_unlock);
	UT_RUN(test_collector_saved_policy_error_leaves_original_lease_lock_and_caller_intact);
	UT_RUN(test_collector_saved_current_uses_original_sources_without_history_resample);
	UT_RUN(test_collector_saved_current_refuses_worker_lease_and_admission_drift);
	UT_RUN(test_collector_saved_current_refuses_original_registration_changes);
	UT_RUN(test_collector_saved_current_refuses_original_end_clock_staleness);
	UT_RUN(test_collector_saved_current_error_preserves_caller_page_pin_and_admission);
	UT_RUN(test_collector_original_mrp_config_and_shared_source_feed_history);
	UT_RUN(test_collector_original_mrp_publisher_preserves_monotone_watermark);
	UT_RUN(test_collector_original_mrp_unknown_source_blocks_single_and_batch_wal);
	UT_RUN(test_collector_original_mrp_publication_reaches_native_creation_and_wal);
	UT_RUN(test_collector_original_mrp_source_lost_after_history_unlock_refuses);
	UT_RUN(test_collector_original_history_same_cut_clock_and_compact_snapshot);
	UT_RUN(test_collector_original_history_refusals_block_single_and_batch_wal);
	UT_RUN(test_collector_original_history_mrp_fold_keeps_same_cut_attribution);
	UT_RUN(test_collector_original_history_minimum_reaches_native_creation_plan);
	UT_RUN(test_collector_original_history_unlock_registration_invalidates_cut);
	UT_RUN(test_collector_original_cohort_acquisition_publishes_only_after_full_local_checks);
	UT_RUN(test_collector_original_cohort_acquisition_refusal_blocks_single_and_batch_wal);
	UT_RUN(test_collector_original_cohort_end_clock_rechecks_worker_admission_and_ingress);
	UT_RUN(test_collector_original_cohort_missing_required_peer_never_borrows_caller_view);
	UT_RUN(test_collector_original_cohort_provider_error_retains_caller_resources);
	UT_RUN(test_collector_observer_owned_sources_reach_single_and_batch_native_wal);
	UT_RUN(test_collector_observer_refusals_cannot_supply_native_bound_sources);
	UT_RUN(test_collector_observer_borrows_still_require_original_native_source_current);
	UT_RUN(test_collector_observer_owned_sources_are_rechecked_after_actual_space);
	UT_RUN(test_collector_observer_error_preserves_caller_pin_admission_and_page);
	UT_RUN(test_collector_observer_keep_unproven_preserves_selected_native_subset);
	UT_RUN(test_census_durable_certificate_rejects_empty_snapshot_without_commit);
	UT_RUN(test_census_original_durable_publisher_commits_after_readback_and_cleanup);
	UT_RUN(test_census_original_durable_publisher_preflight_keeps_origin_before_wait);
	UT_RUN(test_census_original_durable_publisher_refusals_never_commit_origin);
	UT_RUN(test_census_original_durable_publisher_error_unpins_before_guard_and_admission);
	UT_RUN(test_census_original_durable_publish_then_retirement_keeps_old_native_candidate_closed);
	UT_RUN(test_census_origin_rejects_unarmed_certificate_reply_without_retiring);
	UT_RUN(test_census_original_certificate_reply_retires_origin_and_inbox_once);
	UT_RUN(test_census_original_origin_reply_rejects_stale_request_or_result);
	UT_RUN(test_census_original_origin_reclaim_rechecks_exact_ack_inbox);
	UT_RUN(test_census_original_origin_duplicate_reply_id_retains_both_origins);
	UT_RUN(test_census_original_origin_retirement_advances_walk_and_rejects_old_candidate);
	UT_RUN(test_census_original_nonempty_ack_snapshot_can_freeze_cancelled_receipt);
	UT_RUN(test_census_original_ack_hashes_full_sorted_receipt_chain_before_reclaim);
	UT_RUN(test_census_original_ack_refuses_durability_or_two_capture_drift);
	UT_RUN(test_census_original_ack_allocation_refusal_retains_ready_receiver);
	UT_RUN(test_census_original_ack_error_unwinds_private_allocations_before_caller);
	UT_RUN(test_census_original_ack_then_certificate_keeps_origin_before_native_wal);
	UT_RUN(test_census_premature_certificate_cannot_reclaim_retained_receipt);
	UT_RUN(test_census_certificate_reclaims_only_frozen_cancelled_receiver);
	UT_RUN(test_census_certificate_refuses_stale_summary_or_incomplete_chain);
	UT_RUN(test_census_certificate_preflight_keeps_all_slots_on_bad_batch);
	UT_RUN(test_census_certificate_upgrade_rechecks_before_first_delete);
	UT_RUN(test_census_certificate_receiver_retirement_keeps_origin_through_native_wal);
	UT_RUN(test_census_original_receipt_cancel_keeps_participant_full_identity);
	UT_RUN(test_census_original_receipt_apply_is_idempotent_and_retains_full_identity);
	UT_RUN(test_census_mixed_receipt_dispositions_keep_original_chain_and_protection);
	UT_RUN(test_census_receipt_terminal_stale_handles_refuse_before_disposition_change);
	UT_RUN(test_census_receipt_apply_invalid_final_targets_keep_pending_protection);
	UT_RUN(test_census_terminal_receipts_remain_in_complete_walk_before_native_wal);
	UT_RUN(test_census_participant_publisher_retains_original_origin_full_identity);
	UT_RUN(test_census_participant_original_publish_and_duplicate_keep_chain_and_full_identity);
	UT_RUN(test_census_participant_unknown_or_foreign_origin_stays_unqualified_on_later_replay);
	UT_RUN(test_census_participant_shared_preflight_refuses_before_new_receiver);
	UT_RUN(test_census_participant_known_full_conflict_precedes_chain_refusal_without_mutation);
	UT_RUN(test_census_participant_original_publisher_then_complete_walk_continues_native_wal);

	UT_RUN(test_census_shared_origin_reservation_retains_original_full_identity);
	UT_RUN(test_census_shared_origin_exact_open_and_replay_use_physical_slot);
	UT_RUN(test_census_shared_origin_backend_preflight_refuses_without_publication);
	UT_RUN(test_census_shared_origin_raw_reuse_cannot_replace_original_full_identity);
	UT_RUN(test_census_shared_origin_open_refuses_stale_reservation_without_grant);
	UT_RUN(test_census_shared_origin_producer_then_complete_walk_continues_native_wal);
	UT_RUN(test_census_original_initializer_publishes_first_registration_cut);
	UT_RUN(test_census_original_initializer_clears_new_memory_and_local_state);
	UT_RUN(test_census_original_initializer_reattaches_without_resetting_existing_state);
	UT_RUN(test_census_original_initializer_refuses_capacity_and_existing_header);
	UT_RUN(test_census_original_initializer_then_complete_walk_continues_native_wal);
	UT_RUN(test_census_full_ready_rejects_partial_layout);
	UT_RUN(test_census_original_capacity_rejects_overflow_and_preserves_publish_boundary);
	UT_RUN(test_census_original_ready_accepts_complete_allocated_layout);
	UT_RUN(test_census_original_ready_rejects_header_and_runtime_drift_before_scan);
	UT_RUN(test_census_complete_runtime_layout_reaches_original_native_wal);
	UT_RUN(test_census_runtime_attachment_identity);
	UT_RUN(test_census_bad_attachment_invalidates_original_worker_pass);
	UT_RUN(test_census_attachment_drift_refuses_native_writer_continuation);
	UT_RUN(test_census_bad_attachment_keeps_original_registration_producer);
	UT_RUN(test_xmin_original_census_step_rejects_registration_drift);
	UT_RUN(test_xmin_original_census_rows_continue_native_wal);
	UT_RUN(test_xmin_original_census_busy_and_slice_continuations_keep_original_cut);
	UT_RUN(test_xmin_original_census_malformed_rows_refuse_before_native_wal);
	UT_RUN(test_xmin_original_census_unqualified_rows_remain_observations);
	UT_RUN(test_xmin_original_census_preflight_and_final_cut_refusals);
	UT_RUN(test_xmin_original_census_begin_refuses_invalid_reference);
	UT_RUN(test_xmin_original_census_begin_continues_native_wal);
	UT_RUN(test_xmin_original_census_begin_refuses_unavailable_header);
	UT_RUN(test_xmin_original_census_begin_count_boundary_is_api_only);
	UT_RUN(test_xmin_original_census_begin_retains_registration_across_continuation);
	UT_RUN(test_xmin_original_stripe_future_refuses_exhausted_lease);
	UT_RUN(test_xmin_original_stripe_future_continues_native_wal);
	UT_RUN(test_xmin_original_stripe_future_refuses_unavailable_inputs);
	UT_RUN(test_xmin_original_stripe_future_rounding_and_wrap_are_readonly);
	UT_RUN(test_xmin_original_stripe_future_rechecks_after_snapshot_unlock);
	UT_RUN(test_xmin_original_native_sampler_continues_worker_wal);
	UT_RUN(test_xmin_original_native_sampler_refuses_unknown_identity_and_window);
	UT_RUN(test_xmin_original_native_sampler_uses_compact_membership);
	UT_RUN(test_xmin_original_native_sampler_retains_xidgen_snapshot);
	UT_RUN(test_xmin_original_native_sampler_fullxid_fold_and_atomic_refusal);
	UT_RUN(test_xmin_original_worker_pass_continues_native_wal);
	UT_RUN(test_xmin_original_worker_pass_capture_refusals_do_not_publish);
	UT_RUN(test_xmin_original_worker_pass_continuation_does_not_resample);
	UT_RUN(test_xmin_original_worker_pass_rechecks_source_after_slice);
	UT_RUN(test_xmin_original_worker_pass_registration_drift_stays_unproven);
	UT_RUN(test_xmin_original_worker_pass_new_round_revokes_old_cohort);
	UT_RUN(test_xmin_original_worker_publisher_getter_continues_native_wal);
	UT_RUN(test_original_worker_publisher_refuses_malformed_completed_cursor);
	UT_RUN(test_xmin_original_worker_publisher_incomplete_preserves_last_image);
	UT_RUN(test_xmin_original_worker_publisher_invalid_clears_before_native_wal);
	UT_RUN(test_xmin_original_worker_publisher_nonowner_preserves_image);
	UT_RUN(test_xmin_original_worker_publisher_sequence_exhaustion_does_not_wrap);
	UT_RUN(test_xmin_original_worker_publisher_unlock_revocation_blocks_native_wal);
	UT_RUN(test_xmin_original_worker_getter_continues_native_wal);
	UT_RUN(test_xmin_original_worker_getter_returns_shared_image_and_clears_refusal);
	UT_RUN(test_xmin_original_worker_getter_rechecks_drift_after_copy_unlock);
	UT_RUN(test_xmin_original_worker_getter_error_follows_copy_unlock);
	UT_RUN(test_xmin_original_worker_getter_new_sample_does_not_refresh_old_cohort);

	UT_RUN(test_xmin_original_retained_lease_continues_native_wal);
	UT_RUN(test_xmin_original_retained_lease_refuses_state_drift_at_policy_and_worker);
	UT_RUN(test_xmin_original_retained_lease_prerequisites_refuse_before_lock);
	UT_RUN(test_xmin_original_retained_lease_rechecks_incarnation_quorum_after_unlock);
	UT_RUN(test_xmin_original_retained_lease_observer_error_preserves_caller_resources);
	UT_RUN(test_xmin_original_retained_lease_allows_same_identity_renewal);

	UT_RUN(test_xmin_original_admission_capture_current_continues_native_wal);
	UT_RUN(test_xmin_original_admission_refuses_changed_publication_or_identity);
	UT_RUN(test_xmin_original_admission_lifecycle_prerequisites_refuse_before_writer);
	UT_RUN(test_xmin_original_admission_rechecks_after_membership_callback);
	UT_RUN(test_xmin_original_admission_membership_error_preserves_caller_resources);
	UT_RUN(test_xmin_original_admission_excludes_heartbeat_generation_from_identity);

	UT_RUN(test_xmin_original_worker_publication_continues_native_wal);
	UT_RUN(test_xmin_original_worker_publication_refuses_stale_shared_image);
	UT_RUN(test_xmin_original_worker_fence_refusals_precede_publication_lock);
	UT_RUN(test_xmin_original_worker_fence_error_preserves_caller_resources);
	UT_RUN(test_xmin_original_worker_unlock_rechecks_actual_registration_cut);

	UT_RUN(test_xmin_original_peer_backend_continues_native_wal);
	UT_RUN(test_xmin_original_peer_sequence_generation_and_h_refuse_before_resume);
	UT_RUN(test_xmin_original_cohort_rechecks_local_source_around_end_clock);
	UT_RUN(test_xmin_original_cohort_clock_rejects_stale_or_rollback);
	UT_RUN(test_xmin_original_cohort_clock_error_preserves_caller_resources);
	UT_RUN(test_xmin_original_cohort_entry_refuses_unknown_saved_metadata);
	UT_RUN(test_xmin_original_cohort_entry_rejects_missing_or_replaced_worker);
	UT_RUN(test_xmin_original_cohort_final_cut_follows_backend_observation);
	UT_RUN(test_xmin_original_cohort_current_reaches_same_native_wal);
	UT_RUN(test_xmin_original_worker_read_error_preserves_caller_resources);
	UT_RUN(test_xmin_original_policy_current_uses_retained_allocation_fence);
	UT_RUN(test_xmin_original_policy_current_rejects_disabled_striping_before_fence);
	UT_RUN(test_xmin_original_allocation_fence_error_preserves_caller_resources);
	UT_RUN(test_xmin_unlocked_source_refusal_precedes_original_writer_resume);
	UT_RUN(test_xmin_unlocked_current_checks_keep_saved_inputs_and_native_wal);
	UT_RUN(test_xmin_unlocked_source_error_keeps_caller_pin_and_admission);
	UT_RUN(test_xmin_original_census_recheck_follows_unlocked_source_callbacks);
	UT_RUN(test_xmin_borrowed_content_lock_never_calls_unlocked_source_getters);
	UT_RUN(test_xmin_native_horizon_refuses_creator_outside_legal_window);
	UT_RUN(test_xmin_native_horizon_keeps_strictly_inside_legal_window);
	UT_RUN(test_xmin_native_horizon_rechecks_legal_window_after_final_target);
	UT_RUN(test_xmin_capture_refusal_precedes_writer_resume);
	UT_RUN(test_xmin_apply_requires_same_bound_sources);
	UT_RUN(test_xmin_original_h_source_refusal_precedes_writer_use);
	UT_RUN(test_xmin_registration_cut_refusal_precedes_writer_use);
	UT_RUN(test_xmin_actual_ingress_attachment_and_saturation_refuse_before_wal);
	UT_RUN(test_xmin_registration_cut_rechecked_after_final_callbacks);
	UT_RUN(test_xmin_registration_cut_rechecked_after_actual_space_prepare);
	UT_RUN(test_xmin_same_bounds_rechecked_after_actual_space_prepare);
	UT_RUN(test_xmin_native_window_retained_through_actual_space_prepare);
	UT_RUN(test_xmin_rechecks_same_bound_sources_after_final_target);
	UT_RUN(test_recapture_retires_previous_bound_sources);
	UT_RUN(test_xmin_space_refusal_is_conditional);
	UT_RUN(test_xmin_space_error_still_propagates);
	UT_RUN(test_original_freeze_space_refusal_still_errors);
	UT_RUN(test_xmin_rejects_unknown_generation_or_foreign_origin_before_writer);
	UT_RUN(test_xmin_locked_kernel_refuses_foreign_proof_before_native_wal);
	UT_RUN(test_xmin_foreign_object_refuses_before_writer_or_native_wal);
	UT_RUN(test_xmin_mixed_page_freezes_only_exact_selected_carriers);
	UT_RUN(test_xmin_mixed_page_refuses_missing_selected_or_invalid_supplied_proof);
	UT_RUN(test_xmin_actual_writer_keeps_original_ko_revision_before_lock);
	UT_RUN(test_xmin_actual_resumer_inner_error_cleans_before_native_wrapper);
	UT_RUN(test_xmin_released_writer_requires_actual_original_node_grant);
	UT_RUN(test_xmin_released_writer_single_and_batch_reach_native_wal);
	UT_RUN(test_xmin_released_writer_refusal_never_fetches_or_writes);
	UT_RUN(test_xmin_plan_refusal_before_resume_keeps_original_pin);
	UT_RUN(test_xmin_missing_proof_refuses_before_writer_resume);
	UT_RUN(test_xmin_released_writer_requires_target_and_census_before_resume);
	UT_RUN(test_xmin_released_writer_rejects_missing_or_different_original_token);
	UT_RUN(test_xmin_unlocked_target_error_never_restores_writer);
	UT_RUN(test_xmin_successful_preflight_still_requires_final_target);
	UT_RUN(test_xmin_successful_preflight_still_requires_final_census);
	UT_RUN(test_xmin_resumed_writer_target_error_runs_finally);
	UT_RUN(test_xmin_resumed_writer_flush_error_runs_finally_after_wal);
	UT_RUN(test_xmin_resume_error_keeps_original_pin_and_admission);
	UT_RUN(test_xmin_existing_local_x_never_reacquires_or_releases_it);
	UT_RUN(test_xmin_existing_share_lock_is_not_a_resume_scope);
	UT_RUN(test_xmin_single_batch_keep_original_writer_object_fence);
	UT_RUN(test_page_recapture_drops_original_writer_context);
	UT_RUN(test_xmin_batch_rejects_replacement_original_writer_before_wal);
	UT_RUN(test_xmin_apply_requires_original_writer_context_before_wal);
	UT_RUN(test_xmin_apply_rejects_replacement_t3_with_same_buffer_generation);
	UT_RUN(test_xmin_single_and_batch_require_full_target_not_census_only);
	UT_RUN(test_xmin_target_admission_keeps_original_census_recheck_last);
	UT_RUN(test_xmin_full_target_error_keeps_borrowed_resources_before_wal);
	UT_RUN(test_xmin_bad_final_authority_never_checks_full_target);
	UT_RUN(test_prune_native_plan_and_version);
	UT_RUN(test_shared_freeze_refuses_before_tuple_or_wal);
	UT_RUN(test_vacuum_native_unused_and_version);
	UT_RUN(test_nonshared_native_maintenance);
	UT_RUN(test_prune_hint_only_versions_actual_change);
	UT_RUN(test_prune_hint_nonshared_keeps_native_behavior);
	UT_RUN(test_prune_hint_no_change_has_no_version_or_wal);
	UT_RUN(test_prune_hint_missing_identity_refuses_before_mutation);
	UT_RUN(test_invalid_identity_refuses_before_mutation);
	UT_RUN(test_freeze_preserves_native_transaction_sanity_gate);
	UT_RUN(test_qualified_freeze_kernel_changes_only_planned_headers);
	UT_RUN(test_exact_xmin_plan_preserves_xmax_through_native_wal_and_redo);
	UT_RUN(test_qualified_xmin_apply_wal_flush_and_redo);
	UT_RUN(test_xmin_apply_requires_original_backend_pin_before_page_or_writer_use);
	UT_RUN(test_xmin_batch_requires_original_backend_pin_without_repin_or_write);
	UT_RUN(test_qualified_xmin_rejects_missing_or_changed_ownership);
	UT_RUN(test_qualified_xmin_rejects_stale_physical_row_and_bounds);
	UT_RUN(test_qualified_xmin_flush_error_propagates_after_native_write);
	UT_RUN(test_freeze_native_wal_dedup_and_redo);
	UT_RUN(test_freeze_kernel_rejects_invalid_space_before_write);
	UT_RUN(test_vacuum_native_entry_copies_prelock_identity);
	UT_RUN(test_vacuum_native_entry_refuses_missing_identity);
	UT_RUN(test_explicit_shared_freeze_requests_before_content_locks);
	UT_RUN(test_ordinary_or_nonshared_vacuum_does_not_request);
	UT_RUN(test_deferred_request_preserves_shared_freeze_restriction);
	UT_RUN(test_qualified_xmin_rejects_recapture_with_old_proof_version);
	UT_RUN(test_xmin_apply_rejects_unbounded_cutoffs_before_native_write);
	UT_RUN(test_xmin_batch_rejects_unbounded_cutoffs_before_native_write);
	UT_RUN(test_xmin_apply_rejects_unbound_original_identity_before_wal);
	UT_RUN(test_xmin_batch_later_origin_epoch_mismatch_is_atomic);
	UT_RUN(test_xmin_batch_one_wal_fence_fullxid_horizon_and_redo);
	UT_RUN(test_xmin_batch_later_unqualified_target_changes_nothing);
	UT_RUN(test_xmin_batch_rejects_duplicate_physical_rows);
	UT_RUN(test_xmin_batch_runtime_bounds_and_empty_reject);
	UT_RUN(test_xmin_batch_later_stale_row_rejects_before_fence);
	UT_RUN(test_xmin_batch_rejects_ambiguous_native_horizon);
	UT_RUN(test_xmin_batch_final_authority_refusal_changes_nothing);
	UT_RUN(test_xmin_single_and_batch_reject_unknown_or_behind_wal_clock);
	UT_RUN(test_xmin_single_and_batch_wal_clock_accept_time_equal_or_ahead);
	UT_RUN(test_xmin_batch_flush_error_keeps_borrowed_resources);
	UT_RUN(test_xmin_final_admission_refusal_after_actual_space_is_before_wal);
	UT_RUN(test_xmin_final_admission_error_after_actual_space_unwinds_only_new_writer);
	UT_RUN(test_xmin_final_admission_callbacks_precede_last_source_match);
	UT_RUN(test_real_outer_collector_enters_and_leaves_around_actual_admitted_phase);
	UT_RUN(test_real_outer_collector_early_refusals_preserve_caller_pin_and_other_debt);
	UT_RUN(test_real_outer_collector_rethrows_after_original_leave_on_admitted_errors);

	UT_RUN(test_real_outer_collector_result_cannot_outlive_its_original_admission);
	UT_RUN(test_real_outer_collector_reuse_refusal_retires_previous_observations);

	UT_RUN(test_original_resident_recheck_unpins_before_real_outer_admission_leave);
	UT_RUN(test_original_resident_refusals_release_only_their_pin_before_outer_leave);
	UT_RUN(test_original_resident_errors_unpin_before_real_outer_error_propagation);
	UT_RUN(test_original_recent_first_pin_and_owner_release_precede_outer_leave);
	UT_RUN(test_original_recent_descriptor_misses_never_publish_pin_or_owner);
	UT_RUN(test_original_recent_owner_unpins_before_collector_error_propagation);
	UT_RUN(test_original_resident_recent_pin_chain_reaches_same_owner_native_wal);
	UT_RUN(test_original_resident_recent_native_refuses_read_context_and_late_cut);
	UT_RUN(test_original_resident_recent_native_errors_unwind_two_pin_owner_scopes);
	UT_RUN(test_resident_recent_native_original_startup_qualification_at_each_cut);
	UT_RUN(test_resident_recent_native_original_publication_identity_at_each_cut);
	UT_RUN(test_resident_recent_native_original_admission_errors_retire_owned_debt_last);
	UT_RUN(test_original_group_exit_retains_source_through_recent_wal);
	UT_RUN(test_original_group_exit_late_receiver_revokes_second_pin_wal);
	UT_RUN(test_original_group_exit_certificate_precedes_fresh_recent_cut);
	UT_RUN(test_original_group_member_exit_retains_source_through_recent_wal);
	UT_RUN(test_original_group_member_exit_late_receiver_revokes_recent_wal);
	UT_RUN(test_original_group_member_exit_certificate_needs_fresh_recent_cut);

	UT_RUN(test_recent_xmin_saved_bounds_refuse_before_pin);

	UT_RUN(test_original_snapshot_h_survives_group_exit_to_recent_wal);
	UT_RUN(test_real_outer_collector_retires_continuation_before_admission_leave);
	UT_RUN(test_real_outer_collector_errors_retire_continuation_before_admission_leave);
	UT_RUN(test_admitted_reuse_refusals_retire_previous_continuation);
	UT_RUN(test_mixed_admitted_resident_subset_reaches_same_owner_recent_wal);

	UT_DONE();
	return ut_failed_count != 0;
}
