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
#include "cluster/cluster_conf.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_smgr.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/standby.h"
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
static bool flush_error;
static bool space_buffer_permanent, space_permanence_error;
static uint8 writer_state;
static uint64 route_holder, route_master;
static unsigned flushes;
static unsigned writer_checks, route_checks, authority_checks, admission_checks;
static SCN fixture_wal_clock, inserted_wal_clock;
static unsigned wal_clock_reads;
static bool full_target_current, target_recheck_error, revoke_target_census;
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

/* Compile the original ingress producer and current-cut predicate together
 * with the native apply bodies. Only the shared-header storage projection,
 * runtime attachment and concurrent callback schedule remain fixture leaves. */
static struct {
	pg_atomic_uint64 unqualified_registration_generation;
} registration_shared, *CtrcShared = &registration_shared;
static bool registration_attached;

static bool
ctrc_runtime_attached(void)
{
	registration_checks++;
	UT_ASSERT_EQ(CritSectionCount, 0);
	return registration_attached && CtrcShared != NULL;
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
	UT_ASSERT(buf == GetBufferDescriptor(0) && !local_x && backend_pin_count == 1);
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
	UT_ASSERT(backend_pin_count == 1 && !local_x && !local_share);
	local_lock_attempts++; local_x = resume_allowed;
	if (error_after_local_lock)
		ereport(ERROR, (errmsg("post-local-lock ERROR fixture")));
	return local_x;
}

void
LWLockRelease(LWLock *lock)
{
	UT_ASSERT(lock == BufferDescriptorGetContentLock(GetBufferDescriptor(0)));
	UT_ASSERT(local_x && backend_pin_count == 1);
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
	UT_ASSERT(!local_x && backend_pin_count == 1);
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
		UT_ASSERT(token != NULL && token->entered && backend_pin_count == 1);
		UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
		return admission_current;
	}
	if (space_observation_checks != 0) {
		post_space_census_checks++;
		UT_ASSERT(token != NULL && token->entered && local_x && backend_pin_count == 1);
		UT_ASSERT_EQ(post_space_target_checks, 1);
		UT_ASSERT_EQ(CritSectionCount, 0);
		UT_ASSERT_EQ(inserts + dirties + edges + flushes, 0);
		if (post_space_census_error) {
			post_space_error_seen = 2;
			elog(ERROR, "injected final census ERROR after SPACE");
		}
		return admission_current;
	}
	admission_checks++;
	UT_ASSERT(token != NULL && token->entered);
	return admission_current;
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
		.lease_limit_full = U64FromFullTransactionId(policy->legal_next), .slot = 0 };
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
	policy->allocation_fence = retained_lease_fence;
}

bool
cluster_xid_stripe_sample_fence_current(const ClusterXidStripeSampleFence *fence)
{
	UT_ASSERT(!local_x && !local_share && backend_pin_count == 1);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
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
	UT_ASSERT(!local_x && !local_share && backend_pin_count == 1);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
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

bool
cluster_undo_cleaner_active_sample(ClusterUndoCleanerActiveSample *out)
{
	UT_ASSERT(!local_x && !local_share && backend_pin_count == 1);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
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
	UT_ASSERT(!local_x && !local_share && backend_pin_count == 1);
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
	UT_ASSERT(!local_x && !local_share && backend_pin_count == 1);
	cohort_semantic_current_checks++;
	return cohort_admission_live && original_admission_source_current(fence);
}

static bool
cohort_peer_generation_current(int32 peer, uint32 capability, uint32 generation)
{
	UT_ASSERT(peer >= 0 && peer < CLUSTER_MAX_NODES);
	UT_ASSERT_EQ(capability, PGRAC_IC_HELLO_CAP_HEAP_FREEZE_REPORT_V2);
	UT_ASSERT(!local_x && !local_share && backend_pin_count == 1);
	cohort_peer_current_checks++;
	if (cohort_peer_revoke_worker) cohort_worker_live = false;
	return generation == cohort_live_generation[peer];
}

static TimestampTz
cohort_current_end_clock(void)
{
	UT_ASSERT(!local_x && !local_share && backend_pin_count == 1);
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
	UT_ASSERT(!local_x && !local_share && backend_pin_count == 1);
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
	UT_ASSERT(!local_x && !local_share && backend_pin_count == 1);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
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
		UT_ASSERT(token != NULL && token->entered && backend_pin_count == 1);
		UT_ASSERT_EQ(CritSectionCount, 0);
		UT_ASSERT_EQ(conditional_resumes + conditional_unlocks + local_lock_attempts, 0);
		UT_ASSERT_EQ(writer_checks + route_checks + authority_checks + admission_checks, 0);
		UT_ASSERT_EQ(wal_clock_reads + inserts + dirties + edges + flushes, 0);
		if (preflight_target_error) elog(ERROR, "injected unlocked TARGET preflight error");
		if (preflight_revoke_census) admission_current = false;
		return full_target_current || allow_preflight_target;
	}
	if (space_observation_checks != 0) {
		post_space_target_checks++;
		UT_ASSERT(token != NULL && token->entered && local_x && backend_pin_count == 1);
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
		return full_target_current;
	}
	full_target_checks++;
	if (advance_registration_after_target) ctrc_note_unqualified_registration();
	UT_ASSERT(token != NULL && token->entered);
	UT_ASSERT(local_x && backend_pin_count == 1);
	UT_ASSERT_EQ(CritSectionCount, 0);
	/* Native final authority + real capture; a resumed scope checked once more. */
	UT_ASSERT_EQ(writer_checks, resumed_writer ? 3 : 2);
	UT_ASSERT_EQ(route_checks, 1);
	UT_ASSERT_EQ(authority_checks, 1);
	UT_ASSERT_EQ(admission_checks, 0);
	UT_ASSERT_EQ(wal_clock_reads + inserts + dirties + edges + flushes, 0);
	if (target_recheck_error) elog(ERROR, "injected full TARGET recheck error");
	if (revoke_target_census) admission_current = false;
	if (retire_bounds_after_target != NULL)
		retire_bounds_after_target->horizon_scn++;
	if (advance_legal_next_after_target != NULL)
		advance_legal_next_after_target->legal_next = FullTransactionIdFromU64(
			U64FromFullTransactionId(advance_legal_next_after_target->legal_next) + 1);
	return full_target_current;
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
	f->cohort.local_registration_cut.unqualified_registration_generation = pg_atomic_read_u64(&CtrcShared->unqualified_registration_generation);
	admission_current_publish_fixture(&f->cohort);
	worker_current_publish_fixture(&f->cohort);
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

int
main(void)
{
	UT_PLAN(131);
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
	UT_DONE();
	return ut_failed_count != 0;
}
