/*-------------------------------------------------------------------------
 * test_cluster_authority_storage.c
 *    Storage publication interleavings at the real authority and CF entry.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * The original startup fixture supplies the other service boundaries. Storage
 * publication/current checks, authority lifecycle, and CF S1 are product code.
 * This does not substitute for the four-member startup/stop/restart tests.
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_authority_storage.c
 *
 * NOTES
 *    PGRAC-original integration fixture for the original startup owner.
 *-------------------------------------------------------------------------
 */
#define main startup_phase_fixture_main
#define cluster_qvotec_in_quorum fixture_disk_quorum_current
#define cluster_qvotec_check_admission fixture_quorum_check_admission
#define pg_usleep fixture_startup_usleep
#include "test_cluster_startup_phase.c"
#undef pg_usleep
#undef cluster_qvotec_check_admission
#undef cluster_qvotec_in_quorum
#undef main

void pg_usleep(long microsec);

#ifndef STORAGE_QUORUM_SOURCE_PATH
#error "STORAGE_QUORUM_SOURCE_PATH must identify the production storage predicate"
#endif
#include STORAGE_QUORUM_SOURCE_PATH

static ClusterStorageQuorumState authority_storage;
static ClusterStorageQuorumView authority_provider;
static ClusterLockAcquireRequest authority_cf;
static PGPROC authority_checkpointer;
static bool restore_publication_after_false;
typedef enum AuthorityBusyStage {
	AUTHORITY_BUSY_NONE,
	AUTHORITY_BUSY_BEGIN,
	AUTHORITY_BUSY_BIND,
	AUTHORITY_BUSY_PUBLISH
} AuthorityBusyStage;
static AuthorityBusyStage authority_busy_stage;
static bool authority_busy_started;
static bool authority_busy_persistent;
static int authority_pending_sleeps;
static bool authority_pending_identity_lost;
static ClusterStorageSnapshotStop authority_forced_snapshot_stop;
static bool authority_continuity_invalid;
static bool authority_continuity_pending;
static unsigned authority_admission_calls;

void
pg_usleep(long microsec)
{
	fixture_startup_usleep(microsec);
	if (authority_busy_started && (pg_atomic_read_u32(&authority_storage.sequence) & 1) != 0) {
		authority_pending_sleeps++;
		if (authority_busy_stage != AUTHORITY_BUSY_BEGIN
			&& cluster_authority_readiness_get() != CLUSTER_AUTHORITY_STARTING)
			authority_pending_identity_lost = true;
		if (!authority_busy_persistent && authority_pending_sleeps == 3)
			pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
	}
}

bool cluster_qvotec_in_quorum(void);
bool cluster_qvotec_check_admission(ClusterQvotecAdmissionCheck *out);

bool
cluster_qvotec_check_admission(ClusterQvotecAdmissionCheck *out)
{
	bool allowed;

	authority_admission_calls++;
	memset(out, 0, sizeof(*out));
	if (!authority_busy_started && authority_busy_stage != AUTHORITY_BUSY_NONE
		&& phase_test_recovery_control_formation_calls > 0
		&& ((authority_busy_stage == AUTHORITY_BUSY_BEGIN
			 && cluster_authority_readiness_get() == CLUSTER_AUTHORITY_OFF)
			|| (authority_busy_stage == AUTHORITY_BUSY_BIND
				&& cluster_authority_readiness_get() == CLUSTER_AUTHORITY_STARTING)
			|| (authority_busy_stage == AUTHORITY_BUSY_PUBLISH
				&& phase_test_grd_barrier_calls > 0))) {
		authority_busy_started = true;
		pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
	}
	if (!fixture_disk_quorum_current()) {
		out->result = CLUSTER_QVOTEC_ADMISSION_DB_STATE;
		return false;
	}
	allowed = cluster_storage_quorum_check_node(cluster_node_id, &out->storage);
	/* The storage unit covers real clock failure injection. Here carry that
	 * exact classified result across the authority consumer boundary. */
	if (!allowed && out->storage.result == CLUSTER_STORAGE_CHECK_UNSTABLE
		&& authority_forced_snapshot_stop != CLUSTER_STORAGE_SNAPSHOT_COMPLETE)
		out->storage.snapshot_stop = authority_forced_snapshot_stop;
	out->result = allowed ? CLUSTER_QVOTEC_ADMISSION_ALLOWED : CLUSTER_QVOTEC_ADMISSION_STORAGE;
	if (allowed) {
		out->continuity.quorum_generation = 1;
		out->continuity.storage_generation = out->storage.view.loss_generation;
		out->continuity_valid = out->continuity.storage_generation != 0
								&& out->continuity.storage_generation != UINT64_MAX
								&& !authority_continuity_invalid;
		out->continuity_pending = authority_continuity_pending;
	}

	/* Finish the producer between the first false and the consumer's next
	 * check; that later READY must not reclassify the earlier observation. */
	if (!allowed && restore_publication_after_false) {
		restore_publication_after_false = false;
		pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
	}
	return allowed;
}

bool
cluster_qvotec_in_quorum(void)
{
	ClusterQvotecAdmissionCheck check;

	return cluster_qvotec_check_admission(&check);
}

void
cluster_storage_corosync_sample(ClusterStorageQuorumView *out)
{
	*out = authority_provider;
}

static void
authority_storage_prepare(void)
{
	IsUnderPostmaster = false;
	MyProc = NULL;
	restore_publication_after_false = false;
	authority_busy_stage = AUTHORITY_BUSY_NONE;
	authority_busy_started = false;
	authority_busy_persistent = false;
	authority_pending_sleeps = 0;
	authority_pending_identity_lost = false;
	authority_forced_snapshot_stop = CLUSTER_STORAGE_SNAPSHOT_COMPLETE;
	authority_continuity_invalid = false;
	authority_continuity_pending = false;
	phase_test_cssd_status_busy = false;
	reset_phase_service_fixture(true);
	cluster_shared_config = true;
	test_mount_result = CLUSTER_CONFIG_MOUNT_MATCH;
	memset(&authority_provider, 0, sizeof(authority_provider));
	authority_provider.reason = CLUSTER_STORAGE_QUORUM_READY;
	authority_provider.ring_node = 11;
	authority_provider.ring_sequence = 8;
	authority_provider.members[0] = 15;
	cluster_storage_quorum_attach(&authority_storage, true);
	cluster_storage_quorum_refresh(cluster_storage_quorum_now_us(), UINT64_C(60000000));
}

static void
authority_storage_setup(bool serving)
{
	authority_storage_prepare();
	cluster_run_startup_sequence();
	if (serving)
		cluster_run_phase4_sequence();
	UT_ASSERT_EQ(cluster_authority_readiness_get(),
				 serving ? CLUSTER_AUTHORITY_SERVING_READY : CLUSTER_AUTHORITY_RECOVERY_READY);
	phase_test_control_acquire_ready = true;
	IsUnderPostmaster = true;
	MyBackendType = B_CHECKPOINTER;
	MyAuxProcType = CheckpointerProcess;
	MyProc = &authority_checkpointer;
	memset(&authority_cf, 0, sizeof(authority_cf));
	authority_cf.resid.type = CLUSTER_CF_RESID_TYPE;
	authority_cf.resid.lockmethodid = DEFAULT_LOCKMETHOD;
	authority_cf.lockmode = ShareLock;
}

UT_TEST(resource_x_same_sample_never_crosses_the_original_serving_loss_cut)
{
	ClusterQvotecAdmissionCheck check;
	bool pending = true;

	authority_storage_setup(true);
	UT_ASSERT(cluster_qvotec_check_admission(&check));
	UT_ASSERT(cluster_authority_serving_admission_current_v1(&check, &pending));
	UT_ASSERT(!pending);
	/* The writer publishes real loss and then READY between callers. No
	 * intervening serving consumer clears the old baseline for this test. */
	authority_provider.reason = CLUSTER_STORAGE_QUORUM_NOT_QUORATE;
	cluster_storage_quorum_refresh(cluster_storage_quorum_now_us(), UINT64_C(60000000));
	authority_provider.reason = CLUSTER_STORAGE_QUORUM_READY;
	cluster_storage_quorum_refresh(cluster_storage_quorum_now_us(), UINT64_C(60000000));
	UT_ASSERT(cluster_qvotec_check_admission(&check));
	UT_ASSERT(check.continuity_valid);
	UT_ASSERT(!cluster_authority_serving_admission_current_v1(&check, &pending));
	UT_ASSERT(!pending);
}

UT_TEST(resource_x_pending_requires_the_same_serving_identity)
{
	ClusterQvotecAdmissionCheck check;
	bool pending;

	for (int changed = 0; changed < 3; changed++) {
		authority_storage_setup(true);
		pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
		UT_ASSERT(!cluster_qvotec_check_admission(&check));
		UT_ASSERT(!cluster_authority_serving_admission_current_v1(&check, &pending));
		UT_ASSERT(pending);
		if (changed == 0)
			phase_test_self_incarnation++;
		else if (changed == 1)
			phase_test_lms_generation++;
		else
			phase_test_formation_epoch++;
		UT_ASSERT(!cluster_authority_serving_admission_current_v1(&check, &pending));
		UT_ASSERT(!pending);
	}
}

UT_TEST(resource_x_continuity_never_blocks_or_hides_a_known_refusal)
{
	ClusterQvotecAdmissionCheck check;
	bool pending;
	int blocking;

	authority_storage_setup(true);
	UT_ASSERT(cluster_qvotec_check_admission(&check));
	phase_lwlock_conditional_result = false;
	blocking = phase_lwlock_blocking_calls;
	UT_ASSERT(!cluster_authority_serving_admission_current_v1(&check, &pending));
	UT_ASSERT(pending);
	check.result = CLUSTER_QVOTEC_ADMISSION_LEASE;
	UT_ASSERT(!cluster_authority_serving_admission_current_v1(&check, &pending));
	UT_ASSERT(!pending);
	UT_ASSERT_EQ(phase_lwlock_blocking_calls, blocking);
	phase_lwlock_conditional_result = true;
	UT_ASSERT(cluster_qvotec_check_admission(&check));
	UT_ASSERT(cluster_authority_serving_admission_current_v1(&check, &pending));
	cluster_authority_readiness_clear();
	UT_ASSERT(!cluster_authority_serving_admission_current_v1(&check, &pending));
	UT_ASSERT(!pending);
}

UT_TEST(storage_publication_busy_preserves_serving_identity_for_retry)
{
	authority_storage_setup(true);
	UT_ASSERT_EQ(cluster_lock_acquire_s1_entry(&authority_cf), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
	UT_ASSERT_EQ(cluster_lock_acquire_s1_entry(&authority_cf),
				 CLUSTER_LOCK_ACQUIRE_FAIL_LMS_UNAVAILABLE);
	UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_SERVING_READY);
	pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
	UT_ASSERT_EQ(cluster_lock_acquire_s1_entry(&authority_cf), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
}

UT_TEST(restored_second_sample_cannot_destroy_a_busy_first_binding)
{
	authority_storage_setup(true);
	pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
	restore_publication_after_false = true;
	UT_ASSERT(!cluster_serving_ready_is_current());
	UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_SERVING_READY);
	UT_ASSERT(cluster_serving_ready_is_current());
}

UT_TEST(storage_publication_busy_preserves_recovery_identity)
{
	authority_storage_setup(false);
	pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
	UT_ASSERT(!cluster_recovery_authority_is_current());
	UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_RECOVERY_READY);
	pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
	UT_ASSERT(cluster_recovery_authority_is_current());
}

UT_TEST(serving_publication_busy_retries_the_same_recovery_binding)
{
	authority_storage_setup(false);
	IsUnderPostmaster = false;
	MyBackendType = B_INVALID;
	MyAuxProcType = NotAnAuxProcess;
	MyProc = NULL;
	cluster_advance_phase(CLUSTER_PHASE_4_NORMAL);
	pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
	UT_ASSERT(!cluster_authority_readiness_publish_serving());
	UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_RECOVERY_READY);
	pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
	UT_ASSERT(cluster_authority_readiness_publish_serving());
}

UT_TEST(stable_negative_and_expiry_are_terminal)
{
	int variant;

	for (variant = 0; variant < 3; variant++) {
		authority_storage_setup(true);
		if (variant == 2)
			pg_atomic_write_u64(&authority_storage.expires_us, 1);
		else {
			authority_provider.reason = variant == 0 ? CLUSTER_STORAGE_QUORUM_NOT_QUORATE
													 : CLUSTER_STORAGE_QUORUM_CONFIGURATION;
			cluster_storage_quorum_refresh(cluster_storage_quorum_now_us(), UINT64_C(60000000));
		}
		UT_ASSERT(!cluster_serving_ready_is_current());
		UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_OFF);
		authority_provider.reason = CLUSTER_STORAGE_QUORUM_READY;
		cluster_storage_quorum_refresh(cluster_storage_quorum_now_us(), UINT64_C(60000000));
		UT_ASSERT(!cluster_serving_ready_is_current());
	}
}

UT_TEST(real_loss_between_readers_cannot_be_hidden_by_ready)
{
	authority_storage_setup(true);
	UT_ASSERT(cluster_serving_ready_is_current());
	authority_provider.reason = CLUSTER_STORAGE_QUORUM_NOT_QUORATE;
	cluster_storage_quorum_refresh(cluster_storage_quorum_now_us(), UINT64_C(60000000));
	authority_provider.reason = CLUSTER_STORAGE_QUORUM_READY;
	cluster_storage_quorum_refresh(cluster_storage_quorum_now_us(), UINT64_C(60000000));
	UT_ASSERT(!cluster_serving_ready_is_current());
	UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_OFF);
}

UT_TEST(identity_loss_during_busy_cannot_recover)
{
	authority_storage_setup(true);
	pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
	phase_test_lms_generation++;
	UT_ASSERT(!cluster_serving_ready_is_current());
	UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_OFF);
	pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
	phase_test_lms_generation--;
	UT_ASSERT(!cluster_serving_ready_is_current());
}

static void
authority_storage_begin_again(void)
{
	ClusterFenceAuthorityProof authority = { 0 };
	ClusterFormationSnapshotV1 formation = { 0 };
	IsUnderPostmaster = false;
	MyProc = NULL;
	MyBackendType = B_INVALID;
	MyAuxProcType = NotAnAuxProcess;
	cluster_authority_readiness_clear();
	formation.membership.membership_state[0] = CLUSTER_MEMBER_MEMBER;
	formation.membership.last_admitted_incarnation[0] = 11;
	formation.local_epoch = phase_test_formation_epoch;
	UT_ASSERT(cluster_authority_readiness_begin(1, &authority, &formation));
	UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_STARTING);
}

UT_TEST(starting_bind_busy_can_retry_original_generation)
{
	authority_storage_setup(false);
	authority_storage_begin_again();
	pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
	UT_ASSERT(!cluster_authority_readiness_bind_recovery_generation(phase_test_lms_generation));
	UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_STARTING);
	pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
	UT_ASSERT(cluster_authority_readiness_bind_recovery_generation(phase_test_lms_generation));
	UT_ASSERT(cluster_authority_readiness_publish_recovery(phase_test_lms_generation));
}

UT_TEST(starting_publish_busy_can_retry_original_generation)
{
	authority_storage_setup(false);
	authority_storage_begin_again();
	UT_ASSERT(cluster_authority_readiness_bind_recovery_generation(phase_test_lms_generation));
	pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
	UT_ASSERT(!cluster_authority_readiness_publish_recovery(phase_test_lms_generation));
	UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_STARTING);
	pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
	UT_ASSERT(cluster_authority_readiness_publish_recovery(phase_test_lms_generation));
}

UT_TEST(starting_transport_busy_preserves_the_original_binding)
{
	authority_storage_setup(false);
	authority_storage_begin_again();
	UT_ASSERT(cluster_authority_readiness_bind_recovery_generation(phase_test_lms_generation));
	pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
	UT_ASSERT(!cluster_recovery_transport_is_current());
	UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_STARTING);
	pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
	UT_ASSERT(cluster_recovery_transport_is_current());
}

static void
authority_storage_lose_and_restore(void)
{
	authority_provider.reason = CLUSTER_STORAGE_QUORUM_NOT_QUORATE;
	cluster_storage_quorum_refresh(cluster_storage_quorum_now_us(), UINT64_C(60000000));
	authority_provider.reason = CLUSTER_STORAGE_QUORUM_READY;
	cluster_storage_quorum_refresh(cluster_storage_quorum_now_us(), UINT64_C(60000000));
}

UT_TEST(recovery_loss_between_readers_cannot_be_hidden_by_ready)
{
	authority_storage_setup(false);
	authority_storage_lose_and_restore();
	UT_ASSERT(!cluster_recovery_authority_is_current());
	UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_OFF);
}

UT_TEST(serving_publication_cannot_rebind_after_an_unobserved_loss)
{
	authority_storage_setup(false);
	IsUnderPostmaster = false;
	MyProc = NULL;
	MyBackendType = B_INVALID;
	MyAuxProcType = NotAnAuxProcess;
	cluster_advance_phase(CLUSTER_PHASE_4_NORMAL);
	authority_storage_lose_and_restore();
	UT_ASSERT(!cluster_authority_readiness_publish_serving());
	UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_OFF);
}

UT_TEST(renewal_after_expiry_cannot_hide_the_continuity_break)
{
	authority_storage_setup(true);
	pg_atomic_write_u64(&authority_storage.expires_us, 1);
	/* No reader sees the gap. The real producer must still publish it. */
	cluster_storage_quorum_refresh(cluster_storage_quorum_now_us(), UINT64_C(60000000));
	UT_ASSERT(!cluster_serving_ready_is_current());
	UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_OFF);
}

UT_TEST(phase3_owner_retries_begin_bind_and_publish_without_rebinding)
{
	int stage;

	for (stage = AUTHORITY_BUSY_BEGIN; stage <= AUTHORITY_BUSY_PUBLISH; stage++) {
		authority_storage_prepare();
		authority_busy_stage = (AuthorityBusyStage)stage;
		cluster_run_startup_sequence();
		UT_ASSERT(authority_busy_started);
		UT_ASSERT_EQ(authority_pending_sleeps, 3);
		UT_ASSERT(!authority_pending_identity_lost);
		UT_ASSERT_EQ(phase_test_recovery_control_formation_calls, 1);
		UT_ASSERT_EQ(phase_test_lms_start_calls, 1);
		UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_RECOVERY_READY);
		UT_ASSERT(cluster_recovery_authority_is_current());
	}
}

UT_TEST(phase3_owner_ends_persistent_busy_at_the_original_deadline)
{
	int saved_timeout = cluster_phase3_timeout;
	int stage;

	for (stage = AUTHORITY_BUSY_BEGIN; stage <= AUTHORITY_BUSY_PUBLISH; stage++) {
		bool caught_fatal = false;

		authority_storage_prepare();
		authority_busy_stage = (AuthorityBusyStage)stage;
		authority_busy_persistent = true;
		cluster_phase3_timeout = 1;
		phase4_capture_fatal = true;
		if (setjmp(phase4_fatal_jump) == 0)
			cluster_run_startup_sequence();
		else
			caught_fatal = true;
		phase4_capture_fatal = false;
		UT_ASSERT(caught_fatal);
		UT_ASSERT(authority_busy_started);
		UT_ASSERT(authority_pending_sleeps > 1);
		UT_ASSERT(!authority_pending_identity_lost);
		UT_ASSERT(phase4_test_now >= INT64CONST(1000000));
		UT_ASSERT(phase4_test_now <= INT64CONST(1020000));
		UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_OFF);
	}
	cluster_phase3_timeout = saved_timeout;
}

UT_TEST(observed_terminal_refusal_cannot_be_forgotten_by_begin)
{
	ClusterFenceAuthorityProof authority = { 0 };
	ClusterFormationSnapshotV1 formation = { 0 };

	authority_storage_setup(false);
	phase4_test_in_quorum = false;
	UT_ASSERT(!cluster_recovery_authority_is_current());
	UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_OFF);
	/* The owner has not published a new generation yet. READY alone may
	 * not erase the refusal already seen by this immutable managed boot. */
	phase4_test_in_quorum = true;
	IsUnderPostmaster = false;
	MyProc = NULL;
	MyBackendType = B_INVALID;
	MyAuxProcType = NotAnAuxProcess;
	cluster_authority_readiness_clear();
	formation.membership.membership_state[0] = CLUSTER_MEMBER_MEMBER;
	formation.membership.last_admitted_incarnation[0] = 11;
	formation.local_epoch = phase_test_formation_epoch;
	UT_ASSERT(!cluster_authority_readiness_begin(1, &authority, &formation));
	UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_OFF);
}

UT_TEST(qualified_membership_change_waits_for_original_lmon_rebind)
{
	ClusterStorageQuorumView before, after;

	authority_storage_setup(true);
	UT_ASSERT(cluster_storage_quorum_snapshot(&before));
	authority_provider.members[0] &= ~UINT64_C(8);
	authority_provider.ring_sequence++;
	cluster_storage_quorum_refresh(cluster_storage_quorum_now_us(), UINT64_C(60000000));
	UT_ASSERT(cluster_storage_quorum_snapshot(&after));
	UT_ASSERT_EQ(after.loss_generation, before.loss_generation);
	UT_ASSERT(after.generation > before.generation);
	UT_ASSERT(!cluster_storage_quorum_allows_node(3));
	phase_test_formation_epoch++;
	phase_test_grd_authority_ok = false;
	UT_ASSERT(!cluster_serving_ready_is_current());
	UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_SERVING_READY);
	UT_ASSERT(!cluster_authority_serving_rebind_lmon());
	/* Completing the original barrier permits a new formation, but an
	 * in-progress storage publication still cannot authorize that rebind. */
	phase_test_grd_authority_ok = true;
	pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
	UT_ASSERT(!cluster_authority_serving_rebind_lmon());
	pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
	UT_ASSERT(cluster_authority_serving_rebind_lmon());
	UT_ASSERT(cluster_serving_ready_is_current());
	UT_ASSERT_EQ(cluster_lock_acquire_s1_entry(&authority_cf), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
}

UT_TEST(lmon_rebind_cannot_erase_true_storage_loss)
{
	int variant;

	for (variant = 0; variant < 3; variant++) {
		authority_storage_setup(true);
		if (variant == 0)
			authority_provider.members[0] &= ~(UINT64_C(1) << cluster_node_id);
		else if (variant == 1)
			authority_provider.reason = CLUSTER_STORAGE_QUORUM_NOT_QUORATE;
		else
			pg_atomic_write_u64(&authority_storage.expires_us, 1);
		cluster_storage_quorum_refresh(cluster_storage_quorum_now_us(), UINT64_C(60000000));
		authority_provider.members[0] = 15;
		authority_provider.reason = CLUSTER_STORAGE_QUORUM_READY;
		cluster_storage_quorum_refresh(cluster_storage_quorum_now_us(), UINT64_C(60000000));
		/* No A reader observed the loss. A completed formation cannot replace
		 * the immutable boot's original admission-continuity baseline. */
		phase_test_formation_epoch++;
		UT_ASSERT(!cluster_authority_serving_rebind_lmon());
		UT_ASSERT(!cluster_serving_ready_is_current());
		UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_OFF);
	}
}

UT_TEST(lms_and_lmon_busy_publication_preserve_serving_until_fresh_proof)
{
	for (int role = 0; role < 2; role++) {
		authority_storage_setup(true);
		MyBackendType = role == 0 ? B_LMS : B_LMON;
		pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
		UT_ASSERT(!cluster_serving_ready_is_current());
		UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_SERVING_READY);
		UT_ASSERT(!cluster_serving_ready_is_current());
		pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
		UT_ASSERT(cluster_serving_ready_is_current());
	}
}

UT_TEST(clock_failure_is_not_a_recoverable_publication_wait)
{
	for (int variant = 0; variant < 2; variant++) {
		authority_storage_setup(true);
		MyBackendType = B_LMS;
		pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
		authority_forced_snapshot_stop = variant == 0 ? CLUSTER_STORAGE_SNAPSHOT_CLOCK_UNAVAILABLE
													  : CLUSTER_STORAGE_SNAPSHOT_CLOCK_REGRESSED;
		UT_ASSERT(!cluster_serving_ready_is_current());
		UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_OFF);
		pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
		authority_forced_snapshot_stop = CLUSTER_STORAGE_SNAPSHOT_COMPLETE;
		UT_ASSERT(!cluster_serving_ready_is_current());
	}
}

UT_TEST(stable_continuity_failure_retires_serving_but_publication_overlap_does_not)
{
	for (int publication_pending = 0; publication_pending < 2; publication_pending++) {
		ClusterQvotecAdmissionCheck check;
		bool pending = true;

		authority_storage_setup(true);
		authority_continuity_invalid = true;
		authority_continuity_pending = publication_pending;
		UT_ASSERT(cluster_qvotec_check_admission(&check));
		UT_ASSERT_EQ(check.storage.result, CLUSTER_STORAGE_CHECK_ALLOWED);
		UT_ASSERT(!cluster_authority_serving_admission_current_v1(&check, &pending));
		UT_ASSERT_EQ(pending, publication_pending);
		phase_lwlock_conditional_result = false;
		UT_ASSERT(!cluster_authority_serving_admission_current_v1(&check, &pending));
		UT_ASSERT_EQ(pending, publication_pending);
		phase_lwlock_conditional_result = true;
		UT_ASSERT(!cluster_serving_ready_is_current());
		UT_ASSERT_EQ(cluster_authority_readiness_get(),
					 publication_pending ? CLUSTER_AUTHORITY_SERVING_READY : CLUSTER_AUTHORITY_OFF);
		authority_continuity_invalid = false;
		authority_continuity_pending = false;
		UT_ASSERT_EQ(cluster_serving_ready_is_current(), publication_pending);
	}
}

UT_TEST(resource_x_cssd_busy_yields_before_admission_and_never_hides_loss)
{
	ClusterQvotecAdmissionCheck check;
	bool pending = false;

	authority_storage_setup(true);
	UT_ASSERT(cluster_qvotec_check_admission(&check));
	phase_test_cssd_status_busy = true;
	UT_ASSERT(!cluster_authority_serving_admission_current_v1(&check, &pending));
	UT_ASSERT(pending);
	check.continuity_valid = false;
	UT_ASSERT(!cluster_authority_serving_admission_current_v1(&check, &pending));
	UT_ASSERT(!pending);
	check.continuity_valid = true;
	phase_test_cssd_status_busy = false;
	phase_test_cssd_status = CLUSTER_CSSD_DOWN;
	UT_ASSERT(!cluster_authority_serving_admission_current_v1(&check, &pending));
	UT_ASSERT(!pending);
	phase_test_cssd_status = CLUSTER_CSSD_READY;
	UT_ASSERT(cluster_authority_serving_admission_current_v1(&check, &pending));
	UT_ASSERT(!pending);
}

/* Only the surrounding request is a fixture: reaching true represents the
 * first slot/send action. A pending observation must return to the existing
 * exact reservation abort/rearm owner before either action is possible. */
static bool
authority_requester_gate(bool *out_retry_denied)
{
	*out_retry_denied = false;
#include "test_cluster_gcs_serving_gate.inc"
	return true;
}

static int
authority_requester_result(bool *retry)
{
	volatile int result;

	phase4_capture_fatal = true;
	if (setjmp(phase4_fatal_jump) == 0)
		result = authority_requester_gate(retry) ? 1 : 0;
	else
		result = -1;
	phase4_capture_fatal = false;
	return result;
}

UT_TEST(gcs_requester_publication_overlap_yields_without_sql_error)
{
	for (int owner = 0; owner < 2; owner++) {
		bool retry = false;

		authority_storage_setup(true);
		if (owner == 0)
			pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
		else {
			authority_continuity_invalid = true;
			authority_continuity_pending = true;
		}
		UT_ASSERT_EQ(authority_requester_result(&retry), 0);
		UT_ASSERT(retry);
		UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_SERVING_READY);
		if (owner == 0)
			pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
		else {
			authority_continuity_invalid = false;
			authority_continuity_pending = false;
		}
		UT_ASSERT_EQ(authority_requester_result(&retry), 1);
		UT_ASSERT(!retry);
	}
}

UT_TEST(gcs_requester_does_not_reinterpret_pending_with_a_later_sample)
{
	bool retry = false;

	authority_storage_setup(true);
	pg_atomic_fetch_add_u32(&authority_storage.sequence, 1);
	restore_publication_after_false = true;
	UT_ASSERT_EQ(authority_requester_result(&retry), 0);
	UT_ASSERT(retry);
	UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_SERVING_READY);
	UT_ASSERT_EQ(authority_requester_result(&retry), 1);
	UT_ASSERT(!retry);
}

UT_TEST(gcs_requester_pending_never_hides_identity_or_proven_loss)
{
	for (int variant = 0; variant < 8; variant++) {
		bool retry = false;

		authority_storage_setup(true);
		authority_continuity_invalid = true;
		authority_continuity_pending = variant != 0;
		switch (variant) {
		case 1:
			phase_test_lms_generation++;
			break;
		case 2:
			phase_test_cssd_status = CLUSTER_CSSD_DOWN;
			break;
		case 3:
			phase_test_formation_epoch++;
			break;
		case 4:
			phase_test_membership_member = false;
			break;
		case 5:
			phase_test_last_admitted_incarnation++;
			break;
		case 6:
			phase_test_grd_authority_ok = false;
			break;
		case 7:
			phase_test_self_incarnation++;
			break;
		}
		UT_ASSERT_EQ(authority_requester_result(&retry), -1);
		UT_ASSERT(!retry);
	}
}

UT_TEST(gcs_requester_wait_cannot_renew_an_expired_owner_or_rebind_a_loss)
{
	bool retry = false;

	authority_storage_setup(true);
	authority_continuity_invalid = true;
	authority_continuity_pending = true;
	for (int i = 0; i < 3; i++) {
		UT_ASSERT_EQ(authority_requester_result(&retry), 0);
		UT_ASSERT(retry);
	}
	/* The original admission owner's expiry/refusal wins even when its
	 * publication remains busy. Waiting never renews that owner's lease. */
	phase4_test_in_quorum = false;
	UT_ASSERT_EQ(authority_requester_result(&retry), -1);
	UT_ASSERT(!retry);
	UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_OFF);
	phase4_test_in_quorum = true;
	authority_continuity_invalid = false;
	authority_continuity_pending = false;
	UT_ASSERT_EQ(authority_requester_result(&retry), -1);
	UT_ASSERT(!retry);
}

UT_TEST(gcs_requester_failure_diagnostic_uses_the_original_predicate)
{
	bool pending;
	const char *predicate;

	authority_storage_setup(true);
	authority_continuity_invalid = true;
	authority_continuity_pending = true;
	UT_ASSERT(!cluster_serving_ready_check(&pending, &predicate));
	UT_ASSERT(pending);
	UT_ASSERT(strcmp(predicate, "QUORUM_OBSERVATION_PENDING") == 0);
	phase_test_formation_epoch++;
	UT_ASSERT(!cluster_serving_ready_check(&pending, &predicate));
	UT_ASSERT(!pending);
	UT_ASSERT(strcmp(predicate, "FORMATION_CHANGED") == 0);
	phase_test_cssd_status = CLUSTER_CSSD_DOWN;
	UT_ASSERT(!cluster_serving_ready_check(&pending, &predicate));
	UT_ASSERT(!pending);
	UT_ASSERT(strcmp(predicate, "CSSD_NOT_READY") == 0);
	UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_OFF);
	UT_ASSERT(!cluster_serving_ready_check(&pending, &predicate));
	UT_ASSERT(!pending);
	UT_ASSERT(strcmp(predicate, "BINDING_ABSENT") == 0);
}

UT_TEST(serving_uses_one_admission_and_unknown_formation_does_not_clear_binding)
{
	bool pending = false;
	const char *predicate = NULL;

	authority_storage_setup(true);
	authority_admission_calls = 0;
	UT_ASSERT(cluster_serving_ready_check(&pending, &predicate));
	UT_ASSERT_EQ(authority_admission_calls, 1);
	phase_test_serving_formation_busy = true;
	authority_admission_calls = 0;
	UT_ASSERT(!cluster_serving_ready_check(&pending, &predicate));
	UT_ASSERT(pending);
	UT_ASSERT_EQ(authority_admission_calls, 1);
	UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_SERVING_READY);
	phase_test_serving_formation_busy = false;
	UT_ASSERT(cluster_serving_ready_check(&pending, &predicate));
	UT_ASSERT(!pending);
	/* A known invalid GRD seal beats an unavailable formation sample. */
	phase_test_serving_formation_busy = true;
	phase_test_grd_authority_ok = false;
	UT_ASSERT(!cluster_serving_ready_check(&pending, &predicate));
	UT_ASSERT(!pending);
	UT_ASSERT_STR_EQ(predicate, "GRD_SEAL_CHANGED");
	phase_test_serving_formation_busy = false;
}

int
main(void)
{
	UT_PLAN(31);
	UT_RUN(gcs_requester_publication_overlap_yields_without_sql_error);
	UT_RUN(gcs_requester_does_not_reinterpret_pending_with_a_later_sample);
	UT_RUN(gcs_requester_pending_never_hides_identity_or_proven_loss);
	UT_RUN(gcs_requester_wait_cannot_renew_an_expired_owner_or_rebind_a_loss);
	UT_RUN(gcs_requester_failure_diagnostic_uses_the_original_predicate);
	UT_RUN(resource_x_same_sample_never_crosses_the_original_serving_loss_cut);
	UT_RUN(resource_x_pending_requires_the_same_serving_identity);
	UT_RUN(resource_x_continuity_never_blocks_or_hides_a_known_refusal);

	UT_RUN(storage_publication_busy_preserves_serving_identity_for_retry);
	UT_RUN(restored_second_sample_cannot_destroy_a_busy_first_binding);
	UT_RUN(storage_publication_busy_preserves_recovery_identity);
	UT_RUN(serving_publication_busy_retries_the_same_recovery_binding);
	UT_RUN(stable_negative_and_expiry_are_terminal);
	UT_RUN(real_loss_between_readers_cannot_be_hidden_by_ready);
	UT_RUN(identity_loss_during_busy_cannot_recover);
	UT_RUN(starting_bind_busy_can_retry_original_generation);
	UT_RUN(starting_publish_busy_can_retry_original_generation);
	UT_RUN(starting_transport_busy_preserves_the_original_binding);
	UT_RUN(recovery_loss_between_readers_cannot_be_hidden_by_ready);
	UT_RUN(serving_publication_cannot_rebind_after_an_unobserved_loss);
	UT_RUN(renewal_after_expiry_cannot_hide_the_continuity_break);
	UT_RUN(phase3_owner_retries_begin_bind_and_publish_without_rebinding);
	UT_RUN(phase3_owner_ends_persistent_busy_at_the_original_deadline);
	UT_RUN(observed_terminal_refusal_cannot_be_forgotten_by_begin);
	UT_RUN(qualified_membership_change_waits_for_original_lmon_rebind);
	UT_RUN(lmon_rebind_cannot_erase_true_storage_loss);
	UT_RUN(lms_and_lmon_busy_publication_preserve_serving_until_fresh_proof);
	UT_RUN(clock_failure_is_not_a_recoverable_publication_wait);
	UT_RUN(stable_continuity_failure_retires_serving_but_publication_overlap_does_not);
	UT_RUN(resource_x_cssd_busy_yields_before_admission_and_never_hides_loss);
	UT_RUN(serving_uses_one_admission_and_unknown_formation_does_not_clear_binding);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
