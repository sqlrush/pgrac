/*-------------------------------------------------------------------------
 * test_cluster_normal_stop.c
 *    Normal-stop runtime: actual shared-region and owner entry points.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_normal_stop.c
 *
 * Allocation/locks are process-boundary fixtures. No table-cleanup verdict,
 * durable close or full-cluster shutdown is synthesized by this test.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "postmaster/interrupt.h"
#include "libpq/pqsignal.h"
#include "utils/ps_status.h"
#include "utils/guc.h"
#include "cluster/cluster_sinval.h"
#include "cluster/cluster_sinval_bcast.h"
#include "cluster/cluster_ko.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_ges_dedup.h"
#include "cluster/cluster_native_lock_probe.h"
#include "cluster/cluster_ic_tier1.h"
#include "storage/smgr.h"
#include "storage/fd.h"
#include "storage/lmgr.h"
#include "storage/buf_internals.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "../../backend/cluster/cluster_clean_leave.c"

#undef printf
#undef fprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static ClusterCleanLeaveSharedState test_region;
static bool test_found;
static Size requested_size;
static unsigned lock_initializations;
static unsigned lock_holds, lock_acquisitions;
static unsigned cleaner_wakes;
static bool modifier_held;
static LWLock *fixture_cleaner_lock;
static LWLock *fixture_lms_lock;
static bool local_idle;
static ClusterNormalStopPollResult ctrc_observation;
static int identity_lock_countdown;
static uint8 identity_root[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
static TimestampTz fixture_now = 1000000;
/* Module result/lock boundaries are fixtures here; each underlying original
 * table observer has its own real-C producer/retirement tests. */
static ClusterNormalStopPollResult module_results[14];
static unsigned module_calls[14];
static unsigned module_post_calls;
static bool module_pi_pending;
static unsigned module_pi_retire_calls;
static void (*pi_cut_race_after_shared_unlock)(void);
static LWLockMode last_leave_lock_mode;
static bool module_change_root;
static uint32 module_late_active;
static bool module_late_completed_debt;
static int disconnected_terminal_peer = -1;
static ClusterNormalStopPollResult lmon_local_observation;
static bool lmon_late_actor;
static void (*checkpoint_wait_action)(void);
static unsigned checkpoint_waits, checkpoint_resets, checkpoint_wakes;
static long checkpoint_previous_timeout;
bool IsUnderPostmaster;
bool IsPostmasterEnvironment;
bool cluster_lmd_enabled = true;
AuxProcType MyAuxProcType = NotAnAuxProcess;

void
cluster_undo_cleaner_wakeup(void)
{
	if (lock_holds != 0)
		abort();
	cleaner_wakes++;
}

void
cluster_lmon_wakeup(void)
{
	if (lock_holds != 0)
		abort();
}

uint64
cluster_cssd_get_dead_generation(void)
{
	if (lock_holds != 0)
		abort();
	return 0;
}

ClusterNormalStopPollResult
cluster_lmon_normal_stop_poll(void)
{
	if (lock_holds != 0 || !AmLmonProcess())
		abort();
	if (lmon_late_actor) {
		MyAuxProcType = LmsProcess;
		if (!cluster_normal_stop_service_enter())
			abort();
		MyAuxProcType = LmonProcess;
		lmon_late_actor = false;
	}
	return lmon_local_observation;
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if ((lock != &test_region.leave.lock && lock != fixture_cleaner_lock
		 && lock != fixture_lms_lock)
		|| lock_holds != 0
		|| (mode != LW_EXCLUSIVE && !(lock == &test_region.leave.lock && mode == LW_SHARED))
		|| !IsUnderPostmaster)
		abort();
	if (identity_lock_countdown > 0 && --identity_lock_countdown == 0)
		identity_root[80] ^= 1;
	lock_holds++;
	lock_acquisitions++;
	if (lock == &test_region.leave.lock)
		last_leave_lock_mode = mode;
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	if ((lock != &test_region.leave.lock && lock != fixture_cleaner_lock
		 && lock != fixture_lms_lock)
		|| lock_holds != 1)
		abort();
	lock_holds--;
	if (lock == &test_region.leave.lock && last_leave_lock_mode == LW_SHARED
		&& pi_cut_race_after_shared_unlock != NULL) {
		void (*action)(void) = pi_cut_race_after_shared_unlock;
		pi_cut_race_after_shared_unlock = NULL;
		action();
	}
}

bool
cluster_ctrc_cleaner_local_idle(void)
{
	if (lock_holds != 0 || modifier_held)
		abort();
	return local_idle;
}

ClusterNormalStopPollResult
cluster_ctrc_normal_stop_poll(ClusterCtrcNormalStopObservation *observed)
{
	if (lock_holds != 0)
		abort();
	MemSet(observed, 0, sizeof(*observed));
	module_calls[3]++;
	observed->result = ctrc_observation;
	observed->object_index = 123;
	observed->domain = CTRC_STOP_DOMAIN_RECEIPT;
	observed->reason = ctrc_observation == CLUSTER_NORMAL_STOP_READY
						   ? CTRC_STOP_REASON_NONE
						   : CTRC_STOP_REASON_NOT_RECLAIMED;
	return ctrc_observation;
}

static ClusterNormalStopPollResult
module_fixture(unsigned index, bool post, const char **reason)
{
	if (lock_holds != 0 || (!AmCheckpointerProcess() && !AmLmonProcess()))
		abort();
	module_calls[index]++;
	module_post_calls += post;
	if (index == 12 && module_change_root)
		identity_root[80] ^= 1;
	if (index == 12 && module_late_active != 0) {
		MyAuxProcType = LmsProcess;
		if (!cluster_normal_stop_service_enter())
			abort();
		MyAuxProcType = CheckpointerProcess;
		module_late_active = 0;
	}
	if (index == 12 && module_late_completed_debt) {
		MyAuxProcType = LmsProcess;
		if (!cluster_normal_stop_service_enter())
			abort();
		module_results[0] = CLUSTER_NORMAL_STOP_PENDING;
		if (!cluster_normal_stop_service_leave(true)
			|| cluster_normal_stop_service_idle(CLUSTER_NORMAL_STOP_READY)
				   != CLUSTER_NORMAL_STOP_READY)
			abort();
		MyAuxProcType = CheckpointerProcess;
		module_late_completed_debt = false;
	}
	if (reason != NULL)
		*reason = "FIXTURE_EXACT_OWNER_REASON";
	return module_results[index];
}

ClusterNormalStopPollResult
cluster_undo_active_write_normal_stop_poll(int *backend, const char **reason)
{
	if (backend != NULL)
		*backend = 73;
	return module_fixture(0, false, reason);
}

ClusterNormalStopPollResult
cluster_tt_slot_normal_stop_poll(uint32 *segment, int *slot, const char **reason)
{
	if (segment != NULL)
		*segment = 259;
	if (slot != NULL)
		*slot = 47;
	return module_fixture(1, false, reason);
}

ClusterNormalStopPollResult
cluster_undo_block0_normal_stop_poll(bool post,
									 const uint8 root[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES],
									 uint64 epoch, uint32 *segment, int *slot, const char **reason)
{
	if (memcmp(root, identity_root, sizeof(identity_root)) != 0 || epoch != 9)
		abort();
	if (segment != NULL)
		*segment = 515;
	if (slot != NULL)
		*slot = 46;
	return module_fixture(2, post, reason);
}

ClusterNormalStopPollResult
cluster_pcm_normal_stop_poll(bool post, BufferTag *tag, uint32 *slot, const char **reason)
{
	ClusterNormalStopPollResult result;
	if (tag != NULL) {
		memset(tag, 0, sizeof(*tag));
		tag->blockNum = 17003;
	}
	if (slot != NULL)
		*slot = 11;
	result = module_fixture(4, post, reason);
	return result == CLUSTER_NORMAL_STOP_READY && post && module_pi_pending
			   ? CLUSTER_NORMAL_STOP_PENDING
			   : result;
}

ClusterNormalStopPollResult
cluster_gcs_block_normal_stop_poll(bool post, int *backend, int *slot, const char **reason)
{
	if (backend != NULL)
		*backend = 72;
	if (slot != NULL)
		*slot = 10;
	return module_fixture(5, post, reason);
}

ClusterNormalStopPollResult
cluster_sf_dep_normal_stop_poll(bool post, int *slot, int *origin, const char **reason)
{
	if (slot != NULL)
		*slot = 17;
	if (origin != NULL)
		*origin = 3;
	return module_fixture(6, post, reason);
}

ClusterNormalStopPollResult
cluster_ges_reply_wait_normal_stop_poll(GesReplyWaitKey *key, const char **reason)
{
	if (key != NULL) {
		memset(key, 0, sizeof(*key));
		key->request_id = 1001;
	}
	return module_fixture(7, false, reason);
}

ClusterNormalStopPollResult
cluster_grd_normal_stop_poll(ClusterResId *resid, uint32 *shard, const char **reason)
{
	if (resid != NULL)
		memset(resid, 0, sizeof(*resid));
	if (shard != NULL)
		*shard = 23;
	return module_fixture(8, false, reason);
}

ClusterNormalStopPollResult
cluster_bufmgr_normal_stop_poll(bool post, BufferTag *tag, int *buffer, const char **reason)
{
	ClusterNormalStopPollResult result;
	if (tag != NULL) {
		memset(tag, 0, sizeof(*tag));
		tag->blockNum = 16972;
	}
	if (buffer != NULL)
		*buffer = 31;
	result = module_fixture(9, post, reason);
	return result == CLUSTER_NORMAL_STOP_READY && post && module_pi_pending
			   ? CLUSTER_NORMAL_STOP_PENDING
			   : result;
}

ClusterNormalStopPollResult
cluster_pcm_normal_stop_local_checkpoint_poll(BufferTag *tag, uint32 *slot, const char **reason)
{
	return cluster_pcm_normal_stop_poll(false, tag, slot, reason);
}

ClusterNormalStopPollResult
cluster_bufmgr_normal_stop_local_checkpoint_poll(BufferTag *tag, int *buffer, const char **reason)
{
	return cluster_bufmgr_normal_stop_poll(false, tag, buffer, reason);
}

ClusterNormalStopPollResult
cluster_bufmgr_normal_stop_pi_retire(BufferTag *tag, int *buffer, const char **reason)
{
	/* The separate bufmgr test executes the actual owner. Here the real
	 * coordinator must establish permission before crossing that boundary. */
	if (!AmCheckpointerProcess() || !cluster_normal_stop_pi_retirement_allowed())
		abort();
	module_pi_retire_calls++;
	return cluster_bufmgr_normal_stop_poll(false, tag, buffer, reason);
}

ClusterNormalStopPollResult
cluster_pcm_normal_stop_pi_retire(BufferTag *tag, uint32 *slot, const char **reason)
{
	if (!AmCheckpointerProcess() || !cluster_normal_stop_pi_retirement_allowed()
		|| module_pi_retire_calls == 0)
		abort();
	module_pi_retire_calls++;
	module_pi_pending = false;
	return cluster_pcm_normal_stop_poll(true, tag, slot, reason);
}

ClusterNormalStopPollResult
cluster_oid_lease_normal_stop_poll(const char **reason)
{
	return module_fixture(10, false, reason);
}

ClusterNormalStopPollResult
cluster_sequence_normal_stop_poll(ClusterResId *resid, const char **reason)
{
	if (resid != NULL)
		memset(resid, 0, sizeof(*resid));
	return module_fixture(11, false, reason);
}

ClusterNormalStopPollResult
cluster_hw_normal_stop_poll(const char **domain, uint64 *key, int *backend, const char **reason)
{
	if (domain != NULL)
		*domain = "REFILL";
	if (key != NULL)
		*key = 999;
	if (backend != NULL)
		*backend = 71;
	return module_fixture(12, false, reason);
}

ClusterNormalStopPollResult
cluster_cf_normal_stop_poll(bool post_checkpoint, const char **reason)
{
	return module_fixture(13, post_checkpoint, reason);
}

void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	if (strcmp(name, "pgrac cluster clean_leave") != 0 || size > sizeof(test_region))
		abort();
	requested_size = size;
	*found = test_found;
	test_found = true;
	return &test_region;
}

void
LWLockInitialize(LWLock *lock, int tranche_id)
{
	if (lock != &test_region.leave.lock || tranche_id != LWTRANCHE_CLUSTER_CLEAN_LEAVE)
		abort();
	MemSet(lock, 0, sizeof(*lock));
	lock->tranche = tranche_id;
	lock_initializations++;
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

static void
reset_region(void)
{
	/* ShmemInitStruct does not promise zero allocation to its initializer. */
	MemSet(&test_region, 0xa5, sizeof(test_region));
	test_found = false;
	requested_size = 0;
	lock_initializations = 0;
	cl_state = NULL;
	cl_normal_stop = NULL;
	cl_normal_stop_service_depth = cl_normal_stop_service_bit = 0;
	cl_normal_stop_expected_services = 0;
	lock_holds = lock_acquisitions = 0;
	cleaner_wakes = 0;
	modifier_held = false;
	fixture_cleaner_lock = NULL;
	fixture_lms_lock = NULL;
	cluster_lmd_enabled = true;
	IsUnderPostmaster = false;
	IsPostmasterEnvironment = true;
	MyAuxProcType = NotAnAuxProcess;
	local_idle = true;
	ctrc_observation = CLUSTER_NORMAL_STOP_READY;
	identity_lock_countdown = 0;
	fixture_now = 1000000;
	memset(cl_normal_stop_front_inbox, 0, sizeof(cl_normal_stop_front_inbox));
	cl_phase1_post_stopped_request_round_nonce = 0;
	memset(cl_phase1_post_stopped_request_sent, 0, sizeof(cl_phase1_post_stopped_request_sent));
	for (unsigned index = 0; index < lengthof(module_results); index++)
		module_results[index] = CLUSTER_NORMAL_STOP_READY;
	memset(module_calls, 0, sizeof(module_calls));
	module_post_calls = 0;
	module_pi_pending = false;
	module_pi_retire_calls = 0;
	pi_cut_race_after_shared_unlock = NULL;
	module_change_root = false;
	module_late_active = 0;
	lmon_local_observation = CLUSTER_NORMAL_STOP_READY;
	lmon_late_actor = false;
	module_late_completed_debt = false;
}

UT_TEST(test_actual_region_size_matches_frozen_tail)
{
	UT_ASSERT_EQ(sizeof(ClusterLeaveState), 472);
	UT_ASSERT_EQ(sizeof(ClusterNormalStopState), 744);
	UT_ASSERT_EQ(sizeof(ClusterCleanLeaveSharedState), 1216);
	UT_ASSERT_EQ(offsetof(ClusterNormalStopState, open_record), 80);
	UT_ASSERT_EQ(offsetof(ClusterNormalStopState, root_descriptor), 160);
	UT_ASSERT_EQ(offsetof(ClusterNormalStopState, service_seal), 728);
	UT_ASSERT_EQ(cluster_clean_leave_shmem_size(), MAXALIGN(sizeof(ClusterCleanLeaveSharedState)));
}

UT_TEST(test_actual_fresh_initializer_initializes_full_tail_once)
{
	ClusterNormalStopState zero = { 0 };

	reset_region();
	cluster_clean_leave_shmem_init();
	UT_ASSERT_EQ(requested_size, MAXALIGN(sizeof(ClusterCleanLeaveSharedState)));
	UT_ASSERT_EQ(lock_initializations, 1);
	UT_ASSERT(cl_state == &test_region.leave);
	UT_ASSERT_EQ(test_region.leave.leaving_node_id, -1);
	UT_ASSERT_EQ(memcmp(&test_region.normal_stop, &zero, sizeof(zero)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.phase), CLUSTER_NORMAL_STOP_IDLE);
}

UT_TEST(test_actual_attach_preserves_normal_stop_and_early_peer_state)
{
	ClusterNormalStopState before;

	reset_region();
	cluster_clean_leave_shmem_init();
	pg_atomic_write_u32(&test_region.normal_stop.failure_reason, 17);
	pg_atomic_write_u32(&test_region.normal_stop.identity_published, 1);
	test_region.normal_stop.epoch = 91;
	test_region.normal_stop.peer_requests_seen = 2;
	test_region.normal_stop.peer_request_nonce[1] = 777;
	before = test_region.normal_stop;
	cluster_clean_leave_shmem_init();
	UT_ASSERT_EQ(lock_initializations, 1);
	UT_ASSERT_EQ(memcmp(&test_region.normal_stop, &before, sizeof(before)), 0);
	/* The preexisting phase-1 cleanup is not a normal-stop reset owner. */
	cl_phase1_full_stop_release_state_reset_locked();
	UT_ASSERT_EQ(memcmp(&test_region.normal_stop, &before, sizeof(before)), 0);
}

UT_TEST(test_postmaster_only_monotonic_intent_without_lwlock)
{
	reset_region();
	UT_ASSERT(!cluster_normal_stop_postmaster_request());
	cluster_clean_leave_shmem_init();
	UT_ASSERT(!cluster_normal_stop_postmaster_frontends_gone());
	IsUnderPostmaster = true;
	UT_ASSERT(!cluster_normal_stop_postmaster_request());
	UT_ASSERT(!cluster_normal_stop_requested());
	IsUnderPostmaster = false;
	UT_ASSERT(cluster_normal_stop_postmaster_request());
	UT_ASSERT(cluster_normal_stop_requested());
	UT_ASSERT(cluster_normal_stop_postmaster_request());
	UT_ASSERT(cluster_normal_stop_postmaster_frontends_gone());
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.frontends_gone), 1);
	UT_ASSERT_EQ(lock_acquisitions, 0);
}

UT_TEST(test_normal_stop_first_failure_is_sticky_even_after_protocol_close)
{
	reset_region();
	cluster_clean_leave_shmem_init();
	UT_ASSERT(cluster_normal_stop_postmaster_request());
	pg_atomic_write_u32(&test_region.normal_stop.phase, CLUSTER_NORMAL_STOP_PROTOCOL_CLOSED);
	UT_ASSERT(cluster_normal_stop_protocol_closed());
	cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_CLEANER);
	cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_NONE);
	cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_DEADLINE);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_CLEANER);
	UT_ASSERT(!cluster_normal_stop_protocol_closed());
	UT_ASSERT(!cluster_normal_stop_postmaster_request());
	UT_ASSERT_EQ(lock_acquisitions, 0);
}

/* Prior formation/producer observations are this test's input boundary;
 * their real capture and all-module checks are separate integration tests. */
static void
seed_drain_boundary(void)
{
	reset_region();
	cluster_clean_leave_shmem_init();
	if (!cluster_normal_stop_postmaster_request()
		|| !cluster_normal_stop_postmaster_frontends_gone())
		return;
	pg_atomic_write_u32(&test_region.normal_stop.identity_published, 1);
	pg_atomic_write_u32(&test_region.normal_stop.phase, CLUSTER_NORMAL_STOP_DRAIN);
	IsUnderPostmaster = true;
	MyAuxProcType = CheckpointerProcess;
}

UT_TEST(test_only_checkpointer_can_request_park_after_readonly_ctrc_ready)
{
	seed_drain_boundary();
	/* Primitive input boundary; the combined coordinator tests below drive
	 * the actual actor publications instead of supplying this mask. */
	cl_normal_stop_expected_services = 2047;
	pg_atomic_write_u32(&cl_normal_stop->service_idle_mask, 2047);
	MyAuxProcType = UndoCleanerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_request_cleaner_quiesce(), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT(!cluster_normal_stop_cleaner_park_requested());
	MyAuxProcType = CheckpointerProcess;
	ctrc_observation = CLUSTER_NORMAL_STOP_PENDING;
	UT_ASSERT_EQ(cluster_normal_stop_request_cleaner_quiesce(), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(!cluster_normal_stop_cleaner_park_requested());
	ctrc_observation = CLUSTER_NORMAL_STOP_READY;
	UT_ASSERT_EQ(cluster_normal_stop_request_cleaner_quiesce(), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT(cluster_normal_stop_cleaner_park_requested());
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.phase), CLUSTER_NORMAL_STOP_QUIESCE);
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.cleaner_quiesced_mask), 0);
	UT_ASSERT_EQ(cleaner_wakes, 1);
	UT_ASSERT_EQ(lock_holds, 0);
}

UT_TEST(test_workers_sign_only_their_own_bit_after_local_idle)
{
	seed_drain_boundary();
	cl_normal_stop_expected_services = 2047;
	pg_atomic_write_u32(&cl_normal_stop->service_idle_mask, 2047);
	UT_ASSERT_EQ(cluster_normal_stop_request_cleaner_quiesce(), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT(!cluster_normal_stop_cleaner_park()); /* checkpointer cannot sign */
	for (unsigned i = 0; i < CLUSTER_UNDO_CLEANER_WORKER_TYPES; i++) {
		MyAuxProcType = ClusterUndoCleanerTypeForWorker(i);
		UT_ASSERT(cluster_normal_stop_cleaner_park());
		UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.cleaner_quiesced_mask),
					 (UINT32_C(1) << (i + 1)) - 1);
		MyAuxProcType = CheckpointerProcess;
		UT_ASSERT_EQ(cluster_normal_stop_cleaners_are_parked(), i == 7);
	}
	UT_ASSERT_EQ(lock_holds, 0);
}

UT_TEST(test_busy_or_reborn_worker_cannot_sign_success)
{
	seed_drain_boundary();
	cl_normal_stop_expected_services = 2047;
	pg_atomic_write_u32(&cl_normal_stop->service_idle_mask, 2047);
	UT_ASSERT_EQ(cluster_normal_stop_request_cleaner_quiesce(), CLUSTER_NORMAL_STOP_READY);
	MyAuxProcType = UndoCleanerProcess;
	local_idle = false;
	UT_ASSERT(!cluster_normal_stop_cleaner_park());
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.cleaner_quiesced_mask), 0);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_CLEANER);
	local_idle = true;
	UT_ASSERT(!cluster_normal_stop_cleaner_park());
	seed_drain_boundary();
	cl_normal_stop_expected_services = 2047;
	pg_atomic_write_u32(&cl_normal_stop->service_idle_mask, 2047);
	UT_ASSERT_EQ(cluster_normal_stop_request_cleaner_quiesce(), CLUSTER_NORMAL_STOP_READY);
	MyAuxProcType = UndoCleanerWorker7Process;
	UT_ASSERT(cluster_normal_stop_cleaner_park());
	/* The actor parks once and never re-enters: same role signing again
	 * means illegal re-entry/rebirth, not evidence for another worker. */
	UT_ASSERT(!cluster_normal_stop_cleaner_park());
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_CLEANER);
}

UT_TEST(test_qvotec_final_result_is_not_protocol_or_quorum_success)
{
	seed_drain_boundary();
	UT_ASSERT(!cluster_normal_stop_qvotec_cleared());
	UT_ASSERT(!cluster_normal_stop_qvotec_complete(true)); /* wrong owner */
	MyAuxProcType = QvotecProcess;
	UT_ASSERT(!cluster_normal_stop_qvotec_complete(true)); /* premature */
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_QVOTEC);
	seed_drain_boundary();
	pg_atomic_write_u32(&test_region.normal_stop.phase, CLUSTER_NORMAL_STOP_PROTOCOL_CLOSED);
	MyAuxProcType = QvotecProcess;
	UT_ASSERT(!cluster_normal_stop_qvotec_cleared());
	UT_ASSERT(cluster_normal_stop_qvotec_complete(true));
	UT_ASSERT(cluster_normal_stop_qvotec_cleared());
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.qvotec_clear_result), 1);
	UT_ASSERT(!cluster_normal_stop_qvotec_complete(false));
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.qvotec_clear_result), 2);
	UT_ASSERT(!cluster_normal_stop_protocol_closed());
	UT_ASSERT(!cluster_normal_stop_qvotec_cleared());
}

UT_TEST(test_service_tracks_nested_work_when_request_arrives_mid_pass)
{
	reset_region();
	cluster_clean_leave_shmem_init();
	IsUnderPostmaster = true;
	MyAuxProcType = LmsWorker7Process;
	UT_ASSERT(cluster_normal_stop_service_enter());
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 1);
	UT_ASSERT_EQ(lock_acquisitions, 0); /* ordinary operation needs no leave lock */
	IsUnderPostmaster = false;			/* another process's atomic postmaster publication */
	UT_ASSERT(cluster_normal_stop_postmaster_request());
	UT_ASSERT(cluster_normal_stop_postmaster_frontends_gone());
	pg_atomic_write_u32(&test_region.normal_stop.identity_published, 1);
	pg_atomic_write_u32(&test_region.normal_stop.phase, CLUSTER_NORMAL_STOP_DRAIN);
	IsUnderPostmaster = true;
	UT_ASSERT(cluster_normal_stop_service_new_work(true)); /* outer pass began before request */
	UT_ASSERT_EQ(lock_acquisitions, 0);
	UT_ASSERT(cluster_normal_stop_service_enter());
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_active_mask), 256);
	UT_ASSERT(cluster_normal_stop_service_leave(true));
	UT_ASSERT_EQ(cluster_normal_stop_service_idle(CLUSTER_NORMAL_STOP_READY),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_idle_mask), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_active_mask), 256);
	UT_ASSERT(cluster_normal_stop_service_leave(true));
	UT_ASSERT_EQ(cluster_normal_stop_service_idle(CLUSTER_NORMAL_STOP_READY),
				 CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_idle_mask), 256);
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_active_mask), 0);
	UT_ASSERT_EQ(lock_holds, 0);
}

UT_TEST(test_service_exception_and_invalid_module_never_sign_idle)
{
	seed_drain_boundary();
	MyAuxProcType = LmonProcess;
	UT_ASSERT(cluster_normal_stop_service_enter());
	UT_ASSERT(!cluster_normal_stop_service_leave(false));
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_active_mask), 0);
	UT_ASSERT_EQ(cluster_normal_stop_service_idle(CLUSTER_NORMAL_STOP_READY),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_idle_mask), 0);
	seed_drain_boundary();
	MyAuxProcType = SinvalBcastProcess;
	UT_ASSERT_EQ(cluster_normal_stop_service_idle(CLUSTER_NORMAL_STOP_PENDING),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(cluster_normal_stop_service_idle(CLUSTER_NORMAL_STOP_INVALID),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_MODULE);
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_idle_mask), 0);
}

/* Actor tests consume module results as explicit fixtures, not product poll
 * evidence. Each actual publication API signs only the simulated own role. */
static void
seed_service_cut(void)
{
	seed_drain_boundary();
	cl_normal_stop_expected_services = 2047;
	pg_atomic_write_u32(&cl_normal_stop->service_idle_mask, 2047);
	test_region.normal_stop.peer_requests_seen = 15;
	if (cluster_normal_stop_request_cleaner_quiesce() != CLUSTER_NORMAL_STOP_READY)
		abort();
	for (unsigned i = 0; i < CLUSTER_UNDO_CLEANER_WORKER_TYPES; i++) {
		MyAuxProcType = ClusterUndoCleanerTypeForWorker(i);
		if (!cluster_normal_stop_cleaner_park())
			abort();
	}
	for (unsigned i = 0; i < 11; i++) {
		MyAuxProcType = i == 0	  ? LmonProcess
						: i == 10 ? LmdProcess
						: i == 9  ? SinvalBcastProcess
						: i == 1  ? LmsProcess
								  : (AuxProcType)(LmsWorker1Process + i - 2);
		UT_ASSERT(cluster_normal_stop_service_enter());
		UT_ASSERT(cluster_normal_stop_service_leave(true));
		UT_ASSERT_EQ(cluster_normal_stop_service_idle(CLUSTER_NORMAL_STOP_READY),
					 CLUSTER_NORMAL_STOP_READY);
	}
	MyAuxProcType = CheckpointerProcess;
	/* This helper isolates the real actor/leave-lock cut. The global proof
	 * is an explicit input here; real envelope/coordinator tests below
	 * independently prove that no missing peer can produce this input. */
	pg_atomic_write_u32(&cl_normal_stop->phase, CLUSTER_NORMAL_STOP_WAIT_DRAIN_ACK);
	cl_state->ack_bitmap[0] = UINT32_C(15) & ~(UINT32_C(1) << cluster_node_id);
	cl_normal_stop->peer_reply_sent = cl_state->ack_bitmap[0];
	cl_normal_stop->peer_reply_pending = 0;
}

UT_TEST(test_service_dispatch_and_seal_share_one_cut)
{
	seed_service_cut();
	MyAuxProcType = LmsProcess;
	UT_ASSERT(cluster_normal_stop_service_enter());
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_idle_mask), 2045);
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_service_seal(2047, 1), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_seal), 0);
	MyAuxProcType = LmsProcess;
	UT_ASSERT(cluster_normal_stop_service_leave(true));
	UT_ASSERT_EQ(cluster_normal_stop_service_idle(CLUSTER_NORMAL_STOP_READY),
				 CLUSTER_NORMAL_STOP_READY);
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_service_seal(2047, 1), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_seal), 1);
	/* The reverse ordering: control work may run, a new modifier may not. */
	MyAuxProcType = LmsProcess;
	UT_ASSERT(cluster_normal_stop_service_enter());
	UT_ASSERT(cluster_normal_stop_service_new_work(false));
	UT_ASSERT(!cluster_normal_stop_service_new_work(true));
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_LATE_UNSEALED_WORK);
	UT_ASSERT(!cluster_normal_stop_service_leave(true));
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_active_mask), 0);
}

UT_TEST(test_service_post_stopped_seal_and_expected_roster)
{
	seed_service_cut();
	test_region.normal_stop.peer_requests_seen = 7;
	UT_ASSERT_EQ(cluster_normal_stop_service_seal(2047, 1), CLUSTER_NORMAL_STOP_PENDING);
	test_region.normal_stop.peer_requests_seen = 15;
	pg_atomic_write_u32(&test_region.normal_stop.cleaner_quiesced_mask, 127);
	UT_ASSERT_EQ(cluster_normal_stop_service_seal(2047, 1), CLUSTER_NORMAL_STOP_PENDING);
	pg_atomic_write_u32(&test_region.normal_stop.cleaner_quiesced_mask, 255);
	UT_ASSERT_EQ(cluster_normal_stop_service_seal(2047, 1), CLUSTER_NORMAL_STOP_READY);
	pg_atomic_write_u32(&test_region.normal_stop.phase, CLUSTER_NORMAL_STOP_POST_STOPPED);
	UT_ASSERT_EQ(cluster_normal_stop_service_seal(2047, 2), CLUSTER_NORMAL_STOP_READY);
	MyAuxProcType = LmonProcess;
	UT_ASSERT(cluster_normal_stop_service_enter()); /* original control/duplicate handling */
	UT_ASSERT(!cluster_normal_stop_service_new_work(false));
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_LATE_UNSEALED_WORK);
	UT_ASSERT(!cluster_normal_stop_service_leave(true));
	seed_service_cut();
	/* worker7 cannot be dropped merely because it has no new idle report. */
	pg_atomic_write_u32(&test_region.normal_stop.service_idle_mask, 2047 & ~256);
	UT_ASSERT_EQ(cluster_normal_stop_service_seal(2047, 1), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_seal), 0);
	UT_ASSERT_EQ(cluster_normal_stop_service_seal(0, 1), CLUSTER_NORMAL_STOP_INVALID);
}

UT_TEST(test_control_seal_projection_uses_actual_second_cut_without_mutation)
{
	ClusterCleanLeaveSharedState before;
	seed_service_cut();
	UT_ASSERT(!cluster_normal_stop_service_control_sealed());
	UT_ASSERT_EQ(cluster_normal_stop_service_seal(2047, 1), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT(!cluster_normal_stop_service_control_sealed());
	pg_atomic_write_u32(&cl_normal_stop->phase, CLUSTER_NORMAL_STOP_POST_STOPPED);
	UT_ASSERT_EQ(cluster_normal_stop_service_seal(2047, 2), CLUSTER_NORMAL_STOP_READY);
	before = test_region;
	UT_ASSERT(cluster_normal_stop_service_control_sealed());
	UT_ASSERT(memcmp(&before, &test_region, sizeof(before)) == 0);
	pg_atomic_write_u32(&cl_normal_stop->requested, 0);
	UT_ASSERT(!cluster_normal_stop_service_control_sealed());
	seed_service_cut();
	pg_atomic_write_u32(&cl_normal_stop->service_seal, 3);
	UT_ASSERT(cluster_normal_stop_service_control_sealed());
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_STATE);
}

UT_TEST(test_service_owner_and_stage_errors_are_not_authority)
{
	seed_drain_boundary();
	UT_ASSERT(!cluster_normal_stop_service_enter()); /* checkpointer is not an actor */
	seed_service_cut();
	MyAuxProcType = LmsWorker7Process;
	UT_ASSERT_EQ(cluster_normal_stop_service_seal(2047, 1), CLUSTER_NORMAL_STOP_INVALID);
	seed_service_cut();
	UT_ASSERT_EQ(cluster_normal_stop_service_seal(2047, 2), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_seal), 0);
	seed_drain_boundary();
	MyAuxProcType = LmonProcess;
	UT_ASSERT(!cluster_normal_stop_service_new_work(false)); /* no active bracket */
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
}

UT_TEST(test_service_lmd_participation_is_frozen_not_live_pid_dependent)
{
	seed_service_cut();
	UT_ASSERT_EQ(cl_normal_stop_config_service_mask(), 2047);
	UT_ASSERT(cl_normal_stop_service_mask_valid(2047));
	UT_ASSERT(!cl_normal_stop_service_mask_valid(1023));
	MyAuxProcType = LmdProcess;
	UT_ASSERT(cluster_normal_stop_service_enter());
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->service_active_mask), 1024);
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_service_seal(2047, 1), CLUSTER_NORMAL_STOP_PENDING);
	MyAuxProcType = LmdProcess;
	UT_ASSERT(cluster_normal_stop_service_leave(true));
	UT_ASSERT_EQ(cluster_normal_stop_service_idle(CLUSTER_NORMAL_STOP_PENDING),
				 CLUSTER_NORMAL_STOP_PENDING);
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_service_seal(2047, 1), CLUSTER_NORMAL_STOP_PENDING);
	MyAuxProcType = LmdProcess;
	UT_ASSERT_EQ(cluster_normal_stop_service_idle(CLUSTER_NORMAL_STOP_READY),
				 CLUSTER_NORMAL_STOP_READY);
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_service_seal(2047, 1), CLUSTER_NORMAL_STOP_READY);
	/* PGC_POSTMASTER disabled has no actor; a dead enabled LMD is not this case. */
	reset_region();
	cluster_lmd_enabled = false;
	UT_ASSERT_EQ(cl_normal_stop_config_service_mask(), 1023);
	UT_ASSERT(cl_normal_stop_service_mask_valid(1023));
	UT_ASSERT(!cl_normal_stop_service_mask_valid(2047));
	cluster_lmd_enabled = true;
}

/* The complete production main loop is compiled verbatim. The outer pass
 * remains an explicit boundary fixture here (not proof of real TT/GC I/O):
 * it requests park while owning a modifier, then releases it on return.
 * The guard fixture rejects any attempt to sign while that pass is active. */
static UndoCleanerSharedState cleaner_region;
static UndoCleanerSharedState *undo_cleaner_state = &cleaner_region;
static int undo_cleaner_worker = -1;
static pg_on_exit_callback exit_callback;
static sigjmp_buf main_exit;
static unsigned main_passes, main_waits, main_bindings;
static int main_exit_code, main_error_level;
static bool main_early_shutdown, main_pass_failure;
static bool main_progress;
static bool sinval_main_test;
static bool lms_main_test;
static int test_lms_wait(void);
static int sinval_scenario;
static unsigned sinval_stage, sinval_passes, sinval_polls, ko_polls;
static ClusterNormalStopPollResult sinval_observation, ko_observation;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
MemoryContext TopMemoryContext;
MemoryContext CurrentMemoryContext;
static MemoryContextData sinval_context_fixture;
volatile sig_atomic_t ConfigReloadPending;
volatile sig_atomic_t ShutdownRequestPending;
volatile sig_atomic_t InterruptPending;
volatile uint32 InterruptHoldoffCount;
BackendType MyBackendType;
sigset_t UnBlockSig;
Latch *MyLatch;
PROC_HDR *ProcGlobal;
int cluster_undo_cleaner_interval_ms = 1000;
int cluster_lmon_main_loop_interval = 500;
int cluster_sinval_broadcast_batch_timeout_ms = 10;

bool
cluster_ctrc_cleaner_bind_worker(unsigned worker)
{
	main_bindings++;
	return worker < CLUSTER_UNDO_CLEANER_WORKER_TYPES;
}

uint64
cluster_ctrc_cleaner_local_passes(void)
{
	return main_passes;
}
uint64
cluster_ctrc_cleaner_local_progress(void)
{
	return main_passes;
}
void
init_ps_display(const char *fixed_part pg_attribute_unused())
{}
void
ProcessConfigFile(GucContext context pg_attribute_unused())
{}
void
SignalHandlerForConfigReload(SIGNAL_ARGS)
{
	(void)postgres_signal_arg;
}
void
SignalHandlerForShutdownRequest(SIGNAL_ARGS)
{
	(void)postgres_signal_arg;
}
void
ProcessInterrupts(void)
{
	abort();
}

void
before_shmem_exit(pg_on_exit_callback function, Datum arg)
{
	if (exit_callback != NULL || arg != 0)
		abort();
	exit_callback = function;
}

void
proc_exit(int code)
{
	pg_on_exit_callback callback = exit_callback;
	/* Native shmem_exit removes a callback before invoking it, so a callback
	 * can safely escalate exit(0) to exit(1) without being called again. */
	exit_callback = NULL;
	main_exit_code = code;
	if (sinval_main_test && sinval_scenario == 7 && code == 0)
		ko_observation = CLUSTER_NORMAL_STOP_PENDING;
	if (callback != NULL)
		callback(code, 0);
	siglongjmp(main_exit, 1);
}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	main_error_level = elevel;
	return elevel >= ERROR;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

int
errcode(int code pg_attribute_unused())
{
	return 0;
}
int
errmsg(const char *format pg_attribute_unused(), ...)
{
	return 0;
}
int
errmsg_internal(const char *format pg_attribute_unused(), ...)
{
	return 0;
}
int
errhint(const char *format pg_attribute_unused(), ...)
{
	return 0;
}
int
errdetail(const char *format pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *function pg_attribute_unused())
{
	if (main_error_level >= ERROR)
		proc_exit(1);
}

int
WaitLatch(Latch *latch pg_attribute_unused(), int events, long timeout pg_attribute_unused(),
		  uint32 event pg_attribute_unused())
{
	if (checkpoint_wait_action != NULL) {
		UT_ASSERT(AmCheckpointerProcess());
		UT_ASSERT_EQ(lock_holds, 0);
		UT_ASSERT_EQ(cl_normal_stop_service_depth, 0);
		UT_ASSERT_EQ(events, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH);
		UT_ASSERT_EQ(event, WAIT_EVENT_RECONFIG_BARRIER_WAIT);
		UT_ASSERT(timeout > 0 && timeout <= checkpoint_previous_timeout);
		UT_ASSERT(checkpoint_resets > checkpoint_waits);
		checkpoint_previous_timeout = timeout;
		if (++checkpoint_waits > 8)
			abort();
		checkpoint_wait_action();
		fixture_now += 1000;
		return WL_LATCH_SET;
	}
	if ((events & WL_LATCH_SET) == 0 || modifier_held || lock_holds != 0 || main_waits >= 3)
		abort();
	main_waits++;
	if (lms_main_test)
		return test_lms_wait();
	if (sinval_main_test) {
		UT_ASSERT_EQ(cl_normal_stop_service_depth, 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_active_mask), 0);
		if (cluster_normal_stop_requested())
			UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_idle_mask),
						 sinval_observation == CLUSTER_NORMAL_STOP_READY ? 512 : 0);
		if (main_waits == 1 && sinval_scenario == 3)
			sinval_observation = CLUSTER_NORMAL_STOP_READY;
		if (main_waits == 2) {
			if (cluster_normal_stop_requested()) {
				pg_atomic_write_u32(&test_region.normal_stop.phase,
									CLUSTER_NORMAL_STOP_PROTOCOL_CLOSED);
				pg_atomic_write_u32(&test_region.normal_stop.service_seal, 2);
			}
			if (sinval_scenario == 6)
				ko_observation = CLUSTER_NORMAL_STOP_PENDING;
			ShutdownRequestPending = true;
		}
		return WL_LATCH_SET;
	}
	if (main_waits == 2 || main_early_shutdown) {
		if (!main_early_shutdown)
			pg_atomic_write_u32(&test_region.normal_stop.phase,
								CLUSTER_NORMAL_STOP_PROTOCOL_CLOSED);
		ShutdownRequestPending = true;
	}
	return WL_LATCH_SET;
}

void
ResetLatch(Latch *latch pg_attribute_unused())
{
	if (checkpoint_wait_action != NULL)
		checkpoint_resets++;
}
void
SetLatch(Latch *latch pg_attribute_unused())
{
	if (lock_holds != 0 || modifier_held)
		abort();
	checkpoint_wakes++;
}

static pqsigfunc
test_signal(int signal pg_attribute_unused(), pqsigfunc handler)
{
	return handler;
}

static int
test_sigmask(int how pg_attribute_unused(), const sigset_t *set pg_attribute_unused(),
			 sigset_t *old pg_attribute_unused())
{
	return 0;
}

static void
undo_cleaner_publish_status(UndoCleanerStatus status pg_attribute_unused())
{}
static void
undo_cleaner_advance_liveness_tick(void)
{}
static bool
undo_cleaner_shutdown_requested(void)
{
	return false;
}

static bool
undo_cleaner_run_pass(bool *work_remaining)
{
	modifier_held = true;
	main_passes++;
	if (main_pass_failure)
		proc_exit(1);
	/* The real controller's publication occurs concurrently with this pass. */
	pg_atomic_write_u32(&test_region.normal_stop.phase, CLUSTER_NORMAL_STOP_QUIESCE);
	pg_atomic_write_u32(&test_region.normal_stop.cleaner_quiesce_requested, 1);
	modifier_held = false;
	*work_remaining = main_progress;
	/* Finite old-product RED: do not spin when the missing park edge ignores
	 * the request and takes the work_remaining fast continue a second time. */
	if (main_passes == 2)
		ShutdownRequestPending = true;
	return false;
}

#define pqsignal test_signal
#define sigprocmask test_sigmask
#undef CLUSTER_INJECTION_POINT
#define CLUSTER_INJECTION_POINT(name) ((void)0)
#include "test_cluster_normal_stop_cleaner.inc"
#undef pqsignal
#undef sigprocmask

static void
run_actual_cleaner_main(unsigned worker, bool request_before, bool progress, bool premature_exit,
						bool fail_pass)
{
	sinval_main_test = false;
	seed_drain_boundary();
	MyAuxProcType = ClusterUndoCleanerTypeForWorker(worker);
	if (request_before) {
		pg_atomic_write_u32(&test_region.normal_stop.phase, CLUSTER_NORMAL_STOP_QUIESCE);
		pg_atomic_write_u32(&test_region.normal_stop.cleaner_quiesce_requested, 1);
	}
	MemSet(&cleaner_region, 0, sizeof(cleaner_region));
	fixture_cleaner_lock = &cleaner_region.lwlock;
	main_passes = main_waits = main_bindings = 0;
	main_exit_code = -1;
	main_progress = progress;
	main_early_shutdown = premature_exit;
	main_pass_failure = fail_pass;
	ShutdownRequestPending = ConfigReloadPending = InterruptPending = false;
	exit_callback = NULL;
	if (sigsetjmp(main_exit, 0) == 0)
		UndoCleanerMain();
}

UT_TEST(test_actual_main_parks_after_pass_before_fast_continue)
{
	for (unsigned worker = 0; worker < CLUSTER_UNDO_CLEANER_WORKER_TYPES; worker += 7) {
		run_actual_cleaner_main(worker, false, true, false, false);
		UT_ASSERT_EQ(main_passes, 1);
		UT_ASSERT_EQ(main_waits, 2);
		UT_ASSERT_EQ(main_exit_code, 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.cleaner_quiesced_mask),
					 UINT32_C(1) << worker);
		UT_ASSERT(!modifier_held);
	}
}

UT_TEST(test_actual_main_does_not_restart_pass_after_idle_park_request)
{
	run_actual_cleaner_main(7, true, true, false, false);
	UT_ASSERT_EQ(main_passes, 0);
	UT_ASSERT_EQ(main_waits, 2);
	UT_ASSERT_EQ(main_exit_code, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.cleaner_quiesced_mask), 128);
}

UT_TEST(test_actual_main_early_exit_or_failed_pass_never_signs_clean_shutdown)
{
	run_actual_cleaner_main(0, false, false, true, false);
	UT_ASSERT_EQ(main_exit_code, 1);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_CLEANER);
	run_actual_cleaner_main(0, false, false, false, true);
	UT_ASSERT_EQ(main_exit_code, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.cleaner_quiesced_mask), 0);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_CLEANER);
	modifier_held = false; /* The simulated dying process never resumes a pass. */
}

/* Complete SI Main is extracted from the production source, not rewritten.
 * Module observations and native runtime cleanup are explicit fixtures; their
 * actual producers/consumers are exercised by the SI/KO module tests. */
MemoryContext
AllocSetContextCreateInternal(MemoryContext parent, const char *name, Size minsize, Size initsize,
							  Size maxsize)
{
	return &sinval_context_fixture;
}
void
MemoryContextReset(MemoryContext context)
{}
void
EmitErrorReport(void)
{}
void
FlushErrorState(void)
{}
void
pg_re_throw(void)
{
	Assert(PG_exception_stack != NULL);
	siglongjmp(*PG_exception_stack, 1);
}
void
LWLockReleaseAll(void)
{
	lock_holds = 0;
}
bool
ConditionVariableCancelSleep(void)
{
	return false;
}
void
UnlockBuffers(void)
{
	modifier_held = false;
}
void
ReleaseAuxProcessResources(bool isCommit)
{}
void
AtEOXact_Buffers(bool isCommit)
{}
void
AtEOXact_SMgr(void)
{}
void
AtEOXact_Files(bool isCommit)
{}
void
AtEOXact_HashTables(bool isCommit)
{}
void
procsignal_sigusr1_handler(SIGNAL_ARGS)
{
	(void)postgres_signal_arg;
}
void
cluster_sinval_register_proc_latch(Latch *latch)
{}
void
cluster_sinval_unregister_proc_latch(void)
{}

ClusterNormalStopPollResult
cluster_sinval_normal_stop_poll(const char **domain, uint64 *key, const char **reason)
{
	UT_ASSERT_EQ(lock_holds, 0);
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 0);
	sinval_polls++;
	*domain = "sinval";
	*key = 0;
	*reason = "FIXTURE";
	return sinval_observation;
}
ClusterNormalStopPollResult
cluster_ko_normal_stop_poll(uint32 *slot, const char **reason)
{
	UT_ASSERT_EQ(lock_holds, 0);
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 0);
	ko_polls++;
	*slot = 0;
	*reason = "FIXTURE";
	return ko_observation;
}
void
cluster_sinval_apply_inbound_overflow_reset_if_pending(void)
{
	UT_ASSERT_EQ(sinval_stage, 0);
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 1);
	if (sinval_scenario == 5 && sinval_passes == 0) {
		/* Request arrives during an ordinary already-entered work segment. */
		pg_atomic_write_u32(&test_region.normal_stop.requested, 1);
		UT_ASSERT_EQ(cluster_normal_stop_service_idle(CLUSTER_NORMAL_STOP_READY),
					 CLUSTER_NORMAL_STOP_PENDING);
	}
	if (sinval_scenario == 2 && sinval_passes == 0) {
		sinval_passes++;
		/* The native module throws while holding an original lock/pin.
		 * FINALLY must release native locks before taking the leave lock. */
		lock_holds = 1;
		modifier_held = true;
		siglongjmp(*PG_exception_stack, 1);
	}
	sinval_stage = 1;
}
void
cluster_sinval_drain_inbound_and_apply(void)
{
	UT_ASSERT_EQ(sinval_stage, 1);
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 1);
	sinval_stage = 2;
}
void
cluster_ko_drain_inbound_and_apply(void)
{
	UT_ASSERT_EQ(sinval_stage, 2);
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 1);
	sinval_stage = 0;
	sinval_passes++;
}

#define pqsignal test_signal
#define sigprocmask test_sigmask
#include "test_cluster_normal_stop_sinval.inc"
#undef pqsignal
#undef sigprocmask

static void
run_actual_sinval_main(int scenario)
{
	seed_drain_boundary();
	MyAuxProcType = SinvalBcastProcess;
	sinval_main_test = true;
	sinval_scenario = scenario;
	sinval_stage = sinval_passes = sinval_polls = ko_polls = 0;
	main_passes = main_waits = 0;
	main_exit_code = -1;
	main_early_shutdown = false;
	sinval_observation = scenario == 3 ? CLUSTER_NORMAL_STOP_PENDING : CLUSTER_NORMAL_STOP_READY;
	ko_observation = scenario == 4 ? CLUSTER_NORMAL_STOP_INVALID : CLUSTER_NORMAL_STOP_READY;
	if (scenario == 0 || scenario == 5)
		pg_atomic_write_u32(&test_region.normal_stop.requested, 0);
	ShutdownRequestPending = scenario == 1;
	ConfigReloadPending = InterruptPending = false;
	InterruptHoldoffCount = 0;
	PG_exception_stack = NULL;
	error_context_stack = NULL;
	exit_callback = NULL;
	if (sigsetjmp(main_exit, 0) == 0)
		SinvalBcastMain();
	PG_exception_stack = NULL;
	sinval_main_test = false;
}
UT_TEST(test_sinval_actual_main_wraps_full_work_not_sleep)
{
	run_actual_sinval_main(0);
	UT_ASSERT_EQ(main_exit_code, 0);
	UT_ASSERT_EQ(sinval_passes, 2);
	UT_ASSERT_EQ(sinval_polls, 0); /* Ordinary runtime adds no module scan. */
	run_actual_sinval_main(5);
	UT_ASSERT_EQ(main_exit_code, 0);
	UT_ASSERT_EQ(sinval_passes, 2);
	UT_ASSERT(sinval_polls >= 3 && ko_polls >= 3);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_NONE);
}
UT_TEST(test_sinval_actual_main_pending_cannot_sign_idle)
{
	run_actual_sinval_main(3);
	UT_ASSERT_EQ(main_exit_code, 0);
	UT_ASSERT_EQ(sinval_passes, 2);
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_idle_mask), 512);
}
UT_TEST(test_sinval_actual_main_premature_shutdown_fails)
{
	run_actual_sinval_main(1);
	UT_ASSERT_EQ(main_exit_code, 1);
	UT_ASSERT_EQ(sinval_passes, 0);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
}
UT_TEST(test_sinval_actual_main_error_and_invalid_do_not_recover_to_clean)
{
	run_actual_sinval_main(2);
	UT_ASSERT_EQ(main_exit_code, 1);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_idle_mask), 0);
	run_actual_sinval_main(4);
	UT_ASSERT_EQ(main_exit_code, 1);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_MODULE);
}
UT_TEST(test_sinval_actual_main_rechecks_before_exit)
{
	run_actual_sinval_main(6);
	UT_ASSERT_EQ(main_exit_code, 1);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
	/* A later change after Main's check but before before_shmem_exit must
	 * also make the actual exit nonzero, not merely set shared failure. */
	run_actual_sinval_main(7);
	UT_ASSERT_EQ(main_exit_code, 1);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
}

/* Actual LMS Main/WorkerMain and shutdown helpers; startup, work payloads,
 * module observations and the DATA tick are controlled boundaries. The DATA
 * tick's own wait/dispatch brackets have a separate real-source test. */
static ClusterLmsSharedState lms_fixture;
static ClusterLmsSharedState *cluster_lms_state = &lms_fixture;
static unsigned lms_worker, lms_drains, lms_ticks, lms_closes, lms_polls;
static int lms_scenario;
static bool lms_data_enabled;
static ClusterNormalStopPollResult lms_ic_observation, lms_cr_observation;
int cluster_lms_workers = 8;
int MyProcPid = 2345;
TimestampTz
GetCurrentTimestamp(void)
{
	return fixture_now;
}
#define LMS_IDLE_TIMEOUT_MS 100

static void
lms_sigusr1_handler(SIGNAL_ARGS)
{
	(void)postgres_signal_arg;
}
static void
lms_note_pcm_x_finish_flush_injection_reload(int worker)
{
	(void)worker;
}
void
cluster_lms_apply_nice(void)
{}
bool
cluster_authority_readiness_managed(void)
{
	return false;
}
uint32
cluster_ges_dedup_drop_stale_entries(void)
{
	return 0;
}
void
cluster_lms_bump_restart_generation_at_main_entry(void)
{}
uint64
cluster_lms_get_lms_restart_generation(void)
{
	return 7;
}
static uint64
lms_r4_publish_worker_incarnation(ClusterLmsSharedState *state, int worker)
{
	state->r4_controls.data_worker_incarnation[worker] = 7;
	return 7;
}
static void
lms_set_state(ClusterLmsState state)
{
	pg_atomic_write_u32(&lms_fixture.lms_state, state);
}
static bool
lms_shutdown_requested(void)
{
	return false;
}
static bool
lms_r4_drain_ack_tick(ClusterLmsSharedState *state, uint64 generation)
{
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 1);
	return true;
}
void
cluster_cr_server_publish_lms_latch(Latch *latch)
{
	(void)latch;
}
bool
cluster_lms_data_plane_startup(int worker, int count)
{
	UT_ASSERT_EQ(worker, lms_worker);
	UT_ASSERT_EQ(count, 8);
	return lms_data_enabled;
}
bool
cluster_lms_data_plane_enabled(void)
{
	return lms_data_enabled;
}
bool
cluster_gcs_block_family_on_data_plane(void)
{
	return true;
}
bool
cluster_write_fence_enforcing(void)
{
	return false;
}
bool
cluster_write_fence_allowed(void)
{
	return true;
}

static void
test_lms_work(void)
{
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 1);
	if (cluster_normal_stop_requested())
		UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_active_mask),
					 lms_scenario == 5 && main_waits == 0 ? 0 : UINT32_C(2) << lms_worker);
}
void
cluster_lms_native_probe_retry_tick(void)
{
	test_lms_work();
}
void
cluster_lms_cr_drain(void)
{
	test_lms_work();
}
void
cluster_gcs_block_r4_tx_resolve_drain(void)
{
	test_lms_work();
}
void
cluster_lms_cr_ship_ready(void)
{
	test_lms_work();
}
void
cluster_gcs_block_pi_discard_drain(void)
{
	test_lms_work();
}
void
cluster_gcs_block_invalidate_park_tick(void)
{
	test_lms_work();
}
long
cluster_gcs_block_r4_tx_resolve_wait_timeout(long idle)
{
	return idle;
}
int
cluster_lms_outbound_resource_x_intent_pump(void)
{
	test_lms_work();
	return 0;
}
int
cluster_lms_outbound_drain_send(int worker)
{
	UT_ASSERT_EQ(worker, lms_worker);
	test_lms_work();
	lms_drains++;
	if (lms_scenario == 5 && lms_drains == 1) {
		pg_atomic_write_u32(&test_region.normal_stop.requested, 1);
		UT_ASSERT_EQ(cluster_normal_stop_service_idle(CLUSTER_NORMAL_STOP_READY),
					 CLUSTER_NORMAL_STOP_PENDING);
	}
	if (lms_scenario == 3) {
		lock_holds = 1; /* original module ERROR with a held native lock */
		siglongjmp(*PG_exception_stack, 1);
	}
	return 0;
}
static void
test_lms_poll(void)
{
	UT_ASSERT_EQ(lock_holds, 0);
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 0);
	UT_ASSERT_EQ(lms_closes, 0); /* never inspect transport after close */
	lms_polls++;
}
ClusterNormalStopPollResult
cluster_cr_server_normal_stop_poll(int *slot, const char **reason)
{
	test_lms_poll();
	*slot = 0;
	*reason = "CR_FIXTURE";
	return lms_cr_observation;
}
ClusterNormalStopPollResult
cluster_lms_native_probe_normal_stop_poll(int *slot, const char **reason)
{
	test_lms_poll();
	*slot = 0;
	*reason = "PROBE_FIXTURE";
	return CLUSTER_NORMAL_STOP_READY;
}
ClusterNormalStopPollResult
cluster_gcs_block_normal_stop_local_poll(int *slot, const char **reason)
{
	test_lms_poll();
	*slot = 0;
	*reason = "GCS_FIXTURE";
	return CLUSTER_NORMAL_STOP_READY;
}
ClusterNormalStopPollResult
cluster_lms_outbound_normal_stop_poll(int *worker, uint32 *slot, const char **reason)
{
	test_lms_poll();
	*worker = lms_worker;
	*slot = 0;
	*reason = "OUTBOUND_FIXTURE";
	return CLUSTER_NORMAL_STOP_READY;
}
ClusterNormalStopPollResult
cluster_ic_normal_stop_poll(const char **domain, int *peer, uint32 *sequence, const char **reason)
{
	test_lms_poll();
	*domain = "IC_FIXTURE";
	*peer = 1;
	*sequence = 7;
	*reason = "IC_FIXTURE";
	return lms_ic_observation;
}

static int
test_lms_wait(void)
{
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_active_mask), 0);
	if (cluster_normal_stop_requested())
		UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_idle_mask),
					 lms_ic_observation == CLUSTER_NORMAL_STOP_READY ? UINT32_C(2) << lms_worker
																	 : 0);
	if (main_waits == 1 && lms_scenario == 1)
		lms_ic_observation = CLUSTER_NORMAL_STOP_READY;
	if (main_waits == 2) {
		if (cluster_normal_stop_requested()) {
			pg_atomic_write_u32(&test_region.normal_stop.phase,
								CLUSTER_NORMAL_STOP_PROTOCOL_CLOSED);
			pg_atomic_write_u32(&test_region.normal_stop.service_seal, 2);
		}
		if (lms_scenario == 6)
			lms_ic_observation = CLUSTER_NORMAL_STOP_PENDING;
		ShutdownRequestPending = true;
	}
	return WL_LATCH_SET;
}
void
cluster_lms_data_plane_tick(long timeout)
{
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 0);
	lms_ticks++;
	(void)WaitLatch(MyLatch, WL_LATCH_SET, timeout, WAIT_EVENT_PG_SLEEP);
}
void
cluster_lms_data_plane_shutdown(void)
{
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 0);
	if (cluster_normal_stop_requested()) {
		UT_ASSERT(cluster_normal_stop_protocol_closed());
		UT_ASSERT(lms_polls >= 15); /* two passes and the actual last cut */
	}
	lms_closes++;
	if (lms_scenario == 7)
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_MODULE);
}

#define pqsignal test_signal
#define sigprocmask test_sigmask
#include "test_cluster_normal_stop_lms.inc"
#undef pqsignal
#undef sigprocmask

static void
run_actual_lms_main(unsigned worker, int scenario)
{
	sigjmp_buf outer_error;

	seed_drain_boundary();
	MyAuxProcType = worker == 0 ? LmsProcess : (AuxProcType)(LmsWorker1Process + worker - 1);
	lms_main_test = true;
	lms_worker = worker;
	lms_scenario = scenario;
	lms_drains = lms_ticks = lms_closes = lms_polls = 0;
	lms_data_enabled = scenario != 8;
	lms_ic_observation
		= scenario == 1 ? CLUSTER_NORMAL_STOP_PENDING
						: (scenario == 4 ? CLUSTER_NORMAL_STOP_INVALID : CLUSTER_NORMAL_STOP_READY);
	lms_cr_observation = scenario == 4 ? CLUSTER_NORMAL_STOP_PENDING : CLUSTER_NORMAL_STOP_READY;
	MemSet(&lms_fixture, 0, sizeof(lms_fixture));
	fixture_lms_lock = &lms_fixture.lwlock;
	main_passes = main_waits = 0;
	main_exit_code = -1;
	main_early_shutdown = false;
	lms_normal_stop_exit_verified = false;
	if (scenario == 0 || scenario == 5)
		pg_atomic_write_u32(&test_region.normal_stop.requested, 0);
	ShutdownRequestPending = scenario == 2;
	ConfigReloadPending = InterruptPending = false;
	InterruptHoldoffCount = 0;
	PG_exception_stack = &outer_error;
	error_context_stack = NULL;
	exit_callback = NULL;
	if (sigsetjmp(main_exit, 0) == 0) {
		if (sigsetjmp(outer_error, 0) != 0)
			proc_exit(1);
		if (worker == 0)
			LmsMain();
		else
			LmsWorkerMain(worker);
	}
	PG_exception_stack = NULL;
	lms_main_test = false;
}
UT_TEST(test_lms_actual_both_mains_work_sleep_and_ordinary_runtime)
{
	for (unsigned worker = 0; worker <= 7; worker += 7) {
		run_actual_lms_main(worker, 0);
		UT_ASSERT_EQ(main_exit_code, 0);
		UT_ASSERT_EQ(lms_drains, 2);
		UT_ASSERT_EQ(lms_ticks, 2);
		UT_ASSERT_EQ(lms_closes, 1);
		UT_ASSERT_EQ(lms_polls, 0);
		run_actual_lms_main(worker, 8);
		UT_ASSERT_EQ(main_exit_code, 0);
		UT_ASSERT_EQ(lms_ticks, 0);
		UT_ASSERT_EQ(main_waits, 2);
	}
}
UT_TEST(test_lms_actual_midpass_request_and_owned_pending)
{
	for (unsigned worker = 0; worker <= 7; worker += 7) {
		for (int scenario = 1; scenario <= 5; scenario += 4) {
			run_actual_lms_main(worker, scenario);
			UT_ASSERT_EQ(main_exit_code, 0);
			UT_ASSERT_EQ(lms_closes, 1);
			UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_idle_mask),
						 UINT32_C(2) << worker);
			UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_NONE);
		}
	}
}
UT_TEST(test_lms_actual_premature_exit_never_closes_transport)
{
	for (unsigned worker = 0; worker <= 7; worker += 7) {
		run_actual_lms_main(worker, 2);
		UT_ASSERT_EQ(main_exit_code, 1);
		UT_ASSERT_EQ(lms_closes, 0);
		UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
	}
}
UT_TEST(test_lms_actual_error_and_invalid_cannot_sign_idle)
{
	for (unsigned worker = 0; worker <= 7; worker += 7) {
		for (int scenario = 3; scenario <= 4; scenario++) {
			run_actual_lms_main(worker, scenario);
			UT_ASSERT_EQ(main_exit_code, 1);
			UT_ASSERT_EQ(lms_closes, 0);
			UT_ASSERT_EQ(cl_normal_stop_service_depth, 0);
			UT_ASSERT_EQ(lock_holds, 0);
			UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_idle_mask), 0);
			UT_ASSERT_EQ(cluster_normal_stop_failure(), scenario == 3
															? CLUSTER_NORMAL_STOP_FAILURE_SERVICE
															: CLUSTER_NORMAL_STOP_FAILURE_MODULE);
		}
	}
}
UT_TEST(test_lms_actual_last_cut_and_after_close_failure)
{
	for (unsigned worker = 0; worker <= 7; worker += 7) {
		for (int scenario = 6; scenario <= 7; scenario++) {
			run_actual_lms_main(worker, scenario);
			UT_ASSERT_EQ(main_exit_code, 1);
			UT_ASSERT_EQ(lms_closes, scenario == 6 ? 0 : 1);
			UT_ASSERT(cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE);
		}
	}
}

/* Execute the actual DATA tick separately from the Main fixture above.
 * Only sockets, wait readiness and dispatched payload are boundary inputs;
 * both work brackets and the LMS composite idle check remain production. */
bool actual_data_startup(int worker, int count);
bool actual_data_enabled(void);
void actual_data_tick(long timeout);
void actual_data_shutdown(void);
#define cluster_lms_data_plane_startup actual_data_startup
#define cluster_lms_data_plane_enabled actual_data_enabled
#define cluster_lms_data_plane_tick actual_data_tick
#define cluster_lms_data_plane_shutdown actual_data_shutdown
#include "../../backend/cluster/cluster_lms_data_plane.c"
#undef cluster_lms_data_plane_startup
#undef cluster_lms_data_plane_enabled
#undef cluster_lms_data_plane_tick
#undef cluster_lms_data_plane_shutdown

static int data_scenario;
static unsigned data_waits, data_dispatches, data_drains, data_mutations;
static bool data_tail;
int cluster_node_id;
int cluster_interconnect_heartbeat_interval_ms = 1000;
int cluster_interconnect_connect_timeout_ms = 1000;
uint64
cluster_epoch_get_current(void)
{
	return 9;
}
int
cluster_ic_tier1_my_data_channel(void)
{
	return 7;
}
void
cluster_lms_obs_note_conn_reset(void)
{}
bool
cluster_injection_should_skip(const char *name)
{
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 1);
	if (data_scenario == 4) {
		pg_atomic_write_u32(&test_region.normal_stop.requested, 1);
		UT_ASSERT_EQ(cluster_normal_stop_service_idle(CLUSTER_NORMAL_STOP_READY),
					 CLUSTER_NORMAL_STOP_PENDING);
	}
	if (data_scenario == 6) {
		lock_holds = 1;
		siglongjmp(*PG_exception_stack, 1);
	}
	return false;
}
bool
cluster_ic_tier1_connect_one(int32 peer, int *fd)
{
	*fd = -1;
	return false;
}
bool
cluster_ic_tier1_finish_connect(int32 peer, int fd)
{
	return true;
}
bool
cluster_ic_tier1_accept_one(int *fd, int32 *peer)
{
	*fd = -1;
	*peer = -1;
	return false;
}
bool
cluster_ic_tier1_continue_hello_send(int32 peer, int fd)
{
	return true;
}
int
cluster_ic_tier1_hello_send_remaining(int32 peer)
{
	return 0;
}
bool
cluster_ic_tier1_continue_hello_recv(int slot, int fd, int32 *peer)
{
	*peer = -1;
	return true;
}
void
cluster_ic_tier1_anon_hello_reset(int slot)
{}
void
cluster_ic_tier1_close_peer(int32 peer, const char *reason)
{
	UT_ASSERT(false);
}
bool
cluster_ic_tier1_pending_outbound(int32 peer)
{
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 1);
	return data_tail;
}
bool
cluster_ic_tier1_recv_heartbeat_drain(int32 peer, int fd)
{
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 1);
	UT_ASSERT_EQ(peer, 1);
	data_dispatches++;
	if (data_scenario == 2) {
		lock_holds = 1;
		siglongjmp(*PG_exception_stack, 1);
	}
	if (data_scenario == 3) {
		if (cluster_normal_stop_service_new_work(true))
			data_mutations++;
	} else if (data_scenario == 7)
		lms_ic_observation = CLUSTER_NORMAL_STOP_PENDING;
	return true;
}
ClusterICSendResult
cluster_ic_tier1_drain_outbound(int32 peer)
{
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 1);
	data_drains++;
	data_tail = false;
	lms_ic_observation = CLUSTER_NORMAL_STOP_READY;
	return CLUSTER_IC_SEND_DONE;
}
WaitEventSet *
CreateWaitEventSet(MemoryContext context, int events)
{
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 1);
	return (WaitEventSet *)&dp_wes;
}
void
FreeWaitEventSet(WaitEventSet *set)
{
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 1);
}
int
AddWaitEventToSet(WaitEventSet *set, uint32 events, pgsocket fd, Latch *latch, void *data)
{
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 1);
	return 0;
}
int
WaitEventSetWait(WaitEventSet *set, long timeout, WaitEvent *events, int cap, uint32 info)
{
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 0);
	UT_ASSERT_EQ(lock_holds, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_active_mask), 0);
	data_waits++;
	if (cluster_normal_stop_requested())
		UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_idle_mask) & 256,
					 lms_ic_observation == CLUSTER_NORMAL_STOP_READY ? 256 : 0);
	if (data_scenario == 1)
		return 0;
	if (data_scenario == 3) {
		MyAuxProcType = CheckpointerProcess;
		UT_ASSERT_EQ(cluster_normal_stop_service_seal(2047, 1), CLUSTER_NORMAL_STOP_READY);
		MyAuxProcType = LmsWorker7Process;
	}
	events[0].events = WL_LATCH_SET;
	events[0].user_data = NULL;
	events[1].events = WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE;
	events[1].user_data = (void *)(intptr_t)1;
	return 2;
}
static void
run_actual_data_tick(int scenario)
{
	sigjmp_buf outer_error;

	if (scenario == 3)
		seed_service_cut();
	else
		seed_drain_boundary();
	MyAuxProcType = LmsWorker7Process;
	MyBackendType = B_LMS_WORKER;
	lms_worker = 7;
	lms_main_test = sinval_main_test = false;
	lms_closes = lms_polls = 0;
	lms_ic_observation = scenario == 0 ? CLUSTER_NORMAL_STOP_PENDING : CLUSTER_NORMAL_STOP_READY;
	lms_cr_observation = CLUSTER_NORMAL_STOP_READY;
	data_scenario = scenario;
	data_waits = data_dispatches = data_drains = data_mutations = 0;
	data_tail = scenario == 0;
	for (int i = 0; i < CLUSTER_MAX_NODES; i++) {
		MemSet(&dp_track[i], 0, sizeof(dp_track[i]));
		dp_track[i].fd = -1;
		dp_pending_fds[i] = -1;
		dp_wes_writable[i] = false;
	}
	dp_track[1].fd = 12; /* sentinel only; all socket edges are fixtures */
	dp_track[1].substate = LMS_DP_CONNECTED;
	dp_enabled = true;
	dp_wes = NULL;
	dp_wes_dirty = true;
	if (scenario == 4 || scenario == 5)
		pg_atomic_write_u32(&test_region.normal_stop.requested, 0);
	PG_exception_stack = &outer_error;
	exit_callback = NULL;
	main_exit_code = 0;
	if (sigsetjmp(main_exit, 0) == 0) {
		if (sigsetjmp(outer_error, 0) != 0)
			proc_exit(1);
		actual_data_tick(100);
	}
	PG_exception_stack = NULL;
}
UT_TEST(test_data_actual_tick_two_segments_and_pending_transfer)
{
	run_actual_data_tick(0);
	UT_ASSERT_EQ(main_exit_code, 0);
	UT_ASSERT_EQ(data_waits, 1);
	UT_ASSERT_EQ(data_dispatches, 1);
	UT_ASSERT_EQ(data_drains, 1);
	UT_ASSERT_EQ(lms_polls, 10);
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_idle_mask), 256);
	run_actual_data_tick(7);
	UT_ASSERT_EQ(main_exit_code, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_idle_mask), 0);
}
UT_TEST(test_data_actual_tick_timeout_and_ordinary_do_not_create_debt)
{
	run_actual_data_tick(1);
	UT_ASSERT_EQ(main_exit_code, 0);
	UT_ASSERT_EQ(data_dispatches, 0);
	UT_ASSERT_EQ(lms_polls, 5); /* no event segment after timeout */
	run_actual_data_tick(5);
	UT_ASSERT_EQ(main_exit_code, 0);
	UT_ASSERT_EQ(data_dispatches, 1);
	UT_ASSERT_EQ(lms_polls, 0);
	run_actual_data_tick(4);
	UT_ASSERT_EQ(main_exit_code, 0);
	UT_ASSERT_EQ(lms_polls, 10);
}
UT_TEST(test_data_actual_tick_seal_during_wait_precedes_dispatch)
{
	run_actual_data_tick(3);
	UT_ASSERT_EQ(data_mutations, 0);
	UT_ASSERT_EQ(main_exit_code, 1);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_LATE_UNSEALED_WORK);
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 0);
}
UT_TEST(test_data_actual_tick_both_errors_clear_active_without_idle)
{
	for (int scenario = 2; scenario <= 6; scenario += 4) {
		run_actual_data_tick(scenario);
		UT_ASSERT_EQ(main_exit_code, 1);
		UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
		UT_ASSERT_EQ(lock_holds, 0);
		UT_ASSERT_EQ(cl_normal_stop_service_depth, 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&test_region.normal_stop.service_idle_mask), 0);
	}
}

/* Native formation/WAL and full semantic results are explicit boundaries;
 * the actual leave-lock publication/revalidation code is included above. */
static ClusterSemanticActivationRecord identity_open;
static ClusterFormationSnapshotV1 identity_formation;
static ClusterWalStateSlot identity_wal;
static ClusterWalStateSlot identity_peer_wal[4];
static ClusterNormalStopPollResult identity_read_result;
static ClusterNormalStopPollResult identity_match_result;
static ClusterWalSlotVerdict identity_wal_result;
static unsigned identity_reads;
static unsigned identity_wal_reads, identity_change_on_wal_read;
static unsigned identity_reserve_on_wal_read;
static bool identity_snapshot_ok, identity_quorum, identity_suppressed, identity_pristine;
static uint64 identity_self_incarnation;
static uint16 identity_thread;
bool cluster_enabled;
char *cluster_wal_threads_dir = "/shared-wal";
bool cluster_controlfile_shared_authority = true;
bool cluster_merged_recovery;
static bool native_control_stopped;
static bool native_control_valid = true;
static unsigned native_control_reads;

/* Actual xlog observer is tested separately. Only its native control/IO
 * boundary is supplied here; the real leave owner and protocol execute. */
bool
cluster_native_wal_shutdown_observe(bool stopped, int64 *started_at)
{
	if (lock_holds != 0 || (!AmLmonProcess() && !AmCheckpointerProcess()))
		abort();
	native_control_reads++;
	*started_at = 0;
	if (!native_control_valid || stopped != native_control_stopped)
		return false;
	*started_at = 555;
	return true;
}

bool
cluster_semantic_activation_phase1_pristine(void)
{
	return identity_pristine;
}

bool
cluster_lmon_reconfig_suppressed(void)
{
	if (lock_holds != 0)
		abort();
	return identity_suppressed;
}

bool
cluster_qvotec_in_quorum(void)
{
	if (lock_holds != 0)
		abort();
	return identity_quorum;
}

int
cluster_qvotec_get_quorum_state(void)
{
	return identity_quorum ? CLUSTER_QVOTEC_QUORUM_OK : CLUSTER_QVOTEC_QUORUM_UNCERTAIN;
}

uint64
cluster_qvotec_get_self_incarnation(void)
{
	return identity_self_incarnation;
}

uint16
cluster_wal_thread_id(void)
{
	return identity_thread;
}

bool
cluster_reconfig_capture_formation_snapshot_v1(uint16 thread, ClusterFormationSnapshotV1 *out)
{
	if (lock_holds != 0 || thread != identity_thread)
		abort();
	*out = identity_formation;
	return identity_snapshot_ok;
}

ClusterWalSlotVerdict
cluster_wal_state_read_slot(uint16 thread, ClusterWalStateSlot *out)
{
	if (lock_holds != 0 || thread < 1 || thread > 4)
		abort();
	*out = thread == identity_thread ? identity_wal : identity_peer_wal[thread - 1];
	if (++identity_wal_reads == identity_change_on_wal_read)
		identity_root[80] ^= 1;
	if (identity_wal_reads == identity_reserve_on_wal_read) {
		uint32 expected = 0;
		/* Same original reservation CAS as the operator entry; the rest of
		 * that entry is a boundary here, not an executed leave protocol. */
		if (!pg_atomic_compare_exchange_u32(&cl_state->request_in_progress, &expected, 1))
			abort();
	}
	return identity_wal_result;
}

int cluster_clean_leave_drain_timeout_ms = 30000;
static ClusterICSendResult front_send_result[4];
static unsigned front_sends[4], front_ack_sends;
static ClusterLeaveAnnouncePayload front_last_request[4];
static ClusterLeaveAckPayload front_last_ack[4];
static bool release_reply_on_request, release_receipt_on_reply;

static void
release_peer_message(int peer, uint8 round, uint64 nonce)
{
	ClusterLeaveAnnouncePayload p = { 0 };
	ClusterICEnvelope env = { 0 };
	p.magic = CLUSTER_CLEAN_LEAVE_IC_MAGIC;
	p.version = CLUSTER_CLEAN_LEAVE_IC_VERSION;
	p.leaving_node_id = peer;
	p.preflight = round;
	p.producer_kind = CLUSTER_LEAVE_PRODUCER_SHUTDOWN;
	p.leave_epoch = 9;
	p.leave_nonce = nonce;
	cluster_clean_leave_announce_compute_crc(&p);
	env.msg_type = PGRAC_IC_MSG_CLEAN_LEAVE_ANNOUNCE;
	env.source_node_id = peer;
	env.dest_node_id = cluster_node_id;
	env.epoch = p.leave_epoch;
	env.payload_length = sizeof(p);
	MyAuxProcType = LmonProcess;
	cl_normal_stop_release_announce(&env, &p);
}

static void
release_peer_reply(int peer, uint64 nonce)
{
	ClusterLeaveAckPayload p = { 0 };
	ClusterICEnvelope env = { 0 };
	p.magic = CLUSTER_CLEAN_LEAVE_IC_MAGIC;
	p.version = CLUSTER_CLEAN_LEAVE_IC_VERSION;
	p.survivor_node_id = peer;
	p.leaving_node_id = cluster_node_id;
	p.phase1_round = CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE;
	p.leave_epoch = 9;
	p.leave_nonce = nonce;
	cluster_clean_leave_ack_compute_crc(&p);
	env.msg_type = PGRAC_IC_MSG_LEAVE_DRAIN_ACK;
	env.source_node_id = peer;
	env.dest_node_id = cluster_node_id;
	env.epoch = p.leave_epoch;
	env.payload_length = sizeof(p);
	MyAuxProcType = LmonProcess;
	cl_normal_stop_release_ack(&env, &p);
}

ClusterICSendResult
cluster_ic_send_envelope(uint8 type, int32 destination, const void *payload, uint32 size)
{
	if (lock_holds != 0 || !AmLmonProcess() || destination < 0 || destination >= 4
		|| destination == cluster_node_id)
		abort();
	if (type == PGRAC_IC_MSG_LEAVE_DRAIN_ACK) {
		if (size != sizeof(ClusterLeaveAckPayload)
			|| !cluster_clean_leave_ack_payload_valid(payload))
			abort();
		front_ack_sends++;
		front_last_ack[destination] = *(const ClusterLeaveAckPayload *)payload;
		if (release_receipt_on_reply && front_send_result[destination] == CLUSTER_IC_SEND_DONE
			&& front_last_ack[destination].phase1_round == CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE)
			release_peer_message(destination, CLUSTER_PHASE1_FULL_STOP_WIRE_RECEIPT,
								 front_last_ack[destination].leave_nonce);
	} else if (type != PGRAC_IC_MSG_CLEAN_LEAVE_ANNOUNCE
			   || size != sizeof(ClusterLeaveAnnouncePayload)
			   || !cluster_clean_leave_announce_payload_valid(payload))
		abort();
	else {
		front_sends[destination]++;
		front_last_request[destination] = *(const ClusterLeaveAnnouncePayload *)payload;
		if (release_reply_on_request && front_send_result[destination] == CLUSTER_IC_SEND_DONE
			&& front_last_request[destination].preflight == CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE)
			release_peer_reply(destination, front_last_request[destination].leave_nonce);
	}
	return front_send_result[destination];
}

ClusterNormalStopPollResult
cluster_semantic_normal_stop_read_identity(ClusterSemanticActivationRecord *out,
										   uint8 root[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES],
										   uint64 *incarnations, const char **reason)
{
	if (lock_holds != 0 || !AmLmonProcess())
		abort();
	identity_reads++;
	if (identity_read_result == CLUSTER_NORMAL_STOP_READY) {
		*out = identity_open;
		memcpy(root, identity_root, sizeof(identity_root));
		if (incarnations != NULL)
			memcpy(incarnations, identity_formation.membership.last_admitted_incarnation,
				   4 * sizeof(uint64));
	}
	if (reason != NULL)
		*reason = "FIXTURE_DURABLE_IDENTITY";
	return identity_read_result;
}

ClusterNormalStopPollResult
cluster_semantic_normal_stop_match(const ClusterSemanticActivationRecord *open,
								   const uint8 root[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES],
								   uint64 *incarnations, const char **reason)
{
	int node;
	if (lock_holds != 0)
		abort();
	if (reason != NULL)
		*reason = "FIXTURE_LIVE_IDENTITY";
	if (identity_match_result != CLUSTER_NORMAL_STOP_READY)
		return identity_match_result;
	if (memcmp(open, &identity_open, sizeof(*open)) != 0
		|| memcmp(root, identity_root, sizeof(identity_root)) != 0)
		return CLUSTER_NORMAL_STOP_INVALID;
	/* Full semantic matcher is tested independently with the real capability
	 * invalidation. This fixture composes its result with actual close owners. */
	if (disconnected_terminal_peer >= 0
		&& !cluster_normal_stop_peer_receipt_tail(open, root, disconnected_terminal_peer,
												  UINT64_C(100) + disconnected_terminal_peer))
		return CLUSTER_NORMAL_STOP_PENDING;
	if (incarnations != NULL)
		for (node = 0; node < 4; node++)
			incarnations[node] = UINT64_C(100) + node;
	return CLUSTER_NORMAL_STOP_READY;
}

static void
reset_identity(void)
{
	int node;
	reset_region();
	cluster_clean_leave_shmem_init();
	IsUnderPostmaster = true;
	MyAuxProcType = LmonProcess;
	cluster_node_id = 2;
	disconnected_terminal_peer = -1;
	cluster_enabled = true;
	cluster_wal_threads_dir = "/shared-wal";
	cluster_controlfile_shared_authority = true;
	cluster_merged_recovery = false;
	native_control_stopped = false;
	native_control_valid = true;
	native_control_reads = 0;
	memset(&identity_open, 0, sizeof(identity_open));
	identity_open.phase = CLUSTER_SEMANTIC_PHASE_OPEN;
	identity_open.record_generation = 6;
	identity_open.transition_epoch = 9;
	identity_open.coordinator_incarnation = 100;
	identity_open.source_feature_bitmap = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	identity_open.target_feature_bitmap = identity_open.source_feature_bitmap
										  | CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1;
	identity_open.admitted_members_lo = 15;
	identity_open.capability_sample_digest = 123;
	memset(identity_root, 0x6a, sizeof(identity_root));
	memset(&identity_formation, 0, sizeof(identity_formation));
	identity_formation.local_epoch = identity_open.transition_epoch;
	identity_formation.self_join_admitted = 1;
	for (node = 0; node < 4; node++) {
		identity_formation.membership.membership_state[node] = CLUSTER_MEMBER_MEMBER;
		identity_formation.membership.last_admitted_incarnation[node] = UINT64_C(100) + node;
		memset(&identity_peer_wal[node], 0, sizeof(identity_peer_wal[node]));
		identity_peer_wal[node].node_id = node;
		identity_peer_wal[node].thread_id = node + 1;
		identity_peer_wal[node].state = CLUSTER_WAL_SLOT_STATE_ACTIVE;
		identity_peer_wal[node].started_at = 400 + node;
		front_send_result[node] = CLUSTER_IC_SEND_DONE;
	}
	memset(front_sends, 0, sizeof(front_sends));
	memset(front_last_request, 0, sizeof(front_last_request));
	memset(front_last_ack, 0, sizeof(front_last_ack));
	release_reply_on_request = release_receipt_on_reply = false;
	lms_ic_observation = CLUSTER_NORMAL_STOP_READY;
	front_ack_sends = 0;
	cluster_clean_leave_drain_timeout_ms = 30000;
	identity_formation.victim_incarnation = identity_self_incarnation = 102;
	memset(&identity_wal, 0, sizeof(identity_wal));
	identity_wal.thread_id = identity_thread = 3;
	identity_wal.node_id = 2;
	identity_wal.state = CLUSTER_WAL_SLOT_STATE_ACTIVE;
	identity_wal.started_at = 555;
	identity_wal_result = CLUSTER_WAL_SLOT_OK;
	identity_read_result = identity_match_result = CLUSTER_NORMAL_STOP_READY;
	identity_snapshot_ok = identity_quorum = identity_suppressed = true;
	identity_pristine = false;
	identity_reads = 0;
	identity_wal_reads = identity_change_on_wal_read = 0;
	identity_reserve_on_wal_read = 0;
}

UT_TEST(test_identity_lmon_binds_once_without_request_or_ack)
{
	ClusterPhase1FullStopPlan out;
	ClusterNormalStopState before;
	reset_identity();
	identity_read_result = CLUSTER_NORMAL_STOP_PENDING;
	UT_ASSERT_EQ(cl_normal_stop_identity_poll(false, &out, NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->identity_published), 0);
	identity_read_result = CLUSTER_NORMAL_STOP_READY;
	UT_ASSERT_EQ(cl_normal_stop_identity_poll(false, &out, NULL), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(out.epoch, identity_open.transition_epoch);
	UT_ASSERT_EQ(out.own_wal_started_at, 555);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->identity_published), 1);
	UT_ASSERT_EQ(cl_normal_stop->peer_requests_seen, 0);
	UT_ASSERT(!cluster_normal_stop_requested());
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->phase), CLUSTER_NORMAL_STOP_IDLE);
	UT_ASSERT_EQ(memcmp(&cl_normal_stop->open_record, &identity_open, sizeof(identity_open)), 0);
	before = *cl_normal_stop;
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cl_normal_stop_identity_poll(false, &out, NULL), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(identity_reads, 2);
	UT_ASSERT_EQ(memcmp(&before, cl_normal_stop, sizeof(before)), 0);
	identity_wal.state = CLUSTER_WAL_SLOT_STATE_STOPPED;
	UT_ASSERT_EQ(cl_normal_stop_identity_poll(true, &out, NULL), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(identity_reads, 2);
	UT_ASSERT_EQ(memcmp(&before, cl_normal_stop, sizeof(before)), 0);
}

UT_TEST(test_identity_role_gaps_and_original_pristine_are_separate)
{
	ClusterPhase1FullStopPlan out;
	reset_identity();
	IsUnderPostmaster = false;
	UT_ASSERT_EQ(cl_normal_stop_identity_poll(false, &out, NULL), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(lock_acquisitions, 0);
	IsUnderPostmaster = true;
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cl_normal_stop_identity_poll(false, &out, NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(identity_reads, 0);
	MyAuxProcType = LmonProcess;
	identity_snapshot_ok = false;
	UT_ASSERT_EQ(cl_normal_stop_identity_poll(false, &out, NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_NONE);
	identity_snapshot_ok = true;
	UT_ASSERT(!cl_phase1_full_stop_capture_identity(CLUSTER_WAL_SLOT_STATE_ACTIVE, false, &out));
	UT_ASSERT_EQ(cl_normal_stop_identity_poll(false, &out, NULL), CLUSTER_NORMAL_STOP_READY);
	identity_pristine = true;
	identity_formation.local_epoch = 0;
	UT_ASSERT(cl_phase1_full_stop_capture_identity(CLUSTER_WAL_SLOT_STATE_ACTIVE, false, &out));
}

UT_TEST(test_identity_real_formation_wal_and_published_drift_refuse)
{
	ClusterPhase1FullStopPlan out;
	int fault;
	for (fault = 0; fault < 10; fault++) {
		reset_identity();
		switch (fault) {
		case 0:
			identity_formation.membership.last_admitted_incarnation[1]++;
			break;
		case 1:
			identity_formation.membership.membership_state[4] = CLUSTER_MEMBER_MEMBER;
			break;
		case 2:
			identity_formation.pending_join_bitmap[0] = 1;
			break;
		case 3:
			identity_formation.local_epoch++;
			break;
		case 4:
			identity_self_incarnation++;
			break;
		case 5:
			identity_wal.node_id++;
			break;
		case 6:
			identity_wal.thread_id++;
			break;
		case 7:
			identity_wal.started_at = 0;
			break;
		case 8:
			identity_wal_result = CLUSTER_WAL_SLOT_CORRUPT;
			break;
		case 9:
			identity_formation.self_join_failed = 1;
			break;
		}
		UT_ASSERT_EQ(cl_normal_stop_identity_poll(false, &out, NULL), CLUSTER_NORMAL_STOP_INVALID);
		UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->identity_published), 0);
		UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
		UT_ASSERT_EQ(out.own_wal_started_at, 0);
	}
	reset_identity();
	UT_ASSERT_EQ(cl_normal_stop_identity_poll(false, &out, NULL), CLUSTER_NORMAL_STOP_READY);
	cl_normal_stop->peer_requests_seen = 3;
	cl_normal_stop->peer_request_nonce[1] = 444;
	identity_wal.started_at++;
	UT_ASSERT_EQ(cl_normal_stop_identity_poll(false, &out, NULL), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(cl_normal_stop->own_wal_started_at, 555);
	UT_ASSERT_EQ(cl_normal_stop->peer_requests_seen, 3);
	UT_ASSERT_EQ(cl_normal_stop->peer_request_nonce[1], 444);
	UT_ASSERT_EQ(identity_reads, 1);
	identity_wal.started_at--;
	UT_ASSERT_EQ(cl_normal_stop_identity_poll(false, &out, NULL), CLUSTER_NORMAL_STOP_INVALID);
}

UT_TEST(test_identity_post_publication_recheck_cannot_sign_mixed_root)
{
	ClusterPhase1FullStopPlan out;
	reset_identity();
	/* First leave lock observes unpublished; second publishes. */
	identity_lock_countdown = 2;
	UT_ASSERT_EQ(cl_normal_stop_identity_poll(false, &out, NULL), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->identity_published), 1);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
	UT_ASSERT_EQ(out.own_wal_started_at, 0);
	UT_ASSERT_EQ(lock_holds, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->requested), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->phase), 0);
}

UT_TEST(test_identity_rechecks_semantics_after_last_wal_read)
{
	ClusterPhase1FullStopPlan out;
	reset_identity();
	identity_change_on_wal_read = 4;
	UT_ASSERT_EQ(cl_normal_stop_identity_poll(false, &out, NULL), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
	UT_ASSERT_EQ(out.own_wal_started_at, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->identity_published), 1);
}

UT_TEST(test_identity_final_cut_rejects_new_operator_leave_reservation)
{
	ClusterPhase1FullStopPlan out;
	reset_identity();
	identity_reserve_on_wal_read = 4;
	UT_ASSERT_EQ(cl_normal_stop_identity_poll(false, &out, NULL), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
	UT_ASSERT_EQ(out.own_wal_started_at, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_state->request_in_progress), 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_state->shutdown_driven), 0);
}

static void
front_local_postmaster_cut(bool gone)
{
	IsUnderPostmaster = false;
	MyAuxProcType = NotAnAuxProcess;
	if (!cluster_normal_stop_postmaster_request()
		|| (gone && !cluster_normal_stop_postmaster_frontends_gone()))
		abort();
	IsUnderPostmaster = true;
	MyAuxProcType = CheckpointerProcess;
}

static void
front_peer_request(int peer, uint64 nonce)
{
	ClusterICEnvelope env = { 0 };
	ClusterLeaveAnnouncePayload request = { 0 };
	MyAuxProcType = LmonProcess;
	env.msg_type = PGRAC_IC_MSG_CLEAN_LEAVE_ANNOUNCE;
	env.source_node_id = peer;
	env.dest_node_id = cluster_node_id;
	env.epoch = identity_open.transition_epoch;
	env.payload_length = sizeof(request);
	request.magic = CLUSTER_CLEAN_LEAVE_IC_MAGIC;
	request.version = CLUSTER_CLEAN_LEAVE_IC_VERSION;
	request.leaving_node_id = peer;
	request.preflight = CLUSTER_PHASE1_FULL_STOP_WIRE_BARRIER;
	request.producer_kind = CLUSTER_LEAVE_PRODUCER_SHUTDOWN;
	request.leave_epoch = env.epoch;
	request.leave_nonce = nonce;
	cluster_clean_leave_announce_compute_crc(&request);
	cl_normal_stop_fronts_announce(&env, &request);
}

UT_TEST(test_front_cut_waits_for_real_local_and_all_peer_fronts_not_ack)
{
	ClusterPhase1FullStopPlan out;
	uint64 nonce, deadline;
	reset_identity();
	front_local_postmaster_cut(false);
	UT_ASSERT_EQ(cluster_normal_stop_fronts_poll(&out, NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_state->request_in_progress), 0);
	front_local_postmaster_cut(true);
	identity_read_result = CLUSTER_NORMAL_STOP_PENDING;
	UT_ASSERT_EQ(cluster_normal_stop_fronts_poll(&out, NULL), CLUSTER_NORMAL_STOP_PENDING);
	nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
	deadline = cl_state->barrier_deadline_us;
	UT_ASSERT(nonce != 0);
	UT_ASSERT_EQ(deadline, fixture_now + UINT64_C(30000000));
	UT_ASSERT(!out.valid);
	MyAuxProcType = LmonProcess;
	cl_normal_stop_fronts_lmon_tick();
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->identity_published), 0);
	identity_read_result = CLUSTER_NORMAL_STOP_READY;
	cl_normal_stop_fronts_lmon_tick();
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->identity_published), 1);
	UT_ASSERT_EQ(front_sends[0], 0);
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_fronts_poll(&out, NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->phase), CLUSTER_NORMAL_STOP_WAIT_PEER_FRONTS);
	UT_ASSERT_EQ(cl_normal_stop->peer_requests_seen, 4);
	front_peer_request(0, 1001);
	front_peer_request(1, 1002);
	front_peer_request(3, 1004);
	cl_normal_stop_fronts_lmon_tick();
	UT_ASSERT_EQ(cl_normal_stop->peer_requests_seen, 15);
	UT_ASSERT_EQ(cl_normal_stop->peer_request_sent, 11);
	UT_ASSERT_EQ(front_ack_sends, 0);
	UT_ASSERT_EQ(cl_state->ack_bitmap[0], 0);
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_fronts_poll(&out, NULL), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT(out.valid);
	UT_ASSERT_EQ(out.attempt_nonce, nonce);
	UT_ASSERT_EQ(out.absolute_deadline_us, deadline);
	UT_ASSERT_EQ(out.epoch, 9);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->phase), CLUSTER_NORMAL_STOP_DRAIN);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->cleaner_quiesce_requested), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->service_seal), 0);
	UT_ASSERT_EQ(front_last_request[0].leave_epoch, 9);
	UT_ASSERT_EQ(front_last_request[0].leave_nonce, nonce);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_state->preflight_pending), 0);
	UT_ASSERT(!cluster_normal_stop_protocol_closed());
}

UT_TEST(test_front_early_transport_ownership_survives_identity_gap)
{
	ClusterPhase1FullStopPlan out;
	reset_identity();
	identity_read_result = CLUSTER_NORMAL_STOP_PENDING;
	front_peer_request(1, 8001);
	UT_ASSERT(cl_normal_stop_front_inbox[1].pending);
	cl_normal_stop_fronts_lmon_tick();
	UT_ASSERT(cl_normal_stop_front_inbox[1].pending);
	UT_ASSERT_EQ(cl_normal_stop->peer_requests_seen, 0);
	UT_ASSERT(!cluster_normal_stop_requested());
	front_peer_request(1, 8001);
	identity_read_result = CLUSTER_NORMAL_STOP_READY;
	cl_normal_stop_fronts_lmon_tick();
	UT_ASSERT(!cl_normal_stop_front_inbox[1].pending);
	UT_ASSERT_EQ(cl_normal_stop->peer_request_nonce[1], 8001);
	UT_ASSERT_EQ(cl_normal_stop->peer_requests_seen, 2);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->phase), CLUSTER_NORMAL_STOP_IDLE);
	UT_ASSERT_EQ(front_ack_sends, 0);
	UT_ASSERT_EQ(front_sends[0], 0);
	front_local_postmaster_cut(true);
	UT_ASSERT_EQ(cluster_normal_stop_fronts_poll(&out, NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(cl_normal_stop->peer_requests_seen, 6);
	UT_ASSERT_EQ(cl_normal_stop->peer_request_nonce[1], 8001);
	UT_ASSERT_EQ(identity_reads, 2);
}

UT_TEST(test_front_transport_refusal_retries_only_unadmitted_peer)
{
	ClusterPhase1FullStopPlan out;
	reset_identity();
	front_peer_request(0, 1101);
	front_peer_request(1, 1102);
	front_peer_request(3, 1104);
	cl_normal_stop_fronts_lmon_tick();
	front_local_postmaster_cut(true);
	UT_ASSERT_EQ(cluster_normal_stop_fronts_poll(&out, NULL), CLUSTER_NORMAL_STOP_PENDING);
	front_send_result[1] = CLUSTER_IC_SEND_NOT_ADMITTED;
	MyAuxProcType = LmonProcess;
	cl_normal_stop_fronts_lmon_tick();
	UT_ASSERT_EQ(cl_normal_stop->peer_request_sent, 9);
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_fronts_poll(&out, NULL), CLUSTER_NORMAL_STOP_PENDING);
	front_send_result[1] = CLUSTER_IC_SEND_WOULD_BLOCK;
	MyAuxProcType = LmonProcess;
	cl_normal_stop_fronts_lmon_tick();
	UT_ASSERT_EQ(front_sends[0], 1);
	UT_ASSERT_EQ(front_sends[1], 2);
	UT_ASSERT_EQ(front_sends[3], 1);
	UT_ASSERT_EQ(cl_normal_stop->peer_request_sent, 11);
	cl_normal_stop_fronts_lmon_tick();
	UT_ASSERT_EQ(front_sends[1], 2);
	UT_ASSERT_EQ(front_ack_sends, 0);
}

UT_TEST(test_front_conflicting_nonce_never_replaces_pending_or_bound_request)
{
	int published;
	for (published = 0; published < 2; published++) {
		reset_identity();
		front_peer_request(1, 1201);
		if (published)
			cl_normal_stop_fronts_lmon_tick();
		front_peer_request(1, 1202);
		if (published)
			cl_normal_stop_fronts_lmon_tick();
		UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
		if (published)
			UT_ASSERT_EQ(cl_normal_stop->peer_request_nonce[1], 1201);
		else
			UT_ASSERT_EQ(cl_normal_stop_front_inbox[1].request.leave_nonce, 1201);
		UT_ASSERT_EQ(front_ack_sends, 0);
	}
}

UT_TEST(test_front_fixed_deadline_and_role_failure_cannot_requalify)
{
	ClusterPhase1FullStopPlan out;
	uint64 nonce, deadline;
	reset_identity();
	front_local_postmaster_cut(true);
	MyAuxProcType = LmonProcess;
	UT_ASSERT_EQ(cluster_normal_stop_fronts_poll(&out, NULL), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_state->request_in_progress), 0);
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_fronts_poll(&out, NULL), CLUSTER_NORMAL_STOP_PENDING);
	nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
	deadline = cl_state->barrier_deadline_us;
	fixture_now += 1;
	UT_ASSERT_EQ(cluster_normal_stop_fronts_poll(&out, NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(cl_state->barrier_deadline_us, deadline);
	UT_ASSERT_EQ(pg_atomic_read_u64(&cl_state->leave_attempt_nonce), nonce);
	fixture_now = deadline;
	UT_ASSERT_EQ(cluster_normal_stop_fronts_poll(&out, NULL), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_DEADLINE);
	fixture_now = 1000000;
	UT_ASSERT_EQ(cluster_normal_stop_fronts_poll(&out, NULL), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(cl_state->barrier_deadline_us, deadline);
	UT_ASSERT(!out.valid);
}

UT_TEST(test_front_source_wal_and_root_recheck_precede_publication)
{
	int fault;
	for (fault = 0; fault < 3; fault++) {
		reset_identity();
		front_peer_request(1, 1401);
		if (fault == 0)
			identity_peer_wal[1].node_id = 0;
		else if (fault == 1)
			identity_peer_wal[1].started_at = 0;
		else
			/* Four own-slot reads bind identity; the peer read is fifth. */
			identity_change_on_wal_read = 5;
		cl_normal_stop_fronts_lmon_tick();
		UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
		UT_ASSERT_EQ(cl_normal_stop->peer_requests_seen, 0);
		UT_ASSERT(cl_normal_stop_front_inbox[1].pending);
		UT_ASSERT_EQ(front_ack_sends, 0);
	}
}

static void
front_peer_ack(int peer, uint64 nonce, bool nak)
{
	ClusterICEnvelope env = { 0 };
	ClusterLeaveAckPayload ack = { 0 };
	MyAuxProcType = LmonProcess;
	env.msg_type = nak ? PGRAC_IC_MSG_LEAVE_DRAIN_NAK : PGRAC_IC_MSG_LEAVE_DRAIN_ACK;
	env.source_node_id = peer;
	env.dest_node_id = cluster_node_id;
	env.epoch = identity_open.transition_epoch;
	env.payload_length = sizeof(ack);
	ack.magic = CLUSTER_CLEAN_LEAVE_IC_MAGIC;
	ack.version = CLUSTER_CLEAN_LEAVE_IC_VERSION;
	ack.survivor_node_id = peer;
	ack.leaving_node_id = cluster_node_id;
	ack.leave_epoch = env.epoch;
	ack.leave_nonce = nonce;
	ack.nak = nak;
	ack.nak_reason = nak ? CLUSTER_LEAVE_NAK_NOT_IN_QUORUM : CLUSTER_LEAVE_NAK_NONE;
	cluster_clean_leave_ack_compute_crc(&ack);
	cl_normal_stop_fronts_ack(&env, &ack);
}

UT_TEST(test_front_ack_retained_until_identity_and_peer_request_without_local_drain)
{
	ClusterPhase1FullStopPlan out;
	uint64 nonce;
	reset_identity();
	front_local_postmaster_cut(true);
	UT_ASSERT_EQ(cluster_normal_stop_fronts_poll(&out, NULL), CLUSTER_NORMAL_STOP_PENDING);
	nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
	front_peer_ack(1, nonce, false);
	identity_read_result = CLUSTER_NORMAL_STOP_PENDING;
	cl_normal_stop_fronts_lmon_tick();
	UT_ASSERT(cl_normal_stop_front_inbox[1].ack_pending);
	UT_ASSERT_EQ(cl_state->ack_bitmap[0], 0);
	identity_read_result = CLUSTER_NORMAL_STOP_READY;
	cl_normal_stop_fronts_lmon_tick();
	UT_ASSERT(cl_normal_stop_front_inbox[1].ack_pending);
	UT_ASSERT_EQ(cl_state->ack_bitmap[0], 0);
	front_peer_request(1, 1501);
	cl_normal_stop_fronts_lmon_tick();
	UT_ASSERT(!cl_normal_stop_front_inbox[1].ack_pending);
	UT_ASSERT_EQ(cl_state->ack_bitmap[0], 2);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->phase), CLUSTER_NORMAL_STOP_IDLE);
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_fronts_poll(&out, NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->phase), CLUSTER_NORMAL_STOP_WAIT_PEER_FRONTS);
	UT_ASSERT_EQ(cl_state->ack_bitmap[0], 2);
	UT_ASSERT_EQ(front_ack_sends, 0);
}

UT_TEST(test_front_stale_ack_and_exact_nak_do_not_manufacture_clean)
{
	ClusterPhase1FullStopPlan out;
	uint64 nonce;
	reset_identity();
	front_local_postmaster_cut(true);
	UT_ASSERT_EQ(cluster_normal_stop_fronts_poll(&out, NULL), CLUSTER_NORMAL_STOP_PENDING);
	nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
	front_peer_ack(1, nonce + 1, false);
	UT_ASSERT(!cl_normal_stop_front_inbox[1].ack_pending);
	UT_ASSERT_EQ(cl_state->ack_bitmap[0], 0);
	front_peer_ack(1, nonce, true);
	cl_normal_stop_fronts_lmon_tick();
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_MODULE);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_state->nak_received), 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_state->nak_reason), CLUSTER_LEAVE_NAK_NOT_IN_QUORUM);
	UT_ASSERT_EQ(cl_state->ack_bitmap[0], 0);
	UT_ASSERT_EQ(front_ack_sends, 0);
}

UT_TEST(test_front_ack_send_requires_controller_cut_not_just_early_request)
{
	ClusterPhase1FullStopPlan out;
	int fault;
	/* The not-yet-integrated controller/other workers are explicit inputs
	 * here. This proves only the actual LMON sender's refusal and transfer,
	 * never that a forged phase constitutes a successful module drain. */
	for (fault = 0; fault < 4; fault++) {
		reset_identity();
		front_peer_request(0, 1601);
		front_peer_request(1, 1602);
		front_peer_request(3, 1604);
		cl_normal_stop_fronts_lmon_tick();
		front_local_postmaster_cut(true);
		UT_ASSERT_EQ(cluster_normal_stop_fronts_poll(&out, NULL), CLUSTER_NORMAL_STOP_PENDING);
		MyAuxProcType = LmonProcess;
		cl_normal_stop_fronts_lmon_tick();
		MyAuxProcType = CheckpointerProcess;
		UT_ASSERT_EQ(cluster_normal_stop_fronts_poll(&out, NULL), CLUSTER_NORMAL_STOP_READY);
		pg_atomic_write_u32(&cl_normal_stop->phase, CLUSTER_NORMAL_STOP_WAIT_DRAIN_ACK);
		pg_atomic_write_u32(&cl_normal_stop->cleaner_quiesce_requested, fault == 0 ? 0 : 1);
		pg_atomic_write_u32(&cl_normal_stop->cleaner_quiesced_mask, fault == 1 ? 127 : 255);
		pg_atomic_write_u32(&cl_normal_stop->service_seal, fault == 2 ? 1 : 0);
		MyAuxProcType = LmonProcess;
		cl_normal_stop_fronts_lmon_tick();
		if (fault < 3) {
			UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_STATE);
			UT_ASSERT_EQ(front_ack_sends, 0);
			UT_ASSERT_EQ(cl_normal_stop->peer_reply_sent, 0);
		} else {
			UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_NONE);
			UT_ASSERT_EQ(front_ack_sends, 3);
			UT_ASSERT_EQ(cl_normal_stop->peer_reply_sent, 11);
			UT_ASSERT_EQ(cl_normal_stop->peer_reply_pending, 0);
			cl_normal_stop_fronts_lmon_tick();
			UT_ASSERT_EQ(front_ack_sends, 3);
			UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->phase),
						 CLUSTER_NORMAL_STOP_WAIT_DRAIN_ACK);
			UT_ASSERT(!cluster_normal_stop_protocol_closed());
		}
	}
}

UT_TEST(test_front_stopped_successor_is_retained_not_a_frontend_vote)
{
	reset_identity();
	front_peer_request(1, 1701);
	cl_normal_stop_fronts_lmon_tick();
	identity_peer_wal[1].state = CLUSTER_WAL_SLOT_STATE_STOPPED;
	front_peer_request(1, 1702);
	cl_normal_stop_fronts_lmon_tick();
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_NONE);
	UT_ASSERT(cl_normal_stop_front_inbox[1].pending);
	UT_ASSERT_EQ(cl_normal_stop_front_inbox[1].request.leave_nonce, 1702);
	UT_ASSERT_EQ(cl_normal_stop->peer_request_nonce[1], 1701);
	UT_ASSERT_EQ(cl_normal_stop->peer_requests_seen, 2);
	UT_ASSERT_EQ(front_ack_sends, 0);
}

/* Drive the real pre-cut coordinator. Only native membership/WAL/transport
 * and module observations are fixtures; no phase or ACK bitmap is seeded. */
static void
seed_module_drain(void)
{
	ClusterPhase1FullStopPlan plan;
	reset_identity();
	front_peer_request(0, 1801);
	front_peer_request(1, 1802);
	front_peer_request(3, 1804);
	cl_normal_stop_fronts_lmon_tick();
	front_local_postmaster_cut(true);
	if (cluster_normal_stop_fronts_poll(&plan, NULL) != CLUSTER_NORMAL_STOP_PENDING)
		abort();
	MyAuxProcType = LmonProcess;
	cl_normal_stop_fronts_lmon_tick();
	MyAuxProcType = CheckpointerProcess;
	if (cluster_normal_stop_fronts_poll(&plan, NULL) != CLUSTER_NORMAL_STOP_READY)
		abort();
}

UT_TEST(test_module_census_calls_all_original_owners_with_full_root_and_cut)
{
	ClusterNormalStopModuleObservation observation;
	seed_module_drain();
	UT_ASSERT_EQ(cluster_normal_stop_modules_poll(false, &observation), CLUSTER_NORMAL_STOP_READY);
	for (unsigned index = 0; index < lengthof(module_results); index++)
		UT_ASSERT_EQ(module_calls[index], 1);
	UT_ASSERT_EQ(module_post_calls, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->cleaner_quiesce_requested), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->service_seal), 0);
	UT_ASSERT_EQ(front_ack_sends, 0);
	/* Actual checkpoint/disk boundary is an explicit input in this host test. */
	pg_atomic_write_u32(&cl_normal_stop->phase, CLUSTER_NORMAL_STOP_POST_STOPPED);
	identity_wal.state = CLUSTER_WAL_SLOT_STATE_STOPPED;
	UT_ASSERT_EQ(cluster_normal_stop_modules_poll(true, &observation), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(module_post_calls, 6);
}

UT_TEST(test_module_census_each_pending_retains_exact_cause_without_advancing)
{
	ClusterNormalStopModuleObservation observation;
	for (unsigned index = 0; index < lengthof(module_results); index++) {
		seed_module_drain();
		if (index == 3)
			ctrc_observation = CLUSTER_NORMAL_STOP_PENDING;
		else
			module_results[index] = CLUSTER_NORMAL_STOP_PENDING;
		UT_ASSERT_EQ(cluster_normal_stop_modules_poll(false, &observation),
					 CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT(observation.module != NULL && observation.reason != NULL);
		UT_ASSERT(observation.object[0] != '\0');
		if (index == 1)
			UT_ASSERT(strstr(observation.object, "259") != NULL);
		if (index == 2)
			UT_ASSERT(strstr(observation.object, "515") != NULL);
		if (index == 4)
			UT_ASSERT(strstr(observation.object, "17003") != NULL);
		if (index == 9)
			UT_ASSERT(strstr(observation.object, "16972") != NULL);
		UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_NONE);
		UT_ASSERT_EQ(module_calls[lengthof(module_results) - 1], 1);
		UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->phase), CLUSTER_NORMAL_STOP_DRAIN);
		ctrc_observation = module_results[index] = CLUSTER_NORMAL_STOP_READY;
		UT_ASSERT_EQ(cluster_normal_stop_modules_poll(false, &observation),
					 CLUSTER_NORMAL_STOP_READY);
	}
}

UT_TEST(test_module_invalid_and_unknown_override_earlier_pending_and_stick)
{
	ClusterNormalStopModuleObservation observation;
	for (unsigned fault = 0; fault < 2; fault++) {
		seed_module_drain();
		module_results[0] = CLUSTER_NORMAL_STOP_PENDING;
		module_results[12] = fault ? (ClusterNormalStopPollResult)93 : CLUSTER_NORMAL_STOP_INVALID;
		UT_ASSERT_EQ(cluster_normal_stop_modules_poll(false, &observation),
					 CLUSTER_NORMAL_STOP_INVALID);
		UT_ASSERT(observation.module != NULL && strcmp(observation.module, "HW") == 0);
		UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_MODULE);
		module_results[0] = module_results[12] = CLUSTER_NORMAL_STOP_READY;
		UT_ASSERT_EQ(cluster_normal_stop_modules_poll(false, &observation),
					 CLUSTER_NORMAL_STOP_INVALID);
		UT_ASSERT_EQ(module_calls[0], 1);
	}
}

UT_TEST(test_module_identity_final_recheck_refuses_changed_root_and_wrong_role)
{
	ClusterNormalStopModuleObservation observation;
	seed_module_drain();
	MyAuxProcType = LmonProcess;
	UT_ASSERT_EQ(cluster_normal_stop_modules_poll(false, &observation),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(module_calls[0], 0);
	MyAuxProcType = CheckpointerProcess;
	module_change_root = true;
	UT_ASSERT_EQ(cluster_normal_stop_modules_poll(false, &observation),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(module_calls[12], 1);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
}

static void
idle_original_actors(void)
{
	/* Module READY is the input boundary; the original actor publication and
	 * leave-lock seal execute. Actual Main tests above cover the owner polls. */
	for (unsigned i = 0; i < 11; i++) {
		MyAuxProcType = i == 0	  ? LmonProcess
						: i == 10 ? LmdProcess
						: i == 9  ? SinvalBcastProcess
						: i == 1  ? LmsProcess
								  : (AuxProcType)(LmsWorker1Process + i - 2);
		if (!cluster_normal_stop_service_enter() || !cluster_normal_stop_service_leave(true)
			|| cluster_normal_stop_service_idle(CLUSTER_NORMAL_STOP_READY)
				   != CLUSTER_NORMAL_STOP_READY)
			abort();
	}
	MyAuxProcType = CheckpointerProcess;
}

static void
park_and_idle_original_actors(void)
{
	for (unsigned i = 0; i < 8; i++) {
		MyAuxProcType = ClusterUndoCleanerTypeForWorker(i);
		if (!cluster_normal_stop_cleaner_park())
			abort();
	}
	idle_original_actors();
}

static void
deliver_all_drain_acks(void)
{
	uint64 nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
	front_peer_ack(0, nonce, false);
	front_peer_ack(1, nonce, false);
	front_peer_ack(3, nonce, false);
	cl_normal_stop_fronts_lmon_tick();
	MyAuxProcType = CheckpointerProcess;
}

UT_TEST(test_global_producer_cut_keeps_peer_cleaner_service_open_until_last_ack)
{
	ClusterNormalStopModuleObservation observation;
	ClusterPhase1FullStopPlan plan;
	uint64 nonce;
	bool admitted;

	seed_module_drain();
	idle_original_actors();
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
				 CLUSTER_NORMAL_STOP_PENDING);
	park_and_idle_original_actors();
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->phase), CLUSTER_NORMAL_STOP_WAIT_DRAIN_ACK);
	nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
	front_peer_ack(0, nonce, false);
	front_peer_ack(1, nonce, false);
	cl_normal_stop_fronts_lmon_tick();
	UT_ASSERT_EQ(cl_state->ack_bitmap[0], 3);
	UT_ASSERT_EQ(cl_normal_stop->peer_reply_sent, 11);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->service_seal), 0);
	/* The remaining peer is still cleaning. This is the actual production
	 * actor/new-work guard used by a NEW remote current admission, not a
	 * fabricated duplicate or a replacement table-cleanup verdict. */
	MyAuxProcType = LmsProcess;
	UT_ASSERT(cluster_normal_stop_service_enter());
	admitted = cluster_normal_stop_service_new_work(true);
	UT_ASSERT(admitted);
	UT_ASSERT_EQ(cl_state->ack_bitmap[0], 3);
	if (!admitted)
		return;
	UT_ASSERT(cluster_normal_stop_service_leave(true));
	UT_ASSERT_EQ(cluster_normal_stop_service_idle(CLUSTER_NORMAL_STOP_READY),
				 CLUSTER_NORMAL_STOP_READY);
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(!plan.valid);
	front_peer_ack(3, nonce, false);
	cl_normal_stop_fronts_lmon_tick();
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
				 CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT(plan.valid);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->service_seal), 1);
	MyAuxProcType = LmsProcess;
	UT_ASSERT(cluster_normal_stop_service_enter());
	UT_ASSERT(!cluster_normal_stop_service_new_work(true));
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_LATE_UNSEALED_WORK);
}

UT_TEST(test_data_seal_requires_complete_producer_ack_and_send_cut)
{
	for (int missing = 0; missing < 3; missing++) {
		ClusterNormalStopModuleObservation observation;
		ClusterPhase1FullStopPlan plan;
		seed_module_drain();
		idle_original_actors();
		UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
					 CLUSTER_NORMAL_STOP_PENDING);
		park_and_idle_original_actors();
		UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
					 CLUSTER_NORMAL_STOP_PENDING);
		deliver_all_drain_acks();
		if (missing == 0)
			cl_state->ack_bitmap[0] &= ~8;
		else if (missing == 1)
			cl_normal_stop->peer_reply_sent &= ~8;
		else
			cl_normal_stop->peer_reply_pending |= 8;
		UT_ASSERT_EQ(cluster_normal_stop_service_seal(2047, 1), CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->service_seal), 0);
		UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_NONE);
	}
}

UT_TEST(test_checkpoint_cut_requires_each_owner_park_seal_send_and_ack)
{
	ClusterNormalStopModuleObservation observation;
	ClusterPhase1FullStopPlan plan;
	seed_module_drain();
	idle_original_actors();
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(!plan.valid);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->phase), CLUSTER_NORMAL_STOP_QUIESCE);
	UT_ASSERT_EQ(cleaner_wakes, 1);
	if (pg_atomic_read_u32(&cl_normal_stop->phase) != CLUSTER_NORMAL_STOP_QUIESCE)
		return;
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(front_ack_sends, 0);
	park_and_idle_original_actors();
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->phase), CLUSTER_NORMAL_STOP_WAIT_DRAIN_ACK);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->service_seal), 0);
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(front_ack_sends, 0);
	deliver_all_drain_acks();
	UT_ASSERT_EQ(front_ack_sends, 3);
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
				 CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT(plan.valid);
	UT_ASSERT_EQ(plan.epoch, 9);
	UT_ASSERT_EQ(plan.attempt_nonce, pg_atomic_read_u64(&cl_state->leave_attempt_nonce));
	UT_ASSERT_EQ(plan.absolute_deadline_us, cl_state->barrier_deadline_us);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->phase), CLUSTER_NORMAL_STOP_CHECKPOINT);
	UT_ASSERT(!cluster_normal_stop_protocol_closed());
}

UT_TEST(test_checkpoint_last_lock_cut_refuses_actor_started_after_module_poll)
{
	ClusterNormalStopModuleObservation observation;
	ClusterPhase1FullStopPlan plan;
	seed_module_drain();
	idle_original_actors();
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
				 CLUSTER_NORMAL_STOP_PENDING);
	if (pg_atomic_read_u32(&cl_normal_stop->phase) != CLUSTER_NORMAL_STOP_QUIESCE)
		return;
	park_and_idle_original_actors();
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
				 CLUSTER_NORMAL_STOP_PENDING);
	deliver_all_drain_acks();
	module_late_active = 2;
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(!plan.valid);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->phase), CLUSTER_NORMAL_STOP_WAIT_DRAIN_ACK);
	MyAuxProcType = LmsProcess;
	UT_ASSERT(cluster_normal_stop_service_leave(true));
	UT_ASSERT_EQ(cluster_normal_stop_service_idle(CLUSTER_NORMAL_STOP_READY),
				 CLUSTER_NORMAL_STOP_READY);
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
				 CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_checkpoint_repolls_debt_after_ack_and_never_renews_deadline)
{
	ClusterNormalStopModuleObservation observation;
	ClusterPhase1FullStopPlan plan;
	seed_module_drain();
	idle_original_actors();
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
				 CLUSTER_NORMAL_STOP_PENDING);
	if (pg_atomic_read_u32(&cl_normal_stop->phase) != CLUSTER_NORMAL_STOP_QUIESCE)
		return;
	park_and_idle_original_actors();
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
				 CLUSTER_NORMAL_STOP_PENDING);
	deliver_all_drain_acks();
	module_results[5] = CLUSTER_NORMAL_STOP_PENDING;
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->phase), CLUSTER_NORMAL_STOP_WAIT_DRAIN_ACK);
	UT_ASSERT(observation.module != NULL && strcmp(observation.module, "GCS") == 0);
	module_results[5] = CLUSTER_NORMAL_STOP_READY;
	fixture_now = cl_state->barrier_deadline_us;
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_DEADLINE);
	UT_ASSERT(!plan.valid);
}

UT_TEST(test_checkpoint_cannot_park_cleaners_while_private_actor_has_not_signed_idle)
{
	ClusterNormalStopModuleObservation observation;
	ClusterPhase1FullStopPlan plan;
	seed_module_drain();
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->phase), CLUSTER_NORMAL_STOP_DRAIN);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->cleaner_quiesce_requested), 0);
	UT_ASSERT_EQ(cleaner_wakes, 0);
}

UT_TEST(test_checkpoint_seal_must_recheck_debt_created_after_pre_seal_census)
{
	ClusterNormalStopModuleObservation observation;
	ClusterPhase1FullStopPlan plan;
	seed_module_drain();
	idle_original_actors();
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
				 CLUSTER_NORMAL_STOP_PENDING);
	if (pg_atomic_read_u32(&cl_normal_stop->phase) != CLUSTER_NORMAL_STOP_QUIESCE)
		return;
	park_and_idle_original_actors();
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
				 CLUSTER_NORMAL_STOP_PENDING);
	deliver_all_drain_acks();
	module_late_completed_debt = true;
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->service_seal), 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->phase), CLUSTER_NORMAL_STOP_WAIT_DRAIN_ACK);
	UT_ASSERT_EQ(front_ack_sends, 3);
	UT_ASSERT(observation.module != NULL && strcmp(observation.module, "ACTIVE_WRITE") == 0);
	module_results[0] = CLUSTER_NORMAL_STOP_READY;
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
				 CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->phase), CLUSTER_NORMAL_STOP_CHECKPOINT);
}

UT_TEST(test_producer_ack_rechecks_debt_after_last_cleaner_park)
{
	ClusterNormalStopModuleObservation observation;
	ClusterPhase1FullStopPlan plan;
	seed_module_drain();
	idle_original_actors();
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
				 CLUSTER_NORMAL_STOP_PENDING);
	park_and_idle_original_actors();
	module_late_completed_debt = true;
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->phase), CLUSTER_NORMAL_STOP_QUIESCE);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->service_seal), 0);
	UT_ASSERT_EQ(front_ack_sends, 0);
	UT_ASSERT(observation.module != NULL && strcmp(observation.module, "ACTIVE_WRITE") == 0);
	module_results[0] = CLUSTER_NORMAL_STOP_READY;
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->phase), CLUSTER_NORMAL_STOP_WAIT_DRAIN_ACK);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->service_seal), 0);
}

static void
seed_at_checkpoint(ClusterPhase1FullStopPlan *plan)
{
	ClusterNormalStopModuleObservation observation;
	seed_module_drain();
	idle_original_actors();
	if (cluster_normal_stop_checkpoint_poll(2047, plan, &observation)
		!= CLUSTER_NORMAL_STOP_PENDING)
		abort();
	park_and_idle_original_actors();
	if (cluster_normal_stop_checkpoint_poll(2047, plan, &observation)
		!= CLUSTER_NORMAL_STOP_PENDING)
		abort();
	deliver_all_drain_acks();
	if (cluster_normal_stop_checkpoint_poll(2047, plan, &observation) != CLUSTER_NORMAL_STOP_READY)
		abort();
}

UT_TEST(test_post_checkpoint_arm_uses_exact_stop_identity_and_one_fresh_nonce)
{
	ClusterPhase1FullStopPlan plan, before, armed;
	ClusterNormalStopModuleObservation observation;
	seed_at_checkpoint(&plan);
	before = plan;
	/* The actual WAL producer is outside this unit's native boundary. */
	identity_wal.state = CLUSTER_WAL_SLOT_STATE_STOPPED;
	UT_ASSERT_EQ(cluster_normal_stop_post_checkpoint_arm(&plan, &observation),
				 CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->phase), CLUSTER_NORMAL_STOP_POST_STOPPED);
	UT_ASSERT_EQ(plan.epoch, before.epoch);
	UT_ASSERT(plan.attempt_nonce != before.attempt_nonce);
	UT_ASSERT_EQ(cl_normal_stop->peer_request_nonce[cluster_node_id], before.attempt_nonce);
	UT_ASSERT_EQ(plan.absolute_deadline_us, before.absolute_deadline_us);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_state->preflight_pending), 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_state->preflight_sent), 1);
	UT_ASSERT_EQ(cl_state->ack_bitmap[0], 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->service_seal), 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_state->phase1_release_pending), 0);
	armed = plan;
	fixture_now += 100;
	UT_ASSERT_EQ(cluster_normal_stop_post_checkpoint_arm(&plan, &observation),
				 CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(memcmp(&plan, &armed, sizeof(plan)), 0);
	UT_ASSERT(!cluster_normal_stop_protocol_closed());
}

UT_TEST(test_post_checkpoint_arm_pending_preserves_predecessor_and_peer_ownership)
{
	ClusterPhase1FullStopPlan plan, before;
	ClusterNormalStopModuleObservation observation;
	seed_at_checkpoint(&plan);
	before = plan;
	identity_wal.state = CLUSTER_WAL_SLOT_STATE_STOPPED;
	module_results[9] = CLUSTER_NORMAL_STOP_PENDING;
	UT_ASSERT_EQ(cluster_normal_stop_post_checkpoint_arm(&plan, &observation),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(memcmp(&plan, &before, sizeof(plan)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->phase), CLUSTER_NORMAL_STOP_CHECKPOINT);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_state->preflight_pending), 0);
	UT_ASSERT_EQ(cl_state->ack_bitmap[0], 11);
	module_results[9] = CLUSTER_NORMAL_STOP_READY;
	module_late_active = 2;
	UT_ASSERT_EQ(cluster_normal_stop_post_checkpoint_arm(&plan, &observation),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(memcmp(&plan, &before, sizeof(plan)), 0);
	/* An absent arm body did not enter the fixture actor. */
	if (cl_normal_stop_service_depth == 0)
		return;
	MyAuxProcType = LmsProcess;
	UT_ASSERT(cluster_normal_stop_service_leave(true));
	UT_ASSERT_EQ(cluster_normal_stop_service_idle(CLUSTER_NORMAL_STOP_READY),
				 CLUSTER_NORMAL_STOP_READY);
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_post_checkpoint_arm(&plan, &observation),
				 CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_pi_waits_for_all_checkpoints_not_before_checkpoint_exchange)
{
	ClusterPhase1FullStopPlan plan;
	seed_at_checkpoint(&plan);
	identity_wal.state = CLUSTER_WAL_SLOT_STATE_STOPPED;
	module_pi_pending = true;
	/* The real coordinator used to require PI discard before it could
	 * exchange the very durability proof needed to authorize that discard. */
	UT_ASSERT_EQ(cluster_normal_stop_modules_poll(true, NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(cluster_normal_stop_post_checkpoint_arm(&plan, NULL), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->phase), CLUSTER_NORMAL_STOP_POST_STOPPED);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_state->phase1_release_pending), 0);
	UT_ASSERT(module_pi_pending);
	UT_ASSERT(!cluster_normal_stop_protocol_closed());
	UT_ASSERT(!cluster_normal_stop_pi_retirement_allowed());
	UT_ASSERT_EQ(cluster_normal_stop_close_poll(&plan, NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(module_pi_retire_calls, 0);
	UT_ASSERT(module_pi_pending);
}

UT_TEST(test_post_checkpoint_arm_never_substitutes_active_wal_or_changed_plan)
{
	ClusterPhase1FullStopPlan plan;
	ClusterNormalStopModuleObservation observation;
	for (unsigned fault = 0; fault < 5; fault++) {
		seed_at_checkpoint(&plan);
		identity_wal.state
			= fault == 0 ? CLUSTER_WAL_SLOT_STATE_ACTIVE : CLUSTER_WAL_SLOT_STATE_STOPPED;
		if (fault == 1)
			plan.epoch++;
		if (fault == 2)
			plan.member_incarnations[3]++;
		if (fault == 3)
			plan.attempt_nonce++;
		if (fault == 4)
			plan.absolute_deadline_us++;
		UT_ASSERT_EQ(cluster_normal_stop_post_checkpoint_arm(&plan, &observation),
					 CLUSTER_NORMAL_STOP_INVALID);
		UT_ASSERT(cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE);
		UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->phase), CLUSTER_NORMAL_STOP_CHECKPOINT);
		UT_ASSERT_EQ(pg_atomic_read_u32(&cl_state->preflight_pending), 0);
		UT_ASSERT(!cluster_normal_stop_protocol_closed());
	}
}

UT_TEST(test_checkpoint_roster_cannot_shrink_or_replace_in_same_attempt)
{
	ClusterPhase1FullStopPlan plan;
	ClusterNormalStopModuleObservation observation;
	seed_module_drain();
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, &observation),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(527, &plan, &observation),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
	UT_ASSERT_EQ(cl_normal_stop_expected_services, 2047);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->cleaner_quiesce_requested), 0);
}

UT_TEST(test_post_control_retains_early_successor_until_own_stopped_and_armed)
{
	ClusterPhase1FullStopPlan plan;
	seed_at_checkpoint(&plan);
	identity_peer_wal[1].state = CLUSTER_WAL_SLOT_STATE_STOPPED;
	front_peer_request(1, 2202);
	cl_normal_stop_post_lmon_tick();
	UT_ASSERT(cl_normal_stop_front_inbox[1].pending);
	UT_ASSERT_EQ(front_ack_sends, 3);
	identity_wal.state = CLUSTER_WAL_SLOT_STATE_STOPPED;
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_post_checkpoint_arm(&plan, NULL), CLUSTER_NORMAL_STOP_READY);
	MyAuxProcType = LmonProcess;
	cl_normal_stop_post_lmon_tick();
	UT_ASSERT(!cl_normal_stop_front_inbox[1].pending);
	UT_ASSERT_EQ(cl_state->phase1_release_request_nonce[1], 2202);
	UT_ASSERT_EQ(cl_normal_stop->peer_request_nonce[1], 1802);
	UT_ASSERT_EQ(cl_state->phase1_post_stopped_reply_sent[0], 2);
	UT_ASSERT_EQ(front_ack_sends, 4);
	UT_ASSERT_EQ(cl_phase1_post_stopped_request_sent[0], 11);
	UT_ASSERT_EQ(front_last_request[0].leave_epoch, 9);
	UT_ASSERT_EQ(front_last_request[0].leave_nonce, plan.attempt_nonce);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_state->phase1_release_pending), 0);
	UT_ASSERT(!cluster_normal_stop_protocol_closed());
}

UT_TEST(test_post_control_retries_only_unadmitted_send_before_reply_permission)
{
	ClusterPhase1FullStopPlan plan;
	seed_at_checkpoint(&plan);
	identity_wal.state = CLUSTER_WAL_SLOT_STATE_STOPPED;
	UT_ASSERT_EQ(cluster_normal_stop_post_checkpoint_arm(&plan, NULL), CLUSTER_NORMAL_STOP_READY);
	identity_peer_wal[1].state = CLUSTER_WAL_SLOT_STATE_STOPPED;
	front_peer_request(1, 2302);
	front_send_result[1] = CLUSTER_IC_SEND_NOT_ADMITTED;
	cl_normal_stop_post_lmon_tick();
	UT_ASSERT_EQ(cl_phase1_post_stopped_request_sent[0], 9);
	UT_ASSERT_EQ(front_ack_sends, 3);
	UT_ASSERT_EQ(front_sends[0], 2);
	UT_ASSERT_EQ(front_sends[1], 2);
	front_send_result[1] = CLUSTER_IC_SEND_WOULD_BLOCK;
	cl_normal_stop_post_lmon_tick();
	UT_ASSERT_EQ(cl_phase1_post_stopped_request_sent[0], 11);
	UT_ASSERT_EQ(front_sends[0], 2);
	UT_ASSERT_EQ(front_sends[1], 3);
	UT_ASSERT_EQ(front_sends[3], 2);
	UT_ASSERT_EQ(cl_state->phase1_post_stopped_reply_sent[0], 2);
	UT_ASSERT_EQ(front_ack_sends, 4);
}

UT_TEST(test_post_control_old_active_duplicate_never_replaces_retained_successor)
{
	ClusterPhase1FullStopPlan plan;
	seed_at_checkpoint(&plan);
	identity_peer_wal[1].state = CLUSTER_WAL_SLOT_STATE_STOPPED;
	front_peer_request(1, 2402);
	front_peer_request(1, 1802);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_NONE);
	UT_ASSERT(cl_normal_stop_front_inbox[1].pending);
	UT_ASSERT_EQ(cl_normal_stop_front_inbox[1].request.leave_nonce, 2402);
}

UT_TEST(test_post_control_ack_waits_for_peer_stopped_request_and_original_identity)
{
	ClusterPhase1FullStopPlan plan;
	seed_at_checkpoint(&plan);
	identity_wal.state = CLUSTER_WAL_SLOT_STATE_STOPPED;
	UT_ASSERT_EQ(cluster_normal_stop_post_checkpoint_arm(&plan, NULL), CLUSTER_NORMAL_STOP_READY);
	identity_peer_wal[1].state = CLUSTER_WAL_SLOT_STATE_STOPPED;
	front_peer_ack(1, plan.attempt_nonce, false);
	cl_normal_stop_post_lmon_tick();
	UT_ASSERT_EQ(cl_state->ack_bitmap[0], 0);
	UT_ASSERT(cl_normal_stop_front_inbox[1].ack_pending);
	front_peer_request(1, 2502);
	cl_normal_stop_post_lmon_tick();
	UT_ASSERT_EQ(cl_state->ack_bitmap[0], 2);
	UT_ASSERT(!cl_normal_stop_front_inbox[1].ack_pending);
	front_peer_request(1, 2503);
	cl_normal_stop_post_lmon_tick();
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
	UT_ASSERT_EQ(cl_state->phase1_release_request_nonce[1], 2502);
	UT_ASSERT(!cluster_normal_stop_protocol_closed());
}

UT_TEST(test_post_control_request_cannot_use_prior_arm_over_current_actor)
{
	ClusterPhase1FullStopPlan plan;
	seed_at_checkpoint(&plan);
	identity_wal.state = CLUSTER_WAL_SLOT_STATE_STOPPED;
	UT_ASSERT_EQ(cluster_normal_stop_post_checkpoint_arm(&plan, NULL), CLUSTER_NORMAL_STOP_READY);
	MyAuxProcType = LmsProcess;
	UT_ASSERT(cluster_normal_stop_service_enter());
	MyAuxProcType = LmonProcess;
	cl_normal_stop_post_lmon_tick();
	UT_ASSERT_EQ(cl_phase1_post_stopped_request_sent[0], 0);
	UT_ASSERT_EQ(front_sends[0], 1);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_NONE);
	MyAuxProcType = LmsProcess;
	UT_ASSERT(cluster_normal_stop_service_leave(true));
	UT_ASSERT_EQ(cluster_normal_stop_service_idle(CLUSTER_NORMAL_STOP_READY),
				 CLUSTER_NORMAL_STOP_READY);
	MyAuxProcType = LmonProcess;
	cl_normal_stop_post_lmon_tick();
	UT_ASSERT_EQ(cl_phase1_post_stopped_request_sent[0], 11);
}

UT_TEST(test_post_control_reply_requires_current_lmon_private_census)
{
	ClusterPhase1FullStopPlan plan;
	seed_at_checkpoint(&plan);
	identity_wal.state = CLUSTER_WAL_SLOT_STATE_STOPPED;
	UT_ASSERT_EQ(cluster_normal_stop_post_checkpoint_arm(&plan, NULL), CLUSTER_NORMAL_STOP_READY);
	MyAuxProcType = LmonProcess;
	cl_normal_stop_post_lmon_tick();
	identity_peer_wal[1].state = CLUSTER_WAL_SLOT_STATE_STOPPED;
	front_peer_request(1, 2602);
	lmon_local_observation = CLUSTER_NORMAL_STOP_PENDING;
	UT_ASSERT(cluster_normal_stop_service_enter());
	cl_normal_stop_post_lmon_tick();
	UT_ASSERT_EQ(cl_state->phase1_post_stopped_reply_sent[0], 0);
	UT_ASSERT_EQ(cl_state->phase1_post_stopped_reply_pending[0], 2);
	UT_ASSERT_EQ(front_ack_sends, 3);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_NONE);
	lmon_local_observation = CLUSTER_NORMAL_STOP_READY;
	cl_normal_stop_post_lmon_tick();
	UT_ASSERT_EQ(cl_state->phase1_post_stopped_reply_sent[0], 2);
	UT_ASSERT(cluster_normal_stop_service_leave(true));
}

UT_TEST(test_post_control_send_cut_rechecks_actor_after_local_census)
{
	ClusterPhase1FullStopPlan plan;
	seed_at_checkpoint(&plan);
	identity_wal.state = CLUSTER_WAL_SLOT_STATE_STOPPED;
	UT_ASSERT_EQ(cluster_normal_stop_post_checkpoint_arm(&plan, NULL), CLUSTER_NORMAL_STOP_READY);
	MyAuxProcType = LmonProcess;
	lmon_late_actor = true;
	cl_normal_stop_post_lmon_tick();
	UT_ASSERT_EQ(cl_phase1_post_stopped_request_sent[0], 0);
	UT_ASSERT_EQ(front_sends[0], 1);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_NONE);
}

static void
seed_post_barrier(ClusterPhase1FullStopPlan *plan)
{
	seed_at_checkpoint(plan);
	identity_wal.state = CLUSTER_WAL_SLOT_STATE_STOPPED;
	UT_ASSERT_EQ(cluster_normal_stop_post_checkpoint_arm(plan, NULL), CLUSTER_NORMAL_STOP_READY);
	for (int peer = 0; peer < 4; peer++)
		if (peer != cluster_node_id) {
			identity_peer_wal[peer].state = CLUSTER_WAL_SLOT_STATE_STOPPED;
			front_peer_request(peer, 3000 + peer);
		}
	cl_normal_stop_post_lmon_tick();
	for (int peer = 0; peer < 4; peer++)
		if (peer != cluster_node_id)
			front_peer_ack(peer, plan->attempt_nonce, false);
	cl_normal_stop_post_lmon_tick();
	UT_ASSERT_EQ(cl_state->ack_bitmap[0], 11);
	UT_ASSERT_EQ(cl_state->phase1_post_stopped_reply_sent[0], 11);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_NONE);
	MyAuxProcType = CheckpointerProcess;
}

static void
seed_release_exchange(ClusterPhase1FullStopPlan *plan)
{
	seed_post_barrier(plan);
	for (int peer = 0; peer < 4; peer++)
		if (peer != cluster_node_id)
			release_peer_message(peer, CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE, 3000 + peer);
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_close_poll(plan, NULL), CLUSTER_NORMAL_STOP_PENDING);
	release_reply_on_request = release_receipt_on_reply = true;
	MyAuxProcType = LmonProcess;
	cl_normal_stop_release_lmon_tick();
}

UT_TEST(test_pi_retirement_requires_complete_exact_post_checkpoint_cut)
{
	ClusterPhase1FullStopPlan plan;
	for (int fault = 0; fault < 17; fault++) {
		seed_post_barrier(&plan);
		UT_ASSERT(cluster_normal_stop_pi_retirement_allowed());
		module_pi_pending = true;
		switch (fault) {
		case 0:
			cl_state->ack_bitmap[0] &= ~2;
			break;
		case 1:
			cl_state->phase1_post_stopped_reply_sent[0] &= ~2;
			break;
		case 2:
			cl_state->phase1_post_stopped_reply_pending[0] = 2;
			break;
		case 3:
			cl_state->phase1_release_request_nonce[1] = cl_normal_stop->peer_request_nonce[1];
			break;
		case 4:
			cl_state->ack_bitmap[1] = 1;
			break;
		case 5:
			pg_atomic_write_u32(&cl_normal_stop->cleaner_quiesced_mask, 127);
			break;
		case 6:
			pg_atomic_write_u32(&cl_normal_stop->frontends_gone, 0);
			break;
		case 7:
			pg_atomic_write_u32(&cl_normal_stop->service_seal, 0);
			break;
		case 8:
			pg_atomic_write_u32(&cl_normal_stop->identity_published, 0);
			break;
		case 9:
			pg_atomic_write_u32(&cl_normal_stop->phase, CLUSTER_NORMAL_STOP_CHECKPOINT);
			break;
		case 10:
			fixture_now = cl_state->barrier_deadline_us;
			break;
		case 11:
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
			break;
		case 12:
			cl_normal_stop->member_incarnations[1] = 0;
			break;
		case 13:
			MyAuxProcType = NotAnAuxProcess;
			break;
		case 14:
			cl_normal_stop->peer_reply_sent &= ~2;
			break;
		case 15:
			pg_atomic_write_u64(&cl_state->leave_attempt_nonce,
								cl_normal_stop->peer_request_nonce[cluster_node_id]);
			break;
		case 16:
			cl_normal_stop->post_checkpoint_deadline_us++;
			break;
		}
		UT_ASSERT(!cluster_normal_stop_pi_retirement_allowed());
		UT_ASSERT(module_pi_pending);
		UT_ASSERT_EQ(module_pi_retire_calls, 0);
	}
	seed_post_barrier(&plan);
	module_pi_pending = true;
	UT_ASSERT_EQ(cluster_normal_stop_modules_poll(true, NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(cluster_normal_stop_close_poll(&plan, NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(module_pi_retire_calls, 2);
	UT_ASSERT(!module_pi_pending);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_state->phase1_release_pending), 1);
	UT_ASSERT(!cluster_normal_stop_protocol_closed());
}

UT_TEST(test_pi_retirement_rejects_changed_identity_or_other_owner)
{
	ClusterPhase1FullStopPlan plan;
	for (int fault = 0; fault < 4; fault++) {
		seed_post_barrier(&plan);
		module_pi_pending = true;
		if (fault == 0)
			identity_root[80] ^= 1;
		else if (fault == 1)
			module_results[9] = CLUSTER_NORMAL_STOP_PENDING; /* real dirty/IO owner */
		else if (fault == 2)
			module_results[4] = CLUSTER_NORMAL_STOP_INVALID;
		else {
			MyAuxProcType = LmsProcess;
			UT_ASSERT(cluster_normal_stop_service_enter());
			MyAuxProcType = CheckpointerProcess;
		}
		UT_ASSERT(cluster_normal_stop_close_poll(&plan, NULL) != CLUSTER_NORMAL_STOP_READY);
		UT_ASSERT_EQ(module_pi_retire_calls, 0);
		UT_ASSERT(module_pi_pending);
		UT_ASSERT_EQ(pg_atomic_read_u32(&cl_state->phase1_release_pending), 0);
	}
}

static void
pi_hint_enter_before_last_checkpoint_ack(void)
{
	MyAuxProcType = LmsProcess;
	UT_ASSERT(cluster_normal_stop_service_enter());
	UT_ASSERT(!cluster_normal_stop_pi_retirement_allowed());
	/* A real in-flight hint has read the old cut; its PI write has not
	 * happened. LMON now consumes the final real post-STOPPED ACK. */
	front_peer_ack(1, pg_atomic_read_u64(&cl_state->leave_attempt_nonce), false);
	cl_normal_stop_post_lmon_tick();
	UT_ASSERT_EQ(cl_state->ack_bitmap[0], 11);
	MyAuxProcType = CheckpointerProcess;
}

UT_TEST(test_pi_actor_idle_sample_must_follow_checkpoint_proof)
{
	ClusterPhase1FullStopPlan plan;
	seed_post_barrier(&plan);
	cl_state->ack_bitmap[0] &= ~2;
	module_pi_pending = true;
	pi_cut_race_after_shared_unlock = pi_hint_enter_before_last_checkpoint_ack;
	UT_ASSERT_EQ(cluster_normal_stop_close_poll(&plan, NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(pi_cut_race_after_shared_unlock == NULL);
	UT_ASSERT_EQ(module_pi_retire_calls, 0);
	UT_ASSERT(module_pi_pending);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_state->phase1_release_pending), 0);
	MyAuxProcType = LmsProcess;
	UT_ASSERT(cluster_normal_stop_service_leave(true));
	UT_ASSERT_EQ(cluster_normal_stop_service_idle(CLUSTER_NORMAL_STOP_READY),
				 CLUSTER_NORMAL_STOP_READY);
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_close_poll(&plan, NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(module_pi_retire_calls, 2);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_state->phase1_release_pending), 1);
}

UT_TEST(test_current_release_codec_not_pristine_authority)
{
	ClusterLeaveAnnouncePayload p = { 0 };
	ClusterLeaveAckPayload ack = { 0 };
	ClusterPhase1FullStopPlan plan;
	seed_post_barrier(&plan);
	p.magic = ack.magic = CLUSTER_CLEAN_LEAVE_IC_MAGIC;
	p.version = ack.version = CLUSTER_CLEAN_LEAVE_IC_VERSION;
	p.leaving_node_id = ack.survivor_node_id = 1;
	ack.leaving_node_id = 2;
	p.leave_epoch = ack.leave_epoch = 9;
	p.leave_nonce = ack.leave_nonce = 3001;
	p.producer_kind = CLUSTER_LEAVE_PRODUCER_SHUTDOWN;
	p.preflight = ack.phase1_round = CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE;
	cluster_clean_leave_announce_compute_crc(&p);
	cluster_clean_leave_ack_compute_crc(&ack);
	UT_ASSERT(cluster_clean_leave_announce_payload_valid(&p));
	UT_ASSERT(cluster_clean_leave_ack_payload_valid(&ack));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_plan_valid(&plan));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_release_probe_accepts(
		p.producer_kind, p.preflight, 1, 1, 9, 9, 3001, true, true, true));
	p.leave_epoch = ack.leave_epoch = UINT64_MAX;
	cluster_clean_leave_announce_compute_crc(&p);
	cluster_clean_leave_ack_compute_crc(&ack);
	UT_ASSERT(!cluster_clean_leave_announce_payload_valid(&p));
	UT_ASSERT(!cluster_clean_leave_ack_payload_valid(&ack));
}

UT_TEST(test_release_arm_waits_for_barrier_and_preserves_early_request)
{
	ClusterPhase1FullStopPlan plan, before;
	seed_at_checkpoint(&plan);
	identity_wal.state = CLUSTER_WAL_SLOT_STATE_STOPPED;
	UT_ASSERT_EQ(cluster_normal_stop_post_checkpoint_arm(&plan, NULL), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(cluster_normal_stop_close_poll(&plan, NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_state->phase1_release_pending), 0);
	seed_post_barrier(&plan);
	before = plan;
	release_peer_message(1, CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE, 3001);
	UT_ASSERT_EQ(cl_state->phase1_release_request_seen[0], 2);
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_close_poll(&plan, NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_state->preflight_pending), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_state->phase1_release_pending), 1);
	UT_ASSERT_EQ(cl_state->phase1_release_request_seen[0], 2);
	UT_ASSERT_EQ(memcmp(&before, &plan, sizeof(plan)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->service_seal), 1);
	UT_ASSERT(!cluster_normal_stop_protocol_closed());
}

UT_TEST(test_release_real_send_and_early_receipt_require_six_complete_legs)
{
	ClusterPhase1FullStopPlan plan;
	seed_release_exchange(&plan);
	UT_ASSERT_EQ(cl_state->phase1_release_request_sent[0], 11);
	UT_ASSERT_EQ(cl_state->phase1_release_request_seen[0], 11);
	UT_ASSERT_EQ(cl_state->phase1_release_reply_sent[0], 11);
	UT_ASSERT_EQ(cl_state->phase1_release_reply_seen[0], 11);
	UT_ASSERT_EQ(cl_state->phase1_release_receipt_sent[0], 11);
	UT_ASSERT_EQ(cl_state->phase1_release_receipt_seen[0], 11);
	UT_ASSERT_EQ(front_last_request[1].preflight, CLUSTER_PHASE1_FULL_STOP_WIRE_RECEIPT);
	UT_ASSERT_EQ(front_last_request[1].leave_epoch, 9);
	UT_ASSERT_EQ(front_last_ack[1].phase1_round, CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE);
	UT_ASSERT_EQ(front_last_ack[1].leave_epoch, 9);
	UT_ASSERT_EQ(front_last_ack[1].leave_nonce, 3001);
	UT_ASSERT(!cluster_normal_stop_protocol_closed());
}

UT_TEST(test_release_unadmitted_leg_and_real_transport_tail_cannot_close)
{
	ClusterPhase1FullStopPlan plan;
	seed_post_barrier(&plan);
	for (int peer = 0; peer < 4; peer++)
		if (peer != cluster_node_id)
			release_peer_message(peer, CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE, 3000 + peer);
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_close_poll(&plan, NULL), CLUSTER_NORMAL_STOP_PENDING);
	front_send_result[1] = CLUSTER_IC_SEND_NOT_ADMITTED;
	release_reply_on_request = release_receipt_on_reply = true;
	lms_ic_observation = CLUSTER_NORMAL_STOP_PENDING;
	MyAuxProcType = LmonProcess;
	cl_normal_stop_release_lmon_tick();
	UT_ASSERT_EQ(cl_state->phase1_release_request_sent[0], 9);
	UT_ASSERT_EQ(cl_state->phase1_release_reply_sent[0], 9);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_state->phase1_release_transport_drained), 0);
	front_send_result[1] = CLUSTER_IC_SEND_DONE;
	cl_normal_stop_release_lmon_tick();
	UT_ASSERT_EQ(cl_state->phase1_release_request_sent[0], 11);
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_close_poll(&plan, NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->service_seal), 1);
	lms_ic_observation = CLUSTER_NORMAL_STOP_READY;
	MyAuxProcType = LmonProcess;
	cl_normal_stop_release_lmon_tick();
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_state->phase1_release_transport_drained), 1);
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_close_poll(&plan, NULL), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->service_seal), 2);
	UT_ASSERT(cluster_normal_stop_protocol_closed());
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->qvotec_clear_result), 0);
}

UT_TEST(test_release_final_cut_rechecks_modules_actor_identity_and_full_bitmap)
{
	ClusterPhase1FullStopPlan plan;
	for (unsigned fault = 0; fault < 5; fault++) {
		seed_release_exchange(&plan);
		MyAuxProcType = CheckpointerProcess;
		if (fault == 0)
			module_results[9] = CLUSTER_NORMAL_STOP_PENDING;
		if (fault == 1)
			module_late_active = 2;
		if (fault == 2)
			identity_root[100] ^= 1;
		if (fault == 3)
			cl_state->phase1_release_reply_seen[1] = 1;
		if (fault == 4)
			fixture_now = plan.absolute_deadline_us;
		UT_ASSERT_EQ(cluster_normal_stop_close_poll(&plan, NULL),
					 fault < 2 ? CLUSTER_NORMAL_STOP_PENDING : CLUSTER_NORMAL_STOP_INVALID);
		UT_ASSERT(!cluster_normal_stop_protocol_closed());
		UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->service_seal), 1);
	}
}

UT_TEST(test_release_identity_contradiction_and_unsolicited_receipt_never_advance)
{
	ClusterPhase1FullStopPlan plan;
	for (unsigned fault = 0; fault < 5; fault++) {
		seed_post_barrier(&plan);
		if (fault == 1)
			identity_peer_wal[1].state = CLUSTER_WAL_SLOT_STATE_ACTIVE;
		if (fault == 2)
			fixture_now = plan.absolute_deadline_us;
		if (fault == 3)
			identity_root[100] ^= 1;
		release_peer_message(1,
							 fault == 4 ? CLUSTER_PHASE1_FULL_STOP_WIRE_RECEIPT
										: CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE,
							 fault == 0 ? 3002 : 3001);
		UT_ASSERT(cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE);
		UT_ASSERT_EQ(cl_state->phase1_release_request_seen[0], 0);
		UT_ASSERT_EQ(cl_state->phase1_release_receipt_seen[0], 0);
		UT_ASSERT_EQ(cl_state->phase1_release_request_nonce[1], 3001);
	}
}

UT_TEST(test_release_exact_completed_control_replay_cannot_reopen_pending)
{
	ClusterPhase1FullStopPlan plan;
	seed_release_exchange(&plan);
	front_peer_request(1, 3001);
	front_peer_ack(1, plan.attempt_nonce, false);
	UT_ASSERT(!cl_normal_stop_front_inbox[1].pending);
	UT_ASSERT(!cl_normal_stop_front_inbox[1].ack_pending);
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_close_poll(&plan, NULL), CLUSTER_NORMAL_STOP_READY);
	release_peer_message(1, CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE, 3001);
	release_peer_message(1, CLUSTER_PHASE1_FULL_STOP_WIRE_RECEIPT, 3001);
	release_peer_reply(1, plan.attempt_nonce);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_NONE);
	UT_ASSERT(cluster_normal_stop_protocol_closed());
	front_peer_request(1, 3002);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
	UT_ASSERT(!cluster_normal_stop_protocol_closed());
}

UT_TEST(test_release_received_frame_survives_temporary_identity_gap)
{
	ClusterPhase1FullStopPlan plan;
	seed_post_barrier(&plan);
	identity_match_result = CLUSTER_NORMAL_STOP_PENDING;
	release_peer_message(1, CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE, 3001);
	UT_ASSERT_EQ(cl_state->phase1_release_request_seen[0], 0);
	UT_ASSERT_EQ(cluster_clean_leave_normal_stop_local_poll(NULL, NULL),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_NONE);
	identity_match_result = CLUSTER_NORMAL_STOP_READY;
	cl_normal_stop_release_lmon_tick();
	UT_ASSERT_EQ(cl_state->phase1_release_request_seen[0], 2);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_state->phase1_release_pending), 0);
	UT_ASSERT_EQ(cluster_clean_leave_normal_stop_local_poll(NULL, NULL), CLUSTER_NORMAL_STOP_READY);
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_close_poll(&plan, NULL), CLUSTER_NORMAL_STOP_PENDING);
	MyAuxProcType = LmonProcess;
	cl_normal_stop_release_lmon_tick();
	identity_match_result = CLUSTER_NORMAL_STOP_PENDING;
	release_peer_reply(1, plan.attempt_nonce);
	release_peer_message(1, CLUSTER_PHASE1_FULL_STOP_WIRE_RECEIPT, 3001);
	UT_ASSERT_EQ(cl_state->phase1_release_reply_seen[0], 0);
	UT_ASSERT_EQ(cl_state->phase1_release_receipt_seen[0], 0);
	identity_match_result = CLUSTER_NORMAL_STOP_READY;
	cl_normal_stop_release_lmon_tick();
	UT_ASSERT_EQ(cl_state->phase1_release_reply_seen[0], 2);
	UT_ASSERT_EQ(cl_state->phase1_release_receipt_seen[0], 2);
	UT_ASSERT_EQ(cl_state->phase1_release_receipt_sent[0], 2);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_NONE);
}

UT_TEST(test_release_pending_identity_frame_cannot_be_overwritten)
{
	ClusterPhase1FullStopPlan plan;
	seed_post_barrier(&plan);
	identity_match_result = CLUSTER_NORMAL_STOP_PENDING;
	release_peer_message(1, CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE, 3001);
	release_peer_message(1, CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE, 3001);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_NONE);
	release_peer_message(1, CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE, 3002);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
	UT_ASSERT_EQ(cl_state->phase1_release_request_seen[0], 0);
}


/* Actual loop callers and controller, native completion driven at WaitLatch.
 * It is an explicit scheduler/I/O fixture, not a real running checkpoint. */
static void
checkpoint_pre_wait(void)
{
	uint32 phase = pg_atomic_read_u32(&cl_normal_stop->phase);
	if (phase == CLUSTER_NORMAL_STOP_QUIESCE)
		park_and_idle_original_actors();
	else if (phase == CLUSTER_NORMAL_STOP_WAIT_DRAIN_ACK)
		deliver_all_drain_acks();
	else
		abort();
}
static void
checkpoint_post_wait(void)
{
	MyAuxProcType = LmonProcess;
	if (pg_atomic_read_u32(&cl_state->phase1_release_pending) == 0) {
		for (int peer = 0; peer < 4; peer++)
			if (peer != cluster_node_id) {
				identity_peer_wal[peer].state = CLUSTER_WAL_SLOT_STATE_STOPPED;
				front_peer_request(peer, 3000 + peer);
				front_peer_ack(peer, pg_atomic_read_u64(&cl_state->leave_attempt_nonce), false);
			}
		cl_normal_stop_post_lmon_tick();
	} else {
		for (int peer = 0; peer < 4; peer++)
			if (peer != cluster_node_id)
				release_peer_message(peer, CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE, 3000 + peer);
		release_reply_on_request = release_receipt_on_reply = true;
		cl_normal_stop_release_lmon_tick();
	}
	idle_original_actors();
}
static void
checkpoint_expire_wait(void)
{
	fixture_now = cl_state->barrier_deadline_us;
}
static void
start_checkpoint_wait(void (*action)(void))
{
	checkpoint_wait_action = action;
	checkpoint_waits = checkpoint_resets = 0;
	checkpoint_previous_timeout = LONG_MAX;
	InterruptPending = false;
}
UT_TEST(test_actual_prepare_wait_drives_original_quiesce_and_ack)
{
	ClusterPhase1FullStopPlan plan = { 0 };
	uint64 deadline, nonce;
	seed_module_drain();
	idle_original_actors();
	deadline = cl_state->barrier_deadline_us;
	nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
	start_checkpoint_wait(checkpoint_pre_wait);
	UT_ASSERT(cluster_normal_stop_checkpoint_prepare(&plan, NULL));
	checkpoint_wait_action = NULL;
	UT_ASSERT_EQ(checkpoint_waits, 2);
	UT_ASSERT_EQ(plan.absolute_deadline_us, deadline);
	UT_ASSERT_EQ(plan.attempt_nonce, nonce);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->phase), CLUSTER_NORMAL_STOP_CHECKPOINT);
	UT_ASSERT(!cluster_normal_stop_protocol_closed());
}
UT_TEST(test_actual_complete_wait_drives_original_post_stop_and_release)
{
	ClusterPhase1FullStopPlan plan;
	uint64 deadline, nonce;
	seed_at_checkpoint(&plan);
	deadline = plan.absolute_deadline_us;
	nonce = plan.attempt_nonce;
	identity_wal.state = CLUSTER_WAL_SLOT_STATE_STOPPED;
	start_checkpoint_wait(checkpoint_post_wait);
	UT_ASSERT(cluster_normal_stop_checkpoint_complete(&plan, NULL));
	checkpoint_wait_action = NULL;
	UT_ASSERT_EQ(checkpoint_waits, 2);
	UT_ASSERT_EQ(plan.absolute_deadline_us, deadline);
	UT_ASSERT(plan.attempt_nonce != nonce);
	UT_ASSERT(cluster_normal_stop_protocol_closed());
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->qvotec_clear_result), 0);
}
UT_TEST(test_completed_checkpoint_gets_once_only_post_budget)
{
	ClusterPhase1FullStopPlan plan;
	uint64 post_start;
	seed_at_checkpoint(&plan);
	fixture_now = plan.absolute_deadline_us + 1;
	post_start = fixture_now;
	identity_wal.state = CLUSTER_WAL_SLOT_STATE_STOPPED;
	start_checkpoint_wait(checkpoint_post_wait);
	UT_ASSERT(cluster_normal_stop_checkpoint_complete(&plan, NULL));
	checkpoint_wait_action = NULL;
	UT_ASSERT_EQ(plan.absolute_deadline_us,
				 post_start + (uint64)cluster_clean_leave_drain_timeout_ms * 1000);
	UT_ASSERT(cluster_normal_stop_protocol_closed());
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->qvotec_clear_result), 0);
}
UT_TEST(test_post_budget_starts_before_pending_owners_and_never_renews)
{
	ClusterPhase1FullStopPlan plan;
	uint64 post_start, deadline;
	seed_at_checkpoint(&plan);
	fixture_now = plan.absolute_deadline_us + 1;
	post_start = fixture_now;
	identity_wal.state = CLUSTER_WAL_SLOT_STATE_STOPPED;
	module_results[9] = CLUSTER_NORMAL_STOP_PENDING;
	UT_ASSERT_EQ(cluster_normal_stop_post_checkpoint_arm(&plan, NULL), CLUSTER_NORMAL_STOP_PENDING);
	deadline = post_start + (uint64)cluster_clean_leave_drain_timeout_ms * 1000;
	UT_ASSERT_EQ(plan.absolute_deadline_us, deadline);
	fixture_now += 500;
	UT_ASSERT_EQ(cluster_normal_stop_post_checkpoint_arm(&plan, NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(plan.absolute_deadline_us, deadline);
	fixture_now = deadline;
	module_results[9] = CLUSTER_NORMAL_STOP_READY;
	UT_ASSERT_EQ(cluster_normal_stop_post_checkpoint_arm(&plan, NULL), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_DEADLINE);
	UT_ASSERT_EQ(plan.absolute_deadline_us, deadline);
	UT_ASSERT(!cluster_normal_stop_protocol_closed());
}
UT_TEST(test_actual_wait_deadline_is_fixed_not_new_budget)
{
	ClusterPhase1FullStopPlan plan;
	uint64 deadline, nonce;
	seed_module_drain();
	idle_original_actors();
	deadline = cl_state->barrier_deadline_us;
	nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
	start_checkpoint_wait(checkpoint_expire_wait);
	UT_ASSERT(!cluster_normal_stop_checkpoint_prepare(&plan, NULL));
	checkpoint_wait_action = NULL;
	UT_ASSERT_EQ(checkpoint_waits, 1);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_DEADLINE);
	UT_ASSERT_EQ(cl_state->barrier_deadline_us, deadline);
	UT_ASSERT_EQ(pg_atomic_read_u64(&cl_state->leave_attempt_nonce), nonce);
}
UT_TEST(test_actual_wait_invalid_owner_does_not_wait_or_requalify)
{
	ClusterNormalStopModuleObservation observation = { 0 };
	ClusterPhase1FullStopPlan plan;
	seed_module_drain();
	idle_original_actors();
	module_results[12] = CLUSTER_NORMAL_STOP_INVALID;
	start_checkpoint_wait(checkpoint_expire_wait);
	UT_ASSERT(!cluster_normal_stop_checkpoint_prepare(&plan, &observation));
	checkpoint_wait_action = NULL;
	UT_ASSERT_EQ(checkpoint_waits, 0);
	UT_ASSERT(observation.module != NULL && strcmp(observation.module, "HW") == 0);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_MODULE);
}
UT_TEST(test_actual_wait_wrong_actor_cannot_touch_close_state)
{
	ClusterPhase1FullStopPlan plan = { 0 };
	unsigned before;
	seed_module_drain();
	before = lock_acquisitions;
	IsUnderPostmaster = false;
	UT_ASSERT(!cluster_normal_stop_checkpoint_prepare(&plan, NULL));
	UT_ASSERT(!cluster_normal_stop_checkpoint_complete(&plan, NULL));
	UT_ASSERT_EQ(lock_acquisitions, before);
	IsUnderPostmaster = true;
}
UT_TEST(test_original_idle_and_park_wake_checkpointer_without_new_timer)
{
	PROC_HDR processes = { 0 };
	Latch latch = { 0 };
	ClusterPhase1FullStopPlan plan;
	seed_module_drain();
	idle_original_actors();
	ProcGlobal = &processes;
	processes.checkpointerLatch = &latch;
	checkpoint_wakes = 0;
	MyAuxProcType = LmonProcess;
	UT_ASSERT_EQ(cluster_normal_stop_service_idle(CLUSTER_NORMAL_STOP_PENDING),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(checkpoint_wakes, 1);
	UT_ASSERT_EQ(cluster_normal_stop_service_idle(CLUSTER_NORMAL_STOP_READY),
				 CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(checkpoint_wakes, 2);
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_checkpoint_poll(2047, &plan, NULL),
				 CLUSTER_NORMAL_STOP_PENDING);
	checkpoint_wakes = 0;
	MyAuxProcType = UndoCleanerProcess;
	UT_ASSERT(cluster_normal_stop_cleaner_park());
	UT_ASSERT_EQ(checkpoint_wakes, 1);
	ProcGlobal = NULL;
}

static void
use_native_wal_topology(void)
{
	cluster_wal_threads_dir = "";
	cluster_controlfile_shared_authority = false;
	cluster_merged_recovery = false;
	identity_wal_result = CLUSTER_WAL_SLOT_EMPTY;
}

UT_TEST(test_native_wal_identity_uses_own_control_not_absent_registry)
{
	ClusterPhase1FullStopPlan plan;
	reset_identity();
	use_native_wal_topology();
	UT_ASSERT_EQ(cl_normal_stop_identity_poll(false, &plan, NULL), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(plan.own_wal_started_at, 555);
	UT_ASSERT(native_control_reads > 0);
	UT_ASSERT_EQ(identity_wal_reads, 0);
	UT_ASSERT(!cluster_normal_stop_requested());
	UT_ASSERT(!cluster_normal_stop_protocol_closed());
}

UT_TEST(test_native_wal_never_repairs_configured_registry_or_invalid_control)
{
	ClusterPhase1FullStopPlan plan;
	for (int fault = 0; fault < 4; fault++) {
		reset_identity();
		use_native_wal_topology();
		if (fault == 0)
			cluster_wal_threads_dir = "/configured-but-absent";
		if (fault == 1)
			cluster_controlfile_shared_authority = true;
		if (fault == 2)
			cluster_merged_recovery = true;
		if (fault == 3)
			native_control_valid = false;
		UT_ASSERT_EQ(cl_normal_stop_identity_poll(false, &plan, NULL), CLUSTER_NORMAL_STOP_INVALID);
		UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->identity_published), 0);
	}
}

UT_TEST(test_native_front_request_not_stopped_without_real_drain_ack)
{
	reset_identity();
	use_native_wal_topology();
	front_peer_request(1, 401);
	cl_normal_stop_fronts_lmon_tick();
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_NONE);
	UT_ASSERT_EQ(cl_normal_stop->peer_request_nonce[1], 401);
	UT_ASSERT_EQ(front_ack_sends, 0);
	UT_ASSERT(!cluster_normal_stop_requested());
	front_peer_request(1, 402);
	cl_normal_stop_fronts_lmon_tick();
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
	UT_ASSERT_EQ(cl_normal_stop->peer_request_nonce[1], 401);
	UT_ASSERT_EQ(cl_state->phase1_release_request_nonce[1], 0);
}

UT_TEST(test_native_post_checkpoint_requires_actual_own_stopped_control)
{
	ClusterPhase1FullStopPlan plan;
	seed_at_checkpoint(&plan);
	use_native_wal_topology();
	UT_ASSERT_EQ(cluster_normal_stop_post_checkpoint_arm(&plan, NULL), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->phase), CLUSTER_NORMAL_STOP_CHECKPOINT);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_state->preflight_pending), 0);
	seed_at_checkpoint(&plan);
	use_native_wal_topology();
	native_control_stopped = true;
	UT_ASSERT_EQ(cluster_normal_stop_post_checkpoint_arm(&plan, NULL), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT(!cluster_normal_stop_protocol_closed());
}

UT_TEST(test_native_post_successor_requires_nonce_lineage_and_retains_early_ack)
{
	ClusterPhase1FullStopPlan plan;
	seed_at_checkpoint(&plan);
	use_native_wal_topology();
	native_control_stopped = true;
	UT_ASSERT_EQ(cluster_normal_stop_post_checkpoint_arm(&plan, NULL), CLUSTER_NORMAL_STOP_READY);
	front_peer_ack(1, plan.attempt_nonce, false);
	cl_normal_stop_post_lmon_tick();
	UT_ASSERT(cl_normal_stop_front_inbox[1].ack_pending);
	UT_ASSERT_EQ(cl_state->ack_bitmap[0], 0);
	front_peer_request(1, 4402);
	cl_normal_stop_post_lmon_tick();
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_NONE);
	UT_ASSERT_EQ(cl_state->phase1_release_request_nonce[1], 4402);
	UT_ASSERT(!cl_normal_stop_front_inbox[1].ack_pending);
	front_peer_request(1, 1802); /* exact frontend predecessor remains a duplicate */
	cl_normal_stop_post_lmon_tick();
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_NONE);
	UT_ASSERT_EQ(cl_state->phase1_release_request_nonce[1], 4402);
	front_peer_request(1, 4403);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
}

UT_TEST(test_identity_failure_names_the_first_failed_formation_predicate)
{
	const char *expected[]
		= { "NORMAL_STOP_FORMATION_EPOCH",			 "NORMAL_STOP_FORMATION_PREBUMP",
			"NORMAL_STOP_FORMATION_SELF_JOIN",		 "NORMAL_STOP_FORMATION_APPLIED_EVENT",
			"NORMAL_STOP_FORMATION_PENDING_JOIN",	 "NORMAL_STOP_FORMATION_CLEAN_DEPARTED",
			"NORMAL_STOP_FORMATION_REMOVED",		 "NORMAL_STOP_FORMATION_EXCLUDED",
			"NORMAL_STOP_FORMATION_MEMBER_STATE",	 "NORMAL_STOP_FORMATION_MEMBER_INCARNATION",
			"NORMAL_STOP_FORMATION_OWN_INCARNATION", "NORMAL_STOP_NATIVE_CONTROL_INVALID" };
	for (unsigned bad = 0; bad < lengthof(expected); bad++) {
		ClusterPhase1FullStopPlan plan;
		const char *reason = NULL;
		reset_identity();
		use_native_wal_topology();
		switch (bad) {
		case 0:
			identity_formation.local_epoch++;
			break;
		case 1:
			identity_formation.prebump_sync_active = 1;
			break;
		case 2:
			identity_formation.self_join_admitted = 0;
			break;
		case 3:
			identity_formation.applied.event_id = 1;
			break;
		case 4:
			identity_formation.pending_join_bitmap[0] = 1;
			break;
		case 5:
			identity_formation.clean_departed_bitmap[0] = 1;
			break;
		case 6:
			identity_formation.removed_bitmap[0] = 1;
			break;
		case 7:
			identity_formation.excluded_bitmap[0] = 1;
			break;
		case 8:
			identity_formation.membership.membership_state[1] = CLUSTER_MEMBER_ABSENT;
			break;
		case 9:
			identity_formation.membership.last_admitted_incarnation[1] = 0;
			break;
		case 10:
			identity_self_incarnation++;
			break;
		case 11:
			native_control_valid = false;
			break;
		}
		UT_ASSERT_EQ(cl_normal_stop_identity_poll(false, &plan, &reason),
					 CLUSTER_NORMAL_STOP_INVALID);
		UT_ASSERT(reason != NULL && strcmp(reason, expected[bad]) == 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->identity_published), 0);
	}
}

UT_TEST(test_wait_snapshot_preserves_state_and_exposes_each_wait_edge)
{
	ClusterPhase1FullStopPlan plan;
	ClusterCleanLeaveSharedState before;
	char snapshot[256] = { 0 };
	seed_at_checkpoint(&plan);
	pg_atomic_write_u32(&cl_normal_stop->phase, CLUSTER_NORMAL_STOP_WAIT_DRAIN_ACK);
	pg_atomic_write_u32(&cl_normal_stop->service_active_mask, 1);
	pg_atomic_write_u32(&cl_normal_stop->service_idle_mask, 1538);
	cl_normal_stop_expected_services = 1539;
	cl_normal_stop->peer_reply_sent = 2;
	cl_normal_stop->peer_reply_pending = 9;
	cl_state->ack_bitmap[0] = 1;
	before = test_region;
	UT_ASSERT(cl_normal_stop_wait_snapshot(snapshot, sizeof(snapshot)));
	UT_ASSERT(strstr(snapshot, "phase=4") != NULL);
	UT_ASSERT(strstr(snapshot, "active=1 idle=1538 expected=1539") != NULL);
	UT_ASSERT(strstr(snapshot, "parked=255 seal=1") != NULL);
	UT_ASSERT(strstr(snapshot, "reply_sent=2 reply_pending=9 ack=1") != NULL);
	UT_ASSERT(memcmp(&before, &test_region, sizeof(before)) == 0);
	UT_ASSERT_EQ(lock_holds, 0);
	UT_ASSERT(!cl_normal_stop_wait_snapshot(NULL, 0));
}

UT_TEST(test_terminal_peer_tail_requires_real_five_legs_not_last_receipt)
{
	ClusterPhase1FullStopPlan plan;
	ClusterCleanLeaveSharedState before;
	seed_post_barrier(&plan);
	UT_ASSERT(!cluster_normal_stop_peer_receipt_tail(&identity_open, identity_root, 1, 101));
	for (int peer = 0; peer < 4; peer++)
		if (peer != cluster_node_id)
			release_peer_message(peer, CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE, 3000 + peer);
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_close_poll(&plan, NULL), CLUSTER_NORMAL_STOP_PENDING);
	release_reply_on_request = true;
	release_receipt_on_reply = false;
	MyAuxProcType = LmonProcess;
	cl_normal_stop_release_lmon_tick();
	UT_ASSERT_EQ(cl_state->phase1_release_receipt_seen[0], 0);
	before = test_region;
	UT_ASSERT(cluster_normal_stop_peer_receipt_tail(&identity_open, identity_root, 1, 101));
	UT_ASSERT(memcmp(&before, &test_region, sizeof(before)) == 0);
	UT_ASSERT(!cluster_normal_stop_protocol_closed());
}

UT_TEST(test_terminal_peer_tail_rejects_each_missing_leg_or_changed_binding)
{
	ClusterPhase1FullStopPlan plan;
	ClusterCleanLeaveSharedState before;
	ClusterSemanticActivationRecord open;
	uint8 root[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	int peer;
	uint64 incarnation;
	for (unsigned fault = 0; fault < 22; fault++) {
		seed_release_exchange(&plan);
		open = identity_open;
		memcpy(root, identity_root, sizeof(root));
		peer = 1;
		incarnation = 101;
		switch (fault) {
		case 0:
			cl_state->phase1_release_request_sent[0] &= ~2;
			break;
		case 1:
			cl_state->phase1_release_request_seen[0] &= ~2;
			break;
		case 2:
			cl_state->phase1_release_reply_sent[0] &= ~2;
			break;
		case 3:
			cl_state->phase1_release_reply_seen[0] &= ~2;
			break;
		case 4:
			cl_state->phase1_release_receipt_sent[0] &= ~2;
			break;
		case 5:
			cl_state->phase1_release_receipt_seen[1] = 1;
			break;
		case 6:
			cl_state->phase1_post_stopped_reply_sent[0] &= ~2;
			break;
		case 7:
			cl_state->phase1_post_stopped_reply_pending[0] |= 2;
			break;
		case 8:
			cl_state->ack_bitmap[0] &= ~2;
			break;
		case 9:
			cl_normal_stop->peer_reply_sent &= ~2;
			break;
		case 10:
			cl_state->phase1_release_request_nonce[1] = cl_normal_stop->peer_request_nonce[1];
			break;
		case 11:
			pg_atomic_write_u32(&cl_normal_stop->service_seal, 0);
			break;
		case 12:
			pg_atomic_write_u32(&cl_normal_stop->phase, CLUSTER_NORMAL_STOP_CHECKPOINT);
			break;
		case 13:
			pg_atomic_write_u32(&cl_state->phase1_release_pending, 0);
			break;
		case 14:
			open.record_generation++;
			break;
		case 15:
			root[100] ^= 1;
			break;
		case 16:
			incarnation++;
			break;
		case 17:
			peer = cluster_node_id;
			break;
		case 18:
			MyAuxProcType = LmsProcess;
			break;
		case 19:
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_MODULE);
			break;
		case 20:
			pg_atomic_write_u32(&cl_normal_stop->frontends_gone, 0);
			break;
		case 21:
			cl_state->leaving_node_id = 1;
			break;
		}
		before = test_region;
		UT_ASSERT(!cluster_normal_stop_peer_receipt_tail(&open, root, peer, incarnation));
		UT_ASSERT(memcmp(&before, &test_region, sizeof(before)) == 0);
	}
}

UT_TEST(test_terminal_peer_last_real_receipt_after_disconnect_closes_without_reconnect)
{
	ClusterPhase1FullStopPlan plan;
	seed_post_barrier(&plan);
	for (int peer = 0; peer < 4; peer++)
		if (peer != cluster_node_id)
			release_peer_message(peer, CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE, 3000 + peer);
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_close_poll(&plan, NULL), CLUSTER_NORMAL_STOP_PENDING);
	release_reply_on_request = true;
	release_receipt_on_reply = false;
	MyAuxProcType = LmonProcess;
	cl_normal_stop_release_lmon_tick();
	disconnected_terminal_peer = 3;
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_close_poll(&plan, NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(!cluster_normal_stop_protocol_closed());
	MyAuxProcType = LmonProcess;
	for (int peer = 0; peer < 4; peer++)
		if (peer != cluster_node_id)
			release_peer_message(peer, CLUSTER_PHASE1_FULL_STOP_WIRE_RECEIPT, 3000 + peer);
	UT_ASSERT_EQ(cl_state->phase1_release_receipt_seen[0], 11);
	cl_normal_stop_release_lmon_tick();
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_close_poll(&plan, NULL), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT(cluster_normal_stop_protocol_closed());
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_NONE);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->qvotec_clear_result), 0);
	disconnected_terminal_peer = -1;
}

int
main(void)
{
	UT_PLAN(100);
	UT_RUN(test_actual_region_size_matches_frozen_tail);
	UT_RUN(test_actual_fresh_initializer_initializes_full_tail_once);
	UT_RUN(test_actual_attach_preserves_normal_stop_and_early_peer_state);
	UT_RUN(test_postmaster_only_monotonic_intent_without_lwlock);
	UT_RUN(test_normal_stop_first_failure_is_sticky_even_after_protocol_close);
	UT_RUN(test_only_checkpointer_can_request_park_after_readonly_ctrc_ready);
	UT_RUN(test_workers_sign_only_their_own_bit_after_local_idle);
	UT_RUN(test_busy_or_reborn_worker_cannot_sign_success);
	UT_RUN(test_qvotec_final_result_is_not_protocol_or_quorum_success);
	UT_RUN(test_service_tracks_nested_work_when_request_arrives_mid_pass);
	UT_RUN(test_service_exception_and_invalid_module_never_sign_idle);
	UT_RUN(test_service_dispatch_and_seal_share_one_cut);
	UT_RUN(test_service_post_stopped_seal_and_expected_roster);
	UT_RUN(test_control_seal_projection_uses_actual_second_cut_without_mutation);
	UT_RUN(test_service_owner_and_stage_errors_are_not_authority);
	UT_RUN(test_service_lmd_participation_is_frozen_not_live_pid_dependent);
	UT_RUN(test_actual_main_parks_after_pass_before_fast_continue);
	UT_RUN(test_actual_main_does_not_restart_pass_after_idle_park_request);
	UT_RUN(test_actual_main_early_exit_or_failed_pass_never_signs_clean_shutdown);
	UT_RUN(test_sinval_actual_main_wraps_full_work_not_sleep);
	UT_RUN(test_sinval_actual_main_pending_cannot_sign_idle);
	UT_RUN(test_sinval_actual_main_premature_shutdown_fails);
	UT_RUN(test_sinval_actual_main_error_and_invalid_do_not_recover_to_clean);
	UT_RUN(test_sinval_actual_main_rechecks_before_exit);
	UT_RUN(test_lms_actual_both_mains_work_sleep_and_ordinary_runtime);
	UT_RUN(test_lms_actual_midpass_request_and_owned_pending);
	UT_RUN(test_lms_actual_premature_exit_never_closes_transport);
	UT_RUN(test_lms_actual_error_and_invalid_cannot_sign_idle);
	UT_RUN(test_lms_actual_last_cut_and_after_close_failure);
	UT_RUN(test_data_actual_tick_two_segments_and_pending_transfer);
	UT_RUN(test_data_actual_tick_timeout_and_ordinary_do_not_create_debt);
	UT_RUN(test_data_actual_tick_seal_during_wait_precedes_dispatch);
	UT_RUN(test_data_actual_tick_both_errors_clear_active_without_idle);
	UT_RUN(test_identity_lmon_binds_once_without_request_or_ack);
	UT_RUN(test_identity_role_gaps_and_original_pristine_are_separate);
	UT_RUN(test_identity_real_formation_wal_and_published_drift_refuse);
	UT_RUN(test_identity_post_publication_recheck_cannot_sign_mixed_root);
	UT_RUN(test_identity_rechecks_semantics_after_last_wal_read);
	UT_RUN(test_identity_final_cut_rejects_new_operator_leave_reservation);
	UT_RUN(test_front_cut_waits_for_real_local_and_all_peer_fronts_not_ack);
	UT_RUN(test_front_early_transport_ownership_survives_identity_gap);
	UT_RUN(test_front_transport_refusal_retries_only_unadmitted_peer);
	UT_RUN(test_front_conflicting_nonce_never_replaces_pending_or_bound_request);
	UT_RUN(test_front_fixed_deadline_and_role_failure_cannot_requalify);
	UT_RUN(test_front_source_wal_and_root_recheck_precede_publication);
	UT_RUN(test_front_ack_retained_until_identity_and_peer_request_without_local_drain);
	UT_RUN(test_front_stale_ack_and_exact_nak_do_not_manufacture_clean);
	UT_RUN(test_front_ack_send_requires_controller_cut_not_just_early_request);
	UT_RUN(test_front_stopped_successor_is_retained_not_a_frontend_vote);
	UT_RUN(test_module_census_calls_all_original_owners_with_full_root_and_cut);
	UT_RUN(test_module_census_each_pending_retains_exact_cause_without_advancing);
	UT_RUN(test_module_invalid_and_unknown_override_earlier_pending_and_stick);
	UT_RUN(test_module_identity_final_recheck_refuses_changed_root_and_wrong_role);
	UT_RUN(test_global_producer_cut_keeps_peer_cleaner_service_open_until_last_ack);
	UT_RUN(test_data_seal_requires_complete_producer_ack_and_send_cut);
	UT_RUN(test_checkpoint_cut_requires_each_owner_park_seal_send_and_ack);
	UT_RUN(test_checkpoint_last_lock_cut_refuses_actor_started_after_module_poll);
	UT_RUN(test_checkpoint_repolls_debt_after_ack_and_never_renews_deadline);
	UT_RUN(test_checkpoint_cannot_park_cleaners_while_private_actor_has_not_signed_idle);
	UT_RUN(test_checkpoint_seal_must_recheck_debt_created_after_pre_seal_census);
	UT_RUN(test_producer_ack_rechecks_debt_after_last_cleaner_park);
	UT_RUN(test_post_checkpoint_arm_uses_exact_stop_identity_and_one_fresh_nonce);
	UT_RUN(test_post_checkpoint_arm_pending_preserves_predecessor_and_peer_ownership);
	UT_RUN(test_pi_waits_for_all_checkpoints_not_before_checkpoint_exchange);
	UT_RUN(test_pi_retirement_requires_complete_exact_post_checkpoint_cut);
	UT_RUN(test_pi_retirement_rejects_changed_identity_or_other_owner);
	UT_RUN(test_pi_actor_idle_sample_must_follow_checkpoint_proof);
	UT_RUN(test_post_checkpoint_arm_never_substitutes_active_wal_or_changed_plan);
	UT_RUN(test_checkpoint_roster_cannot_shrink_or_replace_in_same_attempt);
	UT_RUN(test_post_control_retains_early_successor_until_own_stopped_and_armed);
	UT_RUN(test_post_control_retries_only_unadmitted_send_before_reply_permission);
	UT_RUN(test_post_control_old_active_duplicate_never_replaces_retained_successor);
	UT_RUN(test_post_control_ack_waits_for_peer_stopped_request_and_original_identity);
	UT_RUN(test_post_control_request_cannot_use_prior_arm_over_current_actor);
	UT_RUN(test_post_control_reply_requires_current_lmon_private_census);
	UT_RUN(test_post_control_send_cut_rechecks_actor_after_local_census);
	UT_RUN(test_current_release_codec_not_pristine_authority);
	UT_RUN(test_release_arm_waits_for_barrier_and_preserves_early_request);
	UT_RUN(test_release_real_send_and_early_receipt_require_six_complete_legs);
	UT_RUN(test_release_unadmitted_leg_and_real_transport_tail_cannot_close);
	UT_RUN(test_release_final_cut_rechecks_modules_actor_identity_and_full_bitmap);
	UT_RUN(test_release_identity_contradiction_and_unsolicited_receipt_never_advance);
	UT_RUN(test_release_exact_completed_control_replay_cannot_reopen_pending);
	UT_RUN(test_release_received_frame_survives_temporary_identity_gap);
	UT_RUN(test_release_pending_identity_frame_cannot_be_overwritten);
	UT_RUN(test_actual_prepare_wait_drives_original_quiesce_and_ack);
	UT_RUN(test_actual_complete_wait_drives_original_post_stop_and_release);
	UT_RUN(test_completed_checkpoint_gets_once_only_post_budget);
	UT_RUN(test_post_budget_starts_before_pending_owners_and_never_renews);
	UT_RUN(test_actual_wait_deadline_is_fixed_not_new_budget);
	UT_RUN(test_actual_wait_invalid_owner_does_not_wait_or_requalify);
	UT_RUN(test_actual_wait_wrong_actor_cannot_touch_close_state);
	UT_RUN(test_original_idle_and_park_wake_checkpointer_without_new_timer);
	UT_RUN(test_native_wal_identity_uses_own_control_not_absent_registry);
	UT_RUN(test_native_wal_never_repairs_configured_registry_or_invalid_control);
	UT_RUN(test_native_front_request_not_stopped_without_real_drain_ack);
	UT_RUN(test_native_post_checkpoint_requires_actual_own_stopped_control);
	UT_RUN(test_native_post_successor_requires_nonce_lineage_and_retains_early_ack);
	UT_RUN(test_identity_failure_names_the_first_failed_formation_predicate);
	UT_RUN(test_wait_snapshot_preserves_state_and_exposes_each_wait_edge);
	UT_RUN(test_terminal_peer_tail_requires_real_five_legs_not_last_receipt);
	UT_RUN(test_terminal_peer_tail_rejects_each_missing_leg_or_changed_binding);
	UT_RUN(test_terminal_peer_last_real_receipt_after_disconnect_closes_without_reconnect);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
