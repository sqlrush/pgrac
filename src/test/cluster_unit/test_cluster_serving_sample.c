/*-------------------------------------------------------------------------
 * test_cluster_serving_sample.c
 *    One production admission observation through formation, GRD and SERVING.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Clock, lock and process boundaries come from the startup fixture. The
 * QVOTEC sampler, storage publication, formation capture, complete GRD seal
 * predicate and SERVING consumer are production functions. No disk-quorum
 * or GRD success stub supplies their result.
 *-------------------------------------------------------------------------
 */
#define main startup_fixture_main
#define ShmemInitStruct fixture_ShmemInitStruct
#define pg_usleep fixture_pg_usleep
#define cluster_qvotec_in_quorum fixture_in_quorum
#define cluster_qvotec_check_admission fixture_check_admission
#define cluster_reconfig_capture_serving_formation_v1 fixture_capture_serving
#define cluster_grd_recovery_authority_for_admission fixture_grd_admission
#define cluster_grd_recovery_authority_is_current fixture_grd_current
#define cluster_lms_get_lms_restart_generation fixture_lms_generation
#include "test_cluster_startup_phase.c"
#undef cluster_lms_get_lms_restart_generation
#undef cluster_grd_recovery_authority_is_current
#undef cluster_grd_recovery_authority_for_admission
#undef cluster_reconfig_capture_serving_formation_v1
#undef cluster_qvotec_check_admission
#undef cluster_qvotec_in_quorum
#undef pg_usleep
#undef ShmemInitStruct
#undef main

#include <time.h>
#include <unistd.h>
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_replacement_episode.h"
#include "utils/hsearch.h"
#include "access/xlog.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_cf_authority.h"
#include "cluster/cluster_external_fence.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_wal_source.h"
#include "cluster/cluster_write_fence.h"
#include "postmaster/interrupt.h"
#include "postmaster/bgwriter.h"
#include "utils/elog.h"

static ClusterPhaseSharedState sample_phase;
static ClusterReconfigState sample_reconfig;
static ClusterReconfigState *ReconfigShmem = &sample_reconfig;
static ClusterGrdShared sample_grd;
static ClusterGrdShared *cluster_grd_state = &sample_grd;
static HTAB *cluster_grd_entry_htab;
static ClusterStorageQuorumView sample_provider;
static uint64 sample_mono_us;
static unsigned sample_waits;
static bool sample_oversleep;
static unsigned sample_generation_reads;

void *ShmemInitStruct(const char *name, Size size, bool *found);
void pg_usleep(long microsec);
uint64 cluster_lms_get_lms_restart_generation(void);
bool cluster_qvotec_check_admission(ClusterQvotecAdmissionCheck *out);
bool cluster_qvotec_in_quorum(void);
bool cluster_grd_recovery_authority_is_current(uint64 boot, uint64 generation);
bool cluster_grd_recovery_authority_for_admission(uint64 boot, uint64 generation,
												  const ClusterQvotecAdmissionCheck *check,
												  bool *pending);
ClusterServingFormationResult cluster_reconfig_capture_serving_formation_v1(
	uint16 origin, const ClusterQvotecAdmissionCheck *check, ClusterFormationSnapshotV1 *out,
	bool *valid, const char **predicate);
static int sample_clock_gettime(clockid_t clock, struct timespec *out);

uint64
cluster_lms_get_lms_restart_generation(void)
{
	sample_generation_reads++;
	return phase_test_lms_generation;
}

void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	UT_ASSERT_EQ(size, sizeof(sample_phase));
	*found = false;
	memset(&sample_phase, 0, sizeof(sample_phase));
	return &sample_phase;
}

void
pg_usleep(long microsec)
{
	sample_waits++;
	sample_mono_us += sample_oversleep ? 1500 : microsec;
}

static int
sample_clock_gettime(clockid_t clock, struct timespec *out)
{
	UT_ASSERT_EQ(clock, CLOCK_MONOTONIC);
	out->tv_sec = sample_mono_us / 1000000;
	out->tv_nsec = (sample_mono_us % 1000000) * 1000;
	return 0;
}

#define clock_gettime sample_clock_gettime
#include STORAGE_QUORUM_SOURCE_PATH
#undef clock_gettime

void
cluster_storage_corosync_sample(ClusterStorageQuorumView *out)
{
	*out = sample_provider;
}

void
cluster_qvotec_diagnostic_format(char *out, size_t size)
{
	if (size != 0)
		out[0] = '\0';
}

const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node)
{
	static ClusterNodeInfo declared[4];

	return node >= 0 && node < 4 ? &declared[node] : NULL;
}

#include "test_cluster_serving_sample.inc"

/* These are transport boundaries only; admission below runs through the
 * production QVOTEC, formation, GRD and SERVING functions above. */
#include "cluster/cluster_ic_chunk.h"
#include "cluster/cluster_ic_rdma.h"
#include "cluster/cluster_ic_router.h"
#undef HAVE_LIBIBVERBS
#undef HAVE_LIBRDMACM
#undef HAVE_RDMA_RDMA_CMA_H
static unsigned sample_sends;
static unsigned sample_releases;
int cluster_interconnect_payload_max_bytes = PGRAC_IC_PAYLOAD_MAX_DEFAULT;
static void rdma_release_sge_callbacks(const ClusterICSge *sge, int count);

void *
palloc(Size bytes)
{
	return malloc(bytes);
}
void
pfree(void *allocation)
{
	free(allocation);
}
const ClusterICMsgTypeInfo *
cluster_ic_get_msg_type_info(uint8 type)
{
	static const ClusterICMsgTypeInfo info = { .msg_type = 8,
											   .name = "serving-send",
											   .allowed_producer_mask = (1u << B_INVALID),
											   .plane = CLUSTER_IC_PLANE_DATA };
	return &info;
}

bool
cluster_ic_envelope_build(ClusterICEnvelope *env, uint8 type, uint32 source, uint32 destination,
						  const void *payload, uint32 bytes)
{
	memset(env, 0, sizeof(*env));
	return true;
}
bool
cluster_ic_rdma_block_sge_supported(const char **reason)
{
	return false;
}
ClusterICPeerTransport
cluster_ic_mux_peer_transport(int32 peer)
{
	return CLUSTER_IC_PEER_TRANSPORT_TCP;
}
void
cluster_ic_rdma_stats_note_fallback(int32 peer, const char *reason)
{}
static uint32
rdma_compute_sge_crc(ClusterICEnvelope *env, const ClusterICSge *sge, int count)
{
	return 1;
}
static ClusterICSendResult
rdma_send_envelope_sge_fallback(const ClusterICEnvelope *env, int32 peer, const ClusterICSge *sge,
								int count, uint32 bytes)
{
	sample_sends++;
	rdma_release_sge_callbacks(sge, count);
	return CLUSTER_IC_SEND_DONE;
}
ClusterICSendResult
cluster_ic_send_envelope(uint8 type, int32 peer, const void *payload, uint32 bytes)
{
	sample_sends++;
	return CLUSTER_IC_SEND_DONE;
}
static void
sample_release(void *arg)
{
	sample_releases++;
}
#include "test_cluster_serving_send.inc"

/* Execute the real Prepare and runtime gate. Only CF ownership, the selected
 * immutable file contents, WAL ref, and scheduler are fixture boundaries;
 * test_cluster_control_root covers the actual ROOT/claim/anchor file reads. */
static ClusterControlRootResult runtime_v2_owner_check(uint64 epoch, uint64 incarnation,
													   bool admitted);
static ClusterWalSourceRef sample_ref;
static unsigned sample_cf_reads, sample_cf_releases, sample_checkpoint_waits;
static bool sample_loss_on_wait;
static Latch sample_latch;
Latch *MyLatch = &sample_latch;
static LWLockPadded sample_locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = sample_locks;
volatile sig_atomic_t InterruptPending, ShutdownRequestPending;
volatile uint32 InterruptHoldoffCount, QueryCancelHoldoffCount;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

bool
cluster_wal_thread_dir_validated(void)
{
	return true;
}
bool
cluster_write_fence_allowed(void)
{
	return true;
}
bool
cluster_external_fence_runtime_active(void)
{
	return true;
}
bool
cluster_wal_thread_current_v2_ref(ClusterWalSourceRef *out)
{
	*out = sample_ref;
	return true;
}
bool
cluster_wal_thread_initialized_writer_matches(const ClusterWalSourceRef *ref, uint64 epoch)
{
	return false;
}
bool
cluster_wal_thread_clean_writer_matches(const ClusterWalSourceRef *ref, uint64 epoch)
{
	return false;
}
bool
LWLockHeldByMe(LWLock *lock)
{
	return false;
}
bool
cluster_cf_lock(LOCKMODE mode)
{
	UT_ASSERT_EQ(mode, ShareLock);
	UT_ASSERT(!phase_test_cf_held);
	phase_test_cf_held = true;
	return true;
}
bool
cluster_cf_held_is_clusterwide(LOCKMODE mode)
{
	return mode == ShareLock && phase_test_cf_held;
}
ClusterCfReleaseResult
cluster_cf_unlock_confirmed(LOCKMODE mode)
{
	UT_ASSERT(cluster_cf_held_is_clusterwide(mode));
	phase_test_cf_held = false;
	sample_cf_releases++;
	return CLUSTER_CF_RELEASE_CONFIRMED;
}
bool
cluster_cf_authority_read_check(ControlFileData *out, bool *pending)
{
	ClusterControlRootResult result;
	UT_ASSERT(phase_test_cf_held);
	sample_cf_reads++;
	result = runtime_v2_owner_check(phase_test_formation_epoch, phase_test_self_incarnation, false);
	*pending = result == CLUSTER_CONTROL_ROOT_ADMISSION_PENDING;
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return false;
	memset(out, 0, sizeof(*out));
	out->system_identifier = sample_ref.claim.identity.system_identifier;
	out->checkPointCopy.ThisTimeLineID = sample_ref.timeline;
	out->state = DB_IN_PRODUCTION;
	out->checkPoint = 150;
	return true;
}
static void
ClusterStartupCheckpointPrepare(int flags, ControlFileData *selected)
{
	UT_ASSERT(false);
}
void
ProcessInterrupts(void)
{
	UT_ASSERT(false);
}
void
pg_re_throw(void)
{
	longjmp(phase4_fatal_jump, 1);
}
void
ResetLatch(Latch *latch)
{
	UT_ASSERT(latch == MyLatch);
}
int
WaitLatch(Latch *latch, int events, long timeout, uint32 event)
{
	UT_ASSERT(latch == MyLatch && (events & WL_EXIT_ON_PM_DEATH));
	UT_ASSERT_EQ(timeout, 20);
	UT_ASSERT_EQ(event, WAIT_EVENT_CHECKPOINTER_MAIN);
	UT_ASSERT(!phase_test_cf_held && phase_lwlock_depth == 0 && CritSectionCount == 0);
	sample_checkpoint_waits++;
	phase_lwlock_conditional_result = true;
	if (sample_loss_on_wait)
		pg_atomic_write_u32(&QvotecShmem->quorum_state, CLUSTER_QVOTEC_QUORUM_LOST);
	return WL_TIMEOUT;
}
#include "test_cluster_serving_root.inc"

static void
sample_setup(void)
{
	static ClusterQvotecShmem qvotec;
	ClusterQvotecAdmissionCheck check;
	ClusterFormationSnapshotV1 formation;
	ClusterFormationCommitMarker *marker;
	bool snapshot_valid;
	const char *predicate;
	int i;

	reset_phase_service_fixture(true);
	phase_lwlock_conditional_result = true;
	cluster_phase_shmem_init();
	cluster_shared_config = true;
	phase_test_cssd_status = CLUSTER_CSSD_READY;
	phase_test_qvotec_status = CLUSTER_QVOTEC_READY;
	/* Deliberately poison the old success stubs. The tested chain must not
	 * consult either one, even though the remaining process fixture uses it. */
	phase4_test_in_quorum = false;
	phase_test_grd_authority_ok = false;
	phase4_test_now = 1000000;
	sample_mono_us = 1000000;
	sample_waits = 0;
	sample_oversleep = false;
	cluster_writes_frozen = false;
	memset(&qvotec, 0, sizeof(qvotec));
	QvotecShmem = &qvotec;
	pg_atomic_init_u32(&qvotec.quorum_state, CLUSTER_QVOTEC_QUORUM_OK);
	pg_atomic_init_u64(&qvotec.lease_expire_at_us, 61000000);
	pg_atomic_init_u64(&qvotec.admission_sequence, 2);
	pg_atomic_init_u64(&qvotec.admission_loss_generation, 1);
	pg_atomic_init_u64(&qvotec.admission_lease_sampled_us, sample_mono_us);
	pg_atomic_init_u64(&qvotec.admission_lease_expires_us, 61000000);
	memset(&sample_provider, 0, sizeof(sample_provider));
	sample_provider.reason = CLUSTER_STORAGE_QUORUM_READY;
	sample_provider.ring_node = 11;
	sample_provider.ring_sequence = 8;
	sample_provider.members[0] = 15;
	cluster_storage_quorum_attach(&qvotec.storage_quorum, true);
	cluster_storage_quorum_refresh(sample_mono_us, UINT64_C(60000000));
	memset(&sample_reconfig, 0, sizeof(sample_reconfig));
	sample_reconfig.self_join_admitted = true;
	marker = &sample_reconfig.startup_formation;
	marker->magic = CLUSTER_FORMATION_MARKER_MAGIC;
	marker->version = CLUSTER_FORMATION_MARKER_VERSION;
	marker->phase = CLUSTER_FORMATION_MARKER_PHASE_COMMITTED;
	marker->formation_generation = 1;
	marker->formation_epoch = 1;
	marker->commit_nonce = 17;
	marker->n_admitted = 4;
	marker->admitted_nodes[0] = 15;
	marker->arbiter_node = 0;
	marker->arbiter_incarnation = 11;
	memset(&sample_grd, 0, sizeof(sample_grd));
	cluster_grd_entry_htab = (HTAB *)&sample_grd;
	pg_atomic_init_u32(&sample_grd.master_map_initialized, 1);
	pg_atomic_init_u64(&sample_grd.master_map_refresh_count, 1);
	pg_atomic_init_u64(&sample_grd.recovery_authority_boot_incarnation, 11);
	pg_atomic_init_u64(&sample_grd.recovery_authority_lms_generation, 7);
	pg_atomic_init_u64(&sample_grd.recovery_authority_master_refresh, 1);
	pg_atomic_init_u64(&sample_grd.recovery_authority_formation_epoch, 1);
	pg_atomic_init_u64(&sample_grd.recovery_authority_bitmap_hash, 77);
	pg_atomic_init_u64(&sample_grd.recovery_authority_members[0], 15);
	for (i = 0; i < 4; i++) {
		sample_reconfig.startup_formation_incarnations[i] = 11;
		sample_reconfig.membership.membership_state[i] = CLUSTER_MEMBER_MEMBER;
		sample_reconfig.membership.last_admitted_incarnation[i] = 11;
		pg_atomic_init_u64(&sample_grd.recovery_authority_done_epoch[i], 1);
		pg_atomic_init_u64(&sample_grd.recovery_authority_done_hash[i], 77);
	}
	for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++) {
		pg_atomic_init_u32(&sample_grd.master[i], i % 4);
		pg_atomic_init_u32(&sample_grd.shard_phase[i], GRD_SHARD_NORMAL);
	}
	UT_ASSERT(cluster_qvotec_check_admission(&check));
	UT_ASSERT_EQ(cluster_reconfig_capture_serving_formation_v1(1, &check, &formation,
															   &snapshot_valid, &predicate),
				 CLUSTER_SERVING_FORMATION_CURRENT);
	UT_ASSERT(snapshot_valid);
	/* Seed a previously published SERVING identity; every observation and
	 * lifecycle operation under test below is production code. */
	pg_atomic_write_u32(&sample_phase.current_phase, CLUSTER_PHASE_RUNNING);
	pg_atomic_write_u32(&sample_phase.authority_readiness, CLUSTER_AUTHORITY_SERVING_READY);
	pg_atomic_write_u32(&sample_phase.authority_managed, 1);
	sample_phase.authority_origin_thread = 1;
	sample_phase.authority_boot_incarnation = 11;
	sample_phase.authority_lms_generation = 7;
	sample_phase.authority_quorum_generation = check.continuity.quorum_generation;
	sample_phase.authority_storage_generation = check.continuity.storage_generation;
	sample_phase.authority_formation = formation;
	UT_ASSERT(cluster_serving_ready_is_current());
	UT_ASSERT_EQ(phase4_quorum_check_calls, 0);
}

UT_TEST(deadline_projects_pending_through_real_formation_and_grd)
{
	ClusterQvotecAdmissionCheck check;
	ClusterFormationSnapshotV1 formation;
	bool valid, pending;
	const char *predicate;

	sample_setup();
	pg_atomic_fetch_add_u32(&QvotecShmem->storage_quorum.sequence, 1);
	sample_oversleep = true;
	UT_ASSERT(!cluster_qvotec_check_admission(&check));
	UT_ASSERT_EQ(check.result, CLUSTER_QVOTEC_ADMISSION_STORAGE);
	UT_ASSERT_EQ(check.storage.snapshot_stop, CLUSTER_STORAGE_SNAPSHOT_DEADLINE);
	UT_ASSERT_EQ(sample_waits, 1);
	UT_ASSERT_EQ(
		cluster_reconfig_capture_serving_formation_v1(1, &check, &formation, &valid, &predicate),
		CLUSTER_SERVING_FORMATION_PENDING);
	UT_ASSERT(valid);
	UT_ASSERT(!cluster_grd_recovery_authority_for_admission(11, 7, &check, &pending));
	UT_ASSERT(pending);
	/* Later publication cannot reinterpret this original DEADLINE sample. */
	pg_atomic_fetch_add_u32(&QvotecShmem->storage_quorum.sequence, 1);
	UT_ASSERT(!cluster_grd_recovery_authority_for_admission(11, 7, &check, &pending));
	UT_ASSERT(pending);
	/* Genuine seal drift still overrides the incomplete quorum observation. */
	pg_atomic_write_u64(&sample_grd.recovery_authority_done_hash[2], 78);
	UT_ASSERT(!cluster_grd_recovery_authority_for_admission(11, 7, &check, &pending));
	UT_ASSERT(!pending);
	UT_ASSERT_EQ(phase4_quorum_check_calls, 0);
}

UT_TEST(serving_deadline_preserves_binding_until_proven_loss_or_identity_drift)
{
	ClusterFormationSnapshotV1 before;
	bool pending;
	const char *predicate;

	sample_setup();
	before = sample_phase.authority_formation;
	pg_atomic_fetch_add_u32(&QvotecShmem->storage_quorum.sequence, 1);
	sample_oversleep = true;
	UT_ASSERT(!cluster_serving_ready_check(&pending, &predicate));
	UT_ASSERT(pending);
	UT_ASSERT_STR_EQ(predicate, "QUORUM_OBSERVATION_PENDING");
	UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_SERVING_READY);
	UT_ASSERT_EQ(memcmp(&before, &sample_phase.authority_formation, sizeof(before)), 0);
	UT_ASSERT_EQ(sample_phase.authority_boot_incarnation, 11);
	UT_ASSERT_EQ(sample_phase.authority_lms_generation, 7);
	pg_atomic_fetch_add_u32(&QvotecShmem->storage_quorum.sequence, 1);
	UT_ASSERT(cluster_serving_ready_check(&pending, &predicate));
	UT_ASSERT(!pending);
	pg_atomic_write_u32(&QvotecShmem->quorum_state, CLUSTER_QVOTEC_QUORUM_LOST);
	UT_ASSERT(!cluster_serving_ready_check(&pending, &predicate));
	UT_ASSERT(!pending);
	UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_OFF);
	sample_setup();
	pg_atomic_fetch_add_u32(&QvotecShmem->storage_quorum.sequence, 1);
	sample_oversleep = true;
	phase_test_lms_generation++;
	sample_generation_reads = 0;
	UT_ASSERT(!cluster_serving_ready_check(&pending, &predicate));
	UT_ASSERT(!pending);
	UT_ASSERT_STR_EQ(predicate, "LMS_GENERATION_CHANGED");
	UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_OFF);
	UT_ASSERT_EQ(sample_generation_reads, 1);
	UT_ASSERT_EQ(phase4_quorum_check_calls, 0);
}

UT_TEST(lock_entry_keeps_real_serving_deadline_pending)
{
	ClusterLockAcquireRequest req = { 0 };

	sample_setup();
	/* OBJECT does not belong to the CF/WALR reconstruction gate. */
	phase_test_control_acquire_ready = true;
	cluster_lms_enabled = true;
	req.resid.type = LOCKTAG_OBJECT;
	req.lockmode = ExclusiveLock;
	pg_atomic_fetch_add_u32(&QvotecShmem->storage_quorum.sequence, 1);
	sample_oversleep = true;
	UT_ASSERT_EQ(cluster_lock_acquire_s1_entry(&req), CLUSTER_LOCK_ACQUIRE_PENDING);
	UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_SERVING_READY);
	pg_atomic_fetch_add_u32(&QvotecShmem->storage_quorum.sequence, 1);
	UT_ASSERT_EQ(cluster_lock_acquire_s1_entry(&req), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	pg_atomic_write_u32(&QvotecShmem->quorum_state, CLUSTER_QVOTEC_QUORUM_LOST);
	UT_ASSERT_EQ(cluster_lock_acquire_s1_entry(&req), CLUSTER_LOCK_ACQUIRE_FAIL_LMS_UNAVAILABLE);
	UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_OFF);
	UT_ASSERT_EQ(phase4_quorum_check_calls, 0);
}

static void
sample_send_pending_case(bool rdma)
{
	PGPROC sender = { 0 };
	uint32 bytes = 71;
	ClusterICSge sge = { .addr = &bytes, .len = sizeof(bytes), .release_cb = sample_release };
	volatile bool raised = false;
	volatile int result = -1;

	sample_setup();
	MyProc = &sender;
	MyBackendType = B_INVALID;
	sample_sends = sample_releases = 0;
	/* The real conditional formation/GRD lock boundary cannot be captured. */
	phase_lwlock_conditional_result = false;
	phase4_capture_fatal = true;
	if (setjmp(phase4_fatal_jump) == 0)
		result = rdma ? (int)cluster_ic_rdma_send_envelope_sge(8, 1, &sge, 1, sizeof(bytes))
					  : (int)cluster_ic_send_envelope_chunked(8, 1, &bytes, sizeof(bytes));
	else
		raised = true;
	phase4_capture_fatal = false;
	phase_lwlock_conditional_result = true;
	MyProc = NULL;
	UT_ASSERT(!raised);
	UT_ASSERT_EQ(result, rdma ? CLUSTER_IC_SEND_NOT_ADMITTED : false);
	UT_ASSERT_EQ(sample_sends, 0);
	UT_ASSERT_EQ(bytes, 71);
	UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_SERVING_READY);
	/* The exact caller retries after publication; no admission was invented. */
	result = rdma ? (int)cluster_ic_rdma_send_envelope_sge(8, 1, &sge, 1, sizeof(bytes))
				  : (int)cluster_ic_send_envelope_chunked(8, 1, &bytes, sizeof(bytes));
	UT_ASSERT_EQ(result, rdma ? CLUSTER_IC_SEND_DONE : true);
	UT_ASSERT_EQ(sample_sends, 1);
	UT_ASSERT_EQ(phase4_quorum_check_calls, 0);
}

UT_TEST(rdma_send_waits_for_real_formation_lock_without_error)
{
	sample_send_pending_case(true);
}
UT_TEST(chunk_send_waits_for_real_formation_lock_without_error)
{
	sample_send_pending_case(false);
}

UT_TEST(real_loss_still_refuses_transport_sends)
{
	for (int rdma = 0; rdma < 2; rdma++) {
		uint32 bytes = 71;
		ClusterICSge sge = { .addr = &bytes, .len = sizeof(bytes), .release_cb = sample_release };
		volatile bool raised = false;
		volatile int result = -1;

		sample_setup();
		MyBackendType = B_INVALID;
		sample_sends = sample_releases = 0;
		pg_atomic_write_u32(&QvotecShmem->quorum_state, CLUSTER_QVOTEC_QUORUM_LOST);
		phase4_capture_fatal = true;
		if (setjmp(phase4_fatal_jump) == 0)
			result = rdma ? (int)cluster_ic_rdma_send_envelope_sge(8, 1, &sge, 1, sizeof(bytes))
						  : (int)cluster_ic_send_envelope_chunked(8, 1, &bytes, sizeof(bytes));
		else
			raised = true;
		phase4_capture_fatal = false;
		UT_ASSERT(raised || result == (rdma ? CLUSTER_IC_SEND_HARD_ERROR : false));
		UT_ASSERT_EQ(sample_sends, 0);
		UT_ASSERT_EQ(cluster_authority_readiness_get(), CLUSTER_AUTHORITY_OFF);
	}
}

UT_TEST(checkpoint_prepare_waits_for_real_formation_lock_and_refuses_real_loss)
{
	PGPROC checkpointer = { 0 };

	for (int fault = 0; fault < 2; fault++) {
		for (int shutdown = 0; shutdown < 2; shutdown++) {
			static ControlFileData selected;
			volatile bool raised = false;
			sample_setup();
			memset(&sample_ref, 0, sizeof(sample_ref));
			sample_ref.claim.identity.system_identifier = 1234;
			sample_ref.timeline = 1;
			sample_cf_reads = sample_cf_releases = sample_checkpoint_waits = 0;
			sample_loss_on_wait = fault != 0;
			MyBackendType = B_CHECKPOINTER;
			MyAuxProcType = CheckpointerProcess;
			MyProc = &checkpointer;
			ShutdownRequestPending = shutdown != 0;
			phase_lwlock_conditional_result = false;
			phase4_capture_fatal = true;
			if (setjmp(phase4_fatal_jump) == 0)
				ClusterCheckpointV3Prepare(shutdown ? CHECKPOINT_IS_SHUTDOWN : CHECKPOINT_FORCE,
										   &selected);
			else
				raised = true;
			phase4_capture_fatal = false;
			UT_ASSERT_EQ(raised, fault != 0);
			UT_ASSERT_EQ(sample_checkpoint_waits, 1);
			UT_ASSERT_EQ(sample_cf_reads, 2);
			UT_ASSERT_EQ(sample_cf_releases, 2);
			UT_ASSERT(!phase_test_cf_held);
			UT_ASSERT_EQ(selected.checkPoint, fault ? 0 : 150);
			UT_ASSERT_EQ(cluster_authority_readiness_get(),
						 fault ? CLUSTER_AUTHORITY_OFF : CLUSTER_AUTHORITY_SERVING_READY);
			UT_ASSERT_EQ(phase4_quorum_check_calls, 0);
			MyProc = NULL;
		}
	}
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(deadline_projects_pending_through_real_formation_and_grd);
	UT_RUN(serving_deadline_preserves_binding_until_proven_loss_or_identity_drift);
	UT_RUN(lock_entry_keeps_real_serving_deadline_pending);
	UT_RUN(rdma_send_waits_for_real_formation_lock_without_error);
	UT_RUN(chunk_send_waits_for_real_formation_lock_without_error);
	UT_RUN(real_loss_still_refuses_transport_sends);
	UT_RUN(checkpoint_prepare_waits_for_real_formation_lock_and_refuses_real_loss);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
