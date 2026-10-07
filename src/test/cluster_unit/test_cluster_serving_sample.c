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

int
main(void)
{
	UT_PLAN(2);
	UT_RUN(deadline_projects_pending_through_real_formation_and_grd);
	UT_RUN(serving_deadline_preserves_binding_until_proven_loss_or_identity_drift);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
