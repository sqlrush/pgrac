/*-------------------------------------------------------------------------
 * STOP-02 \u00a717.6 opaque formation-witness lifecycle tests.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdlib.h>

#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_semantic_activation.h" /* ACK stage enum (G3 stub) */
#include "storage/latch.h"						 /* recovery path retry stubs (MyLatch/WaitLatch) */

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

static ClusterFormationSnapshotV1 snapshots[2];
static int snapshot_call;
static bool snapshot_available;
static ClusterFenceAuthorityProof durable_proof;
static ClusterFenceAuthorityReadResult durable_result;
static bool cache_publish_ok;
static ClusterFenceAuthorityCacheResult cache_result;
static uint64 cache_sequence;

/* Added to the product header by the GREEN; keep the RED link-exact rather
 * than relying on an implicit declaration. */
extern ClusterFormationWitnessResult
cluster_formation_witness_build_recovery_control_wait(uint16 origin_thread, int timeout_ms,
													  ClusterFormationWitnessV1 **out);

void *
palloc(Size size)
{
	return malloc(size);
}
void
pfree(void *pointer)
{
	free(pointer);
}
void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

void
pg_usleep(long microsec)
{
	(void)microsec;
}

bool
cluster_reconfig_capture_formation_snapshot_v1(uint16 origin_thread,
											   ClusterFormationSnapshotV1 *out)
{
	(void)origin_thread;
	if (!snapshot_available)
		return false;
	*out = snapshots[snapshot_call % 2];
	snapshot_call++;
	return true;
}

ClusterFenceAuthorityReadResult
cluster_write_fence_read_durable_authority(ClusterFenceAuthorityProof *out)
{
	if (durable_result == CLUSTER_FENCE_AUTHORITY_OK)
		*out = durable_proof;
	return durable_result;
}

bool
cluster_write_fence_authority_cache_publish_if_unchanged(const ClusterFenceMarker *marker,
														 uint64 published_at_us,
														 uint64 expected_sequence)
{
	(void)marker;
	(void)published_at_us;
	(void)expected_sequence;
	return cache_publish_ok;
}

uint64
cluster_write_fence_authority_cache_sequence(void)
{
	return cache_sequence;
}

/* This fixture exercises only formation-witness entry points.  The same
 * product object also owns the runtime owner-rejoin path; keep those unused
 * dependencies fail-closed without widening this unit's link boundary. */
ClusterControlRootResult
cluster_control_root_lookup_owner_by_node_runtime(int32 old_node_id,
												  ClusterControlRootIdentity *out_identity,
												  ClusterControlRootSnapshot *out_snapshot,
												  ClusterControlRootReadToken *out_token)
{
	(void)old_node_id;
	(void)out_identity;
	(void)out_snapshot;
	(void)out_token;
	return CLUSTER_CONTROL_ROOT_ABSENT;
}

/* RF-ROOT P7 G3/G4: the create/activate coordinator proofs and the runtime
 * census gate live in the same product object; this unit exercises only the
 * formation-witness entry points, so keep the proofs fail-closed. */
bool
cluster_semantic_activation_ack_complete_matches(
	uint64 transition_epoch pg_attribute_unused(), uint64 record_generation pg_attribute_unused(),
	uint64 expected_members_lo pg_attribute_unused(),
	uint64 expected_members_hi pg_attribute_unused(),
	uint64 source_feature_bitmap pg_attribute_unused(),
	uint64 target_feature_bitmap pg_attribute_unused(),
	uint64 capability_sample_digest pg_attribute_unused(),
	ClusterSemanticActivationAckStage minimum_stage pg_attribute_unused())
{
	return false;
}

bool
cluster_control_root_feature_bitmap_is_known(uint64 active_feature_bitmap pg_attribute_unused())
{
	return false;
}

bool
cluster_wal_state_correctness_census_ok(void)
{
	return false;
}

ClusterRecoveryOwnerImportResult
cluster_recovery_owner_import_read_v1(int32 node_id, const ClusterWalThreadClaim *immutable_claim,
									  uint64 frozen_admitted_bitmap_low,
									  uint64 frozen_admitted_bitmap_high, uint64 *out_incarnation)
{
	(void)node_id;
	(void)immutable_claim;
	(void)frozen_admitted_bitmap_low;
	(void)frozen_admitted_bitmap_high;
	(void)out_incarnation;
	return CLUSTER_RECOVERY_OWNER_IMPORT_IO_FAILED;
}

ClusterControlRootResult
cluster_control_root_compare_and_publish(const ClusterControlRootReadToken *expected_token,
										 const ClusterControlRootPatch *patch,
										 ClusterControlRootPublishReason reason,
										 ClusterControlRootSnapshot *out_snapshot,
										 ClusterControlRootReadToken *out_token)
{
	(void)expected_token;
	(void)patch;
	(void)reason;
	(void)out_snapshot;
	(void)out_token;
	return CLUSTER_CONTROL_ROOT_ABSENT;
}

ClusterFenceAuthorityCacheResult
cluster_write_fence_revalidate_cached_nowait(const ClusterFenceMarker *expected, uint64 now_us)
{
	(void)expected;
	(void)now_us;
	return cache_result;
}

/* RF-ROOT P6 (L4 admission / phase-3 gate diag refs): cluster_recovery_duty.o
 * samples the live-component predicates; the pure unit pins them inert so the
 * binary stays standalone. */
int cluster_node_id = 0;

int
cluster_cssd_get_status(void)
{
	return 0;
}

int
cluster_qvotec_get_status(void)
{
	return 0;
}

bool
cluster_qvotec_in_quorum(void)
{
	return false;
}

bool
cluster_membership_is_member(int32 node_id pg_attribute_unused())
{
	return false;
}

bool
cluster_reconfig_self_join_admitted(void)
{
	return false;
}

/* Link-only stub (RF-ROOT P6 contract): recovery_duty.o's missed
 * clean-close repair reads the durable clean-departed evidence; this
 * fixture never exercises the repair path. */
bool
cluster_reconfig_is_clean_departed(int32 node_id pg_attribute_unused())
{
	return false;
}

/* Link-only stubs (RF-ROOT P7 recovery path): recovery_duty.o's bounded
 * THREAD_CLEAN_CLOSE retry (cluster_control_root_thread_clean_close_publish_retry)
 * waits on MyLatch with a clock; this fixture never exercises the retry
 * path, so the clock stands still and every wait times out instantly. */
static Latch ut_retry_latch;
Latch *MyLatch = &ut_retry_latch;
static TimestampTz ut_retry_now_us = 1700000000000000LL;

TimestampTz
GetCurrentTimestamp(void)
{
	return ut_retry_now_us;
}

TimestampTz
TimestampTzPlusMilliseconds(TimestampTz t, int64 ms)
{
	return t + (TimestampTz)ms * 1000;
}

int
WaitLatch(Latch *latch, int wakeEvents, long timeout, uint32 wait_event_info)
{
	(void)latch;
	(void)wakeEvents;
	(void)timeout;
	(void)wait_event_info;
	return WL_TIMEOUT;
}

void
ResetLatch(Latch *latch)
{
	(void)latch;
}

volatile sig_atomic_t InterruptPending = 0;

void
ProcessInterrupts(void)
{}

/* recovery path: the retry rebinds the leaver's serving authority on the success
 * path; this fixture never reaches it. */
bool
cluster_authority_serving_rebind_leaver(void)
{
	return false;
}

bool
cluster_lms_is_recovery_ready(void)
{
	return false;
}

int
cluster_current_phase(void)
{
	return 0;
}

bool
cluster_recovery_transport_components_current(void)
{
	return false;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

static void
build_ready_fixture(void)
{
	ClusterFormationSnapshotV1 *s = &snapshots[0];
	int32 origin = 3;

	memset(snapshots, 0, sizeof(snapshots));
	s->applied.event_id = UINT64_C(0xAA);
	s->applied.coordinator_node_id = 0;
	s->applied.new_epoch = 9;
	s->applied.cssd_dead_generation = 5;
	s->applied.reconfig_kind = RECONFIG_KIND_FAIL_STOP;
	s->applied.dead_bitmap[0] = 0x08;
	s->excluded_bitmap[0] = 0x08;
	s->membership.membership_state[0] = CLUSTER_MEMBER_MEMBER;
	s->membership.membership_state[origin] = CLUSTER_MEMBER_DEAD;
	s->membership.last_admitted_incarnation[origin] = 77;
	s->victim_incarnation = 77;
	s->local_epoch = 9;
	s->self_join_admitted = 1;
	snapshots[1] = snapshots[0];

	memset(&durable_proof, 0, sizeof(durable_proof));
	durable_proof.marker.magic = CLUSTER_FENCE_MARKER_MAGIC;
	durable_proof.marker.version = CLUSTER_FENCE_MARKER_VERSION;
	durable_proof.marker.fence_epoch = 9;
	durable_proof.marker.fence_event_id = UINT64_C(0xAA);
	durable_proof.marker.fence_generation = 5;
	durable_proof.marker.issuer_node_id = 0;
	durable_proof.marker.fenced_dead_bitmap[0] = 0x08;
	durable_proof.agree_disk_count = 2;
	durable_proof.total_disk_count = 3;
	snapshot_call = 0;
	snapshot_available = true;
	durable_result = CLUSTER_FENCE_AUTHORITY_OK;
	cache_publish_ok = true;
	cache_result = CLUSTER_FENCE_CACHE_MATCH;
	cache_sequence = 2;
}

UT_TEST(test_witness_bad_arguments_leave_null)
{
	ClusterFormationWitnessV1 *witness = NULL;

	build_ready_fixture();
	UT_ASSERT_EQ(cluster_formation_witness_build_wait(0, true, 10, &witness),
				 CLUSTER_FORMATION_WITNESS_BAD_ARGUMENT);
	UT_ASSERT(witness == NULL);
	UT_ASSERT_EQ(cluster_formation_witness_build_wait(4, true, 0, &witness),
				 CLUSTER_FORMATION_WITNESS_BAD_ARGUMENT);
	UT_ASSERT_EQ(cluster_formation_witness_build_wait(4, true, 10, NULL),
				 CLUSTER_FORMATION_WITNESS_BAD_ARGUMENT);
}

UT_TEST(test_witness_ready_borrow_revalidate_destroy)
{
	ClusterFormationWitnessV1 *witness = NULL;
	const ClusterFenceAuthorityProof *proof;

	build_ready_fixture();
	UT_ASSERT_EQ(cluster_formation_witness_build_wait(4, true, 10, &witness),
				 CLUSTER_FORMATION_WITNESS_READY);
	UT_ASSERT(witness != NULL);
	proof = cluster_formation_witness_authority(witness);
	UT_ASSERT(proof != NULL);
	UT_ASSERT_EQ(proof->marker.fence_epoch, 9);
	UT_ASSERT_EQ(cluster_formation_witness_revalidate_nowait(witness),
				 CLUSTER_FORMATION_WITNESS_READY);
	cluster_formation_witness_destroy(&witness);
	UT_ASSERT(witness == NULL);
	cluster_formation_witness_destroy(&witness);
}

UT_TEST(test_witness_unstable_or_unavailable_never_installs_handle)
{
	ClusterFormationWitnessV1 *witness = NULL;

	build_ready_fixture();
	snapshots[1].local_epoch++;
	UT_ASSERT_EQ(cluster_formation_witness_build_wait(4, true, 1, &witness),
				 CLUSTER_FORMATION_WITNESS_UNSTABLE);
	UT_ASSERT(witness == NULL);
	build_ready_fixture();
	durable_result = CLUSTER_FENCE_AUTHORITY_NO_CONFIG;
	UT_ASSERT_EQ(cluster_formation_witness_build_wait(4, true, 10, &witness),
				 CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE);
	UT_ASSERT(witness == NULL);
}

UT_TEST(test_witness_revalidate_maps_stale_and_unavailable)
{
	ClusterFormationWitnessV1 *witness = NULL;

	build_ready_fixture();
	UT_ASSERT_EQ(cluster_formation_witness_build_wait(4, true, 10, &witness),
				 CLUSTER_FORMATION_WITNESS_READY);
	cache_result = CLUSTER_FENCE_CACHE_STALE;
	UT_ASSERT_EQ(cluster_formation_witness_revalidate_nowait(witness),
				 CLUSTER_FORMATION_WITNESS_UNSTABLE);
	cache_result = CLUSTER_FENCE_CACHE_UNAVAILABLE;
	UT_ASSERT_EQ(cluster_formation_witness_revalidate_nowait(witness),
				 CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE);
	cluster_formation_witness_destroy(&witness);
}

UT_TEST(test_live_witness_requires_same_stable_member_formation)
{
	ClusterFormationWitnessV1 *witness = NULL;

	build_ready_fixture();
	snapshots[0].membership.last_admitted_incarnation[0] = 55;
	snapshots[1] = snapshots[0];
	UT_ASSERT_EQ(cluster_formation_witness_build_live_wait(1, 10, &witness),
				 CLUSTER_FORMATION_WITNESS_READY);
	UT_ASSERT_NOT_NULL(witness);
	cluster_formation_witness_destroy(&witness);

	build_ready_fixture();
	snapshots[0].membership.membership_state[0] = CLUSTER_MEMBER_JOINING;
	snapshots[0].membership.last_admitted_incarnation[0] = 55;
	snapshots[1] = snapshots[0];
	UT_ASSERT_EQ(cluster_formation_witness_build_live_wait(1, 10, &witness),
				 CLUSTER_FORMATION_WITNESS_OWNER_MISMATCH);
	UT_ASSERT_NULL(witness);
}

UT_TEST(test_recovery_control_witness_is_initial_only_and_survives_gate_open)
{
	ClusterFormationWitnessV1 *witness = NULL;
	ClusterFenceAuthorityProof authority;
	ClusterFormationSnapshotV1 classification;
	uint16 origin_thread = 0;

	build_ready_fixture();
	memset(snapshots, 0, sizeof(snapshots));
	snapshots[0].membership.membership_state[0] = CLUSTER_MEMBER_MEMBER;
	snapshots[0].membership.last_admitted_incarnation[0] = UINT64_C(55);
	snapshots[1] = snapshots[0];
	memset(&durable_proof, 0, sizeof(durable_proof));
	durable_proof.marker.magic = CLUSTER_FENCE_MARKER_MAGIC;
	durable_proof.marker.version = CLUSTER_FENCE_MARKER_VERSION;
	durable_proof.marker.issuer_node_id = CLUSTER_FENCE_BASELINE_INITIAL_ISSUER;
	durable_proof.marker.marker_kind = CLUSTER_FENCE_MARKER_KIND_BASELINE;
	durable_proof.agree_disk_count = 2;
	durable_proof.total_disk_count = 3;
	snapshot_call = 0;

	/* The ordinary live contract remains closed while the write admission byte
	 * is zero; only the phase-3 recovery-control builder may accept this shape. */
	UT_ASSERT_EQ(cluster_formation_witness_build_live_wait(1, 1, &witness),
				 CLUSTER_FORMATION_WITNESS_UNSTABLE);
	UT_ASSERT_NULL(witness);
	snapshots[0].local_epoch = 1;
	snapshots[1] = snapshots[0];
	snapshot_call = 0;
	UT_ASSERT_EQ(cluster_formation_witness_build_recovery_control_wait(1, 1, &witness),
				 CLUSTER_FORMATION_WITNESS_UNSTABLE);
	UT_ASSERT_NULL(witness);
	snapshots[0].local_epoch = UINT64_C(0);
	snapshots[1] = snapshots[0];
	snapshot_call = 0;
	UT_ASSERT_EQ(cluster_formation_witness_build_recovery_control_wait(1, 10, &witness),
				 CLUSTER_FORMATION_WITNESS_READY);
	UT_ASSERT_NOT_NULL(witness);
	UT_ASSERT(cluster_formation_witness_copy_classification_v1(witness, &origin_thread, &authority,
															   &classification));
	UT_ASSERT_EQ(origin_thread, 1);

	/* StartupXLOG later opens the exact same formation's write gate.  That one
	 * monotone edge keeps the control binding current; unrelated drift does not. */
	snapshots[0].self_join_admitted = 1;
	snapshots[1].self_join_admitted = 1;
	snapshot_call = 0;
	UT_ASSERT_EQ(cluster_formation_classification_revalidate_nowait(origin_thread, &authority,
																	&classification),
				 CLUSTER_FORMATION_WITNESS_READY);
	snapshots[0].membership.last_admitted_incarnation[0]++;
	snapshots[1] = snapshots[0];
	snapshot_call = 0;
	UT_ASSERT_EQ(cluster_formation_classification_revalidate_nowait(origin_thread, &authority,
																	&classification),
				 CLUSTER_FORMATION_WITNESS_UNSTABLE);
	cluster_formation_witness_destroy(&witness);
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_witness_bad_arguments_leave_null);
	UT_RUN(test_witness_ready_borrow_revalidate_destroy);
	UT_RUN(test_witness_unstable_or_unavailable_never_installs_handle);
	UT_RUN(test_witness_revalidate_maps_stale_and_unavailable);
	UT_RUN(test_live_witness_requires_same_stable_member_formation);
	UT_RUN(test_recovery_control_witness_is_initial_only_and_survives_gate_open);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
