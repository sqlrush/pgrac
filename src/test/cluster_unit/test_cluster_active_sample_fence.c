/*-------------------------------------------------------------------------
 * test_cluster_active_sample_fence.c
 *    Execute the read-only admission/cohort sample fence.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_active_sample_fence.c
 *
 * Private types, admission seqlock, policy and fence bodies are extracted
 * verbatim. Membership/QVOTEC and lifecycle observers are boundary inputs.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/xlog.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_semantic_activation.h"
#include "port/atomics.h"
#include "test_cluster_active_sample_types.inc"

#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

static ClusterSemanticActivationShmem gate;
static ClusterSemanticActivationShmem *SemanticActivationShmem = &gate;
static ClusterR4MembershipSnapshot members;
int cluster_node_id;
static uint64 system_id, epoch, self_incarnation, membership_cut;
static bool recovering, stopping, serving, allowed, quorum, membership_available;
static unsigned membership_reads;
static int change_during_read;

/* The real atomics retain their cassert alignment checks in this fixture. */
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

static bool
normal_start_serving_allowed(void)
{
	return serving;
}

bool
RecoveryInProgress(void)
{
	return recovering;
}

bool
cluster_normal_stop_maintenance_cut(void)
{
	return stopping;
}

uint64
GetSystemIdentifier(void)
{
	return system_id;
}

uint64
cluster_epoch_get_current(void)
{
	return epoch;
}

uint64
cluster_qvotec_get_self_incarnation(void)
{
	return self_incarnation;
}

bool
cluster_qvotec_in_quorum(void)
{
	return quorum;
}

uint64
cluster_membership_cut_generation(void)
{
	return membership_cut;
}

bool
cluster_membership_cut_generation_current(uint64 expected)
{
	return expected != 0 && expected == membership_cut;
}

ClusterJoinGateVerdict
cluster_reconfig_self_join_gate_verdict(void)
{
	return allowed ? CLUSTER_JOIN_GATE_ALLOW : CLUSTER_JOIN_GATE_BLOCK_53R60;
}

bool
cluster_reconfig_lmon_snapshot_r4_membership(ClusterR4MembershipSnapshot *out)
{
	membership_reads++;
	*out = members;
	/* Inject replacement after the original owner has returned its snapshot. */
	switch (change_during_read) {
	case 1:
		pg_atomic_fetch_add_u64(&gate.admission_seq, 2);
		break;
	case 2:
		membership_cut += 2;
		break;
	case 3:
		epoch++;
		break;
	case 4:
		self_incarnation++;
		break;
	case 5:
		stopping = true;
		break;
	}
	return membership_available;
}

#include "test_cluster_active_sample_fence.inc"

static void
reset_fixture(void)
{
	MemSet(&gate, 0, sizeof(gate));
	MemSet(&members, 0, sizeof(members));
	SemanticActivationShmem = &gate;
	pg_atomic_init_u64(&gate.admission_seq, 2);
	pg_atomic_init_u64(&gate.active_bits, 0);
	pg_atomic_init_u64(&gate.record_generation, 11);
	pg_atomic_init_u64(&gate.formation_epoch, 7);
	pg_atomic_init_u32(&gate.transition_closed, 0);
	for (int side = 0; side < 2; side++)
		for (int bit = 0; bit < 64; bit++)
			pg_atomic_init_u32(&gate.inflight[side][bit], 0);
	system_id = 12345;
	epoch = members.formation_epoch = 7;
	cluster_node_id = 0;
	self_incarnation = members.local_self_boot_incarnation = 9;
	membership_cut = 4;
	members.admitted_members_lo = UINT64_C(1);
	members.admitted_members_hi = UINT64_C(1) << 63;
	members.admitted_incarnation[0] = 9;
	members.admitted_incarnation[CLUSTER_MAX_NODES - 1] = 37;
	members.observed_generation[0] = members.observed_generation[CLUSTER_MAX_NODES - 1] = 12;
	recovering = stopping = false;
	serving = allowed = quorum = membership_available = true;
	membership_reads = 0;
	change_during_read = 0;
}

static void
expect_refused_and_cleared(void)
{
	ClusterSemanticActiveSampleFence fence, zero;

	MemSet(&fence, 0xa5, sizeof(fence));
	MemSet(&zero, 0, sizeof(zero));
	UT_ASSERT(!cluster_semantic_activation_active_sample_capture(&fence));
	UT_ASSERT_EQ(0, memcmp(&fence, &zero, sizeof(fence)));
}

UT_TEST(test_source_capture_preserves_exact_high_member)
{
	ClusterSemanticActiveSampleFence fence;

	reset_fixture();
	UT_ASSERT(cluster_semantic_activation_active_sample_capture(&fence));
	UT_ASSERT_EQ(system_id, fence.system_identifier);
	UT_ASSERT_EQ(2, fence.admission_publication_seq);
	UT_ASSERT_EQ(11, fence.record_generation);
	UT_ASSERT_EQ(epoch, fence.formation_epoch);
	UT_ASSERT_EQ(membership_cut, fence.membership_cut_generation);
	UT_ASSERT_EQ(0, fence.local_node_id);
	UT_ASSERT_EQ(37, fence.admitted_incarnation[CLUSTER_MAX_NODES - 1]);
	UT_ASSERT_EQ(UINT64_C(1) << 63, fence.admitted_members_hi);
	UT_ASSERT(cluster_semantic_activation_active_sample_current(&fence));
}

UT_TEST(test_target_capture_preserves_active_side)
{
	ClusterSemanticActiveSampleFence fence;

	reset_fixture();
	pg_atomic_write_u64(&gate.active_bits,
						CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
							| CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1);
	UT_ASSERT(cluster_semantic_activation_active_sample_capture(&fence));
	UT_ASSERT_EQ(pg_atomic_read_u64(&gate.active_bits), fence.active_bits);
	pg_atomic_write_u64(&gate.active_bits, 0);
	pg_atomic_fetch_add_u64(&gate.admission_seq, 2);
	UT_ASSERT(!cluster_semantic_activation_active_sample_current(&fence));
}

UT_TEST(test_original_admission_policy_and_seqlock_refuse)
{
	reset_fixture();
	pg_atomic_write_u32(&gate.transition_closed, 1);
	expect_refused_and_cleared();
	reset_fixture();
	pg_atomic_write_u64(&gate.record_generation, 0);
	expect_refused_and_cleared();
	reset_fixture();
	pg_atomic_write_u64(&gate.active_bits, UINT64_C(1) << 63);
	expect_refused_and_cleared();
	reset_fixture();
	pg_atomic_write_u64(&gate.active_bits, CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1);
	expect_refused_and_cleared();
	reset_fixture();
	pg_atomic_write_u64(&gate.admission_seq, 3);
	expect_refused_and_cleared();
	reset_fixture();
	pg_atomic_write_u64(&gate.admission_seq, 0);
	expect_refused_and_cleared();
	reset_fixture();
	SemanticActivationShmem = NULL;
	expect_refused_and_cleared();
}

UT_TEST(test_lifecycle_and_admission_gates_refuse)
{
	reset_fixture();
	recovering = true;
	expect_refused_and_cleared();
	reset_fixture();
	stopping = true;
	expect_refused_and_cleared();
	reset_fixture();
	serving = false;
	expect_refused_and_cleared();
	reset_fixture();
	allowed = false;
	expect_refused_and_cleared();
	reset_fixture();
	quorum = false;
	expect_refused_and_cleared();
	reset_fixture();
	membership_available = false;
	expect_refused_and_cleared();
	reset_fixture();
	membership_cut = 0;
	expect_refused_and_cleared();
	reset_fixture();
	system_id = 0;
	expect_refused_and_cleared();
}

UT_TEST(test_formation_and_local_member_identity_are_required)
{
	reset_fixture();
	cluster_node_id = -1;
	expect_refused_and_cleared();
	reset_fixture();
	cluster_node_id = CLUSTER_MAX_NODES;
	expect_refused_and_cleared();
	reset_fixture();
	members.formation_epoch++;
	expect_refused_and_cleared();
	reset_fixture();
	epoch++;
	expect_refused_and_cleared();
	reset_fixture();
	members.local_self_boot_incarnation = 0;
	expect_refused_and_cleared();
	reset_fixture();
	members.admitted_incarnation[0]++;
	expect_refused_and_cleared();
	reset_fixture();
	members.admitted_members_lo = 0;
	expect_refused_and_cleared();
}

UT_TEST(test_same_payload_admission_republication_invalidates)
{
	ClusterSemanticActiveSampleFence fence;

	reset_fixture();
	UT_ASSERT(cluster_semantic_activation_active_sample_capture(&fence));
	pg_atomic_fetch_add_u64(&gate.admission_seq, 2);
	UT_ASSERT(!cluster_semantic_activation_active_sample_current(&fence));
}

UT_TEST(test_same_cohort_membership_republication_invalidates)
{
	ClusterSemanticActiveSampleFence fence;

	reset_fixture();
	UT_ASSERT(cluster_semantic_activation_active_sample_capture(&fence));
	membership_cut += 2;
	UT_ASSERT(!cluster_semantic_activation_active_sample_current(&fence));
}

UT_TEST(test_remote_member_replacement_or_removal_invalidates)
{
	ClusterSemanticActiveSampleFence fence;

	reset_fixture();
	UT_ASSERT(cluster_semantic_activation_active_sample_capture(&fence));
	members.admitted_incarnation[CLUSTER_MAX_NODES - 1]++;
	UT_ASSERT(!cluster_semantic_activation_active_sample_current(&fence));
	reset_fixture();
	UT_ASSERT(cluster_semantic_activation_active_sample_capture(&fence));
	members.admitted_members_hi = 0;
	members.admitted_incarnation[CLUSTER_MAX_NODES - 1] = 0;
	UT_ASSERT(!cluster_semantic_activation_active_sample_current(&fence));
}

UT_TEST(test_local_origin_replacement_invalidates)
{
	ClusterSemanticActiveSampleFence fence;

	reset_fixture();
	UT_ASSERT(cluster_semantic_activation_active_sample_capture(&fence));
	system_id++;
	UT_ASSERT(!cluster_semantic_activation_active_sample_current(&fence));
	reset_fixture();
	UT_ASSERT(cluster_semantic_activation_active_sample_capture(&fence));
	self_incarnation = members.local_self_boot_incarnation = members.admitted_incarnation[0] = 10;
	UT_ASSERT(!cluster_semantic_activation_active_sample_current(&fence));
	reset_fixture();
	UT_ASSERT(cluster_semantic_activation_active_sample_capture(&fence));
	cluster_node_id = CLUSTER_MAX_NODES - 1;
	self_incarnation = members.local_self_boot_incarnation = 37;
	UT_ASSERT(!cluster_semantic_activation_active_sample_current(&fence));
}

UT_TEST(test_publication_changes_inside_capture_are_refused)
{
	for (int change = 1; change <= 5; change++) {
		reset_fixture();
		change_during_read = change;
		expect_refused_and_cleared();
	}
}

UT_TEST(test_fresh_receiver_observation_preserves_same_cohort)
{
	ClusterSemanticActiveSampleFence fence;

	reset_fixture();
	UT_ASSERT(cluster_semantic_activation_active_sample_capture(&fence));
	members.observed_generation[CLUSTER_MAX_NODES - 1]++;
	UT_ASSERT(cluster_semantic_activation_active_sample_current(&fence));
	membership_available = false;
	UT_ASSERT(!cluster_semantic_activation_active_sample_current(&fence));
}

UT_TEST(test_observer_has_no_modifier_debt_and_null_is_refused)
{
	ClusterSemanticActiveSampleFence fence;

	reset_fixture();
	UT_ASSERT(!cluster_semantic_activation_active_sample_capture(NULL));
	UT_ASSERT(!cluster_semantic_activation_active_sample_current(NULL));
	UT_ASSERT_EQ(0, membership_reads);
	UT_ASSERT(cluster_semantic_activation_active_sample_capture(&fence));
	UT_ASSERT(cluster_semantic_activation_active_sample_current(&fence));
	for (int side = 0; side < 2; side++)
		for (int bit = 0; bit < 64; bit++)
			UT_ASSERT_EQ(0, pg_atomic_read_u32(&gate.inflight[side][bit]));
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(test_source_capture_preserves_exact_high_member);
	UT_RUN(test_target_capture_preserves_active_side);
	UT_RUN(test_original_admission_policy_and_seqlock_refuse);
	UT_RUN(test_lifecycle_and_admission_gates_refuse);
	UT_RUN(test_formation_and_local_member_identity_are_required);
	UT_RUN(test_same_payload_admission_republication_invalidates);
	UT_RUN(test_same_cohort_membership_republication_invalidates);
	UT_RUN(test_remote_member_replacement_or_removal_invalidates);
	UT_RUN(test_local_origin_replacement_invalidates);
	UT_RUN(test_publication_changes_inside_capture_are_refused);
	UT_RUN(test_fresh_receiver_observation_preserves_same_cohort);
	UT_RUN(test_observer_has_no_modifier_debt_and_null_is_refused);
	UT_DONE();
	return ut_failed_count != 0;
}
