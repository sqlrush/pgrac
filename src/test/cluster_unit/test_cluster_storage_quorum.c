/*-------------------------------------------------------------------------
 *
 * test_cluster_storage_quorum.c
 *    Check storage membership and observation expiry.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_storage_quorum.c
 *
 * NOTES
 *    PGRAC-original unit tests; product symbols retain the cluster_ prefix.
 *    Spec: spec-s9p2-06-online-membership.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "utils/timestamp.h"
#include <time.h>
static uint64 fake_monotonic = 100;
static int storage_test_clock_gettime(clockid_t clock_id, struct timespec *out);
#define clock_gettime storage_test_clock_gettime
#include "../../backend/cluster/cluster_storage_quorum.c"
#undef clock_gettime
#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_shared_config = true;
int cluster_node_id = 0;
static TimestampTz fake_now = 100;
static ClusterStorageQuorumState test_state;
static ClusterStorageQuorumView supplied;

static int
storage_test_clock_gettime(clockid_t clock_id, struct timespec *out)
{
	Assert(clock_id == CLOCK_MONOTONIC);
	out->tv_sec = fake_monotonic / 1000000;
	out->tv_nsec = (fake_monotonic % 1000000) * 1000;
	return 0;
}

TimestampTz
GetCurrentTimestamp(void)
{
	return fake_now;
}

void
cluster_storage_corosync_sample(ClusterStorageQuorumView *out)
{
	*out = supplied;
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	abort();
}

static void
ready(void)
{
	memset(&supplied, 0, sizeof(supplied));
	supplied.reason = CLUSTER_STORAGE_QUORUM_READY;
	supplied.ring_node = 11;
	supplied.ring_sequence = 8;
	supplied.members[0] = 3;
	fake_now = 100;
	fake_monotonic = 100;
	cluster_storage_quorum_attach(&test_state, true);
	cluster_storage_quorum_refresh(100, 50);
}

UT_TEST(test_mapping_rejects_aliases_missing_slots_and_overflow)
{
	uint64 configured[2] = { 3, 0 };
	uint32 map[CLUSTER_MAX_NODES];
	const char *bad[] = { "",
						  "0:11",
						  "0:11,0:12",
						  "0:11,1:11",
						  "0:0,1:12",
						  "0:4294967296,1:12",
						  "0:11,128:12",
						  "0:11,1:12,2:13",
						  "0:11,1:12garbage",
						  "0:11,1:12,",
						  "-0:11,1:12",
						  "0:11,,1:12" };
	unsigned int i;

	UT_ASSERT(cluster_storage_quorum_parse_nodes("0:11,1:12", configured, map));
	UT_ASSERT_EQ(map[0], 11);
	UT_ASSERT_EQ(map[1], 12);
	for (i = 0; i < lengthof(bad); i++)
		UT_ASSERT(!cluster_storage_quorum_parse_nodes(bad[i], configured, map));
}

UT_TEST(test_provider_component_requires_exact_mapping_and_local_identity)
{
	uint64 configured[2] = { 3, 0 };
	uint32 map[CLUSTER_MAX_NODES];
	uint32 ids[] = { 11, 12 };
	ClusterStorageQuorumView view;
	bool parsed = cluster_storage_quorum_parse_nodes("0:11,1:12", configured, map);

	UT_ASSERT(parsed);
	UT_ASSERT(cluster_storage_quorum_decode_component(&view, 11, 8, 1, ids, 2, 11, 0, map));
	UT_ASSERT_EQ(view.members[0], 3);
	UT_ASSERT(!cluster_storage_quorum_decode_component(&view, 11, 8, 1, ids, 2, 12, 0, map));
	ids[1] = 11;
	UT_ASSERT(!cluster_storage_quorum_decode_component(&view, 11, 8, 1, ids, 2, 11, 0, map));
	ids[1] = 13;
	UT_ASSERT(!cluster_storage_quorum_decode_component(&view, 11, 8, 1, ids, 2, 11, 0, map));
	ids[1] = 12;
	UT_ASSERT(!cluster_storage_quorum_decode_component(&view, 11, 8, 0, ids, 2, 11, 0, map));
	UT_ASSERT(!cluster_storage_quorum_decode_component(&view, 11, 0, 1, ids, 2, 11, 0, map));
	UT_ASSERT(!cluster_storage_quorum_decode_component(&view, 13, 8, 1, ids, 2, 11, 0, map));
}

UT_TEST(test_current_component_gates_self_peers_and_candidate_as_one_snapshot)
{
	ClusterStorageQuorumView view;
	ready();
	UT_ASSERT(cluster_storage_quorum_allows_node(0));
	UT_ASSERT(cluster_storage_quorum_allows_node(1));
	UT_ASSERT(!cluster_storage_quorum_allows_node(2));
	UT_ASSERT(cluster_storage_quorum_allows_members(3, 0));
	UT_ASSERT(!cluster_storage_quorum_allows_members(5, 0));
	UT_ASSERT(!cluster_storage_quorum_allows_members(0, 1));
	UT_ASSERT(!cluster_storage_quorum_allows_members(0, 0));
	UT_ASSERT(cluster_storage_quorum_snapshot(&view));
	UT_ASSERT_EQ(view.ring_sequence, 8);
	UT_ASSERT_EQ(view.expires_us, 150);
}

UT_TEST(test_expiry_and_clock_reversal_do_not_renew_evidence)
{
	ready();
	fake_now = 149;
	fake_monotonic = 149;
	UT_ASSERT(cluster_storage_quorum_allows_node(0));
	fake_now = 150;
	fake_monotonic = 150;
	UT_ASSERT(!cluster_storage_quorum_allows_node(0));
	fake_now = 99;
	fake_monotonic = 99;
	UT_ASSERT(!cluster_storage_quorum_allows_node(0));
	fake_now = 150;
	fake_monotonic = 150;
	UT_ASSERT_EQ(pg_atomic_read_u64(&test_state.expires_us), 150);
}

UT_TEST(test_provider_failure_and_partial_publication_fail_closed)
{
	ready();
	supplied.reason = CLUSTER_STORAGE_QUORUM_UNAVAILABLE;
	cluster_storage_quorum_refresh(101, 50);
	UT_ASSERT(!cluster_storage_quorum_allows_node(0));
	UT_ASSERT_EQ(pg_atomic_read_u64(&test_state.members[0]), 0);
	ready();
	pg_atomic_fetch_add_u32(&test_state.sequence, 1);
	UT_ASSERT(!cluster_storage_quorum_allows_node(0));
	ready();
	cluster_storage_quorum_refresh(UINT64_MAX - 5, 50);
	UT_ASSERT(!cluster_storage_quorum_allows_node(0));
}

UT_TEST(test_new_storage_component_revokes_old_targets)
{
	ready();
	supplied.ring_sequence = 12;
	supplied.members[0] = 1;
	cluster_storage_quorum_refresh(101, 50);
	fake_now = 101;
	fake_monotonic = 101;
	UT_ASSERT(cluster_storage_quorum_allows_node(0));
	UT_ASSERT(!cluster_storage_quorum_allows_node(1));
	UT_ASSERT(!cluster_storage_quorum_allows_members(3, 0));
	supplied.members[0] = 2;
	cluster_storage_quorum_refresh(102, 50);
	fake_now = 102;
	fake_monotonic = 102;
	UT_ASSERT(!cluster_storage_quorum_allows_node(0));
	UT_ASSERT(!cluster_storage_quorum_allows_node(1));
}

UT_TEST(test_native_profile_preserved_and_uninitialized_shared_refused)
{
	cluster_storage_quorum_attach(NULL, false);
	UT_ASSERT(!cluster_storage_quorum_allows_node(0));
	cluster_shared_config = false;
	UT_ASSERT(cluster_storage_quorum_allows_node(0));
	cluster_shared_config = true;
}

UT_TEST(test_wall_clock_rollback_cannot_extend_storage_eligibility)
{
	ready();
	fake_monotonic = 151;
	fake_now = 125;
	UT_ASSERT(!cluster_storage_quorum_allows_node(0));
}

UT_TEST(test_refusal_capture_keeps_the_actual_expired_sample)
{
	ClusterStorageQuorumCheck check;

	ready();
	fake_monotonic = 150;
	UT_ASSERT(!cluster_storage_quorum_check_node(0, &check));
	UT_ASSERT_EQ(check.result, CLUSTER_STORAGE_CHECK_EXPIRED);
	UT_ASSERT(check.stable);
	UT_ASSERT_EQ(check.attempts, 1);
	UT_ASSERT_EQ(check.sequence_before, check.sequence_after);
	UT_ASSERT_EQ(check.now_us, 150);
	UT_ASSERT_EQ(check.view.sampled_us, 100);
	UT_ASSERT_EQ(check.view.expires_us, 150);
	UT_ASSERT_EQ(check.view.generation, 1);
	UT_ASSERT_EQ(check.view.members[0], 3);
	/* A later successful publication cannot rewrite the rejection evidence. */
	cluster_storage_quorum_refresh(150, 50);
	UT_ASSERT(cluster_storage_quorum_allows_node(0));
	UT_ASSERT_EQ(check.view.generation, 1);
	UT_ASSERT_EQ(check.view.expires_us, 150);
	UT_ASSERT_EQ(check.result, CLUSTER_STORAGE_CHECK_EXPIRED);
}

UT_TEST(test_refusal_capture_distinguishes_unsampled_and_unstable)
{
	ClusterStorageQuorumCheck check;
	ClusterStorageQuorumView zero = { 0 };

	memset(&zero, 0, sizeof(zero));
	ready();
	pg_atomic_fetch_add_u32(&test_state.sequence, 1);
	UT_ASSERT(!cluster_storage_quorum_check_node(0, &check));
	UT_ASSERT_EQ(check.result, CLUSTER_STORAGE_CHECK_UNSTABLE);
	UT_ASSERT(!check.stable);
	UT_ASSERT_EQ(check.attempts, 4);
	UT_ASSERT(check.sequence_before & 1);
	UT_ASSERT_EQ(check.now_us, 0);
	UT_ASSERT_EQ(memcmp(&check.view, &zero, sizeof(zero)), 0);
	cluster_storage_quorum_attach(NULL, false);
	UT_ASSERT(!cluster_storage_quorum_check_node(0, &check));
	UT_ASSERT_EQ(check.result, CLUSTER_STORAGE_CHECK_UNATTACHED);
	UT_ASSERT_EQ(check.attempts, 0);
	UT_ASSERT(!check.stable);
	UT_ASSERT_EQ(memcmp(&check.view, &zero, sizeof(zero)), 0);
	ready();
}

UT_TEST(test_refusal_capture_does_not_change_member_or_clock_polarity)
{
	ClusterStorageQuorumCheck check;

	ready();
	UT_ASSERT(cluster_storage_quorum_check_node(0, &check));
	UT_ASSERT_EQ(check.result, CLUSTER_STORAGE_CHECK_ALLOWED);
	UT_ASSERT_EQ(check.target_node, 0);
	UT_ASSERT_EQ(check.self_node, 0);
	UT_ASSERT(!cluster_storage_quorum_check_node(2, &check));
	UT_ASSERT_EQ(check.result, CLUSTER_STORAGE_CHECK_TARGET_ABSENT);
	fake_monotonic = 99;
	UT_ASSERT(!cluster_storage_quorum_check_node(0, &check));
	UT_ASSERT_EQ(check.result, CLUSTER_STORAGE_CHECK_CLOCK_BEFORE_SAMPLE);
	fake_monotonic = 100;
	supplied.members[0] = 2;
	cluster_storage_quorum_refresh(100, 50);
	UT_ASSERT(!cluster_storage_quorum_check_node(1, &check));
	UT_ASSERT_EQ(check.result, CLUSTER_STORAGE_CHECK_SELF_ABSENT);
	UT_ASSERT(!cluster_storage_quorum_check_node(CLUSTER_MAX_NODES, &check));
	UT_ASSERT_EQ(check.result, CLUSTER_STORAGE_CHECK_INVALID_TARGET);
	UT_ASSERT_EQ(check.attempts, 0);
	cluster_shared_config = false;
	UT_ASSERT(cluster_storage_quorum_check_node(-1, &check));
	UT_ASSERT_EQ(check.result, CLUSTER_STORAGE_CHECK_NATIVE);
	UT_ASSERT(!check.stable);
	cluster_shared_config = true;
	ready();
}

UT_TEST(test_provider_failure_detail_travels_with_the_rejected_generation)
{
	ClusterStorageQuorumCheck check;

	ready();
	supplied.reason = CLUSTER_STORAGE_QUORUM_UNAVAILABLE;
	supplied.provider_diagnostic
		= CLUSTER_STORAGE_PROVIDER_DIAGNOSTIC(CLUSTER_STORAGE_PROVIDER_TRACK_CURRENT, 2);
	cluster_storage_quorum_refresh(101, 50);
	UT_ASSERT(!cluster_storage_quorum_check_node(0, &check));
	UT_ASSERT_EQ(check.result, CLUSTER_STORAGE_CHECK_PROVIDER);
	UT_ASSERT(check.stable);
	UT_ASSERT_EQ(check.view.generation, 2);
	UT_ASSERT_EQ(check.view.members[0], 0);
	UT_ASSERT_EQ(check.view.provider_diagnostic, supplied.provider_diagnostic);
	ready();
	UT_ASSERT(cluster_storage_quorum_allows_node(0));
	UT_ASSERT_EQ(check.view.generation, 2);
	UT_ASSERT_EQ(check.result, CLUSTER_STORAGE_CHECK_PROVIDER);
}

UT_TEST(test_incomplete_never_retains_invalid_or_expired_evidence)
{
	unsigned scenario;

	for (scenario = 0; scenario < 8; scenario++) {
		uint64 start = 101;
		uint64 duration = 50;

		ready();
		fake_monotonic = 101;
		switch (scenario) {
		case 0:
			fake_monotonic = 150;
			break;
		case 1:
			fake_monotonic = 99;
			break;
		case 2:
			start = 0;
			break;
		case 3:
			duration = 0;
			break;
		case 4:
			start = UINT64_MAX - 5;
			break;
		case 5:
			pg_atomic_write_u64(&test_state.generation, UINT64_MAX);
			break;
		case 6:
			pg_atomic_write_u32(&test_state.reason, CLUSTER_STORAGE_QUORUM_NOT_QUORATE);
			break;
		case 7:
			pg_atomic_write_u64(&test_state.members[0], 2);
			break;
		}
		memset(&supplied, 0, sizeof(supplied));
		supplied.reason = CLUSTER_STORAGE_QUORUM_INCOMPLETE;
		cluster_storage_quorum_refresh(start, duration);
		UT_ASSERT(!cluster_storage_quorum_allows_node(0));
		UT_ASSERT_EQ(pg_atomic_read_u64(&test_state.members[0]), 0);
	}
}

int
main(void)
{
	UT_PLAN(13);
	UT_RUN(test_incomplete_never_retains_invalid_or_expired_evidence);
	UT_RUN(test_mapping_rejects_aliases_missing_slots_and_overflow);
	UT_RUN(test_provider_component_requires_exact_mapping_and_local_identity);
	UT_RUN(test_current_component_gates_self_peers_and_candidate_as_one_snapshot);
	UT_RUN(test_expiry_and_clock_reversal_do_not_renew_evidence);
	UT_RUN(test_provider_failure_and_partial_publication_fail_closed);
	UT_RUN(test_new_storage_component_revokes_old_targets);
	UT_RUN(test_native_profile_preserved_and_uninitialized_shared_refused);
	UT_RUN(test_wall_clock_rollback_cannot_extend_storage_eligibility);
	UT_RUN(test_refusal_capture_keeps_the_actual_expired_sample);
	UT_RUN(test_refusal_capture_distinguishes_unsampled_and_unstable);
	UT_RUN(test_refusal_capture_does_not_change_member_or_clock_polarity);
	UT_RUN(test_provider_failure_detail_travels_with_the_rejected_generation);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
