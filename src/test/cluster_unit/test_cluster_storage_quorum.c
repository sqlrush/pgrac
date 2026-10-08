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
#include <errno.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
static uint64 fake_monotonic = 100;
static int storage_test_clock_gettime(clockid_t clock_id, struct timespec *out);
static void storage_test_usleep(long microsec);
#define clock_gettime storage_test_clock_gettime
#define pg_usleep storage_test_usleep
#include "../../backend/cluster/cluster_storage_quorum.c"
#undef pg_usleep
#undef clock_gettime
#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_shared_config = true;
int cluster_node_id = 0;
static TimestampTz fake_now = 100;
static ClusterStorageQuorumState test_state;
static ClusterStorageQuorumView supplied;
static int publisher_ready_fd = -1, publisher_go_fd = -1;
static int reader_release_fd = -1, reader_done_fd = -1;
static unsigned snapshot_sleeps;
static uint64 snapshot_sleep_us;
static unsigned snapshot_sleep_mode;
static const uint64 *reader_clock_samples;
static unsigned reader_clock_index;
static unsigned reader_release_after_sleeps = 1;

typedef struct StorageClockScenario {
	uint64 samples[4];
	unsigned release_after_sleeps;
	ClusterStorageCheckResult expected;
	uint64 expires_us;
} StorageClockScenario;

static bool
pipe_byte(int fd, bool writing, char value)
{
	char actual = value;
	ssize_t n;

	do {
		n = writing ? write(fd, &actual, 1) : read(fd, &actual, 1);
	} while (n < 0 && errno == EINTR);
	return n == 1 && actual == value;
}

static void
storage_test_usleep(long microsec)
{
	snapshot_sleeps++;
	snapshot_sleep_us += microsec;
	fake_monotonic += microsec;
	if (snapshot_sleep_mode == 1)
		fake_monotonic += 2000; /* The OS may oversleep the requested delay. */
	else if (snapshot_sleep_mode == 2)
		fake_monotonic = 400; /* Before wait start, but after the new sample. */
	else if (snapshot_sleep_mode == 3)
		fake_monotonic = 0;
	/* Release the real concurrent publisher at the first reader yield. */
	if (reader_release_fd >= 0 && snapshot_sleeps >= reader_release_after_sleeps) {
		UT_ASSERT(pipe_byte(reader_release_fd, true, 'g'));
		UT_ASSERT(pipe_byte(reader_done_fd, false, 'd'));
		reader_release_fd = -1;
	}
}

static int
storage_test_clock_gettime(clockid_t clock_id, struct timespec *out)
{
	Assert(clock_id == CLOCK_MONOTONIC);
	if (publisher_ready_fd >= 0) {
		/* The real refresh has entered its short, odd publication section. */
		if (!(pg_atomic_read_u32(&storage_state->sequence) & 1)
			|| !pipe_byte(publisher_ready_fd, true, 'r') || !pipe_byte(publisher_go_fd, false, 'g'))
			_exit(3);
		publisher_ready_fd = -1;
	}
	if (reader_clock_samples != NULL) {
		unsigned index = Min(reader_clock_index, 3);

		reader_clock_index++;
		fake_monotonic = reader_clock_samples[index];
	}
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
	snapshot_sleeps = 0;
	snapshot_sleep_us = 0;
	snapshot_sleep_mode = 0;
	reader_clock_samples = NULL;
	reader_clock_index = 0;
	reader_release_after_sleeps = 1;
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

/* Two local unit processes share only the real quorum state, not a database.
 * Pipe handshakes place the read exactly inside the real publisher's odd cut. */
static void
concurrent_publication(unsigned scenario, const StorageClockScenario *clock_case, unsigned reader)
{
	ClusterStorageQuorumState *shared;
	ClusterStorageQuorumCheck check;
	uint64 prior_loss;
	int to_child[2], from_child[2], status;
	pid_t child;
	bool allowed;

	ready();
	if (clock_case != NULL)
		reader_release_after_sleeps = clock_case->release_after_sleeps;
	if (scenario >= 4) {
		snapshot_sleep_mode = scenario - 3;
		fake_monotonic = 500;
	}
	shared = mmap(NULL, sizeof(*shared), PROT_READ | PROT_WRITE, MAP_ANON | MAP_SHARED, -1, 0);
	UT_ASSERT(shared != MAP_FAILED);
	if (shared == MAP_FAILED)
		return;
	memcpy(shared, &test_state, sizeof(*shared));
	cluster_storage_quorum_attach(shared, false);
	prior_loss = pg_atomic_read_u64(&shared->loss_generation);
	if (pipe(to_child) != 0) {
		UT_ASSERT(false);
		goto detach;
	}
	if (pipe(from_child) != 0) {
		UT_ASSERT(false);
		close(to_child[0]);
		close(to_child[1]);
		goto detach;
	}
	child = fork();
	if (child == 0) {
		/* A broken test handshake exits, rather than leaving a stuck child. */
		alarm(5);
		close(to_child[1]);
		close(from_child[0]);
		fake_monotonic = clock_case != NULL ? 900 : scenario >= 4 ? 501 : 101;
		supplied.ring_sequence++;
		supplied.members[0] = scenario == 2 ? 2 : 1;
		if (scenario == 1)
			supplied.reason = CLUSTER_STORAGE_QUORUM_NOT_QUORATE;
		publisher_ready_fd = from_child[1];
		publisher_go_fd = to_child[0];
		cluster_storage_quorum_refresh(clock_case != NULL ? 900 : 101,
									   clock_case != NULL ? clock_case->expires_us - 900
									   : scenario == 3	  ? 50
														  : 1000000);
		_exit(pipe_byte(from_child[1], true, 'd') ? 0 : 4);
	}
	close(to_child[0]);
	close(from_child[1]);
	UT_ASSERT(child > 0);
	if (child > 0) {
		UT_ASSERT(pipe_byte(from_child[0], false, 'r'));
		reader_release_fd = to_child[1];
		reader_done_fd = from_child[0];
		if (clock_case != NULL)
			reader_clock_samples = clock_case->samples;
		if (reader == 1)
			allowed = cluster_storage_quorum_allows_node(0);
		else if (reader == 2)
			allowed = cluster_storage_quorum_allows_members(1, 0);
		else if (reader == 3) {
			ClusterStorageQuorumView view;
			ClusterStorageQuorumView zero = { 0 };

			memset(&view, 0xff, sizeof(view));
			allowed = cluster_storage_quorum_snapshot(&view);
			if (!allowed)
				UT_ASSERT_EQ(memcmp(&view, &zero, sizeof(view)), 0);
		} else
			allowed = cluster_storage_quorum_check_node(0, &check);
		UT_ASSERT_EQ(snapshot_sleeps, 1);
		if (clock_case != NULL) {
			UT_ASSERT_EQ(allowed, clock_case->expected == CLUSTER_STORAGE_CHECK_ALLOWED);
			if (reader == 0) {
				bool stable = clock_case->expected != CLUSTER_STORAGE_CHECK_UNSTABLE;
				ClusterStorageQuorumView zero = { 0 };

				UT_ASSERT_EQ(check.result, clock_case->expected);
				UT_ASSERT_EQ(check.stable, stable);
				UT_ASSERT_EQ(check.view.generation, stable ? 2 : 0);
				UT_ASSERT_EQ(check.now_us, stable ? clock_case->samples[3] : 0);
				if (!stable)
					UT_ASSERT_EQ(memcmp(&check.view, &zero, sizeof(zero)), 0);
			}
		} else if (scenario >= 4) {
			UT_ASSERT(!allowed);
			UT_ASSERT(!check.stable);
			UT_ASSERT_EQ(check.result, CLUSTER_STORAGE_CHECK_UNSTABLE);
			UT_ASSERT_EQ(check.view.generation, 0);
			UT_ASSERT_EQ(check.now_us, 0);
			UT_ASSERT_EQ(check.attempts, 4);
			/* The original 439 tuple cannot distinguish an overslept yield
			 * from a failed clock. Keep the sampled cause without granting. */
			UT_ASSERT_EQ(check.wait_count, 1);
			UT_ASSERT_EQ(check.wait_started_us, 500);
			UT_ASSERT_EQ(check.wait_sampled_us, scenario == 4 ? 2600 : scenario == 5 ? 400 : 0);
			UT_ASSERT_EQ(check.snapshot_stop, scenario == 4 ? CLUSTER_STORAGE_SNAPSHOT_DEADLINE
											  : scenario == 5
												  ? CLUSTER_STORAGE_SNAPSHOT_CLOCK_REGRESSED
												  : CLUSTER_STORAGE_SNAPSHOT_CLOCK_UNAVAILABLE);
		} else {
			UT_ASSERT_EQ(allowed, scenario == 0);
			UT_ASSERT(check.stable);
			UT_ASSERT_EQ(check.result, scenario == 0   ? CLUSTER_STORAGE_CHECK_ALLOWED
									   : scenario == 1 ? CLUSTER_STORAGE_CHECK_PROVIDER
									   : scenario == 2 ? CLUSTER_STORAGE_CHECK_SELF_ABSENT
													   : CLUSTER_STORAGE_CHECK_EXPIRED);
			UT_ASSERT_EQ(check.view.generation, 2);
			UT_ASSERT_EQ(check.attempts, 5);
		}
		if (scenario == 0 && clock_case == NULL) {
			UT_ASSERT_EQ(check.view.ring_sequence, 9);
			UT_ASSERT_EQ(check.view.members[0], 1);
			UT_ASSERT_EQ(check.view.loss_generation, prior_loss);
			UT_ASSERT(!cluster_storage_quorum_allows_node(1));
		}
		/* RED returns before yielding: release and reap the original writer. */
		if (reader_release_fd >= 0) {
			UT_ASSERT(pipe_byte(to_child[1], true, 'g'));
			UT_ASSERT(pipe_byte(from_child[0], false, 'd'));
		}
		UT_ASSERT_EQ(waitpid(child, &status, 0), child);
		UT_ASSERT(WIFEXITED(status) && WEXITSTATUS(status) == 0);
		if (clock_case != NULL) {
			UT_ASSERT_EQ(pg_atomic_read_u64(&shared->expires_us), clock_case->expires_us);
			UT_ASSERT_EQ(pg_atomic_read_u64(&shared->loss_generation), prior_loss + 1);
		}
	}
	reader_clock_samples = NULL;
	reader_release_fd = reader_done_fd = -1;
	close(to_child[1]);
	close(from_child[0]);
detach:
	cluster_storage_quorum_attach(&test_state, false);
	UT_ASSERT_EQ(munmap(shared, sizeof(*shared)), 0);
}

UT_TEST(test_concurrent_publication_is_waited_for_not_reported_as_loss)
{
	concurrent_publication(0, NULL, 0);
}

UT_TEST(test_concurrent_loss_or_expiry_never_uses_previous_positive_view)
{
	for (unsigned scenario = 1; scenario <= 3; scenario++)
		concurrent_publication(scenario, NULL, 0);
}

UT_TEST(test_completed_publisher_does_not_hide_oversleep_or_clock_failure)
{
	for (unsigned scenario = 4; scenario <= 6; scenario++)
		concurrent_publication(scenario, NULL, 0);
}

UT_TEST(test_mid_wait_clock_regression_above_start_never_qualifies)
{
	const StorageClockScenario cases[]
		= { /* The publisher completes at the first yield; its view has already
		 * expired at 1600, before the copy's final clock falls to 1500. */
			{ { 1000, 1600, 1500, 1500 }, 1, CLUSTER_STORAGE_CHECK_UNSTABLE, 1550 },
			/* Keep the writer odd: the next pre-sleep check must remember the
		 * previous post-sleep clock, even though the wait began at 1000. */
			{ { 1000, 1600, 1500, 1500 }, 2, CLUSTER_STORAGE_CHECK_UNSTABLE, 1550 }
		  };

	for (unsigned c = 0; c < lengthof(cases); c++)
		for (unsigned reader = 0; reader < 4; reader++)
			concurrent_publication(0, &cases[c], reader);
}

UT_TEST(test_final_qualification_cannot_reaccept_an_expired_view_after_clock_regression)
{
	const StorageClockScenario clock_case
		= { { 1000, 1100, 1600, 1500 }, 1, CLUSTER_STORAGE_CHECK_UNSTABLE, 1550 };

	for (unsigned reader = 0; reader < 3; reader++)
		concurrent_publication(0, &clock_case, reader);
}

UT_TEST(test_final_qualification_keeps_zero_clock_and_original_wait_deadline)
{
	const StorageClockScenario cases[]
		= { { { 1000, 1100, 1200, 0 }, 1, CLUSTER_STORAGE_CHECK_UNSTABLE, 1550 },
			/* The lease remains valid, but the original 1ms wait budget does not. */
			{ { 1000, 1100, 1200, 2000 }, 1, CLUSTER_STORAGE_CHECK_UNSTABLE, 3000 } };

	for (unsigned c = 0; c < lengthof(cases); c++)
		for (unsigned reader = 0; reader < 3; reader++)
			concurrent_publication(0, &cases[c], reader);
}

UT_TEST(test_monotonic_qualification_keeps_success_and_expiry_polarity)
{
	const StorageClockScenario cases[]
		= { { { 1000, 1000, 1000, 1000 }, 1, CLUSTER_STORAGE_CHECK_ALLOWED, 1550 },
			{ { 1000, 1100, 1500, 1549 }, 1, CLUSTER_STORAGE_CHECK_ALLOWED, 1550 },
			{ { 1000, 1100, 1500, 1550 }, 1, CLUSTER_STORAGE_CHECK_EXPIRED, 1550 },
			{ { 1000, 1100, 1600, 1600 }, 1, CLUSTER_STORAGE_CHECK_EXPIRED, 1550 } };

	for (unsigned c = 0; c < lengthof(cases); c++)
		for (unsigned reader = 0; reader < 3; reader++)
			concurrent_publication(0, &cases[c], reader);
}

UT_TEST(test_stuck_publisher_wait_is_bounded_and_does_not_change_loss_history)
{
	ClusterStorageQuorumCheck check;
	uint64 loss;

	ready();
	loss = pg_atomic_read_u64(&test_state.loss_generation);
	pg_atomic_fetch_add_u32(&test_state.sequence, 1);
	UT_ASSERT(!cluster_storage_quorum_check_node(0, &check));
	UT_ASSERT_EQ(check.result, CLUSTER_STORAGE_CHECK_UNSTABLE);
	UT_ASSERT(!check.stable);
	UT_ASSERT_EQ(check.view.generation, 0);
	UT_ASSERT_EQ(check.now_us, 0);
	UT_ASSERT_EQ(check.attempts, 13); /* The last sleep reaches the deadline. */
	UT_ASSERT_EQ(snapshot_sleeps, 10);
	UT_ASSERT_EQ(snapshot_sleep_us, 1000);
	UT_ASSERT_EQ(pg_atomic_read_u64(&test_state.loss_generation), loss);
	ready();
	supplied.reason = CLUSTER_STORAGE_QUORUM_NOT_QUORATE;
	cluster_storage_quorum_refresh(100, 50);
	UT_ASSERT(!cluster_storage_quorum_check_node(0, &check));
	UT_ASSERT_EQ(check.result, CLUSTER_STORAGE_CHECK_PROVIDER);
	UT_ASSERT_EQ(check.attempts, 1);
	UT_ASSERT_EQ(snapshot_sleeps, 0);
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
	UT_ASSERT_EQ(check.attempts, 13); /* No read after the final sleep reaches 1ms. */
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
	UT_PLAN(21);
	UT_RUN(test_mid_wait_clock_regression_above_start_never_qualifies);
	UT_RUN(test_final_qualification_cannot_reaccept_an_expired_view_after_clock_regression);
	UT_RUN(test_final_qualification_keeps_zero_clock_and_original_wait_deadline);
	UT_RUN(test_monotonic_qualification_keeps_success_and_expiry_polarity);
	UT_RUN(test_concurrent_publication_is_waited_for_not_reported_as_loss);
	UT_RUN(test_concurrent_loss_or_expiry_never_uses_previous_positive_view);
	UT_RUN(test_completed_publisher_does_not_hide_oversleep_or_clock_failure);
	UT_RUN(test_stuck_publisher_wait_is_bounded_and_does_not_change_loss_history);
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
