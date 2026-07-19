/*-------------------------------------------------------------------------
 *
 * test_cluster_pcm_own.c
 *	  C1 ownership-reservation and D5a buffer-reuse contract tests.
 *
 * This binary links the production cluster_pcm_own object.  Buffer-manager
 * behavior that cannot be linked standalone is covered in two paired ways:
 * the real reusable decision helpers are exercised here, and the production
 * bufmgr source is checked to prove both eviction paths call those helpers
 * before dropping header authority and use the saved-tag release API.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_pcm_own.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_shmem.h"

#include "unit_test.h"

#include <limits.h>

UT_DEFINE_GLOBALS();

int NBuffers = 4;

static union {
	uint64 align;
	char bytes[4096];
} fake_shmem;
static bool fake_found;

static char *read_bufmgr_source(void);
static void assert_ordered_in_function(const char *source, const char *function_start,
									   const char *function_end, const char *const *needles,
									   int needle_count);
static void assert_source_range_contains(const char *start, const char *end, const char *needle);

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *foundPtr)
{
	UT_ASSERT(size <= sizeof(fake_shmem.bytes));
	*foundPtr = fake_found;
	fake_found = true;
	return fake_shmem.bytes;
}

Size
mul_size(Size s1, Size s2)
{
	return s1 * s2;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

static void
reset_fixture(void)
{
	memset(&fake_shmem, 0xA5, sizeof(fake_shmem));
	fake_found = false;
	ClusterPcmOwnArray = NULL;
	cluster_pcm_own_shmem_init();
}

static void
assert_entry(uint64 generation, uint64 token, uint32 flags)
{
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterPcmOwnArray[0].generation), generation);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterPcmOwnArray[0].reservation_token), token);
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterPcmOwnArray[0].flags), flags);
}

UT_TEST(test_shmem_initializes_complete_entry)
{
	int i;

	reset_fixture();
	UT_ASSERT_EQ(cluster_pcm_own_shmem_size(), (Size)NBuffers * sizeof(ClusterPcmOwnEntry));
	UT_ASSERT_EQ(sizeof(ClusterPcmOwnEntry), 24);
	for (i = 0; i < NBuffers; i++) {
		UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterPcmOwnArray[i].generation), 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterPcmOwnArray[i].reservation_token), 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterPcmOwnArray[i].flags), 0);
	}
}

UT_TEST(test_begin_abort_is_exact_and_monotonic)
{
	uint64 token = UINT64_MAX;

	reset_fixture();
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(token, 1);
	assert_entry(0, 1, PCM_OWN_FLAG_GRANT_PENDING);

	/* A second begin cannot overwrite or advance the live token. */
	token = UINT64_MAX;
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_BUSY);
	UT_ASSERT_EQ(token, 0);
	assert_entry(0, 1, PCM_OWN_FLAG_GRANT_PENDING);

	/* Old/wrong cleanup is a strict no-op. */
	UT_ASSERT_EQ(cluster_pcm_own_reservation_abort_exact(0, 0, 2, PCM_OWN_FLAG_GRANT_PENDING),
				 CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_abort_exact(0, 1, 1, PCM_OWN_FLAG_GRANT_PENDING),
				 CLUSTER_PCM_OWN_STALE);
	assert_entry(0, 1, PCM_OWN_FLAG_GRANT_PENDING);

	UT_ASSERT_EQ(cluster_pcm_own_reservation_abort_exact(0, 0, 1, PCM_OWN_FLAG_GRANT_PENDING),
				 CLUSTER_PCM_OWN_OK);
	assert_entry(0, 1, 0);

	/* A delayed duplicate abort cannot clear the next reservation. */
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(token, 2);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_abort_exact(0, 0, 1, PCM_OWN_FLAG_GRANT_PENDING),
				 CLUSTER_PCM_OWN_STALE);
	assert_entry(0, 2, PCM_OWN_FLAG_GRANT_PENDING);
}

UT_TEST(test_invalid_live_flag_shapes_are_corrupt_not_busy)
{
	static const char *const classifier_contract[]
		= { "cluster_pcm_own_reservation_token_get", "cluster_pcm_own_flags_get",
			"cluster_pcm_own_classify_live_flags", "live_result != CLUSTER_PCM_OWN_OK",
			"return live_result" };
	char *source;
	uint64 token = UINT64_MAX;

	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, 7);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags,
						PCM_OWN_FLAG_GRANT_PENDING | PCM_OWN_FLAG_REVOKING);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_CORRUPT);
	UT_ASSERT_EQ(token, 0);
	assert_entry(0, 7, PCM_OWN_FLAG_GRANT_PENDING | PCM_OWN_FLAG_REVOKING);

	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, 7);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags, (uint32)0x4);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_CORRUPT);
	UT_ASSERT_EQ(token, 0);
	assert_entry(0, 7, (uint32)0x4);

	/* Even a recognized singleton flag is corrupt without a published token. */
	reset_fixture();
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags, PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_CORRUPT);
	UT_ASSERT_EQ(token, 0);
	assert_entry(0, 0, PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(cluster_pcm_own_classify_live_flags(0, 0), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_own_classify_live_flags(0, 7), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_own_classify_live_flags(PCM_OWN_FLAG_GRANT_PENDING, 7),
				 CLUSTER_PCM_OWN_BUSY);
	UT_ASSERT_EQ(cluster_pcm_own_classify_live_flags(PCM_OWN_FLAG_REVOKING, 7),
				 CLUSTER_PCM_OWN_BUSY);
	UT_ASSERT_EQ(
		cluster_pcm_own_classify_live_flags(PCM_OWN_FLAG_GRANT_PENDING | PCM_OWN_FLAG_REVOKING, 7),
		CLUSTER_PCM_OWN_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_own_classify_live_flags(PCM_OWN_FLAG_GRANT_PENDING, 0),
				 CLUSTER_PCM_OWN_CORRUPT);

	source = read_bufmgr_source();
	assert_ordered_in_function(source, "\ncluster_pcm_own_bump_failure(", "\nstatic ",
							   classifier_contract, lengthof(classifier_contract));
	free(source);
}

UT_TEST(test_grant_commit_is_exact_and_bumps_once)
{
	uint64 token;
	uint64 committed = UINT64_MAX;

	reset_fixture();
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_OK);

	UT_ASSERT_EQ(cluster_pcm_own_grant_commit_exact(0, 0, token + 1, &committed),
				 CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(committed, 0);
	assert_entry(0, token, PCM_OWN_FLAG_GRANT_PENDING);

	UT_ASSERT_EQ(cluster_pcm_own_grant_commit_exact(0, 0, token, &committed), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(committed, 1);
	assert_entry(1, token, 0);

	committed = UINT64_MAX;
	UT_ASSERT_EQ(cluster_pcm_own_grant_commit_exact(0, 0, token, &committed),
				 CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(committed, 0);
	assert_entry(1, token, 0);

	/* A competing well-formed lifecycle is BUSY; a malformed tuple is
	 * corruption.  Neither may be flattened into a retryable stale result. */
	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, 9);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags, PCM_OWN_FLAG_REVOKING);
	UT_ASSERT_EQ(cluster_pcm_own_grant_commit_exact(0, 0, 9, &committed), CLUSTER_PCM_OWN_BUSY);
	assert_entry(0, 9, PCM_OWN_FLAG_REVOKING);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags,
						PCM_OWN_FLAG_REVOKING | PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(cluster_pcm_own_grant_commit_exact(0, 0, 9, &committed), CLUSTER_PCM_OWN_CORRUPT);
	assert_entry(0, 9, PCM_OWN_FLAG_REVOKING | PCM_OWN_FLAG_GRANT_PENDING);
}

UT_TEST(test_s_revoke_handoff_reuses_exact_token_and_bumps_once)
{
	uint64 committed = UINT64_MAX;
	uint64 token;

	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].generation, 7);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 7, PCM_OWN_FLAG_REVOKING, &token),
				 CLUSTER_PCM_OWN_OK);
	assert_entry(7, token, PCM_OWN_FLAG_REVOKING);

	/* Stale identities cannot steal or rewrite the source revoke lifecycle. */
	UT_ASSERT_EQ(cluster_pcm_own_revoke_to_grant_handoff_exact(0, 8, token), CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(cluster_pcm_own_revoke_to_grant_handoff_exact(0, 7, token + 1),
				 CLUSTER_PCM_OWN_STALE);
	assert_entry(7, token, PCM_OWN_FLAG_REVOKING);

	/* Handoff changes only the role of the same source lifecycle. */
	UT_ASSERT_EQ(cluster_pcm_own_revoke_to_grant_handoff_exact(0, 7, token), CLUSTER_PCM_OWN_OK);
	assert_entry(7, token, PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(cluster_pcm_own_revoke_to_grant_handoff_exact(0, 7, token), CLUSTER_PCM_OWN_OK);
	assert_entry(7, token, PCM_OWN_FLAG_GRANT_PENDING);

	/* Malformed live tuples are corruption, never a stale/duplicate handoff. */
	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].generation, 7);
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, token);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags,
						PCM_OWN_FLAG_REVOKING | PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(cluster_pcm_own_revoke_to_grant_handoff_exact(0, 7, token),
				 CLUSTER_PCM_OWN_CORRUPT);
	assert_entry(7, token, PCM_OWN_FLAG_REVOKING | PCM_OWN_FLAG_GRANT_PENDING);
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, 0);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags, PCM_OWN_FLAG_REVOKING);
	UT_ASSERT_EQ(cluster_pcm_own_revoke_to_grant_handoff_exact(0, 7, token),
				 CLUSTER_PCM_OWN_CORRUPT);
	assert_entry(7, 0, PCM_OWN_FLAG_REVOKING);

	/* Restore the exact handed-off tuple before its sole generation bump. */
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, token);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags, PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(cluster_pcm_own_grant_commit_exact(0, 7, token, &committed), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(committed, 8);
	assert_entry(8, token, 0);
	UT_ASSERT_EQ(cluster_pcm_own_revoke_to_grant_handoff_exact(0, 7, token), CLUSTER_PCM_OWN_STALE);
}

UT_TEST(test_revoke_handoff_kinds_cover_n_s_x_with_one_lifecycle)
{
	ClusterPcmOwnSnapshot base;
	ClusterPcmOwnSnapshot live;
	uint64 committed = UINT64_MAX;
	uint64 token;
	uint8 states[] = { (uint8)PCM_STATE_N, (uint8)PCM_STATE_S, (uint8)PCM_STATE_X };
	ClusterPcmXGrantReservationKind expected_kinds[]
		= { CLUSTER_PCM_X_GRANT_RESERVATION_N_REVOKE_HANDOFF,
			CLUSTER_PCM_X_GRANT_RESERVATION_S_REVOKE_HANDOFF,
			CLUSTER_PCM_X_GRANT_RESERVATION_X_REVOKE_HANDOFF };
	int i;

	/* The added handoff arms must not broaden or shadow the ordinary new-token
	 * N reservation. */
	memset(&base, 0, sizeof(base));
	base.generation = 7;
	base.reservation_token = 4;
	base.pcm_state = (uint8)PCM_STATE_N;
	live = base;
	live.reservation_token = 5;
	live.flags = PCM_OWN_FLAG_GRANT_PENDING;
	UT_ASSERT_EQ(cluster_pcm_x_grant_reservation_kind(&live, &base, 5),
				 CLUSTER_PCM_X_GRANT_RESERVATION_N_NEW);
	UT_ASSERT_EQ(cluster_pcm_x_grant_reservation_kind(&live, &base, 4),
				 CLUSTER_PCM_X_GRANT_RESERVATION_INVALID);

	for (i = 0; i < lengthof(states); i++) {
		reset_fixture();
		pg_atomic_write_u64(&ClusterPcmOwnArray[0].generation, 7);
		UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 7, PCM_OWN_FLAG_REVOKING, &token),
					 CLUSTER_PCM_OWN_OK);

		memset(&base, 0, sizeof(base));
		base.generation = 7;
		base.reservation_token = token;
		base.flags = PCM_OWN_FLAG_REVOKING;
		base.pcm_state = states[i];
		live = base;
		live.flags = PCM_OWN_FLAG_GRANT_PENDING;

		UT_ASSERT_EQ(cluster_pcm_x_grant_reservation_kind(&live, &base, token), expected_kinds[i]);
		UT_ASSERT_EQ(cluster_pcm_own_revoke_to_grant_handoff_exact(0, 7, token),
					 CLUSTER_PCM_OWN_OK);
		UT_ASSERT_EQ(cluster_pcm_own_grant_commit_exact(0, 7, token, &committed),
					 CLUSTER_PCM_OWN_OK);
		UT_ASSERT_EQ(committed, 8);
		assert_entry(8, token, 0);
	}
}

UT_TEST(test_revoke_commit_is_exact_and_classifies_live_races)
{
	uint64 committed = UINT64_MAX;
	uint64 token;

	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].generation, 7);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 7, PCM_OWN_FLAG_REVOKING, &token),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(token, 1);
	assert_entry(7, token, PCM_OWN_FLAG_REVOKING);

	/* A delayed/wrong lifecycle must not clear the current revoke. */
	UT_ASSERT_EQ(cluster_pcm_own_revoke_commit_exact(0, 7, token + 1, &committed),
				 CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(committed, 0);
	assert_entry(7, token, PCM_OWN_FLAG_REVOKING);

	UT_ASSERT_EQ(cluster_pcm_own_revoke_commit_exact(0, 7, token, &committed), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(committed, 8);
	assert_entry(8, token, 0);

	/* A duplicate cannot bump the ownership generation twice. */
	committed = UINT64_MAX;
	UT_ASSERT_EQ(cluster_pcm_own_revoke_commit_exact(0, 7, token, &committed),
				 CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(committed, 0);
	assert_entry(8, token, 0);

	/* A different well-formed lifecycle is contention, not corruption. */
	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, 9);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags, PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(cluster_pcm_own_revoke_commit_exact(0, 0, 9, &committed), CLUSTER_PCM_OWN_BUSY);
	assert_entry(0, 9, PCM_OWN_FLAG_GRANT_PENDING);

	/* Malformed live metadata is corruption, not ordinary contention. */
	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, 9);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags,
						PCM_OWN_FLAG_GRANT_PENDING | PCM_OWN_FLAG_REVOKING);
	UT_ASSERT_EQ(cluster_pcm_own_revoke_commit_exact(0, 0, 9, &committed), CLUSTER_PCM_OWN_CORRUPT);
	assert_entry(0, 9, PCM_OWN_FLAG_GRANT_PENDING | PCM_OWN_FLAG_REVOKING);
}

UT_TEST(test_revoke_retain_commit_keeps_exact_token_until_release)
{
	ClusterPcmOwnEvictionCapture capture;
	uint64 committed = UINT64_MAX;
	uint64 token;

	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].generation, 7);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 7, PCM_OWN_FLAG_REVOKING, &token),
				 CLUSTER_PCM_OWN_OK);

	/* The retained commit bumps ownership exactly once but deliberately keeps
	 * the same live token: descriptor reuse remains fail-closed until the
	 * matching DRAIN/RELEASE_IMAGE arrives. */
	UT_ASSERT_EQ(cluster_pcm_own_revoke_retain_commit_exact(0, 7, token + 1, &committed),
				 CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(committed, 0);
	assert_entry(7, token, PCM_OWN_FLAG_REVOKING);

	UT_ASSERT_EQ(cluster_pcm_own_revoke_retain_commit_exact(0, 7, token, &committed),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(committed, 8);
	assert_entry(8, token, PCM_OWN_FLAG_REVOKING);

	memset(&capture, 0, sizeof(capture));
	capture.generation = committed;
	capture.reservation_token = token;
	capture.flags = PCM_OWN_FLAG_REVOKING;
	UT_ASSERT(!cluster_pcm_own_eviction_reuse_allowed(&capture));

	/* A stale DRAIN from either the pre-commit generation or a prior token is
	 * a strict no-op and cannot unpin a newer retained round. */
	UT_ASSERT_EQ(cluster_pcm_own_revoke_retain_release_exact(0, 7, token), CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(cluster_pcm_own_revoke_retain_release_exact(0, 8, token + 1),
				 CLUSTER_PCM_OWN_STALE);
	assert_entry(8, token, PCM_OWN_FLAG_REVOKING);

	UT_ASSERT_EQ(cluster_pcm_own_revoke_retain_release_exact(0, 8, token), CLUSTER_PCM_OWN_OK);
	assert_entry(8, token, 0);
	UT_ASSERT_EQ(cluster_pcm_own_revoke_retain_release_exact(0, 8, token), CLUSTER_PCM_OWN_STALE);
}

UT_TEST(test_revoke_commit_exhaustion_is_side_effect_free)
{
	uint64 committed = UINT64_MAX;
	uint64 token;

	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].generation, UINT64_MAX);
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, 17);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags, PCM_OWN_FLAG_REVOKING);
	token = 17;

	UT_ASSERT_EQ(cluster_pcm_own_revoke_commit_exact(0, UINT64_MAX, token, &committed),
				 CLUSTER_PCM_OWN_EXHAUSTED);
	UT_ASSERT_EQ(committed, 0);
	assert_entry(UINT64_MAX, token, PCM_OWN_FLAG_REVOKING);
}

UT_TEST(test_token_and_generation_never_wrap)
{
	uint64 token = UINT64_MAX;
	uint64 last_token;
	uint64 generation = 0;

	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, UINT64_MAX);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_EXHAUSTED);
	UT_ASSERT_EQ(token, 0);
	assert_entry(0, UINT64_MAX, 0);

	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].generation, UINT64_MAX - 1);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, UINT64_MAX - 1,
														 PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_OK);
	last_token = token;
	UT_ASSERT_EQ(cluster_pcm_own_grant_commit_exact(0, UINT64_MAX - 1, token, &generation),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(generation, UINT64_MAX);
	assert_entry(UINT64_MAX, token, 0);

	/* MAX is terminal: begin must fail before token/flag side effects. */
	token = UINT64_MAX;
	UT_ASSERT_EQ(
		cluster_pcm_own_reservation_begin_exact(0, UINT64_MAX, PCM_OWN_FLAG_GRANT_PENDING, &token),
		CLUSTER_PCM_OWN_EXHAUSTED);
	UT_ASSERT_EQ(token, 0);
	assert_entry(UINT64_MAX, last_token, 0);
	UT_ASSERT(!cluster_pcm_own_gen_bump_checked(0, &generation));
	UT_ASSERT_EQ(generation, UINT64_MAX);
	assert_entry(UINT64_MAX, last_token, 0);
}

UT_TEST(test_ordinary_generation_bump_rejects_live_reservation)
{
	uint64 generation = UINT64_MAX;
	uint64 token;

	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].generation, 7);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 7, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_OK);

	/* Only the token-exact finish/revoke lifecycle may advance generation
	 * while a transient ownership flag is live.  The ordinary transition
	 * helper must be a no-op so it cannot bypass the reservation token. */
	UT_ASSERT(!cluster_pcm_own_gen_bump_checked(0, &generation));
	UT_ASSERT_EQ(generation, 7);
	assert_entry(7, token, PCM_OWN_FLAG_GRANT_PENDING);

	/* The same helper remains valid after exact cleanup makes the entry idle. */
	reset_fixture();
	UT_ASSERT(cluster_pcm_own_gen_bump_checked(0, &generation));
	UT_ASSERT_EQ(generation, 1);
	assert_entry(1, 0, 0);
}

UT_TEST(test_eviction_rejects_live_reservation_and_exhaustion)
{
	ClusterPcmOwnEvictionCapture capture;

	memset(&capture, 0, sizeof(capture));
	capture.generation = 9;
	UT_ASSERT(cluster_pcm_own_eviction_reuse_allowed(&capture));

	capture.flags = PCM_OWN_FLAG_GRANT_PENDING;
	UT_ASSERT(!cluster_pcm_own_eviction_reuse_allowed(&capture));
	capture.flags = PCM_OWN_FLAG_REVOKING;
	UT_ASSERT(!cluster_pcm_own_eviction_reuse_allowed(&capture));
	capture.flags = 0;
	capture.reservation_token = 3;
	/* The single token is monotonic and remains nonzero while idle; flags are
	 * the only active-lifecycle marker. */
	UT_ASSERT(cluster_pcm_own_eviction_reuse_allowed(&capture));
	capture.generation = UINT64_MAX;
	UT_ASSERT(!cluster_pcm_own_eviction_reuse_allowed(&capture));
}

static char *
read_bufmgr_source(void)
{
	FILE *file;
	long length;
	char *source;

	file = fopen(BUFMGR_SOURCE_PATH, "rb");
	UT_ASSERT_NOT_NULL(file);
	UT_ASSERT_EQ(fseek(file, 0, SEEK_END), 0);
	length = ftell(file);
	UT_ASSERT(length > 0);
	UT_ASSERT_EQ(fseek(file, 0, SEEK_SET), 0);
	source = malloc((size_t)length + 1);
	UT_ASSERT_NOT_NULL(source);
	UT_ASSERT_EQ(fread(source, 1, (size_t)length, file), (size_t)length);
	source[length] = '\0';
	fclose(file);
	return source;
}

static void
assert_commit_before_tail(const char *source, const char *function_start, const char *function_end)
{
	const char *begin = strstr(source, function_start);
	const char *end;
	const char *commit;
	const char *tail;

	UT_ASSERT_NOT_NULL(begin);
	if (begin == NULL)
		return;
	end = strstr(begin + strlen(function_start), function_end);
	UT_ASSERT_NOT_NULL(end);
	if (end == NULL)
		return;
	commit = strstr(begin, "cluster_pcm_own_eviction_commit_locked");
	tail = strstr(begin, "InvalidateBufferCommitTailLocked");
	UT_ASSERT_NOT_NULL(commit);
	UT_ASSERT_NOT_NULL(tail);
	if (commit == NULL || tail == NULL)
		return;
	UT_ASSERT(commit < tail);
	UT_ASSERT(commit < end);
	UT_ASSERT(tail < end);
}

static int
count_occurrences(const char *source, const char *needle)
{
	int count = 0;
	size_t needle_length = strlen(needle);

	while ((source = strstr(source, needle)) != NULL) {
		count++;
		source += needle_length;
	}
	return count;
}

static void
assert_ordered_in_function(const char *source, const char *function_start, const char *function_end,
						   const char *const *needles, int needle_count)
{
	const char *cursor = strstr(source, function_start);
	const char *end;
	int i;

	UT_ASSERT_NOT_NULL(cursor);
	if (cursor == NULL)
		return;
	end = strstr(cursor + strlen(function_start), function_end);
	UT_ASSERT_NOT_NULL(end);
	if (end == NULL)
		return;

	for (i = 0; i < needle_count; i++) {
		cursor = strstr(cursor, needles[i]);
		UT_ASSERT_NOT_NULL(cursor);
		if (cursor == NULL)
			return;
		UT_ASSERT(cursor < end);
		if (cursor >= end)
			return;
		cursor += strlen(needles[i]);
	}
}

static void
assert_source_range_contains(const char *start, const char *end, const char *needle)
{
	const char *found;

	UT_ASSERT_NOT_NULL(start);
	UT_ASSERT_NOT_NULL(end);
	if (start == NULL || end == NULL)
		return;
	found = strstr(start, needle);
	UT_ASSERT_NOT_NULL(found);
	if (found != NULL)
		UT_ASSERT(found < end);
}

UT_TEST(test_bufmgr_d5a_commitlocked_uses_locked_commit_and_saved_tag_release)
{
	static const char *const commit_contract[]
		= { "ClusterPcmOwnEvictionCapture eviction_capture",
			"cluster_pcm_own_eviction_capture_locked", "cluster_pcm_own_eviction_commit_locked",
			"eviction_result != CLUSTER_PCM_OWN_OK", "InvalidateBufferCommitTailLocked" };
	static const char *const tail_contract[]
		= { "ClearBufferTag", "UnlockBufHdr", "cluster_pcm_lock_release_saved_tag_for_eviction" };
	char *source = read_bufmgr_source();

	/* Descriptor reuse is a single header-authority commit.  A live token,
	 * exhausted generation, or tuple mismatch must leave the old tag resident
	 * and return fail-closed; only an exact successful commit may clear the tag
	 * and later release the master holder by the saved immutable tag. */
	assert_commit_before_tail(source, "\nInvalidateBufferCommitLocked(",
							  "\n/*\n * InvalidateBufferCommitTailLocked");
	assert_ordered_in_function(source, "\nInvalidateBufferCommitLocked(",
							   "\n/*\n * InvalidateBufferCommitTailLocked", commit_contract,
							   lengthof(commit_contract));
	assert_ordered_in_function(source, "\nInvalidateBufferCommitTailLocked(",
							   "\n/*\n * InvalidateBufferTry", tail_contract,
							   lengthof(tail_contract));
	UT_ASSERT_NOT_NULL(strstr(source, "static bool\nInvalidateBufferCommitLocked"));
	UT_ASSERT_NOT_NULL(strstr(source, "cluster_pcm_lock_release_saved_tag_for_eviction"));
	UT_ASSERT_NULL(strstr(source, "buf->tag = *oldTag"));
	free(source);
}

UT_TEST(test_bufmgr_abort_cleanup_is_never_silent)
{
	static const char *const normal_cleanup[]
		= { "cluster_pcm_own_abort_grant_reservation", "CLUSTER_PCM_OWN_OK", "ereport(ERROR" };
	static const char *const error_cleanup[]
		= { "cluster_pcm_own_abort_grant_reservation", "CLUSTER_PCM_OWN_OK", "elog(LOG" };
	char *source = read_bufmgr_source();

	/* Every normal false/READ_IMAGE exit must prove exact cleanup or ERROR.
	 * During PG_CATCH, preserve the original error but emit LOG evidence when
	 * exact cleanup did not converge.  No call may discard the result. */
	UT_ASSERT_NULL(strstr(source, "(void) cluster_pcm_own_abort_grant_reservation"));
	assert_ordered_in_function(source, "\ncluster_pcm_own_abort_grant_or_error(",
							   "\nstatic void\ncluster_pcm_own_abort_grant_after_error(",
							   normal_cleanup, lengthof(normal_cleanup));
	assert_ordered_in_function(
		source, "\ncluster_pcm_own_abort_grant_after_error(",
		"\nstatic ClusterPcmOwnResult\ncluster_pcm_own_abort_grant_after_master_rollback(",
		error_cleanup, lengthof(error_cleanup));
	free(source);
}

UT_TEST(test_bufmgr_finish_failure_rolls_back_acquired_master_grant)
{
	static const char *const rollback_contract[]
		= { "cluster_pcm_own_finish_grant_reservation",
			"PG_TRY",
			"cluster_pcm_lock_release_buffer_for_eviction",
			"PG_CATCH",
			"elog(LOG",
			"PG_RE_THROW",
			"cluster_pcm_own_abort_grant_after_master_rollback",
			"ereport(ERROR" };
	char *source = read_bufmgr_source();

	/* Definition + direct-lock caller + LockBuffer caller.  Keeping the real
	 * acquire and exact finish behind one helper prevents either entrance from
	 * leaking a master holder when local finish rejects the token/tuple. */
	UT_ASSERT(count_occurrences(source, "cluster_pcm_own_finish_grant_or_rollback(") >= 3);
	assert_ordered_in_function(source, "\ncluster_pcm_own_finish_grant_or_rollback(", "\nstatic ",
							   rollback_contract, lengthof(rollback_contract));
	free(source);
}

UT_TEST(test_bufmgr_s_base_rollback_normalizes_to_n_under_header_authority)
{
	static const char *const s_to_n_contract[] = { "LockBufHdr",
												   "base->pcm_state != (uint8)PCM_STATE_N",
												   "base->pcm_state != (uint8)PCM_STATE_S",
												   "base->pcm_state == (uint8)PCM_STATE_S",
												   "base->generation == UINT64_MAX",
												   "cluster_pcm_own_reservation_abort_exact",
												   "base->pcm_state == (uint8)PCM_STATE_S",
												   "cluster_pcm_own_gen_bump_checked",
												   "buf->pcm_state = (uint8)PCM_STATE_N",
												   "UnlockBufHdr" };
	char *source = read_bufmgr_source();

	/* A legacy S-base acquire that reached master X cannot restore S after a
	 * failed local finish: the master rollback is X->N.  The local half must
	 * therefore exact-abort the live token and commit S->N plus one checked
	 * generation bump under a single header-lock hold.  N-base skips the bump
	 * and remains N.  All prechecks precede abort, so rejection leaves the live
	 * flag as fail-closed evidence rather than advertising successful cleanup. */
	assert_ordered_in_function(source, "\ncluster_pcm_own_abort_grant_after_master_rollback(",
							   "\nstatic void\ncluster_pcm_own_finish_grant_or_rollback(",
							   s_to_n_contract, lengthof(s_to_n_contract));
	free(source);
}

UT_TEST(test_lockbuffer_content_error_uses_post_master_rollback_contract)
{
	static const char *const rethrow_contract[]
		= { "Assert(original_error != NULL)",
			"PG_TRY",
			"cluster_pcm_lock_release_buffer_for_eviction",
			"PG_CATCH",
			"CopyErrorData",
			"FlushErrorState",
			"elog(LOG",
			"if (master_released)",
			"cluster_pcm_own_abort_grant_after_master_rollback",
			"ReThrowError" };
	static const char *const content_error_contract[]
		= { "MemoryContextSwitchTo(pcm_error_context)",
			"original_error = CopyErrorData()",
			"FlushErrorState()",
			"cluster_bufmgr_pcm_x_holder_abort_acquiring",
			"if (pcm_acquired)",
			"cluster_pcm_own_rollback_grant_after_error_and_rethrow",
			"else if (pcm_pending_set)",
			"cluster_pcm_own_abort_grant_after_error",
			"ReThrowError(original_error)" };
	char *source = read_bufmgr_source();

	/* A content-lock error can occur after a durable master grant but before
	 * local finish.  Both holder detach and remote release may themselves
	 * throw, so LockBuffer must copy and flush the original ErrorData before
	 * either cleanup begins.  Cleanup failure is LOGged, leaves exact evidence
	 * fail-closed, and still rethrows the original error.  Exact local
	 * convergence is legal only after release succeeded. */
	assert_ordered_in_function(source, "\ncluster_pcm_own_rollback_grant_after_error_and_rethrow(",
							   "\nstatic ", rethrow_contract, lengthof(rethrow_contract));
	assert_ordered_in_function(
		source, "\nLockBuffer(Buffer buffer, int mode)",
		"\n/*\n * Acquire the content_lock for the buffer, but only if we don't have to wait.",
		content_error_contract, lengthof(content_error_contract));
	free(source);
}

UT_TEST(test_bufmgr_generation_bump_failure_is_classified_under_header_lock)
{
	static const char *const diagnostic_contract[] = { "cluster_pcm_own_reservation_token_get",
													   "cluster_pcm_own_flags_get",
													   "cluster_pcm_own_classify_live_flags",
													   "live_result != CLUSTER_PCM_OWN_OK",
													   "generation == UINT64_MAX",
													   "CLUSTER_PCM_OWN_EXHAUSTED" };
	static const char *const transition_contract[]
		= { "LockBufHdr", "cluster_pcm_own_bump_locked", "UnlockBufHdr",
			"cluster_pcm_own_report_bump_failure" };
	char *source = read_bufmgr_source();

	/* A checked bump can reject either a live exact lifecycle or terminal MAX.
	 * Both observations must be made while header authority is still held and
	 * must not be collapsed into the misleading "exhausted" diagnosis. */
	assert_ordered_in_function(source, "\ncluster_pcm_own_bump_failure(", "\nstatic ",
							   diagnostic_contract, lengthof(diagnostic_contract));
	assert_ordered_in_function(source, "\ncluster_pcm_own_transition(", "\n/*", transition_contract,
							   lengthof(transition_contract));
	UT_ASSERT(count_occurrences(source, "cluster_pcm_own_bump_failure(") >= 2);
	UT_ASSERT_NOT_NULL(strstr(source, "active reservation"));
	UT_ASSERT_NOT_NULL(strstr(source, "generation exhausted"));
	free(source);
}

UT_TEST(test_lockbuffer_reservation_failures_use_busy_corrupt_classifier)
{
	static const char *const initial_reservation_contract[]
		= { "cluster_pcm_own_begin_grant_reservation", "pcm_pending_result != CLUSTER_PCM_OWN_OK",
			"cluster_pcm_own_report_bump_failure", "pcm_pending_set = true" };
	static const char *const revalidate_reservation_contract[]
		= { "cluster_pcm_own_begin_grant_reservation", "pcm_pending_result != CLUSTER_PCM_OWN_OK",
			"cluster_pcm_own_report_bump_failure", "pcm_pending_set = true" };
	char *source = read_bufmgr_source();

	/* A live queue reservation is BUSY, not damaged metadata.  Both legacy
	 * LockBuffer begin sites must preserve that distinction while malformed
	 * flag/token shapes continue through the same helper as DATA_CORRUPTED. */
	assert_ordered_in_function(source, "Legacy acquire path:", "PG_END_TRY();",
							   initial_reservation_contract,
							   lengthof(initial_reservation_contract));
	assert_ordered_in_function(source, "cluster_pcm_note_writer_cover_stale_detected();",
							   "cluster_pcm_note_writer_reverify_reacquire();",
							   revalidate_reservation_contract,
							   lengthof(revalidate_reservation_contract));
	free(source);
}

UT_TEST(test_bufmgr_finish_rejects_invalid_state_and_initializes_acquire_result)
{
	static const char *const finish_gate[]
		= { "new_pcm_state != (uint8)PCM_STATE_S", "new_pcm_state != (uint8)PCM_STATE_X",
			"return CLUSTER_PCM_OWN_INVALID", "LockBufHdr" };
	char *source = read_bufmgr_source();

	/* The only durable grant mirrors are S and X.  Validate that before any
	 * header/sidecar mutation, and never let PG_TRY leave an indeterminate
	 * acquire result for its catch/finalize paths. */
	assert_ordered_in_function(source, "\ncluster_pcm_own_finish_grant_reservation(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_finish_x_commit(",
							   finish_gate, lengthof(finish_gate));
	UT_ASSERT_NOT_NULL(strstr(source, "grant_acquired = false"));
	free(source);
}

UT_TEST(test_bufmgr_finish_and_abort_gate_on_exact_base_state)
{
	static const char *const finish_contract[]
		= { "LockBufHdr", "cluster_pcm_own_snapshot_locked", "cluster_pcm_x_grant_reservation_kind",
			"cluster_pcm_own_grant_commit_exact" };
	static const char *const abort_contract[]
		= { "LockBufHdr", "BufferTagsEqual", "buf->pcm_state != base->pcm_state",
			"cluster_pcm_own_reservation_abort_exact" };
	char *source = read_bufmgr_source();

	/* Tag/gen/token/flag identity is insufficient: a concurrent ownership
	 * transition that changed only the descriptor mirror must make both exact
	 * finish and abort return STALE before touching generation or flags. */
	assert_ordered_in_function(source, "\ncluster_pcm_own_finish_grant_reservation(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_finish_x_commit(",
							   finish_contract, lengthof(finish_contract));
	assert_ordered_in_function(source, "\ncluster_pcm_own_abort_grant_reservation(",
							   "\nstatic void\ncluster_pcm_own_abort_grant_or_error(",
							   abort_contract, lengthof(abort_contract));
	free(source);
}

UT_TEST(test_d5a_release_error_keeps_descriptor_out_of_freelist)
{
	static const char *const fail_closed_contract[]
		= { "BufTableDelete", "LWLockRelease(oldPartitionLock)",
			"PG_TRY",		  "cluster_pcm_lock_release_saved_tag_for_eviction",
			"PG_CATCH",		  "elog(LOG",
			"PG_RE_THROW",	  "StrategyFreeBuffer" };
	char *source = read_bufmgr_source();

	/* A remote release may throw only after the old mapping is gone.  Emit
	 * module evidence and rethrow before StrategyFreeBuffer, leaving the
	 * descriptor unmapped and non-reusable rather than losing a master holder
	 * through descriptor reuse. */
	assert_ordered_in_function(source, "\nInvalidateBufferCommitLocked(",
							   "\n/*\n * InvalidateBufferTry", fail_closed_contract,
							   lengthof(fail_closed_contract));
	free(source);
}

UT_TEST(test_queue_begin_requires_normalized_n_snapshot)
{
	static const char *const normalized_n_gate[]
		= { "expected->pcm_state != (uint8)PCM_STATE_N", "return CLUSTER_PCM_OWN_STALE" };
	char *source = read_bufmgr_source();

	/* Ordinary queued acquisition must use a fresh normalized N snapshot.
	 * Sole-requester S conversion has a separate exact handoff API that reuses
	 * REVOKING and therefore still cannot enter this new-token path. */
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_begin_x_reservation(",
		"\nstatic ClusterPcmOwnResult\ncluster_pcm_own_finish_grant_reservation(",
		normalized_n_gate, lengthof(normalized_n_gate));
	free(source);
}

UT_TEST(test_queue_contract_exposes_prepare_only_begin_api)
{
	typedef ClusterPcmOwnResult (*BeginFn)(BufferDesc *, const ClusterPcmOwnSnapshot *, uint64 *);
	typedef ClusterPcmOwnResult (*HandoffFn)(BufferDesc *, const ClusterPcmOwnSnapshot *, uint64 *);
	typedef ClusterPcmOwnResult (*ReleaseSFn)(BufferDesc *, const ClusterPcmOwnSnapshot *,
											  ClusterPcmOwnSnapshot *);
	typedef ClusterPcmOwnResult (*FinishFn)(BufferDesc *, const ClusterPcmOwnSnapshot *, uint64,
											uint64 *);
	typedef ClusterPcmOwnResult (*AbortFn)(BufferDesc *, const ClusterPcmOwnSnapshot *, uint64);

	/* The queue owns timing, but not the reservation lifecycle: JOIN/WAIT must
	 * never call this begin API.  ACTIVE_TRANSFER/PREPARE stores the returned
	 * token and all later finish/abort operations are exact. */
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_begin_x_reservation),
										   BeginFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_handoff_s_revoke_to_x_reservation), HandoffFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_handoff_revoke_to_x_reservation), HandoffFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_finish_s_release_to_n), ReleaseSFn));
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_finish_x_commit),
										   FinishFn));
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_abort_x_reservation),
										   AbortFn));
}

UT_TEST(test_queue_contract_exposes_opaque_retained_revoke_api)
{
	typedef ClusterPcmOwnResult (*BeginRevokeFn)(BufferDesc *, const ClusterPcmOwnSnapshot *,
												 ClusterPcmOwnSnapshot *);
	typedef ClusterPcmOwnResult (*PrepareNSourceFn)(BufferDesc *, const ClusterPcmOwnSnapshot *,
													ClusterPcmOwnSnapshot *, char *, XLogRecPtr *,
													uint64 *);
	typedef ClusterPcmOwnResult (*AbortRevokeFn)(BufferDesc *, const ClusterPcmOwnSnapshot *);
	typedef ClusterPcmOwnResult (*FinishRetainFn)(BufferDesc *, const ClusterPcmOwnSnapshot *,
												  XLogRecPtr, ClusterPcmOwnSnapshot *);
	typedef ClusterPcmOwnResult (*ReleaseRetainedFn)(const BufferTag *, uint64);
	typedef bool (*ContentWriteFn)(BufferDesc *);

	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_begin_x_revoke),
										   BeginRevokeFn));
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_abort_x_revoke),
										   AbortRevokeFn));
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_begin_s_revoke),
										   BeginRevokeFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_prepare_n_source_image), PrepareNSourceFn));
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_abort_n_revoke),
										   AbortRevokeFn));
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_abort_s_revoke),
										   AbortRevokeFn));
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_finish_revoke_retain),
										   FinishRetainFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_release_retained_image), ReleaseRetainedFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_x_content_write_permitted), ContentWriteFn));
}

UT_TEST(test_queue_n_source_refresh_is_exact_and_publishes_only_complete_image)
{
	static const char *const prepare_contract[]
		= { "PGIOAlignedBlock scratch",
			"expected_n->pcm_state != (uint8)PCM_STATE_N",
			"ReservePrivateRefCountEntry",
			"ResourceOwnerEnlargeBuffers(CurrentResourceOwner)",
			"cluster_pcm_own_snapshot_matches_locked",
			"BM_VALID",
			"BM_IO_ERROR",
			"BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED",
			"PinBuffer_Locked",
			"FlushBuffer",
			"UnpinBuffer",
			"BM_IO_IN_PROGRESS",
			"cluster_pcm_own_reservation_begin_exact",
			"PCM_OWN_FLAG_REVOKING",
			"smgrread",
			"PageIsVerifiedExtended",
			"LWLockAcquire(content_lock, LW_EXCLUSIVE)",
			"cluster_pcm_own_snapshot_matches_locked",
			"PCM_OWN_FLAG_REVOKING",
			"BM_VALID",
			"BM_IO_ERROR",
			"BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED",
			"BM_IO_IN_PROGRESS",
			"memcpy((char *)BufHdrGetBlock(buf), scratch.data, BLCKSZ)",
			"buf->buffer_type = (uint8)BUF_TYPE_CURRENT",
			"memcpy(block_data, scratch.data, BLCKSZ)",
			"PageGetLSN((Page)scratch.data)",
			"pd_block_scn",
			"*out_revoking = live" };
	char *source = read_bufmgr_source();

	/* READY may be built only after one verified storage scratch has replaced
	 * the exact fenced N descriptor and supplied all image evidence.  A dirty
	 * pre-grant N page is legal (extension / redo dirt): the contract now pins
	 * the flush-then-BUSY convergence (reserve/pin under the header lock,
	 * WAL-first FlushBuffer, unpin) ahead of the reservation, with BM_IO_ERROR
	 * judged before the dirty branch on both passes. */
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_prepare_n_source_image(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_begin_s_revoke(",
							   prepare_contract, lengthof(prepare_contract));
	free(source);
}

UT_TEST(test_revoke_finish_mode_rejects_pinned_vm_fsm_and_retains_main)
{
	BufferTag tag;

	memset(&tag, 0, sizeof(tag));
	tag.forkNum = MAIN_FORKNUM;
	UT_ASSERT_EQ(cluster_pcm_x_revoke_finish_mode(&tag, 0), CLUSTER_PCM_X_REVOKE_FINISH_RETAIN);
	UT_ASSERT_EQ(cluster_pcm_x_revoke_finish_mode(&tag, 7), CLUSTER_PCM_X_REVOKE_FINISH_RETAIN);
	tag.forkNum = INIT_FORKNUM;
	UT_ASSERT_EQ(cluster_pcm_x_revoke_finish_mode(&tag, 3), CLUSTER_PCM_X_REVOKE_FINISH_RETAIN);
	tag.forkNum = FSM_FORKNUM;
	UT_ASSERT_EQ(cluster_pcm_x_revoke_finish_mode(&tag, 0), CLUSTER_PCM_X_REVOKE_FINISH_DROP);
	UT_ASSERT_EQ(cluster_pcm_x_revoke_finish_mode(&tag, 1), CLUSTER_PCM_X_REVOKE_FINISH_BUSY);
	tag.forkNum = VISIBILITYMAP_FORKNUM;
	UT_ASSERT_EQ(cluster_pcm_x_revoke_finish_mode(&tag, 0), CLUSTER_PCM_X_REVOKE_FINISH_DROP);
	UT_ASSERT_EQ(cluster_pcm_x_revoke_finish_mode(&tag, UINT32_MAX),
				 CLUSTER_PCM_X_REVOKE_FINISH_BUSY);
	tag.forkNum = InvalidForkNumber;
	UT_ASSERT_EQ(cluster_pcm_x_revoke_finish_mode(&tag, 0), CLUSTER_PCM_X_REVOKE_FINISH_INVALID);
	UT_ASSERT_EQ(cluster_pcm_x_revoke_finish_mode(NULL, 0), CLUSTER_PCM_X_REVOKE_FINISH_INVALID);
}

UT_TEST(test_queue_revoke_retains_main_but_drops_unpinned_vm_fsm)
{
	static const char *const begin_contract[] = { "expected_s->pcm_state != (uint8) PCM_STATE_S",
												  "LockBufHdr",
												  "cluster_pcm_own_gen_get",
												  "cluster_bufmgr_pcm_current_image_locked",
												  "cluster_pcm_own_classify_live_flags",
												  "cluster_pcm_own_reservation_begin_exact",
												  "PCM_OWN_FLAG_REVOKING",
												  "cluster_pcm_own_snapshot_locked",
												  "UnlockBufHdr" };
	static const char *const abort_contract[]
		= { "expected_revoking->pcm_state != (uint8) PCM_STATE_S",
			"LockBufHdr",
			"cluster_pcm_own_classify_live_flags",
			"cluster_pcm_own_reservation_abort_exact",
			"PCM_OWN_FLAG_REVOKING",
			"UnlockBufHdr" };
	static const char *const finish_contract[] = { "LWLockAcquire(content_lock, LW_EXCLUSIVE)",
												   "LockBufHdr",
												   "cluster_pcm_own_gen_get",
												   "cluster_bufmgr_pcm_current_image_locked",
												   "BM_IO_IN_PROGRESS",
												   "PageGetLSN",
												   "cluster_pcm_own_revoke_retain_commit_exact",
												   "buf->pcm_state = (uint8) PCM_STATE_N",
												   "buf->buffer_type = (uint8) BUF_TYPE_PI",
												   "BM_DIRTY | BM_JUST_DIRTIED",
												   "BM_CHECKPOINT_NEEDED | BM_IO_ERROR",
												   "cluster_pcm_own_snapshot_locked" };
	static const char *const drop_contract[] = { "BufMappingPartitionLock",
												 "LWLockAcquire(partition_lock, LW_EXCLUSIVE)",
												 "BufTableLookup",
												 "LockBufHdr",
												 "BUF_STATE_GET_REFCOUNT",
												 "CLUSTER_PCM_X_REVOKE_FINISH_BUSY",
												 "PageGetLSN",
												 "cluster_pcm_own_revoke_commit_exact",
												 "buf->pcm_state = (uint8)PCM_STATE_N",
												 "cluster_pcm_own_snapshot_locked",
												 "InvalidateBufferCommitTailLocked" };
	char *source = read_bufmgr_source();
	const char *begin_s;
	const char *abort_s;
	const char *begin_x;
	const char *abort_x;
	const char *drop_helper;
	const char *finish;
	const char *finish_end;

	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_begin_s_revoke(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_abort_s_revoke(",
							   begin_contract, lengthof(begin_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_abort_s_revoke(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_begin_x_revoke(",
							   abort_contract, lengthof(abort_contract));
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_finish_revoke_retain(",
		"\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_release_retained_image(", finish_contract,
		lengthof(finish_contract));
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_finish_revoke_drop_unpinned(",
		"\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_finish_revoke_retain(", drop_contract,
		lengthof(drop_contract));

	/* Main/init passive PG pins are not PCM holders, so their retained commit
	 * must not recreate the S3 pin ring.  VM/FSM take the separate exact-drop
	 * arm above, where a foreign pin returns BUSY before any ownership commit. */
	begin_s = strstr(source, "\ncluster_bufmgr_pcm_own_begin_s_revoke(");
	abort_s = strstr(source, "\ncluster_bufmgr_pcm_own_abort_s_revoke(");
	begin_x = strstr(source, "\ncluster_bufmgr_pcm_own_begin_x_revoke(");
	abort_x = strstr(source, "\ncluster_bufmgr_pcm_own_abort_x_revoke(");
	drop_helper = strstr(source, "\ncluster_bufmgr_pcm_own_finish_revoke_drop_unpinned(");
	finish = strstr(source, "\ncluster_bufmgr_pcm_own_finish_revoke_retain(");
	finish_end = strstr(source, "\ncluster_bufmgr_pcm_own_release_retained_image(");
	UT_ASSERT_NOT_NULL(begin_s);
	UT_ASSERT_NOT_NULL(abort_s);
	UT_ASSERT_NOT_NULL(begin_x);
	UT_ASSERT_NOT_NULL(abort_x);
	UT_ASSERT_NOT_NULL(drop_helper);
	UT_ASSERT_NOT_NULL(finish);
	UT_ASSERT_NOT_NULL(finish_end);
	/* buffer_type is a monotone hint: every exact source lifecycle must
	 * accept a yielded S+XCUR through the centralized current-image gate. */
	assert_source_range_contains(begin_s, abort_s, "cluster_bufmgr_pcm_current_image_locked");
	assert_source_range_contains(abort_s, begin_x, "cluster_bufmgr_pcm_current_image_locked");
	assert_source_range_contains(begin_x, abort_x, "cluster_bufmgr_pcm_current_image_locked");
	assert_source_range_contains(abort_x, drop_helper, "cluster_bufmgr_pcm_current_image_locked");
	assert_source_range_contains(drop_helper, finish, "cluster_bufmgr_pcm_current_image_locked");
	assert_source_range_contains(finish, finish_end, "cluster_bufmgr_pcm_current_image_locked");
	if (begin_s != NULL && begin_x != NULL)
		UT_ASSERT(strstr(begin_s, "BUF_STATE_GET_REFCOUNT") == NULL
				  || strstr(begin_s, "BUF_STATE_GET_REFCOUNT") >= begin_x);
	if (begin_x != NULL && drop_helper != NULL)
		UT_ASSERT(strstr(begin_x, "BUF_STATE_GET_REFCOUNT") == NULL
				  || strstr(begin_x, "BUF_STATE_GET_REFCOUNT") >= drop_helper);
	if (finish != NULL && finish_end != NULL) {
		const char *refcount = strstr(finish, "BUF_STATE_GET_REFCOUNT");
		const char *drop = strstr(finish, "InvalidateBuffer");
		const char *legacy_pi = strstr(finish, "cluster_bufmgr_convert_to_pi_locked");
		const char *mapping = strstr(finish, "partition_lock");

		UT_ASSERT(refcount == NULL || refcount >= finish_end);
		UT_ASSERT(drop == NULL || drop >= finish_end);
		UT_ASSERT(legacy_pi == NULL || legacy_pi >= finish_end);
		UT_ASSERT(mapping == NULL || mapping >= finish_end);
	}
	free(source);
}

UT_TEST(test_retained_image_release_and_writeback_gates_are_exact)
{
	static const char *const content_write_contract[]
		= { "LockBufHdr",
			"cluster_pcm_own_flags_get",
			"PCM_OWN_FLAG_REVOKING",
			"cluster_bufmgr_pcm_x_retained_image_locked",
			"PCM_OWN_FLAG_GRANT_PENDING",
			"UnlockBufHdr" };
	static const char *const release_contract[]
		= { "source_generation + 1",
			"BufTableHashCode",
			"LWLockAcquire(partition_lock, LW_SHARED)",
			"BufTableLookup",
			"LockBufHdr",
			"PCM_OWN_FLAG_REVOKING",
			"LWLockRelease(partition_lock)",
			"LWLockAcquire(content_lock, LW_EXCLUSIVE)",
			"LockBufHdr",
			"BufferTagsEqual",
			"BM_VALID",
			"BUF_TYPE_PI",
			"PCM_STATE_N",
			"BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED | BM_IO_ERROR",
			"PCM_OWN_FLAG_REVOKING",
			"cluster_pcm_own_revoke_retain_release_exact" };
	char *source = read_bufmgr_source();
	const char *victim;
	const char *sync;
	const char *flush;
	const char *dirty;
	const char *hint;
	const char *lockbuffer;
	const char *conditional;
	const char *resident_stamp;
	const char *storage_refresh;

	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_release_retained_image(",
		"\n/* ========================================================================\n * PGRAC "
		"MODIFICATIONS by SqlRush — spec-6.12h D-h2",
		release_contract, lengthof(release_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_content_write_permitted(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_snapshot_by_tag(",
							   content_write_contract, lengthof(content_write_contract));

	/* The content-lock ordering is the race proof: an already-started flush
	 * owns SHARE and finishes before retain; after retain owns EXCLUSIVE, all
	 * later Sync/Flush/dirty paths see the immutable retained shape. */
	victim = strstr(source, "\nInvalidateVictimBuffer(");
	sync = strstr(source, "\nSyncOneBuffer(");
	flush = strstr(source, "\nFlushBuffer(");
	dirty = strstr(source, "\nMarkBufferDirty(Buffer buffer)");
	hint = strstr(source, "\nMarkBufferDirtyHint(Buffer buffer, bool buffer_std)");
	lockbuffer = strstr(source, "\nLockBuffer(Buffer buffer, int mode)");
	conditional = strstr(source, "\nConditionalLockBuffer(Buffer buffer)");
	resident_stamp = strstr(source, "\ncluster_bufmgr_lock_resident_for_stamp(");
	storage_refresh = strstr(source, "\ncluster_bufmgr_refresh_block_from_storage_for_gcs(");
	UT_ASSERT_NOT_NULL(victim);
	UT_ASSERT_NOT_NULL(sync);
	UT_ASSERT_NOT_NULL(flush);
	UT_ASSERT_NOT_NULL(dirty);
	UT_ASSERT_NOT_NULL(hint);
	UT_ASSERT_NOT_NULL(lockbuffer);
	UT_ASSERT_NOT_NULL(conditional);
	UT_ASSERT_NOT_NULL(resident_stamp);
	UT_ASSERT_NOT_NULL(storage_refresh);
	if (victim != NULL)
		UT_ASSERT(strstr(victim, "cluster_bufmgr_pcm_x_retained_image_reuse_blocked_locked")
				  < strstr(victim, "ClearBufferTag"));
	if (sync != NULL) {
		const char *first_gate
			= strstr(sync, "cluster_bufmgr_pcm_x_retained_image_reuse_blocked_locked");
		const char *content_share
			= strstr(sync, "LWLockAcquire(BufferDescriptorGetContentLock(bufHdr), LW_SHARED)");
		const char *second_gate
			= first_gate != NULL
				  ? strstr(first_gate + 1,
						   "cluster_bufmgr_pcm_x_retained_image_reuse_blocked_locked")
				  : NULL;

		UT_ASSERT_NOT_NULL(first_gate);
		UT_ASSERT_NOT_NULL(content_share);
		UT_ASSERT_NOT_NULL(second_gate);
		if (first_gate != NULL)
			UT_ASSERT(first_gate < strstr(sync, "result |= BUF_REUSABLE"));
		if (content_share != NULL && second_gate != NULL)
			UT_ASSERT(content_share < second_gate);
	}
	if (flush != NULL)
		UT_ASSERT(strstr(flush, "cluster_bufmgr_pcm_x_retained_image_locked")
				  < strstr(flush, "StartBufferIO(buf, false)"));
	if (dirty != NULL)
		UT_ASSERT(strstr(dirty, "cluster_bufmgr_pcm_x_retained_image_locked")
				  < strstr(dirty, "buf_state |= BM_DIRTY"));
	if (hint != NULL)
		UT_ASSERT(strstr(hint, "cluster_bufmgr_pcm_x_retained_image_locked")
				  < strstr(hint, "BM_DIRTY | BM_JUST_DIRTIED"));
	if (lockbuffer != NULL) {
		const char *reserve = strstr(lockbuffer, "cluster_pcm_own_begin_grant_reservation(");
		const char *content
			= strstr(lockbuffer, "LWLockAcquire(BufferDescriptorGetContentLock(buf), LW_SHARED)");
		const char *w1_revoke
			= strstr(lockbuffer, "PCM_OWN_FLAG_GRANT_PENDING | PCM_OWN_FLAG_REVOKING");

		UT_ASSERT_NOT_NULL(reserve);
		UT_ASSERT_NOT_NULL(content);
		UT_ASSERT_NOT_NULL(w1_revoke);
		if (reserve != NULL && content != NULL)
			UT_ASSERT(reserve < content);
	}
	if (conditional != NULL) {
		const char *content
			= strstr(conditional, "LWLockConditionalAcquire(BufferDescriptorGetContentLock(buf)");
		const char *ownership = strstr(conditional, "cluster_pcm_x_conditional_lock_allowed(");
		const char *release
			= strstr(conditional, "LWLockRelease(BufferDescriptorGetContentLock(buf))");

		UT_ASSERT_NOT_NULL(content);
		UT_ASSERT_NOT_NULL(ownership);
		UT_ASSERT_NOT_NULL(release);
		if (content != NULL && ownership != NULL && release != NULL)
			UT_ASSERT(content < ownership && ownership < release);
	}
	if (resident_stamp != NULL) {
		const char *content = strstr(
			resident_stamp, "LWLockAcquire(BufferDescriptorGetContentLock(buf), LW_EXCLUSIVE)");
		const char *gate = strstr(resident_stamp, "cluster_bufmgr_pcm_x_retained_image_locked");

		UT_ASSERT_NOT_NULL(content);
		UT_ASSERT_NOT_NULL(gate);
		if (content != NULL && gate != NULL)
			UT_ASSERT(content < gate);
	}
	if (storage_refresh != NULL) {
		const char *content = strstr(storage_refresh, "LWLockAcquire(content_lock, LW_EXCLUSIVE)");
		const char *gate = strstr(storage_refresh, "cluster_bufmgr_pcm_x_content_write_permitted");
		const char *copy = strstr(storage_refresh, "memcpy((char *) BufHdrGetBlock(buf)");

		UT_ASSERT_NOT_NULL(content);
		UT_ASSERT_NOT_NULL(gate);
		UT_ASSERT_NOT_NULL(copy);
		if (content != NULL && gate != NULL && copy != NULL)
			UT_ASSERT(content < gate && gate < copy);
	}
	free(source);
}

UT_TEST(test_retained_drain_retags_invalid_only_after_exact_token_release)
{
	static const char *const drain_contract[] = { "cluster_pcm_own_revoke_retain_release_exact",
												  "result == CLUSTER_PCM_OWN_OK",
												  "buf_state &= ~BM_VALID",
												  "buf->buffer_type",
												  "BUF_TYPE_CURRENT",
												  "UnlockBufHdr" };
	char *source = read_bufmgr_source();
	const char *release;
	const char *release_end;

	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_release_retained_image(",
		"\n/* ========================================================================\n * PGRAC "
		"MODIFICATIONS by SqlRush — spec-6.12h D-h2",
		drain_contract, lengthof(drain_contract));

	release = strstr(source, "\ncluster_bufmgr_pcm_own_release_retained_image(");
	release_end = release != NULL ? strstr(release + 1, "\n/* "
														"=========================================="
														"==============================\n * PGRAC ")
								  : NULL;
	UT_ASSERT_NOT_NULL(release);
	UT_ASSERT_NOT_NULL(release_end);
	if (release != NULL && release_end != NULL)
		UT_ASSERT(strstr(release, "InvalidateBufferCommitTailLocked") == NULL
				  || strstr(release, "InvalidateBufferCommitTailLocked") >= release_end);

	free(source);
}

UT_TEST(test_queue_s_release_finish_is_header_exact_and_returns_fresh_n)
{
	static const char *const release_contract[] = { "expected_s->pcm_state != (uint8) PCM_STATE_S",
													"expected_s->flags != 0",
													"LockBufHdr",
													"cluster_pcm_own_snapshot_matches_locked",
													"cluster_pcm_own_bump_locked",
													"buf->pcm_state = (uint8) PCM_STATE_N",
													"cluster_pcm_own_snapshot_locked",
													"UnlockBufHdr" };
	char *source = read_bufmgr_source();

	/* The caller proves the exact remote RELEASE application ACK before this
	 * adapter.  The adapter then normalizes only the matching S tuple and
	 * returns the fresh N generation that PREPARE must reserve. */
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_finish_s_release_to_n(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_begin_x_reservation(",
							   release_contract, lengthof(release_contract));
	free(source);
}

UT_TEST(test_lockbuffer_pcm_x_holder_ledger_brackets_both_content_acquires)
{
	static const char *const unlock_contract[]
		= { "cluster_bufmgr_pcm_x_holder_mark_releasing",
			"LWLockRelease(BufferDescriptorGetContentLock(buf))",
			"cluster_bufmgr_pcm_x_holder_unregister" };
	static const char *const acquire_contract[]
		= { "pcm_x_holder = cluster_bufmgr_pcm_x_holder_prepare(buf)",
			"LWLockAcquire(BufferDescriptorGetContentLock(buf)",
			"LWLockRelease(BufferDescriptorGetContentLock(buf))",
			"pcm_x_holder = cluster_bufmgr_pcm_x_holder_prepare(buf)",
			"LWLockAcquire(BufferDescriptorGetContentLock(buf)",
			"cluster_bufmgr_pcm_x_holder_activate(pcm_x_holder)" };
	char *source = read_bufmgr_source();

	/* One ACQUIRING entry brackets the first content-lock acquire and remains
	 * published across the W1 release/reacquire fallback.  Normal release
	 * publishes RELEASING before unlocking and unlinks only afterwards. */
	assert_ordered_in_function(
		source, "\nLockBuffer(Buffer buffer, int mode)",
		"\n/*\n * Acquire the content_lock for the buffer, but only if we don't have to wait.",
		unlock_contract, lengthof(unlock_contract));
	assert_ordered_in_function(
		source, "\nLockBuffer(Buffer buffer, int mode)",
		"\n/*\n * Acquire the content_lock for the buffer, but only if we don't have to wait.",
		acquire_contract, lengthof(acquire_contract));
	free(source);
}

UT_TEST(test_bufmgr_pcm_x_holder_ledger_is_bounded_and_uses_private_identity)
{
	static const char *const identity_contract[]
		= { "cluster_bufmgr_pcm_x_holder_find",
			"entry != NULL",
			"return entry",
			"cluster_pcm_own_read",
			"cluster_epoch_get_current",
			"cluster_node_id",
			"MyProc->pgprocno",
			"cluster_bufmgr_pcm_x_holder_identity == UINT64_MAX",
			"ERRCODE_PROGRAM_LIMIT_EXCEEDED",
			"++cluster_bufmgr_pcm_x_holder_identity",
			"key.identity.request_id = identity",
			"key.identity.wait_seq = identity",
			"key.identity.base_own_generation = own_generation",
			"cluster_pcm_x_local_holder_register" };
	char *source = read_bufmgr_source();

	/* The ledger is backend-local and no larger than PG's own maximum held
	 * LWLock set.  Its checked identity populates only the protocol key; it
	 * must never borrow or mutate PGPROC's deadlock wait sequence. */
	UT_ASSERT_NOT_NULL(
		strstr(source, "cluster_bufmgr_pcm_x_holder_ledger[LWLOCK_MAX_HELD_BY_PROC]"));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_prepare(", "\nstatic ",
							   identity_contract, lengthof(identity_contract));
	UT_ASSERT_NULL(strstr(source, "cluster_lmd_wait.wait_seq"));
	free(source);
}

UT_TEST(test_unlockbuffers_exceptionally_detaches_released_pcm_x_holders)
{
	static const char *const cleanup_contract[]
		= { "LWLockHeldByMe(entry->content_lock)",
			"cluster_pcm_x_local_holder_exceptional_detach_exact(&entry->handle",
			"entry->content_lock" };
	static const char *const catch_contract[]
		= { "!LWLockHeldByMe(BufferDescriptorGetContentLock(buf))",
			"cluster_bufmgr_pcm_x_holder_abort_acquiring" };
	static const char *const eoxact_contract[]
		= { "AtEOXact_LocalBuffers(isCommit)",
			"cluster_bufmgr_pcm_x_holder_drain_deferred_nowait()",
			"Assert(PrivateRefCountOverflowed == 0)" };
	char *source = read_bufmgr_source();
	const char *cleanup_begin;
	const char *cleanup_end;
	const char *mirror_read;

	/* AbortTransaction releases all LWLocks before UnlockBuffers.  The
	 * process-local ledger must prove each content lock is gone before using
	 * the multi-state exceptional detach; an acquire that threw before owning
	 * the lock keeps the narrower ACQUIRING-only rollback. */
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_exception_cleanup_all(",
							   "\nstatic ", cleanup_contract, lengthof(cleanup_contract));
	cleanup_begin = strstr(source, "\ncluster_bufmgr_pcm_x_holder_exception_cleanup_all(");
	UT_ASSERT_NOT_NULL(cleanup_begin);
	cleanup_end = cleanup_begin == NULL ? NULL : strstr(cleanup_begin + 1, "\nstatic ");
	UT_ASSERT_NOT_NULL(cleanup_end);
	mirror_read = cleanup_begin == NULL ? NULL : strstr(cleanup_begin, "pcm_state");
	UT_ASSERT(mirror_read == NULL || (cleanup_end != NULL && mirror_read >= cleanup_end));
	UT_ASSERT(cleanup_begin == NULL || cleanup_end == NULL
			  || strstr(cleanup_begin, "WaitLatch(") == NULL
			  || strstr(cleanup_begin, "WaitLatch(") >= cleanup_end);
	UT_ASSERT(cleanup_begin == NULL || cleanup_end == NULL
			  || strstr(cleanup_begin, "CHECK_FOR_INTERRUPTS()") == NULL
			  || strstr(cleanup_begin, "CHECK_FOR_INTERRUPTS()") >= cleanup_end);
	assert_ordered_in_function(source,
							   "CLUSTER_INJECTION_POINT(\"cluster-pcm-writer-cached-x-stall\")",
							   "PG_END_TRY();", catch_contract, lengthof(catch_contract));
	UT_ASSERT_NOT_NULL(strstr(source, "cluster_bufmgr_pcm_x_holder_exception_cleanup_all();"));
	assert_ordered_in_function(source, "\nAtEOXact_Buffers(bool isCommit)",
							   "\n/*\n * Initialize access to shared buffer pool", eoxact_contract,
							   lengthof(eoxact_contract));
	free(source);
}

UT_TEST(test_bufmgr_pcm_x_holder_gate_retry_is_bounded_outside_content_lock)
{
	static const char *const wait_contract[]
		= { "content_lock == NULL || LWLockHeldByMe(content_lock)",
			"ereport(ERROR",
			"cluster_pcm_x_nested_wait_guard_before_block()",
			"cluster_bufmgr_pcm_x_holder_report_failure(",
			"cluster_pcm_x_holder_retry_delay_ms(wait_index)",
			"CHECK_FOR_INTERRUPTS()",
			"WaitLatch(MyLatch",
			"WAIT_EVENT_PCM_BLOCK_CONVERT_WAIT",
			"CHECK_FOR_INTERRUPTS()" };
	static const char *const register_contract[]
		= { "cluster_bufmgr_pcm_x_holder_drain_deferred_nowait()",
			"entry->phase == PCM_X_HOLDER_LEDGER_DEFERRED",
			"cluster_bufmgr_pcm_x_holder_drain_deferred(entry)",
			"cluster_pcm_x_local_holder_register(&key, &handle)",
			"cluster_pcm_x_holder_register_retry_action(",
			"cluster_bufmgr_pcm_x_holder_retry_wait(" };
	static const char *const unregister_contract[]
		= { "entry->content_lock == NULL || LWLockHeldByMe(entry->content_lock)",
			"cluster_pcm_x_local_holder_unregister_exact(&entry->handle)",
			"cluster_pcm_x_holder_unregister_retry_action(result, waits_used)",
			"CLUSTER_PCM_X_HOLDER_RETRY_DEFER",
			"entry->phase = PCM_X_HOLDER_LEDGER_DEFERRED",
			"return" };
	static const PcmXQueueResult register_script[]
		= { PCM_X_QUEUE_GATE_RETRY, PCM_X_QUEUE_GATE_RETRY, PCM_X_QUEUE_OK };
	char *source = read_bufmgr_source();
	const char *wait_begin;
	const char *wait_end;
	int i;

	for (i = 0; i < lengthof(register_script); i++)
		UT_ASSERT_EQ(cluster_pcm_x_holder_register_retry_action(register_script[i], true),
					 i + 1 == lengthof(register_script) ? CLUSTER_PCM_X_HOLDER_RETRY_COMPLETE
														: CLUSTER_PCM_X_HOLDER_RETRY_WAIT);
	UT_ASSERT_EQ(cluster_pcm_x_holder_register_retry_action(PCM_X_QUEUE_DUPLICATE, true),
				 CLUSTER_PCM_X_HOLDER_RETRY_COMPLETE);
	UT_ASSERT_EQ(cluster_pcm_x_holder_register_retry_action(PCM_X_QUEUE_BARRIER_CLOSED, true),
				 CLUSTER_PCM_X_HOLDER_RETRY_WAIT);
	UT_ASSERT_EQ(cluster_pcm_x_holder_register_retry_action(PCM_X_QUEUE_NOT_READY, true),
				 CLUSTER_PCM_X_HOLDER_RETRY_WAIT);
	UT_ASSERT_EQ(cluster_pcm_x_holder_register_retry_action(PCM_X_QUEUE_NOT_READY, false),
				 CLUSTER_PCM_X_HOLDER_RETRY_FAIL);
	UT_ASSERT_EQ(cluster_pcm_x_holder_register_retry_action(PCM_X_QUEUE_NO_CAPACITY, true),
				 CLUSTER_PCM_X_HOLDER_RETRY_FAIL);
	for (i = 0; i < CLUSTER_PCM_X_HOLDER_RETRY_BATCH_WAITS; i++) {
		UT_ASSERT_EQ(cluster_pcm_x_holder_unregister_retry_action(PCM_X_QUEUE_GATE_RETRY, i),
					 CLUSTER_PCM_X_HOLDER_RETRY_WAIT);
		UT_ASSERT_EQ(cluster_pcm_x_writer_release_retry_action(PCM_X_QUEUE_BUSY, i),
					 CLUSTER_PCM_X_WRITER_RETRY_WAIT);
		UT_ASSERT_EQ(cluster_pcm_x_holder_retry_delay_ms(i), 2L << i);
	}
	UT_ASSERT_EQ(cluster_pcm_x_holder_unregister_retry_action(
					 PCM_X_QUEUE_BUSY, CLUSTER_PCM_X_HOLDER_RETRY_BATCH_WAITS),
				 CLUSTER_PCM_X_HOLDER_RETRY_DEFER);
	UT_ASSERT_EQ(cluster_pcm_x_holder_unregister_retry_action(PCM_X_QUEUE_NOT_FOUND, 0),
				 CLUSTER_PCM_X_HOLDER_RETRY_COMPLETE);
	UT_ASSERT_EQ(cluster_pcm_x_holder_unregister_retry_action(PCM_X_QUEUE_CORRUPT, 0),
				 CLUSTER_PCM_X_HOLDER_RETRY_FAIL);
	UT_ASSERT_EQ(cluster_pcm_x_writer_release_retry_action(PCM_X_QUEUE_OK, 0),
				 CLUSTER_PCM_X_WRITER_RETRY_COMPLETE);
	UT_ASSERT_EQ(cluster_pcm_x_writer_release_retry_action(PCM_X_QUEUE_GATE_RETRY,
														   CLUSTER_PCM_X_HOLDER_RETRY_BATCH_WAITS),
				 CLUSTER_PCM_X_WRITER_RETRY_DEFER);
	UT_ASSERT_EQ(cluster_pcm_x_writer_release_retry_action(PCM_X_QUEUE_NOT_READY, 0),
				 CLUSTER_PCM_X_WRITER_RETRY_FAIL);
	UT_ASSERT_EQ(cluster_pcm_x_holder_retry_delay_ms(UINT32_C(99)), 32);
	UT_ASSERT_EQ(cluster_pcm_x_owner_exit_action(PCM_X_QUEUE_OK, false, true),
				 CLUSTER_PCM_X_OWNER_EXIT_COMPLETE);
	UT_ASSERT_EQ(cluster_pcm_x_owner_exit_action(PCM_X_QUEUE_NOT_FOUND, true, true),
				 CLUSTER_PCM_X_OWNER_EXIT_COMPLETE);
	UT_ASSERT_EQ(cluster_pcm_x_owner_exit_action(PCM_X_QUEUE_BUSY, false, true),
				 CLUSTER_PCM_X_OWNER_EXIT_RETRY);
	UT_ASSERT_EQ(cluster_pcm_x_owner_exit_action(PCM_X_QUEUE_GATE_RETRY, true, true),
				 CLUSTER_PCM_X_OWNER_EXIT_RETRY);
	UT_ASSERT_EQ(cluster_pcm_x_owner_exit_action(PCM_X_QUEUE_BUSY, false, false),
				 CLUSTER_PCM_X_OWNER_EXIT_PRESERVE);
	UT_ASSERT_EQ(cluster_pcm_x_owner_exit_action(PCM_X_QUEUE_NOT_READY, true, true),
				 CLUSTER_PCM_X_OWNER_EXIT_PRESERVE);
	UT_ASSERT_EQ(cluster_pcm_x_owner_exit_action(PCM_X_QUEUE_CORRUPT, false, true),
				 CLUSTER_PCM_X_OWNER_EXIT_PRESERVE);

	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_retry_wait(", "\nstatic ",
							   wait_contract, lengthof(wait_contract));
	wait_begin = strstr(source, "\ncluster_bufmgr_pcm_x_holder_retry_wait(");
	wait_end = wait_begin == NULL ? NULL : strstr(wait_begin + 1, "\nstatic ");
	UT_ASSERT_NOT_NULL(wait_begin);
	UT_ASSERT_NOT_NULL(wait_end);
	UT_ASSERT(wait_begin == NULL || wait_end == NULL
			  || strstr(wait_begin, "ResetLatch(MyLatch)") == NULL
			  || strstr(wait_begin, "ResetLatch(MyLatch)") >= wait_end);
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_prepare(", "\nstatic ",
							   register_contract, lengthof(register_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_unregister(", "\nstatic ",
							   unregister_contract, lengthof(unregister_contract));
	UT_ASSERT_NOT_NULL(strstr(source, "cluster_bufmgr_pcm_x_holder_drain_deferred_nowait();"));
	free(source);
}

UT_TEST(test_bufmgr_pcm_x_holder_reuse_and_deferred_failure_are_fail_closed)
{
	static const char *const exact_contract[]
		= { "entry == NULL || buf == NULL",
			"entry->buffer_id != buf->buf_id",
			"entry->content_lock != BufferDescriptorGetContentLock(buf)",
			"entry->handle.key.buffer_id != buf->buf_id",
			"!BufferTagsEqual(&entry->handle.key.identity.tag, &buf->tag)",
			"entry->handle.key.identity.node_id != cluster_node_id",
			"MyProc == NULL",
			"entry->handle.key.identity.procno != (uint32) MyProc->pgprocno",
			"entry->handle.key.identity.request_id == 0",
			"entry->handle.key.identity.wait_seq != entry->handle.key.identity.request_id",
			"cluster_epoch = cluster_epoch_get_current()",
			"runtime = cluster_pcm_x_runtime_snapshot()",
			"entry->handle.key.identity.cluster_epoch != cluster_epoch",
			"runtime.state != PCM_X_RUNTIME_ACTIVE",
			"runtime.master_session_incarnation == 0",
			"return true" };
	static const char *const prepare_contract[]
		= { "entry = cluster_bufmgr_pcm_x_holder_find(buf)",
			"if (entry != NULL)",
			"entry->phase != PCM_X_HOLDER_LEDGER_ACQUIRING",
			"!cluster_bufmgr_pcm_x_holder_entry_exact(entry, buf)",
			"cluster_bufmgr_pcm_x_holder_defer_fail_closed(entry)",
			"cluster_bufmgr_pcm_x_holder_report_failure",
			"LWLockHeldByMe(entry->content_lock)" };
	static const char *const deferred_contract[]
		= { "cluster_bufmgr_pcm_x_holder_retry_wait(",
			"wait_index++",
			"wait_index % CLUSTER_PCM_X_HOLDER_RETRY_BATCH_WAITS == 0",
			"runtime = cluster_pcm_x_runtime_snapshot()",
			"runtime.state != PCM_X_RUNTIME_ACTIVE",
			"runtime.master_session_incarnation == 0",
			"cluster_bufmgr_pcm_x_holder_defer_fail_closed(entry)",
			"cluster_bufmgr_pcm_x_holder_report_failure" };
	static const char *const nowait_contract[]
		= { "cluster_pcm_x_local_holder_exceptional_detach_exact", "PCM_X_QUEUE_GATE_RETRY",
			"cluster_bufmgr_pcm_x_holder_defer_fail_closed(entry)", "elog(LOG" };
	static const char *const abort_contract[]
		= { "cluster_pcm_x_local_holder_abort_acquiring_exact", "PCM_X_QUEUE_GATE_RETRY",
			"entry->phase = PCM_X_HOLDER_LEDGER_DEFERRED",
			"cluster_bufmgr_pcm_x_holder_defer_fail_closed(entry)", "elog(LOG" };
	static const char *const exception_contract[]
		= { "cluster_pcm_x_local_holder_exceptional_detach_exact", "PCM_X_QUEUE_GATE_RETRY",
			"entry->phase = PCM_X_HOLDER_LEDGER_DEFERRED",
			"cluster_bufmgr_pcm_x_holder_defer_fail_closed(entry)", "elog(LOG" };
	static const char *const unregister_contract[]
		= { "action != CLUSTER_PCM_X_HOLDER_RETRY_WAIT",
			"cluster_bufmgr_pcm_x_holder_defer_fail_closed(entry)",
			"cluster_bufmgr_pcm_x_holder_report_failure" };
	char *source = read_bufmgr_source();
	const char *entry_begin;
	const char *entry_end;

	/* A W1 fallback may reuse one ACQUIRING ledger entry, but buffer-id alone
	 * is not identity: descriptor retag, backend/epoch drift, or a stale lock
	 * pointer must close the runtime and retain the exact old handle. */
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_entry_exact(", "\nstatic ",
							   exact_contract, lengthof(exact_contract));
	entry_begin = strstr(source, "\ncluster_bufmgr_pcm_x_holder_entry_exact(");
	entry_end = entry_begin == NULL ? NULL : strstr(entry_begin + 1, "\nstatic ");
	UT_ASSERT_NOT_NULL(entry_begin);
	UT_ASSERT_NOT_NULL(entry_end);
	UT_ASSERT(entry_begin == NULL || entry_end == NULL
			  || strstr(entry_begin, "cluster_epoch == 0") == NULL
			  || strstr(entry_begin, "cluster_epoch == 0") >= entry_end);
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_prepare(", "\nstatic ",
							   prepare_contract, lengthof(prepare_contract));

	/* A live RETIRE gate can legitimately span one retry batch.  It cannot
	 * justify an infinite same-buffer wait after the formation runtime has
	 * left ACTIVE; all hard cleanup outcomes preserve exact evidence under a
	 * single explicit fail-closed transition. */
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_drain_deferred(", "\nstatic ",
							   deferred_contract, lengthof(deferred_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_drain_deferred_nowait(",
							   "\nstatic ", nowait_contract, lengthof(nowait_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_abort_acquiring(",
							   "\nstatic ", abort_contract, lengthof(abort_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_exception_cleanup_all(",
							   "\nstatic ", exception_contract, lengthof(exception_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_unregister(", "\nstatic ",
							   unregister_contract, lengthof(unregister_contract));
	free(source);
}

UT_TEST(test_queue_holder_snapshot_by_tag_is_mapping_and_header_exact)
{
	typedef ClusterPcmOwnResult (*SnapshotByTagFn)(const BufferTag *, int *,
												   ClusterPcmOwnSnapshot *);
	static const char *const snapshot_contract[]
		= { "BufTableHashCode", "LWLockAcquire(partition_lock, LW_SHARED)",
			"BufTableLookup",	"GetBufferDescriptor",
			"LockBufHdr",		"BufferTagsEqual",
			"BM_VALID",			"cluster_pcm_own_snapshot_locked",
			"UnlockBufHdr",		"LWLockRelease(partition_lock)" };
	char *source = read_bufmgr_source();

	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_snapshot_by_tag),
										   SnapshotByTagFn));
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_snapshot_by_tag(",
		"\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_finish_s_release_to_n(", snapshot_contract,
		lengthof(snapshot_contract));
	free(source);
}

UT_TEST(test_queue_passive_pinned_s_release_serializes_bytes_and_ownership)
{
	typedef ClusterPcmOwnResult (*PassiveReleaseFn)(const BufferTag *, XLogRecPtr *, uint64 *);
	static const char *const release_contract[]
		= { "BufTableHashCode",
			"LWLockAcquire(partition_lock, LW_SHARED)",
			"BufTableLookup",
			"LockBufHdr",
			"cluster_pcm_x_revoke_finish_mode(tag, shared_refcount)",
			"cluster_bufmgr_pin_for_gcs_locked",
			"LWLockRelease(partition_lock)",
			"LWLockAcquire(content_lock, LW_EXCLUSIVE)",
			"cluster_pcm_own_snapshot_matches_locked",
			"PageGetLSN",
			"XLogFlush",
			"LockBufHdr",
			"cluster_pcm_own_snapshot_matches_locked",
			"cluster_pcm_own_bump_locked",
			"buf->pcm_state = (uint8) PCM_STATE_N",
			"buf->buffer_type = (uint8) BUF_TYPE_PI",
			"BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED",
			"BM_IO_ERROR",
			"cluster_bufmgr_unpin_for_gcs" };
	char *source = read_bufmgr_source();

	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_release_pinned_s_for_gcs), PassiveReleaseFn));
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_release_pinned_s_for_gcs(",
		"\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_publish_installed_x_image(",
		release_contract, lengthof(release_contract));
	free(source);
}

UT_TEST(test_current_image_shape_accepts_monotone_xcur_after_x_to_s_yield)
{
	UT_ASSERT(cluster_pcm_x_current_image_shape((uint8)PCM_STATE_S, (uint8)BUF_TYPE_SCUR, true));
	UT_ASSERT(cluster_pcm_x_current_image_shape((uint8)PCM_STATE_S, (uint8)BUF_TYPE_XCUR, true));
	UT_ASSERT(!cluster_pcm_x_current_image_shape((uint8)PCM_STATE_X, (uint8)BUF_TYPE_SCUR, true));
	UT_ASSERT(cluster_pcm_x_current_image_shape((uint8)PCM_STATE_X, (uint8)BUF_TYPE_XCUR, true));
	UT_ASSERT(!cluster_pcm_x_current_image_shape((uint8)PCM_STATE_N, (uint8)BUF_TYPE_PI, true));
	UT_ASSERT(!cluster_pcm_x_current_image_shape((uint8)PCM_STATE_S, (uint8)BUF_TYPE_PI, true));
	UT_ASSERT(!cluster_pcm_x_current_image_shape((uint8)PCM_STATE_S, (uint8)BUF_TYPE_XCUR, false));
}

UT_TEST(test_conditional_lock_preserves_native_off_and_enforces_tracked_x)
{
	UT_ASSERT(cluster_pcm_x_conditional_lock_allowed(false, true, false, (uint8)PCM_STATE_N, 0));
	UT_ASSERT(cluster_pcm_x_conditional_lock_allowed(true, false, false, (uint8)PCM_STATE_N, 0));
	UT_ASSERT(!cluster_pcm_x_conditional_lock_allowed(true, true, false, (uint8)PCM_STATE_N, 0));
	UT_ASSERT(!cluster_pcm_x_conditional_lock_allowed(true, true, false, (uint8)PCM_STATE_S, 0));
	UT_ASSERT(cluster_pcm_x_conditional_lock_allowed(true, true, false, (uint8)PCM_STATE_X, 0));
	UT_ASSERT(!cluster_pcm_x_conditional_lock_allowed(false, false, true, (uint8)PCM_STATE_X, 0));
	UT_ASSERT(!cluster_pcm_x_conditional_lock_allowed(false, false, false, (uint8)PCM_STATE_X,
													  PCM_OWN_FLAG_GRANT_PENDING));
}

UT_TEST(test_queue_passive_n_mirror_is_never_gcs_ship_authority)
{
	static const char *const probe_contract[]
		= { "LockBufHdr", "cluster_bufmgr_pcm_current_image_locked", "UnlockBufHdr" };
	static const char *const copy_contract[] = { "LockBufHdr",
												 "cluster_bufmgr_pcm_current_image_locked",
												 "cluster_bufmgr_pin_for_gcs_locked",
												 "LWLockAcquire(content_lock, LW_SHARED)",
												 "cluster_bufmgr_pcm_current_image_locked",
												 "memcpy(dst, page, BLCKSZ)" };
	static const char *const live_sge_contract[] = { "LockBufHdr",
													 "cluster_bufmgr_pcm_current_image_locked",
													 "cluster_bufmgr_pin_for_gcs_locked",
													 "LWLockAcquire(content_lock, LW_SHARED)",
													 "cluster_bufmgr_pcm_current_image_locked",
													 "*out_page_addr = page" };
	static const char *const smart_contract[] = { "LockBufHdr",
												  "cluster_bufmgr_pcm_current_image_locked",
												  "cluster_bufmgr_pin_for_gcs_locked",
												  "LWLockAcquire(content_lock, LW_SHARED)",
												  "cluster_bufmgr_pcm_current_image_locked",
												  "memcpy(dst, page, BLCKSZ)" };
	char *source = read_bufmgr_source();

	assert_ordered_in_function(source, "\ncluster_bufmgr_probe_block_for_gcs(",
							   "\n/*\n * Read the shared-storage version", probe_contract,
							   lengthof(probe_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_copy_block_for_gcs(",
							   "\n/*\n * Borrow a live shared_buffers page", copy_contract,
							   lengthof(copy_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_borrow_block_for_gcs_live_sge(",
							   "\nvoid\ncluster_bufmgr_release_block_for_gcs_live_sge(",
							   live_sge_contract, lengthof(live_sge_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_copy_block_for_gcs_smart_fusion(",
							   "\n/*\n * cluster_bufmgr_redeclare_scan_chunk", smart_contract,
							   lengthof(smart_contract));
	free(source);
}

UT_TEST(test_queue_installed_image_publication_is_exact_and_content_locked)
{
	typedef ClusterPcmOwnResult (*PublishImageFn)(BufferDesc *, const ClusterPcmOwnSnapshot *,
												  uint64);
	static const char *const publish_contract[]
		= { "LWLockHeldByMe(BufferDescriptorGetContentLock(buf))",
			"LockBufHdr",
			"BufferTagsEqual",
			"cluster_pcm_own_gen_get",
			"cluster_pcm_own_reservation_token_get",
			"PCM_OWN_FLAG_GRANT_PENDING",
			"buf->pcm_state != (uint8) PCM_STATE_N",
			"buf_state |= BM_VALID",
			"UnlockBufHdr" };
	char *source = read_bufmgr_source();

	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_publish_installed_x_image), PublishImageFn));
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_publish_installed_x_image(",
		"\nstatic ClusterPcmOwnResult\ncluster_pcm_own_begin_grant_reservation(", publish_contract,
		lengthof(publish_contract));
	free(source);
}

UT_TEST(test_queue_self_source_handoff_is_single_lifecycle_and_readonly_drain)
{
	static const char *const handoff_contract[] = { "LWLockHeldByMe(content_lock)",
													"LockBufHdr",
													"cluster_pcm_own_classify_live_flags",
													"cluster_bufmgr_pcm_current_image_locked",
													"cluster_pcm_own_revoke_to_grant_handoff_exact",
													"UnlockBufHdr" };
	static const char *const drain_proof_contract[] = { "BufMappingPartitionLock",
														"LockBufHdr",
														"source_generation + 1",
														"flags != 0",
														"PCM_STATE_X",
														"BUF_TYPE_XCUR",
														"UnlockBufHdr" };
	char *source = read_bufmgr_source();
	const char *handoff;
	const char *handoff_end;
	const char *forbidden;

	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_handoff_revoke_to_x_reservation(",
		"\nstatic ClusterPcmOwnResult\ncluster_pcm_own_finish_grant_reservation(", handoff_contract,
		lengthof(handoff_contract));
	handoff = strstr(source, "\ncluster_bufmgr_pcm_own_handoff_revoke_to_x_reservation(");
	handoff_end
		= handoff != NULL
			  ? strstr(handoff,
					   "\nstatic ClusterPcmOwnResult\ncluster_pcm_own_finish_grant_reservation(")
			  : NULL;
	UT_ASSERT_NOT_NULL(handoff);
	UT_ASSERT_NOT_NULL(handoff_end);
	if (handoff != NULL && handoff_end != NULL) {
		forbidden = strstr(handoff, "cluster_pcm_own_reservation_begin_exact(");
		UT_ASSERT(forbidden == NULL || forbidden >= handoff_end);
		forbidden = strstr(handoff, "cluster_pcm_own_reservation_abort_exact(");
		UT_ASSERT(forbidden == NULL || forbidden >= handoff_end);
	}
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_self_handoff_x_exact(",
							   "\n/* ==========", drain_proof_contract,
							   lengthof(drain_proof_contract));
	free(source);
}

UT_TEST(test_queue_writer_grant_snapshot_is_claim_and_generation_exact)
{
	PcmXLocalWriterClaim claim;
	ClusterPcmOwnSnapshot granted;
	ClusterPcmOwnSnapshot live;

	memset(&claim, 0, sizeof(claim));
	claim.writer.identity.base_own_generation = 10;
	claim.writer.membership_slot.slot_index = 3;
	claim.writer.membership_slot.slot_generation = 5;
	claim.writer.local_round = 7;
	claim.writer.role = PCM_X_LOCAL_ROLE_NODE_LEADER;
	claim.active_slot = claim.writer.membership_slot;
	claim.claim_generation = 11;
	claim.local_round = claim.writer.local_round;
	claim.role = claim.writer.role;
	memset(&granted, 0, sizeof(granted));
	granted.generation = 11;
	granted.reservation_token = 13;
	granted.pcm_state = (uint8)PCM_STATE_X;
	live = granted;

	UT_ASSERT(cluster_pcm_x_writer_grant_snapshot_exact(&claim, &granted, &live));
	live.generation++;
	UT_ASSERT(!cluster_pcm_x_writer_grant_snapshot_exact(&claim, &granted, &live));
	live = granted;
	live.reservation_token++;
	UT_ASSERT(!cluster_pcm_x_writer_grant_snapshot_exact(&claim, &granted, &live));
	live = granted;
	live.flags = PCM_OWN_FLAG_REVOKING;
	UT_ASSERT(!cluster_pcm_x_writer_grant_snapshot_exact(&claim, &granted, &live));
	live = granted;
	live.pcm_state = (uint8)PCM_STATE_S;
	UT_ASSERT(!cluster_pcm_x_writer_grant_snapshot_exact(&claim, &granted, &live));
	live = granted;
	/* A' rebase: a published effective grant base supersedes the enqueue-time
	 * identity base in the exact granted-generation binding. */
	claim.grant_base_own_generation = 14;
	UT_ASSERT(!cluster_pcm_x_writer_grant_snapshot_exact(&claim, &granted, &live));
	granted.generation = 15;
	live = granted;
	UT_ASSERT(cluster_pcm_x_writer_grant_snapshot_exact(&claim, &granted, &live));
	claim.grant_base_own_generation = UINT64_MAX;
	UT_ASSERT(!cluster_pcm_x_writer_grant_snapshot_exact(&claim, &granted, &live));
	claim.grant_base_own_generation = 0;
	granted.generation = 11;
	live = granted;
	UT_ASSERT(cluster_pcm_x_writer_grant_snapshot_exact(&claim, &granted, &live));
	claim.active_slot.slot_generation++;
	UT_ASSERT(!cluster_pcm_x_writer_grant_snapshot_exact(&claim, &granted, &live));

	UT_ASSERT(cluster_pcm_x_should_release_legacy_on_unlock(false, false));
	UT_ASSERT(!cluster_pcm_x_should_release_legacy_on_unlock(false, true));
	UT_ASSERT(!cluster_pcm_x_should_release_legacy_on_unlock(true, false));
	UT_ASSERT(!cluster_pcm_x_should_release_legacy_on_unlock(true, true));

	UT_ASSERT(cluster_pcm_x_cached_cover_bypasses_queue(true, true, (uint8)PCM_STATE_X, 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_bypasses_queue(false, true, (uint8)PCM_STATE_X, 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_bypasses_queue(true, false, (uint8)PCM_STATE_X, 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_bypasses_queue(true, true, (uint8)PCM_STATE_S, 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_bypasses_queue(true, true, (uint8)PCM_STATE_X,
														 PCM_OWN_FLAG_GRANT_PENDING));
	UT_ASSERT(!cluster_pcm_x_cached_cover_bypasses_queue(true, true, (uint8)PCM_STATE_X,
														 PCM_OWN_FLAG_REVOKING));
}

UT_TEST(test_lockbuffer_pcm_x_writer_ledger_is_distinct_and_brackets_content_authority)
{
	static const char *const prepare_contract[]
		= { "entry->phase = PCM_X_WRITER_LEDGER_HANDOFF",
			"cluster_gcs_pcm_x_acquire_writer(buf, &entry->claim",
			"entry->claim_handed_off",
			"cluster_bufmgr_pcm_own_snapshot(buf, &granted)",
			"cluster_pcm_x_writer_grant_snapshot_exact(&entry->claim, &granted, &granted)",
			"entry->phase = PCM_X_WRITER_LEDGER_ACQUIRING" };
	static const char *const unlock_contract[]
		= { "pcm_x_writer_managed = pcm_x_writer != NULL",
			"cluster_bufmgr_pcm_x_writer_mark_releasing(pcm_x_writer)",
			"LWLockRelease(BufferDescriptorGetContentLock(buf))",
			"cluster_bufmgr_pcm_x_writer_release(pcm_x_writer)",
			"cluster_bufmgr_pcm_x_holder_unregister(pcm_x_holder)",
			"cluster_pcm_x_should_release_legacy_on_unlock(",
			"cluster_pcm_lock_unlock_content_buffer(buf, old_mode)" };
	static const char *const acquire_contract[]
		= { "pcm_covered = cluster_pcm_x_cached_cover_bypasses_queue(",
			"pcm_x_writer = cluster_bufmgr_pcm_x_writer_prepare(buf, pcm_mode)",
			"pcm_x_holder = cluster_bufmgr_pcm_x_holder_prepare(buf)",
			"LWLockAcquire(BufferDescriptorGetContentLock(buf)",
			"cluster_bufmgr_pcm_x_writer_activate(pcm_x_writer)" };
	static const char *const cleanup_contract[]
		= { "cluster_bufmgr_pcm_x_writer_exception_cleanup_all()",
			"cluster_bufmgr_pcm_x_holder_exception_cleanup_all()" };
	static const char *const owner_exit_contract[]
		= { "UnlockBuffers()", "cluster_bufmgr_pcm_x_owner_exit_drain()", "CheckForBufferLeaks()" };
	static const char *const owner_drain_contract[]
		= { "cluster_bufmgr_pcm_x_writer_owner_exit_drain_once(runtime_active)",
			"cluster_bufmgr_pcm_x_holder_owner_exit_drain_once(runtime_active)",
			"if (!writer_retry && !holder_retry)", "pg_usleep(1000L)" };
	static const char *const snapshot_failure_contract[] = {
		"release_result = cluster_gcs_pcm_x_writer_claim_cleanup_and_wake_noexcept(&entry->claim)",
		"cluster_bufmgr_pcm_x_writer_clear(entry)",
		"cluster_bufmgr_pcm_x_writer_report_failure(PCM_X_QUEUE_CORRUPT, buf"
	};
	static const char *const writer_holder_publish_contract[]
		= { "cluster_pcm_x_local_writer_holder_register_exact(", "entry->handle = handle",
			"entry->phase = PCM_X_HOLDER_LEDGER_ACQUIRING",
			"committed_own_generation != writer_entry->granted.generation",
			"cluster_bufmgr_pcm_x_writer_report_failure(PCM_X_QUEUE_CORRUPT, buf" };
	static const char *const writer_holder_runtime_contract[]
		= { "runtime = cluster_pcm_x_runtime_snapshot()", "runtime.state != PCM_X_RUNTIME_ACTIVE",
			"if (writer_authorized)",
			"cluster_bufmgr_pcm_x_writer_report_failure(PCM_X_QUEUE_NOT_READY, buf",
			"return NULL" };
	static const char *const deferred_release_contract[]
		= { "cluster_bufmgr_pcm_x_writer_claim_entry_exact(entry, buf)",
			"cluster_gcs_pcm_x_writer_claim_release_and_wake_exact(&entry->claim)" };
	static const char *const deferred_cleanup_contract[]
		= { "result = cluster_gcs_pcm_x_writer_claim_cleanup_and_wake_noexcept(&entry->claim)",
			"cluster_bufmgr_pcm_x_writer_clear(entry)" };
	static const char *const abort_cleanup_contract[]
		= { "result = cluster_gcs_pcm_x_writer_claim_cleanup_and_wake_noexcept(&entry->claim)",
			"entry->phase = PCM_X_WRITER_LEDGER_DEFERRED" };
	static const char *const exception_cleanup_contract[]
		= { "result = cluster_gcs_pcm_x_writer_claim_cleanup_and_wake_noexcept(&entry->claim)",
			"entry->phase = PCM_X_WRITER_LEDGER_DEFERRED" };
	static const char *const owner_exit_cleanup_contract[]
		= { "result = cluster_gcs_pcm_x_writer_claim_cleanup_and_wake_noexcept(&entry->claim)",
			"action = cluster_pcm_x_owner_exit_action(result, false, runtime_active)" };
	char *source = read_bufmgr_source();
	const char *writer_prepare;
	const char *writer_prepare_end;
	const char *protocol_external_wait;
	const char *claim_exact;
	const char *claim_exact_end;
	const char *grant_exact;
	const char *grant_exact_end;
	const char *owner_drain;
	const char *owner_drain_end;

	UT_ASSERT_NOT_NULL(
		strstr(source, "cluster_bufmgr_pcm_x_writer_ledger[LWLOCK_MAX_HELD_BY_PROC]"));
	UT_ASSERT_NOT_NULL(
		strstr(source, "cluster_bufmgr_pcm_x_holder_ledger[LWLOCK_MAX_HELD_BY_PROC]"));
	writer_prepare = strstr(source, "\ncluster_bufmgr_pcm_x_writer_prepare(");
	writer_prepare_end = writer_prepare != NULL ? strstr(writer_prepare, "\nstatic ") : NULL;
	protocol_external_wait = writer_prepare != NULL
								 ? strstr(writer_prepare, "cluster_bufmgr_pcm_x_holder_retry_wait(")
								 : NULL;
	UT_ASSERT_NOT_NULL(writer_prepare);
	UT_ASSERT_NOT_NULL(writer_prepare_end);
	UT_ASSERT(protocol_external_wait == NULL || protocol_external_wait >= writer_prepare_end);
	claim_exact = strstr(source, "\ncluster_bufmgr_pcm_x_writer_claim_entry_exact(");
	claim_exact_end = claim_exact != NULL ? strstr(claim_exact, "\n}\n") : NULL;
	grant_exact = strstr(source, "\ncluster_bufmgr_pcm_x_writer_entry_exact(");
	grant_exact_end = grant_exact != NULL ? strstr(grant_exact, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(claim_exact);
	UT_ASSERT_NOT_NULL(claim_exact_end);
	UT_ASSERT_NOT_NULL(grant_exact);
	UT_ASSERT_NOT_NULL(grant_exact_end);
	if (claim_exact != NULL && claim_exact_end != NULL)
		UT_ASSERT(strstr(claim_exact, "BufferTagsEqual(") == NULL
				  || strstr(claim_exact, "BufferTagsEqual(") >= claim_exact_end);
	if (grant_exact != NULL && grant_exact_end != NULL)
		UT_ASSERT(strstr(grant_exact, "BufferTagsEqual(") != NULL
				  && strstr(grant_exact, "BufferTagsEqual(") < grant_exact_end);
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_writer_prepare(", "\nstatic ",
							   prepare_contract, lengthof(prepare_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_writer_prepare(", "\nstatic ",
							   snapshot_failure_contract, lengthof(snapshot_failure_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_writer_release(", "\nstatic ",
							   deferred_release_contract, lengthof(deferred_release_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_writer_drain_deferred_nowait(",
							   "\nstatic ", deferred_cleanup_contract,
							   lengthof(deferred_cleanup_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_writer_abort_acquiring(",
							   "\nstatic ", abort_cleanup_contract,
							   lengthof(abort_cleanup_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_writer_exception_cleanup_all(",
							   "\nstatic ", exception_cleanup_contract,
							   lengthof(exception_cleanup_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_writer_owner_exit_drain_once(",
							   "\nstatic ", owner_exit_cleanup_contract,
							   lengthof(owner_exit_cleanup_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_prepare(", "\nstatic ",
							   writer_holder_publish_contract,
							   lengthof(writer_holder_publish_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_holder_prepare(", "\nstatic ",
							   writer_holder_runtime_contract,
							   lengthof(writer_holder_runtime_contract));
	assert_ordered_in_function(
		source, "\nLockBuffer(Buffer buffer, int mode)",
		"\n/*\n * Acquire the content_lock for the buffer, but only if we don't have to wait.",
		acquire_contract, lengthof(acquire_contract));
	assert_ordered_in_function(
		source, "\nLockBuffer(Buffer buffer, int mode)",
		"\n/*\n * Acquire the content_lock for the buffer, but only if we don't have to wait.",
		unlock_contract, lengthof(unlock_contract));
	assert_ordered_in_function(source, "\nUnlockBuffers(void)",
							   "\n/*\n * Acquire or release the content_lock for the buffer.",
							   cleanup_contract, lengthof(cleanup_contract));
	assert_ordered_in_function(source, "\nAtProcExit_Buffers(int code, Datum arg)",
							   "\n/*\n *\t\tCheckForBufferLeaks", owner_exit_contract,
							   lengthof(owner_exit_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_owner_exit_drain(", "\nstatic ",
							   owner_drain_contract, lengthof(owner_drain_contract));
	owner_drain = strstr(source, "\ncluster_bufmgr_pcm_x_owner_exit_drain(");
	owner_drain_end = owner_drain != NULL ? strstr(owner_drain, "\nstatic ") : NULL;
	UT_ASSERT_NOT_NULL(owner_drain);
	UT_ASSERT_NOT_NULL(owner_drain_end);
	if (owner_drain != NULL && owner_drain_end != NULL) {
		UT_ASSERT(strstr(owner_drain, "CHECK_FOR_INTERRUPTS") == NULL
				  || strstr(owner_drain, "CHECK_FOR_INTERRUPTS") >= owner_drain_end);
		UT_ASSERT(strstr(owner_drain, "WaitLatch(") == NULL
				  || strstr(owner_drain, "WaitLatch(") >= owner_drain_end);
		UT_ASSERT(strstr(owner_drain, "ereport(") == NULL
				  || strstr(owner_drain, "ereport(") >= owner_drain_end);
	}
	free(source);
}

int
main(void)
{
	UT_PLAN(45);
	UT_RUN(test_shmem_initializes_complete_entry);
	UT_RUN(test_begin_abort_is_exact_and_monotonic);
	UT_RUN(test_invalid_live_flag_shapes_are_corrupt_not_busy);
	UT_RUN(test_grant_commit_is_exact_and_bumps_once);
	UT_RUN(test_s_revoke_handoff_reuses_exact_token_and_bumps_once);
	UT_RUN(test_revoke_handoff_kinds_cover_n_s_x_with_one_lifecycle);
	UT_RUN(test_revoke_commit_is_exact_and_classifies_live_races);
	UT_RUN(test_revoke_retain_commit_keeps_exact_token_until_release);
	UT_RUN(test_revoke_commit_exhaustion_is_side_effect_free);
	UT_RUN(test_token_and_generation_never_wrap);
	UT_RUN(test_ordinary_generation_bump_rejects_live_reservation);
	UT_RUN(test_eviction_rejects_live_reservation_and_exhaustion);
	UT_RUN(test_bufmgr_d5a_commitlocked_uses_locked_commit_and_saved_tag_release);
	UT_RUN(test_bufmgr_abort_cleanup_is_never_silent);
	UT_RUN(test_bufmgr_finish_failure_rolls_back_acquired_master_grant);
	UT_RUN(test_bufmgr_s_base_rollback_normalizes_to_n_under_header_authority);
	UT_RUN(test_lockbuffer_content_error_uses_post_master_rollback_contract);
	UT_RUN(test_bufmgr_generation_bump_failure_is_classified_under_header_lock);
	UT_RUN(test_lockbuffer_reservation_failures_use_busy_corrupt_classifier);
	UT_RUN(test_bufmgr_finish_rejects_invalid_state_and_initializes_acquire_result);
	UT_RUN(test_bufmgr_finish_and_abort_gate_on_exact_base_state);
	UT_RUN(test_d5a_release_error_keeps_descriptor_out_of_freelist);
	UT_RUN(test_queue_begin_requires_normalized_n_snapshot);
	UT_RUN(test_queue_contract_exposes_prepare_only_begin_api);
	UT_RUN(test_queue_contract_exposes_opaque_retained_revoke_api);
	UT_RUN(test_queue_n_source_refresh_is_exact_and_publishes_only_complete_image);
	UT_RUN(test_revoke_finish_mode_rejects_pinned_vm_fsm_and_retains_main);
	UT_RUN(test_queue_revoke_retains_main_but_drops_unpinned_vm_fsm);
	UT_RUN(test_retained_image_release_and_writeback_gates_are_exact);
	UT_RUN(test_retained_drain_retags_invalid_only_after_exact_token_release);
	UT_RUN(test_queue_s_release_finish_is_header_exact_and_returns_fresh_n);
	UT_RUN(test_lockbuffer_pcm_x_holder_ledger_brackets_both_content_acquires);
	UT_RUN(test_bufmgr_pcm_x_holder_ledger_is_bounded_and_uses_private_identity);
	UT_RUN(test_unlockbuffers_exceptionally_detaches_released_pcm_x_holders);
	UT_RUN(test_bufmgr_pcm_x_holder_gate_retry_is_bounded_outside_content_lock);
	UT_RUN(test_bufmgr_pcm_x_holder_reuse_and_deferred_failure_are_fail_closed);
	UT_RUN(test_queue_holder_snapshot_by_tag_is_mapping_and_header_exact);
	UT_RUN(test_queue_passive_pinned_s_release_serializes_bytes_and_ownership);
	UT_RUN(test_current_image_shape_accepts_monotone_xcur_after_x_to_s_yield);
	UT_RUN(test_conditional_lock_preserves_native_off_and_enforces_tracked_x);
	UT_RUN(test_queue_installed_image_publication_is_exact_and_content_locked);
	UT_RUN(test_queue_self_source_handoff_is_single_lifecycle_and_readonly_drain);
	UT_RUN(test_queue_passive_n_mirror_is_never_gcs_ship_authority);
	UT_RUN(test_queue_writer_grant_snapshot_is_claim_and_generation_exact);
	UT_RUN(test_lockbuffer_pcm_x_writer_ledger_is_distinct_and_brackets_content_authority);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
