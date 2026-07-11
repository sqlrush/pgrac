/*-------------------------------------------------------------------------
 *
 * test_cluster_xnode_profile.c
 *	  Unit tests for the cross-node profiling buckets (spec-5.59 D10).
 *
 *	  Links cluster_xnode_profile.o standalone with a heap-allocated
 *	  fake shmem struct.  INSTR_TIME_SET_CURRENT is overridden with a
 *	  counting fake clock BEFORE cluster_xnode_profile.h is included, so
 *	  the header's inline begin/end helpers instantiate against the fake
 *	  in this translation unit: U4 can assert the off path performs ZERO
 *	  clock reads (not just zero accumulation), and U2 gets fully
 *	  deterministic timing.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_xnode_profile.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.59-two-node-crossnode-perf-profile.md (D10, §4.1)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "portability/instr_time.h"

/* ----------
 * Fake clock: count every INSTR_TIME_SET_CURRENT and serve a settable
 * nanosecond value.  Must be defined before cluster_xnode_profile.h so
 * the inline probe helpers in this TU bind to the fake.
 * ----------
 */
static int ut_clock_calls = 0;
static int64 ut_fake_now_ns = 0;

#undef INSTR_TIME_SET_CURRENT
#define INSTR_TIME_SET_CURRENT(t) (ut_clock_calls++, (t).ticks = ut_fake_now_ns)

#include "cluster/cluster_xnode_profile.h"
#include "storage/buf_internals.h"

/*
 * postgres.h transitively pulls in port.h which redirects printf etc.
 * Standalone unit-test binaries do not link libpgport, so undo the
 * redirection before pulling in unit_test.h.
 */
#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"

#include <stdlib.h>
#include <string.h>

UT_DEFINE_GLOBALS();

/* ----------
 * Minimal PG stubs so cluster_xnode_profile.o links standalone.
 *
 *	The GUC variable lives in cluster_guc.c (not linked here); shmem
 *	registration/init are never called by these tests -- the stubs only
 *	satisfy the linker.  Real registration is exercised by cluster_tap
 *	t/020_shmem_registry.pl.
 * ----------
 */
bool cluster_xnode_profile_enabled = false;

extern void *ShmemInitStruct(const char *name, Size size, bool *foundPtr);
struct ClusterShmemRegion;
extern void cluster_shmem_register_region(const struct ClusterShmemRegion *region);
extern void ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber);

void *
ShmemInitStruct(const char *name, Size size, bool *foundPtr)
{
	(void)name;
	(void)size;
	*foundPtr = false;
	abort(); /* never reached in unit context */
}

void
cluster_shmem_register_region(const struct ClusterShmemRegion *region)
{
	(void)region;
}

/* Assert()/ExceptionalCondition backstop for cassert builds. */
void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	fprintf(stderr, "Assert failed: %s (%s:%d)\n", conditionName, fileName, lineNumber);
	abort();
}

/* ----------
 * Fake shmem struct helpers.
 * ----------
 */
static ClusterXnodeProfileShared ut_shared;

static void
ut_attach_fresh_shared(void)
{
	memset(&ut_shared, 0, sizeof(ut_shared));
	ClusterXnodeProfileCtl = &ut_shared;
}

static uint64
ut_bucket_nanos(ClusterXnodeBucket b)
{
	return pg_atomic_read_u64(&ut_shared.bucket[b].total_nanos);
}

static uint64
ut_bucket_events(ClusterXnodeBucket b)
{
	return pg_atomic_read_u64(&ut_shared.bucket[b].n_events);
}

/* ----------
 * U1 — bucket enum completeness: every bucket has a unique non-NULL
 * name; out-of-range returns NULL; the enum has the spec'd 23 buckets
 * (spec-5.59) + 5 commit-decomposition buckets (spec-7.4 D0) = 28.
 * ----------
 */
UT_TEST(test_u1_bucket_enum_complete)
{
	int i;
	int j;

	UT_ASSERT_EQ(CLXP_NBUCKETS, 28);
	for (i = 0; i < CLXP_NBUCKETS; i++) {
		const char *name = cluster_xp_bucket_name((ClusterXnodeBucket)i);

		UT_ASSERT_NOT_NULL((void *)name);
		UT_ASSERT(strlen(name) > 0);
		for (j = 0; j < i; j++)
			UT_ASSERT(strcmp(name, cluster_xp_bucket_name((ClusterXnodeBucket)j)) != 0);
	}
	UT_ASSERT_NULL((void *)cluster_xp_bucket_name((ClusterXnodeBucket)CLXP_NBUCKETS));
	UT_ASSERT_NULL((void *)cluster_xp_bucket_name((ClusterXnodeBucket)-1));
}

/* ----------
 * U2 — accumulators are monotonic and deterministic under the fake
 * clock: two begin/end intervals of 4000ns and 6000ns accumulate.
 * ----------
 */
UT_TEST(test_u2_accum_monotonic)
{
	ClusterXpScope s;

	ut_attach_fresh_shared();
	cluster_xnode_profile_enabled = true;

	ut_fake_now_ns = 1000;
	cluster_xp_begin(&s, CLXP_W_GCS_X_REQUEST);
	ut_fake_now_ns = 5000;
	cluster_xp_end(&s);
	UT_ASSERT_EQ(ut_bucket_nanos(CLXP_W_GCS_X_REQUEST), 4000);
	UT_ASSERT_EQ(ut_bucket_events(CLXP_W_GCS_X_REQUEST), 1);

	ut_fake_now_ns = 10000;
	cluster_xp_begin(&s, CLXP_W_GCS_X_REQUEST);
	ut_fake_now_ns = 16000;
	cluster_xp_end(&s);
	UT_ASSERT_EQ(ut_bucket_nanos(CLXP_W_GCS_X_REQUEST), 10000);
	UT_ASSERT_EQ(ut_bucket_events(CLXP_W_GCS_X_REQUEST), 2);

	/* abort discards the sample */
	ut_fake_now_ns = 20000;
	cluster_xp_begin(&s, CLXP_W_GCS_X_REQUEST);
	cluster_xp_abort(&s);
	cluster_xp_end(&s); /* inactive: no-op */
	UT_ASSERT_EQ(ut_bucket_events(CLXP_W_GCS_X_REQUEST), 2);

	cluster_xnode_profile_enabled = false;
}

/* ----------
 * U3 — reset zeroes every accumulator and probe counter and bumps
 * reset_generation (which itself is never zeroed).
 * ----------
 */
UT_TEST(test_u3_reset)
{
	ClusterXpScope s;

	ut_attach_fresh_shared();
	cluster_xnode_profile_enabled = true;

	ut_fake_now_ns = 0;
	cluster_xp_begin(&s, CLXP_R_CR_CONSTRUCT);
	ut_fake_now_ns = 777;
	cluster_xp_end(&s);
	cluster_xp_note_read(true);
	cluster_xp_note_read(false);
	cluster_xp_note_hw_extend(true);
	cluster_xp_note_hw_extend(false);
	cluster_xp_count(CLXP_I_RIGHTMOST_LEAF_PING);

	UT_ASSERT(ut_bucket_nanos(CLXP_R_CR_CONSTRUCT) > 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_shared.read_reship_count), 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_shared.read_sholder_hit_count), 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_shared.hw_extend_remote_count), 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_shared.hw_extend_local_count), 1);
	UT_ASSERT_EQ(ut_bucket_events(CLXP_I_RIGHTMOST_LEAF_PING), 1);

	cluster_xp_reset();

	UT_ASSERT_EQ(ut_bucket_nanos(CLXP_R_CR_CONSTRUCT), 0);
	UT_ASSERT_EQ(ut_bucket_events(CLXP_R_CR_CONSTRUCT), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_shared.read_reship_count), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_shared.read_sholder_hit_count), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_shared.hw_extend_remote_count), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_shared.hw_extend_local_count), 0);
	UT_ASSERT_EQ(ut_bucket_events(CLXP_I_RIGHTMOST_LEAF_PING), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_shared.reset_generation), 1);

	cluster_xp_reset();
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_shared.reset_generation), 2);

	cluster_xnode_profile_enabled = false;
}

/* ----------
 * U4 — off path performs ZERO clock reads and zero accumulation: with
 * the GUC off, 100 begin/end cycles plus every probe API never touch
 * the fake clock or the shared struct.
 * ----------
 */
UT_TEST(test_u4_off_path_zero_syscall)
{
	ClusterXpScope s;
	int i;

	ut_attach_fresh_shared();
	cluster_xnode_profile_enabled = false;
	ut_clock_calls = 0;

	for (i = 0; i < 100; i++) {
		cluster_xp_begin(&s, CLXP_W_GES_ENQUEUE);
		cluster_xp_end(&s);
	}
	cluster_xp_note_read(true);
	cluster_xp_note_hw_extend(true);
	cluster_xp_count(CLXP_I_RIGHTMOST_LEAF_PING);

	UT_ASSERT_EQ(ut_clock_calls, 0);
	for (i = 0; i < CLXP_NBUCKETS; i++) {
		UT_ASSERT_EQ(ut_bucket_nanos((ClusterXnodeBucket)i), 0);
		UT_ASSERT_EQ(ut_bucket_events((ClusterXnodeBucket)i), 0);
	}
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_shared.read_reship_count), 0);

	/* on path DOES read the clock (sanity that the fake is wired) */
	cluster_xnode_profile_enabled = true;
	cluster_xp_begin(&s, CLXP_W_GES_ENQUEUE);
	cluster_xp_end(&s);
	UT_ASSERT_EQ(ut_clock_calls, 2);
	cluster_xnode_profile_enabled = false;
}

/* ----------
 * U5 — dump key surface: 23 buckets x {total_nanos, n_events} plus the
 * 5 probe keys (reset_generation, read probe x2, HW locality x2) = 51
 * keys.  The SRF emission itself is covered end-to-end by cluster_tap
 * (t/017 category list + t/334 legs); the unit level pins the formula
 * and the name table the emission iterates.
 * ----------
 */
UT_TEST(test_u5_dump_key_surface)
{
	UT_ASSERT_EQ(CLUSTER_XP_N_PROBE_KEYS, 5);
	UT_ASSERT_EQ(CLXP_NBUCKETS * 2 + CLUSTER_XP_N_PROBE_KEYS, 61);
}

/* ----------
 * U6 — shmem sizing matches the struct (MAXALIGN'd).
 * ----------
 */
UT_TEST(test_u6_shmem_size)
{
	UT_ASSERT_EQ(cluster_xnode_profile_shmem_size(), MAXALIGN(sizeof(ClusterXnodeProfileShared)));
}

/* ----------
 * U7 — relkind hint: set/match/miss semantics, and the setter is
 * GUC-gated (off invalidates instead of recording).
 * ----------
 */
UT_TEST(test_u7_relkind_hint)
{
	BufferTag tag_a;
	BufferTag tag_b;

	memset(&tag_a, 0, sizeof(tag_a));
	memset(&tag_b, 0, sizeof(tag_b));
	tag_a.blockNum = 7;
	tag_b.blockNum = 8;

	cluster_xnode_profile_enabled = true;
	cluster_xp_relkind_hint_set((const struct buftag *)&tag_a, true);
	UT_ASSERT(cluster_xp_relkind_hint_is_index_for((const struct buftag *)&tag_a));
	UT_ASSERT(!cluster_xp_relkind_hint_is_index_for((const struct buftag *)&tag_b));

	/* heap hint: valid but not index */
	cluster_xp_relkind_hint_set((const struct buftag *)&tag_a, false);
	UT_ASSERT(!cluster_xp_relkind_hint_is_index_for((const struct buftag *)&tag_a));

	/* GUC off: setter invalidates */
	cluster_xnode_profile_enabled = false;
	cluster_xp_relkind_hint_set((const struct buftag *)&tag_a, true);
	UT_ASSERT(!cluster_xp_relkind_hint_is_index_for((const struct buftag *)&tag_a));
}

/* ----------
 * U8 — NULL-shmem safety: with no attached region every probe is a
 * no-op and begin marks the scope inactive.
 * ----------
 */
UT_TEST(test_u8_null_shmem_safe)
{
	ClusterXpScope s;

	ClusterXnodeProfileCtl = NULL;
	cluster_xnode_profile_enabled = true;
	ut_clock_calls = 0;

	cluster_xp_begin(&s, CLXP_C_SCN_COMMIT_ADVANCE);
	UT_ASSERT(!s.active);
	cluster_xp_end(&s);
	cluster_xp_note_read(true);
	cluster_xp_note_hw_extend(false);
	cluster_xp_count(CLXP_C_SCN_COMMIT_ADVANCE);
	cluster_xp_reset();

	UT_ASSERT_EQ(ut_clock_calls, 0);
	cluster_xnode_profile_enabled = false;
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_u1_bucket_enum_complete);
	UT_RUN(test_u2_accum_monotonic);
	UT_RUN(test_u3_reset);
	UT_RUN(test_u4_off_path_zero_syscall);
	UT_RUN(test_u5_dump_key_surface);
	UT_RUN(test_u6_shmem_size);
	UT_RUN(test_u7_relkind_hint);
	UT_RUN(test_u8_null_shmem_safe);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
