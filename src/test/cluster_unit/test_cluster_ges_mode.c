/*-------------------------------------------------------------------------
 *
 * test_cluster_ges_mode.c
 *	  Compile-time + link-time contract tests for the GES enqueue
 *	  lock-mode encoding layer + frozen 8x8 compatibility matrix.
 *
 *	  Exercises the pure layer of cluster_ges_mode.c standalone (no PG
 *	  backend): the const matrix, ges_modes_compatible, ges_mode_compat_set,
 *	  ges_mode_convert_class, ges_mode_to_dlm / ges_dlm_mode_name,
 *	  ges_mode_pg_name / ges_mode_from_pg_name, and the GES_MODE_COUNT
 *	  invariant.  The live cross-check against PG DoLockModesConflict runs
 *	  backend-side (TAP t/<NNN>_ges_mode_contract.pl), not here.
 *
 *	  The expected_compat[] grid below is an independent hand transcription
 *	  of PostgreSQL's LockConflicts[] table; U2 compares the module's frozen
 *	  matrix against it so a transcription error in either copy is caught.
 *
 *	  Spec: spec-5.1a-ges-mode-encoding-compat-matrix.md
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_ges_mode.c
 *
 * NOTES
 *	  Pure layer only; links cluster_ges_mode.o with no PG-backend stubs.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "cluster/cluster_ges_mode.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

/*
 * Independent transcription of PG LockConflicts[] expressed as a
 * compatibility grid: expected_compat[held][wanted] == 1 when compatible.
 * Index 0 (NoLock) is unused.
 */
static const int expected_compat[GES_MODE_COUNT + 1][GES_MODE_COUNT + 1] = {
	/*               w: -  AS RS RE SUE S  SRE E  AE */
	/* unused 0 */ {0, 0, 0, 0, 0, 0, 0, 0, 0},
	/* AS  1   */ {0, 1, 1, 1, 1, 1, 1, 1, 0},
	/* RS  2   */ {0, 1, 1, 1, 1, 1, 1, 0, 0},
	/* RE  3   */ {0, 1, 1, 1, 1, 0, 0, 0, 0},
	/* SUE 4   */ {0, 1, 1, 1, 0, 0, 0, 0, 0},
	/* S   5   */ {0, 1, 1, 0, 0, 1, 0, 0, 0},
	/* SRE 6   */ {0, 1, 1, 0, 0, 0, 0, 0, 0},
	/* E   7   */ {0, 1, 0, 0, 0, 0, 0, 0, 0},
	/* AE  8   */ {0, 0, 0, 0, 0, 0, 0, 0, 0},
};

/* Compute compat-set (bit i = mode i compatible) from expected_compat. */
static LOCKMASK
expected_compat_set(int m)
{
	LOCKMASK mask = 0;
	int			w;

	for (w = GES_MODE_FIRST; w <= GES_MODE_LAST; w++)
		if (expected_compat[m][w])
			mask |= GES_MODE_BIT(w);
	return mask;
}

/* Independent convert classification from compat-set containment. */
static ClusterGesConvertClass
expected_convert_class(int from, int to)
{
	LOCKMASK	sf = expected_compat_set(from);
	LOCKMASK	st = expected_compat_set(to);

	if (from == to)
		return GES_CONVERT_SAME;
	if (sf == st)
		return GES_CONVERT_LATERAL; /* equal-strength distinct modes */
	if ((st & sf) == st)			/* st subset of sf */
		return GES_CONVERT_UPGRADE;
	if ((sf & st) == sf)			/* sf subset of st */
		return GES_CONVERT_DOWNGRADE;
	return GES_CONVERT_LATERAL;
}

/* U1 -- mode space is exactly the 8 PG lock modes. */
UT_TEST(test_mode_count_is_eight)
{
	UT_ASSERT_EQ(GES_MODE_COUNT, 8);
	UT_ASSERT_EQ(GES_MODE_FIRST, 1);
	UT_ASSERT_EQ(GES_MODE_LAST, 8);
}

/* U2 -- frozen matrix matches the independent expected grid (all 64). */
UT_TEST(test_matrix_matches_expected_grid)
{
	int			h,
				w;

	for (h = GES_MODE_FIRST; h <= GES_MODE_LAST; h++)
		for (w = GES_MODE_FIRST; w <= GES_MODE_LAST; w++)
		{
			UT_ASSERT_EQ((int) ges_mode_compat_matrix[h][w], expected_compat[h][w]);
			UT_ASSERT_EQ((int) ges_modes_compatible(h, w), expected_compat[h][w]);
		}
}

/* U3 -- matrix is symmetric. */
UT_TEST(test_matrix_symmetric)
{
	int			h,
				w;

	for (h = GES_MODE_FIRST; h <= GES_MODE_LAST; h++)
		for (w = GES_MODE_FIRST; w <= GES_MODE_LAST; w++)
			UT_ASSERT_EQ((int) ges_mode_compat_matrix[h][w],
						 (int) ges_mode_compat_matrix[w][h]);
}

/* U4 -- diagonal self-conflict: SUE/SRE/E/AE conflict with self; others don't. */
UT_TEST(test_diagonal_self_conflict)
{
	UT_ASSERT(ges_modes_compatible(AccessShareLock, AccessShareLock));
	UT_ASSERT(ges_modes_compatible(RowShareLock, RowShareLock));
	UT_ASSERT(ges_modes_compatible(RowExclusiveLock, RowExclusiveLock));
	UT_ASSERT(ges_modes_compatible(ShareLock, ShareLock));
	UT_ASSERT(!ges_modes_compatible(ShareUpdateExclusiveLock, ShareUpdateExclusiveLock));
	UT_ASSERT(!ges_modes_compatible(ShareRowExclusiveLock, ShareRowExclusiveLock));
	UT_ASSERT(!ges_modes_compatible(ExclusiveLock, ExclusiveLock));
	UT_ASSERT(!ges_modes_compatible(AccessExclusiveLock, AccessExclusiveLock));
}

/* U5 -- compat_set is derived consistently with the matrix. */
UT_TEST(test_compat_set_derivation)
{
	int			m;

	for (m = GES_MODE_FIRST; m <= GES_MODE_LAST; m++)
		UT_ASSERT_EQ((int) ges_mode_compat_set(m), (int) expected_compat_set(m));
}

/* U6 -- convert classification over all 64 pairs + symmetry invariants. */
UT_TEST(test_convert_class_all_pairs)
{
	int			from,
				to;

	for (from = GES_MODE_FIRST; from <= GES_MODE_LAST; from++)
		for (to = GES_MODE_FIRST; to <= GES_MODE_LAST; to++)
		{
			ClusterGesConvertClass got = ges_mode_convert_class(from, to);

			UT_ASSERT_EQ((int) got, (int) expected_convert_class(from, to));

			if (from == to)
				UT_ASSERT_EQ((int) got, (int) GES_CONVERT_SAME);

			/* UPGRADE(a,b) iff DOWNGRADE(b,a); LATERAL is symmetric. */
			if (got == GES_CONVERT_UPGRADE)
				UT_ASSERT_EQ((int) ges_mode_convert_class(to, from),
							 (int) GES_CONVERT_DOWNGRADE);
			if (got == GES_CONVERT_LATERAL && from != to)
				UT_ASSERT_EQ((int) ges_mode_convert_class(to, from),
							 (int) GES_CONVERT_LATERAL);
		}

	/* The canonical incomparable pair (Share vs RowExclusive) is LATERAL. */
	UT_ASSERT_EQ((int) ges_mode_convert_class(ShareLock, RowExclusiveLock),
				 (int) GES_CONVERT_LATERAL);
}

/* U7 -- Oracle DLM alias mapping + non-empty names. */
UT_TEST(test_dlm_alias_mapping)
{
	UT_ASSERT_EQ((int) ges_mode_to_dlm(AccessShareLock), (int) GES_DLM_CR);
	UT_ASSERT_EQ((int) ges_mode_to_dlm(RowShareLock), (int) GES_DLM_CR);
	UT_ASSERT_EQ((int) ges_mode_to_dlm(RowExclusiveLock), (int) GES_DLM_CW);
	UT_ASSERT_EQ((int) ges_mode_to_dlm(ShareUpdateExclusiveLock), (int) GES_DLM_CW);
	UT_ASSERT_EQ((int) ges_mode_to_dlm(ShareLock), (int) GES_DLM_PR);
	UT_ASSERT_EQ((int) ges_mode_to_dlm(ShareRowExclusiveLock), (int) GES_DLM_PW);
	UT_ASSERT_EQ((int) ges_mode_to_dlm(ExclusiveLock), (int) GES_DLM_PW);
	UT_ASSERT_EQ((int) ges_mode_to_dlm(AccessExclusiveLock), (int) GES_DLM_EX);

	UT_ASSERT_STR_EQ(ges_dlm_mode_name(GES_DLM_NL), "NL");
	UT_ASSERT_STR_EQ(ges_dlm_mode_name(GES_DLM_CR), "CR");
	UT_ASSERT_STR_EQ(ges_dlm_mode_name(GES_DLM_EX), "EX");
}

/* U8 -- out-of-range fails closed (conservative), never crashes. */
UT_TEST(test_invalid_mode_fails_closed)
{
	UT_ASSERT(!ges_mode_is_valid(0));
	UT_ASSERT(!ges_mode_is_valid(GES_MODE_LAST + 1));
	UT_ASSERT(ges_mode_is_valid(GES_MODE_FIRST));
	UT_ASSERT(ges_mode_is_valid(GES_MODE_LAST));

	/* conservative answers: incompatible / empty / lateral */
	UT_ASSERT(!ges_modes_compatible(0, AccessShareLock));
	UT_ASSERT(!ges_modes_compatible(AccessShareLock, GES_MODE_LAST + 1));
	UT_ASSERT_EQ((int) ges_mode_compat_set(0), 0);
	UT_ASSERT_EQ((int) ges_mode_convert_class(0, AccessShareLock),
				 (int) GES_CONVERT_LATERAL);
}

/* U9 -- canonical PG name parser (case-insensitive); unknown/DLM -> 0. */
UT_TEST(test_pg_name_parser)
{
	UT_ASSERT_EQ((int) ges_mode_from_pg_name("AccessShareLock"), AccessShareLock);
	UT_ASSERT_EQ((int) ges_mode_from_pg_name("accessexclusivelock"), AccessExclusiveLock);
	UT_ASSERT_EQ((int) ges_mode_from_pg_name("ShareLock"), ShareLock);

	UT_ASSERT_STR_EQ(ges_mode_pg_name(AccessShareLock), "AccessShareLock");
	UT_ASSERT_STR_EQ(ges_mode_pg_name(AccessExclusiveLock), "AccessExclusiveLock");

	/* DLM alias and unknown names are not accepted as input. */
	UT_ASSERT_EQ((int) ges_mode_from_pg_name("CR"), 0);
	UT_ASSERT_EQ((int) ges_mode_from_pg_name("EX"), 0);
	UT_ASSERT_EQ((int) ges_mode_from_pg_name("bogus"), 0);
	UT_ASSERT_EQ((int) ges_mode_from_pg_name(""), 0);
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(test_mode_count_is_eight);
	UT_RUN(test_matrix_matches_expected_grid);
	UT_RUN(test_matrix_symmetric);
	UT_RUN(test_diagonal_self_conflict);
	UT_RUN(test_compat_set_derivation);
	UT_RUN(test_convert_class_all_pairs);
	UT_RUN(test_dlm_alias_mapping);
	UT_RUN(test_invalid_mode_fails_closed);
	UT_RUN(test_pg_name_parser);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
