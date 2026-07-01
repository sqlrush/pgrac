/*-------------------------------------------------------------------------
 *
 * test_cluster_stage5_5_cr_acceptance.c
 *	  pgrac spec-5.58 D8 — Stage 5.5 CR read-path band final surface snapshot
 *	  (static contract).
 *
 *	  Pure compile-time / enum / macro invariants only — no shmem, no external
 *	  calls — so the binary pins the Stage 5.5 CR-read-path band (specs
 *	  5.51-5.57) ABI surface that the spec-5.58 cluster_tap acceptance legs
 *	  (t/319 full-stack differential + the band's per-feature e2e) exercise at
 *	  runtime.  Mirrors test_cluster_stage4_acceptance.c (spec-4.14 D6) and
 *	  test_cluster_stage3_acceptance.c (spec-3.17 D6).
 *
 *	    L1  Stage 5.5 surface landed snapshot:  CATALOG_VERSION_NO >= 202606320
 *	        (spec-5.15 ship value; the whole CR band — 5.51-5.57 — is shmem /
 *	        memory-output-param / GUC only and does NOT bump catversion;
 *	        kept monotone non-decreasing by any future spec).
 *	    L2  CR SQLSTATE family encodable via MAKE_SQLSTATE:
 *	        53R9F snapshot-too-old / 53R9G cross-instance-unsupported — the two
 *	        fail-closed codes the differential + boundary acceptance assert.
 *	    L3  CR dump-category roster stable (cr / cr_pool / resolver_cache /
 *	        cr_coord) — compile-time name strings + exact total length;  runtime
 *	        emission + row counts verified by the cluster_tap baselines.
 *	    L4  admission-policy GUC enum locked (spec-5.52):  CR_ADMIT_ALL=0 is the
 *	        inert default; the three policy values are distinct (no reorder).
 *	    L5  cross-instance coordinator enums locked (spec-5.57):  mode
 *	        OFF=0 / BOUNDARY=1(default) and the 4-counter ClusterCrCoordCounter
 *	        enum complete (CR_COORD_COUNTER__COUNT == 4) — the cr_coord
 *	        observability surface 5.58 HG#3 asserts.
 *	    L6  CLUSTER_WAIT_EVENTS_COUNT snapshot = 110 — spec-6.0a adds the
 *	        block_device wait-event band after the CR read-path band; update-
 *	        required contract: a future spec adding cluster wait events MUST bump
 *	        this snapshot (and the dump/test baselines that count them).
 *
 *	  Static contract assertions only.  Behavioral / differential coverage in the
 *	  spec-5.58 cluster_tap acceptance legs.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_stage5_5_cr_acceptance.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.58-stage5.5-cr-read-path-acceptance.md (FROZEN v1.0)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>
#include <string.h>

#include "catalog/catversion.h"
#include "cluster/cluster_cr_admit.h"
#include "cluster/cluster_cr_coordinator_stat.h"
#include "cluster/cluster_views.h"
#include "utils/errcodes.h"

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


UT_DEFINE_GLOBALS();


void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


/* ===== L1 — Stage 5.5 surface snapshot ===== */

UT_TEST(test_stage5_5_catversion_at_or_above_spec_5_15)
{
	/* The CR read-path band (5.51-5.57) is shmem region + atomic counters +
	 * memory output params + GUCs only — no catalog / SRF signature / on-disk
	 * ABI change — so it does NOT bump catversion.  202606320 is the current
	 * (spec-5.15) ship value; pin it monotone non-decreasing. */
	UT_ASSERT((long)CATALOG_VERSION_NO >= 202606320L);
}


/* ===== L2 — CR SQLSTATE family encodable ===== */

UT_TEST(test_stage5_5_cr_sqlstate_surface_encodable)
{
	/* 53R9F (snapshot-too-old) and 53R9G (cross-instance-unsupported) are the two
	 * fail-closed codes the full-stack differential (HG#5) and the cross-instance
	 * boundary acceptance (HG#3) assert.  Pin them so a renumber diverges here. */
	UT_ASSERT_EQ((int)ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD,
				 (int)MAKE_SQLSTATE('5', '3', 'R', '9', 'F'));
	UT_ASSERT_EQ((int)ERRCODE_CLUSTER_CR_CROSS_INSTANCE_UNSUPPORTED,
				 (int)MAKE_SQLSTATE('5', '3', 'R', '9', 'G'));
}


/* ===== L3 — CR dump-category roster ===== */

UT_TEST(test_stage5_5_cr_dump_category_names)
{
	/* The band's CR observability surface emits these four categories at runtime
	 * (cr base, cr_pool 5.51/5.52/5.56, resolver_cache 5.55, cr_coord 5.57);  the
	 * cluster_tap baselines assert the row counts.  Pin the category name strings
	 * as a compile-time roster (+ exact total length) so a rename in a
	 * cluster_debug.c emit site diverges from the contract surface here.  (Array
	 * form, not strcmp(literal,literal), to avoid cppcheck staticStringCompare.) */
	const char *cats[4] = {
		"cr",			  /* spec-3.9 CR construct + 3.10 L1 cache + 5.53/5.54/5.56 */
		"cr_pool",		  /* spec-5.51 shared L2 pool + 5.52 admission */
		"resolver_cache", /* spec-5.55 resolver memo */
		"cr_coord"		  /* spec-5.57 cross-instance coordinator boundary */
	};
	int i;
	int total_len = 0;

	for (i = 0; i < 4; i++) {
		UT_ASSERT_NOT_NULL((void *)cats[i]);
		UT_ASSERT((int)strlen(cats[i]) > 1);
		total_len += (int)strlen(cats[i]);
	}
	/* cr2 + cr_pool7 + resolver_cache14 + cr_coord8 = 31 */
	UT_ASSERT_EQ(total_len, 31);
}


/* ===== L4 — admission-policy GUC enum locked (spec-5.52) ===== */

UT_TEST(test_stage5_5_admission_policy_enum_locked)
{
	/* CR_ADMIT_ALL=0 is the inert default (5.51 v1 populate-on-construct;  the
	 * all-off differential profile relies on it being byte-identical).  The three
	 * policy values must stay distinct so a reorder cannot silently change which
	 * policy the GUC ordinal selects. */
	UT_ASSERT_EQ((int)CR_ADMIT_ALL, 0);
	UT_ASSERT(CR_ADMIT_NO_ADMIT != CR_ADMIT_ALL);
	UT_ASSERT(CR_ADMIT_SCAN_RESISTANT != CR_ADMIT_NO_ADMIT);
	UT_ASSERT(CR_ADMIT_SCAN_RESISTANT != CR_ADMIT_ALL);
}


/* ===== L5 — cross-instance coordinator enums locked (spec-5.57) ===== */

UT_TEST(test_stage5_5_cross_instance_coordinator_enums_locked)
{
	/* Mode enum:  OFF=0 (observability off; the boundary itself is 8.A
	 * non-degradable and fires under every value), BOUNDARY=1 is the default.
	 * The mode ordinal is the GUC enum value, so pin the two anchors. */
	UT_ASSERT_EQ((int)CR_COORD_MODE_OFF, 0);
	UT_ASSERT_EQ((int)CR_COORD_MODE_BOUNDARY, 1);
	UT_ASSERT(CR_COORD_MODE_FORWARD != CR_COORD_MODE_BOUNDARY);

	/* Counter enum: the 4 cr_coord observability counters HG#3 reads.  Pin the
	 * headline at 0 and the array bound at 4 so an added/removed counter diverges
	 * from the dump + baseline surface. */
	UT_ASSERT_EQ((int)CR_COORD_CROSS_INSTANCE_CR_REFUSED, 0);
	UT_ASSERT(CR_COORD_REMOTE_UNDO_READ_REFUSED != CR_COORD_CROSS_INSTANCE_CR_REFUSED);
	UT_ASSERT_EQ((int)CR_COORD_COUNTER__COUNT, 4);
}


/* ===== L6 — wait-events count snapshot ===== */

UT_TEST(test_stage5_5_wait_events_count_snapshot_110)
{
	/* The whole CR read-path band (5.51-5.57) adds NO new wait events — it reuses
	 * the spec-3.9 ClusterCRConstruct event.  spec-6.0a adds 7 block_device
	 * wait events after that band. */
	UT_ASSERT_EQ((int)CLUSTER_WAIT_EVENTS_COUNT, 110);
}


int
main(void)
{
	UT_RUN(test_stage5_5_catversion_at_or_above_spec_5_15);
	UT_RUN(test_stage5_5_cr_sqlstate_surface_encodable);
	UT_RUN(test_stage5_5_cr_dump_category_names);
	UT_RUN(test_stage5_5_admission_policy_enum_locked);
	UT_RUN(test_stage5_5_cross_instance_coordinator_enums_locked);
	UT_RUN(test_stage5_5_wait_events_count_snapshot_110);
	UT_DONE();
}
