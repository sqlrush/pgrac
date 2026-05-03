/*-------------------------------------------------------------------------
 *
 * test_cluster_startup_phase.c
 *	  Compile-time / link-level invariants for the cluster startup
 *	  phase machinery shipped at stage 1.10.
 *
 *	  Locks:
 *	    - ClusterStartupPhase enum values (PRE_INIT=0, 0_BASE=1, ...,
 *	      SHUTDOWN=7) are frozen.  Spec-1.10 §1.5 HC2 SSOT relies on
 *	      these specific integer values.
 *	    - CLUSTER_PHASE_LAST == CLUSTER_PHASE_SHUTDOWN (the array
 *	      bounds in cluster_phase_start_times depend on this).
 *	    - CLUSTER_PHASE_HISTORY_RING_SIZE == 8 (HC5 fixed-size ring;
 *	      avoids unbounded string accumulation under reconfig phase
 *	      reentry in Stage 6).
 *	    - cluster_startup_phase_to_string() returns non-null for every
 *	      enum value and "(unknown)" for out-of-range values.
 *	    - Public symbols cluster_advance_phase / cluster_run_startup_
 *	      sequence / cluster_run_shutdown_sequence resolve at link
 *	      time.
 *
 *	  Behavior-level tests (transition rejection of backward / skip,
 *	  Postmaster-once Assert(!IsUnderPostmaster) firing) live in TAP
 *	  t/060_postmaster_phases.pl because they depend on postmaster
 *	  startup orchestration that is not reproducible at unit test level.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_startup_phase.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Linking cluster_startup_phase.o
 *	  standalone pulls in references to ereport, GetCurrentTimestamp,
 *	  timestamptz_to_str, the cluster_phase legacy mirror, and the
 *	  cluster_inject framework; the test stubs every one of those
 *	  because we only take addresses + read enum constants.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_startup_phase.h"

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


/* ----------
 * Stubs needed to link cluster_startup_phase.o standalone.  None of
 * these paths run during the unit test -- we only take addresses and
 * exercise pure-function APIs (cluster_startup_phase_to_string).
 * ----------
 */

#include "cluster/cluster_inject.h"
int cluster_injection_armed_count = 0;
char *cluster_injection_points = NULL;
void
cluster_injection_run(const char *name pg_attribute_unused())
{}

/* miscadmin: HC1 Assert reads IsUnderPostmaster. */
bool IsUnderPostmaster = false;

/* cluster_phase legacy mirror (HC2 derived).  Real backend gets it from
 * cluster_elog.o; the unit test provides a local writable storage so
 * cluster_advance_phase's mirror update has somewhere to land. */
const char *cluster_phase = "pre_init";

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* ereport machinery -- never invoked by the runtime tests below. */
bool
errstart(int e pg_attribute_unused(), const char *d pg_attribute_unused())
{
	return false;
}
bool
errstart_cold(int e pg_attribute_unused(), const char *d pg_attribute_unused())
{
	return false;
}
void
errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		  const char *fn pg_attribute_unused())
{}
int
errcode(int s pg_attribute_unused())
{
	return 0;
}
int
errmsg(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errmsg_internal(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errdetail(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errhint(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
void
elog_start(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		   const char *fn pg_attribute_unused())
{}
void
elog_finish(int e pg_attribute_unused(), const char *f pg_attribute_unused(), ...)
{}
void
pre_format_elog_string(int n pg_attribute_unused(), const char *d pg_attribute_unused())
{}
char *
format_elog_string(const char *f pg_attribute_unused(), ...)
{
	return NULL;
}

/* timestamp.h stubs -- cluster_advance_phase calls GetCurrentTimestamp;
 * the runtime tests below exercise only the pure function lookups so
 * these stubs return zero. */
TimestampTz
GetCurrentTimestamp(void)
{
	return 0;
}
void
TimestampDifference(TimestampTz start_time pg_attribute_unused(),
					TimestampTz stop_time pg_attribute_unused(), long *secs, int *microsecs)
{
	*secs = 0;
	*microsecs = 0;
}
const char *
timestamptz_to_str(TimestampTz dt pg_attribute_unused())
{
	return "(stub)";
}

/* pg_snprintf: cluster_startup_phase.c uses snprintf (macro'd to
 * pg_snprintf in PG).  Forward to libc vsnprintf in unit test. */
#include <stdarg.h>
int
pg_snprintf(char *str, size_t count, const char *fmt, ...)
{
	int n;
	va_list ap;

	va_start(ap, fmt);
	n = vsnprintf(str, count, fmt, ap);
	va_end(ap);
	return n;
}


UT_DEFINE_GLOBALS();


/* ============================================================
 * Compile-time anchors
 * ============================================================ */

UT_TEST(test_phase_enum_values_frozen)
{
	/*
	 * Spec-1.10 §1.5 HC2 SSOT: these specific integer values are
	 * relied upon by pg_cluster_state.phase.phase_enum_value (int4
	 * field exposed to user) and by 030 acceptance §S.  Changing them
	 * is a public-facing breakage.
	 */
	UT_ASSERT_EQ((int)CLUSTER_PHASE_PRE_INIT, 0);
	UT_ASSERT_EQ((int)CLUSTER_PHASE_0_BASE, 1);
	UT_ASSERT_EQ((int)CLUSTER_PHASE_1_CLUSTER, 2);
	UT_ASSERT_EQ((int)CLUSTER_PHASE_2_LOCK, 3);
	UT_ASSERT_EQ((int)CLUSTER_PHASE_3_RECOVERY, 4);
	UT_ASSERT_EQ((int)CLUSTER_PHASE_4_NORMAL, 5);
	UT_ASSERT_EQ((int)CLUSTER_PHASE_RUNNING, 6);
	UT_ASSERT_EQ((int)CLUSTER_PHASE_SHUTDOWN, 7);
}


UT_TEST(test_phase_last_is_shutdown)
{
	/*
	 * Spec-1.10 enum total at Stage 1.10 is 8 values; CLUSTER_PHASE_
	 * LAST is used as the upper bound when sizing per-phase arrays
	 * (e.g. cluster_phase_start_times in cluster_startup_phase.c).
	 */
	UT_ASSERT_EQ((int)CLUSTER_PHASE_LAST, (int)CLUSTER_PHASE_SHUTDOWN);
	UT_ASSERT_EQ((int)CLUSTER_PHASE_LAST, 7);
}


UT_TEST(test_phase_history_ring_size_is_eight)
{
	/*
	 * Spec-1.10 §1.5 HC5: fixed-size ring at 8 entries to bound
	 * pg_cluster_state.phase.phase_history string length and prevent
	 * Stage 6 reconfig phase reentry from causing unbounded growth.
	 */
	UT_ASSERT_EQ((int)CLUSTER_PHASE_HISTORY_RING_SIZE, 8);
}


UT_TEST(test_phase_string_lookup_returns_non_null_for_each_value)
{
	int i;

	for (i = 0; i <= (int)CLUSTER_PHASE_LAST; i++) {
		const char *s = cluster_startup_phase_to_string((ClusterStartupPhase)i);

		UT_ASSERT_NOT_NULL(s);
		/*
		 * Defensive: cppcheck doesn't model UT_ASSERT_NOT_NULL's abort
		 * semantics, so the explicit `s != NULL` keeps the static
		 * analyser happy without relaxing the assertion's meaning.
		 */
		if (s != NULL)
			UT_ASSERT(s[0] != '\0');
	}
}


UT_TEST(test_phase_string_lookup_invalid_returns_unknown)
{
	const char *neg = cluster_startup_phase_to_string((ClusterStartupPhase)-1);
	const char *over
		= cluster_startup_phase_to_string((ClusterStartupPhase)((int)CLUSTER_PHASE_LAST + 1));

	UT_ASSERT_STR_EQ(neg, "(unknown)");
	UT_ASSERT_STR_EQ(over, "(unknown)");
}


/* ============================================================
 * Public symbol linkability
 *
 *	If any of these unresolves at link time, this test binary will
 *	fail to build.  Taking the address (cast to void *) is enough to
 *	pin link-time presence without invoking the body.
 * ============================================================ */

UT_TEST(test_public_symbols_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_startup_phase_to_string);
	UT_ASSERT_NOT_NULL((void *)cluster_current_phase);
	UT_ASSERT_NOT_NULL((void *)cluster_phase_started_at);
	UT_ASSERT_NOT_NULL((void *)cluster_phase_elapsed_seconds);
	UT_ASSERT_NOT_NULL((void *)cluster_phase_history_format);
	UT_ASSERT_NOT_NULL((void *)cluster_advance_phase);
	UT_ASSERT_NOT_NULL((void *)cluster_run_startup_sequence);
	UT_ASSERT_NOT_NULL((void *)cluster_run_shutdown_sequence);
}


/* ============================================================
 * Test runner
 * ============================================================ */

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_phase_enum_values_frozen);
	UT_RUN(test_phase_last_is_shutdown);
	UT_RUN(test_phase_history_ring_size_is_eight);
	UT_RUN(test_phase_string_lookup_returns_non_null_for_each_value);
	UT_RUN(test_phase_string_lookup_invalid_returns_unknown);
	UT_RUN(test_public_symbols_linkable);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
