/*-------------------------------------------------------------------------
 *
 * test_cluster_views.c
 *	  Compile-time invariants for the cluster views framework
 *	  introduced in stage 0.16.
 *
 *	  Stage 0.16 backs pg_stat_cluster_wait_events with the SRF
 *	  cluster_get_wait_events() in cluster_views.c, registered via
 *	  pg_proc.dat.  The actual SRF call requires the PG executor /
 *	  tuplestore machinery which cluster_unit deliberately does not
 *	  link.  This test asserts only the structural pieces a PG-free
 *	  binary can verify:
 *
 *	  - CLUSTER_WAIT_EVENTS_COUNT == 46 (matches spec-0.11 registration).
 *	  - cluster_get_wait_events function symbol resolves at link time.
 *
 *	  Runtime behaviour (the view returns 46 rows with the correct
 *	  type / name values) is validated by cluster_tap t/010_views.pl
 *	  on a real PG instance.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_views.c
 *
 * NOTES
 *	  This is a pgrac-original file.  cluster_views.c references PG
 *	  fmgr / tuplestore / wait_event lookup machinery; the test stubs
 *	  those out locally so the standalone unit binary does not need to
 *	  link the full backend.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_views.h"
#include "cluster/cluster_wait_events.h" /* PG_WAIT_CLUSTER_GES / ADG */
#include "utils/wait_event.h"			 /* WAIT_EVENT_GES_ENQUEUE_ACQUIRE etc. */

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


/* ----------
 * Stubs needed to link cluster_views.o standalone.
 *
 *	cluster_views.c calls PG executor / SRF machinery which would pull
 *	in the entire backend.  This test never invokes
 *	cluster_get_wait_events() (we only take its address); the stubs
 *	below exist purely so the linker can resolve the symbols.
 * ----------
 */
#include "funcapi.h"
#include "utils/builtins.h"

void
InitMaterializedSRF(FunctionCallInfo fcinfo pg_attribute_unused(),
					bits32 flags pg_attribute_unused())
{
	/* Stub: real impl in src/backend/utils/fmgr/funcapi.c */
}

void
tuplestore_putvalues(Tuplestorestate *state pg_attribute_unused(),
					 TupleDesc tdesc pg_attribute_unused(), Datum *values pg_attribute_unused(),
					 bool *isnull pg_attribute_unused())
{
	/* Stub: real impl in src/backend/utils/sort/tuplestore.c */
}

text *
cstring_to_text(const char *s pg_attribute_unused())
{
	/* Stub: real impl in src/backend/utils/adt/varlena.c */
	return NULL;
}

const char *
pgstat_get_wait_event(uint32 wait_event_info pg_attribute_unused())
{
	/* Stub: real impl in src/backend/utils/activity/wait_event.c */
	return "";
}

const char *
pgstat_get_wait_event_type(uint32 wait_event_info pg_attribute_unused())
{
	/* Stub: real impl in src/backend/utils/activity/wait_event.c */
	return "";
}


UT_DEFINE_GLOBALS();


UT_TEST(test_cluster_wait_events_count_is_46)
{
	/*
	 * Anchored against spec-0.11's registration roster.  If a future
	 * subsystem spec adds new cluster wait events, both the enum in
	 * wait_event.h and CLUSTER_WAIT_EVENTS_COUNT must move together.
	 */
	UT_ASSERT_EQ(CLUSTER_WAIT_EVENTS_COUNT, 46);
}


UT_TEST(test_srf_symbol_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_get_wait_events);
}


UT_TEST(test_first_event_is_ges_enqueue_acquire)
{
	/*
	 * Spot-check that the static array in cluster_views.c starts with
	 * the GES events (matching the order the registration policy in
	 * docs/cluster-views-impl-design.md inherits from spec-0.11).  We
	 * cannot peek into the static array directly from the unit binary,
	 * so this test instead verifies the WaitEventCluster anchor value.
	 */
	UT_ASSERT_EQ((uint32)WAIT_EVENT_GES_ENQUEUE_ACQUIRE, PG_WAIT_CLUSTER_GES);
}


UT_TEST(test_last_event_is_adg_scn_sync_wait)
{
	/*
	 * Symmetric anchor for the tail of the array: WAIT_EVENT_ADG_SCN_SYNC_WAIT
	 * is the last enum value, sitting in the ADG class.
	 */
	UT_ASSERT_EQ(((uint32)WAIT_EVENT_ADG_SCN_SYNC_WAIT) & 0xFF000000U, PG_WAIT_CLUSTER_ADG);
}


int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_cluster_wait_events_count_is_46);
	UT_RUN(test_srf_symbol_linkable);
	UT_RUN(test_first_event_is_ges_enqueue_acquire);
	UT_RUN(test_last_event_is_adg_scn_sync_wait);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
