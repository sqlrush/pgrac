/*-------------------------------------------------------------------------
 *
 * test_cluster_gviews.c
 *	  Compile-time / link-level invariants for the cluster global
 *	  views framework introduced in stage 0.17.
 *
 *	  Stage 0.17 ships ONE global view: pg_stat_gcluster_wait_events,
 *	  backed by the cluster_get_gcluster_wait_events SRF (OID 8899)
 *	  declared in pg_proc.dat and implemented in cluster_views.c.  The
 *	  SRF is a placeholder for the future cross-node RPC fan-out
 *	  (Stage 6+ AD-007); at 0.17 it returns one row per cluster wait
 *	  event for the local node only.
 *
 *	  Runtime SQL behavior (registered wait events × 1 node, column
 *	  structure, value spot-checks) is validated by cluster_tap
 *	  t/011_gviews.pl on a real PG instance.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_gviews.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Reuses the test_cluster_views.c
 *	  stub pattern -- cluster_views.o references PG SRF / fmgr machinery
 *	  that we stub locally, plus cluster_node_id (provided by
 *	  cluster_guc.o in real builds; stubbed here as a plain int).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_guc.h"
#include "cluster/cluster_mrp.h"
#include "cluster/cluster_views.h"

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
 * Stubs needed to link cluster_views.o standalone.  Mirror the set in
 * test_cluster_views.c (stage 0.16); only the cluster_node_id stub is
 * new for stage 0.17.
 * ----------
 */
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/wait_event.h" /* prototypes for pgstat_get_wait_event* */

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

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

/*
 * cluster_node_id is defined in cluster_guc.o in real backend builds.
 * The unit test binary does not link cluster_guc.o, so provide a local
 * definition with the same default value as the GUC.
 */
int cluster_node_id = -1;
int cluster_dg_role = CLUSTER_DG_ROLE_PRIMARY;
int cluster_dg_mode = CLUSTER_DG_MODE_ASYNC;
bool cluster_enable_adg = false;

ClusterMrpSharedState *
cluster_mrp_shared_state(void)
{
	return NULL;
}

const char *
cluster_mrp_state_to_string(ClusterMrpState state pg_attribute_unused())
{
	return "disabled";
}

/*
 * Stage 0.28 stubs: cluster_views.c::cluster_get_stat_nodes references
 * ClusterShmem + cluster_conf_lookup_node / cluster_conf_role_to_string.
 * Provide minimal stubs so the linker resolves the SRF symbol address.
 */
#include "cluster/cluster_conf.h"
#include "cluster/cluster_shmem.h"

ClusterShmemCtl *ClusterShmem = NULL;

const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node_id pg_attribute_unused())
{
	return NULL;
}

const char *
cluster_conf_role_to_string(ClusterNodeRole role pg_attribute_unused())
{
	return "unknown";
}

/* Stage 0.30 sweep: cluster_views.c gained CLUSTER_INJECTION_POINT. */
int cluster_injection_armed_count = 0;

void
cluster_injection_run(const char *name pg_attribute_unused())
{}

/* Stage 1.3: cluster_views.c::cluster_shmem_dump_regions calls iter API. */
#include "cluster/cluster_shmem.h"

bool
cluster_shmem_iter_regions(int *idx pg_attribute_unused(),
						   ClusterShmemRegion *out pg_attribute_unused())
{
	return false;
}


UT_DEFINE_GLOBALS();


UT_TEST(test_local_srf_still_linkable)
{
	/* Stage 0.16 SRF must still link after 0.17 additions. */
	UT_ASSERT_NOT_NULL((void *)cluster_get_wait_events);
}


UT_TEST(test_gcluster_srf_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_get_gcluster_wait_events);
}

UT_TEST(test_gcluster_adg_srf_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_get_gcluster_adg);
}


int
main(void)
{
	UT_PLAN(3);
	UT_RUN(test_local_srf_still_linkable);
	UT_RUN(test_gcluster_srf_linkable);
	UT_RUN(test_gcluster_adg_srf_linkable);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
