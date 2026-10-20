/*-------------------------------------------------------------------------
 *
 * test_cluster_views.c
 *	  Registration invariants for the cluster views framework
 *	  introduced in stage 0.16.
 *
 *	  Stage 0.16 backs pg_stat_cluster_wait_events with the SRF
 *	  cluster_get_wait_events() in cluster_views.c, registered via
 *	  pg_proc.dat.  Exercise its actual roster and wait-event decoder
 *	  with text/tuplestore boundaries, without a PG executor:
 *
 *	  - CLUSTER_WAIT_EVENTS_COUNT matches the registered WaitEventCluster table.
 *	  - cluster_get_wait_events function symbol resolves at link time.
 *	  - Its emitted rows name the frontend capacity event exactly once.
 *
 *	  Runtime behaviour (the view returns one row per registered wait
 *	  event with the correct type / name values) is validated by
 *	  cluster_tap t/010_views.pl on a real PG instance.
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
 *	  fmgr / tuplestore machinery; the test supplies explicit boundaries
 *	  while using the production wait_event.c decoder.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_apply_master_election.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_mrp.h"
#include "cluster/cluster_views.h"
#include "cluster/cluster_wait_events.h" /* PG_WAIT_CLUSTER_GES / ADG */
#include "utils/wait_event.h"			 /* WAIT_EVENT_GES_ENQUEUE_ACQUIRE etc. */

#include "../../backend/utils/activity/wait_event.c"

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
 *	Text Datums are opaque string pointers at this boundary; the actual
 *	SRF emits its production roster using the production decoder.
 * ----------
 */
#include "funcapi.h"
#include "utils/builtins.h"

static unsigned captured_rows;
static const char *captured_types[CLUSTER_WAIT_EVENTS_COUNT];
static const char *captured_names[CLUSTER_WAIT_EVENTS_COUNT];

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
					 TupleDesc tdesc pg_attribute_unused(), Datum *values, bool *isnull)
{
	if (captured_rows >= lengthof(captured_names) || isnull[0] || isnull[1])
		abort();
	captured_types[captured_rows] = (const char *)DatumGetPointer(values[0]);
	captured_names[captured_rows] = (const char *)DatumGetPointer(values[1]);
	captured_rows++;
}

text *
cstring_to_text(const char *s)
{
	return (text *)s;
}

const char *
GetLWLockIdentifier(uint32 classId pg_attribute_unused(), uint16 eventId pg_attribute_unused())
{
	abort();
}

const char *
GetLockNameFromTagType(uint16 locktag_type pg_attribute_unused())
{
	abort();
}

/*
 * cluster_node_id is defined in cluster_guc.o in real backend builds.
 * Stage 0.17 added cluster_get_gcluster_wait_events to cluster_views.o,
 * which references this symbol.  The unit test binary does not link
 * cluster_guc.o, so provide a local definition with the same default
 * value as the GUC.  Mirrors the stub in test_cluster_gviews.c.
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

uint64
cluster_apply_master_current_term(void)
{
	return 0;
}

uint32
cluster_apply_master_current_node_id(void)
{
	return UINT32_MAX;
}

/*
 * Stage 0.28 stubs: cluster_views.c::cluster_get_stat_nodes references
 * ClusterShmem (from cluster_shmem.o) + cluster_conf_lookup_node /
 * cluster_conf_role_to_string (from cluster_conf.o).  This unit test
 * does not link those; provide minimal stubs so the linker resolves
 * the SRF symbol address (the SRF body itself is never invoked here).
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

/*
 * Stage 0.30 sweep: cluster_views.c gained a CLUSTER_INJECTION_POINT
 * call at the top of cluster_get_wait_events; stub the inject symbols
 * so the unit test linking succeeds without pulling in cluster_inject.o.
 */
int cluster_injection_armed_count = 0;

void
cluster_injection_run(const char *name pg_attribute_unused())
{}

/*
 * Stage 1.3: cluster_views.c::cluster_shmem_dump_regions calls the
 * registry iter API.  Stub returns "no rows" -- the SRF body is never
 * invoked in this test (we only check linkage).
 */
bool
cluster_shmem_iter_regions(int *idx pg_attribute_unused(),
						   ClusterShmemRegion *out pg_attribute_unused())
{
	return false;
}


UT_DEFINE_GLOBALS();


UT_TEST(test_cluster_wait_events_count_is_125)
{
	/*
	 * Cumulative registration roster: 61 prior + 3 added by spec-2.6 D11
	 * (ClusterBgProcQvotecMainLoop + ClusterVotingDiskRead/Write) + 1
	 * added by spec-2.28 D9 (ClusterFenceBackendInterruptCheck) + 1
	 * added by spec-2.29 D9 (BgProcLmonReconfigTick) + 3 added by
	 * spec-2.19 D12 (LMD lifecycle events) + 1 added by spec-2.20 D12
	 * (ClusterGesS4Wait) + 1 added by spec-2.22 D10 (ClusterLmdProbe) +
	 * 2 added by spec-2.23 D12 (ClusterGesReplyWait + ClusterLmd-
	 * ProbeCollect) + 2 added by spec-2.25 D11
	 * (ClusterLmsNativeProbeWait + ClusterNativeProbeReplyWait) + 2 added
	 * by spec-2.30 D8 (ClusterPcmGrdInit + ClusterPcmTransitionApply) + 1
	 * added by spec-2.32 D9 (ClusterGcsReplyWait) + 4 added by spec-2.33
	 * D9 (ClusterGCSBlockShipWait + request/reply dispatch + checksum fail) +
	 * 2 added by spec-2.34 D7 (ClusterGCSBlockRetransmitWait +
	 * ClusterGCSBlockEpochStaleRetry) + 3 added by spec-2.36 D8
	 * (ClusterGCSBlockInvalidateBroadcast + ClusterGCSBlockInvalidateAckWait
	 * + ClusterGCSBlockStarvationRetry) + 2 added by spec-4.1 D7
	 * (ClusterWalThreadClaimRead/Write) + 1 added by spec-4.6 D4
	 * (ClusterGrdShardRemaster) + 1 added by spec-4.7 D1
	 * (ClusterGCSBlockRecovering) + 1 added by spec-4.11 D5
	 * (ClusterThreadRecovery) + 1 added by spec-5.18 D12
	 * (ClusterReconfigNodeRemoveCleanupWait) + 7 added by spec-6.0a D10
	 * (block_device production wait events) + spec-6.1 D8 RDMA
	 * send/recv/poll/connect/fallback events + 4 added by spec-6.2 D10
	 * (Smart Fusion commit/DBWR/origin durable brakes + terminal resolve) +
	 * 2 added by spec-6.13 D8 (RDMA busypoll + inline send) + 3 added by
	 * spec-5.22b D2-6 (UndoBlock Grant/Invalidate/Remaster grant-plane waits).
	 * If a future subsystem spec adds new cluster wait events, both the
	 * enum in wait_event.h and CLUSTER_WAIT_EVENTS_COUNT must move
	 * together, and this test number must be bumped in lockstep.
	 */
	/* spec-7.2 D6: +2 LMS data-plane -> 120. */
	/* Frontend undo capacity wait adds one registered event. */
	UT_ASSERT_EQ(CLUSTER_WAIT_EVENTS_COUNT, 125);
}

UT_TEST(test_capacity_wait_decoder_keeps_undo_and_background_names_distinct)
{
	UT_ASSERT_STR_EQ(pgstat_get_wait_event(PG_WAIT_CLUSTER_UNDO | 11U), "ClusterUndoCapacityWait");
	UT_ASSERT_STR_EQ(pgstat_get_wait_event_type(PG_WAIT_CLUSTER_UNDO | 11U), "Cluster: Undo");
	UT_ASSERT_STR_EQ(pgstat_get_wait_event(WAIT_EVENT_CLUSTER_BGPROC_UNDO_CLEANER_MAIN_LOOP),
					 "ClusterBgProcUndoCleanerMainLoop");
}

UT_TEST(test_actual_srf_emits_capacity_once_in_undo_class)
{
	LOCAL_FCINFO(fcinfo, 0);
	ReturnSetInfo rsinfo;
	unsigned capacity_rows = 0, undo_rows = 0;

	MemSet(fcinfo, 0, SizeForFunctionCallInfo(0));
	MemSet(&rsinfo, 0, sizeof(rsinfo));
	fcinfo->resultinfo = (Node *)&rsinfo;
	captured_rows = 0;
	UT_ASSERT_EQ(cluster_get_wait_events(fcinfo), 0);
	UT_ASSERT_EQ(captured_rows, 125);
	for (unsigned i = 0; i < captured_rows; i++) {
		UT_ASSERT_NOT_NULL(captured_types[i]);
		UT_ASSERT_NOT_NULL(captured_names[i]);
		UT_ASSERT(strcmp(captured_names[i], "unknown wait event") != 0);
		for (unsigned j = 0; j < i; j++)
			UT_ASSERT(strcmp(captured_names[i], captured_names[j]) != 0);
		if (strcmp(captured_types[i], "Cluster: Undo") == 0)
			undo_rows++;
		if (strcmp(captured_names[i], "ClusterUndoCapacityWait") == 0) {
			capacity_rows++;
			UT_ASSERT_STR_EQ(captured_types[i], "Cluster: Undo");
		}
	}
	UT_ASSERT_EQ(capacity_rows, 1);
	UT_ASSERT_EQ(undo_rows, 8);
}


UT_TEST(test_srf_symbol_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_get_wait_events);
}

UT_TEST(test_adg_srf_symbol_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_get_adg_state);
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


UT_TEST(test_adg_wal_receive_lag_in_adg_class)
{
	/*
	 * Class-membership anchor: WAIT_EVENT_ADG_WAL_RECEIVE_LAG must sit in
	 * the ADG class regardless of where the enum tail moves over time.
	 * (spec-2.5 D8 appended ClusterBgProcCssdMainLoop and spec-2.6 D11
	 * appended 3 more entries past it, so this is no longer the last
	 * enum value — it is still a stable anchor for the ADG class.)
	 */
	UT_ASSERT_EQ(((uint32)WAIT_EVENT_ADG_WAL_RECEIVE_LAG) & 0xFF000000U, PG_WAIT_CLUSTER_ADG);
}


int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_cluster_wait_events_count_is_125);
	UT_RUN(test_capacity_wait_decoder_keeps_undo_and_background_names_distinct);
	UT_RUN(test_actual_srf_emits_capacity_once_in_undo_class);
	UT_RUN(test_srf_symbol_linkable);
	UT_RUN(test_adg_srf_symbol_linkable);
	UT_RUN(test_first_event_is_ges_enqueue_acquire);
	UT_RUN(test_adg_wal_receive_lag_in_adg_class);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
