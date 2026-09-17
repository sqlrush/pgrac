/*-------------------------------------------------------------------------
 *
 * test_cluster_side_route.c
 *    RF-SIDE D-SIDE-01 focused unit tests: the total route registry
 *    (§3.2 matrix rows) and the §2.1 verdict surface.
 *
 *    RED mapping (spec §5.1 U-SIDE-01/02/03 + §5.2 L2):
 *      - every matrix-named route row resolves to exactly one row
 *        (L2 positive corpus);
 *      - unknown rmgr/opcode -> lookup false -> BLOCKED (default)
 *        (L2 negative corpus: the unknown hit stays fenced);
 *      - cold/online agree: the verdict is a pure function of the row
 *        (U-SIDE-02);
 *      - the registry is the SINGLE route judgement point (U-SIDE-03:
 *        no second opcode switch exists — callers consume lookup +
 *        verdict only).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/rmgr.h"
#include "access/xact.h"
#include "catalog/pg_control.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_rf_route.h"
#include "cluster/cluster_side_route.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "storage/standbydefs.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

#include <stdio.h>

UT_TEST(test_route_matrix_named_rows)
{
	ClusterSideRouteRow row;

	/* XLOG: FPI -> PAGE; control-only -> PROVED_NOOP; control -> BLOCKED. */
	UT_ASSERT(cluster_side_route_lookup(RM_XLOG_ID, XLOG_FPI, &row));
	UT_ASSERT_EQ((int)row.kind, (int)CLUSTER_SIDE_ROUTE_PAGE);
	UT_ASSERT(cluster_side_route_lookup(RM_XLOG_ID, XLOG_NOOP, &row));
	UT_ASSERT_EQ((int)row.kind, (int)CLUSTER_SIDE_ROUTE_PROVED_NOOP);
	UT_ASSERT_EQ((int)cluster_side_route_verdict(&row),
				 (int)CLUSTER_SIDE_ROUTE_VERDICT_PROVED_NOOP);
	UT_ASSERT(cluster_side_route_lookup(RM_XLOG_ID, XLOG_CHECKPOINT_SHUTDOWN, &row));
	UT_ASSERT_EQ((int)row.kind, (int)CLUSTER_SIDE_ROUTE_BLOCKED);

	/* XACT: commit/abort/prepare -> TT/undo truth; the HAS_INFO modifier
	 * does not change the row (masked). */
	UT_ASSERT(cluster_side_route_lookup(RM_XACT_ID, XLOG_XACT_COMMIT, &row));
	UT_ASSERT_EQ((int)row.kind, (int)CLUSTER_SIDE_ROUTE_TT_UNDO);
	UT_ASSERT(cluster_side_route_lookup(RM_XACT_ID, XLOG_XACT_ABORT, &row));
	UT_ASSERT_EQ((int)row.kind, (int)CLUSTER_SIDE_ROUTE_TT_UNDO);
	UT_ASSERT(cluster_side_route_lookup(RM_XACT_ID, XLOG_XACT_PREPARE, &row));
	UT_ASSERT_EQ((int)row.kind, (int)CLUSTER_SIDE_ROUTE_TT_UNDO);
	UT_ASSERT(cluster_side_route_lookup(RM_XACT_ID, XLOG_XACT_COMMIT_PREPARED, &row));
	UT_ASSERT_EQ((int)row.kind, (int)CLUSTER_SIDE_ROUTE_TT_UNDO);
	UT_ASSERT(cluster_side_route_lookup(RM_XACT_ID, XLOG_XACT_ABORT_PREPARED, &row));
	UT_ASSERT_EQ((int)row.kind, (int)CLUSTER_SIDE_ROUTE_TT_UNDO);
	UT_ASSERT(cluster_side_route_lookup(RM_XACT_ID, XLOG_XACT_ASSIGNMENT, &row));
	UT_ASSERT_EQ((int)row.kind, (int)CLUSTER_SIDE_ROUTE_TT_UNDO);
	UT_ASSERT(cluster_side_route_lookup(RM_XACT_ID, XLOG_XACT_INVALIDATIONS, &row));
	UT_ASSERT_EQ((int)row.kind, (int)CLUSTER_SIDE_ROUTE_BLOCKED);

	/* SMGR lifecycle -> STORAGE. */
	UT_ASSERT(cluster_side_route_lookup(RM_SMGR_ID, XLOG_SMGR_CREATE, &row));
	UT_ASSERT_EQ((int)row.kind, (int)CLUSTER_SIDE_ROUTE_STORAGE);

	/* CLOG / COMMIT_TS / MULTIXACT -> derived projection. */
	UT_ASSERT(cluster_side_route_lookup(RM_CLOG_ID, CLOG_ZEROPAGE, &row));
	UT_ASSERT_EQ((int)row.kind, (int)CLUSTER_SIDE_ROUTE_PROJECTION);
	UT_ASSERT(cluster_side_route_lookup(RM_COMMIT_TS_ID, COMMIT_TS_TRUNCATE, &row));
	UT_ASSERT_EQ((int)row.kind, (int)CLUSTER_SIDE_ROUTE_PROJECTION);

	/* STANDBY: no standby consumer in the physical primary -> no-op. */
	UT_ASSERT(cluster_side_route_lookup(RM_STANDBY_ID, XLOG_STANDBY_LOCK, &row));
	UT_ASSERT_EQ((int)row.kind, (int)CLUSTER_SIDE_ROUTE_PROVED_NOOP);

	/* Page family -> PAGE. */
	UT_ASSERT(cluster_side_route_lookup(RM_HEAP_ID, 0x10, &row));
	UT_ASSERT_EQ((int)row.kind, (int)CLUSTER_SIDE_ROUTE_PAGE);
	UT_ASSERT(cluster_side_route_lookup(RM_BTREE_ID, 0, &row));
	UT_ASSERT_EQ((int)row.kind, (int)CLUSTER_SIDE_ROUTE_PAGE);

	/* Cluster undo truth; HWM stays BLOCKED (STOP-RF-SIDE-SPACE-ABI). */
	UT_ASSERT(cluster_side_route_lookup(RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_BIND, &row));
	UT_ASSERT_EQ((int)row.kind, (int)CLUSTER_SIDE_ROUTE_TT_UNDO);
	UT_ASSERT(cluster_side_route_lookup(RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_COMMIT, &row));
	UT_ASSERT_EQ((int)row.kind, (int)CLUSTER_SIDE_ROUTE_TT_UNDO);
	UT_ASSERT(cluster_side_route_lookup(RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_CTRC_RELEASE, &row));
	UT_ASSERT_EQ((int)row.kind, (int)CLUSTER_SIDE_ROUTE_TT_UNDO);
	UT_ASSERT(cluster_side_route_lookup(RM_CLUSTER_UNDO_ID, XLOG_HW_RESERVE, &row));
	UT_ASSERT_EQ((int)row.kind, (int)CLUSTER_SIDE_ROUTE_BLOCKED);
}

UT_TEST(test_route_unknown_default_blocked)
{
	ClusterSideRouteRow row;

	/* Unknown rmgr: no row -> caller treats it as BLOCKED. */
	UT_ASSERT(!cluster_side_route_lookup(0xFE, 0x10, &row));
	/* Unknown XLOG opcode (not in the matrix): no row. */
	UT_ASSERT(!cluster_side_route_lookup(RM_XLOG_ID, 0xC0, &row));
	/* NULL out. */
	UT_ASSERT(!cluster_side_route_lookup(RM_XLOG_ID, XLOG_FPI, NULL));
	/* NULL row -> BLOCKED verdict. */
	UT_ASSERT_EQ((int)cluster_side_route_verdict(NULL), (int)CLUSTER_SIDE_ROUTE_VERDICT_BLOCKED);
}

UT_TEST(test_route_verdict_pure_cold_online_agree)
{
	ClusterSideRouteRow row;

	/* The verdict is a pure function of the row: no wrapper/severity
	 * input exists, so cold and online produce the identical verdict for
	 * the same record (U-SIDE-02).  Exercise the full verdict mapping. */
	UT_ASSERT(cluster_side_route_lookup(RM_HEAP_ID, 0x10, &row));
	UT_ASSERT_EQ((int)cluster_side_route_verdict(&row), (int)CLUSTER_SIDE_ROUTE_VERDICT_APPLY);
	UT_ASSERT(cluster_side_route_lookup(RM_XACT_ID, XLOG_XACT_COMMIT, &row));
	UT_ASSERT_EQ((int)cluster_side_route_verdict(&row), (int)CLUSTER_SIDE_ROUTE_VERDICT_APPLY);
	UT_ASSERT(cluster_side_route_lookup(RM_XLOG_ID, XLOG_BACKUP_END, &row));
	UT_ASSERT_EQ((int)cluster_side_route_verdict(&row),
				 (int)CLUSTER_SIDE_ROUTE_VERDICT_PROVED_NOOP);
	UT_ASSERT(cluster_side_route_lookup(RM_XLOG_ID, XLOG_NEXTOID, &row));
	UT_ASSERT_EQ((int)cluster_side_route_verdict(&row), (int)CLUSTER_SIDE_ROUTE_VERDICT_BLOCKED);
	UT_ASSERT(cluster_side_route_lookup(RM_REPLORIGIN_ID, 0, &row));
	UT_ASSERT_EQ((int)cluster_side_route_verdict(&row), (int)CLUSTER_SIDE_ROUTE_VERDICT_BLOCKED);
}

UT_TEST(test_route_consumes_the_exhaustive_manifest)
{
	ClusterSideRouteRow side;
	RfOpcodeRouteV1 route;
	bool active;
	size_t i;

	UT_ASSERT_EQ(rf_opcode_route_manifest_count_v1(), 138);
	for (i = 0; i < rf_opcode_route_manifest_count_v1(); i++) {
		UT_ASSERT(rf_opcode_route_manifest_entry_v1(i, &route, &active));
		UT_ASSERT(cluster_side_route_lookup(route.rmid, route.normalized_info, &side));
		UT_ASSERT_EQ(side.rmid, route.rmid);
		UT_ASSERT_EQ(side.opcode, route.normalized_info);
		if (!active)
			UT_ASSERT_EQ((int)side.kind, (int)CLUSTER_SIDE_ROUTE_BLOCKED);
	}

	/* SIDE disposition must refine SIDE_STANDARD rows rather than letting
	 * an rmgr-wide PAGE wildcard silently claim them. */
	UT_ASSERT(cluster_side_route_lookup(RM_HEAP2_ID, 0x00, &side));
	UT_ASSERT_EQ((int)side.kind, (int)CLUSTER_SIDE_ROUTE_BLOCKED);
	UT_ASSERT(cluster_side_route_lookup(RM_HEAP2_ID, 0x70, &side));
	UT_ASSERT_EQ((int)side.kind, (int)CLUSTER_SIDE_ROUTE_PROVED_NOOP);
	UT_ASSERT(cluster_side_route_lookup(RM_HEAP_ID, 0x30, &side));
	UT_ASSERT_EQ((int)side.kind, (int)CLUSTER_SIDE_ROUTE_BLOCKED);
	UT_ASSERT(cluster_side_route_lookup(RM_GIST_ID, 0x70, &side));
	UT_ASSERT_EQ((int)side.kind, (int)CLUSTER_SIDE_ROUTE_PROVED_NOOP);
	UT_ASSERT(cluster_side_route_lookup(RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_CTRC_RELEASE, &side));
	UT_ASSERT_EQ((int)side.kind, (int)CLUSTER_SIDE_ROUTE_TT_UNDO);
}

int
main(void)
{
	UT_PLAN(4);

	UT_RUN(test_route_matrix_named_rows);
	UT_RUN(test_route_unknown_default_blocked);
	UT_RUN(test_route_verdict_pure_cold_online_agree);
	UT_RUN(test_route_consumes_the_exhaustive_manifest);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
