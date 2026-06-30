/*-------------------------------------------------------------------------
 *
 * test_cluster_backup.c
 *	  spec-6.5 unit tests for cluster backup / restore / PITR helpers.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-6.5-cluster-aware-backup-restore-pitr.md
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_backup.c
 *
 * NOTES
 *	  pgrac-original file.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "catalog/catversion.h"
#include "cluster/cluster_backup.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

int
scn_time_cmp(SCN a, SCN b)
{
	uint64 alocal = scn_local(a);
	uint64 blocal = scn_local(b);

	if (alocal < blocal)
		return -1;
	if (alocal > blocal)
		return 1;
	return 0;
}

static SCN
test_scn(uint64 local)
{
	return scn_encode(0, local);
}

static void
fill_valid_manifest(ClusterBackupManifest *m)
{
	ClusterBackupManifestThread thread;

	cluster_backup_manifest_init(m, "b1");
	m->consistent_scn = test_scn(10);
	m->scn_durable_peak = test_scn(12);
	m->timeline = 1;
	m->catversion = CATALOG_VERSION_NO;
	m->backend_storage_id = 3;
	m->node_count = 1;
	m->control_included = true;

	memset(&thread, 0, sizeof(thread));
	thread.thread_id = 1;
	thread.node_id = 0;
	thread.start_redo_lsn = 10;
	thread.checkpoint_lsn = 20;
	thread.start_tli = 1;
	thread.stop_cut_lsn = 40;
	thread.wal_included = true;
	thread.undo_included = true;
	thread.tt_included = true;
	UT_ASSERT(cluster_backup_manifest_set_thread(m, 0, &thread));
	cluster_backup_manifest_seal(m);
}

UT_TEST(test_manifest_validates_complete_single_thread)
{
	ClusterBackupManifest m;

	fill_valid_manifest(&m);
	UT_ASSERT_EQ(cluster_backup_manifest_validate(&m), CLUSTER_BACKUP_MANIFEST_OK);
	UT_ASSERT_STR_EQ(cluster_backup_manifest_reason_name(CLUSTER_BACKUP_MANIFEST_OK), "ok");
	UT_ASSERT_NE(cluster_backup_manifest_compute_crc(&m), 0);
}

UT_TEST(test_manifest_rejects_missing_control_wal_undo_tt)
{
	ClusterBackupManifest m;

	fill_valid_manifest(&m);
	m.control_included = false;
	cluster_backup_manifest_seal(&m);
	UT_ASSERT_EQ(cluster_backup_manifest_validate(&m), CLUSTER_BACKUP_MANIFEST_MISSING_CONTROL);

	fill_valid_manifest(&m);
	m.threads[0].wal_included = false;
	cluster_backup_manifest_seal(&m);
	UT_ASSERT_EQ(cluster_backup_manifest_validate(&m), CLUSTER_BACKUP_MANIFEST_MISSING_WAL);

	fill_valid_manifest(&m);
	m.threads[0].undo_included = false;
	cluster_backup_manifest_seal(&m);
	UT_ASSERT_EQ(cluster_backup_manifest_validate(&m), CLUSTER_BACKUP_MANIFEST_MISSING_UNDO);

	fill_valid_manifest(&m);
	m.threads[0].tt_included = false;
	cluster_backup_manifest_seal(&m);
	UT_ASSERT_EQ(cluster_backup_manifest_validate(&m), CLUSTER_BACKUP_MANIFEST_MISSING_TT);
}

UT_TEST(test_manifest_rejects_bad_scn_lsn_count_and_crc)
{
	ClusterBackupManifest m;

	fill_valid_manifest(&m);
	m.scn_durable_peak = test_scn(9);
	cluster_backup_manifest_seal(&m);
	UT_ASSERT_EQ(cluster_backup_manifest_validate(&m), CLUSTER_BACKUP_MANIFEST_BAD_SCN_PEAK);

	fill_valid_manifest(&m);
	m.threads[0].stop_cut_lsn = 9;
	cluster_backup_manifest_seal(&m);
	UT_ASSERT_EQ(cluster_backup_manifest_validate(&m), CLUSTER_BACKUP_MANIFEST_BAD_LSN_RANGE);

	fill_valid_manifest(&m);
	m.thread_count = 2;
	cluster_backup_manifest_seal(&m);
	UT_ASSERT_EQ(cluster_backup_manifest_validate(&m), CLUSTER_BACKUP_MANIFEST_MISSING_THREAD);

	fill_valid_manifest(&m);
	m.manifest_crc++;
	UT_ASSERT_EQ(cluster_backup_manifest_validate(&m), CLUSTER_BACKUP_MANIFEST_BAD_CRC);
}

UT_TEST(test_manifest_set_thread_is_bounds_defensive)
{
	ClusterBackupManifest m;
	ClusterBackupManifestThread thread;

	cluster_backup_manifest_init(&m, "bounds");
	memset(&thread, 0, sizeof(thread));
	thread.thread_id = 1;
	UT_ASSERT(!cluster_backup_manifest_set_thread(NULL, 0, &thread));
	UT_ASSERT(!cluster_backup_manifest_set_thread(&m, -1, &thread));
	UT_ASSERT(!cluster_backup_manifest_set_thread(&m, CLUSTER_MAX_NODES, &thread));
	thread.thread_id = 0;
	UT_ASSERT(!cluster_backup_manifest_set_thread(&m, 0, &thread));
	thread.thread_id = CLUSTER_MAX_NODES + 1;
	UT_ASSERT(!cluster_backup_manifest_set_thread(&m, 0, &thread));
}

UT_TEST(test_restore_point_cut_requires_drain_and_fence)
{
	SCN scns[CLUSTER_MAX_NODES];
	XLogRecPtr lsns[CLUSTER_MAX_NODES];
	ClusterRestorePoint point;

	memset(scns, 0, sizeof(scns));
	memset(lsns, 0, sizeof(lsns));
	scns[0] = test_scn(20);
	lsns[0] = 500;

	UT_ASSERT_EQ(
		cluster_restore_point_build(&point, "rp", scns, lsns, CLUSTER_MAX_NODES, false, true, 0),
		CLUSTER_RESTORE_POINT_CUT_PENDING_COMMITS);
	UT_ASSERT_EQ(
		cluster_restore_point_build(&point, "rp", scns, lsns, CLUSTER_MAX_NODES, true, false, 0),
		CLUSTER_RESTORE_POINT_CUT_NO_FENCE);
}

UT_TEST(test_restore_point_cut_records_all_threads)
{
	SCN scns[CLUSTER_MAX_NODES];
	XLogRecPtr lsns[CLUSTER_MAX_NODES];
	ClusterRestorePoint point;

	memset(scns, 0, sizeof(scns));
	memset(lsns, 0, sizeof(lsns));
	scns[0] = test_scn(20);
	lsns[0] = 500;
	scns[2] = test_scn(30);
	lsns[2] = 700;

	UT_ASSERT_EQ(
		cluster_restore_point_build(&point, "rp", scns, lsns, CLUSTER_MAX_NODES, true, true, 9),
		CLUSTER_RESTORE_POINT_CUT_OK);
	UT_ASSERT_EQ(point.thread_count, 2);
	UT_ASSERT_EQ(point.cut_scn, test_scn(30));
	UT_ASSERT_EQ(point.incarnation, 9);
	UT_ASSERT_EQ(point.cut_lsn[2], 700);
}

UT_TEST(test_restore_point_cut_rejects_partial_thread)
{
	SCN scns[CLUSTER_MAX_NODES];
	XLogRecPtr lsns[CLUSTER_MAX_NODES];
	ClusterRestorePoint point;

	memset(scns, 0, sizeof(scns));
	memset(lsns, 0, sizeof(lsns));
	scns[0] = test_scn(20);
	UT_ASSERT_EQ(
		cluster_restore_point_build(&point, "rp", scns, lsns, CLUSTER_MAX_NODES, true, true, 0),
		CLUSTER_RESTORE_POINT_CUT_BAD_THREAD);
}

UT_TEST(test_pitr_resolves_latest_reachable_restore_point)
{
	ClusterRestorePoint points[3];
	ClusterRestorePoint chosen;

	memset(points, 0, sizeof(points));
	points[0].present = true;
	points[0].cut_scn = test_scn(20);
	points[0].thread_count = 1;
	strlcpy(points[0].name, "a", sizeof(points[0].name));
	points[1].present = true;
	points[1].cut_scn = test_scn(30);
	points[1].thread_count = 1;
	strlcpy(points[1].name, "b", sizeof(points[1].name));
	points[2].present = true;
	points[2].cut_scn = test_scn(50);
	points[2].thread_count = 1;
	strlcpy(points[2].name, "c", sizeof(points[2].name));

	UT_ASSERT_EQ(cluster_pitr_resolve_scn(points, 3, test_scn(35), test_scn(10), &chosen),
				 CLUSTER_PITR_TARGET_OK);
	UT_ASSERT_EQ(chosen.cut_scn, test_scn(30));
	UT_ASSERT_STR_EQ(chosen.name, "b");
}

UT_TEST(test_pitr_fail_closed_reasons)
{
	ClusterRestorePoint point;

	memset(&point, 0, sizeof(point));
	point.present = true;
	point.cut_scn = test_scn(20);
	point.thread_count = 0;
	UT_ASSERT_EQ(cluster_pitr_resolve_scn(&point, 1, test_scn(20), test_scn(10), NULL),
				 CLUSTER_PITR_TARGET_MISSING_THREAD);
	UT_ASSERT_EQ(cluster_pitr_resolve_scn(NULL, 0, test_scn(20), test_scn(10), NULL),
				 CLUSTER_PITR_TARGET_NO_RESTORE_POINT);
	UT_ASSERT_EQ(cluster_pitr_resolve_scn(&point, 1, test_scn(5), test_scn(10), NULL),
				 CLUSTER_PITR_TARGET_BEFORE_BACKUP);
}

UT_TEST(test_restore_compatibility_rejects_mismatches)
{
	ClusterBackupManifest m;

	fill_valid_manifest(&m);
	UT_ASSERT_EQ(cluster_backup_manifest_compatible(&m, CATALOG_VERSION_NO, 3, 1),
				 CLUSTER_RESTORE_COMPAT_OK);
	UT_ASSERT_EQ(cluster_backup_manifest_compatible(&m, CATALOG_VERSION_NO + 1, 3, 1),
				 CLUSTER_RESTORE_COMPAT_CATVERSION);
	UT_ASSERT_EQ(cluster_backup_manifest_compatible(&m, CATALOG_VERSION_NO, 4, 1),
				 CLUSTER_RESTORE_COMPAT_STORAGE);
	UT_ASSERT_EQ(cluster_backup_manifest_compatible(&m, CATALOG_VERSION_NO, 3, 2),
				 CLUSTER_RESTORE_COMPAT_TOPOLOGY);
	m.manifest_crc++;
	UT_ASSERT_EQ(cluster_backup_manifest_compatible(&m, CATALOG_VERSION_NO, 3, 1),
				 CLUSTER_RESTORE_COMPAT_MANIFEST);
}

UT_TEST(test_backup_wire_request_crc_and_bounds)
{
	ClusterBackupWireRequest req;

	memset(&req, 0, sizeof(req));
	req.magic = CLUSTER_BACKUP_IC_MAGIC;
	req.version = CLUSTER_BACKUP_IC_VERSION;
	req.op = CLUSTER_BACKUP_WIRE_OP_START;
	req.request_id = 42;
	req.coordinator_node_id = 0;
	strlcpy(req.backup_id, "b-wire", sizeof(req.backup_id));
	cluster_backup_wire_request_compute_crc(&req);
	UT_ASSERT(cluster_backup_wire_request_valid(&req));

	req.request_id = 43;
	UT_ASSERT(!cluster_backup_wire_request_valid(&req));
	req.request_id = 42;
	cluster_backup_wire_request_compute_crc(&req);
	req.backup_id[CLUSTER_BACKUP_ID_MAX - 1] = 'x';
	UT_ASSERT(!cluster_backup_wire_request_valid(&req));
}

UT_TEST(test_backup_wire_ack_fail_closed_validation)
{
	ClusterBackupWireAck ack;

	memset(&ack, 0, sizeof(ack));
	ack.magic = CLUSTER_BACKUP_IC_MAGIC;
	ack.version = CLUSTER_BACKUP_IC_VERSION;
	ack.op = CLUSTER_BACKUP_WIRE_OP_STOP;
	ack.result = CLUSTER_BACKUP_WIRE_RESULT_OK;
	ack.sender_node_id = 1;
	ack.thread_id = 2;
	ack.request_id = 99;
	ack.stop_cut_lsn = 500;
	ack.cut_scn = test_scn(30);
	cluster_backup_wire_ack_compute_crc(&ack);
	UT_ASSERT(cluster_backup_wire_ack_valid(&ack));

	ack.stop_cut_lsn = InvalidXLogRecPtr;
	cluster_backup_wire_ack_compute_crc(&ack);
	UT_ASSERT(!cluster_backup_wire_ack_valid(&ack));

	ack.result = CLUSTER_BACKUP_WIRE_RESULT_EXECUTOR_ERROR;
	ack.thread_id = 0;
	ack.cut_scn = InvalidScn;
	cluster_backup_wire_ack_compute_crc(&ack);
	UT_ASSERT(cluster_backup_wire_ack_valid(&ack));
	UT_ASSERT_STR_EQ(cluster_backup_wire_result_name(CLUSTER_BACKUP_WIRE_RESULT_EXECUTOR_ERROR),
					 "executor_error");
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(test_manifest_validates_complete_single_thread);
	UT_RUN(test_manifest_rejects_missing_control_wal_undo_tt);
	UT_RUN(test_manifest_rejects_bad_scn_lsn_count_and_crc);
	UT_RUN(test_manifest_set_thread_is_bounds_defensive);
	UT_RUN(test_restore_point_cut_requires_drain_and_fence);
	UT_RUN(test_restore_point_cut_records_all_threads);
	UT_RUN(test_restore_point_cut_rejects_partial_thread);
	UT_RUN(test_pitr_resolves_latest_reachable_restore_point);
	UT_RUN(test_pitr_fail_closed_reasons);
	UT_RUN(test_restore_compatibility_rejects_mismatches);
	UT_RUN(test_backup_wire_request_crc_and_bounds);
	UT_RUN(test_backup_wire_ack_fail_closed_validation);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
