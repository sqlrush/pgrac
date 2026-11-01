/*-------------------------------------------------------------------------
 * test_cluster_recovery_managed_census_fixture.h
 *    Real shared table and managed scanner in the original full census.
 *
 * Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Copyright (c) 1994, The Regents of the University of California
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_recovery_managed_census_fixture.h
 *
 * NOTES
 *    PGRAC-original test bridge. Shared-memory setup and fault injection are
 *    local; table borrowing/current/release use the original implementation.
 *    Private managed_table_* and managed_census_* names isolate providers.
 *-------------------------------------------------------------------------
 */
#define cluster_control_native_files_shmem_register managed_table_shmem_register
#define cluster_control_native_files_mode_v2 managed_table_mode_v2
#define cluster_control_native_files_info_v2 managed_table_info_v2
#define cluster_control_native_files_clog_range_v2 managed_table_clog_range_v2
#define cluster_control_native_files_publish_locked_v2 managed_table_publish_locked_v2
#define cluster_control_native_files_preregister_locked_v2 managed_table_preregister_locked_v2
#define cluster_control_native_file_acquire_v2 managed_table_file_acquire_v2
#define cluster_control_native_file_borrow_by_address_v2 managed_table_file_borrow_by_address_v2
#define cluster_control_native_file_current_v2 managed_table_file_current_v2
#define cluster_control_native_file_release_v2 managed_table_file_release_v2
#include "cluster/cluster_control_native_files.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_wal_thread.h"
#include "storage/shmem.h"
/* Keep the real spinlock implementation private to this test bridge. */
#include "storage/spin.h"
extern int managed_table_s_lock(volatile slock_t *, const char *, int, const char *);
extern void managed_table_s_unlock(volatile slock_t *);
extern void managed_table_perform_spin_delay(SpinDelayStatus *);
extern void managed_table_finish_spin_delay(SpinDelayStatus *);
extern void managed_table_set_spins_per_delay(int);
extern int managed_table_update_spins_per_delay(int);
static uint32 managed_table_wait_event;
uint32 *my_wait_event_info = &managed_table_wait_event;
#define s_lock managed_table_s_lock
#define s_unlock managed_table_s_unlock
#define perform_spin_delay managed_table_perform_spin_delay
#define finish_spin_delay managed_table_finish_spin_delay
#define set_spins_per_delay managed_table_set_spins_per_delay
#define update_spins_per_delay managed_table_update_spins_per_delay
#include "../../backend/storage/lmgr/s_lock.c"
static void
managed_table_register(const ClusterShmemRegion *r)
{
	(void)r;
}
static void *
managed_table_shmem(const char *name, Size size, bool *found)
{
	(void)name;
	*found = false;
	return calloc(1, size);
}
static const PgracInitdbRecoveryContext *
managed_table_creation(void)
{
	return NULL;
}
#define cluster_shmem_register_region managed_table_register
#define ShmemInitStruct managed_table_shmem
#define cluster_wal_thread_initdb_recovery_context managed_table_creation
#include "../../backend/cluster/cluster_control_native_files.c"
#undef cluster_shmem_register_region
#undef ShmemInitStruct
#undef cluster_wal_thread_initdb_recovery_context
static NativeFileShared managed_census_shared;
static struct {
	bool current, error;
	unsigned rechecks;
} managed_census_control;
static ClusterControlNativeTableInfoV2 managed_census_expected;
static NativeFileBank *
managed_census_bank(void)
{
	return &managed_census_shared.banks[managed_census_shared.active];
}
static bool
managed_census_info(ClusterControlNativeTableInfoV2 *out)
{
	managed_census_control.rechecks++;
	if (managed_census_control.error)
		pg_re_throw();
	return managed_census_control.current && managed_table_info_v2(out);
}
#undef cluster_control_native_files_info_v2
#define cluster_control_native_files_info_v2 managed_census_info
#include "cluster/cluster_recovery_relmap_scan.h"
#define cluster_recovery_relmap_scan_begin managed_census_raw_begin
#define cluster_recovery_relmap_scan_begin_managed_v2 managed_census_scan_begin
#define cluster_recovery_relmap_scan_begin_native_v2 managed_census_native_begin
#define cluster_recovery_relmap_scan_output_distinct_v2 managed_census_scan_distinct
#define cluster_recovery_relmap_scan_next managed_census_scan_next
#define cluster_recovery_relmap_scan_diag managed_census_scan_diag
#define cluster_recovery_relmap_scan_destroy managed_census_scan_destroy
extern ClusterRecoveryRelmapResult managed_census_raw_begin(ClusterRecoveryRootSet *, uint64,
															uint64, const uint8[16], size_t,
															ClusterRecoveryRelmapScan **);
extern ClusterRecoveryRelmapResult managed_census_scan_begin(ClusterRecoveryRootSet *,
															 ClusterControlRelmapSelectionV2 *,
															 size_t, ClusterRecoveryRelmapScan **);
extern ClusterRecoveryRelmapResult
managed_census_native_begin(ClusterRecoveryRootSet *, const ClusterControlNativeTableInfoV2 *,
							size_t, ClusterRecoveryRelmapScan **);
extern bool managed_census_scan_distinct(const ClusterRecoveryRelmapScan *, const void *, Size);
extern ClusterRecoveryRelmapResult managed_census_scan_next(ClusterRecoveryRelmapScan *,
															ClusterRecoveryRelmapEntry *);
extern bool managed_census_scan_diag(const ClusterRecoveryRelmapScan *,
									 ClusterRecoveryRelmapDiag *);
extern void managed_census_scan_destroy(ClusterRecoveryRelmapScan *);
#include "../../backend/cluster/cluster_recovery_relmap_scan.c"
