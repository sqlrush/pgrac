/*-------------------------------------------------------------------------
 * test_cluster_recovery_relmap_table_unavailable.h
 *    Refuse table use in the independent raw and explicit-CF fixtures.
 *
 * Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Copyright (c) 1994, The Regents of the University of California
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_recovery_relmap_table_unavailable.h
 *
 * NOTES
 *    PGRAC-original abort-only link providers. Any table call aborts; the
 *    shared-table companion tests the real implementation independently.
 *-------------------------------------------------------------------------
 */
#include "cluster/cluster_control_native_files.h"
bool
cluster_control_native_files_info_v2(ClusterControlNativeTableInfoV2 *out)
{
	abort();
}
ClusterControlNativeResultV2
cluster_control_native_file_borrow_by_address_v2(ClusterRecoveryFileKindV2 kind, Oid db, Oid ts,
												 uint64 object,
												 ClusterControlNativeFileBorrowV2 *out)
{
	abort();
}
bool
cluster_control_native_file_current_v2(const ClusterControlNativeFileBorrowV2 *borrow)
{
	abort();
}
bool
cluster_control_native_file_release_v2(ClusterControlNativeFileBorrowV2 *borrow)
{
	abort();
}
/* These scanner-only fixtures have no server error reporter. Any unexpected
 * shared-table cleanup branch must fail loudly, never simulate a release. */
bool
errstart_cold(int level, const char *domain)
{
	abort();
}
int
errmsg(const char *format, ...)
{
	abort();
}
void
errfinish(const char *file, int line, const char *function)
{
	abort();
}
