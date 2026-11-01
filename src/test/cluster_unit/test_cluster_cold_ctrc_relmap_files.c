/*-------------------------------------------------------------------------
 * test_cluster_cold_ctrc_relmap_files.c
 *     Original Start observation with real relmap files, SIDE and pins.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_ctrc_relmap_files.c
 * NOTES
 *     Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
#define TEST_CLUSTER_STARTUP_RELMAP_FILES 1
#include "test_cluster_cold_native_side_namespace.h"
#include "test_cluster_cold_tx_start.c"
