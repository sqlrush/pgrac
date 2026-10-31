/*-------------------------------------------------------------------------
 * test_cluster_cold_relmap_side.c
 *    Declared cold relmap records coexist with original sealed SIDE input.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_cold_relmap_side.c
 * NOTES
 *    Original cold/SIDE plans and record consumers; authority/storage leaves
 *    use the existing SIDE fixture. No native lifetime qualification.
 *-------------------------------------------------------------------------
 */
#define TEST_CLUSTER_COLD_RELMAP_SIDE 1
#include "test_cluster_side_xact.c"
