/*-------------------------------------------------------------------------
 * test_cluster_cold_startup_relmap_authority.c
 *     Original authority with the isolated storage syscall providers.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_startup_relmap_authority.c
 * NOTES
 *     Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
#include "test_cluster_cold_startup_relmap_storage_namespace.h"
/* ERROR follows Startup's original exception stack, including inner cleanup. */
#undef PG_exception_stack
#undef error_context_stack
#undef pg_re_throw
#define write relmap_test_write
#define read relmap_test_read
#define renameat relmap_test_renameat
#define close relmap_test_close
#include "../../backend/cluster/cluster_relmap_authority.c"
