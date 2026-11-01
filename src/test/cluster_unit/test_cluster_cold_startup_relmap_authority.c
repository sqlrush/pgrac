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
/* Resolve libc fortified inlines before mapping calls to the real-I/O fixture.
 * Otherwise glibc's renamed read inline calls its literal libc read alias and
 * bypasses the original read observation and fault callback. */
#include "postgres.h"
#include <unistd.h>

ssize_t relmap_test_write(int fd, const void *bytes, size_t len);
ssize_t test_cold_relmap_storage_read(int fd, void *bytes, size_t len);
int relmap_test_renameat(int olddir, const char *old, int newdir, const char *new);
int relmap_test_close(int fd);

#define write relmap_test_write
#define read test_cold_relmap_storage_read
#define renameat relmap_test_renameat
#define close relmap_test_close
#include "../../backend/cluster/cluster_relmap_authority.c"
