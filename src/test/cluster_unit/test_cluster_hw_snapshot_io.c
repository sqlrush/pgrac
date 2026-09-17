/*-------------------------------------------------------------------------
 *
 * test_cluster_hw_snapshot_io.c
 *    Compile the production reader with an exact read-fault injection seam.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_hw_snapshot_io.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

extern ssize_t hw_snapshot_test_read(int fd, void *buf, size_t count);

/*
 * Define the seam after libc declarations.  A command-line -Dread also renames
 * fortified libc declarations whose assembler alias still calls real read(),
 * silently bypassing the injected EINTR, short-read and I/O-error witnesses.
 */
#define read hw_snapshot_test_read
#include "../../backend/cluster/cluster_hw_snapshot.c"
#undef read
