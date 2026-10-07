/*-------------------------------------------------------------------------
 * PGRAC: deterministic syscall interleavings around the production collector.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <fcntl.h>
#include <unistd.h>

extern int unit_bootstrap_openat(int dir, const char *name, int flags, ...);
extern int unit_bootstrap_close(int fd);

/* Include native prototypes/fortified aliases first, then redirect calls only.
 * Defining openat on the compiler command line is defeated by glibc's inline
 * alias. No read, codec, identity or positive result is replaced here.
 */
#define openat unit_bootstrap_openat
#define close unit_bootstrap_close
#include "../../backend/cluster/cluster_control_bootstrap_read.c"
