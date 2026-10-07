/* Interpose after libc declarations: _FILE_OFFSET_BITS must not redirect
 * the test hook's declaration to the real pread64/pwrite64 symbol.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <unistd.h>

extern int cluster_qvotec_test_fdatasync(int fd);
extern ssize_t cluster_qvotec_test_pwrite(int fd, const void *buf, size_t size, off_t offset);
extern ssize_t cluster_qvotec_test_pread(int fd, void *buf, size_t size, off_t offset);

#define fdatasync cluster_qvotec_test_fdatasync
#define pwrite cluster_qvotec_test_pwrite
#define pread cluster_qvotec_test_pread
#include VOTING_DISK_IO_SOURCE_PATH
