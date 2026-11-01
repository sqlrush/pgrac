/* Fault only the native syscall boundary; all registry bytes and I/O are real. */
#include "postgres_fe.h"
#include <unistd.h>
#include <errno.h>
static int fault_sync;
static void (*read_hook)(void);
static int registry_fixture_fsync(int fd);
static ssize_t registry_fixture_pread(int fd, void *buffer, size_t count, off_t offset);
void test_registry_io_fault(int at);
void test_registry_io_read_hook(void (*hook)(void));
#define fsync registry_fixture_fsync
#define pread registry_fixture_pread
#include "../../common/cluster_recovery_registry.c"
#undef fsync
#undef pread
static int registry_fixture_fsync(int fd)
{
	if (fault_sync > 0 && --fault_sync == 0)
	{
		errno = EIO;
		return -1;
	}
	return fsync(fd);
}
static ssize_t registry_fixture_pread(int fd, void *buffer, size_t count, off_t offset)
{
	ssize_t n = pread(fd, buffer, count, offset);
	if (read_hook)
	{
		void (*hook)(void) = read_hook;
		read_hook = NULL;
		hook();
	}
	return n;
}
void test_registry_io_fault(int at)
{
	fault_sync = at;
}
void test_registry_io_read_hook(void (*hook)(void))
{
	read_hook = hook;
}
