/* Test-only libc boundary probe; never linked into a product.
 * Author: SqlRush <sqlrush@gmail.com>
 * PGRAC-original: inspect actual process dumpability at the first secret read.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/prctl.h>
#include <unistd.h>

int
prctl(int option, ...)
{
	int (*native)(int, ...) = dlsym(RTLD_NEXT, "prctl");
	if (getenv("PGRAC_TEST_DUMP_FAIL") != NULL && option == PR_SET_DUMPABLE) {
		errno = EPERM;
		return -1;
	}
	/* This fixture intercepts only GET and SET(0), used by this process. */
	if (native == NULL || (option != PR_GET_DUMPABLE && option != PR_SET_DUMPABLE))
		_exit(91);
	return native(option, 0, 0, 0, 0);
}

size_t
fread(void *ptr, size_t size, size_t count, FILE *stream)
{
	size_t (*native)(void *, size_t, size_t, FILE *) = dlsym(RTLD_NEXT, "fread");
	const char *fd = getenv("PGRAC_TEST_DUMP_FD");
	if (native == NULL)
		_exit(92);
	if (stream == stdin && fd != NULL) {
		int value = prctl(PR_GET_DUMPABLE, 0, 0, 0, 0);
		char marker = value == 0 ? '0' : '1';
		if (write(atoi(fd), &marker, 1) != 1)
			_exit(93);
	}
	return native(ptr, size, count, stream);
}
