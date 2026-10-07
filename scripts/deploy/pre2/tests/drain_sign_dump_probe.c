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

static void
observe_stream(FILE *stream)
{
	const char *fd = getenv("PGRAC_TEST_DUMP_FD");
	if (stream == stdin && fd != NULL && getenv("PGRAC_TEST_SEED_FD") == NULL) {
		int value = prctl(PR_GET_DUMPABLE, 0, 0, 0, 0);
		char marker = value == 0 ? '0' : '1';
		if (write(atoi(fd), &marker, 1) != 1)
			_exit(93);
	}
}

size_t
fread(void *ptr, size_t size, size_t count, FILE *stream)
{
	size_t (*native)(void *, size_t, size_t, FILE *) = dlsym(RTLD_NEXT, "fread");
	if (native == NULL)
		_exit(92);
	observe_stream(stream);
	return native(ptr, size, count, stream);
}

/* The selected Linux build uses glibc's fortified entry for variable bounds. */
size_t
__fread_chk(void *ptr, size_t bound, size_t size, size_t count, FILE *stream)
{
	size_t (*native)(void *, size_t, size_t, size_t, FILE *) = dlsym(RTLD_NEXT, "__fread_chk");
	if (native == NULL)
		_exit(96);
	observe_stream(stream);
	return native(ptr, bound, size, count, stream);
}

ssize_t
pread(int source, void *ptr, size_t count, off_t offset)
{
	ssize_t (*native)(int, void *, size_t, off_t) = dlsym(RTLD_NEXT, "pread");
	const char *fd = getenv("PGRAC_TEST_DUMP_FD");
	const char *seed = getenv("PGRAC_TEST_SEED_FD");
	if (native == NULL)
		_exit(94);
	if (fd != NULL && seed != NULL && source == atoi(seed)) {
		int value = prctl(PR_GET_DUMPABLE, 0, 0, 0, 0);
		char marker = value == 0 ? '0' : '1';
		if (write(atoi(fd), &marker, 1) != 1)
			_exit(95);
	}
	return native(source, ptr, count, offset);
}
