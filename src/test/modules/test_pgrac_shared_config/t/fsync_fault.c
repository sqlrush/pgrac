/* Process-local syscall fault for a single disposable relation inode.
 * No fault is armed until the control file contains the exact pathname.
 * Author: SqlRush <sqlrush@gmail.com> */
#if defined(__linux__) && !defined(_GNU_SOURCE)
#define _GNU_SOURCE
#endif
#include <sys/stat.h>
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int
target_matches(int fd)
{
	const char *control = getenv("PGRAC_FSYNC_FAULT_CONTROL");
	char path[PATH_MAX];
	struct stat target;
	struct stat opened;
	ssize_t len;
	int input;
	int saved_errno = errno;
	int match = 0;

	if (control == NULL || (input = open(control, O_RDONLY)) < 0)
		return 0;
	len = read(input, path, sizeof(path) - 1);
	close(input);
	if (len > 0 && len < (ssize_t) sizeof(path) - 1)
	{
		path[len] = '\0';
		if (path[len - 1] == '\n')
			path[len - 1] = '\0';
		match = path[0] == '/' && stat(path, &target) == 0 &&
			fstat(fd, &opened) == 0 && S_ISREG(opened.st_mode) &&
			target.st_dev == opened.st_dev && target.st_ino == opened.st_ino;
	}
	errno = saved_errno;
	return match;
}

static int
fault_fsync(int fd)
{
#ifndef __APPLE__
	static int (*native_fsync)(int);
#endif

	if (target_matches(fd))
	{
		const char *witness = getenv("PGRAC_FSYNC_FAULT_WITNESS");
		int output = witness ? open(witness, O_WRONLY | O_CREAT | O_APPEND, 0600) : -1;

		if (output >= 0)
		{
			char line[80];
			int len = snprintf(line, sizeof(line), "pid=%ld fd=%d fsync=EIO\n", (long) getpid(), fd);

			(void) write(output, line, len);
			close(output);
		}
		errno = EIO;
		return -1;
	}
#ifdef __APPLE__
	/* dyld leaves references originating in this interposer unmodified. */
	return fsync(fd);
#else
	if (native_fsync == NULL)
		native_fsync = (int (*)(int)) dlsym(RTLD_NEXT, "fsync");
	if (native_fsync == NULL)
		_exit(125);
	return native_fsync(fd);
#endif
}

#ifdef __APPLE__
__attribute__((used)) static const struct {
	const void *replacement;
	const void *original;
} interpose_fsync __attribute__((section("__DATA,__interpose"))) = {
	(const void *) fault_fsync, (const void *) fsync
};
#else
int fsync(int fd) { return fault_fsync(fd); }
#endif
