/*-------------------------------------------------------------------------
 *
 * vb_interpose.c
 *	  Optional libc-level I/O counter for the verdict micro-benchmark.
 *
 *	  A dyld interposition library (macOS only).  When preloaded into the
 *	  postmaster it counts, per process, the open / close / pread / access
 *	  calls that touch undo segment files, independently of the server's own
 *	  counters.  test_pgrac_vb_run() reports the deltas when it finds
 *	  vb_interpose_snapshot() in the process.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/modules/test_pgrac_verdict_bench/vb_interpose.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *	  Not part of any server build.  Compile it explicitly before a count run:
 *	    cc -O2 -dynamiclib -I"$(pg_config --includedir-server)" \
 *	       -I"$(pg_config --includedir)" -o vb_interpose.dylib vb_interpose.c
 *	  and starts the postmaster with DYLD_INSERT_LIBRARIES pointing at it.
 *	  Use it for call-count verification runs only; timing runs go without.
 *
 *	  Snapshot slots:
 *	    0  pread calls on undo segment descriptors
 *	    1  bytes returned by those preads
 *	    2  successful opens of undo segment paths
 *	    3  failed opens of undo segment paths
 *	    4  closes of undo segment descriptors
 *	    5  access() probes of undo segment paths
 *	    6  pread calls on any descriptor
 *	    7  open calls on any path
 *	    8  nanoseconds spent in the preads of slot 0
 *	    9  nanoseconds spent in the opens of slot 2
 *	   10  nanoseconds spent in the opens of slot 3
 *	   11  nanoseconds spent in the closes of slot 4
 *	   12  nanoseconds spent in the probes of slot 5
 *	   13  undo preads whose held LWLocks were inspected
 *	   14  undo preads with at least one held LWLock
 *	   15  undo preads where the lock inspector was unavailable
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "storage/lwlock.h"

#define VB_TRACKED_FDS 4096
#define VB_UNDO_MARKER "/pg_undo/"

#define VB_INTERPOSE(replacement, replacee)                                                        \
	__attribute__((used)) static struct {                                                         \
		const void *new_function;                                                                  \
		const void *old_function;                                                                  \
	} vb_interpose_##replacee __attribute__((section("__DATA,__interpose")))                      \
	= { (const void *)(unsigned long)&replacement, (const void *)(unsigned long)&replacee }

#define VB_SLOTS 16

static uint64_t vb_counters[VB_SLOTS];
static unsigned char vb_undo_fd[VB_TRACKED_FDS];

typedef void (*VbLockInspector)(void (*)(LWLock *, LWLockMode, void *), void *);

static VbLockInspector vb_lock_inspector;
static bool vb_inspector_resolved;

void vb_interpose_snapshot(uint64_t *out);

void
vb_interpose_snapshot(uint64_t *out)
{
	memcpy(out, vb_counters, sizeof(vb_counters));
}

static uint64_t
vb_now_ns(void)
{
	return clock_gettime_nsec_np(CLOCK_UPTIME_RAW);
}

static int
vb_is_undo_path(const char *path)
{
	return path != NULL && strstr(path, VB_UNDO_MARKER) != NULL;
}

/* Read the backend's real held-lock list; acquire no lock in the observer. */
static void
vb_count_lock(LWLock *lock, LWLockMode mode, void *context)
{
	unsigned *count = context;

	(void)lock;
	(void)mode;
	(*count)++;
}

static void
vb_observe_locks(void)
{
	unsigned held = 0;

	if (!vb_inspector_resolved) {
		vb_lock_inspector = (VbLockInspector)dlsym(RTLD_DEFAULT, "ForEachLWLockHeldByMe");
		vb_inspector_resolved = true;
	}
	if (vb_lock_inspector == NULL) {
		vb_counters[15]++;
		return;
	}
	vb_lock_inspector(vb_count_lock, &held);
	vb_counters[13]++;
	if (held != 0)
		vb_counters[14]++;
}

static int
vb_open(const char *path, int flags, ...)
{
	mode_t mode = 0;
	uint64_t started;
	int fd;
	int saved_errno;

	if ((flags & O_CREAT) != 0) {
		va_list args;

		va_start(args, flags);
		mode = (mode_t)va_arg(args, int);
		va_end(args);
	}
	started = vb_now_ns();
	fd = open(path, flags, mode);
	saved_errno = errno;
	vb_counters[7]++;
	if (vb_is_undo_path(path)) {
		if (fd >= 0) {
			vb_counters[2]++;
			vb_counters[9] += vb_now_ns() - started;
			if (fd < VB_TRACKED_FDS)
				vb_undo_fd[fd] = 1;
		} else {
			vb_counters[3]++;
			vb_counters[10] += vb_now_ns() - started;
		}
	}
	errno = saved_errno;
	return fd;
}
VB_INTERPOSE(vb_open, open);

static int
vb_close(int fd)
{
	if (fd >= 0 && fd < VB_TRACKED_FDS && vb_undo_fd[fd]) {
		uint64_t started = vb_now_ns();
		int result = close(fd);
		int saved_errno = errno;

		vb_undo_fd[fd] = 0;
		vb_counters[4]++;
		vb_counters[11] += vb_now_ns() - started;
		errno = saved_errno;
		return result;
	}
	return close(fd);
}
VB_INTERPOSE(vb_close, close);

static ssize_t
vb_pread(int fd, void *buffer, size_t count, off_t offset)
{
	int entry_errno = errno;
	bool undo = fd >= 0 && fd < VB_TRACKED_FDS && vb_undo_fd[fd];
	uint64_t started;
	ssize_t got;
	int saved_errno;

	if (undo)
		vb_observe_locks();
	started = vb_now_ns();
	errno = entry_errno;
	got = pread(fd, buffer, count, offset);
	saved_errno = errno;

	vb_counters[6]++;
	if (undo) {
		vb_counters[0]++;
		vb_counters[8] += vb_now_ns() - started;
		if (got > 0)
			vb_counters[1] += (uint64_t)got;
	}
	errno = saved_errno;
	return got;
}
VB_INTERPOSE(vb_pread, pread);

static int
vb_access(const char *path, int mode)
{
	if (vb_is_undo_path(path)) {
		uint64_t started = vb_now_ns();
		int result = access(path, mode);
		int saved_errno = errno;

		vb_counters[5]++;
		vb_counters[12] += vb_now_ns() - started;
		errno = saved_errno;
		return result;
	}
	return access(path, mode);
}
VB_INTERPOSE(vb_access, access);
