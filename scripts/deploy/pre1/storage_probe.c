/*-------------------------------------------------------------------------
 * storage_probe.c
 *    Standalone deployment filesystem witness; never linked into postgres.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, PGRAC contributors
 *
 * Work is restricted to one new, owned, token-marked scratch directory.
 * stdin supplies an external barrier; timestamps from different kernels
 * must not be compared as if they were a shared clock. No probe timeout
 * changes any database wait semantics. The controller bounds each process.
 *-------------------------------------------------------------------------
 */

#define _POSIX_C_SOURCE 200809L
#define _DARWIN_C_SOURCE
#define _DEFAULT_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <inttypes.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <time.h>
#include <unistd.h>

#define PAGE_BYTES 8192
#define MAX_BYTES 32768
#define TOKEN_BYTES 32

static const char *probe_case = "arguments";
static const char *token = "";
static unsigned int node;
static unsigned int writer;
static unsigned int sequence;
static unsigned int length = PAGE_BYTES;
static unsigned int offset;
static unsigned int iterations = 1200;
static unsigned int capacity_bytes;
static uintmax_t capacity_device;

/* Every syscall failure and content mismatch remains an explicit event. */
static void
event(const char *call, long result, int error, unsigned int count, uint32_t crc,
	  const char *status)
{
	struct timespec now;
	uint64_t stamp = 0;
	unsigned int actual_offset
		= strncmp(call, "fcntl", 5) == 0 || strcmp(probe_case, "capacity-fill") == 0 ? offset : 0;

	if (clock_gettime(CLOCK_MONOTONIC, &now) == 0)
		stamp = (uint64_t)now.tv_sec * UINT64_C(1000000000) + (uint64_t)now.tv_nsec;
	printf("{\"case\":\"%s\",\"syscall\":\"%s\",\"result\":%ld,"
		   "\"errno\":%d,\"offset\":%u,\"length\":%u,\"crc32\":%" PRIu32 ","
		   "\"token\":\"%s\",\"node\":%u,\"mono_ns\":%" PRIu64 ","
		   "\"status\":\"%s\"}\n",
		   probe_case, call, result, error, actual_offset, count, crc, token, node, stamp, status);
	if (fflush(stdout) != 0)
		exit(3);
}

static void
fail(const char *call, int error, int rc)
{
	event(call, -1, error, 0, 0, rc == 3 ? "INCOMPLETE" : "FAIL");
	exit(rc);
}

static long
checked(const char *call, long result, unsigned int count)
{
	int error = result < 0 ? errno : 0;

	event(call, result, error, count, 0, result < 0 ? "FAIL" : "OK");
	if (result < 0)
		exit(1);
	return result;
}

static unsigned int
number(const char *value, unsigned int maximum)
{
	char *end;
	unsigned long parsed;

	if (*value == '\0' || strspn(value, "0123456789") != strlen(value))
		fail("arguments", EINVAL, 2);
	errno = 0;
	parsed = strtoul(value, &end, 10);
	if (errno != 0 || *end != '\0' || parsed > maximum)
		fail("arguments", EINVAL, 2);
	return (unsigned int)parsed;
}

static void
sync_fd(int fd)
{
	checked("fsync", fsync(fd), 0);
}

static void
close_fd(int fd)
{
	checked("close", close(fd), 0);
}

/* Reject aliases in every path component, not just the final directory. */
static int
open_parent(const char *root)
{
	char path[PATH_MAX];
	char expected[64];
	char *component;
	char *slash;
	int directory;

	if (strlen(root) >= sizeof(path) || root[0] != '/' || strstr(root, "//") != NULL)
		fail("arguments", EINVAL, 2);
	strcpy(path, root);
	slash = strrchr(path, '/');
	snprintf(expected, sizeof(expected), "pre1-probe-%s", token);
	if (slash == path || strcmp(slash + 1, expected) != 0)
		fail("arguments", EINVAL, 2);
	*slash = '\0';
	directory = (int)checked("open-root", open("/", O_RDONLY | O_DIRECTORY), 0);
	component = path + 1;
	while (*component != '\0') {
		int next;

		slash = strchr(component, '/');
		if (slash != NULL)
			*slash = '\0';
		if (strcmp(component, ".") == 0 || strcmp(component, "..") == 0)
			fail("path-component", EINVAL, 2);
		next = (int)checked("open-parent",
							openat(directory, component, O_RDONLY | O_DIRECTORY | O_NOFOLLOW), 0);
		close_fd(directory);
		directory = next;
		if (slash == NULL)
			break;
		component = slash + 1;
	}
	return directory;
}

static int
owned_file(int directory, const char *name, int flags)
{
	struct stat st;
	int fd
		= (int)checked("openat", openat(directory, name, flags | O_NOFOLLOW | O_NONBLOCK, 0600), 0);

	checked("fstat", fstat(fd, &st), 0);
	if (!S_ISREG(st.st_mode) || st.st_uid != geteuid() || st.st_nlink != 1
		|| (st.st_mode & 0077) != 0 || st.st_size > MAX_BYTES)
		fail("scratch-file-identity", EINVAL, 1);
	return fd;
}

static void
write_exact(int fd, const void *bytes, unsigned int count)
{
	ssize_t written = pwrite(fd, bytes, count, 0);

	checked("pwrite", (long)written, count);
	if (written != (ssize_t)count)
		fail("short-write", EIO, 1);
}

static void
read_exact(int fd, void *bytes, unsigned int count)
{
	ssize_t got = pread(fd, bytes, count, 0);

	checked("pread", (long)got, count);
	if (got != (ssize_t)count)
		fail("short-read", EIO, 1);
}

static int
open_scratch(const char *root, int initialize)
{
	char name[64];
	char manifest[128];
	char actual[128];
	struct stat st;
	int parent = open_parent(root);
	int directory;
	int fd;
	unsigned int count;

	snprintf(name, sizeof(name), "pre1-probe-%s", token);
	count = (unsigned int)snprintf(manifest, sizeof(manifest), "%s\ndata\nnext\nlock\n", token);
	if (initialize)
		checked("mkdirat", mkdirat(parent, name, 0700), 0);
	directory = (int)checked("open-scratch",
							 openat(parent, name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW), 0);
	checked("fstat", fstat(directory, &st), 0);
	if (st.st_uid != geteuid() || (st.st_mode & 0077) != 0)
		fail("scratch-directory-identity", EINVAL, 1);
	fd = owned_file(directory, ".pre1-manifest", initialize ? O_RDWR | O_CREAT | O_EXCL : O_RDONLY);
	if (initialize) {
		write_exact(fd, manifest, count);
		sync_fd(fd);
		sync_fd(directory);
		sync_fd(parent);
	} else {
		checked("fstat", fstat(fd, &st), 0);
		if (st.st_size != (off_t)count)
			fail("manifest-size", EINVAL, 1);
		read_exact(fd, actual, count);
		if (memcmp(actual, manifest, count) != 0)
			fail("manifest-token", EINVAL, 1);
	}
	close_fd(fd);
	close_fd(parent);
	return directory;
}

static uint32_t
crc32(const unsigned char *bytes, unsigned int count)
{
	uint32_t crc = UINT32_MAX;
	unsigned int i;

	for (i = 0; i < count; i++) {
		unsigned int bit;

		crc ^= bytes[i];
		for (bit = 0; bit < 8; bit++)
			crc = (crc >> 1) ^ (UINT32_C(0xedb88320) & (0 - (crc & 1)));
	}
	return ~crc;
}

static void
payload(unsigned char *bytes)
{
	uint32_t state = sequence ^ (writer * UINT32_C(0x9e3779b9));
	unsigned int i;

	memset(bytes, 0, MAX_BYTES);
	for (i = 0; i < TOKEN_BYTES; i++)
		state = state * 33 + (unsigned char)token[i];
	for (i = 0; i < PAGE_BYTES; i++) {
		state = state * UINT32_C(1664525) + UINT32_C(1013904223);
		bytes[i] = (unsigned char)(state >> 24);
	}
	/* Identity is explicit, not merely a hash seed that may alias another
	 * (sequence, writer) tuple. Barrier witnesses read this complete header. */
	for (i = 0; i < 4; i++) {
		bytes[i] = (unsigned char)(sequence >> ((3 - i) * 8));
		bytes[4 + i] = (unsigned char)(writer >> ((3 - i) * 8));
	}
	memcpy(bytes + 8, token, TOKEN_BYTES);
}

static void
verify_fd(int fd)
{
	unsigned char expected[MAX_BYTES];
	unsigned char actual[MAX_BYTES];
	struct stat st;

	payload(expected);
	checked("fstat", fstat(fd, &st), 0);
	if (st.st_size != (off_t)length)
		fail("size-mismatch", EIO, 1);
	read_exact(fd, actual, length);
	if (memcmp(expected, actual, length) != 0)
		fail("content-mismatch", EIO, 1);
	event("verify", 0, 0, length, crc32(actual, length), "OK");
}

/* A missing/wrong barrier cannot produce a successful witness. */
static void
barrier(int new_version)
{
	char line[128];
	char word[8];
	char seen_token[64];
	char seq[32];
	char owner[16];
	char extra;
	int fields;

	event("barrier", 0, 0, 0, 0, "READY");
	if (fgets(line, sizeof(line), stdin) == NULL || strchr(line, '\n') == NULL)
		fail("barrier-eof", 0, 3);
	if (new_version)
		fields = sscanf(line, "%7s %63s %31s %15s %c", word, seen_token, seq, owner, &extra);
	else
		fields = sscanf(line, "%7s %63s %c", word, seen_token, &extra);
	if (fields != (new_version ? 4 : 2) || strcmp(word, "GO") != 0
		|| strcmp(seen_token, token) != 0)
		fail("barrier-token", EINVAL, 3);
	if (new_version) {
		unsigned int next_sequence = number(seq, 1000000000);

		if (next_sequence <= sequence)
			fail("barrier-version-not-advanced", EINVAL, 3);
		sequence = next_sequence;
		writer = number(owner, 3);
	}
	event("barrier", 0, 0, 0, 0, "OK");
}

static void
lock_case(int directory)
{
	int use_fcntl = strncmp(probe_case, "fcntl", 5) == 0;
	int wait_barrier = strstr(probe_case, "-try") == NULL;
	int exit_held = strcmp(probe_case, "flock-exit") == 0;
	int fd = owned_file(directory, "lock", O_RDWR | O_CREAT);
	struct flock request;
	int result;
	int error;

	memset(&request, 0, sizeof(request));
	request.l_type = F_WRLCK;
	request.l_whence = SEEK_SET;
	request.l_start = (off_t)offset;
	request.l_len = (off_t)length;
	result = use_fcntl ? fcntl(fd, F_SETLK, &request) : flock(fd, LOCK_EX | LOCK_NB);
	error = result < 0 ? errno : 0;
	if (result < 0 && (error == EAGAIN || error == EACCES || error == EWOULDBLOCK)) {
		event(use_fcntl ? "fcntl" : "flock", result, error, length, 0, "CONFLICT");
		exit(4);
	}
	checked(use_fcntl ? "fcntl" : "flock", result, length);
	if (wait_barrier)
		barrier(0);
	if (exit_held) {
		event("normal-exit-held", 0, 0, length, 0, "PASS");
		exit(0);
	}
	request.l_type = F_UNLCK;
	checked(use_fcntl ? "fcntl-unlock" : "flock-unlock",
			use_fcntl ? fcntl(fd, F_SETLK, &request) : flock(fd, LOCK_UN), length);
	close_fd(fd);
}

static void
data_case(int directory)
{
	int fd;

	if (strcmp(probe_case, "write") == 0 || strcmp(probe_case, "rename") == 0) {
		unsigned char bytes[MAX_BYTES];
		int replace = strcmp(probe_case, "rename") == 0;

		fd = owned_file(directory, replace ? "next" : "data",
						O_RDWR | O_CREAT | (replace ? O_EXCL : 0));
		payload(bytes);
		write_exact(fd, bytes, PAGE_BYTES);
		checked("ftruncate", ftruncate(fd, PAGE_BYTES), PAGE_BYTES);
		sync_fd(fd);
		close_fd(fd);
		if (replace)
			checked("renameat", renameat(directory, "next", directory, "data"), 0);
		sync_fd(directory);
		event("payload", 0, 0, PAGE_BYTES, crc32(bytes, PAGE_BYTES), "OK");
		return;
	}
	if (strcmp(probe_case, "unlink") == 0) {
		fd = owned_file(directory, "data", O_RDONLY);
		close_fd(fd);
		checked("unlinkat", unlinkat(directory, "data", 0), 0);
		sync_fd(directory);
		return;
	}
	fd = owned_file(directory, "data", strcmp(probe_case, "resize") == 0 ? O_RDWR : O_RDONLY);
	if (strcmp(probe_case, "resize") == 0) {
		checked("ftruncate", ftruncate(fd, (off_t)length), length);
		sync_fd(fd);
	} else {
		verify_fd(fd);
		if (strcmp(probe_case, "cache-reader") == 0 || strcmp(probe_case, "rename-reader") == 0) {
			unsigned int old_sequence = sequence;
			unsigned int old_writer = writer;
			unsigned int new_sequence;
			unsigned int new_writer;
			struct stat old_st;
			struct stat new_st;
			int same_inode;
			int new_fd;

			barrier(1);
			new_sequence = sequence;
			new_writer = writer;
			if (strcmp(probe_case, "rename-reader") == 0) {
				sequence = old_sequence;
				writer = old_writer;
			}
			verify_fd(fd);
			sequence = new_sequence;
			writer = new_writer;
			new_fd = owned_file(directory, "data", O_RDONLY);
			checked("fstat", fstat(fd, &old_st), 0);
			checked("fstat", fstat(new_fd, &new_st), 0);
			same_inode = old_st.st_dev == new_st.st_dev && old_st.st_ino == new_st.st_ino;
			if (same_inode != (strcmp(probe_case, "cache-reader") == 0))
				fail("unexpected-inode-identity", EINVAL, 1);
			event("same-inode", same_inode, 0, 0, 0, "OK");
			verify_fd(new_fd);
			close_fd(new_fd);
		} else if (strcmp(probe_case, "unlink-reader") == 0) {
			struct stat st;
			int result;
			int error;

			barrier(0);
			verify_fd(fd);
			result = fstatat(directory, "data", &st, AT_SYMLINK_NOFOLLOW);
			error = result < 0 ? errno : 0;
			event("fstatat-unlinked", result, error, 0, 0, error == ENOENT ? "OK" : "FAIL");
			if (result != -1 || error != ENOENT)
				exit(1);
		}
	}
	close_fd(fd);
}

/* The controller must prove exact guest OFF independently. Neither process
 * death nor this bounded loop ending grants an isolation certificate. */
static void
fence_writer(int directory)
{
	unsigned char bytes[MAX_BYTES];
	int lock = owned_file(directory, "lock", O_RDWR | O_CREAT);
	int fd;
	unsigned int i;

	checked("flock", flock(lock, LOCK_EX | LOCK_NB), PAGE_BYTES);
	fd = owned_file(directory, "data", O_RDWR | O_CREAT | O_EXCL);
	for (i = 0; i < iterations; i++) {
		struct timespec pause = { 0, 100000000 };

		sequence++;
		payload(bytes);
		write_exact(fd, bytes, PAGE_BYTES);
		sync_fd(fd);
		if (i == 0)
			sync_fd(directory);
		event("fence-progress", (long)sequence, 0, PAGE_BYTES, crc32(bytes, PAGE_BYTES),
			  i == 0 ? "READY" : "SYNCED");
		while (nanosleep(&pause, &pause) != 0) {
			if (errno != EINTR)
				fail("nanosleep", errno, 3);
		}
	}
	fail("witness-budget-exhausted", ETIMEDOUT, 3);
}

/* Used only after a controller barrier. Infer the recorded version, but verify
 * the complete deterministic bytes, token and writer before reporting it. */
static void
observe_case(int directory)
{
	unsigned char header[8];
	int fd = owned_file(directory, "data", O_RDONLY);
	unsigned int i;

	read_exact(fd, header, sizeof(header));
	sequence = writer = 0;
	for (i = 0; i < 4; i++) {
		sequence = (sequence << 8) | header[i];
		writer = (writer << 8) | header[4 + i];
	}
	if (sequence > 1000000000 || writer > 3)
		fail("payload-identity", EINVAL, 1);
	verify_fd(fd);
	event("observed-version", sequence, 0, PAGE_BYTES, 0, "OK");
	event("observed-writer", writer, 0, PAGE_BYTES, 0, "OK");
	close_fd(fd);
}

/* Explicitly authorized small scratch filesystem only. Filling the byte budget
 * without a real ENOSPC is incomplete; no file is removed or reused on return. */
static void
capacity_case(int directory)
{
	struct stat st;
	struct statvfs fs;
	unsigned char bytes[MAX_BYTES];
	int fd;
	int exhausted = 0;

	checked("fstat", fstat(directory, &st), 0);
	checked("fstatvfs", fstatvfs(directory, &fs), 0);
	if ((uintmax_t)st.st_dev != capacity_device || fs.f_frsize == 0
		|| fs.f_blocks > capacity_bytes / fs.f_frsize
		|| fs.f_blocks * fs.f_frsize != capacity_bytes)
		fail("capacity-filesystem-identity", EINVAL, 1);
	fd = owned_file(directory, "data", O_RDWR | O_CREAT | O_EXCL);
	sync_fd(directory);
	payload(bytes);
	while (offset < capacity_bytes) {
		unsigned int count = capacity_bytes - offset;
		ssize_t written;
		int error;

		if (count > MAX_BYTES)
			count = MAX_BYTES;
		written = pwrite(fd, bytes, count, (off_t)offset);
		error = written < 0 ? errno : 0;
		if (written < 0) {
			event("pwrite", written, error, count, 0, error == ENOSPC ? "EXPECTED_ENOSPC" : "FAIL");
			if (error != ENOSPC)
				exit(1);
			exhausted = 1;
			break;
		}
		if (written == 0 || written > (ssize_t)count)
			fail("capacity-write-no-progress", EIO, 1);
		offset += (unsigned int)written;
		/* Flush regularly so delayed allocation cannot hide the capacity error. */
		if (offset % (2U * 1024U * 1024U) == 0) {
			int result = fsync(fd);

			error = result < 0 ? errno : 0;
			event("fsync", result, error, 0, 0,
				  error == ENOSPC ? "EXPECTED_ENOSPC"
				  : result < 0	  ? "FAIL"
								  : "OK");
			if (result < 0) {
				if (error != ENOSPC)
					exit(1);
				exhausted = 1;
				break;
			}
		}
	}
	{
		int result = fsync(fd);
		int error = result < 0 ? errno : 0;

		event("fsync", result, error, 0, 0,
			  error == ENOSPC ? "EXPECTED_ENOSPC"
			  : result < 0	  ? "FAIL"
							  : "OK");
		if (result < 0 && error != ENOSPC)
			exit(1);
		if (error == ENOSPC)
			exhausted = 1;
	}
	close_fd(fd);
	close_fd(directory);
	if (!exhausted)
		fail("capacity-budget-exhausted", 0, 3);
	event("complete", 0, 0, offset, 0, "EXPECTED_INJECTION");
	exit(4);
}

int
main(int argc, char **argv)
{
	static const char option_keys[] = "crtnwsloibd";
	static const struct option options[] = { { "case", required_argument, NULL, 'c' },
											 { "root", required_argument, NULL, 'r' },
											 { "token", required_argument, NULL, 't' },
											 { "node", required_argument, NULL, 'n' },
											 { "writer", required_argument, NULL, 'w' },
											 { "sequence", required_argument, NULL, 's' },
											 { "length", required_argument, NULL, 'l' },
											 { "offset", required_argument, NULL, 'o' },
											 { "iterations", required_argument, NULL, 'i' },
											 { "capacity-bytes", required_argument, NULL, 'b' },
											 { "capacity-device", required_argument, NULL, 'd' },
											 { NULL, 0, NULL, 0 } };
	static const char *cases[]
		= { "init",		 "write",		 "read",		  "resize",		   "rename",
			"unlink",	 "cache-reader", "rename-reader", "unlink-reader", "flock-hold",
			"flock-try", "flock-exit",	 "fcntl-hold",	  "fcntl-try",	   "fence-writer",
			"observe",	 "capacity-fill" };
	const char *root = NULL;
	const char *case_arg = NULL;
	const char *token_arg = NULL;
	unsigned int seen = 0;
	unsigned int i;
	int option;
	int directory;
	int found = 0;

	opterr = 0;
	umask(0077);
	while ((option = getopt_long(argc, argv, "", options, NULL)) != -1) {
		const char *key = strchr(option_keys, option);
		unsigned int bit;

		if (key == NULL || option == 0)
			fail("arguments", EINVAL, 2);
		bit = 1U << (unsigned int)(key - option_keys);
		if ((seen & bit) != 0)
			fail("arguments", EINVAL, 2);
		seen |= bit;
		switch (option) {
		case 'c':
			case_arg = optarg;
			break;
		case 'r':
			root = optarg;
			break;
		case 't':
			token_arg = optarg;
			break;
		case 'n':
			node = number(optarg, 3);
			break;
		case 'w':
			writer = number(optarg, 3);
			break;
		case 's':
			sequence = number(optarg, 1000000000);
			break;
		case 'l':
			length = number(optarg, MAX_BYTES);
			break;
		case 'o':
			offset = number(optarg, MAX_BYTES);
			break;
		case 'i':
			iterations = number(optarg, 1200);
			break;
		case 'b':
			capacity_bytes = number(optarg, 1073741824);
			break;
		case 'd': {
			char *end;

			if (*optarg == '\0' || strspn(optarg, "0123456789") != strlen(optarg))
				fail("arguments", EINVAL, 2);
			errno = 0;
			capacity_device = strtoumax(optarg, &end, 10);
			if (errno != 0 || *end != '\0')
				fail("arguments", EINVAL, 2);
			break;
		}
		default:
			fail("arguments", EINVAL, 2);
		}
	}
	if ((seen & 15) != 15 || optind != argc || strlen(token_arg) != TOKEN_BYTES
		|| strspn(token_arg, "0123456789abcdef") != TOKEN_BYTES)
		fail("arguments", EINVAL, 2);
	for (i = 0; i < sizeof(cases) / sizeof(cases[0]); i++)
		if (strcmp(case_arg, cases[i]) == 0) {
			probe_case = cases[i];
			found = 1;
			break;
		}
	if (!found)
		fail("arguments", EINVAL, 2);
	token = token_arg;
	if (strcmp(probe_case, "capacity-fill") == 0) {
		if (seen != (15U | 512U | 1024U) || capacity_bytes == 0)
			fail("arguments", EINVAL, 2);
	} else if ((seen & (512U | 1024U)) != 0)
		fail("arguments", EINVAL, 2);
	if ((seen & 16) == 0)
		writer = node;
	if (offset != 0 && strncmp(probe_case, "fcntl", 5) != 0)
		fail("arguments", EINVAL, 2);
	if (strstr(probe_case, "-reader") != NULL && length < 8 + TOKEN_BYTES)
		fail("arguments", EINVAL, 2);
	if (strstr(probe_case, "fcntl") != NULL && (length == 0 || length + offset > MAX_BYTES))
		fail("arguments", EINVAL, 2);
	if ((seen & 256) != 0 && strcmp(probe_case, "fence-writer") != 0)
		fail("arguments", EINVAL, 2);
	if ((strcmp(probe_case, "fence-writer") == 0
		 && (iterations == 0 || writer != node || sequence > 1000000000 - iterations))
		|| ((strcmp(probe_case, "observe") == 0 || strcmp(probe_case, "fence-writer") == 0)
			&& length != PAGE_BYTES))
		fail("arguments", EINVAL, 2);
	directory = open_scratch(root, strcmp(probe_case, "init") == 0);
	if (strcmp(probe_case, "capacity-fill") == 0)
		capacity_case(directory);
	else if (strcmp(probe_case, "fence-writer") == 0)
		fence_writer(directory);
	else if (strcmp(probe_case, "observe") == 0)
		observe_case(directory);
	else if (strncmp(probe_case, "flock", 5) == 0 || strncmp(probe_case, "fcntl", 5) == 0)
		lock_case(directory);
	else if (strcmp(probe_case, "init") != 0)
		data_case(directory);
	close_fd(directory);
	event("complete", 0, 0, 0, 0, "PASS");
	return 0;
}
