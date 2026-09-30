/*-------------------------------------------------------------------------
 *
 * cluster_wal_restart_read.c
 *    Open restart WAL without consulting the writable pg_wal route.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/backend/cluster/cluster_wal_restart_read.c
 *
 * NOTES
 *    PGRAC-original file. Claim authentication precedes raw descriptor use;
 *    exact bytes and namespace are rechecked through pinned descriptors.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog_internal.h"
#include "cluster/cluster_wal_restart_read.h"
#include "cluster/cluster_wal_thread.h"

static bool
restart_root_valid(const char *path)
{
	const char *part;

	if (path == NULL || path[0] != '/' || path[1] == '\0' || strlen(path) >= MAXPGPATH)
		return false;
	part = path + 1;
	while (*part) {
		size_t len = strcspn(part, "/");
		if (len == 0 || (len == 1 && part[0] == '.')
			|| (len == 2 && part[0] == '.' && part[1] == '.'))
			return false;
		part += len;
		if (*part == '/' && *++part == '\0')
			return false;
	}
	return true;
}

static bool
restart_owned(const struct stat *st, bool directory)
{
	return st->st_uid == geteuid() && !(st->st_mode & (S_IWGRP | S_IWOTH))
		   && (directory ? S_ISDIR(st->st_mode) : (S_ISREG(st->st_mode) && st->st_nlink == 1));
}

static bool
restart_same_entry(int parent, const char *name, int fd, bool directory)
{
	struct stat actual, named;

	return fstat(fd, &actual) == 0 && restart_owned(&actual, directory)
		   && fstatat(parent, name, &named, AT_SYMLINK_NOFOLLOW) == 0
		   && restart_owned(&named, directory) && actual.st_dev == named.st_dev
		   && actual.st_ino == named.st_ino;
}

static bool
restart_claim_matches(int generation, const uint8 *expected)
{
	uint8 bytes[CLUSTER_WAL_CLAIM_V2_BYTES + 1];
	struct stat st;
	size_t used = 0;
	bool ok = false;
	int fd = openat(generation, CLUSTER_WAL_THREAD_CLAIM_FILENAME,
					O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK | PG_BINARY);

	if (fd < 0)
		return false;
	if (fstat(fd, &st) != 0 || !restart_owned(&st, false)
		|| st.st_size != CLUSTER_WAL_CLAIM_V2_BYTES)
		goto done;
	while (used < sizeof(bytes)) {
		ssize_t n = read(fd, bytes + used, sizeof(bytes) - used);
		if (n < 0 && errno == EINTR)
			continue;
		if (n < 0)
			goto done;
		if (n == 0)
			break;
		used += n;
	}
	ok = used == CLUSTER_WAL_CLAIM_V2_BYTES && memcmp(bytes, expected, used) == 0
		 && restart_same_entry(generation, CLUSTER_WAL_THREAD_CLAIM_FILENAME, fd, false);
done:
	if (close(fd) != 0)
		ok = false;
	return ok;
}

/* No allocator, ereport or other error-throwing backend call with FDs open. */
static ClusterControlRootResult
restart_open_selected(const char *wal_root, const ClusterWalSourceRef *input,
					  XLogSegNo segno, int segsize, const uint8 *claim, int *fd_out)
{
	const int dirflags = O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC;
	int dirs[3] = { -1, -1, -1 }, fd = -1;
	char thread[32], generation[48], segment[MAXFNAMELEN];
	const char *parts[3] = { wal_root, thread, generation };
	struct stat st;
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_IO_ERROR;

	snprintf(thread, sizeof(thread), "thread_%u", input->claim.identity.origin_thread_id);
	snprintf(generation, sizeof(generation), "generation_" UINT64_FORMAT,
			 input->claim.identity.origin_owner_incarnation);
	XLogFileName(segment, input->timeline, segno, segsize);
	for (size_t i = 0; i < lengthof(dirs); ++i) {
		int parent = i == 0 ? AT_FDCWD : dirs[i - 1];
		dirs[i] = openat(parent, parts[i], dirflags);
		if (dirs[i] < 0 || !restart_same_entry(parent, parts[i], dirs[i], true))
			goto done;
	}
	if (!restart_claim_matches(dirs[2], claim))
		goto done;
	fd = openat(dirs[2], segment, O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK | PG_BINARY);
	if (fd < 0) {
		if (errno != ENOENT)
			goto done;
		result = CLUSTER_CONTROL_ROOT_ABSENT;
	} else {
		if (fstat(fd, &st) != 0 || !restart_owned(&st, false))
			goto done;
		if (st.st_size != segsize) {
			result = CLUSTER_CONTROL_ROOT_BAD_SIZE;
			goto done;
		}
		result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	}
	for (size_t i = 0; i < lengthof(dirs); ++i) {
		if (!restart_same_entry(i == 0 ? AT_FDCWD : dirs[i - 1], parts[i], dirs[i], true)) {
			result = CLUSTER_CONTROL_ROOT_POSTREAD_FAILED;
			goto done;
		}
	}
	if (!restart_claim_matches(dirs[2], claim)
		|| (fd >= 0 && !restart_same_entry(dirs[2], segment, fd, false)))
		result = CLUSTER_CONTROL_ROOT_POSTREAD_FAILED;
done:
	for (size_t i = 0; i < lengthof(dirs); ++i)
		if (dirs[i] >= 0 && close(dirs[i]) != 0)
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		*fd_out = fd;
	else if (fd >= 0 && close(fd) != 0)
		result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	return result;
}

ClusterControlRootResult
cluster_wal_restart_segment_open(const char *wal_root, const ClusterWalSourceRef *input,
								 TimeLineID timeline, XLogSegNo segno, int segsize, int *fd_out)
{
	ClusterWalThreadClaimV2 claim;
	uint8 bytes[CLUSTER_WAL_CLAIM_V2_BYTES];
	ClusterControlRootResult result;

	if (fd_out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	*fd_out = -1;
	if (!restart_root_valid(wal_root) || input == NULL || timeline == 0
		|| !IsValidWalSegSize(segsize) || segno == 0 || segno > PG_UINT64_MAX / segsize)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (input->timeline != timeline)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	result = cluster_wal_claim_v2_read(wal_root, &input->claim, &claim);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result == CLUSTER_CONTROL_ROOT_ABSENT ? CLUSTER_CONTROL_ROOT_IO_ERROR : result;
	result = cluster_wal_claim_v2_encode(&claim, bytes);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	return restart_open_selected(wal_root, input, segno, segsize, bytes, fd_out);
}
