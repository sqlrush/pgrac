/*-------------------------------------------------------------------------
 * cluster_relmap_file_io.c
 *    Exact, control-bound relmap pair reads and checkpoint readback.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/common/cluster_relmap_file_io.c
 * NOTES
 *    Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *-------------------------------------------------------------------------
 */
#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif
#include "common/cluster_relmap_file.h"

#ifdef WIN32
/* Managed *at-based custody is not implemented by the Windows port. */
bool
cluster_relmap_file_pair_read_v2(int directory,
							   const ClusterRecoveryFileIdentityV2 *identity,
							   ClusterRelmapFilePairV2 *output)
{
	return false;
}

bool
cluster_relmap_file_pair_checkpoint_v2(int directory,
									 const ClusterRecoveryFileIdentityV2 *identity,
									 ClusterRelmapFilePairV2 *output)
{
	return false;
}
bool
cluster_relmap_file_pair_create_v2(int directory,
	const ClusterRecoveryFileIdentityV2 *identity, const RelMapFile *committed,
	ClusterRelmapFilePairV2 *output)
{
	return false;
}
bool
cluster_relmap_file_pair_replay_create_v2(int directory,
	const ClusterRecoveryFileIdentityV2 *identity, const RelMapFile *committed,
	ClusterRelmapFilePairV2 *output)
{
	return false;
}
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "common/cluster_relmap_file.h"
#ifndef FRONTEND
#include "miscadmin.h"
#include "storage/fd.h"
#endif

static int
managed_sync(int fd)
{
#ifdef FRONTEND
	return fsync(fd);
#else
	if (!enableFsync)
		return -1;
	return pg_fsync(fd);
#endif
}

static bool
managed_overlap(const void *a, size_t alen, const void *b, size_t blen)
{
	uintptr_t x = (uintptr_t) a, y = (uintptr_t) b;
	return x <= y ? y - x < alen : x - y < blen;
}

static const char *const managed_names[] = {
	"pgrac_relmap_authority", "pgrac_relmap_authority.bak"
};

static bool
managed_same_file(const struct stat *a, const struct stat *b)
{
	return a->st_dev == b->st_dev && a->st_ino == b->st_ino
		&& a->st_size == b->st_size && a->st_mode == b->st_mode
		&& a->st_uid == b->st_uid && a->st_nlink == b->st_nlink
		&& a->st_mtime == b->st_mtime && a->st_ctime == b->st_ctime;
}

static bool
managed_file_safe(const struct stat *st)
{
	return S_ISREG(st->st_mode) && st->st_nlink == 1
		&& st->st_uid == geteuid() && (st->st_mode & 0022) == 0
		&& st->st_size == CLUSTER_RELMAP_FILE_SIZE_V2;
}

static bool
managed_directory_safe(int directory)
{
	struct stat st;
	return fstat(directory, &st) == 0 && S_ISDIR(st.st_mode)
		&& st.st_uid == geteuid() && (st.st_mode & 0022) == 0;
}

static bool
managed_current(int directory, int which, int fd, const struct stat *before)
{
	struct stat held, named;
	return fstat(fd, &held) == 0 && managed_file_safe(&held)
		&& fstatat(directory, managed_names[which], &named, AT_SYMLINK_NOFOLLOW) == 0
		&& managed_same_file(before, &held) && managed_same_file(&held, &named);
}

static bool
managed_read(int directory, int which, int fd, const struct stat *st, void *bytes)
{
	size_t done = 0;
	while (done < CLUSTER_RELMAP_FILE_SIZE_V2)
	{
		ssize_t n = pread(fd, (char *) bytes + done,
						  CLUSTER_RELMAP_FILE_SIZE_V2 - done, done);
		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			return false;
		done += n;
	}
	return managed_current(directory, which, fd, st);
}

static bool
managed_pair_observe(int directory, const ClusterRecoveryFileIdentityV2 *identity,
					 ClusterRelmapFilePairV2 *output, bool checkpoint)
{
	int fds[2] = {-1, -1};
	struct stat st[2];
	ClusterRelmapFilePairV2 pair;
	ClusterRelmapV2Image images[2];
	ClusterRecoveryFileIdentityV2 bound;
	bool ok = false;

	if (output == NULL || identity == NULL
		|| managed_overlap(output, sizeof(*output), identity, sizeof(*identity))
		|| !managed_directory_safe(directory))
		return false;
	memcpy(&bound, identity, sizeof(bound));
	for (int i = 0; i < 2; ++i)
	{
		fds[i] = openat(directory, managed_names[i],
			(checkpoint ? O_RDWR : O_RDONLY) | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC | PG_BINARY);
		if (fds[i] < 0 || fstat(fds[i], &st[i]) != 0 || !managed_file_safe(&st[i])
			|| !managed_current(directory, i, fds[i], &st[i]))
			goto done;
	}
	if (st[0].st_dev == st[1].st_dev && st[0].st_ino == st[1].st_ino)
		goto done;
	if (checkpoint && (managed_sync(fds[0]) != 0 || managed_sync(fds[1]) != 0 || managed_sync(directory) != 0))
		goto done;
	for (int i = 0; i < 2; ++i)
		if (!managed_read(directory, i, fds[i], &st[i], pair.bytes[i])
			|| !cluster_relmap_file_decode_v2(pair.bytes[i], sizeof(pair.bytes[i]), &bound, &images[i]))
			goto done;
	if (cluster_relmap_v2_read_pair(pair.bytes[0] + CLUSTER_RELMAP_FILE_HEADER_SIZE_V2,
			CLUSTER_RELMAP_V2_FILE_SIZE, pair.bytes[1] + CLUSTER_RELMAP_FILE_HEADER_SIZE_V2,
			CLUSTER_RELMAP_V2_FILE_SIZE, &images[0].identity, &pair.view)
		!= CLUSTER_RELMAP_V2_PAIR_VALID || !pair.view.pending_complete)
		goto done;
	/* Revisit the first copy after reading the second, without dropping either
	 * descriptor. No result escapes from a replaced or changed pair. */
	for (int i = 0; i < 2; ++i)
	{
		unsigned char actual[CLUSTER_RELMAP_FILE_SIZE_V2];
		if (!managed_read(directory, i, fds[i], &st[i], actual)
			|| memcmp(actual, pair.bytes[i], sizeof(actual)) != 0)
			goto done;
	}
	ok = managed_directory_safe(directory)
		&& managed_current(directory, 0, fds[0], &st[0])
		&& managed_current(directory, 1, fds[1], &st[1])
		&& cluster_relmap_file_decode_v2(pair.bytes[0], sizeof(pair.bytes[0]), identity, &images[0]);
done:
	for (int i = 0; i < 2; ++i)
		if (fds[i] >= 0 && close(fds[i]) != 0)
			ok = false;
	if (ok)
		ok = cluster_relmap_file_decode_v2(pair.bytes[0], sizeof(pair.bytes[0]), identity, &images[0]);
	if (ok)
		*output = pair;
	return ok;
}

bool
cluster_relmap_file_pair_read_v2(int directory, const ClusterRecoveryFileIdentityV2 *identity,
								ClusterRelmapFilePairV2 *output)
{
	return managed_pair_observe(directory, identity, output, false);
}

bool
cluster_relmap_file_pair_checkpoint_v2(int directory, const ClusterRecoveryFileIdentityV2 *identity,
									  ClusterRelmapFilePairV2 *output)
{
	return managed_pair_observe(directory, identity, output, true);
}

bool
cluster_relmap_file_pair_create_v2(int directory,
	const ClusterRecoveryFileIdentityV2 *identity, const RelMapFile *committed,
	ClusterRelmapFilePairV2 *output)
{
	ClusterRelmapV2Image image = {0};
	ClusterRelmapFilePairV2 pair;
	ClusterRecoveryFileIdentityV2 bound;
	struct stat st[2], named;
	int fds[2] = {-1, -1};
	bool ok = false;

	if (!identity || !committed || !output
		|| managed_overlap(identity, sizeof(*identity), output, sizeof(*output))
		|| managed_overlap(committed, sizeof(*committed), output, sizeof(*output))
		|| !managed_directory_safe(directory))
		return false;
#ifndef FRONTEND
	if (!enableFsync) return false;
#endif
	bound = *identity;
	image.identity = (ClusterRelmapV2Namespace){bound.system_identifier,
		bound.database_incarnation, bound.database_oid, bound.database_oid == InvalidOid};
	image.committed_generation = 1;
	image.committed = *committed;
	if (!cluster_relmap_file_encode_v2(&bound, &image, pair.bytes[0], sizeof(pair.bytes[0])))
		return false;
	memcpy(pair.bytes[1], pair.bytes[0], sizeof(pair.bytes[0]));
	/* Refuse an existing backup before touching primary. O_EXCL also closes
	 * the race after this whole-pair precheck. No repair or overwrite path. */
	for (int i = 0; i < 2; ++i)
		if (fstatat(directory, managed_names[i], &named, AT_SYMLINK_NOFOLLOW) == 0
			|| errno != ENOENT)
			return false;
	for (int i = 0; i < 2; ++i) {
		size_t done = 0;
		fds[i] = openat(directory, managed_names[i],
			O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC | PG_BINARY, 0600);
		if (fds[i] < 0) goto done;
		while (done < sizeof(pair.bytes[i])) {
			ssize_t n = pwrite(fds[i], pair.bytes[i] + done, sizeof(pair.bytes[i]) - done, done);
			if (n < 0 && errno == EINTR) continue;
			if (n <= 0) goto done;
			done += n;
		}
		if (managed_sync(fds[i]) != 0 || fstat(fds[i], &st[i]) != 0
			|| !managed_file_safe(&st[i]) || !managed_current(directory, i, fds[i], &st[i]))
			goto done;
	}
	if ((st[0].st_dev == st[1].st_dev && st[0].st_ino == st[1].st_ino)
		|| managed_sync(directory) != 0)
		goto done;
	for (int pass = 0; pass < 2; ++pass)
		for (int i = 0; i < 2; ++i) {
			unsigned char actual[CLUSTER_RELMAP_FILE_SIZE_V2];
			if (!managed_read(directory, i, fds[i], &st[i], actual)
				|| memcmp(actual, pair.bytes[i], sizeof(actual)))
				goto done;
		}
	ok = managed_directory_safe(directory)
		&& managed_current(directory, 0, fds[0], &st[0])
		&& managed_current(directory, 1, fds[1], &st[1])
		&& cluster_relmap_v2_read_pair(pair.bytes[0] + CLUSTER_RELMAP_FILE_HEADER_SIZE_V2,
			CLUSTER_RELMAP_V2_FILE_SIZE, pair.bytes[1] + CLUSTER_RELMAP_FILE_HEADER_SIZE_V2,
			CLUSTER_RELMAP_V2_FILE_SIZE, &image.identity, &pair.view) == CLUSTER_RELMAP_V2_PAIR_VALID;
done:
	for (int i = 0; i < 2; ++i)
		if (fds[i] >= 0 && close(fds[i]) != 0) ok = false;
	if (ok) ok = !memcmp(identity, &bound, sizeof(bound));
	if (ok) *output = pair;
	return ok;
}

bool
cluster_relmap_file_pair_replay_create_v2(int directory,
	const ClusterRecoveryFileIdentityV2 *identity, const RelMapFile *committed,
	ClusterRelmapFilePairV2 *output)
{
	ClusterRelmapV2Image image = {0};
	ClusterRelmapFilePairV2 pair;
	ClusterRecoveryFileIdentityV2 bound;
	unsigned char expected[CLUSTER_RELMAP_FILE_SIZE_V2];
	struct stat named;
	unsigned present = 0;
	bool ok;

	if (!identity || !committed || !output
		|| managed_overlap(identity, sizeof(*identity), output, sizeof(*output))
		|| managed_overlap(committed, sizeof(*committed), output, sizeof(*output)))
		return false;
	bound = *identity;
	image.identity = (ClusterRelmapV2Namespace){bound.system_identifier,
		bound.database_incarnation, bound.database_oid, bound.database_oid == InvalidOid};
	image.committed_generation = 1;
	image.committed = *committed;
	/* Validate the complete replay image before even probing target names. */
	if (!cluster_relmap_file_encode_v2(&bound, &image, expected, sizeof(expected))
		|| !managed_directory_safe(directory))
		return false;
	for (int i = 0; i < 2; ++i) {
		if (fstatat(directory, managed_names[i], &named, AT_SYMLINK_NOFOLLOW) == 0)
			++present;
		else if (errno != ENOENT)
			return false;
	}
	/* The original creator owns uncertainty. Never fill a missing half or
	 * replace any prior material, even if its surviving bytes look expected. */
	if (!present)
		ok = cluster_relmap_file_pair_create_v2(directory, &bound, &image.committed, &pair);
	else if (present == 2)
		ok = managed_pair_observe(directory, &bound, &pair, true);
	else
		return false;
	if (!ok
		|| memcmp(pair.bytes[0], expected, sizeof(expected))
		|| memcmp(pair.bytes[1], expected, sizeof(expected))
		|| memcmp(identity, &bound, sizeof(bound))
		|| memcmp(committed, &image.committed, sizeof(*committed)))
		return false;
	*output = pair;
	return true;
}

#endif /* WIN32 */
