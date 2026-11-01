/* ROOTv4's explicit registry selection. Original v3 records keep their codec. */
#include "postgres.h"
#include "cluster_control_root_registry_private.h"
#include "cluster/cluster_cf_enqueue.h"
#include "port/pg_crc32c.h"

static uint64 get_le(const uint8 *p, Size at, unsigned width)
{
	uint64 n = 0;
	for (unsigned i = 0; i < width; i++)
		n |= (uint64)p[at + i] << (8 * i);
	return n;
}
static uint64 get_be(const uint8 *p, Size at, unsigned width)
{
	uint64 n = 0;
	for (unsigned i = 0; i < width; i++)
		n = (n << 8) | p[at + i];
	return n;
}
static void put_le(uint8 *p, Size at, uint64 n, unsigned width)
{
	for (unsigned i = 0; i < width; i++)
		p[at + i] = (uint8)(n >> (8 * i));
}
static bool zero(const uint8 *p, Size n)
{
	for (Size i = 0; i < n; i++)
		if (p[i])
			return false;
	return true;
}
static uint32 root_crc(const uint8 *p, Size n)
{
	pg_crc32c c;
	INIT_CRC32C(c);
	COMP_CRC32C(c, p, n);
	FIN_CRC32C(c);
	return c;
}
static bool overlaps(const void *a, Size na, const void *b, Size nb)
{
	uintptr_t x = (uintptr_t)a, y = (uintptr_t)b;
	return a && b && na && nb && (x <= y ? y - x < na : x - y < nb);
}
static bool output_distinct(const ClusterRecoveryRegistryV2 *r, const void *out, Size n)
{
	const ClusterRecoveryCheckpointCoverageV2 *c = &r->coverage;
	if (overlaps(r, sizeof(*r), out, n) ||
		overlaps(c->sources, c->source_count * sizeof(*c->sources), out, n) ||
		overlaps(c->files, c->file_count * sizeof(*c->files), out, n))
		return false;
	for (uint64 i = 0; i < c->file_count; i++)
		if (overlaps(c->files[i].sources, c->files[i].source_count * sizeof(*c->files[i].sources),
					 out, n))
			return false;
	return true;
}
/* Decode the original fields with their original validator, in private scratch.
 * No normalized bytes or downgraded token are ever returned to a caller. */
static bool decode(const uint8 *p, Size n, uint64 sysid, const uint8 storage[16],
				   ControlRootImage *old)
{
	uint64 target, source;
	if (!p || n != 66560 || memcmp(p, "PGCH", 4) || get_le(p, 4, 2) != 4 ||
		get_le(p, 6, 2) != 1024 || get_le(p, 8, 2) != 512 || get_le(p, 10, 2) != 128 ||
		get_le(p, 72, 2) != 4 || get_le(p, 74, 2) != 4 || !zero(p + 376, 136) ||
		!zero(p + 640, 380) || get_le(p, 1020, 4) != root_crc(p, 1020) ||
		get_le(p, 96, 4) != root_crc(p + 1024, 65536) ||
		!cluster_recovery_registry_reference_valid_v2(p + 512) ||
		get_be(p + 512, 16, 8) != get_le(p, 16, 8))
		return false;
	target = get_le(p, 188, 8);
	source = get_le(p, 180, 8);
	if (!(target & CLUSTER_RECOVERY_CONTROL_ROOT_FEATURE) ||
		(get_le(p, 64, 8) == CLUSTER_CONTROL_ROOT_FORMAT_CREATION_FLAGS_V1 && source != 0))
		return false;
	memset(old, 0, sizeof(*old));
	memcpy(old->bytes, p, 376);
	memcpy(old->bytes + 512, p + 1024, 65536);
	put_le(old->bytes, 4, 3, 2);
	put_le(old->bytes, 6, 512, 2);
	put_le(old->bytes, 72, 3, 2);
	put_le(old->bytes, 74, 3, 2);
	put_le(old->bytes, 180, source & ~CLUSTER_RECOVERY_CONTROL_ROOT_FEATURE, 8);
	put_le(old->bytes, 188, target & ~CLUSTER_RECOVERY_CONTROL_ROOT_FEATURE, 8);
	put_le(old->bytes, 504, root_crc(old->bytes, 504), 4);
	return cluster_control_root_v3_decode(old->bytes, sizeof(old->bytes), storage, sysid, old) ==
		   CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
static bool common_matches(const ControlRootImage *root, const ClusterRecoveryRegistryV2 *r)
{
	if (root->header.system_identifier != r->system_identifier ||
		memcmp(root->header.storage_uuid, r->storage_uuid, 16) ||
		root->header.v2.database_incarnation != r->database_incarnation ||
		root->header.file_txn_seq != r->coverage.control_generation)
		return false;
	for (uint64 i = 0; i < r->coverage.source_count; i++)
		if (memcmp(r->coverage.sources[i].source.claim.identity.authority_uuid,
				   root->header.authority_uuid, 16))
			return false;
	return true;
}
static bool reference_matches(const ClusterRecoveryRegistryV2 *r, const uint8 ref[128])
{
	uint8 *bytes, expected[128];
	Size n;
	bool ok;
	if (!cluster_recovery_registry_reference_valid_v2(ref) ||
		!cluster_recovery_registry_size_v2(r, (Size)get_be(ref, 48, 8), &n))
		return false;
	bytes = malloc(n);
	if (!bytes)
		return false;
	ok = cluster_recovery_registry_encode_v2(r, bytes, n, expected) && !memcmp(ref, expected, 128);
	free(bytes);
	return ok;
}
bool cluster_control_root_registry_encode_v4(const ControlRootImage *candidate,
											 const ClusterRecoveryRegistryV2 *r,
											 const uint8 ref[128], uint8 *out, Size n)
{
	ControlRootImage *old;
	uint8 *p;
	bool ok = false;
	if (!candidate || !r || !ref || !out || n != 66560 || !reference_matches(r, ref) ||
		!common_matches(candidate, r) || !output_distinct(r, out, n) ||
		overlaps(candidate, sizeof(*candidate), out, n) || overlaps(ref, 128, out, n) ||
		candidate->header.format_version != 3)
		return false;
	old = malloc(sizeof(*old));
	p = malloc(n);
	if (!old || !p)
		goto done;
	*old = *candidate;
	if (cluster_control_root_v3_encode(old) != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	memset(p, 0, n);
	memcpy(p, old->bytes, 376);
	memcpy(p + 1024, old->bytes + 512, 65536);
	put_le(p, 4, 4, 2);
	put_le(p, 6, 1024, 2);
	put_le(p, 72, 4, 2);
	put_le(p, 74, 4, 2);
	put_le(p, 188, old->header.target_feature_bitmap | CLUSTER_RECOVERY_CONTROL_ROOT_FEATURE, 8);
	memcpy(p + 512, ref, 128);
	put_le(p, 1020, root_crc(p, 1020), 4);
	ok = decode(p, n, r->system_identifier, r->storage_uuid, old) && common_matches(old, r);
	if (ok)
		memcpy(out, p, n);
done:
	free(p);
	free(old);
	return ok;
}
bool cluster_control_root_registry_matches_v4(const uint8 *p, Size n,
											  const ClusterRecoveryRegistryV2 *r)
{
	ControlRootImage *old;
	bool ok;
	if (!p || !r || n != 66560 || !reference_matches(r, p + 512))
		return false;
	old = malloc(sizeof(*old));
	if (!old)
		return false;
	ok = decode(p, n, r->system_identifier, r->storage_uuid, old) && common_matches(old, r);
	free(old);
	return ok;
}
#ifdef WIN32
bool cluster_control_root_registry_read_selected_v4(int d, const uint8 *b, Size n,
													const ClusterRecoveryRegistryV2 *r, Size budget)
{
	return false;
}
bool cluster_control_root_registry_capture_v4(int d, const uint8 s[16], uint64 sys,
	uint8 pair[133120], ControlRootImage *metadata) { return false; }
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
static bool same(const struct stat *a, const struct stat *b)
{
	return a->st_dev == b->st_dev && a->st_ino == b->st_ino && a->st_size == b->st_size &&
		   a->st_uid == b->st_uid && a->st_mode == b->st_mode && a->st_nlink == b->st_nlink &&
		   a->st_mtime == b->st_mtime && a->st_ctime == b->st_ctime;
}
static bool current(int dir, const char *name, int fd, const struct stat *st)
{
	struct stat held, named;
	return fstat(fd, &held) == 0 && fstatat(dir, name, &named, AT_SYMLINK_NOFOLLOW) == 0 &&
		   S_ISREG(held.st_mode) && held.st_nlink == 1 && held.st_uid == geteuid() &&
		   !(held.st_mode & 0022) && held.st_size == 66560 && same(st, &held) &&
		   same(&held, &named);
}
static bool read_exact(int fd, uint8 *bytes)
{
	Size done = 0;
	while (done < 66560)
	{
		ssize_t n = pread(fd, bytes + done, 66560 - done, done);
		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			return false;
		done += n;
	}
	return true;
}
bool cluster_control_root_registry_read_selected_v4(int dir, const uint8 *expected, Size n,
													const ClusterRecoveryRegistryV2 *r, Size budget)
{
	static const char *const names[] = {"pgrac_control_root", "pgrac_control_root.bak"};
	int fds[2] = {-1, -1};
	struct stat st[2], directory, named;
	uint8 *bytes = NULL, *recheck = NULL;
	ControlRootImage *old = NULL;
	bool ok = false;
	if (!expected || !r || n != 66560 ||
		(!cluster_cf_held_is_clusterwide(ShareLock) &&
		 !cluster_cf_held_is_clusterwide(ExclusiveLock)) ||
		fstat(dir, &directory) != 0 || !S_ISDIR(directory.st_mode) ||
		directory.st_uid != geteuid() || (directory.st_mode & 0022) ||
		!cluster_control_root_registry_matches_v4(expected, n, r))
		return false;
	bytes = malloc(2 * n);
	recheck = malloc(n);
	old = malloc(sizeof(*old));
	if (!bytes || !recheck || !old)
		goto done;
	for (int i = 0; i < 2; i++)
	{
		fds[i] = openat(dir, names[i], O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC | PG_BINARY);
		if (fds[i] < 0 || fstat(fds[i], &st[i]) != 0 || !current(dir, names[i], fds[i], &st[i]) ||
			!read_exact(fds[i], bytes + i * n))
			goto done;
	}
	if ((st[0].st_dev == st[1].st_dev && st[0].st_ino == st[1].st_ino) ||
		memcmp(bytes, expected, n) ||
		!decode(bytes + n, n, r->system_identifier, r->storage_uuid, old) ||
		memcmp(bytes + 24, bytes + n + 24, 48) || memcmp(bytes + 100, bytes + n + 100, 96) ||
		get_le(bytes + n, 16, 8) > get_le(bytes, 16, 8) ||
		(get_le(bytes + n, 16, 8) == get_le(bytes, 16, 8) && memcmp(bytes, bytes + n, n)))
		goto done;
	if (!cluster_recovery_registry_read_v2(dir, r, bytes + 512, budget))
		goto done;
	for (int i = 0; i < 2; i++)
		if (!read_exact(fds[i], recheck) || memcmp(bytes + i * n, recheck, n) ||
			!current(dir, names[i], fds[i], &st[i]))
			goto done;
	ok = !memcmp(bytes, expected, n) && fstat(dir, &named) == 0 && same(&directory, &named) &&
		 cluster_control_root_registry_matches_v4(bytes, n, r) &&
		 (cluster_cf_held_is_clusterwide(ShareLock) ||
		  cluster_cf_held_is_clusterwide(ExclusiveLock));
done:
	for (int i = 0; i < 2; i++)
		if (fds[i] >= 0 && close(fds[i]) != 0)
			ok = false;
	free(old);
	free(recheck);
	free(bytes);
	return ok;
}

bool
cluster_control_root_registry_capture_v4(int dir, const uint8 storage[16], uint64 sys,
										 uint8 pair[133120], ControlRootImage *metadata)
{
	static const char *const names[] = { "pgrac_control_root", "pgrac_control_root.bak" };
	int fds[2] = { -1, -1 };
	struct stat st[2], directory, after;
	uint8 *bytes = NULL, *again = NULL;
	ControlRootImage *logical = NULL, *backup = NULL;
	bool ok = false;
	if (!storage || !sys || !pair || !metadata
		|| overlaps(pair, 133120, metadata, sizeof(*metadata))
		|| overlaps(storage, 16, pair, 133120) || overlaps(storage, 16, metadata, sizeof(*metadata))
		|| (!cluster_cf_held_is_clusterwide(ShareLock)
			&& !cluster_cf_held_is_clusterwide(ExclusiveLock))
		|| fstat(dir, &directory) || !S_ISDIR(directory.st_mode) || directory.st_uid != geteuid()
		|| (directory.st_mode & 0022))
		return false;
	bytes = malloc(133120);
	again = malloc(66560);
	logical = malloc(sizeof(*logical));
	backup = malloc(sizeof(*backup));
	if (!bytes || !again || !logical || !backup)
		goto done;
	for (int i = 0; i < 2; i++) {
		fds[i] = openat(dir, names[i], O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC | PG_BINARY);
		if (fds[i] < 0 || fstat(fds[i], &st[i]) || !current(dir, names[i], fds[i], &st[i])
			|| !read_exact(fds[i], bytes + i * 66560))
			goto done;
	}
	if ((st[0].st_dev == st[1].st_dev && st[0].st_ino == st[1].st_ino)
		|| !decode(bytes, 66560, sys, storage, logical)
		|| !decode(bytes + 66560, 66560, sys, storage, backup)
		|| memcmp(bytes + 24, bytes + 66560 + 24, 48)
		|| memcmp(bytes + 100, bytes + 66560 + 100, 96)
		|| get_le(bytes + 66560, 16, 8) > get_le(bytes, 16, 8)
		|| (get_le(bytes + 66560, 16, 8) == get_le(bytes, 16, 8)
			&& memcmp(bytes, bytes + 66560, 66560)))
		goto done;
	for (int i = 0; i < 2; i++)
		if (!read_exact(fds[i], again) || memcmp(bytes + i * 66560, again, 66560)
			|| !current(dir, names[i], fds[i], &st[i]))
			goto done;
	ok = fstat(dir, &after) == 0 && same(&directory, &after)
		 && (cluster_cf_held_is_clusterwide(ShareLock)
			 || cluster_cf_held_is_clusterwide(ExclusiveLock));
done:
	for (int i = 0; i < 2; i++)
		if (fds[i] >= 0 && close(fds[i]))
			ok = false;
	if (ok) {
		memset(logical->bytes, 0, sizeof(logical->bytes));
		logical->header.format_version = 4;
		logical->header.source_feature_bitmap = get_le(bytes, 180, 8);
		logical->header.target_feature_bitmap = get_le(bytes, 188, 8);
		memcpy(pair, bytes, 133120);
		*metadata = *logical;
	}
	free(backup);
	free(logical);
	free(again);
	free(bytes);
	return ok;
}
#endif
