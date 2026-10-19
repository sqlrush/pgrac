/*-------------------------------------------------------------------------
 *
 * cluster_relmap_authority.c
 *	  Shared relmapper (pg_filenode.map) single authority: durable substrate
 *	  (spec-6.14 D5).
 *
 *	  Torn-safe two-phase authority file (mirrors cluster_cf_authority): a
 *	  fixed-size on-disk image
 *	      [ header ][ committed slot ][ pending slot ]
 *	  written via temp + fsync + .bak roll + durable_rename.  Readers adopt the
 *	  committed slot only (INV-14-7); writers stage the pending slot and later
 *	  publish (pending -> committed).  The header carries the two generations
 *	  and the crash-arbitration owner identity (spec §2.1).  Image bytes are
 *	  opaque (they carry RelMapFile's own CRC, checked by the relmapper on
 *	  load), so the header CRC covers only the header.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_relmap_authority.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.14-shared-catalog-single-authority.md (D5, §2.1)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cluster/cluster_guc.h" /* cluster_shared_data_dir */
#include "cluster/cluster_relmap_authority.h"
#include "cluster/cluster_relmap_lock.h"
#include "cluster/cluster_relmap_retention.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/lwlock.h"

#define COMMITTED_SLOT_OFFSET CLUSTER_RELMAP_COMMITTED_OFFSET
#define PENDING_SLOT_OFFSET CLUSTER_RELMAP_PENDING_OFFSET

/* Relative authority path within cluster_shared_data_dir. */
#define AUTHORITY_BASENAME "pgrac_relmap_authority"

/* ============================================================
 * Pure classification (standalone-linkable).
 * ============================================================ */

ClusterRelmapAuthorityValidity
cluster_relmap_authority_classify(const char *buf, size_t len)
{
	ClusterRelmapAuthorityHeader hdr;
	pg_crc32c crc;

	if (buf == NULL || len < (size_t)CLUSTER_RELMAP_AUTHORITY_FILE_SIZE)
		return CLUSTER_RELMAP_AUTHORITY_INVALID_SHORT;

	memcpy(&hdr, buf, sizeof(hdr));

	if (hdr.magic != CLUSTER_RELMAP_AUTHORITY_MAGIC)
		return CLUSTER_RELMAP_AUTHORITY_INVALID_MAGIC;

	if (hdr.image_size == 0 || hdr.image_size > CLUSTER_RELMAP_IMAGE_MAX)
		return CLUSTER_RELMAP_AUTHORITY_INVALID_IMAGE_SIZE;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, buf, offsetof(ClusterRelmapAuthorityHeader, crc));
	FIN_CRC32C(crc);
	if (!EQ_CRC32C(crc, hdr.crc))
		return CLUSTER_RELMAP_AUTHORITY_INVALID_CRC;

	return CLUSTER_RELMAP_AUTHORITY_VALID;
}

/* ============================================================
 * Path resolution.
 * ============================================================ */

/*
 * build_authority_path -- compose the authority path for (shared_map, dbid)
 * with the given suffix ("" / ".bak" / ".tmp" / ".bak.tmp").  Shared maps live
 * under global/; per-db maps under base/<dbid>/ (default-tablespace case;
 * non-default-tablespace relmap is a D5-activation forward item).  Returns
 * false when the shared root is not configured.
 */
static bool
build_authority_path(char *dst, size_t dstlen, bool shared_map, Oid dbid, const char *suffix)
{
	if (cluster_shared_data_dir == NULL || cluster_shared_data_dir[0] == '\0')
		return false;

	if (shared_map)
		snprintf(dst, dstlen, "%s/global/%s%s", cluster_shared_data_dir, AUTHORITY_BASENAME,
				 suffix);
	else
		snprintf(dst, dstlen, "%s/base/%u/%s%s", cluster_shared_data_dir, dbid, AUTHORITY_BASENAME,
				 suffix);
	return true;
}

/* ============================================================
 * Torn-safe file I/O (mirrors cluster_cf_authority).
 * ============================================================ */

static void
write_durable(const char *tmp, const char *final, const char *buf)
{
	int fd;

	fd = OpenTransientFile(tmp, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY);
	if (fd < 0)
		ereport(PANIC, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", tmp)));

	errno = 0;
	if (write(fd, buf, CLUSTER_RELMAP_AUTHORITY_FILE_SIZE) != CLUSTER_RELMAP_AUTHORITY_FILE_SIZE) {
		if (errno == 0)
			errno = ENOSPC;
		ereport(PANIC, (errcode_for_file_access(), errmsg("could not write file \"%s\": %m", tmp)));
	}

	if (pg_fsync(fd) != 0)
		ereport(PANIC, (errcode_for_file_access(), errmsg("could not fsync file \"%s\": %m", tmp)));

	if (CloseTransientFile(fd) != 0)
		ereport(PANIC, (errcode_for_file_access(), errmsg("could not close file \"%s\": %m", tmp)));

	if (durable_rename(tmp, final, PANIC) != 0)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not rename file \"%s\" to \"%s\": %m", tmp, final)));
}

static void
roll_primary_to_bak(const char *primary, const char *bak, const char *baktmp)
{
	char buf[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE];
	int fd;
	int r;

	fd = OpenTransientFile(primary, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return; /* first write: no prior primary */

	r = read(fd, buf, CLUSTER_RELMAP_AUTHORITY_FILE_SIZE);
	CloseTransientFile(fd);
	if (r != CLUSTER_RELMAP_AUTHORITY_FILE_SIZE)
		return;

	write_durable(baktmp, bak, buf);
}

/*
 * read_whole -- read a candidate authority file into image (must be at least
 * CLUSTER_RELMAP_AUTHORITY_FILE_SIZE) and classify it.
 */
static ClusterRelmapAuthorityValidity
read_whole(const char *path, char *image)
{
	int fd;
	int r;

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return CLUSTER_RELMAP_AUTHORITY_INVALID_SHORT;

	r = read(fd, image, CLUSTER_RELMAP_AUTHORITY_FILE_SIZE);
	CloseTransientFile(fd);
	if (r != CLUSTER_RELMAP_AUTHORITY_FILE_SIZE)
		return CLUSTER_RELMAP_AUTHORITY_INVALID_SHORT;

	return cluster_relmap_authority_classify(image, CLUSTER_RELMAP_AUTHORITY_FILE_SIZE);
}

/*
 * load_authority -- fail-closed load of the whole authority image (primary
 * then .bak).  Returns false when neither is trustworthy.
 */
static bool
load_authority(bool shared_map, Oid dbid, char *image)
{
	char primary[MAXPGPATH];
	char bak[MAXPGPATH];

	if (!build_authority_path(primary, sizeof(primary), shared_map, dbid, "")
		|| !build_authority_path(bak, sizeof(bak), shared_map, dbid, ".bak"))
		return false;

	if (read_whole(primary, image) == CLUSTER_RELMAP_AUTHORITY_VALID)
		return true;
	if (read_whole(bak, image) == CLUSTER_RELMAP_AUTHORITY_VALID)
		return true;
	return false;
}

/*
 * store_authority -- torn-safe write of the whole authority image (roll
 * primary -> .bak, then temp + durable_rename).
 */
static void
store_authority(bool shared_map, Oid dbid, const char *image)
{
	char primary[MAXPGPATH];
	char bak[MAXPGPATH];
	char tmp[MAXPGPATH];
	char baktmp[MAXPGPATH];

	if (!build_authority_path(primary, sizeof(primary), shared_map, dbid, "")
		|| !build_authority_path(bak, sizeof(bak), shared_map, dbid, ".bak")
		|| !build_authority_path(tmp, sizeof(tmp), shared_map, dbid, ".tmp")
		|| !build_authority_path(baktmp, sizeof(baktmp), shared_map, dbid, ".bak.tmp"))
		ereport(PANIC, (errmsg("cluster shared_data_dir is not configured")));

	roll_primary_to_bak(primary, bak, baktmp);
	write_durable(tmp, primary, image);
}

/*
 * finalize_header_crc -- (re)compute the header CRC in place inside image.
 */
static void
finalize_header_crc(char *image)
{
	ClusterRelmapAuthorityHeader *hdr = (ClusterRelmapAuthorityHeader *)image;

	INIT_CRC32C(hdr->crc);
	COMP_CRC32C(hdr->crc, image, offsetof(ClusterRelmapAuthorityHeader, crc));
	FIN_CRC32C(hdr->crc);
}

/* ============================================================
 * Public API.
 * ============================================================ */

bool
cluster_relmap_authority_read_committed(bool shared_map, Oid dbid, char *map_out, uint32 *out_len)
{
	char image[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE];
	ClusterRelmapAuthorityHeader hdr;

	if (!load_authority(shared_map, dbid, image))
		return false;

	memcpy(&hdr, image, sizeof(hdr));
	memcpy(map_out, image + COMMITTED_SLOT_OFFSET, hdr.image_size);
	*out_len = hdr.image_size;
	return true;
}

bool
cluster_relmap_authority_read_header(bool shared_map, Oid dbid, ClusterRelmapAuthorityHeader *out)
{
	char image[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE];

	if (!load_authority(shared_map, dbid, image))
		return false;

	memcpy(out, image, sizeof(*out));
	return true;
}

void
cluster_relmap_authority_write_pending(bool shared_map, Oid dbid, const char *map_image,
									   uint32 image_size, uint64 pending_generation,
									   const ClusterRelmapOwner *owner)
{
	char image[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE];
	ClusterRelmapAuthorityHeader *hdr = (ClusterRelmapAuthorityHeader *)image;
	bool have_prior;

	if (image_size == 0 || image_size > CLUSTER_RELMAP_IMAGE_MAX)
		ereport(PANIC, (errmsg("relmap authority image size %u out of range", image_size)));

	/* Start from the current authority if any, else a zeroed image. */
	have_prior = load_authority(shared_map, dbid, image);
	if (!have_prior)
		memset(image, 0, sizeof(image));

	hdr->magic = CLUSTER_RELMAP_AUTHORITY_MAGIC;
	hdr->version = CLUSTER_RELMAP_AUTHORITY_VERSION;
	hdr->dbid = dbid;
	hdr->shared_map = shared_map ? 1 : 0;
	hdr->pad0 = 0;
	hdr->image_size = image_size;
	hdr->pad1 = 0;

	/* Stage the new image in the pending slot; committed slot untouched. */
	memset(image + PENDING_SLOT_OFFSET, 0, CLUSTER_RELMAP_IMAGE_MAX);
	memcpy(image + PENDING_SLOT_OFFSET, map_image, image_size);
	hdr->pending_generation = pending_generation;

	/* Owner identity for crash arbitration (spec §2.1). */
	hdr->owner_node = owner->owner_node;
	hdr->owner_xid = owner->owner_xid;
	hdr->owner_epoch = owner->owner_epoch;
	hdr->relmap_lsn = owner->relmap_lsn;

	/*
	 * First-ever write: seed the committed slot from the same image and set
	 * committed_generation so a reader before the first publish still adopts a
	 * valid map (there is no "old committed" to preserve).
	 */
	if (!have_prior) {
		memset(image + COMMITTED_SLOT_OFFSET, 0, CLUSTER_RELMAP_IMAGE_MAX);
		memcpy(image + COMMITTED_SLOT_OFFSET, map_image, image_size);
		hdr->committed_generation = pending_generation;
	}

	finalize_header_crc(image);
	store_authority(shared_map, dbid, image);
}

void
cluster_relmap_authority_publish(bool shared_map, Oid dbid, uint64 generation)
{
	char image[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE];
	ClusterRelmapAuthorityHeader *hdr = (ClusterRelmapAuthorityHeader *)image;

	if (!load_authority(shared_map, dbid, image))
		ereport(PANIC, (errmsg("relmap authority unavailable at publish for db %u", dbid)));

	if (hdr->pending_generation != generation) {
		/*
		 * Already published (idempotent retry) or a stale generation.  If the
		 * committed generation already matches, treat as done; otherwise this
		 * is a programming error in the activation layer.
		 */
		if (hdr->committed_generation == generation)
			return;
		ereport(PANIC, (errmsg("relmap authority publish generation mismatch: "
							   "pending %llu committed %llu requested %llu",
							   (unsigned long long)hdr->pending_generation,
							   (unsigned long long)hdr->committed_generation,
							   (unsigned long long)generation)));
	}

	/* Flip pending -> committed: copy the pending slot over the committed slot. */
	memcpy(image + COMMITTED_SLOT_OFFSET, image + PENDING_SLOT_OFFSET, CLUSTER_RELMAP_IMAGE_MAX);
	hdr->committed_generation = generation;
	hdr->pending_generation = 0;

	finalize_header_crc(image);
	store_authority(shared_map, dbid, image);
}

void
cluster_relmap_authority_discard_pending(bool shared_map, Oid dbid, uint64 generation)
{
	char image[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE] = { 0 }; /* zero-init: cppcheck
														   * cannot see through
														   * load_authority()'s
														   * fill of image */
	ClusterRelmapAuthorityHeader *hdr = (ClusterRelmapAuthorityHeader *)image;

	if (!load_authority(shared_map, dbid, image)) {
		/*
		 * Must not throw (abort-path callable).  The unresolved pending, if
		 * any survives on disk, keeps relmap writes fail-closed until a
		 * trustworthy authority reappears.
		 */
		elog(LOG,
			 "relmap authority for db %u unavailable at pending discard; "
			 "leaving it for arbitration",
			 dbid);
		return;
	}

	if (hdr->pending_generation != generation)
		return; /* already discarded or published */

	hdr->pending_generation = 0;
	hdr->owner_node = 0;
	hdr->owner_xid = 0;
	hdr->owner_epoch = 0;
	hdr->relmap_lsn = 0;
	memset(image + PENDING_SLOT_OFFSET, 0, CLUSTER_RELMAP_IMAGE_MAX);

	finalize_header_crc(image);
	store_authority(shared_map, dbid, image);
}

/*
 * V2 storage.  Keep descriptors bounded and process no interrupts or ereports
 * while they are open.  All exits close them; the caller supplies the original
 * precommit/postcommit error polarity and retains any unresolved obligation.
 *
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 */
typedef struct RelmapV2Directory {
	int root;
	int base;
	int map;
	struct stat root_stat;
	struct stat base_stat;
	struct stat map_stat;
	char dbname[32];
	bool shared;
} RelmapV2Directory;

typedef struct RelmapV2Copy {
	bool present;
	struct stat stat;
	size_t length;
	unsigned char bytes[CLUSTER_RELMAP_V2_FILE_SIZE];
} RelmapV2Copy;

static const char *const relmap_v2_names[] = { AUTHORITY_BASENAME, AUTHORITY_BASENAME ".bak" };
static const char *const relmap_v2_temps[] = { AUTHORITY_BASENAME ".tmp",
											AUTHORITY_BASENAME ".bak.tmp" };

static bool
relmap_v2_same_inode(const struct stat *a, const struct stat *b)
{
	return a->st_dev == b->st_dev && a->st_ino == b->st_ino;
}

static bool
relmap_v2_regular(const struct stat *st)
{
	return S_ISREG(st->st_mode) && st->st_nlink == 1;
}

static bool
relmap_v2_entry(int dir, const char *name, const struct stat *expected)
{
	struct stat st;

	return fstatat(dir, name, &st, AT_SYMLINK_NOFOLLOW) == 0 && relmap_v2_regular(&st)
		   && relmap_v2_same_inode(&st, expected);
}

static bool
relmap_v2_dirs_current(const RelmapV2Directory *dirs)
{
	struct stat st;

	if (lstat(cluster_shared_data_dir, &st) != 0 || !S_ISDIR(st.st_mode)
		|| !relmap_v2_same_inode(&st, &dirs->root_stat))
		return false;
	if (dirs->shared)
		return fstatat(dirs->root, "global", &st, AT_SYMLINK_NOFOLLOW) == 0
			   && S_ISDIR(st.st_mode) && relmap_v2_same_inode(&st, &dirs->map_stat);
	return fstatat(dirs->root, "base", &st, AT_SYMLINK_NOFOLLOW) == 0 && S_ISDIR(st.st_mode)
		   && relmap_v2_same_inode(&st, &dirs->base_stat)
		   && fstatat(dirs->base, dirs->dbname, &st, AT_SYMLINK_NOFOLLOW) == 0
		   && S_ISDIR(st.st_mode) && relmap_v2_same_inode(&st, &dirs->map_stat);
}

static bool
relmap_v2_close_dirs(RelmapV2Directory *dirs)
{
	bool ok = true;

	if (dirs->map >= 0 && close(dirs->map) != 0)
		ok = false;
	if (dirs->base >= 0 && close(dirs->base) != 0)
		ok = false;
	if (dirs->root >= 0 && close(dirs->root) != 0)
		ok = false;
	return ok;
}

static bool
relmap_v2_open_dirs(const ClusterRelmapV2Namespace *expected, RelmapV2Directory *dirs)
{
	const int flags = O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC;

	memset(dirs, 0, sizeof(*dirs));
	dirs->root = dirs->base = dirs->map = -1;
	if (expected == NULL || expected->system_identifier == 0
		|| expected->database_incarnation == 0 || expected->shared != (expected->dbid == InvalidOid)
		|| cluster_shared_data_dir == NULL || cluster_shared_data_dir[0] != '/')
		return false;
	dirs->shared = expected->shared;
	dirs->root = open(cluster_shared_data_dir, flags);
	if (dirs->root < 0 || fstat(dirs->root, &dirs->root_stat) != 0)
		return false;
	if (expected->shared)
		dirs->map = openat(dirs->root, "global", flags);
	else {
		dirs->base = openat(dirs->root, "base", flags);
		if (dirs->base < 0 || fstat(dirs->base, &dirs->base_stat) != 0)
			return false;
		snprintf(dirs->dbname, sizeof(dirs->dbname), "%u", expected->dbid);
		dirs->map = openat(dirs->base, dirs->dbname, flags);
	}
	return dirs->map >= 0 && fstat(dirs->map, &dirs->map_stat) == 0 && relmap_v2_dirs_current(dirs);
}

/* A missing or short copy can only yield a degraded view.  Unsafe paths and
 * inode replacement are refusals, even when the other copy is valid. */
static bool
relmap_v2_read_copy(int dir, int which, RelmapV2Copy *copy)
{
	struct stat opened;
	struct stat after;
	int fd;
	size_t done = 0;
	bool ok = false;

	memset(copy, 0, sizeof(*copy));
	if (fstatat(dir, relmap_v2_names[which], &copy->stat, AT_SYMLINK_NOFOLLOW) != 0)
		return errno == ENOENT;
	if (!relmap_v2_regular(&copy->stat) || copy->stat.st_size < 0)
		return false;
	copy->present = true;
	fd = openat(dir, relmap_v2_names[which], O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK | PG_BINARY);
	if (fd < 0)
		return false;
	if (fstat(fd, &opened) != 0 || !relmap_v2_regular(&opened)
		|| !relmap_v2_same_inode(&opened, &copy->stat) || opened.st_size != copy->stat.st_size)
		goto done;
	/* Keep an overlength marker without reading outside the bounded buffer. */
	copy->length = Min((uint64)opened.st_size, (uint64)sizeof(copy->bytes) + 1);
	while (done < Min(copy->length, sizeof(copy->bytes))) {
		ssize_t n = read(fd, copy->bytes + done, Min(copy->length, sizeof(copy->bytes)) - done);

		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			goto done;
		done += n;
	}
	ok = fstat(fd, &after) == 0 && after.st_size == opened.st_size
		 && relmap_v2_same_inode(&after, &opened)
		 && relmap_v2_entry(dir, relmap_v2_names[which], &opened);
done:
	if (close(fd) != 0)
		ok = false;
	return ok;
}

static bool
relmap_v2_copies_current(int dir, const RelmapV2Copy copies[2])
{
	struct stat st;

	for (int i = 0; i < 2; i++) {
		if (copies[i].present) {
			if (!relmap_v2_entry(dir, relmap_v2_names[i], &copies[i].stat))
				return false;
		} else if (fstatat(dir, relmap_v2_names[i], &st, AT_SYMLINK_NOFOLLOW) == 0 || errno != ENOENT)
			return false;
	}
	return true;
}

static ClusterRelmapV2PairResult
relmap_v2_load(const RelmapV2Directory *dirs, const ClusterRelmapV2Namespace *expected,
			   RelmapV2Copy copies[2], ClusterRelmapV2View *view)
{
	if (!relmap_v2_read_copy(dirs->map, 0, &copies[0])
		|| !relmap_v2_read_copy(dirs->map, 1, &copies[1]) || !relmap_v2_dirs_current(dirs)
		|| !relmap_v2_copies_current(dirs->map, copies))
		return CLUSTER_RELMAP_V2_PAIR_INVALID;
	return cluster_relmap_v2_read_pair(copies[0].bytes, copies[0].length,
									 copies[1].bytes, copies[1].length, expected, view);
}

ClusterRelmapV2PairResult
cluster_relmap_authority_v2_read(const ClusterRelmapV2Namespace *expected,
								ClusterRelmapV2View *output)
{
	RelmapV2Directory dirs;
	RelmapV2Copy copies[2];
	ClusterRelmapV2View view;
	ClusterRelmapV2PairResult result = CLUSTER_RELMAP_V2_PAIR_INVALID;

	if (output == NULL)
		return result;
	memset(output, 0, sizeof(*output));
	if (relmap_v2_open_dirs(expected, &dirs))
		result = relmap_v2_load(&dirs, expected, copies, &view);
	if (!relmap_v2_close_dirs(&dirs))
		result = CLUSTER_RELMAP_V2_PAIR_INVALID;
	if (result == CLUSTER_RELMAP_V2_PAIR_VALID || result == CLUSTER_RELMAP_V2_PAIR_DEGRADED)
		*output = view;
	return result;
}

bool
cluster_relmap_authority_v2_read_committed(const ClusterRelmapV2Namespace *expected,
										RelMapFile *map_out, uint64 *generation_out)
{
	ClusterRelmapV2View view;

	if (map_out == NULL || generation_out == NULL)
		return false;
	memset(map_out, 0, sizeof(*map_out));
	*generation_out = 0;
	/* The full pair reader is also used by unpublished cold BUILDING restore.
	 * Only this committed runtime facade requires the namespace X lock. */
	if (expected == NULL || !cluster_relmap_authority_x_held())
		return false;
	if (cluster_relmap_authority_v2_read(expected, &view) != CLUSTER_RELMAP_V2_PAIR_VALID
		|| !view.pending_complete)
		return false;
	/* The raw pair read performs disk I/O; do not publish its result if the
	 * caller dropped the runtime serialization lock during that call. */
	if (!cluster_relmap_authority_x_held())
		return false;
	*map_out = view.committed;
	*generation_out = view.committed_generation;
	return true;
}

#ifdef USE_PGRAC_CLUSTER
ClusterRelmapPendingResult
cluster_relmap_authority_v2_read_rebuild_pending(
	const ClusterRelmapV2Namespace *expected,
	const ClusterRelmapPendingDescriptor *descriptors, size_t descriptor_count,
	ClusterRelmapPendingSet *pending_out, RelMapFile *committed_out,
	uint64 *generation_out)
{
	ClusterRelmapV2View view;
	ClusterRelmapV2PairResult pair_result;
	ClusterRelmapPendingResult rebuild_result;

	if (expected == NULL || pending_out == NULL || committed_out == NULL ||
		generation_out == NULL)
		return CLUSTER_RELMAP_PENDING_INVALID_INPUT;
	memset(committed_out, 0, sizeof(*committed_out));
	*generation_out = 0;
	pair_result = cluster_relmap_authority_v2_read(expected, &view);
	if (pair_result != CLUSTER_RELMAP_V2_PAIR_VALID || !view.pending_complete ||
		view.committed_generation == 0)
		return CLUSTER_RELMAP_PENDING_NOT_READY;
	rebuild_result = cluster_relmap_pending_rebuild_from_committed(
		pending_out, expected, pair_result, &view, descriptors, descriptor_count);
	if (rebuild_result != CLUSTER_RELMAP_PENDING_OK)
		return rebuild_result;
	*committed_out = view.committed;
	*generation_out = view.committed_generation;
	return CLUSTER_RELMAP_PENDING_OK;
}
#endif

/* Each replacement is file sync -> rename -> directory sync -> exact inode
 * and full byte readback.  Never roll the old primary into backup: the backup
 * must receive the same new pending obligation before stage can succeed. */
static bool
relmap_v2_install(const RelmapV2Directory *dirs, int which,
				  const unsigned char bytes[CLUSTER_RELMAP_V2_FILE_SIZE], RelmapV2Copy copies[2])
{
	RelmapV2Copy actual;
	struct stat temp_stat;
	int fd = -1;
	size_t done = 0;
	bool own_temp = false;
	bool installed = false;
	bool ok = false;

	fd = openat(dirs->map, relmap_v2_temps[which],
				O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC | PG_BINARY, 0600);
	if (fd < 0 || fstat(fd, &temp_stat) != 0 || !relmap_v2_regular(&temp_stat))
		goto cleanup;
	own_temp = true;
	while (done < CLUSTER_RELMAP_V2_FILE_SIZE) {
		ssize_t n = write(fd, bytes + done, CLUSTER_RELMAP_V2_FILE_SIZE - done);

		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			goto cleanup;
		done += n;
	}
	if (pg_fsync(fd) != 0 || !relmap_v2_dirs_current(dirs)
		|| !relmap_v2_copies_current(dirs->map, copies)
		|| !relmap_v2_entry(dirs->map, relmap_v2_temps[which], &temp_stat))
		goto cleanup;
	if (renameat(dirs->map, relmap_v2_temps[which], dirs->map, relmap_v2_names[which]) != 0)
		goto cleanup;
	installed = true;
	if (pg_fsync(dirs->map) != 0 || !relmap_v2_dirs_current(dirs)
		|| !relmap_v2_read_copy(dirs->map, which, &actual) || !actual.present
		|| actual.length != CLUSTER_RELMAP_V2_FILE_SIZE
		|| !relmap_v2_same_inode(&actual.stat, &temp_stat)
		|| memcmp(actual.bytes, bytes, CLUSTER_RELMAP_V2_FILE_SIZE) != 0)
		goto cleanup;
	copies[which] = actual;
	ok = true;
cleanup:
	if (fd >= 0 && close(fd) != 0)
		ok = false;
	/* A preexisting or replaced temp belongs to somebody else. */
	if (own_temp && !installed && relmap_v2_entry(dirs->map, relmap_v2_temps[which], &temp_stat))
		if (unlinkat(dirs->map, relmap_v2_temps[which], 0) != 0)
			ok = false;
	return ok;
}

static bool
relmap_v2_store_pair(const RelmapV2Directory *dirs, const ClusterRelmapV2Namespace *expected,
					const unsigned char bytes[CLUSTER_RELMAP_V2_FILE_SIZE], RelmapV2Copy copies[2])
{
	RelmapV2Copy final[2];
	ClusterRelmapV2View view;

	if (!relmap_v2_install(dirs, 0, bytes, copies) || !relmap_v2_install(dirs, 1, bytes, copies))
		return false;
	/* Recheck both after the second install, not just each one in isolation. */
	return relmap_v2_load(dirs, expected, final, &view) == CLUSTER_RELMAP_V2_PAIR_VALID
		   && relmap_v2_same_inode(&final[0].stat, &copies[0].stat)
		   && relmap_v2_same_inode(&final[1].stat, &copies[1].stat)
		   && memcmp(final[0].bytes, bytes, CLUSTER_RELMAP_V2_FILE_SIZE) == 0
		   && memcmp(final[1].bytes, bytes, CLUSTER_RELMAP_V2_FILE_SIZE) == 0;
}

bool
cluster_relmap_authority_v2_write_pending(const ClusterRelmapV2Namespace *expected,
										uint64 generation, const ClusterRelmapV2Owner *owner,
										const RelMapFile *pending,
										ClusterRelmapRetentionDirectory *retention,
										LWLock *retention_lock)
{
	RelmapV2Directory dirs;
	RelmapV2Copy copies[2];
	ClusterRelmapV2View view;
	ClusterRelmapV2Image image;
	ClusterRelmapRetentionKey key = { 0 };
	ClusterRelmapRetentionResult retained;
	unsigned char bytes[CLUSTER_RELMAP_V2_FILE_SIZE];
	bool ok = false;

	/* No I/O can turn a disabled sync policy into durable stage permission. */
	if (!enableFsync || owner == NULL || pending == NULL || retention == NULL
		|| retention_lock == NULL || LWLockHeldByMe(retention_lock)
		|| !cluster_relmap_authority_x_held())
		return false;
	if (!relmap_v2_open_dirs(expected, &dirs))
		goto done;
	if (relmap_v2_load(&dirs, expected, copies, &view) != CLUSTER_RELMAP_V2_PAIR_VALID
		|| view.has_pending || view.committed_generation == PG_UINT64_MAX
		|| generation != view.committed_generation + 1)
		goto done;
	memset(&image, 0, sizeof(image));
	image.identity = *expected;
	image.committed_generation = view.committed_generation;
	image.committed = view.committed;
	image.pending_generation = generation;
	image.pending_owner = *owner;
	image.pending = *pending;
	if (cluster_relmap_v2_encode(&image, bytes, sizeof(bytes)) != CLUSTER_RELMAP_V2_VALID)
		goto done;
	/* Bind the pin to the bytes being installed, never to a caller-supplied
	 * "retained" boolean. Preparation/cancellation may race the read-only
	 * preflight, so check and mark under the same lock immediately before the
	 * first possible pending write. No I/O or other lock runs under it. */
	key.identity = image.identity;
	key.generation = image.pending_generation;
	key.origin = image.pending_owner.node;
	key.top_full_xid = image.pending_owner.top_full_xid;
	key.membership_epoch = image.pending_owner.membership_epoch;
	key.admitted_incarnation = image.pending_owner.admitted_incarnation;
	key.boot_incarnation = image.pending_owner.boot_incarnation;
	LWLockAcquire(retention_lock, LW_EXCLUSIVE);
	retained = cluster_relmap_retention_stage_start(retention, &key, image.pending_owner.wal_end);
	LWLockRelease(retention_lock);
	if (retained != CLUSTER_RELMAP_RETENTION_OK)
		goto done;
	/* Every later failure keeps may_be_staged, including a failed first write
	 * or directory close. Ordinary ERROR cleanup must not cancel this pin. */
	ok = relmap_v2_store_pair(&dirs, expected, bytes, copies);
done:
	if (!relmap_v2_close_dirs(&dirs))
		ok = false;
	return ok;
}

static ClusterRelmapV2ClearResult
relmap_v2_finish(const ClusterRelmapV2Image *staged, bool publish)
{
	RelmapV2Directory dirs;
	RelmapV2Copy copies[2];
	ClusterRelmapV2View view;
	ClusterRelmapV2Image clean;
	unsigned char staged_bytes[CLUSTER_RELMAP_V2_FILE_SIZE];
	unsigned char original_bytes[CLUSTER_RELMAP_V2_FILE_SIZE];
	unsigned char final_bytes[CLUSTER_RELMAP_V2_FILE_SIZE];
	bool ok = false;

	if (!enableFsync || staged == NULL || staged->pending_generation == 0
		|| !cluster_relmap_authority_x_held()
		|| cluster_relmap_v2_encode(staged, staged_bytes, sizeof(staged_bytes))
			   != CLUSTER_RELMAP_V2_VALID)
		return CLUSTER_RELMAP_V2_RETAINED_UNKNOWN;
	clean = *staged;
	clean.pending_generation = 0;
	memset(&clean.pending_owner, 0, sizeof(clean.pending_owner));
	memset(&clean.pending, 0, sizeof(clean.pending));
	if (cluster_relmap_v2_encode(&clean, original_bytes, sizeof(original_bytes))
		!= CLUSTER_RELMAP_V2_VALID)
		return CLUSTER_RELMAP_V2_RETAINED_UNKNOWN;
	if (publish) {
		clean.committed_generation = staged->pending_generation;
		clean.committed = staged->pending;
	}
	if (cluster_relmap_v2_encode(&clean, final_bytes, sizeof(final_bytes))
		!= CLUSTER_RELMAP_V2_VALID)
		return CLUSTER_RELMAP_V2_RETAINED_UNKNOWN;
	if (!relmap_v2_open_dirs(&staged->identity, &dirs))
		goto done;
	if (relmap_v2_load(&dirs, &staged->identity, copies, &view) != CLUSTER_RELMAP_V2_PAIR_VALID)
		goto done;
	for (int i = 0; i < 2; i++) {
		/* No other owner, old image, generation or pending payload may be cleared. */
		if (memcmp(copies[i].bytes, staged_bytes, sizeof(staged_bytes)) != 0
			&& memcmp(copies[i].bytes, original_bytes, sizeof(original_bytes)) != 0
			&& memcmp(copies[i].bytes, final_bytes, sizeof(final_bytes)) != 0)
			goto done;
		/* A clean old pair is not evidence that a new map was staged. */
		if (!view.has_pending
			&& memcmp(copies[i].bytes, final_bytes, sizeof(final_bytes)) != 0)
			goto done;
	}
	/* Even a clean retry must establish sync and postread in this call. */
	ok = relmap_v2_store_pair(&dirs, &staged->identity, final_bytes, copies);
done:
	if (!relmap_v2_close_dirs(&dirs))
		ok = false;
	return ok ? CLUSTER_RELMAP_V2_CLEARED_POSTREAD : CLUSTER_RELMAP_V2_RETAINED_UNKNOWN;
}

ClusterRelmapV2ClearResult
cluster_relmap_authority_v2_publish(const ClusterRelmapV2Image *staged)
{
	return relmap_v2_finish(staged, true);
}

ClusterRelmapV2ClearResult
cluster_relmap_authority_v2_discard_pending(const ClusterRelmapV2Image *staged)
{
	return relmap_v2_finish(staged, false);
}
