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
#include <unistd.h>

#include "cluster/cluster_guc.h"	/* cluster_shared_data_dir */
#include "cluster/cluster_relmap_authority.h"
#include "storage/fd.h"

/* Fixed on-disk file size: header + two full-width image slots. */
#define CLUSTER_RELMAP_AUTHORITY_FILE_SIZE \
	((int) (sizeof(ClusterRelmapAuthorityHeader) + 2 * CLUSTER_RELMAP_IMAGE_MAX))

#define COMMITTED_SLOT_OFFSET ((int) sizeof(ClusterRelmapAuthorityHeader))
#define PENDING_SLOT_OFFSET   (COMMITTED_SLOT_OFFSET + CLUSTER_RELMAP_IMAGE_MAX)

/* Relative authority path within cluster_shared_data_dir. */
#define AUTHORITY_BASENAME "pgrac_relmap_authority"

/* ============================================================
 * Pure classification (standalone-linkable).
 * ============================================================ */

ClusterRelmapAuthorityValidity
cluster_relmap_authority_classify(const char *buf, size_t len)
{
	ClusterRelmapAuthorityHeader hdr;
	pg_crc32c	crc;

	if (buf == NULL || len < (size_t) CLUSTER_RELMAP_AUTHORITY_FILE_SIZE)
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
build_authority_path(char *dst, size_t dstlen, bool shared_map, Oid dbid,
					 const char *suffix)
{
	if (cluster_shared_data_dir == NULL || cluster_shared_data_dir[0] == '\0')
		return false;

	if (shared_map)
		snprintf(dst, dstlen, "%s/global/%s%s",
				 cluster_shared_data_dir, AUTHORITY_BASENAME, suffix);
	else
		snprintf(dst, dstlen, "%s/base/%u/%s%s",
				 cluster_shared_data_dir, dbid, AUTHORITY_BASENAME, suffix);
	return true;
}

/* ============================================================
 * Torn-safe file I/O (mirrors cluster_cf_authority).
 * ============================================================ */

static void
write_durable(const char *tmp, const char *final, const char *buf)
{
	int			fd;

	fd = OpenTransientFile(tmp, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY);
	if (fd < 0)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not open file \"%s\": %m", tmp)));

	errno = 0;
	if (write(fd, buf, CLUSTER_RELMAP_AUTHORITY_FILE_SIZE) != CLUSTER_RELMAP_AUTHORITY_FILE_SIZE)
	{
		if (errno == 0)
			errno = ENOSPC;
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not write file \"%s\": %m", tmp)));
	}

	if (pg_fsync(fd) != 0)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not fsync file \"%s\": %m", tmp)));

	if (CloseTransientFile(fd) != 0)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not close file \"%s\": %m", tmp)));

	if (durable_rename(tmp, final, PANIC) != 0)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not rename file \"%s\" to \"%s\": %m", tmp, final)));
}

static void
roll_primary_to_bak(const char *primary, const char *bak, const char *baktmp)
{
	char		buf[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE];
	int			fd;
	int			r;

	fd = OpenTransientFile(primary, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return;					/* first write: no prior primary */

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
	int			fd;
	int			r;

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
	char		primary[MAXPGPATH];
	char		bak[MAXPGPATH];

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
	char		primary[MAXPGPATH];
	char		bak[MAXPGPATH];
	char		tmp[MAXPGPATH];
	char		baktmp[MAXPGPATH];

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
	ClusterRelmapAuthorityHeader *hdr = (ClusterRelmapAuthorityHeader *) image;

	INIT_CRC32C(hdr->crc);
	COMP_CRC32C(hdr->crc, image, offsetof(ClusterRelmapAuthorityHeader, crc));
	FIN_CRC32C(hdr->crc);
}

/* ============================================================
 * Public API.
 * ============================================================ */

bool
cluster_relmap_authority_read_committed(bool shared_map, Oid dbid,
										char *map_out, uint32 *out_len)
{
	char		image[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE];
	ClusterRelmapAuthorityHeader hdr;

	if (!load_authority(shared_map, dbid, image))
		return false;

	memcpy(&hdr, image, sizeof(hdr));
	memcpy(map_out, image + COMMITTED_SLOT_OFFSET, hdr.image_size);
	*out_len = hdr.image_size;
	return true;
}

bool
cluster_relmap_authority_read_header(bool shared_map, Oid dbid,
									 ClusterRelmapAuthorityHeader *out)
{
	char		image[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE];

	if (!load_authority(shared_map, dbid, image))
		return false;

	memcpy(out, image, sizeof(*out));
	return true;
}

void
cluster_relmap_authority_write_pending(bool shared_map, Oid dbid,
									   const char *map_image, uint32 image_size,
									   uint64 pending_generation,
									   const ClusterRelmapOwner *owner)
{
	char		image[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE];
	ClusterRelmapAuthorityHeader *hdr = (ClusterRelmapAuthorityHeader *) image;
	bool		have_prior;

	if (image_size == 0 || image_size > CLUSTER_RELMAP_IMAGE_MAX)
		ereport(PANIC,
				(errmsg("relmap authority image size %u out of range", image_size)));

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
	if (!have_prior)
	{
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
	char		image[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE];
	ClusterRelmapAuthorityHeader *hdr = (ClusterRelmapAuthorityHeader *) image;

	if (!load_authority(shared_map, dbid, image))
		ereport(PANIC,
				(errmsg("relmap authority unavailable at publish for db %u", dbid)));

	if (hdr->pending_generation != generation)
	{
		/*
		 * Already published (idempotent retry) or a stale generation.  If the
		 * committed generation already matches, treat as done; otherwise this
		 * is a programming error in the activation layer.
		 */
		if (hdr->committed_generation == generation)
			return;
		ereport(PANIC,
				(errmsg("relmap authority publish generation mismatch: "
						"pending %llu committed %llu requested %llu",
						(unsigned long long) hdr->pending_generation,
						(unsigned long long) hdr->committed_generation,
						(unsigned long long) generation)));
	}

	/* Flip pending -> committed: copy the pending slot over the committed slot. */
	memcpy(image + COMMITTED_SLOT_OFFSET, image + PENDING_SLOT_OFFSET,
		   CLUSTER_RELMAP_IMAGE_MAX);
	hdr->committed_generation = generation;
	hdr->pending_generation = 0;

	finalize_header_crc(image);
	store_authority(shared_map, dbid, image);
}
