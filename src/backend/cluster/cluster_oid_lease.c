/*-------------------------------------------------------------------------
 *
 * cluster_oid_lease.c
 *	  Shared OID authority: pure lease math + torn-safe authority file I/O
 *	  (spec-6.14 D6).
 *
 *	  The shared OID authority is a single small durable file under
 *	  cluster.shared_data_dir holding the cluster-wide next-OID high-water
 *	  mark.  This file mirrors cluster_cf_authority's torn-safe write pattern
 *	  (temp + fsync + .bak roll + durable_rename) and fail-closed read (primary
 *	  then .bak, false when neither is trustworthy).  The per-node lease shmem
 *	  and the cross-node refill live in cluster_oid_lease_shmem.c; the pure
 *	  math here (carve / consume / normalize / classify) is standalone-linkable
 *	  so cluster_unit exercises it without a running backend.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_oid_lease.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.14-shared-catalog-single-authority.md (D6, U5-U7)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/transam.h"			/* FirstNormalObjectId */
#include "cluster/cluster_guc.h"	/* cluster_shared_data_dir */
#include "cluster/cluster_oid_lease.h"
#include "storage/fd.h"

/* On-disk image size: the fixed header, exactly. */
#define CLUSTER_OID_AUTHORITY_FILE_SIZE ((int) sizeof(ClusterOidAuthorityHeader))

/* ============================================================
 * Pure layer (no elog/shmem/fd; standalone-linkable for cluster_unit).
 * ============================================================ */

/*
 * cluster_oid_resid_encode -- singleton OID-authority resource id.  All map
 * fields zero; only the type byte places it in the OID namespace.  Mirrors
 * cluster_cf_resid_encode.
 */
void
cluster_oid_resid_encode(ClusterResId *dst)
{
	Assert(dst != NULL);
	if (dst == NULL)
		return;

	dst->field1 = 0;
	dst->field2 = 0;
	dst->field3 = 0;
	dst->field4 = 0;
	dst->type = CLUSTER_OID_RESID_TYPE;
	dst->lockmethodid = DEFAULT_LOCKMETHOD;
}

/*
 * cluster_oid_authority_classify -- validate an authority image buffer.
 */
ClusterOidAuthorityValidity
cluster_oid_authority_classify(const char *buf, size_t len)
{
	ClusterOidAuthorityHeader hdr;
	pg_crc32c	crc;

	if (buf == NULL || len < sizeof(ClusterOidAuthorityHeader))
		return CLUSTER_OID_AUTHORITY_INVALID_SHORT;

	memcpy(&hdr, buf, sizeof(hdr));

	if (hdr.magic != CLUSTER_OID_AUTHORITY_MAGIC)
		return CLUSTER_OID_AUTHORITY_INVALID_MAGIC;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, buf, offsetof(ClusterOidAuthorityHeader, crc));
	FIN_CRC32C(crc);
	if (!EQ_CRC32C(crc, hdr.crc))
		return CLUSTER_OID_AUTHORITY_INVALID_CRC;

	return CLUSTER_OID_AUTHORITY_VALID;
}

/*
 * cluster_oid_lease_normalize_start -- force a high-water past the reserved
 * range (mirrors the stock GetNewObjectId wraparound handling).
 */
Oid
cluster_oid_lease_normalize_start(Oid start)
{
	if (start < (Oid) FirstNormalObjectId)
		return (Oid) FirstNormalObjectId;
	return start;
}

/*
 * cluster_oid_lease_consume -- hand out one OID, advancing the lease.  Returns
 * InvalidOid when exhausted (next == end).  carve() guarantees the block never
 * spans a reserved OID, so this is a plain next++ with unsigned wrap.
 */
Oid
cluster_oid_lease_consume(ClusterOidLease *lease)
{
	Oid			oid;

	Assert(lease != NULL);
	if (lease->next == lease->end)
		return InvalidOid;

	oid = lease->next;
	lease->next = oid + 1;		/* uint32 wrap is fine: end==0 stops at 2^32 */

	Assert(oid >= (Oid) FirstNormalObjectId);
	return oid;
}

/*
 * cluster_oid_lease_carve -- pure refill math.  See header.  Normalizes hw,
 * carves [start, start+size), and computes the value to write back.  When the
 * block would overflow into the reserved range it is capped at the top of the
 * OID space (end == 0) and the authority is reset to FirstNormalObjectId.
 */
void
cluster_oid_lease_carve(Oid hw, uint32 lease_size,
						Oid *out_start, Oid *out_end, Oid *out_new_authority)
{
	Oid			start = cluster_oid_lease_normalize_start(hw);
	Oid			end;

	Assert(lease_size > 0);

	end = start + lease_size;	/* may wrap */

	if (end <= start)
	{
		/*
		 * The block would wrap the 32-bit OID space and spill into the
		 * reserved (< FirstNormalObjectId) range.  Cap it at the top: an
		 * exclusive end of 0 covers [start, 2^32), which contains no reserved
		 * OID, and reset the authority so the next refill starts fresh at
		 * FirstNormalObjectId.
		 */
		end = 0;
		*out_new_authority = (Oid) FirstNormalObjectId;
	}
	else
		*out_new_authority = end;

	*out_start = start;
	*out_end = end;
}

/* ============================================================
 * Torn-safe authority file I/O (backend; mirrors cluster_cf_authority).
 * ============================================================ */

/*
 * build_path -- join cluster_shared_data_dir + relpath into dst.  Returns
 * false when the shared root is not configured.
 */
static bool
build_path(char *dst, size_t dstlen, const char *relpath)
{
	if (cluster_shared_data_dir == NULL || cluster_shared_data_dir[0] == '\0')
		return false;
	snprintf(dst, dstlen, "%s/%s", cluster_shared_data_dir, relpath);
	return true;
}

/*
 * write_durable -- write buf into tmp, fsync, then durable_rename over final.
 * PANICs on any I/O failure (mirrors cluster_cf_authority write_durable).
 */
static void
write_durable(const char *tmp, const char *final, const char *buf)
{
	int			fd;

	fd = OpenTransientFile(tmp, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY);
	if (fd < 0)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not open file \"%s\": %m", tmp)));

	errno = 0;
	if (write(fd, buf, CLUSTER_OID_AUTHORITY_FILE_SIZE) != CLUSTER_OID_AUTHORITY_FILE_SIZE)
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

/*
 * roll_primary_to_bak -- preserve an existing primary into .bak before a new
 * primary write.  A missing/short primary (first write) is skipped.
 */
static void
roll_primary_to_bak(const char *primary, const char *bak, const char *baktmp)
{
	char		buf[CLUSTER_OID_AUTHORITY_FILE_SIZE];
	int			fd;
	int			r;

	fd = OpenTransientFile(primary, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return;					/* first write: no prior primary */

	r = read(fd, buf, CLUSTER_OID_AUTHORITY_FILE_SIZE);
	CloseTransientFile(fd);
	if (r != CLUSTER_OID_AUTHORITY_FILE_SIZE)
		return;					/* short/odd primary: don't manufacture a .bak */

	write_durable(baktmp, bak, buf);
}

/*
 * read_image -- read a candidate authority file and classify it.  Missing or
 * short is INVALID_SHORT.
 */
static ClusterOidAuthorityValidity
read_image(const char *path, char *image)
{
	int			fd;
	int			r;

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return CLUSTER_OID_AUTHORITY_INVALID_SHORT;

	r = read(fd, image, CLUSTER_OID_AUTHORITY_FILE_SIZE);
	CloseTransientFile(fd);
	if (r != CLUSTER_OID_AUTHORITY_FILE_SIZE)
		return CLUSTER_OID_AUTHORITY_INVALID_SHORT;

	return cluster_oid_authority_classify(image, CLUSTER_OID_AUTHORITY_FILE_SIZE);
}

/*
 * cluster_oid_authority_read -- fail-closed read of the shared high-water.
 * Tries primary then .bak; returns false when neither is trustworthy.  Never
 * ereports (safe on the bootstrap early-read path).
 */
bool
cluster_oid_authority_read(Oid *next_oid)
{
	char		primary_path[MAXPGPATH];
	char		bak_path[MAXPGPATH];
	char		image[CLUSTER_OID_AUTHORITY_FILE_SIZE];
	ClusterOidAuthorityHeader hdr;

	if (!build_path(primary_path, sizeof(primary_path), CLUSTER_OID_AUTHORITY_REL_PATH)
		|| !build_path(bak_path, sizeof(bak_path), CLUSTER_OID_AUTHORITY_BAK_REL_PATH))
		return false;

	if (read_image(primary_path, image) != CLUSTER_OID_AUTHORITY_VALID)
	{
		if (read_image(bak_path, image) != CLUSTER_OID_AUTHORITY_VALID)
			return false;		/* fail-closed: neither trustworthy */
	}

	memcpy(&hdr, image, sizeof(hdr));
	*next_oid = hdr.next_oid;
	return true;
}

/*
 * cluster_oid_authority_present -- does any authority image (primary or .bak)
 * exist on disk at all, trustworthy or not?  Lets the catalog bootstrap
 * distinguish "absent: genuine first seed" from "present but corrupt:
 * fail-closed" (spec-6.14 §3.6) -- a corrupt authority must never be silently
 * re-seeded from the checkpointed shared pg_control high-water, which can lag
 * OID ranges the cluster already leased out.  Never ereports.
 */
bool
cluster_oid_authority_present(void)
{
	char		primary_path[MAXPGPATH];
	char		bak_path[MAXPGPATH];
	struct stat st;

	if (!build_path(primary_path, sizeof(primary_path), CLUSTER_OID_AUTHORITY_REL_PATH)
		|| !build_path(bak_path, sizeof(bak_path), CLUSTER_OID_AUTHORITY_BAK_REL_PATH))
		return false;

	return stat(primary_path, &st) == 0 || stat(bak_path, &st) == 0;
}

/*
 * cluster_oid_authority_write -- atomically replace the authority high-water.
 * Recomputes CRC, rolls the live primary into .bak, installs via temp +
 * durable_rename.  Caller must hold the OID X lock.
 */
void
cluster_oid_authority_write(Oid next_oid)
{
	char		buffer[CLUSTER_OID_AUTHORITY_FILE_SIZE];
	char		primary[MAXPGPATH];
	char		bak[MAXPGPATH];
	char		tmp[MAXPGPATH];
	char		baktmp[MAXPGPATH];
	ClusterOidAuthorityHeader hdr;

	if (!build_path(primary, sizeof(primary), CLUSTER_OID_AUTHORITY_REL_PATH)
		|| !build_path(bak, sizeof(bak), CLUSTER_OID_AUTHORITY_BAK_REL_PATH)
		|| !build_path(tmp, sizeof(tmp), CLUSTER_OID_AUTHORITY_TMP_REL_PATH)
		|| !build_path(baktmp, sizeof(baktmp), CLUSTER_OID_AUTHORITY_BAK_TMP_REL_PATH))
		ereport(PANIC, (errmsg("cluster shared_data_dir is not configured")));

	memset(&hdr, 0, sizeof(hdr));
	hdr.magic = CLUSTER_OID_AUTHORITY_MAGIC;
	hdr.version = CLUSTER_OID_AUTHORITY_VERSION;
	hdr.next_oid = next_oid;
	hdr.reserved = 0;
	INIT_CRC32C(hdr.crc);
	COMP_CRC32C(hdr.crc, (char *) &hdr, offsetof(ClusterOidAuthorityHeader, crc));
	FIN_CRC32C(hdr.crc);

	memset(buffer, 0, sizeof(buffer));
	memcpy(buffer, &hdr, sizeof(hdr));

	roll_primary_to_bak(primary, bak, baktmp);
	write_durable(tmp, primary, buffer);
}

/*
 * cluster_oid_authority_seed_if_absent -- see header.  Read-then-seed: if the
 * authority is already present (any trustworthy image) do nothing; otherwise
 * write the normalized initial high-water.
 */
bool
cluster_oid_authority_seed_if_absent(Oid initial_next_oid)
{
	Oid			existing;

	if (cluster_oid_authority_read(&existing))
		return false;			/* already seeded (join node / prior seed) */

	cluster_oid_authority_write(cluster_oid_lease_normalize_start(initial_next_oid));
	return true;
}
