/*-------------------------------------------------------------------------
 *
 * cluster_cf_authority.c
 *	  Shared pg_control single-authority file (spec-5.6 Da1).
 *
 *	  Owns the one shared control file that backs every node's
 *	  $PGDATA/global/pg_control symlink under cluster.shared_data_dir.
 *	  Reads choose between the primary image and a strictly-validated
 *	  .bak fallback, failing closed when neither can be trusted; writes
 *	  are made torn-safe by writing a temp file and durable_rename()-ing
 *	  it over the primary (after first rolling the live primary into the
 *	  .bak so a single bad write is recoverable).
 *
 *	  The "when to fail-closed" decision is factored into two pure,
 *	  unit-tested functions (cluster_cf_classify_buffer and
 *	  cluster_cf_decide_source); the rest of this file is the surrounding
 *	  durable I/O.
 *
 *	  Cross-node freshness of a renamed image is NOT guaranteed by POSIX
 *	  rename alone and is established separately by the storage
 *	  rename-contract probe (spec §3.9 T6, Da2).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cf_authority.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md (Da1)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>

#include "catalog/pg_control.h"
#include "cluster/cluster_cf_authority.h"
#include "cluster/cluster_cf_stats.h"
#include "cluster/cluster_guc.h"
#include "port/pg_crc32c.h"
#include "storage/fd.h"

/*
 * build_path -- join cluster_shared_data_dir with a relative name into the
 * caller's buffer.  Returns false (empty buffer) when the shared root is
 * unset, so callers can fail-closed rather than touch a bogus path.
 */
static bool
build_path(char *dst, size_t dstlen, const char *relpath)
{
	if (cluster_shared_data_dir == NULL || cluster_shared_data_dir[0] == '\0')
	{
		if (dstlen > 0)
			dst[0] = '\0';
		return false;
	}
	snprintf(dst, dstlen, "%s/%s", cluster_shared_data_dir, relpath);
	return true;
}

/*
 * cluster_cf_shared_path / cluster_cf_bak_path -- accessors returning a
 * per-function static buffer (see header).  NULL when the root is unset.
 */
const char *
cluster_cf_shared_path(void)
{
	static char path[MAXPGPATH];

	if (!build_path(path, sizeof(path), CLUSTER_CF_REL_PATH))
		return NULL;
	return path;
}

const char *
cluster_cf_bak_path(void)
{
	static char path[MAXPGPATH];

	if (!build_path(path, sizeof(path), CLUSTER_CF_BAK_REL_PATH))
		return NULL;
	return path;
}

/*
 * cluster_cf_classify_buffer -- pure classification of one raw image.
 *
 *	CRC is checked first: a torn/corrupt image cannot have its other
 *	fields trusted (so byte order / identity are only examined once the
 *	CRC validates).  expected_sysid == 0 skips the identity cross-check.
 */
ClusterCfValidity
cluster_cf_classify_buffer(const char *buf, size_t len, uint64 expected_sysid)
{
	ControlFileData cf;
	pg_crc32c	crc;

	if (buf == NULL || len < sizeof(ControlFileData))
		return CLUSTER_CF_INVALID_SHORT;

	memcpy(&cf, buf, sizeof(cf));

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, buf, offsetof(ControlFileData, crc));
	FIN_CRC32C(crc);
	if (!EQ_CRC32C(crc, cf.crc))
		return CLUSTER_CF_INVALID_CRC;

	/* Foreign byte order: version is a nonzero multiple of 65536. */
	if (cf.pg_control_version % 65536 == 0 &&
		cf.pg_control_version / 65536 != 0)
		return CLUSTER_CF_INVALID_BYTE_ORDER;

	if (expected_sysid != 0 && cf.system_identifier != expected_sysid)
		return CLUSTER_CF_INVALID_IDENTITY;

	return CLUSTER_CF_VALID;
}

/*
 * cluster_cf_decide_source -- pure read-source decision.
 *
 *	A valid primary always wins.  The .bak is used only when it is valid
 *	AND passed the strict acceptance check (bak_strict_ok): a .bak that is
 *	merely CRC-valid but stale/unreplayable must not silently override a
 *	corrupt primary (spec §3.9 T3).  Otherwise fail-closed.
 */
ClusterCfReadSource
cluster_cf_decide_source(ClusterCfValidity primary, ClusterCfValidity bak,
						 bool bak_strict_ok)
{
	if (primary == CLUSTER_CF_VALID)
		return CLUSTER_CF_SOURCE_PRIMARY;
	if (bak == CLUSTER_CF_VALID && bak_strict_ok)
		return CLUSTER_CF_SOURCE_BAK;
	return CLUSTER_CF_SOURCE_FAILCLOSED;
}

/*
 * cluster_cf_bak_strict_ok -- strict (non-CRC-only) .bak acceptance (Dc2).
 *
 *	The caller has already classified the .bak as structurally VALID; this
 *	adds the spec §3.9 T3 conditions a corruption-recovery fallback must also
 *	satisfy: a matching system_identifier (when an expected one is known) and
 *	a checkpoint that is still recoverable.  A .bak that is merely CRC-correct
 *	but stale, foreign, or whose WAL is gone is rejected -- silently replaying
 *	from it would corrupt recovery.  Pure: the impure recoverability probe is
 *	performed by the caller and its result passed in.
 */
bool
cluster_cf_bak_strict_ok(const ControlFileData *bak, uint64 expected_sysid,
						 bool checkpoint_recoverable)
{
	if (bak == NULL)
		return false;
	if (expected_sysid != 0 && bak->system_identifier != expected_sysid)
		return false;
	return checkpoint_recoverable;
}

/*
 * read_image -- read one control-file image from `path` into `image`
 * (sizeof(ControlFileData) bytes) and classify it.  A missing/short file
 * classifies as INVALID_SHORT so the caller treats it as unusable.
 */
static ClusterCfValidity
read_image(const char *path, char *image)
{
	int			fd;
	int			r;

	if (path == NULL)
		return CLUSTER_CF_INVALID_SHORT;

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return CLUSTER_CF_INVALID_SHORT;

	r = read(fd, image, sizeof(ControlFileData));
	CloseTransientFile(fd);

	if (r != (int) sizeof(ControlFileData))
		return CLUSTER_CF_INVALID_SHORT;

	return cluster_cf_classify_buffer(image, sizeof(ControlFileData), 0);
}

/*
 * cluster_cf_authority_read -- load the shared authority into *out.
 *
 *	Tries the primary first; on failure falls back to a valid .bak.
 *	Returns false (leaving *out untouched) when the read must fail-closed.
 *	Never ereports, so it is safe on the bootstrap early-read path.
 */
bool
cluster_cf_authority_read(ControlFileData *out)
{
	char		primary_img[sizeof(ControlFileData)];
	char		bak_img[sizeof(ControlFileData)];
	ClusterCfValidity pv;
	ClusterCfValidity bv;
	bool		bak_strict_ok;
	ClusterCfReadSource src;

	pv = read_image(cluster_cf_shared_path(), primary_img);
	if (pv == CLUSTER_CF_VALID)
	{
		memcpy(out, primary_img, sizeof(ControlFileData));
		return true;
	}

	bv = read_image(cluster_cf_bak_path(), bak_img);

	/*
	 * spec-5.6 Dc2 strict .bak acceptance: a corruption-recovery fallback to
	 * the .bak is taken only when it is structurally valid AND its checkpoint
	 * is still recoverable (the redo WAL segment exists).  A .bak that is
	 * merely CRC-correct but stale/unreplayable is rejected so recovery never
	 * silently restarts from an unreachable checkpoint (spec §3.9 T3).  No
	 * independent expected identity exists at this layer -- the primary, the
	 * only same-storage reference, is the corrupt image -- so the identity leg
	 * is skipped here (0); the symlink/migrate gates already reject a foreign
	 * authority at startup (§3.2 M3).
	 */
	if (bv == CLUSTER_CF_VALID)
	{
		ControlFileData bak_cf;

		memcpy(&bak_cf, bak_img, sizeof(ControlFileData));
		bak_strict_ok = cluster_cf_bak_strict_ok(&bak_cf, 0,
												  cluster_cf_bak_checkpoint_recoverable(&bak_cf));
	}
	else
		bak_strict_ok = false;

	src = cluster_cf_decide_source(pv, bv, bak_strict_ok);
	switch (src)
	{
		case CLUSTER_CF_SOURCE_PRIMARY:
			memcpy(out, primary_img, sizeof(ControlFileData));
			return true;
		case CLUSTER_CF_SOURCE_BAK:
			cluster_cf_counter_inc(CLUSTER_CF_BAK_FALLBACK);
			memcpy(out, bak_img, sizeof(ControlFileData));
			return true;
		case CLUSTER_CF_SOURCE_FAILCLOSED:
			break;
	}
	return false;
}

/*
 * write_durable -- write `buf` (PG_CONTROL_FILE_SIZE bytes) to `tmp`, fsync
 * it, then durable_rename() it over `final` (which fsyncs the directory).
 * PANICs on any I/O failure, mirroring the stock update_controlfile contract.
 */
static void
write_durable(const char *tmp, const char *final, const char *buf)
{
	int			fd;

	fd = OpenTransientFile(tmp, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY);
	if (fd < 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", tmp)));

	errno = 0;
	if (write(fd, buf, PG_CONTROL_FILE_SIZE) != PG_CONTROL_FILE_SIZE)
	{
		if (errno == 0)
			errno = ENOSPC;
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m", tmp)));
	}

	if (pg_fsync(fd) != 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", tmp)));

	if (CloseTransientFile(fd) != 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", tmp)));

	if (durable_rename(tmp, final, PANIC) != 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not rename file \"%s\" to \"%s\": %m",
						tmp, final)));
}

/*
 * roll_primary_to_bak -- if the primary currently exists, copy its raw bytes
 * into the .bak (durably) so a subsequent bad primary write is recoverable.
 * A missing or short primary (first write) is simply skipped.
 */
static void
roll_primary_to_bak(const char *primary, const char *bak, const char *baktmp)
{
	char		buf[PG_CONTROL_FILE_SIZE];
	int			fd;
	int			r;

	fd = OpenTransientFile(primary, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return;					/* first write: no prior primary to preserve */

	r = read(fd, buf, PG_CONTROL_FILE_SIZE);
	CloseTransientFile(fd);
	if (r != PG_CONTROL_FILE_SIZE)
		return;					/* short/odd primary: don't manufacture a .bak */

	write_durable(baktmp, bak, buf);
}

/*
 * cluster_cf_authority_write -- atomically replace the shared authority with
 * *cf.  Recomputes the CRC, rolls the live primary into .bak, then installs
 * the new image via temp-write + durable_rename.  Caller must hold CF X.
 */
void
cluster_cf_authority_write(const ControlFileData *cf)
{
	char		buffer[PG_CONTROL_FILE_SIZE];
	char		primary[MAXPGPATH];
	char		bak[MAXPGPATH];
	char		tmp[MAXPGPATH];
	char		baktmp[MAXPGPATH];
	ControlFileData local;

	if (!build_path(primary, sizeof(primary), CLUSTER_CF_REL_PATH) ||
		!build_path(bak, sizeof(bak), CLUSTER_CF_BAK_REL_PATH) ||
		!build_path(tmp, sizeof(tmp), CLUSTER_CF_TMP_REL_PATH) ||
		!build_path(baktmp, sizeof(baktmp), CLUSTER_CF_BAK_TMP_REL_PATH))
		ereport(PANIC,
				(errmsg("cluster shared_data_dir is not configured")));

	/* Recompute CRC over a private copy (cf is const). */
	memcpy(&local, cf, sizeof(local));
	INIT_CRC32C(local.crc);
	COMP_CRC32C(local.crc, (char *) &local, offsetof(ControlFileData, crc));
	FIN_CRC32C(local.crc);

	/* Zero-pad to the full on-disk size, as update_controlfile does. */
	memset(buffer, 0, PG_CONTROL_FILE_SIZE);
	memcpy(buffer, &local, sizeof(local));

	roll_primary_to_bak(primary, bak, baktmp);
	write_durable(tmp, primary, buffer);
}
