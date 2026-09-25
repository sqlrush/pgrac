/*-------------------------------------------------------------------------
 *
 * cluster_cf_authority.c
 *	  Shared pg_control single-authority file (spec-5.6).
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
 *	  rename-contract probe.
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
 *	  Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "catalog/catversion.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_cf_authority.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_cf_stats.h"
#include "cluster/cluster_guc.h"
#include "common/cryptohash.h"
#include "miscadmin.h"
#include "port/pg_crc32c.h"
#include "storage/fd.h"
#include "cluster_control_root_private.h"

/*
 * build_path -- join cluster_shared_data_dir with a relative name into the
 * caller's buffer.  Returns false (empty buffer) when the shared root is
 * unset, so callers can fail-closed rather than touch a bogus path.
 */
static bool
build_path(char *dst, size_t dstlen, const char *relpath)
{
	if (cluster_shared_data_dir == NULL || cluster_shared_data_dir[0] == '\0') {
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
	pg_crc32c crc;

	if (buf == NULL || len < sizeof(ControlFileData))
		return CLUSTER_CF_INVALID_SHORT;

	memcpy(&cf, buf, sizeof(cf));

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, buf, offsetof(ControlFileData, crc));
	FIN_CRC32C(crc);
	if (!EQ_CRC32C(crc, cf.crc))
		return CLUSTER_CF_INVALID_CRC;

	/* Foreign byte order: version is a nonzero multiple of 65536. */
	if (cf.pg_control_version % 65536 == 0 && cf.pg_control_version / 65536 != 0)
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
 *	corrupt primary.  Otherwise fail-closed.
 */
ClusterCfReadSource
cluster_cf_decide_source(ClusterCfValidity primary, ClusterCfValidity bak, bool bak_strict_ok)
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
 *	adds the conditions a corruption-recovery fallback must also
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
	int fd;
	int r;

	if (path == NULL)
		return CLUSTER_CF_INVALID_SHORT;

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return CLUSTER_CF_INVALID_SHORT;

	r = read(fd, image, sizeof(ControlFileData));
	CloseTransientFile(fd);

	if (r != (int)sizeof(ControlFileData))
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
	char primary_img[sizeof(ControlFileData)];
	char bak_img[sizeof(ControlFileData)];
	ClusterCfValidity pv;
	ClusterCfValidity bv;
	bool bak_strict_ok;
	ClusterCfReadSource src;

	/* PGRAC: runtime v2 never reads the compatibility projection or .bak.
	 * The caller already owns CF-S/X; the adapter checks its exact local
	 * runtime owner again after reading. Author: SqlRush <sqlrush@gmail.com>
	 */
	if (cluster_shared_config) {
		ControlFileData verified;
		ClusterControlRootResult result;

		if (out == NULL)
			return false;
		/* The native caller passes shared ControlFile, which also backs
		 * GetSystemIdentifier(). Never clear it as decoder scratch or expose
		 * an unverified partial view. Preserve this API's untouched-on-failure
		 * contract; a false return is not authority to use the old contents.
		 */
		result = cluster_control_root_v2_read_runtime_local_locked(&verified);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
			&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
			return false;
		*out = verified;
		return true;
	}
	pv = read_image(cluster_cf_shared_path(), primary_img);
	if (pv == CLUSTER_CF_VALID) {
		memcpy(out, primary_img, sizeof(ControlFileData));
		return true;
	}

	bv = read_image(cluster_cf_bak_path(), bak_img);

	/*
	 * spec-5.6 Dc2 strict .bak acceptance: a corruption-recovery fallback to
	 * the .bak is taken only when it is structurally valid AND its checkpoint
	 * is still recoverable (the redo WAL segment exists).  A .bak that is
	 * merely CRC-correct but stale/unreplayable is rejected so recovery never
	 * silently restarts from an unreachable checkpoint.  No
	 * independent expected identity exists at this layer -- the primary, the
	 * only same-storage reference, is the corrupt image -- so the identity leg
	 * is skipped here (0); the symlink/migrate gates already reject a foreign
	 * authority at startup.
	 */
	if (bv == CLUSTER_CF_VALID) {
		ControlFileData bak_cf;

		memcpy(&bak_cf, bak_img, sizeof(ControlFileData));
		bak_strict_ok
			= cluster_cf_bak_strict_ok(&bak_cf, 0, cluster_cf_bak_checkpoint_recoverable(&bak_cf));
	} else
		bak_strict_ok = false;

	src = cluster_cf_decide_source(pv, bv, bak_strict_ok);
	switch (src) {
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
	int fd;

	fd = OpenTransientFile(tmp, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY);
	if (fd < 0)
		ereport(PANIC, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", tmp)));

	errno = 0;
	if (write(fd, buf, PG_CONTROL_FILE_SIZE) != PG_CONTROL_FILE_SIZE) {
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

/*
 * roll_primary_to_bak -- if the primary currently exists, copy its raw bytes
 * into the .bak (durably) so a subsequent bad primary write is recoverable.
 * A missing or short primary (first write) is simply skipped.
 */
static void
roll_primary_to_bak(const char *primary, const char *bak, const char *baktmp)
{
	char buf[PG_CONTROL_FILE_SIZE];
	int fd;
	int r;

	fd = OpenTransientFile(primary, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return; /* first write: no prior primary to preserve */

	r = read(fd, buf, PG_CONTROL_FILE_SIZE);
	CloseTransientFile(fd);
	if (r != PG_CONTROL_FILE_SIZE)
		return; /* short/odd primary: don't manufacture a .bak */

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
	char buffer[PG_CONTROL_FILE_SIZE];
	char primary[MAXPGPATH];
	char bak[MAXPGPATH];
	char tmp[MAXPGPATH];
	char baktmp[MAXPGPATH];
	ControlFileData local;

	/* PGRAC: an untyped native struct cannot overwrite common/thread authority.
	 * Use purpose-bound v2 publishers, never the old projection writer.
	 * Author: SqlRush <sqlrush@gmail.com>
	 */
	if (cluster_shared_config)
		ereport(PANIC, (errcode(ERRCODE_CLUSTER_CONTROLFILE_AUTHORITY_UNAVAILABLE),
						errmsg("root-v2 control update requires a purpose-bound publisher")));
	if (!build_path(primary, sizeof(primary), CLUSTER_CF_REL_PATH)
		|| !build_path(bak, sizeof(bak), CLUSTER_CF_BAK_REL_PATH)
		|| !build_path(tmp, sizeof(tmp), CLUSTER_CF_TMP_REL_PATH)
		|| !build_path(baktmp, sizeof(baktmp), CLUSTER_CF_BAK_TMP_REL_PATH))
		ereport(PANIC, (errmsg("cluster shared_data_dir is not configured")));

	/* Recompute CRC over a private copy (cf is const). */
	memcpy(&local, cf, sizeof(local));
	INIT_CRC32C(local.crc);
	COMP_CRC32C(local.crc, (char *)&local, offsetof(ControlFileData, crc));
	FIN_CRC32C(local.crc);

	/* Zero-pad to the full on-disk size, as update_controlfile does. */
	memset(buffer, 0, PG_CONTROL_FILE_SIZE);
	memcpy(buffer, &local, sizeof(local));

	roll_primary_to_bak(primary, bak, baktmp);
	write_durable(tmp, primary, buffer);
}

/* PGRAC: immutable object entry points; no root publication/admission here.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#define CF_IMAGE_STAGED 1
#define CF_IMAGE_INSTALLED 2
#define CF_IMAGE_DISCARDED 3

typedef struct CfImageDirs {
	int objects;
	int staging;
	struct stat object_stat;
	struct stat staging_stat;
} CfImageDirs;

static bool
cf_image_nonzero(const uint8 *bytes, size_t len)
{
	size_t i;

	if (bytes == NULL)
		return false;
	for (i = 0; i < len; ++i)
		if (bytes[i] != 0)
			return true;
	return false;
}

static bool
cf_image_owned(const struct stat *st, bool directory)
{
	return (directory ? S_ISDIR(st->st_mode) : S_ISREG(st->st_mode)) && st->st_uid == geteuid()
		   && (st->st_mode & (S_IWGRP | S_IWOTH)) == 0;
}

static void
cf_image_close(int fd, ClusterControlRootResult *result)
{
	if (fd >= 0 && close(fd) != 0)
		*result = CLUSTER_CONTROL_ROOT_IO_ERROR;
}

static void
cf_image_close_dirs(CfImageDirs *dirs, ClusterControlRootResult *result)
{
	cf_image_close(dirs->staging, result);
	cf_image_close(dirs->objects, result);
}

/* No allocation, ereport or interrupt processing while these bounded, raw
 * descriptors are open. All public exits close them, including error exits.
 */
static ClusterControlRootResult
cf_image_open_dirs(CfImageDirs *dirs, bool need_staging)
{
	const int flags = O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC;
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	struct stat st;
	int root = -1;
	int global = -1;

	memset(dirs, 0, sizeof(*dirs));
	dirs->objects = dirs->staging = -1;
	if (cluster_shared_data_dir == NULL || cluster_shared_data_dir[0] == '\0')
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	root = open(cluster_shared_data_dir, flags);
	if (root < 0 || fstat(root, &st) != 0 || !cf_image_owned(&st, true))
		goto done;
	global = openat(root, "global", flags);
	if (global < 0 || fstat(global, &st) != 0 || !cf_image_owned(&st, true))
		goto done;
	dirs->objects = openat(global, "control_images", flags);
	if (dirs->objects < 0 || fstat(dirs->objects, &dirs->object_stat) != 0
		|| !cf_image_owned(&dirs->object_stat, true))
		goto done;
	if (need_staging) {
		dirs->staging = openat(dirs->objects, ".staging", flags);
		if (dirs->staging < 0 || fstat(dirs->staging, &dirs->staging_stat) != 0
			|| !cf_image_owned(&dirs->staging_stat, true))
			goto done;
	}
	result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
done:
	cf_image_close(global, &result);
	cf_image_close(root, &result);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		cf_image_close_dirs(dirs, &result);
		dirs->objects = dirs->staging = -1;
	}
	return result;
}

static void
cf_image_hex(const uint8 *bytes, size_t len, char *out)
{
	static const char hex[] = "0123456789abcdef";
	size_t i;

	for (i = 0; i < len; ++i) {
		out[i * 2] = hex[bytes[i] >> 4];
		out[i * 2 + 1] = hex[bytes[i] & 15];
	}
	out[len * 2] = '\0';
}

static void
cf_image_names(uint64 generation, const uint8 hash[32], const uint8 *uuid, char final[96],
			   char temp[40])
{
	char hex[65];

	cf_image_hex(hash, 32, hex);
	snprintf(final, 96, UINT64_FORMAT "-%s.bin", generation, hex);
	if (uuid != NULL) {
		cf_image_hex(uuid, 16, hex);
		snprintf(temp, 40, "%s.tmp", hex);
	}
}

static bool
cf_image_hash(pg_cryptohash_ctx *ctx, const uint8 *bytes, uint8 hash[32])
{
	return pg_cryptohash_init(ctx) >= 0
		   && pg_cryptohash_update(ctx, bytes, PG_CONTROL_FILE_SIZE) >= 0
		   && pg_cryptohash_final(ctx, hash, 32) >= 0;
}

static bool
cf_image_stage_valid(const ClusterCfImageStage *stage)
{
	return stage != NULL && stage->generation != 0 && stage->system_identifier != 0
		   && stage->owner_pid == (uint32)getpid()
		   && (stage->state == CF_IMAGE_STAGED || stage->state == CF_IMAGE_INSTALLED
			   || stage->state == CF_IMAGE_DISCARDED)
		   && cf_image_nonzero(stage->operation_uuid, 16)
		   && cf_image_nonzero(stage->image_sha256, 32);
}

static bool
cf_image_dirs_match(const CfImageDirs *dirs, const ClusterCfImageStage *stage)
{
	return (uint64)dirs->object_stat.st_dev == stage->object_dir_dev
		   && (uint64)dirs->object_stat.st_ino == stage->object_dir_ino
		   && (uint64)dirs->staging_stat.st_dev == stage->staging_dir_dev
		   && (uint64)dirs->staging_stat.st_ino == stage->staging_dir_ino;
}

/* Never unlink a name that no longer denotes this process's exact inode.
 * Other actors must not mutate an operation owner's staging entries; crash
 * cleanup is separately serialized with that owner's proven retirement.
 */
static bool
cf_image_exact_entry(int dir, const char *name, const ClusterCfImageStage *stage)
{
	struct stat st;

	return fstatat(dir, name, &st, AT_SYMLINK_NOFOLLOW) == 0 && cf_image_owned(&st, false)
		   && (uint64)st.st_dev == stage->file_dev && (uint64)st.st_ino == stage->file_ino;
}

ClusterControlRootResult
cluster_cf_control_image_encode(const ControlFileData *cf, uint8 bytes[PG_CONTROL_FILE_SIZE])
{
	ControlFileData image;

	if (bytes == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(bytes, 0, PG_CONTROL_FILE_SIZE);
	if (cf == NULL || cf->system_identifier == 0)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (cf->pg_control_version != PG_CONTROL_VERSION
		|| cf->catalog_version_no != CATALOG_VERSION_NO)
		return CLUSTER_CONTROL_ROOT_BAD_VERSION;

	/* Named fields only: never hash uninitialized native structure padding.
	 * Preserve recovery/backup/parameter evidence; admission is not this API.
	 */
	memset(&image, 0, sizeof(image));
#define CF_COPY(field) image.field = cf->field
	CF_COPY(system_identifier);
	CF_COPY(pg_control_version);
	CF_COPY(catalog_version_no);
	CF_COPY(state);
	CF_COPY(time);
	CF_COPY(checkPoint);
	CF_COPY(checkPointCopy.redo);
	CF_COPY(checkPointCopy.ThisTimeLineID);
	CF_COPY(checkPointCopy.PrevTimeLineID);
	CF_COPY(checkPointCopy.fullPageWrites);
	CF_COPY(checkPointCopy.nextXid);
	CF_COPY(checkPointCopy.nextOid);
	CF_COPY(checkPointCopy.nextMulti);
	CF_COPY(checkPointCopy.nextMultiOffset);
	CF_COPY(checkPointCopy.oldestXid);
	CF_COPY(checkPointCopy.oldestXidDB);
	CF_COPY(checkPointCopy.oldestMulti);
	CF_COPY(checkPointCopy.oldestMultiDB);
	CF_COPY(checkPointCopy.time);
	CF_COPY(checkPointCopy.oldestCommitTsXid);
	CF_COPY(checkPointCopy.newestCommitTsXid);
	CF_COPY(checkPointCopy.oldestActiveXid);
	CF_COPY(unloggedLSN);
	CF_COPY(minRecoveryPoint);
	CF_COPY(minRecoveryPointTLI);
	CF_COPY(backupStartPoint);
	CF_COPY(backupEndPoint);
	CF_COPY(backupEndRequired);
	CF_COPY(wal_level);
	CF_COPY(wal_log_hints);
	CF_COPY(MaxConnections);
	CF_COPY(max_worker_processes);
	CF_COPY(max_wal_senders);
	CF_COPY(max_prepared_xacts);
	CF_COPY(max_locks_per_xact);
	CF_COPY(track_commit_timestamp);
	CF_COPY(maxAlign);
	CF_COPY(floatFormat);
	CF_COPY(blcksz);
	CF_COPY(relseg_size);
	CF_COPY(xlog_blcksz);
	CF_COPY(xlog_seg_size);
	CF_COPY(nameDataLen);
	CF_COPY(indexMaxKeys);
	CF_COPY(toast_max_chunk_size);
	CF_COPY(loblksize);
	CF_COPY(float8ByVal);
	CF_COPY(data_checksum_version);
#undef CF_COPY
	memcpy(image.mock_authentication_nonce, cf->mock_authentication_nonce, MOCK_AUTH_NONCE_LEN);
	INIT_CRC32C(image.crc);
	COMP_CRC32C(image.crc, &image, offsetof(ControlFileData, crc));
	FIN_CRC32C(image.crc);
	memcpy(bytes, &image, sizeof(image));
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

/* PGRAC: compatibility output is not a second control authority. The root
 * publisher owns CF-X and supplies its just-revalidated selected view.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static bool
cf_projection_same(const struct stat *left, const struct stat *right)
{
	return left->st_dev == right->st_dev && left->st_ino == right->st_ino;
}

static bool
cf_projection_dirs_current(int root, const struct stat *root_st, const struct stat *global_st)
{
	struct stat fresh;
	int fd = open(cluster_shared_data_dir, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	bool valid = fd >= 0 && fstat(fd, &fresh) == 0 && cf_image_owned(&fresh, true)
				 && cf_projection_same(root_st, &fresh);

	if (fd >= 0 && close(fd) != 0)
		valid = false;
	return valid && fstatat(root, "global", &fresh, AT_SYMLINK_NOFOLLOW) == 0
		   && cf_image_owned(&fresh, true) && cf_projection_same(global_st, &fresh);
}

static bool
cf_projection_entry(int dir, const char *name, const struct stat *expected)
{
	struct stat st;
	return fstatat(dir, name, &st, AT_SYMLINK_NOFOLLOW) == 0 && cf_image_owned(&st, false)
		   && st.st_nlink == 1 && cf_projection_same(&st, expected);
}

static bool
cf_projection_target_safe(int global)
{
	struct stat st;
	if (fstatat(global, "pg_control", &st, AT_SYMLINK_NOFOLLOW) != 0)
		return errno == ENOENT;
	return cf_image_owned(&st, false) && st.st_nlink == 1;
}

ClusterControlRootResult
cluster_cf_control_projection_write_locked(const ControlFileData *selected)
{
	uint8 bytes[PG_CONTROL_FILE_SIZE], actual[PG_CONTROL_FILE_SIZE], uuid[16];
	char hex[33], temp[64];
	struct stat root_st, global_st, temp_st, read_st;
	int root = -1, global = -1, fd = -1, check = -1;
	bool owned_temp = false, installed = false;
	ClusterControlRootResult result;
	size_t done = 0;
	const int flags = O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC;

	if (!cluster_shared_config || !enableFsync || selected == NULL
		|| cluster_shared_data_dir == NULL || cluster_shared_data_dir[0] != '/')
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (!cluster_cf_held_is_clusterwide(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	if (cluster_cf_classify_buffer((const char *)selected, sizeof(*selected),
								   selected->system_identifier)
		!= CLUSTER_CF_VALID)
		return CLUSTER_CONTROL_ROOT_BAD_BODY_CRC;
	result = cluster_cf_control_image_encode(selected, bytes);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	/* Random generation and native canonicalization precede every raw FD. */
	if (!pg_strong_random(uuid, sizeof(uuid)))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	cf_image_hex(uuid, sizeof(uuid), hex);
	snprintf(temp, sizeof(temp), ".pg_control-%s.tmp", hex);
	result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	root = open(cluster_shared_data_dir, flags);
	if (root < 0 || fstat(root, &root_st) != 0 || !cf_image_owned(&root_st, true))
		goto cleanup;
	global = openat(root, "global", flags);
	if (global < 0 || fstat(global, &global_st) != 0 || !cf_image_owned(&global_st, true)
		|| !cf_projection_target_safe(global))
		goto cleanup;
	fd = openat(global, temp, O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC | PG_BINARY, 0600);
	if (fd < 0 || fstat(fd, &temp_st) != 0 || !cf_image_owned(&temp_st, false)
		|| temp_st.st_nlink != 1)
		goto cleanup;
	owned_temp = true;
	while (done < sizeof(bytes)) {
		ssize_t n = write(fd, bytes + done, sizeof(bytes) - done);
		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			goto cleanup;
		done += n;
	}
	if (pg_fsync(fd) != 0)
		goto cleanup;
	if (!cf_projection_dirs_current(root, &root_st, &global_st)
		|| !cf_projection_entry(global, temp, &temp_st) || !cf_projection_target_safe(global)) {
		result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		goto cleanup;
	}
	if (renameat(global, temp, global, "pg_control") != 0)
		goto cleanup;
	installed = true;
	/* Once installed, never unlink or restore the previous projection. The
	 * already published root remains authoritative even if this sync fails. */
	if (pg_fsync(global) != 0)
		goto cleanup;
	check
		= openat(global, "pg_control", O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK | PG_BINARY);
	if (check < 0 || fstat(check, &read_st) != 0 || !cf_image_owned(&read_st, false)
		|| read_st.st_nlink != 1 || read_st.st_size != sizeof(actual)
		|| !cf_projection_same(&read_st, &temp_st))
		goto cleanup;
	done = 0;
	while (done < sizeof(actual)) {
		ssize_t n = read(check, actual + done, sizeof(actual) - done);
		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			goto cleanup;
		done += n;
	}
	if (memcmp(actual, bytes, sizeof(actual)) != 0
		|| !cf_projection_dirs_current(root, &root_st, &global_st)
		|| !cf_projection_entry(global, "pg_control", &temp_st)) {
		result = CLUSTER_CONTROL_ROOT_POSTREAD_FAILED;
		goto cleanup;
	}
	result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
cleanup:
	/* No callback/allocator/interrupt processing while these descriptors are
	 * live. Failure cleanup can remove only this exact unpublished temp. */
	if (owned_temp && !installed && cf_projection_entry(global, temp, &temp_st))
		(void)unlinkat(global, temp, 0);
	cf_image_close(check, &result);
	cf_image_close(fd, &result);
	cf_image_close(global, &result);
	cf_image_close(root, &result);
	return result;
}

static ClusterControlRootResult
cf_image_read_at(int dir, const char *name, const ClusterCfImageStage *stage,
				 const uint8 expected_hash[32], uint64 sysid, pg_cryptohash_ctx *ctx,
				 uint8 bytes[PG_CONTROL_FILE_SIZE])
{
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	uint8 hash[32];
	uint8 canonical[PG_CONTROL_FILE_SIZE];
	ControlFileData native;
	struct stat st;
	size_t used = 0;
	int fd;

	/* O_NONBLOCK makes a malformed FIFO a refusal, not an unbounded wait. */
	fd = openat(dir, name, O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK | PG_BINARY);
	if (fd < 0)
		return errno == ENOENT ? CLUSTER_CONTROL_ROOT_ABSENT : result;
	if (fstat(fd, &st) != 0 || !cf_image_owned(&st, false))
		goto done;
	if (stage != NULL
		&& ((uint64)st.st_dev != stage->file_dev || (uint64)st.st_ino != stage->file_ino)) {
		result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		goto done;
	}
	if (st.st_size != PG_CONTROL_FILE_SIZE) {
		result = CLUSTER_CONTROL_ROOT_BAD_SIZE;
		goto done;
	}
	while (used < PG_CONTROL_FILE_SIZE) {
		ssize_t n = read(fd, bytes + used, PG_CONTROL_FILE_SIZE - used);

		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			goto done;
		used += n;
	}
	if (!cf_image_hash(ctx, bytes, hash))
		goto done;
	if (memcmp(hash, expected_hash, sizeof(hash)) != 0) {
		result = CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
		goto done;
	}
	if (cluster_cf_classify_buffer((const char *)bytes, PG_CONTROL_FILE_SIZE, sysid)
		== CLUSTER_CF_INVALID_IDENTITY) {
		result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		goto done;
	}
	if (cluster_cf_classify_buffer((const char *)bytes, PG_CONTROL_FILE_SIZE, sysid)
		!= CLUSTER_CF_VALID) {
		result = CLUSTER_CONTROL_ROOT_BAD_BODY_CRC;
		goto done;
	}
	memcpy(&native, bytes, sizeof(native));
	result = cluster_cf_control_image_encode(&native, canonical);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& memcmp(bytes, canonical, PG_CONTROL_FILE_SIZE) != 0)
		result = CLUSTER_CONTROL_ROOT_BAD_RESERVED;
done:
	cf_image_close(fd, &result);
	return result;
}

ClusterControlRootResult
cluster_cf_control_image_prepare(const ControlFileData *cf, uint64 generation,
								 const uint8 operation_uuid[16], ClusterCfImageStage *out)
{
	ClusterControlRootResult result;
	ClusterCfImageStage stage;
	CfImageDirs dirs;
	struct stat st;
	pg_cryptohash_ctx *ctx;
	uint8 bytes[PG_CONTROL_FILE_SIZE];
	char final[96], temp[40];
	size_t used = 0;
	int fd = -1;

	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (generation == 0 || !cf_image_nonzero(operation_uuid, 16) || !enableFsync)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = cluster_cf_control_image_encode(cf, bytes);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	memset(&stage, 0, sizeof(stage));
	stage.generation = generation;
	stage.system_identifier = cf->system_identifier;
	stage.owner_pid = (uint32)getpid();
	memcpy(stage.operation_uuid, operation_uuid, 16);
	/* The only fallible backend allocation precedes every file descriptor. */
	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL)
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (!cf_image_hash(ctx, bytes, stage.image_sha256)) {
		pg_cryptohash_free(ctx);
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	}
	pg_cryptohash_free(ctx);
	cf_image_names(generation, stage.image_sha256, operation_uuid, final, temp);
	result = cf_image_open_dirs(&dirs, true);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	stage.object_dir_dev = (uint64)dirs.object_stat.st_dev;
	stage.object_dir_ino = (uint64)dirs.object_stat.st_ino;
	stage.staging_dir_dev = (uint64)dirs.staging_stat.st_dev;
	stage.staging_dir_ino = (uint64)dirs.staging_stat.st_ino;
	result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	fd = openat(dirs.staging, temp,
				O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC | PG_BINARY, 0600);
	if (fd < 0 || fstat(fd, &st) != 0 || !cf_image_owned(&st, false))
		goto done;
	stage.file_dev = (uint64)st.st_dev;
	stage.file_ino = (uint64)st.st_ino;
	stage.state = CF_IMAGE_STAGED;
	while (used < sizeof(bytes)) {
		ssize_t n = write(fd, bytes + used, sizeof(bytes) - used);

		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			goto done;
		used += n;
	}
	if (pg_fsync(fd) != 0 || pg_fsync(dirs.staging) != 0)
		goto done;
	result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
done:
	cf_image_close(fd, &result);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY && stage.state == CF_IMAGE_STAGED
		&& cf_image_exact_entry(dirs.staging, temp, &stage)) {
		/* Best effort exact cleanup; the original failure remains a failure. */
		if (unlinkat(dirs.staging, temp, 0) == 0)
			(void)pg_fsync(dirs.staging);
	}
	cf_image_close_dirs(&dirs, &result);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		*out = stage;
	return result;
}

ClusterControlRootResult
cluster_cf_control_image_install(ClusterCfImageStage *stage)
{
	ClusterControlRootResult result;
	CfImageDirs dirs;
	pg_cryptohash_ctx *ctx;
	uint8 staged[PG_CONTROL_FILE_SIZE], installed[PG_CONTROL_FILE_SIZE];
	char final[96], temp[40];

	if (!cf_image_stage_valid(stage) || stage->state == CF_IMAGE_DISCARDED || !enableFsync)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (!cluster_cf_held_is_clusterwide(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL)
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	cf_image_names(stage->generation, stage->image_sha256, stage->operation_uuid, final, temp);
	result = cf_image_open_dirs(&dirs, true);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	if (!cf_image_dirs_match(&dirs, stage)) {
		result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		goto done;
	}
	if (stage->state == CF_IMAGE_STAGED) {
		result = cf_image_read_at(dirs.staging, temp, stage, stage->image_sha256,
								  stage->system_identifier, ctx, staged);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			goto done;
		/* EEXIST is not success until the immutable destination is verified. */
		if (linkat(dirs.staging, temp, dirs.objects, final, 0) != 0 && errno != EEXIST) {
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
			goto done;
		}
	}
	result = cf_image_read_at(dirs.objects, final, NULL, stage->image_sha256,
							  stage->system_identifier, ctx, installed);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	if (stage->state == CF_IMAGE_STAGED && memcmp(staged, installed, sizeof(staged)) != 0) {
		result = CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
		goto done;
	}
	if (pg_fsync(dirs.objects) != 0) {
		result = CLUSTER_CONTROL_ROOT_IO_ERROR;
		goto done;
	}
	if (stage->state == CF_IMAGE_STAGED) {
		if (!cf_image_exact_entry(dirs.staging, temp, stage)) {
			result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
			goto done;
		}
		if (unlinkat(dirs.staging, temp, 0) != 0) {
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
			goto done;
		}
		/* A failed final fsync must retry the sync, not look for a removed temp. */
		stage->state = CF_IMAGE_INSTALLED;
	}
	if (pg_fsync(dirs.staging) != 0)
		result = CLUSTER_CONTROL_ROOT_IO_ERROR;
done:
	cf_image_close_dirs(&dirs, &result);
	pg_cryptohash_free(ctx);
	return result;
}

ClusterControlRootResult
cluster_cf_control_image_discard(ClusterCfImageStage *stage)
{
	ClusterControlRootResult result;
	CfImageDirs dirs;
	char final[96], temp[40];

	if (!cf_image_stage_valid(stage) || !enableFsync)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	cf_image_names(stage->generation, stage->image_sha256, stage->operation_uuid, final, temp);
	result = cf_image_open_dirs(&dirs, true);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!cf_image_dirs_match(&dirs, stage)) {
		result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		goto done;
	}
	if (stage->state == CF_IMAGE_STAGED) {
		if (!cf_image_exact_entry(dirs.staging, temp, stage)) {
			result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
			goto done;
		}
		if (unlinkat(dirs.staging, temp, 0) != 0) {
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
			goto done;
		}
	}
	/* No resurrection, including an installed operation whose sync failed. */
	stage->state = CF_IMAGE_DISCARDED;
	if (pg_fsync(dirs.staging) != 0)
		result = CLUSTER_CONTROL_ROOT_IO_ERROR;
done:
	cf_image_close_dirs(&dirs, &result);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		memset(stage, 0, sizeof(*stage));
	return result;
}

ClusterControlRootResult
cluster_cf_control_image_read_locked(uint64 generation, const uint8 sha256[32],
									 uint64 system_identifier, ControlFileData *out)
{
	ClusterControlRootResult result;
	CfImageDirs dirs;
	pg_cryptohash_ctx *ctx;
	uint8 bytes[PG_CONTROL_FILE_SIZE];
	char final[96], temp[40];

	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (generation == 0 || system_identifier == 0 || !cf_image_nonzero(sha256, 32))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (!cluster_cf_held_is_clusterwide(ShareLock)
		&& !cluster_cf_held_is_clusterwide(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL)
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	cf_image_names(generation, sha256, NULL, final, temp);
	result = cf_image_open_dirs(&dirs, false);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		result = cf_image_read_at(dirs.objects, final, NULL, sha256, system_identifier, ctx, bytes);
	cf_image_close_dirs(&dirs, &result);
	pg_cryptohash_free(ctx);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		memcpy(out, bytes, sizeof(*out));
	return result;
}
