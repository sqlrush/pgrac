/*-------------------------------------------------------------------------
 *
 * cluster_recovery_anchor.c
 *	  Per-node durable recovery anchor (spec-5.6a).
 *
 *	  Owns one small per-node sidecar file under the shared data root,
 *	  (shared_data_dir)/global/pgrac_recovery_anchor_n<node_id>, holding
 *	  this node's own last checkpoint record LSN, a full CheckPoint copy,
 *	  and this node's own DBState.  Under the shared pg_control authority
 *	  (cluster.controlfile_shared_authority=on) the shared control file's
 *	  checkpoint fields belong to whichever node wrote them last, so a
 *	  restarting node consumes its anchor instead: the anchor restores,
 *	  field for field, the restart inputs a per-node pg_control provided
 *	  before the authority was shared.
 *
 *	  Writes mirror the shared-authority torn-safe pattern (roll the live
 *	  primary into .bak, temp-write the new image, durable_rename it into
 *	  place) and PANIC on I/O failure: an anchor that silently stops
 *	  advancing loses this node's recoverability as soon as WAL recycling
 *	  passes its stale redo point.  Reads never ereport; they classify the
 *	  primary and then the .bak under the same strict validation and
 *	  return false so the caller decides the error face (FATAL 53RB3).
 *	  The anchor is one checkpoint stale at worst -- it is written before
 *	  the same checkpoint cycle recycles WAL -- so a .bak needs no extra
 *	  recoverability probe beyond full validity.
 *
 *	  Single writer: the owning node's startup process or checkpointer,
 *	  serialized by the checkpoint interlock.  No cross-node access; other
 *	  nodes never read this file.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_recovery_anchor.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.6a-per-node-recovery-anchor.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <time.h>
#include <unistd.h>

#include "catalog/pg_control.h"
#include "cluster/cluster_cf_stats.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_recovery_anchor.h"
#include "port/pg_crc32c.h"
#include "storage/fd.h"

/* Suffixes of the torn-safe file family next to the primary. */
#define ANCHOR_TMP_SUFFIX ".tmp"
#define ANCHOR_BAK_SUFFIX ".bak"
#define ANCHOR_BAK_TMP_SUFFIX ".bak.tmp"

/*
 * Boot-time adoption statics (startup process only; loaded once per boot
 * by cluster_recovery_anchor_load).
 */
static ClusterRecoveryAnchor boot_anchor;
static bool boot_anchor_active = false;

/*
 * build_anchor_path -- join cluster_shared_data_dir with this node's anchor
 * name plus `suffix` ("" for the primary) into the caller's buffer.
 * Returns false (empty buffer) when the shared root is unset, so callers
 * fail-closed rather than touch a bogus path.
 */
static bool
build_anchor_path(char *dst, size_t dstlen, const char *suffix)
{
	if (cluster_shared_data_dir == NULL || cluster_shared_data_dir[0] == '\0') {
		if (dstlen > 0)
			dst[0] = '\0';
		return false;
	}
	snprintf(dst, dstlen, "%s/" CLUSTER_RECOVERY_ANCHOR_REL_FMT "%s", cluster_shared_data_dir,
			 cluster_node_id, suffix);
	return true;
}

/*
 * cluster_recovery_anchor_path / cluster_recovery_anchor_bak_path --
 * accessors returning a per-function static buffer (see header).  NULL when
 * the shared root is unset.
 */
const char *
cluster_recovery_anchor_path(void)
{
	static char path[MAXPGPATH];

	if (!build_anchor_path(path, sizeof(path), ""))
		return NULL;
	return path;
}

const char *
cluster_recovery_anchor_bak_path(void)
{
	static char path[MAXPGPATH];

	if (!build_anchor_path(path, sizeof(path), ANCHOR_BAK_SUFFIX))
		return NULL;
	return path;
}

/*
 * cluster_recovery_anchor_classify -- pure classification of one raw image.
 *
 *	CRC is checked first: a torn image cannot have its other fields
 *	trusted, so magic/version and identity are only examined once the CRC
 *	validates.  Both identity legs (system_identifier and node_id) are
 *	always enforced -- every consumer knows both expected values, and a
 *	restart authority must never be adopted on partial identity.
 */
ClusterRecoveryAnchorValidity
cluster_recovery_anchor_classify(const char *buf, size_t len, uint64 expected_sysid,
								 int32 expected_node)
{
	ClusterRecoveryAnchor ra;
	pg_crc32c crc;

	if (buf == NULL || len < sizeof(ClusterRecoveryAnchor))
		return CLUSTER_RA_INVALID_SHORT;

	memcpy(&ra, buf, sizeof(ra));

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, buf, offsetof(ClusterRecoveryAnchor, crc));
	FIN_CRC32C(crc);
	if (!EQ_CRC32C(crc, ra.crc))
		return CLUSTER_RA_INVALID_CRC;

	if (ra.magic != CLUSTER_RECOVERY_ANCHOR_MAGIC || ra.version != CLUSTER_RECOVERY_ANCHOR_VERSION)
		return CLUSTER_RA_INVALID_MAGIC;

	if (ra.system_identifier != expected_sysid || ra.node_id != expected_node)
		return CLUSTER_RA_INVALID_IDENTITY;

	return CLUSTER_RA_VALID;
}

/*
 * read_image -- read one anchor image from `path` into `image`
 * (CLUSTER_RECOVERY_ANCHOR_SIZE bytes) and classify it.  A missing/short
 * file classifies as INVALID_SHORT so the caller treats it as unusable.
 */
static ClusterRecoveryAnchorValidity
read_image(const char *path, char *image, uint64 expected_sysid)
{
	int fd;
	int r;

	if (path == NULL)
		return CLUSTER_RA_INVALID_SHORT;

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return CLUSTER_RA_INVALID_SHORT;

	r = read(fd, image, CLUSTER_RECOVERY_ANCHOR_SIZE);
	CloseTransientFile(fd);

	if (r != CLUSTER_RECOVERY_ANCHOR_SIZE)
		return CLUSTER_RA_INVALID_SHORT;

	return cluster_recovery_anchor_classify(image, CLUSTER_RECOVERY_ANCHOR_SIZE, expected_sysid,
											cluster_node_id);
}

/*
 * cluster_recovery_anchor_read -- load this node's anchor into *out.
 *
 *	Tries the primary first; on failure falls back to a .bak that passes
 *	the same strict classification (VALID under CRC + magic + identity --
 *	a merely-stale .bak is safe, see the file header).  Returns false
 *	(leaving *out untouched) when the read must fail-closed.  Never
 *	ereports; the caller raises FATAL 53RB3 on the restart path.
 */
bool
cluster_recovery_anchor_read(uint64 expected_sysid, ClusterRecoveryAnchor *out, bool *used_bak)
{
	char primary_img[CLUSTER_RECOVERY_ANCHOR_SIZE];
	char bak_img[CLUSTER_RECOVERY_ANCHOR_SIZE];
	char bak_path[MAXPGPATH];

	if (used_bak != NULL)
		*used_bak = false;

	if (read_image(cluster_recovery_anchor_path(), primary_img, expected_sysid)
		== CLUSTER_RA_VALID) {
		memcpy(out, primary_img, sizeof(ClusterRecoveryAnchor));
		return true;
	}

	if (!build_anchor_path(bak_path, sizeof(bak_path), ANCHOR_BAK_SUFFIX))
		return false;

	if (read_image(bak_path, bak_img, expected_sysid) == CLUSTER_RA_VALID) {
		memcpy(out, bak_img, sizeof(ClusterRecoveryAnchor));
		if (used_bak != NULL)
			*used_bak = true;
		return true;
	}

	return false;
}

/*
 * write_durable -- write `buf` (CLUSTER_RECOVERY_ANCHOR_SIZE bytes) to
 * `tmp`, fsync it, then durable_rename() it over `final` (which fsyncs the
 * directory).  PANICs on any I/O failure, mirroring the shared-authority
 * write contract (see the file header for why WARNING-and-continue is not
 * an option here).
 */
static void
write_durable(const char *tmp, const char *final, const char *buf)
{
	int fd;

	fd = OpenTransientFile(tmp, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY);
	if (fd < 0)
		ereport(PANIC, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", tmp)));

	errno = 0;
	if (write(fd, buf, CLUSTER_RECOVERY_ANCHOR_SIZE) != CLUSTER_RECOVERY_ANCHOR_SIZE) {
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
 * roll_primary_to_bak -- if the primary currently exists (and is a full
 * image), copy its raw bytes into the .bak durably so a subsequent bad
 * primary write is recoverable.  A missing or short primary (first write)
 * is simply skipped.
 */
static void
roll_primary_to_bak(const char *primary, const char *bak, const char *baktmp)
{
	char buf[CLUSTER_RECOVERY_ANCHOR_SIZE];
	int fd;
	int r;

	fd = OpenTransientFile(primary, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return; /* first write: no prior primary to preserve */

	r = read(fd, buf, CLUSTER_RECOVERY_ANCHOR_SIZE);
	CloseTransientFile(fd);
	if (r != CLUSTER_RECOVERY_ANCHOR_SIZE)
		return; /* short/odd primary: don't manufacture a .bak */

	write_durable(baktmp, bak, buf);
}

/*
 * cluster_recovery_anchor_write -- atomically replace this node's anchor
 * with *ra.  Recomputes the CRC, rolls the live primary into .bak, then
 * installs the new image via temp-write + durable_rename.
 */
void
cluster_recovery_anchor_write(const ClusterRecoveryAnchor *ra)
{
	char primary[MAXPGPATH];
	char bak[MAXPGPATH];
	char tmp[MAXPGPATH];
	char baktmp[MAXPGPATH];
	ClusterRecoveryAnchor local;

	if (!build_anchor_path(primary, sizeof(primary), "")
		|| !build_anchor_path(bak, sizeof(bak), ANCHOR_BAK_SUFFIX)
		|| !build_anchor_path(tmp, sizeof(tmp), ANCHOR_TMP_SUFFIX)
		|| !build_anchor_path(baktmp, sizeof(baktmp), ANCHOR_BAK_TMP_SUFFIX))
		ereport(PANIC, (errmsg("cluster shared_data_dir is not configured")));

	/* Recompute CRC over a private copy (ra is const). */
	memcpy(&local, ra, sizeof(local));
	INIT_CRC32C(local.crc);
	COMP_CRC32C(local.crc, (char *)&local, offsetof(ClusterRecoveryAnchor, crc));
	FIN_CRC32C(local.crc);

	roll_primary_to_bak(primary, bak, baktmp);
	write_durable(tmp, primary, (const char *)&local);
	cluster_cf_counter_inc(CLUSTER_CF_RECOVERY_ANCHOR_WRITE);
}

/*
 * cluster_recovery_anchor_build_from_controlfile -- map a ControlFileData
 * snapshot onto an anchor image (seed path: before the control file is
 * migrated into the shared authority, the local pg_control still carries
 * this node's own checkpoint fields).  The CRC is left to the writer.
 */
void
cluster_recovery_anchor_build_from_controlfile(const ControlFileData *cf,
											   ClusterRecoveryAnchor *out)
{
	memset(out, 0, sizeof(*out));
	out->magic = CLUSTER_RECOVERY_ANCHOR_MAGIC;
	out->version = CLUSTER_RECOVERY_ANCHOR_VERSION;
	out->node_id = cluster_node_id;
	out->state = (uint32)cf->state;
	out->system_identifier = cf->system_identifier;
	out->checkPoint = cf->checkPoint;
	out->write_time = (pg_time_t)time(NULL);
	out->checkPointCopy = cf->checkPointCopy;
	out->unloggedLSN = cf->unloggedLSN;
}

/*
 * cluster_recovery_anchor_publish_checkpoint -- write hook #1 (spec-5.6a
 * D2): publish this node's just-logged checkpoint as the new anchor.  The
 * caller (CreateCheckPoint) invokes this after UpdateControlFile and
 * before this cycle's WAL recycling, so the invariant "the anchor's redo
 * point lies within retained WAL" holds across any crash window.
 */
void
cluster_recovery_anchor_publish_checkpoint(XLogRecPtr checkpoint_lsn,
										   const CheckPoint *checkpoint_copy, uint64 sysid,
										   uint32 state, XLogRecPtr unlogged_lsn)
{
	ClusterRecoveryAnchor ra;

	memset(&ra, 0, sizeof(ra));
	ra.magic = CLUSTER_RECOVERY_ANCHOR_MAGIC;
	ra.version = CLUSTER_RECOVERY_ANCHOR_VERSION;
	ra.node_id = cluster_node_id;
	ra.state = state;
	ra.system_identifier = sysid;
	ra.checkPoint = checkpoint_lsn;
	ra.write_time = (pg_time_t)time(NULL);
	ra.checkPointCopy = *checkpoint_copy;
	ra.unloggedLSN = unlogged_lsn;

	cluster_recovery_anchor_write(&ra);
}

/*
 * cluster_recovery_anchor_refresh_state -- write hook #2 (spec-5.6a D2):
 * flip an existing valid anchor's state, keeping its checkpoint fields.
 * Without this, a clean shutdown followed by a restart that writes WAL and
 * then crashes would be misread as a clean shutdown on the next boot (the
 * anchor would still say DB_SHUTDOWNED), skipping crash recovery -- the
 * same lost-write hazard vanilla prevents by setting DB_IN_PRODUCTION in
 * pg_control at startup.
 *
 * A missing or invalid anchor is a no-op returning false: creation
 * happens only at the checkpoint hook and the seed path, and an invalid
 * leftover is either rewritten by the imminent first checkpoint or caught
 * fail-closed (53RB3) at the next label-less boot.
 */
bool
cluster_recovery_anchor_refresh_state(uint64 expected_sysid, uint32 state)
{
	ClusterRecoveryAnchor ra;

	if (!cluster_recovery_anchor_read(expected_sysid, &ra, NULL))
		return false;

	ra.state = state;
	ra.write_time = (pg_time_t)time(NULL);
	cluster_recovery_anchor_write(&ra);
	return true;
}

/*
 * cluster_recovery_anchor_load / _active / _get -- boot-time adoption
 * (spec-5.6a D3).  The startup process loads the anchor once, right after
 * the shared-authority bootstrap window, and the restart consumption
 * points read the process-local copy.  No shmem, no EXEC_BACKEND face:
 * only the startup process consumes these.
 */
bool
cluster_recovery_anchor_load(uint64 expected_sysid, bool *used_bak)
{
	if (!cluster_recovery_anchor_read(expected_sysid, &boot_anchor, used_bak)) {
		boot_anchor_active = false;
		return false;
	}
	boot_anchor_active = true;
	cluster_cf_counter_inc(CLUSTER_CF_RECOVERY_ANCHOR_BOOT_ADOPT);
	return true;
}

bool
cluster_recovery_anchor_active(void)
{
	return boot_anchor_active;
}

const ClusterRecoveryAnchor *
cluster_recovery_anchor_get(void)
{
	return boot_anchor_active ? &boot_anchor : NULL;
}
