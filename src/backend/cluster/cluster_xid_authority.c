/*-------------------------------------------------------------------------
 *
 * cluster_xid_authority.c
 *	  Shared XID authority: never-lowered native-era xid high-water plus
 *	  one-way lifecycle flags, as a torn-safe file under
 *	  cluster.shared_data_dir (spec-6.15b D1).
 *
 *	  The authority records the first xid NOT consumed by the native era
 *	  (the pre-formation cluster.enabled=off single-writer window of the
 *	  seed node) and two one-way flags: SEALED (a clean native-era
 *	  shutdown published a complete high-water together with the pg_xact
 *	  prehistory blob, so joiners may adopt) and CLUSTER_ERA (a
 *	  cluster.enabled=on boot closed the native era forever).  Writers are
 *	  single by construction: the seed node's postmaster/checkpointer
 *	  during the native era, and the catalog bootstrap window afterwards.
 *
 *	  File discipline mirrors cluster_oid_lease.c: temp + fsync + .bak
 *	  roll + durable_rename on write; primary-then-.bak fail-closed read;
 *	  ENOENT-only-absent presence probe.  The pure classify layer is
 *	  standalone-linkable for cluster_unit.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_xid_authority.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.15b-xid-authority-native-era.md (D1, §4 U1-U3/U5)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cluster/cluster_guc.h" /* cluster_shared_data_dir */
#include "cluster/cluster_xid_authority.h"
#include "storage/fd.h"

/* On-disk image size: the fixed header, exactly. */
#define CLUSTER_XID_AUTHORITY_FILE_SIZE ((int)sizeof(ClusterXidAuthorityHeader))

/* ============================================================
 * Pure layer (no elog/shmem/fd; standalone-linkable for cluster_unit).
 * ============================================================ */

/*
 * cluster_xid_authority_classify -- validate an authority image buffer.
 */
ClusterXidAuthorityValidity
cluster_xid_authority_classify(const char *buf, size_t len)
{
	ClusterXidAuthorityHeader hdr;
	pg_crc32c crc;

	if (buf == NULL || len < sizeof(ClusterXidAuthorityHeader))
		return CLUSTER_XID_AUTHORITY_INVALID_SHORT;

	memcpy(&hdr, buf, sizeof(hdr));

	if (hdr.magic != CLUSTER_XID_AUTHORITY_MAGIC)
		return CLUSTER_XID_AUTHORITY_INVALID_MAGIC;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, buf, offsetof(ClusterXidAuthorityHeader, crc));
	FIN_CRC32C(crc);
	if (!EQ_CRC32C(crc, hdr.crc))
		return CLUSTER_XID_AUTHORITY_INVALID_CRC;

	return CLUSTER_XID_AUTHORITY_VALID;
}

/* ============================================================
 * Torn-safe authority file I/O (mirrors cluster_oid_lease.c).
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
 * PANICs on any I/O failure (mirrors cluster_oid_lease write_durable).
 */
static void
write_durable(const char *tmp, const char *final, const char *buf)
{
	int fd;

	fd = OpenTransientFile(tmp, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY);
	if (fd < 0)
		ereport(PANIC, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", tmp)));

	errno = 0;
	if (write(fd, buf, CLUSTER_XID_AUTHORITY_FILE_SIZE) != CLUSTER_XID_AUTHORITY_FILE_SIZE) {
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
 * roll_primary_to_bak -- preserve an existing primary into .bak before a new
 * primary write.  A missing/short primary (first write) is skipped.
 */
static void
roll_primary_to_bak(const char *primary, const char *bak, const char *baktmp)
{
	char buf[CLUSTER_XID_AUTHORITY_FILE_SIZE];
	int fd;
	int r;

	fd = OpenTransientFile(primary, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return; /* first write: no prior primary */

	r = read(fd, buf, CLUSTER_XID_AUTHORITY_FILE_SIZE);
	CloseTransientFile(fd);
	if (r != CLUSTER_XID_AUTHORITY_FILE_SIZE)
		return; /* short/odd primary: don't manufacture a .bak */

	write_durable(baktmp, bak, buf);
}

/*
 * read_image -- read a candidate authority file and classify it.  Missing or
 * short is INVALID_SHORT.
 */
static ClusterXidAuthorityValidity
read_image(const char *path, char *image)
{
	int fd;
	int r;

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return CLUSTER_XID_AUTHORITY_INVALID_SHORT;

	r = read(fd, image, CLUSTER_XID_AUTHORITY_FILE_SIZE);
	CloseTransientFile(fd);
	if (r != CLUSTER_XID_AUTHORITY_FILE_SIZE)
		return CLUSTER_XID_AUTHORITY_INVALID_SHORT;

	return cluster_xid_authority_classify(image, CLUSTER_XID_AUTHORITY_FILE_SIZE);
}

/*
 * cluster_xid_authority_read -- fail-closed read of the shared authority.
 * Tries primary then .bak; returns false when neither is trustworthy.  Never
 * ereports (safe on the bootstrap early-read path).
 */
bool
cluster_xid_authority_read(ClusterXidAuthorityHeader *out)
{
	return cluster_xid_authority_read_checked(out, NULL);
}

/*
 * cluster_xid_authority_read_checked -- as above, additionally reporting
 * whether the image came from the .bak fallback so consumers can surface a
 * LOG-once operator signal (review F8; the read itself stays silent for
 * the bootstrap early-read path).
 */
bool
cluster_xid_authority_read_checked(ClusterXidAuthorityHeader *out, bool *used_bak)
{
	char primary_path[MAXPGPATH];
	char bak_path[MAXPGPATH];
	char image[CLUSTER_XID_AUTHORITY_FILE_SIZE];

	if (used_bak != NULL)
		*used_bak = false;

	if (!build_path(primary_path, sizeof(primary_path), CLUSTER_XID_AUTHORITY_REL_PATH)
		|| !build_path(bak_path, sizeof(bak_path), CLUSTER_XID_AUTHORITY_BAK_REL_PATH))
		return false;

	if (read_image(primary_path, image) != CLUSTER_XID_AUTHORITY_VALID) {
		if (read_image(bak_path, image) != CLUSTER_XID_AUTHORITY_VALID)
			return false; /* fail-closed: neither trustworthy */
		if (used_bak != NULL)
			*used_bak = true;
	}

	memcpy(out, image, sizeof(*out));
	return true;
}

/*
 * cluster_xid_authority_present -- does any authority image (primary or .bak)
 * exist on disk at all, trustworthy or not?  Lets callers distinguish
 * "absent: genuine first seed" from "present but corrupt: fail-closed" -- a
 * corrupt authority must never be silently re-seeded (a re-seed from a
 * checkpointed value could put already-consumed xids back "in the future").
 * Never ereports.
 *
 * Fail-closed errno discipline: only ENOENT proves absence (mirrors
 * cluster_oid_authority_present; EIO/EACCES/ESTALE report present).
 */
bool
cluster_xid_authority_present(void)
{
	char primary_path[MAXPGPATH];
	char bak_path[MAXPGPATH];
	struct stat st;

	if (!build_path(primary_path, sizeof(primary_path), CLUSTER_XID_AUTHORITY_REL_PATH)
		|| !build_path(bak_path, sizeof(bak_path), CLUSTER_XID_AUTHORITY_BAK_REL_PATH))
		return false;

	if (stat(primary_path, &st) == 0 || errno != ENOENT)
		return true;
	if (stat(bak_path, &st) == 0 || errno != ENOENT)
		return true;
	return false;
}

/*
 * write_header -- CRC-stamp and atomically install an authority header.
 */
static void
write_header(ClusterXidAuthorityHeader *hdr)
{
	char buffer[CLUSTER_XID_AUTHORITY_FILE_SIZE];
	char primary[MAXPGPATH];
	char bak[MAXPGPATH];
	char tmp[MAXPGPATH];
	char baktmp[MAXPGPATH];

	if (!build_path(primary, sizeof(primary), CLUSTER_XID_AUTHORITY_REL_PATH)
		|| !build_path(bak, sizeof(bak), CLUSTER_XID_AUTHORITY_BAK_REL_PATH)
		|| !build_path(tmp, sizeof(tmp), CLUSTER_XID_AUTHORITY_TMP_REL_PATH)
		|| !build_path(baktmp, sizeof(baktmp), CLUSTER_XID_AUTHORITY_BAK_TMP_REL_PATH))
		ereport(PANIC, (errmsg("cluster shared_data_dir is not configured")));

	hdr->magic = CLUSTER_XID_AUTHORITY_MAGIC;
	hdr->version = CLUSTER_XID_AUTHORITY_VERSION;
	hdr->reserved = 0;
	INIT_CRC32C(hdr->crc);
	COMP_CRC32C(hdr->crc, (char *)hdr, offsetof(ClusterXidAuthorityHeader, crc));
	FIN_CRC32C(hdr->crc);

	memset(buffer, 0, sizeof(buffer));
	memcpy(buffer, hdr, sizeof(*hdr));

	roll_primary_to_bak(primary, bak, baktmp);
	write_durable(tmp, primary, buffer);
}

/*
 * write_header_both -- CRC-stamp the header and install the SAME image as
 * both primary and .bak (primary first).  For FLAG transitions (unseal,
 * CLUSTER_ERA stamp) the usual roll-primary-to-.bak is unsafe: it would
 * preserve a pre-transition image that the fail-closed read falls back to
 * when the primary is later damaged -- a stale SEALED .bak hands a joiner
 * the previous pass's high-water (review F1, 8.A), and a stale
 * pre-CLUSTER_ERA .bak re-opens the native-era re-entry guard.  Crash
 * points: both copies old (transition not yet taken: state still
 * consistent), primary new + .bak old (read prefers the valid primary),
 * both new.  A later single-copy corruption therefore never resurrects the
 * pre-transition flags.
 */
static void
write_header_both(ClusterXidAuthorityHeader *hdr)
{
	char buffer[CLUSTER_XID_AUTHORITY_FILE_SIZE];
	char primary[MAXPGPATH];
	char bak[MAXPGPATH];
	char tmp[MAXPGPATH];
	char baktmp[MAXPGPATH];

	if (!build_path(primary, sizeof(primary), CLUSTER_XID_AUTHORITY_REL_PATH)
		|| !build_path(bak, sizeof(bak), CLUSTER_XID_AUTHORITY_BAK_REL_PATH)
		|| !build_path(tmp, sizeof(tmp), CLUSTER_XID_AUTHORITY_TMP_REL_PATH)
		|| !build_path(baktmp, sizeof(baktmp), CLUSTER_XID_AUTHORITY_BAK_TMP_REL_PATH))
		ereport(PANIC, (errmsg("cluster shared_data_dir is not configured")));

	hdr->magic = CLUSTER_XID_AUTHORITY_MAGIC;
	hdr->version = CLUSTER_XID_AUTHORITY_VERSION;
	hdr->reserved = 0;
	INIT_CRC32C(hdr->crc);
	COMP_CRC32C(hdr->crc, (char *)hdr, offsetof(ClusterXidAuthorityHeader, crc));
	FIN_CRC32C(hdr->crc);

	memset(buffer, 0, sizeof(buffer));
	memcpy(buffer, hdr, sizeof(*hdr));

	write_durable(tmp, primary, buffer);
	write_durable(baktmp, bak, buffer);
}

/*
 * cluster_xid_authority_seed_if_absent -- read-then-seed: if the authority is
 * already present (any trustworthy image) do nothing; otherwise create the
 * unsealed initial image.  Returns true when this call created it.
 */
bool
cluster_xid_authority_seed_if_absent(uint64 initial_native_hw)
{
	ClusterXidAuthorityHeader hdr;

	if (cluster_xid_authority_read(&hdr))
		return false; /* already seeded (join node / prior seed) */
	if (cluster_xid_authority_present())
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
						errmsg("shared XID authority is present but corrupt at seed"),
						errhint("Restore \"%s\" (or its .bak) from a backup of the shared tree; "
								"do not re-seed from a stale xid high-water.",
								CLUSTER_XID_AUTHORITY_REL_PATH)));

	memset(&hdr, 0, sizeof(hdr));
	hdr.flags = 0;
	hdr.native_hw_full = initial_native_hw;
	hdr.next_multi = 0;
	write_header(&hdr);
	return true;
}

/*
 * cluster_xid_authority_publish_native -- monotone raise of the native-era
 * high-water; seal=true additionally stamps SEALED.  Flags are never
 * cleared and the high-water is never lowered (spec §3.1).  An absent
 * authority is created outright (crash between bootstrap-seed and the
 * first publish); a present-but-corrupt one PANICs rather than re-seed.
 */
void
cluster_xid_authority_publish_native(uint64 native_hw_full, uint64 next_multi, bool seal)
{
	ClusterXidAuthorityHeader hdr;

	if (!cluster_xid_authority_read(&hdr)) {
		if (cluster_xid_authority_present())
			ereport(PANIC,
					(errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
					 errmsg("shared XID authority is present but corrupt at publish"),
					 errhint("Restore \"%s\" (or its .bak) from a backup of the shared tree; "
							 "re-seeding from a stale high-water could re-issue consumed xids.",
							 CLUSTER_XID_AUTHORITY_REL_PATH)));
		memset(&hdr, 0, sizeof(hdr));
	}

	if (native_hw_full > hdr.native_hw_full)
		hdr.native_hw_full = native_hw_full;
	if (next_multi > hdr.next_multi)
		hdr.next_multi = next_multi;
	if (seal)
		hdr.flags |= CLUSTER_XID_AUTHORITY_FLAG_SEALED;

	write_header(&hdr);
}

/*
 * cluster_xid_authority_mark_cluster_era -- one-way CLUSTER_ERA stamp (first
 * cluster.enabled=on boot; spec §3.1).  No-op when already stamped.  A
 * missing/corrupt authority PANICs: the catalog bootstrap seeds it before
 * any caller can reach this point, so absence here is real damage.
 */
void
cluster_xid_authority_mark_cluster_era(void)
{
	ClusterXidAuthorityHeader hdr;

	if (!cluster_xid_authority_read(&hdr))
		ereport(PANIC, (errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
						errmsg("shared XID authority is missing or corrupt at cluster-era stamp")));

	if (hdr.flags & CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA)
		return;

	hdr.flags |= CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA;
	write_header_both(&hdr); /* review F1 variant: one-way flag in BOTH copies */
}

/*
 * cluster_xid_authority_begin_native_run -- re-open the native era for a
 * follow-up cluster.enabled=off seed run (spec-6.15b §3.1 multi-pass arm).
 *
 *	A sealed authority means "the high-water and prehistory are complete as
 *	of a clean native shutdown".  A NEW native run invalidates that
 *	completeness the moment it allocates its first xid, so the seal is
 *	cleared up front: if this run crashes, joiners fail closed (unsealed,
 *	53RB5) instead of adopting the previous pass's stale high-water --
 *	false-invisible for every xid the crashed pass consumed (rule 8.A).
 *	The clean shutdown of this run re-publishes and re-seals monotonically.
 *
 *	Only legal while CLUSTER_ERA is unset (the caller FATALs re-entry
 *	first; re-checked here defensively).  Formation recipes keep native-era
 *	boots and first cluster boots strictly ordered; if a racing first
 *	cluster boot stamps CLUSTER_ERA inside this read-modify-write window,
 *	the stamp is re-applied by the next cluster boot (mark is re-issued
 *	whenever unset) and this authority is left unsealed -- which only ever
 *	blocks adoption, never yields a wrong answer.
 */
void
cluster_xid_authority_begin_native_run(void)
{
	ClusterXidAuthorityHeader hdr;

	if (!cluster_xid_authority_read(&hdr))
		ereport(PANIC, (errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
						errmsg("shared XID authority is missing or corrupt at native-run open")));

	if (hdr.flags & CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA)
		ereport(FATAL,
				(errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
				 errmsg("native seed era cannot be re-entered on a formed shared catalog tree")));

	if ((hdr.flags & CLUSTER_XID_AUTHORITY_FLAG_SEALED) == 0)
		return; /* already open (first run, or a prior pass crashed) */

	hdr.flags &= ~CLUSTER_XID_AUTHORITY_FLAG_SEALED;
	write_header_both(&hdr); /* review F1: no copy may retain SEALED */
}
