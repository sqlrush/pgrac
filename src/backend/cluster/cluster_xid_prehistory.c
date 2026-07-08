/*-------------------------------------------------------------------------
 *
 * cluster_xid_prehistory.c
 *	  Native-era pg_xact prehistory: publish the seed node's CLOG page run
 *	  [0, native_hw) into a CRC-guarded blob under cluster.shared_data_dir,
 *	  and adopt it into a joiner's local pg_xact (spec-6.15b D2).
 *
 *	  The native era (pre-formation, cluster.enabled=off) is a single-
 *	  writer window: the seed node's local pg_xact is the complete commit-
 *	  status truth for every xid below the native high-water, including
 *	  aborted ones.  Publishing that page run with the shared tree lets a
 *	  pre-seed-lineage joiner adopt first-hand truth instead of trusting
 *	  hint bits (false-invisible / poison-stamp hazard; spec §0).  This is
 *	  a one-shot formation-time carry of pre-cluster history -- the same
 *	  bytes a post-seed clone would have carried -- NOT a runtime mirror of
 *	  cluster-era foreign commit bits (the rejected CLOG-overlay design;
 *	  spec §1.4.1 boundary note).
 *
 *	  Both publish and adopt stream page-at-a-time (no large allocations):
 *	  a CRC pass validates or stamps the whole image, then a second pass
 *	  moves the bytes.  Their callers run in single-writer windows (the
 *	  seed's shutdown checkpoint; the joiner's postmaster-once bootstrap,
 *	  strictly before StartupCLOG), so the two passes see stable bytes.
 *	  Adopt fsyncs every touched segment and the pg_xact directory before
 *	  returning (spec P2).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_xid_prehistory.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.15b-xid-authority-native-era.md (D2, §4 U1/U4)
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

/*
 * CLOG geometry, restated locally so the module stays standalone-linkable
 * for cluster_unit: 2 status bits per xact -> 4 xacts per byte -> BLCKSZ*4
 * xacts per page (clog.c CLOG_XACTS_PER_PAGE, private to clog.c), and 32
 * pages per SLRU segment file (slru.h SLRU_PAGES_PER_SEGMENT).  The unit
 * truth table locks this arithmetic against drift.
 */
#define PREHISTORY_XACTS_PER_PAGE ((uint32)(BLCKSZ * 4))
#define PREHISTORY_PAGES_PER_SEGMENT 32

static bool prehistory_adopted_this_boot = false;

/* ============================================================
 * Pure layer.
 * ============================================================ */

/*
 * cluster_xid_prehistory_payload_bytes -- whole-page payload size covering
 * xids [0, native_hw_full); 0 for an empty or over-cap native era (callers
 * treat 0 as "nothing to carry" / refuse respectively).
 */
uint32
cluster_xid_prehistory_payload_bytes(uint64 native_hw_full)
{
	uint64 pages;

	if (native_hw_full == 0 || native_hw_full > CLUSTER_XID_PREHISTORY_MAX_XID)
		return 0;

	pages = (native_hw_full + PREHISTORY_XACTS_PER_PAGE - 1) / PREHISTORY_XACTS_PER_PAGE;
	return (uint32)(pages * BLCKSZ);
}

/*
 * cluster_xid_prehistory_classify -- validate a full in-memory blob image
 * (header + payload).  Used by the unit truth table; the streaming adopt
 * path performs the same checks incrementally.
 */
ClusterXidAuthorityValidity
cluster_xid_prehistory_classify(const char *buf, size_t len)
{
	ClusterXidPrehistoryHeader hdr;
	pg_crc32c crc;

	if (buf == NULL || len < sizeof(ClusterXidPrehistoryHeader))
		return CLUSTER_XID_AUTHORITY_INVALID_SHORT;

	memcpy(&hdr, buf, sizeof(hdr));

	if (hdr.magic != CLUSTER_XID_PREHISTORY_MAGIC)
		return CLUSTER_XID_AUTHORITY_INVALID_MAGIC;
	if (len != sizeof(hdr) + hdr.payload_len
		|| hdr.payload_len != cluster_xid_prehistory_payload_bytes(hdr.native_hw_full))
		return CLUSTER_XID_AUTHORITY_INVALID_SHORT;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, buf, offsetof(ClusterXidPrehistoryHeader, crc));
	COMP_CRC32C(crc, buf + sizeof(hdr), hdr.payload_len);
	FIN_CRC32C(crc);
	if (!EQ_CRC32C(crc, hdr.crc))
		return CLUSTER_XID_AUTHORITY_INVALID_CRC;

	return CLUSTER_XID_AUTHORITY_VALID;
}

/* ============================================================
 * Streaming file plumbing.
 * ============================================================ */

static bool
build_path(char *dst, size_t dstlen, const char *relpath)
{
	if (cluster_shared_data_dir == NULL || cluster_shared_data_dir[0] == '\0')
		return false;
	snprintf(dst, dstlen, "%s/%s", cluster_shared_data_dir, relpath);
	return true;
}

/*
 * read_clog_page -- read local pg_xact page `pageno` into buf.  Fail-closed:
 * a missing or short segment is FATAL -- the caller's window (post
 * CheckPointGuts / clean shutdown) guarantees every allocated page is on
 * disk, so a hole would mean fabricating "aborted" truth for real xids.
 */
static void
read_clog_page(const char *local_pgdata, uint32 pageno, char *buf)
{
	char seg[MAXPGPATH];
	off_t offset;
	int fd;

	snprintf(seg, sizeof(seg), "%s/pg_xact/%04X", local_pgdata,
			 pageno / PREHISTORY_PAGES_PER_SEGMENT);
	offset = (off_t)(pageno % PREHISTORY_PAGES_PER_SEGMENT) * BLCKSZ;

	fd = OpenTransientFile(seg, O_RDONLY | PG_BINARY);
	if (fd < 0)
		ereport(FATAL,
				(errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
				 errmsg("could not open pg_xact segment \"%s\" for prehistory publish: %m", seg),
				 errhint("The native-era CLOG must be complete on disk; publish runs only "
						 "after a CLOG flush.")));
	if (lseek(fd, offset, SEEK_SET) != offset || read(fd, buf, BLCKSZ) != BLCKSZ)
		ereport(FATAL,
				(errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
				 errmsg("short read of pg_xact segment \"%s\" page %u for prehistory publish", seg,
						pageno),
				 errhint("The native-era CLOG must be complete on disk; a hole would fabricate "
						 "commit-status truth.")));
	CloseTransientFile(fd);
}

/*
 * roll_blob_to_bak -- preserve an existing primary blob into .bak (streamed
 * in page-sized chunks; variable length).  Missing primary is skipped.
 */
static void
roll_blob_to_bak(const char *primary, const char *bak, const char *baktmp)
{
	char chunk[BLCKSZ];
	int src;
	int dst;
	int r;

	src = OpenTransientFile(primary, O_RDONLY | PG_BINARY);
	if (src < 0)
		return; /* first write: no prior primary */

	dst = OpenTransientFile(baktmp, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY);
	if (dst < 0) {
		CloseTransientFile(src);
		ereport(PANIC,
				(errcode_for_file_access(), errmsg("could not open file \"%s\": %m", baktmp)));
	}
	while ((r = read(src, chunk, sizeof(chunk))) > 0) {
		if (write(dst, chunk, r) != r)
			ereport(PANIC,
					(errcode_for_file_access(), errmsg("could not write file \"%s\": %m", baktmp)));
	}
	if (r < 0)
		ereport(PANIC,
				(errcode_for_file_access(), errmsg("could not read file \"%s\": %m", primary)));
	CloseTransientFile(src);
	if (pg_fsync(dst) != 0)
		ereport(PANIC,
				(errcode_for_file_access(), errmsg("could not fsync file \"%s\": %m", baktmp)));
	CloseTransientFile(dst);
	if (durable_rename(baktmp, bak, PANIC) != 0)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not rename file \"%s\" to \"%s\": %m", baktmp, bak)));
}

/* ============================================================
 * Publish (seed side).
 * ============================================================ */

/*
 * cluster_xid_prehistory_publish -- snapshot local pg_xact [0, native_hw)
 * into the shared blob, torn-safe.  Two passes over the source pages: one
 * to compute the image CRC, one to write; the caller's single-writer
 * window (seed shutdown checkpoint / catalog-seed StartupXLOG) keeps the
 * bytes stable between the passes.
 */
void
cluster_xid_prehistory_publish(const char *local_pgdata, uint64 native_hw_full)
{
	ClusterXidPrehistoryHeader hdr;
	char primary[MAXPGPATH];
	char bak[MAXPGPATH];
	char tmp[MAXPGPATH];
	char baktmp[MAXPGPATH];
	char page[BLCKSZ];
	uint32 payload_len;
	uint32 pages;
	uint32 p;
	int fd;

	if (native_hw_full == 0 || native_hw_full > CLUSTER_XID_PREHISTORY_MAX_XID)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
						errmsg("native-era xid high-water " UINT64_FORMAT
							   " is outside the prehistory publish range",
							   native_hw_full),
						errhint("Native-era seed loads are capped at " UINT64_FORMAT
								" xids; split the seed load or move it in-protocol.",
								CLUSTER_XID_PREHISTORY_MAX_XID)));

	if (!build_path(primary, sizeof(primary), CLUSTER_XID_PREHISTORY_REL_PATH)
		|| !build_path(bak, sizeof(bak), CLUSTER_XID_PREHISTORY_BAK_REL_PATH)
		|| !build_path(tmp, sizeof(tmp), CLUSTER_XID_PREHISTORY_TMP_REL_PATH)
		|| !build_path(baktmp, sizeof(baktmp), CLUSTER_XID_PREHISTORY_BAK_TMP_REL_PATH))
		ereport(PANIC, (errmsg("cluster shared_data_dir is not configured")));

	payload_len = cluster_xid_prehistory_payload_bytes(native_hw_full);
	pages = payload_len / BLCKSZ;

	memset(&hdr, 0, sizeof(hdr));
	hdr.magic = CLUSTER_XID_PREHISTORY_MAGIC;
	hdr.version = CLUSTER_XID_PREHISTORY_VERSION;
	hdr.native_hw_full = native_hw_full;
	hdr.payload_len = payload_len;
	hdr.reserved = 0;

	/* pass 1: CRC over header fields + source pages */
	INIT_CRC32C(hdr.crc);
	COMP_CRC32C(hdr.crc, (char *)&hdr, offsetof(ClusterXidPrehistoryHeader, crc));
	for (p = 0; p < pages; p++) {
		read_clog_page(local_pgdata, p, page);
		COMP_CRC32C(hdr.crc, page, BLCKSZ);
	}
	FIN_CRC32C(hdr.crc);

	/* pass 2: stream header + pages into tmp, then install torn-safe */
	fd = OpenTransientFile(tmp, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY);
	if (fd < 0)
		ereport(PANIC, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", tmp)));
	errno = 0;
	if (write(fd, &hdr, sizeof(hdr)) != sizeof(hdr)) {
		if (errno == 0)
			errno = ENOSPC;
		ereport(PANIC, (errcode_for_file_access(), errmsg("could not write file \"%s\": %m", tmp)));
	}
	for (p = 0; p < pages; p++) {
		read_clog_page(local_pgdata, p, page);
		errno = 0;
		if (write(fd, page, BLCKSZ) != BLCKSZ) {
			if (errno == 0)
				errno = ENOSPC;
			ereport(PANIC,
					(errcode_for_file_access(), errmsg("could not write file \"%s\": %m", tmp)));
		}
	}
	if (pg_fsync(fd) != 0)
		ereport(PANIC, (errcode_for_file_access(), errmsg("could not fsync file \"%s\": %m", tmp)));
	if (CloseTransientFile(fd) != 0)
		ereport(PANIC, (errcode_for_file_access(), errmsg("could not close file \"%s\": %m", tmp)));

	roll_blob_to_bak(primary, bak, baktmp);
	if (durable_rename(tmp, primary, PANIC) != 0)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not rename file \"%s\" to \"%s\": %m", tmp, primary)));
}

/* ============================================================
 * Adopt (joiner side).
 * ============================================================ */

/*
 * verify_blob -- streaming validation of one candidate blob file against
 * the sealed authority's native_hw.  Returns true (and leaves *out_pages
 * set) only when magic/version/hw/length/CRC all hold.
 */
static bool
verify_blob(const char *path, uint64 native_hw_full, uint32 *out_pages)
{
	ClusterXidPrehistoryHeader hdr;
	char page[BLCKSZ];
	pg_crc32c crc;
	uint32 pages;
	uint32 p;
	int fd;

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return false;
	if (read(fd, &hdr, sizeof(hdr)) != sizeof(hdr)) {
		CloseTransientFile(fd);
		return false;
	}
	if (hdr.magic != CLUSTER_XID_PREHISTORY_MAGIC || hdr.version != CLUSTER_XID_PREHISTORY_VERSION
		|| hdr.native_hw_full != native_hw_full
		|| hdr.payload_len != cluster_xid_prehistory_payload_bytes(native_hw_full)) {
		CloseTransientFile(fd);
		return false;
	}

	pages = hdr.payload_len / BLCKSZ;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, (char *)&hdr, offsetof(ClusterXidPrehistoryHeader, crc));
	for (p = 0; p < pages; p++) {
		if (read(fd, page, BLCKSZ) != BLCKSZ) {
			CloseTransientFile(fd);
			return false;
		}
		COMP_CRC32C(crc, page, BLCKSZ);
	}
	FIN_CRC32C(crc);
	CloseTransientFile(fd);

	if (!EQ_CRC32C(crc, hdr.crc))
		return false;

	*out_pages = pages;
	return true;
}

/*
 * cluster_xid_prehistory_adopt -- decode the shared blob into local pg_xact
 * page files.  Validates the whole image (primary, then .bak) BEFORE the
 * first local write -- a joiner must never take half a truth.  Writes are
 * whole-page, sequential from page 0, fsynced per touched segment, then the
 * pg_xact directory is fsynced (spec P2: complete and durable strictly
 * before StartupCLOG).  Idempotent: re-running overwrites the same bytes.
 */
void
cluster_xid_prehistory_adopt(const char *local_pgdata, uint64 native_hw_full)
{
	ClusterXidPrehistoryHeader hdr;
	char primary[MAXPGPATH];
	char bak[MAXPGPATH];
	char seg[MAXPGPATH];
	char dir[MAXPGPATH];
	char page[BLCKSZ];
	const char *src_path = NULL;
	uint32 pages = 0;
	uint32 p;
	int src;
	int dst = -1;
	int cur_segno = -1;
	int fd;

	if (!build_path(primary, sizeof(primary), CLUSTER_XID_PREHISTORY_REL_PATH)
		|| !build_path(bak, sizeof(bak), CLUSTER_XID_PREHISTORY_BAK_REL_PATH))
		ereport(PANIC, (errmsg("cluster shared_data_dir is not configured")));

	if (verify_blob(primary, native_hw_full, &pages))
		src_path = primary;
	else if (verify_blob(bak, native_hw_full, &pages))
		src_path = bak;
	else
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
						errmsg("shared XID prehistory is missing, corrupt, or does not match the "
							   "sealed authority high-water " UINT64_FORMAT,
							   native_hw_full),
						errhint("The seed node must complete a clean native-era shutdown before "
								"joiners can adopt; restore \"%s\" from the shared tree's backup "
								"if it was damaged.",
								CLUSTER_XID_PREHISTORY_REL_PATH)));

	/* pass 2: stream the validated pages into local pg_xact segments */
	src = OpenTransientFile(src_path, O_RDONLY | PG_BINARY);
	if (src < 0 || read(src, &hdr, sizeof(hdr)) != sizeof(hdr))
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
						errmsg("could not re-open shared XID prehistory \"%s\": %m", src_path)));

	for (p = 0; p < pages; p++) {
		int segno = (int)(p / PREHISTORY_PAGES_PER_SEGMENT);
		off_t offset = (off_t)(p % PREHISTORY_PAGES_PER_SEGMENT) * BLCKSZ;

		if (read(src, page, BLCKSZ) != BLCKSZ)
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
					 errmsg("short read of shared XID prehistory \"%s\" page %u", src_path, p)));

		if (segno != cur_segno) {
			if (dst >= 0) {
				if (pg_fsync(dst) != 0)
					ereport(PANIC, (errcode_for_file_access(),
									errmsg("could not fsync pg_xact segment: %m")));
				CloseTransientFile(dst);
			}
			snprintf(seg, sizeof(seg), "%s/pg_xact/%04X", local_pgdata, segno);
			dst = OpenTransientFile(seg, O_RDWR | O_CREAT | PG_BINARY);
			if (dst < 0)
				ereport(PANIC, (errcode_for_file_access(),
								errmsg("could not open pg_xact segment \"%s\": %m", seg)));
			cur_segno = segno;
		}
		errno = 0;
		if (lseek(dst, offset, SEEK_SET) != offset || write(dst, page, BLCKSZ) != BLCKSZ) {
			if (errno == 0)
				errno = ENOSPC;
			ereport(PANIC, (errcode_for_file_access(),
							errmsg("could not write pg_xact segment \"%s\": %m", seg)));
		}
	}
	if (dst >= 0) {
		if (pg_fsync(dst) != 0)
			ereport(PANIC,
					(errcode_for_file_access(), errmsg("could not fsync pg_xact segment: %m")));
		CloseTransientFile(dst);
	}
	CloseTransientFile(src);

	/* P2: directory fsync makes the adopted truth durable before StartupCLOG */
	snprintf(dir, sizeof(dir), "%s/pg_xact", local_pgdata);
	fd = OpenTransientFile(dir, O_RDONLY | PG_BINARY);
	if (fd < 0)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not open pg_xact directory \"%s\": %m", dir)));
	if (pg_fsync(fd) != 0)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not fsync pg_xact directory \"%s\": %m", dir)));
	CloseTransientFile(fd);
	prehistory_adopted_this_boot = true;
}

bool
cluster_xid_prehistory_was_adopted(void)
{
	return prehistory_adopted_this_boot;
}

/* ============================================================
 * Divergent-lineage guard (review F2; spec Q6 amendment).
 * ============================================================ */

/*
 * read_local_clog_page_optional -- read local pg_xact page `pageno` into
 * buf.  Returns false when the segment file or the page does not exist
 * (short clone: the comparable prefix ends there); PANICs on a real read
 * error so an I/O fault is never mistaken for a short prefix.
 */
static bool
read_local_clog_page_optional(const char *local_pgdata, uint32 pageno, char *buf)
{
	char seg[MAXPGPATH];
	off_t offset;
	int fd;
	int r;

	snprintf(seg, sizeof(seg), "%s/pg_xact/%04X", local_pgdata,
			 pageno / PREHISTORY_PAGES_PER_SEGMENT);
	offset = (off_t)(pageno % PREHISTORY_PAGES_PER_SEGMENT) * BLCKSZ;

	fd = OpenTransientFile(seg, O_RDONLY | PG_BINARY);
	if (fd < 0) {
		if (errno == ENOENT)
			return false;
		ereport(PANIC, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", seg)));
	}
	if (lseek(fd, offset, SEEK_SET) != offset) {
		CloseTransientFile(fd);
		return false;
	}
	errno = 0;
	r = read(fd, buf, BLCKSZ);
	CloseTransientFile(fd);
	if (r < 0)
		ereport(PANIC, (errcode_for_file_access(), errmsg("could not read file \"%s\": %m", seg)));
	return r == BLCKSZ;
}

/*
 * cluster_xid_prehistory_prefix_check -- see header.  Streams the sealed
 * blob (primary, then .bak) and byte-compares the local pg_xact prefix
 * covering xids [0, limit_xid_full) at per-xact (2-bit) precision.
 */
ClusterXidPrefixVerdict
cluster_xid_prehistory_prefix_check(const char *local_pgdata, uint64 native_hw_full,
									uint64 limit_xid_full)
{
	ClusterXidPrehistoryHeader hdr;
	char primary[MAXPGPATH];
	char bak[MAXPGPATH];
	char blob_page[BLCKSZ];
	char local_page[BLCKSZ];
	const char *src_path;
	uint32 pages = 0;
	uint32 p;
	uint64 limit;
	int src;

	if (!build_path(primary, sizeof(primary), CLUSTER_XID_PREHISTORY_REL_PATH)
		|| !build_path(bak, sizeof(bak), CLUSTER_XID_PREHISTORY_BAK_REL_PATH))
		return CLUSTER_XID_PREFIX_UNAVAILABLE;

	if (verify_blob(primary, native_hw_full, &pages))
		src_path = primary;
	else if (verify_blob(bak, native_hw_full, &pages))
		src_path = bak;
	else
		return CLUSTER_XID_PREFIX_UNAVAILABLE;

	limit = Min(limit_xid_full, native_hw_full);
	if (limit == 0)
		return CLUSTER_XID_PREFIX_CONSISTENT;

	src = OpenTransientFile(src_path, O_RDONLY | PG_BINARY);
	if (src < 0 || read(src, &hdr, sizeof(hdr)) != sizeof(hdr)) {
		if (src >= 0)
			CloseTransientFile(src);
		return CLUSTER_XID_PREFIX_UNAVAILABLE;
	}

	for (p = 0; p < pages; p++) {
		uint64 page_first_xid = (uint64)p * PREHISTORY_XACTS_PER_PAGE;
		uint64 in_scope;
		uint32 full_bytes;
		uint32 partial_bits;

		if (read(src, blob_page, BLCKSZ) != BLCKSZ) {
			CloseTransientFile(src);
			return CLUSTER_XID_PREFIX_UNAVAILABLE;
		}
		if (page_first_xid >= limit)
			break; /* comparison scope ended on a page boundary */

		if (!read_local_clog_page_optional(local_pgdata, p, local_page))
			break; /* short clone: no local bits left to contradict */

		in_scope = Min((uint64)PREHISTORY_XACTS_PER_PAGE, limit - page_first_xid);
		full_bytes = (uint32)(in_scope / 4);
		partial_bits = (uint32)(in_scope % 4) * 2;

		if (full_bytes > 0 && memcmp(blob_page, local_page, full_bytes) != 0) {
			CloseTransientFile(src);
			return CLUSTER_XID_PREFIX_DIVERGED;
		}
		if (partial_bits > 0) {
			unsigned char mask = (unsigned char)((1 << partial_bits) - 1);

			if (((unsigned char)blob_page[full_bytes] & mask)
				!= ((unsigned char)local_page[full_bytes] & mask)) {
				CloseTransientFile(src);
				return CLUSTER_XID_PREFIX_DIVERGED;
			}
		}
	}
	CloseTransientFile(src);
	return CLUSTER_XID_PREFIX_CONSISTENT;
}
