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

#include "access/clog.h"		 /* GCS-race round-2 RC-E: verify reads through the
							 * SLRU (TransactionIdGetStatus) and repairs holes
							 * with ClusterClogAdoptNativeStatus */
#include "access/transam.h"		 /* ShmemVariableCache->oldestXid */
#include "cluster/cluster_cr.h"	 /* native-prehistory coverage latch */
#include "cluster/cluster_guc.h" /* cluster_shared_data_dir */
#include "cluster/cluster_xid_authority.h"
#include "cluster/cluster_xid_stripe.h" /* cluster_xid_widen (review F1) */
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
 * Post-recovery coverage verify + latch (GCS-race round-2 RC-E).
 * ============================================================ */

/*
 * cluster_xid_native_prehistory_provable_full -- pure judge: is `xid`
 * provably a native-era transaction whose outcome the local adopted CLOG
 * answers alias-free?
 *
 * Three conditions, each fail-closed on doubt:
 *	 covered_hw_full != 0	the post-recovery verify proved local pg_xact ==
 *							sealed blob over the whole surviving native range
 *							THIS boot (0 = not proven -> never route).
 *	 widen succeeds			the bare 32-bit tuple value is given its full
 *							64-bit identity in the signed +/- 2^31 window
 *							around the live counter (cluster_xid_widen) -- a
 *							bare compare with a "no wraparound yet" sentinel
 *							misjudges both halfspaces near an epoch boundary
 *							(review F1): a counter one shy of 2^32 makes a
 *							small value the NEXT epoch's upcoming allocation,
 *							not native prehistory.  Widen failure (special
 *							value, invalid counter, behind-window underflow)
 *							stays unprovable.
 *	 widened < native_hw	cluster-era xids start at the stripe activation
 *							floor, which is >= the native high-water, so a
 *							full identity below hw cannot have been allocated
 *							by any cluster-era writer (wrapped identities
 *							carry their epoch and compare above every epoch-0
 *							hw by construction).
 */
bool
cluster_xid_native_prehistory_provable_full(uint64 next_full_xid, uint64 covered_hw_full,
											TransactionId xid)
{
	FullTransactionId widened;

	if (covered_hw_full == 0)
		return false;
	if (!TransactionIdIsNormal(xid))
		return false;
	widened = cluster_xid_widen(xid, FullTransactionIdFromU64(next_full_xid));
	if (!FullTransactionIdIsValid(widened))
		return false;
	return U64FromFullTransactionId(widened) < covered_hw_full;
}

/*
 * cluster_xid_prehistory_verify_native_coverage -- StartupXLOG-tail boot
 * latch: prove local CLOG == sealed native prehistory, repair legitimate
 * replay-wiped holes, and only then enable native-era LOCAL routing.
 *
 * Runs in the startup process after redo (all boots, recovery or not),
 * before backends are admitted.  Reads the local side THROUGH the SLRU
 * (TransactionIdGetStatus), so no CLOG flush-timing assumption is made; the
 * blob side is the CRC-validated whole image (primary then .bak).
 *
 * Verdicts per xid in [max(oldestXid, FirstNormal), native_hw):
 *	 SUB_COMMITTED either side -> latch REJECTED (WARNING, return unlatched):
 *								  the prehistory carries no child->parent map,
 *								  so a sub-committed bit can never be proven
 *								  resolved or crash-aborted (review F2 /
 *								  calibration 1).  Both lineages can carry it
 *								  legitimately after a native-era crash mid
 *								  subcommit, so this is unprovable, NOT
 *								  divergent -- degrade to 53R97, never FATAL.
 *	 equal                     -> ok.  This includes IN_PROGRESS ==
 *								  IN_PROGRESS: the seal itself is the proof
 *								  (a true shutdown checkpoint seals only with
 *								  zero prepared and zero active xacts), so a
 *								  sealed in-progress bit is crash-aborted
 *								  forever -- no future can resolve it.
 *	 local IN_PROGRESS, blob
 *	 terminal                  -> repairable hole: a base-backup boot adopts
 *								  the blob into pg_xact files BEFORE redo, and
 *								  replaying the backup window's CLOG-extend
 *								  records legitimately re-zeroes pages the
 *								  adopt had filled.  Repair through the SLRU
 *								  and continue.
 *	 any other mismatch        -> FATAL: the node's own CLOG claims a
 *								  DIFFERENT outcome than the sealed history --
 *								  a divergent lineage the bootstrap prefix
 *								  check could not see (same class as review F2
 *								  / t/361 N5), never maskable.
 *
 * Every skip leg (authority unsealed/absent, over-cap era, native range
 * frozen away, blob unreadable) leaves the latch at 0: the resolver simply
 * keeps today's fail-closed 53R97 for below-floor xids -- degraded, never
 * wrong.
 */
void
cluster_xid_prehistory_verify_native_coverage(void)
{
	ClusterXidAuthorityHeader auth;
	bool from_bak = false;
	char primary[MAXPGPATH];
	char bak[MAXPGPATH];
	char page[BLCKSZ];
	const char *src_path = NULL;
	ClusterXidPrehistoryHeader hdr;
	uint32 pages = 0;
	uint32 p;
	uint64 oldest;
	uint64 repaired = 0;
	int src;

	if (!cluster_shared_catalog || !cluster_enabled)
		return;
	if (!cluster_xid_authority_read_checked(&auth, &from_bak))
		return; /* bootstrap already vetted presence; stay unlatched */
	if ((auth.flags & CLUSTER_XID_AUTHORITY_FLAG_SEALED) == 0 || auth.native_hw_full == 0)
		return;
	if (cluster_xid_prehistory_payload_bytes(auth.native_hw_full) == 0)
		return; /* over-cap era publishes nothing: never latched */

	/*
	 * Epoch gate (round-2 review F3): past an xid epoch rollover the local
	 * pg_xact positions below the native high-water belong to cluster-era
	 * xids -- "repairing" them from the blob would corrupt live outcomes,
	 * and a latch would prove nothing (the resolver's widen judge already
	 * rejects every bare value there).  Runs post-redo, so the counter is
	 * the recovered cluster truth.  Skip leg: latch stays off, 53R97.
	 */
	if (U64FromFullTransactionId(ReadNextFullTransactionId()) > (uint64)PG_UINT32_MAX) {
		elog(LOG, "cluster shared_catalog: xid epoch advanced past the native era; "
				  "prehistory coverage latch stays off");
		return;
	}

	oldest = (uint64)ShmemVariableCache->oldestXid;
	if (oldest < (uint64)FirstNormalTransactionId)
		oldest = (uint64)FirstNormalTransactionId;
	if (oldest >= auth.native_hw_full)
		return; /* native range frozen + truncated away: nothing to prove */

	if (!build_path(primary, sizeof(primary), CLUSTER_XID_PREHISTORY_REL_PATH)
		|| !build_path(bak, sizeof(bak), CLUSTER_XID_PREHISTORY_BAK_REL_PATH))
		return;
	if (verify_blob(primary, auth.native_hw_full, &pages))
		src_path = primary;
	else if (verify_blob(bak, auth.native_hw_full, &pages))
		src_path = bak;
	else {
		ereport(WARNING,
				(errmsg("shared XID prehistory is unreadable for the post-recovery coverage "
						"verify; native-era xids will fail closed on this node"),
				 errhint("Restore \"%s\" (or its .bak) under cluster.shared_data_dir to "
						 "re-enable native-era local CLOG routing.",
						 CLUSTER_XID_PREHISTORY_REL_PATH)));
		return;
	}

	src = OpenTransientFile(src_path, O_RDONLY | PG_BINARY);
	if (src < 0 || read(src, &hdr, sizeof(hdr)) != sizeof(hdr))
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
						errmsg("could not re-open shared XID prehistory \"%s\": %m", src_path)));

	for (p = 0; p < pages; p++) {
		uint64 page_first = (uint64)p * PREHISTORY_XACTS_PER_PAGE;
		uint64 lo;
		uint64 hi;
		uint64 x;

		if (read(src, page, BLCKSZ) != BLCKSZ)
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
					 errmsg("short read of shared XID prehistory \"%s\" page %u", src_path, p)));

		lo = Max(oldest, page_first);
		hi = Min(auth.native_hw_full, page_first + PREHISTORY_XACTS_PER_PAGE);
		for (x = lo; x < hi; x++) {
			uint32 idx = (uint32)(x - page_first);
			int blob_status = (page[idx / 4] >> ((idx % 4) * 2)) & 0x03;
			XLogRecPtr lsn;
			int local_status = (int)TransactionIdGetStatus((TransactionId)x, &lsn);

			/*
			 * SUB_COMMITTED on either side: unprovable, reject the latch
			 * (review F2 / calibration 1) -- see the verdict table above.
			 */
			if (blob_status == TRANSACTION_STATUS_SUB_COMMITTED
				|| local_status == TRANSACTION_STATUS_SUB_COMMITTED) {
				CloseTransientFile(src);
				ereport(WARNING,
						(errmsg("native-era xid " UINT64_FORMAT " is sub-committed (local %d, "
								"sealed blob %d); coverage cannot be proven, native-era xids "
								"will fail closed on this node",
								x, local_status, blob_status),
						 errhint("The native era crashed mid-subcommit; its sub-transaction "
								 "outcomes are unresolvable from the sealed prehistory.")));
				return;
			}
			if (local_status == blob_status)
				continue;
			if (local_status == TRANSACTION_STATUS_IN_PROGRESS) {
				ClusterClogAdoptNativeStatus((TransactionId)x, (XidStatus)blob_status);
				repaired++;
				continue;
			}
			CloseTransientFile(src);
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
					 errmsg("local pg_xact contradicts the sealed native-era prehistory after "
							"recovery"),
					 errdetail("xid " UINT64_FORMAT " is %d locally but %d in the sealed blob; "
							   "divergent native-era lineage.",
							   x, local_status, blob_status),
					 errhint("This node is not a clone of the seed lineage; destroy and "
							 "re-provision it from the shared tree.")));
		}
	}
	CloseTransientFile(src);

	if (repaired > 0)
		elog(LOG,
			 "cluster shared_catalog: repaired " UINT64_FORMAT " native-era pg_xact statuses "
			 "wiped by backup-window replay",
			 repaired);

	cluster_cr_native_prehistory_latch(auth.native_hw_full);
	elog(LOG,
		 "cluster shared_catalog: native XID prehistory coverage verified through " UINT64_FORMAT
		 "; native-era local CLOG routing enabled",
		 auth.native_hw_full);
}

/* ============================================================
 * Divergent-lineage guard (review F2; spec Q6 amendment).
 * ============================================================ */

/*
 * read_local_clog_page_optional -- read local pg_xact page `pageno` into
 * buf.  Returns false when the segment file or the page does not exist;
 * PANICs on a real read error so an I/O fault is never mistaken for a
 * missing page.  The caller decides what a missing page means: below
 * page(oldestXid) it is a legitimately truncated frozen segment, at or
 * above it is an anomaly (review r3-X2).
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
 * clog_slot_mask -- byte mask covering the 2-bit CLOG status slots
 * [lo, hi) within one byte (0 <= lo < hi <= 4).  Slot n of a byte holds
 * xid bits at (n * CLOG_BITS_PER_XACT), low-order first (clog.c
 * TransactionIdToBIndex).
 */
static inline unsigned char
clog_slot_mask(uint32 lo, uint32 hi)
{
	unsigned int m;

	m = ((hi >= 4) ? 0xFFu : ((1u << (hi * 2)) - 1u)) & ~((1u << (lo * 2)) - 1u);
	return (unsigned char)m;
}

/*
 * clog_page_range_equal -- compare blob vs local CLOG page content for the
 * page-local 2-bit slots [from_xact, to_xact) only.  Sub-byte boundaries
 * are masked so bits outside the range never influence the verdict.
 */
static bool
clog_page_range_equal(const char *blob_page, const char *local_page, uint32 from_xact,
					  uint32 to_xact)
{
	uint32 first_byte = from_xact / 4;
	uint32 last_byte = (to_xact - 1) / 4;

	Assert(from_xact < to_xact && to_xact <= PREHISTORY_XACTS_PER_PAGE);

	if (first_byte == last_byte)
		return ((unsigned char)(blob_page[first_byte] ^ local_page[first_byte])
				& clog_slot_mask(from_xact % 4, to_xact - first_byte * 4))
			   == 0;

	if (from_xact % 4 != 0) {
		if (((unsigned char)(blob_page[first_byte] ^ local_page[first_byte])
			 & clog_slot_mask(from_xact % 4, 4))
			!= 0)
			return false;
		first_byte++;
	}
	if (to_xact % 4 != 0) {
		if (((unsigned char)(blob_page[last_byte] ^ local_page[last_byte])
			 & clog_slot_mask(0, to_xact % 4))
			!= 0)
			return false;
		last_byte--;
	}
	return first_byte > last_byte
		   || memcmp(blob_page + first_byte, local_page + first_byte, last_byte - first_byte + 1)
				  == 0;
}

/*
 * cluster_xid_prehistory_prefix_check -- see header.  Streams the sealed
 * blob (primary, then .bak) and byte-compares the local pg_xact prefix
 * covering xids [oldest_xid_full, min(limit_xid_full, native_hw_full)) at
 * per-xact (2-bit) precision.
 *
 * Frozen-prefix exemption (review r3-X2): SimpleLruTruncate removes whole
 * pg_xact SEGMENTS once every page they cover precedes page(oldestXid)
 * (slru.c SlruMayDeleteSegment), so a joiner that froze past >=1 segment
 * legitimately has no file where the blob still has pages -- the old
 * break-on-first-missing-local-page treated exactly that shape as a
 * tail-short clone and returned CONSISTENT without ever comparing the
 * surviving pages.  Neither truncation nor tuple freezing rewrites the
 * CLOG bytes that survive, but bits below oldestXid are dead content
 * anyway (every tuple with such an xmin/xmax is frozen, so CLOG is never
 * consulted for them again) and whether they still exist is an accident
 * of truncation timing; lineage evidence therefore starts at oldestXid's
 * own 2-bit slot, and everything below it is skipped whether the page
 * survives or not.  Within the comparable range [oldestXid, limit) a
 * MISSING local page is an anomaly -- pg_xact always covers
 * [page(oldestXid), page(nextXid)] on a well-formed node -- and returns
 * UNAVAILABLE (fail-closed), never CONSISTENT.
 */
ClusterXidPrefixVerdict
cluster_xid_prehistory_prefix_check(const char *local_pgdata, uint64 native_hw_full,
									uint64 limit_xid_full, uint64 oldest_xid_full)
{
	ClusterXidPrehistoryHeader hdr;
	char primary[MAXPGPATH];
	char bak[MAXPGPATH];
	char blob_page[BLCKSZ];
	char local_page[BLCKSZ];
	const char *src_path;
	uint32 pages = 0;
	uint32 start_page;
	uint32 p;
	uint64 limit;
	uint64 oldest;
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

	oldest = Min(oldest_xid_full, limit);
	if (oldest >= limit)
		return CLUSTER_XID_PREFIX_CONSISTENT; /* everything comparable is frozen */
	start_page = (uint32)(oldest / PREHISTORY_XACTS_PER_PAGE);

	src = OpenTransientFile(src_path, O_RDONLY | PG_BINARY);
	if (src < 0 || read(src, &hdr, sizeof(hdr)) != sizeof(hdr)) {
		if (src >= 0)
			CloseTransientFile(src);
		return CLUSTER_XID_PREFIX_UNAVAILABLE;
	}

	for (p = 0; p < pages; p++) {
		uint64 page_first_xid = (uint64)p * PREHISTORY_XACTS_PER_PAGE;
		uint64 cmp_from;
		uint64 cmp_to;

		if (read(src, blob_page, BLCKSZ) != BLCKSZ) {
			CloseTransientFile(src);
			return CLUSTER_XID_PREFIX_UNAVAILABLE;
		}
		if (page_first_xid >= limit)
			break; /* comparison scope ended on a page boundary */
		if (p < start_page)
			continue; /* wholly frozen page: skipped, present or not */

		cmp_from = Max(page_first_xid, oldest);
		cmp_to = Min(page_first_xid + PREHISTORY_XACTS_PER_PAGE, limit);
		/* oldest < limit and p >= start_page guarantee a non-empty range */
		Assert(cmp_from < cmp_to);

		if (!read_local_clog_page_optional(local_pgdata, p, local_page)) {
			/* hole inside the comparable range: fail closed (r3-X2) */
			CloseTransientFile(src);
			return CLUSTER_XID_PREFIX_UNAVAILABLE;
		}

		if (!clog_page_range_equal(blob_page, local_page, (uint32)(cmp_from - page_first_xid),
								   (uint32)(cmp_to - page_first_xid))) {
			CloseTransientFile(src);
			return CLUSTER_XID_PREFIX_DIVERGED;
		}
	}
	CloseTransientFile(src);
	return CLUSTER_XID_PREFIX_CONSISTENT;
}

/* ============================================================
 * Pre-migrate xid epoch witness (round-2 review F3).
 * ============================================================ */

/*
 * cluster_xid_epoch_witness_write -- durably persist this node's own
 * pre-migration nextFullXid under local_pgdata.  See the header for why
 * this must happen BEFORE the shared-authority symlink flip.  tmp + fsync
 * + durable_rename keeps it torn-safe; rewriting on a crash-retry of the
 * migrate arm is idempotent (the local control file is still real there).
 */
bool
cluster_xid_epoch_witness_write(const char *local_pgdata, uint64 next_full_xid)
{
	ClusterXidEpochWitness w;
	char tmp[MAXPGPATH];
	char final[MAXPGPATH];
	pg_crc32c crc;
	int fd;

	if (snprintf(final, sizeof(final), "%s/%s", local_pgdata, CLUSTER_XID_EPOCH_WITNESS_REL_PATH)
			>= (int)sizeof(final)
		|| snprintf(tmp, sizeof(tmp), "%s/%s", local_pgdata, CLUSTER_XID_EPOCH_WITNESS_TMP_REL_PATH)
			   >= (int)sizeof(tmp))
		return false;

	memset(&w, 0, sizeof(w));
	w.magic = CLUSTER_PGXW_MAGIC;
	w.version = CLUSTER_PGXW_VERSION;
	w.next_full_xid = next_full_xid;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &w, offsetof(ClusterXidEpochWitness, crc));
	FIN_CRC32C(crc);
	w.crc = crc;

	fd = OpenTransientFile(tmp, O_CREAT | O_TRUNC | O_WRONLY | PG_BINARY);
	if (fd < 0)
		return false;
	if (write(fd, &w, sizeof(w)) != (ssize_t)sizeof(w) || pg_fsync(fd) != 0) {
		CloseTransientFile(fd);
		unlink(tmp);
		return false;
	}
	if (CloseTransientFile(fd) != 0)
		return false;
	if (durable_rename(tmp, final, LOG) != 0)
		return false;
	return true;
}

/*
 * cluster_xid_epoch_witness_read -- read the witness back; every doubt leg
 * (absent, short, bad magic/version/CRC) returns false.
 */
bool
cluster_xid_epoch_witness_read(const char *local_pgdata, uint64 *next_full_xid)
{
	ClusterXidEpochWitness w;
	char path[MAXPGPATH];
	pg_crc32c crc;
	int fd;
	int r;

	if (snprintf(path, sizeof(path), "%s/%s", local_pgdata, CLUSTER_XID_EPOCH_WITNESS_REL_PATH)
		>= (int)sizeof(path))
		return false;

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return false;
	r = (int)read(fd, &w, sizeof(w));
	CloseTransientFile(fd);
	if (r != (int)sizeof(w))
		return false;
	if (w.magic != CLUSTER_PGXW_MAGIC || w.version != CLUSTER_PGXW_VERSION)
		return false;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &w, offsetof(ClusterXidEpochWitness, crc));
	FIN_CRC32C(crc);
	if (!EQ_CRC32C(crc, w.crc))
		return false;
	*next_full_xid = w.next_full_xid;
	return true;
}
