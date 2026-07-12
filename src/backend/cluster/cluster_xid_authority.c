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
#include <sys/file.h> /* round-3b P0-1: flock mutation lock */
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

	/* Round-3b P0-4: stamped images carry the RAW_REUSED magic; this
	 * binary accepts both, a pre-barrier binary rejects the stamped one
	 * (its only-magic check) and fail-closes instead of ignoring the
	 * flag it cannot understand. */
	if (hdr.magic != CLUSTER_XID_AUTHORITY_MAGIC
		&& hdr.magic != CLUSTER_XID_AUTHORITY_MAGIC_RAW_REUSED)
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
 * copy_flags_settled -- does the on-disk copy at relpath validate AND carry
 * all `must_set` flag bits with all `must_clear` bits clear?
 */
static bool
copy_flags_settled(const char *relpath, uint32 must_set, uint32 must_clear)
{
	ClusterXidAuthorityHeader hdr;
	char path[MAXPGPATH];
	char image[CLUSTER_XID_AUTHORITY_FILE_SIZE];

	if (!build_path(path, sizeof(path), relpath))
		return false;
	if (read_image(path, image) != CLUSTER_XID_AUTHORITY_VALID)
		return false;
	memcpy(&hdr, image, sizeof(hdr));
	/* Round-3b P0-4: the RAW_REUSED transition is settled only once the copy
	 * ALSO carries the stamped magic -- a flag written under the old magic
	 * (pre-hardening tree) stays readable to a pre-barrier binary, so the
	 * re-assert must upgrade it. */
	if ((must_set & CLUSTER_XID_AUTHORITY_FLAG_NATIVE_RAW_REUSED)
		&& hdr.magic != CLUSTER_XID_AUTHORITY_MAGIC_RAW_REUSED)
		return false;
	return (hdr.flags & must_set) == must_set && (hdr.flags & must_clear) == 0;
}

/*
 * both_copies_flags_settled -- is a one-way flag transition complete in BOTH
 * on-disk copies?  Flag transitions install the same image as primary and
 * .bak (primary first), so a crash between the two durable renames leaves a
 * pre-transition .bak behind an already-transitioned primary.  Nothing else
 * on the restart path rewrites the pair, so without a re-assert the stale
 * copy survives the whole run, and any later transient primary read failure
 * resurrects the pre-transition flags through the .bak fallback -- a stale
 * SEALED .bak hands joiners the previous pass's high-water and a stale
 * pre-CLUSTER_ERA .bak re-opens the native-era re-entry guard (review
 * r3-X1, 8.A).  The transition entry points therefore skip their write only
 * when this returns true; a missing or invalid copy is "not settled" too,
 * which restores the damaged copy for free.
 */
static bool
both_copies_flags_settled(uint32 must_set, uint32 must_clear)
{
	return copy_flags_settled(CLUSTER_XID_AUTHORITY_REL_PATH, must_set, must_clear)
		   && copy_flags_settled(CLUSTER_XID_AUTHORITY_BAK_REL_PATH, must_set, must_clear);
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
 * build_writer_tmp_path -- per-writer staging name (round-3b P0-1).
 *
 * Every node's LMON crosses the wrap-barrier margin within the same
 * herding window and re-asserts the one-way flags concurrently
 * (mark_native_raw_reused; concurrent first cluster boots do the same
 * with mark_cluster_era), so a SHARED tmp name would let two writers
 * truncate each other's staging file mid-write and fail the durable
 * rename PANIC-hard.  A per-writer suffix keeps staging private while
 * the final rename stays atomic; concurrent read-modify-write converges
 * because the flags are one-way ORs and the high-waters monotone maxes,
 * so whichever install lands last carries the same transition.  The
 * node id is the stable suffix (bounded leftover set); a pre-identity
 * writer falls back to its pid.
 */
static bool
build_writer_tmp_path(char *dst, size_t dstlen, const char *relpath)
{
	size_t used;

	if (!build_path(dst, dstlen, relpath))
		return false;
	used = strlen(dst);
	if (cluster_node_id >= 0)
		snprintf(dst + used, dstlen - used, ".n%d", cluster_node_id);
	else
		snprintf(dst + used, dstlen - used, ".p%d", (int)getpid());
	return true;
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
		|| !build_writer_tmp_path(tmp, sizeof(tmp), CLUSTER_XID_AUTHORITY_TMP_REL_PATH)
		|| !build_writer_tmp_path(baktmp, sizeof(baktmp), CLUSTER_XID_AUTHORITY_BAK_TMP_REL_PATH))
		ereport(PANIC, (errmsg("cluster shared_data_dir is not configured")));

	/* Round-3b P0-4: a stamped image switches to the RAW_REUSED magic so a
	 * pre-barrier binary fail-closes on it instead of ignoring the flag. */
	hdr->magic = (hdr->flags & CLUSTER_XID_AUTHORITY_FLAG_NATIVE_RAW_REUSED)
					 ? CLUSTER_XID_AUTHORITY_MAGIC_RAW_REUSED
					 : CLUSTER_XID_AUTHORITY_MAGIC;
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
 * both new.  The primary-new/.bak-old window does not self-repair on its
 * own: the transition entry points are re-run by every boot and re-assert
 * both copies until both_copies_flags_settled holds (review r3-X1), so a
 * later single-copy corruption never resurrects the pre-transition flags.
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
		|| !build_writer_tmp_path(tmp, sizeof(tmp), CLUSTER_XID_AUTHORITY_TMP_REL_PATH)
		|| !build_writer_tmp_path(baktmp, sizeof(baktmp), CLUSTER_XID_AUTHORITY_BAK_TMP_REL_PATH))
		ereport(PANIC, (errmsg("cluster shared_data_dir is not configured")));

	/* Round-3b P0-4: a stamped image switches to the RAW_REUSED magic so a
	 * pre-barrier binary fail-closes on it instead of ignoring the flag. */
	hdr->magic = (hdr->flags & CLUSTER_XID_AUTHORITY_FLAG_NATIVE_RAW_REUSED)
					 ? CLUSTER_XID_AUTHORITY_MAGIC_RAW_REUSED
					 : CLUSTER_XID_AUTHORITY_MAGIC;
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
 * Round-3b review P0-1: cross-node authority mutation lock.
 *
 * Every transition below is a read-modify-write of the SAME shared file
 * pair from multiple nodes (concurrent first cluster boots stamping
 * CLUSTER_ERA, every LMON re-asserting NATIVE_RAW_REUSED inside the same
 * herding window, a follow-up native run unsealing).  The per-writer tmp
 * names only keep the STAGING private; two unserialized read-modify-writes
 * can still install each other's stale header -- erasing a one-way flag or
 * the RAW_REUSED magic -- and the LMON never re-reads after marked/done,
 * so a lost stamp could let a later boot re-latch the native prehistory
 * after the wrap (rule 8.A).  Serialize with flock(2) on a dedicated lock
 * file in the shared root: the kernel drops the lock on any process exit
 * (crash-safe, no stale-lockfile recovery), and the shared-FS tier
 * presents it cluster-wide.  Every transition re-reads the freshest header
 * INSIDE the critical section, applies its change to that image (one-way
 * flag ORs and monotone maxes make sequential application a merge), writes,
 * and read-back-verifies both installed copies.
 *
 * Mixed-version (round-4 review): a pre-flock binary does not take this
 * lock, and the barrier bit (XID_NATIVE_DISABLE_V1) does NOT prove the
 * flock protocol -- a round-3b binary advertises it while still writing
 * lock-free.  The wrap barrier therefore refuses to open the allocation
 * gate until every conf-declared member is connected and advertises the
 * DISTINCT XID_AUTHORITY_FLOCK_V2 capability; pre-gate erases by such a
 * writer are harmless (no epoch-1 xid exists yet) and are repaired by the
 * tick's settle re-assert.  Post-stamp, an old binary fail-closes on the
 * RAW_REUSED magic before it can reach any transition write.  Cross-host
 * advisory-lock fidelity is a property of the shared-FS tier; the
 * production backend contract (spec-6.0a) must pin it, and the gate's
 * settle re-verify stays the last line either way.
 */
static int
authority_mutation_lock(void)
{
	char path[MAXPGPATH];
	int fd;

	if (!build_path(path, sizeof(path), CLUSTER_XID_AUTHORITY_LOCK_REL_PATH))
		return -1; /* no shared root configured: nothing shared to race */
	fd = OpenTransientFile(path, O_RDWR | O_CREAT | PG_BINARY);
	if (fd < 0)
		ereport(PANIC, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", path)));
	while (flock(fd, LOCK_EX) != 0) {
		if (errno == EINTR)
			continue;
		ereport(PANIC, (errcode_for_file_access(), errmsg("could not lock file \"%s\": %m", path)));
	}
	return fd;
}

static void
authority_mutation_unlock(int lockfd)
{
	if (lockfd < 0)
		return;
	/* the flock drops with the descriptor */
	if (CloseTransientFile(lockfd) != 0)
		ereport(PANIC, (errcode_for_file_access(), errmsg("could not close file \"%s\": %m",
														  CLUSTER_XID_AUTHORITY_LOCK_REL_PATH)));
}

/*
 * verify_installed -- read back one installed copy and require it to match
 * the just-written header exactly (magic / flags / high-waters; the CRC is
 * implied by classify).  Under the mutation lock nobody else may write, so
 * any mismatch means the durable rename did not land what we staged --
 * storage-level damage, not a concurrency artifact -- and consuming it
 * could hand out a pre-transition image (rule 8.A).  PANIC.
 */
static void
verify_installed(const char *relpath, const ClusterXidAuthorityHeader *want)
{
	ClusterXidAuthorityHeader got;
	char path[MAXPGPATH];
	char image[CLUSTER_XID_AUTHORITY_FILE_SIZE];

	if (!build_path(path, sizeof(path), relpath))
		return;
	if (read_image(path, image) != CLUSTER_XID_AUTHORITY_VALID)
		ereport(PANIC, (errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
						errmsg("shared XID authority read-back failed for \"%s\" after install",
							   relpath)));
	memcpy(&got, image, sizeof(got));
	if (got.magic != want->magic || got.flags != want->flags
		|| got.native_hw_full != want->native_hw_full || got.next_multi != want->next_multi)
		ereport(PANIC, (errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
						errmsg("shared XID authority read-back mismatch for \"%s\" after install",
							   relpath),
						errdetail("magic %08X/%08X flags %04X/%04X (installed/expected).",
								  got.magic, want->magic, got.flags, want->flags)));
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
	int lockfd;

	/* round-4 P0-1 (review P1): the read AND the corrupt-vs-absent probe
	 * both run under the lock -- a pre-lock probe could see a concurrent
	 * seeder's half-visible install and mis-FATAL a healthy tree. */
	lockfd = authority_mutation_lock();
	if (cluster_xid_authority_read(&hdr)) {
		/* already seeded (join node / prior seed / concurrent seeder) */
		authority_mutation_unlock(lockfd);
		return false;
	}
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
	verify_installed(CLUSTER_XID_AUTHORITY_REL_PATH, &hdr);
	authority_mutation_unlock(lockfd);
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
	int lockfd;

	/* round-3b P0-1: fresh read + apply + install are one critical section */
	lockfd = authority_mutation_lock();
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
	verify_installed(CLUSTER_XID_AUTHORITY_REL_PATH, &hdr);
	authority_mutation_unlock(lockfd);
}

/*
 * cluster_xid_authority_mark_cluster_era -- one-way CLUSTER_ERA stamp (first
 * cluster.enabled=on boot; spec §3.1).  Idempotent re-assert: re-run by
 * every cluster boot, it rewrites BOTH copies until both carry the flag, so
 * a crash between write_header_both's two renames (stamped primary, stale
 * .bak) is repaired on the next boot instead of surviving the run (review
 * r3-X1); once both copies are settled it is a no-write no-op.  A
 * missing/corrupt authority PANICs: the catalog bootstrap seeds it before
 * any caller can reach this point, so absence here is real damage.
 */
void
cluster_xid_authority_mark_cluster_era(void)
{
	ClusterXidAuthorityHeader hdr;
	int lockfd;

	/* round-3b P0-1: fresh read + apply + install are one critical section */
	lockfd = authority_mutation_lock();
	if (!cluster_xid_authority_read(&hdr))
		ereport(PANIC, (errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
						errmsg("shared XID authority is missing or corrupt at cluster-era stamp")));

	if ((hdr.flags & CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA) != 0
		&& both_copies_flags_settled(CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA, 0)) {
		authority_mutation_unlock(lockfd);
		return; /* transition complete in both copies */
	}

	hdr.flags |= CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA;
	write_header_both(&hdr); /* review F1 variant: one-way flag in BOTH copies */
	verify_installed(CLUSTER_XID_AUTHORITY_REL_PATH, &hdr);
	verify_installed(CLUSTER_XID_AUTHORITY_BAK_REL_PATH, &hdr);
	authority_mutation_unlock(lockfd);
}

/*
 * cluster_xid_authority_mark_native_raw_reused -- one-way NATIVE_RAW_REUSED
 * stamp (GCS-race round-3 P0-1).  The LMON wrap barrier stamps this durably
 * BEFORE any node may allocate an epoch>=1 xid: from that point on a raw
 * 32-bit value below the native high-water is no longer an alias-free
 * native-era identity, so no future boot may latch the native-prehistory
 * coverage (below-hw recycled refs stay fail-closed 53R97).  Same idempotent
 * both-copies re-assert discipline as the cluster-era stamp: re-run until
 * BOTH copies carry the flag (repairing a torn previous stamp), then a
 * no-write no-op.  A missing/corrupt authority PANICs -- the bootstrap
 * seeded it long before any allocator can approach the epoch boundary, so
 * absence here is real damage.
 */
void
cluster_xid_authority_mark_native_raw_reused(void)
{
	ClusterXidAuthorityHeader hdr;
	int lockfd;

	/* round-3b P0-1: fresh read + apply + install are one critical section */
	lockfd = authority_mutation_lock();
	if (!cluster_xid_authority_read(&hdr))
		ereport(PANIC,
				(errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
				 errmsg("shared XID authority is missing or corrupt at native-raw-reused stamp")));

	if ((hdr.flags & CLUSTER_XID_AUTHORITY_FLAG_NATIVE_RAW_REUSED) != 0
		&& both_copies_flags_settled(CLUSTER_XID_AUTHORITY_FLAG_NATIVE_RAW_REUSED, 0)) {
		authority_mutation_unlock(lockfd);
		return; /* transition complete in both copies */
	}

	hdr.flags |= CLUSTER_XID_AUTHORITY_FLAG_NATIVE_RAW_REUSED;
	write_header_both(&hdr);
	verify_installed(CLUSTER_XID_AUTHORITY_REL_PATH, &hdr);
	verify_installed(CLUSTER_XID_AUTHORITY_BAK_REL_PATH, &hdr);
	authority_mutation_unlock(lockfd);
}

/*
 * Round-3b P0-1: durable settle probe for the wrap barrier's gate-open
 * re-verify.  True only when BOTH copies validate, carry the flag, and
 * carry the stamped magic (the settle predicate already enforces the
 * magic for RAW_REUSED, review P0-4).
 */
bool
cluster_xid_authority_raw_reused_settled(void)
{
	return both_copies_flags_settled(CLUSTER_XID_AUTHORITY_FLAG_NATIVE_RAW_REUSED, 0);
}

/*
 * cluster_xid_authority_mark_epoch_gate_admitted -- one-way admission proof
 * (GCS-race round-4c P0-1 residual #2).  The wrap-barrier coordinator calls
 * this AFTER the full admission held (every declared member connected +
 * flock-capable + ack'd) and RAW_REUSED settled, right BEFORE opening the
 * epoch-allocation gate.  Also re-asserts RAW_REUSED (admission implies the
 * stamp; keeps a single repair call sufficient for the post-done settle
 * watch).  Same idempotent both-copies discipline as the other one-way
 * flags; missing/corrupt authority PANICs (bootstrap seeded it long ago).
 */
void
cluster_xid_authority_mark_epoch_gate_admitted(void)
{
	ClusterXidAuthorityHeader hdr;
	int lockfd;
	uint32 want = CLUSTER_XID_AUTHORITY_FLAG_NATIVE_RAW_REUSED
				  | CLUSTER_XID_AUTHORITY_FLAG_EPOCH_GATE_ADMITTED;

	lockfd = authority_mutation_lock();
	if (!cluster_xid_authority_read(&hdr))
		ereport(
			PANIC,
			(errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
			 errmsg("shared XID authority is missing or corrupt at epoch-gate-admitted stamp")));

	if ((hdr.flags & want) == want && both_copies_flags_settled(want, 0)) {
		authority_mutation_unlock(lockfd);
		return; /* transition complete in both copies */
	}

	hdr.flags |= want;
	write_header_both(&hdr);
	verify_installed(CLUSTER_XID_AUTHORITY_REL_PATH, &hdr);
	verify_installed(CLUSTER_XID_AUTHORITY_BAK_REL_PATH, &hdr);
	authority_mutation_unlock(lockfd);
}

/*
 * Settle probe for the boot shortcut + the post-done settle watch: true only
 * when BOTH copies validate and carry BOTH one-way flags (the settle
 * predicate enforces the stamped magic for RAW_REUSED, review P0-4).
 */
bool
cluster_xid_authority_epoch_gate_admitted_settled(void)
{
	return both_copies_flags_settled(CLUSTER_XID_AUTHORITY_FLAG_NATIVE_RAW_REUSED
										 | CLUSTER_XID_AUTHORITY_FLAG_EPOCH_GATE_ADMITTED,
									 0);
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
 *
 *	Idempotent re-assert (review r3-X1): re-run by every native-era boot,
 *	it rewrites BOTH copies until neither retains SEALED, so a crash
 *	between write_header_both's two renames (unsealed primary, stale
 *	SEALED .bak) is repaired on the next boot instead of surviving the
 *	run; once both copies are settled it is a no-write no-op.
 */
void
cluster_xid_authority_begin_native_run(void)
{
	ClusterXidAuthorityHeader hdr;
	int lockfd;

	/* round-3b P0-1: fresh read + apply + install are one critical section
	 * (FATAL/PANIC exits drop the flock with the process) */
	lockfd = authority_mutation_lock();
	if (!cluster_xid_authority_read(&hdr))
		ereport(PANIC, (errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
						errmsg("shared XID authority is missing or corrupt at native-run open")));

	if (hdr.flags & CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA)
		ereport(FATAL,
				(errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
				 errmsg("native seed era cannot be re-entered on a formed shared catalog tree")));

	if ((hdr.flags & CLUSTER_XID_AUTHORITY_FLAG_SEALED) == 0
		&& both_copies_flags_settled(0, CLUSTER_XID_AUTHORITY_FLAG_SEALED)) {
		authority_mutation_unlock(lockfd);
		return; /* open, and no copy retains SEALED */
	}

	hdr.flags &= ~CLUSTER_XID_AUTHORITY_FLAG_SEALED;
	write_header_both(&hdr); /* review F1: no copy may retain SEALED */
	verify_installed(CLUSTER_XID_AUTHORITY_REL_PATH, &hdr);
	verify_installed(CLUSTER_XID_AUTHORITY_BAK_REL_PATH, &hdr);
	authority_mutation_unlock(lockfd);
}
