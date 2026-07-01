/*-------------------------------------------------------------------------
 *
 * cluster_voting_disk_io.c
 *	  Voting disk slot R/W primitives — spec-2.6 Sprint A Step 2 D3.
 *
 *	  See cluster_voting_disk_io.h for full design notes.
 *
 *	  Q2 v0.2 correctness model — the I/O path does NOT rely on POSIX
 *	  sector-atomic write guarantees (those are storage-backend +
 *	  filesystem dependent and absent on NFS / iSCSI / cloud block).
 *	  Defence:
 *	    1. Each slot write bumps `generation` (caller's responsibility)
 *	    2. CRC32C covers byte 0..507 (everything except the CRC itself)
 *	    3. Read verifies CRC + magic + version + node_id round-trip
 *	    4. Any mismatch → return non-OK state → caller marks disk_
 *	       io_failure_inflight + retries next cycle (multi-disk
 *	       redundancy + multi-cycle convergence absorb the brief
 *	       uncertainty)
 *
 *	  O_DIRECT is best-effort:  we attempt with O_DIRECT first to
 *	  bypass OS page cache (per spec-2.6 Q2 v0.2 + feature-002
 *	  voting-disk design);on EINVAL (filesystem refuses) we fall
 *	  back to O_SYNC + explicit fdatasync after every write.  Either
 *	  path is correct under the generation+CRC protocol;O_DIRECT is
 *	  preferred when available because it reduces double-buffering
 *	  during partition-style cluster I/O storms.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_voting_disk_io.c
 *
 * NOTES
 *	  pgrac-original file.  Compiled only in --enable-cluster builds.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_voting_disk_io.h"

#ifdef USE_PGRAC_CLUSTER

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>


/* ============================================================
 * Hardening v0.4 P1.4 — per-I/O timeout via SIGALRM + setitimer.
 *
 *	cluster.voting_disk_io_timeout_ms in the original v0.14.0 ship was
 *	a GUC with no enforcement — pread / pwrite / fdatasync ran without
 *	any deadline, so a hung disk could pin qvotec indefinitely.  This
 *	round wires the GUC to a real per-syscall guard:
 *
 *	  1. qvotec and ADG MRP startup call cluster_voting_disk_io_install_
 *	     timeout_handler() once.  That replaces SIG_IGN with our async-
 *	     signal-safe handler that flips a sig_atomic_t flag.
 *	  2. cluster_voting_disk_io_set_timeout_ms(...) is called by those aux
 *	     processes at startup (and on SIGHUP for long-lived loops) to seed
 *	     the deadline used by every subsequent read_slot / write_slot call.
 *	  3. read_slot / write_slot wrap their syscalls in arm_timeout()
 *	     / disarm_timeout() so SIGALRM fires after the configured
 *	     deadline if the syscall hasn't returned.  EINTR + flag-set ⇒
 *	     return CLUSTER_VOTING_DISK_IO_FAILED.
 *
 *	Only qvotec/MRP install the handler;backends use SIGALRM for
 *	statement_timeout / per-statement deadlines, so installing this handler
 *	in their context would clobber PG's machinery.
 *
 *	timeout_ms == 0 disarms the timer (format / fsck tools want
 *	unbounded I/O).
 * ============================================================ */

static volatile sig_atomic_t voting_disk_io_timeout_fired = 0;
static int voting_disk_io_timeout_ms = 0;

static void
voting_disk_io_alarm_handler(SIGNAL_ARGS pg_attribute_unused())
{
	/* async-signal-safe: just set the flag.  No palloc, no elog. */
	voting_disk_io_timeout_fired = 1;
}

void
cluster_voting_disk_io_install_timeout_handler(void)
{
	struct sigaction act;

	memset(&act, 0, sizeof(act));
	act.sa_handler = voting_disk_io_alarm_handler;
	sigemptyset(&act.sa_mask);
	/* No SA_RESTART — we WANT pread/pwrite to return EINTR so we can
	 * observe the timeout flag.  A restarted syscall would never
	 * return until success/failure of its own. */
	act.sa_flags = 0;
	(void)sigaction(SIGALRM, &act, NULL);
}

void
cluster_voting_disk_io_set_timeout_ms(int timeout_ms)
{
	voting_disk_io_timeout_ms = (timeout_ms < 0) ? 0 : timeout_ms;
}

static void
voting_disk_io_arm_timeout(void)
{
	struct itimerval it;

	voting_disk_io_timeout_fired = 0;
	if (voting_disk_io_timeout_ms <= 0)
		return; /* disabled */

	memset(&it, 0, sizeof(it));
	it.it_value.tv_sec = voting_disk_io_timeout_ms / 1000;
	it.it_value.tv_usec = (long)(voting_disk_io_timeout_ms % 1000) * 1000;
	(void)setitimer(ITIMER_REAL, &it, NULL);
}

static void
voting_disk_io_disarm_timeout(void)
{
	struct itimerval it;

	memset(&it, 0, sizeof(it));
	(void)setitimer(ITIMER_REAL, &it, NULL);
}


/* ============================================================
 * cluster_voting_disk_open / _close
 * ============================================================ */

int
cluster_voting_disk_open(const char *path, bool create_if_missing)
{
	int flags;
	int fd;
	int saved_errno;

	if (path == NULL || path[0] == '\0') {
		errno = EINVAL;
		return -1;
	}

	flags = O_RDWR;
	if (create_if_missing)
		flags |= O_CREAT | O_EXCL;

		/*
		 * Best-effort O_DIRECT (Q2 v0.2):  bypass OS page cache when the
		 * underlying filesystem supports it.  Linux ext4 / xfs / nvme do;
		 * tmpfs / NFS commonly reject with EINVAL.  Fall back to O_SYNC
		 * which forces flush-to-storage on every write — slower but
		 * still correct under generation+CRC protocol.
		 */
#ifdef O_DIRECT
	fd = open(path, flags | O_DIRECT, S_IRUSR | S_IWUSR);
	if (fd < 0 && errno == EINVAL)
		fd = open(path, flags | O_SYNC, S_IRUSR | S_IWUSR);
#else
	fd = open(path, flags | O_SYNC, S_IRUSR | S_IWUSR);
#endif

	if (fd < 0) {
		saved_errno = errno;
		errno = saved_errno;
		return -1;
	}

	return fd;
}

void
cluster_voting_disk_close(int fd)
{
	if (fd >= 0)
		close(fd);
}


/* ============================================================
 * CRC32C helper
 * ============================================================ */

pg_crc32c
cluster_voting_disk_compute_crc32c(const ClusterVotingSlot *slot)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	/* Cover byte 0..507 (everything except the trailing 4-byte crc32c). */
	COMP_CRC32C(crc, slot, offsetof(ClusterVotingSlot, crc32c));
	FIN_CRC32C(crc);

	return crc;
}


/* ============================================================
 * cluster_voting_disk_read_slot
 *
 *	Reads 512 bytes at offset = node_id × 512;verifies CRC + magic +
 *	version + node_id;populates `*out`.  The buffer alignment
 *	requirement (some O_DIRECT implementations demand 512-byte
 *	alignment) is satisfied by an aligned automatic variable —
 *	using a struct field directly may not be sufficiently aligned
 *	on all platforms, but the test harness exercises both paths.
 * ============================================================ */

ClusterVotingDiskIoState
cluster_voting_disk_read_slot(int fd, int expected_disk_index, uint32 node_id,
							  ClusterVotingSlot *out)
{
	/* 512-byte aligned buffer for O_DIRECT compatibility. */
	union {
		ClusterVotingSlot slot;
		char bytes[CLUSTER_VOTING_SLOT_BYTES];
	} aligned __attribute__((aligned(512)));

	ssize_t nread;
	pg_crc32c expected_crc;

	if (fd < 0)
		return CLUSTER_VOTING_DISK_IO_NOT_TRIED;
	if (out == NULL)
		return CLUSTER_VOTING_DISK_IO_FAILED;

	memset(&aligned, 0, sizeof(aligned));

	/* Hardening v0.4 P1.4: arm per-I/O timeout (no-op when disabled). */
	voting_disk_io_arm_timeout();
	nread
		= pread(fd, aligned.bytes, CLUSTER_VOTING_SLOT_BYTES, CLUSTER_VOTING_SLOT_OFFSET(node_id));
	voting_disk_io_disarm_timeout();
	if (nread != CLUSTER_VOTING_SLOT_BYTES) {
		/* Short read / EIO / EOF / SIGALRM-interrupted (timeout) —
		 * defensive treat as torn or failed.  voting_disk_io_timeout_
		 * fired distinguishes timeout from other failures for the
		 * caller's diagnostics. */
		return CLUSTER_VOTING_DISK_IO_FAILED;
	}

	/*
	 * Verify CRC32C first — this catches torn writes and corruption
	 * before we trust any field.  Mismatch = TORN (caller decrements
	 * disks_ok_count + retries next cycle per Q2 v0.2 protocol).
	 */
	expected_crc = cluster_voting_disk_compute_crc32c(&aligned.slot);
	if (aligned.slot.crc32c != expected_crc)
		return CLUSTER_VOTING_DISK_IO_TORN;

	/*
	 * Sanity:  magic + version must match expected.  Field mismatch is
	 * a programming error or wrong-file read, not a torn write — return
	 * FAILED.
	 */
	if (aligned.slot.magic != CLUSTER_VOTING_SLOT_MAGIC
		|| aligned.slot.version != CLUSTER_VOTING_SLOT_VERSION)
		return CLUSTER_VOTING_DISK_IO_FAILED;

	/*
	 * Sanity:  slot.node_id MUST equal the requested node_id.  If they
	 * differ, a different instance wrote at the wrong offset (data
	 * corruption / misconfiguration);refuse to trust this slot.
	 */
	if (aligned.slot.node_id != node_id)
		return CLUSTER_VOTING_DISK_IO_FAILED;

	/*
	 * Q3 v0.2 misroute defense:  slot.disk_index MUST equal the caller's
	 * expected_disk_index.  Mismatch means the fd is reading from a
	 * voting disk file that does NOT correspond to its CSV slot — likely
	 * SAN/NFS multipath misroute, wrong mount point, swapped symlink, or
	 * incorrect cluster.voting_disks ordering.  Treat as FAILED; the
	 * caller increments io_error_count and the disk drops out of the
	 * healthy-disk count for this poll cycle.  expected_disk_index < 0
	 * skips the check (format / repair / fsck tools).
	 */
	if (expected_disk_index >= 0 && aligned.slot.disk_index != (uint32)expected_disk_index)
		return CLUSTER_VOTING_DISK_IO_FAILED;

	*out = aligned.slot;
	return CLUSTER_VOTING_DISK_IO_OK;
}


/* ============================================================
 * cluster_voting_disk_write_slot
 *
 *	Caller has populated all slot fields except crc32c (recomputed
 *	here).  Caller is responsible for bumping `slot->generation` per
 *	Q2 v0.2 torn-write detection protocol — every successful write
 *	must increment generation so readers can distinguish a fresh
 *	write from a stale leftover.
 * ============================================================ */

ClusterVotingDiskIoState
cluster_voting_disk_write_slot(int fd, ClusterVotingSlot *slot)
{
	/* 512-byte aligned buffer for O_DIRECT compatibility. */
	union {
		ClusterVotingSlot slot;
		char bytes[CLUSTER_VOTING_SLOT_BYTES];
	} aligned __attribute__((aligned(512)));

	ssize_t nwritten;

	if (fd < 0)
		return CLUSTER_VOTING_DISK_IO_NOT_TRIED;
	if (slot == NULL)
		return CLUSTER_VOTING_DISK_IO_FAILED;

	/* Copy caller's slot into aligned buffer + recompute CRC. */
	aligned.slot = *slot;
	aligned.slot.crc32c = cluster_voting_disk_compute_crc32c(&aligned.slot);

	/* Reflect updated CRC back to caller for round-trip parity. */
	slot->crc32c = aligned.slot.crc32c;

	/* Hardening v0.4 P1.4: arm per-I/O timeout for pwrite + fdatasync.
	 * We arm once for the combined operation since either can hang on
	 * a stuck disk and the deadline applies to the full write attempt. */
	voting_disk_io_arm_timeout();

	nwritten = pwrite(fd, aligned.bytes, CLUSTER_VOTING_SLOT_BYTES,
					  CLUSTER_VOTING_SLOT_OFFSET(slot->node_id));
	if (nwritten != CLUSTER_VOTING_SLOT_BYTES) {
		voting_disk_io_disarm_timeout();
		return CLUSTER_VOTING_DISK_IO_FAILED;
	}

	/*
	 * fdatasync forces in-flight blocks to durable storage.  O_SYNC
	 * fallback path arguably makes this redundant, but we always call
	 * it for protocol uniformity — the cost is dwarfed by the actual
	 * pwrite latency on shared storage.
	 */
	if (fdatasync(fd) != 0) {
		voting_disk_io_disarm_timeout();
		return CLUSTER_VOTING_DISK_IO_FAILED;
	}

	voting_disk_io_disarm_timeout();
	return CLUSTER_VOTING_DISK_IO_OK;
}


/* ============================================================
 * cluster_voting_disk_format
 *
 *	initdb-time slot init.  Writes a zeroed slot (magic + version +
 *	node_id + disk_index + crc + generation = 0) for every node_id in
 *	[0, max_nodes).  After format, runtime qvotec polling can
 *	read the file without hitting EOF and can reliably distinguish
 *	"never written" (generation == 0) from "live" (generation > 0).
 * ============================================================ */

ClusterVotingDiskIoState
cluster_voting_disk_format(int fd, uint32 max_nodes, uint32 disk_index)
{
	uint32 node_id;
	ClusterVotingSlot slot;

	if (fd < 0)
		return CLUSTER_VOTING_DISK_IO_NOT_TRIED;

	for (node_id = 0; node_id < max_nodes; node_id++) {
		ClusterVotingDiskIoState rc;

		memset(&slot, 0, sizeof(slot));
		slot.magic = CLUSTER_VOTING_SLOT_MAGIC;
		slot.version = CLUSTER_VOTING_SLOT_VERSION;
		slot.node_id = node_id;
		slot.disk_index = disk_index;
		/* generation = 0 marks "never written" — runtime first write
		 * will bump to 1. */
		slot.generation = 0;
		slot.flags = 0;

		rc = cluster_voting_disk_write_slot(fd, &slot);
		if (rc != CLUSTER_VOTING_DISK_IO_OK)
			return rc;
	}

	/*
	 * spec-5.13 D2/§2.5 — the clean-leave marker region (region 2, at
	 * CLUSTER_VOTING_LEAVE_SLOT_OFFSET) is intentionally NOT seeded here.
	 * An unwritten leave-slot reads back as EOF (file not yet doubled) or
	 * zeros, both of which the clean-leave policy layer rejects as "no
	 * marker" (magic mismatch) — the correct fail-closed empty state.  The
	 * region is materialised lazily: qvotec's first leave-marker write
	 * pwrites past the voting region, extending the file, and external file
	 * provisioning honours the doubled cluster.voting_disk_size_bytes
	 * default.  Seeding here would only turn would-be-EOF reads of the
	 * voting region's tail into sparse-hole zero reads.
	 */

	return CLUSTER_VOTING_DISK_IO_OK;
}


/* ============================================================
 * cluster_voting_disk_read_leave_slot / _write_leave_slot — spec-5.13 D2.
 *
 *	Raw 512-byte slot I/O in the clean-leave marker region (region 2), at
 *	offset CLUSTER_VOTING_LEAVE_SLOT_OFFSET(node_id).  Payload-agnostic: the
 *	caller marshals the ClusterLeaveIntentMarker and owns its integrity check.
 *	Same SIGALRM per-I/O timeout discipline as the voting-slot path.
 * ============================================================ */

ClusterVotingDiskIoState
cluster_voting_disk_read_leave_slot(int fd, uint32 node_id, void *out_slot512)
{
	/* 512-byte aligned buffer for O_DIRECT compatibility. */
	char aligned[CLUSTER_VOTING_SLOT_BYTES] __attribute__((aligned(512)));
	ssize_t nread;

	if (fd < 0)
		return CLUSTER_VOTING_DISK_IO_NOT_TRIED;
	if (out_slot512 == NULL || node_id >= CLUSTER_MAX_NODES)
		return CLUSTER_VOTING_DISK_IO_FAILED;

	memset(aligned, 0, sizeof(aligned));

	voting_disk_io_arm_timeout();
	nread
		= pread(fd, aligned, CLUSTER_VOTING_SLOT_BYTES, CLUSTER_VOTING_LEAVE_SLOT_OFFSET(node_id));
	voting_disk_io_disarm_timeout();
	if (nread != CLUSTER_VOTING_SLOT_BYTES) {
		/* Short read / EOF (file not yet doubled) / EIO / SIGALRM timeout —
		 * fail-closed; the caller treats this as "no trustworthy marker". */
		return CLUSTER_VOTING_DISK_IO_FAILED;
	}

	memcpy(out_slot512, aligned, CLUSTER_VOTING_SLOT_BYTES);
	return CLUSTER_VOTING_DISK_IO_OK;
}

ClusterVotingDiskIoState
cluster_voting_disk_write_leave_slot(int fd, uint32 node_id, const void *in_slot512)
{
	/* 512-byte aligned buffer for O_DIRECT compatibility. */
	char aligned[CLUSTER_VOTING_SLOT_BYTES] __attribute__((aligned(512)));
	ssize_t nwritten;

	if (fd < 0)
		return CLUSTER_VOTING_DISK_IO_NOT_TRIED;
	if (in_slot512 == NULL || node_id >= CLUSTER_MAX_NODES)
		return CLUSTER_VOTING_DISK_IO_FAILED;

	memcpy(aligned, in_slot512, CLUSTER_VOTING_SLOT_BYTES);

	voting_disk_io_arm_timeout();
	nwritten
		= pwrite(fd, aligned, CLUSTER_VOTING_SLOT_BYTES, CLUSTER_VOTING_LEAVE_SLOT_OFFSET(node_id));
	if (nwritten != CLUSTER_VOTING_SLOT_BYTES) {
		voting_disk_io_disarm_timeout();
		return CLUSTER_VOTING_DISK_IO_FAILED;
	}
	if (fdatasync(fd) != 0) {
		voting_disk_io_disarm_timeout();
		return CLUSTER_VOTING_DISK_IO_FAILED;
	}
	voting_disk_io_disarm_timeout();
	return CLUSTER_VOTING_DISK_IO_OK;
}

/* ============================================================
 * spec-5.15 D4 — raw 512-byte join-commit-marker slot R/W (region 3), at
 * CLUSTER_VOTING_JOIN_SLOT_OFFSET(node_id).  Mirrors the leave-slot path.
 * ============================================================ */

ClusterVotingDiskIoState
cluster_voting_disk_read_join_slot(int fd, uint32 node_id, void *out_slot512)
{
	char aligned[CLUSTER_VOTING_SLOT_BYTES] __attribute__((aligned(512)));
	ssize_t nread;

	if (fd < 0)
		return CLUSTER_VOTING_DISK_IO_NOT_TRIED;
	if (out_slot512 == NULL || node_id >= CLUSTER_MAX_NODES)
		return CLUSTER_VOTING_DISK_IO_FAILED;

	memset(aligned, 0, sizeof(aligned));

	voting_disk_io_arm_timeout();
	nread = pread(fd, aligned, CLUSTER_VOTING_SLOT_BYTES, CLUSTER_VOTING_JOIN_SLOT_OFFSET(node_id));
	voting_disk_io_disarm_timeout();
	if (nread != CLUSTER_VOTING_SLOT_BYTES)
		return CLUSTER_VOTING_DISK_IO_FAILED;

	memcpy(out_slot512, aligned, CLUSTER_VOTING_SLOT_BYTES);
	return CLUSTER_VOTING_DISK_IO_OK;
}

ClusterVotingDiskIoState
cluster_voting_disk_write_join_slot(int fd, uint32 node_id, const void *in_slot512)
{
	char aligned[CLUSTER_VOTING_SLOT_BYTES] __attribute__((aligned(512)));
	ssize_t nwritten;

	if (fd < 0)
		return CLUSTER_VOTING_DISK_IO_NOT_TRIED;
	if (in_slot512 == NULL || node_id >= CLUSTER_MAX_NODES)
		return CLUSTER_VOTING_DISK_IO_FAILED;

	memcpy(aligned, in_slot512, CLUSTER_VOTING_SLOT_BYTES);

	voting_disk_io_arm_timeout();
	nwritten
		= pwrite(fd, aligned, CLUSTER_VOTING_SLOT_BYTES, CLUSTER_VOTING_JOIN_SLOT_OFFSET(node_id));
	if (nwritten != CLUSTER_VOTING_SLOT_BYTES) {
		voting_disk_io_disarm_timeout();
		return CLUSTER_VOTING_DISK_IO_FAILED;
	}
	if (fdatasync(fd) != 0) {
		voting_disk_io_disarm_timeout();
		return CLUSTER_VOTING_DISK_IO_FAILED;
	}
	voting_disk_io_disarm_timeout();
	return CLUSTER_VOTING_DISK_IO_OK;
}

/* ============================================================
 * spec-6.4 D7 — raw 512-byte apply-master lease slot R/W (region 4).
 * Mirrors the leave/join payload-agnostic slot paths.
 * ============================================================ */

ClusterVotingDiskIoState
cluster_voting_disk_read_apply_lease_slot(int fd, uint32 node_id, void *out_slot512)
{
	char aligned[CLUSTER_VOTING_SLOT_BYTES] __attribute__((aligned(512)));
	ssize_t nread;

	if (fd < 0)
		return CLUSTER_VOTING_DISK_IO_NOT_TRIED;
	if (out_slot512 == NULL || node_id >= CLUSTER_MAX_NODES)
		return CLUSTER_VOTING_DISK_IO_FAILED;

	memset(aligned, 0, sizeof(aligned));

	voting_disk_io_arm_timeout();
	nread = pread(fd, aligned, CLUSTER_VOTING_SLOT_BYTES,
				  CLUSTER_VOTING_APPLY_LEASE_SLOT_OFFSET(node_id));
	voting_disk_io_disarm_timeout();
	if (nread != CLUSTER_VOTING_SLOT_BYTES)
		return CLUSTER_VOTING_DISK_IO_FAILED;

	memcpy(out_slot512, aligned, CLUSTER_VOTING_SLOT_BYTES);
	return CLUSTER_VOTING_DISK_IO_OK;
}

ClusterVotingDiskIoState
cluster_voting_disk_write_apply_lease_slot(int fd, uint32 node_id, const void *in_slot512)
{
	char aligned[CLUSTER_VOTING_SLOT_BYTES] __attribute__((aligned(512)));
	ssize_t nwritten;

	if (fd < 0)
		return CLUSTER_VOTING_DISK_IO_NOT_TRIED;
	if (in_slot512 == NULL || node_id >= CLUSTER_MAX_NODES)
		return CLUSTER_VOTING_DISK_IO_FAILED;

	memcpy(aligned, in_slot512, CLUSTER_VOTING_SLOT_BYTES);

	voting_disk_io_arm_timeout();
	nwritten = pwrite(fd, aligned, CLUSTER_VOTING_SLOT_BYTES,
					  CLUSTER_VOTING_APPLY_LEASE_SLOT_OFFSET(node_id));
	if (nwritten != CLUSTER_VOTING_SLOT_BYTES) {
		voting_disk_io_disarm_timeout();
		return CLUSTER_VOTING_DISK_IO_FAILED;
	}
	if (fdatasync(fd) != 0) {
		voting_disk_io_disarm_timeout();
		return CLUSTER_VOTING_DISK_IO_FAILED;
	}
	voting_disk_io_disarm_timeout();
	return CLUSTER_VOTING_DISK_IO_OK;
}

#endif /* USE_PGRAC_CLUSTER */
