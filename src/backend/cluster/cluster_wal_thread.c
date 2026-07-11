/*-------------------------------------------------------------------------
 *
 * cluster_wal_thread.c
 *	  pgrac per-thread WAL routing: thread identity, claim file, startup
 *	  validation (spec-4.1).
 *
 *	  This module activates the spec-1.19 xlp_thread_id placeholder.
 *	  The write hot path gets exactly one entry point,
 *	  cluster_wal_thread_stamp(), called by AdvanceXLInsertBuffer per
 *	  new WAL page; everything else runs once at postmaster startup.
 *
 *	  Layout contract (spec-4.1 Q1-A): the WAL stream is relocated to
 *	  <cluster.wal_threads_dir>/thread_<id>/ by bootstrap tooling
 *	  (pgrac-init --wal-threads-dir, which passes initdb -X), NOT by
 *	  engine-side path rewriting.  The engine validates the routing at
 *	  startup fail-closed (53RA0 / 53RA1) and never falls back to the
 *	  flat layout silently.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_wal_thread.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.1-per-thread-wal-routing.md FROZEN v1.0
 *	  Design: docs/wal-record-format-design.md §4.2, feature-034, AD-009
 *
 *	  Shmem region "pgrac wal thread" exists so the dump accessors work
 *	  under EXEC_BACKEND too (children do not inherit postmaster globals
 *	  there); it carries one counter and a small identity mirror written
 *	  once by the postmaster before any child is forked.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog_internal.h" /* XLOGDIR */
#include "cluster/cluster_conf.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_hw.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_wal_state.h" /* spec-4.2 ensure() */
#include "cluster/cluster_wal_thread.h"
#include "miscadmin.h" /* IsUnderPostmaster, DataDir */
#include "port/atomics.h"
#include "storage/fd.h" /* BasicOpenFile, pg_fsync */
#include "storage/shmem.h"
#include "utils/timestamp.h" /* GetCurrentTimestamp */
#include "utils/wait_event.h"

/* ----------------------------------------------------------------
 * Shmem region (L206 five-step registration; "pgrac wal thread")
 * ----------------------------------------------------------------
 */
typedef struct ClusterWalThreadShmemData {
	pg_atomic_uint64 page_stamp_count; /* real-id stamps since startup */

	/*
	 * Identity mirror for the cluster_debug dump accessors.  Written
	 * exactly once by cluster_wal_thread_init() in the postmaster
	 * (before any child exists), read-only afterwards -- so plain
	 * fields are race-free by construction.
	 */
	uint16 thread_id;	  /* cluster_wal_thread_id() at startup */
	uint8 dir_configured; /* cluster.wal_threads_dir != '' */
	uint8 dir_validated;  /* routing validation passed */
	uint8 claim_created;  /* this boot created the claim file */
	uint8 _pad[3];

	/* spec-4.2 D5: WAL-state registry refresh-failure counter (bumped by
	 * cluster_stats on best-effort refresh failures, read by the dump
	 * SRF in any backend). */
	pg_atomic_uint64 wal_state_refresh_fail_count;
	char _reserved[8]; /* remaining headroom */
} ClusterWalThreadShmemData;

static ClusterWalThreadShmemData *cluster_wal_thread_shmem = NULL;

static Size
cluster_wal_thread_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterWalThreadShmemData));
}

static void
cluster_wal_thread_shmem_init(void)
{
	bool found;

	cluster_wal_thread_shmem = (ClusterWalThreadShmemData *)ShmemInitStruct(
		"pgrac wal thread", cluster_wal_thread_shmem_size(), &found);
	if (!found) {
		pg_atomic_init_u64(&cluster_wal_thread_shmem->page_stamp_count, 0);
		cluster_wal_thread_shmem->thread_id = XLP_THREAD_ID_LEGACY;
		cluster_wal_thread_shmem->dir_configured = 0;
		cluster_wal_thread_shmem->dir_validated = 0;
		cluster_wal_thread_shmem->claim_created = 0;
		memset(cluster_wal_thread_shmem->_pad, 0, sizeof(cluster_wal_thread_shmem->_pad));
		pg_atomic_init_u64(&cluster_wal_thread_shmem->wal_state_refresh_fail_count, 0);
		memset(cluster_wal_thread_shmem->_reserved, 0, sizeof(cluster_wal_thread_shmem->_reserved));
	}
}

static const ClusterShmemRegion cluster_wal_thread_region = {
	.name = "pgrac wal thread",
	.size_fn = cluster_wal_thread_shmem_size,
	.init_fn = cluster_wal_thread_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "cluster_wal_thread",
	.reserved_flags = 0,
};

void
cluster_wal_thread_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_wal_thread_region);
}

/* ----------------------------------------------------------------
 * Identity + stamp (the only hot-path entry points)
 * ----------------------------------------------------------------
 */

uint16
cluster_wal_thread_id(void)
{
	return cluster_wal_thread_id_for(cluster_enabled, cluster_node_id);
}

uint16
cluster_wal_thread_stamp(void)
{
	uint16 tid = cluster_wal_thread_id();

	/*
	 * nofail + critical-section-safe: one conditional atomic add, no
	 * locks, no elog.  Tolerate an unattached region (L19) by skipping
	 * the counter, never the stamp value.
	 */
	if (tid != XLP_THREAD_ID_LEGACY && cluster_wal_thread_shmem != NULL)
		pg_atomic_fetch_add_u64(&cluster_wal_thread_shmem->page_stamp_count, 1);
	return tid;
}

/* ----------------------------------------------------------------
 * Dump accessors (cluster_debug.c category 'wal_thread')
 * ----------------------------------------------------------------
 */

uint64
cluster_wal_thread_page_stamp_count(void)
{
	if (cluster_wal_thread_shmem == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_wal_thread_shmem->page_stamp_count);
}

uint16
cluster_wal_thread_dump_thread_id(void)
{
	if (cluster_wal_thread_shmem == NULL)
		return XLP_THREAD_ID_LEGACY;
	return cluster_wal_thread_shmem->thread_id;
}

bool
cluster_wal_thread_dir_configured(void)
{
	return cluster_wal_thread_shmem != NULL && cluster_wal_thread_shmem->dir_configured != 0;
}

bool
cluster_wal_thread_dir_validated(void)
{
	return cluster_wal_thread_shmem != NULL && cluster_wal_thread_shmem->dir_validated != 0;
}

bool
cluster_wal_thread_claim_created(void)
{
	return cluster_wal_thread_shmem != NULL && cluster_wal_thread_shmem->claim_created != 0;
}

/* spec-4.2: refresh-failure counter (L19-tolerant on both sides). */
uint64
cluster_wal_thread_refresh_fail_fetch_add(void)
{
	if (cluster_wal_thread_shmem == NULL)
		return 1; /* no shmem -> pretend non-first so callers stay quiet */
	return pg_atomic_fetch_add_u64(&cluster_wal_thread_shmem->wal_state_refresh_fail_count, 1);
}

uint64
cluster_wal_thread_refresh_fail_read(void)
{
	if (cluster_wal_thread_shmem == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_wal_thread_shmem->wal_state_refresh_fail_count);
}

/* ----------------------------------------------------------------
 * Startup validation
 * ----------------------------------------------------------------
 */

/*
 * same_directory -- do two paths name the same directory object?
 *
 *	st_dev/st_ino identity covers both symlink and bind-mount
 *	relocation (spec-4.1 §2.4).  stat() follows symlinks, which is
 *	exactly what we want: $PGDATA/pg_wal is typically a symlink to
 *	<wal_threads_dir>/thread_<id>.
 *
 *	WIN32 note: st_ino is not meaningful there; fall back to a
 *	canonicalized path comparison.  pgrac CI covers Linux + macOS;
 *	the fallback keeps the build honest on Windows without claiming
 *	tested support.
 */
static bool
same_directory(const char *a, const char *b)
{
#ifndef WIN32
	struct stat sa;
	struct stat sb;

	if (stat(a, &sa) != 0 || stat(b, &sb) != 0)
		return false;
	if (!S_ISDIR(sa.st_mode) || !S_ISDIR(sb.st_mode))
		return false;
	return sa.st_dev == sb.st_dev && sa.st_ino == sb.st_ino;
#else
	char ca[MAXPGPATH];
	char cb[MAXPGPATH];

	strlcpy(ca, a, sizeof(ca));
	strlcpy(cb, b, sizeof(cb));
	canonicalize_path(ca);
	canonicalize_path(cb);
	return strcmp(ca, cb) == 0;
#endif
}

/*
 * claim_read -- read the claim file into *claim.
 *
 *	Returns 1 on success, 0 when the file does not exist (first boot),
 *	and -1 on any other failure (open error, short read) with *claim
 *	zeroed; the caller turns -1 into FATAL 53RA1 (a half-written or
 *	unreadable claim is fail-closed, never auto-rebuilt).
 */
static int
claim_read(const char *path, ClusterWalThreadClaim *claim)
{
	int fd;
	ssize_t nread;

	memset(claim, 0, sizeof(*claim));

	fd = BasicOpenFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0) {
		if (errno == ENOENT)
			return 0;
		return -1;
	}

	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_WAL_THREAD_CLAIM_READ);
	nread = read(fd, claim, sizeof(*claim));
	pgstat_report_wait_end();

	/*
	 * Keep errno meaningful for the caller's %m: a short read (torn
	 * claim) sets none, and close() may clobber whatever read() set.
	 */
	if (nread >= 0 && nread < (ssize_t)sizeof(*claim))
		errno = EIO;
	{
		int save_errno = errno;

		close(fd);
		errno = save_errno;
	}

	return (nread == (ssize_t)sizeof(*claim)) ? 1 : -1;
}

/*
 * claim_create -- first-boot claim creation (write-once semantics).
 *
 *	O_EXCL create + full write + pg_fsync(file) + pg_fsync(parent dir)
 *	(L47 create-side discipline: the dirent must be durable too).
 *	Returns false on any failure; the caller raises FATAL 53RA1.
 */
static bool
claim_create(const char *path, const char *dir, const ClusterWalThreadClaim *claim)
{
	int fd;
	ssize_t nwritten;

	/*
	 * Decision-style injection (L101): when armed, simulate a create
	 * failure so cluster_tap can exercise the 53RA1 path without real
	 * storage faults.
	 */
	if (cluster_injection_should_skip("cluster-wal-thread-claim-create-fail"))
		return false;

	fd = BasicOpenFile(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);
	if (fd < 0)
		return false;

	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_WAL_THREAD_CLAIM_WRITE);
	nwritten = write(fd, claim, sizeof(*claim));
	if (nwritten == (ssize_t)sizeof(*claim) && pg_fsync(fd) == 0) {
		pgstat_report_wait_end();
		close(fd);
	} else {
		/*
		 * Preserve the write()/pg_fsync() errno across close()/unlink()
		 * so the caller's FATAL %m names the real failure; a short
		 * write sets none, so supply EIO.
		 */
		int save_errno = (nwritten >= 0 && nwritten < (ssize_t)sizeof(*claim)) ? EIO : errno;

		pgstat_report_wait_end();
		close(fd);
		(void)unlink(path); /* best-effort: do not leave a torn claim */
		errno = save_errno;
		return false;
	}

	/* fsync the parent directory so the new dirent is durable */
	fd = BasicOpenFile(dir, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return false;
	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_WAL_THREAD_CLAIM_WRITE);
	if (pg_fsync(fd) != 0) {
		int save_errno = errno;

		pgstat_report_wait_end();
		close(fd);
		errno = save_errno;
		return false;
	}
	pgstat_report_wait_end();
	close(fd);
	return true;
}

void
cluster_wal_thread_init(void)
{
	uint16 tid;
	char dirname[64];
	char thread_dir[MAXPGPATH];
	char pg_wal_path[MAXPGPATH];
	char claim_path[MAXPGPATH];
	ClusterWalThreadClaim claim;
	int got;
	bool dir_set;

	/*
	 * Postmaster-once (CLAUDE.md rule 16): EXEC_BACKEND children re-run
	 * CreateSharedMemoryAndSemaphores; everything below must happen
	 * exactly once, in the postmaster (or a standalone backend), before
	 * StartupXLOG reads any WAL.
	 */
	if (IsUnderPostmaster)
		return;

	tid = cluster_wal_thread_id();
	dir_set = (cluster_wal_threads_dir != NULL && cluster_wal_threads_dir[0] != '\0');

	/* Mirror identity for the dump accessors (EXEC_BACKEND-safe). */
	if (cluster_wal_thread_shmem != NULL) {
		cluster_wal_thread_shmem->thread_id = tid;
		cluster_wal_thread_shmem->dir_configured = dir_set ? 1 : 0;
	}

	if (!dir_set) {
		if (cluster_shared_catalog && cluster_conf_has_peers())
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("cluster.shared_catalog requires cluster.wal_threads_dir in a "
							"multi-node cluster"),
					 errhint("Set cluster.wal_threads_dir to the shared per-thread WAL root and "
							 "initialise each node with pgrac-init --wal-threads-dir.")));
		if (!cluster_hw_remaster_recoverable())
			ereport(WARNING,
					(errmsg("cluster HW remaster is not recoverable because "
							"cluster.wal_threads_dir is unset"),
					 errhint("A dead node's adopted HW authority cannot be rebuilt from per-thread "
							 "WAL until cluster.wal_threads_dir is configured and the node is "
							 "restarted.")));
		return; /* flat layout: identity stamping only (Q3-A) */
	}

	/*
	 * Configuration coherence, fail-closed (L58: enumerate the
	 * syntactically-valid-but-semantically-empty boundaries too).
	 */
	if (!cluster_enabled)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_WAL_THREAD_ROUTING_MISMATCH),
						errmsg("cluster.wal_threads_dir is set but cluster.enabled is off"),
						errhint("Either enable the cluster or unset cluster.wal_threads_dir.")));
	if (cluster_node_id < 0)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_WAL_THREAD_ROUTING_MISMATCH),
						errmsg("cluster.wal_threads_dir is set but cluster.node_id is not"),
						errhint("Set cluster.node_id to this node's identifier (0..127).")));

	CLUSTER_INJECTION_POINT("cluster-wal-thread-validate-pre");

	Assert(tid >= XLP_THREAD_ID_FIRST_REAL && tid <= CLUSTER_WAL_THREAD_MAX);
	cluster_wal_thread_dir_name(tid, dirname, sizeof(dirname));
	snprintf(thread_dir, sizeof(thread_dir), "%s/%s", cluster_wal_threads_dir, dirname);
	snprintf(pg_wal_path, sizeof(pg_wal_path), "%s/%s", DataDir, XLOGDIR);

	/*
	 * The thread directory must pre-exist (bootstrap creates it via
	 * initdb -X).  Never mkdir here: auto-creating on a typo would
	 * silently open a brand-new WAL stream (spec-4.1 t/243 L6).
	 */
	{
		struct stat st;

		if (stat(thread_dir, &st) != 0 || !S_ISDIR(st.st_mode))
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_WAL_THREAD_ROUTING_MISMATCH),
					 errmsg("WAL thread directory \"%s\" does not exist", thread_dir),
					 errdetail("cluster.wal_threads_dir is \"%s\" and this node's thread id is %u.",
							   cluster_wal_threads_dir, (unsigned)tid),
					 errhint("Initialise the node with pgrac-init --wal-threads-dir, or create "
							 "and relocate the WAL directory manually (see the pgrac manual).")));
	}

	if (!same_directory(pg_wal_path, thread_dir))
		ereport(FATAL,
				(errcode(ERRCODE_CLUSTER_WAL_THREAD_ROUTING_MISMATCH),
				 errmsg("\"%s\" does not resolve to this node's WAL thread directory", pg_wal_path),
				 errdetail("Expected %s to be the same directory as \"%s\" (thread %u for "
						   "cluster.node_id %d).",
						   XLOGDIR, thread_dir, (unsigned)tid, cluster_node_id),
				 errhint("Re-link " XLOGDIR " to the thread directory, or fix "
						 "cluster.wal_threads_dir / cluster.node_id.")));

	snprintf(claim_path, sizeof(claim_path), "%s/%s", thread_dir,
			 CLUSTER_WAL_THREAD_CLAIM_FILENAME);

	got = claim_read(claim_path, &claim);
	if (got == 0) {
		/* First validated boot of this thread directory: claim it. */
		cluster_wal_thread_claim_fill(&claim, tid, cluster_node_id, (int64)GetCurrentTimestamp());
		if (!claim_create(claim_path, thread_dir, &claim))
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_WAL_THREAD_CLAIM_CONFLICT),
					 errmsg("could not create WAL thread claim file \"%s\": %m", claim_path),
					 errhint("Check that the shared WAL storage is writable by this node.")));
		if (cluster_wal_thread_shmem != NULL)
			cluster_wal_thread_shmem->claim_created = 1;
		ereport(LOG, (errmsg("pgrac WAL thread %u claimed by node %d (\"%s\")", (unsigned)tid,
							 cluster_node_id, claim_path)));
	} else if (got < 0) {
		ereport(FATAL,
				(errcode(ERRCODE_CLUSTER_WAL_THREAD_CLAIM_CONFLICT),
				 errmsg("could not read WAL thread claim file \"%s\": %m", claim_path),
				 errhint("The claim file is unreadable or torn.  After confirming no other node "
						 "owns this thread directory, remove the file and restart.")));
	} else {
		const char *reason = NULL;

		if (!cluster_wal_thread_claim_validate(&claim, tid, cluster_node_id, &reason))
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_WAL_THREAD_CLAIM_CONFLICT),
							errmsg("WAL thread directory \"%s\" is claimed by node %d (thread %u)",
								   thread_dir, claim.node_id, (unsigned)claim.thread_id),
							errdetail("Claim validation failed: %s.", reason ? reason : "unknown"),
							errhint("Each WAL thread directory belongs to exactly one node.  After "
									"confirming ownership, remove \"%s\" and restart (it is never "
									"rebuilt automatically).",
									claim_path)));
	}

	if (cluster_wal_thread_shmem != NULL)
		cluster_wal_thread_shmem->dir_validated = 1;

	ereport(LOG, (errmsg("pgrac WAL thread routing validated: thread %u at \"%s\"", (unsigned)tid,
						 thread_dir)));

	/*
	 * spec-4.2: bring up the ClusterWalState registry (create-once or
	 * validate; FATAL 53RA2 inside on corruption/IO).  The own slot is
	 * NOT touched here: ACTIVE is published only after recovery has
	 * succeeded, at the phase4 -> CLUSTER_PHASE_RUNNING transition
	 * (spec-4.2 v0.2 P1).
	 */
	CLUSTER_INJECTION_POINT("cluster-wal-state-ensure-pre");
	(void)cluster_wal_state_ensure();
}

#else /* !USE_PGRAC_CLUSTER */

/*
 * Disable-cluster build: this file compiles to nothing.  xlog.c gates
 * its calls under USE_PGRAC_CLUSTER and stamps the legacy constants
 * directly (spec-4.1 §3.1: byte-identical stream).
 */

#endif /* USE_PGRAC_CLUSTER */
