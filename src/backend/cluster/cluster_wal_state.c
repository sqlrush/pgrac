/*-------------------------------------------------------------------------
 *
 * cluster_wal_state.c
 *	  pgrac ClusterWalState registry I/O (spec-4.2).
 *
 *	  File layout, slot classification and the owner-only write protocol
 *	  are documented in cluster_wal_state.h.  This module implements the
 *	  backend file I/O:
 *
 *	    ensure()           postmaster startup, after the spec-4.1 claim
 *	                       validation: create-once (O_EXCL + L47 fsync
 *	                       discipline) or validate the existing header.
 *	                       Fail-closed FATAL 53RA2.
 *	    publish_active()   phase4 -> CLUSTER_PHASE_RUNNING transition:
 *	                       recovery succeeded, the node is about to
 *	                       serve -- ONLY now does the slot say ACTIVE
 *	                       (spec-4.2 v0.2 P1: never publish ACTIVE
 *	                       before recovery succeeded).  FATAL 53RA2.
 *	    publish_stopped()  clean shutdown only (postmaster exit path,
 *	                       Shutdown < ImmediateShutdown && !FatalError);
 *	                       WARNING on failure -- shutdown is never
 *	                       blocked.  Crash / immediate shutdown leaves
 *	                       the slot ACTIVE with a stale timestamp: the
 *	                       raw material for spec-4.3's crashed
 *	                       inference.
 *	    refresh_own_slot() cluster_stats main loop: best-effort liveness
 *	                       stamp + WAL watermarks.  Gated on reading the
 *	                       own slot back as OK/ACTIVE, which both keeps
 *	                       it EXEC_BACKEND-safe (no inherited statics)
 *	                       and naturally orders it after
 *	                       publish_active().
 *	    read_slot()        pread + classify (readers surface CORRUPT /
 *	                       FOREIGN as UNKNOWN -- spec-4.2 §3.3).
 *
 *	  Slots are 512B sector-shaped with CRC32C torn-write detection; no
 *	  sector atomicity is claimed.  v1 writes are buffered pwrite +
 *	  pg_fsync (the voting-disk best-effort O_DIRECT path is a later
 *	  optimisation; correctness rests on the CRC protocol either way).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_wal_state.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.2-wal-thread-metadata-catalog.md FROZEN v1.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog.h" /* GetXLogWriteRecPtr, GetWALInsertionTimeLine */
#include "cluster/cluster_guc.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_wal_state.h"
#include "cluster/cluster_wal_thread.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

/*
 * Path builders.  The registry co-exists with the per-thread layout:
 * everything here is a no-op unless cluster.wal_threads_dir is set.
 */
static bool
registry_configured(void)
{
	return cluster_wal_threads_dir != NULL && cluster_wal_threads_dir[0] != '\0'
		   && cluster_wal_thread_id() != XLP_THREAD_ID_LEGACY;
}

static void
registry_path(char *buf, size_t buflen)
{
	snprintf(buf, buflen, "%s/%s", cluster_wal_threads_dir, CLUSTER_WAL_STATE_FILENAME);
}

/*
 * read_block -- pread one 512B block at `off`.
 *
 *	Returns 1 on success, 0 when the file does not exist, -1 on any
 *	other failure (short read => errno EIO so the caller's %m stays
 *	meaningful).
 */
static int
read_block(const char *path, off_t off, void *block)
{
	int fd;
	ssize_t nread;

	fd = BasicOpenFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return (errno == ENOENT) ? 0 : -1;

	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_WAL_STATE_READ);
	nread = pg_pread(fd, block, CLUSTER_WAL_STATE_SLOT_SIZE, off);
	pgstat_report_wait_end();

	if (nread >= 0 && nread < (ssize_t)CLUSTER_WAL_STATE_SLOT_SIZE)
		errno = EIO;
	{
		int save_errno = errno;

		close(fd);
		errno = save_errno;
	}
	return (nread == (ssize_t)CLUSTER_WAL_STATE_SLOT_SIZE) ? 1 : -1;
}

/*
 * write_block -- pwrite one 512B block at `off` + pg_fsync.
 *
 *	Owner-only callers (the macro-addressed own slot, or the header
 *	during ensure()).  Returns false on failure with errno preserved.
 */
static bool
write_block(const char *path, off_t off, const void *block)
{
	int fd;
	ssize_t nwritten;

	fd = BasicOpenFile(path, O_RDWR | PG_BINARY);
	if (fd < 0)
		return false;

	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_WAL_STATE_WRITE);
	nwritten = pg_pwrite(fd, block, CLUSTER_WAL_STATE_SLOT_SIZE, off);
	if (nwritten == (ssize_t)CLUSTER_WAL_STATE_SLOT_SIZE && pg_fsync(fd) == 0) {
		pgstat_report_wait_end();
		close(fd);
		return true;
	}
	{
		int save_errno
			= (nwritten >= 0 && nwritten < (ssize_t)CLUSTER_WAL_STATE_SLOT_SIZE) ? EIO : errno;

		pgstat_report_wait_end();
		close(fd);
		errno = save_errno;
	}
	return false;
}

/*
 * cluster_wal_state_ensure
 *
 *	Create-once or validate.  Called from cluster_wal_thread_init()
 *	(postmaster, after the claim validation, before StartupXLOG).
 *	FATAL 53RA2 on corruption or I/O failure -- never a silent
 *	fallback, never an automatic rebuild of a corrupt registry.
 */
bool
cluster_wal_state_ensure(void)
{
	char path[MAXPGPATH];
	int fd;
	ClusterWalStateHeader header;
	int got;
	const char *reason = NULL;
	struct stat st;

	if (!registry_configured())
		return false;

	registry_path(path, sizeof(path));

	fd = BasicOpenFile(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);
	if (fd >= 0) {
		/* First boot of the whole registry: zero file + header. */
		char *zeros = palloc0(CLUSTER_WAL_STATE_FILE_SIZE);
		ssize_t nwritten;
		bool ok;

		cluster_wal_state_header_fill((ClusterWalStateHeader *)zeros, (int64)GetCurrentTimestamp());

		pgstat_report_wait_start(WAIT_EVENT_CLUSTER_WAL_STATE_WRITE);
		nwritten = pg_pwrite(fd, zeros, CLUSTER_WAL_STATE_FILE_SIZE, 0);
		ok = (nwritten == (ssize_t)CLUSTER_WAL_STATE_FILE_SIZE) && pg_fsync(fd) == 0;
		pgstat_report_wait_end();
		pfree(zeros);

		if (!ok) {
			int save_errno = errno;

			close(fd);
			(void)unlink(path);
			errno = save_errno;
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_WAL_STATE_IO_FAILURE),
							errmsg("could not initialise WAL state registry \"%s\": %m", path),
							errhint("Check that the shared WAL storage is writable.")));
		}
		close(fd);

		/* L47 create side: make the new dirent durable too. */
		fd = BasicOpenFile(cluster_wal_threads_dir, O_RDONLY | PG_BINARY);
		if (fd < 0)
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_WAL_STATE_IO_FAILURE),
					 errmsg("could not open WAL threads root \"%s\": %m", cluster_wal_threads_dir),
					 errhint("Check that the shared WAL storage is writable.")));
		if (pg_fsync(fd) != 0) {
			int save_errno = errno;

			close(fd);
			errno = save_errno;
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_WAL_STATE_IO_FAILURE),
					 errmsg("could not fsync WAL threads root \"%s\": %m", cluster_wal_threads_dir),
					 errhint("Check that the shared WAL storage is writable.")));
		}
		close(fd);

		ereport(LOG, (errmsg("pgrac WAL state registry created at \"%s\"", path)));
		return true;
	}
	if (errno != EEXIST)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_WAL_STATE_IO_FAILURE),
						errmsg("could not open WAL state registry \"%s\": %m", path),
						errhint("Check that the shared WAL storage is reachable.")));

	/*
	 * Exists (possibly created by a concurrent first boot): validate.
	 * The layout is a fixed 66048 bytes; a valid header glued to a
	 * truncated (or extended) slot area must not pass, and the file is
	 * never auto-resized (spec-4.2 user codereview round 2, P1).
	 */
	if (stat(path, &st) != 0)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_WAL_STATE_IO_FAILURE),
						errmsg("could not stat WAL state registry \"%s\": %m", path),
						errhint("Check that the shared WAL storage is reachable.")));
	if (st.st_size != (off_t)CLUSTER_WAL_STATE_FILE_SIZE)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_WAL_STATE_IO_FAILURE),
						errmsg("WAL state registry \"%s\" has unexpected size %lld, expected %d",
							   path, (long long)st.st_size, CLUSTER_WAL_STATE_FILE_SIZE),
						errhint("The registry is never resized in place.  After confirming the "
								"shared storage, remove the file and restart (it is rebuilt "
								"empty; slots repopulate as nodes start).")));

	got = read_block(path, 0, &header);
	if (got <= 0)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_WAL_STATE_IO_FAILURE),
						errmsg("could not read WAL state registry header \"%s\": %m", path),
						errhint("The registry is unreadable or torn.  After confirming the "
								"shared storage, remove the file and restart (it is rebuilt "
								"empty; slots repopulate as nodes start).")));
	if (!cluster_wal_state_header_validate(&header, &reason))
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_WAL_STATE_IO_FAILURE),
						errmsg("WAL state registry \"%s\" failed validation", path),
						errdetail("Header validation failed: %s.", reason ? reason : "unknown"),
						errhint("After confirming the shared storage, remove the file and restart "
								"(it is rebuilt empty; slots repopulate as nodes start).")));
	return true;
}

/*
 * fill_own_slot -- collect the live fields for this node's slot.
 */
static void
fill_own_slot(ClusterWalStateSlot *slot, uint32 state, int64 started_at)
{
	cluster_wal_state_slot_fill(slot, cluster_wal_thread_id(), cluster_node_id, state,
								(uint32)GetWALInsertionTimeLine(), started_at,
								(int64)GetCurrentTimestamp(), (uint64)GetXLogWriteRecPtr(),
								(uint64)cluster_scn_current());
}

/*
 * preserve_ext_region -- carry the spec-4.5 extension region (offset
 *	56..503) from `prev` into a freshly filled `slot` and recompute the
 *	CRC.  fill memsets the whole slot, so without this every owner write
 *	(publish/refresh/stopped) would zero checkpoint_redo_lsn /
 *	fpw_was_off / merge_recovered_lsn / refresh_interval_ms every tick
 *	(§3.3d.4, round-5 P0-2).  prev must be an OK read-back; on EMPTY/
 *	CORRUPT the region stays zero (the fill default), which classifies
 *	as "unknown" and is fail-closed at the merge gate.
 */
static void
preserve_ext_region(ClusterWalStateSlot *slot, const ClusterWalStateSlot *prev)
{
	memcpy((char *)slot + offsetof(ClusterWalStateSlot, checkpoint_redo_lsn),
		   (const char *)prev + offsetof(ClusterWalStateSlot, checkpoint_redo_lsn),
		   offsetof(ClusterWalStateSlot, crc) - offsetof(ClusterWalStateSlot, checkpoint_redo_lsn));
	slot->crc = cluster_wal_state_block_crc(slot);
}

static bool
write_own_slot(const ClusterWalStateSlot *slot)
{
	char path[MAXPGPATH];

	/* Decision-style injection (spec-4.2 D5): simulate a write failure. */
	if (cluster_injection_should_skip("cluster-wal-state-write-fail"))
		return false;

	registry_path(path, sizeof(path));
	return write_block(path, CLUSTER_WAL_STATE_SLOT_OFFSET(slot->thread_id), slot);
}

/*
 * cluster_wal_state_publish_active
 *
 *	phase4 -> CLUSTER_PHASE_RUNNING transition (cluster_startup_phase.c):
 *	recovery has succeeded and the node is about to serve SQL.  A node
 *	that dies in recovery never reaches this point, so its slot keeps
 *	the previous incarnation's content (or EMPTY on a first boot) --
 *	spec-4.2 v0.2 P1.
 */
void
cluster_wal_state_publish_active(void)
{
	ClusterWalStateSlot cur;
	ClusterWalStateSlot slot;

	if (!registry_configured())
		return;

	/*
	 * Read-before-write (spec-4.2 §3.4b, user codereview round 2 P1):
	 * a well-formed own slot claimed by ANOTHER node_id is evidence --
	 * a registry on the wrong shared root, a node_id misconfiguration,
	 * or tampering -- and must never be overwritten.  EMPTY (first
	 * boot) and CORRUPT (torn write) self-repair: the owner write below
	 * is the repair.
	 */
	if (cluster_wal_state_read_slot(cluster_wal_thread_id(), &cur) == CLUSTER_WAL_SLOT_OK
		&& cur.node_id != cluster_node_id)
		ereport(FATAL,
				(errcode(ERRCODE_CLUSTER_WAL_STATE_IO_FAILURE),
				 errmsg("WAL state registry slot %u is owned by node %d, but this node is %d",
						(unsigned)cluster_wal_thread_id(), (int)cur.node_id, cluster_node_id),
				 errdetail("The slot is valid and is left untouched."),
				 errhint("cluster.wal_threads_dir may point at the wrong shared WAL root, or "
						 "another node is configured with the same cluster.node_id.")));

	fill_own_slot(&slot, CLUSTER_WAL_SLOT_STATE_ACTIVE, (int64)GetCurrentTimestamp());
	/*
	 * Preserve the 4.5 extension region from the prior incarnation, but
	 * CLEAR merge_recovered_lsn: reaching RUNNING means this node has
	 * finished its own recovery, so any coordinator-set authority bound
	 * is spent (§3.3c).  checkpoint_redo_lsn / fpw_was_off survive.
	 */
	if (cluster_wal_state_read_slot(cluster_wal_thread_id(), &cur) == CLUSTER_WAL_SLOT_OK)
		preserve_ext_region(&slot, &cur);
	slot.merge_recovered_lsn = 0;
	slot.crc = cluster_wal_state_block_crc(&slot);
	if (!write_own_slot(&slot))
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_WAL_STATE_IO_FAILURE),
						errmsg("could not publish ACTIVE to the WAL state registry: %m"),
						errhint("Check that the shared WAL storage is writable.")));
	ereport(LOG, (errmsg("pgrac WAL thread %u published ACTIVE in the WAL state registry",
						 (unsigned)slot.thread_id)));
}

/*
 * cluster_wal_state_publish_stopped
 *
 *	Clean shutdown only (the postmaster exit path gates on
 *	Shutdown < ImmediateShutdown && !FatalError).  Failure must never
 *	block a shutdown: WARNING + carry on.  started_at is preserved
 *	from the slot on disk (same incarnation).
 */
void
cluster_wal_state_publish_stopped(void)
{
	ClusterWalStateSlot cur;
	ClusterWalStateSlot slot;
	ClusterWalSlotVerdict v;
	int64 started_at;

	if (!registry_configured())
		return;

	v = cluster_wal_state_read_slot(cluster_wal_thread_id(), &cur);

	/*
	 * Same foreign-owner gate as publish_active(), demoted to WARNING:
	 * shutdown is never blocked, but foreign evidence is never erased
	 * either (spec-4.2 user codereview round 2 P1).
	 */
	if (v == CLUSTER_WAL_SLOT_OK && cur.node_id != cluster_node_id) {
		ereport(WARNING,
				(errcode(ERRCODE_CLUSTER_WAL_STATE_IO_FAILURE),
				 errmsg("not publishing STOPPED: WAL state registry slot %u is owned by node %d, "
						"but this node is %d",
						(unsigned)cluster_wal_thread_id(), (int)cur.node_id, cluster_node_id),
				 errhint("The foreign slot is left untouched as evidence.")));
		return;
	}

	started_at = (v == CLUSTER_WAL_SLOT_OK) ? cur.started_at : (int64)GetCurrentTimestamp();

	fill_own_slot(&slot, CLUSTER_WAL_SLOT_STATE_STOPPED, started_at);
	if (v == CLUSTER_WAL_SLOT_OK)
		preserve_ext_region(&slot, &cur);
	if (!write_own_slot(&slot))
		ereport(WARNING, (errcode(ERRCODE_CLUSTER_WAL_STATE_IO_FAILURE),
						  errmsg("could not publish STOPPED to the WAL state registry: %m"),
						  errhint("The slot stays ACTIVE; spec-4.3 readers treat it via the "
								  "staleness inference.")));
}

/*
 * cluster_wal_state_refresh_own_slot
 *
 *	cluster_stats main-loop tick.  Best-effort: gated on reading the
 *	own slot back as OK + ACTIVE (EMPTY before publish_active, or any
 *	classification failure, skips quietly -- ordering and
 *	EXEC_BACKEND safety in one check).  Failures LOG once and bump the
 *	shared counter; they never escalate (spec-4.2 §3.4).
 */
void
cluster_wal_state_refresh_own_slot(void)
{
	ClusterWalStateSlot cur;
	ClusterWalStateSlot slot;

	if (!registry_configured())
		return;

	if (cluster_wal_state_read_slot(cluster_wal_thread_id(), &cur) != CLUSTER_WAL_SLOT_OK
		|| cur.state != CLUSTER_WAL_SLOT_STATE_ACTIVE || cur.node_id != cluster_node_id)
		return;

	fill_own_slot(&slot, CLUSTER_WAL_SLOT_STATE_ACTIVE, cur.started_at);
	preserve_ext_region(&slot, &cur);
	if (!write_own_slot(&slot)) {
		if (cluster_wal_thread_refresh_fail_fetch_add() == 0)
			ereport(LOG, (errcode(ERRCODE_CLUSTER_WAL_STATE_IO_FAILURE),
						  errmsg("could not refresh the WAL state registry slot: %m"),
						  errhint("Further refresh failures are counted, not logged "
								  "(cluster.wal_thread.wal_state_refresh_fail_count).")));
	}
}

/*
 * own_slot_modify -- read the own slot OK, apply `mutate`, write back.
 *	Read-modify-preserve: only the field(s) mutate touches change; the
 *	rest of the slot (including the other extension fields) is carried
 *	verbatim.  Best-effort: returns false (caller WARNs) when the slot
 *	is not a clean own-OK read or the write fails.
 */
static bool
own_slot_modify(void (*mutate)(ClusterWalStateSlot *, uint64), uint64 arg)
{
	ClusterWalStateSlot slot;

	if (!registry_configured())
		return false;
	if (cluster_wal_state_read_slot(cluster_wal_thread_id(), &slot) != CLUSTER_WAL_SLOT_OK
		|| slot.node_id != cluster_node_id)
		return false;
	mutate(&slot, arg);
	slot.crc = cluster_wal_state_block_crc(&slot);
	return write_own_slot(&slot);
}

static void
mutate_checkpoint_redo(ClusterWalStateSlot *s, uint64 v)
{
	XLogRecPtr write_lsn;

	s->checkpoint_redo_lsn = v;

	/*
	 * CreateCheckPoint publishes checkpoint_redo_lsn synchronously, while the
	 * normal highest_lsn watermark is refreshed later by cluster_stats.  Do not
	 * expose a transient slot with new redo but stale highest_lsn: online thread
	 * recovery treats highest_lsn <= checkpoint_redo_lsn as fail-closed.
	 */
	write_lsn = GetXLogWriteRecPtr();
	if (s->highest_lsn < (uint64)write_lsn)
		s->highest_lsn = (uint64)write_lsn;
}

static void
mutate_fpw_off(ClusterWalStateSlot *s, uint64 v)
{
	(void)v;
	s->fpw_was_off = 1;
}

void
cluster_wal_state_publish_checkpoint_redo(uint64 redo_lsn)
{
	if (!own_slot_modify(mutate_checkpoint_redo, redo_lsn) && registry_configured())
		ereport(WARNING, (errcode(ERRCODE_CLUSTER_WAL_STATE_IO_FAILURE),
						  errmsg("could not publish checkpoint redo to the WAL state registry: %m"),
						  errhint("Merged recovery may fail-close (53RA3) until the next "
								  "successful checkpoint publishes a start point.")));
}

void
cluster_wal_state_mark_fpw_off(void)
{
	/*
	 * Sticky: set fpw_was_off=1 and never auto-clear.  Persisted on the
	 * authoritative on->off transition BEFORE off-mode WAL is produced
	 * (§3.3d.3); a clean own slot that already has it set is left as is.
	 */
	if (!own_slot_modify(mutate_fpw_off, 0) && registry_configured())
		ereport(WARNING,
				(errcode(ERRCODE_CLUSTER_WAL_STATE_IO_FAILURE),
				 errmsg("could not record full_page_writes=off in the WAL state registry: %m"),
				 errhint("Merged recovery treats an unrecorded fpw history "
						 "conservatively.")));
}

/*
 * cluster_wal_state_publish_merge_recovered -- §3.3c authority write.
 *	Cross-owner exception: the merge coordinator records recovered_lsn
 *	in a CRASHED peer's slot (the peer is down, so there is no racing
 *	owner write).  Read-modify-preserve so the rest of the peer's slot
 *	is untouched.
 */
void
cluster_wal_state_publish_merge_recovered(uint16 thread_id, uint64 recovered_lsn)
{
	char path[MAXPGPATH];
	ClusterWalStateSlot slot;

	if (!registry_configured())
		return;
	if (cluster_wal_state_read_slot(thread_id, &slot) != CLUSTER_WAL_SLOT_OK)
		return;
	slot.merge_recovered_lsn = recovered_lsn;
	slot.crc = cluster_wal_state_block_crc(&slot);
	registry_path(path, sizeof(path));
	if (!write_block(path, CLUSTER_WAL_STATE_SLOT_OFFSET(thread_id), &slot))
		ereport(WARNING, (errcode(ERRCODE_CLUSTER_WAL_STATE_IO_FAILURE),
						  errmsg("could not record merged-recovery progress for thread %u: %m",
								 (unsigned)thread_id)));
}

/*
 * cluster_wal_state_read_slot
 *
 *	Reader-mode pread + classify (expect_node = -1: any owner).
 *	Callers MUST surface CORRUPT/FOREIGN as UNKNOWN.
 */
ClusterWalSlotVerdict
cluster_wal_state_read_slot(uint16 thread_id, ClusterWalStateSlot *slot_out)
{
	char path[MAXPGPATH];
	int got;

	Assert(thread_id >= XLP_THREAD_ID_FIRST_REAL && thread_id <= CLUSTER_WAL_THREAD_MAX);
	if (thread_id < XLP_THREAD_ID_FIRST_REAL || thread_id > CLUSTER_WAL_THREAD_MAX
		|| !registry_configured())
		return CLUSTER_WAL_SLOT_EMPTY;

	registry_path(path, sizeof(path));
	got = read_block(path, CLUSTER_WAL_STATE_SLOT_OFFSET(thread_id), slot_out);
	if (got == 0)
		return CLUSTER_WAL_SLOT_EMPTY;
	if (got < 0)
		return CLUSTER_WAL_SLOT_CORRUPT;

	return cluster_wal_state_slot_classify(slot_out, thread_id, -1, NULL);
}

/*
 * Dump accessors (live reads; no shmem state of their own).
 */
bool
cluster_wal_state_registry_ready(void)
{
	char path[MAXPGPATH];
	ClusterWalStateHeader header;
	struct stat st;

	if (!registry_configured())
		return false;
	registry_path(path, sizeof(path));
	/* Same fixed-size discipline as ensure(): reader surface, no FATAL. */
	if (stat(path, &st) != 0 || st.st_size != (off_t)CLUSTER_WAL_STATE_FILE_SIZE)
		return false;
	if (read_block(path, 0, &header) != 1)
		return false;
	return cluster_wal_state_header_validate(&header, NULL);
}

uint64
cluster_wal_state_refresh_fail_count(void)
{
	return cluster_wal_thread_refresh_fail_read();
}

#else /* !USE_PGRAC_CLUSTER */

/* Disable-cluster build: this file compiles to nothing. */

#endif /* USE_PGRAC_CLUSTER */
