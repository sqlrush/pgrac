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
 *	                       validation: read-only full-registry validation.
 *	                       Offline initdb finalization is the sole creator.
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

#include "access/xlog.h"		 /* GetXLogWriteRecPtr, GetWALInsertionTimeLine */
#include "access/xlogrecovery.h" /* GetXLogReplayRecPtr (spec-6.4 standby stop) */
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_semantic_activation.h" /* source-close writer gate */
#include "cluster/cluster_wal_state.h"
#include "cluster/cluster_wal_thread.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/proc.h"
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
 * RF A1 CF_VERIFIED_X predicate.  The file itself is validated after the
 * coordinated hold; this predicate only proves the existing coordination
 * authority needed before a formed-registry mutation may begin.
 */
static bool
wal_state_cf_prerequisites_ready(void)
{
	ClusterResId cf_resid;

	if (!cluster_controlfile_shared_authority || !cluster_lms_enabled || !cluster_lms_is_ready()
		|| MyProc == NULL)
		return false;
	cluster_cf_resid_encode(&cf_resid);
	return cluster_grd_lookup_master(&cf_resid) >= 0;
}

/*
 * cluster_wal_state_update_own -- RF A1 sole formed-registry RMW.
 *
 * ACQUIRE_X owns the verified CF(X) hold and releases it on every exit.
 * BORROW_X proves the caller already holds that same verified lock and never
 * re-enters or releases it.  All file decisions use one fresh header and own
 * slot image read under that hold.  There is one write attempt and never a
 * compensating overwrite, truncate, unlink or rename.
 */
ClusterWalStateUpdateResult
cluster_wal_state_update_own(const ClusterWalStateUpdate *update, ClusterWalStateCfMode cf_mode,
							 ClusterWalStateSlot *published_slot)
{
	ClusterWalStateHeader header;
	ClusterWalStateSlot fresh_before;
	ClusterWalStateSlot expected_after;
	ClusterWalStateSlot fresh_observed;
	ClusterWalStateUpdateResult result = CLUSTER_WAL_STATE_UPDATE_IO_ERROR;
	char path[MAXPGPATH];
	uint16 thread_id;
	int fd = -1;
	bool acquired_here = false;
	struct stat st;
	ssize_t nbytes;

	if (update == NULL
		|| (cf_mode != CLUSTER_WAL_STATE_CF_ACQUIRE_X && cf_mode != CLUSTER_WAL_STATE_CF_BORROW_X))
		return CLUSTER_WAL_STATE_UPDATE_INVALID;
	if (!cluster_enabled || !registry_configured())
		return CLUSTER_WAL_STATE_UPDATE_DISABLED;
	if (!wal_state_cf_prerequisites_ready())
		return CLUSTER_WAL_STATE_UPDATE_CF_UNAVAILABLE;
	/* RF-ROOT P9 verification (implementation): the bit22 first-open round's BARRIER
	 * freezes the source — a closed source refuses new writers (NOOP:
	 * the caller treats it as a benign no-op, never a failure). */
	if (!cluster_r4_bit22_source_writer_enter())
		return CLUSTER_WAL_STATE_UPDATE_NOOP;

	if (cf_mode == CLUSTER_WAL_STATE_CF_ACQUIRE_X) {
		/* Do not trip cluster_cf_lock's deliberate non-reentrant Assert. */
		if (cluster_cf_held(ExclusiveLock) || !cluster_cf_lock(ExclusiveLock))
			return CLUSTER_WAL_STATE_UPDATE_CF_UNAVAILABLE;
		acquired_here = true;
	} else if (!cluster_cf_held(ExclusiveLock))
		return CLUSTER_WAL_STATE_UPDATE_CF_UNAVAILABLE;

	/* Re-prove the derived predicates together with the successful held state. */
	if (!cluster_cf_held(ExclusiveLock) || !wal_state_cf_prerequisites_ready()) {
		result = CLUSTER_WAL_STATE_UPDATE_CF_UNAVAILABLE;
		goto out;
	}

	thread_id = cluster_wal_thread_id();
	if (thread_id < XLP_THREAD_ID_FIRST_REAL || thread_id > CLUSTER_WAL_THREAD_MAX) {
		result = CLUSTER_WAL_STATE_UPDATE_INVALID;
		goto out;
	}

	registry_path(path, sizeof(path));
	fd = BasicOpenFile(path, O_RDWR | PG_BINARY);
	if (fd < 0) {
		result = CLUSTER_WAL_STATE_UPDATE_IO_ERROR;
		goto out;
	}
	if (fstat(fd, &st) != 0) {
		result = CLUSTER_WAL_STATE_UPDATE_IO_ERROR;
		goto out;
	}
	if (st.st_size != (off_t)CLUSTER_WAL_STATE_FILE_SIZE) {
		result = CLUSTER_WAL_STATE_UPDATE_CORRUPT;
		goto out;
	}

	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_WAL_STATE_READ);
	nbytes = pg_pread(fd, &header, sizeof(header), 0);
	pgstat_report_wait_end();
	if (nbytes != (ssize_t)sizeof(header)) {
		if (nbytes >= 0)
			errno = EIO;
		result = CLUSTER_WAL_STATE_UPDATE_IO_ERROR;
		goto out;
	}
	if (!cluster_wal_state_header_validate(&header, NULL)) {
		result = CLUSTER_WAL_STATE_UPDATE_CORRUPT;
		goto out;
	}

	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_WAL_STATE_READ);
	nbytes = pg_pread(fd, &fresh_before, sizeof(fresh_before),
					  CLUSTER_WAL_STATE_SLOT_OFFSET(thread_id));
	pgstat_report_wait_end();
	if (nbytes != (ssize_t)sizeof(fresh_before)) {
		if (nbytes >= 0)
			errno = EIO;
		result = CLUSTER_WAL_STATE_UPDATE_IO_ERROR;
		goto out;
	}

	result = cluster_wal_state_slot_prepare_update(&fresh_before, thread_id, cluster_node_id,
												   update, &expected_after);
	if (result == CLUSTER_WAL_STATE_UPDATE_NOOP) {
		if (published_slot != NULL)
			memcpy(published_slot, &fresh_before, sizeof(*published_slot));
		goto out;
	}
	if (result != CLUSTER_WAL_STATE_UPDATE_OK)
		goto out;

	if (cluster_injection_should_skip("cluster-wal-state-write-fail")) {
		errno = EIO;
		result = CLUSTER_WAL_STATE_UPDATE_IO_ERROR;
		goto out;
	}

	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_WAL_STATE_WRITE);
	nbytes = pg_pwrite(fd, &expected_after, sizeof(expected_after),
					   CLUSTER_WAL_STATE_SLOT_OFFSET(thread_id));
	if (nbytes != (ssize_t)sizeof(expected_after)) {
		if (nbytes >= 0)
			errno = EIO;
		pgstat_report_wait_end();
		result = CLUSTER_WAL_STATE_UPDATE_IO_ERROR;
		goto out;
	}
	if (pg_fsync(fd) != 0) {
		pgstat_report_wait_end();
		result = CLUSTER_WAL_STATE_UPDATE_IO_ERROR;
		goto out;
	}
	pgstat_report_wait_end();

	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_WAL_STATE_READ);
	nbytes = pg_pread(fd, &fresh_observed, sizeof(fresh_observed),
					  CLUSTER_WAL_STATE_SLOT_OFFSET(thread_id));
	pgstat_report_wait_end();
	if (nbytes != (ssize_t)sizeof(fresh_observed)) {
		if (nbytes >= 0)
			errno = EIO;
		result = CLUSTER_WAL_STATE_UPDATE_IO_ERROR;
		goto out;
	}
	result = cluster_wal_state_slot_verify_postread(&expected_after, &fresh_observed, thread_id,
													cluster_node_id);
	if (result == CLUSTER_WAL_STATE_UPDATE_OK && published_slot != NULL)
		memcpy(published_slot, &fresh_observed, sizeof(*published_slot));

out:
	cluster_r4_bit22_source_writer_leave();
	if (fd >= 0) {
		int save_errno = errno;

		(void)close(fd);
		errno = save_errno;
	}
	if (acquired_here) {
		/* STOP-01 §17.7 (RF A1 W1-W5, frozen): the coordinated CF(X)
		 * release must be CONFIRMED.  An unconfirmed release is
		 * RELEASE_UNCERTAIN — fail-closed (the caller must not re-acquire
		 * or re-publish on the same token; the slot deliberately stays
		 * held and the next acquire drains it — never a double grant). */
		if (cluster_cf_unlock_confirmed(ExclusiveLock) == CLUSTER_CF_RELEASE_UNCONFIRMED)
			return CLUSTER_WAL_STATE_UPDATE_RELEASE_UNCERTAIN;
	}
	return result;
}

/*
 * cluster_wal_state_ensure
 *
 *	Validate only.  Called from cluster_wal_thread_init() (postmaster,
 *	after the claim validation, before StartupXLOG).  initdb --check and
 *	--boot probes are intentionally earlier than the frontend finalizer.
 *	FATAL 53RA2 on missing, corrupt, foreign or unreadable evidence.
 */
bool
cluster_wal_state_ensure(void)
{
	char path[MAXPGPATH];
	int fd;
	unsigned char *image;
	ClusterWalStateSlot own_slot;
	ClusterWalSlotVerdict own_verdict;
	uint16 bad_thread = 0;
	uint16 own_thread;
	const char *reason = NULL;
	struct stat st;
	int block;

	if (IsBootstrapProcessingMode() || !registry_configured())
		return false;

	registry_path(path, sizeof(path));
	fd = BasicOpenFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_WAL_STATE_IO_FAILURE),
						errmsg("could not open required WAL state registry \"%s\": %m", path),
						errhint("Provision or restore a known-valid registry while the cluster is "
								"fully offline; runtime startup never creates or repairs it.")));
	if (fstat(fd, &st) != 0) {
		int save_errno = errno;

		(void)close(fd);
		errno = save_errno;
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_WAL_STATE_IO_FAILURE),
						errmsg("could not stat open WAL state registry \"%s\": %m", path),
						errhint("Check that the shared WAL storage is reachable.")));
	}
	if (!S_ISREG(st.st_mode) || st.st_size != (off_t)CLUSTER_WAL_STATE_FILE_SIZE) {
		long long actual_size = (long long)st.st_size;

		(void)close(fd);
		ereport(FATAL,
				(errcode(ERRCODE_CLUSTER_WAL_STATE_IO_FAILURE),
				 errmsg("WAL state registry \"%s\" is not a regular exact-size file "
						"(size %lld, expected %d)",
						path, actual_size, CLUSTER_WAL_STATE_FILE_SIZE),
				 errhint("Restore a known-valid registry while the cluster is fully offline; "
						 "runtime startup preserves the invalid evidence.")));
	}

	image = palloc0(CLUSTER_WAL_STATE_FILE_SIZE);
	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_WAL_STATE_READ);
	for (block = 0; block <= CLUSTER_WAL_STATE_SLOT_COUNT; block++) {
		off_t offset = (off_t)block * CLUSTER_WAL_STATE_SLOT_SIZE;
		ssize_t nread = pg_pread(fd, image + offset, CLUSTER_WAL_STATE_SLOT_SIZE, offset);

		if (nread != CLUSTER_WAL_STATE_SLOT_SIZE) {
			int save_errno = nread < 0 ? errno : EIO;

			pgstat_report_wait_end();
			pfree(image);
			(void)close(fd);
			errno = save_errno;
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_WAL_STATE_IO_FAILURE),
							errmsg("could not fully read WAL state registry \"%s\": %m", path),
							errhint("Restore a known-valid registry while the cluster is fully "
									"offline; runtime startup preserves the invalid evidence.")));
		}
	}
	pgstat_report_wait_end();
	if (close(fd) != 0) {
		int save_errno = errno;

		pfree(image);
		errno = save_errno;
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_WAL_STATE_IO_FAILURE),
						errmsg("could not close WAL state registry \"%s\": %m", path)));
	}

	if (!cluster_wal_state_image_validate(image, CLUSTER_WAL_STATE_FILE_SIZE, &bad_thread,
										  &reason)) {
		pfree(image);
		if (bad_thread == 0)
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_WAL_STATE_IO_FAILURE),
							errmsg("WAL state registry \"%s\" failed validation", path),
							errdetail("Header validation failed: %s.",
									  reason != NULL ? reason : "unknown"),
							errhint("Restore a known-valid registry while the cluster is fully "
									"offline; runtime startup never deletes, truncates or "
									"rebuilds it.")));
		else
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_WAL_STATE_IO_FAILURE),
							errmsg("WAL state registry \"%s\" failed validation", path),
							errdetail("Slot %u validation failed: %s.", (unsigned)bad_thread,
									  reason != NULL ? reason : "unknown"),
							errhint("Restore a known-valid registry while the cluster is fully "
									"offline; runtime startup never deletes, truncates or "
									"rebuilds it.")));
	}

	own_thread = cluster_wal_thread_id();
	memcpy(&own_slot, image + CLUSTER_WAL_STATE_SLOT_OFFSET(own_thread), sizeof(own_slot));
	own_verdict = cluster_wal_state_slot_classify(&own_slot, own_thread, cluster_node_id, &reason);
	if (own_verdict != CLUSTER_WAL_SLOT_EMPTY && own_verdict != CLUSTER_WAL_SLOT_OK) {
		pfree(image);
		if (own_verdict == CLUSTER_WAL_SLOT_FOREIGN)
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_WAL_STATE_IO_FAILURE),
							errmsg("WAL state registry \"%s\" own slot %u failed validation", path,
								   (unsigned)own_thread),
							errdetail("Own-slot validation failed: node_id mismatch "
									  "(expected %d, found %d).",
									  cluster_node_id, (int)own_slot.node_id),
							errhint("Restore the correct known-valid registry while the cluster is "
									"fully offline; foreign ownership evidence is preserved.")));
		ereport(FATAL,
				(errcode(ERRCODE_CLUSTER_WAL_STATE_IO_FAILURE),
				 errmsg("WAL state registry \"%s\" own slot %u failed validation", path,
						(unsigned)own_thread),
				 errdetail("Own-slot validation failed: %s.", reason != NULL ? reason : "unknown"),
				 errhint("Restore the correct known-valid registry while the cluster is "
						 "fully offline; foreign ownership evidence is preserved.")));
	}
	pfree(image);
	return true;
}

/*
 * fill_own_slot -- collect the live fields for this node's slot.
 *
 *	spec-6.4: an ADG standby publishes STOPPED while it is still in
 *	recovery -- it never leaves it -- so the insertion timeline does not
 *	exist yet (GetWALInsertionTimeLine() asserts RECOVERY_STATE_DONE) and
 *	the write pointer tracks nothing.  Publish the replay position and its
 *	timeline instead; on a primary the original insertion-side values are
 *	unchanged.
 */
static void
fill_own_slot(ClusterWalStateSlot *slot, uint32 state, int64 started_at)
{
	TimeLineID tli;
	XLogRecPtr write_ptr;

	if (RecoveryInProgress()) {
		tli = 0;
		write_ptr = GetXLogReplayRecPtr(&tli);
	} else {
		tli = GetWALInsertionTimeLine();
		write_ptr = GetXLogWriteRecPtr();
	}

	cluster_wal_state_slot_fill(slot, cluster_wal_thread_id(), cluster_node_id, state, (uint32)tli,
								started_at, (int64)GetCurrentTimestamp(), (uint64)write_ptr,
								(uint64)cluster_scn_current());
}

/*
 * preserve_ext_region -- carry the spec-4.5 extension region (offset
 *	56..503) from `prev` into a freshly filled `slot` and recompute the
 *	CRC.  fill memsets the whole slot, so without this every owner write
 *	(publish/refresh/stopped) would zero checkpoint_redo_lsn /
 *	fpw_was_off / the retained merge_recovered_lsn compatibility bytes /
 *	refresh_interval_ms every tick
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
	bool entered;

	/* RF-ROOT P9 verification (implementation): a closed source (bit22 first-open
	 * BARRIER) freezes the slot — benign no-op, never a failure. */
	entered = cluster_r4_bit22_source_writer_enter();
	if (!entered)
		return true;

	/* Decision-style injection (spec-4.2 D5): simulate a write failure. */
	if (cluster_injection_should_skip("cluster-wal-state-write-fail")) {
		cluster_r4_bit22_source_writer_leave();
		return false;
	}

	registry_path(path, sizeof(path));
	if (!write_block(path, CLUSTER_WAL_STATE_SLOT_OFFSET(slot->thread_id), slot)) {
		cluster_r4_bit22_source_writer_leave();
		return false;
	}
	cluster_r4_bit22_source_writer_leave();
	return true;
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
	 * Preserve the 4.5 extension region from the prior incarnation, but clear
	 * the retained merge_recovered_lsn compatibility bytes.  Recovery readers
	 * already treat them as semantic zero; checkpoint_redo_lsn / fpw_was_off
	 * survive.
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
 *	RF A1 W3, called only by the checkpointer after ShutdownXLOG returns.
 *	The common formed-registry RMW reacquires verified CF(X), changes the
 *	frozen STOPPED mask, fsyncs and verifies the exact after-image.  Failure
 *	returns false after WARNING, leaving ACTIVE/evidence.  Ordinary shutdown
 *	may carry on; phase-1 uses the result as its exact pre-barrier gate.
 */
bool
cluster_wal_state_publish_stopped(void)
{
	ClusterWalStateUpdate update;
	ClusterWalStateUpdateResult result;
	TimeLineID tli;
	XLogRecPtr write_ptr;

	if (!registry_configured())
		return false;

	if (RecoveryInProgress()) {
		write_ptr = GetXLogReplayRecPtr(&tli);
	} else {
		tli = GetWALInsertionTimeLine();
		write_ptr = GetXLogWriteRecPtr();
	}

	memset(&update, 0, sizeof(update));
	update.kind = CLUSTER_WAL_STATE_UPDATE_STOPPED;
	update.tli = (uint32)tli;
	update.last_updated = (int64)GetCurrentTimestamp();
	update.highest_lsn = (uint64)write_ptr;
	update.highest_scn = (uint64)cluster_scn_current();
	result = cluster_wal_state_update_own(&update, CLUSTER_WAL_STATE_CF_ACQUIRE_X, NULL);
	if (result != CLUSTER_WAL_STATE_UPDATE_OK && result != CLUSTER_WAL_STATE_UPDATE_NOOP
		&& result != CLUSTER_WAL_STATE_UPDATE_DISABLED)
		ereport(WARNING, (errcode(ERRCODE_CLUSTER_WAL_STATE_IO_FAILURE),
						  errmsg("could not publish STOPPED to the WAL state registry "
								 "(update result %d)",
								 (int)result),
						  errhint("The slot stays ACTIVE; recovery readers treat it "
								  "conservatively.")));
	return result == CLUSTER_WAL_STATE_UPDATE_OK || result == CLUSTER_WAL_STATE_UPDATE_NOOP;
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

/*
 * Durable-flush bound staged by publish_checkpoint_redo for
 * mutate_checkpoint_redo (own_slot_modify carries a single value).
 * Process-local: only one checkpoint publish runs per process at a time.
 */
static uint64 publish_flushed_bound = 0;

static void
mutate_checkpoint_redo(ClusterWalStateSlot *s, uint64 v)
{
	s->checkpoint_redo_lsn = v;

	/*
	 * spec-6.14 D9: raise the observational durable-write watermark alongside
	 * the redo publish.  The watermark otherwise advances only on the
	 * cluster_stats best-effort refresh tick, so a node that checkpoints and
	 * then dies within one refresh interval leaves highest_lsn BELOW its own
	 * checkpoint_redo_lsn -- and the thread-recovery window derivation
	 * correctly treats an inverted slot as garbage and fail-closes, freezing
	 * the dead thread forever.  The caller passes the checkpoint record's
	 * already-XLogFlush'd end LSN as the PROVEN durable lower bound (>= the
	 * just-published redo) -- it must NOT be read via GetFlushRecPtr here:
	 * the END_OF_RECOVERY checkpoint runs while SharedRecoveryState is not
	 * yet RECOVERY_STATE_DONE, where that accessor asserts (spec-6.14 D9
	 * amend, t/339 L2 checkpointer TRAP).  Monotonic max: never lower a
	 * fresher refresh.
	 */
	if (publish_flushed_bound > s->highest_lsn)
		s->highest_lsn = publish_flushed_bound;
}

static void
mutate_fpw_off(ClusterWalStateSlot *s, uint64 v)
{
	(void)v;
	s->fpw_was_off = 1;
}

void
cluster_wal_state_publish_checkpoint_redo(uint64 redo_lsn, uint64 flushed_lsn)
{
	publish_flushed_bound = flushed_lsn;
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

/*
 * RF-ROOT P7 G4 (bit22 open gate; follow-up): the runtime census.  The static
 * CI census (scripts/ci/check-wal-state-correctness-census.sh, strict mode)
 * counts every production correctness reader/writer call site of the
 * wal-state registry; this table is its runtime mirror — every known-deferred
 * correctness site that still exists in the binary is listed here.  The
 * activate proof (cluster_control_root_activate_authority_current_v1) calls
 * cluster_wal_state_correctness_census_ok() and fails closed while the table
 * is non-empty: bit22 must NOT open until each deferred site is migrated to
 * the canonical control root (G1b step 4), at which point the entry is
 * removed from BOTH this table and the script's DEFERRED list in the same
 * commit (the script cross-checks the two lists stay in lockstep).
 *
 * implementation / contract / follow-up (2026-08-18): the census is the POST-bit22
 * static proof (gate modeling).  recovery_plan.c / recovery_worker.c /
 * cluster_thread_recovery_orchestrator.c / cluster_hw_remaster.c registry
 * reads are GATE-BOUND — legal pre-bit22 (frozen §17.8) and statically
 * unreachable post-bit22 behind the recognized
 * cluster_r4_bit22_cutover_active() gate idiom — so the table is EMPTY:
 * the runtime latch apply self-check is GREEN and the static strict census
 * holds (post-bit22 exactly-zero, gate-modeled).  The table stays as the
 * lockstep anchor for any future ungated site (a regression re-lists it
 * here + in the script and turns the latch apply self-check RED).
 */
static const char *const cluster_wal_state_census_deferred_sites[] = { NULL };

bool
cluster_wal_state_correctness_census_ok(void)
{
	return cluster_wal_state_census_deferred_sites[0] == NULL;
}

#else /* !USE_PGRAC_CLUSTER */

/* Disable-cluster build: this file compiles to nothing. */

#endif /* USE_PGRAC_CLUSTER */
