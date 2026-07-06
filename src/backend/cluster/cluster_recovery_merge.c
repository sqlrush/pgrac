/*-------------------------------------------------------------------------
 *
 * cluster_recovery_merge.c
 *	  pgrac k-way SCN merge replay engine -- backend driver (spec-4.5).
 *
 *	  Wraps one file-based XLogReader per merge-set thread (each reading
 *	  <cluster.wal_threads_dir>/thread_<id>) around the pure min-heap in
 *	  cluster_recovery_merge.h.  begin() seeds each stream's first
 *	  record into the heap; next() returns the global-SCN-minimum record
 *	  (lazy-advancing the previously returned stream first); end() frees
 *	  everything.  Streams terminate naturally at their torn tail.
 *
 *	  Per-record decode exposes header.xl_scn as the ordering key, so a
 *	  zero scn that appears mid-stream (only legal as a boot prefix) or
 *	  a backwards scn is already rejected by ValidXLogRecordHeader
 *	  (spec-4.5 D3) before it reaches here.
 *
 *	  The engine only READS and ORDERS; the §3.3b freshness contract,
 *	  the §3.3c authority bound and the §3.3e G/S/L/U apply matrix are
 *	  applied by the caller (xlogrecovery.c) per popped record.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_recovery_merge.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.5-kway-scn-merge-replay.md FROZEN v1.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_tier1.h" /* spec-6.14 D9 amend: claim-holder liveness poll */
#include "cluster/cluster_recovery_plan.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_recovery_worker.h"
#include "cluster/cluster_wal_thread.h"
#include "postmaster/startup.h" /* spec-6.14 D9 amend: HandleStartupProcInterrupts */
#include "cluster/storage/cluster_shared_fs.h" /* spec-4.5a D4: capability gate */
#include "lib/stringinfo.h"
#include "miscadmin.h" /* DataDir (authority marker path) */
#include "port/pg_crc32c.h"
#include "storage/fd.h"
#include "utils/errcodes.h"
#include "utils/wait_event.h"

/*
 * Merged-replay window state (spec-4.5 §3.3b freshness + §3.3d/D8 GCS
 * cold bypass).  Single-process (startup): a plain file-scope flag is
 * enough.  The current record's xl_scn is published per record so the
 * §3.3b freshness skip and the central pd_block_scn stamp can read it.
 */
bool cluster_recmerge_window_active = false;
uint64 cluster_recmerge_window_scn = 0;
/* spec-4.5a: pd_lsn clamp for foreign pages.  A foreign record's EndRecPtr
 * lives in the PEER's WAL sequence and is INCOMPARABLE to (and usually beyond)
 * this node's own WAL flush point; stamping it as a materialized page's pd_lsn
 * makes the end-of-recovery checkpoint flush demand a WAL flush this node can
 * never satisfy.  While applying a foreign record we clamp pd_lsn to this
 * node's own recovery checkpoint redo (always <= own WAL flush end, already
 * durable); pd_block_scn remains the window's freshness authority. */
uint64 cluster_recmerge_window_own_lsn = 0;
bool cluster_recmerge_apply_foreign = false;

void
cluster_recovery_merge_window_enter(void)
{
	cluster_recmerge_window_active = true;
	cluster_recmerge_window_scn = 0;
	cluster_recmerge_apply_foreign = false;
}

void
cluster_recovery_merge_window_leave(void)
{
	cluster_recmerge_window_active = false;
	cluster_recmerge_window_scn = 0;
	cluster_recmerge_window_own_lsn = 0;
	cluster_recmerge_apply_foreign = false;
}

void
cluster_recovery_merge_set_scn(uint64 scn)
{
	cluster_recmerge_window_scn = scn;
}

void
cluster_recovery_merge_set_own_lsn(uint64 lsn)
{
	cluster_recmerge_window_own_lsn = lsn;
}

void
cluster_recovery_merge_set_apply_foreign(bool foreign)
{
	cluster_recmerge_apply_foreign = foreign;
}

bool
cluster_recovery_merge_window_is_active(void)
{
	return cluster_recmerge_window_active;
}

uint64
cluster_recovery_merge_window_scn(void)
{
	return cluster_recmerge_window_scn;
}

/*
 * Node-LOCAL merged-authority marker (spec-4.5a G6).
 *
 *	"This node materialized origin X's state" is knowledge about THIS
 *	node's pgdata (pg_undo/instance_<origin> + pg_xact_remote), so it
 *	must live in a node-local marker, NOT in the shared WAL-state
 *	registry.  The registry's per-stream merge_recovered_lsn serves a
 *	DIFFERENT consumer with a different lifecycle: the crashed origin
 *	reads it for the own-bound skip and CLEARS it on reaching RUNNING
 *	(§3.3c "the bound is spent").  Keying reader authority off that
 *	field severs this node's still-valid materialized authority the
 *	moment the peer self-recovers (t/248 L12 -> L1), and on a >2-node
 *	cluster would make EVERY node claim authority a single merging node
 *	published (false authority, 规则 8.A).
 *
 *	The marker is written at merged-replay completion, after the
 *	per-origin outcome flush; a crash before it re-merges (cold rerun),
 *	so a missing/torn marker is always sound: readers fail closed.
 */
typedef struct ClusterMergedAuthorityFile {
	uint32 magic;
	uint32 origin;		  /* 0-based origin node id */
	uint64 recovered_lsn; /* last merged record EndRecPtr, > 0 */
	pg_crc32c crc;		  /* over the three fields above */
} ClusterMergedAuthorityFile;

#define CLUSTER_MERGED_AUTHORITY_MAGIC 0x4d524741 /* "AGRM" */

static int
merged_authority_path(int origin_node, char *buf, size_t buf_size)
{
	int ret;

	ret = snprintf(buf, buf_size, "%s/pg_undo/instance_%d/merged.authority", DataDir, origin_node);
	if (ret < 0 || (size_t)ret >= buf_size)
		return -1;
	return 0;
}

/*
 * merged_authority_publish_impl -- write the node-local read-authority marker.
 *
 *	elevel selects the fail-closed severity (spec-4.11 3b-2, R13).  Cold merged
 *	replay (startup) passes FATAL: an unreachable marker re-merges cleanly on the
 *	next start.  Online thread recovery passes ERROR (catchable): the orchestrator
 *	runs this under its R13 harness, so a marker write failure demotes to BLOCKED
 *	and the survivor keeps running, having published NO serving authority (8.A).
 */
static void
merged_authority_publish_impl(int origin_node, uint64 recovered_lsn, int elevel)
{
	ClusterMergedAuthorityFile f;
	char dir[MAXPGPATH];
	char path[MAXPGPATH];
	int fd;
	int dret;

	Assert(origin_node >= 0 && origin_node < CLUSTER_WAL_STATE_SLOT_COUNT);
	Assert(recovered_lsn > 0);

	f.magic = CLUSTER_MERGED_AUTHORITY_MAGIC;
	f.origin = (uint32)origin_node;
	f.recovered_lsn = recovered_lsn;
	INIT_CRC32C(f.crc);
	COMP_CRC32C(f.crc, &f, offsetof(ClusterMergedAuthorityFile, crc));
	FIN_CRC32C(f.crc);

	/* The instance subdir normally exists (undo materialization created it);
	 * an origin merged without undo records still gets its marker. */
	dret = snprintf(dir, sizeof(dir), "%s/pg_undo/instance_%d", DataDir, origin_node);
	if (dret < 0 || (size_t)dret >= sizeof(dir)
		|| merged_authority_path(origin_node, path, sizeof(path)) < 0)
		ereport(elevel, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						 errmsg("merged recovery: authority marker path too long for origin %d",
								origin_node)));
	if (mkdir(dir, S_IRWXU) == 0) {
		/* Freshly created (an origin merged without undo records): the new
		 * dirent must be durable too, or a crash strands the marker in an
		 * unreachable directory (harmless -- the cold rerun re-merges -- but
		 * the durability claim below would be false). */
		char parent[MAXPGPATH];

		if (snprintf(parent, sizeof(parent), "%s/pg_undo", DataDir) < (int)sizeof(parent))
			fsync_fname(parent, true);
	} else if (errno != EEXIST)
		ereport(elevel, (errcode_for_file_access(),
						 errmsg("merged recovery: could not create \"%s\": %m", dir)));

	fd = OpenTransientFile(path, O_CREAT | O_TRUNC | O_WRONLY | PG_BINARY);
	if (fd < 0)
		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("merged recovery: could not create authority marker \"%s\": %m", path)));
	if (write(fd, &f, sizeof(f)) != sizeof(f) || pg_fsync(fd) != 0)
		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("merged recovery: could not write authority marker \"%s\": %m", path)));
	if (CloseTransientFile(fd) != 0)
		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("merged recovery: could not close authority marker \"%s\": %m", path)));
	fsync_fname(dir, true);
}

/*
 * cluster_merged_authority_publish -- record local read authority for one
 *	materialized origin (startup process, end of merged replay).  FATAL on
 *	any failure: the materialized state would otherwise be unreachable to
 *	readers, and a clean FATAL re-merges on the next start.
 */
void
cluster_merged_authority_publish(int origin_node, uint64 recovered_lsn)
{
	merged_authority_publish_impl(origin_node, recovered_lsn, FATAL);
}

/*
 * cluster_merged_authority_publish_online -- the spec-4.11 3b-2 online variant:
 *	same marker, but a failure raises a CATCHABLE ERROR so the thread-recovery
 *	orchestrator's R13 harness demotes it to BLOCKED (the survivor keeps running
 *	and publishes no serving authority).  Called only after a full DONE replay +
 *	durability barrier, as the LAST of the 3-way authority writes.
 */
void
cluster_merged_authority_publish_online(int origin_node, uint64 recovered_lsn)
{
	merged_authority_publish_impl(origin_node, recovered_lsn, ERROR);
}

/*
 * cluster_merged_instance_is_materialized -- spec-4.5a remote-read gate.
 *
 *	True iff THIS node completed a merged replay of origin_node's stream,
 *	per the node-local authority marker written at merged-replay
 *	completion (see ClusterMergedAuthorityFile above for why this is NOT
 *	the shared registry's merge_recovered_lsn).  Persistent across this
 *	node's restarts; survives the origin's own restart.  Missing, torn,
 *	or mismatched marker -> false (fail-closed).
 */
bool
cluster_merged_instance_is_materialized(int origin_node)
{
	ClusterMergedAuthorityFile f;
	pg_crc32c crc;
	char path[MAXPGPATH];
	int fd;
	ssize_t got;

	if (origin_node < 0 || origin_node >= CLUSTER_WAL_STATE_SLOT_COUNT)
		return false;
	if (merged_authority_path(origin_node, path, sizeof(path)) < 0)
		return false;

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return false; /* no marker -> never merged here (fail-closed) */
	got = read(fd, &f, sizeof(f));
	CloseTransientFile(fd);
	if (got != (ssize_t)sizeof(f))
		return false;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &f, offsetof(ClusterMergedAuthorityFile, crc));
	FIN_CRC32C(crc);
	if (f.magic != CLUSTER_MERGED_AUTHORITY_MAGIC || f.origin != (uint32)origin_node
		|| !EQ_CRC32C(f.crc, crc) || f.recovered_lsn == 0)
		return false;
	return true;
}

/*
 * cluster_merged_instance_recovered_through -- spec-4.7 D5 (Q5) redo gate.
 *
 *	Like cluster_merged_instance_is_materialized, but returns the actual
 *	persisted recovered_lsn (the EndRecPtr this node's merged replay of
 *	origin_node's stream reached) instead of a bool.  The spec-4.7
 *	redo-before-unfreeze gate compares this against the survivor's observed
 *	max page_lsn (required_lsn):  recovered_through(origin) >= required_lsn is
 *	the ONLY safe condition to unfreeze a block resource — a bool "marker
 *	exists" is too soft (it cannot prove redo covered the version a survivor
 *	already saw → lost-write, spec-2.37).  Missing / torn / mismatched marker
 *	-> 0 (fail-closed:  treated as "recovered through nothing").
 */
uint64
cluster_merged_instance_recovered_through(int origin_node)
{
	ClusterMergedAuthorityFile f;
	pg_crc32c crc;
	char path[MAXPGPATH];
	int fd;
	ssize_t got;

	if (origin_node < 0 || origin_node >= CLUSTER_WAL_STATE_SLOT_COUNT)
		return 0;
	if (merged_authority_path(origin_node, path, sizeof(path)) < 0)
		return 0;

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return 0; /* no marker -> recovered through nothing (fail-closed) */
	got = read(fd, &f, sizeof(f));
	CloseTransientFile(fd);
	if (got != (ssize_t)sizeof(f))
		return 0;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &f, offsetof(ClusterMergedAuthorityFile, crc));
	FIN_CRC32C(crc);
	if (f.magic != CLUSTER_MERGED_AUTHORITY_MAGIC || f.origin != (uint32)origin_node
		|| !EQ_CRC32C(f.crc, crc))
		return 0;
	return f.recovered_lsn;
}

/*
 * cluster_merged_any_remote_materialized -- is ANY remote stream merged here?
 *
 *	True iff at least one foreign origin's stream was merged-replayed by
 *	this node (cluster_merged_instance_is_materialized over every
 *	possible origin except our own).  Opens the per-origin authority
 *	marker (one open+read each) -- hot-path callers must cache the
 *	answer; an origin only becomes materialized during startup recovery
 *	(no live backends), so a backend-lifetime cache cannot go stale in
 *	the unsafe (false-negative) direction.
 */
bool
cluster_merged_any_remote_materialized(void)
{
	int origin;

	for (origin = 0; origin < CLUSTER_WAL_STATE_SLOT_COUNT; origin++) {
		if (origin == cluster_node_id)
			continue;
		if (cluster_merged_instance_is_materialized(origin))
			return true;
	}
	return false;
}

/* ============================================================
 * Shared-regime crash-recovery claim (spec-6.14 D9 amend, INV-D9-R).
 *
 *	One durable file on the shared root serializes crash recovery
 *	across the cluster (see the design note in cluster_recovery_merge.h
 *	above the pure claim core).  Atomicity: the claim content is first
 *	written durably to a per-node tmp file, then link(2)ed to the final
 *	name -- the claim either does not exist or is complete (no torn
 *	window), and exactly one concurrent link() wins.  The holder keeps
 *	it across its whole recovery (merge OR plain own-stream redo) and
 *	releases in StartupXLOG only after the end-of-recovery checkpoint
 *	made the recovered pages durable in shared storage.
 *
 *	Waiter liveness: the tier1 interconnect plane is already up during
 *	startup (listener + HELLO run in cluster startup phase 1, before
 *	phase 3 recovery), so a waiter judges the holder by its IC peer
 *	state.  An unlocked read of the shmem state field is fine for a
 *	100ms liveness poll.  A holder that stays un-CONNECTED for a full
 *	CSSD dead deadband is judged dead -> FATAL fail-closed: there is no
 *	early-fencing oracle to prove the dead holder's in-flight writes
 *	ceased, so the claim is never taken over (posture mirrors the
 *	sole-survivor bootstrap fail-close in cluster_cf_storage.c).
 * ============================================================
 */
#define MERGE_CLAIM_POLL_INTERVAL_MS 100

static bool merge_claim_held = false;

static int
merge_claim_path(char *buf, size_t buf_size)
{
	int ret;

	ret = snprintf(buf, buf_size, "%s/global/pgrac_merge.claim", cluster_shared_data_dir);
	if (ret < 0 || (size_t)ret >= buf_size)
		return -1;
	return 0;
}

/*
 * merge_claim_regime_configured -- is the shared-WAL crash-recovery
 *	regime fully configured on this node?  Outside it there is nothing a
 *	claim could serialize (no shared data root / no per-thread WAL), and
 *	the claim file could not even be named.
 */
static bool
merge_claim_regime_configured(void)
{
	return cluster_shared_storage_backend == CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS
		   && cluster_shared_data_dir != NULL && cluster_shared_data_dir[0] != '\0'
		   && cluster_wal_threads_dir != NULL && cluster_wal_threads_dir[0] != '\0'
		   && cluster_node_id >= 0 && cluster_wal_thread_id() != XLP_THREAD_ID_LEGACY;
}

/*
 * cluster_recovery_merge_claim_acquire_blocking -- see header.
 */
void
cluster_recovery_merge_claim_acquire_blocking(void)
{
	ClusterMergeClaimFile f;
	char claim[MAXPGPATH];
	char tmp[MAXPGPATH];
	char gdir[MAXPGPATH];
	uint64 sysid;
	int fd;
	int ret;
	long dead_ms = 0;
	long deadband_ms;
	int32 wait_holder = -1;

	if (!merge_claim_regime_configured() || merge_claim_held)
		return;

	if (merge_claim_path(claim, sizeof(claim)) < 0)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						errmsg("shared-regime recovery claim path too long")));
	ret = snprintf(tmp, sizeof(tmp), "%s.n%d.tmp", claim, cluster_node_id);
	if (ret < 0 || (size_t)ret >= sizeof(tmp))
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						errmsg("shared-regime recovery claim tmp path too long")));
	ret = snprintf(gdir, sizeof(gdir), "%s/global", cluster_shared_data_dir);
	if (ret < 0 || (size_t)ret >= sizeof(gdir))
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						errmsg("shared-regime recovery claim dir path too long")));

	sysid = GetSystemIdentifier();
	cluster_merge_claim_build(&f, cluster_node_id, sysid);

	/* Durable per-node tmp image; link() below publishes it atomically. */
	fd = OpenTransientFile(tmp, O_CREAT | O_TRUNC | O_WRONLY | PG_BINARY);
	if (fd < 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not create recovery claim tmp file \"%s\": %m", tmp)));
	if (write(fd, &f, sizeof(f)) != sizeof(f) || pg_fsync(fd) != 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not write recovery claim tmp file \"%s\": %m", tmp)));
	if (CloseTransientFile(fd) != 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not close recovery claim tmp file \"%s\": %m", tmp)));

	deadband_ms = (long)cluster_cssd_heartbeat_interval_ms
				  * (long)Max(cluster_cssd_dead_deadband_factor, 1);
	if (deadband_ms < 5000)
		deadband_ms = 5000;

	for (;;) {
		if (link(tmp, claim) == 0) {
			fsync_fname(gdir, true);
			(void)unlink(tmp);
			merge_claim_held = true;
			if (wait_holder >= 0)
				ereport(LOG,
						(errmsg("shared-regime recovery claim acquired by node %d after waiting "
								"for node %d (own-stream records a peer merged are skipped "
								"under the recovered bound)",
								cluster_node_id, wait_holder)));
			else
				ereport(LOG, (errmsg("shared-regime recovery claim acquired by node %d",
									 cluster_node_id)));
			return;
		}
		if (errno != EEXIST)
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("could not link recovery claim \"%s\": %m", claim)));

		/* Claim exists: read it back and judge the holder. */
		{
			char buf[sizeof(ClusterMergeClaimFile) + 1];
			ssize_t n;
			ClusterMergeClaimFile cur;
			ClusterMergeClaimVerdict v;

			fd = OpenTransientFile(claim, O_RDONLY | PG_BINARY);
			if (fd < 0) {
				if (errno == ENOENT)
					continue; /* released between link() and open(); retry */
				ereport(FATAL, (errcode_for_file_access(),
								errmsg("could not open recovery claim \"%s\": %m", claim)));
			}
			n = read(fd, buf, sizeof(buf));
			if (n < 0)
				ereport(FATAL, (errcode_for_file_access(),
								errmsg("could not read recovery claim \"%s\": %m", claim)));
			if (CloseTransientFile(fd) != 0)
				ereport(FATAL, (errcode_for_file_access(),
								errmsg("could not close recovery claim \"%s\": %m", claim)));

			v = cluster_merge_claim_classify(buf, (size_t)n, sysid);
			if (v != CLUSTER_MERGE_CLAIM_VALID)
				ereport(FATAL,
						(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						 errmsg("shared-regime recovery claim \"%s\" is not valid (verdict %d)",
								claim, (int)v),
						 errhint("The claim file on the shared root is corrupt or belongs to "
								 "another cluster.  Verify no node is recovering, remove the "
								 "file, and restart.")));
			memcpy(&cur, buf, sizeof(cur));

			if (cur.node == cluster_node_id) {
				/*
				 * Our own claim from a previous incarnation that crashed
				 * mid-recovery.  A node id maps to one pgdata, so no other
				 * process can be running under it -- adopt the claim and
				 * re-recover (the merge is SCN-idempotent over partially
				 * flushed pages).
				 */
				(void)unlink(tmp);
				merge_claim_held = true;
				ereport(LOG,
						(errmsg("shared-regime recovery claim re-adopted by node %d (previous "
								"incarnation crashed while recovering; re-running recovery)",
								cluster_node_id)));
				return;
			}

			/* Held by a peer: wait for its release, watching its liveness. */
			if (cur.node != wait_holder) {
				wait_holder = cur.node;
				dead_ms = 0;
				ereport(LOG,
						(errmsg("shared-regime crash recovery already claimed by node %d; "
								"waiting for its recovery to complete",
								wait_holder)));
			}
		}

		{
			const ClusterICPeerStateShmem *ps = cluster_ic_tier1_peer_get(wait_holder);

			if (ps != NULL && ps->state == (int32)CLUSTER_IC_PEER_CONNECTED)
				dead_ms = 0; /* holder reachable; keep waiting */
			else
				dead_ms += MERGE_CLAIM_POLL_INTERVAL_MS;
		}
		if (dead_ms >= deadband_ms)
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
					 errmsg("node %d died holding the shared-regime recovery claim", wait_holder),
					 errdetail("The claim holder has been unreachable on the interconnect for "
							   "%ld ms while its claim persists on the shared root.",
							   dead_ms),
					 errhint("The claim is never taken over without fencing (the dead holder's "
							 "in-flight writes cannot be proven ceased).  Restart node %d so it "
							 "re-adopts and completes its recovery, or verify it is down, remove "
							 "\"%s\" from the shared root, and restart this node.",
							 wait_holder, claim)));

		HandleStartupProcInterrupts();
		pg_usleep(MERGE_CLAIM_POLL_INTERVAL_MS * 1000L);
	}
}

/*
 * cluster_recovery_merge_claim_is_held -- is this node the sole merger?
 *	(spec-6.14 D5 arbitration gates on it: only the claim holder may
 *	resolve pending relmap images.)
 */
bool
cluster_recovery_merge_claim_is_held(void)
{
	return merge_claim_held;
}

/*
 * cluster_recovery_merge_claim_release_if_held -- see header.
 */
void
cluster_recovery_merge_claim_release_if_held(void)
{
	char claim[MAXPGPATH];
	char gdir[MAXPGPATH];
	int ret;

	if (!merge_claim_held)
		return;
	merge_claim_held = false;

	if (merge_claim_path(claim, sizeof(claim)) < 0)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						errmsg("shared-regime recovery claim path too long")));
	if (unlink(claim) != 0 && errno != ENOENT)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not release recovery claim \"%s\": %m", claim),
						errhint("A claim this node cannot remove would wedge every peer's "
								"crash recovery; shared storage must support unlink here.")));
	ret = snprintf(gdir, sizeof(gdir), "%s/global", cluster_shared_data_dir);
	if (ret > 0 && (size_t)ret < sizeof(gdir))
		fsync_fname(gdir, true);
	ereport(LOG, (errmsg("shared-regime recovery claim released by node %d "
						 "(recovered content durable after the end-of-recovery checkpoint)",
						 cluster_node_id)));
}

typedef struct MergeStream {
	uint16 thread_id;
	XLogReaderState *reader;
	int seg_fd; /* currently open segment, -1 = none */
	XLogSegNo seg_no;
	char dir[MAXPGPATH];
	bool exhausted;
	bool streaming;
	bool head_ready;
	ClusterRecmergeKey head_key;
	XLogRecPtr valid_end; /* spec-4.5a D6: EndRecPtr of the last complete
						   * record found by the pre-replay decode pass */
	XLogRecPtr last_end;  /* EndRecPtr of the last record replay consumed */
	XLogRecPtr stop_lsn;  /* restore mode: inclusive per-thread cut record */
	bool restore_mode;
} MergeStream;

struct ClusterRecoveryMergeState {
	int n_streams;
	MergeStream streams[CLUSTER_RECMERGE_MAX_STREAMS];
	ClusterRecmergeHeap heap;
	int last_stream; /* stream returned by the previous next(), -1 = none */
	TimeLineID tli;
};

/*
 * Segment open/close + page read for an arbitrary thread directory.
 * Mirrors pg_waldump's file-based reader (no XLogCtl dependency -- the
 * foreign streams are not this node's running WAL).
 */
static void
/* cppcheck-suppress constParameterCallback */
merge_segment_open(XLogReaderState *state, XLogSegNo nextSegNo, TimeLineID *tli_p)
{
	MergeStream *ms = (MergeStream *)state->private_data;
	char fname[MAXFNAMELEN];
	char fpath[MAXPGPATH];

	if (ms->seg_fd >= 0) {
		close(ms->seg_fd);
		ms->seg_fd = -1;
	}
	XLogFileName(fname, *tli_p, nextSegNo, state->segcxt.ws_segsize);
	snprintf(fpath, sizeof(fpath), "%s/%s", ms->dir, fname);
	ms->seg_fd = BasicOpenFile(fpath, O_RDONLY | PG_BINARY);
	if (ms->seg_fd < 0) {
		/*
		 * spec-4.5a: a MISSING next segment is end-of-stream, not a hard
		 * error.  A crashed peer's WAL can stop exactly at a segment
		 * boundary (e.g. it forced a WAL switch then died before writing
		 * the next segment).  Leave seg_fd = -1; merge_page_read returns -1
		 * so the decode stops here.  Whether that is a clean torn tail or
		 * corruption BELOW the validated end is then decided by
		 * merge_compute_valid_end's highest_lsn / first-record checks (hard
		 * obligation 2) -- this never silently drops a peer's committed WAL.
		 * A non-ENOENT failure (real I/O error) is still fatal.
		 */
		if (errno == ENOENT)
			return;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open WAL segment \"%s\" for merged recovery: %m", fpath)));
	}
	ms->seg_no = nextSegNo;
	state->seg.ws_file = ms->seg_fd;
}

static void
merge_segment_close(XLogReaderState *state)
{
	MergeStream *ms = (MergeStream *)state->private_data;

	if (ms->seg_fd >= 0) {
		close(ms->seg_fd);
		ms->seg_fd = -1;
	}
	state->seg.ws_file = -1;
}

static int
merge_page_read(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
				XLogRecPtr targetRecPtr, char *readBuf)
{
	MergeStream *ms = (MergeStream *)state->private_data;
	XLogSegNo segno;
	uint32 offset;
	int nread;

	XLByteToSeg(targetPagePtr, segno, state->segcxt.ws_segsize);
	if (ms->seg_fd < 0 || segno != ms->seg_no)
		merge_segment_open(state, segno, &state->seg.ws_tli);
	if (ms->seg_fd < 0)
		return -1; /* missing segment -> stream end (segment_open ENOENT) */

	offset = XLogSegmentOffset(targetPagePtr, state->segcxt.ws_segsize);
	pgstat_report_wait_start(WAIT_EVENT_WAL_READ);
	nread = pg_pread(ms->seg_fd, readBuf, XLOG_BLCKSZ, (off_t)offset);
	pgstat_report_wait_end();
	if (nread < reqLen)
		return -1; /* torn / short before the requested bytes */
	return (int)nread;
}

/*
 * Read one record on a stream; push its key on success.
 *
 * spec-4.5a D5 (one-head-per-stream invariant): a stream is advanced only
 * from begin (seed) or from next() AFTER its previous record was popped and
 * consumed, so at most ONE entry per stream is ever in the heap.  The heap
 * therefore only ever orders heads of DISTINCT streams by scn_recovery_cmp;
 * two records of the same stream never meet there, and same-stream order is
 * LSN by construction (each stream yields records in reader LSN order).
 * per-thread xl_scn monotonicity is irrelevant to the heap.
 *
 * spec-4.5a D6 (torn-tail boundary): a decode failure is a NORMAL tail only
 * when we have already consumed every complete record the pre-replay pass
 * validated (last_end >= valid_end).  A failure BEFORE valid_end means a
 * record the validation pass could decode is now undecodable at replay --
 * real corruption -- and is fail-closed 53RA3, never a silent stream
 * truncation that would drop a crashed peer's committed WAL.
 */
static void
stream_advance(ClusterRecoveryMergeState *st, int idx)
{
	MergeStream *ms = &st->streams[idx];
	char *errm = NULL;
	const XLogRecord *rec;

	if (ms->exhausted)
		return;
	if (ms->head_ready)
		return;
	rec = XLogReadRecord(ms->reader, &errm);
	if (rec == NULL) {
		if (ms->last_end < ms->valid_end)
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
					 errmsg("merged recovery: WAL decode failed before the validated end on "
							"thread %u",
							(unsigned)ms->thread_id),
					 errdetail("replay decoded through %X/%X but the pre-replay pass validated "
							   "records through %X/%X%s%s.",
							   LSN_FORMAT_ARGS(ms->last_end), LSN_FORMAT_ARGS(ms->valid_end),
							   errm ? ": " : "", errm ? errm : ""),
					 errhint("A crashed peer's WAL stream is corrupt between validation and "
							 "replay; recover this node's own stream with "
							 "cluster.merged_recovery=off.")));
		ms->exhausted = true;
		return;
	}
	if (ms->stop_lsn != InvalidXLogRecPtr && ms->reader->ReadRecPtr > ms->stop_lsn) {
		ms->exhausted = true;
		return;
	}
	ms->last_end = ms->reader->EndRecPtr;
	{
		ClusterRecmergeKey key;

		key.scn = rec->xl_scn;
		key.lsn = ms->reader->ReadRecPtr;
		key.node = (int32)ms->thread_id - 1;
		ms->head_key = key;
		cluster_recmerge_heap_push(&st->heap, key, idx);
		ms->head_ready = true;
	}
}

static void
streaming_stream_advance(ClusterRecoveryMergeState *st, int idx, const XLogRecPtr *receive_lsn)
{
	MergeStream *ms = &st->streams[idx];
	char *errm = NULL;
	const XLogRecord *rec;

	if (ms->exhausted)
		return;
	if (ms->head_ready)
		return;
	if (receive_lsn != NULL) {
		XLogRecPtr received = receive_lsn[ms->thread_id];

		if (XLogRecPtrIsInvalid(received) || received <= ms->last_end)
			return;
	}

	rec = XLogReadRecord(ms->reader, &errm);
	if (rec == NULL) {
		XLogBeginRead(ms->reader, ms->last_end);
		return;
	}

	if (receive_lsn != NULL) {
		XLogRecPtr received = receive_lsn[ms->thread_id];

		if (XLogRecPtrIsInvalid(received) || ms->reader->EndRecPtr > received) {
			XLogBeginRead(ms->reader, ms->last_end);
			return;
		}
	}

	ms->last_end = ms->reader->EndRecPtr;
	{
		ClusterRecmergeKey key;

		key.scn = rec->xl_scn;
		key.lsn = ms->reader->ReadRecPtr;
		key.node = (int32)ms->thread_id - 1;
		ms->head_key = key;
		ms->head_ready = true;
	}
}

/*
 * cluster_recovery_merge_decide -- §3.1 engage + §3.2 53RA3 gate.
 */
ClusterMergeEngage
cluster_recovery_merge_decide(uint16 own_thread, XLogRecPtr own_redo, uint64 out_bitmap[2],
							  XLogRecPtr *out_start)
{
	ClusterRecoveryPlan plan;
	ClusterRecoveryWorkerPool pool;
	bool have_pool;
	uint16 tid;
	StringInfoData blockers;

	if (!cluster_merged_recovery)
		return CLUSTER_MERGE_NO_DISABLED;
	if (cluster_wal_threads_dir == NULL || cluster_wal_threads_dir[0] == '\0'
		|| own_thread == XLP_THREAD_ID_LEGACY)
		return CLUSTER_MERGE_NO_NOT_CONFIGURED;
	if (!cluster_recovery_plan_snapshot(&plan) || plan.failed)
		return CLUSTER_MERGE_NO_NO_PLAN;
	if (plan.n_crashed_candidate == 0)
		return CLUSTER_MERGE_NO_NO_CANDIDATES;
	if (plan.n_alive > 0)
		return CLUSTER_MERGE_NO_NOT_COLD; /* warm -> 4.6/4.7, not us */

	/*
	 * spec-4.5a D4: capability gate.  Merged recovery replays a crashed
	 * peer's SHARED-storage pages, so it requires a genuinely shared data
	 * backend.  spec-4.5 shipped this fail-closed UNCONDITIONALLY because
	 * cluster_shared_fs was stub/local only (per-node, not shared --
	 * spec-3.18 V-2).  4.5a lifts the gate for the shared_fs backend (id
	 * CLUSTER_FS) once a shared root is configured; stub/local still
	 * fail-closed 53RA3 so a per-node backend can never mis-engage.  The
	 * per-candidate "peer wrote to THIS shared root" check (sentinel
	 * participant set) is folded into the §3.2 blocker loop below.
	 */
	if (cluster_shared_storage_backend != CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS
		|| cluster_shared_data_dir == NULL || cluster_shared_data_dir[0] == '\0')
		ereport(FATAL,
				(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
				 errmsg("merged k-way recovery is not supported without a shared-data storage "
						"backend"),
				 errdetail("cluster.merged_recovery is on and crash candidates exist, but "
						   "cluster.shared_storage_backend is not the shared_fs (cluster_fs) "
						   "backend with cluster.shared_data_dir set."),
				 errhint("Set cluster.shared_storage_backend=cluster_fs and "
						 "cluster.shared_data_dir to the shared mount, or recover this node's "
						 "own stream with cluster.merged_recovery=off.")));

	/*
	 * §3.2 hard gate.  Collect every blocking reason, then FATAL 53RA3
	 * once with the full list -- never silently fall back to single
	 * stream (that would skip a crashed peer's committed WAL).
	 */
	initStringInfo(&blockers);
	if (plan.n_unknown > 0)
		appendStringInfo(&blockers, "%s%u UNKNOWN plan thread(s)", blockers.len ? "; " : "",
						 (unsigned)plan.n_unknown);
	if (!fullPageWrites)
		appendStringInfo(&blockers, "%slocal full_page_writes=off", blockers.len ? "; " : "");

	have_pool = cluster_recovery_worker_pool_snapshot(&pool);

	/* Build the merge set (candidates + own) and validate each. */
	memset(out_bitmap, 0, sizeof(uint64) * 2);
	for (tid = 1; tid <= CLUSTER_WAL_STATE_SLOT_COUNT; tid++) {
		bool is_candidate = cluster_recovery_plan_candidate_test(&plan, tid);
		ClusterWalStateSlot slot;

		if (!is_candidate && tid != own_thread)
			continue;

		out_bitmap[(tid - 1) / 64] |= ((uint64)1 << ((tid - 1) % 64));

		if (tid == own_thread) {
			out_start[tid] = own_redo;
			continue; /* own thread: gate items below are peer-only */
		}

		/*
		 * Candidate stream must validate OK.  Use the worker verdict if
		 * present; NONE means the workers did not finish in time, so
		 * re-validate inline (Q6).  SKIPPED is fatal -- the peer was
		 * alive, so the cold premise broke.
		 */
		{
			ClusterRecoveryStreamVerdict v
				= have_pool ? (ClusterRecoveryStreamVerdict)pool.stream_verdict[tid]
							: CLUSTER_RECOVERY_STREAM_NONE;

			if (v == CLUSTER_RECOVERY_STREAM_NONE)
				v = cluster_recovery_worker_revalidate(tid);

			if (v == CLUSTER_RECOVERY_STREAM_SKIPPED)
				appendStringInfo(&blockers, "%sthread %u stream SKIPPED (peer was alive)",
								 blockers.len ? "; " : "", (unsigned)tid);
			else if (v != CLUSTER_RECOVERY_STREAM_OK)
				appendStringInfo(&blockers, "%sthread %u stream not OK (verdict %u)",
								 blockers.len ? "; " : "", (unsigned)tid, (unsigned)v);
		}

		/*
			 * spec-4.5a D4: the peer must have written to THIS shared root.
			 * Its node_id (= tid - 1) is recorded in the shared-root sentinel
			 * participant set when it attached.  A peer absent from the set
			 * never wrote here, so merging its stream would be dishonest --
			 * fail-closed.
			 */
		if (!cluster_shared_fs_sentinel_has_participant((int)tid - 1))
			appendStringInfo(&blockers,
							 "%sthread %u peer (node %d) is not a shared-root participant",
							 blockers.len ? "; " : "", (unsigned)tid, (int)tid - 1);

		/* Candidate start point + fpw history from its slot. */
		if (cluster_wal_state_read_slot(tid, &slot) != CLUSTER_WAL_SLOT_OK) {
			appendStringInfo(&blockers, "%sthread %u slot unreadable", blockers.len ? "; " : "",
							 (unsigned)tid);
		} else {
			if (slot.checkpoint_redo_lsn == 0)
				appendStringInfo(&blockers, "%sthread %u has no checkpoint redo start",
								 blockers.len ? "; " : "", (unsigned)tid);
			if (slot.fpw_was_off != 0)
				appendStringInfo(&blockers, "%sthread %u ran with full_page_writes=off",
								 blockers.len ? "; " : "", (unsigned)tid);
			out_start[tid] = (XLogRecPtr)slot.checkpoint_redo_lsn;
		}
	}

	if (blockers.len > 0)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						errmsg("merged k-way recovery refused"), errdetail("%s.", blockers.data),
						errhint("Resolve the shared WAL storage / configuration, or set "
								"cluster.merged_recovery=off to recover this node's own "
								"stream only (a crashed peer's committed WAL will not be "
								"recovered).")));
	pfree(blockers.data);
	return CLUSTER_MERGE_ENGAGE;
}

/*
 * spec-4.5a D6: pre-replay decode pass.  Decode a thread's stream from
 * start_lsn to its end with a throwaway reader, returning the EndRecPtr of
 * the last COMPLETE record (or start_lsn if the stream has none).  This is
 * the validated torn-tail boundary fed to stream_advance: replay must reach
 * it; a decode failure before it is fail-closed corruption (D6), never a
 * silent truncation of a crashed peer's committed WAL.  Computed here in the
 * startup process (after merge_decide), so -- unlike spec-4.5a v0.5's
 * worker-pool stream_valid_end_lsn ABI -- no cross-process concurrency or
 * release/acquire is involved; the P1-3 torn-snapshot hazard cannot arise.
 */
static XLogRecPtr
merge_compute_valid_end(const char *dir, XLogRecPtr start_lsn, XLogRecPtr validated_min,
						bool is_candidate, uint16 tid, TimeLineID tli, XLogRecPtr stop_lsn,
						bool restore_mode)
{
	MergeStream tmp;
	XLogReaderState *reader;
	XLogRecPtr valid_end = start_lsn;
	char *errm = NULL;

	memset(&tmp, 0, sizeof(tmp));
	tmp.seg_fd = -1;
	strlcpy(tmp.dir, dir, sizeof(tmp.dir));
	reader = XLogReaderAllocate(wal_segment_size, tmp.dir,
								XL_ROUTINE(.page_read = merge_page_read,
										   .segment_open = merge_segment_open,
										   .segment_close = merge_segment_close),
								&tmp);
	if (reader == NULL)
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
						errmsg("out of memory allocating merged-recovery validation reader")));
	reader->seg.ws_tli = tli;
	XLogBeginRead(reader, start_lsn);
	while (XLogReadRecord(reader, &errm) != NULL) {
		valid_end = reader->EndRecPtr;
		if (stop_lsn != InvalidXLogRecPtr && reader->ReadRecPtr >= stop_lsn)
			break;
	}
	if (tmp.seg_fd >= 0) {
		close(tmp.seg_fd);
		tmp.seg_fd = -1;
	}
	XLogReaderFree(reader);

	/*
	 * spec-4.5a hard obligation 2 (foreign candidate streams only -- the own
	 * thread keeps PG-native torn-tail semantics).  A crashed peer that we are
	 * merging published a checkpoint_redo_lsn into the registry, which proves
	 * it durably wrote at least a valid checkpoint at start_lsn.  Two
	 * fail-closed checks keep a corrupt/truncated peer stream from being
	 * silently treated as a short torn tail (which would drop the peer's
	 * committed WAL and let this node start up "clean"):
	 *
	 *   (a) valid_end == start_lsn: not a single complete record decoded from
	 *       the registered checkpoint redo point.  The checkpoint record alone
	 *       must decode in a healthy stream, so zero records = corruption AT
	 *       the start (the worst case -- it would drop EVERYTHING).  This is
	 *       reliable regardless of the observational highest_lsn cadence.
	 *
	 *   (b) valid_end < validated_min: the registry's highest_lsn watermark
	 *       (refreshed AFTER the bytes were written, hence a safe lower bound)
	 *       sits past where decode stopped -> mid-stream corruption.  Only
	 *       enforced when the watermark is fresh enough to exceed start_lsn;
	 *       otherwise (a) is the floor.
	 */
	if (is_candidate
		&& (valid_end == start_lsn
			|| (validated_min != InvalidXLogRecPtr && valid_end < validated_min)))
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						errmsg("merged recovery: thread %u WAL is corrupt below the validated end",
							   (unsigned)tid),
						errdetail("decoded through %X/%X from checkpoint redo %X/%X; the registry "
								  "recorded durable writes through %X/%X.",
								  LSN_FORMAT_ARGS(valid_end), LSN_FORMAT_ARGS(start_lsn),
								  LSN_FORMAT_ARGS(validated_min)),
						errhint("A crashed peer's WAL stream is truncated or corrupt before its "
								"recorded end; recover this node's own stream with "
								"cluster.merged_recovery=off.")));
	if (restore_mode && stop_lsn != InvalidXLogRecPtr && valid_end < stop_lsn)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
						errmsg("cluster backup restore: thread %u WAL does not reach the "
							   "manifest cut",
							   (unsigned)tid),
						errdetail("decoded through %X/%X from start %X/%X; manifest cut is "
								  "%X/%X%s%s.",
								  LSN_FORMAT_ARGS(valid_end), LSN_FORMAT_ARGS(start_lsn),
								  LSN_FORMAT_ARGS(stop_lsn), errm ? ": " : "", errm ? errm : ""),
						errhint("The backup set or archive is missing WAL required by the "
								"cluster backup manifest.")));
	return valid_end;
}

static ClusterRecoveryMergeState *
cluster_recovery_merge_begin_internal(const uint64 merge_bitmap[2], const XLogRecPtr *start_lsn,
									  const XLogRecPtr *stop_lsn, const char *wal_root,
									  uint16 own_thread, TimeLineID tli, bool restore_mode)
{
	ClusterRecoveryMergeState *st;
	uint16 tid;
	int idx = 0;

	st = (ClusterRecoveryMergeState *)palloc0(sizeof(ClusterRecoveryMergeState));
	cluster_recmerge_heap_init(&st->heap);
	st->last_stream = -1;
	st->tli = tli;

	for (tid = 1; tid <= CLUSTER_WAL_STATE_SLOT_COUNT; tid++) {
		MergeStream *ms;

		if ((merge_bitmap[(tid - 1) / 64] & ((uint64)1 << ((tid - 1) % 64))) == 0)
			continue;
		if (idx >= CLUSTER_RECMERGE_MAX_STREAMS)
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
							errmsg("merged recovery merge set exceeds %d streams",
								   CLUSTER_RECMERGE_MAX_STREAMS)));
		ms = &st->streams[idx];
		ms->thread_id = tid;
		ms->seg_fd = -1;
		ms->exhausted = false;
		ms->streaming = false;
		ms->head_ready = false;
		ms->stop_lsn = stop_lsn != NULL ? stop_lsn[tid] : InvalidXLogRecPtr;
		ms->restore_mode = restore_mode;
		snprintf(ms->dir, sizeof(ms->dir), "%s/thread_%u", wal_root, (unsigned)tid);
		ms->reader = XLogReaderAllocate(wal_segment_size, ms->dir,
										XL_ROUTINE(.page_read = merge_page_read,
												   .segment_open = merge_segment_open,
												   .segment_close = merge_segment_close),
										ms);
		if (ms->reader == NULL)
			ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
							errmsg("out of memory allocating merged-recovery reader")));
		ms->reader->seg.ws_tli = tli;
		XLogBeginRead(ms->reader, start_lsn[tid]);
		{
			/* spec-4.5a hard obligation 2: bound the validated end by the
			 * candidate's registry-recorded highest_lsn (durable write end).
			 * A stream whose decode stops short of it is corrupt below the
			 * validated end, not a torn tail -- fail-closed in the helper. */
			ClusterWalStateSlot slot;
			XLogRecPtr validated_min = InvalidXLogRecPtr;

			if (!restore_mode && cluster_wal_state_read_slot(tid, &slot) == CLUSTER_WAL_SLOT_OK
				&& slot.highest_lsn > (uint64)start_lsn[tid])
				validated_min = (XLogRecPtr)slot.highest_lsn;
			ms->valid_end = merge_compute_valid_end(ms->dir, start_lsn[tid], validated_min,
													!restore_mode && tid != own_thread, tid, tli,
													ms->stop_lsn, restore_mode);
		}
		ms->last_end = start_lsn[tid];
		idx++;
	}
	st->n_streams = idx;

	/* Seed the heap with each stream's first record. */
	for (idx = 0; idx < st->n_streams; idx++)
		stream_advance(st, idx);
	return st;
}

ClusterRecoveryMergeState *
cluster_recovery_merge_begin(const uint64 merge_bitmap[2], const XLogRecPtr *start_lsn,
							 uint16 own_thread, TimeLineID tli)
{
	return cluster_recovery_merge_begin_internal(merge_bitmap, start_lsn, NULL,
												 cluster_wal_threads_dir, own_thread, tli, false);
}

ClusterRecoveryMergeState *
cluster_recovery_merge_begin_restore(const uint64 merge_bitmap[2], const XLogRecPtr *start_lsn,
									 const XLogRecPtr *stop_lsn, const char *wal_root,
									 uint16 own_thread, TimeLineID tli)
{
	if (wal_root == NULL || wal_root[0] == '\0')
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
						errmsg("cluster backup restore has no per-thread WAL root")));
	return cluster_recovery_merge_begin_internal(merge_bitmap, start_lsn, stop_lsn, wal_root,
												 own_thread, tli, true);
}

ClusterRecoveryMergeState *
cluster_recovery_merge_streaming_begin(const uint64 merge_bitmap[2], const XLogRecPtr *start_lsn,
									   TimeLineID tli)
{
	ClusterRecoveryMergeState *st;
	uint16 tid;
	int idx = 0;

	st = (ClusterRecoveryMergeState *)palloc0(sizeof(ClusterRecoveryMergeState));
	cluster_recmerge_heap_init(&st->heap);
	st->last_stream = -1;
	st->tli = tli;

	for (tid = 1; tid <= CLUSTER_WAL_STATE_SLOT_COUNT; tid++) {
		MergeStream *ms;

		if ((merge_bitmap[(tid - 1) / 64] & ((uint64)1 << ((tid - 1) % 64))) == 0)
			continue;
		if (idx >= CLUSTER_RECMERGE_MAX_STREAMS)
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
							errmsg("ADG streaming merge set exceeds %d streams",
								   CLUSTER_RECMERGE_MAX_STREAMS)));
		if (start_lsn == NULL || XLogRecPtrIsInvalid(start_lsn[tid]))
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
					 errmsg("ADG streaming merge has no start LSN for thread %u", (unsigned)tid)));

		ms = &st->streams[idx];
		ms->thread_id = tid;
		ms->seg_fd = -1;
		ms->exhausted = false;
		ms->streaming = true;
		ms->head_ready = false;
		ms->valid_end = InvalidXLogRecPtr;
		ms->last_end = start_lsn[tid];
		snprintf(ms->dir, sizeof(ms->dir), "%s/thread_%u", cluster_wal_threads_dir, (unsigned)tid);
		ms->reader = XLogReaderAllocate(wal_segment_size, ms->dir,
										XL_ROUTINE(.page_read = merge_page_read,
												   .segment_open = merge_segment_open,
												   .segment_close = merge_segment_close),
										ms);
		if (ms->reader == NULL)
			ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
							errmsg("out of memory allocating ADG streaming merge reader")));
		ms->reader->seg.ws_tli = tli;
		ms->last_end = XLogFindNextRecord(ms->reader, start_lsn[tid]);
		if (XLogRecPtrIsInvalid(ms->last_end)) {
			st->n_streams = idx + 1;
			cluster_recovery_merge_end(st);
			return NULL;
		}
		idx++;
	}
	st->n_streams = idx;
	return st;
}

XLogReaderState *
cluster_recovery_merge_next(ClusterRecoveryMergeState *st, uint16 *thread_out, char **errmsg_out)
{
	ClusterRecmergeHeapEntry top;

	if (errmsg_out)
		*errmsg_out = NULL;

	/* Lazily advance the stream returned last time (its record has now
	 * been consumed by the caller). */
	if (st->last_stream >= 0) {
		stream_advance(st, st->last_stream);
		st->last_stream = -1;
	}

	if (!cluster_recmerge_heap_pop(&st->heap, &top))
		return NULL; /* all streams exhausted */

	st->last_stream = top.stream;
	st->streams[top.stream].head_ready = false;
	if (thread_out)
		*thread_out = st->streams[top.stream].thread_id;
	return st->streams[top.stream].reader;
}

XLogReaderState *
cluster_recovery_merge_streaming_next(ClusterRecoveryMergeState *st, const XLogRecPtr *receive_lsn,
									  const XLogRecPtr *barrier_lsn, const SCN *barrier_scn,
									  uint16 *thread_out, char **errmsg_out)
{
	int idx;
	int advanced_stream = -1;
	ClusterRecmergeStreamingInput inputs[CLUSTER_RECMERGE_MAX_STREAMS];
	ClusterRecmergeKey selected_key;
	int selected_stream = -1;

	if (errmsg_out)
		*errmsg_out = NULL;
	if (st == NULL)
		return NULL;

	if (st->last_stream >= 0) {
		advanced_stream = st->last_stream;
		streaming_stream_advance(st, st->last_stream, receive_lsn);
		st->last_stream = -1;
	}
	for (idx = 0; idx < st->n_streams; idx++) {
		if (idx == advanced_stream)
			continue;
		streaming_stream_advance(st, idx, receive_lsn);
	}
	memset(inputs, 0, sizeof(inputs));
	for (idx = 0; idx < st->n_streams; idx++) {
		const MergeStream *ms = &st->streams[idx];

		if (ms->head_ready) {
			inputs[idx].record_available = true;
			inputs[idx].key = ms->head_key;
			continue;
		}
		if (barrier_lsn != NULL && barrier_scn != NULL && barrier_lsn[ms->thread_id] >= ms->last_end
			&& SCN_VALID(barrier_scn[ms->thread_id])) {
			inputs[idx].heartbeat_seen = true;
			inputs[idx].heartbeat_key.scn = (uint64)barrier_scn[ms->thread_id];
			inputs[idx].heartbeat_key.lsn = PG_UINT64_MAX;
			inputs[idx].heartbeat_key.node = SCN_MAX_VALID_NODE_ID;
		}
	}
	if (cluster_recmerge_streaming_select(inputs, st->n_streams, &selected_stream, &selected_key)
		!= CLUSTER_RECMERGE_STREAMING_RECORD_READY)
		return NULL;
	if (selected_stream < 0 || selected_stream >= st->n_streams)
		return NULL;

	st->last_stream = selected_stream;
	st->streams[selected_stream].head_ready = false;
	if (thread_out)
		*thread_out = st->streams[selected_stream].thread_id;
	return st->streams[selected_stream].reader;
}

void
cluster_recovery_merge_end(ClusterRecoveryMergeState *st)
{
	int i;

	if (st == NULL)
		return;
	for (i = 0; i < st->n_streams; i++) {
		MergeStream *ms = &st->streams[i];

		if (ms->seg_fd >= 0)
			close(ms->seg_fd);
		if (ms->reader)
			XLogReaderFree(ms->reader);
	}
	pfree(st);
}

#else /* !USE_PGRAC_CLUSTER */

/* Disable-cluster build: this file compiles to nothing. */

#endif /* USE_PGRAC_CLUSTER */
