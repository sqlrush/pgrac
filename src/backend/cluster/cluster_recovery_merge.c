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
#include "access/xlogutils.h"
#include "cluster/cluster_control_root.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_external_fence.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_tier1.h" /* spec-6.14 D9 amend: claim-holder liveness poll */
#include "cluster/cluster_ir.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_recovery_plan.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_recovery_worker.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_startup_phase.h"
#include "cluster/cluster_thread_recovery_authority.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_wal_tail.h"
#include "postmaster/interrupt.h"
#include "postmaster/startup.h"				   /* spec-6.14 D9 amend: HandleStartupProcInterrupts */
#include "cluster/storage/cluster_shared_fs.h" /* spec-4.5a D4: capability gate */
#include "cluster_control_root_private.h"	   /* failed-writer input seal (S9P2-05) */
#include "lib/stringinfo.h"
#include "miscadmin.h" /* DataDir (authority marker path) */
#include "port/pg_crc32c.h"
#include "portability/instr_time.h"
#include "storage/fd.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "utils/errcodes.h"
#include "utils/resowner.h"
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

#define CLUSTER_RECOVERY_FENCE_PLAN_MAGIC UINT32_C(0x52465034) /* "RFP4" */

typedef struct ClusterRecoveryFenceOrigin {
	uint16 origin_thread;
	ClusterRecoveryDutyKey duty;
	ClusterControlRootSnapshot root_snapshot;
	ClusterControlRootReadToken root_token;
	ClusterFormationWitnessV1 *formation;
	PgracExternalFenceNeedSetV1 *needs;
	PgracExternalFenceAdmissionSetV1 *admissions;
	ClusterWalSourceRef tx_source; /* original retained source, never a new duty */
} ClusterRecoveryFenceOrigin;

struct ClusterRecoveryFencePlan {
	uint32 magic;
	int32 owner_pid;
	ResourceOwner owner;
	uint64 owner_boot;
	uint16 own_thread;
	uint16 origin_count;
	bool sealed;
	bool serial_held;
	bool committed;
	bool tx_inputs_bound;
	bool completed;
	bool tx_stale;
	uint8 reserved[2];
	int acquire_timeout_ms_snapshot;
	int merge_wait_timeout_ms_snapshot;
	uint64 merge_deadline_ns;
	char refusal_detail[512]; /* first original refusal; no authority */
	uint64 replay_thread_bitmap[2];
	uint64 foreign_origin_bitmap[2];
	XLogRecPtr start_lsn[CLUSTER_WAL_STATE_SLOT_COUNT + 1];
	ClusterRecoveryFenceOrigin origins[CLUSTER_WAL_STATE_SLOT_COUNT];
	ClusterRecoverySerialGuardSet serial_guards;
	ClusterWalRetentionPin *retention_pin;
};

typedef struct RecoveryFenceCfContext {
	PGPROC *proc;
	Latch *latch;
	ResourceOwner owner;
	ClusterWalRetentionPin *pin;
	int pid;
	uint64 boot;
	uint64 epoch;
	uint64 generation;
	bool recovery;
} RecoveryFenceCfContext;

static ClusterMergeEngage recovery_fence_project_wait(ClusterRecoveryFencePlan *plan,
													  XLogRecPtr own_redo, uint64 replay[2],
													  XLogRecPtr *start);

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

/* Volatile custody of the one object this process actually linked. No disk
 * claim, node id or restarted process can reconstruct this ownership. */
typedef struct MergeClaimCustody {
	ResourceOwner owner;
	pid_t pid;
	uint64 boot;
	uint64 serial;
	bool revoked;
	int fd;
	int directory_fd;
	struct stat file;
	struct stat directory;
	int peer_fd;
	bool peer_observed;
	struct stat peer_file;
	ClusterMergeClaimFile peer_image;
	ClusterMergeClaimFile image;
	char path[MAXPGPATH];
	char directory_path[MAXPGPATH];
} MergeClaimCustody;

static MergeClaimCustody merge_claim_custody = { .fd = -1, .directory_fd = -1, .peer_fd = -1 };
static uint64 merge_claim_serial;
static bool merge_claim_callback_registered;

static void
merge_claim_resource_release(ResourceReleasePhase phase, bool isCommit, bool isTopLevel, void *arg)
{
	(void)isCommit;
	(void)isTopLevel;
	(void)arg;
	/* Retain the disk claim and descriptor debt. Cleanup is never takeover. */
	if (phase == RESOURCE_RELEASE_BEFORE_LOCKS && merge_claim_custody.owner != NULL
		&& merge_claim_custody.owner == CurrentResourceOwner)
		merge_claim_custody.revoked = true;
}

static bool
merge_claim_same_file(const struct stat *a, const struct stat *b)
{
	return S_ISREG(a->st_mode) && S_ISREG(b->st_mode) && a->st_dev == b->st_dev
		&& a->st_ino == b->st_ino && a->st_uid == b->st_uid
		&& a->st_size == sizeof(ClusterMergeClaimFile) && b->st_size == a->st_size;
}

static bool
merge_claim_custody_current(void)
{
	MergeClaimCustody *c = &merge_claim_custody;
	struct stat held, named, directory, named_directory;
	char image[sizeof(ClusterMergeClaimFile) + 1];
	char path[MAXPGPATH];
	int n;

	if (!merge_claim_held || c->revoked)
		return false;
	if (cluster_shared_data_dir == NULL || c->image.node != cluster_node_id
		|| c->image.sysid != GetSystemIdentifier()) {
		c->revoked = true;
		return false;
	}
	n = snprintf(path, sizeof(path), "%s/global/pgrac_merge.claim", cluster_shared_data_dir);
	if (c->owner == NULL || c->owner != CurrentResourceOwner || c->pid != getpid()
		|| c->boot == 0 || c->boot != cluster_qvotec_get_self_incarnation()
		|| c->serial == 0 || c->serial != merge_claim_serial || c->fd < 0 || c->directory_fd < 0
		|| n < 0 || (size_t)n >= sizeof(path) || strcmp(path, c->path) != 0
		|| fstat(c->directory_fd, &directory) != 0
		|| lstat(c->directory_path, &named_directory) != 0
		|| !S_ISDIR(directory.st_mode) || !S_ISDIR(named_directory.st_mode)
		|| directory.st_dev != c->directory.st_dev || directory.st_ino != c->directory.st_ino
		|| named_directory.st_dev != c->directory.st_dev
		|| named_directory.st_ino != c->directory.st_ino
		|| fstat(c->fd, &held) != 0 || !merge_claim_same_file(&held, &c->file)
		|| fstatat(c->directory_fd, "pgrac_merge.claim", &named, AT_SYMLINK_NOFOLLOW) != 0
		|| !merge_claim_same_file(&held, &named)
		|| pg_pread(c->fd, image, sizeof(image), 0) != sizeof(c->image)
		|| memcmp(image, &c->image, sizeof(c->image)) != 0
		/* Descriptor I/O can yield. Recheck the original name and owner
		 * before consuming its bytes; CF serializes protocol mutations. */
		|| fstatat(c->directory_fd, "pgrac_merge.claim", &named, AT_SYMLINK_NOFOLLOW) != 0
		|| !merge_claim_same_file(&held, &named)
		|| lstat(c->directory_path, &named_directory) != 0
		|| !S_ISDIR(named_directory.st_mode)
		|| named_directory.st_dev != c->directory.st_dev
		|| named_directory.st_ino != c->directory.st_ino
		|| c->revoked || c->owner != CurrentResourceOwner || c->pid != getpid()
		|| c->boot != cluster_qvotec_get_self_incarnation()) {
		c->revoked = true;
		return false;
	}
	return true;
}

/* Use the original CF serialization domain only for claim metadata. Never
 * hold it while waiting on another claimant, scanning WAL or acquiring IR.
 * Startup uses the existing blocking CF owner, not LMON/LMS poll APIs. */
static bool merge_claim_waiter_current(void);

static bool
merge_claim_cf_context_current(const MergeClaimCustody *custody, bool owned, uint64 epoch,
							   uint64 generation, bool recovery)
{
	return MyProc != NULL && MyLatch != NULL && AmStartupProcess() && MyBackendType == B_STARTUP
		   && cluster_shared_config && cluster_enabled && cluster_controlfile_shared_authority
		   && !ShutdownRequestPending && CritSectionCount == 0 && !LWLockHeldByMe(ControlFileLock)
		   && InRecovery == recovery && epoch != 0 && generation != 0
		   && cluster_epoch_get_current() == epoch
		   && cluster_grd_redeclare_generation() == generation
		   && cluster_recovery_transport_components_current() && merge_claim_held == owned
		   && merge_claim_serial == custody->serial
		   && memcmp(custody, &merge_claim_custody, sizeof(*custody)) == 0
		   && (owned ? merge_claim_custody_current() : merge_claim_waiter_current());
}

/* A failed request retains cleanup, never claim authority. Finish only its
 * original registry obligation, outside native CF and claim mutation. The
 * original file custody must still qualify before another CF acquisition.
 * Author: SqlRush <sqlrush@gmail.com> */
static void
merge_claim_cf_wait(const ClusterCfAttemptObservation *attempt, const MergeClaimCustody *custody,
					bool owned, uint64 epoch, uint64 generation, bool recovery)
{
	ClusterCfRetirementSnapshot original;
	const char *reason = "UNSAFE_CF_WAIT";
	bool empty = attempt->owner_state_at_return == CLUSTER_LOCK_OWNER_EMPTY;
	bool empty_waited = false;
	PGPROC *proc = MyProc;
	Latch *latch = MyLatch;

	if (MyProc == NULL || MyLatch == NULL || CritSectionCount != 0
		|| LWLockHeldByMe(ControlFileLock) || cluster_cf_held(ShareLock))
		goto refuse;
	if (!empty
		&& (!cluster_cf_retirement_snapshot(ExclusiveLock, NULL, &original)
			|| original.cookie != attempt->control_owner_id
			|| original.registry.message.key.holder.request_id != attempt->request_id
			|| original.registry.message.key.holder.cluster_epoch != attempt->request_epoch)) {
		reason = "CF_WAIT_IDENTITY_OR_COMPLETION";
		goto refuse;
	}
	for (;;) {
		ClusterCfRetirementSnapshot current;
		bool held;

		HandleStartupProcInterrupts();
		CHECK_FOR_INTERRUPTS();
		reason = "CONTEXT_CHANGED_AFTER_WAIT_SLICE";
		if (MyProc != proc || MyLatch != latch
			|| !merge_claim_cf_context_current(custody, owned, epoch, generation, recovery))
			goto refuse;
		ResetLatch(MyLatch);
		held = cluster_cf_held(ExclusiveLock);
		if (!held && !cluster_cf_held(ShareLock)) {
			if (!empty && cluster_cf_release_completed(ExclusiveLock, original.cookie))
				return;
			if (empty) {
				if (empty_waited)
					return;
				/* No request was published. A wake only schedules requalification. */
				empty_waited = true;
				(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 20,
								WAIT_EVENT_CLUSTER_STARTUP_PHASE_3);
				continue;
			}
		}
		reason = "CF_WAIT_IDENTITY_OR_COMPLETION";
		if (empty || !held || cluster_cf_held(ShareLock)
			|| !cluster_cf_retirement_snapshot(ExclusiveLock, NULL, &current)
			|| current.cookie != original.cookie
			|| current.registry.handle.slot != original.registry.handle.slot
			|| current.registry.handle.generation != original.registry.handle.generation
			|| memcmp(&current.registry.owner, &original.registry.owner,
					  sizeof(current.registry.owner))
				   != 0
			|| memcmp(&current.registry.message.key, &original.registry.message.key,
					  sizeof(current.registry.message.key))
				   != 0)
			goto refuse;
		cluster_lock_owners_service_poll();
		cluster_cf_retirement_poll();
		if (cluster_cf_held(ExclusiveLock))
			(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 20,
							WAIT_EVENT_CLUSTER_STARTUP_PHASE_3);
	}

refuse:
	ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
					errmsg("recovery claim cannot continue its control-file wait"),
					errdetail("PGRAC_FAMILY=STARTUP_CONTROL consumer=merge_claim operation=%s "
							  "reason=%s cookie=" UINT64_FORMAT " request=" UINT64_FORMAT,
							  owned ? "release" : "publish", reason, attempt->control_owner_id,
							  attempt->request_id)));
}

static uint64
merge_claim_cf_enter(void)
{
	MergeClaimCustody custody;
	bool owned = merge_claim_held, recovery = InRecovery;
	uint64 epoch = cluster_epoch_get_current();
	uint64 generation = cluster_grd_redeclare_generation();
	uint64 cookie;
	bool logged = false;

	memcpy(&custody, &merge_claim_custody, sizeof(custody));
	for (;;) {
		ClusterCfAttemptObservation attempt;
		uint64 before = cluster_cf_attempt_sequence();

		if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
							errmsg("recovery claim CF serialization is unavailable")));
		if (cluster_cf_lock(ExclusiveLock))
			break;
		if (!cluster_cf_last_attempt_since(before, &attempt)
			|| !cluster_cf_attempt_wait_slice(&attempt) || attempt.mode != ExclusiveLock)
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
							errmsg("recovery claim CF serialization is unavailable"),
							errdetail("PGRAC_FAMILY=STARTUP_CONTROL consumer=merge_claim "
									  "reason=CF_ACQUIRE_REFUSAL")));
		if (!logged) {
			logged = true;
			ereport(LOG,
					(errmsg("recovery claim is waiting for control-file serialization"),
					 errdetail("PGRAC_FAMILY=CF_OWNER_WAIT consumer=merge_claim operation=%s "
							   "reason=WAIT_SLICE cookie=" UINT64_FORMAT " request=" UINT64_FORMAT,
							   owned ? "release" : "publish", attempt.control_owner_id,
							   attempt.request_id)));
		}
		merge_claim_cf_wait(&attempt, &custody, owned, epoch, generation, recovery);
	}
	cookie = cluster_cf_owner_cookie(ExclusiveLock);
	if (cookie == 0 || !cluster_cf_held_is_clusterwide(ExclusiveLock)) {
		(void)cluster_cf_unlock_confirmed(ExclusiveLock);
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
					errmsg("recovery claim requires clusterwide CF ownership")));
	}
	/* The new acquisition can itself yield. The old completion did not
	 * preserve file custody or authorize any mutation under this new cookie. */
	if (logged && !merge_claim_cf_context_current(&custody, owned, epoch, generation, recovery))
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						errmsg("recovery claim context changed during control-file reacquisition"),
						errdetail("PGRAC_FAMILY=STARTUP_CONTROL consumer=merge_claim operation=%s "
								  "reason=CONTEXT_CHANGED_AFTER_REACQUIRE cookie=" UINT64_FORMAT,
								  owned ? "release" : "publish", cookie)));
	return cookie;
}

static void
merge_claim_cf_exit(uint64 cookie)
{
	if (cookie == 0 || cluster_cf_owner_cookie(ExclusiveLock) != cookie
		|| cluster_cf_unlock_confirmed(ExclusiveLock) != CLUSTER_CF_RELEASE_CONFIRMED) {
		merge_claim_custody.revoked = true;
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
					errmsg("recovery claim CF release was not confirmed")));
	}
}

static bool
merge_claim_try_publish(const char *tmp, const char *claim, const char *gdir)
{
	uint64 cookie;
	struct stat existing;
	volatile bool published = false;
	volatile int saved_errno = 0;

	/* Observation only: a visible claim means peer wait, not a CF-X retry
	 * on every tick. The actual link still runs under CF and cannot adopt. */
	if (fstatat(merge_claim_custody.directory_fd, "pgrac_merge.claim", &existing,
				AT_SYMLINK_NOFOLLOW) == 0) {
		errno = EEXIST;
		return false;
	}
	if (errno != ENOENT)
		return false;
	cookie = merge_claim_cf_enter();
	PG_TRY();
	{
		MergeClaimCustody *c = &merge_claim_custody;

		if (c->revoked || c->owner != CurrentResourceOwner || c->pid != getpid()
			|| c->boot != cluster_qvotec_get_self_incarnation())
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						errmsg("recovery claim original owner changed before publication")));
		if (link(tmp, claim) == 0) {
			merge_claim_held = true; /* Debt precedes any throwing operation. */
			if (!merge_claim_custody_current())
				ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
							errmsg("new recovery claim lost its original file identity")));
			fsync_fname(gdir, true);
			(void)unlink(tmp);
			if (!merge_claim_custody_current()
				|| cluster_cf_owner_cookie(ExclusiveLock) != cookie
				|| !cluster_cf_held_is_clusterwide(ExclusiveLock))
				ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
							errmsg("recovery claim publication authority changed during synchronization")));
			published = true;
		} else
			saved_errno = errno;
	}
	PG_FINALLY();
	{
		merge_claim_cf_exit(cookie);
	}
	PG_END_TRY();
	errno = saved_errno;
	return published;
}

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

/* Original wait observation, never the remote executor's ownership. The
 * directory is borrowed from this waiting actor's existing staging custody.
 * Author: SqlRush <sqlrush@gmail.com> */
static bool
merge_claim_waiter_current(void)
{
	MergeClaimCustody *c = &merge_claim_custody;
	struct stat directory, named_directory, file;
	char path[MAXPGPATH];

	return !c->revoked && merge_claim_regime_configured()
		&& c->owner != NULL && c->owner == CurrentResourceOwner && c->pid == getpid()
		&& c->boot != 0 && c->boot == cluster_qvotec_get_self_incarnation()
		&& c->serial != 0 && c->serial == merge_claim_serial
		&& c->image.node == cluster_node_id && c->image.sysid == GetSystemIdentifier()
		&& merge_claim_path(path, sizeof(path)) == 0 && strcmp(path, c->path) == 0
		&& c->fd >= 0 && fstat(c->fd, &file) == 0 && merge_claim_same_file(&file, &c->file)
		&& c->directory_fd >= 0 && fstat(c->directory_fd, &directory) == 0
		&& lstat(c->directory_path, &named_directory) == 0
		&& S_ISDIR(directory.st_mode) && S_ISDIR(named_directory.st_mode)
		&& directory.st_dev == c->directory.st_dev && directory.st_ino == c->directory.st_ino
		&& directory.st_uid == c->directory.st_uid
		&& named_directory.st_dev == directory.st_dev && named_directory.st_ino == directory.st_ino
		&& named_directory.st_uid == directory.st_uid;
}

static void
merge_claim_peer_refuse(const char *stage)
{
	merge_claim_custody.revoked = true;
	ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
					errmsg("shared-regime recovery claim changed while waiting"),
					errdetail("Original read-only claim observation refused at %s.", stage),
					errhint("Retain the claim and its recovery inputs; E1 does not adopt a replacement.")));
}

/* False is only an absent name: retry the original CF-serialized link.
 * Keep the old read descriptor until actual local publication or exit; a
 * same-byte replacement is never a new observation of the original file. */
static bool
merge_claim_peer_observe(ClusterMergeClaimFile *out)
{
	MergeClaimCustody *c = &merge_claim_custody;
	struct stat named, held;
	char bytes[sizeof(ClusterMergeClaimFile) + 1];
	ClusterMergeClaimVerdict verdict;
	ssize_t n;

	memset(out, 0, sizeof(*out));
	if (!merge_claim_waiter_current())
		merge_claim_peer_refuse("waiter");
	if (fstatat(c->directory_fd, "pgrac_merge.claim", &named, AT_SYMLINK_NOFOLLOW) != 0) {
		if (errno == ENOENT)
			return false;
		merge_claim_peer_refuse("name");
	}
	if (!S_ISREG(named.st_mode))
		merge_claim_peer_refuse("file_type");
	if (c->peer_fd < 0) {
		c->peer_fd = OpenTransientFile(c->path,
			O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK | PG_BINARY);
		if (c->peer_fd < 0) {
			if (errno == ENOENT)
				return false;
			merge_claim_peer_refuse("open");
		}
		if (fstat(c->peer_fd, &held) != 0 || !merge_claim_same_file(&held, &named))
			merge_claim_peer_refuse("opened_file");
		c->peer_file = held;
	}
	if (fstat(c->peer_fd, &held) != 0 || !merge_claim_same_file(&held, &c->peer_file)
		|| !merge_claim_same_file(&held, &named))
		merge_claim_peer_refuse("original_file");
	n = pg_pread(c->peer_fd, bytes, sizeof(bytes), 0);
	if (n < 0)
		merge_claim_peer_refuse("read");
	verdict = cluster_merge_claim_classify(bytes, (size_t)n, c->image.sysid);
	if (verdict != CLUSTER_MERGE_CLAIM_VALID) {
		c->revoked = true;
		ereport(FATAL,
				(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
				 errmsg("shared-regime recovery claim \"%s\" is not valid (verdict %d)",
						c->path, (int)verdict),
				 errhint("Retain the claim and its recovery inputs; automatic takeover is unavailable.")));
	}
	if (c->peer_observed && memcmp(bytes, &c->peer_image, sizeof(c->peer_image)) != 0)
		merge_claim_peer_refuse("original_bytes");
	if (!merge_claim_waiter_current() || fstat(c->peer_fd, &held) != 0
		|| !merge_claim_same_file(&held, &c->peer_file))
		merge_claim_peer_refuse("after_read");
	if (!c->peer_observed) {
		memcpy(&c->peer_image, bytes, sizeof(c->peer_image));
		c->peer_observed = true;
	}
	if (fstatat(c->directory_fd, "pgrac_merge.claim", &named, AT_SYMLINK_NOFOLLOW) != 0) {
		if (errno == ENOENT)
			return false;
		merge_claim_peer_refuse("readback_name");
	}
	if (!merge_claim_same_file(&held, &named) || !merge_claim_waiter_current())
		merge_claim_peer_refuse("readback_identity");
	*out = c->peer_image;
	return true;
}

/* Retire only the old read handle after a real original local publication.
 * It never unlinks or adopts the peer object. Uncertain close keeps debt. */
static void
merge_claim_peer_close(void)
{
	MergeClaimCustody *c = &merge_claim_custody;
	struct stat held;
	char bytes[sizeof(ClusterMergeClaimFile) + 1];

	if (c->peer_fd < 0)
		return;
	if (!c->peer_observed || !merge_claim_waiter_current()
		|| fstat(c->peer_fd, &held) != 0 || !merge_claim_same_file(&held, &c->peer_file)
		|| pg_pread(c->peer_fd, bytes, sizeof(bytes), 0) != sizeof(c->peer_image)
		|| memcmp(bytes, &c->peer_image, sizeof(c->peer_image)) != 0
		|| fstat(c->peer_fd, &held) != 0 || !merge_claim_same_file(&held, &c->peer_file)
		|| !merge_claim_waiter_current())
		merge_claim_peer_refuse("retire_read_handle");
	if (CloseTransientFile(c->peer_fd) != 0) {
		c->revoked = true;
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not close original recovery claim observation: %m")));
	}
	c->peer_fd = -1;
	c->peer_observed = false;
	if (!merge_claim_custody_current())
		merge_claim_peer_refuse("published_owner");
}

/*
 * cluster_recovery_merge_claim_acquire_blocking -- see header.
 */
void
cluster_recovery_merge_claim_acquire_blocking(void)
{
	ClusterMergeClaimFile f = { 0 };
	MergeClaimCustody *custody = &merge_claim_custody;
	char claim[MAXPGPATH];
	char tmp[MAXPGPATH];
	char gdir[MAXPGPATH];
	uint64 sysid;
	int fd;
	int ret;
	long dead_ms = 0;
	long deadband_ms;
	int32 wait_holder = -1;

	if (merge_claim_held) {
		if (!merge_claim_custody_current())
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						errmsg("shared-regime recovery claim original owner is no longer current")));
		return;
	}
	if (!merge_claim_regime_configured())
		return;
	if (CurrentResourceOwner == NULL || cluster_qvotec_get_self_incarnation() == 0
		|| merge_claim_serial == UINT64_MAX)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
					errmsg("shared-regime recovery claim has no current owner identity")));


	if (merge_claim_path(claim, sizeof(claim)) < 0)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						errmsg("shared-regime recovery claim path too long")));
	ret = snprintf(tmp, sizeof(tmp), "%s.n%d.p%d.b%llu.tmp", claim, cluster_node_id,
		(int)getpid(), (unsigned long long)cluster_qvotec_get_self_incarnation());
	if (ret < 0 || (size_t)ret >= sizeof(tmp))
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						errmsg("shared-regime recovery claim tmp path too long")));
	ret = snprintf(gdir, sizeof(gdir), "%s/global", cluster_shared_data_dir);
	if (ret < 0 || (size_t)ret >= sizeof(gdir))
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						errmsg("shared-regime recovery claim dir path too long")));

	/*
	 * The claim can be the FIRST writer under <shared>/global (a shared
	 * per-thread WAL layout needs no other global/ file before its first
	 * crash boot -- the t/248 shape), so ensure the directory exists
	 * rather than FATALing the whole recovery on ENOENT.
	 */
	if (MakePGDirectory(gdir) < 0 && errno != EEXIST)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not create shared claim directory \"%s\": %m", gdir)));

	sysid = GetSystemIdentifier();
	cluster_merge_claim_build(&f, cluster_node_id, sysid);

	/* Durable per-node tmp image; link() below publishes it atomically. */
	fd = OpenTransientFile(tmp, O_CREAT | O_EXCL | O_RDWR | O_NOFOLLOW | O_CLOEXEC | PG_BINARY);
	if (fd < 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not create recovery claim tmp file \"%s\": %m", tmp)));
	if (write(fd, &f, sizeof(f)) != sizeof(f) || pg_fsync(fd) != 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not write recovery claim tmp file \"%s\": %m", tmp)));
	memset(custody, 0, sizeof(*custody));
	custody->fd = fd;
	custody->peer_fd = -1;
	custody->directory_fd = OpenTransientFile(gdir, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	custody->owner = CurrentResourceOwner;
	custody->pid = getpid();
	custody->boot = cluster_qvotec_get_self_incarnation();
	custody->serial = ++merge_claim_serial;
	custody->image = f;
	strlcpy(custody->path, claim, sizeof(custody->path));
	strlcpy(custody->directory_path, gdir, sizeof(custody->directory_path));
	if (custody->directory_fd < 0 || fstat(fd, &custody->file) != 0
		|| fstat(custody->directory_fd, &custody->directory) != 0
		|| !merge_claim_same_file(&custody->file, &custody->file))
		ereport(FATAL, (errcode_for_file_access(), errmsg("could not retain recovery claim identity: %m")));
	if (!merge_claim_callback_registered) {
		RegisterResourceReleaseCallback(merge_claim_resource_release, NULL);
		merge_claim_callback_registered = true;
	}

	deadband_ms = (long)cluster_cssd_heartbeat_interval_ms
				  * (long)Max(cluster_cssd_dead_deadband_factor, 1);
	if (deadband_ms < 5000)
		deadband_ms = 5000;

	for (;;) {
		if (custody->revoked || custody->owner != CurrentResourceOwner
			|| custody->pid != getpid() || custody->boot != cluster_qvotec_get_self_incarnation())
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						errmsg("recovery claim original owner changed while waiting")));
		if (custody->peer_fd >= 0 && !merge_claim_waiter_current())
			merge_claim_peer_refuse("waiting_context");
		if (merge_claim_try_publish(tmp, claim, gdir)) {
			merge_claim_peer_close();
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

		/* Observe the same original object without remote-owner authority. */
		{
			ClusterMergeClaimFile cur;

			if (!merge_claim_peer_observe(&cur))
				continue;

			if (cur.node == cluster_node_id)
				ereport(FATAL,
						(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						 errmsg("shared-regime recovery claim belongs to a previous owner on node %d",
								cluster_node_id),
						 errdetail("E1 recovery cannot adopt a persistent claim using a node identifier."),
						 errhint("Retain the claim and its recovery inputs; automatic takeover is unavailable.")));

			/* Held by a peer: wait for its release, watching its liveness. */
			if (cur.node != wait_holder) {
				wait_holder = cur.node;
				dead_ms = 0;
				ereport(LOG, (errmsg("shared-regime crash recovery already claimed by node %d; "
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
					 errhint("Retain the claim and its recovery inputs; E1 does not adopt a dead owner's claim.")));

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
	return merge_claim_custody_current();
}

uint64
cluster_recovery_merge_claim_owner_v1(void)
{
	return merge_claim_custody_current() ? merge_claim_custody.serial : 0;
}

bool
cluster_recovery_merge_claim_owner_current_nowait_v1(uint64 owner)
{
	MergeClaimCustody *c = &merge_claim_custody;
	char path[MAXPGPATH];
	int n;

	/* A foreign old stamp must not revoke another valid original creator. */
	if (owner == 0 || !merge_claim_held || c->revoked
		|| owner != c->serial || owner != merge_claim_serial)
		return false;
	n = cluster_shared_data_dir == NULL ? -1
		: snprintf(path, sizeof(path), "%s/global/pgrac_merge.claim", cluster_shared_data_dir);
	if (c->owner == NULL || c->owner != CurrentResourceOwner || c->pid != getpid()
		|| c->boot == 0 || c->boot != cluster_qvotec_get_self_incarnation()
		|| c->fd < 0 || c->directory_fd < 0 || c->image.node != cluster_node_id
		|| c->image.sysid != GetSystemIdentifier()
		|| n < 0 || (size_t)n >= sizeof(path) || strcmp(path, c->path) != 0) {
		c->revoked = true;
		return false;
	}
	return true;
}

/*
 * cluster_recovery_merge_claim_release_if_held -- see header.
 */
void
cluster_recovery_merge_claim_release_if_held(void)
{
	MergeClaimCustody *custody = &merge_claim_custody;
	uint64 cookie;

	if (!merge_claim_held)
		return;
	cookie = merge_claim_cf_enter();
	PG_TRY();
	{
		if (!merge_claim_custody_current()
			|| cluster_cf_owner_cookie(ExclusiveLock) != cookie
			|| !cluster_cf_held_is_clusterwide(ExclusiveLock))
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						errmsg("cannot release recovery claim without its original owner and file identity")));
		/* All claim publication/retirement uses this same CF domain. The
		 * original held directory also prevents namespace redirection. */
		if (unlinkat(custody->directory_fd, "pgrac_merge.claim", 0) != 0)
			ereport(FATAL, (errcode_for_file_access(), errmsg("could not release recovery claim: %m")));
		if (pg_fsync(custody->directory_fd) != 0)
			ereport(ERROR, (errcode_for_file_access(), errmsg("could not synchronize released recovery claim: %m")));
		if (CloseTransientFile(custody->fd) != 0)
			ereport(FATAL, (errcode_for_file_access(), errmsg("could not close released recovery claim: %m")));
		custody->fd = -1;
		if (CloseTransientFile(custody->directory_fd) != 0)
			ereport(FATAL, (errcode_for_file_access(), errmsg("could not close released claim directory: %m")));
		custody->directory_fd = -1;
	}
	PG_FINALLY();
	{
		merge_claim_cf_exit(cookie);
	}
	PG_END_TRY();
	merge_claim_held = false;
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
 * cluster_recovery_merge_project_readonly -- §3.1 candidate projection and
 * §3.2 53RA3 gate.  This is not replay authority: only the P4 fence-plan
 * producer below may expose its bitmap to xlogrecovery.
 */
static ClusterMergeEngage
recovery_merge_project_attempt(uint16 own_thread, XLogRecPtr own_redo, uint64 out_bitmap[2],
							   XLogRecPtr *out_start, ClusterCfAttemptObservation *retry)
{
	ClusterRecoveryPlan plan;
	ClusterRecoveryWorkerPool pool;
	bool have_pool;
	uint16 tid;
	StringInfoData blockers;

	if (retry != NULL)
		MemSet(retry, 0, sizeof(*retry));

	if (!cluster_merged_recovery)
		return CLUSTER_MERGE_NO_DISABLED;
	if (cluster_wal_threads_dir == NULL || cluster_wal_threads_dir[0] == '\0'
		|| own_thread == XLP_THREAD_ID_LEGACY)
		return CLUSTER_MERGE_NO_NOT_CONFIGURED;
	if (!cluster_recovery_plan_snapshot(&plan)) {
		ereport(LOG, (errmsg("cold recovery plan input refused"),
					  errdetail("predicate=PLAN_SNAPSHOT_UNAVAILABLE own_thread=%u",
								(unsigned)own_thread)));
		return CLUSTER_MERGE_NO_NO_PLAN;
	}
	if (plan.failed) {
		ereport(LOG, (errmsg("cold recovery plan input refused"),
					  errdetail("predicate=PLAN_FAILED own_thread=%u plan_thread=%u "
								"generated=%d threads_scanned=%u generated_at=" INT64_FORMAT,
								(unsigned)own_thread, (unsigned)plan.own_thread,
								plan.generated ? 1 : 0, (unsigned)plan.threads_scanned,
								plan.generated_at)));
		return CLUSTER_MERGE_NO_NO_PLAN;
	}
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
		ClusterControlRootIdentity root_identity;
		ClusterControlRootSnapshot root_snapshot;
		ClusterControlRootReadToken root_token;
		ClusterControlRootResult root_result;
		bool root_ok;
		bool seal_pending;
		uint64 before;
		ClusterCfAttemptObservation attempt;

		if (!is_candidate && tid != own_thread)
			continue;

		out_bitmap[(tid - 1) / 64] |= ((uint64)1 << ((tid - 1) % 64));

		if (tid == own_thread) {
			out_start[tid] = own_redo;
			continue; /* own thread: gate items below are peer-only */
		}

		/* The canonical root comes first: in the shared profile it decides
		 * what the candidate still needs before it can be proven. PRE2's
		 * reader physically validates the selected generation claim; do not
		 * reconstruct the legacy claim CRC over this qualified identity.
		 * Author: SqlRush <sqlrush@gmail.com> */
		MemSet(&root_identity, 0, sizeof(root_identity));
		MemSet(&root_snapshot, 0, sizeof(root_snapshot));
		before = retry != NULL ? cluster_cf_attempt_sequence() : 0;
		root_result = cluster_control_root_lookup_owner_by_node_runtime(
			(int)tid - 1, &root_identity, &root_snapshot, &root_token);
		if (retry != NULL && blockers.len == 0
			&& root_result == CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE
			&& cluster_cf_last_attempt_since(before, &attempt) && attempt.mode == ShareLock
			&& cluster_cf_attempt_wait_slice(&attempt)) {
			*retry = attempt;
			pfree(blockers.data);
			return CLUSTER_MERGE_NO_NO_PLAN;
		}
		root_ok = (root_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
				   || root_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
				  && cluster_recovery_duty_key_valid_for_claim(&root_identity, cluster_shared_config)
				  && cluster_recovery_duty_key_compare_for_claim(
						 &root_identity, &root_snapshot.identity, cluster_shared_config)
						 == CLUSTER_RECOVERY_DUTY_COMPARE_EXACT;

		/*
		 * PGRAC (S9P2-05): in the shared profile a generation another
		 * founder already recovered leaves the merge set, and one whose
		 * writer crashed before anyone sealed its input (OPEN), or whose
		 * sealer failed before validating its tail, waits for the cold
		 * preflight to seal it: it keeps its place without a start or a
		 * source proof, and its stream is checked only once a validated
		 * tail exists.  The preflight projects again after sealing.
		 * Author: SqlRush <sqlrush@gmail.com>
		 */
		seal_pending = false;
		if (root_ok && cluster_shared_config) {
			if (root_snapshot.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE) {
				out_bitmap[(tid - 1) / 64] &= ~((uint64)1 << ((tid - 1) % 64));
				continue;
			}
			seal_pending
				= root_snapshot.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
				  || (root_snapshot.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
					  && !cluster_recovery_worker_root_anchor_valid(&root_snapshot));
		}

		/*
		 * Candidate stream must validate OK.  Use the worker verdict if
		 * present; NONE means the workers did not finish in time, so
		 * re-validate inline (Q6).  SKIPPED is fatal -- the peer was
		 * alive, so the cold premise broke.  PGRAC: the shared profile
		 * always re-validates inline: a worker verdict predates the seal.
		 */
		if (!seal_pending || cluster_recovery_worker_root_anchor_valid(&root_snapshot)) {
			ClusterRecoveryStreamVerdict v
				= have_pool ? (ClusterRecoveryStreamVerdict)pool.stream_verdict[tid]
							: CLUSTER_RECOVERY_STREAM_NONE;

			if (cluster_shared_config ? v != CLUSTER_RECOVERY_STREAM_SKIPPED
									  : v == CLUSTER_RECOVERY_STREAM_NONE)
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

		/* The canonical physical retention floor is not a redo start.  The
		 * shared profile replays from the same-token native redo and hands
		 * the retained prefix to the typed cold plan as ancestry only
		 * (cluster_cold_recovery_plan.c); an unproven anchor still refuses.
		 * The legacy profile keeps its original checkpoint semantics. */
		if (!root_ok) {
			appendStringInfo(&blockers, "%sthread %u canonical root unreadable (result %d)",
							 blockers.len ? "; " : "", (unsigned)tid, (int)root_result);
		} else {
			XLogRecPtr redo = root_snapshot.checkpoint_lower_lsn;

			if (root_snapshot.checkpoint_lower_lsn == 0)
				appendStringInfo(&blockers, "%sthread %u has no checkpoint redo start",
								 blockers.len ? "; " : "", (unsigned)tid);
			if ((root_snapshot.root_flags & CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF) != 0)
				appendStringInfo(&blockers, "%sthread %u ran with full_page_writes=off",
								 blockers.len ? "; " : "", (unsigned)tid);
			if (seal_pending) {
				out_start[tid] = InvalidXLogRecPtr;
				continue;
			}
			if (cluster_shared_config) {
				ClusterWalSourceRef source;

				before = retry != NULL ? cluster_cf_attempt_sequence() : 0;
				root_result = cluster_control_root_recovery_source_v1(&root_snapshot, &root_token,
																	  &source, &redo);
				if (retry != NULL && blockers.len == 0
					&& root_result == CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE
					&& cluster_cf_last_attempt_since(before, &attempt) && attempt.mode == ShareLock
					&& cluster_cf_attempt_wait_slice(&attempt)) {
					*retry = attempt;
					pfree(blockers.data);
					return CLUSTER_MERGE_NO_NO_PLAN;
				}
				if (root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY || redo == 0
					|| redo < root_snapshot.checkpoint_lower_lsn
					|| redo > root_snapshot.validated_tail_lsn_exclusive) {
					appendStringInfo(&blockers,
									 "%sthread %u native checkpoint input is unproven (result %d)",
									 blockers.len ? "; " : "", (unsigned)tid, (int)root_result);
					continue;
				}
			}
			out_start[tid] = redo;
		}
	}

	if (blockers.len > 0)
		ereport(FATAL,
				(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
				 errmsg("merged k-way recovery refused"), errdetail("%s.", blockers.data),
				 errhint("%s", cluster_shared_config
								   ? "Preserve all original thread WAL and shared configuration. "
									 "Shared mode requires merged recovery; do not disable it."
								   : "Resolve the shared WAL storage / configuration, or set "
									 "cluster.merged_recovery=off to recover this node's own "
									 "stream only (a crashed peer's committed WAL will not be "
									 "recovered).")));
	pfree(blockers.data);
	return CLUSTER_MERGE_ENGAGE;
}

static ClusterMergeEngage
cluster_recovery_merge_project_readonly(uint16 own_thread, XLogRecPtr own_redo,
										uint64 out_bitmap[2], XLogRecPtr *out_start)
{
	return recovery_merge_project_attempt(own_thread, own_redo, out_bitmap, out_start, NULL);
}

/*
 * A cold crash that needs another thread merged: the same conditions under
 * which cluster_recovery_merge_project_readonly goes on to its gates.
 */
static bool
recovery_merge_cold_merge_needed(uint16 own_thread)
{
	ClusterRecoveryPlan plan;

	return cluster_merged_recovery && cluster_wal_threads_dir != NULL
		   && cluster_wal_threads_dir[0] != '\0' && own_thread != XLP_THREAD_ID_LEGACY
		   && cluster_recovery_plan_snapshot(&plan) && !plan.failed && plan.n_crashed_candidate > 0
		   && plan.n_alive == 0;
}

static bool
recovery_fence_plan_valid(const ClusterRecoveryFencePlan *plan)
{
	return plan != NULL && plan->magic == CLUSTER_RECOVERY_FENCE_PLAN_MAGIC
		   && plan->owner_pid == MyProcPid && plan->own_thread >= 1
		   && plan->own_thread <= CLUSTER_WAL_STATE_SLOT_COUNT
		   && plan->origin_count <= CLUSTER_WAL_STATE_SLOT_COUNT;
}

static void
recovery_fence_plan_release_members(ClusterRecoveryFencePlan *plan)
{
	uint16 i;

	if (plan == NULL)
		return;
	for (i = plan->origin_count; i > 0; i--) {
		ClusterRecoveryFenceOrigin *origin = &plan->origins[i - 1];

		cluster_external_fence_admission_set_release(&origin->admissions);
		cluster_external_fence_need_set_release(&origin->needs);
		cluster_formation_witness_destroy(&origin->formation);
	}
}

static void
pg_attribute_noreturn() recovery_fence_unavailable(int32 node_id, uint64 incarnation, int verdict,
												   int reason, const char *stage, const char *predicate)
{
	ereport(FATAL,
			(errcode(ERRCODE_CLUSTER_EXTERNAL_FENCE_UNAVAILABLE),
			 errmsg("external write exclusion is not proven for node %d incarnation " UINT64_FORMAT,
					node_id, incarnation),
			 errdetail("Cold recovery external-fence stage %s returned verdict %d, reason %d; predicate=%s.",
					   stage, verdict, reason, predicate != NULL ? predicate : "NOT_EXPORTED"),
			 errhint("Restore the configured root fencing provider and retry; do not disable "
					 "the fence or force recovery.")));
	pg_unreachable();
}

static int
recovery_fence_plan_find_origin(const ClusterRecoveryFencePlan *plan, uint16 origin_thread)
{
	uint16 i;

	for (i = 0; i < plan->origin_count; i++) {
		if (plan->origins[i].origin_thread == origin_thread)
			return (int)i;
	}
	return -1;
}

/*
 * PGRAC (S9P2-05): the input of a fenced origin is either sealed with its
 * validated tail, or -- shared profile only -- still to be sealed by the
 * cold preflight: an OPEN generation (its writer crashed and nobody sealed
 * it) that has a claim and a checkpoint, or a sealed one whose sealer
 * failed before the tail.  Author: SqlRush <sqlrush@gmail.com>
 */
static bool
recovery_fence_origin_input_acceptable(const ClusterControlRootSnapshot *snapshot,
									   uint32 required_flags)
{
	const uint32 sealable
		= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID;

	if (snapshot->lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
		&& (snapshot->root_flags & required_flags) == required_flags)
		return true;
	if (!cluster_shared_config)
		return false;
	if (snapshot->lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN)
		return (snapshot->root_flags & sealable) == sealable;
	return snapshot->lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
		   && (snapshot->root_flags & ~CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF) == sealable;
}

static bool
recovery_fence_origin_unsealed(const ClusterControlRootSnapshot *snapshot)
{
	return snapshot->lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
		   || (snapshot->root_flags & CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID) == 0;
}

static void
pg_attribute_noreturn()
	recovery_fence_seal_failed(ClusterRecoveryFencePlan *plan, const ClusterRecoveryDutyKey *duty,
							   const char *transition, int result)
{
	int32 node_id = duty->origin_node_id;
	uint64 incarnation = duty->origin_owner_incarnation;

	recovery_fence_plan_release_members(plan);
	MemSet(plan, 0, sizeof(*plan));
	pfree(plan);
	ereport(FATAL,
			(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
			 errmsg("could not seal the failed writer input of node %d incarnation " UINT64_FORMAT,
					node_id, incarnation),
			 errdetail("The %s transition of its control root returned result %d; no page was "
					   "changed.",
					   transition, result),
			 errhint("Retry startup.  A generation sealed for recovery stays sealed until it is "
					 "recovered; preserve every thread's WAL.")));
	pg_unreachable();
}

static void
pg_attribute_noreturn()
	recovery_fence_seal_refused(ClusterRecoveryFencePlan *plan, const char *detail)
{
	recovery_fence_plan_release_members(plan);
	MemSet(plan, 0, sizeof(*plan));
	pfree(plan);
	ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
					errmsg("merged k-way recovery refused"), errdetail("%s", detail),
					errhint("Retry startup; preserve every thread's WAL.")));
	pg_unreachable();
}

/* This deadline belongs only to this Startup plan, before its first write.
 * Release budgets remain independent. Zero permits one complete attempt.
 * Author: SqlRush <sqlrush@gmail.com> */
static bool
recovery_fence_monotonic_ns(uint64 *out)
{
	struct timespec now;

	if (clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0 || now.tv_nsec < 0
		|| now.tv_nsec >= NS_PER_S || (uint64)now.tv_sec > (UINT64_MAX - NS_PER_S) / NS_PER_S)
		return false;
	*out = (uint64)now.tv_sec * NS_PER_S + (uint64)now.tv_nsec;
	return true;
}

static void
recovery_fence_deadline_begin(ClusterRecoveryFencePlan *plan)
{
	uint64 now;
	uint64 budget;

	plan->merge_wait_timeout_ms_snapshot
		= cluster_shared_config ? cluster_recovery_merge_wait_timeout : 0;
	if (plan->merge_wait_timeout_ms_snapshot < 0 || plan->merge_wait_timeout_ms_snapshot > 600000)
		recovery_fence_seal_refused(plan, "stage=merge_deadline reason=INVALID_ENTRY_BUDGET");
	if (plan->merge_wait_timeout_ms_snapshot == 0)
		return;
	budget = (uint64)plan->merge_wait_timeout_ms_snapshot * NS_PER_MS;
	if (!recovery_fence_monotonic_ns(&now) || now > UINT64_MAX - budget)
		recovery_fence_seal_refused(plan,
									"stage=merge_deadline reason=MONOTONIC_CLOCK_UNAVAILABLE");
	plan->merge_deadline_ns = now + budget;
}

static bool
recovery_fence_deadline_current(const ClusterRecoveryFencePlan *plan)
{
	uint64 now;

	return plan->merge_wait_timeout_ms_snapshot == 0
		   || (recovery_fence_monotonic_ns(&now) && now < plan->merge_deadline_ns);
}

static void
recovery_fence_deadline_require(ClusterRecoveryFencePlan *plan)
{
	if (!recovery_fence_deadline_current(plan))
		recovery_fence_seal_refused(plan,
									"stage=merge_deadline reason=EXPIRED_OR_CLOCK_UNAVAILABLE");
}

/* Round a wait down to whole milliseconds; its owner must not outlive the
 * original absolute budget. Never apply this to reverse cleanup. */
static int
recovery_fence_acquire_budget(const ClusterRecoveryFencePlan *plan, int owner_budget_ms)
{
	uint64 now;
	uint64 remaining_ms;

	if (plan->merge_wait_timeout_ms_snapshot == 0)
		return owner_budget_ms;
	if (!recovery_fence_monotonic_ns(&now) || now >= plan->merge_deadline_ns)
		return 0;
	remaining_ms = (plan->merge_deadline_ns - now) / NS_PER_MS;
	return (int)Min((uint64)owner_budget_ms, remaining_ms);
}

static int
recovery_fence_acquire_budget_required(ClusterRecoveryFencePlan *plan, int owner_budget_ms)
{
	int budget = recovery_fence_acquire_budget(plan, owner_budget_ms);

	if (budget <= 0)
		recovery_fence_seal_refused(plan,
									"stage=merge_deadline reason=NO_WHOLE_MILLISECOND_REMAINING");
	return budget;
}

/*
 * Seal one fenced origin's input, as the online recovery worker does before
 * it takes replay owners: OPEN -> RECOVERY_REQUIRED, then the validated
 * tail, each a compare-and-swap on the root the plan read.  The publishers
 * take their own WALR/IR/CF and return only after confirmed release.  The
 * origin then carries the sealed root and its read token.
 */
static void
recovery_fence_origin_seal(ClusterRecoveryFencePlan *plan, ClusterRecoveryFenceOrigin *origin,
						   uint32 required_flags)
{
	ClusterRecoverySerialRequest request;
	ClusterControlRootSnapshot snapshot = origin->root_snapshot;
	ClusterControlRootReadToken token = origin->root_token;
	ClusterControlRootResult result;

	MemSet(&request, 0, sizeof(request));
	request.mode = CLUSTER_RECOVERY_SERIAL_INPUT_SEAL;
	request.duty = origin->duty;
	request.expected_root_token = token;
	request.formation = origin->formation;
	request.fence_need_set = origin->needs;
	request.fence_admission_set = origin->admissions;
	request.acquire_timeout_ms
		= recovery_fence_acquire_budget_required(plan, plan->acquire_timeout_ms_snapshot);
	request.release_timeout_ms = plan->acquire_timeout_ms_snapshot;
	if (snapshot.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN) {
		result = cluster_control_root_v3_failure_open_publish(&request, &snapshot, &token);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			recovery_fence_seal_failed(plan, &origin->duty, "input seal", (int)result);
		request.expected_root_token = token;
	}
	if ((snapshot.root_flags & CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID) == 0) {
		request.acquire_timeout_ms
			= recovery_fence_acquire_budget_required(plan, plan->acquire_timeout_ms_snapshot);
		result = cluster_control_root_v3_failure_tail_publish(&request, &snapshot, &token);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			recovery_fence_seal_failed(plan, &origin->duty, "tail seal", (int)result);
	}
	result = cluster_control_root_read_canonical(
		origin->origin_thread, &origin->duty, CLUSTER_CONTROL_ROOT_READ_STRONG, &snapshot, &token);
	recovery_fence_deadline_require(plan);
	if ((result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		 && result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		|| snapshot.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
		|| (snapshot.root_flags & required_flags) != required_flags
		|| cluster_recovery_duty_key_compare_for_claim(&snapshot.identity, &origin->duty,
													 cluster_shared_config)
			   != CLUSTER_RECOVERY_DUTY_COMPARE_EXACT)
		recovery_fence_seal_failed(plan, &origin->duty, "sealed read-back", (int)result);
	origin->root_snapshot = snapshot;
	origin->root_token = token;
}

/*
 * PGRAC (S9P2-05): after every instance failed, the founder seals the
 * input of each fenced origin whose writer never had it sealed, after all
 * external admissions and before the plan is sealed.  Every admission is
 * re-checked before the first root changes.  Sealing changes no page; the
 * same transition precedes online recovery.  A refusal after it leaves the
 * generation RECOVERY_REQUIRED, which the next cold start resumes.  The
 * candidates are then projected again: the sealed roots must keep the merge
 * set and give every origin a proven start.  Author: SqlRush
 * <sqlrush@gmail.com>
 */
static void
recovery_fence_plan_seal_inputs(ClusterRecoveryFencePlan *plan, uint16 own_thread,
								XLogRecPtr own_redo)
{
	uint32 required_flags
		= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
		  | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID;
	uint64 replay[2] = { 0, 0 };
	XLogRecPtr start[CLUSTER_WAL_STATE_SLOT_COUNT + 1];
	ClusterMergeEngage engage;
	uint16 sealed = 0;
	uint16 i;
	uint16 tid;

	for (i = 0; i < plan->origin_count; i++) {
		ClusterRecoveryFenceOrigin *origin = &plan->origins[i];
		PgracExternalFenceDenyReason reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;

		if (!recovery_fence_origin_unsealed(&origin->root_snapshot))
			continue;
		if (!cluster_external_fence_need_set_revalidate_nowait(origin->needs, origin->formation,
															   &reason)
			|| !cluster_external_fence_revalidate_set_nowait(origin->admissions, origin->needs,
															 origin->formation, &reason)) {
			int32 node_id = origin->duty.origin_node_id;
			uint64 incarnation = origin->duty.origin_owner_incarnation;

			recovery_fence_plan_release_members(plan);
			MemSet(plan, 0, sizeof(*plan));
			pfree(plan);
			recovery_fence_unavailable(node_id, incarnation, PGRAC_EXTERNAL_FENCE_UNAVAILABLE,
									   (int)reason, "seal", NULL);
		}
	}
	for (i = 0; i < plan->origin_count; i++) {
		if (!recovery_fence_origin_unsealed(&plan->origins[i].root_snapshot))
			continue;
		recovery_fence_origin_seal(plan, &plan->origins[i], required_flags);
		sealed++;
	}
	/* Without a seal the plan's own projection stands, but every origin
	 * must have been proven by it. */
	MemSet(start, 0, sizeof(start));
	if (sealed == 0) {
		engage = CLUSTER_MERGE_ENGAGE;
		memcpy(replay, plan->replay_thread_bitmap, sizeof(replay));
		memcpy(start, plan->start_lsn, sizeof(start));
	} else
		engage = recovery_fence_project_wait(plan, own_redo, replay, start);
	recovery_fence_deadline_require(plan);
	if (engage == CLUSTER_MERGE_ENGAGE
		&& memcmp(replay, plan->replay_thread_bitmap, sizeof(replay)) != 0)
		recovery_fence_seal_refused(plan, "The set of crashed generations changed while their "
										  "input was sealed.");
	for (tid = 1; engage == CLUSTER_MERGE_ENGAGE && tid <= CLUSTER_WAL_STATE_SLOT_COUNT; tid++) {
		if ((replay[(tid - 1) / 64] & (UINT64_C(1) << ((tid - 1) % 64))) != 0
			&& start[tid] == InvalidXLogRecPtr)
			engage = CLUSTER_MERGE_NO_NO_PLAN;
	}
	if (engage != CLUSTER_MERGE_ENGAGE)
		recovery_fence_seal_refused(plan, "The sealed input of the crashed generations does not "
										  "prove every redo start.");
	memcpy(plan->start_lsn, start, sizeof(start));
}

/* Observe the complete original metadata union under one CF-S cut. A peer's
 * old OPEN, or its unfinished self-seal, is pending only inside this exact
 * accepted cold cohort. A writer from the admitted boot is never pending.
 * History/terminal/pending objects are decoded by their original reader;
 * they remain obligations of the later typed input/graph owners. This read
 * grants no replay, physical isolation, completion or writer authority.
 * Author: SqlRush <sqlrush@gmail.com> */
static ClusterControlRootResult
recovery_merge_cold_inputs_observe(const ClusterWalSourceRef *restart,
	const ClusterFormationSnapshotV1 *cohort, ControlRootImage *root,
	ClusterWalOriginInputs *inputs, bool *pending)
{
	ControlFileData common;
	ClusterControlRootFileToken token;
	ClusterControlRootResult result;

	*pending = false;
	result = cluster_control_root_v3_read_control_locked(restart->claim.identity.storage_uuid,
		restart->claim.identity.system_identifier, root, &common, &token);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	for (uint32 node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; ++node) {
		const ClusterControlRootSnapshot *current;
		uint64 boot = cohort->membership.last_admitted_incarnation[node];
		bool member = cohort->membership.membership_state[node] == CLUSTER_MEMBER_MEMBER;

		if (!root->present[node]) {
			if (member)
				return CLUSTER_CONTROL_ROOT_ABSENT;
			continue;
		}
		result = cluster_wal_origin_inputs_read_locked(root, node, inputs);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		current = &inputs->current.snapshot;
		if (member && (current->lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
			|| (current->lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
				&& !cluster_recovery_worker_root_anchor_valid(current)))) {
			if (boot == 0 || current->identity.origin_owner_incarnation >= boot)
				return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
			*pending = true;
		}
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

/* Each Startup can self-seal before any peer replays. Wait for that original
 * producer across the whole admitted cohort, not for a number of census
 * retries or an age threshold. No CF/WALR/IR hold spans the local wait. A
 * changed cohort, unavailable source or live current-boot writer refuses;
 * elapsed time never supplies missing evidence. The independent fence and
 * full retained-source/graph gates remain in the original preflight below. */
/* Finish only this observation's failed CF request. The old cohort and
 * restart input remain immutable; neither a wake nor retirement proves a
 * usable input seal. The caller re-enters its original proof/read loop.
 * Author: SqlRush <sqlrush@gmail.com> */
static void
recovery_merge_cold_cf_wait(const ClusterRecoveryFencePlan *plan,
							const ClusterCfAttemptObservation *attempt,
							const ClusterFormationSnapshotV1 *cohort,
							const ClusterWalSourceRef *restart, uint16 own_thread, uint64 epoch,
							uint64 generation, ResourceOwner owner, uint64 boot, bool recovery)
{
	ClusterCfRetirementSnapshot original;
	PGPROC *proc = MyProc;
	Latch *latch = MyLatch;
	bool empty = attempt->owner_state_at_return == CLUSTER_LOCK_OWNER_EMPTY;
	bool empty_waited = false;
	const char *reason = "UNSAFE_CF_WAIT";

	if (proc == NULL || latch == NULL || owner == NULL || CritSectionCount != 0
		|| LWLockHeldByMe(ControlFileLock) || cluster_cf_held(ExclusiveLock))
		goto refuse;
	if (!empty
		&& (!cluster_cf_retirement_snapshot(ShareLock, NULL, &original)
			|| original.cookie != attempt->control_owner_id
			|| original.registry.message.key.holder.request_id != attempt->request_id
			|| original.registry.message.key.holder.cluster_epoch != attempt->request_epoch)) {
		reason = "CF_WAIT_IDENTITY_OR_COMPLETION";
		goto refuse;
	}
	for (;;) {
		ClusterCfRetirementSnapshot current;
		ClusterFormationSnapshotV1 observed;
		ClusterWalSourceRef input;
		bool held;
		int wait_ms;

		HandleStartupProcInterrupts();
		CHECK_FOR_INTERRUPTS();
		reason = "CONTEXT_CHANGED_AFTER_WAIT_SLICE";
		if (MyProc != proc || MyLatch != latch || !AmStartupProcess() || MyBackendType != B_STARTUP
			|| CurrentResourceOwner != owner || !cluster_enabled || !cluster_shared_config
			|| !cluster_controlfile_shared_authority || ShutdownRequestPending
			|| CritSectionCount != 0 || LWLockHeldByMe(ControlFileLock) || boot == 0
			|| boot != cluster_qvotec_get_self_incarnation() || InRecovery != recovery || epoch == 0
			|| generation == 0 || epoch != cluster_epoch_get_current()
			|| generation != cluster_grd_redeclare_generation()
			|| !cluster_recovery_transport_components_current())
			goto refuse;
		reason = "MERGE_DEADLINE_OR_ZERO_ATTEMPT";
		if (plan->merge_wait_timeout_ms_snapshot == 0 || !recovery_fence_deadline_current(plan))
			goto refuse;
		reason = "CONTEXT_CHANGED_AFTER_WAIT_SLICE";
		ResetLatch(MyLatch);
		if (!cluster_reconfig_capture_formation_snapshot_v1(own_thread, &observed)
			|| !cluster_formation_snapshot_matches_v1(cohort, &observed)
			|| !cluster_wal_thread_restart_v2_ref(&input)
			|| memcmp(restart, &input, sizeof(input)) != 0)
			goto refuse;
		held = cluster_cf_held(ShareLock);
		if (!held && !cluster_cf_held(ExclusiveLock)) {
			if (!empty && cluster_cf_release_completed(ShareLock, original.cookie))
				return;
			if (empty) {
				if (empty_waited)
					return;
				empty_waited = true;
				wait_ms = recovery_fence_acquire_budget(plan, 100);
				if (wait_ms <= 0)
					goto refuse;
				(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, wait_ms,
								WAIT_EVENT_CLUSTER_STARTUP_PHASE_3);
				continue;
			}
		}
		reason = "CF_WAIT_IDENTITY_OR_COMPLETION";
		if (empty || !held || cluster_cf_held(ExclusiveLock)
			|| !cluster_cf_retirement_snapshot(ShareLock, NULL, &current)
			|| current.cookie != original.cookie
			|| current.registry.handle.slot != original.registry.handle.slot
			|| current.registry.handle.generation != original.registry.handle.generation
			|| memcmp(&current.registry.owner, &original.registry.owner,
					  sizeof(current.registry.owner))
				   != 0
			|| memcmp(&current.registry.message.key, &original.registry.message.key,
					  sizeof(current.registry.message.key))
				   != 0)
			goto refuse;
		cluster_lock_owners_service_poll();
		cluster_cf_retirement_poll();
		if (cluster_cf_held(ShareLock)) {
			reason = "MERGE_DEADLINE_OR_ZERO_ATTEMPT";
			wait_ms = recovery_fence_acquire_budget(plan, 100);
			if (wait_ms <= 0)
				goto refuse;
			(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, wait_ms,
							WAIT_EVENT_CLUSTER_STARTUP_PHASE_3);
		}
	}
refuse:
	ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
					errmsg("cold input observation cannot continue its control-file wait"),
					errdetail("PGRAC_FAMILY=STARTUP_CONTROL consumer=cold_inputs reason=%s "
							  "cookie=" UINT64_FORMAT " request=" UINT64_FORMAT,
							  reason, attempt->control_owner_id, attempt->request_id)));
}

/* Reprove the complete original read context after every yielding retry.
 * This observes no ROOT and grants no CF or replay authority. */
static bool
recovery_fence_project_context_current(const ClusterRecoveryFencePlan *plan,
									   const RecoveryFenceCfContext *context, uint16 own_thread,
									   const ClusterFormationSnapshotV1 *cohort,
									   const ClusterWalSourceRef *restart)
{
	ClusterFormationSnapshotV1 observed;
	ClusterWalSourceRef input;

	if (!recovery_fence_plan_valid(plan) || plan->own_thread != own_thread || context->proc == NULL
		|| context->latch == NULL || context->owner == NULL || context->boot == 0
		|| context->epoch == 0 || context->generation == 0 || MyProc != context->proc
		|| MyLatch != context->latch || MyProcPid != context->pid
		|| CurrentResourceOwner != context->owner || plan->owner != context->owner
		|| plan->owner_boot != context->boot || plan->sealed || plan->serial_held
		|| plan->serial_guards.count != 0 || plan->retention_pin != NULL || plan->committed
		|| MyBackendType != B_STARTUP || !AmStartupProcess() || !cluster_enabled
		|| !cluster_shared_config || !cluster_controlfile_shared_authority || ShutdownRequestPending
		|| CritSectionCount != 0 || LWLockHeldByMe(ControlFileLock)
		|| InRecovery != context->recovery || context->boot != cluster_qvotec_get_self_incarnation()
		|| context->epoch != cluster_epoch_get_current()
		|| context->generation != cluster_grd_redeclare_generation()
		|| !cluster_recovery_transport_components_current()
		|| !cluster_reconfig_capture_formation_snapshot_v1(own_thread, &observed)
		|| !cluster_formation_snapshot_matches_v1(cohort, &observed)
		|| !cluster_wal_thread_restart_v2_ref(&input)
		|| memcmp(restart, &input, sizeof(input)) != 0)
		return false;
	for (uint16 i = 0; i < plan->origin_count; i++) {
		const ClusterRecoveryFenceOrigin *origin = &plan->origins[i];
		PgracExternalFenceDenyReason reason;

		if (cluster_formation_witness_revalidate_nowait(origin->formation)
				!= CLUSTER_FORMATION_WITNESS_READY
			|| !cluster_external_fence_need_set_revalidate_nowait(origin->needs, origin->formation,
																  &reason)
			|| !cluster_external_fence_revalidate_set_nowait(origin->admissions, origin->needs,
															 origin->formation, &reason))
			return false;
	}
	return true;
}

/* Initial and post-seal projections precede this plan's pin/IR acquisition.
 * Only their caller retries a fresh typed CF slice. Keep already published
 * seals; restart all projection outputs after exact cleanup and reproof.
 * Author: SqlRush <sqlrush@gmail.com> */
static ClusterMergeEngage
recovery_fence_project_wait(ClusterRecoveryFencePlan *plan, XLogRecPtr own_redo, uint64 replay[2],
							XLogRecPtr *start)
{
	ClusterFormationSnapshotV1 cohort;
	ClusterWalSourceRef restart;
	RecoveryFenceCfContext context = { .proc = MyProc,
									   .latch = MyLatch,
									   .owner = CurrentResourceOwner,
									   .pid = MyProcPid,
									   .boot = cluster_qvotec_get_self_incarnation(),
									   .epoch = cluster_epoch_get_current(),
									   .generation = cluster_grd_redeclare_generation(),
									   .recovery = InRecovery };
	uint16 own_thread;
	bool wait_allowed;
	bool waited = false;

	if (!recovery_fence_plan_valid(plan) || plan->sealed || plan->serial_held
		|| plan->serial_guards.count != 0 || plan->retention_pin != NULL || plan->committed)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						errmsg("cold projection has no original pre-IR plan")));
	own_thread = plan->own_thread;
	wait_allowed = cluster_shared_config && MyBackendType == B_STARTUP && AmStartupProcess()
				   && context.owner != NULL && plan->owner == context.owner
				   && plan->owner_boot == context.boot && !cluster_cf_held(ShareLock)
				   && !cluster_cf_held(ExclusiveLock)
				   && cluster_reconfig_capture_formation_snapshot_v1(own_thread, &cohort)
				   && cohort.startup_formation_generation != 0
				   && cluster_wal_thread_restart_v2_ref(&restart);
	for (;;) {
		ClusterCfAttemptObservation attempt;
		ClusterMergeEngage engage;

		if (waited
			&& !recovery_fence_project_context_current(plan, &context, own_thread, &cohort,
													   &restart))
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
							errmsg("cold projection context changed during its CF retry")));
		recovery_fence_deadline_require(plan);
		MemSet(start, 0, sizeof(*start) * (CLUSTER_WAL_STATE_SLOT_COUNT + 1));
		engage = recovery_merge_project_attempt(own_thread, own_redo, replay, start,
												wait_allowed ? &attempt : NULL);
		/* A typed failed acquire still owns its original cleanup debt.
		 * Let that waiter refuse expiry without releasing unrelated proof
		 * owners; successful/no-wait projections use the original cleanup. */
		if (!wait_allowed || !cluster_cf_attempt_wait_slice(&attempt))
			recovery_fence_deadline_require(plan);
		if (waited
			&& !recovery_fence_project_context_current(plan, &context, own_thread, &cohort,
													   &restart))
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
							errmsg("cold projection context changed after its new CF grant")));
		if (!wait_allowed || !cluster_cf_attempt_wait_slice(&attempt))
			return engage;
		if (!recovery_fence_project_context_current(plan, &context, own_thread, &cohort, &restart))
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
							errmsg("cold projection context changed before its CF wait")));
		recovery_merge_cold_cf_wait(plan, &attempt, &cohort, &restart, own_thread, context.epoch,
									context.generation, context.owner, context.boot,
									context.recovery);
		waited = true;
	}
}

static bool
recovery_merge_wait_cold_inputs(const ClusterRecoveryFencePlan *plan, uint16 own_thread)
{
	ClusterFormationSnapshotV1 cohort, current;
	ClusterWalSourceRef restart;
	ControlRootImage *root;
	ClusterWalOriginInputs *inputs;
	volatile bool cf_attempted = false;
	bool ready = false;
	bool reported = false;
	bool waited = false;
	uint64 epoch = cluster_epoch_get_current();
	uint64 generation = cluster_grd_redeclare_generation();
	uint64 boot = cluster_qvotec_get_self_incarnation();
	ResourceOwner owner = CurrentResourceOwner;
	bool recovery = InRecovery;

	if (!recovery_fence_plan_valid(plan) || plan->own_thread != own_thread || plan->owner != owner
		|| plan->owner_boot != boot)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						errmsg("cold input observation has no original plan owner")));
	if (!cluster_wal_thread_restart_v2_ref(&restart)
		|| !cluster_reconfig_capture_formation_snapshot_v1(own_thread, &cohort)
		|| cohort.startup_formation_generation == 0)
		return false;
	root = palloc(sizeof(*root));
	inputs = palloc(sizeof(*inputs));
	PG_TRY();
	{
		for (;;) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
			ClusterFormationWitnessResult proof;
			bool pending = true;
			int wait_ms;

			HandleStartupProcInterrupts();
			CHECK_FOR_INTERRUPTS();
			wait_ms = recovery_fence_acquire_budget(plan, 100);
			if (wait_ms <= 0)
				ereport(FATAL,
						(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						 errmsg("cold input observation exceeded its original merge deadline"),
						 errdetail("stage=merge_deadline before=refresh")));
			proof = cluster_authority_startup_refresh_recovery(wait_ms);
			if (!recovery_fence_deadline_current(plan))
				ereport(FATAL,
						(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						 errmsg("cold input observation exceeded its original merge deadline"),
						 errdetail("stage=merge_deadline after=refresh")));
			if (!cluster_reconfig_capture_formation_snapshot_v1(own_thread, &current)
				|| !cluster_formation_snapshot_matches_v1(&cohort, &current))
				break;
			if (proof == CLUSTER_FORMATION_WITNESS_READY) {
				ClusterCfReleaseResult released = CLUSTER_CF_RELEASE_NOT_HELD;
				ClusterCfAttemptObservation attempt;
				uint64 before = cluster_cf_attempt_sequence();

				cf_attempted = true;
				if (cluster_cf_lock(ShareLock) && cluster_cf_held_is_clusterwide(ShareLock)) {
					ClusterWalSourceRef input;

					/* A new acquire can yield too. Reprove the original read
					 * context before any input observation under its new grant. */
					if (waited
						&& (CurrentResourceOwner != owner || MyBackendType != B_STARTUP
							|| !AmStartupProcess() || InRecovery != recovery
							|| ShutdownRequestPending || !cluster_enabled || !cluster_shared_config
							|| !cluster_controlfile_shared_authority || CritSectionCount != 0
							|| LWLockHeldByMe(ControlFileLock)
							|| boot != cluster_qvotec_get_self_incarnation()
							|| epoch != cluster_epoch_get_current()
							|| generation != cluster_grd_redeclare_generation()
							|| !cluster_recovery_transport_components_current()
							|| !cluster_reconfig_capture_formation_snapshot_v1(own_thread, &current)
							|| !cluster_formation_snapshot_matches_v1(&cohort, &current)
							|| !cluster_wal_thread_restart_v2_ref(&input)
							|| memcmp(&restart, &input, sizeof(input)) != 0))
						result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
					else if (!recovery_fence_deadline_current(plan))
						ereport(
							FATAL,
							(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
							 errmsg("cold input observation exceeded its original merge deadline"),
							 errdetail("stage=merge_deadline after=CF_acquire")));
					else
						result = recovery_merge_cold_inputs_observe(&restart, &cohort, root, inputs,
																	&pending);
				} else if (cluster_cf_last_attempt_since(before, &attempt)
						   && cluster_cf_attempt_wait_slice(&attempt)
						   && attempt.mode == ShareLock) {
					/* Already abandoned by the original acquire. Do not turn
					 * its still-pending release into another blocking failure. */
					cf_attempted = false;
					waited = true;
					recovery_merge_cold_cf_wait(plan, &attempt, &cohort, &restart, own_thread,
												epoch, generation, owner, boot, recovery);
					continue;
				}
				if (cluster_cf_held(ShareLock))
					released = cluster_cf_unlock_confirmed(ShareLock);
				cf_attempted = false;
				if (released != CLUSTER_CF_RELEASE_CONFIRMED
					&& released != CLUSTER_CF_RELEASE_NOT_HELD)
					ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						errmsg("cold input observation could not release CF")));
				if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
					&& released != CLUSTER_CF_RELEASE_CONFIRMED)
					result = CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN;
			} else if (proof != CLUSTER_FORMATION_WITNESS_READY
				&& proof != CLUSTER_FORMATION_WITNESS_UNSTABLE
				&& proof != CLUSTER_FORMATION_WITNESS_CACHE_EXPIRED
				&& proof != CLUSTER_FORMATION_WITNESS_MARKER_UNPROVEN)
				break;
			if (!recovery_fence_deadline_current(plan))
				ereport(FATAL,
						(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						 errmsg("cold input observation exceeded its original merge deadline"),
						 errdetail("stage=merge_deadline after=observation")));
			if (!cluster_reconfig_capture_formation_snapshot_v1(own_thread, &current)
				|| !cluster_formation_snapshot_matches_v1(&cohort, &current))
				break;
			if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY && !pending) {
				ready = true;
				break;
			}
			if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
				&& result != CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE) {
				ereport(LOG, (errmsg("cold input observation refused"),
					errdetail("result=%d own_thread=%u", (int)result, (unsigned)own_thread)));
				break;
			}
			if (!reported) {
				ereport(LOG, (errmsg("waiting for the cold cohort's original input seals"),
					errdetail("formation_generation=" UINT64_FORMAT " own_thread=%u",
						cohort.startup_formation_generation, (unsigned)own_thread)));
				reported = true;
			}
			if (plan->merge_wait_timeout_ms_snapshot == 0)
				break;
			ResetLatch(MyLatch);
			wait_ms = recovery_fence_acquire_budget(plan, 100);
			if (wait_ms <= 0)
				ereport(FATAL,
						(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						 errmsg("cold input observation exceeded its original merge deadline"),
						 errdetail("stage=merge_deadline before=metadata_wait")));
			(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, wait_ms,
							WAIT_EVENT_CLUSTER_STARTUP_PHASE_3);
		}
	}
	PG_FINALLY();
	{
		if (cf_attempted && cluster_cf_held(ShareLock)
			&& cluster_cf_unlock_confirmed(ShareLock) != CLUSTER_CF_RELEASE_CONFIRMED)
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
				errmsg("cold input observation cleanup could not release CF")));
		pfree(inputs);
		pfree(root);
	}
	PG_END_TRY();
	return ready;
}

/*
 * RF-ROOT P4 S04-11 cold readonly preflight.  Candidate/WAL validation and
 * all root classifications finish before the first daemon admission.  One
 * failure destroys the complete plan; no subset can reach the merge claim.
 */
ClusterMergeEngage
cluster_recovery_merge_preflight_readonly(uint16 own_thread, XLogRecPtr own_redo,
										  ClusterRecoveryFencePlan **out_plan)
{
	ClusterRecoveryFencePlan *plan;
	ClusterRecoveryPlan observed;
	ClusterMergeEngage engage;
	instr_time admission_started;
	uint16 origin_threads[CLUSTER_WAL_STATE_SLOT_COUNT];
	uint16 tid;
	uint32 required_flags
		= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
		  | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID;

	if (out_plan == NULL || *out_plan != NULL) {
		ereport(LOG, (errmsg("cold recovery plan output refused"),
					  errdetail("predicate=PLAN_OUTPUT_NOT_EMPTY own_thread=%u output_null=%d",
								(unsigned)own_thread, out_plan == NULL ? 1 : 0)));
		return CLUSTER_MERGE_NO_NO_PLAN;
	}
	plan = palloc0(sizeof(*plan));
	plan->magic = CLUSTER_RECOVERY_FENCE_PLAN_MAGIC;
	plan->owner_pid = MyProcPid;
	plan->owner = CurrentResourceOwner;
	plan->owner_boot = cluster_qvotec_get_self_incarnation();
	plan->own_thread = own_thread;
	plan->acquire_timeout_ms_snapshot = cluster_external_fence_acquire_timeout_ms;
	recovery_fence_deadline_begin(plan);
	/* Only the same complete cohort's original input producers can make a
	 * fresh census usable. Re-publish once after readiness, never to age or
	 * override an ALIVE verdict. UNKNOWN may be a predecessor's unsealed
	 * OPEN, but only the complete original cohort input cut can resolve it.
	 * No claim or fence has been acquired yet. */
	if (cluster_shared_config && cluster_recovery_plan_snapshot(&observed)
		&& !observed.failed && observed.own_thread == own_thread
		&& (observed.n_alive > 0 || observed.n_unknown > 0)) {
		if (!recovery_merge_wait_cold_inputs(plan, own_thread)) {
			MemSet(plan, 0, sizeof(*plan));
			pfree(plan);
			return CLUSTER_MERGE_NO_NOT_COLD;
		}
		cluster_recovery_plan_generate(observed.dbstate_at_startup, observed.local_recovery_needed);
	}
	/* PGRAC: refuse the unshared multi-generation merge before any fence
	 * admission, and before the merge blockers, which only the shared
	 * profile's typed cold plan could satisfy.  Author: SqlRush
	 * <sqlrush@gmail.com> */
	if (!cluster_shared_config && recovery_merge_cold_merge_needed(own_thread))
		engage = CLUSTER_MERGE_REFUSE_UNSHARED;
	else
		engage = recovery_fence_project_wait(plan, own_redo, plan->replay_thread_bitmap,
											 plan->start_lsn);
	recovery_fence_deadline_require(plan);
	engage = cluster_recovery_merge_profile_gate(engage, cluster_shared_config);
	if (engage != CLUSTER_MERGE_ENGAGE) {
		MemSet(plan, 0, sizeof(*plan));
		pfree(plan);
		return engage;
	}

	plan->foreign_origin_bitmap[0] = plan->replay_thread_bitmap[0];
	plan->foreign_origin_bitmap[1] = plan->replay_thread_bitmap[1];
	plan->foreign_origin_bitmap[(own_thread - 1) / 64] &= ~(UINT64_C(1) << ((own_thread - 1) % 64));

	/* First close every classification.  No provider call is reachable from
	 * this loop, so a late non-current origin still yields zero sockets. */
	for (tid = 1; tid <= CLUSTER_WAL_STATE_SLOT_COUNT; tid++) {
		ClusterControlRootIdentity identity;
		ClusterControlRootSnapshot snapshot;
		ClusterControlRootReadToken token;
		ClusterControlRootResult root_result;
		ClusterRecoveryFenceOrigin *origin;
		int32 node_id;

		if ((plan->foreign_origin_bitmap[(tid - 1) / 64] & (UINT64_C(1) << ((tid - 1) % 64))) == 0)
			continue;
		node_id = (int32)tid - 1;
		MemSet(&identity, 0, sizeof(identity));
		MemSet(&snapshot, 0, sizeof(snapshot));
		MemSet(&token, 0, sizeof(token));
		root_result = cluster_control_root_lookup_owner_by_node_runtime(node_id, &identity,
																		&snapshot, &token);
		recovery_fence_deadline_require(plan);
		if ((root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
			 && root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
			|| !cluster_recovery_duty_key_valid_for_claim(&identity, cluster_shared_config)
			|| identity.origin_thread_id != tid
			|| identity.origin_node_id != node_id
			|| cluster_recovery_duty_key_compare_for_claim(&snapshot.identity, &identity,
														 cluster_shared_config)
				   != CLUSTER_RECOVERY_DUTY_COMPARE_EXACT) {
			recovery_fence_plan_release_members(plan);
			MemSet(plan, 0, sizeof(*plan));
			pfree(plan);
			recovery_fence_unavailable(node_id, identity.origin_owner_incarnation,
									   PGRAC_EXTERNAL_FENCE_UNAVAILABLE, (int)root_result,
									   "root-classification", NULL);
		}
		if (snapshot.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE) {
			plan->replay_thread_bitmap[(tid - 1) / 64] &= ~(UINT64_C(1) << ((tid - 1) % 64));
			plan->foreign_origin_bitmap[(tid - 1) / 64] &= ~(UINT64_C(1) << ((tid - 1) % 64));
			plan->start_lsn[tid] = InvalidXLogRecPtr;
			continue;
		}
		if (!recovery_fence_origin_input_acceptable(&snapshot, required_flags)) {
			recovery_fence_plan_release_members(plan);
			MemSet(plan, 0, sizeof(*plan));
			pfree(plan);
			recovery_fence_unavailable(
				node_id, identity.origin_owner_incarnation, PGRAC_EXTERNAL_FENCE_UNAVAILABLE,
				PGRAC_EXTERNAL_FENCE_DENY_ROOT_NOT_COMPLETE, "root-lifecycle", NULL);
		}
		origin = &plan->origins[plan->origin_count];
		origin->origin_thread = tid;
		origin->duty = identity;
		origin->root_snapshot = snapshot;
		origin->root_token = token;
		origin_threads[plan->origin_count] = tid;
		plan->origin_count++;
	}

	if (plan->origin_count == 0) {
		MemSet(plan, 0, sizeof(*plan));
		pfree(plan);
		return CLUSTER_MERGE_NO_NO_CANDIDATES;
	}
	if (!cluster_recovery_fence_plan_shape_valid(own_thread, plan->replay_thread_bitmap,
												 plan->foreign_origin_bitmap, origin_threads,
												 plan->origin_count)) {
		ereport(LOG, (errmsg("cold recovery plan shape refused"),
					  errdetail("predicate=PLAN_ORIGIN_SHAPE own_thread=%u origins=%u",
								(unsigned)own_thread, (unsigned)plan->origin_count)));
		MemSet(plan, 0, sizeof(*plan));
		pfree(plan);
		return CLUSTER_MERGE_NO_NO_PLAN;
	}

	/* Then build every formation/NeedSet.  Only after all nonwaitable
	 * classification gates are closed may the aggregate admission loop run. */
	for (tid = 0; tid < plan->origin_count; tid++) {
		ClusterRecoveryFenceOrigin *origin = &plan->origins[tid];
		ClusterFormationWitnessResult formation_result;
		int acquire_budget
			= recovery_fence_acquire_budget_required(plan, plan->acquire_timeout_ms_snapshot);

		/* E1's accepted new cohort coordinates recovery. The old incarnations
		 * come from prior-death-sealed ROOTs, not online CSS DEAD membership. */
		formation_result
			= cluster_shared_config
				  ? cluster_formation_witness_build_recovery_control_wait(
						origin->origin_thread, acquire_budget, &origin->formation)
				  : cluster_formation_witness_build_wait(origin->origin_thread, false,
														 acquire_budget, &origin->formation);
		recovery_fence_deadline_require(plan);
		if (formation_result != CLUSTER_FORMATION_WITNESS_READY) {
			int32 node_id = origin->duty.origin_node_id;
			uint64 incarnation = origin->duty.origin_owner_incarnation;

			recovery_fence_plan_release_members(plan);
			MemSet(plan, 0, sizeof(*plan));
			pfree(plan);
			recovery_fence_unavailable(node_id, incarnation, PGRAC_EXTERNAL_FENCE_UNAVAILABLE,
									   (int)formation_result, "formation", NULL);
		}
	}
	for (tid = 0; tid < plan->origin_count; tid++) {
		ClusterRecoveryFenceOrigin *origin = &plan->origins[tid];
		PgracExternalFenceNeedSetResult need_result;

		need_result = cluster_shared_config
			? cluster_external_fence_need_set_build_cold(&origin->duty, origin->formation, &origin->needs)
			: cluster_external_fence_need_set_build(&origin->duty, origin->formation, &origin->needs);
		if (need_result != PGRAC_EXTERNAL_FENCE_NEED_SET_OK) {
			int32 node_id = origin->duty.origin_node_id;
			uint64 incarnation = origin->duty.origin_owner_incarnation;
			const char *predicate = cluster_external_fence_need_set_last_predicate();

			recovery_fence_plan_release_members(plan);
			MemSet(plan, 0, sizeof(*plan));
			pfree(plan);
			recovery_fence_unavailable(node_id, incarnation, PGRAC_EXTERNAL_FENCE_UNAVAILABLE,
									   (int)need_result, "need-set", predicate);
		}
	}
	INSTR_TIME_SET_CURRENT(admission_started);
	for (tid = 0; tid < plan->origin_count; tid++) {
		ClusterRecoveryFenceOrigin *origin = &plan->origins[tid];
		PgracExternalFenceVerdict verdict;
		PgracExternalFenceDenyReason reason;
		instr_time now;
		double elapsed_ms;
		int elapsed_ms_ceil;
		int remaining_ms;

		INSTR_TIME_SET_CURRENT(now);
		INSTR_TIME_SUBTRACT(now, admission_started);
		elapsed_ms = INSTR_TIME_GET_MILLISEC(now);
		elapsed_ms_ceil = (int)elapsed_ms;
		if ((double)elapsed_ms_ceil < elapsed_ms)
			elapsed_ms_ceil++;
		remaining_ms = plan->acquire_timeout_ms_snapshot - elapsed_ms_ceil;
		if (remaining_ms > 0)
			remaining_ms = recovery_fence_acquire_budget_required(plan, remaining_ms);
		if (remaining_ms <= 0) {
			verdict = PGRAC_EXTERNAL_FENCE_UNAVAILABLE;
			reason = PGRAC_EXTERNAL_FENCE_DENY_TIMEOUT;
		} else {
			verdict = cluster_external_fence_admit_set_wait(origin->needs, origin->formation,
															remaining_ms, &origin->admissions);
			recovery_fence_deadline_require(plan);
			reason = cluster_external_fence_last_deny_reason();
		}
		if (verdict != PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED) {
			int32 node_id = origin->duty.origin_node_id;
			uint64 incarnation = origin->duty.origin_owner_incarnation;

			recovery_fence_plan_release_members(plan);
			MemSet(plan, 0, sizeof(*plan));
			pfree(plan);
			recovery_fence_unavailable(node_id, incarnation, (int)verdict, (int)reason,
									   "admission-set", NULL);
		}
	}

	if (cluster_shared_config)
		recovery_fence_plan_seal_inputs(plan, own_thread, own_redo);
	recovery_fence_deadline_require(plan);

	plan->sealed = true;
	*out_plan = plan;
	return CLUSTER_MERGE_ENGAGE;
}

/* Retain the original false result before cleanup. Formatting uses only the
 * compared copies, and never probes a source/holder or changes authority. */
static bool
recovery_fence_refused_detail(ClusterRecoveryFencePlan *plan, const char *stage, uint16 thread,
							  int index, int result, const char *detail)
{
	char message[512];

	snprintf(message, sizeof(message), "stage=%s origin_thread=%u origin_index=%d result=%d%s%s",
			 stage, thread, index, result, detail != NULL ? " " : "", detail != NULL ? detail : "");
	if (recovery_fence_plan_valid(plan) && plan->refusal_detail[0] == '\0')
		strlcpy(plan->refusal_detail, message, sizeof(plan->refusal_detail));
	if (cluster_trace_control_config && CritSectionCount == 0)
		ereport(LOG, (errmsg("PGRAC_FAMILY=COLD_FENCE host_node=%d caller_pid=%d %s",
							 cluster_node_id, MyProcPid, message)));
	return false;
}

static bool
recovery_fence_refused(ClusterRecoveryFencePlan *plan, const char *stage, uint16 thread, int index,
					   int result)
{
	return recovery_fence_refused_detail(plan, stage, thread, index, result, NULL);
}

static bool
recovery_fence_changed(ClusterRecoveryFencePlan *plan, const char *stage, uint16 thread, int index,
					   const char *field, uint64 expected, uint64 observed)
{
	char detail[192];

	snprintf(detail, sizeof(detail), "field=%s expected=" UINT64_FORMAT " observed=" UINT64_FORMAT,
			 field, expected, observed);
	return recovery_fence_refused_detail(plan, stage, thread, index, 0, detail);
}

const char *
cluster_recovery_merge_fence_plan_refusal_v1(const ClusterRecoveryFencePlan *plan)
{
	return recovery_fence_plan_valid(plan) ? plan->refusal_detail : "";
}

static bool
recovery_fence_cf_context_current(const ClusterRecoveryFencePlan *plan,
								  const RecoveryFenceCfContext *context)
{
	return recovery_fence_plan_valid(plan) && context->proc != NULL && context->latch != NULL
		   && context->owner != NULL && context->pin != NULL && context->boot != 0
		   && context->epoch != 0 && context->generation != 0 && context->proc == MyProc
		   && context->latch == MyLatch && context->pid == MyProcPid
		   && context->owner == CurrentResourceOwner && context->owner == plan->owner
		   && context->boot == plan->owner_boot
		   && context->boot == cluster_qvotec_get_self_incarnation()
		   && context->pin == plan->retention_pin && context->epoch == cluster_epoch_get_current()
		   && context->generation == cluster_grd_redeclare_generation()
		   && context->recovery == InRecovery && MyBackendType == B_STARTUP && AmStartupProcess()
		   && cluster_enabled && cluster_shared_config && cluster_controlfile_shared_authority
		   && !ShutdownRequestPending && CritSectionCount == 0 && !LWLockHeldByMe(ControlFileLock)
		   && cluster_recovery_transport_components_current() && !plan->serial_held
		   && plan->serial_guards.count == 0;
}

/* Only the set's caller can wait: its original acquire-set has already
 * rolled every earlier IR member back with exact ACK. Keep the original
 * unbound pin, and never read ROOT or revalidate that pin while CF retires.
 * Author: SqlRush <sqlrush@gmail.com> */
static bool
recovery_fence_wait_own_cf(ClusterRecoveryFencePlan *plan, const RecoveryFenceCfContext *context,
						   const ClusterCfAttemptObservation *attempt)
{
	ClusterCfRetirementSnapshot original = { 0 };
	bool empty = attempt->owner_state_at_return == CLUSTER_LOCK_OWNER_EMPTY;
	bool empty_waited = false;
	const char *reason = "CF_IDENTITY_OR_COMPLETION";
	char detail[192];

	if (attempt->mode != ShareLock || !cluster_cf_attempt_wait_slice(attempt))
		goto refused;
	if (!empty && cluster_cf_held(ShareLock)
		&& (!cluster_cf_retirement_snapshot(ShareLock, NULL, &original)
			|| original.cookie != attempt->control_owner_id
			|| original.registry.message.key.holder.request_id != attempt->request_id
			|| original.registry.message.key.holder.cluster_epoch != attempt->request_epoch))
		goto refused;
	for (;;) {
		ClusterCfRetirementSnapshot current;
		int wait_ms;
		PgracExternalFenceDenyReason fence_reason;

		reason = "ORIGINAL_CONTEXT_CHANGED";
		HandleStartupProcInterrupts();
		CHECK_FOR_INTERRUPTS();
		if (!recovery_fence_cf_context_current(plan, context))
			goto refused;
		reason = "MERGE_DEADLINE_OR_ZERO_ATTEMPT";
		if (plan->merge_wait_timeout_ms_snapshot == 0 || !recovery_fence_deadline_current(plan))
			goto refused;
		reason = "ORIGINAL_FENCE_CHANGED";
		for (uint16 i = 0; i < plan->origin_count; i++) {
			ClusterRecoveryFenceOrigin *origin = &plan->origins[i];

			if (cluster_formation_witness_revalidate_nowait(origin->formation)
					!= CLUSTER_FORMATION_WITNESS_READY
				|| !cluster_external_fence_need_set_revalidate_nowait(
					origin->needs, origin->formation, &fence_reason)
				|| !cluster_external_fence_revalidate_set_nowait(origin->admissions, origin->needs,
																 origin->formation, &fence_reason))
				goto refused;
		}
		ResetLatch(context->latch);
		reason = "CF_IDENTITY_OR_COMPLETION";
		if (cluster_cf_held(ExclusiveLock))
			goto refused;
		if (!cluster_cf_held(ShareLock)) {
			if (!empty && attempt->control_owner_id != 0
				&& cluster_cf_release_completed(ShareLock, attempt->control_owner_id))
				return true;
			if (!empty)
				goto refused;
			if (empty_waited)
				return true;
		} else {
			if (empty || original.cookie == 0
				|| !cluster_cf_retirement_snapshot(ShareLock, NULL, &current)
				|| current.cookie != original.cookie
				|| current.registry.handle.slot != original.registry.handle.slot
				|| current.registry.handle.generation != original.registry.handle.generation
				|| memcmp(&current.registry.owner, &original.registry.owner,
						  sizeof(current.registry.owner))
					   != 0
				|| memcmp(&current.registry.message.key, &original.registry.message.key,
						  sizeof(current.registry.message.key))
					   != 0)
				goto refused;
			cluster_lock_owners_service_poll();
			cluster_cf_retirement_poll();
			if (!cluster_cf_held(ShareLock))
				continue;
		}
		reason = "MERGE_DEADLINE_OR_ZERO_ATTEMPT";
		wait_ms = recovery_fence_acquire_budget(plan, 100);
		if (wait_ms <= 0)
			goto refused;
		(void)WaitLatch(context->latch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, wait_ms,
						WAIT_EVENT_CLUSTER_STARTUP_PHASE_3);
		empty_waited = true;
	}
refused:
	snprintf(detail, sizeof(detail), "reason=%s cookie=" UINT64_FORMAT " request=" UINT64_FORMAT,
			 reason, attempt->control_owner_id, attempt->request_id);
	return recovery_fence_refused_detail(plan, "serial_cf_wait", 0, -1, (int)attempt->result,
										 detail);
}

static bool
recovery_fence_plan_acquire_serial_inputs(ClusterRecoveryFencePlan *plan,
									   const uint16 *input_threads, uint16 ninput_threads)
{
	ClusterRecoverySerialRequest *requests;
	ClusterRecoverySerialAcquireResult result;
	uint16 failed_index = UINT16_MAX;
	uint16 i;
	ClusterWalPinResult bind_result;
	int acquire_budget;
	RecoveryFenceCfContext context = { .proc = MyProc,
									   .latch = MyLatch,
									   .owner = CurrentResourceOwner,
									   .pid = MyProcPid,
									   .boot = cluster_qvotec_get_self_incarnation(),
									   .epoch = cluster_epoch_get_current(),
									   .generation = cluster_grd_redeclare_generation(),
									   .recovery = InRecovery };
	bool clear_at_entry = !cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock);

	if (!recovery_fence_plan_valid(plan) || !plan->sealed || plan->serial_held || plan->committed
		|| plan->origin_count == 0 || plan->serial_guards.count != 0 || plan->retention_pin != NULL)
		return recovery_fence_refused(plan, "serial_state", 0, -1, 0);
	if (!recovery_fence_deadline_current(plan))
		return recovery_fence_refused(plan, "merge_deadline", 0, -1, 0);
	/* PGRAC: retain the complete original input before acquiring IR. The pin
	 * borrows the plan's fence owners until BOTH releases are confirmed.
	 * Author: SqlRush <sqlrush@gmail.com> */
	if (cluster_shared_config) {
		ClusterWalRetentionPinThreadRequest *pin_requests;
		ClusterWalRetentionInterval *intervals;
		ClusterWalPinResult pin_result;
		bool valid = true;

		pin_requests = palloc0(sizeof(*pin_requests) * plan->origin_count);
		intervals = palloc0(sizeof(*intervals) * plan->origin_count);
		for (i = 0; i < plan->origin_count; i++) {
			ClusterRecoveryFenceOrigin *origin = &plan->origins[i];

			if (!cluster_thread_recovery_pin_request_build_v1(
					origin->origin_thread, &origin->duty, &origin->root_snapshot,
					&origin->root_token, origin->formation, origin->needs, origin->admissions,
					&intervals[i], &pin_requests[i])) {
				valid = false;
				break;
			}
		}
		pin_result = CLUSTER_WAL_PIN_INVALID;
		if (valid) {
			if (ninput_threads != 0)
				pin_result = cluster_wal_retention_pin_acquire_inputs_v1(
					pin_requests, plan->origin_count, input_threads, ninput_threads,
					&plan->retention_pin);
			else
				pin_result = cluster_wal_retention_pin_acquire(pin_requests, plan->origin_count,
																 &plan->retention_pin);
		}
		pfree(intervals);
		pfree(pin_requests);
		if (pin_result != CLUSTER_WAL_PIN_OK)
			return recovery_fence_refused(plan, valid ? "pin_acquire" : "pin_request",
										  valid ? 0 : plan->origins[i].origin_thread,
										  valid ? -1 : (int)i, valid ? (int)pin_result : 0);
	}
	requests = palloc0(sizeof(*requests) * plan->origin_count);
	context.pin = plan->retention_pin;
	for (i = 0; i < plan->origin_count; i++) {
		ClusterRecoveryFenceOrigin *origin = &plan->origins[i];

		requests[i].mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
		requests[i].duty = origin->duty;
		requests[i].expected_root_token = origin->root_token;
		requests[i].formation = origin->formation;
		requests[i].fence_need_set = origin->needs;
		requests[i].fence_admission_set = origin->admissions;
		requests[i].release_timeout_ms = plan->acquire_timeout_ms_snapshot;
	}
	for (;;) {
		ClusterCfAttemptObservation attempt;
		uint64 before;
		bool cf_retry;

		acquire_budget = recovery_fence_acquire_budget(plan, plan->acquire_timeout_ms_snapshot);
		if (acquire_budget <= 0) {
			pfree(requests);
			return recovery_fence_refused(plan, "merge_deadline", 0, -1, 0);
		}
		for (i = 0; i < plan->origin_count; i++)
			requests[i].acquire_timeout_ms = acquire_budget;
		before = cluster_cf_attempt_sequence();
		result = cluster_recovery_serial_acquire_set(requests, plan->origin_count, acquire_budget,
													 &plan->serial_guards, &failed_index);
		if (result == CLUSTER_RECOVERY_SERIAL_GRANTED)
			break;
		/* ROOT_UNAVAILABLE remains hard unless this exact attempt supplies
		 * its own typed CF slice. The IR request itself never waits on CF. */
		cf_retry = clear_at_entry && cluster_shared_config && MyBackendType == B_STARTUP
				   && AmStartupProcess() && failed_index < plan->origin_count
				   && (result == CLUSTER_RECOVERY_SERIAL_ROOT_UNAVAILABLE
					   || (result == CLUSTER_RECOVERY_SERIAL_INTERNAL_FAILURE
						   && plan->serial_guards.count != 0))
				   && cluster_cf_last_attempt_since(before, &attempt) && attempt.mode == ShareLock
				   && cluster_cf_attempt_wait_slice(&attempt);
		if (!cf_retry)
			break;
		if (plan->serial_guards.count != 0) {
			pfree(requests);
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_RECOVERY_OWNER_CONFLICT),
					 errmsg("cold recovery IR rollback was not confirmed before CF wait"),
					 errdetail("origin_index=%u retained_ir=%u cookie=" UINT64_FORMAT, failed_index,
							   plan->serial_guards.count, attempt.control_owner_id)));
		}
		result = CLUSTER_RECOVERY_SERIAL_RETRY;
		if (!recovery_fence_wait_own_cf(plan, &context, &attempt)) {
			pfree(requests);
			return false;
		}
		bind_result = cluster_wal_retention_pin_preflight_revalidate_wait_v1(context.pin);
		if (bind_result != CLUSTER_WAL_PIN_OK
			|| !recovery_fence_cf_context_current(plan, &context)) {
			pfree(requests);
			return recovery_fence_refused(plan, "serial_cf_pin_revalidate", 0, -1,
										  (int)bind_result);
		}
	}
	pfree(requests);
	if (result != CLUSTER_RECOVERY_SERIAL_GRANTED)
		return recovery_fence_refused(
			plan, "serial_acquire",
			failed_index < plan->origin_count ? plan->origins[failed_index].origin_thread : 0,
			failed_index < plan->origin_count ? (int)failed_index : -1, (int)result);
	if (!recovery_fence_deadline_current(plan))
		return recovery_fence_refused(plan, "merge_deadline", 0, -1, 0);
	if (cluster_shared_config
		&& (bind_result = (plan->origin_count == 1
				? cluster_wal_retention_pin_bind_one(plan->retention_pin,
													 &plan->serial_guards.guards[0])
				: cluster_wal_retention_pin_bind_set(plan->retention_pin, &plan->serial_guards)))
			   != CLUSTER_WAL_PIN_OK)
		return recovery_fence_refused(plan, "pin_bind", 0, -1, (int)bind_result);
	plan->serial_held = true;
	return true;
}

bool
cluster_recovery_merge_fence_plan_acquire_serial(ClusterRecoveryFencePlan *plan)
{
	return recovery_fence_plan_acquire_serial_inputs(plan, NULL, 0);
}

bool
cluster_recovery_merge_fence_plan_acquire_serial_inputs_v1(
	ClusterRecoveryFencePlan *plan, const uint16 *input_threads, uint16 ninput_threads)
{
	if (!cluster_shared_config || input_threads == NULL || ninput_threads == 0
		|| ninput_threads > CLUSTER_WAL_STATE_SLOT_COUNT)
		return recovery_fence_refused(plan, "serial_input_arguments", 0, -1, 0);
	return recovery_fence_plan_acquire_serial_inputs(plan, input_threads, ninput_threads);
}

bool
cluster_recovery_merge_commit_plan_nowait(ClusterRecoveryFencePlan *plan)
{
	uint64 current_replay[2] = { 0, 0 };
	uint64 current_foreign[2];
	XLogRecPtr current_start[CLUSTER_WAL_STATE_SLOT_COUNT + 1];
	uint16 current_origins[CLUSTER_WAL_STATE_SLOT_COUNT];
	uint16 current_count = 0;
	ClusterMergeEngage engage;
	uint16 tid;

	if (!recovery_fence_plan_valid(plan) || !plan->sealed || !plan->serial_held || plan->committed)
		return recovery_fence_refused(plan, "commit_state", 0, -1, 0);
	if (!recovery_fence_deadline_current(plan))
		return recovery_fence_refused(plan, "merge_deadline", 0, -1, 0);
	MemSet(current_start, 0, sizeof(current_start));
	engage = cluster_recovery_merge_project_readonly(
		plan->own_thread, plan->start_lsn[plan->own_thread], current_replay, current_start);
	if (!recovery_fence_deadline_current(plan))
		return recovery_fence_refused(plan, "merge_deadline", 0, -1, 0);
	if (engage != CLUSTER_MERGE_ENGAGE)
		return recovery_fence_refused(plan, "commit_project", 0, -1, (int)engage);
	current_foreign[0] = current_replay[0];
	current_foreign[1] = current_replay[1];
	current_foreign[(plan->own_thread - 1) / 64] &= ~(UINT64_C(1) << ((plan->own_thread - 1) % 64));

	for (tid = 1; tid <= CLUSTER_WAL_STATE_SLOT_COUNT; tid++) {
		ClusterControlRootSnapshot snapshot;
		ClusterControlRootReadToken token;
		ClusterControlRootResult root_result;
		ClusterRecoveryDutyCompare duty_result;
		int origin_index;

		if ((current_foreign[(tid - 1) / 64] & (UINT64_C(1) << ((tid - 1) % 64))) == 0)
			continue;
		origin_index = recovery_fence_plan_find_origin(plan, tid);
		if (origin_index < 0)
			return recovery_fence_refused(plan, "commit_origin", tid, origin_index, 0);
		root_result = cluster_control_root_read_canonical(tid, &plan->origins[origin_index].duty,
														  CLUSTER_CONTROL_ROOT_READ_STRONG,
														  &snapshot, &token);
		if (root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
			&& root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
			return recovery_fence_refused(plan, "commit_root_read", tid, origin_index,
										  (int)root_result);
		if (snapshot.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED)
			return recovery_fence_refused(plan, "commit_root_lifecycle", tid, origin_index,
										  (int)snapshot.lifecycle);
		duty_result = cluster_recovery_duty_key_compare_for_claim(
			&snapshot.identity, &plan->origins[origin_index].duty, cluster_shared_config);
		if (duty_result != CLUSTER_RECOVERY_DUTY_COMPARE_EXACT) {
			if (duty_result == CLUSTER_RECOVERY_DUTY_COMPARE_DIFFERENT) {
				const ClusterRecoveryDutyKey *expected = &plan->origins[origin_index].duty;
				const unsigned char *a = (const unsigned char *)expected;
				const unsigned char *b = (const unsigned char *)&snapshot.identity;
				const char *field = "padding";
				char detail[192];
				size_t byte = 0;

				/* DIFFERENT already proves both original keys valid. Reserved
				 * bytes are zero; inspect the copies, never reread their owner. */
				while (byte < sizeof(*expected) && a[byte] == b[byte])
					byte++;
#define DUTY_FIELD(member)                                                                         \
	if (byte >= offsetof(ClusterRecoveryDutyKey, member)                                           \
		&& byte < offsetof(ClusterRecoveryDutyKey, member) + sizeof(expected->member))             \
	field = #member
				DUTY_FIELD(system_identifier);
				DUTY_FIELD(storage_uuid);
				DUTY_FIELD(authority_uuid);
				DUTY_FIELD(origin_thread_id);
				DUTY_FIELD(origin_node_id);
				DUTY_FIELD(thread_claim_created_at);
				DUTY_FIELD(thread_claim_crc32c);
				DUTY_FIELD(origin_owner_incarnation);
				DUTY_FIELD(root_lineage_seq);
#undef DUTY_FIELD
				snprintf(detail, sizeof(detail), "field=%s byte=%zu expected=%u observed=%u", field,
						 byte, a[byte], b[byte]);
				return recovery_fence_refused_detail(plan, "commit_root_duty", tid, origin_index,
													 (int)duty_result, detail);
			}
			return recovery_fence_refused(plan, "commit_root_duty", tid, origin_index,
										  (int)duty_result);
		}
		if (memcmp(&token, &plan->origins[origin_index].root_token, sizeof(token)) != 0) {
			const ClusterControlRootReadToken *expected = &plan->origins[origin_index].root_token;
			for (unsigned byte = 0; byte < sizeof(token.authority_uuid); byte++)
				if (token.authority_uuid[byte] != expected->authority_uuid[byte]) {
					char field[48];
					snprintf(field, sizeof(field), "root_token.authority_uuid[%u]", byte);
					return recovery_fence_changed(plan, "commit_root_token", tid, origin_index,
												  field, expected->authority_uuid[byte],
												  token.authority_uuid[byte]);
				}
#define TOKEN_CHANGED(field)                                                                       \
	do {                                                                                           \
		if (token.field != expected->field)                                                        \
			return recovery_fence_changed(plan, "commit_root_token", tid, origin_index,            \
										  "root_token." #field, expected->field, token.field);     \
	} while (0)
			TOKEN_CHANGED(origin_thread_id);
			TOKEN_CHANGED(source);
			TOKEN_CHANGED(lifecycle);
			TOKEN_CHANGED(reserved20);
			TOKEN_CHANGED(root_lineage_seq);
			TOKEN_CHANGED(reserved32);
			TOKEN_CHANGED(file_txn_seq);
			TOKEN_CHANGED(root_publish_seq);
			TOKEN_CHANGED(record_crc32c);
			TOKEN_CHANGED(root_flags);
#undef TOKEN_CHANGED
			return recovery_fence_refused(plan, "commit_root_token", tid, origin_index, 0);
		}
		if (cluster_shared_config) {
			const ClusterControlRootSnapshot *expected = &plan->origins[origin_index].root_snapshot;
#define EXTENT_CHANGED(field)                                                                      \
	do {                                                                                           \
		if (snapshot.field != expected->field)                                                     \
			return recovery_fence_changed(plan, "commit_root_extent", tid, origin_index, #field,   \
										  expected->field, snapshot.field);                        \
	} while (0)
			EXTENT_CHANGED(checkpoint_tli);
			EXTENT_CHANGED(tail_tli);
			EXTENT_CHANGED(checkpoint_lower_lsn);
			EXTENT_CHANGED(validated_tail_lsn_exclusive);
#undef EXTENT_CHANGED
		}
		current_origins[current_count++] = tid;
	}
	if (!cluster_recovery_fence_plan_shape_valid(plan->own_thread, current_replay, current_foreign,
												 current_origins, current_count))
		return recovery_fence_refused(plan, "commit_shape", 0, -1, 0);
	for (unsigned word = 0; word < lengthof(current_replay); word++) {
		char field[48];

		if (current_replay[word] != plan->replay_thread_bitmap[word]) {
			snprintf(field, sizeof(field), "replay_thread_bitmap[%u]", word);
			return recovery_fence_changed(plan, "commit_shape", 0, -1, field,
										  plan->replay_thread_bitmap[word], current_replay[word]);
		}
	}
	for (unsigned word = 0; word < lengthof(current_foreign); word++) {
		char field[48];

		if (current_foreign[word] != plan->foreign_origin_bitmap[word]) {
			snprintf(field, sizeof(field), "foreign_origin_bitmap[%u]", word);
			return recovery_fence_changed(plan, "commit_shape", 0, -1, field,
										  plan->foreign_origin_bitmap[word], current_foreign[word]);
		}
	}
	if (current_count != plan->origin_count)
		return recovery_fence_changed(plan, "commit_shape", 0, -1, "origin_count",
									  plan->origin_count, current_count);
	for (tid = 1; tid <= CLUSTER_WAL_STATE_SLOT_COUNT; tid++) {
		if ((current_replay[(tid - 1) / 64] & (UINT64_C(1) << ((tid - 1) % 64))) != 0
			&& current_start[tid] != plan->start_lsn[tid])
			return recovery_fence_changed(plan, "commit_start_lsn", tid, -1, "start_lsn",
										  plan->start_lsn[tid], current_start[tid]);
	}
	if (!cluster_recovery_merge_fence_plan_revalidate_nowait(plan))
		return false;
	if (!recovery_fence_deadline_current(plan))
		return recovery_fence_refused(plan, "merge_deadline", 0, -1, 0);
	plan->committed = true;
	return true;
}

/* Copy one fenced origin's exact ROOT input.  Read-only; grants nothing. */
bool
cluster_recovery_merge_fence_plan_origin(const ClusterRecoveryFencePlan *plan, uint16 index,
										 uint16 *origin_thread, ClusterControlRootSnapshot *root,
										 ClusterControlRootReadToken *token)
{
	if (!recovery_fence_plan_valid(plan) || !plan->sealed || index >= plan->origin_count
		|| origin_thread == NULL || root == NULL || token == NULL)
		return false;
	*origin_thread = plan->origins[index].origin_thread;
	*root = plan->origins[index].root_snapshot;
	*token = plan->origins[index].root_token;
	return true;
}

uint16
cluster_recovery_merge_fence_plan_origin_count(const ClusterRecoveryFencePlan *plan)
{
	return recovery_fence_plan_valid(plan) && plan->sealed ? plan->origin_count : 0;
}

bool
cluster_recovery_merge_fence_plan_revalidate_nowait(ClusterRecoveryFencePlan *plan)
{
	PgracExternalFenceDenyReason reason;
	ClusterWalPinResult pin_result;
	uint16 i;

	if (!recovery_fence_plan_valid(plan) || !plan->sealed || !plan->serial_held
		|| plan->origin_count == 0 || plan->serial_guards.count != plan->origin_count)
		return recovery_fence_refused(plan, "revalidate_state", 0, -1, 0);
	if (cluster_shared_config) {
		if (plan->retention_pin == NULL)
			return recovery_fence_refused(plan, "revalidate_pin_missing", 0, -1, 0);
		pin_result = cluster_wal_retention_pin_revalidate(plan->retention_pin);
		if (pin_result != CLUSTER_WAL_PIN_OK)
			return recovery_fence_refused(plan, "revalidate_pin", 0, -1, (int)pin_result);
	}
	for (i = 0; i < plan->origin_count; i++) {
		ClusterRecoveryFenceOrigin *origin = &plan->origins[i];
		ClusterRecoverySerialRevalidateResult serial_result;
		ClusterFormationWitnessResult formation_result;

		serial_result = cluster_recovery_serial_revalidate(&plan->serial_guards.guards[i]);
		if (serial_result != CLUSTER_RECOVERY_SERIAL_CURRENT)
			return recovery_fence_refused(plan, "revalidate_serial", origin->origin_thread, i,
										  (int)serial_result);
		formation_result = cluster_formation_witness_revalidate_nowait(origin->formation);
		if (formation_result != CLUSTER_FORMATION_WITNESS_READY)
			return recovery_fence_refused(plan, "revalidate_formation", origin->origin_thread, i,
										  (int)formation_result);
		if (!cluster_external_fence_need_set_revalidate_nowait(origin->needs, origin->formation,
																&reason))
			return recovery_fence_refused(plan, "revalidate_needs", origin->origin_thread, i,
										  (int)reason);
		if (!cluster_external_fence_revalidate_set_nowait(origin->admissions, origin->needs,
														   origin->formation, &reason))
			return recovery_fence_refused(plan, "revalidate_admissions", origin->origin_thread, i,
										  (int)reason);
	}
	return true;
}

/* Borrow only the exact original owners; an origin number or COLD_FORMED
 * enum alone cannot authorize a canonical SIDE/PAGE mutation. */
bool
cluster_recovery_merge_fence_plan_authority(ClusterRecoveryFencePlan *plan, uint16 origin_thread,
											ClusterThreadRecoveryAuthorityV1 *out)
{
	ClusterThreadRecoveryAuthorityV1 authority;
	ClusterRecoveryFenceOrigin *origin;
	int index;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (!recovery_fence_plan_valid(plan) || !plan->committed
		|| !cluster_recovery_merge_fence_plan_revalidate_nowait(plan)
		|| (index = recovery_fence_plan_find_origin(plan, origin_thread)) < 0)
		return false;
	origin = &plan->origins[index];
	memset(&authority, 0, sizeof(authority));
	authority.duty = &origin->duty;
	authority.root_snapshot = &origin->root_snapshot;
	authority.root_token = &origin->root_token;
	authority.formation = origin->formation;
	authority.fence_need_set = origin->needs;
	authority.fence_admission_set = origin->admissions;
	authority.retention_pin = plan->retention_pin;
	authority.serial_guard = &plan->serial_guards.guards[index];
	if (authority.serial_guard->mode != CLUSTER_RECOVERY_SERIAL_COLD_FORMED
		|| cluster_thread_recovery_authority_revalidate_nowait_v1(&authority)
			   != CLUSTER_THREAD_AUTHORITY_OK)
		return false;
	*out = authority;
	return true;
}

/* Freeze only original input references while the original execution owners
 * still protect them. The complete set is checked before publishing any copy;
 * this cannot bind a new source after IR/WALR have been surrendered. */
bool
cluster_recovery_merge_fence_plan_bind_tx_inputs_v1(ClusterRecoveryFencePlan *plan,
	const ClusterWalSourceRef *sources, uint32 count)
{
	uint32 selected[CLUSTER_WAL_STATE_SLOT_COUNT];

	if (!recovery_fence_plan_valid(plan) || !cluster_shared_config || !plan->committed
		|| plan->tx_inputs_bound || plan->completed || sources == NULL || count == 0
		|| count > CLUSTER_WAL_STATE_SLOT_COUNT || plan->owner == NULL
		|| plan->owner != CurrentResourceOwner || plan->owner_boot == 0
		|| plan->owner_boot != cluster_qvotec_get_self_incarnation()
		|| !cluster_recovery_merge_fence_plan_revalidate_nowait(plan))
		return false;
	/* The input is a separate original typed allocation, never this opaque
	 * owner's storage. Refuse overlap before any source dereference/copy. */
	if ((uintptr_t)sources <= (uintptr_t)plan
		? (uintptr_t)plan - (uintptr_t)sources < sizeof(*sources) * count
		: (uintptr_t)sources - (uintptr_t)plan < sizeof(*plan))
		return false;
	for (uint16 i = 0; i < plan->origin_count; i++) {
		const ClusterRecoveryFenceOrigin *origin = &plan->origins[i];

		selected[i] = UINT32_MAX;
		for (uint32 p = 0; p < count; p++) {
			if (sources[p].claim.identity.origin_thread_id != origin->origin_thread)
				continue;
			if (selected[i] != UINT32_MAX
				|| !cluster_wal_claim_v2_ref_valid(&sources[p].claim)
				|| memcmp(&sources[p].claim.identity, &origin->duty, sizeof(origin->duty)) != 0
				|| sources[p].timeline != origin->root_snapshot.checkpoint_tli)
				return false;
			selected[i] = p;
		}
		if (selected[i] == UINT32_MAX)
			return false;
	}
	/* All fallible checks precede this publication, including the last
	 * complete original execution/proof revalidation. */
	if (!cluster_recovery_merge_fence_plan_revalidate_nowait(plan))
		return false;
	for (uint16 i = 0; i < plan->origin_count; i++)
		memcpy(&plan->origins[i].tx_source, &sources[selected[i]], sizeof(*sources));
	plan->tx_inputs_bound = true;
	return true;
}

/* Original isolation evidence survives COMPLETE and INSTALL as owned input.
 * It is not the released RR serial/pin and grants no replay/PAGE permission.
 * A source, membership, provider or process-owner change must not be adopted
 * by this same post-EOR attempt. Ordinary RR revalidation remains unchanged. */
bool
cluster_recovery_merge_fence_plan_tx_current_v1(ClusterRecoveryFencePlan *plan,
	const ClusterWalSourceRef *source)
{
	PgracExternalFenceDenyReason reason;
	bool matched = source == NULL;

	if (!recovery_fence_plan_valid(plan) || !cluster_shared_config || !plan->sealed
		|| !plan->committed || !plan->completed || !plan->tx_inputs_bound || plan->tx_stale
		|| plan->serial_held || plan->serial_guards.count != 0 || plan->retention_pin != NULL
		|| plan->origin_count == 0)
		return false;
	if (plan->owner == NULL || plan->owner != CurrentResourceOwner || plan->owner_boot == 0
		|| plan->owner_boot != cluster_qvotec_get_self_incarnation()) {
		plan->tx_stale = true;
		return false;
	}
	for (uint16 i = 0; i < plan->origin_count; i++) {
		const ClusterRecoveryFenceOrigin *origin = &plan->origins[i];

		if (cluster_formation_witness_revalidate_nowait(origin->formation)
				!= CLUSTER_FORMATION_WITNESS_READY
			|| !cluster_external_fence_need_set_revalidate_nowait(
				origin->needs, origin->formation, &reason)
			|| !cluster_external_fence_revalidate_set_nowait(
				origin->admissions, origin->needs, origin->formation, &reason)) {
			plan->tx_stale = true;
			return false;
		}
		if (source != NULL && memcmp(source, &origin->tx_source, sizeof(*source)) == 0)
			matched = true;
	}
	return matched;
}

bool
cluster_recovery_merge_fence_plan_tx_request_v1(ClusterRecoveryFencePlan *plan,
	const ClusterWalSourceRef *source, ClusterRecoverySerialRequest *out)
{
	ClusterRecoverySerialRequest request = {0};

	/* No write to an opaque owner or the caller's original source on refusal. */
	if (out == NULL || source == NULL || plan == NULL
		|| ((uintptr_t)out <= (uintptr_t)plan
			? (uintptr_t)plan - (uintptr_t)out < sizeof(*out)
			: (uintptr_t)out - (uintptr_t)plan < sizeof(*plan))
		|| ((uintptr_t)out <= (uintptr_t)source
			? (uintptr_t)source - (uintptr_t)out < sizeof(*out)
			: (uintptr_t)out - (uintptr_t)source < sizeof(*source))
		|| !cluster_recovery_merge_fence_plan_tx_current_v1(plan, source))
		return false;
	for (uint16 i = 0; i < plan->origin_count; i++) {
		const ClusterRecoveryFenceOrigin *origin = &plan->origins[i];
		if (memcmp(source, &origin->tx_source, sizeof(*source)) != 0)
			continue;
		request.mode = CLUSTER_RECOVERY_SERIAL_TX_CLOSURE;
		request.duty = origin->duty;
		request.expected_root_token = origin->root_token;
		request.formation = origin->formation;
		request.fence_need_set = origin->needs;
		request.fence_admission_set = origin->admissions;
		request.acquire_timeout_ms = plan->acquire_timeout_ms_snapshot;
		request.release_timeout_ms = plan->acquire_timeout_ms_snapshot;
		if (!cluster_recovery_merge_fence_plan_tx_current_v1(plan, source))
			return false;
		*out = request;
		return true;
	}
	return false;
}

/*
 * PGRAC (S9P2-05): publish every fenced origin recovered, in the order the
 * online recovery worker uses for one origin.  Each origin's authority and
 * terminal patch are taken while the serial set is still held; once the
 * pin is sealed the serial set is surrendered, and only a confirmed
 * release lets the finalizer compare-and-swap each root through the sealed
 * pin.  The first failure stops: later origins stay RECOVERY_REQUIRED and
 * the next cold start recovers them again.  Author: SqlRush
 * <sqlrush@gmail.com>
 */
ClusterRecoveryFenceCompleteV1
cluster_recovery_merge_fence_plan_complete_v1(ClusterRecoveryFencePlan *plan, uint16 *failed_thread,
											  int *failed_detail)
{
	ClusterThreadRecoveryAuthorityV1 authority[CLUSTER_WAL_STATE_SLOT_COUNT];
	ClusterControlRootPatch patch[CLUSTER_WAL_STATE_SLOT_COUNT];
	ClusterRecoverySerialReleaseResult released;
	ClusterWalPinResult sealed;
	ClusterWalrReleaseResult pin_released;
	uint16 i;

	*failed_thread = 0;
	*failed_detail = 0;
	if (!recovery_fence_plan_valid(plan) || !plan->committed || !plan->serial_held
		|| plan->retention_pin == NULL || plan->origin_count == 0)
		return CLUSTER_RECOVERY_FENCE_COMPLETE_INVALID;
	for (i = 0; i < plan->origin_count; i++) {
		const ClusterRecoveryFenceOrigin *origin = &plan->origins[i];

		*failed_thread = origin->origin_thread;
		if (!cluster_recovery_merge_fence_plan_authority(plan, origin->origin_thread,
														 &authority[i]))
			return CLUSTER_RECOVERY_FENCE_COMPLETE_AUTHORITY;
		if (!cluster_thread_recovery_root_complete_patch_build_v1(
				&authority[i], origin->root_snapshot.validated_tail_lsn_exclusive, &patch[i]))
			return CLUSTER_RECOVERY_FENCE_COMPLETE_PATCH;
	}
	*failed_thread = 0;
	sealed = cluster_wal_retention_pin_seal_for_root_publish(plan->retention_pin);
	if (sealed != CLUSTER_WAL_PIN_OK) {
		*failed_detail = (int)sealed;
		return CLUSTER_RECOVERY_FENCE_COMPLETE_PIN_SEAL;
	}
	/* Starting the release surrenders the set's authority (release_serial). */
	plan->serial_held = false;
	released = cluster_recovery_serial_release_set(&plan->serial_guards);
	if (released != CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED) {
		*failed_detail = (int)released;
		return CLUSTER_RECOVERY_FENCE_COMPLETE_IR_RELEASE;
	}
	for (i = 0; i < plan->origin_count; i++) {
		ClusterThreadRecoveryRootFinalizeResultV1 finalized;
		ClusterControlRootSnapshot published;

		finalized = cluster_thread_recovery_root_finalize_after_ir_v1(&authority[i], &patch[i],
																	  &published);
		if (finalized != CLUSTER_THREAD_ROOT_FINALIZE_OK
			&& finalized != CLUSTER_THREAD_ROOT_FINALIZE_ALREADY_COMPLETE) {
			*failed_thread = plan->origins[i].origin_thread;
			*failed_detail = (int)finalized;
			return CLUSTER_RECOVERY_FENCE_COMPLETE_FINALIZE;
		}
	}
	pin_released = cluster_wal_retention_pin_release(&plan->retention_pin);
	if (pin_released != CLUSTER_WALR_RELEASE_CONFIRMED) {
		*failed_detail = (int)pin_released;
		return CLUSTER_RECOVERY_FENCE_COMPLETE_PIN_RELEASE;
	}
	plan->completed = true;
	return CLUSTER_RECOVERY_FENCE_COMPLETE_OK;
}

bool
cluster_recovery_merge_fence_plan_release_serial(ClusterRecoveryFencePlan *plan)
{
	ClusterRecoverySerialReleaseResult result;

	if (!recovery_fence_plan_valid(plan))
		return false;
	/* A failed set acquisition can still own earlier guards. Conversely,
	 * starting release surrenders full-set authority even while ACKs wait. */
	plan->serial_held = false;
	if (plan->serial_guards.count != 0) {
		result = cluster_recovery_serial_release_set(&plan->serial_guards);
		if (result != CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED)
			return false;
	}
	if (plan->retention_pin != NULL
		&& cluster_wal_retention_pin_release(&plan->retention_pin)
			   != CLUSTER_WALR_RELEASE_CONFIRMED)
		return false;
	return true;
}

void
cluster_recovery_merge_fence_plan_destroy(ClusterRecoveryFencePlan **plan)
{
	ClusterRecoveryFencePlan *owned;

	if (plan == NULL || *plan == NULL)
		return;
	owned = *plan;
	if (!recovery_fence_plan_valid(owned) || owned->serial_held || owned->serial_guards.count != 0
		|| owned->retention_pin != NULL)
		return;
	recovery_fence_plan_release_members(owned);
	MemSet(owned, 0, sizeof(*owned));
	pfree(owned);
	*plan = NULL;
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
			 * candidate's CANONICAL control-root validated tail (RF-ROOT
			 * P7 G1b-A; refreshed per checkpoint via CHECKPOINT_ADVANCE).
			 * A stream whose decode stops short of it is corrupt below the
			 * validated end, not a torn tail -- fail-closed in the helper.
			 * Equivalence: the root tail is a VALIDATED bound (vs the old
			 * registry highest_lsn = an unvalidated write position), and
			 * the inter-checkpoint gap's records are still integrity-
			 * checked by the scan's own record framing — the floor only
			 * rejects a decode stopping short of a known-validated point. */
			ClusterControlRootIdentity root_identity;
			ClusterControlRootSnapshot root_snapshot;
			ClusterControlRootReadToken root_token;
			ClusterControlRootResult root_result;
			XLogRecPtr validated_min = InvalidXLogRecPtr;

			if (!restore_mode) {
				root_result = cluster_control_root_lookup_owner_by_node_runtime(
					(int)tid - 1, &root_identity, &root_snapshot, &root_token);
				if ((root_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
					 || root_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
					&& cluster_recovery_duty_key_compare_for_claim(
						   &root_identity, &root_snapshot.identity, cluster_shared_config)
						   == CLUSTER_RECOVERY_DUTY_COMPARE_EXACT
					&& root_snapshot.validated_tail_lsn_exclusive > (uint64)start_lsn[tid])
					validated_min = (XLogRecPtr)root_snapshot.validated_tail_lsn_exclusive;
			}
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
