/*-------------------------------------------------------------------------
 *
 * cluster_write_fence.c
 *	  pgrac internal cooperative write-fence -- local token region + hot-path
 *	  judge wrapper (spec-4.12 D3, split-brain recovery guard).
 *
 *	  The "pgrac cluster write fence" shmem region holds the local write-fence
 *	  token: the authorized_epoch + lease + self_fenced bit that qvotec (D2)
 *	  refreshes every poll from the durable voting-disk fence marker.  The hot
 *	  shared-storage write paths (D5) call cluster_write_fence_allowed() -- a
 *	  pure-read, no-throw, critical-section-safe check (no LWLock, no durable
 *	  I/O) -- and fail closed by context (ERROR / PANIC / BLOCKED) when this node
 *	  is stale / superseded / lease-expired / self-fenced.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_write_fence.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.12-cooperative-write-fence.md (FROZEN v1.0)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "miscadmin.h"	   /* CritSectionCount */
#include "storage/ipc.h"   /* on_shmem_exit */
#include "storage/latch.h" /* Latch, SetLatch (D4 latch-wake) */
#include "storage/shmem.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h" /* D4 marker-write + D6 verify wait events */

#include "cluster/cluster_epoch.h"			/* cluster_epoch_get_current */
#include "cluster/cluster_guc.h"			/* cluster_node_id, cluster_voting_disks */
#include "cluster/cluster_lmon.h"			/* cluster_lmon_marker_complete_wakeup */
#include "cluster/cluster_qvotec.h"			/* ClusterVotingSlot (marker layout asserts) */
#include "cluster/cluster_shmem.h"			/* cluster_shmem_register_region */
#include "cluster/cluster_voting_disk_io.h" /* D6 direct durable marker read */
#include "cluster/cluster_write_fence.h"	/* region + judge + wrapper + marker */

/*
 * spec-4.12 D1: pin the fence-marker on-disk layout against the voting slot, so a
 * future change to either side fails to compile rather than silently corrupting
 * the durable marker (the marker rides in _reserved1, protected by the slot CRC).
 */
StaticAssertDecl(offsetof(ClusterVotingSlot, _reserved1) == 128,
				 "fence marker assumes voting slot _reserved1 at offset 128");
StaticAssertDecl(sizeof(ClusterFenceMarker) == CLUSTER_FENCE_MARKER_BYTES,
				 "ClusterFenceMarker must be exactly CLUSTER_FENCE_MARKER_BYTES");
StaticAssertDecl(CLUSTER_FENCE_MARKER_BYTES <= sizeof(((ClusterVotingSlot *)0)->_reserved1),
				 "fence marker must fit in the voting slot _reserved1 area");
/* spec-4.12b D5: the refresh guard copies/compares the marker dead bitmap against
 * the shmem token's RECONFIG-width dead bitmap; pin them equal so a future width
 * change to either constant fails to compile rather than over-reading (P2-4). */
StaticAssertDecl(CLUSTER_RECONFIG_DEAD_BITMAP_BYTES == CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES,
				 "reconfig and fence-marker dead bitmaps must be the same width");

/*
 * GUC storage lives in cluster_guc.c (the project convention -- the DefineCustom*
 * registration is there, D7); this file references them via the header externs.
 */

static ClusterWriteFenceShmem *cluster_write_fence_shmem = NULL;

/*
 * D4 qvotec-side in-flight tracking (qvotec is single-process, so file statics are
 * safe).  last_processed advances only after a marker is fully written + acked, so
 * a qvotec crash between poll and complete reprocesses the request (idempotent).
 */
static uint64 qvotec_inflight_marker_seq = 0;
static uint64 qvotec_last_processed_marker_seq = 0;

static Size
cluster_write_fence_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterWriteFenceShmem));
}

static void
cluster_write_fence_shmem_init(void)
{
	bool found;

	cluster_write_fence_shmem = (ClusterWriteFenceShmem *)ShmemInitStruct(
		"pgrac cluster write fence", cluster_write_fence_shmem_size(), &found);
	if (!found) {
		/*
		 * Postmaster restart rebuilds shmem: a node starts with no fence token
		 * (authorized_epoch 0, lease 0).  With enforcement ON the hot gate then
		 * fails closed (lease 0 -> expired) until qvotec's first poll refreshes
		 * the token from the durable marker (D2) -- fail-closed-until-proven is
		 * the correct startup posture (8.A).
		 */
		pg_atomic_init_u64(&cluster_write_fence_shmem->authorized_epoch, 0);
		pg_atomic_init_u64(&cluster_write_fence_shmem->lease_expire_at_us, 0);
		pg_atomic_init_u32(&cluster_write_fence_shmem->self_fenced, 0);
		memset(cluster_write_fence_shmem->fenced_dead_bitmap, 0,
			   sizeof(cluster_write_fence_shmem->fenced_dead_bitmap));
		pg_atomic_init_u64(&cluster_write_fence_shmem->fence_event_id, 0);
		pg_atomic_init_u32(&cluster_write_fence_shmem->fence_engaged, 0); /* D3: not latched */
		pg_atomic_init_u64(&cluster_write_fence_shmem->hot_gate_blocked, 0);
		pg_atomic_init_u64(&cluster_write_fence_shmem->durable_check_blocked, 0);
		pg_atomic_init_u64(&cluster_write_fence_shmem->minority_marker_ignored, 0);
		pg_atomic_init_u64(&cluster_write_fence_shmem->baseline_stale_rejected, 0);
		pg_atomic_init_u64(&cluster_write_fence_shmem->baseline_published, 0); /* D6 */
		pg_atomic_init_u32(&cluster_write_fence_shmem->baseline_author_is_self, 0);
		pg_atomic_init_u64(&cluster_write_fence_shmem->last_authority_refresh_us, 0);

		/* D4 LMON->qvotec submit mailbox starts empty (request == completion). */
		cluster_write_fence_shmem->qvotec_latch = NULL;
		pg_atomic_init_u64(&cluster_write_fence_shmem->marker_request_seq, 0);
		pg_atomic_init_u64(&cluster_write_fence_shmem->marker_completion_seq, 0);
		pg_atomic_init_u32(&cluster_write_fence_shmem->marker_result,
						   CLUSTER_FENCE_MARKER_SUBMIT_FAILED);
		pg_atomic_init_u64(&cluster_write_fence_shmem->marker_write_failed, 0);
		memset(&cluster_write_fence_shmem->pending_marker, 0,
			   sizeof(cluster_write_fence_shmem->pending_marker));
	}
}

static const ClusterShmemRegion cluster_write_fence_region = {
	.name = "pgrac cluster write fence",
	.size_fn = cluster_write_fence_shmem_size,
	.init_fn = cluster_write_fence_shmem_init,
	.lwlock_count = 0, /* atomics-only token + mailbox; no LWLock */
	.owner_subsys = "cluster_write_fence",
	.reserved_flags = 0,
};

void
cluster_write_fence_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_write_fence_region);
}

/*
 * cluster_write_fence_enforcing -- is the cooperative write-fence ACTIVELY enforced
 *	right now?  True iff the GUC is ON *and* voting disks are configured (spec-4.12b
 *	D4).  With default-ON, a single node / no-voting-disk deployment auto-degrades to
 *	a no-op (equivalent to dev): there is no shared storage to fence and qvotec never
 *	refreshes a token, so enforcing would wrongly fail single-node recovery closed
 *	(verify_durable) with zero safety benefit.  Derived only from GUCs, which
 *	propagate correctly across fork AND EXEC_BACKEND, so every backend agrees without
 *	a postmaster-once mutation.  An explicit voting_disks misconfiguration (paths set
 *	but unreadable) still fails closed via the durable-authority quorum below.
 */
bool
cluster_write_fence_enforcing(void)
{
	return cluster_write_fence_enforcement == CLUSTER_WRITE_FENCE_ENFORCE_ON
		   && cluster_voting_disks != NULL && cluster_voting_disks[0] != '\0';
}

/*
 * cluster_write_fence_allowed -- the hot-path wrapper (spec-4.12 D3).  Reads the
 *	live epoch + the shmem token and applies the PURE judge.  No LWLock, no durable
 *	I/O -- safe to call from a critical section (the caller decides the fail-closed
 *	action by context: ERROR / PANIC / BLOCKED).  L110: a detached region (token
 *	not attached) yields enforcement_on with region_attached=false -> fail-closed.
 */
bool
cluster_write_fence_allowed(void)
{
	bool enforcement_on = cluster_write_fence_enforcing(); /* D4: ON + voting disks */
	bool region_attached = (cluster_write_fence_shmem != NULL);
	uint64 epoch_current = cluster_epoch_get_current();
	uint64 authorized_epoch = 0;
	uint64 lease_expire_us = 0;
	bool self_fenced = false;
	bool fence_engaged = false;
	uint64 now_us;

	if (region_attached) {
		/*
		 * Read the lease FIRST, then barrier, then epoch + self_fenced.  qvotec
		 * (the sole writer, cluster_write_fence_refresh_from_marker) invalidates
		 * the lease, writes epoch/self_fenced, write-barriers, then publishes the
		 * lease LAST.  So a non-expired lease observed here guarantees the epoch /
		 * self_fenced we read next are the matching freshly-published values -- a
		 * stale-epoch torn read can only come with a stale (already-invalidated)
		 * lease, which fails closed.  The only residual is the bounded R4 lease
		 * window (marker on disk but lease not yet aged out), an accepted risk.
		 */
		lease_expire_us = pg_atomic_read_u64(&cluster_write_fence_shmem->lease_expire_at_us);
		pg_read_barrier();
		authorized_epoch = pg_atomic_read_u64(&cluster_write_fence_shmem->authorized_epoch);
		self_fenced = (pg_atomic_read_u32(&cluster_write_fence_shmem->self_fenced) != 0);
		/* D3 latch: a relaxed read is fine -- it is one-way, so the worst stale
		 * read grants one extra grace cycle to an in-quorum bring-up node. */
		fence_engaged = (pg_atomic_read_u32(&cluster_write_fence_shmem->fence_engaged) != 0);
	}

	/*
	 * spec-4.12b D3 (Q3=A): before the first authority ever latches, do NOT
	 * hard-block hot writes -- grant a bring-up grace (the qvotec quorum backend
	 * gate covers no-quorum).  A durable self-fence latches fence_engaged via
	 * startup_self_check before the node serves, so a self-fenced node never gets
	 * the grace.  After the latch, fall through to the strict pure judge.
	 */
	if (cluster_write_fence_grace_before_engage(enforcement_on, region_attached, fence_engaged,
												self_fenced))
		return true;

	/* TimestampTz is microseconds since 2000-01-01; the lease is in the same unit. */
	now_us = (uint64)GetCurrentTimestamp();

	return cluster_write_fence_decide(enforcement_on, region_attached, epoch_current,
									  authorized_epoch, now_us, lease_expire_us, self_fenced);
}

/*
 * cluster_write_fence_read_durable_authority -- spec-4.12 D6 helper.  DIRECTLY read
 *	the voting-disk fence markers (bypassing the qvotec cache token) and decide
 *	authority over the TOTAL configured disk count.  Returns false when no voting
 *	disks are configured (caller decides what that means).  An unreadable disk stays
 *	has_marker=false and still counts toward the total, so a minority of readable
 *	disks can never fake a majority (P0a).  Reuses cluster_fence_authority_decide.
 */
static bool
cluster_write_fence_read_durable_authority(ClusterFenceAuthority *out)
{
	ClusterFenceMarker disk_markers[CLUSTER_MAX_VOTING_DISKS];
	/* Zero-init: a disk slot never assigned (unreadable / skipped token) stays
	 * has_marker=false, which is the correct fail-closed default and keeps the
	 * authority quorum honest (it counts toward the configured total). */
	bool disk_has_marker[CLUSTER_MAX_VOTING_DISKS] = { false };
	int n_total = 0;
	const char *p;

	if (cluster_voting_disks == NULL || cluster_voting_disks[0] == '\0')
		return false; /* no voting disks configured (no I/O -> no wait event) */

	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_WRITE_FENCE_VERIFY);
	p = cluster_voting_disks;
	while (*p && n_total < CLUSTER_MAX_VOTING_DISKS) {
		const char *start = p;
		const char *end;
		char path[MAXPGPATH];
		size_t len;
		int fd;
		int slot_idx = n_total;

		while (*p && *p != ',')
			p++;
		end = p;
		if (*p == ',')
			p++;
		/* Trim leading / trailing whitespace.  Explicit-break form so a static
		 * analyzer sees the loop can exit while end > start (a non-whitespace
		 * tail), i.e. len is not provably zero. */
		while (start < end && (*start == ' ' || *start == '\t'))
			start++;
		while (end > start) {
			char tail = end[-1];

			if (tail != ' ' && tail != '\t')
				break;
			end--;
		}
		if (start >= end)
			continue; /* empty token (trailing / double comma): skip */
		len = (size_t)(end - start);
		if (len >= MAXPGPATH)
			continue; /* oversized token: skip without consuming a disk slot */

		memcpy(path, start, len);
		path[len] = '\0';

		disk_has_marker[slot_idx] = false;
		fd = cluster_voting_disk_open(path, false);
		if (fd >= 0) {
			uint32 node;
			bool found = false;
			ClusterFenceMarker best;

			memset(&best, 0, sizeof(best));
			for (node = 0; node < CLUSTER_MAX_NODES; node++) {
				ClusterVotingSlot slot;
				ClusterFenceMarker m;

				if (cluster_voting_disk_read_slot(fd, slot_idx, node, &slot)
					!= CLUSTER_VOTING_DISK_IO_OK)
					continue;
				if (!cluster_fence_marker_unpack(slot._reserved1, &m))
					continue;
				if (!found || m.fence_epoch > best.fence_epoch
					|| (m.fence_epoch == best.fence_epoch
						&& m.fence_generation > best.fence_generation)) {
					best = m;
					found = true;
				}
			}
			cluster_voting_disk_close(fd);
			disk_has_marker[slot_idx] = found;
			if (found)
				disk_markers[slot_idx] = best;
		}
		n_total++;
	}

	pgstat_report_wait_end();

	*out = cluster_fence_authority_decide(disk_markers, disk_has_marker, n_total);
	return true;
}

/*
 * cluster_write_fence_verify_durable -- spec-4.12 D6 (Option B, Oracle-aligned).
 *	The recovery / rejoin / startup direct durable check: true iff a quorum-majority
 *	of the CONFIGURED voting disks carry an identical marker whose fence_epoch ==
 *	required_epoch.  Used to gate authority publication (a superseded recovery worker
 *	whose episode is no longer the durable fence epoch is rejected, 8.A) -- NOT to
 *	bound replay (full redo is applied, Oracle-aligned; the non-cooperating zombie's
 *	over-apply is AD-013 hardware-fence scope).  Enforcement off -> verified (true).
 */
bool
cluster_write_fence_verify_durable(uint64 required_epoch)
{
	ClusterFenceAuthority authority;

	if (!cluster_write_fence_enforcing())
		return true; /* not enforced (off / dev / single-node): PG-native recovery */

	/* Enforcement ON but no durable majority marker for required_epoch -> fail
	 * closed (no authority granted): missing disks / superseded epoch / no marker. */
	if (cluster_write_fence_read_durable_authority(&authority) && authority.has_authority
		&& authority.marker.fence_epoch == required_epoch)
		return true;

	if (cluster_write_fence_shmem != NULL)
		pg_atomic_fetch_add_u64(&cluster_write_fence_shmem->durable_check_blocked, 1);
	return false;
}

/*
 * cluster_write_fence_startup_self_check -- spec-4.12 D6 rejoin/startup gate (Q5=C).
 *	A restarting node reads the durable marker directly; if a quorum-majority marker
 *	lists THIS node in its fenced_dead_bitmap, the node is still fenced.  Sets the
 *	local self_fenced token immediately (so D5 rejects every shared write from the
 *	first instant, closing the window before qvotec's first poll) and returns true.
 *	The caller enters a non-serving, NON-FATAL terminal (no permanent lockout): the
 *	node recovers only via a controlled rejoin / cold-admin procedure (Stage 4 does
 *	not do online rejoin).  Enforcement off -> not fenced (false).
 */
bool
cluster_write_fence_startup_self_check(void)
{
	ClusterFenceAuthority authority;

	if (!cluster_write_fence_enforcing())
		return false; /* not enforced (off / dev / single-node) -> not fenced */

	if (!cluster_write_fence_read_durable_authority(&authority) || !authority.has_authority)
		return false; /* no durable majority marker -> not durably fenced */

	if (!cluster_fence_marker_node_is_fenced(authority.marker.fenced_dead_bitmap, cluster_node_id))
		return false; /* this node is not in the fenced set */

	/* Self is durably fenced: arm the local token so D5 fails closed at once. */
	if (cluster_write_fence_shmem != NULL) {
		pg_atomic_write_u32(&cluster_write_fence_shmem->self_fenced, 1);
		pg_atomic_write_u64(&cluster_write_fence_shmem->authorized_epoch,
							authority.marker.fence_epoch);
		pg_atomic_write_u64(&cluster_write_fence_shmem->fence_event_id,
							authority.marker.fence_event_id);
		/*
		 * Also publish the durable dead set so the FIRST refresh_from_marker runs
		 * its dead-set-superset guard against the real latched set, not an empty one
		 * (defense-in-depth in the sensitive rejoin window).  NOTE: this makes the
		 * startup process a writer of the token in addition to qvotec; the refresh
		 * guard's no-lock read is still safe because writes are monotone (epoch +
		 * superset) and this only ever GROWS the latched dead set.
		 */
		memcpy(cluster_write_fence_shmem->fenced_dead_bitmap, authority.marker.fenced_dead_bitmap,
			   Min(sizeof(cluster_write_fence_shmem->fenced_dead_bitmap),
				   (size_t)CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES));
		/*
		 * spec-4.12b D3: a durably self-fenced node is authoritative state -- engage
		 * the bring-up latch so the hot gate never grants this node the bring-up
		 * grace (it must fail closed from the first instant, not be let through the
		 * boot window).  P1: this only TIGHTENS the hot gate; this direct durable
		 * check itself never consults the latch.
		 */
		pg_atomic_write_u32(&cluster_write_fence_shmem->fence_engaged, 1);
	}
	return true;
}

/*
 * cluster_write_fence_supersede_by_admit -- spec-5.16 (3-node rejoin fence
 * clear).  RC-5 supersede applied to the write-fence: when THIS node observes
 * its OWN durable quorum-majority COMMITTED join marker with an admitted_epoch
 * strictly newer than the fence it is under, the cluster has durably re-admitted
 * this node AFTER the fail-stop fence — so the fence is superseded and self
 * un-fences.  This is NOT manual self-unfencing (forbidden, split-brain): the
 * COMMITTED join marker is authored by the coordinator on a quorum-majority of
 * voting disks, and the epoch monotonicity (admit_epoch > authorized_epoch)
 * guarantees a later genuine fail-stop (epoch > admit) re-fences via the qvotec
 * refresh's advance guard.  Called from the qvotec self-admission detection.
 *
 *	Without this, a node restarting into an online rejoin self-fences at startup
 *	(the fail-stop marker still lists it dead before the rejoin commits) and the
 *	qvotec refresh never clears it, because the leader's no-fence baseline either
 *	races the rejoin window or the survivor's stale dead view keeps re-asserting
 *	the fence — a 3-node convergence deadlock (2-node has no second survivor to
 *	hold the stale view).
 */
void
cluster_write_fence_supersede_by_admit(uint64 admitted_epoch)
{
	if (cluster_write_fence_shmem == NULL)
		return;
	if (pg_atomic_read_u32(&cluster_write_fence_shmem->self_fenced) == 0)
		return; /* not fenced — nothing to supersede */
	if (admitted_epoch <= pg_atomic_read_u64(&cluster_write_fence_shmem->authorized_epoch))
		return; /* the fence is at least as new as this admit — not superseded */

	/*
	 * The cluster durably re-admitted self at a newer epoch than the fence.
	 * Advance the latched authorized_epoch to the admit epoch (so the next qvotec
	 * refresh's monotonic guard does NOT re-fence from the stale lower-epoch
	 * marker) and clear the self-fence + engage latch.
	 */
	pg_atomic_write_u64(&cluster_write_fence_shmem->authorized_epoch, admitted_epoch);
	pg_atomic_write_u32(&cluster_write_fence_shmem->self_fenced, 0);
	pg_atomic_write_u32(&cluster_write_fence_shmem->fence_engaged, 0);
	ereport(LOG,
			(errmsg("cluster write-fence: self un-fenced — durable COMMITTED join marker "
					"(admitted epoch " UINT64_FORMAT ") supersedes the fail-stop fence; this node "
					"may serve again",
					admitted_epoch)));
}

/*
 * cluster_write_fence_reject_if_fenced -- spec-4.12 D5 hot-path gate.  The ONE
 *	helper that every cooperative shared-storage write entry calls so they all
 *	mirror the identical CritSectionCount-aware fail-closed action (L240 -- a single
 *	helper cannot diverge entry-to-entry).  Allowed -> return.  Fenced -> bump the
 *	counter, then PANIC inside a critical section (a half-done critical write cannot
 *	be rolled back) or ERROR 53R51 otherwise (catchable; the recovery PG_TRY harness
 *	downgrades it to BLOCKED).
 */
void
cluster_write_fence_reject_if_fenced(const char *op)
{
	if (cluster_write_fence_allowed())
		return; /* enforcement off, or the token proves this node's authority */

	if (cluster_write_fence_shmem != NULL)
		pg_atomic_fetch_add_u64(&cluster_write_fence_shmem->hot_gate_blocked, 1);

	if (CritSectionCount > 0)
		ereport(PANIC,
				(errcode(ERRCODE_CLUSTER_WRITE_FENCED),
				 errmsg("cluster shared-storage %s rejected by the write fence inside a "
						"critical section",
						op),
				 errdetail("This node is stale / superseded / lease-expired / self-fenced; a "
						   "critical-section write cannot be rolled back, so the node PANICs "
						   "to fail closed.")));

	ereport(ERROR,
			(errcode(ERRCODE_CLUSTER_WRITE_FENCED),
			 errmsg("cluster shared-storage %s rejected: this node is write-fenced", op),
			 errhint("The node's cluster epoch is stale, its write-fence lease expired, or a "
					 "membership reconfiguration declared this node dead (self-fenced).")));
}

/*
 * cluster_write_fence_refresh_from_marker -- qvotec poll refresh (spec-4.12 D2).
 *	Publishes the authoritative fence tuple into the local token.  Lease-published-
 *	last protocol (see the read order above): invalidate lease -> write barrier ->
 *	epoch + self_fenced + event_id + bitmap -> write barrier -> publish lease.
 *
 *	spec-4.12b D5 (P0-2): a double-monotonic guard rejects an authority that would
 *	roll membership backward (epoch decrease OR dead-set shrink) BEFORE publishing,
 *	so a stale baseline that somehow reached quorum-majority can never re-release an
 *	already-fenced node (8.A).  A rejected refresh leaves the token untouched -> its
 *	lease ages out (fail-closed).
 */
void
cluster_write_fence_refresh_from_marker(const ClusterFenceMarker *m, uint64 lease_expire_us)
{
	bool self_fenced;
	uint64 latched_epoch;
	uint8 latched_dead[CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES];

	if (cluster_write_fence_shmem == NULL)
		return; /* region not attached: nothing to refresh (hot gate fails closed) */

	/*
	 * D5 double-monotonic guard.  qvotec is the sole writer of this token, so reading
	 * the currently-latched authority needs no lock.  The first authority (latched
	 * epoch 0, empty dead set) always advances; a same/higher epoch with a shrunk
	 * dead set is rejected (would release a fenced node).
	 */
	latched_epoch = pg_atomic_read_u64(&cluster_write_fence_shmem->authorized_epoch);
	memcpy(latched_dead, cluster_write_fence_shmem->fenced_dead_bitmap, sizeof(latched_dead));
	if (!cluster_write_fence_authority_advances(m->fence_epoch, m->fenced_dead_bitmap,
												latched_epoch, latched_dead)) {
		cluster_write_fence_note_baseline_stale();
		return; /* stale / shrunk authority -> do not publish; token ages out (8.A) */
	}

	self_fenced = cluster_fence_marker_node_is_fenced(m->fenced_dead_bitmap, cluster_node_id);

	/*
	 * spec-4.12b Hardening v1.0.2 (self-fence grace race, P0 8.A).  If THIS authority
	 * fences our own node, engage the one-way bring-up latch BEFORE mutating the token
	 * (a single atomic store + barrier).  Otherwise the hot gate keeps granting the
	 * bring-up grace (cluster_write_fence_grace_before_engage looks only at
	 * fence_engaged) throughout the publish sequence below -- a window qvotec can be
	 * descheduled inside (ms-scale), during which a concurrent backend write on this
	 * already-fenced node would be wrongly ALLOWED (false-permit).  Engaging first
	 * makes the gate fail closed for the entire publish.  The write barrier orders the
	 * engage store ahead of the token mutations; paired with the read side reading
	 * fence_engaged AFTER self_fenced (reverse order) in cluster_write_fence_allowed(),
	 * a reader that observes the freshly-published self_fenced=1 is guaranteed to also
	 * observe fence_engaged=1 (message-passing).  The barrier is necessary but relies
	 * on that read-side ordering -- it is not sufficient on its own.
	 *
	 * ASYMMETRIC on purpose: a NON-self-fencing (baseline / normal) authority keeps
	 * engaging LAST (after the lease is published, below).  Engaging a healthy node
	 * early would leave fence_engaged=1 while the lease is still 0, briefly failing
	 * its legitimate writes closed during bring-up.  Only the self-fence case pays the
	 * early fail-closed cost, which is correct -- that node IS fenced.
	 *
	 * Residual (honest): this closes the descheduling (macro) window.  A bounded
	 * lock-free propagation delay of this single fence_engaged store remains, in the
	 * same accepted-risk class as the R4 lease window (cooperative fence; the hard
	 * guarantee is the AD-013 external fence, not this cooperative gate).
	 */
	if (self_fenced) {
		pg_atomic_write_u32(&cluster_write_fence_shmem->fence_engaged, 1);
		pg_write_barrier();
	}

	/* 1. invalidate the lease so any concurrent reader fails closed mid-update. */
	pg_atomic_write_u64(&cluster_write_fence_shmem->lease_expire_at_us, 0);
	pg_write_barrier();

	/* 2. publish the authoritative epoch / self_fenced / identity. */
	pg_atomic_write_u64(&cluster_write_fence_shmem->authorized_epoch, m->fence_epoch);
	pg_atomic_write_u32(&cluster_write_fence_shmem->self_fenced, self_fenced ? 1 : 0);
	pg_atomic_write_u64(&cluster_write_fence_shmem->fence_event_id, m->fence_event_id);
	memcpy(cluster_write_fence_shmem->fenced_dead_bitmap, m->fenced_dead_bitmap,
		   Min(sizeof(cluster_write_fence_shmem->fenced_dead_bitmap),
			   (size_t)CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES));
	pg_write_barrier();

	/*
	 * spec-4.12b Hardening v1.0.2 invariant: a self-fencing authority MUST have
	 * already engaged the bring-up latch (engage-first, above) before the lease is
	 * published, so the hot gate cannot grant grace to a self-fenced node during or
	 * after publish.  This Assert pins the engage-first ordering against a future
	 * reorder regression -- it is an invariant check, NOT a runtime guard (the real
	 * protection is the engage-first store + the !self_fenced grace guard).  Layer A
	 * has no deterministic e2e (would need test-only catalog surface + a catversion
	 * bump); see the Hardening v1.0.2 appendix for the construction-proof rationale.
	 */
	Assert(!self_fenced || pg_atomic_read_u32(&cluster_write_fence_shmem->fence_engaged) != 0);

	/* 3. publish the lease LAST -- this is what makes the token usable. */
	pg_atomic_write_u64(&cluster_write_fence_shmem->lease_expire_at_us, lease_expire_us);

	/* spec-4.12b D6: record the refresh time so observability can derive the
	 * authority age (now - last refresh). */
	pg_atomic_write_u64(&cluster_write_fence_shmem->last_authority_refresh_us,
						(uint64)GetCurrentTimestamp());

	/*
	 * spec-4.12b D3: the first authority just latched.  Engage the one-way
	 * bring-up latch AFTER the lease is published, so the hot gate ends its
	 * bring-up grace only once a usable token exists.  Set unconditionally (it is
	 * one-way -- a no-op once set).  spec-4.12b Hardening v1.0.2: for a self-fencing
	 * authority this is already engaged above (engage-first), so this is the no-op
	 * path; only the NON-self-fence (healthy) authority first engages here.
	 */
	pg_atomic_write_u32(&cluster_write_fence_shmem->fence_engaged, 1);
}

/*
 * cluster_write_fence_note_minority_marker -- a CRC-ok marker existed but did not
 *	reach quorum-majority (P0a); ignored.  Bump the counter; leave the token alone
 *	so its lease ages out (fail-closed) if no authority is found.
 */
void
cluster_write_fence_note_minority_marker(void)
{
	if (cluster_write_fence_shmem == NULL)
		return;
	pg_atomic_fetch_add_u64(&cluster_write_fence_shmem->minority_marker_ignored, 1);
}

/*
 * cluster_write_fence_note_baseline_stale -- spec-4.12b D5.  A baseline that would
 *	roll membership backward was rejected.  Two callers, same 8.A semantics: the
 *	refresh guard (an authority whose epoch decreased or dead set shrank) and the
 *	qvotec baseline author (P1-1: a would-be baseline whose epoch is below the
 *	durable authority just observed -- never overwrite a higher-epoch fence).
 */
void
cluster_write_fence_note_baseline_stale(void)
{
	if (cluster_write_fence_shmem == NULL)
		return;
	pg_atomic_fetch_add_u64(&cluster_write_fence_shmem->baseline_stale_rejected, 1);
}

/* spec-4.12 D7 observability counter accessors (L110-safe: 0 with no region). */
uint64
cluster_write_fence_get_hot_gate_blocked(void)
{
	return cluster_write_fence_shmem == NULL
			   ? 0
			   : pg_atomic_read_u64(&cluster_write_fence_shmem->hot_gate_blocked);
}

uint64
cluster_write_fence_get_durable_check_blocked(void)
{
	return cluster_write_fence_shmem == NULL
			   ? 0
			   : pg_atomic_read_u64(&cluster_write_fence_shmem->durable_check_blocked);
}

uint64
cluster_write_fence_get_minority_marker_ignored(void)
{
	return cluster_write_fence_shmem == NULL
			   ? 0
			   : pg_atomic_read_u64(&cluster_write_fence_shmem->minority_marker_ignored);
}

uint64
cluster_write_fence_get_marker_write_failed(void)
{
	return cluster_write_fence_shmem == NULL
			   ? 0
			   : pg_atomic_read_u64(&cluster_write_fence_shmem->marker_write_failed);
}

uint64
cluster_write_fence_get_baseline_stale_rejected(void)
{
	return cluster_write_fence_shmem == NULL
			   ? 0
			   : pg_atomic_read_u64(&cluster_write_fence_shmem->baseline_stale_rejected);
}

/* spec-4.12b D3: has the bring-up latch engaged (first authority / self-fence seen)? */
bool
cluster_write_fence_get_fence_engaged(void)
{
	return cluster_write_fence_shmem != NULL
		   && pg_atomic_read_u32(&cluster_write_fence_shmem->fence_engaged) != 0;
}

/* spec-4.12b D6: count of poll cycles in which this node authored a baseline. */
uint64
cluster_write_fence_get_baseline_published(void)
{
	return cluster_write_fence_shmem == NULL
			   ? 0
			   : pg_atomic_read_u64(&cluster_write_fence_shmem->baseline_published);
}

/* spec-4.12b D6: is this node currently the lowest-live baseline-author leader? */
bool
cluster_write_fence_get_baseline_author_is_self(void)
{
	return cluster_write_fence_shmem != NULL
		   && pg_atomic_read_u32(&cluster_write_fence_shmem->baseline_author_is_self) != 0;
}

/*
 * spec-4.12b D6: age (microseconds) of the last successful token refresh, i.e. how
 * long since this node last saw a quorum-majority authority.  0 when no refresh has
 * happened yet OR when the clock appears to run backwards (defensive; never negative).
 */
uint64
cluster_write_fence_get_baseline_authority_age_us(void)
{
	uint64 last;
	uint64 now;

	if (cluster_write_fence_shmem == NULL)
		return 0;
	last = pg_atomic_read_u64(&cluster_write_fence_shmem->last_authority_refresh_us);
	if (last == 0)
		return 0; /* never refreshed */
	now = (uint64)GetCurrentTimestamp();
	return now > last ? now - last : 0;
}

/*
 * cluster_write_fence_note_baseline_published -- spec-4.12b D6.  qvotec calls this
 *	once per poll: is_leader records the current baseline-author leadership state
 *	(observability); published is true on the cycles this node actually wrote a
 *	baseline marker (it was leader, enforcing, and had no fresh fence submit), which
 *	bumps the publish counter.  No-op when the region is not attached.
 */
void
cluster_write_fence_note_baseline_published(bool is_leader, bool published)
{
	if (cluster_write_fence_shmem == NULL)
		return;
	pg_atomic_write_u32(&cluster_write_fence_shmem->baseline_author_is_self, is_leader ? 1 : 0);
	if (published)
		pg_atomic_fetch_add_u64(&cluster_write_fence_shmem->baseline_published, 1);
}

/*
 * cluster_write_fence_submit_marker -- LMON (coordinator) side of the D4 handshake.
 *	Stages the marker, wakes qvotec, and synchronously waits (bounded by the lease)
 *	for qvotec to write + fdatasync it to >= quorum-majority disks.  Returns ACK only
 *	when durable on a majority; the caller publishes the reconfig event ONLY on ACK
 *	(core 8.A order: marker durable BEFORE recovery).
 */
ClusterFenceMarkerSubmitResult
cluster_write_fence_submit_marker(const ClusterFenceMarker *m)
{
	uint64 seq;
	Latch *qlatch;

	if (cluster_write_fence_shmem == NULL)
		return CLUSTER_FENCE_MARKER_SUBMIT_FAILED; /* region not attached (pre-D7) */

	/* stage the marker, then publish the request (barrier between so qvotec never
	 * reads a half-written marker). */
	memcpy(&cluster_write_fence_shmem->pending_marker, m, sizeof(*m));
	pg_write_barrier();
	seq = pg_atomic_add_fetch_u64(&cluster_write_fence_shmem->marker_request_seq, 1);

	/* latch-wake; a NULL latch (qvotec not running) -> we time out below = fail-closed. */
	qlatch = cluster_write_fence_shmem->qvotec_latch;
	if (qlatch != NULL)
		SetLatch(qlatch);

	/* bounded synchronous wait for qvotec to complete THIS exact request. */
	{
		ClusterFenceMarkerSubmitResult result;
		uint64 deadline_us
			= (uint64)GetCurrentTimestamp() + (uint64)cluster_write_fence_lease_ms * 1000ULL;

		pgstat_report_wait_start(WAIT_EVENT_CLUSTER_WRITE_FENCE_MARKER_WRITE);
		for (;;) {
			if (pg_atomic_read_u64(&cluster_write_fence_shmem->marker_completion_seq) == seq) {
				pg_read_barrier();
				result = (ClusterFenceMarkerSubmitResult)pg_atomic_read_u32(
					&cluster_write_fence_shmem->marker_result);
				break;
			}
			if ((uint64)GetCurrentTimestamp() >= deadline_us) {
				result = CLUSTER_FENCE_MARKER_SUBMIT_TIMEOUT;
				break;
			}
			pg_usleep(2 * 1000); /* 2 ms */
		}
		pgstat_report_wait_end();
		return result;
	}
}

bool
cluster_write_fence_submit_marker_async(ClusterMarkerAsync *a, const ClusterFenceMarker *m,
										ClusterMarkerAsyncKind kind, int32 target_node,
										TimestampTz now)
{
	if (cluster_write_fence_shmem == NULL || m == NULL || a == NULL)
		return false;
	if (cluster_marker_async_is_submitted(a))
		return true;
	if (cluster_marker_async_mailbox_busy(&cluster_write_fence_shmem->marker_request_seq,
										  &cluster_write_fence_shmem->marker_completion_seq))
		return false;

	memcpy(&cluster_write_fence_shmem->pending_marker, m, sizeof(*m));
	return cluster_marker_async_submit(
		a, &cluster_write_fence_shmem->marker_request_seq,
		&cluster_write_fence_shmem->marker_completion_seq, cluster_write_fence_shmem->qvotec_latch,
		now, (uint64)cluster_write_fence_lease_ms * 1000ULL, kind, target_node);
}

ClusterMarkerPollResult
cluster_write_fence_poll_marker_async(ClusterMarkerAsync *a, TimestampTz now, uint32 *out_result,
									  uint64 *out_elapsed_us)
{
	if (cluster_write_fence_shmem == NULL || a == NULL)
		return CLUSTER_MARKER_POLL_IDLE;
	return cluster_marker_async_poll(a, &cluster_write_fence_shmem->marker_completion_seq,
									 &cluster_write_fence_shmem->marker_result, now, out_result,
									 out_elapsed_us);
}

/*
 * cluster_write_fence_clear_qvotec_latch / _publish_qvotec_latch -- qvotec publishes
 *	its MyLatch at startup so LMON can wake it; auto-cleared at proc_exit so a stale
 *	pointer is never signalled (a NULL latch just makes LMON time out = fail-closed).
 */
static void
cluster_write_fence_clear_qvotec_latch(int code, Datum arg)
{
	if (cluster_write_fence_shmem != NULL)
		cluster_write_fence_shmem->qvotec_latch = NULL;
}

void
cluster_write_fence_publish_qvotec_latch(struct Latch *latch)
{
	if (cluster_write_fence_shmem == NULL)
		return;
	cluster_write_fence_shmem->qvotec_latch = latch;
	on_shmem_exit(cluster_write_fence_clear_qvotec_latch, (Datum)0);
}

/*
 * cluster_write_fence_qvotec_poll_pending -- qvotec poll: a new submit pending?
 *	Returns true + the staged marker (and marks it in-flight) when request_seq has
 *	advanced past the last fully-processed request.
 */
bool
cluster_write_fence_qvotec_poll_pending(ClusterFenceMarker *out)
{
	uint64 req;

	if (cluster_write_fence_shmem == NULL)
		return false;

	req = pg_atomic_read_u64(&cluster_write_fence_shmem->marker_request_seq);
	if (req == qvotec_last_processed_marker_seq)
		return false; /* nothing new */

	pg_read_barrier();
	memcpy(out, &cluster_write_fence_shmem->pending_marker, sizeof(*out));
	qvotec_inflight_marker_seq = req;
	return true;
}

/*
 * cluster_write_fence_qvotec_complete -- qvotec poll: publish the in-flight result
 *	(acked = the marker reached >= quorum-majority durable disks) back to the waiting
 *	LMON.  last_processed advances only here, so a crash before this point makes the
 *	next poll reprocess the request (idempotent re-write).
 */
void
cluster_write_fence_qvotec_complete(bool acked)
{
	if (cluster_write_fence_shmem == NULL)
		return;

	if (!acked)
		pg_atomic_fetch_add_u64(&cluster_write_fence_shmem->marker_write_failed, 1);

	pg_atomic_write_u32(&cluster_write_fence_shmem->marker_result,
						acked ? CLUSTER_FENCE_MARKER_SUBMIT_ACK
							  : CLUSTER_FENCE_MARKER_SUBMIT_FAILED);
	pg_write_barrier();
	pg_atomic_write_u64(&cluster_write_fence_shmem->marker_completion_seq,
						qvotec_inflight_marker_seq);
	qvotec_last_processed_marker_seq = qvotec_inflight_marker_seq;
	cluster_lmon_marker_complete_wakeup();
}

#endif /* USE_PGRAC_CLUSTER */
