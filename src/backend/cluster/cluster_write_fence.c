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
		pg_atomic_init_u64(&cluster_write_fence_shmem->hot_gate_blocked, 0);
		pg_atomic_init_u64(&cluster_write_fence_shmem->durable_check_blocked, 0);
		pg_atomic_init_u64(&cluster_write_fence_shmem->minority_marker_ignored, 0);

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
};

void
cluster_write_fence_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_write_fence_region);
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
	bool enforcement_on = (cluster_write_fence_enforcement == CLUSTER_WRITE_FENCE_ENFORCE_ON);
	bool region_attached = (cluster_write_fence_shmem != NULL);
	uint64 epoch_current = cluster_epoch_get_current();
	uint64 authorized_epoch = 0;
	uint64 lease_expire_us = 0;
	bool self_fenced = false;
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
	}

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

	if (cluster_write_fence_enforcement != CLUSTER_WRITE_FENCE_ENFORCE_ON)
		return true; /* not enforced: recovery proceeds as PG-native */

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

	if (cluster_write_fence_enforcement != CLUSTER_WRITE_FENCE_ENFORCE_ON)
		return false; /* not enforced */

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
	}
	return true;
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
 */
void
cluster_write_fence_refresh_from_marker(const ClusterFenceMarker *m, uint64 lease_expire_us)
{
	bool self_fenced;

	if (cluster_write_fence_shmem == NULL)
		return; /* region not attached: nothing to refresh (hot gate fails closed) */

	self_fenced = cluster_fence_marker_node_is_fenced(m->fenced_dead_bitmap, cluster_node_id);

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

	/* 3. publish the lease LAST -- this is what makes the token usable. */
	pg_atomic_write_u64(&cluster_write_fence_shmem->lease_expire_at_us, lease_expire_us);
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
}

#endif /* USE_PGRAC_CLUSTER */
