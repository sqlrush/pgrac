/*-------------------------------------------------------------------------
 *
 * cluster_xid_stripe_boot.c
 *	  pgrac xid stripe activation / boot face (spec-6.15 D5b).
 *
 *	  See cluster_xid_stripe_boot.h for the plane split: qvotec (sole
 *	  voting-disk writer) scans region 5 and services the seed mailbox;
 *	  the reconfig joiner gate consults the published verdict before
 *	  admitting this node; every process latches the stripe runtime
 *	  lazily from the publication.
 *
 *	  Single-writer story for the activation record: the seed request
 *	  is staged only by the deterministic seed candidate (lowest fresh-
 *	  alive declared node, computed by the reconfig tick that owns the
 *	  alive view), written only by this node's qvotec (which re-scans
 *	  first and adopts any record that appeared meanwhile), and the
 *	  content is derived from the shared pg_control nextXid authority
 *	  (spec-5.6), so even the pathological two-seeders race writes
 *	  byte-identical records apart from the herding-slack GUC -- and a
 *	  divergent-floor loser is still fail-closed (below-floor xids are
 *	  underivable, never guessed).  There is deliberately NO clear /
 *	  rewrite API: activation is single-shot and irreversible (§3.6).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_xid_stripe_boot.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-6.15-xid-space-segmentation.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include <sys/stat.h>
#include <unistd.h>

#include "access/transam.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_voting_disk_io.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/cluster_xid_stripe_boot.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/elog.h"

/* Cross-check the pure-layer literal against the real slot size. */
StaticAssertDecl(sizeof(ClusterXidStripeSlotRecord) <= CLUSTER_VOTING_SLOT_BYTES,
				 "stripe slot record must fit a voting slot");
StaticAssertDecl(sizeof(ClusterXidStripeActivationRecord) <= CLUSTER_VOTING_SLOT_BYTES,
				 "stripe activation record must fit a voting slot");

/*
 * Shared state.  All fields are guarded by lock except the seed
 * mailbox seqs (single producer LMON / single consumer qvotec,
 * mirroring the spec-5.15 join-marker mailbox).
 */
typedef struct ClusterXidStripeBootShmem {
	LWLock lock;

	/* region-5 publication */
	uint32 disk_state; /* ClusterXidStripeDiskState */
	uint64 floor_full;
	uint64 epoch;
	uint64 generation;

	/* this node's region-4 claim publication (D5c writes; D5e reads) */
	bool my_slot_claimed;
	uint64 my_slot_floor_full;
	uint64 my_owner_incarnation;

	/* seed mailbox: LMON stages -> qvotec writes -> majority ACK */
	pg_atomic_uint64 seed_request_seq;
	pg_atomic_uint64 seed_completion_seq;
	uint64 seed_floor_full; /* staged payload (valid while request > completion) */
	uint64 seed_epoch;
} ClusterXidStripeBootShmem;

static ClusterXidStripeBootShmem *StripeBootShmem = NULL;

/* process-local: lazy latch ran (one-way, per process) */
static bool stripe_latch_done = false;

Size
cluster_xid_stripe_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterXidStripeBootShmem));
}

void
cluster_xid_stripe_shmem_init(void)
{
	bool found;

	StripeBootShmem = (ClusterXidStripeBootShmem *)ShmemInitStruct(
		"pgrac cluster xid stripe", cluster_xid_stripe_shmem_size(), &found);

	if (!found) {
		memset(StripeBootShmem, 0, sizeof(ClusterXidStripeBootShmem));
		LWLockInitialize(&StripeBootShmem->lock, LWTRANCHE_CLUSTER_XID_STRIPE);
		StripeBootShmem->disk_state = CLUSTER_XID_STRIPE_DISK_UNKNOWN;
		pg_atomic_init_u64(&StripeBootShmem->seed_request_seq, 0);
		pg_atomic_init_u64(&StripeBootShmem->seed_completion_seq, 0);
	}
}

/* ============================================================
 * Region-5 scan (qvotec).
 * ============================================================ */

/*
 * Classify one disk's region-5 read.  ABSENT covers both the all-zeros
 * slot and the not-yet-grown file (EOF short read distinguished from
 * an I/O error by fstat); CORRUPT is a non-empty slot that fails the
 * record validation and is NEVER seeded over.
 */
typedef enum StripeSlotReadClass {
	STRIPE_READ_VALID,
	STRIPE_READ_ABSENT,
	STRIPE_READ_CORRUPT,
	STRIPE_READ_UNREADABLE
} StripeSlotReadClass;

static bool
buffer_is_all_zeros(const char *buf, Size len)
{
	Size i;

	for (i = 0; i < len; i++)
		if (buf[i] != 0)
			return false;
	return true;
}

static StripeSlotReadClass
stripe_read_activation_one(int fd, ClusterXidStripeActivationRecord *out)
{
	char slot[CLUSTER_VOTING_SLOT_BYTES];
	ClusterVotingDiskIoState rc;

	rc = cluster_voting_disk_read_stripe_activation(fd, slot);
	if (rc != CLUSTER_VOTING_DISK_IO_OK) {
		struct stat st;

		/* EOF on a file that has not grown past region 4 yet is the
		 * lazily-materialised empty state, not an I/O failure. */
		if (fstat(fd, &st) == 0 && (off_t)st.st_size < CLUSTER_VOTING_FILE_BYTES_MIN)
			return STRIPE_READ_ABSENT;
		return STRIPE_READ_UNREADABLE;
	}

	if (buffer_is_all_zeros(slot, sizeof(slot)))
		return STRIPE_READ_ABSENT;

	memcpy(out, slot, sizeof(*out));
	if (!cluster_xid_stripe_activation_record_valid(out))
		return STRIPE_READ_CORRUPT;
	return STRIPE_READ_VALID;
}

/*
 * Scan region 5 across the opened disks and publish the verdict.
 * Adoption rule: among valid records the newest generation wins (the
 * JCMK "read newest" torn-write guard); epoch regression versus an
 * already-published record is rejected (monotonic envelope).
 */
void
cluster_xid_stripe_scan_disks(const int *fds, int n_disks)
{
	ClusterXidStripeActivationRecord best;
	bool have_valid = false;
	bool have_corrupt = false;
	bool have_readable = false;
	int i;

	if (StripeBootShmem == NULL || fds == NULL || n_disks <= 0)
		return;

	memset(&best, 0, sizeof(best));

	for (i = 0; i < n_disks; i++) {
		ClusterXidStripeActivationRecord rec;

		switch (stripe_read_activation_one(fds[i], &rec)) {
		case STRIPE_READ_VALID:
			have_readable = true;
			if (!have_valid || rec.generation > best.generation) {
				best = rec;
				have_valid = true;
			}
			break;
		case STRIPE_READ_ABSENT:
			have_readable = true;
			break;
		case STRIPE_READ_CORRUPT:
			have_readable = true;
			have_corrupt = true;
			break;
		case STRIPE_READ_UNREADABLE:
			break;
		}
	}

	LWLockAcquire(&StripeBootShmem->lock, LW_EXCLUSIVE);
	if (have_valid) {
		if (StripeBootShmem->disk_state == CLUSTER_XID_STRIPE_DISK_PUBLISHED
			&& best.stride_mode_epoch < StripeBootShmem->epoch) {
			/* Monotonic-epoch violation: a stale copy resurfaced.  Keep
			 * the already-published state; never regress. */
			ereport(LOG, (errmsg("cluster xid stripe: ignoring stale activation record "
								 "(epoch " UINT64_FORMAT " < published " UINT64_FORMAT ")",
								 best.stride_mode_epoch, StripeBootShmem->epoch)));
		} else {
			StripeBootShmem->disk_state = CLUSTER_XID_STRIPE_DISK_PUBLISHED;
			StripeBootShmem->floor_full = best.activated_floor_full;
			StripeBootShmem->epoch = best.stride_mode_epoch;
			StripeBootShmem->generation = best.generation;
		}
	} else if (have_corrupt) {
		/* Garbage and no valid copy anywhere: fail-closed hold.  The
		 * real activation record may be under the damage — refuse to
		 * treat as absent for seeding, refuse to admit (53RB1). */
		if (StripeBootShmem->disk_state != CLUSTER_XID_STRIPE_DISK_PUBLISHED)
			StripeBootShmem->disk_state = CLUSTER_XID_STRIPE_DISK_CORRUPT;
	} else if (have_readable) {
		if (StripeBootShmem->disk_state == CLUSTER_XID_STRIPE_DISK_UNKNOWN)
			StripeBootShmem->disk_state = CLUSTER_XID_STRIPE_DISK_ABSENT;
	}
	LWLockRelease(&StripeBootShmem->lock);
}

/* ============================================================
 * Seed mailbox (LMON stages, qvotec writes).
 * ============================================================ */

/*
 * Stage the activation seed (single-shot).  Called by the joiner gate
 * when striping is on, the record is durably ABSENT and this node is
 * the seed candidate.  Floor: shared-authority nextXid rounded up to
 * the stride boundary plus one herding slack (spec-6.15 §2.3).
 */
static void
stripe_stage_seed_request(void)
{
	uint64 req = pg_atomic_read_u64(&StripeBootShmem->seed_request_seq);
	uint64 done = pg_atomic_read_u64(&StripeBootShmem->seed_completion_seq);
	FullTransactionId next_full;
	uint64 floor;

	if (req != done)
		return; /* one in flight already */

	next_full = ReadNextFullTransactionId();
	floor = U64FromFullTransactionId(next_full);
	floor = floor - (floor % CLUSTER_XID_STRIDE) + CLUSTER_XID_STRIDE; /* round up */
	floor += (uint64)cluster_xid_herding_slack;

	LWLockAcquire(&StripeBootShmem->lock, LW_EXCLUSIVE);
	StripeBootShmem->seed_floor_full = floor;
	StripeBootShmem->seed_epoch = 1;
	LWLockRelease(&StripeBootShmem->lock);

	pg_write_barrier();
	pg_atomic_write_u64(&StripeBootShmem->seed_request_seq, req + 1);

	ereport(LOG, (errmsg("cluster xid stripe: staging activation seed (floor " UINT64_FORMAT
						 ", epoch 1) — this node is the seed candidate",
						 floor)));
}

/*
 * qvotec: service a pending seed request.  Re-scan first (a record
 * that appeared meanwhile is adopted, never overwritten); write to
 * every disk and require a majority before publishing.
 */
void
cluster_xid_stripe_service_seed(const int *fds, int n_disks)
{
	uint64 req, done;
	ClusterXidStripeActivationRecord rec;
	char slot[CLUSTER_VOTING_SLOT_BYTES];
	int disks_ok = 0;
	int majority;
	int i;

	if (StripeBootShmem == NULL || fds == NULL || n_disks <= 0)
		return;

	req = pg_atomic_read_u64(&StripeBootShmem->seed_request_seq);
	done = pg_atomic_read_u64(&StripeBootShmem->seed_completion_seq);
	if (req == done)
		return;
	pg_read_barrier();

	/* Adopt-not-overwrite: a concurrent seeder may have landed one. */
	cluster_xid_stripe_scan_disks(fds, n_disks);
	if (cluster_xid_stripe_disk_state() != CLUSTER_XID_STRIPE_DISK_ABSENT) {
		pg_atomic_write_u64(&StripeBootShmem->seed_completion_seq, req);
		return;
	}

	memset(&rec, 0, sizeof(rec));
	rec.magic = CLUSTER_PGXA_MAGIC;
	rec.version = CLUSTER_PGXA_VERSION;
	LWLockAcquire(&StripeBootShmem->lock, LW_SHARED);
	rec.activated_floor_full = StripeBootShmem->seed_floor_full;
	rec.stride_mode_epoch = StripeBootShmem->seed_epoch;
	LWLockRelease(&StripeBootShmem->lock);
	rec.generation = 1;
	cluster_xid_stripe_activation_record_compute_crc(&rec);

	memset(slot, 0, sizeof(slot));
	memcpy(slot, &rec, sizeof(rec));

	for (i = 0; i < n_disks; i++) {
		if (cluster_voting_disk_write_stripe_activation(fds[i], slot) == CLUSTER_VOTING_DISK_IO_OK)
			disks_ok++;
	}

	majority = (n_disks / 2) + 1;
	if (disks_ok >= majority) {
		LWLockAcquire(&StripeBootShmem->lock, LW_EXCLUSIVE);
		StripeBootShmem->disk_state = CLUSTER_XID_STRIPE_DISK_PUBLISHED;
		StripeBootShmem->floor_full = rec.activated_floor_full;
		StripeBootShmem->epoch = rec.stride_mode_epoch;
		StripeBootShmem->generation = rec.generation;
		LWLockRelease(&StripeBootShmem->lock);
		ereport(LOG, (errmsg("cluster xid stripe: activation record durable on %d/%d disks "
							 "(floor " UINT64_FORMAT ", epoch " UINT64_FORMAT ")",
							 disks_ok, n_disks, rec.activated_floor_full, rec.stride_mode_epoch)));
	} else {
		ereport(LOG, (errmsg("cluster xid stripe: activation seed reached only %d/%d disks "
							 "(majority %d) — will retry",
							 disks_ok, n_disks, majority)));
	}
	/* ACK either way; the gate re-stages on the next tick if unpublished. */
	pg_atomic_write_u64(&StripeBootShmem->seed_completion_seq, req);
}

/* ============================================================
 * Joiner gate (LMON / reconfig).
 * ============================================================ */

ClusterXidStripeJoinVerdict
cluster_xid_stripe_join_gate(bool self_may_seed)
{
	uint32 state;

	if (StripeBootShmem == NULL || !cluster_enabled)
		return CLUSTER_XID_STRIPE_JOIN_PROCEED;

	LWLockAcquire(&StripeBootShmem->lock, LW_SHARED);
	state = StripeBootShmem->disk_state;
	LWLockRelease(&StripeBootShmem->lock);

	if (cluster_xid_striping) {
		switch ((ClusterXidStripeDiskState)state) {
		case CLUSTER_XID_STRIPE_DISK_PUBLISHED:
			return CLUSTER_XID_STRIPE_JOIN_PROCEED;
		case CLUSTER_XID_STRIPE_DISK_ABSENT:
			if (self_may_seed)
				stripe_stage_seed_request();
			return CLUSTER_XID_STRIPE_JOIN_HOLD;
		case CLUSTER_XID_STRIPE_DISK_CORRUPT:
			return CLUSTER_XID_STRIPE_JOIN_REFUSE;
		case CLUSTER_XID_STRIPE_DISK_UNKNOWN:
			return CLUSTER_XID_STRIPE_JOIN_HOLD;
		}
		return CLUSTER_XID_STRIPE_JOIN_HOLD; /* unreachable; fail closed */
	}

	/* striping off: joining an ACTIVATED cluster is a config mismatch
	 * (an unstriped allocator would violate the striped uniqueness
	 * invariant); a corrupt region likewise refuses until repaired.
	 * UNKNOWN holds — proceeding before the qvotec scan publishes
	 * would let a mixed-mode node race past the record (fail-open);
	 * every online-join configuration has voting disks, so the scan
	 * resolves within the first qvotec polls. */
	switch ((ClusterXidStripeDiskState)state) {
	case CLUSTER_XID_STRIPE_DISK_PUBLISHED:
	case CLUSTER_XID_STRIPE_DISK_CORRUPT:
		return CLUSTER_XID_STRIPE_JOIN_REFUSE;
	case CLUSTER_XID_STRIPE_DISK_ABSENT:
		return CLUSTER_XID_STRIPE_JOIN_PROCEED;
	case CLUSTER_XID_STRIPE_DISK_UNKNOWN:
		return CLUSTER_XID_STRIPE_JOIN_HOLD;
	}
	return CLUSTER_XID_STRIPE_JOIN_HOLD; /* unreachable; fail closed */
}

/* ============================================================
 * Lazy latch + published-state accessors.
 * ============================================================ */

void
cluster_xid_stripe_lazy_latch(void)
{
	uint64 floor = 0;
	bool published = false;

	if (stripe_latch_done || StripeBootShmem == NULL)
		return;

	LWLockAcquire(&StripeBootShmem->lock, LW_SHARED);
	if (StripeBootShmem->disk_state == CLUSTER_XID_STRIPE_DISK_PUBLISHED) {
		published = true;
		floor = StripeBootShmem->floor_full;
	}
	LWLockRelease(&StripeBootShmem->lock);

	if (!published)
		return; /* stays unlatched; wrappers keep failing closed */

	cluster_xid_stripe_latch_runtime(true, cluster_xid_allocation_slot(),
									 FullTransactionIdFromU64(floor));
	stripe_latch_done = true;
}

FullTransactionId
cluster_xid_stripe_my_slot_floor(void)
{
	FullTransactionId result = InvalidFullTransactionId;

	if (StripeBootShmem == NULL)
		return result;

	LWLockAcquire(&StripeBootShmem->lock, LW_SHARED);
	if (StripeBootShmem->my_slot_claimed)
		result = FullTransactionIdFromU64(StripeBootShmem->my_slot_floor_full);
	LWLockRelease(&StripeBootShmem->lock);
	return result;
}

ClusterXidStripeDiskState
cluster_xid_stripe_disk_state(void)
{
	uint32 state;

	if (StripeBootShmem == NULL)
		return CLUSTER_XID_STRIPE_DISK_UNKNOWN;

	LWLockAcquire(&StripeBootShmem->lock, LW_SHARED);
	state = StripeBootShmem->disk_state;
	LWLockRelease(&StripeBootShmem->lock);
	return (ClusterXidStripeDiskState)state;
}

bool
cluster_xid_stripe_get_activation(uint64 *floor_full, uint64 *epoch, uint64 *generation)
{
	bool published = false;

	if (StripeBootShmem == NULL)
		return false;

	LWLockAcquire(&StripeBootShmem->lock, LW_SHARED);
	if (StripeBootShmem->disk_state == CLUSTER_XID_STRIPE_DISK_PUBLISHED) {
		published = true;
		if (floor_full)
			*floor_full = StripeBootShmem->floor_full;
		if (epoch)
			*epoch = StripeBootShmem->epoch;
		if (generation)
			*generation = StripeBootShmem->generation;
	}
	LWLockRelease(&StripeBootShmem->lock);
	return published;
}

#endif /* USE_PGRAC_CLUSTER */
