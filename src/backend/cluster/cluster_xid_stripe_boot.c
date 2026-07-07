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

#include "access/multixact.h" /* ReadNextMultiXactId (mxid floor seed sample, spec-7.1 D3-a) */
#include "access/transam.h"
#include "access/xlog.h" /* RecoveryInProgress (D5d gate hold) */
#include "cluster/cluster_conf.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_inject.h"		 /* herding-stall injection (D3, L408) */
#include "cluster/cluster_mxid_stripe.h" /* mxid stripe face (spec-7.1 D3-a) */
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_voting_disk_io.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/cluster_xid_stripe_boot.h"
#include "cluster/cluster_xid_stripe_xlog.h" /* JOIN record emission (D5d) */
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/timestamp.h" /* GetCurrentTimestamp (retire bounded wait) */

/* Cross-check the pure-layer literal against the real slot size. */
StaticAssertDecl(sizeof(ClusterXidStripeSlotRecord) <= CLUSTER_VOTING_SLOT_BYTES,
				 "stripe slot record must fit a voting slot");
StaticAssertDecl(sizeof(ClusterXidStripeActivationRecord) <= CLUSTER_VOTING_SLOT_BYTES,
				 "stripe activation record must fit a voting slot");
StaticAssertDecl(CLUSTER_PGXM_SLOT_OFFSET + sizeof(ClusterMxidStripeExtensionRecord)
					 <= CLUSTER_VOTING_SLOT_BYTES,
				 "mxid stripe extension must fit the activation voting slot");

/*
 * Stripe mailbox ops (single producer LMON / single consumer qvotec,
 * mirroring the spec-5.15 join-marker mailbox; one op in flight).
 */
typedef enum StripeMailboxOp {
	STRIPE_OP_NONE = 0,
	STRIPE_OP_SEED,	 /* write the region-5 activation record */
	STRIPE_OP_CLAIM, /* first-claim THIS node's region-4 slot */
	STRIPE_OP_RETIRE /* 5.18: mark target's region-4 slot retired */
} StripeMailboxOp;

/*
 * Shared state.  All fields are guarded by lock except the mailbox
 * seqs (write barrier between payload and request seq).
 */
typedef struct ClusterXidStripeBootShmem {
	LWLock lock;

	/* region-5 publication */
	uint32 disk_state; /* ClusterXidStripeDiskState */
	uint64 floor_full;
	uint64 epoch;
	uint64 generation;
	uint32 mxid_floor; /* "PGXM" extension floor; 0 = absent (spec-7.1 D3-a) */

	/* this node's region-4 publication (D5c; D5e reads the floor) */
	uint32 slot_state; /* ClusterXidStripeSlotState */
	bool my_slot_claimed;
	uint64 my_slot_floor_full;
	uint64 my_owner_incarnation;
	uint64 my_hwm_on_disk; /* durable hwm last read/written (herding) */

	/*
	 * D3 counter herding (atomics: read on the GetNewTransactionId hot
	 * path without this module's lock).  herding_floor_full is this
	 * node's durable allocation promise — every xid it issues is at or
	 * above it (published as the region-4 hwm BEFORE taking effect, so
	 * a crash can never issue below an already-published promise);
	 * cluster_min/max_active_hwm summarize the other DECLARED,
	 * non-retired slots' published hwm (0 until the first scan).
	 */
	pg_atomic_uint64 herding_floor_full;
	pg_atomic_uint64 cluster_min_active_hwm;
	pg_atomic_uint64 cluster_max_active_hwm;

	/*
	 * Replay-learned stripe knowledge (D5d): written ONLY by WAL redo
	 * (startup process), consumed by the hot-standby KnownAssignedXids
	 * gap-fill in the same process (atomics are for cross-process
	 * observability, e.g. after promotion).  floor 0 = not activated
	 * (dense fill); bit k of the bitmap = slot k active.
	 */
	pg_atomic_uint64 replay_floor_full;
	pg_atomic_uint64 replay_epoch;
	pg_atomic_uint32 replay_active_bitmap;

	/* stripe mailbox: LMON stages -> qvotec writes -> majority ACK */
	pg_atomic_uint64 req_seq;
	pg_atomic_uint64 done_seq;
	pg_atomic_uint32 op_result; /* 1 = durable ok, 0 = not durable */
	pg_atomic_uint32 done_op;	/* StripeMailboxOp the last ACK was for */
	uint32 op;					/* StripeMailboxOp */
	int32 op_target_node;		/* RETIRE target */
	uint64 op_incarnation_hint; /* RETIRE tombstone owner */
	uint64 seed_floor_full;		/* SEED payload */
	uint64 seed_epoch;
	uint32 seed_mxid_floor; /* SEED payload, mxid face (spec-7.1 D3-a) */
} ClusterXidStripeBootShmem;

static ClusterXidStripeBootShmem *StripeBootShmem = NULL;

/* process-local: lazy latch ran (one-way, per process) */
static bool stripe_latch_done = false;
static bool mxid_stripe_latch_done = false;

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
		StripeBootShmem->slot_state = CLUSTER_XID_STRIPE_SLOT_UNKNOWN;
		pg_atomic_init_u64(&StripeBootShmem->req_seq, 0);
		pg_atomic_init_u64(&StripeBootShmem->done_seq, 0);
		pg_atomic_init_u32(&StripeBootShmem->op_result, 0);
		pg_atomic_init_u32(&StripeBootShmem->done_op, 0);
		pg_atomic_init_u64(&StripeBootShmem->replay_floor_full, 0);
		pg_atomic_init_u64(&StripeBootShmem->replay_epoch, 0);
		pg_atomic_init_u32(&StripeBootShmem->replay_active_bitmap, 0);
		pg_atomic_init_u64(&StripeBootShmem->herding_floor_full, 0);
		pg_atomic_init_u64(&StripeBootShmem->cluster_min_active_hwm, 0);
		pg_atomic_init_u64(&StripeBootShmem->cluster_max_active_hwm, 0);
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
stripe_read_activation_one(int fd, ClusterXidStripeActivationRecord *out, uint32 *out_mxid_floor)
{
	char slot[CLUSTER_VOTING_SLOT_BYTES];
	ClusterVotingDiskIoState rc;

	if (out_mxid_floor)
		*out_mxid_floor = 0;

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

	/*
	 * spec-7.1 D3-a: parse the "PGXM" mxid-floor extension riding the
	 * same slot at a fixed offset.  A slot written by a pre-extension
	 * binary reads back zeros there (record-absent); any invalid
	 * content is likewise treated as absent -- the mxid face then
	 * stays fail-closed while the xid activation stands on its own
	 * CRC.  Never a CORRUPT verdict: the extension is optional and
	 * must not hold up the xid face.
	 */
	if (out_mxid_floor) {
		ClusterMxidStripeExtensionRecord ext;

		memcpy(&ext, slot + CLUSTER_PGXM_SLOT_OFFSET, sizeof(ext));
		if (cluster_mxid_stripe_extension_record_valid(&ext))
			*out_mxid_floor = ext.activated_mxid_floor;
	}
	return STRIPE_READ_VALID;
}

/* Same classification for a region-4 per-node slot record. */
static StripeSlotReadClass
stripe_read_slot_one(int fd, int32 node, ClusterXidStripeSlotRecord *out, bool *out_retired)
{
	char slot[CLUSTER_VOTING_SLOT_BYTES];
	ClusterVotingDiskIoState rc;
	ClusterXidStripeSlotRecord probe;

	rc = cluster_voting_disk_read_stripe_slot(fd, (uint32)node, slot);
	if (rc != CLUSTER_VOTING_DISK_IO_OK) {
		struct stat st;

		if (fstat(fd, &st) == 0 && (off_t)st.st_size < CLUSTER_VOTING_FILE_BYTES_MIN)
			return STRIPE_READ_ABSENT;
		return STRIPE_READ_UNREADABLE;
	}

	if (buffer_is_all_zeros(slot, sizeof(slot)))
		return STRIPE_READ_ABSENT;

	memcpy(&probe, slot, sizeof(probe));
	if (!cluster_xid_stripe_slot_record_valid(&probe, node))
		return STRIPE_READ_CORRUPT;
	*out = probe;
	if (out_retired)
		*out_retired = (probe.retired != 0);
	return STRIPE_READ_VALID;
}

/*
 * Read one region-4 slot across all disks with newest-generation-wins
 * adoption.  Returns the aggregate class; on VALID, *out holds the
 * winning record.  Shared by the self-slot scan and the RETIRE
 * read-modify-write.
 */
static StripeSlotReadClass
stripe_read_slot_all(const int *fds, int n_disks, int32 node, ClusterXidStripeSlotRecord *out)
{
	bool have_valid = false;
	bool have_corrupt = false;
	bool have_readable = false;
	int i;

	for (i = 0; i < n_disks; i++) {
		ClusterXidStripeSlotRecord rec;

		switch (stripe_read_slot_one(fds[i], node, &rec, NULL)) {
		case STRIPE_READ_VALID:
			have_readable = true;
			if (!have_valid || rec.generation > out->generation) {
				*out = rec;
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

	if (have_valid)
		return STRIPE_READ_VALID;
	if (have_corrupt)
		return STRIPE_READ_CORRUPT;
	if (have_readable)
		return STRIPE_READ_ABSENT;
	return STRIPE_READ_UNREADABLE;
}

/*
 * Scan region 5 across the opened disks and publish the verdict.
 * Adoption rule: among valid records the newest generation wins (the
 * JCMK "read newest" torn-write guard); epoch regression versus an
 * already-published record is rejected (monotonic envelope).
 * With striping on, also scan THIS node's region-4 slot (D5c) and
 * publish {ABSENT | MINE | RETIRED | CORRUPT} plus the per-slot floor
 * consumed by the D5e allocation clamp.
 */
void
cluster_xid_stripe_scan_disks(const int *fds, int n_disks)
{
	ClusterXidStripeActivationRecord best;
	uint32 best_mxid_floor = 0;
	bool have_valid = false;
	bool have_corrupt = false;
	bool have_readable = false;
	int i;

	if (StripeBootShmem == NULL || fds == NULL || n_disks <= 0)
		return;

	memset(&best, 0, sizeof(best));

	for (i = 0; i < n_disks; i++) {
		ClusterXidStripeActivationRecord rec;
		uint32 rec_mxid_floor;

		switch (stripe_read_activation_one(fds[i], &rec, &rec_mxid_floor)) {
		case STRIPE_READ_VALID:
			have_readable = true;
			if (!have_valid || rec.generation > best.generation) {
				best = rec;
				/* the extension travels with its slot's PGXA record */
				best_mxid_floor = rec_mxid_floor;
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
			StripeBootShmem->mxid_floor = best_mxid_floor;
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

	/* D5c: THIS node's region-4 slot verdict (striping-on nodes only). */
	if (cluster_xid_striping && cluster_node_id >= 0 && cluster_node_id < CLUSTER_XID_STRIDE) {
		ClusterXidStripeSlotRecord mine;

		memset(&mine, 0, sizeof(mine));
		switch (stripe_read_slot_all(fds, n_disks, cluster_node_id, &mine)) {
		case STRIPE_READ_VALID:
			LWLockAcquire(&StripeBootShmem->lock, LW_EXCLUSIVE);
			if (mine.retired) {
				StripeBootShmem->slot_state = CLUSTER_XID_STRIPE_SLOT_RETIRED;
				StripeBootShmem->my_slot_claimed = false;
			} else {
				uint64 cur;

				StripeBootShmem->slot_state = CLUSTER_XID_STRIPE_SLOT_MINE;
				StripeBootShmem->my_slot_claimed = true;
				StripeBootShmem->my_slot_floor_full = mine.floor_full;
				StripeBootShmem->my_owner_incarnation = mine.owner_incarnation;
				if (mine.next_xid_hwm_full > StripeBootShmem->my_hwm_on_disk)
					StripeBootShmem->my_hwm_on_disk = mine.next_xid_hwm_full;
				/* D3: the durable hwm is a crash-surviving allocation
				 * promise — restore it into the herding floor so a
				 * restart never issues below what it once published. */
				cur = pg_atomic_read_u64(&StripeBootShmem->herding_floor_full);
				if (mine.next_xid_hwm_full > cur)
					pg_atomic_write_u64(&StripeBootShmem->herding_floor_full,
										mine.next_xid_hwm_full);
			}
			LWLockRelease(&StripeBootShmem->lock);
			break;
		case STRIPE_READ_ABSENT:
			LWLockAcquire(&StripeBootShmem->lock, LW_EXCLUSIVE);
			if (StripeBootShmem->slot_state == CLUSTER_XID_STRIPE_SLOT_UNKNOWN)
				StripeBootShmem->slot_state = CLUSTER_XID_STRIPE_SLOT_ABSENT;
			LWLockRelease(&StripeBootShmem->lock);
			break;
		case STRIPE_READ_CORRUPT:
			LWLockAcquire(&StripeBootShmem->lock, LW_EXCLUSIVE);
			if (StripeBootShmem->slot_state != CLUSTER_XID_STRIPE_SLOT_MINE)
				StripeBootShmem->slot_state = CLUSTER_XID_STRIPE_SLOT_CORRUPT;
			LWLockRelease(&StripeBootShmem->lock);
			break;
		case STRIPE_READ_UNREADABLE:
			break;
		}
	}
}

/* ============================================================
 * Seed mailbox (LMON stages, qvotec writes).
 * ============================================================ */

/*
 * Stage a mailbox op (single producer = the LMON tick / removal
 * driver, both on this node's LMON; one op in flight).  Returns the
 * staged request seq, or 0 when another op is still pending.
 */
static uint64
stripe_stage_op(StripeMailboxOp op, int32 target_node, uint64 incarnation_hint)
{
	uint64 req;
	uint64 done;

	/*
	 * TWO producers exist (the LMON joiner/removal ticks and the
	 * qvotec-context admission-finalize gate), so the whole
	 * check-payload-bump sequence is serialized under the lock; the
	 * consumer additionally echoes the op it ACKed (done_op) so a
	 * blocking submitter can never trust another op's result.
	 */
	LWLockAcquire(&StripeBootShmem->lock, LW_EXCLUSIVE);
	req = pg_atomic_read_u64(&StripeBootShmem->req_seq);
	done = pg_atomic_read_u64(&StripeBootShmem->done_seq);
	if (req != done) {
		LWLockRelease(&StripeBootShmem->lock);
		return 0; /* one in flight already */
	}
	StripeBootShmem->op = (uint32)op;
	StripeBootShmem->op_target_node = target_node;
	StripeBootShmem->op_incarnation_hint = incarnation_hint;
	pg_write_barrier();
	pg_atomic_write_u64(&StripeBootShmem->req_seq, req + 1);
	LWLockRelease(&StripeBootShmem->lock);
	return req + 1;
}

/*
 * Stage the activation seed (single-shot).  Called by the joiner gate
 * when striping is on, the record is durably ABSENT and this node is
 * the seed candidate.  Floor: shared-authority nextXid rounded up to
 * the stride boundary plus one herding slack (spec-6.15 §2.3).
 */
static void
stripe_stage_seed_request(void)
{
	FullTransactionId next_full;
	uint64 floor;
	uint32 mxid_floor;

	if (pg_atomic_read_u64(&StripeBootShmem->req_seq)
		!= pg_atomic_read_u64(&StripeBootShmem->done_seq))
		return; /* one op in flight already */

	next_full = ReadNextFullTransactionId();
	floor = U64FromFullTransactionId(next_full);
	floor = floor - (floor % CLUSTER_XID_STRIDE) + CLUSTER_XID_STRIDE; /* round up */
	floor += (uint64)cluster_xid_herding_slack;

	/*
	 * spec-7.1 D3-a: sample the mxid activation floor at the same
	 * staging point.  Like the xid floor, its dominance over shared-
	 * page history rests on activation landing at cluster formation,
	 * before member admission opens user transactions (the seed's
	 * post-recovery counter is the observed cluster high-water,
	 * spec-6.15 §2.3 discipline).  Rounded up to the stride plus one
	 * stride of margin; multixacts have no herding, so no slack term.
	 */
	mxid_floor = (uint32)ReadNextMultiXactId();
	mxid_floor = mxid_floor - (mxid_floor % CLUSTER_MXID_STRIDE) + 2 * CLUSTER_MXID_STRIDE;
	if (mxid_floor < FirstMultiXactId)
		mxid_floor += CLUSTER_MXID_STRIDE; /* wrapped onto 0: skip Invalid */

	LWLockAcquire(&StripeBootShmem->lock, LW_EXCLUSIVE);
	StripeBootShmem->seed_floor_full = floor;
	StripeBootShmem->seed_epoch = 1;
	StripeBootShmem->seed_mxid_floor = mxid_floor;
	LWLockRelease(&StripeBootShmem->lock);

	if (stripe_stage_op(STRIPE_OP_SEED, -1, 0) != 0)
		ereport(LOG, (errmsg("cluster xid stripe: staging activation seed (floor " UINT64_FORMAT
							 ", epoch 1) — this node is the seed candidate",
							 floor)));
}

/*
 * qvotec: write the staged activation record.  Re-scan first (a record
 * that appeared meanwhile is adopted, never overwritten); write to
 * every disk and require a majority before publishing.  Returns the
 * mailbox result (1 = durable / adopted).
 */
static uint32
stripe_service_seed(const int *fds, int n_disks)
{
	ClusterXidStripeActivationRecord rec;
	ClusterMxidStripeExtensionRecord ext;
	char slot[CLUSTER_VOTING_SLOT_BYTES];
	int disks_ok = 0;
	int majority;
	int i;

	/* Adopt-not-overwrite: a concurrent seeder may have landed one. */
	cluster_xid_stripe_scan_disks(fds, n_disks);
	if (cluster_xid_stripe_disk_state() != CLUSTER_XID_STRIPE_DISK_ABSENT)
		return 1;

	memset(&rec, 0, sizeof(rec));
	memset(&ext, 0, sizeof(ext));
	rec.magic = CLUSTER_PGXA_MAGIC;
	rec.version = CLUSTER_PGXA_VERSION;
	ext.magic = CLUSTER_PGXM_MAGIC;
	ext.version = CLUSTER_PGXM_VERSION;
	LWLockAcquire(&StripeBootShmem->lock, LW_SHARED);
	rec.activated_floor_full = StripeBootShmem->seed_floor_full;
	rec.stride_mode_epoch = StripeBootShmem->seed_epoch;
	ext.activated_mxid_floor = StripeBootShmem->seed_mxid_floor;
	LWLockRelease(&StripeBootShmem->lock);
	rec.generation = 1;
	cluster_xid_stripe_activation_record_compute_crc(&rec);
	ext.generation = 1;
	cluster_mxid_stripe_extension_record_compute_crc(&ext);

	/*
	 * spec-7.1 D3-a: the "PGXM" mxid-floor extension rides the SAME
	 * 512-byte slot write as the PGXA record, so both floors land
	 * atomically in one sector -- there is no ordering window where
	 * one face is durable without the other.
	 */
	memset(slot, 0, sizeof(slot));
	memcpy(slot, &rec, sizeof(rec));
	memcpy(slot + CLUSTER_PGXM_SLOT_OFFSET, &ext, sizeof(ext));

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
		StripeBootShmem->mxid_floor = ext.activated_mxid_floor;
		LWLockRelease(&StripeBootShmem->lock);
		ereport(LOG, (errmsg("cluster xid stripe: activation record durable on %d/%d disks "
							 "(floor " UINT64_FORMAT ", epoch " UINT64_FORMAT ", mxid floor %u)",
							 disks_ok, n_disks, rec.activated_floor_full, rec.stride_mode_epoch,
							 (unsigned)ext.activated_mxid_floor)));
		return 1;
	}
	ereport(LOG, (errmsg("cluster xid stripe: activation seed reached only %d/%d disks "
						 "(majority %d) — will retry",
						 disks_ok, n_disks, majority)));
	return 0;
}

/*
 * qvotec: first-claim THIS node's region-4 slot.  Owner identity =
 * {node_id, this boot's qvotec self-incarnation} (appendix B.3 — the
 * stored value is written once and never rewritten; a later rejoin
 * resumes on node_id + not-retired, never on incarnation equality).
 * Floor = the published activation floor; hwm starts at the floor.
 * Adopt-not-overwrite: re-scan first, resume/refuse on what is found.
 */
static uint32
stripe_service_claim(const int *fds, int n_disks)
{
	ClusterXidStripeSlotRecord rec;
	char slot[CLUSTER_VOTING_SLOT_BYTES];
	uint64 floor = 0;
	int disks_ok = 0;
	int majority;
	int i;

	if (cluster_node_id < 0 || cluster_node_id >= CLUSTER_XID_STRIDE)
		return 0;

	/* The claim floor is the published activation floor. */
	if (!cluster_xid_stripe_get_activation(&floor, NULL, NULL))
		return 0;

	/* Adopt-not-overwrite: scan publishes MINE / RETIRED / CORRUPT. */
	cluster_xid_stripe_scan_disks(fds, n_disks);
	if (cluster_xid_stripe_slot_state() != CLUSTER_XID_STRIPE_SLOT_ABSENT)
		return 1; /* resolved some other way; the gate re-reads it */

	memset(&rec, 0, sizeof(rec));
	rec.magic = CLUSTER_PGXS_MAGIC;
	rec.version = CLUSTER_PGXS_VERSION;
	rec.node_id = cluster_node_id;
	rec.retired = 0;
	rec.owner_incarnation = cluster_qvotec_self_incarnation_value();
	rec.floor_full = floor;
	rec.next_xid_hwm_full = floor;
	LWLockAcquire(&StripeBootShmem->lock, LW_SHARED);
	rec.stride_mode_epoch = StripeBootShmem->epoch;
	LWLockRelease(&StripeBootShmem->lock);
	rec.generation = 1;
	cluster_xid_stripe_slot_record_compute_crc(&rec);

	if (rec.owner_incarnation == 0)
		return 0; /* no identity seed yet; retry next tick */

	memset(slot, 0, sizeof(slot));
	memcpy(slot, &rec, sizeof(rec));

	for (i = 0; i < n_disks; i++) {
		if (cluster_voting_disk_write_stripe_slot(fds[i], (uint32)cluster_node_id, slot)
			== CLUSTER_VOTING_DISK_IO_OK)
			disks_ok++;
	}

	majority = (n_disks / 2) + 1;
	if (disks_ok >= majority) {
		LWLockAcquire(&StripeBootShmem->lock, LW_EXCLUSIVE);
		StripeBootShmem->slot_state = CLUSTER_XID_STRIPE_SLOT_MINE;
		StripeBootShmem->my_slot_claimed = true;
		StripeBootShmem->my_slot_floor_full = rec.floor_full;
		StripeBootShmem->my_owner_incarnation = rec.owner_incarnation;
		LWLockRelease(&StripeBootShmem->lock);
		ereport(LOG, (errmsg("cluster xid stripe: slot %d claimed (owner incarnation " UINT64_FORMAT
							 ", floor " UINT64_FORMAT ") durable on %d/%d disks",
							 cluster_node_id, rec.owner_incarnation, rec.floor_full, disks_ok,
							 n_disks)));
		return 1;
	}
	return 0;
}

/*
 * qvotec: mark target's region-4 slot retired (spec-5.18 removal,
 * appendix B.3).  Read-modify-write preserving the owner identity and
 * floors; a never-claimed slot gets a retired tombstone seeded from
 * the removal driver's incarnation hint.  A corrupt slot fails closed
 * (never guessed, never overwritten) — the removal driver holds.
 */
static uint32
stripe_service_retire(const int *fds, int n_disks, int32 target, uint64 hint)
{
	ClusterXidStripeSlotRecord rec;
	char slot[CLUSTER_VOTING_SLOT_BYTES];
	int disks_ok = 0;
	int majority;
	int i;

	if (target < 0 || target >= CLUSTER_XID_STRIDE)
		return 0;

	memset(&rec, 0, sizeof(rec));
	switch (stripe_read_slot_all(fds, n_disks, target, &rec)) {
	case STRIPE_READ_VALID:
		if (rec.retired)
			return 1; /* already retired (idempotent) */
		rec.retired = 1;
		rec.generation += 1;
		break;
	case STRIPE_READ_ABSENT: {
		uint64 floor = 0;

		if (!cluster_xid_stripe_get_activation(&floor, NULL, NULL))
			return 1; /* cluster never activated: nothing to retire */
		if (hint == 0)
			return 1; /* never admitted AND never claimed: nothing to retire */
		rec.magic = CLUSTER_PGXS_MAGIC;
		rec.version = CLUSTER_PGXS_VERSION;
		rec.node_id = target;
		rec.retired = 1;
		rec.owner_incarnation = hint;
		rec.floor_full = floor;
		rec.next_xid_hwm_full = floor;
		LWLockAcquire(&StripeBootShmem->lock, LW_SHARED);
		rec.stride_mode_epoch = StripeBootShmem->epoch;
		LWLockRelease(&StripeBootShmem->lock);
		rec.generation = 1;
		break;
	}
	case STRIPE_READ_CORRUPT:
	case STRIPE_READ_UNREADABLE:
		return 0; /* fail closed; removal driver retries */
	}
	cluster_xid_stripe_slot_record_compute_crc(&rec);

	memset(slot, 0, sizeof(slot));
	memcpy(slot, &rec, sizeof(rec));

	for (i = 0; i < n_disks; i++) {
		if (cluster_voting_disk_write_stripe_slot(fds[i], (uint32)target, slot)
			== CLUSTER_VOTING_DISK_IO_OK)
			disks_ok++;
	}

	majority = (n_disks / 2) + 1;
	if (disks_ok >= majority) {
		ereport(LOG, (errmsg("cluster xid stripe: slot %d retired (removal) durable on %d/%d disks",
							 target, disks_ok, n_disks)));
		return 1;
	}
	return 0;
}

/*
 * qvotec: service one pending mailbox op per poll.
 */
void
cluster_xid_stripe_service_seed(const int *fds, int n_disks)
{
	uint64 req, done;
	uint32 result = 0;
	uint32 op;
	int32 target;
	uint64 hint;

	if (StripeBootShmem == NULL || fds == NULL || n_disks <= 0)
		return;

	req = pg_atomic_read_u64(&StripeBootShmem->req_seq);
	done = pg_atomic_read_u64(&StripeBootShmem->done_seq);
	if (req == done)
		return;
	pg_read_barrier();

	LWLockAcquire(&StripeBootShmem->lock, LW_SHARED);
	op = StripeBootShmem->op;
	target = StripeBootShmem->op_target_node;
	hint = StripeBootShmem->op_incarnation_hint;
	LWLockRelease(&StripeBootShmem->lock);

	switch ((StripeMailboxOp)op) {
	case STRIPE_OP_SEED:
		result = stripe_service_seed(fds, n_disks);
		break;
	case STRIPE_OP_CLAIM:
		result = stripe_service_claim(fds, n_disks);
		break;
	case STRIPE_OP_RETIRE:
		result = stripe_service_retire(fds, n_disks, target, hint);
		break;
	case STRIPE_OP_NONE:
		break;
	}

	/* ACK with the result + op echo; producers re-stage on failure. */
	pg_atomic_write_u32(&StripeBootShmem->op_result, result);
	pg_atomic_write_u32(&StripeBootShmem->done_op, op);
	pg_write_barrier();
	pg_atomic_write_u64(&StripeBootShmem->done_seq, req);
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
		uint32 slot_state;

		/*
		 * D5d (8.A): hold until local recovery is over.  The seed floor
		 * must be computed from the POST-recovery shared nextXid — a
		 * mid-recovery read could seed a floor below the recovery-end
		 * value, leaving pre-striping dense history ABOVE the floor,
		 * which the derivation would misattribute to stripe classes
		 * (false origin, 8.A).  It also guarantees the JOIN WAL record
		 * below is insertable.
		 */
		if (RecoveryInProgress())
			return CLUSTER_XID_STRIPE_JOIN_HOLD;

		switch ((ClusterXidStripeDiskState)state) {
		case CLUSTER_XID_STRIPE_DISK_PUBLISHED:
			break; /* activation resolved; fall through to the slot vet */
		case CLUSTER_XID_STRIPE_DISK_ABSENT:
			if (self_may_seed)
				stripe_stage_seed_request();
			return CLUSTER_XID_STRIPE_JOIN_HOLD;
		case CLUSTER_XID_STRIPE_DISK_CORRUPT:
			return CLUSTER_XID_STRIPE_JOIN_REFUSE;
		case CLUSTER_XID_STRIPE_DISK_UNKNOWN:
			return CLUSTER_XID_STRIPE_JOIN_HOLD;
		}

		/*
		 * D5c slot vet: admission additionally requires THIS node's
		 * region-4 slot resolved to MINE.  ABSENT stages the first
		 * claim (own slot, sole writer — no cross-node race) and
		 * holds; RETIRED is the permanent v1 refusal (a removed
		 * identity never resumes its congruence class — 53RB1);
		 * CORRUPT fails closed.
		 */
		LWLockAcquire(&StripeBootShmem->lock, LW_SHARED);
		slot_state = StripeBootShmem->slot_state;
		LWLockRelease(&StripeBootShmem->lock);

		switch ((ClusterXidStripeSlotState)slot_state) {
		case CLUSTER_XID_STRIPE_SLOT_MINE:
			/*
			 * D5d ordering invariant: the JOIN record must be in OUR WAL
			 * thread before the gate opens (i.e. before this node's first
			 * xid-bearing record) so any order-preserving WAL consumer
			 * learns "slot active above floor" first.  Not insertable yet
			 * -> hold (retried next tick).
			 */
			if (!cluster_xid_stripe_emit_join_wal())
				return CLUSTER_XID_STRIPE_JOIN_HOLD;
			return CLUSTER_XID_STRIPE_JOIN_PROCEED;
		case CLUSTER_XID_STRIPE_SLOT_ABSENT:
			(void)stripe_stage_op(STRIPE_OP_CLAIM, cluster_node_id, 0);
			return CLUSTER_XID_STRIPE_JOIN_HOLD;
		case CLUSTER_XID_STRIPE_SLOT_RETIRED:
		case CLUSTER_XID_STRIPE_SLOT_CORRUPT:
			return CLUSTER_XID_STRIPE_JOIN_REFUSE;
		case CLUSTER_XID_STRIPE_SLOT_UNKNOWN:
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

/*
 * Companion one-way lazy latch for the mxid stripe face (spec-7.1
 * D3-a).  Latches only when the published activation carried a valid
 * "PGXM" extension floor; a pre-extension activation (mxid_floor 0)
 * leaves the mxid face unlatched for the life of the process --
 * allocation stays vanilla dense and readers keep failing closed on
 * foreign multis (honest degrade, never a misattribution).
 */
void
cluster_mxid_stripe_lazy_latch(void)
{
	uint32 mxid_floor = 0;
	bool published = false;

	if (mxid_stripe_latch_done || StripeBootShmem == NULL)
		return;

	LWLockAcquire(&StripeBootShmem->lock, LW_SHARED);
	if (StripeBootShmem->disk_state == CLUSTER_XID_STRIPE_DISK_PUBLISHED
		&& StripeBootShmem->mxid_floor != 0) {
		published = true;
		mxid_floor = StripeBootShmem->mxid_floor;
	}
	LWLockRelease(&StripeBootShmem->lock);

	if (!published)
		return; /* stays unlatched; wrappers keep failing closed */

	cluster_mxid_stripe_latch_runtime(true, cluster_xid_allocation_slot(), (MultiXactId)mxid_floor);
	mxid_stripe_latch_done = true;
}

FullTransactionId
cluster_xid_stripe_my_slot_floor(void)
{
	/*
	 * Process-local cache: the claim precedes gate-open, which precedes
	 * any allocation, so the first successful read is the final value
	 * (the per-slot floor is written once and never lowered).  Callers
	 * run under XidGenLock — caching keeps that hot path free of this
	 * module's LWLock after the first claimed read.  Never negatively
	 * cached: until the claim is published we re-read.
	 */
	static bool floor_cached = false;
	static FullTransactionId floor_cache = { 0 };
	FullTransactionId result = InvalidFullTransactionId;

	if (floor_cached)
		return floor_cache;
	if (StripeBootShmem == NULL)
		return result;

	LWLockAcquire(&StripeBootShmem->lock, LW_SHARED);
	if (StripeBootShmem->my_slot_claimed)
		result = FullTransactionIdFromU64(StripeBootShmem->my_slot_floor_full);
	LWLockRelease(&StripeBootShmem->lock);

	if (FullTransactionIdIsValid(result)) {
		floor_cache = result;
		floor_cached = true;
	}
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

ClusterXidStripeSlotState
cluster_xid_stripe_slot_state(void)
{
	uint32 state;

	if (StripeBootShmem == NULL)
		return CLUSTER_XID_STRIPE_SLOT_UNKNOWN;

	LWLockAcquire(&StripeBootShmem->lock, LW_SHARED);
	state = StripeBootShmem->slot_state;
	LWLockRelease(&StripeBootShmem->lock);
	return (ClusterXidStripeSlotState)state;
}

/*
 * spec-5.18 removal hook: durably retire target_node's stripe slot
 * BEFORE the removal point of no return.  Runs on the removal
 * coordinator's LMON (the same single producer as the join gate);
 * blocking-bounded on this node's qvotec, mirroring the join-marker
 * submit.  True = retired durable (or nothing to retire); false =
 * not durable — the caller stays pre-commit and retries.
 */
bool
cluster_xid_stripe_submit_retire(int32 target_node, uint64 owner_incarnation_hint)
{
	uint64 seq;
	uint64 deadline_us;
	int wait_ms;

	if (StripeBootShmem == NULL)
		return false;
	if (target_node < 0 || target_node >= CLUSTER_XID_STRIDE)
		return true; /* outside the stripe width: no slot to retire */

	/* Never activated -> no stripe face to retire.  (PUBLISHED can only
	 * ever appear later, and the gate re-runs each tick until commit.) */
	if (cluster_xid_stripe_disk_state() != CLUSTER_XID_STRIPE_DISK_PUBLISHED)
		return true;

	seq = stripe_stage_op(STRIPE_OP_RETIRE, target_node, owner_incarnation_hint);
	if (seq == 0)
		return false; /* mailbox busy; retry next tick */

	wait_ms = cluster_quorum_poll_interval_ms * 3 + 2000;
	deadline_us = (uint64)GetCurrentTimestamp() + (uint64)wait_ms * 1000ULL;
	for (;;) {
		if (pg_atomic_read_u64(&StripeBootShmem->done_seq) == seq) {
			pg_read_barrier();
			return pg_atomic_read_u32(&StripeBootShmem->done_op) == (uint32)STRIPE_OP_RETIRE
				   && pg_atomic_read_u32(&StripeBootShmem->op_result) == 1;
		}
		if ((uint64)GetCurrentTimestamp() >= deadline_us)
			return false;
		pg_usleep(2 * 1000); /* 2 ms */
	}
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

/* ============================================================
 * D3 counter herding (qvotec tick).
 * ============================================================ */

/*
 * One herding pass, run from the qvotec poll after the stripe face is
 * PUBLISHED and this node's slot is MINE:
 *
 *   1. read every DECLARED, non-retired peer slot's region-4 record
 *      (newest generation across disks) and publish the min/max hwm;
 *   2. compute this node's hwm promise = the first in-class candidate
 *      at or above max(local nextXid, cluster_max - slack, floor) and,
 *      when it moved forward, write it durably into our region-4 slot
 *      (sole writer) BEFORE arming it as the local herding floor.
 *
 * The floor is consumed by GetNewTransactionId's clamp (D5e face), so
 * a lagging node "jumps" at its next allocation — the jump abandons
 * only never-issued in-class positions (pure slack, spec §3.3) and
 * needs no WAL.  Publish-before-arm keeps the promise crash-safe: the
 * scan restores the durable hwm into the floor at every boot.
 */
void
cluster_xid_stripe_herding_tick(const int *fds, int n_disks)
{
	uint64 max_hwm = 0;
	uint64 min_hwm = 0;
	uint64 floor_full;
	uint64 local_next;
	uint64 target;
	uint64 promised;
	int slot;

	if (StripeBootShmem == NULL || fds == NULL || n_disks <= 0)
		return;
	if (!cluster_enabled || !cluster_xid_striping)
		return;
	if (cluster_xid_stripe_disk_state() != CLUSTER_XID_STRIPE_DISK_PUBLISHED
		|| cluster_xid_stripe_slot_state() != CLUSTER_XID_STRIPE_SLOT_MINE)
		return;

	/* An ARMED cluster-xid-herding-stall freezes the herding plane
	 * only, for as long as it stays armed (the non-consuming peek —
	 * arm -> observe -> disarm, test surface, L408).  hwm publication
	 * and jump arming stop; the lease-critical rest of the qvotec
	 * poll is untouched. */
	{
		uint64 dummy;

		if (cluster_cr_injection_armed("cluster-xid-herding-stall", &dummy))
			return;
	}

	/* 1. peer hwm sweep (declared, valid, non-retired slots only). */
	for (slot = 0; slot < CLUSTER_XID_STRIDE; slot++) {
		ClusterXidStripeSlotRecord rec;

		if (cluster_conf_lookup_node(slot) == NULL)
			continue;
		memset(&rec, 0, sizeof(rec));
		if (stripe_read_slot_all(fds, n_disks, slot, &rec) != STRIPE_READ_VALID)
			continue;
		if (rec.retired)
			continue;
		if (rec.next_xid_hwm_full > max_hwm)
			max_hwm = rec.next_xid_hwm_full;
		if (min_hwm == 0 || rec.next_xid_hwm_full < min_hwm)
			min_hwm = rec.next_xid_hwm_full;
	}
	if (max_hwm != 0) {
		pg_atomic_write_u64(&StripeBootShmem->cluster_max_active_hwm, max_hwm);
		pg_atomic_write_u64(&StripeBootShmem->cluster_min_active_hwm, min_hwm);
	}

	/* 2. this node's hwm promise. */
	LWLockAcquire(&StripeBootShmem->lock, LW_SHARED);
	floor_full = StripeBootShmem->my_slot_floor_full;
	promised = StripeBootShmem->my_hwm_on_disk;
	LWLockRelease(&StripeBootShmem->lock);

	local_next = U64FromFullTransactionId(ReadNextFullTransactionId());
	target = local_next;
	if (max_hwm > (uint64)cluster_xid_herding_slack
		&& max_hwm - (uint64)cluster_xid_herding_slack > target)
		target = max_hwm - (uint64)cluster_xid_herding_slack; /* observe-and-jump */
	if (floor_full > target)
		target = floor_full;
	target = U64FromFullTransactionId(
		cluster_xid_next_striped_full(FullTransactionIdFromU64(target), cluster_node_id));

	if (target > promised) {
		ClusterXidStripeSlotRecord rec;
		char slotbuf[CLUSTER_VOTING_SLOT_BYTES];
		bool is_jump = target > local_next;
		int disks_ok = 0;
		int i;

		/* read-modify-write our own record (sole writer). */
		memset(&rec, 0, sizeof(rec));
		if (stripe_read_slot_all(fds, n_disks, cluster_node_id, &rec) != STRIPE_READ_VALID
			|| rec.retired)
			return; /* face shifted under us; next tick re-evaluates */
		rec.next_xid_hwm_full = target;
		rec.generation += 1;
		cluster_xid_stripe_slot_record_compute_crc(&rec);
		memset(slotbuf, 0, sizeof(slotbuf));
		memcpy(slotbuf, &rec, sizeof(rec));

		/*
		 * Durability split: a TRACKING publish (hwm following the local
		 * allocator) skips fdatasync — it runs every poll under
		 * allocation load, and a per-poll fsync across every voting
		 * disk can push qvotec's poll period past the in_quorum lease
		 * window (a self-fence).  Losing a tracking write in a crash
		 * only staleness-skews the peers' min/max view; uniqueness is
		 * untouched.  A JUMP (a promise ABOVE the local allocator) is
		 * armed as an allocation floor, so it stays publish-before-arm
		 * durable.
		 */
		for (i = 0; i < n_disks; i++) {
			if (cluster_voting_disk_write_stripe_slot_ex(fds[i], (uint32)cluster_node_id, slotbuf,
														 is_jump)
				== CLUSTER_VOTING_DISK_IO_OK)
				disks_ok++;
		}
		if (disks_ok < (n_disks / 2) + 1)
			return; /* not on a majority; do NOT arm (publish-before-arm) */

		LWLockAcquire(&StripeBootShmem->lock, LW_EXCLUSIVE);
		if (target > StripeBootShmem->my_hwm_on_disk)
			StripeBootShmem->my_hwm_on_disk = target;
		LWLockRelease(&StripeBootShmem->lock);

		/* arm: only a durably published jump becomes an allocation floor. */
		if (is_jump && target > pg_atomic_read_u64(&StripeBootShmem->herding_floor_full)) {
			pg_atomic_write_u64(&StripeBootShmem->herding_floor_full, target);
			ereport(DEBUG1,
					(errmsg("cluster xid stripe: herding jump armed to " UINT64_FORMAT
							" (cluster max " UINT64_FORMAT ", local next " UINT64_FORMAT ")",
							target, max_hwm, local_next)));
		}
	}
}

/*
 * D3 allocation-side faces (read under XidGenLock; atomics keep the
 * hot path free of this module's lock).  herding_floor: this node's
 * armed allocation promise.  window_exceeded: is candidate ahead of
 * the slowest ACTIVE slot's published hwm by more than the hard limit
 * (slack x 64)?  TRUE means counter herding has been failing long
 * enough that continuing to issue would tear at the single global xid
 * window — the caller refuses fail-closed (53RB2, spec §3.3).  A
 * cluster with no peer publication yet (min == 0) never trips it.
 */
FullTransactionId
cluster_xid_stripe_herding_floor(void)
{
	uint64 v;

	if (StripeBootShmem == NULL)
		return InvalidFullTransactionId;
	v = pg_atomic_read_u64(&StripeBootShmem->herding_floor_full);
	return v == 0 ? InvalidFullTransactionId : FullTransactionIdFromU64(v);
}

/* D6 observability snapshot (single lock acquisition + atomics). */
void
cluster_xid_stripe_observe(ClusterXidStripeObs *obs)
{
	memset(obs, 0, sizeof(*obs));
	if (StripeBootShmem == NULL)
		return;

	LWLockAcquire(&StripeBootShmem->lock, LW_SHARED);
	obs->disk_state = StripeBootShmem->disk_state;
	obs->slot_state = StripeBootShmem->slot_state;
	obs->activated_floor_full = StripeBootShmem->floor_full;
	obs->stride_mode_epoch = StripeBootShmem->epoch;
	obs->my_slot_floor_full
		= StripeBootShmem->my_slot_claimed ? StripeBootShmem->my_slot_floor_full : 0;
	obs->my_hwm_on_disk = StripeBootShmem->my_hwm_on_disk;
	obs->activated_mxid_floor = StripeBootShmem->mxid_floor;
	LWLockRelease(&StripeBootShmem->lock);

	obs->herding_floor_full = pg_atomic_read_u64(&StripeBootShmem->herding_floor_full);
	obs->cluster_min_active_hwm = pg_atomic_read_u64(&StripeBootShmem->cluster_min_active_hwm);
	obs->cluster_max_active_hwm = pg_atomic_read_u64(&StripeBootShmem->cluster_max_active_hwm);
	obs->replay_floor_full = pg_atomic_read_u64(&StripeBootShmem->replay_floor_full);
	obs->replay_active_bitmap = pg_atomic_read_u32(&StripeBootShmem->replay_active_bitmap);
}

bool
cluster_xid_stripe_window_exceeded(FullTransactionId candidate)
{
	uint64 min_hwm;
	uint64 cand = U64FromFullTransactionId(candidate);
	uint64 hard = (uint64)cluster_xid_herding_slack * 64;

	if (StripeBootShmem == NULL)
		return false;
	min_hwm = pg_atomic_read_u64(&StripeBootShmem->cluster_min_active_hwm);
	if (min_hwm == 0)
		return false;
	return cand > min_hwm && cand - min_hwm > hard;
}

/* ============================================================
 * D5d replay face: redo publishes, the standby gap-fill consumes.
 * ============================================================ */

void
cluster_xid_stripe_replay_note_join(uint64 floor_full, uint64 epoch, int slot)
{
	uint32 bitmap;

	if (StripeBootShmem == NULL)
		return;

	/* floor/epoch: first JOIN wins; repeats are idempotent (all carry
	 * the same durable activation content; epoch never regresses). */
	if (pg_atomic_read_u64(&StripeBootShmem->replay_floor_full) == 0) {
		pg_atomic_write_u64(&StripeBootShmem->replay_epoch, epoch);
		pg_write_barrier();
		pg_atomic_write_u64(&StripeBootShmem->replay_floor_full, floor_full);
		ereport(LOG, (errmsg("cluster xid stripe: replay learned activation "
							 "(floor " UINT64_FORMAT ", epoch " UINT64_FORMAT ")",
							 floor_full, epoch)));
	}
	bitmap = pg_atomic_read_u32(&StripeBootShmem->replay_active_bitmap);
	pg_atomic_write_u32(&StripeBootShmem->replay_active_bitmap, bitmap | (1u << slot));
}

void
cluster_xid_stripe_replay_note_retire(int slot)
{
	uint32 bitmap;

	if (StripeBootShmem == NULL)
		return;
	bitmap = pg_atomic_read_u32(&StripeBootShmem->replay_active_bitmap);
	pg_atomic_write_u32(&StripeBootShmem->replay_active_bitmap, bitmap & ~(1u << slot));
}

bool
cluster_xid_stripe_replay_filter_active(void)
{
	return StripeBootShmem != NULL && pg_atomic_read_u64(&StripeBootShmem->replay_floor_full) != 0;
}

/*
 * Should the standby gap-fill treat xid as possibly-assigned?  TRUE
 * (fill) for everything below the replay-learned floor (pre-striping
 * dense history) and for active classes above it; FALSE only for an
 * above-floor xid whose class is not active — by the per-thread
 * ordering invariant such a value can never have been issued.
 * Underivable widening fails toward TRUE (fill = the conservative,
 * treat-as-running direction; never a visibility hazard).
 */
bool
cluster_xid_stripe_replay_should_fill(TransactionId xid)
{
	uint64 floor_full;
	uint32 bitmap;
	FullTransactionId fxid;

	if (StripeBootShmem == NULL)
		return true;
	floor_full = pg_atomic_read_u64(&StripeBootShmem->replay_floor_full);
	if (floor_full == 0)
		return true;

	fxid = cluster_xid_widen(xid, ReadNextFullTransactionId());
	if (!FullTransactionIdIsValid(fxid))
		return true; /* fail toward fill */
	if (U64FromFullTransactionId(fxid) < floor_full)
		return true; /* dense pre-striping history */

	bitmap = pg_atomic_read_u32(&StripeBootShmem->replay_active_bitmap);
	return (bitmap & (1u << (xid % CLUSTER_XID_STRIDE))) != 0;
}

#endif /* USE_PGRAC_CLUSTER */
