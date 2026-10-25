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
#include "cluster/cluster_heap_horizon.h"
#include "cluster/cluster_inject.h"		 /* herding-stall injection (D3, L408) */
#include "cluster/cluster_mxid_stripe.h" /* mxid stripe face (spec-7.1 D3-a) */
#include "cluster/cluster_qvotec.h"		 /* CLUSTER_MAX_VOTING_DISKS (mxid evidence bound) */
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_voting_disk_io.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/cluster_xid_stripe_boot.h"
#include "cluster/cluster_xid_stripe_xlog.h" /* JOIN record emission (D5d) */
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/condition_variable.h"
#include "storage/pmsignal.h"
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/wait_event.h"
#include "utils/timestamp.h" /* GetCurrentTimestamp (retire bounded wait) */

/* Cross-check the pure-layer literal against the real slot size. */
StaticAssertDecl(sizeof(ClusterXidStripeSlotRecord) <= CLUSTER_VOTING_SLOT_BYTES,
				 "stripe slot record must fit a voting slot");
StaticAssertDecl(sizeof(ClusterXidStripeSlotRecord) == 96
					 && offsetof(ClusterXidStripeSlotRecord, crc32c) == 92,
				 "PGXS version 3 layout");
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

/* QVOTEC publishes limit only after its range is majority durable. */
typedef struct ClusterXidStripeLease {
	pg_atomic_uint64 floor_full;
	pg_atomic_uint64 limit_full;
	pg_atomic_uint64 published_incarnation; /* durable lease observation */
	pg_atomic_uint32 failed;
	ConditionVariable changed;
	uint64 publisher_incarnation; /* QVOTEC-only; zero on a fresh shmem boot */
	uint64 claim_owner;
	uint64 claim_floor;
	uint64 claim_epoch;
} ClusterXidStripeLease;

/*
 * Shared state.  All fields are guarded by lock except the mailbox
 * seqs (write barrier between payload and request seq).
 */
typedef struct ClusterXidStripeBootShmem {
	LWLock lock;
	ClusterXidStripeLease lease;

	/* region-5 publication */
	uint32 disk_state; /* ClusterXidStripeDiskState */
	uint64 floor_full;
	uint64 epoch;
	uint64 generation;
	uint32 mxid_floor;		/* "PGXM" extension floor; 0 = absent (spec-7.1 D3-a) */
	uint32 mxid_disk_state; /* ClusterMxidDiskState (spec-7.1 integration review P0) */

	/* this node's region-4 publication (D5c; D5e reads the floor) */
	uint32 slot_state; /* ClusterXidStripeSlotState */
	bool my_slot_claimed;
	uint64 my_slot_floor_full;
	uint64 my_owner_incarnation;
	uint64 my_hwm_on_disk; /* soft HWM cache; never a durable allocation promise */

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

	/* Volatile maintenance intent, consumed only by the original QVOTEC tick.
	 * Sequence prevents completion of old IO from erasing a newer request. */
	bool maintenance_pending;
	uint64 maintenance_seq;
	uint64 maintenance_requested_full;
	ClusterSemanticActiveSampleFence maintenance_identity;
	ClusterXidStripeSampleFence maintenance_stripe;

	/*
	 * GCS-race round-3 P0-1: xid wrap barrier (native raw reuse).
	 * wrap_barrier_marked mirrors "NATIVE_RAW_REUSED is durably stamped in
	 * the shared authority"; wrap_barrier_done latches "every current
	 * member's native-prehistory latch is provably off" (own LMON ack round
	 * this boot, or the boot shortcut on an already-wrapped counter).  The
	 * allocation gate reads wrap_barrier_done lock-free before issuing any
	 * epoch>=1 candidate; ack_bitmap collects the DISABLE_ACK bits of the
	 * in-flight round (LMON-only writer).
	 */
	pg_atomic_uint32 wrap_barrier_marked;
	pg_atomic_uint32 wrap_barrier_done;
	pg_atomic_uint64 wrap_barrier_ack_bitmap;

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
static ClusterXidStripeLease *StripeLease = NULL;

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
	StripeLease = &StripeBootShmem->lease;

	if (!found) {
		memset(StripeBootShmem, 0, sizeof(ClusterXidStripeBootShmem));
		LWLockInitialize(&StripeBootShmem->lock, LWTRANCHE_CLUSTER_XID_STRIPE);
		pg_atomic_init_u64(&StripeLease->floor_full, 0);
		pg_atomic_init_u64(&StripeLease->limit_full, 0);
		pg_atomic_init_u64(&StripeLease->published_incarnation, 0);
		pg_atomic_init_u32(&StripeLease->failed, 0);
		ConditionVariableInit(&StripeLease->changed);
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
		pg_atomic_init_u32(&StripeBootShmem->wrap_barrier_marked, 0);
		pg_atomic_init_u32(&StripeBootShmem->wrap_barrier_done, 0);
		pg_atomic_init_u64(&StripeBootShmem->wrap_barrier_ack_bitmap, 0);
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

typedef enum StripeSlotReadPurpose {
	STRIPE_READ_EVIDENCE,
	STRIPE_READ_UPDATE,
	STRIPE_READ_REPAIR_CLAIM
} StripeSlotReadPurpose;

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
stripe_read_activation_one(int fd, ClusterXidStripeActivationRecord *out, uint32 *out_mxid_floor,
						   uint8 *out_mxid_ext_class)
{
	char slot[CLUSTER_VOTING_SLOT_BYTES];
	ClusterVotingDiskIoState rc;

	if (out_mxid_floor)
		*out_mxid_floor = 0;
	if (out_mxid_ext_class)
		*out_mxid_ext_class = (uint8)CLUSTER_MXID_EXT_ABSENT;

	rc = cluster_voting_disk_read_stripe_activation(fd, slot);
	if (rc != CLUSTER_VOTING_DISK_IO_OK) {
		struct stat st;

		/* EOF on a file that has not grown past region 4 yet is the
		 * lazily-materialised empty state, not an I/O failure. */
		if (fstat(fd, &st) == 0 && (off_t)st.st_size < CLUSTER_VOTING_PGSA_SLOT_OFFSET)
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
	 * same slot at a fixed offset.  Tri-state classification (spec-7.1
	 * integration review, P0): an all-zero extension area is ABSENT (a
	 * pre-extension binary wrote the slot); a validating record is
	 * VALID and carries the floor; anything else is CORRUPT -- the
	 * caller's evidence resolve decides whether the damage poisons the
	 * mxid face (never silently "absent": a lost floor must not reopen
	 * dense allocation).  Never a CORRUPT verdict for the xid face:
	 * the extension must not hold up the xid activation, which stands
	 * on its own CRC.
	 */
	if (out_mxid_floor && out_mxid_ext_class) {
		ClusterMxidStripeExtensionRecord ext;

		memcpy(&ext, slot + CLUSTER_PGXM_SLOT_OFFSET, sizeof(ext));
		if (cluster_mxid_stripe_extension_record_valid(&ext)) {
			*out_mxid_floor = ext.activated_mxid_floor;
			*out_mxid_ext_class = (uint8)CLUSTER_MXID_EXT_VALID;
		} else if (!buffer_is_all_zeros(slot + CLUSTER_PGXM_SLOT_OFFSET, sizeof(ext)))
			*out_mxid_ext_class = (uint8)CLUSTER_MXID_EXT_CORRUPT;
	}
	return STRIPE_READ_VALID;
}

/* Each prior boot occupies its own sector in this same voting authority.
 * There is no wrap/reuse and no dependence on native struct size for offsets. */
static bool
stripe_history_offset(int32 node, uint64 generation, off_t *out)
{
	uint64 base = (uint64)CLUSTER_VOTING_STRIPE_HISTORY_BASE;
	uint64 maximum = (PG_INT64_MAX - base - CLUSTER_VOTING_SLOT_BYTES) / CLUSTER_VOTING_SLOT_BYTES;

	if (node < 0 || node >= CLUSTER_XID_STRIDE || generation == 0
		|| generation - 1 > (maximum - node) / CLUSTER_XID_STRIDE)
		return false;
	*out = (off_t)(base
				   + ((generation - 1) * CLUSTER_XID_STRIDE + node) * CLUSTER_VOTING_SLOT_BYTES);
	return true;
}

static StripeSlotReadClass
stripe_history_read_one(int fd, int32 node, uint64 generation, ClusterXidStripeSlotRecord *out)
{
	uint8 bytes[CLUSTER_VOTING_SLOT_BYTES];
	ClusterVotingDiskRawReadState state;
	off_t offset;
	struct stat st;

	if (!stripe_history_offset(node, generation, &offset))
		return STRIPE_READ_CORRUPT;
	state = cluster_voting_disk_read_raw_slot_at(fd, offset, bytes);
	if (state == CLUSTER_VOTING_DISK_RAW_READ_CLEAN_EOF)
		return fstat(fd, &st) == 0 && S_ISREG(st.st_mode) && st.st_size <= offset
				   ? STRIPE_READ_ABSENT
				   : STRIPE_READ_UNREADABLE;
	if (state == CLUSTER_VOTING_DISK_RAW_READ_SHORT)
		return STRIPE_READ_CORRUPT;
	if (state != CLUSTER_VOTING_DISK_RAW_READ_FULL)
		return STRIPE_READ_UNREADABLE;
	if (buffer_is_all_zeros((char *)bytes, sizeof(bytes)))
		return STRIPE_READ_ABSENT;
	memcpy(out, bytes, sizeof(*out));
	if (!cluster_xid_stripe_slot_record_valid(out, node) || out->lease_incarnation == 0
		|| out->history_generation != generation - 1
		|| !buffer_is_all_zeros((char *)bytes + sizeof(*out), sizeof(bytes) - sizeof(*out)))
		return STRIPE_READ_CORRUPT;
	return STRIPE_READ_VALID;
}

/* Before the first reservation, prove that its future history sector exists
 * (a zero sector on a device) or can grow (EOF only on a regular file). */
static int
stripe_history_initial_capacity(const int *fds, int n_disks, int32 node)
{
	int available = 0;
	for (int d = 0; d < n_disks; d++) {
		ClusterXidStripeSlotRecord unused;
		StripeSlotReadClass state = stripe_history_read_one(fds[d], node, 1, &unused);

		if (state == STRIPE_READ_CORRUPT || state == STRIPE_READ_VALID)
			return -1;
		if (state == STRIPE_READ_ABSENT)
			available++;
	}
	return available >= n_disks / 2 + 1;
}

/* The current record binds the immutable predecessor's index and CRC. A
 * readable majority must agree (empty lagging copies are allowed); a matching
 * image supplies the bytes. Missing replicas never create a different history. */
static StripeSlotReadClass
stripe_history_read_all(const int *fds, int n_disks, const ClusterXidStripeSlotRecord *current,
						ClusterXidStripeSlotRecord *out)
{
	ClusterXidStripeSlotRecord chosen;
	bool found = false;
	int readable = 0;

	for (int d = 0; d < n_disks; d++) {
		ClusterXidStripeSlotRecord prior;
		StripeSlotReadClass result = stripe_history_read_one(fds[d], current->node_id,
															 current->history_generation, &prior);

		if (result == STRIPE_READ_CORRUPT)
			return result;
		if (result == STRIPE_READ_VALID) {
			if (prior.crc32c != current->history_crc32c
				|| prior.owner_incarnation != current->owner_incarnation
				|| prior.floor_full != current->floor_full
				|| prior.stride_mode_epoch != current->stride_mode_epoch
				|| prior.generation >= current->generation
				|| prior.lease_incarnation >= current->lease_incarnation
				|| prior.issued_limit_full > current->lease_floor_full
				|| (found && memcmp(&prior, &chosen, sizeof(prior)) != 0))
				return STRIPE_READ_CORRUPT;
			chosen = prior;
			found = true;
			readable++;
		} else if (result == STRIPE_READ_ABSENT)
			readable++;
	}
	if (!found || readable < n_disks / 2 + 1)
		return STRIPE_READ_UNREADABLE;
	*out = chosen;
	return STRIPE_READ_VALID;
}

/* 1 = durable, 0 = retry exact bytes, -1 = conflicting/corrupt evidence.
 * Read all destinations before writing; never overwrite a nonempty history. */
static int
stripe_history_archive(const int *fds, int n_disks, const ClusterXidStripeSlotRecord *record)
{
	StripeSlotReadClass states[CLUSTER_MAX_VOTING_DISKS];
	uint8 bytes[CLUSTER_VOTING_SLOT_BYTES] = { 0 };
	ClusterXidStripeSlotRecord seen;
	off_t offset;
	int readable = 0, durable = 0, source_durable = 0;

	if (record->history_generation == UINT64_MAX
		|| !stripe_history_offset(record->node_id, record->history_generation + 1, &offset))
		return -1;
	memcpy(bytes, record, sizeof(*record));
	for (int d = 0; d < n_disks; d++) {
		states[d] = stripe_history_read_one(fds[d], record->node_id, record->history_generation + 1,
											&seen);
		if (states[d] == STRIPE_READ_CORRUPT
			|| (states[d] == STRIPE_READ_VALID && memcmp(&seen, record, sizeof(seen)) != 0))
			return -1;
		if (states[d] == STRIPE_READ_VALID) {
			readable++;
		} else if (states[d] == STRIPE_READ_ABSENT)
			readable++;
	}
	if (readable < n_disks / 2 + 1)
		return 0;
	/* Tracking HWM updates need not have reached a majority. Persist this
	 * exact source first, so a restart cannot select an older source after
	 * losing its originally newest copy while the archive is already durable.
	 * The outer herding writer remains frozen until lease activation. */
	for (int d = 0; d < n_disks; d++)
		if (cluster_voting_disk_write_stripe_slot_ex(fds[d], record->node_id, bytes, true)
			== CLUSTER_VOTING_DISK_IO_OK)
			source_durable++;
	if (source_durable < n_disks / 2 + 1)
		return 0;
	for (int d = 0; d < n_disks; d++)
		/* A visible prior write is not a successful fsync receipt. Reissue
		 * only identical bytes and obtain a fresh durable majority. */
		if ((states[d] == STRIPE_READ_ABSENT || states[d] == STRIPE_READ_VALID)
			&& cluster_voting_disk_write_raw_slot_at(fds[d], offset, bytes)
				   == CLUSTER_VOTING_DISK_IO_OK)
			durable++;
	return durable >= n_disks / 2 + 1;
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
		off_t offset = CLUSTER_VOTING_STRIPE_SLOT_OFFSET(node);

		/* Only a regular file ending before this slot proves absence.
		 * A partial sector may contain a reservation; a device reporting
		 * st_size=0 does not turn an I/O failure into an empty slot. */
		if (fstat(fd, &st) == 0 && S_ISREG(st.st_mode)) {
			if (st.st_size <= offset)
				return STRIPE_READ_ABSENT;
			if (st.st_size < offset + CLUSTER_VOTING_SLOT_BYTES)
				return STRIPE_READ_CORRUPT;
		}
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
 * Read one region-4 slot across all disks.  A newer generation may advance
 * this claim's hwm or retire it; it cannot replace the claim identity or
 * resurrect a retired claim.  Equal generations must have identical payloads.
 * A nonempty unsupported/damaged copy is not proof of an absent reservation.
 * On VALID, *out holds the newest compatible record. Writers require a read
 * majority, intersecting every previously durable reservation; readers may
 * select evidence from fewer copies. Neither result is a durable write ACK.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static StripeSlotReadClass
stripe_read_slot_all(const int *fds, int n_disks, int32 node, ClusterXidStripeSlotRecord *out,
					 StripeSlotReadPurpose purpose)
{
	bool have_valid = false;
	bool have_readable = false;
	ClusterXidStripeSlotRecord seen[CLUSTER_MAX_VOTING_DISKS];
	int count = 0;
	int absent = 0;
	int best = 0;
	int i;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || fds == NULL || n_disks <= 0 || n_disks > CLUSTER_MAX_VOTING_DISKS)
		return STRIPE_READ_UNREADABLE;

	for (i = 0; i < n_disks; i++) {
		ClusterXidStripeSlotRecord rec;

		switch (stripe_read_slot_one(fds[i], node, &rec, NULL)) {
		case STRIPE_READ_VALID:
			have_readable = true;
			for (int j = 0; j < count; j++) {
				const ClusterXidStripeSlotRecord *older = &seen[j];
				const ClusterXidStripeSlotRecord *newer = &rec;

				if (rec.owner_incarnation != seen[j].owner_incarnation
					|| rec.floor_full != seen[j].floor_full
					|| rec.stride_mode_epoch != seen[j].stride_mode_epoch)
					return STRIPE_READ_CORRUPT;
				if (rec.generation == seen[j].generation
					&& memcmp(&rec, &seen[j], offsetof(ClusterXidStripeSlotRecord, crc32c)) != 0)
					return STRIPE_READ_CORRUPT;
				if (rec.generation < seen[j].generation) {
					older = &rec;
					newer = &seen[j];
				}
				if (newer->next_xid_hwm_full < older->next_xid_hwm_full
					|| newer->retired < older->retired
					|| newer->history_generation < older->history_generation
					|| newer->issued_limit_full < older->issued_limit_full)
					return STRIPE_READ_CORRUPT;
				if (newer->lease_incarnation == older->lease_incarnation
						? (newer->lease_floor_full != older->lease_floor_full
						   || newer->history_generation != older->history_generation)
						: (newer->lease_floor_full < older->issued_limit_full
						   || (older->lease_incarnation != 0
							   && (newer->lease_incarnation <= older->lease_incarnation
								   || newer->history_generation <= older->history_generation))))
					return STRIPE_READ_CORRUPT;
			}
			seen[count] = rec;
			if (!have_valid || rec.generation > seen[best].generation)
				best = count;
			count++;
			have_valid = true;
			break;
		case STRIPE_READ_ABSENT:
			have_readable = true;
			absent++;
			break;
		case STRIPE_READ_CORRUPT:
			return STRIPE_READ_CORRUPT;
		case STRIPE_READ_UNREADABLE:
			break;
		}
	}

	if (have_valid) {
		if (seen[best].history_generation != 0) {
			ClusterXidStripeSlotRecord prior;
			StripeSlotReadClass history
				= stripe_history_read_all(fds, n_disks, &seen[best], &prior);

			if (history != STRIPE_READ_VALID)
				return history;
		}
		if (purpose == STRIPE_READ_REPAIR_CLAIM) {
			/* Only a never-issued first claim can be copied into proven
			 * empty peers. This grants no range and preserves its exact bytes. */
			const ClusterXidStripeSlotRecord *claim = &seen[best];

			if (count + absent < n_disks / 2 + 1 || claim->generation != 1 || claim->retired
				|| claim->next_xid_hwm_full != claim->floor_full || claim->issued_limit_full != 0
				|| claim->lease_incarnation != 0 || claim->lease_floor_full != 0)
				return STRIPE_READ_UNREADABLE;
		} else if (purpose == STRIPE_READ_UPDATE && count < n_disks / 2 + 1)
			return STRIPE_READ_UNREADABLE;
		*out = seen[best];
		return STRIPE_READ_VALID;
	}
	if (have_readable && (purpose == STRIPE_READ_EVIDENCE || absent >= n_disks / 2 + 1))
		return STRIPE_READ_ABSENT;
	return STRIPE_READ_UNREADABLE;
}

bool
cluster_xid_stripe_lookup_incarnation_fds(const int *fds, int n_disks, FullTransactionId xid,
										  uint64 *out_incarnation)
{
	ClusterXidStripeSlotRecord record, previous;
	uint64 value = U64FromFullTransactionId(xid);
	int node = value % CLUSTER_XID_STRIDE;

	if (out_incarnation == NULL || n_disks <= 0 || n_disks > CLUSTER_MAX_VOTING_DISKS
		|| (n_disks % 2) == 0 || !TransactionIdIsNormal(XidFromFullTransactionId(xid))
		|| stripe_read_slot_all(fds, n_disks, node, &record, STRIPE_READ_UPDATE)
			   != STRIPE_READ_VALID)
		return false;
	for (;;) {
		if (record.lease_incarnation != 0 && value >= record.lease_floor_full
			&& value < record.issued_limit_full) {
			*out_incarnation = record.lease_incarnation;
			return true;
		}
		if (record.history_generation == 0 || value >= record.issued_limit_full
			|| stripe_history_read_all(fds, n_disks, &record, &previous) != STRIPE_READ_VALID)
			return false;
		record = previous;
	}
}

static void
stripe_lease_fail(void)
{
	pg_atomic_write_u32(&StripeLease->failed, 1);
	pg_atomic_write_u64(&StripeLease->limit_full, 0);
	ConditionVariableBroadcast(&StripeLease->changed);
}

bool
cluster_xid_stripe_lease_ready(FullTransactionId candidate)
{
	uint64 limit, floor, value = U64FromFullTransactionId(candidate);

	if (!cluster_enabled || !cluster_shared_catalog || !cluster_xid_striping)
		return true;
	if (StripeLease == NULL || pg_atomic_read_u32(&StripeLease->failed) != 0
		|| !cluster_qvotec_in_quorum())
		return false;
	limit = pg_atomic_read_u64(&StripeLease->limit_full);
	pg_read_barrier();
	floor = pg_atomic_read_u64(&StripeLease->floor_full);
	return floor != 0 && value >= floor && value < limit;
}

/*
 * Recheck the original promise after a bounded census slice. Lease extensions
 * keep the same incarnation/floor and contain the captured interval; they do
 * not force an otherwise conservative sample to restart under allocation load.
 * A changed activation, claim, publisher or revoked interval does. This is
 * only the stripe component of the eventual full admission/cohort fence.
 */
bool
cluster_xid_stripe_sample_fence_current(const ClusterXidStripeSampleFence *fence)
{
	uint64 limit;
	bool current;

	if (fence == NULL || !cluster_enabled || !cluster_shared_catalog || !cluster_xid_striping
		|| StripeBootShmem == NULL || StripeLease == NULL || fence->activation_floor_full == 0
		|| fence->activation_epoch == 0 || fence->activation_generation == 0
		|| fence->slot_floor_full == 0 || fence->slot_owner_incarnation == 0
		|| fence->incarnation == 0 || fence->lease_floor_full == 0
		|| fence->lease_limit_full <= fence->lease_floor_full || fence->slot < 0
		|| fence->slot >= CLUSTER_XID_STRIDE || cluster_xid_allocation_slot() != fence->slot
		|| cluster_qvotec_get_self_incarnation() != fence->incarnation
		|| !cluster_qvotec_in_quorum())
		return false;

	LWLockAcquire(&StripeBootShmem->lock, LW_SHARED);
	current = StripeBootShmem->disk_state == CLUSTER_XID_STRIPE_DISK_PUBLISHED
			  && StripeBootShmem->floor_full == fence->activation_floor_full
			  && StripeBootShmem->epoch == fence->activation_epoch
			  && StripeBootShmem->generation == fence->activation_generation
			  && StripeBootShmem->slot_state == CLUSTER_XID_STRIPE_SLOT_MINE
			  && StripeBootShmem->my_slot_claimed
			  && StripeBootShmem->my_slot_floor_full == fence->slot_floor_full
			  && StripeBootShmem->my_owner_incarnation == fence->slot_owner_incarnation;
	limit = pg_atomic_read_u64(&StripeLease->limit_full);
	pg_read_barrier();
	current = current && limit >= fence->lease_limit_full
			  && pg_atomic_read_u64(&StripeLease->floor_full) == fence->lease_floor_full
			  && pg_atomic_read_u64(&StripeLease->published_incarnation) == fence->incarnation
			  && pg_atomic_read_u32(&StripeLease->failed) == 0;
	pg_read_barrier();
	current = current && pg_atomic_read_u64(&StripeLease->limit_full) >= fence->lease_limit_full
			  && pg_atomic_read_u64(&StripeLease->floor_full) == fence->lease_floor_full
			  && pg_atomic_read_u64(&StripeLease->published_incarnation) == fence->incarnation
			  && pg_atomic_read_u32(&StripeLease->failed) == 0;
	LWLockRelease(&StripeBootShmem->lock);
	return current && cluster_qvotec_get_self_incarnation() == fence->incarnation
		   && cluster_qvotec_in_quorum();
}

/*
 * Read the existing allocation promises at the same XidGenLock cut as N.
 * This neither allocates nor advances a promise. Claim ownership persists
 * across boots; only the durable lease publisher must match this incarnation.
 * The returned lower bound is an input, not a complete freeze certificate.
 */
bool
cluster_xid_stripe_sample_future(FullTransactionId reference, FullTransactionId *future,
								 ClusterXidStripeSampleFence *fence)
{
	ClusterXidStripeSampleFence captured = { 0 };
	uint64 incarnation, floor, limit, base, value;
	bool claimed;
	int slot;
	FullTransactionId candidate;

	if (future != NULL)
		*future = InvalidFullTransactionId;
	if (fence != NULL)
		MemSet(fence, 0, sizeof(*fence));
	if (future == NULL || fence == NULL || !LWLockHeldByMe(XidGenLock) || !cluster_enabled
		|| !cluster_shared_catalog || !cluster_xid_striping || StripeBootShmem == NULL
		|| StripeLease == NULL || !FullTransactionIdIsValid(reference)
		|| !TransactionIdIsNormal(XidFromFullTransactionId(reference))
		|| !cluster_qvotec_in_quorum())
		return false;
	slot = cluster_xid_allocation_slot();
	incarnation = cluster_qvotec_get_self_incarnation();
	if (slot < 0 || slot >= CLUSTER_XID_STRIDE || incarnation == 0)
		return false;

	LWLockAcquire(&StripeBootShmem->lock, LW_SHARED);
	claimed = StripeBootShmem->disk_state == CLUSTER_XID_STRIPE_DISK_PUBLISHED
			  && StripeBootShmem->floor_full != 0 && StripeBootShmem->epoch != 0
			  && StripeBootShmem->generation != 0
			  && StripeBootShmem->slot_state == CLUSTER_XID_STRIPE_SLOT_MINE
			  && StripeBootShmem->my_slot_claimed && StripeBootShmem->my_slot_floor_full != 0
			  && StripeBootShmem->my_owner_incarnation != 0;
	base = Max(U64FromFullTransactionId(reference), StripeBootShmem->floor_full);
	base = Max(base, StripeBootShmem->my_slot_floor_full);
	captured.activation_floor_full = StripeBootShmem->floor_full;
	captured.activation_epoch = StripeBootShmem->epoch;
	captured.activation_generation = StripeBootShmem->generation;
	captured.slot_floor_full = StripeBootShmem->my_slot_floor_full;
	captured.slot_owner_incarnation = StripeBootShmem->my_owner_incarnation;
	LWLockRelease(&StripeBootShmem->lock);
	if (!claimed)
		return false;

	/* QVOTEC publishes floor/identity before limit, after majority durability.
	 * A fresh publisher revokes the old limit before changing that floor. */
	limit = pg_atomic_read_u64(&StripeLease->limit_full);
	pg_read_barrier();
	floor = pg_atomic_read_u64(&StripeLease->floor_full);
	if (floor == 0 || limit <= floor
		|| pg_atomic_read_u64(&StripeLease->published_incarnation) != incarnation)
		return false;
	base = Max(base, floor);
	base = Max(base, pg_atomic_read_u64(&StripeBootShmem->herding_floor_full));
	candidate = cluster_xid_next_striped_full(FullTransactionIdFromU64(base), slot);
	value = U64FromFullTransactionId(candidate);
	if (value < base || !FullTransactionIdIsValid(candidate)
		|| !TransactionIdIsNormal(XidFromFullTransactionId(candidate)) || value >= limit
		|| cluster_xid_stripe_window_exceeded(candidate)
		|| (EpochFromFullTransactionId(candidate) > 0 && !cluster_xid_wrap_barrier_passed()))
		return false;

	/* Reject changes observed during this read, then retain the same start
	 * identity for the worker's later completion check. */
	pg_read_barrier();
	if (pg_atomic_read_u32(&StripeLease->failed) != 0
		|| pg_atomic_read_u64(&StripeLease->limit_full) != limit
		|| pg_atomic_read_u64(&StripeLease->floor_full) != floor
		|| pg_atomic_read_u64(&StripeLease->published_incarnation) != incarnation
		|| cluster_qvotec_get_self_incarnation() != incarnation || !cluster_qvotec_in_quorum())
		return false;
	captured.incarnation = incarnation;
	captured.slot = slot;
	captured.lease_floor_full = floor;
	captured.lease_limit_full = limit;
	if (!cluster_xid_stripe_sample_fence_current(&captured))
		return false;
	*future = candidate;
	*fence = captured;
	return true;
}

/* Only a complete current report set supplies the maintenance ceiling. The
 * request field on the wire is an intent, never authority to burn a range. */
static ClusterXidStripeMaintenanceResult
stripe_maintenance_validate(const ClusterSemanticActiveSampleFence *identity,
							const ClusterXidStripeSampleFence *stripe, FullTransactionId requested)
{
	ClusterHeapFreezeCohortObservation cohort;
	ClusterHeapFreezeCohortStatus status;
	uint64 value = U64FromFullTransactionId(requested);
	uint64 next;

	if (identity == NULL || stripe == NULL || StripeBootShmem == NULL || StripeLease == NULL
		|| !cluster_enabled || !cluster_shared_catalog || !cluster_xid_striping
		|| LWLockHeldByMe(XidGenLock) || pg_atomic_read_u32(&StripeLease->failed) != 0
		|| cluster_xid_herding_slack < 0 || !FullTransactionIdIsValid(requested)
		|| !TransactionIdIsNormal(XidFromFullTransactionId(requested))
		|| value > UINT64_MAX - CLUSTER_XID_STRIPE_LEASE_SPAN - CLUSTER_XID_STRIDE)
		return CLUSTER_XID_STRIPE_MAINTENANCE_UNAVAILABLE;
	if (!cluster_semantic_activation_active_sample_current(identity)
		|| !cluster_xid_stripe_sample_fence_current(stripe)
		|| identity->local_node_id != stripe->slot
		|| identity->local_boot_incarnation != stripe->incarnation)
		return CLUSTER_XID_STRIPE_MAINTENANCE_IDENTITY_CHANGED;
	if (value % CLUSTER_XID_STRIDE != (uint64)stripe->slot
		|| cluster_xid_stripe_window_exceeded(requested)
		|| (EpochFromFullTransactionId(requested) > 0 && !cluster_xid_wrap_barrier_passed()))
		return CLUSTER_XID_STRIPE_MAINTENANCE_UNAVAILABLE;
	next = U64FromFullTransactionId(ReadNextFullTransactionId());
	if (value > next && value - next > (uint64)cluster_xid_herding_slack)
		return CLUSTER_XID_STRIPE_MAINTENANCE_UNAVAILABLE;
	status = cluster_heap_freeze_cohort_observe(&cohort);
	if (status == CLUSTER_HEAP_FREEZE_COHORT_IDENTITY_CHANGED)
		return CLUSTER_XID_STRIPE_MAINTENANCE_IDENTITY_CHANGED;
	if (status != CLUSTER_HEAP_FREEZE_COHORT_READY)
		return CLUSTER_XID_STRIPE_MAINTENANCE_UNAVAILABLE;
	if (cohort.record_generation != identity->record_generation
		|| cohort.formation_epoch != identity->formation_epoch
		|| cohort.admitted_members_lo != identity->admitted_members_lo
		|| cohort.admitted_members_hi != identity->admitted_members_hi
		|| !cluster_semantic_activation_active_sample_current(identity)
		|| !cluster_xid_stripe_sample_fence_current(stripe))
		return CLUSTER_XID_STRIPE_MAINTENANCE_IDENTITY_CHANGED;
	if (value > cohort.future_full_xid_max)
		return CLUSTER_XID_STRIPE_MAINTENANCE_UNAVAILABLE;
	return CLUSTER_XID_STRIPE_MAINTENANCE_PENDING;
}

ClusterXidStripeMaintenanceResult
cluster_xid_stripe_request_maintenance(const ClusterSemanticActiveSampleFence *identity,
									   const ClusterXidStripeSampleFence *stripe,
									   FullTransactionId requested)
{
	ClusterXidStripeMaintenanceResult result;
	uint64 value = U64FromFullTransactionId(requested);
	bool same;

	result = stripe_maintenance_validate(identity, stripe, requested);
	if (result != CLUSTER_XID_STRIPE_MAINTENANCE_PENDING)
		return result;
	LWLockAcquire(&StripeBootShmem->lock, LW_EXCLUSIVE);
	/* Only the armed original promise counts as DURABLE, never tracking HWM. */
	if (pg_atomic_read_u64(&StripeBootShmem->herding_floor_full) >= value
		|| pg_atomic_read_u64(&StripeLease->floor_full) >= value) {
		result = CLUSTER_XID_STRIPE_MAINTENANCE_DURABLE;
		goto release;
	}
	same = StripeBootShmem->maintenance_pending
		   && memcmp(&StripeBootShmem->maintenance_identity, identity, sizeof(*identity)) == 0
		   && StripeBootShmem->maintenance_stripe.activation_floor_full
				  == stripe->activation_floor_full
		   && StripeBootShmem->maintenance_stripe.activation_epoch == stripe->activation_epoch
		   && StripeBootShmem->maintenance_stripe.activation_generation
				  == stripe->activation_generation
		   && StripeBootShmem->maintenance_stripe.slot_floor_full == stripe->slot_floor_full
		   && StripeBootShmem->maintenance_stripe.slot_owner_incarnation
				  == stripe->slot_owner_incarnation
		   && StripeBootShmem->maintenance_stripe.incarnation == stripe->incarnation
		   && StripeBootShmem->maintenance_stripe.lease_floor_full == stripe->lease_floor_full
		   && StripeBootShmem->maintenance_stripe.slot == stripe->slot;
	if (same && StripeBootShmem->maintenance_requested_full >= value) {
		goto release;
	}
	if (StripeBootShmem->maintenance_seq == UINT64_MAX) {
		result = CLUSTER_XID_STRIPE_MAINTENANCE_UNAVAILABLE;
		goto release;
	}
	StripeBootShmem->maintenance_identity = *identity;
	StripeBootShmem->maintenance_stripe = *stripe;
	StripeBootShmem->maintenance_requested_full = value;
	StripeBootShmem->maintenance_seq++;
	StripeBootShmem->maintenance_pending = true;
release:
	LWLockRelease(&StripeBootShmem->lock);
	if (result != CLUSTER_XID_STRIPE_MAINTENANCE_UNAVAILABLE
		&& (!cluster_semantic_activation_active_sample_current(identity)
			|| !cluster_xid_stripe_sample_fence_current(stripe)))
		return CLUSTER_XID_STRIPE_MAINTENANCE_IDENTITY_CHANGED;
	return result;
}

static void
stripe_maintenance_retire(uint64 seq)
{
	LWLockAcquire(&StripeBootShmem->lock, LW_EXCLUSIVE);
	if (StripeBootShmem->maintenance_seq == seq)
		StripeBootShmem->maintenance_pending = false;
	LWLockRelease(&StripeBootShmem->lock);
}

/* Sole writer: the existing QVOTEC poll. A failed partial write retains the
 * exact proposed interval; a new process burns it instead of reissuing it.
 * The caller supplies its latest native/herding minimum, never a grant.
 * Author: SqlRush <sqlrush@gmail.com> */
static void
stripe_lease_tick(const int *fds, int n_disks, uint64 minimum)
{
	static uint64 pending_incarnation, pending_floor, pending_limit;
	ClusterXidStripeSlotRecord rec;
	StripeSlotReadClass state;
	uint64 incarnation, limit, base;
	char bytes[CLUSTER_VOTING_SLOT_BYTES] = { 0 };
	int written = 0;
	bool fresh;

	if (!cluster_enabled || !cluster_shared_catalog || !cluster_xid_striping || StripeLease == NULL
		|| pg_atomic_read_u32(&StripeLease->failed) != 0 || !cluster_qvotec_in_quorum())
		return;
	incarnation = cluster_qvotec_self_incarnation_value();
	if (incarnation == 0)
		return;
	fresh = StripeLease->publisher_incarnation != incarnation;
	limit = pg_atomic_read_u64(&StripeLease->limit_full);
	if (fresh) {
		pg_atomic_write_u64(&StripeLease->limit_full, 0);
		pg_write_barrier();
	}
	state = stripe_read_slot_all(fds, n_disks, cluster_node_id, &rec, STRIPE_READ_UPDATE);
	if (state == STRIPE_READ_UNREADABLE) {
		/* A partial first CLAIM may already have published MINE. Complete
		 * its exact original record before the next poll can reserve XIDs. */
		if (StripeLease->publisher_incarnation == 0 && StripeLease->claim_owner == 0
			&& stripe_read_slot_all(fds, n_disks, cluster_node_id, &rec, STRIPE_READ_REPAIR_CLAIM)
				   == STRIPE_READ_VALID) {
			memcpy(bytes, &rec, sizeof(rec));
			for (int i = 0; i < n_disks; i++)
				(void)cluster_voting_disk_write_stripe_slot(fds[i], cluster_node_id, bytes);
		}
		return;
	}
	if (state != STRIPE_READ_VALID || rec.retired || rec.generation == UINT64_MAX
		|| (!fresh
			&& (rec.lease_incarnation != incarnation
				|| rec.lease_floor_full != pg_atomic_read_u64(&StripeLease->floor_full)
				|| rec.issued_limit_full < limit))
		|| (StripeLease->claim_owner != 0
			&& (rec.owner_incarnation != StripeLease->claim_owner
				|| rec.floor_full != StripeLease->claim_floor
				|| rec.stride_mode_epoch != StripeLease->claim_epoch))) {
		stripe_lease_fail();
		return;
	}
	StripeLease->claim_owner = rec.owner_incarnation;
	StripeLease->claim_floor = rec.floor_full;
	StripeLease->claim_epoch = rec.stride_mode_epoch;
	/* Check authority even while the already durable range has space. */
	if (!fresh && limit > CLUSTER_XID_STRIPE_LEASE_SPAN / 2
		&& minimum < limit - CLUSTER_XID_STRIPE_LEASE_SPAN / 2)
		return;
	if (pending_incarnation != incarnation) {
		pending_incarnation = incarnation;
		pending_limit = 0;
	}
	if (pending_limit == 0) {
		base = Max(minimum, rec.next_xid_hwm_full);
		base = Max(base, rec.issued_limit_full);
		if (base > UINT64_MAX - CLUSTER_XID_STRIPE_LEASE_SPAN - CLUSTER_XID_STRIDE) {
			stripe_lease_fail();
			return;
		}
		base = U64FromFullTransactionId(
			cluster_xid_next_striped_full(FullTransactionIdFromU64(base), cluster_node_id));
		pending_floor = fresh ? base : rec.lease_floor_full;
		pending_limit = base + CLUSTER_XID_STRIPE_LEASE_SPAN;
	}
	if (pending_limit < rec.issued_limit_full
		|| (rec.lease_incarnation == incarnation && rec.lease_floor_full != pending_floor)) {
		stripe_lease_fail();
		return;
	}
	if (rec.lease_incarnation != incarnation || rec.issued_limit_full != pending_limit) {
		if (rec.lease_incarnation == 0) {
			int capacity = stripe_history_initial_capacity(fds, n_disks, rec.node_id);

			if (capacity < 0)
				stripe_lease_fail();
			if (capacity <= 0)
				return;
		}
		if (rec.lease_incarnation != 0 && rec.lease_incarnation != incarnation) {
			int archived;

			if (incarnation <= rec.lease_incarnation) {
				stripe_lease_fail();
				return;
			}
			archived = stripe_history_archive(fds, n_disks, &rec);
			if (archived < 0)
				stripe_lease_fail();
			if (archived <= 0)
				return;
			rec.history_crc32c = rec.crc32c;
			rec.history_generation++;
		}
		rec.lease_incarnation = incarnation;
		rec.lease_floor_full = pending_floor;
		rec.issued_limit_full = pending_limit;
		rec.generation++;
		cluster_xid_stripe_slot_record_compute_crc(&rec);
	}
	memcpy(bytes, &rec, sizeof(rec));
	for (int i = 0; i < n_disks; i++)
		if (cluster_voting_disk_write_stripe_slot(fds[i], cluster_node_id, bytes)
			== CLUSTER_VOTING_DISK_IO_OK)
			written++;
	if (written < n_disks / 2 + 1)
		return;
	pg_atomic_write_u64(&StripeLease->floor_full, pending_floor);
	pg_atomic_write_u64(&StripeLease->published_incarnation, incarnation);
	pg_write_barrier();
	pg_atomic_write_u64(&StripeLease->limit_full, pending_limit);
	StripeLease->publisher_incarnation = incarnation;
	pending_limit = 0;
	ConditionVariableBroadcast(&StripeLease->changed);
}

/* Original QVOTEC is the sole durable consumer. No native/content/census or
 * semantic lock spans IO. A failed proof retains intent; identity change retires
 * only this sequence. Bytes already written cannot be undone, and an armed
 * floor never rolls back. None of these observations publishes global F. */
static bool
stripe_maintenance_tick(const int *fds, int n_disks)
{
	ClusterSemanticActiveSampleFence identity;
	ClusterXidStripeSampleFence stripe;
	ClusterXidStripeMaintenanceResult result;
	ClusterXidStripeSlotRecord rec;
	uint64 seq, requested;
	char bytes[CLUSTER_VOTING_SLOT_BYTES] = { 0 };
	int written = 0;

	LWLockAcquire(&StripeBootShmem->lock, LW_SHARED);
	if (!StripeBootShmem->maintenance_pending) {
		LWLockRelease(&StripeBootShmem->lock);
		return true;
	}
	identity = StripeBootShmem->maintenance_identity;
	stripe = StripeBootShmem->maintenance_stripe;
	seq = StripeBootShmem->maintenance_seq;
	requested = StripeBootShmem->maintenance_requested_full;
	LWLockRelease(&StripeBootShmem->lock);
	if (n_disks > CLUSTER_MAX_VOTING_DISKS || n_disks % 2 == 0
		|| cluster_qvotec_self_incarnation_value() == 0)
		return false;
	result = stripe_maintenance_validate(&identity, &stripe, FullTransactionIdFromU64(requested));
	if (result != CLUSTER_XID_STRIPE_MAINTENANCE_PENDING) {
		if (result == CLUSTER_XID_STRIPE_MAINTENANCE_IDENTITY_CHANGED)
			stripe_maintenance_retire(seq);
		return false;
	}
	if (U64FromFullTransactionId(cluster_xid_stripe_herding_floor()) >= requested) {
		stripe_maintenance_retire(seq);
		return true;
	}
	/* A bounded request may need a larger issued interval. Keep the original
	 * lease/history writer and its meaning; only validated intent reaches it. */
	if (!cluster_xid_stripe_lease_ready(FullTransactionIdFromU64(requested)))
		stripe_lease_tick(fds, n_disks, requested);
	if (!cluster_xid_stripe_sample_fence_current(&stripe)
		|| !cluster_xid_stripe_lease_ready(FullTransactionIdFromU64(requested)))
		return false;
	if (stripe_read_slot_all(fds, n_disks, cluster_node_id, &rec, STRIPE_READ_UPDATE)
			!= STRIPE_READ_VALID
		|| rec.retired || rec.generation == UINT64_MAX
		|| rec.owner_incarnation != stripe.slot_owner_incarnation
		|| rec.floor_full != stripe.slot_floor_full
		|| rec.stride_mode_epoch != stripe.activation_epoch
		|| rec.lease_incarnation != stripe.incarnation
		|| rec.lease_floor_full != stripe.lease_floor_full || rec.issued_limit_full <= requested
		|| rec.issued_limit_full < pg_atomic_read_u64(&StripeLease->limit_full))
		return false;
	rec.next_xid_hwm_full = Max(rec.next_xid_hwm_full, requested);
	rec.generation++;
	cluster_xid_stripe_slot_record_compute_crc(&rec);
	memcpy(bytes, &rec, sizeof(rec));
	/* Even an equal/larger TRACKING HWM is not a durable promise. */
	for (int i = 0; i < n_disks; i++)
		if (cluster_voting_disk_write_stripe_slot_ex(fds[i], cluster_node_id, bytes, true)
			== CLUSTER_VOTING_DISK_IO_OK)
			written++;

	LWLockAcquire(&StripeBootShmem->lock, LW_EXCLUSIVE);
	/* Keep ordinary herding from adopting this attempted image as its own
	 * JUMP after a proof refusal. This soft cache grants nothing; pending
	 * maintenance still retries synchronous writes even when HWM covers it. */
	if (StripeBootShmem->my_slot_claimed
		&& StripeBootShmem->my_owner_incarnation == stripe.slot_owner_incarnation
		&& StripeBootShmem->my_slot_floor_full == stripe.slot_floor_full)
		StripeBootShmem->my_hwm_on_disk
			= Max(StripeBootShmem->my_hwm_on_disk, rec.next_xid_hwm_full);
	LWLockRelease(&StripeBootShmem->lock);
	if (written < n_disks / 2 + 1)
		return false;
	result = stripe_maintenance_validate(&identity, &stripe, FullTransactionIdFromU64(requested));
	if (result != CLUSTER_XID_STRIPE_MAINTENANCE_PENDING) {
		if (result == CLUSTER_XID_STRIPE_MAINTENANCE_IDENTITY_CHANGED)
			stripe_maintenance_retire(seq);
		return false;
	}
	LWLockAcquire(&StripeBootShmem->lock, LW_EXCLUSIVE);
	/* A replacement identity may have staged during IO. Never retire it or
	 * publish the old request on its behalf. Same-identity larger intent can
	 * coexist with this completed prefix. */
	if (memcmp(&StripeBootShmem->maintenance_identity, &identity, sizeof(identity)) != 0
		|| pg_atomic_read_u32(&StripeLease->failed) != 0
		|| pg_atomic_read_u64(&StripeLease->published_incarnation) != stripe.incarnation
		|| pg_atomic_read_u64(&StripeLease->floor_full) != stripe.lease_floor_full
		|| pg_atomic_read_u64(&StripeLease->limit_full) <= requested
		|| cluster_qvotec_get_self_incarnation() != stripe.incarnation
		|| !cluster_qvotec_in_quorum()) {
		LWLockRelease(&StripeBootShmem->lock);
		return false;
	}
	if (requested > pg_atomic_read_u64(&StripeBootShmem->herding_floor_full))
		pg_atomic_write_u64(&StripeBootShmem->herding_floor_full, requested);
	if (StripeBootShmem->maintenance_seq == seq)
		StripeBootShmem->maintenance_pending = false;
	LWLockRelease(&StripeBootShmem->lock);
	ConditionVariableBroadcast(&StripeLease->changed);
	return true;
}

void
cluster_xid_stripe_wait_lease(FullTransactionId candidate)
{
	if (StripeLease == NULL)
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
						errmsg("cluster xid reservation authority is unavailable")));
	ConditionVariablePrepareToSleep(&StripeLease->changed);
	PG_TRY();
	{
		for (;;) {
			uint64 limit, floor;

			CHECK_FOR_INTERRUPTS();
			if (IsUnderPostmaster && !PostmasterIsAlive())
				ereport(FATAL,
						(errmsg("postmaster exited while waiting for a cluster xid reservation")));
			if (pg_atomic_read_u32(&StripeLease->failed) != 0 || !cluster_qvotec_in_quorum())
				ereport(ERROR, (errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
								errmsg("cluster xid reservation authority is unavailable")));
			limit = pg_atomic_read_u64(&StripeLease->limit_full);
			pg_read_barrier();
			floor = pg_atomic_read_u64(&StripeLease->floor_full);
			/* A candidate below a new boot's floor must be rederived too. */
			if (limit > floor && U64FromFullTransactionId(candidate) < limit)
				break;
			(void)ConditionVariableTimedSleep(&StripeLease->changed, 1000, PG_WAIT_EXTENSION);
		}
	}
	PG_FINALLY();
	{
		ConditionVariableCancelSleep();
	}
	PG_END_TRY();
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
	ClusterMxidFloorEvidence mxid_ev[CLUSTER_MAX_VOTING_DISKS];
	int mxid_ev_n = 0;
	ClusterMxidFloorResolve mxid_resolve;
	uint32 mxid_resolved_floor = 0;
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
		uint8 rec_mxid_ext_class;

		switch (stripe_read_activation_one(fds[i], &rec, &rec_mxid_floor, &rec_mxid_ext_class)) {
		case STRIPE_READ_VALID:
			have_readable = true;
			if (!have_valid || rec.generation > best.generation) {
				best = rec;
				have_valid = true;
			}
			/* mxid evidence: EVERY valid-PGXA copy contributes (spec-7.1
			 * integration review P0) — the resolve below decides, not
			 * "the winner's copy" (a winner with a damaged extension
			 * must not shadow a coherent sibling, and a lost extension
			 * must not silently reopen dense allocation). */
			if (mxid_ev_n < CLUSTER_MAX_VOTING_DISKS) {
				mxid_ev[mxid_ev_n].generation = rec.generation;
				mxid_ev[mxid_ev_n].ext_class = rec_mxid_ext_class;
				mxid_ev[mxid_ev_n].floor = rec_mxid_floor;
				mxid_ev_n++;
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

	mxid_resolve = cluster_mxid_stripe_resolve_floor(mxid_ev, mxid_ev_n, &mxid_resolved_floor);

	LWLockAcquire(&StripeBootShmem->lock, LW_EXCLUSIVE);

	/*
	 * mxid face publication (spec-7.1 integration review P0) — decided by
	 * the cross-copy evidence resolve, independent of which copy wins the
	 * xid face below, with sticky/monotonic rules: POISONED never clears,
	 * a PUBLISHED floor never regresses to absent (dense) or moves.
	 */
	switch (StripeBootShmem->mxid_disk_state) {
	case CLUSTER_MXID_DISK_POISONED:
		break; /* sticky */
	case CLUSTER_MXID_DISK_PUBLISHED:
		if (mxid_resolve == CLUSTER_MXID_FLOOR_VALID
			&& mxid_resolved_floor == StripeBootShmem->mxid_floor)
			break; /* steady state */
		if (mxid_resolve == CLUSTER_MXID_FLOOR_ABSENT) {
			/* Incomplete evidence this scan (e.g. the carrying disk was
			 * unreadable): keep the published floor — never regress the
			 * mxid face to absent/dense — and say so. */
			ereport(LOG, (errmsg("cluster mxid stripe: activation extension unreadable this "
								 "scan; keeping published floor %u",
								 (unsigned)StripeBootShmem->mxid_floor)));
			break;
		}
		StripeBootShmem->mxid_disk_state = CLUSTER_MXID_DISK_POISONED;
		ereport(WARNING,
				(errmsg("cluster mxid stripe: conflicting \"PGXM\" activation evidence "
						"(published floor %u, resolve %d floor %u); mxid face poisoned",
						(unsigned)StripeBootShmem->mxid_floor, (int)mxid_resolve,
						(unsigned)mxid_resolved_floor),
				 errdetail("New multixact allocation on this node will be refused and foreign "
						   "multixact reads stay fail-closed until the voting-disk activation "
						   "records are repaired.")));
		break;
	default: /* UNKNOWN / ABSENT */
		if (mxid_resolve == CLUSTER_MXID_FLOOR_VALID) {
			StripeBootShmem->mxid_disk_state = CLUSTER_MXID_DISK_PUBLISHED;
			StripeBootShmem->mxid_floor = mxid_resolved_floor;
		} else if (mxid_resolve == CLUSTER_MXID_FLOOR_POISONED) {
			StripeBootShmem->mxid_disk_state = CLUSTER_MXID_DISK_POISONED;
			StripeBootShmem->mxid_floor = 0;
			ereport(WARNING,
					(errmsg("cluster mxid stripe: conflicting or corrupt \"PGXM\" activation "
							"evidence across voting disks; mxid face poisoned"),
					 errdetail("New multixact allocation on this node will be refused and "
							   "foreign multixact reads stay fail-closed until the voting-disk "
							   "activation records are repaired.")));
		} else if (StripeBootShmem->mxid_disk_state == CLUSTER_MXID_DISK_UNKNOWN && have_readable)
			StripeBootShmem->mxid_disk_state = CLUSTER_MXID_DISK_ABSENT;
		break;
	}

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

	/* D5c: THIS node's region-4 slot verdict (striping-on nodes only). */
	if (cluster_xid_striping && cluster_node_id >= 0 && cluster_node_id < CLUSTER_XID_STRIDE) {
		ClusterXidStripeSlotRecord mine;

		memset(&mine, 0, sizeof(mine));
		switch (stripe_read_slot_all(fds, n_disks, cluster_node_id, &mine, STRIPE_READ_EVIDENCE)) {
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
		StripeBootShmem->mxid_disk_state = (uint32)CLUSTER_MXID_DISK_PUBLISHED;
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
	if (stripe_read_slot_all(fds, n_disks, cluster_node_id, &rec, STRIPE_READ_UPDATE)
		!= STRIPE_READ_ABSENT)
		return 0; /* no read majority proves this claim is still absent */

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
	switch (stripe_read_slot_all(fds, n_disks, target, &rec, STRIPE_READ_UPDATE)) {
	case STRIPE_READ_VALID:
		/* A newest tombstone may exist on only one disk after a partial
		 * write. Retry the same bytes until a majority fsyncs them. */
		if (!rec.retired) {
			if (rec.generation == UINT64_MAX)
				return 0;
			rec.retired = 1;
			rec.generation += 1;
		}
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

ClusterXidStripeJoinProgress
cluster_xid_stripe_join_progress(bool self_may_seed)
{
	uint32 state;

	if (StripeBootShmem == NULL || !cluster_enabled)
		return STRIPE_JOIN_PROCEED;

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
			return STRIPE_JOIN_WAIT_EVIDENCE;

		switch ((ClusterXidStripeDiskState)state) {
		case CLUSTER_XID_STRIPE_DISK_PUBLISHED:
			break; /* activation resolved; fall through to the slot vet */
		case CLUSTER_XID_STRIPE_DISK_ABSENT:
			if (self_may_seed)
				stripe_stage_seed_request();
			return STRIPE_JOIN_WAIT_EVIDENCE;
		case CLUSTER_XID_STRIPE_DISK_CORRUPT:
			return STRIPE_JOIN_REFUSE;
		case CLUSTER_XID_STRIPE_DISK_UNKNOWN:
			return STRIPE_JOIN_WAIT_EVIDENCE;
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
				return STRIPE_JOIN_CLAIM_OWNED;
			return STRIPE_JOIN_PROCEED;
		case CLUSTER_XID_STRIPE_SLOT_ABSENT: {
			uint64 req;
			uint64 done;
			uint64 staged;
			uint32 op;
			int32 target;

			staged = stripe_stage_op(STRIPE_OP_CLAIM, cluster_node_id, 0);
			if (staged != 0)
				return STRIPE_JOIN_CLAIM_OWNED;

			/* Another producer may own the mailbox.  Only the exact pending
			 * self CLAIM establishes this joiner's prerequisite ownership. */
			LWLockAcquire(&StripeBootShmem->lock, LW_SHARED);
			req = pg_atomic_read_u64(&StripeBootShmem->req_seq);
			done = pg_atomic_read_u64(&StripeBootShmem->done_seq);
			op = StripeBootShmem->op;
			target = StripeBootShmem->op_target_node;
			LWLockRelease(&StripeBootShmem->lock);
			if (req > done && op == STRIPE_OP_CLAIM && target == cluster_node_id)
				return STRIPE_JOIN_CLAIM_OWNED;
			return STRIPE_JOIN_WAIT_EVIDENCE;
		}
		case CLUSTER_XID_STRIPE_SLOT_RETIRED:
		case CLUSTER_XID_STRIPE_SLOT_CORRUPT:
			return STRIPE_JOIN_REFUSE;
		case CLUSTER_XID_STRIPE_SLOT_UNKNOWN:
			return STRIPE_JOIN_WAIT_EVIDENCE;
		}
		return STRIPE_JOIN_WAIT_EVIDENCE; /* unreachable; fail closed */
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
		return STRIPE_JOIN_REFUSE;
	case CLUSTER_XID_STRIPE_DISK_ABSENT:
		return STRIPE_JOIN_PROCEED;
	case CLUSTER_XID_STRIPE_DISK_UNKNOWN:
		return STRIPE_JOIN_WAIT_EVIDENCE;
	}
	return STRIPE_JOIN_WAIT_EVIDENCE; /* unreachable; fail closed */
}

ClusterXidStripeJoinVerdict
cluster_xid_stripe_join_gate(bool self_may_seed)
{
	switch (cluster_xid_stripe_join_progress(self_may_seed)) {
	case STRIPE_JOIN_PROCEED:
		return CLUSTER_XID_STRIPE_JOIN_PROCEED;
	case STRIPE_JOIN_REFUSE:
		return CLUSTER_XID_STRIPE_JOIN_REFUSE;
	case STRIPE_JOIN_WAIT_EVIDENCE:
	case STRIPE_JOIN_CLAIM_OWNED:
		return CLUSTER_XID_STRIPE_JOIN_HOLD;
	}
	return CLUSTER_XID_STRIPE_JOIN_HOLD;
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
 * leaves the mxid face unlatched for the life of the process. PGRAC:
 * shared-config allocation then refuses; this observer does not grant
 * permission for dense allocation. Foreign reads still fail closed.
 */
void
cluster_mxid_stripe_lazy_latch(void)
{
	uint32 mxid_floor = 0;
	uint32 mxid_state;
	bool published = false;

	if (mxid_stripe_latch_done || StripeBootShmem == NULL)
		return;

	LWLockAcquire(&StripeBootShmem->lock, LW_SHARED);
	mxid_state = StripeBootShmem->mxid_disk_state;
	if (StripeBootShmem->disk_state == CLUSTER_XID_STRIPE_DISK_PUBLISHED
		&& mxid_state == (uint32)CLUSTER_MXID_DISK_PUBLISHED && StripeBootShmem->mxid_floor != 0) {
		published = true;
		mxid_floor = StripeBootShmem->mxid_floor;
	}
	LWLockRelease(&StripeBootShmem->lock);

	/* POISONED dominates (spec-7.1 integration review P0): latch the
	 * sticky poisoned runtime — allocation refuses instead of going
	 * dense, derivation stays underivable. */
	if (mxid_state == (uint32)CLUSTER_MXID_DISK_POISONED) {
		cluster_mxid_stripe_latch_poisoned();
		mxid_stripe_latch_done = true;
		return;
	}

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

	/* PGRAC: a busy publication is unknown to callers without a PGPROC. */
	if (MyProc == NULL) {
		if (!LWLockConditionalAcquire(&StripeBootShmem->lock, LW_SHARED))
			return CLUSTER_XID_STRIPE_DISK_UNKNOWN;
	} else
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
	if (cluster_shared_catalog && StripeLease != NULL
		&& (cluster_xid_stripe_slot_state() == CLUSTER_XID_STRIPE_SLOT_MINE
			|| StripeLease->claim_owner != 0)) {
		uint64 minimum = Max(U64FromFullTransactionId(ReadNextFullTransactionId()),
							 pg_atomic_read_u64(&StripeBootShmem->herding_floor_full));

		stripe_lease_tick(fds, n_disks, minimum);
		/* A partial archive fixes the old image. Do not let the tracking
		 * writer change its HWM/CRC until the new incarnation is durable. */
		if (pg_atomic_read_u32(&StripeLease->failed) != 0
			|| StripeLease->publisher_incarnation != cluster_qvotec_self_incarnation_value()
			|| pg_atomic_read_u64(&StripeLease->limit_full) == 0 || !cluster_qvotec_in_quorum())
			return;
	}
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

	if (!stripe_maintenance_tick(fds, n_disks))
		return;

	/* 1. peer hwm sweep (declared, valid, non-retired slots only). */
	for (slot = 0; slot < CLUSTER_XID_STRIDE; slot++) {
		ClusterXidStripeSlotRecord rec;

		if (cluster_conf_lookup_node(slot) == NULL)
			continue;
		memset(&rec, 0, sizeof(rec));
		if (stripe_read_slot_all(fds, n_disks, slot, &rec, STRIPE_READ_EVIDENCE)
			!= STRIPE_READ_VALID)
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
	if (cluster_shared_catalog && StripeLease != NULL)
		target = Max(target, pg_atomic_read_u64(&StripeLease->floor_full));
	target = U64FromFullTransactionId(
		cluster_xid_next_striped_full(FullTransactionIdFromU64(target), cluster_node_id));

	if (target > promised) {
		ClusterXidStripeSlotRecord rec;
		char slotbuf[CLUSTER_VOTING_SLOT_BYTES];
		bool is_jump;
		int disks_ok = 0;
		int i;

		/* read-modify-write our own record (sole writer). */
		memset(&rec, 0, sizeof(rec));
		if (stripe_read_slot_all(fds, n_disks, cluster_node_id, &rec, STRIPE_READ_UPDATE)
				!= STRIPE_READ_VALID
			|| rec.retired || rec.generation == UINT64_MAX)
			return; /* face shifted under us; next tick re-evaluates */
		target = Max(target, rec.next_xid_hwm_full);
		is_jump = target > local_next;
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
	if (cluster_shared_catalog && StripeLease != NULL)
		v = Max(v, pg_atomic_read_u64(&StripeLease->floor_full));
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
	obs->mxid_disk_state = StripeBootShmem->mxid_disk_state;
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
 * GCS-race round-3 P0-1: xid wrap barrier shmem face.
 *
 * The protocol lives in cluster_xid_wrap_barrier.c (LMON tick + IC
 * handlers); this module only owns the atomics because they sit next to
 * the other allocation-hot-path fields.  All writers are one-way (0 -> 1
 * marks, monotone bitmap OR), so plain atomic read/write suffices.
 * ============================================================ */

bool
cluster_xid_wrap_barrier_passed(void)
{
	return StripeBootShmem != NULL && pg_atomic_read_u32(&StripeBootShmem->wrap_barrier_done) != 0;
}

void
cluster_xid_wrap_barrier_set_done(void)
{
	if (StripeBootShmem != NULL)
		pg_atomic_write_u32(&StripeBootShmem->wrap_barrier_done, 1);
}

bool
cluster_xid_wrap_barrier_marked(void)
{
	return StripeBootShmem != NULL
		   && pg_atomic_read_u32(&StripeBootShmem->wrap_barrier_marked) != 0;
}

void
cluster_xid_wrap_barrier_set_marked(void)
{
	if (StripeBootShmem != NULL)
		pg_atomic_write_u32(&StripeBootShmem->wrap_barrier_marked, 1);
}

void
cluster_xid_wrap_barrier_note_ack(int32 node_id)
{
	if (StripeBootShmem == NULL || node_id < 0 || node_id >= 64)
		return;
	pg_atomic_fetch_or_u64(&StripeBootShmem->wrap_barrier_ack_bitmap, UINT64CONST(1) << node_id);
}

uint64
cluster_xid_wrap_barrier_ack_bitmap(void)
{
	if (StripeBootShmem == NULL)
		return 0;
	return pg_atomic_read_u64(&StripeBootShmem->wrap_barrier_ack_bitmap);
}

uint64
cluster_xid_stripe_cluster_max_hwm(void)
{
	if (StripeBootShmem == NULL)
		return 0;
	return pg_atomic_read_u64(&StripeBootShmem->cluster_max_active_hwm);
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
cluster_xid_stripe_replay_join_v1(uint64 floor_full, uint64 epoch, int slot, bool apply)
{
	uint64 current_floor, current_epoch;
	uint32 bitmap;

	/* This face is called only by the serialized cold Startup owner, after
	 * its whole-source activation fold and original authority preflight.
	 * Do not let native first-wins or missing shmem become a false success. */
	if (StripeBootShmem == NULL || floor_full == 0 || epoch == 0
		|| slot < 0 || slot >= CLUSTER_XID_STRIDE)
		return false;
	current_floor = pg_atomic_read_u64(&StripeBootShmem->replay_floor_full);
	current_epoch = pg_atomic_read_u64(&StripeBootShmem->replay_epoch);
	bitmap = pg_atomic_read_u32(&StripeBootShmem->replay_active_bitmap);
	if (current_floor == 0 && current_epoch == 0) {
		if (bitmap != 0)
			return false;
	} else if (current_floor != floor_full || current_epoch != epoch)
		return false;
	if (!apply)
		return true;
	cluster_xid_stripe_replay_note_join(floor_full, epoch, slot);
	return pg_atomic_read_u64(&StripeBootShmem->replay_floor_full) == floor_full
		   && pg_atomic_read_u64(&StripeBootShmem->replay_epoch) == epoch
		   && (pg_atomic_read_u32(&StripeBootShmem->replay_active_bitmap) & (UINT32_C(1) << slot)) != 0;
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
