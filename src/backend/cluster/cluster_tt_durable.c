/*-------------------------------------------------------------------------
 *
 * cluster_tt_durable.c
 *	  pgrac durable Transaction Table (TT) slot commit_scn (spec-3.11 D2).
 *
 *	  Activates the durable write + lookup of the undo-segment-header TT slot
 *	  reserved by spec-3.4b (UndoSegmentHeaderData.tt_slots[], 32B each @ file
 *	  offset 112 + slot_offset*32).  See cluster_tt_durable.h for the per-API
 *	  contract and cluster_undo_xlog.c for the WAL/redo half
 *	  (XLOG_UNDO_TT_SLOT_COMMIT).
 *
 *	  Concurrency: per-slot 32B targeted writes are lock-free -- each committing
 *	  xact owns a distinct slot (non-overlapping byte range; spec-3.11 §2.2 /
 *	  Q10) and lifecycle writes the header prefix (offset 32-111), also
 *	  disjoint.  Writes are WAL-protected (no data-file fsync; a torn write is
 *	  recovered by redo -- C10).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.11-durable-tt-slot.md (§2.2, D2)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_tt_durable.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/transam.h"
#include "access/xlog.h"
#include "miscadmin.h"
#include "utils/elog.h"
#include "utils/timestamp.h"

#include "cluster/cluster_guc.h"		  /* cluster_node_id */
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_ges.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_scn.h"		  /* SCN, SCN_VALID, InvalidScn */
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_undo_cleaner.h" /* spec-3.13 D2-B scan-only pass */
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_tt_slot.h"	  /* TTSlot, TT_SLOT_COMMITTED, TT_SLOTS_PER_SEGMENT */
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_undo_segment.h" /* UndoSegmentHeaderData */
#include "cluster/cluster_undo_smgr.h"	  /* header-bytes + block I/O */
#include "cluster/storage/cluster_undo_alloc.h" /* CLUSTER_UNDO_SEGS_PER_INSTANCE */
#include "cluster/storage/cluster_undo_block0_current.h"
#include "cluster/storage/cluster_undo_xlog.h"	/* cluster_undo_emit_tt_slot_commit */


/* Absolute byte offset of TTSlot[slot_offset] within segment header block 0. */
static inline uint32
tt_slot_file_offset(uint16 slot_offset)
{
	return (uint32)offsetof(UndoSegmentHeaderData, tt_slots)
		   + (uint32)slot_offset * (uint32)sizeof(TTSlot);
}

/*
 * Own-instance owner derivation from segment_id (mirrors cluster_undo_alloc.c
 * encoding: segment_id = (owner_instance-1)*SEGS + slot + 1).
 */
static inline uint8
tt_owner_instance_for_segment(uint32 segment_id)
{
	return (uint8)(((segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE) + 1);
}


/* ============================================================
 *	Pure decision predicates (no I/O; cluster_unit-tested)
 * ============================================================ */

ClusterTTRedoDecision
cluster_tt_durable_redo_decide(uint8 slot_status, TransactionId slot_xid, uint16 slot_wrap,
							   TransactionId rec_xid, uint16 rec_wrap)
{
	/*
	 * spec-3.11 §2.3 redo: XLOG_UNDO_TT_SLOT_COMMIT records are authoritative and
	 * replay in LSN order, so the last commit per (segment, slot) wins.  APPLY
	 * unless the on-disk slot already shows a strictly newer generation
	 * (slot_wrap > rec_wrap) -- a later commit already made durable -- in which
	 * case SKIP so an older record does not regress it.
	 *
	 * A zero-init (UNUSED) slot or a FREE-path slot reuse keeps wrap unchanged
	 * while the xid differs: BIND is not WAL'd, so the on-disk slot lags the
	 * record, and the allocator only bumps wrap on recycle (COMMITTED/ABORTED ->
	 * ACTIVE), NOT on FREE -> ACTIVE reuse (cluster_tt_slot.c).  Hence "same
	 * wrap, different xid" is the normal first-write / sequential-reuse case
	 * during redo, NOT corruption -- it must APPLY.  规则 8.A: a crash after a
	 * committed slot reuse must replay cleanly, never PANIC.  slot_xid therefore
	 * does not affect the decision; xid identity is enforced at lookup time (C5),
	 * and WAL CRC -- not this predicate -- guards record authenticity.
	 */
	(void)slot_xid;
	if (slot_status > (uint8)TT_SLOT_RECYCLABLE)
		return CLUSTER_TT_REDO_BADSTATUS; /* on-disk status byte out of [0,4] = garbage */
	if (rec_wrap >= slot_wrap)
		return CLUSTER_TT_REDO_APPLY; /* fresh / reuse / recycle / idempotent */
	return CLUSTER_TT_REDO_SKIP;	  /* rec_wrap < slot_wrap: stale; newer commit durable */
}

ClusterTTActiveTransitionDecision
cluster_tt_active_transition_decide(const TTSlot *predecessor,
									uint32 disk_generation,
									uint32 expected_generation,
									TransactionId xid, uint16 wrap,
									bool identity_authorized)
{
	TTSlot zero_slot;
	bool terminal_shape;

	if (predecessor == NULL || !TransactionIdIsNormal(xid)
		|| wrap == TT_WRAP_INVALID)
		return CLUSTER_TT_ACTIVE_CORRUPT;
	if (disk_generation > expected_generation)
		return CLUSTER_TT_ACTIVE_STALE;
	if (disk_generation < expected_generation)
		return CLUSTER_TT_ACTIVE_CORRUPT;
	if (!identity_authorized)
		return CLUSTER_TT_ACTIVE_CONFLICT;

	memset(&zero_slot, 0, sizeof(zero_slot));
	if (predecessor->status == TT_SLOT_UNUSED)
		return memcmp(predecessor, &zero_slot, sizeof(zero_slot)) == 0
			? CLUSTER_TT_ACTIVE_APPLY : CLUSTER_TT_ACTIVE_CORRUPT;

	if (predecessor->status == TT_SLOT_ACTIVE) {
		if (predecessor->flags != TT_FLAGS_RESERVED
			|| SCN_VALID(predecessor->commit_scn)
			|| !UBA_is_invalid(predecessor->first_undo_block))
			return CLUSTER_TT_ACTIVE_CORRUPT;
		if (predecessor->wrap > wrap)
			return CLUSTER_TT_ACTIVE_STALE;
		if (predecessor->wrap == wrap && predecessor->xid == xid)
			return CLUSTER_TT_ACTIVE_IDEMPOTENT;
		return CLUSTER_TT_ACTIVE_CONFLICT;
	}

	terminal_shape
		= (predecessor->flags & ~TT_SLOT_FLAGS_KNOWN) == 0
		&& UBA_is_invalid(predecessor->first_undo_block)
		&& ((predecessor->status == TT_SLOT_COMMITTED
				 && SCN_VALID(predecessor->commit_scn))
			|| ((predecessor->status == TT_SLOT_ABORTED
				  || predecessor->status == TT_SLOT_RECYCLABLE)
				 && !SCN_VALID(predecessor->commit_scn)));
	if (!terminal_shape)
		return CLUSTER_TT_ACTIVE_CORRUPT;
	if (predecessor->wrap > wrap)
		return CLUSTER_TT_ACTIVE_STALE;
	if (predecessor->wrap >= TT_WRAP_MAX
		|| wrap != (uint16)(predecessor->wrap + 1))
		return CLUSTER_TT_ACTIVE_CONFLICT;
	return CLUSTER_TT_ACTIVE_APPLY;
}

ClusterTTTerminalTransitionDecision
cluster_tt_terminal_transition_decide(const TTSlot *predecessor,
								 uint32 disk_generation,
								 uint32 expected_generation,
								 TransactionId xid, uint16 wrap,
								 uint8 terminal_status, SCN commit_scn)
{
	bool terminal_shape;

	if (predecessor == NULL || expected_generation == UINT32_MAX
		|| disk_generation < expected_generation
		|| !TransactionIdIsNormal(xid) || wrap == TT_WRAP_INVALID
		|| (terminal_status != TT_SLOT_COMMITTED
			&& terminal_status != TT_SLOT_ABORTED)
		|| (terminal_status == TT_SLOT_COMMITTED) != SCN_VALID(commit_scn))
		return CLUSTER_TT_TERMINAL_CORRUPT;
	if (disk_generation > expected_generation)
		return CLUSTER_TT_TERMINAL_STALE;

	if (predecessor->wrap > wrap)
		return CLUSTER_TT_TERMINAL_STALE;
	if (predecessor->wrap < wrap || predecessor->xid != xid)
		return CLUSTER_TT_TERMINAL_CONFLICT;

	if (predecessor->status == TT_SLOT_ACTIVE) {
		if (predecessor->flags != TT_FLAGS_RESERVED
			|| SCN_VALID(predecessor->commit_scn)
			|| !UBA_is_invalid(predecessor->first_undo_block))
			return CLUSTER_TT_TERMINAL_CORRUPT;
		return CLUSTER_TT_TERMINAL_APPLY;
	}

	terminal_shape = (predecessor->flags & ~TT_SLOT_FLAGS_KNOWN) == 0
		&& UBA_is_invalid(predecessor->first_undo_block)
		&& ((predecessor->status == TT_SLOT_COMMITTED
				 && SCN_VALID(predecessor->commit_scn))
			|| (predecessor->status == TT_SLOT_ABORTED
				&& !SCN_VALID(predecessor->commit_scn)));
	if (!terminal_shape)
		return predecessor->status == TT_SLOT_UNUSED
			? CLUSTER_TT_TERMINAL_CONFLICT
			: CLUSTER_TT_TERMINAL_CORRUPT;
	if (predecessor->status != terminal_status)
		return CLUSTER_TT_TERMINAL_CONFLICT;
	if (terminal_status == TT_SLOT_COMMITTED
		&& predecessor->commit_scn != commit_scn)
		return CLUSTER_TT_TERMINAL_CONFLICT;
	return (predecessor->flags & TT_SLOT_FLAG_CTRC_RELEASE_PROVEN) != 0
		? CLUSTER_TT_TERMINAL_APPLY
		: CLUSTER_TT_TERMINAL_IDEMPOTENT;
}

static bool
tt_active_owner_matches(const ClusterTTSlotCurrentOwner *expected,
						const ClusterTTSlotCurrentOwner *observed)
{
	return expected != NULL && observed != NULL
		&& expected->segment_id == observed->segment_id
		&& expected->xid == observed->xid
		&& expected->commit_scn == observed->commit_scn
		&& expected->slot_offset == observed->slot_offset
		&& expected->wrap == observed->wrap
		&& expected->status == observed->status
		&& memcmp(expected->reserved8, observed->reserved8,
				  sizeof(expected->reserved8)) == 0;
}

/*
 * The allocator owns only the node's current segment.  A retention rollover
 * deliberately drops the old segment from that bounded shmem index while its
 * canonical ACTIVE slots remain protected in the old physical block zero.
 * Therefore an exact current-segment owner is required whenever the requested
 * segment is still current, but a different nonzero current segment is a
 * legitimate rolled-away classification rather than an identity mismatch.
 *
 * The second current-segment sample closes the race where rollover happens
 * between the first sample and the owner scan.  This helper never creates
 * transaction authority: callers must still hold exact XCUR, generation, and
 * byte-identical canonical predecessor/successor evidence.
 */
typedef enum TTAllocatorOwnerObservation {
	TT_ALLOCATOR_OWNER_MATCH,
	TT_ALLOCATOR_OWNER_ROLLED_AWAY,
	TT_ALLOCATOR_OWNER_CONFLICT
} TTAllocatorOwnerObservation;

static TTAllocatorOwnerObservation
tt_allocator_classify_owner(const ClusterTTSlotCurrentOwner *expected)
{
	ClusterTTSlotCurrentOwner observed;
	uint32 current_segment;

	if (expected == NULL || expected->segment_id == 0)
		return TT_ALLOCATOR_OWNER_CONFLICT;
	current_segment = cluster_tt_slot_current_segment(cluster_node_id);
	if (current_segment == 0)
		return TT_ALLOCATOR_OWNER_CONFLICT;
	if (current_segment != expected->segment_id)
		return TT_ALLOCATOR_OWNER_ROLLED_AWAY;

	memset(&observed, 0, sizeof(observed));
	if (cluster_tt_slot_current_owner_by_xid(
			cluster_node_id, expected->xid, &observed)
		&& tt_active_owner_matches(expected, &observed))
		return TT_ALLOCATOR_OWNER_MATCH;

	current_segment = cluster_tt_slot_current_segment(cluster_node_id);
	return current_segment != 0 && current_segment != expected->segment_id
			   ? TT_ALLOCATOR_OWNER_ROLLED_AWAY
			   : TT_ALLOCATOR_OWNER_CONFLICT;
}

static bool
tt_allocator_corroborates_or_rolled_away(const ClusterTTSlotCurrentOwner *expected)
{
	return tt_allocator_classify_owner(expected) != TT_ALLOCATOR_OWNER_CONFLICT;
}

XLogRecPtr
cluster_tt_slot_durable_publish_active(
	const ClusterTTSlotCurrentOwner *expected_owner,
	const ClusterSemanticAdmissionToken *admission,
	uint32 *segment_generation_out, TTSlot *successor_out)
{
	TTAllocatorOwnerObservation allocator_observation;
	ClusterUndoBlock0LogicalKey key;
	ClusterUndoBlock0ResolvedRoot root;
	ClusterUndoBlock0ResolvedRoot final_root;
	ClusterUndoBlock0Generation generation = {false, 0};
	ClusterUndoBlock0CurrentGuard guard = {0};
	ClusterUndoBlock0Pin pin;
	ClusterCtrcTxnKeyV1 ctrc_key;
	ClusterCtrcOriginReservation ctrc_reservation;
	ClusterCtrcOriginReserveResult ctrc_reserve_result;
	ClusterUndoBlock0CurrentStep step;
	ClusterUndoBlock0Result current_failure = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	ClusterUndoBlock0Result result;
	const ClusterGesTimeoutDetail *ges_failure;
	PGAlignedBlock disk_block;
	UndoSegmentHeaderData *disk_header = (UndoSegmentHeaderData *)disk_block.data;
	UndoSegmentHeaderData *resident_header;
	TTSlot successor;
	const UBA invalid_uba = InvalidUba_init;
	volatile XLogRecPtr bind_lsn = InvalidXLogRecPtr;
	uint64 publication_epoch;
	uint64 publication_boot_incarnation;
	TimestampTz ctrc_reserve_deadline = 0;
	uint32 ctrc_grant_generation = 0;
	uint8 owner;
	bool target_side;
	bool root_available;
	bool ctrc_reserve_perpetual;
	volatile bool current_active = false;
	volatile bool pin_held = false;
	volatile bool ctrc_pre_bind_cancel_armed = false;
	volatile bool ctrc_post_bind_block_armed = false;

	memset(&root, 0, sizeof(root));
	memset(&final_root, 0, sizeof(final_root));
	memset(&pin, 0, sizeof(pin));
	pin.slot = -1;
	memset(&ctrc_key, 0, sizeof(ctrc_key));
	memset(&ctrc_reservation, 0, sizeof(ctrc_reservation));
	memset(&successor, 0, sizeof(successor));

	if (expected_owner == NULL || admission == NULL || segment_generation_out == NULL
		|| successor_out == NULL || cluster_node_id < 0 || !admission->entered
		|| (admission->side != CLUSTER_SEMANTIC_SOURCE_SIDE
			&& admission->side != CLUSTER_SEMANTIC_TARGET_SIDE)
		|| expected_owner->segment_id == 0 || expected_owner->segment_id > UINT16_MAX
		|| expected_owner->slot_offset >= TT_SLOTS_PER_SEGMENT
		|| !TransactionIdIsNormal(expected_owner->xid) || expected_owner->wrap == TT_WRAP_INVALID
		|| expected_owner->status != CTS_ACTIVE || expected_owner->commit_scn != InvalidScn
		|| expected_owner->reserved8[0] != 0 || expected_owner->reserved8[1] != 0
		|| expected_owner->reserved8[2] != 0)
		ereport(ERROR,
				(errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
				 errmsg("cannot publish canonical ACTIVE without an exact local TT owner")));

	*segment_generation_out = UINT32_MAX;
	memset(successor_out, 0, sizeof(*successor_out));
	owner = tt_owner_instance_for_segment(expected_owner->segment_id);
	allocator_observation = owner == (uint8)(cluster_node_id + 1)
								? tt_allocator_classify_owner(expected_owner)
								: TT_ALLOCATOR_OWNER_CONFLICT;
	/* No authority or reservation has been acquired by this call yet.  Only
	 * an explicit allocator rollover permits the local owner to select again;
	 * a missing owner on the same current segment remains an identity error. */
	if (allocator_observation == TT_ALLOCATOR_OWNER_ROLLED_AWAY)
		return InvalidXLogRecPtr;
	if (allocator_observation != TT_ALLOCATOR_OWNER_MATCH)
		ereport(
			ERROR,
			(errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
			 errmsg("canonical ACTIVE allocator identity changed before publication"),
			 errdetail("PGRAC_FAMILY=TT_ACTIVE PGRAC_REASON=ALLOCATOR_IDENTITY_CONFLICT "
					   "stage=entry node=%d xid=%u segment=%u slot=%u wrap=%u current_segment=%u",
					   cluster_node_id, expected_owner->xid, expected_owner->segment_id,
					   expected_owner->slot_offset, expected_owner->wrap,
					   cluster_tt_slot_current_segment(cluster_node_id))));

	key.segment_id = expected_owner->segment_id;
	key.owner_instance = owner;
	target_side = admission->side == CLUSTER_SEMANTIC_TARGET_SIDE;
	ctrc_reserve_perpetual = cluster_ges_request_timeout_ms == -1;
	if (!ctrc_reserve_perpetual)
		ctrc_reserve_deadline = TimestampTzPlusMilliseconds(
			GetCurrentTimestamp(), cluster_ges_request_timeout_ms);

	PG_TRY();
	{
	retry_active_snapshot:
		MemSet(&root, 0, sizeof(root));
		MemSet(&generation, 0, sizeof(generation));
		root_available = target_side
			? cluster_semantic_activation_resolve_shared_undo_root(
				  admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, owner,
				  expected_owner->segment_id, &root)
			: cluster_semantic_activation_resolve_shared_undo_root_live_owner_source(
				  admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, owner,
				  expected_owner->segment_id, &root);

		step = target_side
			? cluster_undo_block0_current_acquire_begin_live_owner_target(
				  &key, cluster_ges_request_timeout_ms, admission, &guard,
				  &current_failure)
			: cluster_undo_block0_current_acquire_begin_live_owner_source(
				  &key, cluster_ges_request_timeout_ms, admission, &guard,
				  &current_failure);
		if (step == CLUSTER_UNDO_BLOCK0_CURRENT_FAILED)
		{
			/* A failed begin retains no guard under the current API.  A known
			 * obsolete local candidate need not restore its old readiness before
			 * returning the still-unpublished reservation to its caller. */
			if (tt_allocator_classify_owner(expected_owner) == TT_ALLOCATOR_OWNER_ROLLED_AWAY)
				goto active_publication_done;
			ges_failure = cluster_ges_timeout_detail_get();
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
					 errmsg("canonical ACTIVE block-zero current authority is unavailable"),
					 errdetail("PGRAC_FAMILY=BLOCK0_CURRENT PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
							   "PGRAC_NODE=%d PGRAC_ATTEMPT=0 "
							   "segment=%u result=%d source=%s master=%d attempts=%d",
							   cluster_node_id, expected_owner->segment_id, (int)current_failure,
							   cluster_ges_timeout_src_text(ges_failure->source),
							   ges_failure->master_node, ges_failure->attempts)));
		}
		current_active = true;

		while (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING) {
			CHECK_FOR_INTERRUPTS();
			step = cluster_undo_block0_current_acquire_poll(&guard,
				&current_failure);
			if (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING
				&& !cluster_undo_block0_current_wait_reply(
					&guard, CLUSTER_UNDO_BLOCK0_WAIT_TT_ACTIVE_ACQUIRE))
				pg_usleep(1000L);
		}
		/* Classify after the wait but before errors about the obsolete root.
		 * No CTRC reservation, BIND or physical write exists at this cut. */
		allocator_observation = tt_allocator_classify_owner(expected_owner);
		if (allocator_observation == TT_ALLOCATOR_OWNER_ROLLED_AWAY)
			goto active_publication_done;
		if (step != CLUSTER_UNDO_BLOCK0_CURRENT_HELD || !root_available)
		{
			ges_failure = cluster_ges_timeout_detail_get();
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
					 errmsg("canonical ACTIVE shared block-zero root is unavailable"),
					 errdetail("PGRAC_FAMILY=BLOCK0_CURRENT PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
							   "PGRAC_NODE=%d PGRAC_ATTEMPT=0 "
							   "segment=%u side=%s root_available=%s step=%d result=%d "
							   "source=%s master=%d attempts=%d",
							   cluster_node_id, expected_owner->segment_id,
							   target_side ? "target" : "source", root_available ? "true" : "false",
							   (int)step, (int)current_failure,
							   cluster_ges_timeout_src_text(ges_failure->source),
							   ges_failure->master_node, ges_failure->attempts)));
		}
		/* With XCUR held, this exact allocator check is the linearization
		 * point: later rollover cannot reuse the physical segment until this
		 * guard is released.  Every released-CTRC retry passes here again. */
		if (allocator_observation != TT_ALLOCATOR_OWNER_MATCH)
			ereport(
				ERROR,
				(errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
				 errmsg("canonical ACTIVE allocator identity changed before publication"),
				 errdetail(
					 "PGRAC_FAMILY=TT_ACTIVE PGRAC_REASON=ALLOCATOR_IDENTITY_CONFLICT "
					 "stage=prebind node=%d xid=%u segment=%u slot=%u wrap=%u current_segment=%u",
					 cluster_node_id, expected_owner->xid, expected_owner->segment_id,
					 expected_owner->slot_offset, expected_owner->wrap,
					 cluster_tt_slot_current_segment(cluster_node_id))));

		result = cluster_undo_block0_current_sample_generation_exclusive(
			&guard, &root, &generation);
		if (result != CLUSTER_UNDO_BLOCK0_OK || !generation.known
			|| generation.value == UINT32_MAX)
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
					 errmsg("canonical ACTIVE block-zero generation is unavailable"),
					 errdetail("PGRAC_FAMILY=BLOCK0_CURRENT PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
							   "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
							   cluster_node_id)));

		result = cluster_undo_block0_current_pin_exclusive(
			&guard, &root, &generation, &pin, (char **)&resident_header);
		if (result != CLUSTER_UNDO_BLOCK0_OK || resident_header == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
					 errmsg("canonical ACTIVE block-zero content authority is unavailable"),
					 errdetail("PGRAC_FAMILY=BLOCK0_CURRENT PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
							   "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
							   cluster_node_id)));
		pin_held = true;

		cluster_tt_durable_io_wait_start();
		if (!cluster_undo_smgr_read_block(root.intent,
				expected_owner->segment_id, owner, 0, disk_block.data)) {
			cluster_tt_durable_io_wait_end();
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("cannot read canonical TT block zero for ACTIVE publication")));
		}
		cluster_tt_durable_io_wait_end();

		if (disk_header->segment_id != expected_owner->segment_id
			|| disk_header->owner_instance != owner
			|| disk_header->tt_slots_count != TT_SLOTS_PER_SEGMENT
			|| disk_header->wrap_count != generation.value
			|| resident_header->segment_id != disk_header->segment_id
			|| resident_header->owner_instance != disk_header->owner_instance
			|| resident_header->tt_slots_count != disk_header->tt_slots_count
			|| resident_header->wrap_count != disk_header->wrap_count
			|| memcmp(&resident_header->tt_slots[expected_owner->slot_offset],
					  &disk_header->tt_slots[expected_owner->slot_offset],
					  sizeof(TTSlot)) != 0)
			ereport(
				ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("canonical ACTIVE block-zero identity or generation mismatch"),
				 errdetail("PGRAC_FAMILY=BLOCK0_CURRENT PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
						   "PGRAC_NODE=%d PGRAC_ATTEMPT=0 "
						   "segment=%u slot=%u expected_generation=%u "
						   "disk={segment=%u owner=%u slots=%u generation=%u "
						   "xid=%u wrap=%u status=%u} "
						   "resident={segment=%u owner=%u slots=%u generation=%u "
						   "xid=%u wrap=%u status=%u}",
						   cluster_node_id, expected_owner->segment_id, expected_owner->slot_offset,
						   generation.value, disk_header->segment_id, disk_header->owner_instance,
						   disk_header->tt_slots_count, disk_header->wrap_count,
						   disk_header->tt_slots[expected_owner->slot_offset].xid,
						   disk_header->tt_slots[expected_owner->slot_offset].wrap,
						   disk_header->tt_slots[expected_owner->slot_offset].status,
						   resident_header->segment_id, resident_header->owner_instance,
						   resident_header->tt_slots_count, resident_header->wrap_count,
						   resident_header->tt_slots[expected_owner->slot_offset].xid,
						   resident_header->tt_slots[expected_owner->slot_offset].wrap,
						   resident_header->tt_slots[expected_owner->slot_offset].status)));

		publication_epoch = cluster_epoch_get_current();
		publication_boot_incarnation
			= cluster_qvotec_get_self_incarnation();
		if (publication_epoch > UINT32_MAX
			|| publication_boot_incarnation == 0
			|| admission->record_generation == 0
			|| root.root_id == 0 || root.root_generation == 0
			|| GetSystemIdentifier() == 0)
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
					 errmsg("canonical ACTIVE CTRC identity is unavailable"),
					 errdetail("epoch=%llu boot_incarnation=%llu "
							   "record_generation=%llu formation_epoch=%llu "
							   "root_id=%llu root_generation=%llu "
							   "system_identifier=%llu",
							   (unsigned long long)publication_epoch,
							   (unsigned long long)publication_boot_incarnation,
							   (unsigned long long)admission->record_generation,
							   (unsigned long long)admission->formation_epoch,
							   (unsigned long long)root.root_id,
							   (unsigned long long)root.root_generation,
							   (unsigned long long)GetSystemIdentifier())));
		MemSet(&ctrc_key, 0, sizeof(ctrc_key));
		ctrc_key.format_version = CLUSTER_CTRC_FORMAT_VERSION;
		ctrc_key.owner_instance = owner;
		ctrc_key.origin_node_id = (uint16)cluster_node_id;
		ctrc_key.segment_id = expected_owner->segment_id;
		ctrc_key.segment_generation = generation.value;
		ctrc_key.slot_offset = expected_owner->slot_offset;
		ctrc_key.slot_wrap = expected_owner->wrap;
		ctrc_key.xid = expected_owner->xid;
		ctrc_key.cluster_epoch = (uint32)publication_epoch;
		ctrc_key.system_identifier = GetSystemIdentifier();
		ctrc_key.origin_boot_incarnation = publication_boot_incarnation;
		ctrc_key.formation_epoch = admission->formation_epoch;
		ctrc_key.admission_record_generation = admission->record_generation;
		ctrc_key.root_descriptor_incarnation = root.root_generation;
		ctrc_key.root_id = root.root_id;
		ctrc_key.root_generation = root.root_generation;
		ctrc_reserve_result = cluster_ctrc_origin_reserve_active(
			&ctrc_key, &ctrc_reservation);
		if (ctrc_reserve_result
			== CLUSTER_CTRC_ORIGIN_RESERVE_RETRY_RELEASED)
		{
			/* The old row needs cleaner progress before it can be reclaimed.
			 * Never retain block-0 current or its content pin across that
			 * asynchronous wait: the cleaner can require the same authority
			 * before it reaches certificate notification.  The next attempt
			 * reacquires and rebuilds the complete canonical snapshot. */
			if (pin_held)
			{
				cluster_undo_block0_unpin(&pin);
				pin_held = false;
			}
			if (current_active)
			{
				cluster_undo_block0_current_cancel(&guard);
					current_active = false;
					MemSet(&guard, 0, sizeof(guard));
				}
			/* Terminal publication already woke the first cleaner pass.  Re-arm
			 * explicitly for an overlap observed between passes, then poll only
			 * the CTRC row.  Reacquiring block-0 XCUR while the released row is
			 * unchanged creates no new evidence and can starve the cleaner/GES
			 * path that must deliver the notification. */
			cluster_undo_cleaner_wakeup();
			while (cluster_ctrc_origin_release_overlap_pending(&ctrc_key))
			{
				if (!ctrc_reserve_perpetual
					&& GetCurrentTimestamp() >= ctrc_reserve_deadline)
					break;
				CHECK_FOR_INTERRUPTS();
				pg_usleep(1000L);
			}
			if (ctrc_reserve_perpetual
				|| GetCurrentTimestamp() < ctrc_reserve_deadline)
			{
				CHECK_FOR_INTERRUPTS();
				goto retry_active_snapshot;
			}
		}
		if (ctrc_reserve_result == CLUSTER_CTRC_ORIGIN_RESERVE_REFUSED)
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
					 errmsg("canonical ACTIVE CTRC origin reservation is unavailable"),
					 errdetail("ready=%s owner=%u origin=%u segment=%u "
							   "generation=%u slot=%u wrap=%u xid=%u",
							   cluster_ctrc_shmem_ready() ? "true" : "false",
							   ctrc_key.owner_instance,
							   ctrc_key.origin_node_id,
							   ctrc_key.segment_id,
							   ctrc_key.segment_generation,
							   ctrc_key.slot_offset,
							   ctrc_key.slot_wrap,
							   ctrc_key.xid)));
		if (ctrc_reserve_result
			== CLUSTER_CTRC_ORIGIN_RESERVE_RETRY_RELEASED)
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
					 errmsg("canonical ACTIVE CTRC origin release notification timed out"),
					 errdetail("owner=%u origin=%u segment=%u generation=%u "
							   "slot=%u wrap=%u xid=%u timeout_ms=%d",
							   ctrc_key.owner_instance,
							   ctrc_key.origin_node_id,
							   ctrc_key.segment_id,
							   ctrc_key.segment_generation,
							   ctrc_key.slot_offset,
							   ctrc_key.slot_wrap,
							   ctrc_key.xid,
							   cluster_ges_request_timeout_ms)));
		ctrc_pre_bind_cancel_armed
			= ctrc_reserve_result == CLUSTER_CTRC_ORIGIN_RESERVED_PENDING;

		switch (cluster_tt_active_transition_decide(
			&disk_header->tt_slots[expected_owner->slot_offset],
			disk_header->wrap_count, generation.value,
			expected_owner->xid, expected_owner->wrap, true)) {
		case CLUSTER_TT_ACTIVE_APPLY:
			memset(&successor, 0, sizeof(successor));
			successor.xid = expected_owner->xid;
			successor.wrap = expected_owner->wrap;
			successor.status = TT_SLOT_ACTIVE;
			successor.commit_scn = InvalidScn;
			successor.first_undo_block = invalid_uba;
			break;
		case CLUSTER_TT_ACTIVE_IDEMPOTENT:
			successor = disk_header->tt_slots[expected_owner->slot_offset];
			break;
		case CLUSTER_TT_ACTIVE_STALE:
		case CLUSTER_TT_ACTIVE_CONFLICT:
		case CLUSTER_TT_ACTIVE_CORRUPT:
		default:
			ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("canonical ACTIVE predecessor is not an exact legal transition")));
		}

		bind_lsn = cluster_undo_emit_tt_slot_bind(
			owner, expected_owner->segment_id, generation.value,
			expected_owner->slot_offset, expected_owner->wrap,
			expected_owner->xid);
		if (XLogRecPtrIsInvalid(bind_lsn))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("canonical ACTIVE BIND WAL was not inserted")));
		if (ctrc_pre_bind_cancel_armed)
		{
			ctrc_pre_bind_cancel_armed = false;
			ctrc_post_bind_block_armed = true;
		}

		if (memcmp(&disk_header->tt_slots[expected_owner->slot_offset],
				   &successor, sizeof(successor)) != 0) {
			cluster_tt_durable_io_wait_start();
			if (!cluster_undo_smgr_write_header_bytes(
					root.intent, expected_owner->segment_id, owner,
					tt_slot_file_offset(expected_owner->slot_offset),
					(const char *)&successor, sizeof(successor))) {
				cluster_tt_durable_io_wait_end();
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("cannot write canonical ACTIVE TT slot")));
			}
			cluster_tt_durable_io_wait_end();
			memcpy(&resident_header->tt_slots[expected_owner->slot_offset],
				   &successor, sizeof(successor));
		}

		if (!(target_side
				  ? cluster_semantic_activation_resolve_shared_undo_root(
						admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, owner,
						expected_owner->segment_id, &final_root)
				  : cluster_semantic_activation_resolve_shared_undo_root_live_owner_source(
						admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, owner,
						expected_owner->segment_id, &final_root))
			|| !cluster_undo_block0_root_matches(&root, &final_root)
			|| !cluster_semantic_activation_modifier_recheck(admission, true)
			|| cluster_epoch_get_current() != publication_epoch
			|| cluster_qvotec_get_self_incarnation()
			   != publication_boot_incarnation
			|| cluster_undo_block0_current_recheck_exclusive(&guard)
			   != CLUSTER_UNDO_BLOCK0_OK
			|| resident_header->wrap_count != generation.value
			|| memcmp(&resident_header->tt_slots[expected_owner->slot_offset],
					  &successor, sizeof(successor)) != 0
			|| !tt_allocator_corroborates_or_rolled_away(expected_owner))
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
					 errmsg("canonical ACTIVE authority drifted during publication")));
		if (!cluster_ctrc_origin_open_reserved(
				&ctrc_reservation, &ctrc_grant_generation)
			|| ctrc_grant_generation == 0)
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
					 errmsg("canonical ACTIVE CTRC origin grant is unavailable")));
		ctrc_post_bind_block_armed = false;

		*segment_generation_out = generation.value;
		*successor_out = successor;
	active_publication_done:;
	}
	PG_FINALLY();
	{
		if (ctrc_pre_bind_cancel_armed)
		{
			(void)cluster_ctrc_origin_cancel_pre_bind(&ctrc_reservation);
			ctrc_pre_bind_cancel_armed = false;
		}
		else if (ctrc_post_bind_block_armed)
		{
			(void)cluster_ctrc_origin_block_post_bind(&ctrc_reservation);
			ctrc_post_bind_block_armed = false;
		}
		if (pin_held) {
			cluster_undo_block0_unpin(&pin);
			pin_held = false;
		}
		if (current_active) {
			cluster_undo_block0_current_cancel(&guard);
			current_active = false;
		}
	}
	PG_END_TRY();

	return (XLogRecPtr)bind_lsn;
}

ClusterTTActiveTransitionDecision
cluster_tt_durable_bind_preflight_exact(uint8 instance, uint32 segment_id,
										uint32 segment_generation,
										uint16 slot_offset, uint16 wrap,
										TransactionId xid)
{
	PGAlignedBlock first;
	PGAlignedBlock second;
	const UndoSegmentHeaderData *first_header
		= (const UndoSegmentHeaderData *)first.data;
	const UndoSegmentHeaderData *second_header
		= (const UndoSegmentHeaderData *)second.data;
	uint8 expected_instance;

	if (instance == 0 || segment_id == 0
		|| slot_offset >= TT_SLOTS_PER_SEGMENT
		|| !TransactionIdIsNormal(xid) || wrap == TT_WRAP_INVALID)
		return CLUSTER_TT_ACTIVE_CORRUPT;
	expected_instance = tt_owner_instance_for_segment(segment_id);
	if (instance != expected_instance)
		return CLUSTER_TT_ACTIVE_CORRUPT;

	cluster_tt_durable_io_wait_start();
	if (!cluster_undo_smgr_read_block(cluster_undo_intent_for_owner(instance),
			segment_id, instance, 0, first.data)
		|| !cluster_undo_smgr_read_block(cluster_undo_intent_for_owner(instance),
			segment_id, instance, 0, second.data)) {
		cluster_tt_durable_io_wait_end();
		return CLUSTER_TT_ACTIVE_CONFLICT;
	}
	cluster_tt_durable_io_wait_end();

	if (first_header->segment_id != segment_id
		|| first_header->owner_instance != instance
		|| first_header->tt_slots_count != TT_SLOTS_PER_SEGMENT
		|| second_header->segment_id != first_header->segment_id
		|| second_header->owner_instance != first_header->owner_instance
		|| second_header->tt_slots_count != first_header->tt_slots_count)
		return CLUSTER_TT_ACTIVE_CORRUPT;
	if (second_header->wrap_count != first_header->wrap_count
		|| memcmp(&second_header->tt_slots[slot_offset],
				  &first_header->tt_slots[slot_offset], sizeof(TTSlot)) != 0)
		return CLUSTER_TT_ACTIVE_CONFLICT;

	return cluster_tt_active_transition_decide(
		&first_header->tt_slots[slot_offset], first_header->wrap_count,
		segment_generation, xid, wrap, true);
}

ClusterTTTerminalTransitionDecision
cluster_tt_durable_abort_preflight_exact(uint8 instance, uint32 segment_id,
										 uint32 segment_generation,
										 uint16 slot_offset, uint16 wrap,
										 TransactionId xid)
{
	PGAlignedBlock first;
	PGAlignedBlock second;
	const UndoSegmentHeaderData *first_header
		= (const UndoSegmentHeaderData *)first.data;
	const UndoSegmentHeaderData *second_header
		= (const UndoSegmentHeaderData *)second.data;

	if (instance == 0 || segment_id == 0
		|| segment_generation == UINT32_MAX
		|| slot_offset >= TT_SLOTS_PER_SEGMENT
		|| !TransactionIdIsNormal(xid) || wrap == TT_WRAP_INVALID
		|| instance != tt_owner_instance_for_segment(segment_id))
		return CLUSTER_TT_TERMINAL_CORRUPT;
	cluster_tt_durable_io_wait_start();
	if (!cluster_undo_smgr_read_block(cluster_undo_intent_for_owner(instance),
			segment_id, instance, 0, first.data)
		|| !cluster_undo_smgr_read_block(cluster_undo_intent_for_owner(instance),
			segment_id, instance, 0, second.data)) {
		cluster_tt_durable_io_wait_end();
		return CLUSTER_TT_TERMINAL_CONFLICT;
	}
	cluster_tt_durable_io_wait_end();
	if (first_header->segment_id != segment_id
		|| first_header->owner_instance != instance
		|| first_header->tt_slots_count != TT_SLOTS_PER_SEGMENT
		|| second_header->segment_id != first_header->segment_id
		|| second_header->owner_instance != first_header->owner_instance
		|| second_header->tt_slots_count != first_header->tt_slots_count)
		return CLUSTER_TT_TERMINAL_CORRUPT;
	if (second_header->wrap_count != first_header->wrap_count
		|| memcmp(&second_header->tt_slots[slot_offset],
				  &first_header->tt_slots[slot_offset], sizeof(TTSlot)) != 0)
		return CLUSTER_TT_TERMINAL_CONFLICT;
	return cluster_tt_terminal_transition_decide(
		&first_header->tt_slots[slot_offset], first_header->wrap_count,
		segment_generation, xid, wrap, TT_SLOT_ABORTED, InvalidScn);
}

ClusterUndoTtCtrcReleaseRedoDecision
cluster_tt_durable_ctrc_release_preflight_exact(
	const xl_undo_tt_slot_ctrc_release_v1 *record)
{
	PGAlignedBlock first;
	PGAlignedBlock second;
	const UndoSegmentHeaderData *first_header
		= (const UndoSegmentHeaderData *)first.data;
	const UndoSegmentHeaderData *second_header
		= (const UndoSegmentHeaderData *)second.data;
	ClusterUndoPathIntent intent;

	if (!cluster_undo_tt_ctrc_release_valid(record)
		|| record->owner_instance
		   != tt_owner_instance_for_segment(record->segment_id))
		return CLUSTER_UNDO_TT_CTRC_RELEASE_REDO_CONFLICT;
	intent = cluster_undo_intent_for_owner(record->owner_instance);
	cluster_tt_durable_io_wait_start();
	if (!cluster_undo_smgr_read_block(intent, record->segment_id,
			record->owner_instance, 0, first.data)
		|| !cluster_undo_smgr_read_block(intent, record->segment_id,
			record->owner_instance, 0, second.data))
	{
		cluster_tt_durable_io_wait_end();
		return CLUSTER_UNDO_TT_CTRC_RELEASE_REDO_CONFLICT;
	}
	cluster_tt_durable_io_wait_end();
	if (first_header->segment_id != record->segment_id
		|| first_header->owner_instance != record->owner_instance
		|| first_header->tt_slots_count != TT_SLOTS_PER_SEGMENT
		|| second_header->segment_id != first_header->segment_id
		|| second_header->owner_instance != first_header->owner_instance
		|| second_header->tt_slots_count != first_header->tt_slots_count
		|| second_header->wrap_count != first_header->wrap_count
		|| memcmp(&second_header->tt_slots[record->slot_offset],
			&first_header->tt_slots[record->slot_offset], sizeof(TTSlot)) != 0)
		return CLUSTER_UNDO_TT_CTRC_RELEASE_REDO_CONFLICT;
	return cluster_undo_tt_ctrc_release_redo_decide(
		first_header->wrap_count,
		&first_header->tt_slots[record->slot_offset], record);
}

void
cluster_tt_durable_redo_ctrc_release_slot_exact(
	const xl_undo_tt_slot_ctrc_release_v1 *record)
{
	ClusterUndoPathIntent intent;
	PGAlignedBlock block;
	UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)block.data;
	ClusterUndoTtCtrcReleaseRedoDecision decision;
	TTSlot *slot;

	if (!cluster_undo_tt_ctrc_release_valid(record)
		|| record->owner_instance
		   != tt_owner_instance_for_segment(record->segment_id))
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid exact TT CTRC release identity")));
	intent = cluster_undo_intent_for_owner(record->owner_instance);
	cluster_tt_durable_io_wait_start();
	if (!cluster_undo_smgr_read_block(intent, record->segment_id,
			record->owner_instance, 0, block.data))
	{
		cluster_tt_durable_io_wait_end();
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cannot read undo segment %u for CTRC release redo",
					record->segment_id)));
	}
	if (header->segment_id != record->segment_id
		|| header->owner_instance != record->owner_instance
		|| header->tt_slots_count != TT_SLOTS_PER_SEGMENT)
	{
		cluster_tt_durable_io_wait_end();
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("conflicting undo segment header for CTRC release redo")));
	}
	slot = &header->tt_slots[record->slot_offset];
	decision = cluster_undo_tt_ctrc_release_redo_decide(
		header->wrap_count, slot, record);
	if (decision == CLUSTER_UNDO_TT_CTRC_RELEASE_REDO_SKIP_STALE)
	{
		cluster_tt_durable_io_wait_end();
		cluster_vis_bump_recovery_undo_redo_skips();
		return;
	}
	if (decision == CLUSTER_UNDO_TT_CTRC_RELEASE_REDO_IDEMPOTENT)
	{
		cluster_tt_durable_io_wait_end();
		return;
	}
	if (decision != CLUSTER_UNDO_TT_CTRC_RELEASE_REDO_APPLY)
	{
		cluster_tt_durable_io_wait_end();
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("conflicting terminal predecessor for CTRC release redo"),
				 errdetail("segment_generation=%u disk_generation=%u slot=%u wrap=%u xid=%u",
					record->segment_generation, header->wrap_count,
					record->slot_offset, record->slot_wrap, record->xid)));
	}
	slot->flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	if (!cluster_undo_smgr_write_header_bytes(intent, record->segment_id,
			record->owner_instance,
			(uint32)offsetof(UndoSegmentHeaderData, tt_slots)
				+ (uint32)record->slot_offset * (uint32)sizeof(TTSlot),
			(const char *)slot, sizeof(*slot))
		|| !cluster_undo_smgr_fsync_segment_file(record->segment_id,
			record->owner_instance))
	{
		cluster_tt_durable_io_wait_end();
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cannot durably write CTRC release bit for undo segment %u slot %u",
					record->segment_id, record->slot_offset)));
	}
	cluster_tt_durable_io_wait_end();
	cluster_vis_bump_recovery_undo_redo_applies();
}

bool
cluster_tt_durable_slot_match(uint8 slot_status, TransactionId slot_xid, uint16 slot_wrap,
							  SCN slot_commit_scn, TransactionId want_xid, uint32 expected_wrap)
{
	/* spec-3.11 C5: COMMITTED + exact xid + valid commit_scn.  xid mismatch is
	 * the recycle detector (reuse stamps a new owner xid).
	 *
	 * spec-4.5a G4 (F3): when the caller carries the binding-time generation,
	 * a wrap mismatch is ALSO the recycle detector -- it catches the slot
	 * recycled to a new generation whose 32-bit xid wrapped to the same
	 * value, which xid alone cannot.  CLUSTER_TT_WRAP_ANY = no expectation. */
	if (expected_wrap != CLUSTER_TT_WRAP_ANY && slot_wrap != (uint16)expected_wrap)
		return false;
	return slot_status == (uint8)TT_SLOT_COMMITTED && slot_xid == want_xid
		   && SCN_VALID(slot_commit_scn);
}


/*
 * cluster_tt_durable_classify -- spec-3.22 pure classifier (no I/O).  See the
 * header for the contract; the precedence below is what makes the §2.4 split
 * sound: a 0-xid-match is only a RECYCLED proof after a COMPLETE scan, and an
 * owned-by-xid unstamped slot is a 1-match (retained), never a 0-match.
 */
ClusterTTDurableResolve
cluster_tt_durable_classify(int xid_matches, bool match_has_valid_scn, bool scan_complete)
{
	/* >1 is definitive ambiguity (raw-xid wrap residue): fail-closed even if the
	 * scan was incomplete -- we already have two candidates and cannot pick. */
	if (xid_matches > 1)
		return CLUSTER_TT_DURABLE_AMBIGUOUS_WRAP;

	/* An incomplete scan cannot be trusted to a 0- or 1-tally: a missed segment
	 * could hold the owner (turning 0 into 1) or a second match (turning 1 into
	 * ambiguous).  Never let it masquerade as RECYCLED_ZERO_MATCH (规则 8.A). */
	if (!scan_complete)
		return CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE;

	if (xid_matches == 0)
		return CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH;

	/* xid_matches == 1 */
	return match_has_valid_scn ? CLUSTER_TT_DURABLE_RESOLVED_SCN
							   : CLUSTER_TT_DURABLE_XID_MATCH_INVALID_SCN;
}


/* spec-4.8 D7-A (P1#1): canonical invalid chain head for commit/abort stamps. */
static const UBA InvalidUbaVal = InvalidUba_init;

/*
 * tt_slot_write_committed -- the per-slot 32B targeted RMW shared by the
 * WAL-emitting durable commit (2PC, standalone 0x30) and the spec-3.18 D4.1
 * fold path (no 0x30; the delta rides the commit record).  Read the slot
 * (preserve flags), stamp COMMITTED + commit_scn, clear first_undo_block, write
 * 32B back.  Lock-free -- this xact is the sole owner of this slot (spec-3.11
 * §2.2).  NOT fsync'd (C10): durability comes from the WAL flush of whichever
 * record carries the delta; a crash before that flush leaves neither durable.
 *
 * spec-4.8ab D3 durability-ordering contract:  the TT slot WAL is emitted
 * BEFORE this byte-targeted write (cluster_tt_slot_durable_commit emits the 0x30
 * first; the fold path inserts the commit-record delta first), and this write is
 * NOT fsync'd.  So the commit_scn evidence is durable only via that WAL flush +
 * redo re-stamp -- the on-disk block-0 bytes are never authoritative ahead of
 * their WAL.  TT slots live in undo block 0, which is NOT poolable, so the
 * checkpoint-writeback boundary (spec-4.8ab D1, cluster_undo_buf.c) does not
 * cover them:  data blocks are flushed WAL-before-data by the pool, block 0 is
 * WAL-redo-only here.  The two durability domains are disjoint by design.
 *
 * spec-4.8 D7-A (P1#1): clearing first_undo_block here (and in durable_abort +
 * both redo APPLY paths) keeps the D7 physical-rollback invariant -- an ABORTED
 * slot carries a non-invalid chain head ONLY if XLOG_UNDO_TT_SLOT_SET_HEAD (0x90)
 * re-attached one for the slot's current (xid, wrap).  Otherwise a recycled slot
 * would inherit a prior owner's stale head and D7 would walk a foreign chain.
 */
static void
tt_slot_write_committed(uint32 segment_id, uint8 owner, uint16 slot_offset, TransactionId xid,
						uint16 wrap, SCN commit_scn, TTSlot *successor_out)
{
	uint32 off = tt_slot_file_offset(slot_offset);
	TTSlot slot;

	cluster_tt_durable_io_wait_start();

	if (!cluster_undo_smgr_read_header_bytes(cluster_undo_intent_for_owner(owner), segment_id,
											 owner, off, (char *)&slot, sizeof(slot))) {
		cluster_tt_durable_io_wait_end();
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster durable TT: cannot read slot %u of undo segment %u",
							   slot_offset, segment_id)));
	}

	slot.xid = xid;
	slot.wrap = wrap;
	slot.status = (uint8)TT_SLOT_COMMITTED;
	slot.flags = TT_FLAGS_RESERVED;
	slot.commit_scn = commit_scn;
	slot.first_undo_block = InvalidUbaVal; /* spec-4.8 D7-A (P1#1): no stale head */

	if (!cluster_undo_smgr_write_header_bytes(cluster_undo_intent_for_owner(owner), segment_id,
											  owner, off, (const char *)&slot, sizeof(slot))) {
		cluster_tt_durable_io_wait_end();
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster durable TT: cannot write slot %u of undo segment %u",
							   slot_offset, segment_id)));
	}

	cluster_tt_durable_io_wait_end();
	cluster_tt_durable_count_commit();
	if (successor_out != NULL)
		*successor_out = slot;
}

void
cluster_tt_slot_durable_commit(uint32 segment_id, uint16 slot_offset, TransactionId xid,
							   uint16 wrap, SCN commit_scn)
{
	uint8 owner = tt_owner_instance_for_segment(segment_id);

	Assert(slot_offset < TT_SLOTS_PER_SEGMENT);
	Assert(TransactionIdIsValid(xid));
	Assert(SCN_VALID(commit_scn));

	/*
	 * spec-3.11 C1: standalone XLOG_UNDO_TT_SLOT_COMMIT (0x30) BEFORE the
	 * commit record (caller is the 2PC COMMIT PREPARED durable hook).  The
	 * commit record's XLogFlush / group commit makes it durable; the data-file
	 * write below is NOT fsync'd (C10) -- a crash before the commit record
	 * means neither is durable; after, redo replays this WAL.
	 *
	 * spec-3.18 D4.1: only the 2PC path still emits 0x30; normal commits fold
	 * the equivalent delta into the commit record via
	 * cluster_tt_slot_durable_commit_writeonly() (no 0x30).  Leaving 2PC on the
	 * standalone record keeps PREPARE/COMMIT PREPARED untouched (user boundary).
	 */
	(void)cluster_undo_emit_tt_slot_commit(owner, segment_id, slot_offset, wrap, xid, commit_scn);

	tt_slot_write_committed(segment_id, owner, slot_offset, xid, wrap, commit_scn, NULL);
}

static uint8
tt_slot_durable_terminal_exact(uint32 segment_id, uint32 segment_generation,
							   uint16 slot_offset, TransactionId xid,
							   uint16 wrap, uint8 terminal_status,
							   SCN terminal_scn, bool apply_transition,
							   const ClusterSemanticAdmissionToken *admission,
							   TTSlot *successor_out)
{
	uint8 owner = tt_owner_instance_for_segment(segment_id);
	ClusterTTSlotCurrentOwner expected_owner;
	ClusterUndoBlock0LogicalKey key;
	ClusterUndoBlock0ResolvedRoot root;
	ClusterUndoBlock0ResolvedRoot final_root;
	ClusterUndoBlock0Generation generation = { false, 0 };
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0Pin pin;
	ClusterUndoBlock0CurrentStep step;
	ClusterUndoBlock0Result result = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	ClusterUndoBlock0Result current_failure = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	const ClusterGesTimeoutDetail *ges_failure;
	PGAlignedBlock disk_block;
	UndoSegmentHeaderData *disk_header = (UndoSegmentHeaderData *)disk_block.data;
	UndoSegmentHeaderData *resident_header = NULL;
	ClusterTTTerminalTransitionDecision decision;
	TTSlot successor;
	TTSlot final_expected;
	bool root_available;
	volatile bool current_active = false;
	volatile bool pin_held = false;
	bool target_side;

	Assert(slot_offset < TT_SLOTS_PER_SEGMENT);
	Assert(TransactionIdIsValid(xid));
	Assert((terminal_status == TT_SLOT_COMMITTED) == SCN_VALID(terminal_scn));
	if (admission == NULL || successor_out == NULL || !admission->entered
		|| segment_generation == UINT32_MAX
		|| (terminal_status != TT_SLOT_COMMITTED
			&& terminal_status != TT_SLOT_ABORTED)
		|| (admission->side != CLUSTER_SEMANTIC_SOURCE_SIDE
			&& admission->side != CLUSTER_SEMANTIC_TARGET_SIDE)
		|| cluster_node_id < 0 || owner != (uint8)(cluster_node_id + 1))
		ereport(ERROR,
				(errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
				 errmsg("cannot commit a transaction without local undo block-zero authority")));

	key.segment_id = segment_id;
	key.owner_instance = owner;
	memset(&expected_owner, 0, sizeof(expected_owner));
	expected_owner.segment_id = segment_id;
	expected_owner.xid = xid;
	expected_owner.slot_offset = slot_offset;
	expected_owner.wrap = wrap;
	expected_owner.status = CTS_ACTIVE;
	expected_owner.commit_scn = InvalidScn;
	memset(&root, 0, sizeof(root));
	memset(&final_root, 0, sizeof(final_root));
	memset(&pin, 0, sizeof(pin));
	pin.slot = -1;
	target_side = admission->side == CLUSTER_SEMANTIC_TARGET_SIDE;
	root_available = target_side
		? cluster_semantic_activation_resolve_shared_undo_root(
			  admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, owner, segment_id, &root)
		: cluster_semantic_activation_resolve_shared_undo_root_live_owner_source(
			  admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, owner, segment_id, &root);
	if (!tt_allocator_corroborates_or_rolled_away(&expected_owner))
		ereport(ERROR,
				(errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
				 errmsg("cannot commit a transaction: allocator identity is not exact")));

	step = target_side
		? cluster_undo_block0_current_acquire_begin_live_owner_target(
			  &key, cluster_ges_request_timeout_ms, admission, &guard,
			  &current_failure)
		: cluster_undo_block0_current_acquire_begin_live_owner_source(
			  &key, cluster_ges_request_timeout_ms, admission, &guard,
			  &current_failure);
	if (step == CLUSTER_UNDO_BLOCK0_CURRENT_FAILED) {
		ges_failure = cluster_ges_timeout_detail_get();
		ereport(
			ERROR,
			(errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
			 errmsg(
				 "cannot commit a transaction: undo block-zero current authority is unavailable"),
			 errdetail("PGRAC_FAMILY=BLOCK0_CURRENT PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
					   "PGRAC_NODE=%d PGRAC_ATTEMPT=0 "
					   "segment=%u result=%d source=%s master=%d attempts=%d",
					   cluster_node_id, segment_id, (int)current_failure,
					   cluster_ges_timeout_src_text(ges_failure->source), ges_failure->master_node,
					   ges_failure->attempts)));
	}
	current_active = true;

	/*
	 * spec-3.18 D4.1 (normal commit): write the 32B slot WITHOUT emitting a
	 * standalone 0x30.  The caller (cluster_tt_local_precommit_durable_finish)
	 * folds an equivalent xl_xact_tt_commit delta into the commit record, whose
	 * flush makes both the delta and CLOG durable atomically (one record, no
	 * intermediate stamped-but-uncommitted window).  Redo re-stamps via
	 * cluster_tt_durable_redo_stamp_slot() from xact_redo_commit instead of the
	 * 0x30 redo.  Returns the owner instance so the caller can fill the delta's
	 * path-resolution field.
	 */
	PG_TRY();
	{
		while (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING) {
			CHECK_FOR_INTERRUPTS();
			step = cluster_undo_block0_current_acquire_poll(&guard, &current_failure);
			if (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING
				&& !cluster_undo_block0_current_wait_reply(
					&guard, CLUSTER_UNDO_BLOCK0_WAIT_TT_COMMIT_ACQUIRE))
				pg_usleep(1000L);
		}
		if (step != CLUSTER_UNDO_BLOCK0_CURRENT_HELD || !root_available)
			ereport(
				ERROR,
				(errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
				 errmsg("cannot commit a transaction: undo block-zero current acquisition failed"),
				 errdetail("PGRAC_FAMILY=BLOCK0_CURRENT PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
						   "PGRAC_NODE=%d PGRAC_ATTEMPT=0 "
						   "segment=%u result=%d",
						   cluster_node_id, segment_id, (int)current_failure)));

		result = cluster_undo_block0_current_sample_generation_exclusive(
			&guard, &root, &generation);
		if (result != CLUSTER_UNDO_BLOCK0_OK || !generation.known
			|| generation.value != segment_generation)
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
					 errmsg("cannot commit a transaction: canonical generation changed")));
		result = cluster_undo_block0_current_pin_exclusive(
			&guard, &root, &generation, &pin, (char **)&resident_header);
		if (result != CLUSTER_UNDO_BLOCK0_OK || resident_header == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
					 errmsg("cannot commit a transaction: canonical block-zero content is unavailable")));
		pin_held = true;

		cluster_tt_durable_io_wait_start();
		if (!cluster_undo_smgr_read_block(root.intent, segment_id, owner, 0,
				disk_block.data)) {
			cluster_tt_durable_io_wait_end();
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("cannot read canonical TT block zero for commit")));
		}
		cluster_tt_durable_io_wait_end();
		if (disk_header->segment_id != segment_id
			|| disk_header->owner_instance != owner
			|| disk_header->tt_slots_count != TT_SLOTS_PER_SEGMENT
			|| disk_header->wrap_count != segment_generation
			|| resident_header->segment_id != disk_header->segment_id
			|| resident_header->owner_instance != disk_header->owner_instance
			|| resident_header->tt_slots_count != disk_header->tt_slots_count
			|| resident_header->wrap_count != disk_header->wrap_count
			|| memcmp(&resident_header->tt_slots[slot_offset],
					  &disk_header->tt_slots[slot_offset], sizeof(TTSlot)) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("canonical TT commit identity or generation mismatch")));

		decision = cluster_tt_terminal_transition_decide(
			&disk_header->tt_slots[slot_offset], disk_header->wrap_count,
			segment_generation, xid, wrap, terminal_status, terminal_scn);
		if (decision != CLUSTER_TT_TERMINAL_APPLY
			&& decision != CLUSTER_TT_TERMINAL_IDEMPOTENT)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("canonical TT commit predecessor is not exact")));
		successor = disk_header->tt_slots[slot_offset];
		successor.status = terminal_status;
		successor.flags = TT_FLAGS_RESERVED;
		successor.commit_scn = terminal_scn;
		successor.first_undo_block = InvalidUbaVal;
		final_expected = apply_transition ? successor
			: disk_header->tt_slots[slot_offset];

		memset(&final_root, 0, sizeof(final_root));
		if (!(target_side
				  ? cluster_semantic_activation_resolve_shared_undo_root(
						admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, owner,
						segment_id, &final_root)
				  : cluster_semantic_activation_resolve_shared_undo_root_live_owner_source(
						admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, owner,
						segment_id, &final_root))
			|| !cluster_undo_block0_root_matches(&root, &final_root)
			|| !cluster_semantic_activation_modifier_recheck(admission, true)
			|| cluster_undo_block0_current_recheck_exclusive(&guard)
			   != CLUSTER_UNDO_BLOCK0_OK)
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
					 errmsg("canonical TT commit authority drifted before transition")));

		if (apply_transition && decision == CLUSTER_TT_TERMINAL_APPLY) {
			cluster_tt_durable_io_wait_start();
			if (!cluster_undo_smgr_write_header_bytes(
					root.intent, segment_id, owner, tt_slot_file_offset(slot_offset),
					(const char *)&successor, sizeof(successor))) {
				cluster_tt_durable_io_wait_end();
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("cannot write canonical terminal TT slot")));
			}
			cluster_tt_durable_io_wait_end();
			memcpy(&resident_header->tt_slots[slot_offset], &successor,
				   sizeof(successor));
		}

		memset(&final_root, 0, sizeof(final_root));
		if (!(target_side
				  ? cluster_semantic_activation_resolve_shared_undo_root(
						admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, owner,
						segment_id, &final_root)
				  : cluster_semantic_activation_resolve_shared_undo_root_live_owner_source(
						admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, owner,
						segment_id, &final_root))
			|| !cluster_undo_block0_root_matches(&root, &final_root)
			|| !cluster_semantic_activation_modifier_recheck(admission, true)
			|| cluster_undo_block0_current_recheck_exclusive(&guard)
			   != CLUSTER_UNDO_BLOCK0_OK
			|| resident_header->wrap_count != segment_generation
			|| memcmp(&resident_header->tt_slots[slot_offset], &final_expected,
					  sizeof(successor)) != 0
			|| !tt_allocator_corroborates_or_rolled_away(&expected_owner))
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
					 errmsg("canonical TT commit authority drifted after transition")));

		*successor_out = successor;
	}
	PG_FINALLY();
	{
		if (pin_held) {
			cluster_undo_block0_unpin(&pin);
			pin_held = false;
		}
		if (current_active) {
			/* This is the existing no-wait owned-resource release path.  In
			 * particular, after successor publication it stages reliable remote
			 * release (or drains a local holder) without an interruptible wait
			 * or an ordinary ERROR after the durable bytes changed. */
			cluster_undo_block0_current_cancel(&guard);
			current_active = false;
		}
	}
	PG_END_TRY();
	return owner;
}

uint8
cluster_tt_slot_durable_commit_writeonly(uint32 segment_id,
										 uint32 segment_generation,
										 uint16 slot_offset, TransactionId xid,
										 uint16 wrap, SCN commit_scn,
										 const ClusterSemanticAdmissionToken *admission,
										 TTSlot *successor_out)
{
	uint8 owner;

	owner = tt_slot_durable_terminal_exact(segment_id, segment_generation,
		slot_offset, xid, wrap, TT_SLOT_COMMITTED, commit_scn, true,
		admission, successor_out);
	cluster_tt_durable_count_commit();
	return owner;
}

XLogRecPtr
cluster_tt_slot_durable_abort_exact(uint32 segment_id,
								   uint32 segment_generation,
								   uint16 slot_offset, TransactionId xid,
								   uint16 wrap,
								   const ClusterSemanticAdmissionToken *admission,
								   TTSlot *successor_out)
{
	TTSlot prepared_successor;
	XLogRecPtr abort_lsn;
	uint8 owner;

	owner = tt_slot_durable_terminal_exact(segment_id, segment_generation,
		slot_offset, xid, wrap, TT_SLOT_ABORTED, InvalidScn, false,
		admission, &prepared_successor);
	abort_lsn = cluster_undo_emit_tt_slot_abort_exact(owner, segment_id,
		segment_generation, slot_offset, wrap, xid);
	XLogFlush(abort_lsn);
	(void)tt_slot_durable_terminal_exact(segment_id, segment_generation,
		slot_offset, xid, wrap, TT_SLOT_ABORTED, InvalidScn, true,
		admission, successor_out);
	return abort_lsn;
}


/*
 * cluster_tt_slot_durable_abort -- spec-3.15 D5 (ROLLBACK PREPARED).
 *
 *	Mirror of durable_commit: WAL 0x31 first (the prepared-abort record's
 *	flush carries it, C10), then the 32B targeted RMW stamping
 *	TT_SLOT_ABORTED with xid/wrap preserved and commit_scn cleared (V-2:
 *	identity must survive so by-exact-key lookups resolve ABORTED instead
 *	of missing into 53R97).
 */
void
cluster_tt_slot_durable_abort(uint32 segment_id, uint16 slot_offset, TransactionId xid, uint16 wrap)
{
	uint8 owner = tt_owner_instance_for_segment(segment_id);
	uint32 off = tt_slot_file_offset(slot_offset);
	TTSlot slot;

	Assert(slot_offset < TT_SLOTS_PER_SEGMENT);
	Assert(TransactionIdIsValid(xid));

	(void)cluster_undo_emit_tt_slot_abort(owner, segment_id, slot_offset, wrap, xid);

	cluster_tt_durable_io_wait_start();

	if (!cluster_undo_smgr_read_header_bytes(cluster_undo_intent_for_owner(owner), segment_id,
											 owner, off, (char *)&slot, sizeof(slot))) {
		cluster_tt_durable_io_wait_end();
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster durable TT: cannot read slot %u of undo segment %u",
							   slot_offset, segment_id)));
	}

	slot.xid = xid;
	slot.wrap = wrap;
	slot.status = (uint8)TT_SLOT_ABORTED;
	slot.flags = TT_FLAGS_RESERVED;
	slot.commit_scn = InvalidScn;
	slot.first_undo_block = InvalidUbaVal; /* spec-4.8 D7-A (P1#1): cleared; 0x90 re-attaches */

	if (!cluster_undo_smgr_write_header_bytes(cluster_undo_intent_for_owner(owner), segment_id,
											  owner, off, (const char *)&slot, sizeof(slot))) {
		cluster_tt_durable_io_wait_end();
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster durable TT: cannot write slot %u of undo segment %u",
							   slot_offset, segment_id)));
	}

	cluster_tt_durable_io_wait_end();
}


/*
 * cluster_tt_slot_durable_set_head -- spec-4.8 D7-A.
 *
 *	Durably stamp the undo-chain head (TTSlot.first_undo_block) onto the slot,
 *	gated by the slot still owning (xid, wrap).  WAL 0x90 first (the prepared-
 *	abort flush carries it, C10), then the 32B targeted RMW.  Does NOT touch
 *	slot.status (the paired 0x60 abort already set ABORTED + xid + wrap, so the
 *	identity gate matches here).  A slot recycled to a different owner since the
 *	abort is left untouched (规则 8.A: never stamp another xact's slot).  Called
 *	from the ROLLBACK PREPARED prefinish abort path with the head captured into
 *	the 2PC record at PREPARE, so D7 physical rollback can walk it.
 */
void
cluster_tt_slot_durable_set_head(uint32 segment_id, uint16 slot_offset, TransactionId xid,
								 uint16 wrap, UBA first_undo_block)
{
	uint8 owner = tt_owner_instance_for_segment(segment_id);
	uint32 off = tt_slot_file_offset(slot_offset);
	TTSlot slot;

	Assert(slot_offset < TT_SLOTS_PER_SEGMENT);
	Assert(TransactionIdIsValid(xid));

	(void)cluster_undo_emit_tt_slot_set_head(owner, segment_id, slot_offset, wrap, xid,
											 first_undo_block);

	cluster_tt_durable_io_wait_start();

	if (!cluster_undo_smgr_read_header_bytes(cluster_undo_intent_for_owner(owner), segment_id,
											 owner, off, (char *)&slot, sizeof(slot))) {
		cluster_tt_durable_io_wait_end();
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster durable TT: cannot read slot %u of undo segment %u",
							   slot_offset, segment_id)));
	}

	/* Identity gate (规则 8.A): only stamp the head if the slot still owns this
	 * (xid, wrap); a recycled slot belongs to a newer owner -> leave untouched. */
	if (slot.xid == xid && slot.wrap == wrap) {
		slot.first_undo_block = first_undo_block;
		if (!cluster_undo_smgr_write_header_bytes(cluster_undo_intent_for_owner(owner), segment_id,
												  owner, off, (const char *)&slot, sizeof(slot))) {
			cluster_tt_durable_io_wait_end();
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("cluster durable TT: cannot write slot %u of undo segment %u",
								   slot_offset, segment_id)));
		}
	}

	cluster_tt_durable_io_wait_end();
}


bool
cluster_tt_slot_durable_lookup(uint32 segment_id, uint16 slot_offset, TransactionId xid,
							   uint32 expected_wrap, SCN *commit_scn)
{
	uint8 owner;
	uint32 off;
	TTSlot slot;

	if (commit_scn == NULL || slot_offset >= TT_SLOTS_PER_SEGMENT)
		return false;

	owner = tt_owner_instance_for_segment(segment_id);
	off = tt_slot_file_offset(slot_offset);

	cluster_tt_durable_io_wait_start();
	if (!cluster_undo_smgr_read_header_bytes(cluster_undo_intent_for_owner(owner), segment_id,
											 owner, off, (char *)&slot, sizeof(slot))) {
		cluster_tt_durable_io_wait_end();
		cluster_tt_durable_count_lookup(false);
		return false; /* segment absent / I/O -> miss (caller fail-closes) */
	}
	cluster_tt_durable_io_wait_end();

	/*
	 * spec-3.11 C5 (规则 8.A): the slot must still be bound to this xid and be
	 * COMMITTED with a valid commit_scn.  xid mismatch = the slot was recycled
	 * by a later owner; never return that owner's commit_scn.
	 */
	if (!cluster_tt_durable_slot_match(slot.status, slot.xid, slot.wrap, slot.commit_scn, xid,
									   expected_wrap)) {
		cluster_tt_durable_count_lookup(false);
		return false;
	}

	*commit_scn = slot.commit_scn;
	cluster_tt_durable_count_lookup(true);
	return true;
}

bool
cluster_tt_slot_durable_lookup_committed_stable(uint32 segment_id, uint16 slot_offset,
												TransactionId xid, uint32 expected_wrap,
												ClusterTTDurableXidCommitCheck xid_committed,
												SCN *commit_scn)
{
	uint8 owner;
	uint32 off;
	TTSlot first;
	TTSlot second;

	if (commit_scn == NULL || xid_committed == NULL || slot_offset >= TT_SLOTS_PER_SEGMENT)
		return false;

	owner = tt_owner_instance_for_segment(segment_id);
	off = tt_slot_file_offset(slot_offset);

	cluster_tt_durable_io_wait_start();
	if (!cluster_undo_smgr_read_header_bytes(cluster_undo_intent_for_owner(owner), segment_id,
											 owner, off, (char *)&first, sizeof(first))) {
		cluster_tt_durable_io_wait_end();
		return false;
	}
	cluster_tt_durable_io_wait_end();

	if (!cluster_tt_durable_slot_match(first.status, first.xid, first.wrap, first.commit_scn, xid,
									   expected_wrap)) {
		cluster_tt_durable_count_lookup(false);
		return false;
	}

	if (!xid_committed(xid)) {
		cluster_tt_durable_count_lookup(false);
		return false;
	}

	cluster_tt_durable_io_wait_start();
	if (!cluster_undo_smgr_read_header_bytes(cluster_undo_intent_for_owner(owner), segment_id,
											 owner, off, (char *)&second, sizeof(second))) {
		cluster_tt_durable_io_wait_end();
		cluster_tt_durable_count_lookup(false);
		return false;
	}
	cluster_tt_durable_io_wait_end();

	if (memcmp(&first, &second, sizeof(first)) != 0
		|| !cluster_tt_durable_slot_match(second.status, second.xid, second.wrap, second.commit_scn,
										  xid, expected_wrap)) {
		cluster_tt_durable_count_lookup(false);
		return false;
	}

	*commit_scn = second.commit_scn;
	cluster_tt_durable_count_lookup(true);
	return true;
}

bool
cluster_tt_slot_durable_read_exact_stable(uint32 segment_id, uint16 slot_offset,
										  TransactionId xid, uint16 expected_wrap,
										  TTSlot *slot_out)
{
	uint8 owner;
	uint32 off;
	TTSlot first;
	TTSlot second;

	if (slot_out == NULL)
		return false;
	memset(slot_out, 0, sizeof(*slot_out));

	if (segment_id == 0
		|| segment_id
			   > ((uint32)SCN_MAX_VALID_NODE_ID + 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE
		|| slot_offset >= TT_SLOTS_PER_SEGMENT || !TransactionIdIsNormal(xid)
		|| expected_wrap == TT_WRAP_INVALID)
		return false;

	owner = tt_owner_instance_for_segment(segment_id);
	off = tt_slot_file_offset(slot_offset);

	cluster_tt_durable_io_wait_start();
	if (!cluster_undo_smgr_read_header_bytes(cluster_undo_intent_for_owner(owner), segment_id,
											 owner, off, (char *)&first, sizeof(first))) {
		cluster_tt_durable_io_wait_end();
		cluster_tt_durable_count_lookup(false);
		return false;
	}
	cluster_tt_durable_io_wait_end();

	if (first.status > (uint8)TT_SLOT_RECYCLABLE || first.xid != xid
		|| first.wrap != expected_wrap)
		return false;

	cluster_tt_durable_io_wait_start();
	if (!cluster_undo_smgr_read_header_bytes(cluster_undo_intent_for_owner(owner), segment_id,
											 owner, off, (char *)&second, sizeof(second))) {
		cluster_tt_durable_io_wait_end();
		return false;
	}
	cluster_tt_durable_io_wait_end();

	if (memcmp(&first, &second, sizeof(first)) != 0
		|| second.status > (uint8)TT_SLOT_RECYCLABLE || second.xid != xid
		|| second.wrap != expected_wrap)
		return false;

	*slot_out = second;
	return true;
}


void
cluster_tt_durable_redo_abort_slot(uint8 instance, uint32 segment_id,
								   uint16 slot_offset, uint16 wrap,
								   TransactionId xid)
{
	uint32		off;
	TTSlot		slot;
	ClusterTTRedoDecision decision;

	if (instance == 0 || segment_id == 0 ||
		instance != tt_owner_instance_for_segment(segment_id) ||
		slot_offset >= TT_SLOTS_PER_SEGMENT || !TransactionIdIsNormal(xid) ||
		wrap == TT_WRAP_INVALID)
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid typed TT abort redo identity")));
	off = tt_slot_file_offset(slot_offset);
	cluster_tt_durable_io_wait_start();
	if (!cluster_undo_smgr_read_header_bytes(
			cluster_undo_intent_for_owner(instance), segment_id, instance, off,
			(char *) &slot, sizeof(slot)))
	{
		cluster_tt_durable_io_wait_end();
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cannot read TT slot %u of undo segment %u for abort redo",
					 slot_offset, segment_id)));
	}
	decision = cluster_tt_durable_redo_decide(slot.status, slot.xid,
		slot.wrap, xid, wrap);
	if (decision == CLUSTER_TT_REDO_BADSTATUS)
	{
		cluster_tt_durable_io_wait_end();
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid TT status %u in typed abort redo", slot.status)));
	}
	if (decision == CLUSTER_TT_REDO_SKIP)
	{
		cluster_tt_durable_io_wait_end();
		cluster_vis_bump_recovery_undo_redo_skips();
		return;
	}
	slot.xid = xid;
	slot.wrap = wrap;
	slot.status = TT_SLOT_ABORTED;
	slot.flags = TT_FLAGS_RESERVED;
	slot.commit_scn = InvalidScn;
	slot.first_undo_block = InvalidUbaVal;
	if (!cluster_undo_smgr_write_header_bytes(
			cluster_undo_intent_for_owner(instance), segment_id, instance, off,
			(const char *) &slot, sizeof(slot)) ||
		!cluster_undo_smgr_fsync_segment_file(segment_id, instance))
	{
		cluster_tt_durable_io_wait_end();
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cannot durably write TT slot %u of undo segment %u for abort redo",
					 slot_offset, segment_id)));
	}
	cluster_tt_durable_io_wait_end();
	cluster_vis_bump_recovery_undo_redo_applies();
}

void
cluster_tt_durable_redo_abort_slot_exact(uint8 instance, uint32 segment_id,
										 uint32 segment_generation,
										 uint16 slot_offset, uint16 wrap,
										 TransactionId xid)
{
	PGAlignedBlock block;
	UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)block.data;
	TTSlot successor;
	ClusterTTTerminalTransitionDecision decision;

	if (instance == 0 || segment_id == 0
		|| segment_generation == UINT32_MAX
		|| instance != tt_owner_instance_for_segment(segment_id)
		|| slot_offset >= TT_SLOTS_PER_SEGMENT
		|| !TransactionIdIsNormal(xid) || wrap == TT_WRAP_INVALID)
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid exact TT abort redo identity")));
	cluster_tt_durable_io_wait_start();
	if (!cluster_undo_smgr_read_block(cluster_undo_intent_for_owner(instance),
			segment_id, instance, 0, block.data)) {
		cluster_tt_durable_io_wait_end();
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cannot read undo segment %u for exact abort redo",
						segment_id)));
	}
	if (header->segment_id != segment_id
		|| header->owner_instance != instance
		|| header->tt_slots_count != TT_SLOTS_PER_SEGMENT) {
		cluster_tt_durable_io_wait_end();
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("conflicting undo segment header for exact abort redo")));
	}
	decision = cluster_tt_terminal_transition_decide(
		&header->tt_slots[slot_offset], header->wrap_count,
		segment_generation, xid, wrap, TT_SLOT_ABORTED, InvalidScn);
	if (decision == CLUSTER_TT_TERMINAL_STALE) {
		cluster_tt_durable_io_wait_end();
		cluster_vis_bump_recovery_undo_redo_skips();
		return;
	}
	if (decision == CLUSTER_TT_TERMINAL_IDEMPOTENT) {
		cluster_tt_durable_io_wait_end();
		return;
	}
	if (decision != CLUSTER_TT_TERMINAL_APPLY) {
		cluster_tt_durable_io_wait_end();
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("conflicting canonical ACTIVE predecessor for exact abort redo")));
	}
	successor = header->tt_slots[slot_offset];
	successor.status = TT_SLOT_ABORTED;
	successor.flags = TT_FLAGS_RESERVED;
	successor.commit_scn = InvalidScn;
	successor.first_undo_block = InvalidUbaVal;
	if (!cluster_undo_smgr_write_header_bytes(
			cluster_undo_intent_for_owner(instance), segment_id, instance,
			tt_slot_file_offset(slot_offset), (const char *)&successor,
			sizeof(successor))
		|| !cluster_undo_smgr_fsync_segment_file(segment_id, instance)) {
		cluster_tt_durable_io_wait_end();
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cannot durably write exact ABORTED TT slot")));
	}
	cluster_tt_durable_io_wait_end();
	cluster_vis_bump_recovery_undo_redo_applies();
}


void
cluster_tt_durable_redo_set_head_slot(uint8 instance, uint32 segment_id,
									 uint16 slot_offset, uint16 wrap,
									 TransactionId xid,
									 UBA first_undo_block)
{
	uint32		off;
	TTSlot		slot;

	if (instance == 0 || segment_id == 0 ||
		instance != tt_owner_instance_for_segment(segment_id) ||
		slot_offset >= TT_SLOTS_PER_SEGMENT || !TransactionIdIsNormal(xid) ||
		wrap == TT_WRAP_INVALID || UBA_is_invalid(first_undo_block))
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid typed TT set-head redo identity")));
	off = tt_slot_file_offset(slot_offset);
	cluster_tt_durable_io_wait_start();
	if (!cluster_undo_smgr_read_header_bytes(
			cluster_undo_intent_for_owner(instance), segment_id, instance, off,
			(char *) &slot, sizeof(slot)))
	{
		cluster_tt_durable_io_wait_end();
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cannot read TT slot %u of undo segment %u for set-head redo",
					 slot_offset, segment_id)));
	}
	if (slot.status != TT_SLOT_ABORTED || slot.xid != xid ||
		slot.wrap != wrap)
	{
		cluster_tt_durable_io_wait_end();
		cluster_vis_bump_recovery_undo_redo_skips();
		return;
	}
	slot.first_undo_block = first_undo_block;
	if (!cluster_undo_smgr_write_header_bytes(
			cluster_undo_intent_for_owner(instance), segment_id, instance, off,
			(const char *) &slot, sizeof(slot)) ||
		!cluster_undo_smgr_fsync_segment_file(segment_id, instance))
	{
		cluster_tt_durable_io_wait_end();
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cannot durably write TT slot %u of undo segment %u for set-head redo",
					 slot_offset, segment_id)));
	}
	cluster_tt_durable_io_wait_end();
	cluster_vis_bump_recovery_undo_redo_applies();
}


ClusterTTDurableResolve
cluster_tt_slot_durable_resolve_by_xid(TransactionId xid, uint32 expected_wrap, SCN *commit_scn,
									   uint16 *out_seg, uint16 *out_slot, uint16 *out_wrap)
{
	/* spec-3.22 own-instance entry; the origin-qualified scan backs it.
	 * spec-5.55 D1: forward the optional match-identity out params. */
	return cluster_tt_slot_durable_resolve_by_xid_origin(cluster_node_id, xid, expected_wrap,
														 commit_scn, out_seg, out_slot, out_wrap);
}


/*
 * cluster_tt_recovery_classify_liveness -- spec-4.8 D1 pure classifier.
 *
 *	Maps the (determinable, did_commit, is_in_progress) facts about a crash-
 *	left ACTIVE slot's owning xid to a liveness verdict.  No I/O, no shmem --
 *	unit-tested truth table (test_cluster_tt_durable).  Precedence (规则 8.A):
 *	  - !determinable  -> AMBIGUOUS (fail-closed -> the caller aborts the slot);
 *	  - did_commit     -> LIVE (never abort a committed xact; an ACTIVE slot for
 *	                     a committed xid is a lost commit_scn stamp, not an abort);
 *	  - is_in_progress -> LIVE (a resurrected prepared 2PC xact still in flight);
 *	  - otherwise      -> DEAD (an in-flight-at-crash, non-prepared xact -> abort).
 *
 *	did_commit takes precedence over is_in_progress: a committed xact is never
 *	"in progress" post-recovery, but the ordering makes the fail-safe explicit.
 */
ClusterTtRecoveryLiveness
cluster_tt_recovery_classify_liveness(bool determinable, bool did_commit, bool is_in_progress)
{
	if (!determinable)
		return CLUSTER_TT_RECOVERY_AMBIGUOUS;
	if (did_commit)
		return CLUSTER_TT_RECOVERY_LIVE;
	if (is_in_progress)
		return CLUSTER_TT_RECOVERY_LIVE;
	return CLUSTER_TT_RECOVERY_DEAD;
}


/*
 * cluster_tt_recovery_remote_authority_covers -- spec-4.8 D2 pure gate.
 *
 *	True iff a survivor may trust a crashed-and-materialized origin's durable TT
 *	outcome for a tuple whose page LSN is `anchor_lsn`.  is_materialized (the
 *	4.5a G6 bool gate, checked by the caller) only proves a merge marker was
 *	published; this LSN gate (4.7 D5 / Q5 lesson) additionally requires the
 *	origin's recovery to have reconciled THROUGH the tuple's page version.  If
 *	the page LSN is beyond recovered_through, the page carries a version the
 *	origin's redo has not reached -- the durable outcome (COMMITTED or ABORTED)
 *	is untrustworthy and the caller must fail closed (规则 8.A).
 *
 *	anchor_lsn == 0 (InvalidXLogRecPtr -- an unwritten page) skips the LSN gate
 *	(is_materialized-only, pre-D2 behaviour).  Pure; no I/O; unit-tested.
 */
bool
cluster_tt_recovery_remote_authority_covers(uint64 recovered_through, uint64 anchor_lsn)
{
	if (anchor_lsn == 0)
		return true;
	return recovered_through >= anchor_lsn;
}


/*
 * cluster_tt_recovery_wrap_suspect -- spec-4.8 D3 pure gate (task#90).
 *
 *	A WRAP_ANY by-xid resolve that found exactly one COMMITTED match cannot tell
 *	a genuine commit from a 2^32-wrapped raw-xid collision (no generation key to
 *	compare).  Returns true (the 1-match is wrap-suspect; the caller must fail
 *	closed -- a narrowed AMBIGUOUS_WRAP -- never resolve to its commit_scn) iff:
 *	  - the resolve had no generation expectation (expected_wrap == WRAP_ANY;
 *	    a wrap-checked caller is already disambiguated -> never suspect), AND
 *	  - retention is NOT reliable (retention_reliable == false), AND
 *	  - the matched commit_scn is strictly below the retention horizon, OR the
 *	    horizon/scn cannot be judged (fail-closed under unreliable retention).
 *
 *	Why the retention_reliable short-circuit (规则 8.A + healthy-op liveness):
 *	with retention reliable, a below-horizon COMMITTED slot is recycled
 *	(spec-3.12), so a 2^32-wrapped collision's old slot is already gone (the
 *	resolve sees 0-match, not this 1-match) and a surviving below-horizon
 *	1-match is a LEGIT recent commit in the recycle-lag window -> trusting it
 *	avoids a spurious 53R9F in healthy operation.  Only when retention is
 *	unreliable (sticky retention_off_recycle_count > 0, mirroring spec-3.22)
 *	can a long-unrecycled wrapped collision survive -> a below-horizon 1-match
 *	is then genuinely ambiguous -> fail closed.  Pure; no I/O; unit-tested.
 */
bool
cluster_tt_recovery_wrap_suspect(uint32 expected_wrap, SCN matched_scn, SCN horizon,
								 bool retention_reliable)
{
	if (expected_wrap != CLUSTER_TT_WRAP_ANY)
		return false;
	if (retention_reliable)
		return false;
	if (!SCN_VALID(horizon) || !SCN_VALID(matched_scn))
		return true; /* unreliable retention + unjudgeable -> fail closed */
	return scn_time_cmp(matched_scn, horizon) < 0;
}


/*
 * cluster_tt_recovery_classify_revert -- spec-4.8 D7 pure gate (index-aware
 *	physical rollback safety matrix; user-approved mini-plan v2).
 *
 *	Decides whether an ABORTED xact's undo record may be PHYSICALLY reverted on
 *	the real heap during recovery WITHOUT an index operation.  Only a DELETE
 *	inverse is index-safe: PG never removes index entries on DELETE (they stay
 *	until vacuum), so clearing the aborted deleter's xmax (restoring the tuple
 *	to live) leaves every existing index entry valid -- no index op needed.
 *	INSERT / UPDATE revert would have to REMOVE index entries, and PG's index AM
 *	has no synchronous per-entry point-delete (only ambulkdelete via vacuum +
 *	lazy kill_prior_tuple), so they cannot be index-safely closed -> fail closed
 *	(the tuple stays; xmin / new-version aborted -> MVCC-invisible; vacuum
 *	reclaims heap+index together -- the I10 fallback).  Never produces a dangling
 *	index entry.  NOT a full Oracle SMON rollback (closure must say so).
 *
 *	Gates (规则 8.A): only a DELETE record; only a genuinely-aborted deleter;
 *	the tuple must still carry exactly this deleter's xmax (identity).  Idempotent:
 *	a tuple whose xmax is already clear is done -> SKIP.  Pure; no I/O; unit-tested.
 */
ClusterTtRecoveryRevertVerdict
cluster_tt_recovery_classify_revert(bool is_delete_record, bool record_xid_aborted,
									bool tuple_xmax_matches, bool tuple_xmax_already_clear)
{
	if (!is_delete_record)
		return CLUSTER_TT_REVERT_FAILCLOSED; /* INSERT/UPDATE: index-unsafe */
	if (!record_xid_aborted)
		return CLUSTER_TT_REVERT_FAILCLOSED; /* only revert an aborted deleter */
	if (tuple_xmax_already_clear)
		return CLUSTER_TT_REVERT_SKIP_DONE; /* idempotent: already reverted */
	if (!tuple_xmax_matches)
		return CLUSTER_TT_REVERT_FAILCLOSED; /* identity gate: tuple no longer this deleter's */
	return CLUSTER_TT_REVERT_APPLY;
}

/*
 * cluster_tt_slot_durable_resolve_by_xid_origin -- spec-4.5a G6 (P1 #2): the
 * origin-qualified durable by-xid scan.  A materialized foreign read cannot
 * derive the durable slot offset from the live/CR-image ITL slot (the 8-slot
 * heap cache is reused, so the tuple's slot may point at a NEWER xact's
 * durable slot -- the spec-3.11 offset path is unreliable here).  Scan the
 * origin's whole segment range for COMMITTED slots owning (xid, expected_wrap)
 * instead: exactly one resolved match is the authority, anything else (0 /
 * >1 / unstamped / incomplete scan) fails closed at the caller.
 */
ClusterTTDurableResolve
cluster_tt_slot_durable_resolve_by_xid_origin(int origin_node, TransactionId xid,
											  uint32 expected_wrap, SCN *commit_scn,
											  uint16 *out_seg, uint16 *out_slot, uint16 *out_wrap)
{
	int node;
	uint8 owner;
	uint32 seg_lo;
	uint32 seg_hi;
	uint32 segment_id;
	PGAlignedBlock blockbuf;
	int xid_matches = 0;
	bool match_has_valid_scn = false;
	bool scan_complete = true;
	SCN found = InvalidScn;
	/* spec-5.55 D1: identity of the resolved match (only meaningful on RESOLVED_SCN,
	 * which the classifier guarantees is exactly one valid-scn match). */
	uint16 matched_seg = 0;
	uint16 matched_slot = 0;
	uint16 matched_wrap = 0;
	ClusterTTDurableResolve result;

	if (commit_scn == NULL)
		return CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE; /* programming error: fail-closed */
	*commit_scn = InvalidScn;
	if (!TransactionIdIsNormal(xid))
		return CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE;
	if (origin_node < 0)
		return CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE; /* single-node degraded: no scan */

	node = origin_node;
	owner = (uint8)(node + 1);
	seg_lo = (uint32)node * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	seg_hi = seg_lo + CLUSTER_UNDO_SEGS_PER_INSTANCE - 1;

	cluster_tt_durable_count_by_xid_scan();

	/*
	 * spec-3.22: scan the local node's segment headers for COMMITTED slots owned
	 * by xid, counting them INDEPENDENT of commit_scn validity (§2.4).  This is
	 * the soundness split the xmax gate needs:
	 *   - 0 xid-matches after a COMPLETE scan = the slot was recycled to a new
	 *     owner; spec-3.12 only recycles a COMMITTED slot once its commit_scn is
	 *     strictly below the retention horizon, so a 0-match is provably below
	 *     horizon (the caller's retention proof then turns it INVISIBLE);
	 *   - 1 xid-match with an UNSTAMPED commit_scn = a delayed-cleanout slot that
	 *     is RETAINED (not recycled), so it is NOT below horizon -- it must stay
	 *     fail-closed, never be conflated with a 0-match;
	 *   - >1 = raw-xid wrap residue (ambiguous);
	 *   - an EXISTING but unreadable segment makes the scan incomplete, so a
	 *     0-match cannot be trusted (规则 8.A) -> SCAN_UNAVAILABLE.
	 *
	 * Distinguishing "segment absent" (sound skip) from "existing but unreadable"
	 * (incomplete scan): cluster_undo_smgr_read_block returns false for both, so
	 * on a miss we probe cluster_undo_segment_file_exists().  Cost is O(local
	 * segments) as before; spec-3.13's xid index is the scan-cost optimization
	 * (§6 R4), not a correctness change.
	 */
	cluster_tt_durable_io_wait_start();
	for (segment_id = seg_lo; segment_id <= seg_hi; segment_id++) {
		UndoSegmentHeaderData *hdr;
		uint16 i;

		if (!cluster_undo_smgr_read_block(cluster_undo_intent_for_owner(owner), segment_id, owner,
										  0, blockbuf.data)) {
			/* absent segment -> sound skip; existing+unreadable -> incomplete. */
			if (cluster_undo_segment_file_exists(owner, segment_id))
				scan_complete = false;
			continue;
		}

		hdr = (UndoSegmentHeaderData *)blockbuf.data;
		for (i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
			const TTSlot *s = &hdr->tt_slots[i];

			/* spec-4.5a G4 (F3): a known expected_wrap excludes a slot recycled
			 * to a same-valued xid of a NEWER generation -- that exclusion turns
			 * the dangerous false 1-match into a sound 0-match (RECYCLED, the
			 * retention theorem then applies).  WRAP_ANY = pre-4.5a behaviour. */
			if (s->status == (uint8)TT_SLOT_COMMITTED && s->xid == xid
				&& (expected_wrap == CLUSTER_TT_WRAP_ANY || s->wrap == (uint16)expected_wrap)) {
				xid_matches++;
				if (SCN_VALID(s->commit_scn)) {
					match_has_valid_scn = true;
					found = s->commit_scn;
					/* spec-5.55 D1: capture the match identity.  segment_id is
					 * bounded by CLUSTER_MAX_NODES * CLUSTER_UNDO_SEGS_PER_INSTANCE
					 * = 32768 < UINT16_MAX (cluster_undo_alloc.h), so the uint16
					 * cast never truncates.  On >1 matches the classifier returns
					 * AMBIGUOUS_WRAP and these are NOT reported (out_* untouched
					 * unless RESOLVED_SCN). */
					matched_seg = (uint16)segment_id;
					matched_slot = i;
					matched_wrap = s->wrap;
				}
			}
		}
	}
	cluster_tt_durable_io_wait_end();

	result = cluster_tt_durable_classify(xid_matches, match_has_valid_scn, scan_complete);
	if (result == CLUSTER_TT_DURABLE_RESOLVED_SCN) {
		*commit_scn = found;
		if (out_seg != NULL)
			*out_seg = matched_seg;
		if (out_slot != NULL)
			*out_slot = matched_slot;
		if (out_wrap != NULL)
			*out_wrap = matched_wrap;
	}
	return result;
}

ClusterTTDurableLocate
cluster_tt_slot_durable_locate_any_by_xid_origin(int origin_node,
	TransactionId xid, uint16 *out_seg, uint16 *out_slot,
	uint16 *out_wrap, uint8 *out_status)
{
	uint8		owner;
	uint32		seg_lo;
	uint32		seg_hi;
	uint32		segment_id;
	PGAlignedBlock blockbuf;
	uint32		matches = 0;
	bool		scan_complete = true;
	uint16		matched_seg = 0;
	uint16		matched_slot = 0;
	uint16		matched_wrap = 0;
	uint8		matched_status = TT_SLOT_INVALID;

	if (origin_node < 0 || !TransactionIdIsNormal(xid))
		return CLUSTER_TT_DURABLE_LOCATE_SCAN_UNAVAILABLE;
	owner = (uint8) (origin_node + 1);
	seg_lo = (uint32) origin_node * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	seg_hi = seg_lo + CLUSTER_UNDO_SEGS_PER_INSTANCE - 1;
	cluster_tt_durable_count_by_xid_scan();
	cluster_tt_durable_io_wait_start();
	for (segment_id = seg_lo; segment_id <= seg_hi; segment_id++)
	{
		const UndoSegmentHeaderData *header;
		uint16 i;

		if (!cluster_undo_smgr_read_block(cluster_undo_intent_for_owner(owner),
				segment_id, owner, 0, blockbuf.data))
		{
			if (cluster_undo_segment_file_exists(owner, segment_id))
				scan_complete = false;
			continue;
		}
		header = (const UndoSegmentHeaderData *) blockbuf.data;
		for (i = 0; i < TT_SLOTS_PER_SEGMENT; i++)
		{
			const TTSlot *slot = &header->tt_slots[i];

			if (slot->xid != xid ||
				(slot->status != TT_SLOT_ACTIVE &&
				 slot->status != TT_SLOT_COMMITTED &&
				 slot->status != TT_SLOT_ABORTED))
				continue;
			matches++;
			matched_seg = (uint16) segment_id;
			matched_slot = i;
			matched_wrap = slot->wrap;
			matched_status = slot->status;
		}
	}
	cluster_tt_durable_io_wait_end();
	if (matches > 1)
		return CLUSTER_TT_DURABLE_LOCATE_AMBIGUOUS;
	if (!scan_complete)
		return CLUSTER_TT_DURABLE_LOCATE_SCAN_UNAVAILABLE;
	if (matches == 0)
		return CLUSTER_TT_DURABLE_LOCATE_MISSING;
	if (out_seg != NULL)
		*out_seg = matched_seg;
	if (out_slot != NULL)
		*out_slot = matched_slot;
	if (out_wrap != NULL)
		*out_wrap = matched_wrap;
	if (out_status != NULL)
		*out_status = matched_status;
	return CLUSTER_TT_DURABLE_LOCATE_FOUND;
}


bool
cluster_tt_slot_durable_lookup_by_xid(TransactionId xid, SCN *commit_scn)
{
	/*
	 * spec-3.22: thin wrapper preserving the spec-3.11 xmin-side binary contract
	 * (true IFF exactly one resolved match; every other enum -> false).  The
	 * xmax-side gate (spec-3.22) consumes the enum directly via resolve_by_xid.
	 */
	return cluster_tt_slot_durable_resolve_by_xid(xid, CLUSTER_TT_WRAP_ANY, commit_scn, NULL, NULL,
												  NULL)
		   == CLUSTER_TT_DURABLE_RESOLVED_SCN;
}


/*
 * cluster_undo_segment_tt_header_scan_pass -- spec-3.13 D2-B (v0.3
 * scan-only).
 *
 *	READ-ONLY classification of one segment's durable TTSlot[] (block 0
 *	@ offset 112, 48 x 32B).  Produces inventory counts for the segment-
 *	level evaluation (D3) and observability (D6); deliberately writes
 *	NOTHING:
 *	  - rewriting COMMITTED -> RECYCLABLE has zero effect on the segment
 *	    predicate (it only watermarks TT_SLOT_COMMITTED), and
 *	  - it would break cluster_tt_durable_slot_match (COMMITTED-only),
 *	    degrading old unresolved-ITL readers' by-xid resolve to 53R97 —
 *	    worse than the 3.12 lazy status quo.  (spec-3.13 v0.3 ③)
 *
 *	Classification mirror of the shmem predicate, typed for on-disk
 *	TT_SLOT_* (C-R1: shared comparison semantics — strict < horizon,
 *	UNKNOWN retains — without mixing the CTS_* / TT_SLOT_* enums):
 *	  TT_SLOT_COMMITTED + valid scn < horizon  -> below_horizon++
 *	  TT_SLOT_COMMITTED + invalid scn          -> unresolved++ (8.A retain)
 *	  TT_SLOT_ACTIVE                           -> stale_active_skipped++ (HC6)
 *	  UNUSED / ABORTED / RECYCLABLE            -> no inventory impact
 */
bool
cluster_undo_segment_tt_header_scan_pass(uint32 segment_id, uint8 owner_instance, SCN horizon,
										 ClusterUndoCleanerPassStats *stats)
{
	PGAlignedBlock block;
	const UndoSegmentHeaderData *hdr;
	int i;

	Assert(stats != NULL);

	/* Whole-block read mirrors the by-xid scan shape (one smgr surface). */
	cluster_undo_cleaner_scan_wait_start();
	if (!cluster_undo_smgr_read_block(cluster_undo_intent_for_owner(owner_instance), segment_id,
									  owner_instance, 0, block.data)) {
		cluster_undo_cleaner_scan_wait_end();
		return false; /* absent / I/O: caller counts and moves on */
	}
	hdr = (const UndoSegmentHeaderData *)block.data;
	cluster_undo_cleaner_scan_wait_end();

	for (i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		const TTSlot *s = &hdr->tt_slots[i];

		switch (s->status) {
		case TT_SLOT_COMMITTED:
			if (!SCN_VALID(s->commit_scn))
				stats->header_unresolved_committed++;
			else if (SCN_VALID(horizon) && scn_time_cmp(s->commit_scn, horizon) < 0)
				stats->header_tt_slots_below_horizon++;
			else
				stats->header_retained_committed++; /* at/above horizon: pinned signal */
			break;
		case TT_SLOT_ACTIVE:
			stats->stale_active_skipped++; /* HC6: never judged, only counted */
			break;
		default:
			break; /* UNUSED / ABORTED / RECYCLABLE: no inventory impact */
		}
	}

	stats->segments_scanned++;
	return true;
}
