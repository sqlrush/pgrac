/*-------------------------------------------------------------------------
 *
 * cluster_wal_retained_side.c
 *	  SIDE and native-record classification of the retention lower census
 *	  (S07): which history SIDE records an obligation still needs.
 *
 *	  Each SIDE owner class of a record is either keyed or kept by class:
 *
 *	  - UNDO_HEADER (the segment header and its TT slots): keyed by TT slot
 *	    for the TT transitions, the TT delta of a COMMIT and the bindings of
 *	    a PREPARE, and by segment for INIT, REUSE and RECYCLE.  An obligation
 *	    on a slot needs every history record of that slot and of its segment
 *	    header: the TT transitions carry no version the census can compare,
 *	    and block 0 is not made durable by the checkpoint (its TT writes are
 *	    not fsynced), so its before-state may survive only in this WAL.
 *	    INIT and REUSE carry the whole header and need nothing older.
 *	  - UNDO_BLOCK (undo data blocks): keyed by block and by segment.  A
 *	    full-image obligation (a block write with its full page, INIT,
 *	    REUSE) needs nothing; a delta needs every history record of its
 *	    block and of its segment.
 *	  - PREPARED: tracked by transaction.  A history PREPARE is needed until
 *	    a history COMMIT/ABORT PREPARED resolves it: its pending state is
 *	    restored from it.  An obligation COMMIT/ABORT PREPARED leaves it
 *	    unresolved, so it is kept.  Up to RETAINED_PREPARED_MAX are tracked
 *	    exactly; beyond, all PREPARED history is kept.
 *	  - TERMINAL, CLOG, MULTIXACT and COMMIT_TS need no history: a history
 *	    record's effect is durable at its source's completion (each origin's
 *	    pg_xact and pg_multixact are flushed by its checkpoint, the remote
 *	    terminal store fsyncs every transition, commit timestamps are not
 *	    supported in shared mode), and no obligation reads an older record
 *	    of these classes (terminal entries are set per (origin, xid); the
 *	    SLRU records are page initialisations and truncations).
 *
 *	  A record whose keys cannot be derived (its decoder refuses it, or the
 *	  TT effect of a COMMIT/ABORT PREPARED, whose slots are in its PREPARE)
 *	  is unkeyed: an unkeyed obligation keeps all history of its class, an
 *	  unkeyed history record is kept whenever its class has any obligation.
 *	  SIDE keys share the page sketch under their own hash; a collision only
 *	  keeps more.
 *
 *	  Native records the typed SIDE decoder does not own yet are classified
 *	  from their own format instead of abandoning the census: a native SMGR
 *	  CREATE needs nothing older, a native SMGR TRUNCATE ends an incarnation,
 *	  standalone invalidations and XID assignments need nothing, and a
 *	  transaction end carries its SIDE owner classes plus, with relations to
 *	  drop, the structure pin.  A transaction end whose sections do not lie
 *	  within its data is damaged, not unsupported, and refuses the census
 *	  (COMPONENT_INCOMPLETE).
 *
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_wal_retained_side.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-03-shared-wal-and-checkpoint.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/rmgr.h"
#include "access/xact.h"
#include "access/xlogreader.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_side_online_plan.h"
#include "cluster/cluster_side_undo.h"
#include "cluster/cluster_side_xact.h"
#include "common/hashfn.h"
#include "cluster_wal_retained_cut_internal.h"

/* SIDE key kinds, hashed apart from pages and from each other. */
#define RETAINED_KEY_TT_SLOT 1
#define RETAINED_KEY_UNDO_SEGMENT 2
#define RETAINED_KEY_UNDO_BLOCK 3

#define RETAINED_UNDO_SIDE (RF_SIDE_CONTRIBUTION_UNDO_HEADER | RF_SIDE_CONTRIBUTION_UNDO_BLOCK)

/* Index of PREPARED in the per-class arrays. */
#define RETAINED_CLASS_PREPARED 4
StaticAssertDecl(RF_SIDE_CONTRIBUTION_PREPARED == (1u << RETAINED_CLASS_PREPARED),
				 "PREPARED class index matches its owner bit");

/* SIDE keys have no incarnation; every one uses this. */
static const uint8 retained_side_incarnation[16];

static uint64
retained_side_key(uint32 kind, uint32 a, uint32 b)
{
	uint32 words[3];

	words[0] = kind;
	words[1] = a;
	words[2] = b;
	return hash_bytes_extended((const unsigned char *)words, sizeof(words), UINT64CONST(0x5318));
}

/* A SIDE key has no version the census can compare: an obligation on it is
 * a chain start and keeps every history record of the key. */
static void
retained_side_key_seen(RetainedCutWork *work, uint32 kind, uint32 a, uint32 b, uint32 side)
{
	retained_key_seen(work, retained_side_key(kind, a, b), InvalidScn, retained_side_incarnation, 1,
					  retained_side_incarnation, side);
}

/* A TT slot transition; as an obligation it also needs its segment header. */
static void
retained_side_tt_slot(RetainedCutWork *work, uint32 segment, uint16 slot)
{
	retained_side_key_seen(work, RETAINED_KEY_TT_SLOT, segment, slot,
						   RF_SIDE_CONTRIBUTION_UNDO_HEADER);
	if (!work->current_history)
		retained_side_key_seen(work, RETAINED_KEY_UNDO_SEGMENT, segment, 0,
							   RF_SIDE_CONTRIBUTION_UNDO_HEADER);
}

/* An undo data block write; only a delta obligation needs older records. */
static void
retained_side_undo_block(RetainedCutWork *work, uint32 segment, uint32 block, bool full_image)
{
	if (work->current_history)
		retained_side_key_seen(work, RETAINED_KEY_UNDO_BLOCK, segment, block,
							   RF_SIDE_CONTRIBUTION_UNDO_BLOCK);
	else if (!full_image) {
		retained_side_key_seen(work, RETAINED_KEY_UNDO_BLOCK, segment, block,
							   RF_SIDE_CONTRIBUTION_UNDO_BLOCK);
		retained_side_key_seen(work, RETAINED_KEY_UNDO_SEGMENT, segment, 0,
							   RF_SIDE_CONTRIBUTION_UNDO_BLOCK);
	}
}

/* A PREPARE: in history it is needed while unresolved (see the fold). */
static void
retained_side_prepare(RetainedCutWork *work, TransactionId xid)
{
	if (!work->current_history)
		return; /* the whole prepared state: needs nothing older */
	if (work->nprepares == RETAINED_PREPARED_MAX) {
		work->prepared_overflow = true;
		return;
	}
	work->prepares[work->nprepares].xid = xid;
	work->prepares[work->nprepares].source = work->current_source;
	work->prepares[work->nprepares].read_ptr = work->current_read;
	work->nprepares++;
}

/* A COMMIT/ABORT PREPARED in history resolves its PREPARE.  As an
 * obligation it resolves nothing, so its PREPARE stays unresolved and kept. */
static void
retained_side_resolve(RetainedCutWork *work, TransactionId xid)
{
	if (!work->current_history)
		return;
	if (work->nresolved == RETAINED_PREPARED_MAX) {
		work->prepared_overflow = true;
		return;
	}
	work->resolved[work->nresolved++] = xid;
}

/* Record the owner classes of the current record, and which were keyed. */
static void
retained_side_note(RetainedCutWork *work, uint32 owners, uint32 keyed)
{
	RetainedSource *source = &work->sources[work->current_source];
	uint32 unkeyed = owners & ~keyed;

	if (!work->current_history) {
		work->obligation_side |= owners;
		work->obligation_unkeyed |= unkeyed;
		return;
	}
	for (int c = 0; c < RETAINED_SIDE_CLASSES; c++) {
		if ((owners & (1u << c)) != 0
			&& (source->side_first[c] == InvalidXLogRecPtr
				|| work->current_read < source->side_first[c]))
			source->side_first[c] = work->current_read;
		if ((unkeyed & (1u << c)) != 0
			&& (source->side_unkeyed[c] == InvalidXLogRecPtr
				|| work->current_read < source->side_unkeyed[c]))
			source->side_unkeyed[c] = work->current_read;
	}
}

/* Keys of an RM_CLUSTER_UNDO record; returns the owner classes keyed. */
static uint32
retained_side_undo(RetainedCutWork *work, XLogReaderState *record, uint32 owners)
{
	ClusterUndoDecoded undo;

	if (!cluster_undo_decode(record, &undo))
		return 0;
	switch (undo.kind) {
	case CLUSTER_UNDO_KIND_TT_BIND:
	case CLUSTER_UNDO_KIND_TT_COMMIT:
	case CLUSTER_UNDO_KIND_TT_ABORT:
	case CLUSTER_UNDO_KIND_TT_SET_HEAD:
	case CLUSTER_UNDO_KIND_TT_CTRC_RELEASE:
		retained_side_tt_slot(work, undo.segment_id, undo.slot_offset);
		return owners & RF_SIDE_CONTRIBUTION_UNDO_HEADER;
	case CLUSTER_UNDO_KIND_SEGMENT_RECYCLE:
		retained_side_key_seen(work, RETAINED_KEY_UNDO_SEGMENT, undo.segment_id, 0,
							   RF_SIDE_CONTRIBUTION_UNDO_HEADER);
		return owners & RF_SIDE_CONTRIBUTION_UNDO_HEADER;
	case CLUSTER_UNDO_KIND_SEGMENT_INIT:
	case CLUSTER_UNDO_KIND_SEGMENT_REUSE:
		/* The whole header image: needs nothing older. */
		if (work->current_history)
			retained_side_key_seen(work, RETAINED_KEY_UNDO_SEGMENT, undo.segment_id, 0,
								   RETAINED_UNDO_SIDE);
		return owners & RETAINED_UNDO_SIDE;
	case CLUSTER_UNDO_KIND_BLOCK_WRITE:
	case CLUSTER_UNDO_KIND_BLOCK_WRITE_MULTI:
		retained_side_undo_block(work, undo.segment_id, undo.block_no, undo.has_fpi);
		return owners & RF_SIDE_CONTRIBUTION_UNDO_BLOCK;
	default:
		return 0;
	}
}

/* Keys of a typed transaction end or PREPARE; returns the classes keyed. */
static uint32
retained_side_xact(RetainedCutWork *work, XLogReaderState *record,
				   const ClusterWalSourceRef *source, const RfContributorStreamCutV1 *cut,
				   uint32 owners)
{
	RfSideXactOperationV1 *xact = &work->xact;
	uint32 keyed = 0;

	if (!rf_side_xact_decode_v1(record, source->claim.identity.system_identifier,
								cut->failed_thread, xact))
		return 0;
	switch (xact->kind) {
	case RF_SIDE_XACT_COMMIT:
		if (xact->has_tt_delta) {
			retained_side_tt_slot(work, xact->tt_delta.segment_id, xact->tt_delta.slot_offset);
			keyed = RF_SIDE_CONTRIBUTION_UNDO_HEADER;
		}
		break;
	case RF_SIDE_XACT_PREPARE:
		for (uint16 i = 0; i < xact->prepared_binding_count; i++)
			retained_side_tt_slot(work, xact->prepared_bindings[i].undo_segment_id,
								  xact->prepared_bindings[i].slot_offset);
		retained_side_prepare(work, xact->xid);
		keyed = RF_SIDE_CONTRIBUTION_UNDO_HEADER | RF_SIDE_CONTRIBUTION_PREPARED;
		break;
	case RF_SIDE_XACT_COMMIT_PREPARED:
	case RF_SIDE_XACT_ABORT_PREPARED:
		retained_side_resolve(work, xact->xid);
		keyed = RF_SIDE_CONTRIBUTION_PREPARED;
		break;
	default:
		break;
	}
	return owners & keyed;
}

/* The SIDE owner classes the typed decoder reported for a record. */
void
retained_side_record(RetainedCutWork *work, XLogReaderState *record,
					 const ClusterWalSourceRef *source, const RfContributorStreamCutV1 *cut,
					 uint32 owners)
{
	uint32 keyed = 0;

	owners &= RETAINED_SIDE_ANCESTRY;
	if (owners == 0)
		return;
	if (XLogRecGetRmid(record) == RM_CLUSTER_UNDO_ID)
		keyed = retained_side_undo(work, record, owners);
	else if (XLogRecGetRmid(record) == RM_XACT_ID)
		keyed = retained_side_xact(work, record, source, cut, owners);
	retained_side_note(work, owners, keyed);
}

/*
 * A record the page route accepted but the typed SIDE decoder refused,
 * classified from its native format (see the file header).  Records with
 * block references, and every other refusal, stay refused.
 */
RfPageProofDetailV1
retained_native_record(RetainedCutWork *work, XLogReaderState *record, RfPageProofDetailV1 refused)
{
	uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	uint32 owners = 0;
	uint32 keyed = 0;
	bool drops = false;

	if ((refused != RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE
		 && refused != RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED)
		|| XLogRecHasAnyBlockRefs(record))
		return refused;
	if (XLogRecGetRmid(record) == RM_SMGR_ID) {
		if (info == XLOG_SMGR_CREATE)
			return RF_PAGE_PROOF_DETAIL_OK;
		if (info != XLOG_SMGR_TRUNCATE)
			return refused;
		retained_structure(work);
		return RF_PAGE_PROOF_DETAIL_OK;
	}
	if (XLogRecGetRmid(record) != RM_XACT_ID)
		return refused;
	switch (info & XLOG_XACT_OPMASK) {
	case XLOG_XACT_INVALIDATIONS:
	case XLOG_XACT_ASSIGNMENT:
		return RF_PAGE_PROOF_DETAIL_OK;
	case XLOG_XACT_COMMIT:
	case XLOG_XACT_COMMIT_PREPARED: {
		xl_xact_parsed_commit parsed;

		/* Bounded: ParseCommitRecord checks every section against the
		 * main data length first. */
		if (!ParseCommitRecord(XLogRecGetInfo(record), (xl_xact_commit *)XLogRecGetData(record),
							   XLogRecGetDataLen(record), &parsed))
			return RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;
		owners = RF_SIDE_CONTRIBUTION_UNDO_HEADER | RF_SIDE_CONTRIBUTION_TERMINAL;
		if ((info & XLOG_XACT_OPMASK) == XLOG_XACT_COMMIT_PREPARED) {
			owners |= RF_SIDE_CONTRIBUTION_PREPARED;
			retained_side_resolve(work, parsed.twophase_xid);
			keyed = RF_SIDE_CONTRIBUTION_PREPARED;
		} else if (parsed.has_tt_commit) {
			retained_side_tt_slot(work, parsed.tt_commit.segment_id, parsed.tt_commit.slot_offset);
			keyed = RF_SIDE_CONTRIBUTION_UNDO_HEADER;
		}
		drops = parsed.nrels > 0 || parsed.nspace_drops > 0;
		break;
	}
	case XLOG_XACT_ABORT:
	case XLOG_XACT_ABORT_PREPARED: {
		xl_xact_parsed_abort parsed;

		/* ParseAbortRecord trusts the xinfo it reads: a damaged record
		 * refuses the census, it does not become retention evidence. */
		if (!rf_side_xact_completion_shape_v1(record, false))
			return RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;
		ParseAbortRecord(XLogRecGetInfo(record), (xl_xact_abort *)XLogRecGetData(record), &parsed);
		owners = RF_SIDE_CONTRIBUTION_TERMINAL;
		if ((info & XLOG_XACT_OPMASK) == XLOG_XACT_ABORT_PREPARED) {
			owners |= RF_SIDE_CONTRIBUTION_UNDO_HEADER | RF_SIDE_CONTRIBUTION_PREPARED;
			retained_side_resolve(work, parsed.twophase_xid);
			keyed = RF_SIDE_CONTRIBUTION_PREPARED;
		}
		drops = parsed.nrels > 0;
		break;
	}
	default:
		return refused;
	}
	retained_side_note(work, owners & RETAINED_SIDE_ANCESTRY, keyed);
	if (drops)
		retained_structure(work);
	return RF_PAGE_PROOF_DETAIL_OK;
}


static bool
retained_side_resolved(const RetainedCutWork *work, TransactionId xid)
{
	for (uint32 i = 0; i < work->nresolved; i++)
		if (work->resolved[i] == xid)
			return true;
	return false;
}

static void
retained_side_pin(RetainedCutWork *work, RetainedSource *source, XLogRecPtr at, uint32 side)
{
	if (at == InvalidXLogRecPtr)
		return;
	work->pinned_side |= side;
	retained_pin(source, at, CLUSTER_WAL_RETAINED_PIN_SIDE);
}

/*
 * Fold the classes kept as a whole (see the file header) and the unresolved
 * PREPAREs into each source bound.  Keyed history was spooled as edges.
 */
void
retained_side_fold(RetainedCutWork *work)
{
	for (uint32 s = 0; s < work->nsources; s++) {
		RetainedSource *source = &work->sources[s];

		for (int c = 0; c < RETAINED_SIDE_CLASSES; c++) {
			uint32 bit = 1u << c;

			if ((bit & RETAINED_SIDE_ANCESTRY) == 0)
				continue;
			if ((work->obligation_unkeyed & bit) != 0)
				retained_side_pin(work, source, source->side_first[c], bit);
			else if ((work->obligation_side & bit) != 0)
				retained_side_pin(work, source, source->side_unkeyed[c], bit);
		}
		if (work->prepared_overflow)
			retained_side_pin(work, source, source->side_first[RETAINED_CLASS_PREPARED],
							  RF_SIDE_CONTRIBUTION_PREPARED);
	}
	if (work->prepared_overflow)
		return;
	for (uint32 i = 0; i < work->nprepares; i++)
		if (!retained_side_resolved(work, work->prepares[i].xid))
			retained_side_pin(work, &work->sources[work->prepares[i].source],
							  work->prepares[i].read_ptr, RF_SIDE_CONTRIBUTION_PREPARED);
}

#endif /* USE_PGRAC_CLUSTER */
