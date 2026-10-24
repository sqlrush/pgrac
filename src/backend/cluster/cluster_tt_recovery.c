/*-------------------------------------------------------------------------
 *
 * cluster_tt_recovery.c
 *	  Legacy local undo/TT recovery and recovery observations.
 *
 *	  Nonshared startup retains the local CLOG/process liveness resolver.
 *	  Shared recovery must instead prove the exact top binding, checkpoint
 *	  coverage and complete terminal replay before resolving a loser. An
 *	  ACTIVE slot is durable (the canonical BIND producer writes it); neither
 *	  local liveness nor an unreadable/missing source supplies that proof.
 *	  This legacy scan therefore refuses an observed shared ACTIVE slot
 *	  without changing its status or undo head. A zero result is not a
 *	  complete shared transaction census or permission to open the database.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-4.8-undo-tt-recovery.md (D1)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_tt_recovery.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *	  Shared transaction completion uses the typed recovery owners. The
 *	  legacy WAL-free physical DELETE cleanup below is also excluded from
 *	  shared mode; these local helpers are not substitutes for that service.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"   /* HEAP_XMAX_INVALID, HeapTupleHeaderGetRawXmax (D7) */
#include "access/transam.h"		   /* TransactionIdDidCommit */
#include "storage/buf_internals.h" /* GetBufferDescriptor / content lock (D7) */
#include "storage/bufmgr.h"		   /* ReadBufferWithoutRelcache (D7) */
#include "storage/bufpage.h"	   /* PageGetItemId / PageGetItem (D7) */
#include "storage/procarray.h"	   /* TransactionIdIsInProgress */
#include "utils/elog.h"

#include "cluster/cluster_guc.h" /* cluster_enabled / cluster_node_id / GUC */
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_scn.h" /* spec-4.8 D5: SCN high-watermark recovery */
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_tt_slot.h"			/* TTSlot, TT_SLOT_*, TT_SLOTS_PER_SEGMENT */
#include "cluster/cluster_uba.h"				/* UBA_is_invalid (D7 chain walk) */
#include "cluster/cluster_undo_record.h"		/* UndoRecordHeader, UNDO_RECORD_DELETE (D7) */
#include "cluster/cluster_undo_record_api.h"	/* cluster_undo_get_record (D7) */
#include "cluster/cluster_undo_segment.h"		/* UndoSegmentHeaderData */
#include "cluster/cluster_undo_smgr.h"			/* cluster_undo_smgr_read_block */
#include "cluster/storage/cluster_undo_alloc.h" /* CLUSTER_UNDO_SEGS_PER_INSTANCE */

/* spec-4.8 D7: hard cap on an undo chain walk (malformed-chain loop guard). */
#define CLUSTER_TT_RECOVERY_MAX_CHAIN_STEPS 1000000

/*
 * Both startup consumers require the same published-header read contract.
 * Shared input may be skipped only when the final path is explicitly absent;
 * a damaged or unreadable published segment is not an empty recovery input.
 * The existing probe validates file size and header identity, not exact-top
 * transaction identity or completeness of the cross-source recovery census.
 */
static bool
tt_recovery_read_header(uint32 segment_id, uint8 owner, char block0[BLCKSZ])
{
	ClusterUndoSmgrFinalState state;

	if (!cluster_shared_config)
		return cluster_undo_smgr_read_block(cluster_undo_intent_for_owner(owner), segment_id, owner,
											0, block0);

	state = cluster_undo_smgr_probe_segment(cluster_undo_intent_for_owner(owner), segment_id, owner,
											block0);
	if (state == CLUSTER_UNDO_SMGR_FINAL_ABSENT)
		return false;
	if (state != CLUSTER_UNDO_SMGR_FINAL_EXACT)
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
						errmsg("could not validate published undo segment for shared recovery"),
						errdetail("Undo segment %u, owner %u, final-path probe state %d.",
								  segment_id, (unsigned)owner, (int)state),
						errhint("Preserve the original DATA and WAL. A damaged or unreadable "
								"segment cannot be treated as absent.")));
	return true;
}

/*
 * cluster_tt_recovery_xact_liveness -- spec-4.8 D1.
 *
 *	Backend wrapper over the pure classifier: consult CLOG (did it commit)
 *	and the proc array (is it a resurrected prepared 2PC xact still in flight)
 *	for xid, then classify.  An xid that is neither normal nor valid is
 *	AMBIGUOUS. Only the nonshared legacy resolver consumes that classification;
 *	it cannot authorize a shared ABORT.
 *
 *	Note: CLOG alone cannot distinguish a crash-left non-prepared xact from a
 *	resurrected prepared xact -- both read as "not committed, not aborted" in
 *	CLOG.  Only TransactionIdIsInProgress (true for prepared xacts after
 *	RecoverPreparedTransactions, false for a crashed in-flight xact whose proc
 *	is gone) tells them apart, so it is required here.
 */
ClusterTtRecoveryLiveness
cluster_tt_recovery_xact_liveness(TransactionId xid)
{
	bool did_commit;
	bool is_in_progress;

	if (!TransactionIdIsValid(xid) || !TransactionIdIsNormal(xid))
		return CLUSTER_TT_RECOVERY_AMBIGUOUS;

	did_commit = TransactionIdDidCommit(xid);
	is_in_progress = TransactionIdIsInProgress(xid);

	return cluster_tt_recovery_classify_liveness(true, did_commit, is_in_progress);
}

/*
 * cluster_tt_recovery_resolve_active_slots -- spec-4.8 D1.
 *
 *	Scan this instance's undo segment headers. Shared scans skip only an
 *	absent final path and require the original full-size/identity probe for
 *	present segments. Shared ACTIVE slots refuse before consulting local
 *	liveness; nonshared slots retain the original transition to ABORTED
 *	unless their owning transaction is LIVE.
 *	Returns the number of slots resolved to ABORTED.
 *
 *	Called once from StartupXLOG after recovery completes (WAL writes enabled,
 *	prepared set loaded, RecoveryInProgress() false).  Gated by cluster.enabled
 *	+ cluster.tt_recovery_resolve_active.
 *
 *	This instance-local scan is not a complete shared inventory: no caller
 *	may interpret zero resolved slots as transaction recovery completion.
 *	Nonshared durable_abort performs a targeted slot RMW, preserving the
 *	other slots in the in-memory iteration snapshot.
 */
int
cluster_tt_recovery_resolve_active_slots(void)
{
	int node;
	uint8 owner;
	uint32 seg_lo;
	uint32 seg_hi;
	uint32 segment_id;
	PGAlignedBlock blockbuf;
	int resolved = 0;

	if (!cluster_enabled || !cluster_tt_recovery_resolve_active)
		return 0;
	if (cluster_node_id < 0)
		return 0;

	node = cluster_node_id;
	owner = (uint8)(node + 1);
	seg_lo = (uint32)node * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	seg_hi = seg_lo + CLUSTER_UNDO_SEGS_PER_INSTANCE - 1;

	for (segment_id = seg_lo; segment_id <= seg_hi; segment_id++) {
		UndoSegmentHeaderData *hdr;
		uint16 i;

		if (!tt_recovery_read_header(segment_id, owner, blockbuf.data))
			continue;

		hdr = (UndoSegmentHeaderData *)blockbuf.data;
		for (i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
			const TTSlot *s = &hdr->tt_slots[i];
			ClusterTtRecoveryLiveness verdict;

			if (s->status != (uint8)TT_SLOT_ACTIVE)
				continue;
			if (cluster_shared_config)
				ereport(
					ERROR,
					(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
					 errmsg(
						 "shared recovery requires exact transaction proof for ACTIVE undo slot"),
					 errdetail(
						 "Undo segment %u slot %u, transaction %u, wrap %u remains unresolved.",
						 segment_id, (unsigned)i, s->xid, (unsigned)s->wrap),
					 errhint("Preserve the original DATA and WAL. Local transaction status cannot "
							 "replace complete shared transaction recovery.")));
			if (!TransactionIdIsValid(s->xid))
				continue;

			verdict = cluster_tt_recovery_xact_liveness(s->xid);
			if (verdict == CLUSTER_TT_RECOVERY_LIVE)
				continue; /* committed / resurrected prepared -> keep ACTIVE */

			/* Original nonshared DEAD/AMBIGUOUS resolution only. */
			cluster_tt_slot_durable_abort(segment_id, i, s->xid, s->wrap);
			cluster_tt_recovery_count_active_resolved_aborted();
			resolved++;
		}
	}

	if (resolved > 0)
		ereport(
			LOG,
			(errmsg("cluster undo/TT recovery: resolved %d crash-left ACTIVE TT slot(s) to ABORTED",
					resolved)));

	return resolved;
}

/*
 * cluster_tt_recovery_observe_scn_highwater -- spec-4.8 D5 (L222).
 *
 *	Scan this instance's undo segment headers for the maximum durable TT
 *	commit_scn and Lamport-observe it into cluster_scn at startup, so a reader's
 *	read_scn taken after crash-restart is not left below the durable commit/
 *	retention high-watermark.  Without this, the post-restart window has
 *	cluster_scn lagging the durable peak until organic advance catches up, so a
 *	CR read whose read_scn predates the retention horizon over-fail-closes
 *	"snapshot too old" (规则 8.A-compliant -- an error, not wrong data -- but an
 *	availability regression; window-dependent, self-heals as the SCN advances).
 *
 *	cluster_scn_recovery_replay_observe is Lamport-monotonic: observing the peak
 *	can only advance cluster_scn (never rewind), so it is correctness-safe even
 *	if the peak is conservatively high.  Page-level pd_block_scn is already
 *	covered by the redo-path xl_scn observe (spec-1.18); this closes the TT
 *	commit_scn arm of L222.  Returns the number of commit_scn peaks observed
 *	(0 or 1).  Called once from StartupXLOG after recovery completes; gated by
 *	cluster.enabled only (independent of cluster.tt_recovery_resolve_active).
 */
int
cluster_tt_recovery_observe_scn_highwater(void)
{
	int node;
	uint8 owner;
	uint32 seg_lo;
	uint32 seg_hi;
	uint32 segment_id;
	PGAlignedBlock blockbuf;
	SCN max_commit_scn = InvalidScn;

	if (!cluster_enabled)
		return 0;
	if (cluster_node_id < 0)
		return 0;

	node = cluster_node_id;
	owner = (uint8)(node + 1);
	seg_lo = (uint32)node * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	seg_hi = seg_lo + CLUSTER_UNDO_SEGS_PER_INSTANCE - 1;

	for (segment_id = seg_lo; segment_id <= seg_hi; segment_id++) {
		UndoSegmentHeaderData *hdr;
		uint16 i;

		if (!tt_recovery_read_header(segment_id, owner, blockbuf.data))
			continue;

		hdr = (UndoSegmentHeaderData *)blockbuf.data;
		for (i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
			const TTSlot *s = &hdr->tt_slots[i];

			if (s->status != (uint8)TT_SLOT_COMMITTED)
				continue;
			if (!SCN_VALID(s->commit_scn))
				continue;
			if (!SCN_VALID(max_commit_scn) || scn_time_cmp(s->commit_scn, max_commit_scn) > 0)
				max_commit_scn = s->commit_scn;
		}
	}

	if (!SCN_VALID(max_commit_scn))
		return 0;

	/* Lamport-monotonic: bumps cluster_scn to >= the durable commit peak. */
	cluster_scn_recovery_replay_observe(max_commit_scn);
	cluster_tt_recovery_count_scn_highwater_recovered();

	ereport(
		LOG,
		(errmsg(
			"cluster undo/TT recovery: observed durable TT commit_scn high-watermark " UINT64_FORMAT
			" into cluster_scn",
			(uint64)max_commit_scn)));
	return 1;
}

/*
 * TTRevertExpect -- spec-4.8 D7-A (P1#2): the identity an undo record must carry
 *	to be eligible for physical revert, derived from the scanned ABORTED TT slot.
 *	An undo record stamps its binding-time (origin, segment, slot, wrap+1)
 *	(spec-4.5a G4); D7 requires all four to match the slot it is walking from
 *	before touching the heap, so a 2^32 wrap collision or a record bound to a
 *	different slot cannot spoof a revert via raw xid alone.
 */
typedef struct TTRevertExpect {
	uint16 origin_node_id;	   /* this node (D7 only scans its own segments) */
	uint16 tt_slot_segment_id; /* scanned undo segment */
	uint32 tt_slot_id;		   /* cluster_tt_slot_offset_to_id(scanned offset) */
	uint16 tt_wrap_plus1;	   /* scanned slot wrap + 1 (0 = unknown -> no match) */
	TransactionId xid;		   /* scanned ABORTED slot xid */
} TTRevertExpect;

/*
 * revert_one_delete_record -- spec-4.8 D7 part 2: physically revert one DELETE
 *	undo record of an aborted xact, index-safely.
 *
 *	spec-4.8 D7-A (P1#2): before any page read or heap mutation, the record must
 *	match the scanned slot's full TT identity (exp); raw xid alone is not enough.
 *
 *	INSERT/UPDATE records fail closed immediately (index-unsafe: PG has no
 *	synchronous per-entry index point-delete; the matrix leaves them to MVCC
 *	invisible + vacuum -- I10).  For a DELETE record, read the target heap page
 *	(ReadBufferWithoutRelcache, recovery has no relcache), and if the tuple
 *	still carries exactly this aborted deleter's single-xact xmax, set the
 *	HEAP_XMAX_INVALID hint -- the standard PG aborted-xmax cleanout (a hint bit,
 *	WAL-free + idempotent by design), restoring the tuple to live.  DELETE never
 *	removed the index entry, so the restored tuple keeps every index entry valid
 *	(no index op, no dangling entry).  All decisions go through the unit-tested
 *	classifier (cluster_tt_recovery_classify_revert).  Returns 1 if a tuple was
 *	reverted, else 0.
 */
static int
PGRAC_PCM_X_FENCE_DOMINATED(cluster_bufmgr_pcm_x_ordinary_content_write_permitted)
	revert_one_delete_record(const UndoRecordHeader *hdr, const TTRevertExpect *exp)
{
	Buffer buf;
	BufferDesc *desc;
	Page page;
	ItemId iid;
	HeapTupleHeader htup;
	TransactionId raw_xmax;
	bool xmax_matches;
	bool xmax_already_clear;
	ClusterTtRecoveryRevertVerdict verdict;
	int reverted = 0;

	/*
	 * spec-4.8 D7-A (P1#2): full TT-identity gate FIRST -- before any page read
	 * or heap mutation.  A 0 on either tt_wrap_plus1 side means "unknown
	 * generation" (legacy record, no same-slot binding, or a wrap that landed on
	 * 0xFFFF whose +1 encoding collides with the sentinel) and is never trusted
	 * to match.  Any mismatch: no heap touch (rule 8.A), count fail-closed.
	 */
	if (hdr->origin_node_id != exp->origin_node_id
		|| hdr->tt_slot_segment_id != exp->tt_slot_segment_id || hdr->tt_slot_id != exp->tt_slot_id
		|| exp->tt_wrap_plus1 == 0 || hdr->tt_wrap_plus1 != exp->tt_wrap_plus1) {
		cluster_tt_recovery_count_undo_revert_failclosed();
		return 0;
	}

	/* Index-unsafe records never touch the heap (matrix v2: fail-closed). */
	if (hdr->record_type != (uint8)UNDO_RECORD_DELETE) {
		cluster_tt_recovery_count_undo_revert_failclosed();
		return 0;
	}
	/* Malformed locator -> never read/mutate a garbage page (fail-closed). */
	if (!RelFileNumberIsValid(hdr->target_locator.relNumber)) {
		cluster_tt_recovery_count_undo_revert_failclosed();
		return 0;
	}

	buf = ReadBufferWithoutRelcache(hdr->target_locator, hdr->target_fork, hdr->target_block,
									RBM_NORMAL, NULL, true /* permanent */);
	if (!BufferIsValid(buf)) {
		cluster_tt_recovery_count_undo_revert_failclosed();
		return 0;
	}
	desc = GetBufferDescriptor(buf - 1);
	LWLockAcquire(BufferDescriptorGetContentLock(desc), LW_EXCLUSIVE);
	if (!cluster_bufmgr_pcm_x_ordinary_content_write_permitted(desc)) {
		LWLockRelease(BufferDescriptorGetContentLock(desc));
		ReleaseBuffer(buf);
		cluster_tt_recovery_count_undo_revert_failclosed();
		return 0;
	}
	page = BufferGetPage(buf);

	/* Bounds: the target offset must be a normal line pointer on this page. */
	if (hdr->target_offset < FirstOffsetNumber
		|| hdr->target_offset > PageGetMaxOffsetNumber(page)) {
		LWLockRelease(BufferDescriptorGetContentLock(desc));
		ReleaseBuffer(buf);
		cluster_tt_recovery_count_undo_revert_failclosed();
		return 0;
	}
	iid = PageGetItemId(page, hdr->target_offset);
	if (!ItemIdIsNormal(iid)) {
		LWLockRelease(BufferDescriptorGetContentLock(desc));
		ReleaseBuffer(buf);
		cluster_tt_recovery_count_undo_revert_failclosed();
		return 0;
	}
	htup = (HeapTupleHeader)PageGetItem(page, iid);

	/* Identity (I7): the tuple must still carry exactly this aborted deleter's
	 * single-xact xmax.  A multixact xmax is not this deleter alone -> no match. */
	raw_xmax = HeapTupleHeaderGetRawXmax(htup);
	xmax_matches
		= !(htup->t_infomask & HEAP_XMAX_IS_MULTI) && TransactionIdEquals(raw_xmax, exp->xid);
	xmax_already_clear = (htup->t_infomask & HEAP_XMAX_INVALID) != 0;

	verdict = cluster_tt_recovery_classify_revert(true /* delete */, true /* slot aborted */,
												  xmax_matches, xmax_already_clear);
	switch (verdict) {
	case CLUSTER_TT_REVERT_APPLY:
		/* WAL-free idempotent hint: aborted xmax -> tuple live (= PG's own
			 * aborted-xmax cleanout).  No index op; the index entry stays valid. */
		htup->t_infomask |= HEAP_XMAX_INVALID;
		MarkBufferDirty(buf);
		cluster_tt_recovery_count_heap_tuples_physically_reverted();
		reverted = 1;
		break;
	case CLUSTER_TT_REVERT_SKIP_DONE:
		break; /* idempotent: already reverted */
	case CLUSTER_TT_REVERT_FAILCLOSED:
	default:
		cluster_tt_recovery_count_undo_revert_failclosed();
		break;
	}

	LWLockRelease(BufferDescriptorGetContentLock(desc));
	ReleaseBuffer(buf);
	return reverted;
}

/*
 * revert_aborted_undo_chain -- spec-4.8 D7 part 2: walk one aborted xact's undo
 *	chain (prev_uba from the durable TT slot's first_undo_block head) and
 *	physically revert each of its DELETE records.  Only records whose full TT
 *	identity matches the scanned slot (exp; spec-4.8 D7-A P1#2) are reverted --
 *	revert_one_delete_record gates before any heap touch; the cheap xid filter
 *	here just skips foreign records on the chain.  A short/unreadable record
 *	stops the walk (fail-safe); a hard step cap guards a malformed chain.
 *	Returns the number of tuples reverted.
 */
static int
revert_aborted_undo_chain(UBA head, const TTRevertExpect *exp)
{
	UBA uba = head;
	int steps = 0;
	int reverted = 0;

	while (!UBA_is_invalid(uba) && steps < CLUSTER_TT_RECOVERY_MAX_CHAIN_STEPS) {
		PGAlignedBlock recbuf;
		size_t len;
		const UndoRecordHeader *hdr;
		UBA next;

		steps++;
		len = cluster_undo_get_record(uba, recbuf.data, sizeof(recbuf.data));
		if (len < sizeof(UndoRecordHeader))
			break; /* unreadable / short -> stop (fail-safe) */

		hdr = (const UndoRecordHeader *)recbuf.data;
		next = hdr->prev_uba; /* capture before any work */

		if (TransactionIdEquals(hdr->xid, exp->xid))
			reverted += revert_one_delete_record(hdr, exp);

		uba = next;
	}
	return reverted;
}

/*
 * cluster_tt_recovery_physical_rollback -- spec-4.8 D7 part 2 (mini-plan v2).
 *
 *	Index-aware physical rollback of ABORTED transactions' DELETE writes, for
 *	the enumerable subset: durably-ABORTED TT slots (a 2PC ROLLBACK PREPARED
 *	wrote XLOG_UNDO_TT_SLOT_ABORT, so the on-disk slot is TT_SLOT_ABORTED with a
 *	valid first_undo_block chain head).  Crash-left in-flight xacts have no
 *	durable chain head (spec-4.8 D1: the in-flight binding is in-memory and lost
 *	on crash), so they are not enumerable here and stay fail-closed -> MVCC
 *	invisible + vacuum (I10) -- consistent with the matrix.
 *
 *	Reuses the existing cluster_undo_get_record + prev_uba chain walk (no new
 *	undo-region scanner) and the unit-tested classifier; the only heap mutation
 *	is the WAL-free idempotent HEAP_XMAX_INVALID hint on a DELETE-record tuple.
 *	Called once from StartupXLOG after recovery completes, after D1 resolution +
 *	D5 SCN high-watermark.  Returns the number of tuples reverted.
 *
 *	NOT a full Oracle SMON rollback: INSERT/UPDATE and crash-left in-flight are
 *	matrix-reasoned fail-closed (see closure docs).
 */
int
cluster_tt_recovery_physical_rollback(void)
{
	int node;
	uint8 owner;
	uint32 seg_lo;
	uint32 seg_hi;
	uint32 segment_id;
	PGAlignedBlock blockbuf;
	int reverted = 0;

	if (cluster_shared_config)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("legacy physical TT rollback is not supported in shared mode"),
						errhint("Shared recovery requires the typed undo recovery owner.")));
	if (!cluster_enabled || !cluster_tt_recovery_resolve_active)
		return 0;
	if (cluster_node_id < 0)
		return 0;

	node = cluster_node_id;
	owner = (uint8)(node + 1);
	seg_lo = (uint32)node * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	seg_hi = seg_lo + CLUSTER_UNDO_SEGS_PER_INSTANCE - 1;

	for (segment_id = seg_lo; segment_id <= seg_hi; segment_id++) {
		UndoSegmentHeaderData *hdr;
		uint16 i;

		if (!cluster_undo_smgr_read_block(cluster_undo_intent_for_owner(owner), segment_id, owner,
										  0, blockbuf.data))
			continue; /* absent / unreadable -> skip */

		hdr = (UndoSegmentHeaderData *)blockbuf.data;
		for (i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
			const TTSlot *s = &hdr->tt_slots[i];

			if (s->status != (uint8)TT_SLOT_ABORTED)
				continue;
			if (!TransactionIdIsValid(s->xid))
				continue;
			if (UBA_is_invalid(s->first_undo_block))
				continue; /* no durable chain head -> not enumerable here */

			{
				/*
				 * spec-4.8 D7-A (P1#2): derive the expected TT identity from the
				 * scanned slot so the chain walk reverts only records actually
				 * bound to this (origin, segment, slot, wrap) -- not a raw-xid
				 * match.  We only scan our own node's segments, so origin =
				 * cluster_node_id.  A slot whose wrap is 0xFFFF makes wrap+1 == 0
				 * ("unknown"); revert_one_delete_record then fails closed.
				 */
				TTRevertExpect exp;

				exp.origin_node_id = (uint16)cluster_node_id;
				exp.tt_slot_segment_id = (uint16)segment_id;
				exp.tt_slot_id = cluster_tt_slot_offset_to_id(i);
				exp.tt_wrap_plus1 = (uint16)(s->wrap + 1);
				exp.xid = s->xid;
				reverted += revert_aborted_undo_chain(s->first_undo_block, &exp);
			}
		}
	}

	if (reverted > 0)
		ereport(LOG,
				(errmsg("cluster undo/TT recovery: physically reverted %d aborted DELETE tuple(s)",
						reverted)));
	return reverted;
}
