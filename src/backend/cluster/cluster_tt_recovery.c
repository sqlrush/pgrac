/*-------------------------------------------------------------------------
 *
 * cluster_tt_recovery.c
 *	  pgrac undo/TT recovery -- crash-left ACTIVE slot resolution (spec-4.8 D1).
 *
 *	  After StartupXLOG redo + RecoverPreparedTransactions(), any durable TT
 *	  slot still in TT_SLOT_ACTIVE belongs to a transaction that was in flight
 *	  at the crash.  The by-xid resolver (cluster_tt_durable.c) only matches
 *	  TT_SLOT_COMMITTED, so an unresolved crash-left ACTIVE slot yields a
 *	  0-match that the spec-3.22 retention theorem can mis-attribute as
 *	  "recycled committed-below-horizon" rather than the truth ("aborted").
 *
 *	  cluster_tt_recovery_resolve_active_slots() scans this instance's undo
 *	  segment headers and durably transitions each crash-left ACTIVE slot to
 *	  TT_SLOT_ABORTED (WAL-logged via cluster_tt_slot_durable_abort -> 0x60)
 *	  UNLESS its owning xact is LIVE: committed per CLOG (AD-006: CLOG is the
 *	  "did it commit" authority), or a resurrected prepared 2PC xact still in
 *	  the proc array.  An xact we cannot prove live is aborted (fail-closed,
 *	  规则 8.A): cluster visibility must never treat an in-flight-at-crash
 *	  transaction as committed.
 *
 *	  The pure liveness classifier (cluster_tt_recovery_classify_liveness)
 *	  lives in cluster_tt_durable.c so it links into cluster_unit; this file
 *	  holds the backend wrapper + scanner that consult PG's CLOG / proc array
 *	  and the undo smgr, so it is intentionally NOT linked into cluster_unit.
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
 *	  spec-4.8 D0 measure-first finding (Option A reframe): the on-disk TT
 *	  header slots are NEVER written TT_SLOT_ACTIVE.  The only durable header
 *	  writes are durable_commit (-> COMMITTED) and durable_abort (-> ABORTED),
 *	  each a targeted 32-byte RMW; the in-flight ("active") binding lives only
 *	  in the in-memory allocator (CTS_ACTIVE, ClusterTTSlotAllocState) and is
 *	  lost on crash ("BIND is not WAL'd").  So a simple in-flight crash leaves
 *	  the on-disk slot UNUSED (fresh) or a stale prior COMMITTED/ABORTED state
 *	  (reused, below the retention horizon) -- never ACTIVE.  Single-node
 *	  correctness for a crashed in-flight xact is therefore already provided by
 *	  PG-native CLOG (the xact is not committed -> MVCC-invisible).
 *
 *	  cluster_tt_recovery_resolve_active_slots() is consequently a FAIL-CLOSED
 *	  DEFENSIVE NET, not the load-bearing single-node correctness fix: it
 *	  normally resolves 0 slots, but would safely abort any on-disk ACTIVE slot
 *	  that ever appeared (a torn write, a future durable-bind, or corruption),
 *	  never treating it as committed (规则 8.A).  The crash-left in-flight xact
 *	  handling that DOES carry weight lives elsewhere: cross-node TT authority
 *	  (D2) and physical heap rollback apply (D7, which identifies crashed xacts
 *	  via undo records + cluster_tt_recovery_xact_liveness, not on-disk ACTIVE
 *	  TT slots).  The pure classifier + xact_liveness here are reused by both.
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
 * cluster_tt_recovery_xact_liveness -- spec-4.8 D1.
 *
 *	Backend wrapper over the pure classifier: consult CLOG (did it commit)
 *	and the proc array (is it a resurrected prepared 2PC xact still in flight)
 *	for xid, then classify.  An xid that is neither normal nor valid is
 *	AMBIGUOUS (fail-closed -> the slot is aborted).
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
 *	Scan this instance's undo segment headers; durably resolve every crash-
 *	left TT_SLOT_ACTIVE slot to TT_SLOT_ABORTED unless its owning xact is LIVE.
 *	Returns the number of slots resolved to ABORTED.
 *
 *	Called once from StartupXLOG after recovery completes (WAL writes enabled,
 *	prepared set loaded, RecoveryInProgress() false).  Gated by cluster.enabled
 *	+ cluster.tt_recovery_resolve_active.
 *
 *	Fail-safe on read failure: an existing-but-unreadable or absent segment is
 *	skipped (we never abort a slot we cannot read).  Leaving a slot ACTIVE is no
 *	worse than the pre-D1 behaviour (the by-xid 0-match path); aborting a slot we
 *	cannot read would risk a false-abort.  durable_abort does its own targeted
 *	32-byte RMW per slot, so the in-memory header snapshot used to iterate stays
 *	valid across slot resolutions within a segment.
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

		if (!cluster_undo_smgr_read_block(segment_id, owner, 0, blockbuf.data))
			continue; /* absent / unreadable -> skip (never false-abort) */

		hdr = (UndoSegmentHeaderData *)blockbuf.data;
		for (i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
			const TTSlot *s = &hdr->tt_slots[i];
			ClusterTtRecoveryLiveness verdict;

			if (s->status != (uint8)TT_SLOT_ACTIVE)
				continue;
			if (!TransactionIdIsValid(s->xid))
				continue;

			verdict = cluster_tt_recovery_xact_liveness(s->xid);
			if (verdict == CLUSTER_TT_RECOVERY_LIVE)
				continue; /* committed / resurrected prepared -> keep ACTIVE */

			/* DEAD or AMBIGUOUS -> fail-closed ABORTED (durable, WAL 0x60). */
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

		if (!cluster_undo_smgr_read_block(segment_id, owner, 0, blockbuf.data))
			continue; /* absent / unreadable -> skip */

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
 * revert_one_delete_record -- spec-4.8 D7 part 2: physically revert one DELETE
 *	undo record of an aborted xact, index-safely.
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
revert_one_delete_record(const UndoRecordHeader *hdr, TransactionId aborted_xid)
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
		= !(htup->t_infomask & HEAP_XMAX_IS_MULTI) && TransactionIdEquals(raw_xmax, aborted_xid);
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
 *	physically revert each of its DELETE records.  Only records owned by
 *	aborted_xid are acted on (the chain is per-transaction).  A short/unreadable
 *	record stops the walk (fail-safe); a hard step cap guards a malformed chain.
 *	Returns the number of tuples reverted.
 */
static int
revert_aborted_undo_chain(UBA head, TransactionId aborted_xid)
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

		if (TransactionIdEquals(hdr->xid, aborted_xid))
			reverted += revert_one_delete_record(hdr, aborted_xid);

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

		if (!cluster_undo_smgr_read_block(segment_id, owner, 0, blockbuf.data))
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

			reverted += revert_aborted_undo_chain(s->first_undo_block, s->xid);
		}
	}

	if (reverted > 0)
		ereport(LOG,
				(errmsg("cluster undo/TT recovery: physically reverted %d aborted DELETE tuple(s)",
						reverted)));
	return reverted;
}
