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
#include "utils/elog.h"

#include "cluster/cluster_guc.h"		  /* cluster_node_id */
#include "cluster/cluster_scn.h"		  /* SCN, SCN_VALID, InvalidScn */
#include "cluster/cluster_undo_cleaner.h" /* spec-3.13 D2-B scan-only pass */
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_tt_slot.h"	  /* TTSlot, TT_SLOT_COMMITTED, TT_SLOTS_PER_SEGMENT */
#include "cluster/cluster_undo_segment.h" /* UndoSegmentHeaderData */
#include "cluster/cluster_undo_smgr.h"	  /* header-bytes + block I/O */
#include "cluster/storage/cluster_undo_alloc.h" /* CLUSTER_UNDO_SEGS_PER_INSTANCE */
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
						uint16 wrap, SCN commit_scn)
{
	uint32 off = tt_slot_file_offset(slot_offset);
	TTSlot slot;

	cluster_tt_durable_io_wait_start();

	if (!cluster_undo_smgr_read_header_bytes(segment_id, owner, off, (char *)&slot, sizeof(slot))) {
		cluster_tt_durable_io_wait_end();
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster durable TT: cannot read slot %u of undo segment %u",
							   slot_offset, segment_id)));
	}

	slot.xid = xid;
	slot.wrap = wrap;
	slot.status = (uint8)TT_SLOT_COMMITTED;
	slot.commit_scn = commit_scn;
	slot.first_undo_block = InvalidUbaVal; /* spec-4.8 D7-A (P1#1): no stale head */

	if (!cluster_undo_smgr_write_header_bytes(segment_id, owner, off, (const char *)&slot,
											  sizeof(slot))) {
		cluster_tt_durable_io_wait_end();
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster durable TT: cannot write slot %u of undo segment %u",
							   slot_offset, segment_id)));
	}

	cluster_tt_durable_io_wait_end();
	cluster_tt_durable_count_commit();
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

	tt_slot_write_committed(segment_id, owner, slot_offset, xid, wrap, commit_scn);
}

uint8
cluster_tt_slot_durable_commit_writeonly(uint32 segment_id, uint16 slot_offset, TransactionId xid,
										 uint16 wrap, SCN commit_scn)
{
	uint8 owner = tt_owner_instance_for_segment(segment_id);

	Assert(slot_offset < TT_SLOTS_PER_SEGMENT);
	Assert(TransactionIdIsValid(xid));
	Assert(SCN_VALID(commit_scn));

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
	tt_slot_write_committed(segment_id, owner, slot_offset, xid, wrap, commit_scn);
	return owner;
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

	if (!cluster_undo_smgr_read_header_bytes(segment_id, owner, off, (char *)&slot, sizeof(slot))) {
		cluster_tt_durable_io_wait_end();
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster durable TT: cannot read slot %u of undo segment %u",
							   slot_offset, segment_id)));
	}

	slot.xid = xid;
	slot.wrap = wrap;
	slot.status = (uint8)TT_SLOT_ABORTED;
	slot.commit_scn = InvalidScn;
	slot.first_undo_block = InvalidUbaVal; /* spec-4.8 D7-A (P1#1): cleared; 0x90 re-attaches */

	if (!cluster_undo_smgr_write_header_bytes(segment_id, owner, off, (const char *)&slot,
											  sizeof(slot))) {
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

	if (!cluster_undo_smgr_read_header_bytes(segment_id, owner, off, (char *)&slot, sizeof(slot))) {
		cluster_tt_durable_io_wait_end();
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster durable TT: cannot read slot %u of undo segment %u",
							   slot_offset, segment_id)));
	}

	/* Identity gate (规则 8.A): only stamp the head if the slot still owns this
	 * (xid, wrap); a recycled slot belongs to a newer owner -> leave untouched. */
	if (slot.xid == xid && slot.wrap == wrap) {
		slot.first_undo_block = first_undo_block;
		if (!cluster_undo_smgr_write_header_bytes(segment_id, owner, off, (const char *)&slot,
												  sizeof(slot))) {
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
	if (!cluster_undo_smgr_read_header_bytes(segment_id, owner, off, (char *)&slot, sizeof(slot))) {
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


ClusterTTDurableResolve
cluster_tt_slot_durable_resolve_by_xid(TransactionId xid, uint32 expected_wrap, SCN *commit_scn)
{
	/* spec-3.22 own-instance entry; the origin-qualified scan backs it. */
	return cluster_tt_slot_durable_resolve_by_xid_origin(cluster_node_id, xid, expected_wrap,
														 commit_scn);
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
											  uint32 expected_wrap, SCN *commit_scn)
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

		if (!cluster_undo_smgr_read_block(segment_id, owner, 0, blockbuf.data)) {
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
				}
			}
		}
	}
	cluster_tt_durable_io_wait_end();

	result = cluster_tt_durable_classify(xid_matches, match_has_valid_scn, scan_complete);
	if (result == CLUSTER_TT_DURABLE_RESOLVED_SCN)
		*commit_scn = found;
	return result;
}


bool
cluster_tt_slot_durable_lookup_by_xid(TransactionId xid, SCN *commit_scn)
{
	/*
	 * spec-3.22: thin wrapper preserving the spec-3.11 xmin-side binary contract
	 * (true IFF exactly one resolved match; every other enum -> false).  The
	 * xmax-side gate (spec-3.22) consumes the enum directly via resolve_by_xid.
	 */
	return cluster_tt_slot_durable_resolve_by_xid(xid, CLUSTER_TT_WRAP_ANY, commit_scn)
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
	if (!cluster_undo_smgr_read_block(segment_id, owner_instance, 0, block.data)) {
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
