/*-------------------------------------------------------------------------
 *
 * cluster_tt_durable.h
 *	  pgrac durable Transaction Table (TT) slot commit_scn -- write / lookup
 *	  / by-xid scan (spec-3.11, own-instance).
 *
 *	  The durable TT slot lives in the undo segment header block 0
 *	  (UndoSegmentHeaderData.tt_slots[slot_offset], 32 bytes @ file offset
 *	  112 + slot_offset*32; commit_scn @ slot+8).  spec-3.4b reserved the
 *	  on-disk format; spec-3.11 activates the durable write + crash-recovery
 *	  (XLOG_UNDO_TT_SLOT_COMMIT, cluster_undo_xlog.c) + lookup so visibility /
 *	  CR can resolve commit_scn after overlay eviction or restart, retiring the
 *	  spec-3.10 watermark fail-closed for the still-bound case.
 *
 *	  Durable writes are per-slot 32-byte targeted pwrites: each committing
 *	  xact owns a distinct slot (non-overlapping byte range), so writes are
 *	  lock-free, and they are WAL-protected (no data-file fsync; redo recovers
 *	  a torn write -- spec-3.11 §2.2 / C10 / Q10).
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
 *	  src/include/cluster/cluster_tt_durable.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_TT_DURABLE_H
#define CLUSTER_TT_DURABLE_H

#include "cluster/cluster_scn.h" /* SCN */

/*
 * Pure decision predicates (no I/O) -- shared by the runtime lookup/redo paths
 * and exercised directly by cluster_unit (file I/O behavior is e2e in t/219).
 */

/* Redo decision (spec-3.11 §2.3): last-writer-wins by wrap generation. */
typedef enum ClusterTTRedoDecision {
	CLUSTER_TT_REDO_APPLY,	  /* rec.wrap >= slot.wrap: fresh / reuse / recycle / idempotent */
	CLUSTER_TT_REDO_SKIP,	  /* rec.wrap < slot.wrap (stale; a newer commit is already durable) */
	CLUSTER_TT_REDO_BADSTATUS /* on-disk slot.status out of [0,4] (garbage; PANIC) */
} ClusterTTRedoDecision;

/*
 * cluster_tt_durable_redo_decide -- decide what an XLOG_UNDO_TT_SLOT_COMMIT
 *	redo should do to the on-disk slot, given the slot's current
 *	(status, xid, wrap) and the record's (xid, wrap).  Pure; no I/O.
 */
extern ClusterTTRedoDecision cluster_tt_durable_redo_decide(uint8 slot_status,
															TransactionId slot_xid,
															uint16 slot_wrap, TransactionId rec_xid,
															uint16 rec_wrap);

/*
 * CLUSTER_TT_WRAP_ANY -- "no wrap expectation" sentinel for the durable
 *	lookup/match/scan APIs (spec-4.5a G4).  uint32-wide so it can never
 *	collide with a real uint16 slot wrap value.
 */
#define CLUSTER_TT_WRAP_ANY 0xFFFFFFFFU

/*
 * cluster_tt_durable_slot_match -- true iff a durable slot with the given
 *	(status, xid, commit_scn) is a valid still-bound COMMITTED match for the
 *	wanted xid (spec-3.11 C5).  Pure; no I/O.
 *
 *	spec-4.5a G4 (F3): xid alone cannot detect a slot recycled to a NEW
 *	generation whose 32-bit xid wrapped to the SAME value -- the match would
 *	return the new generation's commit_scn for the old tuple.  When the
 *	caller knows the binding-time generation (expected_wrap, carried by the
 *	ITL TT ref / undo record header tt_wrap_plus1 fields), a wrap mismatch
 *	is the recycle detector; CLUSTER_TT_WRAP_ANY preserves the xid-only
 *	pre-4.5a behaviour for callers with no expectation.
 */
extern bool cluster_tt_durable_slot_match(uint8 slot_status, TransactionId slot_xid,
										  uint16 slot_wrap, SCN slot_commit_scn,
										  TransactionId want_xid, uint32 expected_wrap);

/*
 * spec-3.22: durable by-xid RESOLVE result.  Splits the spec-3.21 recycled-slot
 * fail-closed into the provably-invisible case (RECYCLED_ZERO_MATCH after a
 * complete scan) versus the genuinely-unresolvable edges, fixing the §2.4
 * conflation: a 0-xid-match (slot recycled to a new owner -> commit_scn proven
 * below the retention horizon by the caller's proof) is NOT the same as a
 * 1-xid-match whose commit_scn is not yet stamped (delayed cleanout -> retained,
 * NOT below horizon).  Only the caller's retention proof turns RECYCLED_ZERO_MATCH
 * into INVISIBLE; every other result fails closed (53R9F).
 */
typedef enum ClusterTTDurableResolve {
	CLUSTER_TT_DURABLE_RESOLVED_SCN,		  /* 1 match, valid commit_scn */
	CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH,	  /* 0 matches, complete scan */
	CLUSTER_TT_DURABLE_XID_MATCH_INVALID_SCN, /* 1 match, unstamped scn (delayed cleanout) */
	CLUSTER_TT_DURABLE_AMBIGUOUS_WRAP,		  /* >1 matches (raw-xid wrap residue) */
	CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE		  /* degraded / unreadable segment */
} ClusterTTDurableResolve;

/*
 * cluster_tt_durable_classify -- pure classifier mapping the by-xid scan tallies
 *	to the resolve verdict.  No I/O; unit-tested truth table (spec-3.22 D1).
 *
 *	  xid_matches        : COMMITTED durable slots whose xid == the target,
 *	                       counted INDEPENDENT of commit_scn validity (§2.4).
 *	  match_has_valid_scn: when xid_matches == 1, whether that slot's commit_scn
 *	                       is valid (RESOLVED) or unstamped (XID_MATCH_INVALID_SCN).
 *	  scan_complete      : the full existing-segment range was read with no
 *	                       unreadable existing segment, on an own-instance node.
 *
 *	Precedence: >1 is definitive AMBIGUOUS_WRAP (wins even over an incomplete
 *	scan); else an incomplete scan is SCAN_UNAVAILABLE (a 0/1 tally is untrusted);
 *	else 0 -> RECYCLED_ZERO_MATCH, 1+valid -> RESOLVED_SCN, 1+invalid ->
 *	XID_MATCH_INVALID_SCN.
 */
extern ClusterTTDurableResolve
cluster_tt_durable_classify(int xid_matches, bool match_has_valid_scn, bool scan_complete);

/*
 * cluster_tt_slot_durable_commit -- durably stamp commit_scn (status COMMITTED)
 *	on the own-instance TT slot (segment_id, slot_offset) owned by `xid`/`wrap`.
 *	Emits XLOG_UNDO_TT_SLOT_COMMIT (before the commit record -- caller is in the
 *	pre-commit hook), then per-slot 32-byte targeted write of the TTSlot.  No
 *	fsync (WAL-protected).  ereport(ERROR) on I/O failure.
 */
extern void cluster_tt_slot_durable_commit(uint32 segment_id, uint16 slot_offset, TransactionId xid,
										   uint16 wrap, SCN commit_scn);

/*
 * cluster_tt_slot_durable_commit_writeonly -- spec-3.18 D4.1 (normal commit).
 *	Same per-slot 32-byte COMMITTED stamp as cluster_tt_slot_durable_commit, but
 *	WITHOUT the standalone XLOG_UNDO_TT_SLOT_COMMIT (0x30): the caller folds an
 *	equivalent xl_xact_tt_commit delta into the commit record, whose flush makes
 *	both durable atomically.  Returns the owner instance (1..128) for the
 *	delta's path-resolution field.  ereport(ERROR) on I/O failure.  Redo side:
 *	cluster_tt_durable_redo_stamp_slot() (cluster_undo_xlog.c), driven by
 *	xact_redo_commit instead of the 0x30 redo.
 */
extern uint8 cluster_tt_slot_durable_commit_writeonly(uint32 segment_id, uint16 slot_offset,
													  TransactionId xid, uint16 wrap,
													  SCN commit_scn);

/*
 * cluster_tt_slot_durable_abort -- spec-3.15 D5 (ROLLBACK PREPARED).
 * Stamps TT_SLOT_ABORTED preserving xid/wrap (V-2), emitting 0x31.
 * Same C10 durability contract as durable_commit.
 */
extern void cluster_tt_slot_durable_abort(uint32 segment_id, uint16 slot_offset, TransactionId xid,
										  uint16 wrap);

/*
 * cluster_tt_slot_durable_lookup -- read the durable TT slot (segment_id,
 *	slot_offset) and return its commit_scn iff the slot is still bound to
 *	`xid` and COMMITTED with a valid commit_scn.  xid mismatch (slot recycled
 *	by a later owner) or UNUSED -> false (never returns another owner's
 *	commit_scn -- spec-3.11 C5).  Returns true + *commit_scn on hit.  Callers
 *	must apply the C1b CLOG cross-check (the slot is stamped at pre-commit).
 */
extern bool cluster_tt_slot_durable_lookup(uint32 segment_id, uint16 slot_offset, TransactionId xid,
										   uint32 expected_wrap, SCN *commit_scn);

/*
 * cluster_tt_slot_durable_lookup_by_xid -- scan the local node's undo segment
 *	headers for a COMMITTED TT slot owned by `xid`.  Used by the spec-3.10
 *	watermark gate (the ITL slot was recycled, so ITL->UBA->slot is unavailable;
 *	resolve by xid).  Exactly one match -> true + *commit_scn; zero matches
 *	(recycled / overwritten) or more than one (xid wrap residue ambiguity) ->
 *	false -> caller fail-closes (53R9F; spec-3.11 §2.2 / C4 / 规则 8.A).
 *	Cost O(local_segments * TT_SLOTS_PER_SEGMENT); bounded by spec-3.12 retention.
 *
 *	spec-3.22: a thin wrapper over cluster_tt_slot_durable_resolve_by_xid that is
 *	true IFF RESOLVED_SCN -- the xmin-side caller (spec-3.11) keeps its existing
 *	binary contract untouched while the xmax-side gate (spec-3.22) consumes the
 *	finer-grained enum below.
 */
extern bool cluster_tt_slot_durable_lookup_by_xid(TransactionId xid, SCN *commit_scn);

/*
 * cluster_tt_slot_durable_resolve_by_xid -- spec-3.22: the by-xid scan returning
 *	the finer-grained ClusterTTDurableResolve enum (instead of lookup_by_xid's
 *	bool).  Counts xid-matches independent of commit_scn validity so a delayed-
 *	cleanout slot (XID_MATCH_INVALID_SCN) is never conflated with a recycled slot
 *	(RECYCLED_ZERO_MATCH).  Distinguishes an unreadable existing segment / degraded
 *	node (SCAN_UNAVAILABLE) from a genuine 0-match, so the xmax gate can only treat
 *	a 0-match as proof-of-below-horizon after a complete scan.  Sets *commit_scn on
 *	RESOLVED_SCN, else InvalidScn.
 */
extern ClusterTTDurableResolve
cluster_tt_slot_durable_resolve_by_xid(TransactionId xid, uint32 expected_wrap, SCN *commit_scn);


/*
 * Observability (spec-3.11 D7/D8) -- implemented in cluster_tt_durable_stat.c
 * so the pure logic above links into cluster_unit without shmem / wait-event
 * backend symbols (the unit test stubs the hooks below as no-ops).
 */

/* shmem counter region (5 atomic counters, 0 LWLock). */
extern Size cluster_tt_durable_shmem_size(void);
extern void cluster_tt_durable_shmem_init(void);
extern void cluster_tt_durable_shmem_register(void);

/* counter bump hooks. */
extern void cluster_tt_durable_count_commit(void);
extern void cluster_tt_durable_count_lookup(bool hit);
extern void cluster_tt_durable_count_by_xid_scan(void);
extern void cluster_tt_durable_count_redo_apply(void);

/* wait-event bracket hooks (WAIT_EVENT_UNDO_TT_DURABLE_IO). */
extern void cluster_tt_durable_io_wait_start(void);
extern void cluster_tt_durable_io_wait_end(void);

/* counter accessors (dump_undo / pg_cluster_state). */
extern uint64 cluster_tt_durable_commit_count(void);
extern uint64 cluster_tt_durable_lookup_hit_count(void);
extern uint64 cluster_tt_durable_lookup_miss_count(void);
extern uint64 cluster_tt_durable_by_xid_scan_count(void);
extern uint64 cluster_tt_durable_redo_apply_count(void);

#endif /* CLUSTER_TT_DURABLE_H */
