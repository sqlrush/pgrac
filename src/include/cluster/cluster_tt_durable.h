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

#include "cluster/cluster_itl_slot.h" /* UBA (spec-4.8 D7-A set_head) */
#include "cluster/cluster_scn.h"	  /* SCN */

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
 * cluster_tt_slot_durable_set_head -- spec-4.8 D7-A.  Durably stamp the slot's
 *	undo-chain head (first_undo_block) via WAL 0x90 + targeted RMW, gated by the
 *	slot still owning (xid, wrap); does not change slot.status.  Called from the
 *	ROLLBACK PREPARED prefinish abort path so D7 physical rollback can walk it.
 */
extern void cluster_tt_slot_durable_set_head(uint32 segment_id, uint16 slot_offset,
											 TransactionId xid, uint16 wrap, UBA first_undo_block);

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
extern ClusterTTDurableResolve
cluster_tt_slot_durable_resolve_by_xid_origin(int origin_node, TransactionId xid,
											  uint32 expected_wrap, SCN *commit_scn,
											  uint16 *out_seg, uint16 *out_slot, uint16 *out_wrap);

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
 *
 *	spec-5.55 D1: optional out_seg/out_slot/out_wrap report the matched durable TT
 *	slot identity (held at the match point) on RESOLVED_SCN -- the shared resolver
 *	cache caches this {seg,slot,wrap} as a SEARCH position hint so a peer backend
 *	can re-validate the slot in O(1) instead of re-running the O(segments) scan.
 *	NULL = legacy callers; the resolve/classify verdict and the TTSlot on-disk ABI
 *	are unchanged (no catversion bump).
 */
extern ClusterTTDurableResolve
cluster_tt_slot_durable_resolve_by_xid(TransactionId xid, uint32 expected_wrap, SCN *commit_scn,
									   uint16 *out_seg, uint16 *out_slot, uint16 *out_wrap);


/*
 * spec-4.8 D1 -- crash-left ACTIVE TT slot recovery resolution.
 *
 *	After redo + prepared-xact recovery, any TT slot still TT_SLOT_ACTIVE
 *	belongs to a transaction that was in flight at crash.  The by-xid resolver
 *	only matches TT_SLOT_COMMITTED, so an unresolved crash-left ACTIVE slot
 *	yields a 0-match that the spec-3.22 retention theorem can mis-attribute as
 *	"recycled committed-below-horizon" rather than the truth ("aborted").  D1
 *	resolves each crash-left ACTIVE slot to a final verdict so cluster
 *	visibility never treats an in-flight-at-crash xact as committed (规则 8.A).
 */

/*
 * Liveness verdict for a crash-left ACTIVE slot's owning xid.  DEAD and
 * AMBIGUOUS both resolve the slot to TT_SLOT_ABORTED (fail-closed: an ACTIVE
 * slot we cannot prove live is aborted); LIVE keeps the slot ACTIVE.
 */
typedef enum ClusterTtRecoveryLiveness {
	CLUSTER_TT_RECOVERY_DEAD,	  /* not committed, not in-progress -> ABORTED */
	CLUSTER_TT_RECOVERY_LIVE,	  /* committed, or resurrected prepared 2PC -> keep */
	CLUSTER_TT_RECOVERY_AMBIGUOUS /* cannot determine -> fail-closed ABORTED */
} ClusterTtRecoveryLiveness;

/*
 * cluster_tt_recovery_classify_liveness -- pure classifier (no I/O, no shmem;
 *	unit-tested truth table).  Precedence: an indeterminable xid is AMBIGUOUS
 *	(fail-closed); a committed xid is LIVE (never abort a committed xact, even
 *	if its slot is still ACTIVE -- a lost commit_scn stamp is not an abort);
 *	an in-progress xid is LIVE (resurrected prepared 2PC); otherwise DEAD.
 */
extern ClusterTtRecoveryLiveness
cluster_tt_recovery_classify_liveness(bool determinable, bool did_commit, bool is_in_progress);

/*
 * cluster_tt_recovery_remote_authority_covers -- spec-4.8 D2 pure gate: may a
 *	survivor trust a materialized origin's durable TT outcome for a tuple at
 *	page LSN anchor_lsn?  Requires recovered_through >= anchor_lsn (4.7 D5 LSN
 *	gate); anchor_lsn == 0 skips the gate (is_materialized-only).  Pure; no I/O.
 */
extern bool cluster_tt_recovery_remote_authority_covers(uint64 recovered_through,
														uint64 anchor_lsn);

/*
 * cluster_tt_recovery_wrap_suspect -- spec-4.8 D3 pure gate (task#90): is a
 *	WRAP_ANY by-xid 1-match a 2^32-wrapped raw-xid collision?  True (fail-closed,
 *	narrowed AMBIGUOUS_WRAP) iff expected_wrap == WRAP_ANY AND retention is NOT
 *	reliable AND the matched commit_scn is below the horizon (or unjudgeable).
 *	retention_reliable short-circuits to false (a below-horizon collision's slot
 *	is already recycled, so a surviving below-horizon 1-match is a legit
 *	recycle-lag commit).  Pure; no I/O.
 */
extern bool cluster_tt_recovery_wrap_suspect(uint32 expected_wrap, SCN matched_scn, SCN horizon,
											 bool retention_reliable);

/*
 * spec-4.8 D7 -- index-aware physical rollback verdict (mini-plan v2).  Only a
 * DELETE inverse (clear the aborted deleter's xmax) is index-safe; INSERT/UPDATE
 * need a synchronous per-entry index point-delete that PG lacks -> fail-closed.
 */
typedef enum ClusterTtRecoveryRevertVerdict {
	CLUSTER_TT_REVERT_APPLY,	 /* DELETE inverse: clear xmax (index-safe) */
	CLUSTER_TT_REVERT_SKIP_DONE, /* idempotent: xmax already clear */
	CLUSTER_TT_REVERT_FAILCLOSED /* not index-safely revertable -> MVCC fallback */
} ClusterTtRecoveryRevertVerdict;

/*
 * cluster_tt_recovery_classify_revert -- spec-4.8 D7 pure gate: may an ABORTED
 *	xact's undo record be physically reverted on the real heap WITHOUT an index
 *	op?  APPLY iff a DELETE record + aborted deleter + the tuple still carries
 *	this deleter's xmax (identity); SKIP_DONE if the xmax is already clear
 *	(idempotent); FAILCLOSED otherwise (INSERT/UPDATE index-unsafe, or identity
 *	mismatch).  Pure; no I/O.
 */
extern ClusterTtRecoveryRevertVerdict
cluster_tt_recovery_classify_revert(bool is_delete_record, bool record_xid_aborted,
									bool tuple_xmax_matches, bool tuple_xmax_already_clear);

/*
 * cluster_tt_recovery_xact_liveness -- backend wrapper that consults PG's CLOG
 *	(TransactionIdDidCommit) and proc array (TransactionIdIsInProgress) for xid,
 *	then classifies via cluster_tt_recovery_classify_liveness.  Implemented in
 *	cluster_tt_recovery.c (backend-only; not linked into cluster_unit).
 */
extern ClusterTtRecoveryLiveness cluster_tt_recovery_xact_liveness(TransactionId xid);

/*
 * cluster_tt_recovery_resolve_active_slots -- scan this instance's undo segment
 *	headers and durably resolve every crash-left TT_SLOT_ACTIVE slot to
 *	TT_SLOT_ABORTED unless its owning xact is LIVE (committed / resurrected
 *	prepared).  Called once from StartupXLOG after recovery completes (WAL
 *	writes enabled, prepared set loaded), gated by cluster.enabled +
 *	cluster.tt_recovery_resolve_active.  Returns the number of slots resolved
 *	to ABORTED.  Backend-only (cluster_tt_recovery.c).
 */
extern int cluster_tt_recovery_resolve_active_slots(void);

/*
 * cluster_tt_recovery_observe_scn_highwater -- spec-4.8 D5 (L222): scan this
 *	instance's durable TT slots for the max commit_scn and Lamport-observe it
 *	into cluster_scn at startup, so a post-crash-restart read_scn is not left
 *	below the durable high-watermark (which over-fail-closes CR "snapshot too
 *	old").  Monotonic; correctness-safe.  Returns 0/1.  Backend-only.
 */
extern int cluster_tt_recovery_observe_scn_highwater(void);

/*
 * cluster_tt_recovery_physical_rollback -- spec-4.8 D7 part 2 (mini-plan v2):
 *	index-aware physical rollback of ABORTED xacts' DELETE writes (durably-
 *	ABORTED TT slot chains only).  Clears the aborted deleter's xmax
 *	(HEAP_XMAX_INVALID hint; WAL-free, idempotent, index-safe).  INSERT/UPDATE
 *	+ crash-left in-flight (no durable head) are matrix fail-closed (I10).
 *	Gated by classify_revert + identity.  Returns tuples reverted.  Backend-only.
 */
extern int cluster_tt_recovery_physical_rollback(void);


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

/*
 * spec-4.8 D1-D7 -- tt_recovery counters (8), surfaced under dump category
 * 'tt_recovery'.  Bumped by the recovery resolution / cross-node authority /
 * wrap-generation / liveness-relax / SCN-recovery / physical-revert paths.
 */
extern void cluster_tt_recovery_count_active_resolved_aborted(void);		 /* D1 */
extern void cluster_tt_recovery_count_remote_active_failclosed(void);		 /* D2 */
extern void cluster_tt_recovery_count_wrap_generation_disambiguated(void);	 /* D3 */
extern void cluster_tt_recovery_count_recycled_liveness_relaxed(void);		 /* D4 */
extern void cluster_tt_recovery_count_scn_highwater_recovered(void);		 /* D5 */
extern void cluster_tt_recovery_count_recovery_verdict_failclosed(void);	 /* D2/D7 */
extern void cluster_tt_recovery_count_heap_tuples_physically_reverted(void); /* D7 */
extern void cluster_tt_recovery_count_undo_revert_failclosed(void);			 /* D7 */

extern uint64 cluster_tt_recovery_active_resolved_aborted_count(void);
extern uint64 cluster_tt_recovery_remote_active_failclosed_count(void);
extern uint64 cluster_tt_recovery_wrap_generation_disambiguated_count(void);
extern uint64 cluster_tt_recovery_recycled_liveness_relaxed_count(void);
extern uint64 cluster_tt_recovery_scn_highwater_recovered_count(void);
extern uint64 cluster_tt_recovery_recovery_verdict_failclosed_count(void);
extern uint64 cluster_tt_recovery_heap_tuples_physically_reverted_count(void);
extern uint64 cluster_tt_recovery_undo_revert_failclosed_count(void);

#endif /* CLUSTER_TT_DURABLE_H */
