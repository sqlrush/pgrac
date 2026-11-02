/*-------------------------------------------------------------------------
 *
 * cluster_undo_retention.h
 *	  pgrac own-instance undo / TT-slot retention horizon (spec-3.12).
 *
 *	  The retention horizon is the min CLUSTER-source read_scn over all
 *	  backends' live snapshots (each backend publishes its per-backend min into
 *	  PGPROC->cluster_read_scn_atomic via snapmgr; spec-3.12 D1).  TT slots and
 *	  undo segments whose commit_scn is strictly below the horizon are needed by
 *	  no live reader and may be recycled; commit_scn at or newer than the
 *	  horizon is retained.
 *	  This retires the spec-3.11 L4 watermark fail-closed + D5 old-slot
 *	  fail-closed (durable TT slot kept alive while a reader needs it).
 *
 *	  Implementation note: cluster_undo_retention_horizon() lives in
 *	  procarray.c (it scans the ProcArray under ProcArrayLock, mirroring
 *	  GetOldestXmin); the recyclable predicates (spec-3.12 D2/D3) are pure and
 *	  cluster_unit-tested.  Callers must compute the horizon BEFORE taking
 *	  seg->lock / undo lifecycle_lock (spec-3.12 C17 lock ordering).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.12-retention-horizon.md (§2, D1)
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_undo_retention.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_RETENTION_H
#define CLUSTER_UNDO_RETENTION_H

#include "cluster/cluster_scn.h" /* SCN */
#include "cluster/cluster_heap_horizon.h"

/* One ProcArray cut, with the clock sampled before reading any PGPROC.
 * Contributor identity is diagnostic only, never a retention release proof.
 * A nonzero snapshot token is pgprocno + 1 and may be reused after exit. */
typedef struct ClusterUndoRetentionSample {
	SCN horizon_scn;
	SCN clock_scn;
	ClusterHeapFreezeReportV2ContributorKind contributor_kind;
	uint64 contributor_token;
} ClusterUndoRetentionSample;

/* Same locking/ordering as the scalar wrapper below; refusal clears out.
 * Clock and any published reader SCN must be finite, never idle-report MAX.
 * A successful cut retains a local read-publication promise under that lock;
 * it is not a global F/H or producer-lifetime certificate. */
extern bool cluster_undo_retention_horizon_sample(ClusterUndoRetentionSample *out);

/* True only for a finite H covered by this ProcArray boot's retained native
 * promise. Read-only SH lock, no reader/clock resample or PGPROC token. The
 * original boot/admission and every required peer still need validation;
 * this local check alone does not qualify a global F/H or native writer. */
extern bool cluster_undo_retention_horizon_covers(SCN horizon);

/*
 * cluster_undo_retention_horizon -- own-instance retention lower bound:
 *	min(pre-scan local clock, live read_scn, actual ADG future-read SCN).
 *	InvalidScn when
 *	disabled, an xmin-bearing backend has not published its read_scn, or
 *	the clock/reader publication is the reserved nonfinite idle sentinel.
 *	Scans under EXCLUSIVE ProcArrayLock, serializing against snapshot xmin
 *	and read-SCN publication. A regressing cut refuses without lowering the
 *	retained promise; no I/O, remote wait or clock resample inside. spec-3.12
 *	C17: do NOT hold seg->lock / undo lifecycle_lock when calling.
 */
extern SCN cluster_undo_retention_horizon(void);
extern SCN cluster_undo_retention_sample_min(SCN sampled_clock, TransactionId xmin,
											 SCN published_read_scn);

/*
 * cluster_undo_retention_all_quiescent -- TT lane (S3 idle-peer floor pin):
 *	true iff this node PROVABLY holds no retention constraint right now:
 *	every ProcArray entry has no assigned xid, no xmin and no published
 *	CLUSTER read_scn (zero active transactions AND zero held snapshots —
 *	both legs required).  Anything uncertain (cluster disabled, any live
 *	xid/xmin/read_scn, prepared-xact dummy PGPROCs with an xid) returns
 *	false. This diagnostic-only predicate uses SHARED ProcArrayLock; it no
 *	longer authorizes an unconstrained peer report.
 */
extern bool cluster_undo_retention_all_quiescent(void);


/*
 * Pure retention predicates (spec-3.12 D2/D3).  No shmem / no LWLock / no I/O;
 * implemented in cluster_undo_retention.c and cluster_unit-tested
 * (test_cluster_retention).  SCN comparisons go through scn_time_cmp().
 *
 *	cluster_tt_slot_recyclable:
 *	  Given a TT-slot allocator status (ClusterTTSlotAllocStatus value), its
 *	  commit_scn, and the current horizon, decide whether the slot may be
 *	  recycled.  ABORTED -> single-node C7 true; COMMITTED -> only when
 *	  commit_scn is strictly older than the horizon (a reader at the same SCN
 *	  as commit_scn still needs the pre-image). InvalidScn horizon is unproven
 *	  and retains COMMITTED; GUC-off permission belongs to the caller.
 *	  A COMMITTED slot with an unresolved (InvalidScn) commit_scn is retained
 *	  (rule 8.A fail-closed).
 *
 *	cluster_undo_segment_recyclable:
 *	  An undo segment may be recycled only when it is SEGMENT_COMMITTED AND its
 *	  retention watermark (= max commit_scn over the header's COMMITTED TT
 *	  slots) is strictly below the horizon.  ACTIVE / ALLOCATED / FULL-but-
 *	  ACTIVE are never recyclable (C5), even when they carry no COMMITTED slot.
 *	  Both modes retain a segment containing any ACTIVE slot, unknown slot
 *	  status or terminal slot with an invalid xid/wrap, even if no undo record
 *	  has been written yet. A segment with no COMMITTED slot is recyclable only
 *	  after these and the record seal/drain checks pass. An unresolved
 *	  COMMITTED commit_scn retains the segment (rule 8.A fail-closed).
 *
 *	cluster_undo_segment_recyclable_for_mode:
 *	  Peer mode additionally requires exact durable release evidence and an
 *	  empty undo anchor on every terminal slot. COMMITTED requires commit_scn
 *	  <= horizon; ABORTED requires InvalidScn with no invented time horizon.
 *	  The non-peer wrapper keeps its strict watermark < horizon comparison and
 *	  immediate reuse of valid ABORTED slots, subject to the common gates.
 */
struct UndoSegmentHeaderData;

extern bool cluster_tt_slot_recyclable(uint8 cts_status, SCN commit_scn, SCN horizon);
extern bool cluster_undo_segment_recyclable(const struct UndoSegmentHeaderData *hdr, SCN horizon);
extern bool cluster_undo_segment_recyclable_for_mode(const struct UndoSegmentHeaderData *hdr,
													 SCN horizon, bool peer_mode);


/*
 * spec-4.12a D1: oldest active-write boundary.
 *
 *	The minimum first_undo_scn over all in-flight (uncommitted, unprepared)
 *	undo writers, produced by cluster_undo_active_write_boundary() scanning the
 *	active-write registry (cluster_undo_record.c).  `infinite == true` means no
 *	in-flight writer is registered at all (quiesce): every sealed segment then
 *	drains.  Modelled as a struct (not a sentinel SCN value) because the SCN
 *	encoding layer forbids business code from minting a "max" SCN or comparing
 *	SCNs with raw </==/>; only scn_time_cmp() is allowed (L4/L38).
 */
typedef struct ClusterUndoActiveBoundary {
	bool infinite; /* true => no in-flight undo writer registered */
	SCN scn;	   /* min in-flight first_undo_scn (valid iff !infinite) */
} ClusterUndoActiveBoundary;

/*
 * cluster_undo_record_segment_drainable -- spec-4.12a D1/D3 pure ACTIVE ->
 *	COMMITTED drain gate for a record segment.  Pure (no shmem / no lock / no
 *	I/O): a function of the on-disk header plus the caller-supplied boundary,
 *	prepared-xact flag, the three excluded segment ids, and the recovery flag,
 *	so cluster_unit exercises every 8.A hard gate (test_cluster_retention).
 *	Returns true ONLY when the segment may safely advance ACTIVE -> COMMITTED;
 *	every uncertainty fails closed toward "retain".  See the .c for the gates.
 *
 *	recovery_in_progress (spec-4.12a D3, 硬门 4): true while the server is
 *	replaying WAL.  After a crash the in-memory active-write registry is empty,
 *	so the boundary degrades to {infinite} -- it cannot prove the absence of
 *	prepared / in-flight undo until RecoverPreparedTransactions has rebuilt the
 *	protected-slot view.  Draining during recovery is therefore never provably
 *	safe -> retain (fail-closed).  See the .c for why this is the single
 *	auditable recovery decision point.
 */
extern bool cluster_undo_record_segment_drainable(
	const struct UndoSegmentHeaderData *hdr, ClusterUndoActiveBoundary boundary,
	bool any_unresolved_prepared, uint32 fixed_first_segment_id, uint32 active_record_segment_id,
	uint32 active_tt_segment_id, bool recovery_in_progress);

#endif /* CLUSTER_UNDO_RETENTION_H */
