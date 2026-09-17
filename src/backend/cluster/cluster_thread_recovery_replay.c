/*-------------------------------------------------------------------------
 *
 * cluster_thread_recovery_replay.c
 *	  pgrac online thread-recovery RMW replay engine (spec-4.11 D1, contract).
 *
 *	  A survivor online-replays a dead WAL thread's data to shared storage within
 *	  the reconfig freeze window.  This engine streams the dead thread's WAL and,
 *	  for every block reference to a genuinely shared user-relation page, does a
 *	  read-modify-write directly against shared storage -- bypassing the buffer
 *	  pool.  Bypassing is safe because the dead thread's pages are fenced for the
 *	  freeze window: there is no concurrent access (the coherence precondition).
 *
 *	  This is crash-recovery redo, scoped to ONE thread's WAL, applied to shared
 *	  storage, with PG redo's global side effects stripped (the D0 STOP GATE A
 *	  reason ApplyWalRecord is not online-safe for a foreign segment's records:
 *	  it would advance the live survivor's global nextXid with a foreign xid).
 *	  The base for each apply is the LIVE shared page (not a clean FPI-base
 *	  reconstruction like spec-4.10); the record-end-vs-page LSN gate inside
 *	  cluster_thread_apply_record_to_page() makes the stream idempotent, exactly
 *	  as PG redo gates already-applied records on the on-disk page LSN.
 *
 *	  Corruption-critical contract (8.A).  Three gates before any write:
 *	    1. routing -- only cluster_smgr_which_for()==1 (genuinely shared user
 *	       relation) pages are touched; everything else (temp / catalog / opt-in
 *	       off) is a per-node concern the survivor owns -> data-pass skip.
 *	    2. existence (amend 1) -- a relation whose file is gone fails CLOSED, not
 *	       a BLK_NOTFOUND-style skip: 3a runs only the data-page apply matrix and
 *	       never the storage create/drop/truncate rmgr, so it cannot prove a
 *	       missing file is a legitimate drop.
 *	    3. range -- a block at/beyond EOF (relation extension / new init page)
 *	       fails CLOSED and forwards (Stage 5); the engine never reads past EOF.
 *	  Plus: any record the apply matrix cannot handle byte-for-byte (off-matrix
 *	  rmgr / unusable image) -> BLOCKED; a read error in-window -> BLOCKED; and
 *	  reaching clean end-of-WAL short of the validated scan_upper -> BLOCKED (the
 *	  WAL is incomplete; scan_upper is a durable boundary by precondition).
 *
 *	  SCOPE (contract).  This engine ONLY writes shared pages.  It does NOT
 *	  publish authority, start a worker, unfreeze, or flush WAL.  smgrwrite is a
 *	  WRITE-BACK, not a durable write (cluster_fs write is a bare pwrite with no
 *	  inline fsync, amend 2); 3b must issue a durability barrier on the touched
 *	  relations BEFORE publishing any 3-way authority.  A crash before that
 *	  barrier simply re-replays from a validated lower bound (redo-idempotent via
 *	  the LSN-gate), since no authority was ever published.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_thread_recovery_replay.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.11-thread-recovery.md (FROZEN v0.3)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/rmgr.h" /* RmgrId + RM_XACT_ID/RM_CLOG_ID/... for the visibility pass */
#include "access/xact.h" /* spec-6.14 D9: ParseCommitRecord/ParseAbortRecord (missing-rel forget) */
#include "access/xlog.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "access/xlogutils.h"
#include "storage/backendid.h"
#include "storage/buf_internals.h" /* InitBufferTag (spec-6.12h D-h3c PI base) */
#include "storage/bufpage.h"
#include "storage/relfilelocator.h"
#include "storage/smgr.h"
#include "utils/memutils.h" /* MemoryContextAlloc for the touched-rel collector */

#include "cluster/cluster_gcs_block.h"	 /* PI probe/snapshot/discard (spec-6.12h D-h3c) */
#include "cluster/cluster_guc.h"		 /* cluster_past_image; spec-6.14 D9 shared_catalog gate */
#include "cluster/cluster_xnode_lever.h" /* h_pi_recovery_base_* counters */

#include "cluster/cluster_remote_xact.h" /* online visibility divert (spec-4.11 3b-2) */
#include "cluster/cluster_thread_recovery.h"
#include "cluster/cluster_thread_recovery_apply.h"
#include "cluster/cluster_page_handoff.h"
#include "cluster/cluster_page_rmgr.h"
#include "cluster/cluster_page_stats.h"
#include "cluster/cluster_page_version.h"
#include "cluster/cluster_side_recovery.h"
#include "cluster/cluster_side_route.h"
#include "cluster/cluster_side_prepared.h"	 /* D-SIDE-03 production judge (队列 ④) */
#include "cluster/cluster_side_projection.h" /* D-SIDE-04 production judge (队列 ④) */
#include "cluster/cluster_side_stats.h"
#include "cluster/storage/cluster_smgr.h"

/*
 * touched_add -- record one APPLIED (rel, fork) for the durability barrier
 *		(spec-4.11 3b-2, amend 2), deduplicated.  The set is usually tiny (the
 *		relations one dead thread touched in a freeze window), so a linear scan
 *		is fine; recovery is not a hot path.  Grows in touched->mcxt (the
 *		orchestrator's context, so it survives the drive).  NULL = do not collect.
 */
static void
touched_add(ClusterThreadTouchedRels *touched, const RelFileLocator *rl, ForkNumber forknum)
{
	int i;

	if (touched == NULL)
		return;

	for (i = 0; i < touched->n; i++) {
		if (touched->items[i].forknum == forknum
			&& RelFileLocatorEquals(touched->items[i].rlocator, *rl))
			return; /* already collected */
	}

	if (touched->n == touched->cap) {
		int newcap = (touched->cap == 0) ? 8 : touched->cap * 2;
		MemoryContext ctx = (touched->mcxt != NULL) ? touched->mcxt : CurrentMemoryContext;

		if (touched->items == NULL)
			touched->items = (ClusterThreadTouchedRel *)MemoryContextAlloc(
				ctx, sizeof(ClusterThreadTouchedRel) * newcap);
		else
			touched->items = (ClusterThreadTouchedRel *)repalloc(
				touched->items, sizeof(ClusterThreadTouchedRel) * newcap);
		touched->cap = newcap;
	}

	touched->items[touched->n].rlocator = *rl;
	touched->items[touched->n].forknum = forknum;
	touched->n++;
}

/*
 * cluster_thread_recovery_touched_sync_all -- the durability barrier (spec-4.11
 *		3b-2, amend 2).  smgrimmedsync (synchronous fsync via the cluster_fs
 *		backend) every collected (rel, fork) so a published authority can never
 *		outlive an un-fsync'd dead-origin page write (8.A).  The orchestrator
 *		calls this on DONE, before publishing any authority.  May ereport on I/O
 *		failure -- the orchestrator runs it under its R13 harness -> BLOCKED.
 */
/*
 * RF-PAGE PGDEL-07 / RF-SIDE D-SIDE-08 production caller: the FND-10
 * conjunction and the retention exporter over the ACTUAL touched set
 * after the real smgrimmedsync barrier.  The page-proof facts are absent
 * (the production post-read/authority wiring is RED), so the outcome is
 * the honest deny — the failed-origin interval stays retained.
 */
static void cluster_thread_recovery_retention_judge(const ClusterThreadTouchedRels *touched);

void
cluster_thread_recovery_touched_sync_all(const ClusterThreadTouchedRels *touched)
{
	int i;

	if (touched == NULL)
		return;

	for (i = 0; i < touched->n; i++) {
		SMgrRelation reln = smgropen(touched->items[i].rlocator, InvalidBackendId);

		smgrimmedsync(reln, touched->items[i].forknum);
	}

	/*
	 * RF-PAGE PGDEL-07 §10.3 production-caller judgement: after the
	 * real durability barrier, the FND-10 handoff + the RF-SIDE
	 * retention exporter are fired with the actual touched set.
	 * READ-ONLY: the production page proofs (post-read / authority
	 * revalidation) do not exist yet, so the handoff correctly DENIES
	 * retirement — the counter records the denial and the retained
	 * interval stays pinned (PL-12 semantics in the live path).
	 */
	cluster_thread_recovery_retention_judge(touched);
}

/*
 * RF-PAGE PGDEL-07 / RF-SIDE D-SIDE-08 production caller: the FND-10
 * conjunction and the retention exporter over the ACTUAL touched set
 * after the real smgrimmedsync barrier.  The failed-origin identity and
 * the affected-set are real (orchestrator-provided dead_tid + touched
 * set); the page-proof facts are absent (the production post-read /
 * consumers wiring is RED), so the outcome is the honest deny — the
 * failed-origin interval stays retained.
 */
static void
cluster_thread_recovery_retention_judge(const ClusterThreadTouchedRels *touched)
{
	static ClusterPageRecoveryStats page_stats;
	static ClusterSideStats side_stats;
	static bool stats_inited = false;
	ClusterPageProof proof;
	ClusterPageHandoffInput handoff;
	ClusterSideRetentionProof retention;
	ClusterSideRetentionVerdict verdict;

	if (!stats_inited) {
		cluster_page_stats_init(&page_stats);
		cluster_side_stats_init(&side_stats);
		stats_inited = true;
	}

	memset(&proof, 0, sizeof(proof));
	memset(&handoff, 0, sizeof(handoff));
	handoff.proof = &proof; /* incomplete proof: FND-10 denies */
	(void)cluster_page_handoff_ready(&handoff);

	memset(&retention, 0, sizeof(retention));
	retention.failed_origin_thread
		= (touched != NULL ? touched->origin_thread_id : 0); /* real dead_tid */
	retention.affected_count = (uint32)(touched != NULL ? touched->n : 0);
	retention.all_bytes_durable = true; /* smgrimmedsync just ran */
	retention.all_post_read_ok = false; /* post-read wiring RED */
	retention.consumers_zero = false;	/* consumers wiring RED */
	verdict = cluster_side_retention_proof_ready(&retention);
	if (verdict != CLUSTER_SIDE_RETENTION_READY) {
		cluster_side_stats_blocked(&side_stats, false);
		cluster_page_stats_retire_denied(&page_stats);
	}
}

void
cluster_thread_recovery_touched_free(ClusterThreadTouchedRels *touched)
{
	if (touched == NULL)
		return;
	if (touched->items != NULL)
		pfree(touched->items);
	touched->items = NULL;
	touched->n = 0;
	touched->cap = 0;
}

/*
 * Deferred missing-relfile set (spec-6.14 D9).  Under cluster.shared_catalog a
 * relation file legitimately vanishes mid-stream: the dead origin dropped it
 * (a terminal record later in this SAME thread) and completed the unlink
 * before dying.  The engine cannot know that at the page record -- PG redo has
 * the same problem and solves it with the invalid-pages table
 * (log_invalid_page / forget_invalid_pages).  Mirror that protocol: remember
 * the locator, skip the block (a missing file is never written around), forget
 * it when a later terminal record drops the relfilenode, and fail closed at
 * end of drive if anything is left.
 */
typedef struct ClusterThreadMissingRels {
	RelFileLocator *items;
	int n;
	int cap;
} ClusterThreadMissingRels;

static void
missing_add(ClusterThreadMissingRels *missing, const RelFileLocator *rl)
{
	int i;

	for (i = 0; i < missing->n; i++)
		if (RelFileLocatorEquals(missing->items[i], *rl))
			return; /* already deferred */

	if (missing->n == missing->cap) {
		int newcap = (missing->cap == 0) ? 8 : missing->cap * 2;

		if (missing->items == NULL)
			missing->items = (RelFileLocator *)palloc(sizeof(RelFileLocator) * newcap);
		else
			missing->items
				= (RelFileLocator *)repalloc(missing->items, sizeof(RelFileLocator) * newcap);
		missing->cap = newcap;
	}
	missing->items[missing->n++] = *rl;
}

/*
 * missing_forget_dropped -- after the visibility pass consumed a foreign
 *		RM_XACT record cleanly, remove from the deferred set every relfilenode
 *		that record drops (the drop itself already executed inside
 *		cluster_remote_xact_apply -> DropRelationFiles).
 */
static void
missing_forget_dropped(ClusterThreadMissingRels *missing, XLogReaderState *reader)
{
	uint8 info = XLogRecGetInfo(reader) & XLOG_XACT_OPMASK;
	RelFileLocator *locs = NULL;
	int nlocs = 0;
	xl_xact_parsed_commit pc;
	xl_xact_parsed_abort pa;
	int i;

	if (info == XLOG_XACT_COMMIT || info == XLOG_XACT_COMMIT_PREPARED) {
		ParseCommitRecord(XLogRecGetInfo(reader), (xl_xact_commit *)XLogRecGetData(reader), &pc);
		locs = pc.xlocators;
		nlocs = pc.nrels;
	} else if (info == XLOG_XACT_ABORT || info == XLOG_XACT_ABORT_PREPARED) {
		ParseAbortRecord(XLogRecGetInfo(reader), (xl_xact_abort *)XLogRecGetData(reader), &pa);
		locs = pa.xlocators;
		nlocs = pa.nrels;
	}

	for (i = 0; i < nlocs; i++) {
		int keep = 0;
		int k;

		for (k = 0; k < missing->n; k++)
			if (!RelFileLocatorEquals(missing->items[k], locs[i]))
				missing->items[keep++] = missing->items[k];
		missing->n = keep;
	}
}

/*
 * replay_one_block -- classify and (if TARGET) read-modify-write one block
 *		reference of one record against shared storage.
 *
 *	Returns true on a clean outcome (TARGET applied/gated, or OUT_OF_SCOPE
 *	skipped) and false on a fail-closed BLOCKED outcome.  On true, *st is
 *	advanced; on false the caller stops the whole replay (8.A).  The page buffer
 *	is the caller's scratch (one read-modify-write at a time, no buffer pool).
 *
 * PGRAC modifications by SqlRush <sqlrush@gmail.com>:
 * What changed: spec-6.12h D-h3c — when this node kept a stamped Past Image
 *	of the target block (D-h1/D-h3a), it is consumed as the recovery base
 *	instead of the live storage page: the record is judged by the ship-SCN
 *	boundary gate (lineage already in the PI bytes -> skip; first post-ship
 *	record -> write PI+record back and DISCARD the resident PI, after which
 *	the storage page carries the PI lineage with a dead-thread pd_lsn and
 *	every later record rides the normal LSN-gated path below).
 *	window_first_scn is the xl_scn of the FIRST record of this replay
 *	window (any block); the PI is only eligible when
 *	cluster_pi_recovery_gate(window_first_scn, ship_scn) == SKIP: per-thread
 *	xl_scn is non-decreasing in LSN order, so a lineage window-head proves
 *	every pre-window record is lineage too — i.e. the storage page's already-
 *	reflected history is contained in the PI's frozen bytes and the write-
 *	back can never regress shared storage.  Any doubt (no usable snapshot,
 *	judge failure, unusable stamp, apply failure) abandons the PI (discard,
 *	fail-safe) and falls back to the storage path — today's behaviour.
 * Why: the PI shortcut recovers the block without an FPI in the window and
 *	without depending on a stale storage base (the D-h3b differential t/349
 *	proves the byte-for-byte equivalence of this rebuild).
 *
 *	missing (spec-6.14 D9): non-NULL only on the visibility-enabled
 *	shared-catalog path; a missing-file block ref is then DEFERRED (recorded +
 *	skipped) instead of an immediate BLOCKED -- see ClusterThreadMissingRels.
 */

/*
 * RF-PAGE PGDEL-06 §10.3 production-caller judgement — one record+block
 * through the whole PageVersion decision chain.  READ-ONLY: the existing
 * mutation path (LSN-gated apply + write-back) is unchanged, because
 * STOP-RF-PAGE-STABLE-BASE keeps the native apply/mutation face RED.
 *
 * The chain fired here, in order:
 *   1. cluster_page_classify        (§4.1 closed classifier)
 *   2. cluster_page_redo_decode     (§3.1 identity + hints; census-gated)
 *   3. cluster_page_version_decide  (§3.2 admission — the VersionToken
 *      producer contract is RED, so the decision is fail-closed BLOCKED,
 *      which is the honest current outcome)
 *   4. cluster_side_page_consumer_ready (D-SIDE-06 RF-PAGE integration)
 *      and cluster_side_resource_readiness (D-SIDE-07 serve gate)
 * and every outcome feeds the observability counters.  Deleting any gate
 * changes the counter profile (the §10.3 RED requirement).
 */
static void
cluster_thread_recovery_page_judge(XLogReaderState *reader, uint8 block_id,
								   const RelFileLocator *rl, ForkNumber forknum,
								   BlockNumber blocknum)
{
	static ClusterPageRecoveryStats page_stats;
	static ClusterSideStats side_stats;
	static bool stats_inited = false;
	ClusterPageClassifyInput cin;
	ClusterPageRedoDecoded decoded;
	ClusterSidePageConsumeInput consume;
	ClusterSideReadinessInput ready;
	ClusterPageClass cls;
	ClusterPageApplyVerdict verdict;
	uint8 rmid;
	uint16 opcode;
	bool decoded_ok;

	if (!stats_inited) {
		cluster_page_stats_init(&page_stats);
		cluster_side_stats_init(&side_stats);
		stats_inited = true;
	}

	rmid = XLogRecGetRmid(reader);
	opcode = XLogRecGetInfo(reader) & XLR_RMGR_INFO_MASK;

	/* 1. §4.1 closed classifier. */
	memset(&cin, 0, sizeof(cin));
	cin.rmid = rmid;
	cin.opcode = opcode;
	cin.forknum = forknum;
	cin.has_full_page_image = XLogRecHasBlockRef(reader, block_id)
							  && XLogRecHasBlockImage(reader, block_id)
							  && XLogRecBlockImageApply(reader, block_id);
	cls = cluster_page_classify(&cin);
	if (cls == CLUSTER_PAGE_CLASS_UNKNOWN || cls == CLUSTER_PAGE_CLASS_UNCLASSIFIED)
		cluster_page_stats_unknown_class_blocked(&page_stats);

	/* 2. §3.1 decode (census-gated identity + hints). */
	memset(&decoded, 0, sizeof(decoded));
	decoded_ok = cluster_page_redo_decode(reader, block_id, &decoded);
	if (!decoded_ok) {
		cluster_page_stats_source_missing(&page_stats);
		cluster_side_stats_blocked(&side_stats, true);
		return; /* no identity: the chain fails closed */
	}

	/* 3. §3.2 admission — the VersionToken producer contract is RED, so
	 * the working/expected/result versions cannot be constructed yet; the
	 * decision is the honest fail-closed BLOCKED. */
	verdict = cluster_page_version_decide(NULL, NULL, NULL, NULL);
	if (verdict == CLUSTER_PAGE_APPLY_BLOCKED)
		cluster_page_stats_version_mismatch(&page_stats);

	/* 4. D-SIDE-06/07 live consumers. */
	memset(&consume, 0, sizeof(consume));
	consume.identity = &decoded.identity;
	consume.page_class = decoded.page_class;
	consume.expected_before = NULL; /* no producer yet: fails closed */
	(void)cluster_side_page_consumer_ready(&consume);
	memset(&ready, 0, sizeof(ready));
	ready.resource_id = (uint16)blocknum;
	(void)cluster_side_resource_readiness(&ready);
	cluster_side_stats_domain(&side_stats, CLUSTER_SIDE_ROUTE_TT_UNDO);
	cluster_side_stats_durability(&side_stats);
	(void)rl;
}

/*
 * cluster_thread_recovery_prepared_judge -- RF-SIDE D-SIDE-03 production
 *	judge (implementation): fire the PREPARED/in-doubt binding
 *	verdict from the real replay path for XACT prepare/terminal records.
 *
 *	Read-only: no pending store, no mutation, no locks.  The durable
 *	database-scoped pending store is a G3 gap (census 2026-08-20:
 *	RECOVERY_CANONICAL_SHARED absent; the 2PC register binding exists in
 *	cluster_tt_2pc.c but the cluster-side pending store + RECO writer do
 *	not), so pending_durable_ok is HONESTLY false and the verdict stays
 *	BLOCKED (U-SIDE-06: no in-doubt polarity from a partial prepare;
 *	never a guessed commit/abort from an origin-local cache).  The GID
 *	identity leg is validated from the record itself; the TT/undo match
 *	leg needs the TT decode producer and stays false until it lands.
 *	Terminal records (COMMIT_PREPARED / ABORT_PREPARED) run the RECO
 *	resolution readiness check, which fails closed the same way.
 */
static void
cluster_thread_recovery_prepared_judge(XLogReaderState *reader, uint8 info)
{
	static ClusterSideStats side_stats;
	static bool stats_inited = false;
	ClusterSidePreparedVerdict verdict;
	uint8 opmask = info & XLOG_XACT_OPMASK;
	uint32 data_len;

	if (opmask != XLOG_XACT_PREPARE && opmask != XLOG_XACT_COMMIT_PREPARED
		&& opmask != XLOG_XACT_ABORT_PREPARED)
		return;
	if (reader == NULL)
		return;
	if (!stats_inited) {
		cluster_side_stats_init(&side_stats);
		stats_inited = true;
	}

	if (opmask == XLOG_XACT_PREPARE) {
		ClusterSidePreparedInput in;
		xl_xact_prepare *xlrec = (xl_xact_prepare *)XLogRecGetData(reader);

		data_len = XLogRecGetDataLen(reader);
		memset(&in, 0, sizeof(in));
		/* The prepare redo is present by construction (we are replaying
		 * it); the GID leg requires a well-formed non-empty GID that fits
		 * inside the record payload. */
		in.prepare_redo_ok = true;
		in.gid_identity_match = xlrec != NULL && data_len >= sizeof(xl_xact_prepare)
								&& cluster_side_prepared_gid_identity_ok(
									(const char *)xlrec + sizeof(xl_xact_prepare), xlrec->gidlen,
									data_len - (uint32)sizeof(xl_xact_prepare));
		/* G3 gap: no durable database-scoped pending store, no TT/undo
		 * match producer — the honest fail-closed legs. */
		in.pending_durable_ok = false;
		in.tt_undo_match = false;
		verdict = cluster_side_prepared_verdict(&in);
	} else {
		ClusterSidePreparedResolveInput r;

		memset(&r, 0, sizeof(r));
		r.terminal_redo_ok = true; /* we are replaying the terminal */
		r.pending_match = false;   /* G3 gap: no pending store */
		r.tt_undo_complete = false;
		(void)cluster_side_prepared_resolve_ready(&r);
		verdict = CLUSTER_SIDE_PREPARED_BLOCKED;
	}
	if (verdict == CLUSTER_SIDE_PREPARED_IN_DOUBT)
		cluster_side_stats_domain(&side_stats, CLUSTER_SIDE_ROUTE_TT_UNDO);
	else
		cluster_side_stats_blocked(&side_stats, false);
	(void)verdict;
}

/*
 * cluster_thread_recovery_projection_judge -- RF-SIDE D-SIDE-04 production
 *	judge (implementation): fire the derived-projection chain for
 *	CLOG / MULTIXACT / COMMIT_TS records from the real replay path.
 *
 *	Read-only: no SLRU mutation, no invalidate, no locks.  The canonical
 *	truth producer and the rebuild source-retention facts are not wired
 *	in production yet, so verification is HONESTLY false and the lookup
 *	fails closed (spec §2.4: a miss/UNKNOWN never serves a projection, a
 *	rebuild without a verified producer is a guess).  The domain counter
 *	observes the CLOG/MULTIXACT/COMMIT_TS projections.
 */
static void
cluster_thread_recovery_projection_judge(XLogReaderState *reader)
{
	static ClusterSideStats side_stats;
	static bool stats_inited = false;
	RmgrId rmid;
	ClusterSideProjectionKind kind;

	if (reader == NULL)
		return;
	rmid = XLogRecGetRmid(reader);
	if (rmid == RM_CLOG_ID)
		kind = CLUSTER_SIDE_PROJECTION_CLOG;
	else if (rmid == RM_MULTIXACT_ID)
		kind = CLUSTER_SIDE_PROJECTION_MULTIXACT;
	else if (rmid == RM_COMMIT_TS_ID)
		kind = CLUSTER_SIDE_PROJECTION_COMMIT_TS;
	else
		return;
	if (!stats_inited) {
		cluster_side_stats_init(&side_stats);
		stats_inited = true;
	}
	/* canonical_truth_ok / coverage / integrity / producer / retention:
	 * all RED in production — verification and rebuildability both fail
	 * closed and the lookup refuses (no projection is ever served). */
	(void)cluster_side_projection_lookup(false);
	(void)cluster_side_projection_rebuildable(kind, false, false);
	/* D-SIDE-04 real exporter: this IS the projection route (CLOG /
	 * MULTIXACT / COMMIT_TS redo in the replay stream), so the domain
	 * counter must land in the projection bucket — never the TT/undo
	 * one (observability honesty, implementation). */
	cluster_side_stats_domain(&side_stats, CLUSTER_SIDE_ROUTE_PROJECTION);
	cluster_side_stats_blocked(&side_stats, false);
}

static bool
replay_one_block(XLogReaderState *reader, uint8 block_id, char *page, SCN window_first_scn,
				 ClusterThreadTouchedRels *touched, ClusterThreadMissingRels *missing,
				 ClusterThreadReplayStats *st)
{
	RelFileLocator rl;
	ForkNumber forknum;
	BlockNumber blocknum;
	SMgrRelation reln;
	bool rel_exists;
	BlockNumber nblocks;
	ClusterThreadReplayBlockClass cls;
	XLogRecPtr applied_lsn;
	ClusterThreadApplyResult res;

	if (!XLogRecGetBlockTagExtended(reader, block_id, &rl, &forknum, &blocknum, NULL))
		return true; /* this block id carries no reference: nothing to do */

	/* Gate 1: only genuinely shared user-relation pages are this engine's job. */
	if (cluster_smgr_which_for(rl, InvalidBackendId) != 1) {
		st->blocks_out_of_scope++;
		return true; /* per-node (temp / catalog): data-pass skip */
	}

	/*
	 * Gates 2 (existence) and 3 (range) are decided by the pure classifier so
	 * the corruption-critical branches are unit-pinned; here we only compute its
	 * smgr inputs.  smgrnblocks is read only once existence is established.
	 */
	reln = smgropen(rl, InvalidBackendId);
	rel_exists = smgrexists(reln, forknum);
	nblocks = rel_exists ? smgrnblocks(reln, forknum) : 0;
	cls = cluster_thread_replay_classify_block(1, rel_exists, blocknum, nblocks);
	if (cls == CLUSTER_THREADREPLAY_BLK_BLOCKED) {
		if (!rel_exists && missing != NULL) {
			/*
			 * spec-6.14 D9: defer the verdict (see ClusterThreadMissingRels).
			 * The block apply is skipped entirely and the drive stays honest:
			 * end of drive fails closed unless a later terminal record in
			 * this stream dropped the relfilenode.  Beyond-EOF (rel_exists)
			 * keeps the immediate fail-closed below.
			 */
			missing_add(missing, &rl);
			st->blocks_missing_deferred++;
			return true;
		}
		ereport(DEBUG2,
				(errmsg_internal(
					"thread recovery replay fail-closed: rel %u/%u/%u fork %d block %u %s",
					rl.spcOid, rl.dbOid, rl.relNumber, forknum, blocknum,
					rel_exists ? "beyond EOF (extension/new page)" : "relation does not exist")));
		return false;
	}
	/* cls == TARGET (OUT_OF_SCOPE was handled by gate 1 above). */

	/*
	 * PGRAC: spec-6.12h D-h3c — Past Image recovery base (see the function
	 * header).  Probe is cheap (BufTable lookup); every uncertain outcome
	 * discards the PI and falls through to the storage path below.
	 */
	if (cluster_past_image) {
		BufferTag tag;
		SCN ship_scn;

		InitBufferTag(&tag, &rl, forknum, blocknum);
		if (cluster_bufmgr_block_is_pi(tag)) {
			if (cluster_bufmgr_snapshot_pi_block(tag, page, &ship_scn)
				&& cluster_pi_recovery_gate(window_first_scn, ship_scn) == CLUSTER_PI_GATE_SKIP) {
				XLogRecPtr pi_applied_lsn;
				ClusterThreadApplyResult pi_res;

				pi_res = cluster_pi_thread_apply_record_to_page(reader, block_id, page, ship_scn,
																&pi_applied_lsn);
				if (pi_res == CLUSTER_THREADAPPLY_DONE) {
					/* Lineage: already in the PI bytes.  Nothing to write;
					 * the PI stays armed for the first post-ship record. */
					st->blocks_gated++;
					return true;
				}
				if (pi_res == CLUSTER_THREADAPPLY_APPLIED) {
					/* First post-ship record: the PI base is consumed.
					 * Write PI+record back, then discard the resident PI —
					 * storage now carries the PI lineage with a dead-thread
					 * pd_lsn, so every later record of this block rides the
					 * normal LSN-gated storage path. */
					PageSetChecksumInplace((Page)page, blocknum);
					smgrwrite(reln, forknum, blocknum, page, false);
					touched_add(touched, &rl, forknum);
					(void)cluster_bufmgr_discard_pi_block(tag);
					cluster_lever_h_note_recovery_base(true);
					st->blocks_applied++;
					return true;
				}
				/* BLOCKED (unusable stamp / apply failure; NOOP impossible —
				 * the tag matched): abandon the PI, storage path below. */
			}
			/* No usable snapshot, self-containment judge failed, or the
			 * rebuild blocked: the PI cannot prove itself at-or-ahead of
			 * storage for this window — discard it (fail-safe: only the
			 * shortcut is lost) and take the storage path. */
			(void)cluster_bufmgr_discard_pi_block(tag);
			cluster_lever_h_note_recovery_base(false);
		}
	}

	/*
	 * RF-PAGE PGDEL-06 §10.3 production-caller judgement (read-only):
	 * the real orchestrator caller fires the whole PageVersion decision
	 * chain for THIS record+block — class, redo decode (identity/hints),
	 * the §3.2 admission decision and the RF-SIDE page-consumer verdict —
	 * before the existing mutation path runs.  The mutation path itself
	 * is UNCHANGED (STOP-RF-PAGE-STABLE-BASE keeps the native apply RED);
	 * the probe only makes the judgement chain a live production caller
	 * with real counters, so removing any gate turns its RED red.
	 */
	cluster_thread_recovery_page_judge(reader, block_id, &rl, forknum, blocknum);

	/*
	 * Read the LIVE shared page and apply the record onto it.  The LSN-gate
	 * inside cluster_thread_apply_record_to_page leaves an already-reflected
	 * page untouched (DONE), which is what makes a retry / cold redo idempotent.
	 */
	smgrread(reln, forknum, blocknum, page);
	res = cluster_thread_apply_record_to_page(reader, block_id, page, &applied_lsn);

	switch (res) {
	case CLUSTER_THREADAPPLY_APPLIED:

		/*
		 * Write-back (NOT durable, amend 2): stamp the write-time checksum (a
		 * no-op when checksums are off) and pwrite the page to shared storage.
		 * 3b fsyncs before publishing authority.
		 */
		PageSetChecksumInplace((Page)page, blocknum);
		smgrwrite(reln, forknum, blocknum, page, false);
		/* Record the write-back for the orchestrator's durability barrier
		 * (amend 2): a write-back is NOT durable until smgrimmedsync. */
		touched_add(touched, &rl, forknum);
		st->blocks_applied++;
		return true;

	case CLUSTER_THREADAPPLY_DONE:
		st->blocks_gated++; /* LSN-gate idempotent skip */
		return true;

	case CLUSTER_THREADAPPLY_NOOP:

		/*
		 * The tag matched above, so the wrapper cannot report NOOP (it only does
		 * for a missing block ref).  Treat defensively as a clean no-write.
		 */
		return true;

	case CLUSTER_THREADAPPLY_BLOCKED:
	default:
		ereport(DEBUG2, (errmsg_internal("thread recovery replay fail-closed: rel %u/%u/%u fork %d "
										 "block %u off-matrix or unusable record",
										 rl.spcOid, rl.dbOid, rl.relNumber, forknum, blocknum)));
		return false;
	}
}

/*
 * cluster_thread_recovery_replay_stream_ex -- source-agnostic combined replay
 *		core (spec-4.11 3b-2 extends the 3a data-only core).
 *
 *	Replays a positioned WAL reader up to scan_upper onto shared storage.  For
 *	each record: apply its shared user-relation data block refs (RMW), then -- if
 *	vis->do_visibility -- divert a foreign XACT/CLOG/MULTIXACT/COMMIT_TS record's
 *	OUTCOME to the per-origin store (online), the online analog of the cold merge
 *	loop's divert.  recovered_through advances ONLY after a record's data AND
 *	visibility were both handled, so ONE completeness gate covers both (8.A).
 *	touched (if non-NULL) collects every APPLIED (rel, fork) for the
 *	orchestrator's durability barrier.  See the header for the precondition
 *	(scan_upper is a validated-durable boundary) and the DONE / BLOCKED contract.
 *
 *	An online visibility ERROR (an unmaterializable foreign record) propagates
 *	OUT of here -- the caller's R13 harness demotes it to BLOCKED.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 */
ClusterThreadRecResult
cluster_thread_recovery_replay_stream_ex(XLogReaderState *reader, XLogRecPtr scan_upper,
										 const ClusterThreadVisCtx *vis,
										 ClusterThreadTouchedRels *touched,
										 ClusterThreadReplayStats *stats)
{
	ClusterThreadReplayStats st;
	PGAlignedBlock pagebuf;
	bool aborted = false;
	bool saw_straddle = false;
	bool reached;
	SCN window_first_scn = InvalidScn; /* spec-6.12h D-h3c: PI self-containment judge */
	ClusterThreadMissingRels missing = { NULL, 0, 0 };
	ClusterThreadMissingRels *missingp = NULL;

	memset(&st, 0, sizeof(st));

	if (reader == NULL || XLogRecPtrIsInvalid(scan_upper)) {
		if (stats)
			*stats = st;
		return CLUSTER_THREADREC_BLOCKED; /* invalid input -> fail-closed */
	}

	/*
	 * spec-6.14 D9: only the visibility-enabled path can prove a missing file
	 * legitimate (its terminal records execute the drops), and only under
	 * cluster.shared_catalog do foreign terminal records drop for real.  The
	 * data-only callers keep the immediate fail-closed.
	 */
	if (vis != NULL && vis->do_visibility && cluster_shared_catalog)
		missingp = &missing;

	for (;;) {
		char *errormsg;
		const XLogRecord *record;
		int max_id;
		int block_id;
		bool record_blocked = false;

		record = XLogReadRecord(reader, &errormsg);
		if (record == NULL) {
			/*
			 * A clean end of stream is fine; an in-window read error (a torn or
			 * unreadable record) fails closed.  Either way the reached-check
			 * below catches a window that ended short of scan_upper.
			 */
			if (errormsg != NULL && errormsg[0] != '\0')
				aborted = true;
			break;
		}

		/*
		 * Stop before applying any record that ENDS past scan_upper: the
		 * recovered version must not overshoot the validated boundary (mirror
		 * spec-4.10 reconstruct).  Seeing such a record also proves the WAL up
		 * to scan_upper was fully present -> the window was reached.
		 */
		if (reader->EndRecPtr > scan_upper) {
			saw_straddle = true;
			break;
		}

		st.records_scanned++;

		/* PGRAC: spec-6.12h D-h3c — the window's first xl_scn anchors the PI
		 * self-containment judge (per-thread xl_scn is non-decreasing in LSN
		 * order, so it upper-bounds every pre-window record's stamp). */
		if (st.records_scanned == 1)
			window_first_scn = (SCN)XLogRecGetScn(reader);

		max_id = XLogRecMaxBlockId(reader);
		for (block_id = 0; block_id <= max_id; block_id++) {
			if (!replay_one_block(reader, (uint8)block_id, pagebuf.data, window_first_scn, touched,
								  missingp, &st)) {
				record_blocked = true;
				break;
			}
		}

		if (record_blocked) {
			aborted = true;
			break;
		}

		/*
		 * RF-SIDE D-SIDE-03 production judge (implementation):
		 * fire the PREPARED/in-doubt binding chain for XACT
		 * prepare/terminal records — read-only, fail-closed on the G3
		 * pending-store gap (census 2026-08-20), before the visibility
		 * divert below.
		 */
		cluster_thread_recovery_prepared_judge(reader, XLogRecGetInfo(reader));

		/*
		 * Visibility pass (spec-4.11 3b-2): a foreign XACT/CLOG/MULTIXACT/
		 * COMMIT_TS record carries no data block ref (the block loop above was a
		 * no-op for it), but it carries the dead thread's commit/abort OUTCOME.
		 * Divert it to the per-origin outcome store (online), the online analog
		 * of the cold merge loop's divert (xlogrecovery.c) -- without it the
		 * survivor recovers the pages but not whether they are committed
		 * (false-visible, 8.A).  An online unmaterializable record raises a
		 * CATCHABLE ERROR (cluster_remote_xact_blocked_elevel) that propagates to
		 * the caller's R13 harness -> BLOCKED; recovered_through is NOT advanced
		 * for it (this record never completed).
		 */
		if (vis != NULL && vis->do_visibility) {
			RmgrId rmid = XLogRecGetRmid(reader);

			if (rmid == RM_XACT_ID || rmid == RM_CLOG_ID || rmid == RM_MULTIXACT_ID
				|| rmid == RM_COMMIT_TS_ID) {
				cluster_remote_xact_apply(vis->origin_node, reader, true);

				/* spec-6.14 D9: the record may have dropped relfiles whose
				 * earlier page refs were deferred -- legitimize them now. */
				if (rmid == RM_XACT_ID && missingp != NULL && missingp->n > 0)
					missing_forget_dropped(missingp, reader);
			}
		}

		/*
		 * RF-SIDE D-SIDE-04 production judge (implementation):
		 * fire the CLOG/MULTIXACT/COMMIT_TS derived-projection chain for
		 * every affected record — read-only, fail-closed on the RED
		 * canonical-truth/producer facts (the projection layer's real
		 * consumers are not wired yet).  Counters observe the domain.
		 */
		cluster_thread_recovery_projection_judge(reader);

		/*
		 * Advance recovered_through ONLY after every block reference AND the
		 * visibility pass of this record were handled cleanly, so a fail-closed
		 * never claims an unfinished record (8.A).
		 */
		st.recovered_through = reader->EndRecPtr;

		/*
		 * D4-aligned torn-tail tolerance (spec-4.11 3b-4c).  When scan_upper is a
		 * VALIDATED complete-record boundary (cluster_thread_recovery_validated_end
		 * returns the last complete record's EndRecPtr), a record ends EXACTLY at
		 * it: stop HERE.  Reading the next record would hit the dead thread's
		 * legitimate torn / partial tail -- the normal crash point validated_end
		 * deliberately tolerates -- and fail closed (errormsg -> aborted), even
		 * though the validated window applied cleanly.  recovered_through ==
		 * scan_upper already proves the whole window was present, so this is sound.
		 * When scan_upper falls MID-record (the explicit-window TEST path, where no
		 * record ends exactly at it) this never fires and the straddle / clean-EOF
		 * completeness check below still governs -- so the boundary semantics are
		 * unchanged for that path.
		 */
		if (reader->EndRecPtr == scan_upper)
			break;
	}

	/*
	 * spec-6.14 D9: a deferred missing-relfile page ref that no later terminal
	 * record in this stream dropped is real corruption -> fail closed, the
	 * exact verdict the pre-deferral engine gave immediately.
	 */
	if (missing.n > 0) {
		ereport(DEBUG2,
				(errmsg_internal("thread recovery replay fail-closed: %d missing relation(s) "
								 "never dropped by a later terminal record (first: %u/%u/%u)",
								 missing.n, missing.items[0].spcOid, missing.items[0].dbOid,
								 missing.items[0].relNumber)));
		aborted = true;
	}
	if (missing.items != NULL)
		pfree(missing.items);

	if (stats)
		*stats = st;

	/*
	 * Completeness (8.A): success requires reaching the validated boundary --
	 * either we read a record past scan_upper (so everything <= scan_upper was
	 * present) or the last processed record ended exactly at scan_upper.
	 * Reaching clean end-of-WAL short of scan_upper means the WAL is incomplete.
	 */
	reached = saw_straddle || (st.recovered_through == scan_upper);
	if (aborted || !reached)
		return CLUSTER_THREADREC_BLOCKED;
	return CLUSTER_THREADREC_DONE;
}

/*
 * cluster_thread_recovery_replay_stream -- the 3a data-only entry: the combined
 *		core with no visibility pass and no touched-rel collection.  Preserved so
 *		the 3a-local / test callers (and contract's driver) are unchanged.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 */
ClusterThreadRecResult
cluster_thread_recovery_replay_stream(XLogReaderState *reader, XLogRecPtr scan_upper,
									  ClusterThreadReplayStats *stats)
{
	return cluster_thread_recovery_replay_stream_ex(reader, scan_upper, NULL, NULL, stats);
}

/*
 * cluster_thread_recovery_replay_data -- 3a-local / test convenience entry.
 *
 *	Builds a local-WAL reader over [scan_lower, scan_upper], positions it, and
 *	drives the source-agnostic core.  LOCAL source only (the single-machine test
 *	simulates a foreign thread with local WAL); 3b adds the foreign-source entry
 *	calling the same core.  See the header.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 */
ClusterThreadRecResult
cluster_thread_recovery_replay_data(XLogRecPtr scan_lower, XLogRecPtr scan_upper,
									ClusterThreadReplayStats *stats)
{
	XLogReaderState *reader;
	ReadLocalXLogPageNoWaitPrivate *private_data;
	XLogRecPtr first_valid;
	ClusterThreadRecResult res;

	if (stats)
		memset(stats, 0, sizeof(*stats));

	if (XLogRecPtrIsInvalid(scan_lower) || XLogRecPtrIsInvalid(scan_upper)
		|| scan_lower > scan_upper)
		return CLUSTER_THREADREC_BLOCKED;

	private_data
		= (ReadLocalXLogPageNoWaitPrivate *)palloc0(sizeof(ReadLocalXLogPageNoWaitPrivate));
	reader = XLogReaderAllocate(wal_segment_size, NULL,
								XL_ROUTINE(.page_read = &read_local_xlog_page_no_wait,
										   .segment_open = &wal_segment_open,
										   .segment_close = &wal_segment_close),
								private_data);
	if (reader == NULL) {
		pfree(private_data);
		return CLUSTER_THREADREC_BLOCKED;
	}

	first_valid = XLogFindNextRecord(reader, scan_lower);
	if (XLogRecPtrIsInvalid(first_valid)) {
		XLogReaderFree(reader);
		pfree(private_data);
		return CLUSTER_THREADREC_BLOCKED; /* cannot position -> fail-closed */
	}

	res = cluster_thread_recovery_replay_stream(reader, scan_upper, stats);

	XLogReaderFree(reader);
	pfree(private_data);
	return res;
}

#else /* !USE_PGRAC_CLUSTER */

/* Disable-cluster build: this file compiles to nothing. */

#endif /* USE_PGRAC_CLUSTER */
